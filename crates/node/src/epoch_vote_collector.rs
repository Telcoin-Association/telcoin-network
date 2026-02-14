use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use tn_config::KeyConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_storage::{tables::EpochCerts, EpochStore};
use tn_types::{
    BlsAggregateSignature, BlsPublicKey, BlsSignature, Database as TNDatabase, EpochCertificate,
    EpochRecord, EpochVote, Noticer, TaskSpawner, TnReceiver, B256, CHANNEL_CAPACITY,
};
use tokio::{sync::watch, time::MissedTickBehavior};
use tracing::{debug, error, info, warn};

const VOTE_REBROADCAST_INTERVAL: Duration = Duration::from_secs(5);
const QUORUM_COLLECTION_GRACE_PERIOD: Duration = Duration::from_secs(1);
const MAX_BUFFERED_EARLY_VOTES: usize = CHANNEL_CAPACITY;
const FALLBACK_MAX_ATTEMPTS: usize = 6;
const FALLBACK_RETRY_STEP_MS: u64 = 500;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum VoteProcessing {
    Ignored,
    Accepted,
    ReachedQuorum,
    AltQuorumReached(B256),
}

struct VoteTally<'a> {
    committee_index: &'a HashMap<BlsPublicKey, usize>,
    committee_keys: &'a mut HashSet<BlsPublicKey>,
    signed_authorities: &'a mut roaring::RoaringBitmap,
    sigs: &'a mut Vec<BlsSignature>,
    alt_recs: &'a mut HashMap<B256, usize>,
}

fn track_alternative_vote(
    vote: &EpochVote,
    epoch_hash: B256,
    epoch_committee: &[BlsPublicKey],
    quorum: usize,
    committee_keys: &mut HashSet<BlsPublicKey>,
    alt_recs: &mut HashMap<B256, usize>,
) -> bool {
    if vote.epoch_hash == epoch_hash || !epoch_committee.contains(&vote.public_key) {
        return false;
    }

    // Consume the validator on first alternative vote so one key cannot inflate
    // alternative record counts through repeated/equivocating votes.
    if !committee_keys.remove(&vote.public_key) {
        return false;
    }

    let votes = *alt_recs.get(&vote.epoch_hash).unwrap_or(&0);
    if votes + 1 >= quorum {
        return true;
    }

    alt_recs.insert(vote.epoch_hash, votes + 1);
    false
}

fn buffer_early_vote(early_votes: &mut VecDeque<EpochVote>, vote: EpochVote) {
    if early_votes.len() >= MAX_BUFFERED_EARLY_VOTES {
        // Drop oldest so we can keep draining the channel and avoid sender backpressure.
        let _ = early_votes.pop_front();
        debug!(
            target: "epoch-manager",
            "dropping oldest buffered epoch vote while waiting for epoch record",
        );
    }
    early_votes.push_back(vote);
}

fn process_vote_for_epoch(
    vote: EpochVote,
    epoch_hash: B256,
    epoch_record: &EpochRecord,
    quorum: usize,
    tally: VoteTally<'_>,
) -> VoteProcessing {
    // Votes are validated at the primary network handler before reaching this collector.
    // Keep this check to protect against local tests/internal producers bypassing the handler.
    if !vote.check_signature() {
        warn!(
            target: "epoch-manager",
            "received epoch vote with invalid signature for hash {}",
            vote.epoch_hash
        );
        return VoteProcessing::Ignored;
    }

    if vote.epoch_hash == epoch_hash {
        let source = vote.public_key;
        if !tally.committee_keys.remove(&source) {
            return VoteProcessing::Ignored;
        }

        tally.sigs.push(vote.signature);
        if let Some(idx) = tally.committee_index.get(&source) {
            tally.signed_authorities.insert(*idx as u32);
        }

        if tally.signed_authorities.len() >= quorum as u64 {
            VoteProcessing::ReachedQuorum
        } else {
            VoteProcessing::Accepted
        }
    } else if track_alternative_vote(
        &vote,
        epoch_hash,
        &epoch_record.committee,
        quorum,
        tally.committee_keys,
        tally.alt_recs,
    ) {
        VoteProcessing::AltQuorumReached(vote.epoch_hash)
    } else {
        VoteProcessing::Ignored
    }
}

async fn recover_epoch_record_with_peer_fallback<DB: TNDatabase>(
    consensus_db: &DB,
    primary_network: &PrimaryNetworkHandle,
    epoch_rx: &mut watch::Receiver<Option<EpochRecord>>,
    node_shutdown: &Noticer,
    epoch_rec: &EpochRecord,
    epoch_hash: B256,
) -> bool {
    for attempt in 1..=FALLBACK_MAX_ATTEMPTS {
        match primary_network.request_epoch_cert(Some(epoch_rec.epoch), None).await {
            Ok((new_epoch_rec, cert)) => {
                if new_epoch_rec.epoch != epoch_rec.epoch {
                    warn!(
                        target: "epoch-manager",
                        expected_epoch = epoch_rec.epoch,
                        response_epoch = new_epoch_rec.epoch,
                        "received epoch cert response for unexpected epoch",
                    );
                } else if new_epoch_rec.verify_with_cert(&cert) {
                    let new_epoch_hash = new_epoch_rec.digest();
                    if new_epoch_hash != epoch_hash {
                        warn!(
                            target: "epoch-manager",
                            "Over wrote expected epoch record {epoch_hash} with verified epoch record {new_epoch_hash}",
                        );
                        consensus_db.save_epoch_record_with_cert(&new_epoch_rec, &cert);
                    } else {
                        info!(
                            target: "epoch-manager",
                            "retrieved cert for epoch {new_epoch_hash} from a peer",
                        );
                        let _ = consensus_db.insert::<EpochCerts>(&new_epoch_hash, &cert);
                    }
                    return true;
                } else {
                    warn!(
                        target: "epoch-manager",
                        "received invalid epoch cert for epoch {} on fallback attempt {}",
                        epoch_rec.epoch,
                        attempt,
                    );
                }
            }
            Err(err) => warn!(
                target: "epoch-manager",
                "failed to retrieve epoch from peer for {} on fallback attempt {}: {}",
                epoch_hash,
                attempt,
                err,
            ),
        }

        if attempt == FALLBACK_MAX_ATTEMPTS {
            break;
        }

        let delay = Duration::from_millis(FALLBACK_RETRY_STEP_MS * attempt as u64);
        tokio::select! {
            _ = node_shutdown => return false,
            _ = epoch_rx.changed() => {}
            _ = tokio::time::sleep(delay) => {}
        }
    }
    false
}

pub(crate) fn spawn_epoch_vote_collector<DB: TNDatabase>(
    consensus_db: DB,
    consensus_bus: ConsensusBus,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
) {
    let mut vote_rx = consensus_bus.subscribe_new_epoch_votes();
    let mut epoch_rx = consensus_bus.epoch_record_watch().subscribe();

    node_task_spawner.spawn_critical_task("Epoch Vote Collector", async move {
        let mut early_votes = VecDeque::with_capacity(256);
        let mut last_seen_epoch_hash = None;

        loop {
            // Wait for a new EpochRecord while draining vote_rx to avoid channel buildup.
            let epoch_rec = loop {
                if let Some(rec) = epoch_rx.borrow_and_update().clone() {
                    let rec_hash = rec.digest();
                    if Some(rec_hash) != last_seen_epoch_hash {
                        last_seen_epoch_hash = Some(rec_hash);
                        break rec;
                    }
                }

                tokio::select! {
                    _ = &node_shutdown => return,
                    maybe_vote = vote_rx.recv() => {
                        match maybe_vote {
                            Some(vote) => buffer_early_vote(&mut early_votes, vote),
                            None => return,
                        }
                    }
                    _ = epoch_rx.changed() => {}
                }
            };

            // Check if we already have the cert for this epoch.
            if let Some((_, Some(_))) = consensus_db.get_epoch_by_number(epoch_rec.epoch) {
                continue;
            }

            let epoch_hash = epoch_rec.digest();
            let mut committee_keys: HashSet<BlsPublicKey> =
                epoch_rec.committee.iter().copied().collect();
            let committee_index: HashMap<BlsPublicKey, usize> =
                epoch_rec.committee.iter().enumerate().map(|(i, k)| (*k, i)).collect();
            let committee_size = committee_keys.len() as u64;
            let quorum = epoch_rec.super_quorum();
            let mut sigs = Vec::new();
            let mut signed_authorities = roaring::RoaringBitmap::new();
            let mut my_vote = None;
            let mut reached_quorum = false;
            let mut alt_recs: HashMap<B256, usize> = HashMap::default();
            let mut quorum_deadline = None;

            // If we are in the committee, sign and publish our vote.
            let me = key_config.primary_public_key();
            if committee_keys.contains(&me) {
                committee_keys.remove(&me);
                let epoch_vote = epoch_rec.sign_vote(&key_config);
                sigs.push(epoch_vote.signature);
                if let Some(idx) = committee_index.get(&me) {
                    signed_authorities.insert(*idx as u32);
                }
                info!(
                    target: "epoch-manager",
                    "publishing epoch record {epoch_hash}",
                );
                let _ = primary_network.publish_epoch_vote(epoch_vote).await;
                my_vote = Some(epoch_vote);
            }

            if signed_authorities.len() >= quorum as u64 {
                reached_quorum = true;
                quorum_deadline =
                    Some(tokio::time::Instant::now() + QUORUM_COLLECTION_GRACE_PERIOD);
            }

            // Process buffered votes only for this epoch hash.
            let mut buffered_votes_for_epoch = Vec::new();
            let mut remaining_votes = VecDeque::with_capacity(early_votes.len());
            while let Some(vote) = early_votes.pop_front() {
                if vote.epoch_hash == epoch_hash {
                    buffered_votes_for_epoch.push(vote);
                } else {
                    remaining_votes.push_back(vote);
                }
            }
            early_votes = remaining_votes;

            let mut should_stop = false;
            for vote in buffered_votes_for_epoch {
                match process_vote_for_epoch(
                    vote,
                    epoch_hash,
                    &epoch_rec,
                    quorum,
                    VoteTally {
                        committee_index: &committee_index,
                        committee_keys: &mut committee_keys,
                        signed_authorities: &mut signed_authorities,
                        sigs: &mut sigs,
                        alt_recs: &mut alt_recs,
                    },
                ) {
                    VoteProcessing::Ignored => {}
                    VoteProcessing::Accepted => {
                        if reached_quorum {
                            quorum_deadline = Some(
                                tokio::time::Instant::now() + QUORUM_COLLECTION_GRACE_PERIOD,
                            );
                        }
                    }
                    VoteProcessing::ReachedQuorum => {
                        reached_quorum = true;
                        quorum_deadline =
                            Some(tokio::time::Instant::now() + QUORUM_COLLECTION_GRACE_PERIOD);
                    }
                    VoteProcessing::AltQuorumReached(alt_hash) => {
                        error!(
                            target: "epoch-manager",
                            "Reached quorum on epoch record {} instead of {}.",
                            alt_hash,
                            epoch_hash,
                        );
                        should_stop = true;
                        break;
                    }
                }

                if signed_authorities.len() >= committee_size {
                    should_stop = true;
                    break;
                }
            }

            // Collect votes from peers.
            let mut rebroadcast_tick = tokio::time::interval(VOTE_REBROADCAST_INTERVAL);
            rebroadcast_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Consume the initial immediate tick.
            rebroadcast_tick.tick().await;

            while !should_stop {
                tokio::select! {
                    _ = &node_shutdown => return,
                    // Move to the next epoch record.
                    _ = epoch_rx.changed() => break,
                    maybe_vote = vote_rx.recv() => {
                        match maybe_vote {
                            Some(vote) => {
                                match process_vote_for_epoch(
                                    vote,
                                    epoch_hash,
                                    &epoch_rec,
                                    quorum,
                                    VoteTally {
                                        committee_index: &committee_index,
                                        committee_keys: &mut committee_keys,
                                        signed_authorities: &mut signed_authorities,
                                        sigs: &mut sigs,
                                        alt_recs: &mut alt_recs,
                                    },
                                ) {
                                    VoteProcessing::Ignored => {}
                                    VoteProcessing::Accepted => {
                                        if reached_quorum {
                                            quorum_deadline = Some(
                                                tokio::time::Instant::now() + QUORUM_COLLECTION_GRACE_PERIOD,
                                            );
                                        }
                                    }
                                    VoteProcessing::ReachedQuorum => {
                                        reached_quorum = true;
                                        quorum_deadline =
                                            Some(tokio::time::Instant::now() + QUORUM_COLLECTION_GRACE_PERIOD);
                                    }
                                    VoteProcessing::AltQuorumReached(alt_hash) => {
                                        error!(
                                            target: "epoch-manager",
                                            "Reached quorum on epoch record {} instead of {}.",
                                            alt_hash,
                                            epoch_hash,
                                        );
                                        should_stop = true;
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                    _ = rebroadcast_tick.tick() => {
                        // Continue to republish periodically while collecting votes.
                        if let Some(vote) = my_vote {
                            let _ = primary_network.publish_epoch_vote(vote).await;
                        }
                    }
                    _ = async {
                        if let Some(deadline) = quorum_deadline {
                            tokio::time::sleep_until(deadline).await;
                        }
                    }, if quorum_deadline.is_some() => {
                        break;
                    }
                }

                if signed_authorities.len() >= committee_size {
                    break;
                }
            }

            // Aggregate signatures and save the cert.
            if reached_quorum {
                info!(
                    target: "epoch-manager",
                    "reached quorum on epoch close for {epoch_hash}",
                );
                match BlsAggregateSignature::aggregate(&sigs[..], true) {
                    Ok(aggregated_signature) => {
                        let signature: BlsSignature = aggregated_signature.to_signature();
                        let cert = EpochCertificate { epoch_hash, signature, signed_authorities };
                        if epoch_rec.verify_with_cert(&cert) {
                            let _ = consensus_db.insert::<EpochCerts>(&cert.epoch_hash, &cert);
                        } else {
                            error!(
                                target: "epoch-manager",
                                "failed to verify epoch record and cert for {epoch_hash}",
                            );
                        }
                    }
                    Err(_) => {
                        error!(
                            target: "epoch-manager",
                            "failed to aggregate epoch record signatures for {epoch_hash}",
                        );
                    }
                }
            } else {
                error!(
                    target: "epoch-manager",
                    "failed to reach quorum on epoch close for {epoch_hash} {epoch_rec:?}",
                );
                if !recover_epoch_record_with_peer_fallback(
                    &consensus_db,
                    &primary_network,
                    &mut epoch_rx,
                    &node_shutdown,
                    &epoch_rec,
                    epoch_hash,
                )
                .await
                {
                    error!(
                        target: "epoch-manager",
                        "Failed to retrieve an epoch record for epoch {}",
                        epoch_rec.epoch,
                    );
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_network_libp2p::{
        error::NetworkError,
        types::{MessageId, NetworkCommand},
    };
    use tn_primary::network::{PrimaryRequest, PrimaryResponse};
    use tn_storage::mem_db::MemDatabase;
    use tn_types::{BlsKeypair, Notifier, TaskManager, TnSender as _};

    fn make_epoch_record(epoch: u32, committee: Vec<BlsPublicKey>) -> EpochRecord {
        EpochRecord {
            epoch,
            committee: committee.clone(),
            next_committee: committee,
            ..Default::default()
        }
    }

    fn make_epoch_cert(epoch_rec: &EpochRecord, signers: &[&KeyConfig]) -> EpochCertificate {
        let committee_index: HashMap<BlsPublicKey, usize> =
            epoch_rec.committee.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        let mut sigs = Vec::with_capacity(signers.len());
        let mut signed_authorities = roaring::RoaringBitmap::new();

        for signer in signers {
            let vote = epoch_rec.sign_vote(*signer);
            sigs.push(vote.signature);
            let idx = committee_index.get(&vote.public_key).expect("signer in committee");
            signed_authorities.insert(*idx as u32);
        }

        let aggregate =
            BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate signatures");
        EpochCertificate {
            epoch_hash: epoch_rec.digest(),
            signature: aggregate.to_signature(),
            signed_authorities,
        }
    }

    async fn wait_for_cert(db: &MemDatabase, epoch_hash: B256) -> EpochCertificate {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(cert) = db.get::<EpochCerts>(&epoch_hash).expect("db read") {
                    break cert;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("timed out waiting for epoch cert")
    }

    /// Happy path: committee of 4, node signs + receives 3 peer votes -> cert stored.
    #[tokio::test]
    async fn test_collector_reaches_quorum_and_stores_cert() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        let key_config = KeyConfig::new_with_testing_key(kp1);

        let epoch_rec = make_epoch_record(0, vec![pk1, pk2, pk3, pk4]);
        let epoch_hash = epoch_rec.digest();

        let consensus_bus = ConsensusBus::new();
        let db = MemDatabase::default();

        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move {
            while let Some(cmd) = net_rx.recv().await {
                if let NetworkCommand::Publish { reply, .. } = cmd {
                    let _ = reply.send(Ok(MessageId::new(b"test")));
                }
            }
        });

        let task_manager = TaskManager::default();
        let node_shutdown = Notifier::new();

        spawn_epoch_vote_collector(
            db.clone(),
            consensus_bus.clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        let kc4 = KeyConfig::new_with_testing_key(kp4);

        consensus_bus.new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();
        consensus_bus.new_epoch_votes().send(epoch_rec.sign_vote(&kc4)).await.unwrap();

        consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        let cert = wait_for_cert(&db, epoch_hash).await;
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");

        node_shutdown.notify();
    }

    #[tokio::test]
    async fn test_collector_skips_certified_epoch_and_collects_next_epoch() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        let kc1 = KeyConfig::new_with_testing_key(kp1);
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        let kc4 = KeyConfig::new_with_testing_key(kp4);

        let epoch0 = make_epoch_record(0, vec![pk1, pk2, pk3, pk4]);
        let epoch1 = make_epoch_record(1, vec![pk1, pk2, pk3, pk4]);
        let epoch1_hash = epoch1.digest();
        let epoch0_cert = make_epoch_cert(&epoch0, &[&kc1, &kc2, &kc3]);

        let consensus_bus = ConsensusBus::new();
        let db = MemDatabase::default();
        db.save_epoch_record_with_cert(&epoch0, &epoch0_cert);

        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move {
            while let Some(cmd) = net_rx.recv().await {
                if let NetworkCommand::Publish { reply, .. } = cmd {
                    let _ = reply.send(Ok(MessageId::new(b"test")));
                }
            }
        });

        let task_manager = TaskManager::default();
        let node_shutdown = Notifier::new();

        spawn_epoch_vote_collector(
            db.clone(),
            consensus_bus.clone(),
            kc1,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        consensus_bus.epoch_record_watch().send_replace(Some(epoch0));
        tokio::time::sleep(Duration::from_millis(25)).await;

        consensus_bus.new_epoch_votes().send(epoch1.sign_vote(&kc2)).await.unwrap();
        consensus_bus.new_epoch_votes().send(epoch1.sign_vote(&kc3)).await.unwrap();
        consensus_bus.new_epoch_votes().send(epoch1.sign_vote(&kc4)).await.unwrap();
        consensus_bus.epoch_record_watch().send_replace(Some(epoch1.clone()));

        let cert = wait_for_cert(&db, epoch1_hash).await;
        assert!(epoch1.verify_with_cert(&cert), "expected collector to move to next epoch record");

        node_shutdown.notify();
    }

    #[tokio::test]
    async fn test_collector_recovers_cert_from_peer_when_local_quorum_missing() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        let kc1 = KeyConfig::new_with_testing_key(kp1);
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);

        let epoch0 = make_epoch_record(0, vec![pk1, pk2, pk3, pk4]);
        let epoch1 = make_epoch_record(1, vec![pk1, pk2, pk3, pk4]);
        let epoch0_hash = epoch0.digest();
        let epoch0_cert = make_epoch_cert(&epoch0, &[&kc1, &kc2, &kc3]);

        let consensus_bus = ConsensusBus::new();
        let db = MemDatabase::default();

        let fallback_requests = Arc::new(AtomicUsize::new(0));
        let request_counter = fallback_requests.clone();
        let response_epoch0 = epoch0.clone();
        let response_cert0 = epoch0_cert.clone();

        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move {
            while let Some(cmd) = net_rx.recv().await {
                match cmd {
                    NetworkCommand::Publish { reply, .. } => {
                        let _ = reply.send(Ok(MessageId::new(b"test")));
                    }
                    NetworkCommand::SendRequestAny { request, reply } => match request {
                        PrimaryRequest::EpochRecord { epoch: Some(0), .. } => {
                            let seen = request_counter.fetch_add(1, Ordering::Relaxed);
                            if seen < 2 {
                                let _ = reply.send(Err(NetworkError::RPCError(
                                    "simulated transient failure".to_string(),
                                )));
                            } else {
                                let _ = reply.send(Ok(PrimaryResponse::EpochRecord {
                                    record: response_epoch0.clone(),
                                    certificate: response_cert0.clone(),
                                }));
                            }
                        }
                        _ => {
                            let _ = reply.send(Err(NetworkError::RPCError(
                                "unsupported request in test".to_string(),
                            )));
                        }
                    },
                    _ => {}
                }
            }
        });

        let task_manager = TaskManager::default();
        let node_shutdown = Notifier::new();

        spawn_epoch_vote_collector(
            db.clone(),
            consensus_bus.clone(),
            kc1,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        consensus_bus.epoch_record_watch().send_replace(Some(epoch0));
        tokio::time::sleep(Duration::from_millis(50)).await;
        consensus_bus.epoch_record_watch().send_replace(Some(epoch1));

        let cert = wait_for_cert(&db, epoch0_hash).await;
        assert!(cert.epoch_hash == epoch0_hash);
        assert!(
            fallback_requests.load(Ordering::Relaxed) >= 3,
            "expected request_epoch_cert retries"
        );

        node_shutdown.notify();
    }

    #[tokio::test]
    async fn test_collector_drains_vote_channel_before_epoch_record() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        let key_config = KeyConfig::new_with_testing_key(kp1);
        let vote_signer = KeyConfig::new_with_testing_key(kp2);
        let epoch_rec = make_epoch_record(7, vec![pk1, pk2, pk3, pk4]);
        let vote = epoch_rec.sign_vote(&vote_signer);

        let consensus_bus = ConsensusBus::new();
        let db = MemDatabase::default();

        let (net_tx, _net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);

        let task_manager = TaskManager::default();
        let node_shutdown = Notifier::new();

        spawn_epoch_vote_collector(
            db,
            consensus_bus.clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        let send_result = tokio::time::timeout(Duration::from_secs(10), async {
            for _ in 0..(CHANNEL_CAPACITY + 128) {
                consensus_bus.new_epoch_votes().send(vote).await.unwrap();
            }
        })
        .await;
        assert!(
            send_result.is_ok(),
            "collector should drain epoch votes while waiting for epoch records"
        );

        node_shutdown.notify();
    }

    #[test]
    fn test_alt_record_votes_remove_committee_member_once() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);

        let epoch_rec =
            make_epoch_record(0, vec![*kp1.public(), *kp2.public(), *kp3.public(), *kp4.public()]);

        let epoch_hash = epoch_rec.digest();
        let alt_voter = *kp2.public();
        let bad_hash_1 = B256::from([7u8; 32]);
        let bad_hash_2 = B256::from([9u8; 32]);

        let mut committee_keys: HashSet<_> = epoch_rec.committee.iter().copied().collect();
        let mut alt_recs: HashMap<B256, usize> = HashMap::default();

        let vote = {
            let key = KeyConfig::new_with_testing_key(kp2);
            let vote = epoch_rec.sign_vote(&key);
            EpochVote { epoch_hash: bad_hash_1, ..vote }
        };
        let second_alt_vote = EpochVote { epoch_hash: bad_hash_2, ..vote };

        let reached_1 = track_alternative_vote(
            &vote,
            epoch_hash,
            &epoch_rec.committee,
            epoch_rec.super_quorum(),
            &mut committee_keys,
            &mut alt_recs,
        );

        let reached_2 = track_alternative_vote(
            &second_alt_vote,
            epoch_hash,
            &epoch_rec.committee,
            epoch_rec.super_quorum(),
            &mut committee_keys,
            &mut alt_recs,
        );

        assert!(!reached_1);
        assert!(!reached_2);
        assert!(!committee_keys.contains(&alt_voter));
        assert_eq!(alt_recs.get(&bad_hash_1), Some(&1));
        assert_eq!(alt_recs.get(&bad_hash_2), None);
    }
}
