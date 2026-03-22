//! Manage epoch record voting and recording at epoch end.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use tn_config::KeyConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, epoch_records::EpochRecordDb};
use tn_types::{
    BlsAggregateSignature, BlsPublicKey, BlsSignature, Epoch, EpochCertificate, EpochRecord,
    EpochVote, Noticer, TaskSpawner, TnReceiver as _, B256,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{error, info, warn};

type VoteQueue = VecDeque<(Epoch, Sender<EpochVote>, Option<Receiver<EpochVote>>)>;

/// Both save and persist an epoch record and cert with logging.
async fn save_and_persist_with_logs(
    db: &EpochRecordDb,
    epoch_record: EpochRecord,
    cert: EpochCertificate,
) {
    let epoch = epoch_record.epoch;
    if let Err(e) = db.save(epoch_record, cert).await {
        error!(
            target: "epoch-manager",
            ?e,
            "failed to save epoch record/cert after retrieval for epoch {epoch}",
        );
    } else if let Err(e) = db.persist().await {
        error!(
            target: "epoch-manager",
            ?e,
            "failed to persist epoch record/cert after retrieval for epoch {epoch}",
        );
    }
}

/// Collect and manage votes for a specific epoch record.
async fn manage_epoch_votes(
    epoch_rec: EpochRecord,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    mut vote_rx: Receiver<EpochVote>,
    consensus_chain: ConsensusChain,
) {
    let epoch_hash = epoch_rec.digest();
    let mut committee_keys: HashSet<BlsPublicKey> = epoch_rec.committee.iter().copied().collect();
    let committee_index: HashMap<BlsPublicKey, usize> =
        epoch_rec.committee.iter().enumerate().map(|(i, k)| (*k, i)).collect();
    let mut sigs = Vec::new();
    let mut signed_authorities = roaring::RoaringBitmap::new();
    let mut my_vote = None;

    // If we are in the committee, sign and publish our vote
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
    let epoch_hash = epoch_rec.digest();
    // Collect votes from peers
    let mut reached_quorum = false;
    let mut timeout = Duration::from_millis(2500);
    let mut timeouts = 0;
    let mut alt_recs: HashMap<B256, HashSet<BlsPublicKey>> = HashMap::default();
    let mut committee_keys: HashSet<BlsPublicKey> = epoch_rec.committee.iter().copied().collect();
    let committee_size = committee_keys.len() as u64;
    let quorum = epoch_rec.super_quorum();
    loop {
        tokio::select! {
            result = tokio::time::timeout(timeout, vote_rx.recv()) => {
                match result {
                    Ok(None) => break,  // Channel closed- we are done.
                    Ok(Some(vote)) => {
                        if vote.epoch != epoch_rec.epoch {
                            continue;
                        }
                        // Signature already verified by handler, just check
                        // epoch_hash match and committee membership
                        if vote.epoch_hash == epoch_hash
                            && committee_keys.contains(&vote.public_key)
                        {
                            let source = vote.public_key;
                            if committee_keys.remove(&source) {
                                sigs.push(vote.signature);
                                if let Some(idx) = committee_index.get(&source) {
                                    signed_authorities.insert(*idx as u32);
                                }
                                if signed_authorities.len() >= quorum as u64 {
                                    reached_quorum = true;
                                    // Have quorum, wait briefly for more then move on
                                    timeout = Duration::from_secs(1);
                                }
                                if signed_authorities.len() >= committee_size {
                                    break;
                                }
                            }
                        } else if vote.epoch_hash != epoch_hash {
                            // Track votes for alternative epoch records using unique voter sets
                            if epoch_rec.committee.contains(&vote.public_key) {
                                let voters = alt_recs.entry(vote.epoch_hash).or_default();
                                voters.insert(vote.public_key);
                                if voters.len() >= quorum {
                                    error!(
                                        target: "epoch-manager",
                                        "Reached quorum on epoch record {} instead of {}.",
                                        vote.epoch_hash,
                                        epoch_hash,
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Timeout: have quorum or tried long enough
                        if reached_quorum || timeouts > 24 {
                            break;
                        }
                        timeouts += 1;
                        // Republish our vote in case peers are also struggling
                        if let Some(vote) = my_vote {
                            let _ = primary_network.publish_epoch_vote(vote).await;
                        }
                    }
                }
            }
        }
    }

    // Aggregate signatures and save the cert
    if reached_quorum {
        info!(
            target: "epoch-manager",
            "reached quorum on epoch close for {}/{epoch_hash}", epoch_rec.epoch
        );
        match BlsAggregateSignature::aggregate(&sigs[..], true) {
            Ok(aggregated_signature) => {
                let signature: BlsSignature = aggregated_signature.to_signature();
                let cert = EpochCertificate { epoch_hash, signature, signed_authorities };
                if epoch_rec.verify_with_cert(&cert) {
                    let epoch = epoch_rec.epoch;
                    if let Err(e) =
                        consensus_chain.epochs().save_certificate(cert.epoch_hash, cert).await
                    {
                        error!(
                            target: "epoch-manager",
                            ?e,
                            "failed to save epoch cert after reaching quorum {epoch}",
                        );
                    }
                    if let Err(e) = consensus_chain.epochs().persist().await {
                        error!(
                            target: "epoch-manager",
                            ?e,
                            "failed to persist epoch cert after reaching quorum {epoch}",
                        );
                    }
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
        let db = consensus_chain.epochs().clone();
        let network = primary_network.clone();
        // Try to recover by downloading the epoch record and cert from a peer
        let mut got_epoch_record = false;
        for _ in 0..5 {
            match network.request_epoch_cert(Some(epoch_rec.epoch), None).await {
                Ok((new_epoch_rec, cert)) => {
                    if new_epoch_rec.verify_with_cert(&cert) {
                        let new_epoch_hash = new_epoch_rec.digest();
                        if new_epoch_hash != epoch_hash {
                            warn!(
                                target: "epoch-manager",
                                "Over wrote expected epoch record {epoch_hash} with verified epoch record {new_epoch_hash}",
                            );
                            save_and_persist_with_logs(&db, new_epoch_rec, cert).await;
                        } else {
                            info!(
                                target: "epoch-manager",
                                "retrieved cert for epoch {}/{new_epoch_hash} from a peer", epoch_rec.epoch
                            );
                            save_and_persist_with_logs(&db, new_epoch_rec, cert).await;
                        }
                        got_epoch_record = true;
                        break;
                    }
                }
                Err(err) => error!(
                    target: "epoch-manager",
                    "failed to retrieve epoch from a peer {epoch_hash}: {err}",
                ),
            }
        }
        if !got_epoch_record {
            error!(
                target: "epoch-manager",
                "Failed to retrieve an epoch record for epoch {}",
                epoch_rec.epoch,
            );
        }
    }
}

/// Direct a newly received vote to it's task.
fn get_new_vote_channel(epoch: Epoch, vote_queues: &mut VoteQueue) -> Option<Receiver<EpochVote>> {
    for q in vote_queues.iter_mut() {
        if q.0 == epoch {
            return q.2.take();
        }
    }
    let (epoch_vote_tx, epoch_vote_rx) = mpsc::channel(10_000);
    if vote_queues.len() >= 5 {
        vote_queues.pop_front();
    }
    vote_queues.push_back((epoch, epoch_vote_tx, None));
    Some(epoch_vote_rx)
}

/// Direct a newly received vote to it's task.
async fn handle_new_vote(vote: EpochVote, vote_queues: &mut VoteQueue) {
    let mut remove = None;
    let mut found = false;
    for (i, q) in vote_queues.iter().enumerate() {
        if q.0 == vote.epoch {
            if q.1.send(vote).await.is_err() {
                remove = Some(i);
            }
            found = true;
            break;
        }
    }
    if let Some(remove) = remove {
        vote_queues.remove(remove);
    }
    if !found {
        let latest_epoch = vote_queues.iter().last().map(|q| q.0);
        if let Some(latest) = latest_epoch {
            if vote.epoch != latest + 1 {
                // Only collect for one future epoch.
                return;
            }
        }
        let (epoch_vote_tx, epoch_vote_rx) = mpsc::channel(10_000);
        if epoch_vote_tx.send(vote).await.is_ok() {
            if vote_queues.len() >= 5 {
                vote_queues.pop_front();
            }
            vote_queues.push_back((vote.epoch, epoch_vote_tx, Some(epoch_vote_rx)));
        }
    }
}

/// Spawn a node-lifetime task to collect epoch vote signatures.
///
/// This actor subscribes once to the `new_epoch_votes` channel and never drops the receiver,
/// eliminating the gap at epoch boundaries where votes could be lost. It watches for new
/// `EpochRecord`s via a `watch` channel and collects votes for each epoch.
pub(crate) fn spawn_epoch_vote_collector(
    consensus_chain: ConsensusChain,
    consensus_bus: ConsensusBusApp,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
) {
    let mut vote_rx = consensus_bus.subscribe_new_epoch_votes();
    let mut epoch_rx = consensus_bus.epoch_record_watch().subscribe();
    let task_spawner = node_task_spawner.clone();
    let mut vote_queues: VoteQueue = VecDeque::with_capacity(5);

    node_task_spawner.spawn_critical_task("Epoch Vote Collector", async move {
        loop {
            // Wait for an EpochRecord to arrive
            let epoch_rec = loop {
                tokio::select! {
                    _ = &node_shutdown => return,
                    _ = epoch_rx.changed() => {
                        if let Some(rec) = epoch_rx.borrow_and_update().clone() {
                            break rec;
                        }
                    }
                    result = vote_rx.recv() => {
                        match result {
                            None => return,  // Channel closed- we are done.
                            Some(vote) => {
                                handle_new_vote(vote, &mut vote_queues).await;
                            }
                        }
                    }
                }
            };

            if let Some(epoch_vote_rx) = get_new_vote_channel(epoch_rec.epoch, &mut vote_queues) {
                task_spawner.spawn_task(
                    format!("epoch votes for epoch {}", epoch_rec.epoch),
                    manage_epoch_votes(
                        epoch_rec,
                        key_config.clone(),
                        primary_network.clone(),
                        epoch_vote_rx,
                        consensus_chain.clone(),
                    ),
                );
            }
        }
    });
}

#[cfg(test)]
mod epoch_vote_collector_tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_network_libp2p::types::{MessageId, NetworkCommand};
    use tn_primary::{
        network::{PrimaryRequest, PrimaryResponse},
        ConsensusBus,
    };
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{BlsKeypair, Notifier, TaskManager, TnSender as _};

    /// Happy path: committee of 4, node signs + receives 3 peer votes → cert stored.
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

        // Node is kp1
        let key_config = KeyConfig::new_with_testing_key(kp1);

        // Committee of 4: super_quorum = (4*2)/3 + 1 = 3
        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };
        let epoch_hash = epoch_rec.digest();

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let temp_dir =
            TempDir::with_prefix("test_collector_reaches_quorum_and_stores_cert").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();

        // Mock network: drain commands and reply to Publish
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
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        // Sign votes from the 3 other committee members
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        let kc4 = KeyConfig::new_with_testing_key(kp4);
        let vote2 = epoch_rec.sign_vote(&kc2);
        let vote3 = epoch_rec.sign_vote(&kc3);
        let vote4 = epoch_rec.sign_vote(&kc4);

        // Buffer the votes in the channel (channel is already subscribed)
        consensus_bus.app().new_epoch_votes().send(vote2).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(vote3).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(vote4).await.unwrap();

        // Send the epoch record — collector wakes up, self-signs, reads buffered votes
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // Wait for collector to aggregate and store
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify cert is in DB
        let cert = consensus_chain.epochs().cert_by_digest(epoch_hash).await.expect("cert missing");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");

        // Shutdown
        node_shutdown.notify();
    }

    /// Duplicate votes from the same validator for an alt epoch record must not inflate the count.
    /// Before the fix, 4 duplicate alt votes from kp2 would reach quorum (4 >= 3) and break
    /// before correct votes were processed. After the fix, HashSet deduplicates to 1 unique voter.
    #[tokio::test]
    async fn test_duplicate_alt_votes_do_not_inflate_count() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        // Node is kp1
        let key_config = KeyConfig::new_with_testing_key(kp1);

        // Committee of 4: super_quorum = (4*2)/3 + 1 = 3
        let committee = vec![pk1, pk2, pk3, pk4];

        // The "correct" epoch record
        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee.clone(),
            ..Default::default()
        };
        let epoch_hash = epoch_rec.digest();

        // An "alt" epoch record — same epoch & committee but different next_committee → different
        // digest
        let alt_epoch_rec = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: vec![pk4, pk3, pk2, pk1], // reversed order → different digest
            ..Default::default()
        };
        assert_ne!(alt_epoch_rec.digest(), epoch_hash, "alt record must have a different digest");

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let com = fixture.committee();
        let temp_dir =
            TempDir::with_prefix("test_duplicate_alt_votes_do_not_inflate_count").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), com.clone()).await.unwrap();

        // Mock network: drain commands and reply to Publish
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
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        // kp2 signs a vote for the ALT epoch record
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let alt_vote = alt_epoch_rec.sign_vote(&kc2);

        // Buffer 4 duplicate alt votes from kp2 — before the fix this would inflate count to 4 >=
        // quorum(3)
        for _ in 0..4 {
            consensus_bus.app().new_epoch_votes().send(alt_vote).await.unwrap();
        }

        // Buffer correct votes from kp3 and kp4
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        let kc4 = KeyConfig::new_with_testing_key(kp4);
        let vote3 = epoch_rec.sign_vote(&kc3);
        let vote4 = epoch_rec.sign_vote(&kc4);
        consensus_bus.app().new_epoch_votes().send(vote3).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(vote4).await.unwrap();

        // Send the correct epoch record — collector wakes up, self-signs, reads buffered votes
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // Wait for collector to aggregate and store.
        // After reaching quorum the collector waits up to 1s for more votes before aggregating.
        tokio::time::sleep(Duration::from_millis(3000)).await;

        // Verify cert IS stored for the correct epoch hash
        // Quorum is kp1 (self-sign) + kp3 + kp4 = 3 >= 3
        let cert = consensus_chain
            .epochs()
            .cert_by_digest(epoch_hash)
            .await
            .expect("cert should be stored for correct epoch record");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");

        // Shutdown
        node_shutdown.notify();
    }
}
