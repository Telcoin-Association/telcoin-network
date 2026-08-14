//! Manage epoch record voting and recording at epoch end.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use tn_config::KeyConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, epoch_records::EpochRecordDb};
use tn_types::{
    BlsAggregateSignature, BlsPublicKey, BlsSignature, Epoch, EpochCertificate, EpochDigest,
    EpochRecord, EpochVote, Noticer, TaskSpawner, TnReceiver as _,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{error, info, warn};

type VoteQueue = VecDeque<(Epoch, Sender<EpochVote>, Option<Receiver<EpochVote>>)>;

/// Per-iteration wait for the next epoch vote in [`manage_epoch_votes`]'s collection loop.
pub(crate) const EPOCH_VOTE_RECV_TIMEOUT: Duration = Duration::from_millis(2500);

/// Timeout-count threshold in [`manage_epoch_votes`]: a vote round tolerates this many full
/// [`EPOCH_VOTE_RECV_TIMEOUT`] windows of ordinary vote-propagation lag before it ends the
/// round and enters the recovery-then-retry path (peer fetch, DB check, backoff, republish).
/// Rounds repeat until a certificate exists, so this bounds one round, not the whole effort.
pub(crate) const MAX_EPOCH_VOTE_TIMEOUTS: u32 = 24;

/// Backoff before the first vote-round retry in [`manage_epoch_votes`]; doubles per round.
pub(crate) const EPOCH_VOTE_ROUND_BACKOFF_START: Duration = Duration::from_secs(5);

/// Cap on the doubling inter-round backoff in [`manage_epoch_votes`]: retries settle at one
/// round per this interval plus the round itself. The backoff bounds the round-end recovery
/// work (the peer-fetch pass and store check run once per round); the vote republish keeps
/// its per-window cadence ([`EPOCH_VOTE_RECV_TIMEOUT`]) inside every round — deliberately, so
/// a fleet whose peers come up staggered still hears votes promptly — which puts a
/// long-parked member's steady-state gossip at roughly one small vote message per window,
/// pausing only during the backoff sleeps.
pub(crate) const EPOCH_VOTE_ROUND_BACKOFF_CAP: Duration = Duration::from_secs(60);

/// Number of peer-recovery attempts [`manage_epoch_votes`] makes (each one
/// `request_epoch_cert` call) after failing to reach a local vote quorum.
pub(crate) const EPOCH_CERT_RECOVERY_ATTEMPTS: u32 = 5;

/// Peers tried per recovery attempt: mirrors the `0..3` peer-rotation loop inside
/// `PrimaryNetworkHandle::request_epoch_cert` (`tn_primary::network`), which this module has
/// no handle on but whose worst case budget consumers must account for.
pub(crate) const EPOCH_CERT_RECOVERY_PEERS_PER_ATTEMPT: u32 = 3;

/// Upper bound on one peer request during recovery: the libp2p request-response timeout. The
/// swarm is built with `request_response::Config::default()` (10s) and `tn-network-libp2p`
/// never overrides it, so each `request_epoch_cert` peer try is bounded by this.
pub(crate) const EPOCH_CERT_RECOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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

/// One peer-recovery pass: download a certified record for `epoch_rec.epoch` from peers.
///
/// Tries up to [`EPOCH_CERT_RECOVERY_ATTEMPTS`] `request_epoch_cert` calls, anchoring any
/// downloaded record to the locally-trusted committee before storing it (see the comment at
/// the validation site). Returns `true` once a validated `(record, certificate)` pair has been
/// saved — including a verified record that differs from `epoch_rec` (the overwrite case).
async fn try_fetch_certified_record(
    epoch_rec: &EpochRecord,
    primary_network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
) -> bool {
    let epoch_hash = epoch_rec.digest();
    let db = consensus_chain.epochs().clone();
    for _ in 0..EPOCH_CERT_RECOVERY_ATTEMPTS {
        match primary_network.request_epoch_cert(Some(epoch_rec.epoch), None).await {
            Ok((new_epoch_rec, cert)) => {
                // Anchor the downloaded record to the locally-trusted committee using the
                // same routine the state-sync ingest path uses (see crates/state-sync
                // epoch.rs). `verify_with_cert` alone only proves the record is
                // self-consistent with its own embedded committee, so a peer could return an
                // attacker-committee record self-signed by that committee; the anchor rejects
                // it because its committee is not the one derived from prev.next_committee.
                if db
                    .validate_downloaded_record(epoch_rec.epoch, &new_epoch_rec, &cert)
                    .await
                    .is_valid()
                {
                    let new_epoch_hash = new_epoch_rec.digest();
                    if new_epoch_hash != epoch_hash {
                        warn!(
                            target: "epoch-manager",
                            "Over wrote expected epoch record {epoch_hash} with verified epoch record {new_epoch_hash}",
                        );
                    } else {
                        info!(
                            target: "epoch-manager",
                            "retrieved cert for epoch {}/{new_epoch_hash} from a peer", epoch_rec.epoch
                        );
                    }
                    save_and_persist_with_logs(&db, new_epoch_rec, cert).await;
                    return true;
                } else {
                    warn!(
                        target: "epoch-manager",
                        "rejected an unanchored epoch record for epoch {} received from a peer during recovery",
                        epoch_rec.epoch,
                    );
                }
            }
            Err(err) => error!(
                target: "epoch-manager",
                "failed to retrieve epoch from a peer {epoch_hash}: {err}",
            ),
        }
    }
    false
}

/// Collect and manage votes for a specific epoch record.
///
/// Runs vote rounds until a certificate for the epoch exists: a round publishes this node's
/// vote (when it is a record-committee member), collects peer votes, and aggregates on quorum.
/// A round that exhausts [`MAX_EPOCH_VOTE_TIMEOUTS`] wait windows without quorum does NOT
/// abandon the epoch — it runs one peer-recovery pass, checks the store for a certificate
/// obtained from any other source, then backs off ([`EPOCH_VOTE_ROUND_BACKOFF_START`] doubling
/// to [`EPOCH_VOTE_ROUND_BACKOFF_CAP`]), republishes its vote, and starts the next round with
/// all accumulated signatures intact — votes arriving minutes apart still aggregate. A single
/// bounded round certified nothing when a staggered fleet restart spread votes across more
/// than one round's span, and a dead round made late votes unrecoverable (the collector drops
/// votes for epochs whose queue entry has no live task).
///
/// Exits: quorum reached (aggregate and store), certificate found in the store, a validated
/// record+cert fetched from a peer, quorum on a DIFFERENT record digest (equivocation path:
/// fall through to peer recovery for the certified alternative), or vote channel closed.
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
        // The aggregate must hold exactly one signature per bitmap bit, so a
        // signature is only ever pushed when its authority index is newly
        // inserted — a redelivered vote can then never desync the two.
        if let Some(idx) = committee_index.get(&me) {
            if signed_authorities.insert(*idx as u32) {
                sigs.push(epoch_vote.signature);
            }
        }
        info!(
            target: "epoch-manager",
            "publishing epoch record {epoch_hash}",
        );
        let _ = primary_network.publish_epoch_vote(epoch_vote).await;
        my_vote = Some(epoch_vote);
    }
    // Collect votes from peers
    let mut reached_quorum = false;
    let mut timeout = EPOCH_VOTE_RECV_TIMEOUT;
    let mut timeouts = 0;
    let mut round: u64 = 1;
    let mut round_backoff = EPOCH_VOTE_ROUND_BACKOFF_START;
    let mut alt_recs: HashMap<EpochDigest, usize> = HashMap::default();
    let committee_size = epoch_rec.committee.len() as u64;
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
                                if let Some(idx) = committee_index.get(&source) {
                                    if signed_authorities.insert(*idx as u32) {
                                        sigs.push(vote.signature);
                                    }
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
                            // Track votes for alternative epoch records — remove key so
                            // a validator can only vote once (correct or alt), no equivocation.
                            if committee_keys.remove(&vote.public_key) {
                                let count = alt_recs.entry(vote.epoch_hash).or_default();
                                *count += 1;
                                if *count >= quorum {
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
                        // Timeout with quorum: the post-quorum straggler window ended.
                        if reached_quorum {
                            break;
                        }
                        timeouts += 1;
                        if timeouts > MAX_EPOCH_VOTE_TIMEOUTS {
                            // Round exhausted without quorum. Recover instead of abandoning the
                            // epoch: a dead vote task makes late votes unrecoverable, and an
                            // uncertified record blocks the fork-active entry into the next
                            // epoch on every node.
                            if try_fetch_certified_record(
                                &epoch_rec,
                                &primary_network,
                                &consensus_chain,
                            )
                            .await
                            {
                                return;
                            }
                            // A certificate may have arrived from any other source while this
                            // round ran (peer gossip quorum on another node's round, the
                            // state-sync record collector, an alternative certified record).
                            if let Some((_, Some(_cert))) = consensus_chain
                                .epochs()
                                .get_epoch_by_number(epoch_rec.epoch)
                                .await
                            {
                                info!(
                                    target: "epoch-manager",
                                    "certificate for epoch {} obtained; ending vote rounds",
                                    epoch_rec.epoch,
                                );
                                return;
                            }
                            warn!(
                                target: "epoch-manager",
                                "epoch vote round {round} for {}/{epoch_hash} ended without \
                                 certificate; retrying in {round_backoff:?}",
                                epoch_rec.epoch,
                            );
                            tokio::time::sleep(round_backoff).await;
                            round_backoff =
                                (round_backoff.saturating_mul(2)).min(EPOCH_VOTE_ROUND_BACKOFF_CAP);
                            round += 1;
                            // Fresh round: same accumulated signatures, reset window budget.
                            timeouts = 0;
                        }
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
        // e2e-only hook (compiled out of production builds): drop the assembled certificate so
        // the harness can manufacture the records-without-certificates fleet state a
        // whole-committee crash at an epoch close leaves behind.
        if tn_types::test_suppress_epoch_certs() {
            warn!(
                target: "epoch-manager",
                "TEST HOOK: TN_TEST_SUPPRESS_EPOCH_CERTS active - discarding quorum certificate \
                 for epoch {}",
                epoch_rec.epoch,
            );
            return;
        }
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
        // Reached only on the alternative-record quorum break (peers certified a different
        // digest — recover their record) or a closed vote channel (shutdown/eviction). The
        // no-quorum timeout path retries rounds inside the loop and never lands here.
        error!(
            target: "epoch-manager",
            "failed to reach quorum on epoch close for {epoch_hash} {epoch_rec:?}",
        );
        if !try_fetch_certified_record(&epoch_rec, &primary_network, &consensus_chain).await {
            error!(
                target: "epoch-manager",
                "Failed to retrieve an epoch record for epoch {}",
                epoch_rec.epoch,
            );
        }
    }
}

/// Return the vote receiver for `epoch` when a (new) vote round should be spawned.
///
/// `None` means a round for the epoch is already LIVE and the caller must not spawn another.
/// Liveness is tested, not assumed: a queue entry whose receiver was taken by a round that has
/// since exited (its receiver dropped, so the stored sender reports closed) is re-created with
/// a fresh channel rather than treated as live. Without that check, a round that ends without
/// storing a certificate — the alternative-record quorum exit, or a certificate save that
/// fails — would leave a dead entry that turns every later re-arm for the epoch
/// (`rearm_epoch_vote_round`) into a silent no-op, exactly the un-re-armable state the re-arm
/// hook exists to eliminate.
fn get_new_vote_channel(epoch: Epoch, vote_queues: &mut VoteQueue) -> Option<Receiver<EpochVote>> {
    for q in vote_queues.iter_mut() {
        if q.0 == epoch {
            if q.2.is_none() && q.1.is_closed() {
                // The previous round for this epoch already ran and exited; give the re-arm a
                // fresh round instead of ignoring it forever.
                let (epoch_vote_tx, epoch_vote_rx) = mpsc::channel(10_000);
                q.1 = epoch_vote_tx;
                return Some(epoch_vote_rx);
            }
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
                    _ = &node_shutdown => return Ok(()),
                    _ = epoch_rx.changed() => {
                        if let Some(rec) = epoch_rx.borrow_and_update().clone() {
                            break rec;
                        }
                    }
                    result = vote_rx.recv() => {
                        match result {
                            None => return Ok(()),  // Channel closed- we are done.
                            Some(vote) => {
                                handle_new_vote(vote, &mut vote_queues).await;
                            }
                        }
                    }
                }
            };

            if let Some(epoch_vote_rx) = get_new_vote_channel(epoch_rec.epoch, &mut vote_queues) {
                let consensus_chain = consensus_chain.clone();
                let primary_network = primary_network.clone();
                let key_config = key_config.clone();
                task_spawner.spawn_task(
                    format!("epoch votes for epoch {}", epoch_rec.epoch),
                    async move {
                        manage_epoch_votes(
                            epoch_rec,
                            key_config,
                            primary_network,
                            epoch_vote_rx,
                            consensus_chain,
                        )
                        .await;
                        Ok(())
                    },
                );
            }
        }
    });
}

#[cfg(test)]
mod epoch_vote_collector_tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
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

    /// A redelivered copy of the node's own vote (e.g. a gossip echo) must not add a
    /// duplicate signature to the aggregate. The vote loop's committee set must keep the
    /// node's own key removed after self-signing; if the key resurfaces, the echoed
    /// self-vote pushes a second copy of the node's signature into `sigs` while the
    /// authority bitmap stays deduplicated — the aggregate then no longer matches the
    /// bitmap's public keys, certificate verification fails, and no cert is stored.
    #[tokio::test]
    async fn test_redelivered_self_vote_does_not_corrupt_aggregate() {
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
            TempDir::with_prefix("test_redelivered_self_vote_does_not_corrupt_aggregate").unwrap();
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

        // The node's own vote, as gossip would echo it back
        let echoed_self_vote = epoch_rec.sign_vote(&key_config);

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.subscribe(),
        );

        // Buffer the echoed self-vote plus two peer votes: kp1 (self-sign) + kp2 + kp3 = quorum 3
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(echoed_self_vote).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();

        // Send the epoch record — collector wakes up, self-signs, reads buffered votes
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // After reaching quorum the collector waits up to 1s for more votes before aggregating
        tokio::time::sleep(Duration::from_millis(3000)).await;

        // The cert must exist and verify: a corrupted aggregate (duplicate self-signature)
        // fails verify_with_cert and is never stored
        let cert = consensus_chain.epochs().cert_by_digest(epoch_hash).await.expect("cert missing");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");
        // Exactly the three distinct signers (committee indices 0..=2) are in the bitmap
        assert_eq!(cert.signed_authorities.len(), 3, "self vote must be counted exactly once");
        assert!(cert.signed_authorities.contains(0));
        assert!(cert.signed_authorities.contains(1));
        assert!(cert.signed_authorities.contains(2));

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

    /// a vote from a key outside the record committee (ejected mid-epoch) is ignored
    /// entirely — it neither counts toward quorum nor blocks later member votes.
    #[tokio::test]
    async fn test_collector_rejects_ejected_nonmember_vote() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let ejected = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        // Node is kp1, a member of the post-ejection committee
        let key_config = KeyConfig::new_with_testing_key(kp1);

        // Post-ejection record: committee of 4, super_quorum = (4*2)/3 + 1 = 3.
        // The ejected key is absent from the committee.
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
            TempDir::with_prefix("test_collector_rejects_ejected_nonmember_vote").unwrap();
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

        // The ejected key signs a valid vote for the correct record — must not be counted
        let kc_ejected = KeyConfig::new_with_testing_key(ejected);
        let ejected_vote = epoch_rec.sign_vote(&kc_ejected);
        consensus_bus.app().new_epoch_votes().send(ejected_vote).await.unwrap();

        // One member vote: with the self-sign that is only 2 of 3 required
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();

        // Send the epoch record — collector wakes up, self-signs, reads buffered votes
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // kp1 (self) + kp2 = 2 < 3: the ejected vote must not tip this over quorum
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(
            consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none(),
            "non-member vote must not count toward quorum"
        );

        // The third member vote completes quorum
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();

        // After reaching quorum the collector waits up to 1s for more votes before aggregating
        tokio::time::sleep(Duration::from_millis(3000)).await;

        let cert = consensus_chain.epochs().cert_by_digest(epoch_hash).await.expect("cert missing");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");
        // Exactly the three members signed (committee indices 0..=2); the ejected key has
        // no index in the record committee and is absent from the bitmap
        assert_eq!(cert.signed_authorities.len(), 3, "only member votes may be counted");
        assert!(cert.signed_authorities.contains(0));
        assert!(cert.signed_authorities.contains(1));
        assert!(cert.signed_authorities.contains(2));

        // Shutdown
        node_shutdown.notify();
    }

    /// a node whose own key is not in the record committee (ejected mid-epoch, demoted
    /// to observer next epoch) must not self-sign, but still stores the cert formed by the
    /// remaining members' votes so it can keep following the chain.
    #[tokio::test]
    async fn test_ejected_node_does_not_self_sign_but_stores_cert() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let ejected = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        // Node is the ejected validator: NOT in the record committee
        let key_config = KeyConfig::new_with_testing_key(ejected);

        // Post-ejection record: committee of 4, super_quorum = (4*2)/3 + 1 = 3
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
            TempDir::with_prefix("test_ejected_node_does_not_self_sign_but_stores_cert").unwrap();
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

        // Three member votes form quorum without any self-sign from the ejected node
        let kc1 = KeyConfig::new_with_testing_key(kp1);
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc1)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();

        // Send the epoch record — the self-sign gate must skip the non-member node key
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // Quorum (3 of 4) reached without a fourth vote: collector waits its 1s straggler
        // window before aggregating
        tokio::time::sleep(Duration::from_millis(3000)).await;

        let cert = consensus_chain.epochs().cert_by_digest(epoch_hash).await.expect("cert missing");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");
        // Only the three voting members are in the bitmap — no self-signature was added
        assert_eq!(cert.signed_authorities.len(), 3, "ejected node must not self-sign");
        assert!(cert.signed_authorities.contains(0));
        assert!(cert.signed_authorities.contains(1));
        assert!(cert.signed_authorities.contains(2));

        // Shutdown
        node_shutdown.notify();
    }

    /// Votes that arrive only after the first full vote round has exhausted its windows must
    /// still certify: the round retries with backoff instead of abandoning the epoch, and
    /// signatures accumulated in earlier rounds survive into later ones. Paused time drives
    /// the ~62s round plus backoff instantly. (The single-round give-up was the incident
    /// shape: a staggered fleet restart spread votes across more than one round's span and
    /// the dead round dropped them forever.)
    #[tokio::test(start_paused = true)]
    async fn test_late_votes_after_round_exhaustion_still_certify() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        // Node is kp1; committee of 4 → super_quorum = 3
        let key_config = KeyConfig::new_with_testing_key(kp1);
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
            TempDir::with_prefix("test_late_votes_after_round_exhaustion_still_certify").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();

        // Mock network: reply Ok to Publish; every other command's reply channel drops,
        // so the round-end peer-recovery fetch fails fast (no serving peers — the parked
        // fleet shape).
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

        // Arm the round with NO peer votes: it must exhaust round 1 (25 wait windows) and
        // enter the retry path rather than giving up.
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // Advance well past one full round (25 × 2.5s = 62.5s) plus the first 5s backoff.
        tokio::time::sleep(Duration::from_secs(90)).await;
        assert!(
            consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none(),
            "no cert may exist before peer votes arrive"
        );

        // Late votes land in a later round; with the accumulated self-signature they complete
        // the 3-of-4 quorum.
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();

        // Poll for the cert; paused time advances through the straggler window instantly.
        let mut cert = None;
        for _ in 0..600 {
            if let Some(found) = consensus_chain.epochs().cert_by_digest(epoch_hash).await {
                cert = Some(found);
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let cert = cert.expect("late votes must certify in a retry round");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");
        assert_eq!(cert.signed_authorities.len(), 3, "self + two late votes");

        node_shutdown.notify();
    }

    /// A certificate that appears in the store from another source (peer fetch by the
    /// state-sync collector, an earlier round on another task) ends the vote rounds: the
    /// round-exhaustion path checks the store and exits instead of retrying forever. Verified
    /// by the publish count going quiet after round 1 while virtual time keeps advancing.
    #[tokio::test(start_paused = true)]
    async fn test_prestored_cert_ends_vote_rounds() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();

        // Node is kp1; committee of 4 → super_quorum = 3
        let key_config = KeyConfig::new_with_testing_key(kp1.copy());
        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, *kp2.public(), *kp3.public(), *kp4.public()],
            next_committee: vec![pk1, *kp2.public(), *kp3.public(), *kp4.public()],
            ..Default::default()
        };
        let epoch_hash = epoch_rec.digest();

        // Build the genuine 3-of-4 certificate out-of-band and store it BEFORE arming the
        // round — the "certificate arrived from any other source" shape.
        let votes = [&kp1, &kp2, &kp3]
            .map(|kp| epoch_rec.sign_vote(&KeyConfig::new_with_testing_key(kp.copy())));
        let sigs: Vec<BlsSignature> = votes.iter().map(|v| v.signature).collect();
        let aggregated = BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate");
        let mut signed_authorities = roaring::RoaringBitmap::new();
        signed_authorities.insert_range(0..3);
        let cert = EpochCertificate {
            epoch_hash,
            signature: aggregated.to_signature(),
            signed_authorities,
        };
        assert!(epoch_rec.verify_with_cert(&cert), "test cert must verify");

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let temp_dir = TempDir::with_prefix("test_prestored_cert_ends_vote_rounds").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();
        consensus_chain.epochs().save(epoch_rec.clone(), cert).await.expect("save record + cert");

        // Mock network counting Publish commands.
        let publish_count = Arc::new(AtomicUsize::new(0));
        let publish_count_clone = publish_count.clone();
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move {
            while let Some(cmd) = net_rx.recv().await {
                if let NetworkCommand::Publish { reply, .. } = cmd {
                    publish_count_clone.fetch_add(1, Ordering::SeqCst);
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

        // Arm the round with no peer votes: round 1 exhausts, finds the stored cert, exits.
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // Let round 1 fully elapse (62.5s + backoff margin) …
        tokio::time::sleep(Duration::from_secs(200)).await;
        let after_round_one = publish_count.load(Ordering::SeqCst);
        assert!(after_round_one >= 1, "own vote must have been published");

        // … then confirm no further rounds publish anything over ten more virtual minutes.
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(
            publish_count.load(Ordering::SeqCst),
            after_round_one,
            "vote rounds must end once a certificate exists in the store"
        );

        node_shutdown.notify();
    }

    /// A vote round that exits WITHOUT a certificate (here: quorum on an alternative record
    /// digest, whose peer-recovery fetch then fails) must not permanently disarm the epoch:
    /// a later re-arm (`epoch_record_watch` firing again for the same epoch) must spawn a
    /// fresh round that can still certify. Guards the closed-sender re-creation in
    /// `get_new_vote_channel` — without it the dead queue entry swallows every re-arm and the
    /// anchor park can never resolve through votes.
    #[tokio::test]
    async fn test_rearm_after_dead_round_spawns_fresh_round() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let committee = vec![pk1, *kp2.public(), *kp3.public(), *kp4.public()];

        // Node is kp1; committee of 4 → super_quorum = 3.
        let key_config = KeyConfig::new_with_testing_key(kp1);
        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee.clone(),
            ..Default::default()
        };
        let epoch_hash = epoch_rec.digest();
        // Same epoch and committee, different next_committee → different digest.
        let alt_epoch_rec = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee.iter().rev().copied().collect(),
            ..Default::default()
        };
        assert_ne!(alt_epoch_rec.digest(), epoch_hash);

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("test_rearm_after_dead_round").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        // Mock network: Publish succeeds; every other command's reply drops, so the dead
        // round's peer-recovery fetch fails fast (nobody holds a certificate).
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

        // Round 1: three buffered alt-record votes reach the alternative-digest quorum, the
        // round breaks without a certificate, its recovery fetch fails, and the task exits.
        for kp in [&kp2, &kp3, &kp4] {
            let kc = KeyConfig::new_with_testing_key(kp.copy());
            consensus_bus.app().new_epoch_votes().send(alt_epoch_rec.sign_vote(&kc)).await.unwrap();
        }
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none(),
            "alt-quorum round must not certify the correct record"
        );

        // Re-arm the SAME epoch and feed correct votes into the fresh round. Retried because
        // the re-arm only takes effect once the dead round's exit is observable (its sender
        // reports closed); each pass is idempotent.
        let kc2 = KeyConfig::new_with_testing_key(kp2.copy());
        let kc3 = KeyConfig::new_with_testing_key(kp3.copy());
        let mut cert = None;
        for _ in 0..20 {
            consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));
            tokio::time::sleep(Duration::from_millis(300)).await;
            consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
            consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(700)).await;
            if let Some(found) = consensus_chain.epochs().cert_by_digest(epoch_hash).await {
                cert = Some(found);
                break;
            }
        }
        let cert = cert.expect("re-arm after a dead round must spawn a fresh certifying round");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");

        node_shutdown.notify();
    }
}
