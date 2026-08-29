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
    EpochVote, ShutdownNotifier, TaskSpawner, TnReceiver as _, TnSender as _,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    watch,
};
use tracing::{error, info, warn};

type VoteQueue = VecDeque<(Epoch, Sender<EpochVote>)>;

/// Per-iteration wait for the next epoch vote in [`manage_epoch_votes`]'s collection loop.
pub(crate) const EPOCH_VOTE_RECV_TIMEOUT: Duration = Duration::from_millis(2500);

/// Timeout-count threshold in [`manage_epoch_votes`]: the loop gives up on a local vote quorum
/// on the first wait that times out with the counter ABOVE this value. The counter increments
/// once per timed-out wait, so up to `MAX_EPOCH_VOTE_TIMEOUTS + 2` full
/// [`EPOCH_VOTE_RECV_TIMEOUT`] windows of ordinary vote-propagation lag elapse before the
/// peer-recovery path is even entered.
pub(crate) const MAX_EPOCH_VOTE_TIMEOUTS: u32 = 24;

/// Number of peer-recovery attempts [`manage_epoch_votes`] makes (each one
/// `request_epoch_cert` call) after failing to reach a local vote quorum.
pub(crate) const EPOCH_CERT_RECOVERY_ATTEMPTS: u32 = 5;

/// Delay before gossiping our freshly-signed epoch vote, so slower nodes can reach the new epoch
/// and hold its record before our vote arrives (otherwise they drop it and wait for a republish).
/// The local collector is still seeded immediately; only the outbound gossip is staggered.
const INITIAL_VOTE_PUBLISH_DELAY: Duration = Duration::from_millis(500);

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
    node_shutdown: ShutdownNotifier,
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
    // Collect votes from peers
    let mut reached_quorum = false;
    let mut timeout = EPOCH_VOTE_RECV_TIMEOUT;
    let mut timeouts = 0;
    let committee_size = epoch_rec.committee.len() as u64;
    let quorum = epoch_rec.super_quorum();
    let shutdown = node_shutdown.subscribe();
    loop {
        tokio::select! {
            _ = &shutdown => return,
            result = tokio::time::timeout(timeout, vote_rx.recv()) => {
                match result {
                    Ok(None) => break,  // Channel closed- we are done.
                    Ok(Some(vote)) => {
                        if vote.public_key == me {
                            // Record our vote if we see it for this epoch so we can revote if/when needed.
                            my_vote = Some(vote);
                        }
                        if vote.epoch != epoch_rec.epoch {
                            continue;
                        }
                        if vote.public_key == me && vote.epoch_hash == epoch_hash {
                            // Record our vote if we see it for this epoch so we can revote if/when needed.
                            my_vote = Some(vote);
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
                            error!(
                                target: "epoch-manager",
                                "Received an epoch record vote for {} instead of {}. This should not be possible (gossip is filtered).",
                                vote.epoch_hash,
                                epoch_hash,
                            );
                        }
                    }
                    Err(_) => {
                        // Timeout: have quorum or tried long enough
                        if reached_quorum || timeouts > MAX_EPOCH_VOTE_TIMEOUTS {
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
        // Republish our vote one final time.  This is not strictly needed but if we were the first
        // validator to close the epoch and we got no timeouts the laggy validators may have
        // missed our vote- give them one more chance.
        if let Some(vote) = my_vote {
            let _ = primary_network.publish_epoch_vote(vote).await;
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
        // Republish our vote one final time.  This is not strictly needed but if we were the first
        // validator to close the epoch and we got no timeouts the laggy validators may have
        // missed our vote- give them one more chance.
        if let Some(vote) = my_vote {
            let _ = primary_network.publish_epoch_vote(vote).await;
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
        for _ in 0..EPOCH_CERT_RECOVERY_ATTEMPTS {
            if vote_rx.is_closed() {
                // If this channel closed then the sender was dropped and this task needs to exit...
                break;
            }
            match network.request_epoch_cert(Some(epoch_rec.epoch), None).await {
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
                            error!(
                                target: "epoch-manager",
                                "Network came to consensus on epoch record {new_epoch_hash} we expected epoch record {epoch_hash}, we have forked!",
                            );
                            // We generated a different record than the network certified, so we
                            // have forked. A fork is not something a node can safely recover from
                            // (records are deterministic, so ours is simply wrong) — fail-stop
                            // rather than adopt the network's record and pretend to recover.
                            node_shutdown.notify();
                        } else {
                            info!(
                                target: "epoch-manager",
                                "retrieved cert for epoch {}/{new_epoch_hash} from a peer", epoch_rec.epoch
                            );
                            save_and_persist_with_logs(&db, new_epoch_rec, cert).await;
                        }
                        got_epoch_record = true;
                        break;
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
        if !got_epoch_record {
            error!(
                target: "epoch-manager",
                "Failed to retrieve an epoch record for epoch {}- We have a missing epoch certificate, if this is not local then sync will be compromised!",
                epoch_rec.epoch,
            );
        }
    }
}

/// Direct a newly received vote to its task.
async fn handle_new_vote(
    vote: EpochVote,
    vote_queues: &mut VoteQueue,
    consensus_chain: ConsensusChain,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    task_spawner: &TaskSpawner,
    node_shutdown: ShutdownNotifier,
) {
    let mut remove = None;
    let mut found = false;
    for (i, q) in vote_queues.iter().enumerate() {
        if q.0 == vote.epoch {
            // We already have a collector for this epoch so send to it.
            if q.1.send(vote).await.is_err() {
                remove = Some(i);
                break;
            }
            found = true;
            break;
        }
    }
    if let Some(remove) = remove {
        vote_queues.remove(remove);
    }
    if !found {
        // We do not have a collector for this epoch so start one and send it this vote.
        let (epoch_vote_tx, epoch_vote_rx) = mpsc::channel(10_000);
        // If we receive a valid vote and aren't collecting votes to certify then start.
        let Some((epoch_rec, None)) =
            consensus_chain.epochs().get_epoch_by_hash(vote.epoch_hash).await
        else {
            // Missing the record or it is certified.  These were pre-checked when the gossip came
            // in so this should not happen.
            error!(
                target: "epoch-manager",
                "Received a vote for a missing epoch record- this should not happen! {} {}", vote.epoch, vote.epoch_hash
            );
            return;
        };
        // Spawn the vote collector in response to a vote if it was missing.
        // This allows the possibility of recovering a cert with a republished vote even if stale.
        task_spawner.spawn_task(format!("epoch votes for epoch {}", epoch_rec.epoch), async move {
            manage_epoch_votes(
                epoch_rec,
                key_config,
                primary_network,
                epoch_vote_rx,
                consensus_chain,
                node_shutdown,
            )
            .await;
            Ok(())
        });
        if epoch_vote_tx.send(vote).await.is_ok() {
            // In a properly working system only one collector at a time should run.
            // Allow some extras though in case we have to handle epoch certification exceptions in
            // the future.
            if vote_queues.len() >= 5 {
                vote_queues.pop_front();
            }
            vote_queues.push_back((vote.epoch, epoch_vote_tx));
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
    node_shutdown: ShutdownNotifier,
) {
    let mut vote_rx = consensus_bus.subscribe_new_epoch_votes();
    let mut epoch_rx = consensus_bus.epoch_record_watch().subscribe();
    let task_spawner = node_task_spawner.clone();
    let mut vote_queues: VoteQueue = VecDeque::with_capacity(5);
    let shutdown = node_shutdown.subscribe();

    node_task_spawner.spawn_critical_task("Epoch Vote Collector", async move {
        loop {
            // Wait for an EpochRecord to arrive
            let epoch_rec = loop {
                tokio::select! {
                    _ = &shutdown => return Ok(()),
                    _ = epoch_rx.changed() => {
                        if let Some(rec) = epoch_rx.borrow_and_update().clone() {
                            break rec;
                        }
                    }
                    result = vote_rx.recv() => {
                        match result {
                            None => return Ok(()),  // Channel closed- we are done.
                            Some(vote) => {
                                handle_new_vote(vote, &mut vote_queues, consensus_chain.clone(), key_config.clone(),
                                    primary_network.clone(), &task_spawner, node_shutdown.clone()).await;
                            }
                        }
                    }
                }
            };

            let me = key_config.primary_public_key();
            // Failsafe for a previous epoch whose certification failed (e.g. some of its committee
            // were down for an hour or two). It should never fire under normal conditions.
            //
            // Deliberately NOT gated on node mode / `is_active_cvv()`: that reflects the CURRENT
            // committee, but the node we need here was in epoch N-1's committee and may have rotated
            // out of N's (now an `Observer`) — precisely the node that should re-vote to certify N-1.
            // Membership in N-1's committee (checked below) is the only correct gate. This is already
            // sync-safe: state-sync saves each record together with its cert (`epochs().save`) and
            // never fires `epoch_record_watch`, so a syncing node's historic records are certified and
            // this arm is skipped.
            if epoch_rec.epoch > 0 {
                // Previous epoch has no cert; if we were in its committee, re-sign and re-publish our
                // vote to trigger a fresh collection attempt.
                if let Some((last_epoch_rec, None)) = consensus_chain.epochs().get_epoch_by_number(epoch_rec.epoch.saturating_sub(1)).await {
                    // No cert for last epoch.  Were we in the committee?
                    if last_epoch_rec.committee.contains(&me) {
                        // If so then lets send a vote out which will trigger a new collection attempt.
                        let epoch_vote = last_epoch_rec.sign_vote(&key_config);
                        error!(
                            target: "epoch-manager",
                            "Failed to certify last epoch, re-publishing epoch record vote for epoch {} {}", last_epoch_rec.epoch, last_epoch_rec.digest()
                        );
                        // Sending our vote to this channel will trigger us to start the vote collector when we get it.
                        // The other committee members of last epoch should also do the same.
                        // This should not happen and if it does certification may fail again but retry after an epoch anyway.
                        if consensus_bus.new_epoch_votes().send(epoch_vote).await.is_err() {
                            error!(
                                target: "epoch-manager",
                                "Failed to send vote {} for epoch {} on internal bus- this is a critical error", epoch_vote.epoch_hash, epoch_vote.epoch
                            );
                        }
                        let _ = primary_network.publish_epoch_vote(epoch_vote).await;
                    }
                }
            }
            let epoch_hash = epoch_rec.digest();
            // If we already have a cert (for instance we are catching up) then don't vote.
            if consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none() {
                // If we are in the committee and this epoch is un-certified, sign and publish our vote
                if epoch_rec.committee.contains(&me) {
                    let epoch_vote = epoch_rec.sign_vote(&key_config);
                    info!(
                        target: "epoch-manager",
                        "publishing epoch record vote for epoch {} {epoch_hash}", epoch_rec.epoch,
                    );
                    // Sending our vote to this channel will trigger us to start the vote collector when we get it.
                    if consensus_bus.new_epoch_votes().send(epoch_vote).await.is_err() {
                        error!(
                            target: "epoch-manager",
                            "Failed to send vote {} for epoch {} on internal bus- this is a critical error", epoch_vote.epoch_hash, epoch_vote.epoch
                        );
                    }
                    let primary_network_clone = primary_network.clone();
                    task_spawner.spawn_task("publish_epoch_vote_delayed", async move {
                        // Stagger the outbound gossip (see INITIAL_VOTE_PUBLISH_DELAY) so slower
                        // nodes reach the new epoch and hold its record before our vote arrives;
                        // republishes cover any that still miss it.
                        tokio::time::sleep(INITIAL_VOTE_PUBLISH_DELAY).await;
                        let _ = primary_network_clone.publish_epoch_vote(epoch_vote).await;
                        Ok(())
                    });
                }
            }
            // See spawn_epoch_record_collector() for how syncing nodes can keep up their epoch certs if not following the tip.
        }
    });
}

/// Re-arm the epoch-vote round for a stored-but-uncertified latest epoch record.
///
/// The vote collector is armed by a vote for an uncertified epoch record, and only the epoch-close
/// paths write that channel. The channel is in-memory. A node that persists its epoch record and
/// then shuts down before a vote quorum aggregates never writes the channel again: after a
/// restart no vote is re-signed, so a fleet that holds records without certificates cannot
/// self-heal (issue #1198). This startup hook closes that gap.
///
/// The re-publish is idempotent and bounded. Votes re-sign deterministically (BLS over the
/// record's digest), the vote handler verifies and deduplicates them, certificate writes are
/// append-once per digest, and [`manage_epoch_votes`] bounds the round (vote windows, then
/// peer fetch) even when no quorum can form. A node outside the record's committee signs
/// nothing; its round only listens for votes and then tries the peer fetch.
///
/// Epoch 0 is re-armed like any other epoch. The uncertifiable epoch-0 dummy record is not
/// observable here: `run_epochs` seeds it in-memory AFTER this hook runs and
/// `save_dummy_epoch0` never persists it, so a stored epoch-0 record at startup is a genuine
/// closed-epoch-0 record.
///
/// Deliberate scope (the issue defers the rest to the network refactor): this heals the
/// latest stored record or previous record only, once per process start. A historical gap behind an
/// already certified later record needs the refactor's certificate backfill, and a round that
/// expires before enough peers are back up is retried only by the next restart.
///
/// Call this AFTER [`spawn_epoch_vote_collector`]: the collector subscribes to the watch
/// inside the spawn call, and a subscription created after a send treats that value as
/// already seen.
pub(crate) async fn revote_uncertified_epoch_record_on_startup(
    db: &EpochRecordDb,
    epoch_record_watch: &watch::Sender<Option<EpochRecord>>,
) {
    if let Some(record) = db.latest_record().await {
        let epoch_hash = record.digest();
        if db.cert_by_digest(epoch_hash).await.is_none() {
            info!(
                target: "epoch-manager",
                epoch = record.epoch,
                "re-arming epoch vote round for stored-but-uncertified epoch record {epoch_hash}",
            );
            epoch_record_watch.send_replace(Some(record));
        }
    }
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
    use tn_network_libp2p::types::{MessageId, NetworkCommand, NetworkResponseMessage};
    use tn_primary::{
        network::{PrimaryRequest, PrimaryResponse},
        ConsensusBus, NodeMode,
    };
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::wait_until;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{BlsKeypair, TaskManager};

    /// #1198 startup re-vote: a persisted-but-uncertified latest record re-fires the watch.
    #[tokio::test]
    async fn test_startup_revote_fires_for_uncertified_record() {
        let temp_dir = TempDir::with_prefix("startup_revote_uncertified").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        let rec0 = EpochRecord::default();
        let rec1 = EpochRecord { epoch: 1, ..Default::default() };
        db.save_record(rec0).await.unwrap();
        db.save_record(rec1.clone()).await.unwrap();
        db.persist().await.unwrap();

        let (watch_tx, watch_rx) = watch::channel(None);
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx).await;

        let armed = watch_rx.borrow().as_ref().map(|rec| rec.digest());
        assert_eq!(armed, Some(rec1.digest()), "uncertified latest record must re-arm the watch");
    }

    /// #1198 startup re-vote: a latest record whose certificate is already stored stays quiet.
    #[tokio::test]
    async fn test_startup_revote_skips_certified_record() {
        let temp_dir = TempDir::with_prefix("startup_revote_certified").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        let rec0 = EpochRecord::default();
        let rec1 = EpochRecord { epoch: 1, ..Default::default() };
        let mut rng = StdRng::from_os_rng();
        let key_config = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rng));
        let vote = rec1.sign_vote(&key_config);
        let cert = EpochCertificate {
            epoch_hash: rec1.digest(),
            signature: vote.signature,
            signed_authorities: roaring::RoaringBitmap::new(),
        };
        db.save_record(rec0).await.unwrap();
        db.save(rec1.clone(), cert).await.unwrap();
        db.persist().await.unwrap();

        let (watch_tx, watch_rx) = watch::channel(None);
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx).await;

        assert!(watch_rx.borrow().is_none(), "certified latest record must not re-arm the watch");
    }

    /// #1198 startup re-vote: a genuine closed-epoch-0 record without a certificate re-arms
    /// the watch like any other epoch. The in-memory dummy record cannot be observed at
    /// startup (it is seeded after this hook runs and never persisted), so no epoch-number
    /// skip applies at the very first boundary.
    #[tokio::test]
    async fn test_startup_revote_fires_for_uncertified_epoch_zero_record() {
        let temp_dir = TempDir::with_prefix("startup_revote_epoch_zero").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::from_os_rng();
        let pk = *BlsKeypair::generate(&mut rng).public();
        let rec0 = EpochRecord {
            epoch: 0,
            committee: vec![pk],
            next_committee: vec![pk],
            ..Default::default()
        };
        db.save_record(rec0.clone()).await.unwrap();
        db.persist().await.unwrap();

        let (watch_tx, watch_rx) = watch::channel(None);
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx).await;

        let armed = watch_rx.borrow().as_ref().map(|rec| rec.digest());
        assert_eq!(armed, Some(rec0.digest()), "uncertified epoch-0 record must re-arm the watch");
    }

    /// #1198 startup re-vote: an empty store stays quiet.
    #[tokio::test]
    async fn test_startup_revote_skips_empty_store() {
        let temp_dir = TempDir::with_prefix("startup_revote_empty").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();

        let (watch_tx, watch_rx) = watch::channel(None);
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx).await;

        assert!(watch_rx.borrow().is_none(), "empty store must not re-arm the watch");
    }

    /// Reply `Ok` to every `Publish` command until the channel closes.
    fn spawn_publish_ack(
        mut net_rx: tokio::sync::mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
    ) {
        tokio::spawn(async move {
            if let Some(cmd) = net_rx.recv().await {
                if let NetworkCommand::Publish { reply, .. } = cmd {
                    let _ = reply.send(Ok(MessageId::new(b"test")));
                }
                spawn_publish_ack(net_rx);
            }
        });
    }

    /// #1198 end to end: the startup re-vote arms the collector for a record restored from
    /// durable storage, the node re-signs, peer votes aggregate, and the certificate lands in
    /// the store: the exact self-heal a restarted fleet needs after a failed vote quorum.
    #[tokio::test]
    async fn test_startup_revote_reaches_quorum_and_stores_cert() {
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

        // Committee of 4: super_quorum = (4*2)/3 + 1 = 3.
        let epoch_rec = EpochRecord {
            epoch: 1,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };
        let epoch_hash = epoch_rec.digest();

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let temp_dir =
            TempDir::with_prefix("test_startup_revote_reaches_quorum_and_stores_cert").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();

        // The restart precondition: records persisted by the previous process, no certificate.
        consensus_chain.epochs().save_record(EpochRecord::default()).await.unwrap();
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();

        // Mock network: reply to Publish commands
        let (net_tx, net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        spawn_publish_ack(net_rx);

        let task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::new();

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
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

        // The startup hook, not a manual watch write, arms the collector from storage.
        revote_uncertified_epoch_record_on_startup(
            consensus_chain.epochs(),
            consensus_bus.app().epoch_record_watch(),
        )
        .await;

        // Wait for collector to aggregate and store
        wait_until(Duration::from_secs(5), "epoch certificate stored", || async {
            Ok(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_some())
        })
        .await
        .unwrap();

        // Verify cert is in DB
        let cert = consensus_chain.epochs().cert_by_digest(epoch_hash).await.expect("cert missing");
        assert_eq!(cert.epoch_hash, epoch_hash);
        assert!(epoch_rec.verify_with_cert(&cert), "cert should verify against epoch record");

        // Shutdown
        node_shutdown.notify();
    }

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
        let node_shutdown = ShutdownNotifier::new();

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
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
        // Seed the uncertified record so the vote-triggered collector's `get_epoch_by_hash`
        // finds it — production persists the record before firing the watch (see
        // `write_epoch_record`), so mirror that here.
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();
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
        let node_shutdown = ShutdownNotifier::new();

        // The node's own vote, as gossip would echo it back
        let echoed_self_vote = epoch_rec.sign_vote(&key_config);

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
        );

        // Buffer the echoed self-vote plus two peer votes: kp1 (self-sign) + kp2 + kp3 = quorum 3
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(echoed_self_vote).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();

        // Send the epoch record — collector wakes up, self-signs, reads buffered votes
        // Seed the uncertified record so the vote-triggered collector's `get_epoch_by_hash`
        // finds it — production persists the record before firing the watch (see
        // `write_epoch_record`), so mirror that here.
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();
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
        let node_shutdown = ShutdownNotifier::new();

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
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
        // Seed the uncertified record so the vote-triggered collector's `get_epoch_by_hash`
        // finds it — production persists the record before firing the watch (see
        // `write_epoch_record`), so mirror that here.
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();
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
        let node_shutdown = ShutdownNotifier::new();

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
        );

        // The ejected key signs a valid vote for the correct record — must not be counted
        let kc_ejected = KeyConfig::new_with_testing_key(ejected);
        let ejected_vote = epoch_rec.sign_vote(&kc_ejected);
        consensus_bus.app().new_epoch_votes().send(ejected_vote).await.unwrap();

        // One member vote: with the self-sign that is only 2 of 3 required
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();

        // Send the epoch record — collector wakes up, self-signs, reads buffered votes
        // Seed the uncertified record so the vote-triggered collector's `get_epoch_by_hash`
        // finds it — production persists the record before firing the watch (see
        // `write_epoch_record`), so mirror that here.
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();
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
        let node_shutdown = ShutdownNotifier::new();

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
        );

        // Three member votes form quorum without any self-sign from the ejected node
        let kc1 = KeyConfig::new_with_testing_key(kp1);
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc1)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();

        // Send the epoch record — the self-sign gate must skip the non-member node key
        // Seed the uncertified record so the vote-triggered collector's `get_epoch_by_hash`
        // finds it — production persists the record before firing the watch (see
        // `write_epoch_record`), so mirror that here.
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();
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

    /// The previous-epoch certification failsafe is gated on PREVIOUS-committee membership, NOT on
    /// node mode. A node that was in epoch N-1's committee but has rotated out of N's committee
    /// runs as an `Observer` (`is_active_cvv() == false`), yet it must still re-vote to help
    /// certify an uncertified N-1. Here kp1 is in epoch 0's committee but not epoch 1's; epoch
    /// 0 needs kp1's failsafe vote to reach quorum, so it certifies only because the failsafe
    /// is not mode-gated.
    #[tokio::test]
    async fn test_previous_epoch_recovery_not_gated_by_node_mode() {
        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let pk1 = *kp1.public();
        let pk2 = *kp2.public();
        let pk3 = *kp3.public();
        let pk4 = *kp4.public();

        // Node is kp1: in epoch 0's committee (super_quorum 3) but rotated OUT of epoch 1's.
        let key_config = KeyConfig::new_with_testing_key(kp1);

        // Previous epoch (0): uncertified, kp1 in committee — the failsafe target.
        let prev_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk2, pk3, pk4],
            ..Default::default()
        };
        let prev_hash = prev_rec.digest();
        // Current epoch (1): kp1 is NOT in this committee (rotated out).
        let cur_rec = EpochRecord {
            epoch: 1,
            committee: vec![pk2, pk3, pk4],
            next_committee: vec![pk2, pk3, pk4],
            ..Default::default()
        };

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("prev_epoch_recovery_not_gated").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        // Seed both records uncertified so the collector's `get_epoch_by_*` lookups find them.
        consensus_chain.epochs().save_record(prev_rec.clone()).await.unwrap();
        consensus_chain.epochs().save_record(cur_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();

        // Mock network: drain and ack publishes.
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
        let node_shutdown = ShutdownNotifier::new();

        // kp1 rotated out of epoch 1's committee, so it runs as an Observer — is_active_cvv() is
        // false. The failsafe must still fire because kp1 was in epoch 0's committee.
        consensus_bus.app().node_mode().send_replace(NodeMode::Observer);

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
        );

        // Two epoch-0 peers vote — one short of quorum(3). Only kp1's failsafe vote can complete
        // it.
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(prev_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(prev_rec.sign_vote(&kc3)).await.unwrap();

        // Epoch 1 arriving triggers the previous-epoch failsafe for epoch 0.
        consensus_bus.app().epoch_record_watch().send_replace(Some(cur_rec.clone()));

        // Despite the Observer mode, the failsafe re-votes for epoch 0 and completes quorum.
        wait_until(Duration::from_secs(5), "epoch 0 certified via failsafe", || async {
            Ok(consensus_chain.epochs().cert_by_digest(prev_hash).await.is_some())
        })
        .await
        .unwrap();

        node_shutdown.notify();
    }

    /// Build a super-quorum certificate over `record`, signed by `signers` whose committee
    /// indices are `0..signers.len()`. Mirrors the aggregation in [`manage_epoch_votes`] and the
    /// cert construction in `crates/types` epoch tests.
    fn make_cert(record: &EpochRecord, signers: &[&KeyConfig]) -> EpochCertificate {
        let sigs: Vec<_> = signers.iter().map(|kc| record.sign_vote(*kc).signature).collect();
        let signature = BlsAggregateSignature::aggregate(&sigs[..], true).unwrap().to_signature();
        let mut signed_authorities = roaring::RoaringBitmap::new();
        for i in 0..signers.len() as u32 {
            signed_authorities.push(i);
        }
        EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities }
    }

    /// Fail-stop on fork: after a failed local vote quorum, the peer-recovery download returns a
    /// record that validates against our locally-trusted anchor but whose digest differs from the
    /// one we computed. That is an unrecoverable fork (records are deterministic), so the collector
    /// must call `node_shutdown.notify()` rather than silently adopt the peer's record.
    ///
    /// Paused clock: `manage_epoch_votes` reaches the recovery path only after
    /// `MAX_EPOCH_VOTE_TIMEOUTS` vote windows of `EPOCH_VOTE_RECV_TIMEOUT` each (~60s of real
    /// time), and the recovery loop bails if the vote channel is closed, so the sender is held
    /// open and the windows are fast-forwarded.
    #[tokio::test(start_paused = true)]
    async fn test_fork_digest_mismatch_triggers_shutdown() {
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

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("fork_digest_mismatch").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        // Epoch-0 record: its `next_committee` is the locally-trusted anchor for epoch 1.
        let rec0 = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };
        consensus_chain.epochs().save_record(rec0.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();

        // Our local epoch-1 record (the one we tried and failed to certify).
        let local_rec = EpochRecord {
            epoch: 1,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        let local_hash = local_rec.digest();

        // The peer's certified record for the same epoch: same anchor and committee, but a
        // different `next_committee` (a stand-in for any content divergence) so its digest
        // differs, plus a real super-quorum certificate over it.
        let alt_rec = EpochRecord {
            epoch: 1,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk2, pk3, pk4],
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        assert_ne!(alt_rec.digest(), local_hash, "alt record must differ from ours");
        let alt_cert = make_cert(&alt_rec, &[&kc1, &kc2, &kc3]);

        // Mock network: answer the recovery `SendRequestAny` with the alt record + cert.
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let alt_rec = alt_rec.clone();
            tokio::spawn(async move {
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::SendRequestAny { reply, .. } = cmd {
                        let _ = reply.send(Ok(NetworkResponseMessage {
                            peer: pk1,
                            result: PrimaryResponse::EpochRecord {
                                record: alt_rec.clone(),
                                certificate: alt_cert.clone(),
                            },
                        }));
                    }
                }
            });
        }

        // Hold the vote sender open so the recovery loop does not bail at `is_closed()`; send no
        // votes, so the round times out into recovery.
        let (_vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);

        let node_shutdown = ShutdownNotifier::new();
        let noticer = node_shutdown.subscribe();

        // Drive the collector directly: the vote windows auto-advance under the paused clock, the
        // recovery download returns the mismatched record, and the fork is detected. Returns once
        // shutdown is signalled.
        manage_epoch_votes(
            local_rec,
            kc1,
            primary_network,
            vote_rx,
            consensus_chain,
            node_shutdown,
        )
        .await;

        assert!(noticer.noticed(), "digest mismatch must trigger node shutdown");
    }

    /// Dead-collector respawn: a queued collector whose receiver was dropped makes the forward
    /// `send` fail, so `handle_new_vote` must remove the dead entry and spawn a fresh collector for
    /// the epoch (re-queued with a live sender).
    #[tokio::test]
    async fn test_handle_new_vote_respawns_dead_collector() {
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

        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("respawn_dead_collector").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        // The record must be present and uncertified so the respawn's lookup finds it.
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();

        // Drain-only mock network.
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move { while net_rx.recv().await.is_some() {} });

        let task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner();
        let node_shutdown = ShutdownNotifier::new();

        // Seed the queue with a DEAD collector entry: receiver dropped, so a forward send fails.
        let (dead_tx, dead_rx) = mpsc::channel::<EpochVote>(1);
        drop(dead_rx);
        let mut vote_queues: VoteQueue = VecDeque::new();
        vote_queues.push_back((epoch_rec.epoch, dead_tx));

        let vote = epoch_rec.sign_vote(&kc1);
        handle_new_vote(
            vote,
            &mut vote_queues,
            consensus_chain,
            kc1,
            primary_network,
            &spawner,
            node_shutdown,
        )
        .await;

        assert_eq!(vote_queues.len(), 1, "dead entry replaced, not duplicated");
        assert_eq!(vote_queues[0].0, epoch_rec.epoch);
        assert!(
            !vote_queues[0].1.is_closed(),
            "replacement sender must be live (its collector holds the receiver)",
        );
    }

    /// `handle_new_vote` must drop a vote whose record is unknown or already certified — spawning
    /// no collector — because both are pre-filtered upstream and should never reach a fresh
    /// collector.
    #[tokio::test]
    async fn test_handle_new_vote_drops_certified_and_unknown() {
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

        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("drop_certified_unknown").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        // Drain-only mock network.
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move { while net_rx.recv().await.is_some() {} });

        let task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner();
        let node_shutdown = ShutdownNotifier::new();

        // `EpochVote` is `Copy`, so the same vote drives both cases.
        let vote = epoch_rec.sign_vote(&kc1);

        // Case 1: unknown record (nothing saved) → dropped, no collector.
        let mut vote_queues: VoteQueue = VecDeque::new();
        handle_new_vote(
            vote,
            &mut vote_queues,
            consensus_chain.clone(),
            kc1.clone(),
            primary_network.clone(),
            &spawner,
            node_shutdown.clone(),
        )
        .await;
        assert!(vote_queues.is_empty(), "vote for an unknown record must be dropped");

        // Case 2: certified record → dropped, no collector.
        consensus_chain
            .epochs()
            .save(epoch_rec.clone(), make_cert(&epoch_rec, &[&kc1, &kc2, &kc3]))
            .await
            .unwrap();
        consensus_chain.epochs().persist().await.unwrap();
        let mut vote_queues: VoteQueue = VecDeque::new();
        handle_new_vote(
            vote,
            &mut vote_queues,
            consensus_chain,
            kc1,
            primary_network,
            &spawner,
            node_shutdown,
        )
        .await;
        assert!(vote_queues.is_empty(), "vote for a certified record must be dropped");
    }

    /// A committee member arming the collector publishes its vote to the network exactly twice:
    /// once after the 500ms initial-gossip delay, and once as the post-quorum final republish.
    #[tokio::test]
    async fn test_publishes_delayed_initial_and_at_quorum() {
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

        // Committee of 4: super_quorum = 3 (kp1 self-sign + kp2 + kp3).
        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };
        let epoch_hash = epoch_rec.digest();

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("publishes_delayed_and_quorum").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();

        // Counting mock network: tally Publish commands.
        let publishes = Arc::new(AtomicUsize::new(0));
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let publishes = publishes.clone();
            tokio::spawn(async move {
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::Publish { reply, .. } = cmd {
                        publishes.fetch_add(1, Ordering::SeqCst);
                        let _ = reply.send(Ok(MessageId::new(b"test")));
                    }
                }
            });
        }

        let task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::new();

        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
        );

        // Two peer votes buffered: with kp1's self-sign that is exactly quorum (3 of 4).
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc2)).await.unwrap();
        consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(&kc3)).await.unwrap();
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

        // The cert lands from quorum, and exactly two publishes reach the network: the delayed
        // initial gossip and the post-quorum final republish (no pre-quorum timeout republish
        // fires because all three votes are buffered and consumed within one window).
        wait_until(Duration::from_secs(3), "cert stored and two publishes", || async {
            Ok(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_some()
                && publishes.load(Ordering::SeqCst) == 2)
        })
        .await
        .unwrap();

        // Give any spurious extra publish a moment to appear, then pin the count.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(publishes.load(Ordering::SeqCst), 2, "exactly two publishes reach the network");

        node_shutdown.notify();
    }

    /// `my_vote` capture + timeout republish: with our own vote captured from the stream and quorum
    /// unreachable (1 of 4), each vote-window timeout republishes our vote to the network.
    ///
    /// Paused clock fast-forwards the vote windows. `manage_epoch_votes` is driven directly (not
    /// via the collector), so the timeout republish is the only publisher — any publish proves the
    /// captured `my_vote` was resent.
    #[tokio::test(start_paused = true)]
    async fn test_my_vote_republished_on_timeout() {
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

        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("my_vote_republish").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        // Counting mock network (only Publish commands appear on this path).
        let publishes = Arc::new(AtomicUsize::new(0));
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let publishes = publishes.clone();
            tokio::spawn(async move {
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::Publish { reply, .. } = cmd {
                        publishes.fetch_add(1, Ordering::SeqCst);
                        let _ = reply.send(Ok(MessageId::new(b"test")));
                    }
                }
            });
        }

        // Seed only our own vote: `my_vote` is captured, but 1 of 4 never reaches quorum (3). Hold
        // the sender open so the round times out (rather than seeing a closed channel).
        let (vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        vote_tx.send(epoch_rec.sign_vote(&kc1)).await.unwrap();

        let node_shutdown = ShutdownNotifier::new();

        manage_epoch_votes(
            epoch_rec,
            kc1,
            primary_network,
            vote_rx,
            consensus_chain,
            node_shutdown,
        )
        .await;

        assert!(
            publishes.load(Ordering::SeqCst) >= 1,
            "my_vote must be republished on a vote-window timeout",
        );
    }
}
