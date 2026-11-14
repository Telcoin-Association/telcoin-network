//! Manage epoch record voting and recording at epoch end.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    sync::Arc,
    time::Duration,
};

use eyre::WrapErr as _;
use parking_lot::Mutex;
use tn_config::KeyConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, epoch_records::EpochRecordDb};
use tn_types::{
    AuthorityIdentifier, BlsAggregateSignature, BlsPublicKey, BlsSignature, Epoch,
    EpochCertificate, EpochDigest, EpochRecord, EpochVote, ShutdownNotifier, TaskError,
    TaskSpawner, TnReceiver as _, TnSender as _,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    watch,
};
use tracing::{debug, error, info, warn};

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

/// Backoff between peer-recovery attempts in [`manage_epoch_votes`]. Without it, a node with no
/// connected peers gets an immediate `NoPeers` answer and burns all
/// [`EPOCH_CERT_RECOVERY_ATTEMPTS`] in microseconds; spacing them lets the ladder span real time so
/// peers can (re)appear. Cancelled promptly by node shutdown.
const EPOCH_CERT_RECOVERY_BACKOFF: Duration = Duration::from_secs(2);

/// Delay before gossiping our freshly-signed epoch vote, so slower nodes can reach the new epoch
/// and hold its record before our vote arrives (otherwise they drop it and wait for a republish).
/// The local collector is still seeded immediately; only the outbound gossip is staggered.
const INITIAL_VOTE_PUBLISH_DELAY: Duration = Duration::from_millis(500);

/// How many epochs back from a closing epoch the switch `epoch_record_recertify` reaches: the
/// close of epoch `N` re-votes one older uncertified record of epochs `N - 16 ..= N - 2`, and
/// [`RetainedEpochVotes`] keeps signatures for epochs `N - 16 ..= N`.
pub(crate) const MAX_RECERTIFY_LOOKBACK: Epoch = 16;

/// The most records [`RetainedEpochVotes`] keeps signatures for: one per epoch of its window.
const MAX_RETAINED_RECORDS: usize = MAX_RECERTIFY_LOOKBACK as usize + 1;

/// Save and persist a peer's verified epoch record and cert. A refused save is logged; our own
/// store failing to save or persist is returned (see `EpochDbError::stop_on_local_failure`),
/// so the round ends with an error that stops the node (see [`spawn_epoch_vote_collector`]).
async fn save_and_persist_with_logs(
    db: &EpochRecordDb,
    epoch_record: EpochRecord,
    cert: EpochCertificate,
) -> eyre::Result<()> {
    let epoch = epoch_record.epoch;
    if let Err(e) = db.save(epoch_record, cert).await {
        return e.stop_on_local_failure(format!(
            "failed to save epoch record/cert after retrieval for epoch {epoch}"
        ));
    }
    // A persist writes only our own files, so any failure is local.
    db.persist().await.wrap_err_with(|| {
        format!("failed to persist epoch record/cert after retrieval for epoch {epoch}")
    })
}

/// Collect and manage votes for a specific epoch record.
///
/// With `retained` (the switch `epoch_record_recertify` on), the round starts from the signatures
/// earlier rounds for the same record counted and adds each vote it counts to them; once the
/// record is certified the kept signatures for its epoch are dropped. Without it the round starts
/// empty and keeps nothing.
///
/// Returns `Err` only when our own epoch store fails to save or persist the certificate (see
/// `EpochDbError::stop_on_local_failure`); every other way a round ends is `Ok`.
async fn manage_epoch_votes(
    epoch_rec: EpochRecord,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    mut vote_rx: Receiver<EpochVote>,
    consensus_chain: ConsensusChain,
    node_shutdown: ShutdownNotifier,
    retained: Option<RetainedEpochVotes>,
) -> eyre::Result<()> {
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
    // Set when our vote channel closes (Ok(None)): this collector was evicted from `vote_queues`
    // (capacity pop_front dropped our sender) or the node is tearing down. Either way the round is
    // over — skip peer recovery and its alarm.
    let mut channel_closed = false;
    let mut timeout = EPOCH_VOTE_RECV_TIMEOUT;
    let mut timeouts = 0;
    let committee_size = epoch_rec.committee.len() as u64;
    let quorum = epoch_rec.super_quorum();
    // set once a certificate for this record is stored or found stored
    let mut certified = false;
    if let Some(retained) = &retained {
        // the kept signatures were counted for this same record (same digest, so same committee),
        // under the committee indices this round assigns
        for (index, signature) in retained.signatures(epoch_rec.epoch, epoch_hash) {
            let Some(key) = epoch_rec.committee.get(index as usize) else { continue };
            if committee_keys.remove(key) && signed_authorities.insert(index) {
                sigs.push(signature);
            }
        }
        if signed_authorities.len() >= quorum as u64 {
            reached_quorum = true;
            timeout = Duration::from_secs(1);
        }
    }
    let shutdown = node_shutdown.subscribe();
    loop {
        tokio::select! {
            _ = &shutdown => return Ok(()),
            result = tokio::time::timeout(timeout, vote_rx.recv()) => {
                match result {
                    Ok(None) => {
                        // Channel closed: evicted from `vote_queues` or the node is shutting down.
                        channel_closed = true;
                        break;
                    }
                    Ok(Some(vote)) => {
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
                                        if let Some(retained) = &retained {
                                            retained.keep(
                                                epoch_rec.epoch,
                                                epoch_hash,
                                                *idx as u32,
                                                vote.signature,
                                            );
                                        }
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

    // signers when the round ended; members still in `committee_keys` had not voted by then
    let signers = signed_authorities.len();
    let missing = missing_signers(&epoch_rec.committee, &committee_keys);
    // the round for an older record this node picked to re-certify leaves the signer gauge to
    // the rounds of the latest record and the failsafe
    let recertify_round = retained.as_ref().is_some_and(|r| r.is_recertifying(epoch_rec.epoch));

    // Aggregate signatures and save the cert
    if reached_quorum {
        info!(
            target: "epoch-manager",
            epoch = epoch_rec.epoch,
            signers,
            committee = committee_size,
            timeouts,
            missing = %SignerList(&missing),
            %epoch_hash,
            "epoch record quorum"
        );
        if recertify_round {
            record_missing_signers(&epoch_rec.committee, &committee_keys);
        } else {
            record_epoch_record_quorum(&epoch_rec.committee, &committee_keys, signers);
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
                        e.stop_on_local_failure(format!(
                            "failed to save epoch cert after reaching quorum {epoch}"
                        ))?;
                    } else {
                        certified = true;
                    }
                    // A persist writes only our own files, so any failure is local.
                    consensus_chain.epochs().persist().await.wrap_err_with(|| {
                        format!("failed to persist epoch cert after reaching quorum {epoch}")
                    })?;
                } else {
                    error!(
                        target: "epoch-manager",
                        "failed to verify epoch record and cert for {epoch_hash}",
                    );
                    // a kept signature that does not verify would fail every later round too
                    if let Some(retained) = &retained {
                        retained.forget_record(epoch_rec.epoch, epoch_hash);
                    }
                }
            }
            Err(_) => {
                error!(
                    target: "epoch-manager",
                    "failed to aggregate epoch record signatures for {epoch_hash}",
                );
                if let Some(retained) = &retained {
                    retained.forget_record(epoch_rec.epoch, epoch_hash);
                }
            }
        }
        // Republish our vote one final time.  This is not strictly needed but if we were the first
        // validator to close the epoch and we got no timeouts the laggy validators may have
        // missed our vote- give them one more chance.
        if let Some(vote) = my_vote {
            let _ = primary_network.publish_epoch_vote(vote).await;
        }
    } else if channel_closed {
        // Evicted from `vote_queues` (capacity pop_front dropped our sender) or the node is
        // shutting down: the round is over. No peer recovery and no alarm — this is not a
        // certification failure, and another collector (or a restart) owns the epoch now.
        debug!(
            target: "epoch-manager",
            "epoch vote round for {epoch_hash} ended before quorum (collector evicted or shutting down)",
        );
    } else {
        // The record can be certified without this round: the record collector or an earlier
        // round may have stored a certificate for the same digest while this round waited. Then
        // the round's own failure is not a certification failure. The count is touched by 0 on
        // the other branch so its series exists before the first such round.
        let held = consensus_chain.epochs().cert_by_digest(epoch_hash).await;
        let already_certified = held.is_some();
        certified = already_certified;
        metrics::counter!("tn_node.epoch_record_round_ended_certified_total")
            .increment(u64::from(already_certified));
        if let Some(cert) = held {
            info!(
                target: "epoch-manager",
                epoch = epoch_rec.epoch,
                digest = %epoch_hash,
                signers,
                cert_signers = cert.signed_authorities.len(),
                timeouts,
                missing = %SignerList(&missing),
                "epoch vote round ended without a local quorum; the record is already certified",
            );
            // the epoch is certified: report the held certificate's signers, not this round's
            if !recertify_round {
                record_epoch_record_signers(cert.signed_authorities.len());
            }
        } else {
            error!(
                target: "epoch-manager",
                signers,
                timeouts,
                missing = %SignerList(&missing),
                "failed to reach quorum on epoch close for {epoch_hash} {epoch_rec:?}",
            );
            if !recertify_round {
                record_epoch_record_signers(signers);
            }
        }
        let db = consensus_chain.epochs().clone();
        let network = primary_network.clone();
        // Try to recover by downloading the epoch record and cert from a peer. This needs only the
        // network and db; it is bounded by EPOCH_CERT_RECOVERY_ATTEMPTS, spaced by
        // EPOCH_CERT_RECOVERY_BACKOFF, and cancelled promptly by node shutdown.
        let mut got_epoch_record = false;
        let mut shutting_down = false;
        for attempt in 0..EPOCH_CERT_RECOVERY_ATTEMPTS {
            if attempt > 0 {
                // Space attempts so the ladder spans real time even when peers are absent
                // (request_epoch_cert returns immediately with NoPeers); cancellable by shutdown.
                tokio::select! {
                    _ = &shutdown => {
                        shutting_down = true;
                        break;
                    }
                    _ = tokio::time::sleep(EPOCH_CERT_RECOVERY_BACKOFF) => {}
                }
            }
            tokio::select! {
                // Shutdown must cancel recovery promptly, even mid-request: request_epoch_cert
                // blocks on sequential peer-request timeouts that a between-attempts check cannot.
                _ = &shutdown => {
                    shutting_down = true;
                    break;
                }
                res = network.request_epoch_cert(Some(epoch_rec.epoch), None) => {
                    match res {
                        Ok((new_epoch_rec, cert, peer)) => {
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
                                        %peer,
                                        signers = cert.signed_authorities.len(),
                                        "retrieved cert for epoch {}/{new_epoch_hash} from a peer", epoch_rec.epoch
                                    );
                                    save_and_persist_with_logs(&db, new_epoch_rec, cert).await?;
                                    certified = true;
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
            }
        }
        // Alarm only on a genuine fetch failure: not while shutting down, not if we were evicted
        // mid-recovery (is_closed() here gates the log only, never the fetch above), and not when
        // the certificate is already held.
        if !got_epoch_record && !shutting_down && !vote_rx.is_closed() && !already_certified {
            error!(
                target: "epoch-manager",
                "Failed to retrieve an epoch record for epoch {}- We have a missing epoch certificate, if this is not local then sync will be compromised!",
                epoch_rec.epoch,
            );
        }
    }
    if let Some(retained) = &retained {
        if certified {
            retained.forget_epoch(epoch_rec.epoch);
        } else {
            // votes forwarded after the vote loop ended (during peer recovery) were not counted,
            // and the handler drops their repeats for the rest of its epoch: keep them
            while let Ok(vote) = vote_rx.try_recv() {
                if vote.epoch != epoch_rec.epoch || vote.epoch_hash != epoch_hash {
                    continue;
                }
                if let Some(idx) = committee_index.get(&vote.public_key) {
                    retained.keep(epoch_rec.epoch, epoch_hash, *idx as u32, vote.signature);
                }
            }
        }
        if retained.take_recertify(epoch_rec.epoch) {
            let result = if certified { "certified" } else { "short" };
            metrics::counter!("tn_node.epoch_record_recertify_rounds_total", "result" => result)
                .increment(1);
        }
    }
    Ok(())
}

/// Direct a newly received vote to its task.
///
/// `retained` is handed to a round this starts (see [`manage_epoch_votes`]); it is `Some` only
/// with the switch `epoch_record_recertify` on. With it, a full `vote_queues` first drops the
/// senders of rounds that ended and then evicts the round of the oldest epoch, instead of the
/// front entry, so the up to three rounds a close starts do not evict the newest record's round.
#[allow(clippy::too_many_arguments)]
async fn handle_new_vote(
    vote: EpochVote,
    vote_queues: &mut VoteQueue,
    consensus_chain: ConsensusChain,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    task_spawner: &TaskSpawner,
    node_shutdown: ShutdownNotifier,
    store_failed: &Sender<TaskError>,
    retained: Option<RetainedEpochVotes>,
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
        let epoch_rec = match consensus_chain.epochs().get_epoch_by_hash(vote.epoch_hash).await {
            Some((epoch_rec, None)) => epoch_rec,
            Some((_, Some(_))) => {
                info!(
                    target: "epoch-manager",
                    "Received a vote for an already certified epoch record {} {}- ignoring.", vote.epoch, vote.epoch_hash
                );
                return;
            }
            None => {
                // Missing the record.  These were pre-checked when the gossip came
                // in so this should not happen.
                error!(
                    target: "epoch-manager",
                    "Received a vote for a missing epoch record- this should not happen! {} {}", vote.epoch, vote.epoch_hash
                );
                return;
            }
        };
        // Spawn the vote collector in response to a vote if it was missing.
        // This allows the possibility of recovering a cert with a republished vote even if stale.
        // A round is not critical (it ends once its epoch is certified or given up), so its one
        // error, our own store failing, goes to the critical collector, which stops the node.
        let store_failed = store_failed.clone();
        let recertify = retained.is_some();
        task_spawner.spawn_task(format!("epoch votes for epoch {}", epoch_rec.epoch), async move {
            if let Err(e) = manage_epoch_votes(
                epoch_rec,
                key_config,
                primary_network,
                epoch_vote_rx,
                consensus_chain,
                node_shutdown,
                retained,
            )
            .await
            {
                let _ = store_failed.send(TaskError::from_message(format!("{e:#}"))).await;
            }
            Ok(())
        });
        if epoch_vote_tx.send(vote).await.is_ok() {
            // In a properly working system only one collector at a time should run.
            // Allow some extras though in case we have to handle epoch certification exceptions in
            // the future.
            if recertify {
                // a close starts up to three rounds of its own: drop the entries of rounds that
                // already ended, then evict the oldest epoch's round, never the newest record's
                vote_queues.retain(|(_, tx)| !tx.is_closed());
                if vote_queues.len() >= 5 {
                    let oldest = vote_queues
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, (epoch, _))| *epoch)
                        .map(|(position, _)| position);
                    if let Some(oldest) = oldest {
                        vote_queues.remove(oldest);
                    }
                }
            } else if vote_queues.len() >= 5 {
                vote_queues.pop_front();
            }
            vote_queues.push_back((vote.epoch, epoch_vote_tx));
        }
    }
}

/// Send an epoch vote to the new_epoch_votes channel and log an error.
/// Note, this is not async since blocking at the callsite will most likely
/// wedge an epoch vote collector.
fn new_epoch_vote(consensus_bus: &ConsensusBusApp, epoch_vote: EpochVote) {
    if let Err(e) = consensus_bus.new_epoch_votes().try_send(epoch_vote) {
        error!(
            target: "epoch-manager",
            "Failed to send vote {} for epoch {} on internal bus: {e}", epoch_vote.epoch_hash, epoch_vote.epoch
        );
    }
}

/// Spawn a node-lifetime task to collect epoch vote signatures.
///
/// This actor subscribes once to the `new_epoch_votes` channel and never drops the receiver,
/// eliminating the gap at epoch boundaries where votes could be lost. It watches for new
/// `EpochRecord`s via a `watch` channel and collects votes for each epoch.
///
/// A round that finds our own epoch store failing (see `EpochDbError::stop_on_local_failure`)
/// reports it here, and this critical task returns the error, so the node exits for operator
/// intervention.
///
/// `recertify` is the switch `epoch_record_recertify`. On, every record that arrives also prunes
/// the [`RetainedEpochVotes`] every round shares and re-votes one older uncertified record of
/// this node's own (see [`recertify_older_record`]), so a close starts at most three rounds of
/// its own (this record, the previous one, the older one). Off, rounds keep no signatures and
/// only the previous record is re-voted, as before the switch existed.
pub(crate) fn spawn_epoch_vote_collector(
    consensus_chain: ConsensusChain,
    consensus_bus: ConsensusBusApp,
    key_config: KeyConfig,
    primary_network: PrimaryNetworkHandle,
    node_task_spawner: TaskSpawner,
    node_shutdown: ShutdownNotifier,
    recertify: bool,
) {
    let mut vote_rx = consensus_bus.subscribe_new_epoch_votes();
    let mut epoch_rx = consensus_bus.epoch_record_watch().subscribe();
    let task_spawner = node_task_spawner.clone();
    let mut vote_queues: VoteQueue = VecDeque::with_capacity(5);
    let retained = recertify.then(RetainedEpochVotes::default);
    let shutdown = node_shutdown.subscribe();
    // The first failure is enough to stop the node.
    let (store_failed_tx, mut store_failed_rx) = mpsc::channel(1);

    node_task_spawner.spawn_critical_task("Epoch Vote Collector", async move {
        loop {
            // Wait for an EpochRecord to arrive
            let epoch_rec = loop {
                tokio::select! {
                    _ = &shutdown => return Ok(()),
                    Some(err) = store_failed_rx.recv() => return Err(err),
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
                                    primary_network.clone(), &task_spawner, node_shutdown.clone(), &store_failed_tx,
                                    retained.clone()).await;
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
                    if last_epoch_rec.committee.contains(&me)
                        && !withhold_own_epoch_vote(last_epoch_rec.epoch)
                    {
                        // If so then lets send a vote out which will trigger a new collection attempt.
                        let epoch_vote = last_epoch_rec.sign_vote(&key_config);
                        error!(
                            target: "epoch-manager",
                            "Failed to certify last epoch, re-publishing epoch record vote for epoch {} {}", last_epoch_rec.epoch, last_epoch_rec.digest()
                        );
                        // Sending our vote to this channel will trigger us to start the vote collector when we get it.
                        // The other committee members of last epoch should also do the same.
                        // This should not happen and if it does certification may fail again but retry after an epoch anyway.
                        new_epoch_vote(&consensus_bus, epoch_vote);
                        let _ = primary_network.publish_epoch_vote(epoch_vote).await;
                    }
                }
            }
            if let Some(retained) = &retained {
                // prune first, so this close's rounds start against the new window
                retained.prune(consensus_chain.epochs(), epoch_rec.epoch).await;
                recertify_older_record(
                    consensus_chain.epochs(),
                    &consensus_bus,
                    &key_config,
                    &primary_network,
                    retained,
                    epoch_rec.epoch,
                )
                .await;
            }
            let epoch_hash = epoch_rec.digest();
            // If we already have a cert (for instance we are catching up) then don't vote.
            if consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none() {
                // If we are in the committee and this epoch is un-certified, sign and publish our vote
                if epoch_rec.committee.contains(&me) && !withhold_own_epoch_vote(epoch_rec.epoch) {
                    let epoch_vote = epoch_rec.sign_vote(&key_config);
                    info!(
                        target: "epoch-manager",
                        "publishing epoch record vote for epoch {} {epoch_hash}", epoch_rec.epoch,
                    );
                    // Sending our vote to this channel will trigger us to start the vote collector when we get it.
                    new_epoch_vote(&consensus_bus, epoch_vote);
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
/// Normally a fresh `EpochRecord` reaches the collector over the in-memory `epoch_record_watch`
/// (written by the epoch-close path), and that is what makes the collector re-sign and publish
/// its vote. A node that persists its epoch record and then shuts down before the round
/// certifies loses that signal: on restart the watch holds nothing, no vote is re-signed, and a
/// fleet holding records without certificates cannot self-heal (issue #1198). This startup hook
/// closes that gap by writing the stored-but-uncertified record back to `epoch_record_watch`,
/// re-arming the collector exactly as an epoch close would.
///
/// The re-publish is idempotent and bounded. Votes re-sign deterministically (BLS over the
/// record's digest), the vote handler verifies and deduplicates them, certificate writes are
/// append-once per digest, and [`manage_epoch_votes`] bounds the round (vote windows, then
/// peer fetch) even when no quorum can form. A node outside the record's committee signs
/// nothing; its round only listens for votes and then tries the peer fetch.
///
/// Epoch 0 is re-armed like any other epoch. The uncertifiable epoch-0 dummy record is not
/// observable here: `run` seeds it in-memory AFTER this hook and before the startup sync gate, and
/// `save_dummy_epoch0` never persists it, so a stored epoch-0 record at startup is a genuine
/// closed-epoch-0 record.
///
/// Deliberate scope (the issue defers the rest to the network refactor): this inspects only the
/// latest stored record and re-arms it when it is uncertified; the collector's failsafe then also
/// retries the record before it (N-1) as a side effect of that arming, when N-1 is likewise
/// uncertified. So a gap behind an already-certified latest record — the common "N certified,
/// N-1 uncertified" shape — is NOT healed here (nothing writes the watch, so the failsafe never
/// fires); that needs the refactor's certificate backfill. A round that expires before enough
/// peers are back up is retried only by the next restart.
///
/// With `recertify` (the switch `epoch_record_recertify`) that shape is healed too: a certified
/// latest record is written to the watch when a record of the [`MAX_RECERTIFY_LOOKBACK`] epochs
/// before it is stored uncertified, so the collector runs the failsafe and the re-certify pick
/// for it as an epoch close would. The collector does not vote again for the certified record.
///
/// Call this AFTER [`spawn_epoch_vote_collector`]: the collector subscribes to the watch
/// inside the spawn call, and a subscription created after a send treats that value as
/// already seen.
pub(crate) async fn revote_uncertified_epoch_record_on_startup(
    db: &EpochRecordDb,
    epoch_record_watch: &watch::Sender<Option<EpochRecord>>,
    recertify: bool,
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
        } else if recertify && holds_uncertified_before(db, record.epoch).await {
            info!(
                target: "epoch-manager",
                epoch = record.epoch,
                "re-arming epoch vote rounds to re-certify older epoch records",
            );
            epoch_record_watch.send_replace(Some(record));
        }
    }
}

/// True when a record of epochs `epoch - MAX_RECERTIFY_LOOKBACK ..= epoch - 1` is stored without
/// its certificate.
async fn holds_uncertified_before(db: &EpochRecordDb, epoch: Epoch) -> bool {
    let Some(newest) = epoch.checked_sub(1) else { return false };
    for older in epoch.saturating_sub(MAX_RECERTIFY_LOOKBACK)..=newest {
        if matches!(db.get_epoch_by_number(older).await, Some((_, None))) {
            return true;
        }
    }
    false
}

/// The older uncertified record of this node's own the close of `epoch` re-votes, or `None`.
///
/// The candidates are the records of epochs `epoch - MAX_RECERTIFY_LOOKBACK ..= epoch - 2` this
/// node holds without a certificate, in epoch order; `epoch - 1` is the failsafe's. The pick is
/// candidate `epoch % count`, so successive closes walk the candidates round-robin, and nodes
/// holding the same uncertified records pick the same one at the same close, which lets their
/// re-votes meet in one round. Committee membership is not a filter here, so that a node outside
/// one candidate's committee does not shift the pick of the others.
async fn pick_recertify_record(db: &EpochRecordDb, epoch: Epoch) -> Option<EpochRecord> {
    let newest = epoch.checked_sub(2)?;
    let mut candidates = Vec::new();
    for older in epoch.saturating_sub(MAX_RECERTIFY_LOOKBACK)..=newest {
        if let Some((record, None)) = db.get_epoch_by_number(older).await {
            candidates.push(record);
        }
    }
    if candidates.is_empty() {
        return None;
    }
    let pick = epoch as usize % candidates.len();
    Some(candidates.swap_remove(pick))
}

/// Re-vote one older uncertified record of this node's own at the close of `epoch` (switch
/// `epoch_record_recertify`).
///
/// The record is [`pick_recertify_record`]'s. This node signs only when its key is in that
/// record's committee; the vote is the same deterministic BLS vote as the record's first round,
/// sent to the local collector and gossiped, as the failsafe does for the previous record. The
/// round it joins or starts reports its outcome in `tn_node.epoch_record_recertify_rounds_total`
/// (`certified` or `short`) and leaves `tn_node.epoch_record_signers` alone; a pick this node may
/// not sign counts as `skipped`. The outcome is credited to the next round for the picked epoch to
/// end, which can be a round already open for it or one evicted from the queue (`short`), and a
/// pick whose round never starts is not counted.
async fn recertify_older_record(
    db: &EpochRecordDb,
    consensus_bus: &ConsensusBusApp,
    key_config: &KeyConfig,
    primary_network: &PrimaryNetworkHandle,
    retained: &RetainedEpochVotes,
    epoch: Epoch,
) {
    let Some(record) = pick_recertify_record(db, epoch).await else { return };
    if !record.committee.contains(&key_config.primary_public_key()) {
        metrics::counter!("tn_node.epoch_record_recertify_rounds_total", "result" => "skipped")
            .increment(1);
        return;
    }
    let epoch_hash = record.digest();
    info!(
        target: "epoch-manager",
        epoch = record.epoch,
        digest = %epoch_hash,
        retained = retained.signatures(record.epoch, epoch_hash).len(),
        needed = record.super_quorum(),
        "epoch record re-certify round",
    );
    retained.mark_recertify(record.epoch);
    if withhold_own_epoch_vote(record.epoch) {
        return;
    }
    let epoch_vote = record.sign_vote(key_config);
    new_epoch_vote(consensus_bus, epoch_vote);
    let _ = primary_network.publish_epoch_vote(epoch_vote).await;
}

/// Verified epoch-vote signatures kept across vote rounds (switch `epoch_record_recertify`).
///
/// Signatures are kept per record, keyed by its epoch and digest, and within a record by the
/// signer's index in that record's committee: the index [`manage_epoch_votes`] sets in the
/// certificate's `signed_authorities`. A round for a record starts from the signatures earlier
/// rounds for the same record counted, so votes that reached this node in rounds that never
/// overlapped add up to a quorum. A signature is kept only once a round counted it, that is a
/// vote for this node's own record of that epoch from a member of the record's committee, whose
/// signature the primary's vote handler verified (or this node's own vote). The certificate a
/// round builds from kept signatures is the one a single round with the same votes builds: the
/// aggregate is a sum and the signer set is a bitmap of committee indices.
///
/// Bounds: signatures of at most [`MAX_RETAINED_RECORDS`] records, at most one per committee
/// member each. An epoch's signatures are dropped when a round stores or finds its certificate,
/// when the collector's prune at the next record finds its certificate stored by any other path
/// (the record collector), and when the epoch falls out of the window. A record's signatures are
/// also dropped when an aggregate built from them does not verify. The lock is a `parking_lot`
/// mutex, never held across an `.await`.
#[derive(Clone, Debug, Default)]
pub(crate) struct RetainedEpochVotes {
    /// The kept signatures and the window they are kept for.
    inner: Arc<Mutex<RetainedVotes>>,
}

impl RetainedEpochVotes {
    /// The signatures kept for the record `(epoch, digest)`, by committee index.
    fn signatures(&self, epoch: Epoch, digest: EpochDigest) -> Vec<(u32, BlsSignature)> {
        self.inner
            .lock()
            .votes
            .get(&(epoch, digest))
            .map(|kept| kept.iter().map(|(index, signature)| (*index, *signature)).collect())
            .unwrap_or_default()
    }

    /// Keep a signature a round counted for the record `(epoch, digest)`.
    ///
    /// Refused for an epoch below the window. A new record beyond [`MAX_RETAINED_RECORDS`]
    /// replaces the record of the lowest epoch kept, or is refused when its epoch is lower still.
    fn keep(&self, epoch: Epoch, digest: EpochDigest, index: u32, signature: BlsSignature) {
        let mut inner = self.inner.lock();
        if !inner.in_window(epoch) {
            return;
        }
        let key = (epoch, digest);
        if !inner.votes.contains_key(&key) && inner.votes.len() >= MAX_RETAINED_RECORDS {
            match inner.votes.keys().next().copied() {
                Some(lowest) if lowest.0 < epoch => {
                    inner.votes.remove(&lowest);
                }
                _ => return,
            }
        }
        inner.votes.entry(key).or_default().insert(index, signature);
        inner.report();
    }

    /// Drop every signature kept for `epoch`: its certificate is stored.
    fn forget_epoch(&self, epoch: Epoch) {
        let mut inner = self.inner.lock();
        inner.votes.retain(|(kept, _), _| *kept != epoch);
        inner.report();
    }

    /// Drop the signatures kept for the record `(epoch, digest)`: an aggregate built from them
    /// did not verify.
    fn forget_record(&self, epoch: Epoch, digest: EpochDigest) {
        let mut inner = self.inner.lock();
        inner.votes.remove(&(epoch, digest));
        inner.report();
    }

    /// Note that this node sent its re-certify vote for `epoch`; the round for `epoch` that
    /// ends next reports its outcome.
    fn mark_recertify(&self, epoch: Epoch) {
        self.inner.lock().recertifying.insert(epoch);
    }

    /// True while `epoch` is marked by [`Self::mark_recertify`]: a round for it ending now is a
    /// re-certify round.
    fn is_recertifying(&self, epoch: Epoch) -> bool {
        self.inner.lock().recertifying.contains(&epoch)
    }

    /// True, once, when a round for `epoch` ends after [`Self::mark_recertify`].
    fn take_recertify(&self, epoch: Epoch) -> bool {
        self.inner.lock().recertifying.remove(&epoch)
    }

    /// Move the window to end at `latest`, the epoch of the newest record the collector saw, and
    /// drop what is below it, then drop every record whose certificate the store holds.
    async fn prune(&self, db: &EpochRecordDb, latest: Epoch) {
        let kept: Vec<(Epoch, EpochDigest)> = {
            let mut inner = self.inner.lock();
            inner.latest = inner.latest.max(latest);
            let floor = inner.latest.saturating_sub(MAX_RECERTIFY_LOOKBACK);
            inner.votes.retain(|(epoch, _), _| *epoch >= floor);
            inner.recertifying.retain(|epoch| *epoch >= floor);
            inner.report();
            inner.votes.keys().copied().collect()
        };
        // the lock is released across the store reads
        let mut certified = Vec::new();
        for (epoch, digest) in kept {
            if db.cert_by_digest(digest).await.is_some() {
                certified.push(epoch);
            }
        }
        for epoch in certified {
            self.forget_epoch(epoch);
        }
    }
}

/// The state behind [`RetainedEpochVotes`]'s lock.
#[derive(Debug, Default)]
struct RetainedVotes {
    /// Kept signatures per record `(epoch, digest)`, by the signer's committee index.
    votes: BTreeMap<(Epoch, EpochDigest), BTreeMap<u32, BlsSignature>>,
    /// The newest record epoch the collector has pruned at; the window is
    /// `latest - MAX_RECERTIFY_LOOKBACK ..`.
    latest: Epoch,
    /// Epochs this node sent a re-certify vote for whose round has not reported yet.
    recertifying: BTreeSet<Epoch>,
}

impl RetainedVotes {
    /// True when signatures for `epoch` may be kept.
    fn in_window(&self, epoch: Epoch) -> bool {
        epoch >= self.latest.saturating_sub(MAX_RECERTIFY_LOOKBACK)
    }

    /// Set `tn_node.epoch_record_retained_votes` to the number of signatures kept.
    fn report(&self) {
        let held: usize = self.votes.values().map(BTreeMap::len).sum();
        metrics::gauge!("tn_node.epoch_record_retained_votes").set(held as f64);
    }
}

/// Log rendering of a list of authority ids: `[id1,id2]`, or `[]` when empty.
///
/// Comma-separated with no spaces and no quotes, so a `key=value` log parser that reads the value
/// up to the next whitespace captures the whole list. `Debug` of a `Vec<String>` would quote each
/// id and put a space after every comma.
struct SignerList<'a>(&'a [AuthorityIdentifier]);

impl fmt::Display for SignerList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (i, id) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{id}")?;
        }
        f.write_str("]")
    }
}

/// Committee members whose vote was not counted in this round, as authority ids in committee
/// order.
///
/// `remaining` is the set of committee keys that have not voted yet. The walk follows the
/// committee `Vec`, never the set, so the order is the same on every node and every run.
fn missing_signers(
    committee: &[BlsPublicKey],
    remaining: &HashSet<BlsPublicKey>,
) -> Vec<AuthorityIdentifier> {
    committee
        .iter()
        .filter(|key| remaining.contains(*key))
        .map(|key| AuthorityIdentifier::from(*key))
        .collect()
}

/// Set `tn_node.epoch_record_signers` when a vote round ends: to the number of committee members
/// whose vote the round counted, or, for a round that ended without quorum on a record this node
/// already holds certified, to that certificate's signer count.
///
/// A round that fails to reach quorum on a record not yet certified sets it to its own count, so
/// during that failure the gauge shows the failed round's count, not the count of the last round
/// that reached quorum. A round cut short by eviction or shutdown leaves it alone. So does a
/// re-certify round for an older record (switch `epoch_record_recertify`), whose outcome is in
/// `tn_node.epoch_record_recertify_rounds_total`.
fn record_epoch_record_signers(signers: u64) {
    metrics::gauge!("tn_node.epoch_record_signers").set(signers as f64);
}

/// Record a vote round that reached quorum: the signer count, and one missed vote per committee
/// member that did not sign.
///
/// Every member's `signer` series is touched (by 0 when it signed), so the series exists before
/// its first miss and that miss shows up in a Prometheus `increase()`. Runs once per epoch, so the
/// unresolved macro handles are fine here.
fn record_epoch_record_quorum(
    committee: &[BlsPublicKey],
    remaining: &HashSet<BlsPublicKey>,
    signers: u64,
) {
    record_epoch_record_signers(signers);
    record_missing_signers(committee, remaining);
}

/// Count one missed vote in `tn_node.epoch_record_missing_signers_total` per committee member
/// that did not sign a round that reached quorum, touching every member's series (see
/// [`record_epoch_record_quorum`]).
fn record_missing_signers(committee: &[BlsPublicKey], remaining: &HashSet<BlsPublicKey>) {
    for key in committee {
        let signer = AuthorityIdentifier::from(*key).to_string();
        metrics::counter!("tn_node.epoch_record_missing_signers_total", "signer" => signer)
            .increment(u64::from(remaining.contains(key)));
    }
}

/// Whether this node withholds its own vote for the record of `epoch` in the round it is about
/// to sign for (test hook `TN_TEST_WITHHOLD_EPOCH_VOTE`, see
/// [`tn_types::test_hooks::withhold_epoch_vote`]), logging the withheld round.
///
/// Each call counts one of this node's rounds for the record, so every site that signs this
/// node's own vote consults it once, right before signing: the vote for a new record, the
/// failsafe for the previous record, and the re-certify pick. A withheld vote is neither sent to
/// the local collector nor gossiped; a round for the record still starts on the first vote of
/// another member, counts the votes of others, recovers from peers and stores a certificate as
/// usual. Unset, this is `false` and nothing else changes.
fn withhold_own_epoch_vote(epoch: Epoch) -> bool {
    let Some(round) = withhold_epoch_vote_round(epoch) else { return false };
    info!(target: "epoch-manager", epoch, round, "test hook: withholding own epoch vote");
    true
}

/// The withhold hook's answer for `epoch`: the process-wide hook, read from the environment.
#[cfg(not(test))]
fn withhold_epoch_vote_round(epoch: Epoch) -> Option<u32> {
    tn_types::test_hooks::withhold_epoch_vote(epoch)
}

/// The withhold hook's answer for `epoch` in this crate's unit tests: the hook a test installed on
/// its own thread, so no test reads the environment and none can change another's votes.
#[cfg(test)]
fn withhold_epoch_vote_round(epoch: Epoch) -> Option<u32> {
    epoch_vote_collector_tests::withheld_round(epoch)
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
    use tn_types::{encode, BlsKeypair, TaskJoinError, TaskManager};

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
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx, false).await;

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
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx, false).await;

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
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx, false).await;

        let armed = watch_rx.borrow().as_ref().map(|rec| rec.digest());
        assert_eq!(armed, Some(rec0.digest()), "uncertified epoch-0 record must re-arm the watch");
    }

    /// #1198 startup re-vote: an empty store stays quiet.
    #[tokio::test]
    async fn test_startup_revote_skips_empty_store() {
        let temp_dir = TempDir::with_prefix("startup_revote_empty").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();

        let (watch_tx, watch_rx) = watch::channel(None);
        revote_uncertified_epoch_record_on_startup(&db, &watch_tx, false).await;

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
            false,
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
            false,
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
            false,
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
            false,
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
        wait_until(Duration::from_secs(9), "epoch certificate stored", || async {
            Ok(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_some())
        })
        .await
        .unwrap();

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
            false,
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
        wait_until(Duration::from_secs(9), "epoch certificate stored", || async {
            Ok(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_some())
        })
        .await
        .unwrap();

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
            false,
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
        wait_until(Duration::from_secs(9), "epoch certificate stored", || async {
            Ok(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_some())
        })
        .await
        .unwrap();

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
            false,
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
        wait_until(Duration::from_secs(9), "epoch certificate stored", || async {
            Ok(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_some())
        })
        .await
        .unwrap();

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
            false,
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

        // Drive the collector directly: the vote windows auto-advance under the paused clock and
        // the recovery download returns the mismatched record, so the fork is detected and fires
        // `node_shutdown.notify()`. The call then returns because the recovery loop breaks after
        // the download — nothing on that path awaits the notifier.
        manage_epoch_votes(
            local_rec,
            kc1,
            primary_network,
            vote_rx,
            consensus_chain.clone(),
            node_shutdown,
            None,
        )
        .await
        .expect("no local store failure");

        assert!(noticer.noticed(), "digest mismatch must trigger node shutdown");
        // Fail-stop, NOT adopt: the peer's forked record must be neither certified nor stored.
        assert!(
            consensus_chain.epochs().cert_by_digest(alt_rec.digest()).await.is_none(),
            "the peer's forked record must not be certified",
        );
        assert_eq!(
            consensus_chain.epochs().latest_record().await.map(|r| r.epoch),
            Some(0),
            "no newer record may be adopted from the peer (latest stays epoch 0)",
        );
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
        let (store_failed, _store_failed_rx) = mpsc::channel(1);

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
            &store_failed,
            None,
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
        let (store_failed, _store_failed_rx) = mpsc::channel(1);

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
            &store_failed,
            None,
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
            &store_failed,
            None,
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
            false,
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

    /// Our own epoch store failing to save the quorum certificate ends the critical collector
    /// with an error, so the node exits. The store here is closed under the round (a dead actor,
    /// `SendFailed`): nothing a peer sends can cause that.
    #[tokio::test]
    async fn test_failed_epoch_store_stops_the_collector() {
        let mut rng = StdRng::from_os_rng();
        let kps: Vec<BlsKeypair> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let keys: Vec<BlsPublicKey> = kps.iter().map(|kp| *kp.public()).collect();
        let mut kcs: Vec<KeyConfig> =
            kps.into_iter().map(KeyConfig::new_with_testing_key).collect();
        let key_config = kcs.remove(0);

        // Committee of 4: super_quorum = 3, so our vote and two peers' certify it.
        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys,
            ..Default::default()
        };

        let consensus_bus = ConsensusBus::new();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("failed_epoch_store_stops_collector").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        consensus_chain.epochs().save_record(epoch_rec.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();

        // Counting mock network: the first publish is our delayed initial vote gossip.
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

        let mut task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::new();
        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            key_config,
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
            false,
        );

        // Our self-vote starts the round (its record read needs the live store); the initial
        // gossip goes out INITIAL_VOTE_PUBLISH_DELAY later, by when the round is running.
        consensus_bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));
        wait_until(Duration::from_secs(5), "initial vote gossip", || async {
            Ok(publishes.load(Ordering::SeqCst) >= 1)
        })
        .await
        .unwrap();

        // The store fails under the round, then the peer votes bring it to quorum.
        consensus_chain.clone().close().await;
        for kc in &kcs {
            consensus_bus.app().new_epoch_votes().send(epoch_rec.sign_vote(kc)).await.unwrap();
        }

        let ended = tokio::time::timeout(
            Duration::from_secs(5),
            task_manager.until_task_ends(node_shutdown),
        )
        .await
        .expect("the collector must stop on a failed epoch store");
        let Err(TaskJoinError::CriticalExitError(name, err)) = ended else {
            panic!("expected the collector's error, got {ended:?}");
        };
        assert_eq!(name, "Epoch Vote Collector");
        assert!(
            err.to_string().contains("failed to save epoch cert after reaching quorum"),
            "unexpected error: {err}",
        );
    }

    /// `my_vote` capture + timeout republish: our own vote is captured from the stream and, with
    /// quorum unreachable (1 of 4), republished on each vote-window timeout — and it is the
    /// *matching* vote, not a self-signed vote for some other record (the capture is guarded on
    /// `epoch_hash`, `epoch_votes.rs:100`).
    ///
    /// Paused clock fast-forwards the vote windows. `manage_epoch_votes` is driven directly (not
    /// via the collector), so the timeout republish is the only publisher; the mock inspects each
    /// published payload to confirm which vote was resent.
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
        // Same epoch, different content ⇒ different digest. A self-signed vote over this is the
        // decoy: it clears the `:97` epoch check but fails the `epoch_hash` clause of the capture.
        let alt_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk2, pk3, pk4],
            ..Default::default()
        };
        assert_ne!(alt_rec.digest(), epoch_rec.digest());

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("my_vote_republish").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        // Our matching vote (the one that must be republished) and its wire encoding. The gossip
        // payload is `encode(&PrimaryGossip::EpochVote(Box::new(vote)))`; BCS prefixes the enum
        // variant tag and encodes `Box<T>` transparently, so every payload ends with
        // `encode(&vote)`. (`PrimaryGossip` is `pub(super)`, so decoding it here is not possible;
        // this suffix check is the equivalent using only public APIs.)
        let matching = epoch_rec.sign_vote(&kc1);
        let expected = encode(&matching);

        // Counting mock network: classify each Publish payload as the matching vote or something
        // else (the decoy, under an unguarded-capture regression).
        let matching_pubs = Arc::new(AtomicUsize::new(0));
        let other_pubs = Arc::new(AtomicUsize::new(0));
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let matching_pubs = matching_pubs.clone();
            let other_pubs = other_pubs.clone();
            tokio::spawn(async move {
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::Publish { msg, reply, .. } = cmd {
                        if msg.ends_with(&expected) {
                            matching_pubs.fetch_add(1, Ordering::SeqCst);
                        } else {
                            other_pubs.fetch_add(1, Ordering::SeqCst);
                        }
                        let _ = reply.send(Ok(MessageId::new(b"test")));
                    }
                }
            });
        }

        // Seed the matching vote FIRST, then the decoy. `my_vote` is captured but 1 of 4 never
        // reaches quorum (3), so the round times out and republishes it. Order matters: were the
        // capture unguarded, the decoy (arriving second) would overwrite `my_vote` — matching-first
        // is the only order under which the guarded and unguarded code diverge. Hold `vote_tx` open
        // so the round times out rather than seeing a closed channel.
        let (vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        vote_tx.send(matching).await.unwrap();
        vote_tx.send(alt_rec.sign_vote(&kc1)).await.unwrap();

        let node_shutdown = ShutdownNotifier::new();

        manage_epoch_votes(
            epoch_rec,
            kc1,
            primary_network,
            vote_rx,
            consensus_chain,
            node_shutdown,
            None,
        )
        .await
        .expect("no local store failure");

        // Every republish must carry our matching vote; an unguarded capture would republish the
        // decoy (a self-vote for a different record) instead.
        assert!(
            matching_pubs.load(Ordering::SeqCst) >= 1,
            "my_vote (the matching vote) must be republished on a vote-window timeout",
        );
        assert_eq!(
            other_pubs.load(Ordering::SeqCst),
            0,
            "only the matching vote may be republished; a mismatched self-vote must be ignored",
        );
    }

    /// Node shutdown must cancel a stalled peer-recovery instead of hanging inside
    /// `request_epoch_cert`. The mock answers the recovery request by signalling shutdown and then
    /// never replying, so the only way `manage_epoch_votes` returns is the recovery loop's shutdown
    /// arm. Without that arm the held reply blocks forever and the watchdog fires.
    #[tokio::test(start_paused = true)]
    async fn test_shutdown_cancels_recovery() {
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

        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("shutdown_cancels_recovery").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        let node_shutdown = ShutdownNotifier::new();

        // Mock network: on the recovery request, signal shutdown and HOLD the reply (never respond)
        // so `request_epoch_cert` would otherwise block forever.
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let node_shutdown = node_shutdown.clone();
            tokio::spawn(async move {
                // Own the held reply senders so their oneshots stay unresolved.
                let mut held = Vec::new();
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::SendRequestAny { reply, .. } = cmd {
                        node_shutdown.notify();
                        held.push(reply);
                    }
                }
            });
        }

        // Hold the vote sender open so the round reaches recovery via timeout exhaustion.
        let (_vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);

        // With the shutdown arm, `manage_epoch_votes` returns; without it, it blocks on the held
        // reply and the watchdog fires. The watchdog arms only once the recovery request signalled
        // shutdown: before that the round reads the epoch store, whose actor runs on its own
        // thread, and the paused clock would jump to an armed timer while the reply is on its way.
        let noticer = node_shutdown.subscribe();
        let watchdog = async {
            noticer.await;
            tokio::time::sleep(Duration::from_secs(120)).await;
        };
        let returned = tokio::select! {
            result = manage_epoch_votes(
                epoch_rec,
                key_config,
                primary_network,
                vote_rx,
                consensus_chain,
                node_shutdown,
                None,
            ) => result.is_ok(),
            _ = watchdog => false,
        };

        assert!(returned, "shutdown must cancel recovery so manage_epoch_votes returns");
    }

    /// Four deterministic committee keys, in committee order.
    fn committee_of_four() -> Vec<BlsPublicKey> {
        let mut rng = StdRng::seed_from_u64(7);
        (0..4).map(|_| *BlsKeypair::generate(&mut rng).public()).collect()
    }

    /// `missing_signers` walks the committee, not the set: the result is in committee order no
    /// matter how `remaining` was filled, and members that voted are left out.
    #[test]
    fn test_missing_signers_follow_committee_order() {
        let committee = committee_of_four();
        let id = |i: usize| AuthorityIdentifier::from(committee[i]);

        // insert out of committee order
        let remaining: HashSet<BlsPublicKey> = [committee[3], committee[1]].into_iter().collect();
        assert_eq!(missing_signers(&committee, &remaining), vec![id(1), id(3)]);

        let everyone: HashSet<BlsPublicKey> = committee.iter().rev().copied().collect();
        assert_eq!(missing_signers(&committee, &everyone), vec![id(0), id(1), id(2), id(3)]);

        assert!(missing_signers(&committee, &HashSet::new()).is_empty());
    }

    /// The `missing=` field renders as `[id,id]`: brackets, bs58 ids, comma with no space, no
    /// quotes; `[]` when nobody is missing.
    #[test]
    fn test_signer_list_renders_bracketed_ids() {
        assert_eq!(SignerList(&[]).to_string(), "[]");

        let one = AuthorityIdentifier::from_bytes([1; 32]);
        assert_eq!(
            SignerList(std::slice::from_ref(&one)).to_string(),
            "[4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi]"
        );

        let two = AuthorityIdentifier::from_bytes([2; 32]);
        assert_eq!(
            SignerList(&[one, two]).to_string(),
            "[4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi,8qbHbw2BbbTHBW1sbeqakYXVKRQM8Ne7pLK7m6CVfeR]"
        );
    }

    /// A quorum round sets the signer gauge and counts one miss for each member that did not sign;
    /// members that signed get a zero-valued series under the same name and label.
    #[test]
    fn test_epoch_record_quorum_metrics() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let committee = committee_of_four();
        let remaining: HashSet<BlsPublicKey> = [committee[2]].into_iter().collect();

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_epoch_record_quorum(&committee, &remaining, 3);
        });
        let snapshot = snapshotter.snapshot().into_vec();

        let (_, _, _, gauge) = snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == "tn_node.epoch_record_signers")
            .expect("signer gauge registered");
        assert!(matches!(gauge, DebugValue::Gauge(g) if g.0 == 3.0), "got {gauge:?}");

        let mut misses: Vec<(String, u64)> = snapshot
            .iter()
            .filter(|(key, ..)| key.key().name() == "tn_node.epoch_record_missing_signers_total")
            .map(|(key, _, _, value)| {
                let labels: Vec<_> = key.key().labels().collect();
                assert_eq!(labels.len(), 1, "only the signer label");
                assert_eq!(labels[0].key(), "signer");
                let DebugValue::Counter(n) = value else {
                    panic!("counter expected, got {value:?}")
                };
                (labels[0].value().to_string(), *n)
            })
            .collect();
        misses.sort();

        let mut expected: Vec<(String, u64)> = committee
            .iter()
            .map(|key| {
                (AuthorityIdentifier::from(*key).to_string(), u64::from(*key == committee[2]))
            })
            .collect();
        expected.sort();
        assert_eq!(misses, expected);
    }

    /// A round that fails to reach quorum replaces the signer count the last quorum round left in
    /// the gauge, and counts no missed votes.
    ///
    /// `manage_epoch_votes` is driven through the failed-quorum branch as in
    /// `test_shutdown_cancels_recovery`: two of four members vote, short of the quorum of three,
    /// the vote windows time out, and the mock answers the recovery request by signalling
    /// shutdown so the round returns. Only the quorum path and the failure branch set the gauge,
    /// and two votes cannot reach quorum, so the failure branch is the only way the gauge can read
    /// two.
    #[tokio::test(start_paused = true)]
    async fn test_failed_round_sets_signer_gauge() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

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

        let epoch_rec = EpochRecord {
            epoch: 0,
            committee: vec![pk1, pk2, pk3, pk4],
            next_committee: vec![pk1, pk2, pk3, pk4],
            ..Default::default()
        };
        // neither vote is ours, so the timed-out windows republish nothing
        let votes = [
            epoch_rec.sign_vote(&KeyConfig::new_with_testing_key(kp2)),
            epoch_rec.sign_vote(&KeyConfig::new_with_testing_key(kp3)),
        ];

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("failed_round_signer_gauge").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();

        let node_shutdown = ShutdownNotifier::new();

        // Mock network: on the recovery request, signal shutdown and hold the reply, so the
        // recovery loop's shutdown arm ends the round right after the failure branch ran.
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let node_shutdown = node_shutdown.clone();
            tokio::spawn(async move {
                // Own the held reply senders so their oneshots stay unresolved.
                let mut held = Vec::new();
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::SendRequestAny { reply, .. } = cmd {
                        node_shutdown.notify();
                        held.push(reply);
                    }
                }
            });
        }

        // The round runs on this thread (current-thread runtime, awaited below, not spawned), so
        // the local recorder sees its gauge write.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);

        // the last quorum round left three signers and one missed vote
        let remaining: HashSet<BlsPublicKey> = [pk4].into_iter().collect();
        record_epoch_record_quorum(&epoch_rec.committee, &remaining, 3);

        // Hold the vote sender open so the round reaches the failure branch via timeout
        // exhaustion rather than a closed channel.
        let (vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for vote in votes {
            vote_tx.send(vote).await.unwrap();
        }

        // No outer virtual-time timeout: the failed round reads the epoch store, whose actor runs
        // on its own thread, and the paused clock would jump to that timeout while the
        // reply is on its way. A hung round is ended by nextest's slow-timeout.
        manage_epoch_votes(
            epoch_rec,
            key_config,
            primary_network,
            vote_rx,
            consensus_chain,
            node_shutdown,
            None,
        )
        .await
        .expect("no local store failure");

        let snapshot = snapshotter.snapshot().into_vec();

        let (_, _, _, gauge) = snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == "tn_node.epoch_record_signers")
            .expect("signer gauge registered");
        assert!(matches!(gauge, DebugValue::Gauge(g) if g.0 == 2.0), "got {gauge:?}");

        let misses: u64 = snapshot
            .iter()
            .filter(|(key, ..)| key.key().name() == "tn_node.epoch_record_missing_signers_total")
            .map(|(.., value)| match value {
                DebugValue::Counter(n) => *n,
                other => panic!("counter expected, got {other:?}"),
            })
            .sum();
        assert_eq!(misses, 1, "only the quorum round counts a missed vote");
    }

    // ---- bench7 epoch-record-collector-hardening ----

    /// Records every `epoch-manager` event as `LEVEL message key=value ...` on the thread it is
    /// installed on with [`tracing::subscriber::set_default`].
    struct EpochManagerLines(Arc<parking_lot::Mutex<Vec<String>>>);

    /// Collects an event's message and its other fields, in the order the event records them.
    #[derive(Default)]
    struct LineVisitor {
        message: String,
        fields: String,
    }

    impl tracing::field::Visit for LineVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            } else {
                self.fields.push_str(&format!(" {}={value:?}", field.name()));
            }
        }
    }

    impl tracing::Subscriber for EpochManagerLines {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "epoch-manager"
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut visitor = LineVisitor::default();
            event.record(&mut visitor);
            let level = event.metadata().level();
            self.0.lock().push(format!("{level} {}{}", visitor.message, visitor.fields));
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// What [`failed_round`] saw.
    struct FailedRound {
        /// The round's `epoch-manager` lines.
        lines: Vec<String>,
        /// `tn_node.epoch_record_round_ended_certified_total`, when the round touched it.
        ended_certified: Option<u64>,
        /// `tn_node.epoch_record_signers`, when the round set it.
        signers_gauge: Option<f64>,
        /// Our record's digest, as the lines print it.
        digest: String,
        /// The key the mock network answers from.
        peer: BlsPublicKey,
    }

    /// Run a vote round for our epoch-1 record that fails to reach quorum: two of four members
    /// vote, short of the quorum of three, and the vote windows time out under the paused clock.
    ///
    /// With `cert_held`, the store already holds a certificate for our record, as when the record
    /// collector stored one while the round waited. With `serve`, the mock network answers the
    /// recovery request with our record and a three-signer certificate from `peer`; without it,
    /// the mock signals shutdown and holds the reply, so the round ends right after the failure
    /// branch. The round runs on this thread (current-thread runtime, awaited, not spawned), so
    /// the local recorder and the line subscriber see it.
    async fn failed_round(cert_held: bool, serve: bool) -> FailedRound {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let mut rng = StdRng::from_os_rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let kp3 = BlsKeypair::generate(&mut rng);
        let kp4 = BlsKeypair::generate(&mut rng);
        let committee = vec![*kp1.public(), *kp2.public(), *kp3.public(), *kp4.public()];
        let peer = committee[1];
        let kc1 = KeyConfig::new_with_testing_key(kp1);
        let kc2 = KeyConfig::new_with_testing_key(kp2);
        let kc3 = KeyConfig::new_with_testing_key(kp3);

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("failed_round_held_cert").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        // the epoch-0 record anchors epoch 1 for the recovery download
        let rec0 = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee.clone(),
            ..Default::default()
        };
        consensus_chain.epochs().save_record(rec0.clone()).await.unwrap();
        let local_rec = EpochRecord {
            epoch: 1,
            committee: committee.clone(),
            next_committee: committee,
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        let cert = make_cert(&local_rec, &[&kc1, &kc2, &kc3]);
        if cert_held {
            consensus_chain.epochs().save(local_rec.clone(), cert.clone()).await.unwrap();
        }
        consensus_chain.epochs().persist().await.unwrap();

        let node_shutdown = ShutdownNotifier::new();
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let node_shutdown = node_shutdown.clone();
            let record = local_rec.clone();
            tokio::spawn(async move {
                // Own the held reply senders so their oneshots stay unresolved.
                let mut held = Vec::new();
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::SendRequestAny { reply, .. } = cmd {
                        if serve {
                            let _ = reply.send(Ok(NetworkResponseMessage {
                                peer,
                                result: PrimaryResponse::EpochRecord {
                                    record: record.clone(),
                                    certificate: cert.clone(),
                                },
                            }));
                        } else {
                            node_shutdown.notify();
                            held.push(reply);
                        }
                    }
                }
            });
        }

        let lines = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);

        // neither vote is ours, so the timed-out windows republish nothing
        let (vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for key_config in [&kc2, &kc3] {
            vote_tx.send(local_rec.sign_vote(key_config)).await.unwrap();
        }
        let digest = local_rec.digest().to_string();
        // No outer virtual-time timeout: the round reads the epoch store, whose actor runs on its
        // own thread, and the paused clock would jump to that timeout while the reply is on its
        // way. A hung round is ended by nextest's slow-timeout.
        manage_epoch_votes(
            local_rec,
            kc1,
            primary_network,
            vote_rx,
            consensus_chain,
            node_shutdown,
            None,
        )
        .await
        .expect("no local store failure");

        let snapshot = snapshotter.snapshot().into_vec();
        let ended_certified = snapshot.iter().find_map(|(key, .., value)| match value {
            DebugValue::Counter(n)
                if key.key().name() == "tn_node.epoch_record_round_ended_certified_total" =>
            {
                Some(*n)
            }
            _ => None,
        });
        let signers_gauge = snapshot.iter().find_map(|(key, .., value)| match value {
            DebugValue::Gauge(gauge) if key.key().name() == "tn_node.epoch_record_signers" => {
                Some(gauge.0)
            }
            _ => None,
        });
        let lines = lines.lock().clone();
        FailedRound { lines, ended_certified, signers_gauge, digest, peer }
    }

    /// A round that ends without its own quorum while the store already holds the record's
    /// certificate logs one info line and counts the round, and raises no quorum error.
    #[tokio::test(start_paused = true)]
    async fn test_failed_round_with_a_held_cert_logs_info_and_counts() {
        let round = failed_round(true, false).await;
        let info = round
            .lines
            .iter()
            .find(|line| {
                line.starts_with(
                    "INFO epoch vote round ended without a local quorum; the record is already \
                     certified",
                )
            })
            .unwrap_or_else(|| panic!("no already-certified line in {:?}", round.lines));
        for field in [
            " epoch=1".to_string(),
            format!(" digest={}", round.digest),
            " signers=2".to_string(),
            " cert_signers=3".to_string(),
        ] {
            assert!(info.contains(&field), "{field} missing from {info}");
        }
        assert_eq!(round.ended_certified, Some(1));
        assert!(
            !round.lines.iter().any(|line| line.contains("failed to reach quorum on epoch close")),
            "a held certificate must not raise the quorum error: {:?}",
            round.lines,
        );
    }

    /// Without a held certificate the failed round raises the quorum error as before; the
    /// already-certified count is touched by 0 and no already-certified line is logged.
    #[tokio::test(start_paused = true)]
    async fn test_failed_round_without_a_cert_logs_the_quorum_error() {
        let round = failed_round(false, false).await;
        let prefix = format!("ERROR failed to reach quorum on epoch close for {}", round.digest);
        let error = round
            .lines
            .iter()
            .find(|line| line.starts_with(&prefix))
            .unwrap_or_else(|| panic!("no quorum error in {:?}", round.lines));
        assert!(error.contains(" signers=2"), "{error}");
        assert_eq!(round.ended_certified, Some(0));
        assert!(
            !round.lines.iter().any(|line| line.contains("the record is already certified")),
            "{:?}",
            round.lines,
        );
    }

    /// The recovery download's `retrieved cert` line names the peer that served the certificate
    /// and how many signers the certificate carries.
    #[tokio::test(start_paused = true)]
    async fn test_recovered_cert_line_names_the_peer_and_signers() {
        let round = failed_round(false, true).await;
        let prefix = format!("INFO retrieved cert for epoch 1/{} from a peer", round.digest);
        let line = round
            .lines
            .iter()
            .find(|line| line.starts_with(&prefix))
            .unwrap_or_else(|| panic!("no retrieved line in {:?}", round.lines));
        assert!(line.contains(&format!(" peer={}", round.peer)), "{line}");
        assert!(line.contains(" signers=3"), "{line}");
    }

    /// A failed round sets `tn_node.epoch_record_signers` from the held certificate (three
    /// signers) when there is one, and from its own partial count (two votes) when there is not.
    #[tokio::test(start_paused = true)]
    async fn test_failed_round_signer_gauge_follows_a_held_cert() {
        let held = failed_round(true, false).await;
        assert_eq!(held.signers_gauge, Some(3.0), "the held certificate's signers");
        let not_held = failed_round(false, false).await;
        assert_eq!(not_held.signers_gauge, Some(2.0), "the failed round's own count");
    }

    /// Four fresh committee keys, in committee order, with their key configs.
    fn four_keys() -> (Vec<BlsPublicKey>, Vec<KeyConfig>) {
        let mut rng = StdRng::from_os_rng();
        let pairs: Vec<BlsKeypair> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let committee = pairs.iter().map(|pair| *pair.public()).collect();
        (committee, pairs.into_iter().map(KeyConfig::new_with_testing_key).collect())
    }

    /// A store holding the epoch-0 record and, uncertified, the epoch-1 record it anchors, both
    /// with `committee`. Returns the epoch-1 record.
    async fn record_chain(
        name: &str,
        committee: &[BlsPublicKey],
    ) -> (TempDir, ConsensusChain, EpochRecord) {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix(name).unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let rec0 = EpochRecord {
            epoch: 0,
            committee: committee.to_vec(),
            next_committee: committee.to_vec(),
            ..Default::default()
        };
        let rec1 = EpochRecord {
            epoch: 1,
            committee: committee.to_vec(),
            next_committee: committee.to_vec(),
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        consensus_chain.epochs().save_record(rec0).await.unwrap();
        consensus_chain.epochs().save_record(rec1.clone()).await.unwrap();
        consensus_chain.epochs().persist().await.unwrap();
        (temp_dir, consensus_chain, rec1)
    }

    /// A certificate the store accepts for `record`: one signature and no signer bits, as the
    /// startup tests above store. Not a quorum certificate.
    fn stored_cert(record: &EpochRecord, key_config: &KeyConfig) -> EpochCertificate {
        EpochCertificate {
            epoch_hash: record.digest(),
            signature: record.sign_vote(key_config).signature,
            signed_authorities: roaring::RoaringBitmap::new(),
        }
    }

    /// A mock network that acknowledges every publish, records each gossiped epoch vote, and
    /// drops the reply of every request, so a recovery download ends unanswered.
    fn spawn_quiet_network() -> (PrimaryNetworkHandle, Arc<parking_lot::Mutex<Vec<EpochVote>>>) {
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let published = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen = published.clone();
        tokio::spawn(async move {
            while let Some(cmd) = net_rx.recv().await {
                if let NetworkCommand::Publish { msg, reply, .. } = cmd {
                    // an epoch vote is gossiped as the third `PrimaryGossip` variant
                    if let Some((&2, vote)) = msg.split_first() {
                        if let Ok(vote) = tn_types::try_decode::<EpochVote>(vote) {
                            seen.lock().push(vote);
                        }
                    }
                    let _ = reply.send(Ok(MessageId::new(b"test")));
                }
            }
        });
        (PrimaryNetworkHandle::new_for_test(net_tx), published)
    }

    /// `tn_node.epoch_record_recertify_rounds_total` by its `result` label, from one snapshot (a
    /// snapshot drains the counters, so read it once per check).
    fn recertify_rounds(
        snapshotter: &metrics_util::debugging::Snapshotter,
    ) -> BTreeMap<String, u64> {
        use metrics_util::debugging::DebugValue;
        let mut rounds = BTreeMap::new();
        for (key, .., value) in snapshotter.snapshot().into_vec() {
            let DebugValue::Counter(n) = value else { continue };
            if key.key().name() != "tn_node.epoch_record_recertify_rounds_total" {
                continue;
            }
            for label in key.key().labels().filter(|label| label.key() == "result") {
                rounds.insert(label.value().to_string(), n);
            }
        }
        rounds
    }

    /// P2's same-digest branch: after a failed local quorum the peer-recovery download returns
    /// our own record with a quorum certificate, which is saved; the node does not stop. With
    /// the switch on, the saved certificate drops the signatures kept for the epoch, and a
    /// re-certify round counts as `certified`.
    #[tokio::test(start_paused = true)]
    async fn test_recovery_saves_a_same_digest_cert() {
        use metrics_util::debugging::DebuggingRecorder;

        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) =
            record_chain("recovery_same_digest", &committee).await;
        let epoch_hash = local_rec.digest();
        let cert = make_cert(&local_rec, &[&keys[0], &keys[1], &keys[2]]);
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        {
            let record = local_rec.clone();
            let peer = committee[1];
            tokio::spawn(async move {
                while let Some(cmd) = net_rx.recv().await {
                    if let NetworkCommand::SendRequestAny { reply, .. } = cmd {
                        let _ = reply.send(Ok(NetworkResponseMessage {
                            peer,
                            result: PrimaryResponse::EpochRecord {
                                record: record.clone(),
                                certificate: cert.clone(),
                            },
                        }));
                    }
                }
            });
        }
        let retained = RetainedEpochVotes::default();
        retained.keep(1, epoch_hash, 1, local_rec.sign_vote(&keys[1]).signature);
        retained.mark_recertify(1);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);
        let node_shutdown = ShutdownNotifier::new();
        let noticer = node_shutdown.subscribe();

        // Hold the vote sender open and send no further vote, so the round times out into
        // recovery. No outer virtual-time timeout: the round reads the store.
        let (_vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        manage_epoch_votes(
            local_rec.clone(),
            keys[0].clone(),
            primary_network,
            vote_rx,
            consensus_chain.clone(),
            node_shutdown,
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");

        assert!(!noticer.noticed(), "our own digest is no fork");
        let stored =
            consensus_chain.epochs().cert_by_digest(epoch_hash).await.expect("certificate saved");
        assert!(local_rec.verify_with_cert(&stored));
        assert!(retained.signatures(1, epoch_hash).is_empty(), "kept signatures dropped");
        assert_eq!(recertify_rounds(&snapshotter), BTreeMap::from([("certified".into(), 1)]));
    }

    /// Two rounds for the same record, each one vote short of the quorum of three, add up to a
    /// quorum through the kept signatures. The certificate is byte for byte the one a single
    /// round with the same three votes stores.
    #[tokio::test(start_paused = true)]
    async fn test_retained_votes_reach_quorum_across_rounds() {
        use metrics_util::debugging::DebuggingRecorder;

        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) =
            record_chain("retained_across_rounds", &committee).await;
        let epoch_hash = local_rec.digest();
        let (primary_network, _published) = spawn_quiet_network();
        let retained = RetainedEpochVotes::default();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);

        // round 1: members 1 and 2 vote; the sender stays open, so the round times out, its
        // recovery goes unanswered, and it ends short
        retained.mark_recertify(1);
        let (round1_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for key_config in [&keys[1], &keys[2]] {
            round1_tx.send(local_rec.sign_vote(key_config)).await.unwrap();
        }
        manage_epoch_votes(
            local_rec.clone(),
            keys[0].clone(),
            primary_network.clone(),
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");
        assert!(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none(), "short");
        assert_eq!(retained.signatures(1, epoch_hash).len(), 2, "round 1's votes are kept");

        // round 2: members 2 and 3 vote, also one short on its own
        retained.mark_recertify(1);
        let (round2_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for key_config in [&keys[2], &keys[3]] {
            round2_tx.send(local_rec.sign_vote(key_config)).await.unwrap();
        }
        manage_epoch_votes(
            local_rec.clone(),
            keys[0].clone(),
            primary_network.clone(),
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");
        let across = consensus_chain
            .epochs()
            .cert_by_digest(epoch_hash)
            .await
            .expect("the two rounds add up to a quorum");
        assert!(local_rec.verify_with_cert(&across));
        assert_eq!(across.signed_authorities.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert!(retained.signatures(1, epoch_hash).is_empty(), "kept signatures dropped");
        assert_eq!(
            recertify_rounds(&snapshotter),
            BTreeMap::from([("certified".into(), 1), ("short".into(), 1)]),
        );

        // one round, the switch off, receiving the same three votes for the same record
        let (_dir, single_chain, single_rec) =
            record_chain("retained_single_round", &committee).await;
        assert_eq!(single_rec.digest(), epoch_hash);
        let (single_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for key_config in [&keys[1], &keys[2], &keys[3]] {
            single_tx.send(single_rec.sign_vote(key_config)).await.unwrap();
        }
        manage_epoch_votes(
            single_rec,
            keys[0].clone(),
            primary_network,
            vote_rx,
            single_chain.clone(),
            ShutdownNotifier::new(),
            None,
        )
        .await
        .expect("no local store failure");
        let single = single_chain.epochs().cert_by_digest(epoch_hash).await.expect("certified");
        assert_eq!(encode(&across), encode(&single), "same certificate, byte for byte");
    }

    /// Signatures kept for another record of the same epoch never count toward our record.
    #[tokio::test]
    async fn test_retained_votes_for_another_digest_never_mix() {
        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) =
            record_chain("retained_other_digest", &committee).await;
        let epoch_hash = local_rec.digest();
        let other = EpochRecord { next_committee: committee[1..].to_vec(), ..local_rec.clone() };
        assert_ne!(other.digest(), epoch_hash);
        let retained = RetainedEpochVotes::default();
        for (index, key_config) in [(1, &keys[1]), (2, &keys[2])] {
            retained.keep(1, other.digest(), index, other.sign_vote(key_config).signature);
        }
        let (primary_network, _published) = spawn_quiet_network();

        // one vote for our record; with the other record's two it would be a quorum of three
        let (vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        vote_tx.send(local_rec.sign_vote(&keys[3])).await.unwrap();
        // the closed channel ends the round once it has counted the vote
        drop(vote_tx);
        manage_epoch_votes(
            local_rec,
            keys[0].clone(),
            primary_network,
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");

        assert!(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none());
        let ours: Vec<u32> =
            retained.signatures(1, epoch_hash).into_iter().map(|(index, _)| index).collect();
        assert_eq!(ours, vec![3], "only our record's vote is kept for it");
        assert_eq!(retained.signatures(1, other.digest()).len(), 2, "the other record's stay");
    }

    /// A certificate the record collector stores (here written straight to the store, as its
    /// `save` does) drops the kept signatures for the epoch at the next prune.
    #[tokio::test]
    async fn test_retained_votes_dropped_when_the_collector_stores_the_cert() {
        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) =
            record_chain("retained_dropped_on_cert", &committee).await;
        let epoch_hash = local_rec.digest();
        let retained = RetainedEpochVotes::default();
        retained.keep(1, epoch_hash, 1, local_rec.sign_vote(&keys[1]).signature);

        retained.prune(consensus_chain.epochs(), 2).await;
        assert_eq!(retained.signatures(1, epoch_hash).len(), 1, "no certificate yet");

        let cert = make_cert(&local_rec, &[&keys[0], &keys[1], &keys[2]]);
        consensus_chain.epochs().save(local_rec, cert).await.unwrap();
        retained.prune(consensus_chain.epochs(), 2).await;
        assert!(retained.signatures(1, epoch_hash).is_empty(), "the stored cert drops them");
    }

    /// Kept signatures stay inside the lookback window and the record cap: at most
    /// `MAX_RETAINED_RECORDS` records, the newest ones, none below
    /// `latest - MAX_RECERTIFY_LOOKBACK`.
    #[tokio::test]
    async fn test_retained_votes_stay_inside_the_lookback() {
        let temp_dir = TempDir::with_prefix("retained_lookback").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        let (_, keys) = four_keys();
        let record = |epoch: Epoch| EpochRecord { epoch, ..Default::default() };
        let keep = |retained: &RetainedEpochVotes, epoch: Epoch| {
            let record = record(epoch);
            retained.keep(epoch, record.digest(), 0, record.sign_vote(&keys[0]).signature);
        };
        let held = |retained: &RetainedEpochVotes| -> Vec<Epoch> {
            (0..=40)
                .filter(|epoch| !retained.signatures(*epoch, record(*epoch).digest()).is_empty())
                .collect()
        };
        let retained = RetainedEpochVotes::default();
        for epoch in 0..=20 {
            keep(&retained, epoch);
        }
        // before any prune the window is open; the cap keeps the newest records
        assert_eq!(MAX_RETAINED_RECORDS, 17);
        assert_eq!(held(&retained), (4..=20).collect::<Vec<_>>());
        // a record older than every kept one is refused at the cap
        keep(&retained, 2);
        assert_eq!(held(&retained), (4..=20).collect::<Vec<_>>());

        // the close of epoch 24 moves the window to 8..=24
        retained.prune(&db, 24).await;
        assert_eq!(held(&retained), (8..=20).collect::<Vec<_>>());
        keep(&retained, 7);
        keep(&retained, 21);
        assert_eq!(held(&retained), (8..=21).collect::<Vec<_>>());
    }

    /// A vote that reaches a round after its vote loop ended, while it is in peer recovery, is
    /// kept for the next round instead of being dropped with the channel.
    #[tokio::test(start_paused = true)]
    async fn test_retained_votes_keep_a_vote_sent_during_recovery() {
        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) =
            record_chain("retained_during_recovery", &committee).await;
        let epoch_hash = local_rec.digest();
        let late_vote = local_rec.sign_vote(&keys[3]);
        let (vote_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        vote_tx.send(local_rec.sign_vote(&keys[1])).await.unwrap();
        // the mock answers no recovery request; its first one sends the late vote
        let (net_tx, mut net_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(100);
        let primary_network = PrimaryNetworkHandle::new_for_test(net_tx);
        tokio::spawn(async move {
            let mut late = Some(late_vote);
            while let Some(cmd) = net_rx.recv().await {
                if let NetworkCommand::SendRequestAny { .. } = cmd {
                    if let Some(vote) = late.take() {
                        vote_tx.send(vote).await.unwrap();
                    }
                }
            }
        });
        let retained = RetainedEpochVotes::default();

        manage_epoch_votes(
            local_rec,
            keys[0].clone(),
            primary_network,
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");

        assert!(consensus_chain.epochs().cert_by_digest(epoch_hash).await.is_none());
        let kept: Vec<u32> =
            retained.signatures(1, epoch_hash).into_iter().map(|(index, _)| index).collect();
        assert_eq!(kept, vec![1, 3], "the loop's vote and the late one are kept");
    }

    /// The pick walks the uncertified records of `N - 16 ..= N - 2` round-robin by `N`, leaves
    /// `N - 1` to the failsafe, and finds nothing past the lookback.
    #[tokio::test]
    async fn test_recertify_pick_walks_the_candidates_round_robin() {
        let temp_dir = TempDir::with_prefix("recertify_pick").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        let (_, keys) = four_keys();
        // records 0..=10; only 2, 5 and 7 lack a certificate
        for epoch in 0..=10 {
            let record = EpochRecord { epoch, ..Default::default() };
            if [2, 5, 7].contains(&epoch) {
                db.save_record(record).await.unwrap();
            } else {
                let cert = stored_cert(&record, &keys[0]);
                db.save(record, cert).await.unwrap();
            }
        }
        db.persist().await.unwrap();

        for (closing, expected) in [
            (1, None),
            (2, None),
            (4, Some(2)),
            (7, Some(5)),
            (8, Some(2)),
            (9, Some(2)),
            (10, Some(5)),
            (11, Some(7)),
            (12, Some(2)),
            (21, Some(7)),
            (22, Some(7)),
            (24, None),
        ] {
            let picked = pick_recertify_record(&db, closing).await.map(|record| record.epoch);
            assert_eq!(picked, expected, "close of epoch {closing}");
        }
    }

    /// A node outside the picked record's committee signs nothing for it and counts the pick as
    /// `skipped`; the pick of a record whose committee it is in is signed, sent to the local
    /// collector and gossiped.
    #[tokio::test]
    async fn test_recertify_skips_a_record_outside_our_committee() {
        use metrics_util::debugging::DebuggingRecorder;
        use tn_types::TnReceiver as _;

        let (committee, keys) = four_keys();
        let temp_dir = TempDir::with_prefix("recertify_not_a_member").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        // records 0..=3, uncertified; record 1's committee leaves this node (member 0) out
        for epoch in 0..=3 {
            let members = if epoch == 1 { committee[1..].to_vec() } else { committee.clone() };
            db.save_record(EpochRecord { epoch, committee: members, ..Default::default() })
                .await
                .unwrap();
        }
        db.persist().await.unwrap();
        let consensus_bus = ConsensusBus::new();
        let mut bus_votes = consensus_bus.app().subscribe_new_epoch_votes();
        let (primary_network, published) = spawn_quiet_network();
        let retained = RetainedEpochVotes::default();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);

        // the close of epoch 3 picks candidate 3 % 2 = 1 of {0, 1}: record 1
        recertify_older_record(&db, consensus_bus.app(), &keys[0], &primary_network, &retained, 3)
            .await;
        assert_eq!(recertify_rounds(&snapshotter), BTreeMap::from([("skipped".into(), 1)]));
        assert!(bus_votes.try_recv().is_err(), "no vote for the local collector");
        assert!(published.lock().is_empty(), "no vote gossiped");
        assert!(!retained.take_recertify(1), "no round to report");

        // the close of epoch 6 picks candidate 6 % 4 = 2 of {0, 1, 2, 3}: record 2
        recertify_older_record(&db, consensus_bus.app(), &keys[0], &primary_network, &retained, 6)
            .await;
        let vote = bus_votes.try_recv().expect("our vote for the local collector");
        assert_eq!((vote.epoch, vote.public_key), (2, committee[0]));
        assert_eq!(published.lock().iter().map(|vote| vote.epoch).collect::<Vec<_>>(), vec![2]);
        assert!(retained.take_recertify(2), "the round for record 2 reports");
    }

    /// Close epoch 3 through the collector with records 0..=3 stored uncertified and this node in
    /// every committee, and return the epochs this node gossiped a vote for.
    async fn epochs_voted_at_close(recertify: bool) -> BTreeSet<Epoch> {
        let (committee, keys) = four_keys();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("epochs_voted_at_close").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let records: Vec<EpochRecord> = (0..=3)
            .map(|epoch| EpochRecord {
                epoch,
                committee: committee.clone(),
                next_committee: committee.clone(),
                ..Default::default()
            })
            .collect();
        for record in &records {
            consensus_chain.epochs().save_record(record.clone()).await.unwrap();
        }
        consensus_chain.epochs().persist().await.unwrap();
        let (primary_network, published) = spawn_quiet_network();
        let consensus_bus = ConsensusBus::new();
        let task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::new();
        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            keys[0].clone(),
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
            recertify,
        );

        consensus_bus.app().epoch_record_watch().send_replace(Some(records[3].clone()));
        // the vote for the closing record goes out last, after INITIAL_VOTE_PUBLISH_DELAY
        wait_until(Duration::from_secs(5), "the vote for epoch 3 gossiped", || async {
            Ok(published.lock().iter().any(|vote| vote.epoch == 3))
        })
        .await
        .unwrap();
        node_shutdown.notify();
        let votes = published.lock().clone();
        assert!(votes.iter().all(|vote| vote.public_key == committee[0]), "only our votes");
        votes.iter().map(|vote| vote.epoch).collect()
    }

    /// With the switch on, the close of epoch 3 re-votes the previous record (the failsafe) and
    /// one older uncertified record: candidate 3 % 2 = 1 of {0, 1}.
    #[tokio::test]
    async fn test_failsafe_revotes_an_older_uncertified_record() {
        assert_eq!(epochs_voted_at_close(true).await, BTreeSet::from([1, 2, 3]));
    }

    /// The switch-off golden: the close of epoch 3 votes for its record and re-votes only the
    /// previous one, whatever older records are uncertified.
    #[tokio::test]
    async fn test_switch_off_revotes_only_the_previous_record() {
        assert_eq!(epochs_voted_at_close(false).await, BTreeSet::from([2, 3]));
    }

    /// The startup re-arm with the switch on also arms a certified latest record when a record
    /// within the lookback before it is uncertified; off, or with every record certified, it
    /// stays quiet as before.
    #[tokio::test]
    async fn test_startup_rearm_recertifies_behind_a_certified_latest_record() {
        let (_, keys) = four_keys();
        for (recertify, older_certified, armed) in
            [(false, false, false), (true, false, true), (true, true, false)]
        {
            let temp_dir = TempDir::with_prefix("startup_rearm_recertify").unwrap();
            let db = EpochRecordDb::open(temp_dir.path()).unwrap();
            let records: Vec<EpochRecord> =
                (0..=2).map(|epoch| EpochRecord { epoch, ..Default::default() }).collect();
            for record in &records[..2] {
                if older_certified {
                    db.save(record.clone(), stored_cert(record, &keys[0])).await.unwrap();
                } else {
                    db.save_record(record.clone()).await.unwrap();
                }
            }
            db.save(records[2].clone(), stored_cert(&records[2], &keys[0])).await.unwrap();
            db.persist().await.unwrap();

            let (watch_tx, watch_rx) = watch::channel(None);
            revote_uncertified_epoch_record_on_startup(&db, &watch_tx, recertify).await;
            let expected = armed.then(|| records[2].digest());
            assert_eq!(
                watch_rx.borrow().as_ref().map(|record| record.digest()),
                expected,
                "recertify={recertify} older_certified={older_certified}",
            );
        }
    }

    /// The epochs of the queued round senders after `handle_new_vote` starts a round for epoch 1
    /// with rounds already queued for epochs 10, 4, 6, 7 and 8; with `ended`, the round of epoch 6
    /// has ended (its receiver is gone).
    async fn queue_after_a_new_round(recertify: bool, ended: bool) -> Vec<Epoch> {
        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) =
            record_chain("vote_queue_eviction", &committee).await;
        let (primary_network, _published) = spawn_quiet_network();
        let task_manager = TaskManager::default();
        let (store_failed, _store_failed_rx) = mpsc::channel(1);
        let mut receivers = Vec::new();
        let mut vote_queues: VoteQueue = VecDeque::new();
        for epoch in [10, 4, 6, 7, 8] {
            let (tx, rx) = mpsc::channel::<EpochVote>(1);
            if !(ended && epoch == 6) {
                receivers.push(rx);
            }
            vote_queues.push_back((epoch, tx));
        }
        handle_new_vote(
            local_rec.sign_vote(&keys[1]),
            &mut vote_queues,
            consensus_chain,
            keys[0].clone(),
            primary_network,
            &task_manager.get_spawner(),
            ShutdownNotifier::new(),
            &store_failed,
            recertify.then(RetainedEpochVotes::default),
        )
        .await;
        vote_queues.iter().map(|(epoch, _)| *epoch).collect()
    }

    /// With the switch on, a full vote queue first drops the senders of rounds that ended, then
    /// evicts the round of the oldest epoch, so the newest record's round is not evicted first.
    /// Off, the front entry is evicted as before.
    #[tokio::test]
    async fn test_vote_queue_eviction_spares_the_newest_round() {
        assert_eq!(queue_after_a_new_round(false, false).await, vec![4, 6, 7, 8, 1]);
        assert_eq!(queue_after_a_new_round(false, true).await, vec![4, 6, 7, 8, 1]);
        assert_eq!(queue_after_a_new_round(true, false).await, vec![10, 6, 7, 8, 1]);
        assert_eq!(queue_after_a_new_round(true, true).await, vec![10, 4, 7, 8, 1]);
    }

    /// A re-certify round for an older record that ends short, and one that certifies, leave
    /// `tn_node.epoch_record_signers` at the value the latest record's round set; their outcomes
    /// are counted in `tn_node.epoch_record_recertify_rounds_total`.
    #[tokio::test(start_paused = true)]
    async fn test_recertify_rounds_leave_the_signers_gauge_alone() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, latest) =
            record_chain("recertify_signers_gauge", &committee).await;
        let older = consensus_chain.epochs().get_epoch_by_number(0).await.expect("record 0").0;
        let (primary_network, _published) = spawn_quiet_network();
        let retained = RetainedEpochVotes::default();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);

        // the latest record's round: all four members vote, so the gauge reads 4
        let (latest_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for key_config in &keys {
            latest_tx.send(latest.sign_vote(key_config)).await.unwrap();
        }
        manage_epoch_votes(
            latest,
            keys[0].clone(),
            primary_network.clone(),
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");

        // the re-certify round for record 0 ends short with one vote (it would write 1)
        retained.mark_recertify(0);
        let (short_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        short_tx.send(older.sign_vote(&keys[1])).await.unwrap();
        manage_epoch_votes(
            older.clone(),
            keys[0].clone(),
            primary_network.clone(),
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");

        // the next re-certify round for record 0 certifies with three signers (it would write 3)
        retained.mark_recertify(0);
        let (certify_tx, vote_rx) = mpsc::channel::<EpochVote>(10);
        for key_config in [&keys[2], &keys[3]] {
            certify_tx.send(older.sign_vote(key_config)).await.unwrap();
        }
        manage_epoch_votes(
            older.clone(),
            keys[0].clone(),
            primary_network,
            vote_rx,
            consensus_chain.clone(),
            ShutdownNotifier::new(),
            Some(retained.clone()),
        )
        .await
        .expect("no local store failure");
        assert!(consensus_chain.epochs().cert_by_digest(older.digest()).await.is_some());

        // one snapshot: it drains the counters
        let mut signers_gauge = None;
        let mut rounds = BTreeMap::new();
        for (key, .., value) in snapshotter.snapshot().into_vec() {
            match value {
                DebugValue::Gauge(gauge) if key.key().name() == "tn_node.epoch_record_signers" => {
                    signers_gauge = Some(gauge.0);
                }
                DebugValue::Counter(n)
                    if key.key().name() == "tn_node.epoch_record_recertify_rounds_total" =>
                {
                    for label in key.key().labels() {
                        rounds.insert(label.value().to_string(), n);
                    }
                }
                _ => {}
            }
        }
        assert_eq!(signers_gauge, Some(4.0), "the latest record's round set the gauge");
        assert_eq!(rounds, BTreeMap::from([("certified".into(), 1), ("short".into(), 1)]));
    }

    // ---- bench7 epoch-record-collector-hardening: withhold hook ----

    thread_local! {
        /// The withhold hook a test installed on this thread with [`WithholdOnThisThread`]. The
        /// tests run on current-thread runtimes, so the rounds they spawn consult it too.
        static WITHHOLD: std::cell::RefCell<Option<tn_types::test_hooks::WithholdEpochVote>> =
            const { std::cell::RefCell::new(None) };
    }

    /// The hook installed on this thread, if any, asked for `epoch` (see
    /// [`super::withhold_epoch_vote_round`]).
    pub(super) fn withheld_round(epoch: Epoch) -> Option<u32> {
        WITHHOLD.with(|hook| hook.borrow().as_ref().and_then(|hook| hook.withhold(epoch)))
    }

    /// A withhold hook installed on this thread until the value is dropped.
    struct WithholdOnThisThread;

    impl WithholdOnThisThread {
        /// Withhold this node's vote for the record of `epoch` in its first `rounds` rounds.
        fn install(epoch: Epoch, rounds: u32) -> Self {
            let hook = tn_types::test_hooks::WithholdEpochVote::new(epoch, rounds);
            WITHHOLD.with(|slot| *slot.borrow_mut() = Some(hook));
            Self
        }
    }

    impl Drop for WithholdOnThisThread {
        fn drop(&mut self) {
            WITHHOLD.with(|slot| *slot.borrow_mut() = None);
        }
    }

    /// The hook's lines among `lines`.
    fn hook_lines(lines: &parking_lot::Mutex<Vec<String>>) -> Vec<String> {
        lines.lock().iter().filter(|line| line.contains("test hook")).cloned().collect()
    }

    /// Run the collector as member 0 of four with records 0..=4 stored uncertified and the hook
    /// withholding the first `rounds` rounds for record 3, close epoch 3 and then epoch 4, and
    /// return the epochs this node gossiped a vote for and the hook's lines.
    ///
    /// The close of 3 re-votes record 2 (the failsafe) and would vote for record 3 (round 1);
    /// the close of 4 would re-vote record 3 (the failsafe, round 2) and votes for record 4. The
    /// vote for record 4 goes out `INITIAL_VOTE_PUBLISH_DELAY` after the close of 4, so a vote for
    /// record 3 delayed the same way from the earlier close would have gone out before it.
    async fn closes_with_the_hook(rounds: u32) -> (BTreeSet<Epoch>, Vec<String>) {
        let _hook = WithholdOnThisThread::install(3, rounds);
        let lines = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        let (committee, keys) = four_keys();
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::with_prefix("withhold_closes").unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let records: Vec<EpochRecord> = (0..=4)
            .map(|epoch| EpochRecord {
                epoch,
                committee: committee.clone(),
                next_committee: committee.clone(),
                ..Default::default()
            })
            .collect();
        for record in &records {
            consensus_chain.epochs().save_record(record.clone()).await.unwrap();
        }
        consensus_chain.epochs().persist().await.unwrap();
        let (primary_network, published) = spawn_quiet_network();
        let consensus_bus = ConsensusBus::new();
        let task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::new();
        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            keys[0].clone(),
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
            false,
        );

        // the failsafe's vote for record 2 shows the collector took record 3 off the watch
        consensus_bus.app().epoch_record_watch().send_replace(Some(records[3].clone()));
        wait_until(Duration::from_secs(5), "the failsafe vote for epoch 2", || async {
            Ok(published.lock().iter().any(|vote| vote.epoch == 2))
        })
        .await
        .unwrap();
        consensus_bus.app().epoch_record_watch().send_replace(Some(records[4].clone()));
        wait_until(Duration::from_secs(5), "the vote for epoch 4 gossiped", || async {
            Ok(published.lock().iter().any(|vote| vote.epoch == 4))
        })
        .await
        .unwrap();
        node_shutdown.notify();
        let votes = published.lock().clone();
        assert!(votes.iter().all(|vote| vote.public_key == committee[0]), "only our votes");
        (votes.iter().map(|vote| vote.epoch).collect(), hook_lines(&lines))
    }

    /// The default form withholds the original round for the named record only: the failsafe for
    /// the record before it, the failsafe for the named record at the next close and the vote for
    /// the record after it all go out.
    #[tokio::test]
    async fn test_withhold_hook_silences_only_the_first_round_by_default() {
        let (voted, lines) = closes_with_the_hook(1).await;
        assert_eq!(voted, BTreeSet::from([2, 3, 4]));
        assert_eq!(lines, vec!["INFO test hook: withholding own epoch vote epoch=3 round=1"]);
    }

    /// Two rounds withhold the failsafe at the next close as well, so no vote of this node for
    /// the named record goes out.
    #[tokio::test]
    async fn test_withhold_hook_rounds_reach_the_failsafe() {
        let (voted, lines) = closes_with_the_hook(2).await;
        assert_eq!(voted, BTreeSet::from([2, 4]));
        assert_eq!(
            lines,
            vec![
                "INFO test hook: withholding own epoch vote epoch=3 round=1",
                "INFO test hook: withholding own epoch vote epoch=3 round=2",
            ]
        );
    }

    /// A withheld round still starts on another member's vote and counts the others: three peer
    /// votes certify our record, and the certificate carries their signatures and not ours.
    #[tokio::test]
    async fn test_withheld_vote_is_not_counted_but_the_others_are() {
        let _hook = WithholdOnThisThread::install(1, 1);
        let lines = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        let (committee, keys) = four_keys();
        let (_dir, consensus_chain, local_rec) = record_chain("withheld_vote", &committee).await;
        let (primary_network, published) = spawn_quiet_network();
        let consensus_bus = ConsensusBus::new();
        let task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::new();
        spawn_epoch_vote_collector(
            consensus_chain.clone(),
            consensus_bus.app().clone(),
            keys[0].clone(),
            primary_network,
            task_manager.get_spawner(),
            node_shutdown.clone(),
            false,
        );

        // the failsafe's vote for record 0 shows the collector took record 1 off the watch
        consensus_bus.app().epoch_record_watch().send_replace(Some(local_rec.clone()));
        wait_until(Duration::from_secs(5), "the failsafe vote for epoch 0", || async {
            Ok(published.lock().iter().any(|vote| vote.epoch == 0))
        })
        .await
        .unwrap();
        for key_config in &keys[1..] {
            new_epoch_vote(consensus_bus.app(), local_rec.sign_vote(key_config));
        }
        let digest = local_rec.digest();
        wait_until(Duration::from_secs(10), "the certificate for record 1", || async {
            Ok(consensus_chain.epochs().cert_by_digest(digest).await.is_some())
        })
        .await
        .unwrap();
        node_shutdown.notify();

        let cert = consensus_chain.epochs().cert_by_digest(digest).await.expect("stored");
        assert_eq!(cert.signed_authorities.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert!(local_rec.verify_with_cert(&cert));
        assert!(published.lock().iter().all(|vote| vote.epoch != 1), "no vote of ours for 1");
        assert_eq!(
            hook_lines(&lines),
            vec!["INFO test hook: withholding own epoch vote epoch=1 round=1"]
        );
    }

    /// The re-certify pick is one more round for the record: inside the configured rounds it is
    /// withheld (the pick's line and mark stay), and the next pick past them votes.
    #[tokio::test]
    async fn test_withhold_hook_reaches_recertify_picks_only_within_its_rounds() {
        use tn_types::TnReceiver as _;

        let _hook = WithholdOnThisThread::install(1, 3);
        let lines = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        let (committee, keys) = four_keys();
        let temp_dir = TempDir::with_prefix("withhold_recertify").unwrap();
        let db = EpochRecordDb::open(temp_dir.path()).unwrap();
        for epoch in 0..=3 {
            db.save_record(EpochRecord {
                epoch,
                committee: committee.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        db.persist().await.unwrap();
        let consensus_bus = ConsensusBus::new();
        let mut bus_votes = consensus_bus.app().subscribe_new_epoch_votes();
        let (primary_network, published) = spawn_quiet_network();
        let retained = RetainedEpochVotes::default();

        // rounds 1 and 2 for record 1 stand for its original round and the failsafe
        assert!(withhold_own_epoch_vote(1));
        assert!(withhold_own_epoch_vote(1));
        // the close of epoch 3 picks candidate 3 % 2 = 1 of {0, 1}: round 3, withheld
        recertify_older_record(&db, consensus_bus.app(), &keys[0], &primary_network, &retained, 3)
            .await;
        assert!(bus_votes.try_recv().is_err(), "no vote for the local collector");
        assert!(published.lock().is_empty(), "no vote gossiped");
        assert!(retained.take_recertify(1), "the pick still marks its round");
        // the close of epoch 5 picks candidate 5 % 4 = 1 of {0, 1, 2, 3}: round 4 votes
        recertify_older_record(&db, consensus_bus.app(), &keys[0], &primary_network, &retained, 5)
            .await;
        let vote = bus_votes.try_recv().expect("our vote for the local collector");
        assert_eq!((vote.epoch, vote.public_key), (1, committee[0]));
        assert_eq!(published.lock().iter().map(|vote| vote.epoch).collect::<Vec<_>>(), vec![1]);
        let lines = hook_lines(&lines);
        assert_eq!(
            lines
                .iter()
                .map(|line| line.rsplit(' ').next().unwrap_or_default())
                .collect::<Vec<_>>(),
            vec!["round=1", "round=2", "round=3"],
        );
    }

    /// Without a hook installed every round votes, as on a node without the variables.
    #[test]
    fn test_withhold_hook_unset_withholds_nothing() {
        for epoch in [0, 1, 3, Epoch::MAX] {
            assert!(!withhold_own_epoch_vote(epoch));
        }
        let hook = WithholdOnThisThread::install(3, 1);
        assert!(withhold_own_epoch_vote(3));
        drop(hook);
        assert!(!withhold_own_epoch_vote(3), "the hook is gone with its guard");
    }
}
