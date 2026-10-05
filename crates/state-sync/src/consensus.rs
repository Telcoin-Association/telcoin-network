//! Tasks and helpers for collecting consensus headers and epoch pack files trustlessly.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, tables::ConsensusCache};
use tn_types::{
    ConsensusHeaderDigest, ConsensusNumHash, Database as TNDatabase, Epoch, EpochRecord, Noticer,
    TaskSpawner, TnReceiver, TnSender as _,
};
use tracing::{debug, error, info, warn};

/// How long to wait before retrying a failed pack file download.
const PACK_DOWNLOAD_RETRY_SECS: u64 = 5;
const PACK_RECORD_TIMEOUT_SECS: u64 = 10;
/// Minimum number of consensus outputs we must be behind on the in-progress current epoch before
/// bulk-downloading a verified partial pack (into staging) rather than crawling headers one at a
/// time. Small gaps stay on the cheap header-by-header path.
const PARTIAL_PACK_CATCHUP_THRESHOLD: u64 = 5;
/// A backward walk that keeps failing to fetch one output warns on the first failure and then on
/// every this many attempts (about every 100 s at the 5 s retry cap), and logs the rest at debug.
const RETRY_WARN_EVERY: u64 = 20;
/// Seconds between attempts at the current epoch's partial pack while no local pack can decode that
/// epoch (see [`PartialPackGate`]).
///
/// Fixed, with no growth: each attempt opens at most `MAX_EPOCH_SYNC_PROBES` (3) sync streams, a
/// peer that refuses is cached unsyncable for the epoch, and a peer that fails moves to the back of
/// the probe order (`order_probe_peers`), so successive attempts work through the peer set until
/// they reach one that serves. A growing backoff would only delay reaching that peer.
const PARTIAL_PACK_RETRY_SECS: u64 = 30;

enum ConsensusHeaderResult {
    Done,
    Continue(u64, ConsensusHeaderDigest),
    Retry,
    /// No local pack (or staging) can decode this epoch, so no peer's output for it can be
    /// verified until its pack arrives; the walk must stop rather than retry.
    Abort(Epoch),
}

/// A single consensus-height range `[floor..=ceil]` currently being walked by a fetch task.
///
/// Ranges carry no epoch tag even though the tracker is app-scoped (a walk from one epoch can
/// linger into the next). This is sound ONLY because consensus numbers are globally monotonic
/// across epochs, so a `(floor, ceil)` pair names a unique height range regardless of epoch. If
/// numbering ever became per-epoch, two epochs' ranges could alias and a walk could wrongly defer.
struct WalkRange {
    id: u64,
    floor: u64,
    ceil: u64,
}

#[derive(Default)]
struct WalkLedger {
    next_id: u64,
    ranges: Vec<WalkRange>,
}

/// Cloneable ledger of the consensus-height ranges fetch tasks are walking right now.
///
/// Every download walk registers its range here: the initial bulk backfill, each gossip-driven
/// backfill, and the catch-up gap fill. A single ledger lets a new walk see whether another task
/// already owns its range — so it can defer instead of racing a duplicate download — and bounds how
/// many walks run at once (`active`). Because a reservation is released by its `WalkGuard`'s
/// `Drop`, a walk that panics or is cancelled frees its claim, so a lost walk never wedges gap
/// recovery.
///
/// The lock is held only for the small, synchronous ledger updates below (never across an
/// `.await`), so it cannot deadlock the fetch loop or the walk tasks.
#[derive(Clone, Default)]
struct WalkTracker {
    inner: Arc<Mutex<WalkLedger>>,
}

impl WalkTracker {
    /// Number of walks in flight. A loose throttle metric: the value can race a walk finishing, but
    /// (as with the previous counter) one walk more or less does not matter.
    fn active(&self) -> usize {
        self.inner.lock().ranges.len()
    }

    /// Reserve `[floor..=ceil]` unconditionally, returning a guard that releases it on `Drop`.
    fn reserve(&self, floor: u64, ceil: u64) -> WalkGuard {
        let id = self.inner.lock().push(floor, ceil);
        WalkGuard { tracker: self.clone(), id }
    }

    /// Reserve `[floor..=ceil]` only if the in-flight walks do not TOGETHER already cover it.
    /// Returns `None` when they do: the stall that prompted the request is those walks still
    /// filling top-down (the execute loop drains bottom-up, so it sees no progress until they
    /// reach the bottom), not an unfilled gap, so we defer rather than duplicate the download.
    /// Coverage is a UNION test, not a single-range one: during deep catch-up the gossip
    /// backfills TILE the space (`[0..=n2], [n2+1..=n3], ...`), so no single walk covers a
    /// full-tip gap request even though together they do, so we sort the overlapping ranges by
    /// floor and sweep up from `floor`.
    ///
    /// Deferring is safe even when a covering walk is stuck retrying a height rather than
    /// progressing: `get_consensus_output` fetches via `request_consensus_output`, which re-samples
    /// peers on every attempt, so a fresh walk would retry the exact same way and gain nothing over
    /// the retries the covering walk is already making. A covering walk that instead panics or is
    /// cancelled releases its reservation on `Drop`, so the next stall re-drives with a fresh walk.
    fn reserve_if_uncovered(&self, floor: u64, ceil: u64) -> Option<WalkGuard> {
        let mut ledger = self.inner.lock();
        let mut ranges: Vec<(u64, u64)> = ledger
            .ranges
            .iter()
            .filter(|r| r.ceil >= floor && r.floor <= ceil)
            .map(|r| (r.floor, r.ceil))
            .collect();
        ranges.sort_unstable();
        // Sweep upward from `floor`. `needed` is the lowest height still to cover; using `Err` as
        // the short-circuit signal, a range starting above `needed` leaves a gap
        // (`Err(false)`), a range reaching `ceil` completes coverage (`Err(true)`),
        // otherwise advance past it. Running out of ranges without reaching `ceil` (`Ok`)
        // also means uncovered.
        let covered = ranges
            .into_iter()
            .try_fold(floor, |needed, (range_floor, range_ceil)| {
                if range_floor > needed {
                    Err(false)
                } else if range_ceil >= ceil {
                    Err(true)
                } else {
                    Ok(needed.max(range_ceil.saturating_add(1)))
                }
            })
            .err()
            .unwrap_or(false);
        (!covered).then(|| {
            let id = ledger.push(floor, ceil);
            WalkGuard { tracker: self.clone(), id }
        })
    }
}

impl WalkLedger {
    fn push(&mut self, floor: u64, ceil: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.ranges.push(WalkRange { id, floor, ceil });
        id
    }
}

/// Releases a [`WalkTracker`] reservation when the walk task ends (including on panic/cancel).
struct WalkGuard {
    tracker: WalkTracker,
    id: u64,
}

impl Drop for WalkGuard {
    fn drop(&mut self) {
        self.tracker.inner.lock().ranges.retain(|r| r.id != self.id);
    }
}

/// Decides when the gossip handler may start a partial pack catch-up of the current epoch.
///
/// The first attempt is always allowed. After that an attempt is allowed only when none is in
/// flight and the previous one started at least [`PARTIAL_PACK_RETRY_SECS`] earlier. The caller
/// asks again only while no local pack decodes the gossiped epoch, the one state in which the
/// backward walk cannot run at all, so an observer that merely lags a little keeps using the walk
/// instead of re-streaming a small prefix every 30 s.
#[derive(Default)]
struct PartialPackGate {
    /// Set while an attempt runs and cleared by its [`PartialPackFlight`] on drop.
    in_flight: Arc<AtomicBool>,
    /// When the most recent attempt started; `None` before the first.
    last_attempt: Option<Instant>,
}

impl PartialPackGate {
    /// True until the first attempt has started.
    fn is_first(&self) -> bool {
        self.last_attempt.is_none()
    }

    /// Start an attempt at `now`, or return `None` while one is in flight or the previous one
    /// started less than [`PARTIAL_PACK_RETRY_SECS`] before `now`. Taking `now` as a parameter
    /// keeps the backoff testable without a clock.
    fn try_begin(&mut self, now: Instant) -> Option<PartialPackFlight> {
        let backing_off = self.last_attempt.is_some_and(|last| {
            now.saturating_duration_since(last) < Duration::from_secs(PARTIAL_PACK_RETRY_SECS)
        });
        if backing_off || self.in_flight.swap(true, Ordering::AcqRel) {
            return None;
        }
        self.last_attempt = Some(now);
        Some(PartialPackFlight { in_flight: self.in_flight.clone() })
    }
}

/// Marks a partial pack attempt finished when dropped, including when its task panics or is
/// cancelled, so a lost attempt cannot close the [`PartialPackGate`] for good.
struct PartialPackFlight {
    in_flight: Arc<AtomicBool>,
}

impl Drop for PartialPackFlight {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
    }
}

/// Retrieve a verified consensus OUTPUT (header + batches) for `number` and cache it.
///
/// `hash` is the already-verified consensus header digest for `number` — it comes from validated
/// consensus gossip (the tip) or from a verified descendant's `parent_hash`.
/// `request_consensus_output` pulls the output from a peer and, because the v1 pack is
/// header-first, stream-decodes it with the epoch committee and verifies the decoded header's
/// digest equals `hash` BEFORE buffering batches — so a wrong/forked output is rejected (and the
/// peer penalized) without ever materializing it. This upholds the invariant that we never
/// cache/execute an output that is not verified (directly by gossip, or as an ancestor of one). The
/// walk proceeds recent→earliest, so each cached entry is a verified output.
///
/// Returns [`ConsensusHeaderResult::Abort`] without touching the network when `number` falls in an
/// epoch that no local pack or staging pack can decode: `request_consensus_output` refuses that
/// case on every attempt, so a retry could only succeed after the epoch's pack arrives, and that
/// arrival raises the stream target on its own. `attempt` counts the consecutive failed attempts
/// at this height and only rate-limits the retry warning.
async fn get_consensus_output<DB: TNDatabase>(
    number: u64,
    hash: ConsensusHeaderDigest,
    db: &DB,
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
    attempt: u64,
) -> ConsensusHeaderResult {
    // Already in a pack file (chain/staging) -> done.
    if consensus_chain.consensus_header_by_number(number).await.ok().flatten().is_some() {
        return ConsensusHeaderResult::Done;
    }
    // Already cached (verified when inserted) -> continue from its parent.
    if let Ok(Some(output)) = db.get::<ConsensusCache>(&number) {
        let header = output.consensus_header();
        return if header.number > 0 {
            ConsensusHeaderResult::Continue(header.number - 1, header.parent_hash)
        } else {
            ConsensusHeaderResult::Done
        };
    }
    // the same test `request_consensus_output` makes, done before any network round trip
    let epoch = consensus_chain.epochs().number_to_epoch(number);
    if !consensus_chain.contains_decode_epoch(epoch).await {
        return ConsensusHeaderResult::Abort(epoch);
    }
    // Pull, stream-decode, and header-hash-verify the output from any peer in one shot. The v1
    // pack is header-first, so the fetch checks the decoded header digest against `hash` before
    // buffering batches and returns only a verified output; a wrong-hash peer is penalized and
    // skipped inside the probe.
    let output = match network.request_consensus_output(number, consensus_chain, hash).await {
        Ok(output) => output,
        Err(e) => {
            // includes no peer serving it yet
            if attempt.is_multiple_of(RETRY_WARN_EVERY) {
                warn!(target: "tn::observer", %e, ?hash, ?number, attempt, "failed to fetch/verify consensus output from peer, will retry");
            } else {
                debug!(target: "tn::observer", %e, ?hash, ?number, attempt, "failed to fetch/verify consensus output from peer, will retry");
            }
            return ConsensusHeaderResult::Retry;
        }
    };
    let header = output.consensus_header();
    let parent = header.parent_hash;
    if let Err(e) = db.insert::<ConsensusCache>(&number, &output) {
        error!(target: "state-sync", ?e, "error saving a consensus output to cache storage!");
    }
    consensus_bus.send_last_consensus_header_if_newer(header);
    if number > 0 {
        ConsensusHeaderResult::Continue(number - 1, parent)
    } else {
        ConsensusHeaderResult::Done
    }
}

/// Attempt to request epoch packs for every epoch from current_fetch_epoch to latest epoch
/// record.
///
/// An epoch whose pack is already complete locally is not requested, so no fetcher will raise the
/// stream target for it. Instead, raise the `last_consensus_header` watch to the final header of
/// the highest complete epoch seen here, as a fetcher does after a download, so the stream task
/// replays those packs locally. The stream stops at the first missing output and drains the rest
/// once a requested pack below fills it.
async fn request_epochs(
    current_fetch_epoch: &mut Epoch,
    consensus_chain: &ConsensusChain,
    consensus_bus: &ConsensusBusApp,
) {
    // If we still have epochs to fetch then add to the queue until we are out of epoch records.
    // For epoch 0: `saturating_sub(1)` yields 0, so record_by_epoch(0) would return the
    // *real* epoch-0 record- use a synthetic epoch record instead.
    let maybe_previous = if *current_fetch_epoch == 0 {
        consensus_chain.epochs().record_by_epoch(0).await.map(|r| EpochRecord {
            committee: r.committee.clone(),
            next_committee: r.committee.clone(),
            ..EpochRecord::default()
        })
    } else {
        consensus_chain.epochs().record_by_epoch(current_fetch_epoch.saturating_sub(1)).await
    };
    let mut highest_complete = None;
    if let Some(mut previous_epoch_record) = maybe_previous {
        while let Some(epoch_record) =
            consensus_chain.epochs().record_by_epoch(*current_fetch_epoch).await
        {
            *current_fetch_epoch += 1;
            let contains_final_header = consensus_chain.is_epoch_complete(&epoch_record).await;
            // If the pack file is missing or incomplete request it.
            // Note since we have an epoch record this is a past epoch
            // not the current epoch.
            if contains_final_header {
                highest_complete = Some(epoch_record.final_consensus.number);
            } else {
                consensus_bus
                    .request_epoch_pack_file(previous_epoch_record, epoch_record.clone())
                    .await;
            }
            previous_epoch_record = epoch_record;
        }
    }
    let Some(final_number) = highest_complete else {
        return;
    };
    match consensus_chain.consensus_header_by_number(final_number).await {
        Ok(Some(final_header)) => {
            if consensus_bus.send_last_consensus_header_if_newer(final_header) {
                info!(target: "state-sync", final_header_number = final_number,
                    "local epoch packs complete, signaling stream to replay locally");
            }
        }
        Ok(None) => warn!(target: "state-sync", final_header_number = final_number,
            "complete local epoch pack is missing its final header"),
        Err(e) => warn!(target: "state-sync", final_header_number = final_number, ?e,
            "unable to read the final header of a complete local epoch pack"),
    }
}

/// Bulk fast-path for catching up the in-progress current epoch.
///
/// When far behind the latest *verified* gossip point, stream a verifiable PREFIX of the current
/// epoch's pack (up to `number`) into a side STAGING directory via
/// [`PrimaryNetworkHandle::request_partial_epoch_pack`]. The staged pack is read-only and lives in
/// its own dir — it is NEVER swapped over the live `epoch-{N}` dir — so importing it cannot race
/// the node's own in-order pack build. The forward drain ([`catch_up_consensus_from_to`]) then
/// sources headers/outputs from staging.
///
/// `(epoch, number, hash)` comes from verified consensus gossip, so `hash` is a trustworthy stop
/// point. The bail conditions keep this from firing on a live validator, for completed epochs, or
/// for small gaps the header path handles cheaply.
async fn try_partial_pack_catch_up(
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
    walk_tracker: &WalkTracker,
    epoch: Epoch,
    number: u64,
    hash: ConsensusHeaderDigest,
) -> bool {
    // An active validator builds the epoch itself; never run there.
    if consensus_bus.is_active_cvv() {
        return false;
    }
    // Dedup concurrent attempts.
    if consensus_chain.already_streaming_epoch(epoch) || consensus_chain.staging_final().is_some() {
        return false;
    }
    // Only for the in-progress current epoch; completed epochs use the full-pack path.
    if consensus_chain.epochs().record_by_epoch(epoch).await.is_some() {
        return false;
    }
    // Only worth a bulk download when far behind; small gaps stay on the header path.
    let our_latest = consensus_chain
        .latest_consensus_header_from_pack(epoch)
        .await
        .ok()
        .flatten()
        .map(|h| h.number)
        .unwrap_or(0);
    if number.saturating_sub(our_latest) <= PARTIAL_PACK_CATCHUP_THRESHOLD {
        return false;
    }
    // The previous epoch is complete; reuse the synthetic-for-epoch-0 shape `request_epochs` uses.
    let maybe_previous = if epoch == 0 {
        consensus_chain.epochs().record_by_epoch(0).await.map(|r| EpochRecord {
            committee: r.committee.clone(),
            next_committee: r.committee.clone(),
            ..EpochRecord::default()
        })
    } else {
        consensus_chain.epochs().record_by_epoch(epoch.saturating_sub(1)).await
    };
    let Some(previous_epoch_record) = maybe_previous else {
        return false;
    };
    // Only `.epoch` and `.final_consensus` are used for verification; the streamed pack carries and
    // self-verifies its own committee.
    let epoch_record = EpochRecord {
        epoch,
        committee: previous_epoch_record.next_committee.clone(),
        final_consensus: ConsensusNumHash::new(number, hash),
        parent_hash: previous_epoch_record.digest(),
        ..EpochRecord::default()
    };
    info!(target: "state-sync", epoch, number, our_latest, "bulk catching up current epoch via partial pack stream (staging)");
    // Reserve exactly the range this stream fills into staging (`our_latest+1..=number`). Staging
    // publishes atomically, so the observer drain sees nothing and stalls at `our_latest+1` for the
    // whole stream; without this reservation a catch-up gap fill would race it over the same range.
    // Held across the await; dropped when this returns (success or fall-back-to-header).
    let _guard = walk_tracker.reserve(our_latest.saturating_add(1), number);
    match network
        .request_partial_epoch_pack(
            &epoch_record,
            &previous_epoch_record,
            consensus_chain,
            number,
            Duration::from_secs(PACK_RECORD_TIMEOUT_SECS),
        )
        .await
    {
        Ok(()) => {
            // Nudge the forward drain to consume the staged prefix.
            if let Ok(Some(header)) = consensus_chain.consensus_header_by_number(number).await {
                consensus_bus.send_last_consensus_header_if_newer(header);
            }
            true
        }
        Err(e) => {
            warn!(target: "state-sync", epoch, number, ?e, "partial pack catch-up failed; falling back to header-by-header");
            false
        }
    }
}

/// Spawn a long running task on task_manager that will keep the last_consensus_header watch on
/// consensus_bus up to date. This should only be used when NOT participating in active consensus.
/// Note, this an epoch scoped task so it will be stopped on each epoch boundary.  It should handle
/// this but it is worth considering this will happen.  This is required because we only use this
/// task when an observer, never as a CVV.
pub(crate) async fn spawn_track_recent_consensus<DB: TNDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBusApp,
) {
    // Get the epoch of our last executed consensus.
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let rx_shutdown = config.shutdown().subscribe();
    // This loop will track current consensus as well as try to backfill from current.
    // This task backfills the current epoch records as well as requesting entire pack files
    // be downloaded for missing historic epochs.
    loop {
        tokio::select! {
            _ = rx_gossip_update.changed() => {
                let (epoch, number, hash) = *rx_gossip_update.borrow_and_update();
                let _ = consensus_bus.consensus_request_queue().send((epoch, number, hash)).await;
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");
            }

            _ = &rx_shutdown => {
                return;
            }
        }
    }
}

/// Spawn a long running task (application scoped)
/// that will fetch and download entire pack files for epochs.
/// This should only be used when NOT participating in active consensus.
/// Several of these will run but will do nothing unless requested.
/// This works by streaming an entire epochs pack file from a peer.
pub async fn spawn_fetch_consensus(
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBusApp,
    network: PrimaryNetworkHandle,
    task_index: u32, // Task index for logging.
    consensus_chain: ConsensusChain,
) {
    // requests this worker gave up on while the queue was full. a worker never waits for room in
    // the queue it drains, so it holds them here and offers them back after taking each request
    // and every retry interval while idle
    let mut deferred = Vec::new();
    // Get the epoch of our last executed consensus.
    loop {
        tokio::select! {
            Some((previous_epoch_record, mut epoch_record)) = consensus_bus.get_next_epoch_pack_file_request() => {
                // taking this request freed a slot for a held one
                requeue_deferred(&consensus_bus, &mut deferred);
                let epoch = epoch_record.epoch;
                let already_streaming = consensus_chain.already_streaming_epoch(epoch);
                if already_streaming || consensus_chain.is_epoch_complete(&epoch_record).await {
                    // If we have already streamed this epoch or are in process of streaming then continue.
                    // Note, it is a lot less complex to do this check here than to make sure we don't request
                    // the same pack more than once so do it this way.
                    info!(target: "state-sync", "epoch consensus fetcher {task_index} skipping epoch {epoch} we are streaming {already_streaming} or already have");
                    continue;
                }
                info!(target: "state-sync", "epoch consensus fetcher {task_index} retrieving epoch {epoch}");
                crate::STATE_SYNC_METRICS.epoch_pack_fetches_total.increment(1);
                let mut attempts = 1;
                loop {
                    tokio::select! {
                        result = network.request_epoch_pack(&epoch_record, &previous_epoch_record, &consensus_chain, Duration::from_secs(PACK_RECORD_TIMEOUT_SECS)) => {
                            match result {
                                Ok(_) => {
                                    // After a successful pack download, signal spawn_stream_consensus_headers
                                    // that locally-available blocks are ready. This unblocks streaming even
                                    // when the gossip/network path (request_consensus) is slow or unresponsive.
                                    match consensus_chain
                                        .consensus_header_by_number(epoch_record.final_consensus.number)
                                        .await {
                                        Ok(Some(final_header)) => {
                                            let number = final_header.number;
                                            if consensus_bus.send_last_consensus_header_if_newer(final_header) {
                                                info!(target: "state-sync",
                                                    epoch = epoch_record.epoch,
                                                    final_header_number = number,
                                                    "epoch pack downloaded, signaling stream to process locally available blocks");
                                            }
                                            break;
                                        }
                                        Ok(None) => error!(target: "state-sync",
                                            epoch = epoch_record.epoch,
                                            "Unable to find header by number for new pack file"),
                                        Err(e) => error!(target: "state-sync",
                                            epoch = epoch_record.epoch,
                                            ?e,
                                            "Unable to find header by number for new pack file"),
                                    }
                                }
                                Err(e) => error!(target: "state-sync",
                                        "failed to request epoch pack for epoch {epoch}, attempt {attempts}: {e}"),
                            }
                        }
                        _ = &rx_shutdown => {
                            info!(target: "state-sync",
                                "epoch consensus fetcher {task_index} shutting down during pack fetch");
                            break;
                        }
                    }
                    if attempts > 100 {
                        // We are giving up on this epoch for now.
                        // This is not a real solution, without getting this pack file execution will be stuck.
                        // But put it back on the queue and try another one since this one is not getting anywhere.
                        error!(target: "state-sync",
                            "failed to request epoch pack for epoch {epoch}, after {attempts}, will try to again later (WE ARE STUCK)");
                        if let Some(request) = consensus_bus.try_requeue_epoch_pack_file(previous_epoch_record, epoch_record) {
                            warn!(target: "state-sync",
                                "epoch request queue full, holding epoch {epoch} until there is room");
                            deferred.push(request);
                        }
                        break;
                    }
                    // The epoch record may have been a dummy (final_consensus.number=0)
                    // when first queued at startup. Refresh from DB so subsequent
                    // retries use the real signed cert if it has since arrived.
                    if let Some(fresh) = consensus_chain.epochs().record_by_epoch(epoch).await {
                        if fresh.final_consensus.number > epoch_record.final_consensus.number {
                            info!(target: "state-sync",
                                "refreshed epoch {epoch} record for retry: final_consensus {} -> {}",
                                epoch_record.final_consensus.number, fresh.final_consensus.number);
                            epoch_record = fresh;
                        }
                    }
                    // this worker is about to wait; offer the requests it holds back first so they
                    // do not wait with it
                    requeue_deferred(&consensus_bus, &mut deferred);
                    // Wait a beat before we try again, may have a network issue.
                    // Wait time will increase as attempts grow.
                    tokio::select! {
                        _ = &rx_shutdown => {
                            info!(target: "state-sync",
                                "epoch consensus fetcher {task_index} shutting down during pack fetch");
                            return;
                        },
                        _ = tokio::time::sleep(Duration::from_secs(((attempts / 10) + 1) * PACK_DOWNLOAD_RETRY_SECS)) => { }
                    }
                    attempts += 1;
                }
            }
            // a held request is not stranded if the queue empties while this worker waits on it
            _ = tokio::time::sleep(Duration::from_secs(PACK_DOWNLOAD_RETRY_SECS)), if !deferred.is_empty() => {
                requeue_deferred(&consensus_bus, &mut deferred);
            }
            _ = &rx_shutdown => {
                break;
            }
        }
    }
}

/// Offer each request a fetch worker holds back to the epoch request queue, keeping the ones the
/// full queue hands back.
fn requeue_deferred(
    consensus_bus: &ConsensusBusApp,
    deferred: &mut Vec<(EpochRecord, EpochRecord)>,
) {
    for (previous_epoch_record, epoch_record) in std::mem::take(deferred) {
        if let Some(request) =
            consensus_bus.try_requeue_epoch_pack_file(previous_epoch_record, epoch_record)
        {
            deferred.push(request);
        }
    }
}

/// Retrieve a consensus headers from a peer.
/// Start at number/hash and work backwards to end number.
///
/// The walk gives up when it reaches an epoch that no local pack can decode (see
/// [`get_consensus_output`]); returning drops the caller's [`WalkGuard`], which releases its range
/// so gap fills under it are no longer deferred to a walk that cannot progress.
async fn get_consensus_header_range<DB: TNDatabase>(
    number: u64,
    hash: ConsensusHeaderDigest,
    db: &DB,
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
    end_number: u64,
) {
    if number < end_number {
        return;
    }
    info!(target: "state-sync", ?number, ?hash, ?end_number, "fetching consensus from peer");
    let mut number = number;
    let mut hash = hash;
    let mut count = 1;
    let mut retries = 0;
    let mut attempts = 0;
    loop {
        match get_consensus_output(
            number,
            hash,
            db,
            consensus_bus,
            network,
            consensus_chain,
            attempts,
        )
        .await
        {
            ConsensusHeaderResult::Continue(next_number, next_hash) => {
                number = next_number;
                hash = next_hash;
                if number < end_number {
                    break;
                }
                if count % 10 == 0 {
                    info!(target: "state-sync", ?number, ?hash, ?end_number, "fetching consensus from peer");
                }
                count += 1;
                retries = 0;
                attempts = 0;
            }
            ConsensusHeaderResult::Done => break,
            ConsensusHeaderResult::Retry => {
                if retries < 5 {
                    retries += 1;
                }
                attempts += 1;
                tokio::time::sleep(Duration::from_secs(retries)).await;
            }
            ConsensusHeaderResult::Abort(epoch) => {
                warn!(target: "state-sync", epoch, ?number, ?end_number, "no local pack decodes this epoch; abandoning backward walk");
                break;
            }
        }
    }
}

/// Deal with an incoming consensus header request.
/// This exists to keep the select macro below smaller, hence the
/// large parameter list.
///
/// The first gossip tries a partial pack of the current epoch, as does every later gossip (gated
/// by `partial_gate`) while no local pack decodes the epoch `number` falls in. In that state a
/// backward walk cannot fetch anything, so only the partial pack, or the full pack once the epoch
/// closes, can make progress. No walk is started then: it would abort at once, and its
/// `last_number` bump would leave a hole below the next walk's floor. Otherwise the gossip starts a
/// backward walk down to the previous gossip point.
#[allow(clippy::too_many_arguments)]
async fn manage_new_consensus<DB: TNDatabase>(
    db: &DB,
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
    task_spawner: &TaskSpawner,
    walk_tracker: &WalkTracker,
    epoch: Epoch,
    number: u64,
    hash: ConsensusHeaderDigest,
    partial_gate: &mut PartialPackGate,
    last_number: &mut Option<u64>,
    current_fetch_epoch: &mut Epoch,
) {
    let db_clone = db.clone();
    let consensus_bus_clone = consensus_bus.clone();
    let network_clone = network.clone();
    let consensus_chain_clone = consensus_chain.clone();
    // map the number to an epoch the same way `request_consensus_output` does
    let walk_epoch = consensus_chain.epochs().number_to_epoch(number);
    let decodable = consensus_chain.contains_decode_epoch(walk_epoch).await;
    // the last three conditions are bails inside `try_partial_pack_catch_up`, checked here so an
    // attempt that would bail does not start the backoff
    let want_partial = (partial_gate.is_first() || !decodable)
        && !consensus_bus.is_active_cvv()
        && consensus_chain.staging_final().is_none()
        && !consensus_chain.already_streaming_epoch(epoch);
    let flight = want_partial.then(|| partial_gate.try_begin(Instant::now())).flatten();
    if let Some(flight) = flight {
        let end_number = last_number.unwrap_or_default();
        let consensus_bus = consensus_bus.clone();
        let network = network.clone();
        let consensus_chain = consensus_chain.clone();
        let walk_tracker = walk_tracker.clone();
        task_spawner.spawn_task(format!("partial pack catchup epoch {epoch}"), async move {
            // Bulk fast-path: if we're far behind on the in-progress current epoch, stream a verified
            // partial pack into staging in one shot instead of only crawling headers.
            let caught_up = try_partial_pack_catch_up(
                &consensus_bus,
                &network,
                &consensus_chain,
                &walk_tracker,
                epoch,
                number,
                hash,
            )
            .await;
            // the gate tracks the partial pack attempt only, so a fallback walk below does not hold
            // off the next attempt
            drop(flight);
            if caught_up {
                return Ok(());
            }
            if consensus_chain.contains_decode_epoch(walk_epoch).await {
                info!(target: "state-sync", "Failed to initialize a bulk current epoch {epoch} download, falling back to backwards download");
                // If this fails then try to do "normal" backwards download.
                // Note we do this no matter the tasks count "for free"
                // This should be atypical and must happen and the task count is a loose metric
                // to avoid tasks running amock anyway.
                // Reserve this backwards header walk's range (the partial-pack path above reserves
                // its own) so a catch-up gap fill defers to it instead of racing it.
                let _guard = walk_tracker.reserve(end_number, number);
                get_consensus_header_range(
                    number,
                    hash,
                    &db_clone,
                    &consensus_bus_clone,
                    &network_clone,
                    &consensus_chain_clone,
                    end_number,
                )
                .await;
            } else {
                info!(target: "state-sync", epoch, number, walk_epoch,
                    "partial pack unavailable and no local pack decodes this epoch; next attempt in {PARTIAL_PACK_RETRY_SECS}s, or the full pack once the epoch closes");
            }
            Ok(())
        });
    } else if decodable && walk_tracker.active() < 6 {
        // A loose throttle: the ledger read can race a walk finishing, but one walk more or less
        // does not matter, and the reservation below still keeps this walk from racing another over
        // the same range.
        // Skip for now, this number will be subsumed by gossip once enough tasks end.
        let end_number = last_number.unwrap_or_default();
        *last_number = Some(number + 1);
        let guard = walk_tracker.reserve(end_number, number);
        task_spawner.spawn_task(
            format!("backfilling epoch {epoch} consensus from {number}/{hash} to {end_number}"),
            async move {
                let _guard = guard;
                get_consensus_header_range(
                    number,
                    hash,
                    &db_clone,
                    &consensus_bus_clone,
                    &network_clone,
                    &consensus_chain_clone,
                    end_number,
                )
                .await;
                Ok(())
            },
        );
    }

    if *current_fetch_epoch < epoch {
        // This will switch to pack download if we change
        // epochs. This will almost certainly be faster and more reliable...
        request_epochs(current_fetch_epoch, consensus_chain, consensus_bus).await
    }
}

/// Spawn a long running task (application scope) that will retrieve consensus headers when
/// requested.
pub async fn spawn_fetch_recent_consensus<DB: TNDatabase>(
    db: DB,
    consensus_bus: ConsensusBusApp,
    network: PrimaryNetworkHandle,
    consensus_chain: ConsensusChain,
    rx_shutdown: Noticer,
    task_spawner: TaskSpawner,
    mut rx_consensus_request: impl TnReceiver<(Epoch, u64, ConsensusHeaderDigest)>,
) {
    // Attempt to clear the consensus header cache on startup.
    // This should not really be needed (records are evicted as they are processed) but
    // should not hurt and can clear up an issue if something interferes with eviction.
    // Note, on longer shutdowns this will have no real effect but could lead to churn
    // if a node is being restarted relatively quickly.
    if let Err(e) = db.clear_table::<ConsensusCache>() {
        error!(target: "state-sync", ?e, "Error clearing consensus header cache, ignoring...");
    }
    // Get the epoch of our last executed consensus.
    let mut current_fetch_epoch = consensus_chain.latest_consensus_epoch();
    // Gates the current-epoch partial pack: always on the first gossip, then retried while no
    // local pack decodes the gossiped epoch.
    let mut partial_gate = PartialPackGate::default();
    let mut last_number = None;
    // One ledger for every fetch walk (bulk backfill, gossip backfill, gap fill): it throttles how
    // many run at once and lets a gap fill defer to a walk already covering its range instead of
    // racing a duplicate download.
    let walk_tracker = WalkTracker::default();
    let mut rx_consensus_gap = consensus_bus.consensus_gap_request().subscribe();
    // This loop will track current consensus as well as try to backfill from current.
    // This task backfills the current epoch records as well as requesting entire pack files
    // be downloaded for missing historic epochs.
    loop {
        tokio::select! {
            req = rx_consensus_request.recv() => {
                let Some((epoch, number, hash)) = req else {
                    // We lost our channel so shutdown- this is a critical task so node will stop.
                    return;
                };
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");
                manage_new_consensus(&db,
                    &consensus_bus,
                    &network,
                    &consensus_chain,
                    &task_spawner,
                    &walk_tracker,
                    epoch, number, hash,
                    &mut partial_gate,
                    &mut last_number,
                    &mut current_fetch_epoch,
                ).await;
            }

            // The observer catch-up loop stalled on a bottom gap the gossip-driven walk cannot
            // reach (its floor only ever rises). Fill exactly that range here, in the task that
            // owns the fetch accounting, walking back from the verified tip down to `floor`.
            gap = rx_consensus_gap.changed() => {
                if gap.is_err() {
                    // Bus dropped: the node is shutting down and this critical task ends with it.
                    return;
                }
                let request = *rx_consensus_gap.borrow_and_update();
                if let Some((epoch, number, hash, floor)) = request {
                    // Fill only a non-empty range that no walk already owns. `reserve_if_uncovered`
                    // returns `None` when a bulk/gossip/earlier-gap walk already covers this range
                    // (the stall is that walk still filling top-down, not an unfilled gap), which
                    // also subsumes the old single-flight guard. It reserves the range so a second
                    // stall coalesces, and the guard releases it on completion/panic/cancel.
                    if let Some(guard) =
                        (number >= floor).then(|| walk_tracker.reserve_if_uncovered(floor, number)).flatten()
                    {
                        let db = db.clone();
                        let consensus_bus = consensus_bus.clone();
                        let network = network.clone();
                        let consensus_chain = consensus_chain.clone();
                        task_spawner.spawn_task(
                            format!("gap-fill epoch {epoch} consensus {floor}..={number}"),
                            async move {
                                let _guard = guard;
                                get_consensus_header_range(
                                    number,
                                    hash,
                                    &db,
                                    &consensus_bus,
                                    &network,
                                    &consensus_chain,
                                    floor,
                                )
                                .await;
                                Ok(())
                            },
                        );
                    }
                }
            }

            _ = &rx_shutdown => {
                return;
            }
        }
    }
}

/// Send a request to stream any pack files that are missing or incomplete for any epoch records we
/// have. This should not be strictly needed but it can help with some wonky states to get synced
/// (was added in response to an early testnet freeze).
/// Note this just trigers a test and resync for any epoch pack files that are incomplete from our
/// current epoch.
pub async fn request_missing_packs(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) {
    // Get the epoch of our last executed consensus.
    let mut current_fetch_epoch = consensus_chain.latest_consensus_epoch();
    request_epochs(&mut current_fetch_epoch, consensus_chain, consensus_bus).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, VecDeque};
    use tempfile::TempDir;
    use tn_network_libp2p::types::NetworkCommand;
    use tn_primary::{
        network::{PrimaryRequest, PrimaryResponse},
        NodeMode, EPOCH_REQUEST_QUEUE_CAPACITY,
    };
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{
        CommittedSubDag, ConsensusOutput, EpochSeedChainValue, ReputationScores, ShutdownNotifier,
        TaskManager,
    };
    use tokio::sync::mpsc::{self, error::TryRecvError};

    type NetworkRx = mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>;

    /// A fresh chain whose only pack is the (current) epoch-0 pack.
    async fn test_chain(dir: &TempDir, fixture: &CommitteeFixture<MemDatabase>) -> ConsensusChain {
        ConsensusChain::new_for_test(dir.path().to_owned(), fixture.committee())
            .await
            .expect("test consensus chain")
    }

    /// A network handle plus its command receiver. Holding the receiver without answering makes
    /// every network call hang; dropping it makes every network call fail at once.
    fn test_network() -> (PrimaryNetworkHandle, NetworkRx) {
        let (tx, rx) = mpsc::channel(16);
        (PrimaryNetworkHandle::new_for_test(tx), rx)
    }

    /// Save the epoch-0 record ending at `final_number`, so every higher consensus number maps to
    /// epoch 1.
    async fn save_epoch0_record(
        chain: &ConsensusChain,
        final_number: u64,
        final_hash: ConsensusHeaderDigest,
    ) {
        let record = EpochRecord {
            epoch: 0,
            final_consensus: ConsensusNumHash::new(final_number, final_hash),
            ..EpochRecord::default()
        };
        chain.epochs().save_record(record).await.expect("save epoch 0 record");
    }

    /// Save chained outputs `1..=count` into the epoch-0 pack and return the last header's digest.
    async fn save_chained_outputs(
        chain: &ConsensusChain,
        fixture: &CommitteeFixture<MemDatabase>,
        count: u64,
    ) -> ConsensusHeaderDigest {
        let committee = fixture.committee();
        let genesis: BTreeSet<_> = fixture.genesis().collect();
        let (_, headers) = fixture.headers_round(0, &genesis);
        let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        let leader = certificates.last().cloned().expect("a leader certificate");
        let mut parent_hash = ConsensusHeaderDigest::default();
        for number in 1..=count {
            let sub_dag = CommittedSubDag::new(
                certificates.clone(),
                leader.clone(),
                number - 1,
                ReputationScores::new(&committee),
                None,
                EpochSeedChainValue::genesis_placeholder(),
            );
            let output =
                ConsensusOutput::new(sub_dag, parent_hash, number, false, VecDeque::new(), vec![]);
            parent_hash = output.consensus_header_hash();
            chain.save_consensus_output(output).await.expect("save consensus output");
        }
        chain.persist_current().await.expect("persist current pack");
        parent_hash
    }

    /// A backward walk from a height in an epoch that no local pack can decode must end at once
    /// without touching the network. `request_consensus_output` refuses such an epoch on every
    /// attempt, so before the fix the walk retried forever and its range reservation blocked
    /// every gap fill under it.
    #[tokio::test]
    async fn backward_walk_aborts_on_undecodable_epoch() {
        let dir = TempDir::new().expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain = test_chain(&dir, &fixture).await;
        // epoch 0 ends at 3, so 10 is in epoch 1, which has no local pack
        save_epoch0_record(&chain, 3, ConsensusHeaderDigest::default()).await;
        // never answered: a network call would hang the walk
        let (network, mut rx) = test_network();
        let db = MemDatabase::default();
        let consensus_bus = ConsensusBusApp::new();

        let walk = get_consensus_header_range(
            10,
            ConsensusHeaderDigest::default(),
            &db,
            &consensus_bus,
            &network,
            &chain,
            0,
        );
        tokio::time::timeout(Duration::from_secs(1), walk)
            .await
            .expect("a walk over an undecodable epoch must return");
        assert!(
            matches!(rx.try_recv(), Err(TryRecvError::Empty)),
            "the walk must not issue a network command"
        );
    }

    /// The partial pack gate lets the first attempt through, holds every other attempt while one
    /// is in flight, and after that waits out the fixed backoff measured from when the previous
    /// attempt started.
    #[test]
    fn partial_pack_gate_retries_after_backoff() {
        let mut gate = PartialPackGate::default();
        let t0 = Instant::now();
        let backoff = Duration::from_secs(PARTIAL_PACK_RETRY_SECS);
        assert!(gate.is_first());

        let flight = gate.try_begin(t0).expect("the first attempt always starts");
        assert!(!gate.is_first());
        assert!(gate.try_begin(t0).is_none(), "no second attempt while one is in flight");
        assert!(
            gate.try_begin(t0 + backoff).is_none(),
            "no second attempt while one is in flight, even past the backoff"
        );

        drop(flight);
        assert!(gate.try_begin(t0 + Duration::from_secs(10)).is_none(), "inside the backoff");
        let retry = gate.try_begin(t0 + backoff).expect("a retry starts once the backoff passes");
        assert!(gate.try_begin(t0 + backoff * 3).is_none(), "the retry is in flight");
        drop(retry);
    }

    /// Gossip for an epoch that no local pack decodes tries the partial pack and, when no peer
    /// serves it, starts no backward walk: such a walk could fetch nothing, and before the fix its
    /// `last_number` bump (to 12 on the second gossip here) left a hole and its reservation
    /// blocked gap fills. The failed attempt leaves nothing in flight and starts the backoff.
    #[tokio::test]
    async fn manage_new_consensus_skips_walk_for_undecodable_epoch() {
        let dir = TempDir::new().expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain = test_chain(&dir, &fixture).await;
        // epoch 0 ends at 3, so 10 and 11 are in epoch 1, which has no local pack
        save_epoch0_record(&chain, 3, ConsensusHeaderDigest::default()).await;
        // a dropped receiver makes every probe fail at once, as when no peer serves the pack
        let (network, rx) = test_network();
        drop(rx);
        let db = MemDatabase::default();
        let consensus_bus = ConsensusBusApp::new();
        consensus_bus.node_mode().send_replace(NodeMode::Observer);
        let task_manager = TaskManager::new("t");
        let spawner = task_manager.get_spawner();
        let walk_tracker = WalkTracker::default();
        let mut partial_gate = PartialPackGate::default();
        let mut last_number = None;
        // the epoch-0 record is already known, so no pack request is queued
        let mut current_fetch_epoch = 1;

        for number in [10, 11] {
            manage_new_consensus(
                &db,
                &consensus_bus,
                &network,
                &chain,
                &spawner,
                &walk_tracker,
                1,
                number,
                ConsensusHeaderDigest::default(),
                &mut partial_gate,
                &mut last_number,
                &mut current_fetch_epoch,
            )
            .await;
        }
        assert_eq!(last_number, None, "no walk may claim heights it cannot fetch");

        tokio::time::timeout(Duration::from_secs(5), async {
            while partial_gate.in_flight.load(Ordering::Acquire) || walk_tracker.active() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the failed attempt must end without leaving a walk behind");
        assert!(
            partial_gate.try_begin(Instant::now()).is_none(),
            "the failed attempt starts the retry backoff"
        );
    }

    /// An epoch whose pack is already complete locally is never fetched, so before the fix nothing
    /// raised the stream target for it and the observer replayed none of it until gossip or a
    /// later download did. Startup now seeds the target with the final header of the highest
    /// complete local epoch.
    #[tokio::test]
    async fn request_missing_packs_raises_target_for_complete_local_pack() {
        let dir = TempDir::new().expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain = test_chain(&dir, &fixture).await;
        let final_hash = save_chained_outputs(&chain, &fixture, 3).await;
        save_epoch0_record(&chain, 3, final_hash).await;
        let consensus_bus = ConsensusBusApp::new();

        request_missing_packs(&consensus_bus, &chain).await;

        let target = consensus_bus
            .last_consensus_header()
            .borrow()
            .clone()
            .expect("a complete local pack seeds the stream target");
        assert_eq!(target.number, 3);
        assert_eq!(target.digest(), final_hash);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                consensus_bus.get_next_epoch_pack_file_request()
            )
            .await
            .is_err(),
            "a complete local pack is not requested"
        );
    }

    /// An epoch whose local pack stops short of its final header is queued for download and does
    /// not raise the stream target; the fetcher raises it once the download lands.
    #[tokio::test]
    async fn request_epochs_queues_but_does_not_raise_for_incomplete_pack() {
        let dir = TempDir::new().expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain = test_chain(&dir, &fixture).await;
        save_chained_outputs(&chain, &fixture, 3).await;
        // the record ends the epoch at 5, past the last local output
        save_epoch0_record(&chain, 5, ConsensusHeaderDigest::default()).await;
        let consensus_bus = ConsensusBusApp::new();
        let mut current_fetch_epoch = chain.latest_consensus_epoch();

        request_epochs(&mut current_fetch_epoch, &chain, &consensus_bus).await;

        assert_eq!(current_fetch_epoch, 1, "every known record was visited");
        assert!(consensus_bus.last_consensus_header().borrow().is_none());
        let (_, record) = tokio::time::timeout(
            Duration::from_millis(100),
            consensus_bus.get_next_epoch_pack_file_request(),
        )
        .await
        .expect("the incomplete pack is requested")
        .expect("request queue open");
        assert_eq!(record.epoch, 0);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                consensus_bus.get_next_epoch_pack_file_request()
            )
            .await
            .is_err(),
            "exactly one request is queued"
        );
    }

    /// A fetch worker that gives up on an epoch while the request queue is full must keep taking
    /// requests. Before the fix it waited for room in its own queue; once every worker waited
    /// there nothing drained the queue, so every producer (node startup included) waited forever
    /// and shutdown could not stop the workers (#1563). One worker makes the outcome
    /// deterministic.
    #[tokio::test(start_paused = true)]
    async fn fetch_worker_keeps_consuming_after_giving_up_on_a_full_queue() {
        let dir = TempDir::new().expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain = test_chain(&dir, &fixture).await;
        // epoch 0 ends at 3, so every synthetic final number below maps to epoch 1, which has no
        // pack: the completeness check misses at once and the record refresh finds nothing
        save_epoch0_record(&chain, 3, ConsensusHeaderDigest::default()).await;
        let (network, mut rx) = test_network();
        let consensus_bus = ConsensusBusApp::new();
        let shutdown = ShutdownNotifier::new();
        let worker = tokio::spawn(spawn_fetch_consensus(
            shutdown.subscribe(),
            consensus_bus.clone(),
            network,
            0,
            chain.clone(),
        ));

        let record = |epoch: Epoch| EpochRecord {
            epoch,
            final_consensus: ConsensusNumHash::new(
                1_000 + u64::from(epoch),
                ConsensusHeaderDigest::default(),
            ),
            ..EpochRecord::default()
        };
        // the last send completes only once the worker has taken epoch 1, which leaves the queue
        // full with epochs 2..=capacity + 1
        let capacity =
            Epoch::try_from(EPOCH_REQUEST_QUEUE_CAPACITY).expect("the capacity fits an epoch");
        for epoch in 1..=capacity + 1 {
            consensus_bus.request_epoch_pack_file(EpochRecord::default(), record(epoch)).await;
        }

        // phase 1: fail all 101 attempts at epoch 1 by dropping each attempt's peer query. no
        // timer may be pending here: each retry refreshes the record through a background thread,
        // and a paused clock jumps to the next pending timer while the runtime waits on another
        // thread. the 100 backoffs add up to 2,800 virtual seconds
        for attempt in 1..=101 {
            let command = rx.recv().await.expect("network handle open");
            assert!(
                matches!(command, NetworkCommand::ConnectedPeers { .. }),
                "attempt {attempt} starts with a peer query"
            );
        }

        // phase 2: having given up on epoch 1, the worker goes on to epoch 2
        let command = tokio::time::timeout(Duration::from_secs(600), rx.recv())
            .await
            .expect("fetch worker stuck re-queueing into its own full queue (#1563)")
            .expect("network handle open");
        assert!(
            matches!(command, NetworkCommand::ConnectedPeers { .. }),
            "epoch 2's first attempt starts with a peer query"
        );

        // phase 3: shutdown stops the worker while epoch 2's peer query is still unanswered
        shutdown.notify();
        tokio::time::timeout(Duration::from_secs(600), worker)
            .await
            .expect("the worker stops on shutdown")
            .expect("the worker does not panic");
        drop(command);

        // phase 4: epoch 1 went back on the queue once, behind everything queued before it
        let mut epochs = Vec::new();
        while let Ok(request) = tokio::time::timeout(
            Duration::from_millis(100),
            consensus_bus.get_next_epoch_pack_file_request(),
        )
        .await
        {
            let (_, record) = request.expect("request queue open");
            epochs.push(record.epoch);
        }
        assert_eq!(epochs.last(), Some(&1), "the given-up epoch drains last");
        assert_eq!(
            epochs.iter().filter(|epoch| **epoch == 1).count(),
            1,
            "the given-up epoch is queued once"
        );
    }

    /// The shared walk ledger is what keeps a catch-up gap fill from racing a walk that already
    /// covers its range: it defers while such a walk is in flight, still fires for a range no walk
    /// reaches, coalesces a repeated request, and — because a reservation is released on `Drop` —
    /// re-drives once a covering walk ends (so a crashed walk cannot wedge recovery).
    #[test]
    fn walk_tracker_defers_only_to_a_covering_walk() {
        let tracker = WalkTracker::default();
        assert_eq!(tracker.active(), 0);

        // A bulk backfill claims the whole catch-up range [0..=100].
        let backfill = tracker.reserve(0, 100);
        assert_eq!(tracker.active(), 1);

        // A stall at [1..=100] while that backfill is still filling top-down is a false alarm:
        // the range is fully covered, so the gap fill must defer (add no walk).
        assert!(tracker.reserve_if_uncovered(1, 100).is_none());
        assert_eq!(tracker.active(), 1, "a deferred gap fill must not spawn a walk");

        // A range extending above the covering walk's ceiling is not fully covered, so it fires.
        let above = tracker.reserve_if_uncovered(1, 150).expect("uncovered range must fire");
        assert_eq!(tracker.active(), 2);
        // A repeat of that request now coalesces onto the in-flight walk (old single-flight guard).
        assert!(tracker.reserve_if_uncovered(1, 150).is_none());
        assert_eq!(tracker.active(), 2);
        drop(above);
        assert_eq!(tracker.active(), 1);

        // When the covering backfill ends its range is released...
        drop(backfill);
        assert_eq!(tracker.active(), 0);

        // ...so the same gap now fires: a finished (or crashed) covering walk re-drives recovery.
        let gap = tracker.reserve_if_uncovered(1, 100).expect("uncovered gap must fire");
        assert_eq!(tracker.active(), 1);
        drop(gap);
        assert_eq!(tracker.active(), 0);
    }

    /// Coverage is a UNION test: during deep catch-up the gossip backfills tile the space, so a
    /// full-tip gap request must defer when the tiles TOGETHER cover it even though no single tile
    /// does, but still fire when the tiles leave a real gap.
    #[test]
    fn walk_tracker_defers_to_a_covering_union() {
        let tracker = WalkTracker::default();
        // Two tiles that together cover [0..=200] (the second floors at the first's ceil + 1).
        let lo = tracker.reserve(0, 100);
        let hi = tracker.reserve(101, 200);
        assert_eq!(tracker.active(), 2);

        // No single tile covers [50..=200], but their union does -> defer.
        assert!(
            tracker.reserve_if_uncovered(50, 200).is_none(),
            "a gap the tiles jointly cover must defer"
        );
        assert_eq!(tracker.active(), 2, "a deferred union gap must not spawn a walk");

        // A hole between the tiles means the union does NOT cover -> fire.
        drop(hi);
        let with_hole = tracker.reserve(150, 200); // now [0..=100] and [150..=200], gap 101..=149
        let fill = tracker
            .reserve_if_uncovered(50, 200)
            .expect("a range with an uncovered sub-range must fire");
        drop(fill);
        drop(with_hole);
        drop(lo);
        assert_eq!(tracker.active(), 0);
    }
}
