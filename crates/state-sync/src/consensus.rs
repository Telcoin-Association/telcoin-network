//! Tasks and helpers for collecting consensus headers and epoch pack files trustlessly.

use std::{sync::Arc, time::Duration};

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

enum ConsensusHeaderResult {
    Done,
    Continue(u64, ConsensusHeaderDigest),
    Retry,
}

/// A single consensus-height range `[floor..=ceil]` currently being walked by a fetch task.
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

    /// Reserve `[floor..=ceil]` only if no in-flight walk already covers all of it. Returns `None`
    /// when another walk already owns the range: the stall that prompted the request is that walk
    /// still filling top-down (the execute loop drains bottom-up, so it sees no progress until the
    /// walk reaches the bottom), not an unfilled gap, so we defer to it rather than duplicate the
    /// download.
    ///
    /// Deferring is safe even when the covering walk is stuck retrying a height rather than
    /// progressing: `get_consensus_output` fetches via `request_consensus_output`, which re-samples
    /// peers on every attempt, so a fresh walk would retry the exact same way and gain nothing over
    /// the retries the covering walk is already making. A covering walk that instead panics or is
    /// cancelled releases its reservation on `Drop`, so the next stall re-drives with a fresh walk.
    fn reserve_if_uncovered(&self, floor: u64, ceil: u64) -> Option<WalkGuard> {
        let mut ledger = self.inner.lock();
        ledger.ranges.iter().all(|r| !(r.floor <= floor && r.ceil >= ceil)).then(|| {
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

/// Retrieve a verified consensus OUTPUT (header + batches) for `number` and cache it.
///
/// `hash` is the already-verified consensus header digest for `number` — it comes from validated
/// consensus gossip (the tip) or from a verified descendant's `parent_hash`. We pull the full
/// output BYTES from a peer (`request_consensus_output`), decode them with the epoch committee, and
/// verify the decoded header's digest equals `hash` BEFORE caching. This upholds the invariant that
/// we never cache/execute an output that is not verified (directly by gossip, or as an ancestor of
/// one). The walk proceeds recent→earliest, so each cached entry is a verified output.
async fn get_consensus_output<DB: TNDatabase>(
    number: u64,
    hash: ConsensusHeaderDigest,
    db: &DB,
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
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
    // Pull the full output bytes from any peer.
    let bytes = match network.request_consensus_output(number).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(target: "tn::observer", %e, ?hash, ?number, "failed to fetch consensus output from peer");
            return ConsensusHeaderResult::Retry;
        }
    };
    // Decode with the epoch's committee (resolves cert authors -> execution addresses).
    let epoch = consensus_chain.epochs().number_to_epoch(number);
    let output = match consensus_chain.decode_consensus_output(epoch, bytes).await {
        Ok(output) => output,
        Err(e) => {
            // Includes the case where we do not yet hold this epoch's committee/pack.
            warn!(target: "tn::observer", ?e, ?number, "failed to decode consensus output, will retry");
            return ConsensusHeaderResult::Retry;
        }
    };
    let header = output.consensus_header();
    // VERIFY: the decoded output must match the already-verified hash. A peer that returns a
    // wrong/forked output is rejected here and never cached or executed.
    if header.digest() != hash {
        warn!(target: "tn::observer", ?number, expected = ?hash, got = ?header.digest(), "consensus output digest mismatch - rejecting");
        return ConsensusHeaderResult::Retry;
    }
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
    if let Some(mut previous_epoch_record) = maybe_previous {
        while let Some(epoch_record) =
            consensus_chain.epochs().record_by_epoch(*current_fetch_epoch).await
        {
            *current_fetch_epoch += 1;
            let contains_final_header = consensus_chain.is_epoch_complete(&epoch_record).await;
            // If the pack file is missing or incomplete request it.
            // Note since we have an epoch record this is a past epoch
            // not the current epoch.
            if !contains_final_header {
                consensus_bus
                    .request_epoch_pack_file(previous_epoch_record, epoch_record.clone())
                    .await;
            }
            previous_epoch_record = epoch_record;
        }
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
    // Get the epoch of our last executed consensus.
    loop {
        tokio::select! {
            Some((previous_epoch_record, mut epoch_record)) = consensus_bus.get_next_epoch_pack_file_request() => {
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
                        consensus_bus.request_epoch_pack_file(previous_epoch_record, epoch_record).await;
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
            _ = &rx_shutdown => {
                break;
            }
        }
    }
}

/// Retrieve a consensus headers from a peer.
/// Start at number/hash and work backwards to end number.
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
    loop {
        match get_consensus_output(number, hash, db, consensus_bus, network, consensus_chain).await
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
            }
            ConsensusHeaderResult::Done => break,
            ConsensusHeaderResult::Retry => {
                if retries < 5 {
                    retries += 1;
                }
                tokio::time::sleep(Duration::from_secs(retries)).await;
            }
        }
    }
}

/// Deal with an incoming consensus header request.
/// This exists to keep the select macro below smaller, hence the
/// large parameter list.
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
    first_gossipped_epoch: &mut Option<Epoch>,
    last_number: &mut Option<u64>,
    current_fetch_epoch: &mut Epoch,
) {
    let db_clone = db.clone();
    let consensus_bus_clone = consensus_bus.clone();
    let network_clone = network.clone();
    let consensus_chain_clone = consensus_chain.clone();
    if first_gossipped_epoch.is_none() {
        *first_gossipped_epoch = Some(epoch);
        // On the first epoch we will try to do partial pack download to build retrieve consensus.
        let end_number = last_number.unwrap_or_default();
        let consensus_bus = consensus_bus.clone();
        let network = network.clone();
        let consensus_chain = consensus_chain.clone();
        let walk_tracker = walk_tracker.clone();
        task_spawner.spawn_task(format!("partial pack catchup epoch {epoch}"), async move {
            // Bulk fast-path: if we're far behind on the in-progress current epoch, stream a verified
            // partial pack into staging in one shot instead of only crawling headers.
            if !try_partial_pack_catch_up(
                &consensus_bus,
                &network,
                &consensus_chain,
                epoch,
                number,
                hash,
            )
            .await
            {
                info!(target: "state-sync", "Failed to initialize a bulk current epoch {epoch} download, falling back to backwards download");
                // If this fails then try to do "normal" backwards download.
                // Note we do this no matter the tasks count "for free"
                // This should be atypical and must happen and the task count is a loose metric
                // to avoid tasks running amock anyway.
                // Register only this backwards header walk (not the partial-pack fast path, which
                // fills staging directly): the reservation then matches exactly the range this walk
                // fills top-down, so a catch-up gap fill defers to it instead of racing it.
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
            }
            Ok(())
        });
    } else {
        // A loose throttle: the ledger read can race a walk finishing, but one walk more or less
        // does not matter, and the reservation below still keeps this walk from racing another over
        // the same range.
        // Skip for now, this number will be subsumed by gossip once enough tasks end.
        if walk_tracker.active() < 6 {
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
    let mut first_gossipped_epoch = None; // Track the first epoch we see via gossip.
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
                    &mut first_gossipped_epoch,
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
}
