//! Tasks and helpers for collecting consensus headers and epoch pack files trustlessly.

use std::{
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
    time::Duration,
};

use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, tables::ConsensusHeaderCache};
use tn_types::{
    Database as TNDatabase, Epoch, EpochRecord, Noticer, TaskSpawner, TnReceiver, TnSender as _,
    B256,
};
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{debug, error, info, warn};

/// How long to wait before retrying a failed pack file download.
const PACK_DOWNLOAD_RETRY_SECS: u64 = 5;
const PACK_RECORD_TIMEOUT_SECS: u64 = 10;

/// Retrieve a consensus header from a peer.
/// If we are requesting a hash then that hash should
/// have already been "validated" so the only check we
/// make is that the returned header matches the hash.
async fn get_consensus_header<DB: TNDatabase>(
    number: u64,
    hash: B256,
    db: &DB,
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
) -> Option<(u64, B256)> {
    // Use the persisted ConsensusChain DB number as the cutoff, not the in-memory
    // recent_blocks tracker. The in-memory tracker can advance during a brief CvvActive
    // phase (local Bullshark commits) before the node transitions to CvvInactive, causing
    // the backward traversal to incorrectly skip blocks that haven't been fetched from
    // peers and stored in ConsensusHeaderCache yet.
    if number <= consensus_chain.latest_consensus_number() {
        return None;
    }
    if let Ok(Some(block)) = db.get::<ConsensusHeaderCache>(&number) {
        return if block.number > 0 { Some((block.number - 1, block.parent_hash)) } else { None };
    }
    // request consensus from any peer
    match network.request_consensus(number, hash).await {
        Ok(header) => {
            if let Err(e) = db.insert::<ConsensusHeaderCache>(&header.number, &header) {
                error!(target: "state-sync", ?e, "error saving a consensus header to cache storage!");
            }
            // The header we got will match hash (request_consensus() contract).
            let parent = header.parent_hash;
            let parent_number = header.number - 1;
            let last_seen_header_number = consensus_bus
                .last_consensus_header()
                .borrow()
                .as_ref()
                .map(|h| h.number)
                .unwrap_or_default();
            if header.number > last_seen_header_number {
                // Update our last seen valid consensus header if it is newer.
                consensus_bus.last_consensus_header().send_replace(Some(header));
            }
            Some((parent_number, parent))
        }
        Err(e) => {
            warn!(
                target: "tn::observer",
                %e,
                ?hash,
                ?number,
                "failed to fetch consensus header from peer"
            );
            // Return the failed data so we try again.
            // We will not be able to progress without this info so pause and keep trying...
            tokio::time::sleep(Duration::from_secs(5)).await;
            Some((number, hash))
        }
    }
}

/// Attempt to request epoch packs for every epoch from current_fetch_epoch to latest epoch
/// record.
async fn request_epochs(
    current_fetch_epoch: &mut Epoch,
    consensus_chain: &ConsensusChain,
    consensus_bus: &ConsensusBusApp,
    last_gossipped_epoch: Option<Epoch>,
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
            if epoch_record.epoch < last_gossipped_epoch.unwrap_or(u32::MAX) {
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
            } else {
                break;
            }
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
    async fn next_epoch<'s>(
        consensus_bus: &ConsensusBusApp,
        next_sem: &'s Arc<Semaphore>,
    ) -> Option<(SemaphorePermit<'s>, EpochRecord, EpochRecord)> {
        let permit = next_sem.acquire().await.ok()?;
        consensus_bus.get_next_epoch_pack_file_request().await.map(|(pe, e)| (permit, pe, e))
    }
    // When can we accept more work (a new epoch).
    let next_sem = Arc::new(Semaphore::new(1));
    // Get the epoch of our last executed consensus.
    loop {
        tokio::select! {
            Some((_permit, previous_epoch_record, mut epoch_record)) = next_epoch(&consensus_bus, &next_sem) => {
                let epoch = epoch_record.epoch;
                if consensus_chain.already_streaming_epoch(epoch) || consensus_chain.is_epoch_complete(&epoch_record).await {
                    // If we have already streamed this epoch or are in process of streaming then continue.
                    // Note, it is a lot less complex to do this check here than to make sure we don't request
                    // the same pack more than once so do it this way.
                    continue;
                }
                info!(target: "state-sync", "epoch consensus fetcher {task_index} retrieving epoch {epoch}");
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
                                            let current_last = consensus_bus
                                                .last_consensus_header()
                                                .borrow()
                                                .as_ref()
                                                .map(|h| h.number)
                                                .unwrap_or_default();
                                            if final_header.number > current_last {
                                                info!(target: "state-sync",
                                                    epoch = epoch_record.epoch,
                                                    final_header_number = final_header.number,
                                                    "epoch pack downloaded, signaling stream to process locally available blocks");
                                                consensus_bus.last_consensus_header().send_replace(Some(final_header));
                                            }
                                        }
                                        Ok(None) => error!(target: "state-sync",
                                            epoch = epoch_record.epoch,
                                            "Unable to find header by number for new pack file"),
                                        Err(e) => error!(target: "state-sync",
                                            epoch = epoch_record.epoch,
                                            ?e,
                                            "Unable to find header by number for new pack file"),
                                    }
                                    break;
                                }
                                Err(e) => {
                                    error!(target: "state-sync",
                                        "failed to request epoch pack for epoch {epoch}, attempt {attempts}: {e}");
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
                                }
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
    hash: B256,
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
    while let Some((next_number, next_hash)) =
        get_consensus_header(number, hash, db, consensus_bus, network, consensus_chain).await
    {
        number = next_number;
        hash = next_hash;
        if number < end_number {
            break;
        }
        if consensus_chain.consensus_header_by_number(number).await.unwrap_or_default().is_some() {
            // The next number showed up in a pack file.  We don't need to continue this download...
            break;
        }
        if count % 10 == 0 {
            info!(target: "state-sync", ?number, ?hash, ?end_number, "fetching consensus from peer");
        }
        count += 1;
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
    tasks: Arc<AtomicI32>,
    epoch: Epoch,
    number: u64,
    hash: B256,
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
        *last_number = Some(number + 1);
        let tasks_clone = tasks.clone();
        // Start one backtracking fetch at the first consensus we get.
        tasks.store(1, Ordering::Relaxed);
        task_spawner.spawn_task(
            format!("backfilling epoch {epoch} consensus from {number}/{hash}"),
            async move {
                get_consensus_header_range(
                    number,
                    hash,
                    &db_clone,
                    &consensus_bus_clone,
                    &network_clone,
                    &consensus_chain_clone,
                    0,
                )
                .await;
                tasks_clone.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            },
        );
    } else {
        // Note that the way tasks is used is open to "races" but this is a simple throttle for not
        // firing too many fetch tasks so not worth the overhead of using a full lock here.  I.e.
        // one more or less task won't matter.
        let task_num = tasks.load(Ordering::Relaxed);
        // Skip for now, this number will be subsumed by gossip once enough tasks end.
        if task_num < 6 {
            let end_number = last_number.unwrap_or_default();
            *last_number = Some(number + 1);
            let tasks_clone = tasks.clone();
            tasks.fetch_add(1, Ordering::Relaxed);
            task_spawner.spawn_task(
                format!("backfilling epoch {epoch} consensus from {number}/{hash} to {end_number}"),
                async move {
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
                    tasks_clone.fetch_sub(1, Ordering::Relaxed);
                    Ok(())
                },
            );
        }
    }

    if *current_fetch_epoch < epoch {
        // Use Some(epoch) instead of start_gossipped_epoch- switch to pack download if we change
        // epochs. This will almost certainly be faster and more reliable...
        request_epochs(current_fetch_epoch, consensus_chain, consensus_bus, Some(epoch)).await
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
    mut rx_consensus_request: impl TnReceiver<(Epoch, u64, B256)>,
) {
    // Attempt to clear the consensus header cache on startup.
    // This should not really be needed (records are evicted as they are processed) but
    // should not hurt and can clear up an issue if something interferes with eviction.
    // Note, on longer shutdowns this will have no real effect but could lead to churn
    // if a node is being restarted relatively quickly.
    if let Err(e) = db.clear_table::<ConsensusHeaderCache>() {
        error!(target: "state-sync", ?e, "Error clearing consensus header cache, ignoring...");
    }
    // Get the epoch of our last executed consensus.
    let mut current_fetch_epoch = consensus_chain.latest_consensus_epoch();
    let mut first_gossipped_epoch = None; // Track the first epoch we see via gossip.
    let mut last_number = None;
    let tasks = Arc::new(AtomicI32::new(0));
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
                    tasks.clone(),
                    epoch, number, hash,
                    &mut first_gossipped_epoch,
                    &mut last_number,
                    &mut current_fetch_epoch,
                ).await;
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
pub async fn request_missing_packs(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) {
    // If we have any epoch records with missing or incomplete pack files then request the pack
    // files.
    let mut epoch_rec = consensus_chain.epochs().latest_record().await;
    let mut first_missing = None;
    while let Some(rec) = epoch_rec {
        let has_final = consensus_chain.is_epoch_complete(&rec).await;
        epoch_rec = if !has_final {
            first_missing = Some(rec.epoch);
            consensus_chain.epochs().get_epoch_by_hash(rec.parent_hash).await.map(|r| r.0)
        } else {
            None
        };
    }
    // Get the epoch of our last executed consensus.
    if let Some(mut first_missing) = first_missing {
        request_epochs(&mut first_missing, consensus_chain, consensus_bus, None).await
    }
}
