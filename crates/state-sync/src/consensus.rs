//! Tasks and helpers for collecting consensus headers trustlessly.

use std::{sync::Arc, time::Duration};

use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, tables::ConsensusHeaderCache};
use tn_types::{Database as TNDatabase, Epoch, EpochRecord, Noticer, B256};
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
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBusApp,
    network: &PrimaryNetworkHandle,
    consensus_chain: &ConsensusChain,
) -> Option<(Epoch, u64, B256)> {
    let db = config.node_storage();
    // Use the persisted ConsensusChain DB number as the cutoff, not the in-memory
    // recent_blocks tracker. The in-memory tracker can advance during a brief CvvActive
    // phase (local Bullshark commits) before the node transitions to CvvInactive, causing
    // the backward traversal to incorrectly skip blocks that haven't been fetched from
    // peers and stored in ConsensusHeaderCache yet.
    if number <= consensus_chain.latest_consensus_number() {
        return None;
    }
    if let Ok(Some(block)) = config.node_storage().get::<ConsensusHeaderCache>(&number) {
        return if block.number > 0 {
            Some((
                consensus_chain.epochs().number_to_epoch(block.number - 1),
                block.number - 1,
                block.parent_hash,
            ))
        } else {
            None
        };
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
            let epoch = consensus_chain.epochs().number_to_epoch(parent_number);
            Some((epoch, parent_number, parent))
        }
        Err(e) => {
            warn!(
                target: "tn::observer",
                %e,
                ?hash,
                ?number,
                "failed to fetch consensus header from peer"
            );
            None
        }
    }
}

/// Spawn a long running task on task_manager that will keep the last_consensus_header watch on
/// consensus_bus up to date. This should only be used when NOT participating in active consensus.
pub(crate) async fn spawn_track_recent_consensus<DB: TNDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBusApp,
    network: PrimaryNetworkHandle,
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
    /// Attempt to request epoch packs for every epoch from current_fetch_epoch to latest epoch
    /// record.
    async fn request_epochs(
        current_fetch_epoch: &mut Epoch,
        consensus_chain: &ConsensusChain,
        consensus_bus: &ConsensusBusApp,
        last_gossipped_epoch: Option<Epoch>,
    ) {
        // If we still have epochs to fetch then add to the queue until we are out of epoch records.
        let previous_epoch = current_fetch_epoch.saturating_sub(1);
        if let Some(mut previous_epoch_record) =
            consensus_chain.epochs().record_by_epoch(previous_epoch).await
        {
            while let Some(epoch_record) =
                consensus_chain.epochs().record_by_epoch(*current_fetch_epoch).await
            {
                *current_fetch_epoch += 1;
                if epoch_record.epoch < last_gossipped_epoch.unwrap_or_default() {
                    let contains_final_header = consensus_chain
                        .consensus_header_by_number(epoch_record.final_consensus.number)
                        .await
                        .unwrap_or_default()
                        .is_some();
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

    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let (tx, mut rx) = tokio::sync::mpsc::channel(10_000);
    // Get the epoch of our last executed consensus.
    let mut current_fetch_epoch = consensus_chain.latest_consensus_epoch();
    let mut last_gossipped_epoch = None;
    // This loop will track current consensus as well as try to backfill from current.
    // This task backfills the current epoch records as well as requesting entire pack files
    // be downloaded for missing historic epochs.
    loop {
        tokio::select! {
            _ = rx_gossip_update.changed() => {
                let (epoch, number, hash) = *rx_gossip_update.borrow_and_update();
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");
                if last_gossipped_epoch.is_none() {
                    last_gossipped_epoch = Some(epoch);
                }

                if current_fetch_epoch < epoch {
                    request_epochs(&mut current_fetch_epoch, &consensus_chain, &consensus_bus, last_gossipped_epoch).await
                }
                if let Some(next) = get_consensus_header(number, hash, &config, &consensus_bus, &network, &consensus_chain).await {
                    // Each gossip event starts a backward traversal from the new tip.
                    // The traversal terminates naturally when it reaches an already-executed
                    // or already-cached block, ensuring new consensus blocks are always cached.
                    let _ = tx.send(next).await;
                }
            }

            Some((epoch, number, hash)) = rx.recv() => {
                if let Some(last_gossipped_epoch) = last_gossipped_epoch {
                    // epochs before last_gossipped_epoch are retrieved as pack files
                    if epoch < last_gossipped_epoch {
                        continue;
                    }
                }
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");

                if let Some(next) = get_consensus_header(number, hash, &config, &consensus_bus, &network, &consensus_chain).await {
                    let _ = tx.send(next).await;
                }
                // else: chain ended (block already in DB or peer fetch failed);
                // next gossip event will start a new traversal from the latest tip.
            }

            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Spawn a long running task (application scoped)
/// that will fetch download entire pack files for epochs.
/// This should only be used when NOT participating in active consensus.
/// Several of these will run but will do nothing unless requested.
/// This works by streaming an entire epochs pack file from a peer.
pub async fn spawn_fetch_consensus(
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBusApp,
    network: PrimaryNetworkHandle,
    task_index: u32, // Task index for logging.
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
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
            Some((_permit, previous_epoch_record, epoch_record)) = next_epoch(&consensus_bus, &next_sem) => {
                let epoch = epoch_record.epoch;
                info!(target: "state-sync", "epoch consensus fetcher {task_index} retrieving epoch {epoch}");
                let mut attempts = 1;
                loop {
                    tokio::select! {
                        result = network.request_epoch_pack(&epoch_record, &previous_epoch_record, &consensus_chain, Duration::from_secs(PACK_RECORD_TIMEOUT_SECS)) => {
                            match result {
                                Ok(_) => break,
                                Err(e) => {
                                    error!(target: "state-sync",
                                        "failed to request epoch pack for epoch {epoch}, attempt {attempts}: {e}");
                                    // Wait a beat before we try again, may have a network issue.
                                    // Wait time will increase as attempts grow.
                                    tokio::select!(
                                        _ = &rx_shutdown => {
                                            info!(target: "state-sync",
                                                "epoch consensus fetcher {task_index} shutting down during pack fetch");
                                            return Ok(());
                                        },
                                        _ = tokio::time::sleep(Duration::from_secs(((attempts / 10) + 1) * PACK_DOWNLOAD_RETRY_SECS)) => { }
                                    );
                                }
                            }
                        }
                        _ = &rx_shutdown => {
                            info!(target: "state-sync",
                                "epoch consensus fetcher {task_index} shutting down during pack fetch");
                            return Ok(());
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
                return Ok(())
            }
        }
    }
}
