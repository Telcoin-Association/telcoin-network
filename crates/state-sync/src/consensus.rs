//! Tasks and helpers for collecting consensus headers trustlessly.

use std::sync::Arc;

use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, tables::ConsensusHeaderCache};
use tn_types::{Database as TNDatabase, Epoch, EpochRecord, TaskSpawner, B256};
use tokio::sync::{mpsc::Receiver, Mutex, Semaphore, SemaphorePermit};
use tracing::{debug, error, info, warn};

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
    task_spawner: TaskSpawner,
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let (tx, mut rx) = tokio::sync::mpsc::channel(10_000);
    // Get the epoch of our last executed consensus.
    let mut current_fetch_epoch =
        if let Some(block) = consensus_bus.last_executed_consensus_block(&consensus_chain).await {
            block.sub_dag.leader_epoch()
        } else {
            0
        };
    let (epochs_tx, epochs_rx) = tokio::sync::mpsc::channel(10_000);
    let epoch_queue = Arc::new(Mutex::new(epochs_rx));
    // spawn four critical workers that will fetch consensus outputs from an epoch work queue.
    // Note, these workers will just go dormant once we have caught up- that's ok.
    for i in 0..4 {
        task_spawner.spawn_critical_task(
            format!("epoch-consensus-worker-{i}"),
            spawn_fetch_consensus(
                config.clone(),
                consensus_bus.clone(),
                network.clone(),
                epoch_queue.clone(),
                i,
                consensus_chain.clone(),
            ),
        );
    }
    // This loop will track current consensus as well as try to backfill from current.
    // The spawned workers above will try to fetch consensus for previous epochs in
    // parrallel starting with earliest so we can start executing sooner.
    let mut last_gossipped_epoch = None;
    loop {
        tokio::select! {
            _ = rx_gossip_update.changed() => {
                let (number, hash) = *rx_gossip_update.borrow_and_update();
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");
                if last_gossipped_epoch.is_none() {
                    last_gossipped_epoch = Some(consensus_chain.epochs().number_to_epoch(number))
                }

                if let Some(next) = get_consensus_header(number, hash, &config, &consensus_bus, &network, &consensus_chain).await {
                    if current_fetch_epoch < next.0 {
                        // If we still have epochs to fetch then add to the queue until we are out of epoch records.
                        while let Some(epoch_record) = consensus_chain.epochs().record_by_epoch(current_fetch_epoch).await {
                            if epoch_record.epoch < last_gossipped_epoch.unwrap_or_default() {
                                let _ = epochs_tx.send(epoch_record).await;
                                current_fetch_epoch += 1;
                            } else {
                                break;
                            }
                        }
                    }
                    // Each gossip event starts a backward traversal from the new tip.
                    // The traversal terminates naturally when it reaches an already-executed
                    // or already-cached block, ensuring new consensus blocks are always cached.
                    let _ = tx.send(next).await;
                }
            }

            Some((epoch, number, hash)) = rx.recv() => {
                if let Some(last_gossipped_epoch) = last_gossipped_epoch {
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

/// Spawn a long running task on task_manager that will fetch consensus for an epoch.
/// This should only be used when NOT participating in active consensus.
/// This works by stream an entire epochs pack file from a peer.
async fn spawn_fetch_consensus<DB: TNDatabase>(
    config: ConsensusConfig<DB>,
    _consensus_bus: ConsensusBusApp,
    network: PrimaryNetworkHandle,
    epoch_queue: Arc<Mutex<Receiver<EpochRecord>>>,
    worker: u32, // Worker number for logging.
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
    async fn next_epoch<'s>(
        epoch_queue: &Arc<Mutex<Receiver<EpochRecord>>>,
        next_sem: &'s Arc<Semaphore>,
    ) -> Option<(SemaphorePermit<'s>, EpochRecord)> {
        let permit = next_sem.acquire().await.ok()?;
        epoch_queue.lock().await.recv().await.map(|e| (permit, e))
    }
    let rx_shutdown = config.shutdown().subscribe();
    // When can we accept more work (a new epoch).
    let next_sem = Arc::new(Semaphore::new(1));
    // Get the epoch of our last executed consensus.
    loop {
        tokio::select! {
            Some((_permit, epoch_record)) = next_epoch(&epoch_queue, &next_sem) => {
                let epoch = epoch_record.epoch;
                if let Some((previous_epoch, _)) = consensus_chain.epochs().get_epoch_by_number(epoch).await {
                    info!(target: "state-sync", "epoch consensus fetcher {worker} retreiving epoch {epoch}");
                    loop {
                        match network.request_epoch_pack(
                            epoch_record.clone(),
                            previous_epoch.clone(),
                            &consensus_chain,
                        ).await {
                            Ok(_) => break,
                            Err(e) => {
                                error!(target: "state-sync", "failed to request epoch pack for epoch {epoch}: {e}");
                            }
                        }
                    }
                } else {
                    error!(target: "state-sync", "unable to find previous epoch for epoch {epoch}");
                }
            }
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}
