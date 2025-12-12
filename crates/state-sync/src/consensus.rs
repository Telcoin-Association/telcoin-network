//! Tasks and helpers for collecting consensus headers trustlessly.

use std::sync::Arc;

use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache, EpochRecords},
    ConsensusStore,
};
use tn_types::{Database as TNDatabase, DbTxMut as _, Epoch, EpochRecord, TaskSpawner, B256};
use tokio::sync::{mpsc::Receiver, Mutex, Semaphore, SemaphorePermit};
use tracing::{debug, error, info};

/// Retrieve a consensus header from a peer.
/// If we are requesting a hash then that hash should
/// have already been "validated" so the only check we
/// make is that the returned header matches the hash.
async fn get_consensus_header<DB: TNDatabase>(
    number: Option<u64>,
    hash: B256,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
) -> Option<(Epoch, u64, B256)> {
    let db = config.node_storage();
    if let Some(number) = number {
        // If we have already processed consensus block number then stop.
        if let Ok(Some(_block)) = db.get::<ConsensusBlocks>(&number) {
            return None;
        }
    }
    if let Some(block) = db.get_consensus_by_hash(hash) {
        return Some((block.sub_dag.leader_epoch(), block.number - 1, block.parent_hash));
    }
    // request consensus from any peer
    match network.request_consensus(None, Some(hash)).await {
        Ok(header) => {
            // The header we got will match hash (request_consensus() contract).
            let parent = header.parent_hash;
            match db.write_txn() {
                Ok(mut txn) => {
                    if let Err(e) = txn.insert::<ConsensusBlocksCache>(&header.number, &header) {
                        error!(target: "state-sync", ?e, "error saving a consensus header to persistant storage!");
                    }
                    if let Err(e) = txn
                        .insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
                    {
                        error!(target: "state-sync", ?e, "error saving a consensus header number to persistant storage!");
                    }
                    if let Err(e) = txn.commit() {
                        error!(target: "state-sync", ?e, "error saving committing to persistant storage!");
                    }
                }
                Err(e) => {
                    error!(target: "state-sync", ?e, "error getting a transaction on persistant storage!");
                }
            }
            let parent_number = header.number - 1;
            let epoch = header.sub_dag.leader_epoch();
            let last_seen_header_number = consensus_bus
                .last_consensus_header()
                .borrow()
                .as_ref()
                .map(|h| h.number)
                .unwrap_or_default();
            if header.number > last_seen_header_number {
                // Update our last seen valid consensus header if it is newer.
                let _ = consensus_bus.last_consensus_header().send(Some(header));
            }
            Some((epoch, parent_number, parent))
        }
        Err(e) => {
            error!(target: "state-sync", ?e, "error requesting consensus!");
            None
        }
    }
}

/// Spawn a long running task on task_manager that will keep the last_consensus_header watch on
/// consensus_bus up to date. This should only be used when NOT participating in active consensus.
pub(crate) async fn spawn_track_recent_consensus<DB: TNDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    task_spawner: TaskSpawner,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let mut started_chain = false;
    let (tx, mut rx) = tokio::sync::mpsc::channel(10_000);
    let db = config.node_storage().clone();
    // Get the epoch of our last executed consensus.
    let mut current_fetch_epoch =
        if let Some(block) = consensus_bus.last_executed_consensus_block(&db) {
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
            ),
        );
    }
    // This loop will track current consensus as well as try to backfill from current.
    // The spawned workers above will try to fetch consensus for previous epochs in
    // parrellel starting with earliest so we can start executing sooner.
    loop {
        tokio::select! {
            _ = rx_gossip_update.changed() => {
                let (number, hash) = *rx_gossip_update.borrow_and_update();
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");

                if let Some(next) = get_consensus_header(Some(number), hash, &config, &consensus_bus, &network).await {
                    if current_fetch_epoch < next.0 {
                        // If we still have epochs to fetch then add to the queue until we are out of epoch records.
                        while let Ok(Some(epoch_record)) = db.get::<EpochRecords>(&current_fetch_epoch) {
                            let _ = epochs_tx.send(epoch_record).await;
                            current_fetch_epoch += 1;
                        }
                    }
                    // Once we start fetching previous consensus output don't keep doing that.
                    // Should be self-sustaining once started.
                    if !started_chain {
                        let _ = tx.send(next).await;
                        started_chain = true;
                    }
                }
            }

            Some((_, number, hash)) = rx.recv() => {
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");

                if let Some(next) = get_consensus_header(Some(number), hash, &config, &consensus_bus, &network).await {
                    let _ = tx.send(next).await;
                }
            }

            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Spawn a long running task on task_manager that will keep fetch consensus for an epoch.
/// This should only be used when NOT participating in active consensus.
async fn spawn_fetch_consensus<DB: TNDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    epoch_queue: Arc<Mutex<Receiver<EpochRecord>>>,
    worker: u32, // Worker number for logging.
) -> eyre::Result<()> {
    async fn next_epoch<'s>(
        epoch_queue: &Arc<Mutex<Receiver<EpochRecord>>>,
        next_sem: &'s Arc<Semaphore>,
    ) -> Option<(SemaphorePermit<'s>, EpochRecord)> {
        let permit = next_sem.acquire().await.ok()?;
        epoch_queue.lock().await.recv().await.map(|e| (permit, e))
    }
    let rx_shutdown = config.shutdown().subscribe();
    let mut epoch = 0;
    let mut log_counter = 0;
    let (tx, mut rx) = tokio::sync::mpsc::channel(10_000);
    // When can we accept more work (a new epoch).
    let next_sem = Arc::new(Semaphore::new(1));
    let mut next_permit = None;
    // Get the epoch of our last executed consensus.
    loop {
        tokio::select! {
            Some((permit, epoch_record)) = next_epoch(&epoch_queue, &next_sem) => {
                epoch = epoch_record.epoch;
                info!(target: "state-sync", "epoch consensus fetcher {worker} retreiving epoch {epoch}");
                let _ = tx.send((None, epoch_record.parent_consensus)).await;
                next_permit = Some(permit);
            }
            Some((number, hash)) = rx.recv() => {
                debug!(target: "state-sync", ?epoch, ?hash, "tracking recent consensus for an epoch - requesting consensus from peer");
                if log_counter > 98 {
                    // Log an info every 100 consensus blocks to show progress but not fill the logs.
                    info!(target: "state-sync", ?number, ?epoch, ?hash, "tracking recent consensus for an epoch - requesting consensus from peer");
                    log_counter = 0;
                } else {
                    log_counter += 1;
                }

                if let Some((new_epoch, num, hash)) = get_consensus_header(number, hash, &config, &consensus_bus, &network).await {
                    if new_epoch == epoch {
                        // Stop once we reach another epoch.
                        let _ = tx.send((Some(num), hash)).await;
                    } else {
                        info!(target: "state-sync", ?new_epoch, ?epoch, "tracking recent consensus for an epoch - changed epochs, finished");
                        // We are done so can accept a new epoch.
                        next_permit.take();
                    }
                } else {
                    info!(target: "state-sync", ?epoch, "tracking recent consensus for an epoch - out of consensus, finished");
                    // We are done so can accept a new epoch.
                    next_permit.take();
                }
            }

            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}
