//! Code to sync consensus state between peers.
//! Currently used by nodes that are not participating in consensus
//! to follow along with consensus and execute blocks.

use consensus_metrics::monitored_future;
use tn_config::ConsensusConfig;
use tn_primary::{
    consensus::ConsensusRound, network::PrimaryNetworkHandle, ConsensusBus, NodeMode,
};
use tn_storage::{
    tables::{
        Batches, ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache,
        NodeBatchesCache,
    },
    ConsensusStore,
};
use tn_types::{
    AuthorityIdentifier, ConsensusHeader, ConsensusOutput, Database, DbTxMut, TaskSpawner, TnSender,
};
use tracing::{debug, error, info};

mod epoch;
pub use epoch::spawn_epoch_record_collector;
mod consensus;
use consensus::spawn_track_recent_consensus;

/// Sets some bus defaults.
/// Call this somewhere when starting an epoch.
pub async fn prime_consensus<DB: Database>(
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
) {
    // Get the DB and load our last executed consensus block (note there may be unexecuted
    // blocks, catch up will execute them).
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, config).unwrap_or_default();

    let current_epoch = config.epoch();

    // check if the latest subdag is from the current epoch
    // this function is called at startup and at each epoch boundary
    let last_subdag = &last_executed_block.sub_dag;
    let last_consensus_round = if last_subdag.leader_epoch() < current_epoch {
        // new epoch
        0
    } else {
        // node recovery
        last_subdag.leader_round()
    };

    let _ = consensus_bus.update_consensus_rounds(ConsensusRound::new_with_gc_depth(
        last_consensus_round,
        config.parameters().gc_depth,
    ));
    let _ = consensus_bus.primary_round_updates().send(last_consensus_round);
}

/// Spawn the state sync tasks.
pub fn spawn_state_sync<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    task_manager: TaskSpawner,
) {
    let mode = *consensus_bus.node_mode().borrow();
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {}
        NodeMode::CvvInactive | NodeMode::Observer => {
            // If we are not an active CVV then follow latest consensus from peers.
            let (config_clone, consensus_bus_clone) = (config.clone(), consensus_bus.clone());
            task_manager.spawn_task(
                "state sync: track latest consensus header from peers",
                monitored_future!(
                    async move {
                        info!(target: "state-sync", "Starting state sync: track latest consensus header from peers");
                        if let Err(e) = spawn_track_recent_consensus(config_clone, consensus_bus_clone, network).await {
                            error!(target: "state-sync", "Error tracking latest consensus headers: {e}");
                        }
                    },
                    "StateSyncLatestConsensus"
                ),
            );
            task_manager.spawn_task(
                "state sync: stream consensus headers",
                monitored_future!(
                    async move {
                        info!(target: "state-sync", "Starting state sync: stream consensus header from peers");
                        if let Err(e) = spawn_stream_consensus_headers(config, consensus_bus).await {
                            error!(target: "state-sync", "Error streaming consensus headers: {e}");
                        }
                    },
                    "StateSyncStreamConsensusHeaders"
                ),
            );
        }
    }
}

/// Write the consensus header and it's component transaction batches to the consensus DB.
///
/// An error here indicates a critical node failure.
/// Note, if this returns an error then the DB could not be written to- this is probably fatal.
pub fn save_consensus<DB: Database>(
    db: &DB,
    consensus_output: ConsensusOutput,
    authority_id: &Option<AuthorityIdentifier>,
) -> eyre::Result<()> {
    match db.write_txn() {
        Ok(mut txn) => {
            for certified_batches in consensus_output.batches.iter() {
                for batch in certified_batches.batches.iter() {
                    if let Err(e) = txn.insert::<Batches>(&batch.digest(), batch) {
                        error!(target: "state-sync", ?e, "error saving a batch to persistant storage!");
                        return Err(e);
                    }
                }
            }
            let header: ConsensusHeader = consensus_output.into();
            if let Err(e) = txn.insert::<ConsensusBlocks>(&header.number, &header) {
                error!(target: "state-sync", ?e, "error saving a consensus header to persistant storage!");
                return Err(e);
            }
            if let Err(e) =
                txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
            {
                error!(target: "state-sync", ?e, "error saving a consensus header number to persistant storage!");
                return Err(e);
            }
            // In case this was cached remove it.
            let _ = txn.remove::<ConsensusBlocksCache>(&header.number);
            if let Some(authority_id) = authority_id {
                // If we are a validator we need to clear any of our batches from our cache that are
                // now part of consesnus.
                for cert in &header.sub_dag.certificates {
                    if cert.header().author() == authority_id {
                        for batch_hash in cert.header().payload().keys() {
                            let _ = txn.remove::<NodeBatchesCache>(batch_hash);
                        }
                    }
                }
            }
            if let Err(e) = txn.commit() {
                error!(target: "state-sync", ?e, "error saving committing to persistant storage!");
                return Err(e);
            }
        }
        Err(e) => {
            error!(target: "state-sync", ?e, "error getting a transaction on persistant storage!");
            return Err(e);
        }
    }
    Ok(())
}

/// Returns the ConsensusHeader that created the last executed block if can be found.
/// If we are not starting at genesis or a new epoch, then not finding this indicates a database
/// issue.
pub fn last_executed_consensus_block<DB: Database>(
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
) -> Option<ConsensusHeader> {
    let db = config.node_storage();
    let last = consensus_bus
        .recent_blocks()
        .borrow()
        .latest_block()
        .header()
        .parent_beacon_block_root
        .and_then(|hash| db.get_consensus_by_hash(hash));

    debug!(target: "state-sync", ?last, epoch=?config.epoch(), "last executed consensus block");

    last
}

/// Send any consensus headers that were not executed before last shutdown to the consensus header
/// channel.
pub async fn stream_missing_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
) -> eyre::Result<()> {
    // Get the DB and load our last executed consensus block.
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, config).unwrap_or_default();
    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let db = config.node_storage();
    let (_, last_db_block) = db
        .last_record::<ConsensusBlocks>()
        .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));

    debug!(target: "state-sync", ?last_executed_block, ?last_db_block, "comparing last executed block and last recorded consensus block");

    // if the last recorded consensus block is larger than the last executed block,
    // forward the stored consensus block to engine for execution
    if last_db_block.number > last_executed_block.number {
        for consensus_block_number in last_executed_block.number + 1..=last_db_block.number {
            if let Some(consensus_header) = db.get_consensus_by_number(consensus_block_number) {
                debug!(target: "state-sync", ?consensus_header, "sending missed consensus block through consensus bus");
                consensus_bus.consensus_header().send(consensus_header).await?;
            }
        }
    }

    Ok(())
}

/// Collect and return any consensus headers that were not executed before last shutdown.
/// This will be consensus that was reached but had not executed before a shutdown.
pub async fn get_missing_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
) -> eyre::Result<Vec<ConsensusHeader>> {
    let mut result = Vec::new();
    // Get the DB and load our last executed consensus block.
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, config).unwrap_or_default();

    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let db = config.node_storage();
    let (_, last_db_block) = db
        .last_record::<ConsensusBlocks>()
        .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));

    debug!(target: "state-sync", ?last_executed_block, ?last_db_block, "comparing last executed block and last recorded consensus block");

    // if the last recorded consensus block is larger than the last executed block,
    // forward the stored consensus block to engine for execution
    if last_db_block.number > last_executed_block.number {
        for consensus_block_number in last_executed_block.number + 1..=last_db_block.number {
            if let Some(consensus_header) = db.get_consensus_by_number(consensus_block_number) {
                debug!(target: "state-sync", ?consensus_header, "collecting unexecuted consensus header");
                result.push(consensus_header);
            }
        }
    }

    debug!(target: "state-sync", ?result, "missing consensus headers that need execution:");
    Ok(result)
}

/// Spawn a long running task on task_manager that will stream consensus headers from the
/// last saved to the current and then keep up with current headers.
/// This should only be used when NOT participating in active consensus.
async fn spawn_stream_consensus_headers<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();

    let mut rx_last_consensus_header = consensus_bus.last_consensus_header().subscribe();
    let db = config.node_storage();
    let (_, mut last_consensus_header) =
        db.last_record::<ConsensusBlocks>().unwrap_or_else(|| (0, ConsensusHeader::default()));
    let mut last_consensus_height = last_consensus_header.number;

    // infinite loop over consensus output
    loop {
        tokio::select! {
            _ = rx_last_consensus_header.changed() => {
                let header = rx_last_consensus_header.borrow_and_update().clone();
                debug!(target: "state-sync", rx_last_consensus_header=?header.number, ?last_consensus_height, "streaming consensus headers detected change");

                if header.number > last_consensus_height {
                    last_consensus_header = catch_up_consensus_from_to(
                        &config,
                        &consensus_bus,
                        last_consensus_header,
                        header,
                    )
                    .await?;
                    last_consensus_height = last_consensus_header.number;
                }
            }
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Applies consensus output "from" (exclusive) to height "max_consensus_height" (inclusive).
/// Queries peers for latest height and downloads and executes any missing consensus output.
/// Returns the last ConsensusHeader that was applied on success.
async fn catch_up_consensus_from_to<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    from: ConsensusHeader,
    max_consensus: ConsensusHeader,
) -> eyre::Result<ConsensusHeader> {
    // Note use last_executed_block here because
    let mut last_parent = from.digest();

    // Catch up to the current chain state if we need to.
    let last_consensus_height = from.number;
    let max_consensus_height = max_consensus.number;
    if last_consensus_height >= max_consensus_height {
        return Ok(from);
    }
    let db = config.node_storage();
    let mut result_header = from;
    for number in last_consensus_height + 1..=max_consensus_height {
        debug!(target: "state-sync", "trying to get consensus block {number}");
        // Check if we already have this consensus output in our local DB.
        // This will also allow us to pre load other consensus blocks as a future
        // optimization.
        let consensus_header = if number == max_consensus_height {
            max_consensus.clone()
        } else if let Some(block) = db.get_consensus_by_number(number) {
            block
        } else {
            // We should have all the required headers in local storage by now...
            return Ok(result_header);
        };
        let parent_hash = last_parent;
        last_parent =
            ConsensusHeader::digest_from_parts(parent_hash, &consensus_header.sub_dag, number);
        if last_parent != consensus_header.digest() {
            error!(target: "state-sync", "consensus header digest mismatch!");
            return Err(eyre::eyre!("consensus header digest mismatch!"));
        }

        let base_execution_block = consensus_header.sub_dag.leader.header().latest_execution_block;
        // We need to make sure execution has caught up so we can verify we have not
        // forked. This will force the follow function to not outrun
        // execution...  this is probably fine. Also once we can
        // follow gossiped consensus output this will not really be
        // an issue (except during initial catch up).
        if consensus_bus.wait_for_execution(base_execution_block).await.is_err() {
            // We seem to have forked, so die.
            return Err(eyre::eyre!(
                "consensus_output has a parent not in our chain, missing {base_execution_block:?} recents: {:?}!",
                consensus_bus.recent_blocks().borrow()
            ));
        }
        consensus_bus.consensus_header().send(consensus_header.clone()).await?;
        result_header = consensus_header;
    }
    Ok(result_header)
}
