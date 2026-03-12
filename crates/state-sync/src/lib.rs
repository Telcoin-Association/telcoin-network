//! Code to sync consensus state between peers.
//! Currently used by nodes that are not participating in consensus
//! to follow along with consensus and execute blocks.

// Used in tests
#[cfg(test)]
use tempfile as _;
#[cfg(test)]
use tn_test_utils as _;
#[cfg(test)]
use tn_test_utils_committee as _;

use std::time::Duration;
use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus, NodeMode};
use tn_storage::{
    consensus::ConsensusChain,
    tables::{ConsensusHeaderCache, NodeBatchesCache},
};
use tn_types::{
    AuthorityIdentifier, ConsensusHeader, ConsensusOutput, Database, Epoch, TaskSpawner, TnSender,
};
use tracing::{debug, error, info, warn};

mod epoch;
pub use epoch::spawn_epoch_record_collector;
mod consensus;
use consensus::spawn_track_recent_consensus;

/// Sets some bus defaults.
/// Call this somewhere when starting an epoch.
pub async fn prime_consensus<DB: Database>(
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
    consensus_chain: ConsensusChain,
) {
    // Get the DB and load our last executed consensus block (note there may be unexecuted
    // blocks, catch up will execute them).
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, &consensus_chain).await.unwrap_or_default();

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

    consensus_bus.committed_round_updates().send_replace(last_consensus_round);
    consensus_bus.primary_round_updates().send_replace(last_consensus_round);
}

/// Spawn the state sync tasks.
pub fn spawn_state_sync<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    task_spawner: TaskSpawner,
    consensus_chain: ConsensusChain,
) {
    let mode = *consensus_bus.node_mode().borrow();
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {}
        NodeMode::CvvInactive | NodeMode::Observer => {
            // If we are not an active CVV then follow latest consensus from peers.
            let (config_clone, consensus_bus_clone) = (config.clone(), consensus_bus.clone());
            let task_spawner_clone = task_spawner.clone();
            let consensus_chain_clone = consensus_chain.clone();
            task_spawner.spawn_task(
                "state sync: track latest consensus header from peers",
                async move {
                    info!(target: "state-sync", "Starting state sync: track latest consensus header from peers");
                    if let Err(e) = spawn_track_recent_consensus(
                        config_clone,
                        consensus_bus_clone,
                        network,
                        task_spawner_clone,
                        consensus_chain_clone,
                    )
                    .await
                    {
                        error!(target: "state-sync", "Error tracking latest consensus headers: {e}");
                    }
                },
            );
            task_spawner.spawn_task(
                "state sync: stream consensus headers",
                async move {
                    info!(target: "state-sync", "Starting state sync: stream consensus header from peers");
                    if let Err(e) = spawn_stream_consensus_headers(config, consensus_bus, consensus_chain).await {
                        error!(target: "state-sync", "Error streaming consensus headers: {e}");
                    }
                },
            );
        }
    }
}

/// Write the consensus header and it's component transaction batches to the consensus DB.
///
/// An error here indicates a critical node failure.
/// Note, if this returns an error then the DB could not be written to- this is probably fatal.
pub async fn save_consensus<DB: Database>(
    db: &DB,
    consensus_output: ConsensusOutput,
    authority_id: &Option<AuthorityIdentifier>,
    consensus_chain: &mut ConsensusChain,
) -> eyre::Result<()> {
    let sub_dag = consensus_output.sub_dag().clone();
    consensus_chain.save_consensus_output(consensus_output).await?;
    if let Some(authority_id) = authority_id {
        // If we are a validator we need to clear any of our batches from our cache that are
        // now part of consensus.
        for cert in &sub_dag.certificates {
            if cert.header().author() == authority_id {
                for batch_hash in cert.header().payload().keys() {
                    let _ = db.remove::<NodeBatchesCache>(batch_hash);
                }
            }
        }
    }
    // Make sure we have persisted the consensus output before we execute.
    consensus_chain.persist_current().await?;
    Ok(())
}

/// Returns the ConsensusHeader that created the last executed block if can be found.
/// If we are not starting at genesis or a new epoch, then not finding this indicates a database
/// issue.
pub async fn last_executed_consensus_block(
    consensus_bus: &ConsensusBus,
    consensus_chain: &ConsensusChain,
) -> Option<ConsensusHeader> {
    let last = consensus_bus.last_executed_consensus_block(None, consensus_chain).await;
    debug!(target: "state-sync", ?last, "last executed consensus block");
    last
}

/// Collect and return any consensus headers that were not executed before last shutdown.
/// This will be consensus that was reached but had not executed before a shutdown.
pub async fn get_missing_consensus(
    consensus_bus: &ConsensusBus,
    consensus_chain: &ConsensusChain,
) -> eyre::Result<Vec<ConsensusHeader>> {
    let mut result = Vec::new();
    // Get the DB and load our last executed consensus block.
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, consensus_chain).await.unwrap_or_default();

    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let last_db_block = consensus_chain
        .consensus_header_latest()
        .await
        .unwrap_or_default()
        .unwrap_or_else(|| last_executed_block.clone());

    info!(target: "state-sync", ?last_executed_block, ?last_db_block, "comparing last executed block and last recorded consensus block");

    // if the last recorded consensus block is larger than the last executed block,
    // forward the stored consensus block to engine for execution
    if last_db_block.number > last_executed_block.number {
        for consensus_block_number in last_executed_block.number + 1..=last_db_block.number {
            if let Some(consensus_header) =
                consensus_chain.consensus_header_by_number(None, consensus_block_number).await?
            {
                debug!(target: "state-sync", ?consensus_header, "collecting unexecuted consensus header");
                result.push(consensus_header);
            }
        }
    }

    info!(target: "state-sync", ?result, "missing consensus headers that need execution:");
    Ok(result)
}

/// Spawn a long running task on task_manager that will stream consensus headers from the
/// last saved to the current and then keep up with current headers.
/// This should only be used when NOT participating in active consensus.
async fn spawn_stream_consensus_headers<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();

    let mut rx_last_consensus_header = consensus_bus.last_consensus_header().subscribe();
    let mut last_consensus_header =
        consensus_bus.last_consensus_block(None, &consensus_chain).await.unwrap_or_default();
    let mut last_consensus_height = last_consensus_header.number;
    let epoch = config.committee().epoch();

    // infinite loop over consensus output
    loop {
        tokio::select! {
            _ = rx_last_consensus_header.changed() => {
                // If this changes it should not be None...
                // Use borrow_and_update so the change is marked consumed; any subsequent
                // updates during the retry loop below will re-arm changed() correctly.
                let header = rx_last_consensus_header.borrow_and_update().clone().unwrap_or_default();
                debug!(target: "state-sync", rx_last_consensus_header=?header.number, ?last_consensus_height, "streaming consensus headers detected change");

                if header.number > last_consensus_height {
                    // Retry loop: the concurrent backward traversal in
                    // spawn_track_recent_consensus fills ConsensusHeaderCache asynchronously.
                    // catch_up_consensus_from_to may return early on a cache miss before the
                    // backward traversal has fetched an intermediate block. Retry with a short
                    // delay to let the traversal finish rather than waiting for the next gossip
                    // update (which may never come for an older epoch's blocks).
                    let mut no_progress_count = 0u32;
                    const MAX_NO_PROGRESS: u32 = 50; // 50 * 100ms = 5 seconds max wait
                    loop {
                        let prev_height = last_consensus_height;
                        last_consensus_header = catch_up_consensus_from_to(
                            &consensus_bus,
                            last_consensus_header,
                            header.clone(),
                            config.node_storage(),
                            &consensus_chain,
                            epoch,
                        )
                        .await?;
                        if last_consensus_header.sub_dag.leader_epoch() > epoch { return Ok(()); }
                        last_consensus_height = last_consensus_header.number;

                        if last_consensus_height >= header.number {
                            break; // Fully caught up to the target.
                        }
                        if last_consensus_height == prev_height {
                            // No progress: the backward traversal hasn't yet cached this block.
                            no_progress_count += 1;
                            if no_progress_count >= MAX_NO_PROGRESS {
                                warn!(target: "state-sync", ?epoch, last_consensus_height, target=header.number,
                                    "could not catch up to consensus target after retries, waiting for next gossip update");
                                break;
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                                _ = &rx_shutdown => return Ok(()),
                            }
                        } else {
                            no_progress_count = 0; // Progress was made, reset counter.
                        }
                    }
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
    consensus_bus: &ConsensusBus,
    from: ConsensusHeader,
    max_consensus: ConsensusHeader,
    db: &DB,
    consensus_chain: &ConsensusChain,
    epoch: Epoch,
) -> eyre::Result<ConsensusHeader> {
    let mut last_parent = from.digest();

    // Catch up to the current chain state if we need to.
    let last_consensus_height = from.number;
    let max_consensus_height = max_consensus.number;
    let catchup_distance = max_consensus_height.saturating_sub(last_consensus_height);
    if catchup_distance > 0 {
        info!(
            target: "tn::observer",
            last_consensus_height,
            max_consensus_height,
            catchup_distance,
            "catching up consensus blocks"
        );
    }
    if last_consensus_height >= max_consensus_height {
        return Ok(from);
    }
    let mut result_header = from;
    for number in last_consensus_height + 1..=max_consensus_height {
        debug!(target: "state-sync", "trying to get consensus block {number}");
        let mut remove_cache = false;
        // Check if we already have this consensus output in our local DB.
        // We will be verifying and loading these records elsewhere.
        let consensus_header = if number == max_consensus_height {
            max_consensus.clone()
        } else if let Ok(Some(header)) = db.get::<ConsensusHeaderCache>(&number) {
            remove_cache = true;
            header
        } else {
            error!(
                target: "tn::observer",
                block_number = number,
                "Could not find header"
            );
            // We should have all the required headers in local storage by now...
            return Ok(result_header);
        };
        if consensus_header.sub_dag.leader_epoch() > epoch {
            // Don't outrun the epoch and produce next epochs output.
            return Ok(consensus_header);
        }
        if remove_cache {
            let _ = db.remove::<ConsensusHeaderCache>(&number); // Should be done with this now.
        }
        let parent_hash = last_parent;
        last_parent =
            ConsensusHeader::digest_from_parts(parent_hash, &consensus_header.sub_dag, number);
        if last_parent != consensus_header.digest() {
            error!(
                target: "tn::observer",
                block_number = number,
                "consensus header digest mismatch - possible fork detected"
            );
            return Err(eyre::eyre!("consensus header digest mismatch!"));
        }
        if consensus_header.number <= consensus_chain.latest_consensus_number() {
            // We have already processed this consensus so ignore it.
            result_header = consensus_header;
            continue;
        }

        let base_execution_block = consensus_header.sub_dag.leader.header().latest_execution_block;
        // We need to make sure execution has caught up so we can verify we have not
        // forked. This will force the follow function to not outrun
        // execution...  this is probably fine. Also once we can
        // follow gossiped consensus output this will not really be
        // an issue (except during initial catch up).
        if consensus_bus.wait_for_execution(base_execution_block).await.is_err() {
            // We seem to have forked, so die.
            error!(
                target: "tn::observer",
                block_number = number,
                ?base_execution_block,
                "wait_for_execution failed - execution fork detected"
            );
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
