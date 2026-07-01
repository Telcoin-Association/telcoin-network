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
use tn_primary::{ConsensusBusApp, NodeMode};
use tn_storage::{consensus::ConsensusChain, tables::ConsensusCache};
use tn_types::{
    ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, Database, Epoch, TaskError,
    TaskSpawner, TnSender,
};
use tracing::{debug, error, info, warn};

mod epoch;
pub use epoch::spawn_epoch_record_collector;
mod consensus;
mod metrics;
use crate::metrics::STATE_SYNC_METRICS;
use consensus::spawn_track_recent_consensus;
pub use consensus::{request_missing_packs, spawn_fetch_consensus, spawn_fetch_recent_consensus};

/// Sets some bus defaults.
/// Call this somewhere when starting an epoch.
pub async fn prime_consensus<DB: Database>(
    consensus_bus: &ConsensusBusApp,
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
    consensus_bus: ConsensusBusApp,
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
            task_spawner.spawn_task(
                "state sync: track latest consensus header from peers",
                async move {
                    info!(target: "state-sync", "Starting state sync: track latest consensus header from peers");
                    spawn_track_recent_consensus(
                        config_clone,
                        consensus_bus_clone,
                    ).await;
                    Ok(())
                },
            );
            task_spawner.spawn_task(
                "state sync: stream consensus headers",
                async move {
                    info!(target: "state-sync", "Starting state sync: stream consensus header from peers");
                    if let Err(e) = spawn_stream_consensus_headers(config, consensus_bus, consensus_chain).await {
                        error!(target: "state-sync", "Error streaming consensus headers: {e}");
                        Err(TaskError::from_message(e))
                    } else {
                        Ok(())
                    }
                },
            );
        }
    }
}

/// Write the consensus header and it's component transaction batches to the consensus chain.
///
/// An error here indicates a critical node failure.
/// Note, if this returns an error then the DB could not be written to- this is probably fatal.
pub async fn save_consensus(
    consensus_output: ConsensusOutput,
    consensus_chain: &mut ConsensusChain,
) -> eyre::Result<()> {
    consensus_chain.save_consensus_output(consensus_output).await?;
    // Note it is ok to leave batches in NodeBatchesCache until the epoch ends (when the table is
    // cleared). Make sure we have persisted the consensus output before we execute.
    consensus_chain.persist_current().await?;
    Ok(())
}

/// Returns the ConsensusHeader that created the last executed block if can be found.
/// If we are not starting at genesis or a new epoch, then not finding this indicates a database
/// issue.
pub async fn last_executed_consensus_block(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> Option<ConsensusHeader> {
    let last = consensus_bus.last_executed_consensus_block(consensus_chain).await;
    debug!(target: "state-sync", ?last, "last executed consensus block");
    last
}

/// Return the (hash, number) to use as parent for the next ConsensusHeader.
/// Accounts for outputs committed to DB but not yet executed (which
/// replay_missed_consensus handles before the subscriber starts).
pub async fn last_consensus_parent(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> (ConsensusHeaderDigest, u64) {
    let last_executed =
        last_executed_consensus_block(consensus_bus, consensus_chain).await.unwrap_or_default();
    let last_db = consensus_chain
        .consensus_header_latest()
        .await
        .unwrap_or_default()
        .unwrap_or_else(|| last_executed.clone());
    let parent = if last_db.number > last_executed.number { last_db } else { last_executed };
    (parent.digest(), parent.number)
}

/// Collect and return any consensus headers that were not executed before last shutdown.
/// This will be consensus that was reached but had not executed before a shutdown.
pub async fn get_missing_consensus(
    consensus_bus: &ConsensusBusApp,
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
                consensus_chain.consensus_header_by_number(consensus_block_number).await?
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
    consensus_bus: ConsensusBusApp,
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();

    let mut rx_last_consensus_header = consensus_bus.last_consensus_header().subscribe();
    let mut last_consensus_header =
        consensus_bus.last_consensus_block(&consensus_chain).await.unwrap_or_default();
    let mut last_consensus_height = last_consensus_header.number;
    let epoch = config.committee().epoch();

    // Read and consume the current watch value immediately. This handles a race where a pack
    // file was downloaded and last_consensus_header() was updated before this task subscribed
    // (e.g., epoch N's pack arrives before epoch N's spawn_stream_consensus_headers starts).
    // borrow_and_update() marks the value as seen so that changed() fires correctly for
    // subsequent sends — without it, changed() could fire spuriously for the stale value.
    let mut pending_header =
        rx_last_consensus_header.borrow_and_update().clone().unwrap_or_default();

    // infinite loop over consensus output
    loop {
        if pending_header.number > last_consensus_height {
            debug!(target: "state-sync", rx_last_consensus_header=?pending_header.number, ?last_consensus_height, "streaming consensus headers detected change");
            // Retry loop: the concurrent backward traversal in
            // spawn_track_recent_consensus fills ConsensusHeaderCache asynchronously.
            // catch_up_consensus_from_to may return early on a cache miss before the
            // backward traversal has fetched an intermediate block. Retry with a short
            // delay to let the traversal finish rather than waiting for the next gossip
            // update (which may never come for an older epoch's blocks).
            let mut no_progress_count = 0u32;
            const MAX_NO_PROGRESS: u32 = 600; // 600 * 100ms = 60 seconds max wait
            loop {
                let prev_height = last_consensus_height;
                last_consensus_header = catch_up_consensus_from_to(
                    &consensus_bus,
                    last_consensus_header,
                    pending_header.clone(),
                    config.node_storage(),
                    &consensus_chain,
                    epoch,
                )
                .await?;
                if last_consensus_header.sub_dag.leader_epoch() > epoch {
                    return Ok(());
                }
                last_consensus_height = last_consensus_header.number;
                STATE_SYNC_METRICS
                    .headers_fetched_total
                    .increment(last_consensus_height.saturating_sub(prev_height));

                if last_consensus_height >= pending_header.number {
                    break; // Fully caught up to the target.
                }
                if last_consensus_height == prev_height {
                    // No progress: the backward traversal hasn't yet cached this block.
                    no_progress_count += 1;
                    STATE_SYNC_METRICS.no_progress_total.increment(1);
                    if no_progress_count >= MAX_NO_PROGRESS {
                        warn!(target: "state-sync", ?epoch, last_consensus_height, target=pending_header.number,
                            "could not catch up to consensus target after retries, waiting for next gossip update");
                        break;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        _ = &rx_shutdown => return Ok(()),
                    }
                } else {
                    if no_progress_count > 0 {
                        info!(target: "state-sync", ?epoch, last_consensus_height, no_progress_count,
                            "catch-up made progress after retries");
                    }
                    no_progress_count = 0; // Progress was made, reset counter.
                }
            }
        }

        tokio::select! {
            _ = rx_last_consensus_header.changed() => {
                // Use borrow_and_update so the change is marked consumed; any subsequent
                // updates during the retry loop above will re-arm changed() correctly.
                pending_header = rx_last_consensus_header.borrow_and_update().clone().unwrap_or_default();
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
    consensus_bus: &ConsensusBusApp,
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
    if last_consensus_height >= max_consensus_height {
        return Ok(from);
    }
    let mut result_header = from;
    for number in last_consensus_height + 1..=max_consensus_height {
        debug!(target: "state-sync", "trying to get consensus block {number}");
        // Resolve the full, verified ConsensusOutput for this number: from the sync cache (pulled
        // recent->earliest, verified on insert) or already in a local pack (chain/staging). The
        // committee was applied at decode time, so this output is complete (header + batches).
        let mut from_cache = false;
        let output = if let Ok(Some(output)) = db.get::<ConsensusCache>(&number) {
            from_cache = true;
            output
        } else if let Ok(Some(output)) = consensus_chain.consensus_output_by_number(number).await {
            // Already in a local pack (processed before a restart, or staged current-epoch output).
            output
        } else {
            if number > last_consensus_height + 1 {
                // Only log after we start and hit a missing output (avoid a flood at startup).
                warn!(
                    target: "tn::observer",
                    block_number = number,
                    "Could not find consensus output (we may be catching up)"
                );
            }
            // We should have the required outputs in local storage by now...
            return Ok(result_header);
        };
        let consensus_header = output.consensus_header();
        if consensus_header.sub_dag.leader_epoch() > epoch {
            // Don't outrun the epoch and produce next epochs output.
            return Ok(consensus_header);
        }
        if from_cache {
            let _ = db.remove::<ConsensusCache>(&number); // Done with this cache entry.
        }
        if number == last_consensus_height + 1 {
            // We only want to log this once and only when we are doing something.
            info!(
                target: "tn::observer",
                last_consensus_height,
                max_consensus_height,
                catchup_distance,
                "catching up consensus blocks"
            );
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
            // We have already processed this consensus so ignore it (advance the parent chain
            // only).
            result_header = consensus_header;
            continue;
        }

        let base_execution_block = consensus_header.sub_dag.leader().latest_execution_block();
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
        // Deliver the full, verified output (with batches) for execution.
        result_header = consensus_header;
        consensus_bus.sync_output().send(output).await?;
    }
    Ok(result_header)
}
