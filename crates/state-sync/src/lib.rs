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
use tn_primary::{ConsensusBusApp, NodeMode, PrimaryMetrics};
use tn_storage::{consensus::ConsensusChain, tables::ConsensusCache};
use tn_types::{
    ConsensusHeader, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Database, Epoch,
    TaskError, TaskSpawner, TnSender,
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
/// Returns the number of bytes the encoded Output takes on disk IF this is written to the current
/// pack or 0 otherwise (old consensus).
pub async fn save_consensus(
    consensus_output: ConsensusOutput,
    consensus_chain: &mut ConsensusChain,
    metrics: &PrimaryMetrics,
) -> eyre::Result<u64> {
    let output_bytes = consensus_chain.save_consensus_output(consensus_output).await?;
    // Note it is ok to leave batches in NodeBatchesCache until the epoch ends (when the table is
    // cleared). Make sure we have persisted the consensus output before we execute.
    consensus_chain.persist_current().await?;
    // A zero byte count means this output was old consensus not written to the current pack;
    // recording it as the "most recent" output size would be misleading.
    if output_bytes > 0 {
        metrics.record_consensus_output_bytes(output_bytes);
    }
    Ok(output_bytes)
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

/// The genesis consensus position: number `0` paired with the default header's digest.
///
/// This is the parent the first real `ConsensusHeader` links to; it is distinct from the zero
/// digest ([`ConsensusHeaderDigest::default`]), so it must be produced from the default header
/// rather than from `ConsensusNumHash::default`.
fn genesis_consensus_position() -> ConsensusNumHash {
    let genesis = ConsensusHeader::default();
    ConsensusNumHash::new(genesis.number, genesis.digest())
}

/// Return the (hash, number) to use as parent for the next ConsensusHeader.
/// Accounts for outputs committed to DB but not yet executed (which
/// replay_missed_consensus handles before the subscriber starts).
///
/// Resolves positions (not full headers) so the real recorded consensus hash survives a
/// snapshot-restore placeholder pack: fabricating a header whose `digest()` differs from the
/// recorded `final_consensus.hash` would break chain-linking for the next header.
pub async fn last_consensus_parent(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> (ConsensusHeaderDigest, u64) {
    let last_executed = consensus_bus
        .last_executed_consensus_position(consensus_chain)
        .await
        .unwrap_or_else(genesis_consensus_position);
    let last_db = consensus_chain
        .consensus_position_latest()
        .await
        .unwrap_or_default()
        .unwrap_or(last_executed);
    let parent = if last_db.number > last_executed.number { last_db } else { last_executed };
    (parent.hash, parent.number)
}

/// Collect and return any consensus headers that were not executed before last shutdown.
/// This will be consensus that was reached but had not executed before a shutdown.
pub async fn get_missing_consensus(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> eyre::Result<Vec<ConsensusHeader>> {
    let mut result = Vec::new();
    // Get the DB and load our last executed consensus position. Positions (not full headers) keep
    // the resolution correct across a snapshot-restore placeholder pack; only the numbers are used
    // here, but they must be resolved consistently with `last_consensus_parent` to avoid replaying
    // (double-executing) headers already applied to the restored EVM.
    let last_executed_block = consensus_bus
        .last_executed_consensus_position(consensus_chain)
        .await
        .unwrap_or_else(genesis_consensus_position);

    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let last_db_block = consensus_chain
        .consensus_position_latest()
        .await
        .unwrap_or_default()
        .unwrap_or(last_executed_block);

    info!(target: "state-sync", ?last_executed_block, ?last_db_block, "comparing last executed and last recorded consensus positions");

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
    // Seed the catch-up parent from the last processed consensus *position*. A snapshot-restored
    // node's placeholder pack has no header to return, so a header-based seed would default to
    // genesis (number 0, genesis digest) and the stream would restart from height 0, conflicting
    // with the already-restored EVM. The position seed carries the real recorded `(number, hash)`
    // so catch-up resumes at the boundary and chain-links the next header truthfully.
    let mut last_position = consensus_bus
        .last_consensus_position(&consensus_chain)
        .await
        .unwrap_or_else(genesis_consensus_position);
    let mut last_consensus_height = last_position.number;
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
                let progress = catch_up_consensus_from_to(
                    &consensus_bus,
                    last_position,
                    pending_header.number,
                    config.node_storage(),
                    &consensus_chain,
                    epoch,
                )
                .await?;
                if progress.reached_next_epoch {
                    // catch-up crossed into a later epoch; this epoch's stream task is done.
                    return Ok(());
                }
                last_position = progress.last;
                last_consensus_height = last_position.number;
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

/// Outcome of one [`catch_up_consensus_from_to`] pass.
struct CatchUp {
    /// Position of the last consensus header applied (or the starting position if none was).
    last: ConsensusNumHash,
    /// True when catch-up stopped because it reached output from a later epoch: this epoch's
    /// stream is complete and its task should exit.
    reached_next_epoch: bool,
}

/// Applies consensus output "from" (exclusive) to height "max_consensus_height" (inclusive).
/// Queries peers for latest height and downloads and executes any missing consensus output.
///
/// Takes and returns positions (`ConsensusNumHash`) rather than full headers so the parent used for
/// chain-linking carries the real recorded hash even when the starting point is an epoch boundary a
/// snapshot-restored node has no header for. Returns the last applied position and whether the pass
/// crossed into a later epoch.
async fn catch_up_consensus_from_to<DB: Database>(
    consensus_bus: &ConsensusBusApp,
    from: ConsensusNumHash,
    max_consensus_height: u64,
    db: &DB,
    consensus_chain: &ConsensusChain,
    epoch: Epoch,
) -> eyre::Result<CatchUp> {
    let mut last_parent = from.hash;

    // Catch up to the current chain state if we need to.
    let last_consensus_height = from.number;
    let catchup_distance = max_consensus_height.saturating_sub(last_consensus_height);
    if last_consensus_height >= max_consensus_height {
        return Ok(CatchUp { last: from, reached_next_epoch: false });
    }
    let mut last = from;
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
            return Ok(CatchUp { last, reached_next_epoch: false });
        };
        let consensus_header = output.consensus_header();
        if consensus_header.sub_dag.leader_epoch() > epoch {
            // Don't outrun the epoch and produce next epochs output.
            return Ok(CatchUp { last, reached_next_epoch: true });
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
        // `last_parent` was just verified equal to `consensus_header.digest()`, so it is the real
        // header digest — a truthful position for the next iteration's parent.
        if consensus_header.number <= consensus_chain.latest_consensus_number() {
            // We have already processed this consensus so ignore it (advance the parent chain
            // only).
            last = ConsensusNumHash::new(consensus_header.number, last_parent);
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
        last = ConsensusNumHash::new(consensus_header.number, last_parent);
        consensus_bus.sync_output().send(output).await?;
    }
    Ok(CatchUp { last, reached_next_epoch: false })
}
