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
    ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, Database, Epoch, TaskError,
    TaskSpawner, TnSender,
};
use tracing::{debug, error, info, warn};

mod epoch;
pub use epoch::{spawn_epoch_record_collector, sync_epoch_records_to_tip};
mod consensus;
mod metrics;
use crate::metrics::STATE_SYNC_METRICS;
use consensus::spawn_track_recent_consensus;
pub use consensus::{request_missing_packs, spawn_fetch_consensus, spawn_fetch_recent_consensus};

/// First delay before the consensus stream retries a failed step; doubled after each consecutive
/// failure up to [`STREAM_RETRY_MAX_DELAY`].
const STREAM_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(5);
/// Upper bound on the consensus stream retry delay.
const STREAM_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);

/// Sets some bus defaults.
/// Call this somewhere when starting an epoch.
///
/// A failed storage lookup is propagated so startup halts loudly; only a genuinely absent
/// header (fresh chain or a new epoch) defaults and primes the rounds from a fresh start.
pub async fn prime_consensus<DB: Database>(
    consensus_bus: &ConsensusBusApp,
    config: &ConsensusConfig<DB>,
    consensus_chain: ConsensusChain,
) -> eyre::Result<()> {
    // Get the DB and load our last executed consensus block (note there may be unexecuted
    // blocks, catch up will execute them).
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, &consensus_chain).await?.unwrap_or_default();

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
    Ok(())
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
            // not critical on purpose: the stream returns Ok at every epoch boundary before the
            // epoch shutdown is noticed, which a critical task reports as CriticalExitOk and would
            // fail every healthy epoch change (CvvInactive epochs included, they share this path).
            // catch-up errors are retried inside the task instead of ending it.
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
/// pack or 0 if the output already resides in a static (imported) pack. An output whose number
/// does not advance the chain's latest consensus is an error (it used to be a silent 0-byte
/// no-op, which let a node resuming from a collapsed height 0 discard every output silently).
pub async fn save_consensus(
    consensus_output: ConsensusOutput,
    consensus_chain: &mut ConsensusChain,
    metrics: &PrimaryMetrics,
) -> eyre::Result<u64> {
    let output_bytes = consensus_chain.save_consensus_output(consensus_output).await?;
    // Make sure we have persisted the consensus output before we execute. Forwarding it to
    // execution evicts its batches from NodeBatchesCache, which is only safe once the pack holds
    // them.
    consensus_chain.persist_current().await?;
    // A zero byte count means this output already resides in a static (imported) pack and
    // nothing was written; recording it as the "most recent" output size would be misleading.
    if output_bytes > 0 {
        metrics.record_consensus_output_bytes(output_bytes);
    }
    Ok(output_bytes)
}

/// Returns the ConsensusHeader that created the last executed block if can be found.
/// If we are not starting at genesis or a new epoch, then not finding this indicates a database
/// issue.
///
/// `Ok(None)` means the header is confirmed ABSENT from the consensus store; a storage read
/// failure surfaced by the lookup (for example a sealed static epoch pack that fails to OPEN)
/// is `Err`, never `Ok(None)`. Record-level reads inside a pack that opened cleanly still
/// collapse to `None`; that remaining channel is documented at
/// `ConsensusChain::consensus_header_by_digest` in `tn-storage`.
pub async fn last_executed_consensus_block(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> eyre::Result<Option<ConsensusHeader>> {
    let last = consensus_bus.last_executed_consensus_block(consensus_chain).await?;
    debug!(target: "state-sync", ?last, "last executed consensus block");
    Ok(last)
}

/// Return the (hash, number) to use as parent for the next ConsensusHeader.
/// Accounts for outputs committed to DB but not yet executed (which
/// replay_missed_consensus handles before the subscriber starts).
///
/// Returns an error when the last executed consensus block or the latest recorded consensus
/// header cannot be READ from the consensus store (as opposed to being absent, which falls
/// back to a default header at number 0 as before). Halting on a failed lookup matters because
/// `save_consensus_output` hard-rejects the non-advancing numbers a silently re-rooted default
/// parent would produce, so resuming from the default would strand the node anyway.
pub async fn last_consensus_parent(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> eyre::Result<(ConsensusHeaderDigest, u64)> {
    let last_executed =
        last_executed_consensus_block(consensus_bus, consensus_chain).await?.unwrap_or_default();
    let last_db =
        consensus_chain.consensus_header_latest().await?.unwrap_or_else(|| last_executed.clone());
    let parent = if last_db.number > last_executed.number { last_db } else { last_executed };
    Ok((parent.digest(), parent.number))
}

/// Collect and return any consensus headers that were not executed before last shutdown.
/// This will be consensus that was reached but had not executed before a shutdown.
///
/// A failed storage lookup is propagated (the cursors below decide what gets re-executed, so
/// computing them from silently-defaulted headers at number 0 would skip the replay); a
/// genuinely absent header legitimately defaults to the fresh-chain genesis header.
pub async fn get_missing_consensus(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &ConsensusChain,
) -> eyre::Result<Vec<ConsensusHeader>> {
    let mut result = Vec::new();
    // Get the DB and load our last executed consensus block.
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, consensus_chain).await?.unwrap_or_default();

    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let last_db_block = consensus_chain
        .consensus_header_latest()
        .await?
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
    let epoch = config.committee().epoch();
    // A failed lookup is retried with backoff (nothing restarts this task if it ends, and shutdown
    // still interrupts the wait); only a genuinely absent header (fresh chain) defaults so
    // streaming starts from genesis.
    let mut retry_delay = STREAM_RETRY_INITIAL_DELAY;
    let mut last_consensus_header = loop {
        match consensus_bus.last_consensus_block(&consensus_chain).await {
            Ok(header) => break header.unwrap_or_default(),
            Err(e) => {
                STATE_SYNC_METRICS.stream_errors_total.increment(1);
                error!(target: "state-sync", ?epoch, ?retry_delay,
                    "failed to read the last consensus header, retrying: {e}");
                tokio::select! {
                    _ = tokio::time::sleep(retry_delay) => {}
                    _ = &rx_shutdown => return Ok(()),
                }
                retry_delay = retry_delay.saturating_mul(2).min(STREAM_RETRY_MAX_DELAY);
            }
        }
    };
    retry_delay = STREAM_RETRY_INITIAL_DELAY;
    let mut last_consensus_height = last_consensus_header.number;

    // Catch-up tuning (observer-only): how long to poll local storage between catch-up attempts
    // and how many no-progress polls to tolerate before asking the fetch task to backfill. Copied
    // out (both are `Copy`) so the borrow of `config` does not outlive this line.
    let sync_config = config.network_config().sync_config();
    let catch_up_poll_interval = sync_config.consensus_header_catch_up_poll_interval;
    // Clamp to at least one poll: a misconfigured `= 0` would otherwise make `stall_recheck` zero
    // and busy-spin the bottom select (and signal a gap before any poll elapses).
    let max_no_progress = sync_config.consensus_header_catch_up_max_no_progress.max(1);
    // After a stall we hand the fill off to the fetch task and park on the bottom select. Because
    // that fill lands in the cache silently (it never advances the `last_consensus_header` watch
    // for heights at or below the target, so `changed()` does not fire), we also wake on this
    // interval to re-drain the cache. One stall budget: the same passive window the loop already
    // tolerated. Saturates on overflow.
    let stall_recheck = catch_up_poll_interval.saturating_mul(max_no_progress);

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
            // spawn_track_recent_consensus fills the ConsensusCache asynchronously.
            // catch_up_consensus_from_to may return early on a cache miss before the
            // backward traversal has fetched an intermediate block. Retry with a short
            // delay to let the traversal finish rather than waiting for the next gossip
            // update (which may never come for an older epoch's blocks).
            let mut no_progress_count = 0u32;
            loop {
                let prev_height = last_consensus_height;
                // on error too, the header names the last output applied or skipped, so the
                // retry resumes there and never re-sends an output already queued for execution
                let caught_up = catch_up_consensus_from_to(
                    &consensus_bus,
                    &mut last_consensus_header,
                    pending_header.clone(),
                    config.node_storage(),
                    &consensus_chain,
                    epoch,
                )
                .await;
                if last_consensus_header.sub_dag.leader_epoch() > epoch {
                    return Ok(());
                }
                last_consensus_height = last_consensus_header.number;
                STATE_SYNC_METRICS
                    .headers_fetched_total
                    .increment(last_consensus_height.saturating_sub(prev_height));

                if let Err(e) = caught_up {
                    // a mismatched cache entry was already removed, so the retry refetches it
                    // through the stall and gap path below; a real fork keeps failing here, logged
                    // and counted on every attempt instead of silently halting the stream
                    STATE_SYNC_METRICS.stream_errors_total.increment(1);
                    error!(target: "state-sync", ?epoch, last_consensus_height,
                        target = pending_header.number, ?retry_delay,
                        "consensus catch-up failed, retrying: {e}");
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {}
                        _ = &rx_shutdown => return Ok(()),
                    }
                    retry_delay = retry_delay.saturating_mul(2).min(STREAM_RETRY_MAX_DELAY);
                    continue;
                }
                if last_consensus_height > prev_height {
                    // only progress resets the backoff: a pass that stops on a cache miss also
                    // returns Ok, and resetting there would pin a persistent failure at the
                    // initial delay
                    retry_delay = STREAM_RETRY_INITIAL_DELAY;
                }

                if last_consensus_height >= pending_header.number {
                    break; // Fully caught up to the target.
                }
                if last_consensus_height == prev_height {
                    // No progress: the backward traversal hasn't yet cached this block.
                    no_progress_count += 1;
                    STATE_SYNC_METRICS.no_progress_total.increment(1);
                    if no_progress_count >= max_no_progress {
                        // The gossip-driven backward traversal (spawn_track_recent_consensus ->
                        // spawn_fetch_recent_consensus) only runs when a gossip update arrives, and
                        // even then floors its walk at a watermark that only ever rises, so it
                        // cannot recover a gap at the bottom of the catch-up range. Rather than
                        // spinning here (or parking on the next gossip update, which may never
                        // arrive for an older epoch's blocks or on an independently running node),
                        // hand that exact range to the fetch task, which owns the in-flight
                        // accounting, then break to the bottom select. That select re-drains the
                        // cache on `stall_recheck` even though the fill lands silently (it never
                        // advances last_consensus_header for heights <= the target), and reacts to
                        // a fresh gossip target if one arrives.
                        warn!(target: "state-sync", ?epoch, last_consensus_height, target=pending_header.number,
                            "catch-up stalled after retries, requesting the missing consensus range from the fetch task");
                        STATE_SYNC_METRICS.catch_up_redrives_total.increment(1);
                        consensus_bus.consensus_gap_request().send_replace(Some((
                            epoch,
                            pending_header.number,
                            pending_header.digest(),
                            last_consensus_height.saturating_add(1),
                        )));
                        break;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(catch_up_poll_interval) => {}
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
            // Re-drain the cache after a stall: the fetch task fills the missing range silently
            // (it never advances last_consensus_header for heights <= the target, so changed()
            // above does not fire), so we re-enter the catch-up loop on this interval and drain
            // whatever has landed. When already caught up this is a cheap periodic no-op.
            _ = tokio::time::sleep(stall_recheck) => {}
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Applies consensus output "from" (exclusive) up to "max_consensus" (inclusive) by reading
/// consensus outputs from local storage — first the `ConsensusCache`, then the `ConsensusChain`.
///
/// This function does not itself query peers: the backward traversal spawned by
/// `spawn_track_recent_consensus` / `spawn_fetch_recent_consensus` is what fetches missing outputs
/// into the cache. On a cache miss for an output that has not been fetched yet, it returns early,
/// leaving the caller (`spawn_stream_consensus_headers`) to re-drive a request for the missing
/// range.
///
/// Progress is written through `from`, on success and on error alike: it holds the last header
/// applied (sent for execution) or skipped (already saved), or the first header of the next epoch,
/// which ends the walk. After an error the caller resumes from it. Re-reading the last consensus
/// block (the executed tip) instead would re-send outputs still queued in `sync_output` but not yet
/// saved. The subscriber skips them as stale, but each one uses a queue slot.
///
/// A fresh node (`from` is the default header at number 0) anchors block 1 on the
/// `parent_hash` block 1 carries rather than on `from.digest()`. The default header's digest is
/// build-flavor dependent and differs from the anchor that existing networks (adiri) built block 1
/// on, so checking against it fails every fresh node. Block 1 only reaches this loop as a verified
/// cache entry or from a chain-verified local pack, and its digest commits to its own parent, so
/// the default anchor adds no integrity. The strict parent check applies from block 2 on.
async fn catch_up_consensus_from_to<DB: Database>(
    consensus_bus: &ConsensusBusApp,
    from: &mut ConsensusHeader,
    max_consensus: ConsensusHeader,
    db: &DB,
    consensus_chain: &ConsensusChain,
    epoch: Epoch,
) -> eyre::Result<()> {
    // number 0 is only reachable from the default header a fresh node starts from
    let fresh_start = from.number == 0;
    let mut last_parent = from.digest();

    // Catch up to the current chain state if we need to.
    let last_consensus_height = from.number;
    let max_consensus_height = max_consensus.number;
    let catchup_distance = max_consensus_height.saturating_sub(last_consensus_height);
    if last_consensus_height >= max_consensus_height {
        return Ok(());
    }
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
            return Ok(());
        };
        let consensus_header = output.consensus_header();
        if consensus_header.sub_dag.leader_epoch() > epoch {
            // Don't outrun the epoch and produce next epochs output.
            *from = consensus_header;
            return Ok(());
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
        let parent_hash = if fresh_start && number == 1 {
            // see the doc comment: the default header is not a reliable anchor for block 1
            if consensus_header.parent_hash != last_parent {
                info!(
                    target: "tn::observer",
                    adopted_parent = ?consensus_header.parent_hash,
                    default_parent = ?last_parent,
                    "fresh node: anchoring catch-up on block 1's parent instead of the default header"
                );
            }
            consensus_header.parent_hash
        } else {
            last_parent
        };
        last_parent =
            ConsensusHeader::digest_from_parts(parent_hash, &consensus_header.sub_dag, number);
        if last_parent != consensus_header.digest() {
            let source = if from_cache { "cache" } else { "pack" };
            error!(
                target: "tn::observer",
                block_number = number,
                local_parent = ?parent_hash,
                header_parent = ?consensus_header.parent_hash,
                source,
                "consensus header digest mismatch - possible fork detected"
            );
            return Err(eyre::eyre!(
                "consensus header digest mismatch at block {number}: local parent {parent_hash:?}, header parent {:?}, output from {source}",
                consensus_header.parent_hash
            ));
        }
        if consensus_header.number <= consensus_chain.latest_consensus_number() {
            // We have already processed this consensus so ignore it (advance the parent chain
            // only).
            *from = consensus_header;
            continue;
        }

        let base_execution_block = consensus_header.sub_dag.leader().latest_execution_block();
        // We need to make sure execution has caught up so we can verify we have not
        // forked. This only throttles non-empty outputs: an empty output produces no block, so
        // through a long empty stretch the leaders keep referencing a block that is already
        // executed and this returns at once. There the bounded `sync_output` queue below is the
        // only thing that keeps this loop from outrunning the subscriber.
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
        // Deliver the full, verified output (with batches) for execution. The queue is bounded, so
        // this send blocks while the subscriber is behind; the epoch task manager aborts this task
        // at teardown, so a blocked send cannot outlive the epoch. Record the header as progress
        // only once the output is queued, so `from` never names an output that was not sent.
        consensus_bus.sync_output().send(output).await?;
        *from = consensus_header;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, time::Duration};
    use tempfile::TempDir;
    use tn_config::NetworkConfig;
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{CommittedSubDag, EpochSeedChainValue, ReputationScores, B256};

    /// Consensus output `number` chained on `parent`, built from one round-1 certificate per
    /// authority. The headers carry no batches, so a saved output reads back from the pack without
    /// batch records.
    fn output_at(
        fixture: &CommitteeFixture<MemDatabase>,
        number: u64,
        parent: ConsensusHeaderDigest,
    ) -> ConsensusOutput {
        let committee = fixture.committee();
        let certificates: Vec<_> = fixture
            .authorities()
            .map(|a| fixture.certificate(&a.header_with_round(&committee, 1)))
            .collect();
        let leader = certificates.last().cloned().expect("fixture yields certificates");
        let sub_dag = CommittedSubDag::new(
            certificates,
            leader,
            number - 1,
            ReputationScores::new(&committee),
            None,
            EpochSeedChainValue::genesis_placeholder(),
        );
        ConsensusOutput::new(sub_dag, parent, number, false, VecDeque::new(), vec![])
    }

    /// A parent digest that no honest chain links to.
    fn wrong_parent() -> ConsensusHeaderDigest {
        [99u8; 32].into()
    }

    /// A consensus chain holding block 1 with `parent_hash = B256::ZERO`. Zero is not the default
    /// header's digest in either build flavor, like adiri's block 1, which predates the #1032
    /// default anchor.
    async fn chain_with_block1(
        fixture: &CommitteeFixture<MemDatabase>,
        temp_dir: &TempDir,
    ) -> eyre::Result<(ConsensusChain, ConsensusOutput)> {
        let mut chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee()).await?;
        let block1 = output_at(fixture, 1, B256::ZERO.into());
        save_consensus(block1.clone(), &mut chain, &PrimaryMetrics::default()).await?;
        Ok((chain, block1))
    }

    /// Issue #836: when the observer catch-up loop makes no progress within its budget, it must
    /// hand the missing range to the fetch task (rather than parking on the next gossip update)
    /// by publishing a gap request on the bus. With an empty cache and chain, catch-up can never
    /// make progress, so the stall branch must publish `Some((epoch, target, hash, floor))` within
    /// the (tiny, test-tuned) budget.
    #[tokio::test]
    async fn signals_gap_request_on_catch_up_timeout() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();

        // Borrow one authority's base config/storage/keys, then build a config with a fast,
        // deterministic catch-up budget (signal after a single no-progress poll).
        let (base_config, node_storage, key_config) = {
            let authority = fixture.authorities().next().expect("fixture yields an authority");
            let cc = authority.consensus_config();
            (cc.config().clone(), cc.node_storage().clone(), cc.key_config().clone())
        };
        let mut network_config = NetworkConfig::default();
        let sync = network_config.sync_config_mut();
        // Signal after a single no-progress poll and keep the poll interval short so the test does
        // not wait on the (interval * max_no_progress) recheck window.
        sync.consensus_header_catch_up_poll_interval = Duration::from_millis(50);
        sync.consensus_header_catch_up_max_no_progress = 1;

        let committee = fixture.committee();
        let config = ConsensusConfig::new_with_committee_for_test(
            base_config,
            node_storage,
            key_config,
            committee.clone(),
            network_config,
        )?;
        let consensus_bus = ConsensusBusApp::new();
        let temp_dir = TempDir::new()?;
        let consensus_chain = ConsensusChain::new(temp_dir.path().to_owned(), committee)?;

        // Publish a pending header ahead of local height. The chain and cache are empty, so
        // catch-up returns early on the first missing output and makes no progress, forcing the
        // stall branch.
        let target = ConsensusHeader { number: 5, ..Default::default() };
        consensus_bus.last_consensus_header().send_replace(Some(target.clone()));

        // Subscribe before spawning so we cannot miss the coalescing gap request.
        let mut rx_gap = consensus_bus.consensus_gap_request().subscribe();

        let handle =
            tokio::spawn(spawn_stream_consensus_headers(config, consensus_bus, consensus_chain));

        // The stall must publish a gap request for the missing range within the budget.
        tokio::time::timeout(Duration::from_secs(5), rx_gap.changed())
            .await
            .expect("catch-up stall did not signal a gap request in time")
            .expect("gap request watch closed unexpectedly");

        let request = *rx_gap.borrow_and_update();
        let (_epoch, number, _hash, floor) =
            request.expect("gap request should be Some after a stall");
        assert_eq!(number, target.number, "gap target is the pending header number");
        assert_eq!(floor, 1, "floor is last_consensus_height + 1 (0 + 1) for an empty chain");

        handle.abort();
        Ok(())
    }

    /// A fresh node starts catch-up from the default header, whose digest is build-flavor
    /// dependent and differs from the anchor adiri's block 1 was built on. Block 1's own parent
    /// must be adopted instead of failing the digest check, and the strict chain check must carry
    /// on from block 1's digest.
    #[tokio::test]
    async fn catch_up_adopts_block1_parent_on_fresh_node() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new()?;
        let (mut chain, block1) = chain_with_block1(&fixture, &temp_dir).await?;
        let block2 = output_at(&fixture, 2, block1.consensus_header().digest());
        save_consensus(block2.clone(), &mut chain, &PrimaryMetrics::default()).await?;
        assert_ne!(
            ConsensusHeader::default().digest(),
            ConsensusHeaderDigest::from(B256::ZERO),
            "block 1's parent must differ from the default anchor for this test to mean anything"
        );

        // both outputs are at or below the chain's latest number, so catch-up only walks the
        // parent chain: no execution wait and no send to the subscriber
        let mut from = ConsensusHeader::default();
        catch_up_consensus_from_to(
            &ConsensusBusApp::new(),
            &mut from,
            block2.consensus_header(),
            &MemDatabase::default(),
            &chain,
            fixture.committee().epoch(),
        )
        .await?;
        assert_eq!(from.number, 2, "catch-up must walk past block 1 to the target");
        Ok(())
    }

    /// The fresh-node anchor applies to block 1 only: a block 2 whose parent is not block 1's
    /// digest is still rejected.
    #[tokio::test]
    async fn catch_up_still_rejects_wrong_parent_after_block1() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new()?;
        let (chain, _block1) = chain_with_block1(&fixture, &temp_dir).await?;
        let forked2 = output_at(&fixture, 2, wrong_parent());
        let db = MemDatabase::default();
        db.insert::<ConsensusCache>(&2, &forked2)?;

        let mut from = ConsensusHeader::default();
        let err = catch_up_consensus_from_to(
            &ConsensusBusApp::new(),
            &mut from,
            forked2.consensus_header(),
            &db,
            &chain,
            fixture.committee().epoch(),
        )
        .await
        .expect_err("a wrong parent after block 1 must be rejected");
        assert!(err.to_string().contains("digest mismatch"), "unexpected error: {err}");
        assert!(err.to_string().contains("at block 2"), "rejected the wrong block: {err}");
        // the cache entry is consumed just before the check, so its absence proves the walk
        // accepted block 1 and rejected block 2 (not block 1)
        assert!(db.get::<ConsensusCache>(&2)?.is_none(), "catch-up never reached block 2");
        // the error keeps the caller's progress: block 1 was walked and stays recorded
        assert_eq!(from.number, 1, "progress before the error must survive it");
        Ok(())
    }

    /// A catch-up error (here a digest mismatch on block 2) must not end the stream task, which is
    /// not critical and would otherwise leave the subscriber waiting forever. The task logs, backs
    /// off and retries, and shutdown still ends it during the backoff.
    #[tokio::test]
    async fn stream_task_survives_digest_mismatch() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (base_config, node_storage, key_config) = {
            let authority = fixture.authorities().next().expect("fixture yields an authority");
            let cc = authority.consensus_config();
            (cc.config().clone(), cc.node_storage().clone(), cc.key_config().clone())
        };
        let config = ConsensusConfig::new_with_committee_for_test(
            base_config,
            node_storage.clone(),
            key_config,
            fixture.committee(),
            NetworkConfig::default(),
        )?;
        let temp_dir = TempDir::new()?;
        let (chain, _block1) = chain_with_block1(&fixture, &temp_dir).await?;
        let forked2 = output_at(&fixture, 2, wrong_parent());
        node_storage.insert::<ConsensusCache>(&2, &forked2)?;
        let consensus_bus = ConsensusBusApp::new();
        consensus_bus.last_consensus_header().send_replace(Some(forked2.consensus_header()));
        let shutdown = config.shutdown().clone();

        let handle = tokio::spawn(spawn_stream_consensus_headers(config, consensus_bus, chain));

        // the cache entry is consumed just before the digest check, so its removal marks the point
        // where the task has hit the error; wait for that instead of a fixed sleep
        tokio::time::timeout(Duration::from_secs(5), async {
            while node_storage.get::<ConsensusCache>(&2)?.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            eyre::Ok(())
        })
        .await
        .expect("the mismatched cache entry is dropped so the retry refetches it")?;
        // the task has run past the error and must now be parked in its retry backoff
        assert!(!handle.is_finished(), "a catch-up error must not end the stream task");

        shutdown.notify();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("shutdown must interrupt the retry backoff")??;
        Ok(())
    }
}
