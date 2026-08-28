//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use crate::error::{EngineResult, TnEngineError};
use tn_reth::{
    error::TnRethError,
    payload::{BuildArguments, TNPayload},
    CanonicalInMemoryState, DeferredTrieData, ExecutedBlock, NewCanonicalChain, ProviderError,
    RethEnv,
};
use tn_types::{
    gas_accumulator::GasAccumulator, max_batch_gas, EngineUpdate, Hash as _, SealedHeader, B256,
};
use tokio::sync::mpsc;
use tracing::{debug, error, field, info, info_span, warn};

/// Execute output from consensus to extend the canonical chain.
///
/// The function handles all types of output, included multiple blocks and empty blocks.
/// If the output contains no batches and the epoch is not closing, execution is skipped
/// entirely and the current canonical header is returned unchanged. The leader count is
/// still incremented for rewards tracking.
pub fn execute_consensus_output(
    args: BuildArguments,
    gas_accumulator: GasAccumulator,
    engine_update_tx: mpsc::Sender<EngineUpdate>,
) -> EngineResult<SealedHeader> {
    // rename canonical header for clarity
    let BuildArguments { reth_env, output, parent_header } = args;
    // Last canonical header executed.
    let mut canonical_header = parent_header;
    // Reward accounting is advanced here, before this output is durably finalized at the end of
    // this function, and is deliberately not rewound on any error path below. The ordering is safe
    // by construction: any error returns from this function and halts the engine task in-process
    // (the `?` on the execution result in `ExecutorEngine::run`), so no later output is ever
    // processed and a premature increment can never be consumed. On restart the accumulator is
    // rebuilt from the finalized tip by `catchup_accumulator`, whose replay boundary lines up with
    // the executed tip, so an increment for a not-yet-finalized output is reconstructed from
    // scratch rather than carried across the restart. Do not add a compensating decrement to the
    // error paths below: it would double-subtract against that rebuild.
    gas_accumulator.rewards_counter().inc_leader_count(output.leader().author());
    let epoch = output.leader().epoch();
    // output digest returns the `ConsensusHeader` digest
    let output_digest: B256 = output.digest().into();
    let leader_round = output.leader_round();
    let consensus_num_hash = output.num_hash();
    let batches = output.flatten_batches();

    let span = info_span!(target: "telcoin", "execute-consensus", epoch,
        consensus_number = output.number(),
        consensus_hash = output_digest.to_string(),
        parent_consensus_hash = output.parent_hash().to_string(),
        parent_exec_number = canonical_header.number,
        parent_exec_hash = canonical_header.hash().to_string(),
        executed_blocks = field::Empty,
        batches = batches.len(),
    );
    let _guard = span.enter();
    debug!(target: "engine", ?output, "executing output");

    // The flattened batches and the batch-digest deque share one flat index space:
    // `get_batch_digest(i)` reads the deque while the transactions come from the batch vectors, and
    // `close_epoch_for_last_batch(i)` derives the epoch-close boundary from the deque's length. So
    // unequal lengths mispair digests with payloads (wrong `ommers_hash`/`mix_hash`) and can fire
    // the epoch-close system calls on the wrong block or on none. This is an invalid condition and
    // is rejected fail-closed in every build profile.
    //
    // Do NOT re-add a `debug_assert_eq!` over this same predicate. One used to sit directly above,
    // un-`cfg`-gated, so it fired ahead of the `adiri` tolerance below: any debug-assertions build
    // (including `[profile.e2e]`, which the e2e suite uses) panicked on exactly the early epochs
    // that tolerance exists to survive, and it also made the `Err` arm below unreachable under
    // every profile a test can run in.
    if batches.len() != output.batch_digests().len() {
        #[cfg(not(feature = "adiri"))]
        return Err(TnEngineError::ConsensusOutputUnevenBatches(
            batches.len(),
            output.batch_digests().len(),
        ));

        #[cfg(feature = "adiri")]
        if epoch > tn_types::forks::ADIRI_DUP_BATCH_EPOCH {
            // ADIRI BUG
            // Epochs at or below `ADIRI_DUP_BATCH_EPOCH` (160) of adiri testnet had a bug with
            // duplicate batches: the subscriber pushes a digest for every header payload key but
            // skips the batch for a duplicate, leaving the deque longer than the flattened batches.
            // We have to recreate it in order to sync testnet so we skip this error (it will happen
            // and needs to be ignored) on adiri with early epochs.
            return Err(TnEngineError::ConsensusOutputUnevenBatches(
                batches.len(),
                output.batch_digests().len(),
            ));
        }
    }

    // ensure at least 1 block for empty output when close_epoch is true
    let mut executed_blocks = Vec::with_capacity(batches.len().max(1));
    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    let anchor_hash = canonical_header.hash();
    // The pre-output canonical tip. Each block eagerly advances the shared in-memory state
    // (see `execute_payload`), but the durable commit happens only after the whole output builds.
    // If a later block fails, the earlier blocks' advance is rolled back to this header so no
    // phantom canonical head survives (see `rollback_in_memory_output`).
    let anchor_header = canonical_header.clone();
    let mut ancestors: Vec<DeferredTrieData> = Vec::with_capacity(batches.len().max(1));

    if batches.is_empty() {
        if !output.close_epoch() {
            // Skip execution entirely — no batches and epoch is not closing.
            // Leader count was already incremented above for rewards tracking.
            info!(target: "engine", "skipping execution for empty non-epoch-closing output");
            crate::metrics::ENGINE_METRICS.empty_outputs_skipped_total.increment(1);
            span.record("executed_blocks", "0");
            // Notify consensus that this round was processed (no block produced)
            engine_update_tx.blocking_send((leader_round, consensus_num_hash, None)).map_err(
                |e| {
                    error!(target: "engine", ?e, "engine update channel send failed");
                    TnEngineError::ChannelClosed
                },
            )?;
            return Ok(canonical_header);
        }

        // Execute single empty block to close the epoch.
        // Use parent values for next block (these values would come from the worker's block).
        let base_fee_per_gas = canonical_header.base_fee_per_gas.unwrap_or_default();
        let gas_limit = canonical_header.gas_limit;
        let leader = output.leader().author();
        // INVARIANT: this lookup cannot miss for valid output, and the halt on `None` is a
        // deliberate fail-safe for impossible state; do NOT replace it with a default
        // beneficiary. A silent default is chain-consistent (every node computes the same
        // wrong address from the same state), so nothing downstream would ever detect the
        // regression. Four legs make the miss unreachable:
        //   1. `set_committee` runs at every `run_epoch` start before any output is forwarded (both
        //      the replay and live paths in `crates/node/src/manager/node/run_epoch.rs`).
        //   2. `close_epoch` blocks on `wait_for_consensus_execution` before returning, so the next
        //      epoch's `set_committee` cannot overwrite the committee while the closing output is
        //      still executing.
        //   3. `RewardsCounter::clear` clears only leader counts, never the committee.
        //   4. A committed sub-dag leader is a committee member by construction:
        //      `LeaderSchedule::leader` indexes `committee.authorities()`, and the swap table only
        //      substitutes members of the same committee.
        // The fail-stop is pinned by `test_empty_close_epoch_unknown_leader_fail_stops` and
        // `test_empty_close_epoch_without_committee_fail_stops` (`tests/it/main.rs`), which
        // mirror the subscriber's own fail-stop for the non-empty path
        // (`SubscriberError::UnexpectedAuthority`).
        let beneficiary = gas_accumulator
            .get_authority_address(leader)
            .ok_or(TnEngineError::UnknownAuthority(leader.clone()))
            .inspect_err(|e| error!(target: "engine", ?e, "failed to find leader's execution address for empty output"))?;

        let payload = TNPayload::new(
            canonical_header,
            beneficiary,
            0,
            B256::ZERO, // no batch to digest
            &output,
            output_digest,
            base_fee_per_gas,
            gas_limit,
            output_digest, // use output digest for mix hash
            0,             // Use worker 0 becuase we have to provide on.
        );

        debug!(target: "engine", "executing empty batch payload");

        // execute the payload and update the current canonical header
        let executed = execute_payload(
            payload,
            &vec![],
            &mut executed_blocks,
            &reth_env,
            &canonical_in_memory_state,
            anchor_hash,
            &ancestors,
        );
        // On failure, revert the in-memory advance applied by any earlier block of this output so
        // the propagated error never leaves a phantom canonical head observable to RPC. The leader
        // count incremented above is intentionally left in place; see the note at the top of this
        // function for why that is safe.
        canonical_header = executed.inspect_err(|_| {
            rollback_in_memory_output(&canonical_in_memory_state, &anchor_header, &executed_blocks)
        })?;
        if let Some(last_block) = executed_blocks.last() {
            ancestors.push(last_block.trie_data_handle());
        }
    } else {
        // loop and construct blocks from batches with transactions
        for (batch_index, (cert_idx, batch_idx_in_cert)) in batches.into_iter().enumerate() {
            let batch_digest = output
                .get_batch_digest(batch_index)
                .ok_or(TnEngineError::NextBlockDigestMissing)?;
            let cert_batch = &output.batches()[cert_idx];
            let batch = &cert_batch.batches[batch_idx_in_cert];

            // use batch's base fee, gas limit, and withdrawals
            let base_fee_per_gas = batch.base_fee_per_gas;
            let gas_limit = max_batch_gas(epoch);

            // apply XOR bitwise operator with worker's digest to ensure unique mixed hash per batch
            // for round
            let mix_hash = output_digest ^ batch_digest;
            // The block beneficiary that receives this batch's priority fees is the producer's
            // own `Batch::beneficiary` (#1222). That field is covered by the batch digest, so a
            // byzantine header that copies another validator's batch digest cannot redirect the
            // fees: whichever header references the digest, the batch carries its producer's
            // beneficiary.
            let payload = TNPayload::new(
                canonical_header,
                batch.beneficiary,
                batch_index,
                batch_digest,
                &output,
                output_digest,
                base_fee_per_gas,
                gas_limit,
                mix_hash,
                batch.worker_id,
            );

            // execute the payload and update the current canonical header
            let executed = execute_payload(
                payload,
                &batch.transactions,
                &mut executed_blocks,
                &reth_env,
                &canonical_in_memory_state,
                anchor_hash,
                &ancestors,
            );
            // On failure of a later block, revert the in-memory advance applied by the earlier
            // blocks of this output so the propagated (node-halting) error never leaves a phantom
            // canonical head observable to RPC before the node restarts. The reward-accounting
            // increments already applied for this output (the leader count and earlier blocks'
            // `inc_block`) are intentionally left in place here; see the note at the top of this
            // function for why that is safe.
            canonical_header = executed.inspect_err(|_| {
                rollback_in_memory_output(
                    &canonical_in_memory_state,
                    &anchor_header,
                    &executed_blocks,
                )
            })?;
            if let Some(last_block) = executed_blocks.last() {
                ancestors.push(last_block.trie_data_handle());
            }
            // Advances gas accounting before durable finalization, safe for the reason documented
            // at the top of this function. `inc_block` skips any block whose `gas_used` is zero, so
            // a restart replay does not inflate the per-worker block count.
            gas_accumulator.inc_block(
                batch.worker_id,
                canonical_header.gas_used,
                canonical_header.gas_limit,
            );
        }
    } // end block execution for round

    span.record("executed_blocks", executed_blocks.len().to_string());

    // Durably commit the output, then announce it. The two halves are called separately
    // (rather than through `finish_executing_output`) because their failure contracts
    // differ: a persist error leaves the database transaction uncommitted, so the
    // speculative in-memory advance must be compensated exactly as the in-loop error exits
    // above do, and a node-local provider fault is worth a bounded retry first. An announce
    // error is post-commit: the blocks are canonical for real and must NOT be rolled back. The
    // reward accounting stays in place on every path; see the note at the top of this
    // function.
    persist_output_with_retry(&reth_env, &executed_blocks).inspect_err(|e| {
        error!(
            target: "engine",
            ?e,
            attempts = PERSIST_OUTPUT_ATTEMPTS,
            "persisting executed consensus output failed - rolling back in-memory advance"
        );
        crate::metrics::ENGINE_METRICS.persist_failures_total.increment(1);
        rollback_in_memory_output(&canonical_in_memory_state, &anchor_header, &executed_blocks)
    })?;
    reth_env.announce_executed_output(
        executed_blocks,
        Some((leader_round, consensus_num_hash, engine_update_tx)),
    )?;
    // update the in-memory finalized/safe watches and prune persisted blocks from memory
    // (the database markers committed atomically with the blocks above)
    reth_env.finalize_block(canonical_header.clone())?;

    // return new canonical header for next engine task
    Ok(canonical_header)
}

/// Execute the transaction and update canon chain in-memory.
fn execute_payload(
    payload: TNPayload,
    transactions: &Vec<Vec<u8>>,
    executed_blocks: &mut Vec<ExecutedBlock>,
    reth_env: &RethEnv,
    canonical_in_memory_state: &CanonicalInMemoryState,
    anchor_hash: B256,
    ancestors: &[DeferredTrieData],
) -> EngineResult<SealedHeader> {
    // execute
    let next_canonical_block =
        reth_env.build_block_from_batch_payload(payload, transactions, anchor_hash, ancestors)?;
    debug!(target: "engine", ?next_canonical_block, "block executed");

    // update header for next block execution in loop
    let canonical_header = next_canonical_block.recovered_block.clone_sealed_header();
    info!(target: "engine", hash = canonical_header.hash().to_string(), number = canonical_header.number, "next block executed");
    crate::metrics::ENGINE_METRICS.blocks_executed_total.increment(1);
    crate::metrics::ENGINE_METRICS.block_gas_used.record(canonical_header.gas_used as f64);
    // Eagerly advance the shared in-memory state. This is load-bearing within a multi-block
    // output: the next block in the loop resolves its parent state through this advance. The
    // advance is speculative until the whole output commits durably after the loop; if a later
    // block fails to build, `execute_consensus_output` compensates it via
    // `rollback_in_memory_output`.
    canonical_in_memory_state.set_pending_block(next_canonical_block.clone());
    canonical_in_memory_state
        .update_chain(NewCanonicalChain::Commit { new: vec![next_canonical_block.clone()] });
    canonical_in_memory_state.set_canonical_head(canonical_header.clone());

    // collect all executed blocks for this output
    executed_blocks.push(next_canonical_block);

    Ok(canonical_header)
}

/// Total attempts (first try + retries) for the durable output persist in
/// [`execute_consensus_output`] before a node-local provider fault escalates out of the engine
/// task (a halt). Mirrors the epoch seam's classified read policy (`CLOSE_READ_ATTEMPTS` in
/// `tn-node`), which settled the treatment for this fault class on the read side (#1025).
/// Public so operators interpreting `tn_engine.persist_failures_total` and the fault-seam
/// integration tests reference the same attempt budget.
pub const PERSIST_OUTPUT_ATTEMPTS: u32 = 3;

/// Pause between persist retries in [`persist_output_with_retry`].
const PERSIST_OUTPUT_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// Run [`RethEnv::persist_executed_output`] up to [`PERSIST_OUTPUT_ATTEMPTS`] times, sleeping
/// [`PERSIST_OUTPUT_RETRY_BACKOFF`] between tries, retrying ONLY on a retryable
/// [`TnRethError::Provider`] fault (see [`retryable_persist_fault`]).
///
/// Provider faults on the durable write are node-local (disk pressure, an MDBX I/O error) and
/// often transient, so a bounded retry preserves liveness before the caller escalates to a halt
/// of the engine's critical task (issue #1090). Re-running the whole call is safe only while the
/// failed attempt made no static-file progress: `save_blocks` appends to the process-wide
/// static-file writers outside the database transaction, and once an attempt fsyncs such
/// progress the repeat attempt surfaces [`ProviderError::UnexpectedStaticFileBlockNumber`],
/// which [`retryable_persist_fault`] classifies terminal so the engine fails fast to a restart,
/// where reth's startup consistency check reconciles static files with the database. Any other
/// error class returns immediately for the caller's compensate-then-propagate arm. Sleeping
/// blocks the thread, which is correct here: `execute_consensus_output` already runs on a
/// dedicated blocking task.
///
/// Shape: the first `PERSIST_OUTPUT_ATTEMPTS - 1` attempts form the retry window (`find_map`
/// short-circuits on any non-retryable outcome); if every one of them hit a provider fault, the
/// final attempt's result is returned as-is.
///
/// Every retry is logged and counted (`tn_engine.persist_provider_fault_retries_total`) so a
/// node quietly surviving storage faults stays observable before the fault stops being
/// survivable.
fn persist_output_with_retry(
    reth_env: &RethEnv,
    blocks: &[ExecutedBlock],
) -> Result<(), TnRethError> {
    (1..PERSIST_OUTPUT_ATTEMPTS)
        .find_map(|attempt| match reth_env.persist_executed_output(blocks) {
            Err(TnRethError::Provider(err)) if retryable_persist_fault(&err) => {
                warn!(
                    target: "engine",
                    attempt,
                    max_attempts = PERSIST_OUTPUT_ATTEMPTS,
                    %err,
                    "node-local provider fault persisting executed consensus output - retrying"
                );
                crate::metrics::ENGINE_METRICS.persist_provider_fault_retries_total.increment(1);
                std::thread::sleep(PERSIST_OUTPUT_RETRY_BACKOFF);
                None
            }
            other => Some(other),
        })
        .unwrap_or_else(|| reth_env.persist_executed_output(blocks))
}

/// Whether an attempt of [`RethEnv::persist_executed_output`] that failed with this
/// node-local provider fault may be healed by re-running the call in-process.
///
/// A repeat attempt re-runs `save_blocks`, and any static-file progress the failed
/// attempt already fsynced makes the repeat trip over the advanced writer with
/// [`ProviderError::UnexpectedStaticFileBlockNumber`]. That class can never succeed
/// in-process (the writer stays advanced until reth's startup consistency check
/// reconciles it on restart), so it is terminal and must not burn further attempts.
fn retryable_persist_fault(err: &ProviderError) -> bool {
    !matches!(err, ProviderError::UnexpectedStaticFileBlockNumber(..))
}

/// Roll back the eager per-block in-memory canonical advance for a consensus output whose batch
/// loop failed partway through, or whose post-loop durable persist failed.
///
/// `execute_payload` advances the shared `CanonicalInMemoryState` (pending block, in-memory chain
/// segment, and canonical head) for every block it builds, because a later block in the same output
/// resolves its parent state from that advance. The durable commit and the finalized/safe markers
/// are written only once the whole output has built (`RethEnv::persist_executed_output` then
/// `RethEnv::finalize_block`). So when a block after the first fails to build,
/// `execute_consensus_output` propagates the error before any durable write, leaving the earlier
/// blocks advanced in memory but absent from the database: a transient "phantom" canonical head
/// that RPC `latest`/`pending` reads observe until a restart rebuilds in-memory state from the
/// finalized tip.
///
/// This reverts that advance so the phantom head is never observable. The `advanced` blocks are
/// removed from the in-memory chain with a reorg whose `new` chain is empty (which also clears any
/// pending block), and the canonical head is reset to `anchor_header`, the output's pre-loop parent
/// tip. It is a no-op when nothing advanced: an empty `advanced` slice reorgs no blocks and resets
/// the head to the value it already holds, so it is safe to call on every error exit of the loop.
///
/// Scope: this compensates only the pre-durable-commit advance: a failure inside the batch loop,
/// or a failure of the durable persist itself (`RethEnv::persist_executed_output`, every error
/// return of which leaves the database transaction uncommitted; issue #1090). Once the persist
/// commits, the blocks
/// are canonical for real and must not be reverted; an `announce_executed_output` failure
/// propagates WITHOUT compensation; the durable two-transaction commit/finalize window is a
/// separate concern handled by `RethEnv::heal_finalized_to_persisted_tip`.
fn rollback_in_memory_output(
    canonical_in_memory_state: &CanonicalInMemoryState,
    anchor_header: &SealedHeader,
    advanced: &[ExecutedBlock],
) {
    canonical_in_memory_state
        .update_chain(NewCanonicalChain::Reorg { new: Vec::new(), old: advanced.to_vec() });
    canonical_in_memory_state.set_canonical_head(anchor_header.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_reth::StaticFileSegment;

    /// The static-file mismatch a repeat persist attempt raises over the failed attempt's
    /// fsynced progress can never heal in-process, so it must be classified terminal.
    #[test]
    fn test_static_file_mismatch_is_not_retryable() {
        assert!(!retryable_persist_fault(&ProviderError::UnexpectedStaticFileBlockNumber(
            StaticFileSegment::Headers,
            2,
            1,
        )));
    }

    /// An ordinary node-local provider fault stays inside the bounded retry budget.
    #[test]
    fn test_ordinary_provider_fault_is_retryable() {
        assert!(retryable_persist_fault(&ProviderError::HeaderNotFound(0u64.into())));
    }
}
