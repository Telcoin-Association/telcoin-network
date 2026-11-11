//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use crate::{
    duration_ms,
    error::{EngineResult, TnEngineError},
    metrics::ENGINE_METRICS,
    recovery::{output_tx_count, recover_signers, RecoveryHandle},
};
use std::time::{Duration, Instant};
use tn_reth::{
    error::TnRethError,
    payload::{BuildArguments, TNPayload},
    BlockBuildTimings, CanonicalInMemoryState, ExecutedBlock, NewCanonicalChain, OutputBuildScope,
    ProviderError, RethEnv,
};
use tn_types::{
    gas_accumulator::GasAccumulator, max_batch_gas, repack_monitor::RepackMonitor,
    ConsensusNumHash, ConsensusOutput, EngineUpdate, Hash as _, Recovered, Round, SealedHeader,
    TransactionSigned, B256,
};
use tokio::sync::mpsc;
use tracing::{debug, error, field, info, info_span, trace, warn, Span};

/// Execute output from consensus to extend the canonical chain.
///
/// The function handles all types of output, included multiple blocks and empty blocks.
/// If the output contains no batches and the epoch is not closing, execution is skipped
/// entirely and the current canonical header is returned unchanged. The leader count is
/// still incremented for rewards tracking.
///
/// Every output that produces blocks ends with one `output executed` debug line (target
/// `engine`) splitting its wall time into `execute_ms` (entry to the end of the block loop),
/// `persist_ms` (the bounded durable persist, retries included) and `announce_ms` (the
/// post-commit announce plus the in-memory finalize). `execute_ms` is split further into
/// `recover_wait_ms` (the time spent obtaining the output's recovered signers), the block builds'
/// summed laps (`evm_ms`, `root_ms`, `finish_ms`, see [`BlockBuildTimings`]) and `other_ms`, the
/// rest of it (payload construction, in-memory chain updates, logging); `max_block_root_ms` and
/// `max_block_evm_ms` are the slowest block's laps and `txs` counts the transactions handed to
/// the builder. The skipped empty path does not log it. The pipelined persist's commit task logs
/// the same line with three more fields, which this sequential path never logs:
/// `persist_wait_ms` (the built output's wait for the commit slot), `seed_ms` (seeding the
/// build's overlay with the committing output's deltas) and `pipelined=true`.
///
/// This entry point recovers the output's signers inline, on the calling thread, before the
/// first block. No production code calls it: the engine's run loop starts that recovery on a
/// background job when the output is queued and executes through
/// `execute_consensus_output_with_signers`, and production recovers inline only through the
/// fallback in `RecoveryHandle::wait`. It is kept as the differential oracle for the recovered
/// path (`test_queued_recovery_matches_inline_recovery` pins that both build the same blocks);
/// every caller is a test.
pub fn execute_consensus_output(
    args: BuildArguments,
    gas_accumulator: GasAccumulator,
    repack_monitor: RepackMonitor,
    engine_update_tx: mpsc::Sender<EngineUpdate>,
) -> EngineResult<SealedHeader> {
    execute_consensus_output_with_signers(
        args,
        None,
        gas_accumulator,
        repack_monitor,
        engine_update_tx,
    )
}

/// [`execute_consensus_output`] with the output's signers taken from `signers`, the handle of
/// the background recovery job started when the output was queued, or recovered inline when it
/// is `None`.
///
/// The handle is resolved before the first block, blocking until the job finishes (it usually
/// has, while earlier outputs executed); the wait is the `output executed` line's
/// `recover_wait_ms`. On catch-up or replay up to `MAX_QUEUED_OUTPUTS` jobs start together and
/// share the global rayon pool, with each other, batch validation and the build's own parallel
/// phases, so the first output's wait can exceed what inline recovery would take while the
/// later outputs' waits read about zero. A job that ended without a result falls back to inline
/// recovery (see [`RecoveryHandle::wait`]), so the signers never fail the output. Must run on a
/// blocking thread when `signers` is `Some`.
///
/// The strictly sequential path: [`build_output`] on a fresh [`OutputBuildScope`], then
/// [`commit_output`], then on a persist failure the rollback of the output's in-memory advance
/// to its pre-output tip, all on the calling thread, so nothing of the next output starts until
/// this one is durable and announced.
pub(crate) fn execute_consensus_output_with_signers(
    args: BuildArguments,
    signers: Option<RecoveryHandle>,
    gas_accumulator: GasAccumulator,
    repack_monitor: RepackMonitor,
    engine_update_tx: mpsc::Sender<EngineUpdate>,
) -> EngineResult<SealedHeader> {
    let reth_env = args.reth_env.clone();
    let built =
        build_output(args, signers, gas_accumulator, repack_monitor, OutputBuildScope::new())?;
    match commit_output(&reth_env, &built, &engine_update_tx, Duration::ZERO, false) {
        Ok(committed) => Ok(committed.tip),
        Err(CommitFailure::Persist(e)) => {
            // the database transaction never committed: compensate the output's speculative
            // in-memory advance exactly as a failed block build does (see `build_output`)
            let _guard = built.span.enter();
            rollback_in_memory_output(
                &reth_env.canonical_in_memory_state(),
                &built.anchor,
                &built.blocks,
            );
            Err(e)
        }
        // post-commit: the blocks are canonical for real and must not be rolled back
        Err(CommitFailure::PostCommit(e)) => Err(e),
    }
}

/// The build half of one consensus output: everything up to and including the output's
/// evm/root/finish metrics, on the calling (blocking) thread.
///
/// Builds the output's blocks on `args.parent_header` with `scope` (a fresh
/// [`OutputBuildScope::new`] when the parent is durable, or a seeded scope whose overlay holds
/// the unpersisted ancestors' deltas), advancing reth's in-memory canonical chain per block. A
/// failed block rolls the output's own blocks back to `args.parent_header` before the error
/// returns. Reward accounting (`inc_leader_count`, `inc_block`) runs here, in build order, and
/// is never rewound. An empty output that does not close the epoch builds nothing and sends NO
/// engine update: [`commit_output`] sends it.
pub(crate) fn build_output(
    args: BuildArguments,
    signers: Option<RecoveryHandle>,
    gas_accumulator: GasAccumulator,
    repack_monitor: RepackMonitor,
    scope: OutputBuildScope,
) -> EngineResult<BuiltOutput> {
    // observability only: start of the `execute_ms` span in the `output executed` line
    let execute_start = Instant::now();
    // rename canonical header for clarity
    let BuildArguments { reth_env, output, parent_header } = args;
    // Last canonical header executed.
    let mut canonical_header = parent_header;
    // Reward accounting is advanced here, before this output is durably finalized by
    // `commit_output`, and is deliberately not rewound on any error path below or there. The
    // ordering is safe by construction: any build or commit error returns to the engine and halts
    // the engine task in-process (the `?` on the execution result in `ExecutorEngine::run`), so no
    // later output is ever processed and a premature increment can never be consumed. On restart
    // the accumulator is rebuilt from the finalized tip by `catchup_accumulator`, whose replay
    // boundary lines up with the executed tip, so an increment for a not-yet-finalized output is
    // reconstructed from scratch rather than carried across the restart. Do not add a compensating
    // decrement to the error paths below: it would double-subtract against that rebuild.
    //
    // With the pipelined persist two outputs can be in flight: this one building while the one
    // before it commits, so both outputs' increments may be applied before either is durable.
    // The argument holds for both: any error in either slot stops the engine with an error (see
    // `PersistPipeline`), and the restart rebuilds both outputs' increments from the finalized
    // tip.
    gas_accumulator.rewards_counter().inc_leader_count(output.leader().author());
    let epoch = output.leader().epoch();
    // output digest returns the `ConsensusHeader` digest
    let output_digest: B256 = output.digest().into();
    let leader_round = output.leader_round();
    let consensus_num_hash = output.num_hash();
    let batches = output.flatten_batches();
    let batch_count = batches.len();
    // transactions handed to the block builder across every batch of the output (before any
    // unrecoverable or invalid transaction is dropped)
    let tx_count = output_tx_count(&output);

    let span = info_span!(target: "telcoin", "execute-consensus", epoch,
        consensus_number = output.number(),
        consensus_hash = output_digest.to_string(),
        parent_consensus_hash = output.parent_hash().to_string(),
        parent_exec_number = canonical_header.number,
        parent_exec_hash = canonical_header.hash().to_string(),
        executed_blocks = field::Empty,
        batches = batches.len(),
        txs = tx_count,
    );
    let _guard = span.enter();
    // the whole output (every batch's raw transaction bytes) is a trace-only dump: at debug it
    // was formatted on the executing thread for every output. The debug line is a message-only
    // marker: the entered span already carries the output's shape (consensus number, batches,
    // transactions), and fields repeated here would print twice.
    trace!(target: "engine::dump", ?output, "executing output");
    debug!(target: "engine", "executing output");

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
    // observability only: the block builds' laps, summed for the `output executed` line
    let mut timings = OutputTimings::default();
    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    // The pre-output canonical tip. Each block eagerly advances the shared in-memory state
    // (see `execute_payload`), but the durable commit happens only after the whole output builds.
    // If a later block fails, the earlier blocks' advance is rolled back to this header so no
    // phantom canonical head survives (see `rollback_in_memory_output`).
    let anchor_header = canonical_header.clone();
    // ONE build scope for the whole output, anchored at the database tip or at the snapshot the
    // caller pinned: its trie overlay takes each built block's sorted deltas in place, so later
    // blocks' state roots layer the accumulated deltas over the database instead of re-merging
    // every unpersisted ancestor per block through reth's memory-overlay provider (#1301), and
    // its read cache keeps the parent-state reads of earlier blocks, advanced past each block,
    // so an account or slot read by several blocks of the output is loaded once. Dropped with
    // this function: persistence uses the per-block deltas, never this accumulation, and every
    // error path returns, discarding it together with the rolled-back in-memory advance.
    let mut build_scope = scope;

    if batches.is_empty() {
        if !output.close_epoch() {
            // Skip execution entirely — no batches and epoch is not closing.
            // Leader count was already incremented above for rewards tracking.
            info!(target: "engine", "skipping execution for empty non-epoch-closing output");
            crate::metrics::ENGINE_METRICS.empty_outputs_skipped_total.increment(1);
            span.record("executed_blocks", "0");
            // consensus learns that this round was processed (no block produced) from
            // `commit_output`, behind the commit of every output before this one
            drop(_guard);
            return Ok(BuiltOutput {
                consensus_number: output.number(),
                leader_round,
                consensus_num_hash,
                close_epoch: false,
                anchor: anchor_header,
                tip: canonical_header,
                blocks: executed_blocks,
                txs: tx_count,
                batches: batch_count,
                timings,
                execute: execute_start.elapsed(),
                seed: Duration::ZERO,
                span,
            });
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

        // Pre-fork this reduces to the bare consensus header digest (zero batch digest);
        // post-fork it is the seed-chain PREVRANDAO derivation (#1247).
        let mix_hash = output.prev_randao(0, B256::ZERO);
        let payload = TNPayload::new(
            canonical_header,
            beneficiary,
            0,
            B256::ZERO, // no batch to digest
            &output,
            output_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash,
            0, // Use worker 0 becuase we have to provide on.
        );
        report_timestamp_clamp(&payload, &output);

        debug!(target: "engine", "executing empty batch payload");

        // execute the payload and update the current canonical header
        let executed = execute_payload(
            payload,
            Vec::new(),
            &mut executed_blocks,
            &reth_env,
            &canonical_in_memory_state,
            &mut build_scope,
            &mut timings,
            output.number(),
        );
        // On failure, revert the in-memory advance applied by any earlier block of this output so
        // the propagated error never leaves a phantom canonical head observable to RPC. The leader
        // count incremented above is intentionally left in place; see the note at the top of this
        // function for why that is safe.
        canonical_header = executed.inspect_err(|_| {
            rollback_in_memory_output(&canonical_in_memory_state, &anchor_header, &executed_blocks)
        })?;
    } else {
        // The output's recovered signers, one entry per flattened batch: take them from the
        // background job (blocking until it finishes) or recover them inline. Both come from
        // `RethEnv::recover_output_signers`, which walks the batches in the order of the loop
        // below, so the lengths match by construction; the check fails closed before any block
        // advances the in-memory chain, should that ever change.
        let wait_start = Instant::now();
        let output_signers = match signers {
            Some(handle) => handle.wait(&output, &ENGINE_METRICS),
            None => recover_signers(&output, &ENGINE_METRICS),
        };
        timings.recover_wait = wait_start.elapsed();
        if output_signers.len() != batches.len() {
            return Err(TnEngineError::RecoveredSignersMismatch(
                batches.len(),
                output_signers.len(),
            ));
        }

        // loop and construct blocks from batches with transactions
        for ((batch_index, (cert_idx, batch_idx_in_cert)), recovered) in
            batches.into_iter().enumerate().zip(output_signers)
        {
            let batch_digest = output
                .get_batch_digest(batch_index)
                .ok_or(TnEngineError::NextBlockDigestMissing)?;
            let cert_batch = &output.batches()[cert_idx];
            let batch = &cert_batch.batches[batch_idx_in_cert];

            // Watch for cross-producer transaction re-packing (issue #1259). Batches are
            // observed in ordering position, so the first-recorded copy of a transaction is
            // the copy execution credits with the priority fees. Telemetry only: a flagged
            // duplicate never changes execution, because an honest sender that submits the
            // same transaction to two validators produces the same signature.
            let repacked = repack_monitor.observe_batch(
                output.number(),
                cert_batch.address,
                &batch.transactions,
            );
            if !repacked.is_empty() {
                crate::metrics::ENGINE_METRICS
                    .cross_producer_repacked_txs_total
                    .increment(u64::try_from(repacked.len()).unwrap_or(u64::MAX));
                // Log a bounded sample, never the whole list: the duplicate set is derived
                // from remote-controlled batch contents, so a hostile producer could otherwise
                // force one warn line with thousands of entries per batch at no cost to
                // itself. The counter above carries the full count.
                let sample: Vec<B256> =
                    repacked.iter().take(4).map(|duplicate| duplicate.tx_hash).collect();
                warn!(
                    target: "engine",
                    producer = ?cert_batch.address,
                    repacked = repacked.len(),
                    ?sample,
                    "batch repacks transactions first packed by another producer (priority-fee poaching indicator, issue #1259)"
                );
            }

            // use batch's base fee, gas limit, and withdrawals
            let base_fee_per_gas = batch.base_fee_per_gas;
            let gas_limit = max_batch_gas(epoch);

            // Pre-fork: XOR with the worker's digest for a unique mix hash per batch in the
            // round. Post-fork: the seed-chain PREVRANDAO derivation, still unique per batch
            // via the batch index but immune to payload grinding (#1247).
            let mix_hash = output.prev_randao(batch_index, batch_digest);
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
            report_timestamp_clamp(&payload, &output);

            // execute the payload and update the current canonical header
            let executed = execute_payload(
                payload,
                recovered,
                &mut executed_blocks,
                &reth_env,
                &canonical_in_memory_state,
                &mut build_scope,
                &mut timings,
                output.number(),
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
    let execute = execute_start.elapsed();
    // `tn_engine_recover_seconds` is recorded by the recovery itself (see `recover_signers`)
    crate::metrics::ENGINE_METRICS.evm_seconds.record(timings.evm);
    crate::metrics::ENGINE_METRICS.root_seconds.record(timings.root);
    crate::metrics::ENGINE_METRICS.finish_seconds.record(timings.finish);

    drop(_guard);
    Ok(BuiltOutput {
        consensus_number: output.number(),
        leader_round,
        consensus_num_hash,
        close_epoch: output.close_epoch(),
        anchor: anchor_header,
        tip: canonical_header,
        blocks: executed_blocks,
        txs: tx_count,
        batches: batch_count,
        timings,
        execute,
        seed: Duration::ZERO,
        span,
    })
}

/// The commit half of one consensus output built by [`build_output`]: persist with retry,
/// announce, finalize and the `output executed` line, on the calling (blocking) thread.
///
/// An output without blocks (the skipped empty output) only sends `(round, num_hash, None)` on
/// `engine_update_tx`, so its update reaches consensus behind every earlier output's announce.
/// This half never rolls anything back: on [`CommitFailure::Persist`] the database transaction
/// did not commit and the caller must roll the output's in-memory advance back; on
/// [`CommitFailure::PostCommit`] the blocks are durable and must stay. The reward accounting of
/// the build stays in place on every path (see the note at the top of [`build_output`]).
///
/// `persist_wait` (the output's wait for the commit slot) and `pipelined` only shape the
/// `output executed` line: the pipelined persist adds `persist_wait_ms`, `seed_ms` and
/// `pipelined=true` to it, and the sequential path logs today's fields only.
///
/// Must run on a blocking thread: the persist retry sleeps and the engine update is a
/// `blocking_send`.
pub(crate) fn commit_output(
    reth_env: &RethEnv,
    built: &BuiltOutput,
    engine_update_tx: &mpsc::Sender<EngineUpdate>,
    persist_wait: Duration,
    pipelined: bool,
) -> Result<Committed, CommitFailure> {
    let _guard = built.span.enter();
    if built.blocks.is_empty() {
        // Notify consensus that this round was processed (no block produced)
        engine_update_tx
            .blocking_send((built.leader_round, built.consensus_num_hash, None))
            .map_err(|e| {
                error!(target: "engine", ?e, "engine update channel send failed");
                CommitFailure::PostCommit(TnEngineError::ChannelClosed)
            })?;
        return Ok(Committed {
            tip: built.tip.clone(),
            persist: Duration::ZERO,
            announce: Duration::ZERO,
        });
    }

    // Durably commit the output, then announce it. The two halves are called separately
    // (rather than through `finish_executing_output`) because their failure contracts
    // differ: a persist error leaves the database transaction uncommitted, so the
    // speculative in-memory advance must be compensated exactly as the in-loop error exits
    // of the build do, and a node-local provider fault is worth a bounded retry first. An
    // announce error is post-commit: the blocks are canonical for real and must NOT be rolled
    // back. The reward accounting stays in place on every path; see the note at the top of
    // `build_output`.
    let persist_start = Instant::now();
    let persisted = persist_output_with_retry(reth_env, &built.blocks);
    let persist = persist_start.elapsed();
    crate::metrics::ENGINE_METRICS.persist_duration_seconds.record(persist);
    persisted.map_err(|e| {
        error!(
            target: "engine",
            ?e,
            attempts = PERSIST_OUTPUT_ATTEMPTS,
            "persisting executed consensus output failed - rolling back in-memory advance"
        );
        crate::metrics::ENGINE_METRICS.persist_failures_total.increment(1);
        CommitFailure::Persist(e.into())
    })?;
    let announce_start = Instant::now();
    // `announce_executed_output` consumes its blocks: every field of an `ExecutedBlock` is an
    // `Arc`, so the clone shares the blocks the caller keeps for its own bookkeeping
    reth_env
        .announce_executed_output(
            built.blocks.clone(),
            Some((built.leader_round, built.consensus_num_hash, engine_update_tx.clone())),
        )
        .map_err(|e| CommitFailure::PostCommit(e.into()))?;
    // update the in-memory finalized/safe watches and prune persisted blocks from memory
    // (the database markers committed atomically with the blocks above)
    reth_env.finalize_block(built.tip.clone()).map_err(|e| CommitFailure::PostCommit(e.into()))?;
    let announce = announce_start.elapsed();

    crate::metrics::ENGINE_METRICS
        .executed_transactions_total
        .increment(u64::try_from(built.txs).unwrap_or(u64::MAX));
    let timings = &built.timings;
    let other = built.execute.saturating_sub(timings.laps());
    if pipelined {
        debug!(
            target: "engine",
            consensus_number = built.consensus_number,
            leader_round = built.leader_round,
            blocks = built.blocks.len(),
            batches = built.batches,
            execute_ms = duration_ms(built.execute),
            persist_ms = duration_ms(persist),
            announce_ms = duration_ms(announce),
            txs = built.txs,
            recover_wait_ms = duration_ms(timings.recover_wait),
            evm_ms = duration_ms(timings.evm),
            root_ms = duration_ms(timings.root),
            finish_ms = duration_ms(timings.finish),
            other_ms = duration_ms(other),
            max_block_root_ms = duration_ms(timings.max_block_root),
            max_block_evm_ms = duration_ms(timings.max_block_evm),
            persist_wait_ms = duration_ms(persist_wait),
            seed_ms = duration_ms(built.seed),
            pipelined,
            "output executed"
        );
    } else {
        // the sequential line keeps exactly its fields (`test_engine_lines_carry_consensus_ids`
        // pins them); a line without `pipelined` is a sequential one
        debug!(
            target: "engine",
            consensus_number = built.consensus_number,
            leader_round = built.leader_round,
            blocks = built.blocks.len(),
            batches = built.batches,
            execute_ms = duration_ms(built.execute),
            persist_ms = duration_ms(persist),
            announce_ms = duration_ms(announce),
            txs = built.txs,
            recover_wait_ms = duration_ms(timings.recover_wait),
            evm_ms = duration_ms(timings.evm),
            root_ms = duration_ms(timings.root),
            finish_ms = duration_ms(timings.finish),
            other_ms = duration_ms(other),
            max_block_root_ms = duration_ms(timings.max_block_root),
            max_block_evm_ms = duration_ms(timings.max_block_evm),
            "output executed"
        );
    }

    Ok(Committed { tip: built.tip.clone(), persist, announce })
}

/// One consensus output, built and in memory, not yet committed.
///
/// Its blocks are on reth's in-memory canonical chain (advanced per block by the build) and in
/// nobody's database until [`commit_output`] persists them.
#[derive(Debug)]
pub(crate) struct BuiltOutput {
    /// The output's consensus number.
    pub(crate) consensus_number: u64,
    /// The round of the output's leader, reported with the engine update.
    pub(crate) leader_round: Round,
    /// The output's consensus number and header digest, reported with the engine update.
    pub(crate) consensus_num_hash: ConsensusNumHash,
    /// Whether the output closes the epoch (an empty skipped output never does).
    pub(crate) close_epoch: bool,
    /// The pre-output tip: the parent of the output's first block.
    pub(crate) anchor: SealedHeader,
    /// The last block built, or `anchor` for an output without blocks.
    pub(crate) tip: SealedHeader,
    /// The blocks built, in order; empty for a skipped empty output.
    pub(crate) blocks: Vec<ExecutedBlock>,
    /// Transactions handed to the block builder across every batch of the output.
    pub(crate) txs: usize,
    /// The output's flattened batches.
    pub(crate) batches: usize,
    /// The block builds' laps, for the `output executed` line.
    pub(crate) timings: OutputTimings,
    /// Entry of the build to the end of its block loop: the line's `execute_ms`.
    pub(crate) execute: Duration,
    /// Time the caller spent seeding the build's overlay with the committing output's deltas
    /// before the build (zero unseeded): the line's `seed_ms`.
    pub(crate) seed: Duration,
    /// The output's `execute-consensus` span, entered again by the commit so the commit's lines
    /// carry the output's context.
    pub(crate) span: Span,
}

/// Why [`commit_output`] failed: before the database commit (the caller must roll the output's
/// in-memory advance back) or after it (the blocks are durable and must stay).
#[derive(Debug)]
pub(crate) enum CommitFailure {
    /// The persist failed after its retries; nothing of the output is durable.
    Persist(TnEngineError),
    /// The announce or the in-memory finalize failed after the commit (or the engine update of an
    /// output without blocks could not be sent).
    PostCommit(TnEngineError),
}

/// What [`commit_output`] did, for the caller's metrics.
#[derive(Debug)]
pub(crate) struct Committed {
    /// The output's last block (its pre-output tip when it built none), now durable.
    pub(crate) tip: SealedHeader,
    /// The persist, retries included (zero without blocks).
    pub(crate) persist: Duration,
    /// The announce plus the in-memory finalize (zero without blocks).
    pub(crate) announce: Duration,
}

/// The block builds' phase laps summed over one consensus output, plus the slowest block's
/// root and evm laps.
///
/// Observability only: it feeds the `output executed` line and the `tn_engine_*_seconds`
/// split histograms, never block contents.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutputTimings {
    /// Time the execution thread spent obtaining the output's recovered signers: blocked on the
    /// background recovery job, or recovering inline (the fallback, and callers without a job),
    /// plus any recovery lap inside the builds (zero: blocks build from recovered signers).
    recover_wait: Duration,
    /// Summed builder-setup, pre-execution and transaction-loop laps.
    evm: Duration,
    /// Summed state-root laps.
    root: Duration,
    /// Summed finish laps (the rest of `finish` plus the delta sorts and overlay extend).
    finish: Duration,
    /// The longest single-block state-root lap.
    max_block_root: Duration,
    /// The longest single-block evm lap.
    max_block_evm: Duration,
}

impl OutputTimings {
    /// Fold one block's laps into the output's sums and maxima.
    fn add(&mut self, block: &BlockBuildTimings) {
        self.recover_wait = self.recover_wait.saturating_add(block.recover);
        self.evm = self.evm.saturating_add(block.evm);
        self.root = self.root.saturating_add(block.root);
        self.finish = self.finish.saturating_add(block.finish);
        self.max_block_root = self.max_block_root.max(block.root);
        self.max_block_evm = self.max_block_evm.max(block.evm);
    }

    /// The four summed laps together; `execute_ms` minus this is the line's `other_ms`.
    fn laps(&self) -> Duration {
        self.recover_wait
            .saturating_add(self.evm)
            .saturating_add(self.root)
            .saturating_add(self.finish)
    }
}

/// Execute the transaction and update canon chain in-memory.
///
/// `build_scope` is the consensus output's shared [`OutputBuildScope`]: the build computes this
/// block's state root layered over its trie overlay (#1301) and reads the parent state through
/// its read cache, then advances both to this block's post-state for the NEXT block of the
/// output. `recovered` is the batch's entry of the output's recovered signers. The build's phase
/// laps are added to `timings`. `consensus_number` only labels the `next block executed` line,
/// together with the payload's batch index and digest, so each block joins back to its consensus
/// output and batch.
// the per-output accumulators and the log context are separate on purpose; bundling them into a
// struct would only rename the same eight values
#[allow(clippy::too_many_arguments)]
fn execute_payload(
    payload: TNPayload,
    recovered: Vec<Recovered<TransactionSigned>>,
    executed_blocks: &mut Vec<ExecutedBlock>,
    reth_env: &RethEnv,
    canonical_in_memory_state: &CanonicalInMemoryState,
    build_scope: &mut OutputBuildScope,
    timings: &mut OutputTimings,
    consensus_number: u64,
) -> EngineResult<SealedHeader> {
    // the payload moves into the build: keep its consensus ids for the log line
    let batch_index = payload.batch_index;
    let batch_digest = payload.batch_digest;
    // execute
    let (next_canonical_block, block_timings) =
        reth_env.build_block_from_recovered_payload(payload, recovered, build_scope)?;
    timings.add(&block_timings);
    trace!(target: "engine::dump", ?next_canonical_block, "block executed");

    // update header for next block execution in loop
    let canonical_header = next_canonical_block.recovered_block.clone_sealed_header();
    info!(
        target: "engine",
        hash = canonical_header.hash().to_string(),
        number = canonical_header.number,
        consensus_number,
        batch_index,
        batch_digest = %batch_digest,
        "next block executed"
    );
    crate::metrics::ENGINE_METRICS.blocks_executed_total.increment(1);
    crate::metrics::ENGINE_METRICS.block_gas_used.record(canonical_header.gas_used as f64);
    // Eagerly advance the shared in-memory state. This is load-bearing within a multi-block
    // output: the next block in the loop resolves its parent state through this advance
    // (account/storage/bytecode reads that miss the scope's read cache go to reth's memory
    // overlay; the state root itself runs over the scope's trie overlay instead, see #1301). The
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

/// Count and log a payload whose EVM `timestamp` is not the output's `committed_at`.
///
/// Once the sub-second timestamp fork is active for the leader's epoch, `TNPayload::new` clamps
/// the timestamp up to the parent block's so the EVM chain cannot move backwards. Consensus is
/// meant to produce non-decreasing commit times on its own, so a clamp that changes the value
/// means consensus let commit time go backwards, with one exception. Epoch 0 has no commit floor,
/// and no vote checks a header against the genesis `timestamp`, which the host that built genesis
/// stamped from its own clock. When the fork is active from genesis and the validators' clocks lag
/// that timestamp at launch, the first commits of epoch 0 fall below it and are clamped although
/// consensus is correct. The block stays deterministic either way (every node applies the same
/// clamp to the same inputs); the counter and warning keep a consensus bug visible.
fn report_timestamp_clamp(payload: &TNPayload, output: &ConsensusOutput) {
    let committed_at = output.committed_at();
    if payload.timestamp == committed_at {
        return;
    }

    crate::metrics::ENGINE_METRICS.evm_timestamp_clamped_total.increment(1);
    warn!(
        target: "engine",
        block = payload.parent_header.number.saturating_add(1),
        committed_at,
        timestamp = payload.timestamp,
        "evm timestamp clamped to parent"
    );
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
pub(crate) fn rollback_in_memory_output(
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

    /// The laps sum across an output's blocks, the maxima keep the slowest block's root and evm
    /// laps, and `laps` is the four sums together.
    #[test]
    fn test_output_timings_add_sums_laps_and_keeps_block_maxima() {
        let ms = Duration::from_millis;
        let mut timings = OutputTimings::default();
        timings.add(&BlockBuildTimings { recover: ms(1), evm: ms(10), root: ms(7), finish: ms(2) });
        timings.add(&BlockBuildTimings { recover: ms(3), evm: ms(4), root: ms(9), finish: ms(5) });

        assert_eq!(
            timings,
            OutputTimings {
                recover_wait: ms(4),
                evm: ms(14),
                root: ms(16),
                finish: ms(7),
                max_block_root: ms(9),
                max_block_evm: ms(10),
            }
        );
        assert_eq!(timings.laps(), ms(41));
    }
}
