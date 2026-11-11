//! Prometheus metrics for the execution engine.

use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};
use std::sync::LazyLock;

/// Process-wide engine metrics.
///
/// A `LazyLock` static is justified here: `execute_consensus_output` is a free function and
/// there is exactly one engine per process. First use must happen after the global recorder
/// is installed (the CLI installs it before any engine exists).
pub(crate) static ENGINE_METRICS: LazyLock<EngineMetrics> = LazyLock::new(EngineMetrics::default);

/// Metrics for consensus output execution.
#[derive(Metrics)]
#[metrics(scope = "tn_engine")]
pub(crate) struct EngineMetrics {
    /// The latest canonical block height produced by the engine.
    pub(crate) canonical_height: Gauge,
    /// Consensus outputs queued for execution (a growing backlog means execution
    /// is falling behind consensus).
    pub(crate) queued_outputs: Gauge,
    /// Consensus outputs fully executed.
    pub(crate) outputs_executed_total: Counter,
    /// Time to execute one consensus output (all batches).
    pub(crate) execution_duration_seconds: Histogram,
    /// Blocks executed and added to the canonical chain.
    pub(crate) blocks_executed_total: Counter,
    /// Gas used per executed block.
    pub(crate) block_gas_used: Histogram,
    /// Empty non-epoch-closing outputs skipped without producing a block.
    pub(crate) empty_outputs_skipped_total: Counter,
    /// Bounded retries of the durable output persist after a node-local provider fault
    /// (the engine halts only once the attempt budget is exhausted).
    pub(crate) persist_provider_fault_retries_total: Counter,
    /// Consensus outputs whose durable persist failed after every attempt, rolling back
    /// the speculative in-memory advance and escalating out of the engine task.
    pub(crate) persist_failures_total: Counter,
    /// Transactions observed in a batch whose first-ordered copy came from a different
    /// producer's batch inside the detection window (priority-fee poaching indicator,
    /// issue #1259).
    pub(crate) cross_producer_repacked_txs_total: Counter,
    /// EVM block timestamps clamped up to the parent's because the consensus commit time was
    /// behind it. Non-zero indicates a consensus bug, except for the first commits of epoch 0:
    /// epoch 0 has no commit floor, so they are clamped when validator clocks lag the genesis
    /// timestamp.
    pub(crate) evm_timestamp_clamped_total: Counter,
    /// Time a consensus output waited in the engine's queue, from being received off the
    /// consensus stream to its execution task being spawned.
    pub(crate) queue_wait_seconds: Histogram,
    /// Time to durably persist one executed consensus output (the whole bounded retry,
    /// including any retry backoff), recorded whether or not the persist succeeded.
    pub(crate) persist_duration_seconds: Histogram,
    /// Wall-clock milliseconds the engine spent running execution tasks. The busy fraction of
    /// the engine is `rate(tn_engine_busy_milliseconds_total[1m]) / 1000`.
    pub(crate) busy_milliseconds_total: Counter,
    // bench5 exec-split
    /// Signer recovery time of one consensus output: the duration of the background job that
    /// recovers the output's signers when it is queued (or of the inline recovery, on the
    /// fallback and for callers without a job), logged as `recover_ms` on the `signers
    /// recovered` line. The job runs beside the execution thread, overlapping earlier outputs'
    /// execution, so unlike the evm, root and finish histograms it does not sum to the output's
    /// execute time. On the global rayon pool (`recovery_threads: 0`, the line's `pool=global`)
    /// it shares threads with those builds, and that contention can stretch both it and their
    /// `finish` lap; on the dedicated pool (`pool=dedicated`) it includes any wait for a free
    /// pool thread, and the two pools no longer share rayon threads but still share CPU cores. The
    /// part the execution thread waited for is `recover_wait_ms` on the `output
    /// executed` line, which has no `recover_ms` field.
    pub(crate) recover_seconds: Histogram,
    /// Builder setup, pre-execution system calls and transaction loop of one consensus output,
    /// summed over its blocks.
    pub(crate) evm_seconds: Histogram,
    /// State-root time of one consensus output, summed over its blocks.
    pub(crate) root_seconds: Histogram,
    /// The rest of block finishing for one consensus output (closing system calls, bundle merge,
    /// hashed post-state, assembly, delta sorts, overlay extend), summed over its blocks.
    pub(crate) finish_seconds: Histogram,
    /// Transactions handed to the block builder by fully executed consensus outputs, counted
    /// before unrecoverable or invalid transactions are dropped (the per-transaction cost
    /// denominator).
    pub(crate) executed_transactions_total: Counter,
    /// Time from the node forwarding a consensus output (stamped just before its send on the
    /// `to_engine` channel) to the engine taking it off that channel, a send blocked on a full
    /// channel included. `queue_wait_seconds` covers the next hop.
    pub(crate) forward_wait_seconds: Histogram,
    /// Consensus outputs waiting in the `to_engine` channel behind the engine's own queue,
    /// sampled when an output arrives and when an execution completes.
    pub(crate) channel_depth: Gauge,
    // bench5 signer-reuse
    /// Consensus outputs whose background signer recovery ended without a result (the job
    /// panicked), so the execution task recovered the signers inline from the same bytes.
    /// Nonzero is a bug signal; the blocks built are the same either way.
    pub(crate) recovery_fallback_total: Counter,
    // bench6 pipelined-persist: updated by the pipelined loop only (`pipelined_persist: true`);
    // registered with the rest at first use, so a sequential node exports them at zero
    // (`built_height` then reads 0, below `canonical_height`)
    /// Consensus outputs started and not yet committed: building, built and waiting for the commit
    /// slot, or committing. Zero to two.
    pub(crate) unpersisted_outputs: Gauge,
    /// The engine's in-memory tip: the last block built, which leads `canonical_height` (the
    /// durable tip) by up to two outputs and equals it once the engine is idle.
    pub(crate) built_height: Gauge,
    /// Time from the end of an output's build to the start of its commit: the wait for the
    /// previous output's commit to finish (back-pressure of the single commit slot).
    pub(crate) persist_wait_seconds: Histogram,
    /// Time between two consecutive commits, recorded only when the later output had started
    /// building before the earlier one committed (the engine was saturated): the pipelined cycle.
    pub(crate) commit_interval_seconds: Histogram,
    /// Outputs held in the engine queue because a closing output had not committed yet (each held
    /// output counts once per close). Expected zero: the node forwards nothing past a close.
    pub(crate) close_barrier_holds_total: Counter,
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // construct directly (not via the static) so handles bind to the local recorder
            let metrics = EngineMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.canonical_height.set(123.0);
            metrics.queued_outputs.set(2.0);
            metrics.outputs_executed_total.increment(1);
            metrics.execution_duration_seconds.record(0.1);
            metrics.blocks_executed_total.increment(3);
            metrics.block_gas_used.record(21_000.0);
            metrics.empty_outputs_skipped_total.increment(1);
            metrics.persist_provider_fault_retries_total.increment(2);
            metrics.persist_failures_total.increment(1);
            metrics.cross_producer_repacked_txs_total.increment(4);
            metrics.evm_timestamp_clamped_total.increment(5);
            metrics.queue_wait_seconds.record(0.25);
            metrics.persist_duration_seconds.record(0.5);
            metrics.busy_milliseconds_total.increment(750);
            metrics.recover_seconds.record(0.01);
            metrics.evm_seconds.record(0.02);
            metrics.root_seconds.record(0.03);
            metrics.finish_seconds.record(0.04);
            metrics.executed_transactions_total.increment(12_500);
            metrics.forward_wait_seconds.record(0.05);
            metrics.channel_depth.set(64.0);
            metrics.recovery_fallback_total.increment(1);
            metrics.unpersisted_outputs.set(2.0);
            metrics.built_height.set(130.0);
            metrics.persist_wait_seconds.record(0.06);
            metrics.commit_interval_seconds.record(1.17);
            metrics.close_barrier_holds_total.increment(1);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_engine.canonical_height");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 123.0));
        let (_, _, _, value) = find("tn_engine.blocks_executed_total");
        assert!(matches!(value, DebugValue::Counter(3)));
        find("tn_engine.queued_outputs");
        find("tn_engine.outputs_executed_total");
        find("tn_engine.execution_duration_seconds");
        find("tn_engine.block_gas_used");
        find("tn_engine.empty_outputs_skipped_total");
        let (_, _, _, value) = find("tn_engine.persist_provider_fault_retries_total");
        assert!(matches!(value, DebugValue::Counter(2)));
        let (_, _, _, value) = find("tn_engine.persist_failures_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_engine.cross_producer_repacked_txs_total");
        assert!(matches!(value, DebugValue::Counter(4)));
        let (_, _, _, value) = find("tn_engine.evm_timestamp_clamped_total");
        assert!(matches!(value, DebugValue::Counter(5)));
        let (_, _, _, value) = find("tn_engine.queue_wait_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.25));
        let (_, _, _, value) = find("tn_engine.persist_duration_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.5));
        let (_, _, _, value) = find("tn_engine.busy_milliseconds_total");
        assert!(matches!(value, DebugValue::Counter(750)));
        let (_, _, _, value) = find("tn_engine.recover_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.01));
        let (_, _, _, value) = find("tn_engine.evm_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.02));
        let (_, _, _, value) = find("tn_engine.root_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.03));
        let (_, _, _, value) = find("tn_engine.finish_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.04));
        let (_, _, _, value) = find("tn_engine.executed_transactions_total");
        assert!(matches!(value, DebugValue::Counter(12_500)));
        let (_, _, _, value) = find("tn_engine.forward_wait_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.05));
        let (_, _, _, value) = find("tn_engine.channel_depth");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 64.0));
        let (_, _, _, value) = find("tn_engine.recovery_fallback_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_engine.unpersisted_outputs");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 2.0));
        let (_, _, _, value) = find("tn_engine.built_height");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 130.0));
        let (_, _, _, value) = find("tn_engine.persist_wait_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.06));
        let (_, _, _, value) = find("tn_engine.commit_interval_seconds");
        assert!(matches!(value, DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 1.17));
        let (_, _, _, value) = find("tn_engine.close_barrier_holds_total");
        assert!(matches!(value, DebugValue::Counter(1)));
    }
}
