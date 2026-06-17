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
    }
}
