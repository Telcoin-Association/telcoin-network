//! Prometheus metrics for the executor (subscriber).

use reth_metrics::{
    metrics::{Counter, Histogram},
    Metrics,
};

/// Metrics for the subscriber that assembles consensus outputs for execution.
///
/// Built fresh by `spawn_subscriber` each epoch - the registry identifies series by name,
/// so counters keep accumulating across epochs.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_executor")]
pub(crate) struct ExecutorMetrics {
    /// Total consensus outputs assembled and ready for execution.
    pub(crate) outputs_ready_total: Counter,
    /// Total transactions per consensus output.
    pub(crate) output_transactions: Histogram,
    /// Batches per consensus output.
    pub(crate) output_batches: Histogram,
    /// Failed attempts to fetch batches from workers for a consensus output.
    pub(crate) batch_fetch_failures_total: Counter,
    /// Protocol violations detected while assembling consensus outputs
    /// (certified batch missing from the certificate signers' workers).
    pub(crate) protocol_violations_total: Counter,
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
            // new_with_labels: Default::default() caches the first-bound recorder process-wide
            let metrics = ExecutorMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.outputs_ready_total.increment(1);
            metrics.output_transactions.record(42.0);
            metrics.output_batches.record(5.0);
            metrics.batch_fetch_failures_total.increment(1);
            metrics.protocol_violations_total.increment(1);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_executor.outputs_ready_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_executor.protocol_violations_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        find("tn_executor.output_transactions");
        find("tn_executor.output_batches");
        find("tn_executor.batch_fetch_failures_total");
    }
}
