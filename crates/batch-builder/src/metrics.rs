//! Prometheus metrics for the batch builder.

use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};
use tn_types::{error::BlockSealError, WorkerId};

/// Metrics for the batch builder, labeled per `worker`.
///
/// Built once per epoch in [`BatchBuilder::new`](crate::BatchBuilder::new) via
/// `new_with_labels` - counters keep accumulating across epochs because the underlying
/// series are identified by (name, labels) in the global registry.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_batch_builder")]
pub(crate) struct BatchBuilderMetrics {
    /// Number of transactions in the pending pool at the last batch-builder poll.
    pub(crate) pending_pool_transactions: Gauge,
    /// The base fee for the current epoch (constant for the batch builder's lifetime).
    pub(crate) base_fee: Gauge,
    /// Total number of batches sealed (worker acked quorum).
    pub(crate) batches_sealed_total: Counter,
    /// Time from spawning a batch build until the worker's quorum ack resolves.
    pub(crate) seal_duration_seconds: Histogram,
}

impl BatchBuilderMetrics {
    /// Create the metrics handles for `worker_id`.
    pub(crate) fn new_for_worker(worker_id: WorkerId) -> Self {
        Self::new_with_labels(&[("worker", worker_id.to_string())])
    }

    /// Record a failed seal attempt by failure reason.
    ///
    /// Uses the `metrics!` macro because the `reason` label is per-event; the series
    /// still lives in the same registry as the derive-backed handles.
    pub(crate) fn record_seal_failure(&self, worker_id: WorkerId, error: &BlockSealError) {
        let reason = match error {
            BlockSealError::QuorumRejected => "quorum_rejected",
            BlockSealError::AntiQuorum => "anti_quorum",
            BlockSealError::Timeout => "timeout",
            BlockSealError::NotValidator => "not_validator",
            BlockSealError::FailedToReport => "failed_to_report",
            BlockSealError::FailedQuorum => "failed_quorum",
            BlockSealError::FatalDBFailure => "fatal_db",
        };
        metrics::counter!(
            "tn_batch_builder.seal_failures_total",
            "worker" => worker_id.to_string(),
            "reason" => reason,
        )
        .increment(1);
    }
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
            let metrics = BatchBuilderMetrics::new_for_worker(0);
            metrics.base_fee.set(1_000.0);
            metrics.pending_pool_transactions.set(3.0);
            metrics.batches_sealed_total.increment(1);
            metrics.seal_duration_seconds.record(0.25);
            metrics.record_seal_failure(0, &BlockSealError::Timeout);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_batch_builder.batches_sealed_total");
        assert!(matches!(value, DebugValue::Counter(1)));

        let (_, _, _, value) = find("tn_batch_builder.base_fee");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 1_000.0));

        let (key, _, _, value) = find("tn_batch_builder.seal_failures_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(
            key.key().labels().any(|l| l.key() == "reason" && l.value() == "timeout"),
            "seal failure counter must carry a reason label"
        );

        find("tn_batch_builder.pending_pool_transactions");
        find("tn_batch_builder.seal_duration_seconds");
    }
}
