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
    /// Total transactions skipped because a validated peer batch already carries them (#1329).
    pub(crate) peer_deferred_txs_total: Counter,
    /// Total transactions evicted for whole-batch limit violations, excluding descendants.
    pub(crate) unpackable_txs_total: Counter,
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
    ///
    /// The `consensus_shutting_down` reason counts seals the worker refused because this epoch's
    /// consensus shutdown had begun. It is expected a few times per worker at every healthy epoch
    /// boundary, so alerts on this counter should exclude it.
    pub(crate) fn record_seal_failure(&self, worker_id: WorkerId, error: &BlockSealError) {
        let reason = match error {
            BlockSealError::QuorumRejected => "quorum_rejected",
            BlockSealError::AntiQuorum => "anti_quorum",
            BlockSealError::Timeout => "timeout",
            BlockSealError::NotValidator => "not_validator",
            BlockSealError::FailedToReport => "failed_to_report",
            BlockSealError::FailedQuorum => "failed_quorum",
            BlockSealError::FatalDBFailure => "fatal_db",
            BlockSealError::ConsensusShuttingDown => "consensus_shutting_down",
        };
        metrics::counter!(
            "tn_batch_builder.seal_failures_total",
            "worker" => worker_id.to_string(),
            "reason" => reason,
        )
        .increment(1);
    }
}

/// Set the pending pool gauge for `worker_id` when an epoch entry does not start its batch builder.
///
/// Writes the same series, with the same `worker` label, as the gauge a running batch builder
/// refreshes on every poll. The node calls this for a worker whose batch builder it did not start,
/// so the reading is a snapshot of the pool at that epoch entry. It is not refreshed until a batch
/// builder runs for the worker, apart from the new snapshot each later entry without one takes.
pub fn record_pending_pool_transactions(worker_id: WorkerId, pending: usize) {
    metrics::gauge!(
        "tn_batch_builder.pending_pool_transactions",
        "worker" => worker_id.to_string(),
    )
    .set(pending as f64);
}

/// Set the base fee gauge for `worker_id` when an epoch entry does not start its batch builder.
///
/// Writes the same series, with the same `worker` label, as the gauge a batch builder sets once
/// when it is created. Without this write the gauge keeps the last builder's fee, which is stale
/// once the worker crosses an epoch boundary without a builder, and a process that never started
/// a builder for the worker does not register the series at all.
pub fn record_base_fee(worker_id: WorkerId, base_fee: u64) {
    metrics::gauge!("tn_batch_builder.base_fee", "worker" => worker_id.to_string())
        .set(base_fee as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let metrics = BatchBuilderMetrics::new_for_worker(0);
            metrics.base_fee.set(1_000.0);
            metrics.pending_pool_transactions.set(3.0);
            metrics.batches_sealed_total.increment(1);
            metrics.unpackable_txs_total.increment(2);
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

        let (_, _, _, value) = find("tn_batch_builder.unpackable_txs_total");
        assert!(matches!(value, DebugValue::Counter(2)));

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

    /// The node's write for a worker without a batch builder lands on the builder's own series.
    #[test]
    fn test_record_pending_pool_transactions_shares_builder_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            BatchBuilderMetrics::new_for_worker(7).pending_pool_transactions.set(3.0);
            record_pending_pool_transactions(7, 11);
        });

        assert_one_worker_gauge(
            snapshotter.snapshot(),
            "tn_batch_builder.pending_pool_transactions",
            11.0,
        );
    }

    /// The node's base fee write for a worker without a batch builder lands on the builder's own
    /// series.
    #[test]
    fn test_record_base_fee_shares_builder_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            BatchBuilderMetrics::new_for_worker(7).base_fee.set(1_000.0);
            record_base_fee(7, 2_000);
        });

        assert_one_worker_gauge(snapshotter.snapshot(), "tn_batch_builder.base_fee", 2_000.0);
    }

    /// Assert `name` has exactly one series, labeled only `worker="7"`, holding `expected`.
    ///
    /// A second series would mean the free function and the builder's handle disagree on the
    /// metric name or label, so the builder's series would go stale while no builder runs.
    fn assert_one_worker_gauge(snapshot: Snapshot, name: &str, expected: f64) {
        let series: Vec<_> =
            snapshot.into_vec().into_iter().filter(|(key, ..)| key.key().name() == name).collect();
        assert_eq!(series.len(), 1, "{name} must have exactly one series, got {series:?}");
        let (key, _, _, value) = &series[0];
        let labels: Vec<_> = key.key().labels().map(|l| (l.key(), l.value())).collect();
        assert_eq!(labels, [("worker", "7")]);
        assert!(
            matches!(value, DebugValue::Gauge(g) if g.0 == expected),
            "{name} must hold the last write {expected}, got {value:?}"
        );
    }
}
