//! Prometheus metrics for the worker.

use crate::quorum_waiter::QuorumWaiterError;
use reth_metrics::{
    metrics::{Counter, Histogram},
    Metrics,
};
use std::time::Duration;
use tn_types::WorkerId;

/// Derive-backed metric handles for the worker, labeled per `worker`.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_worker")]
struct WorkerMetricHandles {
    /// Total batches sealed by this worker (quorum reached).
    batches_sealed_total: Counter,
    /// Size in bytes of sealed batches.
    batch_size_bytes: Histogram,
    /// Number of transactions per sealed batch.
    batch_transactions: Histogram,
    /// Time waiting for a proposed batch to reach quorum (or fail).
    quorum_wait_seconds: Histogram,
    /// Retries while reporting a batch to a single committee peer.
    batch_report_retries_total: Counter,
    /// Batches received from peers that failed validation.
    batch_validation_failures_total: Counter,
    /// Time to fetch all missing batches for a consensus output.
    batch_fetch_duration_seconds: Histogram,
}

/// Metrics for the worker batch pipeline.
///
/// One instance per worker id; cheap to clone (all handles are `Arc`-backed). Components
/// that construct this independently for the same worker share the same underlying series -
/// the registry identifies series by (name, labels).
#[derive(Clone, Debug)]
pub struct WorkerMetrics {
    /// The derive-backed metric handles.
    handles: WorkerMetricHandles,
    /// The worker id, kept for per-event labeled counters recorded via the `metrics!` macros.
    worker_id: WorkerId,
}

impl WorkerMetrics {
    /// Create the metric handles for `worker_id`.
    pub fn new_for_worker(worker_id: WorkerId) -> Self {
        Self {
            handles: WorkerMetricHandles::new_with_labels(&[("worker", worker_id.to_string())]),
            worker_id,
        }
    }

    /// Record a successfully sealed batch.
    pub(crate) fn record_batch_sealed(&self, batch_size: usize, num_txs: usize) {
        self.handles.batches_sealed_total.increment(1);
        self.handles.batch_size_bytes.record(batch_size as f64);
        self.handles.batch_transactions.record(num_txs as f64);
    }

    /// Record the time a batch proposal waited on quorum (success or failure).
    pub(crate) fn record_quorum_wait(&self, elapsed: Duration) {
        self.handles.quorum_wait_seconds.record(elapsed);
    }

    /// Record a failed quorum attempt by failure reason.
    pub(crate) fn record_quorum_failure(&self, error: &QuorumWaiterError) {
        let reason = match error {
            QuorumWaiterError::QuorumRejected => "rejected",
            QuorumWaiterError::AntiQuorum => "anti_quorum",
            QuorumWaiterError::Timeout => "timeout",
            QuorumWaiterError::Network => "network",
            QuorumWaiterError::Rpc(_) => "rpc",
            QuorumWaiterError::DroppedReceiver => "dropped",
        };
        metrics::counter!(
            "tn_worker.quorum_failures_total",
            "worker" => self.worker_id.to_string(),
            "reason" => reason,
        )
        .increment(1);
    }

    /// Record a retry while reporting a batch to a single peer.
    pub(crate) fn record_batch_report_retry(&self) {
        self.handles.batch_report_retries_total.increment(1);
    }

    /// Record a peer batch that failed validation.
    pub(crate) fn record_batch_validation_failure(&self) {
        self.handles.batch_validation_failures_total.increment(1);
    }

    /// Record batches recovered while fetching payloads for the primary.
    pub(crate) fn record_batches_fetched(&self, source: &'static str, count: usize) {
        if count == 0 {
            return;
        }
        metrics::counter!(
            "tn_worker.batches_fetched_total",
            "worker" => self.worker_id.to_string(),
            "source" => source,
        )
        .increment(count as u64);
    }

    /// Record the total duration of a payload fetch for the primary.
    pub(crate) fn record_batch_fetch_duration(&self, elapsed: Duration) {
        self.handles.batch_fetch_duration_seconds.record(elapsed);
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
            let metrics = WorkerMetrics::new_for_worker(0);
            metrics.record_batch_sealed(2_048, 7);
            metrics.record_quorum_wait(Duration::from_millis(120));
            metrics.record_quorum_failure(&QuorumWaiterError::Timeout);
            metrics.record_batch_report_retry();
            metrics.record_batch_validation_failure();
            metrics.record_batches_fetched("local", 3);
            metrics.record_batches_fetched("remote", 0); // no-op, no series
            metrics.record_batch_fetch_duration(Duration::from_millis(80));
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (key, _, _, value) = find("tn_worker.batches_sealed_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "worker" && l.value() == "0"));

        let (key, _, _, value) = find("tn_worker.quorum_failures_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "reason" && l.value() == "timeout"));

        let (key, _, _, value) = find("tn_worker.batches_fetched_total");
        assert!(matches!(value, DebugValue::Counter(3)));
        assert!(key.key().labels().any(|l| l.key() == "source" && l.value() == "local"));

        find("tn_worker.batch_size_bytes");
        find("tn_worker.batch_transactions");
        find("tn_worker.quorum_wait_seconds");
        find("tn_worker.batch_report_retries_total");
        find("tn_worker.batch_validation_failures_total");
        find("tn_worker.batch_fetch_duration_seconds");

        // the zero-count fetch must not create a series
        assert!(
            !snapshot.iter().any(|(key, ..)| key.key().name() == "tn_worker.batches_fetched_total"
                && key.key().labels().any(|l| l.value() == "remote")),
            "zero-count fetch should not register a remote series"
        );
    }
}
