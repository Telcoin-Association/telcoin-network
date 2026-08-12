//! Prometheus metrics for the worker.

use crate::quorum_waiter::QuorumWaiterError;
use reth_metrics::{
    metrics::{Counter, Histogram},
    Metrics,
};
use std::time::Duration;
use tn_types::WorkerId;

/// Why the worker dropped a sealed batch's transactions instead of handing them to the
/// transaction forwarder.
///
/// Both variants are whole-batch losses on the observer path (`disburse_txns`): the
/// transactions were accepted by this node's RPC and never reached the forwarder, so the
/// forwarder-side series (`tn_reth.forwarded_txns_*`) cannot see them (issue #1133).
#[derive(Clone, Copy, Debug)]
pub(crate) enum ForwardDropReason {
    /// No committee validator advertised a JSON-RPC endpoint to forward to.
    NoEndpointAdvertised,
    /// Discovering the committee's advertised endpoints failed outright.
    DiscoveryFailed,
}

impl ForwardDropReason {
    /// The `reason` label value this variant records under.
    const fn label(self) -> &'static str {
        match self {
            Self::NoEndpointAdvertised => "no_endpoint_advertised",
            Self::DiscoveryFailed => "discovery_failed",
        }
    }
}

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

    /// Record transactions dropped by the worker before they reached the transaction
    /// forwarder, labeled by [`ForwardDropReason`].
    ///
    /// Modeled on [`Self::record_batches_fetched`]: a zero count records nothing, so a path
    /// that never dropped creates no series. Every call site pairs with a `warn!` line; the
    /// counter is what makes the loss visible on a dashboard, where an observer that drops
    /// every transaction it accepts otherwise looks healthy (issue #1133).
    pub(crate) fn record_forward_dropped(&self, reason: ForwardDropReason, count: usize) {
        if count > 0 {
            metrics::counter!(
                "tn_worker.forwarded_txns_dropped_total",
                "worker" => self.worker_id.to_string(),
                "reason" => reason.label(),
            )
            .increment(u64::try_from(count).unwrap_or(u64::MAX));
        }
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
            metrics.record_forward_dropped(ForwardDropReason::NoEndpointAdvertised, 4);
            metrics.record_forward_dropped(ForwardDropReason::DiscoveryFailed, 0); // no-op, no series
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

        let (key, _, _, value) = find("tn_worker.forwarded_txns_dropped_total");
        assert!(matches!(value, DebugValue::Counter(4)));
        assert!(key
            .key()
            .labels()
            .any(|l| l.key() == "reason" && l.value() == "no_endpoint_advertised"));
        assert!(key.key().labels().any(|l| l.key() == "worker" && l.value() == "0"));

        // the zero-count fetch must not create a series
        assert!(
            !snapshot.iter().any(|(key, ..)| key.key().name() == "tn_worker.batches_fetched_total"
                && key.key().labels().any(|l| l.value() == "remote")),
            "zero-count fetch should not register a remote series"
        );

        // neither must the zero-count forward drop
        assert!(
            !snapshot.iter().any(|(key, ..)| {
                key.key().name() == "tn_worker.forwarded_txns_dropped_total"
                    && key.key().labels().any(|l| l.value() == "discovery_failed")
            }),
            "zero-count forward drop should not register a series"
        );
    }
}
