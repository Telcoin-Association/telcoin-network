//! Prometheus metrics for state sync (consensus catch-up).

use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};
use std::sync::LazyLock;

/// Process-wide state-sync metrics.
///
/// A `LazyLock` static is justified here: the sync paths are free functions and there is
/// one syncing node per process. First use must happen after the global recorder is
/// installed (the CLI installs it before any sync task spawns).
pub(crate) static STATE_SYNC_METRICS: LazyLock<StateSyncMetrics> =
    LazyLock::new(StateSyncMetrics::default);

/// Metrics for consensus catch-up and epoch pack retrieval.
///
/// Cloning shares the registered handles: a clone writes the same series.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_state_sync")]
pub(crate) struct StateSyncMetrics {
    /// Catch-up iterations that made no progress (consensus header cache miss).
    pub(crate) no_progress_total: Counter,
    /// Consensus headers delivered to the subscriber during catch-up.
    pub(crate) headers_fetched_total: Counter,
    /// Epoch pack file fetches started.
    pub(crate) epoch_pack_fetches_total: Counter,
    /// State-sync requests re-driven after a catch-up stall (issue #836).
    pub(crate) catch_up_redrives_total: Counter,
    /// Consensus stream steps that failed and were retried after a backoff: a catch-up pass that
    /// returned an error (for example a consensus header digest mismatch) or a failed startup read
    /// of the last consensus header. A steadily rising count means the stream cannot advance.
    pub(crate) stream_errors_total: Counter,
    /// Warn intervals the catch-up loop spent blocked on the bounded `sync_output` send: the
    /// queue was full for the whole interval and the subscriber took nothing. Through an
    /// empty-output stretch that queue is the loop's only throttle, so a steadily rising count
    /// means the subscriber is not draining catch-up output.
    pub(crate) output_send_stalls_total: Counter,
    /// The highest epoch E such that this node holds a record and its certificate for every epoch
    /// `0..=E`, or `-1` when epoch 0 has no certificate (a fresh node still holding the unsigned
    /// genesis anchor). A certificate stored past a gap does not move it until the gap is filled.
    /// Read from the epoch store by the record collector after each pass and by the startup sync.
    pub(crate) epoch_records_certified_through: Gauge,
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
            let metrics = StateSyncMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.no_progress_total.increment(1);
            metrics.headers_fetched_total.increment(5);
            metrics.epoch_pack_fetches_total.increment(1);
            metrics.catch_up_redrives_total.increment(1);
            metrics.stream_errors_total.increment(1);
            metrics.output_send_stalls_total.increment(1);
            metrics.epoch_records_certified_through.set(-1.0);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_state_sync.headers_fetched_total");
        assert!(matches!(value, DebugValue::Counter(5)));
        find("tn_state_sync.no_progress_total");
        find("tn_state_sync.epoch_pack_fetches_total");
        find("tn_state_sync.catch_up_redrives_total");
        find("tn_state_sync.stream_errors_total");
        find("tn_state_sync.output_send_stalls_total");
        let (_, _, _, value) = find("tn_state_sync.epoch_records_certified_through");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == -1.0), "got {value:?}");
    }
}
