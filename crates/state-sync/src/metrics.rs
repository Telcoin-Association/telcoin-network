//! Prometheus metrics for state sync (consensus catch-up).

use reth_metrics::{metrics::Counter, Metrics};
use std::sync::LazyLock;

/// Process-wide state-sync metrics.
///
/// A `LazyLock` static is justified here: the sync paths are free functions and there is
/// one syncing node per process. First use must happen after the global recorder is
/// installed (the CLI installs it before any sync task spawns).
pub(crate) static STATE_SYNC_METRICS: LazyLock<StateSyncMetrics> =
    LazyLock::new(StateSyncMetrics::default);

/// Metrics for consensus catch-up and epoch pack retrieval.
#[derive(Metrics)]
#[metrics(scope = "tn_state_sync")]
pub(crate) struct StateSyncMetrics {
    /// Catch-up iterations that made no progress (consensus header cache miss).
    pub(crate) no_progress_total: Counter,
    /// Consensus headers applied during catch-up.
    pub(crate) headers_fetched_total: Counter,
    /// Epoch pack file fetches started.
    pub(crate) epoch_pack_fetches_total: Counter,
    /// State-sync requests re-driven after a catch-up stall (issue #836).
    pub(crate) catch_up_redrives_total: Counter,
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
    }
}
