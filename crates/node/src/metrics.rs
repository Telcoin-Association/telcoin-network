//! Prometheus metrics for the node's epoch lifecycle.

use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};

use crate::manager::RunEpochMode;

/// Metrics for the epoch manager.
///
/// One instance per process, owned by the `EpochManager`.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_epoch")]
pub(crate) struct EpochMetrics {
    /// The current epoch.
    pub(crate) current: Gauge,
    /// Unix timestamp (seconds) when the current epoch is scheduled to close.
    pub(crate) boundary_timestamp_seconds: Gauge,
    /// Consensus outputs replayed from the consensus chain after a restart.
    pub(crate) replayed_outputs_total: Counter,
}

impl EpochMetrics {
    /// Record a `run_epoch` iteration by mode.
    ///
    /// Counts epoch transitions AND mid-epoch restarts/mode changes - a node cycling
    /// through epochs faster than the epoch duration is recovering or flapping.
    pub(crate) fn record_epoch_run(&self, mode: &RunEpochMode) {
        let mode = match mode {
            RunEpochMode::Initial => "initial",
            RunEpochMode::NewEpoch => "new_epoch",
            RunEpochMode::ModeChange => "mode_change",
        };
        metrics::counter!("tn_epoch.runs_total", "mode" => mode).increment(1);
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
            let metrics = EpochMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.current.set(3.0);
            metrics.boundary_timestamp_seconds.set(1_700_000_000.0);
            metrics.replayed_outputs_total.increment(2);
            metrics.record_epoch_run(&RunEpochMode::Initial);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_epoch.current");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 3.0));
        let (key, _, _, value) = find("tn_epoch.runs_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "mode" && l.value() == "initial"));
        find("tn_epoch.boundary_timestamp_seconds");
        find("tn_epoch.replayed_outputs_total");
    }
}
