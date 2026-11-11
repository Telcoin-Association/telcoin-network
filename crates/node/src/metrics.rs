//! Prometheus metrics for the node's epoch lifecycle and runtime switches.

use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};
use tn_config::SwitchValue;

use crate::manager::RunEpochMode;

/// The gauge that exports one runtime switch from `parameters.yaml`, labelled `name` with the
/// switch's YAML key.
pub(crate) const NODE_SWITCH_GAUGE: &str = "tn_node_switch";

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

    /// Record one retried pinned chain read at an epoch seam, labelled by which read it was.
    ///
    /// Emitted by `retry_provider_faults` on every node-local provider fault it absorbs. Once
    /// such a fault is survivable it becomes invisible until it is not, so a node quietly
    /// retrying at every boundary needs to show up somewhere other than a `warn!` line.
    ///
    /// An associated function, not a method: the retry helper is shared by the epoch entry,
    /// record, and close paths and holds no `EpochMetrics`.
    pub(crate) fn record_provider_fault_retry(read: &'static str) {
        metrics::counter!("tn_epoch.provider_fault_retries_total", "read" => read).increment(1);
    }
}

/// Export each runtime switch as `tn_node_switch{name="<yaml key>"}`.
///
/// The value is [`SwitchValue::gauge`]: 0 or 1 for an on/off switch, a count as is, and a
/// duration in milliseconds. Set once at start, after `parameters.yaml` is loaded, so a scrape
/// shows which benchmark arm the node runs. A switch takes effect only at restart, so the value
/// does not change while the process lives.
pub(crate) fn record_node_switches(switches: &[(&'static str, SwitchValue)]) {
    for (name, value) in switches {
        metrics::gauge!(NODE_SWITCH_GAUGE, "name" => *name).set(value.gauge());
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
            EpochMetrics::record_provider_fault_retry("epoch-entry state read");
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
        let (key, _, _, value) = find("tn_epoch.provider_fault_retries_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key
            .key()
            .labels()
            .any(|l| l.key() == "read" && l.value() == "epoch-entry state read"));
    }

    // ---- bench6 bench6-switches ----

    /// Every switch exports one `tn_node_switch` series labelled with its YAML key, valued as
    /// 0/1, a count, or milliseconds.
    #[test]
    fn test_node_switch_gauge_per_switch() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let parameters = tn_config::Parameters {
            quorum_exec_block: true,
            recovery_threads: 3,
            parent_grace: std::time::Duration::from_millis(150),
            rpc_admission_backlog_outputs: 2,
            ..Default::default()
        };
        let switches = parameters.switches();

        metrics::with_local_recorder(&recorder, || record_node_switches(&switches));

        let snapshot = snapshotter.snapshot().into_vec();
        let gauge = |switch: &str| {
            snapshot
                .iter()
                .filter(|(key, ..)| key.key().name() == NODE_SWITCH_GAUGE)
                .find(|(key, ..)| {
                    key.key().labels().any(|l| l.key() == "name" && l.value() == switch)
                })
                .map(|(.., value)| match value {
                    DebugValue::Gauge(g) => g.0,
                    other => panic!("{switch} is not a gauge: {other:?}"),
                })
                .unwrap_or_else(|| panic!("no tn_node_switch series for {switch}"))
        };
        assert_eq!(
            snapshot.iter().filter(|(key, ..)| key.key().name() == NODE_SWITCH_GAUGE).count(),
            switches.len()
        );
        assert_eq!(gauge("quorum_exec_block"), 1.0);
        assert_eq!(gauge("vote_queue_fix"), 0.0);
        assert_eq!(gauge("recovery_threads"), 3.0);
        assert_eq!(gauge("parent_grace"), 150.0);
        assert_eq!(gauge("rpc_admission_backlog_outputs"), 2.0);
        assert_eq!(gauge("pipelined_persist"), 0.0);
    }
}
