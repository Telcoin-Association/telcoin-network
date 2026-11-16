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
    /// Stored epoch records whose `final_state` is not the closing block this node executed for
    /// the epoch, and certified records from peers that conflict with this node's own record for
    /// an epoch (counted by state-sync's epoch record collector under this name). Each one stops
    /// the node.
    pub(crate) final_state_mismatches_total: Counter,
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

    /// Record one run of the closing-block check on an epoch record's `final_state`, labelled
    /// `ok` or with the static name of what failed: `no_block` (nothing executed), `beacon_root`,
    /// `nonce_epoch`, `extra_data_length` or `extra_data_seed`.
    ///
    /// Counted on every path the check runs: the record build at the close, the re-derive of a
    /// record a kill left unwritten, a stored record reused at the close, and the previous
    /// epoch's record at every epoch entry. A close that finds this node's own uncertified record
    /// 0 checks it as a stored record and then builds it again, so that close counts twice.
    /// Anything but `ok` stops the node. An associated function for the same reason as
    /// [`Self::record_provider_fault_retry`]: the check is a free function.
    pub(crate) fn record_closing_block_check(result: &'static str) {
        metrics::counter!("tn_epoch.epoch_record_closing_block_check_total", "result" => result)
            .increment(1);
    }

    /// Record one run of the close-time base-fee identity check (canonical tip + 1 equals the
    /// entered epoch's `blockHeight`), labelled `ok` when it holds and `mismatch` when it does
    /// not.
    ///
    /// Not counted when the epoch-info read behind it fails: the identity was not compared.
    pub(crate) fn record_close_identity_check(holds: bool) {
        let result = if holds { "ok" } else { "mismatch" };
        metrics::counter!("tn_epoch.epoch_close_identity_check_total", "result" => result)
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
            let metrics = EpochMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.current.set(3.0);
            metrics.boundary_timestamp_seconds.set(1_700_000_000.0);
            metrics.replayed_outputs_total.increment(2);
            metrics.final_state_mismatches_total.increment(1);
            metrics.record_epoch_run(&RunEpochMode::Initial);
            EpochMetrics::record_provider_fault_retry("epoch-entry state read");
            EpochMetrics::record_closing_block_check("ok");
            EpochMetrics::record_closing_block_check("ok");
            EpochMetrics::record_closing_block_check("beacon_root");
            EpochMetrics::record_close_identity_check(true);
            EpochMetrics::record_close_identity_check(false);
            EpochMetrics::record_close_identity_check(false);
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
        let (_, _, _, value) = find("tn_epoch.final_state_mismatches_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (key, _, _, value) = find("tn_epoch.provider_fault_retries_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key
            .key()
            .labels()
            .any(|l| l.key() == "read" && l.value() == "epoch-entry state read"));

        // the two check counters: one series per `result` label
        let count = |name: &str, result: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == name
                        && key.key().labels().any(|l| l.key() == "result" && l.value() == result)
                })
                .and_then(|(_, _, _, value)| match value {
                    DebugValue::Counter(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("metric {name}{{result={result}}} not registered"))
        };
        let check = "tn_epoch.epoch_record_closing_block_check_total";
        assert_eq!(count(check, "ok"), 2);
        assert_eq!(count(check, "beacon_root"), 1);
        let identity = "tn_epoch.epoch_close_identity_check_total";
        assert_eq!(count(identity, "ok"), 1);
        assert_eq!(count(identity, "mismatch"), 2);
    }
}
