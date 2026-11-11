//! Prometheus metrics for the node's epoch lifecycle and runtime switches.

use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};
use std::time::Duration;
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

/// Gauge: the closing output's number minus the last output the engine had finished when this node
/// detected the close. It counts the closing output itself, so 1 means only the closing output was
/// left to execute.
///
/// The close is "detected" once the boundary arm has handed the closing output to the engine, for
/// this gauge and the two below.
pub(crate) const EPOCH_CLOSE_BACKLOG_GAUGE: &str = "tn_node.epoch_close_backlog_outputs";
/// Gauge: seconds from the closing output's commit timestamp to this node detecting the close. A
/// send blocked on a full engine channel counts here, not in the drain.
pub(crate) const EPOCH_CLOSE_DETECT_LAG_GAUGE: &str = "tn_node.epoch_close_detect_lag_seconds";
/// Gauge: seconds from this node detecting the close to the end of `close_epoch`: the wait for the
/// closing output's execution plus the next-epoch base-fee computation after it.
pub(crate) const EPOCH_CLOSE_DRAIN_GAUGE: &str = "tn_node.epoch_close_drain_seconds";
/// Counter: epoch records this node published without casting its own vote, by `reason`.
pub(crate) const EPOCH_RECORD_VOTE_SKIPPED_COUNTER: &str =
    "tn_node.epoch_record_vote_skipped_total";

/// Why this node casts no vote for an epoch record it publishes to its vote collector.
///
/// One variant per skip in the collector (`spawn_epoch_vote_collector`), in the order it checks
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EpochRecordVoteSkip {
    /// The record already had its certificate when this node closed the epoch: the network
    /// certified it without this node's vote.
    CertifiedBeforeClose,
    /// This node's key is not in the record's committee (an observer, or a member ejected during
    /// the epoch).
    NotInCommittee,
}

impl EpochRecordVoteSkip {
    /// The skip for publishing `committee`'s record, `certified` or not, as the node holding `me`;
    /// `None` when the collector will vote.
    pub(crate) fn for_record(
        certified: bool,
        committee: &[tn_types::BlsPublicKey],
        me: &tn_types::BlsPublicKey,
    ) -> Option<Self> {
        if certified {
            Some(Self::CertifiedBeforeClose)
        } else if !committee.contains(me) {
            Some(Self::NotInCommittee)
        } else {
            None
        }
    }

    /// The `reason` label value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CertifiedBeforeClose => "certified_before_close",
            Self::NotInCommittee => "not_in_committee",
        }
    }
}

/// The engine backlog, in outputs, when the epoch's closing output numbered `closing_number` was
/// detected: the closing number minus the last output the engine finished.
///
/// Before the engine has finished an output in this process, this falls back to the backlog's own
/// reading ([`tn_types::ExecutionBacklog::outputs_behind`]).
pub(crate) fn epoch_close_backlog_outputs(
    closing_number: u64,
    backlog: &tn_types::ExecutionBacklog,
) -> u64 {
    match backlog.last_executed() {
        Some(executed) => closing_number.saturating_sub(executed),
        None => backlog.outputs_behind(),
    }
}

/// Set the two gauges read when the epoch's close is detected: the engine backlog in outputs and
/// the lag from the closing output's commit to its detection.
pub(crate) fn record_epoch_close_detected(backlog_outputs: u64, detect_lag: Duration) {
    metrics::gauge!(EPOCH_CLOSE_BACKLOG_GAUGE).set(backlog_outputs as f64);
    metrics::gauge!(EPOCH_CLOSE_DETECT_LAG_GAUGE).set(detect_lag.as_secs_f64());
}

/// Set the drain gauge: the time from detecting the close to the closing output executed.
pub(crate) fn record_epoch_close_drain(drain: Duration) {
    metrics::gauge!(EPOCH_CLOSE_DRAIN_GAUGE).set(drain.as_secs_f64());
}

/// Count one epoch record published without this node's vote.
pub(crate) fn record_epoch_record_vote_skipped(reason: EpochRecordVoteSkip) {
    metrics::counter!(EPOCH_RECORD_VOTE_SKIPPED_COUNTER, "reason" => reason.as_str()).increment(1);
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

    // ---- bench6 tracker-drain ----

    /// A closing output detected with a backlog of 3 outputs and a detection lag of 2 s, then
    /// drained in 4.5 s, sets each gauge under its exact name; each record-side skip counts under
    /// its own series.
    #[test]
    fn test_epoch_close_gauges_and_record_counters() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let backlog = tn_types::ExecutionBacklog::new();
        backlog.record_executed(97);
        let outputs = epoch_close_backlog_outputs(100, &backlog);
        assert_eq!(outputs, 3);

        metrics::with_local_recorder(&recorder, || {
            record_epoch_close_detected(outputs, Duration::from_secs(2));
            record_epoch_close_drain(Duration::from_millis(4_500));
            record_epoch_record_vote_skipped(EpochRecordVoteSkip::CertifiedBeforeClose);
            record_epoch_record_vote_skipped(EpochRecordVoteSkip::CertifiedBeforeClose);
            record_epoch_record_vote_skipped(EpochRecordVoteSkip::NotInCommittee);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let value = |name: &str, reason: Option<&str>| {
            snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == name
                        && reason.is_none_or(|reason| {
                            key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
                        })
                })
                .map(|(.., value)| match value {
                    DebugValue::Gauge(g) => g.0,
                    DebugValue::Counter(c) => *c as f64,
                    other => panic!("{name} is a {other:?}"),
                })
                .unwrap_or_else(|| panic!("no {name} {reason:?} series"))
        };
        assert_eq!(value("tn_node.epoch_close_backlog_outputs", None), 3.0);
        assert_eq!(value("tn_node.epoch_close_detect_lag_seconds", None), 2.0);
        assert_eq!(value("tn_node.epoch_close_drain_seconds", None), 4.5);
        assert_eq!(
            value("tn_node.epoch_record_vote_skipped_total", Some("certified_before_close")),
            2.0
        );
        assert_eq!(value("tn_node.epoch_record_vote_skipped_total", Some("not_in_committee")), 1.0);
    }

    /// Before the engine has finished an output the backlog is the engine's own reading; after,
    /// it is the closing number minus the last finished output, never below zero.
    #[test]
    fn test_epoch_close_backlog_outputs() {
        let backlog = tn_types::ExecutionBacklog::new();
        assert_eq!(epoch_close_backlog_outputs(100, &backlog), 0);
        backlog.record_queued(2);
        assert_eq!(epoch_close_backlog_outputs(100, &backlog), 2);
        backlog.record_executed(100);
        assert_eq!(epoch_close_backlog_outputs(100, &backlog), 0);
        backlog.record_executed(101);
        assert_eq!(epoch_close_backlog_outputs(100, &backlog), 0);
    }

    /// The skip follows the collector's gates in its order: a certified record first, then
    /// committee membership.
    #[test]
    fn test_epoch_record_vote_skip_follows_the_collector() {
        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::BlsKeypair;
        let key = |seed: u8| *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public();
        let (me, other) = (key(1), key(2));
        let with_me = [me, other];
        let without_me = [other];
        assert_eq!(EpochRecordVoteSkip::for_record(false, &with_me, &me), None);
        assert_eq!(
            EpochRecordVoteSkip::for_record(true, &with_me, &me),
            Some(EpochRecordVoteSkip::CertifiedBeforeClose)
        );
        assert_eq!(
            EpochRecordVoteSkip::for_record(false, &without_me, &me),
            Some(EpochRecordVoteSkip::NotInCommittee)
        );
        assert_eq!(
            EpochRecordVoteSkip::for_record(true, &without_me, &me),
            Some(EpochRecordVoteSkip::CertifiedBeforeClose)
        );
    }
}
