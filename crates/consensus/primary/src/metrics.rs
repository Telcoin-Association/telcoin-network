//! Prometheus metrics for the primary (consensus layer).

use crate::{ConsensusBusApp, NodeMode, RecentBlocks};
use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tn_types::{Noticer, Round, TaskSpawner};
use tokio::sync::watch;
use tracing::debug;

#[cfg(test)]
#[path = "tests/metrics_recorder.rs"]
pub(crate) mod test_recorder;

/// Process-wide running max of consensus output byte sizes.
///
/// The `metrics` `Gauge` handle has no getter, so the "max since startup" gauge needs an
/// external running max. There is one `PrimaryMetrics` per process, so a static is fine.
static MAX_CONSENSUS_OUTPUT_BYTES: AtomicU64 = AtomicU64::new(0);

/// Metrics for the primary's consensus pipeline.
///
/// A single instance lives on the application-lifetime consensus bus
/// ([`ConsensusBusApp`](crate::ConsensusBusApp)), which already reaches the proposer,
/// certifier, certificate manager, and consensus driver. Handles are `Arc`-backed and
/// cheap to clone.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_primary")]
pub struct PrimaryMetrics {
    /// Headers proposed by this primary.
    pub(crate) headers_proposed_total: Counter,
    /// Batch digests queued in the proposer awaiting inclusion in a header.
    pub(crate) proposer_pending_digests: Gauge,
    /// Votes received from peers on own header proposals.
    pub(crate) votes_received_total: Counter,
    /// Invalid votes received from peers on own header proposals.
    pub(crate) invalid_votes_total: Counter,
    /// Failed vote requests during header certification.
    pub(crate) vote_request_failures_total: Counter,
    /// Vote requests from peers answered with a recoverable "not yet" because the header was
    /// created further ahead of the local clock than the drift tolerance, but by no more than the
    /// tolerance plus the vote timeout.
    ///
    /// These carry no penalty, so a steady rate means a peer's clock (or this node's) is skewed.
    pub(crate) votes_deferred_future_header_total: Counter,
    /// Certificates formed from own headers reaching vote quorum.
    pub(crate) certificates_formed_total: Counter,
    /// Time to certify an own header proposal (collect a quorum of votes).
    pub(crate) certificate_form_duration_seconds: Histogram,
    /// Certificates received from peers and accepted for processing.
    pub(crate) certificates_received_total: Counter,
    /// Certificates parked awaiting missing parents.
    pub(crate) certificates_pending: Gauge,
    /// Certificate fetch operations started.
    pub(crate) certificate_fetches_total: Counter,
    /// Certificates retrieved via fetch operations.
    pub(crate) certificates_fetched_total: Counter,
    /// Duration of successful certificate fetch operations.
    pub(crate) certificate_fetch_duration_seconds: Histogram,
    /// Current primary round (mirrored from the round watch channel).
    pub(crate) round: Gauge,
    /// Highest committed round (mirrored from the committed-round watch channel).
    pub(crate) committed_round: Gauge,
    /// Distinct observed rounds finalized at the next round or the observation deadline.
    pub(crate) rounds_evaluated_total: Counter,
    /// Evaluated rounds below the committee quorum voting power, counting each author once.
    pub(crate) round_quorum_shortfalls_total: Counter,
    /// One while the active consensus task owns a quorum observer, otherwise zero.
    pub(crate) quorum_observation_active: Gauge,
    /// Unix seconds when the current active observer started, zero before startup.
    pub(crate) liveness_started_timestamp_seconds: Gauge,
    /// Unix seconds of the last successful subdag handoff this observer witnessed, or zero.
    pub(crate) last_commit_timestamp_seconds: Gauge,
    /// Committed subdags.
    pub(crate) subdags_committed_total: Counter,
    /// Time from leader header creation to subdag commit, in fractional seconds at millisecond
    /// resolution. Before the sub-second timestamp fork a leader's creation time is a whole
    /// second, so a pre-fork reading overstates the latency by up to one second.
    pub(crate) commit_latency_seconds: Histogram,
    /// The largest consensus output (in pack file encoded bytes) we have seen since startup.
    pub(crate) max_consensus_output_bytes: Gauge,
    /// The most recent consensus output (in pack file encoded bytes) we have seen.
    pub(crate) consensus_output_bytes: Gauge,
    /// Epoch records offered to a queued epoch pack request for a different epoch, and dropped.
    ///
    /// Every caller looks the record up by the request's epoch, so a non-zero value is a caller
    /// bug.
    pub(crate) epoch_pack_record_epoch_mismatch_total: Counter,
    // bench5 f1-commit-lag
    /// Commits the consensus task held because the committed leader round ran more than
    /// `MAX_COMMIT_RUN_AHEAD_ROUNDS` ahead of the leader round this node has executed, counted
    /// once per hold.
    pub(crate) commit_run_ahead_blocked_total: Counter,
    // bench5 f3-rejoin
    /// Vote requests refused because this node is not an active committee validator (inactive
    /// catch-up or observer).
    ///
    /// Such a node answers at once with a recoverable refusal instead of holding the request until
    /// the vote timeout, so this moves only while the node is not an active CVV, plus the rare
    /// request whose node was demoted before it signed.
    pub(crate) votes_refused_inactive_total: Counter,
    /// Vote requests refused because this node's execution did not reach the header's block
    /// within the voter's execution wait (`VOTE_EXECUTION_WAIT`).
    ///
    /// A steady rate means this node executes behind the committee: it answers with a
    /// recoverable refusal and votes on a retry once its execution has caught up.
    pub(crate) votes_refused_execution_behind_total: Counter,
    /// Rounds the committed round leads the last leader round the engine processed (mirrored
    /// from the committed-round and recent-blocks watch channels; zero while execution is at or
    /// ahead of it).
    ///
    /// Commits no longer wait for execution, so this is the execution backlog in rounds. It may
    /// reach `MAX_COMMIT_RUN_AHEAD_ROUNDS` before a commit is held.
    pub(crate) consensus_exec_lag_rounds: Gauge,
    /// Rounds the primary round leads the committed round (mirrored from the primary-round and
    /// committed-round watch channels; zero while the committed round is at or ahead of it).
    ///
    /// This is the lag the activity window demotes on, measured against the node's own round
    /// instead of an incoming certificate's.
    pub(crate) commit_lag_rounds: Gauge,
    // bench5 f3-rejoin
    /// Demotions for the activity window held because the node rejoined consensus less than
    /// `REJOIN_DEMOTION_GRACE` ago, counted once per certificate or consensus result that would
    /// have demoted it. Non-zero means a promoted node's DAG was not live yet.
    pub(crate) rejoin_grace_holds_total: Counter,
    // bench5 f1-commit-lag
    /// Times the execution check asked the canonical database again because it could not answer
    /// for a block at or below the executed tip (a read error), one per retry.
    ///
    /// Zero in a healthy node; a rising value means the execution database keeps failing reads
    /// and the waits behind it (votes, the subscriber, catch-up) are held.
    pub(crate) execution_check_retries_total: Counter,
}

impl PrimaryMetrics {
    /// Record the encoded byte size of a freshly saved consensus output.
    ///
    /// Updates the most-recent gauge and the since-startup max gauge.
    pub fn record_consensus_output_bytes(&self, bytes: u64) {
        self.consensus_output_bytes.set(bytes as f64);
        let prev = MAX_CONSENSUS_OUTPUT_BYTES.fetch_max(bytes, Ordering::Relaxed);
        self.max_consensus_output_bytes.set(prev.max(bytes) as f64);
    }

    /// Count a catching-up CVV's promotion back to an active CVV in
    /// `tn_node_mode_transitions_total{to="cvv_active", reason="rejoin"}`.
    ///
    /// `previous` is the mode the watch held before the promotion, the value `send_replace`
    /// returns. The promotion happens in the executor's subscriber, outside this crate, so this
    /// method is its route to [`record_mode_transition`].
    pub fn record_rejoin_transition(&self, previous: NodeMode) {
        record_mode_transition(previous, NodeMode::CvvActive, ModeTransitionReason::Rejoin);
    }

    /// Count an active CVV's demotion after a failed fork check in
    /// `tn_node_mode_transitions_total{to="cvv_inactive", reason="bogus_subdag"}`.
    ///
    /// `previous` is the mode the watch held before the demotion, the value `send_replace`
    /// returns. The check runs in the executor's subscriber before it assembles a committed
    /// sub-dag's output, outside this crate, so this method is its route to
    /// [`record_mode_transition`]. The label is the one the check carried when it ran in the
    /// consensus task.
    pub fn record_execution_fork_transition(&self, previous: NodeMode) {
        record_mode_transition(previous, NodeMode::CvvInactive, ModeTransitionReason::BogusSubdag);
    }
}

/// A stage of the voter's vote path, in the order `RequestHandler::vote_inner` runs them.
///
/// [`Self::as_str`] is the `stage` label of `tn_primary_vote_stage_seconds` and
/// `tn_primary_vote_timeouts_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoteStage {
    /// Header checks, the drift tier decision and the seed signature verify.
    Validate,
    /// The wait for local execution to reach the header's latest execution block.
    ExecWait,
    /// The missing-parent check, parent acceptance, the wait for the parents and their checks.
    Parents,
    /// The wait for a header lead within the drift tolerance to pass.
    LeadSleep,
    /// Synchronizing the header's batches from the workers.
    SyncBatches,
    /// Reading the previous vote, signing the vote and writing it to the `Votes` table.
    VoteStore,
    /// The durability barrier on the `Votes` table before a vote is returned.
    Persist,
}

impl VoteStage {
    /// The number of stages.
    pub(crate) const COUNT: usize = 7;

    /// Every stage, in vote path order. A stage's position here is its [`Self::index`].
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::Validate,
        Self::ExecWait,
        Self::Parents,
        Self::LeadSleep,
        Self::SyncBatches,
        Self::VoteStore,
        Self::Persist,
    ];

    /// The static metric label for this stage.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Validate => "validate",
            Self::ExecWait => "exec_wait",
            Self::Parents => "parents",
            Self::LeadSleep => "lead_sleep",
            Self::SyncBatches => "sync_batches",
            Self::VoteStore => "vote_store",
            Self::Persist => "persist",
        }
    }

    /// The position of this stage in [`Self::ALL`], for per-stage arrays.
    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

// compile-time check that `ALL` lists the stages in declaration order, so every `index` in it is
// a valid position in the per-stage arrays. a stage inserted without updating `COUNT` and `ALL`
// fails the build here. stable rust cannot count variants, so a stage appended after `Persist`
// still needs `COUNT` and `ALL` updated by hand.
const _: () = {
    let mut position = 0;
    while position < VoteStage::COUNT {
        assert!(VoteStage::ALL[position].index() == position, "VoteStage::ALL out of order");
        position += 1;
    }
};

/// Vote path metric handles, resolved once when an epoch's request handler is built.
///
/// Every committee peer sends vote requests several times a second, so the handles are resolved
/// up front and no `metrics` macro runs on the vote path. The handles are `Arc`-backed.
#[derive(Clone, Debug)]
pub(crate) struct VoteStageHandles {
    /// Vote requests received (`tn_primary_vote_requests_total`).
    requests: Counter,
    /// New votes signed, made durable and returned (`tn_primary_votes_created_total`).
    created: Counter,
    /// Votes signed earlier and returned again (`tn_primary_votes_recast_total`).
    recast: Counter,
    /// Requests refused because the header round is beyond the committed round plus the gc
    /// depth (`tn_primary_vote_requests_too_new_total`).
    too_new: Counter,
    /// The wait for the author's vote lock (`tn_primary_vote_stage_seconds{stage="lock"}`).
    ///
    /// The lock is taken before the vote timeout starts, so it has no [`VoteStage`].
    lock_wait: Histogram,
    /// Time per stage, indexed by [`VoteStage::index`] (`tn_primary_vote_stage_seconds{stage}`).
    stage_seconds: [Histogram; VoteStage::COUNT],
    /// Vote timeouts by the stage in progress when the timeout fired, indexed by
    /// [`VoteStage::index`] (`tn_primary_vote_timeouts_total{stage}`).
    timeouts: [Counter; VoteStage::COUNT],
}

impl VoteStageHandles {
    /// Resolve every vote path handle against the installed recorder.
    pub(crate) fn new() -> Self {
        Self {
            requests: metrics::counter!("tn_primary.vote_requests_total"),
            created: metrics::counter!("tn_primary.votes_created_total"),
            recast: metrics::counter!("tn_primary.votes_recast_total"),
            too_new: metrics::counter!("tn_primary.vote_requests_too_new_total"),
            lock_wait: metrics::histogram!("tn_primary.vote_stage_seconds", "stage" => "lock"),
            stage_seconds: VoteStage::ALL.map(|stage| {
                metrics::histogram!("tn_primary.vote_stage_seconds", "stage" => stage.as_str())
            }),
            timeouts: VoteStage::ALL.map(|stage| {
                metrics::counter!("tn_primary.vote_timeouts_total", "stage" => stage.as_str())
            }),
        }
    }

    /// Count a vote request.
    pub(crate) fn record_request(&self) {
        self.requests.increment(1);
    }

    /// Count a new vote returned to its author.
    pub(crate) fn record_created(&self) {
        self.created.increment(1);
    }

    /// Count a vote signed earlier and returned again.
    pub(crate) fn record_recast(&self) {
        self.recast.increment(1);
    }

    /// Count a request refused because its header round is too far ahead.
    pub(crate) fn record_too_new(&self) {
        self.too_new.increment(1);
    }

    /// Record the wait for the author's vote lock.
    pub(crate) fn record_lock_wait(&self, waited: Duration) {
        self.lock_wait.record(waited.as_secs_f64());
    }

    /// Record the time a request spent in `stage`.
    pub(crate) fn record_stage(&self, stage: VoteStage, spent: Duration) {
        self.stage_seconds[stage.index()].record(spent.as_secs_f64());
    }

    /// Count a vote timeout that fired while `stage` was in progress.
    pub(crate) fn record_timeout(&self, stage: VoteStage) {
        self.timeouts[stage.index()].increment(1);
    }
}

/// Why this node changed its mode: the `reason` label of `tn_node_mode_transitions_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModeTransitionReason {
    /// An active CVV saw a certificate or consensus result, in the epoch it has executed, more
    /// than the gc activity window of rounds ahead of the round it has executed.
    OutsideGcWindow,
    /// An active CVV saw a certificate or consensus result from an epoch ahead of the one it has
    /// executed.
    EpochBehind,
    /// A committed sub-dag's leader references an execution block this node did not execute at
    /// that height (the subscriber's fork check before it assembles the output).
    BogusSubdag,
    /// The committed round disagreed with the highest header round just sequenced.
    CommitAccounting,
    /// A catching-up CVV came within the rejoin window of the network head.
    Rejoin,
    // bench5 f1-commit-lag
    /// An active CVV restarted its epoch with a restored DAG whose lowest round lies above the
    /// round after its committed round: the certificates Bullshark needs next were pruned.
    RestartGap,
}

impl ModeTransitionReason {
    /// The static metric label for this reason.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OutsideGcWindow => "outside_gc_window",
            Self::EpochBehind => "epoch_behind",
            Self::BogusSubdag => "bogus_subdag",
            Self::CommitAccounting => "commit_accounting",
            Self::Rejoin => "rejoin",
            Self::RestartGap => "restart_gap",
        }
    }
}

/// Count a node mode change in `tn_node_mode_transitions_total{to, reason}`.
///
/// `previous` is the mode the watch held before the change, the value `send_replace` returns. A
/// change to the mode the node already holds is not counted, so two tasks that decide on the same
/// demotion at once count it once. Mode changes are rare, so the handle is resolved per call.
pub(crate) fn record_mode_transition(
    previous: NodeMode,
    to: NodeMode,
    reason: ModeTransitionReason,
) {
    // `NodeMode` has no `PartialEq`; its variants carry no data, so the discriminant is the mode
    if std::mem::discriminant(&previous) == std::mem::discriminant(&to) {
        return;
    }
    metrics::counter!(
        "tn_node.mode_transitions_total",
        "to" => node_mode_label(to),
        "reason" => reason.as_str(),
    )
    .increment(1);
}

/// Spawn a node-lifetime task that mirrors consensus watch channels into gauges.
///
/// The watch senders are mutated from several crates via `send_replace`/`send_modify`;
/// mirroring the watch (the existing source of truth) is rot-proof compared to
/// instrumenting every mutation site, and gauges only need scrape-interval freshness.
///
/// Spawn once per process (from the epoch manager, before the epoch loop) on the
/// node-lifetime spawner.
pub fn spawn_bus_metrics_mirror(
    bus: &ConsensusBusApp,
    task_spawner: &TaskSpawner,
    shutdown: Noticer,
) {
    let metrics = bus.metrics().clone();
    let mut rx_round = bus.primary_round_updates().subscribe();
    let mut rx_committed = bus.committed_round_updates().subscribe();
    let mut rx_mode = bus.node_mode().subscribe();
    let mut rx_recent = bus.recent_blocks().subscribe();
    let mut rx_published = bus.last_published_consensus_num_hash().subscribe();

    // prime gauges with current values
    metrics.round.set(*rx_round.borrow_and_update() as f64);
    metrics.committed_round.set(*rx_committed.borrow_and_update() as f64);
    set_node_mode_gauges(&rx_mode.borrow_and_update());
    let mut executed_height =
        rx_recent.borrow_and_update().latest_consensus_block_num_hash().number;
    set_lag_gauges(&metrics, &rx_round, &rx_committed, &rx_recent);
    let mut gossip_height = rx_published.borrow_and_update().1;
    set_consensus_height_gauges(executed_height, gossip_height);

    task_spawner.spawn_task("bus-metrics-mirror", async move {
        loop {
            tokio::select! {
                _ = &shutdown => break,
                res = rx_round.changed() => {
                    if res.is_err() {
                        break;
                    }
                    metrics.round.set(*rx_round.borrow_and_update() as f64);
                    set_lag_gauges(&metrics, &rx_round, &rx_committed, &rx_recent);
                }
                res = rx_committed.changed() => {
                    if res.is_err() {
                        break;
                    }
                    metrics.committed_round.set(*rx_committed.borrow_and_update() as f64);
                    set_lag_gauges(&metrics, &rx_round, &rx_committed, &rx_recent);
                }
                res = rx_mode.changed() => {
                    if res.is_err() {
                        break;
                    }
                    let mode = *rx_mode.borrow_and_update();
                    set_node_mode_gauges(&mode);
                }
                res = rx_recent.changed() => {
                    if res.is_err() {
                        break;
                    }
                    executed_height =
                        rx_recent.borrow_and_update().latest_consensus_block_num_hash().number;
                    set_consensus_height_gauges(executed_height, gossip_height);
                    set_lag_gauges(&metrics, &rx_round, &rx_committed, &rx_recent);
                }
                res = rx_published.changed() => {
                    if res.is_err() {
                        break;
                    }
                    gossip_height = rx_published.borrow_and_update().1;
                    set_consensus_height_gauges(executed_height, gossip_height);
                }
            }
        }
        debug!(target: "primary::metrics", "bus metrics mirror shutting down");
        Ok(())
    });
}

/// Round-lag gauges: how far the committed round leads execution, and how far the primary round
/// leads the committed round. Both saturate at zero.
///
/// Every watch is read at its current value, not one cached when it last changed. When
/// `reset_for_epoch` zeroes the committed round and then the primary round back to back, the
/// iteration woken by the first reset normally reads the second too, and the second's own change
/// recomputes both, so a new committed round is not paired with the previous epoch's primary round
/// until some unrelated change.
fn set_lag_gauges(
    metrics: &PrimaryMetrics,
    rx_round: &watch::Receiver<Round>,
    rx_committed: &watch::Receiver<Round>,
    rx_recent: &watch::Receiver<RecentBlocks>,
) {
    let primary = *rx_round.borrow();
    let committed = *rx_committed.borrow();
    let executed = rx_recent.borrow().last_consensus_round();
    metrics.consensus_exec_lag_rounds.set(committed.saturating_sub(executed) as f64);
    metrics.commit_lag_rounds.set(primary.saturating_sub(committed) as f64);
}

/// Consensus-height gauges and the falling-behind alarm.
///
/// `sync_distance` is how far the latest verified gossip height is ahead of local
/// execution - a persistently growing value means this node is falling behind.
fn set_consensus_height_gauges(executed: u64, gossip: u64) {
    metrics::gauge!("tn_node.last_executed_consensus_height").set(executed as f64);
    metrics::gauge!("tn_node.latest_gossip_consensus_height").set(gossip as f64);
    metrics::gauge!("tn_node.consensus_sync_distance").set(gossip.saturating_sub(executed) as f64);
}

/// One-hot `tn_node_mode{mode=...}` gauges: exactly one of the three series is 1.
///
/// Grafana renders this as a state timeline; one-hot avoids encoding the enum as
/// magic numbers.
fn set_node_mode_gauges(mode: &NodeMode) {
    let states = [
        ("cvv_active", mode.is_active_cvv()),
        ("cvv_inactive", mode.is_cvv_inactive()),
        ("observer", mode.is_observer()),
    ];
    for (label, active) in states {
        metrics::gauge!("tn_node.mode", "mode" => label).set(if active { 1.0 } else { 0.0 });
    }
}

/// The `to` label of `tn_node_mode_transitions_total`, spelled as the `tn_node_mode` gauge's
/// `mode` label.
const fn node_mode_label(mode: NodeMode) -> &'static str {
    match mode {
        NodeMode::CvvActive => "cvv_active",
        NodeMode::CvvInactive => "cvv_inactive",
        NodeMode::Observer => "observer",
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
            // new_with_labels: Default::default() caches the first-bound recorder process-wide
            let metrics = PrimaryMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.headers_proposed_total.increment(1);
            metrics.votes_received_total.increment(3);
            metrics.invalid_votes_total.increment(1);
            metrics.votes_deferred_future_header_total.increment(2);
            metrics.certificates_formed_total.increment(1);
            metrics.certificate_form_duration_seconds.record(0.05);
            metrics.certificates_received_total.increment(2);
            metrics.certificates_pending.set(4.0);
            metrics.round.set(17.0);
            metrics.committed_round.set(15.0);
            metrics.subdags_committed_total.increment(1);
            metrics.commit_latency_seconds.record(1.0);
            metrics.record_consensus_output_bytes(123);
            metrics.commit_run_ahead_blocked_total.increment(1);
            metrics.votes_refused_inactive_total.increment(1);
            metrics.votes_refused_execution_behind_total.increment(2);
            metrics.rejoin_grace_holds_total.increment(1);
            metrics.execution_check_retries_total.increment(4);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_primary.headers_proposed_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_primary.votes_received_total");
        assert!(matches!(value, DebugValue::Counter(3)));
        let (_, _, _, value) = find("tn_primary.votes_deferred_future_header_total");
        assert!(matches!(value, DebugValue::Counter(2)));
        let (_, _, _, value) = find("tn_primary.certificates_formed_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_primary.round");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 17.0));
        let (_, _, _, value) = find("tn_primary.committed_round");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 15.0));
        find("tn_primary.certificate_form_duration_seconds");
        find("tn_primary.certificates_pending");
        find("tn_primary.subdags_committed_total");
        find("tn_primary.commit_latency_seconds");
        let (_, _, _, value) = find("tn_primary.consensus_output_bytes");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 123.0));
        let (_, _, _, value) = find("tn_primary.max_consensus_output_bytes");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 123.0));
        let (_, _, _, value) = find("tn_primary.commit_run_ahead_blocked_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_primary.votes_refused_inactive_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_primary.votes_refused_execution_behind_total");
        assert!(matches!(value, DebugValue::Counter(2)));
        let (_, _, _, value) = find("tn_primary.rejoin_grace_holds_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_primary.execution_check_retries_total");
        assert!(matches!(value, DebugValue::Counter(4)));
    }

    /// The vote path handles register every series and label the bench analysis reads: the four
    /// counters, a timeout counter and a stage histogram per stage, and the `lock` histogram.
    #[test]
    fn test_vote_stage_handles_register_labelled_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let handles = VoteStageHandles::new();
            handles.record_request();
            handles.record_request();
            handles.record_created();
            handles.record_recast();
            handles.record_too_new();
            handles.record_lock_wait(Duration::from_millis(4));
            handles.record_stage(VoteStage::SyncBatches, Duration::from_millis(250));
            handles.record_timeout(VoteStage::ExecWait);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str, stage: Option<&str>| {
            snapshot
                .iter()
                .find(|(key, ..)| {
                    let key = key.key();
                    key.name() == name
                        && key.labels().find(|label| label.key() == "stage").map(|l| l.value())
                            == stage
                })
                .map(|(.., value)| value)
                .unwrap_or_else(|| panic!("metric {name} stage={stage:?} not registered"))
        };

        assert_eq!(find("tn_primary.vote_requests_total", None), &DebugValue::Counter(2));
        assert_eq!(find("tn_primary.votes_created_total", None), &DebugValue::Counter(1));
        assert_eq!(find("tn_primary.votes_recast_total", None), &DebugValue::Counter(1));
        assert_eq!(find("tn_primary.vote_requests_too_new_total", None), &DebugValue::Counter(1));
        let labels = [
            "validate",
            "exec_wait",
            "parents",
            "lead_sleep",
            "sync_batches",
            "vote_store",
            "persist",
        ];
        assert_eq!(VoteStage::ALL.map(VoteStage::as_str), labels);
        for (index, stage) in VoteStage::ALL.into_iter().enumerate() {
            assert_eq!(stage.index(), index);
            let timeouts = find("tn_primary.vote_timeouts_total", Some(stage.as_str()));
            let fired = u64::from(stage == VoteStage::ExecWait);
            assert_eq!(timeouts, &DebugValue::Counter(fired), "{} timeouts", stage.as_str());
            let DebugValue::Histogram(samples) =
                find("tn_primary.vote_stage_seconds", Some(stage.as_str()))
            else {
                panic!("{} stage series is not a histogram", stage.as_str());
            };
            let expected = if stage == VoteStage::SyncBatches { vec![0.25] } else { Vec::new() };
            assert_eq!(samples.iter().map(|s| s.0).collect::<Vec<_>>(), expected);
        }
        let DebugValue::Histogram(lock) = find("tn_primary.vote_stage_seconds", Some("lock"))
        else {
            panic!("lock stage series is not a histogram");
        };
        assert_eq!(lock.iter().map(|s| s.0).collect::<Vec<_>>(), vec![0.004]);
    }

    /// Every mode change counts once under its `to` and `reason` labels, and a change to the mode
    /// the node already holds is not counted.
    #[test]
    fn test_mode_transitions_register_labelled_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let metrics = PrimaryMetrics::new_with_labels(Vec::<metrics::Label>::new());
            let demote =
                |reason| record_mode_transition(NodeMode::CvvActive, NodeMode::CvvInactive, reason);
            demote(ModeTransitionReason::OutsideGcWindow);
            demote(ModeTransitionReason::OutsideGcWindow);
            demote(ModeTransitionReason::EpochBehind);
            demote(ModeTransitionReason::BogusSubdag);
            demote(ModeTransitionReason::CommitAccounting);
            // a second demotion racing the first finds the node inactive already
            record_mode_transition(
                NodeMode::CvvInactive,
                NodeMode::CvvInactive,
                ModeTransitionReason::CommitAccounting,
            );
            metrics.record_rejoin_transition(NodeMode::CvvInactive);
            metrics.record_rejoin_transition(NodeMode::CvvActive);
            demote(ModeTransitionReason::RestartGap);
            metrics.record_execution_fork_transition(NodeMode::CvvActive);
            metrics.record_execution_fork_transition(NodeMode::CvvInactive);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let mut series: Vec<(String, String, DebugValue)> = snapshot
            .into_iter()
            .filter(|(key, ..)| key.key().name() == "tn_node.mode_transitions_total")
            .map(|(key, _, _, value)| {
                let label = |name: &str| {
                    key.key()
                        .labels()
                        .find(|label| label.key() == name)
                        .map(|label| label.value().to_string())
                        .unwrap_or_else(|| panic!("missing {name} label"))
                };
                assert_eq!(key.key().labels().count(), 2, "only the to and reason labels");
                (label("to"), label("reason"), value)
            })
            .collect();
        series.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let expected = [
            ("cvv_active", "rejoin", 1),
            ("cvv_inactive", "bogus_subdag", 2),
            ("cvv_inactive", "commit_accounting", 1),
            ("cvv_inactive", "epoch_behind", 1),
            ("cvv_inactive", "outside_gc_window", 2),
            ("cvv_inactive", "restart_gap", 1),
        ]
        .map(|(to, reason, count)| {
            (to.to_string(), reason.to_string(), DebugValue::Counter(count))
        });
        assert_eq!(series, expected);

        assert_eq!(node_mode_label(NodeMode::Observer), "observer");
    }

    /// The mirror task must keep the round gauges in sync with the watch channels, and
    /// its synchronous prime must register the consensus-height gauge trio by name.
    #[tokio::test]
    async fn test_bus_metrics_mirror() {
        use tn_types::{Notifier, TaskManager};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // bind the bus metrics to the local recorder during construction
        let bus = metrics::with_local_recorder(&recorder, ConsensusBusApp::new);

        let task_manager = TaskManager::default();
        let shutdown = Notifier::new();
        // the free-form `gauge!` calls in the prime bind at call time, so the spawn must
        // also run under the local recorder for the trio to be observable here
        metrics::with_local_recorder(&recorder, || {
            spawn_bus_metrics_mirror(&bus, &task_manager.get_spawner(), shutdown.subscribe())
        });

        // a typo or deletion in `set_consensus_height_gauges` compiles clean; only this
        // name pin (and the e2e scrape) catches it
        let primed = snapshotter.snapshot().into_vec();
        [
            "tn_node.last_executed_consensus_height",
            "tn_node.latest_gossip_consensus_height",
            "tn_node.consensus_sync_distance",
        ]
        .iter()
        .for_each(|name| {
            assert!(
                primed.iter().any(|(key, ..)| key.key().name() == *name),
                "prime must register {name}"
            );
        });

        bus.primary_round_updates().send_replace(42);
        bus.committed_round_updates().send_replace(40);

        // wait for the mirror to observe the change
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let snapshot = snapshotter.snapshot().into_vec();
            let round = snapshot.iter().find_map(|(key, _, _, value)| {
                (key.key().name() == "tn_primary.round").then_some(value)
            });
            if matches!(round, Some(DebugValue::Gauge(g)) if g.0 == 42.0) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "mirror never updated round gauge");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // the engine processed the leader of round 35: execution trails the committed round 40
        // by five rounds, and the committed round trails the primary round 42 by two
        bus.recent_blocks().send_modify(|recent| {
            recent.push_latest(
                35,
                tn_types::ConsensusNumHash::new(7, tn_types::ConsensusHeaderDigest::default()),
                None,
            )
        });
        // a snapshot swaps every value it reads back to zero, so both lags come from one snapshot;
        // the mirror sets them together on every change
        let lags = || {
            let snapshot = snapshotter.snapshot().into_vec();
            let gauge = |name: &str| {
                snapshot.iter().find_map(|(key, _, _, value)| match value {
                    DebugValue::Gauge(g) if key.key().name() == name => Some(g.0),
                    _ => None,
                })
            };
            (gauge("tn_primary.consensus_exec_lag_rounds"), gauge("tn_primary.commit_lag_rounds"))
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let (exec_lag, commit_lag) = lags();
            if exec_lag == Some(5.0) {
                assert_eq!(commit_lag, Some(2.0), "commit lag set with the exec lag");
                break;
            }
            assert!(std::time::Instant::now() < deadline, "mirror never updated the exec lag");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        shutdown.notify();
    }
}
