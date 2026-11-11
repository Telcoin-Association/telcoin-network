//! How many consensus outputs execution trails behind, shared between the node, the engine and the
//! RPC submission guard.
//!
//! Under sustained load a validator's execution can fall behind the outputs consensus commits, and
//! every transaction its RPC accepts meanwhile waits behind that backlog. [`ExecutionBacklog`] is a
//! lock-free handle the three parties share: the node records the newest output it forwarded to
//! the engine, the engine records how many outputs it holds and the newest one it finished, and
//! the submission guard asks how many outputs execution is behind. [`AdmissionGate`] turns that
//! number into an admit-or-refuse answer with hysteresis, so the guard does not flip on every
//! output.
//!
//! Every counter is read and written with relaxed ordering, so a reader can see one writer's
//! update before another's. Nothing here feeds consensus or execution: a stale read admits or
//! refuses one more submission and never changes a block.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

/// A cheap, cloneable handle on how many consensus outputs execution trails behind.
///
/// Every clone shares the same counters. The methods take no lock and never panic.
#[derive(Clone, Debug, Default)]
pub struct ExecutionBacklog {
    /// The counters every clone shares.
    inner: Arc<BacklogCounters>,
}

impl ExecutionBacklog {
    /// Create a handle with nothing recorded, so [`Self::outputs_behind`] reads zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the node forwarded the consensus output numbered `number` to the engine.
    ///
    /// The newest number wins: a number below the one already held is ignored, so a replayed or
    /// reordered write cannot shrink the backlog.
    pub fn record_forwarded(&self, number: u64) {
        self.inner.forwarded.fetch_max(encode(number), Ordering::Relaxed);
    }

    /// Record that the engine finished the consensus output numbered `number`, whether it built
    /// blocks for it or skipped it as empty.
    ///
    /// The newest number wins, as in [`Self::record_forwarded`]. The engine records every output
    /// it finishes, including outputs without batches, otherwise the backlog reads high until the
    /// next one. It may also record its last executed output at start.
    pub fn record_executed(&self, number: u64) {
        self.inner.executed.fetch_max(encode(number), Ordering::Relaxed);
    }

    /// Record how many outputs the engine holds: the queued ones plus the one executing.
    ///
    /// This replaces the previous value, since the queue shrinks as well as grows.
    pub fn record_queued(&self, outputs: usize) {
        let outputs = u64::try_from(outputs).unwrap_or(u64::MAX);
        self.inner.queued.store(outputs, Ordering::Relaxed);
    }

    /// The newest output number passed to [`Self::record_forwarded`], or `None` before the first.
    pub fn last_forwarded(&self) -> Option<u64> {
        decode(self.inner.forwarded.load(Ordering::Relaxed))
    }

    /// The newest output number passed to [`Self::record_executed`], or `None` before the first.
    pub fn last_executed(&self) -> Option<u64> {
        decode(self.inner.executed.load(Ordering::Relaxed))
    }

    /// The last count passed to [`Self::record_queued`], zero before the first.
    pub fn queued(&self) -> u64 {
        self.inner.queued.load(Ordering::Relaxed)
    }

    /// How many consensus outputs execution is behind.
    ///
    /// The larger of two estimates. The last forwarded number minus the last executed one also
    /// counts outputs still in the channel to the engine, but it is used only once both numbers
    /// are recorded: a node that has forwarded outputs since start but not yet finished one would
    /// otherwise read its whole history as backlog. The engine's queued count covers that gap.
    /// Zero while nothing is recorded, and zero when execution has finished more than was
    /// recorded as forwarded.
    pub fn outputs_behind(&self) -> u64 {
        let queued = self.queued();
        match (self.last_forwarded(), self.last_executed()) {
            (Some(forwarded), Some(executed)) => forwarded.saturating_sub(executed).max(queued),
            _ => queued,
        }
    }
}

/// An admit-or-refuse answer for new RPC submissions, bounded by an [`ExecutionBacklog`].
///
/// The gate closes once execution is `limit` or more outputs behind and reopens only once the
/// backlog is below `limit - 1` (below one when `limit` is one), so a backlog hovering at the limit
/// does not flip the gate on every output. A `limit` of zero turns the gate off: it admits
/// everything. [`admission_open`] is the rule as a pure function.
///
/// Clones share the open or closed state and the backlog.
#[derive(Clone, Debug)]
pub struct AdmissionGate {
    /// The backlog the gate reads.
    backlog: ExecutionBacklog,
    /// The backlog, in outputs, at which an open gate closes. Zero turns the gate off.
    limit: u64,
    /// Whether the gate admitted at its last check. Starts open.
    open: Arc<AtomicBool>,
}

impl AdmissionGate {
    /// Create an open gate over `backlog` that closes at `limit` outputs behind. A `limit` of
    /// zero turns the gate off.
    pub fn new(backlog: ExecutionBacklog, limit: usize) -> Self {
        let limit = u64::try_from(limit).unwrap_or(u64::MAX);
        Self { backlog, limit, open: Arc::new(AtomicBool::new(true)) }
    }

    /// Whether the gate can ever refuse, that is whether its limit is above zero.
    pub fn is_enabled(&self) -> bool {
        self.limit > 0
    }

    /// The backlog, in outputs, at which an open gate closes.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// The backlog this gate reads.
    pub fn backlog(&self) -> &ExecutionBacklog {
        &self.backlog
    }

    /// Whether a new submission is admitted now.
    ///
    /// Reads the backlog, applies [`admission_open`] to the gate's state from its last check and
    /// keeps the result. Two callers that check at once can each act on the state before the
    /// other's update. That costs at most one extra admission or refusal at a crossing.
    pub fn admits(&self) -> bool {
        if !self.is_enabled() {
            return true;
        }
        let was_open = self.open.load(Ordering::Relaxed);
        let open = admission_open(was_open, self.backlog.outputs_behind(), self.limit);
        if open != was_open {
            self.open.store(open, Ordering::Relaxed);
        }
        open
    }
}

/// The hysteresis rule of [`AdmissionGate`]: whether the gate is open after it reads
/// `outputs_behind`, given whether it was open before.
///
/// A `limit` of zero means no bound, so the gate is always open. An open gate closes at
/// `outputs_behind >= limit`. A closed gate reopens at `outputs_behind < limit - 1`; with a
/// `limit` of one that bound would be zero, which no backlog is below, so the gate reopens at a
/// backlog of zero instead.
pub fn admission_open(was_open: bool, outputs_behind: u64, limit: u64) -> bool {
    if limit == 0 {
        return true;
    }
    let below = if was_open { limit } else { limit.saturating_sub(1).max(1) };
    outputs_behind < below
}

/// The counters behind an [`ExecutionBacklog`].
#[derive(Debug, Default)]
struct BacklogCounters {
    /// The last forwarded output number, stored by [`encode`].
    forwarded: AtomicU64,
    /// The last finished output number, stored by [`encode`].
    executed: AtomicU64,
    /// The outputs the engine holds, queued or executing.
    queued: AtomicU64,
}

/// Store an output number one above its value, so zero can mean "nothing recorded" and
/// `fetch_max` still keeps the newest number.
///
/// `u64::MAX` saturates and reads back as `u64::MAX - 1`; consensus numbers never come near it.
fn encode(number: u64) -> u64 {
    number.saturating_add(1)
}

/// Read a number stored by [`encode`]: `None` when nothing is recorded.
fn decode(raw: u64) -> Option<u64> {
    raw.checked_sub(1)
}

#[cfg(test)]
mod tests {
    use super::{admission_open, AdmissionGate, ExecutionBacklog};

    #[test]
    fn nothing_recorded_reads_zero() {
        let backlog = ExecutionBacklog::new();
        assert_eq!(backlog.last_forwarded(), None);
        assert_eq!(backlog.last_executed(), None);
        assert_eq!(backlog.queued(), 0);
        assert_eq!(backlog.outputs_behind(), 0);
    }

    /// Output number zero is a recorded number, not "nothing recorded".
    #[test]
    fn output_zero_is_recorded() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(0);
        backlog.record_executed(0);
        assert_eq!(backlog.last_forwarded(), Some(0));
        assert_eq!(backlog.last_executed(), Some(0));
        assert_eq!(backlog.outputs_behind(), 0);
    }

    #[test]
    fn behind_is_forwarded_minus_executed() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(10);
        backlog.record_executed(7);
        assert_eq!(backlog.outputs_behind(), 3);
        backlog.record_executed(10);
        assert_eq!(backlog.outputs_behind(), 0);
    }

    /// Before the engine finishes its first output only the queued count is trusted, so a node
    /// that starts at output 5,000 does not read 5,000 outputs behind.
    #[test]
    fn behind_uses_the_queued_count_until_both_numbers_are_recorded() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(5_000);
        assert_eq!(backlog.outputs_behind(), 0);
        backlog.record_queued(2);
        assert_eq!(backlog.outputs_behind(), 2);
        backlog.record_executed(4_999);
        assert_eq!(backlog.outputs_behind(), 2);
        backlog.record_queued(0);
        assert_eq!(backlog.outputs_behind(), 1);
    }

    #[test]
    fn behind_is_the_larger_estimate() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(10);
        backlog.record_executed(8);
        backlog.record_queued(5);
        assert_eq!(backlog.outputs_behind(), 5);
        backlog.record_queued(1);
        assert_eq!(backlog.outputs_behind(), 2);
    }

    /// Execution ahead of the forwarded number (a replay the node did not record) saturates at
    /// zero rather than wrapping.
    #[test]
    fn executed_ahead_of_forwarded_reads_zero() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(3);
        backlog.record_executed(9);
        assert_eq!(backlog.outputs_behind(), 0);
    }

    #[test]
    fn numbers_only_move_forward_and_the_queue_moves_both_ways() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(10);
        backlog.record_forwarded(4);
        backlog.record_executed(6);
        backlog.record_executed(2);
        assert_eq!(backlog.last_forwarded(), Some(10));
        assert_eq!(backlog.last_executed(), Some(6));
        backlog.record_queued(7);
        backlog.record_queued(1);
        assert_eq!(backlog.queued(), 1);
    }

    #[test]
    fn extremes_saturate_without_panicking() {
        let backlog = ExecutionBacklog::new();
        backlog.record_forwarded(u64::MAX);
        backlog.record_executed(0);
        assert_eq!(backlog.last_forwarded(), Some(u64::MAX - 1));
        assert_eq!(backlog.outputs_behind(), u64::MAX - 1);
        backlog.record_queued(usize::MAX);
        let queued = u64::try_from(usize::MAX).unwrap_or(u64::MAX);
        assert_eq!(backlog.queued(), queued);
        assert_eq!(backlog.outputs_behind(), queued.max(u64::MAX - 1));
        backlog.record_executed(u64::MAX);
        assert_eq!(backlog.last_executed(), Some(u64::MAX - 1));
    }

    #[test]
    fn clones_share_the_counters() {
        let backlog = ExecutionBacklog::new();
        let engine = backlog.clone();
        let guard = backlog.clone();
        backlog.record_forwarded(12);
        engine.record_executed(9);
        assert_eq!(guard.outputs_behind(), 3);
    }

    /// Writers on several threads at once leave the newest numbers, whatever the interleaving.
    #[test]
    fn concurrent_writers_keep_the_newest_numbers() {
        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 5_000;
        let backlog = ExecutionBacklog::new();
        std::thread::scope(|scope| {
            for thread in 0..THREADS {
                let backlog = backlog.clone();
                scope.spawn(move || {
                    for i in 0..PER_THREAD {
                        let number = i * THREADS + thread;
                        backlog.record_forwarded(number);
                        backlog.record_executed(number / 2);
                        backlog.record_queued(usize::try_from(i % 9).unwrap_or(0));
                        // readers race the writers and must never panic
                        let _ = backlog.outputs_behind();
                    }
                });
            }
        });
        let newest = PER_THREAD * THREADS - 1;
        assert_eq!(backlog.last_forwarded(), Some(newest));
        assert_eq!(backlog.last_executed(), Some(newest / 2));
        assert!(backlog.queued() < 9);
        assert_eq!(backlog.outputs_behind(), newest - newest / 2);
    }

    /// The hysteresis table: (was open, outputs behind, limit, open after).
    #[test]
    fn hysteresis_table() {
        let max = u64::MAX;
        let rows = [
            // limit 0: no bound
            (true, 0, 0, true),
            (true, max, 0, true),
            (false, max, 0, true),
            // limit 1: close at 1, reopen at 0
            (true, 0, 1, true),
            (true, 1, 1, false),
            (false, 1, 1, false),
            (false, 0, 1, true),
            // limit 2 (the bench-6 arm): close at 2, reopen below 1
            (true, 0, 2, true),
            (true, 1, 2, true),
            (true, 2, 2, false),
            (true, 7, 2, false),
            (false, 2, 2, false),
            (false, 1, 2, false),
            (false, 0, 2, true),
            // limit 3: close at 3, reopen below 2
            (true, 2, 3, true),
            (true, 3, 3, false),
            (false, 2, 3, false),
            (false, 1, 3, true),
            // the largest limit
            (true, max - 1, max, true),
            (true, max, max, false),
            (false, max - 1, max, false),
            (false, max - 2, max, true),
        ];
        for (was_open, behind, limit, open) in rows {
            assert_eq!(
                admission_open(was_open, behind, limit),
                open,
                "was_open={was_open} behind={behind} limit={limit}"
            );
        }
    }

    /// A gate with limit 2 closes at two outputs behind, stays closed at one, and reopens at zero.
    #[test]
    fn gate_follows_the_backlog_with_hysteresis() {
        let backlog = ExecutionBacklog::new();
        let gate = AdmissionGate::new(backlog.clone(), 2);
        assert!(gate.is_enabled());
        assert_eq!(gate.limit(), 2);
        assert!(gate.admits(), "nothing recorded");
        backlog.record_forwarded(11);
        backlog.record_executed(10);
        assert!(gate.admits(), "one behind, open");
        backlog.record_forwarded(12);
        assert!(!gate.admits(), "two behind, closes");
        let other = gate.clone();
        backlog.record_executed(11);
        assert!(!other.admits(), "one behind, clones see the gate closed");
        backlog.record_executed(12);
        assert!(gate.admits(), "zero behind, reopens");
        backlog.record_forwarded(13);
        assert!(other.admits(), "one behind, open again");
        assert_eq!(gate.backlog().outputs_behind(), 1);
    }

    #[test]
    fn a_zero_limit_admits_everything() {
        let backlog = ExecutionBacklog::new();
        let gate = AdmissionGate::new(backlog.clone(), 0);
        assert!(!gate.is_enabled());
        backlog.record_forwarded(1_000);
        backlog.record_executed(0);
        backlog.record_queued(usize::MAX);
        assert!(gate.admits());
    }
}
