//! Durability: the commit thread and the barriers that wait for it.
//!
//! A sync takes no lock a writer takes. Each batch ends at a commit point (its flagged last
//! record), and each write stores the log's frontier (generation, end, batch number) as one
//! value when its batch is whole. A sync round ([`sync_round`]) loads the frontier, `msync`s the
//! log up to its end through the generation's view (and `fsync`s a duplicate descriptor if the
//! log grew), and then every batch numbered up to the frontier's is durable. One round runs at a
//! time (the core's sync lock), however many writers are waiting for it.
//!
//! Who syncs: in `CommitMode::Sync` the first writer waiting runs the round itself
//! ([`wait_durable`]; the others wait for it, and one of them leads the next round if the first
//! did not cover them), so a write costs no thread hand-off. In `CommitMode::Group` the commit
//! thread syncs **on demand**: at once when a [`Durability::persist`] waits; otherwise once the
//! unsynced writes are [`SYNC_DELAY`] old or [`SYNC_BYTES`] large, so a burst nobody waits on is
//! made durable by a few syncs, not one per write. The first failure (a failed sync, a writer that
//! stopped, a panic, damage a read found) is latched: every later barrier and write fails, and the
//! watermark never moves again.

use std::{
    sync::{
        atomic::{fence, AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use super::store::Core;
use crate::archive::data_file::fsync_file;

/// The longest a write nobody waits on stays unsynced in `CommitMode::Group` (the data a crash can
/// lose that no `persist` was told is durable).
const SYNC_DELAY: Duration = Duration::from_millis(2);

/// Unsynced bytes at which the commit thread syncs without waiting out [`SYNC_DELAY`].
const SYNC_BYTES: u64 = 4 << 20;

/// The most unsynced bytes a log holds in `CommitMode::Group` before a writer waits for a sync
/// (backpressure): writes nobody waits on cannot outrun the disk without bound.
const MAX_UNSYNCED: u64 = 64 << 20;

/// Whether the commit thread keeps running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Run {
    Go,
    /// Sync what is pending once more, then stop (the database closed).
    Finish,
    /// Stop at once, syncing nothing (a test's simulated crash).
    Abort,
}

/// The database's durability state, shared by its handles, its writer and its commit thread.
#[derive(Debug)]
pub(super) struct Durability {
    /// The last batch numbered.
    seq: AtomicU64,
    /// Every batch numbered at or below this is durable.
    durable: tokio::sync::watch::Sender<u64>,
    /// The same watermark, for blocking waiters.
    durable_now: Mutex<u64>,
    durable_changed: Condvar,
    /// The first failure, if any (never cleared).
    failed: Mutex<Option<String>>,
    /// Set by every failure but those [`Self::fail_damaged`] latches (damage a read found, a
    /// failure before anything was appended): a failed sync, a
    /// writer that stopped part way or a panic leaves the log's state on disk unknown, so it must
    /// never be sealed as clean. Kept apart from `failed`, which holds only the first cause, so a
    /// sync failing after a read's damage still blocks the seal.
    log_unknown: AtomicBool,
    /// Poked once, at the first failure (see [`Self::failed`]).
    failed_signal: tokio::sync::watch::Sender<()>,
    /// Set to 1 at the first failure (the `tndb_failed` metric).
    failed_gauge: ::metrics::Gauge,
    run: Mutex<Run>,
    wake: Condvar,
    /// The highest batch number a `persist` waits for.
    wanted: AtomicU64,
    /// The commit thread is waiting with nothing to sync: the next write must wake it.
    idle: AtomicBool,
    /// The commit thread has exited: a `persist` whose batches are not durable by then fails
    /// rather than waiting for a sync no thread will run.
    stopped: AtomicBool,
    /// Test-only: the commit thread's next sync fails.
    #[cfg(test)]
    pub(super) fail_next_sync: std::sync::atomic::AtomicBool,
    /// Test-only: the syncs of this database's log so far.
    #[cfg(test)]
    pub(super) syncs: AtomicU64,
    /// Test-only: a delay to use instead of [`SYNC_DELAY`], in milliseconds (0: the default).
    #[cfg(test)]
    pub(super) delay_ms: AtomicU64,
    /// Test-only: a cap to use instead of [`MAX_UNSYNCED`], in bytes (0: the default).
    #[cfg(test)]
    pub(super) max_unsynced: AtomicU64,
    /// Test-only: the commit thread's next sync stops before it starts, reports it on the first
    /// channel and waits for a message (or the sender's drop) on the second.
    #[cfg(test)]
    sync_gate: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

impl Durability {
    pub(super) fn new(failed_gauge: ::metrics::Gauge) -> Arc<Self> {
        Arc::new(Self {
            failed_gauge,
            seq: AtomicU64::new(0),
            durable: tokio::sync::watch::Sender::new(0),
            durable_now: Mutex::new(0),
            durable_changed: Condvar::new(),
            failed: Mutex::new(None),
            log_unknown: AtomicBool::new(false),
            failed_signal: tokio::sync::watch::Sender::new(()),
            run: Mutex::new(Run::Go),
            wake: Condvar::new(),
            wanted: AtomicU64::new(0),
            idle: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_sync: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            syncs: AtomicU64::new(0),
            #[cfg(test)]
            delay_ms: AtomicU64::new(0),
            #[cfg(test)]
            max_unsynced: AtomicU64::new(0),
            #[cfg(test)]
            sync_gate: Mutex::new(None),
        })
    }

    /// The number of the last batch written.
    pub(super) fn last_seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Record the number of the batch just written (under the write lock, before it is
    /// published).
    pub(super) fn set_seq(&self, seq: u64) {
        self.seq.store(seq, Ordering::Release);
    }

    /// A batch moved the frontier: wake the commit thread if it is idle (it starts the delay
    /// before syncing), or if `pending` unsynced bytes reached [`SYNC_BYTES`].
    pub(super) fn wrote(&self, pending: u64) {
        // Pairs with the commit thread's fence between setting `idle` and re-reading the
        // frontier: one of the two sees the other's store.
        fence(Ordering::SeqCst);
        if self.idle.load(Ordering::SeqCst) || pending >= SYNC_BYTES {
            self.wake();
        }
    }

    fn wake(&self) {
        let _run = self.run.lock();
        self.wake.notify_one();
    }

    /// How long writes nobody waits on stay unsynced ([`SYNC_DELAY`]; a test may override it).
    fn delay(&self) -> Duration {
        #[cfg(test)]
        if let ms @ 1.. = self.delay_ms.load(Ordering::Relaxed) {
            return Duration::from_millis(ms);
        }
        SYNC_DELAY
    }

    /// The unsynced bytes past which a group-mode writer waits for a sync ([`MAX_UNSYNCED`]; a
    /// test may override it).
    pub(super) fn max_unsynced(&self) -> u64 {
        #[cfg(test)]
        if let cap @ 1.. = self.max_unsynced.load(Ordering::Relaxed) {
            return cap;
        }
        MAX_UNSYNCED
    }

    /// A `persist` waits for batch `seq`: the commit thread syncs at once.
    fn want(&self, seq: u64) {
        self.wanted.fetch_max(seq, Ordering::AcqRel);
        self.wake();
    }

    /// Wake every writer blocked in [`wait_durable`] (a round ended, or a rollback's flush let go
    /// of the sync lock).
    pub(super) fn notify_waiters(&self) {
        let _now = self.durable_now.lock();
        self.durable_changed.notify_all();
    }

    /// Latch `cause` (the first failure wins, and only it is logged) and wake every waiter: a
    /// failed sync, a writer that stopped, or a panic. The log's state on disk is then unknown
    /// (see `log_unknown`), whatever failed first.
    pub(super) fn fail(&self, cause: String) {
        self.log_unknown.store(true, Ordering::Release);
        self.latch(cause);
    }

    /// Latch damage a read found (see `Core::read_damaged`; a removal's lookup too, see
    /// `Writer::latch_if_poisoned`), or an I/O failure before anything was appended (a table's
    /// registration or first-row setup, see `Core::fail_setup`): every later write and persist
    /// fails, as after any failure, but the log itself is as it was, so the close still seals it.
    pub(super) fn fail_damaged(&self, cause: String) {
        self.latch(cause);
    }

    fn latch(&self, cause: String) {
        {
            let mut failed = self.failed.lock();
            if failed.is_some() {
                return;
            }
            tracing::error!(target: "tndb", "durability failed: {cause}; every later write and persist fails");
            *failed = Some(cause);
            self.failed_gauge.set(1.0);
        }
        self.failed_signal.send_modify(|_| {});
        self.durable.send_modify(|_| {});
        let _now = self.durable_now.lock();
        self.durable_changed.notify_all();
    }

    pub(super) fn is_failed(&self) -> bool {
        self.failed.lock().is_some()
    }

    /// True once a failure left the log's state on disk unknown (see `log_unknown`): it must not
    /// be sealed as clean.
    pub(super) fn log_unknown(&self) -> bool {
        self.log_unknown.load(Ordering::Acquire)
    }

    /// Resolve with the first failure's cause once anything has failed (at once if something
    /// already has); never resolves otherwise. The latch is never cleared.
    pub(super) async fn failed(&self) -> String {
        let mut signal = self.failed_signal.subscribe();
        loop {
            if let Some(cause) = self.failed.lock().clone() {
                return cause;
            }
            // `failed_signal` (a sender the caller's reference keeps alive) never closes.
            let _ = signal.changed().await;
        }
    }

    pub(super) fn check(&self) -> eyre::Result<()> {
        match &*self.failed.lock() {
            Some(cause) => eyre::bail!("tndb: a write or sync failed ({cause})"),
            None => Ok(()),
        }
    }

    /// Test-only: pause the commit thread's next sync (see `sync_gate`).
    #[cfg(test)]
    pub(super) fn gate_next_sync(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (arrived, arrivals) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        *self.sync_gate.lock() = Some((arrived, released));
        (arrivals, release)
    }

    /// The durable watermark.
    pub(super) fn durable(&self) -> u64 {
        *self.durable_now.lock()
    }

    /// Advance the watermark to `durable` (never back, and never after a failure).
    fn advance(&self, durable: u64) {
        if self.is_failed() {
            return;
        }
        self.durable.send_if_modified(|current| {
            let advanced = durable > *current;
            if advanced {
                *current = durable;
            }
            advanced
        });
        let mut now = self.durable_now.lock();
        if durable > *now {
            *now = durable;
            self.durable_changed.notify_all();
        }
    }

    /// The durability barrier: wait until every batch up to `target` (the last one written when
    /// the persist was asked for) is durable, or fail once anything has failed or the commit
    /// thread has stopped short.
    pub(super) async fn persist(&self, target: u64) -> eyre::Result<()> {
        let mut durable = self.durable.subscribe();
        if *durable.borrow() < target {
            self.want(target);
        }
        loop {
            self.check()?;
            if *durable.borrow_and_update() >= target {
                // A failure latched between the check above and this read wins.
                self.check()?;
                return Ok(());
            }
            if self.stopped.load(Ordering::Acquire) {
                eyre::bail!("tndb: the commit thread stopped before the write was durable");
            }
            // `durable` (a sender this future's `Arc` keeps alive) never closes: a stopping commit
            // thread wakes it instead (see `stopped`).
            let _ = durable.changed().await;
        }
    }

    /// The commit thread has exited: wake every waiter (see `stopped`).
    fn mark_stopped(&self) {
        self.stopped.store(true, Ordering::Release);
        self.durable.send_modify(|_| {});
        let _now = self.durable_now.lock();
        self.durable_changed.notify_all();
    }
}

/// Block until batch `seq` is durable, or fail once anything has failed. The first writer waiting
/// leads: it runs the sync round itself while the others wait for it; one whose batch the round
/// did not cover (it landed after the round read the frontier) leads the next.
pub(super) fn wait_durable(core: &Core, seq: u64) -> eyre::Result<()> {
    let durability = &*core.durability;
    loop {
        let lead = {
            let mut now = durability.durable_now.lock();
            loop {
                durability.check()?;
                if *now >= seq {
                    return Ok(());
                }
                if let Some(lead) = core.sync_lock.try_lock() {
                    break lead;
                }
                durability.durable_changed.wait(&mut now);
            }
        };
        let before = durability.durable();
        sync_round(core);
        drop(lead);
        durability.notify_waiters();
        if durability.durable() == before {
            // The round covered nothing new: `seq` is numbered, but its writer has not stored the
            // frontier yet. Give it a moment rather than retake the lock at once and spin.
            let mut now = durability.durable_now.lock();
            if *now < seq && !durability.is_failed() {
                durability.durable_changed.wait_for(&mut now, std::time::Duration::from_millis(1));
            }
        }
    }
}

/// Sync the log up to the current frontier and advance the watermark (or latch the failure). The
/// caller holds the core's sync lock, so one round runs at a time.
fn sync_round(core: &Core) {
    let durability = &*core.durability;
    if durability.is_failed() {
        return;
    }
    let frontier = core.frontier.load_full();
    if frontier.seq <= durability.durable() {
        return;
    }
    #[cfg(test)]
    {
        let gate = durability.sync_gate.lock().take();
        if let Some((arrived, release)) = gate {
            let _ = arrived.send(());
            let _ = release.recv_timeout(std::time::Duration::from_secs(30));
        }
    }
    let started = std::time::Instant::now();
    let sync = &frontier.sync;
    let start = sync.synced.load(Ordering::Acquire);
    let outcome = (|| -> std::io::Result<()> {
        #[cfg(test)]
        if durability.fail_next_sync.swap(false, Ordering::Relaxed) {
            return Err(std::io::Error::other("injected sync failure"));
        }
        if frontier.end > start {
            sync.view.msync(start, frontier.end)?;
        }
        // The writer stores the growth count before the frontier, so read after it, it covers
        // every growth below the frontier's end.
        let growths = sync.growths.load(Ordering::Acquire);
        if growths != sync.size_synced.load(Ordering::Relaxed) {
            fsync_file(&sync.file)?;
            sync.size_synced.store(growths, Ordering::Relaxed);
        }
        Ok(())
    })();
    #[cfg(test)]
    {
        super::stats::add(&super::stats::SYNCS, &super::stats::SYNC_NS, started.elapsed());
        super::stats::log(core.stat_slot).sync(started.elapsed());
        durability.syncs.fetch_add(1, Ordering::Relaxed);
    }
    match outcome {
        Ok(()) => {
            sync.synced.fetch_max(frontier.end, Ordering::AcqRel);
            // Recorded before the watermark moves, so a persist that returns has it counted.
            core.metrics.synced(started.elapsed(), core.unsynced());
            durability.advance(frontier.seq);
        }
        Err(e) => durability.fail(format!("sync the log: {e}")),
    }
}

/// The `tndb-commit` thread, stopped (after a last round) when dropped.
#[derive(Debug)]
pub(super) struct Committer {
    durability: Arc<Durability>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Committer {
    pub(super) fn start(core: Arc<Core>) -> eyre::Result<Self> {
        let durability = Arc::clone(&core.durability);
        let handle = std::thread::Builder::new()
            .name("tndb-commit".to_string())
            .spawn(move || run(&core))?;
        Ok(Self { durability, handle: Mutex::new(Some(handle)) })
    }

    fn stop(&self, how: Run) {
        {
            let mut run = self.durability.run.lock();
            if *run == Run::Go {
                *run = how;
            }
            self.durability.wake.notify_one();
        }
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }

    /// Test-only, for a simulated crash: stop at once, syncing nothing more.
    #[cfg(test)]
    pub(super) fn abort_for_crash(&self) {
        self.stop(Run::Abort);
    }
}

impl Drop for Committer {
    fn drop(&mut self) {
        self.stop(Run::Finish);
    }
}

/// Latches a failure (with the given cause) if dropped while unwinding: a panic in the step it
/// covers leaves state no later write or barrier may build on.
pub(super) struct UnwindLatch<'a>(pub(super) &'a Durability, pub(super) &'static str);

impl Drop for UnwindLatch<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.fail(self.1.to_string());
        }
    }
}

/// Marks the commit thread stopped when dropped (see `Durability::stopped`).
struct StopMark<'a>(&'a Durability);

impl Drop for StopMark<'_> {
    fn drop(&mut self) {
        self.0.mark_stopped();
    }
}

/// The commit thread (`CommitMode::Group`'s syncs): wait until a sync is due (a `persist` waits,
/// or the unsynced writes are [`SYNC_DELAY`] old or [`SYNC_BYTES`] large, or the database is
/// closing), then run a round.
fn run(core: &Core) {
    let durability = &*core.durability;
    // A panic latches: no barrier then waits for this thread forever.
    let _latch = UnwindLatch(durability, "the commit thread panicked");
    // However the thread exits, a persist still waiting on it is woken to fail.
    let _stopped = StopMark(durability);
    // When the commit thread first saw writes not yet durable.
    let mut pending_since: Option<Instant> = None;
    loop {
        {
            let mut run = durability.run.lock();
            loop {
                if *run == Run::Abort {
                    return;
                }
                if durability.is_failed() {
                    // Nothing more becomes durable: wait to be stopped.
                    if *run == Run::Go {
                        durability.wake.wait(&mut run);
                        continue;
                    }
                    return;
                }
                let durable = durability.durable();
                let frontier = core.frontier.load();
                if frontier.seq <= durable {
                    pending_since = None;
                    if *run == Run::Finish {
                        return;
                    }
                    // Idle until a write: announce it, then look again (a write that stored its
                    // frontier before seeing `idle` is seen here; see `Durability::wrote`).
                    durability.idle.store(true, Ordering::SeqCst);
                    fence(Ordering::SeqCst);
                    if core.frontier.load().seq <= durability.durable() {
                        durability.wake.wait(&mut run);
                    }
                    durability.idle.store(false, Ordering::SeqCst);
                    continue;
                }
                let since = *pending_since.get_or_insert_with(Instant::now);
                let wanted = durability.wanted.load(Ordering::Acquire) > durable;
                let pending =
                    frontier.end.saturating_sub(frontier.sync.synced.load(Ordering::Acquire));
                let delay = durability.delay();
                if wanted
                    || pending >= SYNC_BYTES
                    || since.elapsed() >= delay
                    || *run == Run::Finish
                {
                    break;
                }
                let _ = durability.wake.wait_for(&mut run, delay.saturating_sub(since.elapsed()));
            }
        }
        pending_since = None;
        let round = core.sync_lock.lock();
        sync_round(core);
        drop(round);
        durability.notify_waiters();
    }
}
