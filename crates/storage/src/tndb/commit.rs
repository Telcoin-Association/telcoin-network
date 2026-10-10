//! Group commit: writes return without a disk sync, and one background thread commits them.
//!
//! In [`CommitMode::Group`](super::CommitMode) a write applies to its table and is published at
//! once (readable from any thread on return), numbered from a database-wide sequence. A
//! `tndb-commit` thread ([`Committer`]) then commits every table written since its last round:
//! one sync per table per round, however many writes landed, with the sync itself outside the
//! table's writer lock (see `TnTable::group_commit`), so writers never wait on the disk.
//!
//! [`GroupCommit::persist`] is the durability barrier: it resolves once every write numbered
//! before the call is committed, in every table (a whole-database, FIFO barrier), and fails from
//! the first failed write or commit on (a latch never cleared). A table written by an open write
//! transaction is left uncommitted until the transaction ends, so a transaction never becomes
//! durable in part, and the barrier waits for it.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use super::database::StoreType;

/// How often the committer asks every table to compact (see `Database::compact`).
const COMPACT_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// Whether the committer keeps running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Run {
    Go,
    /// Commit what is pending once more, then stop (the database closed).
    Finish,
    /// Stop at once, committing nothing (a test's simulated crash).
    Abort,
}

/// Tables written since the committer's last look, and whether it keeps running.
#[derive(Debug)]
struct Work {
    dirty: BTreeSet<&'static str>,
    run: Run,
}

/// Group-commit state shared by a database's handles, its write transactions and its committer.
#[derive(Debug)]
pub(crate) struct GroupCommit {
    /// The last sequence number given to a write (see `TnTable::publish_pending`).
    pub(crate) seq: AtomicU64,
    /// Every write numbered at or below this is committed.
    durable: tokio::sync::watch::Sender<u64>,
    /// The first failure, if any: every later barrier reports it (never cleared).
    failed: Mutex<Option<String>>,
    work: Mutex<Work>,
    wake: Condvar,
    /// Test-only: the committer's next round stops at a point, reports it, and waits to be
    /// released (see [`Self::gate_next_round`]).
    #[cfg(test)]
    round_gate: Mutex<Option<RoundGate>>,
}

/// Test-only: where a committer round stops (see [`GroupCommit::gate_next_round`]).
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatePoint {
    /// After reading its target number, before taking the dirty tables.
    BeforeTake,
    /// After taking the dirty tables.
    AfterTake,
}

#[cfg(test)]
type RoundGate = (GatePoint, std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);

impl GroupCommit {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            seq: AtomicU64::new(0),
            durable: tokio::sync::watch::Sender::new(0),
            failed: Mutex::new(None),
            work: Mutex::new(Work { dirty: BTreeSet::new(), run: Run::Go }),
            wake: Condvar::new(),
            #[cfg(test)]
            round_gate: Mutex::new(None),
        })
    }

    /// Test-only: the committer's next round stops at `point`, reports it on the first channel,
    /// and waits for a message (or the sender's drop) on the second.
    #[cfg(test)]
    pub(crate) fn gate_next_round(
        &self,
        point: GatePoint,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (arrived, arrivals) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        *self.round_gate.lock() = Some((point, arrived, released));
        (arrivals, release)
    }

    /// Test-only: stop here if the next round's gate is at `point` (see [`Self::gate_next_round`]).
    #[cfg(test)]
    fn gate(&self, point: GatePoint) {
        // Taken first, so the gate's lock is not held while waiting at it.
        let gate = {
            let mut slot = self.round_gate.lock();
            match slot.as_ref() {
                Some((at, ..)) if *at == point => slot.take(),
                _ => None,
            }
        };
        if let Some((_, arrived, release)) = gate {
            let _ = arrived.send(());
            let _ = release.recv_timeout(Duration::from_secs(30));
        }
    }

    /// Table `name` has writes to commit. Called under the table's writer lock, before the
    /// write's sequence number is taken (see `TnTable::publish_pending`).
    pub(crate) fn mark_dirty(&self, name: &'static str) {
        self.work.lock().dirty.insert(name);
        self.wake.notify_one();
    }

    /// Latch `cause` (the first failure wins) and wake every barrier waiting.
    pub(crate) fn fail(&self, cause: String) {
        tracing::error!(target: "tndb", "group commit failed: {cause}; every later persist fails");
        self.failed.lock().get_or_insert(cause);
        self.durable.send_modify(|_| {});
    }

    fn check(&self) -> eyre::Result<()> {
        match &*self.failed.lock() {
            Some(cause) => eyre::bail!("tndb: a write or commit failed ({cause})"),
            None => Ok(()),
        }
    }

    /// The durability barrier: resolves once every write numbered before the call is committed
    /// (in every table), or fails once any write or commit has failed.
    pub(crate) async fn persist(&self) -> eyre::Result<()> {
        let target = self.seq.load(Ordering::Acquire);
        let mut durable = self.durable.subscribe();
        loop {
            self.check()?;
            if *durable.borrow_and_update() >= target {
                return Ok(());
            }
            if durable.changed().await.is_err() {
                eyre::bail!("tndb: the committer stopped");
            }
        }
    }

    /// Advance the durable watermark to `durable` (it never moves back).
    fn advance(&self, durable: u64) {
        self.durable.send_if_modified(|current| {
            let advanced = durable > *current;
            if advanced {
                *current = durable;
            }
            advanced
        });
    }
}

/// The `tndb-commit` thread, stopped (after a last round) when dropped.
#[derive(Debug)]
pub(crate) struct Committer {
    group: Arc<GroupCommit>,
    handle: Option<JoinHandle<()>>,
}

impl Committer {
    pub(crate) fn start(group: Arc<GroupCommit>, store: Arc<StoreType>) -> eyre::Result<Self> {
        let thread_group = Arc::clone(&group);
        let handle = std::thread::Builder::new()
            .name("tndb-commit".to_string())
            .spawn(move || run(&thread_group, &store))?;
        Ok(Self { group, handle: Some(handle) })
    }

    fn stop(&mut self, how: Run) {
        {
            let mut work = self.group.work.lock();
            if work.run == Run::Go {
                work.run = how;
            }
        }
        self.group.wake.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Test-only, for a simulated crash: stop at once, committing nothing more.
    #[cfg(test)]
    pub(crate) fn abort_for_crash(&mut self) {
        self.stop(Run::Abort);
    }
}

impl Drop for Committer {
    fn drop(&mut self) {
        self.stop(Run::Finish);
    }
}

/// The committer: wait for written tables, commit each (one sync per table per round), then
/// advance the durable watermark to the round's starting sequence number, held back by any table
/// still pending (an open transaction, or writes published during its sync). A held-back table
/// stays in every later round until it commits, so the watermark never passes it.
fn run(group: &GroupCommit, store: &StoreType) {
    let compact_all = || {
        for entry in store.load().values() {
            entry.table.compact();
        }
    };
    compact_all();
    let mut last_compact = Instant::now();
    let mut held: BTreeMap<&'static str, u64> = BTreeMap::new();
    // Run another round at once, without a dirty table: see the end of the loop.
    let mut again = false;
    loop {
        {
            let mut work = group.work.lock();
            while work.dirty.is_empty() && work.run == Run::Go && !again {
                let wait = COMPACT_EVERY.saturating_sub(last_compact.elapsed());
                if group.wake.wait_for(&mut work, wait).timed_out() {
                    break;
                }
            }
        }
        // The target number is read before the dirty tables are taken: a writer marks its table
        // dirty before it takes its number, so every write numbered at or below the target has
        // its table in this round's set (or was in an earlier round's).
        let target = group.seq.load(Ordering::Acquire);
        #[cfg(test)]
        group.gate(GatePoint::BeforeTake);
        let (names, run) = {
            let mut work = group.work.lock();
            (std::mem::take(&mut work.dirty), work.run)
        };
        if run == Run::Abort {
            return;
        }
        #[cfg(test)]
        group.gate(GatePoint::AfterTake);
        if last_compact.elapsed() >= COMPACT_EVERY {
            compact_all();
            last_compact = Instant::now();
        }
        let tables = store.load();
        let mut todo = names;
        todo.extend(held.keys().copied());
        for name in todo {
            let Some(entry) = tables.get(name) else {
                held.remove(name);
                continue;
            };
            match entry.table.group_commit() {
                Ok(Some(first)) => {
                    held.insert(name, first);
                }
                Ok(None) => {
                    held.remove(name);
                }
                Err(e) => {
                    held.remove(name);
                    group.fail(format!("table {name}: {e:#}"));
                }
            }
        }
        let durable = held.values().map(|first| first - 1).fold(target, u64::min);
        group.advance(durable);
        if run == Run::Finish {
            return;
        }
        // A write numbered after the target, whose table this round took and committed, has no
        // dirty mark left to start a round, yet the watermark stopped at the target. One more round
        // reads a target covering it (and commits nothing it need not). A table held back by an
        // open transaction is marked dirty when the transaction ends, so it needs no such round.
        again = held.is_empty() && group.seq.load(Ordering::Acquire) > durable;
    }
}
