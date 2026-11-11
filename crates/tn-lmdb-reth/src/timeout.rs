//! The read transaction timeout: MDBX's `max_read_transaction_duration`, as reth's
//! `DatabaseArguments` sets it, on tn-lmdb.
//!
//! A read transaction holds its snapshot, and with it every page freed since, until it ends: a
//! reader that lives on keeps the file growing (`tn_lmdb::Env::oldest_reader_age`). MDBX's
//! transaction manager aborts a read transaction past the limit from another thread; the
//! transaction's later calls fail with `ReadTransactionTimeout`. Here the engine's read
//! transaction cannot be ended from outside while its handle and cursors live (they own it), so
//! the timeout marks the reader instead: a thread per environment ([`ReaderTimeouts`]) keeps the
//! deadline of every read transaction, in order (one limit, so the order they began in), and
//! sets each one's [`ReaderState`] once its deadline passes. From then on every call of the
//! transaction and of its cursors fails with MDBX's `ReadTransactionTimeout` (`-96000`), and
//! reth's code drops the transaction on the error, which ends the engine's reader and frees its
//! pages for reuse. A timed-out transaction nobody calls keeps its pages until it is dropped;
//! `db.timed_out_not_aborted_transactions` counts those still open.
//!
//! `disable_long_read_transaction_safety` takes a transaction off the queue (the state export and
//! the migration use it); one already timed out stays so. The readers pay one relaxed atomic load
//! per call; a begin and an end take the queue's mutex once each.

use crate::error::Mdbx;
use parking_lot::{Condvar, Mutex};
use reth_db_api::DatabaseError;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

/// MDBX's default read transaction duration (reth's `DEFAULT_MAX_READ_TRANSACTION_DURATION`):
/// five minutes.
pub const DEFAULT_READ_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// The state of one read transaction under the timeout, shared by its handle and its cursors.
#[derive(Debug)]
pub(crate) struct ReaderState {
    /// Set by the timeout thread once the reader passed the limit (under the queue's lock);
    /// every call of the transaction and its cursors then fails ([`Self::check`]).
    expired: AtomicBool,
    /// The reader's place in the queue; 0 for a reader of an environment without a timeout.
    seq: u64,
}

impl ReaderState {
    /// A reader no timeout watches.
    pub(crate) fn untimed() -> Arc<Self> {
        Arc::new(Self { expired: AtomicBool::new(false), seq: 0 })
    }

    /// `Ok` while the reader is within its limit, else `error` of MDBX's `ReadTransactionTimeout`.
    #[inline]
    pub(crate) fn check(
        &self,
        error: fn(reth_storage_errors::db::DatabaseErrorInfo) -> DatabaseError,
    ) -> Result<(), DatabaseError> {
        if self.expired.load(Ordering::Relaxed) {
            return Err(error(Mdbx::READ_TRANSACTION_TIMEOUT.info()));
        }
        Ok(())
    }
}

/// The read transactions of one environment by deadline, and the thread that times them out.
#[derive(Debug)]
pub(crate) struct ReaderTimeouts {
    /// How long a read transaction may live.
    limit: Duration,
    /// The readers not yet timed out, by the order they began in, which is their deadlines'.
    queue: Mutex<Queue>,
    /// Signalled when a reader joins an empty queue, and to stop the thread.
    wake: Condvar,
    /// The readers timed out and still open (`db.timed_out_not_aborted_transactions`).
    timed_out_open: AtomicU64,
}

/// The queue behind [`ReaderTimeouts::queue`].
#[derive(Debug, Default)]
struct Queue {
    /// The sequence number the next reader takes (from 1: 0 is no place).
    next_seq: u64,
    /// Each waiting reader's deadline and state, by sequence number.
    readers: BTreeMap<u64, (Instant, Arc<ReaderState>)>,
    /// Set when the environment closes: the thread ends at its next wake.
    stop: bool,
}

impl ReaderTimeouts {
    /// Start the timeout of an environment whose read transactions may live `limit`: the queue
    /// and its thread, named `tn-lmdb-reader-timeout`. A thread the host refuses is
    /// [`DatabaseError::Open`].
    pub(crate) fn start(limit: Duration) -> Result<Arc<Self>, DatabaseError> {
        let this = Arc::new(Self {
            limit,
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
            timed_out_open: AtomicU64::new(0),
        });
        let thread = Arc::clone(&this);
        thread::Builder::new()
            .name("tn-lmdb-reader-timeout".into())
            .spawn(move || thread.run())
            .map_err(|e| {
                DatabaseError::Open(reth_storage_errors::db::DatabaseErrorInfo {
                    message: format!("the read transaction timeout thread: {e}").into(),
                    code: e.raw_os_error().unwrap_or(libc::EAGAIN),
                })
            })?;
        Ok(this)
    }

    /// The limit the environment was opened with.
    pub(crate) fn limit(&self) -> Duration {
        self.limit
    }

    /// Put a read transaction that begins now on the queue, and give it its state.
    pub(crate) fn begin(&self) -> Arc<ReaderState> {
        let mut queue = self.queue.lock();
        queue.next_seq += 1;
        let state = Arc::new(ReaderState { expired: AtomicBool::new(false), seq: queue.next_seq });
        let was_empty = queue.readers.is_empty();
        queue.readers.insert(state.seq, (Instant::now() + self.limit, Arc::clone(&state)));
        if was_empty {
            self.wake.notify_one();
        }
        state
    }

    /// Take a read transaction off the queue (`disable_long_read_transaction_safety`): it no
    /// longer times out. One already timed out stays so.
    pub(crate) fn opt_out(&self, state: &ReaderState) {
        if state.seq != 0 {
            self.queue.lock().readers.remove(&state.seq);
        }
    }

    /// A read transaction ended: off the queue, or off the count of timed-out readers still
    /// open. Under the lock, so a reader timing out as it ends is counted exactly once.
    pub(crate) fn ended(&self, state: &ReaderState) {
        if state.seq == 0 {
            return;
        }
        let mut queue = self.queue.lock();
        if queue.readers.remove(&state.seq).is_none() && state.expired.load(Ordering::Relaxed) {
            self.timed_out_open.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// The readers timed out and still open.
    pub(crate) fn timed_out_open(&self) -> u64 {
        self.timed_out_open.load(Ordering::Relaxed)
    }

    /// Stop the thread (the environment is closing). Readers still open keep their state; one on
    /// the queue is never timed out after this.
    pub(crate) fn stop(&self) {
        self.queue.lock().stop = true;
        self.wake.notify_all();
    }

    /// The thread: time out every reader past its deadline, then sleep until the next deadline
    /// or until a reader joins an empty queue.
    fn run(&self) {
        let mut queue = self.queue.lock();
        loop {
            if queue.stop {
                return;
            }
            let now = Instant::now();
            while let Some((&seq, &(deadline, _))) = queue.readers.first_key_value() {
                if deadline > now {
                    break;
                }
                let (_, state) = queue.readers.remove(&seq).expect("the first reader");
                state.expired.store(true, Ordering::Relaxed);
                self.timed_out_open.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    limit = ?self.limit,
                    "A database read transaction has lived past the read transaction timeout: \
                     its next call fails, and the pages freed since it began are not reused \
                     until it is dropped"
                );
            }
            match queue.readers.first_key_value() {
                Some((_, &(deadline, _))) => {
                    self.wake.wait_until(&mut queue, deadline);
                }
                None => self.wake.wait(&mut queue),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader on the queue times out at its deadline and not before, is counted while open and
    /// not after it ends, and one that opted out never times out.
    #[test]
    fn readers_time_out_at_their_deadline_and_are_counted_while_open() {
        let timeouts = ReaderTimeouts::start(Duration::from_millis(50)).expect("start");
        let early = timeouts.begin();
        let kept = timeouts.begin();
        timeouts.opt_out(&kept);
        assert!(early.check(DatabaseError::Read).is_ok(), "within the limit");
        thread::sleep(Duration::from_millis(150));
        let got = early.check(DatabaseError::Read);
        assert!(matches!(&got, Err(DatabaseError::Read(info)) if info.code == -96000), "{got:?}");
        assert!(kept.check(DatabaseError::Read).is_ok(), "opted out");
        assert_eq!(timeouts.timed_out_open(), 1, "one timed out and open");
        timeouts.ended(&early);
        timeouts.ended(&kept);
        assert_eq!(timeouts.timed_out_open(), 0, "none open");
        let late = timeouts.begin();
        timeouts.ended(&late);
        thread::sleep(Duration::from_millis(100));
        assert!(late.check(DatabaseError::Read).is_ok(), "ended before its deadline");
        assert_eq!(timeouts.timed_out_open(), 0);
        timeouts.stop();
        assert!(ReaderState::untimed().check(DatabaseError::Read).is_ok());
    }
}
