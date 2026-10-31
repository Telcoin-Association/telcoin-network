//! Compaction: rewrite the log's live rows into a new generation, in the background.
//!
//! The `tndb-compact` thread ([`Compactor`]) builds the next generation in a `compact-*`
//! directory:
//! 1. **snapshot**, under the write lock: every table's last publish and the log's end `E`, one
//!    point between two batches;
//! 2. **copy**, holding no lock and paced: each table's rows in key order, each its own batch
//!    (removed, overwritten and cleared records are never reached);
//! 3. **catch-up**, holding no lock and unpaced: what the old log's batches since `E` changed, in
//!    rounds (each synced) until one had little left, or at most 8 (a writer faster than the
//!    catch-up is caught up by the switch's last try). A round writes its **net effect**: each
//!    touched key's last write (a live row, copied as its own batch, or a removal of a row the new
//!    log holds), after a clear of each table it cleared. So what was written and made dead during
//!    the compaction is not copied, and the new log stays about the size of the live rows. Batch
//!    boundaries within the copy do not matter: all of it is synced before the new generation is
//!    renamed into place, and only then can a crash recover it;
//! 4. **switch**, under the registry lock and then the write lock: the last batches, a row-count
//!    check, a sync, the rename into place (`gen-N+1`) and a directory sync, then the writer moves
//!    to the new generation and publishes every table from it. A try that finds more than 4 MiB
//!    landed meanwhile lets go of both locks and catches up again; the last of three takes up to 64
//!    MiB under the write lock (writers wait once), so a compaction finishes at any sustained write
//!    rate.
//!
//! Both generations hold every batch from the moment the new one is renamed into place (it was
//! synced first), so a crash on either side of the rename loses nothing; a `compact-*` directory
//! is never a generation, and an open deletes a leftover one. A compaction that fails before the
//! rename is abandoned (its files are deleted) and the automatic trigger backs off; one that fails
//! after it (or whose rename reports an error yet moved the directory) stops the writer, since
//! which generation the next open picks is then unknown.
//!
//! The automatic trigger ([`Writer::wants_compaction`](super::store)) needs no accounting per
//! write: the log compacts once it is at least `auto_min_bytes` long, has at least doubled since
//! the last compaction (its length then is kept in the generation's `compacted` file), and may
//! hold a dead row.

use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fs,
    io::Write as _,
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use eyre::{bail, WrapErr as _};
use parking_lot::{Condvar, Mutex};

use super::{
    commit::UnwindLatch,
    layout::{available_bytes, compact_dir, gen_dir, remove_in_background, sync_dir, sync_file},
    metrics::{Cause, Outcome},
    record::{self, Op, BATCH_END, KIND_CLEAR, KIND_REMOVE},
    store::{
        idx_dir, open_log, Core, GenAlive, GenSync, Log, NewGen, ScanKind, SnapTable, MAX_RECORD,
    },
    TableId,
};
use crate::archive::{
    btree_index::BtreeIndex, crc::crc32, data_file::MapView, error::fetch::FetchError,
    pack::DataHeader,
};

/// A requested compaction ([`tn_types::Database::compact`]) skips a log smaller than this.
const REQUESTED_MIN_BYTES: u64 = 1 << 20;

/// Bytes copied between pacing (cancellation and free-space) checks, and between the starts of
/// a new log's writeback.
const BATCH_BYTES: u64 = 1 << 20;

/// The background catch-up stops once a round has had at most this many bytes to replay, leaving
/// what accumulates after it to the switch.
const CATCHUP_BYTES: u64 = 1 << 20;

/// Most background catch-up rounds before going on to the switch regardless.
const MAX_CATCHUP_ROUNDS: usize = 8;

/// The longest a pacing sleep goes without checking for a cancellation.
const PACE_SLICE: Duration = Duration::from_millis(10);

/// Room a compaction needs beyond the current generation's size (the most its live rows can
/// take): the new log's growth preallocation, at most this much past its data.
const DISK_HEADROOM: u64 = 128 << 20;

/// Test-only: how long `CompactShared::compact_now` waits for its compaction before it panics.
#[cfg(test)]
const COMPACT_NOW_TIMEOUT: Duration = Duration::from_secs(120);

/// After a failed automatic compaction, the automatic trigger waits this long before trying again,
/// doubling with each further failure up to [`AUTO_BACKOFF_MAX`].
const AUTO_BACKOFF_MIN: Duration = Duration::from_secs(60);
const AUTO_BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);

/// True when `e` comes from reading damaged committed data (a failed CRC, a corrupt record or
/// index), rather than from the environment.
fn is_corruption(e: &eyre::Report) -> bool {
    e.chain().any(|cause| {
        cause.is::<DamagedRecord>()
            || matches!(
                cause.downcast_ref::<FetchError>(),
                Some(FetchError::CrcFailed | FetchError::CorruptIndex(_))
            )
    })
}

/// A record of the old log that read back whole (its CRC held) but does not parse: damage (see
/// [`record::parse`]), carried in the catch-up's error so the run loop counts it as corruption.
#[derive(Debug)]
struct DamagedRecord(String);

impl std::fmt::Display for DamagedRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DamagedRecord {}

/// The most the switch replays under the write lock: past it, the switch lets go of its locks
/// and runs another background round first, so writers wait for a small tail only.
const SWITCH_TAIL_MAX: u64 = 4 << 20;

/// Switch attempts before a compaction is abandoned (the writer kept outrunning it). The last one
/// takes a tail of up to [`STALL_TAIL_MAX`].
const SWITCH_TRIES: usize = 3;

/// The most the switch's last try replays under the write lock: when the writer has outrun every
/// background round and every earlier try, writers wait once while the rest is caught up (a
/// bounded stall), so a compaction finishes at any sustained write rate (and the table indexes,
/// which grow while a copy pins their snapshot, are replaced by fresh ones).
const STALL_TAIL_MAX: u64 = 64 << 20;

/// Free bytes below which a running compaction gives way to the writer (abandons): a full disk
/// under the writer's own log growth would stop the database until a restart.
const MIN_FREE: u64 = 2 * DISK_HEADROOM;

/// A generation's `compacted` file: the trigger's base, the new log's length just before the
/// switch's final tail (the batches it replays under the write lock), not at the switch itself:
/// it is written before the write lock is taken, so the switch's stall holds no extra file sync.
/// The tail is small next to the log (at most [`STALL_TAIL_MAX`]), so the trigger barely moves.
const COMPACTED: &str = "compacted";
const COMPACTED_TMP: &str = "compacted.tmp";

/// A record's frame beyond its payload: its length and CRC.
pub(super) const FRAME: u64 = 8;

/// The trigger's base of the generation at `gen_path` (its log's length before the final tail of
/// the switch that made it; see [`COMPACTED`]), or 0 when no compaction made it (or its
/// `compacted` file is unreadable: it is only the trigger's starting point).
pub(super) fn read_base(gen_path: &Path) -> u64 {
    let path = gen_path.join(COMPACTED);
    match fs::read(&path) {
        Ok(bytes) if bytes.len() == 12 && crc32(&bytes[..8]).to_le_bytes() == bytes[8..] => {
            u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"))
        }
        Ok(_) => {
            tracing::warn!(target: "tndb", "{} is corrupt; compacting from 0", path.display());
            0
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => {
            tracing::warn!(target: "tndb", "read {}: {e}; compacting from 0", path.display());
            0
        }
    }
}

/// Durably write `base` as the `compacted` file of the generation being built in `dir`.
fn write_base(dir: &Path, base: u64) -> eyre::Result<()> {
    let mut bytes = base.to_le_bytes().to_vec();
    bytes.extend_from_slice(&crc32(&bytes).to_le_bytes());
    let tmp = dir.join(COMPACTED_TMP);
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        sync_file(&file)?;
    }
    fs::rename(&tmp, dir.join(COMPACTED))?;
    sync_dir(dir)?;
    Ok(())
}

/// The compaction thread's requests and state, shared through the database's core.
#[derive(Debug)]
pub(super) struct CompactShared {
    state: Mutex<State>,
    /// Wakes the thread (a request, or a stop).
    wake: Condvar,
    /// Wakes those waiting for a compaction to finish.
    finished: Condvar,
    /// Checked by a running compaction between steps: set to stop it (a close).
    cancel: AtomicBool,
    /// A compaction is running: writes need not ask for another.
    running: AtomicBool,
    /// Test-only hooks.
    #[cfg(test)]
    pub(super) test: TestHooks,
}

#[derive(Debug)]
struct State {
    /// The automatic trigger fired.
    wanted: bool,
    /// [`tn_types::Database::compact`] asked for one.
    requested: bool,
    stop: bool,
    /// After a failure, the automatic trigger waits until then, and the next wait is `backoff`.
    retry_at: Option<Instant>,
    backoff: Duration,
    /// Compactions finished (switched, skipped or abandoned), and whether the last one switched
    /// (or why it was abandoned).
    done: u64,
    last: Option<Result<bool, String>>,
    /// Test-only: every finished compaction's outcome, in order. A waiter for the n-th reads the
    /// n-th: a request decided before a switch and delivered after it can start one more (which
    /// finds nothing due), and `last` may already be that one's.
    #[cfg(test)]
    outcomes: Vec<Result<bool, String>>,
}

impl Default for CompactShared {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                wanted: false,
                requested: false,
                stop: false,
                retry_at: None,
                backoff: AUTO_BACKOFF_MIN,
                done: 0,
                last: None,
                #[cfg(test)]
                outcomes: Vec::new(),
            }),
            wake: Condvar::new(),
            finished: Condvar::new(),
            cancel: AtomicBool::new(false),
            running: AtomicBool::new(false),
            #[cfg(test)]
            test: TestHooks::default(),
        }
    }
}

impl CompactShared {
    /// The automatic trigger fired (after a batch, holding no lock): wake the thread, unless a
    /// compaction is running, one is already wanted, or the trigger is backing off.
    pub(super) fn request_auto(&self) {
        if self.running.load(Ordering::Acquire) {
            return;
        }
        #[cfg(test)]
        checkpoint(&self.test.request_pause.lock().take(), 0);
        let mut state = self.state.lock();
        // `running` again, under the lock that starts a compaction: one that started since the
        // check above must not be followed by another as soon as it ends, nor, if it is abandoned,
        // before its backoff.
        if self.running.load(Ordering::Acquire)
            || state.wanted
            || state.stop
            || state.retry_at.is_some_and(|at| Instant::now() < at)
        {
            return;
        }
        state.wanted = true;
        self.wake.notify_one();
    }

    /// Compact now ([`tn_types::Database::compact`]), unless the log is tiny.
    pub(super) fn request(&self) {
        let mut state = self.state.lock();
        state.requested = true;
        self.wake.notify_one();
    }

    /// Test-only: compact now, whatever the log's size, and wait for it: whether it switched, or
    /// why it was abandoned. Panics if it has not finished within [`COMPACT_NOW_TIMEOUT`], so a
    /// stuck compaction fails its test rather than hanging it.
    #[cfg(test)]
    pub(super) fn compact_now(&self) -> Result<bool, String> {
        self.compact_now_within(COMPACT_NOW_TIMEOUT)
    }

    /// Test-only: [`Self::compact_now`], panicking unless it finishes within `timeout`.
    #[cfg(test)]
    pub(super) fn compact_now_within(&self, timeout: Duration) -> Result<bool, String> {
        self.test.force.store(true, Ordering::Relaxed);
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        let target = state.done + 1 + u64::from(self.running.load(Ordering::Acquire));
        state.requested = true;
        self.wake.notify_one();
        while state.done < target {
            if self.finished.wait_until(&mut state, deadline).timed_out() && state.done < target {
                panic!("compact_now: compaction {target} did not finish within {timeout:?}");
            }
        }
        state.outcomes[target as usize - 1].clone()
    }

    /// Test-only: wait until `n` compactions have finished: the n-th one's outcome.
    #[cfg(test)]
    pub(super) fn wait_done(&self, n: u64) -> Option<Result<bool, String>> {
        let mut state = self.state.lock();
        while state.done < n {
            if self.finished.wait_for(&mut state, Duration::from_secs(30)).timed_out() {
                return None;
            }
        }
        state.outcomes.get(usize::try_from(n).ok()?.checked_sub(1)?).cloned()
    }

    /// Test-only: compactions finished so far.
    #[cfg(test)]
    pub(super) fn done(&self) -> u64 {
        self.state.lock().done
    }

    fn check_cancel(&self) -> eyre::Result<()> {
        if self.cancel.load(Ordering::Relaxed) {
            bail!("tndb: compaction cancelled");
        }
        Ok(())
    }
}

/// Test-only hooks into the compaction thread.
#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct TestHooks {
    /// The next requested compaction runs whatever the log's size.
    pub(super) force: AtomicBool,
    /// The next compaction fails before it builds anything.
    pub(super) fail_next: AtomicBool,
    /// The next compaction's copy skips a row (so its switch finds a row count wrong).
    pub(super) skip_a_row: AtomicBool,
    /// The next compaction's new log is tracked by the disk model
    /// (`archive::data_file::disk_model`) from its creation, so its image holds only what the
    /// compaction itself synced.
    pub(super) track_new_log: AtomicBool,
    /// The free space the disk guards see.
    pub(super) free_space: Mutex<Option<u64>>,
    /// How many times a disk guard has run.
    pub(super) room_checks: std::sync::atomic::AtomicU64,
    /// The next switch panics just before its rename, or just after it.
    pub(super) panic_before_rename: AtomicBool,
    pub(super) panic_after_rename: AtomicBool,
    /// The next switch's rename into place fails (moving nothing), or moves the directory and
    /// then reports a failure, or its directory sync after the rename fails.
    pub(super) fail_rename: AtomicBool,
    pub(super) fail_after_rename: AtomicBool,
    pub(super) fail_root_sync: AtomicBool,
    /// The next compaction reports each checkpoint it reaches on the first channel and waits
    /// for a message (or the sender's drop) on the second. Checkpoints: 1 after the copy, 2 after
    /// each background round, 3 holding the registry lock before the write lock.
    pub(super) gate: Mutex<Option<Gate>>,
    /// Switch attempts that let go of their locks to catch up first.
    pub(super) switch_retries: std::sync::atomic::AtomicU64,
    /// The most unpublished pages a compacted index had when the last switch installed it (their
    /// CRCs are stamped under the write lock).
    pub(super) private_at_switch: std::sync::atomic::AtomicU64,
    /// The next automatic request pauses once past its lock-free check, as `gate` does (it
    /// reports checkpoint 0).
    pub(super) request_pause: Mutex<Option<Gate>>,
}

#[cfg(test)]
type Gate = (std::sync::mpsc::Sender<u32>, std::sync::mpsc::Receiver<()>);

#[cfg(test)]
impl TestHooks {
    /// Gate the next compaction (see `gate`).
    pub(super) fn gate_next(
        &self,
    ) -> (std::sync::mpsc::Receiver<u32>, std::sync::mpsc::Sender<()>) {
        let (arrived, arrivals) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        *self.gate.lock() = Some((arrived, released));
        (arrivals, release)
    }

    /// Pause the next automatic request (see `request_pause`).
    pub(super) fn pause_next_request(
        &self,
    ) -> (std::sync::mpsc::Receiver<u32>, std::sync::mpsc::Sender<()>) {
        let (arrived, arrivals) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        *self.request_pause.lock() = Some((arrived, released));
        (arrivals, release)
    }
}

/// A test checkpoint (see `TestHooks::gate`).
#[cfg(test)]
fn checkpoint(gate: &Option<Gate>, n: u32) {
    if let Some((arrived, release)) = gate {
        let _ = arrived.send(n);
        let _ = release.recv_timeout(Duration::from_secs(30));
    }
}

/// The free bytes on the database's filesystem (a test may set them), or `None` if unknown.
fn free_bytes(core: &Core) -> Option<u64> {
    #[cfg(test)]
    if let Some(free) = *core.compact.test.free_space.lock() {
        return Some(free);
    }
    available_bytes(&core.root).ok()
}

/// Stop a running compaction when the database closes (a cancel), or once its durability has
/// failed: a stopped writer never switches, so the rest of the copy would only add I/O to a disk
/// that may be failing.
fn check_stop(core: &Core) -> eyre::Result<()> {
    core.compact.check_cancel()?;
    core.durability.check().wrap_err("tndb: compaction: the database stopped")
}

/// Give way to the writer when the disk is nearly full (an unknown free space does not block).
fn check_room(core: &Core) -> eyre::Result<()> {
    #[cfg(test)]
    core.compact.test.room_checks.fetch_add(1, Ordering::Relaxed);
    match free_bytes(core) {
        Some(free) if free < MIN_FREE => Err(GiveWay::DiskFull(format!(
            "tndb: compaction abandoned: {free} bytes free, below {MIN_FREE}"
        ))
        .into()),
        _ => Ok(()),
    }
}

/// A compaction giving way to the writer for a cause other than damage, carried in its error so
/// the run loop counts it by cause (see [`Cause`]).
#[derive(Debug)]
enum GiveWay {
    /// Too little free disk.
    DiskFull(String),
    /// The writer outran every switch try.
    WriterOutran(String),
}

impl std::fmt::Display for GiveWay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DiskFull(why) | Self::WriterOutran(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for GiveWay {}

/// Why the compaction that failed with `e` was abandoned (a panic is counted by the run loop).
fn cause(e: &eyre::Report) -> Cause {
    match e.chain().find_map(|cause| cause.downcast_ref::<GiveWay>()) {
        Some(GiveWay::DiskFull(_)) => Cause::DiskFull,
        Some(GiveWay::WriterOutran(_)) => Cause::WriterOutran,
        None if is_corruption(e) => Cause::Corruption,
        None => Cause::Error,
    }
}

/// The bytes of every file under `dir` (a generation: its log and its indexes).
fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_bytes(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Whether a rename of `from` to `to` that returned an error took effect anyway. Only `from` still
/// there and `to` absent counts as not moved; anything else (a path that cannot be checked
/// included) counts as moved, so a doubt stops the writer rather than letting it carry on in a
/// generation the next open may pass over.
fn moved(from: &Path, to: &Path) -> bool {
    !matches!((from.try_exists(), to.try_exists()), (Ok(true), Ok(false)))
}

/// Paces the copy at `rate` bytes a second (0: unpaced), checking for a stop ([`check_stop`]) and
/// for room on the disk (the copy is the longest phase) once per [`BATCH_BYTES`], and for a stop
/// during each sleep.
struct Pacer {
    rate: u64,
    written: u64,
    next_check: u64,
    started: Instant,
}

impl Pacer {
    fn new(rate: u64) -> Self {
        Self { rate, written: 0, next_check: BATCH_BYTES, started: Instant::now() }
    }

    fn pace(&mut self, bytes: u64, core: &Core) -> eyre::Result<()> {
        self.written += bytes;
        if self.written < self.next_check {
            return Ok(());
        }
        self.next_check = self.written + BATCH_BYTES;
        check_stop(core)?;
        check_room(core)?;
        if self.rate > 0 {
            let due = Duration::from_secs_f64(self.written as f64 / self.rate as f64);
            let mut ahead = due.saturating_sub(self.started.elapsed());
            while !ahead.is_zero() {
                let slice = ahead.min(PACE_SLICE);
                std::thread::sleep(slice);
                ahead -= slice;
                check_stop(core)?;
            }
        }
        Ok(())
    }
}

/// The page size [`Builder::write_behind`] rounds its ranges down to.
const WRITE_BEHIND_PAGE: u64 = 4096;

/// The generation being built. Unless it is handed over ([`Self::finish`]), its files and
/// directory are deleted when it drops, so a cancelled or failed compaction leaves nothing.
struct Builder {
    /// Emptied once handed over.
    dir: PathBuf,
    log: Option<Log>,
    header: DataHeader,
    idxs: BTreeMap<TableId, BtreeIndex>,
    /// The catch-up made a row dead (overwritten, removed or cleared).
    dead: bool,
    /// A duplicate of the new log's descriptor (Linux: for `sync_file_range`), and where the
    /// bytes not yet handed to writeback begin (see [`Self::write_behind`]).
    #[cfg(target_os = "linux")]
    file: fs::File,
    written_back: u64,
}

impl Builder {
    fn new(dir: PathBuf) -> eyre::Result<Self> {
        fs::create_dir(&dir).wrap_err_with(|| format!("tndb: create {}", dir.display()))?;
        let log = match open_log(&dir) {
            Ok(log) => log,
            Err(e) => {
                let _ = fs::remove_dir_all(&dir);
                return Err(e);
            }
        };
        #[cfg(target_os = "linux")]
        let file = match log.dup_file() {
            Ok(file) => file,
            Err(e) => {
                drop(log);
                let _ = fs::remove_dir_all(&dir);
                return Err(e.into());
            }
        };
        let builder = Self {
            dir,
            header: *log.header(),
            written_back: log.file_len(),
            log: Some(log),
            idxs: BTreeMap::new(),
            dead: false,
            #[cfg(target_os = "linux")]
            file,
        };
        // The log's entry, durable before anything relies on it (dropped on failure, the
        // builder deletes its directory).
        sync_dir(&builder.dir)?;
        Ok(builder)
    }

    fn log(&mut self) -> &mut Log {
        self.log.as_mut().expect("a builder holds its log until it finishes")
    }

    fn log_len(&self) -> u64 {
        self.log.as_ref().map_or(0, Log::file_len)
    }

    /// Table `id`'s new index, created (empty) if it has none yet.
    fn index(&mut self, id: TableId, ksize: u16) -> eyre::Result<&mut BtreeIndex> {
        let Self { dir, header, idxs, .. } = self;
        Ok(match idxs.entry(id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                entry.insert(BtreeIndex::open_btx_file(idx_dir(dir, id), header, ksize, false)?)
            }
        })
    }

    /// Rows in table `id`'s new index.
    fn rows(&self, id: TableId) -> usize {
        self.idxs.get(&id).map_or(0, BtreeIndex::len)
    }

    /// Copy one live row: its stored put record, flagged as a whole batch, indexed under `key`.
    fn copy_row(
        &mut self,
        id: TableId,
        ksize: u16,
        key: &[u8],
        payload: &[u8],
    ) -> eyre::Result<()> {
        let pos = self.log().append_raw_parts(&[&[payload[0] | BATCH_END], &payload[1..]])?;
        let idx = self.index(id, ksize)?;
        let rows = idx.len();
        idx.save(key, pos)?;
        // An overwrite of a row copied earlier leaves that copy dead.
        self.dead |= idx.len() == rows;
        self.write_behind();
        Ok(())
    }

    /// Once [`BATCH_BYTES`] have been appended since the last call, start writing them back to
    /// disk without waiting for it, so the compaction's output never piles up unflushed: on Linux
    /// a pile of dirty pages throttles every thread that writes, the database's writers included.
    /// On Linux this is `sync_file_range(SYNC_FILE_RANGE_WRITE)`, which neither waits nor flushes
    /// the drive's cache; elsewhere there is no such call and the pages wait for the round's sync.
    /// A hint: the round syncs remain the durability points, and an I/O error surfaces there.
    fn write_behind(&mut self) {
        // Whole pages only: the last, partly written one would be written again once filled.
        let end = self.log_len() & !(WRITE_BEHIND_PAGE - 1);
        if end.saturating_sub(self.written_back) < BATCH_BYTES {
            return;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd as _;
            let (from, len) = (self.written_back, end - self.written_back);
            if let (Ok(from), Ok(len)) = (i64::try_from(from), i64::try_from(len)) {
                // SAFETY: `sync_file_range` on a descriptor this builder owns and keeps open for
                // the call; it touches no Rust memory.
                let _ = unsafe {
                    libc::sync_file_range(
                        self.file.as_raw_fd(),
                        from,
                        len,
                        libc::SYNC_FILE_RANGE_WRITE,
                    )
                };
            }
        }
        #[cfg(test)]
        super::stats::WRITEBACKS.fetch_add(1, Ordering::Relaxed);
        self.written_back = end;
    }

    /// Remove `key` from table `id`: a removal record, as its own batch, if the new log holds the
    /// row (otherwise the new log never had it, and needs nothing).
    fn remove(&mut self, id: TableId, key: &[u8]) -> eyre::Result<()> {
        let Some(idx) = self.idxs.get_mut(&id) else { return Ok(()) };
        if idx.contains(key)? {
            self.dead = true;
            let log = self.log.as_mut().expect("a builder holds its log until it finishes");
            log.append_raw_parts(&[&record::head(KIND_REMOVE, id, true), key])?;
            idx.remove(key)?;
        }
        Ok(())
    }

    /// Clear table `id`: a clear record, as its own batch, if the new log holds rows of it.
    fn clear(&mut self, id: TableId) -> eyre::Result<()> {
        let Some(idx) = self.idxs.get_mut(&id) else { return Ok(()) };
        if !idx.is_empty() {
            self.dead = true;
            let log = self.log.as_mut().expect("a builder holds its log until it finishes");
            log.append_raw_parts(&[&record::head(KIND_CLEAR, id, true)])?;
            idx.clear()?;
        }
        Ok(())
    }

    /// Make the new log durable up to its end, and stamp its commit marker. Its indexes are
    /// derived: synced only by a clean close, as any generation's are. They are published here
    /// (their pages CRC-stamped) so the switch, under the write lock, stamps only the pages its
    /// own tail writes.
    fn sync(&mut self) -> eyre::Result<()> {
        let log = self.log();
        log.commit()?;
        log.stamp_commit_marker();
        for idx in self.idxs.values_mut() {
            drop(idx.publish());
        }
        Ok(())
    }

    /// Hand the files over (no longer deleted on drop).
    fn finish(mut self) -> (Log, BTreeMap<TableId, BtreeIndex>, bool) {
        self.dir = PathBuf::new();
        let log = self.log.take().expect("a builder holds its log until it finishes");
        (log, std::mem::take(&mut self.idxs), self.dead)
    }
}

impl Drop for Builder {
    fn drop(&mut self) {
        if let Some(log) = self.log.as_mut() {
            log.set_remove_on_drop();
        }
        for idx in self.idxs.values_mut() {
            idx.set_remove_on_drop();
        }
        self.log = None;
        self.idxs.clear();
        if !self.dir.as_os_str().is_empty() {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

/// Key sizes for the catch-up's records: the snapshot's, or, for a table whose key size was not
/// fixed at the snapshot (or that is newer), the live one. A 0 is never cached: a table's first
/// row can land at any time.
struct Ksizes<'a> {
    known: RefCell<BTreeMap<TableId, u16>>,
    core: &'a Core,
}

impl<'a> Ksizes<'a> {
    fn new(core: &'a Core, tables: &[SnapTable]) -> Self {
        let known = tables.iter().filter(|t| t.ksize > 0).map(|t| (t.id, t.ksize)).collect();
        Self { known: RefCell::new(known), core }
    }

    fn get(&self, id: TableId) -> Option<u16> {
        if let Some(&ksize) = self.known.borrow().get(&id) {
            return Some(ksize);
        }
        let ksize = self.core.table_ksize(id)?;
        if ksize > 0 {
            self.known.borrow_mut().insert(id, ksize);
        }
        Some(ksize)
    }
}

/// A key's last write in a stretch of the old log.
enum Net {
    /// A put: the record's position in the old log.
    Put(u64),
    Remove,
}

/// Bring `out` up to date with the old log's batches in `[from, to)` (whole batches: both ends
/// lie between batches): their net effect, in key order: each table they cleared cleared, then
/// each touched key's last write.
///
/// # Safety
///
/// [`MapView::tail_within`]'s, for `[from, to)` of the log `view` maps: the writer appended every
/// byte of it in full before the call (`to` is a frontier it stored after a batch, or its end
/// read under its lock), and writes and cuts none of it while the call runs.
unsafe fn catch_up(
    out: &mut Builder,
    view: &MapView,
    from: u64,
    to: u64,
    ksizes: &Ksizes<'_>,
    guard: Option<&Core>,
) -> eyre::Result<()> {
    // A round can be large (the first replays everything written during the copy), so outside
    // the write lock it checks for a stop as it reads, and for a stop and room on the disk as it
    // copies, as the copy does (unpaced): a writer's growth must not be what finds the disk full.
    let mut pacer = guard.map(|core| (Pacer::new(0), core));
    let mut cleared = BTreeSet::new();
    let mut net: BTreeMap<(TableId, Vec<u8>), Net> = BTreeMap::new();
    let mut pos = from;
    let mut next_check = from + BATCH_BYTES;
    while pos < to {
        if let Some(core) = guard.filter(|_| pos >= next_check) {
            check_stop(core)?;
            next_check = pos + BATCH_BYTES;
        }
        // SAFETY: `pos < to`, and `[from, to)` meets `tail_within`'s contract (this fn's own).
        let payload = unsafe { Log::record_bytes_within(view, pos, to, MAX_RECORD)? };
        let record = record::parse(payload, |id| ksizes.get(id)).map_err(|e| {
            DamagedRecord(format!("tndb: compaction: the record at {pos} does not parse: {e:#}"))
        })?;
        let id = record.table;
        match record.op {
            Op::Put(key) => {
                net.insert((id, key.to_vec()), Net::Put(pos));
            }
            Op::Remove(key) => {
                net.insert((id, key.to_vec()), Net::Remove);
            }
            Op::Clear => {
                cleared.insert(id);
                net.retain(|(table, _), _| *table != id);
            }
        }
        pos += payload.len() as u64 + FRAME;
    }
    for id in cleared {
        out.clear(id)?;
    }
    for ((id, key), last) in net {
        match last {
            Net::Put(pos) => {
                // SAFETY: as above: `pos` is a record this call read below `to`.
                let payload = unsafe { Log::record_bytes_within(view, pos, to, MAX_RECORD)? };
                let ksize = ksizes.get(id).unwrap_or(0);
                out.copy_row(id, ksize, &key, payload)?;
                if let Some((pacer, core)) = pacer.as_mut() {
                    pacer.pace(payload.len() as u64 + FRAME, core)?;
                }
            }
            Net::Remove => out.remove(id, &key)?,
        }
    }
    Ok(())
}

/// One compaction: `Ok(true)` if the database switched to a compacted generation, `Ok(false)` if
/// none was due (or the writer had stopped).
fn compact_once(core: &Core, requested: bool, rate: u64) -> eyre::Result<bool> {
    #[cfg(test)]
    let shared = &core.compact;
    #[cfg(test)]
    let gate = shared.test.gate.lock().take();
    #[cfg(test)]
    if shared.test.fail_next.swap(false, Ordering::Relaxed) {
        bail!("tndb: injected compaction failure");
    }
    #[cfg(test)]
    let min =
        if shared.test.force.swap(false, Ordering::Relaxed) { 0 } else { REQUESTED_MIN_BYTES };
    #[cfg(not(test))]
    let min = REQUESTED_MIN_BYTES;
    let Some(snap) = core.compaction_snapshot(requested.then_some(min)) else { return Ok(false) };
    #[cfg(test)]
    let mut skip_a_row = shared.test.skip_a_row.swap(false, Ordering::Relaxed);
    // Room for the new generation, and the floor the copy keeps above it: a compaction admitted
    // with less would copy almost everything and then give way (`check_room`), every backoff. The
    // current generation's size bounds the copy; when every table is empty at the snapshot (the
    // epoch tables at an epoch's end) there is nothing to copy, and the catch-up checks for room
    // as it writes. Counting the old log there would refuse the one compaction that frees it.
    let empty =
        snap.tables.iter().all(|table| table.published.index.as_ref().is_none_or(|i| i.len() == 0));
    let copy = if empty { 0 } else { dir_bytes(&snap.gen_dir) };
    let needed = copy + DISK_HEADROOM + MIN_FREE;
    if let Some(free) = free_bytes(core).filter(|&free| free < needed) {
        let why = format!("tndb: compaction skipped: {free} bytes free, {needed} needed");
        return Err(GiveWay::DiskFull(why).into());
    }
    // Keeps the old generation's files open (and so its view valid) until this returns.
    let _alive = Arc::clone(&snap.alive);
    let view = Arc::clone(&snap.view);
    let ksizes = Ksizes::new(core, &snap.tables);
    let mut out = Builder::new(compact_dir(&core.root, snap.gen + 1))?;
    // Nothing is written to the new log yet but the header its open synced.
    #[cfg(test)]
    if shared.test.track_new_log.swap(false, Ordering::Relaxed) {
        crate::archive::data_file::disk_model::track(&out.dir.join("log"));
    }

    // 1. Every table's rows, in key order; each snapshot dropped once copied.
    let mut pacer = Pacer::new(rate);
    for table in snap.tables {
        let SnapTable { id, ksize, published } = table;
        let Some(index) = &published.index else { continue };
        let mut cursor = ScanKind::Forward.cursor(index)?;
        while let Some(item) = cursor.next(index) {
            let (key, pos) = item?;
            #[cfg(test)]
            if std::mem::take(&mut skip_a_row) {
                continue;
            }
            // SAFETY: as for a reader (`store::Published::value_at`): `limit` is the old log's
            // length at the publish this snapshot came from, written in full before it and never
            // rewritten or cut while `_alive` keeps the generation open.
            let payload = unsafe {
                Log::record_bytes_within(&published.view, pos, published.limit, MAX_RECORD)?
            };
            // A damaged index entry is abandoned on, not copied into the new generation.
            record::put_value(payload, id, key)?;
            out.copy_row(id, ksize, key, payload)?;
            pacer.pace(payload.len() as u64 + FRAME, core)?;
        }
    }
    #[cfg(test)]
    checkpoint(&gate, 1);

    // 2. What was written since, in rounds, each synced, until a round has little left (or the
    // rounds run out: a writer faster than the catch-up is caught up by the switch's last try).
    // Unpaced: paced below the writer's rate it would fall further behind every round.
    let mut done = snap.end;
    let mut rounds = 0;
    loop {
        check_stop(core)?;
        check_room(core)?;
        let to = core.frontier.load().end;
        // SAFETY: the frontier is stored (`Core::store_frontier`) under the write lock after a
        // batch was appended in full, so the `ArcSwap` load orders those appends before this read.
        // Only this thread switches generations, so it is still the old log's. The writer writes
        // past it only: a failed batch rewinds to its own start, at or past the frontier.
        unsafe { catch_up(&mut out, &view, done, to, &ksizes, Some(core))? };
        let behind = to - done;
        done = to;
        out.sync()?;
        #[cfg(test)]
        checkpoint(&gate, 2);
        if behind <= CATCHUP_BYTES {
            break;
        }
        rounds += 1;
        if rounds >= MAX_CATCHUP_ROUNDS {
            break;
        }
    }

    // 3. The switch: the registry lock (so no table is registered, nor its key size fixed,
    // until it is done), then the write lock for the last few batches.
    for attempt in 1..=SWITCH_TRIES {
        let registry = core.registry.lock();
        for (id, ksize) in core.table_ksizes() {
            if ksize > 0 {
                // An empty table's index too, so the next clean open rebuilds nothing.
                out.index(id, ksize)?;
            }
        }
        check_stop(core)?;
        check_room(core)?;
        let to = core.frontier.load().end;
        // SAFETY: the frontier is stored (`Core::store_frontier`) under the write lock after a
        // batch was appended in full, so the `ArcSwap` load orders those appends before this read.
        // Only this thread switches generations, so it is still the old log's. The writer writes
        // past it only: a failed batch rewinds to its own start, at or past the frontier.
        unsafe { catch_up(&mut out, &view, done, to, &ksizes, Some(core))? };
        done = to;
        out.sync()?;
        // Before the final tail below: written under the write lock, it would add a file sync to
        // the switch's stall (see `COMPACTED`).
        let base = out.log_len();
        write_base(&out.dir, base)?;
        #[cfg(test)]
        checkpoint(&gate, 3);

        let mut writer = core.writer.lock();
        let held = Instant::now();
        writer.check()?;
        debug_assert_eq!(writer.gen(), snap.gen, "only the compaction thread switches");
        let end = writer.log_len();
        let cap = if attempt == SWITCH_TRIES { STALL_TAIL_MAX } else { SWITCH_TAIL_MAX };
        if end - done > cap {
            // Too much landed meanwhile: catch up without the locks and try again.
            drop(writer);
            drop(registry);
            #[cfg(test)]
            shared.test.switch_retries.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        // Under the write lock, and at most a tail's worth (the cap above): no checks.
        // SAFETY: `end` is the writer's log length read under its lock, held across this call:
        // every append below it is complete and ordered by the lock, and none can start.
        unsafe { catch_up(&mut out, &view, done, end, &ksizes, None)? };
        for (id, rows) in writer.table_rows() {
            if out.rows(id) != rows {
                bail!(
                    "tndb: compaction: table {id} has {} rows compacted, {rows} live",
                    out.rows(id)
                );
            }
        }
        out.sync()?;
        let log = out.log.as_ref().expect("a builder holds its log until it finishes");
        let new_view = log.view();
        let alive = Arc::new(GenAlive::default());
        let sync = Arc::new(GenSync::new(log, &new_view, &alive)?);
        #[cfg(test)]
        if shared.test.panic_before_rename.swap(false, Ordering::Relaxed) {
            panic!("injected panic before the rename");
        }
        let gen = snap.gen + 1;
        let dir = gen_dir(&core.root, gen);
        #[cfg(test)]
        let renamed = if shared.test.fail_rename.swap(false, Ordering::Relaxed) {
            Err(std::io::Error::other("injected rename failure"))
        } else if shared.test.fail_after_rename.swap(false, Ordering::Relaxed) {
            fs::rename(&out.dir, &dir)
                .and(Err(std::io::Error::other("injected failure after the rename")))
        } else {
            fs::rename(&out.dir, &dir)
        };
        #[cfg(not(test))]
        let renamed = fs::rename(&out.dir, &dir);
        if let Err(e) = renamed {
            if !moved(&out.dir, &dir) {
                return Err(eyre::Report::new(e).wrap_err("tndb: compaction: rename into place"));
            }
            // A failing disk can apply a rename and still report an error. The next open may then
            // pick the new generation, so no batch may be acknowledged that it lacks: stop the
            // writer. The new log holds every batch so far (synced above, with the write lock
            // held throughout), so whichever generation the next open picks holds every
            // acknowledged write. The builder's drop deletes only what is left at its own paths
            // (nothing, once moved): the new generation stays unsealed, and the next open
            // recovers it.
            let cause = format!(
                "the compacted generation {gen} moved into place, yet its rename failed: {e}"
            );
            writer.stop(cause.clone());
            bail!("tndb: {cause}");
        }
        let (log, idxs, has_dead) = out.finish();
        #[cfg(test)]
        shared.test.private_at_switch.store(
            idxs.values().map(|idx| idx.private_pages() as u64).max().unwrap_or(0),
            Ordering::Relaxed,
        );
        // From here the next open may pick the new generation: a failure stops the writer.
        let _latch =
            UnwindLatch(&core.durability, "a compaction's switch panicked after its rename");
        #[cfg(test)]
        if shared.test.panic_after_rename.swap(false, Ordering::Relaxed) {
            panic!("injected panic after the rename");
        }
        #[cfg(test)]
        let root_synced = if shared.test.fail_root_sync.swap(false, Ordering::Relaxed) {
            Err(std::io::Error::other("injected directory sync failure"))
        } else {
            sync_dir(&core.root)
        };
        #[cfg(not(test))]
        let root_synced = sync_dir(&core.root);
        if let Err(e) = root_synced {
            let cause = format!("sync the compacted generation {gen}: {e}");
            writer.stop(cause.clone());
            bail!("tndb: {cause}");
        }
        let old_dir = writer.install_gen(NewGen {
            gen,
            dir,
            log,
            idxs,
            view: new_view,
            alive,
            sync,
            has_dead,
            base,
        });
        core.store_frontier(&writer);
        let held = held.elapsed();
        let log_len = writer.log_len();
        drop(writer);
        core.metrics.switched(gen, log_len, held);
        #[cfg(test)]
        super::stats::switch(held);
        #[cfg(test)]
        super::stats::log(core.stat_slot).switch(held);
        drop(registry);
        remove_in_background(old_dir, "tndb-reap");
        return Ok(true);
    }
    Err(GiveWay::WriterOutran(format!(
        "tndb: compaction abandoned: the writer outran {SWITCH_TRIES} switches (the last over \
         {STALL_TAIL_MAX} bytes)"
    ))
    .into())
}

/// The `tndb-compact` thread: waits for a request (the automatic trigger, or
/// [`tn_types::Database::compact`]), then compacts. Stopped (cancelling a running compaction)
/// and joined when dropped.
#[derive(Debug)]
pub(super) struct Compactor {
    core: Arc<Core>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Compactor {
    pub(super) fn start(core: Arc<Core>, rate: u64) -> eyre::Result<Self> {
        let thread_core = Arc::clone(&core);
        let handle = std::thread::Builder::new()
            .name("tndb-compact".to_string())
            .spawn(move || run(&thread_core, rate))?;
        Ok(Self { core, handle: Mutex::new(Some(handle)) })
    }

    /// Stop the thread, cancelling a running compaction (its files are deleted), and wait for it.
    pub(super) fn stop(&self) {
        let shared = &self.core.compact;
        {
            let mut state = shared.state.lock();
            state.stop = true;
            shared.cancel.store(true, Ordering::Relaxed);
            shared.wake.notify_one();
        }
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Compactor {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run(core: &Core, rate: u64) {
    let shared = &core.compact;
    loop {
        let requested = {
            let mut state = shared.state.lock();
            while !state.stop && !state.wanted && !state.requested {
                shared.wake.wait(&mut state);
            }
            if state.stop {
                return;
            }
            state.wanted = false;
            shared.running.store(true, Ordering::Release);
            std::mem::take(&mut state.requested)
        };
        let started = Instant::now();
        let outcome = catch_unwind(AssertUnwindSafe(|| compact_once(core, requested, rate)));
        let took = started.elapsed();
        let (last, ended) = match outcome {
            Ok(Ok(true)) => (Ok(true), Outcome::Switched),
            Ok(Ok(false)) => (Ok(false), Outcome::Skipped),
            Ok(Err(e)) => {
                let ended = if shared.cancel.load(Ordering::Relaxed) {
                    tracing::debug!(target: "tndb", "compaction cancelled: {e:#}");
                    Outcome::Cancelled
                } else {
                    let cause = cause(&e);
                    if cause == Cause::Corruption {
                        tracing::error!(target: "tndb", "compaction abandoned: {e:#}");
                        core.compaction_damaged();
                    } else {
                        tracing::warn!(target: "tndb", "compaction abandoned: {e:#}");
                    }
                    Outcome::Abandoned(cause)
                };
                (Err(format!("{e:#}")), ended)
            }
            Err(_) => {
                tracing::error!(target: "tndb", "the compaction thread panicked; abandoned");
                (Err("panicked".to_string()), Outcome::Abandoned(Cause::Panic))
            }
        };
        // Recorded before the compaction counts as done, so a waiter sees it counted.
        core.metrics.compaction_ended(ended, took);
        #[cfg(test)]
        if let Err(cause) = &last {
            let cancelled = shared.cancel.load(Ordering::Relaxed);
            super::stats::ended(cause, cancelled);
            super::stats::log(core.stat_slot).ended(cancelled);
        }
        let mut state = shared.state.lock();
        match &last {
            Ok(true) => (state.retry_at, state.backoff) = (None, AUTO_BACKOFF_MIN),
            Ok(false) => {}
            Err(_) => {
                state.retry_at = Some(Instant::now() + state.backoff);
                state.backoff = (state.backoff * 2).min(AUTO_BACKOFF_MAX);
            }
        }
        state.done += 1;
        #[cfg(test)]
        state.outcomes.push(last.clone());
        state.last = Some(last);
        shared.running.store(false, Ordering::Release);
        shared.finished.notify_all();
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;
    use tn_types::Database as _;

    use super::{
        super::database::{CommitMode, CompactionConfig, TnDatabase, TnDbOptions},
        *,
    };
    use crate::{archive::pack::DATA_HEADER_BYTES, test::TestTable};

    /// An abandon's cause comes from the typed error wherever it sits in the chain, then from
    /// damage, else it is a plain error.
    #[test]
    fn test_tndb_compaction_abandon_causes() {
        let disk = eyre::Report::new(GiveWay::DiskFull("no room".into()));
        assert_eq!(cause(&disk), Cause::DiskFull);
        let outran =
            eyre::Report::new(GiveWay::WriterOutran("outran".into())).wrap_err("compaction");
        assert_eq!(cause(&outran), Cause::WriterOutran);
        let damaged = eyre::Report::new(FetchError::CrcFailed).wrap_err("copy a row");
        assert_eq!(cause(&damaged), Cause::Corruption);
        assert_eq!(cause(&eyre::eyre!("an I/O error")), Cause::Error);
    }

    /// A second table, so that emptying the database takes more than one clear.
    #[derive(Debug)]
    struct CompactTable;
    impl tn_types::Table for CompactTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "CompactTable";
        const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
    }

    /// The database at `path`, both test tables open, compacting as `compaction` says.
    fn open(path: &Path, compaction: CompactionConfig) -> TnDatabase {
        let options = TnDbOptions { commit: CommitMode::Group, compaction };
        let db = TnDatabase::open_with(path, options).expect("open");
        db.open_table::<TestTable>().expect("open table");
        db.open_table::<CompactTable>().expect("open table");
        db
    }

    /// No automatic compaction, and an unpaced copy.
    fn manual() -> CompactionConfig {
        CompactionConfig { auto_min_bytes: None, bytes_per_sec: 0 }
    }

    fn log_len(db: &TnDatabase) -> u64 {
        db.core().writer.lock().log_len()
    }

    /// The unfinished compactions under `root`.
    fn compact_dirs(root: &Path) -> usize {
        fs::read_dir(root)
            .expect("read root")
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("compact-"))
            .count()
    }

    /// Wait (up to 10 s) for a compaction to start building.
    fn wait_for_a_build(root: &Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while compact_dirs(root) == 0 {
            assert!(Instant::now() < deadline, "the compaction started");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// A test's `compact_now` waits a bounded time: a compaction that does not finish (here held
    /// at a gate) fails the test with a panic naming it instead of hanging it.
    #[test]
    fn test_tndb_compact_now_panics_past_its_timeout() {
        let tmp = TempDir::with_prefix("tndb_compact_now_timeout").expect("temp dir");
        let db = open(tmp.path(), manual());
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
        let compact = &db.core().compact;
        let (arrivals, release) = compact.test.gate_next();
        std::thread::scope(|s| {
            let waiter = s.spawn(|| compact.compact_now_within(Duration::from_millis(200)));
            assert_eq!(
                arrivals.recv_timeout(Duration::from_secs(20)),
                Ok(1),
                "held after the copy"
            );
            let panicked = waiter.join().expect_err("the wait times out while the gate holds it");
            let message = panicked.downcast_ref::<String>().cloned().unwrap_or_default();
            assert!(message.contains("did not finish within"), "the panic names it: {message}");
            drop(release);
        });
        assert!(compact.wait_done(1).is_some(), "the released compaction finishes");
    }

    /// The epoch-end reset (every table emptied) needs no room for a copy: with free space above
    /// the floor a compaction keeps but below that plus the old generation, the reset is admitted
    /// and leaves a log of its header alone, while a compaction with rows to copy is still
    /// refused.
    #[test]
    fn test_tndb_compaction_emptied_reset_needs_no_room_for_a_copy() {
        let tmp = TempDir::with_prefix("tndb_compact_emptied_room").expect("temp dir");
        // Only the emptied trigger fires automatically.
        let config = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
        let db = open(tmp.path(), config);
        let value = "v".repeat(64 << 10);
        let fill = || {
            for i in 0..20_u64 {
                db.insert::<TestTable>(&i, &value).expect("insert");
                db.insert::<CompactTable>(&i, &value).expect("insert");
            }
        };
        fill();
        *db.core().compact.test.free_space.lock() = Some(DISK_HEADROOM + MIN_FREE + (1 << 20));
        db.clear_table::<TestTable>().expect("clear");
        db.clear_table::<CompactTable>().expect("clear");
        assert_eq!(db.core().compact.wait_done(1), Some(Ok(true)), "the reset ran");
        assert_eq!(log_len(&db), DATA_HEADER_BYTES as u64, "nothing copied");
        fill();
        let refused = db.compact_now().expect_err("rows to copy, and too little room");
        assert!(refused.contains("skipped"), "{refused}");
    }

    /// A latched durability failure stops a running compaction promptly, whether it is copying
    /// or about to run a catch-up round: it ends without a switch and leaves nothing.
    #[test]
    fn test_tndb_compaction_stops_once_durability_fails() {
        // Mid-copy: about 8 s of copying at 1 MiB/s.
        let tmp = TempDir::with_prefix("tndb_compact_latch_copy").expect("temp dir");
        let db =
            open(tmp.path(), CompactionConfig { auto_min_bytes: None, bytes_per_sec: 1 << 20 });
        let value = "v".repeat(4096);
        for i in 0..2_048_u64 {
            db.insert::<TestTable>(&i, &value).expect("insert");
        }
        db.compact().expect("request");
        wait_for_a_build(tmp.path());
        let latched = Instant::now();
        db.fail_durability_for_test();
        let ended = db.core().compact.wait_done(1).expect("the compaction ended");
        assert!(latched.elapsed() < Duration::from_secs(2), "stopped in {:?}", latched.elapsed());
        let stopped = ended.expect_err("no switch");
        assert!(stopped.contains("the database stopped"), "{stopped}");
        assert_eq!(compact_dirs(tmp.path()), 0, "nothing left");
        assert_eq!(super::super::layout::list_gens(tmp.path()).expect("gens"), [0]);
        drop(db);

        // After the copy, before the first catch-up round: no round runs.
        let tmp = TempDir::with_prefix("tndb_compact_latch_round").expect("temp dir");
        let db = open(tmp.path(), manual());
        for i in 0..100_u64 {
            db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
        }
        let (arrivals, release) = db.core().compact.test.gate_next();
        std::thread::scope(|s| {
            let compaction = s.spawn(|| db.compact_now());
            assert_eq!(arrivals.recv_timeout(Duration::from_secs(30)), Ok(1), "the copy's end");
            db.fail_durability_for_test();
            drop(release);
            let stopped = compaction.join().expect("compaction thread").expect_err("no switch");
            assert!(stopped.contains("the database stopped"), "{stopped}");
        });
        assert!(arrivals.try_recv().is_err(), "no catch-up round ran");
        assert_eq!(compact_dirs(tmp.path()), 0, "nothing left");
        assert_eq!(super::super::layout::list_gens(tmp.path()).expect("gens"), [0]);
    }

    /// A rename into place that reports an error yet moved the directory stops the writer, as a
    /// failure after the rename does: the next open picks the new generation, which holds every
    /// row. (A rename that moved nothing is abandoned, the database carrying on:
    /// `test_tndb_compaction_failures_before_the_rename_abandon`.)
    #[test]
    fn test_tndb_compaction_rename_that_moved_despite_an_error_stops_the_writer() {
        let tmp = TempDir::with_prefix("tndb_compact_rename_moved").expect("temp dir");
        let db = open(tmp.path(), manual());
        for i in 0..100_u64 {
            db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
        }
        db.core().compact.test.fail_after_rename.store(true, Ordering::Relaxed);
        let failed = db.compact_now().expect_err("the rename reported an error");
        assert!(failed.contains("moved into place"), "{failed}");
        // Carrying on in the old generation would acknowledge writes the next open never sees.
        assert!(db.insert::<TestTable>(&200, &"x".to_string()).is_err(), "the writer stopped");
        assert_eq!(compact_dirs(tmp.path()), 0);
        drop(db);
        let db = open(tmp.path(), manual());
        assert_eq!(db.core().writer.lock().gen(), 1, "the next open picks the new generation");
        assert_eq!(db.iter::<TestTable>().count(), 100, "every row");
    }

    /// A catch-up round stops for a stop (here a close's cancel) while it reads its records,
    /// before it copies any; and a record that reads back whole but does not parse is counted as
    /// corruption.
    #[test]
    fn test_tndb_compaction_catch_up_reads_check_for_a_stop_and_damage() {
        let tmp = TempDir::with_prefix("tndb_compact_catch_up_reads").expect("temp dir");
        let db = open(&tmp.path().join("db"), manual());
        db.insert::<TestTable>(&0, &"fixes the key size".to_string()).expect("insert");
        let core: &Core = db.core();
        let (id, ksize) =
            core.table_ksizes().into_iter().find(|&(_, ksize)| ksize > 0).expect("a key size");
        assert_eq!(ksize, 8);
        // An old log: 1.2 MiB of rows, then a record of a table the database does not have.
        let old_dir = tmp.path().join("old");
        fs::create_dir(&old_dir).expect("mkdir");
        let mut old = open_log(&old_dir).expect("old log");
        let from = old.file_len();
        let value = [7_u8; 4096];
        for k in 0..300_u64 {
            let head = record::head(record::KIND_PUT, id, true);
            old.append_raw_parts(&[&head, &k.to_be_bytes(), &value]).expect("append");
        }
        let to = old.file_len();
        let head = record::head(record::KIND_PUT, TableId::MAX, true);
        old.append_raw_parts(&[&head, &0_u64.to_be_bytes(), &value]).expect("append");
        let end = old.file_len();
        let view = old.view();
        let ksizes = Ksizes::new(core, &[]);
        let catch_up_into = |name: &str, from, to| {
            let mut out = Builder::new(tmp.path().join(name)).expect("builder");
            // SAFETY: the test's own appends end at `end`, before this call, and the log is
            // written no more while the view is read.
            let result = unsafe { catch_up(&mut out, &view, from, to, &ksizes, Some(core)) };
            (result, out.rows(id))
        };

        let (result, rows) = catch_up_into("all", from, to);
        result.expect("caught up");
        assert_eq!(rows, 300);
        core.compact.cancel.store(true, Ordering::Relaxed);
        let (result, rows) = catch_up_into("cancelled", from, to);
        core.compact.cancel.store(false, Ordering::Relaxed);
        let cancelled = result.expect_err("cancelled");
        assert!(format!("{cancelled:#}").contains("cancelled"), "{cancelled:#}");
        assert_eq!(rows, 0, "stopped while reading, before copying a row");
        let (result, _) = catch_up_into("damaged", to, end);
        let damaged = result.expect_err("damaged");
        assert_eq!(cause(&damaged), Cause::Corruption, "{damaged:#}");
    }
}
