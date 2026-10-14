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
//! after it stops the writer, since which generation the next open picks is then unknown.
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

/// After a failed automatic compaction, the automatic trigger waits this long before trying again,
/// doubling with each further failure up to [`AUTO_BACKOFF_MAX`].
const AUTO_BACKOFF_MIN: Duration = Duration::from_secs(60);
const AUTO_BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);

/// True when `e` comes from reading damaged committed data (a failed CRC, a corrupt record or
/// index), rather than from the environment.
fn is_corruption(e: &eyre::Report) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<FetchError>(),
            Some(FetchError::CrcFailed | FetchError::CorruptIndex(_))
        )
    })
}

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

/// A generation's `compacted` file: the log's length when the compaction that made it switched.
const COMPACTED: &str = "compacted";
const COMPACTED_TMP: &str = "compacted.tmp";

/// A record's frame beyond its payload: its length and CRC.
pub(super) const FRAME: u64 = 8;

/// The log's length right after the compaction that made the generation at `gen_path`, or 0 when
/// none did (or its `compacted` file is unreadable: it is only the trigger's starting point).
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
    /// why it was abandoned.
    #[cfg(test)]
    pub(super) fn compact_now(&self) -> Result<bool, String> {
        self.test.force.store(true, Ordering::Relaxed);
        let mut state = self.state.lock();
        let target = state.done + 1 + u64::from(self.running.load(Ordering::Acquire));
        state.requested = true;
        self.wake.notify_one();
        while state.done < target {
            self.finished.wait(&mut state);
        }
        state.last.clone().expect("a compaction finished")
    }

    /// Test-only: wait until `n` compactions have finished.
    #[cfg(test)]
    pub(super) fn wait_done(&self, n: u64) -> Option<Result<bool, String>> {
        let mut state = self.state.lock();
        while state.done < n {
            if self.finished.wait_for(&mut state, Duration::from_secs(30)).timed_out() {
                return None;
            }
        }
        state.last.clone()
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
    /// The free space the disk guards see.
    pub(super) free_space: Mutex<Option<u64>>,
    /// The next switch panics just before its rename, or just after it.
    pub(super) panic_before_rename: AtomicBool,
    pub(super) panic_after_rename: AtomicBool,
    /// The next switch's rename into place fails, or its directory sync after the rename.
    pub(super) fail_rename: AtomicBool,
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

/// Give way to the writer when the disk is nearly full (an unknown free space does not block).
fn check_room(core: &Core) -> eyre::Result<()> {
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

/// Paces the copy at `rate` bytes a second (0: unpaced), checking for a cancellation and for
/// room on the disk (the copy is the longest phase) once per [`BATCH_BYTES`], and for a
/// cancellation during each sleep.
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
        let shared = &core.compact;
        self.written += bytes;
        if self.written < self.next_check {
            return Ok(());
        }
        self.next_check = self.written + BATCH_BYTES;
        shared.check_cancel()?;
        check_room(core)?;
        if self.rate > 0 {
            let due = Duration::from_secs_f64(self.written as f64 / self.rate as f64);
            let mut ahead = due.saturating_sub(self.started.elapsed());
            while !ahead.is_zero() {
                let slice = ahead.min(PACE_SLICE);
                std::thread::sleep(slice);
                ahead -= slice;
                shared.check_cancel()?;
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
        self.dead = true;
        let Some(idx) = self.idxs.get_mut(&id) else { return Ok(()) };
        if idx.contains(key)? {
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
fn catch_up(
    out: &mut Builder,
    view: &MapView,
    from: u64,
    to: u64,
    ksizes: &Ksizes<'_>,
) -> eyre::Result<()> {
    let mut cleared = BTreeSet::new();
    let mut net: BTreeMap<(TableId, Vec<u8>), Net> = BTreeMap::new();
    let mut pos = from;
    while pos < to {
        let payload = Log::record_bytes_within(view, pos, to, MAX_RECORD)?;
        let record = record::parse(payload, |id| ksizes.get(id))
            .wrap_err_with(|| format!("tndb: compaction: the record at {pos}"))?;
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
                let payload = Log::record_bytes_within(view, pos, to, MAX_RECORD)?;
                let ksize = ksizes.get(id).unwrap_or(0);
                out.copy_row(id, ksize, &key, payload)?;
            }
            Net::Remove => out.remove(id, &key)?,
        }
    }
    Ok(())
}

/// One compaction: `Ok(true)` if the database switched to a compacted generation, `Ok(false)` if
/// none was due (or the writer had stopped).
fn compact_once(core: &Core, requested: bool, rate: u64) -> eyre::Result<bool> {
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
    let needed = dir_bytes(&snap.gen_dir) + DISK_HEADROOM;
    if let Some(free) = free_bytes(core).filter(|&free| free < needed) {
        let why = format!("tndb: compaction skipped: {free} bytes free, {needed} needed");
        return Err(GiveWay::DiskFull(why).into());
    }
    // Keeps the old generation's files open (and so its view valid) until this returns.
    let _alive = Arc::clone(&snap.alive);
    let view = Arc::clone(&snap.view);
    let ksizes = Ksizes::new(core, &snap.tables);
    let mut out = Builder::new(compact_dir(&core.root, snap.gen + 1))?;

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
            let payload =
                Log::record_bytes_within(&published.view, pos, published.limit, MAX_RECORD)?;
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
        shared.check_cancel()?;
        check_room(core)?;
        let to = core.frontier.load().end;
        catch_up(&mut out, &view, done, to, &ksizes)?;
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
        shared.check_cancel()?;
        check_room(core)?;
        let to = core.frontier.load().end;
        catch_up(&mut out, &view, done, to, &ksizes)?;
        done = to;
        out.sync()?;
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
        catch_up(&mut out, &view, done, end, &ksizes)?;
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
        if shared.test.fail_rename.swap(false, Ordering::Relaxed) {
            bail!("tndb: compaction: injected rename failure");
        }
        fs::rename(&out.dir, &dir).wrap_err("tndb: compaction: rename into place")?;
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
        state.last = Some(last);
        shared.running.store(false, Ordering::Release);
        shared.finished.notify_all();
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
