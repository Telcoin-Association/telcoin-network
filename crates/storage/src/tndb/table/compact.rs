//! Compaction: rewrite a table's live rows into a new generation, in the background.
//!
//! A compaction builds the table's next generation in a `compact-*` directory on a `tndb-compact`
//! thread ([`run`]):
//! 1. it copies the rows of a published (committed) snapshot, in key order — removed and
//!    overwritten records are never reached, so they are not copied;
//! 2. it catches up on what was committed since, in rounds, by replaying the old logs' committed
//!    tails ([`replay_delta`]), until a round has little left;
//! 3. it makes the new generation durable (a bulk sync, then one more round for what was committed
//!    during it and a small sync), and hands it back.
//!
//! It reads the old generation the way a reader does — a published snapshot and the logs' mapped
//! views — so it takes no lock, and the writer feeds it each publish's committed log lengths
//! ([`Compaction::publish`]). The copy is paced ([`CompactionConfig::bytes_per_sec`]) so it leaves
//! the disk to commits.
//!
//! The writer switches to the new generation at its first commit after the thread finishes: it
//! replays the last small tail under its lock, renames the directory into place (the switch), and
//! retires the old generation as a clear does. Both generations hold the same committed rows from
//! the moment the new one is renamed into place, so a crash on either side of the rename loses
//! nothing; a `compact-*` directory is never a generation, and an open deletes a leftover one.
//!
//! A compaction starts only with room for the new generation on disk; one that fails is dropped
//! and the automatic trigger backs off (see `Writer::back_off`).

use std::{
    ops::Range,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use eyre::bail;
use parking_lot::Mutex;

use super::{
    open_logs, sync_dir, BtreeIndex, GenAlive, GenFiles, KeyFn, KeyMode, Log, MapView, Pack,
    Published, ScanKind, TableMeta, COMMIT,
};
use crate::archive::error::fetch::FetchError;

/// When and how fast a table compacts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionConfig {
    /// Compact automatically once a table's data log is at least this many bytes and at least
    /// half of its puts are dead (overwritten or removed); `None` compacts only on request
    /// ([`tn_types::Database::compact`]).
    pub auto_min_bytes: Option<u64>,
    /// The pace of a compaction's copy, in bytes per second (0: unpaced).
    pub bytes_per_sec: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self { auto_min_bytes: Some(64 << 20), bytes_per_sec: 64 << 20 }
    }
}

/// A requested compaction ([`tn_types::Database::compact`]) skips a table whose data log is
/// smaller than this.
pub(super) const REQUESTED_MIN_BYTES: u64 = 1 << 20;

/// Bytes copied between pacing (and cancellation) checks.
const BATCH_BYTES: u64 = 1 << 20;

/// The background catch-up stops once a round has had at most this many bytes to replay, leaving
/// what accumulates after it to the writer's switch.
const CATCHUP_BYTES: u64 = 1 << 20;

/// Most background catch-up rounds before handing over to the writer regardless.
const MAX_CATCHUP_ROUNDS: usize = 8;

/// The longest a pacing sleep goes without checking for a cancellation.
const PACE_SLICE: Duration = Duration::from_millis(10);

/// Room a compaction needs beyond the current data log's length (the most its live rows can
/// take): the new log's growth preallocation, at most this much past its data.
pub(super) const DISK_HEADROOM: u64 = 128 << 20;

/// After a failed automatic compaction, the automatic trigger waits this long before trying again,
/// doubling with each further failure up to [`AUTO_BACKOFF_MAX`].
pub(super) const AUTO_BACKOFF_MIN: Duration = Duration::from_secs(60);
pub(super) const AUTO_BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);

/// Where a generation's committed records end, in its data and removal logs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Committed {
    pub(super) data: u64,
    pub(super) removed: u64,
}

/// Reads a log's records by position: the writer's own log, or a lock-free view of it.
pub(super) trait Records {
    /// The CRC-checked payload of the record at `pos`.
    fn record(&self, pos: u64) -> Result<&[u8], FetchError>;
}

impl Records for MapView {
    fn record(&self, pos: u64) -> Result<&[u8], FetchError> {
        Pack::<Vec<u8>>::record_bytes_in(self, pos)
    }
}

impl Records for Log {
    fn record(&self, pos: u64) -> Result<&[u8], FetchError> {
        self.record_bytes(pos)
    }
}

/// The generation being built. Unless it is handed over ([`Self::finish`]), its files and
/// directory are deleted when it drops, so a cancelled or failed compaction leaves nothing behind.
pub(super) struct Builder {
    files: Option<GenFiles>,
    dir: PathBuf,
    meta: TableMeta,
    key_fn: Option<KeyFn>,
    /// Checked between batches of the copy (`None` under the writer, which never cancels).
    cancel: Option<Arc<AtomicBool>>,
    rate: u64,
    /// Records written since the last commit record.
    uncommitted: bool,
    /// Puts made dead (overwritten or removed) while building.
    dead: u64,
    /// Pacing: bytes written, the next check, and when the copy started.
    written: u64,
    next_check: u64,
    started: Instant,
}

impl std::fmt::Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder").field("dir", &self.dir).field("rows", &self.len()).finish()
    }
}

impl Builder {
    fn new(
        dir: PathBuf,
        meta: TableMeta,
        key_fn: Option<KeyFn>,
        cancel: Arc<AtomicBool>,
        rate: u64,
    ) -> eyre::Result<Self> {
        std::fs::create_dir(&dir)?;
        let mut builder = Self {
            files: None,
            dir,
            meta,
            key_fn,
            cancel: Some(cancel),
            rate,
            uncommitted: false,
            dead: 0,
            written: 0,
            next_check: BATCH_BYTES,
            started: Instant::now(),
        };
        // Opened once the builder exists, so a failure still deletes the directory.
        let (data, removed) = open_logs(&builder.dir)?;
        builder.files = Some(GenFiles { data, removed, idx: None });
        Ok(builder)
    }

    fn files(&mut self) -> &mut GenFiles {
        self.files.as_mut().expect("a builder holds its files until it finishes")
    }

    /// Rows in the new generation.
    pub(super) fn len(&self) -> usize {
        self.files.as_ref().and_then(|files| files.idx.as_ref()).map_or(0, BtreeIndex::len)
    }

    /// Append a put's payload (unchanged) and index it under `key`.
    fn put(&mut self, key: &[u8], payload: &[u8]) -> eyre::Result<()> {
        let ksize = self.meta.ksize;
        let btx = self.dir.join("btx");
        let files = self.files();
        let pos = files.data.append_raw(payload)?;
        if files.idx.is_none() {
            files.idx = Some(BtreeIndex::open_btx_file(btx, files.data.header(), ksize, false)?);
        }
        let idx = files.idx.as_mut().expect("index just created");
        let live = idx.len();
        idx.save(key, pos)?;
        if idx.len() == live {
            self.dead += 1;
        }
        self.uncommitted = true;
        self.written += payload.len() as u64 + 8;
        Ok(())
    }

    /// Remove `key`, logging the removal (against the new data log's length) so the new
    /// generation's own replay stays correct. An absent key logs nothing.
    fn remove(&mut self, key: &[u8]) -> eyre::Result<()> {
        let files = self.files();
        let Some(idx) = files.idx.as_mut() else { return Ok(()) };
        if idx.remove(key)? {
            let data_len = files.data.file_len().to_le_bytes();
            files.removed.append_raw_parts(&[key, &data_len])?;
            self.dead += 1;
            self.uncommitted = true;
        }
        Ok(())
    }

    /// Append a commit record over what was written since the last one (all of it was already
    /// committed in the old generation).
    pub(super) fn commit_record(&mut self) -> eyre::Result<()> {
        if std::mem::take(&mut self.uncommitted) {
            self.files().data.append_raw(COMMIT)?;
        }
        Ok(())
    }

    /// Make everything written durable: the removal log, then the data log (whose commit records
    /// make the removals count), then the directory's entries. The index records the log length
    /// it matches (it is synced only by a clean close, as any generation's is).
    pub(super) fn sync(&mut self) -> eyre::Result<()> {
        let files = self.files();
        files.removed.commit()?;
        files.data.commit()?;
        files.removed.stamp_commit_marker();
        files.data.stamp_commit_marker();
        let data_len = files.data.file_len();
        if let Some(idx) = files.idx.as_mut() {
            idx.set_data_file_length(data_len);
        }
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// Stop pacing and checking for cancellation (the writer finishes the build under its lock).
    pub(super) fn unpaced(mut self) -> Self {
        self.cancel = None;
        self.rate = 0;
        self
    }

    /// The directory being built in.
    pub(super) fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    /// Hand the files over (no longer deleted on drop), with the dead puts they hold.
    pub(super) fn finish(mut self) -> (GenFiles, u64) {
        self.dir = PathBuf::new();
        (self.files.take().expect("a builder holds its files until it finishes"), self.dead)
    }

    /// Pace the copy and honour a cancellation, once per [`BATCH_BYTES`]. A pacing sleep is cut
    /// into slices of at most [`PACE_SLICE`], each followed by a cancellation check, so a clear or
    /// close never waits out a long sleep.
    fn pace(&mut self) -> eyre::Result<()> {
        if self.written < self.next_check {
            return Ok(());
        }
        self.next_check = self.written + BATCH_BYTES;
        self.check_cancel()?;
        if self.rate > 0 {
            let due = Duration::from_secs_f64(self.written as f64 / self.rate as f64);
            let mut ahead = due.saturating_sub(self.started.elapsed());
            while !ahead.is_zero() {
                let slice = ahead.min(PACE_SLICE);
                std::thread::sleep(slice);
                ahead -= slice;
                self.check_cancel()?;
            }
        }
        Ok(())
    }

    fn check_cancel(&self) -> eyre::Result<()> {
        if self.cancel.as_ref().is_some_and(|cancel| cancel.load(Ordering::Relaxed)) {
            bail!("tndb: compaction cancelled");
        }
        Ok(())
    }
}

impl Drop for Builder {
    fn drop(&mut self) {
        if let Some(mut files) = self.files.take() {
            files.data.set_remove_on_drop();
            files.removed.set_remove_on_drop();
            if let Some(idx) = files.idx.as_mut() {
                idx.set_remove_on_drop();
            }
        }
        if !self.dir.as_os_str().is_empty() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// The key of the data record `payload`: stored before the value, or derived from it.
fn key_of(payload: &[u8], meta: TableMeta, key_fn: Option<&KeyFn>) -> eyre::Result<Vec<u8>> {
    let ksize = meta.ksize as usize;
    let key = match meta.mode {
        KeyMode::Keyed if payload.len() >= ksize => payload[..ksize].to_vec(),
        KeyMode::Keyed => bail!("tndb: a data record is shorter than its key"),
        KeyMode::Derived => key_fn.ok_or_else(|| eyre::eyre!("tndb: no key function"))?(payload)?,
    };
    if key.len() != ksize {
        bail!("tndb: a {}-byte key in a table of {ksize}-byte keys", key.len());
    }
    Ok(key)
}

/// Replay committed records of the old generation into `out`: the data log's records in `data`
/// and the removal log's in `removed`. Puts and removals merge as in recovery: a removal made when
/// the data log was `d` bytes long comes after every put below `d` and before every put at or past
/// it. Commit records are skipped (the caller writes its own).
pub(super) fn replay_delta<D: Records + ?Sized, R: Records + ?Sized>(
    out: &mut Builder,
    data_log: &D,
    data: Range<u64>,
    removed_log: &R,
    removed: Range<u64>,
) -> eyre::Result<()> {
    let ksize = out.meta.ksize as usize;
    let mut removals = Vec::new();
    let mut pos = removed.start;
    while pos < removed.end {
        let payload = removed_log.record(pos)?;
        if payload.len() != ksize + 8 {
            bail!("tndb: a removal record at {pos} is malformed");
        }
        let data_len = u64::from_le_bytes(payload[ksize..].try_into().expect("8 bytes"));
        removals.push((payload[..ksize].to_vec(), data_len));
        pos += payload.len() as u64 + 8;
    }
    let mut removals = removals.into_iter().peekable();
    let mut pos = data.start;
    while pos < data.end {
        let payload = data_log.record(pos)?;
        let next = pos + payload.len() as u64 + 8;
        if !payload.is_empty() {
            while let Some((key, _)) = removals.next_if(|&(_, data_len)| data_len <= pos) {
                out.remove(&key)?;
            }
            let key = key_of(payload, out.meta, out.key_fn.as_ref())?;
            out.put(&key, payload)?;
            out.pace()?;
        }
        pos = next;
    }
    for (key, _) in removals {
        out.remove(&key)?;
    }
    Ok(())
}

/// What a finished compaction thread hands back: the new generation (synced) and how far into
/// the old logs it has replayed.
#[derive(Debug)]
pub(super) struct Compacted {
    pub(super) builder: Builder,
    pub(super) done: Committed,
}

/// Everything a compaction thread needs.
pub(super) struct Job {
    /// The directory to build the new generation in (`compact-*`).
    pub(super) dir: PathBuf,
    /// The committed state to copy; dropped once copied, so its index pages are not pinned for
    /// the catch-up.
    pub(super) snapshot: Arc<Published>,
    /// The old generation's keepalive: its files (and so both views) stay open for the whole run,
    /// even if the table is cleared.
    pub(super) alive: Arc<GenAlive>,
    pub(super) data_view: Arc<MapView>,
    pub(super) removed_view: Arc<MapView>,
    pub(super) meta: TableMeta,
    pub(super) key_fn: Option<KeyFn>,
    pub(super) rate: u64,
    /// Test-only: the thread waits at each checkpoint for a message (or the sender's drop).
    #[cfg(test)]
    pub(super) gate: Option<std::sync::mpsc::Receiver<()>>,
    /// Test-only: told each checkpoint's number as the thread reaches it.
    #[cfg(test)]
    pub(super) arrived: Option<std::sync::mpsc::Sender<u32>>,
    /// Test-only: the thread fails before it builds anything.
    #[cfg(test)]
    pub(super) fail_at_start: bool,
}

/// The writer's handle on a running compaction.
#[derive(Debug)]
pub(super) struct Compaction {
    /// The generation being compacted.
    pub(super) gen: u64,
    cancel: Arc<AtomicBool>,
    /// The old generation's committed log ends, as of the last publish (read by the catch-up).
    progress: Arc<Mutex<Committed>>,
    /// The old removal log's lock-free view, whose published length the writer advances.
    removed_view: Arc<MapView>,
    handle: JoinHandle<eyre::Result<Compacted>>,
}

impl Compaction {
    /// Start `job` on a `tndb-compact` thread, or `None` if no thread can be started (logged).
    pub(super) fn start(gen: u64, job: Job) -> Option<Self> {
        let cancel = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(Mutex::new(job.snapshot.committed));
        let removed_view = Arc::clone(&job.removed_view);
        removed_view.publish_len(job.snapshot.committed.removed);
        let (thread_cancel, thread_progress) = (Arc::clone(&cancel), Arc::clone(&progress));
        let handle = std::thread::Builder::new()
            .name("tndb-compact".to_string())
            .spawn(move || run(job, &thread_progress, &thread_cancel))
            .map_err(|e| tracing::warn!(target: "tndb", "spawn a compaction thread: {e}"))
            .ok()?;
        Some(Self { gen, cancel, progress, removed_view, handle })
    }

    /// Tell the catch-up how far the old generation's logs are committed (at each publish).
    pub(super) fn publish(&self, committed: Committed) {
        self.removed_view.publish_len(committed.removed);
        *self.progress.lock() = committed;
    }

    pub(super) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// The thread's result (waiting for it if still running).
    pub(super) fn join(self) -> eyre::Result<Compacted> {
        join(self.handle)
    }

    /// Stop the thread at its next check; its result is to be discarded.
    pub(super) fn cancel(self) -> JoinHandle<eyre::Result<Compacted>> {
        self.cancel.store(true, Ordering::Relaxed);
        self.handle
    }
}

/// A compaction thread's result (a panic is an error).
pub(super) fn join(handle: JoinHandle<eyre::Result<Compacted>>) -> eyre::Result<Compacted> {
    handle.join().unwrap_or_else(|_| Err(eyre::eyre!("tndb: the compaction thread panicked")))
}

/// Discard a finished compaction thread's result on a short-lived thread: a dropped builder
/// deletes its files, and unlinking a large copy is kept off the caller's (the writer's) path. If
/// no thread can be started the result is dropped here, with the handle.
pub(super) fn discard(handle: JoinHandle<eyre::Result<Compacted>>) {
    let _ =
        std::thread::Builder::new().name("tndb-reap".to_string()).spawn(move || drop(join(handle)));
}

/// True when `e` comes from reading damaged committed data (a failed CRC, a corrupt record),
/// rather than from the environment.
pub(super) fn is_corruption(e: &eyre::Report) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<FetchError>(),
            Some(FetchError::CrcFailed | FetchError::CorruptIndex(_))
        )
    })
}

/// Replay what was committed in the old generation since `done` (as of the last publish the
/// writer reported) and commit it in `out`. Returns the bytes it had to replay.
fn catch_up_round(
    out: &mut Builder,
    data_view: &MapView,
    removed_view: &MapView,
    progress: &Mutex<Committed>,
    done: &mut Committed,
) -> eyre::Result<u64> {
    let now = *progress.lock();
    let behind = (now.data - done.data) + (now.removed - done.removed);
    if behind > 0 {
        let (data, removed) = (done.data..now.data, done.removed..now.removed);
        replay_delta(out, data_view, data, removed_view, removed)?;
        out.commit_record()?;
        *done = now;
    }
    Ok(behind)
}

/// A test checkpoint: report reaching checkpoint `n`, then wait for the test's next message (or
/// its sender's drop). Checkpoints: 1 after the copy, 2 after each catch-up round, 3 after the
/// bulk sync.
#[cfg(test)]
fn checkpoint(
    n: u32,
    gate: &Option<std::sync::mpsc::Receiver<()>>,
    arrived: &Option<std::sync::mpsc::Sender<u32>>,
) {
    if let Some(arrived) = arrived {
        let _ = arrived.send(n);
    }
    if let Some(gate) = gate {
        let _ = gate.recv_timeout(Duration::from_secs(30));
    }
}

/// The compaction thread: copy the snapshot's rows, catch up in rounds, then make the new
/// generation durable and hand it back for the writer's switch.
fn run(job: Job, progress: &Mutex<Committed>, cancel: &Arc<AtomicBool>) -> eyre::Result<Compacted> {
    let Job {
        dir,
        snapshot,
        alive,
        data_view,
        removed_view,
        meta,
        key_fn,
        rate,
        #[cfg(test)]
        gate,
        #[cfg(test)]
        arrived,
        #[cfg(test)]
        fail_at_start,
    } = job;
    #[cfg(test)]
    if fail_at_start {
        bail!("tndb: injected compaction failure");
    }
    // Keeps the old generation's files open (and so both views valid) until the thread ends.
    let _alive = alive;
    let mut out = Builder::new(dir, meta, key_fn, Arc::clone(cancel), rate)?;

    // 1. The snapshot's rows, in key order.
    let mut done = snapshot.committed;
    if let Some(index) = &snapshot.index {
        let mut cursor = ScanKind::Forward.cursor(index)?;
        while let Some(item) = cursor.next(index) {
            let (key, pos) = item?;
            out.put(key, data_view.record(pos)?)?;
            out.pace()?;
        }
    }
    drop(snapshot);
    out.commit_record()?;
    #[cfg(test)]
    checkpoint(1, &gate, &arrived);

    // 2. What was committed since, in rounds, until a round has little left to replay.
    for _ in 0..MAX_CATCHUP_ROUNDS {
        out.check_cancel()?;
        let behind = catch_up_round(&mut out, &data_view, &removed_view, progress, &mut done)?;
        #[cfg(test)]
        checkpoint(2, &gate, &arrived);
        if behind <= CATCHUP_BYTES {
            break;
        }
    }

    // 3. Durable before the writer can rename it into place: the bulk sync, then one more round
    // for what was committed during it and a (small) sync of that, so the writer's catch-up
    // under its lock holds only what lands during the small sync.
    out.sync()?;
    #[cfg(test)]
    checkpoint(3, &gate, &arrived);
    out.check_cancel()?;
    if catch_up_round(&mut out, &data_view, &removed_view, progress, &mut done)? > 0 {
        out.sync()?;
    }
    Ok(Compacted { builder: out.unpaced(), done })
}
