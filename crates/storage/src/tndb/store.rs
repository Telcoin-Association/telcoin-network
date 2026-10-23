//! The shared log and its per-table indexes: the generation, the one write lock, batches,
//! publishing, reads and scans, and recovery at open.
//!
//! Every write takes the database's one write lock ([`Core::write`]): it appends a batch of
//! records to the log, applies them to the tables' indexes, publishes each touched table's
//! snapshot, numbers the batch and moves the log's frontier, all before releasing it. So the log
//! only ever holds whole batches between writes, a snapshot never sees part of one, and a failed
//! batch is taken back exactly (the log is rewound to its start, nothing else having appended
//! since, and each touched index reverted to its last publish). Readers take no lock: they load a
//! table's [`Published`] snapshot.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    hash::BuildHasherDefault,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU16, AtomicU64, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use eyre::{bail, WrapErr as _};
use parking_lot::Mutex;

use super::{
    commit::{Durability, UnwindLatch},
    compact::{read_base, CompactShared},
    manifest::Manifest,
    metrics::TnDbMetrics,
    record::{self, Op, KIND_CLEAR, KIND_PUT, KIND_REMOVE},
    TableId,
};
use crate::{
    archive::{
        btree_index::{
            index::IndexSnapshot,
            iter::{BtreeCursor, PageSource},
            BtreeIndex,
        },
        data_file::{MapView, MmapFileOptions, COMMIT_MARKER_LEN},
        error::{fetch::FetchError, load_header::LoadHeaderError},
        fxhasher::FxHasher,
        pack::{DataHeader, Pack, PackCompression, DATA_HEADER_BYTES},
    },
    tndb::{
        database::CompactionConfig,
        layout::{
            compact_dir, drop_in_background, gen_dir, list_gens, lock_dir, remove_unfinished,
        },
    },
};

/// Pack format version of the shared log.
const PACK_VERSION: u16 = 1;

/// The least log a batch that empties every table compacts (see `Writer::emptied`): below it, an
/// emptied log is not worth a compaction.
const EMPTIED_MIN_BYTES: u64 = 1 << 20;

/// Address space reserved for the log's mapping (virtual only; see
/// [`MmapFileOptions::reserve`]): the log never moves while open unless it outgrows this.
const LOG_MAP_RESERVE: u64 = 16 << 30;

/// The shared log.
pub(super) type Log = Pack<Vec<u8>>;

/// The directory of table `id`'s index in a generation.
pub(super) fn idx_dir(gen_path: &Path, id: TableId) -> PathBuf {
    gen_path.join(format!("idx-{id}"))
}

/// Which ordered scan to run over a table's index.
pub(super) enum ScanKind {
    /// Every entry in ascending key order.
    Forward,
    /// Every entry in descending key order.
    Reverse,
    /// Ascending from the given key bytes (inclusive) to the end.
    From(Vec<u8>),
    /// Descending over the entries whose keys are strictly less than the given key bytes.
    RevFrom(Vec<u8>),
}

impl ScanKind {
    pub(super) fn cursor<S: PageSource + ?Sized>(self, src: &S) -> Result<BtreeCursor, FetchError> {
        let (reverse, lower, upper) = match self {
            Self::Forward => (false, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            Self::Reverse => (true, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            Self::From(key) => (false, std::ops::Bound::Included(key), std::ops::Bound::Unbounded),
            Self::RevFrom(key) => {
                (true, std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key))
            }
        };
        BtreeCursor::new(src, reverse, lower, upper)
    }
}

/// Keeps a generation's files open while snapshots of it live: a compaction's switch retires the
/// old generation's files into it (marked to be deleted on close), so they close when its last
/// snapshot, scan or sync round drops.
#[derive(Debug, Default)]
pub(super) struct GenAlive {
    retired: Mutex<Option<Retired>>,
}

/// A retired generation's files.
#[derive(Debug)]
struct Retired {
    _log: Log,
    _idxs: Vec<BtreeIndex>,
}

impl Drop for GenAlive {
    /// Close a retired generation's files (unmapping and deleting them) on a short-lived thread,
    /// off the path of whichever writer, reader or sync round let go of it last.
    fn drop(&mut self) {
        if let Some(retired) = self.retired.get_mut().take() {
            drop_in_background(retired, "tndb-reap");
        }
    }
}

/// What a table's readers see: the index snapshot of its last publish and the log's view, read up
/// to `limit`. Immutable; replaced as a whole by each publish.
#[derive(Debug)]
pub(super) struct Published {
    pub(super) index: Option<IndexSnapshot>,
    pub(super) view: Arc<MapView>,
    /// The log's length at the publish: every record the snapshot reaches lies below it, written
    /// in full.
    pub(super) limit: u64,
    /// The table's id: what each record an index entry points at must carry.
    table: TableId,
    /// Keeps the snapshot's generation open (declared last, so it drops after the snapshot).
    _alive: Arc<GenAlive>,
}

impl Published {
    /// The value bytes of the put record at `pos`, which the index gave for `key` (checked to
    /// be that row's: see [`record::put_value`]).
    fn value_at(&self, pos: u64, key: &[u8]) -> Result<&[u8], FetchError> {
        let payload =
            Pack::<Vec<u8>>::record_bytes_within(&self.view, pos, self.limit, MAX_RECORD)?;
        record::put_value(payload, self.table, key)
    }

    fn get_with<R>(&self, key: &[u8], decode: impl FnOnce(&[u8]) -> R) -> eyre::Result<Option<R>> {
        let Some(index) = &self.index else { return Ok(None) };
        let pos = match index.load(key) {
            Ok(pos) => pos,
            Err(FetchError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(Some(decode(self.value_at(pos, key)?)))
    }

    fn contains(&self, key: &[u8]) -> Result<bool, FetchError> {
        let Some(index) = &self.index else { return Ok(false) };
        match index.load(key) {
            Ok(_) => Ok(true),
            Err(FetchError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn len(&self) -> usize {
        self.index.as_ref().map_or(0, IndexSnapshot::len)
    }
}

/// A table, as readers (and writers looking it up) see it.
#[derive(Debug)]
pub(super) struct TableShared {
    pub(super) id: TableId,
    pub(super) name: String,
    /// The encoded key size, 0 until the table's first row fixes it.
    ksize: AtomicU16,
    published: ArcSwap<Published>,
}

impl TableShared {
    /// The table's key size, or 0 if not yet fixed.
    pub(super) fn ksize(&self) -> u16 {
        self.ksize.load(Ordering::Acquire)
    }

    /// Read the published value for `key` and map its bytes with `decode`, or `None`. Damage the
    /// read finds is an error, latched in `core` (see [`Core::read_damaged`]).
    pub(super) fn get_with<R>(
        &self,
        core: &Core,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.published
            .load()
            .get_with(key, decode)
            .inspect_err(|e| core.read_damaged(&self.name, e))
    }

    /// True if the published index holds `key`. Damage the index's checks see is an error, not
    /// "absent", latched in `core`; no log record is read, so an entry whose position was damaged
    /// still answers true (a get of that key is `CorruptIndex`).
    pub(super) fn contains(&self, core: &Core, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.published.load().contains(key).inspect_err(|e| core.read_damaged(&self.name, e))?)
    }

    /// Rows in the published state.
    pub(super) fn len(&self) -> usize {
        self.published.load().len()
    }

    /// A lazy scan of `kind` over the published snapshot, holding no lock; `core` keeps the
    /// generation's files open while it lives.
    pub(super) fn scan(&self, kind: ScanKind, core: Arc<Core>) -> TableScan {
        let published = self.published.load_full();
        let cursor = published.index.as_ref().and_then(|index| match kind.cursor(index) {
            Ok(cursor) => Some(cursor),
            Err(e) => {
                scan_error(&core, &self.name, &e);
                None
            }
        });
        TableScan { published, cursor, name: self.name.clone(), core }
    }

    /// Map the single `(key, value)` a scan of `kind` lands on first with `f` (a direct seek).
    pub(super) fn first_with<R>(
        &self,
        core: &Core,
        kind: ScanKind,
        f: impl FnOnce(&[u8], &[u8]) -> R,
    ) -> Option<R> {
        let published = self.published.load();
        let index = published.index.as_ref()?;
        let found = kind.cursor(index).and_then(|mut cursor| {
            let Some(item) = cursor.next(index) else { return Ok(None) };
            let (key, pos) = item?;
            Ok(Some((key, published.value_at(pos, key)?)))
        });
        match found {
            Ok(Some((key, value))) => Some(f(key, value)),
            Ok(None) => None,
            Err(e) => {
                scan_error(core, &self.name, &e);
                None
            }
        }
    }
}

/// A scan or seek ended by an error: `DBIter` cannot carry it, so the shorter answer would pass
/// for a whole one. Log it and latch the damage (see [`Core::read_damaged`]): every later write
/// and persist then fails, so the node stops rather than act on the short answer.
fn scan_error(core: &Core, table: &str, e: &FetchError) {
    tracing::error!(target: "tndb", "scan of table {table} ended by an error: {e}");
    core.read_damaged(table, e);
}

/// A lazy, key-ordered scan of a published snapshot.
pub(super) struct TableScan {
    published: Arc<Published>,
    cursor: Option<BtreeCursor>,
    name: String,
    /// Keeps the database (and so the current generation's files) open while the scan lives, and
    /// latches damage the scan finds.
    core: Arc<Core>,
}

impl TableScan {
    /// Map the next `(key, value)` with `f`, or `None` when the scan is done (or failed).
    pub(super) fn next_with<R>(&mut self, f: impl FnOnce(&[u8], &[u8]) -> R) -> Option<R> {
        let published = &*self.published;
        let index = published.index.as_ref()?;
        let row = self.cursor.as_mut()?.next(index).map(|item| {
            let (key, pos) = item?;
            Ok((key, published.value_at(pos, key)?))
        });
        match row {
            Some(Ok((key, value))) => Some(f(key, value)),
            Some(Err(e)) => {
                scan_error(&self.core, &self.name, &e);
                self.cursor = None;
                None
            }
            None => {
                self.cursor = None;
                None
            }
        }
    }
}

/// What the commit thread syncs of a generation: the log's view (for `msync`) and a duplicate of
/// its descriptor (for the `fsync` a growth leaves owed).
#[derive(Debug)]
pub(super) struct GenSync {
    pub(super) view: Arc<MapView>,
    pub(super) file: fs::File,
    /// The log's growth count, as of the last batch (the writer updates it).
    pub(super) growths: AtomicU64,
    /// The growth count the commit thread last made durable with an `fsync`.
    pub(super) size_synced: AtomicU64,
    /// The bytes of the log made durable so far.
    pub(super) synced: AtomicU64,
    /// Keeps the generation's files open (mapped) while the commit thread syncs them.
    pub(super) _alive: Arc<GenAlive>,
}

impl GenSync {
    /// The sync state of `log` (its view and `alive`), all of it synced.
    pub(super) fn new(
        log: &Log,
        view: &Arc<MapView>,
        alive: &Arc<GenAlive>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            view: Arc::clone(view),
            file: log.dup_file()?,
            growths: AtomicU64::new(log.growths()),
            size_synced: AtomicU64::new(log.growths()),
            synced: AtomicU64::new(log.file_len()),
            _alive: Arc::clone(alive),
        })
    }
}

/// The log's frontier after the last batch: every batch numbered at or below `seq` lies below
/// `end`, in the generation `sync` describes. Replaced as a whole by each batch, so the commit
/// thread reads the three together.
#[derive(Debug)]
pub(super) struct Frontier {
    pub(super) sync: Arc<GenSync>,
    pub(super) end: u64,
    pub(super) seq: u64,
}

/// One write of a batch (see [`Core::write`]).
#[derive(Debug)]
pub(super) enum BatchOp<'a> {
    Put(TableId, &'a [u8], &'a [u8]),
    Remove(TableId, &'a [u8]),
    Clear(TableId),
}

impl BatchOp<'_> {
    fn table(&self) -> TableId {
        match self {
            Self::Put(table, ..) | Self::Remove(table, _) | Self::Clear(table) => *table,
        }
    }
}

/// A table's writer state.
#[derive(Debug)]
struct TableWriter {
    ksize: u16,
    /// Created (and published) when the key size is fixed.
    idx: Option<BtreeIndex>,
    shared: Arc<TableShared>,
}

/// The writer state, behind the one write lock.
#[derive(Debug)]
pub(super) struct Writer {
    /// The current generation's number and directory.
    gen: u64,
    gen_dir: PathBuf,
    log: Log,
    view: Arc<MapView>,
    alive: Arc<GenAlive>,
    sync: Arc<GenSync>,
    tables: BTreeMap<TableId, TableWriter>,
    /// The last batch's number.
    seq: u64,
    /// How far the log's commit marker has been stamped (by a stamp that landed). 0 at open and
    /// after a switch, so the first stamp covers everything already in the log.
    stamped: u64,
    /// Set when the writer can no longer write (a batch it could not take back): every later
    /// write fails until a restart.
    failed: Option<String>,
    /// A compaction found damage (`Core::compaction_damaged`): the close marks every index stale,
    /// so the next open replays the log whole, rebuilding a damaged index and refusing a damaged
    /// sealed log. Never cleared.
    damage_seen: bool,
    durability: Arc<Durability>,
    /// When the log compacts automatically.
    config: CompactionConfig,
    /// The trigger's base: the log's length before the final tail of the switch that made this
    /// generation (0 if no compaction did; see `compact::COMPACTED`): the log compacts once it
    /// has doubled since.
    base: u64,
    /// A row may have been made dead (overwritten, removed or cleared) since the last compaction
    /// (assumed at open, unless the log is empty): a log that only grows does not compact.
    has_dead: bool,
    /// A batch left every table empty while the log held at least [`EMPTIED_MIN_BYTES`] (the
    /// epoch tables are all cleared at an epoch's end): the log compacts at once, whatever its
    /// growth threshold, since nothing live is left to copy. Kept until a compaction whose
    /// snapshot came after it switches (or one whose new generation holds nothing dead), so writes
    /// that resume meanwhile do not cancel it. One raised while a compaction copies survives that
    /// compaction's switch, and the new generation's next switch drops it (every snapshot of the
    /// new generation comes after it), unless another emptying came since.
    emptied: bool,
    /// The log's length at the last batch that left every table empty (see `emptied`): moved by
    /// each one, even while an earlier one is pending, so a switch whose snapshot came between
    /// the two keeps the signal. Reset to 0 by a switch, as a position in the old log.
    emptied_at: u64,
    /// The log's length at the last compaction snapshot (see [`Core::compaction_snapshot`]).
    snapshot_end: u64,
    /// Test-only: the next batch panics after appending this many records.
    #[cfg(test)]
    panic_after: Option<usize>,
    /// Test-only: the next batch holds the write lock at least this long.
    #[cfg(test)]
    hold_next: Option<std::time::Duration>,
    /// Test-only: the next rollback, or the next publish of a batch, panics.
    #[cfg(test)]
    panic_in_rollback: bool,
    #[cfg(test)]
    panic_in_publish: bool,
    /// Test-only: rolled-back batches whose zeroed bytes were flushed.
    #[cfg(test)]
    rollback_flushes: u64,
    /// Test-only: the next batch fails once all its records are appended; the next rollback
    /// first flushes the batch's bytes, as the system's writeback might have.
    #[cfg(test)]
    fail_after_append: bool,
    #[cfg(test)]
    writeback_before_rollback: bool,
}

/// A compacted generation, ready to replace the current one (see [`Writer::install_gen`]).
pub(super) struct NewGen {
    pub(super) gen: u64,
    pub(super) dir: PathBuf,
    pub(super) log: Log,
    pub(super) idxs: BTreeMap<TableId, BtreeIndex>,
    pub(super) view: Arc<MapView>,
    pub(super) alive: Arc<GenAlive>,
    pub(super) sync: Arc<GenSync>,
    /// Whether the compaction's catch-up made a row dead.
    pub(super) has_dead: bool,
    /// The trigger's `base`: the length written to the generation's `compacted` file (the log's
    /// length before the switch's tail), so a restart compacts on the same schedule.
    pub(super) base: u64,
}

impl Writer {
    pub(super) fn check(&self) -> eyre::Result<()> {
        if let Some(cause) = &self.failed {
            bail!("tndb: the writer stopped ({cause}); restart to recover");
        }
        if self.durability.log_unknown() {
            // A failed sync leaves the log's state on disk unknown: never seal it as clean. (Damage
            // a read found fails the write below too, but leaves the log to be sealed: see
            // `Writer::drop`.)
            self.log.poison();
        }
        self.durability.check()
    }

    /// Stamp the log's commit marker up to what the commit thread has synced (best-effort and
    /// unsynced, as a commit marker always is), lazily, under the write lock. A stamp the log
    /// skipped (no room past its end: a reopened log until it grows; a sealed one whose sentinel
    /// is still pending) is tried again by the next call.
    fn stamp_synced(&mut self) {
        let synced = self.sync.synced.load(Ordering::Acquire);
        if synced > self.stamped {
            self.log.note_synced(synced);
            if self.log.stamp_commit_marker_at(synced) {
                self.stamped = synced;
            }
        }
    }

    /// The ops of `ops` that change something, in order: a removal of a key the table does not
    /// hold (or that this batch cleared) writes nothing. Validates every op first: an unknown
    /// table, or a key of the wrong size, is the caller's error and writes nothing.
    ///
    /// A batch names each key at most once per table, and a table's clear comes before its other
    /// ops (a transaction's overlay keeps one write per key, its clear first; a bare write is one
    /// op), so whether a removal is effective depends only on the index before the batch.
    fn effective(&self, ops: &[BatchOp<'_>]) -> eyre::Result<Vec<usize>> {
        #[cfg(debug_assertions)]
        {
            let mut seen = std::collections::HashSet::new();
            for op in ops {
                if let BatchOp::Put(id, key, _) | BatchOp::Remove(id, key) = op {
                    debug_assert!(seen.insert((*id, *key)), "a key twice in one batch");
                }
            }
        }
        let mut cleared: Vec<TableId> = Vec::new();
        let mut out = Vec::with_capacity(ops.len());
        for (i, op) in ops.iter().enumerate() {
            let id = op.table();
            let table = self
                .tables
                .get(&id)
                .ok_or_else(|| eyre::eyre!("tndb: table {id} is not in the database"))?;
            match op {
                BatchOp::Put(_, key, _) => {
                    if table.ksize == 0 || key.len() != table.ksize as usize {
                        bail!(
                            "tndb: a {}-byte key in table {} of {}-byte keys",
                            key.len(),
                            table.shared.name,
                            table.ksize
                        );
                    }
                    out.push(i);
                }
                BatchOp::Remove(_, key) => {
                    let held = match &table.idx {
                        Some(idx) if !cleared.contains(&id) => {
                            idx.contains(key).wrap_err_with(|| {
                                format!("tndb: remove from table {}", table.shared.name)
                            })?
                        }
                        _ => false,
                    };
                    if held {
                        out.push(i);
                    }
                }
                BatchOp::Clear(_) => {
                    cleared.push(id);
                    out.push(i);
                }
            }
        }
        Ok(out)
    }

    /// Append the effective ops (the last one flagged as the batch's end) and apply each to its
    /// table's index, noting each table touched before changing its index.
    fn append_and_index(
        &mut self,
        ops: &[BatchOp<'_>],
        effective: &[usize],
        touched: &mut Vec<TableId>,
    ) -> eyre::Result<()> {
        #[cfg(test)]
        let panic_after = self.panic_after.take();
        #[cfg(test)]
        let fail_after = std::mem::take(&mut self.fail_after_append);
        let Self { log, tables, has_dead, .. } = self;
        let last = effective.last().copied();
        for (n, &i) in effective.iter().enumerate() {
            #[cfg(test)]
            if panic_after == Some(n) {
                panic!("injected batch panic");
            }
            #[cfg(not(test))]
            let _ = n;
            let op = &ops[i];
            let id = op.table();
            let end = Some(i) == last;
            if !touched.contains(&id) {
                touched.push(id);
            }
            let table = tables.get_mut(&id).expect("validated by effective()");
            match op {
                BatchOp::Put(_, key, value) => {
                    let pos =
                        log.append_raw_parts(&[&record::head(KIND_PUT, id, end), key, value])?;
                    let idx = table.idx.as_mut().expect("a key size is fixed");
                    let rows = idx.len();
                    idx.save(key, pos)?;
                    // An overwrite (the row count unchanged) leaves the old record dead.
                    *has_dead |= idx.len() == rows;
                }
                BatchOp::Remove(_, key) => {
                    log.append_raw_parts(&[&record::head(KIND_REMOVE, id, end), key])?;
                    let removed = table.idx.as_mut().expect("present").remove(key)?;
                    debug_assert!(removed, "effective() checked the key is present");
                    *has_dead = true;
                }
                BatchOp::Clear(_) => {
                    log.append_raw_parts(&[&record::head(KIND_CLEAR, id, end)])?;
                    if let Some(idx) = table.idx.as_mut() {
                        *has_dead |= !idx.is_empty();
                        idx.clear()?;
                    }
                }
            }
        }
        #[cfg(test)]
        if fail_after {
            bail!("tndb: injected failure after a batch's records were appended");
        }
        Ok(())
    }

    /// Take a failed batch back: rewind the log to its start and revert each touched index to its
    /// last publish (which, under the write lock, is its state before the batch). A batch that
    /// cannot be taken back stops the writer.
    fn rollback(&mut self, start: u64, touched: &[TableId], sync_lock: &Mutex<()>) {
        // A panic part way leaves the writer half-reverted: latched, nothing builds on it.
        if catch_unwind(AssertUnwindSafe(|| self.revert(start, touched, sync_lock))).is_err() {
            self.stop("a failed batch's rollback panicked".to_string());
        }
    }

    /// Take back a failed batch that began at `start` (see [`Self::rollback`]). `sync_lock` is the
    /// log's (`Core::sync_lock`): the rollback's flush is a sync of the log like a round's.
    fn revert(&mut self, start: u64, touched: &[TableId], sync_lock: &Mutex<()>) {
        #[cfg(test)]
        if std::mem::take(&mut self.panic_in_rollback) {
            panic!("injected panic in a rollback");
        }
        let end = self.log.file_len();
        #[cfg(test)]
        if std::mem::take(&mut self.writeback_before_rollback) {
            let _ = self.view.msync(start, end);
        }
        self.log.rewind_to(start);
        if end > start {
            // The batch's bytes may be on disk already (a sync round's msync of the page the
            // batch shares with the last published one, or the system's writeback): the zeros
            // must reach the disk too, or a power loss could bring the failed batch back.
            //
            // One sync of the log in flight at a time, this one included: a writeback error is
            // reported once per open file, so a flush beside a sync round could take the error
            // the round must see, and the round would acknowledge data the disk lost. Holding the
            // round's lock across the flush and its latch, a round after it finds the failure.
            let one_sync = sync_lock.lock();
            if let Err(e) = self.view.msync(start, end) {
                self.stop(format!("flush a failed batch's rollback: {e}"));
            }
            drop(one_sync);
            // A writer that found the sync lock held by this flush waits for a round: wake it to
            // lead one itself.
            self.durability.notify_waiters();
            #[cfg(test)]
            {
                self.rollback_flushes += 1;
            }
        }
        for id in touched {
            if let Some(idx) = self.tables.get_mut(id).and_then(|table| table.idx.as_mut()) {
                if let Err(e) = idx.revert_to_published() {
                    self.stop(format!("revert table {id}'s index: {e}"));
                } else if idx.is_poisoned() {
                    // It refuses every later growth: rather than one table failing its writes
                    // while the rest carry on, the database stops (the next open rebuilds it).
                    // Each index op follows its record's append, so this batch wrote to the log:
                    // a `stop`, unlike damage met before anything is appended (see
                    // `Self::latch_if_poisoned`).
                    self.stop(format!("table {id}'s index is poisoned"));
                }
            }
        }
        if self.log.is_failed() {
            self.stop("the log failed".to_string());
        }
    }

    /// Latch `e`, a failed [`Self::effective`], if it poisoned the index of a table `ops` writes
    /// (a removal's lookup that met damage). Nothing of the batch was appended, so this latches as
    /// damage a read found does (see [`Core::read_damaged`]): every later write and persist fails,
    /// but the close still seals the log, with every index stale (see `Writer::drop`). A
    /// [`Self::stop`] would leave it unsealed, and the next open would cut a damaged record past
    /// the last commit marker as a crash's torn tail.
    fn latch_if_poisoned(&self, ops: &[BatchOp<'_>], e: &eyre::Report) {
        let poisoned = ops.iter().map(BatchOp::table).find(|id| {
            self.tables
                .get(id)
                .and_then(|table| table.idx.as_ref())
                .is_some_and(|idx| idx.is_poisoned())
        });
        if let Some(id) = poisoned {
            self.durability.fail_damaged(format!("tndb: table {id}'s index found damage: {e:#}"));
        }
    }

    pub(super) fn stop(&mut self, cause: String) {
        // Latched before anything else (logging included), so no sync can acknowledge past it.
        self.durability.fail(cause.clone());
        tracing::error!(target: "tndb", "{cause}; writes stop until a restart");
        self.failed.get_or_insert(cause);
    }

    /// Publish table `id`'s working state to its readers.
    fn publish(&mut self, id: TableId) {
        let limit = self.log.file_len();
        let (view, alive) = (Arc::clone(&self.view), Arc::clone(&self.alive));
        let table = self.tables.get_mut(&id).expect("a known table");
        let published = Published {
            index: table.idx.as_mut().map(BtreeIndex::publish),
            view,
            limit,
            table: id,
            _alive: alive,
        };
        table.shared.published.store(Arc::new(published));
    }

    /// True when the log should compact automatically: it is at least the configured size, has
    /// at least doubled since the last compaction, and may hold dead rows. O(1), checked after
    /// every batch.
    fn wants_compaction(&self) -> bool {
        let len = self.log.file_len();
        self.config.auto_min_bytes.is_some_and(|min| {
            self.emptied || (self.has_dead && len >= min && len >= self.base.saturating_mul(2))
        })
    }

    /// Note a batch that left every table empty in a log of at least [`EMPTIED_MIN_BYTES`] (see
    /// `emptied`), moving `emptied_at` even if one is already pending. O(tables) at most (it stops
    /// at the first table with a row), and only once the log is that long and holds dead rows.
    fn note_if_emptied(&mut self) {
        if self.has_dead
            && self.log.file_len() >= EMPTIED_MIN_BYTES
            && self.tables.values().all(|table| table.idx.as_ref().is_none_or(BtreeIndex::is_empty))
        {
            self.emptied = true;
            self.emptied_at = self.log.file_len();
        }
    }

    /// The current generation's number.
    pub(super) fn gen(&self) -> u64 {
        self.gen
    }

    /// The log's length (its end, after the last whole batch).
    pub(super) fn log_len(&self) -> u64 {
        self.log.file_len()
    }

    /// Each table with an index, and its row count.
    pub(super) fn table_rows(&self) -> Vec<(TableId, usize)> {
        self.tables
            .iter()
            .filter_map(|(&id, table)| table.idx.as_ref().map(|idx| (id, idx.len())))
            .collect()
    }

    /// Make `new` (a compacted generation, renamed into place and durable) the current
    /// generation, and publish every table from it. The old generation's files are retired into
    /// its `GenAlive`, to be deleted once its last snapshot drops. Does no I/O and cannot fail.
    /// Returns the old generation's directory, for the caller to delete.
    pub(super) fn install_gen(&mut self, new: NewGen) -> PathBuf {
        let NewGen { gen, dir, log, mut idxs, view, alive, sync, has_dead, base } = new;
        let mut old_log = std::mem::replace(&mut self.log, log);
        old_log.set_remove_on_drop();
        let mut old_idxs = Vec::new();
        for (id, table) in &mut self.tables {
            let old = std::mem::replace(&mut table.idx, idxs.remove(id));
            if let Some(mut old) = old {
                old.set_remove_on_drop();
                old_idxs.push(old);
            }
        }
        debug_assert!(idxs.is_empty(), "every compacted index belongs to a table");
        let old_alive = std::mem::replace(&mut self.alive, alive);
        *old_alive.retired.lock() = Some(Retired { _log: old_log, _idxs: old_idxs });
        self.view = view;
        self.sync = sync;
        self.gen = gen;
        // The compaction stamped the new log's marker, if it had room: the next write stamps it
        // again either way.
        self.stamped = 0;
        self.base = base;
        self.has_dead = has_dead;
        // An emptying after the snapshot was copied with the rows it cleared (live when copied,
        // dead after the catch-up's clears): the new generation still needs its reset, unless
        // nothing it holds is dead (the tables were empty already when copied).
        self.emptied = self.emptied && self.emptied_at > self.snapshot_end && has_dead;
        // `emptied_at` is a position in the old log. Every later snapshot of the new generation
        // comes after that emptying, so its next switch drops a signal kept here unless a new
        // emptying moves it on.
        self.emptied_at = 0;
        let ids: Vec<TableId> = self.tables.keys().copied().collect();
        for id in ids {
            self.publish(id);
        }
        std::mem::replace(&mut self.gen_dir, dir)
    }
}

impl Drop for Writer {
    /// A clean close: the commit thread has stopped; commit the log the ordinary way, and record
    /// the log length every index matches, so the next open needs no rebuild. A close that cannot
    /// commit (or follows a failed sync, or a stopped writer) leaves the log unsealed and every
    /// index marked stale instead.
    ///
    /// After damage a read found (see [`Core::read_damaged`] and [`Self::latch_if_poisoned`]) or a
    /// compaction found (see [`Core::compaction_damaged`]), and nothing else, the log is still
    /// committed and sealed, but every index is marked stale: the next open replays the sealed
    /// log, which must read whole. Damage to an index is then rebuilt; damage to the log fails the
    /// open on every retry, where an unsealed log's replay would take the damaged record for a
    /// crash's torn tail and cut every acknowledged batch after it.
    fn drop(&mut self) {
        let committed =
            self.failed.is_none() && !self.durability.log_unknown() && self.log.commit().is_ok();
        if committed {
            self.log.stamp_commit_marker();
        } else {
            self.log.poison();
        }
        let current = committed && !self.durability.is_failed() && !self.damage_seen;
        let len = if current { self.log.file_len() } else { u64::MAX };
        for table in self.tables.values_mut() {
            if let Some(idx) = table.idx.as_mut() {
                idx.set_data_file_length(len);
            }
        }
    }
}

/// One table as a compaction copies it.
pub(super) struct SnapTable {
    pub(super) id: TableId,
    pub(super) ksize: u16,
    pub(super) published: Arc<Published>,
}

/// What a compaction copies (see [`Core::compaction_snapshot`]).
pub(super) struct Snapshot {
    pub(super) gen: u64,
    pub(super) gen_dir: PathBuf,
    pub(super) view: Arc<MapView>,
    /// Keeps the generation's files open for the whole compaction.
    pub(super) alive: Arc<GenAlive>,
    /// The log's end at the snapshot, between two batches: the catch-up starts here.
    pub(super) end: u64,
    pub(super) tables: Vec<SnapTable>,
}

/// The open tables by name.
pub(super) type Tables = HashMap<String, Arc<TableShared>, BuildHasherDefault<FxHasher>>;

/// A database: the writer behind its one lock, the tables' reader state, and the frontier the
/// commit thread syncs to. Shared by every handle, transaction and scan.
#[derive(Debug)]
pub(super) struct Core {
    pub(super) root: PathBuf,
    pub(super) writer: Mutex<Writer>,
    tables: ArcSwap<Tables>,
    pub(super) frontier: ArcSwap<Frontier>,
    pub(super) durability: Arc<Durability>,
    /// Table registrations and key-size fixes (each rewrites the manifest), and a compaction's
    /// switch: taken before the write lock, never while holding it.
    pub(super) registry: Mutex<Manifest>,
    /// The compaction thread's requests and state.
    pub(super) compact: CompactShared,
    /// The production metrics' handles.
    pub(super) metrics: TnDbMetrics,
    /// Held by whoever runs a sync round (see `commit::sync_round`): one at a time.
    pub(super) sync_lock: Mutex<()>,
    /// Test-only: the tables whose indexes the open rebuilt.
    #[cfg(test)]
    rebuilt: Vec<TableId>,
    /// Test-only: this log's slot in the per-log stats.
    #[cfg(test)]
    pub(super) stat_slot: usize,
    /// The database's `LOCK` (an flock), held until the core drops: declared last, so the files
    /// close first. Taken out by a test's simulated crash.
    #[cfg_attr(not(test), allow(dead_code))]
    lock: Mutex<Option<fs::File>>,
}

impl Core {
    /// Open (creating if absent) the database at `root`, recovering its indexes if needed.
    pub(super) fn open(
        root: &Path,
        durability: Arc<Durability>,
        config: CompactionConfig,
        metrics: TnDbMetrics,
    ) -> eyre::Result<Self> {
        fs::create_dir_all(root)?;
        let lock = lock_dir(root)?;
        Manifest::remove_tmp(root);
        // An unfinished compaction is never a generation.
        remove_unfinished(root)?;
        let manifest = Manifest::read(root)?;
        // A log with records always has a manifest (it is written before a table's first record),
        // so a missing one means it was lost. Its tables' records must not be dropped as an empty
        // database, nor their ids reused: the log is replayed, and its first record (of a table
        // the manifest no longer names) fails the open. A fresh database replays nothing.
        let manifest_lost = manifest.is_none();
        let manifest = manifest.unwrap_or_default();
        let (gen, mut log, older) = open_current_gen(root, !manifest_lost)?;
        let gen_path = gen_dir(root, gen);
        // A table's index is created only after the manifest names the table, so an index the
        // manifest does not name means it was rolled back (an older copy restored): replay the
        // log, as for a lost one, so a record of a table it no longer names fails the open. Once
        // the replay has passed, the orphans index nothing the log holds, and go.
        let orphans =
            if manifest_lost { Vec::new() } else { orphan_indexes(&gen_path, &manifest)? };
        let replay_all = manifest_lost || !orphans.is_empty();
        let header = *log.header();

        // Each table's index: a stale or damaged one is rebuilt from the log.
        let mut idxs: BTreeMap<TableId, Option<BtreeIndex>> = BTreeMap::new();
        let mut stale = Vec::new();
        for entry in &manifest.tables {
            if entry.ksize == 0 {
                idxs.insert(entry.id, None);
                continue;
            }
            let dir = idx_dir(&gen_path, entry.id);
            let idx = if dir.exists() {
                match BtreeIndex::open_btx_file(&dir, &header, entry.ksize, false) {
                    Ok(idx) => Some(idx),
                    Err(LoadHeaderError::IO(e)) => return Err(e.into()),
                    Err(_) => None,
                }
            } else {
                None
            };
            if idx
                .as_ref()
                .is_none_or(|idx| idx.opened_unclean() || idx.data_file_length() != log.file_len())
            {
                stale.push(entry.id);
            }
            idxs.insert(entry.id, idx);
        }
        #[cfg(test)]
        let mut rebuilt = Vec::new();
        if log.opened_unclean() || !stale.is_empty() || replay_all {
            let all = log.opened_unclean();
            let rebuild = |id: TableId| all || stale.contains(&id);
            #[cfg(test)]
            {
                rebuilt = manifest.tables.iter().map(|t| t.id).filter(|&id| rebuild(id)).collect();
            }
            recover(&mut log, &manifest, &gen_path, &header, &mut idxs, rebuild)?;
        }
        for orphan in orphans {
            let _ = fs::remove_dir_all(orphan);
        }
        // The current generation has opened: an older one (left by a crash right after a switch)
        // is no longer the only other copy.
        for old in older {
            if let Err(e) = fs::remove_dir_all(gen_dir(root, old)) {
                tracing::warn!(target: "tndb", "remove old generation {old}: {e}");
            }
        }

        let view = log.view();
        let alive = Arc::new(GenAlive::default());
        let sync = Arc::new(GenSync::new(&log, &view, &alive)?);
        let frontier = Frontier { sync: Arc::clone(&sync), end: log.file_len(), seq: 0 };
        let log_len = log.file_len();
        let base = read_base(&gen_path);
        let mut writer = Writer {
            gen,
            gen_dir: gen_path,
            log,
            view,
            alive,
            sync,
            tables: BTreeMap::new(),
            seq: 0,
            // Nothing stamped yet: the log as it opened (durable: sealed after a sync, or synced
            // by an unclean open) gets its first marker from the first write, the one that grows
            // the log past its end (see `Core::write`).
            stamped: 0,
            failed: None,
            damage_seen: false,
            durability: Arc::clone(&durability),
            config,
            base,
            // Unknown for a log with records; a new one holds nothing dead.
            has_dead: log_len > DATA_HEADER_BYTES as u64,
            emptied: false,
            emptied_at: 0,
            snapshot_end: 0,
            #[cfg(test)]
            panic_after: None,
            #[cfg(test)]
            hold_next: None,
            #[cfg(test)]
            panic_in_rollback: false,
            #[cfg(test)]
            panic_in_publish: false,
            #[cfg(test)]
            rollback_flushes: 0,
            #[cfg(test)]
            fail_after_append: false,
            #[cfg(test)]
            writeback_before_rollback: false,
        };
        let mut tables = Tables::default();
        for entry in &manifest.tables {
            let shared = Arc::new(TableShared {
                id: entry.id,
                name: entry.name.clone(),
                ksize: AtomicU16::new(entry.ksize),
                published: ArcSwap::from_pointee(empty_published(&writer, entry.id)),
            });
            let idx = idxs.remove(&entry.id).flatten();
            writer.tables.insert(
                entry.id,
                TableWriter { ksize: entry.ksize, idx, shared: Arc::clone(&shared) },
            );
            writer.publish(entry.id);
            tables.insert(entry.name.clone(), shared);
        }
        // A log closed (or crashed) after a batch emptied every table, before the compaction
        // that followed: the next batch asks for it again.
        writer.note_if_emptied();
        metrics.opened(gen, writer.log_len());
        Ok(Self {
            root: root.to_path_buf(),
            writer: Mutex::new(writer),
            tables: ArcSwap::from_pointee(tables),
            frontier: ArcSwap::from_pointee(frontier),
            durability,
            registry: Mutex::new(manifest),
            compact: CompactShared::default(),
            metrics,
            sync_lock: Mutex::new(()),
            #[cfg(test)]
            rebuilt,
            #[cfg(test)]
            stat_slot: super::stats::slot(root),
            lock: Mutex::new(Some(lock)),
        })
    }

    /// Latch damage a read of table `table` found (its index, or the log record an index entry
    /// points at): the database can no longer be trusted to answer, so every later write and
    /// persist fails and the node stops. The log is not poisoned: the close seals it with every
    /// index marked stale (see `Writer::drop`), so the restart replays it whole, rebuilding a
    /// damaged index, and refuses a damaged log record rather than cut the log there.
    pub(super) fn read_damaged(&self, table: &str, e: &dyn std::fmt::Display) {
        self.durability.fail_damaged(format!("tndb: a read of table {table} found damage: {e}"));
    }

    /// Note damage a compaction found (an index entry or a log record that does not check out):
    /// the close marks every index stale, so the next open replays the log whole (see
    /// `Writer::drop`). Unlike damage a read found ([`Self::read_damaged`]), nothing is latched:
    /// no caller acted on the compaction's reads, and the rows it read are read again (and latch
    /// if damaged) by the node's own scans and gets; the database keeps writing until then.
    pub(super) fn compaction_damaged(&self) {
        self.writer.lock().damage_seen = true;
    }

    /// Bytes of the log not yet synced.
    pub(super) fn unsynced(&self) -> u64 {
        let frontier = self.frontier.load();
        frontier.end.saturating_sub(frontier.sync.synced.load(Ordering::Acquire))
    }

    /// The table named `name`, if open.
    pub(super) fn table(&self, name: &str) -> Option<Arc<TableShared>> {
        self.tables.load().get(name).cloned()
    }

    /// Table `id`'s key size (0 if not yet fixed), or `None` if the database has no such table. A
    /// scan of the tables (a dozen), which is fine for its one caller: a compaction's `Ksizes`
    /// asks at most once per table (it caches every fixed size).
    pub(super) fn table_ksize(&self, id: TableId) -> Option<u16> {
        self.tables.load().values().find(|table| table.id == id).map(|table| table.ksize())
    }

    /// Every table's id and key size (0 if not yet fixed).
    pub(super) fn table_ksizes(&self) -> Vec<(TableId, u16)> {
        self.tables.load().values().map(|table| (table.id, table.ksize())).collect()
    }

    /// What a compaction copies, taken under the write lock so it is one point between batches:
    /// every table's last publish, and the log's end. `None` when the writer stopped or no
    /// compaction is due: a requested one (`requested`: the least log it compacts) needs a log
    /// that long; an automatic one, [`Writer::wants_compaction`].
    pub(super) fn compaction_snapshot(&self, requested: Option<u64>) -> Option<Snapshot> {
        let mut writer = self.writer.lock();
        writer.check().ok()?;
        let end = writer.log.file_len();
        let due = match requested {
            Some(min) => end >= min,
            None => writer.wants_compaction(),
        };
        if !due {
            return None;
        }
        writer.snapshot_end = end;
        let tables = writer
            .tables
            .iter()
            .map(|(&id, table)| SnapTable {
                id,
                ksize: table.ksize,
                published: table.shared.published.load_full(),
            })
            .collect();
        Some(Snapshot {
            gen: writer.gen,
            gen_dir: writer.gen_dir.clone(),
            view: Arc::clone(&writer.view),
            alive: Arc::clone(&writer.alive),
            end,
            tables,
        })
    }

    /// Store the log's frontier as `writer` leaves it.
    pub(super) fn store_frontier(&self, writer: &Writer) {
        let end = writer.log.file_len();
        let sync = Arc::clone(&writer.sync);
        self.frontier.store(Arc::new(Frontier { sync, end, seq: writer.seq }));
    }

    /// The table named `name`, registered (in the manifest) if new.
    pub(super) fn open_table(&self, name: &str) -> eyre::Result<Arc<TableShared>> {
        if let Some(table) = self.table(name) {
            return Ok(table);
        }
        let mut manifest = self.registry.lock();
        if let Some(table) = self.table(name) {
            return Ok(table);
        }
        let mut updated = manifest.clone();
        let id = updated.add(name)?;
        // A latched database writes nothing more, a table's registration included.
        self.durability.check()?;
        self.write_manifest(&updated)?;
        *manifest = updated;
        let shared = {
            let mut writer = self.writer.lock();
            let shared = Arc::new(TableShared {
                id,
                name: name.to_string(),
                ksize: AtomicU16::new(0),
                published: ArcSwap::from_pointee(empty_published(&writer, id)),
            });
            writer
                .tables
                .insert(id, TableWriter { ksize: 0, idx: None, shared: Arc::clone(&shared) });
            writer.publish(id);
            shared
        };
        self.tables.rcu(|tables| {
            let mut tables = Tables::clone(tables);
            tables.insert(name.to_string(), Arc::clone(&shared));
            tables
        });
        Ok(shared)
    }

    /// Rewrite the manifest as `updated` (a table's registration or first row, under the registry
    /// lock). A failure latches (see [`Self::fail_setup`]).
    fn write_manifest(&self, updated: &Manifest) -> eyre::Result<()> {
        updated.write(&self.root).inspect_err(|e| self.fail_setup("rewrite the manifest", e))
    }

    /// Latch an I/O failure of a table's registration or first-row setup (the manifest rewrite,
    /// the index's creation), as any write failure of the node's own storage latches: the write
    /// that needed it fails, and so does every later write and persist, so the node stops. Nothing
    /// was appended, so the log's state is known and it is not poisoned: the close still seals
    /// it, with every index marked stale, as after damage a read found.
    fn fail_setup(&self, what: &str, e: &eyre::Report) {
        self.durability.fail_damaged(format!("tndb: {what}: {e:#}"));
    }

    /// Fix `table`'s key size at `ksize` if its first row is being written, or check `ksize`
    /// against it: the manifest records it and the table's index is created (and published, so a
    /// batch can be reverted to it), before the write lock is taken.
    pub(super) fn ensure_ksize(&self, table: &TableShared, ksize: usize) -> eyre::Result<()> {
        let fixed = table.ksize();
        if fixed != 0 {
            return check_ksize(table, fixed, ksize);
        }
        let ksize16 = u16::try_from(ksize)
            .map_err(|_| eyre::eyre!("tndb: a {ksize}-byte key is too long"))?;
        if ksize16 == 0 {
            bail!("tndb: table {} has an empty key", table.name);
        }
        let mut manifest = self.registry.lock();
        let fixed = table.ksize();
        if fixed != 0 {
            return check_ksize(table, fixed, ksize);
        }
        // A latched database writes nothing more, a table's first-row setup included.
        self.durability.check()?;
        let mut updated = manifest.clone();
        updated.set_ksize(table.id, ksize16);
        self.write_manifest(&updated)?;
        *manifest = updated;
        let (dir, header) = {
            let writer = self.writer.lock();
            (idx_dir(&writer.gen_dir, table.id), *writer.log.header())
        };
        let _ = fs::remove_dir_all(&dir);
        let mut idx = create_index(&dir, &header, ksize16)
            .inspect_err(|e| self.fail_setup(&format!("create table {}'s index", table.name), e))?;
        idx.publish();
        let mut writer = self.writer.lock();
        let entry = writer.tables.get_mut(&table.id).expect("a registered table");
        entry.ksize = ksize16;
        entry.idx = Some(idx);
        writer.publish(table.id);
        table.ksize.store(ksize16, Ordering::Release);
        Ok(())
    }

    /// Write `ops` as one batch: appended to the log, applied to the indexes and published, all
    /// or nothing. Returns the batch's number, or `None` if it changed nothing.
    pub(super) fn write(&self, ops: &[BatchOp<'_>]) -> eyre::Result<Option<u64>> {
        let wait_started = std::time::Instant::now();
        let mut writer = self.writer.lock();
        let held_from = std::time::Instant::now();
        let waited = held_from - wait_started;
        #[cfg(test)]
        super::stats::add(&super::stats::LOCK_WAITS, &super::stats::LOCK_WAIT_NS, waited);
        #[cfg(test)]
        if let Some(hold) = writer.hold_next.take() {
            std::thread::sleep(hold);
        }
        writer.check()?;
        writer.stamp_synced();
        let effective = match writer.effective(ops) {
            Ok(effective) => effective,
            Err(e) => {
                // A removal's lookup that met index damage poisoned that index: nothing was
                // written, and the database stops as after a read's damage.
                writer.latch_if_poisoned(ops, &e);
                return Err(e);
            }
        };
        if effective.is_empty() {
            return Ok(None);
        }
        let start = writer.log.file_len();
        let mut touched = Vec::new();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            writer.append_and_index(ops, &effective, &mut touched)
        }));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                writer.rollback(start, &touched, &self.sync_lock);
                return Err(e);
            }
            Err(panic) => {
                // Latched first, whatever the rollback then does.
                writer.stop("a write panicked".to_string());
                writer.rollback(start, &touched, &self.sync_lock);
                drop(writer);
                resume_unwind(panic);
            }
        }
        // A panic from here leaves the batch appended but not (wholly) published: latched.
        let latch = UnwindLatch(&self.durability, "a batch's publish panicked");
        // The append has grown a reopened log past its end, so the stamp the call before it could
        // not place lands now: from the first batch's acknowledgement (its sync round fsyncs the
        // growth, and with it the marker's page) a marker covers the log as it opened.
        writer.stamp_synced();
        // Numbered before it is published: a reader that sees it and then writes takes a later
        // number, so a persist covering the reader's write covers this one.
        writer.seq += 1;
        let seq = writer.seq;
        self.durability.set_seq(seq);
        #[cfg(test)]
        if std::mem::take(&mut writer.panic_in_publish) {
            panic!("injected panic while publishing a batch");
        }
        for &id in &touched {
            writer.publish(id);
        }
        writer.sync.growths.store(writer.log.growths(), Ordering::Release);
        self.store_frontier(&writer);
        drop(latch);
        let end = writer.log.file_len();
        let pending = end.saturating_sub(writer.sync.synced.load(Ordering::Acquire));
        writer.note_if_emptied();
        let compact = writer.wants_compaction();
        let held = held_from.elapsed();
        drop(writer);
        #[cfg(test)]
        super::stats::hold(held);
        #[cfg(test)]
        super::stats::log(self.stat_slot).hold(held);
        // The gauges from the frontier as it is now, not as this batch left it: a sync round (or
        // another writer, or a switch) may have moved it since the lock was let go, and values
        // captured under the lock would overwrite theirs with older ones.
        let now = self.frontier.load();
        let unsynced = now.end.saturating_sub(now.sync.synced.load(Ordering::Acquire));
        self.metrics.wrote(now.end, unsynced, waited, held);
        self.durability.wrote(pending);
        if compact {
            self.compact.request_auto();
        }
        Ok(Some(seq))
    }

    /// Test-only, for a simulated crash: release the database's lock, as a dead process's is.
    #[cfg(test)]
    pub(super) fn release_lock_for_crash(&self) {
        self.lock.lock().take();
    }

    /// Test-only: the compaction trigger's `base` (the log's length after the last compaction).
    #[cfg(test)]
    pub(super) fn base(&self) -> u64 {
        self.writer.lock().base
    }

    /// Test-only: whether the log may hold a dead row (see `Writer::has_dead`).
    #[cfg(test)]
    pub(super) fn has_dead(&self) -> bool {
        self.writer.lock().has_dead
    }

    /// Test-only: whether an emptying's reset is pending (see `Writer::emptied`).
    #[cfg(test)]
    pub(super) fn emptied(&self) -> bool {
        self.writer.lock().emptied
    }

    /// Test-only: the next rollback panics.
    #[cfg(test)]
    pub(super) fn panic_in_next_rollback(&self) {
        self.writer.lock().panic_in_rollback = true;
    }

    /// Test-only: the next batch's publish panics.
    #[cfg(test)]
    pub(super) fn panic_in_next_publish(&self) {
        self.writer.lock().panic_in_publish = true;
    }

    /// Test-only: the next batch fails once all its records are appended (it is rolled back).
    #[cfg(test)]
    pub(super) fn fail_next_batch(&self) {
        self.writer.lock().fail_after_append = true;
    }

    /// Test-only: the next batch fails once all its records are appended (it is rolled back),
    /// and its rollback first flushes the batch's bytes, as the system's writeback might have.
    #[cfg(test)]
    pub(super) fn fail_next_batch_after_writeback(&self) {
        let mut writer = self.writer.lock();
        writer.fail_after_append = true;
        writer.writeback_before_rollback = true;
    }

    /// Test-only: rolled-back batches whose zeroed bytes were flushed.
    #[cfg(test)]
    pub(super) fn rollback_flushes(&self) -> u64 {
        self.writer.lock().rollback_flushes
    }

    /// Test-only: the next batch holds the write lock at least `hold` (a stall).
    #[cfg(test)]
    pub(super) fn hold_next_batch(&self, hold: std::time::Duration) {
        self.writer.lock().hold_next = Some(hold);
    }

    /// Test-only: the next batch panics after appending `n` records.
    #[cfg(test)]
    pub(super) fn panic_next_batch_after(&self, n: usize) {
        self.writer.lock().panic_after = Some(n);
    }

    /// Test-only: the tables whose indexes the open rebuilt.
    #[cfg(test)]
    pub(super) fn rebuilt_on_open(&self) -> Vec<TableId> {
        self.rebuilt.clone()
    }

    /// Test-only: run `f` on table `id`'s working index.
    #[cfg(test)]
    pub(super) fn with_index<R>(
        &self,
        id: TableId,
        f: impl FnOnce(&mut BtreeIndex) -> R,
    ) -> Option<R> {
        self.writer.lock().tables.get_mut(&id)?.idx.as_mut().map(f)
    }
}

fn check_ksize(table: &TableShared, fixed: u16, ksize: usize) -> eyre::Result<()> {
    if ksize != fixed as usize {
        bail!("tndb: a {ksize}-byte key in table {} of {fixed}-byte keys", table.name);
    }
    Ok(())
}

#[cfg(test)]
impl Core {
    /// Test-only index damage: point `key`'s entry in table `id` at the record of row `other`,
    /// and publish it.
    pub(super) fn misindex(&self, id: TableId, key: &[u8], other: &[u8]) {
        let mut writer = self.writer.lock();
        let idx =
            writer.tables.get_mut(&id).and_then(|t| t.idx.as_mut()).expect("an indexed table");
        let pos = idx.load(other).expect("the other row");
        idx.save(key, pos).expect("save");
        writer.publish(id);
    }
}

/// The snapshot of a table with no index yet.
fn empty_published(writer: &Writer, table: TableId) -> Published {
    Published {
        index: None,
        view: Arc::clone(&writer.view),
        limit: 0,
        table,
        _alive: Arc::clone(&writer.alive),
    }
}

/// The largest record the log takes: far past a default pack's 16 MiB (a large consensus output
/// is one value), and safe for a local log no peer ever serves (reads borrow from the mapping).
pub(super) const MAX_RECORD: u32 = 1 << 30;

#[cfg(test)]
thread_local! {
    /// Test-only: the address space this thread's next log opens reserve, instead of
    /// [`LOG_MAP_RESERVE`] (a small one makes a log outgrow it, remapping).
    pub(super) static LOG_RESERVE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// The options every generation's log opens with.
fn log_options() -> MmapFileOptions {
    #[cfg(test)]
    let reserve = LOG_RESERVE.with(std::cell::Cell::get).unwrap_or(LOG_MAP_RESERVE);
    #[cfg(not(test))]
    let reserve = LOG_MAP_RESERVE;
    // `defer_seal_retire`: a sealed log stays sealed through the replay at open, and until the
    // first write, so a failed (or killed) open never leaves a damaged log to be cut as a torn
    // tail. `drop_cache_on_unclean_open`: an unclean log's replay reads what the disk holds, so it
    // stops at pages a failed writeback never wrote (a page cache that still holds them would
    // let new acknowledged batches follow a hole a power loss reveals).
    MmapFileOptions {
        reserve,
        max_record_size: Some(MAX_RECORD),
        defer_seal_retire: true,
        drop_cache_on_unclean_open: true,
        ..Default::default()
    }
}

/// Open (creating if absent) a generation's log.
pub(super) fn open_log(dir: &Path) -> eyre::Result<Log> {
    let opts = log_options();
    Ok(Pack::open_with(dir.join("log"), 0, false, PackCompression::None, PACK_VERSION, opts)?)
}

/// Open a generation's log read-only (an inspection): nothing is created, healed or sealed.
pub(super) fn open_log_read_only(dir: &Path) -> eyre::Result<Log> {
    let opts = log_options();
    Ok(Pack::open_with(dir.join("log"), 0, true, PackCompression::None, PACK_VERSION, opts)?)
}

#[cfg(test)]
thread_local! {
    /// Test-only: this thread's next index creation for a table's first row fails, as a full disk
    /// or an I/O error would (see `create_index`).
    pub(super) static FAIL_NEXT_INDEX_CREATE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Create a table's index at `dir`, for its first row (see `Core::ensure_ksize`).
fn create_index(dir: &Path, header: &DataHeader, ksize: u16) -> eyre::Result<BtreeIndex> {
    #[cfg(test)]
    if FAIL_NEXT_INDEX_CREATE.with(|fail| fail.replace(false)) {
        bail!("an index creation failure injected by a test");
    }
    Ok(BtreeIndex::open_btx_file(dir, header, ksize, false)?)
}

#[cfg(test)]
thread_local! {
    /// Test-only: this thread's next replay panics before it reads a record (an open that dies
    /// part way through its replay).
    pub(super) static PANIC_IN_REPLAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
thread_local! {
    /// Test-only: this thread's next creation of a database fails once its first generation's
    /// directory exists, before its log does (as a crash there would leave it).
    pub(super) static FAIL_GEN0_BUILD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The current generation of the database at `root`, opened: the newest one (an error if it does
/// not open), and the older ones, for the caller to delete once the newest has replayed (until
/// then, the only other copy); a new database gets generation 0, created durably. `has_manifest`:
/// the database has a table manifest, which is written only after generation 0 exists, so with
/// it and no generation the log was lost, and the open is refused rather than started empty.
fn open_current_gen(root: &Path, has_manifest: bool) -> eyre::Result<(u64, Log, Vec<u64>)> {
    let gens = list_gens(root)?;
    let Some(&current) = gens.last() else {
        if has_manifest {
            bail!(
                "tndb: {} has a table manifest but no generation (its log is lost); refusing to \
                 open it empty",
                root.display()
            );
        }
        // Built aside and renamed into place, so a failure (or a crash) part way leaves no
        // log-less generation 0, which every later open would refuse; a leftover build is
        // deleted by the next open (`remove_unfinished`).
        let build = compact_dir(root, 0);
        if let Err(e) = build_gen0(&build) {
            let _ = fs::remove_dir_all(&build);
            return Err(e);
        }
        let dir = gen_dir(root, 0);
        fs::rename(&build, &dir)
            .wrap_err_with(|| format!("tndb: rename {} into place", build.display()))?;
        crate::archive::data_file::fsync_directory(root)?;
        return Ok((0, open_log(&dir)?, Vec::new()));
    };
    let dir = gen_dir(root, current);
    if !dir.join("log").exists() {
        bail!("tndb: generation {current} of {} has no log", root.display());
    }
    // A generation's log holds its header before the generation exists (generation 0 is built
    // aside; a compaction syncs its log before the rename), so one with none is damage: refuse it
    // rather than let the open re-initialise it as an empty log.
    if !log_has_header(&dir.join("log"))? {
        bail!(
            "tndb: generation {current} of {} has a log with no header (damaged)",
            root.display()
        );
    }
    let log = open_log(&dir)
        .wrap_err_with(|| format!("tndb: open generation {current} of {}", root.display()))?;
    let older = gens.into_iter().filter(|&n| n < current).collect();
    Ok((current, log, older))
}

/// True if the log file at `path` holds a header: it is at least a header long and that span is not
/// all zeros.
fn log_has_header(path: &Path) -> eyre::Result<bool> {
    use std::io::Read as _;
    let mut header = [0_u8; DATA_HEADER_BYTES];
    let mut file =
        fs::File::open(path).wrap_err_with(|| format!("tndb: open {}", path.display()))?;
    match file.read_exact(&mut header) {
        Ok(()) => Ok(header.iter().any(|&b| b != 0)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e).wrap_err_with(|| format!("tndb: read {}", path.display())),
    }
}

/// Build an empty first generation in `dir`: its log, committed and closed, and the directory
/// synced.
fn build_gen0(dir: &Path) -> eyre::Result<()> {
    fs::create_dir(dir).wrap_err_with(|| format!("tndb: create {}", dir.display()))?;
    #[cfg(test)]
    if FAIL_GEN0_BUILD.with(|fail| fail.replace(false)) {
        bail!("tndb: injected failure building generation 0");
    }
    let log = open_log(dir)?;
    log.commit()?;
    drop(log);
    Ok(crate::archive::data_file::fsync_directory(dir)?)
}

/// The index directories of tables the manifest does not have (or whose key size it has not
/// fixed).
pub(super) fn orphan_indexes(gen_path: &Path, manifest: &Manifest) -> eyre::Result<Vec<PathBuf>> {
    let mut orphans = Vec::new();
    for entry in fs::read_dir(gen_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(id) = name.to_str().and_then(|n| n.strip_prefix("idx-")) else { continue };
        let known = id
            .parse::<TableId>()
            .ok()
            .is_some_and(|id| manifest.tables.iter().any(|t| t.id == id && t.ksize > 0));
        if !known {
            orphans.push(entry.path());
        }
    }
    Ok(orphans)
}

/// A table's live rows found by a replay: key to record position.
type Rows = BTreeMap<Vec<u8>, u64>;

/// One record of a batch still being read in a replay.
pub(super) enum Pending {
    Put(Vec<u8>, u64),
    Remove(Vec<u8>),
    Clear,
}

/// Where a replay applies each whole batch's records.
pub(super) trait ReplaySink {
    /// True if the replay applies table `id`'s records.
    fn wants(&self, id: TableId) -> bool;
    /// Apply one record of a whole batch to table `id`.
    fn apply(&mut self, id: TableId, op: Pending) -> eyre::Result<()>;
}

/// A replay into memory: the live `key → position` map of each wanted table.
pub(super) struct RowsSink(pub(super) BTreeMap<TableId, Rows>);

impl RowsSink {
    /// Collect the rows of the tables `ids`.
    pub(super) fn wanting(ids: impl IntoIterator<Item = TableId>) -> Self {
        Self(ids.into_iter().map(|id| (id, BTreeMap::new())).collect())
    }
}

impl ReplaySink for RowsSink {
    fn wants(&self, id: TableId) -> bool {
        self.0.contains_key(&id)
    }

    fn apply(&mut self, id: TableId, op: Pending) -> eyre::Result<()> {
        let table = self.0.get_mut(&id).expect("only wanted tables are applied");
        match op {
            Pending::Put(key, pos) => {
                table.insert(key, pos);
            }
            Pending::Remove(key) => {
                table.remove(&key);
            }
            Pending::Clear => table.clear(),
        }
        Ok(())
    }
}

/// A replay straight into fresh indexes (recovery's rebuild), so no table's rows are held in
/// memory. An index still here when the sink drops (a failed recovery) is removed, so the next
/// open rebuilds it again.
struct IndexSink(BTreeMap<TableId, BtreeIndex>);

impl ReplaySink for IndexSink {
    fn wants(&self, id: TableId) -> bool {
        self.0.contains_key(&id)
    }

    fn apply(&mut self, id: TableId, op: Pending) -> eyre::Result<()> {
        let idx = self.0.get_mut(&id).expect("only wanted tables are applied");
        match op {
            Pending::Put(key, pos) => idx.save(&key, pos)?,
            Pending::Remove(key) => {
                idx.remove(&key)?;
            }
            Pending::Clear => idx.clear()?,
        }
        Ok(())
    }
}

impl Drop for IndexSink {
    fn drop(&mut self) {
        for idx in self.0.values_mut() {
            idx.set_remove_on_drop();
        }
    }
}

/// What a replay found: where the log's whole batches end, and the tear it stopped at, if any.
pub(super) struct Replayed {
    pub(super) end: u64,
    pub(super) tear: Option<String>,
}

/// Replay the log (read through a cloned handle; nothing is changed) into `sink`, for the tables
/// it wants, and find where the log's whole batches end. Every record is checked (an unknown
/// table or kind, or a malformed record, is an error); a batch applies only once its flagged last
/// record reads back intact, so a crash keeps no part of a partly written batch. A sealed log must
/// read whole, in whole batches; an unclean one may end in a tear (a record that fails its CRC or
/// runs past the end), but not below its commit marker. A read the disk fails is an error, never a
/// tear: acknowledged batches may follow it.
pub(super) fn replay(
    log: &Log,
    manifest: &Manifest,
    sink: &mut impl ReplaySink,
) -> eyre::Result<Replayed> {
    #[cfg(test)]
    if PANIC_IN_REPLAY.with(|panic| panic.replace(false)) {
        panic!("injected panic in a replay");
    }
    let ksizes: BTreeMap<TableId, u16> = manifest.tables.iter().map(|t| (t.id, t.ksize)).collect();
    let mut pending: Vec<(TableId, Pending)> = Vec::new();
    let mut in_batch = false;
    let mut end = DATA_HEADER_BYTES as u64;
    let mut tear = None;
    let mut iter = log.raw_iter()?;
    loop {
        let start = iter.logical_position();
        let payload = match iter.next_raw() {
            None => break,
            Some(Err(FetchError::IO(e))) if e.kind() != std::io::ErrorKind::UnexpectedEof => {
                return Err(eyre::Report::new(e).wrap_err(format!(
                    "tndb: reading the log at {start} failed (a read error, not a torn tail)"
                )));
            }
            Some(Err(e)) => {
                tear = Some(format!("a bad record at {start} ({e})"));
                break;
            }
            Some(Ok(payload)) => payload,
        };
        let record = record::parse(payload, |id| ksizes.get(&id).copied())
            .wrap_err_with(|| format!("tndb: the record at {start}"))?;
        if sink.wants(record.table) {
            pending.push((
                record.table,
                match record.op {
                    Op::Put(key) => Pending::Put(key.to_vec(), start),
                    Op::Remove(key) => Pending::Remove(key.to_vec()),
                    Op::Clear => Pending::Clear,
                },
            ));
        }
        in_batch = !record.end;
        if record.end {
            for (id, op) in pending.drain(..) {
                sink.apply(id, op)?;
            }
            end = iter.logical_position();
        }
    }
    if !log.opened_unclean() {
        if let Some(tear) = &tear {
            bail!(
                "tndb: the log was closed cleanly but does not replay to its end: {tear} (corrupt)"
            );
        }
        if in_batch {
            bail!("tndb: the log was closed cleanly but ends inside a batch after {end} (corrupt)");
        }
    }
    if log.opened_unclean() && log.committed_end().is_some_and(|committed| end < committed) {
        bail!(
            "tndb: the log's whole batches end at {end}, below its commit marker{}",
            tear.map(|tear| format!(", at {tear}")).unwrap_or_default()
        );
    }
    Ok(Replayed { end, tear })
}

/// Rebuild the indexes of the tables `rebuild` selects from the log, after cutting an unclean
/// log back to its last whole batch. The log is not changed until it has replayed whole and been
/// checked. The indexes being rebuilt (stale, or they would not be) are refilled as it replays,
/// batch by batch, so no rows are held in memory; a failed recovery removes them, so the next
/// open rebuilds them again.
fn recover(
    log: &mut Log,
    manifest: &Manifest,
    gen_path: &Path,
    header: &DataHeader,
    idxs: &mut BTreeMap<TableId, Option<BtreeIndex>>,
    rebuild: impl Fn(TableId) -> bool,
) -> eyre::Result<()> {
    let mut sink = IndexSink(BTreeMap::new());
    for entry in manifest.tables.iter().filter(|t| t.ksize > 0 && rebuild(t.id)) {
        let dir = idx_dir(gen_path, entry.id);
        // Close the stale index before its directory goes.
        idxs.insert(entry.id, None);
        let _ = fs::remove_dir_all(&dir);
        sink.0.insert(entry.id, BtreeIndex::open_btx_file(&dir, header, entry.ksize, false)?);
    }
    let Replayed { end, tear } = replay(log, manifest, &mut sink)?;
    let file_len = log.file_len();
    if end < file_len {
        // An unclean log runs to its capacity, whose last bytes hold the commit marker: not data,
        // and there after every crash. Only other bytes past the last whole batch are reported.
        let data_end = file_len.saturating_sub(COMMIT_MARKER_LEN).max(end);
        let data_followed =
            log.read_bytes(end, data_end).ok().is_none_or(|tail| tail.iter().any(|&b| b != 0));
        if let Some(cut) = log.rewind_to(end) {
            // Bytes followed the last whole batch. After a power loss that is normal (a round's
            // pages reach the disk in any order, so unacknowledged batches can survive past a
            // torn page); otherwise it can mean damage to acknowledged data. Say what was cut.
            if data_followed {
                tracing::warn!(target: "tndb", "recovery cut the log at {end}, its last whole batch, after {}; non-zero bytes followed up to {} (commit marker: {:?})", tear.as_deref().unwrap_or("a partial batch"), cut.end, log.committed_end());
            }
            // The open-time sync made the old tail durable, and its zeros are only in memory:
            // flush them now, as a rollback does, or a power loss before writeback could bring
            // stale batches back behind the ones written next.
            log.view().msync(cut.start, cut.end)?;
        }
    }
    let len = log.file_len();
    for (id, mut idx) in std::mem::take(&mut sink.0) {
        idx.set_data_file_length(len);
        idx.sync()?;
        idx.mark_consistent();
        idxs.insert(id, Some(idx));
    }
    log.mark_consistent();
    Ok(())
}
