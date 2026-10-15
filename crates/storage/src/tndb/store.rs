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
        data_file::{MapView, MmapFileOptions},
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

    /// Read the published value for `key` and map its bytes with `decode`, or `None`.
    pub(super) fn get_with<R>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.published.load().get_with(key, decode)
    }

    /// True if the published state holds `key` (a damaged index is an error, not "absent").
    pub(super) fn contains(&self, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.published.load().contains(key)?)
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
                scan_error(&self.name, &e);
                None
            }
        });
        TableScan { published, cursor, name: self.name.clone(), _core: core }
    }

    /// Map the single `(key, value)` a scan of `kind` lands on first with `f` (a direct seek).
    pub(super) fn first_with<R>(
        &self,
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
                scan_error(&self.name, &e);
                None
            }
        }
    }
}

/// Log a scan or seek ended by an error: `DBIter` cannot carry it, but a damaged table must not
/// pass for a shorter one in silence.
fn scan_error(table: &str, e: &FetchError) {
    tracing::error!(target: "tndb", "scan of table {table} ended by an error: {e}");
}

/// A lazy, key-ordered scan of a published snapshot.
pub(super) struct TableScan {
    published: Arc<Published>,
    cursor: Option<BtreeCursor>,
    name: String,
    /// Keeps the database (and so the current generation's files) open while the scan lives.
    _core: Arc<Core>,
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
                scan_error(&self.name, &e);
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
    /// How far the log's commit marker has been stamped.
    stamped: u64,
    /// Set when the writer can no longer write (a batch it could not take back): every later
    /// write fails until a restart.
    failed: Option<String>,
    durability: Arc<Durability>,
    /// When the log compacts automatically.
    config: CompactionConfig,
    /// The log's length right after the compaction that made this generation (0 if none did):
    /// the log compacts once it has doubled since.
    base: u64,
    /// A row may have been made dead (overwritten, removed or cleared) since the last compaction
    /// (assumed at open, unless the log is empty): a log that only grows does not compact.
    has_dead: bool,
    /// A batch left every table empty while the log held at least [`EMPTIED_MIN_BYTES`] (the
    /// epoch tables are all cleared at an epoch's end): the log compacts at once, whatever its
    /// growth threshold, since nothing live is left to copy. Kept until a compaction switches, so
    /// writes that resume meanwhile do not cancel it.
    emptied: bool,
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
        if self.durability.is_failed() {
            // A failed sync leaves the log's state on disk unknown: never seal it as clean.
            self.log.poison();
        }
        self.durability.check()
    }

    /// Stamp the log's commit marker up to what the commit thread has synced (best-effort and
    /// unsynced, as a commit marker always is), lazily, under the write lock.
    fn stamp_synced(&mut self) {
        let synced = self.sync.synced.load(Ordering::Acquire);
        if synced > self.stamped {
            self.log.note_synced(synced);
            self.log.stamp_commit_marker_at(synced);
            self.stamped = synced;
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
            let _one_sync = sync_lock.lock();
            if let Err(e) = self.view.msync(start, end) {
                self.stop(format!("flush a failed batch's rollback: {e}"));
            }
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
                    self.stop(format!("table {id}'s index is poisoned"));
                }
            }
        }
        if self.log.is_failed() {
            self.stop("the log failed".to_string());
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
    /// `emptied`). O(tables), and only once the log is that long and holds dead rows.
    fn note_if_emptied(&mut self) {
        if !self.emptied
            && self.has_dead
            && self.log.file_len() >= EMPTIED_MIN_BYTES
            && self.tables.values().all(|table| table.idx.as_ref().is_none_or(BtreeIndex::is_empty))
        {
            self.emptied = true;
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
        self.stamped = self.log.file_len();
        self.base = base;
        self.has_dead = has_dead;
        self.emptied = false;
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
    /// commit (or follows a failed sync) leaves every index marked stale instead.
    fn drop(&mut self) {
        let committed =
            self.failed.is_none() && !self.durability.is_failed() && self.log.commit().is_ok();
        if committed {
            self.log.stamp_commit_marker();
        } else {
            self.log.poison();
        }
        let len = if committed { self.log.file_len() } else { u64::MAX };
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
        let (gen, mut log, older) = open_current_gen(root)?;
        let gen_path = gen_dir(root, gen);
        if !manifest_lost {
            remove_orphan_indexes(&gen_path, &manifest)?;
        }
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
        if log.opened_unclean() || !stale.is_empty() || manifest_lost {
            let all = log.opened_unclean();
            let rebuild = |id: TableId| all || stale.contains(&id);
            #[cfg(test)]
            {
                rebuilt = manifest.tables.iter().map(|t| t.id).filter(|&id| rebuild(id)).collect();
            }
            recover(&mut log, &manifest, &gen_path, &header, &mut idxs, rebuild)?;
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
        let stamped = log.file_len();
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
            stamped,
            failed: None,
            durability: Arc::clone(&durability),
            config,
            base,
            // Unknown for a log with records; a new one holds nothing dead.
            has_dead: stamped > DATA_HEADER_BYTES as u64,
            emptied: false,
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
        let writer = self.writer.lock();
        writer.check().ok()?;
        let end = writer.log.file_len();
        let due = match requested {
            Some(min) => end >= min,
            None => writer.wants_compaction(),
        };
        if !due {
            return None;
        }
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
        updated.write(&self.root)?;
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
        let mut updated = manifest.clone();
        updated.set_ksize(table.id, ksize16);
        updated.write(&self.root)?;
        *manifest = updated;
        let (dir, header) = {
            let writer = self.writer.lock();
            (idx_dir(&writer.gen_dir, table.id), *writer.log.header())
        };
        let _ = fs::remove_dir_all(&dir);
        let mut idx = BtreeIndex::open_btx_file(&dir, &header, ksize16, false)?;
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
        let effective = writer.effective(ops)?;
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
        self.metrics.wrote(end, pending, waited, held);
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
    MmapFileOptions { reserve, max_record_size: Some(MAX_RECORD), ..Default::default() }
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
    /// Test-only: this thread's next creation of a database fails once its first generation's
    /// directory exists, before its log does (as a crash there would leave it).
    pub(super) static FAIL_GEN0_BUILD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The current generation of the database at `root`, opened: the newest one (an error if it does
/// not open), and the older ones, for the caller to delete once the newest has replayed (until
/// then, the only other copy); a new database gets generation 0, created durably.
fn open_current_gen(root: &Path) -> eyre::Result<(u64, Log, Vec<u64>)> {
    let gens = list_gens(root)?;
    let Some(&current) = gens.last() else {
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
    let log = open_log(&dir)
        .wrap_err_with(|| format!("tndb: open generation {current} of {}", root.display()))?;
    let older = gens.into_iter().filter(|&n| n < current).collect();
    Ok((current, log, older))
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

/// Delete the index directories of tables the manifest does not have (or whose key size it has
/// not fixed): derived data with nothing to index.
fn remove_orphan_indexes(gen_path: &Path, manifest: &Manifest) -> eyre::Result<()> {
    for entry in fs::read_dir(gen_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(id) = name.to_str().and_then(|n| n.strip_prefix("idx-")) else { continue };
        let known = id
            .parse::<TableId>()
            .ok()
            .is_some_and(|id| manifest.tables.iter().any(|t| t.id == id && t.ksize > 0));
        if !known {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
    Ok(())
}

/// A table's live rows found by a replay: key to record position.
type Rows = BTreeMap<Vec<u8>, u64>;

/// One record of a batch still being read in a replay.
enum Pending {
    Put(Vec<u8>, u64),
    Remove(Vec<u8>),
    Clear,
}

/// Replay the log (read through a cloned handle; nothing is changed) for the tables `want`
/// selects: their live `key → position` maps, and where the log's whole batches end. Every record
/// is checked (an unknown table or kind, or a malformed record, is an error); a batch applies only
/// once its flagged last record reads back intact, so a crash keeps no part of a partly written
/// batch. A sealed log must read whole; an unclean one may end in a tear, but not below its
/// commit marker.
pub(super) fn replay(
    log: &Log,
    manifest: &Manifest,
    want: &impl Fn(TableId) -> bool,
) -> eyre::Result<(BTreeMap<TableId, Rows>, u64)> {
    let ksizes: BTreeMap<TableId, u16> = manifest.tables.iter().map(|t| (t.id, t.ksize)).collect();
    let mut rows: BTreeMap<TableId, Rows> =
        ksizes.keys().filter(|&&id| want(id)).map(|&id| (id, BTreeMap::new())).collect();
    let mut pending: Vec<(TableId, Pending)> = Vec::new();
    let mut end = DATA_HEADER_BYTES as u64;
    let mut torn = false;
    let mut iter = log.raw_iter()?;
    loop {
        let start = iter.logical_position();
        let payload = match iter.next_raw() {
            None => break,
            Some(Err(_)) => {
                torn = true;
                break;
            }
            Some(Ok(payload)) => payload,
        };
        let record = record::parse(payload, |id| ksizes.get(&id).copied())
            .wrap_err_with(|| format!("tndb: the record at {start}"))?;
        if rows.contains_key(&record.table) {
            pending.push((
                record.table,
                match record.op {
                    Op::Put(key) => Pending::Put(key.to_vec(), start),
                    Op::Remove(key) => Pending::Remove(key.to_vec()),
                    Op::Clear => Pending::Clear,
                },
            ));
        }
        if record.end {
            for (id, op) in pending.drain(..) {
                let table = rows.get_mut(&id).expect("only wanted tables are pending");
                match op {
                    Pending::Put(key, pos) => {
                        table.insert(key, pos);
                    }
                    Pending::Remove(key) => {
                        table.remove(&key);
                    }
                    Pending::Clear => table.clear(),
                }
            }
            end = iter.logical_position();
        }
    }
    if torn && !log.opened_unclean() {
        bail!("tndb: the log was closed cleanly but does not replay to its end (corrupt)");
    }
    if log.opened_unclean() && log.committed_end().is_some_and(|committed| end < committed) {
        bail!("tndb: the log's whole batches end at {end}, below its commit marker");
    }
    Ok((rows, end))
}

/// Rebuild the indexes of the tables `rebuild` selects from the log, after cutting an unclean
/// log back to its last whole batch. Changes nothing until the whole log has replayed and checked.
fn recover(
    log: &mut Log,
    manifest: &Manifest,
    gen_path: &Path,
    header: &DataHeader,
    idxs: &mut BTreeMap<TableId, Option<BtreeIndex>>,
    rebuild: impl Fn(TableId) -> bool,
) -> eyre::Result<()> {
    let (rows, end) = replay(log, manifest, &|id| {
        rebuild(id) && manifest.tables.iter().any(|t| t.id == id && t.ksize > 0)
    })?;
    if end < log.file_len() {
        log.rewind_to(end);
    }
    let len = log.file_len();
    for (id, rows) in rows {
        let entry = manifest.tables.iter().find(|t| t.id == id).expect("a manifest table");
        let dir = idx_dir(gen_path, id);
        idxs.insert(id, None);
        let _ = fs::remove_dir_all(&dir);
        let mut idx = BtreeIndex::open_btx_file(&dir, header, entry.ksize, false)?;
        idx.rebuild_from(rows)?;
        idx.set_data_file_length(len);
        idx.sync()?;
        idx.mark_consistent();
        idxs.insert(id, Some(idx));
    }
    log.mark_consistent();
    Ok(())
}
