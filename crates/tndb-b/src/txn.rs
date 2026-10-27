//! Transactions: read-only ones ([`RoTxn`]) on a published snapshot, and the single write
//! transaction ([`RwTxn`]), which builds the next state in pages no reader can reach and
//! publishes it at [`RwTxn::commit`]. Tables are named by [`Table`] handles, checked against
//! the catalog of the state they are used in; a read-only transaction's handle is checked once
//! per snapshot, which every transaction on it shares ([`TableRoots`]).

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

#[cfg(test)]
use crate::env::CrashAt;
use crate::{
    catalog::{self, TableRecord},
    env::{EnvInner, Snapshot, SyncMode, Writer},
    error::{Error, Result},
    freelist,
    latch::UnwindLatch,
    meta::Meta,
    page,
    tree::{
        cursor::{self, Cursor, Entry, KeyTail, State, TreeCursor},
        read,
        write::{self, PutKind},
        TreeRoot,
    },
};

/// A handle on one table: its name and its id. A committed table's id is never given to another
/// table over the file's life, so a handle of a table that was dropped (and perhaps re-created
/// under the same name) is refused with [`Error::StaleTable`] rather than reaching the new table;
/// a handle used in a transaction whose state predates the table is refused with
/// [`Error::TableNotFound`]. The id alone does not name a table: the id of a table created in a
/// transaction that aborted goes to the next table created, under its own name, and a handle of
/// another file may carry it too. A handle is the table with its id and its name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Table {
    id: u32,
    name: Arc<str>,
}

impl Table {
    fn new(id: u32, name: &str) -> Self {
        Self { id, name: Arc::from(name) }
    }

    /// The table's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `self == other`, with the shortcut of a clone: handles that share the name's allocation
    /// skip the name compare.
    #[inline]
    fn is(&self, other: &Self) -> bool {
        self.id == other.id && (Arc::ptr_eq(&self.name, &other.name) || self.name == other.name)
    }

    /// Test-only: the table's id.
    #[cfg(test)]
    pub(crate) fn id(&self) -> u32 {
        self.id
    }
}

// A read-only transaction may be shared between threads.
const _: fn() = || {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<RoTxn>();
    send_sync::<RoCursor>();
    send_sync::<TxnCursor>();
    send_sync::<Table>();
};

// A read-only transaction is two `Arc`s, cheap to begin and to put in the `Arc` that
// `RoTxn::cursor_owned` takes: the table roots it resolves live in its snapshot (`TableRoots`).
const _: () = assert!(std::mem::size_of::<RoTxn>() == 2 * std::mem::size_of::<usize>());

/// Figures about one table in a transaction's state, read from its tree's root record: O(1),
/// with no page read. An empty table has every figure zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableStat {
    /// The number of entries.
    pub entries: u64,
    /// The height of the table's B+tree (0: empty).
    pub height: u32,
    /// The branch pages of its tree: every level above the leaves.
    pub branch_pages: u32,
    /// The leaf pages of its tree.
    pub leaf_pages: u32,
    /// The pages of the overflow runs holding its values too large for a leaf.
    pub overflow_pages: u32,
}

impl TableStat {
    fn of(root: &TreeRoot) -> Self {
        Self {
            entries: root.entries,
            height: root.height,
            branch_pages: root.branch_pages,
            leaf_pages: root.leaf_pages,
            overflow_pages: root.overflow_pages,
        }
    }
}

/// The reads both kinds of transaction offer, each on the transaction's own state. Damage a
/// read finds is [`Error::Corrupt`] and latches the environment (later writes fail; reads
/// continue).
pub trait ReadTxn {
    /// The table named `name`, if it exists in this state. [`Error::InvalidTableName`] unless
    /// the name is 1..=255 bytes.
    fn open_table(&self, name: &str) -> Result<Option<Table>>;

    /// The value stored under `key` in `table`, borrowed from the mapping for as long as the
    /// transaction lives. A value held in an overflow run is borrowed from the run's pages; a run
    /// of more than one page passes its full CRC first, so such a get reads every byte of it.
    fn get<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>>;

    /// A new, unset cursor over `table`: ordered access to its entries in both directions,
    /// borrowed from the transaction for as long as it lives (see [`Cursor`]).
    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>>;

    /// The id of the state this transaction reads: a read-only transaction's snapshot, or the
    /// id the write transaction commits as.
    fn txn_id(&self) -> u64;

    /// Figures about `table` in this state.
    fn table_stat(&self, table: &Table) -> Result<TableStat>;
}

/// Check `table` against the record `found` under its name in a state whose next table id is
/// `next_table_id`: the record of the handle's id, else [`Error::TableNotFound`] if the handle's
/// table was created after this state, else [`Error::StaleTable`] (it was dropped, perhaps
/// re-created).
fn resolve(found: Option<TableRecord>, table: &Table, next_table_id: u32) -> Result<TableRecord> {
    match found {
        Some(record) if record.id == table.id => Ok(record),
        _ if table.id >= next_table_id => Err(Error::TableNotFound(table.name().to_string())),
        _ => Err(Error::StaleTable(table.name().to_string())),
    }
}

// Test-only counter, debug builds only: the benchmark is compiled with the test configuration,
// and a thread-local update on a timed path would show in its release timings.
#[cfg(all(test, debug_assertions))]
thread_local! {
    /// Test-only: how many times a [`RoTxn`] resolved a handle through the catalog.
    pub(crate) static CATALOG_RESOLVES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many tables a snapshot's [`TableRoots`] remembers.
const SNAPSHOT_TABLES: usize = 32;

/// The tree roots of the tables resolved in one snapshot, shared by every [`RoTxn`] on it
/// ([`RoTxn::root`]). The snapshot's catalog never changes, so a handle resolved once resolves
/// the same for the snapshot's life, and each commit publishes a new snapshot with an empty set.
/// Only a resolved handle is remembered: a refused one is checked again each time. A handle's
/// slot is the first one from `id % SNAPSHOT_TABLES` on (wrapping) that no other handle holds,
/// and a slot is never emptied, so a lookup stops at the first empty slot. A hit is
/// [`Table::is`]: the id, then the name (a handle of another file, or one of a table an aborted
/// transaction created, may carry the same id). Once every slot holds another handle, a handle
/// resolves through the catalog on each use.
#[derive(Default)]
pub(crate) struct TableRoots([OnceLock<(Table, TreeRoot)>; SNAPSHOT_TABLES]);

impl std::fmt::Debug for TableRoots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let known = self.0.iter().filter(|slot| slot.get().is_some()).count();
        f.debug_struct("TableRoots").field("known", &known).finish()
    }
}

impl TableRoots {
    /// The slot a lookup of `table` starts from.
    #[inline]
    fn home(table: &Table) -> usize {
        table.id as usize % SNAPSHOT_TABLES
    }

    /// `table`'s root if its first slot holds it: the hit path of [`RoTxn::root`].
    #[inline]
    fn at_home(&self, table: &Table) -> Option<TreeRoot> {
        match self.0[Self::home(table)].get() {
            Some((known, root)) if known.is(table) => Some(*root),
            _ => None,
        }
    }

    /// `table`'s root, or else the first empty slot of its probe (`None`: every slot holds
    /// another handle).
    fn find(&self, table: &Table) -> std::result::Result<TreeRoot, Option<usize>> {
        let home = Self::home(table);
        for i in 0..SNAPSHOT_TABLES {
            let at = (home + i) % SNAPSHOT_TABLES;
            match self.0[at].get() {
                Some((known, root)) if known.is(table) => return Ok(*root),
                Some(_) => {}
                None => return Err(Some(at)),
            }
        }
        Err(None)
    }

    /// Remember `table`'s `root` in the first empty slot from `from` on, the one [`Self::find`]
    /// gave. A racing reader may take that slot first: for this handle, which ends the search, or
    /// for another, which moves it to the next.
    fn remember(&self, from: usize, table: &Table, root: TreeRoot) {
        for i in 0..SNAPSHOT_TABLES {
            let slot = &self.0[(from + i) % SNAPSHOT_TABLES];
            if slot.get_or_init(|| (table.clone(), root)).0.is(table) {
                return;
            }
        }
    }
}

/// A read-only transaction: one committed state of every table, unaffected by later commits.
/// The state's pages stay intact while it lives.
pub struct RoTxn {
    inner: Arc<EnvInner>,
    snapshot: Arc<Snapshot>,
}

impl std::fmt::Debug for RoTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoTxn").field("txn", &self.snapshot.meta.txn).finish_non_exhaustive()
    }
}

impl RoTxn {
    pub(crate) fn new(inner: Arc<EnvInner>, snapshot: Arc<Snapshot>) -> Self {
        Self { inner, snapshot }
    }

    /// Run a read, latching damage it finds.
    fn read<'t, T>(&'t self, op: impl FnOnce(&'t Snapshot) -> Result<T>) -> Result<T> {
        op(&self.snapshot).inspect_err(|e| self.inner.latch.latch_error(e))
    }

    /// `table`'s tree root in the snapshot (see [`resolve`]): from the snapshot's
    /// [`TableRoots`] once a reader on it resolved the handle, else from the catalog
    /// ([`Self::resolve_root`]).
    #[inline]
    fn root(&self, table: &Table) -> Result<TreeRoot> {
        match self.snapshot.tables.at_home(table) {
            Some(root) => Ok(root),
            None => self.resolve_root(table),
        }
    }

    /// [`Self::root`] past the handle's first slot: the rest of its probe, else the catalog,
    /// whose answer is remembered if it resolved and a slot is free.
    #[inline(never)]
    fn resolve_root(&self, table: &Table) -> Result<TreeRoot> {
        let tables = &self.snapshot.tables;
        let free = match tables.find(table) {
            Ok(root) => return Ok(root),
            Err(free) => free,
        };
        #[cfg(all(test, debug_assertions))]
        CATALOG_RESOLVES.with(|n| n.set(n.get() + 1));
        let meta = &self.snapshot.meta;
        let found = catalog::get(&*self.snapshot, &meta.catalog, table.name())?;
        let root = resolve(found, table, meta.next_table_id)?.root;
        if let Some(from) = free {
            tables.remember(from, table, root);
        }
        Ok(root)
    }

    /// A new, unset cursor over `table` that owns a share of this transaction (see
    /// [`RoCursor`]): it has no lifetime, and keeps the snapshot readable after every other
    /// handle of the transaction drops.
    #[inline]
    pub fn cursor_owned(self: &Arc<Self>, table: &Table) -> Result<RoCursor> {
        let root = self.read(|_| self.root(table))?;
        Ok(RoCursor { txn: Arc::clone(self), root, tree: TreeCursor::default() })
    }
}

/// A cursor over one table of a read-only transaction that owns a share of the transaction
/// (created by [`RoTxn::cursor_owned`] on an `Arc<RoTxn>`). It has no lifetime: it can be stored,
/// sent to another thread, and outlive every other handle of the transaction, whose snapshot
/// stays readable (and its pages unreused) until the last share drops. Its moves, states and
/// prefix steps are a [`Cursor`]'s; the entries it returns, overflow values included, borrow the
/// cursor until its next call. Where a [`Cursor`] keeps the page of its leaf, each `next` and
/// `prev` fetches it again by page number, without the per-fetch checks the page passed when the
/// cursor entered it (a snapshot's pages do not change while it lives); every leaf it enters gets
/// those checks.
///
/// Damage a move finds is [`Error::Corrupt`], leaves the cursor unset and latches the environment
/// (later writes fail; reads continue).
pub struct RoCursor {
    txn: Arc<RoTxn>,
    /// The table's tree in the transaction's snapshot.
    root: TreeRoot,
    tree: TreeCursor,
}

impl std::fmt::Debug for RoCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoCursor")
            .field("txn", &self.txn.snapshot.meta.txn)
            .field("state", &self.tree.state())
            .finish_non_exhaustive()
    }
}

impl RoCursor {
    /// Test-only: where the cursor is.
    #[cfg(test)]
    pub(crate) fn state(&self) -> cursor::State {
        self.tree.state()
    }

    /// Run a move over the snapshot, latching damage it finds.
    fn moved<'c, T>(
        &'c mut self,
        op: impl FnOnce(&mut TreeCursor, &'c Snapshot, &TreeRoot) -> Result<T>,
    ) -> Result<T> {
        let Self { txn, root, tree } = self;
        let txn: &'c RoTxn = txn;
        op(tree, &txn.snapshot, root).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.first(store, root))
    }

    /// Move to the last entry and return it (`None` for an empty table).
    pub fn last(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.last(store, root))
    }

    /// Move to the next entry and return it; see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.step(true)
    }

    /// Move to the previous entry and return it; see [`Cursor::prev`].
    pub fn prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.step(false)
    }

    /// [`Self::next`] (`forward`) or [`Self::prev`]: from an entry, from the page of its leaf
    /// fetched again without the per-fetch checks it passed when the cursor entered it
    /// ([`Snapshot::page_again`]).
    fn step(&mut self, forward: bool) -> Result<Option<Entry<'_>>> {
        self.moved(|c, store, root| match c.leaf_pgno().and_then(|p| store.page_again(p)) {
            Some(leaf) => c.step_from(store, root, leaf, forward),
            None if forward => c.next(store, root),
            None => c.prev(store, root),
        })
    }

    /// The entry the cursor is at, if it is at one. The cursor does not move.
    pub fn current(&self) -> Result<Option<(&[u8], &[u8])>> {
        self.tree.current(&*self.txn.snapshot).inspect_err(|e| self.txn.inner.latch.latch_error(e))
    }

    /// The entry the cursor is at, if it is at one, as its key and its value with the key's last
    /// `t` bytes before it; see [`Cursor::current_with_key_tail`]. The leaf is fetched again as
    /// [`Self::next`] fetches it. The cursor does not move.
    #[inline]
    pub fn current_with_key_tail(&self, t: usize) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        let snapshot = &*self.txn.snapshot;
        let leaf = self.tree.leaf_pgno().and_then(|p| snapshot.page_again(p));
        self.tree
            .current_with_key_tail(snapshot, leaf, t)
            .inspect_err(|e| self.txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none.
    #[inline]
    pub fn seek(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.seek(store, root, key))
    }

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`], from one leaf access: move to
    /// the first entry whose key is `>= key` and return it as its key and its value with the
    /// key's last `t` bytes before it ([`KeyTail`]); `None` (the cursor after the end) if there
    /// is none. The entry is read from the leaf the descent fetched and parsed, an overflow value
    /// once. The cursor is left as [`Self::seek`] leaves it.
    #[inline]
    pub fn seek_with_key_tail(
        &mut self,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        self.moved(|c, store, root| c.seek_with_key_tail(store, root, key, t))
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    #[inline]
    pub fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.moved(|c, store, root| c.seek_exact(store, root, key))
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay at the current entry. The edge rules are
    /// [`Cursor::next_in_prefix`]'s.
    pub fn next_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.step_in_prefix(store, root, None, n, true).map(cursor::entry))
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay at the current entry. The edge
    /// rules are [`Cursor::prev_in_prefix`]'s.
    pub fn prev_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| {
            c.step_in_prefix(store, root, None, n, false).map(cursor::entry)
        })
    }
}

impl ReadTxn for RoTxn {
    fn open_table(&self, name: &str) -> Result<Option<Table>> {
        catalog::check_name(name)?;
        self.read(|s| {
            Ok(catalog::get(s, &s.meta.catalog, name)?.map(|record| Table::new(record.id, name)))
        })
    }

    fn get<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>> {
        self.read(|s| read::get(s, &self.root(table)?, key))
    }

    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>> {
        let root = self.read(|_| self.root(table))?;
        Ok(Cursor::new(&*self.snapshot, root, &self.inner.latch))
    }

    fn txn_id(&self) -> u64 {
        self.snapshot.meta.txn
    }

    fn table_stat(&self, table: &Table) -> Result<TableStat> {
        self.read(|_| Ok(TableStat::of(&self.root(table)?)))
    }
}

/// The write transaction: the only one at a time (see `Env::begin_rw`). Its changes are seen by
/// its own reads and by no one else until [`Self::commit`]; dropping it (or [`Self::abort`])
/// discards them without I/O.
///
/// Any I/O error, growth failure or damage met by an operation latches the environment: that
/// operation returns the error, and every later write and commit returns [`Error::Failed`].
/// Validation errors ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
/// [`Error::AppendOutOfOrder`], [`Error::KeyExists`], table errors) change nothing and do not
/// latch.
pub struct RwTxn {
    inner: Arc<EnvInner>,
    work: Work,
}

impl std::fmt::Debug for RwTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwTxn")
            .field("txn", &self.work.txn)
            .field("changed", &self.work.changed)
            .finish_non_exhaustive()
    }
}

/// The write transaction's working state.
struct Work {
    /// The writer's state, held until the transaction drops.
    writer: Option<Box<Writer>>,
    /// The id this transaction commits as.
    txn: u64,
    /// The committed state it began from.
    base: Meta,
    /// The working catalog.
    catalog: TreeRoot,
    /// The working catalog root's provenance ([`write::put_sized`]): true only while it names a
    /// page this transaction's writer wrote for it.
    catalog_trusted: bool,
    /// The id the next table created gets.
    next_table_id: u32,
    /// The tables resolved by a write, with their working records (those whose tree changed
    /// are written into the catalog at commit). A dropped table leaves.
    tables: Vec<Working>,
    /// True once anything changed (a commit of no change writes nothing).
    changed: bool,
    /// This transaction's serial: unique in the process ([`NEXT_SERIAL`]), so a [`TxnCursor`]
    /// tells its own transaction from every other (a txn id is reused after an abort).
    serial: u64,
    /// Moves on every table created or dropped: a [`TxnCursor`] that saw another value checks its
    /// table again, and forgets the index of its working record ([`Self::tables`] loses entries
    /// only on a drop).
    schema: u64,
    /// The last write stamp handed out by [`Self::set_record`] ([`Working::writes`]).
    stamp: u64,
}

/// The source of [`Work::serial`].
static NEXT_SERIAL: AtomicU64 = AtomicU64::new(0);

/// A table's working record in the write transaction.
struct Working {
    /// The table (its handle's id and name).
    table: Table,
    /// Its working record.
    record: TableRecord,
    /// Its root's provenance ([`write::put_sized`]): true only while the record's root names a
    /// page this transaction's writer wrote for this table.
    trusted: bool,
    /// Its write-path cache ([`write::put_cached`]).
    cache: write::PathCache,
    /// True once its tree changed: the catalog gets the record at commit.
    changed: bool,
    /// The write stamp of its last change ([`Work::set_record`]; 0: unchanged in this
    /// transaction). A [`TxnCursor`] that saw another stamp finds its place again.
    writes: u64,
}

impl Working {
    fn new(table: Table, record: TableRecord) -> Self {
        Self {
            table,
            record,
            trusted: false,
            cache: write::PathCache::default(),
            changed: false,
            writes: 0,
        }
    }
}

impl Work {
    fn writer(&self) -> &Writer {
        self.writer.as_deref().expect("a live write transaction holds the writer")
    }

    fn writer_mut(&mut self) -> &mut Writer {
        self.writer.as_deref_mut().expect("a live write transaction holds the writer")
    }

    /// The writer and the working catalog with its provenance, borrowed together.
    fn catalog_mut(&mut self) -> (&mut Writer, &mut TreeRoot, &mut bool) {
        let writer = self.writer.as_deref_mut().expect("a live write transaction holds the writer");
        (writer, &mut self.catalog, &mut self.catalog_trusted)
    }

    /// Table `name`'s working record, if it exists.
    fn lookup(&self, name: &str) -> Result<Option<TableRecord>> {
        match self.tables.iter().find(|w| *w.table.name == *name) {
            Some(w) => Ok(Some(w.record)),
            None => catalog::get(self.writer(), &self.catalog, name),
        }
    }

    /// `table`'s working record (see [`resolve`]).
    fn record(&self, table: &Table) -> Result<TableRecord> {
        match self.tables.iter().find(|w| w.table == *table) {
            Some(w) => Ok(w.record),
            None => resolve(self.lookup(table.name())?, table, self.next_table_id),
        }
    }

    /// The index in [`Self::tables`] of `table`'s working record (see [`resolve`]), added if
    /// it is not there yet. Every table write starts here, so the scan is inline and the add is
    /// out of line.
    #[inline]
    fn working(&mut self, table: &Table) -> Result<usize> {
        match self.tables.iter().position(|w| w.table.is(table)) {
            Some(i) => Ok(i),
            None => self.add_working(table),
        }
    }

    /// [`Self::working`] for a table without a working record yet.
    #[inline(never)]
    fn add_working(&mut self, table: &Table) -> Result<usize> {
        let record = self.record(table)?;
        self.tables.push(Working::new(table.clone(), record));
        Ok(self.tables.len() - 1)
    }

    /// The writer and the table at `i` in [`Self::tables`], borrowed together.
    fn writer_and_table(&mut self, i: usize) -> (&mut Writer, &mut Working) {
        let writer = self.writer.as_deref_mut().expect("a live write transaction holds the writer");
        (writer, &mut self.tables[i])
    }

    /// Record the changed tree of the table at `i` in [`Self::tables`], with its root's
    /// provenance, under a new write stamp. Every change to a table's tree comes here (put,
    /// append, insert, delete, clear), so the stamp tells a [`TxnCursor`] whether its table
    /// changed since its last call.
    fn set_record(&mut self, i: usize, record: TableRecord, trusted: bool) {
        self.stamp += 1;
        let w = &mut self.tables[i];
        w.record = record;
        w.trusted = trusted;
        w.changed = true;
        w.writes = self.stamp;
        self.changed = true;
    }

    /// Commit steps 1–7 (see the env module docs): the changed tables' roots into the catalog,
    /// the free list's save, the new meta, the pages' CRCs, the durable steps (or their deferral),
    /// and publication.
    fn commit(&mut self, inner: &EnvInner) -> Result<u64> {
        let mut dirty: Vec<_> = std::mem::take(&mut self.tables)
            .into_iter()
            .filter_map(|w| w.changed.then_some((w.table.name, w.record)))
            .collect();
        dirty.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let Self { writer, txn, base, catalog, catalog_trusted, next_table_id, .. } = self;
        let (txn, base, next_table_id) = (*txn, *base, *next_table_id);
        let writer = writer.as_deref_mut().expect("a live write transaction holds the writer");
        #[cfg(test)]
        writer.crash_point(CrashAt::BeforeSave)?;
        for (name, record) in &dirty {
            catalog::put(writer, catalog, catalog_trusted, name, record)?;
        }
        let mut free = base.free;
        let _rounds = freelist::save(writer, &mut free)?;
        #[cfg(test)]
        {
            writer.max_save_rounds = writer.max_save_rounds.max(_rounds);
            writer.crash_point(CrashAt::AfterSave)?;
        }
        let meta = Meta {
            page_size: base.page_size,
            txn,
            page_count: writer.alloc.page_count(),
            next_table_id,
            catalog: *catalog,
            free,
        };
        let pages = writer.stamp_private()?;
        #[cfg(test)]
        writer.crash_point(CrashAt::AfterStamp)?;
        match inner.sync_mode {
            SyncMode::Durable => writer.make_durable(&pages, &meta)?,
            SyncMode::NoSync => writer.defer_sync(&pages),
        }
        writer.publish(inner, meta);
        writer.alloc.begin_txn(txn + 1);
        Ok(txn)
    }
}

impl RwTxn {
    pub(crate) fn new(inner: Arc<EnvInner>, writer: Box<Writer>) -> Self {
        let base = writer.last;
        debug_assert_eq!(writer.alloc.txn(), base.txn + 1, "the writer is set for the next txn");
        Self {
            inner,
            work: Work {
                writer: Some(writer),
                txn: base.txn + 1,
                base,
                catalog: base.catalog,
                catalog_trusted: false,
                next_table_id: base.next_table_id,
                tables: Vec::new(),
                changed: false,
                serial: NEXT_SERIAL.fetch_add(1, Ordering::Relaxed),
                schema: 0,
                stamp: 0,
            },
        }
    }

    /// Test-only: the writer's state.
    #[cfg(test)]
    pub(crate) fn writer_mut(&mut self) -> &mut Writer {
        self.work.writer_mut()
    }

    /// Run a write: [`Error::Failed`] once latched; a failure (or a panic) part way latches.
    fn write<T>(&mut self, op: impl FnOnce(&mut Work) -> Result<T>) -> Result<T> {
        let latch = &self.inner.latch;
        latch.check()?;
        let _guard = UnwindLatch(latch, "a write panicked part way");
        op(&mut self.work).inspect_err(|e| latch.latch_error(e))
    }

    /// Run a read of the working state, latching damage it finds.
    fn read<'t, T>(&'t self, op: impl FnOnce(&'t Work) -> Result<T>) -> Result<T> {
        op(&self.work).inspect_err(|e| self.inner.latch.latch_error(e))
    }

    /// Open the table named `name`, creating it (empty, with a new id) if it does not exist.
    /// [`Error::InvalidTableName`] unless the name is 1..=255 bytes.
    pub fn create_table(&mut self, name: &str) -> Result<Table> {
        catalog::check_name(name)?;
        self.write(|work| {
            if let Some(record) = work.lookup(name)? {
                return Ok(Table::new(record.id, name));
            }
            let id = work.next_table_id;
            work.next_table_id = id
                .checked_add(1)
                .ok_or_else(|| Error::Failed("every table id has been used".to_string()))?;
            let record = TableRecord::new(id, work.txn);
            let (writer, catalog, trusted) = work.catalog_mut();
            catalog::put(writer, catalog, trusted, name, &record)?;
            work.changed = true;
            work.schema += 1;
            let table = Table::new(id, name);
            work.tables.push(Working::new(table.clone(), record));
            Ok(table)
        })
    }

    /// Drop `table`: its pages are freed and its name removed from the catalog. Every handle of
    /// it is stale from now on.
    pub fn drop_table(&mut self, table: Table) -> Result<()> {
        self.write(|work| {
            let record = work.record(&table)?;
            let trusted = work.tables.iter().any(|w| w.table == table && w.trusted);
            write::free_subtree(work.writer_mut(), &record.root, trusted)?;
            let (writer, catalog, trusted) = work.catalog_mut();
            if !catalog::delete(writer, catalog, trusted, table.name())? {
                return Err(Error::Failed(format!(
                    "table {:?} vanished from the catalog",
                    table.name()
                )));
            }
            work.tables.retain(|w| w.table.id != record.id);
            work.schema += 1;
            work.changed = true;
            Ok(())
        })
    }

    /// Remove every entry of `table` (its pages are freed; the table stays).
    pub fn clear_table(&mut self, table: &Table) -> Result<()> {
        self.write(|work| {
            let i = work.working(table)?;
            let (mut record, trusted) = (work.tables[i].record, work.tables[i].trusted);
            if record.root.is_empty() {
                return Ok(());
            }
            write::clear(work.writer_mut(), &mut record.root, trusted)?;
            work.set_record(i, record, false);
            Ok(())
        })
    }

    /// Store `value` under `key` in `table`, replacing any value there. An entry over the page
    /// size's limits is refused first ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`]; see
    /// `Env::max_key_len` and `Env::max_value_len`), changing nothing.
    pub fn put(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Upsert, None).map(drop)
    }

    /// Store `value` under `key` in `table` as its new last entry: `key` must sort strictly after
    /// the table's last key (an empty table takes any key), else [`Error::AppendOutOfOrder`]. It is
    /// the fast way to load keys in ascending order: an append into the table's cached rightmost
    /// leaf neither descends nor searches. The table's path cache holds the leaf of its last put or
    /// append in this transaction (after an append split, the new right leaf) until a page is
    /// allocated or freed; any other append descends. A refused append
    /// ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes
    /// nothing and does not fail the environment.
    pub fn append(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Append, None).map(drop)
    }

    /// Store `value` under `key` in `table` only if the table does not hold `key`: a present key is
    /// [`Error::KeyExists`], found by the put's own search (in the table's cached leaf, else after
    /// its descent) and refused before anything changes, with no latch. A new key is stored exactly
    /// as [`Self::put`] stores it.
    pub fn insert(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Insert, None).map(drop)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`). With a `cursor` (an
    /// [`RwCursor`]'s path and root), the root becomes the table's new one, and when the put
    /// leaves the table's path cache naming the entry's leaf ([`write::Placed::slot`]), the
    /// path is placed at the entry. Returns whether it was placed.
    fn put_entry(
        &mut self,
        table: &Table,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
        cursor: Option<(&mut TreeCursor, &mut TreeRoot)>,
    ) -> Result<bool> {
        self.inner.geometry.place(key.len(), value.len())?;
        self.write(|work| {
            let i = work.working(table)?;
            let (writer, w) = work.writer_and_table(i);
            let (mut record, mut trusted) = (w.record, w.trusted);
            // Test-only: an emptied cache per put, so every put descends.
            #[cfg(test)]
            if writer.no_path_cache {
                w.cache.forget();
            }
            let cache = &mut w.cache;
            let placed =
                write::put_cached(writer, &mut record.root, &mut trusted, cache, key, value, kind)?;
            let mut at = false;
            if let Some((tree, root)) = cursor {
                *root = record.root;
                if let Some(slot) = placed.slot {
                    tree.place(cache.branches(), cache.leaf(), slot);
                    at = true;
                }
            }
            work.set_record(i, record, trusted);
            Ok(at)
        })
    }

    /// Remove `key` from `table`, returning whether it was there. A page the delete leaves under
    /// a quarter full absorbs same-parent siblings that fit, and the parents that lose a cell get
    /// the same test, up the tree.
    pub fn delete(&mut self, table: &Table, key: &[u8]) -> Result<bool> {
        self.write(|work| {
            let i = work.working(table)?;
            let (mut record, mut trusted) = (work.tables[i].record, work.tables[i].trusted);
            let found = write::delete(work.writer_mut(), &mut record.root, &mut trusted, key)?;
            if found {
                work.set_record(i, record, trusted);
            }
            Ok(found)
        })
    }

    /// A new, unset cursor over `table` that can also write: the moves of a [`Cursor`] over this
    /// transaction's working state, plus [`RwCursor::put`], [`RwCursor::append`] and
    /// [`RwCursor::delete_current`]. It holds the transaction until it is dropped.
    pub fn cursor_mut(&mut self, table: &Table) -> Result<RwCursor<'_>> {
        let root = self.read(|work| Ok(work.record(table)?.root))?;
        Ok(RwCursor {
            txn: self,
            table: table.clone(),
            root,
            tree: TreeCursor::default(),
            deleted: false,
            deleted_key: Vec::new(),
        })
    }

    /// A new, unset cursor over `table` that holds no borrow of this transaction (see
    /// [`TxnCursor`]): each call takes the transaction, `&self` for a move and `&mut self` for a
    /// write, so several cursors and the transaction's own `put` and `delete` can interleave, each
    /// seeing the others' writes.
    pub fn cursor_detached(&self, table: &Table) -> Result<TxnCursor> {
        self.read(|work| {
            let mut cursor = TxnCursor {
                table: table.clone(),
                serial: work.serial,
                schema: work.schema,
                slot: None,
                scanned: usize::MAX,
                writes: 0,
                root: work.record(table)?.root,
                tree: TreeCursor::default(),
                key: Vec::new(),
                deleted: false,
                deleted_key: Vec::new(),
                #[cfg(test)]
                reseeks: 0,
            };
            cursor.writes = cursor.table_writes(work);
            Ok(cursor)
        })
    }

    /// Commit: make this transaction's state the newest (durable when this returns in
    /// `SyncMode::Durable`; at the next sync in `SyncMode::NoSync`) and return its txn id. A
    /// transaction that changed nothing returns the id of the state it began from, with no
    /// I/O. An error latches the environment, and whether the commit survives is unknown until
    /// the file is reopened.
    pub fn commit(mut self) -> Result<u64> {
        let inner = &*self.inner;
        inner.latch.check()?;
        if !self.work.changed {
            return Ok(self.work.base.txn);
        }
        let _guard = UnwindLatch(&inner.latch, "a commit panicked part way");
        let txn = self.work.commit(inner).inspect_err(|e| match e {
            Error::Corrupt(cause) => inner.latch.fail_damaged(format!("commit: {cause}")),
            e => inner.latch.fail(format!("commit: {e}")),
        })?;
        // The allocator has begun the next transaction, which changed nothing: hand the writer
        // back without the abort a drop does.
        if let Some(writer) = self.work.writer.take() {
            inner.return_writer(writer);
        }
        Ok(txn)
    }

    /// Discard every change (as dropping the transaction does): no I/O; the pages it wrote are
    /// unreferenced and reused by the next transaction.
    pub fn abort(self) {
        drop(self);
    }
}

impl Drop for RwTxn {
    fn drop(&mut self) {
        if let Some(mut writer) = self.work.writer.take() {
            writer.alloc.abort();
            self.inner.return_writer(writer);
        }
    }
}

impl ReadTxn for RwTxn {
    fn open_table(&self, name: &str) -> Result<Option<Table>> {
        catalog::check_name(name)?;
        self.read(|work| Ok(work.lookup(name)?.map(|record| Table::new(record.id, name))))
    }

    fn get<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>> {
        self.read(|work| read::get(work.writer(), &work.record(table)?.root, key))
    }

    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>> {
        let root = self.read(|work| Ok(work.record(table)?.root))?;
        Ok(Cursor::new(self.work.writer(), root, &self.inner.latch))
    }

    fn txn_id(&self) -> u64 {
        self.work.txn
    }

    fn table_stat(&self, table: &Table) -> Result<TableStat> {
        self.read(|work| Ok(TableStat::of(&work.record(table)?.root)))
    }
}

/// A cursor over one table of a write transaction that can also write (created by
/// [`RwTxn::cursor_mut`]). Its moves are a [`Cursor`]'s, over the transaction's working state;
/// the entries it returns are borrowed until its next call.
///
/// [`Self::put`], [`Self::append`] and [`Self::insert`] write through the transaction and leave the
/// cursor at the entry written. [`Self::delete_current`] removes the entry the cursor is at and
/// leaves it on the entry that followed it (the follower) in a deleted state, so a walk that
/// deletes as it goes skips nothing: [`Self::next`] returns the follower without stepping past it,
/// [`Self::current`] returns it too, [`Self::prev`] returns the entry before it, another
/// [`Self::delete_current`] deletes it, and the prefix steps compare with the deleted entry's key
/// ([`Self::next_in_prefix`]). Each of these ends the deleted state, except a prefix step that
/// finds nothing (a delete starts a new one). With no follower (the table's last entry deleted) the
/// cursor is after the end: [`Self::next`], [`Self::current`] and a delete find nothing, and
/// [`Self::prev`] returns the new last entry. A put that did not split its leaf, or did an append
/// split, places the cursor from the path the table's path cache then holds for the entry; any
/// other split, and a delete, find the place again by key, so a delete that merged pages still
/// leaves the cursor on the follower.
/// Errors are the transaction's (a failure latches the environment).
pub struct RwCursor<'t> {
    txn: &'t mut RwTxn,
    table: Table,
    /// The table's working tree as of the cursor's last change.
    root: TreeRoot,
    tree: TreeCursor,
    /// Set by [`Self::delete_current`]: the position is the deleted entry's follower (or after
    /// the end), which the next [`Self::next`] returns without moving (see the type docs).
    deleted: bool,
    /// The deleted entry's key while `deleted` is set (the buffer is kept for the next delete).
    deleted_key: Vec<u8>,
}

impl std::fmt::Debug for RwCursor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwCursor")
            .field("table", &self.table)
            .field("state", &self.tree.state())
            .field("deleted", &self.deleted)
            .finish_non_exhaustive()
    }
}

impl RwCursor<'_> {
    /// Test-only: where the cursor is, and whether it sits after a deleted entry.
    #[cfg(test)]
    pub(crate) fn state(&self) -> (crate::tree::cursor::State, bool) {
        (self.tree.state(), self.deleted)
    }

    /// Run a move over the working tree, latching damage it finds.
    fn moved<'c, T>(
        &'c mut self,
        op: impl FnOnce(&mut TreeCursor, &'c Writer, &TreeRoot) -> Result<T>,
    ) -> Result<T> {
        self.deleted = false;
        let Self { txn, root, tree, .. } = self;
        let txn: &'c RwTxn = txn;
        op(tree, txn.work.writer(), root).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.first(store, root))
    }

    /// Move to the last entry and return it (`None` for an empty table).
    pub fn last(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.last(store, root))
    }

    /// Move to the next entry and return it (after [`Self::delete_current`]: the entry that
    /// followed the deleted one); see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        if std::mem::take(&mut self.deleted) {
            let txn: &RwTxn = self.txn;
            return self
                .tree
                .current(txn.work.writer())
                .inspect_err(|e| txn.inner.latch.latch_error(e));
        }
        self.moved(|c, store, root| c.next(store, root))
    }

    /// Move to the previous entry and return it (after [`Self::delete_current`]: the entry
    /// before the deleted one); see [`Cursor::prev`].
    pub fn prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.prev(store, root))
    }

    /// The entry the cursor is at, if it is at one. Right after [`Self::delete_current`] that is
    /// the deleted entry's follower, and the cursor is then at it as after any move ([`Self::next`]
    /// steps past it). The cursor does not move.
    pub fn current(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.deleted = false;
        let txn: &RwTxn = self.txn;
        self.tree.current(txn.work.writer()).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none.
    pub fn seek(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.seek(store, root, key))
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    pub fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.moved(|c, store, root| c.seek_exact(store, root, key))
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay where the cursor is. The edge rules are
    /// [`Cursor::next_in_prefix`]'s. Right after [`Self::delete_current`] the prefix is the deleted
    /// key's, so a walk of a prefix that deletes as it goes stays in it: the deleted entry's
    /// follower is returned, and the cursor moves to it, if it shares the deleted key's first `n`
    /// bytes; else `None`, and the cursor stays on the follower in the deleted state (`n` longer
    /// than the deleted key included), so [`Self::next`] still returns the follower.
    pub fn next_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.step_in_prefix(n, true)
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay where the cursor is. The edge
    /// rules are [`Cursor::prev_in_prefix`]'s. Right after [`Self::delete_current`] the prefix is
    /// the deleted key's: the entry before the deleted one is returned, and the cursor moves to
    /// it, if it shares the deleted key's first `n` bytes; else `None`, and the cursor stays on
    /// the follower in the deleted state.
    pub fn prev_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.step_in_prefix(n, false)
    }

    /// [`Self::next_in_prefix`] (`forward`) or [`Self::prev_in_prefix`].
    fn step_in_prefix(&mut self, n: usize, forward: bool) -> Result<Option<Entry<'_>>> {
        let Self { txn, root, tree, deleted, deleted_key, .. } = self;
        let txn: &RwTxn = txn;
        let store = txn.work.writer();
        prefix_step(tree, store, root, deleted, deleted_key, n, forward)
            .inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Store `value` under `key` (as [`RwTxn::put`] does) and move to that entry. A refused
    /// entry ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor
    /// included.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(key, value, PutKind::Upsert)
    }

    /// Store `value` under `key` as the table's new last entry (as [`RwTxn::append`] does) and
    /// move to that entry. A refused append ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`],
    /// [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(key, value, PutKind::Append)
    }

    /// Store `value` under `key` only if the table does not hold `key` (as [`RwTxn::insert`]
    /// does) and move to that entry. A refused insert ([`Error::KeyExists`],
    /// [`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(key, value, PutKind::Insert)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`): placed from the path the put
    /// left in the table's path cache, else found again by key.
    fn put_entry(&mut self, key: &[u8], value: &[u8], kind: PutKind) -> Result<()> {
        let Self { txn, table, root, tree, deleted, .. } = self;
        if txn.put_entry(table, key, value, kind, Some((tree, root)))? {
            *deleted = false;
            return Ok(());
        }
        self.reseek(key, true)
    }

    /// Remove the entry the cursor is at and return `true`: right after a delete, the deleted
    /// entry's follower, so repeated deletes remove the entries in order. `false` (nothing changes)
    /// if the cursor is not at an entry: unset, before the start, or after the end (a delete of the
    /// table's last entry leaves it there). The cursor is left on the deleted entry's follower in
    /// the deleted state (see the type docs).
    pub fn delete_current(&mut self) -> Result<bool> {
        // The key goes into the deleted-key buffer, reused from the last delete.
        let Self { txn, tree, deleted_key, .. } = self;
        let txn: &RwTxn = txn;
        let Some((k, _)) =
            tree.current(txn.work.writer()).inspect_err(|e| txn.inner.latch.latch_error(e))?
        else {
            return Ok(false);
        };
        deleted_key.clear();
        deleted_key.extend_from_slice(k);
        let key = std::mem::take(deleted_key);
        if !self.txn.delete(&self.table, &key)? {
            let cause =
                format!("the entry under a cursor of table {:?} is missing", self.table.name());
            self.txn.inner.latch.fail(cause.clone());
            return Err(Error::Failed(cause));
        }
        self.reseek(&key, false)?;
        self.deleted_key = key;
        Ok(true)
    }

    /// After a change to the tree, find the place again by `key` ([`find_again`]).
    fn reseek(&mut self, key: &[u8], put: bool) -> Result<()> {
        let Self { txn, table, root, tree, deleted, .. } = self;
        let txn: &RwTxn = txn;
        let found = find_again(&txn.work, table, root, tree, key, put);
        *deleted = !put && found.is_ok();
        found.inspect_err(|e| txn.inner.latch.latch_error(e))
    }
}

/// A write cursor's prefix step ([`RwCursor::next_in_prefix`], [`TxnCursor::next_in_prefix`]):
/// from the entry it is at, or, while `deleted`, with `deleted_key`'s prefix from the deleted
/// entry's follower (forward: the follower itself; backward: the entry before it). A step that
/// lands, or fails, clears `deleted`.
#[inline]
fn prefix_step<'s>(
    tree: &mut TreeCursor,
    store: &'s Writer,
    root: &TreeRoot,
    deleted: &mut bool,
    deleted_key: &[u8],
    n: usize,
    forward: bool,
) -> Result<Option<Entry<'s>>> {
    if !*deleted {
        return tree.step_in_prefix(store, root, None, n, forward).map(cursor::entry);
    }
    let Some(prefix) = deleted_key.get(..n) else { return Ok(None) };
    // The position is the deleted entry's follower (or after the end).
    let landed = if forward {
        tree.current(store).map(|e| e.filter(|(k, _)| k.starts_with(prefix)))
    } else {
        tree.step_if(store, root, prefix, false).map(cursor::entry)
    };
    if matches!(landed, Ok(Some(_)) | Err(_)) {
        *deleted = false;
    }
    landed
}

/// After a write cursor's own change to `table`, find its place again by `key` in the table's
/// working tree, refreshing `root`: the entry just written after a put (`put`; it must be
/// there), else the deleted entry's follower (or after the end).
fn find_again(
    work: &Work,
    table: &Table,
    root: &mut TreeRoot,
    tree: &mut TreeCursor,
    key: &[u8],
    put: bool,
) -> Result<()> {
    *root = work.record(table)?.root;
    let store = work.writer();
    if put {
        if tree.seek_exact(store, root, key)?.is_none() {
            return Err(Error::Failed(format!(
                "the entry just put in table {:?} is missing",
                table.name()
            )));
        }
    } else {
        tree.seek(store, root, key)?;
    }
    Ok(())
}

/// Keep a copy of `key` in `saved`, a [`TxnCursor`]'s key buffer. A table's keys often share one
/// length, so a key as long as the one saved overwrites it in place with [`page::copy_short`],
/// which makes no `memcpy` call; any other length goes through `clear` and `extend_from_slice`.
#[inline(always)]
fn save_key(saved: &mut Vec<u8>, key: &[u8]) {
    if saved.len() == key.len() {
        page::copy_short(saved, key);
    } else {
        saved.clear();
        saved.extend_from_slice(key);
    }
}

/// A cursor over one table of a write transaction that holds no borrow of it (created by
/// [`RwTxn::cursor_detached`]): each call takes the transaction, `&RwTxn` for a move (the entry
/// returned borrows the transaction) and `&mut RwTxn` for a write. So several cursors, over one
/// table or several, and the transaction's own writes can interleave, each seeing the others'
/// writes: what a caller that shares the transaction behind a lock needs (reth's `DbTxMut`, whose
/// methods all take `&self`). Its moves, states and prefix steps are a [`Cursor`]'s, and its writes
/// and the deleted state an [`RwCursor`]'s.
///
/// **Staleness.** Every call first brings the cursor up to date with the transaction:
///
/// - a transaction other than the one that made the cursor (a later one, even one that reuses an
///   aborted transaction's id) is [`Error::WrongTxn`];
/// - a dropped table (perhaps re-created since) is [`Error::StaleTable`];
/// - if the table changed since the cursor's last call (another cursor's write, or the
///   transaction's `put`, `append`, `insert`, `delete` or `clear_table`), the cursor takes the
///   table's new tree and finds its entry again by key. If the key is still there the cursor stays
///   on it (with its new value, if it changed). If not, the cursor is left as if it had deleted the
///   entry itself ([`Self::delete_current`]), so another writer's delete reads as its own: on the
///   first entry after the key (the follower) in the deleted state, so [`Self::next`] and
///   [`Self::current`] return the follower, [`Self::prev`] the entry before it, a delete deletes
///   it, and the prefix steps compare with the gone key. A cursor already in that state stays in
///   it, on its follower, or on the first entry after the follower if that is gone too. An unset
///   cursor, or one after the end or before the start, keeps its state over the new tree.
///
/// The reseek is lazy: it sees the table as it is at the cursor's next call. So if, before that
/// call, another writer deleted the cursor's entry and put it back, or put an entry between it
/// and its follower, the cursor is on that entry, not on the follower the delete left: it finds
/// its place at that call, not at the delete.
///
/// A write to another table, and the cursor's own writes, cause no reseek. Each landing on an
/// entry keeps a copy of its key, so the cursor can find it again.
///
/// Errors are the transaction's (a failure latches the environment); [`Error::WrongTxn`] and
/// [`Error::StaleTable`] change nothing and do not latch.
pub struct TxnCursor {
    table: Table,
    /// The serial of the transaction the cursor belongs to ([`Work::serial`]).
    serial: u64,
    /// The transaction's schema counter as of the cursor's last call ([`Work::schema`]).
    schema: u64,
    /// The index of the table's working record in [`Work::tables`], once it has one.
    slot: Option<usize>,
    /// While `slot` is `None`: the number of working records when the cursor last looked (a new
    /// one is pushed, so an unchanged count means the table still has none).
    scanned: usize,
    /// The table's write stamp as of the cursor's last call ([`Working::writes`]).
    writes: u64,
    /// The table's working tree as of the cursor's last call.
    root: TreeRoot,
    tree: TreeCursor,
    /// The key of the entry the cursor is at (while `deleted`, the deleted entry's follower): the
    /// key a reseek finds it again by.
    key: Vec<u8>,
    /// Set when the cursor's entry was deleted (by it or another writer): the cursor is at the
    /// deleted entry's follower, or after the end, and the next [`Self::next`] does not move (see
    /// the type docs).
    deleted: bool,
    /// The deleted entry's key while `deleted` is set, for the prefix steps (the buffer is
    /// reused).
    deleted_key: Vec<u8>,
    /// Test-only: how many times the cursor found its place again after another writer's change.
    #[cfg(test)]
    pub(crate) reseeks: u32,
}

impl std::fmt::Debug for TxnCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCursor")
            .field("table", &self.table)
            .field("state", &self.tree.state())
            .field("deleted", &self.deleted)
            .finish_non_exhaustive()
    }
}

impl TxnCursor {
    /// Test-only: where the cursor is, and whether it sits after a deleted entry.
    #[cfg(test)]
    pub(crate) fn state(&self) -> (State, bool) {
        (self.tree.state(), self.deleted)
    }

    /// The table's write stamp in `work` (0 while it has no working record), from the remembered
    /// index of its working record.
    #[inline]
    fn table_writes(&mut self, work: &Work) -> u64 {
        match self.slot {
            Some(i) => match work.tables.get(i) {
                Some(w) if w.table.id == self.table.id => w.writes,
                _ => self.find_slot(work),
            },
            None if self.scanned == work.tables.len() => 0,
            None => self.find_slot(work),
        }
    }

    /// [`Self::table_writes`] by a search of the working records.
    #[inline(never)]
    fn find_slot(&mut self, work: &Work) -> u64 {
        self.slot = work.tables.iter().position(|w| w.table.id == self.table.id);
        self.scanned = work.tables.len();
        self.slot.map_or(0, |i| work.tables[i].writes)
    }

    /// The checks every call makes first: the transaction ([`Error::WrongTxn`]), then, after a
    /// table was created or dropped, the table ([`Error::StaleTable`]).
    #[inline]
    fn check(&mut self, work: &Work) -> Result<()> {
        if work.serial != self.serial {
            return Err(Error::WrongTxn);
        }
        if work.schema != self.schema {
            self.schema_changed(work)?;
        }
        Ok(())
    }

    /// [`Self::check`] after a table was created or dropped: the table must still exist, and the
    /// index of its working record is looked up again.
    #[inline(never)]
    fn schema_changed(&mut self, work: &Work) -> Result<()> {
        work.record(&self.table)?;
        self.schema = work.schema;
        self.slot = None;
        self.scanned = usize::MAX;
        Ok(())
    }

    /// True if the cursor is up to date with `work`: its transaction, no table created or
    /// dropped, and its table unchanged since its last call. The check of every call, inline.
    #[inline(always)]
    fn up_to_date(&self, work: &Work) -> bool {
        work.serial == self.serial
            && work.schema == self.schema
            && match self.slot {
                Some(i) => work
                    .tables
                    .get(i)
                    .is_some_and(|w| w.table.id == self.table.id && w.writes == self.writes),
                None => self.scanned == work.tables.len() && self.writes == 0,
            }
    }

    /// Bring the cursor up to date with `work` (see the type docs); [`Self::up_to_date`] is its
    /// fast path.
    #[inline(always)]
    fn sync(&mut self, work: &Work) -> Result<()> {
        if self.up_to_date(work) {
            return Ok(());
        }
        self.sync_slow(work)
    }

    /// [`Self::sync`] for a cursor that is not up to date.
    #[inline(never)]
    fn sync_slow(&mut self, work: &Work) -> Result<()> {
        self.check(work)?;
        let writes = self.table_writes(work);
        if writes != self.writes {
            self.refresh(work, writes)?;
        }
        Ok(())
    }

    /// After a change to the table by another writer: take its new tree and find the place again
    /// by the saved key (see the type docs). An error leaves the cursor unset.
    #[inline(never)]
    fn refresh(&mut self, work: &Work, writes: u64) -> Result<()> {
        #[cfg(test)]
        {
            self.reseeks += 1;
        }
        self.root = match self.slot {
            Some(i) => work.tables[i].record.root,
            None => work.record(&self.table)?.root,
        };
        self.writes = writes;
        let store = work.writer();
        if self.tree.state() != State::At {
            return Ok(());
        }
        let Self { tree, root, key, deleted, deleted_key, .. } = self;
        // At the entry `key` (while `deleted`, the deleted entry's follower): back on it if it is
        // still there, else on the first entry after it, as if its own delete had moved it there.
        let found = match tree.seek(store, root, key) {
            Ok(Some((k, _))) if k == key.as_slice() => Ok(()),
            Ok(landed) => {
                if !*deleted {
                    *deleted = true;
                    std::mem::swap(key, deleted_key);
                }
                if let Some((k, _)) = landed {
                    key.clear();
                    key.extend_from_slice(k);
                }
                Ok(())
            }
            Err(e) => Err(e),
        };
        if found.is_err() {
            *deleted = false;
        }
        found
    }

    /// Run a call: bring the cursor up to date with `txn`, then `op`. A failure latches the
    /// environment if it is one that latches.
    #[inline(always)]
    fn call<'t, T>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut Self, &'t Writer) -> Result<T>,
    ) -> Result<T> {
        if !self.up_to_date(&txn.work) {
            if let Err(e) = self.sync_slow(&txn.work) {
                txn.inner.latch.latch_error(&e);
                return Err(e);
            }
        }
        op(self, txn.work.writer()).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Finish a move: keep the key of the entry it landed on, which the next call may have to
    /// find again.
    #[inline]
    fn landed<'s>(&mut self, moved: Result<Option<Entry<'s>>>) -> Result<Option<Entry<'s>>> {
        if let Ok(Some((k, _))) = moved {
            save_key(&mut self.key, k);
        }
        moved
    }

    /// Run a plain move (it leaves the deleted state) and keep the key it lands on.
    fn moved<'t>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut TreeCursor, &'t Writer, &TreeRoot) -> Result<Option<Entry<'t>>>,
    ) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            let moved = op(&mut c.tree, store, &c.root);
            c.landed(moved)
        })
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.first(store, root))
    }

    /// Move to the last entry and return it (`None` for an empty table). The cursor is then on
    /// that entry as after any move: [`Self::next`] returns an entry another writer has put past
    /// it since, else nothing (the cursor after the end).
    pub fn last<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.last(store, root))
    }

    /// Move to the next entry and return it (after the cursor's entry was deleted, by this cursor
    /// or another writer: the first entry after the deleted key); see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.call(txn, |c, store| {
            let moved = if std::mem::take(&mut c.deleted) {
                c.tree.current(store)
            } else {
                c.tree.next(store, &c.root)
            };
            c.landed(moved)
        })
    }

    /// Move to the previous entry and return it (after the cursor's entry was deleted: the entry
    /// before the deleted entry's follower; from an unset cursor: the last entry); see
    /// [`Cursor::prev`].
    pub fn prev<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.prev(store, root))
    }

    /// The entry the cursor is at, if it is at one: after its entry was deleted, the deleted
    /// entry's follower, which the cursor is then at as after any move ([`Self::next`] steps past
    /// it). The cursor does not move, unless the table changed and it finds its place again.
    pub fn current<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            c.tree.current(store)
        })
    }

    /// [`Self::current`] with the entry's value joined to the key's last `t` bytes; see
    /// [`Cursor::current_with_key_tail`].
    pub fn current_with_key_tail<'t>(
        &mut self,
        txn: &'t RwTxn,
        t: usize,
    ) -> Result<Option<(&'t [u8], KeyTail<'t>)>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            c.tree.current_with_key_tail(store, None, t)
        })
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none.
    pub fn seek<'t>(&mut self, txn: &'t RwTxn, key: &[u8]) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.seek(store, root, key))
    }

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`], from one leaf access: move to
    /// the first entry whose key is `>= key` and return it as its key and its value with the
    /// key's last `t` bytes before it ([`KeyTail`]); `None` (the cursor after the end) if there
    /// is none. The cursor is left as [`Self::seek`] leaves it.
    pub fn seek_with_key_tail<'t>(
        &mut self,
        txn: &'t RwTxn,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&'t [u8], KeyTail<'t>)>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            let found = c.tree.seek_with_key_tail(store, &c.root, key, t);
            if let Ok(Some((k, _))) = found {
                save_key(&mut c.key, k);
            }
            found
        })
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    pub fn seek_exact<'t>(&mut self, txn: &'t RwTxn, key: &[u8]) -> Result<Option<&'t [u8]>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            let found = c.tree.seek_exact(store, &c.root, key);
            if let Ok(Some(_)) = found {
                save_key(&mut c.key, key);
            }
            found
        })
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay where the cursor is. The edge rules are
    /// [`Cursor::next_in_prefix`]'s, and after the cursor's entry was deleted (by it or another
    /// writer) [`RwCursor::next_in_prefix`]'s: the prefix is the deleted key's.
    pub fn next_in_prefix<'t>(
        &mut self,
        txn: &'t RwTxn,
        n: usize,
    ) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step_in_prefix(txn, n, true)
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay where the cursor is. The edge
    /// rules are [`Cursor::prev_in_prefix`]'s, and after the cursor's entry was deleted
    /// [`RwCursor::prev_in_prefix`]'s.
    pub fn prev_in_prefix<'t>(
        &mut self,
        txn: &'t RwTxn,
        n: usize,
    ) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step_in_prefix(txn, n, false)
    }

    /// [`Self::next_in_prefix`] (`forward`) or [`Self::prev_in_prefix`].
    fn step_in_prefix<'t>(
        &mut self,
        txn: &'t RwTxn,
        n: usize,
        forward: bool,
    ) -> Result<Option<Entry<'t>>> {
        self.call(txn, |c, store| {
            let moved = prefix_step(
                &mut c.tree,
                store,
                &c.root,
                &mut c.deleted,
                &c.deleted_key,
                n,
                forward,
            );
            c.landed(moved)
        })
    }

    /// Store `value` under `key` (as [`RwTxn::put`] does) and move to that entry. A refused
    /// entry ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor
    /// included.
    pub fn put(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Upsert)
    }

    /// Store `value` under `key` as the table's new last entry (as [`RwTxn::append`] does) and
    /// move to that entry. A refused append ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`],
    /// [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn append(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Append)
    }

    /// Store `value` under `key` only if the table does not hold `key` (as [`RwTxn::insert`]
    /// does) and move to that entry. A refused insert ([`Error::KeyExists`],
    /// [`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn insert(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Insert)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`): the transaction and table
    /// checked, but no reseek, since the write places the cursor anew; placed from the path the
    /// put left in the table's path cache, else found again by key. The table's new write stamp
    /// is the cursor's own, so its write causes it no reseek.
    fn put_entry(
        &mut self,
        txn: &mut RwTxn,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
    ) -> Result<()> {
        self.check(&txn.work).inspect_err(|e| txn.inner.latch.latch_error(e))?;
        let Self { table, root, tree, .. } = self;
        let placed = txn.put_entry(table, key, value, kind, Some((tree, root)))?;
        self.deleted = false;
        save_key(&mut self.key, key);
        self.writes = self.table_writes(&txn.work);
        if placed {
            return Ok(());
        }
        let Self { table, root, tree, .. } = self;
        find_again(&txn.work, table, root, tree, key, true)
            .inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Remove the entry the cursor is at and return `true`: after its entry was deleted (by it or
    /// another writer), the follower, as [`RwCursor::delete_current`] does. `false` (nothing
    /// changes) if the cursor is not at an entry: unset, before the start, or after the end. The
    /// cursor is left on the deleted entry's follower in the deleted state, as an [`RwCursor`] is.
    pub fn delete_current(&mut self, txn: &mut RwTxn) -> Result<bool> {
        self.sync(&txn.work).inspect_err(|e| txn.inner.latch.latch_error(e))?;
        if self.tree.state() != State::At {
            return Ok(false);
        }
        // The saved key is the current entry's (while `deleted`, the follower's); it becomes the
        // deleted key.
        std::mem::swap(&mut self.key, &mut self.deleted_key);
        let key = std::mem::take(&mut self.deleted_key);
        let deleted = self.delete_key(txn, &key);
        self.deleted_key = key;
        deleted.map(|()| true)
    }

    /// [`Self::delete_current`] of the entry `key`, which must be there.
    fn delete_key(&mut self, txn: &mut RwTxn, key: &[u8]) -> Result<()> {
        if !txn.delete(&self.table, key)? {
            let cause =
                format!("the entry under a cursor of table {:?} is missing", self.table.name());
            txn.inner.latch.fail(cause.clone());
            return Err(Error::Failed(cause));
        }
        self.writes = self.table_writes(&txn.work);
        let Self { table, root, tree, .. } = self;
        let found = find_again(&txn.work, table, root, tree, key, false).and_then(|()| {
            // The follower's key, which a reseek finds the cursor again by.
            if let Some((k, _)) = tree.current(txn.work.writer())? {
                save_key(&mut self.key, k);
            }
            Ok(())
        });
        self.deleted = found.is_ok();
        found.inspect_err(|e| txn.inner.latch.latch_error(e))
    }
}
