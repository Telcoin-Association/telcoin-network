//! Transactions: read-only ones ([`RoTxn`]) on a published snapshot, and the single write
//! transaction ([`RwTxn`]), which builds the next state in pages no reader can reach and
//! publishes it at [`RwTxn::commit`]. Tables are named by [`Table`] handles, checked against
//! the catalog on every use.

use std::sync::{Arc, OnceLock};

#[cfg(test)]
use crate::env::CrashAt;
use crate::{
    catalog::{self, TableRecord},
    env::{EnvInner, Snapshot, SyncMode, Writer},
    error::{Error, Result},
    freelist,
    latch::UnwindLatch,
    meta::Meta,
    tree::{
        cursor::{Cursor, TreeCursor},
        read,
        write::{self, PutKind},
        TreeRoot,
    },
};

/// A handle on one table: its name and its id. The id is unique over the file's life, so a
/// handle of a table that was dropped (and perhaps re-created under the same name) is refused
/// with [`Error::StaleTable`] rather than reaching the new table; a handle used in a transaction
/// whose state predates the table is refused with [`Error::TableNotFound`].
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
    send_sync::<Table>();
};

/// Figures about one table in a transaction's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableStat {
    /// The number of entries.
    pub entries: u64,
    /// The height of the table's B+tree (0: empty).
    pub height: u32,
}

impl TableStat {
    fn of(root: &TreeRoot) -> Self {
        Self { entries: root.entries, height: root.height }
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
    /// transaction lives.
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

/// How many tables a [`RoTxn`] remembers the records of.
const RO_TABLES: usize = 8;

/// A read-only transaction: one committed state of every table, unaffected by later commits.
/// The state's pages stay intact while it lives.
pub struct RoTxn {
    inner: Arc<EnvInner>,
    snapshot: Arc<Snapshot>,
    /// The records of the first tables resolved (filled in order; the snapshot never changes,
    /// so a record resolved once stands for the transaction's life).
    tables: [OnceLock<(Table, TableRecord)>; RO_TABLES],
}

impl std::fmt::Debug for RoTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoTxn").field("txn", &self.snapshot.meta.txn).finish_non_exhaustive()
    }
}

impl RoTxn {
    pub(crate) fn new(inner: Arc<EnvInner>, snapshot: Arc<Snapshot>) -> Self {
        Self { inner, snapshot, tables: Default::default() }
    }

    /// Run a read, latching damage it finds.
    fn read<'t, T>(&'t self, op: impl FnOnce(&'t Snapshot) -> Result<T>) -> Result<T> {
        op(&self.snapshot).inspect_err(|e| self.inner.latch.latch_error(e))
    }

    /// `table`'s record in the snapshot (see [`resolve`]), remembered once resolved. Only a
    /// resolved record is remembered: a refused handle is checked again each time.
    fn record(&self, table: &Table) -> Result<TableRecord> {
        for slot in &self.tables {
            match slot.get() {
                Some((known, record)) if known == table => return Ok(*record),
                Some(_) => {}
                // A slot is set only after every one before it, so the rest are empty.
                None => break,
            }
        }
        let meta = &self.snapshot.meta;
        let found = catalog::get(&*self.snapshot, &meta.catalog, table.name())?;
        let record = resolve(found, table, meta.next_table_id)?;
        // The first empty slot, unless a racing reader takes it first (then the next one).
        for slot in &self.tables {
            if slot.get().is_none() && slot.set((table.clone(), record)).is_ok() {
                break;
            }
        }
        Ok(record)
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
        self.read(|s| read::get(s, &self.record(table)?.root, key))
    }

    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>> {
        let root = self.read(|_| Ok(self.record(table)?.root))?;
        Ok(Cursor::new(&*self.snapshot, root, &self.inner.latch))
    }

    fn txn_id(&self) -> u64 {
        self.snapshot.meta.txn
    }

    fn table_stat(&self, table: &Table) -> Result<TableStat> {
        self.read(|_| Ok(TableStat::of(&self.record(table)?.root)))
    }
}

/// The write transaction: the only one at a time (see `Env::begin_rw`). Its changes are seen by
/// its own reads and by no one else until [`Self::commit`]; dropping it (or [`Self::abort`])
/// discards them without I/O.
///
/// Any I/O error, growth failure or damage met by an operation latches the environment: that
/// operation returns the error, and every later write and commit returns [`Error::Failed`].
/// Validation errors ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`], table errors) change
/// nothing and do not latch.
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
}

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
}

impl Working {
    fn new(table: Table, record: TableRecord) -> Self {
        Self { table, record, trusted: false, cache: write::PathCache::default(), changed: false }
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
    /// provenance.
    fn set_record(&mut self, i: usize, record: TableRecord, trusted: bool) {
        let w = &mut self.tables[i];
        w.record = record;
        w.trusted = trusted;
        w.changed = true;
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
            write::free_subtree(work.writer_mut(), &record.root)?;
            let (writer, catalog, trusted) = work.catalog_mut();
            if !catalog::delete(writer, catalog, trusted, table.name())? {
                return Err(Error::Failed(format!(
                    "table {:?} vanished from the catalog",
                    table.name()
                )));
            }
            work.tables.retain(|w| w.table.id != record.id);
            work.changed = true;
            Ok(())
        })
    }

    /// Remove every entry of `table` (its pages are freed; the table stays).
    pub fn clear_table(&mut self, table: &Table) -> Result<()> {
        self.write(|work| {
            let i = work.working(table)?;
            let mut record = work.tables[i].record;
            if record.root.is_empty() {
                return Ok(());
            }
            write::clear(work.writer_mut(), &mut record.root)?;
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
    /// the table's last key (an empty table takes any key), else [`Error::AppendOutOfOrder`]. Like
    /// MDBX's `MDBX_APPEND`, it is the fast way to load keys in ascending order: an append into
    /// the table's cached rightmost leaf neither descends nor searches. The table's path cache
    /// holds the leaf of its last put or append in this transaction (after an append split, the
    /// new right leaf) until a page is allocated or freed; any other append descends. A refused
    /// append ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`])
    /// changes nothing and does not fail the environment.
    pub fn append(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Append, None).map(drop)
    }

    /// [`Self::put`] or [`Self::append`] (`kind`). With a `cursor` (an [`RwCursor`]'s path and
    /// root), the root becomes the table's new one, and when the put leaves the table's path
    /// cache naming the entry's leaf ([`write::Placed::slot`]), the path is placed at the entry.
    /// Returns whether it was placed.
    fn put_entry(
        &mut self,
        table: &Table,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
        cursor: Option<(&mut TreeCursor, &mut TreeRoot)>,
    ) -> Result<bool> {
        self.inner.geometry.check_entry(key.len(), value.len())?;
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

    /// Remove `key` from `table`, returning whether it was there.
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
/// [`Self::put`] and [`Self::append`] write through the transaction and leave the cursor at the
/// entry written. [`Self::delete_current`] removes the entry the cursor is at and leaves it
/// between that entry's neighbours: [`Self::next`] then returns the entry that followed the
/// deleted one, [`Self::prev`] the entry before it, and [`Self::current`] nothing. A put that did
/// not split its leaf, or did an append split, places the cursor from the path the table's path
/// cache then holds for the entry; any other split, and a delete, find the place again by key.
/// Errors are the transaction's (a failure latches the environment).
pub struct RwCursor<'t> {
    txn: &'t mut RwTxn,
    table: Table,
    /// The table's working tree as of the cursor's last change.
    root: TreeRoot,
    tree: TreeCursor,
    /// Set by [`Self::delete_current`]: the position is the deleted entry's follower (or after
    /// the end), which the next [`Self::next`] returns without moving.
    deleted: bool,
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

    /// The entry the cursor is at, if it is at one (nothing right after
    /// [`Self::delete_current`]). The cursor does not move.
    pub fn current(&self) -> Result<Option<(&[u8], &[u8])>> {
        if self.deleted {
            return Ok(None);
        }
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

    /// [`Self::put`] or [`Self::append`] (`kind`): placed from the path the put left in the
    /// table's path cache, else found again by key.
    fn put_entry(&mut self, key: &[u8], value: &[u8], kind: PutKind) -> Result<()> {
        let Self { txn, table, root, tree, deleted } = self;
        if txn.put_entry(table, key, value, kind, Some((tree, root)))? {
            *deleted = false;
            return Ok(());
        }
        self.reseek(key, true)
    }

    /// Remove the entry the cursor is at and return `true`; `false` (nothing changes) if the
    /// cursor is not at an entry. The cursor is left between the deleted entry's neighbours (see
    /// the type docs).
    pub fn delete_current(&mut self) -> Result<bool> {
        let Some((key, _)) = self.current()? else {
            return Ok(false);
        };
        let key = key.to_vec();
        if !self.txn.delete(&self.table, &key)? {
            let cause =
                format!("the entry under a cursor of table {:?} is missing", self.table.name());
            self.txn.inner.latch.fail(cause.clone());
            return Err(Error::Failed(cause));
        }
        self.reseek(&key, false)?;
        Ok(true)
    }

    /// After a change to the tree, find the place again by `key`: the entry just written after a
    /// put (which must be there), else the deleted entry's follower (or after the end).
    fn reseek(&mut self, key: &[u8], put: bool) -> Result<()> {
        let Self { txn, table, root, tree, deleted } = self;
        let txn: &RwTxn = txn;
        let found = (|| {
            *root = txn.work.record(table)?.root;
            let store = txn.work.writer();
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
        })();
        *deleted = !put && found.is_ok();
        found.inspect_err(|e| txn.inner.latch.latch_error(e))
    }
}
