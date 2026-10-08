//! A [`Database`] backed by per-table [`TnTable`]s: each table owns an append-only value log plus a
//! copy-on-write B+tree index, read lock-free from a published snapshot, and the store maps a table
//! name to that table's `TnTable` handle (a cheap `Clone`).
//!
//! This module is the typed layer, modeled on [`crate::mem_db`]: keys are encoded with `encode_key`
//! (binary-sortable) and values with `encode` (bcs); each op hands the encoded bytes to the table
//! and decodes the reply.  Encodes reuse buffers rather than allocating per op: writes use their
//! table's `EncodeBufs`, point reads a per-thread key buffer.  The index's fixed key length is
//! `encode_key(key).len()` — `size_of::<T::Key>()` is unreliable (e.g. `AuthorityIdentifier` is
//! `Arc<[u8; 32]>`, 8 bytes in memory but 32 encoded).
//!
//! Scans are lazy: each `DBIter` owns a published snapshot of its table and a B+tree cursor,
//! decoding every row straight from the index leaf and the log.
//!
//! Each table is keyed (its log stores every row's key, the default: [`Database::open_table`]) or
//! derived-key (its log stores only values, and a key function recomputes the keys on a rebuild:
//! [`TnDatabase::open_table_with_key`]), fixed when the table is created. A table not closed
//! cleanly rebuilds its index from its logs on open, keeping every committed write; clearing a
//! table deletes its data from disk (see `table.rs`). Transactions commit per table: a write
//! transaction over several tables commits each in turn, so a crash can keep one table's part and
//! not another's. Not yet covered: compacting the logs (garbage from overwrites and removals), and
//! durability-barrier tuning.

use std::{
    cell::RefCell,
    collections::HashMap,
    hash::BuildHasherDefault,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use serde::Serialize;
use tn_types::{
    decode, decode_key, encode_into_buffer, encode_key, encode_key_into, try_decode, DBIter,
    Database, DbTx, DbTxMut, Table,
};

use super::table::{KeyFn, ScanKind, TnTable};
use crate::archive::fxhasher::FxHasher;

/// Reusable encode buffers for a table's writes, so an insert or remove encodes without allocating.
/// The value buffer keeps the capacity of the largest value the table has written.
#[derive(Debug, Default)]
struct EncodeBufs {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// A table's handle plus its write buffers. A write holds its table's buffer lock and then takes
/// the table's write lock; the buffer lock is only ever contended by another write to the same
/// table, which would wait on that table's write lock anyway. `Clone` (sharing both) so a new
/// snapshot of the table map can carry the existing tables.
#[derive(Clone, Debug)]
struct TableStore {
    table: TnTable,
    bufs: Arc<Mutex<EncodeBufs>>,
    /// Opened as a derived-key table (see [`TnDatabase::open_table_with_key`]).
    derived: bool,
}

/// The open tables by name.
type Tables = HashMap<&'static str, TableStore, BuildHasherDefault<FxHasher>>;

/// The table map, read through immutable snapshots: tables are opened at startup and looked up on
/// every op, so a lookup must not write shared memory. `ArcSwap::load` borrows the current snapshot
/// through a per-thread slot (no shared refcount, no lock), and `open_table` publishes a new
/// snapshot (read-copy-update).
type StoreType = ArcSwap<Tables>;

thread_local! {
    /// Key buffer for point reads. Per thread, so concurrent readers of a table never contend on a
    /// shared buffer.
    static READ_KEY: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Encode `key` into this thread's read-key buffer and run `f` on the bytes. `f` must not re-enter
/// tndb on this thread (the buffer is borrowed for its duration).
fn with_read_key<K: Serialize, R>(
    key: &K,
    f: impl FnOnce(&[u8]) -> eyre::Result<R>,
) -> eyre::Result<R> {
    READ_KEY.with_borrow_mut(|buf| {
        buf.clear();
        encode_key_into(buf, key)?;
        f(buf)
    })
}

// ---- shared table operations (used by both the `Database` and the txn impls) ----
//
// Each op borrows its table from the current snapshot of the table map (see `StoreType`), so a
// lookup writes no shared memory. The snapshot is held for the op, including across a blocking
// table op; that only occupies one of the thread's `ArcSwap` slots (scans own their table state).
//
// NOTE: reads (`get`, `contains_key`, iterators, ...) see a table's last published (committed)
// state and take no lock: an autocommit write publishes when it returns, a write transaction's
// writes publish at `commit`, and only that transaction's own `get` sees them earlier. An iterator
// owns the snapshot it started on, so writing (and committing) its table while iterating, from any
// thread, neither blocks nor changes what the iterator yields.

/// Run `f` on the named table, borrowed from the current snapshot of the table map, or return
/// `None` if no such table is open.
fn with_table<R>(store: &StoreType, name: &str, f: impl FnOnce(&TableStore) -> R) -> Option<R> {
    store.load().get(name).map(f)
}

/// Look up a key: read its value bytes from the table, then decode.
fn get<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    with_table(store, T::NAME, |entry| {
        // Decode `T::Value` straight from the log's mmap (no intermediate `Vec`).
        with_read_key(key, |key| entry.table.get_with(key, |bytes| decode::<T::Value>(bytes)))
    })
    .unwrap_or(Ok(None))
}

/// Insert `key → value` (no durability flush; callers flush explicitly).
fn insert<T: Table>(store: &StoreType, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
    with_table(store, T::NAME, |entry| -> eyre::Result<()> {
        let mut bufs = entry.bufs.lock();
        let EncodeBufs { key: key_buf, value: value_buf } = &mut *bufs;
        key_buf.clear();
        encode_key_into(key_buf, key)?;
        value_buf.clear();
        encode_into_buffer(value_buf, value)?;
        entry.table.insert(key_buf, value_buf)
    })
    .unwrap_or(Ok(()))
}

/// Remove a key (its log bytes are left as unreferenced garbage; pack compaction is a later step).
fn remove<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<()> {
    with_table(store, T::NAME, |entry| -> eyre::Result<()> {
        let mut bufs = entry.bufs.lock();
        bufs.key.clear();
        encode_key_into(&mut bufs.key, key)?;
        entry.table.remove(&bufs.key)?;
        Ok(())
    })
    .unwrap_or(Ok(()))
}

/// Reset a table to empty (its log bytes become unreferenced garbage until compaction).
fn clear_table<T: Table>(store: &StoreType) -> eyre::Result<()> {
    with_table(store, T::NAME, |entry| entry.table.clear()).unwrap_or(Ok(()))
}

/// Durably persist a table's value log, then publish its writes to readers.
fn flush_table<T: Table>(store: &StoreType) -> eyre::Result<()> {
    with_table(store, T::NAME, |entry| entry.table.flush()).unwrap_or(Ok(()))
}

/// True if the table contains `key`.
fn contains_key<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<bool> {
    with_table(store, T::NAME, |entry| with_read_key(key, |key| entry.table.contains(key)))
        .unwrap_or(Ok(false))
}

/// True if the table is empty (or absent / unreadable).
fn is_empty<T: Table>(store: &StoreType) -> bool {
    with_table(store, T::NAME, |entry| entry.table.is_empty().unwrap_or(false)).unwrap_or(false)
}

/// A lazy, key-ordered [`DBIter`] over the table's published snapshot (holding no lock).
fn scan<T: Table>(store: &StoreType, kind: ScanKind) -> DBIter<'static, T> {
    match with_table(store, T::NAME, |entry| entry.table.scan(kind)) {
        Some(mut scan) => Box::new(std::iter::from_fn(move || {
            scan.next_with(|key, value| (decode_key::<T::Key>(key), decode::<T::Value>(value)))
        })),
        None => Box::new(std::iter::empty()),
    }
}

/// The single `(key, value)` a one-shot scan lands on (its first item), found by a direct seek.
fn first_of<T: Table>(store: &StoreType, kind: ScanKind) -> Option<(T::Key, T::Value)> {
    with_table(store, T::NAME, |entry| {
        entry
            .table
            .first_with(kind, |key, value| (decode_key::<T::Key>(key), decode::<T::Value>(value)))
    })
    .flatten()
}

/// A [`Database`] backed by per-table [`TnTable`]s.
#[derive(Clone, Debug)]
pub struct TnDatabase {
    store: Arc<StoreType>,
    base: PathBuf,
    /// Serializes table opens, so one table is never opened twice.
    open_lock: Arc<Mutex<()>>,
}

impl TnDatabase {
    /// Open (creating the directory if needed) a tndb rooted at `path`.  Call
    /// [`Database::open_table`] (or [`Self::open_table_with_key`]) for each table before use.
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        let base = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base)?;
        Ok(Self {
            store: Arc::new(ArcSwap::from_pointee(Tables::default())),
            base,
            open_lock: Arc::default(),
        })
    }

    /// Open table `T` as a derived-key table: its log stores only values, and `key_of` recomputes
    /// a row's key from its value when the index is rebuilt (e.g. a digest-keyed table hashes the
    /// value). Every row's key must be `key_of` of its value; debug builds check each insert.
    ///
    /// The mode is fixed when the table is created: opening a derived-key table with
    /// [`Database::open_table`], or a keyed one with this, is an error. Opening an already-open
    /// table keeps it, so this can run before a wrapper (e.g. `LayeredDatabase`) opens the table
    /// again.
    pub fn open_table_with_key<T: Table>(
        &self,
        key_of: impl Fn(&T::Value) -> T::Key + Send + Sync + 'static,
    ) -> eyre::Result<()> {
        let key_fn: KeyFn = Arc::new(move |bytes: &[u8]| {
            let value = try_decode::<T::Value>(bytes)?;
            Ok(encode_key(&key_of(&value)))
        });
        self.open_table_in::<T>(Some(key_fn))
    }

    /// Open table `T` (keyed, or derived-key with `key_fn`) unless it is already open. An open
    /// table is kept as it is for a plain [`Database::open_table`] (e.g. from a wrapper, after
    /// [`Self::open_table_with_key`]); asking for derived keys on a table open as keyed is an
    /// error.
    fn open_table_in<T: Table>(&self, key_fn: Option<KeyFn>) -> eyre::Result<()> {
        let derived = key_fn.is_some();
        let _opening = self.open_lock.lock();
        if let Some(open_derived) = with_table(&self.store, T::NAME, |entry| entry.derived) {
            if derived && !open_derived {
                eyre::bail!("tndb: table {} is already open as a keyed table", T::NAME);
            }
            return Ok(());
        }
        let entry = TableStore {
            table: TnTable::open(self.base.join(T::NAME), key_fn)?,
            bufs: Default::default(),
            derived,
        };
        self.store.rcu(|tables| {
            let mut tables = Tables::clone(tables);
            tables.insert(T::NAME, entry.clone());
            tables
        });
        Ok(())
    }

    /// True if table `T` rebuilt its index from its logs when it was opened.
    #[cfg(test)]
    fn rebuilt_on_open<T: Table>(&self) -> bool {
        with_table(&self.store, T::NAME, |entry| entry.table.rebuilt_on_open()).unwrap_or(false)
    }
}

/// Read-only transaction: reads go straight to the shared store (like [`crate::mem_db`]).
#[derive(Clone, Debug)]
pub struct TnDbTx {
    store: Arc<StoreType>,
}

impl DbTx for TnDbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

/// Read-write transaction: writes apply to each table's working state at once and become readable
/// (and durable) at [`DbTxMut::commit`]; the transaction's own [`DbTx::get`] sees them before
/// that. Loose, like [`crate::mem_db`]: there is no rollback, and a table has one working state, so
/// concurrent write transactions on a table see (and a commit publishes) each other's writes.
#[derive(Clone, Debug)]
pub struct TnDbTxMut {
    store: Arc<StoreType>,
    /// The tables this transaction wrote, so `commit` flushes and publishes only those.
    written: Vec<&'static str>,
}

impl TnDbTxMut {
    fn wrote(&mut self, name: &'static str) {
        if !self.written.contains(&name) {
            self.written.push(name);
        }
    }
}

impl DbTx for TnDbTxMut {
    /// Reads the table's working state, so this transaction's own uncommitted writes are visible.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        with_table(&self.store, T::NAME, |entry| {
            with_read_key(key, |key| {
                entry.table.get_working_with(key, |bytes| decode::<T::Value>(bytes))
            })
        })
        .unwrap_or(Ok(None))
    }
}

impl DbTxMut for TnDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.wrote(T::NAME);
        insert::<T>(&self.store, key, value)
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.wrote(T::NAME);
        remove::<T>(&self.store, key)
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.wrote(T::NAME);
        clear_table::<T>(&self.store)
    }

    fn commit(self) -> eyre::Result<()> {
        // Durably flush the log of each table this transaction wrote, then publish its writes.
        for name in self.written {
            if let Some(result) = with_table(&self.store, name, |entry| entry.table.flush()) {
                result?;
            }
        }
        Ok(())
    }
}

impl Database for TnDatabase {
    type TX<'txn>
        = TnDbTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = TnDbTxMut
    where
        Self: 'txn;

    /// Open table `T` as a keyed table (its log stores each row's key), unless it is already
    /// open. A derived-key table must be opened with [`TnDatabase::open_table_with_key`] first.
    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.open_table_in::<T>(None)
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(TnDbTx { store: self.store.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(TnDbTxMut { store: self.store.clone(), written: Vec::new() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        contains_key::<T>(&self.store, key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        // Bare insert is autocommitting per the trait contract.
        insert::<T>(&self.store, key, value)?;
        flush_table::<T>(&self.store)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        remove::<T>(&self.store, key)?;
        flush_table::<T>(&self.store)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        clear_table::<T>(&self.store)?;
        flush_table::<T>(&self.store)
    }

    fn is_empty<T: Table>(&self) -> bool {
        is_empty::<T>(&self.store)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        scan::<T>(&self.store, ScanKind::Forward)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        Ok(scan::<T>(&self.store, ScanKind::From(encode_key(key))))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        scan::<T>(&self.store, ScanKind::Reverse)
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        // The greatest entry strictly less than `key`.
        first_of::<T>(&self.store, ScanKind::RevFrom(encode_key(key)))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        first_of::<T>(&self.store, ScanKind::Reverse)
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;
    use tn_types::{Database as _, DbTxMut as _};

    use super::TnDatabase;
    use crate::test::*;

    /// Open a fresh tndb in a temp dir with the shared `TestTable` opened.  The `TempDir` is
    /// returned so the caller keeps it alive for the duration of the test.
    fn open_db() -> (TnDatabase, TempDir) {
        let tmp = TempDir::with_prefix("tndb").expect("temp dir");
        let db = TnDatabase::open(tmp.path()).expect("open tndb");
        db.open_table::<TestTable>().expect("tndb open table to succeed");
        (db, tmp)
    }

    #[test]
    fn test_tndb_contains_key() {
        let (db, _tmp) = open_db();
        test_contains_key(db);
    }

    #[test]
    fn test_tndb_get() {
        let (db, _tmp) = open_db();
        test_get(db);
    }

    #[test]
    fn test_tndb_multi_get() {
        let (db, _tmp) = open_db();
        test_multi_get(db);
    }

    #[test]
    fn test_tndb_skip() {
        let (db, _tmp) = open_db();
        test_skip(db);
    }

    #[test]
    fn test_tndb_skip_to_previous_simple() {
        let (db, _tmp) = open_db();
        test_skip_to_previous_simple(db);
    }

    #[test]
    fn test_tndb_iter_skip_to_previous_gap() {
        let (db, _tmp) = open_db();
        test_iter_skip_to_previous_gap(db);
    }

    #[test]
    fn test_tndb_remove() {
        let (db, _tmp) = open_db();
        test_remove(db);
    }

    #[test]
    fn test_tndb_iter() {
        let (db, _tmp) = open_db();
        test_iter(db);
    }

    #[test]
    fn test_tndb_iter_reverse() {
        let (db, _tmp) = open_db();
        test_iter_reverse(db);
    }

    #[test]
    fn test_tndb_clear() {
        let (db, _tmp) = open_db();
        test_clear(db);
    }

    #[test]
    fn test_tndb_is_empty() {
        let (db, _tmp) = open_db();
        test_is_empty(db);
    }

    #[test]
    fn test_tndb_multi_insert() {
        let (db, _tmp) = open_db();
        test_multi_insert(db);
    }

    #[test]
    fn test_tndb_multi_remove() {
        let (db, _tmp) = open_db();
        test_multi_remove(db);
    }

    /// Writes reuse their table's encode buffers: a short value written after a long one (directly
    /// and through a write txn) must read back exactly, with no stale tail from the longer encode.
    #[test]
    fn test_tndb_reused_buffers_keep_no_stale_bytes() {
        use tn_types::DbTxMut as _;

        let (db, _tmp) = open_db();
        let long = "x".repeat(4096);
        db.insert::<TestTable>(&1, &long).expect("insert long");
        db.insert::<TestTable>(&2, &"short".to_string()).expect("insert short");
        let mut txn = db.write_txn().expect("write txn");
        txn.insert::<TestTable>(&3, &long).expect("txn insert long");
        txn.insert::<TestTable>(&1, &"s".to_string()).expect("txn overwrite short");
        txn.commit().expect("commit");

        assert_eq!(db.get::<TestTable>(&1).expect("get 1"), Some("s".to_string()));
        assert_eq!(db.get::<TestTable>(&2).expect("get 2"), Some("short".to_string()));
        assert_eq!(db.get::<TestTable>(&3).expect("get 3"), Some(long));

        db.remove::<TestTable>(&2).expect("remove");
        assert!(!db.contains_key::<TestTable>(&2).expect("contains 2"));
        assert!(db.contains_key::<TestTable>(&1).expect("contains 1"));
    }

    /// Dropping the database releases every table (the snapshot map holds the only handles), so
    /// each table's log is sealed by a clean close, and a reopen reads the data back.
    #[test]
    fn test_tndb_reopen_after_drop() {
        use crate::archive::pack::{Pack, PackCompression};

        let tmp = TempDir::with_prefix("tndb_reopen").expect("temp dir");
        {
            let db = TnDatabase::open(tmp.path()).expect("open tndb");
            db.open_table::<TestTable>().expect("open table");
            for i in 0..100u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            }
        }
        {
            let log = Pack::<Vec<u8>>::open(
                tmp.path().join("TestTable").join("gen-0").join("data"),
                0,
                true,
                PackCompression::None,
                1,
            )
            .expect("open the table log read-only");
            assert!(!log.opened_unclean(), "dropping the database must close (seal) its tables");
        }
        let db = TnDatabase::open(tmp.path()).expect("reopen tndb");
        db.open_table::<TestTable>().expect("reopen table");
        assert!(!db.rebuilt_on_open::<TestTable>(), "a clean close needs no rebuild");
        // Readable at once, before any insert: point reads, scans in both directions, seeks.
        for i in 0..100u64 {
            assert_eq!(db.get::<TestTable>(&i).expect("get"), Some(format!("v{i}")));
        }
        assert!(!db.is_empty::<TestTable>());
        let keys: Vec<u64> = db.iter::<TestTable>().map(|(k, _)| k).collect();
        assert_eq!(keys, (0..100).collect::<Vec<_>>(), "a full ascending scan after reopen");
        assert_eq!(db.reverse_iter::<TestTable>().next().map(|(k, _)| k), Some(99));
        assert_eq!(db.last_record::<TestTable>(), Some((99, "v99".to_string())));
        assert_eq!(db.record_prior_to::<TestTable>(&50).map(|(k, _)| k), Some(49));
        // And writable: the reopened index takes new rows alongside the old ones.
        db.insert::<TestTable>(&1_000, &"new".to_string()).expect("insert after reopen");
        assert_eq!(db.get::<TestTable>(&1_000).expect("get"), Some("new".to_string()));
        assert_eq!(db.get::<TestTable>(&7).expect("get"), Some("v7".to_string()));
        assert_eq!(db.iter::<TestTable>().count(), 101);
    }

    /// A full-memory `LayeredDatabase` over a reopened tndb loads every row into its memory layer
    /// at `open_table` (its reads never reach the disk layer), so the rows must be readable from
    /// tndb right after the reopen.
    #[test]
    fn test_tndb_layered_reopen_loads_rows() {
        use crate::layered_db::LayeredDatabase;

        let tmp = TempDir::with_prefix("tndb_layered_reopen").expect("temp dir");
        {
            let db = LayeredDatabase::open(TnDatabase::open(tmp.path()).expect("open tndb"), true);
            db.open_table::<TestTable>().expect("open table");
            for i in 0..500u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            }
            // Dropping the last handle joins the background writer, so every insert is on disk.
        }
        let db = LayeredDatabase::open(TnDatabase::open(tmp.path()).expect("reopen tndb"), true);
        db.open_table::<TestTable>().expect("reopen table");
        assert_eq!(db.iter::<TestTable>().count(), 500, "every row loaded into the memory layer");
        assert_eq!(db.get::<TestTable>(&321).expect("get"), Some("v321".to_string()));
    }

    /// The index is derived data: a corrupt index header is discarded and the index rebuilt from
    /// the logs.
    #[test]
    fn test_tndb_reopen_corrupt_index_header_rebuilds() {
        let tmp = TempDir::with_prefix("tndb_corrupt_btx").expect("temp dir");
        {
            let db = TnDatabase::open(tmp.path()).expect("open tndb");
            db.open_table::<TestTable>().expect("open table");
            db.insert::<TestTable>(&1, &"one".to_string()).expect("insert");
        }
        let index = tmp.path().join("TestTable").join("gen-0").join("btx").join("index.btx");
        let mut bytes = std::fs::read(&index).expect("read index");
        bytes[30] ^= 0xFF; // inside the header's root-page field, under its CRC
        std::fs::write(&index, bytes).expect("write index");

        let db = TnDatabase::open(tmp.path()).expect("reopen tndb");
        db.open_table::<TestTable>().expect("a corrupt index is rebuilt, not fatal");
        assert!(db.rebuilt_on_open::<TestTable>());
        assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("one".to_string()));
    }

    /// A second table for the cross-table tests.
    #[derive(Debug)]
    struct OtherTable;
    impl tn_types::Table for OtherTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "OtherTable";
        const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
    }

    /// Committing a write txn while another table is being scanned does not wait for the scan.
    #[test]
    fn test_tndb_commit_ignores_unrelated_scans() {
        use std::{sync::mpsc, time::Duration};
        use tn_types::DbTxMut as _;

        let (db, _tmp) = open_db();
        db.open_table::<OtherTable>().expect("open other table");
        for i in 0..10u64 {
            db.insert::<TestTable>(&i, &i.to_string()).expect("insert");
        }
        let mut scan = db.iter::<TestTable>();
        assert!(scan.next().is_some());

        // Commit on another thread, so a regression fails the test instead of hanging it.
        let (done_tx, done_rx) = mpsc::channel();
        let writer = db.clone();
        let commit = std::thread::spawn(move || {
            let mut txn = writer.write_txn().expect("write txn");
            txn.insert::<OtherTable>(&1, &"x".to_string()).expect("txn insert");
            done_tx.send(txn.commit().is_ok()).expect("report commit");
        });
        let committed = done_rx.recv_timeout(Duration::from_secs(10));
        drop(scan);
        commit.join().expect("commit thread");
        assert_eq!(committed, Ok(true), "the commit must not wait on an unrelated table's scan");
    }

    /// A write transaction's writes are readable by others only after its commit, while its own
    /// `get` sees them at once; an autocommit write is readable when it returns.
    #[test]
    fn test_tndb_writes_visible_at_commit() {
        use tn_types::{DbTx as _, DbTxMut as _};

        let (db, _tmp) = open_db();
        db.insert::<TestTable>(&1, &"one".to_string()).expect("autocommit insert");
        assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("one".to_string()));

        let mut txn = db.write_txn().expect("write txn");
        txn.insert::<TestTable>(&2, &"two".to_string()).expect("txn insert");
        txn.remove::<TestTable>(&1).expect("txn remove");
        assert_eq!(txn.get::<TestTable>(&2).expect("txn get"), Some("two".to_string()));
        assert_eq!(txn.get::<TestTable>(&1).expect("txn get"), None);
        assert_eq!(db.get::<TestTable>(&2).expect("get"), None, "uncommitted");
        assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("one".to_string()), "uncommitted");
        assert!(!db.contains_key::<TestTable>(&2).expect("contains"));
        let read = db.read_txn().expect("read txn");
        assert_eq!(read.get::<TestTable>(&2).expect("read txn get"), None);

        txn.commit().expect("commit");
        assert_eq!(db.get::<TestTable>(&2).expect("get"), Some("two".to_string()));
        assert_eq!(db.get::<TestTable>(&1).expect("get"), None);
        assert_eq!(read.get::<TestTable>(&2).expect("read txn get"), Some("two".to_string()));
    }

    /// An iterator takes no lock: the same thread can write (and commit) its table mid-iteration,
    /// and the iterator keeps yielding the state it started on.
    #[test]
    fn test_tndb_write_while_iterating_same_table() {
        let (db, _tmp) = open_db();
        for i in 0..10u64 {
            db.insert::<TestTable>(&i, &i.to_string()).expect("insert");
        }
        let mut iter = db.iter::<TestTable>();
        assert_eq!(iter.next(), Some((0, "0".to_string())));
        for i in 0..10u64 {
            db.insert::<TestTable>(&i, &"x".to_string()).expect("overwrite while iterating");
        }
        db.remove::<TestTable>(&5).expect("remove while iterating");
        assert_eq!(
            iter.collect::<Vec<_>>(),
            (1..10u64).map(|i| (i, i.to_string())).collect::<Vec<_>>()
        );
        assert_eq!(db.get::<TestTable>(&3).expect("get"), Some("x".to_string()));
        assert_eq!(db.iter::<TestTable>().count(), 9);
    }

    #[test]
    fn test_tndb_dbsimpbench() {
        let (db, _tmp) = open_db();
        db_simp_bench(db, "TnDb");
    }

    // ---- recovery ----

    /// A derived-key table: its value is `"{key}:{tag}"`, so the key is derived from the value.
    #[derive(Debug)]
    struct DerivedTable;
    impl tn_types::Table for DerivedTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "DerivedTable";
        const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
    }

    // Takes `&String` to be a key function of a `String`-valued table (`Fn(&T::Value) -> T::Key`).
    #[allow(clippy::ptr_arg)]
    fn derived_key(value: &String) -> u64 {
        value.split(':').next().and_then(|k| k.parse().ok()).expect("a derived-table value")
    }

    /// Simulate a crash: the database's tables are never dropped, so no file is sealed and no
    /// index is synced (their mappings stay, as a crashed process's page cache would).
    fn crash<D>(db: D) {
        std::mem::forget(db);
    }

    fn gen_path(base: &std::path::Path, table: &str, generation: u64) -> std::path::PathBuf {
        base.join(table).join(format!("gen-{generation}"))
    }

    /// The end offset of each whole record of the pack log at `path` (header excluded), read
    /// through the log's raw iterator up to its first bad frame.
    fn record_ends(path: &std::path::Path) -> Vec<u64> {
        use crate::archive::pack_iter::PackIter;
        let len = std::fs::metadata(path).expect("log").len();
        let file = std::fs::File::open(path).expect("open log");
        let mut iter = PackIter::<Vec<u8>, _>::open(file, 0, len).expect("log header");
        let mut ends = Vec::new();
        while let Some(Ok(_)) = iter.next_raw() {
            ends.push(iter.logical_position());
        }
        ends
    }

    fn truncate(path: &std::path::Path, len: u64) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open")
            .set_len(len)
            .expect("truncate");
    }

    /// Every row of `T` (by `get` over `0..keys` and by a full scan) matches `model`, as do the
    /// seeks.
    fn assert_matches<T>(db: &TnDatabase, model: &BTreeMap<u64, String>, keys: u64)
    where
        T: tn_types::Table<Key = u64, Value = String>,
    {
        for k in 0..keys {
            assert_eq!(db.get::<T>(&k).expect("get"), model.get(&k).cloned(), "key {k}");
        }
        let rows: BTreeMap<u64, String> = db.iter::<T>().collect();
        assert_eq!(&rows, model, "scan");
        assert_eq!(db.last_record::<T>(), model.last_key_value().map(|(k, v)| (*k, v.clone())));
        let mid = keys / 2;
        assert_eq!(
            db.record_prior_to::<T>(&mid),
            model.range(..mid).next_back().map(|(k, v)| (*k, v.clone()))
        );
    }

    use std::collections::BTreeMap;

    /// A crash keeps every committed write (puts, overwrites, removes, a put after a remove) and
    /// no part of an uncommitted transaction; the next clean close needs no rebuild.
    #[test]
    fn test_tndb_crash_rebuilds_committed_writes() {
        let tmp = TempDir::with_prefix("tndb_crash").expect("temp dir");
        let mut model = BTreeMap::new();
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("open table");
            for i in 0..50u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
                model.insert(i, format!("v{i}"));
            }
            for i in 10..20u64 {
                db.insert::<TestTable>(&i, &format!("w{i}")).expect("overwrite");
                model.insert(i, format!("w{i}"));
            }
            for i in 0..5u64 {
                db.remove::<TestTable>(&i).expect("remove");
                model.remove(&i);
            }
            db.insert::<TestTable>(&2, &"again".to_string()).expect("insert after remove");
            model.insert(2, "again".to_string());
            // Written but never committed: none of it may survive.
            let mut txn = db.write_txn().expect("txn");
            txn.insert::<TestTable>(&100, &"uncommitted".to_string()).expect("insert");
            txn.remove::<TestTable>(&30).expect("remove");
            txn.insert::<TestTable>(&11, &"uncommitted".to_string()).expect("overwrite");
            crash(txn);
            crash(db);
        }
        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table::<TestTable>().expect("reopen table");
        assert!(db.rebuilt_on_open::<TestTable>(), "an unclean close rebuilds");
        assert_matches::<TestTable>(&db, &model, 110);

        db.insert::<TestTable>(&60, &"after".to_string()).expect("insert after recovery");
        model.insert(60, "after".to_string());
        drop(db);
        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table::<TestTable>().expect("reopen table");
        assert!(!db.rebuilt_on_open::<TestTable>(), "a recovered table closes cleanly");
        assert_matches::<TestTable>(&db, &model, 110);
    }

    /// A derived-key table stores only values and rebuilds its keys from them.
    #[test]
    fn test_tndb_derived_key_table_crash_rebuild() {
        let tmp = TempDir::with_prefix("tndb_derived").expect("temp dir");
        let mut model = BTreeMap::new();
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table_with_key::<DerivedTable>(derived_key).expect("open table");
            for i in 0..40u64 {
                let value = format!("{i}:a");
                db.insert::<DerivedTable>(&i, &value).expect("insert");
                model.insert(i, value);
            }
            for i in 5..15u64 {
                let value = format!("{i}:b");
                db.insert::<DerivedTable>(&i, &value).expect("overwrite");
                model.insert(i, value);
            }
            for i in 20..25u64 {
                db.remove::<DerivedTable>(&i).expect("remove");
                model.remove(&i);
            }
            crash(db);
        }
        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table_with_key::<DerivedTable>(derived_key).expect("reopen table");
        assert!(db.rebuilt_on_open::<DerivedTable>());
        assert_matches::<DerivedTable>(&db, &model, 50);

        // The log holds values only: an 8-byte key per put would make it longer.
        let data = gen_path(tmp.path(), "DerivedTable", 0).join("data");
        let ends = record_ends(&data);
        let first_put = ends[0] - crate::archive::pack::DATA_HEADER_BYTES as u64;
        assert_eq!(first_put, 4 + "0:a".len() as u64 + 1 + 4, "frame of [value] only");
    }

    /// A table's key mode is fixed when it is created; re-opening an open table keeps it.
    #[test]
    fn test_tndb_key_mode_fixed_and_opens_idempotent() {
        let tmp = TempDir::with_prefix("tndb_mode").expect("temp dir");
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("keyed");
            db.open_table::<TestTable>().expect("re-open keeps the table");
            assert!(db.open_table_with_key::<TestTable>(derived_key).is_err(), "other mode");
            db.open_table_with_key::<DerivedTable>(derived_key).expect("derived");
            db.open_table::<DerivedTable>().expect("a wrapper's later open keeps it derived");
            db.insert::<DerivedTable>(&7, &"7:x".to_string()).expect("insert");
        }
        let db = TnDatabase::open(tmp.path()).expect("reopen");
        assert!(db.open_table_with_key::<TestTable>(derived_key).is_err(), "keyed table");
        assert!(db.open_table::<DerivedTable>().is_err(), "derived table opened keyed");
        db.open_table_with_key::<DerivedTable>(derived_key).expect("derived table");
        assert_eq!(db.get::<DerivedTable>(&7).expect("get"), Some("7:x".to_string()));
    }

    /// Debug builds check that a derived-key table's key is the key derived from its value.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "derived from its value")]
    fn test_tndb_derived_key_mismatch_panics_in_debug() {
        let (db, _tmp) = open_db();
        db.open_table_with_key::<DerivedTable>(derived_key).expect("derived");
        let _ = db.insert::<DerivedTable>(&5, &"6:wrong".to_string());
    }

    /// Clearing a table deletes its data from disk at once, while a scan started before the clear
    /// keeps reading the rows it started on.
    #[test]
    fn test_tndb_clear_reclaims_disk_and_keeps_snapshots() {
        let tmp = TempDir::with_prefix("tndb_clear").expect("temp dir");
        let db = TnDatabase::open(tmp.path()).expect("open");
        db.open_table::<TestTable>().expect("open table");
        let big = "x".repeat(1024);
        let mut txn = db.write_txn().expect("txn");
        for i in 0..2_000u64 {
            txn.insert::<TestTable>(&i, &format!("{i}{big}")).expect("insert");
        }
        txn.commit().expect("commit");

        let mut scan = db.iter::<TestTable>();
        assert_eq!(scan.next().map(|(k, _)| k), Some(0));
        db.clear_table::<TestTable>().expect("clear");
        assert!(gen_path(tmp.path(), "TestTable", 1).exists());
        // The old generation is deleted in the background, right after the clear.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while gen_path(tmp.path(), "TestTable", 0).exists() {
            assert!(std::time::Instant::now() < deadline, "the old generation is deleted");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(db.is_empty::<TestTable>());
        assert_eq!(db.get::<TestTable>(&5).expect("get"), None);

        // The scan's snapshot still maps the deleted files.
        let rest: Vec<u64> = scan
            .by_ref()
            .map(|(k, v)| {
                assert_eq!(v, format!("{k}{big}"));
                k
            })
            .collect();
        assert_eq!(rest, (1..2_000).collect::<Vec<_>>());
        drop(scan);

        db.insert::<TestTable>(&9, &"new".to_string()).expect("insert after clear");
        drop(db);
        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table::<TestTable>().expect("reopen table");
        assert!(!db.rebuilt_on_open::<TestTable>());
        assert_eq!(db.iter::<TestTable>().collect::<Vec<_>>(), vec![(9, "new".to_string())]);
    }

    /// A torn tail in the data log (a crash mid-write) is cut back to the last commit.
    #[test]
    fn test_tndb_torn_data_tail_cut_to_last_commit() {
        let tmp = TempDir::with_prefix("tndb_torn_data").expect("temp dir");
        let model: BTreeMap<u64, String> = (0..20u64).map(|i| (i, format!("v{i}"))).collect();
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("open table");
            for (k, v) in &model {
                db.insert::<TestTable>(k, v).expect("insert");
            }
            let mut txn = db.write_txn().expect("txn");
            for i in 20..25u64 {
                txn.insert::<TestTable>(&i, &format!("v{i}")).expect("uncommitted");
            }
            crash(txn);
            crash(db);
        }
        let data = gen_path(tmp.path(), "TestTable", 0).join("data");
        let ends = record_ends(&data);
        // Tear the 23rd put (the 3rd uncommitted one) halfway.
        let tear = ends[ends.len() - 3] + 5;
        truncate(&data, tear);

        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table::<TestTable>().expect("reopen table");
        assert!(db.rebuilt_on_open::<TestTable>());
        assert_matches::<TestTable>(&db, &model, 30);
        db.insert::<TestTable>(&99, &"next".to_string()).expect("write after the cut");
        let mut model = model;
        model.insert(99, "next".to_string());
        drop(db);
        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table::<TestTable>().expect("reopen table");
        assert!(!db.rebuilt_on_open::<TestTable>());
        assert_matches::<TestTable>(&db, &model, 100);
    }

    /// A torn removal log keeps the committed removals and drops the uncommitted ones.
    #[test]
    fn test_tndb_torn_removal_tail() {
        let tmp = TempDir::with_prefix("tndb_torn_removed").expect("temp dir");
        let mut model: BTreeMap<u64, String> = (0..20u64).map(|i| (i, format!("v{i}"))).collect();
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("open table");
            for (k, v) in &model {
                db.insert::<TestTable>(k, v).expect("insert");
            }
            for i in 0..4u64 {
                db.remove::<TestTable>(&i).expect("committed remove");
                model.remove(&i);
            }
            let mut txn = db.write_txn().expect("txn");
            for i in 10..13u64 {
                txn.remove::<TestTable>(&i).expect("uncommitted remove");
            }
            crash(txn);
            crash(db);
        }
        let removed = gen_path(tmp.path(), "TestTable", 0).join("removed");
        let ends = record_ends(&removed);
        assert_eq!(ends.len(), 7, "4 committed and 3 uncommitted removals");
        truncate(&removed, ends[5] + 3); // tear the last removal

        let db = TnDatabase::open(tmp.path()).expect("reopen");
        db.open_table::<TestTable>().expect("reopen table");
        assert_matches::<TestTable>(&db, &model, 25);
    }

    /// A cleanly closed log must replay whole: corruption inside it fails the open, changes no
    /// byte, and fails the same way again.
    #[test]
    fn test_tndb_sealed_log_corruption_fails_closed() {
        let tmp = TempDir::with_prefix("tndb_sealed_corrupt").expect("temp dir");
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("open table");
            for i in 0..10u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            }
        }
        let gen0 = gen_path(tmp.path(), "TestTable", 0);
        let data = gen0.join("data");
        let mut bytes = std::fs::read(&data).expect("read");
        let first_payload = crate::archive::pack::DATA_HEADER_BYTES + 4;
        bytes[first_payload + 2] ^= 0xFF;
        std::fs::write(&data, &bytes).expect("write");
        std::fs::remove_dir_all(gen0.join("btx")).expect("drop the index to force a rebuild");

        for attempt in 0..2 {
            let db = TnDatabase::open(tmp.path()).expect("open");
            assert!(db.open_table::<TestTable>().is_err(), "attempt {attempt} must fail closed");
            drop(db);
            assert_eq!(std::fs::read(&data).expect("read"), bytes, "the log is unchanged");
        }
    }

    /// In an unclean log, damage below the commit marker is corruption of committed data, not a
    /// crash tail: the open fails rather than cutting committed records.
    #[test]
    fn test_tndb_tear_below_commit_marker_fails_closed() {
        let tmp = TempDir::with_prefix("tndb_below_marker").expect("temp dir");
        {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("open table");
            for i in 0..10u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            }
            crash(db);
        }
        let data = gen_path(tmp.path(), "TestTable", 0).join("data");
        let mut bytes = std::fs::read(&data).expect("read");
        bytes[crate::archive::pack::DATA_HEADER_BYTES + 6] ^= 0xFF; // the first record
        std::fs::write(&data, &bytes).expect("write");

        let db = TnDatabase::open(tmp.path()).expect("open");
        assert!(db.open_table::<TestTable>().is_err(), "committed data is damaged");
        drop(db);
        assert_eq!(std::fs::read(&data).expect("read"), bytes, "the log is unchanged");
    }

    /// Random puts, removes, clears and commits, with crashes and clean closes between them:
    /// after every reopen the table holds exactly the committed state.
    #[test]
    fn test_tndb_randomized_crash_recovery() {
        const KEYS: u64 = 64;
        let tmp = TempDir::with_prefix("tndb_random_crash").expect("temp dir");
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut rand = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let mut committed: BTreeMap<u64, String> = BTreeMap::new();
        for cycle in 0..12u64 {
            let db = TnDatabase::open(tmp.path()).expect("open");
            db.open_table::<TestTable>().expect("open table");
            assert_matches::<TestTable>(&db, &committed, KEYS);
            let mut pending = committed.clone();
            let mut txn = db.write_txn().expect("txn");
            for step in 0..150u64 {
                let k = rand(KEYS);
                match rand(20) {
                    0..=9 => {
                        let v = format!("{cycle}:{step}");
                        txn.insert::<TestTable>(&k, &v).expect("insert");
                        pending.insert(k, v);
                    }
                    10..=15 => {
                        txn.remove::<TestTable>(&k).expect("remove");
                        pending.remove(&k);
                    }
                    16 => {
                        // Durable at once; the writes before it are gone with the old generation.
                        txn.clear_table::<TestTable>().expect("clear");
                        committed.clear();
                        pending.clear();
                    }
                    _ => {
                        txn.commit().expect("commit");
                        committed = pending.clone();
                        txn = db.write_txn().expect("txn");
                    }
                }
            }
            if cycle % 3 == 2 {
                // A clean close commits the open writes.
                drop(txn);
                drop(db);
                committed = pending;
            } else {
                crash(txn);
                crash(db);
            }
        }
        let db = TnDatabase::open(tmp.path()).expect("open");
        db.open_table::<TestTable>().expect("open table");
        assert_matches::<TestTable>(&db, &committed, KEYS);
    }

    /// A `LayeredDatabase` over a crashed tndb loads the rebuilt rows.
    #[test]
    fn test_tndb_layered_crash_reopen_loads_rows() {
        use crate::layered_db::LayeredDatabase;

        let tmp = TempDir::with_prefix("tndb_layered_crash").expect("temp dir");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        {
            let db = LayeredDatabase::open(TnDatabase::open(tmp.path()).expect("open"), true);
            db.open_table::<TestTable>().expect("open table");
            for i in 0..300u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            }
            rt.block_on(db.persist::<TestTable>()).expect("persist");
            crash(db);
        }
        let db = LayeredDatabase::open(TnDatabase::open(tmp.path()).expect("reopen"), true);
        db.open_table::<TestTable>().expect("reopen table");
        assert_eq!(db.iter::<TestTable>().count(), 300);
        assert_eq!(db.get::<TestTable>(&123).expect("get"), Some("v123".to_string()));
    }
}
