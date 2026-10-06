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
//! decoding every row straight from the index leaf and the log.  Not yet covered: pack compaction
//! on clear, warm-start reads before the first insert, and durability-barrier tuning.

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
    decode, decode_key, encode_into_buffer, encode_key, encode_key_into, DBIter, Database, DbTx,
    DbTxMut, Table,
};

use super::table::{ScanKind, TnTable};
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
}

impl TnDatabase {
    /// Open (creating the directory if needed) a tndb rooted at `path`.  Call
    /// [`Database::open_table`] for each table before use.
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        let base = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base)?;
        Ok(Self { store: Arc::new(ArcSwap::from_pointee(Tables::default())), base })
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

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        // Open once, outside the read-copy-update (whose closure may run again on a race), then
        // publish a snapshot with the table added (replacing any earlier open of the same name).
        let entry =
            TableStore { table: TnTable::open(self.base.join(T::NAME))?, bufs: Default::default() };
        self.store.rcu(|tables| {
            let mut tables = Tables::clone(tables);
            tables.insert(T::NAME, entry.clone());
            tables
        });
        Ok(())
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
    use tn_types::Database as _;

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
                tmp.path().join("TestTable").join("data"),
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
        // The index reopens on the first insert (reads before it are a known gap).
        db.insert::<TestTable>(&1_000, &"new".to_string()).expect("insert after reopen");
        for i in 0..100u64 {
            assert_eq!(db.get::<TestTable>(&i).expect("get"), Some(format!("v{i}")));
        }
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
}
