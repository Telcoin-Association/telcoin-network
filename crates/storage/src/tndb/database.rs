//! A [`Database`] backed by per-table [`TnTable`]s: each table owns an append-only value log plus a
//! sorted B+tree index behind its own lock, and the store maps a table name to that table's
//! `TnTable` handle (a cheap `Clone`).
//!
//! This module is the typed layer, modeled on [`crate::mem_db`]: keys are encoded with `encode_key`
//! (binary-sortable) and values with `encode` (bcs); each op hands the encoded bytes to the table
//! and decodes the reply.  Encodes reuse buffers rather than allocating per op: writes use their
//! table's `EncodeBufs`, point reads a per-thread key buffer.  The index's fixed key length is
//! `encode_key(key).len()` — `size_of::<T::Key>()` is unreliable (e.g. `AuthorityIdentifier` is
//! `Arc<[u8; 32]>`, 8 bytes in memory but 32 encoded).
//!
//! Scans are lazy: each `DBIter` holds its table's read lock and a B+tree cursor, decoding every
//! row straight from the index leaf and the log.  Not yet covered: pack compaction on clear,
//! warm-start reads before the first insert, and durability-barrier tuning.

use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;
use tn_types::{
    decode, decode_key, encode_into_buffer, encode_key, encode_key_into, DBIter, Database, DbTx,
    DbTxMut, Table,
};

use super::table::{ScanKind, TnTable};

/// Reusable encode buffers for a table's writes, so an insert or remove encodes without allocating.
/// The value buffer keeps the capacity of the largest value the table has written.
#[derive(Debug, Default)]
struct EncodeBufs {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// A table's handle plus its write buffers. The buffers sit behind their own lock, cloned out with
/// the handle, so encoding never holds a `DashMap` shard lock. A write holds its table's buffer
/// lock and then takes the table's write lock; the buffer lock is only ever contended by another
/// write to the same table, which would wait on that table's write lock anyway.
#[derive(Debug)]
struct TableStore {
    table: TnTable,
    bufs: Arc<Mutex<EncodeBufs>>,
}

type StoreType = DashMap<&'static str, TableStore>;

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
// Each op clones the table's handle (and, for writes, its buffers) out of the `DashMap` and drops
// the shard `Ref` before the (blocking) table operation, so no shard lock is held across one.
//
// NOTE: an iterator (`iter`/`reverse_iter`/`skip_to`) holds its table's read lock until dropped —
// the same contract as `mem_db`'s iterators. A caller must drop it before writing the *same* table
// on the same thread, and a same-thread read of that table can block behind another thread's
// pending write while the iterator is alive.

/// Clone the table's handle out of the store, dropping the `DashMap` shard lock.
fn handle(store: &StoreType, name: &'static str) -> Option<TnTable> {
    store.get(name).map(|h| h.table.clone())
}

/// Clone the table's handle and its write buffers out of the store, dropping the shard lock.
fn writer(store: &StoreType, name: &'static str) -> Option<(TnTable, Arc<Mutex<EncodeBufs>>)> {
    store.get(name).map(|h| (h.table.clone(), Arc::clone(&h.bufs)))
}

/// Look up a key: read its value bytes from the table, then decode.
fn get<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    let Some(table) = handle(store, T::NAME) else { return Ok(None) };
    // Decode `T::Value` straight from the log's mmap under the read lock (no intermediate `Vec`).
    with_read_key(key, |key| table.get_with(key, |bytes| decode::<T::Value>(bytes)))
}

/// Insert `key → value` (no durability flush; callers flush explicitly).
fn insert<T: Table>(store: &StoreType, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
    let Some((table, bufs)) = writer(store, T::NAME) else { return Ok(()) };
    let mut bufs = bufs.lock();
    let EncodeBufs { key: key_buf, value: value_buf } = &mut *bufs;
    key_buf.clear();
    encode_key_into(key_buf, key)?;
    value_buf.clear();
    encode_into_buffer(value_buf, value)?;
    table.insert(key_buf, value_buf)
}

/// Remove a key (its log bytes are left as unreferenced garbage; pack compaction is a later step).
fn remove<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<()> {
    let Some((table, bufs)) = writer(store, T::NAME) else { return Ok(()) };
    let mut bufs = bufs.lock();
    bufs.key.clear();
    encode_key_into(&mut bufs.key, key)?;
    table.remove(&bufs.key)?;
    Ok(())
}

/// Reset a table to empty (its log bytes become unreferenced garbage until compaction).
fn clear_table<T: Table>(store: &StoreType) -> eyre::Result<()> {
    match handle(store, T::NAME) {
        Some(table) => table.clear(),
        None => Ok(()),
    }
}

/// Durably persist a table's value log.
fn flush_table<T: Table>(store: &StoreType) -> eyre::Result<()> {
    match handle(store, T::NAME) {
        Some(table) => table.flush(),
        None => Ok(()),
    }
}

/// True if the table contains `key`.
fn contains_key<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<bool> {
    match handle(store, T::NAME) {
        Some(table) => with_read_key(key, |key| table.contains(key)),
        None => Ok(false),
    }
}

/// True if the table is empty (or absent / unreadable).
fn is_empty<T: Table>(store: &StoreType) -> bool {
    handle(store, T::NAME).and_then(|table| table.is_empty().ok()).unwrap_or(false)
}

/// A lazy, key-ordered [`DBIter`] over the table (holding its read lock until dropped).
fn scan<T: Table>(store: &StoreType, kind: ScanKind) -> DBIter<'static, T> {
    match handle(store, T::NAME) {
        Some(table) => {
            let mut scan = table.scan(kind);
            Box::new(std::iter::from_fn(move || {
                scan.next_with(|key, value| (decode_key::<T::Key>(key), decode::<T::Value>(value)))
            }))
        }
        None => Box::new(std::iter::empty()),
    }
}

/// The single `(key, value)` a one-shot scan lands on (its first item), found by a direct seek.
fn first_of<T: Table>(store: &StoreType, kind: ScanKind) -> Option<(T::Key, T::Value)> {
    handle(store, T::NAME)?
        .first_with(kind, |key, value| (decode_key::<T::Key>(key), decode::<T::Value>(value)))
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
        Ok(Self { store: Arc::new(DashMap::new()), base })
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

/// Read-write transaction: writes apply immediately to the shared store; durability is deferred to
/// [`DbTxMut::commit`] (loose transactions, matching [`crate::mem_db`]).
#[derive(Clone, Debug)]
pub struct TnDbTxMut {
    store: Arc<StoreType>,
    /// The tables this transaction wrote, so `commit` flushes only those (flushing takes a table's
    /// write lock, which must not wait on an unrelated table's live scan).
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
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
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
        // Durably flush the log of each table this transaction wrote (`handle` drops the shard lock
        // before the blocking flush).
        for name in self.written {
            if let Some(table) = handle(&self.store, name) {
                table.flush()?;
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
        let table = TnTable::open(self.base.join(T::NAME))?;
        self.store.insert(T::NAME, TableStore { table, bufs: Default::default() });
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

    /// A second table for the cross-table tests.
    #[derive(Debug)]
    struct OtherTable;
    impl tn_types::Table for OtherTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "OtherTable";
        const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
    }

    /// Committing a write txn flushes only the tables it wrote: a live scan of another table
    /// (holding that table's read lock) must not block the commit.
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

    #[test]
    fn test_tndb_dbsimpbench() {
        let (db, _tmp) = open_db();
        db_simp_bench(db, "TnDb");
    }
}
