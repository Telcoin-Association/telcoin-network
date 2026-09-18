//! A [`Database`] backed by per-table actors ([`TnTable`]): each table owns an append-only value
//! log plus a sorted B+tree index on its own thread, so operations take no lock — the store maps a
//! table name to that table's `TnTable` handle (a cheap `Clone` channel sender).
//!
//! This module is the typed layer, modeled on [`crate::mem_db`]: keys are encoded with `encode_key`
//! (binary-sortable) and values with `encode` (bcs); each op sends the encoded bytes to the table's
//! actor and decodes the reply.  The index's fixed key length is `encode_key(key).len()` —
//! `size_of::<T::Key>()` is unreliable (e.g. `AuthorityIdentifier` is `Arc<[u8; 32]>`, 8 bytes in
//! memory but 32 encoded).
//!
//! Scans stream lazily off the table actor.  Not yet covered: pack compaction on clear, warm-start
//! reads before the first insert, and durability-barrier tuning.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;
use tn_types::{decode, decode_key, encode, encode_key, DBIter, Database, DbTx, DbTxMut, Table};

use super::table::{ScanKind, TnTable};

type StoreType = DashMap<&'static str, TnTable>;

// ---- shared table operations (used by both the `Database` and the txn impls) ----
//
// Each op clones the table's actor handle out of the `DashMap` and drops the shard `Ref` before the
// (blocking) round-trip, so no lock is held across a table operation.
//
// NOTE: a table actor serves one scan at a time, so a caller must drain or drop an iterator
// (`iter`/`reverse_iter`/`skip_to`) before issuing another blocking op on the *same* table from the
// same thread — an unconsumed scan keeps the actor busy.

/// Clone the table's actor handle out of the store, dropping the `DashMap` shard lock.
fn handle(store: &StoreType, name: &'static str) -> Option<TnTable> {
    store.get(name).map(|h| h.clone())
}

/// Look up a key: read its value bytes from the table, then decode.
fn get<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    let Some(table) = handle(store, T::NAME) else { return Ok(None) };
    Ok(table.get(encode_key(key))?.map(|bytes| decode::<T::Value>(&bytes)))
}

/// Insert `key → value` (no durability flush; callers flush explicitly).
fn insert<T: Table>(store: &StoreType, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
    match handle(store, T::NAME) {
        Some(table) => table.insert(encode_key(key), encode(value)),
        None => Ok(()),
    }
}

/// Remove a key (its log bytes are left as unreferenced garbage; pack compaction is a later step).
fn remove<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<()> {
    if let Some(table) = handle(store, T::NAME) {
        table.remove(encode_key(key))?;
    }
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
        Some(table) => table.contains(encode_key(key)),
        None => Ok(false),
    }
}

/// True if the table is empty (or absent / unreadable).
fn is_empty<T: Table>(store: &StoreType) -> bool {
    handle(store, T::NAME).and_then(|table| table.is_empty().ok()).unwrap_or(false)
}

/// A lazy, key-ordered [`DBIter`] streamed straight off the table actor.
fn scan<T: Table>(store: &StoreType, kind: ScanKind) -> DBIter<'static, T> {
    match handle(store, T::NAME) {
        Some(table) => Box::new(table.scan(kind).map(|(key_bytes, value_bytes)| {
            (decode_key::<T::Key>(&key_bytes), decode::<T::Value>(&value_bytes))
        })),
        None => Box::new(std::iter::empty()),
    }
}

/// The single `(key, value)` a one-shot scan lands on (its first item).
fn first_of<T: Table>(store: &StoreType, kind: ScanKind) -> Option<(T::Key, T::Value)> {
    let (key_bytes, value_bytes) = handle(store, T::NAME)?.scan(kind).next()?;
    Some((decode_key::<T::Key>(&key_bytes), decode::<T::Value>(&value_bytes)))
}

/// A [`Database`] backed by per-table actors ([`super::table::TnTable`]).
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
}

impl DbTx for TnDbTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

impl DbTxMut for TnDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        insert::<T>(&self.store, key, value)
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        remove::<T>(&self.store, key)
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        clear_table::<T>(&self.store)
    }

    fn commit(self) -> eyre::Result<()> {
        // Durably flush every table's log.  Clone the handles out first so no shard lock is held
        // across a (blocking) flush.
        let tables: Vec<TnTable> = self.store.iter().map(|entry| entry.value().clone()).collect();
        for table in tables {
            table.flush()?;
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
        self.store.insert(T::NAME, TnTable::open(self.base.join(T::NAME))?);
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(TnDbTx { store: self.store.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(TnDbTxMut { store: self.store.clone() })
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

    #[test]
    fn test_tndb_dbsimpbench() {
        let (db, _tmp) = open_db();
        db_simp_bench(db, "TnDb");
    }
}
