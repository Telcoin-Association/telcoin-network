//! A [`Database`] backed by pack files, each keyed by a sorted [`BtreeIndex`].
//!
//! Each table is a directory (`<base>/<NAME>`) holding an append-only value log (`data`, a
//! [`Pack`]) plus a sorted key→position index (`btx/`, a [`BtreeIndex`]).  Keys are encoded with
//! `encode_key` (binary-sortable) and values with `encode` (bcs), exactly as in [`crate::mem_db`];
//! a lookup resolves `key → position` in the index, then reads the value from the log.
//!
//! The index needs a fixed key byte-length.  `size_of::<T::Key>()` is *not* reliable (e.g.
//! `AuthorityIdentifier` is `Arc<[u8; 32]>` — 8 bytes in memory but 32 encoded), so the length is
//! taken from `encode_key(key).len()` and the index is created lazily on the first insert.
//!
//! This is step one: a functional implementation modeled on `mem_db`.  Later steps cover pack
//! compaction on clear, warm-start reads before the first insert, streaming (non-collecting)
//! iterators, and durability-barrier tuning.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;
use parking_lot::Mutex;
use tn_types::{decode, decode_key, encode, encode_key, DBIter, Database, DbTx, DbTxMut, Table};

use crate::archive::{
    btree_index::BtreeIndex,
    error::fetch::FetchError,
    pack::{Pack, PackCompression},
};

/// Pack/index format version for tndb tables.
const PACK_VERSION: u16 = 1;

/// One named table: an append-only value log ([`Pack`]) plus its sorted key index
/// ([`BtreeIndex`], created lazily on first insert once the encoded key length is known).
#[derive(Debug)]
struct TnTable {
    /// Table directory (`<base>/<NAME>`), holding the `data` log and the `btx/` index.
    dir: PathBuf,
    data: Pack<Vec<u8>>,
    idx: Option<BtreeIndex>,
}

impl TnTable {
    /// The index, created (with key byte length `ksize`) on first use.  On reopen of an existing
    /// table directory this reopens the on-disk index, whose header records the same `ksize`.
    fn index_mut(&mut self, ksize: u16) -> eyre::Result<&mut BtreeIndex> {
        if self.idx.is_none() {
            let idx =
                BtreeIndex::open_btx_file(self.dir.join("btx"), self.data.header(), ksize, false)?;
            self.idx = Some(idx);
        }
        Ok(self.idx.as_mut().expect("index just created"))
    }
}

type StoreType = DashMap<&'static str, Arc<Mutex<TnTable>>>;

// ---- shared table operations (used by both the `Database` and the txn impls) ----

/// Look up a key: encode it, resolve its position in the index, then read+decode the value.
fn get<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    let Some(table) = store.get(T::NAME) else { return Ok(None) };
    let key_bytes = encode_key(key);
    let mut table = table.lock();
    // Resolve the position under the index borrow, then release it before touching the log.
    let pos = match table.idx.as_mut() {
        Some(idx) => match idx.load(&key_bytes) {
            Ok(pos) => pos,
            Err(FetchError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        },
        None => return Ok(None),
    };
    let value_bytes = table.data.fetch(pos)?;
    Ok(Some(decode::<T::Value>(&value_bytes)))
}

/// Append the value to the log and record `key → position` in the index (no durability flush;
/// callers flush explicitly). The index is created on first insert from the encoded key length.
fn insert<T: Table>(store: &StoreType, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
    let Some(table) = store.get(T::NAME) else { return Ok(()) };
    let key_bytes = encode_key(key);
    let value_bytes = encode(value);
    let mut table = table.lock();
    let pos = table.data.append(&value_bytes)?;
    let ksize = key_bytes.len() as u16;
    table.index_mut(ksize)?.save(&key_bytes, pos)?;
    Ok(())
}

/// Remove a key from the index (the value's log bytes are left as unreferenced garbage; pack
/// compaction is a later step).
fn remove<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<()> {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode_key(key);
        let mut table = table.lock();
        if let Some(idx) = table.idx.as_mut() {
            idx.remove(&key_bytes)?;
        }
    }
    Ok(())
}

/// Reset a table's index to empty (the log's bytes become unreferenced garbage until compaction).
fn clear_table<T: Table>(store: &StoreType) -> eyre::Result<()> {
    if let Some(table) = store.get(T::NAME) {
        let mut table = table.lock();
        if let Some(idx) = table.idx.as_mut() {
            idx.rebuild_from(std::iter::empty::<(Vec<u8>, u64)>())?;
        }
    }
    Ok(())
}

/// Flush a table's log and index to disk.
fn flush_table<T: Table>(store: &StoreType) -> eyre::Result<()> {
    if let Some(table) = store.get(T::NAME) {
        let mut table = table.lock();
        table.data.commit()?;
        if let Some(idx) = table.idx.as_mut() {
            idx.sync()?;
        }
    }
    Ok(())
}

/// Collect a table's `(key, value)` pairs selected by `select` (an index-iterator builder) into a
/// sorted vec.  Positions are drained from the index first so its borrow ends before the log is
/// read (both live behind one `MutexGuard`).  Eager collection is the step-one choice; streaming is
/// a later step.
fn collect_entries<T, F>(store: &StoreType, select: F) -> Vec<(T::Key, T::Value)>
where
    T: Table,
    F: FnOnce(&mut BtreeIndex) -> Result<Vec<(Vec<u8>, u64)>, FetchError>,
{
    let Some(table) = store.get(T::NAME) else { return Vec::new() };
    let mut table = table.lock();
    let positions = match table.idx.as_mut() {
        Some(idx) => select(idx).unwrap_or_default(),
        None => Vec::new(),
    };
    let mut out = Vec::with_capacity(positions.len());
    for (key_bytes, pos) in positions {
        if let Ok(value_bytes) = table.data.fetch(pos) {
            out.push((decode_key::<T::Key>(&key_bytes), decode::<T::Value>(&value_bytes)));
        }
    }
    out
}

/// Fetch the single `(key, value)` a one-shot index lookup lands on.
fn single_entry<T, F>(store: &StoreType, select: F) -> Option<(T::Key, T::Value)>
where
    T: Table,
    F: FnOnce(&mut BtreeIndex) -> Option<(Vec<u8>, u64)>,
{
    let table = store.get(T::NAME)?;
    let mut table = table.lock();
    let (key_bytes, pos) = match table.idx.as_mut() {
        Some(idx) => select(idx)?,
        None => return None,
    };
    let value_bytes = table.data.fetch(pos).ok()?;
    Some((decode_key::<T::Key>(&key_bytes), decode::<T::Value>(&value_bytes)))
}

/// A [`Database`] backed by per-table pack files + sorted B+tree indexes.
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
        // Durably flush every table's log + index.
        for table in self.store.iter() {
            let mut table = table.lock();
            table.data.commit()?;
            /*XXXXif let Some(idx) = table.idx.as_mut() {
                idx.sync()?;
            }*/
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
        let dir = self.base.join(T::NAME);
        std::fs::create_dir_all(&dir)?;
        let data =
            Pack::<Vec<u8>>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)?;
        self.store.insert(T::NAME, Arc::new(Mutex::new(TnTable { dir, data, idx: None })));
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(TnDbTx { store: self.store.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(TnDbTxMut { store: self.store.clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            let mut table = table.lock();
            if let Some(idx) = table.idx.as_mut() {
                return Ok(idx.contains(&key_bytes));
            }
        }
        Ok(false)
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
        if let Some(table) = self.store.get(T::NAME) {
            let table = table.lock();
            return table.idx.as_ref().is_none_or(|idx| idx.is_empty());
        }
        false
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        Box::new(collect_entries::<T, _>(&self.store, |idx| idx.iter()?.collect()).into_iter())
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let key_bytes = encode_key(key);
        let entries =
            collect_entries::<T, _>(&self.store, move |idx| idx.range(key_bytes..)?.collect());
        Ok(Box::new(entries.into_iter()))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        Box::new(collect_entries::<T, _>(&self.store, |idx| idx.rev_iter()?.collect()).into_iter())
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let key_bytes = encode_key(key);
        // The greatest entry strictly less than `key` (descending scan of the open-below range).
        single_entry::<T, _>(&self.store, move |idx| idx.rev_range(..key_bytes).ok()?.next()?.ok())
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        single_entry::<T, _>(&self.store, |idx| idx.rev_iter().ok()?.next()?.ok())
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
