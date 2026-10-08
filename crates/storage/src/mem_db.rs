//! Impermanent storage in memory: the in-memory layer of `LayeredDatabase` (which serves every row
//! of the full-memory tables in production) and the database most tests run on.
//!
//! Each table keeps its rows (`Arc<[u8]>` keys and values, stored once) in two indexes: a sharded
//! hash map for point reads (`get`, `contains_key`), so a lookup is O(1) and takes only the lock
//! of the one shard its key hashes to (readers spread out instead of serializing on a table lock),
//! and an ordered map for scans and seeks. Writes take the ordered map's write lock first, so a
//! table's writers serialize and the two indexes always agree. Tables are found through an
//! immutable snapshot of the table map (`ArcSwap`), and reads encode their key into a per-thread
//! buffer, so a point read writes only its shard's lock and allocates nothing. Writes apply
//! immediately and there are no rollbacks, so a write transaction's `commit` is a no-op.
//!
//! An iterator (`iter`, `reverse_iter`, `skip_to`) holds its table's read lock until dropped: drop
//! it before writing that table on the same thread (a write would wait for it forever).

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::BuildHasherDefault,
    marker::PhantomData,
    ops::Bound,
    sync::Arc,
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use ouroboros::self_referencing;
use parking_lot::{RwLock, RwLockReadGuard};
use serde::Serialize;
use tn_types::{
    decode, decode_key, encode_into_buffer, encode_key, encode_key_into, DBIter, Database, DbTx,
    DbTxMut, Table,
};

use crate::archive::fxhasher::FxHasher;

/// A row's encoded key or value, stored once and shared (by refcount) between a table's two
/// indexes.
type Bytes = Arc<[u8]>;

/// A table's rows in key order: the index scans and seeks use.
type Rows = BTreeMap<Bytes, Bytes>;

/// The same rows by key hash: the index point reads use. Randomly seeded, since some keys are
/// derived from network content.
type PointIndex = DashMap<Bytes, Bytes, foldhash::fast::RandomState>;

/// One table: its rows in both indexes.
#[derive(Debug)]
struct MemTable {
    /// Scans and seeks read it; every write takes its write lock first, so a table's writers
    /// serialize and `point` always matches it once a write returns.
    ordered: RwLock<Rows>,
    point: PointIndex,
}

impl MemTable {
    fn new() -> Self {
        Self {
            ordered: RwLock::new(Rows::new()),
            point: DashMap::with_hasher(foldhash::fast::RandomState::default()),
        }
    }

    fn insert(&self, key: Bytes, value: Bytes) {
        let mut ordered = self.ordered.write();
        self.point.insert(Arc::clone(&key), Arc::clone(&value));
        ordered.insert(key, value);
    }

    fn remove(&self, key: &[u8]) {
        let mut ordered = self.ordered.write();
        if ordered.remove(key).is_some() {
            self.point.remove(key);
        }
    }

    fn clear(&self) {
        let mut ordered = self.ordered.write();
        self.point.clear();
        ordered.clear();
    }
}

/// The open tables by name.
type Tables = HashMap<&'static str, Arc<MemTable>, BuildHasherDefault<FxHasher>>;

/// The table map, read through immutable snapshots: tables are opened at startup and looked up on
/// every op, so a lookup must not write shared memory. `ArcSwap::load` borrows the current snapshot
/// through a per-thread slot (no shared refcount, no lock), and `open_table` publishes a new
/// snapshot (read-copy-update).
type StoreType = ArcSwap<Tables>;

thread_local! {
    /// The per-thread buffer reads encode their key into, so a lookup does not allocate.
    static READ_KEY: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// The per-thread buffer writes encode their value into before copying it into the row.
    static WRITE_VALUE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Encode `key` into this thread's read-key buffer and run `f` on the bytes. `f` must not re-enter
/// the mem db on this thread (the buffer is borrowed for its duration).
fn with_read_key<K: Serialize, R>(key: &K, f: impl FnOnce(&[u8]) -> R) -> eyre::Result<R> {
    READ_KEY.with_borrow_mut(|buf| {
        buf.clear();
        encode_key_into(buf, key)?;
        Ok(f(buf))
    })
}

/// Encode a row into the per-thread buffers, then copy each into its own `Arc<[u8]>` (one
/// allocation for the key, one for the value).
fn encode_row<K: Serialize, V: Serialize>(key: &K, value: &V) -> eyre::Result<(Bytes, Bytes)> {
    let key = with_read_key(key, |key| Bytes::from(key))?;
    let value = WRITE_VALUE.with_borrow_mut(|buf| -> eyre::Result<Bytes> {
        buf.clear();
        encode_into_buffer(buf, value)?;
        Ok(Bytes::from(&buf[..]))
    })?;
    Ok((key, value))
}

/// Run `f` on the named table, borrowed from the current snapshot of the table map, or return
/// `None` if no such table is open.
fn with_table<R>(store: &StoreType, name: &str, f: impl FnOnce(&Arc<MemTable>) -> R) -> Option<R> {
    store.load().get(name).map(f)
}

/// A point read: the key's hash shard only, decoding straight from the stored bytes.
fn get<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    with_table(store, T::NAME, |table| {
        with_read_key(key, |key| table.point.get(key).map(|row| decode(row.value())))
    })
    .unwrap_or(Ok(None))
}

fn insert<T: Table>(store: &StoreType, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
    with_table(store, T::NAME, |table| -> eyre::Result<()> {
        let (key, value) = encode_row(key, value)?;
        table.insert(key, value);
        Ok(())
    })
    .unwrap_or(Ok(()))
}

fn remove<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<()> {
    with_table(store, T::NAME, |table| with_read_key(key, |key| table.remove(key)))
        .unwrap_or(Ok(()))
}

fn clear_table<T: Table>(store: &StoreType) {
    with_table(store, T::NAME, |table| table.clear());
}

/// Read-only transaction over a [`MemDatabase`]; shares the database's table store.
#[derive(Clone, Debug)]
pub struct MemDbTx {
    store: Arc<StoreType>,
}

impl DbTx for MemDbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

/// Read-write transaction over a [`MemDatabase`]; writes apply directly to the shared store
/// (no rollback, so `commit` is a no-op).
#[derive(Clone, Debug)]
pub struct MemDbTxMut {
    store: Arc<StoreType>,
}

impl DbTx for MemDbTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

impl DbTxMut for MemDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        insert::<T>(&self.store, key, value)
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        remove::<T>(&self.store, key)
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        clear_table::<T>(&self.store);
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        // We are already "committed"...
        Ok(())
    }
}

/// Implement the Database trait with an in-memory store.
/// This means no persistance.
/// This DB also plays loose with transactions, but since it is in-memory and we do not do
/// roll-backs this should be fine.
#[derive(Clone, Debug)]
pub struct MemDatabase {
    store: Arc<StoreType>,
}

impl MemDatabase {
    /// Create a new, empty in-memory database.
    pub fn new() -> Self {
        Self { store: Arc::new(ArcSwap::from_pointee(Tables::default())) }
    }

    /// An iterator over the named table's ordered rows built by `rows` from its read guard, or
    /// `None` if no such table is open. The iterator holds the ordered index's read lock until
    /// dropped (point reads do not take it).
    fn table_iter<T: Table>(
        &self,
        rows: impl for<'a> FnOnce(&'a Rows) -> Box<dyn Iterator<Item = (&'a Bytes, &'a Bytes)> + 'a>,
    ) -> Option<MemDBIter<T>> {
        let table = with_table(&self.store, T::NAME, Arc::clone)?;
        Some(
            MemDBIterBuilder {
                table: TabAndGuardBuilder {
                    table,
                    guard_builder: |table| table.ordered.read(),
                    casper: PhantomData::<T>,
                }
                .build(),
                iter_builder: |table: &'_ TabAndGuard<T>| table.with(|fields| rows(fields.guard)),
                casper: PhantomData::<T>,
            }
            .build(),
        )
    }
}

impl MemDatabase {
    /// Insert every `(key, value)` into table `T` under one write lock: a bulk load (e.g. the
    /// layered database filling its in-memory layer at open), without a lock round trip per row.
    /// Rows for keys already present are replaced, as by [`Database::insert`].
    pub fn insert_all<T: Table>(
        &self,
        rows: impl IntoIterator<Item = (T::Key, T::Value)>,
    ) -> eyre::Result<()> {
        with_table(&self.store, T::NAME, |table| -> eyre::Result<()> {
            let mut ordered = table.ordered.write();
            for (key, value) in rows {
                let (key, value) = encode_row(&key, &value)?;
                table.point.insert(Arc::clone(&key), Arc::clone(&value));
                ordered.insert(key, value);
            }
            Ok(())
        })
        .unwrap_or(Ok(()))
    }
}

impl Default for MemDatabase {
    fn default() -> Self {
        let db = Self::new();
        let _ = db.open_table::<crate::tables::LastProposed>();
        let _ = db.open_table::<crate::tables::Votes>();
        let _ = db.open_table::<crate::tables::Certificates>();
        let _ = db.open_table::<crate::tables::CertificateDigestByRound>();
        let _ = db.open_table::<crate::tables::CertificateDigestByOrigin>();
        let _ = db.open_table::<crate::tables::ProposedCertificates>();
        let _ = db.open_table::<crate::tables::Payload>();
        let _ = db.open_table::<crate::tables::NodeBatchesCache>();
        let _ = db.open_table::<crate::tables::OurNodeBatchesCache>();
        let _ = db.open_table::<crate::tables::ConsensusCache>();
        let _ = db.open_table::<crate::tables::KadRecords>();
        let _ = db.open_table::<crate::tables::KadProviderRecords>();
        let _ = db.open_table::<crate::tables::KadWorkerRecords>();
        let _ = db.open_table::<crate::tables::KadWorkerProviderRecords>();
        db
    }
}

impl Database for MemDatabase {
    type TX<'txn>
        = MemDbTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = MemDbTxMut
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        // (Re)opening a table replaces it with an empty one. Built once, outside the
        // read-copy-update (whose closure may run again on a race).
        let table = Arc::new(MemTable::new());
        self.store.rcu(|tables| {
            let mut tables = Tables::clone(tables);
            tables.insert(T::NAME, Arc::clone(&table));
            tables
        });
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(MemDbTx { store: Arc::clone(&self.store) })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MemDbTxMut { store: Arc::clone(&self.store) })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        with_table(&self.store, T::NAME, |table| {
            with_read_key(key, |key| table.point.contains_key(key))
        })
        .unwrap_or(Ok(false))
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        insert::<T>(&self.store, key, value)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        remove::<T>(&self.store, key)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        clear_table::<T>(&self.store);
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        with_table(&self.store, T::NAME, |table| table.ordered.read().is_empty()).unwrap_or(false)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.table_iter::<T>(|rows| Box::new(rows.iter())) {
            Some(iter) => Box::new(iter),
            None => panic!("Invalid table {}", T::NAME),
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let key = encode_key(key);
        let iter = self.table_iter::<T>(move |rows| {
            Box::new(rows.range::<[u8], _>((Bound::Included(&key[..]), Bound::Unbounded)))
        });
        match iter {
            Some(iter) => Ok(Box::new(iter)),
            None => Err(eyre::eyre!("Invalid table {}", T::NAME)),
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.table_iter::<T>(|rows| Box::new(rows.iter().rev())) {
            Some(iter) => Box::new(iter),
            None => panic!("Invalid table {}", T::NAME),
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        with_table(&self.store, T::NAME, |table| {
            with_read_key(key, |key| {
                table
                    .ordered
                    .read()
                    .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(key)))
                    .next_back()
                    .map(|(key, value)| (decode_key(key), decode(value)))
            })
            .ok()
            .flatten()
        })
        .flatten()
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        with_table(&self.store, T::NAME, |table| {
            table
                .ordered
                .read()
                .last_key_value()
                .map(|(key, value)| (decode_key(key), decode(value)))
        })
        .flatten()
    }
}

#[self_referencing]
struct TabAndGuard<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: Arc<MemTable>,
    #[borrows(table)]
    #[covariant]
    guard: RwLockReadGuard<'this, Rows>,
}

#[self_referencing]
pub struct MemDBIter<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: TabAndGuard<T>,
    #[borrows(table)]
    #[not_covariant]
    iter: Box<dyn Iterator<Item = (&'this Bytes, &'this Bytes)> + 'this>,
}

impl<T: Table> Iterator for MemDBIter<T> {
    type Item = (T::Key, T::Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| {
            fields.iter.next().map(|(key_bytes, value_bytes)| {
                let key = decode_key(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        })
    }
}

#[cfg(test)]
mod test {
    use tn_types::Database as _;

    use crate::{mem_db::MemDatabase, test::*};

    fn open_db() -> MemDatabase {
        let db = MemDatabase::new();
        db.open_table::<TestTable>().expect("mem db open to succeed");
        db
    }

    #[test]
    fn test_memdb_contains_key() {
        let db = open_db();
        test_contains_key(db)
    }

    #[test]
    fn test_memdb_get() {
        let db = open_db();
        test_get(db)
    }

    #[test]
    fn test_memdb_multi_get() {
        let db = open_db();
        test_multi_get(db)
    }

    #[test]
    fn test_memdb_skip() {
        let db = open_db();
        test_skip(db)
    }

    #[test]
    fn test_memdb_skip_to_previous_simple() {
        let db = open_db();
        test_skip_to_previous_simple(db)
    }

    #[test]
    fn test_memdb_iter_skip_to_previous_gap() {
        let db = open_db();
        test_iter_skip_to_previous_gap(db)
    }

    #[test]
    fn test_memdb_remove() {
        let db = open_db();
        test_remove(db)
    }

    #[test]
    fn test_memdb_iter() {
        let db = open_db();
        test_iter(db)
    }

    #[test]
    fn test_memdb_iter_reverse() {
        let db = open_db();
        test_iter_reverse(db)
    }

    #[test]
    fn test_memdb_clear() {
        let db = open_db();
        test_clear(db)
    }

    #[test]
    fn test_memdb_is_empty() {
        let db = open_db();
        test_is_empty(db)
    }

    #[test]
    fn test_memdb_multi_insert() {
        // Init a DB
        let db = open_db();
        test_multi_insert(db)
    }

    #[test]
    fn test_memdb_multi_remove() {
        // Init a DB
        let db = open_db();
        test_multi_remove(db)
    }

    /// `skip_to` and `record_prior_to` seek correctly at every boundary: a present key, an absent
    /// one, before the first and after the last.
    #[test]
    fn test_memdb_seek_boundaries() {
        use tn_types::DBIter;

        let db = open_db();
        for k in [10_u64, 20, 30] {
            db.insert::<TestTable>(&k, &k.to_string()).expect("insert");
        }
        let keys = |iter: DBIter<'_, TestTable>| iter.map(|(k, _)| k).collect::<Vec<_>>();
        let skip = |k: u64| keys(db.skip_to::<TestTable>(&k).expect("skip_to"));
        assert_eq!(skip(20), vec![20, 30], "present key");
        assert_eq!(skip(15), vec![20, 30], "absent key");
        assert_eq!(skip(5), vec![10, 20, 30], "before the first");
        assert!(skip(35).is_empty(), "after the last");
        let prior = |k: u64| db.record_prior_to::<TestTable>(&k);
        assert_eq!(prior(20), Some((10, "10".to_string())), "present key");
        assert_eq!(prior(25), Some((20, "20".to_string())), "absent key");
        assert_eq!(prior(10), None, "nothing before the first");
        assert_eq!(prior(99), Some((30, "30".to_string())), "after the last");
        assert_eq!(db.last_record::<TestTable>(), Some((30, "30".to_string())));
    }

    /// Readers on several threads against a writer that keeps overwriting every key: every read
    /// sees a whole value for its key, never a torn or foreign one.
    #[test]
    fn test_memdb_readers_against_writer() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        const KEYS: u64 = 200;
        let db = open_db();
        for k in 0..KEYS {
            db.insert::<TestTable>(&k, &format!("{k}:0")).expect("insert");
        }
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4_u64)
            .map(|t| {
                let (db, stop) = (db.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut x = t + 1;
                    let mut reads = 0_u64;
                    while !stop.load(Ordering::Relaxed) {
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let k = (x >> 33) % KEYS;
                        let v = db.get::<TestTable>(&k).expect("get").expect("always present");
                        assert!(v.starts_with(&format!("{k}:")), "a foreign value {v} for {k}");
                        if reads.is_multiple_of(64) {
                            assert_eq!(db.iter::<TestTable>().count() as u64, KEYS);
                        }
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();
        for generation in 1..=200_u64 {
            for k in 0..KEYS {
                db.insert::<TestTable>(&k, &format!("{k}:{generation}")).expect("overwrite");
            }
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            assert!(reader.join().expect("reader") > 0);
        }
    }

    /// A second table, to show a write to one table leaves another untouched.
    #[derive(Debug)]
    struct OtherTable;

    impl tn_types::Table for OtherTable {
        type Key = u64;
        type Value = String;
        const NAME: &'static str = "OtherTable";
        const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
    }

    /// `insert_all` loads a table to exactly what per-row inserts would, replacing keys already
    /// present, and touches no other table.
    #[test]
    fn test_memdb_insert_all_matches_inserts() {
        let bulk = open_db();
        let rows = open_db();
        for db in [&bulk, &rows] {
            db.open_table::<OtherTable>().expect("open other");
            db.insert::<OtherTable>(&1, &"other".to_string()).expect("insert other");
            db.insert::<TestTable>(&5, &"old".to_string()).expect("insert");
        }
        let data: Vec<(u64, String)> = (0..1_000_u64).map(|k| (k, format!("v{k}"))).collect();
        bulk.insert_all::<TestTable>(data.clone()).expect("insert_all");
        for (k, v) in &data {
            rows.insert::<TestTable>(k, v).expect("insert");
        }
        assert_eq!(
            bulk.iter::<TestTable>().collect::<Vec<_>>(),
            rows.iter::<TestTable>().collect::<Vec<_>>()
        );
        assert_eq!(bulk.get::<TestTable>(&5).expect("get"), Some("v5".to_string()));
        assert_eq!(bulk.get::<OtherTable>(&1).expect("get"), Some("other".to_string()));
    }

    /// The point index (`get`, `contains_key`) and the ordered index (scans) always agree: after
    /// every random insert, overwrite, remove, `insert_all` and clear, each key in the domain
    /// (present or absent) reads the same through both, and matches a model map.
    #[test]
    fn test_memdb_point_and_ordered_indexes_agree() {
        use std::collections::BTreeMap;

        let db = open_db();
        let mut model: BTreeMap<u64, String> = BTreeMap::new();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rand = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..2_000_u64 {
            let k = rand() % 300;
            match rand() % 10 {
                0..=4 => {
                    let v = format!("{k}:{step}");
                    db.insert::<TestTable>(&k, &v).expect("insert");
                    model.insert(k, v);
                }
                5..=7 => {
                    db.remove::<TestTable>(&k).expect("remove");
                    model.remove(&k);
                }
                8 => {
                    let rows: Vec<(u64, String)> = (0..20)
                        .map(|_| rand() % 300)
                        .map(|k| (k, format!("{k}:bulk{step}")))
                        .collect();
                    db.insert_all::<TestTable>(rows.clone()).expect("insert_all");
                    model.extend(rows);
                }
                _ if step % 500 == 499 => {
                    db.clear_table::<TestTable>().expect("clear");
                    model.clear();
                }
                _ => {}
            }
            if step % 50 == 0 || step == 1_999 {
                let scanned: BTreeMap<u64, String> = db.iter::<TestTable>().collect();
                assert_eq!(scanned, model, "ordered index vs model at step {step}");
                for k in 0..300_u64 {
                    let expected = model.get(&k).cloned();
                    assert_eq!(db.get::<TestTable>(&k).expect("get"), expected, "key {k}");
                    assert_eq!(
                        db.contains_key::<TestTable>(&k).expect("contains"),
                        expected.is_some(),
                        "key {k}"
                    );
                }
                assert_eq!(db.is_empty::<TestTable>(), model.is_empty());
            }
        }
    }

    #[test]
    fn test_memdb_dbsimpbench() {
        // Init a DB
        let db = open_db();
        db_simp_bench(db, "MemDb");
    }
}
