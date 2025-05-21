//! Impermanent storage in memory - useful for tests.

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    marker::PhantomData,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use ouroboros::self_referencing;
use parking_lot::{RwLock, RwLockReadGuard};
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use tn_types::{decode, decode_key, encode, encode_key, DBIter, Database, DbTx, DbTxMut, Table};

type StoreType = DashMap<&'static str, Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>>;

fn get<T: Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode_key(key);
        if let Some(val_bytes) = table.read().get(&key_bytes) {
            let val = decode(val_bytes);
            return Ok(Some(val));
        }
    }
    Ok(None)
}

#[derive(Clone, Debug)]
pub struct MemDbTx {
    store: StoreType,
}

impl DbTx for MemDbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

#[derive(Clone, Debug)]
pub struct MemDbTxMut {
    store: StoreType,
}

impl DbTx for MemDbTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

impl DbTxMut for MemDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            let value_bytes = encode(value);
            table.write().insert(key_bytes, value_bytes);
        }
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            table.write().remove(&key_bytes);
        }
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            table.write().clear();
        }
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
    metrics: Arc<RwLock<MemDBMetrics>>,
    shutdown_tx: Arc<SyncSender<()>>,
}

impl Drop for MemDatabase {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shutdown_tx) <= 1 {
            tracing::info!(target: "telcoin::memdb", "MemDatabase Dropping, shutting down metrics thread");
            // shutdown_tx is a sync sender with no buffer so this should block until the thread
            // reads it and shuts down.
            if let Err(e) = self.shutdown_tx.send(()) {
                tracing::error!(target: "telcoin::memdb",
                    "Error while trying to send shutdown to MemDatabase metrics thread {e}"
                );
            }
        }
    }
}

impl MemDatabase {
    pub fn new() -> Self {
        let store: Arc<StoreType> = Arc::new(DashMap::new());
        let metrics = Arc::new(RwLock::new(MemDBMetrics::default()));
        let (shutdown_tx, rx) = mpsc::sync_channel::<()>(0);

        let store_cloned = Arc::clone(&store);
        let metrics_cloned = metrics.clone();
        // Spawn thread to update metrics from MemDB stats every 30 seconds.
        std::thread::spawn(move || {
            tracing::info!(target: "telcoin::memdb", "Starting MemDB metrics thread");
            while let Err(mpsc::RecvTimeoutError::Timeout) =
                rx.recv_timeout(Duration::from_secs(30))
            {
                for kv in &*store_cloned {
                    if let Some(m) = metrics_cloned.read().table_counts.get(kv.key()) {
                        m.set(kv.value().read().len().try_into().unwrap_or(-1));
                    }
                }
            }
            tracing::info!(target: "telcoin::memdb", "Ending MemDB metrics thread");
        });

        Self { store, metrics, shutdown_tx: Arc::new(shutdown_tx) }
    }

    pub fn open_table<T: Table>(&self) {
        self.store.insert(T::NAME, Arc::new(RwLock::new(BTreeMap::new())));
        match register_int_gauge_with_registry!(
            format!("memdb_{}_count", T::NAME),
            format!("Entries in the {} memory table.", T::NAME),
            default_registry(),
        ) {
            Ok(m) => {
                self.metrics.write().table_counts.insert(T::NAME, m);
            }
            Err(e) => {
                // This will happen for tests.  Nothing really to do, if the guage is missing then
                // the metrics thread will just not update it... Log at debug level
                // in case something else is going on and someone is debugging.
                tracing::debug!(target: "telcoin::memdb", "Error adding metrics for table {}: {e}", T::NAME)
            }
        }
    }

    /// Create a new instance of [Self] with network tables.
    pub fn new_for_network() -> Self {
        let db = Self::new();
        db.open_table::<crate::tables::KadRecords>();
        db.open_table::<crate::tables::KadProviderRecords>();
        db
    }
}

impl Default for MemDatabase {
    fn default() -> Self {
        let db = Self::new();
        db.open_table::<crate::tables::LastProposed>();
        db.open_table::<crate::tables::Votes>();
        db.open_table::<crate::tables::Certificates>();
        db.open_table::<crate::tables::CertificateDigestByRound>();
        db.open_table::<crate::tables::CertificateDigestByOrigin>();
        db.open_table::<crate::tables::Payload>();
        db.open_table::<crate::tables::Batches>();
        db.open_table::<crate::tables::ConsensusBlocks>();
        db.open_table::<crate::tables::ConsensusBlockNumbersByDigest>();
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

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(MemDbTx { store: (*self.store).clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MemDbTxMut { store: (*self.store).clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            return Ok(table.read().contains_key(&key_bytes));
        }
        Ok(false)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            let value_bytes = encode(value);
            table.write().insert(key_bytes, value_bytes);
        }
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            table.write().remove(&key_bytes);
        }
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            table.write().clear();
        }
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        if let Some(table) = self.store.get(T::NAME) {
            table.read().is_empty()
        } else {
            false
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            Box::new(
                MemDBIterBuilder {
                    table: TabAndGuardBuilder {
                        table: table.clone(),
                        guard_builder: |table| table.read(),
                        casper: PhantomData::<T>,
                    }
                    .build(),
                    iter_builder: |table: &'_ TabAndGuard<T>| {
                        table.with(|fields| {
                            let iter = Box::new(fields.guard.iter());
                            iter
                        })
                    },
                    casper: PhantomData::<T>,
                }
                .build(),
            )
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        if let Some(table) = self.store.get(T::NAME) {
            Ok(Box::new(
                MemDBIterBuilder {
                    table: TabAndGuardBuilder {
                        table: table.clone(),
                        guard_builder: |table| table.read(),
                        casper: PhantomData::<T>,
                    }
                    .build(),
                    iter_builder: |table: &'_ TabAndGuard<T>| {
                        table.with(|fields| {
                            let key_bytes = encode_key(key);
                            let iter = Box::new(
                                fields.guard.iter().skip_while(move |(k, _)| **k < key_bytes),
                            );
                            iter
                        })
                    },
                    casper: PhantomData::<T>,
                }
                .build(),
            ))
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            Box::new(
                MemDBIterBuilder {
                    table: TabAndGuardBuilder {
                        table: table.clone(),
                        guard_builder: |table| table.read(),
                        casper: PhantomData::<T>,
                    }
                    .build(),
                    iter_builder: |table: &'_ TabAndGuard<T>| {
                        table.with(|fields| {
                            let iter = Box::new(fields.guard.iter().rev());
                            iter
                        })
                    },
                    casper: PhantomData::<T>,
                }
                .build(),
            )
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            let mut last = None;
            let guard = table.read();
            for (k, v) in guard.iter() {
                if k >= &key_bytes {
                    break;
                }
                last = Some((k, v));
            }
            last.map(|(key_bytes, value_bytes)| {
                let key = decode_key(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        } else {
            None
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            table.read().last_key_value().map(|(key_bytes, value_bytes)| {
                let key = decode_key(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        } else {
            None
        }
    }
}

#[self_referencing]
struct TabAndGuard<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    #[borrows(table)]
    #[covariant]
    guard: RwLockReadGuard<'this, BTreeMap<Vec<u8>, Vec<u8>>>,
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
    iter: Box<dyn Iterator<Item = (&'this Vec<u8>, &'this Vec<u8>)> + 'this>,
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

#[derive(Debug)]
struct MemDBMetrics {
    table_counts: HashMap<&'static str, IntGauge>,
}

impl MemDBMetrics {
    fn try_new(_registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self { table_counts: HashMap::default() })
    }
}

impl Default for MemDBMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(_) => {
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{mem_db::MemDatabase, test::*};

    fn open_db() -> MemDatabase {
        let db = MemDatabase::new();
        db.open_table::<TestTable>();
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

    #[test]
    fn test_memdb_dbsimpbench() {
        // Init a DB
        let db = open_db();
        db_simp_bench(db, "MemDb");
    }
}
