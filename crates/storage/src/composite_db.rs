//! Define a composite DB that presents a single Database interface but
//! is intenally split into multiple Databases to split workload up.

use std::{future::Future, sync::Arc};

use tn_types::{Database, DbTx, DbTxMut, Table, TableHint};

use crate::layered_db::{LayeredDatabase, LayeredDbTxMut};

#[derive(Clone, Debug)]
struct Inner<DB: Database> {
    epoch_db: LayeredDatabase<DB>,
    batch_db: LayeredDatabase<DB>,
    consensus_chain_db: LayeredDatabase<DB>,
    epoch_chain_db: LayeredDatabase<DB>,
    kad_db: LayeredDatabase<DB>,
    cache_db: LayeredDatabase<DB>,
}

/// A composite DB that is composed of multiple [`LayeredDatabase`]s.
#[derive(Clone, Debug)]
pub struct CompositeDatabase<DB: Database> {
    inner: Arc<Inner<DB>>,
}

impl<DB: Database> CompositeDatabase<DB> {
    pub fn open(
        epoch_db: DB,
        batch_db: DB,
        consensus_chain_db: DB,
        epoch_chain_db: DB,
        kad_db: DB,
        cache_db: DB,
    ) -> Self {
        let epoch_db = LayeredDatabase::open(epoch_db, true);
        let batch_db = LayeredDatabase::open(batch_db, false);
        let consensus_chain_db = LayeredDatabase::open(consensus_chain_db, false);
        let epoch_chain_db = LayeredDatabase::open(epoch_chain_db, false);
        let kad_db = LayeredDatabase::open(kad_db, true);
        let cache_db = LayeredDatabase::open(cache_db, true);
        Self {
            inner: Arc::new(Inner {
                epoch_db,
                batch_db,
                consensus_chain_db,
                epoch_chain_db,
                kad_db,
                cache_db,
            }),
        }
    }

    fn get_db(&self, hint: TableHint) -> &LayeredDatabase<DB> {
        match hint {
            TableHint::Epoch => &self.inner.epoch_db,
            TableHint::EpochChain => &self.inner.epoch_chain_db,
            TableHint::Batch => &self.inner.batch_db,
            TableHint::ConsensusChain => &self.inner.consensus_chain_db,
            TableHint::Kad => &self.inner.kad_db,
            TableHint::Cache => &self.inner.cache_db,
        }
    }
}

impl<DB: Database> Database for CompositeDatabase<DB> {
    type TX<'txn>
        = CompositeDbTx<DB>
    where
        Self: 'txn;

    type TXMut<'txn>
        = CompositeDbTxMut<DB>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.get_db(T::HINT).open_table::<T>()
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(CompositeDbTx { inner: self.inner.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(CompositeDbTxMut {
            inner: self.inner.clone(),
            epoch_tx: None,
            batch_tx: None,
            consensus_chain_tx: None,
            epoch_chain_tx: None,
            kad_tx: None,
            cache_tx: None,
        })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.get_db(T::HINT).contains_key::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.get_db(T::HINT).get::<T>(key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.get_db(T::HINT).insert::<T>(key, value)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.get_db(T::HINT).remove::<T>(key)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.get_db(T::HINT).clear_table::<T>()
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.get_db(T::HINT).is_empty::<T>()
    }

    fn iter<T: Table>(&self) -> tn_types::DBIter<'_, T> {
        self.get_db(T::HINT).iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<tn_types::DBIter<'_, T>> {
        self.get_db(T::HINT).skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> tn_types::DBIter<'_, T> {
        self.get_db(T::HINT).reverse_iter::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.get_db(T::HINT).record_prior_to::<T>(key)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.get_db(T::HINT).last_record::<T>()
    }

    fn persist<T: Table>(&self) -> impl Future<Output = ()> + Send {
        self.get_db(T::HINT).persist::<T>()
    }

    fn sync_persist(&self) {
        self.inner.epoch_db.sync_persist();
        self.inner.batch_db.sync_persist();
        self.inner.consensus_chain_db.sync_persist();
        self.inner.epoch_chain_db.sync_persist();
        self.inner.kad_db.sync_persist();
        self.inner.cache_db.sync_persist();
    }
}

/// Read transaction for a composite DB.
#[derive(Clone)]
pub struct CompositeDbTx<DB: Database> {
    inner: Arc<Inner<DB>>,
}

impl<DB: Database> std::fmt::Debug for CompositeDbTx<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompositeDbTx")
    }
}

impl<DB: Database> DbTx for CompositeDbTx<DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        match T::HINT {
            TableHint::Epoch => self.inner.epoch_db.get::<T>(key),
            TableHint::EpochChain => self.inner.epoch_chain_db.get::<T>(key),
            TableHint::Batch => self.inner.batch_db.get::<T>(key),
            TableHint::ConsensusChain => self.inner.consensus_chain_db.get::<T>(key),
            TableHint::Kad => self.inner.kad_db.get::<T>(key),
            TableHint::Cache => self.inner.cache_db.get::<T>(key),
        }
    }
}

/// Mutable transaction for a composite DB.
///
/// This contains the underlying DB as well as optimistically
/// created mutable transactions for each underlying DB.
#[derive(Clone)]
pub struct CompositeDbTxMut<DB: Database> {
    inner: Arc<Inner<DB>>,
    epoch_tx: Option<LayeredDbTxMut<DB>>,
    batch_tx: Option<LayeredDbTxMut<DB>>,
    consensus_chain_tx: Option<LayeredDbTxMut<DB>>,
    epoch_chain_tx: Option<LayeredDbTxMut<DB>>,
    kad_tx: Option<LayeredDbTxMut<DB>>,
    cache_tx: Option<LayeredDbTxMut<DB>>,
}

impl<DB: Database> CompositeDbTxMut<DB> {
    fn get_tx(&mut self, hint: TableHint) -> eyre::Result<&mut LayeredDbTxMut<DB>> {
        match hint {
            TableHint::Epoch => {
                if self.epoch_tx.is_none() {
                    let epoch_tx = self.inner.epoch_db.write_txn()?;
                    self.epoch_tx = Some(epoch_tx);
                }
                Ok(self.epoch_tx.as_mut().expect("epoch transaction"))
            }
            TableHint::EpochChain => {
                if self.epoch_chain_tx.is_none() {
                    let tx = self.inner.epoch_chain_db.write_txn()?;
                    self.epoch_chain_tx = Some(tx);
                }
                Ok(self.epoch_chain_tx.as_mut().expect("epoch chain transaction"))
            }
            TableHint::ConsensusChain => {
                if self.consensus_chain_tx.is_none() {
                    let tx = self.inner.consensus_chain_db.write_txn()?;
                    self.consensus_chain_tx = Some(tx);
                }
                Ok(self.consensus_chain_tx.as_mut().expect("consensus transaction"))
            }
            TableHint::Batch => {
                if self.batch_tx.is_none() {
                    let tx = self.inner.batch_db.write_txn()?;
                    self.batch_tx = Some(tx);
                }
                Ok(self.batch_tx.as_mut().expect("batch transaction"))
            }
            TableHint::Kad => {
                if self.kad_tx.is_none() {
                    let tx = self.inner.kad_db.write_txn()?;
                    self.kad_tx = Some(tx);
                }
                Ok(self.kad_tx.as_mut().expect("kad transaction"))
            }
            TableHint::Cache => {
                if self.cache_tx.is_none() {
                    let tx = self.inner.cache_db.write_txn()?;
                    self.cache_tx = Some(tx);
                }
                Ok(self.cache_tx.as_mut().expect("cache transaction"))
            }
        }
    }
}

impl<DB: Database> std::fmt::Debug for CompositeDbTxMut<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompositeDbTxMut")
    }
}

impl<DB: Database> DbTx for CompositeDbTxMut<DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // We don't have a mutable self so can not call get_tx().
        // Use the tx if we have it else just use DB get().
        match T::HINT {
            TableHint::Epoch => {
                if let Some(tx) = self.epoch_tx.as_ref() {
                    tx.get::<T>(key)
                } else {
                    self.inner.epoch_db.get::<T>(key)
                }
            }
            TableHint::EpochChain => {
                if let Some(tx) = self.epoch_chain_tx.as_ref() {
                    tx.get::<T>(key)
                } else {
                    self.inner.epoch_chain_db.get::<T>(key)
                }
            }
            TableHint::Batch => {
                if let Some(tx) = self.batch_tx.as_ref() {
                    tx.get::<T>(key)
                } else {
                    self.inner.batch_db.get::<T>(key)
                }
            }
            TableHint::ConsensusChain => {
                if let Some(tx) = self.consensus_chain_tx.as_ref() {
                    tx.get::<T>(key)
                } else {
                    self.inner.consensus_chain_db.get::<T>(key)
                }
            }
            TableHint::Kad => {
                if let Some(tx) = self.kad_tx.as_ref() {
                    tx.get::<T>(key)
                } else {
                    self.inner.kad_db.get::<T>(key)
                }
            }
            TableHint::Cache => {
                if let Some(tx) = self.cache_tx.as_ref() {
                    tx.get::<T>(key)
                } else {
                    self.inner.cache_db.get::<T>(key)
                }
            }
        }
    }
}
impl<DB: Database> DbTxMut for CompositeDbTxMut<DB> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.get_tx(T::HINT)?.insert::<T>(key, value)
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.get_tx(T::HINT)?.remove::<T>(key)
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.get_tx(T::HINT)?.clear_table::<T>()
    }

    fn commit(self) -> eyre::Result<()> {
        if let Some(tx) = self.epoch_tx {
            tx.commit()?;
        }
        if let Some(tx) = self.batch_tx {
            tx.commit()?;
        }
        if let Some(tx) = self.consensus_chain_tx {
            tx.commit()?;
        }
        if let Some(tx) = self.epoch_chain_tx {
            tx.commit()?;
        }
        if let Some(tx) = self.kad_tx {
            tx.commit()?;
        }
        if let Some(tx) = self.cache_tx {
            tx.commit()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "redb")]
    use crate::redb::ReDB;
    use crate::{
        composite_db::CompositeDatabase,
        mdbx::{database::MEGABYTE, MdbxDatabase},
        test::*,
    };
    use std::path::Path;
    use tempfile::tempdir;
    use tn_types::Database as _;

    #[cfg(feature = "redb")]
    fn open_redb(path: &Path) -> CompositeDatabase<ReDB> {
        let db = ReDB::open(path.join("redb")).expect("Cannot open database");

        let db =
            CompositeDatabase::open(db.clone(), db.clone(), db.clone(), db.clone(), db.clone(), db);
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    fn open_mdbx(path: &Path) -> CompositeDatabase<MdbxDatabase> {
        let db =
            MdbxDatabase::open(path, 4, 16 * MEGABYTE, 8 * MEGABYTE).expect("Cannot open database");

        let db =
            CompositeDatabase::open(db.clone(), db.clone(), db.clone(), db.clone(), db.clone(), db);
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    #[test]
    fn test_compositedb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_contains_key(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_contains_key_1"));
        test_contains_key(db);
    }

    #[test]
    fn test_compositedb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_get(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_get_1"));
        test_get(db);
    }

    #[test]
    fn test_compositedb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_get(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_multi_get_1"));
        test_multi_get(db);
    }

    #[test]
    fn test_compositedb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_skip(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_skip_1"));
        test_skip(db);
    }

    #[test]
    fn test_compositedb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_skip_to_previous_simple(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_skip_to_previous_simple_1"));
        test_skip_to_previous_simple(db);
    }

    #[test]
    fn test_compositedb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter_skip_to_previous_gap(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_iter_skip_to_previous_gap_1"));
        test_iter_skip_to_previous_gap(db);
    }

    #[test]
    fn test_compositedb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_remove(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_remove_1"));
        test_remove(db);
    }

    #[test]
    fn test_compositedb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_iter_1"));
        test_iter(db);
    }

    #[test]
    fn test_compositedb_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter_reverse(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_iter_reverse_1"));
        test_iter_reverse(db);
    }

    #[test]
    fn test_compositedb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_clear(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_clear_1"));
        test_clear(db);
    }

    #[test]
    fn test_compositedb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_is_empty(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_is_empty_1"));
        test_is_empty(db);
    }

    #[test]
    fn test_compositedb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_insert(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_multi_insert_1"));
        test_multi_insert(db);
    }

    #[test]
    fn test_compositedb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_remove(db);
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_multi_remove_1"));
        test_multi_remove(db);
    }

    #[test]
    fn test_compositedb_dbsimpbench() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            db_simp_bench(db, "LayeredDB<ReDB>");
        }
        let db = open_mdbx(&temp_dir.path().join("mdbx_dbsimpbench_1"));
        db_simp_bench(db, "LayeredDB<MdbxDatabase>");
    }
}
