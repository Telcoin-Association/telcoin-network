// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use ouroboros::self_referencing;
use redb::{
    Database as ReDatabase, ReadOnlyTable, ReadTransaction, ReadableTable, ReadableTableMetadata,
    TableDefinition, WriteTransaction,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::traits::{DBIter, Database, DbTx, DbTxMut, Table};

use super::wraps::{KeyWrap, ValWrap};

pub fn open_redatabase<P: AsRef<Path>>(path: P) -> eyre::Result<ReDB> {
    Ok(ReDB { db: Arc::new(RwLock::new(ReDatabase::create(path.as_ref().join("redb"))?)) })
}

#[derive(Debug)]
pub struct ReDbTx {
    tx: ReadTransaction,
}

impl DbTx for ReDbTx {
    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        Ok(self.tx.open_table(td)?.get(key)?.map(|v| v.value().clone()))
    }
}

pub struct ReDbTxMut {
    tx: WriteTransaction,
}

impl Debug for ReDbTxMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReDbTxMut")
    }
}

impl DbTx for ReDbTxMut {
    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        Ok(self.tx.open_table(td)?.get(key)?.map(|v| v.value().clone()))
    }
}

impl DbTxMut for ReDbTxMut {
    fn insert<T: crate::traits::Table>(
        &mut self,
        key: &T::Key,
        value: &T::Value,
    ) -> eyre::Result<()> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx.open_table(td)?.insert(key, value)?;
        Ok(())
    }

    fn remove<T: crate::traits::Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx.open_table(td)?.remove(key)?;
        Ok(())
    }

    fn clear_table<T: crate::traits::Table>(&mut self) -> eyre::Result<()> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx.open_table(td)?.retain(|_, _| false)?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        self.tx.commit()?;
        Ok(())
    }
}

/// An interface to a btree map database. This is mainly intended
/// for tests and performing benchmark comparisons or anywhere where an ephemeral database is
/// useful.
#[derive(Clone)]
pub struct ReDB {
    db: Arc<RwLock<ReDatabase>>,
}

impl ReDB {
    pub fn open_table<T: Table>(&self) -> eyre::Result<()> {
        let txn = self.db.read().expect("poisoned lock").begin_write()?;
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        txn.open_table(td)?;
        txn.commit()?;
        Ok(())
    }
}

impl Database for ReDB {
    type TX<'txn> = ReDbTx;
    type TXMut<'txn> = ReDbTxMut;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        let tx = self.db.read().expect("Poisoned lock!").begin_read()?;
        Ok(ReDbTx { tx })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        let tx = self.db.read().expect("Poisoned lock!").begin_write()?;
        Ok(ReDbTxMut { tx })
    }

    fn contains_key<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.read_txn()?.contains_key::<T>(key)
    }

    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.read_txn()?.get::<T>(key)
    }

    fn insert<T: crate::traits::Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let mut tx = self.write_txn()?;
        tx.insert::<T>(key, value)?;
        tx.commit()
    }

    fn remove<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<()> {
        let mut tx = self.write_txn()?;
        tx.remove::<T>(key)?;
        tx.commit()
    }

    fn clear_table<T: crate::traits::Table>(&self) -> eyre::Result<()> {
        let mut tx = self.write_txn()?;
        tx.clear_table::<T>()?;
        tx.commit()
    }

    fn is_empty<T: crate::traits::Table>(&self) -> bool {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        if let Ok(txn) = self.read_txn() {
            if let Ok(table) = txn.tx.open_table(td) {
                return table.is_empty().unwrap_or_default();
            }
        }
        false
    }

    fn iter<T: crate::traits::Table>(&self) -> DBIter<'_, T> {
        let guard = self.db.read().expect("Poisoned lock!");
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, ReDatabase>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(td)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<T::Key>, ValWrap<T::Value>>| {
                    Box::new(
                        table.iter().expect("Unable to get a DB iter").filter(|r| r.is_ok()).map(
                            |r| {
                                let (k, v) = r.unwrap();
                                (k.value().clone(), v.value().clone())
                            },
                        ),
                    )
                },
            }
            .build(),
        )
    }

    fn skip_to<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let guard = self.db.read().expect("Poisoned lock!");
        let key = key.clone();
        Ok(Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, ReDatabase>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(td)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<T::Key>, ValWrap<T::Value>>| {
                    Box::new(
                        table
                            .iter()
                            .expect("Unable to get a DB iter")
                            .filter(|r| r.is_ok())
                            .map(|r| {
                                let (k, v) = r.unwrap();
                                (k.value().clone(), v.value().clone())
                            })
                            .skip_while(move |(k, _)| k < &key),
                    )
                },
            }
            .build(),
        ))
    }

    fn reverse_iter<T: crate::traits::Table>(&self) -> DBIter<'_, T> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let guard = self.db.read().expect("Poisoned lock!");
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, ReDatabase>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(td)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<T::Key>, ValWrap<T::Value>>| {
                    Box::new(
                        table
                            .iter()
                            .expect("Unable to get a DB iter")
                            .rev()
                            .filter(|r| r.is_ok())
                            .map(|r| {
                                let (k, v) = r.unwrap();
                                (k.value().clone(), v.value().clone())
                            }),
                    )
                },
            }
            .build(),
        )
    }

    fn record_prior_to<T: crate::traits::Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let read_table =
            self.db.read().expect("Poisoned lock!").begin_read().ok()?.open_table(td).ok()?;
        let mut last = None;
        for (k, v) in read_table.iter().ok()?.flatten() {
            let (k, v) = (k.value().clone(), v.value().clone());
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last.map(|(k, v)| (k.clone(), v.clone()))
    }

    fn last_record<T: crate::traits::Table>(&self) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let read_table =
            self.db.read().expect("Poisoned lock!").begin_read().ok()?.open_table(td).ok()?;
        read_table.last().ok().flatten().map(|(k, v)| (k.value().clone(), v.value().clone()))
        //.map(|t| t.last().ok().flatten().map(|(k, v)| (k.value().clone(), v.value().clone())))
        //.ok()
        //.flatten()
    }
}

#[self_referencing(pub_extras)]
pub struct ReDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    guard: RwLockReadGuard<'a, ReDatabase>,
    #[borrows(mut guard)]
    table: ReadOnlyTable<KeyWrap<K>, ValWrap<V>>,
    #[borrows(table)]
    #[covariant]
    iter: Box<dyn Iterator<Item = (K, V)> + 'this>,
}

impl<'a, K, V> Iterator for ReDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| fields.iter.next())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use tempfile::tempdir;

    use crate::{
        redb::database::open_redatabase,
        traits::{Database, DbTxMut, Table},
    };

    use super::ReDB;

    #[derive(Debug)]
    struct TestTable {}
    impl Table for TestTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "TestTable";
    }

    fn open_db(path: &Path) -> ReDB {
        let db = open_redatabase(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    #[test]
    fn test_redb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    #[test]
    fn test_redb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    #[test]
    fn test_redb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result =
            db.multi_get::<TestTable>([123, 456, 789].iter()).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_redb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");

        // Skip all smaller
        let key_vals: Vec<_> = db.skip_to::<TestTable>(&456).expect("Seek failed").collect();
        assert_eq!(key_vals.len(), 2);
        assert_eq!(key_vals[0], (456, "456".to_string()));
        assert_eq!(key_vals[1], (789, "789".to_string()));

        // Skip to the end
        assert_eq!(db.skip_to::<TestTable>(&999).expect("Seek failed").count(), 0);

        // Skip to last
        assert_eq!(db.last_record::<TestTable>(), Some((789, "789".to_string())));

        // Skip to successor of first value
        assert_eq!(db.skip_to::<TestTable>(&000).expect("Skip failed").count(), 3);
    }

    #[test]
    fn test_redb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        txn.commit().unwrap();

        // Skip to the one before the end
        let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to::<TestTable>(&000).is_none());
    }

    #[test]
    fn test_redb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        for i in 1..100 {
            if i != 50 {
                txn.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        txn.commit().unwrap();

        // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
        // is not in the table
        let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    #[test]
    fn test_redb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    #[test]
    fn test_redb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_redb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Test clear of empty map
        let _ = db.clear_table::<TestTable>();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries
        assert!(db.iter::<TestTable>().count() > 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear with one item
        let _ = db.insert::<TestTable>(&1, &"e".to_string());
        assert_eq!(db.iter::<TestTable>().count(), 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
    }

    #[test]
    fn test_redb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Test empty map is truly empty
        assert!(db.is_empty::<TestTable>());
        let _ = db.clear_table::<TestTable>();
        assert!(db.is_empty::<TestTable>());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries and not empty
        assert!(db.iter::<TestTable>().count() > 1);
        assert!(!db.is_empty::<TestTable>());

        // Clear again to ensure empty works after clearing
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        assert!(db.is_empty::<TestTable>());
    }

    #[test]
    fn test_redb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_redb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Create kv pairs
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check insertion
        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        let mut txn = db.write_txn().unwrap();
        for (key, _val) in (0..101).map(|i| (i, i.to_string())).take(50) {
            txn.remove::<TestTable>(&key).expect("Failed to batch remove");
        }
        txn.commit().unwrap();
        assert_eq!(db.iter::<TestTable>().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in (0..101).map(|i| (i, i.to_string())).skip(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
