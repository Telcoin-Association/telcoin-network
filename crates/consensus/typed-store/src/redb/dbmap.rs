// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use ouroboros::self_referencing;
use redb::{
    Database, ReadOnlyTable, ReadTransaction, ReadableTable, ReadableTableMetadata,
    TableDefinition, WriteTransaction,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::DBMap;

use super::wraps::{KeyWrap, ValWrap};

#[macro_export]
macro_rules! reopen_redb {
    ( $db:expr, $($cf:expr;<$K:ty, $V:ty>),*) => {
        (
            $(
                Arc::new($crate::redb::dbmap::ReDBMap::<$K, $V>::reopen($db.clone(), $crate::redb::TableDefinition::<$crate::redb::wraps::KeyWrap<$K>, $crate::redb::wraps::ValWrap<$V>>::new($cf)).expect("can not open database"))
            ),*
        )
    };
}

/// An interface to a btree map database. This is mainly intended
/// for tests and performing benchmark comparisons or anywhere where an ephemeral database is
/// useful.
pub struct ReDBMap<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    db: Arc<RwLock<Database>>,
    table_def: TableDefinition<'a, KeyWrap<K>, ValWrap<V>>,
}

pub fn open_redb<P: AsRef<Path>>(path: P) -> eyre::Result<Arc<RwLock<Database>>> {
    Ok(Arc::new(RwLock::new(Database::create(path.as_ref().join("redb"))?)))
}

impl<'a, K, V> ReDBMap<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug,
{
    pub fn reopen(
        db: Arc<RwLock<Database>>,
        table_def: TableDefinition<'a, KeyWrap<K>, ValWrap<V>>,
    ) -> eyre::Result<Self> {
        let txn = db.read().expect("poisoned lock").begin_write()?;
        txn.open_table(table_def)?;
        txn.commit()?;
        Ok(Self { db, table_def })
    }

    fn read_txn(&self) -> eyre::Result<ReadTransaction> {
        Ok(self.db.read().expect("Poisoned lock!").begin_read()?)
    }

    fn read_table(&self) -> eyre::Result<ReadOnlyTable<KeyWrap<K>, ValWrap<V>>> {
        Ok(self.read_txn()?.open_table(self.table_def)?)
    }

    fn write_txn(&self) -> eyre::Result<WriteTransaction> {
        Ok(self.db.write().expect("Poisoned lock!").begin_write()?)
    }
}

impl<'a, K, V> DBMap<K, V> for ReDBMap<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug,
{
    fn contains_key(&self, key: &K) -> eyre::Result<bool> {
        Ok(self.read_table()?.get(key)?.map(|_| true).unwrap_or_default())
    }

    fn get(&self, key: &K) -> eyre::Result<Option<V>> {
        Ok(self.read_table()?.get(key)?.map(|v| v.value().clone()))
    }

    fn insert(&self, key: &K, value: &V) -> eyre::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(self.table_def)?;
            table.insert(key, value)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn remove(&self, key: &K) -> eyre::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(self.table_def)?;
            table.remove(key)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn clear(&self) -> eyre::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(self.table_def)?;
            table.retain(|_, _| false)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.read_table().map(|t| t.is_empty().unwrap_or(true)).unwrap_or(true)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        let guard = self.db.read().expect("Poisoned lock!");
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, Database>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(self.table_def)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<K>, ValWrap<V>>| {
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

    fn skip_to(&self, key: &K) -> eyre::Result<Box<dyn Iterator<Item = (K, V)> + '_>> {
        let guard = self.db.read().expect("Poisoned lock!");
        let key = key.clone();
        Ok(Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, Database>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(self.table_def)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<K>, ValWrap<V>>| {
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

    fn reverse_iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        let guard = self.db.read().expect("Poisoned lock!");
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, Database>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(self.table_def)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<K>, ValWrap<V>>| {
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

    fn record_prior_to(&self, key: &K) -> Option<(K, V)> {
        let mut last = None;
        for (k, v) in self.read_table().ok()?.iter().ok()?.flatten() {
            let (k, v) = (k.value().clone(), v.value().clone());
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last.map(|(k, v)| (k.clone(), v.clone()))
    }

    fn last_record(&self) -> Option<(K, V)> {
        self.read_table()
            .map(|t| t.last().ok().flatten().map(|(k, v)| (k.value().clone(), v.value().clone())))
            .ok()
            .flatten()
    }

    fn commit(&self) -> eyre::Result<()> {
        Ok(())
    }
}

#[self_referencing(pub_extras)]
pub struct ReDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    guard: RwLockReadGuard<'a, Database>,
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
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        traits::{multi_get, multi_insert, multi_remove},
        DBMap,
    };

    use super::open_redb;

    fn open_db(path: &Path) -> Arc<dyn DBMap<u64, String>> {
        let redb = open_redb(path).expect("Cannot open database");

        reopen_redb!(redb,
            "test_table";<u64, String>
        )
    }

    #[test]
    fn test_redb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key(&000000000).expect("Failed to call contains key"));
    }

    #[test]
    fn test_redb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(Some("123456789".to_string()), db.get(&123456789).expect("Failed to get"));
        assert_eq!(None, db.get(&000000000).expect("Failed to get"));
    }

    #[test]
    fn test_redb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123, &"123".to_string()).expect("Failed to insert");
        db.insert(&456, &"456".to_string()).expect("Failed to insert");

        let result = multi_get(&*db, [123, 456, 789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_redb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123, &"123".to_string()).expect("Failed to insert");
        db.insert(&456, &"456".to_string()).expect("Failed to insert");
        db.insert(&789, &"789".to_string()).expect("Failed to insert");

        // Skip all smaller
        let key_vals: Vec<_> = db.skip_to(&456).expect("Seek failed").collect();
        assert_eq!(key_vals.len(), 2);
        assert_eq!(key_vals[0], (456, "456".to_string()));
        assert_eq!(key_vals[1], (789, "789".to_string()));

        // Skip to the end
        assert_eq!(db.skip_to(&999).expect("Seek failed").count(), 0);

        // Skip to last
        assert_eq!(db.last_record(), Some((789, "789".to_string())));

        // Skip to successor of first value
        assert_eq!(db.skip_to(&000).expect("Skip failed").count(), 3);
    }

    #[test]
    fn test_redb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123, &"123".to_string()).expect("Failed to insert");
        db.insert(&456, &"456".to_string()).expect("Failed to insert");
        db.insert(&789, &"789".to_string()).expect("Failed to insert");

        // Skip to the one before the end
        let key_val = db.record_prior_to(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to(&000).is_none());
    }

    #[test]
    fn test_redb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        for i in 1..100 {
            if i != 50 {
                db.insert(&i, &i.to_string()).unwrap();
            }
        }

        // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
        // is not in the table
        let val = db.record_prior_to(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    #[test]
    fn test_redb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get(&123456789).expect("Failed to get").is_some());

        db.remove(&123456789).expect("Failed to remove");
        assert!(db.get(&123456789).expect("Failed to get").is_none());
    }

    #[test]
    fn test_redb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_redb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Test clear of empty map
        let _ = db.clear();

        let keys_vals = (0..101).map(|i| (i, i.to_string()));
        multi_insert(&*db, keys_vals).expect("Failed to batch insert");

        // Check we have multiple entries
        assert!(db.iter().count() > 1);
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
        // Clear with one item
        let _ = db.insert(&1, &"e".to_string());
        assert_eq!(db.iter().count(), 1);
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
    }

    #[test]
    fn test_redb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Test empty map is truly empty
        assert!(db.is_empty());
        let _ = db.clear();
        assert!(db.is_empty());

        let keys_vals = (0..101).map(|i| (i, i.to_string()));
        multi_insert(&*db, keys_vals).expect("Failed to batch insert");

        // Check we have multiple entries and not empty
        assert!(db.iter().count() > 1);
        assert!(!db.is_empty());

        // Clear again to ensure empty works after clearing
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
        assert!(db.is_empty());
    }

    #[test]
    fn test_redb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Create kv pairs
        let keys_vals = (0..101).map(|i| (i, i.to_string()));

        multi_insert(&*db, keys_vals.clone()).expect("Failed to multi-insert");

        for (k, v) in keys_vals {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_redb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Create kv pairs
        let keys_vals = (0..101).map(|i| (i, i.to_string()));

        multi_insert(&*db, keys_vals.clone()).expect("Failed to multi-insert");

        // Check insertion
        for (k, v) in keys_vals.clone() {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        multi_remove(&*db, keys_vals.clone().map(|kv| kv.0).take(50))
            .expect("Failed to multi-remove");
        assert_eq!(db.iter().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in keys_vals.skip(50) {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
