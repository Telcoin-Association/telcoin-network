// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, sync::Arc};

use bincode::Options;
use dashmap::DashMap;
use ouroboros::self_referencing;
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

use crate::traits::{DBIter, Database, DbTx, DbTxMut, Table};

type StoreType = DashMap<&'static str, Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>>;

fn get<T: crate::traits::Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode(key);
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
    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
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
            let key_bytes = encode(key);
            let value_bytes = encode(value);
            table.write().insert(key_bytes, value_bytes);
        }
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
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
    store: StoreType,
}

impl MemDatabase {
    pub fn new() -> Self {
        Self { store: DashMap::new() }
    }

    pub fn open_table<T: Table>(&self) {
        self.store.insert(T::NAME, Arc::new(RwLock::new(BTreeMap::new())));
    }
}

impl Default for MemDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl Database for MemDatabase {
    type TX<'txn> = MemDbTx
    where
        Self: 'txn;

    type TXMut<'txn> = MemDbTxMut
    where
        Self: 'txn;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(MemDbTx { store: self.store.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MemDbTxMut { store: self.store.clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            return Ok(table.read().contains_key(&key_bytes));
        }
        Ok(false)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            let value_bytes = encode(value);
            table.write().insert(key_bytes, value_bytes);
        }
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
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
                            let key_bytes = encode(key);
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

    fn reverse_iter<T: Table>(&self) -> crate::traits::DBIter<'_, T> {
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
            let key_bytes = encode(key);
            let mut last = None;
            let guard = table.read();
            for (k, v) in guard.iter() {
                if k >= &key_bytes {
                    break;
                }
                last = Some((k, v));
            }
            last.map(|(key_bytes, value_bytes)| {
                let key = decode(key_bytes);
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
                let key = decode(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        } else {
            None
        }
    }
}

fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(bytes)
        .expect("Invalid bytes!")
}

fn encode<T: Serialize>(obj: &T) -> Vec<u8> {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(obj)
        .expect("Can not serialize!")
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
                let key = bincode::DefaultOptions::new()
                    .with_big_endian()
                    .with_fixint_encoding()
                    .deserialize(key_bytes)
                    .expect("Invalid bytes!");
                let value = bincode::DefaultOptions::new()
                    .with_big_endian()
                    .with_fixint_encoding()
                    .deserialize(value_bytes)
                    .expect("Invalid bytes!");
                (key, value)
            })
        })
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
