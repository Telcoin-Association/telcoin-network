// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//

// TODO: uint crate needs to be refactored for this nightly clippy
#![allow(clippy::assign_op_pattern)]

use super::*;
use crate::traits::{Database, DbTxMut, Table};
use database::RocksDatabase;
use tempfile::TempDir;

uint::construct_uint! {
    // 32 byte number
    struct Num32(4);
}

#[tokio::test]
async fn test_rocksdb_helpers() {
    let v = vec![];
    assert!(is_max(&v));

    fn check_add(v: Vec<u8>) {
        let mut v = v;
        let num = Num32::from_big_endian(&v);
        big_endian_saturating_add_one(&mut v);
        assert!(num + 1 == Num32::from_big_endian(&v));
    }

    let mut v = vec![255; 32];
    big_endian_saturating_add_one(&mut v);
    assert!(Num32::MAX == Num32::from_big_endian(&v));

    check_add(vec![1; 32]);
    check_add(vec![6; 32]);
    check_add(vec![254; 32]);

    // TBD: More tests coming with randomized arrays
}

#[derive(Debug)]
struct TestTable {}
impl Table for TestTable {
    type Key = u64;
    type Value = String;

    const NAME: &'static str = "TestTable";
}

fn open_db(path: &Path) -> RocksDatabase {
    RocksDatabase::open_db_with_table::<TestTable, &Path>(path).expect("Cannot open database")
}

#[tokio::test]
async fn test_rocksdb_open() {}

#[tokio::test]
async fn test_rocksdb_contains_key() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
    assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
    assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
}

#[tokio::test]
async fn test_rocksdb_get() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
    assert_eq!(
        Some("123456789".to_string()),
        db.get::<TestTable>(&123456789).expect("Failed to get")
    );
    assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
}

#[tokio::test]
async fn test_rocksdb_multi_get() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
    db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

    let result = db.multi_get::<TestTable>([123, 456, 789].iter()).expect("Failed to multi get");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Some("123".to_string()));
    assert_eq!(result[1], Some("456".to_string()));
    assert_eq!(result[2], None);
}

#[tokio::test]
async fn test_rocksdb_skip() {
    let temp_dir = TempDir::new().unwrap();
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

#[tokio::test]
async fn test_rocksdb_skip_to_previous_simple() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
    db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
    db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");

    // Skip to the one before the end
    let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
    assert_eq!(key_val, (789, "789".to_string()));

    // Skip to prior of first value
    // Note: returns an empty iterator!
    assert!(db.record_prior_to::<TestTable>(&000).is_none());
}

#[tokio::test]
async fn test_rocksdb_iter_skip_to_previous_gap() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    for i in 1..100 {
        if i != 50 {
            db.insert::<TestTable>(&i, &i.to_string()).unwrap();
        }
    }

    // Skip prior to will return an iterator starting with an "unexpected" key if the sought one is
    // not in the table
    let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
    assert_eq!(49, val);
}

#[tokio::test]
async fn test_rocksdb_remove() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
    assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

    db.remove::<TestTable>(&123456789).expect("Failed to remove");
    assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
}

#[tokio::test]
async fn test_rocksdb_iter() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

    let mut iter = db.iter::<TestTable>();
    assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
    assert_eq!(None, iter.next());
}

#[tokio::test]
async fn test_rocksdb_delete_range() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    // Note that the last element is (100, "100".to_owned()) here
    let mut txn = db.write_txn().unwrap();
    for (key, val) in (0..101).map(|i| (i, i.to_string())) {
        txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
    }

    for key in 50..100 {
        txn.remove::<TestTable>(&key).expect("Failed to delete range");
    }

    txn.commit().expect("Failed to execute batch");

    for k in 0..50 {
        assert!(db.contains_key::<TestTable>(&k).expect("Failed to query legal key"),);
    }
    for k in 50..100 {
        assert!(!db.contains_key::<TestTable>(&k).expect("Failed to query legal key"));
    }

    // range operator is not inclusive of to
    assert!(db.contains_key::<TestTable>(&100).expect("Failed to query legal key"));
}

#[tokio::test]
async fn test_rocksdb_clear() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    // Test clear of empty map
    let _ = db.clear_table::<TestTable>();

    let keys_vals = (0..101).map(|i| (i, i.to_string()));
    let mut insert_batch = db.write_txn().unwrap();
    for (key, val) in keys_vals.clone() {
        insert_batch.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
    }

    insert_batch.commit().expect("Failed to execute batch");

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

#[tokio::test]
async fn test_rocksdb_is_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    // Test empty map is truly empty
    assert!(db.is_empty::<TestTable>());
    let _ = db.clear_table::<TestTable>();
    assert!(db.is_empty::<TestTable>());

    let keys_vals = (0..101).map(|i| (i, i.to_string()));
    let mut insert_batch = db.write_txn().unwrap();
    for (key, val) in keys_vals.clone() {
        insert_batch.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
    }

    insert_batch.commit().expect("Failed to execute batch");

    // Check we have multiple entries and not empty
    assert!(db.iter::<TestTable>().count() > 1);
    assert!(!db.is_empty::<TestTable>());

    // Clear again to ensure empty works after clearing
    let _ = db.clear_table::<TestTable>();
    assert_eq!(db.iter::<TestTable>().count(), 0);
    assert!(db.is_empty::<TestTable>());
}

#[tokio::test]
async fn test_rocksdb_multi_insert() {
    // Init a DB
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    // Create kv pairs
    let keys_vals = (0..101).map(|i| (i, i.to_string()));

    for (key, val) in keys_vals.clone() {
        db.insert::<TestTable>(&key, &val).expect("Failed to multi-insert");
    }

    for (k, v) in keys_vals {
        let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

/*
#[derive(Serialize, Deserialize, Copy, Clone)]
struct ObjectWithRefCount {
    value: i64,
    ref_count: i64,
}

fn increment_counter(db: &RocksDBMap<String, ObjectWithRefCount>, key: &str, value: i64) {
    let mut batch = db.batch();
    batch.partial_merge_batch(db, [(key.to_string(), value.to_le_bytes())]).unwrap();
    batch.write().unwrap();
}

#[tokio::test]
async fn refcount_test() {
    let key = "key".to_string();
    let mut options = rocksdb::Options::default();
    options.set_merge_operator(
        "refcount operator",
        reference_count_merge_operator,
        reference_count_merge_operator,
    );
    let db = RocksDBMap::<String, ObjectWithRefCount>::open(
        temp_dir(),
        MetricConf::default(),
        Some(options),
        None,
        &ReadWriteOptions::default(),
    )
    .expect("failed to open rocksdb");
    let object = ObjectWithRefCount { value: 3, ref_count: 1 };
    // increment value 10 times
    let iterations = 10;
    for _ in 0..iterations {
        let mut batch = db.batch();
        batch.merge_batch(&db, [(key.to_string(), object)]).unwrap();
        batch.write().unwrap();
    }
    let value = db.get(&key).expect("failed to read value").expect("value is empty");
    assert_eq!(value.value, object.value);
    assert_eq!(value.ref_count, iterations);

    // decrement value
    increment_counter(&db, &key, -1);
    let value = db.get(&key).unwrap().unwrap();
    assert_eq!(value.value, object.value);
    assert_eq!(value.ref_count, iterations - 1);
}

#[tokio::test]
async fn refcount_with_compaction_test() {
    let key = "key".to_string();
    let mut options = rocksdb::Options::default();
    options.set_merge_operator(
        "refcount operator",
        reference_count_merge_operator,
        reference_count_merge_operator,
    );
    let db = RocksDBMap::<String, ObjectWithRefCount>::open(
        temp_dir(),
        MetricConf::default(),
        Some(options),
        None,
        &ReadWriteOptions::default(),
    )
    .expect("failed to open rocksdb");

    let object = ObjectWithRefCount { value: 3, ref_count: 1 };
    let mut batch = db.batch();
    batch.merge_batch(&db, [(key.to_string(), object)]).unwrap();
    batch.write().unwrap();
    // increment value once
    increment_counter(&db, &key, 1);
    let value = db.get(&key).unwrap().unwrap();
    assert_eq!(value.value, object.value);

    // decrement value to 0
    increment_counter(&db, &key, -1);
    increment_counter(&db, &key, -1);
    // ref count went to zero. Reading value returns empty array
    assert!(db.get(&key).is_err());

    // refcount increment makes value visible again
    increment_counter(&db, &key, 1);
    let value = db.get(&key).unwrap().unwrap();
    assert_eq!(value.value, object.value);

    increment_counter(&db, &key, -1);
    db.compact_range(&object, &ObjectWithRefCount { value: 100, ref_count: 1 }).unwrap();

    increment_counter(&db, &key, 1);
    let value = db.get_raw_bytes(&key).unwrap().unwrap();
    assert!(is_ref_count_value(&value));
}
*/
