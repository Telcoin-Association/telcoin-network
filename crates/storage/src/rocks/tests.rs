// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//

// TODO: uint crate needs to be refactored for this nightly clippy
#![allow(clippy::assign_op_pattern)]

use super::*;
use crate::test::*;
use database::RocksDatabase;
use tempfile::TempDir;

uint::construct_uint! {
    // 32 byte number
    struct Num32(4);
}

#[test]
fn test_rocksdb_helpers() {
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

fn open_db(path: &Path) -> RocksDatabase {
    RocksDatabase::open_db_with_table::<TestTable, &Path>(path).expect("Cannot open database")
}

#[test]
fn test_rocksdb_open() {}

#[test]
fn test_rocksdb_contains_key() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_contains_key(db)
}

#[test]
fn test_rocksdb_get() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_get(db)
}

#[test]
fn test_rocksdb_multi_get() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_multi_get(db)
}

#[test]
fn test_rocksdb_skip() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_skip(db)
}

#[test]
fn test_rocksdb_skip_to_previous_simple() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_skip_to_previous_simple(db)
}

#[test]
fn test_rocksdb_iter_skip_to_previous_gap() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_iter_skip_to_previous_gap(db)
}

#[test]
fn test_rocksdb_remove() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_remove(db)
}

#[test]
fn test_rocksdb_iter() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_iter(db)
}

#[test]
fn test_rocksdb_iter_reverse() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_iter_reverse(db)
}

#[test]
fn test_rocksdb_clear() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_clear(db)
}

#[test]
fn test_rocksdb_is_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_is_empty(db)
}

#[test]
fn test_rocksdb_multi_insert() {
    // Init a DB
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_multi_insert(db)
}

#[test]
fn test_rocksdb_multi_remove() {
    // Init a DB
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    test_multi_remove(db)
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

#[test]
fn refcount_test() {
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

#[test]
fn refcount_with_compaction_test() {
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

#[test]
fn test_rocksdb_dbsimpbench() {
    // Init a DB
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());
    db_simp_bench(db, "RocksDB");
}
