//! Whole-engine tests through the public API and the disk model: the crash matrix of the commit
//! protocol, damaged files, a randomized multi-table model with crashes and power losses,
//! concurrent readers under a churning writer, and the read-only open.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::FileExt as _,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

use crate::{
    env::{Env, EnvOptions, SyncMode},
    meta::{Meta, META_LEN},
    CheckReport, ReadTxn, RwTxn, Table,
};

mod append;
mod concurrency;
mod crash;
mod crash_runs;
mod damage;
mod damage_merge;
mod damage_runs;
mod dup;
mod dup_cursor;
mod dup_txn_cursor;
mod key_tail;
mod merge;
mod multi_model;
mod nested_damage;
mod path_cache;
mod read_only;
mod txn_cursor;

/// Every table's rows by name: one committed state of a file.
type State = BTreeMap<String, Rows>;

/// One table's rows.
type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

fn temp(prefix: &str) -> (TempDir, PathBuf) {
    let dir = TempDir::with_prefix(prefix).expect("temp dir");
    let path = dir.path().join("db");
    (dir, path)
}

fn options(sync: SyncMode) -> EnvOptions {
    EnvOptions { sync, ..EnvOptions::default() }
}

fn open(path: &Path, sync: SyncMode) -> Env {
    Env::open(path, options(sync)).expect("open")
}

/// `Env::check`, which must pass, and whose page counts must add up to the page count.
fn check(env: &Env) -> CheckReport {
    let r = env.check().expect("check");
    let used = 2 + r.catalog_pages + r.table_pages + r.free_tree_pages + r.free_pages;
    assert_eq!(used, u64::from(r.page_count), "{r:?}");
    r
}

/// True if meta slot `slot` of the file (of `page_size`-byte pages) holds an intact record of
/// txn `txn`.
fn slot_holds(path: &Path, page_size: u32, slot: usize, txn: u64) -> bool {
    let file = fs::File::open(path).expect("open");
    let mut record = [0; META_LEN];
    file.read_exact_at(&mut record, slot as u64 * u64::from(page_size)).is_ok()
        && Meta::decode(&record).is_ok_and(|meta| meta.txn == txn)
}

/// A value of `len` bytes that differs by `tag` and `i`.
fn value(tag: u32, i: u32, len: usize) -> Vec<u8> {
    let seed = i.wrapping_mul(0x9E37_79B9) ^ tag.wrapping_mul(0x85EB_CA6B);
    (0..len).map(|j| (seed.wrapping_add(j as u32 * 31) >> 3) as u8).collect()
}

/// The rows of `table` as `txn` sees them: a forward cursor scan, which a backward scan and the
/// table's entry count must agree with.
fn rows(txn: &impl ReadTxn, table: &Table) -> Rows {
    let mut cursor = txn.cursor(table).expect("cursor");
    let mut forward = Vec::new();
    while let Some((k, v)) = cursor.next().expect("next") {
        forward.push((k.to_vec(), v.to_vec()));
    }
    let mut cursor = txn.cursor(table).expect("cursor");
    let mut backward = Vec::new();
    while let Some((k, v)) = cursor.prev().expect("prev") {
        backward.push((k.to_vec(), v.to_vec()));
    }
    backward.reverse();
    assert_eq!(forward, backward, "table {}: both scans agree", table.name());
    let entries = txn.table_stat(table).expect("stat").entries;
    assert_eq!(entries, forward.len() as u64, "table {}: the entry count", table.name());
    forward.into_iter().collect()
}

/// The state `txn` sees of the tables among `names` that exist in it.
fn state_of<'n>(txn: &impl ReadTxn, names: impl IntoIterator<Item = &'n str>) -> State {
    let mut state = State::new();
    for name in names {
        if let Some(table) = txn.open_table(name).expect("open table") {
            state.insert(name.to_string(), rows(txn, &table));
        }
    }
    state
}

/// The newest committed state of `env`, every table of it (the check names them), once
/// `Env::check` passes.
fn newest_state(env: &Env) -> State {
    let report = check(env);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(ro.txn_id(), report.txn_id);
    let state = state_of(&ro, report.tables.keys().map(String::as_str));
    assert_eq!(state.len(), report.tables.len());
    state
}

/// Store `value` under `key` in table `name` (created if missing), in the transaction and in
/// the model.
fn put(txn: &mut RwTxn, state: &mut State, name: &str, key: &[u8], value: &[u8]) {
    let table = txn.create_table(name).expect("table");
    txn.put(&table, key, value).expect("put");
    state.entry(name.to_string()).or_default().insert(key.to_vec(), value.to_vec());
}

/// Remove `key` from table `name` (which must exist), in the transaction and in the model.
fn delete(txn: &mut RwTxn, state: &mut State, name: &str, key: &[u8]) {
    let table = txn.open_table(name).expect("open").expect("the table exists");
    let rows = state.get_mut(name).expect("the model has the table");
    assert_eq!(txn.delete(&table, key).expect("delete"), rows.remove(key).is_some());
}

/// Drop table `name` (which must exist), in the transaction and in the model.
fn drop_table(txn: &mut RwTxn, state: &mut State, name: &str) {
    let table = txn.open_table(name).expect("open").expect("the table exists");
    txn.drop_table(table).expect("drop");
    state.remove(name).expect("the model has the table");
}
