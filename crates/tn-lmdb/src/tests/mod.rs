//! Whole-engine tests through the public API and the disk model: the crash matrix of the commit
//! protocol, damaged files, a randomized multi-table model with crashes and power losses,
//! concurrent readers under a churning writer, and the read-only open.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::FileExt as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use tempfile::TempDir;

use crate::{
    catalog::TableRecord,
    env::{Env, EnvOptions, GapFill, SyncMode, MAX_GAP_FILL},
    meta::{Meta, Tag, MAX_WEAK_RUN, META_LEN},
    overflow::{run_pages, run_txn},
    page::{self, Val},
    tree::{
        test_harness::{walk_outer_tree, walk_packing_tree, walk_tree},
        TreeRoot,
    },
    CheckReport, ReadTxn, RoTxn, RwTxn, Table,
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
mod dup_key_tail;
mod dup_packed;
mod dup_sweep;
mod dup_txn_cursor;
mod key_tail;
mod merge;
mod multi_model;
mod nested_damage;
mod packed_damage;
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

/// The units a test runs a clean close's trim at: the host's OS page (`None`), and 4 KiB, the
/// page of the hosts the node runs on (x86_64 Linux), where the trim leaves a 4 KiB-page file no
/// page past the ones in use. On a host with 4 KiB pages the two are the same trim.
pub(crate) fn trim_units() -> [Option<u64>; 2] {
    [None, Some(4096)]
}

/// Close the cleanly closed file at `path` again with its trim rounded to `trim_unit`
/// (`Writer::trim_unit`; `None`: the OS page), so the file past its pages in use is what a close
/// on a host with that OS page leaves. A durable open changes nothing else.
pub(crate) fn retrim(path: &Path, trim_unit: Option<u64>) {
    let env = open(path, SyncMode::Durable);
    env.with_writer(|w| w.trim_unit = trim_unit);
    drop(env);
}

/// `Env::check`, which must pass, and whose page counts must add up to the page count.
fn check(env: &Env) -> CheckReport {
    let r = env.check().expect("check");
    let used = 2 + r.catalog_pages + r.table_pages + r.free_tree_pages + r.free_pages;
    assert_eq!(used, u64::from(r.page_count), "{r:?}");
    r
}

/// The intact record meta slot `slot` of the file (of `page_size`-byte pages) holds, if any: its
/// txn and whether it is weak.
fn slot_record(path: &Path, page_size: u32, slot: usize) -> Option<(u64, bool)> {
    let file = fs::File::open(path).expect("open");
    let mut record = [0; META_LEN];
    file.read_exact_at(&mut record, slot as u64 * u64::from(page_size)).ok()?;
    Meta::decode(&record).ok().map(|(meta, tag)| (meta.txn, tag.weak))
}

/// The txn a writer open gives its first transaction over slots holding `records` (each one's txn
/// and whether it is weak, as `slot_record` reads them, `None` for a slot with no intact record):
/// `MAX_WEAK_RUN` + 1 past the newest steady record's, and at least one past every record's
/// (`Chosen::next_txn`).
pub(crate) fn first_txn_over(records: [Option<(u64, bool)>; 2]) -> u64 {
    let newest = records.iter().flatten().map(|&(txn, _)| txn + 1).max().expect("a record");
    let steady = records.iter().flatten().filter(|&&(_, weak)| !weak).map(|&(txn, _)| txn).max();
    steady.map_or(newest, |txn| newest.max(after_steady(txn)))
}

/// The txn a writer open gives its first transaction over the file at `path` (of
/// `page_size`-byte pages): `first_txn_over` its slots' records, read before the open (which
/// may rewrite a slot).
pub(crate) fn first_txn(path: &Path, page_size: u32) -> u64 {
    first_txn_over([slot_record(path, page_size, 0), slot_record(path, page_size, 1)])
}

/// The txn a writer open gives its first transaction over a file whose newest steady record is
/// at `txn` (a new file's, at 0; the last durable commit's or sync's): `MAX_WEAK_RUN` + 1 past
/// it, past every weak record a writer leaves beside it (`Chosen::next_txn`).
pub(crate) const fn after_steady(txn: u64) -> u64 {
    txn + MAX_WEAK_RUN + 1
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
/// `Env::check` passes: a plain table's rows by its cursors ([`rows`]), a dup table's by a walk
/// of its trees that checks the totals (`dup_rows_for_test`), which its dup cursor must agree
/// with ([`dup_cursor_rows`]), with its figures in the check those of its `table_stat`.
fn newest_state(env: &Env) -> State {
    let report = check(env);
    let ro = Arc::new(env.begin_ro().expect("ro"));
    assert_eq!(ro.txn_id(), report.txn_id);
    let mut state = State::new();
    for name in report.tables.keys() {
        let table = ro.open_table(name).expect("open table").expect("the check names it");
        let rows = if let Some(dup) = ro.table_stat(&table).expect("stat").dup {
            dup::assert_dup_figures(&report, &*ro, name);
            let walked = ro.dup_rows_for_test(&table);
            dup_cursor_rows(&ro, &table, &walked, usize::from(dup.key_len));
            walked.into_iter().collect()
        } else {
            rows(&*ro, &table)
        };
        state.insert(name.clone(), rows);
    }
    state
}

/// Dup table `table`'s rows by its dup cursor (`DupRoCursor`: `first`, then `next` to the end,
/// which crosses outer keys) are `walked`'s, the walk of its trees, and `seek_dup` of every
/// seventh row's outer key (`key_len` bytes) and subkey lands on that row: the dup cursors read
/// the files the reopens recover.
fn dup_cursor_rows(ro: &Arc<RoTxn>, table: &Table, walked: &[(Vec<u8>, Vec<u8>)], key_len: usize) {
    let mut cursor = ro.dup_cursor_owned(table).expect("dup cursor");
    let mut scanned = Vec::new();
    let mut row = cursor.first().expect("first").map(|(k, v)| (k.to_vec(), v.to_vec()));
    while let Some(kv) = row {
        scanned.push(kv);
        row = cursor.next().expect("next").map(|(k, v)| (k.to_vec(), v.to_vec()));
    }
    assert!(
        scanned == walked,
        "table {}: the dup cursor's rows differ from the walk's",
        table.name()
    );
    for (k, v) in walked.iter().step_by(7) {
        let got = cursor.seek_dup(&k[..key_len], &k[key_len..]).expect("seek_dup");
        assert_eq!(got, Some((&k[..], &v[..])), "table {}: seek_dup", table.name());
    }
}

/// The pages the newest state of `env` names that were written after txn `since`: the catalog's,
/// every table's (a dup table's outer tree's and its nested trees'), the free tree's, each
/// tree's pages by the test harness's walk and the overflow runs its leaves name, kept when the
/// page's or the run's header txn is past `since`. With `since` the last sync's txn these are the
/// pages a trusted weak head needs on a reopen: what open's check of it reads
/// (`check::check_since`), and what a loss must keep for the head to be opened.
fn pages_since(env: &Env, since: u64) -> Vec<u32> {
    use crate::tree::PageRead as _;
    let snapshot = env.inner().current();
    let store = &*snapshot;
    let meta = snapshot.meta;
    let catalog = walk_tree(store, &meta.catalog);
    let mut walks = vec![walk_tree(store, &meta.free)];
    for (_, record) in &catalog.entries {
        let record = TableRecord::decode(record).expect("a table record");
        let Some(dup) = record.dup else {
            walks.push(walk_tree(store, &record.root));
            continue;
        };
        let outer = if dup.packed_max == 0 {
            walk_outer_tree(store, &record.root)
        } else {
            walk_packing_tree(store, &record.root)
        };
        for (_, cell) in outer.entries.iter().filter(|(key, _)| !outer.packed.contains(key)) {
            let cell = cell.as_slice().try_into().expect("a nested cell");
            walks.push(walk_tree(store, &TreeRoot::decode(cell, 0).expect("a nested root")));
        }
        walks.push(outer);
    }
    walks.push(catalog);
    let mut pages = Vec::new();
    for walk in &walks {
        let written_since = |&p: &u32| page::txn(store.page_any(p).expect("a page")) > since;
        pages.extend(walk.pages.iter().filter(|p| written_since(p)));
        for &leaf in &walk.leaves {
            let leaf = store.page_any(leaf).expect("a leaf");
            for i in 0..page::nkeys(leaf) {
                let Val::Overflow(ovf) = page::leaf_entry(leaf, i).expect("an entry").1 else {
                    continue;
                };
                if run_txn(store.page_any(ovf.pgno).expect("a run")) > since {
                    let n = run_pages(ovf.len as usize, snapshot.geometry.page_size()) as u32;
                    pages.extend(ovf.pgno..ovf.pgno + n);
                }
            }
        }
    }
    pages
}

/// Whether a volume loss kept every page the trusted weak head at txn `head` needs: each of
/// `pages` ([`pages_since`] at the head, before the loss) reads the same bytes in `before` (the
/// file's bytes at the head) and in `after` (the bytes the loss left), and `after` holds the
/// head's `page_count` pages (of `page_size` bytes; the head's record is read from `before`'s
/// slots). Open's check of the head reads exactly those pages, with the pages written by the last
/// sync, which a loss never changes, so it must pass and open must keep the head. The converse is
/// not asserted: a lost page may still pass (a sector torn inside a leaf's free gap, which its
/// CRC does not cover), so a fallen-back open after a lost page is left to the engine's check.
fn head_survived(before: &[u8], after: &[u8], page_size: u32, head: u64, pages: &[u32]) -> bool {
    let size = page_size as usize;
    let page_count = (0..2)
        .find_map(|slot| {
            let record = before[slot * size..][..META_LEN].try_into().expect("a record");
            Meta::decode(record).ok().map(|(meta, _)| meta).filter(|meta| meta.txn == head)
        })
        .expect("the head's record")
        .page_count;
    after.len() >= page_count as usize * size
        && pages.iter().all(|&p| {
            let at = p as usize * size..(p as usize + 1) * size;
            before[at.clone()] == after[at]
        })
}

/// Store `value` under `key` in table `name` (created if missing), in the transaction and in
/// the model.
fn put(txn: &mut RwTxn, state: &mut State, name: &str, key: &[u8], value: &[u8]) {
    let table = txn.create_table(name).expect("table");
    txn.put(&table, key, value).expect("put");
    state.entry(name.to_string()).or_default().insert(key.to_vec(), value.to_vec());
}

/// The table of the open's first commit in `SyncMode::NoSync` ([`durable_first_commit`]).
const OPENED: &str = "opened";

/// The first commit that changes something after an open, which commits durably in
/// `SyncMode::NoSync` too (`Writer::sync_due`): one row in table [`OPENED`], in the transaction
/// and in `state`, so the `NoSync` commits a test makes after it defer. Its txn: with `state`,
/// the last sync before them.
fn durable_first_commit(env: &Env, state: &mut State) -> u64 {
    let mut txn = env.begin_rw().expect("begin");
    put(&mut txn, state, OPENED, b"k", b"v");
    txn.commit().expect("the open's first commit")
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
