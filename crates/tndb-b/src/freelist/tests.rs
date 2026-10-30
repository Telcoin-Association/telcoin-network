//! Free-list tests: the record codec, gated reuse (held readers, durability, `SyncMode::NoSync`),
//! bounded growth, reuse of dropped and cleared tables (overflow runs included), the save's fixed
//! point, exact page accounting (`Env::check`) in a randomized workload, damaged records, damaged
//! runs refused by clear, drop and `Env::check`, and power loss.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    os::unix::fs::FileExt as _,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

use super::*;
use crate::{
    catalog, disk_model,
    env::{Env, EnvOptions, SyncMode},
    meta::{Meta, PAGE_SIZES},
    tree::{
        test_harness::{ReportSeed, Rng},
        PageWrite,
    },
    CheckReport, Error, ReadTxn, RoTxn, RwTxn, Table, TableStat,
};

fn temp() -> (TempDir, PathBuf) {
    let dir = TempDir::with_prefix("tndb_b_free").expect("temp dir");
    let path = dir.path().join("db");
    (dir, path)
}

fn open(path: &Path, sync: SyncMode) -> Env {
    Env::open(path, EnvOptions { sync, ..EnvOptions::default() }).expect("open")
}

/// `Env::check`, which must pass, and whose page counts must add up to the page count.
fn check(env: &Env) -> CheckReport {
    let r = env.check().expect("check");
    let used = 2 + r.catalog_pages + r.table_pages + r.free_tree_pages + r.free_pages;
    assert_eq!(used, u64::from(r.page_count), "{r:?}");
    r
}

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A value of `len` bytes that differs by key and generation.
fn value(i: u32, generation: u32, len: usize) -> Vec<u8> {
    let seed = i.wrapping_mul(0x9E37_79B9) ^ generation.wrapping_mul(0x85EB_CA6B);
    (0..len).map(|j| (seed.wrapping_add(j as u32 * 31) >> 3) as u8).collect()
}

/// Overwrite keys `0..n` of table `name` with generation `generation` in one commit.
fn churn(env: &Env, name: &str, n: u32, generation: u32, len: usize) -> u64 {
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table(name).expect("table");
    for i in 0..n {
        txn.put(&t, &key(i), &value(i, generation, len)).expect("put");
    }
    txn.commit().expect("commit")
}

fn page_count(env: &Env) -> u32 {
    env.stat().page_count
}

/// `(txn, page)` events the writer logged.
type Log = Vec<(u64, u32)>;

/// The loads and frees the writer logged.
fn logs(env: &Env) -> (Log, Log) {
    env.with_writer(|w| (w.alloc.load_log.clone(), w.alloc.free_log.clone()))
}

/// For each page loaded from the free list by txn `T`, the txn `N < T` that last freed it, as
/// `(T, N, page)`.
fn reuses(env: &Env) -> Vec<(u64, u64, u32)> {
    let (loads, frees) = logs(env);
    let mut by_txn: BTreeMap<u64, (Vec<u32>, Vec<u32>)> = BTreeMap::new();
    for &(t, p) in &loads {
        by_txn.entry(t).or_default().0.push(p);
    }
    for &(t, p) in &frees {
        by_txn.entry(t).or_default().1.push(p);
    }
    let mut last_freed: HashMap<u32, u64> = HashMap::new();
    let mut out = Vec::new();
    for (t, (loaded, freed)) in by_txn {
        for p in loaded {
            let n = *last_freed.get(&p).unwrap_or_else(|| panic!("page {p} loaded, never freed"));
            out.push((t, n, p));
        }
        for p in freed {
            last_freed.insert(p, t);
        }
    }
    out
}

/// Keys order as their encodings, the successor steps over a full sequence, and record values
/// must be whole, ascending tree pages of the state.
#[test]
fn record_codec_round_trips_and_refuses_damage() {
    let keys = [(0, 0), (1, 5), (1, u32::MAX), (2, 0), (256, 1), (u64::MAX, u32::MAX)]
        .map(|(txn, seq)| RecordKey { txn, seq });
    for (a, b) in keys.iter().zip(&keys[1..]) {
        assert!(a < b && a.encode() < b.encode(), "{a:?} {b:?}");
        assert_eq!(RecordKey::decode(&a.encode()).expect("decode"), *a);
    }
    assert!(matches!(RecordKey::decode(&[0; 11]), Err(Error::Corrupt(_))));
    assert_eq!(keys[1].successor(), Some(RecordKey { txn: 1, seq: 6 }));
    assert_eq!(keys[2].successor(), Some(RecordKey { txn: 2, seq: 0 }));
    assert_eq!(keys[5].successor(), None);

    assert_eq!(decode_pages(&encode_pages(&[2, 3, 9]), 10).expect("decode"), [2, 3, 9]);
    assert!(decode_pages(&[], 2).expect("an empty record").is_empty());
    for (bad, count) in [
        (vec![2, 0, 0, 0, 3], 10),
        (encode_pages(&[1, 3]), 10),
        (encode_pages(&[3, 10]), 10),
        (encode_pages(&[3, 3]), 10),
        (encode_pages(&[4, 3]), 10),
    ] {
        assert!(matches!(decode_pages(&bad, count), Err(Error::Corrupt(_))), "{bad:?}");
    }
    for p in PAGE_SIZES {
        let geometry = Geometry::new(p).expect("geometry");
        let capacity = record_capacity(&geometry);
        geometry.check_entry(KEY_LEN, capacity * 4).expect("a full record fits");
        assert!(geometry.check_entry(KEY_LEN, capacity * 4 + 4).is_err(), "and is full");
    }
    assert_eq!(record_capacity(&Geometry::new(4096).expect("4K")), 248);
}

/// A reader holds its snapshot's pages across many commits that reuse pages around it: it reads
/// every value intact throughout, and the pages it reaches are reused only once it is gone.
#[test]
fn a_held_reader_keeps_its_pages() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    for generation in 0..6 {
        churn(&env, "t", 300, generation, 100);
    }
    let ro = env.begin_ro().expect("ro");
    let t = ro.open_table("t").expect("open").expect("t");
    let held = ro.txn_id();
    let checksum = |ro: &RoTxn| -> u64 {
        (0..300).fold(0, |sum, i| {
            let v = ro.get(&t, &key(i)).expect("get").expect("present");
            assert_eq!(v, value(i, 5, 100), "key {i}");
            v.iter().fold(sum, |s, &b| s.wrapping_mul(31).wrapping_add(u64::from(b)))
        })
    };
    let want = checksum(&ro);
    for generation in 6..40 {
        churn(&env, "t", 300, generation, 100);
        check(&env);
        assert_eq!(checksum(&ro), want, "after generation {generation}");
    }
    let during = reuses(&env);
    assert!(!during.is_empty(), "pages freed before the reader's snapshot are reused");
    assert!(during.iter().all(|&(_, n, _)| n <= held), "a page freed after it is not");
    drop(ro);
    for generation in 40..44 {
        churn(&env, "t", 300, generation, 100);
        check(&env);
    }
    assert!(reuses(&env).iter().any(|&(_, n, _)| n > held), "the reader's pages come back");
}

/// In `SyncMode::Durable` a page freed by txn N is reused from txn N + 2, and no earlier.
#[test]
fn durable_reuse_waits_two_commits() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    for generation in 0..30 {
        churn(&env, "t", 200, generation, 120);
        check(&env);
    }
    let reuses = reuses(&env);
    assert!(!reuses.is_empty());
    for &(t, n, p) in &reuses {
        assert!(t >= n + 2, "page {p} freed by txn {n} reused by txn {t}");
    }
    assert_eq!(reuses.iter().map(|&(t, n, _)| t - n).min(), Some(2), "and as soon as that");
}

/// The free records of the newest state, in key order, with the pages each lists.
fn free_records(env: &Env) -> Vec<(RecordKey, Vec<u32>)> {
    let snapshot = env.inner().current();
    let mut records = Vec::new();
    let mut after = None;
    while let Some((key, value)) =
        next_record(&*snapshot, &snapshot.meta.free, after).expect("record")
    {
        records.push((key, decode_pages(value, snapshot.meta.page_count).expect("pages")));
        after = Some(key);
    }
    records
}

/// A transaction that takes a free record and uses only some of its pages lists the rest again
/// under the keys of the records it took, which the gate has passed: the next transaction reuses
/// them at once, not two commits on as the pages the transaction freed itself.
#[test]
fn leftover_pages_are_listed_under_the_keys_taken_and_reused_at_once() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    for i in 0..300 {
        txn.put(&a, &key(i), &value(i, 0, 700)).expect("put");
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    txn.drop_table(a).expect("drop");
    let dropped = txn.commit().expect("commit");
    churn(&env, "s", 1, 0, 8);
    let loads = |env: &Env, txn: u64| -> Vec<u32> {
        logs(env).0.iter().filter(|&&(t, _)| t == txn).map(|&(_, p)| p).collect()
    };

    // The drop's record passes the gate: the next commit takes it and needs a few of its pages.
    let taker = churn(&env, "s", 1, 1, 8);
    let taken = loads(&env, taker);
    assert!(taken.len() > 50, "the drop's record: {taken:?}");
    let records = free_records(&env);
    let leftover: Vec<u32> = records
        .iter()
        .filter(|(k, _)| k.txn <= dropped)
        .flat_map(|(_, pages)| pages.iter().copied())
        .collect();
    assert!(!leftover.is_empty() && leftover.len() < taken.len(), "{taken:?} then {records:?}");
    assert!(leftover.iter().all(|p| taken.contains(p)), "only leftovers keep old keys");
    for (key, pages) in &records {
        assert!(
            key.txn <= dropped || pages.iter().all(|p| !taken.contains(p)),
            "a leftover listed under {key:?}, past the gate: {records:?}"
        );
    }

    // The next commit takes them, though no commit has passed since.
    let next = churn(&env, "s", 1, 2, 8);
    let reused = loads(&env, next);
    assert!(!reused.is_empty(), "the leftovers are reused at once");
    assert!(reused.iter().all(|p| leftover.contains(p)), "{reused:?} not from {leftover:?}");
    check(&env);
}

/// In `SyncMode::NoSync` the meta slots move only at a sync: a freed page comes back once both
/// slots hold states from its freeing on, so after two syncs.
#[test]
fn nosync_reuse_waits_for_two_syncs() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::NoSync);
    let mut slot_floor: HashMap<u64, u64> = HashMap::new();
    let mut step = |env: &Env, generation: u32| {
        let floor = env.with_writer(|w| w.slots().iter().flatten().copied().min().expect("valid"));
        let txn = churn(env, "t", 200, generation, 120);
        slot_floor.insert(txn, floor);
        check(env);
    };
    for generation in 0..10 {
        step(&env, generation);
    }
    env.sync().expect("sync");
    for generation in 10..15 {
        step(&env, generation);
    }
    assert!(logs(&env).0.is_empty(), "nothing is reused before the second sync");
    env.sync().expect("sync");
    for generation in 15..20 {
        step(&env, generation);
    }
    let reuses = reuses(&env);
    assert!(!reuses.is_empty(), "the second sync lets pages come back");
    for &(t, n, p) in &reuses {
        assert!(n <= slot_floor[&t], "page {p} freed by txn {n} reused by txn {t}");
    }
}

/// Overwriting the same keys commit after commit keeps the file at a steady size.
#[test]
fn steady_churn_keeps_the_file_bounded() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let mut early = 0;
    for generation in 0..150 {
        churn(&env, "t", 300, generation, 100);
        if generation == 10 {
            early = page_count(&env);
        }
    }
    let report = check(&env);
    assert!(report.page_count <= early + early / 10, "{early} pages then {}", report.page_count);
}

/// A dropped table's pages, and a cleared one's, build the next table once the gate passes.
#[test]
fn dropped_and_cleared_tables_pages_are_reused() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let fill = |name: &str| {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table(name).expect("table");
        for i in 0..1500 {
            txn.put(&t, &key(i), &value(i, 0, 200)).expect("put");
        }
        txn.commit().expect("commit");
        t
    };
    let small = |n: u32| churn(&env, "small", 1, n, 8);
    let a = fill("a");
    let full = check(&env);
    assert!(full.tables["a"].pages > 50);

    let mut txn = env.begin_rw().expect("begin");
    txn.drop_table(a).expect("drop");
    txn.commit().expect("commit");
    let dropped = check(&env);
    assert!(dropped.free_pages >= full.tables["a"].pages, "{dropped:?}");
    // The drop's pages pass the gate two commits on.
    small(0);
    let b = fill("b");
    let refilled = check(&env);
    assert!(refilled.page_count <= dropped.page_count + 4, "{dropped:?} then {refilled:?}");

    let mut txn = env.begin_rw().expect("begin");
    txn.clear_table(&b).expect("clear");
    txn.commit().expect("commit");
    let cleared = check(&env);
    assert_eq!(cleared.tables["b"], crate::TableCheck::default());
    small(1);
    fill("b");
    let again = check(&env);
    assert!(again.page_count <= cleared.page_count + 4, "{cleared:?} then {again:?}");
}

/// An aborted transaction leaves the records it took in the free tree: the next one takes the
/// same pages again, and the accounting stays exact.
#[test]
fn an_abort_returns_the_records_it_took() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    for generation in 0..4 {
        churn(&env, "t", 300, generation, 100);
    }
    let free_root = env.inner().current().meta.free;
    let run = |env: &Env, commit: bool| -> Vec<u32> {
        let before = logs(env).0.len();
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("t").expect("t");
        for i in 0..300 {
            txn.put(&t, &key(i), &value(i, 9, 100)).expect("put");
        }
        if commit {
            txn.commit().expect("commit");
        } else {
            txn.abort();
        }
        logs(env).0[before..].iter().map(|&(_, p)| p).collect()
    };
    let aborted = run(&env, false);
    assert!(!aborted.is_empty(), "the transaction took records");
    assert_eq!(env.inner().current().meta.free, free_root);
    assert_eq!(env.with_writer(|w| w.alloc.last_consumed()), None);
    assert_eq!(run(&env, true), aborted, "the next transaction takes the same records");
    check(&env);
}

/// The save settles in at most three rounds, from commits that free one page to thousands.
#[test]
fn the_save_settles_within_three_rounds() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let mut rounds = Vec::new();
    for (n, size) in [1_u32, 30, 300, 1200, 2500, 5, 4000].into_iter().enumerate() {
        let name = format!("t{n}");
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table(&name).expect("table");
        for i in 0..size {
            txn.put(&t, &key(i), &value(i, 0, 700)).expect("put");
        }
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("begin");
        txn.drop_table(t).expect("drop");
        txn.writer_mut().max_save_rounds = 0;
        txn.commit().expect("commit");
        rounds.push(env.with_writer(|w| w.max_save_rounds));
        check(&env);
    }
    assert!(rounds.iter().all(|&r| r <= 3), "{rounds:?}");
    assert!(rounds.iter().any(|&r| r >= 2), "the list was written again: {rounds:?}");
}

/// A save whose record count shrinks between rounds: the transaction leaves just over eight
/// records' worth of pages free, and the first round's writes (splits of the free tree) take pages
/// from that pool, so the second round needs one record fewer. The unneeded record is written
/// empty, never deleted inside the loop (a delete could free a page and change the list again),
/// and the save settles.
#[test]
fn a_record_count_that_shrinks_mid_save_leaves_an_empty_record() {
    use crate::tree::PageWrite as _;
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let capacity = record_capacity(&env.inner().geometry);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    txn.put(&t, &key(0), &value(0, 0, 8)).expect("put");
    // Make pages free in the transaction (private pages back to its pool) until it must list
    // eight full records and one page more.
    let writer = txn.writer_mut();
    let want = 8 * capacity + 1;
    let listed = |writer: &crate::env::Writer| {
        let (pool, freed) = writer.alloc.reusable().expect("reusable");
        pool.len() + freed.len()
    };
    let have = listed(writer);
    let pages: Vec<u32> = (have..want).map(|_| writer.alloc().expect("alloc")).collect();
    for p in pages {
        writer.free(p);
    }
    assert_eq!(listed(writer), want);
    writer.max_save_rounds = 0;
    let txn_id = txn.commit().expect("commit");
    assert!((2..=3).contains(&env.with_writer(|w| w.max_save_rounds)));
    check(&env);

    let snapshot = env.inner().current();
    let mut records = Vec::new();
    let mut after = None;
    while let Some((key, value)) =
        next_record(&*snapshot, &snapshot.meta.free, after).expect("record")
    {
        records.push((key, decode_pages(value, snapshot.meta.page_count).expect("pages")));
        after = Some(key);
    }
    let seqs: Vec<(u64, u32)> = records.iter().map(|(k, _)| (k.txn, k.seq)).collect();
    assert_eq!(seqs, (0..9).map(|seq| (txn_id, seq)).collect::<Vec<_>>());
    let listed: usize = records.iter().map(|(_, pages)| pages.len()).sum();
    assert!(listed < 8 * capacity, "the pool shrank below eight records: {listed}");
    assert!(records[..8].iter().all(|(_, pages)| !pages.is_empty()));
    assert!(records[8].1.is_empty(), "the ninth record is left empty");
    drop(snapshot);
    drop(env);
    check(&open(&path, SyncMode::Durable));
}

/// A randomized multi-table workload (puts, deletes, clears, drops, aborts, readers held across
/// commits, syncs and reopens): after every commit and every reopen the accounting is exact, the
/// newest state matches the model, and each reader its snapshot's model.
fn random_workload(seed: u64, page_size: u32, sync: SyncMode, rounds: u32) {
    let _report = ReportSeed(seed);
    let mut rng = Rng(seed);
    let (_dir, path) = temp();
    let options = EnvOptions { page_size, sync, ..EnvOptions::default() };
    let mut env = Env::open(&path, options).expect("open");
    // The inline limit: the workload's values stay in their leaves.
    let max_value = crate::page::Geometry::new(page_size).expect("geometry").max_value(4);
    type Model = BTreeMap<String, BTreeMap<u32, Vec<u8>>>;
    let mut model: Model = BTreeMap::new();
    let mut readers: Vec<(RoTxn, Model)> = Vec::new();
    let (mut max_rounds, mut reused) = (0, 0);
    let verify = |txn: &dyn ReadTxnDyn, model: &Model| {
        for (name, rows) in model {
            let t = txn.table(name);
            assert_eq!(txn.entries(&t), rows.len() as u64, "table {name}");
            for (k, v) in rows {
                assert_eq!(txn.value(&t, *k).as_deref(), Some(&v[..]), "{name}/{k}");
            }
        }
    };
    for round in 0..rounds {
        let mut work = model.clone();
        let mut txn = env.begin_rw().expect("begin");
        for _ in 0..1 + rng.below(40) {
            let name = format!("t{}", rng.below(4));
            let roll = rng.below(100);
            if roll < 2 {
                if let Some(t) = txn.open_table(&name).expect("open") {
                    txn.drop_table(t).expect("drop");
                    work.remove(&name);
                }
                continue;
            }
            let t = txn.create_table(&name).expect("table");
            let rows = work.entry(name).or_default();
            let k = rng.below(400) as u32;
            if roll < 4 {
                txn.clear_table(&t).expect("clear");
                rows.clear();
            } else if roll < 30 {
                assert_eq!(txn.delete(&t, &key(k)).expect("delete"), rows.remove(&k).is_some());
            } else {
                let v = value(k, round, rng.len_upto(max_value / 3));
                txn.put(&t, &key(k), &v).expect("put");
                rows.insert(k, v);
            }
        }
        if rng.below(8) == 0 {
            txn.abort();
        } else {
            txn.commit().expect("commit");
            model = work;
            max_rounds = max_rounds.max(env.with_writer(|w| w.max_save_rounds));
            check(&env);
        }
        if rng.below(4) == 0 && readers.len() < 3 {
            readers.push((env.begin_ro().expect("ro"), model.clone()));
        }
        if !readers.is_empty() && rng.below(5) == 0 {
            let (ro, snapshot) = readers.remove(rng.below(readers.len() as u64) as usize);
            verify(&ro, &snapshot);
        }
        if sync == SyncMode::NoSync && rng.below(4) == 0 {
            env.sync().expect("sync");
        }
        if rng.below(50) == 0 {
            for (ro, snapshot) in readers.drain(..) {
                verify(&ro, &snapshot);
            }
            reused += logs(&env).0.len();
            drop(env);
            env = Env::open(&path, options).expect("reopen");
            check(&env);
        }
        if round % 16 == 0 {
            verify(&env.begin_ro().expect("ro"), &model);
        }
    }
    for (ro, snapshot) in readers.drain(..) {
        verify(&ro, &snapshot);
    }
    verify(&env.begin_ro().expect("ro"), &model);
    assert!(max_rounds <= 3, "the save took {max_rounds} rounds");
    reused += logs(&env).0.len();
    assert!(reused > 0, "pages were reused");
}

/// Object-safe reads for the workload's model comparison.
trait ReadTxnDyn {
    fn table(&self, name: &str) -> Table;
    fn entries(&self, t: &Table) -> u64;
    fn value(&self, t: &Table, k: u32) -> Option<Vec<u8>>;
}

impl<T: ReadTxn> ReadTxnDyn for T {
    fn table(&self, name: &str) -> Table {
        self.open_table(name).expect("open").expect("the table exists")
    }

    fn entries(&self, t: &Table) -> u64 {
        self.table_stat(t).expect("stat").entries
    }

    fn value(&self, t: &Table, k: u32) -> Option<Vec<u8>> {
        self.get(t, &key(k)).expect("get").map(<[u8]>::to_vec)
    }
}

#[test]
fn accounting_is_exact_in_a_random_workload() {
    random_workload(0x5EED_F4EE, 4096, SyncMode::Durable, 160);
    random_workload(0x5EED_F4EF, 4096, SyncMode::NoSync, 160);
    random_workload(0x5EED_F4F0, 16384, SyncMode::Durable, 60);
}

#[test]
#[ignore = "heavy: run with --release"]
fn accounting_is_exact_in_a_random_workload_heavy() {
    for seed in 0..6_u64 {
        for (i, p) in PAGE_SIZES.into_iter().enumerate() {
            let sync = if (seed + i as u64).is_multiple_of(2) {
                SyncMode::Durable
            } else {
                SyncMode::NoSync
            };
            random_workload(0xF4EE_0000 + seed * 16 + i as u64, p, sync, 1500);
        }
    }
}

/// Where the first two free records lie: the free tree's leftmost leaf and each record's value
/// offset in the file, and the pages each lists.
#[derive(Debug)]
struct Records {
    page_count: u32,
    leaf: u32,
    values: [(u64, Vec<u32>); 2],
    /// The file offset of the root's second separator (the leftmost leaf's upper bound).
    separator: u64,
}

/// A file whose free tree has two levels (a dropped table of ~1100 pages) and whose first records
/// pass the reuse gate once reopened.
fn file_with_records(path: &Path) -> Records {
    let env = open(path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("big").expect("big");
    for i in 0..4400 {
        txn.put(&t, &key(i), &value(i, 0, 900)).expect("put");
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    txn.drop_table(t).expect("drop");
    txn.commit().expect("commit");
    churn(&env, "x", 1, 0, 8);
    check(&env);
    let snapshot = env.inner().current();
    let free = snapshot.meta.free;
    assert_eq!(free.height, 2, "{free:?}");
    let root = snapshot.page(free.root).expect("root");
    let leaf = page::branch_child(root, 0).expect("child").0;
    let leaf_page = snapshot.page(leaf).expect("leaf");
    let base = leaf_page.as_ptr() as u64;
    let record = |i: usize| {
        let (k, v) = page::leaf_inline_entry(leaf_page, i).expect("entry");
        assert_eq!(RecordKey::decode(k).expect("key"), RecordKey { txn: 2, seq: i as u32 });
        let offset = u64::from(leaf) * 4096 + (v.as_ptr() as u64 - base);
        (offset, decode_pages(v, snapshot.meta.page_count).expect("pages"))
    };
    let sep = page::branch_key(root, 1).expect("separator");
    let separator = u64::from(free.root) * 4096 + (sep.as_ptr() as u64 - root.as_ptr() as u64);
    assert!(RecordKey::decode(sep).expect("a record key").seq > 0);
    Records {
        page_count: snapshot.meta.page_count,
        leaf,
        values: [record(0), record(1)],
        separator,
    }
}

/// Rewrite the `u32` at `offset` of the file and re-stamp the CRC of the page holding it.
fn patch(path: &Path, offset: u64, p: u32) {
    let file = fs::OpenOptions::new().read(true).write(true).open(path).expect("open");
    file.write_all_at(&p.to_le_bytes(), offset).expect("write");
    let start = offset / 4096 * 4096;
    let mut page = vec![0; 4096];
    file.read_exact_at(&mut page, start).expect("read");
    crc::add_crc32_nonzero(&mut page);
    file.write_all_at(&page, start).expect("write");
}

/// A damaged free record — a flipped byte in its leaf, a page out of range or of order, a meta
/// page, a page another record lists, a leaf outside its parent's separators — is refused when the
/// writer reaches it: the write fails `Corrupt`, the environment latches, and none of the record's
/// pages is handed out.
#[test]
fn damaged_free_records_are_never_handed_out() {
    let (dir, base) = temp();
    let records = file_with_records(&base);
    let [(first, pages0), (second, pages1)] = &records.values;
    let last0 = first + 4 * (pages0.len() as u64 - 1);
    type Damage<'a> = (&'a str, Box<dyn Fn(&Path) + 'a>);
    let damages: Vec<Damage<'_>> = vec![
        (
            "flip",
            Box::new(|p: &Path| {
                let file = fs::OpenOptions::new().read(true).write(true).open(p).expect("open");
                let at = u64::from(records.leaf) * 4096 + 2000;
                let mut b = [0];
                file.read_exact_at(&mut b, at).expect("read");
                file.write_all_at(&[b[0] ^ 0x40], at).expect("write");
            }),
        ),
        ("range", Box::new(|p: &Path| patch(p, last0, records.page_count))),
        ("order", Box::new(|p: &Path| patch(p, *first, pages0[1]))),
        ("meta", Box::new(|p: &Path| patch(p, *first, 1))),
        ("twice", Box::new(|p: &Path| patch(p, *second, pages0[pages0.len() - 1]))),
        // The leftmost leaf's upper bound moved below its keys: a leaf outside its range.
        ("separator", Box::new(|p: &Path| patch(p, records.separator + 8, 0))),
    ];
    for (what, damage) in &damages {
        let path = dir.path().join(what);
        fs::copy(&base, &path).expect("copy");
        damage(&path);
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("x").expect("x");
        let mut failed = None;
        for i in 0..4000 {
            if let Err(e) = txn.put(&t, &key(i), &value(i, 1, 900)) {
                failed = Some(e);
                break;
            }
        }
        assert!(matches!(failed, Some(Error::Corrupt(_))), "{what}: {failed:?}");
        assert!(env.failure().is_some(), "{what}: latched");
        let loaded: Vec<u32> = txn.writer_mut().alloc.load_log.iter().map(|&(_, p)| p).collect();
        let expect: &[u32] = if *what == "twice" { pages0 } else { &[] };
        assert_eq!(loaded, expect, "{what}: only sound records are taken");
        assert!(pages1.iter().all(|&p| !txn.writer_mut().alloc.is_private(p)), "{what}");
    }
}

/// Through a power loss after commits that reuse pages — strict (only flushed bytes survive) and
/// random (unflushed pages old, new or torn by sector) — the file reopens to the last durable
/// commit, passes `Env::check`, and holds exactly that commit's data.
#[test]
fn power_loss_after_reuse_recovers_a_committed_state() {
    for (case, sync) in [SyncMode::Durable, SyncMode::NoSync].into_iter().enumerate() {
        for random in [None, Some(1_u64), Some(2), Some(3)] {
            let (_dir, path) = temp();
            let env = open(&path, sync);
            disk_model::track(&path);
            let (mut latest, mut durable) = (BTreeMap::new(), BTreeMap::new());
            for generation in 0..24 {
                let n = 100 + (generation * 37) % 200;
                churn(&env, "t", n, generation, 100);
                latest.extend((0..n).map(|i| (i, value(i, generation, 100))));
                if sync == SyncMode::Durable || generation % 4 == 3 {
                    env.sync().expect("sync");
                    durable.clone_from(&latest);
                }
            }
            assert!(!logs(&env).0.is_empty(), "pages were reused");
            let mut txn = env.begin_rw().expect("begin");
            let t = txn.create_table("t").expect("t");
            for i in 0..300 {
                txn.put(&t, &key(i), &value(i, 99, 100)).expect("put");
            }
            if sync == SyncMode::Durable {
                txn.writer_mut().file.fail_next_msync();
                assert!(txn.commit().is_err(), "the failed commit");
            } else {
                txn.commit().expect("an unsynced commit");
            }
            env.crash();
            match random {
                None => disk_model::power_loss(&path),
                Some(seed) => disk_model::power_loss_random(&path, seed + case as u64 * 16),
            }
            disk_model::untrack(&path);

            let env = open(&path, sync);
            check(&env);
            let ro = env.begin_ro().expect("ro");
            let t = ro.open_table("t").expect("open").expect("t");
            assert_eq!(ro.table_stat(&t).expect("stat").entries, durable.len() as u64);
            for (k, v) in &durable {
                assert_eq!(ro.get(&t, &key(*k)).expect("get"), Some(&v[..]), "{sync:?} {k}");
            }
        }
    }
}

/// `Env::check` names what it finds: a page failing its CRC, a page leaked (counted by the meta,
/// used by nothing), a page both a tree and the free list claim (with and without a leak), and a
/// tree holding other than the entries its root counts.
#[test]
fn check_reports_damage_leaks_and_double_uses() {
    let (dir, base) = temp();
    let env = open(&base, SyncMode::Durable);
    for generation in 0..4 {
        churn(&env, "t", 300, generation, 100);
    }
    let report = check(&env);
    assert_eq!(report.tables["t"].entries, 300);
    assert!(report.free_pages > 0 && report.tables["t"].height == 2, "{report:?}");
    let snapshot = env.inner().current();
    let (free, meta) = (snapshot.meta.free, snapshot.meta);
    assert_eq!(free.height, 1, "{free:?}");
    let leaf = snapshot.page(free.root).expect("free leaf");
    let (_, v) = page::leaf_inline_entry(leaf, 0).expect("record");
    let listed = decode_pages(v, meta.page_count).expect("pages");
    let value_at = u64::from(free.root) * 4096 + (v.as_ptr() as u64 - leaf.as_ptr() as u64);
    let record = catalog::get(&*snapshot, &meta.catalog, "t").expect("get").expect("t");
    let catalog_leaf = snapshot.page(meta.catalog.root).expect("catalog");
    let (_, rec) = page::leaf_inline_entry(catalog_leaf, 0).expect("t's record");
    let entries_at = u64::from(meta.catalog.root) * 4096
        + (rec.as_ptr() as u64 - catalog_leaf.as_ptr() as u64)
        + 24;
    let table_leaf =
        page::branch_child(snapshot.page(record.root.root).expect("root"), 0).expect("child").0;
    // A tree page that can replace one listed page with the list still ascending.
    let (at, tree_page) = (0..listed.len())
        .find_map(|i| {
            let lo = if i == 0 { 1 } else { listed[i - 1] };
            let hi = listed.get(i + 1).copied().unwrap_or(meta.page_count);
            tree_pages(&env).into_iter().find(|&r| lo < r && r < hi).map(|r| (i, r))
        })
        .expect("a tree page fits the record");
    drop(snapshot);
    drop(env);

    let case = |name: &str, damage: &dyn Fn(&Path)| -> String {
        let path = dir.path().join(name);
        fs::copy(&base, &path).expect("copy");
        damage(&path);
        let env = open(&path, SyncMode::Durable);
        let err = env.check().expect_err(name).to_string();
        assert!(env.failure().is_some(), "{name}: a failed check latches");
        err
    };
    let crc = case("crc", &|p: &Path| {
        let file = fs::OpenOptions::new().read(true).write(true).open(p).expect("open");
        let at = u64::from(table_leaf) * 4096 + 4000;
        let mut b = [0];
        file.read_exact_at(&mut b, at).expect("read");
        file.write_all_at(&[b[0] ^ 1], at).expect("write");
    });
    assert!(crc.contains(&format!("page {table_leaf} fails its CRC")), "{crc}");

    let twice = case("twice", &|p: &Path| patch(p, value_at + 4 * at as u64, tree_page));
    let lost = listed[at];
    assert!(twice.contains(&format!("leaked [{lost}]")), "{twice}");
    assert!(twice.contains(&format!("used twice [{tree_page} (")), "{twice}");
    assert!(twice.contains("and free list)]"), "{twice}");

    let extra = case("extra", &|p: &Path| {
        let file = fs::OpenOptions::new().read(true).write(true).open(p).expect("open");
        let start = u64::from(free.root) * 4096;
        let mut page = vec![0; 4096];
        file.read_exact_at(&mut page, start).expect("read");
        let mut more = listed.clone();
        more.push(tree_page);
        more.sort_unstable();
        assert!(
            page::leaf_replace_value(&mut page, 0, &encode_pages(&more)).expect("fits")
                == page::Fit::Done
        );
        crc::add_crc32_nonzero(&mut page);
        file.write_all_at(&page, start).expect("write");
    });
    assert!(extra.contains(&format!("leaked []; used twice [{tree_page} (")), "{extra}");

    let entries = case("entries", &|p: &Path| patch(p, entries_at, 299));
    assert!(entries.contains("300 entries, not the 299 its root counts"), "{entries}");

    let leak = case("leak", &|p: &Path| {
        let file = fs::OpenOptions::new().read(true).write(true).open(p).expect("open");
        let grown = Meta { page_count: meta.page_count + 1, ..meta };
        file.write_all_at(&grown.encode(), 0).expect("write");
        file.write_all_at(&grown.encode(), 4096).expect("write");
    });
    assert!(leak.contains(&format!("leaked [{}]; used twice []", meta.page_count)), "{leak}");
}

/// `Env::check` refuses a root branch with one child (its other cells deleted, its CRC stamped
/// again): the writer never leaves one, since such a root gives way to its child.
#[test]
fn check_refuses_a_root_branch_with_one_child() {
    let (dir, base) = temp();
    let env = open(&base, SyncMode::Durable);
    churn(&env, "t", 300, 0, 100);
    let root = check(&env).tables["t"];
    assert_eq!(root.height, 2, "{root:?}");
    let r = {
        let snapshot = env.inner().current();
        catalog::get(&*snapshot, &snapshot.meta.catalog, "t").expect("get").expect("t").root.root
    };
    drop(env);
    let path = dir.path().join("single");
    fs::copy(&base, &path).expect("copy");
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let mut page = vec![0; 4096];
    file.read_exact_at(&mut page, u64::from(r) * 4096).expect("read");
    for i in (1..page::nkeys(&page)).rev() {
        page::branch_delete(&mut page, i).expect("delete");
    }
    crc::add_crc32_nonzero(&mut page);
    file.write_all_at(&page, u64::from(r) * 4096).expect("write");
    let env = open(&path, SyncMode::Durable);
    let err = env.check().expect_err("one child").to_string();
    assert!(err.contains(&format!("page {r} is a root branch with one child")), "{err}");
}

/// `Env::check` compares each root's page counters with the pages its walk finds, by kind, and
/// `table_stat` reports them: a table record counting one leaf, one branch or one overflow page
/// too many (its shape still possible, its leaf's CRC re-stamped) is `Corrupt`, naming the kind.
#[test]
fn check_compares_the_page_counters() {
    let (dir, base) = temp();
    let env = open(&base, SyncMode::Durable);
    churn(&env, "t", 300, 0, 100);
    let report = check(&env);
    let ro = env.begin_ro().expect("ro");
    let stat = ro.table_stat(&ro.open_table("t").expect("open").expect("t")).expect("stat");
    assert_eq!((stat.height, stat.branch_pages, stat.overflow_pages), (2, 1, 0), "{stat:?}");
    assert_eq!(u64::from(stat.branch_pages + stat.leaf_pages), report.tables["t"].pages);
    drop(ro);
    let snapshot = env.inner().current();
    let catalog = snapshot.meta.catalog;
    assert_eq!(catalog.height, 1, "the catalog is one leaf");
    let leaf = snapshot.page(catalog.root).expect("catalog");
    let (_, record) = page::leaf_inline_entry(leaf, 0).expect("t's record");
    let root_at =
        u64::from(catalog.root) * 4096 + (record.as_ptr() as u64 - leaf.as_ptr() as u64) + 8;
    drop(snapshot);
    drop(env);

    let case = |name: &str, offset: u64, count: u32| -> String {
        let path = dir.path().join(name);
        fs::copy(&base, &path).expect("copy");
        patch(&path, offset, count);
        let env = open(&path, SyncMode::Durable);
        let err = env.check().expect_err(name).to_string();
        assert!(env.failure().is_some(), "{name}: a failed check latches");
        err
    };
    let leaves = stat.leaf_pages;
    let err = case("leaf", root_at + 28, leaves + 1);
    let want = format!("table \"t\": {leaves} leaf pages, not the {} its root counts", leaves + 1);
    assert!(err.contains(&want), "{err}");
    let err = case("branch", root_at + 24, 2);
    assert!(err.contains("table \"t\": 1 branch pages, not the 2 its root counts"), "{err}");
    let err = case("overflow", root_at + 12, 1);
    assert!(err.contains("table \"t\": 0 overflow pages, not the 1 its root counts"), "{err}");
}

/// The pages of table `t` (two levels) and of the catalog (one) in the newest state.
fn tree_pages(env: &Env) -> Vec<u32> {
    let snapshot = env.inner().current();
    let record = catalog::get(&*snapshot, &snapshot.meta.catalog, "t").expect("get").expect("t");
    let root = snapshot.page(record.root.root).expect("root");
    let mut pages = vec![record.root.root, snapshot.meta.catalog.root];
    for i in 0..page::nkeys(root) {
        pages.push(page::branch_child(root, i).expect("child").0);
    }
    pages
}

/// The length of key `i`'s value in [`fill_with_runs`]: an overflow run of 1 to 6 pages for every
/// 25th key, 100 inline bytes for the rest.
fn run_value_len(i: u32) -> usize {
    if i.is_multiple_of(25) {
        (i / 25 % 6) as usize * 4096 + 2000
    } else {
        100
    }
}

/// Put keys `0..300` into `t`: 288 inline values and 12 in runs of 1 to 6 pages (42 pages) spread
/// over the leaves of a two-level tree, or, without `runs`, every value inline.
fn fill_with_runs(txn: &mut RwTxn, t: &Table, generation: u32, runs: bool) {
    for i in 0..300 {
        let len = if runs { run_value_len(i) } else { 100 };
        txn.put(t, &key(i), &value(i, generation, len)).expect("put");
    }
}

fn stat_of(env: &Env, name: &str) -> TableStat {
    let ro = env.begin_ro().expect("ro");
    ro.table_stat(&ro.open_table(name).expect("open").expect("the table")).expect("stat")
}

/// Test-only: the published pages copy-on-write and tree walks have verified on this thread
/// (debug builds only; 0 in release, where the counter does not exist).
fn verifies() -> u64 {
    #[cfg(debug_assertions)]
    return crate::tree::write::COW_VERIFIES.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    0
}

/// Clear (or drop) table `name` in one commit. Returns the published pages the free verified
/// before following them.
fn free_table(env: &Env, name: &str, drop: bool) -> u64 {
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.open_table(name).expect("open").expect("the table");
    let before = verifies();
    if drop { txn.drop_table(t) } else { txn.clear_table(&t) }.expect("free");
    let verified = verifies() - before;
    txn.commit().expect("commit");
    verified
}

/// A cleared table holding overflow runs, and a dropped one, gives every run page to the free list
/// (`Env::check` accounts for each, as single pages), and the next fill of the same values reuses
/// them once the gate passes. Freeing a table with runs reads and verifies every leaf (each run
/// it names leaves with it); a table with none keeps the fast path, verifying its branches and no
/// leaf (debug builds count the verifies of a writer that stamped none of the pages).
#[test]
fn cleared_and_dropped_runs_are_freed_and_reused() {
    let (_dir, path) = temp();
    let fill = |env: &Env, name: &str, runs: bool| {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table(name).expect("table");
        fill_with_runs(&mut txn, &t, 0, runs);
        txn.commit().expect("commit");
    };
    let mut generation = 0;
    for drop in [false, true] {
        for runs in [true, false] {
            let name = if runs { "runs" } else { "inline" };
            let env = open(&path, SyncMode::Durable);
            fill(&env, name, runs);
            let full = check(&env);
            let stat = stat_of(&env, name);
            let want_overflow = if runs { 42 } else { 0 };
            assert_eq!((stat.height, stat.overflow_pages), (2, want_overflow), "{stat:?}");
            let pages = stat.branch_pages + stat.leaf_pages + stat.overflow_pages;
            assert_eq!(full.tables[name].pages, u64::from(pages), "{full:?}");
            // A fresh writer: the pages it frees are published and unstamped.
            std::mem::drop(env);
            let env = open(&path, SyncMode::Durable);

            let verified = free_table(&env, name, drop);
            if cfg!(debug_assertions) {
                let leaves = if runs { stat.leaf_pages } else { 0 };
                // A drop also copies the catalog's one leaf to delete the table's record.
                let want = u64::from(stat.branch_pages + leaves + u32::from(drop));
                assert_eq!(verified, want, "{name} (drop {drop}): pages verified, {stat:?}");
            }
            let freed = check(&env);
            assert!(freed.free_pages >= u64::from(pages), "{name}: {full:?} then {freed:?}");
            assert_eq!(freed.tables.contains_key(name), !drop, "{freed:?}");
            // The free passes the gate two commits on, as in
            // `dropped_and_cleared_tables_pages_are_reused`.
            churn(&env, "small", 1, generation, 8);
            generation += 1;
            fill(&env, name, runs);
            let again = check(&env);
            assert_eq!(again.tables[name].pages, u64::from(pages), "{again:?}");
            assert!(again.page_count <= freed.page_count + 4, "{freed:?} then {again:?}");
            free_table(&env, name, true);
            churn(&env, "small", 1, generation, 8);
            generation += 1;
        }
    }
}

/// A file whose table `t` holds 300 entries ([`fill_with_runs`], 12 runs) and whose free list holds
/// the pages a second commit (rewriting the first inline values) freed.
struct RunsFile {
    /// Each run of `t` in key order, as `(first page, pages, value length)`.
    runs: Vec<(u32, u32, u32)>,
    /// `t`'s counters.
    stat: TableStat,
    /// The file offset of `t`'s root in its catalog record (the overflow counter is at +12).
    root_at: u64,
    /// The free tree's one leaf.
    free_leaf: u32,
}

fn file_with_runs(path: &Path) -> RunsFile {
    let env = open(path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("table");
    fill_with_runs(&mut txn, &t, 0, true);
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    for i in 1..20 {
        txn.put(&t, &key(i), &value(i, 1, 100)).expect("put");
    }
    txn.commit().expect("commit");
    check(&env);
    let stat = stat_of(&env, "t");
    let snapshot = env.inner().current();
    let meta = snapshot.meta;
    assert_eq!((meta.catalog.height, meta.free.height), (1, 1), "{meta:?}");
    let record = catalog::get(&*snapshot, &meta.catalog, "t").expect("get").expect("t");
    let catalog_leaf = snapshot.page(meta.catalog.root).expect("catalog");
    let (_, rec) = page::leaf_inline_entry(catalog_leaf, 0).expect("t's record");
    let root_at = u64::from(meta.catalog.root) * 4096
        + (rec.as_ptr() as u64 - catalog_leaf.as_ptr() as u64)
        + 8;
    let root = snapshot.page(record.root.root).expect("root");
    let mut runs = Vec::new();
    for c in 0..page::nkeys(root) {
        let leaf = snapshot.page(page::branch_child(root, c).expect("child").0).expect("leaf");
        for i in 0..page::nkeys(leaf) {
            if let (_, page::Val::Overflow(ovf)) = page::leaf_entry(leaf, i).expect("entry") {
                let n = crate::overflow::run_pages(ovf.len as usize, 4096) as u32;
                runs.push((ovf.pgno, n, ovf.len));
            }
        }
    }
    assert_eq!(runs.len(), 12);
    assert!(runs.iter().map(|r| r.1).eq([1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6]), "{runs:?}");
    RunsFile { runs, stat, root_at, free_leaf: meta.free.root }
}

/// Write `bytes` at `offset` into the run starting at page `first` of `n` pages, then re-stamp the
/// run's one CRC (unless `stamp` is false), so only the check under test can find the damage.
fn patch_run(path: &Path, first: u32, n: u32, offset: usize, bytes: &[u8], stamp: bool) {
    let file = fs::OpenOptions::new().read(true).write(true).open(path).expect("open");
    let start = u64::from(first) * 4096;
    let mut run = vec![0; n as usize * 4096];
    file.read_exact_at(&mut run, start).expect("read");
    run[offset..offset + bytes.len()].copy_from_slice(bytes);
    if stamp {
        crc::add_crc32_nonzero(&mut run);
    }
    file.write_all_at(&run, start).expect("write");
}

/// Clearing or dropping a table whose run is damaged (its header), whose leaf is damaged (one a
/// table without runs would not read), or whose overflow counter is not what its leaves name is
/// `Corrupt` before anything is freed: the allocator is untouched, the environment latches, and
/// the file is byte for byte the damaged one, so a reopen finds the accounting as it was.
#[test]
fn a_damaged_run_or_counter_refuses_clear_and_drop() {
    let (dir, base) = temp();
    let f = file_with_runs(&base);
    let (first, n, _) = f.runs[5];
    assert_eq!(n, 6);
    let last_leaf = {
        let env = open(&base, SyncMode::Durable);
        let snapshot = env.inner().current();
        let record =
            catalog::get(&*snapshot, &snapshot.meta.catalog, "t").expect("get").expect("t");
        let root = snapshot.page(record.root.root).expect("root");
        page::branch_child(root, page::nkeys(root) - 1).expect("child").0
    };
    type Damage<'a> = &'a dyn Fn(&Path);
    let header: Damage<'_> = &|p| patch_run(p, first, n, 0, &(first + 1).to_le_bytes(), true);
    let leaf: Damage<'_> = &|p| {
        let file = fs::OpenOptions::new().read(true).write(true).open(p).expect("open");
        let at = u64::from(last_leaf) * 4096 + 2000;
        let mut b = [0];
        file.read_exact_at(&mut b, at).expect("read");
        file.write_all_at(&[b[0] ^ 1], at).expect("write");
    };
    let counter: Damage<'_> = &|p| patch(p, f.root_at + 12, f.stat.overflow_pages + 1);
    let cases = [
        ("header", header, format!("overflow run {first} ")),
        ("leaf", leaf, format!("page {last_leaf} fails its CRC")),
        ("counter", counter, "the leaves name 42 overflow pages, the handle counts 43".to_string()),
    ];
    for (name, damage, want) in cases {
        for drop in [false, true] {
            let path = dir.path().join(format!("{name}-{drop}"));
            fs::copy(&base, &path).expect("copy");
            damage(&path);
            let before = fs::read(&path).expect("read");
            let env = open(&path, SyncMode::Durable);
            let mut txn = env.begin_rw().expect("begin");
            let t = txn.open_table("t").expect("open").expect("t");
            let state = |txn: &mut RwTxn| {
                let a = &txn.writer_mut().alloc;
                (a.freed().len(), a.pool().len(), a.epoch())
            };
            let untouched = state(&mut txn);
            let stat = txn.table_stat(&t).expect("stat");
            let got = if drop { txn.drop_table(t) } else { txn.clear_table(&t) };
            match got {
                Err(Error::Corrupt(e)) => assert!(e.contains(&want), "{name} (drop {drop}): {e}"),
                other => panic!("{name} (drop {drop}): {other:?}"),
            }
            assert_eq!(state(&mut txn), untouched, "{name} (drop {drop}): nothing freed");
            assert!(env.failure().is_some(), "{name} (drop {drop}): the damage latches");
            std::mem::drop((txn, env));
            assert!(fs::read(&path).expect("read") == before, "{name} (drop {drop}): unchanged");
            assert_eq!(stat_of(&open(&path, SyncMode::Durable), "t"), stat, "{name}: reopened");
        }
    }
}

/// `Env::check` reads every run as a read does, and claims each of its pages: a run failing its
/// CRC (a flipped byte in a middle page), a run whose header is not its own (CRC re-stamped), a
/// run with a non-zero pad (CRC re-stamped), an overflow counter that is not the runs' total, and
/// a run page the free list lists too are each reported.
#[test]
fn check_reports_damaged_runs_and_double_claims() {
    let (dir, base) = temp();
    let f = file_with_runs(&base);
    let (first, n, len) = f.runs[4];
    assert_eq!(n, 5);
    let case = |name: &str, damage: &dyn Fn(&Path)| -> String {
        let path = dir.path().join(name);
        fs::copy(&base, &path).expect("copy");
        damage(&path);
        let env = open(&path, SyncMode::Durable);
        let err = env.check().expect_err(name).to_string();
        assert!(env.failure().is_some(), "{name}: a failed check latches");
        err
    };
    let crc = case("crc", &|p| patch_run(p, first, n, 2 * 4096 + 7, &[0xA5], false));
    assert!(crc.contains(&format!("overflow run {first} ")), "{crc}");
    assert!(crc.contains("fails its CRC"), "{crc}");

    let header = case("header", &|p| patch_run(p, first, n, 5, &[1], true));
    assert!(header.contains(&format!("overflow run {first} ")), "{header}");
    assert!(header.contains("the header is not this run's"), "{header}");

    let pad_at = crate::overflow::RUN_HEADER + len as usize + 3;
    assert!(pad_at < n as usize * 4096 - 4, "the run has a pad");
    let pad = case("pad", &|p| patch_run(p, first, n, pad_at, &[1], true));
    assert!(pad.contains(&format!("overflow run {first} has a non-zero pad")), "{pad}");

    let counts = f.stat.overflow_pages;
    let counter = case("counter", &|p| patch(p, f.root_at + 12, counts - 1));
    let want =
        format!("table \"t\": {counts} overflow pages, not the {} its root counts", counts - 1);
    assert!(counter.contains(&want), "{counter}");

    let middle = first + 2;
    let twice = case("twice", &|p: &Path| {
        let file = fs::OpenOptions::new().read(true).write(true).open(p).expect("open");
        let start = u64::from(f.free_leaf) * 4096;
        let mut page = vec![0; 4096];
        file.read_exact_at(&mut page, start).expect("read");
        let (_, v) = page::leaf_inline_entry(&page, 0).expect("record");
        let mut listed = decode_pages(v, u32::MAX).expect("pages");
        listed.push(middle);
        listed.sort_unstable();
        let fit = page::leaf_replace_value(&mut page, 0, &encode_pages(&listed)).expect("fits");
        assert!(fit == page::Fit::Done);
        crc::add_crc32_nonzero(&mut page);
        file.write_all_at(&page, start).expect("write");
    });
    let want = format!("leaked []; used twice [{middle} (table \"t\" and free list)]");
    assert!(twice.contains(&want), "{twice}");
}

/// Key `25 * k`'s value read from the file at `path` in a fresh environment, which is returned too
/// (to see whether the read latched).
fn get_run_key(path: &Path, k: u32) -> (Env, Result<Vec<u8>>) {
    let env = open(path, SyncMode::Durable);
    let ro = env.begin_ro().expect("ro");
    let t = ro.open_table("t").expect("open").expect("t");
    let got = ro.get(&t, &key(25 * k)).map(|v| v.expect("a hit").to_vec());
    drop(ro);
    (env, got)
}

/// A read skips the CRC of a 1-page run, as it skips a leaf's: a flipped byte inside the value is
/// returned as stored, and `Env::check`, which verifies every run's CRC, reports it, as it reports
/// a flipped byte inside a leaf.
#[test]
fn check_finds_a_flip_inside_a_one_page_run() {
    let (_dir, path) = temp();
    let f = file_with_runs(&path);
    let (first, n, len) = f.runs[0];
    assert_eq!(n, 1);
    patch_run(&path, first, n, crate::overflow::RUN_HEADER + 100, &[0xA5], false);
    let (env, got) = get_run_key(&path, 0);
    let got = got.expect("a 1-page run's read checks its header, not its CRC");
    assert_eq!(got.len(), len as usize);
    assert_ne!(got, value(0, 0, len as usize), "the flipped byte");
    let err = env.check().expect_err("check verifies the run's CRC").to_string();
    assert!(err.contains(&format!("overflow run {first} ")), "{err}");
    assert!(err.contains("fails its CRC"), "{err}");
}

/// A read of a 1-page run whose page now holds another page is `Corrupt`, by its header alone (the
/// CRC is valid): another 1-page run of the same commit and length (only the page number in its
/// header differs), and a leaf. The read latches.
#[test]
fn a_get_refuses_a_one_page_run_reused_as_another_page() {
    let (dir, base) = temp();
    let f = file_with_runs(&base);
    let ((first, n, len), (other, other_n, other_len)) = (f.runs[0], f.runs[6]);
    assert_eq!((n, other_n, len), (1, 1, other_len), "two 1-page runs of the same length");
    let leaf = f.free_leaf;
    for (name, from, want) in [
        ("another run", other, "the header is not this run's"),
        ("a leaf", leaf, "the header is not this run's"),
    ] {
        let path = dir.path().join(name);
        fs::copy(&base, &path).expect("copy");
        let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
        let mut page = vec![0; 4096];
        file.read_exact_at(&mut page, u64::from(from) * 4096).expect("read");
        file.write_all_at(&page, u64::from(first) * 4096).expect("write");
        let (env, got) = get_run_key(&path, 0);
        match got {
            Err(Error::Corrupt(e)) => {
                assert!(e.contains(&format!("overflow run {first} ")) && e.contains(want), "{e}")
            }
            other => panic!("{name}: {other:?}"),
        }
        assert!(env.failure().is_some(), "{name}: the damage latches");
    }
}

/// A read of a run of more than one page verifies its CRC: a flipped byte in a middle page, which
/// has no header of its own, is `Corrupt` at get, and latches.
#[test]
fn a_get_refuses_a_flip_in_a_middle_page_of_a_run() {
    let (_dir, path) = temp();
    let f = file_with_runs(&path);
    let (first, n, _) = f.runs[4];
    assert_eq!(n, 5);
    patch_run(&path, first, n, 2 * 4096 + 7, &[0xA5], false);
    let (env, got) = get_run_key(&path, 4);
    match got {
        Err(Error::Corrupt(e)) => {
            assert!(e.contains(&format!("overflow run {first} ")) && e.contains("fails its CRC"))
        }
        other => panic!("{other:?}"),
    }
    assert!(env.failure().is_some(), "the damage latches");
}

/// A file for run allocation's step 2 (`Writer::alloc_run`): table `t` holds keys `0..100`, each
/// in a one-page run, written in order (the runs are consecutive pages); one commit deleted every
/// odd key below 90 (single run pages), the next deleted keys 96, 97 and 98 (three consecutive run
/// pages). A reader held across both kept their records in the free list until a later commit,
/// after which the gate passes both. `record_pages` caps the pages one record lists
/// (`Writer::record_pages`) for every commit.
struct ScatteredRecords {
    env: Env,
    /// Each key's run, by key.
    runs: Vec<u32>,
}

fn scattered_records(path: &Path, record_pages: Option<usize>) -> ScatteredRecords {
    let env = open(path, SyncMode::Durable);
    env.with_writer(|w| w.record_pages = record_pages);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..100 {
        txn.put(&t, &key(i), &value(i, 0, 3000)).expect("put");
    }
    txn.commit().expect("commit");
    let snapshot = env.inner().current();
    let record = catalog::get(&*snapshot, &snapshot.meta.catalog, "t").expect("get").expect("t");
    let runs: Vec<u32> = (0..100)
        .map(|i| match crate::tree::read::find(&*snapshot, &record.root, &key(i)) {
            Ok(Some(page::Val::Overflow(ovf))) => ovf.pgno,
            other => panic!("key {i}: {other:?}"),
        })
        .collect();
    drop(snapshot);
    assert!(runs.windows(2).all(|w| w[1] == w[0] + 1), "{runs:?}");
    let held = env.begin_ro().expect("ro");
    for keys in [(1..90).step_by(2).collect::<Vec<u32>>(), vec![96, 97, 98]] {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table("t").expect("open").expect("t");
        for i in keys {
            assert!(txn.delete(&t, &key(i)).expect("delete"));
        }
        txn.commit().expect("commit");
    }
    churn(&env, "s", 1, 0, 8);
    drop(held);
    check(&env);
    ScatteredRecords { env, runs }
}

/// The records a writer loaded, by any allocation.
fn records_loaded(w: &crate::env::Writer) -> u64 {
    w.alloc.run_stats.records
}

/// A run with no fit in the pool loads more records that pass the gate before it grows the file:
/// the first record (the odd keys' single pages) holds no three consecutive pages, the next one
/// does, and the run takes them.
#[test]
fn a_run_takes_a_fit_from_records_loaded_for_it() {
    let (_dir, path) = temp();
    let f = scattered_records(&path, None);
    let mut txn = f.env.begin_rw().expect("begin");
    let w = txn.writer_mut();
    let (count, stats) = (w.alloc.page_count(), w.alloc.run_stats);
    let p = PageWrite::alloc_run(w, 3).expect("run");
    assert_eq!(p, f.runs[96], "the three pages the second record lists");
    assert_eq!(w.alloc.page_count(), count, "the file did not grow");
    assert_eq!(w.alloc.run_stats.steps[1], stats.steps[1] + 1, "{:?}", w.alloc.run_stats);
    assert_eq!(records_loaded(w), stats.records + 2, "the refill's record, then one more");
    txn.abort();
}

/// Run allocation's step 2 loads at most [`RUN_LOAD_RECORDS`] records, however many pages they
/// list, then the run comes from the end of the file.
///
/// [`RUN_LOAD_RECORDS`]: crate::alloc::RUN_LOAD_RECORDS
#[test]
fn a_run_loads_at_most_its_budget_of_records() {
    let budget = u64::from(crate::alloc::RUN_LOAD_RECORDS);
    for per in [1, 4] {
        let (_dir, path) = temp();
        let f = scattered_records(&path, Some(per));
        let mut txn = f.env.begin_rw().expect("begin");
        let w = txn.writer_mut();
        // The refill's record first, so the run's own loading is counted alone.
        let q = PageWrite::alloc(w).expect("page");
        PageWrite::free(w, q);
        let (count, before) = (w.alloc.page_count(), records_loaded(w));
        let p = PageWrite::alloc_run(w, 3).expect("run");
        assert_eq!(records_loaded(w) - before, budget, "{per} pages per record");
        assert!(w.alloc.loading(), "the gate did not stop the loading: the budget did");
        assert_eq!((p, w.alloc.page_count()), (count, count + 3), "from the end");
        txn.abort();
    }
}

/// An abort after a run allocation that loaded records leaves them in the free tree: the next
/// transaction loads the same records and takes the same run, and its commit accounts for every
/// page.
#[test]
fn an_abort_returns_the_records_a_run_loaded() {
    let (_dir, path) = temp();
    let f = scattered_records(&path, None);
    let free_root = f.env.inner().current().meta.free;
    let take = |commit: bool| -> (u32, Vec<u32>) {
        let before = logs(&f.env).0.len();
        let mut txn = f.env.begin_rw().expect("begin");
        let w = txn.writer_mut();
        let records = records_loaded(w);
        let p = PageWrite::alloc_run(w, 3).expect("run");
        assert_eq!(records_loaded(w), records + 2, "the refill's record, then one more");
        if commit {
            PageWrite::free_run(w, p, 3);
            txn.commit().expect("commit");
        } else {
            txn.abort();
        }
        (p, logs(&f.env).0[before..].iter().map(|&(_, p)| p).collect())
    };
    let aborted = take(false);
    assert_eq!(aborted.0, f.runs[96]);
    assert_eq!(f.env.inner().current().meta.free, free_root);
    assert_eq!(f.env.with_writer(|w| w.alloc.last_consumed()), None);
    assert_eq!(take(true), aborted, "the next transaction loads the same records");
    check(&f.env);
}

/// The bytes [`run_churn`] values are cut from: one random buffer, so a value costs a copy.
fn churn_bytes() -> Vec<u8> {
    let mut rng = Rng(0x0C4E_17B5);
    (0..64 * 1024).map(|_| rng.next() as u8).collect()
}

/// Key `i`'s value of generation `generation` and `len` bytes in [`run_churn`].
fn churn_value(bytes: &[u8], i: u32, generation: u32, len: usize) -> &[u8] {
    let at = (i as usize * 7919 + generation as usize * 104_729) % (bytes.len() - len);
    &bytes[at..at + len]
}

/// Run allocation under churn, the shape of history shards rewritten at each change: table `t`
/// holds `keys` values of 4 to 20 KB (overflow runs of 2 to 6 pages at 4 KiB), and each of
/// `commits` commits rewrites `rewrites` random keys with new random lengths. With `readers`, every
/// 40th commit a reader is taken and held across four commits, so it holds reuse back for two (a
/// page freed by txn `N` waits for txn `N + 2` anyway). The file must grow less than 5% over the
/// second half and never pass `max_ratio` times its live pages (the trees' pages, the free tree's
/// included) at a report; the save must settle within three rounds, and `Env::check` must pass.
/// Prints, every twentieth of the run, the page count, the live and free pages, the records loaded
/// per commit, the share of runs the pool served and the share of each allocation step.
fn run_churn(keys: u32, rewrites: u32, commits: u32, readers: bool, max_ratio: f64, seed: u64) {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let bytes = churn_bytes();
    let mut rng = Rng(seed);
    let mut model: Vec<(u32, usize)> = Vec::with_capacity(keys as usize);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..keys {
        let len = 4096 + rng.len_upto(16 * 1024);
        txn.put(&t, &key(i), churn_value(&bytes, i, 0, len)).expect("put");
        model.push((0, len));
    }
    txn.commit().expect("commit");
    let every = (commits / 20).max(1);
    let (mut held, mut mid) = (None::<(u32, RoTxn)>, None);
    let mut window = env.with_writer(|w| w.alloc.run_stats);
    eprintln!("commit pages live free ratio records/commit pool-hit steps(pool,loaded,tail,grow)");
    for c in 1..=commits {
        if held.as_ref().is_some_and(|&(at, _)| c >= at + 4) {
            held = None;
        }
        if readers && c % 40 == 0 {
            held = Some((c, env.begin_ro().expect("ro")));
        }
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table("t").expect("open").expect("t");
        for _ in 0..rewrites {
            let i = rng.below(u64::from(keys)) as u32;
            let len = 4096 + rng.len_upto(16 * 1024);
            txn.put(&t, &key(i), churn_value(&bytes, i, c, len)).expect("put");
            model[i as usize] = (c, len);
        }
        txn.commit().expect("commit");
        env.with_writer(|w| {
            w.alloc.load_log.clear();
            w.alloc.free_log.clear();
        });
        if c % every == 0 || c == commits {
            let r = check(&env);
            let live = r.catalog_pages + r.table_pages + r.free_tree_pages;
            let stats = env.with_writer(|w| w.alloc.run_stats);
            let steps: Vec<u64> = (0..4).map(|s| stats.steps[s] - window.steps[s]).collect();
            let runs = steps.iter().sum::<u64>().max(1) as f64;
            let records = (stats.records - window.records) as f64 / f64::from(every);
            let share: Vec<String> =
                steps.iter().map(|&s| format!("{:.1}%", s as f64 * 100.0 / runs)).collect();
            eprintln!(
                "{c} {} {live} {} {:.2} {records:.1} {:.1}% [{}]",
                r.page_count,
                r.free_pages,
                f64::from(r.page_count) / live as f64,
                (steps[0] + steps[1]) as f64 * 100.0 / runs,
                share.join(" "),
            );
            window = stats;
            assert!(
                f64::from(r.page_count) <= max_ratio * live as f64,
                "commit {c}: {} pages, {live} live",
                r.page_count
            );
            if c == commits / 2 / every * every {
                mid = Some(r.page_count);
            }
            if c == commits {
                let mid = mid.expect("a report at the half");
                let grown = f64::from(r.page_count - mid.min(r.page_count)) / f64::from(mid);
                assert!(grown < 0.05, "the file grew {:.1}% over the second half", grown * 100.0);
            }
        }
    }
    drop(held);
    let rounds = env.with_writer(|w| w.max_save_rounds);
    assert!(rounds <= 3, "the save took {rounds} rounds");
    let ro = env.begin_ro().expect("ro");
    let t = ro.open_table("t").expect("open").expect("t");
    for (i, &(generation, len)) in model.iter().enumerate() {
        let got = ro.get(&t, &key(i as u32)).expect("get").expect("a hit");
        assert!(got == churn_value(&bytes, i as u32, generation, len), "key {i}");
    }
}

/// 400 keys, 20 rewrites per commit, 400 commits, no reader
/// ([`run_churn_with_readers_keeps_the_file_bounded`] holds them).
#[test]
fn run_churn_keeps_the_file_bounded() {
    run_churn(400, 20, 400, false, 2.0, 0xC4E5_0001);
}

/// 300 keys, 30 rewrites per commit (about a tenth of the pages), 400 commits, with readers held,
/// at most 1.75 times the live pages (it stays near 1.5). A commit lists the pages it took from
/// records and left unused under those records' keys, so the next transaction reuses them. Listed
/// under the committing txn instead, they waited two commits more (longer under a held reader),
/// later commits took more records and left more over, and the file reached 1.9 times its live
/// pages here and 2.5 times over 20K commits.
#[test]
fn run_churn_with_readers_keeps_the_file_bounded() {
    run_churn(300, 30, 400, true, 1.75, 0xC4E5_0003);
}

/// [`run_churn_with_readers_keeps_the_file_bounded`] for 20K commits.
#[test]
#[ignore = "heavy: run with --release"]
fn run_churn_with_readers_keeps_the_file_bounded_heavy() {
    run_churn(300, 30, 20_000, true, 1.75, 0xC4E5_0004);
}

/// 2K keys, 100 rewrites per commit, 50K commits, at most 1.5 times the live pages (it stays near
/// 1.3; with the leftovers listed under the committing txn it passed 1.5). The length matters: a
/// step 2 that stopped after one record per allocation grew the file slowly enough to pass at 20K
/// commits, and without bound past that.
#[test]
#[ignore = "heavy: run with --release"]
fn run_churn_keeps_the_file_bounded_heavy() {
    run_churn(2000, 100, 50_000, true, 1.5, 0xC4E5_0002);
}

/// A merge frees the sibling it absorbs, which the commit's save must list like any freed page:
/// commits that delete most of a table's keys in scattered order merge its pages, with one-page
/// free records (`Writer::record_pages`) so each save writes and takes back many records. Every
/// save settles in at most three rounds, and `Env::check` passes after every commit. (The free
/// tree's own deletes run before the save's loop, which only puts, so they cannot feed it.)
#[test]
fn merges_feed_the_save_which_still_settles() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    env.with_writer(|w| w.record_pages = Some(1));
    let n = 3_000_u32;
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..n {
        txn.put(&t, &key(i * 7919 % n), &value(i, 0, 200)).expect("put");
    }
    txn.commit().expect("commit");
    env.with_writer(|w| (w.max_save_rounds, w.merging_deletes) = (0, 0));
    let mut rounds = 0;
    for chunk in 0..9 {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table("t").expect("open").expect("t");
        for i in (chunk * n / 10..(chunk + 1) * n / 10).map(|i| i * 4801 % n) {
            assert!(txn.delete(&t, &key(i)).expect("delete"), "key {i}");
        }
        txn.writer_mut().max_save_rounds = 0;
        txn.commit().expect("commit");
        rounds = rounds.max(env.with_writer(|w| w.max_save_rounds));
        check(&env);
    }
    let merges = env.with_writer(|w| w.merging_deletes);
    assert!(merges > 50, "{merges} deletes merged");
    assert!((1..=3).contains(&rounds), "a save took {rounds} rounds");
}
