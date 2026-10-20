//! Free-list tests: the record codec, gated reuse (held readers, durability, `SyncMode::NoSync`),
//! bounded growth, reuse of dropped and cleared tables, the save's fixed point, exact page
//! accounting (`Env::check`) in a randomized workload, damaged records, and power loss.

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
    tree::test_harness::{ReportSeed, Rng},
    CheckReport, ReadTxn, RoTxn, Table,
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
    // The drop's pages pass the gate two commits on (a commit taking a record re-lists what it
    // leaves under its own txn, so the fill comes right then).
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
    let have = writer.alloc.reusable().expect("reusable").len();
    let pages: Vec<u32> = (have..want).map(|_| writer.alloc().expect("alloc")).collect();
    for p in pages {
        writer.free(p);
    }
    assert_eq!(writer.alloc.reusable().expect("reusable").len(), want);
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
    let max_value = env.max_value_len(4);
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
        let (k, v) = page::leaf_entry(leaf_page, i).expect("entry");
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
    let (_, v) = page::leaf_entry(leaf, 0).expect("record");
    let listed = decode_pages(v, meta.page_count).expect("pages");
    let value_at = u64::from(free.root) * 4096 + (v.as_ptr() as u64 - leaf.as_ptr() as u64);
    let record = catalog::get(&*snapshot, &meta.catalog, "t").expect("get").expect("t");
    let catalog_leaf = snapshot.page(meta.catalog.root).expect("catalog");
    let (_, rec) = page::leaf_entry(catalog_leaf, 0).expect("t's record");
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
