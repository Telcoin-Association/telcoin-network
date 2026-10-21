//! Deletes that merge under-full pages, through the whole engine: `RwTxn::delete` and
//! `RwCursor::delete_current` workloads whose deletes absorb siblings at the leaf and branch
//! levels and make a root give way (`Writer::merging_deletes`, and the table's height and page
//! counters against a twin environment that does not merge, `Writer::no_merge`), committed,
//! reopened and checked against a model; the write-path cache (`write::PathCache`), which a
//! merge's free of an absorbed page must leave unused; and the crash matrix ([`matrix`]) on a
//! crashing commit whose deletes merge.

use std::ops::Bound::{Excluded, Unbounded};

use super::{
    crash::{assert_gets, base, matrix, Base},
    *,
};
use crate::{RwCursor, TableStat};

/// Key `i`, 48 bytes ordered as `i`: wide keys, so a branch holds few cells and a few thousand
/// entries make a tree of height 3.
fn key(i: u32) -> Vec<u8> {
    let mut k = i.to_be_bytes().to_vec();
    k.resize(48, b'k');
    k
}

/// The deletes whose merges absorbed a sibling so far (`Writer::merging_deletes`).
fn merging(txn: &mut RwTxn) -> u64 {
    txn.writer_mut().merging_deletes
}

/// Table `t`'s counters in `txn`'s working state.
fn stat(txn: &RwTxn, t: &Table) -> TableStat {
    txn.table_stat(t).expect("stat")
}

/// What a run of deletes did to a table's shape, delete by delete.
#[derive(Debug, Default)]
struct Shapes {
    /// Deletes whose merges absorbed a sibling.
    merging: u64,
    /// Merging deletes that took a branch page out at an unchanged height: a branch absorbed.
    branch: u64,
    /// Merging deletes after which the height fell with entries left: the root gave way.
    root: u64,
}

impl Shapes {
    /// Delete `k` (which must be there) from `t` and classify what the delete did.
    fn delete(&mut self, txn: &mut RwTxn, t: &Table, k: &[u8]) {
        let (before, merges) = (stat(txn, t), merging(txn));
        assert!(txn.delete(t, k).expect("delete"), "key {k:?}");
        let after = stat(txn, t);
        if merging(txn) == merges {
            return;
        }
        self.merging += 1;
        if after.height < before.height && after.entries > 0 {
            self.root += 1;
        } else if after.height == before.height && after.branch_pages < before.branch_pages {
            self.branch += 1;
        }
    }
}

/// A table of `n` entries put in scattered order, committed: wide keys ([`key`]) with 100-byte
/// values, a tree of height 3.
fn fill(env: &Env, n: u32) -> Rows {
    let mut rows = Rows::new();
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in (0..n).map(|i| i * 7919 % n) {
        let v = value(1, i, 100);
        txn.put(&t, &key(i), &v).expect("put");
        rows.insert(key(i), v);
    }
    assert_eq!(stat(&txn, &t).height, 3, "the table is three levels high");
    txn.commit().expect("commit");
    rows
}

/// `RwTxn::delete` of 95% of a three-level table's keys in scattered order, over five commits:
/// leaves absorb their siblings, so do branches, and the root gives way to its single child. A
/// twin environment that does not merge (`Writer::no_merge`), given the same deletes, keeps its
/// height and many more pages. The merged table reopens to pass `Env::check`, with every
/// remaining key reading back its value and every deleted key absent.
#[test]
fn deletes_that_merge_commit_and_reopen_to_the_model() {
    let n = 3_000_u32;
    let (dir, path) = temp("tndb_b_merge");
    let twin_path = dir.path().join("twin");
    let (env, twin) = (open(&path, SyncMode::Durable), open(&twin_path, SyncMode::Durable));
    twin.with_writer(|w| w.no_merge = true);
    let mut rows = fill(&env, n);
    assert_eq!(fill(&twin, n), rows);
    let order: Vec<u32> = (0..n).map(|i| i * 4801 % n).filter(|i| i % 20 != 0).collect();
    let (mut shapes, mut twin_shapes) = (Shapes::default(), Shapes::default());
    for chunk in order.chunks(order.len() / 5 + 1) {
        for (env, shapes) in [(&env, &mut shapes), (&twin, &mut twin_shapes)] {
            let mut txn = env.begin_rw().expect("begin");
            let t = txn.open_table("t").expect("open").expect("t");
            for &i in chunk {
                shapes.delete(&mut txn, &t, &key(i));
            }
            txn.commit().expect("commit");
            check(env);
        }
        for &i in chunk {
            rows.remove(&key(i)).expect("in the model");
        }
    }
    let table_stat = |env: &Env| {
        let ro = env.begin_ro().expect("ro");
        ro.table_stat(&ro.open_table("t").expect("open").expect("t")).expect("stat")
    };
    let (got, twin_got) = (table_stat(&env), table_stat(&twin));
    assert_eq!(twin_shapes.merging, 0, "{twin_shapes:?}");
    assert!(
        shapes.merging > 100 && shapes.branch > 0 && shapes.root > 0,
        "leaves and branches merge, and the root gives way: {shapes:?}"
    );
    assert!(
        got.height < twin_got.height && 2 * got.leaf_pages < twin_got.leaf_pages,
        "merged {got:?}, not merged {twin_got:?}"
    );
    assert_eq!(got.entries, rows.len() as u64);
    drop((env, twin));
    let env = open(&path, SyncMode::Durable);
    let state = State::from([("t".to_string(), rows)]);
    assert!(newest_state(&env) == state, "the reopened state differs");
    assert_gets(&env, &state, "reopened");
    let ro = env.begin_ro().expect("ro");
    let t = ro.open_table("t").expect("open").expect("t");
    for &i in &order {
        assert_eq!(ro.get(&t, &key(i)).expect("get"), None, "deleted key {i}");
    }
}

/// The entry after `k` in `rows`.
fn follower<'r>(rows: &'r Rows, k: &[u8]) -> Option<(&'r [u8], &'r [u8])> {
    rows.range::<[u8], _>((Excluded(k), Unbounded)).next().map(|(k, v)| (&k[..], &v[..]))
}

/// Sweep `cursor` from where it is: at each entry, `delete` says whether to delete it (and the
/// model's row) or step past it with `next`. After each `RwCursor::delete_current` the cursor is
/// on the deleted entry's follower, which `current` returns every other time and `next` the
/// others; every tenth time `prev` first returns the entry before (or nothing, from which `next`
/// returns the first entry: the follower). Stops once past `until`, or at the end; returns the
/// deletes.
fn sweep(
    cursor: &mut RwCursor<'_>,
    rows: &mut Rows,
    until: Option<&[u8]>,
    mut delete: impl FnMut(usize) -> bool,
) -> usize {
    let mut deleted = 0;
    let mut at = cursor.current().expect("current").map(|(k, _)| k.to_vec());
    for step in 0.. {
        let Some(k) = at.take() else { break };
        if until.is_some_and(|u| k.as_slice() > u) {
            break;
        }
        let want = follower(rows, &k).map(|(k, v)| (k.to_vec(), v.to_vec()));
        let mut on_follower = false;
        if delete(step) {
            assert!(cursor.delete_current().expect("delete"), "step {step}");
            rows.remove(&k).expect("in the model");
            deleted += 1;
            if deleted % 10 == 0 {
                let before = rows.range::<[u8], _>((Unbounded, Excluded(&k[..]))).next_back();
                let got = cursor.prev().expect("prev");
                assert_eq!(got, before.map(|(k, v)| (&k[..], &v[..])), "step {step}: prev");
            } else {
                on_follower = deleted % 2 == 0;
            }
        }
        let got = if on_follower { cursor.current() } else { cursor.next() };
        let got = got.expect("step").map(|(k, v)| (k.to_vec(), v.to_vec()));
        assert_eq!(got, want, "step {step}: the follower of {k:?}");
        at = got.map(|(k, _)| k);
    }
    deleted
}

/// `RwCursor::delete_current` sweeps whose deletes merge pages under the cursor, each delete
/// followed by `current` or `next` returning the deleted entry's follower (and at times `prev`
/// the entry before), against a model: a contiguous run of 400 keys across many leaves, then two
/// keys in three over the whole table (which keeps the height at 3); committed; then every entry
/// but the first and last three, which merges the branches until the root gives way down to a
/// single leaf. Each sweep merges, and the table reopens to the model and passes `Env::check`.
#[test]
fn delete_current_sweeps_return_each_follower_while_pages_merge() {
    let (_dir, path) = temp("tndb_b_merge_cursor");
    let env = open(&path, SyncMode::Durable);
    let mut rows = fill(&env, 3_000);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.open_table("t").expect("open").expect("t");
    let merges = merging(&mut txn);
    let mut cursor = txn.cursor_mut(&t).expect("cursor");
    cursor.seek(&key(1_000)).expect("seek");
    let n = sweep(&mut cursor, &mut rows, Some(&key(1_399)), |_| true);
    assert_eq!(n, 400);
    drop(cursor);
    let run = merging(&mut txn) - merges;
    let mut cursor = txn.cursor_mut(&t).expect("cursor");
    cursor.first().expect("first");
    sweep(&mut cursor, &mut rows, None, |step| step % 3 != 0);
    drop(cursor);
    let thin = merging(&mut txn) - merges - run;
    assert!(run > 0 && thin > 10, "{run} merges in the run, {thin} thinning");
    assert_eq!(stat(&txn, &t).height, 3);
    txn.commit().expect("commit");
    check(&env);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.open_table("t").expect("open").expect("t");
    let merges = merging(&mut txn);
    let mut cursor = txn.cursor_mut(&t).expect("cursor");
    let keep: Vec<Vec<u8>> = rows.keys().cloned().collect();
    let (third, last_kept) = (&keep[3], &keep[keep.len() - 4]);
    cursor.seek(third).expect("seek");
    let n = sweep(&mut cursor, &mut rows, Some(last_kept), |_| true);
    assert_eq!(n, keep.len() - 6);
    drop(cursor);
    let drained = merging(&mut txn) - merges;
    let s = stat(&txn, &t);
    assert!(drained > 0 && s.height == 1 && s.entries == 6, "{drained} merges: {s:?}");
    txn.commit().expect("commit");
    drop(env);
    let env = open(&path, SyncMode::Durable);
    assert!(newest_state(&env) == State::from([("t".to_string(), rows)]));
}

/// The puts done in a cached leaf on this thread so far (`write::CACHED_PUTS`). Only debug
/// builds count them; a release build reports 0, and the callers check the count only in debug.
fn cached_puts() -> u64 {
    #[cfg(debug_assertions)]
    return crate::tree::write::CACHED_PUTS.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    0
}

/// Put `k` in `t` and the model; returns whether the put went through the path cache (always
/// false in a release build, which does not count).
fn put_counted(txn: &mut RwTxn, t: &Table, rows: &mut Rows, k: &[u8], v: &[u8]) -> bool {
    let before = cached_puts();
    txn.put(t, k, v).expect("put");
    rows.insert(k.to_vec(), v.to_vec());
    assert_eq!(txn.get(t, k).expect("get"), Some(v), "key {k:?} reads back");
    cached_puts() > before
}

/// Delete keys `ks` from `t` and the model, in order, until a delete merges (which must happen);
/// returns how many deletes that took.
fn delete_until_merge(txn: &mut RwTxn, t: &Table, rows: &mut Rows, ks: &[u32]) -> usize {
    let merges = merging(txn);
    for (n, &i) in ks.iter().enumerate() {
        assert!(txn.delete(t, &key(i)).expect("delete"));
        rows.remove(&key(i)).expect("in the model");
        if merging(txn) > merges {
            return n + 1;
        }
    }
    panic!("no delete of {ks:?} merged");
}

/// A put after a merging delete in the same transaction never goes through a path cache naming a
/// page the merge freed, nor one it changed: a merge frees the absorbed sibling, which moves the
/// allocator's epoch. In one transaction, all of whose pages are private (so only the merge's free
/// moves the epoch), the cache names leaf `X`; deletes empty `X`'s right neighbour until it
/// absorbs `X` (freed, still holding its old bytes), and a put into `X`'s range then descends and
/// lands in the tree. Then the cache names the merged leaf, which absorbs its right neighbour (the
/// cached page survives, its range wider), and the next put descends too. After commit the table
/// is the model and passes `Env::check`.
#[test]
fn a_put_after_a_merge_never_uses_the_cache_of_a_freed_page() {
    let (_dir, path) = temp("tndb_b_merge_cache");
    let env = open(&path, SyncMode::Durable);
    let mut rows = Rows::new();
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    // Even keys appended: each append split starts a leaf at the key it appends (the left page
    // stays full), so `starts` holds every leaf's first key index.
    let mut starts = vec![0];
    for i in 0..240 {
        let leaves = stat(&txn, &t).leaf_pages;
        let v = value(1, i, 100);
        txn.append(&t, &key(2 * i), &v).expect("append");
        rows.insert(key(2 * i), v);
        if stat(&txn, &t).leaf_pages > leaves && i > 0 {
            starts.push(i);
        }
    }
    assert!(starts.len() >= 7, "{starts:?}");
    let leaf = |j: usize| starts[j]..starts[j + 1];
    // Thin `X` (leaf 2) and `Z` (leaf 4) to two fifths: still over a quarter, so they do not
    // merge (and their full neighbours would not fit), but each fits beside a quarter-full leaf.
    for j in [2, 4] {
        let keys = leaf(j);
        let keep = keys.len() * 2 / 5;
        for i in keys.start + keep as u32..keys.end {
            assert!(txn.delete(&t, &key(2 * i)).expect("delete"));
            rows.remove(&key(2 * i));
        }
    }
    assert_eq!(merging(&mut txn), 0, "the thinning merged nothing");
    // The cache names `X`: a descent fills it, the next put in `X`'s range uses it.
    let x = 2 * leaf(2).start;
    assert!(!put_counted(&mut txn, &t, &mut rows, &key(x + 1), &value(2, x + 1, 100)));
    let used = put_counted(&mut txn, &t, &mut rows, &key(x + 3), &value(2, x + 3, 100));
    assert_eq!(used, cfg!(debug_assertions), "the second put in X uses the cache");
    // `Y` (leaf 3), emptied from its top, falls under a quarter and absorbs `X` from the left.
    let leaves = stat(&txn, &t).leaf_pages;
    let y: Vec<u32> = leaf(3).rev().map(|i| 2 * i).collect();
    let n = delete_until_merge(&mut txn, &t, &mut rows, &y);
    assert!(n < y.len(), "the merge came before Y emptied");
    assert_eq!(stat(&txn, &t).leaf_pages, leaves - 1, "Y absorbed X");
    let used = put_counted(&mut txn, &t, &mut rows, &key(x + 5), &value(3, x + 5, 100));
    assert!(!used, "the put after the merge descended");
    // The merged leaf, cached by that put, is emptied until it absorbs `Z` (its left neighbour,
    // leaf 1, is full): the cached page survives the merge, and the put after it descends.
    let leaves = stat(&txn, &t).leaf_pages;
    let merged: Vec<u32> =
        rows.keys().map(|k| u32::from_be_bytes(k[..4].try_into().expect("4 bytes"))).collect();
    let merged: Vec<u32> = merged
        .into_iter()
        .filter(|&i| i >= x && i < 2 * leaf(4).start && i != x + 5)
        .rev()
        .collect();
    delete_until_merge(&mut txn, &t, &mut rows, &merged);
    assert_eq!(stat(&txn, &t).leaf_pages, leaves - 1, "the merged leaf absorbed Z");
    let used = put_counted(&mut txn, &t, &mut rows, &key(x + 7), &value(4, x + 7, 100));
    assert!(!used, "the put after the second merge descended");
    let z = 2 * leaf(4).start;
    put_counted(&mut txn, &t, &mut rows, &key(z + 1), &value(4, z + 1, 100));
    assert!(super::rows(&txn, &t) == rows, "the table is the model");
    txn.commit().expect("commit");
    assert!(newest_state(&env) == State::from([("t".to_string(), rows)]));
}

/// Key `i` of the crash matrix's base tables.
fn base_key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// Two small commits that `SyncMode::NoSync` cases leave unsynced before the crash commit: seven
/// in eight keys of a stretch of table `h` (values of 900 bytes) deleted, which merges its leaves.
fn merge_unsynced(txn: &mut RwTxn, state: &mut State, round: u32) {
    let merges = merging(txn);
    for i in (round * 96..(round + 1) * 96).filter(|i| i % 8 != 0) {
        delete(txn, state, "h", &base_key(i));
    }
    assert!(merging(txn) > merges, "round {round}: the deletes merged");
}

/// The crashing commit: seven in eight keys of table `a` deleted in scattered order (its leaves
/// absorb their siblings), every key of table `b` but its first and last (its last two leaves
/// merge and the root gives way), updates spread over `g`, and table `d` filled until the file
/// grows (a size change to sync).
fn merge_crash(txn: &mut RwTxn, state: &mut State) {
    let merges = merging(txn);
    for i in (0..400).map(|i| i * 263 % 400).filter(|i| i % 8 != 0) {
        delete(txn, state, "a", &base_key(i));
    }
    let b = txn.open_table("b").expect("open").expect("b");
    let height = stat(txn, &b).height;
    for i in 1..199 {
        delete(txn, state, "b", &base_key(i));
    }
    let after = stat(txn, &b);
    assert!(height > 1 && after.height == 1, "b's root gave way: {height} to {after:?}");
    assert!(merging(txn) - merges > 10, "{} deletes merged", merging(txn) - merges);
    for i in (0..50).step_by(3) {
        put(txn, state, "g", &base_key(i), &value(22, i, 150));
    }
    let start = txn.writer_mut().file.file_len();
    let mut i = 0;
    while txn.writer_mut().file.file_len() == start || i % 16 != 0 {
        put(txn, state, "d", &base_key(i), &value(24, i, 900));
        i += 1;
    }
}

/// The crash matrix's base with a crashing commit whose deletes merge.
fn merge_base(page_size: u32) -> Base {
    Base { unsynced: merge_unsynced, crash: merge_crash, grows: true, ..base(page_size) }
}

/// The crash matrix on a crashing commit whose deletes merge leaves and make a root give way
/// (after, in `SyncMode::NoSync`, two unsynced commits whose deletes merge): every crash point x
/// {process crash, strict power loss, one random power loss} x {`Durable`, `NoSync`} x {data
/// synced by `fdatasync`, by one `msync`} reopens to exactly the allowed state, which checks.
#[test]
fn every_crash_point_of_a_merging_commit_recovers_the_state_the_protocol_allows() {
    matrix(&merge_base(4096), 1);
}

/// The merging matrix at two page sizes, with many random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_of_a_merging_commit_recovers_the_state_the_protocol_allows_heavy() {
    for page_size in [4096, 16384] {
        matrix(&merge_base(page_size), 40);
    }
}
