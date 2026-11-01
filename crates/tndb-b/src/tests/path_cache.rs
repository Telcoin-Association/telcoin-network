//! The write-path cache (`write::PathCache`) against the descent: the same operations on two
//! environments, one with the cache and one without (`Writer::no_path_cache`, under which a
//! delete also leaves a detached cursor to find its place again by key), give the same results
//! and byte-identical files; and a cache never outlives the pages it names. Beside them, the
//! same pairing for a cursor's write hint (`write::Hint`): writes routed by the cursors' paths
//! against the same writes under `Writer::no_write_hint`.

use std::{collections::BTreeMap, fs, path::Path};

use proptest::prelude::*;

use super::{check, newest_state, temp, value};
use crate::{
    env::{Env, EnvOptions, SyncMode},
    tree::test_harness::{ReportSeed, Rng},
    ReadTxn, RoTxn, RwTxn, Table, TxnCursor,
};

/// The puts done in a cached leaf on this thread so far (`write::CACHED_PUTS`). Only debug
/// builds count them; a release build reports 0, and the callers check the count only in debug.
fn cached_puts() -> u64 {
    #[cfg(debug_assertions)]
    return crate::tree::write::CACHED_PUTS.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    0
}

/// The deletes done in a cached leaf on this thread so far (`write::CACHED_DELETES`), as
/// [`cached_puts`] counts.
fn cached_deletes() -> u64 {
    #[cfg(debug_assertions)]
    return crate::tree::write::CACHED_DELETES.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    0
}

/// The moves that descended from the root on this thread so far (`cursor::ROOT_DESCENTS`), as
/// [`cached_puts`] counts.
fn root_descents() -> u64 {
    #[cfg(debug_assertions)]
    return crate::tree::cursor::ROOT_DESCENTS.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    0
}

const TABLES: [&str; 3] = ["t0", "t1", "t2"];

/// Where a key comes from.
#[derive(Clone, Copy, Debug)]
enum Key {
    /// The table's next ascending key.
    Seq,
    /// A scattered key.
    Rand(u16),
    /// A key in one of four narrow clusters.
    Cluster(u8, u8),
    /// The table's `i`-th present key (modulo its length), or [`Key::Seq`] when it is empty.
    Present(u16),
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Put(usize, Key, u16),
    Delete(usize, Key),
    /// Put the table's next ascending key (its cache is then on the rightmost leaf), delete its
    /// `n` largest keys, largest first (emptying its rightmost leaves), and put the next
    /// ascending key (in the range of the leaves just emptied).
    Slide(usize, u8),
    /// Through a detached cursor: seek the key, then delete up to `n` entries as a walk does,
    /// each `delete_current` deleting the follower the last one left (read with `current` after
    /// every other delete, else deleted from the deleted state).
    CursorDelete(usize, Key, u8),
    /// A slot rewrite through a detached cursor: seek the key, delete the entry if the seek
    /// landed on it, then put the key with a new value.
    Slot(usize, Key, u16),
    Clear(usize),
    Drop(usize),
    Commit,
    Abort,
    Hold,
    Release,
}

fn key_strategy() -> impl Strategy<Value = Key> {
    prop_oneof![
        3 => Just(Key::Seq),
        2 => any::<u16>().prop_map(Key::Rand),
        2 => (0..4u8, any::<u8>()).prop_map(|(c, k)| Key::Cluster(c, k)),
        2 => any::<u16>().prop_map(Key::Present),
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // Most ops go to one table, so it grows several levels and its leaves empty.
    let table = prop_oneof![6 => Just(0), 1 => Just(1), 1 => Just(2)];
    let vlen = prop_oneof![3 => 0..200u16, 1 => 0..900u16];
    prop_oneof![
        24 => (table.clone(), key_strategy(), vlen.clone()).prop_map(|(t, k, v)| Op::Put(t, k, v)),
        6 => (table.clone(), key_strategy()).prop_map(|(t, k)| Op::Delete(t, k)),
        2 => (table.clone(), 1..40u8).prop_map(|(t, n)| Op::Slide(t, n)),
        3 => (table.clone(), key_strategy(), 1..12u8).prop_map(|(t, k, n)| Op::CursorDelete(t, k, n)),
        6 => (table.clone(), key_strategy(), vlen.clone()).prop_map(|(t, k, v)| Op::Slot(t, k, v)),
        1 => table.clone().prop_map(Op::Clear),
        1 => table.prop_map(Op::Drop),
        2 => Just(Op::Commit),
        1 => Just(Op::Abort),
        1 => Just(Op::Hold),
        1 => Just(Op::Release),
    ]
}

/// One environment of the pair, with its open transaction and held readers.
struct Side {
    env: Env,
    txn: Option<RwTxn>,
    readers: Vec<RoTxn>,
}

impl Side {
    fn open(path: &Path, cached: bool) -> Self {
        let options = EnvOptions { sync: SyncMode::NoSync, ..EnvOptions::default() };
        let env = Env::open(path, options).expect("open");
        env.with_writer(|w| w.no_path_cache = !cached);
        Self { env, txn: None, readers: Vec::new() }
    }

    fn txn(&mut self) -> &mut RwTxn {
        self.txn.get_or_insert_with(|| self.env.begin_rw().expect("begin"))
    }

    /// Run `op` (its keys already chosen) and describe the outcome.
    fn run(&mut self, op: Op, keys: &[Vec<u8>], val: &[u8]) -> String {
        match op {
            Op::Put(t, ..)
            | Op::Delete(t, _)
            | Op::Slide(t, _)
            | Op::CursorDelete(t, ..)
            | Op::Slot(t, ..)
            | Op::Clear(t)
            | Op::Drop(t) => {
                let txn = self.txn();
                let table = txn.create_table(TABLES[t]).expect("create");
                match op {
                    Op::Put(..) => format!("{:?}", txn.put(&table, &keys[0], val)),
                    Op::Delete(..) => format!("{:?}", txn.delete(&table, &keys[0])),
                    Op::Slide(..) => {
                        let first = txn.put(&table, &keys[0], val);
                        let (last, top) = keys[1..].split_last().expect("the last put's key");
                        let found: Vec<_> = top.iter().map(|k| txn.delete(&table, k)).collect();
                        format!("{first:?} {found:?} {:?}", txn.put(&table, last, val))
                    }
                    Op::CursorDelete(_, _, n) => {
                        let mut c = txn.cursor_detached(&table).expect("cursor");
                        let mut out = format!("{:?}", c.seek(txn, &keys[0]));
                        for i in 0..n {
                            let deleted = c.delete_current(txn);
                            out += &format!(" {deleted:?} {:?}", c.state());
                            if i % 2 == 0 {
                                out += &format!(" {:?}", c.current(txn));
                            }
                            if !matches!(deleted, Ok(true)) {
                                break;
                            }
                        }
                        out
                    }
                    Op::Slot(..) => {
                        let mut c = txn.cursor_detached(&table).expect("cursor");
                        let landed = c.seek(txn, &keys[0]).map(|e| e.map(|(k, _)| k.to_vec()));
                        let mut out = format!("{landed:?}");
                        if matches!(&landed, Ok(Some(k)) if *k == keys[0]) {
                            out += &format!(" {:?} {:?}", c.delete_current(txn), c.state());
                        }
                        let put = c.put(txn, &keys[0], val);
                        format!("{out} {put:?} {:?} {:?}", c.current(txn), c.next(txn))
                    }
                    Op::Clear(_) => format!("{:?}", txn.clear_table(&table)),
                    _ => format!("{:?}", txn.drop_table(table)),
                }
            }
            Op::Commit => format!("{:?}", self.txn.take().map(RwTxn::commit)),
            Op::Abort => {
                self.txn.take();
                String::new()
            }
            Op::Hold => {
                if self.readers.len() < 3 {
                    self.readers.push(self.env.begin_ro().expect("ro"));
                }
                String::new()
            }
            Op::Release => {
                if !self.readers.is_empty() {
                    self.readers.remove(0);
                }
                String::new()
            }
        }
    }
}

/// The keys of the open transaction's tables (the committed ones while none is open), for
/// [`Key::Present`] and [`Key::Seq`].
#[derive(Default)]
struct Model {
    committed: BTreeMap<usize, BTreeMap<Vec<u8>, ()>>,
    current: BTreeMap<usize, BTreeMap<Vec<u8>, ()>>,
    next_seq: [u32; TABLES.len()],
}

impl Model {
    fn key(&mut self, t: usize, key: Key) -> Vec<u8> {
        let present = self.current.entry(t).or_default();
        match key {
            Key::Present(i) if !present.is_empty() => {
                present.keys().nth(i as usize % present.len()).expect("in range").clone()
            }
            Key::Seq | Key::Present(_) => {
                self.next_seq[t] += 1;
                let mut k = b"s".to_vec();
                k.extend_from_slice(&self.next_seq[t].to_be_bytes());
                k
            }
            Key::Rand(r) => {
                let mut k = b"r".to_vec();
                k.extend_from_slice(&u32::from(r).wrapping_mul(0x9E37_79B9).to_be_bytes());
                k
            }
            Key::Cluster(c, k) => vec![b'c', c, k],
        }
    }

    /// The table's `n` largest keys, largest first.
    fn top(&mut self, t: usize, n: u8) -> Vec<Vec<u8>> {
        self.current.entry(t).or_default().keys().rev().take(n.into()).cloned().collect()
    }

    fn apply(&mut self, op: Op, keys: &[Vec<u8>]) {
        match op {
            Op::Put(t, ..) => {
                self.current.entry(t).or_default().insert(keys[0].clone(), ());
            }
            Op::Delete(t, _) => {
                self.current.entry(t).or_default().remove(&keys[0]);
            }
            Op::Slide(t, _) => {
                let (last, top) = keys[1..].split_last().expect("the last put's key");
                let rows = self.current.entry(t).or_default();
                for key in top {
                    rows.remove(key);
                }
                rows.insert(last.clone(), ());
            }
            Op::CursorDelete(t, _, n) => {
                let rows = self.current.entry(t).or_default();
                let gone: Vec<_> =
                    rows.range(keys[0].clone()..).take(n.into()).map(|(k, ())| k.clone()).collect();
                for key in gone {
                    rows.remove(&key);
                }
            }
            Op::Slot(t, ..) => {
                self.current.entry(t).or_default().insert(keys[0].clone(), ());
            }
            Op::Clear(t) | Op::Drop(t) => {
                self.current.remove(&t);
            }
            Op::Commit => self.committed.clone_from(&self.current),
            Op::Abort => self.current.clone_from(&self.committed),
            Op::Hold | Op::Release => {}
        }
    }
}

/// Run `ops` on a cached and an uncached environment: every outcome must match, and after every
/// commit the two files must be byte-identical. Returns the puts and the deletes done in a cached
/// leaf.
fn cached_matches_uncached(ops: &[Op]) -> (u64, u64) {
    let (_dir_a, path_a) = temp("tndb-b-cache-on");
    let (_dir_b, path_b) = temp("tndb-b-cache-off");
    let mut on = Side::open(&path_a, true);
    let mut off = Side::open(&path_b, false);
    let mut model = Model::default();
    let before = (cached_puts(), cached_deletes());
    for (i, &op) in ops.iter().enumerate() {
        let keys = match op {
            Op::Put(t, k, _) | Op::Delete(t, k) | Op::CursorDelete(t, k, _) | Op::Slot(t, k, _) => {
                vec![model.key(t, k)]
            }
            Op::Slide(t, n) => {
                let first = model.key(t, Key::Seq);
                model.current.entry(t).or_default().insert(first.clone(), ());
                let mut keys = vec![first];
                keys.extend(model.top(t, n));
                keys.push(model.key(t, Key::Seq));
                keys
            }
            _ => Vec::new(),
        };
        let val = match op {
            Op::Put(_, _, len) | Op::Slot(_, _, len) => {
                value(i as u32, keys[0].len() as u32, len as usize)
            }
            Op::Slide(..) => value(i as u32, 0, 100),
            _ => Vec::new(),
        };
        let (a, b) = (on.run(op, &keys, &val), off.run(op, &keys, &val));
        assert_eq!(a, b, "op {i} {op:?}: the cached and the uncached outcomes differ");
        model.apply(op, &keys);
        if matches!(op, Op::Commit) {
            let (fa, fb) = (fs::read(&path_a).expect("read"), fs::read(&path_b).expect("read"));
            assert!(fa == fb, "op {i}: the files differ after the commit");
        }
    }
    let hits = (cached_puts() - before.0, cached_deletes() - before.1);
    for side in [&mut on, &mut off] {
        side.readers.clear();
        if let Some(txn) = side.txn.take() {
            txn.commit().expect("commit");
        }
    }
    assert_eq!(newest_state(&on.env), newest_state(&off.env), "the final states differ");
    assert!(fs::read(&path_a).expect("read") == fs::read(&path_b).expect("read"), "final files");
    hits
}

fn cache_vs_descent(cases: u32, ops: usize) {
    let hits = std::cell::Cell::new((0, 0));
    let config = ProptestConfig { cases, failure_persistence: None, ..ProptestConfig::default() };
    proptest!(config, |(ops in prop::collection::vec(op_strategy(), 1..ops))| {
        let (puts, deletes) = cached_matches_uncached(&ops);
        hits.set((hits.get().0 + puts, hits.get().1 + deletes));
    });
    let (puts, deletes) = hits.get();
    assert!(puts > 0 || !cfg!(debug_assertions), "no put went through a cached leaf");
    assert!(deletes > 0 || !cfg!(debug_assertions), "no delete went through a cached leaf");
}

/// Random multi-table puts (ascending, scattered, clustered, replacing with other sizes),
/// deletes (direct, a detached cursor's walks and slot rewrites), clears, drops, commits and
/// aborts, with readers held: the cached environment answers as the uncached one (a cursor's
/// place after a delete included) and writes the same file bytes.
#[test]
fn the_path_cache_changes_nothing_but_speed() {
    cache_vs_descent(16, 800);
}

#[test]
#[ignore = "heavy: run with --release"]
fn the_path_cache_changes_nothing_but_speed_heavy() {
    cache_vs_descent(256, 1200);
}

fn seq_key(i: u32) -> Vec<u8> {
    let mut k = b"k".to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

/// Table T's cached leaf is emptied (unlinked, its page back in the pool) and the same page
/// becomes table U's root leaf in the same transaction; a put to T in the old leaf's range must
/// land in T, leaving U's page alone.
#[test]
fn a_cached_leaf_reused_by_another_table_is_not_written() {
    let (_dir, path) = temp("tndb-b-cache-reuse");
    let env = Env::open(&path, EnvOptions::default()).expect("open");
    let big = vec![7u8; 300];
    let n = 40; // 4 KiB pages: about a dozen 300-byte entries a leaf, so several leaves
                // Load without the cache: only the second transaction's puts below fill and use it.
    env.with_writer(|w| w.no_path_cache = true);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("create");
    let u = txn.create_table("u").expect("create");
    for i in 0..n {
        txn.put(&t, &seq_key(i), &big).expect("put");
    }
    txn.commit().expect("commit");
    env.with_writer(|w| w.no_path_cache = false);

    let mut txn = env.begin_rw().expect("begin");
    // Make T's path private; then a same-size replace of the last key leaves T's cache on the
    // rightmost leaf (a put in the cached leaf, or a descent that does not split and fills it).
    txn.put(&t, &seq_key(n), &big).expect("put");
    txn.put(&t, &seq_key(n + 1), &big).expect("put");
    txn.put(&t, &seq_key(n + 1), &big).expect("replace");
    // Empty that leaf from its last key down: deletes in it keep the epoch, until the one that
    // unlinks it frees the page.
    let epoch = |txn: &mut RwTxn| txn.writer_mut().alloc.epoch();
    let mut i = n + 1;
    loop {
        let before = epoch(&mut txn);
        assert!(txn.delete(&t, &seq_key(i)).expect("delete"));
        if epoch(&mut txn) != before {
            break;
        }
        i -= 1;
    }
    assert!(txn.table_stat(&t).expect("stat").height >= 2, "an unlink, not a collapse");
    // U's first put takes the freed leaf from the pool.
    txn.put(&u, b"u-key", b"u-value").expect("put u");
    // A key past T's last one lies in the unlinked leaf's old range.
    txn.put(&t, &seq_key(1000), b"t-value").expect("put t");
    assert_eq!(txn.get(&u, b"u-key").expect("get"), Some(&b"u-value"[..]));
    assert_eq!(txn.table_stat(&u).expect("stat").entries, 1);
    assert_eq!(txn.get(&t, &seq_key(1000)).expect("get"), Some(&b"t-value"[..]));
    txn.commit().expect("commit");
    let state = newest_state(&env);
    assert_eq!(state["u"].len(), 1, "U holds only its own entry: {:?}", state["u"].keys());
    assert_eq!(state["t"].len(), i as usize + 1, "T: keys 0..i and the new one");
    check(&env);
}

/// A transaction that filled a cache and aborted leaves nothing behind: the next transaction's
/// puts land as the uncached environment's do, byte for byte.
#[test]
fn an_aborted_transactions_cache_never_reaches_the_next() {
    let mut ops = Vec::new();
    for _ in 0..30 {
        ops.push(Op::Put(0, Key::Seq, 200));
    }
    ops.push(Op::Commit);
    for _ in 0..12 {
        ops.push(Op::Put(0, Key::Seq, 200));
        ops.push(Op::Put(1, Key::Seq, 10));
    }
    ops.push(Op::Abort);
    for i in 0..30 {
        ops.push(Op::Put(1, Key::Cluster(0, i), 10));
        ops.push(Op::Put(0, Key::Present(i.into()), 150));
        ops.push(Op::Put(0, Key::Seq, 200));
    }
    ops.push(Op::Commit);
    let (hits, _) = cached_matches_uncached(&ops);
    assert!(hits > 0 || !cfg!(debug_assertions), "the cache served some puts");
}

/// The slot rewrite over sorted keys, through one detached cursor: seek the key, delete it,
/// put it back with a new value. In each leaf only the first delete descends (it copies the
/// published leaf and refills the cache), or each one in a leaf under a quarter full; every other
/// one is done in the cached leaf, and no
/// `delete_current` descends from the root to find its place again: each lands on its follower
/// from the cached path.
#[test]
fn slot_rewrites_over_sorted_keys_delete_in_the_cached_leaf() {
    let (_dir, path) = temp("tndb-b-cache-slots");
    let env = Env::open(&path, EnvOptions::default()).expect("open");
    let n = 3000;
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("create");
    for i in 0..n {
        txn.put(&t, &seq_key(i), &value(0, i, 40)).expect("put");
    }
    txn.commit().expect("commit");

    let mut txn = env.begin_rw().expect("begin");
    let leaves = u64::from(txn.table_stat(&t).expect("stat").leaf_pages);
    let mut c = txn.cursor_detached(&t).expect("cursor");
    let deletes = cached_deletes();
    let mut descents = 0;
    for i in 0..n {
        let k = seq_key(i);
        let landed = c.seek(&txn, &k).expect("seek").map(|(k, _)| k.to_vec());
        assert_eq!(landed, Some(k.clone()), "key {i}");
        let before = root_descents();
        assert!(c.delete_current(&mut txn).expect("delete_current"), "key {i}");
        descents += root_descents() - before;
        let follower = c.current(&txn).expect("current").map(|(k, _)| k.to_vec());
        assert_eq!(follower, (i + 1 < n).then(|| seq_key(i + 1)), "the follower of key {i}");
        c.put(&mut txn, &k, &value(1, i, 40)).expect("put");
    }
    let cached = cached_deletes() - deletes;
    if cfg!(debug_assertions) {
        assert_eq!(descents, 0, "a delete_current found its place again from the root");
        // The rightmost leaf, which the ascending load left with what its last append split put
        // there, may hold under a quarter: each delete there is due a merge test and descends.
        let quarter = u64::from(n) / leaves / 4;
        assert!(cached + leaves + quarter >= u64::from(n), "{cached} cached, {leaves} leaves");
    }
    assert_eq!(txn.table_stat(&t).expect("stat").leaf_pages, leaves as u32, "same leaves");
    txn.commit().expect("commit");
    let state = newest_state(&env);
    assert_eq!(state["t"].len(), n as usize);
    for (i, (k, v)) in state["t"].iter().enumerate() {
        assert_eq!((k, v), (&seq_key(i as u32), &value(1, i as u32, 40)), "entry {i}");
    }
    check(&env);
}

/// The levels write descents routed by a cursor's hint, and the hints they refused, on this
/// thread so far (`write::HINTED_LEVELS`, `write::HINT_REFUSALS`), as [`cached_puts`] counts.
fn hint_routing() -> [u64; 2] {
    #[cfg(debug_assertions)]
    return [&crate::tree::write::HINTED_LEVELS, &crate::tree::write::HINT_REFUSALS]
        .map(|c| c.with(std::cell::Cell::get));
    #[cfg(not(debug_assertions))]
    [0, 0]
}

/// Key `i` of the hint pair's table: 100 bytes, so a few thousand entries reach three levels at
/// 4 KiB pages.
fn hint_key(i: u32) -> Vec<u8> {
    let mut k = i.to_be_bytes().to_vec();
    k.resize(100, i as u8);
    k
}

/// One step of a hint pair's script, on cursor `c` of the side's two or on the transaction.
#[derive(Clone, Copy, Debug)]
enum HintOp {
    Seek(usize, u32),
    SeekExact(usize, u32),
    /// A slot rewrite: put the key the cursor is at (`current`) with a new value.
    PutAt(usize, usize),
    /// A put through the cursor of any key: its hint holds while the key lies on its path.
    Put(usize, u32, usize),
    /// Up to `n` `delete_current`s as a walk does, each deleting the follower the last one left.
    DeleteRun(usize, u8),
    Next(usize, u8),
    /// The transaction's own put and delete, which use the path cache the cursors' writes left.
    TxnPut(u32, usize),
    TxnDelete(u32),
    Commit,
}

impl HintOp {
    fn pick(rng: &mut Rng, keys: u32) -> Self {
        let c = rng.below(2) as usize;
        let k = rng.below(u64::from(keys)) as u32;
        // Mostly small values; some that split a leaf, and a few in an overflow run.
        let len = match rng.below(20) {
            0 => 3000,
            1..=3 => 600 + rng.len_upto(600),
            _ => rng.len_upto(200),
        };
        match rng.below(100) {
            0..=14 => Self::Seek(c, k),
            15..=29 => Self::SeekExact(c, k),
            30..=44 => Self::PutAt(c, len),
            45..=54 => Self::Put(c, k, len),
            55..=69 => Self::DeleteRun(c, 1 + rng.below(30) as u8),
            70..=79 => Self::Next(c, 1 + rng.below(4) as u8),
            80..=91 => Self::TxnPut(k, len),
            92..=97 => Self::TxnDelete(k),
            _ => Self::Commit,
        }
    }
}

/// One environment of the hint pair: its open transaction, the table and two detached cursors.
struct HintSide {
    env: Env,
    /// Always open: a commit begins the next transaction.
    txn: Option<RwTxn>,
    table: Table,
    cursors: [TxnCursor; 2],
}

impl HintSide {
    /// A fresh environment at `path` with entries `0, 2, 4, ..` below `keys` committed; `hinted`
    /// false sets `Writer::no_write_hint`.
    fn open(path: &Path, hinted: bool, keys: u32) -> Self {
        let options = EnvOptions { sync: SyncMode::NoSync, ..EnvOptions::default() };
        let env = Env::open(path, options).expect("open");
        env.with_writer(|w| w.no_write_hint = !hinted);
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.create_table("t").expect("create");
        for i in (0..keys).step_by(2) {
            txn.put(&table, &hint_key(i), &value(0, i, 60)).expect("put");
        }
        txn.commit().expect("commit");
        let txn = env.begin_rw().expect("begin");
        let cursors = [(); 2].map(|()| txn.cursor_detached(&table).expect("cursor"));
        Self { env, txn: Some(txn), table, cursors }
    }

    /// Run `op` and describe its outcome, then where both cursors are.
    fn run(&mut self, op: HintOp, val: &[u8]) -> String {
        if matches!(op, HintOp::Commit) {
            let done = self.txn.take().map(RwTxn::commit);
            let txn = self.txn.insert(self.env.begin_rw().expect("begin"));
            self.cursors = [(); 2].map(|()| txn.cursor_detached(&self.table).expect("cursor"));
            return format!("{done:?}");
        }
        let Self { txn, table, cursors, .. } = self;
        let txn = txn.as_mut().expect("open");
        let out = match op {
            HintOp::Seek(c, k) => {
                format!(
                    "{:?}",
                    cursors[c].seek(txn, &hint_key(k)).map(|e| e.map(|(k, _)| k[..4].to_vec()))
                )
            }
            HintOp::SeekExact(c, k) => {
                format!(
                    "{:?}",
                    cursors[c].seek_exact(txn, &hint_key(k)).map(|v| v.map(<[u8]>::len))
                )
            }
            HintOp::PutAt(c, _) => {
                match cursors[c].current(txn).map(|e| e.map(|(k, _)| k.to_vec())) {
                    Ok(Some(key)) => format!("{:?}", cursors[c].put(txn, &key, val)),
                    other => format!("{other:?}"),
                }
            }
            HintOp::Put(c, k, _) => format!("{:?}", cursors[c].put(txn, &hint_key(k), val)),
            HintOp::DeleteRun(c, n) => {
                let mut out = String::new();
                for _ in 0..n {
                    let deleted = cursors[c].delete_current(txn);
                    out += &format!(" {deleted:?} {:?}", cursors[c].state());
                    if !matches!(deleted, Ok(true)) {
                        break;
                    }
                }
                out
            }
            HintOp::Next(c, n) => {
                let steps: Vec<_> = (0..n)
                    .map(|_| {
                        cursors[c].next(txn).map(|e| e.map(|(k, v)| (k[..4].to_vec(), v.len())))
                    })
                    .collect();
                format!("{steps:?}")
            }
            HintOp::TxnPut(k, _) => format!("{:?}", txn.put(table, &hint_key(k), val)),
            HintOp::TxnDelete(k) => format!("{:?}", txn.delete(table, &hint_key(k))),
            HintOp::Commit => unreachable!("done above"),
        };
        let at = cursors.iter().map(|c| c.position(txn)).collect::<Vec<_>>();
        format!("{out} at {at:?}")
    }
}

/// Seeded scripts of seeks, slot rewrites, puts of any key and runs of deletes through two
/// detached cursors, with the transaction's own puts and deletes, on an environment whose cursor
/// writes route by the cursors' paths and on one under `Writer::no_write_hint`: every outcome and
/// cursor position match, and after every commit the files are byte-identical. Returns the hinted
/// side's routed levels, refusals and merging deletes.
fn hinted_matches_unhinted(seed: u64, ops: usize, keys: u32) -> [u64; 3] {
    let _report = ReportSeed(seed);
    let mut rng = Rng(seed * 0x9E37_79B9 + 11);
    let (_dir_a, path_a) = temp("tndb-b-hint-on");
    let (_dir_b, path_b) = temp("tndb-b-hint-off");
    let mut on = HintSide::open(&path_a, true, keys);
    let mut off = HintSide::open(&path_b, false, keys);
    let merging =
        |side: &mut HintSide| side.txn.as_mut().expect("open").writer_mut().merging_deletes;
    let height = on.txn.as_ref().expect("open").table_stat(&on.table).expect("stat").height;
    assert!(height >= 3, "three levels: {height}");
    let merges = merging(&mut on);
    let mut routed = [0; 2];
    for i in 0..ops {
        let op = HintOp::pick(&mut rng, keys);
        let val = match op {
            HintOp::PutAt(_, len) | HintOp::Put(_, _, len) | HintOp::TxnPut(_, len) => {
                value(i as u32, seed as u32, len)
            }
            _ => Vec::new(),
        };
        let before = hint_routing();
        let a = on.run(op, &val);
        let after = hint_routing();
        let b = off.run(op, &val);
        assert_eq!(hint_routing(), after, "op {i} {op:?}: the unhinted side routed by a hint");
        routed = std::array::from_fn(|j| routed[j] + after[j] - before[j]);
        assert_eq!(a, b, "op {i} {op:?}: the hinted and the unhinted outcomes differ");
        if matches!(op, HintOp::Commit) {
            let (fa, fb) = (fs::read(&path_a).expect("read"), fs::read(&path_b).expect("read"));
            assert!(fa == fb, "op {i}: the files differ after the commit");
        }
    }
    let merges = merging(&mut on) - merges;
    for side in [on, off] {
        side.txn.expect("open").commit().expect("commit");
        check(&side.env);
    }
    assert!(fs::read(&path_a).expect("read") == fs::read(&path_b).expect("read"), "final files");
    [routed[0], routed[1], merges]
}

fn hint_vs_search(seeds: std::ops::Range<u64>, ops: usize) {
    let mut seen = [0; 3];
    for seed in seeds {
        let got = hinted_matches_unhinted(seed, ops, 3000);
        seen = std::array::from_fn(|j| seen[j] + got[j]);
    }
    let [hinted, refused, merges] = seen;
    assert!(merges > 0, "no delete merged");
    if cfg!(debug_assertions) {
        assert!(hinted > 0 && refused > 0, "{hinted} levels hinted, {refused} hints refused");
    }
}

/// A cursor's writes routed by its path ([`crate::tree::write::Hint`]) leave what the search
/// leaves: over three-level trees at 4 KiB pages with merging on, the slot rewrites, puts of any
/// key and delete runs of two detached cursors (splits, overflow runs, merges and unlinks among
/// them) give the same outcomes, cursor positions and file bytes as under `Writer::no_write_hint`.
#[test]
fn hinted_writes_leave_the_bytes_of_unhinted_ones() {
    hint_vs_search(1..4, 1500);
}

#[test]
#[ignore = "heavy: run with --release"]
fn hinted_writes_leave_the_bytes_of_unhinted_ones_heavy() {
    hint_vs_search(10..26, 6000);
}
