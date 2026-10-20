//! The write-path cache (`write::PathCache`) against the descent: the same operations on two
//! environments, one with the cache and one without (`Writer::no_path_cache`), give the same
//! results and byte-identical files; and a cache never outlives the pages it names.

use std::{collections::BTreeMap, fs, path::Path};

use proptest::prelude::*;

use super::{check, newest_state, temp, value};
use crate::{
    env::{Env, EnvOptions, SyncMode},
    ReadTxn, RoTxn, RwTxn,
};

/// The puts done in a cached leaf on this thread so far (`write::CACHED_PUTS`). Only debug
/// builds count them; a release build reports 0, and the callers check the count only in debug.
fn cached_puts() -> u64 {
    #[cfg(debug_assertions)]
    return crate::tree::write::CACHED_PUTS.with(std::cell::Cell::get);
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
        24 => (table.clone(), key_strategy(), vlen).prop_map(|(t, k, v)| Op::Put(t, k, v)),
        6 => (table.clone(), key_strategy()).prop_map(|(t, k)| Op::Delete(t, k)),
        2 => (table.clone(), 1..40u8).prop_map(|(t, n)| Op::Slide(t, n)),
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
            Op::Put(t, ..) | Op::Delete(t, _) | Op::Slide(t, _) | Op::Clear(t) | Op::Drop(t) => {
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
/// commit the two files must be byte-identical. Returns the puts done in a cached leaf.
fn cached_matches_uncached(ops: &[Op]) -> u64 {
    let (_dir_a, path_a) = temp("tndb-b-cache-on");
    let (_dir_b, path_b) = temp("tndb-b-cache-off");
    let mut on = Side::open(&path_a, true);
    let mut off = Side::open(&path_b, false);
    let mut model = Model::default();
    let before = cached_puts();
    for (i, &op) in ops.iter().enumerate() {
        let keys = match op {
            Op::Put(t, k, _) | Op::Delete(t, k) => vec![model.key(t, k)],
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
            Op::Put(_, _, len) => value(i as u32, keys[0].len() as u32, len as usize),
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
    let hits = cached_puts() - before;
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
    let hits = std::cell::Cell::new(0);
    let config = ProptestConfig { cases, failure_persistence: None, ..ProptestConfig::default() };
    proptest!(config, |(ops in prop::collection::vec(op_strategy(), 1..ops))| {
        hits.set(hits.get() + cached_matches_uncached(&ops));
    });
    assert!(hits.get() > 0 || !cfg!(debug_assertions), "no put went through a cached leaf");
}

/// Random multi-table puts (ascending, scattered, clustered, replacing with other sizes),
/// deletes, clears, drops, commits and aborts, with readers held: the cached environment answers
/// as the uncached one and writes the same file bytes.
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
    let hits = cached_matches_uncached(&ops);
    assert!(hits > 0 || !cfg!(debug_assertions), "the cache served some puts");
}
