//! Detached write cursors ([`TxnCursor`]) sharing one write transaction: several cursors over two
//! tables and the transaction's own writes, interleaved at random, against a model of each cursor
//! (its key, or after a delete its follower's and the deleted key, over the table's rows), its
//! `follower_shares_prefix` answers among them; a write to another table causing no reseek;
//! a dropped table, another transaction, a refused insert; and prefix steps that follow other
//! writers' changes.

use std::collections::BTreeMap;

use super::{check, open, rows, temp, Rows};
use crate::{
    env::SyncMode,
    tree::{
        cursor::State,
        test_harness::{ReportSeed, Rng},
    },
    Env, EnvOptions, Error, ReadTxn, RwTxn, Table, TxnCursor,
};

type Kv = (Vec<u8>, Vec<u8>);

fn owned(entry: Option<(&[u8], &[u8])>) -> Option<Kv> {
    entry.map(|(k, v)| (k.to_vec(), v.to_vec()))
}

/// Where a model cursor is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Pos {
    Unset,
    At(Vec<u8>),
    AfterEnd,
    BeforeStart,
    /// After the entry `key` was deleted: on its follower, the entry `on` (`None`: after the end),
    /// which `next` and `current` return; the prefix steps compare with `key`.
    Deleted {
        on: Option<Vec<u8>>,
        key: Vec<u8>,
    },
}

/// The first entry after `key`.
fn after(rows: &Rows, key: &[u8]) -> Option<Kv> {
    use std::ops::Bound::{Excluded, Unbounded};
    rows.range::<[u8], _>((Excluded(key), Unbounded)).next().map(|(k, v)| (k.clone(), v.clone()))
}

/// The last entry before `key` (`or_at`: or at it).
fn before(rows: &Rows, key: &[u8], or_at: bool) -> Option<Kv> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let hi = if or_at { Included(key) } else { Excluded(key) };
    rows.range::<[u8], _>((Unbounded, hi)).next_back().map(|(k, v)| (k.clone(), v.clone()))
}

/// A move's landing: at the entry, else `end`.
fn land(entry: Option<Kv>, end: Pos) -> (Pos, Option<Kv>) {
    match entry {
        Some(kv) => (Pos::At(kv.0.clone()), Some(kv)),
        None => (end, None),
    }
}

impl Pos {
    /// After a delete of `key`: on the first entry after it.
    fn deleted(rows: &Rows, key: Vec<u8>) -> Self {
        Pos::Deleted { on: after(rows, &key).map(|(k, _)| k), key }
    }

    /// The position as the cursor's next call finds it (it finds its entry again by key): at a
    /// key that is gone, it is after that deleted entry; on a follower that is gone, on the first
    /// entry after that (entries put between the deleted key and the follower stay behind it).
    fn resolved(self, rows: &Rows) -> Self {
        match self {
            Pos::At(k) if !rows.contains_key(&k) => Pos::deleted(rows, k),
            Pos::Deleted { on: Some(f), key } if !rows.contains_key(&f) => {
                Pos::Deleted { on: after(rows, &f).map(|(k, _)| k), key }
            }
            pos => pos,
        }
    }

    /// The cursor's `(state, deleted)` pair.
    fn state(&self) -> (State, bool) {
        match self {
            Pos::Unset => (State::Unset, false),
            Pos::At(_) => (State::At, false),
            Pos::AfterEnd => (State::AfterEnd, false),
            Pos::BeforeStart => (State::BeforeStart, false),
            Pos::Deleted { on: Some(_), .. } => (State::At, true),
            Pos::Deleted { on: None, .. } => (State::AfterEnd, true),
        }
    }

    /// The follower of a deleted entry: the entry the cursor is on.
    fn on(rows: &Rows, on: &Option<Vec<u8>>) -> Option<Kv> {
        on.as_ref().map(|f| (f.clone(), rows.get(f).expect("the follower is resolved").clone()))
    }

    fn first(rows: &Rows) -> (Self, Option<Kv>) {
        land(rows.first_key_value().map(|(k, v)| (k.clone(), v.clone())), Pos::AfterEnd)
    }

    fn last(rows: &Rows) -> (Self, Option<Kv>) {
        land(rows.last_key_value().map(|(k, v)| (k.clone(), v.clone())), Pos::BeforeStart)
    }

    fn next(self, rows: &Rows) -> (Self, Option<Kv>) {
        match self {
            Pos::Unset | Pos::BeforeStart => Self::first(rows),
            Pos::AfterEnd => (Pos::AfterEnd, None),
            Pos::At(k) => land(after(rows, &k), Pos::AfterEnd),
            Pos::Deleted { on, .. } => land(Self::on(rows, &on), Pos::AfterEnd),
        }
    }

    fn prev(self, rows: &Rows) -> (Self, Option<Kv>) {
        let last = || rows.last_key_value().map(|(k, v)| (k.clone(), v.clone()));
        match self {
            Pos::Unset => Self::last(rows),
            Pos::AfterEnd => land(last(), Pos::BeforeStart),
            Pos::BeforeStart => (Pos::BeforeStart, None),
            Pos::At(k) => land(before(rows, &k, false), Pos::BeforeStart),
            // Back from the follower: a key put since the delete, the deleted one included,
            // comes before the cursor.
            Pos::Deleted { on: Some(f), .. } => land(before(rows, &f, false), Pos::BeforeStart),
            Pos::Deleted { on: None, .. } => land(last(), Pos::BeforeStart),
        }
    }

    /// `current`: after a delete, the follower, which the cursor is then at.
    fn current(self, rows: &Rows) -> (Self, Option<Kv>) {
        match self {
            Pos::At(ref k) => {
                let entry = rows.get(k).map(|v| (k.clone(), v.clone()));
                (self, entry)
            }
            Pos::Deleted { on, .. } => land(Self::on(rows, &on), Pos::AfterEnd),
            pos => (pos, None),
        }
    }

    /// `next_in_prefix(n)` (`forward`) or `prev_in_prefix(n)`: a refusal keeps the position.
    /// After a delete the candidates are the follower and the entry before it, and the prefix is
    /// the deleted key's.
    fn step_in_prefix(self, rows: &Rows, n: usize, forward: bool) -> (Self, Option<Kv>) {
        let candidate = match &self {
            Pos::At(k) if n <= k.len() => {
                if forward {
                    after(rows, k)
                } else {
                    before(rows, k, false)
                }
            }
            Pos::Deleted { on, key } if n <= key.len() => match (forward, on) {
                (true, on) => Self::on(rows, on),
                (false, Some(f)) => before(rows, f, false),
                (false, None) => rows.last_key_value().map(|(k, v)| (k.clone(), v.clone())),
            },
            _ => None,
        };
        let prefix = match &self {
            Pos::At(k) | Pos::Deleted { key: k, .. } => k.get(..n).unwrap_or_default().to_vec(),
            _ => Vec::new(),
        };
        match candidate {
            Some(kv) if kv.0.starts_with(&prefix) => (Pos::At(kv.0.clone()), Some(kv)),
            _ => (self, None),
        }
    }
}

impl Pos {
    /// `follower_shares_prefix(n)`: after a delete, whether the cursor is on a follower whose key
    /// starts with the deleted key's first `n` bytes; `false` in every other position.
    fn follower_shares_prefix(&self, n: usize) -> bool {
        match self {
            Pos::Deleted { on: Some(f), key } => key.get(..n).is_some_and(|p| f.starts_with(p)),
            _ => false,
        }
    }
}

/// The prefix lengths the model test asks `follower_shares_prefix` about: none, a group of 65536
/// keys, of 256, one key, the whole key, past it.
const PREFIXES: [usize; 6] = [0, 6, 7, 8, 32, 33];

/// A key of the model test: `i` in front of 24 bytes, so 7 bytes group 256 keys and 8 name one.
fn key(i: u64) -> Vec<u8> {
    let mut k = vec![0xA5; 32];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k
}

/// The `i` of a model key.
fn key_index(k: &[u8]) -> u64 {
    u64::from_be_bytes(k[..8].try_into().expect("8 bytes"))
}

/// A value: mostly short, now and then one that takes an overflow run.
fn value(rng: &mut Rng, step: usize) -> Vec<u8> {
    let len = if rng.below(60) == 0 { 5000 + rng.len_upto(3000) } else { rng.len_upto(100) };
    vec![step as u8; len]
}

/// What the model test counts, so it can show it reached the cases it is for.
#[derive(Debug, Default)]
struct Coverage {
    /// Cursor calls that found their entry deleted by another writer.
    deleted_under: usize,
    /// Cursor calls after a delete that found the follower deleted by another writer.
    follower_gone: usize,
    /// `current` calls after a delete that returned the follower.
    current_after_delete: usize,
    /// `next` and `next_in_prefix` calls after `last` (or `prev` from an unset cursor) with
    /// entries added past the cursor's since, which they move to.
    past_last: usize,
    /// Deletes after a delete (of the follower).
    redeletes: usize,
    /// Prefix steps that moved.
    prefix_hits: usize,
    /// `follower_shares_prefix` answers after a delete with a follower: `true`, and `false`.
    shares: usize,
    not_shares: usize,
    /// Deletes whose follower was in the next leaf, and deletes of the table's last entry.
    next_leaf_followers: usize,
    end_deletes: usize,
    /// `follower_shares_prefix` calls in the deleted state that found the cursor's place again.
    stale_shares: usize,
    /// Refused cursor writes (`KeyExists`, `AppendOutOfOrder`).
    refused: usize,
    /// `clear_table` calls under the cursors.
    clears: usize,
    /// The most leaves a table had when it was cleared.
    max_leaves: u32,
}

/// One model cursor: its table (0 or 1) and position.
struct Model {
    table: usize,
    pos: Pos,
    /// The cursor's last call was `last`, or `prev` from an unset cursor, and landed on an entry
    /// (for [`Coverage::past_last`]).
    from_last: bool,
}

/// A key near where cursor `c` is (its key's `i`, give or take 2), else a random one.
fn near(rng: &mut Rng, models: &[Model], keys: u64) -> Vec<u8> {
    let c = &models[rng.below(models.len() as u64) as usize];
    match &c.pos {
        Pos::At(k) | Pos::Deleted { key: k, .. } if rng.below(4) != 0 => {
            key((key_index(k) + rng.below(5)).saturating_sub(2))
        }
        _ => key(rng.below(keys)),
    }
}

/// Run `steps` random calls on the cursors `cursors` (each over table `models[i].table` of
/// `tables`) and the transaction's own writes, checking every result and state against the
/// model. Returns the reseeks the cursors made.
#[allow(clippy::too_many_arguments)]
fn run_steps(
    rng: &mut Rng,
    rw: &mut RwTxn,
    tables: &[Table; 2],
    cursors: &mut [TxnCursor],
    models: &mut [Model],
    want: &mut [Rows; 2],
    steps: usize,
    keys: u64,
    cov: &mut Coverage,
) {
    for step in 0..steps {
        if step == steps * 2 / 3 {
            // Clear a table under the cursors (it refills in the next round).
            let t = rng.below(2) as usize;
            let leaves = rw.table_stat(&tables[t]).expect("stat").leaf_pages;
            cov.max_leaves = cov.max_leaves.max(leaves);
            rw.clear_table(&tables[t]).expect("clear");
            want[t].clear();
            cov.clears += 1;
        }
        let op = rng.below(23);
        if op >= 16 {
            // The transaction's own writes, often next to where a cursor is.
            let t = rng.below(2) as usize;
            match op {
                16..=18 => {
                    let k = near(rng, models, keys);
                    let v = value(rng, step);
                    rw.put(&tables[t], &k, &v).expect("put");
                    want[t].insert(k, v);
                }
                19..=21 => {
                    let k = near(rng, models, keys);
                    let found = rw.delete(&tables[t], &k).expect("delete");
                    assert_eq!(found, want[t].remove(&k).is_some(), "step {step}: delete");
                }
                _ => {
                    let k = key(rng.below(keys));
                    let v = value(rng, step);
                    rw.put(&tables[t], &k, &v).expect("put");
                    want[t].insert(k, v);
                }
            }
            continue;
        }
        let nk = near(rng, models, keys);
        let i = rng.below(cursors.len() as u64) as usize;
        let (c, m) = (&mut cursors[i], &mut models[i]);
        let rows = &want[m.table];
        let pos = std::mem::replace(&mut m.pos, Pos::Unset);
        let from_last = std::mem::take(&mut m.from_last);
        let to_last = op == 1 || op == 4 && pos == Pos::Unset;
        let refusable = matches!(op, 11 | 12);
        // A refused write does not bring the cursor up to date; every other call does.
        let pos = if refusable {
            pos
        } else {
            match &pos {
                Pos::At(k) if !rows.contains_key(k) => cov.deleted_under += 1,
                Pos::At(k) if from_last && matches!(op, 2 | 3 | 8) && after(rows, k).is_some() => {
                    cov.past_last += 1;
                }
                Pos::Deleted { on: Some(f), .. } if !rows.contains_key(f) => cov.follower_gone += 1,
                _ => {}
            }
            pos.resolved(rows)
        };
        let (got, (pos, expected)) = match op {
            0 => (owned(c.first(rw).expect("first")), Pos::first(rows)),
            1 => (owned(c.last(rw).expect("last")), Pos::last(rows)),
            2 | 3 => (owned(c.next(rw).expect("next")), pos.next(rows)),
            4 => (owned(c.prev(rw).expect("prev")), pos.prev(rows)),
            5 => {
                let got = owned(c.current(rw).expect("current"));
                cov.current_after_delete += usize::from(got.is_some() && pos.state().1);
                (got, pos.current(rows))
            }
            6 => {
                let k = nk;
                let landed = rows.range(k.clone()..).next().map(|(k, v)| (k.clone(), v.clone()));
                (owned(c.seek(rw, &k).expect("seek")), land(landed, Pos::AfterEnd))
            }
            7 => {
                let k = nk;
                let got =
                    c.seek_exact(rw, &k).expect("seek_exact").map(|v| (k.clone(), v.to_vec()));
                let expected = match rows.get(&k) {
                    Some(v) => (Pos::At(k.clone()), Some((k, v.clone()))),
                    None => (Pos::Unset, None),
                };
                (got, expected)
            }
            8 | 9 => {
                let n = [0, 6, 7, 7, 8, 32, 33][rng.below(7) as usize];
                let got = if op == 8 { c.next_in_prefix(rw, n) } else { c.prev_in_prefix(rw, n) };
                let got = owned(got.expect("prefix step"));
                cov.prefix_hits += usize::from(got.is_some());
                (got, pos.step_in_prefix(rows, n, op == 8))
            }
            10..=12 => {
                let t = m.table;
                let (k, v) = match op {
                    12 => {
                        // An append past the last key, or now and then one that is out of order.
                        let last = want[t].last_key_value().map_or(0, |(k, _)| key_index(k) + 1);
                        (
                            key((last + rng.below(3)).saturating_sub(rng.below(8) / 7)),
                            value(rng, step),
                        )
                    }
                    _ => (nk, value(rng, step)),
                };
                let result = match op {
                    10 => c.put(rw, &k, &v),
                    11 => c.insert(rw, &k, &v),
                    _ => c.append(rw, &k, &v),
                };
                let rows = &mut want[t];
                let refused = match op {
                    11 => rows.contains_key(&k),
                    12 => rows.last_key_value().is_some_and(|(last, _)| k <= *last),
                    _ => false,
                };
                if refused {
                    let err = result.expect_err("a refused write");
                    assert!(
                        matches!(err, Error::KeyExists | Error::AppendOutOfOrder),
                        "step {step}: {err:?}"
                    );
                    cov.refused += 1;
                    m.pos = pos;
                    continue;
                }
                result.expect("write");
                rows.insert(k.clone(), v.clone());
                (owned(c.current(rw).expect("current")), (Pos::At(k.clone()), Some((k, v))))
            }
            15 => {
                // In any position, stale included: the answer, and the cursor stays put.
                let n = PREFIXES[rng.below(PREFIXES.len() as u64) as usize];
                let reseeks = c.reseeks;
                let shares = c.follower_shares_prefix(rw, n).expect("follower_shares_prefix");
                assert_eq!(shares, pos.follower_shares_prefix(n), "step {step}: n {n}, {pos:?}");
                cov.stale_shares += usize::from(c.reseeks > reseeks && pos.state().1);
                (None, (pos, None))
            }
            _ => {
                let t = m.table;
                // Whether the entry under the cursor ends its leaf (its follower in the next one).
                let leaf = |c: &TxnCursor, rw: &RwTxn| c.position(rw).1.last().map(|&(p, _)| p);
                let next_leaf = match &pos {
                    Pos::At(k) | Pos::Deleted { on: Some(k), .. } => {
                        let mut probe = rw.cursor_detached(&tables[t]).expect("cursor");
                        probe.seek(rw, k).expect("seek");
                        let at = leaf(&probe, rw);
                        probe.next(rw).expect("next").is_some() && leaf(&probe, rw) != at
                    }
                    _ => false,
                };
                let deleted = c.delete_current(rw).expect("delete_current");
                let pos_was_deleted = pos.state().1;
                let pos = match pos {
                    Pos::At(k) | Pos::Deleted { on: Some(k), .. } => {
                        assert!(deleted, "step {step}: delete_current at an entry");
                        // Placed as a fresh cursor's seek of the deleted key places it.
                        let mut fresh = rw.cursor_detached(&tables[t]).expect("cursor");
                        fresh.seek(rw, &k).expect("seek");
                        assert_eq!(
                            c.position(rw),
                            fresh.position(rw),
                            "step {step}: delete_current lands as a seek of the deleted key"
                        );
                        cov.redeletes += usize::from(pos_was_deleted);
                        want[t].remove(&k).expect("the entry under the cursor");
                        let pos = Pos::deleted(&want[t], k);
                        // Whether the follower is the deleted key's, at every prefix length,
                        // the cursor staying on it.
                        for n in PREFIXES {
                            let shares =
                                c.follower_shares_prefix(rw, n).expect("follower_shares_prefix");
                            assert_eq!(shares, pos.follower_shares_prefix(n), "step {step}: n {n}");
                            if matches!(pos, Pos::Deleted { on: Some(_), .. }) {
                                cov.shares += usize::from(shares);
                                cov.not_shares += usize::from(!shares);
                            }
                        }
                        cov.next_leaf_followers += usize::from(next_leaf);
                        cov.end_deletes += usize::from(pos.state().0 == State::AfterEnd);
                        pos
                    }
                    pos => {
                        assert!(!deleted, "step {step}: delete_current at no entry");
                        pos
                    }
                };
                (None, (pos, None))
            }
        };
        assert_eq!(got, expected, "step {step} op {op} cursor {i}");
        assert_eq!(c.state(), pos.state(), "step {step} op {op} cursor {i}");
        m.from_last = to_last && got.is_some();
        m.pos = pos;
    }
}

/// Three detached cursors (two over one table, one over another) and the transaction's own puts,
/// deletes and clears, interleaved at random, match a model of each cursor in every result and
/// state: writes into the leaf a cursor sits in, before and after its slot, deletes of its entry
/// and its neighbours, overflow values, refused inserts and appends, and `clear_table` under the
/// cursors. `follower_shares_prefix` matches the model after each delete at every prefix length
/// (a follower in the next leaf and a delete of the table's last entry among them), and in any
/// position, stale included. Each round commits; a cursor of an earlier round is `WrongTxn` in a
/// later one; the file then reopens with exact accounting.
#[test]
fn txn_cursors_and_direct_writes_match_a_model() {
    model_test(&[(1, 4096, 700), (2, 16384, 2500), (3, 4096, 60)], 3, 2500);
}

#[test]
#[ignore = "heavy: run with --release"]
fn txn_cursors_and_direct_writes_match_a_model_heavy() {
    let seeds: Vec<_> =
        (10..22).map(|s| (s, if s % 2 == 0 { 4096 } else { 16384 }, 3000)).collect();
    model_test(&seeds, 4, 20_000);
}

fn model_test(seeds: &[(u64, u32, u64)], rounds: usize, steps: usize) {
    for &(seed, page_size, keys) in seeds {
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed * 0x9E37_79B9 + 7);
        let (_dir, path) = temp("tndb_b_txn_cursor");
        let options = EnvOptions { page_size, sync: SyncMode::NoSync, ..EnvOptions::default() };
        let env = Env::open(&path, options).expect("open");
        let mut want: [Rows; 2] = [BTreeMap::new(), BTreeMap::new()];
        let mut cov = Coverage::default();
        let mut reseeks = 0;
        let mut old: Option<TxnCursor> = None;
        for round in 0..rounds {
            let mut rw = env.begin_rw().expect("begin");
            if let Some(mut c) = old.take() {
                assert!(
                    matches!(c.first(&rw), Err(Error::WrongTxn)),
                    "a cursor of an earlier round"
                );
            }
            let tables = [rw.create_table("a").expect("a"), rw.create_table("b").expect("b")];
            // A third of the keys, so the tables span several leaves (refilling a cleared one).
            for (t, table) in tables.iter().enumerate() {
                for i in (round as u64 % 3..keys).step_by(3) {
                    rw.put(table, &key(i), &[i as u8; 40]).expect("put");
                    want[t].insert(key(i), vec![i as u8; 40]);
                }
            }
            let mut models: Vec<Model> = [0, 0, 1]
                .into_iter()
                .map(|table| Model { table, pos: Pos::Unset, from_last: false })
                .collect();
            let mut cursors: Vec<TxnCursor> = models
                .iter()
                .map(|m| rw.cursor_detached(&tables[m.table]).expect("cursor"))
                .collect();
            run_steps(
                &mut rng,
                &mut rw,
                &tables,
                &mut cursors,
                &mut models,
                &mut want,
                steps,
                keys,
                &mut cov,
            );
            for (t, table) in tables.iter().enumerate() {
                assert_eq!(rows(&rw, table), want[t], "seed {seed} round {round}: table {t}");
            }
            reseeks += cursors.iter().map(|c| c.reseeks).sum::<u32>();
            old = cursors.pop();
            rw.commit().expect("commit");
            check(&env);
        }
        assert!(env.failure().is_none(), "nothing latched");
        drop(env);
        let env = open(&path, SyncMode::NoSync);
        check(&env);
        let ro = env.begin_ro().expect("ro");
        for (t, name) in ["a", "b"].into_iter().enumerate() {
            let table = ro.open_table(name).expect("open").expect("the table");
            assert_eq!(rows(&ro, &table), want[t], "seed {seed}: table {name} after reopen");
        }
        // The cases the test is for were reached.
        assert!(cov.deleted_under > 20, "seed {seed}: {cov:?}");
        assert!(cov.follower_gone > 5, "seed {seed}: {cov:?}");
        assert!(cov.current_after_delete > 10 && cov.redeletes > 10, "seed {seed}: {cov:?}");
        assert!(cov.past_last > 3, "seed {seed}: {cov:?}");
        assert!(cov.prefix_hits > 50 && cov.refused > 20 && cov.clears == rounds, "{cov:?}");
        assert!(cov.max_leaves > 4 || keys < 100, "seed {seed}: the tables grew: {cov:?}");
        assert!(cov.shares > 100 && cov.not_shares > 100, "seed {seed}: {cov:?}");
        assert!(cov.next_leaf_followers > 0 && cov.end_deletes > 10, "seed {seed}: {cov:?}");
        assert!(cov.stale_shares > 5, "seed {seed}: {cov:?}");
        assert!(reseeks as usize > steps / 4, "seed {seed}: {reseeks} reseeks");
    }
}

/// `n` entries `key(0..n)` in table `name`, committed.
fn filled(env: &Env, names: &[&str], n: u64) -> Vec<Table> {
    let mut rw = env.begin_rw().expect("begin");
    let tables: Vec<Table> =
        names.iter().map(|name| rw.create_table(name).expect("table")).collect();
    for table in &tables {
        for i in 0..n {
            rw.put(table, &key(i), &[i as u8; 40]).expect("put");
        }
    }
    rw.commit().expect("commit");
    tables
}

fn stale<T>(result: Result<T, Error>) -> bool {
    matches!(result, Err(Error::StaleTable(_)))
}

fn wrong<T>(result: Result<T, Error>) -> bool {
    matches!(result, Err(Error::WrongTxn))
}

fn key_of(entry: Option<(&[u8], &[u8])>) -> u64 {
    key_index(entry.expect("an entry").0)
}

/// A cursor's table changing makes it find its place again, once; another table's writes (puts,
/// deletes, a clear, another cursor's writes), a table created, and the cursor's own writes, make
/// it find nothing again.
#[test]
fn a_write_to_another_table_causes_no_reseek() {
    let (_dir, path) = temp("tndb_b_txn_cursor_other");
    let env = open(&path, SyncMode::NoSync);
    let [a, b] = filled(&env, &["a", "b"], 300).try_into().expect("two tables");
    let mut rw = env.begin_rw().expect("begin");
    let mut ca = rw.cursor_detached(&a).expect("cursor");
    let mut cb = rw.cursor_detached(&b).expect("cursor");
    assert_eq!(key_of(ca.seek(&rw, &key(100)).expect("seek")), 100);
    // Table b's changes, every kind.
    rw.put(&b, &key(1000), b"v").expect("put");
    assert!(rw.delete(&b, &key(5)).expect("delete"));
    assert_eq!(key_of(cb.seek(&rw, &key(50)).expect("seek")), 50);
    cb.put(&mut rw, &key(51), b"w").expect("cursor put");
    assert!(cb.delete_current(&mut rw).expect("delete_current"));
    rw.clear_table(&b).expect("clear");
    rw.create_table("c").expect("create");
    assert_eq!(key_of(ca.next(&rw).expect("next")), 101);
    assert_eq!(ca.reseeks, 0, "another table's writes cause no reseek");
    // The cursor's own writes cause none either.
    ca.put(&mut rw, &key(150), b"x").expect("cursor put");
    assert_eq!(key_of(ca.next(&rw).expect("next")), 151);
    assert!(ca.delete_current(&mut rw).expect("delete_current"));
    assert_eq!(key_of(ca.next(&rw).expect("next")), 152);
    assert_eq!(ca.reseeks, 0, "the cursor's own writes cause no reseek");
    // A write to its table does, once.
    rw.put(&a, &key(1), b"y").expect("put");
    assert_eq!(key_of(ca.next(&rw).expect("next")), 153);
    assert_eq!(key_of(ca.next(&rw).expect("next")), 154);
    assert_eq!(ca.reseeks, 1, "a write to the cursor's table: one reseek");
    rw.commit().expect("commit");
}

/// Dropping a cursor's table makes every later call of the cursor `StaleTable`, with no latch:
/// for a table this transaction had not touched (so it has no working record to lose) and for
/// one it had written; a table re-created under the name is another table.
#[test]
fn drop_table_under_a_live_cursor_is_stale_table() {
    let (_dir, path) = temp("tndb_b_txn_cursor_drop");
    let env = open(&path, SyncMode::NoSync);
    let [a, b] = filled(&env, &["a", "b"], 300).try_into().expect("two tables");
    let mut rw = env.begin_rw().expect("begin");
    let mut ca = rw.cursor_detached(&a).expect("cursor");
    let mut cb = rw.cursor_detached(&b).expect("cursor");
    assert_eq!(key_of(ca.first(&rw).expect("first")), 0);
    cb.put(&mut rw, &key(7), b"v").expect("put");
    rw.drop_table(a.clone()).expect("drop");
    rw.drop_table(b.clone()).expect("drop");
    for c in [&mut ca, &mut cb] {
        assert!(stale(c.next(&rw)), "next after the drop");
        assert!(stale(c.current(&rw)), "current after the drop");
        assert!(stale(c.seek(&rw, &key(3))), "seek after the drop");
        assert!(stale(c.put(&mut rw, &key(3), b"v")), "put after the drop");
        assert!(stale(c.delete_current(&mut rw)), "delete_current after the drop");
    }
    let a2 = rw.create_table("a").expect("re-create");
    rw.put(&a2, &key(1), b"new").expect("put");
    assert!(stale(ca.first(&rw)), "the re-created table is another table");
    let mut c2 = rw.cursor_detached(&a2).expect("cursor");
    assert_eq!(key_of(c2.first(&rw).expect("first")), 1);
    assert!(env.failure().is_none(), "StaleTable does not latch");
    rw.commit().expect("commit");
    check(&env);
}

/// A cursor used with any transaction but its own is `WrongTxn`, changing nothing and with no
/// latch: a later transaction, one that reuses the txn id of the aborted one the cursor came
/// from, and a transaction of another environment.
#[test]
fn a_cursor_used_with_another_transaction_is_wrong_txn() {
    let (_dir, path) = temp("tndb_b_txn_cursor_wrong");
    let env = open(&path, SyncMode::NoSync);
    let [a] = filled(&env, &["a"], 50).try_into().expect("one table");

    let mut rw = env.begin_rw().expect("begin");
    let mut c = rw.cursor_detached(&a).expect("cursor");
    assert_eq!(key_of(c.seek(&rw, &key(10)).expect("seek")), 10);
    rw.put(&a, &key(60), b"v").expect("put");
    let id = rw.commit().expect("commit");
    let mut rw = env.begin_rw().expect("begin");
    assert_eq!(rw.txn_id(), id + 1);
    assert!(wrong(c.next(&rw)), "a later transaction");
    assert!(wrong(c.current(&rw)));
    assert!(wrong(c.put(&mut rw, &key(99), b"v")));
    assert!(wrong(c.delete_current(&mut rw)));
    assert_eq!(rw.get(&a, &key(99)).expect("get"), None, "the refused put changed nothing");
    assert!(rw.get(&a, &key(10)).expect("get").is_some(), "the refused delete changed nothing");
    let id = rw.txn_id();
    let mut aborted = rw.cursor_detached(&a).expect("cursor");
    assert_eq!(key_of(aborted.first(&rw).expect("first")), 0);
    rw.abort();
    let mut rw = env.begin_rw().expect("begin");
    assert_eq!(rw.txn_id(), id, "the next transaction reuses the aborted one's id");
    assert!(wrong(aborted.next(&rw)), "the successor of the cursor's aborted transaction");
    assert!(wrong(aborted.put(&mut rw, &key(98), b"v")));

    let (_dir2, path2) = temp("tndb_b_txn_cursor_wrong2");
    let other = open(&path2, SyncMode::NoSync);
    let [a2] = filled(&other, &["a"], 5).try_into().expect("one table");
    let other_rw = other.begin_rw().expect("begin");
    let mut foreign = other_rw.cursor_detached(&a2).expect("cursor");
    assert!(wrong(foreign.first(&rw)), "another environment's transaction");

    assert!(env.failure().is_none() && other.failure().is_none(), "WrongTxn does not latch");
    let mut own = rw.cursor_detached(&a).expect("cursor");
    own.put(&mut rw, &key(97), b"v").expect("put");
    rw.commit().expect("commit");
    check(&env);
}

/// An insert of a present key through a cursor is `KeyExists`: the table is unchanged, nothing
/// latches, and the cursor stays where it was, its saved key included (a later reseek finds the
/// same entry; a cursor on a deleted entry's follower stays on it, in the deleted state). An
/// insert of a new key moves the cursor to it.
#[test]
fn a_refused_insert_through_a_txn_cursor_changes_nothing() {
    let (_dir, path) = temp("tndb_b_txn_cursor_insert");
    let env = open(&path, SyncMode::NoSync);
    let [a] = filled(&env, &["a"], 300).try_into().expect("one table");
    let mut rw = env.begin_rw().expect("begin");
    let before = rows(&rw, &a);
    let mut c = rw.cursor_detached(&a).expect("cursor");
    assert!(c.seek_exact(&rw, &key(5)).expect("seek").is_some());
    assert!(matches!(c.insert(&mut rw, &key(7), b"new"), Err(Error::KeyExists)));
    assert!(env.failure().is_none(), "KeyExists does not latch");
    assert_eq!(rows(&rw, &a), before, "the table is unchanged");
    assert_eq!(key_of(c.current(&rw).expect("current")), 5, "the cursor stayed");
    rw.put(&a, &key(1000), b"elsewhere").expect("put");
    assert_eq!(key_of(c.current(&rw).expect("current")), 5, "a reseek finds the same entry");
    assert_eq!(c.reseeks, 1);

    assert!(c.delete_current(&mut rw).expect("delete"));
    assert!(matches!(c.insert(&mut rw, &key(8), b"new"), Err(Error::KeyExists)));
    assert_eq!(c.state(), (State::At, true), "still on the deleted entry's follower");
    rw.put(&a, &key(1001), b"elsewhere").expect("put");
    assert_eq!(key_of(c.next(&rw).expect("next")), 6, "a reseek finds the follower again");
    assert_eq!(c.reseeks, 2);

    c.insert(&mut rw, &key(5), b"back").expect("insert");
    assert_eq!(c.current(&rw).expect("current"), Some((key(5).as_slice(), b"back".as_slice())));
    assert_eq!(key_of(c.next(&rw).expect("next")), 6);
    rw.commit().expect("commit");
    check(&env);
}

/// After `last`, or `prev` from an unset cursor, the cursor is on the table's last entry as after
/// any move: `next` and `next_in_prefix` move to an entry another cursor or the transaction has put
/// past it since, and with none return nothing, the cursor then after the end (`current` finds
/// nothing, `prev` returns the last entry). Another writer's delete of the entry leaves the cursor
/// after the deleted entry, as any cursor whose entry another writer deleted.
#[test]
fn next_after_last_finds_entries_put_past_it() {
    let (_dir, path) = temp("tndb_b_txn_cursor_last");
    let env = open(&path, SyncMode::NoSync);
    let [a] = filled(&env, &["a"], 10).try_into().expect("one table");
    let mut rw = env.begin_rw().expect("begin");
    let mut c = rw.cursor_detached(&a).expect("cursor");
    let mut other = rw.cursor_detached(&a).expect("cursor");
    assert_eq!(key_of(c.last(&rw).expect("last")), 9);
    other.append(&mut rw, &key(10), b"v").expect("append");
    assert_eq!(key_of(c.next(&rw).expect("next")), 10, "another cursor's append");
    assert_eq!(key_of(c.last(&rw).expect("last")), 10);
    rw.put(&a, &key(11), b"v").expect("put");
    assert_eq!(key_of(c.next_in_prefix(&rw, 0).expect("step")), 11, "the transaction's own put");

    // With no entry past it, `next` goes past the end.
    assert_eq!(key_of(c.last(&rw).expect("last")), 11);
    assert_eq!(c.next(&rw).expect("next"), None);
    assert_eq!(c.state(), (State::AfterEnd, false));
    assert_eq!(c.current(&rw).expect("current"), None);
    assert_eq!(key_of(c.prev(&rw).expect("prev")), 11, "prev from after the end");

    // Another writer's delete of the entry: the cursor is after a deleted last entry.
    assert_eq!(key_of(c.last(&rw).expect("last")), 11);
    assert!(rw.delete(&a, &key(11)).expect("delete"));
    assert_eq!(c.next(&rw).expect("next"), None);
    assert_eq!(c.state(), (State::AfterEnd, false));

    // `prev` from an unset cursor takes the last entry as `last` does.
    let mut fresh = rw.cursor_detached(&a).expect("cursor");
    assert_eq!(key_of(fresh.prev(&rw).expect("prev")), 10);
    other.append(&mut rw, &key(12), b"v").expect("append");
    assert_eq!(key_of(fresh.next(&rw).expect("next")), 12, "prev from an unset cursor");
    assert!(env.failure().is_none());
    rw.commit().expect("commit");
    check(&env);
}

/// A `delete_current` that is the table's first write since the commit copies the published root,
/// and the cursor, placed from the path cache (its entry not the last of its leaf), reads the new
/// tree: right after the delete, before any step, its `position` (the root included) equals a new
/// cursor's seek of the deleted key.
#[test]
fn a_txn_cursor_delete_that_copies_the_root_places_the_cursor_in_the_new_tree() {
    let (_dir, path) = temp("tndb_b_txn_cursor_delete_root");
    let env = open(&path, SyncMode::NoSync);
    let [a] = filled(&env, &["a"], 300).try_into().expect("one table");
    let mut rw = env.begin_rw().expect("begin");
    let mut c = rw.cursor_detached(&a).expect("cursor");
    let mut follower = rw.cursor_detached(&a).expect("cursor");
    let leaf = |c: &TxnCursor, rw: &RwTxn| c.position(rw).1.last().map(|&(page, _)| page);
    // A key whose follower shares its leaf: the delete leaves it a slot to place the cursor at.
    let k = (100..200)
        .find(|&k| {
            c.seek(&rw, &key(k)).expect("seek");
            follower.seek(&rw, &key(k + 1)).expect("seek");
            leaf(&c, &rw) == leaf(&follower, &rw)
        })
        .expect("a key that is not its leaf's last");
    let published = c.position(&rw).2;
    assert!(c.delete_current(&mut rw).expect("delete_current"));
    let mut fresh = rw.cursor_detached(&a).expect("cursor");
    assert_eq!(key_of(fresh.seek(&rw, &key(k)).expect("seek")), k + 1);
    assert_ne!(fresh.position(&rw).2.root, published.root, "the delete copied the root");
    assert_eq!(
        c.position(&rw),
        fresh.position(&rw),
        "delete_current lands in the new tree as a seek of the deleted key"
    );
    rw.commit().expect("commit");
    check(&env);
}

/// A grouped key: group `g`, then `s` (a cursor's prefix steps over 4 bytes walk one group).
fn gkey(g: u32, s: u32) -> Vec<u8> {
    let mut k = g.to_be_bytes().to_vec();
    k.extend_from_slice(&s.to_be_bytes());
    k
}

fn sub(entry: Option<(&[u8], &[u8])>) -> (u32, u32) {
    let k = entry.expect("an entry").0;
    (
        u32::from_be_bytes(k[..4].try_into().unwrap()),
        u32::from_be_bytes(k[4..8].try_into().unwrap()),
    )
}

/// Prefix steps through a cursor follow other writers' changes: an entry inserted after the
/// cursor's, the cursor's entry deleted (the step then compares with the deleted key, and stays
/// put at the group's end), the entry before it deleted, and a walk through a group that another
/// cursor's inserts split over several leaves.
#[test]
fn prefix_steps_through_txn_cursors_follow_other_writes() {
    let (_dir, path) = temp("tndb_b_txn_cursor_prefix");
    let env = open(&path, SyncMode::NoSync);
    let mut rw = env.begin_rw().expect("begin");
    let t = rw.create_table("t").expect("table");
    for g in 0..4 {
        for s in (0..200).step_by(2) {
            rw.put(&t, &gkey(g, s), &[s as u8; 30]).expect("put");
        }
    }
    let mut a = rw.cursor_detached(&t).expect("cursor");
    let mut b = rw.cursor_detached(&t).expect("cursor");
    assert!(a.seek_exact(&rw, &gkey(1, 50)).expect("seek").is_some());
    b.insert(&mut rw, &gkey(1, 51), b"b").expect("insert");
    assert_eq!(sub(a.next_in_prefix(&rw, 4).expect("step")), (1, 51), "the inserted follower");
    assert!(b.delete_current(&mut rw).expect("delete"), "b deletes the entry under a");
    assert_eq!(sub(a.next_in_prefix(&rw, 4).expect("step")), (1, 52), "the deleted key's follower");

    assert!(a.seek_exact(&rw, &gkey(1, 198)).expect("seek").is_some());
    assert!(rw.delete(&t, &gkey(1, 198)).expect("delete"));
    assert_eq!(a.next_in_prefix(&rw, 4).expect("step"), None, "no follower in the group");
    assert_eq!(a.state(), (State::At, true), "still after the deleted entry");
    assert_eq!(sub(a.prev_in_prefix(&rw, 4).expect("step")), (1, 196), "the entry before it");

    assert!(a.seek_exact(&rw, &gkey(2, 10)).expect("seek").is_some());
    assert!(rw.delete(&t, &gkey(2, 8)).expect("delete"));
    assert_eq!(sub(a.prev_in_prefix(&rw, 4).expect("step")), (2, 6), "past the deleted neighbour");

    // b fills group 2's odd keys with large values, splitting its leaves under a.
    assert!(a.seek_exact(&rw, &gkey(2, 0)).expect("seek").is_some());
    let leaves = rw.table_stat(&t).expect("stat").leaf_pages;
    for s in (1..200).step_by(2) {
        b.put(&mut rw, &gkey(2, s), &[s as u8; 300]).expect("put");
    }
    assert!(rw.table_stat(&t).expect("stat").leaf_pages > leaves + 3, "the group split");
    let mut walked = vec![(2, 0)];
    while let Some(e) = a.next_in_prefix(&rw, 4).expect("step") {
        walked.push(sub(Some(e)));
    }
    let want: Vec<(u32, u32)> = (0..200).filter(|&s| s != 8).map(|s| (2, s)).collect();
    assert_eq!(walked, want, "the walk through the split group");
    assert_eq!(sub(a.next(&rw).expect("next")), (3, 0));
    rw.commit().expect("commit");
    check(&env);
}

/// `follower_shares_prefix` answers from the keys the cursor kept, in each state, without moving
/// it: `false` before a delete and after a move; after a delete, whether the follower shares the
/// deleted key's first `n` bytes (`false` past the key's length, even for a follower that extends
/// the deleted key), the follower in the next leaf included; `false` after a delete of the
/// table's last entry; and after another writer's delete of the follower, for the entry after
/// it, found again by key.
#[test]
fn follower_shares_prefix_answers_for_the_deleted_entrys_follower() {
    let (_dir, path) = temp("tndb_b_txn_cursor_follower");
    let env = open(&path, SyncMode::NoSync);
    let mut rw = env.begin_rw().expect("begin");
    let t = rw.create_table("t").expect("table");
    for g in 0..4 {
        for s in (0..200).step_by(2) {
            rw.put(&t, &gkey(g, s), &[s as u8; 30]).expect("put");
        }
    }
    let mut c = rw.cursor_detached(&t).expect("cursor");
    assert!(c.seek_exact(&rw, &gkey(1, 50)).expect("seek").is_some());
    assert!(!c.follower_shares_prefix(&rw, 4).expect("ask"), "no delete yet");

    assert!(c.delete_current(&mut rw).expect("delete"));
    let asks = [0, 4, 7, 8, 9].map(|n| c.follower_shares_prefix(&rw, n).expect("ask"));
    assert_eq!(asks, [true, true, true, false, false], "the follower (1, 52)");
    assert_eq!(c.state(), (State::At, true), "the cursor did not move");
    assert_eq!(sub(c.next_in_prefix(&rw, 4).expect("step")), (1, 52));
    assert!(!c.follower_shares_prefix(&rw, 4).expect("ask"), "after a move");

    // Another writer deletes the follower: the cursor finds the entry after it.
    assert!(c.delete_current(&mut rw).expect("delete"), "deletes (1, 52)");
    assert!(rw.delete(&t, &gkey(1, 54)).expect("delete"));
    let reseeks = c.reseeks;
    assert!(c.follower_shares_prefix(&rw, 4).expect("ask"), "the follower (1, 56)");
    assert_eq!(c.reseeks, reseeks + 1);
    assert_eq!(sub(c.next(&rw).expect("next")), (1, 56));

    // A group's last entry: the follower is the next group's first.
    assert!(c.seek_exact(&rw, &gkey(1, 198)).expect("seek").is_some());
    assert!(c.delete_current(&mut rw).expect("delete"));
    assert!(!c.follower_shares_prefix(&rw, 4).expect("ask"), "the follower (2, 0)");
    assert!(c.follower_shares_prefix(&rw, 3).expect("ask"), "groups 1 and 2 share 3 bytes");

    // An entry that ends its leaf: the follower is the next leaf's first.
    let mut probe = rw.cursor_detached(&t).expect("cursor");
    let leaf = |c: &TxnCursor, rw: &RwTxn| c.position(rw).1.last().map(|&(page, _)| page);
    let s = (0..196)
        .step_by(2)
        .find(|&s| {
            probe.seek(&rw, &gkey(2, s)).expect("seek");
            let at = leaf(&probe, &rw);
            probe.next(&rw).expect("next");
            leaf(&probe, &rw) != at
        })
        .expect("a leaf ends inside group 2");
    assert!(c.seek_exact(&rw, &gkey(2, s)).expect("seek").is_some());
    assert!(c.delete_current(&mut rw).expect("delete"));
    assert!(c.follower_shares_prefix(&rw, 4).expect("ask"), "the next leaf's first");
    assert!(!c.follower_shares_prefix(&rw, 8).expect("ask"));
    assert_eq!(sub(c.next(&rw).expect("next")), (2, s + 2));

    // A follower that extends the deleted key: `n` past the deleted key's length is `false`.
    let longer = [gkey(3, 10), vec![1]].concat();
    rw.put(&t, &longer, b"longer").expect("put");
    assert!(c.seek_exact(&rw, &gkey(3, 10)).expect("seek").is_some());
    assert!(c.delete_current(&mut rw).expect("delete"));
    assert!(c.follower_shares_prefix(&rw, 8).expect("ask"), "the whole deleted key");
    assert!(!c.follower_shares_prefix(&rw, 9).expect("ask"), "past the deleted key");
    assert_eq!(c.next(&rw).expect("next").map(|(k, _)| k.to_vec()), Some(longer));

    // The table's last entry: after the end, no follower.
    assert_eq!(sub(c.last(&rw).expect("last")), (3, 198));
    assert!(c.delete_current(&mut rw).expect("delete"));
    assert_eq!(c.state(), (State::AfterEnd, true));
    assert!(!c.follower_shares_prefix(&rw, 0).expect("ask"), "after the end");
    assert!(env.failure().is_none());
    rw.commit().expect("commit");
    check(&env);
}
