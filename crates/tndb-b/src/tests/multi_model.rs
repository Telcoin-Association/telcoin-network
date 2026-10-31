//! A randomized multi-table model: tables created, dropped and cleared, puts and deletes,
//! commits and aborts, readers held across commits (each checked against the model of its
//! snapshot), syncs, and reopens after clean closes, process crashes, strict and random power
//! losses, and commits stopped at a random point of the commit protocol — with the sync mode
//! drawn afresh at every reopen. After every commit and every reopen `Env::check` passes, and
//! every reopen finds exactly the state the protocol allows. Values are inline only, or, in the
//! large-value variant, spread over inline values and overflow runs of 1 to 10 pages. In the
//! draining variant, stretches of mostly deletes empty the tables, so many deletes merge pages;
//! every variant's coverage gate counts the deletes that merged (`Writer::merging_deletes`).
//! In the dup variant, two dup tables (outer keys of 4 and of 20 bytes) take puts, inserts,
//! appends, deletes, `delete_prefix` calls, gets, clears and drops in the same transactions as two
//! plain tables, under the same crashes; its gate counts, per dup table, nested and outer trees of
//! two levels, deletes that emptied an outer key, `delete_prefix` hits, a clear and a drop.

use super::*;
use crate::{
    disk_model,
    env::CrashAt,
    meta::slot_to_write,
    tree::{
        test_harness::{walk_outer_tree, ReportSeed, Rng},
        TreeRoot,
    },
    DupOptions, Error, RoTxn,
};

/// The table names the workload uses.
const NAMES: [&str; 5] = ["t0", "t1", "t2", "t3", "a-table-with-a-much-longer-name"];

/// Key `k`: its four bytes, then a pad that varies its length.
fn key(k: u32) -> Vec<u8> {
    let mut key = k.to_be_bytes().to_vec();
    key.resize(4 + (k % 23) as usize, b'k');
    key
}

/// The dup tables of the dup variant ([`Run::dups`]), beside its plain tables `t0` and `t1`.
const DUPS: [(&str, DupOptions); 2] = [
    ("d4", DupOptions { key_len: 4, packed_max: 0 }),
    ("d20", DupOptions { key_len: 20, packed_max: 0 }),
];

/// The hot outer key of a dup table: two in five picks ([`Run::dup_key`]), so its nested tree
/// grows two levels.
const HOT: u32 = 3;

/// The subkey alphabet: small, so short subkeys hit present rows.
const SUBKEY: &[u8] = b"abcd";

/// Outer key number `k` of a dup table of `key_len`-byte outer keys: its four bytes, then a pad
/// that it decides (so the outer keys sort by `k`).
fn outer(k: u32, key_len: usize) -> Vec<u8> {
    let pad = k.wrapping_mul(0x9E37_79B9).to_le_bytes();
    let mut key = k.to_be_bytes().to_vec();
    key.extend((0..key_len - 4).map(|i| pad[i % 4] ^ i as u8));
    key
}

/// The number of an outer key ([`outer`]) of a dup table's logical key.
fn outer_number(key: &[u8]) -> u32 {
    u32::from_be_bytes(key[..4].try_into().expect("4 bytes"))
}

/// The outer keys of `key_len` bytes `rows` (a dup table's) holds rows under.
fn outer_count(rows: &Rows, key_len: usize) -> u32 {
    let keys: std::collections::BTreeSet<_> = rows.keys().map(|key| &key[..key_len]).collect();
    keys.len() as u32
}

/// The first row of `rows` at or after `probe` (or the first row), else `probe`.
fn present(rows: &Rows, probe: Vec<u8>) -> Vec<u8> {
    let found = rows.range(probe.clone()..).next().or_else(|| rows.iter().next());
    found.map_or(probe, |(key, _)| key.clone())
}

/// Dup table `t`'s rows (of `key_len`-byte outer keys) as `txn` sees them: a walk of its trees
/// that checks the totals (`dup_rows_for_test`), whose rows and outer keys its `table_stat`
/// counts, and a `get` of every seventh one finds.
fn dup_rows(txn: &RoTxn, t: &Table, key_len: usize) -> Rows {
    let walked = txn.dup_rows_for_test(t);
    let rows: Rows = walked.iter().cloned().collect();
    let stat = txn.table_stat(t).expect("stat");
    let counts = (walked.len() as u64, outer_count(&rows, key_len));
    assert_eq!((stat.entries, stat.nested_trees), counts, "{}: entries, trees", t.name());
    for (key, v) in walked.iter().step_by(7) {
        assert_eq!(txn.get(t, key).expect("get"), Some(v.as_slice()), "{}: get", t.name());
    }
    rows
}

/// The steps of each stretch of a draining run ([`Run::drains`]).
const DRAIN_STEPS: u32 = 64;

/// How many times each kind of event happened (the coverage gate).
#[derive(Debug, Default)]
struct Coverage {
    /// Puts of values held in overflow runs, and of those in runs of ten pages.
    run_puts: u32,
    ten_page_puts: u32,
    commits: u32,
    aborts: u32,
    drops: u32,
    clears: u32,
    readers_checked: u32,
    syncs: u32,
    clean_reopens: u32,
    process_crashes: u32,
    strict_losses: u32,
    random_losses: u32,
    stopped_commits: u32,
    stopped_to_new: u32,
    stopped_to_old: u32,
    /// Deletes whose merges absorbed a sibling (`Writer::merging_deletes`).
    merging_deletes: u64,
    /// Each dup table's, in the dup variant ([`DUPS`]).
    dups: [DupSeen; 2],
}

/// What one dup table met.
#[derive(Debug, Default)]
struct DupSeen {
    /// The tallest nested tree and outer tree at a commit, and the commits that found both of
    /// two levels since the last clear or drop ([`Run::grown`]).
    nested_height: u32,
    outer_height: u32,
    grown: u32,
    /// Deletes that emptied an outer key, so its tree and cell went.
    emptied_keys: u32,
    /// `delete_prefix` calls that removed rows.
    prefix_hits: u32,
    /// Inserts of present keys (`KeyExists`); appends taken, and refused (`AppendOutOfOrder`).
    key_exists: u32,
    appends: u32,
    appends_refused: u32,
    /// Puts of values in runs.
    run_puts: u32,
    clears: u32,
    drops: u32,
}

/// The run: the environment and what the protocol says it holds.
struct Run {
    path: PathBuf,
    page_size: u32,
    /// Put values in overflow runs too ([`Run::large_len`]), over the keys `0..keys`.
    large: bool,
    /// Alternate stretches of [`DRAIN_STEPS`] steps of the usual operations with stretches of
    /// mostly deletes of present keys, so tables fill and drain, and deletes merge pages.
    drains: bool,
    keys: u64,
    /// The plain tables, and the dup tables (their names and options) beside them.
    plain: &'static [&'static str],
    dups: &'static [(&'static str, DupOptions)],
    /// Outer keys `0..outers` of the dup tables (and the appends' past them).
    outers: u32,
    /// Each dup table showed a nested and an outer tree of two levels at a commit since its last
    /// clear or drop: a clear or drop drawn before that is another operation.
    grown: [bool; 2],
    rng: Rng,
    env: Option<Env>,
    sync: SyncMode,
    /// The newest committed state, and the newest durable one (its txn and state).
    latest: (u64, State),
    durable: (u64, State),
    readers: Vec<(RoTxn, State)>,
    seen: Coverage,
}

impl Run {
    fn env(&self) -> &Env {
        self.env.as_ref().expect("open")
    }

    /// Check a reader against the model of its snapshot: the plain tables' rows by their
    /// cursors, the dup tables' by [`dup_rows`].
    fn verify(&self, ro: &RoTxn, want: &State) {
        let mut state = state_of(ro, self.plain.iter().copied());
        for &(name, options) in self.dups {
            if let Some(t) = ro.open_table(name).expect("open table") {
                state.insert(name.to_string(), dup_rows(ro, &t, options.key_len.into()));
            }
        }
        assert!(state == *want, "reader at txn {}", ro.txn_id());
    }

    /// Check and drop every reader.
    fn drop_readers(&mut self) {
        for (ro, want) in std::mem::take(&mut self.readers) {
            self.verify(&ro, &want);
            self.seen.readers_checked += 1;
        }
    }

    /// Apply 1..=40 random operations to `txn` and to `work`.
    fn random_ops(&mut self, txn: &mut RwTxn, work: &mut State, tag: u32) {
        // Per mille of operations that delete: three in four, of keys that are there, in a
        // draining stretch.
        let draining = self.drains && tag / DRAIN_STEPS % 2 == 1;
        let deletes = if draining { 750 } else { 250 };
        for _ in 0..1 + self.rng.below(40) {
            let i = self.rng.below((self.plain.len() + self.dups.len()) as u64) as usize;
            if i >= self.plain.len() {
                self.dup_op(txn, work, i - self.plain.len(), tag);
                continue;
            }
            let name = self.plain[i];
            let roll = self.rng.below(1000);
            if roll < 3 {
                if let Some(t) = txn.open_table(name).expect("open") {
                    txn.drop_table(t).expect("drop");
                    work.remove(name).expect("the model has it");
                    self.seen.drops += 1;
                }
                continue;
            }
            let t = txn.create_table(name).expect("table");
            let rows = work.entry(name.to_string()).or_default();
            let k = self.rng.below(self.keys) as u32;
            if roll < 7 {
                txn.clear_table(&t).expect("clear");
                rows.clear();
                self.seen.clears += 1;
            } else if roll < deletes {
                let k = match rows.len() as u64 {
                    n if draining && n > 0 => {
                        rows.keys().nth(self.rng.below(n) as usize).expect("a key").clone()
                    }
                    _ => key(k),
                };
                assert_eq!(txn.delete(&t, &k).expect("delete"), rows.remove(&k).is_some());
            } else {
                let max = txn_max_value(self.page_size, key(k).len());
                let len = if self.large {
                    self.large_len(max)
                } else if self.rng.below(20) == 0 {
                    max
                } else {
                    self.rng.len_upto(max / 3)
                };
                let v = value(tag, k, len);
                txn.put(&t, &key(k), &v).expect("put");
                rows.insert(key(k), v);
            }
        }
    }

    /// A key of a dup table of `key_len`-byte outer keys: under the hot outer key ([`HOT`]) two
    /// times in five, else under any of `0..outers`; a subkey of 1 to 12 bytes of [`SUBKEY`].
    fn dup_key(&mut self, key_len: usize) -> Vec<u8> {
        let k = match self.rng.below(5) {
            0..2 => HOT,
            _ => self.rng.below(self.outers.into()) as u32,
        };
        let mut key = outer(k, key_len);
        self.push_subkey(&mut key);
        key
    }

    /// Append a subkey of 1 to 12 bytes of [`SUBKEY`] to `key`.
    fn push_subkey(&mut self, key: &mut Vec<u8>) {
        for _ in 0..1 + self.rng.below(12) {
            key.push(SUBKEY[self.rng.below(SUBKEY.len() as u64) as usize]);
        }
    }

    /// A value for a dup table: 0 to 160 bytes, or one in thirty in a run (longer than any subkey
    /// takes inline, up to a page more).
    fn dup_value(&mut self, tag: u32, j: usize) -> Vec<u8> {
        let len = if self.rng.below(30) == 0 {
            self.seen.dups[j].run_puts += 1;
            txn_max_value(self.page_size, 1) + 1 + self.rng.len_upto(self.page_size as usize)
        } else {
            self.rng.len_upto(160)
        };
        value(tag, self.rng.next() as u32, len)
    }

    /// One random operation on dup table `self.dups[j]` (created if missing) in `txn` and
    /// `work`: a put, insert, append, delete, `delete_prefix` or get, each answering as the model
    /// says, or, once the table has grown ([`Run::grown`]), a clear or a drop. After it the
    /// table's entries and nested trees are the model's.
    fn dup_op(&mut self, txn: &mut RwTxn, work: &mut State, j: usize, tag: u32) {
        let (name, options) = self.dups[j];
        let klen = usize::from(options.key_len);
        let roll = self.rng.below(1000);
        if roll < 6 && self.grown[j] {
            if let Some(t) = txn.open_table(name).expect("open") {
                txn.drop_table(t).expect("drop");
                work.remove(name).expect("the model has it");
                self.grown[j] = false;
                self.seen.dups[j].drops += 1;
                return;
            }
        }
        let t = txn.create_dup_table(name, options).expect("dup table");
        let rows = work.entry(name.to_string()).or_default();
        match roll {
            0..12 if self.grown[j] => {
                txn.clear_table(&t).expect("clear");
                rows.clear();
                self.grown[j] = false;
                self.seen.dups[j].clears += 1;
            }
            0..320 => {
                let (key, v) = (self.dup_key(klen), self.dup_value(tag, j));
                txn.put(&t, &key, &v).expect("put");
                rows.insert(key, v);
            }
            320..400 => {
                let probe = self.dup_key(klen);
                let key = if self.rng.below(2) == 0 { present(rows, probe) } else { probe };
                let v = self.dup_value(tag, j);
                let got = txn.insert(&t, &key, &v);
                match rows.entry(key) {
                    std::collections::btree_map::Entry::Occupied(_) => {
                        assert!(matches!(got, Err(Error::KeyExists)), "{name}: insert: {got:?}");
                        self.seen.dups[j].key_exists += 1;
                    }
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        got.expect("insert");
                        slot.insert(v);
                    }
                }
            }
            400..460 => {
                // Past the last key half the time: a subkey byte more, or a later outer key.
                let key = match rows.keys().next_back() {
                    Some(last) if self.rng.below(2) == 0 => {
                        let c = SUBKEY[self.rng.below(SUBKEY.len() as u64) as usize];
                        if last.len() < klen + 12 && self.rng.below(2) == 0 {
                            [&last[..], &[c]].concat()
                        } else {
                            let k = outer_number(last) + 1 + self.rng.below(3) as u32;
                            let mut key = outer(k, klen);
                            self.push_subkey(&mut key);
                            key
                        }
                    }
                    _ => self.dup_key(klen),
                };
                let v = self.dup_value(tag, j);
                let got = txn.append(&t, &key, &v);
                if rows.keys().next_back().is_none_or(|last| key > *last) {
                    got.expect("an append past the last key");
                    rows.insert(key, v);
                    self.seen.dups[j].appends += 1;
                } else {
                    assert!(matches!(got, Err(Error::AppendOutOfOrder)), "{name}: {got:?}");
                    self.seen.dups[j].appends_refused += 1;
                }
            }
            460..620 => {
                let probe = self.dup_key(klen);
                let key = if self.rng.below(3) < 2 { present(rows, probe) } else { probe };
                let found = txn.delete(&t, &key).expect("delete");
                assert_eq!(found, rows.remove(&key).is_some(), "{name}: delete");
                let k = &key[..klen];
                if found
                    && rows.range(k.to_vec()..).next().is_none_or(|(next, _)| !next.starts_with(k))
                {
                    self.seen.dups[j].emptied_keys += 1;
                }
            }
            620..660 => {
                // Of a present outer key three times in four, else of any (most often absent).
                let k = match self.rng.below(4) {
                    0..3 if !rows.is_empty() => {
                        let probe = outer(self.rng.below(self.outers.into()) as u32, klen);
                        outer_number(&present(rows, probe))
                    }
                    _ => self.rng.below(u64::from(self.outers + self.outers / 8)) as u32,
                };
                let prefix = outer(k, klen);
                let removed = txn.delete_prefix(&t, &prefix).expect("delete_prefix");
                let before = rows.len();
                rows.retain(|key, _| !key.starts_with(&prefix));
                assert_eq!(removed, (before - rows.len()) as u64, "{name}: delete_prefix {k}");
                self.seen.dups[j].prefix_hits += u32::from(removed > 0);
            }
            _ => {
                let probe = self.dup_key(klen);
                let key = match self.rng.below(3) {
                    0 => present(rows, probe),
                    1 => probe,
                    _ => [outer(self.outers + 1000, klen), probe[klen..].to_vec()].concat(),
                };
                let want = rows.get(&key).map(Vec::as_slice);
                assert_eq!(txn.get(&t, &key).expect("get"), want, "{name}: get");
            }
        }
        let stat = txn.table_stat(&t).expect("stat");
        let counts = (rows.len() as u64, outer_count(rows, klen));
        assert_eq!((stat.entries, stat.nested_trees), counts, "{name}: entries, trees");
    }

    /// The dup tables' rows in `txn` (by walks of their trees, with the totals) are `work`'s.
    fn compare_dups(&self, txn: &RwTxn, work: &State) {
        for &(name, _) in self.dups {
            if let Some(t) = txn.open_table(name).expect("open") {
                let want: Vec<_> = work[name].iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                assert!(txn.dup_rows_for_test(&t) == want, "{name}: the transaction's rows");
            }
        }
    }

    /// After a commit (`report` its `Env::check`): each dup table's figures in the check are its
    /// `table_stat`'s, and its tallest nested tree and its outer tree's height count toward the
    /// coverage (and mark it grown once both have two levels).
    fn dup_heights(&mut self, env: &Env, report: &CheckReport) {
        let ro = env.begin_ro().expect("ro");
        let snapshot = env.inner().current();
        for (j, &(name, _)) in self.dups.iter().enumerate() {
            if !report.tables.contains_key(name) {
                continue;
            }
            dup::assert_dup_figures(report, &ro, name);
            let root = damage::tree(env, name);
            let nested = walk_outer_tree(&*snapshot, &root)
                .entries
                .iter()
                .map(|(_, cell)| {
                    let cell = cell.as_slice().try_into().expect("32 bytes");
                    TreeRoot::decode(cell).expect("a nested root").height
                })
                .max()
                .unwrap_or(0);
            let seen = &mut self.seen.dups[j];
            seen.nested_height = seen.nested_height.max(nested);
            seen.outer_height = seen.outer_height.max(root.height);
            if !self.grown[j] && root.height >= 2 && nested >= 2 {
                self.grown[j] = true;
                seen.grown += 1;
            }
        }
    }

    /// The length of a value in the large-value variant, whose key takes inline values up to
    /// `max` bytes: inline (one in three), else in an overflow run of 1 to 10 pages, any length
    /// that takes that many.
    fn large_len(&mut self, max: usize) -> usize {
        if self.rng.below(3) == 0 {
            return self.rng.len_upto(max);
        }
        let pages = 1 + self.rng.below(10) as usize;
        let p = self.page_size as usize;
        let header = crate::overflow::RUN_HEADER + crate::page::TRAILER;
        let low = (max + 1).max(((pages - 1) * p + 1).saturating_sub(header));
        let high = pages * p - header;
        self.seen.run_puts += 1;
        self.seen.ten_page_puts += u32::from(pages == 10);
        low + self.rng.len_upto(high - low)
    }

    /// A write transaction of random operations, committed (then checked) or aborted.
    fn write(&mut self, step: u32) {
        let env = self.env.take().expect("open");
        let mut txn = env.begin_rw().expect("begin");
        let mut work = self.latest.1.clone();
        self.random_ops(&mut txn, &mut work, step);
        self.seen.merging_deletes += std::mem::take(&mut txn.writer_mut().merging_deletes);
        self.compare_dups(&txn, &work);
        if self.rng.below(8) == 0 {
            txn.abort();
            self.seen.aborts += 1;
        } else {
            let id = txn.commit().expect("commit");
            if id != self.latest.0 {
                assert_eq!(id, self.latest.0 + 1);
                self.latest = (id, work);
                if self.sync == SyncMode::Durable {
                    self.durable = self.latest.clone();
                }
            } else {
                // The base id says the transaction changed nothing: neither may the model.
                assert!(work == self.latest.1, "a commit of real changes returned the base id");
            }
            let report = check(&env);
            self.dup_heights(&env, &report);
            self.seen.commits += 1;
        }
        self.env = Some(env);
    }

    /// Drop every handle as a crash would and take the power down as `loss` says (0: process
    /// crash only, 1: strict, 2: random).
    fn go_down(&mut self, loss: u64) {
        self.drop_readers();
        self.env.take().expect("open").crash();
        match loss {
            0 => self.seen.process_crashes += 1,
            1 => {
                disk_model::power_loss(&self.path);
                self.seen.strict_losses += 1;
            }
            _ => {
                disk_model::power_loss_random(&self.path, self.rng.next());
                self.seen.random_losses += 1;
            }
        }
    }

    /// Reopen in a random sync mode: the file must hold exactly `want`, and check. A crash right
    /// after the reopen (no commit) changes nothing.
    fn reopen(&mut self, want: (u64, State)) {
        loop {
            self.sync = if self.rng.below(2) == 0 { SyncMode::Durable } else { SyncMode::NoSync };
            let env = open(&self.path, self.sync);
            assert_eq!(env.stat().txn_id, want.0, "reopen");
            assert!(newest_state(&env) == want.1, "reopen at txn {}: the state differs", want.0);
            if self.rng.below(8) != 0 {
                self.env = Some(env);
                break;
            }
            // A crash and a power loss right after the reopen: the next one must find the same.
            env.crash();
            disk_model::power_loss(&self.path);
        }
        self.latest = want.clone();
        self.durable = want;
    }

    /// A commit (or, in `SyncMode::NoSync`, the sync after it) stopped at a random point of the
    /// commit protocol, a random way down, and a reopen to the state the protocol allows.
    fn stopped_commit(&mut self, step: u32) {
        let points = [
            CrashAt::BeforeSave,
            CrashAt::AfterSave,
            CrashAt::AfterStamp,
            CrashAt::AfterDataSync,
            CrashAt::AfterSizeSync,
            CrashAt::AfterMetaCopy,
            CrashAt::AfterMetaMsync,
        ];
        let at = points[self.rng.below(points.len() as u64) as usize];
        let env = self.env.take().expect("open");
        let slot = env.with_writer(|w| slot_to_write(&w.slots()));
        let mut txn = env.begin_rw().expect("begin");
        let mut work = self.latest.1.clone();
        self.random_ops(&mut txn, &mut work, step);
        self.seen.merging_deletes += std::mem::take(&mut txn.writer_mut().merging_deletes);
        // Make sure the transaction changes something, so the commit has steps to stop in.
        let t = txn.create_table("t0").expect("table");
        let v = value(step, 0, 50);
        txn.put(&t, &key(0), &v).expect("put");
        work.entry("t0".to_string()).or_default().insert(key(0), v);
        let in_commit = self.sync == SyncMode::Durable
            || matches!(at, CrashAt::BeforeSave | CrashAt::AfterSave | CrashAt::AfterStamp);
        let stopped = if in_commit {
            txn.writer_mut().crash_at = Some(at);
            txn.commit().map(|_| ())
        } else {
            txn.commit().expect("an unsynced commit");
            env.with_writer(|w| w.crash_at = Some(at));
            env.sync()
        };
        let e = stopped.expect_err("the commit stops at its crash point");
        assert!(e.to_string().contains("injected crash"), "{at:?}: {e}");
        let new = (self.latest.0 + 1, work);
        self.env = Some(env);
        let loss = self.rng.below(3);
        self.go_down(loss);
        let reaches_new = match at {
            _ if self.sync == SyncMode::NoSync && in_commit => false,
            CrashAt::AfterMetaMsync => true,
            CrashAt::AfterMetaCopy => slot_holds(&self.path, self.page_size, slot, new.0),
            _ => false,
        };
        self.seen.stopped_commits += 1;
        let want = if reaches_new {
            self.seen.stopped_to_new += 1;
            new
        } else {
            self.seen.stopped_to_old += 1;
            self.durable.clone()
        };
        self.reopen(want);
    }
}

/// The longest value a `klen`-byte key takes at `page_size`.
fn txn_max_value(page_size: u32, klen: usize) -> usize {
    crate::page::Geometry::new(page_size).expect("geometry").max_value(klen)
}

/// Run `steps` random steps from `seed` at `page_size`, with large values ([`Run::large_len`],
/// over 40 keys a table) or inline ones only (over 400), draining the tables at times
/// ([`Run::drains`]) or not.
fn random_run(seed: u64, steps: u32, page_size: u32, large: bool, drains: bool) -> Coverage {
    random_run_with(seed, steps, page_size, large, drains, false)
}

/// [`random_run`], over the plain tables `t0` and `t1` and the dup tables [`DUPS`] if `dups`.
fn random_run_with(
    seed: u64,
    steps: u32,
    page_size: u32,
    large: bool,
    drains: bool,
    dups: bool,
) -> Coverage {
    let _report = ReportSeed(seed);
    let (_dir, path) = temp("tndb_b_multi");
    let env = Env::open(&path, EnvOptions { page_size, ..EnvOptions::default() }).expect("open");
    disk_model::track(&path);
    let mut run = Run {
        path,
        page_size,
        large,
        drains,
        keys: if large { 40 } else { 400 },
        plain: if dups { &NAMES[..2] } else { &NAMES },
        dups: if dups { &DUPS } else { &[] },
        outers: page_size / 12,
        grown: [false; 2],
        rng: Rng(seed),
        env: Some(env),
        sync: SyncMode::Durable,
        latest: (0, State::new()),
        durable: (0, State::new()),
        readers: Vec::new(),
        seen: Coverage::default(),
    };
    for step in 0..steps {
        match run.rng.below(100) {
            0..55 => run.write(step),
            55..63 => {
                if run.readers.len() < 4 {
                    let ro = run.env().begin_ro().expect("ro");
                    assert_eq!(ro.txn_id(), run.latest.0);
                    run.readers.push((ro, run.latest.1.clone()));
                }
            }
            63..71 => {
                if !run.readers.is_empty() {
                    let i = run.rng.below(run.readers.len() as u64) as usize;
                    let (ro, want) = run.readers.remove(i);
                    run.verify(&ro, &want);
                    run.seen.readers_checked += 1;
                }
            }
            71..78 => {
                run.env().sync().expect("sync");
                run.durable = run.latest.clone();
                run.seen.syncs += 1;
            }
            78..82 => {
                run.drop_readers();
                drop(run.env.take());
                run.seen.clean_reopens += 1;
                let latest = run.latest.clone();
                run.reopen(latest);
            }
            82..91 => {
                let loss = run.rng.below(3);
                run.go_down(loss);
                let durable = run.durable.clone();
                run.reopen(durable);
            }
            _ => run.stopped_commit(step),
        }
    }
    run.drop_readers();
    let env = run.env.take().expect("open");
    assert!(newest_state(&env) == run.latest.1);
    drop(env);
    disk_model::untrack(&run.path);
    run.seen
}

#[test]
fn a_random_multi_table_workload_with_crashes_matches_the_model() {
    let seen = random_run(0x3A11_7AB1, 800, 4096, false, false);
    let gate = [
        seen.commits >= 300,
        seen.aborts >= 20,
        seen.drops >= 3,
        seen.clears >= 3,
        seen.readers_checked >= 30,
        seen.syncs >= 20,
        seen.clean_reopens >= 10,
        seen.process_crashes >= 10,
        seen.strict_losses >= 10,
        seen.random_losses >= 10,
        seen.stopped_to_new >= 3,
        seen.stopped_to_old >= 20,
        seen.merging_deletes >= 3,
    ];
    assert!(gate.iter().all(|&ok| ok), "{seen:?}");
}

#[test]
#[ignore = "heavy: run with --release"]
fn a_random_multi_table_workload_with_crashes_matches_the_model_heavy() {
    // The runs are independent (the disk model keys files by inode) and mostly wait on syncs:
    // run them side by side.
    let seeds: Vec<u64> = (0..20).collect();
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    for chunk in seeds.chunks(threads) {
        std::thread::scope(|s| {
            for &seed in chunk {
                s.spawn(move || {
                    // One run at the largest page size: its files are the slowest to sync.
                    let page_size = match seed {
                        19 => 65536,
                        _ => [4096, 8192, 16384, 4096, 32768][(seed % 5) as usize],
                    };
                    let seen = random_run(0x3A11_0000 + seed, 5000, page_size, false, false);
                    assert!(seen.stopped_to_new > 0 && seen.stopped_to_old > 0, "{seen:?}");
                });
            }
        });
    }
}

/// The large-value variant: values inline and in overflow runs of 1 to 10 pages, with drops and
/// clears of tables holding runs, under the same crashes, power losses and stopped commits.
#[test]
fn a_random_multi_table_workload_of_large_values_with_crashes_matches_the_model() {
    let seen = random_run(0x7A11_0B16, 300, 4096, true, false);
    let gate = [
        seen.run_puts >= 1000,
        seen.ten_page_puts >= 100,
        seen.commits >= 120,
        seen.aborts >= 10,
        seen.drops >= 3,
        seen.clears >= 3,
        seen.readers_checked >= 10,
        seen.syncs >= 10,
        seen.clean_reopens >= 5,
        seen.process_crashes >= 5,
        seen.strict_losses >= 5,
        seen.random_losses >= 5,
        seen.stopped_to_new >= 3,
        seen.stopped_to_old >= 10,
        seen.merging_deletes >= 10,
    ];
    assert!(gate.iter().all(|&ok| ok), "{seen:?}");
}

#[test]
#[ignore = "heavy: run with --release"]
fn a_random_multi_table_workload_of_large_values_with_crashes_matches_the_model_heavy() {
    // `(page size, steps)` by seed. A run's cost grows with the page size (a ten-page run of 64
    // KiB pages is a 640 KiB value), so the largest page sizes take fewer steps.
    let runs: Vec<(u64, (u32, u32))> = [
        (4096, 2000),
        (8192, 1500),
        (16384, 1000),
        (4096, 2000),
        (8192, 1500),
        (4096, 2000),
        (32768, 400),
        (65536, 250),
    ]
    .into_iter()
    .enumerate()
    .map(|(seed, run)| (seed as u64, run))
    .collect();
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    for chunk in runs.chunks(threads) {
        std::thread::scope(|s| {
            for &(seed, (page_size, steps)) in chunk {
                s.spawn(move || {
                    let seen = random_run(0x7A11_0000 + seed, steps, page_size, true, false);
                    assert!(seen.stopped_to_new > 0 && seen.stopped_to_old > 0, "{seen:?}");
                    assert!(seen.drops > 0 && seen.clears > 0, "{seen:?}");
                });
            }
        });
    }
}

/// The draining variant ([`Run::drains`]): stretches of mostly deletes empty the tables that the
/// stretches between them fill, so many deletes merge pages (and roots give way), under the same
/// aborts, readers, crashes, power losses and stopped commits.
#[test]
fn a_draining_multi_table_workload_with_crashes_matches_the_model() {
    let seen = random_run(0xD4A1_0001, 1600, 4096, false, true);
    let gate = [
        seen.merging_deletes >= 100,
        seen.commits >= 500,
        seen.aborts >= 50,
        seen.readers_checked >= 50,
        seen.process_crashes >= 30,
        seen.strict_losses >= 30,
        seen.random_losses >= 30,
        seen.stopped_to_new >= 10,
        seen.stopped_to_old >= 50,
    ];
    assert!(gate.iter().all(|&ok| ok), "{seen:?}");
}

#[test]
#[ignore = "heavy: run with --release"]
fn a_draining_multi_table_workload_with_crashes_matches_the_model_heavy() {
    let seeds: Vec<u64> = (0..8).collect();
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    for chunk in seeds.chunks(threads) {
        std::thread::scope(|s| {
            for &seed in chunk {
                s.spawn(move || {
                    let page_size = [4096, 8192, 16384, 4096][(seed % 4) as usize];
                    let seen = random_run(0xD4A1_0000 + seed, 4000, page_size, false, true);
                    assert!(seen.merging_deletes >= 100, "{seen:?}");
                    assert!(seen.stopped_to_new > 0 && seen.stopped_to_old > 0, "{seen:?}");
                });
            }
        });
    }
}

/// The dup variant ([`Run::dups`]): dup tables of 4-byte and of 20-byte outer keys beside two plain
/// tables, their operations interleaved in the same transactions, under the same aborts, held
/// readers, crashes, power losses and stopped commits; before every commit or abort the dup
/// tables' rows in the transaction are the model's, after every commit their figures in
/// `Env::check` are their `table_stat`'s. Each dup table grows nested and outer trees of two
/// levels, empties outer keys by deletes and by `delete_prefix`, and is cleared and dropped.
#[test]
fn a_multi_table_workload_with_dup_tables_and_crashes_matches_the_model() {
    let seen = random_run_with(0xD0B1_3A11, 1500, 4096, false, false, true);
    let mut gate = vec![
        seen.commits >= 500,
        seen.aborts >= 50,
        seen.readers_checked >= 50,
        seen.clean_reopens >= 20,
        seen.process_crashes >= 30,
        seen.strict_losses >= 30,
        seen.random_losses >= 30,
        seen.stopped_to_new >= 10,
        seen.stopped_to_old >= 50,
    ];
    for d in &seen.dups {
        gate.extend([
            d.nested_height >= 2,
            d.outer_height >= 2,
            d.grown >= 2,
            d.emptied_keys >= 100,
            d.prefix_hits >= 50,
            d.key_exists >= 50,
            d.appends >= 50,
            d.appends_refused >= 50,
            d.run_puts >= 30,
            d.clears >= 1,
            d.drops >= 1,
        ]);
    }
    assert!(gate.iter().all(|&ok| ok), "{seen:?}");
}

#[test]
#[ignore = "heavy: run with --release"]
fn a_multi_table_workload_with_dup_tables_and_crashes_matches_the_model_heavy() {
    let seeds: Vec<u64> = (0..8).collect();
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    for chunk in seeds.chunks(threads) {
        std::thread::scope(|s| {
            for &seed in chunk {
                s.spawn(move || {
                    let page_size = [4096, 8192, 16384, 4096][(seed % 4) as usize];
                    let seen =
                        random_run_with(0xD0B1_3A00 + seed, 5000, page_size, false, false, true);
                    eprintln!("{seed}: {page_size}: {seen:?}");
                    assert!(seen.stopped_to_new > 0 && seen.stopped_to_old > 0, "{seen:?}");
                    for d in &seen.dups {
                        assert!(
                            d.nested_height >= 2 && d.outer_height >= 2,
                            "{page_size}: {seen:?}"
                        );
                        assert!(d.clears > 0 && d.drops > 0, "{page_size}: {seen:?}");
                    }
                });
            }
        });
    }
}
