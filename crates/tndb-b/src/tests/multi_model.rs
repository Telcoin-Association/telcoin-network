//! A randomized multi-table model: tables created, dropped and cleared, puts and deletes,
//! commits and aborts, readers held across commits (each checked against the model of its
//! snapshot), syncs, and reopens after clean closes, process crashes, strict and random power
//! losses, and commits stopped at a random point of the commit protocol — with the sync mode
//! drawn afresh at every reopen. After every commit and every reopen `Env::check` passes, and
//! every reopen finds exactly the state the protocol allows.

use super::*;
use crate::{
    disk_model,
    env::CrashAt,
    meta::slot_to_write,
    tree::test_harness::{ReportSeed, Rng},
    RoTxn,
};

/// The table names the workload uses.
const NAMES: [&str; 5] = ["t0", "t1", "t2", "t3", "a-table-with-a-much-longer-name"];

/// Key `k`: its four bytes, then a pad that varies its length.
fn key(k: u32) -> Vec<u8> {
    let mut key = k.to_be_bytes().to_vec();
    key.resize(4 + (k % 23) as usize, b'k');
    key
}

/// How many times each kind of event happened (the coverage gate).
#[derive(Debug, Default)]
struct Coverage {
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
}

/// The run: the environment and what the protocol says it holds.
struct Run {
    path: PathBuf,
    page_size: u32,
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

    /// Check a reader against the model of its snapshot.
    fn verify(ro: &RoTxn, want: &State) {
        assert!(state_of(ro, NAMES) == *want, "reader at txn {}", ro.txn_id());
    }

    /// Check and drop every reader.
    fn drop_readers(&mut self) {
        for (ro, want) in self.readers.drain(..) {
            Self::verify(&ro, &want);
            self.seen.readers_checked += 1;
        }
    }

    /// Apply 1..=40 random operations to `txn` and to `work`.
    fn random_ops(&mut self, txn: &mut RwTxn, work: &mut State, tag: u32) {
        for _ in 0..1 + self.rng.below(40) {
            let name = NAMES[self.rng.below(NAMES.len() as u64) as usize];
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
            let k = self.rng.below(400) as u32;
            if roll < 7 {
                txn.clear_table(&t).expect("clear");
                rows.clear();
                self.seen.clears += 1;
            } else if roll < 250 {
                assert_eq!(
                    txn.delete(&t, &key(k)).expect("delete"),
                    rows.remove(&key(k)).is_some()
                );
            } else {
                let max = txn_max_value(self.page_size, key(k).len());
                let len = if self.rng.below(20) == 0 { max } else { self.rng.len_upto(max / 3) };
                let v = value(tag, k, len);
                txn.put(&t, &key(k), &v).expect("put");
                rows.insert(key(k), v);
            }
        }
    }

    /// A write transaction of random operations, committed (then checked) or aborted.
    fn write(&mut self, step: u32) {
        let env = self.env.take().expect("open");
        let mut txn = env.begin_rw().expect("begin");
        let mut work = self.latest.1.clone();
        self.random_ops(&mut txn, &mut work, step);
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
            check(&env);
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

/// Run `steps` random steps from `seed` at `page_size`.
fn random_run(seed: u64, steps: u32, page_size: u32) -> Coverage {
    let _report = ReportSeed(seed);
    let (_dir, path) = temp("tndb_b_multi");
    let env = Env::open(&path, EnvOptions { page_size, ..EnvOptions::default() }).expect("open");
    disk_model::track(&path);
    let mut run = Run {
        path,
        page_size,
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
                    Run::verify(&ro, &want);
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
    let seen = random_run(0x3A11_7AB1, 800, 4096);
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
                    let seen = random_run(0x3A11_0000 + seed, 5000, page_size);
                    assert!(seen.stopped_to_new > 0 && seen.stopped_to_old > 0, "{seen:?}");
                });
            }
        });
    }
}
