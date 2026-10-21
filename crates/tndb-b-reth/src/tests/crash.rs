//! Crash cycles: a child process writes rounds through a [`TndbEnv`] until the test kills it with
//! `SIGKILL` at a random point (in a round, in a commit, or in the open), then the test reopens the
//! environment and checks it against a model.
//!
//! The child is this test binary run again on `crash_cycle_child` with [`CHILD_ENV`] naming the
//! directory. It opens the environment, reads the progress row (`PlainAccountState` at
//! [`progress_key`], the nonce holding the last round committed) and, from the round after it,
//! runs [`round_ops`] in one write transaction per round. It prints `commit <round>` before each
//! commit and `round <round>` once the commit has returned. A round puts and deletes rows of
//! `CanonicalHeaders` through the transaction and appends rows past them through a cursor; puts
//! (reth's way: `seek_by_key_subkey`, `delete_current`, `upsert`), appends (`append_dup`) and
//! deletes rows of `PlainStorageState` through a dup cursor (`delete_current`,
//! `delete_current_duplicates`) and the transaction, and every 13th round first clears that table;
//! puts and deletes `Bytecodes` values of 1.5 KiB to 70 KiB (overflow runs); and writes the
//! progress row. The puts and deletes take keys of a fixed set, so later rounds replace and delete
//! earlier rounds' rows; the appends take keys (and subkeys) from the round, so each sorts after
//! every earlier round's.
//!
//! After each kill the reopened environment must:
//! - open, with no latched failure;
//! - hold the progress the child's last line gives: the round it last printed as committed, or,
//!   when its last line is `commit <round>`, that round or the one before (the kill landed in the
//!   commit, or after it returned but before the child printed so);
//! - hold, in every table, exactly the rows of the model of rounds `1..=progress` (rows and
//!   `entries`);
//! - pass [`tndb_b::Env::check`].
//!
//! The next child must start from the round after that progress.
//!
//! A `SIGKILL` stops the process but keeps the page cache, so this tests the adapter and the
//! engine's commit as a whole under process death, not power loss: the engine's crash matrix
//! (`tndb-b`'s `tests::crash`) covers lost and torn writes with a disk model.

use crate::{TndbArgs, TndbEnv, TndbEnvKind};
use alloy::primitives::{Address, Bytes, B256, U256};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    database::Database,
    table::Table,
    tables::{Bytecodes, CanonicalHeaders, PlainAccountState, PlainStorageState},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{BufRead as _, BufReader, Write as _},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Set in the child's environment to the directory of the environment it writes.
const CHILD_ENV: &str = "TNDB_B_RETH_CRASH_CHILD";

/// The child's entry point: its test name in this binary.
const CHILD_TEST: &str = "tests::crash::crash_cycle_child";

/// Overrides the seed of the kill points (the test prints the seed it used).
const SEED_ENV: &str = "TNDB_B_RETH_CRASH_SEED";

/// What the child prints before each of its lines; the line may also hold the test harness's
/// own output before it.
const MARK: &str = "tndb-b-reth-crash ";

/// The keys a round writes and deletes come from these sets.
const HEADER_KEYS: u64 = 400;
const ACCOUNTS: u64 = 12;
const SLOTS: u64 = 40;
const CODE_KEYS: u64 = 20;

/// The lengths of `Bytecodes` values: all past a 4 KiB page's inline limit, up to runs of 18
/// pages.
const CODE_SIZES: [usize; 6] = [1_500, 4_100, 9_000, 17_000, 40_000, 70_000];

/// The most rounds a child runs, should no kill come (it also stops once its stdout is closed).
const MAX_ROUNDS: u64 = 1_000_000;

/// One change of a round.
#[derive(Clone, Debug)]
enum Op {
    PutHeader(u64, B256),
    DelHeader(u64),
    /// Append a row through a cursor: a key past [`HEADER_KEYS`], after every earlier round's.
    AppendHeader(u64, B256),
    /// Put a row, replacing the row of the same slot.
    PutStorage(Address, StorageEntry),
    /// Append a row through the dup cursor (`append_dup`): a slot after every slot of the
    /// account.
    AppendStorage(Address, StorageEntry),
    /// Delete the row of a slot, if there is one.
    DelStorageRow(Address, B256),
    /// Delete every row of an account through the transaction.
    DelStorageKey(Address),
    /// Delete every row of an account through the dup cursor (`delete_current_duplicates`), if
    /// it has one.
    DelStorageDups(Address),
    /// Clear `PlainStorageState`.
    ClearStorage,
    PutCode(B256, Bytes),
    DelCode(B256),
    /// Write the progress row: the round.
    Progress(u64),
}

fn account(i: u64) -> Address {
    Address::with_last_byte(i as u8)
}

fn slot(i: u64) -> B256 {
    B256::with_last_byte(i as u8)
}

fn code_key(i: u64) -> B256 {
    B256::with_last_byte(i as u8)
}

/// The key of the progress row in `PlainAccountState`.
fn progress_key() -> Address {
    Address::repeat_byte(0xaa)
}

/// The progress row of round `round`.
fn progress_value(round: u64) -> Account {
    Account { nonce: round, balance: U256::from(round), bytecode_hash: None }
}

/// The code the `i`th `Bytecodes` put of round `round` writes: its length from [`CODE_SIZES`],
/// its bytes from the round and `i` (starting with `STOP`, so never an EOF or EIP-7702 prefix).
fn code(round: u64, i: u64) -> Bytes {
    let len = CODE_SIZES[((round + i) % CODE_SIZES.len() as u64) as usize];
    let mut x = round.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ i;
    let mut code: Vec<u8> = (0..len)
        .map(|_| {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (x >> 56) as u8
        })
        .collect();
    code[0] = 0;
    code[1..9].copy_from_slice(&round.to_be_bytes());
    Bytes::from(code)
}

/// The changes of round `round`, in order.
fn round_ops(round: u64) -> Vec<Op> {
    let r = round;
    let mut ops = Vec::new();
    if r.is_multiple_of(13) {
        ops.push(Op::ClearStorage);
    }
    let header = |i: u64| {
        let mut value = [0x5a; 32];
        value[..8].copy_from_slice(&r.to_be_bytes());
        value[8] = i as u8;
        B256::from(value)
    };
    for i in 0..6 {
        ops.push(Op::PutHeader((r * 7 + i * 31) % HEADER_KEYS, header(i)));
    }
    for i in 0..2 {
        ops.push(Op::AppendHeader(HEADER_KEYS + r * 2 + i, header(6 + i)));
    }
    for i in 0..6 {
        let entry = StorageEntry {
            key: slot((r * 5 + i * 7) % SLOTS),
            value: U256::from((r << 8) | i | 1),
        };
        ops.push(Op::PutStorage(account((r * 3 + i) % ACCOUNTS), entry));
    }
    // Slots led by the round, so after every slot of `slot` and of earlier rounds' appends.
    for i in 0..2 {
        let mut key = [0; 32];
        key[..8].copy_from_slice(&r.to_be_bytes());
        key[31] = i as u8;
        let entry = StorageEntry { key: B256::from(key), value: U256::from((r << 8) | i | 1) };
        ops.push(Op::AppendStorage(account((r * 5 + i) % ACCOUNTS), entry));
    }
    for i in 0..2 {
        ops.push(Op::PutCode(code_key((r * 3 + i) % CODE_KEYS), code(r, i)));
    }
    for j in 0..2 {
        ops.push(Op::DelHeader((r * 13 + j * 101) % HEADER_KEYS));
        ops.push(Op::DelStorageRow(
            account((r + j * 5) % ACCOUNTS),
            slot((r * 11 + j * 3) % SLOTS),
        ));
    }
    if r.is_multiple_of(7) {
        ops.push(Op::DelStorageKey(account(r % ACCOUNTS)));
    }
    if r % 5 == 2 {
        ops.push(Op::DelStorageDups(account((r * 2 + 1) % ACCOUNTS)));
    }
    ops.push(Op::DelCode(code_key((r * 7) % CODE_KEYS)));
    ops.push(Op::Progress(r));
    ops
}

/// Run a round's changes in a write transaction, as reth writes them.
fn write_round(tx: &impl DbTxMut, ops: &[Op]) -> Result<(), DatabaseError> {
    let mut headers = tx.cursor_write::<CanonicalHeaders>()?;
    let mut storage = tx.cursor_dup_write::<PlainStorageState>()?;
    for op in ops {
        match op {
            Op::PutHeader(k, v) => tx.put::<CanonicalHeaders>(*k, *v)?,
            Op::DelHeader(k) => drop(tx.delete::<CanonicalHeaders>(*k, None)?),
            Op::AppendHeader(k, v) => headers.append(*k, v)?,
            Op::PutStorage(address, entry) => {
                if storage
                    .seek_by_key_subkey(*address, entry.key)?
                    .is_some_and(|found| found.key == entry.key)
                {
                    storage.delete_current()?;
                }
                storage.upsert(*address, entry)?;
            }
            Op::AppendStorage(address, entry) => storage.append_dup(*address, *entry)?,
            Op::DelStorageRow(address, slot) => {
                if storage.seek_by_key_subkey(*address, *slot)?.is_some_and(|f| f.key == *slot) {
                    storage.delete_current()?;
                }
            }
            Op::DelStorageKey(address) => drop(tx.delete::<PlainStorageState>(*address, None)?),
            Op::DelStorageDups(address) => {
                if storage.seek_exact(*address)?.is_some() {
                    storage.delete_current_duplicates()?;
                }
            }
            Op::ClearStorage => tx.clear::<PlainStorageState>()?,
            Op::PutCode(k, code) => tx.put::<Bytecodes>(*k, Bytecode::new_raw(code.clone()))?,
            Op::DelCode(k) => drop(tx.delete::<Bytecodes>(*k, None)?),
            Op::Progress(round) => {
                tx.put::<PlainAccountState>(progress_key(), progress_value(*round))?
            }
        }
    }
    Ok(())
}

/// The progress the environment holds: the last round committed, 0 before the first.
fn progress(tx: &impl DbTx) -> u64 {
    tx.get::<PlainAccountState>(progress_key()).unwrap().map_or(0, |account| account.nonce)
}

/// Print one line to the parent and flush it. Ends the process once stdout is closed (the parent
/// is gone).
fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    if writeln!(out, "{MARK}{line}").and_then(|()| out.flush()).is_err() {
        std::process::exit(0);
    }
}

/// The child: write rounds until killed. Does nothing unless [`CHILD_ENV`] is set.
#[test]
fn crash_cycle_child() {
    let Some(dir) = std::env::var_os(CHILD_ENV) else { return };
    let mut db = super::open_env(Path::new(&dir), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    db.create_tables().unwrap();
    let start = progress(&db.tx().unwrap()) + 1;
    say(&format!("start {start}"));
    for round in start..start + MAX_ROUNDS {
        let tx = db.tx_mut().unwrap();
        write_round(&tx, &round_ops(round)).unwrap();
        say(&format!("commit {round}"));
        tx.commit().unwrap();
        say(&format!("round {round}"));
    }
}

/// The tables as rounds `1..=round` leave them.
#[derive(Debug, Default)]
struct Model {
    round: u64,
    headers: BTreeMap<u64, B256>,
    storage: BTreeMap<(Address, B256), U256>,
    codes: BTreeMap<B256, Bytes>,
}

impl Model {
    /// Apply the rounds after the model's, up to `round`.
    fn advance(&mut self, round: u64) {
        for r in self.round + 1..=round {
            for op in round_ops(r) {
                match op {
                    Op::PutHeader(k, v) | Op::AppendHeader(k, v) => drop(self.headers.insert(k, v)),
                    Op::DelHeader(k) => drop(self.headers.remove(&k)),
                    Op::PutStorage(address, entry) | Op::AppendStorage(address, entry) => {
                        self.storage.insert((address, entry.key), entry.value);
                    }
                    Op::DelStorageRow(address, slot) => drop(self.storage.remove(&(address, slot))),
                    Op::DelStorageKey(address) | Op::DelStorageDups(address) => {
                        self.storage.retain(|(a, _), _| *a != address);
                    }
                    Op::ClearStorage => self.storage.clear(),
                    Op::PutCode(k, code) => drop(self.codes.insert(k, code)),
                    Op::DelCode(k) => drop(self.codes.remove(&k)),
                    Op::Progress(_) => {}
                }
            }
        }
        self.round = self.round.max(round);
    }

    /// Assert that the environment holds exactly the model's rows, and `entries` counts them.
    fn assert_matches(&self, db: &TndbEnv) {
        let tx = db.tx().unwrap();
        let headers: BTreeMap<u64, B256> = walk::<CanonicalHeaders>(&tx).into_iter().collect();
        assert_same("CanonicalHeaders", &headers, &self.headers);
        let storage: BTreeMap<(Address, B256), U256> = walk::<PlainStorageState>(&tx)
            .into_iter()
            .map(|(address, entry)| ((address, entry.key), entry.value))
            .collect();
        assert_same("PlainStorageState", &storage, &self.storage);
        let codes: BTreeMap<B256, Bytes> = walk::<Bytecodes>(&tx)
            .into_iter()
            .map(|(k, code)| (k, Bytes::copy_from_slice(code.original_byte_slice())))
            .collect();
        assert_same("Bytecodes", &codes, &self.codes);
        let accounts = walk::<PlainAccountState>(&tx);
        let want: Vec<_> = (self.round > 0)
            .then(|| (progress_key(), progress_value(self.round)))
            .into_iter()
            .collect();
        assert_eq!(accounts, want, "PlainAccountState holds only the progress row");
        for (name, entries, rows) in [
            (CanonicalHeaders::NAME, tx.entries::<CanonicalHeaders>().unwrap(), headers.len()),
            (PlainStorageState::NAME, tx.entries::<PlainStorageState>().unwrap(), storage.len()),
            (Bytecodes::NAME, tx.entries::<Bytecodes>().unwrap(), codes.len()),
            (PlainAccountState::NAME, tx.entries::<PlainAccountState>().unwrap(), accounts.len()),
        ] {
            assert_eq!(entries, rows, "{name}: entries against the rows walked");
        }
    }
}

/// Every row of table `T`, in order.
fn walk<T: Table>(tx: &impl DbTx) -> Vec<(T::Key, T::Value)> {
    tx.cursor_read::<T>().unwrap().walk(None).unwrap().collect::<Result<_, _>>().unwrap()
}

/// Assert two maps are equal, naming the first row that differs.
fn assert_same<K: Ord + Debug, V: PartialEq + Debug>(
    table: &str,
    got: &BTreeMap<K, V>,
    want: &BTreeMap<K, V>,
) {
    if got == want {
        return;
    }
    let first = got
        .iter()
        .find(|(k, v)| want.get(k) != Some(v))
        .map(|(k, v)| format!("{k:?} holds {v:?}, model {:?}", want.get(k)))
        .or_else(|| {
            want.iter()
                .find(|(k, _)| !got.contains_key(k))
                .map(|(k, v)| format!("{k:?} missing, model {v:?}"))
        });
    panic!("{table}: {} rows, model {}; first difference: {first:?}", got.len(), want.len());
}

/// splitmix64: the kill points' randomness.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A line of the child's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Line {
    /// The child opened the environment and starts at this round.
    Start(u64),
    /// The child is about to commit this round.
    Commit(u64),
    /// This round's commit returned.
    Round(u64),
}

fn parse(line: &str) -> Option<Line> {
    let (_, rest) = line.split_once(MARK)?;
    let (what, n) = rest.trim().split_once(' ')?;
    let n = n.parse().ok()?;
    match what {
        "start" => Some(Line::Start(n)),
        "commit" => Some(Line::Commit(n)),
        "round" => Some(Line::Round(n)),
        _ => None,
    }
}

/// The child process, killed when dropped (so a failed assertion leaves no writer behind).
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Where a cycle kills the child.
#[derive(Clone, Copy, Debug)]
enum KillAt {
    /// This long after the spawn, whatever the child is doing (starting, opening, writing).
    After(Duration),
    /// This long after the child printed this many rounds (with 0, after it printed its start).
    Rounds(u64, Duration),
}

/// Where the kills landed, by the child's last line.
#[derive(Debug, Default)]
struct Landed {
    /// Before the child's first line: in its start, its open or its `create_tables`.
    open: u32,
    /// In a round's writes, before its commit.
    writes: u32,
    /// In a commit, which the reopen rolled back.
    commit_lost: u32,
    /// In a commit, or right after it returned, which the reopen kept.
    commit_kept: u32,
}

/// Run `cycles` crash cycles in a fresh directory. Returns the final progress and where the
/// kills landed.
fn crash_cycles(cycles: u32) -> (u64, Landed) {
    let seed = std::env::var(SEED_ENV)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64);
    eprintln!(
        "crash cycles: {cycles} cycles, seed {seed} (set {SEED_ENV} to repeat the kill points)"
    );
    let mut rng = Rng(seed);
    let dir = tempfile::tempdir().unwrap();
    // The tables exist before the first child, so a lost commit shows as lost progress.
    super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default())
        .and_then(|mut db| db.create_tables())
        .unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut model = Model::default();
    let mut landed = Landed::default();
    // How long the child's rounds take, from the last cycle that printed two rounds or more.
    let mut round_time = Duration::from_millis(2);
    for cycle in 0..cycles {
        // A kill after some rounds waits up to one and a half rounds more, so it lands anywhere
        // in a round: its writes, its commit, or between the commit and the line saying so. The
        // first cycle always lets the child commit, so every run has progress to check.
        let kill_at = match rng.below(6) {
            0 if cycle > 0 => KillAt::After(Duration::from_micros(rng.below(30_000))),
            _ => KillAt::Rounds(
                rng.below(11).max(u64::from(cycle == 0)),
                round_time.mul_f64(1.5 * rng.below(1_000) as f64 / 1_000.0),
            ),
        };
        let mut child = ChildGuard(
            super::spawn(
                Command::new(&exe)
                    .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads", "1"])
                    .env(CHILD_ENV, dir.path())
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit()),
            )
            .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (send, lines) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if let Some(line) = parse(&line.unwrap()) {
                    let _ = send.send((line, Instant::now()));
                }
            }
        });
        let mut seen = Vec::new();
        match kill_at {
            KillAt::After(delay) => thread::sleep(delay),
            KillAt::Rounds(rounds, delay) => {
                let wanted = |seen: &[(Line, Instant)]| match (rounds, seen.last()) {
                    (0, Some(_)) => true,
                    (n, _) => {
                        seen.iter().filter(|(l, _)| matches!(l, Line::Round(_))).count() as u64 >= n
                    }
                };
                while !wanted(&seen) {
                    match lines.recv() {
                        Ok(line) => seen.push(line),
                        Err(_) => panic!(
                            "cycle {cycle}: the child exited before the kill ({:?}), after {seen:?}",
                            child.0.wait()
                        ),
                    }
                }
                thread::sleep(delay);
            }
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        reader.join().unwrap();
        seen.extend(lines.try_iter());
        let rounds: Vec<Instant> =
            seen.iter().filter(|(l, _)| matches!(l, Line::Round(_))).map(|(_, t)| *t).collect();
        if let [first, .., last] = rounds[..] {
            round_time = (last - first) / (rounds.len() - 1) as u32;
        }
        let seen: Vec<Line> = seen.into_iter().map(|(line, _)| line).collect();

        // The progress the reopen must find: the round the child last printed as committed,
        // or, killed in a commit, that round or the one before.
        let before = model.round;
        if let Some(Line::Start(start)) = seen.first() {
            assert_eq!(*start, before + 1, "cycle {cycle}: the child resumes after the progress");
        }
        let allowed = match seen.last() {
            None | Some(Line::Start(_)) => before..=before,
            Some(Line::Round(n)) => *n..=*n,
            Some(Line::Commit(n)) => n - 1..=*n,
        };

        let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default())
            .unwrap_or_else(|e| panic!("cycle {cycle} ({kill_at:?}): reopen failed: {e:?}"));
        assert_eq!(db.failure(), None, "cycle {cycle}: a failure latched at reopen");
        let got = progress(&db.tx().unwrap());
        assert!(
            allowed.contains(&got),
            "cycle {cycle} ({kill_at:?}): progress {got}, allowed {allowed:?} (lines {seen:?})"
        );
        match seen.last() {
            None => landed.open += 1,
            Some(Line::Start(_) | Line::Round(_)) => landed.writes += 1,
            Some(Line::Commit(n)) if got == *n => landed.commit_kept += 1,
            Some(Line::Commit(_)) => landed.commit_lost += 1,
        }
        model.advance(got);
        model.assert_matches(&db);
        db.env().check().unwrap_or_else(|e| panic!("cycle {cycle}: check failed: {e:?}"));
        assert_eq!(db.failure(), None, "cycle {cycle}: a failure latched");
    }
    (model.round, landed)
}

/// Run `cycles` crash cycles and print the outcome. With `in_commit`, assert that a kill landed
/// in a commit (the child's last line `commit <round>`), so that a change in how long commits take
/// against the rest of a round cannot leave the commit untested; without it (too few cycles for
/// that to be sure) only warn.
fn run_crash_cycles(cycles: u32, in_commit: bool) {
    let started = Instant::now();
    let (progress, landed) = crash_cycles(cycles);
    eprintln!("{cycles} cycles: progress {progress}, kills {landed:?}, {:?}", started.elapsed());
    assert!(progress > 0, "no round committed");
    let commit_kills = landed.commit_lost + landed.commit_kept;
    if in_commit {
        assert!(commit_kills > 0, "no kill landed in a commit: {landed:?}");
    } else if commit_kills == 0 {
        eprintln!("warning: no kill landed in a commit in {cycles} cycles");
    }
}

#[test]
fn killed_child_reopens_to_its_last_commit() {
    run_crash_cycles(5, false);
}

#[test]
#[ignore = "heavy: run with --release"]
fn killed_child_reopens_to_its_last_commit_heavy() {
    run_crash_cycles(200, true);
}
