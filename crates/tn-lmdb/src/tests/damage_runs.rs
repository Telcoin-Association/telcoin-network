//! Damaged overflow runs: table `o` holds inline values and values in runs of 1, 3 and 9 pages,
//! and each kind of damage to a run, to a cell naming one, to the table's overflow counter or to a
//! free record listing a run's page is met as `Error::Corrupt` by the first use that reaches it
//! ([`sweep`]), which latches the environment. A read checks a run's header against the cell that
//! names it (page number, kind, txn identity, length and the tree tag of the leaf holding the
//! cell) and verifies the CRC of a run of more than one page; `Env::check` verifies every run's
//! CRC, a 1-page run's included, and compares the counter with the runs the leaves name, as
//! `drop_table` and `clear_table` do.

use std::{collections::BTreeMap, fs, os::unix::fs::FileExt as _};

use super::{
    damage::{
        self, children, list_in_free_record, read_page, restamped, set_child, sweep, tree,
        write_page, Stage, P,
    },
    *,
};
use crate::{
    crc,
    env::tests::flip,
    overflow::{run_pages, Ovf, RUN_HEADER},
    page,
    tree::{cursor, PageRead as _},
    Error,
};

/// Table `o`'s entries: keys `0..ENTRIES`, inline values of 100 bytes but for [`RUNS`].
const ENTRIES: u32 = 30;

/// The keys of `o` whose values live in runs, with the values' lengths (at 4 KiB pages).
const RUNS: [(u32, usize); 7] = [
    (1, 3000),   // 1 page
    (2, 10_000), // 3 pages
    (3, 35_000), // 9 pages
    (4, 11_000), // 3 pages, another length
    (6, 3500),   // 1 page
    (7, 34_000), // 9 pages
    (8, 10_000), // 3 pages, rewritten by the last commit with a value of the same length
];

/// The key the last commit rewrites, so its first run is a stale copy.
const REWRITTEN: u32 = 8;

/// The keys of `o` whose runs table `p` has a twin of (the same commit, the same length): a 1-page
/// run and a 3-page one.
const TWINS: [u32; 2] = [1, 2];

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A cleanly closed file to damage: table `o` (one leaf; every run written by the first commit
/// but [`REWRITTEN`]'s), table `p` (the first commit too: a run of each of [`TWINS`]' lengths,
/// so `o` has a run of another table of the same txn and length), a dropped table's pages in a
/// free record that passes the reuse gate once reopened, and `stale`: the first page of
/// [`REWRITTEN`]'s old run, intact and of the same length as its new one. No run key is one
/// [`sweep`]'s writes rewrite (every fifth key from 0).
struct Fixture {
    dir: TempDir,
    path: PathBuf,
    state: State,
    stale: u32,
}

fn fixture() -> Fixture {
    let (dir, path) = temp("tn_lmdb_damage_runs");
    let env = open(&path, SyncMode::Durable);
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..ENTRIES {
        let len = RUNS.iter().find(|r| r.0 == i).map_or(100, |r| r.1);
        put(&mut txn, &mut state, "o", &key(i), &value(1, i, len));
    }
    for k in TWINS {
        let len = RUNS.iter().find(|r| r.0 == k).expect("a run key").1;
        put(&mut txn, &mut state, "p", &key(k), &value(4, k, len));
    }
    for i in 0..400 {
        put(&mut txn, &mut state, "big", &key(i), &value(3, i, 900));
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    drop_table(&mut txn, &mut state, "big");
    txn.commit().expect("commit");
    let stale = cells(&env)[&REWRITTEN].0.pgno;
    let mut txn = env.begin_rw().expect("begin");
    let len = RUNS.iter().find(|r| r.0 == REWRITTEN).expect("a run key").1;
    put(&mut txn, &mut state, "o", &key(REWRITTEN), &value(2, REWRITTEN, len));
    txn.commit().expect("commit");
    assert_ne!(cells(&env)[&REWRITTEN].0.pgno, stale, "a new run");
    check(&env);
    drop(env);
    Fixture { dir, path, state, stale }
}

/// The overflow cells of `o`'s one leaf in the newest state of `env`, by key: each cell's
/// reference and the offset of its 12 value bytes in the leaf.
fn cells(env: &Env) -> BTreeMap<u32, (Ovf, usize)> {
    cells_of(env, "o")
}

/// [`cells`] of table `name`'s one leaf.
fn cells_of(env: &Env, name: &str) -> BTreeMap<u32, (Ovf, usize)> {
    let o = tree(env, name);
    assert_eq!(o.height, 1, "{o:?}");
    let snapshot = env.inner().current();
    let leaf = snapshot.page_any(o.root).expect("leaf");
    let mut cells = BTreeMap::new();
    for i in 0..page::nkeys(leaf) {
        let (k, val) = page::leaf_entry(leaf, i).expect("entry");
        if let page::Val::Overflow(ovf) = val {
            let slot = page::HEADER + 2 * i;
            let cell = usize::from(u16::from_le_bytes([leaf[slot], leaf[slot + 1]]));
            let klen = usize::from(u16::from_le_bytes([leaf[cell + 4], leaf[cell + 5]]));
            let at = cell + page::LEAF_CELL_HEADER + klen;
            assert_eq!(Ovf::decode(leaf[at..at + 12].try_into().expect("12 bytes"), ovf.tag), ovf);
            cells.insert(u32::from_be_bytes(k.try_into().expect("a 4-byte key")), (ovf, at));
        }
    }
    cells
}

/// Where the damage goes, found in the fixture's newest state.
#[derive(Debug)]
struct Places {
    /// `o`'s leaf and its overflow cells ([`cells`]).
    leaf: u32,
    cells: BTreeMap<u32, (Ovf, usize)>,
    /// The file offset of `o`'s overflow counter in its catalog record.
    counter: u64,
    /// The free tree's one leaf (its first record passes the reuse gate).
    free_leaf: u32,
}

impl Places {
    /// The reference to key `k`'s run.
    fn ovf(&self, k: u32) -> Ovf {
        self.cells[&k].0
    }

    /// The pages of key `k`'s run.
    fn pages(&self, k: u32) -> u32 {
        run_pages(self.ovf(k).len as usize, P as usize) as u32
    }
}

fn places(path: &Path) -> Places {
    let env = open(path, SyncMode::Durable);
    let cells = cells(&env);
    assert_eq!(cells.len(), RUNS.len());
    let pages: Vec<usize> = RUNS
        .iter()
        .map(|&(k, len)| {
            assert_eq!(cells[&k].0.len as usize, len);
            run_pages(len, P as usize)
        })
        .collect();
    assert_eq!(pages, [1, 3, 9, 3, 1, 9, 3]);
    let snapshot = env.inner().current();
    let meta = snapshot.meta;
    assert_eq!((meta.catalog.height, meta.free.height), (1, 1), "{meta:?}");
    let catalog = snapshot.page_any(meta.catalog.root).expect("catalog");
    let record = (0..page::nkeys(catalog))
        .map(|i| page::leaf_inline_entry(catalog, i).expect("entry"))
        .find(|(k, _)| *k == b"o")
        .expect("o's record")
        .1;
    // The record's tree root starts at byte 8; its overflow counter at byte 12 of the root.
    let counter = u64::from(meta.catalog.root) * P
        + (record.as_ptr() as u64 - catalog.as_ptr() as u64)
        + 8
        + 12;
    Places { leaf: tree(&env, "o").root, cells, counter, free_leaf: meta.free.root }
}

/// Edit the run `ovf` names with `edit`, then (with `restamp`) stamp its one CRC again: damage
/// only the checks under test can find.
fn edit_run(path: &Path, ovf: Ovf, restamp: bool, edit: impl FnOnce(&mut [u8])) {
    let n = run_pages(ovf.len as usize, P as usize);
    let file = fs::OpenOptions::new().read(true).write(true).open(path).expect("open");
    let start = u64::from(ovf.pgno) * P;
    let mut run = vec![0; n * P as usize];
    file.read_exact_at(&mut run, start).expect("read");
    edit(&mut run);
    if restamp {
        crc::add_crc32_nonzero(&mut run);
    }
    file.write_all_at(&run, start).expect("write");
}

/// Rewrite key `k`'s overflow cell in `o`'s leaf as `ovf`, the leaf's CRC stamped again.
fn set_cell(path: &Path, at: &Places, k: u32, ovf: Ovf) {
    let off = at.cells[&k].1;
    restamped(path, at.leaf, |page| page[off..off + 12].copy_from_slice(&ovf.encode()));
}

/// Add one to `o`'s overflow counter in its catalog record, the catalog leaf's CRC stamped again
/// (a counter [`crate::tree::TreeRoot::decode`] accepts).
fn bump_counter(path: &Path, at: &Places) {
    set_counter(path, at, |counter| counter + 1);
}

/// Set `o`'s overflow counter in its catalog record to `to` of its value, the catalog leaf's CRC
/// stamped again.
fn set_counter(path: &Path, at: &Places, to: impl FnOnce(u32) -> u32) {
    let file = fs::File::open(path).expect("open");
    let mut b = [0; 4];
    file.read_exact_at(&mut b, at.counter).expect("read");
    let counter = u32::from_le_bytes(b);
    assert_eq!(counter, RUNS.iter().map(|r| run_pages(r.1, P as usize) as u32).sum());
    let (pgno, off) = ((at.counter / P) as u32, (at.counter % P) as usize);
    restamped(path, pgno, |page| page[off..off + 4].copy_from_slice(&to(counter).to_le_bytes()));
}

/// A copy of the fixture's file, damaged by `damage`, opened (the open changing nothing).
fn damaged(fx: &Fixture, what: &str, damage: &dyn Fn(&Path)) -> (PathBuf, Env) {
    let path = fx.dir.path().join(what);
    fs::copy(&fx.path, &path).expect("copy");
    damage(&path);
    let before = fs::read(&path).expect("read");
    let env = open(&path, SyncMode::Durable);
    assert!(fs::read(&path).expect("read") == before, "{what}: the open changed the file");
    (path, env)
}

/// Each kind of damage to a run or to a cell naming one is `Corrupt` at the first use that meets
/// it, and latches: at a get for any damage a read checks (a run of more than one page verifies
/// its CRC; every run's header must match its cell), at `Env::check` for a flipped byte inside a
/// 1-page run (a read skips its CRC, as it skips a leaf's) and for an overflow counter that is
/// not what the leaves name (reads and puts never compare it; open checks only the catalog and
/// free-tree roots).
#[test]
fn damaged_runs_are_corrupt_at_the_first_use_that_meets_them() {
    let fx = fixture();
    let at = places(&fx.path);
    let (two, three, other3, rewritten) = (at.ovf(2), at.ovf(3), at.ovf(4), at.ovf(REWRITTEN));
    let stale: Vec<u8> =
        (0..at.pages(REWRITTEN)).flat_map(|i| read_page(&fx.path, fx.stale + i)).collect();
    assert!(crc::crc_state(&stale) == crc::CrcState::Valid, "the stale copy is intact");
    assert_eq!(&stale[16..20], &rewritten.len.to_le_bytes(), "of the same length");
    assert_eq!((two.txn, other3.txn), (three.txn, three.txn), "written by one txn");
    assert_eq!(at.pages(2), at.pages(4), "same pages, another length");
    assert!(rewritten.txn != two.txn, "the rewrite is another txn's run");
    let one = at.ovf(1);
    let page_of = |ovf: Ovf, i: u32| u64::from(ovf.pgno + i) * P;

    type Damage<'a> = (&'a str, Stage, Box<dyn Fn(&Path) + 'a>);
    let cases: Vec<Damage<'_>> = vec![
        // A flipped byte in a middle page (which has no header): only the CRC tells.
        ("3-page middle flip", Stage::Get, Box::new(|p| flip(p, page_of(two, 1) + 100))),
        ("9-page middle flip", Stage::Get, Box::new(|p| flip(p, page_of(three, 4) + 2000))),
        // A zeroed last page (the trailer zero: a run being written), and a zeroed middle page.
        (
            "3-page zeroed last page",
            Stage::Get,
            Box::new(|p| write_page(p, two.pgno + 2, &vec![0; P as usize])),
        ),
        (
            "9-page zeroed last page",
            Stage::Get,
            Box::new(|p| write_page(p, three.pgno + 8, &vec![0; P as usize])),
        ),
        (
            "9-page zeroed middle page",
            Stage::Get,
            Box::new(|p| write_page(p, at.ovf(7).pgno + 4, &vec![0; P as usize])),
        ),
        // A cell naming another run of the same txn (the identity check passes): of the same page
        // count, so only the length tells, and of another page count.
        (
            "redirected, same pages",
            Stage::Get,
            Box::new(|p| set_cell(p, &at, 2, Ovf { pgno: other3.pgno, ..two })),
        ),
        (
            "redirected, more pages",
            Stage::Get,
            Box::new(|p| set_cell(p, &at, 2, Ovf { pgno: three.pgno, ..two })),
        ),
        // A cell naming its key's older run, of the same length: only the txn identity tells.
        (
            "stale copy",
            Stage::Get,
            Box::new(|p| set_cell(p, &at, REWRITTEN, Ovf { pgno: fx.stale, ..rewritten })),
        ),
        // A cell's length changed within the same page count: only the run's length tells.
        (
            "cell length",
            Stage::Get,
            Box::new(|p| set_cell(p, &at, 2, Ovf { len: two.len + 1, ..two })),
        ),
        // The run's kind byte changed, the CRC stamped again.
        ("9-page kind", Stage::Get, Box::new(|p| edit_run(p, three, true, |r| r[4] = 1))),
        // A 1-page run's header: its page number, its kind (CRC stamped again).
        (
            "1-page pgno",
            Stage::Get,
            Box::new(|p| edit_run(p, one, true, |r| r[0] = r[0].wrapping_add(1))),
        ),
        ("1-page kind", Stage::Get, Box::new(|p| edit_run(p, one, true, |r| r[4] = 2))),
        // A flipped byte in a 1-page run's value: read as stored, found by the check.
        (
            "1-page value flip",
            Stage::Check,
            Box::new(|p| flip(p, page_of(one, 0) + RUN_HEADER as u64 + 100)),
        ),
        // `o`'s overflow counter one too high, in a catalog leaf whose CRC is stamped again.
        ("overflow counter", Stage::Check, Box::new(|p| bump_counter(p, &at))),
    ];
    for (what, stage, damage) in &cases {
        let (path, env) = damaged(&fx, what, damage);
        sweep(&env, &fx.state, what, *stage);
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}

/// A cell of `o` redirected to a run of table `p` that the same commit wrote, of the same length:
/// the run passes its range, header, identity, length and CRC checks, so only the tree tag in its
/// header tells it is another tree's. A get through the cell and a scan reaching it are
/// `Corrupt`, naming both tags, and latch; a delete of the key (the walk that frees the run,
/// which skips the CRC) is `Corrupt` too; `Env::check` refuses the file. For a 1-page run and a
/// 3-page one.
#[test]
fn a_cell_redirected_to_another_tables_run_of_its_commit_is_corrupt_at_the_read() {
    let fx = fixture();
    let at = places(&fx.path);
    let (twins, tags) = {
        let env = open(&fx.path, SyncMode::Durable);
        (cells_of(&env, "p"), (tree(&env, "p").tag, tree(&env, "o").tag))
    };
    assert_ne!(tags.0, tags.1, "two tables, two tags");
    for k in TWINS {
        let (mine, twin) = (at.ovf(k), twins[&k].0);
        assert_eq!((mine.txn, mine.len), (twin.txn, twin.len), "key {k}: one commit, one length");
        assert_ne!(mine.pgno, twin.pgno);
        let forged = Ovf { pgno: twin.pgno, ..mine };
        let what = format!("key {k} redirected to p's run");
        let damage = |p: &Path| set_cell(p, &at, k, forged);
        let want = format!(
            "overflow run {} (len {}, txn {}): belongs to the tree tagged {:#x}, not to {:#x}",
            twin.pgno, twin.len, twin.txn, tags.0, tags.1
        );

        let (path, env) = damaged(&fx, &what, &damage);
        let ro = env.begin_ro().expect("ro");
        let o = ro.open_table("o").expect("open").expect("o");
        let got = ro.get(&o, &key(k)).map(|v| v.map(<[u8]>::len));
        assert!(matches!(&got, Err(Error::Corrupt(m)) if m == &want), "{what}: get: {got:?}");
        let mut cursor = ro.cursor(&o).expect("cursor");
        let mut seen = 0;
        let scan = loop {
            match cursor.next() {
                Ok(Some(_)) => seen += 1,
                Ok(None) => panic!("{what}: the scan ended after {seen} entries"),
                Err(e) => break e,
            }
        };
        assert!(matches!(&scan, Error::Corrupt(m) if m == &want), "{what}: scan: {scan}");
        assert_eq!(seen, k as usize, "{what}: the scan met the redirect at key {k}");
        drop(ro);
        assert!(env.failure().is_some(), "{what}: latched");
        assert!(
            matches!(env.check(), Err(Error::Corrupt(m)) if m.contains(&want)),
            "{what}: check"
        );
        drop(env);
        fs::remove_file(&path).expect("remove");

        let (path, env) = damaged(&fx, &format!("{what}, deleted"), &damage);
        let mut txn = env.begin_rw().expect("begin");
        let o = txn.open_table("o").expect("open").expect("o");
        let got = txn.delete(&o, &key(k));
        assert!(matches!(&got, Err(Error::Corrupt(m)) if m == &want), "{what}: delete: {got:?}");
        drop(txn);
        assert!(env.failure().is_some(), "{what}: the delete latched");
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}

/// `o`'s overflow counter one too high refuses `drop_table` and `clear_table` as `Corrupt`, which
/// latches: the walk that collects the table's pages to free (`tree_pages`) adds up the runs its
/// leaves name and compares the sum with the counter before anything is freed. This is the first
/// operation after `Env::check` to meet it
/// ([`damaged_runs_are_corrupt_at_the_first_use_that_meets_them`]).
#[test]
fn a_damaged_overflow_counter_refuses_drop_and_clear() {
    let fx = fixture();
    let at = places(&fx.path);
    let pages: usize = RUNS.iter().map(|r| run_pages(r.1, P as usize)).sum();
    let want = format!("the leaves name {pages} overflow pages, the handle counts {}", pages + 1);
    for dropping in [false, true] {
        let what = if dropping { "drop" } else { "clear" };
        let (path, env) = damaged(&fx, what, &|p| bump_counter(p, &at));
        let mut txn = env.begin_rw().expect("begin");
        let o = txn.open_table("o").expect("open").expect("o");
        let got = if dropping { txn.drop_table(o) } else { txn.clear_table(&o) };
        match got {
            Err(Error::Corrupt(e)) => assert!(e.contains(&want), "{what}: {e}"),
            other => panic!("{what}: {other:?}"),
        }
        drop(txn);
        assert!(env.failure().is_some(), "{what}: the damage latched");
        assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "{what}: writes fail");
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}

/// `o`'s overflow counter zero, its runs still named by its leaf, refuses `drop_table` and
/// `clear_table` as `Corrupt`, naming the first run, which latches: the free walk of a tree
/// whose handle counts no overflow page reads every leaf's cells all the same, so a miscounted
/// handle cannot leak the runs it does not count.
#[test]
fn a_zero_overflow_counter_over_runs_refuses_drop_and_clear() {
    let fx = fixture();
    let at = places(&fx.path);
    let first = at.cells.values().map(|c| c.0.pgno).min().expect("a run");
    let want = format!("names overflow run {first} in a tree whose handle counts no overflow page");
    for dropping in [false, true] {
        let what = if dropping { "drop, zero counter" } else { "clear, zero counter" };
        let (path, env) = damaged(&fx, what, &|p| set_counter(p, &at, |_| 0));
        let mut txn = env.begin_rw().expect("begin");
        let o = txn.open_table("o").expect("open").expect("o");
        let got = if dropping { txn.drop_table(o) } else { txn.clear_table(&o) };
        match got {
            Err(Error::Corrupt(e)) => assert!(e.contains(&want), "{what}: {e}"),
            other => panic!("{what}: {other:?}"),
        }
        drop(txn);
        assert!(env.failure().is_some(), "{what}: the damage latched");
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}

/// A branch cell of table `t` redirected to a leaf of table `u` that the same commit wrote (the
/// damage of `damage::a_child_redirected_to_another_tables_page_of_its_commit_is_corrupt_at_the_fetch`)
/// refuses `drop_table` as `Corrupt`, naming both tags, and latches, so no page of `u`'s is
/// freed: the free walk reads every leaf with a descent's checks, a tree without runs included,
/// and `u` reads whole after the refusal.
#[test]
fn a_drop_through_a_leaf_redirected_to_another_table_frees_nothing() {
    let fx = damage::fixture();
    let at = damage::places(&fx.path);
    let (cell, _) = at.same_txn;
    let (target, tags) = {
        let env = open(&fx.path, SyncMode::Durable);
        let (t, u) = (tree(&env, "t"), tree(&env, "u"));
        let (target, txn) = *children(&env, u.root).last().expect("u's last leaf");
        assert_eq!(txn, children(&env, at.t_branch)[cell].1, "one commit");
        (target, (u.tag, t.tag))
    };
    restamped(&fx.path, at.t_branch, |b| set_child(b, cell, target));
    let want =
        format!("page {target} belongs to the tree tagged {:#x}, not to {:#x}", tags.0, tags.1);
    let env = open(&fx.path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.open_table("t").expect("open").expect("t");
    let got = txn.drop_table(t);
    assert!(matches!(&got, Err(Error::Corrupt(m)) if m == &want), "drop: {got:?}");
    drop(txn);
    assert!(env.failure().is_some(), "latched");
    let ro = env.begin_ro().expect("ro");
    let u = ro.open_table("u").expect("open").expect("u");
    let mut cursor = ro.cursor(&u).expect("cursor");
    let mut rows = Rows::new();
    while let Some((k, v)) = cursor.next().expect("u reads whole") {
        rows.insert(k.to_vec(), v.to_vec());
    }
    assert_eq!(rows, fx.state["u"], "u is untouched");
}

/// `RwCursor::delete_current` reads the key of the entry it deletes, not its value, as
/// `RwTxn::delete` reads none: a middle page of key 2's run flipped under the cursor (after the
/// seek that read the value) is never met, the delete moves the cursor to key 3, the commit frees
/// the run and the file checks clean. A key read that fails (`o`'s leaf flipped under the cursor
/// to name another page) is `Corrupt`, latches, and leaves the cursor unset, as a failed delete
/// leaves it.
#[test]
fn a_cursor_delete_reads_the_key_alone() {
    let fx = fixture();
    let at = places(&fx.path);
    let two = at.ovf(2);
    let begin = |env: &Env| {
        let txn = env.begin_rw().expect("begin");
        let o = txn.open_table("o").expect("open").expect("o");
        (txn, o)
    };

    let (path, env) = damaged(&fx, "run under a cursor", &|_| {});
    let (mut txn, o) = begin(&env);
    let mut c = txn.cursor_mut(&o).expect("cursor");
    assert!(c.seek_exact(&key(2)).expect("seek").is_some());
    flip(&path, u64::from(two.pgno + 1) * P + 100);
    assert!(c.delete_current().expect("the delete reads no value"));
    assert_eq!(c.current().expect("current").map(|(k, _)| k.to_vec()), Some(key(3).to_vec()));
    drop(c);
    txn.commit().expect("commit");
    let mut state = fx.state.clone();
    state.get_mut("o").expect("o").remove(&key(2)[..]).expect("key 2");
    assert_eq!(newest_state(&env), state);
    drop(env);
    fs::remove_file(&path).expect("remove");

    let (path, env) = damaged(&fx, "leaf under a cursor", &|_| {});
    let (mut txn, o) = begin(&env);
    let mut c = txn.cursor_mut(&o).expect("cursor");
    assert!(c.seek_exact(&key(2)).expect("seek").is_some());
    flip(&path, u64::from(at.leaf) * P);
    match c.delete_current() {
        Err(Error::Corrupt(e)) => assert!(e.contains(&format!("page {} holds", at.leaf)), "{e}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(c.state(), (cursor::State::Unset, false), "a failed delete leaves it unset");
    drop(c);
    drop(txn);
    assert!(env.failure().is_some(), "the damage latched");
    drop(env);
    fs::remove_file(&path).expect("remove");
}

/// A free record that passes the reuse gate and lists a middle page of a live 9-page run (its
/// leaf's CRC stamped again) is `Corrupt` wherever it is met, never wrong data:
/// - the sweep meets it at `Env::check` (the run's page is claimed twice once nothing reused it, or
///   the run fails its CRC once something did);
/// - a write that takes the page from the pool overwrites the run: the commit succeeds (nothing can
///   tell yet), a get of the run's key fails the run's CRC, every other key reads back, and the
///   check fails;
/// - a write that replaces the run's value while the page sits in the pool frees the page out of
///   the published tree: the commit finds it both free and freed (`Allocator::reusable`), which is
///   damage, not a broken invariant.
#[test]
fn a_free_record_listing_a_live_run_page_is_corrupt_never_wrong_data() {
    let fx = fixture();
    let at = places(&fx.path);
    let run = at.ovf(3);
    let middle = run.pgno + 4;
    let damage = |p: &Path| list_in_free_record(p, at.free_leaf, middle);

    let (path, env) = damaged(&fx, "sweep", &damage);
    sweep(&env, &fx.state, "free record", Stage::Check);
    drop(env);
    fs::remove_file(&path).expect("remove");

    let (path, env) = damaged(&fx, "reused", &damage);
    let mut txn = env.begin_rw().expect("begin");
    let w = txn.create_table("w").expect("w");
    let mut i = 0;
    while !txn.writer_mut().alloc.is_private(middle) {
        assert!(i < 2000, "the run's page was never handed out");
        txn.put(&w, &key(i), &value(9, i, 900)).expect("put");
        i += 1;
    }
    txn.commit().expect("the commit cannot tell");
    let ro = env.begin_ro().expect("ro");
    let o = ro.open_table("o").expect("open").expect("o");
    for (k, want) in &fx.state["o"] {
        match ro.get(&o, k) {
            Ok(got) => assert!(got == Some(&want[..]), "key {k:?}: the value as written"),
            Err(Error::Corrupt(e)) => {
                assert_eq!(&k[..], key(3), "{e}");
                assert!(e.contains(&format!("overflow run {} ", run.pgno)), "{e}");
                assert!(e.contains("fails its CRC"), "{e}");
            }
            Err(e) => panic!("key {k:?}: {e}"),
        }
    }
    drop(ro);
    assert!(env.failure().is_some(), "the get latched");
    assert!(matches!(env.check(), Err(Error::Corrupt(_))));
    drop(env);
    fs::remove_file(&path).expect("remove");

    let (path, env) = damaged(&fx, "freed", &damage);
    let mut txn = env.begin_rw().expect("begin");
    let o = txn.open_table("o").expect("open").expect("o");
    txn.put(&o, &key(3), &value(5, 3, 20_000)).expect("put");
    assert!(txn.writer_mut().alloc.pool().contains(&middle), "the page is still in the pool");
    match txn.commit() {
        Err(Error::Corrupt(e)) => {
            assert!(e.contains(&format!("page {middle} is listed free, yet a published tree")))
        }
        other => panic!("{other:?}"),
    }
    assert!(env.failure().is_some(), "the commit latched");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "writes fail");
    drop(env);
    fs::remove_file(&path).expect("remove");
}

/// Two cells of `o` naming one published run: key 4's cell, CRC-valid, made a copy of key 2's
/// reference (same page, txn and length, so every check a free makes of the run passes for both
/// cells). Deleting both keys in one transaction frees the run's pages twice, which the commit
/// finds (`Allocator::reusable`): `Corrupt`, latched as damage, since only two pointers naming one
/// page can free it twice, and the file keeps its last committed state.
#[test]
fn two_cells_naming_one_run_freed_together_are_corrupt_at_commit() {
    let fx = fixture();
    let at = places(&fx.path);
    let two = at.ovf(2);
    let (path, env) = damaged(&fx, "twice", &|p| set_cell(p, &at, 4, two));
    let mut txn = env.begin_rw().expect("begin");
    let o = txn.open_table("o").expect("open").expect("o");
    assert!(txn.delete(&o, &key(2)).expect("delete 2"));
    assert!(txn.delete(&o, &key(4)).expect("delete 4"));
    let want = format!("page {} left the published trees 2 times", two.pgno);
    match txn.commit() {
        Err(Error::Corrupt(e)) => assert!(e.contains(&want), "{e}"),
        other => panic!("{other:?}"),
    }
    let failure = env.failure().expect("the commit latched");
    assert!(failure.contains(&want), "latched as damage: {failure}");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "writes fail");
    drop(env);
    let env = open(&path, SyncMode::Durable);
    let ro = env.begin_ro().expect("ro");
    let o = ro.open_table("o").expect("open").expect("o");
    assert!(ro.get(&o, &key(2)).expect("get").is_some(), "the delete never committed");
    drop(ro);
    drop(env);
    fs::remove_file(&path).expect("remove");
}
