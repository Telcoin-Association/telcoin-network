//! Damaged files: each kind of damage at rest either fails the open closed (the file unchanged)
//! or, once opened (the open changing nothing), is met by the operations that reach it — reads,
//! a write transaction's copies and free-list loads, `Env::check` — as `Error::Corrupt`, which
//! latches the environment. Nothing panics on any of it.

use std::{fs, os::unix::fs::FileExt as _};

use super::*;
use crate::{
    catalog, crc,
    env::tests::{assert_fails_closed, flip},
    meta::{Meta, META_LEN},
    page,
    tree::{PageRead as _, TreeRoot},
    Error,
};

const P: u64 = 4096;

/// A long key, so table `t`'s branches hold few children and the tree has three levels.
fn long_key(i: u32) -> Vec<u8> {
    let mut key = format!("{i:06}").into_bytes();
    key.resize(180, b'k');
    key
}

/// A long table name, so the catalog has two levels.
fn table_name(i: u32) -> String {
    format!("{i:03}{}", "n".repeat(197))
}

/// A cleanly closed file to damage: table `t` (three levels), `u`, forty tables with long names
/// (a two-level catalog), a dropped table's pages in a two-level free tree whose first records
/// pass the reuse gate once reopened, and `stale`: a page holding an older copy of `t`'s first
/// leaf, still intact.
struct Fixture {
    dir: TempDir,
    path: PathBuf,
    state: State,
    stale: u32,
}

/// Table `name`'s tree in the newest state of `env`.
fn tree(env: &Env, name: &str) -> TreeRoot {
    let snapshot = env.inner().current();
    catalog::get(&*snapshot, &snapshot.meta.catalog, name).expect("get").expect("table").root
}

/// The children `(pgno, txn)` of branch page `pgno` in the newest state of `env`.
fn children(env: &Env, pgno: u32) -> Vec<(u32, u32)> {
    let snapshot = env.inner().current();
    let page = snapshot.page(pgno).expect("branch");
    (0..page::nkeys(page)).map(|i| page::branch_child(page, i).expect("child")).collect()
}

fn fixture() -> Fixture {
    let (dir, path) = temp("tndb_b_damage");
    let env = open(&path, SyncMode::Durable);
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..700 {
        put(&mut txn, &mut state, "t", &long_key(i), &value(1, i, 40));
    }
    for i in 0..300_u32 {
        put(&mut txn, &mut state, "u", &i.to_be_bytes(), &value(2, i, 100));
    }
    for i in 0..40 {
        put(&mut txn, &mut state, &table_name(i), b"k", b"v");
    }
    for i in 0..4400_u32 {
        put(&mut txn, &mut state, "big", &i.to_be_bytes(), &value(3, i, 900));
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    drop_table(&mut txn, &mut state, "big");
    txn.commit().expect("commit");
    let t = tree(&env, "t");
    let stale = children(&env, children(&env, t.root)[0].0)[0].0;
    let mut txn = env.begin_rw().expect("begin");
    put(&mut txn, &mut state, "t", &long_key(0), b"new");
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    put(&mut txn, &mut state, "u", &0_u32.to_be_bytes(), b"new");
    txn.commit().expect("commit");
    let snapshot = env.inner().current();
    let meta = snapshot.meta;
    assert_eq!((tree(&env, "t").height, meta.catalog.height, meta.free.height), (3, 2, 2));
    drop(snapshot);
    check(&env);
    drop(env);
    Fixture { dir, path, state, stale }
}

/// Where the damage goes, found in the fixture's newest state.
#[derive(Debug)]
struct Places {
    page_count: u32,
    /// `t`'s root, its first level-two branch, that branch's first leaf, and the index of a later
    /// child of it written by the same txn as the child after it (which is the last value).
    t_root: u32,
    t_branch: u32,
    t_leaf: u32,
    same_txn: (usize, u32),
    /// A catalog leaf other than the root.
    catalog_leaf: u32,
    /// The free tree's root and first leaf, and the file offset of that leaf's first record's
    /// value.
    free_root: u32,
    free_leaf: u32,
    free_value: u64,
}

fn places(path: &Path) -> Places {
    let env = open(path, SyncMode::Durable);
    let t = tree(&env, "t");
    let t_branch = children(&env, t.root)[0].0;
    let leaves = children(&env, t_branch);
    let t_leaf = leaves[0].0;
    let i = (1..leaves.len() - 1).find(|&i| leaves[i].1 == leaves[i + 1].1).expect("same txn");
    let same_txn = (i, leaves[i + 1].0);
    let snapshot = env.inner().current();
    let meta = snapshot.meta;
    let catalog_leaf = children(&env, meta.catalog.root)[1].0;
    let free_leaf = children(&env, meta.free.root)[0].0;
    let leaf = snapshot.page(free_leaf).expect("free leaf");
    let (_, value) = page::leaf_entry(leaf, 0).expect("record");
    assert!(value.len() >= 8, "the first record lists pages");
    let free_value = u64::from(free_leaf) * P + (value.as_ptr() as u64 - leaf.as_ptr() as u64);
    Places {
        page_count: meta.page_count,
        t_root: t.root,
        t_branch,
        t_leaf,
        same_txn,
        catalog_leaf,
        free_root: meta.free.root,
        free_leaf,
        free_value,
    }
}

/// Page `pgno` of the file.
fn read_page(path: &Path, pgno: u32) -> Vec<u8> {
    let mut page = vec![0; P as usize];
    fs::File::open(path)
        .expect("open")
        .read_exact_at(&mut page, u64::from(pgno) * P)
        .expect("read");
    page
}

/// Write page `pgno` of the file.
fn write_page(path: &Path, pgno: u32, page: &[u8]) {
    let file = fs::OpenOptions::new().write(true).open(path).expect("open");
    file.write_all_at(page, u64::from(pgno) * P).expect("write");
}

/// Change page `pgno` with `edit` and stamp its CRC again: damage a CRC cannot see.
fn restamped(path: &Path, pgno: u32, edit: impl FnOnce(&mut [u8])) {
    let mut page = read_page(path, pgno);
    edit(&mut page);
    crc::add_crc32_nonzero(&mut page);
    write_page(path, pgno, &page);
}

/// Point cell `i` of branch `page` at child `child` (its `child_txn` kept).
fn set_child(page: &mut [u8], i: usize, child: u32) {
    let slot = 24 + 2 * i;
    let cell = usize::from(u16::from_le_bytes([page[slot], page[slot + 1]]));
    page[cell..cell + 4].copy_from_slice(&child.to_le_bytes());
}

/// Which use of a damaged environment meets the damage first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// Point reads of every table and key (pages fetched without recomputing their CRCs).
    Get,
    /// Cursor scans of every table.
    Scan,
    /// A write transaction (copies of published pages verify them; free-list loads verify the
    /// records' pages).
    Write,
    /// `Env::check`.
    Check,
}

/// Use a damaged environment as an application would — get every key, scan every table, then
/// write to each table and fill a new one (copies of published pages, free-list loads) — and then
/// check it: the first error must be `Corrupt` and come from `stage`, the environment must latch,
/// and later writes fail.
fn sweep(env: &Env, state: &State, what: &str, stage: Stage) {
    let mut first = None;
    let ro = env.begin_ro().expect("ro");
    for (name, rows) in state {
        let get = (|| {
            let Some(table) = ro.open_table(name)? else { return Ok(()) };
            for k in rows.keys() {
                ro.get(&table, k)?;
            }
            Ok(())
        })();
        if let Err(e) = get {
            first.get_or_insert((Stage::Get, e));
        }
    }
    for name in state.keys() {
        let scan = (|| {
            let Some(table) = ro.open_table(name)? else { return Ok(()) };
            let mut cursor = ro.cursor(&table)?;
            while cursor.next()?.is_some() {}
            Ok(())
        })();
        if let Err(e) = scan {
            first.get_or_insert((Stage::Scan, e));
        }
    }
    drop(ro);
    if env.failure().is_none() {
        let write = (|| {
            let mut txn = env.begin_rw()?;
            for (name, rows) in state {
                let table = txn.create_table(name)?;
                for k in rows.keys().step_by(5) {
                    txn.put(&table, k, b"rewritten")?;
                }
            }
            let w = txn.create_table("w")?;
            for i in 0..1200_u32 {
                txn.put(&w, &i.to_be_bytes(), &value(9, i, 900))?;
            }
            txn.commit().map(|_| ())
        })();
        if let Err(e) = write {
            first.get_or_insert((Stage::Write, e));
        }
    }
    if env.failure().is_none() {
        if let Err(e) = env.check() {
            first.get_or_insert((Stage::Check, e));
        }
    }
    assert!(
        matches!(&first, Some((s, Error::Corrupt(_))) if *s == stage),
        "{what}: {first:?}, expected Corrupt from {stage:?}"
    );
    assert!(env.failure().is_some(), "{what}: the damage latched");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "{what}: writes fail");
}

/// Every kind of damage at rest fails the open closed or is met as `Corrupt` and latches,
/// without a panic, and the open never changes the file.
#[test]
fn damage_at_rest_fails_closed_or_latches() {
    let fx = fixture();
    let at = places(&fx.path);
    let stale = read_page(&fx.path, fx.stale);
    assert!(crc::crc_state(&stale) == crc::CrcState::Valid, "the stale copy is intact");
    assert_eq!(page::pgno(&stale), fx.stale);
    let current = read_page(&fx.path, at.t_leaf);
    assert!(page::txn(&stale) < page::txn(&current), "{at:?}");
    assert_eq!(page::leaf_entry(&stale, 1).ok(), page::leaf_entry(&current, 1).ok());

    type Damage<'a> = (&'a str, Stage, Box<dyn Fn(&Path) + 'a>);
    let (redirect, target) = at.same_txn;
    let opens: Vec<Damage<'_>> = vec![
        // A byte of a cell of a leaf (reads do not recompute CRCs: the copy-on-write that would
        // spread it refuses it), of the root, and of a level-two branch's slot array.
        ("leaf", Stage::Write, Box::new(|p| flip(p, u64::from(at.t_leaf) * P + P - 16))),
        ("root", Stage::Scan, Box::new(|p| flip(p, u64::from(at.t_root) * P + P - 16))),
        ("branch", Stage::Get, Box::new(|p| flip(p, u64::from(at.t_branch) * P + 26))),
        ("free leaf", Stage::Write, Box::new(|p| flip(p, u64::from(at.free_leaf) * P + P - 16))),
        (
            "catalog leaf",
            Stage::Write,
            Box::new(|p| flip(p, u64::from(at.catalog_leaf) * P + P - 16)),
        ),
        ("zeroed leaf", Stage::Get, Box::new(|p| write_page(p, at.t_leaf, &vec![0; P as usize]))),
        // A torn write that left a leaf's header intact and its last sector zero (trailer
        // included): only the zero-CRC check of a page fetch tells, since a zeroed cell parses.
        (
            "zeroed tail sector",
            Stage::Get,
            Box::new(|p| {
                let mut page = read_page(p, at.t_leaf);
                page[P as usize - 512..].fill(0);
                write_page(p, at.t_leaf, &page);
            }),
        ),
        // A branch pointing at the next leaf (written by the same txn: only the range check can
        // tell), and at an older copy of its own leaf (same range: only the txn check can tell);
        // both pass their CRCs and name themselves in their headers.
        (
            "redirected",
            Stage::Get,
            Box::new(|p| restamped(p, at.t_branch, |b| set_child(b, redirect, target))),
        ),
        (
            "stale copy",
            Stage::Get,
            Box::new(|p| restamped(p, at.t_branch, |b| set_child(b, 0, fx.stale))),
        ),
        // A free record listing a page past the end of the file (its CRC re-stamped).
        (
            "free record",
            Stage::Write,
            Box::new(|p| {
                restamped(p, at.free_leaf, |b| {
                    let offset = (at.free_value - u64::from(at.free_leaf) * P) as usize + 4;
                    b[offset..offset + 4].copy_from_slice(&(at.page_count + 5).to_le_bytes());
                })
            }),
        ),
    ];
    for (what, stage, damage) in &opens {
        let path = fx.dir.path().join("case");
        fs::copy(&fx.path, &path).expect("copy");
        damage(&path);
        let before = fs::read(&path).expect("read");
        let env = open(&path, SyncMode::Durable);
        assert!(fs::read(&path).expect("read") == before, "{what}: the open changed the file");
        sweep(&env, &fx.state, what, *stage);
        drop(env);
        let after = fs::read(&path).expect("read");
        assert!(after[..2 * P as usize] == before[..2 * P as usize], "{what}: a meta changed");
        fs::remove_file(&path).expect("remove");
    }

    let newest = |p: &Path| {
        let file = fs::File::open(p).expect("open");
        let mut best: Option<(u64, Meta)> = None;
        for slot in 0..2 {
            let mut record = [0; META_LEN];
            file.read_exact_at(&mut record, slot * P).expect("read");
            let meta = Meta::decode(&record).expect("meta");
            if best.is_none_or(|(_, b)| meta.txn > b.txn) {
                best = Some((slot, meta));
            }
        }
        best.expect("a meta")
    };
    type Closed<'a> = (&'a str, Box<dyn Fn(&Path) + 'a>, fn(&Error) -> bool);
    let closed: Vec<Closed<'_>> = vec![
        (
            "free root",
            Box::new(|p| flip(p, u64::from(at.free_root) * P + P - 16)),
            |e| matches!(e, Error::Corrupt(m) if m.contains("free tree root")),
        ),
        (
            "truncated mid-page",
            Box::new(|p| {
                let file = fs::OpenOptions::new().write(true).open(p).expect("open");
                file.set_len(u64::from(at.page_count) * P - 100).expect("truncate");
            }),
            |e| matches!(e, Error::Corrupt(m) if m.contains("shorter")),
        ),
        (
            "page count past the end",
            Box::new(|p| {
                let (slot, meta) = newest(p);
                let file = fs::OpenOptions::new().write(true).open(p).expect("open");
                // Past the physical end, not just past the page count (slack is harmless).
                let len = file.metadata().expect("metadata").len();
                let grown = Meta { page_count: (len / P) as u32 + 1, ..meta };
                file.write_all_at(&grown.encode(), slot * P).expect("write");
            }),
            |e| matches!(e, Error::Corrupt(m) if m.contains("shorter")),
        ),
        (
            "newest txn u64::MAX",
            Box::new(|p| {
                // Re-encoded, so the record passes its CRC: only open's txn check refuses it.
                let (slot, meta) = newest(p);
                let file = fs::OpenOptions::new().write(true).open(p).expect("open");
                let last = Meta { txn: u64::MAX, ..meta };
                file.write_all_at(&last.encode(), slot * P).expect("write");
            }),
            |e| matches!(e, Error::Corrupt(m) if m.contains("no next txn")),
        ),
        (
            "metas zeroed",
            Box::new(|p| {
                let file = fs::OpenOptions::new().write(true).open(p).expect("open");
                file.write_all_at(&vec![0; 2 * P as usize], 0).expect("write");
            }),
            |e| matches!(e, Error::NotTndbB),
        ),
    ];
    for (what, damage, ok) in &closed {
        let path = fx.dir.path().join("case");
        fs::copy(&fx.path, &path).expect("copy");
        damage(&path);
        assert_fails_closed(&path, |e| {
            assert!(ok(e), "{what}: {e}");
            true
        });
        fs::remove_file(&path).expect("remove");
    }
}

/// A restamped branch whose cell names a real leaf of another part of `t` (under the next
/// level-two branch), written by the txn the cell names, so the identity check passes: a cursor
/// over a snapshot meets it only by crossing into it, forward and backward, and must find the
/// leaf's keys outside the range its ancestors' separators give it. The crossing is `Corrupt` and
/// latches, never a scan that returns another part's entries out of order.
#[test]
fn a_crossing_into_a_redirected_leaf_is_corrupt() {
    let fx = fixture();
    let (branch, cell, target) = {
        let env = open(&fx.path, SyncMode::Durable);
        let level2 = children(&env, tree(&env, "t").root);
        let (first, next) = (children(&env, level2[0].0), children(&env, level2[1].0));
        let (cell, target) = (1..first.len())
            .find_map(|k| next[1..].iter().find(|c| c.1 == first[k].1).map(|c| (k, c.0)))
            .expect("a leaf of the next branch written by the txn of a later cell");
        (level2[0].0, cell, target)
    };
    restamped(&fx.path, branch, |b| set_child(b, cell, target));
    for forward in [true, false] {
        let env = open(&fx.path, SyncMode::Durable);
        let ro = env.begin_ro().expect("ro");
        let table = ro.open_table("t").expect("open").expect("t");
        let mut cursor = ro.cursor(&table).expect("cursor");
        let mut seen = 0;
        let err = loop {
            match if forward { cursor.next() } else { cursor.prev() } {
                Ok(Some(_)) => seen += 1,
                Ok(None) => panic!("forward {forward}: the scan ended after {seen} entries"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, Error::Corrupt(_)), "forward {forward}: {err}");
        assert!(seen > 0, "forward {forward}: met at a crossing, not the first descent");
        drop(ro);
        assert!(env.failure().is_some(), "forward {forward}: the damage latched");
    }
}

/// A table root number past the end of the file, in a catalog record whose leaf's CRC is intact
/// (reads do not check catalog leaves' CRCs, and open checks only the catalog and free-tree
/// roots): `drop_table` and `clear_table` walk the tree to free it, and `tree_pages` must refuse
/// the root by its range check, before the number sizes the walk's dense page set. The error is
/// `Corrupt` and latches.
#[test]
fn a_table_root_past_the_end_is_refused_by_the_tree_walk() {
    for far in [true, false] {
        for dropping in [false, true] {
            let (_dir, path) = temp("tndb_b_damage_root");
            let (catalog, page_count) = {
                let env = open(&path, SyncMode::Durable);
                let mut txn = env.begin_rw().expect("begin");
                let t = txn.create_table("t").expect("t");
                txn.put(&t, b"k", b"v").expect("put");
                txn.commit().expect("commit");
                let meta = env.inner().current().meta;
                assert_eq!(meta.catalog.height, 1, "the catalog is one leaf");
                (meta.catalog.root, meta.page_count)
            };
            let bad = if far { u32::MAX - 1 } else { page_count };
            restamped(&path, catalog, |page| {
                let i = (0..page::nkeys(page))
                    .find(|&i| page::leaf_entry(page, i).expect("entry").0 == b"t")
                    .expect("t's record");
                let (_, value) = page::leaf_entry(page, i).expect("entry");
                let off = value.as_ptr() as usize - page.as_ptr() as usize;
                let mut record = catalog::TableRecord::decode(value).expect("record");
                record.root.root = bad;
                let bytes = record.encode();
                page[off..off + bytes.len()].copy_from_slice(&bytes);
            });
            let env = open(&path, SyncMode::Durable);
            let mut txn = env.begin_rw().expect("begin");
            let t = txn.open_table("t").expect("open").expect("t");
            let err = if dropping { txn.drop_table(t) } else { txn.clear_table(&t) }.unwrap_err();
            let what = format!("root {bad}, drop {dropping}");
            assert!(
                matches!(&err, Error::Corrupt(m) if m.starts_with("tree walk: root ")),
                "{what}: {err}"
            );
            drop(txn);
            assert!(env.failure().is_some(), "{what}: latched");
            assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "{what}: writes fail");
        }
    }
}
