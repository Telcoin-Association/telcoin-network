//! Damaged files: each kind of damage at rest either fails the open closed (the file unchanged)
//! or, once opened (the open changing nothing), is met by the operations that reach it — reads,
//! a write transaction's copies and free-list loads, `Env::check` — as `Error::Corrupt`, which
//! latches the environment. Nothing panics on any of it.

use std::{fs, os::unix::fs::FileExt as _};

use super::*;
use crate::{
    catalog, crc,
    env::tests::{assert_fails_closed, flip},
    freelist::{decode_pages, encode_pages},
    meta::{Meta, META_LEN},
    page,
    tree::{PageRead as _, TreeRoot},
    Error,
};

pub(super) const P: u64 = 4096;

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
pub(super) struct Fixture {
    pub(super) dir: TempDir,
    pub(super) path: PathBuf,
    pub(super) state: State,
    pub(super) stale: u32,
}

/// Table `name`'s tree in the newest state of `env`.
pub(super) fn tree(env: &Env, name: &str) -> TreeRoot {
    let snapshot = env.inner().current();
    catalog::get(&*snapshot, &snapshot.meta.catalog, name).expect("get").expect("table").root
}

/// The children `(pgno, txn)` of branch page `pgno` in the newest state of `env`.
fn children(env: &Env, pgno: u32) -> Vec<(u32, u32)> {
    let snapshot = env.inner().current();
    let page = snapshot.page(pgno).expect("branch");
    (0..page::nkeys(page)).map(|i| page::branch_child(page, i).expect("child")).collect()
}

pub(super) fn fixture() -> Fixture {
    let (dir, path) = temp("tn_lmdb_damage");
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
pub(super) struct Places {
    pub(super) page_count: u32,
    /// `t`'s root, its first level-two branch, that branch's first leaf, and the index of a later
    /// child of it written by the same txn as the child after it (which is the last value).
    pub(super) t_root: u32,
    pub(super) t_branch: u32,
    pub(super) t_leaf: u32,
    pub(super) same_txn: (usize, u32),
    /// A catalog leaf other than the root.
    pub(super) catalog_leaf: u32,
    /// The free tree's root and first leaf, and the file offset of that leaf's first record's
    /// value.
    pub(super) free_root: u32,
    pub(super) free_leaf: u32,
    pub(super) free_value: u64,
}

pub(super) fn places(path: &Path) -> Places {
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
    let (_, value) = page::leaf_inline_entry(leaf, 0).expect("record");
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
pub(super) fn read_page(path: &Path, pgno: u32) -> Vec<u8> {
    let mut page = vec![0; P as usize];
    fs::File::open(path)
        .expect("open")
        .read_exact_at(&mut page, u64::from(pgno) * P)
        .expect("read");
    page
}

/// Write page `pgno` of the file.
pub(super) fn write_page(path: &Path, pgno: u32, page: &[u8]) {
    let file = fs::OpenOptions::new().write(true).open(path).expect("open");
    file.write_all_at(page, u64::from(pgno) * P).expect("write");
}

/// Change page `pgno` with `edit` and stamp its CRC again: damage a CRC cannot see.
pub(super) fn restamped(path: &Path, pgno: u32, edit: impl FnOnce(&mut [u8])) {
    let mut page = read_page(path, pgno);
    edit(&mut page);
    page::stamp(&mut page);
    write_page(path, pgno, &page);
}

/// Add page `p` to the first record of the free leaf `free_leaf`, the leaf's CRC stamped again.
pub(super) fn list_in_free_record(path: &Path, free_leaf: u32, p: u32) {
    restamped(path, free_leaf, |page| {
        let (_, v) = page::leaf_inline_entry(page, 0).expect("record");
        let mut listed = decode_pages(v, u32::MAX).expect("pages");
        assert!(!listed.is_empty(), "the first record lists pages");
        listed.push(p);
        listed.sort_unstable();
        let fit = page::leaf_replace_value(page, 0, &encode_pages(&listed)).expect("fits");
        assert!(fit == page::Fit::Done);
    });
}

/// Point cell `i` of branch `page` at child `child` (its `child_txn` kept).
pub(super) fn set_child(page: &mut [u8], i: usize, child: u32) {
    let slot = 24 + 2 * i;
    let cell = usize::from(u16::from_le_bytes([page[slot], page[slot + 1]]));
    page[cell..cell + 4].copy_from_slice(&child.to_le_bytes());
}

/// Which use of a damaged environment meets the damage first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
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
pub(super) fn sweep(env: &Env, state: &State, what: &str, stage: Stage) {
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
    assert!(page::crc_state(&stale) == crc::CrcState::Valid, "the stale copy is intact");
    assert_eq!(page::pgno(&stale), fx.stale);
    let current = read_page(&fx.path, at.t_leaf);
    assert!(page::txn(&stale) < page::txn(&current), "{at:?}");
    assert_eq!(page::leaf_inline_entry(&stale, 1).ok(), page::leaf_inline_entry(&current, 1).ok());

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
            |e| matches!(e, Error::NotTnLmdb),
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
            let (_dir, path) = temp("tn_lmdb_damage_root");
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
                    .find(|&i| page::leaf_inline_entry(page, i).expect("entry").0 == b"t")
                    .expect("t's record");
                let (_, value) = page::leaf_inline_entry(page, i).expect("entry");
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

/// A table root that counts one leaf or one branch page more than its tree holds (still a
/// possible root, in a catalog record whose leaf's CRC is stamped again): `drop_table` and
/// `clear_table` walk the tree to free it, and `tree_pages` holds the pages it collects to the
/// handle's counters, refusing the root before anything is freed. The error is `Corrupt`, names
/// the root, and latches.
#[test]
fn a_table_root_miscounting_its_pages_is_refused_by_the_tree_walk() {
    for field in ["leaf_pages", "branch_pages"] {
        for dropping in [false, true] {
            let (_dir, path) = temp("tn_lmdb_damage_miscounted_root");
            let (catalog, root) = {
                let env = open(&path, SyncMode::Durable);
                let mut txn = env.begin_rw().expect("begin");
                let t = txn.create_table("t").expect("t");
                for i in 0..300_u32 {
                    txn.put(&t, &i.to_be_bytes(), &value(3, i, 100)).expect("put");
                }
                txn.commit().expect("commit");
                let meta = env.inner().current().meta;
                assert_eq!(meta.catalog.height, 1, "the catalog is one leaf");
                (meta.catalog.root, tree(&env, "t"))
            };
            assert_eq!(root.height, 2, "{root:?}");
            let miscounted = match field {
                "leaf_pages" => TreeRoot { leaf_pages: root.leaf_pages + 1, ..root },
                _ => TreeRoot { branch_pages: root.branch_pages + 1, ..root },
            };
            restamped(&path, catalog, |page| {
                let i = (0..page::nkeys(page))
                    .find(|&i| page::leaf_inline_entry(page, i).expect("entry").0 == b"t")
                    .expect("t's record");
                let (_, value) = page::leaf_inline_entry(page, i).expect("entry");
                let off = value.as_ptr() as usize - page.as_ptr() as usize;
                let mut record = catalog::TableRecord::decode(value).expect("record");
                record.root = miscounted;
                let bytes = record.encode();
                page[off..off + bytes.len()].copy_from_slice(&bytes);
            });
            let env = open(&path, SyncMode::Durable);
            let mut txn = env.begin_rw().expect("begin");
            let t = txn.open_table("t").expect("open").expect("t");
            let result = if dropping { txn.drop_table(t) } else { txn.clear_table(&t) };
            let what = format!("{field}, drop {dropping}");
            let message = format!(
                "tree walk: the tree of root {} holds {} branch and {} leaf pages, the handle \
                 counts {} and {}",
                root.root,
                root.branch_pages,
                root.leaf_pages,
                miscounted.branch_pages,
                miscounted.leaf_pages
            );
            assert!(
                matches!(&result, Err(Error::Corrupt(m)) if m.contains(&message)),
                "{what}: {result:?}"
            );
            assert!(txn.writer_mut().alloc.freed().is_empty(), "{what}: nothing freed");
            drop(txn);
            assert!(env.failure().is_some(), "{what}: latched");
        }
    }
}

/// A table root one level short of its tree (height 1 over a two-level tree, its counters those
/// of the cut view: no branch, one leaf, a possible root), in a catalog record whose leaf's CRC
/// is stamped again: `drop_table` and `clear_table` walk the tree to free it, and `tree_pages`
/// finds a branch where the handle puts the leaves. The error is `Corrupt` and latches, with
/// nothing freed: freeing the root alone would leak every leaf under it.
#[test]
fn a_table_root_short_of_its_leaves_is_refused_by_the_tree_walk() {
    for dropping in [false, true] {
        let (_dir, path) = temp("tn_lmdb_damage_short_root");
        let (catalog, root) = {
            let env = open(&path, SyncMode::Durable);
            let mut txn = env.begin_rw().expect("begin");
            let t = txn.create_table("t").expect("t");
            for i in 0..300_u32 {
                txn.put(&t, &i.to_be_bytes(), &value(3, i, 100)).expect("put");
            }
            txn.commit().expect("commit");
            let meta = env.inner().current().meta;
            assert_eq!(meta.catalog.height, 1, "the catalog is one leaf");
            (meta.catalog.root, tree(&env, "t"))
        };
        assert_eq!(root.height, 2, "{root:?}");
        let short = TreeRoot { height: 1, branch_pages: 0, leaf_pages: 1, ..root };
        restamped(&path, catalog, |page| {
            let i = (0..page::nkeys(page))
                .find(|&i| page::leaf_inline_entry(page, i).expect("entry").0 == b"t")
                .expect("t's record");
            let (_, value) = page::leaf_inline_entry(page, i).expect("entry");
            let off = value.as_ptr() as usize - page.as_ptr() as usize;
            let mut record = catalog::TableRecord::decode(value).expect("record");
            record.root = short;
            let bytes = record.encode();
            page[off..off + bytes.len()].copy_from_slice(&bytes);
        });
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table("t").expect("open").expect("t");
        let result = if dropping { txn.drop_table(t) } else { txn.clear_table(&t) };
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains("does not reach the leaves")),
            "drop {dropping}: {result:?}"
        );
        assert!(txn.writer_mut().alloc.freed().is_empty(), "drop {dropping}: nothing freed");
        drop(txn);
        assert!(env.failure().is_some(), "drop {dropping}: latched");
    }
}

/// Table `u`'s last leaf in the fixture (`u` has two levels and no runs, so a free of it reads its
/// root and only its first leaf), and a copy of the fixture's file, named `what`, whose first free
/// record (which passes the reuse gate) also lists that leaf, the record's leaf CRC stamped again.
fn u_leaf_listed_free(fx: &Fixture, what: &str) -> (PathBuf, u32) {
    let free_leaf = places(&fx.path).free_leaf;
    let leaf = {
        let env = open(&fx.path, SyncMode::Durable);
        let u = tree(&env, "u");
        assert_eq!((u.height, u.overflow_pages), (2, 0), "{u:?}");
        children(&env, u.root).last().expect("a leaf").0
    };
    let path = fx.dir.path().join(what);
    fs::copy(&fx.path, &path).expect("copy");
    list_in_free_record(&path, free_leaf, leaf);
    (path, leaf)
}

/// Put entries `0..` of `len`-byte values into table `w` of `txn` until the allocator hands out
/// page `p`; returns how many were put.
fn put_until_private(txn: &mut RwTxn, w: &Table, p: u32, len: usize) -> u32 {
    let mut i = 0_u32;
    while !txn.writer_mut().alloc.is_private(p) {
        assert!(i < 20_000, "page {p} was never handed out");
        txn.put(w, &i.to_be_bytes(), &value(9, i, len)).expect("put");
        i += 1;
    }
    i
}

/// A free record that passes the reuse gate lists table `u`'s last leaf `L`, which a write then
/// hands out to a new table `w`, as a tree page or as a 1-page overflow run. In the same
/// transaction, `drop_table` and `clear_table` of `u` meet `L` private to the transaction under
/// `u`'s published root (`tree_pages` reads only `u`'s first leaf, but checks every child's
/// provenance): `Corrupt`, which latches, with nothing freed, and every `w` key still reads back
/// in the transaction. `w`'s puts overwrote `L` in place, so once the transaction is gone a get
/// of `u`'s keys in `L` is `Corrupt` (naming `L`), and of the others their committed values.
/// Freeing `L` would pool it at once: the next put would take it for another table and `w`'s gets
/// would return that table's entries, or, for a run page, the commit would stamp `L` as a page of
/// its own.
#[test]
fn a_free_of_a_tree_refuses_a_leaf_this_transaction_holds_for_another() {
    let fx = fixture();
    for run in [false, true] {
        for dropping in [false, true] {
            let what = format!("run {run}, drop {dropping}");
            let (path, leaf) = u_leaf_listed_free(&fx, &format!("same-{run}-{dropping}"));
            let env = open(&path, SyncMode::Durable);
            let mut txn = env.begin_rw().expect("begin");
            let w = txn.create_table("w").expect("w");
            let len = if run { 3000 } else { 900 };
            let n = put_until_private(&mut txn, &w, leaf, len);
            assert_eq!(txn.writer_mut().alloc.is_private_run(leaf), run, "{what}");
            let lists = |txn: &mut RwTxn| {
                let alloc = &txn.writer_mut().alloc;
                (alloc.pool().to_vec(), alloc.freed().to_vec())
            };
            let before = lists(&mut txn);
            let u = txn.open_table("u").expect("open").expect("u");
            let got = if dropping { txn.drop_table(u) } else { txn.clear_table(&u) };
            let want = format!("page {leaf} is private to this transaction but named by a page");
            match got {
                Err(Error::Corrupt(e)) => assert!(e.contains(&want), "{what}: {e}"),
                other => panic!("{what}: {other:?}"),
            }
            assert!(lists(&mut txn) == before, "{what}: nothing freed");
            assert!(txn.writer_mut().alloc.is_private(leaf), "{what}: the leaf is still w's");
            for i in 0..n {
                let got = txn.get(&w, &i.to_be_bytes()).expect("get");
                assert!(got == Some(&value(9, i, len)[..]), "{what}: key {i}");
            }
            drop(txn);
            assert!(env.failure().is_some(), "{what}: the damage latched");
            assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "{what}: writes fail");
            // `w`'s puts overwrote `L` in place (the damage), so `u`'s keys in `L` are refused.
            let ro = env.begin_ro().expect("ro");
            let u = ro.open_table("u").expect("open").expect("u");
            let mut refused = 0;
            for (k, want) in &fx.state["u"] {
                match ro.get(&u, k) {
                    Ok(got) => assert!(got == Some(&want[..]), "{what}: u's key {k:?}"),
                    Err(Error::Corrupt(e)) => {
                        assert!(e.contains(&format!("page {leaf} ")), "{what}: {e}");
                        refused += 1;
                    }
                    Err(e) => panic!("{what}: u's key {k:?}: {e}"),
                }
            }
            assert!(refused > 0, "{what}: u's keys in L are refused");
            drop(ro);
            drop(env);
            fs::remove_file(&path).expect("remove");
        }
    }
}

/// The same damage across transactions: `w` takes `L` and commits, so `L` is published when a
/// later transaction drops `u`. That drop cannot tell (it reads only `u`'s first leaf) and frees
/// `L` again. Once a later write hands `L` to table `x`, `w`'s gets of the keys in `L` fail the
/// identity check (the page is not the copy `w`'s branch names) and latch: `Corrupt`, never `x`'s
/// entries.
#[test]
fn a_live_leaf_freed_by_a_later_drop_is_corrupt_once_reused() {
    let fx = fixture();
    let (path, leaf) = u_leaf_listed_free(&fx, "across");
    let env = open(&path, SyncMode::Durable);
    env.with_writer(|w| w.alloc.log_pages = true);
    let mut txn = env.begin_rw().expect("begin");
    let w = txn.create_table("w").expect("w");
    let n = put_until_private(&mut txn, &w, leaf, 900);
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    let u = txn.open_table("u").expect("open").expect("u");
    txn.drop_table(u).expect("the drop cannot tell");
    let dropped = txn.commit().expect("commit");
    let freed = env.with_writer(|w| w.alloc.free_log.contains(&(dropped, leaf)));
    assert!(freed, "the drop freed L");
    // One more commit, so the gate passes the drop's txn and `L` is reusable.
    let mut txn = env.begin_rw().expect("begin");
    let y = txn.create_table("y").expect("y");
    txn.put(&y, b"k", b"v").expect("put");
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    let x = txn.create_table("x").expect("x");
    put_until_private(&mut txn, &x, leaf, 3000);
    txn.commit().expect("the commit cannot tell");
    let ro = env.begin_ro().expect("ro");
    let w = ro.open_table("w").expect("open").expect("w");
    let mut refused = 0;
    for i in 0..n {
        match ro.get(&w, &i.to_be_bytes()) {
            Ok(got) => assert!(got == Some(&value(9, i, 900)[..]), "key {i}: the value as written"),
            Err(Error::Corrupt(e)) => {
                assert!(e.contains(&format!("page {leaf} ")), "{e}");
                assert!(e.contains("is not the copy its parent names"), "{e}");
                refused += 1;
            }
            Err(e) => panic!("key {i}: {e}"),
        }
    }
    assert!(refused > 0, "the keys in L are refused");
    drop(ro);
    assert!(env.failure().is_some(), "the get latched");
}

/// The key of the one row [`two_tables`] gives each table: `key` in a plain table, outer key 1
/// with the subkey `key` in a dup table.
fn first_key(dup: bool) -> Vec<u8> {
    if dup {
        dup::dup_key(1, b"key")
    } else {
        b"key".to_vec()
    }
}

/// A cleanly closed file with the tables `a` and `d`, plain or (with `dup`) dup tables of
/// [`dup::OPTIONS`], each holding one row under [`first_key`] whose value is the table's name,
/// its catalog one leaf: the dir, the path, both handles and the meta's next table id.
fn two_tables(dup: bool) -> (TempDir, PathBuf, [Table; 2], u32) {
    let (dir, path) = temp("tn_lmdb_table_ids");
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let tables = ["a", "d"].map(|name| {
        let table =
            if dup { txn.create_dup_table(name, dup::OPTIONS) } else { txn.create_table(name) };
        let table = table.expect("create");
        txn.put(&table, &first_key(dup), name.as_bytes()).expect("put");
        table
    });
    txn.commit().expect("commit");
    let next = env.inner().current().meta.next_table_id;
    (dir, path, tables, next)
}

/// A catalog record whose id is not below its state's next table id (the id the next table
/// created takes) is `Corrupt` at its first use, before a handle carries the id: either
/// transaction's `open_table`, a get, a table stat and a put through a handle taken before,
/// `create_table` of its name, its `open_table` after a create took the id, and `Env::check`.
/// Each latches; the table beside it reads on.
#[test]
fn a_catalog_record_id_past_the_next_table_id_is_corrupt_at_first_use() {
    let (dir, path, [_, d], next) = two_tables(false);
    for id in [next, u32::MAX] {
        let crafted = dir.path().join(format!("crafted-{id}"));
        fs::copy(&path, &crafted).expect("copy");
        dup::restamp_record(&crafted, |record| record.id = id);
        {
            let (_dir, env) = dup::copy_of(&crafted);
            let ro = env.begin_ro().expect("ro");
            let a = ro.open_table("a").expect("a's record").expect("a");
            assert_eq!(ro.get(&a, b"key").expect("get"), Some(&b"a"[..]));
            assert!(env.failure().is_none(), "id {id}: table a met no damage");
        }
        let uses = [
            "ro open_table",
            "ro get",
            "ro stat",
            "rw open_table",
            "rw put",
            "create_table",
            "after a create",
            "check",
        ];
        for what in uses {
            let (_dir, env) = dup::copy_of(&crafted);
            let result = match what {
                "ro open_table" => env.begin_ro().expect("ro").open_table("d").map(drop),
                "ro get" => env.begin_ro().expect("ro").get(&d, b"key").map(drop),
                "ro stat" => env.begin_ro().expect("ro").table_stat(&d).map(drop),
                "rw open_table" => env.begin_rw().expect("rw").open_table("d").map(drop),
                "rw put" => env.begin_rw().expect("rw").put(&d, b"k", b"v"),
                "create_table" => env.begin_rw().expect("rw").create_table("d").map(drop),
                "after a create" => {
                    let mut txn = env.begin_rw().expect("rw");
                    let c = txn.create_table("c").expect("create");
                    assert_eq!(c.id(), next, "the table created takes the next id");
                    txn.open_table("d").map(drop)
                }
                _ => env.check().map(drop),
            };
            let message =
                if what == "check" { "impossible record" } else { "not below the state's next" };
            assert!(
                matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)),
                "id {id} {what}: {result:?}"
            );
            assert!(env.failure().is_some(), "id {id} {what}: latched");
        }
    }
}

/// The rows a write transaction's detached cursor reads from its first row on.
fn cursor_rows(txn: &RwTxn, cursor: &mut crate::TxnCursor) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut row = cursor.first(txn).expect("first").map(|(k, v)| (k.to_vec(), v.to_vec()));
    while let Some(r) = row {
        rows.push(r);
        row = cursor.next(txn).expect("next").map(|(k, v)| (k.to_vec(), v.to_vec()));
    }
    rows
}

/// Two catalog records of one id below the next table id (damage only `Env::check`, which scans
/// the catalog, can see) are two tables in a write transaction, each with its own working
/// record: a detached cursor of `d` reads `d`'s tree after writes to both tables, `a`'s first,
/// never `a`'s; and dropping `a` after a write to `d` keeps `d`'s write, so the commit records
/// `d`'s new tree and the file's accounting is exact (`d`'s old pages freed once, none leaked).
#[test]
fn two_catalog_records_of_one_id_keep_their_own_working_records() {
    let (_dir, path, [a, _], _) = two_tables(false);
    dup::restamp_record(&path, |record| record.id = a.id());
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let a = txn.open_table("a").expect("open").expect("a");
    let d = txn.open_table("d").expect("open").expect("d");
    assert_eq!(a.id(), d.id(), "the crafted catalog gives both tables one id");
    txn.put(&a, b"a2", b"a").expect("put");
    let mut cursor = txn.cursor_detached(&d).expect("cursor");
    txn.put(&d, b"d2", b"d").expect("put");
    txn.put(&a, b"a3", b"a").expect("put");
    let expected = [(b"d2".to_vec(), b"d".to_vec()), (b"key".to_vec(), b"d".to_vec())];
    assert_eq!(cursor_rows(&txn, &mut cursor), expected, "d's cursor reads d's tree");

    txn.drop_table(a).expect("drop");
    txn.commit().expect("commit");
    let state = newest_state(&env);
    assert_eq!(state, State::from([("d".to_string(), expected.into_iter().collect())]));
    assert!(env.failure().is_none());
}

/// Two dup tables of one id (a crafted catalog's, as in
/// [`two_catalog_records_of_one_id_keep_their_own_working_records`]), written under one outer
/// key, `a` first: a detached dup cursor of `d` takes the key's nested root from the cell while
/// `d` has no working record (though `a`'s working map holds the key), then from `d`'s working
/// map once `d` is written, never from `a`'s.
#[test]
fn a_dup_cursor_reads_its_own_working_map_beside_a_table_of_its_id() {
    let (_dir, path, [a, _], _) = two_tables(true);
    dup::restamp_record(&path, |record| record.id = a.id());
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let a = txn.open_table("a").expect("open").expect("a");
    let d = txn.open_table("d").expect("open").expect("d");
    assert_eq!(a.id(), d.id(), "the crafted catalog gives both tables one id");
    let two = dup::dup_key(1, b"two");
    txn.put(&a, &two, b"a").expect("put");
    let mut cursor = txn.dup_cursor_detached(&d).expect("cursor");
    let mut rows = |txn: &RwTxn| {
        let mut rows = Vec::new();
        let mut row = cursor.first(txn).expect("first").map(|(k, v)| (k.to_vec(), v.to_vec()));
        while let Some(r) = row {
            rows.push(r);
            row = cursor.next(txn).expect("next").map(|(k, v)| (k.to_vec(), v.to_vec()));
        }
        rows
    };
    let of_d = |key: Vec<u8>| (key, b"d".to_vec());
    assert_eq!(rows(&txn), [of_d(first_key(true))], "d's committed rows");
    txn.put(&d, &two, b"d").expect("put");
    assert_eq!(rows(&txn), [of_d(first_key(true)), of_d(two)], "d's working rows");
    assert_eq!(txn.dup_rows_for_test(&a).len(), 2, "a's rows");
    assert!(env.failure().is_none());
}

/// A meta whose txn id is one short of the last (only a crafted record has it) opens, and its
/// reads serve; its commit, which would take the last id and leave the writer none to begin, is
/// `Corrupt` before anything is written, in either sync mode: no panic, no wrap, both meta slots
/// as they were. It latches, reads go on, and a reopen finds the state as it was.
#[test]
fn a_commit_of_the_last_txn_id_is_refused_before_it_writes() {
    let (_dir, path) = temp("tn_lmdb_last_txn");
    {
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let a = txn.create_table("a").expect("a");
        txn.put(&a, b"key", b"a").expect("put");
        txn.commit().expect("commit");
    }
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let slot = |at: u64| {
        let mut record = [0; META_LEN];
        file.read_exact_at(&mut record, at * P).expect("read");
        record
    };
    let txn_of = |at: u64| Meta::decode(&slot(at)).expect("meta").txn;
    let newest = if txn_of(0) > txn_of(1) { 0 } else { 1 };
    // Re-encoded, so the record passes its CRC.
    let last = Meta { txn: u64::MAX - 1, ..Meta::decode(&slot(newest)).expect("meta") };
    file.write_all_at(&last.encode(), newest * P).expect("write");
    let slots = [slot(0), slot(1)];
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        let env = open(&path, sync);
        let ro = env.begin_ro().expect("ro");
        assert_eq!(ro.txn_id(), u64::MAX - 1, "{sync:?}");
        let a = ro.open_table("a").expect("open").expect("a");
        let mut txn = env.begin_rw().expect("rw");
        assert_eq!(txn.txn_id(), u64::MAX, "{sync:?}");
        txn.put(&a, b"key", b"b").expect("put");
        txn.put(&a, b"more", b"b").expect("put");
        let result = txn.commit();
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains("leaves no txn after this")),
            "{sync:?}: {result:?}"
        );
        assert!(env.failure().is_some(), "{sync:?}: latched");
        assert_eq!(ro.get(&a, b"key").expect("get"), Some(&b"a"[..]), "{sync:?}: reads go on");
        drop(ro);
        drop(env);
        assert_eq!([slot(0), slot(1)], slots, "{sync:?}: neither meta slot changed");
    }
    let env = open(&path, SyncMode::Durable);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(ro.txn_id(), u64::MAX - 1);
    assert_eq!(
        state_of(&ro, ["a"]),
        State::from([("a".to_string(), Rows::from([(b"key".to_vec(), b"a".to_vec())]))])
    );
}

/// A meta whose txn id is far above the other slot's (one short of the last: a valid record,
/// which open takes) makes the reuse gate's lag, the writer's txn less the older slot's, near
/// `u64::MAX`: a write that loads free records more than once sums it into
/// `IoStats::gate_lag_sum`, which saturates (no overflow panic, no wrap), and its commit is then
/// the `Corrupt` that [`a_commit_of_the_last_txn_id_is_refused_before_it_writes`] pins.
#[test]
fn a_far_meta_txn_saturates_the_gate_lag() {
    let (_dir, path) = temp("tn_lmdb_far_txn");
    {
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let big = txn.create_table("big").expect("big");
        for i in 0..2000_u32 {
            txn.put(&big, &i.to_be_bytes(), &value(1, i, 900)).expect("put");
        }
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("rw");
        let big = txn.open_table("big").expect("open").expect("big");
        txn.drop_table(big).expect("drop");
        txn.commit().expect("commit");
        // Two more commits, so the dropped pages pass the gate of both slots.
        for k in 0..2_u32 {
            let mut txn = env.begin_rw().expect("rw");
            let t = txn.create_table("t").expect("t");
            txn.put(&t, &k.to_be_bytes(), b"v").expect("put");
            txn.commit().expect("commit");
        }
    }
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let slot = |at: u64| {
        let mut record = [0; META_LEN];
        file.read_exact_at(&mut record, at * P).expect("read");
        Meta::decode(&record).expect("meta")
    };
    let newest = if slot(0).txn > slot(1).txn { 0 } else { 1 };
    // Re-encoded, so the record passes its CRC.
    let far = Meta { txn: u64::MAX - 1, ..slot(newest) };
    file.write_all_at(&far.encode(), newest * P).expect("write");
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let w = txn.create_table("w").expect("w");
    for i in 0..1500_u32 {
        txn.put(&w, &i.to_be_bytes(), &value(2, i, 900)).expect("put");
    }
    let stats = txn.writer_mut().stats;
    assert!(stats.gate_lag_samples >= 2, "{stats:?}");
    assert_eq!(stats.gate_lag_sum, u64::MAX, "a saturated sum");
    let result = txn.commit();
    assert!(
        matches!(&result, Err(Error::Corrupt(m)) if m.contains("leaves no txn after this")),
        "{result:?}"
    );
    assert!(env.failure().is_some(), "latched");
}
