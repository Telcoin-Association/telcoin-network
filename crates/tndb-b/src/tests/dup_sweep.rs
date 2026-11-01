//! The damage sweeps of `damage.rs` and `read_only.rs` over a dup table: every kind of damage to
//! a dup table's cells, trees and record is `Corrupt` at the first use that meets it, and latches.
//! The scan stage scans the dup table with a [`crate::DupRoCursor`], then the plain table beside
//! it. Beside the sweeps: two outer cells naming one nested root, written in one transaction
//! (`Corrupt` at commit) and in two (found by `Env::check`).

use std::{collections::BTreeMap, fs, sync::Arc};

use super::{
    damage::{restamped, tree, Stage, P},
    dup::{dup_key, restamp_cell, restamp_record, OPTIONS},
    *,
};
use crate::{
    page::{self, Find, Val},
    tree::{read::descend, PageRead as _, TreeRoot},
    Error,
};

/// The outer key of the fixture's large nested tree: two levels and a value in a run.
pub(crate) const HOT: u32 = 7;

/// An outer key of the fixture with a nested tree of one leaf.
pub(crate) const COLD: u32 = 8;

/// The outer keys of the fixture's dup table: enough for an outer tree of two levels.
const OUTER: u32 = 400;

/// The dup table's outer key width.
const K: usize = OPTIONS.key_len as usize;

/// A cleanly closed file holding dup table `d` (of [`OPTIONS`]: outer keys 0 to [`OUTER`], 1 to 3
/// rows each but [`HOT`]'s 300 and its run) and plain table `p` (two levels) beside it, and a
/// dropped table's pages in a free tree of one leaf.
pub(crate) struct DupFixture {
    pub(crate) dir: TempDir,
    pub(crate) path: PathBuf,
    pub(crate) plain: Rows,
    pub(crate) dup: Rows,
    /// Each outer key's outer leaf and the nested root its cell names.
    pub(crate) cells: BTreeMap<u32, (u32, TreeRoot)>,
    /// A leaf of `p` and a key it holds.
    plain_leaf: (u32, Vec<u8>),
    /// The leaf of [`HOT`]'s nested tree that holds its first rows.
    hot_leaf: u32,
    /// The catalog, one leaf.
    catalog_leaf: u32,
    /// The free tree, one leaf, and its first record's key.
    free_leaf: (u32, Vec<u8>),
    /// The id the next write transaction takes.
    next_txn: u32,
}

pub(crate) fn dup_fixture() -> DupFixture {
    let (dir, path) = temp("tndb_b_dup_sweep");
    let (mut plain, mut dup) = (Rows::new(), Rows::new());
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let p = txn.create_table("p").expect("p");
    for i in 0..300u32 {
        let (key, v) = (i.to_be_bytes().to_vec(), value(1, i, 40));
        txn.put(&p, &key, &v).expect("put");
        plain.insert(key, v);
    }
    for k in 0..OUTER {
        let rows = if k == HOT { 300 } else { 1 + k % 3 };
        for s in 0..rows {
            let (key, v) =
                (dup_key(k, &[s as u8, 0x5a][..1 + usize::from(k == HOT)]), value(2, s, 40));
            txn.put(&d, &key, &v).expect("put");
            dup.insert(key, v);
        }
    }
    let big = (dup_key(HOT, b"big"), vec![0xb1; 10_000]);
    txn.put(&d, &big.0, &big.1).expect("a value in a run");
    dup.insert(big.0, big.1);
    let x = txn.create_table("x").expect("x");
    for i in 0..150u32 {
        txn.put(&x, &i.to_be_bytes(), &value(3, i, 200)).expect("put");
    }
    txn.commit().expect("commit");
    // A dropped table, so the free tree has a leaf.
    let mut txn = env.begin_rw().expect("rw");
    txn.drop_table(x).expect("drop");
    txn.commit().expect("commit");
    check(&env);
    let snapshot = env.inner().current();
    let (catalog, free) = (snapshot.meta.catalog, snapshot.meta.free);
    assert!(catalog.height == 1 && free.height == 1, "{catalog:?} {free:?}");
    let free_leaf = snapshot.page(free.root).expect("the free leaf");
    let free_leaf =
        (free.root, page::leaf_inline_entry(free_leaf, 0).expect("a record").0.to_vec());
    let leaf_of = |root: &TreeRoot, key: &[u8]| {
        descend(&*snapshot, root, key, |_, _, _, _| Ok(()), None)
            .expect("descend")
            .expect("a leaf")
            .pgno
    };
    let outer = tree(&env, "d");
    let cells = (0..OUTER)
        .map(|k| {
            let leaf = leaf_of(&outer, &k.to_be_bytes());
            let page = snapshot.page(leaf).expect("the outer leaf");
            let Find::Found { value: Val::Nested(root), .. } =
                page::leaf_find(page, &k.to_be_bytes()).expect("find")
            else {
                panic!("key {k}'s nested cell");
            };
            (k, (leaf, TreeRoot::decode(root).expect("a nested root")))
        })
        .collect::<BTreeMap<_, _>>();
    let key = 150u32.to_be_bytes().to_vec();
    let plain_leaf = (leaf_of(&tree(&env, "p"), &key), key);
    let next_txn = u32::try_from(snapshot.meta.txn + 1).expect("a u32 txn");
    let (hot, cold) = (cells[&HOT].1, cells[&COLD].1);
    let hot_leaf = leaf_of(&hot, &[0, 0x5a]);
    assert!(outer.height >= 2 && tree(&env, "p").height >= 2, "{outer:?}");
    assert!(hot.height >= 2 && hot.overflow_pages > 0 && cold.height == 1, "{hot:?} {cold:?}");
    drop(snapshot);
    drop(env);
    DupFixture {
        dir,
        path,
        plain,
        dup,
        cells,
        plain_leaf,
        hot_leaf,
        catalog_leaf: catalog.root,
        free_leaf,
        next_txn,
    }
}

/// Edit, in place, the cell of `key` in leaf `pgno` of the file at `path` (`edit` takes the page
/// and the cell's offset), the leaf's CRC stamped again: a cell no leaf mutator writes.
pub(crate) fn raw_cell(path: &Path, pgno: u32, key: &[u8], edit: impl FnOnce(&mut [u8], usize)) {
    restamped(path, pgno, |page| {
        let Find::Found { index, .. } = page::leaf_find(page, key).expect("find") else {
            panic!("{key:02x?} is in leaf {pgno}");
        };
        let slot = page::HEADER + page::SLOT * index;
        edit(page, usize::from(u16::from_le_bytes([page[slot], page[slot + 1]])));
    });
}

/// Make subkey `[0]`'s cell in the nested leaf `pgno` of the file at `path` one with an empty
/// key, its key byte now the value's first.
fn empty_subkey(path: &Path, pgno: u32) {
    raw_cell(path, pgno, &[0], |page, off| {
        assert_eq!(page[off + 4..off + 8], [1, 0, 0, 0], "a 1-byte key, inline");
        page[off + 4] = 0;
        page[off] += 1;
    });
}

/// Use a damaged environment as `damage.rs`'s sweep does, over plain table `p` and dup table `d`:
/// get every row of `plain` and `dup`, scan `d` (every outer key but `hide`, whose rows `dup`
/// leaves out too: the dup cursor steps inside each key, crosses to the next key with `next`, and
/// seeks the key after `hide` past it) and `p`, then (unless `read_only`) rewrite every fifth row
/// of each through a detached cursor (as [`rewrite_through_cursors`]) and put to it, fill a new
/// table, commit, and check. The first error must be `Corrupt` from `stage`
/// and the environment latched (read-only: by the operation that returned it); writes fail.
pub(crate) fn sweep_dup(
    env: &Env,
    plain: &Rows,
    dup: &Rows,
    hide: Option<u32>,
    what: &str,
    stage: Stage,
    read_only: bool,
) {
    sweep_dup_writing(env, plain, dup, dup, hide, what, stage, read_only);
}

/// [`sweep_dup`] whose write stage puts to every fifth row of `written` in `d`, not of `dup`: a
/// hidden key's rows, left out of the gets and the scan, can still be the write stage's.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sweep_dup_writing(
    env: &Env,
    plain: &Rows,
    dup: &Rows,
    written: &Rows,
    hide: Option<u32>,
    what: &str,
    stage: Stage,
    read_only: bool,
) {
    let mut first = None;
    let ro = Arc::new(env.begin_ro().expect("ro"));
    for (name, rows) in [("d", dup), ("p", plain)] {
        let get = (|| {
            let Some(table) = ro.open_table(name)? else { return Ok(()) };
            for k in rows.keys() {
                ro.get(&table, k)?;
            }
            Ok(())
        })();
        if let Err(e) = get {
            first.get_or_insert((Stage::Get, e, env.failure().is_some()));
        }
    }
    let scan = (|| {
        if let Some(table) = ro.open_table("d")? {
            let mut cursor = ro.dup_cursor_owned(&table)?;
            let mut row = cursor.first()?.map(|(k, _)| k.to_vec());
            while let Some(key) = row {
                row = match cursor.next_in_prefix(K)? {
                    Some((k, _)) => Some(k.to_vec()),
                    None => {
                        let after = u32::from_be_bytes(key[..K].try_into().expect("K")) + 1;
                        match hide == Some(after) {
                            true => cursor.seek_key(&(after + 1).to_be_bytes())?,
                            false => cursor.next()?,
                        }
                        .map(|(k, _)| k.to_vec())
                    }
                };
            }
        }
        let Some(table) = ro.open_table("p")? else { return Ok(()) };
        let mut cursor = ro.cursor(&table)?;
        while cursor.next()?.is_some() {}
        Ok(())
    })();
    if let Err(e) = scan {
        first.get_or_insert((Stage::Scan, e, env.failure().is_some()));
    }
    drop(ro);
    if !read_only && env.failure().is_none() {
        let write = (|| {
            let mut txn = env.begin_rw()?;
            // Opened, not created: a record of other options would be `TableKind`.
            let d = txn.open_table("d")?.expect("d");
            let p = txn.create_table("p")?;
            rewrite_through_cursors(&mut txn, (&d, written), (&p, plain))?;
            for (table, rows) in [(&d, written), (&p, plain)] {
                for k in rows.keys().step_by(5) {
                    txn.put(table, k, b"rewritten")?;
                }
            }
            let w = txn.create_table("w")?;
            for i in 0..1200_u32 {
                txn.put(&w, &i.to_be_bytes(), &value(9, i, 900))?;
            }
            txn.commit().map(|_| ())
        })();
        if let Err(e) = write {
            first.get_or_insert((Stage::Write, e, true));
        }
    }
    if env.failure().is_none() {
        if let Err(e) = env.check() {
            first.get_or_insert((Stage::Check, e, env.failure().is_some()));
        }
    }
    assert!(
        matches!(&first, Some((s, Error::Corrupt(_), true)) if *s == stage),
        "{what}: {first:?}, expected Corrupt from {stage:?}, latched"
    );
    assert!(env.failure().is_some(), "{what}: the damage latched");
    let rw = env.begin_rw();
    match read_only {
        true => assert!(matches!(rw, Err(Error::ReadOnly)), "{what}: {rw:?}"),
        false => assert!(matches!(rw, Err(Error::Failed(_))), "{what}: writes fail"),
    }
}

/// Rewrite every fifth row of `d`'s `dup` and of `p`'s `plain` through a detached cursor, as
/// rewrites of rows are done: a dup row's seek, `delete_current` and `insert`, a plain row's
/// `seek_exact` and put. These are the write stage's first writes into each page, so its damage
/// meets writes that route by a cursor's paths (`write::Hint`); a row the seek does not land on
/// is left to the puts after.
fn rewrite_through_cursors(
    txn: &mut RwTxn,
    (d, dup): (&Table, &Rows),
    (p, plain): (&Table, &Rows),
) -> crate::Result<()> {
    let mut c = txn.dup_cursor_detached(d)?;
    for k in dup.keys().step_by(5) {
        if c.seek(txn, k)?.is_some_and(|(at, _)| at == &k[..]) && c.delete_current(txn)? {
            c.insert(txn, k, b"rewritten")?;
        }
    }
    let mut c = txn.cursor_detached(p)?;
    for k in plain.keys().step_by(5) {
        if c.seek_exact(txn, k)?.is_some() {
            c.put(txn, k, b"rewritten")?;
        }
    }
    Ok(())
}

/// One damage case: its name, the stage of the write-side sweep and of the read-only sweep that
/// first meet it, the outer key whose rows the sweeps leave alone (so a later use meets it), and
/// the damage.
pub(crate) type Case<'a> = (&'a str, Stage, Stage, Option<u32>, Box<dyn Fn(&Path) + 'a>);

/// The cases over `fx`.
fn cases(fx: &DupFixture) -> Vec<Case<'_>> {
    let (cold_leaf, cold) = fx.cells[&COLD];
    let (_, hot) = fx.cells[&HOT];
    let nested = move |root: TreeRoot| {
        Box::new(move |p: &Path| {
            restamp_cell(p, cold_leaf, &COLD.to_be_bytes(), Val::Nested(&root.encode()))
        }) as Box<dyn Fn(&Path)>
    };
    use Stage::{Check, Get, Scan, Write};
    vec![
        ("empty root", Get, Get, None, nested(TreeRoot::EMPTY)),
        ("impossible root", Get, Get, None, nested(TreeRoot { height: 0, ..cold })),
        ("root out of range", Get, Get, None, nested(TreeRoot { root: 1 << 20, ..cold })),
        (
            "root_txn mismatch",
            Get,
            Get,
            None,
            nested(TreeRoot { root_txn: cold.root_txn - 1, ..cold }),
        ),
        (
            "kind mismatch",
            Get,
            Get,
            None,
            nested(TreeRoot { root: hot.root, root_txn: hot.root_txn, ..cold }),
        ),
        ("forged root_txn", Get, Get, None, nested(TreeRoot { root_txn: fx.next_txn, ..cold })),
        (
            "forged root_txn unread",
            Write,
            Check,
            Some(COLD),
            nested(TreeRoot { root_txn: fx.next_txn, ..cold }),
        ),
        (
            "nested cell in a plain table",
            Get,
            Get,
            None,
            Box::new(move |p| {
                restamp_cell(p, fx.plain_leaf.0, &fx.plain_leaf.1, Val::Nested(&cold.encode()))
            }),
        ),
        (
            "nested cell inside a nested tree",
            Get,
            Get,
            None,
            Box::new(move |p| restamp_cell(p, cold.root, &[0], Val::Nested(&cold.encode()))),
        ),
        (
            // A byte of a separator in the nested branch root: gets descend past it to a
            // neighbouring leaf and miss; the cursor's descent checks each page's keys against
            // its ancestors' separators (as `read_only.rs`'s "root" case).
            "nested separator",
            Scan,
            Scan,
            None,
            Box::new(move |p| crate::env::tests::flip(p, u64::from(hot.root) * P + P - 16)),
        ),
        (
            "wrong width",
            Get,
            Get,
            None,
            Box::new(move |p| restamp_record(p, |r| r.dup.as_mut().expect("dup").key_len = 3)),
        ),
        (
            "totals off",
            Check,
            Check,
            None,
            Box::new(|p| restamp_record(p, |r| r.dup.as_mut().expect("dup").nested_entries += 1)),
        ),
        (
            "nested cell of 31 bytes",
            Get,
            Get,
            None,
            Box::new(move |p| {
                raw_cell(p, cold_leaf, &COLD.to_be_bytes(), |page, off| {
                    assert_eq!(page[off..off + 8], [32, 0, 0, 0, 4, 0, 2, 0], "a nested cell");
                    page[off..off + 4].copy_from_slice(&31u32.to_le_bytes());
                })
            }),
        ),
        (
            "nested cell in the catalog",
            Get,
            Get,
            None,
            Box::new(move |p| restamp_cell(p, fx.catalog_leaf, b"d", Val::Nested(&cold.encode()))),
        ),
        (
            "nested cell in the free tree",
            Write,
            Check,
            None,
            Box::new(move |p| {
                restamp_cell(p, fx.free_leaf.0, &fx.free_leaf.1, Val::Nested(&cold.encode()))
            }),
        ),
        ("empty subkey", Get, Get, None, Box::new(move |p| empty_subkey(p, cold.root))),
        // A byte of a row's value in a leaf of the nested tree and of the plain table (reads do
        // not recompute CRCs), each met by the cursor rewrite's first write there, routed by the
        // cursor's paths: the nested delete verifies the leaf on its hit (`verify_path`), the
        // plain put as it copies the leaf (`verify_published`).
        (
            "nested leaf bytes",
            Write,
            Check,
            None,
            Box::new(move |p| crate::env::tests::flip(p, u64::from(fx.hot_leaf) * P + P - 16)),
        ),
        (
            "plain leaf bytes",
            Write,
            Check,
            None,
            Box::new(move |p| crate::env::tests::flip(p, u64::from(fx.plain_leaf.0) * P + P - 16)),
        ),
        (
            "counters off",
            Check,
            Check,
            Some(COLD),
            nested(TreeRoot { entries: cold.entries + 1, ..cold }),
        ),
    ]
}

/// Each case on a fresh copy, rows of its unread key left out.
fn run(read_only: bool) {
    let fx = dup_fixture();
    for (what, write_stage, ro_stage, hide, damage) in &cases(&fx) {
        let path = fx.dir.path().join("case");
        fs::copy(&fx.path, &path).expect("copy");
        damage(&path);
        let mut dup = fx.dup.clone();
        if let Some(k) = hide {
            dup.retain(|key, _| key[..4] != k.to_be_bytes());
        }
        let env = match read_only {
            true => Env::open_read_only(&path, EnvOptions::default()).expect("open read-only"),
            false => open(&path, SyncMode::Durable),
        };
        let stage = if read_only { *ro_stage } else { *write_stage };
        sweep_dup(&env, &fx.plain, &dup, *hide, what, stage, read_only);
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}

/// Every damage case of a dup table is `Corrupt` at its first use by gets, scans, a write
/// transaction or `Env::check`, and latches.
#[test]
fn damage_sweep_on_a_dup_table() {
    run(false);
}

/// Every damage case of a dup table is `Corrupt` at its first use by a read-only environment's
/// gets, scans or `Env::check`, latched by that operation.
#[test]
fn read_only_sweep_on_a_dup_table() {
    run(true);
}

/// A copy of `fx`'s file in which [`COLD`]'s outer cell names the nested root of key 11 (whose
/// rows, counters and values match [`COLD`]'s, so every get reads as before), the leaf's CRC
/// stamped again; the two keys, and the environment open on the copy.
fn shared_root(fx: &DupFixture, name: &str) -> ([u32; 2], Env) {
    let path = fx.dir.path().join(name);
    fs::copy(&fx.path, &path).expect("copy");
    let (a, b) = (COLD, 11);
    restamp_cell(&path, fx.cells[&a].0, &a.to_be_bytes(), Val::Nested(&fx.cells[&b].1.encode()));
    ([a, b], open(&path, SyncMode::Durable))
}

/// A write transaction that rewrites subkey `[0]` of each of `keys` in table `d`, committed.
fn rewrite(env: &Env, keys: &[u32]) -> crate::Result<()> {
    let mut txn = env.begin_rw()?;
    let d = txn.open_table("d")?.expect("d");
    for &k in keys {
        txn.put(&d, &dup_key(k, &[0]), b"rewritten")?;
    }
    txn.commit().map(drop)
}

/// Two outer cells naming one committed nested root, both written in one transaction: each copy
/// of the root frees it, and the commit refuses the page leaving the published trees twice
/// (`Corrupt`, latched); writes then fail.
#[test]
fn a_shared_nested_root_written_once_per_cell_in_one_transaction_is_corrupt() {
    let fx = dup_fixture();
    let (keys, env) = shared_root(&fx, "one");
    let got = rewrite(&env, &keys);
    assert!(
        matches!(&got, Err(Error::Corrupt(m)) if m.contains("left the published trees 2 times")),
        "{got:?}"
    );
    assert!(env.failure().is_some(), "latched");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "writes fail");
}

/// Two outer cells naming one committed nested root, written in two transactions (either key
/// first): the engine's documented limit, as for a forged shared pointer in a plain tree (README
/// "Free list and page reuse"). The first commit frees the root; the second copies the published
/// page and frees it again, which its free cannot tell, so both commit and nothing latches.
/// `Env::check` afterwards reports the root used twice (both free records list it) and the
/// nested root the forged cell no longer names leaked.
#[test]
fn a_shared_nested_root_across_transactions_is_found_by_check() {
    let fx = dup_fixture();
    let (shared, orphan) = (fx.cells[&11].1.root, fx.cells[&COLD].1.root);
    let want = format!("leaked [{orphan}]; used twice [{shared} (free list and free list)]");
    for (name, order) in [("ab", [0, 1]), ("ba", [1, 0])] {
        let (keys, env) = shared_root(&fx, name);
        rewrite(&env, &[keys[order[0]]]).expect("the first commit");
        rewrite(&env, &[keys[order[1]]]).expect("the second commit cannot tell");
        assert!(env.failure().is_none(), "{name}: nothing latched");
        let check = env.check();
        assert!(
            matches!(&check, Err(Error::Corrupt(m)) if m.ends_with(&want)),
            "{name}: {check:?}"
        );
    }
}

/// A nested leaf whose first subkey is empty (the damage of the sweeps' "empty subkey" case): a
/// get of that row's subkey meets the empty key as its miss's neighbour, `Corrupt` there and
/// latched, as an outer key of the wrong width is.
#[test]
fn an_empty_subkey_is_corrupt_at_first_use() {
    let fx = dup_fixture();
    let path = fx.dir.path().join("empty");
    fs::copy(&fx.path, &path).expect("copy");
    empty_subkey(&path, fx.cells[&COLD].1.root);
    let env = open(&path, SyncMode::Durable);
    let ro = env.begin_ro().expect("ro");
    let d = ro.open_table("d").expect("open").expect("d");
    let got = ro.get(&d, &dup_key(COLD, &[0])).map(|v| v.map(<[u8]>::to_vec));
    assert!(matches!(&got, Err(Error::Corrupt(_))), "{got:?}");
    assert!(env.failure().is_some(), "latched");
}
