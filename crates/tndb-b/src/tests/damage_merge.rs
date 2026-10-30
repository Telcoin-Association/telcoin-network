//! Damage to a page a delete's merge absorbs, met through the public API: a delete whose leaf
//! (table `l`) or whose branch, after an unlink (table `b`), falls under a quarter full merges it
//! with a same-parent sibling, and the sibling is read from the file with a descent's checks
//! before its cells move into a page the transaction trusts. Each kind of damage is
//! `Error::Corrupt` at that delete, the first operation of its transaction: the environment
//! latches and the file is byte for byte as it was ([`refused`]).

use super::{
    damage::{read_page, restamped, tree, P},
    *,
};
use crate::{
    env::tests::flip,
    overflow::Ovf,
    page::{self, Val},
    tree::PageRead as _,
    Error,
};

/// A key of table `l`: four entries of it fill a leaf, and one is under a quarter of it.
fn l_key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A key of table `b`: 180 bytes, so a branch holds at most 22 children and one of 6 is under a
/// quarter full. With a value of [`B_VALUE`] bytes four entries fill a leaf, and one is not under
/// a quarter of it: `b`'s leaves never merge, and a leaf's last delete unlinks it.
fn b_key(i: u32) -> Vec<u8> {
    let mut key = format!("{i:06}").into_bytes();
    key.resize(180, b'k');
    key
}

/// The largest inline value for a 180-byte key at 4 KiB pages: its entry (cell and slot) is a
/// quarter of the page's usable bytes.
const B_VALUE: usize = 827;

/// The children `(pgno, txn)` of branch page `p` in the newest state of `env`.
fn children(env: &Env, p: u32) -> Vec<(u32, u32)> {
    let snapshot = env.inner().current();
    let page = snapshot.page(p).expect("branch");
    (0..page::nkeys(page)).map(|i| page::branch_child(page, i).expect("child")).collect()
}

/// The keys of leaf page `p` in the newest state of `env`.
fn leaf_keys(env: &Env, p: u32) -> Vec<Vec<u8>> {
    let snapshot = env.inner().current();
    let page = snapshot.page(p).expect("leaf");
    (0..page::nkeys(page)).map(|i| page::leaf_key(page, i).expect("key").to_vec()).collect()
}

/// A cleanly closed file whose deletes merge. Table `l` (two levels) has leaves `{0, 3}`,
/// `{4, 6, 7}`, `{8, 10, 11}`, `{12, 15}` first: a delete of `0` leaves its leaf under a quarter
/// and it absorbs its right neighbour; a delete of `12` absorbs the left one. Table `b` (three
/// levels) has branches `B0`, `B1`, `B2` of 7, 10 and 7 children first, the last leaf of `B0` and
/// of `B2` holding one key (`b0_last`, `b2_last`): its delete unlinks the leaf and leaves the
/// branch with 6 children, under a quarter, and `B0` absorbs `B1` (right), `B2` absorbs `B1`
/// (left). `next_txn` is the id of the next write transaction.
struct Fixture {
    dir: TempDir,
    path: PathBuf,
    b0_last: Vec<u8>,
    b2_last: Vec<u8>,
    next_txn: u64,
}

fn fixture() -> Fixture {
    let (dir, path) = temp("tndb_b_damage_merge");
    let env = open(&path, SyncMode::Durable);
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..40 {
        put(&mut txn, &mut state, "l", &l_key(i), &value(1, i, 900));
    }
    for i in 0..400 {
        put(&mut txn, &mut state, "b", &b_key(i), &value(2, i, B_VALUE));
    }
    txn.commit().expect("commit");
    let l = tree(&env, "l");
    assert_eq!(l.height, 2, "{l:?}");
    for (j, &(leaf, _)) in children(&env, l.root).iter().take(4).enumerate() {
        let want: Vec<Vec<u8>> =
            (4 * j as u32..4 * j as u32 + 4).map(|i| l_key(i).to_vec()).collect();
        assert_eq!(leaf_keys(&env, leaf), want, "appended leaves are full");
    }
    let b = tree(&env, "b");
    assert_eq!((b.height, l.overflow_pages, b.overflow_pages), (3, 0, 0), "{b:?}");
    let branches = children(&env, b.root);
    assert!(branches.len() >= 4, "{} branches", branches.len());

    let mut txn = env.begin_rw().expect("begin");
    for i in [1, 2, 5, 9, 13, 14] {
        delete(&mut txn, &mut state, "l", &l_key(i));
    }
    // Whole leaves leave from the end of each branch (unlinks that keep it over a quarter), and
    // the last leaf kept in `B0` and `B2` is cut to one key.
    let mut last = Vec::new();
    for (bi, keep) in [(0, 7), (1, 10), (2, 7)] {
        let leaves = children(&env, branches[bi].0);
        assert!(leaves.len() >= keep, "branch {bi}: {} children", leaves.len());
        for &(leaf, _) in &leaves[keep..] {
            for k in leaf_keys(&env, leaf) {
                delete(&mut txn, &mut state, "b", &k);
            }
        }
        if bi != 1 {
            let keys = leaf_keys(&env, leaves[keep - 1].0);
            assert_eq!(keys.len(), 4, "branch {bi}: a full leaf");
            for k in &keys[1..] {
                delete(&mut txn, &mut state, "b", k);
            }
            last.push(keys[0].clone());
        }
    }
    let next_txn = txn.txn_id() + 1;
    txn.commit().expect("commit");

    let l = tree(&env, "l");
    let keys: Vec<Vec<Vec<u8>>> =
        children(&env, l.root).iter().take(4).map(|&(p, _)| leaf_keys(&env, p)).collect();
    let want: Vec<Vec<Vec<u8>>> = [&[0, 3][..], &[4, 6, 7], &[8, 10, 11], &[12, 15]]
        .iter()
        .map(|ks| ks.iter().map(|&i| l_key(i).to_vec()).collect())
        .collect();
    assert_eq!(keys, want, "l's first leaves");
    let b = tree(&env, "b");
    let counts: Vec<usize> =
        children(&env, b.root).iter().take(3).map(|&(p, _)| children(&env, p).len()).collect();
    assert_eq!(counts, [7, 10, 7], "b's first branches");
    check(&env);
    drop(env);
    let (b0_last, b2_last) = (last[0].clone(), last[1].clone());
    Fixture { dir, path, b0_last, b2_last, next_txn }
}

/// Which neighbour the delete's merge absorbs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

/// The delete of each case from `table`: its key, the root's child index of the sibling it
/// absorbs, and of a page small enough to be absorbed in its place whose keys lie outside that
/// sibling's separators (a redirect's target).
fn target(fx: &Fixture, table: &str, side: Side) -> (Vec<u8>, usize, usize) {
    match (table, side) {
        ("l", Side::Left) => (l_key(12).to_vec(), 2, 0),
        ("l", Side::Right) => (l_key(0).to_vec(), 1, 3),
        (_, Side::Left) => (fx.b2_last.clone(), 1, 0),
        (_, Side::Right) => (fx.b0_last.clone(), 1, 2),
    }
}

/// A copy of the fixture's file, damaged by `damage`, opened (the open changing nothing). The
/// delete of `key` from `table`, the first operation of a write transaction, must be `Corrupt`
/// with a message holding `msg`; the environment must latch (writes fail) and the file must be
/// byte for byte as it was before the open.
fn refused(fx: &Fixture, what: &str, table: &str, key: &[u8], msg: &str, damage: &dyn Fn(&Path)) {
    let path = fx.dir.path().join(what.replace(' ', "_"));
    fs::copy(&fx.path, &path).expect("copy");
    damage(&path);
    let before = fs::read(&path).expect("read");
    let env = open(&path, SyncMode::Durable);
    {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table(table).expect("open").expect("the table");
        let result = txn.delete(&t, key);
        let Err(Error::Corrupt(m)) = &result else { panic!("{what}: {result:?}") };
        assert!(m.contains(msg), "{what}: expected {msg:?}: {m}");
    }
    assert!(env.failure().is_some(), "{what}: the damage latched");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))), "{what}: writes fail");
    drop(env);
    assert!(fs::read(&path).expect("read") == before, "{what}: the file changed");
    fs::remove_file(&path).expect("remove");
}

/// The fixture's deletes merge as it says: the undamaged delete of each case takes one leaf
/// (table `l`) or one leaf and one branch (table `b`) out of the tree, and the tree checks.
#[test]
fn the_fixture_deletes_merge() {
    let fx = fixture();
    for (table, side) in
        [("l", Side::Left), ("l", Side::Right), ("b", Side::Left), ("b", Side::Right)]
    {
        let path = fx.dir.path().join(format!("intact_{table}_{side:?}"));
        fs::copy(&fx.path, &path).expect("copy");
        let env = open(&path, SyncMode::Durable);
        let before = tree(&env, table);
        let (key, ..) = target(&fx, table, side);
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table(table).expect("open").expect("the table");
        assert!(txn.delete(&t, &key).expect("delete"));
        txn.commit().expect("commit");
        let after = tree(&env, table);
        let branches = u32::from(table == "b");
        assert_eq!(
            (before.leaf_pages - after.leaf_pages, before.branch_pages - after.branch_pages),
            (1, branches),
            "{table} {side:?}: the pages a merge takes out"
        );
        check(&env);
    }
}

/// A delete whose leaf merges refuses a damaged sibling, from either side, and changes nothing:
/// a flipped value byte (the CRC finds it), a CRC-valid root cell naming the sibling with a
/// stale txn, a root cell redirected (identity and all) to a leaf outside the cell's
/// separators, and an overflow cell in the sibling naming a run by this transaction's id past
/// the file's end (a run allocated later: the copy-time rule).
#[test]
fn a_merge_refuses_a_damaged_leaf_sibling() {
    let fx = fixture();
    for side in [Side::Left, Side::Right] {
        let (key, si, other) = target(&fx, "l", side);
        let r = {
            let env = open(&fx.path, SyncMode::Durable);
            tree(&env, "l").root
        };
        let root = read_page(&fx.path, r);
        let (sib, sib_txn) = page::branch_child(&root, si).expect("child");
        let (away, away_txn) = page::branch_child(&root, other).expect("child");
        let past_end = (fs::metadata(&fx.path).expect("stat").len() / P) as u32 + 2;
        let txn32 = fx.next_txn as u32;
        let case = |what: &str| format!("l {side:?} {what}");
        refused(&fx, &case("flipped byte"), "l", &key, "fails its CRC", &|p| {
            flip(p, u64::from(sib) * P + P - 5)
        });
        refused(&fx, &case("stale txn"), "l", &key, "is not the copy its parent names", &|p| {
            restamped(p, r, |page| {
                page::branch_set_child(page, si, sib, sib_txn.wrapping_sub(1)).expect("set")
            })
        });
        refused(&fx, &case("redirect"), "l", &key, "outside its parents' separators", &|p| {
            restamped(p, r, |page| page::branch_set_child(page, si, away, away_txn).expect("set"))
        });
        refused(&fx, &case("run allocated later"), "l", &key, "of a file of", &|p| {
            let ovf = Ovf { pgno: past_end, txn: txn32, len: 3 * P as u32 };
            restamped(p, sib, |page| {
                let fit = page::LeafMut::new(page)
                    .expect("leaf")
                    .replace_value(0, Val::Overflow(ovf))
                    .expect("forge");
                assert_eq!(fit, page::Fit::Done);
            })
        });
    }
}

/// A delete whose unlink leaves a branch under a quarter refuses a damaged branch sibling, from
/// either side, and changes nothing: a flipped child txn byte (the CRC finds it), a stale txn
/// in the root's cell, a redirect to a branch outside the cell's separators, a cell of the
/// sibling naming a page by this transaction's id past the file's end, and a right page of the
/// pair (the sibling `B1`, or `B2` on the path when it absorbs `B1` from its left) whose key 1
/// is the root's key coming down: the range check passes it. On the path the seam check finds
/// it; in the sibling the descent of its child-0 chain does first (child 0's range is empty).
#[test]
fn a_merge_refuses_a_damaged_branch_sibling() {
    let fx = fixture();
    for side in [Side::Left, Side::Right] {
        let (key, si, other) = target(&fx, "b", side);
        let r = {
            let env = open(&fx.path, SyncMode::Durable);
            tree(&env, "b").root
        };
        let root = read_page(&fx.path, r);
        let (sib, sib_txn) = page::branch_child(&root, si).expect("child");
        let (away, away_txn) = page::branch_child(&root, other).expect("child");
        let past_end = (fs::metadata(&fx.path).expect("stat").len() / P) as u32 + 2;
        let txn32 = fx.next_txn as u32;
        // The right page of the pair and the root's key for it.
        let sep_i = if side == Side::Left { 2 } else { 1 };
        let right = page::branch_child(&root, sep_i).expect("child").0;
        let sep = page::branch_key(&root, sep_i).expect("key").to_vec();
        let case = |what: &str| format!("b {side:?} {what}");
        refused(&fx, &case("flipped byte"), "b", &key, "fails its CRC", &|p| {
            let page = read_page(p, sib);
            let cell =
                usize::from(u16::from_le_bytes([page[page::HEADER], page[page::HEADER + 1]]));
            flip(p, u64::from(sib) * P + cell as u64 + 4);
        });
        refused(&fx, &case("stale txn"), "b", &key, "is not the copy its parent names", &|p| {
            restamped(p, r, |page| {
                page::branch_set_child(page, si, sib, sib_txn.wrapping_sub(1)).expect("set")
            })
        });
        refused(&fx, &case("redirect"), "b", &key, "outside its parents' separators", &|p| {
            restamped(p, r, |page| page::branch_set_child(page, si, away, away_txn).expect("set"))
        });
        refused(&fx, &case("page allocated later"), "b", &key, "of a file of", &|p| {
            restamped(p, sib, |page| page::branch_set_child(page, 1, past_end, txn32).expect("set"))
        });
        let seam = match side {
            Side::Left => "out of order",
            Side::Right => "outside its parents' separators",
        };
        refused(&fx, &case("seam"), "b", &key, seam, &|p| {
            restamped(p, right, |page| {
                let slot = page::HEADER + page::SLOT;
                let cell = usize::from(u16::from_le_bytes([page[slot], page[slot + 1]]));
                assert_eq!(&page[cell + 8..cell + 10], &180_u16.to_le_bytes(), "a 180-byte key");
                page[cell + 10..cell + 190].copy_from_slice(&sep);
            })
        });
    }
}
