//! The damage sweeps of `dup_sweep.rs` over a dup table that packs: every kind of damage to a
//! packed set, to the cells beside it and to the record's packed fields is `Corrupt` at the first
//! use that meets it, and latches. A read checks only what it touches, so each row-local case
//! damages row 3 of a 6-row set, the first probe of every binary search over it; order damage is
//! seen by a scan's step, a hidden key's by the write stage's put (which validates the whole set)
//! or, read-only, by `Env::check`. Beside the sweeps: `drop_table` and `clear_table` over the same
//! damage refuse before any free, and a [`crate::DupRoCursor`]'s seeks, steps and
//! `current_with_key_tail` meet it.

use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

use super::{
    damage::{tree, Stage},
    dup::{dup_key, restamp_cell, restamp_encoded, restamp_record},
    dup_sweep::{dup_fixture, raw_cell, sweep_dup_writing, Case, COLD, HOT},
    *,
};
use crate::{
    packed::{self, PackedView},
    page::{self, Find, Geometry, Val},
    tree::{cursor::State, read::descend, PageRead as _},
    DupOptions, Error, KeyTail,
};

/// The fixture's packed set whose row 3 the row-local cases damage.
const PK: u32 = 20;

/// The fixture's other 6-row packed set: the hidden key of the unread cases.
const PCOLD: u32 = 30;

/// The outer keys of the fixture's dup table: enough for an outer tree of two levels.
const OUTER: u32 = 400;

/// The dup table's options: 4-byte outer keys and a packed-set limit below `max_value(4)` at
/// 4 KiB pages, so a set over the limit is still an inline cell and can be forged.
const PACKING: DupOptions = DupOptions { key_len: 4, packed_max: 512 };

/// The outer key width.
const K: usize = PACKING.key_len as usize;

/// The rows of [`PK`] and [`PCOLD`].
const ROWS: u32 = 6;

/// The encoded length of [`PK`]'s and [`PCOLD`]'s sets: 6 rows of a 4-byte subkey and an 8-byte
/// value.
const BLOB: usize = 2 + 4 * 6 + 12 * 6;

/// A cleanly closed file holding dup table `d` (of [`PACKING`]: outer keys 0 to [`OUTER`] but
/// `PK + 1` and `PK + 2`, packed sets of 1 to 3 rows but [`HOT`]'s and [`COLD`]'s nested trees and
/// the 6-row sets of [`PK`] and [`PCOLD`]) and plain table `p` beside it.
struct PackedFixture {
    dir: TempDir,
    path: PathBuf,
    plain: Rows,
    dup: Rows,
    /// The outer leaf of each outer key.
    leaves: BTreeMap<u32, u32>,
    /// The packed sets of `d`.
    sets: u64,
}

/// The subkey of row `s` of [`PK`] and [`PCOLD`].
fn sub(s: u32) -> [u8; 4] {
    s.to_be_bytes()
}

fn packed_fixture() -> PackedFixture {
    let (dir, path) = temp("tndb_b_packed_damage");
    let (mut plain, mut dup) = (Rows::new(), Rows::new());
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", PACKING).expect("d");
    let p = txn.create_table("p").expect("p");
    for i in 0..300u32 {
        let (key, v) = (i.to_be_bytes().to_vec(), value(1, i, 40));
        txn.put(&p, &key, &v).expect("put");
        plain.insert(key, v);
    }
    let mut put = |txn: &mut RwTxn, key: Vec<u8>, v: Vec<u8>| {
        txn.put(&d, &key, &v).expect("put");
        dup.insert(key, v);
    };
    for k in 0..OUTER {
        match k {
            // 300 rows past the limit: two levels, and a value in a run.
            HOT => {
                for s in 0..300u32 {
                    put(&mut txn, dup_key(k, &[s as u8, 0x5a]), value(2, s, 40));
                }
                put(&mut txn, dup_key(k, b"big"), vec![0xb1; 10_000]);
            }
            // Three rows of 617 encoded bytes: a nested tree of one leaf.
            COLD => (0..3).for_each(|s| put(&mut txn, dup_key(k, &[s as u8]), value(2, s, 200))),
            // A set of 506 bytes, rewritten to `BLOB` below so its outer leaf has room for the
            // forged sets that grow.
            PK => (0..ROWS).for_each(|s| put(&mut txn, dup_key(k, &sub(s)), value(4, s, 76))),
            PCOLD => (0..ROWS).for_each(|s| put(&mut txn, dup_key(k, &sub(s)), value(4, s, 8))),
            _ => {
                (0..1 + k % 3).for_each(|s| put(&mut txn, dup_key(k, &[s as u8]), value(2, s, 40)))
            }
        }
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("rw");
    (0..ROWS).for_each(|s| put(&mut txn, dup_key(PK, &sub(s)), value(4, s, 8)));
    for k in [PK + 1, PK + 2] {
        assert!(txn.delete_prefix(&d, &k.to_be_bytes()).expect("delete_prefix") > 0, "{k}");
        dup.retain(|key, _| key[..K] != k.to_be_bytes());
    }
    txn.commit().expect("commit");
    check(&env);
    let snapshot = env.inner().current();
    let outer = tree(&env, "d");
    assert!(outer.height >= 2, "{outer:?}");
    let mut leaves = BTreeMap::new();
    for k in (0..OUTER).filter(|k| ![PK + 1, PK + 2].contains(k)) {
        let leaf = descend(&*snapshot, &outer, &k.to_be_bytes(), |_, _, _, _| Ok(()), None)
            .expect("descend")
            .expect("a leaf")
            .pgno;
        let page = snapshot.page(leaf).expect("the outer leaf");
        let Find::Found { value, .. } = page::leaf_find(page, &k.to_be_bytes()).expect("find")
        else {
            panic!("key {k}'s cell");
        };
        match (k, value) {
            (HOT | COLD, Val::Nested(_)) => {}
            (PK | PCOLD, Val::Inline(b)) => assert_eq!(b.len(), BLOB, "key {k}'s set"),
            (HOT | COLD | PK | PCOLD, v) => panic!("key {k}'s cell {v:?}"),
            (_, Val::Inline(_)) => {}
            (_, v) => panic!("key {k}'s cell {v:?}"),
        }
        leaves.insert(k, leaf);
    }
    let sets = outer.entries - 2;
    // The write stage rewrites every fifth row of `d`: one of each 6-row set.
    for k in [PK, PCOLD] {
        let at = dup.keys().position(|key| key[..K] == k.to_be_bytes()).expect("a row");
        assert!((at..at + ROWS as usize).any(|i| i % 5 == 0), "key {k}'s rows from {at}");
    }
    drop(snapshot);
    drop(env);
    PackedFixture { dir, path, plain, dup, leaves, sets }
}

/// A set's encoding from its rows, written apart from `packed.rs`'s encoder.
fn encode(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = (rows.len() as u16).to_le_bytes().to_vec();
    let mut end = 2 + 4 * rows.len();
    for (s, v) in rows {
        end += s.len() + v.len();
        out.extend_from_slice(&(end as u16).to_le_bytes());
        out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    }
    for (s, v) in rows {
        out.extend_from_slice(s);
        out.extend_from_slice(v);
    }
    out
}

/// Edit, in place, outer key `k`'s packed set in leaf `leaf` of the file at `path`, the leaf's
/// CRC stamped again.
fn in_set(path: &Path, leaf: u32, k: u32, edit: impl FnOnce(&mut [u8])) {
    raw_cell(path, leaf, &k.to_be_bytes(), |page, off| {
        let len = u32::from_le_bytes(page[off..off + 4].try_into().expect("4 bytes")) as usize;
        assert_eq!(page[off + 4..off + 8], [K as u8, 0, 0, 0], "key {k}: an inline cell");
        let at = off + page::LEAF_CELL_HEADER + K;
        edit(&mut page[at..at + len]);
    });
}

/// Set the `u16` at `at` of `blob`.
fn set16(blob: &mut [u8], at: usize, v: u16) {
    blob[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

/// The `u16` at `at` of `blob`.
fn get16(blob: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([blob[at], blob[at + 1]])
}

/// The offset of row `i`'s end in a set's slot table.
fn end_at(i: usize) -> usize {
    2 + 4 * i
}

/// The offset of row `i`'s subkey length in a set's slot table.
fn slen_at(i: usize) -> usize {
    4 + 4 * i
}

/// The offset of row `i`'s subkey in [`PK`]'s and [`PCOLD`]'s sets.
fn row_at(i: usize) -> usize {
    2 + 4 * ROWS as usize + 12 * i
}

/// An in-place edit of a packed set ([`in_set`]).
type SetEdit = fn(&mut [u8]);

/// Swap rows 1 and 2's subkeys of a 6-row set.
fn swap_1_2(b: &mut [u8]) {
    let (one, two) = (row_at(1), row_at(2));
    let s1: [u8; 4] = b[one..one + 4].try_into().expect("4 bytes");
    b.copy_within(two..two + 4, one);
    b[two..two + 4].copy_from_slice(&s1);
}

/// Table `d`'s record in the file at `path` with `edit` applied, re-encoded without the record
/// rules (a broken record), the catalog leaf's CRC stamped again.
fn broken_record(path: &Path, edit: impl FnOnce(&mut crate::catalog::DupRecord)) {
    restamp_encoded(path, |mut record| {
        edit(record.dup.as_mut().expect("dup"));
        record.encode()
    });
}

/// The names of the cases whose damage is in a packed set (or reads as one) or in the record's
/// `packed_rows` that only a walk sees: the free walks' cases.
const FREE_WALK_CASES: [&str; 14] = [
    "empty set",
    "short blob",
    "over packed_max",
    "slot table past the blob",
    "tiling short",
    "decreasing end",
    "empty subkey",
    "subkey past its row",
    "misordered",
    "duplicate subkey",
    "misordered, unread",
    "empty subkey, unread",
    "nested flag cleared",
    "packed_rows + 1",
];

/// The cases over `fx`, all but the one on a table that never packs ([`never_packs`]).
fn cases(fx: &PackedFixture) -> Vec<Case<'_>> {
    let (pk, pcold, hot) = (fx.leaves[&PK], fx.leaves[&PCOLD], fx.leaves[&HOT]);
    let row3 = move |edit: fn(&mut [u8])| {
        Box::new(move |p: &Path| in_set(p, pk, PK, edit)) as Box<dyn Fn(&Path)>
    };
    let sets = fx.sets;
    let geometry = Geometry::new(4096).expect("4 KiB");
    use Stage::{Check, Get, Scan, Write};
    vec![
        ("empty set", Get, Get, None, row3(|b| set16(b, 0, 0))),
        (
            "short blob",
            Get,
            Get,
            None,
            Box::new(move |p| {
                let short = [1, 0, 7, 0, 1, 0];
                assert!(short.len() < packed::PACKED_MIN);
                restamp_cell(p, pk, &PK.to_be_bytes(), &short[..])
            }),
        ),
        (
            "over packed_max",
            Get,
            Get,
            None,
            Box::new(move |p| {
                let rows = (0..ROWS)
                    .map(|s| (sub(s).to_vec(), vec![s as u8; 77 + usize::from(s == 5)]))
                    .collect::<Vec<_>>();
                let over = encode(&rows);
                assert_eq!(over.len(), 513);
                assert!(packed::validate(&over, 513, 64).is_ok(), "a valid set");
                restamp_cell(p, pk, &PK.to_be_bytes(), &over[..])
            }),
        ),
        ("slot table past the blob", Get, Get, None, row3(|b| set16(b, 0, 30))),
        ("tiling short", Get, Get, None, row3(|b| set16(b, end_at(5), BLOB as u16 - 1))),
        ("decreasing end", Get, Get, None, row3(|b| set16(b, end_at(3), get16(b, end_at(2)) - 1))),
        ("empty subkey", Get, Get, None, row3(|b| set16(b, slen_at(3), 0))),
        ("subkey past its row", Get, Get, None, row3(|b| set16(b, slen_at(3), 13))),
        ("misordered", Scan, Scan, None, row3(swap_1_2)),
        (
            "duplicate subkey",
            Scan,
            Scan,
            None,
            row3(|b| b.copy_within(row_at(1)..row_at(1) + 4, row_at(2))),
        ),
        (
            "misordered, unread",
            Write,
            Check,
            Some(PCOLD),
            Box::new(move |p| in_set(p, pcold, PCOLD, swap_1_2)),
        ),
        (
            "empty subkey, unread",
            Write,
            Check,
            Some(PCOLD),
            Box::new(move |p| in_set(p, pcold, PCOLD, |b| set16(b, slen_at(3), 0))),
        ),
        (
            "nested flag cleared",
            Get,
            Get,
            None,
            Box::new(move |p| {
                raw_cell(p, hot, &HOT.to_be_bytes(), |page, off| {
                    assert_eq!(page[off..off + 8], [32, 0, 0, 0, 4, 0, 2, 0], "a nested cell");
                    page[off + 6] = 0;
                    let at = off + page::LEAF_CELL_HEADER + K;
                    let root = &page[at..at + page::NESTED_CELL];
                    assert!(PackedView::new(root, 512).is_err(), "the root is no set: {root:02x?}");
                })
            }),
        ),
        (
            "packed_rows + 1",
            Check,
            Check,
            None,
            Box::new(|p| restamp_record(p, |r| r.dup.as_mut().expect("dup").packed_rows += 1)),
        ),
        (
            "packed_rows 0 with sets",
            Get,
            Get,
            None,
            Box::new(|p| broken_record(p, |d| d.packed_rows = 0)),
        ),
        (
            "too many packed rows",
            Get,
            Get,
            None,
            Box::new(move |p| broken_record(p, |d| d.packed_rows = sets * (512 - 2) / 5 + 1)),
        ),
        (
            "limit below the smallest set",
            Get,
            Get,
            None,
            Box::new(|p| broken_record(p, |d| d.packed_max = 3)),
        ),
        (
            "limit past the page",
            Get,
            Get,
            None,
            Box::new(move |p| {
                let past = geometry.max_value(K) + 1;
                broken_record(p, |d| d.packed_max = u16::try_from(past).expect("a u16"))
            }),
        ),
    ]
}

/// Damage `damage` on a fresh copy of `src` swept by [`sweep_dup_writing`]: the gets and the
/// scan over `dup` without `hide`'s rows, the write stage over all of `dup`.
fn sweep_case(
    src: &Path,
    plain: &Rows,
    dup: &Rows,
    (what, write_stage, ro_stage, hide, damage): &Case<'_>,
    read_only: bool,
) {
    let path = src.with_file_name("case");
    fs::copy(src, &path).expect("copy");
    damage(&path);
    let mut read = dup.clone();
    if let Some(k) = hide {
        read.retain(|key, _| key[..K] != k.to_be_bytes());
    }
    let env = match read_only {
        true => Env::open_read_only(&path, EnvOptions::default()).expect("open read-only"),
        false => open(&path, SyncMode::Durable),
    };
    let stage = if read_only { *ro_stage } else { *write_stage };
    sweep_dup_writing(&env, plain, &read, dup, *hide, what, stage, read_only);
    drop(env);
    fs::remove_file(&path).expect("remove");
}

/// The case "packed set at packed_max 0": a valid set as [`COLD`]'s cell in `dup_sweep.rs`'s
/// fixture, a table that never packs. Swept, then on a fresh copy a get and a dup cursor's
/// landing are `Corrupt` with the message of a cell that is not a nested tree (the landing does
/// not decode it as a set).
fn never_packs(read_only: bool) {
    let fx = dup_fixture();
    let leaf = fx.cells[&COLD].0;
    let damage = move |p: &Path| {
        let set = encode(&[(vec![0], b"v".to_vec())]);
        assert!(packed::validate(&set, 512, 64).is_ok(), "a valid set");
        restamp_cell(p, leaf, &COLD.to_be_bytes(), &set[..])
    };
    let case: Case<'_> =
        ("packed set at packed_max 0", Stage::Get, Stage::Get, None, Box::new(damage));
    sweep_case(&fx.path, &fx.plain, &fx.dup, &case, read_only);
    let path = fx.dir.path().join("never packs");
    fs::copy(&fx.path, &path).expect("copy");
    damage(&path);
    let env = Env::open_read_only(&path, EnvOptions::default()).expect("open read-only");
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let d = ro.open_table("d").expect("open").expect("d");
    let refused = |r: &crate::Result<_>| {
        matches!(r, Err(Error::Corrupt(m))
            if m.contains("the outer cell of key [00, 00, 00, 08] holds no nested tree"))
    };
    let got = ro.get(&d, &dup_key(COLD, &[0])).map(|v| v.map(<[u8]>::to_vec));
    assert!(refused(&got), "get: {got:?}");
    let mut cursor = ro.dup_cursor_owned(&d).expect("cursor");
    let landed = cursor.seek_key(&COLD.to_be_bytes()).map(|r| r.map(|(k, _)| k.to_vec()));
    assert!(refused(&landed), "seek_key: {landed:?}");
}

/// Each case on a fresh copy, then the case on a table that never packs.
fn run(read_only: bool) {
    let fx = packed_fixture();
    for case in &cases(&fx) {
        sweep_case(&fx.path, &fx.plain, &fx.dup, case, read_only);
    }
    never_packs(read_only);
}

/// Every damage case of a dup table that packs is `Corrupt` at its first use by gets, scans, a
/// write transaction or `Env::check`, and latches.
#[test]
fn damage_sweep_on_a_packed_dup_table() {
    run(false);
}

/// Every damage case of a dup table that packs is `Corrupt` at its first use by a read-only
/// environment's gets, scans or `Env::check`, latched by that operation.
#[test]
fn read_only_sweep_on_a_packed_dup_table() {
    run(true);
}

/// `drop_table` and `clear_table` over a damaged packed set (or a `packed_rows` only a walk sees)
/// are `Corrupt` and latch before they free anything: a reopen shows the free list and the table
/// as they were.
#[test]
fn packed_damage_refuses_drop_and_clear_before_any_free() {
    let fx = packed_fixture();
    let cases = cases(&fx);
    for name in FREE_WALK_CASES {
        let (_, _, _, _, damage) = cases.iter().find(|c| c.0 == name).expect("a case");
        for op in ["drop", "clear"] {
            let path = fx.dir.path().join("case");
            fs::copy(&fx.path, &path).expect("copy");
            damage(&path);
            let figures = |env: &Env| {
                let ro = env.begin_ro().expect("ro");
                let d = ro.open_table("d").expect("open").expect("d");
                (env.free_pages().expect("free pages"), ro.table_stat(&d).expect("stat"))
            };
            let env = open(&path, SyncMode::Durable);
            let before = figures(&env);
            let mut txn = env.begin_rw().expect("rw");
            let d = txn.open_table("d").expect("open").expect("d");
            let result = match op {
                "drop" => txn.drop_table(d),
                _ => txn.clear_table(&d),
            };
            assert!(matches!(&result, Err(Error::Corrupt(_))), "{name} {op}: {result:?}");
            assert!(env.failure().is_some(), "{name} {op}: latched");
            drop(txn);
            drop(env);
            let env = open(&path, SyncMode::Durable);
            assert_eq!(figures(&env), before, "{name} {op}: nothing freed");
            drop(env);
            fs::remove_file(&path).expect("remove");
        }
    }
}

/// A move of a [`crate::DupRoCursor`] that meets damage. A seek inside [`PK`] probes row 3
/// first, so a step onto row 3 starts from a neighbouring key.
#[derive(Clone, Copy, Debug)]
enum Move {
    /// `seek` of this row.
    Seek(&'static [u8]),
    /// `seek_exact` of the row (key, subkey), then `next` (`true`) or `prev`, `steps` times.
    Step(u32, &'static [u8], bool, usize),
}

/// Damage met by a [`crate::DupRoCursor`] in a snapshot: a seek whose search probes the damaged
/// row, a step onto it (or across into a set that does not decode), a forward and a backward walk
/// over a swapped pair are `Corrupt`, unset the cursor and latch. A `current_with_key_tail`
/// re-reads the set from the outer leaf: damage written under a live read-only mapping (a row's
/// subkey length, the row count) is `Corrupt` there too.
#[test]
fn packed_damage_met_by_a_dup_read_cursor_is_corrupt() {
    let fx = packed_fixture();
    let cases = cases(&fx);
    const R3: &[u8] = &[0, 0, 0, 3];
    let moves: [(&str, Move); 13] = [
        ("decreasing end", Move::Seek(R3)),
        ("decreasing end", Move::Step(PK - 1, &[1], true, 4)),
        ("empty subkey", Move::Seek(R3)),
        ("empty subkey", Move::Step(PK + 3, &[0], false, 3)),
        ("subkey past its row", Move::Seek(R3)),
        ("subkey past its row", Move::Step(PK - 1, &[1], true, 4)),
        ("empty set", Move::Seek(&[0, 0, 0, 0])),
        ("short blob", Move::Step(PK - 1, &[1], true, 1)),
        ("over packed_max", Move::Seek(&[0, 0, 0, 0])),
        ("tiling short", Move::Step(PK + 3, &[0], false, 1)),
        ("misordered", Move::Step(PK, &[0, 0, 0, 0], true, 2)),
        ("misordered", Move::Step(PK, &[0, 0, 0, 3], false, 2)),
        ("duplicate subkey", Move::Step(PK, &[0, 0, 0, 1], true, 1)),
    ];
    for (name, mv) in moves {
        let (_, _, _, _, damage) = cases.iter().find(|c| c.0 == name).expect("a case");
        let path = fx.dir.path().join("case");
        fs::copy(&fx.path, &path).expect("copy");
        damage(&path);
        let env = open(&path, SyncMode::Durable);
        let ro = Arc::new(env.begin_ro().expect("ro"));
        let d = ro.open_table("d").expect("open").expect("d");
        let mut c = ro.dup_cursor_owned(&d).expect("cursor");
        let result = match mv {
            Move::Seek(s) => c.seek(&dup_key(PK, s)).map(drop),
            Move::Step(k, s, forward, steps) => {
                let at = c.seek_exact(&dup_key(k, s)).expect("the row before").is_some();
                assert!(at, "{name}: {k} {s:02x?} reads");
                (1..steps).for_each(|_| {
                    let r = if forward { c.next() } else { c.prev() };
                    assert!(r.expect("an undamaged step").is_some(), "{name} {mv:?}");
                });
                if forward { c.next() } else { c.prev() }.map(drop)
            }
        };
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains("dup table d")),
            "{name} {mv:?}: {result:?}"
        );
        assert_eq!(c.state(), State::Unset, "{name} {mv:?}");
        assert!(env.failure().is_some(), "{name} {mv:?}: latched");
        drop((c, ro, env));
        fs::remove_file(&path).expect("remove");
    }
    // Damage under a cursor at row 2 of PK, written to the file through the live mapping.
    let pk = fx.leaves[&PK];
    let live: [(&str, &str, SetEdit); 2] = [
        ("a subkey of 0 bytes", "the packed set of key [00, 00, 00, 14]", |b| {
            set16(b, slen_at(2), 0)
        }),
        ("another row count", "changed under a cursor", |b| {
            set16(b, 0, ROWS as u16 - 1);
            set16(b, end_at(4), BLOB as u16);
        }),
    ];
    for (what, message, edit) in live {
        let path = fx.dir.path().join("live");
        fs::copy(&fx.path, &path).expect("copy");
        let env = Env::open_read_only(&path, EnvOptions::default()).expect("open read-only");
        let ro = Arc::new(env.begin_ro().expect("ro"));
        let d = ro.open_table("d").expect("open").expect("d");
        let mut c = ro.dup_cursor_owned(&d).expect("cursor");
        assert!(c.seek_exact(&dup_key(PK, &sub(2))).expect("row 2").is_some(), "{what}");
        let tail = c
            .current_with_key_tail(2)
            .expect("before")
            .map(|(_, t)| matches!(t, KeyTail::Joined(_)));
        assert_eq!(tail, Some(true), "{what}: a joined tail before the damage");
        in_set(&path, pk, PK, edit);
        let result = c.current_with_key_tail(2).map(|r| r.map(|(k, _)| k.to_vec()));
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)),
            "{what}: {result:?}"
        );
        assert!(env.failure().is_some(), "{what}: latched");
        drop((c, ro, env));
        fs::remove_file(&path).expect("remove");
    }
}
