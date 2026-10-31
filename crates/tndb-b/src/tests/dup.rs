//! Dup tables through the public API: the two kinds of table and the calls each takes, the
//! options a dup table is created with, and the records this build refuses.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use tempfile::TempDir;

use super::{
    check,
    damage::{restamped, tree},
    open, temp,
};
use crate::{
    catalog::{DupRecord, TableRecord},
    env::SyncMode,
    overflow,
    page::{self, Find, Val},
    tree::{
        read::descend,
        test_harness::{walk_outer_tree, walk_packing_tree, ReportSeed, Rng},
        PageRead as _, TreeRoot,
    },
    CheckReport, DupOptions, Env, EnvOptions, Error, ReadTxn, Result, TableStat,
};

/// The options of the dup tables these tests create.
pub(super) const OPTIONS: DupOptions = DupOptions { key_len: 4, packed_max: 0 };

/// The packed-set limits the dup oracles run at: none (every key a nested tree), a small one (most
/// keys are created packed and promote within a few rows) and the default (the most an inline entry
/// takes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Bound {
    Unpacked,
    Small,
    Default,
}

/// [`Bound::Small`]'s limit, in bytes of a packed set's encoding.
pub(super) const PACKED_SMALL: u16 = 96;

impl Bound {
    /// Every bound, in the order the oracles run them.
    pub(super) const ALL: [Self; 3] = [Self::Unpacked, Self::Small, Self::Default];

    /// The options of a dup table of `key_len`-byte outer keys at this bound.
    pub(super) const fn options(self, key_len: u16) -> DupOptions {
        let packed_max = match self {
            Self::Unpacked => 0,
            Self::Small => PACKED_SMALL,
            Self::Default => DupOptions::PACKED_DEFAULT,
        };
        DupOptions { key_len, packed_max }
    }
}

#[test]
fn every_bound_resolves_to_its_limit() {
    let most = DupOptions::packed(OPTIONS.key_len).resolved(4096).expect("default").packed_max;
    let limits: Vec<u16> = Bound::ALL
        .iter()
        .map(|b| b.options(OPTIONS.key_len).resolved(4096).expect("resolves").packed_max)
        .collect();
    assert_eq!(limits, [0, PACKED_SMALL, most]);
    assert!(PACKED_SMALL < most, "the small bound is below the default");
}

/// `Env::check` of `env`, which must pass, with dup table `name`'s figures those of its
/// `table_stat` ([`assert_dup_figures`]).
#[track_caller]
fn check_dup(env: &Env, name: &str) -> CheckReport {
    let report = check(env);
    let ro = env.begin_ro().expect("ro");
    assert_dup_figures(&report, &ro, name);
    report
}

/// Dup table `name`'s figures in `report` (an `Env::check` of `txn`'s state) are those of its
/// `table_stat`: its rows, its outer and nested pages (runs included) and its outer tree's height.
#[track_caller]
pub(super) fn assert_dup_figures(report: &CheckReport, txn: &impl ReadTxn, name: &str) {
    let table = txn.open_table(name).expect("open").expect("the table");
    let stat = txn.table_stat(&table).expect("stat");
    assert!(stat.dup.is_some(), "{name} is a dup table");
    let pages =
        u64::from(stat.branch_pages) + u64::from(stat.leaf_pages) + u64::from(stat.overflow_pages);
    let found = report.tables[name];
    assert_eq!((found.entries, found.pages, found.height), (stat.entries, pages, stat.height));
}

/// A copy of the file at `path`, opened.
pub(super) fn copy_of(path: &Path) -> (TempDir, Env) {
    let (dir, copy) = temp("tndb_b_dup_case");
    fs::copy(path, &copy).expect("copy");
    let env = open(&copy, SyncMode::Durable);
    (dir, env)
}

/// `result` is `Error::TableKind` naming `name`.
#[track_caller]
fn assert_kind<T: std::fmt::Debug>(what: &str, result: Result<T>, name: &str) {
    assert!(matches!(&result, Err(Error::TableKind(n)) if n == name), "{what}: {result:?}");
}

/// Each call for one kind of table refuses the other with `TableKind`, before anything changes
/// and with no latch: `create_table` on a dup table, `create_dup_table` on a plain one or with
/// other options, a plain cursor of each sort on a dup table (a read-only transaction's twice:
/// the catalog's answer, then its snapshot's remembered one), `delete_prefix` on a plain table.
/// `create_dup_table` with the table's own options and `create_table` of a plain table return
/// their handles, changing nothing (the transaction commits as its base).
#[test]
fn table_kinds_are_checked() {
    let (_dir, path) = temp("tndb_b_dup_kinds");
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let p = txn.create_table("p").expect("p");
    txn.put(&p, b"key", b"value").expect("put");
    assert_kind("create_table on d, same txn", txn.create_table("d"), "d");
    assert_kind("create_dup_table on p, same txn", txn.create_dup_table("p", OPTIONS), "p");
    let base = txn.commit().expect("commit");

    let mut txn = env.begin_rw().expect("rw");
    assert_kind("create_table on d", txn.create_table("d"), "d");
    assert_kind("create_dup_table on p", txn.create_dup_table("p", OPTIONS), "p");
    for other in [DupOptions { key_len: 5, ..OPTIONS }, DupOptions { key_len: 3, ..OPTIONS }] {
        assert_kind("create_dup_table, other options", txn.create_dup_table("d", other), "d");
    }
    assert_eq!(txn.create_dup_table("d", OPTIONS).expect("the same options"), d);
    assert_eq!(txn.create_table("p").expect("a plain table"), p);
    assert_kind("rw cursor", txn.cursor(&d).map(drop), "d");
    assert_kind("cursor_mut", txn.cursor_mut(&d).map(drop), "d");
    assert_kind("cursor_detached", txn.cursor_detached(&d), "d");
    assert_kind("delete_prefix on p", txn.delete_prefix(&p, b"key"), "p");
    assert!(txn.cursor(&p).is_ok() && txn.cursor_detached(&p).is_ok(), "plain cursors on p");
    assert!(txn.cursor_mut(&p).is_ok());
    assert_eq!(txn.commit().expect("commit"), base, "nothing changed");

    let ro = Arc::new(env.begin_ro().expect("ro"));
    for pass in ["from the catalog", "from the snapshot"] {
        assert_kind(pass, ro.cursor(&d).map(drop), "d");
        assert_kind(pass, ro.cursor_owned(&d), "d");
    }
    assert!(ro.cursor(&p).is_ok() && ro.cursor_owned(&p).is_ok(), "plain cursors on p");
    assert_eq!(ro.get(&p, b"key").expect("get"), Some(&b"value"[..]));
    assert!(env.failure().is_none(), "TableKind does not latch");

    let mut txn = env.begin_rw().expect("rw");
    txn.put(&p, b"later", b"write").expect("a later write");
    assert!(txn.commit().expect("commit") > base);
    check(&env);
}

/// `create_dup_table` refuses an empty outer key, one past [`Geometry::max_dup_key_len`] and a
/// `packed_max` outside `7..=`[`Geometry::max_value`]`(key_len)` (but 0) with `InvalidDupOptions`,
/// creating nothing and with no latch (on a new name and on an existing one), at 4 KiB and 16 KiB
/// pages; the widest key is accepted, and its table takes rows of a 1-byte subkey, enough of them
/// for an outer tree of two levels.
///
/// [`Geometry::max_dup_key_len`]: crate::page::Geometry::max_dup_key_len
/// [`Geometry::max_value`]: crate::page::Geometry::max_value
#[test]
fn dup_options_are_validated() {
    for page_size in [4096, 16384] {
        let (_dir, path) = temp("tndb_b_dup_options");
        let env =
            Env::open(&path, EnvOptions { page_size, ..EnvOptions::default() }).expect("open");
        let max = env.inner().geometry.max_dup_key_len();
        let max = u16::try_from(max).expect("a u16 key width");
        assert!(usize::from(max) < env.max_key_len(), "{page_size}");
        let mut txn = env.begin_rw().expect("rw");
        let widest = DupOptions { key_len: max, packed_max: 0 };
        let w = txn.create_dup_table("widest", widest).expect("the widest outer key");
        assert_eq!(txn.table_stat(&w).expect("stat").dup, Some(widest));
        let most = env.inner().geometry.max_value(usize::from(OPTIONS.key_len));
        let widest_most = env.inner().geometry.max_value(usize::from(max));
        let range = |n: usize, most: usize| format!("packed_max {n} outside 7..={most} for this");
        for (name, options, what) in [
            ("bad", DupOptions { key_len: 0, ..OPTIONS }, "key_len 0 outside".to_string()),
            ("bad", DupOptions { key_len: max + 1, ..OPTIONS }, "outside 1..".to_string()),
            ("bad", DupOptions::packed(0), "key_len 0 outside".to_string()),
            ("bad", DupOptions { packed_max: 1, ..OPTIONS }, range(1, most)),
            ("bad", DupOptions { packed_max: 6, ..OPTIONS }, range(6, most)),
            ("bad", DupOptions { packed_max: most as u16 + 1, ..OPTIONS }, range(most + 1, most)),
            ("widest", DupOptions { packed_max: 1, ..widest }, range(1, widest_most)),
            ("widest", DupOptions { packed_max: 33, ..widest }, range(33, widest_most)),
        ] {
            let result = txn.create_dup_table(name, options);
            assert!(
                matches!(&result, Err(Error::InvalidDupOptions(m)) if m.contains(&what)),
                "{page_size} {name} {options:?}: {result:?}"
            );
        }
        assert_eq!(txn.open_table("bad").expect("open"), None);
        txn.commit().expect("commit");
        assert!(env.failure().is_none(), "{page_size}: no latch");
        let ro = env.begin_ro().expect("ro");
        assert_eq!(ro.open_table("bad").expect("open"), None, "nothing created");
        assert_eq!(ro.table_stat(&w).expect("stat").dup, Some(widest));
        check(&env);
        drop(ro);
        // The widest outer key with a 1-byte subkey: enough keys for an outer tree of two levels.
        let key = |k: u8, s: u8| [&vec![k; usize::from(max)][..], &[s]].concat();
        let mut txn = env.begin_rw().expect("rw");
        for k in 0..12u8 {
            for s in 0..2u8 {
                txn.put(&w, &key(k, s), &[k, s]).expect("a 1-byte subkey at the widest outer key");
            }
        }
        let short = txn.get(&w, &vec![1; usize::from(max)]);
        let min = usize::from(max) + 1;
        assert!(matches!(short, Err(Error::KeyTooShort { min: m, .. }) if m == min), "{short:?}");
        txn.commit().expect("commit");
        check_dup(&env, "widest");
        let ro = env.begin_ro().expect("ro");
        for k in 0..12u8 {
            for s in 0..2u8 {
                assert_eq!(ro.get(&w, &key(k, s)).expect("get"), Some(&[k, s][..]), "{page_size}");
            }
        }
        let stat = ro.table_stat(&w).expect("stat");
        assert_eq!((stat.entries, stat.nested_trees, stat.height), (24, 12, 2), "{page_size}");
        assert_eq!(ro.dup_rows_for_test(&w).len(), 24);
        assert!(env.failure().is_none(), "{page_size}: no latch");
    }
}

/// An empty dup table's figures are all zero but its shape, in the transaction that created it,
/// after its commit, and in a read-only transaction; a plain table's say no shape.
#[test]
fn an_empty_dup_tables_stat_is_its_shape() {
    let (_dir, path) = temp("tndb_b_dup_stat");
    let env = open(&path, SyncMode::Durable);
    let shape = DupOptions { key_len: 20, packed_max: 0 };
    let empty = TableStat { dup: Some(shape), ..TableStat::default() };
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", shape).expect("d");
    let p = txn.create_table("p").expect("p");
    assert_eq!(txn.table_stat(&d).expect("stat"), empty, "in its own txn");
    assert_eq!(txn.table_stat(&p).expect("stat"), TableStat::default());
    txn.commit().expect("commit");
    let txn = env.begin_rw().expect("rw");
    assert_eq!(txn.table_stat(&d).expect("stat"), empty, "after its commit");
    drop(txn);
    let ro = env.begin_ro().expect("ro");
    for pass in ["from the catalog", "from the snapshot"] {
        assert_eq!(ro.table_stat(&d).expect("stat"), empty, "{pass}");
        assert_eq!(ro.table_stat(&p).expect("stat"), TableStat::default(), "{pass}");
    }
    assert_eq!(check(&env).tables["d"].entries, 0);
}

/// A file with an empty dup table `d` (of [`OPTIONS`]) and a plain table `p` holding one row,
/// whose catalog leaf holds both records: returns the path and `d`'s handle after `d`'s record
/// was replaced in place by `encode`'s bytes for it (which need not keep the record rules), the
/// leaf's CRC stamped again.
fn with_record(
    dir: &Path,
    encode: impl FnOnce(TableRecord) -> [u8; crate::catalog::RECORD_LEN],
) -> (PathBuf, crate::Table) {
    let path = dir.join("db");
    let d = {
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.create_dup_table("d", OPTIONS).expect("d");
        let p = txn.create_table("p").expect("p");
        txn.put(&p, b"key", b"value").expect("put");
        txn.commit().expect("commit");
        d
    };
    restamp_encoded(&path, encode);
    (path, d)
}

/// Table `d`'s record in the cleanly closed file at `path`, whose catalog is one leaf, changed by
/// `edit` in place (keeping the record rules), the leaf's CRC stamped again.
pub(super) fn restamp_record(path: &Path, edit: impl FnOnce(&mut TableRecord)) {
    restamp_encoded(path, |mut record| {
        edit(&mut record);
        let bytes = record.encode();
        assert_eq!(TableRecord::decode(&bytes).expect("the record rules hold"), record);
        bytes
    });
}

/// Table `d`'s record in the cleanly closed file at `path`, whose catalog is one leaf, replaced in
/// place by `encode`'s bytes for it, the leaf's CRC stamped again.
pub(super) fn restamp_encoded(
    path: &Path,
    encode: impl FnOnce(TableRecord) -> [u8; crate::catalog::RECORD_LEN],
) {
    let catalog = {
        let env = open(path, SyncMode::Durable);
        let meta = env.inner().current().meta;
        assert_eq!(meta.catalog.height, 1, "the catalog is one leaf");
        meta.catalog.root
    };
    restamped(path, catalog, |page| {
        let i = (0..page::nkeys(page))
            .find(|&i| page::leaf_inline_entry(page, i).expect("entry").0 == b"d")
            .expect("d's record");
        let (_, value) = page::leaf_inline_entry(page, i).expect("entry");
        let off = value.as_ptr() as usize - page.as_ptr() as usize;
        let bytes = encode(TableRecord::decode(value).expect("record"));
        page[off..off + bytes.len()].copy_from_slice(&bytes);
    });
}

/// A dup record whose `packed_max` is out of range is `Corrupt` wherever the table is resolved:
/// either transaction's `open_table`, `create_table` of its name, a get and a table stat through a
/// handle taken before, and `Env::check`. A limit of 1..=6 breaks a record rule (decode refuses
/// it); one past the longest inline value of the outer key, or the request
/// [`DupOptions::PACKED_DEFAULT`] itself, keeps every rule but is a record this build does not
/// serve, as is an outer key one byte past the page size's widest. Each latches; the plain table
/// beside it reads on.
#[test]
fn a_dup_record_with_packed_max_out_of_range_is_refused() {
    let geometry = crate::page::Geometry::new(4096).expect("4 KiB");
    let wide = 1 + geometry.max_dup_key_len() as u16;
    let most = geometry.max_value(usize::from(OPTIONS.key_len)) as u16;
    let past = format!("packed_max {} outside 7..={most} for this page size", most + 1);
    let request = format!("packed_max {} outside 7..={most}", DupOptions::PACKED_DEFAULT);
    for (case, message) in [
        ("low", "a packed-set limit below the smallest set"),
        ("high", past.as_str()),
        ("request", request.as_str()),
        ("wide", "past this page size"),
    ] {
        let dir = TempDir::with_prefix("tndb_b_dup_packed").expect("temp dir");
        let (path, d) = with_record(dir.path(), |mut record| {
            let dup = record.dup.as_mut().expect("a dup record");
            match case {
                "low" => dup.packed_max = 6,
                "high" => dup.packed_max = most + 1,
                "request" => dup.packed_max = DupOptions::PACKED_DEFAULT,
                _ => dup.key_len = wide,
            }
            let bytes = record.encode();
            let decoded = TableRecord::decode(&bytes);
            assert_eq!(decoded.is_ok(), case != "low", "{case}: the record rules");
            bytes
        });
        {
            let (_dir, env) = copy_of(&path);
            let ro = env.begin_ro().expect("ro");
            let p = ro.open_table("p").expect("a plain table's record").expect("p");
            assert_eq!(ro.get(&p, b"key").expect("get"), Some(&b"value"[..]));
            assert!(env.failure().is_none(), "{case}: the plain table met no damage");
        }
        let uses = ["ro open_table", "ro get", "ro stat", "rw open_table", "create_table", "check"];
        for what in uses {
            let (_dir, env) = copy_of(&path);
            let result = match what {
                "ro open_table" => env.begin_ro().expect("ro").open_table("d").map(drop),
                "ro get" => env.begin_ro().expect("ro").get(&d, b"keyX").map(drop),
                "ro stat" => env.begin_ro().expect("ro").table_stat(&d).map(drop),
                "rw open_table" => env.begin_rw().expect("rw").open_table("d").map(drop),
                "create_table" => env.begin_rw().expect("rw").create_table("d").map(drop),
                _ => env.check().map(drop),
            };
            assert!(
                matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)),
                "{case} {what}: {result:?}"
            );
            assert!(env.failure().is_some(), "{case} {what}: latched");
        }
    }
}

/// A model of a dup table's rows: logical key (outer key then subkey) to value.
type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

/// The logical key of outer key number `k` (a big-endian [`OPTIONS`]-wide key) and subkey `s`.
pub(super) fn dup_key(k: u32, s: &[u8]) -> Vec<u8> {
    [&k.to_be_bytes()[..], s].concat()
}

/// The rows a transaction holds (`rows`, from a `dup_rows_for_test`, which checks every tree and
/// the totals) and its `get` of each model row, of a missing subkey of each outer key, of an outer
/// key with no rows and of a key with no subkey, against `model`.
#[track_caller]
fn assert_rows(
    what: &str,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    get: impl Fn(&[u8]) -> Result<Option<Vec<u8>>>,
    model: &Rows,
) {
    let expected: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert!(rows == expected, "{what}: {} rows, the model {}", rows.len(), expected.len());
    for (key, value) in model {
        assert_eq!(get(key).expect("get").as_ref(), Some(value), "{what}: get {key:02x?}");
        let missing = [&key[..4], &[0xff; 9][..]].concat();
        assert_eq!(get(&missing).expect("get"), model.get(&missing).cloned(), "{what}");
    }
    assert_eq!(get(&dup_key(u32::MAX, b"s")).expect("get"), None, "{what}: no such outer key");
    let short = get(&[0; 4]);
    assert!(matches!(short, Err(Error::KeyTooShort { len: 4, min: 5 })), "{what}: {short:?}");
}

/// Puts over 200 outer keys, a few of them hot enough to grow nested trees of several leaves
/// (one with a value in an overflow run), replaced values, a delete miss and deletes that empty
/// keys, in two transactions: every row reads back, by `get` and by a walk of every tree (with
/// the totals), through the working map before each commit, after it and after a reopen.
#[test]
fn dup_rows_round_trip_through_commit_and_reopen() {
    let (_dir, path) = temp("tndb_b_dup_round_trip");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut rng = Rng(0x5eed_d0b5);
    let hot = |k: u32| k % 40 == 7;
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    for k in 0..200u32 {
        let rows = if hot(k) { 300 } else { 1 + rng.below(3) };
        for _ in 0..rows {
            let s = rng.next().to_be_bytes()[..1 + rng.len_upto(7)].to_vec();
            let value = vec![k as u8; rng.len_upto(40)];
            let key = dup_key(k, &s);
            txn.put(&d, &key, &value).expect("put");
            model.insert(key, value);
        }
    }
    let big = (dup_key(47, b"big"), vec![0xb1; 10_000]);
    txn.put(&d, &big.0, &big.1).expect("a value in a run");
    model.insert(big.0.clone(), big.1.clone());
    let get = |txn: &crate::RwTxn, key: &[u8]| txn.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("first txn", txn.dup_rows_for_test(&d), |key| get(&txn, key), &model);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let stat = env.begin_ro().expect("ro").table_stat(&d).expect("stat");
    assert_eq!(stat.nested_trees, 200, "{stat:?}");
    // The second transaction loads committed trees from their cells and copies them.
    let mut txn = env.begin_rw().expect("rw");
    assert_rows(
        "committed, in a write txn",
        txn.dup_rows_for_test(&d),
        |key| get(&txn, key),
        &model,
    );
    let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
    for key in &keys {
        let k = u32::from_be_bytes(key[..4].try_into().expect("an outer key"));
        if k % 7 == 3 || (hot(k) && rng.below(2) == 0) {
            assert!(txn.delete(&d, key).expect("delete"), "{key:02x?}");
            model.remove(key);
        } else if rng.below(4) == 0 {
            let value = vec![0xee; rng.len_upto(60)];
            txn.put(&d, key, &value).expect("replace");
            model.insert(key.clone(), value);
        }
    }
    assert!(!txn.delete(&d, &dup_key(0, &[0xff; 9])).expect("delete miss"));
    assert!(!txn.delete(&d, &dup_key(3, b"gone")).expect("an emptied key"));
    for k in [1000u32, 1001] {
        let key = dup_key(k, b"new");
        txn.insert(&d, &key, b"v").expect("insert");
        model.insert(key, b"v".to_vec());
    }
    assert_rows("second txn", txn.dup_rows_for_test(&d), |key| get(&txn, key), &model);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let ro_get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after the commit", ro.dup_rows_for_test(&d), ro_get, &model);
    drop(ro);
    assert!(env.failure().is_none(), "no latch");
    drop(env);
    let env = open(&path, SyncMode::Durable);
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let d = ro.open_table("d").expect("open").expect("d");
    let ro_get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after a reopen", ro.dup_rows_for_test(&d), ro_get, &model);
    let txn = env.begin_rw().expect("rw");
    let get = |key: &[u8]| txn.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after a reopen, in a write txn", txn.dup_rows_for_test(&d), get, &model);
}

/// A transaction's dup writes (new keys, rows of committed keys, a key emptied, a hot key's rows
/// replaced, an unchanged entry dropped from its working map), then its abort: the committed rows
/// read back unchanged, and the next transaction's writes commit over them.
#[test]
fn a_dup_txn_aborted_leaves_the_committed_rows() {
    let (_dir, path) = temp("tndb_b_dup_abort");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    for k in 0..20u32 {
        for s in 0..if k == 5 { 400u32 } else { 2 } {
            let (key, value) = (dup_key(k, &s.to_be_bytes()), vec![k as u8; 20]);
            txn.put(&d, &key, &value).expect("put");
            model.insert(key, value);
        }
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let committed = model.clone();
    let mut txn = env.begin_rw().expect("rw");
    for s in 0..400u32 {
        txn.put(&d, &dup_key(5, &s.to_be_bytes()), &[0xaa; 30]).expect("replace");
    }
    for s in 0..2u32 {
        assert!(txn.delete(&d, &dup_key(9, &s.to_be_bytes())).expect("delete"));
    }
    txn.put(&d, &dup_key(100, b"x"), b"new key").expect("put");
    txn.put(&d, &dup_key(3, b"x"), b"new row").expect("put");
    assert_eq!(txn.get(&d, &dup_key(9, &0u32.to_be_bytes())).expect("get"), None);
    // A delete miss loads key 7's committed tree and moves no stamp; forgetting that unchanged
    // entry leaves its cell, which holds the committed root, to answer.
    let writes = txn.table_writes(&d);
    assert!(writes > 0, "the writes moved the stamp");
    assert!(!txn.delete(&d, &dup_key(7, b"zz")).expect("delete miss"));
    assert_eq!(txn.table_writes(&d), writes, "a miss moves no stamp");
    assert!(txn.forget_nested(&d, &7u32.to_be_bytes()), "the miss loaded an entry");
    let row = dup_key(7, &1u32.to_be_bytes());
    assert_eq!(txn.get(&d, &row).expect("get"), committed.get(&row).map(Vec::as_slice));
    txn.abort();
    let ro = env.begin_ro().expect("ro");
    let ro_get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after the abort", ro.dup_rows_for_test(&d), ro_get, &committed);
    drop(ro);
    let mut txn = env.begin_rw().expect("rw");
    let get = |txn: &crate::RwTxn, key: &[u8]| txn.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("a write txn after the abort", txn.dup_rows_for_test(&d), |k| get(&txn, k), &model);
    for s in 0..400u32 {
        let key = dup_key(5, &s.to_be_bytes());
        assert!(txn.delete(&d, &key).expect("delete"));
        model.remove(&key);
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let ro_get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("the next commit", ro.dup_rows_for_test(&d), ro_get, &model);
    assert!(env.failure().is_none(), "no latch");
}

/// The result of a write, without its value: what the dup table and its plain twin are compared
/// by.
type Outcome = std::result::Result<(), String>;

/// `result` as an [`Outcome`]: the error's `Debug` form.
fn outcome<T>(result: Result<T>) -> Outcome {
    result.map(drop).map_err(|e| format!("{e:?}"))
}

/// The key checks of a dup table's point ops, beside a plain twin `p` holding the same rows: a key
/// of at most [`OPTIONS`]' `key_len` bytes (no subkey) is `KeyTooShort` for either transaction's
/// get and for put, insert, append and delete, over an outer key with rows too; a key one byte
/// past [`Env::max_key_len`] is `KeyTooLarge` for each write, and its get and delete answer as
/// `p`'s do; a value at [`Env::max_value_len`] of its logical key (on either side of the overflow
/// limit) is stored, and one byte more is `ValueTooLarge`, as in `p`. Nothing refused latches or
/// changes a row. An outer key for `delete_prefix` of one byte less than `key_len` is
/// `KeyTooShort`, of one byte more `KeyTooLarge`.
#[test]
fn dup_keys_are_validated() {
    let (_dir, path) = temp("tndb_b_dup_keys");
    let env = open(&path, SyncMode::Durable);
    let max_key = env.max_key_len();
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let p = txn.create_table("p").expect("p");
    for k in 0..4u32 {
        let (key, value) = (dup_key(k, b"s"), vec![k as u8; 8]);
        txn.put(&d, &key, &value).expect("put");
        txn.put(&p, &key, &value).expect("put");
        model.insert(key, value);
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let mut txn = env.begin_rw().expect("rw");
    let outer = 1u32.to_be_bytes();
    for len in 0..=outer.len() {
        let key = &outer[..len];
        let refusals = [
            ("ro get", outcome(ro.get(&d, key))),
            ("rw get", outcome(txn.get(&d, key))),
            ("put", outcome(txn.put(&d, key, b"v"))),
            ("insert", outcome(txn.insert(&d, key, b"v"))),
            ("append", outcome(txn.append(&d, key, b"v"))),
            ("delete", outcome(txn.delete(&d, key))),
        ];
        let short = Err(format!("{:?}", Error::KeyTooShort { len, min: 5 }));
        for (what, result) in refusals {
            assert_eq!(result, short, "{what} of a {len}-byte key");
        }
    }
    let long = [&outer[..], &vec![0xab; max_key + 1 - outer.len()]].concat();
    for (what, result) in [
        ("put", outcome(txn.put(&d, &long, b"v"))),
        ("insert", outcome(txn.insert(&d, &long, b"v"))),
        ("append", outcome(txn.append(&d, &long, b"v"))),
    ] {
        assert_eq!(
            result,
            Err(format!("{:?}", Error::KeyTooLarge { len: max_key + 1, max: max_key })),
            "{what} of a long key"
        );
    }
    for (what, twins) in [
        ("put", [outcome(txn.put(&d, &long, b"v")), outcome(txn.put(&p, &long, b"v"))]),
        ("ro get", [outcome(ro.get(&d, &long)), outcome(ro.get(&p, &long))]),
        ("rw get", [outcome(txn.get(&d, &long)), outcome(txn.get(&p, &long))]),
        ("delete", [outcome(txn.delete(&d, &long)), outcome(txn.delete(&p, &long))]),
    ] {
        assert_eq!(twins[0], twins[1], "{what} of a long key, d and p");
    }
    // At the overflow limit (max_key - 10), past it, and at the widest key.
    for len in [max_key - 10, max_key - 9, max_key] {
        let writes = txn.table_writes(&d);
        let limit = env.max_value_len(len);
        assert!(limit > 1, "{len}");
        let key = [&2u32.to_be_bytes()[..], &vec![0xcd; len - 4]].concat();
        let past = vec![0xef; limit + 1];
        for table in [&d, &p] {
            let result = outcome(txn.put(table, &key, &past));
            assert_eq!(
                result,
                Err(format!("{:?}", Error::ValueTooLarge { len: limit + 1, max: limit })),
                "{len}: one past"
            );
        }
        assert_eq!(txn.table_writes(&d), writes, "{len}: a refused put moves no stamp");
        let value = vec![len as u8; limit];
        for table in [&d, &p] {
            txn.put(table, &key, &value).expect("a value at the limit");
            assert_eq!(txn.get(table, &key).expect("get"), Some(&value[..]), "{len}");
        }
        model.insert(key, value);
    }
    let writes = txn.table_writes(&d);
    let short = txn.delete_prefix(&d, &[0; 3]);
    assert!(matches!(short, Err(Error::KeyTooShort { len: 3, min: 4 })), "{short:?}");
    let long = txn.delete_prefix(&d, &dup_key(1, &[0]));
    assert!(matches!(long, Err(Error::KeyTooLarge { len: 5, max: 4 })), "{long:?}");
    assert_eq!(txn.table_writes(&d), writes, "a refused delete_prefix moves no stamp");
    assert_rows("rw", txn.dup_rows_for_test(&d), |k| outcome_get(&txn, &d, k), &model);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    drop(ro);
    assert!(env.failure().is_none(), "no latch");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("committed", ro.dup_rows_for_test(&d), get, &model);
    for (key, value) in &model {
        assert_eq!(ro.get(&p, key).expect("get"), Some(&value[..]), "the plain twin");
    }
}

/// `txn`'s get of `key` in `table`, owned.
fn outcome_get(txn: &crate::RwTxn, table: &crate::Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
    txn.get(table, key).map(|v| v.map(<[u8]>::to_vec))
}

/// Puts `rows` subkeys (big-endian `u32`s from 0) under outer key `k`, with values that differ by
/// `round`, into `txn` and `model`.
fn grow(txn: &mut crate::RwTxn, d: &crate::Table, model: &mut Rows, k: u32, rows: u32, round: u8) {
    for s in 0..rows {
        let (key, value) = (dup_key(k, &s.to_be_bytes()), vec![round ^ s as u8; 24]);
        txn.put(d, &key, &value).expect("put");
        model.insert(key, value);
    }
}

/// A committed key's nested tree copied and split to a new root page, and a key created in the
/// transaction split likewise, leave their outer cells naming the old roots until the commit's
/// flush: every get inside the transaction answers from the working roots. Dropping the committed
/// key's working entry (`forget_nested`, then an abort) shows its cell's committed rows; in the
/// next transaction the same writes read back before the commit, after it and after a reopen.
#[test]
fn reads_inside_the_txn_see_unflushed_roots() {
    let (_dir, path) = temp("tndb_b_dup_unflushed");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    for k in 0..8 {
        grow(&mut txn, &d, &mut model, k, 3, 0);
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let committed = model.clone();
    for round in [1u8, 2] {
        let mut txn = env.begin_rw().expect("rw");
        let mut grown = committed.clone();
        for k in [3, 100] {
            grow(&mut txn, &d, &mut grown, k, 300, round);
        }
        let stat = txn.table_stat(&d).expect("stat");
        assert_eq!(stat.branch_pages, 2, "both trees have a new root above their leaves");
        let get = |key: &[u8]| outcome_get(&txn, &d, key);
        assert_rows("before the commit", txn.dup_rows_for_test(&d), get, &grown);
        if round == 1 {
            // Key 3's cell still names its committed root: its committed rows, none of the new.
            assert!(txn.forget_nested(&d, &3u32.to_be_bytes()));
            for s in 0..300u32 {
                let key = dup_key(3, &s.to_be_bytes());
                let stale = txn.get(&d, &key).expect("get").map(<[u8]>::to_vec);
                assert_eq!(stale, committed.get(&key).cloned(), "the stale cell, subkey {s}");
            }
            txn.abort();
            continue;
        }
        txn.commit().expect("commit");
        check_dup(&env, "d");
        model = grown;
    }
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after the commit", ro.dup_rows_for_test(&d), get, &model);
    drop(ro);
    assert!(env.failure().is_none(), "no latch");
    drop(env);
    let env = open(&path, SyncMode::Durable);
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after a reopen", ro.dup_rows_for_test(&d), get, &model);
}

/// The commit's flush writes the working root of every changed key into its outer cell: 600 keys
/// committed over several outer leaves; then a transaction whose writes change only the nested
/// trees of keys in every leaf (new subkeys, replaced values, deletes that leave rows), adds keys
/// in the first leaves only (so some outer leaves are private when the flush comes and the rest
/// still published), and changes, empties and creates again both a committed key and a new one:
/// every row reads back after the commit and after a reopen, in either transaction.
#[test]
fn the_flush_writes_every_changed_root() {
    let (_dir, path) = temp("tndb_b_dup_flush");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    for k in (0..1200u32).step_by(2) {
        for s in 0..2u8 {
            let (key, value) = (dup_key(k, &[s]), vec![s; 8]);
            txn.put(&d, &key, &value).expect("put");
            model.insert(key, value);
        }
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let leaves = env.begin_ro().expect("ro").table_stat(&d).expect("stat").leaf_pages - 600;
    assert!(leaves >= 6, "several outer leaves: {leaves}");
    let mut txn = env.begin_rw().expect("rw");
    for k in (0..1200u32).step_by(2) {
        match k % 6 {
            0 => {
                let (key, value) = (dup_key(k, &[2]), vec![0x22; 12]);
                txn.put(&d, &key, &value).expect("a new subkey");
                model.insert(key, value);
            }
            2 => {
                let (key, value) = (dup_key(k, &[0]), vec![0x33; 16]);
                txn.put(&d, &key, &value).expect("a replaced value");
                model.insert(key, value);
            }
            _ => {
                assert!(txn.delete(&d, &dup_key(k, &[1])).expect("delete"), "{k}");
                model.remove(&dup_key(k, &[1]));
            }
        }
    }
    for k in (1..60u32).step_by(2) {
        let key = dup_key(k, b"new");
        txn.put(&d, &key, b"a new key").expect("put");
        model.insert(key, b"a new key".to_vec());
    }
    // Changed, emptied and created again: committed key 500 and new key 2001.
    for k in [500u32, 2001] {
        txn.put(&d, &dup_key(k, &[9]), b"changed").expect("put");
        for s in [0u8, 1, 2, 9] {
            txn.delete(&d, &dup_key(k, &[s])).expect("delete");
            model.remove(&dup_key(k, &[s]));
        }
        let get = txn.get(&d, &dup_key(k, &[9])).expect("get");
        assert_eq!(get, None, "{k} emptied");
        txn.put(&d, &dup_key(k, &[7]), b"again").expect("put");
        model.insert(dup_key(k, &[7]), b"again".to_vec());
    }
    let get = |key: &[u8]| outcome_get(&txn, &d, key);
    assert_rows("before the commit", txn.dup_rows_for_test(&d), get, &model);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after the commit", ro.dup_rows_for_test(&d), get, &model);
    assert_eq!(ro.table_stat(&d).expect("stat").nested_trees, 600 + 30 + 1);
    drop(ro);
    assert!(env.failure().is_none(), "no latch");
    drop(env);
    let env = open(&path, SyncMode::Durable);
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after a reopen", ro.dup_rows_for_test(&d), get, &model);
    let txn = env.begin_rw().expect("rw");
    let get = |key: &[u8]| outcome_get(&txn, &d, key);
    assert_rows("after a reopen, in a write txn", txn.dup_rows_for_test(&d), get, &model);
}

/// Each write that changes only a nested tree moves the table's write stamp (`table_writes`): a put
/// of a new subkey, a put replacing a value, an insert, an append and a delete that leaves rows;
/// so does a `delete_prefix` that removes a key's rows. A delete miss (of a subkey, of an outer
/// key), a `delete_prefix` of a key with no rows, a get, an insert of a present subkey and the
/// appends the order refuses do not, before the first write (when the stamp is 0) and after it;
/// the refused writes change no row.
#[test]
fn nested_writes_move_the_table_stamp() {
    let (_dir, path) = temp("tndb_b_dup_stamp");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    for k in 0..4u32 {
        for s in 0..3u8 {
            txn.put(&d, &dup_key(k, &[s]), &[s]).expect("put");
            model.insert(dup_key(k, &[s]), vec![s]);
        }
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let still = |txn: &mut crate::RwTxn, what: &str| {
        let writes = txn.table_writes(&d);
        assert!(!txn.delete(&d, &dup_key(1, &[9])).expect("delete"), "{what}: a subkey miss");
        assert!(!txn.delete(&d, &dup_key(50, &[0])).expect("delete"), "{what}: an outer miss");
        let none = txn.delete_prefix(&d, &50u32.to_be_bytes()).expect("delete_prefix");
        assert_eq!(none, 0, "{what}: a delete_prefix miss");
        assert_eq!(txn.get(&d, &dup_key(1, &[2])).expect("get"), Some(&[2][..]), "{what}");
        assert_eq!(txn.get(&d, &dup_key(1, &[8])).expect("get"), None, "{what}");
        let refused = [
            (outcome(txn.insert(&d, &dup_key(2, &[1]), b"x")), Error::KeyExists),
            (outcome(txn.append(&d, &dup_key(2, &[9]), b"x")), Error::AppendOutOfOrder),
            (outcome(txn.append(&d, &dup_key(3, &[0]), b"x")), Error::AppendOutOfOrder),
        ];
        for (result, error) in refused {
            assert_eq!(result, Err(format!("{error:?}")), "{what}");
        }
        assert_eq!(txn.table_writes(&d), writes, "{what}: nothing moved the stamp");
    };
    let mut txn = env.begin_rw().expect("rw");
    still(&mut txn, "before any write");
    assert_eq!(txn.table_writes(&d), 0, "no write yet");
    let mut last = 0;
    let writes = [
        "put of a new subkey",
        "put replacing a value",
        "insert",
        "append",
        "delete",
        "delete_prefix",
    ];
    for what in writes {
        match what {
            "put of a new subkey" => txn.put(&d, &dup_key(1, &[7]), b"v"),
            "put replacing a value" => txn.put(&d, &dup_key(1, &[0]), b"w"),
            "insert" => txn.insert(&d, &dup_key(2, &[7]), b"v"),
            "append" => txn.append(&d, &dup_key(3, &[7]), b"v"),
            "delete" => txn.delete(&d, &dup_key(0, &[1])).map(|found| assert!(found)),
            _ => txn.delete_prefix(&d, &0u32.to_be_bytes()).map(|rows| assert_eq!(rows, 2)),
        }
        .expect(what);
        let writes = txn.table_writes(&d);
        assert!(writes > last, "{what}: {writes} after {last}");
        last = writes;
        still(&mut txn, what);
    }
    model.insert(dup_key(1, &[7]), b"v".to_vec());
    model.insert(dup_key(1, &[0]), b"w".to_vec());
    model.insert(dup_key(2, &[7]), b"v".to_vec());
    model.insert(dup_key(3, &[7]), b"v".to_vec());
    model.remove(&dup_key(0, &[1]));
    remove_prefix(&mut model, 0);
    let get = |key: &[u8]| outcome_get(&txn, &d, key);
    assert_rows("the writes", txn.dup_rows_for_test(&d), get, &model);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("committed", ro.dup_rows_for_test(&d), get, &model);
}

/// An append to a dup table follows the plain table's rule over the logical key (outer key, then
/// subkey): into an empty table any key; an outer key past the last starts a tree; the last outer
/// key takes a subkey past its last one. A subkey not past the last outer key's last one, and an
/// outer key before the last (with rows or without), is `AppendOutOfOrder`, changing no row and
/// moving no stamp, in the transaction that wrote the last key and over a committed table. A plain
/// twin `p` given the same appends answers each the same way.
#[test]
fn dup_append_orders_by_the_logical_key() {
    let (_dir, path) = temp("tndb_b_dup_append");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let p = txn.create_table("p").expect("p");
    let steps: [&[(u32, &[u8], bool)]; 2] = [
        &[
            (50, b"m", true),
            (60, b"a", true),
            (60, b"b", true),
            (60, b"b", false),
            (60, b"a", false),
            (60, b"az", false),
            (50, b"z", false),
            (55, b"a", false),
            (60, b"b\0", true),
            (70, b"\0", true),
        ],
        &[
            (70, b"\0", false),
            (65, b"z", false),
            (60, b"zz", false),
            (70, b"\0\0", true),
            (70, b"\x01", true),
            (80, b"a", true),
        ],
    ];
    for (round, steps) in steps.into_iter().enumerate() {
        if round == 1 {
            txn.commit().expect("commit");
            check_dup(&env, "d");
            txn = env.begin_rw().expect("rw");
        }
        for &(k, s, ok) in steps {
            let (key, value) = (dup_key(k, s), [&[round as u8][..], s].concat());
            let writes = txn.table_writes(&d);
            let result = outcome(txn.append(&d, &key, &value));
            let twin = outcome(txn.append(&p, &key, &value));
            assert_eq!(result, twin, "{round} {k} {s:02x?}: d and p");
            if ok {
                assert_eq!(result, Ok(()), "{round} {k} {s:02x?}");
                model.insert(key, value);
            } else {
                let refused = Err(format!("{:?}", Error::AppendOutOfOrder));
                assert_eq!(result, refused, "{round} {k} {s:02x?}");
                assert_eq!(txn.table_writes(&d), writes, "{round} {k} {s:02x?}: no stamp");
            }
            let get = |key: &[u8]| outcome_get(&txn, &d, key);
            assert_rows("after the append", txn.dup_rows_for_test(&d), get, &model);
        }
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("committed", ro.dup_rows_for_test(&d), get, &model);
    assert!(env.failure().is_none(), "no latch");
}

/// In the private outer leaf holding the cells of outer keys `from` and `to`, overwrite `to`'s
/// cell with `from`'s bytes.
fn copy_cell(txn: &mut crate::RwTxn, from: &[u8], to: &[u8]) {
    use crate::tree::PageWrite as _;
    let writer = txn.writer_mut();
    for pgno in 0..writer.alloc.page_count() {
        if !writer.alloc.is_private(pgno) {
            continue;
        }
        let page = writer.page_mut(pgno).expect("a private page");
        if page::kind(page).ok() != Some(page::Kind::Leaf) {
            continue;
        }
        let cell = |key: &[u8]| {
            (0..page::nkeys(page)).find_map(|i| match page::leaf_entry(page, i) {
                Ok((k, page::Val::Nested(b))) if k == key => {
                    Some(b.as_ptr() as usize - page.as_ptr() as usize)
                }
                _ => None,
            })
        };
        if let (Some(src), Some(dst)) = (cell(from), cell(to)) {
            page.copy_within(src..src + page::NESTED_CELL, dst);
            return;
        }
    }
    panic!("no private outer leaf holds both cells");
}

/// An outer cell that names a page private to the transaction, with no working root for its key,
/// is `Corrupt` at its first use, and latches (a fresh environment per case): a key created in the
/// transaction and grown to a new root, its working entry dropped (`forget_nested`), so its cell
/// names its first root page, now a leaf of the tree, for a get, a put, a delete and a
/// `delete_prefix` (the key's load), and for a drop and a clear (the free walk, which takes the
/// cell's root untrusted); and a committed key's cell rewritten in the private outer leaf to name
/// another key's private root, for a get, a put and a drop.
#[test]
fn a_cell_naming_a_private_page_with_no_working_root_is_corrupt() {
    let load = "private to this transaction, with no working root";
    let walk = "is private to this transaction but named by a page it did not write";
    // Every case runs; the ones that answered otherwise are reported together.
    let mut wrong = Vec::new();
    let cases = [
        "get",
        "put",
        "delete",
        "delete_prefix",
        "drop",
        "clear",
        "rewritten get",
        "rewritten put",
        "rewritten drop",
    ];
    for case in cases {
        let (_dir, path) = temp("tndb_b_dup_private_cell");
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.create_dup_table("d", OPTIONS).expect("d");
        for k in 0..4u32 {
            txn.put(&d, &dup_key(k, b"s"), b"committed").expect("put");
        }
        txn.commit().expect("commit");
        check_dup(&env, "d");
        let mut txn = env.begin_rw().expect("rw");
        let first = dup_key(9, &0u32.to_be_bytes());
        let key = if case.starts_with("rewritten") {
            txn.put(&d, &dup_key(9, b"b"), b"new key").expect("put");
            copy_cell(&mut txn, &9u32.to_be_bytes(), &1u32.to_be_bytes());
            dup_key(1, b"s")
        } else {
            grow(&mut txn, &d, &mut Rows::new(), 9, 300, 0);
            assert_eq!(txn.table_stat(&d).expect("stat").branch_pages, 1, "a new root");
            assert!(txn.forget_nested(&d, &9u32.to_be_bytes()));
            first
        };
        let (result, message) = match case {
            "get" | "rewritten get" => (txn.get(&d, &key).map(drop), load),
            "put" | "rewritten put" => (txn.put(&d, &key, b"v"), load),
            "delete" => (txn.delete(&d, &key).map(drop), load),
            "delete_prefix" => (txn.delete_prefix(&d, &key[..4]).map(drop), load),
            "clear" => (txn.clear_table(&d), walk),
            _ => (txn.drop_table(d), walk),
        };
        let latched = env.failure().is_some();
        if !matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)) || !latched {
            wrong.push(format!("{case}: {result:?}, latched {latched}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The dup totals, so `table_stat`, follow every change. After each step of a script (the table
/// created; keys of one row; a key grown to a nested tree of two levels with values in overflow
/// runs; that key shrunk back to one row, its runs gone; a key created in the transaction emptied;
/// then, after a commit, the same over committed trees, a committed key emptied; `delete_prefix`
/// of a large tree created in the transaction, of a committed one and of a committed key of one
/// row; a clear over a committed large tree and one created in the transaction, then rows put
/// again) the walk of every tree agrees with the totals (`dup_rows_for_test`), and `table_stat`
/// gives the model's rows and trees and the pages the shape implies; after each commit
/// `Env::check` agrees with `table_stat`, and the committed figures read the same in a read-only
/// transaction and after a reopen.
#[test]
fn dup_totals_follow_every_change() {
    let (_dir, path) = temp("tndb_b_dup_totals");
    let env = open(&path, SyncMode::Durable);
    let run = overflow::run_pages(10_000, 4096) as u32;
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let empty = TableStat { dup: Some(OPTIONS), ..TableStat::default() };
    assert_eq!(txn.table_stat(&d).expect("stat"), empty, "created");
    // A step's walk and stat against the model and `shape`: the outer tree's height, the branch
    // pages, the least leaf pages and the overflow pages (with no branch page, one leaf per tree
    // and the outer leaf).
    let assert_step =
        |txn: &crate::RwTxn, model: &Rows, what: &str, shape: (u32, u32, u32, u32)| {
            let get = |key: &[u8]| outcome_get(txn, &d, key);
            assert_rows(what, txn.dup_rows_for_test(&d), get, model);
            let stat = txn.table_stat(&d).expect("stat");
            let trees =
                model.keys().map(|key| &key[..4]).collect::<std::collections::BTreeSet<_>>();
            let (height, branch, leaves, overflow) = shape;
            assert_eq!(stat.entries, model.len() as u64, "{what}: rows");
            assert_eq!(stat.nested_trees as usize, trees.len(), "{what}: trees");
            assert_eq!(stat.height, height, "{what}: the outer tree's height");
            assert_eq!(stat.branch_pages, branch, "{what}: branch pages");
            assert!(stat.leaf_pages >= leaves, "{what}: leaf pages {}", stat.leaf_pages);
            if branch == 0 {
                assert_eq!(stat.leaf_pages, 1 + trees.len() as u32, "{what}: one leaf per tree");
            }
            assert_eq!(stat.overflow_pages, overflow, "{what}: overflow pages");
            assert_eq!(stat.dup, Some(OPTIONS), "{what}");
            stat
        };
    for round in [1u8, 2] {
        let (one, hot, emptied) = if round == 1 { (0..3, 1, 2) } else { (10..13, 0, 1) };
        for k in one {
            txn.put(&d, &dup_key(k, b"one"), &[round]).expect("put");
            model.insert(dup_key(k, b"one"), vec![round]);
        }
        let trees = model.keys().map(|key| &key[..4]).collect::<std::collections::BTreeSet<_>>();
        let n = trees.len() as u32;
        assert_step(&txn, &model, "keys of one row", (1, 0, 1 + n, 0));
        grow(&mut txn, &d, &mut model, hot, 300, round);
        for s in [b"big0", b"big1"] {
            txn.put(&d, &dup_key(hot, s), &vec![round; 10_000]).expect("a value in a run");
            model.insert(dup_key(hot, s), vec![round; 10_000]);
        }
        assert_step(&txn, &model, "grown", (1, 1, 1 + n + 2, 2 * run));
        let rows: Vec<_> =
            model.range(dup_key(hot, &[])..dup_key(hot + 1, &[])).map(|(k, _)| k.clone()).collect();
        for key in &rows[1..] {
            assert!(txn.delete(&d, key).expect("delete"));
            model.remove(key);
        }
        assert_step(&txn, &model, "shrunk", (1, 0, 1 + n, 0));
        let rows: Vec<_> = model
            .range(dup_key(emptied, &[])..dup_key(emptied + 1, &[]))
            .map(|(k, _)| k.clone())
            .collect();
        for key in &rows {
            assert!(txn.delete(&d, key).expect("delete"));
            model.remove(key);
        }
        let stat = assert_step(&txn, &model, "a key emptied", (1, 0, n, 0));
        txn.commit().expect("commit");
        check_dup(&env, "d");
        let ro = env.begin_ro().expect("ro");
        assert_eq!(ro.table_stat(&d).expect("stat"), stat, "{round}: committed");
        drop(ro);
        txn = env.begin_rw().expect("rw");
        assert_eq!(txn.table_stat(&d).expect("stat"), stat, "{round}: the next txn");
    }
    // `delete_prefix` of a tree created in the transaction (two levels, a run), of a committed
    // one like it and of a committed key of one row.
    grow_large(&mut txn, &d, &mut model, 20, 3);
    let n = outer_keys(&model);
    assert_step(&txn, &model, "a new large key", (1, 1, 1 + n + 1, run));
    let remove = |txn: &mut crate::RwTxn, model: &mut Rows, k: u32, what: &str| {
        let rows = txn.delete_prefix(&d, &k.to_be_bytes()).expect("delete_prefix");
        assert_eq!(rows, remove_prefix(model, k), "{what}: the rows removed");
        assert_step(txn, model, what, (1, 0, 1 + outer_keys(model), 0))
    };
    remove(&mut txn, &mut model, 20, "a new key's tree removed");
    grow_large(&mut txn, &d, &mut model, 21, 3);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    txn = env.begin_rw().expect("rw");
    remove(&mut txn, &mut model, 21, "a committed key's tree removed");
    let stat = remove(&mut txn, &mut model, 10, "a committed key of one row removed");
    grow_large(&mut txn, &d, &mut model, 22, 4);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let committed_stat = ro.table_stat(&d).expect("stat");
    assert_eq!(committed_stat.entries, stat.entries + 301, "a large key's rows on top");
    drop(ro);
    // `clear_table` over a committed tree of two levels with a run and one created in the
    // transaction, then rows put again.
    txn = env.begin_rw().expect("rw");
    grow_large(&mut txn, &d, &mut model, 23, 4);
    let n = outer_keys(&model);
    assert_step(&txn, &model, "two large keys", (1, 2, 1 + n + 2, 2 * run));
    txn.clear_table(&d).expect("clear");
    model.clear();
    assert_eq!(txn.table_stat(&d).expect("stat"), empty, "cleared");
    assert_rows("cleared", txn.dup_rows_for_test(&d), |key| outcome_get(&txn, &d, key), &model);
    for k in [10, 22, 30] {
        txn.put(&d, &dup_key(k, b"again"), &[5]).expect("put after the clear");
        model.insert(dup_key(k, b"again"), vec![5]);
    }
    let stat = assert_step(&txn, &model, "rows after the clear", (1, 0, 1 + 3, 0));
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    assert_eq!(ro.table_stat(&d).expect("stat"), stat, "cleared: committed");
    drop(ro);
    let committed = Some(stat);
    drop(env);
    let env = open(&path, SyncMode::Durable);
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let d = ro.open_table("d").expect("open").expect("d");
    assert_eq!(Some(ro.table_stat(&d).expect("stat")), committed, "after a reopen");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("after a reopen", ro.dup_rows_for_test(&d), get, &model);
}

/// The outer key of [`damaged`]'s large nested tree, of two levels with a value in a run.
pub(super) const HOT: u32 = 7;

/// An outer key of [`damaged`]'s with a nested tree of one leaf.
pub(super) const COLD: u32 = 8;

/// A cleanly closed file `dir/db` holding dup table `d` (of [`OPTIONS`]): outer keys 0 to 39 with
/// 1 to 3 rows of 1-byte subkeys each, but [`HOT`] with 300 rows and a value in a run; then
/// `forge` damaged it, given the path and the committed outer cells (each key's outer leaf and
/// the nested root its cell names). Returns the path and the rows.
pub(super) fn damaged(
    dir: &Path,
    forge: impl FnOnce(&Path, &BTreeMap<u32, (u32, TreeRoot)>),
) -> (PathBuf, Rows) {
    let path = dir.join("db");
    let mut model = Rows::new();
    let cells = {
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.create_dup_table("d", OPTIONS).expect("d");
        for k in 0..40u32 {
            if k == HOT {
                grow(&mut txn, &d, &mut model, k, 300, 0);
                let big = (dup_key(k, b"big"), vec![0xb1; 10_000]);
                txn.put(&d, &big.0, &big.1).expect("a value in a run");
                model.insert(big.0, big.1);
                continue;
            }
            for s in 0..1 + k % 3 {
                let (key, value) = (dup_key(k, &[s as u8]), vec![k as u8; 8]);
                txn.put(&d, &key, &value).expect("put");
                model.insert(key, value);
            }
        }
        txn.commit().expect("commit");
        check_dup(&env, "d");
        let snapshot = env.inner().current();
        let outer = tree(&env, "d");
        let cell = |k: u32| {
            let leaf = descend(&*snapshot, &outer, &k.to_be_bytes(), |_, _, _, _| Ok(()), None)
                .expect("descend")
                .expect("an outer leaf")
                .pgno;
            let page = snapshot.page(leaf).expect("the outer leaf");
            let found = page::leaf_find(page, &k.to_be_bytes()).expect("find");
            let Find::Found { value: Val::Nested(root), .. } = found else {
                panic!("key {k}'s nested cell");
            };
            (leaf, TreeRoot::decode(root).expect("a nested root"))
        };
        (0..40u32).map(|k| (k, cell(k))).collect::<BTreeMap<_, _>>()
    };
    let (hot, cold) = (cells[&HOT].1, cells[&COLD].1);
    assert!(hot.height == 2 && hot.overflow_pages > 0 && cold.height == 1, "{hot:?} {cold:?}");
    forge(&path, &cells);
    (path, model)
}

/// Replace the value of `key`'s cell in the leaf `pgno` of the file at `path` with `value`, the
/// leaf's CRC stamped again.
pub(super) fn restamp_cell(path: &Path, pgno: u32, key: &[u8], value: impl page::CellVal) {
    restamped(path, pgno, |page| {
        let Find::Found { index, .. } = page::leaf_find(page, key).expect("find") else {
            panic!("{key:02x?} is in leaf {pgno}");
        };
        let fit = page::LeafMut::new(page).expect("a leaf").replace_value(index, value);
        assert_eq!(fit.expect("replaced"), page::Fit::Done);
    });
}

/// `Env::check` of a fresh copy of the file at `path`: `Corrupt` with a message holding each of
/// `parts`, and the environment latched.
#[track_caller]
fn assert_check_refuses(what: &str, path: &Path, parts: &[&str]) {
    let (_dir, env) = copy_of(path);
    let result = env.check();
    assert!(
        matches!(&result, Err(Error::Corrupt(m)) if parts.iter().all(|p| m.contains(p))),
        "{what}: {result:?}"
    );
    assert!(env.failure().is_some(), "{what}: latched");
}

/// Every row of `model` reads back from a fresh copy of the file at `path`, with no latch.
#[track_caller]
fn assert_reads(what: &str, path: &Path, model: &Rows) {
    let (_dir, env) = copy_of(path);
    let ro = env.begin_ro().expect("ro");
    let d = ro.open_table("d").expect("open").expect("d");
    for (key, value) in model {
        assert_eq!(ro.get(&d, key).expect("get"), Some(&value[..]), "{what}: {key:02x?}");
    }
    assert!(env.failure().is_none(), "{what}: no latch");
}

/// A dup record whose nested totals are one off what its trees hold, keeping every record rule
/// (one row too many, then one nested leaf page too many), fails `Env::check`, which names the
/// field, the trees' figure and the record's, and latches; every row still reads.
#[test]
fn check_refuses_dup_totals_off_by_one() {
    // The field one past the trees' figure, returning that figure.
    let edit = |field: &str, dup: &mut DupRecord| match field {
        "nested_entries" => {
            dup.nested_entries += 1;
            dup.nested_entries - 1
        }
        _ => {
            dup.nested_leaf_pages += 1;
            u64::from(dup.nested_leaf_pages - 1)
        }
    };
    for field in ["nested_entries", "nested_leaf_pages"] {
        let dir = TempDir::with_prefix("tndb_b_dup_totals_off").expect("temp dir");
        let mut was = 0;
        let (path, model) = damaged(dir.path(), |path, _| {
            restamp_record(path, |record| was = edit(field, record.dup.as_mut().expect("dup")));
        });
        assert_reads(field, &path, &model);
        let message = format!("{field} {was} in its trees, not the {} its record counts", was + 1);
        assert_check_refuses(field, &path, &["check: table \"d\": ", &message]);
    }
}

/// A dup record whose outer key width is 3 over a tree of 4-byte outer keys fails `Env::check` at
/// the first outer key, and latches.
#[test]
fn check_refuses_an_outer_key_of_another_width() {
    let dir = TempDir::with_prefix("tndb_b_dup_width").expect("temp dir");
    let (path, _) = damaged(dir.path(), |path, _| {
        restamp_record(path, |record| record.dup.as_mut().expect("dup").key_len = 3);
    });
    assert_check_refuses("width 3", &path, &["outer key of 4 bytes in a table of width 3"]);
}

/// A dup record whose outer key width is 3 over a tree of 4-byte outer keys is `Corrupt` at first
/// use, before `Env::check`, and latches, each op on a fresh copy: a get in either transaction, a
/// put of a new key, a delete and a `delete_prefix` (a 3-byte key is never found, and a miss
/// beside a 4-byte key is refused, naming the leaf), and an append in order by the 3-byte key
/// (the last outer key has 4 bytes), none of which may answer `None` or write a 3-byte cell.
#[test]
fn a_dup_record_of_another_width_is_corrupt_at_first_use() {
    let dir = TempDir::with_prefix("tndb_b_dup_width_use").expect("temp dir");
    let mut first = 0;
    let (path, _) = damaged(dir.path(), |path, cells| {
        first = cells[&0].0;
        restamp_record(path, |record| record.dup.as_mut().expect("dup").key_len = 3);
    });
    let miss = format!("page {first}: an outer key of 4 bytes in a dup table of width 3");
    let last = "its last outer key has 4 bytes, not the table's width 3".to_string();
    let key = dup_key(COLD, &[0]);
    for op in ["ro get", "rw get", "put", "append", "delete", "delete_prefix"] {
        let (_dir, env) = copy_of(&path);
        let result = if op == "ro get" {
            let ro = env.begin_ro().expect("ro");
            let d = ro.open_table("d").expect("open").expect("d");
            ro.get(&d, &key).map(drop)
        } else {
            let mut txn = env.begin_rw().expect("rw");
            let d = txn.open_table("d").expect("open").expect("d");
            match op {
                "rw get" => txn.get(&d, &key).map(drop),
                "put" => txn.put(&d, &dup_key(40, b"new"), b"value"),
                "append" => txn.append(&d, &dup_key(u32::MAX, b"new"), b"value"),
                "delete" => txn.delete(&d, &key).map(drop),
                _ => txn.delete_prefix(&d, &key[..3]).map(drop),
            }
        };
        let message = if op == "append" { &last } else { &miss };
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains(message.as_str())),
            "{op}: {result:?}"
        );
        assert!(env.failure().is_some(), "{op}: latched");
    }
}

/// An outer cell that is not a nested cell fails `Env::check`, and latches: an inline value (every
/// outer cell of a table that never packs is nested) and an overflow reference (no outer tree
/// holds a run).
#[test]
fn check_refuses_an_outer_cell_that_is_not_nested() {
    let ovf = overflow::Ovf { pgno: 3, txn: 1, len: 10_000 };
    for (case, message) in [
        ("inline", "a cell that is not nested in a dup table's outer tree"),
        ("overflow", "holds an overflow cell in a dup table's outer tree (a reference to run 3)"),
    ] {
        let dir = TempDir::with_prefix("tndb_b_dup_outer_cell").expect("temp dir");
        let (path, _) = damaged(dir.path(), |path, cells| {
            let (leaf, key) = (cells[&COLD].0, COLD.to_be_bytes());
            match case {
                "inline" => restamp_cell(path, leaf, &key, &b"an inline value"[..]),
                _ => restamp_cell(path, leaf, &key, Val::Overflow(ovf)),
            }
        });
        assert_check_refuses(case, &path, &["check: table \"d\": page ", message]);
    }
}

/// Key [`COLD`]'s outer cell made to name key [`HOT`]'s nested root, the record's totals adjusted
/// to the trees the cells now name: `Env::check` reports the shared root used twice, both times by
/// the table, and [`COLD`]'s own tree leaked (the walk descends no page claimed already), and
/// latches.
#[test]
fn a_shared_nested_root_is_a_double_claim() {
    let dir = TempDir::with_prefix("tndb_b_dup_shared_root").expect("temp dir");
    let mut roots = (TreeRoot::EMPTY, TreeRoot::EMPTY);
    let (path, _) = damaged(dir.path(), |path, cells| {
        let ((leaf, cold), (_, hot)) = (cells[&COLD], cells[&HOT]);
        roots = (hot, cold);
        restamp_cell(path, leaf, &COLD.to_be_bytes(), Val::Nested(&hot.encode()));
        restamp_record(path, |record| {
            let dup = record.dup.as_mut().expect("dup");
            dup.nested_entries = dup.nested_entries - cold.entries + hot.entries;
            dup.nested_branch_pages =
                dup.nested_branch_pages - cold.branch_pages + hot.branch_pages;
            dup.nested_leaf_pages = dup.nested_leaf_pages - cold.leaf_pages + hot.leaf_pages;
            dup.nested_overflow_pages += hot.overflow_pages - cold.overflow_pages;
        });
    });
    let (hot, cold) = roots;
    let twice =
        format!("leaked [{}]; used twice [{} (table \"d\" and table \"d\")]", cold.root, hot.root);
    assert_check_refuses("a shared root", &path, &["check: page accounting of txn ", &twice]);
    // The drop's free walk meets the root a second time at COLD's cell (keys in order) and
    // refuses it before freeing a page: nothing in the transaction's freed list or pool moved.
    let (_copy_dir, copy) = temp("tndb_b_dup_case");
    fs::copy(&path, &copy).expect("copy");
    {
        let env = open(&copy, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.create_dup_table("d", OPTIONS).expect("d");
        let pages = |txn: &mut crate::RwTxn| {
            let alloc = &txn.writer_mut().alloc;
            (alloc.freed().to_vec(), alloc.pool().to_vec())
        };
        let before = pages(&mut txn);
        let result = txn.drop_table(d);
        let reached = format!(
            "the nested root {} of key {:02x?} is reached twice",
            hot.root,
            COLD.to_be_bytes()
        );
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains(&reached)),
            "drop: {result:?}"
        );
        assert!(env.failure().is_some(), "drop: latched");
        assert_eq!(pages(&mut txn), before, "drop: nothing freed");
    }
    assert_check_refuses("after the refused drop", &copy, &[&twice]);
}

/// A nested cell forged into a nested tree's leaf (the leaf's CRC stamped again) is `Corrupt` at
/// a get of its row and in `Env::check`, which names the outer key, and each latches.
#[test]
fn a_nested_cell_inside_a_nested_tree_is_corrupt() {
    const NESTED: &str = "a nested cell in a tree that holds none";
    let dir = TempDir::with_prefix("tndb_b_dup_nested_in_nested").expect("temp dir");
    let forged =
        TreeRoot { root: 9, root_txn: 7, height: 1, entries: 3, leaf_pages: 1, ..TreeRoot::EMPTY };
    let mut leaf = 0;
    let (path, model) = damaged(dir.path(), |path, cells| {
        leaf = cells[&COLD].1.root;
        restamp_cell(path, leaf, &[1], Val::Nested(&forged.encode()));
    });
    {
        let (_dir, env) = copy_of(&path);
        let ro = env.begin_ro().expect("ro");
        let d = ro.open_table("d").expect("open").expect("d");
        let row = dup_key(COLD, &[0]);
        assert_eq!(ro.get(&d, &row).expect("its neighbour"), model.get(&row).map(Vec::as_slice));
        let result = ro.get(&d, &dup_key(COLD, &[1]));
        assert!(matches!(&result, Err(Error::Corrupt(m)) if m.contains(NESTED)), "{result:?}");
        assert!(env.failure().is_some(), "get: latched");
    }
    let inner = format!("key {COLD:08x}: check: table \"d\": page {leaf} ");
    assert_check_refuses("check", &path, &["check: table \"d\": page ", &inner, NESTED]);
}

/// The private leaf of `txn` holding exactly one entry, of key `key`: the nested tree a put of a
/// new outer key with subkey `key` created.
fn private_leaf_holding(txn: &mut crate::RwTxn, key: &[u8]) -> u32 {
    use crate::tree::PageWrite as _;
    let writer = txn.writer_mut();
    let holds = |page: &[u8]| {
        page::kind(page).ok() == Some(page::Kind::Leaf)
            && page::nkeys(page) == 1
            && page::leaf_entry(page, 0).is_ok_and(|(k, _)| k == key)
    };
    let private: Vec<u32> =
        (0..writer.alloc.page_count()).filter(|&pgno| writer.alloc.is_private(pgno)).collect();
    private
        .into_iter()
        .find(|&pgno| holds(writer.page_mut(pgno).expect("a private page")))
        .expect("a private leaf holding the key")
}

/// A committed outer leaf whose cell (key [`COLD`]'s) names a nested root by the id of the
/// transaction that writes next (the cell's `root_txn`, the leaf's CRC stamped again) is refused
/// when that transaction's write copies the leaf, by the copy-on-write rule for a cell naming
/// this transaction: whether the page it names is published (key [`HOT`]'s root, of an older
/// txn) or private to the transaction (the tree its put of a new outer key just created, so the
/// copy would keep a cell naming another key's tree). Each write that copies the leaf meets it: a
/// put of a new outer key, a delete emptying another key, a `delete_prefix` of another key, and a
/// nested-only put to another key, whose cell the commit's flush rewrites. Each is `Corrupt` and
/// latches; on the file before the forge the same writes commit and pass `Env::check`.
#[test]
fn a_published_outer_cell_naming_this_txn_is_corrupt_at_copy() {
    let dir = TempDir::with_prefix("tndb_b_dup_outer_this_txn").expect("temp dir");
    let mut cells = BTreeMap::new();
    let (clean, _) = damaged(dir.path(), |_, committed| cells = committed.clone());
    let (leaf, cold) = cells[&COLD];
    assert!(cells.values().all(|&(l, _)| l == leaf), "the outer tree is one leaf");
    let new_key = dup_key(1000, b"new");
    // The next transaction's id, and the page its put of `new_key` takes for the key's tree.
    let (next, new_tree) = {
        let (_dir, env) = copy_of(&clean);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.open_table("d").expect("open").expect("d");
        txn.put(&d, &new_key, b"v").expect("put");
        (txn.txn_id(), private_leaf_holding(&mut txn, b"new"))
    };
    let writes = ["put of a new outer key", "emptying delete", "delete_prefix", "nested put"];
    let write = |txn: &mut crate::RwTxn, d: &crate::Table, what: &str| match what {
        "put of a new outer key" => txn.put(d, &new_key, b"v"),
        "emptying delete" => txn.delete(d, &dup_key(3, &[0])).map(|found| assert!(found)),
        "delete_prefix" => txn.delete_prefix(d, &4u32.to_be_bytes()).map(|n| assert_eq!(n, 2)),
        _ => txn.put(d, &dup_key(5, &[9]), b"v"),
    };
    for what in writes {
        let (_dir, env) = copy_of(&clean);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.open_table("d").expect("open").expect("d");
        write(&mut txn, &d, what).expect(what);
        txn.commit().expect("commit");
        check_dup(&env, "d");
    }
    let mut wrong = Vec::new();
    for (case, named) in [("published", cells[&HOT].1.root), ("private", new_tree)] {
        let (_case_dir, path) = temp("tndb_b_dup_outer_this_txn_case");
        fs::copy(&clean, &path).expect("copy");
        let forged = TreeRoot { root: named, root_txn: next as u32, ..cold }.encode();
        restamp_cell(&path, leaf, &COLD.to_be_bytes(), Val::Nested(&forged));
        let message = format!(
            "published page {leaf} names nested root page {named} with this txn's id {}",
            next as u32
        );
        // The private case names the page only the put of a new outer key has allocated.
        let writes = if case == "private" { &writes[..1] } else { &writes[..] };
        for &what in writes {
            let (_dir, env) = copy_of(&path);
            let mut txn = env.begin_rw().expect("rw");
            assert_eq!(txn.txn_id(), next, "{case} {what}: the id the cell names");
            let d = txn.open_table("d").expect("open").expect("d");
            let mut result = write(&mut txn, &d, what);
            if what == "nested put" {
                assert!(result.is_ok() && env.failure().is_none(), "{case} {what}: no copy yet");
                result = txn.commit().map(drop);
            } else {
                drop(txn);
            }
            let latched = env.failure().is_some();
            if !matches!(&result, Err(Error::Corrupt(m)) if m.contains(&message)) || !latched {
                wrong.push(format!("{case} {what}: {result:?}, latched {latched}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Puts 300 subkeys under outer key `k` ([`grow`]) and a value in a run: a nested tree of two
/// levels at 4 KiB pages that counts overflow pages.
fn grow_large(txn: &mut crate::RwTxn, d: &crate::Table, model: &mut Rows, k: u32, round: u8) {
    grow(txn, d, model, k, 300, round);
    let big = (dup_key(k, b"big"), vec![round; 10_000]);
    txn.put(d, &big.0, &big.1).expect("a value in a run");
    model.insert(big.0, big.1);
}

/// Removes outer key `k`'s rows from `model`, returning how many there were.
fn remove_prefix(model: &mut Rows, k: u32) -> u64 {
    let before = model.len();
    model.retain(|key, _| key[..4] != k.to_be_bytes());
    (before - model.len()) as u64
}

/// The outer keys `model` holds rows under.
fn outer_keys(model: &Rows) -> u32 {
    let keys: std::collections::BTreeSet<_> = model.keys().map(|key| &key[..4]).collect();
    keys.len() as u32
}

/// `txn`'s rows of dup table `d` (by a walk of every tree, with the totals, and by `get`) and
/// its `table_stat` rows and trees against `model`.
#[track_caller]
fn assert_txn_rows(what: &str, txn: &crate::RwTxn, d: &crate::Table, model: &Rows) {
    assert_rows(what, txn.dup_rows_for_test(d), |key| outcome_get(txn, d, key), model);
    let stat = txn.table_stat(d).expect("stat");
    assert_eq!(
        (stat.entries, stat.nested_trees),
        (model.len() as u64, outer_keys(model)),
        "{what}"
    );
}

/// `delete_prefix` removes every row of an outer key and returns how many: of a committed key
/// with a nested tree of two levels and a run, of a committed key written in the same
/// transaction, and of a key created in it (a tree of private pages); 0 and no stamp for a key
/// with no rows, a stamp for each hit. The rows are gone (by `get`, by a walk of every tree with
/// the totals, by `table_stat`), the keys take rows again in the same transaction, and the result
/// commits, reopens and passes `Env::check`. Then 50 cycles of a large key's create and
/// `delete_prefix` with no reader leave no page behind (`Env::check` after each) and keep the
/// file's page count flat.
#[test]
fn delete_prefix_frees_the_tree_and_its_cell() {
    let (_dir, path) = temp("tndb_b_dup_delete_prefix");
    let env = open(&path, SyncMode::Durable);
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    grow_large(&mut txn, &d, &mut model, 1, 0);
    for k in 2..6 {
        grow(&mut txn, &d, &mut model, k, 3, 0);
    }
    txn.commit().expect("commit");
    check_dup(&env, "d");

    let mut txn = env.begin_rw().expect("rw");
    assert_eq!(txn.delete_prefix(&d, &9u32.to_be_bytes()).expect("a miss"), 0, "no such key");
    assert_eq!(txn.table_writes(&d), 0, "a miss moves no stamp");
    grow(&mut txn, &d, &mut model, 3, 5, 1);
    grow_large(&mut txn, &d, &mut model, 7, 1);
    for k in [1, 3, 7] {
        let writes = txn.table_writes(&d);
        let removed = remove_prefix(&mut model, k);
        let got = txn.delete_prefix(&d, &k.to_be_bytes()).expect("delete_prefix");
        assert_eq!(got, removed, "key {k}: the rows removed");
        assert!(txn.table_writes(&d) > writes, "key {k}: the stamp moved");
        assert_eq!(txn.delete_prefix(&d, &k.to_be_bytes()).expect("again"), 0, "key {k}: gone");
        assert_txn_rows(&format!("key {k} removed"), &txn, &d, &model);
    }
    grow(&mut txn, &d, &mut model, 1, 2, 2);
    grow(&mut txn, &d, &mut model, 7, 2, 2);
    assert_txn_rows("keys 1 and 7 again", &txn, &d, &model);
    txn.commit().expect("commit");
    check_dup(&env, "d");
    let ro = env.begin_ro().expect("ro");
    let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
    assert_rows("committed", ro.dup_rows_for_test(&d), get, &model);
    drop(ro);
    drop(env);

    let env = open(&path, SyncMode::Durable);
    check_dup(&env, "d");
    let mut counts = Vec::new();
    for round in 0..50u8 {
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.create_dup_table("d", OPTIONS).expect("d");
        let mut rows = model.clone();
        grow_large(&mut txn, &d, &mut rows, 10, round);
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("rw");
        let removed = txn.delete_prefix(&d, &10u32.to_be_bytes()).expect("delete_prefix");
        assert_eq!(removed, remove_prefix(&mut rows, 10), "round {round}");
        assert_txn_rows(&format!("round {round}"), &txn, &d, &model);
        txn.commit().expect("commit");
        counts.push(check_dup(&env, "d").page_count);
    }
    let (early, late) = counts.split_at(25);
    assert!(late.iter().max() <= early.iter().max(), "the file grew: {counts:?}");
}

/// Dup table `d`'s rows for [`dropping_or_clearing_a_dup_table_frees_every_page`]: outer keys 0
/// to 149 with 1 to 3 rows each (an outer tree of two levels at 4 KiB pages), keys 1 and 2 large
/// ([`grow_large`]).
fn fill(txn: &mut crate::RwTxn, d: &crate::Table, model: &mut Rows, round: u8) {
    for k in 0..150u32 {
        match k {
            1 | 2 => grow_large(txn, d, model, k, round),
            _ => grow(txn, d, model, k, 1 + k % 3, round),
        }
    }
}

/// A dup table with committed nested trees (of two levels, with runs) and, in the transaction
/// that drops or clears it, a tree created there (private pages), a committed tree changed there
/// and one only loaded (a refused insert): after the commit `Env::check` finds no page left behind
/// and none claimed twice, a dropped table is gone and a cleared one empty (no rows, no pages, no
/// trees; rows put after the clear in the same transaction are its only rows). A reader held
/// across the drop or clear keeps reading the committed rows through the next build's commit;
/// once it is gone, each rebuild reuses the freed pages (the file's page count stays flat).
#[test]
fn dropping_or_clearing_a_dup_table_frees_every_page() {
    for dropping in [true, false] {
        let what = if dropping { "drop" } else { "clear" };
        let (_dir, path) = temp("tndb_b_dup_drop_clear");
        let env = open(&path, SyncMode::Durable);
        let mut counts = Vec::new();
        let mut reader = None;
        let mut model = Rows::new();
        for round in 0..4u8 {
            let mut txn = env.begin_rw().expect("rw");
            let d = txn.create_dup_table("d", OPTIONS).expect("d");
            fill(&mut txn, &d, &mut model, round);
            txn.commit().expect("commit");
            let report = check_dup(&env, "d");
            assert!(report.tables["d"].height >= 2, "{what}: an outer tree of two levels");
            counts.push(report.page_count);
            if let Some((ro, committed)) = reader.take() {
                let ro: crate::RoTxn = ro;
                let d = ro.open_table("d").expect("open").expect("d");
                let rows = ro.dup_rows_for_test(&d);
                let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
                assert_rows(&format!("{what}: the held reader"), rows, get, &committed);
            }
            if round == 0 {
                reader = Some((env.begin_ro().expect("ro"), model.clone()));
            }
            let mut txn = env.begin_rw().expect("rw");
            let d = txn.create_dup_table("d", OPTIONS).expect("d");
            grow_large(&mut txn, &d, &mut model, 500, round);
            grow(&mut txn, &d, &mut model, 1, 5, round ^ 0x55);
            let loaded = txn.insert(&d, &dup_key(3, &0u32.to_be_bytes()), b"x");
            assert!(matches!(loaded, Err(Error::KeyExists)), "{what}: {loaded:?}");
            model.clear();
            if dropping {
                txn.drop_table(d).expect("drop");
            } else {
                txn.clear_table(&d).expect("clear");
                let stat = txn.table_stat(&d).expect("stat");
                assert_eq!(
                    stat,
                    TableStat { dup: Some(OPTIONS), ..TableStat::default() },
                    "{what}"
                );
                for k in [1, 3, 500] {
                    let (key, value) = (dup_key(k, b"again"), vec![round; 8]);
                    txn.put(&d, &key, &value).expect("put after the clear");
                    model.insert(key, value);
                }
                assert_txn_rows(&format!("{what}: after the clear"), &txn, &d, &model);
            }
            txn.commit().expect("commit");
            let report = check(&env);
            if dropping {
                assert!(!report.tables.contains_key("d"), "{what}: the table is gone");
                assert_eq!(report.table_pages, 0, "{what}: no table page left");
            } else {
                check_dup(&env, "d");
                let ro = env.begin_ro().expect("ro");
                let d = ro.open_table("d").expect("open").expect("d");
                let get = |key: &[u8]| ro.get(&d, key).map(|v| v.map(<[u8]>::to_vec));
                assert_rows(&format!("{what}: cleared"), ro.dup_rows_for_test(&d), get, &model);
            }
        }
        assert!(counts[2] <= counts[1] && counts[3] <= counts[1], "{what}: {counts:?}");
    }
}

/// A dup record whose nested totals are one off what its trees hold (keeping every record rule)
/// fails the free walk of a drop and of a clear, which name the field, before anything is
/// freed, and latch.
#[test]
fn a_free_walk_refuses_dup_totals_that_miss_its_trees() {
    let dir = TempDir::with_prefix("tndb_b_dup_free_totals").expect("temp dir");
    let (path, _) = damaged(dir.path(), |path, _| {
        restamp_record(path, |record| record.dup.as_mut().expect("dup").nested_entries += 1);
    });
    for dropping in [true, false] {
        let (_dir, env) = copy_of(&path);
        let mut txn = env.begin_rw().expect("rw");
        let d = txn.create_dup_table("d", OPTIONS).expect("d");
        let result = if dropping { txn.drop_table(d) } else { txn.clear_table(&d) };
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains("nested entries")),
            "drop {dropping}: {result:?}"
        );
        assert!(env.failure().is_some(), "drop {dropping}: latched");
        let alloc = &txn.writer_mut().alloc;
        assert!(alloc.freed().is_empty(), "drop {dropping}: nothing freed");
    }
}

/// The free walk of a drop and of a clear refuses an outer tree that `Env::check` refuses, before
/// anything is freed, and latches: a record of outer key width 3 over 4-byte outer keys, and a
/// key's ([`COLD`]'s) outer cell made an inline value or an overflow reference; each error names
/// the outer leaf.
#[test]
fn a_free_walk_refuses_a_damaged_outer_tree() {
    let ovf = overflow::Ovf { pgno: 3, txn: 1, len: 10_000 };
    for (case, message) in [
        ("width", "an outer key of 4 bytes in a dup table of width 3"),
        ("inline", "a cell that is not nested in a dup table's outer tree"),
        ("overflow", "an overflow cell in a dup table's outer tree (a reference to run 3)"),
    ] {
        let dir = TempDir::with_prefix("tndb_b_dup_free_outer").expect("temp dir");
        let mut leaf = 0;
        let (path, _) = damaged(dir.path(), |path, cells| {
            leaf = cells[&COLD].0;
            let key = COLD.to_be_bytes();
            match case {
                "width" => restamp_record(path, |r| r.dup.as_mut().expect("dup").key_len = 3),
                "inline" => restamp_cell(path, leaf, &key, &b"an inline value"[..]),
                _ => restamp_cell(path, leaf, &key, Val::Overflow(ovf)),
            }
        });
        let message = format!("page {leaf}: {message}");
        for dropping in [true, false] {
            let what = format!("{case} {}", if dropping { "drop" } else { "clear" });
            let (_dir, env) = copy_of(&path);
            let mut txn = env.begin_rw().expect("rw");
            let d = txn.open_table("d").expect("open").expect("d");
            let result = if dropping { txn.drop_table(d) } else { txn.clear_table(&d) };
            assert!(
                matches!(&result, Err(Error::Corrupt(m)) if m.contains(&message)),
                "{what}: {result:?}"
            );
            assert!(env.failure().is_some(), "{what}: latched");
            assert!(txn.writer_mut().alloc.freed().is_empty(), "{what}: nothing freed");
        }
    }
}
/// The steps of one stretch of an [`Oracle`] run: even stretches fill (a draw of a delete, a
/// `delete_prefix`, an abort, a clear or a drop is a put), odd ones run the whole mix, so the
/// tables grow nested and outer trees of two levels between the mix's shrinking steps.
const STRETCH: u32 = 250;

/// The subkey alphabet of [`Oracle::subkey`]: small, so short subkeys hit present rows.
const ALPHABET: &[u8] = b"abcd";

/// Oracle 1's dup tables, one per bound of [`Bound::ALL`], in its order.
const SUBJECTS: [&str; 3] = ["d0", "ds", "dd"];

/// What an [`Oracle`] run met in one subject (its coverage gate at that bound): the tallest
/// trees, and how many times each transition of [`Forms::change`] happened.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct FormSeen {
    /// The tallest nested tree (at a commit) and outer tree (at any step).
    pub(super) nested_height: u32,
    pub(super) outer_height: u32,
    /// Keys created packed.
    pub(super) packed_creates: u32,
    /// Packed keys promoted in the transaction that created them, and in a later one.
    pub(super) promotions_mid_txn: u32,
    pub(super) promotions_across_commits: u32,
    /// Promotions by a new value of a present row.
    pub(super) upsert_promotions: u32,
    /// Steps that left a packed set exactly at the limit, so still packed, and of those the
    /// creates (a one-row set at the limit).
    pub(super) at_bound: u32,
    pub(super) created_at_bound: u32,
    /// Deletes of a packed key's one row, and `delete_prefix` calls that removed a packed key.
    pub(super) packed_emptied: u32,
    pub(super) packed_prefix_hits: u32,
    /// Deletes that left a nested key one row, still nested.
    pub(super) nested_shrunk_to_one: u32,
    /// Keys created packed in the transaction that emptied their nested tree.
    pub(super) recreated_packed: u32,
}

impl FormSeen {
    fn figures(&self) -> [u32; 12] {
        [
            self.nested_height,
            self.outer_height,
            self.packed_creates,
            self.promotions_mid_txn,
            self.promotions_across_commits,
            self.upsert_promotions,
            self.at_bound,
            self.created_at_bound,
            self.packed_emptied,
            self.packed_prefix_hits,
            self.nested_shrunk_to_one,
            self.recreated_packed,
        ]
    }

    fn figures_mut(&mut self) -> [&mut u32; 12] {
        [
            &mut self.nested_height,
            &mut self.outer_height,
            &mut self.packed_creates,
            &mut self.promotions_mid_txn,
            &mut self.promotions_across_commits,
            &mut self.upsert_promotions,
            &mut self.at_bound,
            &mut self.created_at_bound,
            &mut self.packed_emptied,
            &mut self.packed_prefix_hits,
            &mut self.nested_shrunk_to_one,
            &mut self.recreated_packed,
        ]
    }

    /// `other` added: the heights (the first two figures) as the larger, the counts summed.
    fn add(&mut self, other: &Self) {
        for (i, (a, b)) in self.figures_mut().into_iter().zip(other.figures()).enumerate() {
            *a = if i < 2 { (*a).max(b) } else { *a + b };
        }
    }

    /// True if every figure reaches `min`'s.
    fn meets(&self, min: &Self) -> bool {
        self.figures().iter().zip(min.figures()).all(|(&a, b)| a >= b)
    }

    /// The counts of packed forms (every count but `nested_shrunk_to_one`).
    pub(super) fn packed(&self) -> [u32; 9] {
        [
            self.packed_creates,
            self.promotions_mid_txn,
            self.promotions_across_commits,
            self.upsert_promotions,
            self.at_bound,
            self.created_at_bound,
            self.packed_emptied,
            self.packed_prefix_hits,
            self.recreated_packed,
        ]
    }
}

/// What an [`Oracle`] run met (its coverage gate): how many times each kind of step happened,
/// and each subject's forms ([`FormSeen`], in [`SUBJECTS`]' order).
#[derive(Clone, Copy, Debug, Default)]
struct Coverage {
    /// Deletes that emptied an outer key, so its tree or set and its cell went.
    emptied_cells: u32,
    /// `delete_prefix` calls that removed rows, and those of a tree too large for one leaf.
    prefix_hits: u32,
    tall_prefix_hits: u32,
    key_exists: u32,
    /// Appends refused: an outer key below the last one; the last outer key with a subkey not
    /// past its last.
    append_below: u32,
    append_not_past: u32,
    /// Appends taken: under the last outer key; under a new one.
    append_same: u32,
    append_new: u32,
    commits: u32,
    aborts: u32,
    reopens: u32,
    clears: u32,
    drops: u32,
    readers_checked: u32,
    forms: [FormSeen; 3],
}

impl Coverage {
    /// `other` added: the counts summed, each subject's forms by [`FormSeen::add`].
    fn add(&mut self, o: &Self) {
        for (a, b) in self.counts_mut().into_iter().zip(o.counts()) {
            *a += b;
        }
        for (a, b) in self.forms.iter_mut().zip(&o.forms) {
            a.add(b);
        }
    }

    fn counts(&self) -> [u32; 14] {
        [
            self.emptied_cells,
            self.prefix_hits,
            self.tall_prefix_hits,
            self.key_exists,
            self.append_below,
            self.append_not_past,
            self.append_same,
            self.append_new,
            self.commits,
            self.aborts,
            self.reopens,
            self.clears,
            self.drops,
            self.readers_checked,
        ]
    }

    fn counts_mut(&mut self) -> [&mut u32; 14] {
        [
            &mut self.emptied_cells,
            &mut self.prefix_hits,
            &mut self.tall_prefix_hits,
            &mut self.key_exists,
            &mut self.append_below,
            &mut self.append_not_past,
            &mut self.append_same,
            &mut self.append_new,
            &mut self.commits,
            &mut self.aborts,
            &mut self.reopens,
            &mut self.clears,
            &mut self.drops,
            &mut self.readers_checked,
        ]
    }

    /// Every figure reaches `min`'s, each subject's forms too, and the subject that never packs
    /// met no packed form at all.
    #[track_caller]
    fn assert_meets(&self, min: &Self, what: &str) {
        assert!(self.counts().iter().zip(min.counts()).all(|(&a, b)| a >= b), "{what}: {self:?}");
        for ((name, seen), min) in SUBJECTS.iter().zip(&self.forms).zip(&min.forms) {
            assert!(seen.meets(min), "{what}: {name}: {seen:?}, at least {min:?}");
        }
        assert_eq!(self.forms[0].packed(), [0; 9], "{what}: {} never packs", SUBJECTS[0]);
    }
}

/// The bytes of a packed set's encoding of `rows` rows whose subkeys and values take `bytes`
/// together: the row count (2 bytes), a slot per row (its end and its subkey's length, 2 bytes
/// each), then the rows.
pub(super) const fn packed_len(rows: usize, bytes: usize) -> usize {
    2 + 4 * rows + bytes
}

/// The outer key number of logical key `key` (of a 4-byte outer key).
fn outer_of(key: &[u8]) -> u32 {
    u32::from_be_bytes(key[..4].try_into().expect("4 bytes"))
}

/// Each outer key `model` holds rows under, with their count and their set's [`packed_len`].
fn sets(model: &Rows) -> BTreeMap<u32, (usize, usize)> {
    let mut sets = BTreeMap::<u32, (usize, usize)>::new();
    for (key, value) in model {
        let set = sets.entry(outer_of(key)).or_default();
        set.0 += 1;
        set.1 += key.len() - 4 + value.len();
    }
    sets.into_iter().map(|(k, (rows, bytes))| (k, (rows, packed_len(rows, bytes)))).collect()
}

/// A step that changed an outer key's rows ([`Forms::change`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Change {
    /// A put, insert or append that added a row or (`upsert`) gave a present row a new value.
    Grow { upsert: bool },
    /// A delete that removed a row.
    Delete,
    /// A `delete_prefix` that removed every row.
    Prefix,
}

/// One subject's outer keys by form, by the size rules of a packed set (this test's own
/// statement of them, sharing no code with the engine): a set takes [`packed_len`] bytes; an
/// absent key is created packed if the table packs and its one row's set is within the limit,
/// else nested; a packed key is promoted to a nested tree by a step that leaves its set past the
/// limit, and stays packed otherwise; a nested key stays nested until it is emptied (its last row
/// deleted, `delete_prefix`, a clear or a drop).
#[derive(Debug)]
pub(super) struct Forms {
    /// The table's options as its `table_stat` reports them (the limit resolved).
    options: DupOptions,
    /// The nested outer keys: in the write transaction, and at the last commit.
    pub(super) nested: BTreeSet<u32>,
    committed: BTreeSet<u32>,
    /// The keys the write transaction created packed, and the nested keys it emptied.
    created: BTreeSet<u32>,
    emptied: BTreeSet<u32>,
    pub(super) seen: FormSeen,
}

impl Forms {
    /// The forms of an empty table of `options` at `page_size`-byte pages.
    pub(super) fn new(options: DupOptions, page_size: u32) -> Self {
        Self {
            options: options.resolved(page_size).expect("resolves"),
            nested: BTreeSet::new(),
            committed: BTreeSet::new(),
            created: BTreeSet::new(),
            emptied: BTreeSet::new(),
            seen: FormSeen::default(),
        }
    }

    /// A step `change` took outer key `k` from `was` rows to `rows` rows whose set takes `len`
    /// bytes.
    pub(super) fn change(
        &mut self,
        k: u32,
        was: usize,
        (rows, len): (usize, usize),
        change: Change,
    ) {
        let max = usize::from(self.options.packed_max);
        let seen = &mut self.seen;
        if rows == 0 {
            if self.nested.remove(&k) {
                self.emptied.insert(k);
            } else if change == Change::Prefix {
                seen.packed_prefix_hits += 1;
            } else {
                seen.packed_emptied += 1;
            }
        } else if was == 0 {
            if max != 0 && len <= max {
                seen.packed_creates += 1;
                seen.at_bound += u32::from(len == max);
                seen.created_at_bound += u32::from(len == max);
                seen.recreated_packed += u32::from(self.emptied.contains(&k));
                self.created.insert(k);
            } else {
                self.nested.insert(k);
            }
        } else if self.nested.contains(&k) {
            seen.nested_shrunk_to_one += u32::from(rows == 1 && was > 1);
        } else if len > max {
            self.nested.insert(k);
            if self.created.contains(&k) {
                seen.promotions_mid_txn += 1;
            } else {
                seen.promotions_across_commits += 1;
            }
            seen.upsert_promotions += u32::from(change == Change::Grow { upsert: true });
        } else {
            seen.at_bound += u32::from(len == max);
        }
    }

    /// A write transaction from a committed state whose nested keys are `nested` (the latest
    /// commit's, or a reopen's, which a crash may have taken back past later commits).
    pub(super) fn restart(&mut self, nested: &BTreeSet<u32>) {
        self.nested.clone_from(nested);
        self.end(true);
    }

    /// A clear or a drop: every key gone.
    pub(super) fn empty(&mut self) {
        self.emptied.append(&mut self.nested);
    }

    /// The write transaction ended: committed, or aborted (the last commit's forms back).
    pub(super) fn end(&mut self, commit: bool) {
        if commit {
            self.committed.clone_from(&self.nested);
        } else {
            self.nested.clone_from(&self.committed);
        }
        self.created.clear();
        self.emptied.clear();
    }
}

/// A read-only transaction held across later commits, with its handles and the rows of its
/// snapshot.
struct Reader {
    ro: crate::RoTxn,
    d: [crate::Table; 3],
    p: crate::Table,
    rows: Rows,
}

/// `got` (a call on a subject `name`) is `want` (the same call on `p`): one value, or the same
/// `Err` variant.
#[track_caller]
fn agree<T: PartialEq + std::fmt::Debug>(
    what: &str,
    name: &str,
    got: &Result<T>,
    want: &Result<T>,
) {
    match (got, want) {
        (Ok(x), Ok(y)) => assert_eq!(x, y, "{what}: {name} and p"),
        (Err(x), Err(y)) => assert_eq!(
            std::mem::discriminant(x),
            std::mem::discriminant(y),
            "{what}: {name} {x:?}, p {y:?}"
        ),
        _ => panic!("{what}: {name} {got:?}, p {want:?}"),
    }
}

/// Oracle 1: three dup tables, the subjects [`SUBJECTS`] (one per [`Bound`]), and their plain
/// twin `p`, keyed by the logical key `k ‖ s`, driven by the same steps in one write transaction
/// at a time, against the model `work` (the transaction's rows) and `committed`, and each
/// subject's [`Forms`].
struct Oracle {
    path: PathBuf,
    page_size: u32,
    /// Add values from one byte past the inline limit to three pages (about one in ten).
    large: bool,
    /// The outer keys `0..prefixes`, and the three hot ones that take about 60% of the picks.
    prefixes: u32,
    hot: [u32; 3],
    /// The longest value inline under every nested key: a value no longer is in a nested leaf.
    inline_max: usize,
    rng: Rng,
    env: Option<Env>,
    txn: Option<crate::RwTxn>,
    d: [crate::Table; 3],
    p: crate::Table,
    work: Rows,
    committed: Rows,
    forms: [Forms; 3],
    readers: Vec<Reader>,
    ops: u32,
    /// Every subject showed an outer and a nested tree of two levels at a commit since the last
    /// clear or drop: a clear or drop drawn before that is a stat.
    grown: bool,
    seen: Coverage,
}

impl Oracle {
    fn new(seed: u64, page_size: u32, large: bool, prefixes: u32) -> (TempDir, Self) {
        let (dir, path) = temp("tndb_b_dup_oracle");
        let env =
            Env::open(&path, EnvOptions { page_size, ..EnvOptions::default() }).expect("open");
        let mut txn = env.begin_rw().expect("rw");
        let d = Self::create(&mut txn);
        let p = txn.create_table("p").expect("p");
        txn.commit().expect("commit");
        let geometry = page::Geometry::new(page_size).expect("geometry");
        let mut oracle = Self {
            path,
            page_size,
            large,
            prefixes,
            hot: [1, prefixes / 2, prefixes - 2],
            inline_max: geometry.max_value(12),
            rng: Rng(seed),
            env: Some(env),
            txn: None,
            d,
            p,
            work: Rows::new(),
            committed: Rows::new(),
            forms: Bound::ALL.map(|bound| Forms::new(bound.options(4), page_size)),
            readers: Vec::new(),
            ops: 0,
            grown: false,
            seen: Coverage::default(),
        };
        oracle.begin();
        (dir, oracle)
    }

    /// The subjects, created in `txn`.
    fn create(txn: &mut crate::RwTxn) -> [crate::Table; 3] {
        let mut bounds = Bound::ALL.into_iter();
        SUBJECTS.map(|name| {
            let options = bounds.next().expect("a bound per subject").options(4);
            txn.create_dup_table(name, options).expect("a subject")
        })
    }

    /// The subjects' and `p`'s handles in `txn`.
    fn open(txn: &impl ReadTxn) -> ([crate::Table; 3], crate::Table) {
        let d = SUBJECTS.map(|name| txn.open_table(name).expect("open").expect("a subject"));
        (d, txn.open_table("p").expect("open").expect("p"))
    }

    fn env(&self) -> &Env {
        self.env.as_ref().expect("open")
    }

    fn txn(&mut self) -> &mut crate::RwTxn {
        self.txn.as_mut().expect("a write transaction")
    }

    /// A new write transaction, the handles resolved in it.
    fn begin(&mut self) {
        let txn = self.env().begin_rw().expect("rw");
        (self.d, self.p) = Self::open(&txn);
        self.txn = Some(txn);
    }

    /// `call` on `p`, then on each subject, whose result must be `p`'s ([`agree`]); returns
    /// `p`'s.
    fn all<T: PartialEq + std::fmt::Debug>(
        &mut self,
        what: &str,
        call: impl Fn(&mut crate::RwTxn, &crate::Table) -> Result<T>,
    ) -> Result<T> {
        let txn = self.txn.as_mut().expect("a write transaction");
        let want = call(txn, &self.p);
        for (name, d) in SUBJECTS.iter().zip(&self.d) {
            agree(what, name, &call(txn, d), &want);
        }
        want
    }

    fn prefix(&mut self) -> u32 {
        if self.rng.below(10) < 6 {
            self.hot[self.rng.below(3) as usize]
        } else {
            self.rng.below(u64::from(self.prefixes)) as u32
        }
    }

    fn subkey(&mut self) -> Vec<u8> {
        let len = 1 + self.rng.below(12) as usize;
        (0..len).map(|_| ALPHABET[self.rng.below(ALPHABET.len() as u64) as usize]).collect()
    }

    fn key(&mut self) -> Vec<u8> {
        let (k, s) = (self.prefix(), self.subkey());
        dup_key(k, &s)
    }

    /// A row of `work` (the first at or after a random key), or a random key when it is empty.
    fn present(&mut self) -> Vec<u8> {
        let probe = self.key();
        let found = self.work.range(probe.clone()..).next().or_else(|| self.work.iter().next());
        found.map_or(probe, |(key, _)| key.clone())
    }

    /// A value for logical key `key`: 0 to 64 bytes; in a `large` run about one in ten from one
    /// byte past `p`'s inline limit to three pages; else one in 16 the length that makes `key`'s
    /// row alone a set at the limit of a subject that packs, or one byte past it.
    fn value(&mut self, key: &[u8]) -> Vec<u8> {
        let len = if self.large && self.rng.below(10) == 0 {
            let low =
                page::Geometry::new(self.page_size).expect("geometry").max_value(key.len()) + 1;
            low + self.rng.below((3 * self.page_size as usize - low + 1) as u64) as usize
        } else if self.rng.below(16) == 0 {
            let limit = usize::from(self.forms[1 + self.rng.below(2) as usize].options.packed_max);
            limit - packed_len(1, key.len() - 4) + self.rng.below(2) as usize
        } else {
            self.rng.below(65) as usize
        };
        super::value(self.rng.next() as u32, self.ops, len)
    }

    /// Outer key `k`'s rows in `work`, and their set's [`packed_len`].
    fn set_of(&self, k: u32) -> (usize, usize) {
        let (mut rows, mut bytes) = (0, 0);
        for (key, value) in
            self.work.range(dup_key(k, &[])..).take_while(|(key, _)| outer_of(key) == k)
        {
            rows += 1;
            bytes += key.len() - 4 + value.len();
        }
        (rows, packed_len(rows, bytes))
    }

    /// Every subject's forms after step `change` took outer key `k` from `was` rows to those
    /// `work` now holds.
    fn changed(&mut self, k: u32, was: usize, change: Change) {
        let now = self.set_of(k);
        for forms in &mut self.forms {
            forms.change(k, was, now, change);
        }
    }

    /// The checks after every step: the entries of every table and each subject's nested trees
    /// against `work` and its [`Forms`], and every 16 steps the rows of all by walks.
    fn after_op(&mut self) {
        let txn = self.txn.as_ref().expect("a write transaction");
        let n = self.work.len() as u64;
        let stat = txn.table_stat(&self.p).expect("stat");
        assert_eq!(stat.entries, n, "step {}: p's entries", self.ops);
        for ((name, d), forms) in SUBJECTS.iter().zip(&self.d).zip(&mut self.forms) {
            let stat = txn.table_stat(d).expect("stat");
            let (entries, trees) = (stat.entries, stat.nested_trees as usize);
            let want = (n, forms.nested.len());
            assert_eq!((entries, trees), want, "step {}: {name}'s entries, trees", self.ops);
            forms.seen.outer_height = forms.seen.outer_height.max(stat.height);
        }
        self.ops += 1;
        if self.ops.is_multiple_of(16) {
            self.compare();
        }
    }

    /// The rows of each subject (the overlay walker) and of `p` (both scans) against `work`.
    fn compare(&self) {
        let txn = self.txn.as_ref().expect("a write transaction");
        let expected: Vec<_> = self.work.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (name, d) in SUBJECTS.iter().zip(&self.d) {
            let walked = txn.dup_rows_for_test(d);
            assert!(
                walked == expected,
                "step {}: {name}'s rows {}, the model {}",
                self.ops,
                walked.len(),
                expected.len()
            );
        }
        assert!(super::rows(txn, &self.p) == self.work, "step {}: p's rows", self.ops);
    }

    /// A put: one in four a new value of a present row, so packed sets grow past the limit by
    /// an upsert too.
    fn put(&mut self) {
        let key = if self.rng.below(4) == 0 { self.present() } else { self.key() };
        let value = self.value(&key);
        let (k, upsert) = (outer_of(&key), self.work.contains_key(&key));
        let was = self.set_of(k).0;
        self.all("put", |txn, t| txn.put(t, &key, &value)).expect("put");
        self.work.insert(key, value);
        self.changed(k, was, Change::Grow { upsert });
    }

    fn insert(&mut self) {
        let key = if self.rng.below(2) == 0 { self.present() } else { self.key() };
        let value = self.value(&key);
        let k = outer_of(&key);
        let was = self.set_of(k).0;
        let got = self.all("insert", |txn, t| txn.insert(t, &key, &value));
        match self.work.entry(key) {
            std::collections::btree_map::Entry::Occupied(_) => {
                assert!(matches!(got, Err(Error::KeyExists)), "insert of a present key: {got:?}");
                self.seen.key_exists += 1;
            }
            std::collections::btree_map::Entry::Vacant(slot) => {
                got.expect("insert");
                slot.insert(value);
                self.changed(k, was, Change::Grow { upsert: false });
            }
        }
    }

    fn append(&mut self) {
        let last = self.work.keys().next_back().cloned();
        let valid = self.rng.below(2) == 0;
        let (key, kind) = match last {
            None => (self.key(), 3),
            Some(last) => {
                let (k, s) = (outer_of(&last), &last[4..]);
                if valid && s.len() < 12 && self.rng.below(2) == 0 {
                    let c = ALPHABET[self.rng.below(ALPHABET.len() as u64) as usize];
                    ([&last[..], &[c]].concat(), 2)
                } else if valid {
                    let s = self.subkey();
                    (dup_key(k + 1 + self.rng.below(3) as u32, &s), 3)
                } else if k > 0 && self.rng.below(2) == 0 {
                    let (below, s) = (self.rng.below(u64::from(k)) as u32, self.subkey());
                    (dup_key(below, &s), 0)
                } else {
                    let len = 1 + self.rng.below(s.len() as u64) as usize;
                    (dup_key(k, &s[..len]), 1)
                }
            }
        };
        let value = self.value(&key);
        let k = outer_of(&key);
        let was = self.set_of(k).0;
        let got = self.all("append", |txn, t| txn.append(t, &key, &value));
        let past = self.work.keys().next_back().is_none_or(|last| key > *last);
        assert_eq!(past, kind >= 2, "the generator's kind {kind}");
        if past {
            got.expect("append past the last key");
            self.work.insert(key, value);
            self.changed(k, was, Change::Grow { upsert: false });
        } else {
            assert!(matches!(got, Err(Error::AppendOutOfOrder)), "append: {got:?}");
        }
        *[
            &mut self.seen.append_below,
            &mut self.seen.append_not_past,
            &mut self.seen.append_same,
            &mut self.seen.append_new,
        ][kind] += 1;
    }

    fn delete(&mut self) {
        let key = if self.rng.below(3) < 2 { self.present() } else { self.key() };
        let k = outer_of(&key);
        let was = self.set_of(k).0;
        let found = self.all("delete", |txn, t| txn.delete(t, &key)).expect("delete");
        assert_eq!(found, self.work.remove(&key).is_some(), "delete {key:02x?}");
        if found {
            self.changed(k, was, Change::Delete);
            self.seen.emptied_cells += u32::from(was == 1);
        }
    }

    /// The rows of outer key `k` in `work`, and whether they cannot be one leaf: their cells
    /// (each a slot, a header, the subkey and an inline value or, past the limit that holds under
    /// every subkey, at least a run reference) outgrow a leaf's room, so the tree has two levels.
    fn rows_of(&self, k: u32) -> (Vec<Vec<u8>>, bool) {
        let rows: Vec<_> = self
            .work
            .range(dup_key(k, &[])..)
            .take_while(|(key, _)| key[..4] == k.to_be_bytes())
            .collect();
        let bytes: usize = rows
            .iter()
            .map(|(key, v)| {
                let value = if v.len() <= self.inline_max { v.len() } else { overflow::OVF_CELL };
                page::SLOT + page::LEAF_CELL_HEADER + key.len() - 4 + value
            })
            .sum();
        let room = self.page_size as usize - page::HEADER - page::TRAILER;
        (rows.into_iter().map(|(key, _)| key.clone()).collect(), bytes > room)
    }

    fn delete_prefix(&mut self) {
        // Three in 64 take the fullest hot key once it has two levels, 45 a present key, the rest
        // any.
        let tall = self.hot.into_iter().max_by_key(|&k| self.rows_of(k).0.len()).expect("hot");
        let k = match self.rng.below(64) {
            _ if self.work.is_empty() => self.rng.below(u64::from(self.prefixes)) as u32,
            0..3 if self.rows_of(tall).1 => tall,
            0..48 => {
                // The outer key of the first row at or past a uniform one (a hot key no more
                // often than another).
                let probe = dup_key(self.rng.below(u64::from(self.prefixes)) as u32, &[]);
                let found = self.work.range(probe..).next().or_else(|| self.work.iter().next());
                outer_of(&found.expect("a row").0[..])
            }
            _ => self.rng.below(u64::from(self.prefixes + self.prefixes / 8)) as u32,
        };
        let (rows, two_levels) = self.rows_of(k);
        let txn = self.txn.as_mut().expect("a write transaction");
        let removed = self
            .d
            .each_ref()
            .map(|d| txn.delete_prefix(d, &k.to_be_bytes()).expect("delete_prefix"));
        for key in &rows {
            assert!(txn.delete(&self.p, key).expect("delete"), "p's twin of {key:02x?}");
        }
        let want = remove_prefix(&mut self.work, k);
        assert_eq!(removed, [want; 3], "delete_prefix {k}: {SUBJECTS:?}");
        if want > 0 {
            self.changed(k, rows.len(), Change::Prefix);
            self.seen.prefix_hits += 1;
            self.seen.tall_prefix_hits += u32::from(two_levels);
        }
    }

    fn get(&mut self) {
        let key = match self.rng.below(3) {
            0 => self.present(),
            1 => self.key(),
            _ => {
                let s = self.subkey();
                dup_key(self.prefixes + 1000 + self.rng.below(100) as u32, &s)
            }
        };
        let txn = self.txn.as_ref().expect("a write transaction");
        let want = self.work.get(&key).map(Vec::as_slice);
        for (name, d) in SUBJECTS.iter().zip(&self.d) {
            assert_eq!(txn.get(d, &key).expect("get"), want, "{name}'s get {key:02x?}");
        }
        assert_eq!(txn.get(&self.p, &key).expect("get"), want, "p's get {key:02x?}");
    }

    fn stat(&mut self) {
        let txn = self.txn.as_ref().expect("a write transaction");
        for ((name, d), forms) in SUBJECTS.iter().zip(&self.d).zip(&self.forms) {
            assert_eq!(txn.table_stat(d).expect("stat").dup, Some(forms.options), "{name}");
        }
        assert_eq!(txn.table_stat(&self.p).expect("stat").dup, None);
    }

    /// The write transaction ended: committed (after a full compare) or aborted.
    fn end(&mut self, commit: bool) {
        if commit {
            self.compare();
            self.txn.take().expect("a write transaction").commit().expect("commit");
            self.committed = self.work.clone();
            self.seen.commits += 1;
        } else {
            self.txn.take().expect("a write transaction").abort();
            self.work = self.committed.clone();
            self.seen.aborts += 1;
        }
        for forms in &mut self.forms {
            forms.end(commit);
        }
        if commit {
            self.after_commit();
        }
    }

    /// `Env::check` with every table's entries, each subject's cells by its [`Forms`], and every
    /// held reader against its snapshot's rows.
    fn after_commit(&mut self) {
        let n = self.committed.len() as u64;
        let env = self.env.as_ref().expect("open");
        let report = check_subjects(env);
        assert_eq!(report.tables["p"].entries, n, "check: p");
        for name in SUBJECTS {
            assert_eq!(report.tables[name].entries, n, "check: {name}");
        }
        self.check_cells();
        for reader in &self.readers {
            self.seen.readers_checked += 1;
            Self::verify(reader, &mut self.rng);
        }
    }

    /// Each subject's outer cells at the last commit, by its [`Forms`]: a nested root under every
    /// key they hold nested (the tallest's height into its gate) and under every other a packed
    /// set of the committed rows (its row count and its encoding's length, by [`packed_len`]).
    fn check_cells(&mut self) {
        let env = self.env.as_ref().expect("open");
        let snapshot = env.inner().current();
        let sets = sets(&self.committed);
        let mut grown = true;
        for (name, forms) in SUBJECTS.iter().zip(&mut self.forms) {
            let outer = tree(env, name);
            let walk = if forms.options.packed_max == 0 {
                walk_outer_tree(&*snapshot, &outer)
            } else {
                walk_packing_tree(&*snapshot, &outer)
            };
            assert_eq!(walk.entries.len(), sets.len(), "{name}: outer keys");
            let mut tallest = 0;
            for ((key, cell), (&k, &(rows, len))) in walk.entries.iter().zip(&sets) {
                assert_eq!(outer_of(key), k, "{name}: outer keys");
                if forms.committed.contains(&k) {
                    let root = TreeRoot::decode(cell.as_slice().try_into().expect("a nested cell"));
                    tallest = tallest.max(root.expect("a nested root").height);
                } else {
                    let n = cell.get(..2).map(|n| usize::from(u16::from_le_bytes([n[0], n[1]])));
                    assert_eq!((cell.len(), n), (len, Some(rows)), "{name}: key {k}'s packed set");
                }
            }
            forms.seen.nested_height = forms.seen.nested_height.max(tallest);
            grown &= outer.height >= 2 && tallest >= 2;
        }
        self.grown |= grown;
    }

    /// `reader`'s rows by the walker, a sample of gets on every table and their entries, against
    /// its snapshot's.
    fn verify(reader: &Reader, rng: &mut Rng) {
        let Reader { ro, d, p, rows } = reader;
        let expected: Vec<_> = rows.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let n = rows.len() as u64;
        let id = ro.txn_id();
        for (name, d) in SUBJECTS.iter().zip(d) {
            assert!(ro.dup_rows_for_test(d) == expected, "reader {id}: {name}'s rows");
            assert_eq!(ro.table_stat(d).expect("stat").entries, n, "reader {id}: {name}");
        }
        assert_eq!(ro.table_stat(p).expect("stat").entries, n, "reader {id}: p");
        for _ in 0..8 {
            let probe = dup_key(rng.below(1 << 10) as u32, &[ALPHABET[rng.below(4) as usize]]);
            let key = rows.range(probe.clone()..).next().map_or(probe, |(k, _)| k.clone());
            let want = rows.get(&key).map(Vec::as_slice);
            for (name, d) in SUBJECTS.iter().zip(d) {
                assert_eq!(ro.get(d, &key).expect("get"), want, "reader {id}: {name}");
            }
            assert_eq!(ro.get(p, &key).expect("get"), want, "reader {id}: p");
        }
    }

    fn hold(&mut self) {
        if self.readers.len() < 4 {
            let ro = self.env().begin_ro().expect("ro");
            let (d, p) = Self::open(&ro);
            self.readers.push(Reader { ro, d, p, rows: self.committed.clone() });
        }
    }

    fn release(&mut self) {
        if !self.readers.is_empty() {
            let reader = self.readers.remove(self.rng.below(self.readers.len() as u64) as usize);
            Self::verify(&reader, &mut self.rng);
            self.seen.readers_checked += 1;
        }
    }

    /// Commit or abort, drop the readers, close and reopen; then `Env::check` and the committed
    /// rows of every subject (walker and gets) and of `p`.
    fn reopen(&mut self) {
        let commit = self.rng.below(2) == 0;
        self.end(commit);
        self.readers.clear();
        drop(self.env.take());
        let options = EnvOptions { page_size: self.page_size, ..EnvOptions::default() };
        self.env = Some(Env::open(&self.path, options).expect("reopen"));
        self.seen.reopens += 1;
        let env = self.env();
        let report = check_subjects(env);
        let n = self.committed.len() as u64;
        assert_eq!(report.tables["p"].entries, n, "reopen: p");
        let ro = env.begin_ro().expect("ro");
        let (d, p) = Self::open(&ro);
        for (name, d) in SUBJECTS.iter().zip(&d) {
            assert_eq!(report.tables[*name].entries, n, "reopen: {name}");
            let get = |key: &[u8]| ro.get(d, key).map(|v| v.map(<[u8]>::to_vec));
            assert_rows(&format!("reopen: {name}"), ro.dup_rows_for_test(d), get, &self.committed);
        }
        assert!(super::rows(&ro, &p) == self.committed, "reopen: p's rows");
        drop(ro);
        self.begin();
    }

    fn clear(&mut self) {
        let txn = self.txn.as_mut().expect("a write transaction");
        for d in &self.d {
            txn.clear_table(d).expect("clear a subject");
        }
        txn.clear_table(&self.p).expect("clear p");
        self.work.clear();
        self.forms.iter_mut().for_each(Forms::empty);
        self.seen.clears += 1;
        self.grown = false;
    }

    fn drop_and_create(&mut self) {
        let (d, p) = (self.d.clone(), self.p.clone());
        let txn = self.txn();
        for d in d {
            txn.drop_table(d).expect("drop a subject");
        }
        txn.drop_table(p).expect("drop p");
        self.d = Self::create(txn);
        self.p = self.txn().create_table("p").expect("p");
        self.work.clear();
        self.forms.iter_mut().for_each(Forms::empty);
        self.seen.drops += 1;
        self.grown = false;
    }

    /// One step drawn per mille from the mix (in a filling stretch, a shrinking draw is a put).
    fn step(&mut self, step: u32) {
        let fill = (step / STRETCH).is_multiple_of(2);
        match self.rng.below(1000) {
            0..300 => self.put(),
            300..400 => self.insert(),
            400..480 => self.append(),
            480..700 | 940..960 | 995..1000 if fill => self.put(),
            480..660 => self.delete(),
            660..700 => self.delete_prefix(),
            700..850 => self.get(),
            850..880 => self.stat(),
            880..940 => {
                self.end(true);
                self.begin();
            }
            940..960 => {
                self.end(false);
                self.begin();
            }
            960..980 => self.hold(),
            980..990 => self.release(),
            990..995 => self.reopen(),
            995..998 if self.grown => self.clear(),
            998..1000 if self.grown => self.drop_and_create(),
            _ => self.stat(),
        }
        self.after_op();
    }
}

/// `Env::check` of `env`, which must pass, with each subject's figures those of its
/// `table_stat` ([`assert_dup_figures`]).
#[track_caller]
fn check_subjects(env: &Env) -> CheckReport {
    let report = check(env);
    let ro = env.begin_ro().expect("ro");
    for name in SUBJECTS {
        assert_dup_figures(&report, &ro, name);
    }
    report
}

/// Run `steps` steps of Oracle 1 from `seed` at `page_size` over `prefixes` outer keys, with
/// large values or not; returns what it met.
fn oracle_run(seed: u64, steps: u32, page_size: u32, large: bool, prefixes: u32) -> Coverage {
    let _report = ReportSeed(seed);
    let (_dir, mut oracle) = Oracle::new(seed, page_size, large, prefixes);
    for step in 0..steps {
        oracle.step(step);
    }
    oracle.end(true);
    oracle.readers.clear();
    let mut seen = oracle.seen;
    seen.forms = oracle.forms.each_ref().map(|forms| forms.seen);
    seen
}

/// The forms gate of each subject ([`SUBJECTS`]): nested and outer trees of two levels at every
/// bound; at [`Bound::Small`] (most keys created packed, promoted at the second or third row)
/// every packed transition, `at_bound` of them at least `at_bound` times; at [`Bound::Default`]
/// (the cold keys packed, the hot ones promoted) creates, a create at the limit, promotions of
/// each kind and `delete_prefix` of packed keys.
fn forms_gate(at_bound: u32) -> [FormSeen; 3] {
    let trees = FormSeen { nested_height: 2, outer_height: 2, ..FormSeen::default() };
    [
        trees,
        FormSeen {
            packed_creates: 50,
            promotions_mid_txn: 20,
            promotions_across_commits: 10,
            upsert_promotions: 3,
            at_bound,
            created_at_bound: 3,
            packed_emptied: 5,
            packed_prefix_hits: 5,
            nested_shrunk_to_one: 3,
            recreated_packed: 2,
            ..trees
        },
        FormSeen {
            packed_creates: 50,
            promotions_mid_txn: 1,
            promotions_across_commits: 1,
            upsert_promotions: 1,
            created_at_bound: 1,
            packed_prefix_hits: 3,
            ..trees
        },
    ]
}

/// Oracle 1: three dup tables, one per [`Bound`] (never packing, a small packed-set limit and the
/// default one), and a plain table keyed by their logical keys (`k ‖ s`) take the same random
/// puts, inserts (present keys `KeyExists`), appends (past the last logical key under the last
/// outer key or a new one; refused below the last outer key and under it not past its last
/// subkey: `AppendOutOfOrder`), deletes, `delete_prefix` calls (the plain twin deleting the
/// model's rows of the outer key one by one), gets, stats, commits, aborts, held readers, reopens,
/// clears and drops, against a model: all answer alike and as the model says; after every step
/// each holds the model's entry count and each dup table the nested trees of its [`Forms`] (the
/// size rules of a packed set, so every promotion, every set left at the limit and no demotion
/// are the engine's totals); every 16 steps and before each commit the rows of all by walks;
/// after every commit `Env::check` passes, every dup table's cells are its forms' (a packed set's
/// row count and length) and every held reader reads its snapshot; after every reopen the
/// committed rows read back. Three hot outer keys grow nested trees of two levels, and enough
/// outer keys an outer tree of two levels, in every dup table.
#[test]
fn dup_tables_match_a_composite_model() {
    let mut seen = Coverage::default();
    for (seed, large) in [(0xD0B1_0001, false), (0xD0B1_0002, false), (0xD0B1_0003, true)] {
        seen.add(&oracle_run(seed, 2000, 4096, large, 120));
    }
    let min = Coverage {
        emptied_cells: 5,
        prefix_hits: 5,
        tall_prefix_hits: 1,
        key_exists: 5,
        append_below: 3,
        append_not_past: 3,
        append_same: 5,
        append_new: 5,
        commits: 20,
        aborts: 3,
        reopens: 2,
        clears: 1,
        drops: 1,
        readers_checked: 10,
        forms: forms_gate(1),
    };
    seen.assert_meets(&min, "the runs");
}

#[test]
#[ignore = "heavy: run with --release"]
fn dup_tables_match_a_composite_model_heavy() {
    // The runs are independent: run them side by side.
    let runs: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8u64)
            .map(|seed| {
                s.spawn(move || {
                    let (page_size, large) = ([4096, 16384][(seed % 2) as usize], seed % 4 >= 2);
                    let run = oracle_run(0xD0B1_1000 + seed, 40_000, page_size, large, 400);
                    eprintln!("{seed}: {page_size} large {large}: {run:?}");
                    (page_size, run)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("a run")).collect()
    });
    // The trees of three levels are out of reach: a nested one takes some ten thousand rows
    // under one outer key at 4 KiB, an outer one some ten thousand outer keys.
    let min = Coverage {
        emptied_cells: 200,
        prefix_hits: 200,
        tall_prefix_hits: 1,
        key_exists: 200,
        append_below: 100,
        append_not_past: 100,
        append_same: 100,
        append_new: 100,
        commits: 1000,
        aborts: 100,
        reopens: 50,
        clears: 1,
        drops: 1,
        readers_checked: 1000,
        forms: forms_gate(20),
    };
    // Each page size's four runs together: a drop needs a grown table at a one-in-a-thousand
    // draw, and a promotion inside one transaction at the default bound a large value.
    for page_size in [4096, 16384] {
        let mut seen = Coverage::default();
        for (_, run) in runs.iter().filter(|(size, _)| *size == page_size) {
            seen.add(run);
        }
        seen.assert_meets(&min, &format!("{page_size}"));
    }
}
