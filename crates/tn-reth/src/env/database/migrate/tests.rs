//! Tests for the MDBX to tndb-b migration: a round trip of every table, the directory states, a
//! stop at each step, the verification before the commit point, the refusals, and an on-demand
//! migration of a copy of a real database.

use super::*;
use crate::env::database::{db_table_stats, open_tndb, temp_chain_db_args, RethDb};
use reth_db::{
    cursor::DbDupCursorRO as _,
    mdbx::DatabaseFlags,
    table::DupSort,
    tables::{self, RawDupSort, RawKey, RawValue},
    transaction::DbTxMut,
    ClientVersion,
};
use reth_node_core::args::DatabaseArgs;
use std::collections::BTreeMap;
use tempfile::TempDir;

/// One table's raw rows: encoded keys and compressed values, in order.
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// MDBX's page size in these tests: a dup value may be up to about a third of a page on MDBX, so
/// 16 KiB pages take the dup values below on every platform.
const MDBX_PAGE_SIZE: usize = 16384;

/// The page size the migrated file is asked for (not tndb-b's default 4096), so the tests see the
/// migration take the node's `--db.page-size`.
const TNDB_PAGE_SIZE: usize = 8192;

/// A table the test's MDBX database never creates, as one whose schema predates the table.
const MISSING: &str = tables::PruneCheckpoints::NAME;

/// A table the test's MDBX database creates and leaves empty.
const EMPTY: &str = Tables::Transactions.name();

/// reth's dup-sort tables in the order of `Tables::ALL`, with each one's key and subkey widths
/// (the adapter's layouts).
const DUP_TABLES: [(&str, usize, usize); 5] = [
    (tables::PlainStorageState::NAME, 20, 32),
    (tables::AccountChangeSets::NAME, 8, 20),
    (tables::StorageChangeSets::NAME, 28, 32),
    (tables::HashedStorages::NAME, 32, 32),
    (tables::StoragesTrie::NAME, 32, 65),
];

/// The `--db.*` settings the node opens the migrated database with.
fn tndb_db_args() -> DatabaseArgs {
    DatabaseArgs { page_size: Some(TNDB_PAGE_SIZE), ..temp_chain_db_args() }
}

/// A client version for the `VersionHistory` rows.
fn version(version: &str) -> ClientVersion {
    ClientVersion {
        version: version.to_string(),
        git_sha: "0123abc".to_string(),
        build_timestamp: "2026-01-01T00:00:00Z".to_string(),
    }
}

/// The raw rows the test's MDBX database holds, by table:
/// - `CanonicalHeaders`: 3000 rows, so its copy takes several of the tests' write transactions;
/// - `Bytecodes`: three values in overflow runs on both engines;
/// - `PlainAccountState`: values of 2500 bytes, past tndb-b's inline limit at its page size;
/// - every dup-sort table: three keys of four values each, distinct subkeys written out of order,
///   one value per key of 2500 bytes (an overflow run on tndb-b);
/// - `VersionHistory`: two client versions;
/// - [`EMPTY`] none, and [`MISSING`] is not created.
fn mdbx_rows() -> BTreeMap<&'static str, Rows> {
    let mut rows = BTreeMap::<&'static str, Rows>::new();
    rows.insert(
        tables::CanonicalHeaders::NAME,
        (0..3000u64).map(|n| (n.to_be_bytes().to_vec(), vec![n as u8; 32])).collect(),
    );
    rows.insert(
        tables::Bytecodes::NAME,
        (1..=3u8).map(|n| (vec![n; 32], vec![n; 20_000 + usize::from(n)])).collect(),
    );
    rows.insert(
        tables::PlainAccountState::NAME,
        (1..=4u8).map(|n| (vec![n; 20], vec![n; 2500])).collect(),
    );
    for (name, key_len, subkey_len) in DUP_TABLES {
        let mut table = Rows::new();
        for key in 1..=3u8 {
            for subkey in [9u8, 2, 7, 4] {
                let mut value = vec![subkey; subkey_len];
                value.extend(vec![key; if subkey == 7 { 2500 } else { usize::from(subkey) }]);
                table.push((vec![key; key_len], value));
            }
        }
        table.sort();
        rows.insert(name, table);
    }
    let history = [(1000u64, version("0.9.0")), (2000, version("1.0.0"))];
    let history = history.into_iter().map(|(at, v)| {
        use reth_db::table::{Compress as _, Encode as _};
        (at.encode().to_vec(), v.compress().to_vec())
    });
    rows.insert(tables::VersionHistory::NAME, history.collect());
    rows
}

/// The dup-sort tables [`rows_with_big_dup_keys`] gives a [`big_key`], with its rows: more than
/// [`COMMIT_ROWS`], so the copy commits inside the key (twice, or once for `StoragesTrie`). The
/// three state tables are the adapter's nested ones, `StorageChangeSets` a composite one.
const BIG_KEYS: [(&str, u64); 4] = [
    (tables::PlainStorageState::NAME, 2500),
    (tables::StorageChangeSets::NAME, 2500),
    (tables::HashedStorages::NAME, 2500),
    (tables::StoragesTrie::NAME, 1200),
];

/// The big key of a dup-sort table whose keys are `key_len` bytes: between [`mdbx_rows`]'s second
/// and third keys, so rows of other keys come before and after it.
fn big_key(key_len: usize) -> Vec<u8> {
    let mut key = vec![2; key_len];
    key[key_len - 1] = 0x80;
    key
}

/// [`mdbx_rows`] with a [`big_key`] in each table of [`BIG_KEYS`], of that many rows: subkey `n`
/// is `n` big-endian in a 32-byte subkey, and in `StoragesTrie`'s 65-byte one the nibble path of
/// `n`'s three low nibbles (64 nibble bytes, then the path's length, as reth encodes a
/// `StoredNibblesSubKey`).
fn rows_with_big_dup_keys() -> BTreeMap<&'static str, Rows> {
    let mut rows = mdbx_rows();
    for (name, count) in BIG_KEYS {
        assert!(count > COMMIT_ROWS, "{name}: the big key's rows span a commit");
        let (_, key_len, subkey_len) =
            DUP_TABLES.into_iter().find(|(table, ..)| *table == name).expect("a dup-sort table");
        let table = rows.get_mut(name).expect("a dup-sort table's rows");
        for n in 0..count {
            let mut value = vec![0; subkey_len];
            if subkey_len == 65 {
                value[..3].copy_from_slice(&[
                    (n >> 8) as u8 & 0xf,
                    (n >> 4) as u8 & 0xf,
                    n as u8 & 0xf,
                ]);
                value[64] = 3;
            } else {
                value[subkey_len - 8..].copy_from_slice(&n.to_be_bytes());
            }
            value.extend([n as u8, 1]);
            table.push((big_key(key_len), value));
        }
        table.sort();
    }
    rows
}

/// The passes of [`Step::MidCopy`] a migration of `rows` makes before it copies `table`: a table
/// copied before it (in the order of `Tables::ALL`) passes once per bounded commit and once at its
/// end, and [`MISSING`], which MDBX lacks, not at all.
fn mid_copy_passes_before(rows: &BTreeMap<&'static str, Rows>, table: &str) -> u32 {
    let passes: u64 = Tables::ALL
        .iter()
        .map(Tables::name)
        .take_while(|name| *name != table)
        .filter(|name| *name != MISSING)
        .map(|name| rows.get(name).map_or(0, Vec::len) as u64 / COMMIT_ROWS + 1)
        .sum();
    passes.try_into().expect("passes")
}

/// The [`TableViewer`] that writes a table's rows from a map of raw rows.
struct PutRows<'a, TX> {
    tx: &'a TX,
    rows: &'a BTreeMap<&'static str, Rows>,
}

impl<TX: DbTxMut> TableViewer<()> for PutRows<'_, TX> {
    type Error = DatabaseError;

    fn view<T: Table>(&self) -> Result<(), DatabaseError> {
        for (key, value) in self.rows.get(T::NAME).into_iter().flatten() {
            let (key, value) = (RawKey::from_vec(key.clone()), RawValue::from_vec(value.clone()));
            self.tx.put::<RawTable<T>>(key, value)?;
        }
        Ok(())
    }
}

/// The [`TableViewer`] that reads a table's raw rows; a table the database lacks has none.
struct ReadRows<'a, TX> {
    tx: &'a TX,
}

impl<TX: DbTx> TableViewer<Rows> for ReadRows<'_, TX> {
    type Error = DatabaseError;

    fn view<T: Table>(&self) -> Result<Rows, DatabaseError> {
        let mut cursor = match self.tx.cursor_read::<RawTable<T>>() {
            Ok(cursor) => cursor,
            Err(DatabaseError::Open(info)) if info.code == MdbxError::NotFound.to_err_code() => {
                return Ok(Rows::new());
            }
            Err(e) => return Err(e),
        };
        cursor
            .walk(None)?
            .map(|row| row.map(|(key, value)| (key.into_key(), value.into_value())))
            .collect()
    }
}

/// Every one of reth's tables' raw rows in `db`, by name (a table it lacks with none).
fn raw_rows<D: Database>(db: &D) -> BTreeMap<&'static str, Rows> {
    let tx = db.tx().expect("tx");
    Tables::ALL
        .iter()
        .map(|table| (table.name(), table.view(&ReadRows { tx: &tx }).expect("rows")))
        .collect()
}

/// Create an MDBX reth database in `path` with [`mdbx_rows`], as an older node left it (reth's
/// tables but [`MISSING`], and reth's `database.version` file), and return every table's rows.
fn build_mdbx(path: &Path) -> BTreeMap<&'static str, Rows> {
    build_mdbx_with(path, mdbx_rows())
}

/// [`build_mdbx`] with `rows`, written through MDBX's raw tables (so a dup-sort table takes any
/// values MDBX stores), and checked to read back from MDBX as written.
fn build_mdbx_with(
    path: &Path,
    rows: BTreeMap<&'static str, Rows>,
) -> BTreeMap<&'static str, Rows> {
    let args = DatabaseArgs { page_size: Some(MDBX_PAGE_SIZE), ..temp_chain_db_args() };
    let db = reth_db::create_db(path, args.database_args()).expect("create MDBX");
    let txn = db.begin_rw_txn().expect("begin");
    for table in Tables::ALL.iter().filter(|table| table.name() != MISSING) {
        let flags =
            if table.is_dupsort() { DatabaseFlags::DUP_SORT } else { DatabaseFlags::default() };
        txn.create_db(Some(table.name()), flags).expect("create table");
    }
    txn.commit().expect("commit tables");
    let tx = db.tx_mut().expect("tx_mut");
    for table in Tables::ALL {
        table.view(&PutRows { tx: &tx, rows: &rows }).expect("put");
    }
    tx.commit().expect("commit rows");

    let written = raw_rows(&db);
    let tx = db.tx().expect("tx");
    let missing = tx.entries::<RawTable<tables::PruneCheckpoints>>().expect_err("never created");
    assert!(matches!(missing, DatabaseError::Open(_)), "{missing:?}");
    assert_eq!(written, rows_with_every_table(&rows), "MDBX holds the rows written");
    written
}

/// `rows` with an empty entry for every one of reth's tables it lacks.
fn rows_with_every_table(rows: &BTreeMap<&'static str, Rows>) -> BTreeMap<&'static str, Rows> {
    Tables::ALL
        .iter()
        .map(|table| (table.name(), rows.get(table.name()).cloned().unwrap_or_default()))
        .collect()
}

/// Whether `file` exists.
fn exists(file: &Path) -> bool {
    file.try_exists().expect("stat")
}

/// Open the database in `path` as the node does, recording client version 2.0.0, which differs
/// from the copied `VersionHistory` rows (so the first open adds a row, which the tests drop
/// before comparing).
fn open(path: &Path) -> eyre::Result<RethDb> {
    open_tndb(path, &tndb_db_args(), version("2.0.0"))
}

/// The test covers every dup-sort table reth has: a new one needs a row in [`DUP_TABLES`] (and a
/// layout in the adapter).
#[test]
fn the_tests_cover_every_dup_sort_table() {
    let dup_sort: Vec<_> =
        Tables::ALL.iter().filter(|table| table.is_dupsort()).map(Tables::name).collect();
    let covered: Vec<_> = DUP_TABLES.iter().map(|(name, ..)| *name).collect();
    assert_eq!(dup_sort, covered);
}

/// An MDBX database moves onto tndb-b at the first open: every table's raw rows are the same, row
/// for row (a table MDBX lacks is empty), the file has the configured page size, MDBX's files and
/// the temp directory are gone, the marker lists the rows copied, and reth's version file is
/// kept. The client version is recorded after the copied `VersionHistory` rows. A second open is
/// a normal open.
#[test]
fn an_mdbx_database_migrates_row_for_row() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let mut expected = build_mdbx(&path);
    let version_file = reth_db::version::db_version_file_path(&path);
    let version_bytes = std::fs::read(&version_file).expect("reth's version file");

    let db = open(&path).expect("the open migrates");
    assert_eq!(db.page_size(), TNDB_PAGE_SIZE as u32, "the configured page size");
    let mut migrated = raw_rows(&*db);
    let history = migrated.get_mut(tables::VersionHistory::NAME).expect("history");
    let (_, recorded) = history.pop().expect("the client version is recorded");
    use reth_db::table::Decompress as _;
    assert_eq!(ClientVersion::decompress(&recorded).expect("decode"), version("2.0.0"));
    assert_eq!(migrated, expected, "every table, row for row");
    let tx = db.tx().expect("tx");
    for table in [EMPTY, MISSING] {
        assert!(migrated[table].is_empty(), "{table} has no rows");
    }
    assert_eq!(tx.entries::<RawTable<tables::PruneCheckpoints>>().expect("created"), 0);
    drop(tx);

    for gone in [MDBX_DATA_FILE, MDBX_LOCK_FILE, MIGRATING_DIR] {
        assert!(!exists(&path.join(gone)), "{gone} is removed");
    }
    assert_eq!(std::fs::read(&version_file).expect("read"), version_bytes, "version file kept");
    let marker = std::fs::read_to_string(path.join(MIGRATED_MARKER)).expect("the marker");
    let total: usize = expected.values().map(Vec::len).sum();
    assert!(
        marker.starts_with(&format!("migrated from MDBX to tndb-b: {total} rows\n")),
        "{marker}"
    );
    assert!(marker.contains(&format!("{} 3000\n", tables::CanonicalHeaders::NAME)), "{marker}");

    // A second open is a normal one: the rows written since stay, nothing is copied again.
    let tx = db.tx_mut().expect("tx_mut");
    let (key, value) = (3000u64.to_be_bytes().to_vec(), vec![7; 32]);
    tx.put::<RawTable<tables::CanonicalHeaders>>(
        RawKey::from_vec(key.clone()),
        RawValue::from_vec(value.clone()),
    )
    .expect("put");
    tx.commit().expect("commit");
    drop(db);
    let db = open(&path).expect("reopen");
    expected.get_mut(tables::CanonicalHeaders::NAME).expect("headers").push((key, value));
    let mut reopened = raw_rows(&*db);
    reopened.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert_eq!(reopened, expected, "the rows of the first open, and the row written since");
    assert_eq!(std::fs::read_to_string(path.join(MIGRATED_MARKER)).expect("marker"), marker);
}

/// A dup key whose rows outnumber a commit's ([`COMMIT_ROWS`]) migrates whole: the copy commits
/// inside the key ([`BIG_KEYS`]), and the next write transaction appends the key's further rows
/// to those committed, on the adapter's nested tables (`PlainStorageState`, `HashedStorages`,
/// `StoragesTrie`) and on a composite one (`StorageChangeSets`). The migration's own verification
/// passes, and the open after it checks each table's form against its layout. Every table holds
/// MDBX's rows, row for row, and counts them (`entries`); each big key reads back whole by
/// `walk_dup` and row by row by `seek_by_key_subkey`.
#[test]
fn a_dup_key_with_more_rows_than_a_commit_migrates_whole() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx_with(&path, rows_with_big_dup_keys());

    let db = open(&path).expect("the open migrates");
    let mut migrated = raw_rows(&*db);
    migrated.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert!(migrated == expected, "every table, row for row");
    let tx = db.tx().expect("tx");
    read_back_big_key::<tables::PlainStorageState>(&tx, &expected);
    read_back_big_key::<tables::StorageChangeSets>(&tx, &expected);
    read_back_big_key::<tables::HashedStorages>(&tx, &expected);
    read_back_big_key::<tables::StoragesTrie>(&tx, &expected);
}

/// Check table `T` in `tx` against `expected`: its row count, and its [`big_key`]'s rows, read
/// whole by `walk_dup` and one by one by `seek_by_key_subkey`.
fn read_back_big_key<T: DupSort>(tx: &impl DbTx, expected: &BTreeMap<&'static str, Rows>) {
    let name = T::NAME;
    let rows = &expected[name];
    assert_eq!(tx.entries::<RawDupSort<T>>().expect("entries"), rows.len(), "{name}: entries");
    let (_, key_len, subkey_len) =
        DUP_TABLES.into_iter().find(|(table, ..)| *table == name).expect("a dup-sort table");
    let key = big_key(key_len);
    let big: Rows = rows.iter().filter(|(k, _)| *k == key).cloned().collect();
    assert!(big.len() as u64 > COMMIT_ROWS, "{name}: the big key's rows span a commit");

    let mut cursor = tx.cursor_dup_read::<RawDupSort<T>>().expect("cursor");
    let walked: Rows = cursor
        .walk_dup(Some(RawKey::from_vec(key.clone())), None)
        .expect("walk_dup")
        .map(|row| row.map(|(k, v)| (k.into_key(), v.into_value())))
        .collect::<Result<_, _>>()
        .expect("the key's rows");
    assert!(
        walked == big,
        "{name}: walk_dup reads {} of the big key's {} rows",
        walked.len(),
        big.len()
    );
    for (n, (_, value)) in big.iter().enumerate() {
        let subkey = RawKey::from_vec(value[..subkey_len].to_vec());
        let found = cursor.seek_by_key_subkey(RawKey::from_vec(key.clone()), subkey).expect("seek");
        assert_eq!(found.map(RawValue::into_value).as_ref(), Some(value), "{name}: row {n}");
    }
}

/// Stop a migration of [`build_mdbx`]'s database at `stop`, after letting `passes` earlier passes
/// of that step by, as a kill would stop it (no cleanup), and check what the stop left with
/// `left`. Then open again: the open completes the migration, with every row as MDBX held it,
/// MDBX's files and the temp directory gone and the marker written, and a further open is a normal
/// one, which changes neither. Returns the marker's text.
fn stop_then_reopen(stop: Step, passes: u32, left: impl FnOnce(&Path)) -> String {
    stop_then_reopen_with(mdbx_rows(), stop, passes, left)
}

/// [`stop_then_reopen`] of a migration of an MDBX database holding `rows`.
fn stop_then_reopen_with(
    rows: BTreeMap<&'static str, Rows>,
    stop: Step,
    passes: u32,
    left: impl FnOnce(&Path),
) -> String {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx_with(&path, rows);

    STOP_AT.set(Some(stop));
    STOP_PASSES.set(passes);
    let err = open(&path).expect_err("the migration is stopped");
    assert!(matches!(err.downcast_ref::<Killed>(), Some(Killed(at)) if *at == stop), "{err:#}");
    left(&path);

    let db = open(&path).unwrap_or_else(|e| panic!("{stop:?}: the next open completes: {e:#}"));
    let mut rows = raw_rows(&*db);
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert!(rows == expected, "{stop:?}: every row, as MDBX held it");
    for gone in [MDBX_DATA_FILE, MDBX_LOCK_FILE, MIGRATING_DIR] {
        assert!(!exists(&path.join(gone)), "{stop:?}: {gone} is removed");
    }
    let marker = std::fs::read_to_string(path.join(MIGRATED_MARKER))
        .unwrap_or_else(|e| panic!("{stop:?}: the marker is written: {e}"));
    drop(db);

    let db = open(&path).unwrap_or_else(|e| panic!("{stop:?}: a further open: {e:#}"));
    let mut rows = raw_rows(&*db);
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert!(rows == expected, "{stop:?}: a further open keeps the rows");
    let again = std::fs::read_to_string(path.join(MIGRATED_MARKER)).expect("marker");
    assert_eq!(again, marker, "{stop:?}: a further open leaves the marker");
    assert!(!exists(&path.join(MIGRATING_DIR)), "{stop:?}: no temp directory");
    marker
}

/// The rows of every one of reth's tables in a stopped migration's temp file in `path`.
fn temp_rows(path: &Path) -> BTreeMap<&'static str, Rows> {
    let temp = path.join(MIGRATING_DIR);
    raw_rows(&TndbEnv::open(&temp, TndbEnvKind::RO, TndbArgs::default()).expect("the temp file"))
}

/// Assert the files a stop before the commit point leaves in `path`: MDBX's, the temp directory's
/// `tndb.dat`, and no `tndb.dat` or marker beside MDBX's.
fn assert_stopped_before_the_commit_point(path: &Path) {
    assert!(exists(&path.join(MDBX_DATA_FILE)), "mdbx.dat is left");
    assert!(exists(&path.join(MIGRATING_DIR).join(DATA_FILE)), "the temp file is left");
    for absent in [DATA_FILE, MIGRATED_MARKER] {
        assert!(!exists(&path.join(absent)), "no {absent}");
    }
}

/// A migration stopped right after a bounded commit in the middle of a table, its temp file
/// holding part of the table, migrates afresh at the next open; so does one stopped inside a dup
/// key of a nested table (`PlainStorageState`'s [`big_key`]), its temp file holding part of the
/// key's rows.
#[test]
fn a_migration_stopped_inside_a_table_migrates_again_at_the_next_open() {
    stop_then_reopen(Step::MidCopy, 0, |path| {
        assert_stopped_before_the_commit_point(path);
        let partial = temp_rows(path).remove(tables::CanonicalHeaders::NAME).expect("headers");
        assert_eq!(partial.len() as u64, COMMIT_ROWS, "one commit of the first table's 3000 rows");
    });

    let rows = rows_with_big_dup_keys();
    let name = tables::PlainStorageState::NAME;
    let all = rows[name].clone();
    let passes = mid_copy_passes_before(&rows, name);
    stop_then_reopen_with(rows, Step::MidCopy, passes, |path| {
        assert_stopped_before_the_commit_point(path);
        let partial = temp_rows(path).remove(name).expect("PlainStorageState");
        let committed = COMMIT_ROWS as usize;
        assert!(partial[..] == all[..committed], "the table's first commit, as MDBX holds it");
        let key = big_key(20);
        assert!(partial[committed - 1].0 == key && all[committed].0 == key, "inside the big key");
    });
}

/// A migration stopped after some tables' copies are committed migrates afresh at the next open.
#[test]
fn a_migration_stopped_between_tables_migrates_again_at_the_next_open() {
    // The passes: three bounded commits and the last of `CanonicalHeaders`, then one per table
    // up to `PlainStorageState`.
    stop_then_reopen(Step::MidCopy, 16, |path| {
        assert_stopped_before_the_commit_point(path);
        let copied = temp_rows(path);
        let expected = mdbx_rows();
        for done in [tables::CanonicalHeaders::NAME, tables::Bytecodes::NAME] {
            assert_eq!(copied[done], expected[done], "{done} is copied");
        }
        assert!(copied[tables::StoragesTrie::NAME].is_empty(), "StoragesTrie is not yet copied");
    });
}

/// A migration stopped once its copy is verified and durable, before the rename (its commit
/// point), migrates afresh at the next open.
#[test]
fn a_migration_stopped_before_its_commit_point_migrates_again_at_the_next_open() {
    stop_then_reopen(Step::BeforeRename, 0, assert_stopped_before_the_commit_point);
}

/// A migration stopped after its commit point (`tndb.dat` in place beside MDBX's files, no
/// marker) is finished by the next open: MDBX's files and the temp directory are removed, the
/// marker is written, and the migrated rows are intact.
#[test]
fn a_migration_stopped_after_its_commit_point_is_finished() {
    let marker = stop_then_reopen(Step::AfterRename, 0, |path| {
        for present in [DATA_FILE, MDBX_DATA_FILE, MIGRATING_DIR] {
            assert!(exists(&path.join(present)), "{present} is left as a kill leaves it");
        }
        assert!(!exists(&path.join(MIGRATED_MARKER)));
    });
    assert!(marker.contains("finished by a later open"), "{marker}");
}

/// A migration stopped after it removed MDBX's files, before it wrote the marker, is finished by
/// the next open, which writes the marker: the emptied temp directory beside `tndb.dat` says the
/// migration is unfinished.
#[test]
fn a_migration_stopped_before_its_marker_is_finished() {
    let marker = stop_then_reopen(Step::BeforeMarker, 0, |path| {
        assert!(exists(&path.join(DATA_FILE)), "tndb.dat is in place");
        assert!(exists(&path.join(MIGRATING_DIR)), "the temp directory is kept");
        assert!(!exists(&path.join(MIGRATING_DIR).join(DATA_FILE)), "emptied by the rename");
        for absent in [MDBX_DATA_FILE, MDBX_LOCK_FILE, MIGRATED_MARKER] {
            assert!(!exists(&path.join(absent)), "no {absent}");
        }
    });
    assert!(marker.contains("finished by a later open"), "{marker}");
}

/// An `mdbx.dat` beside a finished migration's marker was written after the migration: the open
/// refuses it, naming both files, and changes nothing. `db stats` refuses it the same way.
#[test]
fn mdbx_written_after_a_migration_is_refused() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx(&path);
    drop(open(&path).expect("migrate"));
    std::fs::write(path.join(MDBX_DATA_FILE), b"written by an older binary").expect("plant");
    let marker = std::fs::read(path.join(MIGRATED_MARKER)).expect("marker");
    let tndb = std::fs::read(path.join(DATA_FILE)).expect("tndb.dat");

    let refused = |err: eyre::Report| {
        let msg = format!("{err:#}");
        assert!(msg.contains("was written since"), "says why: {msg}");
        assert!(msg.contains(&path.join(MIGRATED_MARKER).display().to_string()), "{msg}");
        assert!(msg.contains(&path.join(MDBX_DATA_FILE).display().to_string()), "{msg}");
    };
    refused(open(&path).expect_err("MDBX written after the migration"));
    refused(db_table_stats(&path).expect_err("the stats refuse it too"));

    let mdbx = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");
    assert_eq!(mdbx, b"written by an older binary", "mdbx.dat unchanged");
    assert_eq!(std::fs::read(path.join(MIGRATED_MARKER)).expect("marker"), marker);
    assert_eq!(std::fs::read(path.join(DATA_FILE)).expect("tndb.dat"), tndb, "tndb.dat unchanged");
    std::fs::remove_file(path.join(MDBX_DATA_FILE)).expect("resolved by hand");
    let mut rows = raw_rows(&*open(&path).expect("open once resolved"));
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert_eq!(rows, expected);
}

/// A migrated database whose `tndb.dat` is missing (the marker alone) is refused, and nothing is
/// created in its place beside the static files.
#[test]
fn a_migrated_database_without_its_file_is_refused() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    std::fs::create_dir(&path).expect("db dir");
    std::fs::write(path.join(MIGRATED_MARKER), b"migrated").expect("marker");

    let err = open(&path).expect_err("the marker without its database");
    let msg = format!("{err:#}");
    assert!(msg.contains("is missing") && msg.contains(DATA_FILE), "{msg}");
    assert!(!exists(&path.join(DATA_FILE)), "no database is created in its place");
}

/// A stopped migration's temp directory found beside a tndb-b database, still holding a copy (so
/// not one emptied by a migration's rename), is removed by its open, which keeps the database's
/// rows and writes no marker.
#[test]
fn a_stale_temp_directory_beside_tndb_is_removed() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let db = open(&path).expect("a new database");
    let tx = db.tx_mut().expect("tx_mut");
    tx.put::<tables::CanonicalHeaders>(7, Default::default()).expect("put");
    tx.commit().expect("commit");
    drop(db);
    let temp = path.join(MIGRATING_DIR);
    std::fs::create_dir(&temp).expect("temp dir");
    std::fs::write(temp.join(DATA_FILE), b"a stopped copy").expect("stale file");

    let db = open(&path).expect("reopen");
    assert!(!exists(&temp), "the stale temp directory is removed");
    assert!(!exists(&path.join(MIGRATED_MARKER)), "no migration is finished");
    assert_eq!(db.tx().expect("tx").entries::<tables::CanonicalHeaders>().expect("entries"), 1);
}

/// A stopped migration's temp directory found before a migration is replaced: the migration
/// builds its file afresh, so a stale `tndb.dat` there (here not a tndb-b file at all) is never
/// opened.
#[test]
fn a_stale_temp_directory_before_a_migration_is_replaced() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx(&path);
    let temp = path.join(MIGRATING_DIR);
    std::fs::create_dir(&temp).expect("temp dir");
    std::fs::write(temp.join(DATA_FILE), b"a stopped copy").expect("stale file");
    std::fs::write(temp.join("leftover"), b"anything").expect("stale file");

    let db = open(&path).expect("the open migrates");
    assert!(!exists(&temp), "the temp directory is gone");
    let mut rows = raw_rows(&*db);
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert_eq!(rows, expected);
}

/// The copy is verified before the commit point: a row lost on the way (a short read) fails the
/// count against MDBX's, and a row changed on the way fails the read-back hash. Either failure
/// removes the temp directory and leaves MDBX as it was, and the next open migrates.
#[test]
fn the_copy_is_verified_before_the_commit_point() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx(&path);
    let mdbx = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");

    for (fault, says) in [
        (CopyFault::SkipRow(tables::CanonicalHeaders::NAME), "(a short read)"),
        (CopyFault::FlipValue(tables::Bytecodes::NAME), "reads back"),
        (CopyFault::FlipValue(tables::StoragesTrie::NAME), "reads back"),
    ] {
        COPY_FAULT.set(Some(fault));
        let err = open(&path).expect_err("the fault is caught");
        assert_eq!(COPY_FAULT.get(), None, "{fault:?} was made");
        let msg = format!("{err:#}");
        assert!(msg.contains(says) && msg.contains("MDBX database is unchanged"), "{msg}");
        for gone in [DATA_FILE, MIGRATING_DIR, MIGRATED_MARKER] {
            assert!(!exists(&path.join(gone)), "{fault:?}: no {gone}");
        }
        let now = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");
        assert!(now == mdbx, "{fault:?}: mdbx.dat unchanged");
    }

    let mut rows = raw_rows(&*open(&path).expect("the next open migrates"));
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert_eq!(rows, expected);
}

/// Assert that a refused migration left the database in `path` as it found it: `mdbx.dat` holds
/// `mdbx`, and there is no `tndb.dat`, temp directory or marker.
fn assert_untouched(path: &Path, mdbx: &[u8], case: &str) {
    let now = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");
    assert!(now == mdbx, "{case}: mdbx.dat unchanged");
    for gone in [DATA_FILE, MIGRATING_DIR, MIGRATED_MARKER] {
        assert!(!exists(&path.join(gone)), "{case}: no {gone}");
    }
}

/// The MDBX error code of the reth database write error at the root of `err`, if there is one.
fn mdbx_code(err: &eyre::Report) -> Option<i32> {
    err.chain().find_map(|e| match e.downcast_ref::<DatabaseError>()? {
        DatabaseError::Write(write) => Some(write.info.code),
        _ => None,
    })
}

/// The free space a migration needs: the live bytes of reth's tables in MDBX, a quarter of them,
/// and 1 GiB.
#[test]
fn needed_bytes_is_the_live_bytes_plus_a_quarter_and_a_gib() {
    const GIB: u64 = 1 << 30;
    assert_eq!(needed_bytes(0), GIB);
    assert_eq!(needed_bytes(1000), 1250 + GIB);
    assert_eq!(needed_bytes(4 * GIB), 6 * GIB);
    assert_eq!(needed_bytes(u64::MAX), u64::MAX, "saturates");
}

/// The live bytes of reth's tables in the MDBX database in `path`: each table's branch, leaf and
/// overflow pages times the page size, from MDBX's statistics of it.
fn mdbx_live(path: &Path) -> u64 {
    let db = reth_db::mdbx::open_db_read_only(path, DatabaseArguments::default()).expect("MDBX");
    let tx = db.tx().expect("tx");
    let tables = Tables::ALL.iter().filter(|table| table.name() != MISSING);
    tables
        .map(|table| {
            let dbi = tx.inner().open_db(Some(table.name())).expect("table").dbi();
            let stat = tx.inner().db_stat(dbi).expect("stat");
            let pages = stat.branch_pages() + stat.leaf_pages() + stat.overflow_pages();
            (stat.page_size() as usize * pages) as u64
        })
        .sum()
}

/// A migration refuses to start with less free space than it needs ([`needed_bytes`] of the live
/// bytes of reth's tables in MDBX, not of `mdbx.dat`'s size), naming the live bytes and the bytes
/// needed and free, and creates nothing; with exactly that much free it runs.
#[test]
fn a_migration_without_the_free_space_it_needs_is_refused() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx(&path);
    let mdbx = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");
    let live = mdbx_live(&path);
    let file = mdbx.len() as u64;
    // MDBX's file holds more than its tables' live pages, so a check sized on the file would
    // refuse the run below that has exactly the free space the live bytes need.
    assert!(0 < live && live < file, "live {live} bytes, file {file} bytes");
    let needed = needed_bytes(live);
    assert!(needed < needed_bytes(file));

    FREE_BYTES.set(Some(needed - 1));
    let err = open(&path).expect_err("too little free space");
    let msg = format!("{err:#}");
    let says = [
        format!("needs {needed} bytes free"),
        format!("take {live} bytes of pages"),
        format!("but {} are free", needed - 1),
    ];
    assert!(says.iter().all(|says| msg.contains(says.as_str())), "{msg}");
    assert_untouched(&path, &mdbx, "too little free space");

    FREE_BYTES.set(Some(needed));
    let mut rows = raw_rows(&*open(&path).expect("exactly the free space needed"));
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert_eq!(rows, expected);
}

/// A named database in MDBX that is not one of reth's tables is listed ([`foreign_tables`], which
/// the migration warns of) and not copied: the migration runs and moves reth's tables row for row.
#[test]
fn a_table_that_is_not_reths_is_listed_and_not_copied() {
    const FOREIGN: &str = "NotARethTable";
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let expected = build_mdbx(&path);
    {
        // `create_db` opens the existing database without recording a client version, as
        // `open_db` would: that row's key is the current second, so whether the migrating open's
        // row replaced it would depend on the clock
        let args = DatabaseArgs { page_size: Some(MDBX_PAGE_SIZE), ..temp_chain_db_args() };
        let db = reth_db::create_db(&path, args.database_args()).expect("open MDBX");
        let txn = db.begin_rw_txn().expect("begin");
        let table = txn.create_db(Some(FOREIGN), DatabaseFlags::default()).expect("create");
        txn.put(table.dbi(), b"key", b"value", Default::default()).expect("put");
        txn.commit().expect("commit");
        assert_eq!(
            raw_rows(&db),
            expected,
            "adding the foreign table leaves reth's tables as built"
        );
    }
    {
        let db =
            reth_db::mdbx::open_db_read_only(&path, DatabaseArguments::default()).expect("MDBX");
        let tx = db.tx().expect("tx");
        assert_eq!(foreign_tables(&tx).expect("foreign tables"), [FOREIGN]);
    }

    let mut rows = raw_rows(&*open(&path).expect("migrates"));
    rows.get_mut(tables::VersionHistory::NAME).expect("history").pop();
    assert_eq!(rows, expected);
}

/// MDBX keeps any distinct values of a dup-sort key, so it can hold two that share a subkey (the
/// value's first W bytes; here `PlainStorageState`'s 32-byte slot). tndb-b-reth stores one value
/// per subkey, and refuses the second with `MDBX_KEYEXIST`: the migration fails with that refusal,
/// and leaves MDBX untouched.
#[test]
fn a_second_value_for_a_dup_subkey_fails_the_migration() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let mut rows = mdbx_rows();
    let table = rows.get_mut(tables::PlainStorageState::NAME).expect("table");
    let (key, mut value) = table[0].clone();
    *value.last_mut().expect("a value past its subkey") ^= 0xff;
    table.push((key, value));
    table.sort();
    // `build_mdbx_with` checks that MDBX holds both values.
    build_mdbx_with(&path, rows);
    let mdbx = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");

    let err = open(&path).expect_err("a second value for a subkey");
    let msg = format!("{err:#}");
    assert_eq!(mdbx_code(&err), Some(-30799), "MDBX_KEYEXIST: {msg}");
    assert!(msg.contains("stores one value per subkey"), "{msg}");
    assert!(msg.contains(tables::PlainStorageState::NAME), "{msg}");
    assert!(msg.contains("MDBX database is unchanged"), "{msg}");
    assert_untouched(&path, &mdbx, "a second value for a subkey");
}

/// A value larger than tndb-b takes (16 MiB with a short key, the engine's limit; MDBX takes
/// larger) fails the migration with the adapter's `MDBX_BAD_VALSIZE`, and leaves MDBX untouched.
#[test]
fn a_value_too_large_for_tndb_fails_the_migration() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let mut rows = mdbx_rows();
    let codes = rows.get_mut(tables::Bytecodes::NAME).expect("table");
    codes.push((vec![0xff; 32], vec![7; (16 << 20) + 1]));
    build_mdbx_with(&path, rows);
    let mdbx = std::fs::read(path.join(MDBX_DATA_FILE)).expect("mdbx.dat");

    let err = open(&path).expect_err("a value too large");
    let msg = format!("{err:#}");
    assert_eq!(mdbx_code(&err), Some(-30781), "MDBX_BAD_VALSIZE: {msg}");
    assert!(msg.contains(tables::Bytecodes::NAME), "{msg}");
    assert!(msg.contains("MDBX database is unchanged"), "{msg}");
    assert_untouched(&path, &mdbx, "a value too large");
}

/// A table's rows and the hash of its raw rows in order ([`hash_row`]).
#[derive(Debug, PartialEq, Eq)]
struct Digest {
    rows: u64,
    hash: u64,
}

/// The [`TableViewer`] that digests a table's first `limit` raw rows (every row without one); a
/// table the database lacks has none.
struct DigestTable<'a, TX> {
    tx: &'a TX,
    limit: Option<u64>,
}

impl<TX: DbTx> TableViewer<Digest> for DigestTable<'_, TX> {
    type Error = DatabaseError;

    fn view<T: Table>(&self) -> Result<Digest, DatabaseError> {
        let mut hasher = DefaultHasher::new();
        let mut rows = 0;
        let mut cursor = match self.tx.cursor_read::<RawTable<T>>() {
            Ok(cursor) => cursor,
            Err(DatabaseError::Open(info)) if info.code == MdbxError::NotFound.to_err_code() => {
                return Ok(Digest { rows, hash: hasher.finish() });
            }
            Err(e) => return Err(e),
        };
        for row in cursor.walk(None)? {
            if self.limit == Some(rows) {
                break;
            }
            let (key, value) = row?;
            hash_row(&mut hasher, key.raw_key(), value.raw_value());
            rows += 1;
        }
        Ok(Digest { rows, hash: hasher.finish() })
    }
}

/// On demand: migrate a copy of a real reth database through `open_rw` with the default `--db.*`
/// settings, and check every table's rows and their hash against the copy's MDBX. It prints the
/// live bytes of the copy's tables and the free space the migration needs ([`needed_bytes`]), then
/// the time, the sizes of `mdbx.dat` and `tndb.dat`, and each table's rows.
///
/// `TN_RETH_MIGRATE_SRC` names a reth database directory holding `mdbx.dat` (a stopped node's
/// `<datadir>/db`). The test copies its `mdbx.dat` and `database.version` into a temp dir and
/// migrates the copy; it never opens the source. Unset, the test prints a note and passes. Set
/// `TMPDIR` to a directory on a real disk with room for the copy and the migration, for the
/// command only (the test prints the temp dir it uses):
///
/// ```text
/// TN_RETH_MIGRATE_SRC=/path/to/datadir/db TMPDIR=/path/on/a/real/disk \
///   cargo nextest run -p tn-reth --features tndb-b --run-ignored only --no-capture \
///   -E 'test(migrates_a_copy_of_a_real_database)'
/// ```
///
/// Add `--release` to time a large database as a release build would.
#[test]
#[ignore = "on demand: TN_RETH_MIGRATE_SRC names a reth database directory to migrate a copy of"]
fn migrates_a_copy_of_a_real_database() {
    let Some(src) = std::env::var_os("TN_RETH_MIGRATE_SRC").map(std::path::PathBuf::from) else {
        println!("TN_RETH_MIGRATE_SRC is not set: no database to migrate, nothing run");
        return;
    };
    assert!(exists(&src.join(MDBX_DATA_FILE)), "{} holds no {MDBX_DATA_FILE}", src.display());
    assert!(!exists(&src.join(DATA_FILE)), "{} holds {DATA_FILE}: not MDBX", src.display());
    let tmp = TempDir::new().expect("tempdir");
    println!("temp dir: {} (from TMPDIR)", tmp.path().display());
    let path = tmp.path().join("db");
    std::fs::create_dir(&path).expect("db dir");
    // The database's files, not MDBX's lock file or reth's: a lock file is a running node's.
    std::fs::copy(src.join(MDBX_DATA_FILE), path.join(MDBX_DATA_FILE)).expect("copy mdbx.dat");
    let version_file = reth_db::version::db_version_file_path(&src);
    std::fs::copy(&version_file, reth_db::version::db_version_file_path(&path))
        .unwrap_or_else(|e| panic!("copy {}: {e}", version_file.display()));
    let mdbx_len = std::fs::metadata(path.join(MDBX_DATA_FILE)).expect("mdbx.dat").len();

    let (before, live): (Vec<Digest>, u64) = {
        let mdbx = reth_db::mdbx::open_db_read_only(&path, DatabaseArguments::default())
            .expect("open the copy's MDBX");
        let live = mdbx_live_bytes(&mdbx).expect("live bytes");
        let tx = mdbx.tx().expect("tx");
        let viewer = DigestTable { tx: &tx, limit: None };
        (Tables::ALL.iter().map(|table| table.view(&viewer).expect("digest")).collect(), live)
    };
    println!(
        "mdbx.dat {mdbx_len} bytes, its tables' live pages {live} bytes: the migration needs {} \
         bytes free",
        needed_bytes(live)
    );

    crate::init_reth_defaults();
    let chain = std::sync::Arc::new(tn_types::test_genesis().into());
    let config = crate::RethConfig(reth_node_builder::NodeConfig {
        chain,
        db: DatabaseArgs::default(),
        ..reth_node_builder::NodeConfig::default()
    });
    let started = Instant::now();
    let db = crate::env::database::open_rw(&config, &path).expect("the open migrates");
    let elapsed = started.elapsed();

    let tx = db.tx().expect("tx");
    for (table, mdbx) in Tables::ALL.iter().zip(&before) {
        let mut limit = None;
        if *table == Tables::VersionHistory {
            // The open records its client version after the copied rows: at most one row more.
            let rows = tx.entries::<RawTable<tables::VersionHistory>>().expect("entries") as u64;
            assert!((mdbx.rows..=mdbx.rows + 1).contains(&rows), "VersionHistory: {rows} rows");
            limit = Some(mdbx.rows);
        }
        let tndb = table.view(&DigestTable { tx: &tx, limit }).expect("digest");
        assert_eq!(&tndb, mdbx, "table {}: the migrated rows", table.name());
        println!("{:<32} {:>14} rows", table.name(), mdbx.rows);
    }
    drop(tx);
    assert!(exists(&path.join(MIGRATED_MARKER)) && !exists(&path.join(MDBX_DATA_FILE)));
    let tndb_len = std::fs::metadata(path.join(DATA_FILE)).expect("tndb.dat").len();
    let rows: u64 = before.iter().map(|digest| digest.rows).sum();
    println!(
        "migrated {rows} rows in {elapsed:?}: mdbx.dat {mdbx_len} bytes, tndb.dat {tndb_len} bytes"
    );
}
