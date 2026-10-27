//! Tests for the backend switch: the backend guard, the table stats of `db stats`, and on the
//! tndb build the `--db.*` mapping, the database version file and the client version record.

use super::*;
use reth_db::{
    table::Table as _,
    tables::{self, Tables},
    transaction::{DbTx as _, DbTxMut as _},
    Database as _,
};
use reth_node_builder::NodeConfig;
use reth_primitives_traits::{Bytecode, StorageEntry};
use std::sync::Arc;
use tempfile::TempDir;
use tn_types::{test_genesis, Address, Bytes, B256, U256};

/// `--db.*` settings for a test database: a temp chain's, which this build accepts.
fn test_db_args() -> DatabaseArgs {
    temp_chain_db_args()
}

/// A [`RethConfig`] with `db` as its `--db.*` settings.
fn config(db: DatabaseArgs) -> RethConfig {
    crate::init_reth_defaults();
    let chain = Arc::new(test_genesis().into());
    RethConfig(NodeConfig { chain, db, ..NodeConfig::default() })
}

/// This build's data file, and the other build's.
#[cfg(not(feature = "tndb-b"))]
const OWN_AND_OTHER_DATA_FILE: (&str, &str) = ("mdbx.dat", TNDB_DATA_FILE);
#[cfg(feature = "tndb-b")]
const OWN_AND_OTHER_DATA_FILE: (&str, &str) = (tndb_b_reth::DATA_FILE, MDBX_DATA_FILE);

/// A database directory holding the other engine's data file is refused before anything is
/// created in it, and the error names the directory and both engines. On the tndb build the
/// planted `mdbx.dat` goes to the migration, which fails to open it as MDBX and leaves it as it
/// was, with no temp directory left behind.
#[test]
fn open_rw_refuses_the_other_engines_database() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    std::fs::create_dir(&path).expect("db dir");
    let (own, other) = OWN_AND_OTHER_DATA_FILE;
    std::fs::write(path.join(other), b"planted").expect("plant the other engine's file");

    let err = open_rw(&config(test_db_args()), &path).expect_err("the other engine's file");
    let msg = format!("{err:#}");
    assert!(msg.contains(&path.display().to_string()), "names the directory: {msg}");
    assert!(msg.contains("MDBX") && msg.contains("tndb-b"), "names both engines: {msg}");
    #[cfg(not(feature = "tndb-b"))]
    assert!(msg.contains("fresh datadir"), "says what the operator can do: {msg}");
    #[cfg(feature = "tndb-b")]
    {
        assert!(msg.contains("migration") && msg.contains("unchanged"), "says why: {msg}");
        assert_eq!(std::fs::read(path.join(other)).expect("read"), b"planted", "MDBX unchanged");
        let temp = path.join(migrate::MIGRATING_DIR).try_exists().expect("stat");
        assert!(!temp, "the failed migration's temp directory is removed");
    }

    let own_exists = path.join(own).try_exists().expect("stat");
    assert!(!own_exists, "nothing is created beside the other engine's file");
    let version_exists = reth_db::version::db_version_file_path(&path).try_exists().expect("stat");
    assert!(!version_exists, "no version file is written beside the other engine's file");
}

/// A stat error while looking for the other engine's file fails the guard: it is never read as
/// an absent file. A path under a regular file stats with `ENOTDIR` on macOS and Linux.
#[test]
fn check_backend_fails_on_a_stat_error() {
    let tmp = TempDir::new().expect("tempdir");
    let file = tmp.path().join("file");
    std::fs::write(&file, b"not a directory").expect("file");
    let path = file.join("db");

    #[cfg(not(feature = "tndb-b"))]
    let checked = check_backend(&path);
    #[cfg(feature = "tndb-b")]
    let checked = check_backend(&path, tndb_b_reth::TndbArgs::default());
    let err = checked.expect_err("a stat error fails the guard");
    let msg = format!("{err:#}");
    assert!(msg.contains("failed to check for"), "the guard's own error: {msg}");
    assert!(msg.contains(OWN_AND_OTHER_DATA_FILE.1), "names the file it checked: {msg}");
}

/// A fresh directory opens on this build's engine, and the database reopens once dropped.
#[test]
fn open_rw_creates_and_reopens_this_engines_database() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let config = config(test_db_args());

    drop(open_rw(&config, &path).expect("fresh open"));
    let own_exists = path.join(OWN_AND_OTHER_DATA_FILE.0).try_exists().expect("stat");
    assert!(own_exists, "this engine's data file");
    reth_db::version::check_db_version_file(&path).expect("reth's version file");

    drop(open_rw(&config, &path).expect("reopen"));
    let other_exists = path.join(OWN_AND_OTHER_DATA_FILE.1).try_exists().expect("stat");
    assert!(!other_exists, "never the other engine's file");
}

/// The page size of the table stats tests' databases, which both engines accept, so each table's
/// bytes are known from its pages.
const STATS_PAGE_SIZE: usize = 4096;

/// Create a database in `path` through [`open_rw`], write rows the table stats tests count, and
/// drop the handle: 2000 plain `CanonicalHeaders` rows (enough for a branch page), one `Bytecodes`
/// value in an overflow run, and five values under two keys of the dup-sort `PlainStorageState`.
fn write_stats_rows(path: &Path) {
    let db_args = DatabaseArgs { page_size: Some(STATS_PAGE_SIZE), ..test_db_args() };
    let db = open_rw(&config(db_args), path).expect("open");
    let tx = db.tx_mut().expect("tx_mut");
    for n in 0..2000u64 {
        tx.put::<tables::CanonicalHeaders>(n, B256::with_last_byte(n as u8)).expect("put");
    }
    let code = Bytecode::new_raw(Bytes::from(vec![0x60; 20_000]));
    tx.put::<tables::Bytecodes>(B256::with_last_byte(1), code).expect("put");
    for (address, slots) in [(1u8, 3u8), (2, 2)] {
        for slot in 0..slots {
            let entry = StorageEntry { key: B256::with_last_byte(slot), value: U256::from(slot) };
            tx.put::<tables::PlainStorageState>(Address::with_last_byte(address), entry)
                .expect("put");
        }
    }
    tx.commit().expect("commit");
}

/// Once the writer is gone, the stats list every one of reth's tables (`open_rw` creates them
/// all) sorted by name, with each table's rows and pages, and its size as the page size times its
/// pages.
#[test]
fn db_table_stats_lists_populated_tables() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    write_stats_rows(&path);

    let stats = db_table_stats(&path).expect("stats");
    let names: Vec<_> = stats.iter().map(|stat| stat.name).collect();
    let mut sorted: Vec<_> = Tables::ALL.iter().map(Tables::name).collect();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "every table, sorted by name");

    let stat = |name: &str| *stats.iter().find(|stat| stat.name == name).expect("listed");
    let headers = stat(tables::CanonicalHeaders::NAME);
    assert_eq!(headers.entries, 2000);
    assert!(headers.branch_pages > 0 && headers.leaf_pages > 1, "{headers:?}");
    let bytecodes = stat(tables::Bytecodes::NAME);
    assert_eq!(bytecodes.entries, 1);
    assert!(bytecodes.overflow_pages > 0, "{bytecodes:?}");
    let storage = stat(tables::PlainStorageState::NAME);
    assert_eq!(storage.entries, 5, "each value of a dup-sort table is a row");
    assert!(storage.leaf_pages > 0, "{storage:?}");
    for stat in &stats {
        let pages = stat.branch_pages + stat.leaf_pages + stat.overflow_pages;
        assert_eq!(stat.total_size, STATS_PAGE_SIZE as u64 * pages, "{stat:?}");
    }
}

/// A table the database lacks is skipped rather than failing the report: here a database created
/// without reth's tables lists none.
#[test]
fn db_table_stats_skips_tables_the_database_lacks() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    #[cfg(not(feature = "tndb-b"))]
    drop(reth_db::create_db(&path, test_db_args().database_args()).expect("create"));
    #[cfg(feature = "tndb-b")]
    drop(
        tndb_b_reth::TndbEnv::open(
            &path,
            tndb_b_reth::TndbEnvKind::RW,
            tndb_b_reth::TndbArgs::default(),
        )
        .expect("create"),
    );

    assert_eq!(db_table_stats(&path).expect("stats"), []);
}

/// The stats refuse a database directory holding only the other engine's database, naming the
/// directory and both engines. On the MDBX build the directory also holds a readable MDBX
/// database, which the guard must not read. On the tndb build an MDBX database is one whose
/// migration has not run, which the stats never start.
#[test]
fn db_table_stats_refuses_the_other_engines_database() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    #[cfg(not(feature = "tndb-b"))]
    write_stats_rows(&path);
    #[cfg(feature = "tndb-b")]
    std::fs::create_dir(&path).expect("db dir");
    std::fs::write(path.join(OWN_AND_OTHER_DATA_FILE.1), b"planted").expect("plant");

    let err = db_table_stats(&path).expect_err("the other engine's file");
    let msg = format!("{err:#}");
    assert!(msg.contains(&path.display().to_string()), "names the directory: {msg}");
    assert!(msg.contains("MDBX") && msg.contains("tndb-b"), "names both engines: {msg}");
    #[cfg(feature = "tndb-b")]
    assert!(msg.contains("pending or unfinished") && msg.contains("start the node"), "{msg}");
    #[cfg(feature = "tndb-b")]
    assert!(!path.join(OWN_AND_OTHER_DATA_FILE.0).try_exists().expect("stat"), "nothing made");
}

/// A stat error while looking for a database's file fails the stats: it is never read as an
/// absent file.
#[test]
fn db_table_stats_fails_on_a_stat_error() {
    let tmp = TempDir::new().expect("tempdir");
    let file = tmp.path().join("file");
    std::fs::write(&file, b"not a directory").expect("file");

    let err = db_table_stats(&file.join("db")).expect_err("a stat error fails the stats");
    assert!(format!("{err:#}").contains("failed to check for"), "the check's own error: {err:#}");
}

#[cfg(feature = "tndb-b")]
mod tndb {
    use super::*;
    use reth_db::{cursor::DbCursorRO as _, mdbx::SyncMode as MdbxSyncMode, ClientVersion};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tndb_b_reth::{SyncMode, TndbArgs};

    /// Each flag tndb-b cannot honour is refused, and the error names it.
    #[test]
    fn tndb_args_refuses_unsupported_flags() {
        let cases = [
            ("--db.exclusive", DatabaseArgs { exclusive: Some(false), ..Default::default() }),
            (
                "--db.read-transaction-timeout",
                DatabaseArgs { read_transaction_timeout: Some(0), ..Default::default() },
            ),
            ("--db.max-readers", DatabaseArgs { max_readers: Some(1), ..Default::default() }),
            ("--db.growth-step", DatabaseArgs { growth_step: Some(4096), ..Default::default() }),
            (
                "--db.log-level",
                DatabaseArgs {
                    log_level: Some("notice".parse().expect("a log level")),
                    ..Default::default()
                },
            ),
            (
                "--db.sync-mode safe-no-sync",
                DatabaseArgs { sync_mode: Some(MdbxSyncMode::SafeNoSync), ..Default::default() },
            ),
            (
                "the UtterlyNoSync sync mode",
                DatabaseArgs { sync_mode: Some(MdbxSyncMode::UtterlyNoSync), ..Default::default() },
            ),
            (
                "the NoMetaSync sync mode",
                DatabaseArgs { sync_mode: Some(MdbxSyncMode::NoMetaSync), ..Default::default() },
            ),
        ];
        let accepted: Vec<_> = cases
            .iter()
            .filter(|(flag, args)| match tndb_args(args) {
                Ok(_) => true,
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains(flag) && msg.contains("not supported by the tndb-b backend"),
                        "the error names {flag}: {msg}"
                    );
                    false
                }
            })
            .map(|(flag, _)| *flag)
            .collect();
        assert!(accepted.is_empty(), "unsupported flags accepted: {accepted:?}");

        // The no-sync refusals also say why, and what to use instead.
        for mode in [MdbxSyncMode::SafeNoSync, MdbxSyncMode::UtterlyNoSync] {
            let msg = tndb_args(&DatabaseArgs { sync_mode: Some(mode), ..Default::default() })
                .expect_err("a no-sync mode is refused")
                .to_string();
            assert!(
                msg.contains("only after a sync") && msg.contains("use --db.sync-mode durable"),
                "the {mode:?} refusal says why: {msg}"
            );
        }
    }

    /// The flags tndb-b honours map onto the adapter's settings; unset flags keep its defaults.
    #[test]
    fn tndb_args_maps_supported_flags() {
        let default = TndbArgs::default();
        let cases = [
            ("no flags", DatabaseArgs::default(), default),
            (
                "--db.max-size",
                DatabaseArgs { max_size: Some(3 << 30), ..Default::default() },
                default.with_map_reserve(3 << 30),
            ),
            (
                "--db.page-size",
                DatabaseArgs { page_size: Some(16384), ..Default::default() },
                default.with_page_size(16384),
            ),
            (
                "--db.sync-mode durable",
                DatabaseArgs { sync_mode: Some(MdbxSyncMode::Durable), ..Default::default() },
                default.with_sync_mode(SyncMode::Durable),
            ),
        ];
        let wrong: Vec<_> = cases
            .iter()
            .filter(|(_, args, expected)| tndb_args(args).ok().as_ref() != Some(expected))
            .map(|(flag, args, _)| (*flag, tndb_args(args).map_err(|e| e.to_string())))
            .collect();
        assert!(wrong.is_empty(), "mapped wrongly: {wrong:?}");

        let too_large = usize::try_from(u64::from(u32::MAX) + 1).expect("64-bit usize");
        let err = tndb_args(&DatabaseArgs { page_size: Some(too_large), ..Default::default() })
            .expect_err("a page size past u32");
        assert!(err.to_string().contains("--db.page-size"), "names the flag: {err}");
    }

    /// A refused flag fails the open before anything is created.
    #[test]
    fn open_rw_refuses_an_unsupported_flag_before_creating_anything() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db = DatabaseArgs { growth_step: Some(4096), ..test_db_args() };

        let err = open_rw(&config(db), &path).expect_err("a refused flag");
        assert!(err.to_string().contains("--db.growth-step"), "names the flag: {err}");
        assert!(!path.try_exists().expect("stat"), "the directory is not created");
    }

    /// A client version for the record tests.
    fn version(version: &str) -> ClientVersion {
        ClientVersion {
            version: version.to_string(),
            git_sha: "0123abc".to_string(),
            build_timestamp: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    /// The `VersionHistory` rows, oldest first.
    fn version_history(db: &RethDb) -> Vec<(u64, ClientVersion)> {
        let tx = db.tx().expect("tx");
        let mut cursor = tx.cursor_read::<tables::VersionHistory>().expect("cursor");
        cursor.walk(None).expect("walk").collect::<Result<_, _>>().expect("rows")
    }

    /// Sleep into the next whole second: rows are keyed by the time in seconds, so a row written
    /// in the same second as the last would replace it rather than add one.
    fn next_second() {
        let into = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").subsec_nanos();
        std::thread::sleep(
            Duration::from_nanos(1_000_000_000 - u64::from(into)) + Duration::from_millis(20),
        );
    }

    /// The client version is recorded on the first open, not again while it is unchanged, and
    /// again when it changes. An empty version records nothing.
    #[test]
    fn client_version_is_recorded_once_and_again_on_change() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db_args = test_db_args();
        let open = |v: ClientVersion| open_tndb(&path, &db_args, v).expect("open");

        let db = open(ClientVersion::default());
        assert!(version_history(&db).is_empty(), "an empty version records nothing");
        drop(db);

        let db = open(version("1.0.0"));
        let rows = version_history(&db);
        assert_eq!(rows.len(), 1, "recorded on the first open: {rows:?}");
        assert_eq!(rows[0].1, version("1.0.0"));
        drop(db);

        next_second();
        let db = open(version("1.0.0"));
        let rows = version_history(&db);
        assert_eq!(rows.len(), 1, "an unchanged version is not recorded again: {rows:?}");
        drop(db);

        next_second();
        let db = open(version("1.1.0"));
        let rows = version_history(&db);
        assert_eq!(rows.len(), 2, "a changed version is recorded: {rows:?}");
        assert_eq!(rows[0].1, version("1.0.0"));
        assert_eq!(rows[1].1, version("1.1.0"));
        assert!(rows[0].0 < rows[1].0, "keyed by time: {rows:?}");
    }

    /// Reopening an existing database keeps its rows: creating the tables again changes nothing.
    #[test]
    fn reopen_keeps_rows_and_create_tables_is_idempotent() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db_args = test_db_args();
        let hash = B256::repeat_byte(0xab);

        let db = open_tndb(&path, &db_args, version("1.0.0")).expect("fresh open");
        let tx = db.tx_mut().expect("tx_mut");
        tx.put::<tables::CanonicalHeaders>(7, hash).expect("put");
        tx.commit().expect("commit");
        drop(db);

        let mut db = open_tndb(&path, &db_args, version("1.0.0")).expect("reopen");
        Arc::get_mut(&mut db).expect("sole handle").create_tables().expect("create_tables again");
        let tx = db.tx().expect("tx");
        assert_eq!(tx.get::<tables::CanonicalHeaders>(7).expect("get"), Some(hash));
        assert_eq!(tx.entries::<tables::CanonicalHeaders>().expect("entries"), 1);
    }

    /// The stats need the node stopped: while a read-write handle holds the database they are
    /// refused with an error saying so, and once it is dropped they read.
    #[test]
    fn db_table_stats_refuses_while_the_database_is_in_use() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db = open_rw(&config(test_db_args()), &path).expect("open");

        let err = db_table_stats(&path).expect_err("a held database");
        let msg = format!("{err:#}");
        assert!(msg.contains("is in use: stop the node first"), "says to stop the node: {msg}");
        assert!(msg.contains(&path.display().to_string()), "names the directory: {msg}");

        drop(db);
        assert!(!db_table_stats(&path).expect("stats once the node stopped").is_empty());
    }

    /// A directory without either engine's file holds no reth database, and the stats say so.
    #[test]
    fn db_table_stats_refuses_a_directory_without_a_database() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        std::fs::create_dir(&path).expect("db dir");

        let err = db_table_stats(&path).expect_err("no database");
        let msg = format!("{err:#}");
        assert!(msg.contains("no reth database at"), "says there is none: {msg}");
        assert!(msg.contains(tndb_b_reth::DATA_FILE) && msg.contains(MDBX_DATA_FILE), "{msg}");
        assert!(!path.join(tndb_b_reth::DATA_FILE).try_exists().expect("stat"), "nothing made");
    }

    /// reth's database version file is checked on reopen: a missing one is written, another
    /// version is refused.
    #[test]
    fn reopen_checks_the_database_version_file() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db_args = test_db_args();
        let version_file = reth_db::version::db_version_file_path(&path);

        drop(open_tndb(&path, &db_args, version("1.0.0")).expect("fresh open"));
        std::fs::remove_file(&version_file).expect("remove the version file");
        drop(open_tndb(&path, &db_args, version("1.0.0")).expect("reopen without it"));
        reth_db::version::check_db_version_file(&path).expect("written again");

        std::fs::write(&version_file, "1").expect("another version");
        let err = open_tndb(&path, &db_args, version("1.0.0")).expect_err("another version");
        assert!(format!("{err:#}").contains("version"), "a version error: {err:#}");
    }
}
