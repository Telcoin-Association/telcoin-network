//! Tests for the backend switch: the backend guard, the table stats of `db stats`, and on the
//! tn-lmdb build the `--db.*` mapping, the database version file and the client version record.

use super::*;
use reth_db::{
    table::Table as _,
    tables::{self, Tables},
    transaction::{DbTx as _, DbTxMut as _},
    Database as _,
};
use reth_node_core::node_config::NodeConfig;
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
    RethConfig::from_node_config(NodeConfig { chain, db, ..NodeConfig::default() })
}

/// This build's data file, and the other build's.
#[cfg(feature = "mdbx")]
const OWN_AND_OTHER_DATA_FILE: (&str, &str) = ("mdbx.dat", TN_LMDB_DATA_FILE);
#[cfg(not(feature = "mdbx"))]
const OWN_AND_OTHER_DATA_FILE: (&str, &str) = (tn_lmdb_reth::DATA_FILE, MDBX_DATA_FILE);

/// A database directory holding the other engine's data file is refused before anything is
/// created in it, and the error names the directory and both engines. On the tn-lmdb build the
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
    assert!(msg.contains("MDBX") && msg.contains("tn-lmdb"), "names both engines: {msg}");
    #[cfg(feature = "mdbx")]
    assert!(msg.contains("fresh datadir"), "says what the operator can do: {msg}");
    #[cfg(not(feature = "mdbx"))]
    {
        assert!(msg.contains("migration") && msg.contains("unchanged"), "says why: {msg}");
        assert_eq!(std::fs::read(path.join(other)).expect("read"), b"planted", "MDBX unchanged");
        let temp = path.join(MIGRATING_DIR).try_exists().expect("stat");
        assert!(!temp, "the failed migration's temp directory is removed");
    }

    let own_exists = path.join(own).try_exists().expect("stat");
    assert!(!own_exists, "nothing is created beside the other engine's file");
    let version_exists = reth_db::version::db_version_file_path(&path).try_exists().expect("stat");
    assert!(!version_exists, "no version file is written beside the other engine's file");
}

/// The MDBX build refuses `db check` without opening anything: MDBX keeps no page checksums.
#[cfg(feature = "mdbx")]
#[test]
fn db_check_is_refused_on_the_mdbx_build() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    let err = format!("{:#}", db_check(&path).expect_err("refused"));
    assert!(err.contains("MDBX") && err.contains("tn-lmdb"), "{err}");
    assert!(!path.try_exists().expect("stat"), "nothing created");
}

/// A dangling symbolic link named as the other engine's file counts as that file: the guard
/// refuses it, and creates nothing. Followed, the link would read as no database, and the open
/// would create one beside the datadir's static files.
#[test]
fn a_dangling_link_to_the_other_engines_file_is_refused() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    std::fs::create_dir(&path).expect("db dir");
    let (own, other) = OWN_AND_OTHER_DATA_FILE;
    std::os::unix::fs::symlink(tmp.path().join("gone"), path.join(other)).expect("link");

    let err = open_rw(&config(test_db_args()), &path).expect_err("the other engine's file");
    let msg = format!("{err:#}");
    // the MDBX build refuses the tn-lmdb file as such; the tn-lmdb build takes the link for an
    // MDBX database to migrate and fails reading it (No such file), before anything is written
    assert!(msg.contains(other), "names the file: {msg}");
    assert!(!path.join(own).try_exists().expect("stat"), "nothing is created beside the link");
    let link = std::fs::symlink_metadata(path.join(other)).expect("the link is kept");
    assert!(link.file_type().is_symlink());
    assert!(!path.join(MIGRATING_DIR).try_exists().expect("stat"), "no migration left behind");
}

/// A stat error while looking for the other engine's file fails the guard: it is never read as
/// an absent file. A path under a regular file stats with `ENOTDIR` on macOS and Linux.
#[test]
fn check_backend_fails_on_a_stat_error() {
    let tmp = TempDir::new().expect("tempdir");
    let file = tmp.path().join("file");
    std::fs::write(&file, b"not a directory").expect("file");
    let path = file.join("db");

    #[cfg(feature = "mdbx")]
    let checked = check_backend(&path);
    #[cfg(not(feature = "mdbx"))]
    let checked = check_backend(&path, tn_lmdb_reth::TnLmdbArgs::default());
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
    // the MDBX build writes reth's version; the tn-lmdb build its own, which reth's check refuses
    #[cfg(feature = "mdbx")]
    reth_db::version::check_db_version_file(&path).expect("reth's version file");
    #[cfg(not(feature = "mdbx"))]
    assert_eq!(reth_db::version::get_db_version(&path).expect("a version"), TN_LMDB_DB_VERSION);

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
    #[cfg(feature = "mdbx")]
    drop(reth_db::create_db(&path, test_db_args().database_args()).expect("create"));
    #[cfg(not(feature = "mdbx"))]
    drop(
        tn_lmdb_reth::TnLmdbEnv::open(
            &path,
            tn_lmdb_reth::TnLmdbEnvKind::RW,
            tn_lmdb_reth::TnLmdbArgs::default(),
        )
        .expect("create"),
    );

    assert_eq!(db_table_stats(&path).expect("stats"), []);
}

/// The stats refuse a database directory holding only the other engine's database, naming the
/// directory and both engines. On the MDBX build the directory also holds a readable MDBX
/// database, which the guard must not read. On the tn-lmdb build an MDBX database is one whose
/// migration has not run, which the stats never start.
#[test]
fn db_table_stats_refuses_the_other_engines_database() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    #[cfg(feature = "mdbx")]
    write_stats_rows(&path);
    #[cfg(not(feature = "mdbx"))]
    std::fs::create_dir(&path).expect("db dir");
    std::fs::write(path.join(OWN_AND_OTHER_DATA_FILE.1), b"planted").expect("plant");

    let err = db_table_stats(&path).expect_err("the other engine's file");
    let msg = format!("{err:#}");
    assert!(msg.contains(&path.display().to_string()), "names the directory: {msg}");
    assert!(msg.contains("MDBX") && msg.contains("tn-lmdb"), "names both engines: {msg}");
    #[cfg(not(feature = "mdbx"))]
    assert!(msg.contains("pending or unfinished") && msg.contains("start the node"), "{msg}");
    #[cfg(not(feature = "mdbx"))]
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

/// [`refuse_pending_migration`] refuses only a directory whose open would first migrate it: on the
/// tn-lmdb build one holding `mdbx.dat`, whatever it holds, naming the file; the MDBX build never
/// migrates, so it refuses nothing. Neither changes the directory.
#[test]
fn refuse_pending_migration_refuses_only_a_directory_the_open_would_migrate() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("db");
    refuse_pending_migration(&path).expect("no directory");
    std::fs::create_dir(&path).expect("db dir");
    refuse_pending_migration(&path).expect("an empty directory");
    std::fs::write(path.join("tn-lmdb.dat"), b"a tn-lmdb database").expect("plant");
    refuse_pending_migration(&path).expect("a tn-lmdb database alone");

    std::fs::write(path.join("mdbx.dat"), b"an MDBX database").expect("plant");
    let checked = refuse_pending_migration(&path);
    #[cfg(feature = "mdbx")]
    checked.expect("the MDBX build never migrates");
    #[cfg(not(feature = "mdbx"))]
    {
        let msg = format!("{:#}", checked.expect_err("the open would migrate it"));
        assert!(msg.contains(&path.join("mdbx.dat").display().to_string()), "names it: {msg}");
    }
    assert_eq!(std::fs::read(path.join("mdbx.dat")).expect("read"), b"an MDBX database");
    assert_eq!(std::fs::read_dir(&path).expect("list").count(), 2, "nothing else is made");
}

/// The MDBX build's guard on a migration's leftovers: its marker and its temp directory.
/// A subscriber keeping the text of every WARN event's fields, for a test asserting a warning.
#[cfg(feature = "mdbx")]
#[derive(Clone, Default)]
struct WarnCapture(Arc<std::sync::Mutex<Vec<String>>>);

#[cfg(feature = "mdbx")]
impl WarnCapture {
    /// The first warning whose text holds `text`.
    fn find(&self, text: &str) -> Option<String> {
        self.0.lock().expect("warnings").iter().find(|w| w.contains(text)).cloned()
    }
}

#[cfg(feature = "mdbx")]
impl tracing::Subscriber for WarnCapture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut text = String::new();
            event.record(&mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                text += &format!("{}={value:?} ", field.name());
            });
            self.0.lock().expect("warnings").push(text);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[cfg(feature = "mdbx")]
mod mdbx {
    use super::*;

    /// A migrated database whose `tn-lmdb.dat` is missing (the marker alone) is refused, saying to
    /// run a tn-lmdb build, and nothing is created beside the static files in its place. `db stats`
    /// refuses it the same way.
    #[test]
    fn open_rw_refuses_a_migrated_directory_without_its_file() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        std::fs::create_dir(&path).expect("db dir");
        std::fs::write(path.join(MIGRATED_MARKER), b"migrated").expect("marker");

        let refused = |err: eyre::Report| {
            let msg = format!("{err:#}");
            let marker = path.join(MIGRATED_MARKER).display().to_string();
            assert!(msg.contains(&marker), "names the marker: {msg}");
            assert!(msg.contains("is missing") && msg.contains("`mdbx` feature"), "{msg}");
        };
        refused(open_rw(&config(test_db_args()), &path).expect_err("the marker alone"));
        refused(db_table_stats(&path).expect_err("the stats refuse it too"));
        for absent in [OWN_AND_OTHER_DATA_FILE.0, "mdbx.lck", TN_LMDB_DATA_FILE] {
            assert!(!path.join(absent).try_exists().expect("stat"), "no {absent} is created");
        }
        let version = reth_db::version::db_version_file_path(&path).try_exists().expect("stat");
        assert!(!version, "no version file is written");
    }

    /// A stopped migration's temp directory beside an MDBX database is left as it is: the database
    /// opens, and the directory and its partial copy are kept. The guard warns of it, naming the
    /// directory and its size ([`dir_bytes`]).
    #[test]
    fn open_rw_keeps_a_stopped_migrations_temp_directory() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let config = config(test_db_args());
        drop(open_rw(&config, &path).expect("an MDBX database"));
        let temp = path.join(MIGRATING_DIR);
        std::fs::create_dir(&temp).expect("temp dir");
        std::fs::write(temp.join(TN_LMDB_DATA_FILE), b"a stopped copy").expect("partial copy");
        std::fs::write(temp.join("other"), b"more").expect("another file");

        let warnings = WarnCapture::default();
        let opened =
            tracing::subscriber::with_default(warnings.clone(), || open_rw(&config, &path));
        drop(opened.expect("the MDBX database opens beside it"));
        db_table_stats(&path).expect("the stats read beside it");
        let copy = std::fs::read(temp.join(TN_LMDB_DATA_FILE)).expect("the temp directory is kept");
        assert_eq!(copy, b"a stopped copy", "its copy is unchanged");
        assert_eq!(dir_bytes(&temp).expect("size"), 18, "the size the warning names");
        let warned = warnings
            .find("a stopped MDBX to tn-lmdb migration left its temp directory")
            .expect("the guard warns");
        assert!(warned.contains(&temp.display().to_string()), "names the directory: {warned}");
        assert!(warned.contains("18 bytes"), "names its size: {warned}");
    }
}

#[cfg(not(feature = "mdbx"))]
mod tn_lmdb {
    use super::*;
    use reth_db::{cursor::DbCursorRO as _, mdbx::SyncMode as MdbxSyncMode, ClientVersion};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tn_lmdb_reth::{SyncMode, TnLmdbArgs, MIN_GROWTH};

    /// Each flag tn-lmdb cannot honour is refused, and the error names it.
    #[test]
    fn tn_lmdb_args_refuses_unsupported_flags() {
        let cases = [
            ("--db.exclusive", DatabaseArgs { exclusive: Some(false), ..Default::default() }),
            ("--db.max-readers", DatabaseArgs { max_readers: Some(1), ..Default::default() }),
            (
                "--db.log-level",
                DatabaseArgs {
                    log_level: Some("notice".parse().expect("a log level")),
                    ..Default::default()
                },
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
            .filter(|(flag, args)| match tn_lmdb_args(args) {
                Ok(_) => true,
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains(flag) && msg.contains("not supported by the tn-lmdb backend"),
                        "the error names {flag}: {msg}"
                    );
                    false
                }
            })
            .map(|(flag, _)| *flag)
            .collect();
        assert!(accepted.is_empty(), "unsupported flags accepted: {accepted:?}");

        // The UtterlyNoSync refusal also says why, and what to use instead.
        let utterly =
            DatabaseArgs { sync_mode: Some(MdbxSyncMode::UtterlyNoSync), ..Default::default() };
        let msg = tn_lmdb_args(&utterly).expect_err("UtterlyNoSync is refused").to_string();
        assert!(
            msg.contains("no mode that never syncs")
                && msg.contains("use --db.sync-mode safe-no-sync or durable"),
            "the UtterlyNoSync refusal says why: {msg}"
        );
    }

    /// The flags tn-lmdb honours map onto the adapter's settings; unset flags keep its defaults.
    #[test]
    fn tn_lmdb_args_maps_supported_flags() {
        let default = TnLmdbArgs::default();
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
            (
                "--db.sync-mode safe-no-sync",
                DatabaseArgs { sync_mode: Some(MdbxSyncMode::SafeNoSync), ..Default::default() },
                default
                    .with_sync_mode(SyncMode::NoSync)
                    .with_sync_period(Some(SAFE_NO_SYNC_PERIOD))
                    .with_sync_bytes(Some(SAFE_NO_SYNC_BYTES)),
            ),
            (
                "--db.growth-step",
                DatabaseArgs { growth_step: Some(64 << 20), ..Default::default() },
                default.with_max_growth(64 << 20),
            ),
            (
                "--db.growth-step at the engine's minimum",
                DatabaseArgs {
                    growth_step: Some(usize::try_from(MIN_GROWTH).expect("64-bit usize")),
                    ..Default::default()
                },
                default.with_max_growth(MIN_GROWTH),
            ),
            (
                "--db.read-transaction-timeout 0 (off)",
                DatabaseArgs { read_transaction_timeout: Some(0), ..Default::default() },
                default.with_read_transaction_timeout(None),
            ),
            (
                "--db.read-transaction-timeout 90",
                DatabaseArgs { read_transaction_timeout: Some(90), ..Default::default() },
                default.with_read_transaction_timeout(Some(Duration::from_secs(90))),
            ),
        ];
        assert_eq!(
            default.read_transaction_timeout,
            Some(tn_lmdb_reth::DEFAULT_READ_TRANSACTION_TIMEOUT),
            "unset, the flag keeps MDBX's default"
        );
        let wrong: Vec<_> = cases
            .iter()
            .filter(|(_, args, expected)| tn_lmdb_args(args).ok().as_ref() != Some(expected))
            .map(|(flag, args, _)| (*flag, tn_lmdb_args(args).map_err(|e| e.to_string())))
            .collect();
        assert!(wrong.is_empty(), "mapped wrongly: {wrong:?}");

        let too_large = usize::try_from(u64::from(u32::MAX) + 1).expect("64-bit usize");
        let err = tn_lmdb_args(&DatabaseArgs { page_size: Some(too_large), ..Default::default() })
            .expect_err("a page size past u32");
        assert!(err.to_string().contains("--db.page-size"), "names the flag: {err}");

        let under = usize::try_from(MIN_GROWTH - 1).expect("64-bit usize");
        let err = tn_lmdb_args(&DatabaseArgs { growth_step: Some(under), ..Default::default() })
            .expect_err("a growth step under the engine's minimum");
        assert!(
            err.to_string().contains("--db.growth-step") && err.to_string().contains("minimum"),
            "names the flag and the minimum: {err}"
        );
    }

    /// `--db.gap-fill` parses into `RethConfig::db_gap_fill` (unset: `crate::DEFAULT_DB_GAP_FILL`,
    /// `auto` on Linux and off on macOS; a width in pages, `off`
    /// or `auto`; past `MAX_GAP_FILL` or anything else refused at the parse, naming the flag) and
    /// reaches the engine: a width past its `MAX_GAP_FILL` given by `with_db_gap_fill` is refused
    /// at the open, naming the flag, before the database is created; a width or `auto` opens, and
    /// its durable commits fill gaps (`IoStats::gaps_filled` is the engine's own count; `auto`'s
    /// warm-up fills from its first scattered commit, `IoStats::gap_auto_steps`). On a file system
    /// that refuses reads that do not wait for the disk (tmpfs refuses `RWF_NOWAIT`) the first
    /// commit with a gap to fill turns gap fill off instead, a width and `auto` alike
    /// (`IoStats::gap_fill_refusals`, the engine's documented fallback), filling none: the option
    /// reached the engine either way.
    #[test]
    fn db_gap_fill_reaches_the_engine_and_is_refused_past_its_maximum() {
        use clap::Parser as _;
        use tn_lmdb_reth::GapFill;
        let command = |flags: &[&str]| {
            crate::RethCommand::try_parse_from(
                std::iter::once("tn-reth").chain(flags.iter().copied()),
            )
        };
        let parse = |flags: &[&str]| {
            let chain = Arc::new(test_genesis().into());
            let command = command(flags).expect("parse");
            RethConfig::new(command, None, TempDir::new().expect("tempdir").path(), true, chain)
        };
        let unset = if cfg!(target_os = "linux") { GapFill::Auto } else { GapFill::OFF };
        assert_eq!(crate::DEFAULT_DB_GAP_FILL, unset, "auto on Linux, off elsewhere");
        assert_eq!(parse(&[]).db_gap_fill(), unset, "an unset flag takes the default");
        assert_eq!(parse(&["--db.gap-fill", "4"]).db_gap_fill(), GapFill::Pages(4));
        assert_eq!(parse(&["--db.gap-fill", "off"]).db_gap_fill(), GapFill::OFF);
        assert_eq!(parse(&["--db.gap-fill", "auto"]).db_gap_fill(), GapFill::Auto);
        for bad in ["65", "fast"] {
            let err = command(&["--db.gap-fill", bad]).expect_err(bad).to_string();
            assert!(err.contains("--db.gap-fill") && err.contains(bad), "{bad}: {err}");
        }
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let wide =
            config(test_db_args()).with_db_gap_fill(GapFill::Pages(tn_lmdb_reth::MAX_GAP_FILL + 1));
        let err = open_rw(&wide, &path).expect_err("past the maximum").to_string();
        assert!(err.contains("--db.gap-fill"), "{err}");
        assert!(!path.join(tn_lmdb_reth::DATA_FILE).exists(), "the refusal created the database");
        for (gap_fill, dir) in [(GapFill::Pages(4), "fixed"), (GapFill::Auto, "auto")] {
            let path = tmp.path().join(dir);
            let db =
                open_rw(&config(test_db_args()).with_db_gap_fill(gap_fill), &path).expect("opens");
            for i in 0..64u64 {
                db.update(|tx| {
                    for j in 0..64u64 {
                        let k = (i * 64 + j).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 40;
                        tx.put::<tables::CanonicalHeaders>(k, B256::with_last_byte(i as u8))?;
                    }
                    Ok::<_, reth_db::DatabaseError>(())
                })
                .expect("tx")
                .expect("put");
            }
            let s = db.io_stats();
            assert!(s.sync_runs > s.commits, "{gap_fill}: the commits formed runs: {s:?}");
            let auto = gap_fill == GapFill::Auto;
            assert!(auto || s.gap_auto_steps == 0, "{gap_fill}: only auto chooses: {s:?}");
            match s.gap_fill_refusals {
                0 => {
                    assert!(
                        s.gaps_filled > 0,
                        "{gap_fill}: the durable commits filled gaps: {s:?}"
                    );
                    assert!(!auto || s.gap_auto_steps > 0, "{gap_fill}: auto chose: {s:?}");
                }
                _ => {
                    let counts = (s.gap_fill_refusals, s.gaps_filled);
                    assert_eq!(counts, (1, 0), "{gap_fill}: refused once: {s:?}");
                }
            }
        }
    }

    /// `--db.sync-mode safe-no-sync`'s automatic syncs: one second, 64 MiB.
    #[test]
    fn safe_no_sync_syncs_every_second_or_64_mib() {
        assert_eq!(SAFE_NO_SYNC_PERIOD, Duration::from_secs(1));
        assert_eq!(SAFE_NO_SYNC_BYTES, 64 << 20);
    }

    /// `--db.sync-mode safe-no-sync` opens the database in the engine's `NoSync`: its commits
    /// make no state durable but by an automatic sync (`auto_syncs`), and the clean close makes
    /// the rest durable, so a reopen reads every row.
    #[test]
    fn open_rw_with_safe_no_sync_defers_commits_to_the_automatic_syncs_and_the_close() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let config =
            config(DatabaseArgs { sync_mode: Some(MdbxSyncMode::SafeNoSync), ..test_db_args() });
        let db = open_rw(&config, &path).expect("safe-no-sync opens");
        db.update(|tx| tx.put::<tables::CanonicalHeaders>(7, B256::with_last_byte(7)))
            .expect("tx")
            .expect("put");
        let s = db.io_stats();
        assert!(s.nosync_commits + s.auto_syncs > 0, "{s:?}");
        assert_eq!(s.commits, s.auto_syncs, "only automatic syncs made a state durable: {s:?}");
        drop(db);
        let db = open_rw(&config, &path).expect("reopen");
        let row = db.tx().expect("tx").get::<tables::CanonicalHeaders>(7).expect("get");
        assert_eq!(row, Some(B256::with_last_byte(7)), "the close made the commit durable");
    }

    /// A refused flag fails the open before anything is created.
    #[test]
    fn open_rw_refuses_an_unsupported_flag_before_creating_anything() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db = DatabaseArgs { max_readers: Some(1), ..test_db_args() };

        let err = open_rw(&config(db), &path).expect_err("a refused flag");
        assert!(err.to_string().contains("--db.max-readers"), "names the flag: {err}");
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
        let open = |v: ClientVersion| open_tn_lmdb(&path, &db_args, v).expect("open");

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

        let db = open_tn_lmdb(&path, &db_args, version("1.0.0")).expect("fresh open");
        let tx = db.tx_mut().expect("tx_mut");
        tx.put::<tables::CanonicalHeaders>(7, hash).expect("put");
        tx.commit().expect("commit");
        drop(db);

        let mut db = open_tn_lmdb(&path, &db_args, version("1.0.0")).expect("reopen");
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

    /// `db check` runs the engine's full check on a stopped node's database: it passes on a
    /// healthy one, listing every table the node created with the file's page accounting;
    /// it is refused while a read-write handle holds the database; and damage at rest (pages
    /// overwritten) fails it with an error naming the directory, the file unchanged.
    #[test]
    fn db_check_reports_the_state_and_fails_on_damage() {
        use std::os::unix::fs::FileExt as _;

        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        write_stats_rows(&path);
        let db = open_rw(&config(test_db_args()), &path).expect("open");
        let err = format!("{:#}", db_check(&path).expect_err("a held database"));
        assert!(err.contains("is in use: stop the node first"), "{err}");
        drop(db);

        let report = db_check(&path).expect("a healthy database checks");
        assert!(
            report.tables.iter().any(|t| t.name == tables::CanonicalHeaders::NAME),
            "{report:?}"
        );
        assert!(report.tables.windows(2).all(|w| w[0].name < w[1].name), "sorted: {report:?}");
        let counted = 2
            + report.catalog_pages
            + report.table_pages
            + report.free_tree_pages
            + report.free_pages;
        assert_eq!(counted, report.page_count, "the accounting is exact: {report:?}");

        // Pages past the roots open checks (the 2000 `CanonicalHeaders` rows span many leaves),
        // so the damage is the check's to find; an open that refuses the file is a refusal too.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path.join(tn_lmdb_reth::DATA_FILE))
            .expect("the data file");
        file.write_all_at(&[0xFF; 4 * 4096], 8 * 4096).expect("overwrite pages 8 to 11");
        let before = std::fs::read(path.join(tn_lmdb_reth::DATA_FILE)).expect("read");
        let err = format!("{:#}", db_check(&path).expect_err("damaged pages fail the check"));
        assert!(
            err.contains("fails its check") || err.contains("corrupted"),
            "the damage is reported: {err}"
        );
        assert!(err.contains(&path.display().to_string()), "names the directory: {err}");
        assert_eq!(std::fs::read(path.join(tn_lmdb_reth::DATA_FILE)).expect("read"), before);
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
        assert!(msg.contains(tn_lmdb_reth::DATA_FILE) && msg.contains(MDBX_DATA_FILE), "{msg}");
        assert!(!path.join(tn_lmdb_reth::DATA_FILE).try_exists().expect("stat"), "nothing made");
    }

    /// reth's database version file is checked on reopen: a new database gets tn-lmdb's version;
    /// a missing file, or reth's own version (a database from before the tripwire, or one the
    /// migration moved), is rewritten with it; another version, or a file that does not parse, is
    /// refused and left as it is.
    #[test]
    fn reopen_checks_the_database_version_file() {
        use reth_db::version::{get_db_version, DB_VERSION};

        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db_args = test_db_args();
        let version_file = reth_db::version::db_version_file_path(&path);
        let open = || open_tn_lmdb(&path, &db_args, version("1.0.0"));

        drop(open().expect("fresh open"));
        assert_eq!(get_db_version(&path).expect("a version"), TN_LMDB_DB_VERSION, "new");
        drop(open().expect("reopen"));
        assert_eq!(get_db_version(&path).expect("a version"), TN_LMDB_DB_VERSION, "reopened");

        std::fs::remove_file(&version_file).expect("remove the version file");
        drop(open().expect("reopen without it"));
        assert_eq!(get_db_version(&path).expect("a version"), TN_LMDB_DB_VERSION, "written again");

        std::fs::write(&version_file, DB_VERSION.to_string()).expect("reth's version");
        drop(open().expect("reopen with reth's version"));
        assert_eq!(get_db_version(&path).expect("a version"), TN_LMDB_DB_VERSION, "rewritten");
        let tmp_file = path.join(format!("{}.tmp", reth_db::version::DB_VERSION_FILE_NAME));
        assert!(!tmp_file.try_exists().expect("stat"), "the rewrite's temp file is renamed");

        for (other, says) in [("1", "database version 1"), ("1002\n", "version file")] {
            std::fs::write(&version_file, other).expect("another version");
            let err = open().expect_err("another version");
            let msg = format!("{err:#}");
            assert!(msg.contains(says), "{other:?}: {msg}");
            assert_eq!(std::fs::read(&version_file).expect("read"), other.as_bytes(), "kept");
        }
    }

    /// The version check runs before the backend guard: a directory another binary wrote (a
    /// version this build does not read) is refused with `mdbx.dat` in it untouched, where the
    /// guard would have migrated it.
    #[test]
    fn another_binarys_version_is_refused_before_the_migration() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        std::fs::create_dir(&path).expect("db dir");
        std::fs::write(path.join(MDBX_DATA_FILE), b"planted").expect("plant MDBX's file");
        let version_file = reth_db::version::db_version_file_path(&path);
        std::fs::write(&version_file, "7").expect("another version");

        let err = open_tn_lmdb(&path, &test_db_args(), version("1.0.0")).expect_err("refused");
        let msg = format!("{err:#}");
        assert!(msg.contains("database version 7"), "the version check refused it: {msg}");
        assert_eq!(std::fs::read(path.join(MDBX_DATA_FILE)).expect("read"), b"planted", "kept");
        assert!(!path.join(tn_lmdb_reth::DATA_FILE).try_exists().expect("stat"), "nothing made");
        assert!(!path.join(MIGRATING_DIR).try_exists().expect("stat"), "no migration started");
    }

    /// The rollback tripwire: reth's `create_db`, which a binary running reth on MDBX alone calls
    /// first on every start (`v0.16.0-adiri`'s reth v1.11.3 has the same function), refuses a
    /// tn-lmdb database before it creates anything, so it never opens an empty MDBX database beside
    /// `tn-lmdb.dat` (whose consistency check would prune the static files down to it). Run over a
    /// new tn-lmdb database and over one that held reth's version, as a migrated one does.
    #[test]
    fn an_mdbx_only_binary_refuses_a_tn_lmdb_database() {
        use reth_db::version::{db_version_file_path, DatabaseVersionError, DB_VERSION};

        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("db");
        let db_args = test_db_args();
        let open = || open_tn_lmdb(&path, &db_args, version("1.0.0"));
        drop(open().expect("fresh open"));

        for run in ["new", "migrated"] {
            if run == "migrated" {
                std::fs::write(db_version_file_path(&path), DB_VERSION.to_string())
                    .expect("reth's version");
                drop(open().expect("reopen with reth's version"));
            }
            let before = listing(&path);
            let err = reth_db::mdbx::create_db(&path, reth_db::mdbx::DatabaseArguments::default())
                .expect_err("reth's create_db refuses a tn-lmdb database");
            let version_err = err.downcast_ref::<DatabaseVersionError>();
            assert!(
                matches!(
                    version_err,
                    Some(DatabaseVersionError::VersionMismatch { version: TN_LMDB_DB_VERSION })
                ),
                "{run}: refused by the version check: {err:#}"
            );
            assert_eq!(listing(&path), before, "{run}: nothing created or changed");
            assert!(!path.join(MDBX_DATA_FILE).try_exists().expect("stat"), "{run}: no mdbx.dat");
        }
    }

    /// The names and contents of the files directly in `dir`, sorted.
    fn listing(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .expect("list the directory")
            .map(|entry| {
                let entry = entry.expect("a directory entry");
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    std::fs::read(entry.path()).expect("read"),
                )
            })
            .collect();
        files.sort();
        files
    }
}
