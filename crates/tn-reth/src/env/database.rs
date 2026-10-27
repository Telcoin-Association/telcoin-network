//! The engine under TN's reth execution database.
//!
//! The default build runs reth's database on MDBX (`reth_db::DatabaseEnv`, `<db>/mdbx.dat`). The
//! `tndb-b` feature runs it on the tndb-b engine instead (`tndb_b_reth::TndbEnv`,
//! `<db>/tndb.dat`). Every difference between the two builds lives in this module: the rest of the
//! crate names [`RethDbEnv`] and [`RethDb`], never either engine.
//!
//! [`open_rw`] opens (or creates) the read-write database a node runs on. Both builds first run
//! [`check_backend`]: the MDBX build refuses a database directory that holds tndb-b's file, and
//! the tndb build migrates an MDBX database onto tndb-b (`migrate`).
//! [`db_table_stats`] reads each table's figures for `db stats`: live beside a running node on
//! MDBX, with the node stopped on tndb-b ([`DB_TABLE_STATS_NEEDS_STOPPED_NODE`]).

use std::{path::Path, sync::Arc};

use eyre::WrapErr as _;
use reth_node_core::args::DatabaseArgs;

use crate::RethConfig;

#[cfg(feature = "tndb-b")]
mod migrate;
#[cfg(test)]
mod tests;

/// The database environment reth runs on: MDBX's `DatabaseEnv`.
#[cfg(not(feature = "tndb-b"))]
pub type RethDbEnv = reth_db::DatabaseEnv;

/// The database environment reth runs on: tndb-b's `TndbEnv`.
#[cfg(feature = "tndb-b")]
pub type RethDbEnv = tndb_b_reth::TndbEnv;

/// Type wrapper for a Reth DB.
/// Used primarily as an opaque type so the node launcher can create the DB upfront and reuse it.
pub type RethDb = Arc<RethDbEnv>;

/// MDBX's data file inside the database directory.
#[cfg(feature = "tndb-b")]
const MDBX_DATA_FILE: &str = "mdbx.dat";

/// tndb-b's data file inside the database directory: `tndb_b_reth::DATA_FILE`, which the MDBX
/// build does not compile in.
#[cfg(not(feature = "tndb-b"))]
const TNDB_DATA_FILE: &str = "tndb.dat";

/// The `--db.*` settings of a throwaway temp chain's database (`RethEnv::new_for_temp_chain`).
///
/// The map is bounded at 2 GiB. reth's MDBX default reserves 8 TB of address space per
/// environment, and `cargo test` runs a test binary as threads in ONE process, so N concurrent
/// 8 TB reservations exhaust the address space and MDBX aborts env-open with ENOMEM ("Cannot
/// allocate memory (12)"). A temp chain never holds a full node's history, so a small map is safe
/// and lets many envs coexist. On tndb-b the same 2 GiB is the mapping's reservation, past which
/// the engine remaps rather than fails.
///
/// The MDBX build also grows the file in 4 MiB steps, reth's `test()` step (reth pairs its 8 TB
/// default with a 4 GiB step), so throwaway databases stay small on disk. tndb-b grows its file
/// as pages are written and refuses `--db.growth-step`.
pub(crate) fn temp_chain_db_args() -> DatabaseArgs {
    /// The temp chain's map size.
    const TEMP_CHAIN_DB_MAX_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2 GiB
    /// The temp chain's MDBX growth step.
    #[cfg(not(feature = "tndb-b"))]
    const TEMP_CHAIN_DB_GROWTH_STEP: usize = 4 * 1024 * 1024; // 4 MiB

    DatabaseArgs {
        max_size: Some(TEMP_CHAIN_DB_MAX_SIZE),
        #[cfg(not(feature = "tndb-b"))]
        growth_step: Some(TEMP_CHAIN_DB_GROWTH_STEP),
        ..Default::default()
    }
}

/// Open (or create) the read-write reth database in directory `path`, after [`check_backend`].
///
/// The MDBX build opens through reth's `init_db` and records per-operation latency metrics
/// (`with_metrics`; a noop unless the global metrics recorder is installed, i.e. the node runs
/// with `--metrics`).
#[cfg(not(feature = "tndb-b"))]
pub(crate) fn open_rw(reth_config: &RethConfig, path: &Path) -> eyre::Result<RethDb> {
    check_backend(path)?;
    Ok(Arc::new(reth_db::init_db(path, reth_config.0.db.database_args())?.with_metrics()))
}

/// Open (or create) the read-write reth database in directory `path`, after [`check_backend`].
///
/// The tndb build maps the `--db.*` flags ([`tndb_args`]) first, so a refused flag fails before
/// anything is read or written, and its [`check_backend`] migrates an MDBX database onto tndb-b
/// with them. It then opens as reth's `init_db` does: the database version file, every one of
/// reth's tables, and the client version.
#[cfg(feature = "tndb-b")]
pub(crate) fn open_rw(reth_config: &RethConfig, path: &Path) -> eyre::Result<RethDb> {
    let db_args = &reth_config.0.db;
    open_tndb(path, db_args, db_args.database_args().client_version().clone())
}

#[cfg(all(test, feature = "tndb-b"))]
thread_local! {
    /// Test-only: the tndb-b databases this thread opens through [`open_tndb`] commit with the
    /// engine's `NoSync`, which [`tndb_args`] refuses on the CLI. The persist bench's `nosync` mode
    /// sets it (`env::persist_bench`).
    pub(crate) static TEST_NO_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`open_rw`] of the tndb build, with the client version to record.
#[cfg(feature = "tndb-b")]
fn open_tndb(
    path: &Path,
    db_args: &DatabaseArgs,
    client_version: reth_db::ClientVersion,
) -> eyre::Result<RethDb> {
    use tndb_b_reth::{TndbEnv, TndbEnvKind};

    let args = tndb_args(db_args)?;
    #[cfg(test)]
    let args =
        if TEST_NO_SYNC.get() { args.with_sync_mode(tndb_b_reth::SyncMode::NoSync) } else { args };
    check_backend(path, args)?;
    init_db_version_file(path)?;
    let mut env = TndbEnv::open(path, TndbEnvKind::RW, args).wrap_err_with(|| {
        format!("failed to open the tndb-b reth database at {}", path.display())
    })?;
    env.create_tables()?;
    record_client_version(&env, client_version)?;
    Ok(Arc::new(env))
}

/// Refuse a database directory that holds the other engine's data file.
///
/// Opening the wrong engine would create an empty database beside the datadir's static files,
/// and reth's `check_consistency` then prunes the static files back to the empty database's tip:
/// the node would lose its chain. Each check is `try_exists`, so a stat error fails the open and
/// is never read as an absent file.
///
/// The MDBX build refuses `tndb.dat`.
#[cfg(not(feature = "tndb-b"))]
fn check_backend(path: &Path) -> eyre::Result<()> {
    let tndb = path.join(TNDB_DATA_FILE);
    if file_exists(&tndb)? {
        eyre::bail!(
            "the reth database at {} is a tndb-b database ({} exists), but this build runs \
             reth on MDBX: run a build with the `tndb-b` feature, or start from a fresh datadir \
             (or `db load-state`)",
            path.display(),
            tndb.display(),
        );
    }
    Ok(())
}

/// Never open a database directory that holds the other engine's data file as it is.
///
/// Opening the wrong engine would create an empty database beside the datadir's static files,
/// and reth's `check_consistency` then prunes the static files back to the empty database's tip:
/// the node would lose its chain. Each check is `try_exists`, so a stat error fails the open and
/// is never read as an absent file.
///
/// The tndb build moves an MDBX database (`mdbx.dat`) onto tndb-b instead, through
/// `migrate::migrate_from_mdbx` (its module docs list the states and steps), with `args`' page
/// size and reservation. It also finishes a migration stopped after its commit point, refuses an
/// `mdbx.dat` written after a migration (or a migrated database whose `tndb.dat` is missing), and
/// removes a stopped migration's temp directory.
#[cfg(feature = "tndb-b")]
fn check_backend(path: &Path, args: tndb_b_reth::TndbArgs) -> eyre::Result<()> {
    migrate::migrate_from_mdbx(path, args)
}

/// The figures of one of reth's tables in the database's newest committed state
/// ([`db_table_stats`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbTableStat {
    /// The table's name.
    pub name: &'static str,
    /// Its rows (each value of a dup-sort table is one).
    pub entries: u64,
    /// The branch pages of its tree.
    pub branch_pages: u64,
    /// The leaf pages of its tree.
    pub leaf_pages: u64,
    /// The pages of the overflow runs holding its values too large for a leaf.
    pub overflow_pages: u64,
    /// The bytes those pages take: the page size times the branch, leaf and overflow pages.
    pub total_size: u64,
}

/// Whether [`db_table_stats`] needs the node stopped. The MDBX build reads beside a running node.
/// The tndb build cannot: its read-only open is refused while another handle holds the database
/// with write access, so a caller holds the datadir lock for the report, and a node starting
/// meanwhile is refused at that lock rather than at its database open.
pub const DB_TABLE_STATS_NEEDS_STOPPED_NODE: bool = cfg!(feature = "tndb-b");

/// The figures of each of reth's tables in the database in directory `path`, sorted by name. A
/// table the database lacks is skipped, so an older database whose schema predates a table later
/// added to reth's tables still reports.
///
/// The MDBX build opens read-only through reth's `open_db_read_only`, which works while a node
/// runs. It first refuses a directory holding a tndb-b database, as the backend guard does.
#[cfg(not(feature = "tndb-b"))]
pub fn db_table_stats(path: &Path) -> eyre::Result<Vec<DbTableStat>> {
    use reth_db::{
        mdbx::{open_db_read_only, DatabaseArguments},
        Database as _, Tables,
    };

    check_backend(path)?;
    let db = open_db_read_only(path, DatabaseArguments::default())?;
    let mut stats = Vec::new();
    db.view(|tx| {
        let mut names = Tables::ALL.iter().map(Tables::name).collect::<Vec<_>>();
        names.sort_unstable();

        for name in names {
            stats.extend(mdbx_table_stat(tx, name)?);
        }

        Ok::<(), eyre::Report>(())
    })??;
    Ok(stats)
}

/// The figures of table `name` in the MDBX database `tx` reads, or `None` if the database lacks
/// it: a read-only transaction cannot create a sub-database, so a table never written reports
/// `NotFound`. The MDBX build's [`db_table_stats`] reports them; the tndb build's migration sums
/// them for the free space it needs.
fn mdbx_table_stat(
    tx: &reth_db::mdbx::tx::Tx<reth_db::mdbx::RO>,
    name: &'static str,
) -> eyre::Result<Option<DbTableStat>> {
    let table_db = match tx.inner().open_db(Some(name)) {
        Ok(table_db) => table_db,
        Err(reth_db::mdbx::Error::NotFound) => return Ok(None),
        Err(err) => eyre::bail!("Could not open table {name}: {err}"),
    };
    let table_stats = tx
        .inner()
        .db_stat(table_db.dbi())
        .map_err(|err| eyre::eyre!("Could not read statistics for table {name}: {err}"))?;

    let page_size = table_stats.page_size() as usize;
    let leaf_pages = table_stats.leaf_pages();
    let branch_pages = table_stats.branch_pages();
    let overflow_pages = table_stats.overflow_pages();
    let num_pages = leaf_pages + branch_pages + overflow_pages;

    Ok(Some(DbTableStat {
        name,
        entries: table_stats.entries() as u64,
        branch_pages: branch_pages as u64,
        leaf_pages: leaf_pages as u64,
        overflow_pages: overflow_pages as u64,
        total_size: page_size.saturating_mul(num_pages) as u64,
    }))
}

/// The figures of each of reth's tables in the database in directory `path`, sorted by name. A
/// table the database lacks is skipped, so an older database whose schema predates a table later
/// added to reth's tables still reports.
///
/// The tndb build opens read-only, which needs the node stopped
/// ([`DB_TABLE_STATS_NEEDS_STOPPED_NODE`]): a database another handle holds with write access is
/// refused with an error saying to stop the node. It never migrates: a directory holding an MDBX
/// database (`mdbx.dat`) is refused, as one whose migration to tndb-b is pending or unfinished
/// (the node's open runs it), or, beside a finished migration's marker, as the node's open
/// refuses it. With neither `mdbx.dat` nor `tndb.dat` there is no reth database at `path`.
#[cfg(feature = "tndb-b")]
pub fn db_table_stats(path: &Path) -> eyre::Result<Vec<DbTableStat>> {
    use reth_db::{mdbx::Error as MdbxError, DatabaseError};
    use tndb_b_reth::{TndbArgs, TndbEnv, TndbEnvKind, DATA_FILE};

    let mdbx = path.join(MDBX_DATA_FILE);
    if file_exists(&mdbx)? {
        if file_exists(&path.join(migrate::MIGRATED_MARKER))? {
            return Err(migrate::written_after_migration(path));
        }
        eyre::bail!(
            "the reth database at {} holds an MDBX database ({} exists): its MDBX to tndb-b \
             migration is pending or unfinished; start the node to run it",
            path.display(),
            mdbx.display(),
        );
    }
    let tndb = path.join(DATA_FILE);
    if !file_exists(&tndb)? {
        eyre::bail!(
            "no reth database at {}: neither {} nor {} exists",
            path.display(),
            tndb.display(),
            mdbx.display(),
        );
    }

    // The adapter reports a database another handle holds as MDBX's `MDBX_BUSY`.
    let busy = MdbxError::Busy.to_err_code();
    let env = TndbEnv::open(path, TndbEnvKind::RO, TndbArgs::default()).map_err(|e| match e {
        DatabaseError::Open(info) if info.code == busy => {
            eyre::eyre!("the reth database at {} is in use: stop the node first", path.display())
        }
        e => eyre::Report::new(e)
            .wrap_err(format!("failed to open the tndb-b reth database at {}", path.display())),
    })?;
    let page_size = u64::from(env.page_size());
    let mut stats: Vec<_> = env
        .table_stats()
        .wrap_err_with(|| {
            format!("failed to read the tables of the reth database at {}", path.display())
        })?
        .into_iter()
        .map(|stat| DbTableStat {
            name: stat.name,
            entries: stat.entries,
            branch_pages: stat.branch_pages,
            leaf_pages: stat.leaf_pages,
            overflow_pages: stat.overflow_pages,
            total_size: page_size.saturating_mul(stat.pages()),
        })
        .collect();
    stats.sort_unstable_by_key(|stat| stat.name);
    Ok(stats)
}

/// Whether `file` exists. A stat error is an error, never "absent".
fn file_exists(file: &Path) -> eyre::Result<bool> {
    file.try_exists().wrap_err_with(|| {
        format!("failed to check for {} before opening the reth database", file.display())
    })
}

/// Map reth's `--db.*` flags onto the tndb-b adapter's settings.
///
/// - `--db.max-size` is the address space the mapping reserves (`map_reserve`); unset, the engine's
///   default. Past the reservation the engine remaps at double the size, so it is a reservation,
///   not a cap on the database's size.
/// - `--db.page-size` is the page size of a new file; the engine validates it (a power of two in 4
///   KiB..=64 KiB). A size that does not fit a `u32` is refused here.
/// - `--db.sync-mode`: `durable` is durable commits. `safe-no-sync` and reth's `UtterlyNoSync` (no
///   CLI value) are refused: the engine's `NoSync` reuses the pages a commit frees only after a
///   sync, and nothing syncs a `NoSync` environment on its own, so the file would grow until one.
///   reth's `NoMetaSync` (no CLI value either) is refused.
/// - `--db.exclusive`, `--db.read-transaction-timeout`, `--db.max-readers`, `--db.growth-step` and
///   `--db.log-level` have no tndb-b equivalent and are refused when set.
#[cfg(feature = "tndb-b")]
fn tndb_args(args: &DatabaseArgs) -> eyre::Result<tndb_b_reth::TndbArgs> {
    use reth_db::mdbx::SyncMode as MdbxSyncMode;
    use tndb_b_reth::{SyncMode, TndbArgs};

    let refuse = |flag: &str| eyre::eyre!("{flag} is not supported by the tndb-b backend");
    // Destructured whole, so a flag reth adds fails to compile here until it is mapped or refused.
    let DatabaseArgs {
        log_level,
        exclusive,
        max_size,
        page_size,
        growth_step,
        read_transaction_timeout,
        max_readers,
        sync_mode,
    } = *args;
    if exclusive.is_some() {
        return Err(refuse("--db.exclusive"));
    }
    if read_transaction_timeout.is_some() {
        return Err(refuse("--db.read-transaction-timeout"));
    }
    if max_readers.is_some() {
        return Err(refuse("--db.max-readers"));
    }
    if growth_step.is_some() {
        return Err(refuse("--db.growth-step"));
    }
    if log_level.is_some() {
        return Err(refuse("--db.log-level"));
    }

    let mut tndb = TndbArgs::default();
    if let Some(max_size) = max_size {
        tndb = tndb.with_map_reserve(
            u64::try_from(max_size).wrap_err("--db.max-size does not fit in 64 bits")?,
        );
    }
    if let Some(page_size) = page_size {
        let page_size = u32::try_from(page_size).map_err(|_| {
            eyre::eyre!("--db.page-size {page_size} is too large for the tndb-b backend")
        })?;
        tndb = tndb.with_page_size(page_size);
    }
    if let Some(sync_mode) = sync_mode {
        let refuse_no_sync = |mode: &str| {
            eyre::eyre!(
                "{mode} is not supported by the tndb-b backend: its no-sync mode reuses the pages \
                 a commit frees only after a sync, so the file would grow until one; use \
                 --db.sync-mode durable"
            )
        };
        let sync = match sync_mode {
            MdbxSyncMode::Durable => SyncMode::Durable,
            MdbxSyncMode::SafeNoSync => return Err(refuse_no_sync("--db.sync-mode safe-no-sync")),
            MdbxSyncMode::UtterlyNoSync => {
                return Err(refuse_no_sync("the UtterlyNoSync sync mode"))
            }
            MdbxSyncMode::NoMetaSync => return Err(refuse("the NoMetaSync sync mode")),
        };
        tndb = tndb.with_sync_mode(sync);
    }
    Ok(tndb)
}

/// Create or check reth's database version file in `path`, as reth's `create_db` does: an absent
/// or empty directory is created and gets the file; otherwise the file must hold reth's version,
/// and a missing one is written.
#[cfg(feature = "tndb-b")]
fn init_db_version_file(path: &Path) -> eyre::Result<()> {
    use reth_db::version::{check_db_version_file, create_db_version_file, DatabaseVersionError};

    if database_is_empty(path)? {
        std::fs::create_dir_all(path)
            .wrap_err_with(|| format!("could not create database directory {}", path.display()))?;
        create_db_version_file(path)?;
    } else {
        match check_db_version_file(path) {
            Ok(()) => (),
            Err(DatabaseVersionError::MissingFile) => create_db_version_file(path)?,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

/// Whether the database directory `path` is absent or empty: reth's `is_database_empty`, except
/// that a stat or listing error is an error, never "empty".
#[cfg(feature = "tndb-b")]
fn database_is_empty(path: &Path) -> eyre::Result<bool> {
    if !file_exists(path)? {
        return Ok(true);
    }
    let mut entries = std::fs::read_dir(path)
        .wrap_err_with(|| format!("failed to list database directory {}", path.display()))?;
    Ok(entries.next().is_none())
}

/// Record the client version that opens the database with write access, as reth's
/// `DatabaseEnv::record_client_version` does: a new `VersionHistory` row, keyed by the time in
/// seconds, when `version` differs from the last row. An empty version records nothing.
#[cfg(feature = "tndb-b")]
fn record_client_version<DB: reth_db::Database>(
    db: &DB,
    version: reth_db::ClientVersion,
) -> Result<(), reth_db::DatabaseError> {
    use reth_db::{
        cursor::{DbCursorRO as _, DbCursorRW as _},
        tables,
        transaction::{DbTx as _, DbTxMut as _},
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    if version.is_empty() {
        return Ok(());
    }

    let tx = db.tx_mut()?;
    let mut version_cursor = tx.cursor_write::<tables::VersionHistory>()?;

    let last_version = version_cursor.last()?.map(|(_, v)| v);
    if Some(&version) != last_version.as_ref() {
        version_cursor.upsert(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            &version,
        )?;
        tx.commit()?;
    }

    Ok(())
}
