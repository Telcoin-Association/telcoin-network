//! The engine under TN's reth execution database.
//!
//! The default build runs reth's database on MDBX (`reth_db::DatabaseEnv`, `<db>/mdbx.dat`). The
//! `tn-lmdb` feature runs it on the tn-lmdb engine instead (`tn_lmdb_reth::TnLmdbEnv`,
//! `<db>/tn-lmdb.dat`). Every difference between the two builds lives in this module: the rest of
//! the crate names [`RethDbEnv`] and [`RethDb`], never either engine.
//!
//! [`open_rw`] opens (or creates) the read-write database a node runs on. Both builds first run
//! [`check_backend`]: the MDBX build refuses a database directory that holds tn-lmdb's file or a
//! migration's marker, and the tn-lmdb build migrates an MDBX database onto tn-lmdb (`migrate`).
//! [`db_table_stats`] reads each table's figures for `db stats`: live beside a running node on
//! MDBX, with the node stopped on tn-lmdb ([`DB_TABLE_STATS_NEEDS_STOPPED_NODE`]).
//! [`default_page_size`] is the page size a new database gets without `--db.page-size`.

use std::{path::Path, sync::Arc};

use eyre::WrapErr as _;
use reth_node_core::args::DatabaseArgs;

use crate::RethConfig;

#[cfg(feature = "tn-lmdb")]
mod migrate;
#[cfg(test)]
mod tests;

/// The database environment reth runs on: MDBX's `DatabaseEnv`.
#[cfg(not(feature = "tn-lmdb"))]
pub type RethDbEnv = reth_db::DatabaseEnv;

/// The database environment reth runs on: tn-lmdb's `TnLmdbEnv`.
#[cfg(feature = "tn-lmdb")]
pub type RethDbEnv = tn_lmdb_reth::TnLmdbEnv;

/// Type wrapper for a Reth DB.
/// Used primarily as an opaque type so the node launcher can create the DB upfront and reuse it.
pub type RethDb = Arc<RethDbEnv>;

/// MDBX's data file inside the database directory.
#[cfg(feature = "tn-lmdb")]
const MDBX_DATA_FILE: &str = "mdbx.dat";

/// tn-lmdb's data file inside the database directory: `tn_lmdb_reth::DATA_FILE`, which the MDBX
/// build does not compile in.
#[cfg(not(feature = "tn-lmdb"))]
const TN_LMDB_DATA_FILE: &str = "tn-lmdb.dat";

/// The marker a finished MDBX to tn-lmdb migration leaves in the database directory. The tn-lmdb
/// build refuses an `mdbx.dat` beside it (written after the migration); the MDBX build refuses
/// the directory.
const MIGRATED_MARKER: &str = "mdbx-migrated";

/// The directory, inside the database directory, an MDBX to tn-lmdb migration builds its file in.
/// The tn-lmdb build removes a stopped migration's; the MDBX build warns of it and leaves it.
const MIGRATING_DIR: &str = "tn-lmdb-migrating";

/// The `--db.*` settings of a throwaway temp chain's database (`RethEnv::new_for_temp_chain`).
///
/// The map is bounded at 2 GiB. reth's MDBX default reserves 8 TB of address space per
/// environment, and `cargo test` runs a test binary as threads in ONE process, so N concurrent
/// 8 TB reservations exhaust the address space and MDBX aborts env-open with ENOMEM ("Cannot
/// allocate memory (12)"). A temp chain never holds a full node's history, so a small map is safe
/// and lets many envs coexist. On tn-lmdb the same 2 GiB is the mapping's reservation, past which
/// the engine remaps rather than fails.
///
/// The file also grows in 4 MiB steps, reth's `test()` step (reth pairs its 8 TB default with a
/// 4 GiB step), so throwaway databases stay small on disk. On tn-lmdb it is the largest growth
/// step: the file doubles from 1 MiB up to it, then grows by it.
pub(crate) fn temp_chain_db_args() -> DatabaseArgs {
    /// The temp chain's map size.
    const TEMP_CHAIN_DB_MAX_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2 GiB
    /// The temp chain's growth step.
    const TEMP_CHAIN_DB_GROWTH_STEP: usize = 4 * 1024 * 1024; // 4 MiB

    DatabaseArgs {
        max_size: Some(TEMP_CHAIN_DB_MAX_SIZE),
        growth_step: Some(TEMP_CHAIN_DB_GROWTH_STEP),
        ..Default::default()
    }
}

/// The page size, in bytes, of a new reth database when `--db.page-size` is unset: the one source
/// `open_rw` and the callers that key data by the page size (the execution bench's cached
/// bases) both read, so they cannot drift.
///
/// The MDBX build's is reth's own default: the OS page size clamped to 4 KiB..=64 KiB (16 KiB on
/// Apple Silicon, 4 KiB on x86 Linux), which `open_rw` passes to MDBX explicitly.
#[cfg(not(feature = "tn-lmdb"))]
pub fn default_page_size() -> u32 {
    /// The smallest page size reth gives MDBX: smaller pages may not hold reth's largest rows.
    const MIN: usize = 4096;
    /// libmdbx's largest page size.
    const MAX: usize = 0x10000;
    // at most 64 KiB, so the cast is exact
    page_size::get().clamp(MIN, MAX) as u32
}

/// The page size, in bytes, of a new reth database when `--db.page-size` is unset: the one source
/// `open_rw` and the callers that key data by the page size (the execution bench's cached
/// bases) both read, so they cannot drift.
///
/// The tn-lmdb build's is the adapter's default (`tn_lmdb_reth::TnLmdbArgs::default()`, 4 KiB),
/// which `tn_lmdb_args` starts from.
#[cfg(feature = "tn-lmdb")]
pub fn default_page_size() -> u32 {
    tn_lmdb_reth::TnLmdbArgs::default().page_size
}

// The MDBX build's `default_page_size` reads the OS page size through `page_size`, as reth's own
// default does; the tn-lmdb build does not.
#[cfg(feature = "tn-lmdb")]
use page_size as _;

/// Open (or create) the read-write reth database in directory `path`, after [`check_backend`].
///
/// The MDBX build opens through reth's `init_db`, a new file with `--db.page-size` or else
/// [`default_page_size`], and records per-operation latency metrics (`with_metrics`; a noop unless
/// the global metrics recorder is installed, i.e. the node runs with `--metrics`).
#[cfg(not(feature = "tn-lmdb"))]
pub(crate) fn open_rw(reth_config: &RethConfig, path: &Path) -> eyre::Result<RethDb> {
    check_backend(path)?;
    let db_args = &reth_config.0.db;
    let page_size = db_args.page_size.unwrap_or(default_page_size() as usize);
    let args = db_args.database_args().with_geometry_page_size(Some(page_size));
    Ok(Arc::new(reth_db::init_db(path, args)?.with_metrics()))
}

/// Open (or create) the read-write reth database in directory `path`, after [`check_backend`].
///
/// The tn-lmdb build maps the `--db.*` flags ([`tn_lmdb_args`]) first, so a refused flag fails
/// before anything is read or written, and its [`check_backend`] migrates an MDBX database onto
/// tn-lmdb with them. It then opens as reth's `init_db` does: the database version file, every one
/// of reth's tables, and the client version.
#[cfg(feature = "tn-lmdb")]
pub(crate) fn open_rw(reth_config: &RethConfig, path: &Path) -> eyre::Result<RethDb> {
    let db_args = &reth_config.0.db;
    open_tn_lmdb(path, db_args, db_args.database_args().client_version().clone())
}

#[cfg(all(test, feature = "tn-lmdb"))]
thread_local! {
    /// Test-only: the tn-lmdb databases this thread opens through [`open_tn_lmdb`] commit with the
    /// engine's `NoSync` and no automatic syncs, which no CLI flag gives (`--db.sync-mode
    /// safe-no-sync` adds them: [`tn_lmdb_args`]). The persist bench's `nosync` mode sets it
    /// (`env::persist_bench`), so its no-sync rows time no sync at all.
    pub(crate) static TEST_NO_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`open_rw`] of the tn-lmdb build, with the client version to record.
#[cfg(feature = "tn-lmdb")]
fn open_tn_lmdb(
    path: &Path,
    db_args: &DatabaseArgs,
    client_version: reth_db::ClientVersion,
) -> eyre::Result<RethDb> {
    use tn_lmdb_reth::{TnLmdbEnv, TnLmdbEnvKind};

    let args = tn_lmdb_args(db_args)?;
    #[cfg(test)]
    let args = if TEST_NO_SYNC.get() {
        args.with_sync_mode(tn_lmdb_reth::SyncMode::NoSync)
            .with_sync_period(None)
            .with_sync_bytes(None)
    } else {
        args
    };
    check_backend(path, args)?;
    init_db_version_file(path)?;
    let mut env = TnLmdbEnv::open(path, TnLmdbEnvKind::RW, args).wrap_err_with(|| {
        format!("failed to open the tn-lmdb reth database at {}", path.display())
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
/// The MDBX build refuses `tn-lmdb.dat`, and a migrated directory whose `tn-lmdb.dat` is missing
/// (the marker [`MIGRATED_MARKER`] alone). It warns of a stopped migration's temp directory
/// ([`MIGRATING_DIR`], a partial copy beside an unchanged MDBX database), naming it and its size,
/// and never removes it.
#[cfg(not(feature = "tn-lmdb"))]
fn check_backend(path: &Path) -> eyre::Result<()> {
    let tn_lmdb = path.join(TN_LMDB_DATA_FILE);
    if file_exists(&tn_lmdb)? {
        eyre::bail!(
            "the reth database at {} is a tn-lmdb database ({} exists), but this build runs \
             reth on MDBX: run a build with the `tn-lmdb` feature, or start from a fresh datadir \
             (or `db load-state`)",
            path.display(),
            tn_lmdb.display(),
        );
    }
    let marker = path.join(MIGRATED_MARKER);
    if file_exists(&marker)? {
        eyre::bail!(
            "the reth database at {} was migrated from MDBX to tn-lmdb ({} exists), but {} is \
             missing, and this build runs reth on MDBX: it does not create a new database beside \
             the datadir's static files in its place; restore the file and run a build with the \
             `tn-lmdb` feature, or start from a fresh datadir (or `db load-state`)",
            path.display(),
            marker.display(),
            tn_lmdb.display(),
        );
    }
    let temp = path.join(MIGRATING_DIR);
    if file_exists(&temp)? {
        let size = match dir_bytes(&temp) {
            Ok(bytes) => format!("{bytes} bytes"),
            Err(e) => format!("unknown ({e})"),
        };
        tracing::warn!(target: "tn::reth", dir = %temp.display(), %size, "a stopped MDBX to tn-lmdb migration left its temp directory, a partial copy (the MDBX database is unchanged): this build never removes it; remove it by hand to free its space");
    }
    Ok(())
}

/// The bytes of the files directly in `dir`: a migration's temp directory holds no directories.
#[cfg(not(feature = "tn-lmdb"))]
fn dir_bytes(dir: &Path) -> std::io::Result<u64> {
    std::fs::read_dir(dir)?
        .try_fold(0u64, |sum, entry| Ok(sum.saturating_add(entry?.metadata()?.len())))
}

/// Never open a database directory that holds the other engine's data file as it is.
///
/// Opening the wrong engine would create an empty database beside the datadir's static files,
/// and reth's `check_consistency` then prunes the static files back to the empty database's tip:
/// the node would lose its chain. Each check is `try_exists`, so a stat error fails the open and
/// is never read as an absent file.
///
/// The tn-lmdb build moves an MDBX database (`mdbx.dat`) onto tn-lmdb instead, through
/// `migrate::migrate_from_mdbx` (its module docs list the states and steps), with `args`' page
/// size, reservation and growth step. It also finishes a migration stopped after its commit point,
/// refuses an `mdbx.dat` written after a migration or beside a `tn-lmdb.dat` no migration left (or
/// a migrated database whose `tn-lmdb.dat` is missing), and removes a stopped migration's temp
/// directory.
#[cfg(feature = "tn-lmdb")]
fn check_backend(path: &Path, args: tn_lmdb_reth::TnLmdbArgs) -> eyre::Result<()> {
    migrate::migrate_from_mdbx(path, args)
}

/// Refuse the database directory `path`, before anything opens it, if this build's open would
/// first migrate it: for `db load-state`, which restores into a fresh datadir and must not move a
/// node's database onto another engine before it refuses the datadir. A stat error fails it.
///
/// The MDBX build never migrates, so it refuses nothing here: its open refuses a tn-lmdb or
/// migrated directory before creating anything.
#[cfg(not(feature = "tn-lmdb"))]
pub fn refuse_pending_migration(path: &Path) -> eyre::Result<()> {
    let _ = path;
    Ok(())
}

/// Refuse the database directory `path`, before anything opens it, if this build's open would
/// first migrate it: for `db load-state`, which restores into a fresh datadir and must not move a
/// node's database onto another engine before it refuses the datadir. A stat error fails it.
///
/// The tn-lmdb build refuses a directory holding MDBX's data file (`mdbx.dat`), whatever its
/// database holds: the open would migrate it, or finish a stopped migration, which removes MDBX's
/// files.
#[cfg(feature = "tn-lmdb")]
pub fn refuse_pending_migration(path: &Path) -> eyre::Result<()> {
    let mdbx = path.join(MDBX_DATA_FILE);
    if file_exists(&mdbx)? {
        eyre::bail!(
            "the reth database at {} holds an MDBX database ({} exists), which this build's open \
             would first migrate to tn-lmdb: refused before anything opens it",
            path.display(),
            mdbx.display(),
        );
    }
    Ok(())
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
/// The tn-lmdb build cannot: its read-only open is refused while another handle holds the database
/// with write access, so a caller holds the datadir lock for the report, and a node starting
/// meanwhile is refused at that lock rather than at its database open.
pub const DB_TABLE_STATS_NEEDS_STOPPED_NODE: bool = cfg!(feature = "tn-lmdb");

/// The figures of each of reth's tables in the database in directory `path`, sorted by name. A
/// table the database lacks is skipped, so an older database whose schema predates a table later
/// added to reth's tables still reports.
///
/// The MDBX build opens read-only through reth's `open_db_read_only`, which works while a node
/// runs. It first refuses a directory holding a tn-lmdb database, or a migrated one, as the
/// backend guard does.
#[cfg(not(feature = "tn-lmdb"))]
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
/// `NotFound`. The MDBX build's [`db_table_stats`] reports them; the tn-lmdb build's migration sums
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
/// The tn-lmdb build opens read-only, which needs the node stopped
/// ([`DB_TABLE_STATS_NEEDS_STOPPED_NODE`]): a database another handle holds with write access is
/// refused with an error saying to stop the node. It never migrates: a directory holding an MDBX
/// database (`mdbx.dat`) is refused, as one whose migration to tn-lmdb is pending or unfinished
/// (the node's open runs it), or, beside a finished migration's marker or a `tn-lmdb.dat` no
/// migration left, as the node's open refuses it. With neither `mdbx.dat` nor `tn-lmdb.dat` there
/// is no reth database at `path`.
#[cfg(feature = "tn-lmdb")]
pub fn db_table_stats(path: &Path) -> eyre::Result<Vec<DbTableStat>> {
    use reth_db::{mdbx::Error as MdbxError, DatabaseError};
    use tn_lmdb_reth::{TnLmdbArgs, TnLmdbEnv, TnLmdbEnvKind, DATA_FILE};

    let mdbx = path.join(MDBX_DATA_FILE);
    if file_exists(&mdbx)? {
        if file_exists(&path.join(MIGRATED_MARKER))? {
            return Err(migrate::written_after_migration(path));
        }
        if file_exists(&path.join(DATA_FILE))? && !migrate::stopped_after_commit_point(path)? {
            return Err(migrate::both_engines(path));
        }
        eyre::bail!(
            "the reth database at {} holds an MDBX database ({} exists): its MDBX to tn-lmdb \
             migration is pending or unfinished; start the node to run it",
            path.display(),
            mdbx.display(),
        );
    }
    let tn_lmdb = path.join(DATA_FILE);
    if !file_exists(&tn_lmdb)? {
        eyre::bail!(
            "no reth database at {}: neither {} nor {} exists",
            path.display(),
            tn_lmdb.display(),
            mdbx.display(),
        );
    }

    // The adapter reports a database another handle holds as MDBX's `MDBX_BUSY`.
    let busy = MdbxError::Busy.to_err_code();
    let env =
        TnLmdbEnv::open(path, TnLmdbEnvKind::RO, TnLmdbArgs::default()).map_err(|e| match e {
            DatabaseError::Open(info) if info.code == busy => {
                eyre::eyre!(
                    "the reth database at {} is in use: stop the node first",
                    path.display()
                )
            }
            e => eyre::Report::new(e).wrap_err(format!(
                "failed to open the tn-lmdb reth database at {}",
                path.display()
            )),
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

/// Map reth's `--db.*` flags onto the tn-lmdb adapter's settings.
///
/// - `--db.max-size` is the address space the mapping reserves (`map_reserve`); unset, the engine's
///   default. Past the reservation the engine remaps at double the size, so it is a reservation,
///   not a cap on the database's size.
/// - `--db.page-size` is the page size of a new file; the engine validates it (a power of two in 4
///   KiB..=64 KiB). A size that does not fit a `u32` is refused here.
/// - `--db.sync-mode`: `durable` is durable commits (the default). `safe-no-sync` is the engine's
///   `NoSync` with automatic syncs: a commit returns before it is durable, and the first commit
///   made at least [`SAFE_NO_SYNC_PERIOD`] after the last durable state, or whose pages bring those
///   written since then to [`SAFE_NO_SYNC_BYTES`], commits durably, with every commit before it
///   (the engine's `sync_period` and `sync_bytes`). A crash, of the system or of the process alone,
///   rolls back to the last durable state and never corrupts the file; the automatic syncs bound
///   what it loses and how long the pages commits free wait before they are reused, so the file
///   stays bounded. reth's `UtterlyNoSync` (no CLI value) is refused: the engine has no mode that
///   never syncs. reth's `NoMetaSync` (no CLI value either) is refused.
/// - `--db.growth-step` is the largest growth step (`max_growth`; unset, the engine's 1 GiB): the
///   file grows by doubling up to it, then by it, each step preallocated. A step under the engine's
///   minimum (`MIN_GROWTH`, 1 MiB) is refused here.
/// - `--db.exclusive`, `--db.read-transaction-timeout`, `--db.max-readers` and `--db.log-level`
///   have no tn-lmdb equivalent and are refused when set.
#[cfg(feature = "tn-lmdb")]
fn tn_lmdb_args(args: &DatabaseArgs) -> eyre::Result<tn_lmdb_reth::TnLmdbArgs> {
    use reth_db::mdbx::SyncMode as MdbxSyncMode;
    use tn_lmdb_reth::{SyncMode, TnLmdbArgs, MIN_GROWTH};

    let refuse = |flag: &str| eyre::eyre!("{flag} is not supported by the tn-lmdb backend");
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
    if log_level.is_some() {
        return Err(refuse("--db.log-level"));
    }

    let mut tn_lmdb = TnLmdbArgs::default();
    if let Some(max_size) = max_size {
        tn_lmdb = tn_lmdb.with_map_reserve(
            u64::try_from(max_size).wrap_err("--db.max-size does not fit in 64 bits")?,
        );
    }
    if let Some(page_size) = page_size {
        let page_size = u32::try_from(page_size).map_err(|_| {
            eyre::eyre!("--db.page-size {page_size} is too large for the tn-lmdb backend")
        })?;
        tn_lmdb = tn_lmdb.with_page_size(page_size);
    }
    if let Some(growth_step) = growth_step {
        let step =
            u64::try_from(growth_step).wrap_err("--db.growth-step does not fit in 64 bits")?;
        if step < MIN_GROWTH {
            return Err(eyre::eyre!(
                "--db.growth-step {growth_step} is under the tn-lmdb backend's minimum of \
                 {MIN_GROWTH} bytes"
            ));
        }
        tn_lmdb = tn_lmdb.with_max_growth(step);
    }
    if let Some(sync_mode) = sync_mode {
        tn_lmdb = match sync_mode {
            MdbxSyncMode::Durable => tn_lmdb.with_sync_mode(SyncMode::Durable),
            MdbxSyncMode::SafeNoSync => tn_lmdb
                .with_sync_mode(SyncMode::NoSync)
                .with_sync_period(Some(SAFE_NO_SYNC_PERIOD))
                .with_sync_bytes(Some(SAFE_NO_SYNC_BYTES)),
            MdbxSyncMode::UtterlyNoSync => {
                return Err(eyre::eyre!(
                    "the UtterlyNoSync sync mode is not supported by the tn-lmdb backend: it has \
                     no mode that never syncs; use --db.sync-mode safe-no-sync or durable"
                ))
            }
            MdbxSyncMode::NoMetaSync => return Err(refuse("the NoMetaSync sync mode")),
        };
    }
    Ok(tn_lmdb)
}

/// `--db.sync-mode safe-no-sync`'s automatic sync by time on tn-lmdb (the engine's
/// `sync_period`): a commit made at least this long after the last durable state commits durably,
/// with every commit since. A crash, of the system or of the process alone, loses at most about
/// this long's commits (those made within it of the last durable state), and the pages commits
/// free wait at most about two of these before they are reused. Checked at commit: commits that
/// then stop wait for the next commit or the close.
#[cfg(feature = "tn-lmdb")]
const SAFE_NO_SYNC_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);

/// `--db.sync-mode safe-no-sync`'s automatic sync by size on tn-lmdb (the engine's `sync_bytes`):
/// a commit whose pages bring those stamped since the last durable state to this many bytes
/// commits durably, with every commit since. A crash, of the system or of the process alone, loses
/// at most about this many bytes of commits, and the pages commits free wait at most about two of
/// these before they are reused, so they hold at most about twice this beyond the live pages.
#[cfg(feature = "tn-lmdb")]
const SAFE_NO_SYNC_BYTES: u64 = 64 << 20;

/// Create or check reth's database version file in `path`, as reth's `create_db` does: an absent
/// or empty directory is created and gets the file; otherwise the file must hold reth's version,
/// and a missing one is written.
#[cfg(feature = "tn-lmdb")]
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
#[cfg(feature = "tn-lmdb")]
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
#[cfg(feature = "tn-lmdb")]
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
