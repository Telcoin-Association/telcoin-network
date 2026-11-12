//! The MDBX to tn-lmdb migration of TN's reth database, in the tn-lmdb (default) build.
//!
//! A node built without the `mdbx` feature that finds an MDBX reth database (`<db>/mdbx.dat`)
//! moves it onto tn-lmdb when it first opens the database, before anything else reads the
//! directory: the backend guard calls [`migrate_from_mdbx`]. The copy is raw: each of reth's
//! tables is walked on MDBX as its encoded keys and compressed values (`RawTable`) and appended to
//! tn-lmdb unchanged, so no row is decoded. reth's static files are not touched; its
//! `database.version` file gets tn-lmdb's version at the commit point (the rollback tripwire,
//! `TN_LMDB_DB_VERSION`), and the open that follows records the client version as on any open,
//! after the `VersionHistory` rows the copy brought over.
//!
//! **States.** The directory's files decide what an open does. Each is looked for with
//! `symlink_metadata` (a symbolic link counts as present, dangling or not), so a stat error fails
//! the open:
//!
//! | `mdbx.dat` | `tn-lmdb.dat` | `mdbx-migrated` | the open |
//! |---|---|---|---|
//! | yes | no | no | migrates |
//! | yes | yes | no, and the temp directory without a `tn-lmdb.dat` | a migration stopped after its commit point: finishes it (removes MDBX's files, writes the marker), unless `mdbx.dat` differs from the record the migration took of it ([`MDBX_SOURCE_RECORD`] in the temp directory): refused, both files kept |
//! | yes | yes | no, nor that temp directory | refused, nothing changed: both engines' data files, not a stopped migration (a tn-lmdb database an older binary ran on MDBX?) |
//! | yes | either | yes | refused: something wrote MDBX after the migration (an older binary), and the static files may no longer match tn-lmdb |
//! | no | yes | no, and the temp directory without a `tn-lmdb.dat` | a migration stopped after removing MDBX's files: finishes it (writes the marker) |
//! | no | yes | either | a normal open |
//! | no | no | yes | refused: the migrated database is missing |
//! | no | no | no | a normal open, of a new database |
//!
//! The temp directory a migration builds in ([`MIGRATING_DIR`]) is live only while the migration
//! runs, and the node holds its datadir lock while it opens the database, so one found by an open
//! is a stopped migration's. Before the commit point it holds a partial or unrenamed copy, and is
//! removed; after it, emptied by the rename, it is kept until the marker is written, so beside
//! `tn-lmdb.dat` without the marker it says a migration stopped after its commit point. Without
//! it, an `mdbx.dat` beside `tn-lmdb.dat` is no migration's, and is refused.
//!
//! The datadir lock is one datadir's, and two datadirs can name one database directory, so while
//! `mdbx.dat` or the temp directory is there the open also locks the database directory itself
//! (an exclusive `flock` on the directory, [`lock_db_dir`], held until it returns): a second
//! process migrating or finishing the same files is refused.
//!
//! **Steps.**
//! 1. Remove a stopped migration's temp directory.
//! 2. Open MDBX read-only ([`open_mdbx_read_only`]; an environment stopped uncleanly under
//!    `--db.sync-mode safe-no-sync` is refused by MDBX with `MDBX_WANNA_RECOVERY`, and the error
//!    says to run the previous binary once and stop it cleanly), and sum the live bytes of reth's
//!    tables in it: their branch, leaf and overflow pages times the page size, as `db stats`
//!    reports them on the MDBX build ([`mdbx_live_bytes`]). Warn of any named database in it that
//!    is not one of reth's tables: its rows are not copied (reth does not read them either).
//! 3. Check the free disk: the live bytes plus headroom ([`needed_bytes`]), else refuse, naming the
//!    live bytes and the bytes needed and free. `mdbx.dat`'s size is no measure: the file grows in
//!    large steps (a local node's is 4 GiB, mostly sparse) and keeps the pages it frees.
//! 4. Open tn-lmdb in the temp directory with `NoSync` (no automatic syncs, whatever
//!    `--db.sync-mode`) and the page size, reservation and growth step of the node's `--db.*`
//!    flags, and create reth's tables.
//! 5. Copy each of reth's tables (`Tables::ALL`), read from MDBX in one transaction with the
//!    long-read safety off, by appends, in write transactions of at most [`COMMIT_ROWS`] rows or
//!    [`COMMIT_BYTES`] bytes, hashing each row's key and value as it goes (std's `DefaultHasher`,
//!    SipHash with fixed keys: the same in both passes of one process, which is all the comparison
//!    needs). MDBX walks a table in its key order, and a dup-sort table's values in their byte
//!    order, which is the order of the adapter's composite rows (key, then subkey), so every row
//!    takes the engine's append. Any refused row (out of order, a second value for a dup row's
//!    subkey, a size the adapter cannot store) fails the migration.
//! 6. Verify: each table's rows copied equal MDBX's count of them (`db_stat`), so a short read
//!    fails; a read-back pass over each tn-lmdb table hashes equal to its copy; then
//!    `TnLmdbEnv::sync`, the engine's full check of the file (`TnLmdbEnv::check`), and its entry
//!    count of each table equal to the rows copied.
//! 7. Close the tn-lmdb file, record `mdbx.dat`'s length and modification time, with MDBX's last
//!    transaction id, in the temp directory ([`MDBX_SOURCE_RECORD`], fsynced), fsync the temp
//!    directory, and rename its `tn-lmdb.dat` into the database directory: the commit point. Then
//!    fsync the database directory, and record tn-lmdb's version in reth's `database.version`
//!    (`TN_LMDB_DB_VERSION`, the rollback tripwire), so a build running reth on MDBX alone refuses
//!    the directory from here on and never advances the `mdbx.dat` that step 8 removes.
//! 8. Fsync the database directory again (a later open finishing a stopped migration starts here,
//!    and the rename must be durable before MDBX goes), then remove MDBX's `mdbx.lck`, then
//!    `mdbx.dat` last (beside `tn-lmdb.dat`, its presence marks a migration unfinished), and fsync
//!    the directory. Before removing them, compare `mdbx.dat`'s length and modification time with
//!    the record of step 7 (a stat, since `mdbx.lck` may already be gone): a stop between the
//!    rename and the version file leaves a window in which a build running reth on MDBX alone can
//!    open and advance `mdbx.dat`, and removing it then would silently roll those writes back, so a
//!    differing file is refused and both files are kept. A temp directory without the record (an
//!    older build's migration) is finished as before.
//! 9. Write the marker [`MIGRATED_MARKER`] atomically (a temp file, fsync, rename, fsync of the
//!    directory). Its text lists the rows copied per table.
//! 10. Remove the temp directory, the sign of an unfinished migration until the marker is written.
//!
//! A failure before the commit point removes the temp directory and leaves MDBX as it was. A stop
//! there (a kill, a power loss) leaves MDBX in place too, and the next open migrates again from the
//! start; a stop after the rename leaves `tn-lmdb.dat` beside the emptied temp directory (and
//! MDBX's files until they are removed), and the next open finishes the migration. A failure after
//! the rename (the database directory's fsync, or a step of finishing) keeps the temp directory as
//! a stop does, and its error says the next start finishes the migration.
//!
//! **Why a temp directory.** A copy stopped part way cannot resume into the same file: the
//! adapter's appends are not idempotent (a row appended again is refused, or replaces the last),
//! and a partial `tn-lmdb.dat` in the database directory would open as a short database beside the
//! static files. So the copy is built aside, and moved into place by one rename once it is
//! verified and durable.
//!
//! **Disk.** While it copies, the filesystem holds both databases: MDBX's file, and a copy about
//! the size of its tables' live pages.
//!
//! **No rollback.** Once MDBX's files are removed there is no way back to MDBX: a problem after
//! the migration is fixed forward, as for the consensus DB.

use std::{
    hash::{DefaultHasher, Hasher as _},
    io::Write as _,
    path::Path,
    time::{Instant, UNIX_EPOCH},
};

use eyre::WrapErr as _;
use reth_db::{
    cursor::{DbCursorRO as _, DbCursorRW as _},
    mdbx::{tx::Tx, DatabaseArguments, Error as MdbxError, RO},
    table::Table,
    tables::{RawTable, TableViewer, Tables},
    transaction::{DbTx, DbTxMut as _},
    Database, DatabaseEnv, DatabaseError,
};
use tn_lmdb_reth::{SyncMode, TnLmdbArgs, TnLmdbEnv, TnLmdbEnvKind, DATA_FILE};
use tn_storage::disk::{available_bytes, fsync_directory, fsync_file};
use tracing::{info, warn};

use super::{file_exists, mdbx_table_stat, MDBX_DATA_FILE, MIGRATED_MARKER, MIGRATING_DIR};

#[cfg(test)]
mod tests;

/// MDBX's lock file, removed before its data file.
const MDBX_LOCK_FILE: &str = "mdbx.lck";

/// The record a migration keeps of its source in the temp directory ([`MdbxSource`]).
const MDBX_SOURCE_RECORD: &str = "mdbx-source";

/// The rows a write transaction of the copy takes before it commits. With [`COMMIT_BYTES`], it
/// bounds what each commit writes; a `NoSync` commit is cheap, so the bounds need not be tight.
/// Tests take 1000, so their tables' copies span several transactions.
const COMMIT_ROWS: u64 = if cfg!(test) { 1000 } else { 100_000 };

/// The key and value bytes a write transaction of the copy takes before it commits.
const COMMIT_BYTES: u64 = 64 << 20;

/// A table's copy logs its progress every this many rows.
const PROGRESS_ROWS: u64 = 10_000_000;

/// The least fixed part of the headroom a migration needs on the disk ([`needed_bytes`]).
const HEADROOM_FLOOR: u64 = 1 << 30;

/// The named steps of a migration, at which a test can stop it as a kill would (the test-only
/// `STOP_AT`), or fail it as an I/O error would (`FAIL_AT`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// After a commit of the copy: rows are in the temp directory's file.
    MidCopy,
    /// Verified, synced and closed, before the rename (the commit point).
    BeforeRename,
    /// After the rename, before the database directory's fsync and the removal of MDBX's files.
    AfterRename,
    /// After MDBX's files are removed, before the marker is written.
    BeforeMarker,
}

/// Test-only: the error of a migration stopped at a [`Step`] by `STOP_AT`. Its cleanup is skipped,
/// as a kill would skip it.
#[cfg(test)]
#[derive(Debug)]
pub(super) struct Killed(pub(super) Step);

#[cfg(test)]
impl std::fmt::Display for Killed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the migration was stopped at {:?} by a test", self.0)
    }
}

#[cfg(test)]
impl std::error::Error for Killed {}

/// Test-only: a fault the copy makes in one row of a table, which its verification must catch.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CopyFault {
    /// The table's first row is neither copied nor counted, as a short read would lose it.
    SkipRow(&'static str),
    /// The table's first row is copied with its value's last byte changed, as a bad write would
    /// change it; its hash is taken from MDBX's bytes.
    FlipValue(&'static str),
}

#[cfg(test)]
thread_local! {
    /// Test-only: the step at which the next migration on this thread stops ([`Killed`]).
    pub(super) static STOP_AT: std::cell::Cell<Option<Step>> = const { std::cell::Cell::new(None) };
    /// Test-only: the passes of the `STOP_AT` step to let by before stopping there.
    pub(super) static STOP_PASSES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Test-only: the step at which the next migration on this thread fails with an ordinary error,
    /// as a failed I/O call would fail it: unlike a [`Killed`] stop, the failure's cleanup runs.
    pub(super) static FAIL_AT: std::cell::Cell<Option<Step>> = const { std::cell::Cell::new(None) };
    /// Test-only: the free bytes the migrations on this thread read in place of the filesystem's.
    pub(super) static FREE_BYTES: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
    /// Test-only: the fault the next migration on this thread makes ([`CopyFault`]).
    pub(super) static COPY_FAULT: std::cell::Cell<Option<CopyFault>> =
        const { std::cell::Cell::new(None) };
}

/// Pass step `at`; a test stops the migration there through `STOP_AT` (after `STOP_PASSES`
/// passes of it), or fails it there through `FAIL_AT`.
fn step(at: Step) -> eyre::Result<()> {
    #[cfg(test)]
    if FAIL_AT.get() == Some(at) {
        FAIL_AT.set(None);
        eyre::bail!("the migration was failed at {at:?} by a test");
    }
    #[cfg(test)]
    if STOP_AT.get() == Some(at) {
        match STOP_PASSES.get() {
            0 => {
                STOP_AT.set(None);
                return Err(Killed(at).into());
            }
            passes => STOP_PASSES.set(passes - 1),
        }
    }
    let _ = at;
    Ok(())
}

/// Test-only: whether the copy makes `fault` now (once).
#[cfg(test)]
fn take_fault(fault: CopyFault) -> bool {
    COPY_FAULT.with(|armed| {
        let hit = armed.get() == Some(fault);
        if hit {
            armed.set(None);
        }
        hit
    })
}

/// Settle the reth database directory `path` before the tn-lmdb build opens it (the module docs'
/// states): migrate an MDBX database, finish a migration stopped after its commit point, refuse
/// MDBX written after a migration or beside a tn-lmdb database no migration left, and remove a
/// stopped migration's temp directory. `args` are the settings the node opens tn-lmdb with: a
/// migrated file takes their page size, reservation and growth step.
pub(super) fn migrate_from_mdbx(path: &Path, args: TnLmdbArgs) -> eyre::Result<()> {
    let mdbx = file_exists(&path.join(MDBX_DATA_FILE))?;
    let tn_lmdb = file_exists(&path.join(DATA_FILE))?;
    let marker = file_exists(&path.join(MIGRATED_MARKER))?;
    // Held until this returns: a migration, a finish or a refusal runs alone on the directory.
    let _lock =
        (mdbx || file_exists(&path.join(MIGRATING_DIR))?).then(|| lock_db_dir(path)).transpose()?;
    match (mdbx, tn_lmdb, marker) {
        (true, _, true) => Err(written_after_migration(path)),
        (true, false, false) => migrate(path, args),
        (true, true, false) if stopped_after_commit_point(path)? => {
            info!(target: "tn::reth", path = %path.display(), "finishing an MDBX to tn-lmdb migration stopped after its commit point");
            finish(path, FINISHED_LATER)
        }
        (true, true, false) => Err(both_engines(path)),
        (false, true, false) if stopped_after_commit_point(path)? => {
            info!(target: "tn::reth", path = %path.display(), "finishing an MDBX to tn-lmdb migration stopped before its marker");
            finish(path, FINISHED_LATER)
        }
        (false, false, true) => eyre::bail!(
            "the reth database at {} was migrated from MDBX to tn-lmdb ({} exists), but {} is \
             missing: this build does not create a new database beside the datadir's static files \
             in its place; restore the file, or start from a fresh datadir (or `db load-state`)",
            path.display(),
            path.join(MIGRATED_MARKER).display(),
            path.join(DATA_FILE).display(),
        ),
        (false, _, _) => remove_temp_dir(path),
    }
}

/// Lock the database directory `path` against another process migrating it: an exclusive,
/// non-blocking OS file lock on the directory itself (`flock` on unix, the primitive the datadir's
/// `PidLock` uses), held until the returned handle is dropped, which a crash does with the process.
/// The datadir lock does not serve: two datadirs can name one database directory, and their nodes
/// would migrate, or migrate and finish, the same files at once. The directory is the lock, not a
/// file in it, which would make an empty directory look like a database (`database_is_empty`).
fn lock_db_dir(path: &Path) -> eyre::Result<std::fs::File> {
    let dir = std::fs::File::open(path)
        .wrap_err_with(|| format!("failed to open directory {} to lock it", path.display()))?;
    match dir.try_lock() {
        Ok(()) => Ok(dir),
        Err(std::fs::TryLockError::WouldBlock) => eyre::bail!(
            "another process is migrating this database: the reth database directory {} is \
             locked by the MDBX to tn-lmdb migration of another process (a second datadir naming \
             the same directory?); wait for it to finish, then start again",
            path.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).wrap_err_with(|| format!("failed to lock directory {}", path.display()))
        }
    }
}

/// The marker's text when an open finishes a migration another run stopped after its commit point.
const FINISHED_LATER: &str =
    "migrated from MDBX to tn-lmdb; finished by a later open (the run that copied the rows logged \
     them)\n";

/// Whether `path`, holding `tn-lmdb.dat` and no marker, holds a migration stopped after its commit
/// point (before or after it removed MDBX's files): its temp directory, which a migration removes
/// only after the marker, is there without a `tn-lmdb.dat` (the file moved out of it at the commit
/// point).
pub(super) fn stopped_after_commit_point(path: &Path) -> eyre::Result<bool> {
    let temp = path.join(MIGRATING_DIR);
    Ok(file_exists(&temp)? && !file_exists(&temp.join(DATA_FILE))?)
}

/// The refusal of an `mdbx.dat` beside a `tn-lmdb.dat` in `path` that no migration left (no marker,
/// no emptied temp directory): a tn-lmdb database a build running reth on MDBX opened, say, which
/// creates MDBX's file beside it. Either file may be the one the static files match, so neither is
/// removed.
pub(super) fn both_engines(path: &Path) -> eyre::Report {
    eyre::eyre!(
        "the reth database at {} holds both {} and {}, with neither a finished MDBX to tn-lmdb \
         migration's marker ({}) nor a stopped one's emptied temp directory ({}): this is not a \
         stopped migration, so this build changes neither file. Resolve it by hand (start from a \
         fresh datadir, or `db load-state`)",
        path.display(),
        path.join(MDBX_DATA_FILE).display(),
        path.join(DATA_FILE).display(),
        path.join(MIGRATED_MARKER).display(),
        path.join(MIGRATING_DIR).display(),
    )
}

/// What a migration records of `mdbx.dat` before its commit point ([`MDBX_SOURCE_RECORD`] in the
/// temp directory), so that [`finish`] can tell whether the file was written since: its length and
/// modification time as `stat` gives them (nanoseconds since the Unix epoch), and MDBX's last
/// transaction id, read while the source was open. `finish` compares the stat alone: it may run
/// after `mdbx.lck` is gone, when the file is not opened as MDBX again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MdbxSource {
    /// `mdbx.dat`'s length.
    len: u64,
    /// `mdbx.dat`'s modification time, in nanoseconds since the Unix epoch.
    mtime_nanos: u128,
    /// MDBX's last transaction id (`Info::last_txnid`), for the record and the refusal's text.
    last_txnid: u64,
}

impl MdbxSource {
    /// Stat `mdbx`, with `last_txnid` as read from the open source.
    fn stat(mdbx: &Path, last_txnid: u64) -> eyre::Result<Self> {
        let meta = std::fs::metadata(mdbx)
            .wrap_err_with(|| format!("failed to stat {}", mdbx.display()))?;
        let mtime = meta.modified().wrap_err_with(|| {
            format!("failed to read the modification time of {}", mdbx.display())
        })?;
        let mtime_nanos = mtime
            .duration_since(UNIX_EPOCH)
            .wrap_err_with(|| format!("{} is modified before the Unix epoch", mdbx.display()))?
            .as_nanos();
        Ok(Self { len: meta.len(), mtime_nanos, last_txnid })
    }

    /// Write the record into the temp directory `temp`, fsynced; the caller fsyncs the directory.
    fn write(&self, temp: &Path) -> eyre::Result<()> {
        let record = temp.join(MDBX_SOURCE_RECORD);
        let write = || -> std::io::Result<()> {
            let mut file = std::fs::File::create(&record)?;
            let Self { len, mtime_nanos, last_txnid } = self;
            write!(file, "len {len}\nmtime_nanos {mtime_nanos}\nlast_txnid {last_txnid}\n")?;
            fsync_file(&file)
        };
        write().wrap_err_with(|| format!("failed to write {}", record.display()))
    }

    /// Read the record from the temp directory `temp`: `None` without one (a migration of a build
    /// from before the record). One that does not parse is an error.
    fn read(temp: &Path) -> eyre::Result<Option<Self>> {
        let record = temp.join(MDBX_SOURCE_RECORD);
        let text = match std::fs::read_to_string(&record) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(e).wrap_err_with(|| format!("failed to read {}", record.display()))
            }
        };
        let malformed = || eyre::eyre!("{} does not parse: {text:?}", record.display());
        let mut fields = text.lines().map(|line| line.split_once(' ').ok_or_else(malformed));
        let mut next = |name: &str| -> eyre::Result<&str> {
            let (key, value) = fields.next().ok_or_else(malformed)??;
            if key != name {
                return Err(malformed());
            }
            Ok(value)
        };
        let len = next("len")?.parse().map_err(|_| malformed())?;
        let mtime_nanos = next("mtime_nanos")?.parse().map_err(|_| malformed())?;
        let last_txnid = next("last_txnid")?.parse().map_err(|_| malformed())?;
        Ok(Some(Self { len, mtime_nanos, last_txnid }))
    }
}

/// The refusal, when finishing a migration stopped after its commit point in `path`, of an
/// `mdbx.dat` whose length or modification time (`now`) differs from the record the migration
/// took of it before the commit point (`recorded`): a build running reth on MDBX alone wrote it in
/// the window before the version file, and removing it would silently roll those writes back.
/// Neither file is removed.
fn written_after_commit_point(path: &Path, recorded: MdbxSource, now: MdbxSource) -> eyre::Report {
    eyre::eyre!(
        "the reth database at {} holds an MDBX to tn-lmdb migration stopped after its commit point \
         ({} is in place), but {} was written since: the migration recorded it at {} bytes, \
         modified at {} ns (MDBX transaction {}), and it is now {} bytes, modified at {} ns. A \
         build running reth on MDBX (an older binary?) ran on it, so the static files may no \
         longer match the tn-lmdb database: this build does not open it, and removes neither \
         file. Resolve it by hand (start from a fresh datadir, or `db load-state`)",
        path.display(),
        path.join(DATA_FILE).display(),
        path.join(MDBX_DATA_FILE).display(),
        recorded.len,
        recorded.mtime_nanos,
        recorded.last_txnid,
        now.len,
        now.mtime_nanos,
    )
}

/// The refusal of an `mdbx.dat` beside the marker of a finished migration in `path`.
pub(super) fn written_after_migration(path: &Path) -> eyre::Report {
    eyre::eyre!(
        "the reth database at {} was migrated from MDBX to tn-lmdb ({} exists), but {} was written \
         since, by a build running reth on MDBX (an older binary?): the static files may no longer \
         match the tn-lmdb database, so this build does not open it. Resolve it by hand (start from \
         a fresh datadir, or `db load-state`)",
        path.display(),
        path.join(MIGRATED_MARKER).display(),
        path.join(MDBX_DATA_FILE).display(),
    )
}

/// The free bytes a migration needs on the database's filesystem: `live`, the bytes of the pages
/// reth's tables take in MDBX ([`mdbx_live_bytes`]), which hold the rows the copy writes, plus
/// headroom of a quarter of it and the larger of [`HEADROOM_FLOOR`] (1 GiB) and `max_growth`, the
/// copy's largest growth step (`--db.growth-step`), which the file's last growth can take whole.
/// The quarter is a margin, not a measurement. The copy appends every row in key order, so its
/// leaves fill as the engine's append fills them, where MDBX's tree keeps whatever fill its own
/// writes and splits left; each engine keeps a value in the leaf up to its own limit (tn-lmdb's
/// about half a page, `Geometry::max_value`) and the rest in overflow runs of whole pages, so the
/// two files' page counts differ either way; the tn-lmdb file also holds its catalog and free
/// tree; and the filesystem needs room to work. The on-demand `migrates_a_copy_of_a_real_database`
/// prints the copy's table pages against the live bytes, and `the_migrated_tables_fit_the_headroom`
/// holds the test database within the margin. The check is an early refusal, not a promise: a
/// disk that fills during the copy fails the migration, which removes its temp directory and
/// leaves MDBX as it was.
pub(super) const fn needed_bytes(live: u64, max_growth: u64) -> u64 {
    let fixed = if max_growth > HEADROOM_FLOOR { max_growth } else { HEADROOM_FLOOR };
    live.saturating_add(live / 4).saturating_add(fixed)
}

/// Refuse an MDBX database that is not in storage layout v1, before anything is copied: the
/// migration changes the engine, never the layout, and every MDBX database a TN build writes is v1
/// (v2 is tn-lmdb only). A `storage_settings` row or TN layout marker that does not parse, or a
/// marker of another format version, is refused too. The open after a migration checks the same
/// rows (`env::layout`), but by then MDBX's files are gone.
fn refuse_non_v1_source(mdbx: &DatabaseEnv) -> eyre::Result<()> {
    use crate::env::layout::{self, StorageLayout};

    let tx = mdbx.tx().wrap_err("failed to begin a read of the MDBX database")?;
    let row = |key: &str| -> eyre::Result<Option<Vec<u8>>> {
        match tx.get::<reth_db::tables::Metadata>(key.to_string()) {
            Ok(row) => Ok(row),
            // a database written before reth added the table has no rows in it
            Err(DatabaseError::Open(info)) if info.code == MdbxError::NotFound.to_err_code() => {
                Ok(None)
            }
            Err(e) => Err(eyre::Report::new(e).wrap_err("failed to read the MDBX Metadata table")),
        }
    };
    let stored = layout::stored_layout(row(layout::RETH_SETTINGS_KEY)?)?;
    let marker = layout::stored_marker(row(layout::TN_LAYOUT_KEY)?)?;
    let marked = marker.map(|marker| marker.layout);
    if stored.unwrap_or(StorageLayout::V1) != StorageLayout::V1
        || marked.is_some_and(|l| l != StorageLayout::V1)
    {
        eyre::bail!(
            "the MDBX database is not in storage layout v1 (storage_settings: {stored:?}, TN's \
             marker: {marked:?}): the migration to tn-lmdb moves only v1 databases"
        );
    }
    Ok(())
}

/// Open the MDBX database in `path` read-only, for the copy, with a hint on MDBX's
/// `MDBX_WANNA_RECOVERY` refusal ([`describe_mdbx_open_error`]).
fn open_mdbx_read_only(path: &Path) -> eyre::Result<DatabaseEnv> {
    reth_db::mdbx::open_db_read_only(path, DatabaseArguments::default())
        .map_err(describe_mdbx_open_error)
        .wrap_err("failed to open the MDBX database read-only")
}

/// The hint [`describe_mdbx_open_error`] adds to MDBX's `MDBX_WANNA_RECOVERY`.
const WANNA_RECOVERY_HINT: &str =
    "the MDBX database was not closed cleanly (MDBX_WANNA_RECOVERY: a stop under --db.sync-mode \
     safe-no-sync left a state a read-only open does not recover): start the previous binary, \
     the one that ran reth on MDBX, once and stop it cleanly, then start this build; MDBX is \
     unchanged";

/// The error of an MDBX open, `err`, with [`WANNA_RECOVERY_HINT`] when MDBX refused with
/// `MDBX_WANNA_RECOVERY` (an environment stopped uncleanly under `safe-no-sync`, whose recovery
/// needs a read-write open by a build that runs reth on MDBX); any other error as it is.
fn describe_mdbx_open_error(err: eyre::Report) -> eyre::Report {
    let wanna_recovery = MdbxError::WannaRecovery.to_err_code();
    let needs_recovery = err.chain().any(|e| {
        matches!(e.downcast_ref::<DatabaseError>(), Some(DatabaseError::Open(info)) if info.code == wanna_recovery)
    });
    if needs_recovery {
        err.wrap_err(WANNA_RECOVERY_HINT)
    } else {
        err
    }
}

/// The live bytes of reth's tables in the MDBX database `mdbx`: the pages of each table's tree and
/// overflow runs times the page size (the figures `db stats` reports on the MDBX build), summed
/// over the tables it has. `mdbx.dat` is larger: it grows in steps of its geometry, and keeps the
/// pages it has freed. Warns of the named databases in it that are not reth's tables
/// ([`foreign_tables`]).
fn mdbx_live_bytes(mdbx: &DatabaseEnv) -> eyre::Result<u64> {
    let tx = mdbx.tx().wrap_err("failed to begin a read of the MDBX database")?;
    let mut live = 0u64;
    for table in Tables::ALL {
        if let Some(stat) = mdbx_table_stat(&tx, table.name())? {
            live = live.saturating_add(stat.total_size);
        }
    }
    let foreign = foreign_tables(&tx)?;
    if !foreign.is_empty() {
        warn!(target: "tn::reth", tables = ?foreign, "the MDBX database holds tables that are not reth's: the migration to tn-lmdb does not copy them");
    }
    Ok(live)
}

/// The names of the named databases in the MDBX database `tx` reads that are not reth's tables:
/// the migration does not copy their rows, and reth does not read them. The names are the keys of
/// MDBX's main (unnamed) database.
fn foreign_tables(tx: &Tx<RO>) -> eyre::Result<Vec<String>> {
    let main = tx.inner().open_db(None).wrap_err("failed to open MDBX's main database")?;
    let mut cursor =
        tx.inner().cursor(main.dbi()).wrap_err("failed to read MDBX's main database")?;
    let mut foreign = Vec::new();
    for row in cursor.iter_start::<Vec<u8>, ()>() {
        let (name, ()) = row.wrap_err("failed to read MDBX's main database")?;
        let name = String::from_utf8_lossy(&name).into_owned();
        if !Tables::ALL.iter().any(|table| table.name() == name) {
            foreign.push(name);
        }
    }
    Ok(foreign)
}

/// The bytes free to an unprivileged user on the filesystem holding `path` (a test can set them
/// through `FREE_BYTES`).
fn free_bytes(path: &Path) -> eyre::Result<u64> {
    #[cfg(test)]
    if let Some(free) = FREE_BYTES.get() {
        return Ok(free);
    }
    available_bytes(path)
        .wrap_err_with(|| format!("failed to read the free space at {}", path.display()))
}

/// Migrate the MDBX database in `path` (the module docs' steps).
fn migrate(path: &Path, args: TnLmdbArgs) -> eyre::Result<()> {
    let started = Instant::now();
    let mdbx = path.join(MDBX_DATA_FILE);
    let temp = path.join(MIGRATING_DIR);
    remove_temp_dir(path)?;

    let mdbx_len = std::fs::metadata(&mdbx)
        .wrap_err_with(|| format!("failed to read the size of {}", mdbx.display()))?
        .len();
    // Every failure before the commit point leaves MDBX as it was.
    let unchanged = || {
        format!(
            "the MDBX to tn-lmdb migration of the reth database at {} failed, and the MDBX \
             database is unchanged",
            path.display()
        )
    };
    let src = open_mdbx_read_only(path).wrap_err_with(unchanged)?;
    let last_txnid = src
        .info()
        .wrap_err("failed to read the MDBX database's info")
        .wrap_err_with(unchanged)?
        .last_txnid() as u64;
    refuse_non_v1_source(&src).wrap_err_with(unchanged)?;
    let live = mdbx_live_bytes(&src).wrap_err_with(unchanged)?;
    let needed = needed_bytes(live, args.max_growth);
    let free = free_bytes(path)?;
    if free < needed {
        eyre::bail!(
            "the MDBX to tn-lmdb migration of the reth database at {} needs {needed} bytes free \
             (its tables take {live} bytes of pages in {}, plus headroom), but {free} are free: \
             free some space and start the node again",
            path.display(),
            mdbx.display(),
        );
    }
    info!(target: "tn::reth", path = %path.display(), mdbx_bytes = mdbx_len, live_bytes = live, free_bytes = free, "migrating the reth database from MDBX to tn-lmdb");

    let copies = match copy_and_install(path, &temp, src, last_txnid, args) {
        Ok(copies) => copies,
        Err(e) => {
            #[cfg(test)]
            if e.downcast_ref::<Killed>().is_some() {
                return Err(e);
            }
            if let Err(rm) = std::fs::remove_dir_all(&temp) {
                warn!(target: "tn::reth", dir = %temp.display(), error = %rm, "failed to remove the failed migration's temp directory; the next open removes it");
            }
            return Err(e.wrap_err(unchanged()));
        }
    };
    // Past the commit point a failure keeps the temp directory, emptied by the rename, so the
    // next open finishes the migration.
    let committed = || {
        format!(
            "the MDBX to tn-lmdb migration of the reth database at {} failed after its commit \
             point ({} is in place): the next start finishes the migration",
            path.display(),
            path.join(DATA_FILE).display(),
        )
    };
    let sync_rename = || {
        step(Step::AfterRename)?;
        fsync_directory(path)
            .wrap_err_with(|| format!("failed to fsync directory {}", path.display()))
    };
    sync_rename().wrap_err_with(committed)?;
    // The rollback tripwire from the commit point on: a build running reth on MDBX alone refuses
    // the directory before it can open and advance the `mdbx.dat` that is still beside the new
    // file, whose removal when the migration is finished later would silently roll it back.
    super::write_db_version_file(path).wrap_err_with(committed)?;

    let total: u64 = copies.iter().map(|copy| copy.rows).sum();
    let mut summary = format!("migrated from MDBX to tn-lmdb: {total} rows\n");
    for copy in &copies {
        summary.push_str(&format!("{} {}\n", copy.name, copy.rows));
    }
    finish(path, &summary).wrap_err_with(committed)?;
    info!(target: "tn::reth", path = %path.display(), rows = total, tables = copies.len(), elapsed = ?started.elapsed(), "migrated the reth database from MDBX to tn-lmdb");
    Ok(())
}

/// Build the tn-lmdb copy of `src`, the MDBX database in `path`, in the directory `temp`, verified
/// and durable ([`copy_and_verify`]), record `mdbx.dat`'s stat there with `last_txnid`, MDBX's
/// last transaction id ([`MdbxSource`]; the stat is taken once `src` is closed), and move the copy
/// into `path`: the commit point, so a failure here leaves MDBX as it was. Returns the copy of
/// each table.
fn copy_and_install(
    path: &Path,
    temp: &Path,
    src: DatabaseEnv,
    last_txnid: u64,
    args: TnLmdbArgs,
) -> eyre::Result<Vec<TableCopy>> {
    let copies = copy_and_verify(src, temp, args)?;
    MdbxSource::stat(&path.join(MDBX_DATA_FILE), last_txnid)?.write(temp)?;
    fsync_directory(temp)
        .wrap_err_with(|| format!("failed to fsync directory {}", temp.display()))?;
    step(Step::BeforeRename)?;
    let (from, to) = (temp.join(DATA_FILE), path.join(DATA_FILE));
    std::fs::rename(&from, &to)
        .wrap_err_with(|| format!("failed to move {} to {}", from.display(), to.display()))?;
    Ok(copies)
}

/// Copy every one of reth's tables from the MDBX database `src` into a new tn-lmdb file in `temp`,
/// then verify the copy and make it durable (steps 4 to 6 of the module docs). Both databases are
/// closed when it returns.
fn copy_and_verify(
    src: DatabaseEnv,
    temp: &Path,
    args: TnLmdbArgs,
) -> eyre::Result<Vec<TableCopy>> {
    // `NoSync` with no automatic syncs is fine here: an append-only copy frees few pages and
    // syncs before the move.
    let args = args.with_sync_mode(SyncMode::NoSync).with_sync_period(None).with_sync_bytes(None);
    let mut tn_lmdb = TnLmdbEnv::open(temp, TnLmdbEnvKind::RW, args)
        .wrap_err_with(|| format!("failed to create the tn-lmdb database in {}", temp.display()))?;
    tn_lmdb.create_tables()?;

    let copies = copy_tables(&src, &tn_lmdb)?;
    verify_tables(&tn_lmdb, &copies)?;
    tn_lmdb.sync().wrap_err("failed to sync the tn-lmdb database")?;
    let report = tn_lmdb.check().map_err(|e| {
        let detail = tn_lmdb.failure().unwrap_or_default();
        eyre::Report::new(e).wrap_err(format!("the tn-lmdb database fails its check: {detail}"))
    })?;
    for copy in &copies {
        let entries = report.tables.get(copy.name).map_or(0, |table| table.entries);
        if entries != copy.rows {
            eyre::bail!(
                "table {}: the tn-lmdb database's check counts {entries} rows, but {} were copied",
                copy.name,
                copy.rows,
            );
        }
    }
    Ok(copies)
}

/// What the copy of one table did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableCopy {
    /// The table's name.
    name: &'static str,
    /// The rows copied.
    rows: u64,
    /// The key and value bytes copied.
    bytes: u64,
    /// The hash of every row copied, in order ([`hash_row`]).
    hash: u64,
}

/// Add a row to `hasher`: its key and its value, each after its length, so no two rows' bytes
/// run together.
fn hash_row(hasher: &mut DefaultHasher, key: &[u8], value: &[u8]) {
    hasher.write_usize(key.len());
    hasher.write(key);
    hasher.write_usize(value.len());
    hasher.write(value);
}

/// Copy every one of reth's tables from `src` into `dst`, in the order of `Tables::ALL`, through
/// one read transaction of `src`.
fn copy_tables<S: Database, D: Database>(src: &S, dst: &D) -> eyre::Result<Vec<TableCopy>> {
    let mut tx = src.tx().wrap_err("failed to begin a read of the MDBX database")?;
    tx.disable_long_read_transaction_safety();
    let viewer = CopyTable { src: &tx, dst };
    Tables::ALL.iter().map(|table| table.view(&viewer)).collect()
}

/// The [`TableViewer`] that copies one table ([`copy_table`]).
struct CopyTable<'a, TX, D> {
    src: &'a TX,
    dst: &'a D,
}

impl<TX: DbTx, D: Database> TableViewer<TableCopy> for CopyTable<'_, TX, D> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> eyre::Result<TableCopy> {
        copy_table::<T, _, _>(self.src, self.dst)
    }
}

/// Copy table `T` from the read transaction `src` into `dst` by raw appends, in bounded write
/// transactions, and check that the rows copied are the rows `src` counts. A table `src` lacks
/// (never created) has no rows.
fn copy_table<T: Table, TX: DbTx, D: Database>(src: &TX, dst: &D) -> eyre::Result<TableCopy> {
    let name = T::NAME;
    let started = Instant::now();
    // A table `src` lacks reads back as an empty one: the hash of no rows.
    let mut copy = TableCopy { name, rows: 0, bytes: 0, hash: DefaultHasher::new().finish() };
    let expected = match src.entries::<RawTable<T>>() {
        Ok(entries) => entries as u64,
        Err(DatabaseError::Open(info)) if info.code == MdbxError::NotFound.to_err_code() => {
            info!(target: "tn::reth", table = name, "table not in the MDBX database: no rows to copy");
            return Ok(copy);
        }
        Err(e) => {
            return Err(e)
                .wrap_err_with(|| format!("failed to count the MDBX rows of table {name}"));
        }
    };

    let mut hasher = DefaultHasher::new();
    let (mut txn_rows, mut txn_bytes) = (0u64, 0u64);
    let mut tx = dst.tx_mut()?;
    let mut cursor = tx.cursor_write::<RawTable<T>>()?;
    let mut source = src.cursor_read::<RawTable<T>>()?;
    for row in source.walk(None)? {
        let (key, value) =
            row.wrap_err_with(|| format!("failed to read row {} of table {name}", copy.rows))?;
        #[cfg(test)]
        if take_fault(CopyFault::SkipRow(name)) {
            continue;
        }
        hash_row(&mut hasher, key.raw_key(), value.raw_value());
        let len = (key.raw_key().len() + value.raw_value().len()) as u64;
        #[cfg(test)]
        let value = if take_fault(CopyFault::FlipValue(name)) {
            let mut bytes = value.into_value();
            if let Some(last) = bytes.last_mut() {
                *last ^= 1;
            }
            reth_db::tables::RawValue::from_vec(bytes)
        } else {
            value
        };
        cursor
            .append(key, &value)
            .wrap_err_with(|| format!("failed to append row {} of table {name}", copy.rows))?;
        copy.rows += 1;
        copy.bytes += len;
        txn_rows += 1;
        txn_bytes += len;
        if txn_rows >= COMMIT_ROWS || txn_bytes >= COMMIT_BYTES {
            drop(cursor);
            tx.commit()?;
            step(Step::MidCopy)?;
            tx = dst.tx_mut()?;
            cursor = tx.cursor_write::<RawTable<T>>()?;
            (txn_rows, txn_bytes) = (0, 0);
        }
        if copy.rows.is_multiple_of(PROGRESS_ROWS) {
            info!(target: "tn::reth", table = name, rows = copy.rows, of = expected, elapsed = ?started.elapsed(), "copying a table from MDBX to tn-lmdb");
        }
    }
    drop(cursor);
    tx.commit()?;
    step(Step::MidCopy)?;
    copy.hash = hasher.finish();

    if copy.rows != expected {
        eyre::bail!(
            "table {name}: {} rows were copied, but MDBX counts {expected} (a short read)",
            copy.rows,
        );
    }
    info!(target: "tn::reth", table = name, rows = copy.rows, bytes = copy.bytes, elapsed = ?started.elapsed(), "copied a table from MDBX to tn-lmdb");
    Ok(copy)
}

/// Read every table of `db` back, through one read transaction, and check each against its copy
/// in `copies` (in the order of `Tables::ALL`): the same rows, hashing the same. The read takes
/// as long as the copy, so, like the copy's read of the source, it opts out of the read
/// transaction timeout.
fn verify_tables<D: Database>(db: &D, copies: &[TableCopy]) -> eyre::Result<()> {
    let mut tx = db.tx().wrap_err("failed to begin a read of the tn-lmdb database")?;
    tx.disable_long_read_transaction_safety();
    for (table, copy) in Tables::ALL.iter().zip(copies) {
        table.view(&VerifyTable { tx: &tx, copy })?;
    }
    Ok(())
}

/// The [`TableViewer`] that checks one table against its copy ([`verify_tables`]).
struct VerifyTable<'a, TX> {
    tx: &'a TX,
    copy: &'a TableCopy,
}

impl<TX: DbTx> TableViewer<()> for VerifyTable<'_, TX> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> eyre::Result<()> {
        let mut hasher = DefaultHasher::new();
        let mut rows = 0u64;
        let mut cursor = self.tx.cursor_read::<RawTable<T>>()?;
        for row in cursor.walk(None)? {
            let (key, value) = row
                .wrap_err_with(|| format!("failed to read back row {rows} of table {}", T::NAME))?;
            hash_row(&mut hasher, key.raw_key(), value.raw_value());
            rows += 1;
        }
        let copy = self.copy;
        if rows != copy.rows || hasher.finish() != copy.hash {
            eyre::bail!(
                "table {}: the tn-lmdb database reads back {rows} rows that differ from the {} \
                 copied",
                T::NAME,
                copy.rows,
            );
        }
        Ok(())
    }
}

/// Finish a migration whose `tn-lmdb.dat` is in place in `path`: refuse an `mdbx.dat` that differs
/// from the record the migration took of it ([`MdbxSource`], [`written_after_commit_point`]);
/// fsync the directory, so the rename that put `tn-lmdb.dat` there is durable before MDBX goes (a
/// later open finishes a migration stopped right after the rename, whose fsync may never have
/// run); then remove MDBX's files (`mdbx.lck`, then `mdbx.dat` last), fsync the directory, write
/// the marker with `summary` as its text, and remove the temp directory last: until the marker is
/// written, the temp directory beside `tn-lmdb.dat` marks the migration unfinished
/// ([`stopped_after_commit_point`]).
fn finish(path: &Path, summary: &str) -> eyre::Result<()> {
    let mdbx = path.join(MDBX_DATA_FILE);
    if file_exists(&mdbx)? {
        if let Some(recorded) = MdbxSource::read(&path.join(MIGRATING_DIR))? {
            let now = MdbxSource::stat(&mdbx, recorded.last_txnid)?;
            if now != recorded {
                return Err(written_after_commit_point(path, recorded, now));
            }
        }
    }
    fsync_directory(path)
        .wrap_err_with(|| format!("failed to fsync directory {}", path.display()))?;
    remove_file_if_present(&path.join(MDBX_LOCK_FILE))?;
    remove_file_if_present(&path.join(MDBX_DATA_FILE))?;
    fsync_directory(path)
        .wrap_err_with(|| format!("failed to fsync directory {}", path.display()))?;
    step(Step::BeforeMarker)?;

    let marker = path.join(MIGRATED_MARKER);
    let tmp = path.join(format!("{MIGRATED_MARKER}.tmp"));
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(summary.as_bytes())?;
        fsync_file(&file)?;
        drop(file);
        std::fs::rename(&tmp, &marker)?;
        fsync_directory(path)
    };
    write().wrap_err_with(|| format!("failed to write {}", marker.display()))?;
    remove_temp_dir(path)
}

/// Remove a migration's temp directory from `path`, if there is one: a stopped migration's, or
/// once a migration's file has moved out of it.
fn remove_temp_dir(path: &Path) -> eyre::Result<()> {
    let temp = path.join(MIGRATING_DIR);
    if file_exists(&temp)? {
        info!(target: "tn::reth", dir = %temp.display(), "removing an MDBX to tn-lmdb migration's temp directory");
        std::fs::remove_dir_all(&temp)
            .wrap_err_with(|| format!("failed to remove {}", temp.display()))?;
    }
    Ok(())
}

/// Remove `file`; one already gone is not an error.
fn remove_file_if_present(file: &Path) -> eyre::Result<()> {
    match std::fs::remove_file(file) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(e).wrap_err_with(|| format!("failed to remove {}", file.display()))
        }
        _ => Ok(()),
    }
}
