//! The MDBX to tndb-b migration of TN's reth database, in the `tndb-b` build.
//!
//! A node built with the `tndb-b` feature that finds an MDBX reth database (`<db>/mdbx.dat`)
//! moves it onto tndb-b when it first opens the database, before anything else reads the
//! directory: the backend guard calls [`migrate_from_mdbx`]. The copy is raw: each of reth's
//! tables is walked on MDBX as its encoded keys and compressed values (`RawTable`) and appended to
//! tndb-b unchanged, so no row is decoded. reth's static files and its `database.version` file are
//! not touched; the open that follows checks the version file and records the client version as
//! on any open, after the `VersionHistory` rows the copy brought over.
//!
//! **States.** The directory's files decide what an open does. Each is looked for with
//! `try_exists`, so a stat error fails the open:
//!
//! | `mdbx.dat` | `tndb.dat` | `mdbx-migrated` | the open |
//! |---|---|---|---|
//! | yes | no | no | migrates |
//! | yes | yes | no | a migration stopped after its commit point: finishes it (removes MDBX's files, writes the marker) |
//! | yes | either | yes | refused: something wrote MDBX after the migration (an older binary), and the static files may be ahead of tndb-b |
//! | no | yes | no, and the temp directory without a `tndb.dat` | a migration stopped after removing MDBX's files: finishes it (writes the marker) |
//! | no | yes | either | a normal open |
//! | no | no | yes | refused: the migrated database is missing |
//! | no | no | no | a normal open, of a new database |
//!
//! The temp directory a migration builds in ([`MIGRATING_DIR`]) is live only while the migration
//! runs, and the node holds its datadir lock while it opens the database, so one found by an open
//! is a stopped migration's. Before the commit point it holds a partial or unrenamed copy, and is
//! removed; after it, emptied by the rename, it is kept until the marker is written, so beside
//! `tndb.dat` with neither MDBX's data file nor the marker it says a migration stopped before
//! writing its marker.
//!
//! **Steps.**
//! 1. Remove a stopped migration's temp directory.
//! 2. Open MDBX read-only, and sum the live bytes of reth's tables in it: their branch, leaf and
//!    overflow pages times the page size, as `db stats` reports them on the MDBX build
//!    ([`mdbx_live_bytes`]). Warn of any named database in it that is not one of reth's tables: its
//!    rows are not copied (reth does not read them either).
//! 3. Check the free disk: the live bytes plus headroom ([`needed_bytes`]), else refuse, naming the
//!    live bytes and the bytes needed and free. `mdbx.dat`'s size is no measure: the file grows in
//!    large steps (a local node's is 4 GiB, mostly sparse) and keeps the pages it frees.
//! 4. Open tndb-b in the temp directory with `NoSync`, the page size and reservation of the node's
//!    `--db.*` flags, and create reth's tables.
//! 5. Copy each of reth's tables (`Tables::ALL`), read from MDBX in one transaction with the
//!    long-read safety off, by appends, in write transactions of at most [`COMMIT_ROWS`] rows or
//!    [`COMMIT_BYTES`] bytes, hashing each row's key and value as it goes (std's `DefaultHasher`,
//!    SipHash with fixed keys: the same in both passes of one process, which is all the comparison
//!    needs). MDBX walks a table in its key order, and a dup-sort table's values in their byte
//!    order, which is the order of the adapter's composite rows (key, then subkey), so every row
//!    takes the engine's append. Any refused row (out of order, a second value for a dup row's
//!    subkey, a size the adapter cannot store) fails the migration.
//! 6. Verify: each table's rows copied equal MDBX's count of them (`db_stat`), so a short read
//!    fails; a read-back pass over each tndb-b table hashes equal to its copy; then
//!    `TndbEnv::sync`, the engine's full check of the file (`TndbEnv::check`), and its entry count
//!    of each table equal to the rows copied.
//! 7. The commit point: close the tndb-b file, fsync the temp directory, rename its `tndb.dat` into
//!    the database directory, and fsync that.
//! 8. Remove MDBX's `mdbx.lck`, then `mdbx.dat` last (beside `tndb.dat`, its presence marks a
//!    migration unfinished), and fsync the directory.
//! 9. Write the marker [`MIGRATED_MARKER`] atomically (a temp file, fsync, rename, fsync of the
//!    directory). Its text lists the rows copied per table.
//! 10. Remove the temp directory, the sign of an unfinished migration until the marker is written.
//!
//! A failure before the commit point removes the temp directory and leaves MDBX as it was. A stop
//! there (a kill, a power loss) leaves MDBX in place too, and the next open migrates again from the
//! start; a stop after the rename leaves `tndb.dat` beside MDBX's files, or beside the emptied temp
//! directory once MDBX's files are gone, and the next open finishes the migration.
//!
//! **Why a temp directory.** A copy stopped part way cannot resume into the same file: the
//! adapter's appends are not idempotent (a row appended again is refused, or replaces the last),
//! and a partial `tndb.dat` in the database directory would open as a short database beside the
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
    time::Instant,
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
use tn_storage::disk::{available_bytes, fsync_directory, fsync_file};
use tndb_b_reth::{SyncMode, TndbArgs, TndbEnv, TndbEnvKind, DATA_FILE};
use tracing::{info, warn};

use super::{file_exists, mdbx_table_stat, MDBX_DATA_FILE};

#[cfg(test)]
mod tests;

/// The marker a finished migration leaves in the database directory. An `mdbx.dat` beside it was
/// written after the migration, which is refused.
pub(super) const MIGRATED_MARKER: &str = "mdbx-migrated";

/// The directory, inside the database directory, a migration builds the tndb-b file in.
pub(super) const MIGRATING_DIR: &str = "tndb-migrating";

/// MDBX's lock file, removed before its data file.
const MDBX_LOCK_FILE: &str = "mdbx.lck";

/// The rows a write transaction of the copy takes before it commits. With [`COMMIT_BYTES`], it
/// bounds what each commit writes; a `NoSync` commit is cheap, so the bounds need not be tight.
/// Tests take 1000, so their tables' copies span several transactions.
const COMMIT_ROWS: u64 = if cfg!(test) { 1000 } else { 100_000 };

/// The key and value bytes a write transaction of the copy takes before it commits.
const COMMIT_BYTES: u64 = 64 << 20;

/// A table's copy logs its progress every this many rows.
const PROGRESS_ROWS: u64 = 10_000_000;

/// The fixed part of the headroom a migration needs on the disk ([`needed_bytes`]).
const HEADROOM_FLOOR: u64 = 1 << 30;

/// The named steps of a migration, at which a test can stop it as a kill would (the test-only
/// `STOP_AT`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// After a commit of the copy: rows are in the temp directory's file.
    MidCopy,
    /// Verified, synced and closed, before the rename (the commit point).
    BeforeRename,
    /// After the rename, before the temp directory and MDBX's files are removed.
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
    /// Test-only: the free bytes the migrations on this thread read in place of the filesystem's.
    pub(super) static FREE_BYTES: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
    /// Test-only: the fault the next migration on this thread makes ([`CopyFault`]).
    pub(super) static COPY_FAULT: std::cell::Cell<Option<CopyFault>> =
        const { std::cell::Cell::new(None) };
}

/// Pass step `at`; a test stops the migration there through `STOP_AT` (after `STOP_PASSES`
/// passes of it).
fn step(at: Step) -> eyre::Result<()> {
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

/// Settle the reth database directory `path` before the tndb build opens it (the module docs'
/// states): migrate an MDBX database, finish a migration stopped after its commit point, refuse
/// MDBX written after a migration, and remove a stopped migration's temp directory. `args` are
/// the settings the node opens tndb-b with: a migrated file takes their page size and reservation.
pub(super) fn migrate_from_mdbx(path: &Path, args: TndbArgs) -> eyre::Result<()> {
    let mdbx = file_exists(&path.join(MDBX_DATA_FILE))?;
    let tndb = file_exists(&path.join(DATA_FILE))?;
    let marker = file_exists(&path.join(MIGRATED_MARKER))?;
    match (mdbx, tndb, marker) {
        (true, _, true) => Err(written_after_migration(path)),
        (true, false, false) => migrate(path, args),
        (true, true, false) => {
            info!(target: "tn::reth", path = %path.display(), "finishing an MDBX to tndb-b migration stopped after its commit point");
            finish(path, FINISHED_LATER)
        }
        (false, true, false) if stopped_before_marker(path)? => {
            info!(target: "tn::reth", path = %path.display(), "finishing an MDBX to tndb-b migration stopped before its marker");
            finish(path, FINISHED_LATER)
        }
        (false, false, true) => eyre::bail!(
            "the reth database at {} was migrated from MDBX to tndb-b ({} exists), but {} is \
             missing: this build does not create a new database beside the datadir's static files \
             in its place; restore the file, or start from a fresh datadir (or `db load-state`)",
            path.display(),
            path.join(MIGRATED_MARKER).display(),
            path.join(DATA_FILE).display(),
        ),
        (false, _, _) => remove_temp_dir(path),
    }
}

/// The marker's text when an open finishes a migration another run stopped after its commit point.
const FINISHED_LATER: &str =
    "migrated from MDBX to tndb-b; finished by a later open (the run that copied the rows logged \
     them)\n";

/// Whether `path`, holding `tndb.dat` and neither MDBX's data file nor the marker, holds a
/// migration stopped after it removed MDBX's files and before it wrote the marker: its temp
/// directory, which a migration removes only after the marker, is there without a `tndb.dat` (the
/// file moved out of it at the commit point).
fn stopped_before_marker(path: &Path) -> eyre::Result<bool> {
    let temp = path.join(MIGRATING_DIR);
    Ok(file_exists(&temp)? && !file_exists(&temp.join(DATA_FILE))?)
}

/// The refusal of an `mdbx.dat` beside the marker of a finished migration in `path`.
pub(super) fn written_after_migration(path: &Path) -> eyre::Report {
    eyre::eyre!(
        "the reth database at {} was migrated from MDBX to tndb-b ({} exists), but {} was written \
         since, by a build running reth on MDBX (an older binary?): the static files may be ahead \
         of the tndb-b database, so this build does not open it. Resolve it by hand (start from a \
         fresh datadir, or `db load-state`)",
        path.display(),
        path.join(MIGRATED_MARKER).display(),
        path.join(MDBX_DATA_FILE).display(),
    )
}

/// The free bytes a migration needs on the database's filesystem: `live`, the bytes of the pages
/// reth's tables take in MDBX ([`mdbx_live_bytes`]), which hold the rows the copy writes, plus
/// headroom of a quarter of it and [`HEADROOM_FLOOR`] (1 GiB). The headroom covers the tndb-b
/// file's own layout (a value past about a quarter page goes to an overflow run of whole pages,
/// where MDBX keeps up to about half a page in its leaf) and leaves the filesystem room to work.
/// The check is an early refusal, not a promise: a disk that fills during the copy fails the
/// migration, which removes its temp directory and leaves MDBX as it was.
pub(super) const fn needed_bytes(live: u64) -> u64 {
    live.saturating_add(live / 4).saturating_add(HEADROOM_FLOOR)
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
        warn!(target: "tn::reth", tables = ?foreign, "the MDBX database holds tables that are not reth's: the migration to tndb-b does not copy them");
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
fn migrate(path: &Path, args: TndbArgs) -> eyre::Result<()> {
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
            "the MDBX to tndb-b migration of the reth database at {} failed, and the MDBX \
             database is unchanged",
            path.display()
        )
    };
    let src = reth_db::mdbx::open_db_read_only(path, DatabaseArguments::default())
        .wrap_err("failed to open the MDBX database read-only")
        .wrap_err_with(unchanged)?;
    let live = mdbx_live_bytes(&src).wrap_err_with(unchanged)?;
    let needed = needed_bytes(live);
    let free = free_bytes(path)?;
    if free < needed {
        eyre::bail!(
            "the MDBX to tndb-b migration of the reth database at {} needs {needed} bytes free \
             (its tables take {live} bytes of pages in {}, plus headroom), but {free} are free: \
             free some space and start the node again",
            path.display(),
            mdbx.display(),
        );
    }
    info!(target: "tn::reth", path = %path.display(), mdbx_bytes = mdbx_len, live_bytes = live, free_bytes = free, "migrating the reth database from MDBX to tndb-b");

    let copies = match copy_and_install(path, &temp, src, args) {
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
    step(Step::AfterRename)?;

    let total: u64 = copies.iter().map(|copy| copy.rows).sum();
    let mut summary = format!("migrated from MDBX to tndb-b: {total} rows\n");
    for copy in &copies {
        summary.push_str(&format!("{} {}\n", copy.name, copy.rows));
    }
    finish(path, &summary)?;
    info!(target: "tn::reth", path = %path.display(), rows = total, tables = copies.len(), elapsed = ?started.elapsed(), "migrated the reth database from MDBX to tndb-b");
    Ok(())
}

/// Build the tndb-b copy of `src`, the MDBX database in `path`, in the directory `temp`, verified
/// and durable ([`copy_and_verify`]), and move it into `path`: the commit point. Returns the copy
/// of each table.
fn copy_and_install(
    path: &Path,
    temp: &Path,
    src: DatabaseEnv,
    args: TndbArgs,
) -> eyre::Result<Vec<TableCopy>> {
    let copies = copy_and_verify(src, temp, args)?;
    fsync_directory(temp)
        .wrap_err_with(|| format!("failed to fsync directory {}", temp.display()))?;
    step(Step::BeforeRename)?;
    let (from, to) = (temp.join(DATA_FILE), path.join(DATA_FILE));
    std::fs::rename(&from, &to)
        .wrap_err_with(|| format!("failed to move {} to {}", from.display(), to.display()))?;
    fsync_directory(path)
        .wrap_err_with(|| format!("failed to fsync directory {}", path.display()))?;
    Ok(copies)
}

/// Copy every one of reth's tables from the MDBX database `src` into a new tndb-b file in `temp`,
/// then verify the copy and make it durable (steps 4 to 6 of the module docs). Both databases are
/// closed when it returns.
fn copy_and_verify(src: DatabaseEnv, temp: &Path, args: TndbArgs) -> eyre::Result<Vec<TableCopy>> {
    // `NoSync` is fine here: an append-only copy frees few pages and syncs before the move.
    let mut tndb = TndbEnv::open(temp, TndbEnvKind::RW, args.with_sync_mode(SyncMode::NoSync))
        .wrap_err_with(|| format!("failed to create the tndb-b database in {}", temp.display()))?;
    tndb.create_tables()?;

    let copies = copy_tables(&src, &tndb)?;
    verify_tables(&tndb, &copies)?;
    tndb.sync().wrap_err("failed to sync the tndb-b database")?;
    let report = tndb.check().map_err(|e| {
        let detail = tndb.failure().unwrap_or_default();
        eyre::Report::new(e).wrap_err(format!("the tndb-b database fails its check: {detail}"))
    })?;
    for copy in &copies {
        let entries = report.tables.get(copy.name).map_or(0, |table| table.entries);
        if entries != copy.rows {
            eyre::bail!(
                "table {}: the tndb-b database's check counts {entries} rows, but {} were copied",
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
            info!(target: "tn::reth", table = name, rows = copy.rows, of = expected, elapsed = ?started.elapsed(), "copying a table from MDBX to tndb-b");
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
    info!(target: "tn::reth", table = name, rows = copy.rows, bytes = copy.bytes, elapsed = ?started.elapsed(), "copied a table from MDBX to tndb-b");
    Ok(copy)
}

/// Read every table of `db` back, through one read transaction, and check each against its copy
/// in `copies` (in the order of `Tables::ALL`): the same rows, hashing the same.
fn verify_tables<D: Database>(db: &D, copies: &[TableCopy]) -> eyre::Result<()> {
    let tx = db.tx().wrap_err("failed to begin a read of the tndb-b database")?;
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
                "table {}: the tndb-b database reads back {rows} rows that differ from the {} \
                 copied",
                T::NAME,
                copy.rows,
            );
        }
        Ok(())
    }
}

/// Finish a migration whose `tndb.dat` is in place in `path`: remove MDBX's files (`mdbx.lck`,
/// then `mdbx.dat` last), fsync the directory, write the marker with `summary` as its text, and
/// remove the temp directory last: until the marker is written, the temp directory beside
/// `tndb.dat` marks the migration unfinished ([`stopped_before_marker`]).
fn finish(path: &Path, summary: &str) -> eyre::Result<()> {
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
        info!(target: "tn::reth", dir = %temp.display(), "removing an MDBX to tndb-b migration's temp directory");
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
