//! `db reset-execution`: move the execution database aside, so the next start rebuilds it from
//! genesis by re-executing the chain, in the storage layout new datadirs get.
//!
//! The command moves the datadir's reth database (`db`), its static files (`static_files`) and the
//! restored-state floor marker (`restored-state-floor`, present on a datadir bootstrapped from a
//! snapshot) into `execution-reset-<UTC time>` under the datadir. It renames, never deletes, so
//! moving the three entries back restores the execution database as it was; the consensus
//! database keeps what the node wrote meanwhile (`crates/tn-reth/README.md`, "Rollback"). The
//! consensus database, the keys and the config stay where they are.
//!
//! The moves are staged in `execution-reset-<UTC time>.partial`, renamed to its final name once
//! every entry is in it. A node refuses to start while a `.partial` directory holding an entry is
//! there ([`refuse_interrupted_reset`]): a start between two moves would open a datadir holding
//! half an execution database. An empty one (a reset stopped before its first move, or undone by
//! moving its entries back) holds none of it and blocks nothing. Running the command again finishes
//! an interrupted reset into the same directory. A move that fails (the rename of a bind mount,
//! say) moves back what the run had moved, so a failed run leaves the datadir as it was.
//!
//! The rebuild re-executes every epoch from 0 from the node's own consensus packs, so the command
//! refuses a datadir whose consensus database lacks the pack of an epoch the node cannot fetch
//! again, one at or below the epoch it resumes from ([`refuse_missing_packs`]): a datadir
//! bootstrapped with `db load-state` (no pack below the restored epoch), or one that lost a past
//! pack or the resume epoch's. The side-observer swap moves the execution database
//! aside to copy in a synced observer's, which replays nothing below the copy's tip, so it skips
//! that check ([`ResetMode::Swap`]). Both refuse a datadir an unfinished `db load-state` left, as
//! the node does ([`refuse_interrupted_load_state`]): what they would move aside is half a restore,
//! and the datadir would stay refused.

use eyre::{bail, eyre, WrapErr as _};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tn_config::{PidLock, TelcoinDirs as _};
use tn_storage::{
    consensus::ConsensusChain,
    disk::{available_bytes, fsync_directory},
};
use tn_types::Epoch;

use super::load_state_marker::refuse_interrupted_load_state;

/// The prefix of the directories a reset moves the execution database into.
const RESET_DIR_PREFIX: &str = "execution-reset-";

/// The suffix of a reset's directory while it is being filled.
const PARTIAL_SUFFIX: &str = ".partial";

/// The datadir entries a reset moves, in order: reth's database, its static files, and the
/// restored-state floor marker, which would refuse every pinned read of the rebuilt chain below the
/// old snapshot's block if it stayed.
fn execution_entries(datadir: &Path) -> eyre::Result<[PathBuf; 3]> {
    let floor = tn_reth::RethEnv::restored_state_floor_marker(datadir);
    let floor = floor
        .file_name()
        .ok_or_else(|| eyre!("the restored-state floor marker {} has no name", floor.display()))?;
    Ok([PathBuf::from("db"), PathBuf::from("static_files"), PathBuf::from(floor)])
}

/// Whether `path` exists, as a file, a directory or a link. A stat error is an error, never
/// "absent".
fn exists(path: &Path) -> eyre::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(eyre!("failed to check for {}: {e}", path.display())),
    }
}

/// The reset directories in `datadir` still being filled (`execution-reset-*.partial`).
fn partial_resets(datadir: &Path) -> eyre::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(datadir)
        .wrap_err_with(|| format!("failed to list the datadir {}", datadir.display()))?
    {
        let entry =
            entry.wrap_err_with(|| format!("failed to list the datadir {}", datadir.display()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(RESET_DIR_PREFIX) && name.ends_with(PARTIAL_SUFFIX) {
            found.push(entry.path());
        }
    }
    found.sort();
    Ok(found)
}

/// Refuse to start a node, or to run a `db` command on the execution database, on `datadir` while
/// an execution reset there is unfinished: the datadir may hold part of the old execution database
/// beside nothing, and the node would open it. An empty staging directory holds none of the
/// execution database (a reset stopped before its first move, or undone by moving its entries
/// back), so it does not refuse.
pub(crate) fn refuse_interrupted_reset(datadir: &Path) -> eyre::Result<()> {
    for partial in partial_resets(datadir)? {
        if is_empty_dir(&partial)? {
            continue;
        }
        bail!(
            "an execution reset of {} did not finish: {} holds part of the execution database. \
             Run the `telcoin-network db reset-execution --force` that started it again, with \
             `--for-swap` if it was the side-observer swap's, to finish it, or move its entries \
             back into the datadir to undo it (an empty one blocks nothing)",
            datadir.display(),
            partial.display(),
        );
    }
    Ok(())
}

/// Whether `path` is a directory with no entries. Anything else (a file, a link, a directory
/// holding an entry) is not.
fn is_empty_dir(path: &Path) -> eyre::Result<bool> {
    let meta = fs::symlink_metadata(path)
        .wrap_err_with(|| format!("failed to check {}", path.display()))?;
    if !meta.is_dir() {
        return Ok(false);
    }
    let mut entries =
        fs::read_dir(path).wrap_err_with(|| format!("failed to list {}", path.display()))?;
    Ok(entries.next().is_none())
}

/// The epoch of a consensus pack directory named `name` (`epoch-N`), or of one an interrupted
/// install moved aside (`epoch-N.replaced`, which the node's next start puts back when `epoch-N`
/// is missing).
fn pack_epoch(name: &str) -> Option<Epoch> {
    let name = name.strip_suffix(".replaced").unwrap_or(name);
    name.strip_prefix("epoch-")?.parse().ok()
}

/// The epochs from 0 to `end` that `present` (ascending, deduplicated) lacks, as inclusive ranges
/// `(first, last)`.
fn missing_ranges(present: &[Epoch], end: Epoch) -> Vec<(Epoch, Epoch)> {
    let mut ranges = Vec::new();
    // the first epoch not yet accounted for; `None` past the last epoch there is
    let mut next = Some(0);
    for &epoch in present.iter().take_while(|&&epoch| epoch <= end) {
        if let Some(first) = next.filter(|&first| epoch > first) {
            ranges.push((first, epoch - 1));
        }
        next = epoch.checked_add(1);
    }
    if let Some(first) = next.filter(|&first| first <= end) {
        ranges.push((first, end));
    }
    ranges
}

/// `ranges` split into the epochs at or below `epoch` and the epochs above it.
fn split_ranges(ranges: &[(Epoch, Epoch)], epoch: Epoch) -> SplitRanges {
    let mut split = SplitRanges::default();
    for &(first, last) in ranges {
        if last <= epoch {
            split.at_or_below.push((first, last));
        } else if first > epoch {
            split.above.push((first, last));
        } else {
            split.at_or_below.push((first, epoch));
            split.above.push((epoch + 1, last));
        }
    }
    split
}

/// Ranges of epochs split at one epoch ([`split_ranges`]).
#[derive(Debug, Default, PartialEq, Eq)]
struct SplitRanges {
    /// The ranges at or below the epoch.
    at_or_below: Vec<(Epoch, Epoch)>,
    /// The ranges above it.
    above: Vec<(Epoch, Epoch)>,
}

/// `ranges` as runs: `0..=2, 5`.
fn runs(ranges: &[(Epoch, Epoch)]) -> String {
    ranges
        .iter()
        .map(
            |&(first, last)| {
                if first == last {
                    first.to_string()
                } else {
                    format!("{first}..={last}")
                }
            },
        )
        .collect::<Vec<_>>()
        .join(", ")
}

/// Refuse a datadir whose consensus database lacks the pack of an epoch the node cannot fetch
/// again: one at or below the epoch it resumes from ([`ConsensusChain::resume_epoch`], the
/// latest-consensus hint; genesis without one, as the node's start does). After a reset the node
/// re-executes every epoch from 0 from its own packs. Its start opens the resume epoch's pack,
/// refusing to start without it past epoch 0, and state-sync fetches from peers only the packs of
/// the recorded epochs after it (`request_missing_packs`), so a missing pack at or below it would
/// stop every start. A datadir bootstrapped with `db load-state` is one (it holds consensus only
/// from the restored epoch on, where its hint points), and so is one that lost a past pack or the
/// resume epoch's. A missing pack above the resume epoch (a catch-up stopped while several imports
/// ran) is fetched again by the next start, so it passes, and a refusal for the others names it
/// apart. A pack is present as the node's start finds it ([`ConsensusChain::pack_data_at_start`]:
/// the epoch's data log, the one an interrupted install moved aside included). A datadir with no
/// pack and no hint past epoch 0 starts as a new node does and syncs from genesis, so it passes.
fn refuse_missing_packs(datadir: &Path) -> eyre::Result<()> {
    let epochs = datadir.to_path_buf().epochs_db_path();
    if !exists(&epochs)? {
        return Ok(());
    }
    let mut present = Vec::new();
    for entry in fs::read_dir(&epochs)
        .wrap_err_with(|| format!("failed to list the epoch packs in {}", epochs.display()))?
    {
        let entry = entry
            .wrap_err_with(|| format!("failed to list the epoch packs in {}", epochs.display()))?;
        let Some(epoch) = pack_epoch(&entry.file_name().to_string_lossy()) else { continue };
        if ConsensusChain::pack_data_at_start(&epochs, epoch).wrap_err_with(|| {
            format!("failed to look for the pack of epoch {epoch} in {}", epochs.display())
        })? {
            present.push(epoch);
        }
    }
    present.sort_unstable();
    present.dedup();
    let resume = ConsensusChain::resume_epoch(&epochs).unwrap_or(0);
    // the start opens the resume epoch's pack past epoch 0, so a missing one counts even above
    // every pack the datadir holds
    let end = match present.last() {
        Some(&latest) => latest.max(resume),
        None if resume > 0 => resume,
        None => return Ok(()),
    };
    let missing = split_ranges(&missing_ranges(&present, end), resume);
    if missing.at_or_below.is_empty() {
        return Ok(());
    }
    let lost = runs(&missing.at_or_below);
    let held = match present.last() {
        Some(latest) => format!("packs up to epoch {latest}"),
        None => "no pack".to_string(),
    };
    let fetched = if missing.above.is_empty() {
        String::new()
    } else {
        format!(
            " The pack(s) of epoch(s) {} are missing too, but those are after epoch {resume}, so \
             the node's next start fetches them from peers: they need nothing.",
            runs(&missing.above)
        )
    };
    // the resume epoch is the one the node writes: no copy of a closed epoch puts it back
    let resume_lost = missing.at_or_below.last().is_some_and(|&(_, last)| last == resume);
    let past = match resume.checked_sub(1) {
        Some(below) => split_ranges(&missing.at_or_below, below).at_or_below,
        None => Vec::new(),
    };
    let mut ways = String::new();
    // another node's sealed copy, checked against this node's records, puts back any past epoch
    let import = "another node's sealed copy (`db import-epoch --epoch N --from <copy>` verifies \
                  and installs one)";
    match past.as_slice() {
        [] => {}
        // every epoch below a point lost, the shape `db load-state` leaves: it never held them
        &[(0, last)] => ways.push_str(&format!(
            " A datadir bootstrapped with `db load-state` holds packs only from the restored epoch \
             on, and this one holds none below epoch {}. To get a new execution database, run \
             `db load-state` again instead, with `db`, `static_files`, `restored-state-floor` and \
             `consensus-db` moved aside (its import creates the restored epoch's pack and refuses \
             a datadir that holds one; a validator then stays stopped until the network has closed \
             the last epoch it took part in). To rebuild it, put back each missing pack (epoch(s) \
             {}) from {import}.",
            last + 1,
            runs(&past),
        )),
        _ => ways.push_str(&format!(
            " To rebuild it, put back each missing `epoch-N` directory from a backup taken after \
             that epoch closed (epoch(s) {}; a sealed epoch's pack never changes) or {import}.",
            runs(&past),
        )),
    }
    if resume_lost {
        ways.push_str(&format!(
            " Epoch {resume} is the epoch the node resumes, whose pack it writes: neither a backup \
             taken after an epoch closed nor `db import-epoch` (which refuses that epoch) puts its \
             pack back. Restore the whole `consensus-db` from a backup taken with the node stopped \
             (a validator whose whole `consensus-db/` is restored stays stopped until the network \
             has closed the epoch it was last in)."
        ));
    }
    let swap_too = if resume_lost && resume > 0 {
        format!(" (its start opens the pack of epoch {resume} too)")
    } else {
        String::new()
    };
    bail!(
        "{} cannot rebuild its execution database from genesis: the rebuild re-executes every \
         epoch from 0 from this datadir's own consensus packs, and its consensus database ({held}) \
         lacks the pack of epoch(s) {lost}, so every start would stop at the first missing one: \
         the node resumes from epoch {resume}, its start opens that epoch's pack, and it fetches \
         only the packs of the epochs after it.{fetched}{ways} Or start over: move `consensus-db` \
         aside as well, so the node syncs from genesis as a new one does (a validator then stays \
         stopped until the network has closed the last epoch it took part in). For the \
         side-observer swap, which copies a synced observer's execution database in instead of \
         rebuilding, pass `--for-swap`{swap_too}",
        datadir.display(),
    )
}

/// What the moved-aside execution database is replaced with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResetMode {
    /// The next start rebuilds it from genesis, replaying the node's own consensus packs from
    /// epoch 0: refuse a datadir that lacks one ([`refuse_missing_packs`]).
    Rebuild,
    /// The side-observer swap copies a synced observer's execution database in before the next
    /// start, which executes only the consensus output past the copy's tip: no pack check.
    Swap,
}

/// The UTC time `secs` after the Unix epoch, as `YYYYMMDDTHHMMSSZ`.
fn utc_stamp(secs: u64) -> String {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    // days since 1970-01-01 to a civil date (proleptic Gregorian), after Howard Hinnant's
    // `civil_from_days`
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z", rem / 3_600, rem % 3_600 / 60, rem % 60)
}

/// What a reset did, or would do without `--force`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ResetPlan {
    /// The entries of the datadir to move, relative to it.
    pub(crate) moves: Vec<PathBuf>,
    /// The directory they move into, under its final name.
    pub(crate) target: PathBuf,
    /// An interrupted reset's directory this one finishes, if any.
    pub(crate) resumes: Option<PathBuf>,
    /// The bytes the execution database being moved aside holds (its files' lengths, links not
    /// followed). It keeps them until the reset directory is deleted, while the rebuild (or the
    /// swap's copy) writes a new database of about that size on the datadir's filesystem.
    pub(crate) moved_bytes: u64,
    /// The bytes free on the datadir's filesystem, when the system reports them.
    pub(crate) available_bytes: Option<u64>,
    /// Moved entries that are links: the reset moves the link, not what it points to, and the
    /// next start creates a new entry in the datadir, on the datadir's filesystem.
    pub(crate) links: Vec<(PathBuf, PathBuf)>,
}

/// The bytes the files under `path` hold (their lengths), not following links.
fn bytes_under(path: &Path) -> eyre::Result<u64> {
    let meta = fs::symlink_metadata(path)
        .wrap_err_with(|| format!("failed to check {}", path.display()))?;
    if !meta.is_dir() {
        return Ok(if meta.is_file() { meta.len() } else { 0 });
    }
    let mut total = 0;
    for entry in
        fs::read_dir(path).wrap_err_with(|| format!("failed to list {}", path.display()))?
    {
        let entry = entry.wrap_err_with(|| format!("failed to list {}", path.display()))?;
        total += bytes_under(&entry.path())?;
    }
    Ok(total)
}

/// Refuse a move the reset's rename cannot make: an entry on another filesystem than the datadir
/// (`rename(2)` fails there, `EXDEV` or, for the mount point of one, `EBUSY`). Checked before
/// anything moves. It compares `st_dev`, so it catches another filesystem only: a bind mount from
/// the datadir's own filesystem passes, its rename then fails with `EBUSY`, and the reset moves
/// back what it had moved ([`move_back`]).
#[cfg(unix)]
fn refuse_other_filesystem(datadir: &Path, entry: &Path) -> eyre::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let path = datadir.join(entry);
    let meta = fs::symlink_metadata(&path)
        .wrap_err_with(|| format!("failed to check {}", path.display()))?;
    let datadir_meta =
        fs::metadata(datadir).wrap_err_with(|| format!("failed to check {}", datadir.display()))?;
    if meta.dev() != datadir_meta.dev() {
        bail!(
            "{} is a mount point, or on another filesystem than the datadir: the reset moves the \
             execution database aside by renaming it within the datadir and cannot move it. Move \
             its contents aside by hand, or unmount it and run the reset again",
            path.display(),
        );
    }
    Ok(())
}

/// Refuse while a process holds the execution database open: tn-lmdb holds an exclusive `flock` on
/// `db/tn-lmdb.dat` for as long as it is open. This backs up the datadir lock, which a node no
/// longer holds once its pid file has been deleted under it. A file without the lock, or no file,
/// passes.
fn refuse_open_execution_database(datadir: &Path) -> eyre::Result<()> {
    let path = datadir.join("db").join("tn-lmdb.dat");
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(eyre!("failed to open {}: {e}", path.display())),
    };
    match file.try_lock() {
        // released when `file` closes, on return
        Ok(()) => Ok(()),
        Err(fs::TryLockError::WouldBlock) => bail!(
            "a process holds the execution database {} open: stop the node before resetting it",
            path.display(),
        ),
        Err(fs::TryLockError::Error(e)) => {
            Err(eyre!("failed to check whether {} is open: {e}", path.display()))
        }
    }
}

/// Move `datadir`'s execution database aside (with `force`), or report what would move (without
/// it). Holds the datadir lock for the whole run, so it refuses while a node runs. Refuses, before
/// anything moves, a datadir the node refuses for an unfinished `db load-state`
/// ([`refuse_interrupted_load_state`]), and for [`ResetMode::Rebuild`] one whose rebuild would stop
/// at a missing pack ([`refuse_missing_packs`]).
pub(crate) fn reset_execution(
    datadir: &Path,
    force: bool,
    mode: ResetMode,
) -> eyre::Result<ResetPlan> {
    super::require_datadir(datadir)?;
    let _pid_lock = PidLock::acquire(&datadir.to_path_buf())?;
    // the node refuses this datadir until `db load-state` is rerun, which redoes the restore
    refuse_interrupted_load_state(datadir)?;
    let partials = partial_resets(datadir)?;
    if mode == ResetMode::Rebuild {
        if let Err(refusal) = refuse_missing_packs(datadir) {
            // a swap's reset stopped between its moves is finished by a swap's rerun, which skips
            // this check: the node's refusal sends the operator here first
            for partial in &partials {
                if !is_empty_dir(partial)? {
                    bail!(
                        "{refusal:#}. This datadir also holds the unfinished reset {}: if it was \
                         started with `--for-swap`, `db reset-execution --for-swap --force` \
                         finishes it",
                        partial.display(),
                    );
                }
            }
            return Err(refusal);
        }
    }

    let entries = execution_entries(datadir)?;
    if partials.len() > 1 {
        bail!(
            "{} holds more than one unfinished execution reset ({}): resolve them by hand",
            datadir.display(),
            partials.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", "),
        );
    }
    let resumes = partials.into_iter().next();
    let mut moves = Vec::new();
    let mut moved_bytes = 0;
    for entry in &entries {
        if let Some(partial) = &resumes {
            if exists(&partial.join(entry))? {
                moved_bytes += bytes_under(&partial.join(entry))?;
            }
        }
        if exists(&datadir.join(entry))? {
            if let Some(partial) = &resumes {
                if exists(&partial.join(entry))? {
                    bail!(
                        "{} is both in the datadir and in the unfinished reset {}: a node created \
                         it after the reset stopped, or it was copied back. Resolve it by hand",
                        entry.display(),
                        partial.display(),
                    );
                }
            }
            moves.push(entry.clone());
        }
    }
    if moves.is_empty() && resumes.is_none() {
        bail!(
            "{} holds no execution database (none of {} exists): nothing to reset",
            datadir.display(),
            entries.iter().map(|e| e.display().to_string()).collect::<Vec<_>>().join(", "),
        );
    }
    refuse_open_execution_database(datadir)?;
    let mut links = Vec::new();
    for entry in &moves {
        let path = datadir.join(entry);
        #[cfg(unix)]
        refuse_other_filesystem(datadir, entry)?;
        if fs::symlink_metadata(&path)
            .wrap_err_with(|| format!("failed to check {}", path.display()))?
            .is_symlink()
        {
            let to = fs::read_link(&path)
                .wrap_err_with(|| format!("failed to read the link {}", path.display()))?;
            links.push((entry.clone(), to));
        }
        moved_bytes += bytes_under(&path)?;
    }
    let available_bytes = available_bytes(datadir).ok();

    let staging = match &resumes {
        Some(partial) => partial.clone(),
        None => {
            let secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .wrap_err("the system clock is before the Unix epoch")?
                .as_secs();
            datadir.join(format!("{RESET_DIR_PREFIX}{}{PARTIAL_SUFFIX}", utc_stamp(secs)))
        }
    };
    let target = final_name(&staging)?;
    if let Some(partial) = &resumes {
        // the name is the unfinished reset's own, so a rerun meets the same directory
        if exists(&target)? {
            bail!(
                "{} already exists beside the unfinished reset {}, which finishing renames to that \
                 name: rename or move {} out of the datadir, then run the reset again",
                target.display(),
                partial.display(),
                target.display(),
            );
        }
    }
    let plan = ResetPlan { moves, target, resumes, moved_bytes, available_bytes, links };
    if !force {
        return Ok(plan);
    }

    if exists(&plan.target)? {
        // a new reset's name is the current second's, so a rerun a second later gets another one
        bail!("{} already exists: run the reset again in a second", plan.target.display());
    }
    let created = plan.resumes.is_none();
    if created {
        fs::create_dir(&staging)
            .wrap_err_with(|| format!("failed to create {}", staging.display()))?;
        sync_dir(datadir)?;
    }
    // a failed move (a bind mount's rename fails with `EBUSY` though the filesystem check passed
    // it) moves back what this run moved, so it leaves no half-moved reset behind
    let mut moved = Vec::new();
    for entry in &plan.moves {
        let from = datadir.join(entry);
        let to = staging.join(entry);
        if let Err(e) = move_entry(&from, &to) {
            let failure = eyre!("failed to move {} to {}: {e}", from.display(), to.display());
            return Err(move_back(datadir, &staging, created, &moved, failure));
        }
        moved.push(entry);
        if let Err(failure) = sync_dir(&staging).and_then(|()| sync_dir(datadir)) {
            return Err(move_back(datadir, &staging, created, &moved, failure));
        }
    }
    fs::rename(&staging, &plan.target).wrap_err_with(|| {
        format!("failed to rename {} to {}", staging.display(), plan.target.display())
    })?;
    sync_dir(datadir)?;
    Ok(plan)
}

/// Undo this run's moves after `failure`: move each entry of `moved` back from `staging` into
/// `datadir`, the last first, sync both, and remove `staging` when this run `created` it and
/// moved everything back. Returns the error to report: `failure`, then what was moved back, or
/// what could not be and is left for the operator.
fn move_back(
    datadir: &Path,
    staging: &Path,
    created: bool,
    moved: &[&PathBuf],
    failure: eyre::Report,
) -> eyre::Report {
    let mut stuck = Vec::new();
    for entry in moved.iter().rev() {
        if let Err(e) = fs::rename(staging.join(entry), datadir.join(entry)) {
            stuck.push(format!("{} ({e})", entry.display()));
        }
    }
    let mut notes = Vec::new();
    if let Err(e) = sync_dir(staging) {
        notes.push(format!("{e:#}"));
    }
    if created && stuck.is_empty() {
        if let Err(e) = fs::remove_dir(staging) {
            notes.push(format!(
                "failed to remove {} ({e}), which is empty and blocks nothing",
                staging.display()
            ));
        }
    }
    if let Err(e) = sync_dir(datadir) {
        notes.push(format!("{e:#}"));
    }
    let notes = if notes.is_empty() { String::new() } else { format!(" ({})", notes.join("; ")) };
    let names = |entries: &[&PathBuf]| {
        entries.iter().map(|e| e.display().to_string()).collect::<Vec<_>>().join(", ")
    };
    if !stuck.is_empty() {
        return eyre!(
            "{failure:#}. Moving back what it had moved failed too: {} stay in {}, and the node \
             refuses this datadir until they are moved back into it by hand{notes}",
            stuck.join(", "),
            staging.display(),
        );
    }
    let back = if moved.is_empty() {
        String::new()
    } else {
        format!(" The reset moved {} back into the datadir:", names(moved))
    };
    if created {
        eyre!("{failure:#}.{back} nothing moved{notes}")
    } else {
        eyre!(
            "{failure:#}.{back} nothing else moved, and the unfinished reset {} holds what it held \
             before this run{notes}",
            staging.display(),
        )
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only: once this many more of the reset's moves on this thread have run, the next one
    /// fails, as the rename of a bind mount from the datadir's own filesystem does (`EBUSY`).
    static FAIL_MOVE_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Rename `from` to `to`, one of the reset's moves. Tests inject its failure.
fn move_entry(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(left) = FAIL_MOVE_AFTER.with(std::cell::Cell::get) {
        FAIL_MOVE_AFTER.with(|fail| fail.set(left.checked_sub(1)));
        if left == 0 {
            return Err(std::io::Error::other("injected move failure"));
        }
    }
    fs::rename(from, to)
}

/// The final name of the reset directory `staging` (`...partial` without its suffix).
fn final_name(staging: &Path) -> eyre::Result<PathBuf> {
    let name = staging
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(PARTIAL_SUFFIX))
        .ok_or_else(|| eyre!("{} is not a reset's staging directory", staging.display()))?;
    Ok(staging.with_file_name(name))
}

/// fsync the directory `dir`, so the renames in it are durable.
fn sync_dir(dir: &Path) -> eyre::Result<()> {
    fsync_directory(dir).wrap_err_with(|| format!("failed to fsync {}", dir.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Put a pack's data log for `epoch` in `dir` (an `epoch-N` or `epoch-N.replaced` directory).
    fn pack(dir: &Path) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join(tn_storage::consensus_pack::DATA_NAME), b"pack").unwrap();
    }

    /// A datadir with every entry a reset moves, and the entries it leaves: its consensus database
    /// holds the packs of epochs 0 and 1, as a node that synced from genesis does.
    fn datadir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("db")).unwrap();
        fs::write(root.join("db/tn-lmdb.dat"), b"database").unwrap();
        fs::create_dir_all(root.join("static_files")).unwrap();
        fs::write(root.join("static_files/static_file_headers_0_499999"), b"headers").unwrap();
        fs::write(root.join("restored-state-floor"), b"42\n").unwrap();
        pack(&root.join("consensus-db/epochs/epoch-0"));
        pack(&root.join("consensus-db/epochs/epoch-1"));
        fs::create_dir_all(root.join("node-keys")).unwrap();
        dir
    }

    /// The names of the entries directly in `dir`, sorted.
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn utc_stamp_formats_civil_time() {
        assert_eq!(utc_stamp(0), "19700101T000000Z");
        // 2000-02-29 (a leap day of a century leap year) 23:59:59
        assert_eq!(utc_stamp(951_868_799), "20000229T235959Z");
        // 2026-11-09 16:32:05
        assert_eq!(utc_stamp(1_794_241_925), "20261109T163205Z");
        // 2100-03-01, after a century year that is not a leap year
        assert_eq!(utc_stamp(4_107_542_400), "21000301T000000Z");
    }

    /// Without `--force` nothing moves; with it the three execution entries move into one dated
    /// directory, with their contents, and everything else stays.
    #[test]
    fn a_reset_moves_the_execution_entries_aside() {
        let dir = datadir();
        let root = dir.path();
        let plan = reset_execution(root, false, ResetMode::Rebuild).unwrap();
        assert_eq!(
            plan.moves,
            ["db", "static_files", "restored-state-floor"].map(PathBuf::from).to_vec()
        );
        assert_eq!(plan.resumes, None);
        assert_eq!(plan.moved_bytes, (b"database".len() + b"headers".len() + b"42\n".len()) as u64);
        assert!(plan.available_bytes.is_some());
        assert!(plan.links.is_empty());
        assert_eq!(
            names(root),
            [
                "consensus-db",
                "db",
                "node-keys",
                "restored-state-floor",
                "static_files",
                "telcoin.pid"
            ],
            "a dry run moves nothing"
        );

        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        let target_name = plan.target.file_name().unwrap().to_string_lossy().into_owned();
        assert!(target_name.starts_with("execution-reset-"), "{target_name}");
        assert!(!target_name.ends_with(".partial"), "{target_name}");
        assert_eq!(names(root), ["consensus-db", target_name.as_str(), "node-keys", "telcoin.pid"]);
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
        assert_eq!(fs::read(plan.target.join("db/tn-lmdb.dat")).unwrap(), b"database");
        assert_eq!(fs::read(plan.target.join("restored-state-floor")).unwrap(), b"42\n");
        refuse_interrupted_reset(root).unwrap();

        let err = reset_execution(root, true, ResetMode::Rebuild).unwrap_err().to_string();
        assert!(err.contains("holds no execution database"), "{err}");
    }

    /// A datadir whose consensus database starts at a later epoch (as `db load-state` leaves one)
    /// is refused, by the dry run too, and nothing moves. An epoch-0 pack an interrupted
    /// install moved aside counts, as the node's start puts it back; a datadir with no pack at
    /// all passes, as it syncs from genesis.
    #[test]
    fn a_reset_refuses_consensus_without_the_epoch_zero_pack() {
        let dir = datadir();
        let root = dir.path();
        let epochs = root.join("consensus-db/epochs");
        fs::remove_dir_all(epochs.join("epoch-0")).unwrap();
        for force in [false, true] {
            let err =
                format!("{:#}", reset_execution(root, force, ResetMode::Rebuild).unwrap_err());
            assert!(err.contains("(packs up to epoch 1) lacks the pack of epoch(s) 0,"), "{err}");
            assert!(err.contains("move `consensus-db` aside"), "{err}");
            assert!(err.contains("until the network has closed the last epoch"), "{err}");
            assert!(err.contains("pass `--for-swap`"), "{err}");
            assert_eq!(
                names(root),
                [
                    "consensus-db",
                    "db",
                    "node-keys",
                    "restored-state-floor",
                    "static_files",
                    "telcoin.pid"
                ],
                "nothing moved"
            );
            assert_eq!(fs::read(root.join("db/tn-lmdb.dat")).unwrap(), b"database");
        }

        pack(&epochs.join("epoch-0.replaced"));
        reset_execution(root, false, ResetMode::Rebuild).unwrap();
        fs::remove_dir_all(root.join("consensus-db")).unwrap();
        reset_execution(root, false, ResetMode::Rebuild).unwrap();
    }

    /// A gap below the latest pack (epoch 0 present, a later epoch lost) is refused as the missing
    /// epoch 0 is, by the dry run too, and nothing moves: the rebuild would stop at the gap on
    /// every start. A pack counts only as the node's start finds it, its data log: an `epoch-N`
    /// directory without one is a gap, and an interrupted install's aside holding one fills it.
    /// The newest epoch's directory without a data log (its creation stopped first) is no gap: the
    /// node creates it again when it enters the epoch. Each lost epoch gets only the recoveries
    /// that put it back: a backup or another node's copy for a gap between packs, and for the
    /// shape `db load-state` leaves (no pack below the restored epoch, which it never held) a
    /// rerun of `db load-state` or other nodes' copies, not a backup.
    #[test]
    fn a_reset_refuses_a_gap_in_the_packs() {
        let dir = datadir();
        let root = dir.path();
        let epochs = root.join("consensus-db/epochs");
        for epoch in [2, 3, 5, 6] {
            pack(&epochs.join(format!("epoch-{epoch}")));
        }
        // the node resumes from epoch 6, so it fetches no pack below it again
        ConsensusChain::write_latest_consensus_hint(&epochs, 6, 60).unwrap();
        // epoch 4 is lost; epoch 1's directory lost its data log
        fs::remove_file(epochs.join("epoch-1").join(tn_storage::consensus_pack::DATA_NAME))
            .unwrap();
        for force in [false, true] {
            let err =
                format!("{:#}", reset_execution(root, force, ResetMode::Rebuild).unwrap_err());
            assert!(
                err.contains("(packs up to epoch 6) lacks the pack of epoch(s) 1, 4,"),
                "{err}"
            );
            assert!(
                err.contains(
                    "each missing `epoch-N` directory from a backup taken after that epoch closed \
                     (epoch(s) 1, 4;"
                ),
                "{err}"
            );
            assert!(err.contains("`db import-epoch --epoch N --from <copy>`"), "{err}");
            assert!(!err.contains("`db load-state` again"), "{err}");
            assert!(err.contains("pass `--for-swap`"), "{err}");
            assert_eq!(
                names(root),
                [
                    "consensus-db",
                    "db",
                    "node-keys",
                    "restored-state-floor",
                    "static_files",
                    "telcoin.pid"
                ],
                "nothing moved"
            );
        }

        pack(&epochs.join("epoch-1"));
        pack(&epochs.join("epoch-4.replaced"));
        fs::create_dir(epochs.join("epoch-7")).unwrap();
        reset_execution(root, false, ResetMode::Rebuild).unwrap();
        // the swap replays nothing below the copy's tip, so a gap does not refuse it
        fs::remove_dir_all(epochs.join("epoch-4.replaced")).unwrap();
        reset_execution(root, false, ResetMode::Swap).unwrap();

        // the shape `db load-state` leaves: only the restored epoch's pack, the hint at it
        let dir = datadir();
        let root = dir.path();
        let epochs = root.join("consensus-db/epochs");
        fs::remove_dir_all(epochs.join("epoch-0")).unwrap();
        fs::remove_dir_all(epochs.join("epoch-1")).unwrap();
        pack(&epochs.join("epoch-3"));
        ConsensusChain::write_latest_consensus_hint(&epochs, 3, 30).unwrap();
        let err = format!("{:#}", reset_execution(root, false, ResetMode::Rebuild).unwrap_err());
        assert!(err.contains("(packs up to epoch 3) lacks the pack of epoch(s) 0..=2,"), "{err}");
        assert!(err.contains("run `db load-state` again"), "{err}");
        assert!(
            err.contains(
                "with `db`, `static_files`, `restored-state-floor` and `consensus-db` moved aside \
                 (its import creates the restored epoch's pack and refuses a datadir that holds \
                 one; a validator then stays stopped until the network has closed the last epoch \
                 it took part in)"
            ),
            "{err}"
        );
        assert!(err.contains("`db import-epoch --epoch N --from <copy>`"), "{err}");
        assert!(!err.contains("from a backup"), "it never held them: {err}");
        assert!(err.contains("pass `--for-swap`"), "{err}");

        // the lowest pack lost too, with a gap above it: no longer that shape
        pack(&epochs.join("epoch-1"));
        let err = format!("{:#}", reset_execution(root, false, ResetMode::Rebuild).unwrap_err());
        assert!(err.contains("lacks the pack of epoch(s) 0, 2,"), "{err}");
        assert!(
            err.contains("from a backup taken after that epoch closed (epoch(s) 0, 2;"),
            "{err}"
        );
        assert!(err.contains("`db import-epoch --epoch N --from <copy>`"), "{err}");
        assert!(!err.contains("`db load-state` again"), "{err}");
    }

    #[test]
    fn missing_epochs_print_as_runs() {
        assert!(missing_ranges(&[0, 1, 2], 2).is_empty());
        assert_eq!(runs(&missing_ranges(&[3], 3)), "0..=2");
        assert_eq!(runs(&missing_ranges(&[0, 2, 3, 7], 7)), "1, 4..=6");
        assert_eq!(runs(&missing_ranges(&[1], 1)), "0");
        // through an end past every pack held, and not past the end
        assert_eq!(runs(&missing_ranges(&[], 5)), "0..=5");
        assert_eq!(runs(&missing_ranges(&[0, 1], 3)), "2..=3");
        assert_eq!(runs(&missing_ranges(&[0, 2, 9], 5)), "1, 3..=5");
        assert_eq!(missing_ranges(&[Epoch::MAX], Epoch::MAX), [(0, Epoch::MAX - 1)]);
    }

    #[test]
    fn missing_epochs_split_at_the_resume_epoch() {
        let ranges = missing_ranges(&[0, 2, 3, 7], 7);
        let split = |epoch| split_ranges(&ranges, epoch);
        assert_eq!(split(0), SplitRanges { at_or_below: vec![], above: vec![(1, 1), (4, 6)] });
        assert_eq!(split(1), SplitRanges { at_or_below: vec![(1, 1)], above: vec![(4, 6)] });
        assert_eq!(
            split(5),
            SplitRanges { at_or_below: vec![(1, 1), (4, 5)], above: vec![(6, 6)] }
        );
        assert_eq!(split(6), SplitRanges { at_or_below: vec![(1, 1), (4, 6)], above: vec![] });
        assert_eq!(split(9), SplitRanges { at_or_below: vec![(1, 1), (4, 6)], above: vec![] });
    }

    /// A missing pack after the epoch the node resumes from (its latest-consensus hint) does not
    /// refuse a rebuild: the node's start fetches it from peers, as after a catch-up stopped while
    /// several pack imports ran. One at or below it does, by the dry run too, and the refusal
    /// names the ones the start fetches apart from the ones it cannot.
    #[test]
    fn a_reset_refuses_only_the_gaps_the_node_cannot_fetch_again() {
        let dir = datadir();
        let root = dir.path();
        let epochs = root.join("consensus-db/epochs");
        pack(&epochs.join("epoch-3"));
        // packs 0, 1, 3: epoch 2 is missing
        ConsensusChain::write_latest_consensus_hint(&epochs, 1, 10).unwrap();
        // the dry run runs the same check `--force` does, and moves nothing
        let plan = reset_execution(root, false, ResetMode::Rebuild);
        assert!(plan.is_ok(), "the start fetches epoch 2: {plan:?}");
        ConsensusChain::write_latest_consensus_hint(&epochs, 3, 30).unwrap();
        for force in [false, true] {
            let err =
                format!("{:#}", reset_execution(root, force, ResetMode::Rebuild).unwrap_err());
            assert!(err.contains("(packs up to epoch 3) lacks the pack of epoch(s) 2,"), "{err}");
            assert!(err.contains("the node resumes from epoch 3"), "{err}");
            assert!(!err.contains("missing too"), "no gap after epoch 3: {err}");
            assert_eq!(names(root).len(), 6, "nothing moved: {:?}", names(root));
        }

        // packs 0, 1, 3, 5 resuming from epoch 3: epoch 2 cannot be fetched again, epoch 4 can
        pack(&epochs.join("epoch-5"));
        let err = format!("{:#}", reset_execution(root, false, ResetMode::Rebuild).unwrap_err());
        assert!(err.contains("(packs up to epoch 5) lacks the pack of epoch(s) 2,"), "{err}");
        assert!(
            err.contains(
                "The pack(s) of epoch(s) 4 are missing too, but those are after epoch 3, so the \
                 node's next start fetches them from peers"
            ),
            "{err}"
        );
        pack(&epochs.join("epoch-2"));
        reset_execution(root, false, ResetMode::Rebuild).expect("only epoch 4 is missing");
    }

    /// The pack of the epoch the node resumes from is one its start opens, so a missing one
    /// refuses a rebuild even with no pack after it, by the dry run too, and nothing moves. The
    /// refusal names it apart: the node writes that epoch, so neither a backup taken after an
    /// epoch closed nor `db import-epoch` puts it back. With no pack at all, a hint past epoch 0
    /// refuses every epoch up to it; with no slot files either the datadir starts as a new node
    /// does and passes.
    #[test]
    fn a_reset_refuses_a_missing_resume_epoch_pack() {
        let dir = datadir();
        let root = dir.path();
        let epochs = root.join("consensus-db/epochs");
        for epoch in 2..=4 {
            pack(&epochs.join(format!("epoch-{epoch}")));
        }
        ConsensusChain::write_latest_consensus_hint(&epochs, 5, 50).unwrap();
        for force in [false, true] {
            let err =
                format!("{:#}", reset_execution(root, force, ResetMode::Rebuild).unwrap_err());
            assert!(err.contains("(packs up to epoch 4) lacks the pack of epoch(s) 5,"), "{err}");
            assert!(err.contains("Epoch 5 is the epoch the node resumes"), "{err}");
            assert!(err.contains("Restore the whole `consensus-db`"), "{err}");
            assert!(!err.contains("`db import-epoch --epoch N"), "no past epoch is lost: {err}");
            assert_eq!(names(root).len(), 6, "nothing moved: {:?}", names(root));
        }

        for epoch in 0..=4 {
            fs::remove_dir_all(epochs.join(format!("epoch-{epoch}"))).unwrap();
        }
        let err = format!("{:#}", reset_execution(root, false, ResetMode::Rebuild).unwrap_err());
        assert!(err.contains("(no pack) lacks the pack of epoch(s) 0..=5,"), "{err}");
        assert!(err.contains("Epoch 5 is the epoch the node resumes"), "{err}");

        for slot in ["consensus_slot1", "consensus_slot2"] {
            let _ = fs::remove_file(epochs.join(slot));
        }
        reset_execution(root, false, ResetMode::Rebuild).expect("a new node's datadir");
    }

    /// The side-observer swap (`--for-swap`) moves the execution database of a datadir whose
    /// consensus database starts at a later epoch, as for a validator bootstrapped with `db
    /// load-state`: the copied database replaces it, so no replay of epoch 0 follows. The consensus
    /// database stays.
    #[test]
    fn a_swap_reset_moves_a_datadir_without_the_epoch_zero_pack() {
        let dir = datadir();
        let root = dir.path();
        fs::remove_dir_all(root.join("consensus-db/epochs/epoch-0")).unwrap();
        let plan = reset_execution(root, false, ResetMode::Swap).unwrap();
        assert_eq!(
            plan.moves,
            ["db", "static_files", "restored-state-floor"].map(PathBuf::from).to_vec()
        );
        let plan = reset_execution(root, true, ResetMode::Swap).unwrap();
        let target_name = plan.target.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(names(root), ["consensus-db", target_name.as_str(), "node-keys", "telcoin.pid"]);
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
        assert_eq!(names(&root.join("consensus-db/epochs")), ["epoch-1"]);
    }

    /// A datadir an unfinished `db load-state` left is refused with the node's own message, in
    /// both modes and by the dry run too, and nothing moves; once the restore is finished (its
    /// marker gone) the reset runs.
    #[test]
    fn a_reset_refuses_an_unfinished_load_state() {
        let dir = datadir();
        let root = dir.path();
        super::super::load_state_marker::write(root, &[]).unwrap();
        let node = format!("{:#}", refuse_interrupted_load_state(root).unwrap_err());
        for mode in [ResetMode::Rebuild, ResetMode::Swap] {
            for force in [false, true] {
                let err = format!("{:#}", reset_execution(root, force, mode).unwrap_err());
                assert_eq!(err, node, "the node's refusal");
                assert!(err.contains("Run `telcoin-network db load-state` again"), "{err}");
                assert_eq!(
                    names(root),
                    [
                        "consensus-db",
                        "db",
                        "load-state-in-progress",
                        "node-keys",
                        "restored-state-floor",
                        "static_files",
                        "telcoin.pid"
                    ],
                    "nothing moved"
                );
            }
        }
        super::super::load_state_marker::remove(root).unwrap();
        reset_execution(root, true, ResetMode::Rebuild).unwrap();
    }

    /// A datadir without a floor marker moves what it has.
    #[test]
    fn a_reset_moves_only_what_exists() {
        let dir = datadir();
        let root = dir.path();
        fs::remove_file(root.join("restored-state-floor")).unwrap();
        fs::remove_dir_all(root.join("static_files")).unwrap();
        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(plan.moves, [PathBuf::from("db")]);
        assert_eq!(names(&plan.target), ["db"]);
    }

    /// The command refuses while a process holds the execution database open (tn-lmdb's `flock` on
    /// `db/tn-lmdb.dat`), as a node does whose pid file was deleted under it, and moves nothing.
    #[test]
    fn a_reset_refuses_while_the_execution_database_is_open() {
        let dir = datadir();
        let root = dir.path();
        let held = fs::File::open(root.join("db/tn-lmdb.dat")).unwrap();
        held.lock().unwrap();
        for force in [false, true] {
            let err = reset_execution(root, force, ResetMode::Rebuild).unwrap_err().to_string();
            assert!(err.contains("holds the execution database"), "{err}");
            assert!(root.join("db/tn-lmdb.dat").exists(), "nothing moved");
        }
        drop(held);
        reset_execution(root, true, ResetMode::Rebuild).unwrap();
    }

    /// A linked `db` is reported: the reset moves the link and leaves what it points to.
    #[cfg(unix)]
    #[test]
    fn a_reset_reports_a_linked_entry() {
        let dir = datadir();
        let root = dir.path();
        let elsewhere = tempfile::tempdir().unwrap();
        fs::rename(root.join("db"), elsewhere.path().join("db")).unwrap();
        std::os::unix::fs::symlink(elsewhere.path().join("db"), root.join("db")).unwrap();
        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(plan.links, [(PathBuf::from("db"), elsewhere.path().join("db"))]);
        assert!(plan.target.join("db").is_symlink());
        assert!(elsewhere.path().join("db/tn-lmdb.dat").exists(), "the link's target stays");
    }

    /// The command refuses while a node holds the datadir lock, and moves nothing.
    #[cfg(unix)]
    #[test]
    fn a_reset_refuses_while_a_node_runs() {
        let dir = datadir();
        let root = dir.path().to_path_buf();
        let held = PidLock::acquire(&root).unwrap();
        let err = reset_execution(&root, true, ResetMode::Rebuild).unwrap_err().to_string();
        assert!(err.contains("another telcoin process"), "{err}");
        assert!(root.join("db/tn-lmdb.dat").exists(), "nothing moved");
        drop(held);
    }

    /// A move that fails after an earlier one moved (as the rename of a bind mount from the
    /// datadir's own filesystem does, which the filesystem check passes) moves back what this run
    /// moved and removes the staging directory it created: the datadir is as it was, no
    /// unfinished reset blocks the node, and the error says so. A rerun finishing an unfinished
    /// reset moves back only its own moves and keeps that reset's directory and what it held.
    #[test]
    fn a_failed_move_moves_the_entries_back() {
        let dir = datadir();
        let root = dir.path();
        FAIL_MOVE_AFTER.with(|fail| fail.set(Some(1)));
        let err = format!("{:#}", reset_execution(root, true, ResetMode::Rebuild).unwrap_err());
        assert!(err.contains("injected move failure"), "{err}");
        assert!(err.contains("moved db back into the datadir"), "{err}");
        assert!(err.contains("nothing moved"), "{err}");
        assert_eq!(
            names(root),
            [
                "consensus-db",
                "db",
                "node-keys",
                "restored-state-floor",
                "static_files",
                "telcoin.pid"
            ],
            "db is back and no staging directory is left"
        );
        assert_eq!(fs::read(root.join("db/tn-lmdb.dat")).unwrap(), b"database");
        refuse_interrupted_reset(root).unwrap();

        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir(&partial).unwrap();
        fs::rename(root.join("db"), partial.join("db")).unwrap();
        FAIL_MOVE_AFTER.with(|fail| fail.set(Some(1)));
        let err = format!("{:#}", reset_execution(root, true, ResetMode::Rebuild).unwrap_err());
        assert!(err.contains("moved static_files back into the datadir"), "{err}");
        assert!(err.contains(&partial.display().to_string()), "{err}");
        assert_eq!(names(&partial), ["db"], "the unfinished reset holds what it held");
        assert!(root.join("static_files").exists());
        assert!(root.join("restored-state-floor").exists());

        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
    }

    /// A reset stopped between two moves blocks a node's start, and a rerun finishes it into the
    /// same directory.
    #[test]
    fn an_interrupted_reset_blocks_the_node_and_a_rerun_finishes_it() {
        let dir = datadir();
        let root = dir.path();
        // the state a stop after the first move leaves
        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir(&partial).unwrap();
        fs::rename(root.join("db"), partial.join("db")).unwrap();

        let err = refuse_interrupted_reset(root).unwrap_err().to_string();
        assert!(err.contains("did not finish"), "{err}");

        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(plan.resumes.as_deref(), Some(partial.as_path()));
        assert_eq!(plan.target, root.join("execution-reset-20261109T163205Z"));
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
        refuse_interrupted_reset(root).unwrap();
    }

    /// An interrupted side-observer swap's reset on a datadir whose rebuild the pack check refuses:
    /// the node's refusal names `--for-swap`, a rerun without it refuses naming the unfinished
    /// reset and how to finish it, and a swap's rerun finishes it.
    #[test]
    fn an_interrupted_swap_reset_is_finished_by_a_swap_rerun() {
        let dir = datadir();
        let root = dir.path();
        let epochs = root.join("consensus-db/epochs");
        // the shape `db load-state` leaves, which only the swap resets
        fs::remove_dir_all(epochs.join("epoch-0")).unwrap();
        ConsensusChain::write_latest_consensus_hint(&epochs, 1, 10).unwrap();
        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir(&partial).unwrap();
        fs::rename(root.join("db"), partial.join("db")).unwrap();

        let node = refuse_interrupted_reset(root).unwrap_err().to_string();
        assert!(node.contains("with `--for-swap` if it was the side-observer swap's"), "{node}");
        for force in [false, true] {
            let err =
                format!("{:#}", reset_execution(root, force, ResetMode::Rebuild).unwrap_err());
            assert!(err.contains("lacks the pack of epoch(s) 0,"), "{err}");
            assert!(
                err.contains(&format!(
                    "This datadir also holds the unfinished reset {}",
                    partial.display()
                )),
                "{err}"
            );
            assert!(err.contains("`db reset-execution --for-swap --force` finishes it"), "{err}");
            assert_eq!(names(&partial), ["db"], "nothing moved");
        }

        let plan = reset_execution(root, true, ResetMode::Swap).unwrap();
        assert_eq!(plan.resumes.as_deref(), Some(partial.as_path()));
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
        refuse_interrupted_reset(root).unwrap();
    }

    /// An interrupted reset undone as its refusal says (its entries moved back) leaves an empty
    /// staging directory, which blocks nothing; a later reset fills it.
    #[test]
    fn an_interrupted_reset_moved_back_no_longer_blocks_the_node() {
        let dir = datadir();
        let root = dir.path();
        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir(&partial).unwrap();
        fs::rename(root.join("db"), partial.join("db")).unwrap();
        assert!(refuse_interrupted_reset(root).is_err());

        fs::rename(partial.join("db"), root.join("db")).unwrap();
        refuse_interrupted_reset(root).unwrap();
        assert_eq!(fs::read(root.join("db/tn-lmdb.dat")).unwrap(), b"database");

        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(plan.resumes.as_deref(), Some(partial.as_path()));
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
    }

    /// The resume branch where every entry is in the staging directory but the final rename did
    /// not happen (a stop just before it): the rerun moves nothing and only renames.
    #[test]
    fn a_reset_stopped_before_its_final_rename_is_finished_by_a_rerun() {
        let dir = datadir();
        let root = dir.path();
        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir(&partial).unwrap();
        for entry in ["db", "static_files", "restored-state-floor"] {
            fs::rename(root.join(entry), partial.join(entry)).unwrap();
        }
        assert!(refuse_interrupted_reset(root).is_err());

        let plan = reset_execution(root, false, ResetMode::Rebuild).unwrap();
        assert!(plan.moves.is_empty(), "{:?}", plan.moves);
        assert_eq!(plan.resumes.as_deref(), Some(partial.as_path()));
        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(plan.target, root.join("execution-reset-20261109T163205Z"));
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
        assert_eq!(
            names(root),
            ["consensus-db", "execution-reset-20261109T163205Z", "node-keys", "telcoin.pid"]
        );
        refuse_interrupted_reset(root).unwrap();
    }

    /// A finished reset under the name an unfinished one finishes into is refused, by the dry run
    /// too, with advice a rerun can follow (not "again in a second": the name is fixed).
    #[test]
    fn a_rerun_refuses_a_finished_reset_under_its_name() {
        let dir = datadir();
        let root = dir.path();
        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir(&partial).unwrap();
        fs::rename(root.join("db"), partial.join("db")).unwrap();
        let finished = root.join("execution-reset-20261109T163205Z");
        fs::create_dir(&finished).unwrap();
        for force in [false, true] {
            let err = reset_execution(root, force, ResetMode::Rebuild).unwrap_err().to_string();
            assert!(err.contains(&finished.display().to_string()), "{err}");
            assert!(err.contains(&partial.display().to_string()), "{err}");
            assert!(err.contains("rename or move"), "{err}");
            assert!(!err.contains("in a second"), "{err}");
            assert!(root.join("static_files").exists(), "nothing moved");
            assert_eq!(names(&partial), ["db"]);
        }

        fs::remove_dir(&finished).unwrap();
        let plan = reset_execution(root, true, ResetMode::Rebuild).unwrap();
        assert_eq!(plan.target, finished);
        assert_eq!(names(&finished), ["db", "restored-state-floor", "static_files"]);
    }

    /// An entry in both the datadir and an unfinished reset is refused: which one is the node's
    /// is for the operator to say.
    #[test]
    fn a_rerun_refuses_an_entry_in_both_places() {
        let dir = datadir();
        let root = dir.path();
        let partial = root.join("execution-reset-20261109T163205Z.partial");
        fs::create_dir_all(partial.join("db")).unwrap();
        let err = reset_execution(root, true, ResetMode::Rebuild).unwrap_err().to_string();
        assert!(err.contains("is both in the datadir and in the unfinished reset"), "{err}");
        assert!(root.join("db/tn-lmdb.dat").exists(), "nothing moved");
    }
}
