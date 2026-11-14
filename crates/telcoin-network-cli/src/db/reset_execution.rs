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
//! every entry is in it. A node refuses to start while a `.partial` directory is there
//! ([`refuse_interrupted_reset`]): a start between two moves would open a datadir holding half an
//! execution database. Running the command again finishes an interrupted reset into the same
//! directory.
//!
//! The rebuild replays epoch 0 from the node's own consensus packs first, so the command refuses a
//! datadir whose consensus database holds a later epoch's pack and none for epoch 0
//! ([`refuse_consensus_without_epoch_zero`]), as a datadir bootstrapped with `db load-state` does.

use eyre::{bail, eyre, WrapErr as _};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tn_config::{PidLock, TelcoinDirs as _};
use tn_storage::disk::fsync_directory;

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
/// beside nothing, and the node would open it.
pub(crate) fn refuse_interrupted_reset(datadir: &Path) -> eyre::Result<()> {
    if let Some(partial) = partial_resets(datadir)?.first() {
        bail!(
            "an execution reset of {} did not finish: {} holds part of the execution database. \
             Run `telcoin-network db reset-execution --force` to finish it, or move its entries \
             back into the datadir to undo it",
            datadir.display(),
            partial.display(),
        );
    }
    Ok(())
}

/// The epoch of a consensus pack directory named `name` (`epoch-N`), or of one an interrupted
/// install moved aside (`epoch-N.replaced`, which the node's next start puts back when `epoch-N`
/// is missing).
fn pack_epoch(name: &str) -> Option<u32> {
    let name = name.strip_suffix(".replaced").unwrap_or(name);
    name.strip_prefix("epoch-")?.parse().ok()
}

/// Refuse a datadir whose consensus database holds a pack of a later epoch and none for epoch 0.
/// After a reset the node replays epoch 0 from its own packs, and it fetches no pack below the
/// epoch its consensus database is at, so it would fail at that replay on every start. A datadir
/// bootstrapped with `db load-state` is one: it holds consensus only from the restored epoch on. A
/// datadir with no pack at all starts as a new node does and syncs from genesis, so it passes.
fn refuse_consensus_without_epoch_zero(datadir: &Path) -> eyre::Result<()> {
    let epochs = datadir.to_path_buf().epochs_db_path();
    if !exists(&epochs)? {
        return Ok(());
    }
    let mut lowest: Option<u32> = None;
    for entry in fs::read_dir(&epochs)
        .wrap_err_with(|| format!("failed to list the epoch packs in {}", epochs.display()))?
    {
        let entry = entry
            .wrap_err_with(|| format!("failed to list the epoch packs in {}", epochs.display()))?;
        if let Some(epoch) = pack_epoch(&entry.file_name().to_string_lossy()) {
            lowest = Some(lowest.map_or(epoch, |lowest| lowest.min(epoch)));
        }
    }
    match lowest {
        Some(lowest) if lowest > 0 => bail!(
            "{} cannot rebuild its execution database from genesis: its consensus database holds \
             epoch packs from epoch {lowest} on and none for epoch 0 (a datadir bootstrapped with \
             `db load-state` holds consensus only from the restored epoch on), and the rebuild \
             replays epoch 0 from this datadir's packs. To rebuild it, move `consensus-db` aside \
             first, so the node syncs from genesis as a new one does (a validator then stays \
             stopped until the network has closed the epoch it was in)",
            datadir.display(),
        ),
        _ => Ok(()),
    }
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
}

/// Move `datadir`'s execution database aside (with `force`), or report what would move (without
/// it). Holds the datadir lock for the whole run, so it refuses while a node runs. Refuses, before
/// anything moves, a datadir whose rebuild could not start
/// ([`refuse_consensus_without_epoch_zero`]).
pub(crate) fn reset_execution(datadir: &Path, force: bool) -> eyre::Result<ResetPlan> {
    if !datadir.is_dir() {
        bail!("datadir {} does not exist", datadir.display());
    }
    let _pid_lock = PidLock::acquire(&datadir.to_path_buf())?;
    refuse_consensus_without_epoch_zero(datadir)?;

    let entries = execution_entries(datadir)?;
    let partials = partial_resets(datadir)?;
    if partials.len() > 1 {
        bail!(
            "{} holds more than one unfinished execution reset ({}): resolve them by hand",
            datadir.display(),
            partials.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", "),
        );
    }
    let resumes = partials.into_iter().next();
    let mut moves = Vec::new();
    for entry in &entries {
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
    let plan = ResetPlan { moves, target, resumes };
    if !force {
        return Ok(plan);
    }

    if exists(&plan.target)? {
        // a new reset's name is the current second's, so a rerun a second later gets another one
        bail!("{} already exists: run the reset again in a second", plan.target.display());
    }
    if plan.resumes.is_none() {
        fs::create_dir(&staging)
            .wrap_err_with(|| format!("failed to create {}", staging.display()))?;
        sync_dir(datadir)?;
    }
    for entry in &plan.moves {
        let from = datadir.join(entry);
        let to = staging.join(entry);
        fs::rename(&from, &to)
            .wrap_err_with(|| format!("failed to move {} to {}", from.display(), to.display()))?;
        sync_dir(&staging)?;
        sync_dir(datadir)?;
    }
    fs::rename(&staging, &plan.target).wrap_err_with(|| {
        format!("failed to rename {} to {}", staging.display(), plan.target.display())
    })?;
    sync_dir(datadir)?;
    Ok(plan)
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
        fs::create_dir_all(root.join("consensus-db/epochs/epoch-0")).unwrap();
        fs::create_dir_all(root.join("consensus-db/epochs/epoch-1")).unwrap();
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
        let plan = reset_execution(root, false).unwrap();
        assert_eq!(
            plan.moves,
            ["db", "static_files", "restored-state-floor"].map(PathBuf::from).to_vec()
        );
        assert_eq!(plan.resumes, None);
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

        let plan = reset_execution(root, true).unwrap();
        let target_name = plan.target.file_name().unwrap().to_string_lossy().into_owned();
        assert!(target_name.starts_with("execution-reset-"), "{target_name}");
        assert!(!target_name.ends_with(".partial"), "{target_name}");
        assert_eq!(names(root), ["consensus-db", target_name.as_str(), "node-keys", "telcoin.pid"]);
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
        assert_eq!(fs::read(plan.target.join("db/tn-lmdb.dat")).unwrap(), b"database");
        assert_eq!(fs::read(plan.target.join("restored-state-floor")).unwrap(), b"42\n");
        refuse_interrupted_reset(root).unwrap();

        let err = reset_execution(root, true).unwrap_err().to_string();
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
        fs::remove_dir(epochs.join("epoch-0")).unwrap();
        for force in [false, true] {
            let err = format!("{:#}", reset_execution(root, force).unwrap_err());
            assert!(err.contains("from epoch 1 on and none for epoch 0"), "{err}");
            assert!(err.contains("move `consensus-db` aside"), "{err}");
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

        fs::create_dir(epochs.join("epoch-0.replaced")).unwrap();
        reset_execution(root, false).unwrap();
        fs::remove_dir_all(root.join("consensus-db")).unwrap();
        reset_execution(root, false).unwrap();
    }

    /// A datadir without a floor marker moves what it has.
    #[test]
    fn a_reset_moves_only_what_exists() {
        let dir = datadir();
        let root = dir.path();
        fs::remove_file(root.join("restored-state-floor")).unwrap();
        fs::remove_dir_all(root.join("static_files")).unwrap();
        let plan = reset_execution(root, true).unwrap();
        assert_eq!(plan.moves, [PathBuf::from("db")]);
        assert_eq!(names(&plan.target), ["db"]);
    }

    /// The command refuses while a node holds the datadir lock, and moves nothing.
    #[cfg(unix)]
    #[test]
    fn a_reset_refuses_while_a_node_runs() {
        let dir = datadir();
        let root = dir.path().to_path_buf();
        let held = PidLock::acquire(&root).unwrap();
        let err = reset_execution(&root, true).unwrap_err().to_string();
        assert!(err.contains("another telcoin process"), "{err}");
        assert!(root.join("db/tn-lmdb.dat").exists(), "nothing moved");
        drop(held);
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

        let plan = reset_execution(root, true).unwrap();
        assert_eq!(plan.resumes.as_deref(), Some(partial.as_path()));
        assert_eq!(plan.target, root.join("execution-reset-20261109T163205Z"));
        assert_eq!(names(&plan.target), ["db", "restored-state-floor", "static_files"]);
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
            let err = reset_execution(root, force).unwrap_err().to_string();
            assert!(err.contains(&finished.display().to_string()), "{err}");
            assert!(err.contains(&partial.display().to_string()), "{err}");
            assert!(err.contains("rename or move"), "{err}");
            assert!(!err.contains("in a second"), "{err}");
            assert!(root.join("static_files").exists(), "nothing moved");
            assert_eq!(names(&partial), ["db"]);
        }

        fs::remove_dir(&finished).unwrap();
        let plan = reset_execution(root, true).unwrap();
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
        let err = reset_execution(root, true).unwrap_err().to_string();
        assert!(err.contains("is both in the datadir and in the unfinished reset"), "{err}");
        assert!(root.join("db/tn-lmdb.dat").exists(), "nothing moved");
    }
}
