//! The marker `db load-state` keeps in the datadir while it restores (`load-state-in-progress`).
//!
//! A restore commits in several steps: the execution database's header scaffold, then its state,
//! then the consensus packs and epoch records. A run stopped between two of them (killed, out of
//! memory, power lost) leaves a datadir no node can run on: its canonical tip is the snapshot's
//! block while its state, or its consensus, is missing. The marker names that state:
//!
//! - [`write`] puts it in place, fsynced, once the restore has checked that the datadir holds no
//!   chain data and before it writes any (the restored-state floor marker included). It lists the
//!   chain-data directories the run created, one name per line.
//! - [`remove`] takes it away once every step has committed.
//! - A node refuses to start while it is there ([`refuse_interrupted_load_state`]).
//! - A rerun of `db load-state` removes what the stopped run created, then restores again
//!   ([`clear_interrupted_load_state`]). It refuses, with what to remove by hand, when the stopped
//!   run wrote into chain data that was there before it.

use eyre::{bail, eyre, WrapErr as _};
use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};
use tn_storage::disk::{fsync_directory, fsync_file};

/// The marker's name in the datadir.
const MARKER_NAME: &str = "load-state-in-progress";

/// The marker's path in `datadir`.
pub(crate) fn marker_path(datadir: &Path) -> PathBuf {
    datadir.join(MARKER_NAME)
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

/// Remove the file or directory at `path`; an absent one is fine.
fn remove_entry(path: &Path) -> eyre::Result<()> {
    let removed = match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) => Err(e),
    };
    match removed {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(eyre!("failed to remove {}: {e}", path.display())),
    }
}

/// Put the marker in `datadir`, listing `created` (the chain-data directories this run created),
/// and make it durable before the restore writes anything else.
pub(crate) fn write(datadir: &Path, created: &[PathBuf]) -> eyre::Result<()> {
    let path = marker_path(datadir);
    let mut contents = String::new();
    for dir in created {
        let name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| eyre!("{} has no name", dir.display()))?;
        contents.push_str(name);
        contents.push('\n');
    }
    let mut file =
        fs::File::create(&path).wrap_err_with(|| format!("failed to create {}", path.display()))?;
    file.write_all(contents.as_bytes())
        .and_then(|()| fsync_file(&file))
        .wrap_err_with(|| format!("failed to write {}", path.display()))?;
    fsync_directory(datadir).wrap_err_with(|| format!("failed to fsync {}", datadir.display()))
}

/// Remove the marker from `datadir`, durably: the restore is complete.
pub(crate) fn remove(datadir: &Path) -> eyre::Result<()> {
    remove_entry(&marker_path(datadir))?;
    fsync_directory(datadir).wrap_err_with(|| format!("failed to fsync {}", datadir.display()))
}

/// Whether the marker is in `datadir`.
pub(crate) fn present(datadir: &Path) -> eyre::Result<bool> {
    exists(&marker_path(datadir))
}

/// Refuse to start a node on `datadir` while a `db load-state` there is unfinished: the execution
/// and consensus databases it was restoring are incomplete, and the node would fail on them with
/// errors that do not say why.
pub(crate) fn refuse_interrupted_load_state(datadir: &Path) -> eyre::Result<()> {
    if present(datadir)? {
        bail!(
            "a `db load-state` into {} did not finish ({} is there): the execution and consensus \
             databases it was restoring are incomplete. Run `telcoin-network db load-state` again \
             with the same bundle: it removes what the unfinished run created and restores again",
            datadir.display(),
            marker_path(datadir).display(),
        );
    }
    Ok(())
}

/// Before a restore into `datadir`: if an earlier `db load-state` there did not finish, remove the
/// chain data it created (the `chain_data_dirs` its marker lists, the restored-state floor marker
/// it wrote) and then the marker, so this run starts on the datadir that one started on. Refuses,
/// removing nothing, when one of `chain_data_dirs` is there but not on the marker's list: the
/// stopped run wrote into data that was there before it, and only the operator can say what it is.
/// Returns whether there was an unfinished restore.
pub(crate) fn clear_interrupted_load_state(
    datadir: &Path,
    chain_data_dirs: &[PathBuf],
) -> eyre::Result<bool> {
    let path = marker_path(datadir);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(eyre!("failed to read {}: {e}", path.display())),
    };
    let mut created = Vec::new();
    for name in contents.lines() {
        let dir = chain_data_dirs
            .iter()
            .find(|dir| dir.file_name().is_some_and(|dir_name| dir_name == name))
            .ok_or_else(|| {
                eyre!(
                    "{} names {name:?}, which is not a chain-data directory of a restore: remove \
                     the unfinished restore's chain data and {} by hand",
                    path.display(),
                    path.display(),
                )
            })?;
        created.push(dir);
    }
    let mut not_created = Vec::new();
    for dir in chain_data_dirs {
        if !created.contains(&dir) && exists(dir)? {
            not_created.push(dir.display().to_string());
        }
    }
    if !not_created.is_empty() {
        bail!(
            "a `db load-state` into {} did not finish, and it wrote into {}, which were there \
             before it started: remove them by hand (node keys and config stay), and {}, then run \
             `db load-state` again",
            datadir.display(),
            not_created.join(", "),
            path.display(),
        );
    }
    for dir in created {
        remove_entry(dir)?;
    }
    remove_entry(&tn_reth::RethEnv::restored_state_floor_marker(datadir))?;
    // the marker goes last: a stop before here leaves it, and the next run clears again
    remove(datadir)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chain-data directories of a restore into `datadir`.
    fn chain_data_dirs(datadir: &Path) -> [PathBuf; 3] {
        [datadir.join("db"), datadir.join("static_files"), datadir.join("consensus-db")]
    }

    /// A datadir a restore stopped part-way through: keys and config, the three chain-data
    /// directories the run created, the floor marker and the run's marker.
    fn interrupted() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("node-keys")).unwrap();
        fs::write(root.join("node-keys/bls.key"), b"secret").unwrap();
        fs::write(root.join("node-info.yaml"), b"identity").unwrap();
        for dir in chain_data_dirs(root) {
            fs::create_dir_all(dir.join("inner")).unwrap();
        }
        write(root, &chain_data_dirs(root)).unwrap();
        fs::write(tn_reth::RethEnv::restored_state_floor_marker(root), b"42\n").unwrap();
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

    /// The marker refuses a node's start until a rerun clears the stopped run's chain data, which
    /// leaves keys and config and lets the node start.
    #[test]
    fn an_interrupted_restore_blocks_the_node_until_a_rerun_clears_it() {
        let dir = interrupted();
        let root = dir.path();
        assert_eq!(
            fs::read_to_string(marker_path(root)).unwrap(),
            "db\nstatic_files\nconsensus-db\n"
        );
        let err = refuse_interrupted_load_state(root).unwrap_err().to_string();
        assert!(err.contains("did not finish"), "{err}");
        assert!(err.contains("db load-state` again"), "{err}");

        assert!(clear_interrupted_load_state(root, &chain_data_dirs(root)).unwrap());
        assert_eq!(names(root), ["node-info.yaml", "node-keys"]);
        refuse_interrupted_load_state(root).unwrap();
        assert!(
            !clear_interrupted_load_state(root, &chain_data_dirs(root)).unwrap(),
            "nothing left"
        );
    }

    /// A chain-data directory that was there before the stopped run (not on its list) is refused
    /// by the rerun, and nothing is removed.
    #[test]
    fn a_rerun_refuses_chain_data_the_stopped_run_did_not_create() {
        let dir = interrupted();
        let root = dir.path();
        let [db, static_files, consensus] = chain_data_dirs(root);
        write(root, &[static_files, consensus]).unwrap();
        let err = clear_interrupted_load_state(root, &chain_data_dirs(root)).unwrap_err();
        let err = err.to_string();
        assert!(err.contains(&db.display().to_string()), "{err}");
        assert!(err.contains("by hand"), "{err}");
        assert_eq!(
            names(root),
            [
                "consensus-db",
                "db",
                "load-state-in-progress",
                "node-info.yaml",
                "node-keys",
                "restored-state-floor",
                "static_files"
            ],
            "nothing removed"
        );
    }

    /// A marker naming something other than a chain-data directory is refused, and nothing is
    /// removed.
    #[test]
    fn a_rerun_refuses_a_marker_it_cannot_read() {
        let dir = interrupted();
        let root = dir.path();
        fs::write(marker_path(root), b"node-keys\n").unwrap();
        let err = clear_interrupted_load_state(root, &chain_data_dirs(root)).unwrap_err();
        assert!(err.to_string().contains("not a chain-data directory"), "{err}");
        assert!(root.join("node-keys/bls.key").exists());
        assert!(root.join("db").exists());
    }
}
