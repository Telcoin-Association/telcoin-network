// SPDX-License-Identifier: MIT or Apache-2.0
//! A cooperative single-writer lockfile for a node's data directory.
//!
//! A telcoin node is the sole writer of its datadir. Starting a second node — or running
//! `db repair` — against the same directory while a node is live would rewrite the memory-mapped
//! pack files under the running node and corrupt them. This module provides a small PID lockfile at
//! [`TelcoinDirs::node_pid_path`] (`<datadir>/telcoin.pid`) that makes that collision detectable
//! and refuses it, without reaching into any storage-engine-internal lock.
//!
//! Semantics:
//! - [`PidLock::acquire`] writes the current process's PID to the file. If the file already holds a
//!   *live* PID (a different, running process) it errors. A stale PID (the previous writer
//!   crashed), an unparseable file, or a missing file is reclaimed and overwritten with our PID.
//! - The returned guard removes the file on drop — but only if it still holds *our* PID, so a lock
//!   that another process has legitimately reclaimed is never deleted out from under it.
//!
//! The guard is advisory. Two false-negative edges are accepted, as with any PID lockfile: PID
//! reuse (a stale PID reassigned to an unrelated live process reads as "running"), and separate PID
//! namespaces sharing a volume (e.g. two containers on the same mount).

use crate::TelcoinDirs;
use eyre::{bail, eyre};
use std::{fs, path::PathBuf};
use tracing::warn;

/// A held datadir PID lock. Dropping the guard releases the lock (removes the file) if we still own
/// it.
#[derive(Debug)]
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct PidLock {
    path: PathBuf,
    pid: u32,
}

impl PidLock {
    /// Acquire the datadir lock for the current process.
    ///
    /// Errors if the lockfile holds the PID of a different, still-running process. A stale or
    /// unparseable lockfile, or no lockfile at all, is reclaimed and overwritten with our PID.
    pub fn acquire(dirs: &impl TelcoinDirs) -> eyre::Result<Self> {
        Self::acquire_at(dirs.node_pid_path())
    }

    fn acquire_at(path: PathBuf) -> eyre::Result<Self> {
        let me = std::process::id();
        if let Ok(contents) = fs::read_to_string(&path) {
            match contents.trim().parse::<u32>() {
                Ok(pid) if pid != me && process_is_alive(pid) => {
                    bail!(
                        "another telcoin process (pid {pid}) is using this data directory; stop it \
                         first, or delete {} if you are certain it is stale",
                        path.display()
                    );
                }
                // Our own PID (a re-acquire) or a dead PID: reclaim it.
                Ok(_) => {}
                // Garbage in the file: a live node always writes a clean integer, so this is a
                // stale leftover — reclaim it rather than bricking startup on a corrupt lockfile.
                Err(_) => {
                    warn!(
                        target: "tn::pid_lock",
                        path = %path.display(),
                        "ignoring unparseable PID lockfile (treating as stale)"
                    );
                }
            }
        }
        fs::write(&path, me.to_string())
            .map_err(|e| eyre!("failed to write PID lockfile {}: {e}", path.display()))?;
        Ok(Self { path, pid: me })
    }

    /// The lockfile path this guard owns.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for PidLock {
    fn drop(&mut self) {
        // Only remove the file if it still holds our PID. If another process reclaimed the lock
        // (e.g. after we were assumed stale), its PID is in the file now and must be left alone.
        let still_ours = fs::read_to_string(&self.path)
            .ok()
            .and_then(|c| c.trim().parse::<u32>().ok())
            .is_some_and(|p| p == self.pid);
        if still_ours {
            if let Err(e) = fs::remove_file(&self.path) {
                warn!(
                    target: "tn::pid_lock",
                    path = %self.path.display(),
                    "failed to remove PID lockfile on shutdown: {e}"
                );
            }
        }
    }
}

/// Whether `pid` is a currently-running process.
#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    // `kill(pid, 0)` sends no signal but runs the existence/permission checks:
    //   0     -> the process exists and we may signal it        => alive
    //   EPERM -> the process exists but is owned by another user => alive
    //   ESRCH -> no such process                                 => dead
    match unsafe { libc::kill(pid as libc::pid_t, 0) } {
        0 => true,
        _ => std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM),
    }
}

/// Non-unix fallback: conservatively assume any recorded PID is alive so we never allow a second
/// concurrent writer. Telcoin nodes run on unix; this only keeps the crate buildable elsewhere.
#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn acquire_writes_our_pid_and_drop_removes_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        {
            let lock = PidLock::acquire_at(path.clone()).unwrap();
            assert_eq!(lock.path(), path);
            let recorded = fs::read_to_string(&path).unwrap();
            assert_eq!(recorded.trim().parse::<u32>().unwrap(), std::process::id());
        }
        assert!(!path.exists(), "drop must remove the lockfile we own");
    }

    #[test]
    fn reacquire_by_same_process_succeeds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        let _a = PidLock::acquire_at(path.clone()).unwrap();
        // Our own PID is in the file; acquiring again reclaims it rather than erroring.
        let _b = PidLock::acquire_at(path.clone()).unwrap();
    }

    #[test]
    fn unparseable_file_is_reclaimed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        fs::write(&path, "not-a-pid").unwrap();
        let _lock = PidLock::acquire_at(path.clone()).unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap().trim().parse::<u32>().unwrap(),
            std::process::id()
        );
    }

    #[test]
    fn drop_does_not_remove_a_reclaimed_lock() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        let lock = PidLock::acquire_at(path.clone()).unwrap();
        // Simulate another owner reclaiming the file after we assumed the lock.
        fs::write(&path, "1").unwrap();
        drop(lock);
        assert!(path.exists(), "drop must not delete a lock reclaimed by another owner");
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "1");
    }

    #[cfg(unix)]
    #[test]
    fn process_liveness_matches_reality() {
        // We are alive; pid 1 (init/launchd) is always alive and owned by root.
        assert!(process_is_alive(std::process::id()));
        assert!(process_is_alive(1));
        // A reaped child is dead.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        assert!(!process_is_alive(pid));
    }

    #[cfg(unix)]
    #[test]
    fn live_foreign_pid_is_refused_and_file_untouched() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        // pid 1 is alive and is not us.
        fs::write(&path, "1").unwrap();
        let err = PidLock::acquire_at(path.clone()).unwrap_err();
        assert!(err.to_string().contains("another telcoin process"), "got: {err}");
        // A refused acquire must not modify or remove the existing lock.
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "1");
    }

    #[cfg(unix)]
    #[test]
    fn stale_pid_is_reclaimed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        // Use a child's PID after it exits: dead (barring immediate reuse).
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();
        fs::write(&path, dead_pid.to_string()).unwrap();
        let _lock = PidLock::acquire_at(path.clone()).unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap().trim().parse::<u32>().unwrap(),
            std::process::id()
        );
    }
}
