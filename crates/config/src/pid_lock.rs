// SPDX-License-Identifier: MIT or Apache-2.0
//! A cooperative single-writer lock for a node's data directory.
//!
//! A telcoin node is the sole writer of its datadir. Starting a second node — or running
//! `db repair` — against the same directory while a node is live would rewrite the memory-mapped
//! pack files under the running node and corrupt them. This module provides a small lock at
//! [`TelcoinDirs::node_pid_path`] (`<datadir>/telcoin.pid`) that makes that collision detectable
//! and refuses it, without reaching into any storage-engine-internal lock.
//!
//! Mechanism:
//! - The real mutual exclusion is an **exclusive OS file lock** ([`File::try_lock`]: `flock` on
//!   unix, `LockFileEx` on Windows) taken on an open handle that the guard holds for its whole
//!   lifetime. Acquisition is atomic — two racing starts cannot both win — and the kernel releases
//!   the lock automatically when the holding process exits or crashes, so a crash never leaves a
//!   lock that blocks restart. This closes the read-then-write TOCTOU window a plain PID file would
//!   have.
//! - The file *contents* (our PID) are **advisory only**: they exist so an operator (and our own
//!   error message) can see which process holds the directory. Whoever wins the lock overwrites any
//!   stale content.
//! - The file itself is never deleted. Dropping the guard clears the PID it recorded (through the
//!   still-locked handle) and closes the handle, which releases the lock. Deleting the file would
//!   break the exclusion: a process that opened the old file just before the unlink could lock that
//!   orphaned inode after we release it, while the next starter creates and locks a new file at the
//!   same path — two holders. An empty file is simply "no holder recorded".
//!
//! The guard is advisory. Separate PID namespaces sharing a volume (e.g. two containers on the same
//! mount) still coordinate correctly through `flock` on the shared file, but network filesystems
//! with unreliable `flock` semantics are out of scope, as with any file lock.

use crate::TelcoinDirs;
use eyre::{bail, eyre};
use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    path::PathBuf,
};
use tracing::warn;

/// A held datadir lock. Dropping the guard clears the recorded PID and releases the advisory lock;
/// the file stays in place.
#[derive(Debug)]
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct PidLock {
    path: PathBuf,
    /// The locked handle, held for the guard's lifetime. Dropping it closes the handle, which
    /// releases the lock.
    file: File,
}

impl PidLock {
    /// Acquire the datadir lock for the current process.
    ///
    /// Errors if another process currently holds the advisory lock. A stale file (the previous
    /// holder crashed or exited — its lock is already released by the kernel), an unparseable file,
    /// or no file at all is reclaimed and overwritten with our PID.
    pub fn acquire(dirs: &impl TelcoinDirs) -> eyre::Result<Self> {
        Self::acquire_at(dirs.node_pid_path())
    }

    fn acquire_at(path: PathBuf) -> eyre::Result<Self> {
        let me = std::process::id();
        // Open (creating if absent) WITHOUT truncating: if the lock is held we must be able to read
        // the current holder's PID for the error message before we know whether we may take it.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| eyre!("failed to open PID lockfile {}: {e}", path.display()))?;

        // The real mutual exclusion. Non-blocking, so a live holder is reported rather than waited
        // on. This is atomic: exactly one open handle can hold the exclusive lock at a time.
        if !try_lock_exclusive(&file)
            .map_err(|e| eyre!("failed to lock PID lockfile {}: {e}", path.display()))?
        {
            let mut buf = String::new();
            let _ = file.read_to_string(&mut buf);
            let holder = buf.trim();
            let who = if holder.is_empty() { String::new() } else { format!(" (pid {holder})") };
            // Never suggest deleting the lockfile: a refused lock means a live holder (the kernel
            // releases a dead one), and a deleted lockfile lets two processes lock two files at
            // the same path.
            bail!(
                "another telcoin process{who} holds the lock on this data directory ({}); stop \
                 it first",
                path.display()
            );
        }

        // We hold the lock. Record our PID (advisory) — overwriting any stale content the previous
        // holder left behind.
        write_pid(&mut file, me)
            .map_err(|e| eyre!("failed to write PID lockfile {}: {e}", path.display()))?;

        Ok(Self { path, file })
    }

    /// The lockfile path this guard owns.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for PidLock {
    fn drop(&mut self) {
        // Clear our PID through the handle we still hold locked (nobody else can hold the lock, so
        // this cannot clobber another holder's record); the lock itself is released when `file` is
        // dropped (fd close) right after. Never unlink the file — see the module docs.
        if let Err(e) = self.file.set_len(0) {
            warn!(
                target: "tn::pid_lock",
                path = %self.path.display(),
                "failed to clear the PID lockfile on shutdown: {e}"
            );
        }
    }
}

/// Truncate `file` and write `pid` as its sole contents (advisory, for operator inspection).
fn write_pid(file: &mut File, pid: u32) -> std::io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(pid.to_string().as_bytes())?;
    file.flush()
}

/// Try to take an exclusive lock on `file` without blocking.
///
/// Returns `Ok(true)` if the lock is now held by this handle, `Ok(false)` if another handle holds
/// it, or `Err` on an unexpected OS error. The lock is tied to this open handle (on unix, its open
/// file description) and released when the handle closes or the process exits.
fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        // Held by someone else: not an error, just "no".
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(err)) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn acquire_writes_our_pid_and_drop_clears_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        {
            let lock = PidLock::acquire_at(path.clone()).unwrap();
            assert_eq!(lock.path(), path);
            let recorded = fs::read_to_string(&path).unwrap();
            assert_eq!(recorded.trim().parse::<u32>().unwrap(), std::process::id());
        }
        assert!(path.exists(), "drop must never unlink the lockfile");
        assert!(fs::read_to_string(&path).unwrap().is_empty(), "drop must clear the recorded PID");
    }

    #[test]
    fn a_held_lock_blocks_a_second_acquire() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        // The advisory lock is the real mutual exclusion: while one guard holds it, a second
        // acquire (from any process — here the same process via a fresh handle) is refused, and the
        // refused acquire must not disturb the held lock's recorded PID.
        let _held = PidLock::acquire_at(path.clone()).unwrap();
        let err = PidLock::acquire_at(path.clone()).unwrap_err();
        assert!(err.to_string().contains("another telcoin process"), "got: {err}");
        assert_eq!(
            fs::read_to_string(&path).unwrap().trim().parse::<u32>().unwrap(),
            std::process::id(),
            "a refused acquire must leave the held lock untouched"
        );
    }

    #[test]
    fn released_lock_is_reclaimable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        // Dropping the guard releases the advisory lock...
        drop(PidLock::acquire_at(path.clone()).unwrap());
        // ...so a fresh acquire succeeds.
        let _lock = PidLock::acquire_at(path.clone()).unwrap();
    }

    #[test]
    fn stale_file_without_a_held_lock_is_reclaimed() {
        // A leftover file from a crashed node (content present, but no process holds the advisory
        // lock — the kernel released it on exit) must not block startup: we take the lock and
        // overwrite the stale content.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        fs::write(&path, "424242").unwrap();
        let _lock = PidLock::acquire_at(path.clone()).unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap().trim().parse::<u32>().unwrap(),
            std::process::id()
        );
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

    /// The race an unlink-on-drop would open: a starter opens the lockfile while the holder is
    /// shutting down and locks it once the holder releases. Because the file is never unlinked,
    /// that starter holds the lock on THE lockfile, so a third acquire is refused rather than
    /// creating and locking a fresh file at the same path (two holders).
    #[cfg(unix)]
    #[test]
    fn a_racing_opener_holds_the_one_lockfile() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        let holder = PidLock::acquire_at(path.clone()).unwrap();
        // The racer opened the path before the holder released it.
        let racer = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        assert!(!try_lock_exclusive(&racer).unwrap(), "the holder still has the lock");
        drop(holder);
        assert!(try_lock_exclusive(&racer).unwrap(), "the racer takes the released lock");
        let err = PidLock::acquire_at(path.clone()).unwrap_err();
        assert!(err.to_string().contains("another telcoin process"), "got: {err}");
    }
}
