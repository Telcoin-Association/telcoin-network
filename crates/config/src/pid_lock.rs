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
//! - The real mutual exclusion is an **advisory file lock** (`flock(LOCK_EX)` on unix) taken on an
//!   open handle that the guard holds for its whole lifetime. Acquisition is atomic — two racing
//!   starts cannot both win — and the kernel releases the lock automatically when the holding
//!   process exits or crashes, so a crash never leaves a lock that blocks restart. This closes the
//!   read-then-write TOCTOU window a plain PID file would have.
//! - The file *contents* (our PID) are **advisory only**: they exist so an operator (and our own
//!   error message) can see which process holds the directory. Whoever wins the lock overwrites any
//!   stale content.
//! - The returned guard removes the file on drop — but only if it still holds *our* PID, so a lock
//!   that another process has legitimately reclaimed is never deleted out from under it. (The
//!   advisory lock itself is released regardless, when the guard's handle is dropped.)
//!
//! The guard is advisory. Separate PID namespaces sharing a volume (e.g. two containers on the same
//! mount) still coordinate correctly through `flock` on the shared file, but network filesystems
//! with unreliable `flock` semantics are out of scope, as with any file lock.

use crate::TelcoinDirs;
use eyre::{bail, eyre};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    path::PathBuf,
};
use tracing::warn;

/// A held datadir lock. Dropping the guard releases the advisory lock and removes the file if we
/// still own it.
#[derive(Debug)]
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct PidLock {
    path: PathBuf,
    pid: u32,
    /// The locked handle, held for the guard's lifetime. Dropping it closes the fd, which releases
    /// the advisory `flock`. Never read directly — it exists purely to keep the lock held (RAII).
    _file: File,
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
            bail!(
                "another telcoin process{who} is using this data directory; stop it first, or \
                 delete {} if you are certain it is stale",
                path.display()
            );
        }

        // We hold the lock. Record our PID (advisory) — overwriting any stale content the previous
        // holder left behind.
        write_pid(&mut file, me)
            .map_err(|e| eyre!("failed to write PID lockfile {}: {e}", path.display()))?;

        Ok(Self { path, pid: me, _file: file })
    }

    /// The lockfile path this guard owns.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for PidLock {
    fn drop(&mut self) {
        // The advisory lock is released when `_file` is dropped (fd close) regardless of the below.
        // Best-effort file cleanup: only remove it if it still holds our PID. If another process
        // reclaimed the lock after we released it, its PID is in the file now and must be left
        // alone.
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

/// Truncate `file` and write `pid` as its sole contents (advisory, for operator inspection).
fn write_pid(file: &mut File, pid: u32) -> std::io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(pid.to_string().as_bytes())?;
    file.flush()
}

/// Try to take an exclusive advisory lock on `file` without blocking.
///
/// Returns `Ok(true)` if the lock is now held by this handle, `Ok(false)` if another handle holds
/// it, or `Err` on an unexpected OS error.
#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
    use std::os::unix::io::AsRawFd;
    // LOCK_EX | LOCK_NB: take the exclusive lock or fail immediately if another open file
    // description holds it. The lock is tied to this fd and released on close/exit.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    // EWOULDBLOCK (== EAGAIN) means the lock is held by someone else — not an error, just "no".
    match err.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(false),
        _ => Err(err),
    }
}

/// Non-unix fallback: no advisory-lock primitive is wired up (Telcoin nodes run on unix). Keep the
/// crate buildable elsewhere by conservatively treating a non-empty pre-existing file as a live
/// holder, so we never allow a second concurrent writer.
#[cfg(not(unix))]
fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
    Ok(file.metadata()?.len() == 0)
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
        // Dropping the guard releases the advisory lock and removes the file...
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

    #[test]
    fn drop_does_not_remove_a_reclaimed_lock() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("telcoin.pid");
        let lock = PidLock::acquire_at(path.clone()).unwrap();
        // Simulate another owner reclaiming the file after we (conceptually) exited. `flock` is
        // advisory, so a plain write still succeeds; on drop we must see the foreign PID and leave
        // the file alone.
        fs::write(&path, "1").unwrap();
        drop(lock);
        assert!(path.exists(), "drop must not delete a lock reclaimed by another owner");
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "1");
    }
}
