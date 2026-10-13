// SPDX-License-Identifier: MIT or Apache-2.0

//! The database directory's files and the helpers that manage them (see the README's Layout):
//! `<root>/LOCK` (held by the one open handle), `<root>/tables` (the manifest), the generations
//! `<root>/gen-<N>/` (the newest is current), and a compaction's `<root>/compact-<N>-<pid>-<n>/`
//! while it is built (never a generation; deleted at open if left behind).

use std::{
    fs, io,
    os::fd::AsRawFd as _,
    path::{Path, PathBuf},
};

use eyre::{bail, WrapErr as _};

use crate::archive::data_file::fsync_file;

/// `fsync(2)` an open file or directory: its contents, and for a directory its entries. A plain
/// `fsync` on every platform, never macOS's `F_FULLFSYNC` (see [`fsync_file`]).
pub(super) fn sync_file(file: &fs::File) -> io::Result<()> {
    fsync_file(file)
}

/// [`sync_file`] a directory, making its entries (created, renamed or removed) durable.
pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
    sync_file(&fs::File::open(dir)?)
}

/// Lock the database directory `root` for one handle: an exclusive, non-blocking `flock` on
/// `<root>/LOCK`, held until the returned file is dropped (a crash releases it with the process).
/// A second open, from this process or another, fails instead of becoming a second writer on the
/// same log.
pub(super) fn lock_dir(root: &Path) -> eyre::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("LOCK"))
        .wrap_err("tndb: open the database lock file")?;
    // SAFETY: `flock` on a valid, open file descriptor borrowed for the call.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock {
            bail!("tndb: database {} is already open (its LOCK is held)", root.display());
        }
        return Err(e).wrap_err("tndb: lock the database");
    }
    Ok(file)
}

/// Bytes available to this process on the filesystem holding `dir` (`statvfs`: free blocks
/// available to unprivileged users times the fragment size).
pub(super) fn available_bytes(dir: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt as _;

    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(io::Error::other)?;
    // SAFETY: an all-zero `statvfs` is a valid value of the plain C struct the call fills in.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is NUL-terminated and outlives the call; `stat` is a valid out-parameter.
    if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // The field types differ by platform (`u32` block counts on macOS, `u64` on Linux).
    #[allow(clippy::unnecessary_cast)]
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

/// Delete a retired generation's directory on a short-lived thread named `thread` (its files are
/// unlinked now; their space returns once no mapping of them is left), or here if no thread can be
/// started. A failure leaves it for the next open to remove.
pub(super) fn remove_in_background(dir: PathBuf, thread: &str) {
    let remove = |dir: &Path| {
        if let Err(e) = fs::remove_dir_all(dir) {
            tracing::warn!(target: "tndb", "remove retired generation {}: {e}", dir.display());
        }
    };
    let thread_dir = dir.clone();
    let spawned =
        std::thread::Builder::new().name(thread.to_string()).spawn(move || remove(&thread_dir));
    if spawned.is_err() {
        remove(&dir);
    }
}

/// Drop `value` (a retired generation's files: closing them unmaps them, and may delete them) on
/// a short-lived thread named `thread`, off the path of whoever let go of it last; if no thread
/// can be started, here.
pub(super) fn drop_in_background<T: Send + 'static>(value: T, thread: &str) {
    let _ = std::thread::Builder::new().name(thread.to_string()).spawn(move || drop(value));
}

/// The directory of generation `n`.
pub(super) fn gen_dir(root: &Path, n: u64) -> PathBuf {
    root.join(format!("gen-{n}"))
}

/// A fresh directory to build generation `n` in (a compaction's, or a new database's first).
/// Unique (by process and a per-process counter), so no two builds ever share files.
pub(super) fn compact_dir(root: &Path, n: u64) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    root.join(format!("compact-{n}-{}-{id}", std::process::id()))
}

/// Delete every unfinished build in `root` (left by a close or a crash). Best-effort: a build is
/// never read, so one that cannot be deleted now (logged) is only garbage for a later open.
pub(super) fn remove_unfinished(root: &Path) -> eyre::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(|name| name.starts_with("compact-")) {
            if let Err(e) = fs::remove_dir_all(entry.path()) {
                tracing::warn!(target: "tndb", "remove {}: {e}", entry.path().display());
            }
        }
    }
    Ok(())
}

/// The generation numbers present in `root`, ascending.
pub(super) fn list_gens(root: &Path) -> eyre::Result<Vec<u64>> {
    let mut gens = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(n) =
            name.to_str().and_then(|n| n.strip_prefix("gen-")).and_then(|n| n.parse().ok())
        {
            gens.push(n);
        }
    }
    gens.sort_unstable();
    Ok(gens)
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_tndb_list_gens_sorted() {
        let tmp = TempDir::with_prefix("tndb_gens").expect("temp dir");
        for n in [10_u64, 2, 0] {
            fs::create_dir(gen_dir(tmp.path(), n)).expect("mkdir");
        }
        fs::create_dir(tmp.path().join("gen-x")).expect("mkdir");
        fs::create_dir(compact_dir(tmp.path(), 11)).expect("mkdir");
        assert_eq!(list_gens(tmp.path()).expect("list"), vec![0, 2, 10]);
        remove_unfinished(tmp.path()).expect("remove");
        assert_eq!(list_gens(tmp.path()).expect("list"), vec![0, 2, 10], "generations kept");
        assert_eq!(fs::read_dir(tmp.path()).expect("read").count(), 4, "the build is gone");
    }
}
