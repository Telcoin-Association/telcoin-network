//! Thin wrappers over the system calls the page file needs: file and directory syncs, block
//! preallocation, the OS page size and the exclusive open lock.
//!
//! **macOS.** Neither `msync` nor `fsync` is a full power-loss barrier there (that needs
//! `F_FULLFSYNC`, a flush of the whole drive's write cache). Commits are `msync`, so every other
//! sync here is a plain `fsync` too ([`fsync_file`], [`fdatasync_file`]): std's
//! `File::sync_all`/`sync_data` would issue `F_FULLFSYNC`, costing 10–20 ms each and stalling
//! every other write on the drive, for a guarantee the commits never give. A crash of the process
//! or the kernel loses nothing committed durably; a power loss can, as with MDBX's `Durable` mode
//! with `write_map`. On Linux these are exactly `File::sync_all`/`sync_data` (`fsync`/`fdatasync`,
//! which do flush the drive's cache).
//!
//! In tests every successful sync reports to the disk model (`disk_model`).

use std::{fs::File, io, os::fd::AsRawFd, path::Path};

/// Fsync a directory so recent directory-entry changes (file creates, renames) are durable.
///
/// `fsync` on a regular file does not flush the parent directory entry that names it: a crash
/// between a create/rename and a directory fsync can leave the entry missing even though the
/// file's own data is durable. Calling this on the parent directory closes that gap.
///
/// `path` must refer to a directory (a regular file would just have its contents synced).
pub(crate) fn fsync_directory(path: &Path) -> io::Result<()> {
    fsync_file(&File::open(path)?)
}

/// `fsync(2)` `file`: [`File::sync_all`] everywhere but macOS, where that is `F_FULLFSYNC`
/// (see the module docs) and this is a plain `fsync`.
pub(crate) fn fsync_file(file: &File) -> io::Result<()> {
    // Retried when a signal interrupts it, as `File::sync_all` does: every caller fail-stops on a
    // sync error, and an interrupted call is not one.
    #[cfg(target_os = "macos")]
    let synced = loop {
        // SAFETY: `fsync` on a file descriptor `file` owns and keeps open for the call; it touches
        // no Rust memory.
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            break Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            break Err(error);
        }
    };
    #[cfg(not(target_os = "macos"))]
    let synced = file.sync_all();
    #[cfg(test)]
    if synced.is_ok() {
        crate::disk_model::flushed_all(file);
    }
    synced
}

/// `fdatasync(2)` `file`: [`File::sync_data`] everywhere but macOS, where that is `F_FULLFSYNC`
/// and this is a plain `fsync` (see [`fsync_file`]).
pub(crate) fn fdatasync_file(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        fsync_file(file)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let synced = file.sync_data();
        #[cfg(test)]
        if synced.is_ok() {
            crate::disk_model::flushed_all(file);
        }
        synced
    }
}

/// Reserve real disk blocks for `[from, to)` of `file` and grow it to `to` bytes, so a later
/// store through a mapping into that range can never SIGBUS on a full filesystem: running out of
/// space surfaces here as an `io::Error` instead. A bare `set_len`/ftruncate only moves EOF and
/// leaves the new bytes as sparse holes whose first write fault allocates a block and, when the
/// disk is full, delivers `SIGBUS` with no Rust error path.
///
/// `from` must be the file's current length (macOS reserves from the physical end of file). Where
/// the platform or filesystem cannot preallocate, this falls back to a plain (sparse) resize so the
/// file still works.
#[cfg(target_os = "linux")]
pub(crate) fn allocate_range(file: &File, from: u64, to: u64) -> io::Result<()> {
    let len = to.saturating_sub(from);
    if len == 0 {
        return Ok(());
    }
    // fallocate(2) mode 0: allocate blocks for [from, from+len) and extend the size to cover it.
    // SAFETY: a plain syscall on a file descriptor the caller owns and keeps open for the call; it
    // touches no Rust memory, and the offsets are range-checked by the kernel.
    let rc =
        unsafe { libc::fallocate(file.as_raw_fd(), 0, from as libc::off_t, len as libc::off_t) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    // Some filesystems (tmpfs, some network/older FS) don't implement fallocate; fall back to a
    // plain resize (sparse) rather than failing an otherwise-serviceable growth.
    if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)) {
        return file.set_len(to);
    }
    Err(err) // ENOSPC / EDQUOT / EFBIG / EIO propagate as the growth's io::Error
}

/// See the Linux variant.
#[cfg(target_os = "macos")]
pub(crate) fn allocate_range(file: &File, from: u64, to: u64) -> io::Result<()> {
    let len = to.saturating_sub(from);
    if len == 0 {
        return Ok(());
    }
    // F_PREALLOCATE reserves blocks from the physical EOF (== `from`) but does NOT change the file
    // size, so a `set_len` still follows to grow the logical size. Try a contiguous reservation
    // first, then allow a fragmented one. Both requests carry `F_ALLOCATEALL` (all or nothing):
    // without it the call may succeed having reserved only part of the range, and `set_len` would
    // then extend over unreserved blocks — the SIGBUS this preallocation exists to prevent.
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATECONTIG | libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len as libc::off_t,
        fst_bytesalloc: 0,
    };
    let fd = file.as_raw_fd();
    // SAFETY: `fd` belongs to a file the caller owns and keeps open for the call, and `store` is a
    // live, properly initialized `fstore_t` that `F_PREALLOCATE` reads and updates
    // (`fst_bytesalloc`) for the duration of the call only.
    let mut rc = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) };
    if rc == -1 {
        store.fst_flags = libc::F_ALLOCATEALL;
        // SAFETY: as above.
        rc = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) };
    }
    if rc == -1 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOTSUP) {
            return file.set_len(to);
        }
        return Err(err);
    }
    // Belt and braces: never extend past what was actually reserved.
    if (store.fst_bytesalloc as u64) < len {
        return Err(io::Error::from_raw_os_error(libc::ENOSPC));
    }
    file.set_len(to)
}

/// See the Linux variant: no portable preallocation on this target, so a plain (sparse) resize.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn allocate_range(file: &File, _from: u64, to: u64) -> io::Result<()> {
    file.set_len(to)
}

/// The OS page size (cached): the unit `msync` addresses and the kernel writes back.
pub(crate) fn os_page_size() -> u64 {
    static PAGE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    // SAFETY: `sysconf` reads a system constant.
    *PAGE
        .get_or_init(|| u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(4096))
}

/// Take an exclusive, non-blocking `flock` on `file`, held until every descriptor of its open file
/// description is closed (a crash releases it with the process). `Ok(false)` when another open of
/// the file (from this process or another) already holds it.
pub(crate) fn flock_exclusive(file: &File) -> io::Result<bool> {
    // SAFETY: `flock` on a valid, open file descriptor borrowed for the call.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let e = io::Error::last_os_error();
    if e.kind() == io::ErrorKind::WouldBlock {
        return Ok(false);
    }
    Err(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt as _;

    /// A second open of the same file, even in this process, cannot take the lock while the first
    /// holds it, and can once the first is closed.
    #[test]
    fn flock_exclusive_excludes_a_second_open() {
        let tmp = tempfile::TempDir::with_prefix("tndb_b_flock").expect("temp dir");
        let path = tmp.path().join("db");
        let first = File::create(&path).expect("create");
        assert!(flock_exclusive(&first).expect("lock"), "the first open takes the lock");
        let second = File::open(&path).expect("open again");
        assert!(!flock_exclusive(&second).expect("try lock"), "a second open must not get it");
        drop(first);
        assert!(flock_exclusive(&second).expect("lock"), "free once the first open closes");
    }

    /// `allocate_range` reserves real blocks for the whole range, not a sparse hole.
    #[test]
    fn allocate_range_reserves_real_blocks() {
        let tmp = tempfile::TempDir::with_prefix("tndb_b_alloc").expect("temp dir");
        let path = tmp.path().join("db");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create");
        allocate_range(&file, 0, 1 << 20).expect("allocate");
        let meta = file.metadata().expect("metadata");
        assert_eq!(meta.len(), 1 << 20, "the file is grown to the end of the range");
        assert!(meta.blocks() * 512 >= 1 << 20, "real blocks back the range ({})", meta.blocks());
        allocate_range(&file, 1 << 20, 1 << 20).expect("an empty range is a no-op");
        assert_eq!(file.metadata().expect("metadata").len(), 1 << 20);
    }

    #[test]
    fn os_page_size_is_a_power_of_two() {
        let page = os_page_size();
        assert!(page >= 4096 && page.is_power_of_two(), "page size {page}");
    }
}
