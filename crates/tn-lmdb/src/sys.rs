//! Thin wrappers over the system calls the page file needs: file and directory syncs, block
//! preallocation, the OS page size, the open locks (exclusive for a writer, shared for a
//! read-only open) and the boot id ([`boot_id`]).
//!
//! **macOS.** Neither `msync` nor `fsync` is a full power-loss barrier there (that needs
//! `F_FULLFSYNC`, a flush of the whole drive's write cache). Commits are `msync`, so every other
//! sync here is a plain `fsync` too ([`fsync_file`], [`fdatasync_file`]): std's
//! `File::sync_all`/`sync_data` would issue `F_FULLFSYNC`, costing 10–20 ms each and stalling
//! every other write on the drive, for a guarantee the commits never give. A plain `fsync` there
//! also leaves some pages dirtied through a shared mapping unwritten (only `msync` writes them),
//! so on macOS the page file never relies on an `fsync` for such pages: it syncs them by `msync`
//! (a commit's data, `fdatasync_all`, the writer open: `file.rs`, Durability), and the syncs here
//! keep what an `fsync` does write: the file's size, bytes written by `write` (a create's metas)
//! and the directory. With that, a crash of the process or the kernel loses nothing committed
//! durably; a power loss can lose what the drive's write cache held. On Linux these are exactly
//! `File::sync_all`/`sync_data` (`fsync`/`fdatasync`, which do flush the drive's cache and write
//! every dirty page of the file).
//!
//! In tests every successful sync reports to the disk model (`disk_model`).

use std::{fs::File, io, os::fd::AsRawFd, path::Path, sync::OnceLock};

/// A boot id: 16 bytes that differ from one boot of the host to the next ([`boot_id`]). All zeros
/// stands for an unknown one.
pub(crate) type BootId = [u8; 16];

/// The id of the host's running boot, read once per process: a weak meta carries it, and open
/// trusts a weak meta only in the boot that wrote it (`meta::choose_meta`). Linux reads
/// `/proc/sys/kernel/random/boot_id`, macOS `sysctlbyname("kern.bootsessionuuid")`, each a UUID
/// string. `None` when it cannot be read or parsed, or is the nil UUID: a weak meta then carries
/// zeros, which no open trusts, so such a host recovers every crash at the last sync.
///
/// In tests the thread's machine model answers first (`disk_model::boot_id`): the host's id until
/// a power loss on the thread reboots it.
pub(crate) fn boot_id() -> Option<BootId> {
    #[cfg(test)]
    if let Some(id) = crate::disk_model::boot_id() {
        return id;
    }
    static ID: OnceLock<Option<BootId>> = OnceLock::new();
    *ID.get_or_init(|| read_boot_id().as_deref().and_then(parse_uuid))
}

/// The host's boot id as the kernel prints it (see [`boot_id`]).
#[cfg(target_os = "linux")]
fn read_boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()
}

/// The host's boot id as the kernel prints it (see [`boot_id`]).
#[cfg(target_os = "macos")]
fn read_boot_id() -> Option<String> {
    let mut buf = [0_u8; 64];
    let mut len = buf.len();
    // SAFETY: the name is a NUL-terminated string; `buf` is a live stack buffer of `len` bytes,
    // the most the kernel writes, and `len` a live `size_t` it reads and then sets to the length
    // written; no new value is set (null, 0). No Rust memory past the call is touched.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    let text = buf.get(..len).filter(|_| rc == 0)?;
    let text = text.split(|&b| b == 0).next()?;
    String::from_utf8(text.to_vec()).ok()
}

/// A UUID's 16 bytes from its text (`8-4-4-4-12` hex digits, white space around them allowed);
/// `None` for any other text, and for the nil UUID, which names no boot.
fn parse_uuid(text: &str) -> Option<BootId> {
    let text = text.trim();
    if !text.split('-').map(str::len).eq([8, 4, 4, 4, 12]) {
        return None;
    }
    let mut digits = text.chars().filter(|&c| c != '-').map(|c| c.to_digit(16));
    let mut id = [0_u8; 16];
    for byte in &mut id {
        let (high, low) = (digits.next()??, digits.next()??);
        *byte = (high << 4 | low) as u8;
    }
    (id != [0; 16]).then_some(id)
}

/// Fsync a directory so recent directory-entry changes (file creates, renames) are durable.
///
/// `fsync` on a regular file does not flush the parent directory entry that names it: a crash
/// between a create/rename and a directory fsync can leave the entry missing even though the
/// file's own data is durable. Calling this on the parent directory closes that gap.
///
/// `path` must refer to a directory (a regular file would just have its contents synced).
///
/// PostgreSQL's rule for a directory that cannot be synced: an `EACCES` from opening it (a
/// directory the process may enter and write but not read) and an `EINVAL`, `ENOTSUP` or
/// `EOPNOTSUPP` from its fsync (a file system with no directory fsync) are skipped with a
/// warning naming the directory, since such a directory never could make the name durable and
/// the file's own sync still runs. Every other error fails, naming the directory, with the
/// original error as its source (its kind and OS error code kept: [`Context`]).
pub(crate) fn fsync_directory(path: &Path) -> io::Result<()> {
    let skipped = |what: &str, e: &io::Error| {
        tracing::warn!(
            target: "tn_lmdb",
            "the directory {} cannot be synced ({what}: {e}); a crash may lose a name made in it",
            path.display()
        );
    };
    let failed = |what: &str, e: io::Error| {
        Context::wrap(format!("syncing the directory {}: {what}", path.display()), e)
    };
    let dir = match File::open(path) {
        Ok(dir) => dir,
        Err(e) if e.raw_os_error() == Some(libc::EACCES) => {
            skipped("it cannot be opened", &e);
            return Ok(());
        }
        Err(e) => return Err(failed("open", e)),
    };
    if let Err(e) = fsync_file(&dir) {
        let unsupported = e.raw_os_error().is_some_and(|code| {
            code == libc::EINVAL || code == libc::ENOTSUP || code == libc::EOPNOTSUPP
        });
        if !unsupported {
            return Err(failed("fsync", e));
        }
        skipped("its file system has no directory fsync", &e);
        return Ok(());
    }
    #[cfg(test)]
    DIR_SYNC_LOG.with(|log| log.borrow_mut().push(path.to_path_buf()));
    Ok(())
}

/// An I/O error with what was being done when it happened (`what: error`). Wrapped in an
/// `io::Error` of the original's kind, it is that error's payload and keeps the original as its
/// source, so a caller that walks the sources (as the reth adapter does for an error's code)
/// still finds the OS error code, which the wrapping `io::Error` itself no longer carries.
#[derive(Debug)]
struct Context {
    /// What was being done.
    what: String,
    /// The error it met.
    source: io::Error,
}

impl Context {
    /// `source`, as an `io::Error` of its kind that says it happened while doing `what`.
    #[cold]
    fn wrap(what: String, source: io::Error) -> io::Error {
        io::Error::new(source.kind(), Context { what, source })
    }
}

impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.what, self.source)
    }
}

impl std::error::Error for Context {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only: the directories [`fsync_directory`] synced on this thread, in order (a
    /// directory it skipped is not one). Read with [`take_dir_sync_log`].
    static DIR_SYNC_LOG: std::cell::RefCell<Vec<std::path::PathBuf>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Test-only: the directories this thread synced since the last call, oldest first (see
/// [`DIR_SYNC_LOG`]).
#[cfg(test)]
pub(crate) fn take_dir_sync_log() -> Vec<std::path::PathBuf> {
    DIR_SYNC_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

/// `fsync(2)` `file`: [`File::sync_all`] everywhere but macOS, where that is `F_FULLFSYNC`
/// (see the module docs) and this is a plain `fsync`.
pub(crate) fn fsync_file(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    let synced = retry_interrupted(|| {
        // SAFETY: `fsync` on a file descriptor `file` owns and keeps open for the call; it touches
        // no Rust memory.
        os_result(unsafe { libc::fsync(file.as_raw_fd()) })
    });
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

/// Reserve real disk blocks for `[from, to)` of `file` and grow it to `to` bytes, so the first
/// store through a mapping into that range cannot SIGBUS on a full filesystem: running out of
/// space surfaces here as an `io::Error` instead. A bare `set_len`/ftruncate only moves EOF and
/// leaves the new bytes as sparse holes whose first write fault allocates a block and, when the
/// disk is full, delivers `SIGBUS` with no Rust error path. A later rewrite is covered only on a
/// filesystem that overwrites in place: a copy-on-write one, or a file sharing blocks with a
/// reflink copy or a snapshot, needs new space at each rewrite (README, Platform notes).
///
/// `from` must be the file's current length (macOS reserves from the physical end of file). Where
/// the filesystem cannot preallocate, this falls back to a plain (sparse) resize so the file
/// still works.
#[cfg(target_os = "linux")]
pub(crate) fn allocate_range(file: &File, from: u64, to: u64) -> io::Result<()> {
    let len = to.saturating_sub(from);
    if len == 0 {
        return Ok(());
    }
    // fallocate(2) mode 0: allocate blocks for [from, from+len) and extend the size to cover it.
    // Reissuing it after an interruption is safe: mode 0 over the same range is idempotent.
    let allocated = retry_interrupted(|| {
        // SAFETY: a plain syscall on a file descriptor the caller owns and keeps open for the
        // call; it touches no Rust memory, and the offsets are range-checked by the kernel.
        os_result(unsafe {
            libc::fallocate(file.as_raw_fd(), 0, from as libc::off_t, len as libc::off_t)
        })
    });
    match allocated {
        Ok(()) => Ok(()),
        // Some filesystems (NFS before 4.2, FUSE filesystems without it) don't implement
        // fallocate; fall back to a plain resize (sparse) rather than failing an
        // otherwise-serviceable growth.
        Err(err) if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)) => {
            file.set_len(to)
        }
        Err(err) => Err(err), // ENOSPC / EDQUOT / EFBIG / EIO propagate as the growth's io::Error
    }
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
    let preallocate = |store: &mut libc::fstore_t| {
        retry_interrupted(|| {
            // SAFETY: `fd` belongs to a file the caller owns and keeps open for the call, and
            // `store` is a live, properly initialized `fstore_t` that `F_PREALLOCATE` reads and
            // updates (`fst_bytesalloc`) for the duration of the call only.
            os_result(unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut *store) })
        })
    };
    let mut reserved = preallocate(&mut store);
    if reserved.is_err() {
        store.fst_flags = libc::F_ALLOCATEALL;
        reserved = preallocate(&mut store);
    }
    if let Err(err) = reserved {
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

/// Run `call` again while a signal interrupts it (`EINTR`), as std does inside `File::sync_all`
/// and `set_len`: every caller fail-stops on the error, and an interrupted call is not one.
fn retry_interrupted(mut call: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    loop {
        match call() {
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            done => return done,
        }
    }
}

/// A libc call's return code as a result: `-1` is the error in `errno`.
fn os_result(rc: libc::c_int) -> io::Result<()> {
    if rc == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
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
/// the file (from this process or another) already holds it, exclusive or shared.
pub(crate) fn flock_exclusive(file: &File) -> io::Result<bool> {
    flock(file, libc::LOCK_EX)
}

/// Take a shared, non-blocking `flock` on `file` (a read-only open's), held as
/// [`flock_exclusive`]'s is. Any number of opens hold it together. `Ok(false)` when another open
/// of the file (from this process or another) holds it exclusive.
pub(crate) fn flock_shared(file: &File) -> io::Result<bool> {
    flock(file, libc::LOCK_SH)
}

/// `flock(file, op | LOCK_NB)`: `Ok(false)` when it would block.
fn flock(file: &File, op: libc::c_int) -> io::Result<bool> {
    // SAFETY: `flock` on a valid, open file descriptor borrowed for the call.
    if unsafe { libc::flock(file.as_raw_fd(), op | libc::LOCK_NB) } == 0 {
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

    /// A directory sync that fails names the directory and keeps the original error as its
    /// source, the OS error code with it, and the original kind.
    #[test]
    fn a_failed_directory_sync_keeps_the_original_error() {
        let tmp = tempfile::TempDir::with_prefix("tn_lmdb_dir_sync").expect("temp dir");
        let missing = tmp.path().join("missing");
        let e = fsync_directory(&missing).expect_err("no such directory");
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
        assert!(e.to_string().contains(&missing.display().to_string()), "{e}");
        let source = std::error::Error::source(&e).and_then(|s| s.downcast_ref::<io::Error>());
        assert_eq!(source.and_then(io::Error::raw_os_error), Some(libc::ENOENT), "{e:?}");
    }

    /// A second open of the same file, even in this process, cannot take the lock while the first
    /// holds it, and can once the first is closed.
    #[test]
    fn flock_exclusive_excludes_a_second_open() {
        let tmp = tempfile::TempDir::with_prefix("tn_lmdb_flock").expect("temp dir");
        let path = tmp.path().join("db");
        let first = File::create(&path).expect("create");
        assert!(flock_exclusive(&first).expect("lock"), "the first open takes the lock");
        let second = File::open(&path).expect("open again");
        assert!(!flock_exclusive(&second).expect("try lock"), "a second open must not get it");
        drop(first);
        assert!(flock_exclusive(&second).expect("lock"), "free once the first open closes");
    }

    /// Shared locks coexist; an exclusive one excludes them and they exclude it.
    #[test]
    fn flock_shared_coexists_and_excludes_an_exclusive_lock() {
        let tmp = tempfile::TempDir::with_prefix("tn_lmdb_flock_sh").expect("temp dir");
        let path = tmp.path().join("db");
        let writer = File::create(&path).expect("create");
        let (a, b) = (File::open(&path).expect("open"), File::open(&path).expect("open"));
        assert!(flock_exclusive(&writer).expect("lock"));
        assert!(!flock_shared(&a).expect("try lock"), "an exclusive lock excludes a shared one");
        drop(writer);
        assert!(flock_shared(&a).expect("lock"), "free once the exclusive holder closes");
        assert!(flock_shared(&b).expect("lock"), "shared locks coexist");
        let writer = File::open(&path).expect("open");
        assert!(!flock_exclusive(&writer).expect("try lock"), "a shared lock excludes a writer");
        drop((a, b));
        assert!(flock_exclusive(&writer).expect("lock"), "free once every reader closes");
    }

    /// `allocate_range` reserves real blocks for the whole range, not a sparse hole.
    #[test]
    fn allocate_range_reserves_real_blocks() {
        let tmp = tempfile::TempDir::with_prefix("tn_lmdb_alloc").expect("temp dir");
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

    /// An interrupted call is issued again until it completes; any other error ends it at once.
    #[test]
    fn retry_interrupted_reissues_only_an_interrupted_call() {
        let mut calls = 0;
        let done = retry_interrupted(|| {
            calls += 1;
            if calls < 3 {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        });
        assert!(done.is_ok(), "the third call completes: {done:?}");
        assert_eq!(calls, 3, "both interrupted calls are reissued");
        let mut calls = 0;
        let failed = retry_interrupted(|| {
            calls += 1;
            Err(io::Error::from_raw_os_error(libc::ENOSPC))
        });
        assert_eq!(failed.expect_err("a full disk is an error").raw_os_error(), Some(libc::ENOSPC));
        assert_eq!(calls, 1, "a real error is not retried");
    }

    /// A UUID parses to its 16 bytes, in either case and with the kernel's newline; any other
    /// shape, a non-hex digit (a sign included) and the nil UUID are refused.
    #[test]
    fn a_boot_id_parses_from_its_uuid() {
        let id = [
            0x68, 0x48, 0xEE, 0x5F, 0xDF, 0x00, 0x45, 0x50, 0x96, 0xC7, 0x6F, 0x1B, 0x92, 0xC8,
            0x21, 0xF2,
        ];
        assert_eq!(parse_uuid("6848EE5F-DF00-4550-96C7-6F1B92C821F2"), Some(id));
        assert_eq!(parse_uuid("6848ee5f-df00-4550-96c7-6f1b92c821f2\n"), Some(id));
        for bad in [
            "",
            "6848EE5FDF00455096C76F1B92C821F2",
            "6848EE5F-DF00-4550-96C7-6F1B92C821F",
            "6848EE5F-DF00-4550-96C76-F1B92C821F2",
            "6848EE5F-DF00-4550-96C7-6F1B92C821G2",
            "+848EE5F-DF00-4550-96C7-6F1B92C821F2",
            "6848EE5F-DF00-4550-96C7-6F1B92C821F2-",
            "00000000-0000-0000-0000-000000000000",
        ] {
            assert_eq!(parse_uuid(bad), None, "{bad:?}");
        }
    }

    /// The host's boot id is known on both supported platforms, and stays the same within the
    /// process: on a host without one, no weak meta would ever be trusted.
    #[test]
    fn the_host_boot_id_is_known_and_stable() {
        let id = boot_id().expect("the host's boot id");
        assert_ne!(id, [0; 16]);
        assert_eq!(boot_id(), Some(id));
    }

    #[test]
    fn os_page_size_is_a_power_of_two() {
        let page = os_page_size();
        assert!(page >= 4096 && page.is_power_of_two(), "page size {page}");
    }
}
