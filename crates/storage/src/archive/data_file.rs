//! The mmap-backed data file behind every [`Pack`](crate::archive::pack::Pack) (its data log and
//! position index) and the index files written in place ([`WriteMode::Random`]: the digest index
//! and the B+tree).
//!
//! Rather than buffered `read`/`write` syscalls, it maps the file into memory and does reads/writes
//! as `memcpy` against the mapping, so there is no per-IO syscall and no read/write buffers. It
//! implements `Read`/`Write`/`Seek` plus the inherent methods a pack needs (`slice`, `sync_all`,
//! `try_clone`, `truncate`, …), and takes mmap-specific open options ([`MmapFileOptions`]).
//!
//! ## Growth
//!
//! mmap cannot write past end-of-file, so the physical file must be sized *ahead* of the data. A
//! fresh file is left 0-length until the first write; then it is grown to
//! [`MmapFileOptions::initial_size`] and thereafter geometrically (doubling, each step capped at
//! [`MmapFileOptions::max_map_size`]). When a single mapping reaches `max_map_size`,
//! [`GrowMode::Reopen`] (the default) keeps one file and remaps it larger (absolute byte offsets
//! are preserved); [`GrowMode::Segment`] is reserved for a future multi-file layout and currently
//! errors on rollover.
//!
//! Each growth step **preallocates** the new range (`fallocate` on Linux, `F_PREALLOCATE` on macOS)
//! rather than a bare ftruncate, so it reserves real disk blocks up front: a full filesystem then
//! fails the growing write with an `io::Error` (which the pack turns into a failed state and a
//! clean shutdown) instead of a SIGBUS on the first `memcpy` store into an unbacked hole page. The
//! reserved padding is transient — the clean-close `Drop` truncates it back to the logical end — so
//! the extra real disk (≤ one growth step past the data) is only used while a file is open.
//! This holds where the filesystem implements preallocation: on Linux, ext4 and xfs, the supported
//! filesystems. Where it does not (`EOPNOTSUPP`/`ENOSYS`: ext2/ext3, NFS before 4.2,
//! FUSE without `fallocate`), growth falls back to a sparse `set_len`, with a warning logged once
//! per process on Linux, and a full disk may then surface as SIGBUS. On copy-on-write filesystems
//! (btrfs, ZFS) even a successful preallocation does not guarantee a later write can allocate.
//!
//! ## Transient padding, exact on exposure
//!
//! Because the file is sized ahead of the data, the physical file is padded to `capacity >= end`
//! while actively appending (`end` is the logical data length). Consumers never see the padding:
//! `MmapDataFile::try_clone` (for `PackIter`/`raw_iter`) does NOT truncate — it returns the logical
//! `end` as the boundary the reader must stop at — and only `Drop` (clean close) reconciles the
//! physical file to **exactly `end`** and then appends an 8-byte *clean-close sentinel*. Our own
//! reads are bounded by `end` and never see the padding.
//!
//! ## Clean-close sentinel
//!
//! On clean close `Drop` appends an 8-byte sentinel at physical EOF (`crc32(end)` followed by the
//! `crc32` of those four bytes; see `clean_close_sentinel`). A reopen validates it against the
//! physical size, and on a match strips it back to `end` and knows the file was sealed. A missing
//! or invalid sentinel means the file was **not** closed cleanly (most likely still padded after a
//! crash); `MmapDataFile::opened_unclean` surfaces that, the logical end is left at physical EOF,
//! and the pack's CRC + `recover_pack` path truncates it back to the last good record. The
//! self-referential second CRC is what stops trailing zero padding from masquerading as a clean
//! close (`crc32(0x00000000) != 0`).
//!
//! A writable reopen of a sealed file retires the sentinel on disk at open, so a crash after it
//! reopens unclean. With [`MmapFileOptions::defer_seal_retire`] the retirement waits for the
//! handle's first write instead: until then the file stays sealed exactly as it was opened, and a
//! drop leaves it untouched (tndb's log, whose open replays it and must refuse, never cut, a sealed
//! log that does not replay whole).
//!
//! ## Durability
//!
//! The default barrier `MmapDataFile::sync_all` is `msync` (flush dirty pages to the backing store)
//! — this is sufficient for data written within an already-fsync'd file size. `msync` does not
//! persist a size extension, so a growth marks the size unsynced and the next barrier fsyncs it
//! once after its `msync` (a barrier with no growth since the last stays `msync`-only). A
//! [`derived`](MmapFileOptions::derived) index skips that fsync and leaves its size to the
//! clean-close seal: its owner rebuilds it from the data log after any unclean open anyway.
//! `MmapDataFile::sync_disk` is the full,
//! slower `msync` + `fsync`, which additionally persists the file's size/metadata.
//!
//! **macOS.** Neither `msync` nor `fsync` is a full power-loss barrier there (that needs
//! `F_FULLFSYNC`, a flush of the whole drive's write cache). Commits are `msync`, so every other
//! sync here is a plain `fsync` too ([`fsync_file`], [`fdatasync_file`]): std's
//! `File::sync_all`/`sync_data` would issue `F_FULLFSYNC`, costing 10–20 ms each and stalling
//! every other write on the drive, for a guarantee the commits never give. A crash of the process
//! or the kernel loses nothing acked; a power loss can, as with MDBX's `Durable` mode with
//! `write_map`. On Linux these are exactly `File::sync_all`/`sync_data` (`fsync`/`fdatasync`,
//! which do flush the drive's cache).
//!
//! This module also holds the shared directory-durability helpers (`fsync_directory`,
//! `create_dir_synced`) used across the pack file types.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

use memmap2::{Mmap, MmapMut, MmapOptions};

use crate::archive::error::rename::RenameError;

/// Fsync a directory so recent directory-entry changes (file creates, renames)
/// are durable on Unix.
///
/// `File::sync_all` on a regular file does not flush the parent directory entry
/// that names it.  A crash between a create/rename and a subsequent directory
/// fsync can leave the filesystem with the entry missing even though the file's
/// own data and metadata are durable.  Calling this on the parent directory
/// closes that gap.
///
/// # Precondition
///
/// `path` must refer to a directory.  Passing a regular file silently fsyncs
/// that file's contents instead of providing the directory-entry durability
/// guarantee callers expect, so all in-crate call sites pass a directory.
pub(crate) fn fsync_directory(path: &Path) -> Result<(), io::Error> {
    #[cfg(test)]
    if FAIL_DIR_SYNC.with_borrow(|fail| fail.as_deref() == Some(path)) {
        return Err(io::Error::other(format!("injected directory fsync failure for {path:?}")));
    }
    fsync_file(&File::open(path)?)
}

#[cfg(test)]
thread_local! {
    /// Test-only: the next [`MapView::msync`] on this thread fails after its `msync` call (a
    /// failed writeback).
    pub(crate) static FAIL_NEXT_VIEW_MSYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Test-only: every [`fsync_directory`] of this path on this thread fails (a failed directory
    /// sync), so a test can check that a create propagates it.
    pub(crate) static FAIL_DIR_SYNC: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// The directory holding `path`'s entry: its parent, with `.` for a bare relative name (whose
/// [`Path::parent`] is empty), or `None` for a root.
fn entry_dir(path: &Path) -> Option<&Path> {
    path.parent().map(|parent| if parent.as_os_str().is_empty() { Path::new(".") } else { parent })
}

/// `fsync(2)` `file`: [`File::sync_all`] everywhere but macOS, where that is `F_FULLFSYNC`
/// (see the module docs' Durability section) and this is a plain `fsync`.
pub(crate) fn fsync_file(file: &File) -> io::Result<()> {
    // Retried when a signal interrupts it, as `File::sync_all` does: an interrupted call is not a
    // sync error, and the callers that fail-stop on one (the barriers, which poison the file, the
    // tndb commit thread, an open) would stop the node for nothing. Not every caller fail-stops:
    // the clean-close seal in `Drop` only logs a failed fsync (the file was msynced before its
    // seal, and a lost seal costs a recovery pass; an owner that must know seals explicitly with
    // `MmapDataFile::seal`), as do the `LatestConsensus` slot syncs.
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
        disk_model::flushed_all(file);
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
            disk_model::flushed_all(file);
        }
        synced
    }
}

/// Drop `file`'s cached pages, so later reads of it come from the disk (Linux:
/// `posix_fadvise(POSIX_FADV_DONTNEED)` over the whole file). An unclean open with
/// [`MmapFileOptions::drop_cache_on_unclean_open`] calls it right after its fsync and before it
/// maps the file, so no page is dirty and this handle maps none. The kernel keeps any page that
/// is dirty, locked, under writeback or mapped by another process; nothing in this crate maps the
/// file then (tndb's `lock_dir` keeps a second handle off its directory, and `try_clone` readers
/// use `pread`). Elsewhere a no-op: macOS has no such advice, and is not a validator platform.
fn drop_cached_pages(file: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a plain syscall on a file descriptor `file` owns and keeps open for the call; it
        // touches no Rust memory. Offset 0 with length 0 covers the whole file.
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        // `posix_fadvise` returns the error number itself, not -1 with `errno` set.
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
    #[cfg(test)]
    disk_model::cache_dropped(file);
    Ok(())
}

/// Test-only model of what a power loss keeps (the tndb power-loss tests use it): for each
/// tracked file (by inode, so a rename keeps it), an image of the bytes that reached the disk and
/// the file's durable length. Every real flush updates the bytes: an msync copies the pages it
/// covers (whole pages, as the kernel flushes them), an fsync or fdatasync the whole file. Only an
/// fsync or fdatasync moves the durable length (both make the size durable; an msync writes data
/// pages but no size change): it starts at the file's length when tracked. A power loss cuts the
/// file back to its durable length and writes the image over it, zeroing what the image lacks:
/// bytes never flushed are gone, and so are bytes msynced past a size no sync made durable (a lost
/// size update leaves the file shorter, not zero-filled). A dropped page cache
/// (`drop_cached_pages`) leaves the file reading the image's bytes at its current length. Untracked
/// files cost one atomic load per flush.
///
/// [`set_flushes_lost`] models a failed writeback that a sync can no longer report: the flushes
/// succeed, the image stays as it was, and the page cache keeps the newer bytes.
#[cfg(test)]
pub(crate) mod disk_model {
    use std::{
        cell::Cell,
        collections::HashMap,
        fs::{File, OpenOptions},
        os::unix::fs::{FileExt as _, MetadataExt as _},
        path::Path,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex, PoisonError,
        },
    };

    static TRACKED: AtomicUsize = AtomicUsize::new(0);
    static IMAGES: Mutex<Option<HashMap<u64, Image>>> = Mutex::new(None);

    /// What the disk holds of one tracked file.
    struct Image {
        /// The bytes that reached the disk; an msync can carry them past `durable_len`.
        bytes: Vec<u8>,
        /// The file's length on the disk: its length when tracked, then as of its last fsync or
        /// fdatasync.
        durable_len: usize,
    }

    thread_local! {
        /// See [`set_flushes_lost`].
        static FLUSHES_LOST: Cell<bool> = const { Cell::new(false) };
    }

    /// While `lost`, the flushes this thread runs (msyncs and fsyncs) report success but leave
    /// every image as it was. That is what a process restarted after a failed writeback sees on
    /// Linux: the error is reported once, to the process that then exited, and the failed pages
    /// stay in the page cache, clean, so the next open's fsync succeeds without writing them.
    pub(crate) fn set_flushes_lost(lost: bool) {
        FLUSHES_LOST.with(|flag| flag.set(lost));
    }

    fn flushes_lost() -> bool {
        FLUSHES_LOST.with(Cell::get)
    }

    fn images() -> std::sync::MutexGuard<'static, Option<HashMap<u64, Image>>> {
        IMAGES.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Start modeling the file at `path`: its current bytes and length are taken as on disk.
    pub(crate) fn track(path: &Path) {
        let ino = std::fs::metadata(path).expect("metadata").ino();
        let bytes = std::fs::read(path).expect("read the tracked file");
        let image = Image { durable_len: bytes.len(), bytes };
        images().get_or_insert_with(HashMap::new).insert(ino, image);
        TRACKED.fetch_add(1, Ordering::Relaxed);
    }

    /// Stop modeling the file at `path`.
    pub(crate) fn untrack(path: &Path) {
        let ino = std::fs::metadata(path).expect("metadata").ino();
        if images().as_mut().and_then(|images| images.remove(&ino)).is_some() {
            TRACKED.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// An msync of `[start, end)` of the file with inode `ino`, whose mapping is `map`.
    pub(crate) fn flushed_range(ino: u64, map: &[u8], start: u64, end: u64) {
        if TRACKED.load(Ordering::Relaxed) == 0 || start >= end || flushes_lost() {
            return;
        }
        let mut images = images();
        let Some(image) = images.as_mut().and_then(|images| images.get_mut(&ino)) else {
            return;
        };
        let page = super::page_size();
        let from = (start - start % page) as usize;
        let to = (end.div_ceil(page) * page).min(map.len() as u64) as usize;
        if from >= to {
            return;
        }
        let image = &mut image.bytes;
        if image.len() < to {
            image.resize(to, 0);
        }
        image[from..to].copy_from_slice(&map[from..to]);
    }

    /// [`flushed_range`] for a mapping another thread may be appending to (a [`MapView`]'s): its
    /// pages are copied through the raw pointer, never borrowed as a slice, since bytes past `end`
    /// in the last page can be written meanwhile (the model keeps whatever the page holds, as the
    /// flush did).
    pub(crate) fn flushed_range_raw(ino: u64, base: *const u8, mapped: u64, start: u64, end: u64) {
        if TRACKED.load(Ordering::Relaxed) == 0 || start >= end || flushes_lost() {
            return;
        }
        let mut images = images();
        let Some(image) = images.as_mut().and_then(|images| images.get_mut(&ino)) else {
            return;
        };
        let page = super::page_size();
        let from = (start - start % page) as usize;
        let to = (end.div_ceil(page) * page).min(mapped) as usize;
        if from >= to {
            return;
        }
        let image = &mut image.bytes;
        if image.len() < to {
            image.resize(to, 0);
        }
        // SAFETY: the caller's `[base, base + mapped)` is a live mapping and `[from, to)` lies
        // within it; the copy goes into the model's own buffer, which no one else touches.
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(from), image[from..to].as_mut_ptr(), to - from);
        }
    }

    /// The inode of `file`, if any file is tracked (else 0, never a tracked one).
    pub(crate) fn ino_if_tracking(file: &File) -> u64 {
        if TRACKED.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        file.metadata().map(|meta| meta.ino()).unwrap_or(0)
    }

    /// An fsync or fdatasync of `file`: every byte of it is on disk, and so is its length.
    pub(crate) fn flushed_all(file: &File) {
        let ino = ino_if_tracking(file);
        if ino == 0 || flushes_lost() {
            return;
        }
        let mut images = images();
        let Some(image) = images.as_mut().and_then(|images| images.get_mut(&ino)) else {
            return;
        };
        let len = file.metadata().map(|meta| meta.len()).unwrap_or(0) as usize;
        let mut bytes = vec![0_u8; len];
        if file.read_exact_at(&mut bytes, 0).is_ok() {
            *image = Image { bytes, durable_len: len };
        }
    }

    /// A power loss: the file at `path` is cut back (or extended) to its durable length and keeps
    /// only what its image holds below it; every byte the image lacks reads zero. The image is cut
    /// with it: what lay past the durable length is gone from the disk too.
    pub(crate) fn power_loss(path: &Path) {
        let ino = std::fs::metadata(path).expect("metadata").ino();
        let mut images = images();
        let image =
            images.as_mut().and_then(|images| images.get_mut(&ino)).expect("the file is tracked");
        image.bytes.resize(image.durable_len, 0);
        let file = OpenOptions::new().write(true).open(path).expect("open");
        file.set_len(image.durable_len as u64).expect("cut to the durable length");
        restore(&file, &image.bytes);
    }

    /// `file`'s cached pages were dropped (`drop_cached_pages`): a tracked file now reads what its
    /// image holds, as after a power loss, but keeps its length (the size is not cached data; it
    /// is opened writable then).
    pub(crate) fn cache_dropped(file: &File) {
        let ino = ino_if_tracking(file);
        if ino == 0 {
            return;
        }
        let Some(bytes) =
            images().as_ref().and_then(|images| images.get(&ino).map(|image| image.bytes.clone()))
        else {
            return;
        };
        restore(file, &bytes);
    }

    /// Write `image` over `file`, keeping its length: every byte past the image reads zero.
    fn restore(file: &File, image: &[u8]) {
        let len = file.metadata().expect("metadata").len() as usize;
        file.write_all_at(&image[..image.len().min(len)], 0).expect("write the image");
        let zeros = vec![0_u8; 1 << 20];
        let mut at = image.len();
        while at < len {
            let n = zeros.len().min(len - at);
            file.write_all_at(&zeros[..n], at as u64).expect("zero the lost bytes");
            at += n;
        }
    }
}

/// Create `dir` and any missing parents, and fsync the directory holding each level it created,
/// the outermost first, so every new entry survives a crash (an entry is durable only once the
/// directory holding it is synced, and a lost upper level loses everything below it). A failed
/// sync is an error. When `dir` already exists this syncs its parent once, which is harmless and
/// covers a retried call whose earlier sync failed after the create.
pub(crate) fn create_dir_synced(dir: &Path) -> Result<(), io::Error> {
    let mut missing = Vec::new();
    let mut level = Some(dir);
    while let Some(d) = level.filter(|d| !d.as_os_str().is_empty() && !d.exists()) {
        missing.push(d);
        level = d.parent();
    }
    std::fs::create_dir_all(dir)?;
    if missing.is_empty() {
        missing.push(dir);
    }
    for created in missing.iter().rev() {
        if let Some(parent) = entry_dir(created) {
            fsync_directory(parent)?;
        }
    }
    Ok(())
}

/// Size a fresh file is first grown to on the first write (1 MiB).
pub const DEFAULT_INITIAL_SIZE: u64 = 1 << 20;
/// Default cap on a single growth step / mapping increment (128 MiB).
pub const DEFAULT_MAX_MAP_SIZE: u64 = 128 << 20;

/// What to do when a single mapped file reaches [`MmapFileOptions::max_map_size`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrowMode {
    /// Keep one file and remap it larger; absolute byte offsets are preserved. Fully supported.
    Reopen,
    /// Roll over into a new segment file. Reserved for a future multi-file layout; currently
    /// errors on rollover.
    Segment,
}

/// Where [`MmapDataFile::write`] places bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Writes always go to the logical `end` (append log). The default — used by the pack data
    /// file and the position index.
    #[default]
    Append,
    /// Writes go to the current seek position (overwrite), extending the high-water `end` only
    /// when they pass it. Used by the digest index (fixed-layout hash buckets overwritten in
    /// place, plus its append-only overflow log driven by explicit `seek(End)`).
    Random,
}

/// Access-pattern hint applied to the mapping via `madvise` (best-effort; unix only, and a failed
/// hint is non-fatal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MmapAccess {
    /// No hint — keep the kernel's default readahead. The default.
    #[default]
    Normal,
    /// `MADV_SEQUENTIAL`: expect sequential access (aggressive readahead) — for scan-heavy files.
    Sequential,
    /// `MADV_RANDOM`: expect random access (suppress readahead) — for point-lookup-heavy files
    /// such as the digest index's hash buckets.
    Random,
}

/// Options controlling the mmap-backed file's allocation, growth, write placement, and access hint.
#[derive(Debug, Clone, Copy)]
pub struct MmapFileOptions {
    /// Bytes the file is first grown to on the first write; growth is geometric from here.
    pub initial_size: u64,
    /// Cap on a single growth step: growth doubles until a step would exceed this, then advances
    /// in `max_map_size` increments. Also the rollover threshold for [`GrowMode::Segment`].
    pub max_map_size: u64,
    /// Behaviour when a mapping reaches `max_map_size`.
    pub grow_mode: GrowMode,
    /// Append vs random-overwrite write placement (see [`WriteMode`]).
    pub write_mode: WriteMode,
    /// Access-pattern `madvise` hint for the mapping (see [`MmapAccess`]).
    pub access: MmapAccess,
    /// The file is a derived index that its owner rebuilds from the data log after any unclean
    /// open, so its barriers skip the deferred size fsync ([`MmapDataFile::sync_size_if_grown`]):
    /// a growth's size becomes durable at the clean-close seal (or [`MmapDataFile::sync_disk`]).
    /// After a crash such a file may be shorter than its own header claims; that is safe only
    /// because an unsealed derived file is never trusted to keep data: its owner rebuilds it, and
    /// nothing reads one before that except the consensus pack's position index in
    /// `recover_pack`, which takes it only as evidence to refuse a cut. The position indexes
    /// (pdx) and tndb's B+tree indexes set it. The digest indexes (hdx, odx) do not: the epoch
    /// records' and certificate pack's recoveries read an unsealed hdx's attested data length
    /// before any rebuild, as evidence to refuse a cut, and a short hdx fails to open, attesting
    /// nothing.
    pub derived: bool,
    /// Bytes of address space a writable handle reserves for its mapping (0 = off). With a
    /// reservation the file is mapped once, at `max(reserve, file length)` bytes, and grows
    /// underneath that mapping (preallocate + extend) without ever remapping, so the mapping never
    /// moves while the handle is open — the property a reader that holds no lock needs. Pages past
    /// the physical EOF are never touched (reads are bounded by the logical end, writes by the
    /// physical size). Growing past the reservation falls back to a remap with a larger one, the
    /// only case in which the mapping moves. Virtual address space only; read-only handles (which
    /// map sealed files) ignore it.
    pub reserve: u64,
    /// For a [`Pack`](crate::archive::pack::Pack) over this file: the largest record it writes and
    /// reads (`None`: [`MAX_RECORD_SIZE`](crate::archive::pack_iter::MAX_RECORD_SIZE)). A local
    /// log that is never read from a peer may take more (tndb's does); the data file ignores
    /// it.
    pub max_record_size: Option<u32>,
    /// A writable open of a sealed file keeps its clean-close sentinel, on disk and in the
    /// mapping, until the handle's first [`Write::write`], instead of retiring it at open (see
    /// [`MmapDataFile::open_with`]). Until then the file stays sealed exactly as it was opened: a
    /// crash, a panic or a failed open leaves it sealed, and its drop changes no byte. For a log
    /// whose owner replays it at open and must refuse, not cut, a sealed log that does not
    /// replay whole (tndb's): with the sentinel retired before that replay, a kill during a
    /// failing open would let the next open take the damage for a crash's torn tail. Such a file
    /// must change only through `write` while the seal is pending (`slice_mut`, `ensure_len`,
    /// `truncate` and a `rewind_to` below the end assert it in debug builds).
    pub defer_seal_retire: bool,
    /// An unclean writable open drops the file's cached pages right after its fsync (see
    /// [`MmapDataFile::open_with`]), so the open, and its owner's recovery, read what the disk
    /// holds rather than what the page cache holds. Linux reports a writeback error once: when
    /// the process that saw it has exited, the next open's fsync succeeds while the failed pages
    /// sit in the page cache, clean (so no later sync writes them) but never written. A recovery
    /// reading them would accept bytes the disk lacks, and new writes acknowledged after them
    /// would sit past a hole that a power loss reveals. For a log whose owner replays it at open
    /// and goes on appending (tndb's). It costs that replay a read from the disk instead of the
    /// warm cache, which is why the packs (which mostly refuse at such a hole rather than cut)
    /// keep the cache. Linux only; elsewhere the drop is a no-op.
    pub drop_cache_on_unclean_open: bool,
}

impl Default for MmapFileOptions {
    fn default() -> Self {
        Self {
            initial_size: DEFAULT_INITIAL_SIZE,
            max_map_size: DEFAULT_MAX_MAP_SIZE,
            grow_mode: GrowMode::Reopen,
            write_mode: WriteMode::Append,
            access: MmapAccess::Normal,
            derived: false,
            reserve: 0,
            max_record_size: None,
            defer_seal_retire: false,
            drop_cache_on_unclean_open: false,
        }
    }
}

/// A reader's view of a file's mapping that needs no lock: the mapping's base address and length,
/// plus a length the owner has published as fully written. Shared (`Arc`) between the file and its
/// lock-free readers; the file updates the mapping fields whenever its mapping changes, and the
/// owner advances [`Self::publish_len`].
///
/// Validity, checked rather than trusted: a slice borrows mapped memory, so
/// - every read is clamped to the published length, the mapping, and the file's physical size
///   (stored before any shrink), so a stale length returns `None` instead of reaching unmapped or
///   truncated pages;
/// - the file refuses to shrink or rewind below the published length (`truncate` errs, `rewind_to`
///   poisons), so no published byte a reader borrowed is cut or zeroed;
/// - a mapping replaced while the view is shared is retired (kept mapped until the file drops), and
///   a file dropped while the view is still shared hands its mappings to the view, which unmaps
///   them when the last reader lets go.
///
/// What no check sees is why [`Self::slice`] and [`Self::tail_within`] are `unsafe`: bytes past the
/// published length that `tail_within` reads under the caller's own limit, and in-place rewrites
/// of published bytes (a B+tree reuses only pages no reader's snapshot reaches). Each states the
/// contract its caller keeps (`# Safety`). [`Self::msync`] borrows no memory and stays safe.
#[derive(Debug)]
pub(crate) struct MapView {
    base: AtomicPtr<u8>,
    mapped: AtomicU64,
    published: AtomicU64,
    /// The file's sticky poison latch (see [`MmapDataFile::poison`]). Kept here, not on the file,
    /// so a barrier that fails through a shared view ([`Self::msync`], from tndb's commit thread
    /// or a pack's rollback) latches the file wherever it runs, without its owner having to
    /// remember to. Relaxed: the latch orders no other memory, and an owner that acts on a
    /// barrier's failure learns of it through the channel or lock that reported it, which orders
    /// the store before its load.
    write_failed: AtomicBool,
    /// The file's physical size: stored after the file grows and before it shrinks, so a read
    /// clamped to it never reaches a page past the end of the file (a SIGBUS).
    physical: AtomicU64,
    /// Mappings the file left when it dropped while this view was still shared (see
    /// `MmapDataFile::release_mappings`): unmapped when the last reader drops the view.
    kept: std::sync::Mutex<Vec<Backing>>,
    /// Test-only: the file's inode, for the disk model (see [`disk_model`]).
    #[cfg(test)]
    ino: AtomicU64,
}

impl MapView {
    fn new() -> Self {
        Self {
            base: AtomicPtr::new(std::ptr::null_mut()),
            mapped: AtomicU64::new(0),
            published: AtomicU64::new(0),
            write_failed: AtomicBool::new(false),
            physical: AtomicU64::new(0),
            kept: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            ino: AtomicU64::new(0),
        }
    }

    /// Point the view at a mapping. The base is stored before the length, and a reader loads the
    /// length first: one that sees the new length then sees the new base, and one that sees the
    /// old length reads a prefix of the (only ever larger) new mapping or the retired old one.
    fn set_mapping(&self, base: *const u8, len: u64) {
        self.base.store(base.cast_mut(), Ordering::Release);
        self.mapped.store(len, Ordering::Release);
    }

    /// Publish `len` bytes as fully written and readable (they must already be in the mapping, and
    /// no byte a reader can reach may change: an append-only log never rewrites them, and a B+tree
    /// rewrites only pages no reader's snapshot reaches).
    pub(crate) fn publish_len(&self, len: u64) {
        self.published.store(len, Ordering::Release);
    }

    /// Latch the file's poison (see [`MmapDataFile::poison`]) from a holder of the view: for a
    /// barrier on the file run outside both the file and the view (tndb's commit thread fsyncs a
    /// duplicate descriptor after its log grows), so a failure there keeps the file unsealed
    /// however its owner is later dropped.
    pub(crate) fn poison(&self) {
        self.write_failed.store(true, Ordering::Relaxed);
    }

    /// The readable bound: the mapping and the file's physical size, whichever is shorter.
    fn backed(&self) -> u64 {
        self.mapped.load(Ordering::Acquire).min(self.physical.load(Ordering::Acquire))
    }

    /// `msync(MS_SYNC)` the mapped bytes `[start, end)` (the start rounded down to a page), from
    /// any thread, while the owner keeps appending past `end` (tndb's commit thread syncs its log
    /// this way). The caller keeps the file open for the call (see the type docs). A failure
    /// latches the file's poison (see [`MmapDataFile::poison`]).
    pub(crate) fn msync(&self, start: u64, end: u64) -> io::Result<()> {
        if start >= end {
            return Ok(());
        }
        // Length first, then base (see `set_mapping`): either the current mapping, or a retired
        // one still mapped, covers `[0, mapped)`.
        let mapped = self.mapped.load(Ordering::Acquire);
        let base = self.base.load(Ordering::Acquire);
        if base.is_null() || end > mapped.min(self.physical.load(Ordering::Acquire)) {
            self.poison();
            return Err(io::Error::other("sync range is not mapped within the file"));
        }
        let aligned = start - start % page_size();
        // SAFETY: `[base, base + mapped)` is a live mapping (a mapping replaced while the view is
        // shared is retired, not unmapped, until the file drops, and the ticket holding this view
        // is used while its file is open), and `[aligned, end)` lies within it. `msync` reads no
        // Rust memory.
        let rc = unsafe {
            libc::msync(base.add(aligned as usize).cast(), (end - aligned) as usize, libc::MS_SYNC)
        };
        let synced = if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) };
        #[cfg(test)]
        let synced = synced.and_then(|()| match FAIL_NEXT_VIEW_MSYNC.replace(false) {
            true => Err(io::Error::other("injected msync failure")),
            false => Ok(()),
        });
        if let Err(error) = synced {
            self.poison();
            return Err(error);
        }
        #[cfg(test)]
        disk_model::flushed_range_raw(self.ino.load(Ordering::Relaxed), base, mapped, start, end);
        Ok(())
    }

    /// Borrow exactly `[offset, offset + len)` if it lies within the published bytes, else
    /// `None`. Only those bytes are borrowed, so a file whose owner rewrites other published bytes
    /// in place (a B+tree reusing a page no reader's snapshot can reach) is never borrowed from
    /// where it is being written.
    ///
    /// # Safety
    ///
    /// No byte of `[offset, offset + len)` may be written while the returned slice lives, and
    /// every earlier write to it must happen before this call. The view checks that the range is
    /// published, mapped and inside the file, and the file never cuts, zeroes or unmaps a
    /// published byte under it, so an owner that writes bytes before publishing them
    /// ([`Self::publish_len`]) and never again meets this. What the view cannot see is an owner
    /// rewriting published bytes in place: the caller must know no such rewrite reaches the range
    /// (a B+tree reuses a page only once no snapshot that reaches it is alive).
    pub(crate) unsafe fn slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len as u64)?;
        let limit = self.published.load(Ordering::Acquire).min(self.backed());
        if end > limit {
            return None;
        }
        let base = self.base.load(Ordering::Acquire);
        if base.is_null() {
            return None;
        }
        // SAFETY: `[base, base + mapped)` is a live mapping (see the type docs), `[offset, end)`
        // lies within it, within the file and within the published bytes, and the caller keeps
        // every byte of it unwritten while the slice lives (this fn's `# Safety`).
        Some(unsafe { std::slice::from_raw_parts(base.add(offset as usize), len) })
    }

    /// Borrow every byte from `offset` up to `limit` (`None` past it), where `limit` is a length
    /// the owner wrote in full before the reader's snapshot: one bounds check for a reader that
    /// learns a record's length from its own prefix. For an append-only file (a log), whose
    /// written bytes never change; a file rewritten in place reads through [`Self::slice`].
    ///
    /// # Safety
    ///
    /// Every byte of `[offset, limit)` must be written in full before the call, by writes that
    /// happen before it (the caller learned `limit` under the writer's lock, or by loading a value
    /// the writer stored after those writes), and none may be written, or cut from the file
    /// (`truncate`, `rewind_to`), while the returned slice lives. The view clamps the range to the
    /// mapping and the file's physical size but not to the published length (a log's owner reads
    /// bytes it wrote and never published), so it cannot check this: a `limit` past what the
    /// writer has finished reads bytes it may be writing.
    pub(crate) unsafe fn tail_within(&self, offset: u64, limit: u64) -> Option<&[u8]> {
        let end = limit.min(self.backed());
        let len = end.checked_sub(offset)?;
        let base = self.base.load(Ordering::Acquire);
        if base.is_null() {
            return None;
        }
        // SAFETY: `[base, base + mapped)` is a live mapping (see the type docs: replaced mappings
        // stay mapped until the file drops, and a dropped file's go to the view), `[offset, end)`
        // lies within it and within the file, and the caller has written every byte of it before
        // the call and neither writes nor cuts any while the slice lives (this fn's `# Safety`).
        Some(unsafe { std::slice::from_raw_parts(base.add(offset as usize), len as usize) })
    }
}

/// The OS page size (cached).
fn page_size() -> u64 {
    static PAGE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    // SAFETY: `sysconf` reads a system constant.
    *PAGE
        .get_or_init(|| u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(4096))
}

#[cfg(test)]
thread_local! {
    /// Test-only: the sync barriers (`flush_dirty` with a sync) run on this thread so far.
    pub(crate) static SYNC_BARRIERS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Borrow `[start, start + len)` of a writable mapping mutably through its raw pointer, so only
/// that range is borrowed: `MmapMut`'s `DerefMut` would assert unique access to the whole mapping
/// while lock-free readers ([`MapView`]) hold slices of other, published bytes of it.
fn map_range_mut(map: &mut MmapMut, start: usize, len: usize) -> &mut [u8] {
    assert!(start.checked_add(len).is_some_and(|end| end <= map.len()), "range outside the map");
    // SAFETY: the range lies within the live mapping (checked above), and `&mut map` makes this the
    // only writer. Readers never read bytes being written: a reader sees only published bytes,
    // which are never written again.
    unsafe { std::slice::from_raw_parts_mut(map.as_mut_ptr().add(start), len) }
}

/// The active memory map, or none for a zero-length file.
#[derive(Debug)]
enum Backing {
    /// Writable shared mapping over `[0, capacity)`.
    Rw(MmapMut),
    /// Read-only mapping over `[0, capacity)`.
    Ro(Mmap),
    /// No mapping (file is currently 0-length).
    Empty,
}

/// Width of the clean-close sentinel appended at physical EOF by [`MmapDataFile`]'s `Drop`.
pub(crate) const SENTINEL_LEN: u64 = 8;

/// Size of the optional commit marker written to the tail of the mmap capacity (see
/// [`MmapDataFile::stamp_commit_marker`]): `[committed_end u64][crc32 u32][crc32(crc32) u32]`.
pub(crate) const COMMIT_MARKER_LEN: u64 = 16;

/// Build the 16-byte commit marker for a durable data end of `committed_end`.
///
/// Unlike the clean-close sentinel (which encodes only CRCs and derives the length from the file
/// size), this carries the raw `committed_end` because the marker sits in the capacity padding, not
/// at a length-defining EOF. The trailing CRC-of-CRC is the same self-referential guard the
/// clean-close sentinel uses: it stops zero padding from validating, and — because it ties the
/// bytes to `committed_end` rather than to the file size — a marker can never be mistaken for a
/// clean-close sentinel (which encodes `crc32(disk_len - 8)`).
fn commit_marker(committed_end: u64) -> [u8; 16] {
    let mut marker = [0_u8; 16];
    marker[0..8].copy_from_slice(&committed_end.to_le_bytes());
    let pos_crc = crc32fast::hash(&marker[0..8]);
    marker[8..12].copy_from_slice(&pos_crc.to_le_bytes());
    let crc_of_crc = crc32fast::hash(&marker[8..12]);
    marker[12..16].copy_from_slice(&crc_of_crc.to_le_bytes());
    marker
}

/// Parse and validate the 16-byte commit marker at the tail of a `disk_len`-byte file. Returns the
/// stored `committed_end` only when both CRCs check out and the position lies within the file
/// (`<= disk_len - COMMIT_MARKER_LEN`). Absent/torn/zeroed markers return `None` so recovery falls
/// back to the WAL probe. This is fail-safe: the double CRC rejects zero padding and garbage, and
/// even an (astronomically unlikely) CRC-valid but too-small `committed_end` is harmless — recovery
/// only errors when the WAL stops *below* the marker, so a low/stale marker never fabricates a
/// watermark ahead of the durable data.
fn parse_commit_marker(bytes: &[u8; 16], disk_len: u64) -> Option<u64> {
    let committed_end = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
    if crc32fast::hash(&bytes[0..8]) != u32::from_le_bytes(bytes[8..12].try_into().ok()?) {
        return None;
    }
    if crc32fast::hash(&bytes[8..12]) != u32::from_le_bytes(bytes[12..16].try_into().ok()?) {
        return None;
    }
    let max_pos = disk_len.checked_sub(COMMIT_MARKER_LEN)?;
    (committed_end <= max_pos).then_some(committed_end)
}

/// Build the 8-byte clean-close sentinel for a file whose logical data length is `end`.
///
/// Layout (little-endian): bytes `[0..4]` are `crc32(end)`, bytes `[4..8]` are the `crc32` of those
/// first four bytes. The self-referential second CRC is what stops trailing zero padding from
/// masquerading as a clean close: `crc32(0x00000000) != 0`, so an all-zero tail never satisfies it.
fn clean_close_sentinel(end: u64) -> [u8; 8] {
    let mut sentinel = [0_u8; 8];
    let len_crc = crc32fast::hash(&end.to_le_bytes());
    sentinel[0..4].copy_from_slice(&len_crc.to_le_bytes());
    let crc_of_crc = crc32fast::hash(&sentinel[0..4]);
    sentinel[4..8].copy_from_slice(&crc_of_crc.to_le_bytes());
    sentinel
}

/// True iff `tail` is a valid clean-close sentinel for a file whose logical data length is
/// `data_len`. Because the sentinel is fully determined by `data_len`, an exact match confirms both
/// that the first CRC equals `crc32(data_len)` (ties the marker to the actual file size, so a
/// torn/padded tail that happens to be self-consistent still fails) and that the trailing CRC
/// equals `crc32` of the first four bytes (self-consistency / zero-padding guard).
pub(crate) fn sentinel_matches(tail: &[u8; 8], data_len: u64) -> bool {
    *tail == clean_close_sentinel(data_len)
}

/// Given the physical size `disk_len` of `file`, return `(logical_end, opened_unclean)` by checking
/// for a clean-close sentinel at physical EOF. A valid sentinel means the file was sealed and the
/// logical data ends [`SENTINEL_LEN`] bytes before EOF; a missing/invalid sentinel means the file
/// was not cleanly closed (most likely still padded), so the logical end is left at the physical
/// size for the heal path. A 0-length file is fresh/empty, not unclean.
fn detect_sentinel(file: &File, disk_len: u64) -> io::Result<(u64, bool)> {
    if disk_len == 0 {
        return Ok((0, false));
    }
    if disk_len >= SENTINEL_LEN {
        let mut tail = [0_u8; 8];
        file.read_exact_at(&mut tail, disk_len - SENTINEL_LEN)?;
        if sentinel_matches(&tail, disk_len - SENTINEL_LEN) {
            return Ok((disk_len - SENTINEL_LEN, false));
        }
    }
    // Too short to hold a sentinel, or the tail is not a valid one: not cleanly sealed.
    Ok((disk_len, true))
}

/// An mmap-backed data file — the storage behind every [`Pack`](crate::archive::pack::Pack) and the
/// in-place index files (see [`WriteMode`]).
#[derive(Debug)]
pub struct MmapDataFile {
    file: File,
    path: PathBuf,
    backing: Backing,
    /// Logical length: bytes of real data (also the append cursor). `<= capacity` while writing.
    end: u64,
    /// Mapped length == current physical file size while open.
    capacity: u64,
    /// Read/seek cursor over the logical `[0, end)` range.
    seek_pos: u64,
    read_only: bool,
    remove_on_drop: bool,
    /// High-water offset already `msync`'d to the backing store. The `WriteMode::Append` sync fast
    /// path flushes only the newly-written tail `[flushed_end, end)` instead of re-scanning the
    /// whole mapping each sync. Only meaningful for append (in-place `Random` writes can land
    /// below this offset, so that mode always flushes the full `[0, end)` range). `AtomicU64`
    /// (not `Cell`) so the file stays `Sync` behind `&self` (e.g. the `&self` `sync_all`), letting
    /// a pack hold it behind a `Send + Sync` trait object.
    flushed_end: AtomicU64,
    /// True when this handle was opened from a file with no valid clean-close sentinel — the file
    /// was not sealed by a clean `Drop` and is most likely still padded, so the pack's heal path
    /// should run. Set once at open; a fresh (0-length) file is not considered unclean.
    opened_unclean: bool,
    /// A writable handle opened a sealed file with [`MmapFileOptions::defer_seal_retire`] and has
    /// not yet written: the clean-close sentinel still sits at `[end, end + SENTINEL_LEN)`, on
    /// disk and in the mapping, and the file on disk is exactly as it was opened. The first
    /// `write` retires the sentinel ([`Self::retire_seal_if_pending`]); until then `Drop` leaves
    /// the file alone.
    seal_pending: bool,
    /// Set by [`Self::seal`]: the handle is closed (sealed, or its seal failed and was reported),
    /// so `Drop` leaves the file as the seal left it.
    closed: bool,
    /// Durable `committed_end` recovered from the tail commit marker of an unclean file, if a
    /// valid one is present (see [`Self::stamp_commit_marker`] / [`Self::committed_end`]).
    /// `None` on clean or fresh opens, or when no valid marker was written/flushed
    /// (best-effort). Recovery uses it as an index-free acked-data watermark to catch at-rest
    /// corruption of the last committed record.
    committed_marker: Option<u64>,
    /// Set when a growth extended the file's size since the last fsync. `msync` does not persist
    /// size (inode metadata), so the next durability barrier fsyncs once before it returns (see
    /// [`Self::sync_size_if_grown`]); a growth itself does not sync. `AtomicBool` because the
    /// barriers take `&self`.
    size_unsynced: AtomicBool,
    /// Length of the reserved mapping (see [`MmapFileOptions::reserve`]), or 0 in the classic
    /// mode, where the mapping is exactly the physical file and is replaced on every growth.
    reserved: u64,
    /// The lock-free reader view of the current mapping (see [`MapView`]).
    view: Arc<MapView>,
    /// Mappings replaced while [`Self::view`] was shared: kept mapped until this file drops so a
    /// reader still holding an old base stays valid.
    retired: Vec<Backing>,
    /// Growths so far (see [`Self::growths`]: a size fsync made outside this file covers only the
    /// growths before it).
    growths: u64,
    /// Test-only: how many deferred size fsyncs barriers have run.
    #[cfg(test)]
    size_syncs: std::sync::atomic::AtomicUsize,
    /// Test-only: how many open-time fsyncs of an unclean file's existing bytes this open ran.
    #[cfg(test)]
    open_syncs: std::sync::atomic::AtomicUsize,
    /// Test-only: how many times this open dropped the file's cached pages after its fsync (see
    /// [`MmapFileOptions::drop_cache_on_unclean_open`]).
    #[cfg(test)]
    open_evictions: std::sync::atomic::AtomicUsize,
    /// Test-only: the next clean-close seal's final fsync reports a failure after it ran (see
    /// [`Self::fail_next_seal_sync_for_test`]).
    #[cfg(test)]
    fail_seal_sync: bool,
    opts: MmapFileOptions,
}

impl MmapDataFile {
    /// Open with default [`MmapFileOptions`] (append log, normal access hint).
    pub fn open<P: AsRef<Path>>(path: P, read_only: bool) -> io::Result<Self> {
        Self::open_with(path, read_only, MmapFileOptions::default())
    }

    /// Open the mmap-backed file with explicit growth options.
    pub fn open_with<P: AsRef<Path>>(
        path: P,
        read_only: bool,
        opts: MmapFileOptions,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        if !read_only {
            // Create the file if missing. A create that fails (the file exists) is ignored; the
            // open below reports any real error.
            let _ = File::create_new(path);
        }
        let file = OpenOptions::new().read(true).write(!read_only).open(path)?;
        let orig_len = file.metadata()?.len();
        if !read_only && orig_len == 0 {
            // Sync the new entry before anything is written to the file, so data later made
            // durable never sits in a file the directory lost. A 0-length file that already
            // existed is synced as well: it is a retried open whose create may have landed while
            // its sync did not. A failed sync fails the open.
            if let Some(parent) = entry_dir(path) {
                fsync_directory(parent)?;
            }
        }

        // A clean `Drop` appends an 8-byte sentinel at physical EOF (see `clean_close_sentinel`).
        // Detect it here: a valid sentinel means the file was sealed, so the logical data ends 8
        // bytes before physical EOF; a missing/invalid sentinel means the file was not closed
        // cleanly (most likely still padded), which `opened_unclean` surfaces so the pack's heal
        // path runs — the logical end is left at physical EOF for that scan.
        let (logical_end, opened_unclean) = detect_sentinel(&file, orig_len)?;

        // An unclean file's bytes are readable but not necessarily durable: the process that
        // crashed wrote them through its mapping and may never have msynced them, so they can
        // still be dirty pages in the page cache. `flushed_end` below starts the barriers at
        // `logical_end`, and they flush only from there on (a ranged msync does not flush pages
        // outside its range), so fsync the whole file once first; that flushes the inode's dirty
        // pages whichever process dirtied them. A clean close msynced before it sealed, a
        // read-only handle vouches for nothing, and a derived file is rebuilt after any unclean
        // open, so none of those need it.
        //
        // The fsync does not prove the bytes are on disk. Linux reports a writeback error once:
        // if the process that saw it has exited (a failed sync stops the node, and a supervisor
        // restarts it in the same boot), this fsync succeeds while the failed pages sit in the
        // page cache never written, and clean, so no sync writes them again. With
        // `drop_cache_on_unclean_open` (tndb's logs) the open then drops the file's cached pages
        // before anything maps it, so it and its owner's recovery read what the disk holds, and
        // the recovery stops at the hole instead of building on it. Without it the owner reads
        // the page cache (a pack's recovery mostly refuses, rather than cuts, at a hole a later
        // power loss reveals). The kernel keeps any page another process maps; nothing in this
        // crate maps the file at this point (see `drop_cached_pages`).
        #[cfg(test)]
        let (mut open_syncs, mut open_evictions) = (0, 0);
        if !read_only && opened_unclean && !opts.derived {
            fsync_file(&file)?;
            #[cfg(test)]
            {
                open_syncs += 1;
            }
            if opts.drop_cache_on_unclean_open {
                drop_cached_pages(&file)?;
                #[cfg(test)]
                {
                    open_evictions += 1;
                }
            }
        }

        // On an unclean file, recover a durable commit marker from the tail of the mmap capacity if
        // one is present (best-effort — see `stamp_commit_marker`). A clean/fresh file has none (a
        // clean close truncates the padding, and its own trailing bytes are the 8-byte clean-close
        // sentinel, not this 16-byte marker). Absent/torn/stale → `None` → the WAL probe decides.
        // Read after the fsync and any cache drop above: the marker's page may hold a marker
        // only the page cache has.
        let committed_marker = if opened_unclean && orig_len >= COMMIT_MARKER_LEN {
            let mut tail = [0_u8; COMMIT_MARKER_LEN as usize];
            file.read_exact_at(&mut tail, orig_len - COMMIT_MARKER_LEN)?;
            parse_commit_marker(&tail, orig_len)
        } else {
            None
        };

        // A writable reopen of a sealed file retires its clean-close sentinel on disk, durably,
        // before anything is written. The sentinel only vouches for the bytes as they were sealed;
        // once this handle writes (in place for `WriteMode::Random` files, which never overwrite
        // the sentinel themselves), a crash must reopen as unclean so recovery runs. Zeroing it
        // only in the mapping would leave that to page writeback, and a power loss before the
        // sentinel's page reached disk would present torn writes as sealed. The zeros land in the
        // capacity padding past `logical_end`, so a crash right after this leaves exactly the
        // zero padding every unclean-recovery path already expects. One 8-byte write and a data
        // sync per writable reopen of a sealed file (an open, never the persist hot path).
        //
        // With `defer_seal_retire` the retirement waits for the handle's first `write` instead
        // (`retire_seal_if_pending`), so the file stays sealed through whatever its owner does at
        // open (tndb's replay), and through a crash or failure before anything is written.
        let sealed_reopen = !read_only && !opened_unclean && logical_end < orig_len;
        let seal_pending = sealed_reopen && opts.defer_seal_retire;
        if sealed_reopen && !seal_pending {
            file.write_all_at(&[0_u8; SENTINEL_LEN as usize], logical_end)?;
            fdatasync_file(&file)?;
        }

        // Map only the bytes that already exist. A fresh (0-length) RW file is left unallocated
        // until the first write, so a crash before any data keeps it 0-length (and it reopens as
        // empty). Existing content is mapped as-is, including a clean file's 8 trailing bytes in
        // the `[end, capacity)` padding region (the sentinel on a read-only open or while its
        // retirement is deferred; zeros on a writable one, retired above); any trailing padding a
        // crashed writer left is handled by the pack's heal path.
        // A writable handle with a reservation maps it once (even over an empty file) and grows the
        // file underneath; if the address space cannot be reserved, it runs in the classic mode.
        let reserved_map = if !read_only && opts.reserve > 0 {
            let map_len = opts.reserve.max(orig_len);
            // SAFETY: single-writer pack model (as for the classic writable map below). The map may
            // extend past EOF; those pages are never touched (reads are bounded by `end`, writes by
            // `capacity`, the physical size).
            match usize::try_from(map_len)
                .map_err(io::Error::other)
                .and_then(|len| unsafe { MmapOptions::new().len(len).map_mut(&file) })
            {
                Ok(map) => Some((map, map_len)),
                Err(e) => {
                    tracing::warn!(
                        "MmapDataFile: could not reserve {map_len} bytes of address space for \
                         {path:?} ({e}); using the classic remap-on-growth mapping"
                    );
                    None
                }
            }
        } else {
            None
        };
        let (backing, capacity, reserved) = if let Some((map, map_len)) = reserved_map {
            (Backing::Rw(map), orig_len, map_len)
        } else if orig_len == 0 {
            (Backing::Empty, 0, 0)
        } else if read_only {
            // SAFETY: a read-only handle must only ever map a *sealed* file — one that is
            // clean-closed (its `Drop` truncated the mmap capacity padding away and appended the
            // clean-close sentinel, validated above; `opened_unclean` flags a file that was not)
            // and has no live writer. A writer that later shrinks the file under this
            // mapping would make a touch of a page past the new EOF deliver SIGBUS, which no Rust
            // error path catches. `get_static` serves the live epoch from the writer handle (never
            // a read-only map of it), but `open_static` is not gated by `files_consistent` before
            // this map (it checks the files after the open), so a writer still sealing the file
            // can shrink it between the `metadata()` above and this map. Two guards keep reads
            // inside the file: the length check below refuses a mapping shorter than `orig_len`,
            // so at open the mapping covers every offset below `end` (mapping `orig_len`
            // explicitly instead would turn the shrink into SIGBUS); and `set_read_bound` clamps
            // `end` afterward to the index-attested length, so reads never touch bytes a later
            // writer truncation could remove.
            let map = unsafe { Mmap::map(&file)? };
            if (map.len() as u64) < orig_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{} shrank while being opened read-only", path.display()),
                ));
            }
            (Backing::Ro(map), orig_len, 0)
        } else {
            // SAFETY: single-writer pack model — we hold the file open for writing for this
            // handle's lifetime and nothing else writes/truncates it concurrently.
            let map = unsafe { MmapMut::map_mut(&file)? };
            (Backing::Rw(map), orig_len, 0)
        };

        let df = Self {
            file,
            path: path.to_owned(),
            backing,
            end: logical_end,
            capacity,
            seek_pos: 0,
            read_only,
            remove_on_drop: false,
            // The sync fast path starts at the existing content's end: a sealed file was msynced
            // before its seal, and an unclean one fsynced above. That fsync cannot vouch for pages
            // a failed writeback left clean in the page cache but never wrote; with
            // `drop_cache_on_unclean_open` they were dropped and the content is what the disk
            // holds, without it the owner trusts the page cache's view (see above).
            flushed_end: AtomicU64::new(logical_end),
            opened_unclean,
            seal_pending,
            closed: false,
            committed_marker,
            size_unsynced: AtomicBool::new(false),
            growths: 0,
            reserved,
            view: Arc::new(MapView::new()),
            retired: Vec::new(),
            #[cfg(test)]
            size_syncs: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            open_syncs: std::sync::atomic::AtomicUsize::new(open_syncs),
            #[cfg(test)]
            open_evictions: std::sync::atomic::AtomicUsize::new(open_evictions),
            #[cfg(test)]
            fail_seal_sync: false,
            opts,
        };
        df.sync_view();
        df.advise_backing();
        #[cfg(test)]
        df.view.ino.store(
            df.file.metadata().map(|meta| std::os::unix::fs::MetadataExt::ino(&meta)).unwrap_or(0),
            Ordering::Relaxed,
        );
        Ok(df)
    }

    /// Path to this file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes of real data on disk. With no write buffer this equals [`Self::len`].
    pub fn data_file_end(&self) -> u64 {
        self.end
    }

    /// Logical length (bytes of real data) — also the position a new record is appended at.
    pub fn len(&self) -> u64 {
        self.end
    }

    /// True when this file was opened without a valid clean-close sentinel — it was not sealed by a
    /// clean shutdown and is most likely still padded. Callers (e.g. the pack heal path) use this
    /// to decide whether a recovery scan is needed. Always `false` for a freshly created
    /// (0-length) file.
    pub fn opened_unclean(&self) -> bool {
        self.opened_unclean
    }

    /// Clear the "opened unclean" flag after a successful recovery/heal, so a clean `Drop` re-seals
    /// the file with the clean-close sentinel (and a reopen reports it clean).
    ///
    /// Recovery rewinds the log to its last consistent record and rebuilds the derived indexes from
    /// it; once that succeeds the file's content IS consistent, so it should be sealed normally
    /// rather than left to replay on every restart. Because [`Self::opened_unclean`] gates the
    /// clean-close seal in `Drop`, a recovered handle that is never marked would never re-seal.
    ///
    /// Does NOT clear the poison latch ([`Self::poison`]): a durability failure (a failed
    /// `msync`/`fsync`/remap) independently suppresses the seal, so recovery can never paper
    /// over data that did not reach disk. A no-op signal, cheap to call unconditionally on
    /// every recovery success path.
    pub fn mark_consistent(&mut self) {
        self.opened_unclean = false;
    }

    /// Retire a deferred clean-close sentinel (see [`MmapFileOptions::defer_seal_retire`]) before
    /// the first byte is written: zero `[end, end + SENTINEL_LEN)` with a positional write (the
    /// shared mapping sees it) and data-sync it, so from here a crash reopens the file unclean and
    /// recovery runs, exactly as for a sentinel retired at open. A no-op once retired, or when
    /// nothing was deferred. A failure poisons the handle and keeps the seal pending, so `Drop`
    /// leaves the file alone: no data byte was written, and whether the zeros reach the disk
    /// decides only whether the next open replays the data as sealed or as unclean.
    fn retire_seal_if_pending(&mut self) -> io::Result<()> {
        if !self.seal_pending {
            return Ok(());
        }
        self.file
            .write_all_at(&[0_u8; SENTINEL_LEN as usize], self.end)
            .and_then(|()| fdatasync_file(&self.file))
            .inspect_err(|_| self.poison())?;
        self.seal_pending = false;
        Ok(())
    }

    /// Latch the sticky "poison", set on any failed durability barrier (`msync`/`fsync`) or failed
    /// `remap`, and by an owner that finds its file's durability unknown. Once set,
    /// `write`/`ensure_len` and this handle's sync entry points return `Err` without retrying (a
    /// retried `msync` after a writeback error can spuriously succeed on Linux errseq, laundering
    /// lost data), `stamp_commit_marker` is a no-op, and `Drop` skips the clean-close seal so the
    /// next open re-runs recovery instead of trusting a possibly-non-durable tail. The latch lives
    /// in the shared [`MapView`], so a barrier that fails through a view (`MapView::msync`, on
    /// whatever thread) sets it too. Callable behind `&self` so the `&self` sync paths
    /// (`flush_dirty`, `sync_range`, `try_clone`) can set it. Never cleared for the life of the
    /// handle (a durability failure is permanent until reopen); in particular `mark_consistent`
    /// does NOT clear it, so recovery cannot paper over lost data.
    pub(crate) fn poison(&self) {
        self.view.poison();
    }

    /// True once any durability barrier or remap has failed on this file, through this handle or
    /// through its shared [`MapView`] (see [`Self::poison`]).
    pub(crate) fn is_poisoned(&self) -> bool {
        self.view.write_failed.load(Ordering::Relaxed)
    }

    /// True iff the file has a physical size but every logical byte is zero — the signature of a
    /// first write whose `grow_to` sized the file (to `DEFAULT_INITIAL_SIZE`) but whose header
    /// never reached disk before a crash. Such a file is semantically *unwritten*
    /// (the same "all-zero == unwritten" rule the pack applies to records), not corrupt. Only
    /// meaningful on an [`Self::opened_unclean`] file: a clean close always leaves a non-zero
    /// trailing sentinel, so a sealed file is never all-zero.
    ///
    /// The scan is bounded to `opts.initial_size` — a never-written file is exactly that first-grow
    /// size, and growing past it requires writing (a non-zero header first), so a larger all-zero
    /// file is not a first-write artifact ("something else is wrong"). Bounding here both excludes
    /// that case and keeps a pathological large all-zero file from bogging down the open.
    ///
    /// This assumes `opts.initial_size` is stable across opens of the same file. A file grown to a
    /// larger *old* initial size and then reopened with a *smaller* one could be mis-classified as
    /// not-unwritten — but that only downgrades error precision (it still fails safe, as a
    /// corrupt/unwritten open), and in practice every pack type opens with a fixed per-file
    /// `initial_size`.
    pub fn is_unwritten(&self) -> bool {
        self.end != 0
            && self.end <= self.opts.initial_size
            && self.slice(0, self.end as usize).is_some_and(|b| b.iter().all(|&x| x == 0))
    }

    /// The durable acked-data watermark recovered from the tail commit marker of an unclean file,
    /// if a valid one was found at open. `None` on clean/fresh opens or when no valid marker
    /// survived (best-effort). Recovery uses it as an index-free way to detect at-rest
    /// corruption of the last committed record: if a WAL replay stops *below* this offset,
    /// durable data was damaged.
    pub fn committed_end(&self) -> Option<u64> {
        self.committed_marker
    }

    /// Write the commit marker (`committed_end == end`) into the last `COMMIT_MARKER_LEN` bytes
    /// of the mmap capacity (see [`Self::stamp_commit_marker_at`], which also reports whether
    /// it stamped). Best-effort: it dirties one padding page but adds NO sync — the
    /// durability barrier stays the caller's `sync_all` (`msync` of `[flushed_end, end)`),
    /// which does not cover this page, so the marker reaches disk via OS writeback or the next
    /// fsync (a barrier after a growth, or the clean close). That is enough for its purpose
    /// (at-rest rot is detected long after the persist).
    ///
    /// Fail-safe by construction: callers stamp *after* the data `msync`, so the marker can never
    /// be ahead of durable data — a crash leaves it behind or absent, never fabricating a
    /// watermark past real data. A no-op on a read-only handle, an empty file, a sealed file
    /// whose sentinel is still pending (see [`MmapFileOptions::defer_seal_retire`]: it has no
    /// padding, and the sentinel must stay intact), or when the capacity padding cannot hold the
    /// marker without overlapping `[0, end)` (in which case it is simply skipped this time — the
    /// next stamp, after the next append grows capacity, records it).
    pub fn stamp_commit_marker(&mut self) {
        self.stamp_commit_marker_at(self.end);
    }

    /// [`Self::stamp_commit_marker`] for a committed end below the current one: the end a sync
    /// made outside this file covered while appends continued past it (see [`Self::note_synced`]).
    /// Ignored past the synced watermark, so it never claims bytes not yet durable. Returns
    /// whether the marker was stamped, so a caller tracking what the marker covers records only
    /// a stamp that landed.
    pub(crate) fn stamp_commit_marker_at(&mut self, committed: u64) -> bool {
        if self.read_only
            || self.seal_pending
            || committed == 0
            || committed > self.end
            || committed > self.flushed_end.load(Ordering::Relaxed)
            || self.is_poisoned()
        {
            return false;
        }
        // Need `end + COMMIT_MARKER_LEN <= capacity` so the marker sits in the padding past the
        // data.
        let Some(marker_pos) = self.capacity.checked_sub(COMMIT_MARKER_LEN) else {
            return false;
        };
        if marker_pos < self.end {
            // No headroom this persist; skip (fail-safe — falls back to the WAL probe).
            return false;
        }
        let Backing::Rw(map) = &mut self.backing else {
            return false;
        };
        let marker = commit_marker(committed);
        let pos = marker_pos as usize;
        map_range_mut(map, pos, COMMIT_MARKER_LEN as usize).copy_from_slice(&marker);
        true
    }

    /// Clamp a read-only handle's read bound down to `logical_end` (the caller's index-attested
    /// record end). `slice`/`read`/`len` are all bounded by `end`, so after this no read touches a
    /// byte above `logical_end` — the region a writer truncation would remove — even if this handle
    /// mapped a file that was physically padded past its logical data. Never grows `end` (a
    /// read-only handle cannot have more logical data than it opened with) and does not re-map: the
    /// mapping may still span padding, but those pages are never accessed. No-op on a writable
    /// handle.
    pub fn set_read_bound(&mut self, logical_end: u64) {
        if self.read_only {
            self.end = self.end.min(logical_end);
            if self.seek_pos > self.end {
                self.seek_pos = self.end;
            }
        }
    }

    /// Is the logical file empty?
    pub fn is_empty(&self) -> bool {
        self.end == 0
    }

    /// Borrow the mapped bytes `[offset, offset + len)` directly, without copying — the building
    /// block for zero-copy record reads (no `memcpy`, no allocation).
    ///
    /// Returns `None` if the range falls outside the logical data `[0, len())` (so the transient
    /// capacity padding past `end` is never exposed), the file is currently unmapped, or the
    /// mapping does not cover the range (a file that shrank under a read-only open). The
    /// returned slice borrows `&self`, so no concurrent write/remap (which needs `&mut self`)
    /// can invalidate it while it is held. A caller that does not know a record's length up
    /// front takes [`Self::tail`] and parses the prefix from it.
    pub fn slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let range_end = offset.checked_add(len as u64)?;
        if range_end > self.end {
            return None;
        }
        let start = offset as usize;
        match &self.backing {
            Backing::Rw(map) => map.get(start..start + len),
            Backing::Ro(map) => map.get(start..start + len),
            // Only a zero-length borrow (necessarily at offset 0) is valid with no mapping.
            Backing::Empty if len == 0 => Some(&[]),
            Backing::Empty => None,
        }
    }

    /// Borrow the logical data from `offset` to the end (`None` past the end, or where the
    /// mapping does not reach the end): see [`Self::slice`].
    pub fn tail(&self, offset: u64) -> Option<&[u8]> {
        self.slice(offset, usize::try_from(self.end.checked_sub(offset)?).ok()?)
    }

    /// Borrow the mapped bytes `[offset, offset + len)` directly, without copying — the building
    /// block for zero-copy record reads (no `memcpy`, no allocation).
    ///
    /// Returns `None` if the range falls outside the logical data `[0, len())` (so the transient
    /// capacity padding past `end` is never exposed), the file is currently unmapped, or the
    /// mapping does not cover the range. The returned slice borrows `&mut self`, so no
    /// concurrent write/remap (which needs `&mut self`) can invalidate it while it is held.
    pub fn slice_mut(&mut self, offset: u64, len: usize) -> Option<&mut [u8]> {
        debug_assert!(!self.seal_pending, "a sealed file changes only through `write`");
        let range_end = offset.checked_add(len as u64)?;
        if range_end > self.end {
            return None;
        }
        let start = offset as usize;
        match &mut self.backing {
            Backing::Rw(map) => {
                // Bounded here, not with `get_mut` (which would borrow the whole mapping; see
                // `map_range_mut`), so a short mapping is `None` rather than its assert.
                if start + len > map.len() {
                    return None;
                }
                Some(map_range_mut(map, start, len))
            }
            Backing::Ro(_map) => None,
            Backing::Empty => None,
        }
    }

    /// Next capacity that holds `needed` bytes: double until a step would exceed `max_map_size`,
    /// then advance in `max_map_size` increments. Every allocation is floored at `initial_size`, so
    /// a small reopened file (its capacity is the reopened physical size, e.g. a 36-byte
    /// header-only pack) jumps straight to `initial_size` instead of paying an
    /// ftruncate+mmap (and a barrier fsync) per doubling back up from that tiny capacity.
    fn next_capacity(&self, needed: u64) -> u64 {
        let max_step = self.opts.max_map_size.max(1);
        let mut cap = self.capacity.max(self.opts.initial_size).max(1);
        while cap < needed {
            // Grow by min(cap, max_step): geometric early, linear once a step hits the cap.
            cap = cap.saturating_mul(2).min(cap.saturating_add(max_step));
        }
        cap
    }

    /// Apply the configured [`MmapAccess`] `madvise` hint to the current mapping. Best-effort: a
    /// failed hint is logged and ignored, and it is a no-op for `Normal` or an empty mapping.
    fn advise_backing(&self) {
        let advice = match self.opts.access {
            MmapAccess::Normal => return,
            MmapAccess::Sequential => memmap2::Advice::Sequential,
            MmapAccess::Random => memmap2::Advice::Random,
        };
        let res = match &self.backing {
            Backing::Rw(map) => map.advise(advice),
            Backing::Ro(map) => map.advise(advice),
            Backing::Empty => return,
        };
        if let Err(e) = res {
            tracing::trace!("MmapDataFile: madvise failed (non-fatal): {e}");
        }
    }

    /// Point [`Self::view`] at the current mapping (or at nothing).
    fn sync_view(&self) {
        let (base, len) = match &self.backing {
            Backing::Rw(map) => (map.as_ptr(), map.len() as u64),
            Backing::Ro(map) => (map.as_ptr(), map.len() as u64),
            Backing::Empty => (std::ptr::null(), 0),
        };
        self.view.set_mapping(base, len);
        self.view.physical.store(self.capacity, Ordering::Release);
    }

    /// Unmap this file's mappings, current and retired, or, while a reader may still hold its view
    /// (and so a slice borrowed through it), hand them to the view, which unmaps them when the last
    /// reader drops it. No reader can obtain the view once its count is 1: a new one comes from
    /// [`Self::view`], which this `&mut self` excludes.
    fn release_mappings(&mut self) {
        let mut maps = std::mem::take(&mut self.retired);
        maps.push(std::mem::replace(&mut self.backing, Backing::Empty));
        maps.retain(|map| !matches!(map, Backing::Empty));
        if !maps.is_empty() && Arc::strong_count(&self.view) > 1 {
            self.view.kept.lock().unwrap_or_else(std::sync::PoisonError::into_inner).extend(maps);
        }
    }

    /// Take the current mapping out of `backing`, retiring it (kept mapped until drop) when a
    /// lock-free reader may still hold its base, else letting it unmap.
    fn take_backing(&mut self) {
        let old = std::mem::replace(&mut self.backing, Backing::Empty);
        if Arc::strong_count(&self.view) > 1 && !matches!(old, Backing::Empty) {
            // A reader may hold the old base: keep it mapped (and the view on it) until the
            // replacement is installed.
            self.retired.push(old);
        } else {
            // Unmapped as `old` drops: the view must not keep its base.
            self.view.set_mapping(std::ptr::null(), 0);
        }
    }

    /// The lock-free reader view of this file's mapping (see [`MapView`]).
    pub(crate) fn view(&self) -> Arc<MapView> {
        Arc::clone(&self.view)
    }

    /// Drop the current mapping, resize the physical file to `new_len`, and re-map it (leaving it
    /// unmapped when `new_len == 0`). In the classic mode the new mapping is exactly the file; a
    /// reserving handle (here only because a growth outran its reservation) maps a new, larger
    /// reservation instead — the one case in which its mapping moves.
    fn remap(&mut self, new_len: u64) -> io::Result<()> {
        // Release (or retire, if a lock-free reader may hold it) the existing map before resizing.
        self.take_backing();
        // Keep `capacity` consistent with `backing`: with no live mapping, a `set_len`/`map_mut`
        // failure below must leave `capacity == 0` rather than a stale value that lies about the
        // mapping size. Restored to `new_len` only once the new map is installed.
        self.capacity = 0;
        // Readers clamp to the physical size: lower it before the file shrinks.
        self.view.physical.fetch_min(new_len, Ordering::AcqRel);
        // A failed resize/remap leaves the handle with no live mapping and unknown durability for
        // any tail dirtied through the released map; latch the poison so later writes/syncs fail
        // and `Drop` does not seal a file whose mapping was lost mid-flight.
        self.file.set_len(new_len).inspect_err(|_| self.poison())?;
        if new_len == 0 {
            // Nothing is mapped now, and the file is empty: a retired map's pages are gone too.
            self.sync_view();
            return Ok(());
        }
        let reserved_map = if self.reserved > 0 {
            let map_len = new_len.max(self.reserved.saturating_mul(2));
            // SAFETY: single-writer model; the file was sized to `new_len` immediately above, and
            // the pages of the reservation past it are never touched.
            match usize::try_from(map_len)
                .map_err(io::Error::other)
                .and_then(|len| unsafe { MmapOptions::new().len(len).map_mut(&self.file) })
            {
                Ok(map) => Some((map, map_len)),
                Err(e) => {
                    // As at open: without the address space, fall back to the classic mapping.
                    tracing::warn!(
                        "MmapDataFile: could not reserve {map_len} bytes of address space for {:?} \
                         ({e}); using the classic remap-on-growth mapping",
                        self.path
                    );
                    self.reserved = 0;
                    None
                }
            }
        } else {
            None
        };
        let map = if let Some((map, map_len)) = reserved_map {
            self.reserved = map_len;
            map
        } else {
            // SAFETY: single-writer model; the file was sized to `new_len` immediately above.
            unsafe { MmapMut::map_mut(&self.file) }.inspect_err(|_| self.poison())?
        };
        self.backing = Backing::Rw(map);
        self.capacity = new_len;
        self.sync_view();
        self.advise_backing();
        Ok(())
    }

    /// Reserve real disk blocks for `[from, to)` and grow the file to `to`, so a later `memcpy`
    /// store into that range can never SIGBUS on a full filesystem — an out-of-space condition
    /// surfaces here as an `io::Error` (which the pack turns into a failed state and a clean
    /// shutdown) instead. A bare `set_len`/ftruncate only moves EOF and leaves the new bytes as
    /// sparse holes whose first write-fault allocates a block and, when the disk is full,
    /// delivers `VM_FAULT_SIGBUS` with no Rust error path.
    ///
    /// The reserved padding is transient: the clean-close `Drop` truncates back to `end`, so the
    /// real disk cost (≤ one growth step past the data) is only paid while the file is open.
    /// Where the platform/filesystem cannot preallocate, it falls back to a plain resize (the
    /// old sparse behaviour) so the file still functions, and warns once per process (Linux)
    /// that a full disk may then surface as SIGBUS.
    #[cfg(target_os = "linux")]
    fn allocate_range(&self, from: u64, to: u64) -> io::Result<()> {
        let len = to.saturating_sub(from);
        if len == 0 {
            return Ok(());
        }
        // fallocate(2) mode 0: allocate blocks for [from, from+len) and extend the size to cover
        // it.
        // SAFETY: a plain syscall on a file descriptor this handle owns and keeps open for the
        // call; it touches no Rust memory, and the offsets are range-checked by the kernel.
        let rc = unsafe {
            libc::fallocate(self.file.as_raw_fd(), 0, from as libc::off_t, len as libc::off_t)
        };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        // Some filesystems (ext2/ext3, NFS before 4.2, FUSE without fallocate, older ZFS) don't
        // implement it; fall back to a plain resize (sparse) rather than failing an
        // otherwise-serviceable write, but say once per process what that costs.
        if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)) {
            static WARNED: AtomicBool = AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    file = %self.path.display(),
                    error = %err,
                    "this filesystem cannot preallocate (fallocate); storage files grow sparse, so \
                     a full disk may stop the node with SIGBUS instead of a write error. ext4 and \
                     xfs are the supported filesystems (logged once per process)"
                );
            }
            return self.file.set_len(to);
        }
        Err(err) // ENOSPC / EDQUOT / EFBIG / EIO propagate as the write's io::Error
    }

    #[cfg(target_os = "macos")]
    fn allocate_range(&self, from: u64, to: u64) -> io::Result<()> {
        let len = to.saturating_sub(from);
        if len == 0 {
            return Ok(());
        }
        // F_PREALLOCATE reserves blocks from the physical EOF (== `from` while open) but does NOT
        // change the file size, so a `set_len` still follows to grow the logical size. Try a
        // contiguous reservation first, then allow a fragmented one. Both requests carry
        // `F_ALLOCATEALL` (all or nothing): without it the call may succeed having reserved only
        // part of the range, and `set_len` would then extend over unreserved blocks — the SIGBUS
        // this preallocation exists to prevent.
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATECONTIG | libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as libc::off_t,
            fst_bytesalloc: 0,
        };
        let fd = self.file.as_raw_fd();
        // SAFETY: `fd` belongs to a file this handle owns and keeps open for the call, and `store`
        // is a live, properly initialized `fstore_t` that `F_PREALLOCATE` reads and updates
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
                return self.file.set_len(to);
            }
            return Err(err);
        }
        // Belt and braces: never extend past what was actually reserved.
        if (store.fst_bytesalloc as u64) < len {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        self.file.set_len(to)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn allocate_range(&self, _from: u64, to: u64) -> io::Result<()> {
        // No portable preallocation on this target; keep the previous (sparse) resize behaviour.
        self.file.set_len(to)
    }

    /// Grow the file to `new_cap`, preallocating the new range. Preallocation
    /// ([`Self::allocate_range`]) reserves real blocks so a full disk fails here with an
    /// `io::Error` rather than a later SIGBUS on the first store into an unbacked page.
    ///
    /// The size extension is not fsync'd here: `msync` does not persist size growth, so the next
    /// durability barrier fsyncs it once ([`Self::sync_size_if_grown`]) before returning. Nothing
    /// in the grown region is durable, or acked, before that barrier, so a crash in between loses
    /// only unacked bytes (and a lost extension reads as unwritten, like any torn tail).
    fn grow_to(&mut self, new_cap: u64) -> io::Result<()> {
        // `capacity` is the current physical size (== physical EOF); reserve the new range from it.
        let from = self.capacity;
        self.allocate_range(from, new_cap).inspect_err(|_| self.poison())?;
        if new_cap <= self.reserved && matches!(self.backing, Backing::Rw(_)) {
            // The reserved mapping already covers the grown file (`allocate_range` extended it to
            // `new_cap`): no remap, so the mapping does not move.
            self.capacity = new_cap;
            self.view.physical.store(new_cap, Ordering::Release);
        } else {
            self.remap(new_cap)?;
        }
        self.size_unsynced.store(true, Ordering::Relaxed);
        self.growths += 1;
        Ok(())
    }

    /// The fsync a growth deferred: if the file grew since the last fsync, persist its size (and,
    /// with it, everything written so far) before a durability barrier returns. Barriers call this
    /// after their `msync`, so a barrier promises what it always has (data `[0, end)` durable
    /// within a durable file size) while one with no growth since the last stays `msync`-only.
    /// A [`derived`](MmapFileOptions::derived) file skips it and leaves its size to the seal.
    fn sync_size_if_grown(&self) -> io::Result<()> {
        if self.size_unsynced.load(Ordering::Relaxed) && !self.opts.derived {
            fsync_file(&self.file).inspect_err(|_| self.poison())?;
            self.size_unsynced.store(false, Ordering::Relaxed);
            #[cfg(test)]
            self.size_syncs.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Ensure a writable mapping large enough for `[0, needed)`, growing (reopen-larger) if needed.
    fn ensure_capacity(&mut self, needed: u64) -> io::Result<()> {
        if needed <= self.capacity {
            return Ok(());
        }
        // Size the regrow from `max(needed, end)`, never `needed` alone: after a failed `remap`
        // reset `capacity` to 0 while `end` stayed put, a regrow sized from a small
        // `needed` would `set_len` below `end` and leave `end > map.len()`, so a later
        // `slice`/`rewind_to` would index the map out of bounds.
        let new_cap = self.next_capacity(needed.max(self.end));
        if self.opts.grow_mode == GrowMode::Segment && new_cap > self.opts.max_map_size {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "segment mode not yet implemented: mapping reached max_map_size",
            ));
        }
        self.grow_to(new_cap)
    }

    /// Ensure the logical length is at least `new_len`, extending the mapping (growing capacity
    /// geometrically if needed) so `[end, new_len)` becomes addressable for `slice`/`slice_mut`.
    /// The extended region reads as zero: a fresh grow is zero-filled (preallocation /
    /// ftruncate-extend), and the one historical exception — the previous clean-close sentinel
    /// sitting in the `[end, end + SENTINEL_LEN)` padding after a clean reopen — is now zeroed
    /// at open (see `open_with`), or with [`MmapFileOptions::defer_seal_retire`] by the first
    /// `write`, before which this must not be called. Never shrinks. Unlike [`Self::truncate`],
    /// growth is geometric (a remap only when a step crosses the current capacity), so repeated
    /// one-record extensions (e.g. the digest index adding a bucket per split) do not remap
    /// every call.
    pub fn ensure_len(&mut self, new_len: u64) -> io::Result<()> {
        if new_len <= self.end {
            return Ok(());
        }
        // The region it extends into would hold a pending sentinel, not zeros.
        debug_assert!(!self.seal_pending, "a sealed file changes only through `write`");
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                "file not open for write",
            ));
        }
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing to grow",
            ));
        }
        self.ensure_capacity(new_len)?;
        self.end = new_len;
        Ok(())
    }

    /// Truncate the logical (and physical) file to `len`, leaving it exactly `len` bytes. This is a
    /// SHRINK-only operation (the pack heal path: position-index alignment heal, torn-tail
    /// truncate, `truncate(0)` reset), so `len` must be `<= capacity`. To GROW, use
    /// [`Self::ensure_len`] / [`Self::ensure_capacity`], which preallocate through `grow_to`;
    /// extending here via the bare `remap` below would leave a sparse (unbacked) region that
    /// SIGBUSes on the first store when the disk is full — the very footgun the preallocation
    /// path exists to close. For a cheap LOGICAL shrink that keeps the physical file (no
    /// ftruncate/remap), use [`Self::rewind_to`] instead. Errs, changing nothing, for a `len`
    /// below the length published to the file's [`MapView`] readers.
    pub fn truncate(&mut self, len: u64) -> io::Result<()> {
        debug_assert!(!self.seal_pending, "a sealed file changes only through `write`");
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                "file not open for write",
            ));
        }
        // Match every other write/grow/sync entry point: refuse once poisoned so a prior durability
        // failure is never papered over by a later truncate.
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing to truncate",
            ));
        }
        if len > self.capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "truncate is shrink-only (len {len} > capacity {}); use ensure_len to grow",
                    self.capacity
                ),
            ));
        }
        let published = self.view.published.load(Ordering::Acquire);
        if len < published {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "truncate to {len} would cut bytes published to readers (up to {published})"
                ),
            ));
        }
        if self.reserved > 0 && matches!(self.backing, Backing::Rw(_)) {
            // Shrink the file under the reserved mapping, which stays put; the pages past the new
            // EOF are never touched (and read back as zeros if a later growth re-extends the file).
            self.view.physical.fetch_min(len, Ordering::AcqRel);
            self.file.set_len(len).inspect_err(|_| self.poison())?;
            self.capacity = len;
        } else {
            self.remap(len)?;
        }
        self.end = len;
        // Bytes past `len` are gone; clamp the watermark so a later append below the old high-water
        // is still flushed (bytes still present under `len` stay durable).
        let fe = self.flushed_end.load(Ordering::Relaxed).min(len);
        self.flushed_end.store(fe, Ordering::Relaxed);
        if self.seek_pos > len {
            self.seek_pos = len;
        }
        Ok(())
    }

    /// Roll the logical end back to `new_len`, zeroing the abandoned region `[new_len, end)` in the
    /// mapping, WITHOUT physically truncating or re-`mmap`ping the file.
    ///
    /// Unlike [`Self::truncate`] this keeps the current capacity (no `ftruncate`, no remap), so it
    /// opens no read-only-mmap SIGBUS window and is cheap. The abandoned bytes become ordinary
    /// capacity padding: every read/slice/iterator is bounded to `end`, a clean close truncates the
    /// padding away, and recovery bounds it out via the index-attested length. Zeroing keeps the
    /// "capacity padding reads as zeros" invariant intact in memory (the zeros sit past `end`, so
    /// they are not force-flushed — recovery correctness does not depend on them).
    ///
    /// Used to atomically undo a partial append (see the consensus pack's save rollback). `new_len`
    /// must be `<= end`; a value at or beyond `end` is ignored (use [`Self::ensure_len`] to grow).
    /// No-op on a read-only handle. Returns the span it zeroed (the chunks that held non-zero
    /// bytes), if any, for a caller that must flush the zeros or report what it cut. A `new_len`
    /// below the length published to the file's [`MapView`] readers is refused: nothing is cut,
    /// and the file is poisoned (see [`Self::poison`]), so every later write and sync fails.
    pub fn rewind_to(&mut self, new_len: u64) -> Option<std::ops::Range<u64>> {
        if self.read_only || new_len >= self.end {
            return None;
        }
        debug_assert!(!self.seal_pending, "a sealed file changes only through `write`");
        let published = self.view.published.load(Ordering::Acquire);
        if new_len < published {
            // Zeroing bytes a reader may be borrowing is a data race: refuse, leave every byte in
            // place, and poison the file, so it accepts no later write or sync and is not sealed
            // (the next open recovers it).
            tracing::error!(
                "MmapDataFile: refusing to rewind {:?} to {new_len}, below the {published} bytes \
                 published to readers; the file is poisoned",
                self.path
            );
            self.poison();
            return None;
        }
        let mut zeroed: Option<std::ops::Range<u64>> = None;
        if let Backing::Rw(map) = &mut self.backing {
            // Zero only the chunks that actually hold non-zero bytes (the torn tail). `[new_len,
            // end)` on an unclean open is mostly sparse capacity padding (holes that
            // already read as zero); reading a hole maps the shared zero page and
            // allocates nothing, whereas `fill(0)` over the whole range would
            // write-fault and allocate every hole page (up to a 128 MiB growth
            // step) — dirtying the page cache and allocating disk blocks at writeback only to
            // truncate them at clean close, and SIGBUS-ing on a nearly-full disk.
            // Skipping already-zero chunks keeps the "padding reads as zeros" invariant
            // at a fraction of the cost.
            let tail = (self.end - new_len) as usize;
            let mut at = new_len;
            for chunk in map_range_mut(map, new_len as usize, tail).chunks_mut(64 << 10) {
                let next = at + chunk.len() as u64;
                if chunk.iter().any(|&b| b != 0) {
                    chunk.fill(0);
                    let start = zeroed.as_ref().map_or(at, |span| span.start);
                    zeroed = Some(start..next);
                }
                at = next;
            }
        }
        self.end = new_len;
        // The tail is gone; clamp the append watermark so a later write is flushed, and pull a
        // past-end read cursor back (mirrors `truncate`).
        let fe = self.flushed_end.load(Ordering::Relaxed).min(new_len);
        self.flushed_end.store(fe, Ordering::Relaxed);
        if self.seek_pos > new_len {
            self.seek_pos = new_len;
        }
        zeroed
    }

    /// Clone the underlying file handle, `msync`ing its dirty range first (`[flushed_end, end)` for
    /// an append file, `[0, end)` for a random-write one). That `flush_range` is a durability
    /// barrier (`MS_SYNC`), not merely a visibility hint: it writes the mapped region back to the
    /// file so the cloned fd reads committed bytes even on platforms where mmap stores and plain
    /// `read()` are not guaranteed coherent, and so a consumer copying through the clone gets
    /// durable data; it also advances `flushed_end`. Returns the clone together with the logical
    /// `end` at the moment of the call.
    ///
    /// Unlike a clean close, this does NOT truncate the capacity padding: the physical file may be
    /// larger than `end` (and a later append re-grows and re-pads it further), so the returned
    /// `end` is the ONLY reliable data boundary. A consumer that reads to physical EOF would run
    /// into the padding; readers must stop at `end` instead
    /// ([`PackIter`](crate::archive::pack_iter)
    /// via [`raw_iter`](crate::archive::pack::Pack::raw_iter) does). An external byte consumer that
    /// cannot be told a length (a raw `std::fs::copy`, a read-to-EOF network stream) must bound its
    /// own read to `end`; the physical padding is only ever removed on a clean close (`Drop`).
    ///
    /// The clone is a [`DataFileReader`] with a cursor of its own, so any number of clones can be
    /// read at once without disturbing each other.
    pub fn try_clone(&self) -> io::Result<(DataFileReader, u64)> {
        // A prior write/sync/remap failure means the mapped tail is of unknown durability; refuse
        // to hand out a clone (an iterator/export reader) that would read
        // possibly-non-durable bytes.
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing to clone",
            ));
        }
        if !self.read_only && self.end > 0 {
            if let Backing::Rw(map) = &self.backing {
                // msync the dirty range back to the file: a durability barrier, and the coherence
                // guarantee for the clone's plain `read()`/copy syscalls. As in `flush_dirty`, an
                // append-mode file is already durable below `flushed_end`, so only `[flushed_end,
                // end)` needs it (nothing when clean); a random-write file flushes `[0, end)`.
                // Advance the watermark only when the msync actually ran -- if a prior `remap`
                // failed and left no live mapping, the tail is NOT durable and the watermark must
                // not claim otherwise. A failed barrier poisons the handle.
                let start = match self.opts.write_mode {
                    WriteMode::Append => self.flushed_end.load(Ordering::Relaxed).min(self.end),
                    WriteMode::Random => 0,
                };
                if start < self.end {
                    map.flush_range(start as usize, (self.end - start) as usize)
                        .inspect_err(|_| self.poison())?;
                    #[cfg(test)]
                    disk_model::flushed_range(
                        disk_model::ino_if_tracking(&self.file),
                        map,
                        start,
                        self.end,
                    );
                    self.flushed_end.store(self.end, Ordering::Relaxed);
                }
                self.sync_size_if_grown()?;
            }
        }
        Ok((DataFileReader { file: self.file.try_clone()?, pos: 0 }, self.end))
    }

    /// `msync` the dirty region to the backing store. For [`WriteMode::Append`] this is only the
    /// tail written since the last durable flush (`[flushed_end, end)`); for [`WriteMode::Random`]
    /// it is the whole `[0, end)` range, since an in-place overwrite can land anywhere. `sync`
    /// picks a durable `MS_SYNC` (which then advances the append watermark) over a fire-and-forget
    /// `MS_ASYNC`.
    fn flush_dirty(&self, sync: bool) -> io::Result<()> {
        if self.read_only {
            return Ok(());
        }
        #[cfg(test)]
        if sync {
            SYNC_BARRIERS.with(|n| n.set(n.get() + 1));
        }
        // A prior failure poisoned this handle: return `Err` without retrying. A retried `msync`
        // after a writeback error can spuriously return success on Linux (errseq) while the dirty
        // pages were already dropped, so a retry would launder lost data and could advance
        // `flushed_end` over a tail that never reached disk. Checked before any early return so a
        // poisoned handle never reports a successful sync.
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing to sync",
            ));
        }
        if self.end == 0 {
            return Ok(());
        }
        let Backing::Rw(map) = &self.backing else {
            // No live mapping -- a prior `remap` failed and released it. NOTE: `remap` also
            // *poisons* on failure (see `remap`), so the `is_poisoned()` check above normally
            // returns `Err` before control reaches here — this branch is effectively unreachable
            // while that invariant holds. It is retained as defense-in-depth (still correct if the
            // poison-on-remap-failure guarantee ever regresses): any tail dirtied through the
            // released mmap still sits in the page cache; only an fsync can push it out now, and
            // only a durable flush may advance the append watermark. A silent `Ok(())`
            // here would let the clean-close sentinel be stamped over a
            // possibly-non-durable tail.
            if sync && self.flushed_end.load(Ordering::Relaxed) < self.end {
                fsync_file(&self.file).inspect_err(|_| self.poison())?;
                self.flushed_end.store(self.end, Ordering::Relaxed);
                // That fsync persisted the size too.
                self.size_unsynced.store(false, Ordering::Relaxed);
            } else if sync {
                self.sync_size_if_grown()?;
            }
            return Ok(());
        };
        let start = match self.opts.write_mode {
            WriteMode::Append => self.flushed_end.load(Ordering::Relaxed).min(self.end),
            WriteMode::Random => 0,
        };
        if start >= self.end {
            // No new data, but a growth (e.g. `ensure_len`) may still owe its size fsync.
            if sync {
                self.sync_size_if_grown()?;
            }
            return Ok(());
        }
        let len = (self.end - start) as usize;
        if sync {
            map.flush_range(start as usize, len).inspect_err(|_| self.poison())?;
            #[cfg(test)]
            disk_model::flushed_range(
                disk_model::ino_if_tracking(&self.file),
                map,
                start,
                self.end,
            );
            // The tail is now durable; the next append-mode flush can start from here.
            self.flushed_end.store(self.end, Ordering::Relaxed);
            // And within a durable file size: a growth since the last fsync is synced now.
            self.sync_size_if_grown()?;
        } else {
            map.flush_async_range(start as usize, len).inspect_err(|_| self.poison())?;
        }
        Ok(())
    }

    /// Test-only: the end of the bytes synced so far (the synced watermark).
    #[cfg(test)]
    pub(crate) fn synced_end(&self) -> u64 {
        self.flushed_end.load(Ordering::Relaxed)
    }

    /// How many times the file has grown (each growth leaves its new size to be fsynced).
    pub(crate) fn growths(&self) -> u64 {
        self.growths
    }

    /// Record that `[0, end)` was made durable by a sync this file did not run itself (an `msync`
    /// through its [`MapView`] and an `fsync` of a duplicate descriptor): its watermark moves to
    /// `end` (never past the current end, nor back), so a commit marker can be stamped up to it.
    pub(crate) fn note_synced(&self, end: u64) {
        self.flushed_end.fetch_max(end.min(self.end), Ordering::Relaxed);
    }

    /// A duplicate of the file's descriptor, for an `fsync` from another thread.
    pub(crate) fn dup_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    /// Default durability barrier: `msync` the region written since the last sync. Cheaper than
    /// `fsync` and sufficient for data within the already-fsync'd file size; after a growth it also
    /// fsyncs once, to persist the size extension (see [`Self::sync_size_if_grown`]).
    pub fn sync_all(&self) -> io::Result<()> {
        self.flush_dirty(true)
    }

    /// `msync` an explicit byte range (clamped to `[0, end)`). Used to *sequence* a durability
    /// barrier: flush the bulk of the file with [`Self::sync_all`], then flush a small commit
    /// region (e.g. the header page) with this so it lands durably last. No-op when read-only,
    /// empty, or unmapped.
    pub fn sync_range(&self, start: u64, len: u64) -> io::Result<()> {
        if self.read_only {
            return Ok(());
        }
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing to sync",
            ));
        }
        if self.end == 0 || len == 0 {
            return Ok(());
        }
        let Backing::Rw(map) = &self.backing else {
            return Ok(());
        };
        let start = start.min(self.end);
        let end = start.saturating_add(len).min(self.end);
        if start >= end {
            return Ok(());
        }
        map.flush_range(start as usize, (end - start) as usize).inspect_err(|_| self.poison())?;
        #[cfg(test)]
        disk_model::flushed_range(disk_model::ino_if_tracking(&self.file), map, start, end);
        Ok(())
    }

    /// Full, slow disk sync: `msync` then `fsync`, additionally persisting the file's
    /// size/metadata. See the module docs for the macOS `F_FULLFSYNC` caveat.
    pub fn sync_disk(&self) -> io::Result<()> {
        if self.read_only {
            return Ok(());
        }
        self.flush_dirty(true)?;
        fsync_file(&self.file).inspect_err(|_| self.poison())?;
        self.size_unsynced.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Refresh the logical end for a read-only handle so it observes a writer's appends, re-mapping
    /// if the file grew. Intended for following a cleanly-closed writer; a writer that is
    /// mid-append may have the file padded beyond its logical data, so pair with index-bounded
    /// reads.
    ///
    /// This re-adopts the current physical length and recomputes `opened_unclean`, so it **clears
    /// any prior [`Self::set_read_bound`] clamp**: if a caller had clamped this handle below a
    /// writer's padding and then refreshes against a still-mid-append (unsentineled) writer, `end`
    /// returns to the padded physical size and the caller must re-clamp before reading. (No
    /// production path currently refreshes a clamped handle — the only caller is a test — so this
    /// is a documented precondition, not a live hazard.)
    pub fn refresh_data_file_end(&mut self) -> io::Result<()> {
        let disk_len = self.file.metadata()?.len();
        // A cleanly-closed writer leaves a sentinel past its logical data; strip it so `end` is the
        // logical length (physical == logical + sentinel). A mid-append writer has no valid
        // sentinel and `end` stays at the padded physical size — pair with
        // `set_read_bound`.
        let (logical_end, opened_unclean) = detect_sentinel(&self.file, disk_len)?;
        if disk_len != self.capacity
            && !self.read_only
            && disk_len <= self.reserved
            && matches!(self.backing, Backing::Rw(_))
        {
            // The reserved mapping already covers the file at its new size: keep it in place.
            self.capacity = disk_len;
        } else if disk_len != self.capacity {
            self.take_backing();
            if disk_len > 0 {
                self.backing = if self.read_only {
                    // SAFETY: read-only refresh is only sound while the underlying file is sealed
                    // (a cleanly-closed writer, physical == logical + sentinel,
                    // no live writer). Following a mid-append writer would
                    // adopt its padded physical size and SIGBUS on a later
                    // truncation; pair with `set_read_bound`. A mapping shorter than `disk_len`
                    // is refused, as in the read-only branch of `open_with`.
                    let map = unsafe { Mmap::map(&self.file)? };
                    if (map.len() as u64) < disk_len {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "{} shrank while being refreshed read-only",
                                self.path.display()
                            ),
                        ));
                    }
                    Backing::Ro(map)
                } else {
                    // SAFETY: single-writer model — this handle is the sole writer for its
                    // lifetime.
                    Backing::Rw(unsafe { MmapMut::map_mut(&self.file)? })
                };
            }
            self.capacity = disk_len;
            self.sync_view();
            self.advise_backing();
        }
        self.end = logical_end;
        self.opened_unclean = opened_unclean;
        Ok(())
    }

    /// Test-only: release the mapping as a failed `remap` would, leaving `end` unmapped.
    #[cfg(test)]
    pub(crate) fn drop_mapping_for_test(&mut self) {
        self.view.set_mapping(std::ptr::null(), 0);
        self.backing = Backing::Empty;
    }

    /// Mark the file to be removed when this handle drops (instead of the flush+sync clean close).
    pub fn delete(mut self) {
        self.remove_on_drop = true;
    }

    /// True once the file is to be removed when this handle drops (see
    /// [`Self::set_remove_on_drop`]).
    pub(crate) fn removes_on_drop(&self) -> bool {
        self.remove_on_drop
    }

    /// Like [`Self::delete`] but keeps the handle: the file is removed (not sealed) when this
    /// handle eventually drops. Used to abandon a partial/failed build cheaply — its `Drop`
    /// skips the whole msync + truncate + sentinel + fsync clean-close.
    pub fn set_remove_on_drop(&mut self) {
        self.remove_on_drop = true;
    }

    /// Rename the underlying file, fsync'ing affected directories so the rename is durable. The
    /// active mapping is backed by the open handle, not the path, so it stays valid across the
    /// move.
    pub fn rename<P: AsRef<Path>>(&mut self, path: P) -> Result<(), RenameError> {
        let path = path.as_ref();
        if self.path == path {
            return Ok(());
        }
        if path.exists() {
            return Err(RenameError::FilesExist);
        }
        let old_parent = self.path.parent().map(Path::to_owned);
        let res = std::fs::rename(&self.path, path);
        if res.is_ok() {
            self.path = path.to_owned();
            if let Some(new_parent) = path.parent() {
                fsync_directory(new_parent).map_err(RenameError::RenameIO)?;
            }
            if let Some(old_parent) = &old_parent {
                if path.parent() != Some(old_parent.as_path()) {
                    fsync_directory(old_parent).map_err(RenameError::RenameIO)?;
                }
            }
        }
        res.map_err(RenameError::RenameIO)
    }
}

impl Read for MmapDataFile {
    /// Read from the logical `[0, end)` region at the current seek position. Never reads capacity
    /// padding. An empty target buffer reads nothing. Fails with `UnexpectedEof` if the mapping
    /// does not cover the range (a file that shrank under a read-only open), as a file read past
    /// its end would.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.seek_pos >= self.end {
            return Ok(0);
        }
        let start = self.seek_pos as usize;
        let avail = (self.end - self.seek_pos) as usize;
        let n = avail.min(buf.len());
        let src = match &self.backing {
            Backing::Rw(map) => map.get(start..start + n),
            Backing::Ro(map) => map.get(start..start + n),
            Backing::Empty => return Ok(0),
        };
        let src = src.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{} is shorter than its logical end", self.path.display()),
            )
        })?;
        buf[..n].copy_from_slice(src);
        self.seek_pos += n as u64;
        Ok(n)
    }
}

impl Seek for MmapDataFile {
    /// Seek within the logical byte range; `SeekFrom::End` is relative to the logical length.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Compute the target in `i64`, failing on any overflow (an out-of-range `Start`, or an
        // `End`/`Current` offset that wraps) rather than the raw `as`/`+` that could silently wrap.
        let out_of_range =
            || io::Error::new(io::ErrorKind::InvalidInput, "seek position out of range");
        let new: i64 = match pos {
            SeekFrom::Start(p) => i64::try_from(p).map_err(|_| out_of_range())?,
            SeekFrom::End(p) => i64::try_from(self.end)
                .ok()
                .and_then(|e| e.checked_add(p))
                .ok_or_else(out_of_range)?,
            SeekFrom::Current(p) => i64::try_from(self.seek_pos)
                .ok()
                .and_then(|c| c.checked_add(p))
                .ok_or_else(out_of_range)?,
        };
        if new < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek to negative position"));
        }
        self.seek_pos = new as u64;
        Ok(self.seek_pos)
    }
}

impl Write for MmapDataFile {
    /// Place `buf` per the configured [`WriteMode`]: at the logical end ([`WriteMode::Append`],
    /// ignoring the seek position) or at the current seek position ([`WriteMode::Random`],
    /// extending the high-water `end` when it passes it), growing the mapping if required.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                "file not open for write",
            ));
        }
        // Fail closed after any earlier write/sync/remap failure: the durability of what is already
        // written is unknown, so admitting more data would only pile unsyncable bytes on a failing
        // mapping and hide the fault. Like every write error, `PackInner::append` moves the pack
        // to its failed state on it (see `pack.rs`): every later append and commit fails until the
        // file is reopened, and that open's recovery replays the WAL and truncates the unacked
        // tail. (A failed consensus-pack save stops the node; a same-epoch re-entry before then
        // keeps the failed handle, whose saves keep failing.)
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing further writes",
            ));
        }
        if buf.is_empty() {
            return Ok(0);
        }
        // The first write after a sealed open with a deferred retirement retires the sentinel
        // (durably) before it changes a byte.
        self.retire_seal_if_pending()?;
        let n = buf.len() as u64;
        let start = match self.opts.write_mode {
            WriteMode::Append => self.end,
            WriteMode::Random => self.seek_pos,
        };
        // Checked add: in `WriteMode::Random` `start` is the caller-controlled seek position, so
        // guard the end offset rather than wrap/panic — matching the overflow-hardened `seek`.
        let write_end = start.checked_add(n).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "write offset overflows u64")
        })?;
        self.ensure_capacity(write_end)?;
        let start_us = start as usize;
        match &mut self.backing {
            Backing::Rw(map) => map_range_mut(map, start_us, buf.len()).copy_from_slice(buf),
            Backing::Ro(_) | Backing::Empty => return Err(io::Error::other("no writable mapping")),
        }
        match self.opts.write_mode {
            WriteMode::Append => self.end += n,
            WriteMode::Random => {
                self.seek_pos += n;
                self.end = self.end.max(self.seek_pos);
            }
        }
        Ok(buf.len())
    }

    /// Page-cache visibility for a separate reader, without a durability barrier (like a buffered
    /// `flush`: the data is out of our hands but not necessarily on disk).
    fn flush(&mut self) -> io::Result<()> {
        self.flush_dirty(false)
    }
}

impl Drop for MmapDataFile {
    fn drop(&mut self) {
        self.close_on_drop();
        self.release_mappings();
    }
}

impl MmapDataFile {
    /// `Drop`'s work before the mappings go: remove, leave, or seal the file.
    fn close_on_drop(&mut self) {
        // Whatever path this takes, a reader still holding the view finds nothing mapped from now
        // on, rather than a mapping about to go.
        self.view.set_mapping(std::ptr::null(), 0);
        if self.remove_on_drop {
            self.release_mappings(); // release the map before removing the file
            match std::fs::remove_file(&self.path) {
                // Already unlinked (e.g. its directory was removed while it was still mapped).
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) if !std::thread::panicking() => {
                    tracing::error!("MmapDataFile: failed to remove file on drop: {e}");
                }
                _ => {}
            }
            return;
        }
        // A read-only handle writes nothing, and [`Self::seal`] already closed a sealed one.
        if self.read_only || self.closed {
            return;
        }
        // A sealed file whose sentinel retirement is still pending was never written: on disk it
        // is still sealed, exactly as it was opened. Leave it alone; a re-seal here would
        // truncate and rewrite the sentinel, and a crash inside that would leave it unsealed.
        if self.seal_pending {
            return;
        }
        // Seal with the clean-close sentinel ONLY when this handle was opened clean (or a
        // successful recovery cleared `opened_unclean` via `mark_consistent`), no
        // write/sync/remap failed this session, and the owner is not unwinding from a panic.
        // Sealing an unclean (still-padded/torn) file would hide its tail from the next open's
        // recovery — turning a truncatable tail into a permanent `CorruptPack`; sealing a
        // poisoned file would vouch for a tail of unknown durability; and a panic can strike
        // mid-write (e.g. between an output's header and its last batch), so sealing then would
        // certify a half-written record set as complete. In each case leave the file exactly as-is
        // (no truncate, no sentinel) so the next open re-runs recovery/heal.
        if self.opened_unclean || self.is_poisoned() || std::thread::panicking() {
            // Best-effort push of any committed data. A poisoned handle's `flush_dirty` returns
            // `Err` without retrying (never launder a failed sync); a merely-unclean handle msyncs
            // its tail so committed bytes are durable even though the file stays unsealed.
            if let Err(e) = self.flush_dirty(true) {
                if !std::thread::panicking() {
                    tracing::debug!(
                        "MmapDataFile: leaving file unsealed on drop (unclean or poisoned); \
                         best-effort flush returned: {e}"
                    );
                }
            }
            return;
        }
        // Best effort: each failed step is logged by `clean_close`; an owner that must know the
        // outcome seals explicitly with `seal`.
        let _ = self.clean_close();
    }

    /// Seal the file now with the clean-close seal `Drop` runs (msync, truncate to the logical
    /// end, the clean-close sentinel, fsync) and report its outcome: the first step that failed,
    /// or a refusal. For an owner that installs, trusts or deletes something on the strength of
    /// the seal: a seal in `Drop` can only log a failure, and a read-back of the file afterwards
    /// goes through the page cache, so it cannot see a failed final fsync (after a power loss the
    /// truncate and the sentinel may then be gone).
    ///
    /// It is refused, with an error and nothing written, where `Drop` would leave the file
    /// unsealed: opened unclean and not recovered ([`Self::mark_consistent`]), poisoned
    /// ([`Self::poison`]), dropped during a panic, read-only, or to be removed on drop. `Drop`
    /// then does what it always does. A sealed file never written since its open (a deferred
    /// sentinel retirement, see [`MmapFileOptions::defer_seal_retire`]) is sealed on disk as it
    /// was opened: `Ok` with nothing written.
    ///
    /// Once the seal ran, whatever its outcome, the handle is closed: its mappings are released,
    /// `Drop` does nothing more (a retried sync after a failed one could report success for
    /// writes that never reached the disk), and it must not be used again; a second `seal` is
    /// refused.
    pub(crate) fn seal(&mut self) -> io::Result<()> {
        let refused = if self.closed {
            Some("it was already sealed")
        } else if self.read_only {
            Some("it is read-only")
        } else if self.remove_on_drop {
            Some("it is to be removed on drop")
        } else if self.opened_unclean {
            Some("it was opened unclean and not recovered")
        } else if self.is_poisoned() {
            Some("an earlier write or sync failed (poisoned)")
        } else if std::thread::panicking() {
            Some("the thread is panicking")
        } else {
            None
        };
        if let Some(why) = refused {
            return Err(io::Error::other(format!("{} not sealed: {why}", self.path.display())));
        }
        self.closed = true;
        self.view.set_mapping(std::ptr::null(), 0);
        if self.seal_pending {
            self.release_mappings();
            return Ok(());
        }
        self.clean_close()
    }

    /// The clean-close seal: msync, truncate away the padding, then (only if the msync succeeded)
    /// append an 8-byte clean-close sentinel and fsync, so the on-disk file is exactly `end` data
    /// bytes plus the sentinel and durable — a reopen validates the sentinel, strips it back to
    /// `end`, and knows the file was sealed. A 0-length file is left empty (nothing to seal).
    /// Every step runs whatever the earlier ones returned (as `Drop` always has); each failure is
    /// logged, and the first is returned. Callers check the gate first (see `close_on_drop` and
    /// [`Self::seal`]).
    fn clean_close(&mut self) -> io::Result<()> {
        let mut first: Option<io::Error> = None;
        let path = self.path.clone();
        let mut failed = |step: &str, e: io::Error| {
            tracing::error!("MmapDataFile: failed to {step} {} on close: {e}", path.display());
            if first.is_none() {
                first = Some(io::Error::new(
                    e.kind(),
                    format!("failed to {step} {} on close: {e}", path.display()),
                ));
            }
        };
        let flushed = match self.flush_dirty(true) {
            Ok(()) => true,
            Err(e) => {
                failed("msync", e);
                false
            }
        };
        self.view.set_mapping(std::ptr::null(), 0);
        self.view.physical.fetch_min(self.end, Ordering::AcqRel);
        self.release_mappings(); // unmap before truncating
        if let Err(e) = self.file.set_len(self.end) {
            failed("truncate", e);
        }
        // Only stamp the clean-close sentinel when the tail msync succeeded. If it failed we cannot
        // vouch for the durability of the `[flushed_end, end)` tail, so leave the file unsentineled
        // and let the next open take the recovery/heal path rather than trust a possibly-short
        // tail.
        //
        // Crash-window note: if the process dies after the `set_len(self.end)` above but before
        // this sentinel is durable, the file is left at exactly `end` data bytes. A later
        // open then tests the last 8 DATA bytes as a candidate sentinel and could read the
        // file as cleanly sealed at `end - 8` — but only if those 8 bytes happen to equal
        // `clean_close_sentinel(end - 8)`, a ~2^-64 coincidence (and not forgeable: the
        // writer is trusted, the sentinel is not a security boundary). This residual window
        // is accepted rather than fixed: a magic prefix would only shrink it while forcing
        // an on-disk format/version break, and a missed seal merely costs a recovery pass
        // on the next open.
        if flushed && self.end > 0 {
            let sentinel = clean_close_sentinel(self.end);
            if let Err(e) = self.file.write_all_at(&sentinel, self.end) {
                failed("write the clean-close sentinel of", e);
            }
        }
        let synced = fsync_file(&self.file);
        #[cfg(test)]
        let synced = match synced {
            Ok(()) if std::mem::take(&mut self.fail_seal_sync) => {
                Err(io::Error::other("injected seal fsync failure"))
            }
            synced => synced,
        };
        if let Err(e) = synced {
            failed("fsync", e);
        }
        first.map_or(Ok(()), Err)
    }

    /// Test-only: the next clean-close seal of this handle ([`Self::seal`] or `Drop`) reports its
    /// final fsync as failed after the fsync ran: the sentinel is then in the page cache (and, in
    /// a test, on disk), so a read-back finds the file sealed, as after a real fsync error.
    #[cfg(test)]
    pub(crate) fn fail_next_seal_sync_for_test(&mut self) {
        self.fail_seal_sync = true;
    }

    /// True once [`Self::seal`] ran on this handle (whatever its outcome).
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
}

/// A read handle on a data file, from [`MmapDataFile::try_clone`], that keeps its own cursor and
/// reads with positional reads (`pread`). A cloned `File` descriptor shares one file offset with
/// every other clone of it, so two readers over the same file would move each other's position; a
/// `DataFileReader` never can.
#[derive(Debug)]
pub struct DataFileReader {
    file: File,
    pos: u64,
}

impl Read for DataFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read_at(buf, self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for DataFileReader {
    /// `SeekFrom::End` is relative to the physical file length (which includes any mmap capacity
    /// padding; readers bound themselves by the logical end `try_clone` returned).
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let (base, offset) = match pos {
            SeekFrom::Start(p) => (p, 0),
            SeekFrom::End(p) => (self.file.metadata()?.len(), p),
            SeekFrom::Current(p) => (self.pos, p),
        };
        self.pos = base.checked_add_signed(offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "seek position out of range")
        })?;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A deterministic, non-trivial byte pattern.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(7)).collect()
    }

    fn tiny_opts() -> MmapFileOptions {
        MmapFileOptions {
            initial_size: 64,
            max_map_size: 128,
            grow_mode: GrowMode::Reopen,
            write_mode: WriteMode::Append,
            access: MmapAccess::Normal,
            derived: false,
            reserve: 0,
            max_record_size: None,
            defer_seal_retire: false,
            drop_cache_on_unclean_open: false,
        }
    }

    fn random_opts() -> MmapFileOptions {
        MmapFileOptions {
            initial_size: 64,
            max_map_size: 128,
            grow_mode: GrowMode::Reopen,
            write_mode: WriteMode::Random,
            access: MmapAccess::Random,
            derived: false,
            reserve: 0,
            max_record_size: None,
            defer_seal_retire: false,
            drop_cache_on_unclean_open: false,
        }
    }

    /// A growth does not fsync: it marks the size extension for the next durability barrier, which
    /// fsyncs it once after its `msync`. A barrier with no growth since the last stays
    /// `msync`-only, and every barrier, `try_clone`'s included, pays a pending one.
    #[test]
    fn growth_defers_its_size_fsync_to_the_next_barrier() {
        let tmp = TempDir::with_prefix("mmap_df_deferred_size_sync").expect("temp dir");
        let mut df =
            MmapDataFile::open_with(tmp.path().join("data"), false, tiny_opts()).expect("open");
        let size_syncs = |df: &MmapDataFile| df.size_syncs.load(Ordering::Relaxed);

        // 64-byte initial capacity in 128-byte steps: 400 bytes grow the file several times.
        df.write_all(&pattern(400)).expect("write");
        assert_eq!(size_syncs(&df), 0, "a growth itself must not fsync");
        df.sync_all().expect("barrier");
        assert_eq!(size_syncs(&df), 1, "the barrier after growth fsyncs the size once");

        // Append within the current capacity (no growth), then barrier again.
        let room = (df.capacity - df.len()).min(8) as usize;
        df.write_all(&pattern(room)).expect("write within capacity");
        df.sync_all().expect("barrier without growth");
        assert_eq!(size_syncs(&df), 1, "a barrier with no growth since the last stays msync-only");

        df.write_all(&pattern(1_000)).expect("grow again");
        let (_reader, end) = df.try_clone().expect("clone barrier");
        assert_eq!(end, df.len());
        assert_eq!(size_syncs(&df), 2, "try_clone's barrier pays the pending size fsync");
    }

    #[test]
    fn derived_file_leaves_its_size_to_the_seal() {
        let tmp = TempDir::with_prefix("mmap_df_derived_size").expect("temp dir");
        let path = tmp.path().join("index");
        let opts = MmapFileOptions { derived: true, ..tiny_opts() };
        {
            let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
            df.write_all(&pattern(400)).expect("write");
            df.sync_all().expect("barrier");
            let (_reader, _end) = df.try_clone().expect("clone barrier");
            assert_eq!(
                df.size_syncs.load(Ordering::Relaxed),
                0,
                "derived barriers skip the size fsync"
            );
        }
        // The clean close still seals: the reopen is clean and reads every byte back.
        let mut df = MmapDataFile::open_with(&path, true, opts).expect("reopen");
        assert!(!df.opened_unclean(), "the seal made the derived file clean");
        let mut buf = vec![0u8; 400];
        df.read_exact(&mut buf).expect("read back");
        assert_eq!(buf, pattern(400));
    }

    /// Write `len` bytes and exit with no barrier and no clean-close seal, as a crash would: the
    /// bytes may still be dirty pages in the page cache.
    fn write_and_crash(path: &Path, opts: MmapFileOptions, len: usize) {
        let mut df = MmapDataFile::open_with(path, false, opts).expect("open");
        df.write_all(&pattern(len)).expect("write");
        std::mem::forget(df);
    }

    fn open_syncs(df: &MmapDataFile) -> usize {
        df.open_syncs.load(Ordering::Relaxed)
    }

    fn open_evictions(df: &MmapDataFile) -> usize {
        df.open_evictions.load(Ordering::Relaxed)
    }

    /// [`tiny_opts`] with [`MmapFileOptions::drop_cache_on_unclean_open`] (tndb's logs set it).
    fn drop_cache_opts() -> MmapFileOptions {
        MmapFileOptions { drop_cache_on_unclean_open: true, ..tiny_opts() }
    }

    /// A writable open of an unclean file fsyncs it once before `flushed_end` vouches for its
    /// existing bytes: the crashed writer may never have msynced them, and later barriers flush
    /// only from `flushed_end` on. With `drop_cache_on_unclean_open` it then drops the file's
    /// cached pages once; without it, never.
    #[test]
    fn unclean_writable_open_syncs_existing_bytes_once() {
        for opts in [tiny_opts(), drop_cache_opts()] {
            let tmp = TempDir::with_prefix("mmap_df_unclean_open_sync").expect("temp dir");
            let path = tmp.path().join("data");
            write_and_crash(&path, opts, 200);
            let df = MmapDataFile::open_with(&path, false, opts).expect("unclean reopen");
            assert!(df.opened_unclean(), "precondition: opened unclean");
            assert_eq!(open_syncs(&df), 1, "an unclean writable open syncs the file once");
            let evictions = usize::from(opts.drop_cache_on_unclean_open);
            assert_eq!(open_evictions(&df), evictions, "cache drops with {opts:?}");
            assert_eq!(df.synced_end(), df.len(), "the synced bytes are the durable watermark");
            assert_eq!(df.slice(0, 200), Some(&pattern(200)[..]), "the bytes read back whole");
            std::mem::forget(df); // no recovery in this unit test; don't seal the padding on drop
        }
    }

    /// A clean close synced before sealing, so a clean reopen trusts its bytes without a sync,
    /// and keeps its cached pages.
    #[test]
    fn clean_open_skips_the_open_sync() {
        let tmp = TempDir::with_prefix("mmap_df_clean_open_sync").expect("temp dir");
        let path = tmp.path().join("data");
        {
            let mut df = MmapDataFile::open_with(&path, false, drop_cache_opts()).expect("open");
            df.write_all(&pattern(200)).expect("write");
            assert_eq!(open_syncs(&df), 0, "a fresh file has nothing to sync");
            assert_eq!(open_evictions(&df), 0, "a fresh file has nothing to drop");
        }
        let df = MmapDataFile::open_with(&path, false, drop_cache_opts()).expect("clean reopen");
        assert!(!df.opened_unclean(), "precondition: opened clean");
        assert_eq!(open_syncs(&df), 0, "a clean open needs no open-time sync");
        assert_eq!(open_evictions(&df), 0, "a clean open keeps the cache");
    }

    /// A read-only handle never writes or advances a durable watermark, so it never syncs at open
    /// (nor drops the cache).
    #[test]
    fn read_only_open_skips_the_open_sync() {
        let tmp = TempDir::with_prefix("mmap_df_ro_open_sync").expect("temp dir");
        let path = tmp.path().join("data");
        write_and_crash(&path, drop_cache_opts(), 200);
        let df = MmapDataFile::open_with(&path, true, drop_cache_opts()).expect("read-only reopen");
        assert!(df.opened_unclean(), "precondition: opened unclean");
        assert_eq!(open_syncs(&df), 0, "a read-only open never syncs");
        assert_eq!(open_evictions(&df), 0, "a read-only open keeps the cache");
    }

    /// A derived file is rebuilt from the data log after any unclean open, so its existing bytes
    /// are never trusted and need no open-time sync (nor a cache drop).
    #[test]
    fn derived_unclean_open_skips_the_open_sync() {
        let tmp = TempDir::with_prefix("mmap_df_derived_open_sync").expect("temp dir");
        let path = tmp.path().join("index");
        let opts = MmapFileOptions { derived: true, ..drop_cache_opts() };
        write_and_crash(&path, opts, 200);
        let df = MmapDataFile::open_with(&path, false, opts).expect("unclean reopen");
        assert!(df.opened_unclean(), "precondition: opened unclean");
        assert_eq!(open_syncs(&df), 0, "a derived file is rebuilt, not synced, after a crash");
        assert_eq!(open_evictions(&df), 0, "a derived file keeps the cache");
        std::mem::forget(df);
    }

    /// A process restarted after a failed writeback whose error went to the process that exited:
    /// newer bytes and a later commit marker sit in the page cache, clean but never written, and
    /// the open's fsync succeeds without writing them (Linux reports a writeback error once). With
    /// `drop_cache_on_unclean_open` the open reads what the disk holds: the commit marker that
    /// reached the disk, and the disk's bytes past the last good write.
    #[test]
    fn unclean_open_drops_the_cache_and_reads_the_disk() {
        let tmp = TempDir::with_prefix("mmap_df_drop_cache").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions { drop_cache_on_unclean_open: true, ..Default::default() };
        let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
        disk_model::track(&path);
        df.write_all(&pattern(200)).expect("write");
        df.sync_all().expect("msync");
        df.stamp_commit_marker();
        df.sync_disk().expect("the marker reaches the disk");

        // Writeback fails from here on, and no sync of this thread reaches the model's disk: not
        // the newer bytes, not their marker, and not the restarted open's fsync.
        disk_model::set_flushes_lost(true);
        df.write_all(&[0xAB; 300]).expect("newer bytes");
        df.sync_all().expect("msync");
        df.stamp_commit_marker();
        df.sync_disk().expect("fsync");
        std::mem::forget(df); // the process exits: no seal
        let reopened = MmapDataFile::open_with(&path, false, opts);
        disk_model::set_flushes_lost(false);
        let df = reopened.expect("unclean reopen");
        assert!(df.opened_unclean(), "precondition: opened unclean");
        assert_eq!(open_evictions(&df), 1, "the open dropped the cache");
        assert_eq!(df.committed_end(), Some(200), "the marker the disk holds, not the cache's 500");
        assert_eq!(df.slice(0, 200), Some(&pattern(200)[..]), "the bytes that reached the disk");
        assert_eq!(df.slice(200, 300), Some(&[0_u8; 300][..]), "the disk's zeros past them");
        std::mem::forget(df);
        disk_model::untrack(&path);
    }

    /// The reserving mode's point: the mapping never moves while the file grows underneath it, and
    /// the data written through it is durable and reads back after a clean close.
    #[test]
    fn reserved_mapping_never_moves_across_growth() {
        let tmp = TempDir::with_prefix("mmap_df_reserved").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions { reserve: 1 << 20, ..tiny_opts() };
        let mut expected = pattern(10);
        {
            let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
            df.write_all(&pattern(10)).expect("write");
            let base = df.slice(0, 1).expect("slice").as_ptr();
            for _ in 0..20 {
                df.write_all(&pattern(500)).expect("grow");
                expected.extend_from_slice(&pattern(500));
                assert_eq!(df.slice(0, 1).expect("slice").as_ptr(), base, "the mapping moved");
            }
            assert!(df.capacity > 4096, "the file grew several times (capacity {})", df.capacity);
            df.sync_all().expect("barrier");
        }
        let mut df = MmapDataFile::open(&path, true).expect("reopen");
        assert!(!df.opened_unclean(), "the clean close sealed the file");
        let mut buf = vec![0u8; expected.len()];
        df.read_exact(&mut buf).expect("read back");
        assert_eq!(buf, expected);
    }

    /// A shrink under a reservation keeps the mapping in place, and a later growth re-extends the
    /// file through the same mapping (the re-grown range reads as the new data, not stale bytes).
    #[test]
    fn reserved_mapping_truncate_keeps_mapping() {
        let tmp = TempDir::with_prefix("mmap_df_reserved_trunc").expect("temp dir");
        let opts = MmapFileOptions { reserve: 1 << 20, ..tiny_opts() };
        let mut df = MmapDataFile::open_with(tmp.path().join("data"), false, opts).expect("open");
        df.write_all(&pattern(3000)).expect("write");
        let base = df.slice(0, 1).expect("slice").as_ptr();
        df.truncate(1000).expect("truncate");
        assert_eq!(df.len(), 1000);
        assert_eq!(df.slice(0, 1).expect("slice").as_ptr(), base, "truncate moved the mapping");
        df.write_all(&pattern(2000)).expect("regrow");
        assert_eq!(df.slice(0, 1).expect("slice").as_ptr(), base, "regrowth moved the mapping");
        let mut expected = pattern(3000)[..1000].to_vec();
        expected.extend_from_slice(&pattern(2000));
        assert_eq!(df.slice(0, expected.len()).expect("slice"), &expected[..]);
    }

    /// Growing past the reservation falls back to a remap with a larger reservation (the mapping
    /// may move then) and stays correct.
    #[test]
    fn reserved_mapping_falls_back_past_reservation() {
        let tmp = TempDir::with_prefix("mmap_df_reserved_past").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions { reserve: 4096, ..tiny_opts() };
        {
            let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
            df.write_all(&pattern(20_000)).expect("write past the reservation");
            assert!(df.reserved >= 20_000, "re-reserved larger (reserved {})", df.reserved);
            assert_eq!(df.slice(0, 20_000).expect("slice"), &pattern(20_000)[..]);
            df.sync_all().expect("barrier");
        }
        let mut df = MmapDataFile::open(&path, true).expect("reopen");
        assert!(!df.opened_unclean());
        let mut buf = vec![0u8; 20_000];
        df.read_exact(&mut buf).expect("read back");
        assert_eq!(buf, pattern(20_000));
    }

    /// A [`MapView`] reads only published bytes, follows the mapping when growth outgrows the
    /// reservation, and a slice borrowed before that move stays readable: the old mapping is
    /// retired (kept mapped until the file drops), not unmapped, while the view is shared.
    #[test]
    fn map_view_survives_reservation_overflow() {
        let tmp = TempDir::with_prefix("mmap_df_map_view").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions { reserve: 4096, ..tiny_opts() };
        let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
        let view = df.view();
        df.write_all(&pattern(1_000)).expect("write");
        // SAFETY (each view read in this test): one thread, and every write lands before the reads
        // after it, past every byte a live slice borrows (the growth appends past the published
        // length).
        let old = unsafe {
            assert_eq!(view.slice(0, 100), None, "nothing published yet");
            view.publish_len(1_000);
            let old = view.slice(0, 1_000).expect("published slice");
            assert_eq!(old, &pattern(1_000)[..]);
            assert_eq!(view.slice(900, 101), None, "past the published length");
            assert_eq!(view.slice(1_000, 0).map(<[u8]>::len), Some(0));
            assert_eq!(view.slice(1_001, 0), None);
            assert_eq!(
                view.tail_within(900, 1_000).map(<[u8]>::len),
                Some(100),
                "the tail stops at its limit"
            );
            assert_eq!(view.tail_within(1_001, 1_000), None);
            old
        };

        df.write_all(&pattern(40_000)[1_000..]).expect("grow past the reservation");
        assert!(df.reserved >= 40_000, "re-reserved larger (reserved {})", df.reserved);
        assert_eq!(df.retired.len(), 1, "the old mapping is retired, not unmapped");
        assert_eq!(old, &pattern(1_000)[..], "a slice of the old mapping is still readable");
        view.publish_len(40_000);
        // SAFETY: as above.
        let new = unsafe { view.slice(0, 40_000) }.expect("slice after the move");
        assert_eq!(new, &pattern(40_000)[..]);
        assert_ne!(new.as_ptr(), old.as_ptr(), "the view follows the new mapping");
    }

    /// A view outlives its file only as an empty view: once the file drops, by any of its paths
    /// (here a read-only handle's, and an unclean writable one's), a reader holding the view finds
    /// nothing mapped instead of a mapping that is gone. A slice is exactly the bytes asked for.
    #[test]
    fn map_view_is_empty_once_its_file_drops() {
        let tmp = TempDir::with_prefix("mmap_df_view_drop").expect("temp dir");
        let path = tmp.path().join("data");
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&pattern(1_000)).expect("write");
            let view = df.view();
            view.publish_len(1_000);
            // SAFETY (each view read in this test): one thread; nothing writes a byte a live
            // slice borrows.
            let asked = unsafe { view.slice(10, 20) }.map(<[u8]>::len);
            assert_eq!(asked, Some(20), "exactly the bytes asked");
            df.sync_all().expect("sync");
        }
        let read_only = MmapDataFile::open_with(&path, true, tiny_opts()).expect("open read-only");
        let view = read_only.view();
        view.publish_len(1_000);
        assert!(unsafe { view.slice(0, 8) }.is_some(), "mapped while the file is open");
        drop(read_only);
        assert_eq!(unsafe { view.slice(0, 8) }, None, "nothing mapped once a read-only file drops");
        assert_eq!(unsafe { view.tail_within(0, 8) }, None);

        let mut unclean = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        unclean.write_all(&pattern(10)).expect("write");
        unclean.opened_unclean = true;
        let view = unclean.view();
        view.publish_len(8);
        drop(unclean);
        assert_eq!(unsafe { view.slice(0, 8) }, None, "nor once an unclean file drops, unsealed");
    }

    /// A [`MapView`] read is clamped to the file's physical size, not only to the published length
    /// and the mapping: under a reservation the mapping runs past the end of the file, so a length
    /// published (or a limit passed) past it reads nothing there instead of a page past EOF.
    #[test]
    fn map_view_clamps_to_the_physical_size() {
        let tmp = TempDir::with_prefix("mmap_df_view_physical").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions { reserve: 1 << 20, ..tiny_opts() };
        let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
        df.write_all(&pattern(100)).expect("write");
        let physical = df.capacity;
        let view = df.view();
        assert!(view.mapped.load(Ordering::Relaxed) > physical + 8192, "the reservation runs on");
        // A stale or wrong length, past the file.
        view.publish_len(physical + 8192);
        // SAFETY: one thread, every write before the reads and none while they live.
        unsafe {
            assert!(view.slice(0, 100).is_some(), "bytes in the file read");
            assert!(view.slice(physical, 8).is_none(), "nothing past the physical size");
            let tail = view.tail_within(0, physical + 8192).map(<[u8]>::len);
            assert_eq!(tail, Some(physical as usize));
        }
        assert!(view.msync(0, physical + 8192).is_err(), "a sync past the file errs");
    }

    /// The file refuses to cut or zero bytes published to readers: `truncate` below the published
    /// length errs and `rewind_to` below it poisons the file, each leaving every byte in place.
    #[test]
    fn shrinks_below_the_published_length_are_refused() {
        let tmp = TempDir::with_prefix("mmap_df_shrink_refused").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(200);
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        let view = df.view();
        view.publish_len(100);
        assert!(df.rewind_to(150).is_some(), "a rewind above the published length runs");
        assert!(df.truncate(50).is_err(), "a truncate below it is refused");
        assert_eq!(df.len(), 150);
        df.truncate(120).expect("a truncate down to it runs");
        assert_eq!(df.rewind_to(50), None, "a rewind below it is refused");
        assert!(df.is_poisoned(), "and poisons the file");
        assert_eq!(df.len(), 120);
        // SAFETY: one thread, every write before the read and none while it lives.
        let published = unsafe { view.slice(0, 100) };
        assert_eq!(published, Some(&data[..100]), "the published bytes are untouched");
    }

    /// A file dropped while its [`MapView`] is still shared leaves its mapping to the view, so a
    /// slice borrowed through the view stays readable until the view goes.
    #[test]
    fn dropped_file_leaves_its_mapping_to_a_shared_view() {
        let tmp = TempDir::with_prefix("mmap_df_view_keeps_map").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(1_000);
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        let view = df.view();
        view.publish_len(1_000);
        // SAFETY (each view read in this test): one thread; the drop's clean close writes its
        // sentinel and cuts the file only past the published bytes.
        let borrowed = unsafe { view.slice(0, 1_000) }.expect("published");
        drop(df);
        assert!(!view.kept.lock().expect("kept").is_empty(), "the view keeps the mapping");
        assert_eq!(borrowed, &data[..], "the borrowed bytes are still mapped");
        assert_eq!(unsafe { view.slice(0, 8) }, None, "but the view reads nothing new");
    }

    /// An empty file opened with a reservation is mapped at once (no first-write remap) and works
    /// like any other: write, close, reopen.
    #[test]
    fn reserved_mapping_on_empty_file() {
        let tmp = TempDir::with_prefix("mmap_df_reserved_empty").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions { reserve: 1 << 20, ..tiny_opts() };
        {
            let mut df = MmapDataFile::open_with(&path, false, opts).expect("open empty");
            assert_eq!((df.len(), df.capacity), (0, 0));
            assert!(matches!(df.backing, Backing::Rw(_)), "the reservation is mapped up front");
            df.write_all(&pattern(100)).expect("write");
        }
        let mut df = MmapDataFile::open(&path, true).expect("reopen");
        assert!(!df.opened_unclean());
        let mut buf = vec![0u8; 100];
        df.read_exact(&mut buf).expect("read back");
        assert_eq!(buf, pattern(100));
    }

    /// The digest-index write pattern: sequential fill, in-place overwrite, and extend-at-end.
    #[test]
    fn random_write_overwrites_and_extends() {
        let tmp = TempDir::with_prefix("mmap_df_random_write").expect("temp dir");
        let path = tmp.path().join("data");
        let expected = {
            let mut e = pattern(300);
            e[100..116].fill(0xAA);
            e.extend_from_slice(&pattern(50));
            e
        };
        {
            let mut df = MmapDataFile::open_with(&path, false, random_opts()).expect("open");
            // Sequential fill (crosses the tiny initial_size/max_map_size to exercise growth).
            df.write_all(&pattern(300)).expect("initial write");
            assert_eq!(df.len(), 300);
            // In-place overwrite in the middle — no length change.
            df.seek(SeekFrom::Start(100)).expect("seek");
            df.write_all(&[0xAA; 16]).expect("overwrite");
            assert_eq!(df.len(), 300, "overwrite within bounds keeps length");
            // Extend past end from an explicit seek(End) (the odx append pattern).
            df.seek(SeekFrom::End(0)).expect("seek end");
            df.write_all(&pattern(50)).expect("append via seek(End)");
            assert_eq!(df.len(), 350);
            df.seek(SeekFrom::Start(0)).expect("seek");
            let mut all = vec![0u8; 350];
            df.read_exact(&mut all).expect("read all");
            assert_eq!(all, expected);
        }
        // Clean close truncates to the high-water length; reopen and re-verify.
        let mut df = MmapDataFile::open(&path, true).expect("reopen ro");
        assert_eq!(df.len(), 350);
        let mut all = vec![0u8; 350];
        df.read_exact(&mut all).expect("read all after reopen");
        assert_eq!(all, expected);
    }

    /// `commit_marker`/`parse_commit_marker` round-trip; garbage and out-of-file positions are
    /// rejected by the double CRC; a marker never reads as a clean-close sentinel.
    #[test]
    fn commit_marker_encode_validate() {
        assert_eq!(parse_commit_marker(&[0u8; 16], 1024), None, "zero tail is not a marker");
        let m = commit_marker(466);
        assert_eq!(parse_commit_marker(&m, 1024), Some(466));
        for i in 0..16 {
            let mut bad = m;
            bad[i] ^= 1;
            assert_eq!(parse_commit_marker(&bad, 1024), None, "bit flip at {i} must invalidate");
        }
        assert_eq!(parse_commit_marker(&commit_marker(2000), 1024), None, "pos past EOF rejected");
        // The marker's trailing 8 bytes must not satisfy the clean-close check for the file size.
        let last8: [u8; 8] = m[8..16].try_into().unwrap();
        assert!(
            !sentinel_matches(&last8, 1024 - SENTINEL_LEN),
            "a commit marker must not read as a clean-close sentinel"
        );
    }

    /// After an unclean exit (no clean-close sentinel), a valid tail commit marker exposes the
    /// durable committed end for recovery. Data is msync'd before the marker is stamped (the
    /// `persist()` ordering that makes the marker fail-safe).
    #[test]
    fn commit_marker_recovered_on_unclean_reopen() {
        let tmp = TempDir::with_prefix("mmap_df_marker_unclean").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(200);
        {
            let mut df = MmapDataFile::open(&path, false).expect("open");
            df.write_all(&data).expect("write");
            df.sync_all().expect("msync data");
            df.stamp_commit_marker();
            std::mem::forget(df); // skip Drop → unclean, padded, marker intact
        }
        let df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(df.opened_unclean(), "no clean-close sentinel → unclean");
        assert_eq!(df.committed_end(), Some(data.len() as u64));
        std::mem::forget(df); // no heal path in this unit test; don't seal the padding on drop
    }

    /// A clean close truncates the marker away (it lives in the padding), so the reopen is clean
    /// and exposes no committed_end.
    #[test]
    fn commit_marker_removed_by_clean_close() {
        let tmp = TempDir::with_prefix("mmap_df_marker_clean").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(200);
        {
            let mut df = MmapDataFile::open(&path, false).expect("open");
            df.write_all(&data).expect("write");
            df.sync_all().expect("msync");
            df.stamp_commit_marker();
            // Drops here: clean close truncates to `end` (+ sentinel), removing the marker.
        }
        let df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(!df.opened_unclean(), "clean close sealed the file");
        assert_eq!(df.committed_end(), None, "no marker on a clean file");
        assert_eq!(df.len(), data.len() as u64);
    }

    /// The zero-copy slice accessor: borrowed windows equal the written bytes; out-of-bounds or
    /// into-the-padding ranges return `None`.
    #[test]
    fn slice_borrows_mapped_bytes() {
        let tmp = TempDir::with_prefix("mmap_df_slice").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(300); // crosses the tiny initial_size/max_map_size growth boundary
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
            // Borrowed windows match the written bytes (no copy).
            assert_eq!(df.slice(0, 300).expect("full"), &data[..]);
            assert_eq!(df.slice(100, 50).expect("mid"), &data[100..150]);
            assert!(df.slice(300, 0).expect("zero-len at end").is_empty());
            // Out of bounds (past the logical end / into padding) and overflow return None.
            assert!(df.slice(300, 1).is_none(), "one past end");
            assert!(df.slice(280, 40).is_none(), "window overruns end");
            assert!(df.slice(u64::MAX, 1).is_none(), "offset+len overflow");
        }
        // Same slices from a read-only reopen (exact length after clean close).
        let df = MmapDataFile::open(&path, true).expect("reopen ro");
        assert_eq!(df.len(), 300);
        assert_eq!(df.slice(0, 300).expect("ro full"), &data[..]);
        assert_eq!(df.slice(250, 50).expect("ro tail"), &data[250..300]);
        // A never-written (empty) file exposes only a zero-length borrow.
        let empty = MmapDataFile::open(tmp.path().join("empty"), false).expect("open empty");
        assert!(empty.slice(0, 0).expect("empty zero-len").is_empty());
        assert!(empty.slice(0, 1).is_none());
    }

    /// A mapping shorter than the logical end (a file that shrank under a read-only open) makes the
    /// borrows and reads report the gap instead of panicking, while in-range borrows still succeed.
    #[test]
    fn borrows_past_the_mapping_fail_without_panicking() {
        let tmp = TempDir::with_prefix("mmap_df_short_map").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(20);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
        } // clean close: the file is the data plus the sentinel

        let mut ro = MmapDataFile::open_with(&path, true, tiny_opts()).expect("open ro");
        let Backing::Ro(map) = &ro.backing else { panic!("a read-only open maps the file") };
        let mapped = map.len() as u64;
        assert_eq!(mapped, 20 + SENTINEL_LEN);
        // Leave `end` past the mapping, as a file that shrank between the open's length read and
        // its map would without the open-time check.
        ro.end = mapped + 8;
        let len = ro.end as usize;
        assert!(ro.slice(0, len).is_none(), "a slice past the mapping is None");
        assert!(ro.tail(0).is_none(), "a tail past the mapping is None");
        assert!(!ro.is_unwritten(), "an unreadable range is not classified as unwritten");
        assert_eq!(ro.slice(0, 20).expect("in-range slice"), &data[..]);
        let mut buf = vec![0u8; len];
        let err = ro.read(&mut buf).expect_err("a read past the mapping fails");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        // The same on a writable handle, whose mapping is exactly its capacity.
        let mut rw =
            MmapDataFile::open_with(tmp.path().join("rw"), false, tiny_opts()).expect("open rw");
        rw.write_all(&data).expect("write");
        let Backing::Rw(map) = &rw.backing else { panic!("a written file is mapped") };
        let mapped = map.len() as u64;
        let end = rw.end;
        rw.end = mapped + 8;
        let len = rw.end as usize;
        assert!(rw.slice(0, len).is_none(), "a slice past the mapping is None");
        assert!(rw.slice_mut(0, len).is_none(), "a mutable slice past the mapping is None");
        assert!(rw.slice_mut(0, 20).is_some(), "an in-range mutable slice");
        // Restore the real end before the clean close.
        rw.end = end;
    }

    #[test]
    fn roundtrip_and_empty_read() {
        let tmp = TempDir::with_prefix("mmap_df_roundtrip").expect("temp dir");
        let path = tmp.path().join("data");
        let mut df = MmapDataFile::open(&path, false).expect("open");
        df.write_all(&[1, 2, 3, 4, 5]).expect("write");
        df.flush().expect("flush");
        df.seek(SeekFrom::Start(0)).expect("seek");
        // Empty read returns Ok(0), never panics.
        assert_eq!(df.read(&mut []).expect("empty read"), 0);
        let mut buf = [0u8; 5];
        df.read_exact(&mut buf).expect("read");
        assert_eq!(buf, [1, 2, 3, 4, 5]);
        assert_eq!(df.len(), 5);
        assert_eq!(df.data_file_end(), 5);
        assert!(!df.is_empty());
    }

    #[test]
    fn random_reads_by_seek() {
        let tmp = TempDir::with_prefix("mmap_df_random").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(300);
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        // Random-access reads of arbitrary ranges (mimics Pack::read_bytes).
        for &(start, len) in &[(0usize, 10usize), (200, 50), (295, 5), (128, 4)] {
            df.seek(SeekFrom::Start(start as u64)).expect("seek");
            let mut buf = vec![0u8; len];
            df.read_exact(&mut buf).expect("read");
            assert_eq!(&buf[..], &data[start..start + len], "range {start}..{}", start + len);
        }
    }

    #[test]
    fn len_tracks_data_not_capacity() {
        let tmp = TempDir::with_prefix("mmap_df_len").expect("temp dir");
        let path = tmp.path().join("data");
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&pattern(10)).expect("write");
        // len() is the logical data length, not the (padded) mmap capacity.
        assert_eq!(df.len(), 10);
        assert!(df.capacity >= 64, "capacity padded to at least initial_size");
        df.write_all(&pattern(100)).expect("write past initial size");
        assert_eq!(df.len(), 110);
    }

    #[test]
    fn grow_reopen_larger_past_max() {
        let tmp = TempDir::with_prefix("mmap_df_grow").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(500); // forces growth past max_map_size (128) several times
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        // Write in small chunks so growth happens repeatedly.
        for chunk in data.chunks(37) {
            df.write_all(chunk).expect("write chunk");
        }
        assert_eq!(df.len(), 500);
        // Read everything back, including across old capacity boundaries.
        df.seek(SeekFrom::Start(0)).expect("seek");
        let mut all = vec![0u8; 500];
        df.read_exact(&mut all).expect("read all");
        assert_eq!(all, data);
        // Absolute offset read across a boundary.
        df.seek(SeekFrom::Start(120)).expect("seek");
        let mut mid = vec![0u8; 20];
        df.read_exact(&mut mid).expect("read mid");
        assert_eq!(&mid[..], &data[120..140]);
    }

    /// A growth step must reserve real disk blocks (fallocate / F_PREALLOCATE), not leave the new
    /// capacity as a sparse hole — otherwise the first `memcpy` store into that hole SIGBUSes on a
    /// full disk. A preallocated file reports allocated blocks covering its physical size; a sparse
    /// file would report far fewer than its (padded) length.
    #[test]
    fn grow_preallocates_real_blocks_not_sparse() {
        use std::os::unix::fs::MetadataExt;
        let tmp = TempDir::with_prefix("mmap_df_prealloc").expect("temp dir");
        let path = tmp.path().join("data");
        // A 1 MiB first-grow is many filesystem blocks, so allocated-vs-sparse is unambiguous.
        let opts = MmapFileOptions { initial_size: 1 << 20, ..MmapFileOptions::default() };
        let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
        // A few bytes trigger the first grow_to(initial_size), which preallocates the whole
        // capacity.
        df.write_all(&pattern(100)).expect("write");
        // Inspect BEFORE drop: the clean close would truncate the padding away.
        let meta = std::fs::metadata(&path).expect("meta");
        let physical = meta.len();
        assert!(physical >= (1 << 20), "file grew to at least initial_size, got {physical}");
        let allocated = meta.blocks() * 512;
        assert!(
            allocated >= physical,
            "growth must preallocate real blocks (allocated {allocated} >= physical {physical}); a \
             sparse hole would report far fewer"
        );
    }

    /// A handle dropped while its thread unwinds from a panic (which can strike mid-write) must
    /// not seal the file: it is left unsealed so the next open runs recovery instead of trusting a
    /// possibly half-written tail.
    #[test]
    fn drop_during_panic_leaves_file_unsealed() {
        let tmp = TempDir::with_prefix("mmap_df_panic").expect("temp dir");
        let path = tmp.path().join("data");
        let thread_path = path.clone();
        let joined = std::thread::spawn(move || {
            let mut df = MmapDataFile::open_with(&thread_path, false, tiny_opts()).expect("open");
            df.write_all(&pattern(100)).expect("write");
            panic!("simulated panic mid-write");
        })
        .join();
        assert!(joined.is_err(), "the writer thread must have panicked");
        let df = MmapDataFile::open_with(&path, true, tiny_opts()).expect("reopen");
        assert!(df.opened_unclean(), "a file dropped during a panic must not be sealed");
    }

    #[test]
    fn truncate_shrinks_and_persists() {
        let tmp = TempDir::with_prefix("mmap_df_setlen").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(100);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
            df.truncate(40).expect("truncate");
            assert_eq!(df.len(), 40);
            df.seek(SeekFrom::Start(0)).expect("seek");
            let mut buf = vec![0u8; 40];
            df.read_exact(&mut buf).expect("read");
            assert_eq!(&buf[..], &data[..40]);
            // Reading past the truncated end yields nothing.
            df.seek(SeekFrom::Start(40)).expect("seek");
            assert_eq!(df.read(&mut [0u8; 8]).expect("read past end"), 0);
        }
        // Physical file is the truncated length plus the clean-close sentinel.
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), 40 + SENTINEL_LEN);
    }

    #[test]
    fn clean_close_reopen_exact() {
        let tmp = TempDir::with_prefix("mmap_df_reopen").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(250);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
        } // clean close: truncates padding, appends sentinel, fsync
          // After a clean close the physical file is the logical data plus the 8-byte sentinel.
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), 250 + SENTINEL_LEN);
        // Reopen read-write and read back.
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("reopen rw");
            assert_eq!(df.len(), 250);
            let mut buf = vec![0u8; 250];
            df.read_exact(&mut buf).expect("read");
            assert_eq!(buf, data);
        }
        // Reopen read-only and read back.
        {
            let mut df = MmapDataFile::open(&path, true).expect("reopen ro");
            assert_eq!(df.len(), 250);
            let mut buf = vec![0u8; 250];
            df.read_exact(&mut buf).expect("read");
            assert_eq!(buf, data);
        }
    }

    #[test]
    fn clean_close_writes_and_strips_sentinel() {
        let tmp = TempDir::with_prefix("mmap_df_sentinel").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(120);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
        } // clean close: truncate to `end`, append the sentinel, fsync
          // Physical file is the logical data plus the 8-byte sentinel, whose bytes are exactly
          // `clean_close_sentinel(end)`.
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), data.len() as u64 + SENTINEL_LEN);
        let raw = std::fs::read(&path).expect("read raw");
        assert_eq!(&raw[data.len()..], &clean_close_sentinel(data.len() as u64));

        // Reopen: the sentinel is validated and stripped, `len()` is the logical data, and the file
        // is reported as cleanly closed.
        let df = MmapDataFile::open(&path, false).expect("reopen");
        assert_eq!(df.len(), data.len() as u64);
        assert!(!df.opened_unclean(), "a sealed file must not be flagged unclean");
    }

    #[test]
    fn missing_sentinel_flags_unclean_and_keeps_padding() {
        let tmp = TempDir::with_prefix("mmap_df_padded").expect("temp dir");
        let path = tmp.path().join("data");
        // A crashed writer leaves the logical data followed by zero padding and no sentinel.
        let data = pattern(80);
        let mut raw = data.clone();
        raw.extend(std::iter::repeat_n(0u8, 40));
        std::fs::write(&path, &raw).expect("write padded file");

        let df = MmapDataFile::open(&path, false).expect("open padded");
        assert!(df.opened_unclean(), "a file with no valid sentinel must be flagged unclean");
        // The logical end is left at physical EOF for the heal path (zero padding never masquerades
        // as a clean close: crc32(0x00000000) != 0).
        assert_eq!(df.len(), raw.len() as u64);
    }

    #[test]
    fn self_consistent_sentinel_for_wrong_length_is_rejected() {
        let tmp = TempDir::with_prefix("mmap_df_nearmiss").expect("temp dir");
        let path = tmp.path().join("data");
        // Craft a tail that IS a valid sentinel — but for the wrong logical length. It satisfies
        // the self-consistency check (`last4 == crc32(first4)`) yet fails the length
        // tie-in, so the file must still read as unclean.
        let data = pattern(100);
        let mut raw = data.clone();
        raw.extend_from_slice(&clean_close_sentinel(999)); // encodes len 999, not 100
        std::fs::write(&path, &raw).expect("write near-miss file");

        let df = MmapDataFile::open(&path, false).expect("open near-miss");
        assert!(df.opened_unclean(), "a sentinel for the wrong length must not count as sealed");
        assert_eq!(df.len(), raw.len() as u64);
    }

    #[test]
    fn reopen_append_overwrites_sentinel_and_reseals() {
        let tmp = TempDir::with_prefix("mmap_df_reappend").expect("temp dir");
        let path = tmp.path().join("data");
        let first = pattern(60);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&first).expect("write");
        }
        // Reopen (strips the sentinel), append more, clean close again.
        let second = pattern(30);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("reopen rw");
            assert!(!df.opened_unclean());
            assert_eq!(df.len(), first.len() as u64);
            df.seek(SeekFrom::End(0)).expect("seek end");
            df.write_all(&second).expect("append");
        }
        // A fresh sentinel now seals the combined data; the old one was overwritten by the append.
        let total = (first.len() + second.len()) as u64;
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), total + SENTINEL_LEN);
        let mut df = MmapDataFile::open(&path, false).expect("final reopen");
        assert!(!df.opened_unclean());
        assert_eq!(df.len(), total);
        let mut buf = vec![0u8; total as usize];
        df.read_exact(&mut buf).expect("read");
        assert_eq!(&buf[..first.len()], &first[..]);
        assert_eq!(&buf[first.len()..], &second[..]);
    }

    /// A clean close leaves an 8-byte sentinel in the padding past `end`; on a writable reopen that
    /// region is still mapped (`capacity == end + SENTINEL_LEN`). It must read as zero so a later
    /// `ensure_len` that grows `end` into it exposes zeros, not the stale sentinel bytes.
    #[test]
    fn clean_reopen_zeroes_stale_sentinel_padding() {
        let tmp = TempDir::with_prefix("mmap_df_reopen_zero").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(100);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write"); // clean close on drop: truncate padding + sentinel
        }
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("reopen");
        assert!(!df.opened_unclean(), "a clean-closed file must reopen clean");
        let end_before = df.len();
        assert_eq!(end_before, data.len() as u64);
        // Grow `end` into the former sentinel region — no physical grow (capacity == end +
        // SENTINEL_LEN).
        df.ensure_len(end_before + SENTINEL_LEN).expect("grow into the padding gap");
        let gap = df.slice(end_before, SENTINEL_LEN as usize).expect("gap is now addressable");
        assert!(
            gap.iter().all(|&b| b == 0),
            "the stale clean-close sentinel must be zeroed at open, got {gap:?}"
        );
    }

    /// A writable reopen retires the clean-close sentinel on disk before anything is written. A
    /// random-mode file updated in place (which never overwrites its own sentinel) and then
    /// abandoned without a clean close must reopen as unclean, never as sealed over its in-place
    /// writes.
    #[test]
    fn writable_reopen_then_crash_reopens_unclean() {
        let tmp = TempDir::with_prefix("mmap_df_reopen_crash").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(100);
        {
            let mut df = MmapDataFile::open_with(&path, false, random_opts()).expect("open");
            df.write_all(&data).expect("write"); // clean close on drop seals the file
        }
        assert!(
            !MmapDataFile::open_with(&path, true, random_opts()).expect("ro open").opened_unclean(),
            "precondition: the file is sealed"
        );

        let mut df = MmapDataFile::open_with(&path, false, random_opts()).expect("reopen rw");
        assert!(!df.opened_unclean(), "a sealed file reopens clean");
        let on_disk = std::fs::read(&path).expect("read file");
        assert_eq!(
            &on_disk[data.len()..],
            &[0_u8; SENTINEL_LEN as usize],
            "the sentinel is retired on disk at open"
        );
        df.seek(SeekFrom::Start(10)).expect("seek");
        df.write_all(&[0xAB; 5]).expect("in-place write");
        // Crash: the handle never runs its clean-close seal.
        std::mem::forget(df);

        let reopened =
            MmapDataFile::open_with(&path, true, random_opts()).expect("reopen after crash");
        assert!(reopened.opened_unclean(), "an unsealed in-place write must reopen unclean");
    }

    /// With `defer_seal_retire`, a writable open of a sealed file keeps its sentinel on disk until
    /// the first write: a drop or a crash with nothing written leaves the file byte-identical and
    /// sealed. The first write (here in place, which never overwrites the sentinel itself) retires
    /// it on disk, so a crash after it reopens unclean.
    #[test]
    fn deferred_seal_retire_keeps_the_seal_until_the_first_write() {
        let tmp = TempDir::with_prefix("mmap_df_defer_seal").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(100);
        {
            let mut df = MmapDataFile::open_with(&path, false, random_opts()).expect("open");
            df.write_all(&data).expect("write"); // clean close on drop seals the file
        }
        let sealed = std::fs::read(&path).expect("read file");
        assert_eq!(sealed.len() as u64, data.len() as u64 + SENTINEL_LEN, "precondition: sealed");
        // An old modification time, which any write (a re-seal's truncate and sentinel included)
        // would move to now, whatever the clock's granularity.
        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1 << 20);
        File::options().write(true).open(&path).expect("open").set_modified(old).expect("mtime");
        let modified = || std::fs::metadata(&path).expect("meta").modified().expect("mtime");
        let opts = MmapFileOptions { defer_seal_retire: true, ..random_opts() };

        for crash in [false, true] {
            let mut df = MmapDataFile::open_with(&path, false, opts).expect("reopen rw");
            assert!(!df.opened_unclean(), "a sealed file reopens clean");
            assert_eq!(std::fs::read(&path).expect("read file"), sealed, "the sentinel stays");
            assert!(!df.stamp_commit_marker_at(df.len()), "no marker over a pending sentinel");
            df.sync_all().expect("a barrier with nothing written");
            if crash {
                std::mem::forget(df);
            } else {
                drop(df);
            }
            assert_eq!(
                std::fs::read(&path).expect("read file"),
                sealed,
                "byte-identical with nothing written (crash: {crash})"
            );
            assert_eq!(modified(), old, "never written, not even re-sealed (crash: {crash})");
        }

        let mut df = MmapDataFile::open_with(&path, false, opts).expect("reopen rw");
        df.seek(SeekFrom::Start(10)).expect("seek");
        df.write_all(&[0xAB; 5]).expect("in-place write");
        assert_eq!(
            &std::fs::read(&path).expect("read file")[data.len()..],
            &[0_u8; SENTINEL_LEN as usize],
            "the first write retired the sentinel on disk"
        );
        // Crash: the handle never runs its clean-close seal.
        std::mem::forget(df);
        let reopened = MmapDataFile::open_with(&path, true, opts).expect("reopen after crash");
        assert!(reopened.opened_unclean(), "an unsealed in-place write must reopen unclean");
    }

    #[test]
    fn rewind_to_rolls_back_logical_end_without_physical_truncate() {
        let tmp = TempDir::with_prefix("mmap_df_rewind").expect("temp dir");
        let path = tmp.path().join("data");
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&pattern(200)).expect("write");
        assert_eq!(df.len(), 200);
        let phys_before = std::fs::metadata(&path).expect("meta").len();

        // Roll the logical end back to 80 — no physical truncate (capacity/padding unchanged).
        df.rewind_to(80);
        assert_eq!(df.len(), 80, "logical end moved back");
        assert_eq!(
            std::fs::metadata(&path).expect("meta").len(),
            phys_before,
            "rewind_to must not physically truncate the file"
        );
        assert!(df.slice(80, 10).is_none(), "reads are bounded to the rewound end");

        // A subsequent (shorter) append lands exactly at the rewound end.
        df.seek(SeekFrom::End(0)).expect("seek end");
        df.write_all(&pattern(20)).expect("append after rewind");
        assert_eq!(df.len(), 100);
        drop(df); // clean close truncates the padding and seals

        // Reopen: exactly [first 80 kept bytes][20 appended bytes]; no stale tail from [80, 200).
        let mut df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(!df.opened_unclean());
        assert_eq!(df.len(), 100);
        let mut buf = vec![0u8; 100];
        df.read_exact(&mut buf).expect("read");
        assert_eq!(&buf[..80], &pattern(80)[..], "kept prefix survives");
        assert_eq!(&buf[80..], &pattern(20)[..], "append landed at the rewound end");
    }

    #[test]
    fn rewind_to_clears_a_multi_chunk_torn_tail() {
        // Drive the chunked skip-zero loop in `rewind_to` across several 64 KiB chunks: a torn tail
        // spanning multiple chunks must be fully cleared (no stale bytes survive a re-append +
        // reopen), exercising the chunk bounds beyond the single-chunk case above.
        let tmp = TempDir::with_prefix("mmap_df_rewind_big").expect("temp dir");
        let path = tmp.path().join("data");
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        const BIG: usize = 200 * 1024; // > three 64 KiB rewind chunks
        df.write_all(&pattern(BIG)).expect("write");
        assert_eq!(df.len(), BIG as u64);

        df.rewind_to(100);
        assert_eq!(df.len(), 100, "logical end moved back across many chunks");

        // Re-append and reopen: exactly [kept 100][appended 40], nothing from the cleared tail.
        df.seek(SeekFrom::End(0)).expect("seek end");
        df.write_all(&pattern(40)).expect("append after rewind");
        drop(df);
        let mut df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(!df.opened_unclean());
        assert_eq!(df.len(), 140);
        let mut buf = vec![0u8; 140];
        df.read_exact(&mut buf).expect("read");
        assert_eq!(&buf[..100], &pattern(100)[..], "kept prefix survives");
        assert_eq!(&buf[100..], &pattern(40)[..], "append landed at the rewound end");
    }

    #[test]
    fn empty_clean_close_writes_no_sentinel() {
        let tmp = TempDir::with_prefix("mmap_df_empty").expect("temp dir");
        let path = tmp.path().join("data");
        {
            let _df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            // No writes: a fresh 0-length file has nothing to seal.
        }
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), 0, "empty file stays 0 bytes");
        let df = MmapDataFile::open(&path, false).expect("reopen empty");
        assert_eq!(df.len(), 0);
        assert!(!df.opened_unclean(), "a fresh/empty file is not unclean");
    }

    #[test]
    fn try_clone_returns_end_without_truncating() {
        let tmp = TempDir::with_prefix("mmap_df_clone").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(200);
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        // The mmap backend pads the physical file past the logical end.
        let phys_padded = std::fs::metadata(&path).expect("metadata").len();
        assert!(phys_padded > data.len() as u64, "precondition: file is padded past the data");

        // try_clone reports the logical end and does NOT truncate the physical padding.
        let (mut clone, end) = df.try_clone().expect("clone");
        assert_eq!(end, data.len() as u64, "clone reports the logical end");
        assert_eq!(
            std::fs::metadata(&path).expect("metadata").len(),
            phys_padded,
            "try_clone must not shrink the physical file"
        );
        // A consumer bounded to `end` (the PackIter/raw_iter path) reads exactly the written bytes.
        clone.seek(SeekFrom::Start(0)).expect("seek clone");
        let mut bounded = vec![0u8; end as usize];
        clone.read_exact(&mut bounded).expect("bounded read");
        assert_eq!(bounded, data, "bytes [0, end) are exactly the written data");
        // Reading past `end` to physical EOF would hit the padding (the hazard readers must avoid).
        let mut rest = Vec::new();
        clone.read_to_end(&mut rest).expect("read padding");
        assert!(rest.iter().all(|&b| b == 0), "bytes past end are zero padding");
    }

    /// `try_clone` msyncs only the dirty tail: after a barrier it has nothing to flush, after an
    /// append it flushes (and advances the watermark over) just that tail, and either way the clone
    /// reads exactly the logical bytes.
    #[test]
    fn try_clone_flushes_only_the_dirty_tail() {
        let tmp = TempDir::with_prefix("mmap_df_clone_tail").expect("temp dir");
        let mut df =
            MmapDataFile::open_with(tmp.path().join("data"), false, tiny_opts()).expect("open");
        let first = pattern(100);
        df.write_all(&first).expect("write");
        df.sync_all().expect("barrier");
        assert_eq!(df.flushed_end.load(Ordering::Relaxed), 100);

        let read_all = |clone: &mut DataFileReader, len: u64| {
            let mut buf = vec![0_u8; len as usize];
            clone.read_exact(&mut buf).expect("read clone");
            buf
        };
        let (mut clone, end) = df.try_clone().expect("clone a clean file");
        assert_eq!(end, 100);
        assert_eq!(read_all(&mut clone, end), first);
        assert_eq!(df.flushed_end.load(Ordering::Relaxed), 100, "nothing was dirty");

        let second = pattern(40);
        df.write_all(&second).expect("append");
        let (mut clone, end) = df.try_clone().expect("clone after an append");
        assert_eq!(end, 140);
        assert_eq!(df.flushed_end.load(Ordering::Relaxed), 140, "the dirty tail was flushed");
        assert_eq!(read_all(&mut clone, end), [first, second].concat());
    }

    /// After a failed `remap` releases the mapping (`Backing::Empty`) with `end > 0`,
    /// `try_clone`/`flush_dirty` must not lie about durability. `try_clone` must not advance the
    /// flush watermark (nothing was msync'd), an async `flush_dirty(false)` stays a no-op, and
    /// a durable `flush_dirty(true)` must fall back to a real `fsync` before advancing the
    /// watermark (so a later clean-close sentinel is truthful).
    #[test]
    fn failed_remap_flush_and_clone_do_not_lie_about_durability() {
        use std::sync::atomic::Ordering;

        let tmp = TempDir::with_prefix("mmap_df_remap_fail").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(40); // < initial_size (64): a single mapping, no grow
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        let end = data.len() as u64;
        assert_eq!(df.end, end);

        // Simulate a `remap` that failed and released the mapping, leaving `end > 0` unmapped.
        df.backing = Backing::Empty;

        // try_clone with no live mapping must NOT advance the watermark (nothing was flushed).
        df.flushed_end.store(0, Ordering::Relaxed);
        let (_clone, cloned_end) = df.try_clone().expect("clone succeeds");
        assert_eq!(cloned_end, end, "clone still reports the logical end");
        assert_eq!(
            df.flushed_end.load(Ordering::Relaxed),
            0,
            "try_clone must not advance the watermark when there is no mapping to msync"
        );

        // An async flush is a no-op on a lost mapping: no false durability, no watermark advance.
        df.flush_dirty(false).expect("async flush ok");
        assert_eq!(
            df.flushed_end.load(Ordering::Relaxed),
            0,
            "async flush must not advance the watermark with no mapping"
        );

        // A durable flush must fall back to a real fsync and only then advance the watermark.
        df.flush_dirty(true).expect("sync flush ok");
        assert_eq!(
            df.flushed_end.load(Ordering::Relaxed),
            end,
            "flush_dirty(true) must fsync the page-cache tail and advance the watermark"
        );
    }

    /// A file opened unclean and dropped WITHOUT a successful recovery (`mark_consistent`) must NOT
    /// be sealed — the padded/torn tail is left intact so the next open re-runs recovery. Sealing
    /// an unclean file would turn a truncatable tail into a permanent CorruptPack.
    #[test]
    fn unclean_open_without_recovery_is_not_sealed_on_drop() {
        let tmp = TempDir::with_prefix("mmap_df_unclean_no_seal").expect("temp dir");
        let path = tmp.path().join("data");
        // A crashed writer left logical data followed by zero padding and no sentinel.
        let data = pattern(80);
        let mut raw = data.clone();
        raw.extend(std::iter::repeat_n(0u8, 40));
        std::fs::write(&path, &raw).expect("write padded file");
        let phys_before = raw.len() as u64;

        {
            let df = MmapDataFile::open(&path, false).expect("open padded rw");
            assert!(df.opened_unclean(), "precondition: opened unclean");
            // Drop without `mark_consistent`: must leave the file untouched (no truncate, no seal).
        }
        assert_eq!(
            std::fs::metadata(&path).expect("meta").len(),
            phys_before,
            "an un-recovered unclean file must not be truncated or sentinel-sealed on drop"
        );
        let df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(df.opened_unclean(), "an un-recovered unclean file must stay unclean across drop");
    }

    /// After a successful recovery, `mark_consistent` clears the unclean flag so the clean `Drop`
    /// re-seals the (trimmed) file — the recovered pack does not replay on every restart.
    #[test]
    fn mark_consistent_reseals_on_drop() {
        let tmp = TempDir::with_prefix("mmap_df_mark_clean").expect("temp dir");
        let path = tmp.path().join("data");
        // Unclean padded file: 80 real bytes + 40 zero padding, no sentinel.
        let data = pattern(80);
        let mut raw = data.clone();
        raw.extend(std::iter::repeat_n(0u8, 40));
        std::fs::write(&path, &raw).expect("write padded file");

        {
            let mut df = MmapDataFile::open(&path, false).expect("open padded rw");
            assert!(df.opened_unclean(), "precondition: opened unclean");
            // Stand in for a successful recovery: trim to the last good record, then mark
            // consistent.
            df.rewind_to(80);
            df.mark_consistent();
            assert!(!df.opened_unclean(), "mark_consistent clears the unclean flag");
        } // clean Drop: truncate the padding to 80 and append the sentinel.
        assert_eq!(
            std::fs::metadata(&path).expect("meta").len(),
            80 + SENTINEL_LEN,
            "a recovered+marked file is sealed to exactly its data plus the sentinel"
        );
        let file_bytes = std::fs::read(&path).expect("read raw");
        assert_eq!(&file_bytes[80..], &clean_close_sentinel(80));
        let df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(!df.opened_unclean(), "a recovered+marked file reopens clean");
        assert_eq!(df.len(), 80);
    }

    /// Once poisoned by a write/sync/remap failure, `write` and the sync entry points fail closed
    /// (no errseq laundering) and `Drop` skips the seal, so the next open re-runs recovery rather
    /// than trusting a possibly-non-durable tail.
    #[test]
    fn poison_blocks_writes_and_skips_seal() {
        let tmp = TempDir::with_prefix("mmap_df_poison").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(80);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
            // Simulate a durability failure mid-session (a real msync/fsync/remap error latches
            // this same flag in production via `poison`).
            df.poison();
            assert!(df.is_poisoned());
            assert!(df.write(&pattern(10)).is_err(), "writes must fail closed after poison");
            assert!(df.sync_all().is_err(), "syncs must fail (no retry) after poison");
        } // Drop: poisoned → must NOT seal.
        let df = MmapDataFile::open(&path, false).expect("reopen");
        assert!(
            df.opened_unclean(),
            "a poisoned handle must leave the file unsealed so the next open recovers"
        );
    }

    /// A barrier that fails through a shared [`MapView`] on another thread (as tndb's commit
    /// thread syncs its log) poisons the owning file without the owner calling `poison`: its own
    /// barriers then refuse, and its `Drop` leaves the file unsealed.
    #[test]
    fn failed_view_msync_poisons_the_owner() {
        let tmp = TempDir::with_prefix("mmap_df_view_poison").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(80);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
            let view = df.view();
            view.publish_len(data.len() as u64);
            std::thread::spawn(move || {
                FAIL_NEXT_VIEW_MSYNC.set(true);
                assert!(view.msync(0, data.len() as u64).is_err(), "the injected failure");
            })
            .join()
            .expect("sync thread");
            assert!(df.is_poisoned(), "a failed view barrier poisons the owner");
            assert!(df.sync_all().is_err(), "the owner's barriers refuse after it");
        } // Drop: poisoned → must NOT seal.
        let df = MmapDataFile::open(&path, true).expect("reopen");
        assert!(df.opened_unclean(), "a file whose barrier failed is not sealed");
    }

    /// An explicit [`MmapDataFile::seal`] reports what `Drop` can only log. A good seal leaves the
    /// file sealed and closes the handle (its `Drop` changes nothing, a second seal is refused); a
    /// failed final fsync comes back as an error although the file then reads back sealed (the
    /// sentinel is in the page cache), which is why an owner must not infer the seal from a
    /// read-back; and a file `Drop` would leave unsealed (poisoned, or opened unclean and not
    /// recovered) is refused with nothing written.
    #[test]
    fn seal_reports_its_outcome() {
        let tmp = TempDir::with_prefix("mmap_df_seal").expect("temp dir");
        let data = pattern(80);
        let sealed_len = data.len() as u64 + SENTINEL_LEN;

        let good = tmp.path().join("good");
        let mut df = MmapDataFile::open_with(&good, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        df.seal().expect("a healthy file seals");
        assert!(df.is_closed());
        assert_eq!(std::fs::metadata(&good).expect("stat").len(), sealed_len, "truncated, sealed");
        let on_disk = std::fs::read(&good).expect("read");
        assert!(df.seal().is_err(), "a second seal is refused");
        let barriers = SYNC_BARRIERS.get();
        drop(df);
        assert_eq!(SYNC_BARRIERS.get(), barriers, "Drop ran no second seal");
        assert_eq!(std::fs::read(&good).expect("read"), on_disk, "Drop changed nothing");
        let reopened = MmapDataFile::open_with(&good, true, tiny_opts()).expect("reopen");
        assert!(!reopened.opened_unclean());
        assert_eq!(reopened.len(), data.len() as u64);

        let fsync_failed = tmp.path().join("fsync_failed");
        let mut df = MmapDataFile::open_with(&fsync_failed, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        df.fail_next_seal_sync_for_test();
        let err = df.seal().expect_err("a failed final fsync fails the seal");
        assert!(err.to_string().contains("fsync"), "{err}");
        let barriers = SYNC_BARRIERS.get();
        drop(df);
        assert_eq!(SYNC_BARRIERS.get(), barriers, "Drop does not retry a failed seal");
        let reopened = MmapDataFile::open_with(&fsync_failed, true, tiny_opts()).expect("reopen");
        assert!(!reopened.opened_unclean(), "the read-back cannot see the failed fsync");

        let poisoned = tmp.path().join("poisoned");
        let mut df = MmapDataFile::open_with(&poisoned, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        df.poison();
        let err = df.seal().expect_err("a poisoned file is not sealed");
        assert!(err.to_string().contains("poisoned"), "{err}");
        assert!(!df.is_closed(), "a refused seal leaves the handle to its Drop");
        drop(df);
        let mut df = MmapDataFile::open_with(&poisoned, false, tiny_opts()).expect("reopen");
        assert!(df.opened_unclean(), "the refused seal wrote nothing");
        let err = df.seal().expect_err("an unrecovered unclean file is not sealed");
        assert!(err.to_string().contains("unclean"), "{err}");
        drop(df);
        let reopened = MmapDataFile::open_with(&poisoned, true, tiny_opts()).expect("reopen");
        assert!(reopened.opened_unclean(), "still unsealed");
    }

    #[test]
    fn rename_moves_file() {
        let tmp = TempDir::with_prefix("mmap_df_rename").expect("temp dir");
        let src = tmp.path().join("data");
        let dst = tmp.path().join("moved");
        let data = pattern(64);
        let mut df = MmapDataFile::open_with(&src, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        df.rename(&dst).expect("rename");
        assert_eq!(df.path(), dst.as_path());
        // Data still readable through the (moved) handle.
        df.seek(SeekFrom::Start(0)).expect("seek");
        let mut buf = vec![0u8; 64];
        df.read_exact(&mut buf).expect("read");
        assert_eq!(buf, data);
        drop(df);
        assert!(!src.exists(), "source path removed");
        assert!(dst.exists(), "destination present");
    }

    #[test]
    fn delete_removes_on_drop() {
        let tmp = TempDir::with_prefix("mmap_df_delete").expect("temp dir");
        let path = tmp.path().join("data");
        let mut df = MmapDataFile::open(&path, false).expect("open");
        df.write_all(&pattern(16)).expect("write");
        assert!(path.exists());
        df.delete();
        assert!(!path.exists(), "file removed on delete-drop");
    }

    #[test]
    fn ro_refresh_sees_growth() {
        let tmp = TempDir::with_prefix("mmap_df_refresh").expect("temp dir");
        let path = tmp.path().join("data");
        // Writer 1: 50 bytes, clean close (exact length).
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&pattern(50)).expect("write");
        }
        let mut ro = MmapDataFile::open(&path, true).expect("open ro");
        assert_eq!(ro.len(), 50);
        // Writer 2: append 30 more, clean close.
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("reopen rw");
            df.seek(SeekFrom::End(0)).expect("seek end");
            df.write_all(&pattern(30)).expect("append");
        }
        ro.refresh_data_file_end().expect("refresh");
        assert_eq!(ro.len(), 80, "ro handle observes the writer's growth");
        ro.seek(SeekFrom::Start(0)).expect("seek");
        let mut buf = vec![0u8; 80];
        ro.read_exact(&mut buf).expect("read");
        assert_eq!(&buf[..50], &pattern(50)[..]);
    }

    /// A read-only handle bounded to the logical length never reads the writer's capacity padding,
    /// so a later writer truncation (which removes that padding) cannot deliver SIGBUS: bounded
    /// reads stay within the committed region that survives the truncation.
    #[test]
    fn ro_read_bound_keeps_reads_below_a_later_truncation() {
        let tmp = TempDir::with_prefix("mmap_df_bound").expect("temp dir");
        let path = tmp.path().join("data");
        // Writer stays LIVE (not dropped), so the physical file keeps its capacity padding.
        let mut w = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open writer");
        w.write_all(&pattern(50)).expect("write");
        w.sync_all().expect("sync");
        let physical = std::fs::metadata(&path).expect("metadata").len();
        assert!(
            physical > 50,
            "a live writer file must be padded past its logical data ({physical})"
        );

        // A read-only handle adopts the padded physical length...
        let mut ro = MmapDataFile::open(&path, true).expect("open ro");
        assert_eq!(ro.len(), physical);
        // ...clamp its read bound to the logical data length.
        ro.set_read_bound(50);
        assert_eq!(ro.len(), 50, "the read bound clamps len to the logical end");
        // `set_read_bound` never grows the bound.
        ro.set_read_bound(physical);
        assert_eq!(ro.len(), 50, "the read bound never grows back toward the padding");
        // A slice past the bound is a clean `None`, never a fault into the padding.
        assert!(ro.slice(0, 50).is_some());
        assert!(ro.slice(0, 51).is_none(), "reads never exceed the clamped bound");
        assert!(ro.slice(50, 1).is_none());

        // The writer truncates the padding away on clean close. The read-only handle, bounded to
        // `[0, 50)`, still reads the committed bytes and never touches a now-truncated page.
        drop(w);
        assert_eq!(
            std::fs::metadata(&path).expect("metadata").len(),
            50 + SENTINEL_LEN,
            "clean close truncates padding and appends the sentinel"
        );
        ro.seek(SeekFrom::Start(0)).expect("seek");
        let mut buf = vec![0u8; 50];
        ro.read_exact(&mut buf).expect("bounded read survives the truncation");
        assert_eq!(&buf[..], &pattern(50)[..]);
    }

    #[test]
    fn segment_mode_unsupported_on_rollover() {
        let tmp = TempDir::with_prefix("mmap_df_segment").expect("temp dir");
        let path = tmp.path().join("data");
        let opts = MmapFileOptions {
            initial_size: 128,
            max_map_size: 128,
            grow_mode: GrowMode::Segment,
            write_mode: WriteMode::Append,
            access: MmapAccess::Normal,
            derived: false,
            reserve: 0,
            max_record_size: None,
            defer_seal_retire: false,
            drop_cache_on_unclean_open: false,
        };
        let mut df = MmapDataFile::open_with(&path, false, opts).expect("open");
        // Fits within the first mapping (<= max_map_size).
        df.write_all(&pattern(100)).expect("first write fits");
        // Next append would require growing past max_map_size -> segment rollover, not yet
        // supported.
        let err = df.write_all(&pattern(100)).expect_err("rollover must error");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn sync_all_and_sync_disk_persist() {
        let tmp = TempDir::with_prefix("mmap_df_sync").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(300); // spans a growth so sync_disk covers a size extension
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        df.sync_all().expect("msync (default)");
        df.sync_disk().expect("full fsync");
        // Data still intact after both syncs.
        df.seek(SeekFrom::Start(0)).expect("seek");
        let mut buf = vec![0u8; 300];
        df.read_exact(&mut buf).expect("read");
        assert_eq!(buf, data);
        // The physical file is at least the logical length (padding may make it larger until
        // close).
        assert!(std::fs::metadata(&path).expect("meta").len() >= 300);
    }

    /// The platform sync helpers work on a file and on a directory (this platform's branch is the
    /// one compiled here), and a path that cannot be opened is an error, not a silent success.
    #[test]
    fn fsync_helpers_sync_files_and_directories() {
        let tmp = TempDir::with_prefix("mmap_df_fsync_helpers").expect("temp dir");
        let path = tmp.path().join("file");
        std::fs::write(&path, b"bytes").expect("write");
        let file = File::open(&path).expect("open file");
        fsync_file(&file).expect("fsync a file");
        fdatasync_file(&file).expect("fdatasync a file");
        fsync_file(&File::open(tmp.path()).expect("open dir")).expect("fsync a directory");
        fsync_directory(tmp.path()).expect("fsync_directory");
        assert!(fsync_directory(&tmp.path().join("missing")).is_err(), "a missing path errors");
    }

    /// `create_dir_synced` syncs the directory holding every level it creates, not only the leaf's
    /// parent, and a failed sync is an error.
    #[test]
    fn create_dir_synced_syncs_every_created_level() {
        let tmp = TempDir::with_prefix("mmap_df_create_dir_synced").expect("temp dir");
        let root = tmp.path().to_path_buf();
        let leaf = root.join("a").join("b").join("c");
        for holder in [root.clone(), root.join("a"), root.join("a").join("b")] {
            let _ = std::fs::remove_dir_all(root.join("a"));
            FAIL_DIR_SYNC.set(Some(holder.clone()));
            let synced = create_dir_synced(&leaf);
            FAIL_DIR_SYNC.set(None);
            assert!(synced.is_err(), "a failed sync of {holder:?} must fail the create");
        }
        std::fs::remove_dir_all(root.join("a")).expect("remove");
        create_dir_synced(&leaf).expect("create every level");
        assert!(leaf.is_dir());
        // An existing directory syncs only its parent.
        FAIL_DIR_SYNC.set(Some(root.clone()));
        let synced = create_dir_synced(&leaf);
        FAIL_DIR_SYNC.set(None);
        synced.expect("an existing directory syncs only its parent");
    }

    /// A writable open that creates a file syncs its directory first and fails when that sync
    /// fails; a retried open, finding the file present but empty, syncs again. A file holding data
    /// and a read-only open skip the sync.
    #[test]
    fn writable_open_of_a_new_file_syncs_its_directory() {
        let tmp = TempDir::with_prefix("mmap_df_open_dir_sync").expect("temp dir");
        let path = tmp.path().join("new.dat");
        FAIL_DIR_SYNC.set(Some(tmp.path().to_path_buf()));
        let created = MmapDataFile::open(&path, false).map(drop);
        let retried = MmapDataFile::open(&path, false).map(drop);
        FAIL_DIR_SYNC.set(None);
        assert!(created.is_err(), "a failed directory sync fails the create");
        assert!(retried.is_err(), "the retried open of the empty file syncs again");
        let mut df = MmapDataFile::open(&path, false).expect("open");
        df.write_all(b"data").expect("write");
        drop(df);
        FAIL_DIR_SYNC.set(Some(tmp.path().to_path_buf()));
        let writable = MmapDataFile::open(&path, false).map(drop);
        let read_only = MmapDataFile::open(&path, true).map(drop);
        FAIL_DIR_SYNC.set(None);
        writable.expect("a file holding data skips the directory sync");
        read_only.expect("a read-only open skips the directory sync");
    }

    /// The append `msync` watermark must never drop earlier-synced data. Sync, append past the
    /// watermark and sync again, then append once more and let only the clean-close `Drop` flush
    /// it (crossing growth boundaries throughout) — every byte must survive a reopen.
    #[test]
    fn incremental_append_sync_persists_all() {
        let tmp = TempDir::with_prefix("mmap_df_incremental_sync").expect("temp dir");
        let path = tmp.path().join("data");
        let a = pattern(300); // crosses the tiny growth boundary
        let b = pattern(150);
        let c = pattern(90);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&a).expect("write a");
            df.sync_all().expect("sync a");
            // Append past the synced watermark, then flush only the new tail.
            df.write_all(&b).expect("write b");
            df.sync_all().expect("sync b");
            // A final append left for the clean-close Drop to flush.
            df.write_all(&c).expect("write c");
        } // Drop flushes the remaining tail, truncates the padding, and fsyncs.
        let total = a.len() + b.len() + c.len();
        let mut df = MmapDataFile::open(&path, true).expect("reopen ro");
        assert_eq!(df.len(), total as u64);
        let mut all = vec![0u8; total];
        df.read_exact(&mut all).expect("read all");
        assert_eq!(&all[..a.len()], &a[..], "first synced segment");
        assert_eq!(&all[a.len()..a.len() + b.len()], &b[..], "tail synced after watermark");
        assert_eq!(&all[a.len() + b.len()..], &c[..], "segment flushed only by Drop");
    }
}
