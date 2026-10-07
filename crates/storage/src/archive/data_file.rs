//! The mmap-backed, append-only data file behind every [`Pack`](crate::archive::pack::Pack) (its
//! data log and position index).
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
//! ## Durability
//!
//! The default barrier `MmapDataFile::sync_all` is `msync` (flush dirty pages to the backing store)
//! — this is sufficient for data written within an already-fsync'd file size. `msync` does not
//! persist a size extension, so a growth marks the size unsynced and the next barrier fsyncs it
//! once after its `msync` (a barrier with no growth since the last stays `msync`-only). A
//! [`derived`](MmapFileOptions::derived) index skips that fsync and leaves its size to the
//! clean-close seal: its owner rebuilds it from the data log after any unclean open anyway.
//! `MmapDataFile::sync_disk` is the full,
//! slower `msync` + `fsync`, which additionally persists the file's size/metadata. (On macOS
//! `fsync` is not a full power-loss barrier — that needs `F_FULLFSYNC`; the real win of the msync
//! default is on Linux.)
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
    File::open(path)?.sync_all()
}

/// Create `dir` (and any missing parents) and fsync its parent so the new directory
/// entry survives a crash. Best-effort fsync; a redundant fsync when `dir` already
/// exists is harmless.
pub(crate) fn create_dir_synced(dir: &Path) -> Result<(), io::Error> {
    std::fs::create_dir_all(dir)?;
    if let Some(parent) = dir.parent() {
        let _ = fsync_directory(parent);
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
    /// because an unsealed derived file is never trusted.
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
        }
    }
}

/// A reader's view of a file's mapping that needs no lock: the mapping's base address and length,
/// plus a length the owner has published as fully written. Shared (`Arc`) between the file and its
/// lock-free readers; the file updates the mapping fields whenever its mapping changes, and the
/// owner advances [`Self::publish_len`].
///
/// Validity: a slice from [`Self::slice`] borrows mapped memory. The owner must keep the file open
/// while any reader can reach the view, and a mapping replaced while the view is shared is retired
/// (kept mapped until the file drops) rather than unmapped, so a reader that loaded the old base is
/// still reading live memory. Mapping changes only ever grow the mapping while readers exist (a
/// reservation overflow); anything that shrinks or unmaps happens with no readers (open-time
/// recovery, close).
#[derive(Debug)]
pub(crate) struct MapView {
    base: AtomicPtr<u8>,
    mapped: AtomicU64,
    published: AtomicU64,
}

impl MapView {
    fn new() -> Self {
        Self {
            base: AtomicPtr::new(std::ptr::null_mut()),
            mapped: AtomicU64::new(0),
            published: AtomicU64::new(0),
        }
    }

    /// Point the view at a mapping. The base is stored before the length, and a reader loads the
    /// length first: one that sees the new length then sees the new base, and one that sees the
    /// old length reads a prefix of the (only ever larger) new mapping or the retired old one.
    fn set_mapping(&self, base: *const u8, len: u64) {
        self.base.store(base.cast_mut(), Ordering::Release);
        self.mapped.store(len, Ordering::Release);
    }

    /// Publish `len` bytes as fully written and readable (they must already be in the mapping and
    /// never change while readers can see them).
    pub(crate) fn publish_len(&self, len: u64) {
        self.published.store(len, Ordering::Release);
    }

    /// Borrow `[offset, offset + len)` if it lies within the published bytes, else `None`.
    pub(crate) fn slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        self.tail(offset)?.get(..len)
    }

    /// Borrow every published byte from `offset` on (`None` past the published end): one bounds
    /// check for a reader that learns a record's length from its own prefix.
    pub(crate) fn tail(&self, offset: u64) -> Option<&[u8]> {
        let end = self.published.load(Ordering::Acquire).min(self.mapped.load(Ordering::Acquire));
        let len = end.checked_sub(offset)?;
        let base = self.base.load(Ordering::Acquire);
        if base.is_null() {
            return None;
        }
        // SAFETY: `[base, base + mapped)` is a live mapping (see the type docs: the owner keeps the
        // file open while readers can reach the view, and replaced mappings stay mapped until the
        // file drops), and `[offset, end)` lies within it and within the published bytes, which
        // the owner never modifies while readers can see them.
        Some(unsafe { std::slice::from_raw_parts(base.add(offset as usize), len as usize) })
    }
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

/// An mmap-backed, append-only data file — the storage behind every
/// [`Pack`](crate::archive::pack::Pack).
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
    /// Durable `committed_end` recovered from the tail commit marker of an unclean file, if a
    /// valid one is present (see [`Self::stamp_commit_marker`] / [`Self::committed_end`]).
    /// `None` on clean or fresh opens, or when no valid marker was written/flushed
    /// (best-effort). Recovery uses it as an index-free acked-data watermark to catch at-rest
    /// corruption of the last committed record.
    committed_marker: Option<u64>,
    /// Sticky "poison" latch set on any failed durability barrier (`msync`/`fsync`) or failed
    /// `remap`. Once set, `write`/`ensure_len` and the sync entry points return `Err` without
    /// retrying (a retried `msync` after a writeback error can spuriously succeed on Linux errseq,
    /// laundering lost data), `stamp_commit_marker` is a no-op, and `Drop` skips the clean-close
    /// seal so the next open re-runs recovery instead of trusting a possibly-non-durable tail.
    /// `AtomicBool` (not `bool`) so it can be set from the `&self` sync paths (`flush_dirty`,
    /// `sync_range`, `try_clone`) — same reason `flushed_end` is an `AtomicU64`. Never cleared for
    /// the life of the handle (a durability failure is permanent until reopen); in particular
    /// `mark_consistent` does NOT clear it, so recovery cannot paper over lost data.
    write_failed: AtomicBool,
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
    /// Test-only: how many deferred size fsyncs barriers have run.
    #[cfg(test)]
    size_syncs: std::sync::atomic::AtomicUsize,
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
            // Create the file if missing and fsync the parent so the entry is durable. A redundant
            // create attempt just fails and is ignored.
            if File::create_new(path).is_ok() {
                if let Some(parent) = path.parent() {
                    let _ = fsync_directory(parent);
                }
            }
        }
        let file = OpenOptions::new().read(true).write(!read_only).open(path)?;
        let orig_len = file.metadata()?.len();

        // A clean `Drop` appends an 8-byte sentinel at physical EOF (see `clean_close_sentinel`).
        // Detect it here: a valid sentinel means the file was sealed, so the logical data ends 8
        // bytes before physical EOF; a missing/invalid sentinel means the file was not closed
        // cleanly (most likely still padded), which `opened_unclean` surfaces so the pack's heal
        // path runs — the logical end is left at physical EOF for that scan.
        let (logical_end, opened_unclean) = detect_sentinel(&file, orig_len)?;

        // On an unclean file, recover a durable commit marker from the tail of the mmap capacity if
        // one is present (best-effort — see `stamp_commit_marker`). A clean/fresh file has none (a
        // clean close truncates the padding, and its own trailing bytes are the 8-byte clean-close
        // sentinel, not this 16-byte marker). Absent/torn/stale → `None` → the WAL probe decides.
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
        if !read_only && !opened_unclean && logical_end < orig_len {
            file.write_all_at(&[0_u8; SENTINEL_LEN as usize], logical_end)?;
            file.sync_data()?;
        }

        // Map only the bytes that already exist. A fresh (0-length) RW file is left unallocated
        // until the first write, so a crash before any data keeps it 0-length (and it reopens as
        // empty). Existing content is mapped as-is, including a clean file's 8 trailing bytes in
        // the `[end, capacity)` padding region (the sentinel on a read-only open; zeros on a
        // writable one, retired above); any trailing padding a crashed writer left is handled by
        // the pack's heal path.
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
            // error path catches. The pack upholds this: `get_static` serves the live epoch from
            // the writer handle (never a read-only map of it), and `open_static` runs
            // only on sealed epochs and is gated by `files_consistent`. As
            // defense-in-depth, the read bound (`end`) is additionally clamped to the
            // index-attested length via `set_read_bound`, so reads never touch bytes a
            // writer truncation could remove even if that discipline slipped.
            let map = unsafe { Mmap::map(&file)? };
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
            // Existing content is already durable on disk, so the sync fast path starts here.
            flushed_end: AtomicU64::new(logical_end),
            opened_unclean,
            committed_marker,
            write_failed: AtomicBool::new(false),
            size_unsynced: AtomicBool::new(false),
            reserved,
            view: Arc::new(MapView::new()),
            retired: Vec::new(),
            #[cfg(test)]
            size_syncs: std::sync::atomic::AtomicUsize::new(0),
            opts,
        };
        df.sync_view();
        df.advise_backing();
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
    /// Does NOT clear [`Self::write_failed`]: a durability failure (a failed `msync`/`fsync`/remap)
    /// independently suppresses the seal, so recovery can never paper over data that did not reach
    /// disk. A no-op signal, cheap to call unconditionally on every recovery success path.
    pub fn mark_consistent(&mut self) {
        self.opened_unclean = false;
    }

    /// Latch the sticky write/sync poison (see [`Self::write_failed`]). Callable behind `&self` so
    /// the `&self` sync paths can set it.
    fn poison(&self) {
        self.write_failed.store(true, Ordering::Relaxed);
    }

    /// True once any durability barrier or remap has failed on this handle.
    fn is_poisoned(&self) -> bool {
        self.write_failed.load(Ordering::Relaxed)
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
    /// of the mmap capacity. Best-effort: it dirties one padding page but adds NO sync — the
    /// durability barrier stays the caller's `sync_all` (`msync` of `[flushed_end, end)`),
    /// which does not cover this page, so the marker reaches disk via OS writeback or the next
    /// fsync (a barrier after a growth, or the clean close). That is enough for its purpose
    /// (at-rest rot is detected long after the persist).
    ///
    /// Fail-safe by construction: callers stamp *after* the data `msync`, so the marker can never
    /// be ahead of durable data — a crash leaves it behind or absent, never fabricating a
    /// watermark past real data. A no-op on a read-only handle, an empty file, or when the
    /// capacity padding cannot hold the marker without overlapping `[0, end)` (in which case it
    /// is simply skipped this time — the next stamp, after the next append grows capacity,
    /// records it).
    pub fn stamp_commit_marker(&mut self) {
        if self.read_only || self.end == 0 || self.is_poisoned() {
            return;
        }
        // Need `end + COMMIT_MARKER_LEN <= capacity` so the marker sits in the padding past the
        // data.
        let Some(marker_pos) = self.capacity.checked_sub(COMMIT_MARKER_LEN) else {
            return;
        };
        if marker_pos < self.end {
            return; // no headroom this persist; skip (fail-safe — falls back to the WAL probe)
        }
        if let Backing::Rw(map) = &mut self.backing {
            let marker = commit_marker(self.end);
            let pos = marker_pos as usize;
            map_range_mut(map, pos, COMMIT_MARKER_LEN as usize).copy_from_slice(&marker);
        }
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
    /// capacity padding past `end` is never exposed) or the file is currently unmapped. The
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
            Backing::Rw(map) => Some(&map[start..start + len]),
            Backing::Ro(map) => Some(&map[start..start + len]),
            // Only a zero-length borrow (necessarily at offset 0) is valid with no mapping.
            Backing::Empty if len == 0 => Some(&[]),
            Backing::Empty => None,
        }
    }

    /// Borrow the logical data from `offset` to the end (`None` past the end): see
    /// [`Self::slice`].
    pub fn tail(&self, offset: u64) -> Option<&[u8]> {
        self.slice(offset, usize::try_from(self.end.checked_sub(offset)?).ok()?)
    }

    /// Borrow the mapped bytes `[offset, offset + len)` directly, without copying — the building
    /// block for zero-copy record reads (no `memcpy`, no allocation).
    ///
    /// Returns `None` if the range falls outside the logical data `[0, len())` (so the transient
    /// capacity padding past `end` is never exposed) or the file is currently unmapped. The
    /// returned slice borrows `&mut self`, so no concurrent write/remap (which needs `&mut self`)
    /// can invalidate it while it is held. A caller that does not know a record's length up
    /// front reads the size prefix with one `slice` call and the value with another; passing
    /// `len = self.len() - offset` gives an offset-to-end view.
    pub fn slice_mut(&mut self, offset: u64, len: usize) -> Option<&mut [u8]> {
        let range_end = offset.checked_add(len as u64)?;
        if range_end > self.end {
            return None;
        }
        let start = offset as usize;
        match &mut self.backing {
            Backing::Rw(map) => Some(map_range_mut(map, start, len)),
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
    }

    /// Take the current mapping out of `backing`, retiring it (kept mapped until drop) when a
    /// lock-free reader may still hold its base, else letting it unmap.
    fn take_backing(&mut self) {
        let old = std::mem::replace(&mut self.backing, Backing::Empty);
        if Arc::strong_count(&self.view) > 1 && !matches!(old, Backing::Empty) {
            self.retired.push(old);
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
        // A failed resize/remap leaves the handle with no live mapping and unknown durability for
        // any tail dirtied through the released map; latch the poison so later writes/syncs fail
        // and `Drop` does not seal a file whose mapping was lost mid-flight.
        self.file.set_len(new_len).inspect_err(|_| self.poison())?;
        if new_len == 0 {
            return Ok(());
        }
        let map = if self.reserved > 0 {
            let map_len = new_len.max(self.reserved.saturating_mul(2));
            let len = usize::try_from(map_len)
                .map_err(io::Error::other)
                .inspect_err(|_| self.poison())?;
            // SAFETY: single-writer model; the file was sized to `new_len` immediately above, and
            // the pages of the reservation past it are never touched.
            let map = unsafe { MmapOptions::new().len(len).map_mut(&self.file) }
                .inspect_err(|_| self.poison())?;
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
    /// old sparse behaviour) so the file still functions.
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
        // Some filesystems (tmpfs, some network/older FS) don't implement fallocate; fall back to a
        // plain resize (sparse) rather than failing an otherwise-serviceable write.
        if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)) {
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
        } else {
            self.remap(new_cap)?;
        }
        self.size_unsynced.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// The fsync a growth deferred: if the file grew since the last fsync, persist its size (and,
    /// with it, everything written so far) before a durability barrier returns. Barriers call this
    /// after their `msync`, so a barrier promises what it always has (data `[0, end)` durable
    /// within a durable file size) while one with no growth since the last stays `msync`-only.
    /// A [`derived`](MmapFileOptions::derived) file skips it and leaves its size to the seal.
    fn sync_size_if_grown(&self) -> io::Result<()> {
        if self.size_unsynced.load(Ordering::Relaxed) && !self.opts.derived {
            self.file.sync_all().inspect_err(|_| self.poison())?;
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
    /// at open (see `open_with`). Never shrinks. Unlike [`Self::truncate`], growth is geometric
    /// (a remap only when a step crosses the current capacity), so repeated one-record
    /// extensions (e.g. the digest index adding a bucket per split) do not remap every call.
    pub fn ensure_len(&mut self, new_len: u64) -> io::Result<()> {
        if new_len <= self.end {
            return Ok(());
        }
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
    /// ftruncate/remap), use [`Self::rewind_to`] instead.
    pub fn truncate(&mut self, len: u64) -> io::Result<()> {
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
        if self.reserved > 0 && matches!(self.backing, Backing::Rw(_)) {
            // Shrink the file under the reserved mapping, which stays put; the pages past the new
            // EOF are never touched (and read back as zeros if a later growth re-extends the file).
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
    /// No-op on a read-only handle.
    pub fn rewind_to(&mut self, new_len: u64) {
        if self.read_only || new_len >= self.end {
            return;
        }
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
            for chunk in map_range_mut(map, new_len as usize, tail).chunks_mut(64 << 10) {
                if chunk.iter().any(|&b| b != 0) {
                    chunk.fill(0);
                }
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
    }

    /// Clone the underlying file handle, `msync`ing `[0, end)` first. That `flush_range` is a
    /// durability barrier (`MS_SYNC`), not merely a visibility hint: it writes the mapped region
    /// back to the file so the cloned fd reads committed bytes even on platforms where mmap
    /// stores and plain `read()` are not guaranteed coherent, and so a consumer copying through
    /// the clone gets durable data; it also advances `flushed_end`. Returns the clone together
    /// with the logical `end` at the moment of the call.
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
                // msync `[0, end)` back to the file: a durability barrier, and the coherence
                // guarantee for the clone's plain `read()`/copy syscalls. Advance the watermark
                // only when the msync actually ran -- if a prior `remap` failed and
                // left no live mapping, the tail is NOT durable and the watermark
                // must not claim otherwise. A failed barrier poisons the handle.
                map.flush_range(0, self.end as usize).inspect_err(|_| self.poison())?;
                self.flushed_end.store(self.end, Ordering::Relaxed);
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
                self.file.sync_all().inspect_err(|_| self.poison())?;
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
            // The tail is now durable; the next append-mode flush can start from here.
            self.flushed_end.store(self.end, Ordering::Relaxed);
            // And within a durable file size: a growth since the last fsync is synced now.
            self.sync_size_if_grown()?;
        } else {
            map.flush_async_range(start as usize, len).inspect_err(|_| self.poison())?;
        }
        Ok(())
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
        map.flush_range(start as usize, (end - start) as usize).inspect_err(|_| self.poison())
    }

    /// Full, slow disk sync: `msync` then `fsync`, additionally persisting the file's
    /// size/metadata. See the module docs for the macOS `F_FULLFSYNC` caveat.
    pub fn sync_disk(&self) -> io::Result<()> {
        if self.read_only {
            return Ok(());
        }
        self.flush_dirty(true)?;
        self.file.sync_all().inspect_err(|_| self.poison())?;
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
                    // truncation; pair with `set_read_bound` (see the read-only branch of
                    // `open_with`).
                    Backing::Ro(unsafe { Mmap::map(&self.file)? })
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

    /// Mark the file to be removed when this handle drops (instead of the flush+sync clean close).
    pub fn delete(mut self) {
        self.remove_on_drop = true;
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
    /// padding. An empty target buffer reads nothing.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.seek_pos >= self.end {
            return Ok(0);
        }
        let start = self.seek_pos as usize;
        let avail = (self.end - self.seek_pos) as usize;
        let n = avail.min(buf.len());
        match &self.backing {
            Backing::Rw(map) => buf[..n].copy_from_slice(&map[start..start + n]),
            Backing::Ro(map) => buf[..n].copy_from_slice(&map[start..start + n]),
            Backing::Empty => return Ok(0),
        }
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
        // to its failed state on it (see `pack.rs`), which the consensus actor treats as fatal and
        // reopens, where recovery replays the WAL and truncates the unacked tail.
        if self.is_poisoned() {
            return Err(io::Error::other(
                "data file poisoned by an earlier write/sync failure; refusing further writes",
            ));
        }
        if buf.is_empty() {
            return Ok(0);
        }
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
        if self.remove_on_drop {
            self.view.set_mapping(std::ptr::null(), 0);
            self.backing = Backing::Empty; // release the map before removing the file
            if let Err(e) = std::fs::remove_file(&self.path) {
                if !std::thread::panicking() {
                    tracing::error!("MmapDataFile: failed to remove file on drop: {e}");
                }
            }
            return;
        }
        if self.read_only {
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
        // Clean close: msync, truncate away the padding, then (only if the msync succeeded) append
        // an 8-byte clean-close sentinel and fsync so the on-disk file is exactly `end` data bytes
        // plus the sentinel and durable — a reopen validates the sentinel, strips it back to `end`,
        // and knows the file was sealed. A 0-length file is left empty (nothing to seal).
        let flushed = match self.flush_dirty(true) {
            Ok(()) => true,
            Err(e) => {
                if !std::thread::panicking() {
                    tracing::error!("MmapDataFile: failed to msync on drop: {e}");
                }
                false
            }
        };
        self.view.set_mapping(std::ptr::null(), 0);
        self.backing = Backing::Empty; // unmap before truncating
        if let Err(e) = self.file.set_len(self.end) {
            if !std::thread::panicking() {
                tracing::error!("MmapDataFile: failed to truncate on drop: {e}");
            }
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
                if !std::thread::panicking() {
                    tracing::error!(
                        "MmapDataFile: failed to write clean-close sentinel on drop: {e}"
                    );
                }
            }
        }
        if let Err(e) = self.file.sync_all() {
            if !std::thread::panicking() {
                tracing::error!("MmapDataFile: failed to fsync on drop: {e}");
            }
        }
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
        assert_eq!(view.slice(0, 100), None, "nothing published yet");
        view.publish_len(1_000);
        let old = view.slice(0, 1_000).expect("published slice");
        assert_eq!(old, &pattern(1_000)[..]);
        assert_eq!(view.slice(900, 101), None, "past the published length");
        assert_eq!(
            view.tail(900).map(<[u8]>::len),
            Some(100),
            "the tail stops at the published end"
        );
        assert_eq!(view.tail(1_000).map(<[u8]>::len), Some(0));
        assert_eq!(view.tail(1_001), None);

        df.write_all(&pattern(40_000)[1_000..]).expect("grow past the reservation");
        assert!(df.reserved >= 40_000, "re-reserved larger (reserved {})", df.reserved);
        assert_eq!(df.retired.len(), 1, "the old mapping is retired, not unmapped");
        assert_eq!(old, &pattern(1_000)[..], "a slice of the old mapping is still readable");
        view.publish_len(40_000);
        let new = view.slice(0, 40_000).expect("slice after the move");
        assert_eq!(new, &pattern(40_000)[..]);
        assert_ne!(new.as_ptr(), old.as_ptr(), "the view follows the new mapping");
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
