//! An mmap-backed, append-only data file — an API-compatible alternative to
//! [`crate::archive::data_file::DataFile`].
//!
//! Where `DataFile` uses buffered `read`/`write` syscalls, this maps the file into memory and does
//! reads/writes as `memcpy` against the mapping, so no per-IO syscall and no read/write buffers are
//! needed. It is a **drop-in** for `DataFile` (same public surface + trait impls) plus
//! mmap-specific open options, but is intentionally standalone (not wired into `pack.rs`) so it can
//! be validated in isolation and swapped in deliberately.
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
//! ## Transient padding, exact on exposure
//!
//! Because the file is sized ahead of the data, the physical file is padded to `capacity >= end`
//! while actively appending (`end` is the logical data length). The physical file is reconciled to
//! **exactly `end`** at every point an external consumer can observe it — [`Self::try_clone`] (for
//! `PackIter`/`raw_iter`, which read to EOF) and `Drop` (clean close) both truncate to `end` —
//! while our own reads are bounded by `end` and never see the padding. After a crash the file may
//! be left padded; the pack's CRC + `recover_pack` path truncates it back exactly as it does for
//! the buffered `DataFile`.
//!
//! ## Durability
//!
//! The default barrier [`Self::sync_all`] is `msync` (flush dirty pages to the backing store) —
//! this is sufficient for data written within an already-fsync'd file size, and each size extension
//! is fsync'd when it happens (in the grow path). [`Self::sync_disk`] is the full, slower `msync` +
//! `fsync`, which additionally persists the file's size/metadata. (On macOS `fsync` is not a full
//! power-loss barrier — that needs `F_FULLFSYNC`; the real win of the msync default is on Linux.)

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use memmap2::{Mmap, MmapMut};

use crate::archive::{data_file::fsync_directory, error::rename::RenameError};

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
}

impl Default for MmapFileOptions {
    fn default() -> Self {
        Self {
            initial_size: DEFAULT_INITIAL_SIZE,
            max_map_size: DEFAULT_MAX_MAP_SIZE,
            grow_mode: GrowMode::Reopen,
            write_mode: WriteMode::Append,
            access: MmapAccess::Normal,
        }
    }
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

/// An mmap-backed, append-only data file that is API-compatible with
/// [`crate::archive::data_file::DataFile`].
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
    /// Set by [`Self::try_clone`] after it truncates the file to `end`; the next write re-maps
    /// first. `AtomicBool` (not `Cell`) so the file stays `Sync`, letting a pack hold it
    /// behind a `Send + Sync` trait object.
    remap_needed: AtomicBool,
    /// High-water offset already `msync`'d to the backing store. The `WriteMode::Append` sync fast
    /// path flushes only the newly-written tail `[flushed_end, end)` instead of re-scanning the
    /// whole mapping each sync. Only meaningful for append (in-place `Random` writes can land
    /// below this offset, so that mode always flushes the full `[0, end)` range). `AtomicU64`
    /// for the same `Sync`-through-`&self`-`sync_all` reason as `remap_needed`.
    flushed_end: AtomicU64,
    opts: MmapFileOptions,
}

impl MmapDataFile {
    /// Open with default [`MmapFileOptions`] — the drop-in analogue of `DataFile::open`.
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
            // Create the file if missing and fsync the parent so the entry is durable (matches
            // `DataFile::open`). A redundant create attempt just fails and is ignored.
            if File::create_new(path).is_ok() {
                if let Some(parent) = path.parent() {
                    let _ = fsync_directory(parent);
                }
            }
        }
        let file = OpenOptions::new().read(true).write(!read_only).open(path)?;
        let orig_len = file.metadata()?.len();

        // Map only the bytes that already exist. A fresh (0-length) RW file is left unallocated
        // until the first write, so a crash before any data keeps it 0-length (and it reopens as
        // empty), matching the buffered `DataFile`. Existing content is mapped as-is; any trailing
        // padding a crashed writer left is handled by the pack's heal path.
        let (backing, capacity) = if orig_len == 0 {
            (Backing::Empty, 0)
        } else if read_only {
            // SAFETY: single-writer pack model — the file is not concurrently truncated/replaced.
            let map = unsafe { Mmap::map(&file)? };
            (Backing::Ro(map), orig_len)
        } else {
            // SAFETY: as above; we hold the file open for writing for this handle's lifetime.
            let map = unsafe { MmapMut::map_mut(&file)? };
            (Backing::Rw(map), orig_len)
        };

        let df = Self {
            file,
            path: path.to_owned(),
            backing,
            end: orig_len,
            capacity,
            seek_pos: 0,
            read_only,
            remove_on_drop: false,
            remap_needed: AtomicBool::new(false),
            // Existing content is already durable on disk, so the sync fast path starts here.
            flushed_end: AtomicU64::new(orig_len),
            opts,
        };
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
    /// front reads the size prefix with one `slice` call and the value with another; passing
    /// `len = self.len() - offset` gives an offset-to-end view.
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
            Backing::Rw(map) => Some(&mut map[start..start + len]),
            Backing::Ro(_map) => None,
            Backing::Empty => None,
        }
    }

    /// Next capacity that holds `needed` bytes: double until a step would exceed `max_map_size`,
    /// then advance in `max_map_size` increments. Floors the first allocation at `initial_size`.
    fn next_capacity(&self, needed: u64) -> u64 {
        let max_step = self.opts.max_map_size.max(1);
        let mut cap =
            if self.capacity == 0 { self.opts.initial_size.max(1) } else { self.capacity };
        while cap < needed {
            // Grow by min(cap, max_step): geometric early, linear once a step hits the cap.
            cap = cap.saturating_mul(2).min(cap.saturating_add(max_step));
        }
        cap
    }

    /// Apply the configured [`MmapAccess`] `madvise` hint to the current mapping. Best-effort: a
    /// failed hint is logged and ignored, and it is a no-op for `Normal`, an empty mapping, or a
    /// non-unix target.
    fn advise_backing(&self) {
        #[cfg(unix)]
        {
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
    }

    /// Drop the current mapping, resize the physical file to `new_len`, and re-map it (leaving it
    /// unmapped when `new_len == 0`). Never holds a mapping past EOF.
    fn remap(&mut self, new_len: u64) -> io::Result<()> {
        self.backing = Backing::Empty; // release any existing map before resizing
        self.file.set_len(new_len)?;
        if new_len == 0 {
            self.capacity = 0;
            return Ok(());
        }
        // SAFETY: single-writer model; the file was sized to `new_len` immediately above.
        let map = unsafe { MmapMut::map_mut(&self.file)? };
        self.backing = Backing::Rw(map);
        self.capacity = new_len;
        self.advise_backing();
        Ok(())
    }

    /// Grow the file to `new_cap` and fsync the size extension so data later `msync`'d into the
    /// grown region survives a crash (`msync` alone does not persist size growth).
    fn grow_to(&mut self, new_cap: u64) -> io::Result<()> {
        self.remap(new_cap)?;
        self.file.sync_all()?;
        // The fsync just persisted every dirty page (and the size), so all data `[0, end)` is now
        // durable; advance the append sync watermark so the next flush only covers new writes.
        self.flushed_end.store(self.end, Ordering::Relaxed);
        Ok(())
    }

    /// Ensure a writable mapping large enough for `[0, needed)`, growing (reopen-larger) if needed.
    fn ensure_capacity(&mut self, needed: u64) -> io::Result<()> {
        if self.remap_needed.load(Ordering::Relaxed) {
            // `try_clone` truncated the physical file to `end` and left a stale over-length map;
            // reflect the real physical size so the growth math is correct, then force a fresh map
            // below (writes always append, so `needed > end == capacity` and we take the grow
            // path).
            self.capacity = self.end;
            self.remap_needed.store(false, Ordering::Relaxed);
        }
        if needed <= self.capacity {
            return Ok(());
        }
        let new_cap = self.next_capacity(needed);
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
    /// The extended region reads as zeros — fresh file growth is zero-filled and the capacity
    /// padding past `end` is never written — so callers can treat it as freshly-zeroed space. Never
    /// shrinks. Unlike [`Self::set_len`], growth is geometric (a remap only when a step crosses the
    /// current capacity), so repeated one-record extensions (e.g. the digest index adding a bucket
    /// per split) do not remap every call.
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
        self.ensure_capacity(new_len)?;
        self.end = new_len;
        Ok(())
    }

    /// Truncate or extend the logical (and physical) file to `len`. Used by the pack heal/truncate
    /// path; leaves the physical file exactly `len` bytes.
    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                "file not open for write",
            ));
        }
        self.remap(len)?;
        self.end = len;
        self.remap_needed.store(false, Ordering::Relaxed);
        // Bytes past `len` are gone; clamp the watermark so a later append below the old high-water
        // is still flushed (bytes still present under `len` stay durable).
        let fe = self.flushed_end.load(Ordering::Relaxed).min(len);
        self.flushed_end.store(fe, Ordering::Relaxed);
        if self.seek_pos > len {
            self.seek_pos = len;
        }
        Ok(())
    }

    /// Clone the underlying file handle, first reconciling the physical file to the logical length
    /// so a consumer that reads to EOF (e.g. `PackIter` via `raw_iter`) sees exactly the written
    /// bytes with no trailing padding. The next write re-establishes a writable mapping.
    pub fn try_clone(&self) -> io::Result<File> {
        if !self.read_only {
            if self.end > 0 {
                if let Backing::Rw(map) = &self.backing {
                    // Flush dirty pages so the cloned handle observes current data.
                    map.flush_range(0, self.end as usize)?;
                }
                // The whole `[0, end)` region was just flushed durably.
                self.flushed_end.store(self.end, Ordering::Relaxed);
            }
            // Truncate away the capacity padding. `File::set_len` takes `&self`; the existing map
            // still spans the old capacity but `[end, capacity)` is never touched (reads are
            // bounded by `end`), and the next write re-maps first (see `remap_needed` /
            // `ensure_capacity`).
            self.file.set_len(self.end)?;
            self.remap_needed.store(true, Ordering::Relaxed);
        }
        self.file.try_clone()
    }

    /// `msync` the dirty region to the backing store. For [`WriteMode::Append`] this is only the
    /// tail written since the last durable flush (`[flushed_end, end)`); for [`WriteMode::Random`]
    /// it is the whole `[0, end)` range, since an in-place overwrite can land anywhere. `sync`
    /// picks a durable `MS_SYNC` (which then advances the append watermark) over a fire-and-forget
    /// `MS_ASYNC`.
    fn flush_dirty(&self, sync: bool) -> io::Result<()> {
        if self.read_only || self.end == 0 {
            return Ok(());
        }
        let Backing::Rw(map) = &self.backing else {
            return Ok(());
        };
        let start = match self.opts.write_mode {
            WriteMode::Append => self.flushed_end.load(Ordering::Relaxed).min(self.end),
            WriteMode::Random => 0,
        };
        if start >= self.end {
            return Ok(());
        }
        let len = (self.end - start) as usize;
        if sync {
            map.flush_range(start as usize, len)?;
            // The tail is now durable; the next append-mode flush can start from here.
            self.flushed_end.store(self.end, Ordering::Relaxed);
        } else {
            map.flush_async_range(start as usize, len)?;
        }
        Ok(())
    }

    /// Default durability barrier: `msync` the region written since the last sync. Cheaper than
    /// `fsync` and sufficient for data within the already-fsync'd file size (size extensions fsync
    /// in the grow path).
    pub fn sync_all(&self) -> io::Result<()> {
        self.flush_dirty(true)
    }

    /// Full, slow disk sync: `msync` then `fsync`, additionally persisting the file's
    /// size/metadata. See the module docs for the macOS `F_FULLFSYNC` caveat.
    pub fn sync_disk(&self) -> io::Result<()> {
        if self.read_only {
            return Ok(());
        }
        self.flush_dirty(true)?;
        self.file.sync_all()
    }

    /// Refresh the logical end for a read-only handle so it observes a writer's appends, re-mapping
    /// if the file grew. Intended for following a cleanly-closed writer; a writer that is
    /// mid-append may have the file padded beyond its logical data, so pair with index-bounded
    /// reads.
    pub fn refresh_data_file_end(&mut self) -> io::Result<()> {
        let disk_len = self.file.metadata()?.len();
        if disk_len == self.capacity {
            self.end = disk_len;
            return Ok(());
        }
        self.backing = Backing::Empty;
        if disk_len > 0 {
            self.backing = if self.read_only {
                // SAFETY: single-writer model.
                Backing::Ro(unsafe { Mmap::map(&self.file)? })
            } else {
                // SAFETY: as above.
                Backing::Rw(unsafe { MmapMut::map_mut(&self.file)? })
            };
        }
        self.capacity = disk_len;
        self.end = disk_len;
        self.advise_backing();
        Ok(())
    }

    /// Mark the file to be removed when this handle drops (instead of the flush+sync clean close).
    pub fn delete(mut self) {
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
        let new = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.end as i64 + p,
            SeekFrom::Current(p) => self.seek_pos as i64 + p,
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
        if buf.is_empty() {
            return Ok(0);
        }
        let n = buf.len() as u64;
        let start = match self.opts.write_mode {
            WriteMode::Append => self.end,
            WriteMode::Random => self.seek_pos,
        };
        self.ensure_capacity(start + n)?;
        let start_us = start as usize;
        match &mut self.backing {
            Backing::Rw(map) => map[start_us..start_us + buf.len()].copy_from_slice(buf),
            _ => return Err(io::Error::other("no writable mapping")),
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

    /// Page-cache visibility for a separate reader, without a durability barrier (matches the
    /// buffered `DataFile`'s `flush`: the data is out of our hands but not necessarily on disk).
    fn flush(&mut self) -> io::Result<()> {
        self.flush_dirty(false)
    }
}

impl Drop for MmapDataFile {
    fn drop(&mut self) {
        if self.remove_on_drop {
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
        // Clean close: msync, then truncate away the padding and fsync so the on-disk file is
        // exactly `end` bytes and durable — a reopen sees the same bytes a buffered DataFile would.
        if let Err(e) = self.flush_dirty(true) {
            if !std::thread::panicking() {
                tracing::error!("MmapDataFile: failed to msync on drop: {e}");
            }
        }
        self.backing = Backing::Empty; // unmap before truncating
        if let Err(e) = self.file.set_len(self.end) {
            if !std::thread::panicking() {
                tracing::error!("MmapDataFile: failed to truncate on drop: {e}");
            }
        }
        if let Err(e) = self.file.sync_all() {
            if !std::thread::panicking() {
                tracing::error!("MmapDataFile: failed to fsync on drop: {e}");
            }
        }
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
        }
    }

    fn random_opts() -> MmapFileOptions {
        MmapFileOptions {
            initial_size: 64,
            max_map_size: 128,
            grow_mode: GrowMode::Reopen,
            write_mode: WriteMode::Random,
            access: MmapAccess::Random,
        }
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

    #[test]
    fn set_len_truncates_and_persists() {
        let tmp = TempDir::with_prefix("mmap_df_setlen").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(100);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
            df.set_len(40).expect("truncate");
            assert_eq!(df.len(), 40);
            df.seek(SeekFrom::Start(0)).expect("seek");
            let mut buf = vec![0u8; 40];
            df.read_exact(&mut buf).expect("read");
            assert_eq!(&buf[..], &data[..40]);
            // Reading past the truncated end yields nothing.
            df.seek(SeekFrom::Start(40)).expect("seek");
            assert_eq!(df.read(&mut [0u8; 8]).expect("read past end"), 0);
        }
        // Physical file is exactly the truncated length after a clean close.
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), 40);
    }

    #[test]
    fn clean_close_reopen_exact() {
        let tmp = TempDir::with_prefix("mmap_df_reopen").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(250);
        {
            let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
            df.write_all(&data).expect("write");
        } // clean close: truncates padding + fsync
          // No padding after a clean close.
        assert_eq!(std::fs::metadata(&path).expect("meta").len(), 250);
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
    fn try_clone_reads_to_eof_exact() {
        let tmp = TempDir::with_prefix("mmap_df_clone").expect("temp dir");
        let path = tmp.path().join("data");
        let data = pattern(200);
        let mut df = MmapDataFile::open_with(&path, false, tiny_opts()).expect("open");
        df.write_all(&data).expect("write");
        // A raw clone read to EOF must yield exactly the written bytes (the PackIter/raw_iter
        // path): no trailing zero padding.
        let mut clone = df.try_clone().expect("clone");
        clone.seek(SeekFrom::Start(0)).expect("seek clone");
        let mut all = Vec::new();
        clone.read_to_end(&mut all).expect("read clone to eof");
        assert_eq!(all, data, "clone must expose exactly the logical bytes");
        // Appending after a try_clone still works (re-maps first).
        df.write_all(&pattern(50)).expect("append after clone");
        assert_eq!(df.len(), 250);
        df.seek(SeekFrom::Start(200)).expect("seek");
        let mut tail = vec![0u8; 50];
        df.read_exact(&mut tail).expect("read tail");
        assert_eq!(tail, pattern(50));
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
