//! The page file: one file mapped read-write (shared) into a reserved range of address space, the
//! single writer's page accessors, its durability barriers, and the lock-free readers' view; and
//! [`ReadOnlyFile`], a read-only environment's mapping.
//!
//! ## Mapping
//!
//! The file is mapped once at `max(reserve, file length)` bytes and grows underneath that mapping
//! (preallocate + extend) without remapping, so the mapping's base never moves while the file
//! stays inside the reservation. Pages past the physical end of file are never touched: the
//! writer's accessors are bounded by the file length and readers by the length the owner
//! published. Growing past the reservation maps a new, larger reservation (the only case in which
//! the base moves) and retires the old mapping, keeping it mapped until the file drops, so a
//! reader that loaded the old base still reads live memory.
//!
//! ## Growth
//!
//! [`PageFile::ensure_len`] grows geometrically: to at least the length asked for, and at least by
//! the current length (doubling), with each step between [`MIN_GROWTH`] and [`MAX_GROWTH`], rounded
//! up to whole OS pages. Every step **preallocates** its range ([`allocate_range`]) so a full disk
//! is an `io::Error` at the growth, never a SIGBUS on the first store into an unbacked page. The
//! slack past the data is harmless: the meta page's page count, not the file length, says which
//! pages are allocated.
//!
//! ## Durability
//!
//! [`PageFile::msync_range`] / [`PageFile::msync_page`] are `msync(MS_SYNC)`; `msync` does not
//! persist a size extension, so a growth marks the size unsynced and
//! [`PageFile::sync_size_if_grown`] fdatasyncs it once. [`PageFile::fdatasync_all`] writes every
//! dirty page of the mapping and the size in one call, so after it the size sync has nothing left
//! to do. Any failed sync or growth poisons the file:
//! every later write or sync returns [`Error::Failed`] and is never retried (a retried `msync`
//! after a writeback error can spuriously succeed on Linux errseq, laundering lost data). Reads
//! continue.
//!
//! ## Read-only
//!
//! [`ReadOnlyFile`] maps a file opened `O_RDONLY` once, `PROT_READ`, at its length: no
//! reservation, no growth, no sync. Readers reach it through the same [`MapView`].

use std::{
    fs::File,
    io,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

use memmap2::{Mmap, MmapMut, MmapOptions};

use crate::{
    error::{Error, Result},
    sys::{allocate_range, fdatasync_file, os_page_size},
};

/// Address space a page file reserves for its mapping by default (1 GiB).
pub(crate) const DEFAULT_RESERVE: u64 = 1 << 30;

/// The smallest growth step (1 MiB): a small file does not grow, and fdatasync, page by page.
pub(crate) const MIN_GROWTH: u64 = 1 << 20;

/// The largest growth step (1 GiB): past it a large file grows linearly, not by doubling.
pub(crate) const MAX_GROWTH: u64 = 1 << 30;

/// A reader's view of a page file's mapping that needs no lock: the mapping's base address and
/// length, plus a length the owner has published as readable. Shared (`Arc`) between the file and
/// its readers; the file updates the mapping fields whenever its mapping moves, and the owner
/// advances [`Self::publish_len`].
///
/// Validity: a slice from [`Self::slice`] borrows mapped memory. The owner must keep the file open
/// while any reader can reach the view; a mapping replaced while the view is shared is retired
/// (kept mapped until the file drops) rather than unmapped, so a reader that loaded the old base is
/// still reading live memory. Once the file drops the view maps nothing.
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

    /// Publish `len` bytes as readable. They must already lie within the file, and no byte a
    /// reader's snapshot can reach may change while the reader can see it (the writer rewrites
    /// only pages no snapshot reaches).
    pub(crate) fn publish_len(&self, len: u64) {
        self.published.store(len, Ordering::Release);
    }

    /// Borrow exactly `[offset, offset + len)` if it lies within the published bytes, else `None`.
    /// Only those bytes are borrowed, so the writer rewriting other published bytes in place (a
    /// page no reader's snapshot can reach) is never borrowed from where it is being written.
    pub(crate) fn slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len as u64)?;
        let limit = self.published.load(Ordering::Acquire).min(self.mapped.load(Ordering::Acquire));
        if end > limit {
            return None;
        }
        let base = self.base.load(Ordering::Acquire);
        if base.is_null() {
            return None;
        }
        // SAFETY: `[base, base + mapped)` is a live mapping (see the type docs), and
        // `[offset, end)` lies within it and within the published bytes, which the owner does not
        // modify while a reader can reach them.
        Some(unsafe { std::slice::from_raw_parts(base.add(offset as usize), len) })
    }
}

/// The page file (see the module docs). One writer owns it: growth and the mutable page accessor
/// take `&mut self`; the barriers take `&self`.
#[derive(Debug)]
pub(crate) struct PageFile {
    file: File,
    /// The current mapping: shared, read-write, over the reservation `[0, map.len())`.
    map: MmapMut,
    /// The physical file length (`<= map.len()`): the writer's accessors stop here.
    len: u64,
    /// The lock-free reader view of the current mapping.
    view: Arc<MapView>,
    /// Mappings replaced while [`Self::view`] was shared: kept mapped until this file drops so a
    /// reader still holding an old base stays valid.
    retired: Vec<MmapMut>,
    /// Set when a growth extended the file since the last size sync (see
    /// [`Self::sync_size_if_grown`]).
    size_unsynced: AtomicBool,
    /// Sticky latch set by any failed sync or growth (see the module docs). Never cleared.
    poisoned: AtomicBool,
    /// Test-only: the file's inode, for the disk model.
    #[cfg(test)]
    ino: u64,
    /// Test-only: fail the next msync (see [`Self::fail_next_msync`]).
    #[cfg(test)]
    fail_next_msync: AtomicBool,
}

impl PageFile {
    /// Open (creating it if missing) the file at `path` read-write and map it with a reservation
    /// of `reserve` bytes of address space.
    #[cfg(test)]
    pub(crate) fn open(path: &std::path::Path, reserve: u64) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Self::from_file(file, reserve)
    }

    /// Map `file`, opened read-write (the caller has locked and validated it), with a reservation
    /// of `reserve` bytes of address space (at least the file's length). Fails if the address
    /// space cannot be reserved.
    pub(crate) fn from_file(file: File, reserve: u64) -> Result<Self> {
        let len = file.metadata()?.len();
        let map = map_reservation(&file, reserve.max(len))?;
        let view = Arc::new(MapView::new());
        view.set_mapping(map.as_ptr(), map.len() as u64);
        Ok(Self {
            #[cfg(test)]
            ino: std::os::unix::fs::MetadataExt::ino(&file.metadata()?),
            file,
            map,
            len,
            view,
            retired: Vec::new(),
            size_unsynced: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_msync: AtomicBool::new(false),
        })
    }

    /// The physical file length in bytes.
    #[cfg(test)]
    pub(crate) fn file_len(&self) -> u64 {
        self.len
    }

    /// The lock-free reader view of this file's mapping (see [`MapView`]).
    pub(crate) fn view(&self) -> Arc<MapView> {
        Arc::clone(&self.view)
    }

    /// True once any sync or growth has failed on this file.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Latch the poison.
    fn poison(&self) {
        self.poisoned.store(true, Ordering::Relaxed);
    }

    /// `Err(Failed)` once poisoned.
    fn check_poison(&self) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::Failed("page file: an earlier sync or growth failed".to_string()));
        }
        Ok(())
    }

    /// Make the file at least `needed` bytes long, growing it geometrically (see the module docs)
    /// with preallocated blocks. The size extension is not synced here: it is marked for
    /// [`Self::sync_size_if_grown`]. A failure poisons the file.
    pub(crate) fn ensure_len(&mut self, needed: u64) -> Result<()> {
        self.check_poison()?;
        if needed <= self.len {
            return Ok(());
        }
        self.grow(needed).inspect_err(|_| self.poison())
    }

    /// The growth step of [`Self::ensure_len`].
    fn grow(&mut self, needed: u64) -> Result<()> {
        let step = self.len.clamp(MIN_GROWTH, MAX_GROWTH);
        let target = needed
            .max(self.len.saturating_add(step))
            .checked_next_multiple_of(os_page_size())
            .ok_or_else(|| Error::Failed(format!("page file: cannot grow to {needed} bytes")))?;
        // `len` is the physical end of file: reserve the new range from it.
        allocate_range(&self.file, self.len, target)?;
        if target > self.map.len() as u64 {
            self.remap(target)?;
        }
        self.len = target;
        self.size_unsynced.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Map a new reservation of `max(target, 2 × the current one)` bytes and point the view at it,
    /// retiring the old mapping if a reader may hold its base (else it unmaps here).
    fn remap(&mut self, target: u64) -> Result<()> {
        let reserve = target.max((self.map.len() as u64).saturating_mul(2));
        let old = std::mem::replace(&mut self.map, map_reservation(&self.file, reserve)?);
        self.view.set_mapping(self.map.as_ptr(), self.map.len() as u64);
        // `&mut self` means the writer holds no page borrow, and a reader can only borrow through
        // its own clone of the view: with no clone, nothing can still point into `old`.
        if Arc::strong_count(&self.view) > 1 {
            self.retired.push(old);
        }
        Ok(())
    }

    /// The byte offset of the `len` bytes from page `pgno` of `page_size` bytes, if they lie
    /// within the file.
    fn page_offset(&self, pgno: u32, page_size: usize, len: usize) -> Option<usize> {
        let start = u64::from(pgno) * page_size as u64;
        (start + len as u64 <= self.len).then_some(start as usize)
    }

    /// Borrow page `pgno` (of `page_size` bytes), or `None` if it lies past the end of the file.
    /// Only that page is borrowed, never the whole mapping.
    pub(crate) fn page(&self, pgno: u32, page_size: usize) -> Option<&[u8]> {
        self.pages(pgno, page_size, page_size)
    }

    /// Borrow the `len` bytes from page `pgno` (of `page_size` bytes): an overflow run of
    /// `len / page_size` pages. `None` if they reach past the end of the file.
    pub(crate) fn pages(&self, pgno: u32, page_size: usize, len: usize) -> Option<&[u8]> {
        let start = self.page_offset(pgno, page_size, len)?;
        // SAFETY: the pages lie within the file and so within the live mapping (`len <=
        // map.len()`). Nothing writes them while this shared borrow lives: the only writer is
        // this file's owner, through `pages_mut`, which needs `&mut self`.
        Some(unsafe { std::slice::from_raw_parts(self.map.as_ptr().add(start), len) })
    }

    /// Borrow page `pgno` (of `page_size` bytes) mutably, through the mapping's raw pointer so only
    /// that page is borrowed: readers may hold slices of other pages meanwhile.
    ///
    /// The tree layer upholds the rule that makes this sound: the writer mutates only its private
    /// pages (pages it allocated in this transaction, which no reader's snapshot reaches), so no
    /// reader ever reads bytes being written.
    #[inline]
    pub(crate) fn page_mut(&mut self, pgno: u32, page_size: usize) -> Result<&mut [u8]> {
        self.pages_mut(pgno, page_size, page_size)
    }

    /// Borrow the `len` bytes from page `pgno` (of `page_size` bytes) mutably, as
    /// [`Self::page_mut`] borrows one page: an overflow run the writer is writing.
    #[inline]
    pub(crate) fn pages_mut(
        &mut self,
        pgno: u32,
        page_size: usize,
        len: usize,
    ) -> Result<&mut [u8]> {
        let start = match self.page_offset(pgno, page_size, len) {
            Some(start) if !self.is_poisoned() => start,
            _ => return Err(self.page_mut_error(pgno)),
        };
        // SAFETY: the pages lie within the file and so within the live mapping, `&mut self` makes
        // this the only writer and the only borrow through `self`, and no reader reaches the pages
        // (see above).
        Ok(unsafe { std::slice::from_raw_parts_mut(self.map.as_mut_ptr().add(start), len) })
    }

    /// [`Self::page_mut`]'s error: the poison first, else the page past the end.
    #[cold]
    #[inline(never)]
    fn page_mut_error(&self, pgno: u32) -> Error {
        match self.check_poison() {
            Err(e) => e,
            Ok(()) => {
                Error::Failed(format!("page file: page {pgno} lies past the end of the file"))
            }
        }
    }

    /// `msync(MS_SYNC)` the byte range `range` (its start rounded down to an OS page). A failure
    /// poisons the file, and the range is never retried.
    pub(crate) fn msync_range(&self, range: Range<u64>) -> Result<()> {
        self.check_poison()?;
        self.msync(range.start, range.end).inspect_err(|_| self.poison())
    }

    /// `msync(MS_SYNC)` page `pgno` of `page_size` bytes (a meta page). A failure poisons the file.
    pub(crate) fn msync_page(&self, pgno: u32, page_size: usize) -> Result<()> {
        self.check_poison()?;
        let start = u64::from(pgno) * page_size as u64;
        self.msync(start, start + page_size as u64).inspect_err(|_| self.poison())
    }

    /// One `msync(MS_SYNC)` of `[start, end)`.
    fn msync(&self, start: u64, end: u64) -> Result<()> {
        if start >= end {
            return Ok(());
        }
        if end > self.len {
            return Err(Error::Failed(format!(
                "page file: msync of {start}..{end} past the end of the file ({})",
                self.len
            )));
        }
        #[cfg(test)]
        if self.fail_next_msync.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected msync failure").into());
        }
        let aligned = start - start % os_page_size();
        let base = self.map.as_ptr();
        // SAFETY: `[aligned, end)` lies within the file and so within the live mapping; `msync`
        // reads no Rust memory.
        let rc = unsafe {
            libc::msync(
                base.add(aligned as usize).cast_mut().cast(),
                (end - aligned) as usize,
                libc::MS_SYNC,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error().into());
        }
        #[cfg(test)]
        crate::disk_model::flushed_range(self.ino, base, self.len, start, end);
        Ok(())
    }

    /// The fsync a growth deferred: if the file grew since the last call, `fdatasync` it once so
    /// its new size is durable. A failure poisons the file.
    pub(crate) fn sync_size_if_grown(&self) -> Result<()> {
        self.check_poison()?;
        if self.size_unsynced.load(Ordering::Relaxed) {
            fdatasync_file(&self.file).inspect_err(|_| self.poison())?;
            self.size_unsynced.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    /// `fdatasync` the whole file, which makes every page written through the mapping durable and
    /// its size too. A failure poisons the file.
    pub(crate) fn fdatasync_all(&self) -> Result<()> {
        self.check_poison()?;
        fdatasync_file(&self.file).inspect_err(|_| self.poison())?;
        self.size_unsynced.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Test-only: make the next msync fail as if the kernel had reported a writeback error.
    #[cfg(test)]
    pub(crate) fn fail_next_msync(&self) {
        self.fail_next_msync.store(true, Ordering::Relaxed);
    }
}

impl Drop for PageFile {
    fn drop(&mut self) {
        // A reader still holding the view finds nothing mapped from now on, rather than a mapping
        // about to go. The mappings (current and retired) unmap as the fields drop.
        self.view.set_mapping(std::ptr::null(), 0);
    }
}

/// A read-only environment's page file (see the module docs): the file, opened read-only and
/// holding its shared lock, mapped once at its length. Nothing changes the file while it is held,
/// so readers borrow the mapping through [`MapView`] as they borrow a writer's.
#[derive(Debug)]
pub(crate) struct ReadOnlyFile {
    /// The file, kept open so its shared lock lasts as long as the mapping.
    _file: File,
    /// The mapping: shared, read-only, over `[0, file length)`.
    _map: Mmap,
    /// The lock-free reader view of the mapping.
    view: Arc<MapView>,
}

impl ReadOnlyFile {
    /// Map all of `file`, opened read-only (the caller has locked it shared and checked that it
    /// is not empty).
    pub(crate) fn from_file(file: File) -> Result<Self> {
        let len = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
        // SAFETY: the shared lock keeps every tndb-b writer out, so nothing changes the file while
        // it is mapped (a foreign process truncating it is outside the crate's contract, as for a
        // writer's mapping), and readers read only the published bytes, inside the file.
        let map = unsafe { MmapOptions::new().len(len).map(&file) }?;
        let view = Arc::new(MapView::new());
        view.set_mapping(map.as_ptr(), map.len() as u64);
        Ok(Self { _file: file, _map: map, view })
    }

    /// The lock-free reader view of the mapping (see [`MapView`]).
    pub(crate) fn view(&self) -> Arc<MapView> {
        Arc::clone(&self.view)
    }
}

impl Drop for ReadOnlyFile {
    fn drop(&mut self) {
        // As for `PageFile`: a reader still holding the view finds nothing mapped from now on.
        self.view.set_mapping(std::ptr::null(), 0);
    }
}

/// Map `[0, len)` of `file` (at least one OS page) read-write and shared. The range may extend
/// past the end of file; those pages are never touched.
fn map_reservation(file: &File, len: u64) -> Result<MmapMut> {
    let len = usize::try_from(len.max(os_page_size())).map_err(io::Error::other)?;
    // SAFETY: the file is only modified through this mapping by its single writer (the database
    // holds an exclusive lock on it), and the pages past the end of file are never touched: the
    // writer's accessors stop at the file length, readers at the published length.
    Ok(unsafe { MmapOptions::new().len(len).map_mut(file) }?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_model;
    use std::os::unix::fs::MetadataExt as _;
    use tempfile::TempDir;

    /// A page size equal to the OS page, so a synced page maps to exactly one modeled page.
    fn os_page() -> usize {
        os_page_size() as usize
    }

    /// Growth follows the documented policy and preallocates real blocks for the whole file
    /// rather than leaving sparse holes.
    #[test]
    fn ensure_len_grows_geometrically_with_real_blocks() {
        let tmp = TempDir::with_prefix("tndb_b_file_grow").expect("temp dir");
        let path = tmp.path().join("db");
        let mut pf = PageFile::open(&path, 64 << 20).expect("open");
        assert_eq!(pf.file_len(), 0);
        pf.ensure_len(8192).expect("grow");
        assert_eq!(pf.file_len(), MIN_GROWTH, "a small file grows by the minimum step");
        pf.ensure_len(MIN_GROWTH + 1).expect("grow");
        assert_eq!(pf.file_len(), 2 * MIN_GROWTH, "then doubles");
        pf.ensure_len(5 * MIN_GROWTH).expect("grow");
        assert_eq!(pf.file_len(), 5 * MIN_GROWTH, "to at least what is needed");
        pf.ensure_len(MIN_GROWTH).expect("no growth needed");
        assert_eq!(pf.file_len(), 5 * MIN_GROWTH);
        let meta = std::fs::metadata(&path).expect("metadata");
        assert_eq!(meta.len(), 5 * MIN_GROWTH);
        assert!(
            meta.blocks() * 512 >= meta.len(),
            "growth must preallocate real blocks (allocated {} of {})",
            meta.blocks() * 512,
            meta.len()
        );
    }

    /// Inside the reservation the mapping never moves while the file grows, and written pages
    /// keep their bytes.
    #[test]
    fn mapping_base_is_stable_inside_the_reservation() {
        let tmp = TempDir::with_prefix("tndb_b_file_stable").expect("temp dir");
        let mut pf = PageFile::open(&tmp.path().join("db"), 32 << 20).expect("open");
        let _reader = pf.view();
        pf.ensure_len(4096).expect("grow");
        pf.page_mut(0, 4096).expect("page").fill(0x5A);
        let base = pf.page(0, 4096).expect("page").as_ptr();
        for mib in [2, 3, 6, 12, 16] {
            pf.ensure_len(mib << 20).expect("grow");
            assert_eq!(pf.page(0, 4096).expect("page").as_ptr(), base, "the mapping moved");
        }
        assert!(pf.retired.is_empty(), "nothing was remapped");
        assert!(pf.page(0, 4096).expect("page").iter().all(|&b| b == 0x5A));
        assert_eq!(pf.page((16 << 20) / 4096, 4096), None, "past the end of the file");
    }

    /// Growing past the reservation remaps a larger one, `max(needed, 2 x reserve)`, and a slice a
    /// reader borrowed before stays readable: the old mapping is retired, not unmapped. Once the
    /// file drops, the view maps nothing.
    #[test]
    fn slice_survives_a_remap_past_the_reservation() {
        let tmp = TempDir::with_prefix("tndb_b_file_remap").expect("temp dir");
        let reserve = MIN_GROWTH + (64 << 10);
        let mut pf = PageFile::open(&tmp.path().join("db"), reserve).expect("open");
        let view = pf.view();
        pf.ensure_len(8192).expect("grow inside the reservation");
        pf.page_mut(1, 4096).expect("page").fill(0xC3);
        assert_eq!(view.slice(4096, 4096), None, "nothing published yet");
        view.publish_len(8192);
        let old = view.slice(4096, 4096).expect("published page");
        assert!(old.iter().all(|&b| b == 0xC3));
        assert_eq!(view.slice(4096, 4097), None, "past the published length");

        pf.ensure_len(MIN_GROWTH + 4096).expect("grow past the reservation");
        assert_eq!(pf.file_len(), 2 * MIN_GROWTH);
        assert_eq!(pf.map.len() as u64, 2 * reserve, "twice the old reservation");
        assert_eq!(pf.retired.len(), 1, "the old mapping is retired, not unmapped");
        assert!(old.iter().all(|&b| b == 0xC3), "a slice of the old mapping is still readable");
        let new = view.slice(4096, 4096).expect("slice after the move");
        assert_ne!(new.as_ptr(), old.as_ptr(), "the view follows the new mapping");
        assert!(new.iter().all(|&b| b == 0xC3));
        assert!(pf.page(1, 4096).expect("page").iter().all(|&b| b == 0xC3));
        pf.ensure_len(8 * MIN_GROWTH).expect("grow past the reservation again");
        assert_eq!(pf.map.len() as u64, 8 * MIN_GROWTH, "what is needed, if that is more");
        assert_eq!(pf.retired.len(), 2);
        assert!(old.iter().all(|&b| b == 0xC3), "every retired mapping stays readable");

        drop(pf);
        assert_eq!(view.slice(0, 1), None, "a dropped file's view maps nothing");
    }

    /// `msync_range` flushes exactly the OS pages its range covers: after a power loss those
    /// pages hold the new bytes and the others the old ones.
    #[test]
    fn msync_range_flushes_only_the_synced_pages() {
        let tmp = TempDir::with_prefix("tndb_b_file_msync").expect("temp dir");
        let path = tmp.path().join("db");
        let page = os_page();
        let mut pf = PageFile::open(&path, 16 << 20).expect("open");
        pf.ensure_len(4 * page as u64).expect("grow");
        pf.sync_size_if_grown().expect("size sync");
        disk_model::track(&path);
        for pgno in 0..4 {
            pf.page_mut(pgno, page).expect("page").fill(0xEE);
        }
        let p = page as u64;
        // Page 1 whole, and a few bytes inside page 3 (which flush its whole OS page).
        pf.msync_range(p..2 * p).expect("msync");
        pf.msync_range(3 * p + 10..3 * p + 20).expect("msync");
        disk_model::power_loss(&path);
        let bytes = std::fs::read(&path).expect("read");
        disk_model::untrack(&path);
        for (pgno, synced) in [(0, false), (1, true), (2, false), (3, true)] {
            let want = if synced { 0xEE } else { 0 };
            assert!(
                bytes[pgno * page..(pgno + 1) * page].iter().all(|&b| b == want),
                "page {pgno} (synced: {synced})"
            );
        }
    }

    /// A failed msync poisons the file: later writes and syncs fail without retrying (even though
    /// the failure was transient), while reads continue.
    #[test]
    fn failed_msync_poisons_the_file() {
        let tmp = TempDir::with_prefix("tndb_b_file_poison").expect("temp dir");
        let mut pf = PageFile::open(&tmp.path().join("db"), 16 << 20).expect("open");
        pf.ensure_len(8192).expect("grow");
        pf.page_mut(0, 4096).expect("page").fill(1);
        pf.fail_next_msync();
        assert!(matches!(pf.msync_range(0..8192), Err(Error::Io(_))));
        assert!(pf.is_poisoned());
        assert!(matches!(pf.msync_range(0..8192), Err(Error::Failed(_))), "never retried");
        assert!(matches!(pf.fdatasync_all(), Err(Error::Failed(_))));
        assert!(matches!(pf.msync_page(0, 4096), Err(Error::Failed(_))));
        assert!(matches!(pf.sync_size_if_grown(), Err(Error::Failed(_))));
        assert!(matches!(pf.ensure_len(16 << 20), Err(Error::Failed(_))));
        assert!(matches!(pf.page_mut(0, 4096), Err(Error::Failed(_))));
        assert!(pf.page(0, 4096).expect("reads continue").iter().all(|&b| b == 1));
    }

    /// A read-only mapping of a file opened read-only (mode 0444 here) reads its bytes up to the
    /// published length, and maps nothing once dropped.
    #[test]
    fn a_read_only_file_maps_its_bytes_until_dropped() {
        let tmp = TempDir::with_prefix("tndb_b_file_ro").expect("temp dir");
        let path = tmp.path().join("db");
        let bytes: Vec<u8> = (0..3 * 4096_u32).map(|i| (i / 4096 + 1) as u8).collect();
        std::fs::write(&path, &bytes).expect("write");
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o444))
            .expect("chmod");
        let file = File::open(&path).expect("open read-only");
        let ro = ReadOnlyFile::from_file(file).expect("map");
        let view = ro.view();
        assert_eq!(view.slice(0, 1), None, "nothing published yet");
        view.publish_len(2 * 4096);
        assert!(view.slice(4096, 4096).expect("page 1").iter().all(|&b| b == 2));
        assert_eq!(view.slice(4096, 4097), None, "past the published length");
        view.publish_len(3 * 4096);
        assert!(view.slice(2 * 4096, 4096).expect("page 2").iter().all(|&b| b == 3));
        view.publish_len(4 * 4096);
        assert_eq!(view.slice(3 * 4096, 1), None, "past the mapping");
        drop(ro);
        assert_eq!(view.slice(0, 1), None, "a dropped file's view maps nothing");
        assert!(std::fs::read(&path).expect("read") == bytes);
    }

    /// A growth that cannot be satisfied fails with an error (never a panic or a wrapped length)
    /// and poisons the file.
    #[test]
    fn failed_growth_poisons_the_file() {
        let tmp = TempDir::with_prefix("tndb_b_file_grow_fail").expect("temp dir");
        let mut pf = PageFile::open(&tmp.path().join("db"), 16 << 20).expect("open");
        assert!(pf.ensure_len(u64::MAX - 5).is_err(), "no file grows that large");
        assert!(pf.is_poisoned());
        assert!(matches!(pf.ensure_len(4096), Err(Error::Failed(_))));
    }
}
