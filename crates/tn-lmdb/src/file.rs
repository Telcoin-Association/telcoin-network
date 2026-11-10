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
//! The writer's mapping is an [`MmapRaw`], which hands out only raw pointers: every page slice,
//! the writer's and the readers' alike, is formed from the base pointer over exactly its own
//! bytes, so no reference to the whole mapping ever exists beside a reader's slice.
//!
//! ## Access advice
//!
//! Every mapping of the file, the writer's at open and after each remap past the reservation and
//! a read-only environment's, is advised `MADV_RANDOM` ([`advise_access`]) unless the environment
//! was opened with `EnvOptions::readahead`, which defaults to `false` on Linux only (macOS writes
//! back a whole VM page either way, and the advice slowed its first writes into new file space,
//! README do-not-flag item 16). A B+tree's faults are random: a descent touches one
//! page per level, nowhere near its neighbours, so readahead mostly reads pages nothing asks for.
//! On Linux it also sizes what a commit writes: the page cache holds a file in folios, readahead
//! builds large ones, and a page dirtied through a shared mapping writes back its whole folio.
//! Measured on Linux 7.1, ext4, NVMe, as the disk bytes written per 4 KiB page dirtied through a
//! shared mapping and synced: 439 KiB for a file written by `write(2)` and still cached, 115 KiB
//! for one read cold in full with the default readahead, 4.2 KiB for one read cold at random with
//! it, and 4.0 KiB with `MADV_RANDOM`. Advice is a hint: a failed one is logged and the mapping is
//! used without it, reading the same bytes.
//!
//! ## Growth
//!
//! [`PageFile::ensure_len`] grows geometrically: to at least the length asked for, and at least by
//! the current length (doubling), with each step between [`MIN_GROWTH`] and the environment's
//! `EnvOptions::max_growth` (default [`MAX_GROWTH`], 1 GiB), rounded up to whole OS pages. Every
//! step **preallocates** its range ([`allocate_range`]) so a full disk is an `io::Error` at the
//! growth, never a SIGBUS on the first store into an unbacked page. The slack past the data is
//! harmless: the meta page's page count, not the file length, says which pages are allocated. A
//! running file holds up to one step of it; a clean close gives it back ([`PageFile::trim_to`],
//! from `EnvInner::drop` through `Writer::trim_tail`), truncating the file to its pages rounded up
//! to the OS page, so a resting file holds no preallocated tail. A crash, a latched close or a
//! failed trim keeps the slack. A writer that opens such a file does not take the slack as its
//! own ([`PageFile::distrust_tail`]): a sparse copy or restore may have made it holes. Its first
//! growth past the pages in use truncates the file to them and grows from there, preallocated.
//! The cut covers the tail only: holes such a copy made inside the pages in use stay.
//!
//! ## Durability
//!
//! [`PageFile::msync_range`] / [`PageFile::msync_page`] are `msync(MS_SYNC)`; `msync` does not
//! persist a size extension, so a growth marks the size unsynced and
//! [`PageFile::sync_size_if_grown`] fdatasyncs it once. [`PageFile::fdatasync_all`] writes every
//! dirty page of the mapping and the size in one call, so after it the size sync has nothing left
//! to do. On macOS an `fsync` leaves some pages dirtied through the mapping unwritten and only
//! `msync` writes them, so there every such page is synced by an `msync`: `fdatasync_all` msyncs
//! the whole mapping before its `fsync`, a commit's scattered pages take `PageFile::msync_ranges`,
//! and a writer open (`Env::open`) msyncs the mapping once, for the pages a crashed writer left
//! dirty (README, Platform notes). Any failed sync or growth poisons the file: every later write or
//! sync returns [`Error::Failed`] and is never retried (a retried `msync` after a writeback error
//! can spuriously succeed on Linux errseq, laundering lost data). Reads continue.
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

use memmap2::{Advice, Mmap, MmapOptions, MmapRaw};

use crate::{
    error::{Error, Result},
    sys::{allocate_range, fdatasync_file, os_page_size},
};

/// Address space a page file reserves for its mapping by default (1 GiB).
pub(crate) const DEFAULT_RESERVE: u64 = 1 << 30;

/// The smallest growth step of the page file (1 MiB), and the least `EnvOptions::max_growth`
/// accepts: a small file does not grow, and fdatasync, page by page.
pub const MIN_GROWTH: u64 = 1 << 20;

/// The default largest growth step (1 GiB), `EnvOptions::max_growth`'s default: past it a large
/// file grows linearly, not by doubling.
pub(crate) const MAX_GROWTH: u64 = 1 << 30;

/// What [`PageFile::read_resident`] read.
#[derive(Debug)]
pub(crate) enum Resident {
    /// Every byte: the page cache held the whole range (on macOS, always: a plain `pread`).
    Whole,
    /// Not every byte: some page of the range is not in the page cache.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    NotWhole,
    /// The kernel or the file system refuses a read that does not wait for the disk.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Unsupported(io::Error),
}

/// A reader's view of a page file's mapping that needs no lock: the mapping's base address and
/// length, plus a length the owner has published as readable. One view, shared (`Arc`) between the
/// file, its readers' snapshots and, in an environment, the writer, which holds a clone of its own
/// (`Writer::view`) to borrow a copy-on-write source. The file updates the mapping fields in place
/// whenever its mapping moves, so every clone, the writer's like the readers', follows it without
/// a refresh; the owner advances [`Self::publish_len`].
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
    /// only pages no snapshot reaches, and the meta pages, which no reader slices: see
    /// [`Self::slice`]).
    pub(crate) fn publish_len(&self, len: u64) {
        self.published.store(len, Ordering::Release);
    }

    /// Borrow exactly `[offset, offset + len)` if it lies within the published bytes, else `None`.
    /// Only those bytes are borrowed, so the writer rewriting other published bytes in place (a
    /// page no reader's snapshot can reach) is never borrowed from where it is being written.
    ///
    /// No reader slices pages 0 and 1, the meta slots: they lie inside the published bytes, but
    /// the writer rewrites one of them in place at every durable commit and sync. A reader's page
    /// number passes `checked_page`, which refuses one below 2, before it is sliced, and
    /// `Snapshot::page_again` takes only page numbers that passed it. Only the writer may slice
    /// one, on its own thread, between its writes of them (`Writer::copy_published_with` slices
    /// its source before `checked_page` refuses it).
    #[inline]
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
        // modify while a reader can reach them, but for the meta pages, which only the writer
        // that rewrites them slices (see the method docs).
        Some(unsafe { std::slice::from_raw_parts(base.add(offset as usize), len) })
    }
}

/// The page file (see the module docs). One writer owns it: growth and the mutable page accessor
/// take `&mut self`; the barriers take `&self`.
#[derive(Debug)]
pub(crate) struct PageFile {
    /// The current mapping: shared, read-write, over the reservation `[0, map.len())`. Only ever
    /// reached through its raw pointer ([`MmapRaw::as_ptr`], [`MmapRaw::as_mut_ptr`]).
    map: MmapRaw,
    /// The physical file length (`<= map.len()`): the writer's accessors stop here.
    len: u64,
    /// The length below which [`Self::ensure_len`] needs no growth (`<= len`): `len` itself once
    /// this file has grown or been trimmed, and at open the pages in use when the environment
    /// says so ([`Self::distrust_tail`]).
    backed: u64,
    /// The lock-free reader view of the current mapping.
    view: Arc<MapView>,
    /// Mappings replaced while [`Self::view`] was shared: kept mapped until this file drops so a
    /// reader still holding an old base stays valid.
    retired: Vec<MmapRaw>,
    /// The file, declared after the mappings so they unmap before it closes (fields drop in
    /// declaration order) and its lock lasts as long as any of them.
    file: File,
    /// Leave every mapping to the kernel's readahead instead of advising `MADV_RANDOM` (see the
    /// module docs' Access advice): `EnvOptions::readahead`, applied to each remap too.
    readahead: bool,
    /// The largest growth step, `EnvOptions::max_growth` (at least [`MIN_GROWTH`]): read only by
    /// [`Self::grow`].
    max_growth: u64,
    /// Set when a growth extended the file since the last size sync (see
    /// [`Self::sync_size_if_grown`]).
    size_unsynced: AtomicBool,
    /// The bytes the growths added since this file was mapped ([`Self::bytes_grown`]).
    bytes_grown: u64,
    /// Sticky latch set by any failed sync or growth (see the module docs). Never cleared.
    poisoned: AtomicBool,
    /// Test-only: the file's inode, for the disk model.
    #[cfg(test)]
    ino: u64,
    /// Test-only: fail the next msync (see [`Self::fail_next_msync`]).
    #[cfg(test)]
    fail_next_msync: AtomicBool,
    /// Test-only: fail the next fdatasync (see [`Self::fail_next_fsync`]).
    #[cfg(test)]
    fail_next_fsync: AtomicBool,
    /// Test-only: [`Self::read_resident`] finds no range whole in the page cache, as a gap fill
    /// over a file the cache does not hold would.
    #[cfg(test)]
    pub(crate) miss_reads: bool,
}

impl PageFile {
    /// Open (creating it if missing) the file at `path` read-write and map it with a reservation
    /// of `reserve` bytes of address space, advised `MADV_RANDOM` (the default access advice),
    /// growing by steps of at most [`MAX_GROWTH`] (the default).
    #[cfg(test)]
    pub(crate) fn open(path: &std::path::Path, reserve: u64) -> Result<Self> {
        Self::open_with_max_growth(path, reserve, MAX_GROWTH)
    }

    /// [`Self::open`] with growth steps of at most `max_growth` bytes.
    #[cfg(test)]
    pub(crate) fn open_with_max_growth(
        path: &std::path::Path,
        reserve: u64,
        max_growth: u64,
    ) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Self::from_file(file, reserve, false, max_growth)
    }

    /// Map `file`, opened read-write (the caller has locked and validated it), with a reservation
    /// of `reserve` bytes of address space (at least the file's length), advised `MADV_RANDOM`
    /// unless `readahead` (see the module docs' Access advice), growing by steps of at most
    /// `max_growth` bytes (`Env::open` refuses less than [`MIN_GROWTH`]; a smaller one is taken
    /// as [`MIN_GROWTH`]). Fails if the address space cannot be reserved.
    pub(crate) fn from_file(
        file: File,
        reserve: u64,
        readahead: bool,
        max_growth: u64,
    ) -> Result<Self> {
        let len = file.metadata()?.len();
        let map = map_reservation(&file, reserve.max(len), readahead)?;
        let view = Arc::new(MapView::new());
        view.set_mapping(map.as_ptr(), map.len() as u64);
        Ok(Self {
            #[cfg(test)]
            ino: std::os::unix::fs::MetadataExt::ino(&file.metadata()?),
            file,
            map,
            len,
            backed: len,
            view,
            retired: Vec::new(),
            readahead,
            max_growth: max_growth.max(MIN_GROWTH),
            size_unsynced: AtomicBool::new(false),
            bytes_grown: 0,
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_msync: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_fsync: AtomicBool::new(false),
            #[cfg(test)]
            miss_reads: false,
        })
    }

    /// The physical file length in bytes.
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
        if needed <= self.backed {
            return Ok(());
        }
        self.grow(needed).inspect_err(|_| self.poison())
    }

    /// Take the bytes past `used` as a tail this file did not preallocate: an earlier process
    /// left it (a crash, a latched close or a failed trim keeps the slack), and a sparse copy or
    /// restore of the file (`cp --sparse=always`, `rsync -S`, `tar -S`) may have made it holes,
    /// where a first store on a full disk is a SIGBUS. The first growth past `used`
    /// ([`Self::grow`]) truncates the file to `used` and grows it again, preallocated. Writes
    /// nothing now. `used` covers every page any state the file holds names; past the file's
    /// length it changes nothing. The cut covers the tail only: holes a sparse copy made inside
    /// the pages in use stay, and nothing allocates them again (README, Platform notes).
    pub(crate) fn distrust_tail(&mut self, used: u64) {
        self.backed = self.backed.min(used);
    }

    /// Test-only: take the whole file as this file's own growth, as if it had grown it, undoing
    /// [`Self::distrust_tail`]. For a crash test whose commit must not grow a reopened file.
    #[cfg(test)]
    pub(crate) fn trust_tail(&mut self) {
        self.backed = self.len;
    }

    /// The growth step of [`Self::ensure_len`].
    fn grow(&mut self, needed: u64) -> Result<()> {
        if self.len > self.backed {
            // The tail `distrust_tail` marked: cut it, then grow from the pages in use like any
            // growth. Nothing reads or writes past `backed` before this (the writer allocates no
            // page past it), and a power loss that brings the old length back leaves only slack.
            self.file.set_len(self.backed)?;
            #[cfg(test)]
            crate::disk_model::truncated(self.ino, self.backed);
            self.len = self.backed;
            self.size_unsynced.store(true, Ordering::Relaxed);
        }
        let step = self.len.clamp(MIN_GROWTH, self.max_growth);
        let target = needed
            .max(self.len.saturating_add(step))
            .checked_next_multiple_of(os_page_size())
            .ok_or_else(|| Error::Failed(format!("page file: cannot grow to {needed} bytes")))?;
        // `len` is the physical end of file: reserve the new range from it.
        allocate_range(&self.file, self.len, target)?;
        if target > self.map.len() as u64 {
            self.remap(target)?;
        }
        self.bytes_grown += target - self.len;
        self.len = target;
        self.backed = target;
        self.size_unsynced.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Truncate the file to `len` bytes, giving back the preallocated tail past the pages in use
    /// (a clean close's last step, `Writer::trim_tail`). Refused, changing nothing, once poisoned
    /// or when `len` does not shorten the file. No sync is made, and the size is marked unsynced as
    /// a growth's is: the bytes below `len` are durable already and either length is a valid file
    /// (an open accepts any length of at least its meta's pages), so a power loss that brings the
    /// old length back leaves only slack. A failure poisons the file.
    ///
    /// The mapping stays as it is. No reader can reach a byte past `len`: every snapshot reads
    /// through [`MapView::slice`], bounded by the published length, and each commit publishes its
    /// own `page_count` pages, which the caller keeps at or below `len`; the writer's accessors
    /// stop at the new length.
    pub(crate) fn trim_to(&mut self, len: u64) -> Result<()> {
        self.check_poison()?;
        if len >= self.len {
            return Err(Error::Failed(format!(
                "page file: a trim to {len} bytes does not shorten the file ({} bytes)",
                self.len
            )));
        }
        self.file.set_len(len).inspect_err(|_| self.poison())?;
        #[cfg(test)]
        crate::disk_model::truncated(self.ino, len);
        self.len = len;
        self.backed = self.backed.min(len);
        self.size_unsynced.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// The bytes the growths added to the file's physical length since it was mapped
    /// (`IoStats::bytes_grown`).
    pub(crate) fn bytes_grown(&self) -> u64 {
        self.bytes_grown
    }

    /// True if a growth extended the file since the last size sync (or `fdatasync`): its size
    /// is not yet durable.
    pub(crate) fn size_unsynced(&self) -> bool {
        self.size_unsynced.load(Ordering::Relaxed)
    }

    /// Map a new reservation of `max(target, 2 × the current one)` bytes, advised as the first
    /// one was, and point the view at it, retiring the old mapping if a clone of the view may hold
    /// its base (else it unmaps here).
    fn remap(&mut self, target: u64) -> Result<()> {
        let reserve = target.max((self.map.len() as u64).saturating_mul(2));
        let new = map_reservation(&self.file, reserve, self.readahead)?;
        let old = std::mem::replace(&mut self.map, new);
        self.view.set_mapping(self.map.as_ptr(), self.map.len() as u64);
        // `&mut self` means the writer holds no page borrow through the file, and every other
        // borrow goes through a clone of the view: a reader's, or the writer's own
        // (`Writer::view`), which `set_mapping` has just updated in place like the readers'. With
        // no clone, nothing can still point into `old`. An environment always holds clones (the
        // writer's and the current snapshot's), so there the old mapping is always retired.
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
        // map.len()`), and the slice is formed from the mapping's raw pointer over exactly those
        // bytes. Nothing writes them while this shared borrow lives: the only writer is this
        // file's owner, through `pages_mut`, which needs `&mut self`.
        Some(unsafe { std::slice::from_raw_parts(self.map.as_ptr().add(start), len) })
    }

    /// Borrow page `pgno` (of `page_size` bytes) mutably, through the mapping's raw pointer
    /// ([`MmapRaw::as_mut_ptr`], which forms no reference to the mapping) so only that page is
    /// borrowed: readers, and the writer's own copy-on-write source, may hold slices of other
    /// pages meanwhile.
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
        // SAFETY: the pages lie within the file and so within the live mapping, and the slice is
        // formed from the mapping's raw pointer over exactly those bytes, so it overlaps no slice
        // of another page. `&mut self` makes this the only writer and the only borrow through
        // `self`, and no reader reaches the pages (see above).
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

    /// Read the `buf.len()` bytes at `offset` into `buf` through the file, not the mapping, for
    /// the gap fill (`Writer::fill_gaps`), when the page cache holds them all
    /// ([`Self::read_cached`]). Any error but those `read_cached` maps is returned (and the caller
    /// fails the commit). The range must lie inside the file.
    pub(crate) fn read_resident(&self, offset: u64, buf: &mut [u8]) -> Result<Resident> {
        self.check_poison()?;
        self.check_inside(offset, buf.len())?;
        #[cfg(test)]
        if self.miss_reads {
            return Ok(Resident::NotWhole);
        }
        Ok(self.read_cached(offset, buf)?)
    }

    /// Linux: one `preadv2` with `RWF_NOWAIT` (through `rustix`, so no `unsafe` here), which
    /// copies only what the page cache holds and never waits for the disk. `Whole` once it read
    /// every byte; `NotWhole` for a short read (a page past the first is not cached: nothing was
    /// read from the disk) or `EAGAIN` (the first is not: the kernel starts a read of the range,
    /// which nothing waits for, of the range alone under [`Self::advise_random_reads`]);
    /// `Unsupported` when the kernel or the file system refuses the flag (`EOPNOTSUPP`, `EINVAL`,
    /// `ENOSYS`). `EINTR` reissues the call.
    #[cfg(target_os = "linux")]
    fn read_cached(&self, offset: u64, buf: &mut [u8]) -> io::Result<Resident> {
        use rustix::io::{preadv2, Errno, ReadWriteFlags};
        let want = buf.len();
        loop {
            let flags = ReadWriteFlags::NOWAIT;
            match preadv2(&self.file, &mut [io::IoSliceMut::new(&mut *buf)], offset, flags) {
                Err(Errno::INTR) => {}
                Ok(n) if n == want => return Ok(Resident::Whole),
                Ok(_) | Err(Errno::AGAIN) => return Ok(Resident::NotWhole),
                Err(e @ (Errno::OPNOTSUPP | Errno::INVAL | Errno::NOSYS)) => {
                    return Ok(Resident::Unsupported(e.into()))
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// macOS: no read there reports what the page cache holds without new `unsafe` (`mincore` is
    /// a raw call), so a plain `pread` (`read_exact_at`, which reissues an interrupted call):
    /// always `Whole`, and a page not in memory costs a synchronous read.
    #[cfg(target_os = "macos")]
    fn read_cached(&self, offset: u64, buf: &mut [u8]) -> io::Result<Resident> {
        use std::os::unix::fs::FileExt as _;
        self.file.read_exact_at(buf, offset)?;
        Ok(Resident::Whole)
    }

    /// Write `bytes` at `offset` through the file (`pwrite`, `write_all_at`), never through the
    /// mapping: the gap fill's write of bytes [`Self::read_resident`] read from the same offset
    /// (`Writer::fill_gaps` says why that changes no byte). The bytes reach the page cache only;
    /// step 4's sync writes them with the commit's pages. Refused once poisoned; a failure
    /// poisons the file. The range must lie inside the file.
    pub(crate) fn rewrite(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        use std::os::unix::fs::FileExt as _;
        self.check_poison()?;
        self.check_inside(offset, bytes.len())?;
        self.file.write_all_at(bytes, offset).inspect_err(|_| self.poison())?;
        Ok(())
    }

    /// `Err(Failed)` unless `[offset, offset + len)` lies inside the file.
    fn check_inside(&self, offset: u64, len: usize) -> Result<()> {
        if offset.checked_add(len as u64).is_some_and(|end| end <= self.len) {
            return Ok(());
        }
        Err(Error::Failed(format!(
            "page file: {len} bytes at {offset} reach past the end of the file ({})",
            self.len
        )))
    }

    /// Linux: advise the file's reads `POSIX_FADV_RANDOM` (through `rustix`), so a gap fill's
    /// read that misses the page cache starts a read of its own range only, not the larger
    /// window the kernel's readahead would add. It concerns reads through the file, never the
    /// mapping (whose advice is [`advise_access`]'s), and the engine reads through the file only
    /// at open and for the gap fill. A hint: a failure is logged and changes nothing else.
    /// Elsewhere nothing: macOS's gap fill reads with a plain `pread`.
    pub(crate) fn advise_random_reads(&self) {
        #[cfg(target_os = "linux")]
        if let Err(err) = rustix::fs::fadvise(&self.file, 0, None, rustix::fs::Advice::Random) {
            tracing::warn!("tn-lmdb: POSIX_FADV_RANDOM for the gap fill's reads failed: {err}");
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

    /// macOS: make `ranges` durable by one `msync(MS_ASYNC)` of each, which starts the writes
    /// of their dirty pages, then one `msync(MS_SYNC)` of each, which waits for those writes (as
    /// for any write in flight) and writes any page still dirty. Only the `MS_SYNC` pass is relied
    /// on; the `MS_ASYNC` pass is for speed (one `MS_SYNC` per range alone writes one range at a
    /// time). A failure poisons the file, and the ranges are never retried.
    #[cfg(target_os = "macos")]
    pub(crate) fn msync_ranges(
        &self,
        mut ranges: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<()> {
        self.check_poison()?;
        ranges
            .clone()
            .try_for_each(|range| self.msync_flags(range.start, range.end, libc::MS_ASYNC))
            .and_then(|()| ranges.try_for_each(|range| self.msync(range.start, range.end)))
            .inspect_err(|_| self.poison())
    }

    /// One `msync(MS_SYNC)` of `[start, end)`.
    fn msync(&self, start: u64, end: u64) -> Result<()> {
        #[cfg(test)]
        if start < end && end <= self.len && self.fail_next_msync.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected msync failure").into());
        }
        self.msync_flags(start, end, libc::MS_SYNC)
    }

    /// One `msync` of `[start, end)` with `flags`: `MS_SYNC` ([`Self::msync`], and on macOS
    /// [`Self::fdatasync_all`]'s msync of the mapping), or on macOS `MS_ASYNC`
    /// (`Self::msync_ranges`' first pass). Only [`Self::msync`] is failed by
    /// `Self::fail_next_msync`, so `fdatasync_all`'s injected failure stays
    /// `Self::fail_next_fsync`'s. An `MS_ASYNC` makes nothing durable, so only an `MS_SYNC`
    /// reports to the disk model.
    fn msync_flags(&self, start: u64, end: u64, flags: libc::c_int) -> Result<()> {
        if start >= end {
            return Ok(());
        }
        if end > self.len {
            return Err(Error::Failed(format!(
                "page file: msync of {start}..{end} past the end of the file ({})",
                self.len
            )));
        }
        let aligned = start - start % os_page_size();
        let base = self.map.as_ptr();
        // SAFETY: `[aligned, end)` lies within the file and so within the live mapping; `msync`
        // reads no Rust memory, and `base` is the mapping's raw pointer, so no reference to the
        // mapping is formed.
        let rc = unsafe {
            libc::msync(
                base.add(aligned as usize).cast_mut().cast(),
                (end - aligned) as usize,
                flags,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error().into());
        }
        #[cfg(test)]
        if flags == libc::MS_SYNC {
            crate::disk_model::flushed_range(self.ino, base, self.len, start, end);
        }
        Ok(())
    }

    /// The fsync a growth deferred: if the file grew since the last call, `fdatasync` it once so
    /// its new size is durable. A failure poisons the file.
    pub(crate) fn sync_size_if_grown(&self) -> Result<()> {
        self.check_poison()?;
        if self.size_unsynced.load(Ordering::Relaxed) {
            self.fdatasync().inspect_err(|_| self.poison())?;
            self.size_unsynced.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    /// `fdatasync` the whole file, which makes every page written through the mapping durable and
    /// its size too (on macOS after an `msync` of the whole mapping: see the module docs). A
    /// failure poisons the file.
    pub(crate) fn fdatasync_all(&self) -> Result<()> {
        self.check_poison()?;
        #[cfg(target_os = "macos")]
        self.msync_flags(0, self.len, libc::MS_SYNC).inspect_err(|_| self.poison())?;
        self.fdatasync().inspect_err(|_| self.poison())?;
        self.size_unsynced.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// One `fdatasync` of the file, for the size sync and [`Self::fdatasync_all`].
    fn fdatasync(&self) -> Result<()> {
        #[cfg(test)]
        if self.fail_next_fsync.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected fsync failure").into());
        }
        Ok(fdatasync_file(&self.file)?)
    }

    /// Test-only: make the next msync fail as if the kernel had reported a writeback error.
    #[cfg(test)]
    pub(crate) fn fail_next_msync(&self) {
        self.fail_next_msync.store(true, Ordering::Relaxed);
    }

    /// Test-only: make the next fdatasync (the size sync's or [`Self::fdatasync_all`]'s) fail as
    /// if the kernel had reported a writeback error.
    #[cfg(test)]
    pub(crate) fn fail_next_fsync(&self) {
        self.fail_next_fsync.store(true, Ordering::Relaxed);
    }

    /// Test-only (macOS): how many OS pages of the file the kernel holds modified and not yet
    /// written to it (`mincore`'s `MINCORE_MODIFIED` or `MINCORE_MODIFIED_OTHER`), whichever
    /// mapping modified them.
    #[cfg(all(test, target_os = "macos"))]
    pub(crate) fn dirty_os_pages(&self) -> usize {
        let len = self.len as usize;
        let mut flags = vec![0 as libc::c_char; len.div_ceil(os_page_size() as usize)];
        // SAFETY: `[0, len)` lies within the live mapping, and `mincore` writes one byte per OS
        // page of it into `flags`, which holds that many.
        let rc = unsafe { libc::mincore(self.map.as_ptr().cast(), len, flags.as_mut_ptr()) };
        assert_eq!(rc, 0, "mincore: {}", io::Error::last_os_error());
        let modified = libc::MINCORE_MODIFIED | libc::MINCORE_MODIFIED_OTHER;
        flags.iter().filter(|&&f| i32::from(f) & modified != 0).count()
    }
}

impl Drop for PageFile {
    fn drop(&mut self) {
        // A reader still holding the view finds nothing mapped from now on, rather than a mapping
        // about to go. The mappings (current and retired) unmap as the fields drop, then the file
        // closes.
        self.view.set_mapping(std::ptr::null(), 0);
    }
}

/// A read-only environment's page file (see the module docs): the file, opened read-only and
/// holding its shared lock, mapped once at its length. Nothing changes the file while it is held,
/// so readers borrow the mapping through [`MapView`] as they borrow a writer's.
#[derive(Debug)]
pub(crate) struct ReadOnlyFile {
    /// The mapping: shared, read-only, over `[0, file length)`.
    _map: Mmap,
    /// The file, declared after the mapping so it unmaps before the file closes (fields drop in
    /// declaration order) and the shared lock lasts as long as the mapping.
    _file: File,
    /// The lock-free reader view of the mapping.
    view: Arc<MapView>,
}

impl ReadOnlyFile {
    /// Map all of `file`, opened read-only (the caller has locked it shared and checked that it
    /// is not empty), advised `MADV_RANDOM` unless `readahead` (see the module docs' Access
    /// advice).
    pub(crate) fn from_file(file: File, readahead: bool) -> Result<Self> {
        let len = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
        // SAFETY: the shared lock keeps every tn-lmdb writer out, so nothing changes the file while
        // it is mapped (a foreign process truncating it is outside the crate's contract, as for a
        // writer's mapping), and readers read only the published bytes, inside the file.
        let map = mapped(unsafe { MmapOptions::new().len(len).map(&file) }, len)?;
        advise_access(readahead, |advice| map.advise(advice));
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

/// Map `[0, len)` of `file` (at least one OS page) read-write and shared (`PROT_READ |
/// PROT_WRITE`, `MAP_SHARED`), advised `MADV_RANDOM` unless `readahead` ([`advise_access`]). The
/// range may extend past the end of file; those pages are never touched.
fn map_reservation(file: &File, len: u64, readahead: bool) -> Result<MmapRaw> {
    let len = usize::try_from(len.max(os_page_size())).map_err(io::Error::other)?;
    // `map_raw` is safe because an `MmapRaw` hands out only raw pointers; the slices this file
    // forms from them are sound because the file is only modified through this mapping by its
    // single writer (the database holds an exclusive lock on it), and the pages past the end of
    // file are never touched: the writer's accessors stop at the file length, readers at the
    // published length.
    let map = mapped(MmapOptions::new().len(len).map_raw(file), len)?;
    advise_access(readahead, |advice| map.advise(advice));
    Ok(map)
}

/// The mapping `map` of `len` bytes, its refusal for want of memory ([`libc::ENOMEM`]) as
/// [`Error::OutOfMemory`], any other failure as [`Error::Io`].
pub(crate) fn mapped<M>(map: io::Result<M>, len: usize) -> Result<M> {
    map.map_err(|source| match source.raw_os_error() {
        Some(libc::ENOMEM) => Error::OutOfMemory { bytes: len as u64, source },
        _ => Error::Io(source),
    })
}

/// Advise a new mapping of the page file through `advise` (its `advise` method): `MADV_RANDOM`,
/// unless `readahead`, which leaves the kernel's default (see the module docs' Access advice).
/// Advice is a hint, so a failure does not fail the open or the growth that made the mapping: it
/// is logged, and the mapping reads the same bytes with the kernel's readahead. Mappings are made
/// at open and at a growth past the reservation, so a failure logs rarely.
fn advise_access(readahead: bool, advise: impl FnOnce(Advice) -> io::Result<()>) {
    let applied = if readahead {
        None
    } else {
        match advise(Advice::Random) {
            Ok(()) => Some(Advice::Random),
            Err(e) => {
                tracing::warn!(
                    target: "tn_lmdb",
                    "madvise(MADV_RANDOM) on the page file's mapping failed: {e}; the mapping \
                     keeps the kernel's readahead"
                );
                None
            }
        }
    };
    #[cfg(test)]
    ADVICE_LOG.with(|log| log.borrow_mut().push(applied));
    #[cfg(not(test))]
    let _ = applied;
}

#[cfg(test)]
thread_local! {
    /// Test-only: the advice [`advise_access`] put into effect on each mapping this thread made, in
    /// order (`None`: no advice, as `readahead` asks, or a failed one). Read with
    /// [`take_advice_log`].
    static ADVICE_LOG: std::cell::RefCell<Vec<Option<Advice>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Test-only: the advice put into effect on each mapping this thread made since the last call,
/// oldest first (see [`ADVICE_LOG`]).
#[cfg(test)]
pub(crate) fn take_advice_log() -> Vec<Option<Advice>> {
    ADVICE_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
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
        let tmp = TempDir::with_prefix("tn_lmdb_file_grow").expect("temp dir");
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

    /// A growth cap of 2 MiB (`EnvOptions::max_growth`) stops the doubling at the cap: the file
    /// grows 1, 2, 4 MiB, then linearly by the cap, to 6 and 8 MiB.
    #[test]
    fn growth_steps_stop_doubling_at_the_cap() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_grow_cap").expect("temp dir");
        let mut pf =
            PageFile::open_with_max_growth(&tmp.path().join("db"), 64 << 20, 2 * MIN_GROWTH)
                .expect("open");
        let mut lengths = Vec::new();
        for _ in 0..5 {
            pf.ensure_len(pf.file_len() + 1).expect("grow");
            lengths.push(pf.file_len() / MIN_GROWTH);
        }
        assert_eq!(lengths, [1, 2, 4, 6, 8], "MiB after each growth");
    }

    /// Inside the reservation the mapping never moves while the file grows, and written pages
    /// keep their bytes.
    #[test]
    fn mapping_base_is_stable_inside_the_reservation() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_stable").expect("temp dir");
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
        let tmp = TempDir::with_prefix("tn_lmdb_file_remap").expect("temp dir");
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
        let tmp = TempDir::with_prefix("tn_lmdb_file_msync").expect("temp dir");
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

    /// macOS: `fdatasync_all` leaves no page dirtied through the mapping unwritten, rewrites of
    /// pages already on disk included, which an `fsync` alone leaves dirty there.
    #[test]
    #[cfg(target_os = "macos")]
    fn fdatasync_all_writes_every_page_dirtied_through_the_mapping() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_fdatasync").expect("temp dir");
        let mut pf = PageFile::open(&tmp.path().join("db"), 64 << 20).expect("open");
        pf.ensure_len(16 << 20).expect("grow");
        for fill in [1, 2] {
            for pgno in 0..(16 << 20) / 4096 {
                pf.page_mut(pgno, 4096).expect("page").fill(fill);
            }
            pf.fdatasync_all().expect("fdatasync");
            assert_eq!(pf.dirty_os_pages(), 0, "fill {fill}: pages left dirty");
        }
    }

    /// A failed msync poisons the file: later writes and syncs fail without retrying (even though
    /// the failure was transient), while reads continue.
    #[test]
    fn failed_msync_poisons_the_file() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_poison").expect("temp dir");
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

    /// macOS: so does a failed `msync_ranges` (a scattered commit's data sync), which a poisoned
    /// file then refuses without an msync.
    #[test]
    #[cfg(target_os = "macos")]
    fn a_failed_msync_of_ranges_poisons_the_file() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_poison_ranges").expect("temp dir");
        let mut pf = PageFile::open(&tmp.path().join("db"), 16 << 20).expect("open");
        pf.ensure_len(65536).expect("grow");
        let ranges = || [0..4096_u64, 32768..36864].into_iter();
        pf.fail_next_msync();
        assert!(matches!(pf.msync_ranges(ranges()), Err(Error::Io(_))));
        assert!(pf.is_poisoned());
        assert!(matches!(pf.msync_ranges(ranges()), Err(Error::Failed(_))), "never retried");
    }

    /// Every mapping a page file makes is advised `MADV_RANDOM` (`advise_access`): the first and
    /// each remap's past the reservation, unless the file was mapped with `readahead`, which
    /// advises none. A read-only file's one mapping likewise.
    #[test]
    fn every_mapping_is_advised_random_unless_readahead() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_advice").expect("temp dir");
        for (readahead, want) in [(false, Some(Advice::Random)), (true, None)] {
            let path = tmp.path().join(format!("db-{readahead}"));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .expect("open");
            take_advice_log();
            let mut pf = PageFile::from_file(file, MIN_GROWTH, readahead, MAX_GROWTH).expect("map");
            let _reader = pf.view();
            pf.ensure_len(2 * MIN_GROWTH).expect("grow past the reservation");
            pf.ensure_len(8 * MIN_GROWTH).expect("grow past it again");
            assert_eq!(pf.retired.len(), 2, "two remaps");
            assert_eq!(
                take_advice_log(),
                [want; 3],
                "readahead {readahead}: open, then each remap"
            );
            drop(pf);
            let ro = ReadOnlyFile::from_file(File::open(&path).expect("open read-only"), readahead)
                .expect("map");
            assert_eq!(take_advice_log(), [want], "readahead {readahead}: a read-only mapping");
            drop(ro);
        }
    }

    /// A read-only mapping of a file opened read-only (mode 0444 here) reads its bytes up to the
    /// published length, and maps nothing once dropped.
    #[test]
    fn a_read_only_file_maps_its_bytes_until_dropped() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_ro").expect("temp dir");
        let path = tmp.path().join("db");
        let bytes: Vec<u8> = (0..3 * 4096_u32).map(|i| (i / 4096 + 1) as u8).collect();
        std::fs::write(&path, &bytes).expect("write");
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o444))
            .expect("chmod");
        let file = File::open(&path).expect("open read-only");
        let ro = ReadOnlyFile::from_file(file, false).expect("map");
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
        let tmp = TempDir::with_prefix("tn_lmdb_file_grow_fail").expect("temp dir");
        let mut pf = PageFile::open(&tmp.path().join("db"), 16 << 20).expect("open");
        assert!(pf.ensure_len(u64::MAX - 5).is_err(), "no file grows that large");
        assert!(pf.is_poisoned());
        assert!(matches!(pf.ensure_len(4096), Err(Error::Failed(_))));
    }

    /// A growth that fails after cutting a distrusted tail leaves the file at the pages in use,
    /// poisoned, each page below them as it was and none past them reachable; it reopens at
    /// that length with the same bytes.
    #[test]
    fn a_growth_failing_after_the_tail_cut_leaves_the_pages_in_use() {
        let tmp = TempDir::with_prefix("tn_lmdb_file_cut_fail").expect("temp dir");
        let path = tmp.path().join("db");
        let mut pf = PageFile::open(&path, 16 << 20).expect("open");
        let pages = 4;
        let used = u64::from(pages) * 4096;
        pf.ensure_len(used).expect("grow");
        for pgno in 0..pages {
            pf.page_mut(pgno, 4096).expect("page").fill(0xA0 + pgno as u8);
        }
        let assert_pages = |pf: &PageFile| {
            for pgno in 0..pages {
                let page = pf.page(pgno, 4096).expect("a page in use");
                assert!(page.iter().all(|&b| b == 0xA0 + pgno as u8), "page {pgno}");
            }
            assert_eq!(pf.page(pages, 4096), None, "past the pages in use");
        };
        assert!(pf.file_len() > used, "the growth left a tail");
        pf.distrust_tail(used);
        // The cut comes first; this target (no OS-page multiple holds it) is refused after it.
        assert!(matches!(pf.ensure_len(u64::MAX - 5), Err(Error::Failed(_))));
        assert!(pf.is_poisoned());
        assert_eq!(pf.file_len(), used);
        assert_eq!(std::fs::metadata(&path).expect("metadata").len(), used);
        assert_pages(&pf);
        drop(pf);

        let pf = PageFile::open(&path, 16 << 20).expect("reopen");
        assert!(!pf.is_poisoned());
        assert_eq!(pf.file_len(), used);
        assert_pages(&pf);
    }
}
