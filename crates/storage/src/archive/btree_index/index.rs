//! The [`BtreeIndex`]: a paged, on-disk B+tree mapping fixed-size byte keys to `u64` pack-file
//! offsets, with sorted point lookup plus (via [`super::iter`]) range/prefix/forward/reverse
//! iteration.
//!
//! The tree lives in a single `index.btx` file of fixed 4 KiB pages (page 0 is the header),
//! **memory-mapped** and worked on in place through [`MmapDataFile`] — no separate page cache; the
//! OS page cache is the cache. It complements the hash-based
//! [`HdxIndex`](crate::archive::digest_index::index::HdxIndex), which offers only point lookups.
//!
//! ## CRC (deferred, rebuildable)
//! Like the digest index, this index is not the durability source — it is derived from its data
//! log and rebuilt from it ([`BtreeIndex::rebuild_from`]) — so it does not pay a per-op CRC. A
//! modified page's 4-byte CRC trailer is `zero_crc`'d as a "dirty" marker and the page recorded as
//! written by this handle; [`Index::sync`] CRC-stamps exactly those pages. Reads do not verify the
//! full CRC ([`BtreeIndex::page_crc_scan`] is the off-path check), but a zero trailer on a page
//! this handle did not write is at-rest damage and surfaces as `CorruptIndex`, never a wrong answer
//! — and a write into such a page is refused rather than stamped valid at the next sync.
//!
//! ## Crash consistency (owner-driven rebuild)
//! [`Index::sync`] msyncs the data pages first and the header page (root pointer, page count,
//! `data_file_length`) last, so a header never names pages that did not reach disk with it. The
//! index is synced only at an explicit sync or a clean close (the file is `derived`: its size is
//! made durable by the clean-close seal). An index opened without a clean-close sentinel
//! ([`BtreeIndex::opened_unclean`]) is therefore never trusted — it may lag its log — and its owner
//! rebuilds it from the log and calls [`BtreeIndex::mark_consistent`] so the next clean close
//! seals it. A file shorter than the pages its header names (a lost size extension) is rejected
//! at open, as is a cleanly-sealed one whose root page fails its CRC.
//!
//! ## Copy-on-write and page reuse
//! A published page is never modified: a write copies the pages it changes, and readers of a
//! published [`IndexSnapshot`] need no lock. A replaced page is reused once no snapshot that could
//! reach it is alive (see [`BtreeIndex::publish`]). Reuse does not wait for a sync, so between
//! syncs the on-disk tree is not self-consistent — the rebuild-on-unclean contract above is what
//! makes that safe. The free list lives in memory; a clean reopen recovers it by walking the tree.

use std::{
    collections::VecDeque,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{fence, Ordering},
        Arc, Weak,
    },
};

use tn_types::B256;

use crate::archive::{
    btree_index::{
        header::{BtreeHeader, VALUE_SIZE},
        iter::{PageSource, MAX_DEPTH},
        page::{Node, NULL_PAGE, PAGE_SIZE},
    },
    crc::{add_crc32_nonzero, crc_is_zero, crc_state, zero_crc, CrcState},
    data_file::{fsync_directory, MapView, MmapAccess, MmapDataFile, MmapFileOptions, WriteMode},
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    index::Index,
    pack::DataHeader,
    page_set::PageSet,
};

/// Address space reserved for an index file's mapping (see
/// [`MmapFileOptions::reserve`](crate::archive::data_file::MmapFileOptions::reserve)): virtual
/// only, so the mapping never moves while the file stays within it; a file that outgrows it is
/// re-reserved at double the size (the one case in which the mapping moves). 1 GiB rather than
/// tens of GiB: on macOS the per-commit `msync` cost measurably grew with a 64 GiB reservation
/// (tndb one-write-plus-sync +5–8%), and was flat at 1 GiB.
pub(crate) const BTX_MAP_RESERVE: u64 = 1 << 30;

/// Map a read failure encountered on the write path into an append error.
fn fetch_to_append(e: FetchError) -> AppendError {
    match e {
        FetchError::IO(io) => AppendError::WriteDataError(io),
        FetchError::CrcFailed => AppendError::CrcError,
        FetchError::CorruptIndex(e) => AppendError::CorruptIndex(e),
        other => AppendError::SerializeValue(other.to_string()),
    }
}

/// Counts of data pages by CRC state — the B+tree analogue of the digest index's
/// `BucketCrcReport`. A clean, synced index reports zero of both; `dirty > 0` means writes were not
/// synced (rebuild from the pack), `corrupt > 0` means genuine on-disk corruption.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageCrcReport {
    /// Data pages whose CRC trailer is all-zero (written but not yet CRC'd / unsynced).
    pub dirty: u64,
    /// Data pages whose non-zero CRC fails to match the payload — genuine corruption.
    pub corrupt: u64,
}

/// A paged, mmap-backed on-disk B+tree "sortable index" over fixed `ksize`-byte keys → `u64` file
/// positions.  The key length is chosen at creation and recorded in the header (see
/// [`BtreeIndex::open_btx_file`]); keys are ordered lexicographically.
///
/// The primary API takes byte-slice keys (`&[u8]`); a fixed [`Index`] over `[u8; 32]` plus `B256`
/// digest adapters are provided for the common 32-byte case.
#[derive(Debug)]
pub struct BtreeIndex {
    header: BtreeHeader,
    file: MmapDataFile,
    /// Page geometry derived from the header's key size.
    node: Node,
    read_only: bool,
    synced: bool,
    /// Set by [`Self::set_remove_on_drop`]: the file is deleted on drop, so `Drop` skips the sync
    /// it would otherwise run.
    remove_on_drop: bool,
    /// Pages this handle created (allocated or copied) since the last publish: the only pages it
    /// modifies in place (copy-on-write — a published page is never modified). Their all-zero CRC
    /// trailer is the lazy-write marker; a zero trailer on any OTHER page is at-rest damage (every
    /// published page is CRC-stamped, and an unclean index is rebuilt rather than trusted). Empty
    /// on open, so a read-only handle trusts no zero-trailer page; drained by each publish,
    /// which stamps exactly this set.
    private: PageSet,
    /// Published pages replaced by a copy (copy-on-write) since the last publish: no longer in the
    /// working tree, but still in the latest published state.
    superseded: Vec<u32>,
    /// The pin shared by snapshots of the latest published state (`None` if none was taken).
    latest_pin: Option<Weak<SnapshotPin>>,
    /// Replaced pages per published state, oldest first, waiting until no snapshot can reach them.
    retiring: VecDeque<Retiring>,
    /// Pages no snapshot and not the working tree can reach: reused before the file grows.
    free: Vec<u32>,
    /// The lock-free reader view of the index file's mapping, shared with published snapshots.
    view: Arc<MapView>,
    _index_dir: PathBuf,
    /// Test-only failure injector: the page allocation after this many more fails.
    #[cfg(test)]
    fail_allocs_after: Option<usize>,
}

/// Held by every [`IndexSnapshot`] of one published state; the index keeps only a `Weak` to it. Its
/// strong count therefore drops to zero exactly when the last snapshot of that state is gone, and
/// a reader pays nothing for it beyond holding the snapshot.
#[derive(Debug)]
struct SnapshotPin;

/// Pages replaced after one published state, waiting until no snapshot can reach them.
#[derive(Debug)]
struct Retiring {
    /// The pin of the state these pages were last visible in (`None`: no snapshot was taken).
    pin: Option<Weak<SnapshotPin>>,
    pages: Vec<u32>,
}

/// True while some snapshot holds the pin.
fn pin_alive(pin: &Option<Weak<SnapshotPin>>) -> bool {
    pin.as_ref().is_some_and(|pin| pin.strong_count() > 0)
}

/// A published, immutable view of a B-tree: its root and page count at a [`BtreeIndex::publish`],
/// read through a [`MapView`] with no lock and no shared write. Published pages are never modified
/// (copy-on-write) and are always CRC-stamped, so a reader needs no coordination with the writer.
/// The index's owner must keep it open while a snapshot is in use (see [`MapView`]).
#[derive(Debug, Clone)]
pub(crate) struct IndexSnapshot {
    view: Arc<MapView>,
    node: Node,
    root: u32,
    page_count: u32,
    values: u64,
    /// Keeps this state's pages (and older states') from being reused while the snapshot lives.
    _pin: Arc<SnapshotPin>,
}

impl IndexSnapshot {
    /// Number of keys in the snapshot.
    pub(crate) fn len(&self) -> usize {
        self.values as usize
    }

    /// The file position stored for `key`, or [`FetchError::NotFound`].
    pub(crate) fn load(&self, key: &[u8]) -> Result<u64, FetchError> {
        lookup(self, key)
    }
}

impl PageSource for IndexSnapshot {
    fn node(&self) -> Node {
        self.node
    }

    fn root(&self) -> u32 {
        self.root
    }

    fn page(&self, p: u32) -> Result<&[u8], FetchError> {
        if p == 0 || p >= self.page_count {
            return Err(FetchError::CorruptIndex(format!(
                "page {p} is outside the published tree (page_count {})",
                self.page_count
            )));
        }
        let buf = self
            .view
            .slice(BtreeIndex::page_offset(p), PAGE_SIZE)
            .ok_or_else(|| FetchError::CorruptIndex(format!("page {p} is not published")))?;
        if crc_is_zero(buf) {
            return Err(FetchError::CorruptIndex(format!(
                "published page {p} has an all-zero CRC (at-rest corruption)"
            )));
        }
        Ok(buf)
    }
}

/// Point lookup of `key` in the tree `src` reads.
fn lookup<S: PageSource + ?Sized>(src: &S, key: &[u8]) -> Result<u64, FetchError> {
    let node = src.node();
    let mut pno = src.root();
    for _ in 0..MAX_DEPTH {
        let buf = src.page(pno)?;
        if node.is_leaf(buf) {
            return match node.leaf_search(buf, key) {
                Ok(i) => Ok(node.leaf_value(buf, i)),
                Err(_) => Err(FetchError::NotFound),
            };
        }
        let ci = node.internal_child_index(buf, key);
        pno = node.internal_child(buf, ci);
    }
    Err(FetchError::CorruptIndex("btree descent exceeded max depth".to_string()))
}

impl PageSource for BtreeIndex {
    fn node(&self) -> Node {
        self.node
    }

    fn root(&self) -> u32 {
        self.header.root_page
    }

    fn page(&self, p: u32) -> Result<&[u8], FetchError> {
        BtreeIndex::page(self, p)
    }
}

impl BtreeIndex {
    /// Open (or create) a B+tree index in directory `dir` (file `index.btx`).
    ///
    /// `ksize` is the key length in bytes.  Identity (`version`/`uid`/`appnum`) and geometry
    /// (`page_size`/`ksize`/`value_size`) are stamped from `data_header` and `ksize` on create and
    /// validated against them on reopen.  A fresh index starts as a single empty leaf.
    pub fn open_btx_file<P: AsRef<Path>>(
        dir: P,
        data_header: &DataHeader,
        ksize: u16,
        read_only: bool,
    ) -> Result<BtreeIndex, LoadHeaderError> {
        // Build the page geometry for this key size and check it fits a page (was a compile-time
        // assert on the const generic).  Do this before touching the filesystem.
        let node = Node::new(ksize as usize);
        if !node.geometry_ok() {
            return Err(LoadHeaderError::InvalidIndexGeometry);
        }

        let dir = dir.as_ref();
        // A read-only open must not create the index directory (an absent dir still fails
        // `NotFound` in `open_with` below), matching the digest index.
        let dir_created = !read_only && fs::create_dir(dir).is_ok();
        if dir_created {
            // Brand new index directory; fsync the parent so the entry survives a crash.
            if let Some(parent) = dir.parent() {
                let _ = fsync_directory(parent);
            }
        }
        // In-place page overwrites → Random write mode; point-lookup descent → Random access hint.
        // Derived from its data log, so its size waits for the seal (see the short-file check
        // below).
        let opts = MmapFileOptions {
            write_mode: WriteMode::Random,
            access: MmapAccess::Random,
            derived: true,
            reserve: BTX_MAP_RESERVE,
            ..Default::default()
        };
        let mut file = MmapDataFile::open_with(dir.join("index.btx"), read_only, opts)?;

        let fresh = file.is_empty();
        let header = if fresh {
            if read_only {
                return Err(LoadHeaderError::ReadOnlyEmpty);
            }
            let header = BtreeHeader::new(data_header, ksize);
            file.ensure_len(2 * PAGE_SIZE as u64)?;
            // Page 0: header (valid CRC — it is the commit marker).
            let page = header.to_page();
            file.slice_mut(0, PAGE_SIZE)
                .ok_or_else(|| io::Error::other("header page not mapped"))?
                .copy_from_slice(&page);
            // Page 1: the empty root leaf (valid, never-zero CRC).
            {
                let leaf = file
                    .slice_mut(PAGE_SIZE as u64, PAGE_SIZE)
                    .ok_or_else(|| io::Error::other("root leaf page not mapped"))?;
                node.init_leaf(leaf, NULL_PAGE, NULL_PAGE);
                add_crc32_nonzero(leaf);
            }
            file.sync_all()?; // msync the fresh empty tree
            let _ = fsync_directory(dir);
            header
        } else {
            let header = {
                let hbuf = file.slice(0, PAGE_SIZE).ok_or(LoadHeaderError::CrcFailed)?;
                BtreeHeader::from_page(hbuf)?
            };
            if header.version != data_header.version() {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum != data_header.appnum() {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid != data_header.uid() {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            // The on-disk page/key/value geometry must match this binary's compile-time layout,
            // or every offset computation would be wrong.  Reject like the identity fields.
            if header.page_size != PAGE_SIZE as u32
                || header.ksize != ksize
                || header.value_size != VALUE_SIZE
            {
                return Err(LoadHeaderError::InvalidIndexGeometry);
            }
            // A CRC-valid header can still be absurd (a writer bug or a forged page): every page it
            // names must lie inside the tree, and the height must stay under the descent cap.
            // Reject it so the writable doors rebuild from the data log.
            let in_tree = |p: u32| p >= 1 && p < header.page_count;
            if header.page_count < 2
                || !in_tree(header.root_page)
                || header.height == 0
                || header.height as usize > MAX_DEPTH
            {
                return Err(LoadHeaderError::InvalidIndexGeometry);
            }
            // The file must physically hold every page the header names. Barriers do not fsync a
            // derived file's size, so a crash after a sync can leave the header durable but a
            // growth's size extension lost: the named pages are gone, and zero-filling them would
            // trust a torn tree. Reject it so the owner rebuilds from the data log.
            let want = header.page_count as u64 * PAGE_SIZE as u64;
            if file.len() < want {
                return Err(LoadHeaderError::InvalidIndexGeometry);
            }
            // Trim any crash padding or torn tail past `page_count` when writable, so pages
            // allocated later start zero-filled. This is NOT recovery: an unclean index stays
            // `opened_unclean` (and unsealed) until its owner rebuilds it from the data log and
            // calls `mark_consistent`. A read-only handle maps as-is; reads are bounded by
            // `page_count`, so trailing junk is never addressed.
            if !read_only && file.len() > want {
                file.truncate(want)?;
            }
            // A cleanly-sealed index has a CRC-valid root (a clean close stamps every written
            // page). Checking it catches at-rest damage to the page every lookup starts from, so
            // the writable doors rebuild and read-only refuses. Skipped for an unclean file, whose
            // pages may legitimately be unstamped (it is rebuilt, not trusted).
            if !file.opened_unclean() {
                let root_valid = file
                    .slice(Self::page_offset(header.root_page), PAGE_SIZE)
                    .is_some_and(|buf| crc_state(buf) == CrcState::Valid);
                if !root_valid {
                    return Err(LoadHeaderError::CrcFailed);
                }
            }
            header
        };

        // The tree as opened is the published state readers may see.
        let view = file.view();
        view.publish_len(header.page_count as u64 * PAGE_SIZE as u64);
        // A brand-new tree has never been published, so its root leaf is still private: the first
        // writes change it in place instead of copying it.
        let mut private = PageSet::default();
        if fresh {
            private.insert(header.root_page);
        }
        let mut index = Self {
            header,
            file,
            node,
            read_only,
            synced: true,
            remove_on_drop: false,
            private,
            superseded: Vec::new(),
            latest_pin: None,
            retiring: VecDeque::new(),
            free: Vec::new(),
            view,
            _index_dir: dir.to_owned(),
            #[cfg(test)]
            fail_allocs_after: None,
        };
        // A cleanly-sealed tree's unreachable pages are free; an unclean one is rebuilt, not
        // trusted, and a read-only handle never allocates.
        if !read_only && !fresh && !index.opened_unclean() {
            index.free = index.unreachable_pages();
        }
        Ok(index)
    }

    /// Test-only: make the page allocation after `n` more fail (`None` clears it).
    #[cfg(test)]
    pub(crate) fn fail_allocations_after(&mut self, n: Option<usize>) {
        self.fail_allocs_after = n;
    }

    /// Pages in the file, including free ones (the allocation high-water mark).
    #[cfg(test)]
    pub(crate) fn page_count(&self) -> u32 {
        self.header.page_count
    }

    /// Number of keys stored in this index.
    pub fn len(&self) -> usize {
        self.header.values as usize
    }

    /// True if there are no keys stored in this index.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current height of the tree (1 = a single leaf).
    pub fn height(&self) -> u32 {
        self.header.height
    }

    /// The key length in bytes this index was created with.
    pub fn ksize(&self) -> usize {
        self.node.ksize()
    }

    /// True when the index file was opened without a valid clean-close sentinel. An unclean index
    /// is not trusted: it may lag its data log (the index is not synced on the hot path) or hold
    /// unstamped pages, so its owner must rebuild it from the log ([`Self::rebuild_from`]) and
    /// then [`Self::mark_consistent`].
    pub fn opened_unclean(&self) -> bool {
        self.file.opened_unclean()
    }

    /// Clear the "opened unclean" flag after a successful rebuild, so a clean `Drop` re-seals the
    /// file and the next open skips recovery. See
    /// [`MmapDataFile::mark_consistent`](crate::archive::data_file::MmapDataFile::mark_consistent).
    pub fn mark_consistent(&mut self) {
        self.file.mark_consistent();
    }

    /// Mark the index file to be removed (not synced) when this handle drops. Used to abandon a
    /// partial/failed build cheaply, skipping the drop-time sync.
    pub fn set_remove_on_drop(&mut self) {
        self.remove_on_drop = true;
        self.file.set_remove_on_drop();
    }

    /// Set the tracked length of the paired pack file (used by pack wrappers for crash repair);
    /// persisted with the header on the next [`Index::sync`].
    pub fn set_data_file_length(&mut self, data_file_length: u64) {
        if self.header.data_file_length != data_file_length {
            self.header.data_file_length = data_file_length;
            self.synced = false;
        }
    }

    /// The tracked length of the paired pack file.
    pub fn data_file_length(&self) -> u64 {
        self.header.data_file_length
    }

    /// Set the owner's value (any `u64` the owner keeps with the index; tndb keeps its dead-put
    /// count); persisted with the header on the next [`Index::sync`].
    pub fn set_owner_value(&mut self, value: u64) {
        if self.header.owner_value != value {
            self.header.owner_value = value;
            self.synced = false;
        }
    }

    /// The owner's value as last set and synced (0 if never set).
    pub fn owner_value(&self) -> u64 {
        self.header.owner_value
    }

    /// Classify the data pages by their trailing CRC — an off-hot-path integrity/verification hook
    /// (reads themselves never verify a CRC). Lets a pack wrapper decide whether to
    /// [`Self::rebuild_from`]: `dirty > 0` = unsynced writes, `corrupt > 0` = on-disk corruption.
    pub fn page_crc_scan(&self) -> PageCrcReport {
        let mut report = PageCrcReport::default();
        for p in 1..self.header.page_count {
            match self.file.slice(Self::page_offset(p), PAGE_SIZE) {
                Some(buf) => match crc_state(buf) {
                    CrcState::Valid => {}
                    CrcState::Dirty => report.dirty += 1,
                    CrcState::Corrupt => report.corrupt += 1,
                },
                None => report.corrupt += 1,
            }
        }
        report
    }

    /// Discard the current tree and rebuild it from `entries`.
    ///
    /// The index is deterministically derivable from the immutable pack it accompanies, so a pack
    /// wrapper can recover a corrupt index (see [`Self::page_crc_scan`]) by re-scanning the pack
    /// and feeding `(key, position)` pairs here. The index must be writable; call
    /// [`Index::sync`] afterwards to make the rebuild durable.
    pub fn rebuild_from<K, I>(&mut self, entries: I) -> Result<(), AppendError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (K, u64)>,
    {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.reset_empty()?;
        for (k, v) in entries {
            self.insert_kv(k.as_ref(), v)?;
        }
        Ok(())
    }

    /// Reset to a fresh, empty single-leaf tree, rewriting a consistent empty tree to disk so a
    /// crash mid-[`Self::rebuild_from`] reopens clean.
    fn reset_empty(&mut self) -> Result<(), io::Error> {
        // Truncation rewrites every page: never under a live snapshot.
        if pin_alive(&self.latest_pin) || self.retiring.iter().any(|r| pin_alive(&r.pin)) {
            return Err(io::Error::other(
                "cannot reset a B-tree index while snapshots of it are alive",
            ));
        }
        self.header.root_page = 1;
        self.header.height = 1;
        self.header.page_count = 2;
        self.header.values = 0;
        // Open-time recovery only (nothing published to readers yet): readers see nothing until
        // the rebuilt tree is published.
        self.view.publish_len(0);
        self.file.truncate(0)?; // unmap + truncate to nothing
        self.file.ensure_len(2 * PAGE_SIZE as u64)?; // grow back to header + root leaf (zero-filled)
        let page = self.header.to_page();
        self.file
            .slice_mut(0, PAGE_SIZE)
            .ok_or_else(|| io::Error::other("header page not mapped"))?
            .copy_from_slice(&page);
        {
            let leaf = self
                .file
                .slice_mut(PAGE_SIZE as u64, PAGE_SIZE)
                .ok_or_else(|| io::Error::other("root leaf page not mapped"))?;
            self.node.init_leaf(leaf, NULL_PAGE, NULL_PAGE);
            add_crc32_nonzero(leaf);
        }
        // The reset tree is unpublished: its root leaf stays private, so the rebuild's first writes
        // change it in place instead of copying it.
        self.private.clear();
        self.private.insert(self.header.root_page);
        self.superseded.clear();
        self.latest_pin = None;
        self.retiring.clear();
        self.free.clear();
        self.file.sync_all()?;
        self.synced = true;
        Ok(())
    }

    // ---- page IO (zero-copy over the mapping; no cache) ----

    fn page_offset(p: u32) -> u64 {
        p as u64 * PAGE_SIZE as u64
    }

    /// Borrow page `p`'s bytes directly from the mapping. Reads do not verify the full CRC (the
    /// mapping is trusted; [`Self::page_crc_scan`] is the off-path check), but two cheap checks
    /// turn damage into [`FetchError::CorruptIndex`] instead of a wrong answer: `p` must lie inside
    /// the tree, and a page with an all-zero CRC trailer must be one this handle wrote since its
    /// last sync (anywhere else the zero marker is at-rest damage, e.g. a zeroed page).
    fn page(&self, p: u32) -> Result<&[u8], FetchError> {
        if p == 0 || p >= self.header.page_count {
            return Err(FetchError::CorruptIndex(format!(
                "page {p} is outside the tree (page_count {})",
                self.header.page_count
            )));
        }
        let buf = self.file.slice(Self::page_offset(p), PAGE_SIZE).ok_or_else(|| {
            FetchError::CorruptIndex(format!("page {p} is beyond the mapped index"))
        })?;
        if crc_is_zero(buf) && !self.private.contains(p) {
            return Err(FetchError::CorruptIndex(format!(
                "page {p} has an all-zero CRC this handle did not write (at-rest corruption, not a \
                 live unsynced write)"
            )));
        }
        Ok(buf)
    }

    /// Borrow page `p`'s bytes mutably for in-place modification, recording it as written. The
    /// same checks as [`Self::page`] run first, so a write never lands in (and a later sync never
    /// CRC-stamps as valid) a page that is damaged at rest.
    fn page_mut(&mut self, p: u32) -> Result<&mut [u8], FetchError> {
        self.page(p)?;
        self.private.insert(p);
        self.file
            .slice_mut(Self::page_offset(p), PAGE_SIZE)
            .ok_or_else(|| FetchError::CorruptIndex(format!("page {p} is beyond the mapped index")))
    }

    /// Allocate the next page number (append-only bump allocator; no free list), growing the
    /// mapping to cover it. The grown region is zero-filled, so a fresh page reads as a zero-CRC
    /// (dirty) page — recorded as written by this handle — until it is CRC'd at [`Index::sync`].
    /// The page count only moves once the growth succeeded (a failed growth also poisons the file,
    /// so it never seals and is rebuilt on the next open).
    fn allocate_page(&mut self) -> Result<u32, io::Error> {
        #[cfg(test)]
        if let Some(left) = self.fail_allocs_after.as_mut() {
            if *left == 0 {
                return Err(io::Error::other("injected page allocation failure"));
            }
            *left -= 1;
        }
        if self.free.is_empty() {
            self.reclaim();
        }
        if let Some(p) = self.free.pop() {
            // A reused page starts exactly like a freshly grown one: all zeros, its zero CRC
            // trailer marking it private.
            self.file
                .slice_mut(Self::page_offset(p), PAGE_SIZE)
                .ok_or_else(|| io::Error::other("free page not mapped"))?
                .fill(0);
            self.private.insert(p);
            return Ok(p);
        }
        let p = self.header.page_count;
        self.file.ensure_len((p as u64 + 1) * PAGE_SIZE as u64)?;
        self.header.page_count += 1;
        self.private.insert(p);
        Ok(p)
    }

    // ---- copy-on-write ----

    /// The writable copy of page `p`: `p` itself if this handle created it since the last publish
    /// (no reader can see it), otherwise a fresh copy — a published page is never modified, so a
    /// snapshot reading it is unaffected. The replaced page is reused once no snapshot can reach
    /// it.
    fn make_writable(&mut self, p: u32) -> Result<u32, AppendError> {
        if self.private.contains(p) {
            return Ok(p);
        }
        let mut copy = [0_u8; PAGE_SIZE];
        copy.copy_from_slice(self.page(p).map_err(fetch_to_append)?);
        let q = self.allocate_page()?;
        let dst = self.page_mut(q).map_err(fetch_to_append)?;
        dst.copy_from_slice(&copy);
        zero_crc(dst);
        self.superseded.push(p);
        Ok(q)
    }

    /// Make the root-to-leaf path for `key` writable, top-down: a copied child is re-pointed in its
    /// (already writable) parent and a copied root becomes the working root. Returns the leaf and
    /// the internal `(page, child_index)` path, every page on it private.
    fn writable_path(&mut self, key: &[u8]) -> Result<(u32, Vec<(u32, usize)>), AppendError> {
        let node = self.node;
        let root = self.make_writable(self.header.root_page)?;
        self.header.root_page = root;
        let mut path = Vec::new();
        let mut pno = root;
        for _ in 0..MAX_DEPTH {
            let (ci, child) = {
                let buf = self.page(pno).map_err(fetch_to_append)?;
                if node.is_leaf(buf) {
                    return Ok((pno, path));
                }
                let ci = node.internal_child_index(buf, key);
                (ci, node.internal_child(buf, ci))
            };
            let writable = self.make_writable(child)?;
            if writable != child {
                let buf = self.page_mut(pno).map_err(fetch_to_append)?;
                node.set_internal_child(buf, ci, writable);
                zero_crc(buf);
            }
            path.push((pno, ci));
            pno = writable;
        }
        Err(AppendError::CorruptIndex("btree descent exceeded max depth".to_string()))
    }

    // ---- lookup ----

    fn get_value(&self, key: &[u8]) -> Result<u64, FetchError> {
        lookup(self, key)
    }

    // ---- insertion (copy-on-write: private pages change in place) ----

    fn insert_kv(&mut self, key: &[u8], val: u64) -> Result<(), AppendError> {
        let (leaf_no, path) = self.writable_path(key)?;
        self.insert_into_leaf(leaf_no, path, key, val)
    }

    fn insert_into_leaf(
        &mut self,
        leaf_no: u32,
        path: Vec<(u32, usize)>,
        key: &[u8],
        val: u64,
    ) -> Result<(), AppendError> {
        let node = self.node;
        // Decide the action from a read-only view of the leaf.
        let (found, at, n) = {
            let buf = self.page(leaf_no).map_err(fetch_to_append)?;
            let n = node.entry_count(buf);
            match node.leaf_search(buf, key) {
                Ok(i) => (Some(i), 0usize, n),
                Err(at) => (None, at, n),
            }
        };
        let full = n >= node.max_leaf_keys();
        if let Some(i) = found {
            // Duplicate key: overwrite the value in place; tree shape and count unchanged.
            {
                let buf = self.page_mut(leaf_no).map_err(fetch_to_append)?;
                node.set_leaf_value(buf, i, val);
                zero_crc(buf);
            }
            self.synced = false;
            return Ok(());
        }
        if !full {
            {
                let buf = self.page_mut(leaf_no).map_err(fetch_to_append)?;
                node.leaf_insert(buf, at, key, val);
                zero_crc(buf);
            }
            self.synced = false;
            self.header.values += 1;
            return Ok(());
        }
        // A full leaf splits. An insert past the end of the rightmost leaf is an append (ascending
        // keys), whose split keeps the left pages full.
        let append = at == n && self.is_rightmost(&path)?;
        // Every page the split can need is allocated before anything changes, so a failed
        // allocation (e.g. a full disk) leaves the tree as it was rather than half split.
        let mut reserve = self.reserve_split_pages(&path)?;
        self.split_leaf(leaf_no, path, at, key, val, append, &mut reserve)?;
        debug_assert!(reserve.is_empty(), "the split used every reserved page");
        self.header.values += 1;
        Ok(())
    }

    /// True if `path` is the tree's rightmost path (every step takes the last child).
    fn is_rightmost(&self, path: &[(u32, usize)]) -> Result<bool, AppendError> {
        for &(p, ci) in path {
            if ci != self.node.entry_count(self.page(p).map_err(fetch_to_append)?) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Allocate every page splitting a full leaf on `path` can need: the new right leaf, a new
    /// right page for each full internal page above it (the split climbs while parents are full),
    /// and a new root if the root splits. On a failure the pages taken so far go back to the free
    /// list and nothing else has changed.
    fn reserve_split_pages(&mut self, path: &[(u32, usize)]) -> Result<Vec<u32>, AppendError> {
        let node = self.node;
        let mut needed = 1;
        let mut root_splits = true;
        for &(p, _) in path.iter().rev() {
            if node.entry_count(self.page(p).map_err(fetch_to_append)?) < node.max_internal_keys() {
                root_splits = false;
                break;
            }
            needed += 1;
        }
        if root_splits {
            needed += 1;
        }
        let mut pages = Vec::with_capacity(needed);
        for _ in 0..needed {
            match self.allocate_page() {
                Ok(p) => pages.push(p),
                Err(e) => {
                    self.free.extend(pages);
                    return Err(e.into());
                }
            }
        }
        Ok(pages)
    }

    /// Split the full leaf `leaf_no` (inserting `(key, val)` at `at`) using pages from `reserve`
    /// (see [`Self::reserve_split_pages`]), then insert the separator up the `path`.
    #[allow(clippy::too_many_arguments)]
    fn split_leaf(
        &mut self,
        leaf_no: u32,
        path: Vec<(u32, usize)>,
        at: usize,
        key: &[u8],
        val: u64,
        append: bool,
        reserve: &mut Vec<u32>,
    ) -> Result<(), AppendError> {
        let node = self.node;
        let right_no = reserve.pop().expect("a reserved page for the right leaf");

        // Split the left leaf in place; build the right leaf in a scratch buffer. Leaves are not
        // linked (copy-on-write could not keep sibling links current), so both links stay null.
        let mut rbuf = vec![0_u8; PAGE_SIZE];
        let sep = {
            let left = self.page_mut(leaf_no).map_err(fetch_to_append)?;
            let sep = node.leaf_split(left, &mut rbuf, at, key, val, append);
            node.set_leaf_prev(left, NULL_PAGE);
            node.set_leaf_next(left, NULL_PAGE);
            zero_crc(left);
            sep
        };
        node.set_leaf_prev(&mut rbuf, NULL_PAGE);
        node.set_leaf_next(&mut rbuf, NULL_PAGE);
        zero_crc(&mut rbuf);
        {
            let r = self.page_mut(right_no).map_err(fetch_to_append)?;
            r.copy_from_slice(&rbuf);
        }
        self.synced = false;
        self.insert_into_parent(path, sep, right_no, append, reserve)
    }

    /// Insert `(sep, right_no)` into the parent, splitting internal nodes and growing a new root
    /// as needed, with pages from `reserve`. `append` is set on the rightmost path (see
    /// [`Node::internal_split`]).
    fn insert_into_parent(
        &mut self,
        mut path: Vec<(u32, usize)>,
        sep: Vec<u8>,
        right_no: u32,
        append: bool,
        reserve: &mut Vec<u32>,
    ) -> Result<(), AppendError> {
        let node = self.node;
        let mut sep = sep;
        let mut right_no = right_no;
        while let Some((pno, ci)) = path.pop() {
            let has_room = {
                let buf = self.page(pno).map_err(fetch_to_append)?;
                node.entry_count(buf) < node.max_internal_keys()
            };
            if has_room {
                {
                    let buf = self.page_mut(pno).map_err(fetch_to_append)?;
                    node.internal_insert(buf, ci, &sep, right_no);
                    zero_crc(buf);
                }
                self.synced = false;
                return Ok(());
            }
            // Internal node full: split it in place (right half to scratch) and propagate the
            // median.
            let new_right_no = reserve.pop().expect("a reserved page for the right internal page");
            let mut qbuf = vec![0_u8; PAGE_SIZE];
            let median = {
                let p = self.page_mut(pno).map_err(fetch_to_append)?;
                let median = node.internal_split(p, &mut qbuf, ci, &sep, right_no, append);
                zero_crc(p);
                median
            };
            zero_crc(&mut qbuf);
            {
                let q = self.page_mut(new_right_no).map_err(fetch_to_append)?;
                q.copy_from_slice(&qbuf);
            }
            sep = median;
            right_no = new_right_no;
        }
        // Path exhausted with a pending split: grow a new root one level up.
        let new_root_no = reserve.pop().expect("a reserved page for the new root");
        let old_root = self.header.root_page;
        {
            let r = self.page_mut(new_root_no).map_err(fetch_to_append)?;
            node.init_internal(r, old_root);
            node.internal_insert(r, 0, &sep, right_no);
            zero_crc(r);
        }
        self.header.root_page = new_root_no;
        self.header.height += 1;
        self.synced = false;
        Ok(())
    }

    // ---- removal ----

    fn remove_kv(&mut self, key: &[u8]) -> Result<bool, AppendError> {
        // Check first so a miss copies nothing (copy-on-write would otherwise replace the path).
        match lookup(self, key) {
            Err(FetchError::NotFound) => return Ok(false),
            Err(e) => return Err(fetch_to_append(e)),
            Ok(_) => {}
        }
        let node = self.node;
        let (leaf_no, path) = self.writable_path(key)?;
        let emptied = {
            let buf = self.page_mut(leaf_no).map_err(fetch_to_append)?;
            let Ok(i) = node.leaf_search(buf, key) else {
                return Err(AppendError::CorruptIndex("key vanished from its leaf".to_string()));
            };
            node.leaf_delete(buf, i);
            zero_crc(buf);
            node.entry_count(buf) == 0
        };
        self.header.values -= 1;
        self.synced = false;
        if emptied && !path.is_empty() {
            self.unlink_emptied(leaf_no, path)?;
        }
        Ok(true)
    }

    /// Unlink the emptied page `emptied` from the tree: drop it from its parent on the writable
    /// `path`, and the parent too while it was the only child, up the path (an emptied root becomes
    /// an empty leaf); then collapse a root left with a single child. Pages on the path are fresh
    /// private copies, so an unlinked one is free at once. Under-full pages are not merged; only
    /// empty ones go, so the tree never keeps (or scans) pages that hold nothing.
    fn unlink_emptied(
        &mut self,
        mut emptied: u32,
        mut path: Vec<(u32, usize)>,
    ) -> Result<(), AppendError> {
        let node = self.node;
        while let Some((parent, ci)) = path.pop() {
            self.retire_page(emptied);
            if node.entry_count(self.page(parent).map_err(fetch_to_append)?) > 0 {
                let buf = self.page_mut(parent).map_err(fetch_to_append)?;
                node.internal_remove_child(buf, ci);
                zero_crc(buf);
                return self.collapse_root();
            }
            // `emptied` was the parent's only child: the parent empties too.
            emptied = parent;
        }
        // Every page on the path emptied, the root included: it becomes an empty leaf.
        let root = self.header.root_page;
        let buf = self.page_mut(root).map_err(fetch_to_append)?;
        node.init_leaf(buf, NULL_PAGE, NULL_PAGE);
        zero_crc(buf);
        self.header.height = 1;
        Ok(())
    }

    /// While the root is an internal page with a single child (no keys), make that child the root.
    fn collapse_root(&mut self) -> Result<(), AppendError> {
        let node = self.node;
        while self.header.height > 1 {
            let root = self.header.root_page;
            let child = {
                let buf = self.page(root).map_err(fetch_to_append)?;
                if node.entry_count(buf) > 0 {
                    break;
                }
                node.internal_child(buf, 0)
            };
            self.retire_page(root);
            self.header.root_page = child;
            self.header.height -= 1;
        }
        Ok(())
    }

    /// Take page `p` out of the working tree: free at once if no snapshot can see it (this handle
    /// created it since the last publish), otherwise retired until no snapshot can reach it.
    fn retire_page(&mut self, p: u32) {
        if self.private.contains(p) {
            self.free.push(p);
        } else {
            self.superseded.push(p);
        }
    }

    /// The key-size check of the public point ops: `key` must be `ksize()` bytes.
    fn key_size_ok(&self, key: &[u8]) -> Result<(), (usize, usize)> {
        let expected = self.node.ksize();
        if key.len() == expected {
            Ok(())
        } else {
            Err((expected, key.len()))
        }
    }

    /// Remove `key` from the index. Returns `true` if the key was present and removed, `false` if
    /// not found. An emptied leaf is unlinked (see [`Self::unlink_emptied`]); under-full ones are
    /// not merged. A key of the wrong size is an error.
    pub fn remove(&mut self, key: &[u8]) -> Result<bool, AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.key_size_ok(key).map_err(|(expected, got)| AppendError::KeySize { expected, got })?;
        self.remove_kv(key)
    }

    // ---- point API (byte-slice keys; the index's key length is `ksize()`) ----

    /// Save the file position `record_pos` for `key` (inserting or overwriting). A key of the
    /// wrong size is an error.
    pub fn save(&mut self, key: &[u8], record_pos: u64) -> Result<(), AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.key_size_ok(key).map_err(|(expected, got)| AppendError::KeySize { expected, got })?;
        self.synced = false;
        self.insert_kv(key, record_pos)
    }

    /// Load the file position for `key`, or [`FetchError::NotFound`]. A key of the wrong size is
    /// an error.
    pub fn load(&self, key: &[u8]) -> Result<u64, FetchError> {
        self.key_size_ok(key).map_err(|(expected, got)| FetchError::KeySize { expected, got })?;
        self.get_value(key)
    }

    /// True if the index contains `key`.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.load(key).is_ok()
    }

    /// Flush and sync all index data to disk (see the lazy-CRC, header-last commit regime).
    pub fn sync(&mut self) -> Result<(), CommitError> {
        self.sync_impl()
    }

    // ---- durability (lazy CRC + msync, header-last commit) ----

    /// CRC-stamp the pages this handle wrote since the last sync (and only those), consuming the
    /// set. Never iterating every page keeps sync O(written) and, crucially, never re-stamps a page
    /// zeroed at rest as valid — that page stays dirty, so lookups and [`Self::page_crc_scan`]
    /// still flag it. Page 0 (the header) is written separately.
    fn crc_dirty_pages(&mut self) {
        for p in self.private.drain() {
            let p = p as u32; // inserted as a u32 page number
            if let Some(buf) = self.file.slice_mut(Self::page_offset(p), PAGE_SIZE) {
                if crc_is_zero(buf) {
                    // `crc_state` classifies pages, so stamp never-zero: a genuine CRC of 0 must
                    // not be re-read as the all-zero dirty marker.
                    add_crc32_nonzero(buf);
                }
            }
        }
    }

    /// Make every page this handle created since the last publish immutable and visible: stamp
    /// their CRCs (they are never modified again; later writes copy them) and extend the readers'
    /// view over them.
    fn publish_pages(&mut self) {
        self.crc_dirty_pages();
        self.view.publish_len(self.header.page_count as u64 * PAGE_SIZE as u64);
        // The pages replaced since the previous publish were last visible in the previous state.
        let pin = self.latest_pin.take().filter(|pin| pin.strong_count() > 0);
        let pages = std::mem::take(&mut self.superseded);
        if pin.is_some() || !pages.is_empty() {
            self.retiring.push_back(Retiring { pin, pages });
        }
    }

    /// Publish the working tree: every write since the last publish becomes visible to readers of
    /// the returned snapshot (and of later ones). Durability is separate ([`Self::sync`]).
    ///
    /// The snapshot pins the published state: pages it can reach are not reused until it (and
    /// every snapshot of an older state) is dropped, so a long-lived snapshot lets the file grow
    /// meanwhile.
    pub(crate) fn publish(&mut self) -> IndexSnapshot {
        self.publish_pages();
        let pin = Arc::new(SnapshotPin);
        self.latest_pin = Some(Arc::downgrade(&pin));
        IndexSnapshot {
            view: Arc::clone(&self.view),
            node: self.node,
            root: self.header.root_page,
            page_count: self.header.page_count,
            values: self.header.values,
            _pin: pin,
        }
    }

    /// Free the pages no snapshot can reach any more: pop retiring states from the front while no
    /// snapshot of them is alive. Front only: a state's replaced pages may be reachable from any
    /// older state too, and each older entry was popped only once its own snapshots were gone.
    fn reclaim(&mut self) {
        let mut freed = false;
        while self.retiring.front().is_some_and(|front| !pin_alive(&front.pin)) {
            let Some(entry) = self.retiring.pop_front() else { break };
            self.free.extend(entry.pages);
            freed = true;
        }
        if freed {
            // Pairs with the `Release` decrement of each snapshot's last `Arc` drop, so a reader's
            // final reads of a freed page happen before this writer reuses it.
            fence(Ordering::Acquire);
        }
    }

    /// Every page of the tree at `root` with `height` levels. Internal pages are read (each must be
    /// internal, with a sane entry count and in-bounds children never seen before); leaves are
    /// taken from their parents' child pointers, and only the first is read (it must be a leaf:
    /// the tree is balanced, so that checks `height`). `verify_crc` also checks each internal
    /// page's full CRC.
    fn tree_pages(&self, root: u32, height: u32, verify_crc: bool) -> Result<PageSet, FetchError> {
        let node = self.node;
        let corrupt = |what: String| FetchError::CorruptIndex(format!("btree walk: {what}"));
        let mut seen = PageSet::default();
        seen.insert(root);
        let mut level = vec![root];
        for _ in 1..height {
            let mut next = Vec::new();
            for &p in &level {
                let buf = self.page(p)?;
                if verify_crc && crc_state(buf) != CrcState::Valid {
                    return Err(corrupt(format!("page {p} fails its CRC")));
                }
                let n = node.entry_count(buf);
                if node.is_leaf(buf) || n > node.max_internal_keys() {
                    return Err(corrupt(format!("page {p} is not a sane internal page")));
                }
                for i in 0..=n {
                    let c = node.internal_child(buf, i);
                    if c == 0 || c >= self.header.page_count || seen.contains(c) {
                        return Err(corrupt(format!("page {p} has a bad child {c}")));
                    }
                    seen.insert(c);
                    next.push(c);
                }
            }
            level = next;
        }
        if !node.is_leaf(self.page(level[0])?) {
            return Err(corrupt(format!("height {height} does not reach the leaves")));
        }
        Ok(seen)
    }

    /// The pages a cleanly-sealed tree does not reach (free to reuse), lowest popped first. Any
    /// walk failure yields none: a page that might be live is never reused, and the damage
    /// surfaces on the lookups that reach it, as before.
    fn unreachable_pages(&self) -> Vec<u32> {
        match self.tree_pages(self.header.root_page, self.header.height, true) {
            Ok(reachable) => {
                (1..self.header.page_count).rev().filter(|&p| !reachable.contains(p)).collect()
            }
            Err(e) => {
                tracing::warn!(target: "btree-index", "no page reuse until rebuilt: {e}");
                Vec::new()
            }
        }
    }

    /// Reset to an empty tree without touching published pages: a fresh empty root leaf becomes the
    /// working root (published snapshots keep reading the old tree). The old tree's pages are
    /// retired: reused once no snapshot can reach them (at once for pages never published).
    pub fn clear(&mut self) -> Result<(), AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        let node = self.node;
        let mut old = self
            .tree_pages(self.header.root_page, self.header.height, false)
            .map_err(fetch_to_append)?;
        let leaf = self.allocate_page()?;
        let buf = self.page_mut(leaf).map_err(fetch_to_append)?;
        node.init_leaf(buf, NULL_PAGE, NULL_PAGE);
        zero_crc(buf);
        self.header.root_page = leaf;
        self.header.height = 1;
        self.header.values = 0;
        self.synced = false;
        for p in old.drain() {
            // A page never published goes straight to `free` (it stays private, stamped at the
            // next publish, until reused); a published one waits until no snapshot can reach it.
            self.retire_page(p as u32); // inserted as a u32 page number
        }
        Ok(())
    }

    /// Write the in-memory header into page 0 (with a valid CRC — the commit marker).
    fn write_header(&mut self) -> Result<(), io::Error> {
        let page = self.header.to_page();
        self.file
            .slice_mut(0, PAGE_SIZE)
            .ok_or_else(|| io::Error::other("header page not mapped"))?
            .copy_from_slice(&page);
        Ok(())
    }

    fn sync_impl(&mut self) -> Result<(), CommitError> {
        if self.read_only {
            return Err(CommitError::ReadOnly);
        }
        // Publish (CRC-stamp) and msync all data pages BEFORE rewriting/msyncing the header, so the
        // header (root pointer, page_count) never becomes durable ahead of the pages it names.
        self.publish_pages();
        self.file.sync_all().map_err(CommitError::IndexFileSync)?;
        self.write_header().map_err(CommitError::IndexFileSync)?;
        self.file.sync_range(0, PAGE_SIZE as u64).map_err(CommitError::IndexFileSync)?;
        self.synced = true;
        Ok(())
    }
}

/// A fixed `[u8; 32]` [`Index`] impl so the generic point-index bench harness (and any other
/// `Index<K, u64>` consumer) can drive a 32-byte-key B+tree; it forwards to the inherent byte-slice
/// API.  It is only reachable through a generic `Index` bound — on a concrete `BtreeIndex`,
/// `save`/`load`/`sync` resolve to the inherent methods (inherent-method priority).
impl Index<[u8; 32], u64> for BtreeIndex {
    fn save(&mut self, key: [u8; 32], record_pos: u64) -> Result<(), AppendError> {
        self.save(&key, record_pos)
    }

    fn load(&mut self, key: [u8; 32]) -> Result<u64, FetchError> {
        BtreeIndex::load(self, &key)
    }

    fn sync(&mut self) -> Result<(), CommitError> {
        self.sync()
    }
}

/// Convenience adapters so 32-byte digests ([`B256`]) can be used as keys without manual
/// conversion, matching how [`HdxIndex`](crate::archive::digest_index::index::HdxIndex) is called.
/// `B256` is a newtype over `[u8; 32]`, so these forward to the byte-slice API (valid on a 32-byte
/// index; otherwise the length guard trips).
impl BtreeIndex {
    /// Save a `B256` digest → file position mapping (see [`BtreeIndex::save`]).
    pub fn save_digest(&mut self, key: B256, record_pos: u64) -> Result<(), AppendError> {
        self.save(&key.0, record_pos)
    }

    /// Load the file position for a `B256` digest (see [`BtreeIndex::load`]).
    pub fn load_digest(&self, key: B256) -> Result<u64, FetchError> {
        BtreeIndex::load(self, &key.0)
    }

    /// Remove a `B256` digest key (see [`BtreeIndex::remove`]).
    pub fn remove_digest(&mut self, key: B256) -> Result<bool, AppendError> {
        self.remove(&key.0)
    }
}

impl Drop for BtreeIndex {
    fn drop(&mut self) {
        if !self.read_only && !self.synced && !self.remove_on_drop {
            // The data log is the source of truth and the index is not synced on the hot path, so
            // a clean close is the expected place it is made durable — not a misuse to warn about.
            // Commit the tree (pages, then the header) before MmapDataFile's own clean-close seal.
            if let Err(e) = self.sync_impl() {
                if !std::thread::panicking() {
                    tracing::error!("BtreeIndex: failed to sync on drop: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tn_types::DefaultHashFunction;

    use super::*;
    use crate::archive::{data_file::SENTINEL_LEN, pack::PackCompression};

    /// Deterministic 32-byte key from an integer.
    fn key_of(i: u64) -> [u8; 32] {
        let mut hasher = DefaultHashFunction::new();
        hasher.update(format!("btx-{i}").as_bytes());
        let mut k = [0_u8; 32];
        k.copy_from_slice(hasher.finalize().as_bytes());
        k
    }

    #[test]
    fn test_archive_btx_basic_and_reopen() {
        let tmp = TempDir::with_prefix("test_archive_btx_basic").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        // Empty tree: nothing found.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            assert!(idx.is_empty());
            assert!(matches!(idx.load(&key_of(0)), Err(FetchError::NotFound)));
            // A handful of keys, then sync.
            for i in 0..500 {
                idx.save(&key_of(i), i).expect("save");
            }
            assert_eq!(idx.len(), 500);
            for i in 0..500 {
                assert_eq!(idx.load(&key_of(i)).expect("load"), i);
            }
            idx.sync().expect("sync");
        }

        // Reopen read-write, verify, add more.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
            assert_eq!(idx.len(), 500);
            for i in 0..500 {
                assert_eq!(idx.load(&key_of(i)).expect("load"), i);
            }
            // Exercise the B256 write adapter on the way in.
            for i in 500..800 {
                idx.save_digest(B256::from(key_of(i)), i).expect("save_digest");
            }
            idx.sync().expect("sync");
        }

        // Reopen read-only, verify all, and confirm write/sync are rejected.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
            assert_eq!(idx.len(), 800);
            for i in 0..800 {
                assert_eq!(idx.load(&key_of(i)).expect("load"), i);
            }
            // B256 read adapter resolves to the same entry.
            assert_eq!(idx.load_digest(B256::from(key_of(7))).expect("load_digest"), 7);
            assert!(matches!(idx.save(&key_of(0), 0), Err(AppendError::ReadOnly)));
            assert!(matches!(idx.sync(), Err(CommitError::ReadOnly)));
        }
    }

    #[test]
    fn test_archive_btx_million_with_splits() {
        let tmp = TempDir::with_prefix("test_archive_btx_million").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
        for i in 0..1_000_000u64 {
            idx.save(&key_of(i), i).unwrap_or_else(|e| panic!("save {i}: {e}"));
        }
        assert_eq!(idx.len(), 1_000_000);
        // A million random-ish keys must have grown the tree past a single leaf.
        assert!(idx.height() >= 3, "expected a multi-level tree, got height {}", idx.height());
        for i in 0..1_000_000u64 {
            assert_eq!(idx.load(&key_of(i)).unwrap_or_else(|e| panic!("load {i}: {e}")), i);
        }

        // Duplicate key overwrites the value; count is unchanged.
        idx.save(&key_of(42), 999_999_999).expect("overwrite");
        assert_eq!(idx.load(&key_of(42)).expect("load dup"), 999_999_999);
        assert_eq!(idx.len(), 1_000_000);
        idx.sync().expect("sync");
        // A fully synced tree has no dirty or corrupt pages.
        assert_eq!(idx.page_crc_scan(), PageCrcReport::default());
        drop(idx);

        // Reopen read-only and re-verify persistence across the split-heavy tree.
        let idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
        assert_eq!(idx.len(), 1_000_000);
        for i in (0..1_000_000u64).step_by(7) {
            let expect = if i == 42 { 999_999_999 } else { i };
            assert_eq!(idx.load(&key_of(i)).expect("load"), expect, "mismatch at {i}");
        }
    }

    #[test]
    fn test_archive_btx_geometry_mismatch() {
        let tmp = TempDir::with_prefix("test_archive_btx_geometry").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            idx.save(&key_of(1), 1).expect("save");
            idx.sync().expect("sync");
        }

        // Reopen with a different key size (16) -> geometry mismatch.
        let res = BtreeIndex::open_btx_file(&dir, &data_header, 16, false);
        assert!(
            matches!(res, Err(LoadHeaderError::InvalidIndexGeometry)),
            "expected InvalidIndexGeometry, got {res:?}"
        );
    }

    #[test]
    fn test_archive_btx_uid_mismatch() {
        let tmp = TempDir::with_prefix("test_archive_btx_uid").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            idx.save(&key_of(1), 1).expect("save");
            idx.sync().expect("sync");
        }
        // A DataHeader built from a different uid_idx must be rejected.
        let other = DataHeader::new(7, PackCompression::ZStd, 0);
        let res = BtreeIndex::open_btx_file(&dir, &other, 32, true);
        assert!(
            matches!(res, Err(LoadHeaderError::InvalidIndexUID)),
            "expected InvalidIndexUID, got {res:?}"
        );
    }

    /// A torn tail (here: junk after the clean-close sentinel) makes the index unclean, and an
    /// unclean index is never trusted on its own: closing it without `mark_consistent` leaves it
    /// unsealed, and only a rebuild from the data log plus `mark_consistent` seals it again.
    #[test]
    fn test_archive_btx_torn_tail_is_unclean_until_rebuilt() {
        use std::io::Write as _;

        let tmp = TempDir::with_prefix("test_archive_btx_torn").expect("temp dir");
        let dir = tmp.path().join("idx");
        let file = dir.join("index.btx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for i in 0..2_000 {
                idx.save(&key_of(i), i).expect("save");
            }
            idx.sync().expect("sync");
        }
        let before = std::fs::metadata(&file).expect("meta").len();
        // MmapDataFile appends an 8-byte clean-close sentinel on drop, so the physical size is
        // page_count * PAGE_SIZE + SENTINEL_LEN, not a bare multiple of PAGE_SIZE.
        assert!(
            (before - SENTINEL_LEN).is_multiple_of(PAGE_SIZE as u64),
            "clean close leaves whole pages (excluding sentinel): before={before}"
        );

        // Simulate a torn tail: append a few sub-page bytes past the committed pages.
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&file).expect("append");
            f.write_all(&[0xAB, 0xCD, 0xEF]).expect("write torn");
            f.sync_all().expect("sync");
        }
        assert_eq!(std::fs::metadata(&file).expect("meta").len(), before + 3);

        // The reopen reports unclean. Closing without a rebuild + `mark_consistent` must not seal
        // it: the next open is unclean again.
        {
            let idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
            assert!(idx.opened_unclean(), "a torn tail is an unclean close");
        }
        let entries: Vec<([u8; 32], u64)> = (0..2_000).map(|i| (key_of(i), i)).collect();
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
            assert!(idx.opened_unclean(), "an unclean index must not seal itself on close");
            // The owner's recovery: rebuild from the data log, then mark consistent.
            idx.rebuild_from(entries.iter().copied()).expect("rebuild");
            idx.mark_consistent();
        }
        assert_eq!(
            std::fs::metadata(&file).expect("meta").len(),
            before,
            "the rebuilt index seals to whole pages plus the sentinel"
        );
        let idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
        assert!(!idx.opened_unclean(), "rebuilt and marked consistent: sealed clean");
        for (k, v) in &entries {
            assert_eq!(idx.load(k).expect("load"), *v);
        }
    }

    /// A btx shorter than its header's `page_count` — a growth's size extension lost in a crash,
    /// which barriers do not fsync for a derived file — is rejected so its owner rebuilds it, never
    /// zero-filled and trusted.
    #[test]
    fn test_archive_btx_short_file_rejected() {
        let tmp = TempDir::with_prefix("test_archive_btx_short").expect("temp dir");
        let dir = tmp.path().join("idx");
        let file = dir.join("index.btx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for i in 0..2_000 {
                idx.save(&key_of(i), i).expect("save");
            }
            idx.sync().expect("sync");
        }
        // Drop the last committed page, and the clean-close sentinel with it.
        let len = std::fs::metadata(&file).expect("meta").len();
        let short = len - SENTINEL_LEN - PAGE_SIZE as u64;
        let f = std::fs::OpenOptions::new().write(true).open(&file).expect("open file");
        f.set_len(short).expect("truncate");
        drop(f);

        for read_only in [false, true] {
            let Err(err) = BtreeIndex::open_btx_file(&dir, &data_header, 32, read_only) else {
                panic!("a btx missing committed pages must not open (read_only={read_only})");
            };
            assert!(matches!(err, LoadHeaderError::InvalidIndexGeometry), "{err:?}");
        }
        assert_eq!(
            std::fs::metadata(&file).expect("meta").len(),
            short,
            "rejection leaves the file"
        );
    }

    /// Overwrite `len` bytes at `offset` of a closed index file.
    fn write_at(file: &Path, offset: u64, bytes: &[u8]) {
        use std::io::{Seek as _, SeekFrom, Write as _};
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(file).expect("open rw");
        f.seek(SeekFrom::Start(offset)).expect("seek");
        f.write_all(bytes).expect("write");
        f.sync_all().expect("sync");
    }

    /// A sealed 2,000-key index whose leftmost leaf (page 1, never the root at this size) was
    /// zeroed at rest. Returns the index dir, its data header, and the smallest key (which lives
    /// in that leaf).
    fn sealed_index_with_zeroed_first_leaf(tmp: &TempDir) -> (PathBuf, DataHeader, [u8; 32]) {
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for i in 0..2_000 {
                idx.save(&key_of(i), i).expect("save");
            }
            assert_ne!(idx.header.root_page, 1, "page 1 must not be the root here");
        }
        write_at(&dir.join("index.btx"), PAGE_SIZE as u64, &[0_u8; PAGE_SIZE]);
        let smallest = (0..2_000).map(key_of).min().expect("keys");
        (dir, data_header, smallest)
    }

    /// A page zeroed AT REST in a sealed index is not laundered back to valid by a later sync,
    /// and a lookup that reaches it surfaces `CorruptIndex` — not a silent `NotFound`. A zero CRC
    /// trailer is the lazy-write marker only for a page the handle wrote since its last sync.
    #[test]
    fn test_archive_btx_zeroed_page_not_laundered_and_errors_on_read() {
        let tmp = TempDir::with_prefix("test_archive_btx_zeroed").expect("temp dir");
        let (dir, data_header, smallest) = sealed_index_with_zeroed_first_leaf(&tmp);

        let idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
        assert!(!idx.opened_unclean(), "content damage leaves the seal intact");
        assert!(
            matches!(idx.load(&smallest), Err(FetchError::CorruptIndex(_))),
            "a lookup into the zeroed leaf must be CorruptIndex"
        );
        // The iterator fetches its first leaf eagerly, so the error may surface from `iter()`.
        let first = idx.iter().and_then(|mut scan| scan.next().transpose());
        assert!(
            matches!(first, Err(FetchError::CorruptIndex(_))),
            "a scan starting at the zeroed leaf must be CorruptIndex, got {first:?}"
        );
        drop(idx);

        // A writable handle that syncs without touching the page must not stamp it valid.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
            idx.set_data_file_length(1_000); // force a real sync on close
            idx.sync().expect("sync");
        }
        let idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
        assert_eq!(idx.page_crc_scan().dirty, 1, "the zeroed page stays dirty (not laundered)");
    }

    /// A `save` into a page that is damaged at rest is refused with `CorruptIndex`, rather than
    /// overwriting it and having the next sync stamp it valid.
    #[test]
    fn test_archive_btx_save_into_at_rest_dirty_page_is_refused() {
        let tmp = TempDir::with_prefix("test_archive_btx_refuse").expect("temp dir");
        let (dir, data_header, smallest) = sealed_index_with_zeroed_first_leaf(&tmp);

        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
        // The all-zero key sorts first, so it descends into the zeroed leaf.
        assert!(matches!(idx.save(&[0_u8; 32], 7), Err(AppendError::CorruptIndex(_))));
        assert!(matches!(idx.remove(&smallest), Err(AppendError::CorruptIndex(_))));
        idx.sync().expect("sync");
        assert_eq!(idx.page_crc_scan().dirty, 1, "the refused page was not stamped valid");
    }

    /// A cleanly-sealed index whose root page fails its CRC is rejected at open (the writable
    /// doors rebuild it, read-only refuses), rather than misreading every lookup.
    #[test]
    fn test_archive_btx_clean_open_rejects_corrupt_root_page() {
        let tmp = TempDir::with_prefix("test_archive_btx_root").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let root = {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for i in 0..2_000 {
                idx.save(&key_of(i), i).expect("save");
            }
            idx.sync().expect("sync");
            idx.header.root_page
        };
        write_at(&dir.join("index.btx"), BtreeIndex::page_offset(root) + 100, &[0xFF; 16]);
        for read_only in [true, false] {
            let Err(err) = BtreeIndex::open_btx_file(&dir, &data_header, 32, read_only) else {
                panic!("a corrupt root must not open (read_only={read_only})");
            };
            assert!(matches!(err, LoadHeaderError::CrcFailed), "{err:?}");
        }
    }

    /// A CRC-valid but absurd header (pages outside the tree, an impossible height) is rejected
    /// as `InvalidIndexGeometry` instead of being descended.
    #[test]
    fn test_archive_btx_open_rejects_absurd_header() {
        let tmp = TempDir::with_prefix("test_archive_btx_absurd").expect("temp dir");
        let dir = tmp.path().join("idx");
        let file = dir.join("index.btx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for i in 0..2_000 {
                idx.save(&key_of(i), i).expect("save");
            }
        }
        let good = {
            let bytes = std::fs::read(&file).expect("read");
            BtreeHeader::from_page(&bytes[..PAGE_SIZE]).expect("header")
        };
        type Mutation = (&'static str, fn(&mut BtreeHeader));
        let mutations: [Mutation; 4] = [
            ("root past the tree", |h| h.root_page = h.page_count + 5),
            ("root is the header page", |h| h.root_page = 0),
            ("zero height", |h| h.height = 0),
            ("height over the descent cap", |h| h.height = MAX_DEPTH as u32 + 1),
        ];
        for (what, mutate) in mutations {
            let mut bad = good.clone();
            mutate(&mut bad);
            write_at(&file, 0, &bad.to_page());
            let Err(err) = BtreeIndex::open_btx_file(&dir, &data_header, 32, true) else {
                panic!("{what}: an absurd header must not open");
            };
            assert!(matches!(err, LoadHeaderError::InvalidIndexGeometry), "{what}: {err:?}");
        }
        write_at(&file, 0, &good.to_page());
        BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("the original header opens");
    }

    /// `set_remove_on_drop` deletes the index file on drop without syncing it (abandoning a
    /// partial build).
    #[test]
    fn test_archive_btx_remove_on_drop_deletes_without_sync() {
        let tmp = TempDir::with_prefix("test_archive_btx_remove_on_drop").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
        for i in 0..500 {
            idx.save(&key_of(i), i).expect("save");
        }
        idx.set_remove_on_drop();
        drop(idx);
        assert!(!dir.join("index.btx").exists(), "the abandoned index file is removed");
    }

    /// A read-only open never creates the index directory (it fails instead).
    #[test]
    fn test_archive_btx_read_only_open_does_not_create_dir() {
        let tmp = TempDir::with_prefix("test_archive_btx_ro_dir").expect("temp dir");
        let dir = tmp.path().join("absent");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        assert!(BtreeIndex::open_btx_file(&dir, &data_header, 32, true).is_err());
        assert!(!dir.exists(), "a read-only open must not create the directory");
    }

    /// `data_file_length` is part of the header commit: it reaches disk only when the index is
    /// synced, never on a bare `set_data_file_length`, so a second (read-only) handle sees the
    /// last synced value.
    #[test]
    fn test_archive_btx_data_file_length_durable_only_at_sync() {
        let tmp = TempDir::with_prefix("test_archive_btx_dfl").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
        idx.save(&key_of(1), 1).expect("save");
        idx.sync().expect("sync");
        let synced = idx.data_file_length();

        idx.set_data_file_length(12_345);
        let reader: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("open ro");
        assert_eq!(reader.data_file_length(), synced, "an unsynced length is not visible");
        drop(reader);

        idx.sync().expect("sync");
        let reader: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("open ro");
        assert_eq!(reader.data_file_length(), 12_345, "the synced length is visible");
    }

    /// The owner's value round-trips through the header like `data_file_length`: 0 in a fresh
    /// index, durable only at a sync, and kept across a reopen without disturbing the tree.
    #[test]
    fn test_archive_btx_owner_value_durable_at_sync() {
        let tmp = TempDir::with_prefix("test_archive_btx_owner").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
        assert_eq!(idx.owner_value(), 0, "unset in a fresh index");
        idx.save(&key_of(1), 1).expect("save");
        idx.sync().expect("sync");

        idx.set_owner_value(u64::MAX - 7);
        let reader: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("open ro");
        assert_eq!(reader.owner_value(), 0, "an unsynced value is not visible");
        drop(reader);

        idx.sync().expect("sync");
        drop(idx);
        let idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen");
        assert_eq!(idx.owner_value(), u64::MAX - 7, "the synced value is kept");
        assert_eq!(idx.load(&key_of(1)).expect("load"), 1, "the tree is unchanged");
    }

    /// Every entry of the tree `src` reads, in scan order, through a fresh cursor.
    fn scan_src<S: PageSource>(
        src: &S,
        reverse: bool,
        lower: std::ops::Bound<Vec<u8>>,
        upper: std::ops::Bound<Vec<u8>>,
    ) -> Vec<(Vec<u8>, u64)> {
        let mut cursor =
            crate::archive::btree_index::iter::BtreeCursor::new(src, reverse, lower, upper)
                .expect("cursor");
        let mut out = Vec::new();
        while let Some(item) = cursor.next(src) {
            let (k, v) = item.expect("scan step");
            out.push((k.to_vec(), v));
        }
        out
    }

    /// Copy-on-write against a `BTreeMap` model: random inserts, overwrites and removes with
    /// publishes (and periodic syncs) in between; the working tree and every snapshot agree with
    /// the model on lookups and on forward, reverse and range scans.
    #[test]
    fn test_archive_btx_cow_matches_model() {
        cow_matches_model(2, 3_000);
    }

    /// The model test with mostly removals over few keys, so leaves keep emptying (and are
    /// unlinked, the root collapsing and regrowing) between snapshots.
    #[test]
    fn test_archive_btx_cow_matches_model_heavy_removals() {
        cow_matches_model(5, 600);
    }

    /// Random ops over `key_space` keys, `removes_in_8` of every 8 a remove (see
    /// [`test_archive_btx_cow_matches_model`]).
    fn cow_matches_model(removes_in_8: u64, key_space: u64) {
        use std::{collections::BTreeMap, ops::Bound};

        let tmp = TempDir::with_prefix("test_archive_btx_model").expect("temp dir");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(tmp.path().join("idx"), &data_header, 32, false)
                .expect("open");
        let mut model: BTreeMap<[u8; 32], u64> = BTreeMap::new();
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut rand = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let all = |m: &BTreeMap<[u8; 32], u64>| -> Vec<(Vec<u8>, u64)> {
            m.iter().map(|(k, v)| (k.to_vec(), *v)).collect()
        };
        for round in 0..30 {
            for _ in 0..400 {
                let k = key_of(rand() % key_space);
                if rand() % 8 < removes_in_8 {
                    let had = model.remove(&k).is_some();
                    assert_eq!(idx.remove(&k).expect("remove"), had);
                } else {
                    let v = rand();
                    model.insert(k, v);
                    idx.save(&k, v).expect("save");
                }
            }
            let snap = idx.publish();
            if round % 7 == 0 {
                idx.sync().expect("sync");
            }
            let expect = all(&model);
            assert_eq!(scan_src(&idx, false, Bound::Unbounded, Bound::Unbounded), expect);
            assert_eq!(scan_src(&snap, false, Bound::Unbounded, Bound::Unbounded), expect);
            let mut rev = expect.clone();
            rev.reverse();
            assert_eq!(scan_src(&snap, true, Bound::Unbounded, Bound::Unbounded), rev);
            assert_eq!(snap.len(), model.len());
            let (a, b) = (key_of(rand() % key_space), key_of(rand() % key_space));
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            let fwd: Vec<_> = model.range(lo..hi).map(|(k, v)| (k.to_vec(), *v)).collect();
            assert_eq!(
                scan_src(&snap, false, Bound::Included(lo.to_vec()), Bound::Excluded(hi.to_vec())),
                fwd
            );
            let back: Vec<_> = model.range(..=hi).rev().map(|(k, v)| (k.to_vec(), *v)).collect();
            assert_eq!(scan_src(&snap, true, Bound::Unbounded, Bound::Included(hi.to_vec())), back);
            for (k, v) in model.iter().take(64) {
                assert_eq!(snap.load(k).expect("load"), *v);
            }
        }
    }

    /// A fresh writable index in its own temp dir (the dir must outlive the index).
    fn reclaim_index(prefix: &str) -> (TempDir, BtreeIndex) {
        let tmp = TempDir::with_prefix(prefix).expect("temp dir");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let idx = BtreeIndex::open_btx_file(tmp.path().join("idx"), &data_header, 32, false)
            .expect("open");
        (tmp, idx)
    }

    /// Pages reachable from the working tree.
    fn live_pages(idx: &BtreeIndex) -> usize {
        idx.tree_pages(idx.header.root_page, idx.header.height, false)
            .expect("walk")
            .drain()
            .count()
    }

    /// Random writes, publishes, syncs and clears while a changing set of snapshots is held and
    /// dropped out of order: every held snapshot keeps matching the model it was published with,
    /// so no page a live snapshot can reach is ever reused.
    #[test]
    fn test_archive_btx_reclaim_never_reuses_visible_pages() {
        use std::{collections::BTreeMap, ops::Bound};

        let (_tmp, mut idx) = reclaim_index("test_archive_btx_reclaim_model");
        let mut model: BTreeMap<[u8; 32], u64> = BTreeMap::new();
        // Each held snapshot with the entries it was published with.
        type Rows = Vec<(Vec<u8>, u64)>;
        let mut held: Vec<(IndexSnapshot, Rows)> = Vec::new();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rand = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..80 {
            if round % 25 == 24 {
                idx.clear().expect("clear");
                model.clear();
            }
            for _ in 0..rand() % 200 {
                let k = key_of(rand() % 6_000);
                if rand() % 3 == 0 {
                    assert_eq!(idx.remove(&k).expect("remove"), model.remove(&k).is_some());
                } else {
                    let v = rand();
                    model.insert(k, v);
                    idx.save(&k, v).expect("save");
                }
            }
            if rand() % 5 == 0 {
                idx.sync().expect("sync"); // a publish with no snapshot
            }
            let snap = idx.publish();
            let expect: Vec<_> = model.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
            if rand() % 2 == 0 {
                if held.len() == 6 {
                    held.swap_remove((rand() % 6) as usize);
                }
                held.push((snap, expect.clone()));
            }
            for _ in 0..rand() % 3 {
                if !held.is_empty() {
                    held.swap_remove((rand() % held.len() as u64) as usize);
                }
            }
            assert_eq!(scan_src(&idx, false, Bound::Unbounded, Bound::Unbounded), expect);
            for (snap, want) in &held {
                assert_eq!(&scan_src(snap, false, Bound::Unbounded, Bound::Unbounded), want);
                let mut rev = want.clone();
                rev.reverse();
                assert_eq!(scan_src(snap, true, Bound::Unbounded, Bound::Unbounded), rev);
                assert_eq!(snap.len(), want.len());
            }
        }
    }

    /// With no snapshot held, commit-per-write keeps the file within a few pages of the live tree;
    /// a held snapshot makes it grow until dropped, after which its pages are reused.
    #[test]
    fn test_archive_btx_reclaim_bounds_growth() {
        let (_tmp, mut idx) = reclaim_index("test_archive_btx_reclaim_growth");
        for i in 0..20_000u64 {
            idx.save(&key_of(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)), i).expect("save");
            drop(idx.publish());
        }
        let live = live_pages(&idx);
        let pages = idx.header.page_count as usize - 1;
        assert!(pages <= live + 16, "{pages} pages for {live} live");

        let held = idx.publish();
        for i in 0..200u64 {
            idx.save(&key_of(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)), i + 1).expect("overwrite");
            drop(idx.publish());
        }
        let grown = idx.header.page_count as usize - 1;
        assert!(grown >= pages + 200, "a held snapshot stops reuse ({pages} -> {grown})");
        drop(held);
        for i in 0..2_000u64 {
            idx.save(&key_of(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)), i + 2).expect("overwrite");
            drop(idx.publish());
        }
        assert!(
            idx.header.page_count as usize - 1 <= grown + 2,
            "the released pages are reused ({grown} -> {})",
            idx.header.page_count - 1
        );
    }

    /// Snapshots released out of order. S1's pages partly survive into S2 and S3, and some are
    /// replaced only after S2 (which is dropped first): they must stay unreused while S1 lives, as
    /// must S3's while S3 lives, through heavy churn — byte-identical pages.
    #[test]
    fn test_archive_btx_reclaim_out_of_order_release() {
        let (_tmp, mut idx) = reclaim_index("test_archive_btx_reclaim_order");
        // Ordered keys, so a key range is a leaf range (`key_of` hashes).
        let key_of = |i: u64| -> [u8; 32] {
            let mut k = [0_u8; 32];
            k[..8].copy_from_slice(&i.to_be_bytes());
            k
        };
        let pages_of = |idx: &BtreeIndex, snap: &IndexSnapshot| -> Vec<(u32, Vec<u8>)> {
            let mut walk =
                idx.tree_pages(idx.header.root_page, idx.header.height, false).expect("walk");
            walk.drain().map(|p| (p as u32, snap.page(p as u32).expect("page").to_vec())).collect()
        };
        let unchanged = |snap: &IndexSnapshot, pages: &[(u32, Vec<u8>)]| {
            for (p, bytes) in pages {
                assert_eq!(snap.page(*p).expect("page"), &bytes[..], "page {p} of a live snapshot");
            }
        };
        let churn = |idx: &mut BtreeIndex, keys: std::ops::Range<u64>, v: u64| {
            for i in keys {
                idx.save(&key_of(i), i + v).expect("save");
            }
            drop(idx.publish());
        };
        for i in 0..3_000u64 {
            idx.save(&key_of(i), i).expect("save");
        }
        let s1 = idx.publish();
        let s1_pages = pages_of(&idx, &s1);
        churn(&mut idx, 0..1_000, 1); // S1's leaves for 1_000.. survive into the next state
        let s2 = idx.publish();
        for i in 1_000..2_000u64 {
            idx.save(&key_of(i), i + 2).expect("save"); // replaces S1 pages only after S2
        }
        let s3 = idx.publish();
        let s3_pages = pages_of(&idx, &s3);
        drop(s2);
        for round in 3..30u64 {
            churn(&mut idx, 0..3_000, round * 10);
        }
        unchanged(&s1, &s1_pages);
        unchanged(&s3, &s3_pages);
        drop(s1);
        for round in 30..60u64 {
            churn(&mut idx, 0..3_000, round * 10);
        }
        unchanged(&s3, &s3_pages);
        assert_eq!(s3.load(&key_of(1_500)).expect("load"), 1_502);
    }

    /// `clear` retires the old tree: a snapshot from before it stays intact, and once that is
    /// dropped the old pages are reused rather than the file growing.
    #[test]
    fn test_archive_btx_clear_retires_old_tree() {
        let (_tmp, mut idx) = reclaim_index("test_archive_btx_clear_reclaim");
        for i in 0..3_000u64 {
            idx.save(&key_of(i), i).expect("save");
        }
        let before = idx.publish();
        idx.clear().expect("clear");
        for i in 0..50u64 {
            idx.save(&key_of(100_000 + i), i).expect("save");
            drop(idx.publish());
        }
        assert_eq!(before.len(), 3_000);
        assert_eq!(before.load(&key_of(2_999)).expect("old snapshot intact"), 2_999);
        drop(before);
        let high = idx.header.page_count;
        for i in 0..3_000u64 {
            idx.save(&key_of(i), i).expect("refill");
            if i % 100 == 99 {
                drop(idx.publish());
            }
        }
        assert!(
            idx.header.page_count <= high + 4,
            "old tree reused ({high} -> {})",
            idx.header.page_count
        );
    }

    /// A clean reopen recovers the unreachable pages as free (reused before the file grows); a
    /// tree whose walk fails (a duplicated child pointer) gets no free pages at all.
    #[test]
    fn test_archive_btx_reopen_recovers_free_pages() {
        let tmp = TempDir::with_prefix("test_archive_btx_reopen_free").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let garbage = {
            let mut idx = BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for round in 0..5u64 {
                for i in 0..3_000u64 {
                    idx.save(&key_of(i), i + round).expect("save");
                }
                let _held = idx.publish(); // dropped at the end of the round
                for i in 0..3_000u64 {
                    idx.save(&key_of(i), i + round + 1).expect("save");
                }
            }
            idx.header.page_count as usize - 1 - live_pages(&idx)
        }; // clean close
        assert!(garbage > 10, "the workload leaves garbage ({garbage})");
        {
            let mut idx = BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen");
            assert_eq!(idx.free.len(), garbage, "every unreachable page is free");
            let pages = idx.header.page_count;
            for i in 0..3_000u64 {
                idx.save(&key_of(i), i).expect("overwrite");
                if i % 300 == 299 {
                    drop(idx.publish());
                }
            }
            assert_eq!(idx.header.page_count, pages, "free pages are reused before the file grows");
            assert_eq!(idx.load(&key_of(2_999)).expect("load"), 2_999);
        }

        // Duplicate the root's first child pointer (CRC re-stamped so the open accepts the root).
        let file = dir.join("index.btx");
        let root =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("ro").header.root_page;
        let mut bytes = std::fs::read(&file).expect("read");
        let node = Node::new(32);
        {
            let page = &mut bytes[root as usize * PAGE_SIZE..(root as usize + 1) * PAGE_SIZE];
            assert!(!node.is_leaf(page));
            let first = node.internal_child(page, 0);
            node.set_internal_child(page, 1, first);
            add_crc32_nonzero(page);
        }
        std::fs::write(&file, &bytes).expect("write");
        let idx = BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen");
        assert!(idx.free.is_empty(), "a failed walk frees nothing");
    }

    /// A rebuild truncates every page, so it is refused while a snapshot is alive.
    #[test]
    fn test_archive_btx_reset_refused_with_live_snapshot() {
        let (_tmp, mut idx) = reclaim_index("test_archive_btx_reset_pinned");
        idx.save(&key_of(1), 1).expect("save");
        let snap = idx.publish();
        assert!(idx.rebuild_from(std::iter::empty::<([u8; 32], u64)>()).is_err());
        assert_eq!(snap.load(&key_of(1)).expect("snapshot intact"), 1);
        drop(snap);
        idx.rebuild_from(std::iter::empty::<([u8; 32], u64)>()).expect("rebuild once released");
        assert!(idx.is_empty());
    }

    /// A published snapshot never changes: later overwrites, removes, inserts, a clear and new
    /// publishes copy pages instead of modifying them, so the old snapshot's pages are
    /// byte-identical and it still reads its original contents.
    #[test]
    fn test_archive_btx_published_snapshot_is_immutable() {
        use std::ops::Bound;

        let tmp = TempDir::with_prefix("test_archive_btx_snapshot").expect("temp dir");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(tmp.path().join("idx"), &data_header, 32, false)
                .expect("open");
        for i in 0..3_000 {
            idx.save(&key_of(i), i).expect("save");
        }
        let snap = idx.publish();
        let pages: Vec<Vec<u8>> =
            (1..snap.page_count).map(|p| snap.page(p).expect("page").to_vec()).collect();
        let expect = scan_src(&snap, false, Bound::Unbounded, Bound::Unbounded);

        for i in 0..3_000 {
            idx.save(&key_of(i), i + 1_000_000).expect("overwrite");
        }
        for i in 0..1_000 {
            assert!(idx.remove(&key_of(i)).expect("remove"));
        }
        for i in 3_000..6_000 {
            idx.save(&key_of(i), i).expect("insert");
        }
        let snap2 = idx.publish();
        idx.clear().expect("clear");
        let snap3 = idx.publish();

        for (i, p) in (1..snap.page_count).enumerate() {
            assert_eq!(snap.page(p).expect("page"), &pages[i][..], "published page {p} changed");
        }
        assert_eq!(scan_src(&snap, false, Bound::Unbounded, Bound::Unbounded), expect);
        assert_eq!(snap.load(&key_of(0)).expect("old snapshot"), 0);
        assert!(matches!(snap2.load(&key_of(0)), Err(FetchError::NotFound)));
        assert_eq!(snap2.load(&key_of(2_000)).expect("new snapshot"), 2_000 + 1_000_000);
        assert_eq!(snap2.len(), 5_000);
        assert_eq!(snap3.len(), 0);
        assert!(matches!(snap3.load(&key_of(4_000)), Err(FetchError::NotFound)));
    }

    #[test]
    fn test_archive_btx_rebuild_from() {
        let tmp = TempDir::with_prefix("test_archive_btx_rebuild").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
        for i in 0..1_000 {
            idx.save(&key_of(i), i).expect("save");
        }
        idx.sync().expect("sync");

        // Rebuild from a different set, as if re-derived by re-scanning the pack.
        let entries: Vec<([u8; 32], u64)> = (2_000..2_500).map(|i| (key_of(i), i)).collect();
        idx.rebuild_from(entries.iter().copied()).expect("rebuild");
        idx.sync().expect("sync");
        assert_eq!(idx.len(), 500);
        assert!(matches!(idx.load(&key_of(0)), Err(FetchError::NotFound)), "old keys gone");
        for i in 2_000..2_500 {
            assert_eq!(idx.load(&key_of(i)).expect("load"), i);
        }
        // Rebuild is rejected on a read-only index.
        drop(idx);
        let mut ro: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
        assert_eq!(ro.len(), 500);
        assert!(matches!(ro.rebuild_from(entries.iter().copied()), Err(AppendError::ReadOnly)));
    }

    #[test]
    fn test_archive_btx_remove() {
        let tmp = TempDir::with_prefix("test_archive_btx_remove").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
        for i in 0..100u64 {
            idx.save(&key_of(i), i).expect("save");
        }
        assert_eq!(idx.len(), 100);

        // Remove an existing key.
        assert!(idx.remove(&key_of(42)).expect("remove"), "expected true for present key");
        assert_eq!(idx.len(), 99);
        assert!(matches!(idx.load(&key_of(42)), Err(FetchError::NotFound)), "42 should be gone");

        // Remove again: not found.
        assert!(!idx.remove(&key_of(42)).expect("remove again"), "expected false for absent key");
        assert_eq!(idx.len(), 99);

        // Remove a key that was never inserted.
        assert!(
            !idx.remove(&key_of(999)).expect("remove missing"),
            "expected false for never-inserted key"
        );

        // Remaining keys are intact.
        for i in 0..100u64 {
            if i == 42 {
                continue;
            }
            assert_eq!(idx.load(&key_of(i)).expect("load"), i);
        }

        // Remove on read-only index is rejected.
        idx.sync().expect("sync");
        drop(idx);
        let mut ro: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
        assert!(matches!(ro.remove(&key_of(0)), Err(AppendError::ReadOnly)));

        // remove_digest adapter.
        drop(ro);
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
        assert!(idx.remove_digest(B256::from(key_of(7))).expect("remove_digest"));
        assert_eq!(idx.len(), 98);
    }

    #[test]
    fn test_archive_btx_crc_regime_and_rebuild() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        let tmp = TempDir::with_prefix("test_archive_btx_crc").expect("temp dir");
        let dir = tmp.path().join("idx");
        let file = dir.join("index.btx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let all: Vec<([u8; 32], u64)> = (0..3_000u64).map(|i| (key_of(i), i)).collect();

        // Insert without syncing: modified pages carry the zero-CRC dirty marker.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("open");
            for (k, v) in &all {
                idx.save(k, *v).expect("save");
            }
            let before = idx.page_crc_scan();
            assert!(before.dirty > 0, "unsynced writes should leave dirty pages, got {before:?}");
            assert_eq!(before.corrupt, 0, "no corruption yet");
            // sync() CRCs every dirty page.
            idx.sync().expect("sync");
            assert_eq!(
                idx.page_crc_scan(),
                PageCrcReport::default(),
                "sync clears all dirty pages"
            );
        }

        // Corrupt a leaf page's payload on disk (leaving its now-stale, non-zero CRC).
        {
            let mut f =
                std::fs::OpenOptions::new().read(true).write(true).open(&file).expect("open rw");
            f.seek(SeekFrom::Start(PAGE_SIZE as u64 + 100)).expect("seek");
            f.write_all(&[0xFF; 16]).expect("corrupt");
            f.sync_all().expect("sync");
        }

        // Reads no longer verify a per-op CRC, but the off-path scan detects the corruption.
        {
            let idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, true).expect("reopen ro");
            let rep = idx.page_crc_scan();
            assert!(rep.corrupt > 0, "corrupted page must be flagged by the scan, got {rep:?}");
        }

        // Rebuilding from the (pack-derived) entries recovers a fully readable, clean index.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, 32, false).expect("reopen rw");
            idx.rebuild_from(all.iter().copied()).expect("rebuild");
            idx.sync().expect("sync");
            for (k, v) in &all {
                assert_eq!(idx.load(k).expect("load"), *v);
            }
            assert_eq!(idx.page_crc_scan(), PageCrcReport::default(), "rebuilt index is clean");
        }
    }

    #[test]
    fn test_archive_btx_custom_ksize() {
        let tmp = TempDir::with_prefix("test_archive_btx_ksize16").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        // 16-byte big-endian keys; 2_000 entries force splits (170 keys/leaf at ksize=16).
        let key = |i: u64| -> [u8; 16] {
            let mut k = [0_u8; 16];
            k[8..16].copy_from_slice(&i.to_be_bytes());
            k
        };

        {
            let mut idx = BtreeIndex::open_btx_file(&dir, &data_header, 16, false).expect("open");
            assert_eq!(idx.ksize(), 16);
            for i in 0..2_000u64 {
                idx.save(&key(i), i).expect("save");
            }
            assert_eq!(idx.len(), 2_000);
            assert!(idx.height() >= 2, "expected splits, got height {}", idx.height());
            idx.sync().expect("sync");
        }

        // Reopen read-only with the same key size: values persist and iterate in sorted order.
        let idx = BtreeIndex::open_btx_file(&dir, &data_header, 16, true).expect("reopen ro");
        assert_eq!(idx.ksize(), 16);
        for i in 0..2_000u64 {
            assert_eq!(idx.load(&key(i)).expect("load"), i);
        }
        let keys: Vec<u64> = idx
            .iter()
            .expect("iter")
            .map(|r| u64::from_be_bytes(r.expect("item").0[8..16].try_into().unwrap()))
            .collect();
        assert_eq!(keys, (0..2_000u64).collect::<Vec<_>>());

        // Opening the same file as 32-byte keys is a geometry mismatch.
        assert!(matches!(
            BtreeIndex::open_btx_file(&dir, &data_header, 32, true),
            Err(LoadHeaderError::InvalidIndexGeometry)
        ));
    }

    // ---- split failures, emptied leaves, append splits, key sizes ----

    /// A `ksize`-byte key whose leading big-endian `u64` orders it numerically.
    fn wide_key(i: u64, ksize: usize) -> Vec<u8> {
        let mut k = vec![0_u8; ksize];
        k[..8].copy_from_slice(&i.to_be_bytes());
        k
    }

    fn open_ksize(tmp: &TempDir, ksize: u16) -> BtreeIndex {
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        BtreeIndex::open_btx_file(tmp.path().join("idx"), &data_header, ksize, false).expect("open")
    }

    /// Every `(key, position)` of the working tree, in order.
    fn entries(idx: &BtreeIndex) -> Vec<(Vec<u8>, u64)> {
        idx.iter().expect("iter").map(|item| item.expect("scan step")).collect()
    }

    /// Average leaf fill: stored keys over leaf capacity.
    fn leaf_fill(idx: &BtreeIndex) -> f64 {
        let (mut leaves, mut keys) = (0_usize, 0_usize);
        for p in
            idx.tree_pages(idx.header.root_page, idx.header.height, false).expect("walk").drain()
        {
            let buf = idx.page(p as u32).expect("page");
            if idx.node.is_leaf(buf) {
                leaves += 1;
                keys += idx.node.entry_count(buf);
            }
        }
        keys as f64 / (leaves * idx.node.max_leaf_keys()) as f64
    }

    /// An insert whose split cascade fails to allocate a page part-way changes nothing: the tree
    /// keeps exactly its previous entries (no orphaned half), at every failure point.
    #[test]
    fn test_archive_btx_split_failure_leaves_tree_intact() {
        use std::collections::BTreeMap;

        // The widest key: two keys per page, so cascades several levels deep come quickly.
        const KSIZE: usize = 2032;
        let tmp = TempDir::with_prefix("btx_split_failure").expect("temp dir");
        let mut idx = open_ksize(&tmp, KSIZE as u16);
        let mut model: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for n in 0..300_u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let key = wide_key(if n % 3 == 0 { n } else { x % 100_000 }, KSIZE);
            // Entries as (key number, position), for readable failures.
            let numbered = |entries: Vec<(Vec<u8>, u64)>| -> Vec<(u64, u64)> {
                entries
                    .into_iter()
                    .map(|(k, v)| (u64::from_be_bytes(k[..8].try_into().expect("8 bytes")), v))
                    .collect()
            };
            for fail_at in 0.. {
                idx.fail_allocs_after = Some(fail_at);
                let result = idx.save(&key, n);
                idx.fail_allocs_after = None;
                if result.is_ok() {
                    break;
                }
                let expect: Vec<_> = model.iter().map(|(k, v)| (k.clone(), *v)).collect();
                assert_eq!(
                    numbered(entries(&idx)),
                    numbered(expect),
                    "failure at allocation {fail_at} of insert {n}"
                );
                assert_eq!(idx.len(), model.len());
            }
            model.insert(key, n);
            if n % 50 == 49 {
                drop(idx.publish());
            }
        }
        assert!(idx.height() >= 5, "the test reaches deep cascades");
        let expect: Vec<_> = model.into_iter().collect();
        assert_eq!(entries(&idx), expect);
    }

    /// Ascending keys with the oldest removed (a sliding window): emptied leaves are unlinked and
    /// their pages reused, so the tree stays the size of the window, not of every key ever written.
    #[test]
    fn test_archive_btx_fifo_removal_keeps_tree_bounded() {
        const WINDOW: u64 = 1_000;
        let tmp = TempDir::with_prefix("btx_fifo").expect("temp dir");
        let mut idx = open_ksize(&tmp, 8);
        for i in 0..200_000_u64 {
            idx.save(&i.to_be_bytes(), i).expect("save");
            if i >= WINDOW {
                assert!(idx.remove(&(i - WINDOW).to_be_bytes()).expect("remove"));
            }
            if i % 100 == 0 {
                drop(idx.publish());
            }
        }
        assert!(idx.page_count() < 100, "{} pages for a {WINDOW}-key window", idx.page_count());
        let expect: Vec<_> =
            (200_000 - WINDOW..200_000).map(|i| (i.to_be_bytes().to_vec(), i)).collect();
        assert_eq!(entries(&idx), expect);

        // A clean reopen walks the shrunken tree and serves the same entries.
        idx.sync().expect("sync");
        drop(idx);
        let idx = open_ksize(&tmp, 8);
        assert_eq!(entries(&idx), expect);
    }

    /// Removing every key empties leaves, collapses the root and leaves a working empty tree; the
    /// tree then refills correctly, with snapshots of each state intact throughout.
    #[test]
    fn test_archive_btx_remove_all_collapses_and_refills() {
        let tmp = TempDir::with_prefix("btx_remove_all").expect("temp dir");
        let mut idx = open_ksize(&tmp, 8);
        for round in 0..3_u64 {
            for i in 0..5_000_u64 {
                idx.save(&(i * 7 % 5_000).to_be_bytes(), i + round).expect("save");
            }
            let full = idx.publish();
            assert!(idx.height() >= 2);
            for i in 0..5_000_u64 {
                assert!(idx.remove(&(i * 3 % 5_000).to_be_bytes()).expect("remove"));
            }
            assert!(idx.is_empty());
            assert_eq!(idx.height(), 1, "an emptied tree collapses to one leaf");
            assert!(entries(&idx).is_empty());
            // The snapshot published before the removals still reads every key.
            assert_eq!(full.len(), 5_000);
            assert!(full.load(&4_999_u64.to_be_bytes()).is_ok());
        }
    }

    /// A key of the wrong size is an error from every point op (in release builds too), not a
    /// panic or a silent miss.
    #[test]
    fn test_archive_btx_wrong_key_size_is_an_error() {
        let tmp = TempDir::with_prefix("btx_key_size").expect("temp dir");
        let mut idx = open_ksize(&tmp, 32);
        let short = [7_u8; 31];
        assert!(matches!(idx.save(&short, 1), Err(AppendError::KeySize { expected: 32, got: 31 })));
        assert!(matches!(idx.load(&short), Err(FetchError::KeySize { expected: 32, got: 31 })));
        assert!(matches!(
            idx.remove(&[7_u8; 33]),
            Err(AppendError::KeySize { expected: 32, got: 33 })
        ));
        assert!(!idx.contains(&short));
        assert!(idx.is_empty(), "nothing was written");
    }

    /// Ascending inserts (and a sorted rebuild) fill pages nearly full; random inserts keep the
    /// usual split.
    #[test]
    fn test_archive_btx_append_split_fills_pages() {
        let tmp = TempDir::with_prefix("btx_append").expect("temp dir");
        let mut ascending = open_ksize(&tmp, 8);
        for i in 0..100_000_u64 {
            ascending.save(&i.to_be_bytes(), i).expect("save");
        }
        assert!(leaf_fill(&ascending) > 0.95, "ascending fill {}", leaf_fill(&ascending));

        let entries: Vec<_> = (0..100_000_u64).map(|i| (i.to_be_bytes(), i)).collect();
        ascending.rebuild_from(entries).expect("rebuild");
        assert!(leaf_fill(&ascending) > 0.95, "sorted rebuild fill {}", leaf_fill(&ascending));

        let tmp = TempDir::with_prefix("btx_random").expect("temp dir");
        let mut random = open_ksize(&tmp, 8);
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        for _ in 0..100_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            random.save(&x.to_be_bytes(), x).expect("save");
        }
        let fill = leaf_fill(&random);
        assert!((0.55..0.9).contains(&fill), "random fill {fill}");
    }
}
