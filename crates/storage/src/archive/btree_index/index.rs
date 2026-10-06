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

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use tn_types::B256;

use crate::archive::{
    btree_index::{
        header::{BtreeHeader, VALUE_SIZE},
        page::{Node, NULL_PAGE, PAGE_SIZE},
    },
    crc::{add_crc32_nonzero, crc_is_zero, crc_state, zero_crc, CrcState},
    data_file::{fsync_directory, MmapAccess, MmapDataFile, MmapFileOptions, WriteMode},
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    index::Index,
    pack::DataHeader,
};

/// Hard cap on tree height while descending, a corruption tripwire (real heights are tiny: at a
/// branching factor of dozens, even 2^48 keys stay well under this).
const MAX_DEPTH: usize = 48;

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

/// A set of page numbers as a dense bitset. Pages are numbered `1..page_count`, so a bit per page
/// is compact, and the hot-path test/set is a single bit operation with no hashing.
#[derive(Debug, Default)]
struct PageSet {
    words: Vec<u64>,
}

impl PageSet {
    fn insert(&mut self, p: u32) {
        let word = p as usize / 64;
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        self.words[word] |= 1 << (p % 64);
    }

    fn contains(&self, p: u32) -> bool {
        self.words.get(p as usize / 64).is_some_and(|word| word & (1 << (p % 64)) != 0)
    }

    /// Empty the set, keeping its allocation.
    fn clear(&mut self) {
        self.words.fill(0);
    }

    /// Yield every page in ascending order, clearing each word as it is reached (consume it fully
    /// to empty the set).
    fn drain(&mut self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter_mut().enumerate().flat_map(|(i, word)| {
            let mut bits = std::mem::take(word);
            std::iter::from_fn(move || {
                (bits != 0).then(|| {
                    let bit = bits.trailing_zeros();
                    bits &= bits - 1;
                    (i * 64) as u32 + bit
                })
            })
        })
    }
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
    /// Pages this handle has written (or allocated) since the last sync. Their all-zero CRC
    /// trailer is the lazy-write marker; a zero trailer on any OTHER page is at-rest damage (a
    /// clean close CRC-stamps every written page, and an unclean index is rebuilt rather than
    /// trusted). Empty on open, so a read-only handle trusts no zero-trailer page; cleared by each
    /// sync, which stamps exactly this set.
    unsynced_pages: PageSet,
    _index_dir: PathBuf,
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
            ..Default::default()
        };
        let mut file = MmapDataFile::open_with(dir.join("index.btx"), read_only, opts)?;

        let header = if file.is_empty() {
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
                || !in_tree(header.first_leaf)
                || !in_tree(header.last_leaf)
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

        Ok(Self {
            header,
            file,
            node,
            read_only,
            synced: true,
            remove_on_drop: false,
            unsynced_pages: PageSet::default(),
            _index_dir: dir.to_owned(),
        })
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
        self.header.root_page = 1;
        self.header.height = 1;
        self.header.page_count = 2;
        self.header.values = 0;
        self.header.first_leaf = 1;
        self.header.last_leaf = 1;
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
        self.unsynced_pages.clear();
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
        if crc_is_zero(buf) && !self.unsynced_pages.contains(p) {
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
        self.unsynced_pages.insert(p);
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
        let p = self.header.page_count;
        self.file.ensure_len((p as u64 + 1) * PAGE_SIZE as u64)?;
        self.header.page_count += 1;
        self.unsynced_pages.insert(p);
        Ok(p)
    }

    // ---- helpers for the leaf-chain iterators (see `super::iter`) ----

    /// The leftmost leaf page (start of an ascending scan).
    pub(super) fn first_leaf(&self) -> u32 {
        self.header.first_leaf
    }

    /// The rightmost leaf page (start of a descending scan).
    pub(super) fn last_leaf(&self) -> u32 {
        self.header.last_leaf
    }

    /// The page geometry (a small `Copy` value) for the iterators to decode leaves with.
    pub(super) fn node(&self) -> Node {
        self.node
    }

    /// Borrow leaf page `p` for a scan step (via [`Self::page`], so its corruption checks apply).
    /// The slice is valid while the index is not modified, which the scan's holder guarantees.
    pub(super) fn leaf_page(&self, p: u32) -> Result<&[u8], FetchError> {
        self.page(p)
    }

    /// Descend to the leaf page that would contain `key`.
    pub(super) fn find_leaf(&self, key: &[u8]) -> Result<u32, FetchError> {
        let node = self.node;
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno)?;
            if node.is_leaf(buf) {
                return Ok(pno);
            }
            let ci = node.internal_child_index(buf, key);
            pno = node.internal_child(buf, ci);
        }
        Err(FetchError::CorruptIndex("btree descent exceeded max depth".to_string()))
    }

    // ---- lookup ----

    fn get_value(&self, key: &[u8]) -> Result<u64, FetchError> {
        let node = self.node;
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno)?;
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

    // ---- insertion (all in place on the mapping) ----

    fn insert_kv(&mut self, key: &[u8], val: u64) -> Result<(), AppendError> {
        let node = self.node;
        // Descend to the target leaf, recording the (page, child_index) path for split propagation.
        let mut path: Vec<(u32, usize)> = Vec::new();
        let mut pno = self.header.root_page;
        let mut leaf_no = None;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno).map_err(fetch_to_append)?;
            if node.is_leaf(buf) {
                leaf_no = Some(pno);
                break;
            }
            let ci = node.internal_child_index(buf, key);
            let child = node.internal_child(buf, ci);
            path.push((pno, ci));
            pno = child;
        }
        let leaf_no = leaf_no.ok_or_else(|| {
            AppendError::CorruptIndex("btree descent exceeded max depth".to_string())
        })?;
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
        let (found, at, full) = {
            let buf = self.page(leaf_no).map_err(fetch_to_append)?;
            match node.leaf_search(buf, key) {
                Ok(i) => (Some(i), 0usize, false),
                Err(at) => (None, at, node.entry_count(buf) >= node.max_leaf_keys()),
            }
        };
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
        self.split_leaf(leaf_no, path, at, key, val)?;
        self.header.values += 1;
        Ok(())
    }

    fn split_leaf(
        &mut self,
        leaf_no: u32,
        path: Vec<(u32, usize)>,
        at: usize,
        key: &[u8],
        val: u64,
    ) -> Result<(), AppendError> {
        let node = self.node;
        // Read the old successor before mutating, then allocate the right sibling (may remap).
        let old_next = {
            let l = self.page(leaf_no).map_err(fetch_to_append)?;
            node.leaf_next(l)
        };
        let right_no = self.allocate_page()?;

        // Split the left leaf in place; build the right leaf in a scratch buffer.
        let mut rbuf = vec![0_u8; PAGE_SIZE];
        let sep = {
            let left = self.page_mut(leaf_no).map_err(fetch_to_append)?;
            let sep = node.leaf_split(left, &mut rbuf, at, key, val);
            node.set_leaf_next(left, right_no);
            zero_crc(left);
            sep
        };
        node.set_leaf_prev(&mut rbuf, leaf_no);
        node.set_leaf_next(&mut rbuf, old_next);
        zero_crc(&mut rbuf);
        {
            let r = self.page_mut(right_no).map_err(fetch_to_append)?;
            r.copy_from_slice(&rbuf);
        }
        // Relink the old successor's back-pointer, or record the new rightmost leaf.
        if old_next != NULL_PAGE {
            let nb = self.page_mut(old_next).map_err(fetch_to_append)?;
            node.set_leaf_prev(nb, right_no);
            zero_crc(nb);
        } else {
            self.header.last_leaf = right_no;
        }
        self.synced = false;
        self.insert_into_parent(path, sep, right_no)
    }

    /// Insert `(sep, right_no)` into the parent, splitting internal nodes and growing a new root
    /// as needed.
    fn insert_into_parent(
        &mut self,
        mut path: Vec<(u32, usize)>,
        sep: Vec<u8>,
        right_no: u32,
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
            let new_right_no = self.allocate_page()?;
            let mut qbuf = vec![0_u8; PAGE_SIZE];
            let median = {
                let p = self.page_mut(pno).map_err(fetch_to_append)?;
                let median = node.internal_split(p, &mut qbuf, ci, &sep, right_no);
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
        let new_root_no = self.allocate_page()?;
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
        let node = self.node;
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno).map_err(fetch_to_append)?;
            if node.is_leaf(buf) {
                match node.leaf_search(buf, key) {
                    Err(_) => return Ok(false),
                    Ok(i) => {
                        let buf = self.page_mut(pno).map_err(fetch_to_append)?;
                        node.leaf_delete(buf, i);
                        zero_crc(buf);
                        self.header.values -= 1;
                        self.synced = false;
                        return Ok(true);
                    }
                }
            }
            let ci = node.internal_child_index(buf, key);
            pno = node.internal_child(buf, ci);
        }
        Err(AppendError::CorruptIndex("btree descent exceeded max depth".to_string()))
    }

    /// Remove `key` from the index. Returns `true` if the key was present and removed, `false` if
    /// not found. No node merging is performed — underflowing leaves are left sparse.
    pub fn remove(&mut self, key: &[u8]) -> Result<bool, AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        debug_assert_eq!(
            key.len(),
            self.node.ksize(),
            "key wrong size, expected {}, got {}",
            self.node.ksize(),
            key.len()
        );
        self.remove_kv(key)
    }

    // ---- point API (byte-slice keys; the index's key length is `ksize()`) ----

    /// Save the file position `record_pos` for `key` (inserting or overwriting).
    pub fn save(&mut self, key: &[u8], record_pos: u64) -> Result<(), AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        debug_assert_eq!(
            key.len(),
            self.node.ksize(),
            "key wrong size, expected {}, got {}",
            self.node.ksize(),
            key.len()
        );
        self.synced = false;
        self.insert_kv(key, record_pos)
    }

    /// Load the file position for `key`, or [`FetchError::NotFound`].
    pub fn load(&self, key: &[u8]) -> Result<u64, FetchError> {
        debug_assert_eq!(
            key.len(),
            self.node.ksize(),
            "key wrong size, expected {}, got {}",
            self.node.ksize(),
            key.len()
        );
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
        for p in self.unsynced_pages.drain() {
            if let Some(buf) = self.file.slice_mut(Self::page_offset(p), PAGE_SIZE) {
                if crc_is_zero(buf) {
                    // `crc_state` classifies pages, so stamp never-zero: a genuine CRC of 0 must
                    // not be re-read as the all-zero dirty marker.
                    add_crc32_nonzero(buf);
                }
            }
        }
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
        // CRC + msync all data pages BEFORE rewriting/msyncing the header, so the header (root
        // pointer, page_count, first/last leaf) never becomes durable ahead of the pages it names.
        self.crc_dirty_pages();
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

    #[test]
    fn test_archive_btx_page_set() {
        let mut set = PageSet::default();
        for p in [1, 63, 64, 65, 200, 4_000] {
            set.insert(p);
        }
        set.insert(64); // idempotent
        assert!(
            set.contains(63) && set.contains(4_000) && !set.contains(2) && !set.contains(9_999)
        );
        assert_eq!(set.drain().collect::<Vec<_>>(), vec![1, 63, 64, 65, 200, 4_000]);
        assert!(!set.contains(1) && set.drain().next().is_none(), "drain empties the set");
        set.insert(7);
        set.clear();
        assert!(!set.contains(7));
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
            assert_eq!(idx.header.first_leaf, 1, "the first leaf stays page 1 across splits");
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
        let mutations: [Mutation; 5] = [
            ("root past the tree", |h| h.root_page = h.page_count + 5),
            ("root is the header page", |h| h.root_page = 0),
            ("first leaf past the tree", |h| h.first_leaf = h.page_count),
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
}
