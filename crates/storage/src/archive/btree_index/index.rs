//! The [`BtreeIndex`]: a paged, on-disk B+tree mapping fixed-size byte keys to `u64` pack-file
//! offsets, with sorted point lookup plus (via [`super::iter`]) range/prefix/forward/reverse
//! iteration.
//!
//! The tree lives in a single `index.btx` file of fixed 4 KiB pages (page 0 is the header),
//! **memory-mapped** and worked on in place through [`MmapDataFile`] — no separate page cache; the
//! OS page cache is the cache. It complements the hash-based
//! [`HdxIndex`](crate::archive::digest_index::index::HdxIndex), which offers only point lookups.
//!
//! Durability mirrors the digest index's **lazy-CRC** regime: a modified page's 4-byte CRC trailer
//! is `zero_crc`'d as a "dirty" marker, reads do **not** verify a per-op CRC, and [`Index::sync`]
//! CRCs only the dirty pages (`crc_is_zero`) in one pass, then `msync`s the data pages and finally
//! rewrites + `msync`s the header page (the commit marker) — so a crash always reopens on the last
//! consistent tree. The index is deterministically rebuildable from its pack
//! ([`BtreeIndex::rebuild_from`]), which is why it can defer the CRC and skip per-read
//! verification; [`BtreeIndex::page_crc_scan`] is the off-hot-path integrity check.

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
    crc::{add_crc32, crc_is_zero, crc_state, zero_crc, CrcState},
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

/// A paged, mmap-backed on-disk B+tree "sortable index" over `KSIZE`-byte keys → `u64` file
/// positions.
///
/// Implements [`Index`] over `[u8; KSIZE]` (plus, for `KSIZE == 32`, `B256` adapters). Keys are
/// ordered lexicographically.
#[derive(Debug)]
pub struct BtreeIndex<const KSIZE: usize = 32> {
    header: BtreeHeader,
    file: MmapDataFile,
    read_only: bool,
    synced: bool,
    _index_dir: PathBuf,
}

impl<const KSIZE: usize> BtreeIndex<KSIZE> {
    /// Open (or create) a B+tree index in directory `dir` (file `index.btx`).
    ///
    /// Identity (`version`/`uid`/`appnum`) and geometry (`page_size`/`ksize`/`value_size`) are
    /// stamped from `data_header` on create and validated on reopen.  A fresh index starts as a
    /// single empty leaf.
    pub fn open_btx_file<P: AsRef<Path>>(
        dir: P,
        data_header: &DataHeader,
        read_only: bool,
    ) -> Result<BtreeIndex<KSIZE>, LoadHeaderError> {
        // Force the compile-time page/key feasibility check for this KSIZE.
        let () = Node::<KSIZE>::GEOMETRY_OK;

        let dir = dir.as_ref();
        let dir_created = fs::create_dir(dir).is_ok();
        if dir_created {
            // Brand new index directory; fsync the parent so the entry survives a crash.
            if let Some(parent) = dir.parent() {
                let _ = fsync_directory(parent);
            }
        }
        // In-place page overwrites → Random write mode; point-lookup descent → Random access hint.
        let opts = MmapFileOptions {
            write_mode: WriteMode::Random,
            access: MmapAccess::Random,
            ..Default::default()
        };
        let mut file = MmapDataFile::open_with(dir.join("index.btx"), read_only, opts)?;

        let header = if file.is_empty() {
            if read_only {
                return Err(LoadHeaderError::ReadOnlyEmpty);
            }
            let header = BtreeHeader::new(data_header, KSIZE as u16);
            file.ensure_len(2 * PAGE_SIZE as u64)?;
            // Page 0: header (valid CRC — it is the commit marker).
            let page = header.to_page();
            file.slice_mut(0, PAGE_SIZE)
                .ok_or_else(|| io::Error::other("header page not mapped"))?
                .copy_from_slice(&page);
            // Page 1: the empty root leaf (valid CRC).
            {
                let leaf = file
                    .slice_mut(PAGE_SIZE as u64, PAGE_SIZE)
                    .ok_or_else(|| io::Error::other("root leaf page not mapped"))?;
                Node::<KSIZE>::init_leaf(leaf, NULL_PAGE, NULL_PAGE);
                add_crc32(leaf);
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
                || header.ksize != KSIZE as u16
                || header.value_size != VALUE_SIZE
            {
                return Err(LoadHeaderError::InvalidIndexGeometry);
            }
            // Normalize to exactly the committed pages when writable — trims any crash padding or a
            // torn tail past `page_count` (which is authoritative). A read-only handle maps as-is;
            // reads are bounded by `page_count`, so trailing junk is simply never addressed.
            if !read_only {
                let want = header.page_count as u64 * PAGE_SIZE as u64;
                if file.len() != want {
                    file.set_len(want)?;
                }
            }
            header
        };

        Ok(Self { header, file, read_only, synced: true, _index_dir: dir.to_owned() })
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
    pub fn rebuild_from<I>(&mut self, entries: I) -> Result<(), AppendError>
    where
        I: IntoIterator<Item = ([u8; KSIZE], u64)>,
    {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.reset_empty()?;
        for (k, v) in entries {
            self.insert_kv(&k, v)?;
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
        self.file.set_len(0)?; // unmap + truncate to nothing
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
            Node::<KSIZE>::init_leaf(leaf, NULL_PAGE, NULL_PAGE);
            add_crc32(leaf);
        }
        self.file.sync_all()?;
        self.synced = true;
        Ok(())
    }

    // ---- page IO (zero-copy over the mapping; no cache) ----

    fn page_offset(p: u32) -> u64 {
        p as u64 * PAGE_SIZE as u64
    }

    /// Borrow page `p`'s bytes directly from the mapping (no CRC verification — reads trust the
    /// mapping; corruption is caught off-path by [`Self::page_crc_scan`]).
    fn page(&self, p: u32) -> Result<&[u8], FetchError> {
        self.file.slice(Self::page_offset(p), PAGE_SIZE).ok_or(FetchError::CrcFailed)
    }

    /// Borrow page `p`'s bytes mutably from the mapping for in-place modification.
    fn page_mut(&mut self, p: u32) -> Result<&mut [u8], FetchError> {
        self.file.slice_mut(Self::page_offset(p), PAGE_SIZE).ok_or(FetchError::CrcFailed)
    }

    /// Allocate the next page number (append-only bump allocator; no free list), growing the
    /// mapping to cover it. The grown region is zero-filled, so a fresh page reads as a zero-CRC
    /// (dirty) page until it is written and CRC'd at [`Index::sync`].
    fn allocate_page(&mut self) -> Result<u32, io::Error> {
        let p = self.header.page_count;
        self.header.page_count += 1;
        self.file.ensure_len((p as u64 + 1) * PAGE_SIZE as u64)?;
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

    /// Return an owned copy of page `p` for iteration (one copy per leaf hop).
    pub(super) fn fetch_page(&mut self, p: u32) -> Result<Vec<u8>, FetchError> {
        Ok(self.page(p)?.to_vec())
    }

    /// Descend to the leaf page that would contain `key`.
    pub(super) fn find_leaf(&mut self, key: &[u8]) -> Result<u32, FetchError> {
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno)?;
            if Node::<KSIZE>::is_leaf(buf) {
                return Ok(pno);
            }
            let ci = Node::<KSIZE>::internal_child_index(buf, key);
            pno = Node::<KSIZE>::internal_child(buf, ci);
        }
        Err(FetchError::CrcFailed)
    }

    // ---- lookup ----

    fn get_value(&self, key: &[u8]) -> Result<u64, FetchError> {
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno)?;
            if Node::<KSIZE>::is_leaf(buf) {
                return match Node::<KSIZE>::leaf_search(buf, key) {
                    Ok(i) => Ok(Node::<KSIZE>::leaf_value(buf, i)),
                    Err(_) => Err(FetchError::NotFound),
                };
            }
            let ci = Node::<KSIZE>::internal_child_index(buf, key);
            pno = Node::<KSIZE>::internal_child(buf, ci);
        }
        Err(FetchError::CrcFailed)
    }

    // ---- insertion (all in place on the mapping) ----

    fn insert_kv(&mut self, key: &[u8], val: u64) -> Result<(), AppendError> {
        // Descend to the target leaf, recording the (page, child_index) path for split propagation.
        let mut path: Vec<(u32, usize)> = Vec::new();
        let mut pno = self.header.root_page;
        let mut leaf_no = None;
        for _ in 0..MAX_DEPTH {
            let buf = self.page(pno).map_err(fetch_to_append)?;
            if Node::<KSIZE>::is_leaf(buf) {
                leaf_no = Some(pno);
                break;
            }
            let ci = Node::<KSIZE>::internal_child_index(buf, key);
            let child = Node::<KSIZE>::internal_child(buf, ci);
            path.push((pno, ci));
            pno = child;
        }
        let leaf_no = leaf_no.ok_or_else(|| {
            AppendError::SerializeValue("btree descent exceeded max depth".to_string())
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
        // Decide the action from a read-only view of the leaf.
        let (found, at, full) = {
            let buf = self.page(leaf_no).map_err(fetch_to_append)?;
            match Node::<KSIZE>::leaf_search(buf, key) {
                Ok(i) => (Some(i), 0usize, false),
                Err(at) => {
                    (None, at, Node::<KSIZE>::entry_count(buf) >= Node::<KSIZE>::MAX_LEAF_KEYS)
                }
            }
        };
        if let Some(i) = found {
            // Duplicate key: overwrite the value in place; tree shape and count unchanged.
            {
                let buf = self.page_mut(leaf_no).map_err(fetch_to_append)?;
                Node::<KSIZE>::set_leaf_value(buf, i, val);
                zero_crc(buf);
            }
            self.synced = false;
            return Ok(());
        }
        if !full {
            {
                let buf = self.page_mut(leaf_no).map_err(fetch_to_append)?;
                Node::<KSIZE>::leaf_insert(buf, at, key, val);
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
        // Read the old successor before mutating, then allocate the right sibling (may remap).
        let old_next = {
            let l = self.page(leaf_no).map_err(fetch_to_append)?;
            Node::<KSIZE>::leaf_next(l)
        };
        let right_no = self.allocate_page()?;

        // Split the left leaf in place; build the right leaf in a scratch buffer.
        let mut rbuf = vec![0_u8; PAGE_SIZE];
        let sep = {
            let left = self.page_mut(leaf_no).map_err(fetch_to_append)?;
            let sep = Node::<KSIZE>::leaf_split(left, &mut rbuf, at, key, val);
            Node::<KSIZE>::set_leaf_next(left, right_no);
            zero_crc(left);
            sep
        };
        Node::<KSIZE>::set_leaf_prev(&mut rbuf, leaf_no);
        Node::<KSIZE>::set_leaf_next(&mut rbuf, old_next);
        zero_crc(&mut rbuf);
        {
            let r = self.page_mut(right_no).map_err(fetch_to_append)?;
            r.copy_from_slice(&rbuf);
        }
        // Relink the old successor's back-pointer, or record the new rightmost leaf.
        if old_next != NULL_PAGE {
            let nb = self.page_mut(old_next).map_err(fetch_to_append)?;
            Node::<KSIZE>::set_leaf_prev(nb, right_no);
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
        sep: [u8; KSIZE],
        right_no: u32,
    ) -> Result<(), AppendError> {
        let mut sep = sep;
        let mut right_no = right_no;
        while let Some((pno, ci)) = path.pop() {
            let has_room = {
                let buf = self.page(pno).map_err(fetch_to_append)?;
                Node::<KSIZE>::entry_count(buf) < Node::<KSIZE>::MAX_INTERNAL_KEYS
            };
            if has_room {
                {
                    let buf = self.page_mut(pno).map_err(fetch_to_append)?;
                    Node::<KSIZE>::internal_insert(buf, ci, &sep, right_no);
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
                let median = Node::<KSIZE>::internal_split(p, &mut qbuf, ci, &sep, right_no);
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
            Node::<KSIZE>::init_internal(r, old_root);
            Node::<KSIZE>::internal_insert(r, 0, &sep, right_no);
            zero_crc(r);
        }
        self.header.root_page = new_root_no;
        self.header.height += 1;
        self.synced = false;
        Ok(())
    }

    // ---- durability (lazy CRC + msync, header-last commit) ----

    /// CRC every dirty (zero-CRC) data page in one pass. Page 0 (the header) is written separately.
    fn crc_dirty_pages(&mut self) {
        let page_count = self.header.page_count;
        for p in 1..page_count {
            if let Some(buf) = self.file.slice_mut(Self::page_offset(p), PAGE_SIZE) {
                if crc_is_zero(buf) {
                    add_crc32(buf);
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

impl<const KSIZE: usize> Index<[u8; KSIZE], u64> for BtreeIndex<KSIZE> {
    fn save(&mut self, key: [u8; KSIZE], record_pos: u64) -> Result<(), AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.synced = false;
        self.insert_kv(&key, record_pos)
    }

    fn load(&mut self, key: [u8; KSIZE]) -> Result<u64, FetchError> {
        self.get_value(&key)
    }

    fn sync(&mut self) -> Result<(), CommitError> {
        self.sync_impl()
    }
}

/// Convenience adapters so 32-byte digests ([`B256`]) can be used as keys without manual
/// conversion, matching how [`HdxIndex`](crate::archive::digest_index::index::HdxIndex) is called.
///
/// These are inherent methods rather than a second `Index<B256, u64>` impl: a second `Index` impl
/// would make argument-less trait methods (`sync`, and `contains` by inference) ambiguous at the
/// call site.  `B256` is a newtype over `[u8; 32]`, so these just forward to the byte-array impl.
impl BtreeIndex<32> {
    /// Save a `B256` digest → file position mapping (see [`Index::save`]).
    pub fn save_digest(&mut self, key: B256, record_pos: u64) -> Result<(), AppendError> {
        self.save(key.0, record_pos)
    }

    /// Load the file position for a `B256` digest (see [`Index::load`]).
    pub fn load_digest(&mut self, key: B256) -> Result<u64, FetchError> {
        self.load(key.0)
    }
}

impl<const KSIZE: usize> Drop for BtreeIndex<KSIZE> {
    fn drop(&mut self) {
        if !self.read_only && !self.synced {
            if !std::thread::panicking() {
                tracing::warn!("BtreeIndex dropped with unsynced data - caller should call sync()");
            }
            // Commit the tree before MmapDataFile's own clean-close (msync + truncate + fsync).
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
    use crate::archive::pack::PackCompression;

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
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
            assert!(idx.is_empty());
            assert!(matches!(idx.load(key_of(0)), Err(FetchError::NotFound)));
            // A handful of keys, then sync.
            for i in 0..500 {
                idx.save(key_of(i), i).expect("save");
            }
            assert_eq!(idx.len(), 500);
            for i in 0..500 {
                assert_eq!(idx.load(key_of(i)).expect("load"), i);
            }
            idx.sync().expect("sync");
        }

        // Reopen read-write, verify, add more.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("reopen rw");
            assert_eq!(idx.len(), 500);
            for i in 0..500 {
                assert_eq!(idx.load(key_of(i)).expect("load"), i);
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
                BtreeIndex::open_btx_file(&dir, &data_header, true).expect("reopen ro");
            assert_eq!(idx.len(), 800);
            for i in 0..800 {
                assert_eq!(idx.load(key_of(i)).expect("load"), i);
            }
            // B256 read adapter resolves to the same entry.
            assert_eq!(idx.load_digest(B256::from(key_of(7))).expect("load_digest"), 7);
            assert!(matches!(idx.save(key_of(0), 0), Err(AppendError::ReadOnly)));
            assert!(matches!(idx.sync(), Err(CommitError::ReadOnly)));
        }
    }

    #[test]
    fn test_archive_btx_million_with_splits() {
        let tmp = TempDir::with_prefix("test_archive_btx_million").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
        for i in 0..1_000_000u64 {
            idx.save(key_of(i), i).unwrap_or_else(|e| panic!("save {i}: {e}"));
        }
        assert_eq!(idx.len(), 1_000_000);
        // A million random-ish keys must have grown the tree past a single leaf.
        assert!(idx.height() >= 3, "expected a multi-level tree, got height {}", idx.height());
        for i in 0..1_000_000u64 {
            assert_eq!(idx.load(key_of(i)).unwrap_or_else(|e| panic!("load {i}: {e}")), i);
        }

        // Duplicate key overwrites the value; count is unchanged.
        idx.save(key_of(42), 999_999_999).expect("overwrite");
        assert_eq!(idx.load(key_of(42)).expect("load dup"), 999_999_999);
        assert_eq!(idx.len(), 1_000_000);
        idx.sync().expect("sync");
        // A fully synced tree has no dirty or corrupt pages.
        assert_eq!(idx.page_crc_scan(), PageCrcReport::default());
        drop(idx);

        // Reopen read-only and re-verify persistence across the split-heavy tree.
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, true).expect("reopen ro");
        assert_eq!(idx.len(), 1_000_000);
        for i in (0..1_000_000u64).step_by(7) {
            let expect = if i == 42 { 999_999_999 } else { i };
            assert_eq!(idx.load(key_of(i)).expect("load"), expect, "mismatch at {i}");
        }
    }

    #[test]
    fn test_archive_btx_geometry_mismatch() {
        let tmp = TempDir::with_prefix("test_archive_btx_geometry").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
            idx.save(key_of(1), 1).expect("save");
            idx.sync().expect("sync");
        }

        // Reopen with a different key size (16) -> geometry mismatch.
        let res = BtreeIndex::<16>::open_btx_file(&dir, &data_header, false);
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
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
            idx.save(key_of(1), 1).expect("save");
            idx.sync().expect("sync");
        }
        // A DataHeader built from a different uid_idx must be rejected.
        let other = DataHeader::new(7, PackCompression::ZStd, 0);
        let res = BtreeIndex::<32>::open_btx_file(&dir, &other, true);
        assert!(
            matches!(res, Err(LoadHeaderError::InvalidIndexUID)),
            "expected InvalidIndexUID, got {res:?}"
        );
    }

    #[test]
    fn test_archive_btx_torn_tail_normalized() {
        use std::io::Write as _;

        let tmp = TempDir::with_prefix("test_archive_btx_torn").expect("temp dir");
        let dir = tmp.path().join("idx");
        let file = dir.join("index.btx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
            for i in 0..2_000 {
                idx.save(key_of(i), i).expect("save");
            }
            idx.sync().expect("sync");
        }
        let before = std::fs::metadata(&file).expect("meta").len();
        assert!(before.is_multiple_of(PAGE_SIZE as u64), "clean close leaves whole pages");

        // Simulate a torn tail: append a few sub-page bytes past the committed pages.
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&file).expect("append");
            f.write_all(&[0xAB, 0xCD, 0xEF]).expect("write torn");
            f.sync_all().expect("sync");
        }
        assert_eq!(std::fs::metadata(&file).expect("meta").len(), before + 3);

        // A writable reopen normalizes the file back to exactly the committed pages; keys read
        // back.
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, false).expect("reopen rw");
        for i in 0..2_000 {
            assert_eq!(idx.load(key_of(i)).expect("load"), i);
        }
        idx.sync().expect("sync");
        drop(idx);
        assert_eq!(
            std::fs::metadata(&file).expect("meta").len(),
            before,
            "torn tail should be trimmed to page_count pages"
        );
    }

    #[test]
    fn test_archive_btx_rebuild_from() {
        let tmp = TempDir::with_prefix("test_archive_btx_rebuild").expect("temp dir");
        let dir = tmp.path().join("idx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);

        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
        for i in 0..1_000 {
            idx.save(key_of(i), i).expect("save");
        }
        idx.sync().expect("sync");

        // Rebuild from a different set, as if re-derived by re-scanning the pack.
        let entries: Vec<([u8; 32], u64)> = (2_000..2_500).map(|i| (key_of(i), i)).collect();
        idx.rebuild_from(entries.iter().copied()).expect("rebuild");
        idx.sync().expect("sync");
        assert_eq!(idx.len(), 500);
        assert!(matches!(idx.load(key_of(0)), Err(FetchError::NotFound)), "old keys gone");
        for i in 2_000..2_500 {
            assert_eq!(idx.load(key_of(i)).expect("load"), i);
        }
        // Rebuild is rejected on a read-only index.
        drop(idx);
        let mut ro: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, true).expect("reopen ro");
        assert_eq!(ro.len(), 500);
        assert!(matches!(ro.rebuild_from(entries.iter().copied()), Err(AppendError::ReadOnly)));
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
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
            for (k, v) in &all {
                idx.save(*k, *v).expect("save");
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
                BtreeIndex::open_btx_file(&dir, &data_header, true).expect("reopen ro");
            let rep = idx.page_crc_scan();
            assert!(rep.corrupt > 0, "corrupted page must be flagged by the scan, got {rep:?}");
        }

        // Rebuilding from the (pack-derived) entries recovers a fully readable, clean index.
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("reopen rw");
            idx.rebuild_from(all.iter().copied()).expect("rebuild");
            idx.sync().expect("sync");
            for (k, v) in &all {
                assert_eq!(idx.load(*k).expect("load"), *v);
            }
            assert_eq!(idx.page_crc_scan(), PageCrcReport::default(), "rebuilt index is clean");
        }
    }
}
