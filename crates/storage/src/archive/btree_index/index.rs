//! The [`BtreeIndex`]: a paged, on-disk B+tree mapping fixed-size byte keys to `u64` pack-file
//! offsets, with sorted point lookup plus (via [`super::iter`]) range/prefix/forward/reverse
//! iteration.
//!
//! The tree is stored in a single `index.btx` file of fixed 4 KiB pages (page 0 holds the header).
//! It complements the hash-based
//! [`HdxIndex`](crate::archive::digest_index::index::HdxIndex), which offers only point lookups.
//! Pages are cached (a clean read cache with FIFO eviction plus a dirty write cache) and made
//! durable by [`Index::sync`], which flushes and fsyncs all data pages *before* rewriting the
//! header page, so a crash always leaves a consistent tree.

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use tn_types::B256;

use crate::archive::{
    btree_index::{
        header::{BtreeHeader, VALUE_SIZE},
        page::{Node, NULL_PAGE, PAGE_SIZE},
    },
    data_file::fsync_directory,
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    fxhasher::FxHashMap,
    index::Index,
    pack::DataHeader,
};

/// How many clean pages to cache before FIFO eviction.  Sized to never be hit in normal use
/// (a backstop against unbounded memory); the OS page cache backs everything beyond it.
const CACHED_PAGES: usize = 400_000;

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

/// A paged, on-disk B+tree "sortable index" over `KSIZE`-byte keys → `u64` file positions.
///
/// Implements [`Index`] over `[u8; KSIZE]` (and, for `KSIZE == 32`, over [`B256`]).  Keys are
/// ordered lexicographically.
#[derive(Debug)]
pub struct BtreeIndex<const KSIZE: usize = 32> {
    header: BtreeHeader,
    /// Clean (disk-backed) pages, bounded by `page_cache_fifo`.
    page_cache: FxHashMap<u32, Vec<u8>>,
    /// Dirty pages awaiting [`Index::sync`].
    dirty_page_cache: FxHashMap<u32, Vec<u8>>,
    /// FIFO of clean cache page numbers for bounded eviction.
    page_cache_fifo: VecDeque<u32>,
    file: File,
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
        let mut file = if read_only {
            OpenOptions::new().read(true).write(false).open(dir.join("index.btx"))?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.join("index.btx"))?
        };
        let file_end = file.seek(SeekFrom::End(0))?;

        let header = if file_end == 0 {
            if read_only {
                return Err(LoadHeaderError::ReadOnlyEmpty);
            }
            let header = BtreeHeader::new(data_header, KSIZE as u16);
            header.write(&mut file)?;
            // Page 1: the empty root leaf.
            let mut leaf = vec![0_u8; PAGE_SIZE];
            Node::<KSIZE>::init_leaf(&mut leaf, NULL_PAGE, NULL_PAGE);
            Node::<KSIZE>::finalize(&mut leaf);
            file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;
            file.write_all(&leaf)?;
            // Header + root leaf are written; fsync the directory so index.btx is durable.
            let _ = fsync_directory(dir);
            header
        } else {
            let header = BtreeHeader::load(&mut file)?;
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
            // The file must be a whole number of pages.  A misaligned tail is a torn final page
            // (crash mid-write); drop it when writable, reject when read-only.
            let len = file.seek(SeekFrom::End(0))?;
            if !len.is_multiple_of(PAGE_SIZE as u64) {
                if read_only {
                    return Err(LoadHeaderError::InvalidIndexGeometry);
                }
                let aligned = (len / PAGE_SIZE as u64) * PAGE_SIZE as u64;
                file.set_len(aligned)?;
            }
            header
        };

        Ok(Self {
            header,
            page_cache: FxHashMap::default(),
            dirty_page_cache: FxHashMap::default(),
            page_cache_fifo: VecDeque::default(),
            file,
            read_only,
            synced: true,
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

    /// Discard the current tree and rebuild it from `entries`.
    ///
    /// The index is deterministically derivable from the immutable pack it accompanies, so a pack
    /// wrapper can recover a corrupt index (e.g. an [`Index::load`] returning
    /// [`FetchError::CrcFailed`]) by re-scanning the pack and feeding `(key, position)` pairs here.
    /// The index must be writable; call [`Index::sync`] afterwards to make the rebuild durable.
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
        self.page_cache.clear();
        self.dirty_page_cache.clear();
        self.page_cache_fifo.clear();
        self.header.root_page = 1;
        self.header.height = 1;
        self.header.page_count = 2;
        self.header.values = 0;
        self.header.first_leaf = 1;
        self.header.last_leaf = 1;
        self.file.set_len(0)?;
        self.header.write(&mut self.file)?;
        let mut leaf = vec![0_u8; PAGE_SIZE];
        Node::<KSIZE>::init_leaf(&mut leaf, NULL_PAGE, NULL_PAGE);
        Node::<KSIZE>::finalize(&mut leaf);
        self.file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;
        self.file.write_all(&leaf)?;
        self.file.sync_all()?;
        self.synced = true;
        Ok(())
    }

    // ---- page cache / IO ----

    fn page_offset(p: u32) -> u64 {
        p as u64 * PAGE_SIZE as u64
    }

    /// Read a page from disk and verify its CRC32.
    fn read_page_from_disk(&self, p: u32) -> Result<Vec<u8>, FetchError> {
        let mut buf = vec![0_u8; PAGE_SIZE];
        let mut f = &self.file;
        f.seek(SeekFrom::Start(Self::page_offset(p)))?;
        f.read_exact(&mut buf)?;
        if !Node::<KSIZE>::verify(&buf) {
            return Err(FetchError::CrcFailed);
        }
        Ok(buf)
    }

    /// Ensure page `p` is present in a cache (dirty or clean), reading from disk on a miss.
    fn ensure_cached(&mut self, p: u32) -> Result<(), FetchError> {
        if self.dirty_page_cache.contains_key(&p) || self.page_cache.contains_key(&p) {
            return Ok(());
        }
        let buf = self.read_page_from_disk(p)?;
        self.cache_clean(p, buf);
        Ok(())
    }

    /// Borrow a page that [`Self::ensure_cached`] has already loaded.
    fn cached(&self, p: u32) -> &[u8] {
        if let Some(b) = self.dirty_page_cache.get(&p) {
            return b;
        }
        self.page_cache.get(&p).expect("page must be ensure_cached() first")
    }

    /// Insert a clean page into the read cache, evicting FIFO if over capacity.
    fn cache_clean(&mut self, p: u32, buf: Vec<u8>) {
        self.page_cache.insert(p, buf);
        self.page_cache_fifo.push_back(p);
        while self.page_cache_fifo.len() > CACHED_PAGES {
            if let Some(old) = self.page_cache_fifo.pop_front() {
                if old != p {
                    self.page_cache.remove(&old);
                }
            }
        }
    }

    /// Take ownership of a page's buffer for mutation (removing it from whichever cache holds it,
    /// or reading it from disk).
    fn take_page(&mut self, p: u32) -> Result<Vec<u8>, FetchError> {
        if let Some(b) = self.dirty_page_cache.remove(&p) {
            return Ok(b);
        }
        if let Some(b) = self.page_cache.remove(&p) {
            return Ok(b);
        }
        self.read_page_from_disk(p)
    }

    /// Return a (mutated) page to the dirty cache.
    fn put_dirty(&mut self, p: u32, buf: Vec<u8>) {
        self.page_cache.remove(&p);
        self.dirty_page_cache.insert(p, buf);
        self.synced = false;
    }

    /// Allocate the next page number (append-only bump allocator; no free list).
    fn allocate_page(&mut self) -> u32 {
        let p = self.header.page_count;
        self.header.page_count += 1;
        p
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

    /// Return an owned, CRC-checked copy of leaf/page `p` for iteration.
    pub(super) fn fetch_page(&mut self, p: u32) -> Result<Vec<u8>, FetchError> {
        self.ensure_cached(p)?;
        Ok(self.cached(p).to_vec())
    }

    /// Descend to the leaf page that would contain `key`.
    pub(super) fn find_leaf(&mut self, key: &[u8]) -> Result<u32, FetchError> {
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            self.ensure_cached(pno)?;
            let buf = self.cached(pno);
            if Node::<KSIZE>::is_leaf(buf) {
                return Ok(pno);
            }
            let ci = Node::<KSIZE>::internal_child_index(buf, key);
            pno = Node::<KSIZE>::internal_child(buf, ci);
        }
        Err(FetchError::CrcFailed)
    }

    // ---- lookup ----

    fn get_value(&mut self, key: &[u8]) -> Result<u64, FetchError> {
        let mut pno = self.header.root_page;
        for _ in 0..MAX_DEPTH {
            self.ensure_cached(pno)?;
            let buf = self.cached(pno);
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

    // ---- insertion ----

    fn insert_kv(&mut self, key: &[u8], val: u64) -> Result<(), AppendError> {
        // Descend to the target leaf, recording the (page, child_index) path for split propagation.
        let mut path: Vec<(u32, usize)> = Vec::new();
        let mut pno = self.header.root_page;
        let mut leaf_no = None;
        for _ in 0..MAX_DEPTH {
            self.ensure_cached(pno).map_err(fetch_to_append)?;
            let buf = self.cached(pno);
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
        let leaf_buf = self.take_page(leaf_no).map_err(fetch_to_append)?;
        self.insert_into_leaf(leaf_no, leaf_buf, path, key, val)
    }

    fn insert_into_leaf(
        &mut self,
        leaf_no: u32,
        mut buf: Vec<u8>,
        path: Vec<(u32, usize)>,
        key: &[u8],
        val: u64,
    ) -> Result<(), AppendError> {
        match Node::<KSIZE>::leaf_search(&buf, key) {
            Ok(i) => {
                // Duplicate key: overwrite the value, tree shape and count unchanged.
                Node::<KSIZE>::set_leaf_value(&mut buf, i, val);
                self.put_dirty(leaf_no, buf);
                Ok(())
            }
            Err(at) => {
                if Node::<KSIZE>::entry_count(&buf) < Node::<KSIZE>::MAX_LEAF_KEYS {
                    Node::<KSIZE>::leaf_insert(&mut buf, at, key, val);
                    self.put_dirty(leaf_no, buf);
                    self.header.values += 1;
                    Ok(())
                } else {
                    self.split_leaf(leaf_no, buf, path, at, key, val)?;
                    self.header.values += 1;
                    Ok(())
                }
            }
        }
    }

    fn split_leaf(
        &mut self,
        leaf_no: u32,
        mut left: Vec<u8>,
        path: Vec<(u32, usize)>,
        at: usize,
        key: &[u8],
        val: u64,
    ) -> Result<(), AppendError> {
        let right_no = self.allocate_page();
        let mut right = vec![0_u8; PAGE_SIZE];
        let sep = Node::<KSIZE>::leaf_split(&mut left, &mut right, at, key, val);

        // Splice `right` into the leaf chain between `left` and its old successor.
        let old_next = Node::<KSIZE>::leaf_next(&left);
        Node::<KSIZE>::set_leaf_prev(&mut right, leaf_no);
        Node::<KSIZE>::set_leaf_next(&mut right, old_next);
        Node::<KSIZE>::set_leaf_next(&mut left, right_no);
        if old_next != NULL_PAGE {
            let mut nb = self.take_page(old_next).map_err(fetch_to_append)?;
            Node::<KSIZE>::set_leaf_prev(&mut nb, right_no);
            self.put_dirty(old_next, nb);
        } else {
            self.header.last_leaf = right_no;
        }
        self.put_dirty(leaf_no, left);
        self.put_dirty(right_no, right);

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
            let mut buf = self.take_page(pno).map_err(fetch_to_append)?;
            if Node::<KSIZE>::entry_count(&buf) < Node::<KSIZE>::MAX_INTERNAL_KEYS {
                Node::<KSIZE>::internal_insert(&mut buf, ci, &sep, right_no);
                self.put_dirty(pno, buf);
                return Ok(());
            }
            // Internal node full: split it and propagate the median upward.
            let mut new_right = vec![0_u8; PAGE_SIZE];
            let median = Node::<KSIZE>::internal_split(&mut buf, &mut new_right, ci, &sep, right_no);
            let new_right_no = self.allocate_page();
            self.put_dirty(pno, buf);
            self.put_dirty(new_right_no, new_right);
            sep = median;
            right_no = new_right_no;
        }
        // Path exhausted with a pending split: grow a new root one level up.
        let old_root = self.header.root_page;
        let new_root_no = self.allocate_page();
        let mut root = vec![0_u8; PAGE_SIZE];
        Node::<KSIZE>::init_internal(&mut root, old_root);
        Node::<KSIZE>::internal_insert(&mut root, 0, &sep, right_no);
        self.put_dirty(new_root_no, root);
        self.header.root_page = new_root_no;
        self.header.height += 1;
        Ok(())
    }

    // ---- durability ----

    /// Write every dirty page (CRC-stamped) to disk, moving it into the clean cache.
    fn flush_dirty(&mut self) -> Result<(), io::Error> {
        let pages: Vec<u32> = self.dirty_page_cache.keys().copied().collect();
        for p in pages {
            let mut buf = self.dirty_page_cache.remove(&p).expect("just enumerated");
            Node::<KSIZE>::finalize(&mut buf);
            {
                let mut f = &self.file;
                f.seek(SeekFrom::Start(Self::page_offset(p)))?;
                f.write_all(&buf)?;
            }
            self.cache_clean(p, buf);
        }
        Ok(())
    }

    fn sync_impl(&mut self) -> Result<(), CommitError> {
        if self.read_only {
            return Err(CommitError::ReadOnly);
        }
        // Flush + fsync all data pages BEFORE rewriting the header, so the header (root pointer,
        // page_count, first/last leaf) never references a not-yet-durable page.
        self.flush_dirty().map_err(CommitError::IndexFileSync)?;
        self.file.sync_all().map_err(CommitError::IndexFileSync)?;
        self.header.write(&mut self.file).map_err(CommitError::IndexFileSync)?;
        self.file.sync_all().map_err(CommitError::IndexFileSync)?;
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
            if let Err(e) = self.flush_dirty() {
                if !std::thread::panicking() {
                    tracing::error!("BtreeIndex: failed to flush pages on drop: {e}");
                }
            }
            // Data pages first, then header, mirroring sync().
            let _ = self.file.sync_all();
            if let Err(e) = self.header.write(&mut self.file) {
                if !std::thread::panicking() {
                    tracing::error!("BtreeIndex: failed to write header on drop: {e}");
                }
            }
            let _ = self.file.sync_all();
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
    fn test_archive_btx_torn_tail_heals() {
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
        assert!(before.is_multiple_of(PAGE_SIZE as u64), "file should be whole pages");

        // Simulate a torn final page: append a few sub-page bytes.
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&file).expect("append");
            f.write_all(&[0xAB, 0xCD, 0xEF]).expect("write torn");
            f.sync_all().expect("sync");
        }
        assert_eq!(std::fs::metadata(&file).expect("meta").len(), before + 3);

        // A read-only open cannot repair and must reject the misaligned file.
        let ro = BtreeIndex::<32>::open_btx_file(&dir, &data_header, true);
        assert!(matches!(ro, Err(LoadHeaderError::InvalidIndexGeometry)), "got {ro:?}");

        // A writable open heals the torn tail; every key still reads back.
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, false).expect("reopen rw");
        assert_eq!(
            std::fs::metadata(&file).expect("meta").len(),
            before,
            "torn bytes should be truncated"
        );
        for i in 0..2_000 {
            assert_eq!(idx.load(key_of(i)).expect("load"), i);
        }
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
    fn test_archive_btx_crc_detected_and_rebuilt() {
        use std::io::{Seek as _, Write as _};

        let tmp = TempDir::with_prefix("test_archive_btx_crc").expect("temp dir");
        let dir = tmp.path().join("idx");
        let file = dir.join("index.btx");
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        let all: Vec<([u8; 32], u64)> = (0..3_000u64).map(|i| (key_of(i), i)).collect();
        {
            let mut idx: BtreeIndex =
                BtreeIndex::open_btx_file(&dir, &data_header, false).expect("open");
            for (k, v) in &all {
                idx.save(*k, *v).expect("save");
            }
            idx.sync().expect("sync");
        }

        // Corrupt the payload of page 1 (the leftmost leaf) without touching its CRC bytes.
        {
            let mut f =
                std::fs::OpenOptions::new().read(true).write(true).open(&file).expect("open rw");
            f.seek(SeekFrom::Start(PAGE_SIZE as u64 + 100)).expect("seek");
            f.write_all(&[0xFF; 16]).expect("corrupt");
            f.sync_all().expect("sync");
        }

        // The corruption surfaces as a CRC failure for at least one (smallest) key.
        let mut idx: BtreeIndex =
            BtreeIndex::open_btx_file(&dir, &data_header, false).expect("reopen");
        let saw_crc = all.iter().any(|(k, _)| matches!(idx.load(*k), Err(FetchError::CrcFailed)));
        assert!(saw_crc, "expected a CRC failure from the corrupted page");

        // Rebuilding from the (pack-derived) entries recovers a fully readable index.
        idx.rebuild_from(all.iter().copied()).expect("rebuild");
        idx.sync().expect("sync");
        for (k, v) in &all {
            assert_eq!(idx.load(*k).expect("load"), *v);
        }
    }
}
