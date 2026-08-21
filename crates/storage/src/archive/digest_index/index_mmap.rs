//! An mmap-optimized, cache-free twin of [`HdxIndex`](super::index::HdxIndex).
//!
//! [`HdxIndexMmap`] is a drop-in for `HdxIndex` — same [`Index`] trait, same public helpers, and
//! the **same on-disk format** (so their files are cross-compatible) — but it is memory-mapped
//! only and keeps **no bucket caches**. Where `HdxIndex` copies every bucket into `Vec<u8>`
//! buffers held in `HashMap`/`VecDeque` caches, this reads bucket bytes **directly from the
//! mapping** via [`PackFileIo::slice`] (zero-copy) and writes each modified bucket straight back
//! through the mapping. On the mmap backend the OS page cache already *is* the bucket cache, so
//! those copies and caches are pure overhead.
//!
//! ## Reads (zero-copy)
//! [`HdxIndexMmap::find_in_bucket`] borrows the target bucket from the hdx mapping, verifies its
//! CRC, scans it, and — on a miss — follows the append-only overflow chain by re-slicing the odx
//! mapping one record at a time. Exactly one shared slice is live at any moment and no slice is
//! ever held across a write, so a growth/remap (which needs `&mut`) can never invalidate one.
//!
//! ## Writes (write-through)
//! Inserts are read-modify-write: the bucket is copied into a pooled scratch buffer, mutated (its
//! overflow appended to the odx if it fills), stamped with a fresh CRC, and written back at the
//! bucket's fixed offset. mmap writes are `memcpy` into the page cache; durability still happens
//! only at [`Index::sync`] (msync), exactly as the buffered index defers to `fsync`.

use tn_types::B256;

use crate::archive::{
    crc::{add_crc32, check_crc},
    data_file::fsync_directory,
    digest_index::{
        bloom::{Bloom, BLOOM_SIZE_BYTES},
        index::{HdxHeader, HdxIndex, HEADER_SIZE},
        odx_header::OdxHeader,
    },
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    fxhasher::FxHasher,
    index::Index,
    pack::{DataHeader, FileBackend, PackFileIo},
};
use std::{
    collections::BTreeSet,
    fs,
    hash::{BuildHasher, BuildHasherDefault},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// A hash digest index (256-bit digest -> u64 record position) that is memory-mapped only and
/// reads/writes hash buckets directly through the mapping with no in-memory bucket caches.
///
/// It shares the linear-hashing layout and on-disk format of [`HdxIndex`](super::index::HdxIndex)
/// (68-byte header + bloom filter + fixed-size CRC'd buckets in `index.hdx`, backed by an
/// append-only overflow log in `index.odx`), so files written by one can be read by the other.
#[derive(Debug)]
pub struct HdxIndexMmap<
    const KSIZE: usize = 32,
    S: BuildHasher + Default = BuildHasherDefault<FxHasher>,
> {
    header: HdxHeader,
    modulus: u32,
    hdx_file: Box<dyn PackFileIo>,
    // Note, if odx_file is ever replaced in HdxIndexMmap then any held overflow slice would
    // dangle; the read paths take a fresh slice per hop and never hold one across a mutation.
    odx_file: Box<dyn PackFileIo>,
    capacity: u64,
    hasher_builder: S,
    read_only: bool,
    synced: bool,
    bloom: Bloom,
    _index_dir: PathBuf,
}

impl<const KSIZE: usize, S: BuildHasher + Default> HdxIndexMmap<KSIZE, S> {
    /// Bytes of one bucket element: a KSIZE-byte digest plus its u64 record position.
    const BUCKET_ELEMENT_SIZE: usize = HdxIndex::<KSIZE, S>::BUCKET_ELEMENT_SIZE;
    /// Elements a bucket holds before overflowing to the odx log.
    const BUCKET_ELEMENTS: usize = HdxIndex::<KSIZE, S>::BUCKET_ELEMENTS;
    /// On-disk size in bytes of one bucket (including its trailing CRC32).
    const BUCKET_SIZE: usize = HdxIndex::<KSIZE, S>::BUCKET_SIZE;

    /// Open (creating if empty) a memory-mapped HDX index in directory `dir`.
    ///
    /// Note you MUST supply a stable hasher (e.g. fxhasher); the default Rust hasher is not stable
    /// across instances and would invalidate the index. This is the mmap-only analogue of
    /// [`HdxIndex::open_hdx_file`](super::index::HdxIndex::open_hdx_file) and always uses the
    /// memory-mapped file backend.
    pub fn open_hdx_file<P: AsRef<Path>>(
        dir: P,
        data_header: &DataHeader,
        hasher_builder: S,
        read_only: bool,
    ) -> Result<HdxIndexMmap<KSIZE, S>, LoadHeaderError> {
        // This index is memory-mapped only.
        let backend = FileBackend::Mmap;
        let dir = dir.as_ref();
        let dir_created = fs::create_dir(dir).is_ok();
        if dir_created {
            // The index directory is brand new; fsync the parent so the entry survives a crash.
            if let Some(parent) = dir.parent() {
                let _ = fsync_directory(parent);
            }
        }
        // hdx does random-offset overwrites (not append), so `append = false`.
        let mut hdx_file = backend.open_boxed_random(dir.join("index.hdx"), read_only, false)?;
        let file_end = hdx_file.seek(SeekFrom::End(0))?;

        let (header, bloom) = if file_end == 0 {
            if read_only {
                return Err(LoadHeaderError::ReadOnlyEmpty);
            }
            let salt = hasher_builder.hash_one(data_header.uid());
            let pepper = hasher_builder.hash_one(salt);
            let mut header = HdxHeader::from_data_header::<KSIZE, S>(data_header, salt, pepper);
            header.write_header(&mut *hdx_file)?;
            let bloom = Bloom::new();
            hdx_file.write_all(bloom.data())?;
            let bucket_size = header.bucket_size() as usize;
            let mut single_bucket = vec![0_u8; bucket_size];
            add_crc32(&mut single_bucket[..]);
            // Write buckets in large chunks to avoid 100k individual syscalls.
            // All buckets are identical (zeros + CRC32), so tile a chunk buffer.
            let chunk_buckets = 1024.min(header.buckets as usize);
            let chunk = single_bucket.repeat(chunk_buckets);
            let mut remaining = header.buckets as usize;
            while remaining > 0 {
                let n = chunk_buckets.min(remaining);
                hdx_file.write_all(&chunk[..n * bucket_size])?;
                remaining -= n;
            }
            // Header and initial buckets were just written; fsync the directory so the index.hdx
            // entry is durable.
            let _ = fsync_directory(dir);
            (header, bloom)
        } else {
            let header = HdxHeader::load_header(&mut *hdx_file)?;
            if header.version() != data_header.version() {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != data_header.appnum() {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != data_header.uid() {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            // The on-disk bucket geometry must match this binary's compile-time layout, otherwise
            // the fixed-offset slice reads would silently misinterpret bytes.
            if header.bucket_size() != Self::BUCKET_SIZE as u16
                || header.bucket_elements() != Self::BUCKET_ELEMENTS as u16
            {
                return Err(LoadHeaderError::InvalidIndexGeometry);
            }
            // Check the salt/pepper to confirm the same (stable) hasher is in use.
            if header.pepper() != hasher_builder.hash_one(header.salt()) {
                return Err(LoadHeaderError::InvalidHasher);
            }
            let mut bloom_bits = vec![0_u8; BLOOM_SIZE_BYTES];
            hdx_file.read_exact(&mut bloom_bits[..])?;
            let bloom: Bloom = bloom_bits.try_into()?;
            (header, bloom)
        };
        let (odx_file, _odx_header) = OdxHeader::open_odx_file(
            header.version(),
            header.uid(),
            header.appnum(),
            dir.join("index.odx"),
            read_only,
            backend,
        )?;
        // Don't want buckets and modulus to be the same, so +1.
        let modulus = (header.buckets + 1).next_power_of_two();
        let capacity = header.buckets as u64 * header.bucket_elements() as u64;
        Ok(Self {
            header,
            modulus,
            hdx_file,
            odx_file,
            capacity,
            hasher_builder,
            read_only,
            synced: true,
            bloom,
            _index_dir: dir.to_owned(),
        })
    }

    /// Number of keys hashed in this index.
    pub fn len(&self) -> usize {
        self.header.values as usize
    }

    /// True if there are no keys stored in this index.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set the data_file_length field. This tracks information about another file but does not
    /// affect the index.
    pub fn set_data_file_length(&mut self, data_file_length: u64) {
        if self.header.data_file_length != data_file_length {
            self.header.data_file_length = data_file_length;
            self.synced = false;
        }
    }

    /// Get the data_file_length field.
    pub fn data_file_length(&self) -> u64 {
        self.header.data_file_length
    }

    /// Byte offset of `bucket` within the hdx file (after the header and bloom filter).
    fn bucket_pos(&self, bucket: u64) -> u64 {
        (HEADER_SIZE + BLOOM_SIZE_BYTES) as u64 + bucket * Self::BUCKET_SIZE as u64
    }

    /// Number of buckets in the index.
    fn buckets(&self) -> u32 {
        self.header.buckets
    }

    /// Increment the buckets count by 1.
    fn inc_buckets(&mut self) {
        self.header.buckets += 1;
    }

    /// Increment the values count by 1.
    fn inc_values(&mut self) {
        self.header.values += 1;
    }

    /// Return the bucket that will contain hash.
    fn hash_to_bucket(&self, key: &[u8]) -> u64 {
        if key.len() != KSIZE {
            panic!("key wrong size, expected {KSIZE}, got {}", key.len())
        }
        let hash = self.hasher_builder.hash_one(key);
        let modulus = self.modulus as u64;
        let bucket = hash % modulus;
        if bucket >= self.buckets() as u64 {
            bucket - modulus / 2
        } else {
            bucket
        }
    }

    /// Read the overflow-record position stored in the first 8 bytes of a bucket (0 = none).
    fn read_overflow_pos(buf: &[u8]) -> u64 {
        let mut b = [0_u8; 8];
        b.copy_from_slice(&buf[0..8]);
        u64::from_le_bytes(b)
    }

    /// Number of elements currently stored in a bucket buffer.
    fn bucket_elements(buf: &[u8]) -> usize {
        let mut b = [0_u8; 4];
        b.copy_from_slice(&buf[8..12]);
        u32::from_le_bytes(b) as usize
    }

    /// Scan a single bucket buffer for `key`, returning its stored record position if present.
    fn scan_bucket(buf: &[u8], key: &[u8]) -> Option<u64> {
        for i in 0..Self::bucket_elements(buf) {
            let pos = 12 + i * Self::BUCKET_ELEMENT_SIZE;
            if &buf[pos..pos + KSIZE] == key {
                let mut p = [0_u8; 8];
                p.copy_from_slice(&buf[pos + KSIZE..pos + KSIZE + 8]);
                return Some(u64::from_le_bytes(p));
            }
        }
        None
    }

    /// Push every not-yet-seen `(digest, position)` element of a bucket buffer into `out`.
    fn collect_from_buffer(buf: &[u8], out: &mut Vec<(B256, u64)>, seen: &mut BTreeSet<B256>) {
        for i in 0..Self::bucket_elements(buf) {
            let pos = 12 + i * Self::BUCKET_ELEMENT_SIZE;
            let hash = B256::from_slice(&buf[pos..pos + KSIZE]);
            if seen.insert(hash) {
                let mut p = [0_u8; 8];
                p.copy_from_slice(&buf[pos + KSIZE..pos + KSIZE + 8]);
                out.push((hash, u64::from_le_bytes(p)));
            }
        }
    }

    /// Look up `key`, reading bucket bytes zero-copy from the mappings (main bucket in the hdx,
    /// then the append-only overflow chain in the odx). Only one shared slice is live at a time.
    fn find_in_bucket(&self, bucket: u64, key: &[u8]) -> Result<Option<u64>, FetchError> {
        // Scan the in-place (main) bucket directly from the hdx mapping.
        let mut overflow_pos = match self.hdx_file.slice(self.bucket_pos(bucket), Self::BUCKET_SIZE)
        {
            Some(buf) => {
                if !check_crc(buf) {
                    return Err(FetchError::CrcFailed);
                }
                if let Some(pos) = Self::scan_bucket(buf, key) {
                    return Ok(Some(pos));
                }
                Self::read_overflow_pos(buf)
            }
            None => return Ok(None),
        };
        // Follow the append-only overflow chain, one zero-copy slice per hop.
        while overflow_pos > 0 {
            let buf = match self.odx_file.slice(overflow_pos, Self::BUCKET_SIZE) {
                Some(buf) => buf,
                None => return Err(FetchError::CrcFailed),
            };
            if !check_crc(buf) {
                return Err(FetchError::CrcFailed);
            }
            if let Some(pos) = Self::scan_bucket(buf, key) {
                return Ok(Some(pos));
            }
            let next = Self::read_overflow_pos(buf);
            // A non-terminal link must point strictly backwards (the log is append-only); anything
            // else is corruption, so bail rather than loop forever.
            if next != 0 && next >= overflow_pos {
                return Err(FetchError::CrcFailed);
            }
            overflow_pos = next;
        }
        Ok(None)
    }

    /// Collect every unique `(digest, position)` of `bucket` (main bucket + overflow chain) into an
    /// owned vector, keeping the first (most recent) entry per digest. Uses only shared borrows so
    /// the caller can then take `&mut self` to rewrite the buckets.
    fn collect_bucket_elements(&self, bucket: u64) -> Result<Vec<(B256, u64)>, AppendError> {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        let mut overflow_pos = match self.hdx_file.slice(self.bucket_pos(bucket), Self::BUCKET_SIZE)
        {
            Some(buf) => {
                if !check_crc(buf) {
                    return Err(AppendError::CrcError);
                }
                Self::collect_from_buffer(buf, &mut out, &mut seen);
                Self::read_overflow_pos(buf)
            }
            None => return Ok(out),
        };
        while overflow_pos > 0 {
            let buf = match self.odx_file.slice(overflow_pos, Self::BUCKET_SIZE) {
                Some(buf) => buf,
                None => return Err(AppendError::CrcError),
            };
            if !check_crc(buf) {
                return Err(AppendError::CrcError);
            }
            Self::collect_from_buffer(buf, &mut out, &mut seen);
            let next = Self::read_overflow_pos(buf);
            if next != 0 && next >= overflow_pos {
                return Err(AppendError::CrcError);
            }
            overflow_pos = next;
        }
        Ok(out)
    }

    /// Write a fully-formed bucket buffer (CRC already stamped by the caller) back at `bucket_pos`.
    fn write_bucket(&mut self, bucket_pos: u64, buf: &[u8]) -> Result<(), AppendError> {
        self.hdx_file.seek(SeekFrom::Start(bucket_pos)).map_err(AppendError::WriteDataError)?;
        self.hdx_file.write_all(buf).map_err(AppendError::WriteDataError)?;
        Ok(())
    }

    /// Save the (hash, position) tuple into `buffer`, appending an overflow record to the odx log
    /// when the bucket is full. On a duplicate key the existing entry is overwritten in place.
    fn save_to_bucket_buffer(
        &mut self,
        key: &[u8],
        record_pos: u64,
        bucket_pos: u64,
        inc_values: bool,
    ) -> Result<(), AppendError> {
        fn read_u32(buffer: &[u8], pos: &mut usize) -> u32 {
            let mut buf32 = [0_u8; 4];
            buf32.copy_from_slice(&buffer[*pos..(*pos + 4)]);
            *pos += 4;
            u32::from_le_bytes(buf32)
        }

        let Some(buffer) = self.hdx_file.slice_mut(bucket_pos, Self::BUCKET_SIZE) else {
            return Err(AppendError::ReadOnly);
        };
        let mut pos = 8; // Skip over overflow_pos.
        let elements = read_u32(buffer, &mut pos);
        if elements >= self.header.bucket_elements() as u32 {
            // Current bucket is full so overflow: save the full bucket as an overflow record and
            // start a fresh bucket that points to it. A duplicate remains in the overflow but is
            // shadowed by the more recent entry added now.
            let overflow_pos =
                self.odx_file.seek(SeekFrom::End(0)).map_err(AppendError::WriteDataError)?;
            add_crc32(buffer);
            self.odx_file.write_all(buffer).map_err(AppendError::WriteDataError)?;
            buffer.fill(0);
            buffer[0..8].copy_from_slice(&overflow_pos.to_le_bytes());
            buffer[8..12].copy_from_slice(&1_u32.to_le_bytes());
            buffer[12..(12 + KSIZE)].copy_from_slice(key);
            buffer[(12 + KSIZE)..(20 + KSIZE)].copy_from_slice(&record_pos.to_le_bytes());
        } else if elements == 0 {
            // Empty bucket, add first element.
            buffer[8..12].copy_from_slice(&1_u32.to_le_bytes());
            buffer[12..(12 + KSIZE)].copy_from_slice(key);
            buffer[(12 + KSIZE)..(20 + KSIZE)].copy_from_slice(&record_pos.to_le_bytes());
        } else {
            for element in 0..elements {
                let mut pos = 12 + (element as usize * Self::BUCKET_ELEMENT_SIZE);
                let rec_key = &buffer[pos..(pos + KSIZE)];
                if rec_key == key {
                    // Overwrite a duplicate.
                    pos += KSIZE;
                    buffer[pos..pos + 8].copy_from_slice(&record_pos.to_le_bytes());
                    return Ok(());
                }
            }
            let new_elements: u32 = elements + 1;
            buffer[8..12].copy_from_slice(&new_elements.to_le_bytes());
            let mut pos = 12 + (elements as usize * Self::BUCKET_ELEMENT_SIZE);
            buffer[pos..(pos + KSIZE)].copy_from_slice(key);
            pos += KSIZE;
            buffer[pos..pos + 8].copy_from_slice(&record_pos.to_le_bytes());
        }
        if inc_values {
            self.inc_values();
        }
        Ok(())
    }

    /// Add buckets to expand capacity: while the load factor is exceeded, split one bucket.
    fn expand_buckets(&mut self) -> Result<(), AppendError> {
        while self.header.values >= (self.capacity as f32 * self.header.load_factor()) as u64 {
            self.split_one_bucket()?;
            self.capacity = self.buckets() as u64 * self.header.bucket_elements() as u64;
        }
        Ok(())
    }

    /// Split one bucket (in modulus order) into itself plus a newly appended bucket, rehashing and
    /// redistributing its elements. The split bucket is overwritten in place; the new bucket is
    /// written at the current file end (which the random-write mmap extends).
    fn split_one_bucket(&mut self) -> Result<(), AppendError> {
        let old_modulus = self.modulus;
        // The bucket being split.
        let split_bucket = (self.buckets() - (old_modulus / 2)) as u64;
        self.inc_buckets();
        // The newly created bucket that some of split_bucket's items may move into.
        let new_bucket = self.buckets() as u64 - 1;
        // Don't want buckets and modulus to be the same, so +1.
        self.modulus = (self.buckets() + 1).next_power_of_two();
        let split_pos = self.bucket_pos(split_bucket);
        let new_pos = self.bucket_pos(new_bucket);
        // Make sure we have zeroed space for the new bucket.
        self.write_bucket(new_pos, &vec![0_u8; Self::BUCKET_SIZE])?;

        // Gather the split bucket's elements with only shared borrows so the writes below can take
        // `&mut self`.
        let elements = self.collect_bucket_elements(split_bucket)?;

        for (hash, rec_pos) in elements {
            let bucket = self.hash_to_bucket(hash.as_slice());
            if bucket != split_bucket && bucket != new_bucket {
                panic!(
                    "got bucket {}, expected {} or {}, mod {}",
                    bucket, split_bucket, new_bucket, self.modulus
                );
            }
            if bucket == split_bucket {
                self.save_to_bucket_buffer(hash.as_slice(), rec_pos, split_pos, false)?;
            } else {
                self.save_to_bucket_buffer(hash.as_slice(), rec_pos, new_pos, false)?;
            }
        }
        if let Some(buffer) = self.hdx_file.slice_mut(split_pos, Self::BUCKET_SIZE) {
            add_crc32(buffer);
        }
        if let Some(buffer) = self.hdx_file.slice_mut(new_pos, Self::BUCKET_SIZE) {
            add_crc32(buffer);
        }
        //self.write_bucket(split_pos, &buffer)?;
        //self.write_bucket(new_pos, &buffer2)?;
        Ok(())
    }

    /// Save the (hash, position) tuple to its bucket, reading and rewriting the bucket through the
    /// mapping (write-through, no cache).
    fn save_to_bucket(&mut self, key: &[u8], record_pos: u64) -> Result<(), AppendError> {
        let bucket = self.hash_to_bucket(key);
        let bucket_pos = self.bucket_pos(bucket);
        self.save_to_bucket_inner(key, record_pos, bucket_pos)
    }

    /// Read-modify-write body of [`Self::save_to_bucket`], operating on a caller-owned `scratch`
    /// buffer (never `self.scratch`, to avoid a `&mut self` + `&mut self.scratch` overlap). On any
    /// error the on-disk bucket is left untouched.
    fn save_to_bucket_inner(
        &mut self,
        key: &[u8],
        record_pos: u64,
        bucket_pos: u64,
    ) -> Result<(), AppendError> {
        self.save_to_bucket_buffer(key, record_pos, bucket_pos, true)?;
        if let Some(scratch) = self.hdx_file.slice_mut(bucket_pos, Self::BUCKET_SIZE) {
            add_crc32(scratch);
        }
        Ok(())
    }

    /// Write the index's header and bloom filter to disk.
    fn write_header(&mut self) -> Result<(), io::Error> {
        self.header.write_header(&mut *self.hdx_file)?;
        self.hdx_file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        self.hdx_file.write_all(self.bloom.data())?;
        Ok(())
    }

    /// Allow direct test of the bloom filter.
    #[cfg(test)]
    fn test_bloom_contains(&mut self, key: B256) -> bool {
        self.bloom.contains(key)
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Drop for HdxIndexMmap<KSIZE, S> {
    fn drop(&mut self) {
        if !self.read_only && !self.synced {
            if !std::thread::panicking() {
                tracing::warn!(
                    "HdxIndexMmap dropped with unsynced data - caller should call sync()"
                );
            }
            if let Err(e) = self.write_header() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndexMmap: failed to write header on drop: {e}");
                }
            }
            if let Err(e) = self.hdx_file.flush() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndexMmap: failed to flush file on drop: {e}");
                }
            }
            // Sync the overflow log before the index: hdx buckets reference odx overflow records by
            // position, so if the hdx is durable but the odx tail is lost, those positions dangle.
            if let Err(e) = self.odx_file.sync_all() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndexMmap: failed to sync overflow file on drop: {e}");
                }
            }
            if let Err(e) = self.hdx_file.sync_all() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndexMmap: failed to sync file on drop: {e}");
                }
            }
        }
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Index<B256, u64> for HdxIndexMmap<KSIZE, S> {
    fn save(&mut self, key: B256, record_pos: u64) -> Result<(), AppendError> {
        if self.read_only {
            Err(AppendError::ReadOnly)
        } else {
            self.synced = false;
            // Make sure we have reasonable capacity first.
            self.expand_buckets()?;
            // Add to our bloom filter for quick lookups.
            self.bloom.accrue(key);
            self.save_to_bucket(key.as_slice(), record_pos)
        }
    }

    fn load(&mut self, key: B256) -> Result<u64, FetchError> {
        // Quick check the bloom filter first.
        if !self.bloom.contains(key) {
            Err(FetchError::NotFound)
        } else {
            let bucket = self.hash_to_bucket(key.as_slice());
            match self.find_in_bucket(bucket, key.as_slice())? {
                Some(pos) => Ok(pos),
                None => Err(FetchError::NotFound),
            }
        }
    }

    /// Flush and sync all the index data to disk.
    fn sync(&mut self) -> Result<(), CommitError> {
        if self.read_only {
            Err(CommitError::ReadOnly)
        } else {
            self.write_header().map_err(CommitError::IndexFileSync)?;
            self.odx_file.sync_all().map_err(CommitError::IndexFileSync)?;
            self.hdx_file.sync_all().map_err(CommitError::IndexFileSync)?;
            self.synced = true;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tn_types::DefaultHashFunction;

    use super::*;
    use crate::archive::digest_index::index::HdxIndex;

    fn key(i: u64) -> B256 {
        let mut hasher = DefaultHashFunction::new();
        hasher.update(&format!("idx-{i}").into_bytes());
        B256::from_slice(hasher.finalize().as_bytes())
    }

    /// Full lifecycle on the memory-mapped, cache-free index: fill (forcing bucket splits, i.e.
    /// random-offset overwrites + file growth) and look up, reopen for append and add more, then
    /// reopen read-only and re-verify every key.
    #[test]
    fn test_archive_hdx_index_mmap() {
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_index_mmap").expect("temp dir");
        let tmp_path = tmp_dir.path();
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);
        // Enough to blow past the 1000 initial buckets (× 32 elements) and force splits.
        const N: u64 = 50_000;
        const MORE: u64 = N + 1_000;
        let open = |read_only: bool| -> HdxIndexMmap {
            HdxIndexMmap::open_hdx_file(
                tmp_path.join("index.hdx"),
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                read_only,
            )
            .expect("hdx mmap")
        };

        {
            let mut idx = open(false);
            for i in 0..N {
                idx.save(key(i), i).unwrap_or_else(|e| panic!("save {i}: {e}"));
            }
            for i in 0..N {
                assert!(idx.test_bloom_contains(key(i)));
                assert_eq!(idx.load(key(i)).unwrap_or_else(|e| panic!("load {i}: {e}")), i);
            }
        } // drop flushes the unsynced data
          // Reopen for append, add more, verify old + new, sync.
        {
            let mut idx = open(false);
            for i in N..MORE {
                idx.save(key(i), i).expect("save more");
            }
            for i in 0..MORE {
                assert_eq!(idx.load(key(i)).expect("load"), i);
            }
            idx.sync().expect("sync");
        }
        // Reopen read-only and re-verify everything.
        let mut idx = open(true);
        for i in 0..MORE {
            assert_eq!(idx.load(key(i)).expect("load ro"), i);
        }
    }

    /// Reopening with a bucket geometry that does not match this binary's compile-time layout must
    /// fail rather than silently misread. A different KSIZE produces a different BUCKET_SIZE.
    #[test]
    fn test_archive_hdx_index_mmap_geometry_mismatch() {
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_mmap_geometry").expect("temp dir");
        let tmp_path = tmp_dir.path();
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);

        {
            let builder = BuildHasherDefault::<FxHasher>::default();
            let mut idx: HdxIndexMmap = HdxIndexMmap::open_hdx_file(
                tmp_path.join("index.hdx"),
                &data_header,
                builder,
                false,
            )
            .expect("hdx file");
            idx.save(key(0), 1).expect("add to index");
            idx.sync().expect("sync");
        }

        // Reopen with a different key size (16) -> different BUCKET_SIZE -> rejected.
        let builder = BuildHasherDefault::<FxHasher>::default();
        let res = HdxIndexMmap::<16>::open_hdx_file(
            tmp_path.join("index.hdx"),
            &data_header,
            builder,
            false,
        );
        assert!(
            matches!(res, Err(LoadHeaderError::InvalidIndexGeometry)),
            "expected InvalidIndexGeometry, got {res:?}"
        );
    }

    /// The on-disk format is identical to `HdxIndex`: a file written by `HdxIndexMmap` reads back
    /// correctly through the original `HdxIndex` (on the mmap backend), and vice versa — a file
    /// written by `HdxIndex` on the buffered backend reads back through `HdxIndexMmap`.
    #[test]
    fn test_archive_hdx_cross_compat() {
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_cross").expect("temp dir");
        let tmp_path = tmp_dir.path();
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);
        // Force some splits + overflow so both the hdx and odx paths are exercised.
        const N: u64 = 20_000;

        // (a) mmap writer -> HdxIndex (mmap backend) reader.
        let dir_a = tmp_path.join("a.hdx");
        {
            let mut idx: HdxIndexMmap = HdxIndexMmap::open_hdx_file(
                &dir_a,
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                false,
            )
            .expect("mmap writer");
            for i in 0..N {
                idx.save(key(i), i).expect("save");
            }
            idx.sync().expect("sync");
        }
        {
            let mut idx: HdxIndex = HdxIndex::open_hdx_file_with_backend(
                &dir_a,
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                true,
                FileBackend::Mmap,
            )
            .expect("HdxIndex reader");
            for i in 0..N {
                assert_eq!(idx.load(key(i)).expect("load via HdxIndex"), i);
            }
        }

        // (b) HdxIndex (buffered backend) writer -> mmap reader.
        let dir_b = tmp_path.join("b.hdx");
        {
            let mut idx: HdxIndex = HdxIndex::open_hdx_file_with_backend(
                &dir_b,
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                false,
                FileBackend::Buffered,
            )
            .expect("buffered writer");
            for i in 0..N {
                idx.save(key(i), i).expect("save");
            }
            idx.sync().expect("sync");
        }
        {
            let mut idx: HdxIndexMmap = HdxIndexMmap::open_hdx_file(
                &dir_b,
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                true,
            )
            .expect("mmap reader");
            for i in 0..N {
                assert_eq!(idx.load(key(i)).expect("load via HdxIndexMmap"), i);
            }
        }
    }
}
