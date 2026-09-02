//! The memory-mapped, cache-free digest index — the default backend for a pack's hash indexes.
//!
//! [`HdxIndex`] implements the [`Index`] trait over two memory-mapped files (the `hdx` buckets and
//! the `odx` overflow log) and keeps **no bucket caches**: it reads bucket bytes **directly from
//! the mapping** (zero-copy) and writes each modified bucket straight back through it. On an mmap
//! file the OS page cache already *is* the bucket cache, so a separate in-memory bucket cache would
//! be pure overhead.
//!
//! ## Reads (zero-copy)
//! [`HdxIndex::find_in_bucket`] borrows the target bucket from the hdx mapping, scans it, and — on
//! a miss — follows the append-only overflow chain by re-slicing the odx mapping one record at a
//! time. Exactly one shared slice is live at any moment and no slice is ever held across a write,
//! so a growth/remap (which needs `&mut`) can never invalidate one. Reads do **not** verify a
//! per-op CRC (see the CRC note below).
//!
//! ## Writes (write-through)
//! Inserts are read-modify-write: the bucket is mutated in place through the mapping (its overflow
//! appended to the odx if it fills) and written back at the bucket's fixed offset. mmap writes are
//! `memcpy` into the page cache; durability happens at [`Index::sync`] / on drop.
//!
//! ## CRC (deferred, WAL/rebuildable)
//! This index is not the durability source — it is rebuilt from the data-log WAL on an unclean
//! shutdown — so it does not pay a per-op CRC. Each modified bucket has its 4-byte CRC trailer
//! **zeroed** (a "dirty / not-yet-CRC'd" marker); [`Index::sync`] then CRCs **only** the dirty
//! buckets in one pass, leaving a CRC-valid on-disk image. The zero marker also lets recovery tell
//! a dirty bucket (CRC == 0) from a corrupt one (non-zero CRC that fails) via
//! [`HdxIndex::bucket_crc_scan`]. Overflow (odx) records are append-only and CRC'd when written.
//!
//! ## Crash consistency (ordered commit)
//! The header carries `data_file_length`, which `files_consistent` compares against the data log to
//! decide clean-vs-rebuild. [`Index::sync`] (via `ordered_sync`) writes that marker **last**: the
//! bloom + buckets are made durable first, then the header page is msync'd on its own. A crash
//! before that final step leaves the previous, smaller `data_file_length` on disk, so recovery
//! rebuilds from the WAL rather than trusting a torn index.

use tn_types::B256;

use crate::archive::{
    crc::{add_crc32, check_crc, crc_is_zero, crc_state, zero_crc, CrcState},
    data_file::{fsync_directory, MmapAccess, MmapDataFile, MmapFileOptions, WriteMode},
    digest_index::{
        bloom::{Bloom, BLOOM_SIZE_BYTES},
        odx_header::OdxHeader,
    },
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    fxhasher::FxHasher,
    index::Index,
    pack::{DataHeader, DATA_HEADER_BYTES},
};
use std::{
    collections::BTreeSet,
    fs,
    hash::{BuildHasher, BuildHasherDefault},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Size of a header.
const HEADER_SIZE: usize = 68;

/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
struct HdxHeader {
    type_id: [u8; 8], // The characters "telcoinx"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u32,      // Application defined constant
    buckets: u32,
    bucket_elements: u16,
    bucket_size: u16,
    salt: u64,
    pepper: u64,
    load_factor: u16,
    values: u64,
    data_file_length: u64,
}

impl HdxHeader {
    /// Return a default HdxHeader with any values from data_header overridden.
    /// This includes the version, uid, appnum, bucket_size and bucket_elements.
    fn from_data_header<const KSIZE: usize, S: BuildHasher + Default>(
        data_header: &DataHeader,
        salt: u64,
        pepper: u64,
    ) -> Self {
        Self {
            type_id: *b"telcoinx",
            version: data_header.version(),
            uid: data_header.uid(),
            appnum: data_header.appnum(),
            bucket_elements: HdxIndex::<KSIZE, S>::BUCKET_ELEMENTS as u16,
            bucket_size: HdxIndex::<KSIZE, S>::BUCKET_SIZE as u16,
            buckets: HdxIndex::<KSIZE, S>::INITIAL_BUCKETS as u32,
            load_factor: (u16::MAX as f32 * 0.5) as u16,
            salt,
            pepper,
            values: 0,
            data_file_length: DATA_HEADER_BYTES as u64,
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    fn load_header<F: Read + Seek + ?Sized>(hdx_file: &mut F) -> Result<Self, LoadHeaderError> {
        hdx_file.rewind()?;
        let mut buffer = [0_u8; HEADER_SIZE];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        hdx_file.read_exact(&mut buffer[..])?;
        if !check_crc(&buffer[..]) {
            return Err(LoadHeaderError::CrcFailed);
        }
        let mut type_id = [0_u8; 8];
        type_id.copy_from_slice(&buffer[0..8]);
        pos += 8;
        if &type_id != b"telcoinx" {
            return Err(LoadHeaderError::InvalidType);
        }
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let version = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let uid = u64::from_le_bytes(buf64);
        pos += 8;
        buf32.copy_from_slice(&buffer[pos..(pos + 4)]);
        let appnum = u32::from_le_bytes(buf32);
        pos += 4;
        buf32.copy_from_slice(&buffer[pos..(pos + 4)]);
        let buckets = u32::from_le_bytes(buf32);
        pos += 4;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let bucket_elements = u16::from_le_bytes(buf16);
        pos += 2;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let bucket_size = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let salt = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let pepper = u64::from_le_bytes(buf64);
        pos += 8;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let load_factor = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let values = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let data_file_length = u64::from_le_bytes(buf64);
        let header = Self {
            type_id,
            version,
            uid,
            appnum,
            buckets,
            bucket_elements,
            bucket_size,
            salt,
            pepper,
            load_factor,
            values,
            data_file_length,
        };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    fn write_header<F: Write + Seek + ?Sized>(
        &mut self,
        hdx_file: &mut F,
    ) -> Result<(), io::Error> {
        hdx_file.rewind()?;
        let header_size = self.header_size();
        let mut buffer = vec![0_u8; header_size];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 4)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 4;
        buffer[pos..(pos + 4)].copy_from_slice(&self.buckets.to_le_bytes());
        pos += 4;
        buffer[pos..(pos + 2)].copy_from_slice(&self.bucket_elements.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 2)].copy_from_slice(&self.bucket_size.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.salt.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.pepper.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.load_factor.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.values.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.data_file_length.to_le_bytes());
        add_crc32(&mut buffer[..]);
        hdx_file.write_all(&buffer[..])?;
        Ok(())
    }

    /// Return the size of the HDX header.
    fn header_size(&self) -> usize {
        HEADER_SIZE
    }

    /// Number of elements in each bucket.
    fn bucket_elements(&self) -> u16 {
        self.bucket_elements
    }

    /// Size in bytes of a bucket.
    fn bucket_size(&self) -> u16 {
        self.bucket_size
    }

    /// File version number.
    fn version(&self) -> u16 {
        self.version
    }

    /// Unique ID generated on creation
    fn uid(&self) -> u64 {
        self.uid
    }

    /// Application defined constant
    fn appnum(&self) -> u32 {
        self.appnum
    }

    /// Return the index salt.
    fn salt(&self) -> u64 {
        self.salt
    }

    /// Return the index pepper.
    fn pepper(&self) -> u64 {
        self.pepper
    }
}
/// A hash digest index (256-bit digest -> u64 record position) that is memory-mapped only and
/// reads/writes hash buckets directly through the mapping with no in-memory bucket caches.
///
/// It is format compatible with the older direct IO version.
#[derive(Debug)]
pub struct HdxIndex<
    const KSIZE: usize = 32,
    S: BuildHasher + Default = BuildHasherDefault<FxHasher>,
> {
    header: HdxHeader,
    modulus: u32,
    // We require an mmap backed file so use it directly.
    hdx_file: MmapDataFile,
    // Note, if odx_file is ever replaced in HdxIndex then any held overflow slice would
    // dangle; the read paths take a fresh slice per hop and never hold one across a mutation.
    // We require an mmap backed file so use it directly.
    odx_file: MmapDataFile,
    capacity: u64,
    /// Precomputed `values` count at which a bucket split is due (`capacity * load_factor`), kept
    /// in lockstep with `capacity` so the per-insert check in `expand_buckets` is a plain u64
    /// compare instead of u128 math.
    expand_at_capacity: u64,
    hasher_builder: S,
    read_only: bool,
    synced: bool,
    bloom: Bloom,
    _index_dir: PathBuf,
}

/// Counts from [`HdxIndex::bucket_crc_scan`] over the main buckets: how many are dirty
/// (deliberately un-CRC'd — CRC trailer zero) vs corrupt (non-zero CRC that fails to verify). A
/// clean, synced index reports zero of both; `dirty > 0` means writes were not synced (rebuild from
/// the WAL), `corrupt > 0` means genuine on-disk corruption.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketCrcReport {
    /// Main buckets whose CRC trailer is all-zero (written but not yet CRC'd).
    pub dirty: u64,
    /// Main buckets whose non-zero CRC fails to match the payload.
    pub corrupt: u64,
}

impl<const KSIZE: usize, S: BuildHasher + Default> HdxIndex<KSIZE, S> {
    /// Bytes of one bucket element: a KSIZE-byte digest plus its u64 record position.
    const BUCKET_ELEMENT_SIZE: usize = KSIZE + 8;
    /// Elements a bucket holds before overflowing to the odx log.
    const BUCKET_ELEMENTS: usize = 32;
    /// On-disk size in bytes of one bucket: an 8-byte overflow pointer + 4-byte element count +
    /// the elements + a trailing 4-byte CRC32.
    const BUCKET_SIZE: usize = 16 + (Self::BUCKET_ELEMENT_SIZE * Self::BUCKET_ELEMENTS);
    /// Number of buckets to allocate in a fresh index.
    const INITIAL_BUCKETS: usize = 1_000;

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
    ) -> Result<HdxIndex<KSIZE, S>, LoadHeaderError> {
        let dir = dir.as_ref();
        let dir_created = fs::create_dir(dir).is_ok();
        if dir_created {
            // The index directory is brand new; fsync the parent so the entry survives a crash.
            if let Some(parent) = dir.parent() {
                let _ = fsync_directory(parent);
            }
        }

        // The digest index does point lookups over fixed-offset hash buckets — random
        // access with no benefit from readahead — so hint `MADV_RANDOM`.
        let opts = MmapFileOptions {
            write_mode: WriteMode::Random,
            access: MmapAccess::Random,
            ..Default::default()
        };
        let mut hdx_file = MmapDataFile::open_with(dir.join("index.hdx"), read_only, opts)?;
        let file_end = hdx_file.seek(SeekFrom::End(0))?;

        let (header, bloom) = if file_end == 0 {
            if read_only {
                return Err(LoadHeaderError::ReadOnlyEmpty);
            }
            let salt = hasher_builder.hash_one(data_header.uid());
            let pepper = hasher_builder.hash_one(salt);
            let mut header = HdxHeader::from_data_header::<KSIZE, S>(data_header, salt, pepper);
            header.write_header(&mut hdx_file)?;
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
            let header = HdxHeader::load_header(&mut hdx_file)?;
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
        let (odx_file, _odx_header) = OdxHeader::open_odx_file_mmap(
            header.version(),
            header.uid(),
            header.appnum(),
            dir.join("index.odx"),
            read_only,
        )?;
        // Don't want buckets and modulus to be the same, so +1.
        let modulus = (header.buckets + 1).next_power_of_two();
        let capacity = header.buckets as u64 * header.bucket_elements() as u64;
        let expand_at_capacity = Self::expand_threshold(capacity, header.load_factor);
        Ok(Self {
            header,
            modulus,
            hdx_file,
            odx_file,
            capacity,
            expand_at_capacity,
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
        debug_assert_eq!(key.len(), KSIZE, "key wrong size, expected {KSIZE}, got {}", key.len());
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
            Self::collect_from_buffer(buf, &mut out, &mut seen);
            let next = Self::read_overflow_pos(buf);
            if next != 0 && next >= overflow_pos {
                return Err(AppendError::CrcError);
            }
            overflow_pos = next;
        }
        Ok(out)
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

    /// The `values` count at which a bucket split is due (`capacity * load_factor`), precomputed to
    /// a u64 so the per-insert check in [`Self::expand_buckets`] is a plain compare, not u128 math.
    ///
    /// `load_factor` is the fraction `stored / u16::MAX`, so the real threshold is
    /// `capacity * load_factor / u16::MAX`. `div_ceil` preserves the exact
    /// `values * u16::MAX >= capacity * load_factor` boundary (an integer `values >= ceil(x)` is
    /// equivalent to `values * u16::MAX >= capacity * load_factor`), and the f32 route's 24-bit
    /// mantissa rounding above ~16M entries is gone. The result is `<= capacity <= u64::MAX`, so
    /// the `as u64` is lossless; `u128` keeps the `u64 * u16` product from overflowing.
    fn expand_threshold(capacity: u64, load_factor: u16) -> u64 {
        (u128::from(capacity) * u128::from(load_factor)).div_ceil(u128::from(u16::MAX)) as u64
    }

    /// Add buckets to expand capacity: while the load factor is exceeded, split one bucket.
    fn expand_buckets(&mut self) -> Result<(), AppendError> {
        while self.header.values >= self.expand_at_capacity {
            self.split_one_bucket()?;
            self.capacity = self.buckets() as u64 * self.header.bucket_elements() as u64;
            self.expand_at_capacity =
                Self::expand_threshold(self.capacity, self.header.load_factor);
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

        // Gather the split bucket's elements (shared borrows) BEFORE clearing either bucket, then
        // redistribute into two freshly-zeroed buckets — mirroring the original `HdxIndex` split,
        // which builds fresh buffers. The split bucket must be cleared too: if it kept its old
        // contents, the elements that move to `new_bucket` would linger here as stale duplicates,
        // and a later re-split would re-collect them, hash them into a third bucket, and trip the
        // guard below (they are never consulted by lookups, so the only visible symptom is that
        // panic plus unbounded overflow-chain growth).
        let elements = self.collect_bucket_elements(split_bucket)?;

        // Clear both buckets before redistributing. The split bucket is already within the logical
        // end, so zero it in place. The new bucket lies at/after the current end (buckets are
        // contiguous), so first extend the mapping to make room for it: `ensure_len` grows
        // geometrically and zero-extends, so `slice_mut(new_pos, ..)` is then in-bounds and the
        // fresh region is already an empty bucket (the fill below is a cheap defensive memset).
        if let Some(buffer) = self.hdx_file.slice_mut(split_pos, Self::BUCKET_SIZE) {
            // Note this will zero the CRC as well (we want that- marks it "dirty").
            buffer.fill(0);
        } else {
            return Err(AppendError::ReadOnly);
        }
        self.hdx_file.ensure_len(new_pos + Self::BUCKET_SIZE as u64)?;
        if let Some(buffer) = self.hdx_file.slice_mut(new_pos, Self::BUCKET_SIZE) {
            // Note this will zero the CRC as well (we want that- marks it "dirty").
            buffer.fill(0);
        } else {
            return Err(AppendError::ReadOnly);
        }

        for (hash, rec_pos) in elements {
            let bucket = self.hash_to_bucket(hash.as_slice());
            if bucket != split_bucket && bucket != new_bucket {
                // A rehash landing outside the split pair means the on-disk index is corrupt. Fail
                // the save (the pack rebuilds from the WAL) rather than panicking the pack's worker
                // thread and wedging every later request.
                return Err(AppendError::CrcError);
            }
            if bucket == split_bucket {
                self.save_to_bucket_buffer(hash.as_slice(), rec_pos, split_pos, false)?;
            } else {
                self.save_to_bucket_buffer(hash.as_slice(), rec_pos, new_pos, false)?;
            }
        }
        Ok(())
    }

    /// Save the (hash, position) tuple to its bucket, reading and rewriting the bucket through the
    /// mapping (write-through, no cache).
    fn save_to_bucket(&mut self, key: &[u8], record_pos: u64) -> Result<(), AppendError> {
        let bucket = self.hash_to_bucket(key);
        let bucket_pos = self.bucket_pos(bucket);
        self.save_to_bucket_inner(key, record_pos, bucket_pos)
    }

    /// Read-modify-write body of [`Self::save_to_bucket`]: rewrites the bucket in place through the
    /// mapping, then zeroes its trailer to mark it dirty for the bulk CRC at `sync()`. On any error
    /// the on-disk bucket is left untouched.
    fn save_to_bucket_inner(
        &mut self,
        key: &[u8],
        record_pos: u64,
        bucket_pos: u64,
    ) -> Result<(), AppendError> {
        self.save_to_bucket_buffer(key, record_pos, bucket_pos, true)?;
        // Zero the trailer to mark the bucket dirty (CRC'd in bulk at `sync()`); the zero also lets
        // recovery tell a dirty bucket from a corrupt one.
        if let Some(scratch) = self.hdx_file.slice_mut(bucket_pos, Self::BUCKET_SIZE) {
            zero_crc(scratch);
        }
        Ok(())
    }

    /// Write the bloom filter to its fixed region (immediately after the header). Split out from
    /// the header write so [`Self::ordered_sync`] can make the bloom durable *before*
    /// publishing the header, which carries the `data_file_length` commit marker.
    fn write_bloom(&mut self) -> Result<(), io::Error> {
        self.hdx_file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        self.hdx_file.write_all(self.bloom.data())?;
        Ok(())
    }

    /// Write the index header (offset 0). It carries `data_file_length` — the length
    /// `files_consistent` compares against the data log to decide clean-vs-rebuild.
    fn write_header_only(&mut self) -> Result<(), io::Error> {
        self.header.write_header(&mut self.hdx_file)?;
        Ok(())
    }

    /// Durably flush the index with the `data_file_length` commit marker written LAST.
    ///
    /// `files_consistent` trusts the on-disk `data_file_length`: if it matches the data log's
    /// length the index is assumed complete and WAL recovery is skipped. That is only safe if,
    /// whenever `data_file_length` is durable, the buckets + bloom it accounts for are durable
    /// too. So the order matters: stamp bucket CRCs, write the bloom, make the overflow log
    /// then the bloom+buckets durable, and only then publish the header and msync its page on
    /// its own. A crash before that final msync leaves the *previous* (smaller)
    /// `data_file_length` on disk, so `files_consistent` fails and the index is rebuilt from
    /// the WAL rather than trusted torn.
    fn ordered_sync(&mut self) -> Result<(), io::Error> {
        // Stamp a CRC on every dirty (zero-CRC) bucket so the on-disk image is CRC-valid.
        self.crc_dirty_buckets();
        // Write the bloom but NOT the header: the header page keeps its previous data_file_length
        // until the commit msync below.
        self.write_bloom()?;
        // Overflow records the buckets reference must be durable before the buckets.
        self.odx_file.sync_all()?;
        // Make bloom + buckets (and the still-previous header) durable.
        self.hdx_file.sync_all()?;
        // Commit marker: publish the new header, then msync just its page so data_file_length lands
        // durably after everything it accounts for.
        self.write_header_only()?;
        self.hdx_file.sync_range(0, HEADER_SIZE as u64)?;
        Ok(())
    }

    /// Stamp a fresh CRC on every *dirty* main bucket — one whose CRC trailer is zero
    /// (`crc_is_zero`), the marker each write leaves behind. Called by [`Index::sync`]: this makes
    /// the on-disk index CRC-valid (so a later reopen + [`Self::bucket_crc_scan`] sees it clean)
    /// while skipping untouched/clean buckets after a cheap 4-byte check — no CRC is computed for
    /// them. Overflow records in the odx were already CRC'd when appended.
    fn crc_dirty_buckets(&mut self) {
        for bucket in 0..self.buckets() as u64 {
            let pos = self.bucket_pos(bucket);
            if let Some(buffer) = self.hdx_file.slice_mut(pos, Self::BUCKET_SIZE) {
                if crc_is_zero(buffer) {
                    add_crc32(buffer);
                }
            }
        }
    }

    /// Scan every main bucket and classify its CRC trailer (see [`BucketCrcReport`]). Read-only
    /// (shared slices), so it is the recovery/verification hook: a `(0, 0)` report means the
    /// on-disk main buckets are all CRC-valid; any `dirty` means writes were not synced
    /// (rebuild from the data-log WAL); any `corrupt` is genuine corruption. Overflow (odx)
    /// records are not scanned — they are append-only and always CRC'd when written.
    pub fn bucket_crc_scan(&self) -> BucketCrcReport {
        let mut report = BucketCrcReport::default();
        for bucket in 0..self.buckets() as u64 {
            if let Some(buffer) = self.hdx_file.slice(self.bucket_pos(bucket), Self::BUCKET_SIZE) {
                match crc_state(buffer) {
                    CrcState::Valid => {}
                    CrcState::Dirty => report.dirty += 1,
                    CrcState::Corrupt => report.corrupt += 1,
                }
            }
        }
        report
    }

    /// Allow direct test of the bloom filter.
    #[cfg(test)]
    fn test_bloom_contains(&mut self, key: B256) -> bool {
        self.bloom.contains(key)
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Drop for HdxIndex<KSIZE, S> {
    fn drop(&mut self) {
        if !self.read_only && !self.synced {
            // The WAL model never syncs the index on the hot path, so a clean close is the expected
            // place it is made durable — not a misuse to warn about. Use the same ordered flush as
            // `sync` (which also sequences the odx before the hdx buckets that reference it) so a
            // torn close can't fool `files_consistent`.
            if let Err(e) = self.ordered_sync() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndex: failed to sync on drop: {e}");
                }
            }
        }
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Index<B256, u64> for HdxIndex<KSIZE, S> {
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

    /// Flush and sync all the index data to disk. The `data_file_length` commit marker is written
    /// last (see [`Self::ordered_sync`]) so `files_consistent` never trusts a torn index.
    fn sync(&mut self) -> Result<(), CommitError> {
        if self.read_only {
            Err(CommitError::ReadOnly)
        } else {
            self.ordered_sync().map_err(CommitError::IndexFileSync)?;
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
        let open = |read_only: bool| -> HdxIndex {
            HdxIndex::open_hdx_file(
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
            let mut idx: HdxIndex =
                HdxIndex::open_hdx_file(tmp_path.join("index.hdx"), &data_header, builder, false)
                    .expect("hdx file");
            idx.save(key(0), 1).expect("add to index");
            idx.sync().expect("sync");
        }

        // Reopen with a different key size (16) -> different BUCKET_SIZE -> rejected.
        let builder = BuildHasherDefault::<FxHasher>::default();
        let res =
            HdxIndex::<16>::open_hdx_file(tmp_path.join("index.hdx"), &data_header, builder, false);
        assert!(
            matches!(res, Err(LoadHeaderError::InvalidIndexGeometry)),
            "expected InvalidIndexGeometry, got {res:?}"
        );
    }

    /// The zero-CRC "dirty" sentinel. A freshly-opened index is all-valid; a write leaves its
    /// bucket dirty (zero CRC) and `sync()` clears it; `bucket_crc_scan` counts dirty/corrupt
    /// buckets.
    #[test]
    fn test_archive_hdx_index_mmap_dirty_markers() {
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);

        // Part A: a full build + sync leaves no dirty/corrupt bucket, and reopening finds every
        // key.
        let tmp_a = TempDir::with_prefix("test_archive_hdx_dirty_a").expect("temp dir");
        const N: u64 = 50_000; // force splits + overflow
        {
            let mut idx: HdxIndex = HdxIndex::open_hdx_file(
                tmp_a.path().join("index.hdx"),
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                false,
            )
            .expect("open");
            for i in 0..N {
                idx.save(key(i), i).unwrap_or_else(|e| panic!("save {i}: {e}"));
            }
            idx.sync().expect("sync stamps dirty buckets");
            assert_eq!(
                idx.bucket_crc_scan(),
                BucketCrcReport::default(),
                "sync must clear every dirty bucket"
            );
        }
        {
            let mut idx: HdxIndex = HdxIndex::open_hdx_file(
                tmp_a.path().join("index.hdx"),
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                true,
            )
            .expect("reopen");
            for i in 0..N {
                assert_eq!(idx.load(key(i)).unwrap_or_else(|e| panic!("verify {i}: {e}")), i);
            }
        }

        // Part B: exact dirty targeting on a tiny index (one bucket, no splits). Fresh = all valid;
        // one write dirties exactly one bucket; sync clears it; rewriting re-dirties it.
        let tmp_b = TempDir::with_prefix("test_archive_hdx_dirty_b").expect("temp dir");
        let mut idx: HdxIndex = HdxIndex::open_hdx_file(
            tmp_b.path().join("index.hdx"),
            &data_header,
            BuildHasherDefault::<FxHasher>::default(),
            false,
        )
        .expect("open tiny");
        assert_eq!(
            idx.bucket_crc_scan(),
            BucketCrcReport::default(),
            "a fresh index's init buckets all carry a valid (non-zero) CRC"
        );
        idx.save(key(0), 0).expect("save one");
        assert_eq!(
            idx.bucket_crc_scan(),
            BucketCrcReport { dirty: 1, corrupt: 0 },
            "one write dirties exactly one bucket"
        );
        idx.sync().expect("sync one");
        assert_eq!(idx.bucket_crc_scan(), BucketCrcReport::default(), "sync clears the one dirty");
        idx.save(key(0), 0).expect("rewrite one");
        assert_eq!(
            idx.bucket_crc_scan(),
            BucketCrcReport { dirty: 1, corrupt: 0 },
            "rewriting the key re-dirties exactly its bucket"
        );
        idx.sync().expect("resync one");
        assert_eq!(idx.bucket_crc_scan(), BucketCrcReport::default());
    }

    /// `crc_state`/`bucket_crc_scan` tell a dirty bucket (zeroed CRC) from a corrupt one (non-zero
    /// CRC that fails). Build eagerly (all valid), corrupt one bucket's payload (stale CRC =>
    /// corrupt), then zero another bucket's CRC (=> dirty).
    #[test]
    fn test_archive_hdx_index_mmap_crc_state_corrupt_vs_dirty() {
        use crate::archive::crc::zero_crc;
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_crcstate").expect("temp dir");
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);
        const N: u64 = 5_000;

        let mut idx: HdxIndex = HdxIndex::open_hdx_file(
            tmp_dir.path().join("index.hdx"),
            &data_header,
            BuildHasherDefault::<FxHasher>::default(),
            false,
        )
        .expect("open");
        for i in 0..N {
            idx.save(key(i), i).expect("save");
        }
        idx.sync().expect("sync");
        assert_eq!(idx.bucket_crc_scan(), BucketCrcReport::default(), "eager build is all valid");

        // Corrupt bucket 0's payload but keep its (now stale, non-zero) CRC -> corrupt.
        let pos0 = idx.bucket_pos(0);
        {
            let buf =
                idx.hdx_file.slice_mut(pos0, HdxIndex::<32>::BUCKET_SIZE).expect("slice bucket 0");
            buf[12] ^= 0xFF;
        }
        assert_eq!(
            idx.bucket_crc_scan(),
            BucketCrcReport { dirty: 0, corrupt: 1 },
            "a payload change under a stale CRC reads as corrupt"
        );

        // Zero bucket 1's CRC -> dirty (bucket 0 stays corrupt).
        let pos1 = idx.bucket_pos(1);
        {
            let buf =
                idx.hdx_file.slice_mut(pos1, HdxIndex::<32>::BUCKET_SIZE).expect("slice bucket 1");
            zero_crc(buf);
        }
        assert_eq!(
            idx.bucket_crc_scan(),
            BucketCrcReport { dirty: 1, corrupt: 1 },
            "a zeroed CRC reads as dirty, independent of the corrupt bucket"
        );
    }

    /// The ordered commit sync (`ordered_sync`, used by `sync`/`Drop`) leaves a fully consistent
    /// on-disk image: every dirty bucket CRC'd, the `data_file_length` commit marker persisted, and
    /// — on reopen — every key found (no bloom false negative).
    #[test]
    fn test_archive_hdx_index_mmap_ordered_sync() {
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_ordered_sync").expect("temp dir");
        let tmp_path = tmp_dir.path();
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);
        const N: u64 = 50_000; // force splits + overflow
        const DL: u64 = 123_456;

        {
            let mut idx: HdxIndex = HdxIndex::open_hdx_file(
                tmp_path.join("index.hdx"),
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                false,
            )
            .expect("open");
            for i in 0..N {
                idx.save(key(i), i).unwrap_or_else(|e| panic!("save {i}: {e}"));
            }
            idx.set_data_file_length(DL);
            idx.sync().expect("ordered sync");
            assert_eq!(
                idx.bucket_crc_scan(),
                BucketCrcReport::default(),
                "ordered sync must leave no dirty/corrupt bucket"
            );
        } // already synced, so no Drop resync

        // Reopen read-only: the commit marker is persisted and every key is found.
        let mut idx: HdxIndex = HdxIndex::open_hdx_file(
            tmp_path.join("index.hdx"),
            &data_header,
            BuildHasherDefault::<FxHasher>::default(),
            true,
        )
        .expect("reopen");
        assert_eq!(idx.data_file_length(), DL, "data_file_length marker must survive reopen");
        assert_eq!(
            idx.bucket_crc_scan(),
            BucketCrcReport::default(),
            "reopened image is CRC-clean"
        );
        for i in 0..N {
            assert_eq!(idx.load(key(i)).unwrap_or_else(|e| panic!("load {i}: {e}")), i);
        }
    }

    /// `data_file_length` is a commit marker: it reaches disk only when the index is synced, never
    /// on a bare `set_data_file_length`. That is what makes `files_consistent` trigger a WAL
    /// rebuild after an unclean shutdown. A second (read-only) handle must see the last
    /// *synced* value, not a later un-synced one.
    #[test]
    fn test_archive_hdx_index_mmap_commit_marker_last() {
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_commit_marker").expect("temp dir");
        let path = tmp_dir.path().join("index.hdx");
        let data_header = DataHeader::new(0, crate::archive::pack::PackCompression::ZStd, 0);
        let open = |read_only: bool| -> HdxIndex {
            HdxIndex::open_hdx_file(
                &path,
                &data_header,
                BuildHasherDefault::<FxHasher>::default(),
                read_only,
            )
            .expect("open")
        };

        let mut writer = open(false);
        writer.save(key(0), 0).expect("save 0");
        writer.set_data_file_length(100);
        writer.sync().expect("sync 100");

        // Advance the length + write another key but DON'T sync: both stay in-memory only.
        writer.save(key(1), 1).expect("save 1");
        writer.set_data_file_length(200);

        // A fresh read-only handle reads the on-disk header -> still the last synced marker (100).
        let reader = open(true);
        assert_eq!(
            reader.data_file_length(),
            100,
            "un-synced data_file_length must not be on disk"
        );
        drop(reader);

        // After syncing, the new marker is durable.
        writer.sync().expect("sync 200");
        let reader = open(true);
        assert_eq!(reader.data_file_length(), 200, "sync publishes the new marker");
    }
}
