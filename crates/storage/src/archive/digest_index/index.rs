//! Contains the Digest Index structure and code.

use tn_types::B256;

use crate::archive::{
    crc::{add_crc32, check_crc},
    digest_index::{
        bloom::{Bloom, BLOOM_SIZE_BYTES},
        bucket_iter::BucketIter,
        odx_header::OdxHeader,
    },
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    fxhasher::{FxHashMap, FxHasher},
    index::Index,
    pack::{DataHeader, DATA_HEADER_BYTES},
};
use std::{
    collections::{BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    hash::{BuildHasher, BuildHasherDefault},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Size of a header.
const HEADER_SIZE: usize = 72;

/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
struct HdxHeader {
    type_id: [u8; 8], // The characters "telcoinx"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
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
    fn load_header(hdx_file: &mut File) -> Result<Self, LoadHeaderError> {
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
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let appnum = u64::from_le_bytes(buf64);
        pos += 8;
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
    fn write_header(&mut self, hdx_file: &mut File) -> Result<(), io::Error> {
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
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 8;
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

    /// Number of buckets in this index file.
    fn buckets(&self) -> u32 {
        self.buckets
    }

    /// Number of elements in each bucket.
    fn bucket_elements(&self) -> u16 {
        self.bucket_elements
    }

    /// Size in bytes of a bucket.
    fn bucket_size(&self) -> u16 {
        self.bucket_size
    }

    /// Load factor converted to a f32.
    fn load_factor(&self) -> f32 {
        self.load_factor as f32 / u16::MAX as f32
    }

    /// Number of elements stored in this DB.
    fn values(&self) -> u64 {
        self.values
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
    fn appnum(&self) -> u64 {
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

    /// How long this index thinks the data file is.
    fn _data_file_length(&self) -> u64 {
        self.data_file_length
    }
}

/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
pub struct HdxIndex<
    const KSIZE: usize = 32,
    S: BuildHasher + Default = BuildHasherDefault<FxHasher>,
> {
    header: HdxHeader,
    modulus: u32,
    bucket_cache: FxHashMap<u64, Vec<u8>>,
    dirty_bucket_cache: FxHashMap<u64, Vec<u8>>,
    /// Maintain a fifo of cached buckets.  This is a simple way to make sure the cache is not the
    /// entire set of buckets (bounded by disk space).
    bucket_cache_fifo: VecDeque<u64>,
    hdx_file: File,
    // Note, if odx_file is ever replaced in HdxIndex then see bucket_iter for undefined behaviour.
    pub(crate) odx_file: File,
    capacity: u64,
    hasher_builder: S,
    read_only: bool,
    synced: bool,
    bloom: Bloom,
    _index_dir: PathBuf,
}

impl<const KSIZE: usize, S: BuildHasher + Default> HdxIndex<KSIZE, S> {
    /// Each bucket element is a (KSIZE bytes, u64)- (digest, record_pos).
    pub const BUCKET_ELEMENT_SIZE: usize = KSIZE + 8;
    /// Number of buckets to allocate in a fresh index.
    pub const INITIAL_BUCKETS: usize = 100_000;
    /// Number of elements each bucket can hold before overflowing.
    pub const BUCKET_ELEMENTS: usize = 32;
    /// How large (in bytes) is each bucket.
    pub const BUCKET_SIZE: usize = 16 + (Self::BUCKET_ELEMENT_SIZE * Self::BUCKET_ELEMENTS);
    /// How many buckets to cache (read) at once.
    /// At current settings (32 byte keys) this could lead to an 500M bucket cache...
    /// This should be a large enough limit to never be hit in use but provide a backstop just in
    /// case.
    pub const CACHED_BUCKETS: usize = 400_000;

    /// Open a HDX index file and return the open file and the header.
    /// Note you MUST supply a stable hasher or the index will not work-
    /// for instance fxhasher.  The default Rust hasher is NOT stable (i.e.
    /// it can produce different hashes for the same input on different instances)
    pub fn open_hdx_file<P: AsRef<Path>>(
        dir: P,
        data_header: &DataHeader,
        hasher_builder: S,
        read_only: bool,
    ) -> Result<HdxIndex<KSIZE, S>, LoadHeaderError> {
        let dir = dir.as_ref();
        let _ = fs::create_dir(dir);
        let mut hdx_file = if read_only {
            OpenOptions::new().read(true).write(false).open(dir.join("index.hdx"))?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.join("index.hdx"))?
        };
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
            let mut buffer = vec![0_u8; bucket_size];
            add_crc32(&mut buffer[..]);
            for _ in 0..header.buckets() {
                hdx_file.write_all(&buffer[..])?;
            }
            (header, bloom)
        } else {
            let header = HdxHeader::load_header(&mut hdx_file)?;
            // Basic validation of the odx header.
            if header.version() != data_header.version() {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != data_header.appnum() {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != data_header.uid() {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            // Check the salt/pepper.  This will make sure you are using the same hasher and it
            // seems to be stable (not the default Rust hasher for instance) since
            // changing the hasher would invalidate the index.
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
        )?;
        // Don't want buckets and modulus to be the same, so +1
        let modulus = (header.buckets + 1).next_power_of_two();
        let capacity = header.buckets() as u64 * header.bucket_elements() as u64;
        Ok(Self {
            header,
            modulus,
            bucket_cache: FxHashMap::default(),
            dirty_bucket_cache: FxHashMap::default(),
            bucket_cache_fifo: VecDeque::default(),
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
        self.header.values() as usize
    }

    /// True if there are no keys stored in this index.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set the data_file_length field.
    /// This can be useful for tracking information about another file but
    /// does not effect the index.
    pub fn set_data_file_length(&mut self, data_file_length: u64) {
        self.header.data_file_length = data_file_length;
    }

    /// Get the data_file_length field.
    /// This can be useful for tracking information about another file but
    /// does not effect the index.
    pub fn data_file_length(&self) -> u64 {
        self.header.data_file_length
    }

    /// Save a bucket to the bucket cache.
    fn add_bucket_to_cache(&mut self, bucket: u64, buffer: Vec<u8>) {
        // Remove the least recently added buckets from cache until we are below CACHED_BUCKETS.
        // This is a D-U-M but very simple way to keep memory usage for the cache in check.
        // Also, it should not actually get used unless something has gone wrong, the default
        // should be more than enough buckets for single epoch pack file.
        while self.bucket_cache_fifo.len() >= Self::CACHED_BUCKETS {
            if let Some(bucket) = self.bucket_cache_fifo.pop_front() {
                self.bucket_cache.remove(&bucket);
            }
        }
        self.bucket_cache.insert(bucket, buffer);
        self.bucket_cache_fifo.push_back(bucket);
    }

    /// Add buckets to expand capacity.
    /// Capacity is number of elements per bucket * number of buckets.
    /// If current length >= capacity * load factor then split buckets until this is not true.
    fn expand_buckets(&mut self) -> Result<(), AppendError> {
        while self.header.values >= (self.capacity as f32 * self.header.load_factor()) as u64 {
            self.split_one_bucket()?;
            self.capacity = self.buckets() as u64 * self.header.bucket_elements() as u64;
        }
        Ok(())
    }

    /// Write the indexes header and bloom filter to disk.
    fn write_header(&mut self) -> Result<(), io::Error> {
        self.header.write_header(&mut self.hdx_file)?;
        self.hdx_file.seek(SeekFrom::Start(self.header.header_size() as u64))?;
        self.hdx_file.write_all(self.bloom.data())?;
        Ok(())
    }

    /// Increment the buckets count by 1.
    fn inc_buckets(&mut self) {
        self.header.buckets += 1;
    }

    /// Number of buckets in the index.
    fn buckets(&self) -> u32 {
        self.header.buckets()
    }

    /// Increment the values by 1.
    fn inc_values(&mut self) {
        self.header.values += 1;
    }

    /// Remove a value from a cache, try the dirty buckets then the read cached buckets.
    fn remove_bucket_cache(&mut self, bucket: u64) -> Option<Vec<u8>> {
        if let Some(buffer) = self.dirty_bucket_cache.remove(&bucket) {
            Some(buffer)
        } else {
            self.bucket_cache.remove(&bucket)
        }
    }

    /// Return the buffer for bucket, if found in cache then remove and return that buffer vs
    /// allocate.
    fn remove_bucket(&mut self, bucket: u64) -> Result<Vec<u8>, AppendError> {
        if let Some(buf) = self.remove_bucket_cache(bucket) {
            // Get the bucket from the bucket cache.
            Ok(buf)
        } else {
            // Read the bucket from the index and verify (crc32) it.
            let bucket_size = self.header.bucket_size as usize;
            let mut buffer = vec![0_u8; bucket_size];
            let bucket_pos: u64 = (self.header.header_size()
                + BLOOM_SIZE_BYTES
                + (bucket as usize * bucket_size)) as u64;
            {
                self.hdx_file
                    .seek(SeekFrom::Start(bucket_pos))
                    .map_err(AppendError::WriteDataError)?;
                self.hdx_file.read_exact(&mut buffer[..]).map_err(AppendError::WriteDataError)?;
                if !check_crc(&buffer[..]) {
                    return Err(AppendError::CrcError);
                }
            }
            Ok(buffer)
        }
    }

    /// Flush (save) the hash bucket cache to disk.
    fn save_bucket_cache(&mut self) -> Result<(), io::Error> {
        let bucket_size = self.header.bucket_size as usize;
        let header_size = self.header.header_size();
        for (bucket, mut buffer) in self.dirty_bucket_cache.drain() {
            let bucket_pos: u64 =
                (header_size + BLOOM_SIZE_BYTES + (bucket as usize * bucket_size)) as u64;
            add_crc32(&mut buffer[..]);
            // Seeking and writing past the file end extends it.
            self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            self.hdx_file.write_all(&buffer[..])?;
            self.bucket_cache.insert(bucket, buffer);
        }
        self.dirty_bucket_cache.shrink_to_fit();
        Ok(())
    }

    /// Return the bucket that will contain hash (if hash is available).
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

    /// Add one new bucket to the hash index.
    /// Buckets are split "in order" determined by the current modulus not based on how full any
    /// bucket is.
    fn split_one_bucket(&mut self) -> Result<(), AppendError> {
        let old_modulus = self.modulus;
        // This is the bucket that is being split.
        let split_bucket = (self.buckets() - (old_modulus / 2)) as u64;
        self.inc_buckets();
        // This is the newly created bucket that the items in split_bucket will possibly be moved
        // into.
        let new_bucket = self.buckets() as u64 - 1;
        // Don't want buckets and modulus to be the same, so +1
        self.modulus = (self.buckets() + 1).next_power_of_two();

        let bucket_size = self.header.bucket_size() as usize;
        let mut buffer = vec![0; bucket_size];
        let mut buffer2 = vec![0; bucket_size];

        let mut _t_buf = None;
        let mut iter = if let Ok(buffer) = self.remove_bucket(split_bucket) {
            _t_buf = Some(buffer);
            BucketIter::new(_t_buf.as_ref().expect("not None"))
        } else {
            BucketIter::new_empty()
        };
        // Make sure we only keep the first (most recent) key if we find duplicates.
        let mut rec_hashes = BTreeSet::new();
        while let Some((rec_hash, rec_pos)) = self.next_bucket_element(&mut iter) {
            if rec_pos > 0 {
                let hash = B256::from_slice(rec_hash);
                if !rec_hashes.contains(&hash) {
                    let bucket = self.hash_to_bucket(rec_hash);
                    if bucket != split_bucket && bucket != new_bucket {
                        panic!(
                            "got bucket {}, expected {} or {}, mod {}",
                            bucket,
                            split_bucket,
                            self.buckets() - 1,
                            self.modulus
                        );
                    }
                    if bucket == split_bucket {
                        self.save_to_bucket_buffer(rec_hash, rec_pos, &mut buffer, false)?;
                    } else {
                        self.save_to_bucket_buffer(rec_hash, rec_pos, &mut buffer2, false)?;
                    }
                    rec_hashes.insert(hash);
                }
            }
        }
        if iter.crc_failure() {
            return Err(AppendError::CrcError);
        }
        drop(iter);
        self.dirty_bucket_cache.insert(split_bucket, buffer);
        self.dirty_bucket_cache.insert(new_bucket, buffer2);
        Ok(())
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    fn save_to_bucket(&mut self, key: &[u8], record_pos: u64) -> Result<(), AppendError> {
        let bucket = self.hash_to_bucket(key);
        let mut buffer = self.remove_bucket(bucket)?;

        let result = self.save_to_bucket_buffer(key, record_pos, &mut buffer[..], true);
        // Need to make sure the bucket goes into the cache even on error.
        self.dirty_bucket_cache.insert(bucket, buffer);
        result
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    fn fetch_position(&mut self, key: &[u8]) -> Result<u64, FetchError> {
        let bucket = self.hash_to_bucket(key);

        let mut result = None;
        let mut t_buf = None;
        let t_buf_dirty = self.dirty_bucket_cache.contains_key(&bucket);
        let mut iter = if let Ok(buffer) = self.remove_bucket(bucket) {
            t_buf = Some(buffer);
            BucketIter::new(t_buf.as_ref().expect("not None"))
        } else {
            BucketIter::new_empty()
        };
        while let Some((rec_hash, rec_pos)) = self.next_bucket_element(&mut iter) {
            if key == rec_hash {
                result = Some(rec_pos);
                break;
            }
        }
        let crc_failure = iter.crc_failure();
        drop(iter);
        if let Some(buffer) = t_buf.take() {
            // Make sure we return this to the cache especially if dirty.
            if t_buf_dirty {
                self.dirty_bucket_cache.insert(bucket, buffer);
            } else {
                self.add_bucket_to_cache(bucket, buffer);
            }
        }
        if let Some(result) = result {
            Ok(result)
        } else if crc_failure {
            Err(FetchError::CrcFailed)
        } else {
            Err(FetchError::NotFound)
        }
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    /// If this produces an Error then buffer will contain the same data.
    fn save_to_bucket_buffer(
        &mut self,
        key: &[u8],
        record_pos: u64,
        buffer: &mut [u8],
        inc_values: bool,
    ) -> Result<(), AppendError> {
        fn read_u32(buffer: &[u8], pos: &mut usize) -> u32 {
            let mut buf32 = [0_u8; 4];
            buf32.copy_from_slice(&buffer[*pos..(*pos + 4)]);
            *pos += 4;
            u32::from_le_bytes(buf32)
        }

        let mut pos = 8; // Skip over overflow_pos
        let elements = read_u32(buffer, &mut pos);
        if elements >= self.header.bucket_elements() as u32 {
            // Current bucket is full so overflow.
            // First, save bucket as an overflow record and add to the fresh bucket.
            // Note if this is a duplicate then the old record will remain in the overflow
            // but will be "shadowed" by the more recent entry added now.
            let overflow_pos =
                self.odx_file.seek(SeekFrom::End(0)).map_err(AppendError::WriteDataError)?;
            add_crc32(buffer);
            // Write the old buffer into the data file as an overflow record.
            self.odx_file.write_all(buffer).map_err(AppendError::WriteDataError)?;
            // clear buffer and reset to 0.
            buffer.fill(0);
            // Copy the position of the overflow record into the first u64.
            buffer[0..8].copy_from_slice(&overflow_pos.to_le_bytes());
            buffer[8..12].copy_from_slice(&1_u32.to_le_bytes());
            // First element will be the hash and position being saved (rest of new bucket is
            // empty).
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
                    // We just overwrite a duplicate.
                    pos += KSIZE;
                    buffer[pos..pos + 8].copy_from_slice(&record_pos.to_le_bytes());
                    return Ok(());
                }
            }
            let new_elements: u32 = elements + 1;
            buffer[8..12].copy_from_slice(&new_elements.to_le_bytes());
            // Seek to the element we found, insert hash and position into it.
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

    /// Allow direct test of the bloom filter.
    #[cfg(test)]
    fn test_bloom_contains(&mut self, key: B256) -> bool {
        self.bloom.contains(key)
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Drop for HdxIndex<KSIZE, S> {
    fn drop(&mut self) {
        if !self.read_only && !self.synced {
            if !std::thread::panicking() {
                tracing::warn!("HdxIndex dropped with unsynced data - caller should call sync()");
            }
            if let Err(e) = self.write_header() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndex: failed to write header on drop: {e}");
                }
            }
            if let Err(e) = self.save_bucket_cache() {
                if !std::thread::panicking() {
                    tracing::error!("HdxIndex: failed to flush bucket cache on drop: {e}");
                }
            }
        }
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Index<B256> for HdxIndex<KSIZE, S> {
    fn save(&mut self, key: B256, record_pos: u64) -> Result<(), AppendError> {
        if self.read_only {
            Err(AppendError::ReadOnly)
        } else {
            self.synced = false;
            // Make sure we have resonable capacity first.
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
            self.fetch_position(key.as_slice())
        }
    }

    /// Flush and sync all the index data to disk.
    fn sync(&mut self) -> Result<(), CommitError> {
        if self.read_only {
            Err(CommitError::ReadOnly)
        } else {
            self.write_header().map_err(CommitError::IndexFileSync)?;
            self.save_bucket_cache().map_err(CommitError::IndexFileSync)?;
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

    #[test]
    fn test_archive_hdx_index() {
        let tmp_dir = TempDir::with_prefix("test_archive_hdx_index").expect("temp dir");
        let tmp_path = tmp_dir.path();
        let data_header = DataHeader::new(0);
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut idx: HdxIndex =
            HdxIndex::open_hdx_file(tmp_path.join("index.hdx"), &data_header, builder, false)
                .expect("hdx file");
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            idx.save(hash, i).expect("add to index");
        }
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(idx.test_bloom_contains(hash));
            assert_eq!(idx.load(hash).expect("load idx"), i);
        }
        drop(idx);

        // Verify inder when opened for write.
        // Add some more values as well.
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut idx: HdxIndex =
            HdxIndex::open_hdx_file(tmp_path.join("index.hdx"), &data_header, builder, false)
                .expect("hdx file");
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(idx.test_bloom_contains(hash));
            assert_eq!(idx.load(hash).expect("load idx"), i);
        }
        for i in 1_000_000..1_001_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            idx.save(hash, i).expect("add to index");
        }
        drop(idx);

        // Verify index when opened read only.
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut idx: HdxIndex =
            HdxIndex::open_hdx_file(tmp_path.join("index.hdx"), &data_header, builder, true)
                .expect("hdx file");
        for i in 0..1_001_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(idx.test_bloom_contains(hash));
            assert_eq!(idx.load(hash).expect("load idx"), i);
        }
    }
}
