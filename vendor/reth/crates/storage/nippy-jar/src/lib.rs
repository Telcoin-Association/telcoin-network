//! Immutable data store format.
//!
//! *Warning*: The `NippyJar` encoding format and its implementations are
//! designed for storing and retrieving data internally. They are not hardened
//! to safely read potentially malicious data.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]

use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fs::File,
    io::{self, Read, Write},
    ops::Range,
    path::{Path, PathBuf},
};
use tracing::*;

/// Compression algorithms supported by `NippyJar`.
pub mod compression;
#[cfg(test)]
use compression::Compression;
use compression::Compressors;

/// empty enum for backwards compatibility
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Functions {}

/// empty enum for backwards compatibility
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum InclusionFilters {}

mod error;
pub use error::NippyJarError;

mod cursor;
pub use cursor::NippyJarCursor;

mod writer;
pub use writer::NippyJarWriter;

mod consistency;
pub use consistency::NippyJarChecker;

/// The version number of the Nippy Jar format.
const NIPPY_JAR_VERSION: usize = 1;
/// The version of a jar whose values, configuration and changeset offsets carry checksums.
///
/// TN(TN-0120): a version-1 jar is upstream's format, byte for byte. A version-2 jar has the same
/// configuration encoding followed by a CRC32 of it, stores every value as its bytes followed by
/// [`value_checksum`], and its `.csoff` sidecar uses checksummed records
/// (`reth_static_file_types::ChangesetOffsetReader`).
const NIPPY_JAR_VERSION_CHECKSUMS: usize = 2;
/// Size in bytes of the checksum that follows each value of a checksummed jar.
pub const CHECKSUM_SIZE: usize = 4;
/// The file extension used for index files.
const INDEX_FILE_EXTENSION: &str = "idx";
/// The file extension used for offsets files.
const OFFSETS_FILE_EXTENSION: &str = "off";
/// The file extension used for configuration files.
pub const CONFIG_FILE_EXTENSION: &str = "conf";
/// The file extension used for changeset offset sidecar files.
pub const CHANGESET_OFFSETS_FILE_EXTENSION: &str = "csoff";

/// TN(TN-0120): the checksum stored after a value of a checksummed jar: the CRC-32 (ISO-HDLC, the
/// zlib CRC) of the value's stored bytes (compressed, if the jar compresses), started from
/// [`value_seed`] of its position instead of the standard initial value.
///
/// The seed binds the value to its row and column: for inputs of one length the CRC is an
/// injective function of the initial value, so a value read at another position (a damaged offset
/// that lands exactly on another value) fails as surely as a changed byte, without hashing more
/// bytes.
pub fn value_checksum(row: u64, column: usize, stored: &[u8]) -> u32 {
    checksum(value_seed(row, column), stored)
}

/// TN(TN-0120): the initial value of a checksummed jar's value at `row` (within the jar) and
/// `column`, in crc32fast's convention (`crc32fast::Hasher::new_with_initial`, where 0 gives the
/// standard CRC-32):
/// `0x9E37_79B9 ^ (row as u32) ^ ((row >> 32) as u32).rotate_left(16) ^ (column as u32) *
/// 0x85EB_CA6B` (wrapping). For rows below 2^32, two values of one column never share a seed, and
/// values of two columns share one only if their rows differ by the XOR of the two column terms
/// (at least 2^27 rows apart), so neighbouring values always differ.
pub const fn value_seed(row: u64, column: usize) -> u32 {
    0x9E37_79B9 ^
        (row as u32) ^
        ((row >> 32) as u32).rotate_left(16) ^
        (column as u32).wrapping_mul(0x85EB_CA6B)
}

/// TN(TN-0120): the CRC-32 (ISO-HDLC) of `bytes` from the initial value `seed` (crc32fast's
/// convention). On `x86_64` below 128 bytes crc32fast runs a byte-at-a-time loop, so these inputs
/// take the `crc` crate's slice-by-16 table instead; both give the same value.
pub fn checksum(seed: u32, bytes: &[u8]) -> u32 {
    if cfg!(target_arch = "x86_64") && bytes.len() < 128 {
        checksum_slice16(seed, bytes)
    } else {
        checksum_crc32fast(seed, bytes)
    }
}

/// [`checksum`] through crc32fast: hardware CRC on aarch64, PCLMULQDQ on `x86_64` from 128 bytes.
fn checksum_crc32fast(seed: u32, bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new_with_initial(seed);
    hasher.update(bytes);
    hasher.finalize()
}

/// [`checksum`] through the `crc` crate's slice-by-16 table.
fn checksum_slice16(seed: u32, bytes: &[u8]) -> u32 {
    /// The CRC-32 the jar uses, with a 16-table slicing implementation.
    static CRC32: crc::Crc<u32, crc::Table<16>> =
        crc::Crc::<u32, crc::Table<16>>::new(&crc::CRC_32_ISO_HDLC);
    // crc32fast keeps the finished (inverted) value as its state; `crc` takes the register's
    // initial value, which for this reflected CRC it bit-reverses first.
    let mut digest = CRC32.digest_with_initial((!seed).reverse_bits());
    digest.update(bytes);
    digest.finalize()
}

/// A [`RefRow`] is a list of column value slices pointing to either an internal buffer or a
/// memory-mapped file.
type RefRow<'a> = Vec<&'a [u8]>;

/// Alias type for a column value wrapped in `Result`.
pub type ColumnResult<T> = Result<T, Box<dyn StdError + Send + Sync>>;

/// A trait for the user-defined header of [`NippyJar`].
pub trait NippyJarHeader:
    Send + Sync + Serialize + for<'b> Deserialize<'b> + std::fmt::Debug + 'static
{
}

// Blanket implementation for all types that implement the required traits.
impl<T> NippyJarHeader for T where
    T: Send + Sync + Serialize + for<'b> Deserialize<'b> + std::fmt::Debug + 'static
{
}

/// `NippyJar` is a specialized storage format designed for immutable data.
///
/// Data is organized into a columnar format, enabling column-based compression. Data retrieval
/// entails consulting an offset list and fetching the data from file via `mmap`.
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct NippyJar<H = ()> {
    /// The version of the `NippyJar` format.
    version: usize,
    /// User-defined header data.
    /// Default: zero-sized unit type: no header data
    user_header: H,
    /// Number of data columns in the jar.
    columns: usize,
    /// Number of data rows in the jar.
    rows: usize,
    /// Optional compression algorithm applied to the data.
    compressor: Option<Compressors>,
    #[serde(skip)]
    /// Optional field for backwards compatibility
    filter: Option<InclusionFilters>,
    #[serde(skip)]
    /// Optional field for backwards compatibility
    phf: Option<Functions>,
    /// Maximum uncompressed row size of the set. This will enable decompression without any
    /// resizing of the output buffer.
    max_row_size: usize,
    /// Data path for file. Supporting files will have a format `{path}.{extension}`.
    #[serde(skip)]
    path: PathBuf,
}

impl<H: NippyJarHeader> std::fmt::Debug for NippyJar<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NippyJar")
            .field("version", &self.version)
            .field("user_header", &self.user_header)
            .field("rows", &self.rows)
            .field("columns", &self.columns)
            .field("compressor", &self.compressor)
            .field("filter", &self.filter)
            .field("phf", &self.phf)
            .field("path", &self.path)
            .field("max_row_size", &self.max_row_size)
            .finish_non_exhaustive()
    }
}

impl NippyJar<()> {
    /// Creates a new [`NippyJar`] without an user-defined header data.
    pub fn new_without_header(columns: usize, path: &Path) -> Self {
        Self::new(columns, path, ())
    }

    /// Loads the file configuration and returns [`Self`] on a jar without user-defined header data.
    pub fn load_without_header(path: &Path) -> Result<Self, NippyJarError> {
        Self::load(path)
    }
}

impl<H: NippyJarHeader> NippyJar<H> {
    /// Creates a new [`NippyJar`] with a user-defined header data.
    pub fn new(columns: usize, path: &Path, user_header: H) -> Self {
        Self {
            version: NIPPY_JAR_VERSION,
            user_header,
            columns,
            rows: 0,
            max_row_size: 0,
            compressor: None,
            filter: None,
            phf: None,
            path: path.to_path_buf(),
        }
    }

    /// Adds [`compression::Zstd`] compression.
    pub fn with_zstd(mut self, use_dict: bool, max_dict_size: usize) -> Self {
        self.compressor =
            Some(Compressors::Zstd(compression::Zstd::new(use_dict, max_dict_size, self.columns)));
        self
    }

    /// Adds [`compression::Lz4`] compression.
    pub fn with_lz4(mut self) -> Self {
        self.compressor = Some(Compressors::Lz4(compression::Lz4::default()));
        self
    }

    /// Makes this a checksummed jar (format version 2): every value, the configuration and the
    /// changeset offsets sidecar carry a CRC32 that every read verifies.
    ///
    /// TN(TN-0120): only for a jar that has no data yet. A jar's own configuration decides how it
    /// is written and read, so an existing jar keeps its format.
    pub const fn with_checksums(mut self) -> Self {
        self.version = NIPPY_JAR_VERSION_CHECKSUMS;
        self
    }

    /// Whether the jar's values, configuration and changeset offsets carry checksums.
    pub const fn has_checksums(&self) -> bool {
        self.version == NIPPY_JAR_VERSION_CHECKSUMS
    }

    /// Gets a reference to the user header.
    pub const fn user_header(&self) -> &H {
        &self.user_header
    }

    /// Gets total columns in jar.
    pub const fn columns(&self) -> usize {
        self.columns
    }

    /// Gets total rows in jar.
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Gets a reference to the compressor.
    pub const fn compressor(&self) -> Option<&Compressors> {
        self.compressor.as_ref()
    }

    /// Gets a mutable reference to the compressor.
    pub const fn compressor_mut(&mut self) -> Option<&mut Compressors> {
        self.compressor.as_mut()
    }

    /// Loads the file configuration and returns [`Self`].
    ///
    /// **The user must ensure the header type matches the one used during the jar's creation.**
    pub fn load(path: &Path) -> Result<Self, NippyJarError> {
        // Read [`Self`] located at the data file.
        let config_path = path.with_extension(CONFIG_FILE_EXTENSION);
        let config_file = File::open(&config_path)
            .inspect_err(|e| {
                warn!(?path, %e, "Failed to load static file jar");
            })
            .map_err(|err| reth_fs_util::FsPathError::open(err, &config_path))?;

        // TN(TN-0120): the whole file, so a checksummed configuration is verified before it is
        // decoded.
        let mut bytes = Vec::new();
        io::BufReader::new(config_file).read_to_end(&mut bytes)?;
        let mut obj = Self::decode_config(&bytes, &config_path)?;
        obj.path = path.to_path_buf();
        Ok(obj)
    }

    /// Deserializes an instance of [`Self`] from a [`Read`] type.
    ///
    /// A [`NippyJarError::ConfigChecksumMismatch`] from here names an empty path.
    pub fn load_from_reader<R: Read>(mut reader: R) -> Result<Self, NippyJarError> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Self::decode_config(&bytes, Path::new(""))
    }

    /// Serializes an instance of [`Self`] to a [`Write`] type.
    pub fn save_to_writer<W: Write>(&self, mut writer: W) -> Result<(), NippyJarError> {
        if !self.has_checksums() {
            return Ok(bincode::serialize_into(writer, self)?)
        }

        // TN(TN-0120): a checksummed jar's configuration is followed by a CRC32 of its bytes.
        let mut bytes = bincode::serialize(self)?;
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Decodes a configuration file's bytes.
    ///
    /// TN(TN-0120): the first field is the version, a `u64` LE in bincode's encoding. A
    /// checksummed configuration's trailing CRC32 is verified before the rest is decoded, so a
    /// damaged length or row count is an error rather than a plausible configuration. A
    /// version-1 configuration decodes as upstream's does; any other version is refused.
    fn decode_config(bytes: &[u8], config_path: &Path) -> Result<Self, NippyJarError> {
        let version = bytes.get(..8).map(|v| u64::from_le_bytes(v.try_into().expect("8 bytes")));
        let body = if version == Some(NIPPY_JAR_VERSION_CHECKSUMS as u64) {
            let checksummed = bytes
                .len()
                .checked_sub(CHECKSUM_SIZE)
                .map(|len| bytes.split_at(len))
                .filter(|(body, stored)| crc32fast::hash(body).to_le_bytes() == **stored);
            let Some((body, _)) = checksummed else {
                return Err(NippyJarError::ConfigChecksumMismatch(config_path.to_path_buf()))
            };
            body
        } else {
            bytes
        };

        let jar: Self = bincode::deserialize(body)?;
        if jar.version != NIPPY_JAR_VERSION && jar.version != NIPPY_JAR_VERSION_CHECKSUMS {
            return Err(NippyJarError::UnsupportedVersion(jar.version))
        }
        // TN(TN-0120): the dictionary zstd path is not usable (the writer compresses without the
        // dictionaries the cursor decompresses with, and the cursor `expect`s them), so a
        // checksummed jar may not name it: refused here rather than panicking on a read.
        if jar.has_checksums() &&
            matches!(&jar.compressor, Some(Compressors::Zstd(zstd)) if zstd.use_dict)
        {
            return Err(NippyJarError::UnsupportedCompression)
        }
        Ok(jar)
    }

    /// Returns the path for the data file
    pub fn data_path(&self) -> &Path {
        self.path.as_ref()
    }

    /// Returns the path for the index file
    pub fn index_path(&self) -> PathBuf {
        self.path.with_extension(INDEX_FILE_EXTENSION)
    }

    /// Returns the path for the offsets file
    pub fn offsets_path(&self) -> PathBuf {
        self.path.with_extension(OFFSETS_FILE_EXTENSION)
    }

    /// Returns the path for the config file
    pub fn config_path(&self) -> PathBuf {
        self.path.with_extension(CONFIG_FILE_EXTENSION)
    }

    /// Returns the path for the changeset offsets sidecar file.
    pub fn changeset_offsets_path(&self) -> PathBuf {
        self.path.with_extension(CHANGESET_OFFSETS_FILE_EXTENSION)
    }

    /// Deletes from disk this [`NippyJar`] alongside every satellite file.
    ///
    /// TN(TN-0132): the data file goes first, and its removal is made durable before any
    /// satellite goes. Jars are found by their data file (`iter_static_files`), so a deletion
    /// stopped at any point leaves either the whole jar or files no listing sees, which
    /// [`NippyJarWriter`] empties when it creates a jar at that path again.
    pub fn delete(self) -> Result<(), NippyJarError> {
        let data_path = self.data_path();
        if data_path.exists() {
            debug!(target: "nippy-jar", path = ?data_path, "Removing file.");
            reth_fs_util::remove_file(data_path)?;
            if let Some(dir) = data_path.parent() {
                reth_fs_util::fsync_file(&File::open(dir)?)?;
            }
        }

        for path in [
            self.index_path(),
            self.offsets_path(),
            self.config_path(),
            self.changeset_offsets_path(),
        ] {
            if path.exists() {
                debug!(target: "nippy-jar", ?path, "Removing file.");
                reth_fs_util::remove_file(path)?;
            }
        }

        Ok(())
    }

    /// Returns a [`DataReader`] of the data and offset file
    pub fn open_data_reader(&self) -> Result<DataReader, NippyJarError> {
        DataReader::new(self.data_path())
    }

    /// Writes all necessary configuration to file.
    fn freeze_config(&self) -> Result<(), NippyJarError> {
        Ok(reth_fs_util::atomic_write_file(&self.config_path(), |file| self.save_to_writer(file))?)
    }
}

#[cfg(test)]
impl<H: NippyJarHeader> NippyJar<H> {
    /// If required, prepares any compression algorithm to an early pass of the data.
    pub fn prepare_compression(
        &mut self,
        columns: Vec<impl IntoIterator<Item = Vec<u8>>>,
    ) -> Result<(), NippyJarError> {
        // Makes any necessary preparations for the compressors
        if let Some(compression) = &mut self.compressor {
            debug!(target: "nippy-jar", columns=columns.len(), "Preparing compression.");
            compression.prepare_compression(columns)?;
        }
        Ok(())
    }

    /// Writes all data and configuration to a file and the offset index to another.
    pub fn freeze(
        self,
        columns: Vec<impl IntoIterator<Item = ColumnResult<Vec<u8>>>>,
        total_rows: u64,
    ) -> Result<Self, NippyJarError> {
        self.check_before_freeze(&columns)?;

        debug!(target: "nippy-jar", path=?self.data_path(), "Opening data file.");

        // Creates the writer, data and offsets file
        let mut writer = NippyJarWriter::new(self)?;

        // Append rows to file while holding offsets in memory
        writer.append_rows(columns, total_rows)?;

        // Flushes configuration and offsets to disk
        writer.commit()?;

        debug!(target: "nippy-jar", ?writer, "Finished writing data.");

        Ok(writer.into_jar())
    }

    /// Safety checks before creating and returning a [`File`] handle to write data to.
    fn check_before_freeze(
        &self,
        columns: &[impl IntoIterator<Item = ColumnResult<Vec<u8>>>],
    ) -> Result<(), NippyJarError> {
        if columns.len() != self.columns {
            return Err(NippyJarError::ColumnLenMismatch(self.columns, columns.len()))
        }

        if let Some(compression) = &self.compressor &&
            !compression.is_ready()
        {
            return Err(NippyJarError::CompressorNotReady)
        }

        Ok(())
    }
}

/// Manages the reading of static file data using memory-mapped files.
///
/// Holds file and mmap descriptors of the data and offsets files of a `static_file`.
#[derive(Debug)]
pub struct DataReader {
    /// Data file descriptor. Needs to be kept alive as long as `data_mmap` handle.
    #[expect(dead_code)]
    data_file: File,
    /// Mmap handle for data.
    data_mmap: Mmap,
    /// Offset file descriptor. Needs to be kept alive as long as `offset_mmap` handle.
    offset_file: File,
    /// Mmap handle for offsets.
    offset_mmap: Mmap,
    /// Number of bytes that represent one offset.
    offset_size: u8,
}

impl DataReader {
    /// Reads the respective data and offsets file and returns [`DataReader`].
    pub fn new(path: impl AsRef<Path>) -> Result<Self, NippyJarError> {
        let data_file = File::open(path.as_ref())?;
        // SAFETY: File is read-only and its descriptor is kept alive as long as the mmap handle.
        let data_mmap = unsafe { Mmap::map(&data_file)? };

        let offset_file = File::open(path.as_ref().with_extension(OFFSETS_FILE_EXTENSION))?;
        // SAFETY: File is read-only and its descriptor is kept alive as long as the mmap handle.
        let offset_mmap = unsafe { Mmap::map(&offset_file)? };

        // First byte is the size of one offset in bytes
        // TN(TN-0131): an empty offsets file (a crash right after a new jar's configuration was
        // written can leave one) is an error, not a panic on the empty map.
        let Some(&offset_size) = offset_mmap.first() else {
            return Err(NippyJarError::EmptyOffsetsFile(
                path.as_ref().with_extension(OFFSETS_FILE_EXTENSION),
            ))
        };

        // Ensure that the size of an offset is at most 8 bytes.
        if offset_size > 8 {
            return Err(NippyJarError::OffsetSizeTooBig { offset_size })
        } else if offset_size == 0 {
            return Err(NippyJarError::OffsetSizeTooSmall { offset_size })
        }

        Ok(Self { data_file, data_mmap, offset_file, offset_size, offset_mmap })
    }

    /// Returns the offset for the requested data index
    pub fn offset(&self, index: usize) -> Result<u64, NippyJarError> {
        // + 1 represents the offset_len u8 which is in the beginning of the file
        let from = index * self.offset_size as usize + 1;

        self.offset_at(from)
    }

    /// Returns the offset for the requested data index starting from the end
    pub fn reverse_offset(&self, index: usize) -> Result<u64, NippyJarError> {
        let offsets_file_size = self.offset_file.metadata()?.len() as usize;

        if offsets_file_size > 1 {
            let from = offsets_file_size - self.offset_size as usize * (index + 1);

            self.offset_at(from)
        } else {
            Ok(0)
        }
    }

    /// Returns total number of offsets in the file.
    /// The size of one offset is determined by the file itself.
    pub fn offsets_count(&self) -> Result<usize, NippyJarError> {
        Ok((self.offset_file.metadata()?.len().saturating_sub(1) / self.offset_size as u64)
            as usize)
    }

    /// Reads one offset-sized (determined by the offset file) u64 at the provided index.
    fn offset_at(&self, index: usize) -> Result<u64, NippyJarError> {
        let mut buffer: [u8; 8] = [0; 8];

        let offset_end = index.saturating_add(self.offset_size as usize);
        if offset_end > self.offset_mmap.len() {
            return Err(NippyJarError::OffsetOutOfBounds { index })
        }

        buffer[..self.offset_size as usize].copy_from_slice(&self.offset_mmap[index..offset_end]);
        Ok(u64::from_le_bytes(buffer))
    }

    /// Returns number of bytes that represent one offset.
    pub const fn offset_size(&self) -> u8 {
        self.offset_size
    }

    /// Returns the underlying data as a slice of bytes for the provided range.
    pub fn data(&self, range: Range<usize>) -> &[u8] {
        &self.data_mmap[range]
    }

    /// Returns total size of data file.
    pub fn size(&self) -> usize {
        self.data_mmap.len()
    }

    /// Returns total size of offsets file.
    pub fn offsets_size(&self) -> usize {
        self.offset_mmap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compression::Compression;
    use rand::{rngs::SmallRng, seq::SliceRandom, RngCore, SeedableRng};
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom},
    };

    type ColumnResults<T> = Vec<ColumnResult<T>>;
    type ColumnValues = Vec<Vec<u8>>;

    fn test_data(seed: Option<u64>) -> (ColumnValues, ColumnValues) {
        let value_length = 32;
        let num_rows = 100;

        let mut vec: Vec<u8> = vec![0; value_length];
        let mut rng = seed.map(SmallRng::seed_from_u64).unwrap_or_else(SmallRng::from_os_rng);

        let mut entry_gen = || {
            (0..num_rows)
                .map(|_| {
                    rng.fill_bytes(&mut vec[..]);
                    vec.clone()
                })
                .collect()
        };

        (entry_gen(), entry_gen())
    }

    fn clone_with_result(col: &ColumnValues) -> ColumnResults<Vec<u8>> {
        col.iter().map(|v| Ok(v.clone())).collect()
    }

    #[test]
    fn test_config_serialization() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let jar = NippyJar::new_without_header(23, file.path()).with_lz4();
        jar.freeze_config().unwrap();

        let mut config_file = OpenOptions::new().read(true).open(jar.config_path()).unwrap();
        let config_file_len = config_file.metadata().unwrap().len();
        assert_eq!(config_file_len, 37);

        let mut buf = Vec::with_capacity(config_file_len as usize);
        config_file.read_to_end(&mut buf).unwrap();

        assert_eq!(
            vec![
                1, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ],
            buf
        );

        let mut read_jar = bincode::deserialize_from::<_, NippyJar>(&buf[..]).unwrap();
        // Path is not ser/de
        read_jar.path = file.path().to_path_buf();
        assert_eq!(jar, read_jar);
    }

    #[test]
    fn test_zstd_with_dictionaries() {
        let (col1, col2) = test_data(None);
        let num_rows = col1.len() as u64;
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        let nippy = NippyJar::new_without_header(num_columns, file_path.path());
        assert!(nippy.compressor().is_none());

        let mut nippy =
            NippyJar::new_without_header(num_columns, file_path.path()).with_zstd(true, 5000);
        assert!(nippy.compressor().is_some());

        if let Some(Compressors::Zstd(zstd)) = &mut nippy.compressor_mut() {
            assert!(matches!(zstd.compressors(), Err(NippyJarError::CompressorNotReady)));

            // Make sure the number of column iterators match the initial set up ones.
            assert!(matches!(
                zstd.prepare_compression(vec![col1.clone(), col2.clone(), col2.clone()]),
                Err(NippyJarError::ColumnLenMismatch(columns, 3)) if columns == num_columns
            ));
        }

        // If ZSTD is enabled, do not write to the file unless the column dictionaries have been
        // calculated.
        assert!(matches!(
            nippy.freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], num_rows),
            Err(NippyJarError::CompressorNotReady)
        ));

        let mut nippy =
            NippyJar::new_without_header(num_columns, file_path.path()).with_zstd(true, 5000);
        assert!(nippy.compressor().is_some());

        nippy.prepare_compression(vec![col1.clone(), col2.clone()]).unwrap();

        if let Some(Compressors::Zstd(zstd)) = &nippy.compressor() {
            assert!(matches!(
                (&zstd.state, zstd.dictionaries.as_ref().map(|dict| dict.len())),
                (compression::ZstdState::Ready, Some(columns)) if columns == num_columns
            ));
        }

        let nippy = nippy
            .freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], num_rows)
            .unwrap();

        let loaded_nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        assert_eq!(nippy.version, loaded_nippy.version);
        assert_eq!(nippy.columns, loaded_nippy.columns);
        assert_eq!(nippy.filter, loaded_nippy.filter);
        assert_eq!(nippy.phf, loaded_nippy.phf);
        assert_eq!(nippy.max_row_size, loaded_nippy.max_row_size);
        assert_eq!(nippy.path, loaded_nippy.path);

        if let Some(Compressors::Zstd(zstd)) = loaded_nippy.compressor() {
            assert!(zstd.use_dict);
            let mut cursor = NippyJarCursor::new(&loaded_nippy).unwrap();

            // Iterate over compressed values and compare
            let mut row_index = 0usize;
            while let Some(row) = cursor.next_row().unwrap() {
                assert_eq!(
                    (row[0], row[1]),
                    (col1[row_index].as_slice(), col2[row_index].as_slice())
                );
                row_index += 1;
            }
        } else {
            panic!("Expected Zstd compressor")
        }
    }

    #[test]
    fn test_lz4() {
        let (col1, col2) = test_data(None);
        let num_rows = col1.len() as u64;
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        let nippy = NippyJar::new_without_header(num_columns, file_path.path());
        assert!(nippy.compressor().is_none());

        let nippy = NippyJar::new_without_header(num_columns, file_path.path()).with_lz4();
        assert!(nippy.compressor().is_some());

        let nippy = nippy
            .freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], num_rows)
            .unwrap();

        let loaded_nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        assert_eq!(nippy, loaded_nippy);

        if let Some(Compressors::Lz4(_)) = loaded_nippy.compressor() {
            let mut cursor = NippyJarCursor::new(&loaded_nippy).unwrap();

            // Iterate over compressed values and compare
            let mut row_index = 0usize;
            while let Some(row) = cursor.next_row().unwrap() {
                assert_eq!(
                    (row[0], row[1]),
                    (col1[row_index].as_slice(), col2[row_index].as_slice())
                );
                row_index += 1;
            }
        } else {
            panic!("Expected Lz4 compressor")
        }
    }

    #[test]
    fn test_zstd_no_dictionaries() {
        let (col1, col2) = test_data(None);
        let num_rows = col1.len() as u64;
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        let nippy = NippyJar::new_without_header(num_columns, file_path.path());
        assert!(nippy.compressor().is_none());

        let nippy =
            NippyJar::new_without_header(num_columns, file_path.path()).with_zstd(false, 5000);
        assert!(nippy.compressor().is_some());

        let nippy = nippy
            .freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], num_rows)
            .unwrap();

        let loaded_nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        assert_eq!(nippy, loaded_nippy);

        if let Some(Compressors::Zstd(zstd)) = loaded_nippy.compressor() {
            assert!(!zstd.use_dict);

            let mut cursor = NippyJarCursor::new(&loaded_nippy).unwrap();

            // Iterate over compressed values and compare
            let mut row_index = 0usize;
            while let Some(row) = cursor.next_row().unwrap() {
                assert_eq!(
                    (row[0], row[1]),
                    (col1[row_index].as_slice(), col2[row_index].as_slice())
                );
                row_index += 1;
            }
        } else {
            panic!("Expected Zstd compressor")
        }
    }

    /// Tests `NippyJar` with everything enabled.
    #[test]
    fn test_full_nippy_jar() {
        let (col1, col2) = test_data(None);
        let num_rows = col1.len() as u64;
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();
        let data = vec![col1.clone(), col2.clone()];

        let block_start = 500;

        #[derive(Serialize, Deserialize, Debug)]
        struct BlockJarHeader {
            block_start: usize,
        }

        // Create file
        {
            let mut nippy =
                NippyJar::new(num_columns, file_path.path(), BlockJarHeader { block_start })
                    .with_zstd(true, 5000);

            nippy.prepare_compression(data.clone()).unwrap();
            nippy
                .freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], num_rows)
                .unwrap();
        }

        // Read file
        {
            let loaded_nippy = NippyJar::<BlockJarHeader>::load(file_path.path()).unwrap();

            assert!(loaded_nippy.compressor().is_some());
            assert_eq!(loaded_nippy.user_header().block_start, block_start);

            if let Some(Compressors::Zstd(_zstd)) = loaded_nippy.compressor() {
                let mut cursor = NippyJarCursor::new(&loaded_nippy).unwrap();

                // Iterate over compressed values and compare
                let mut row_num = 0usize;
                while let Some(row) = cursor.next_row().unwrap() {
                    assert_eq!(
                        (row[0], row[1]),
                        (data[0][row_num].as_slice(), data[1][row_num].as_slice())
                    );
                    row_num += 1;
                }

                // Shuffled for chaos.
                let mut data = col1.iter().zip(col2.iter()).enumerate().collect::<Vec<_>>();
                data.shuffle(&mut rand::rng());

                for (row_num, (v0, v1)) in data {
                    // Simulates `by_number` queries
                    let row_by_num = cursor.row_by_number(row_num).unwrap().unwrap();
                    assert_eq!((&row_by_num[0].to_vec(), &row_by_num[1].to_vec()), (v0, v1));
                }
            }
        }
    }

    #[test]
    fn test_selectable_column_values() {
        let (col1, col2) = test_data(None);
        let num_rows = col1.len() as u64;
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();
        let data = vec![col1.clone(), col2.clone()];

        // Create file
        {
            let mut nippy =
                NippyJar::new_without_header(num_columns, file_path.path()).with_zstd(true, 5000);
            nippy.prepare_compression(data).unwrap();
            nippy
                .freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], num_rows)
                .unwrap();
        }

        // Read file
        {
            let loaded_nippy = NippyJar::load_without_header(file_path.path()).unwrap();

            if let Some(Compressors::Zstd(_zstd)) = loaded_nippy.compressor() {
                let mut cursor = NippyJarCursor::new(&loaded_nippy).unwrap();

                // Shuffled for chaos.
                let mut data = col1.iter().zip(col2.iter()).enumerate().collect::<Vec<_>>();
                data.shuffle(&mut rand::rng());

                // Imagine `Blocks` static file has two columns: `Block | StoredWithdrawals`
                const BLOCKS_FULL_MASK: usize = 0b11;

                // Read both columns
                for (row_num, (v0, v1)) in &data {
                    // Simulates `by_number` queries
                    let row_by_num = cursor
                        .row_by_number_with_cols(*row_num, BLOCKS_FULL_MASK)
                        .unwrap()
                        .unwrap();
                    assert_eq!((&row_by_num[0].to_vec(), &row_by_num[1].to_vec()), (*v0, *v1));
                }

                // Read first column only: `Block`
                const BLOCKS_BLOCK_MASK: usize = 0b01;
                for (row_num, (v0, _)) in &data {
                    // Simulates `by_number` queries
                    let row_by_num = cursor
                        .row_by_number_with_cols(*row_num, BLOCKS_BLOCK_MASK)
                        .unwrap()
                        .unwrap();
                    assert_eq!(row_by_num.len(), 1);
                    assert_eq!(&row_by_num[0].to_vec(), *v0);
                }

                // Read second column only: `Block`
                const BLOCKS_WITHDRAWAL_MASK: usize = 0b10;
                for (row_num, (_, v1)) in &data {
                    // Simulates `by_number` queries
                    let row_by_num = cursor
                        .row_by_number_with_cols(*row_num, BLOCKS_WITHDRAWAL_MASK)
                        .unwrap()
                        .unwrap();
                    assert_eq!(row_by_num.len(), 1);
                    assert_eq!(&row_by_num[0].to_vec(), *v1);
                }

                // Read nothing
                const BLOCKS_EMPTY_MASK: usize = 0b00;
                for (row_num, _) in &data {
                    // Simulates `by_number` queries
                    assert!(cursor
                        .row_by_number_with_cols(*row_num, BLOCKS_EMPTY_MASK)
                        .unwrap()
                        .unwrap()
                        .is_empty());
                }
            }
        }
    }

    #[test]
    fn test_writer() {
        let (col1, col2) = test_data(None);
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        append_two_rows(num_columns, file_path.path(), &col1, &col2);

        // Appends a third row and prunes two rows, to make sure we prune from memory and disk
        // offset list
        prune_rows(num_columns, file_path.path(), &col1, &col2);

        // Should be able to append new rows
        append_two_rows(num_columns, file_path.path(), &col1, &col2);

        // Simulate an unexpected shutdown before there's a chance to commit, and see that it
        // unwinds successfully
        test_append_consistency_no_commit(file_path.path(), &col1, &col2);

        // Simulate an unexpected shutdown during commit, and see that it unwinds successfully
        test_append_consistency_partial_commit(file_path.path(), &col1, &col2);
    }

    #[test]
    fn test_pruner() {
        let (col1, col2) = test_data(None);
        let num_columns = 2;
        let num_rows = 2;

        // (missing_offsets, expected number of rows)
        // If a row wasn't fully pruned, then it should clear it up as well
        let missing_offsets_scenarios = [(1, 1), (2, 1), (3, 0)];

        for (missing_offsets, expected_rows) in missing_offsets_scenarios {
            let file_path = tempfile::NamedTempFile::new().unwrap();

            append_two_rows(num_columns, file_path.path(), &col1, &col2);

            simulate_interrupted_prune(num_columns, file_path.path(), num_rows, missing_offsets);

            let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
            assert_eq!(nippy.rows, expected_rows);
        }
    }

    #[test]
    fn test_read_last_row_while_appending() {
        let (col1, col2) = test_data(None);
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        append_two_rows(num_columns, file_path.path(), &col1, &col2);

        // Appends a third row to the data file, without committing its offsets and the row count.
        let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        let mut writer = NippyJarWriter::new(nippy).unwrap();
        writer.append_column(Some(Ok(&col1[2]))).unwrap();
        writer.append_column(Some(Ok(&col2[2]))).unwrap();
        writer.data_file().flush().unwrap();

        // A jar loaded meanwhile reads the last committed row as it was written.
        let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        assert_eq!(nippy.rows, 2);
        let mut cursor = NippyJarCursor::new(&nippy).unwrap();
        assert_eq!(
            cursor.row_by_number(1).unwrap().unwrap().as_slice(),
            [col1[1].as_slice(), col2[1].as_slice()]
        );
        assert!(cursor.row_by_number(2).unwrap().is_none());
    }

    #[test]
    fn test_read_last_row_with_invalid_final_offset() {
        let (col1, col2) = test_data(None);
        let num_columns = 2;

        // Overwrites the final offset of a committed jar, then reads its last row.
        let assert_inconsistent = |final_offset: fn(u64) -> u64| {
            let file_path = tempfile::NamedTempFile::new().unwrap();
            append_two_rows(num_columns, file_path.path(), &col1, &col2);

            let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
            let data_len = File::open(nippy.data_path()).unwrap().metadata().unwrap().len();
            let mut offsets =
                OpenOptions::new().read(true).write(true).open(nippy.offsets_path()).unwrap();
            offsets.seek(SeekFrom::End(-8)).unwrap();
            offsets.write_all(&final_offset(data_len).to_le_bytes()).unwrap();
            offsets.sync_all().unwrap();

            let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
            let mut cursor = NippyJarCursor::new(&nippy).unwrap();
            assert!(matches!(cursor.row_by_number(1), Err(NippyJarError::InconsistentState)));
        };

        // Before the start of the last value.
        assert_inconsistent(|_| 0);
        // Past the end of the data file.
        assert_inconsistent(|data_len| data_len + 1);
    }

    #[test]
    fn test_prune_partially_uncommitted_rows() {
        let (col1, col2) = test_data(None);
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        // Two committed rows.
        append_two_rows(num_columns, file_path.path(), &col1, &col2);

        let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        let mut writer = NippyJarWriter::new(nippy).unwrap();

        // Two uncommitted rows, of which only one is pruned again.
        append_row(&mut writer, &col1, &col2, 2);
        append_row(&mut writer, &col1, &col2, 3);
        writer.prune_rows(1).unwrap();
        assert_eq!(writer.rows(), 3);

        // The config on disk must never claim rows whose offsets are not on disk.
        let expected_offsets_len = 1 + 3 * num_columns as u64 * 8 + 8;
        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
            expected_offsets_len
        );
        let expected_data_len = col1[..3].iter().chain(&col2[..3]).map(Vec::len).sum::<usize>();
        assert_eq!(
            File::open(writer.data_path()).unwrap().metadata().unwrap().len() as usize,
            expected_data_len
        );
        assert_jar_rows(file_path.path(), &col1, &col2, 3);

        // Committing must not change anything, and the writer remains usable.
        writer.commit().unwrap();
        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
            expected_offsets_len
        );
        assert_jar_rows(file_path.path(), &col1, &col2, 3);

        append_row(&mut writer, &col1, &col2, 3);
        writer.commit().unwrap();
        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
            1 + 4 * num_columns as u64 * 8 + 8
        );
        assert_jar_rows(file_path.path(), &col1, &col2, 4);
    }

    #[test]
    fn test_prune_uncommitted_and_committed_rows() {
        let (col1, col2) = test_data(None);
        let num_columns = 2;
        let file_path = tempfile::NamedTempFile::new().unwrap();

        // Two committed rows.
        append_two_rows(num_columns, file_path.path(), &col1, &col2);

        let nippy = NippyJar::load_without_header(file_path.path()).unwrap();
        let mut writer = NippyJarWriter::new(nippy).unwrap();

        // One uncommitted row, which is pruned together with one committed row.
        append_row(&mut writer, &col1, &col2, 2);
        writer.prune_rows(2).unwrap();
        assert_eq!(writer.rows(), 1);

        // Committing must not write the stale in-memory offset of the pruned rows.
        writer.commit().unwrap();
        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
            1 + num_columns as u64 * 8 + 8
        );
        assert_eq!(
            File::open(writer.data_path()).unwrap().metadata().unwrap().len() as usize,
            col1[0].len() + col2[0].len()
        );
        assert_jar_rows(file_path.path(), &col1, &col2, 1);

        // Rows appended through the same writer afterwards read back correctly.
        append_row(&mut writer, &col1, &col2, 1);
        writer.commit().unwrap();
        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
            1 + 2 * num_columns as u64 * 8 + 8
        );
        assert_jar_rows(file_path.path(), &col1, &col2, 2);
    }

    fn test_append_consistency_partial_commit(
        file_path: &Path,
        col1: &[Vec<u8>],
        col2: &[Vec<u8>],
    ) {
        let nippy = NippyJar::load_without_header(file_path).unwrap();

        // Set the baseline that should be unwinded to
        let initial_rows = nippy.rows;
        let initial_data_size =
            File::open(nippy.data_path()).unwrap().metadata().unwrap().len() as usize;
        let initial_offset_size =
            File::open(nippy.offsets_path()).unwrap().metadata().unwrap().len() as usize;
        assert!(initial_data_size > 0);
        assert!(initial_offset_size > 0);

        // Appends a third row
        let mut writer = NippyJarWriter::new(nippy).unwrap();
        writer.append_column(Some(Ok(&col1[2]))).unwrap();
        writer.append_column(Some(Ok(&col2[2]))).unwrap();

        // Makes sure it doesn't write the last one offset (which is the expected file data size)
        let _ = writer.offsets_mut().pop();

        // `commit_offsets` is not a pub function. we call it here to simulate the shutdown before
        // it can flush nippy.rows (config) to disk.
        writer.commit_offsets().unwrap();

        // Simulate an unexpected shutdown of the writer, before it can finish commit()
        drop(writer);

        let nippy = NippyJar::load_without_header(file_path).unwrap();
        assert_eq!(initial_rows, nippy.rows);

        // Data was written successfully
        let new_data_size =
            File::open(nippy.data_path()).unwrap().metadata().unwrap().len() as usize;
        assert_eq!(new_data_size, initial_data_size + col1[2].len() + col2[2].len());

        // It should be + 16 (two columns were added), but there's a missing one (the one we pop)
        assert_eq!(
            initial_offset_size + 8,
            File::open(nippy.offsets_path()).unwrap().metadata().unwrap().len() as usize
        );

        // Writer will execute a consistency check and verify first that the offset list on disk
        // doesn't match the nippy.rows, and prune it. Then, it will prune the data file
        // accordingly as well.
        let writer = NippyJarWriter::new(nippy).unwrap();
        assert_eq!(initial_rows, writer.rows());
        assert_eq!(
            initial_offset_size,
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len() as usize
        );
        assert_eq!(
            initial_data_size,
            File::open(writer.data_path()).unwrap().metadata().unwrap().len() as usize
        );
    }

    fn test_append_consistency_no_commit(file_path: &Path, col1: &[Vec<u8>], col2: &[Vec<u8>]) {
        let nippy = NippyJar::load_without_header(file_path).unwrap();

        // Set the baseline that should be unwinded to
        let initial_rows = nippy.rows;
        let initial_data_size =
            File::open(nippy.data_path()).unwrap().metadata().unwrap().len() as usize;
        let initial_offset_size =
            File::open(nippy.offsets_path()).unwrap().metadata().unwrap().len() as usize;
        assert!(initial_data_size > 0);
        assert!(initial_offset_size > 0);

        // Appends a third row, so we have an offset list in memory, which is not flushed to disk,
        // while the data has been.
        let mut writer = NippyJarWriter::new(nippy).unwrap();
        writer.append_column(Some(Ok(&col1[2]))).unwrap();
        writer.append_column(Some(Ok(&col2[2]))).unwrap();

        // Simulate an unexpected shutdown of the writer, before it can call commit()
        drop(writer);

        let nippy = NippyJar::load_without_header(file_path).unwrap();
        assert_eq!(initial_rows, nippy.rows);

        // Data was written successfully
        let new_data_size =
            File::open(nippy.data_path()).unwrap().metadata().unwrap().len() as usize;
        assert_eq!(new_data_size, initial_data_size + col1[2].len() + col2[2].len());

        // Since offsets only get written on commit(), this remains the same
        assert_eq!(
            initial_offset_size,
            File::open(nippy.offsets_path()).unwrap().metadata().unwrap().len() as usize
        );

        // Writer will execute a consistency check and verify that the data file has more data than
        // it should, and resets it to the last offset of the list (on disk here)
        let writer = NippyJarWriter::new(nippy).unwrap();
        assert_eq!(initial_rows, writer.rows());
        assert_eq!(
            initial_data_size,
            File::open(writer.data_path()).unwrap().metadata().unwrap().len() as usize
        );
    }

    fn append_two_rows(num_columns: usize, file_path: &Path, col1: &[Vec<u8>], col2: &[Vec<u8>]) {
        // Create and add 1 row
        {
            let nippy = NippyJar::new_without_header(num_columns, file_path);
            nippy.freeze_config().unwrap();
            assert_eq!(nippy.max_row_size, 0);
            assert_eq!(nippy.rows, 0);

            let mut writer = NippyJarWriter::new(nippy).unwrap();
            assert_eq!(writer.column(), 0);

            writer.append_column(Some(Ok(&col1[0]))).unwrap();
            assert_eq!(writer.column(), 1);
            assert!(writer.is_dirty());

            writer.append_column(Some(Ok(&col2[0]))).unwrap();
            assert!(writer.is_dirty());

            // Adding last column of a row resets writer and updates jar config
            assert_eq!(writer.column(), 0);

            // One offset per column + 1 offset at the end representing the expected file data size
            assert_eq!(writer.offsets().len(), 3);
            let expected_data_file_size = *writer.offsets().last().unwrap();
            writer.commit().unwrap();
            assert!(!writer.is_dirty());

            assert_eq!(writer.max_row_size(), col1[0].len() + col2[0].len());
            assert_eq!(writer.rows(), 1);
            assert_eq!(
                File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
                1 + num_columns as u64 * 8 + 8
            );
            assert_eq!(
                File::open(writer.data_path()).unwrap().metadata().unwrap().len(),
                expected_data_file_size
            );
        }

        // Load and add 1 row
        {
            let nippy = NippyJar::load_without_header(file_path).unwrap();
            // Check if it was committed successfully
            assert_eq!(nippy.max_row_size, col1[0].len() + col2[0].len());
            assert_eq!(nippy.rows, 1);

            let mut writer = NippyJarWriter::new(nippy).unwrap();
            assert_eq!(writer.column(), 0);

            writer.append_column(Some(Ok(&col1[1]))).unwrap();
            assert_eq!(writer.column(), 1);

            writer.append_column(Some(Ok(&col2[1]))).unwrap();

            // Adding last column of a row resets writer and updates jar config
            assert_eq!(writer.column(), 0);

            // One offset per column + 1 offset at the end representing the expected file data size
            assert_eq!(writer.offsets().len(), 3);
            let expected_data_file_size = *writer.offsets().last().unwrap();
            writer.commit().unwrap();

            assert_eq!(writer.max_row_size(), col1[0].len() + col2[0].len());
            assert_eq!(writer.rows(), 2);
            assert_eq!(
                File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
                1 + writer.rows() as u64 * num_columns as u64 * 8 + 8
            );
            assert_eq!(
                File::open(writer.data_path()).unwrap().metadata().unwrap().len(),
                expected_data_file_size
            );
        }
    }

    fn prune_rows(num_columns: usize, file_path: &Path, col1: &[Vec<u8>], col2: &[Vec<u8>]) {
        let nippy = NippyJar::load_without_header(file_path).unwrap();
        let mut writer = NippyJarWriter::new(nippy).unwrap();

        // Appends a third row, so we have an offset list in memory, which is not flushed to disk
        writer.append_column(Some(Ok(&col1[2]))).unwrap();
        writer.append_column(Some(Ok(&col2[2]))).unwrap();
        assert!(writer.is_dirty());

        // This should prune from the on-memory offset list and ondisk offset list
        writer.prune_rows(2).unwrap();
        assert_eq!(writer.rows(), 1);

        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len(),
            1 + writer.rows() as u64 * num_columns as u64 * 8 + 8
        );

        let expected_data_size = col1[0].len() + col2[0].len();
        assert_eq!(
            File::open(writer.data_path()).unwrap().metadata().unwrap().len() as usize,
            expected_data_size
        );

        let nippy = NippyJar::load_without_header(file_path).unwrap();
        {
            let data_reader = nippy.open_data_reader().unwrap();
            // there are only two valid offsets. so index 2 actually represents the expected file
            // data size.
            assert_eq!(data_reader.offset(2).unwrap(), expected_data_size as u64);
        }

        // This should prune from the ondisk offset list and clear the jar.
        let mut writer = NippyJarWriter::new(nippy).unwrap();
        writer.prune_rows(1).unwrap();
        assert!(writer.is_dirty());

        assert_eq!(writer.rows(), 0);
        assert_eq!(writer.max_row_size(), 0);
        assert_eq!(File::open(writer.data_path()).unwrap().metadata().unwrap().len() as usize, 0);
        // Offset size byte (1) + final offset (8) = 9 bytes
        assert_eq!(
            File::open(writer.offsets_path()).unwrap().metadata().unwrap().len() as usize,
            9
        );
        writer.commit().unwrap();
        assert!(!writer.is_dirty());
    }

    fn simulate_interrupted_prune(
        num_columns: usize,
        file_path: &Path,
        num_rows: u64,
        missing_offsets: u64,
    ) {
        let nippy = NippyJar::load_without_header(file_path).unwrap();
        let reader = nippy.open_data_reader().unwrap();
        let offsets_file =
            OpenOptions::new().read(true).write(true).open(nippy.offsets_path()).unwrap();
        let offsets_len = 1 + num_rows * num_columns as u64 * 8 + 8;
        assert_eq!(offsets_len, offsets_file.metadata().unwrap().len());

        let data_file = OpenOptions::new().read(true).write(true).open(nippy.data_path()).unwrap();
        let data_len = reader.reverse_offset(0).unwrap();
        assert_eq!(data_len, data_file.metadata().unwrap().len());

        // each data column is 32 bytes long
        // by deleting from the data file, the `consistency_check` will go through both branches:
        //      when the offset list wasn't updated after clearing the data (data_len > last
        // offset).      fixing above, will lead to offset count not match the rows (*
        // columns) of the configuration file
        data_file.set_len(data_len - 32 * missing_offsets).unwrap();

        // runs the consistency check.
        let _ = NippyJarWriter::new(nippy).unwrap();
    }

    /// Freezes `col1` and `col2` into a checksummed jar at `path`, LZ4-compressed if `lz4`.
    fn freeze_checksummed(path: &Path, col1: &ColumnValues, col2: &ColumnValues, lz4: bool) {
        let mut nippy = NippyJar::new_without_header(2, path).with_checksums();
        if lz4 {
            nippy = nippy.with_lz4();
        }
        nippy
            .freeze(vec![clone_with_result(col1), clone_with_result(col2)], col1.len() as u64)
            .unwrap();
    }

    /// Reads every row of the jar at `path` with every column, and with each column alone.
    fn read_all_rows(path: &Path) -> Vec<Result<(), NippyJarError>> {
        let nippy = NippyJar::load_without_header(path).unwrap();
        let mut cursor = NippyJarCursor::new(&nippy).unwrap();
        (0..nippy.rows)
            .map(|row| {
                cursor.row_by_number(row)?.expect("row exists");
                cursor.row_by_number_with_cols(row, 0b01)?.expect("row exists");
                cursor.row_by_number_with_cols(row, 0b10)?.expect("row exists");
                Ok(())
            })
            .collect()
    }

    /// XORs `mask` into the byte at `pos` of `path`.
    fn flip_byte(path: &Path, pos: u64, mask: u8) {
        let mut file = OpenOptions::new().read(true).write(true).open(path).unwrap();
        let mut byte = [0u8];
        file.seek(SeekFrom::Start(pos)).unwrap();
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(pos)).unwrap();
        file.write_all(&[byte[0] ^ mask]).unwrap();
        file.sync_all().unwrap();
    }

    /// Overwrites the offset at `index` (row * columns + column) of the jar at `path`.
    fn write_offset(path: &Path, index: usize, offset: u64) {
        let nippy = NippyJar::load_without_header(path).unwrap();
        let mut file = OpenOptions::new().write(true).open(nippy.offsets_path()).unwrap();
        file.seek(SeekFrom::Start(1 + 8 * index as u64)).unwrap();
        file.write_all(&offset.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    /// TN(TN-0120): a checksummed jar reads back exactly what was written, plain and compressed,
    /// whole rows and single columns, and stores four more bytes per value than an unchecked jar
    /// whose files keep upstream's layout.
    #[test]
    fn test_checksummed_jar_round_trip() {
        let (col1, col2) = test_data(Some(7));
        let values_len = col1.iter().chain(&col2).map(Vec::len).sum::<usize>() as u64;
        let values = (col1.len() * 2) as u64;

        for lz4 in [false, true] {
            let file = tempfile::NamedTempFile::new().unwrap();
            freeze_checksummed(file.path(), &col1, &col2, lz4);

            let nippy = NippyJar::load_without_header(file.path()).unwrap();
            assert!(nippy.has_checksums());
            assert_eq!(nippy.version, NIPPY_JAR_VERSION_CHECKSUMS);
            assert_eq!(nippy.max_row_size, col1[0].len() + col2[0].len());
            let mut cursor = NippyJarCursor::new(&nippy).unwrap();
            for row in 0..col1.len() {
                let values = cursor.row_by_number(row).unwrap().unwrap();
                assert_eq!((values[0], values[1]), (col1[row].as_slice(), col2[row].as_slice()));
                let second = cursor.row_by_number_with_cols(row, 0b10).unwrap().unwrap();
                assert_eq!(second, [col2[row].as_slice()]);
            }
            assert!(cursor.row_by_number(col1.len()).unwrap().is_none());

            let data_len = std::fs::metadata(file.path()).unwrap().len();
            if !lz4 {
                assert_eq!(data_len, values_len + CHECKSUM_SIZE as u64 * values);
            }
        }

        // An unchecked jar: upstream's version, and the data file is the values alone.
        let file = tempfile::NamedTempFile::new().unwrap();
        NippyJar::new_without_header(2, file.path())
            .freeze(vec![clone_with_result(&col1), clone_with_result(&col2)], col1.len() as u64)
            .unwrap();
        let nippy = NippyJar::load_without_header(file.path()).unwrap();
        assert!(!nippy.has_checksums());
        assert_eq!(nippy.version, NIPPY_JAR_VERSION);
        assert_eq!(std::fs::metadata(file.path()).unwrap().len(), values_len);
        assert_eq!(std::fs::metadata(nippy.offsets_path()).unwrap().len(), 1 + 8 * (values + 1));
    }

    /// TN(TN-0120): the configuration of a checksummed jar is upstream's encoding with version 2,
    /// followed by a CRC32 of it.
    #[test]
    fn test_checksummed_config_serialization() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let jar = NippyJar::new_without_header(23, file.path()).with_lz4().with_checksums();
        jar.freeze_config().unwrap();

        let buf = std::fs::read(jar.config_path()).unwrap();
        let body = [
            2, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(buf[..37], body);
        assert_eq!(buf[37..], crc32fast::hash(&body).to_le_bytes());

        let mut read_jar = NippyJar::load_from_reader(&buf[..]).unwrap();
        read_jar.path = file.path().to_path_buf();
        assert_eq!(jar, read_jar);
    }

    /// TN(TN-0120): any byte of a checksummed configuration that changes, or a configuration cut
    /// short, fails to load; so does an unknown format version.
    #[test]
    fn test_checksummed_config_damage_is_refused() {
        let (col1, col2) = test_data(Some(8));
        let file = tempfile::NamedTempFile::new().unwrap();
        freeze_checksummed(file.path(), &col1, &col2, true);
        let config_path = file.path().with_extension(CONFIG_FILE_EXTENSION);
        let config = std::fs::read(&config_path).unwrap();

        for pos in 0..config.len() {
            let mut damaged = config.clone();
            damaged[pos] ^= 0x10;
            std::fs::write(&config_path, &damaged).unwrap();
            let err = NippyJar::load_without_header(file.path()).unwrap_err();
            if pos < 8 {
                // The version itself, now one this build does not read.
                let version = 2 ^ (0x10 << (8 * pos));
                assert!(
                    matches!(err, NippyJarError::UnsupportedVersion(v) if v == version),
                    "{pos}: {err}"
                );
            } else {
                assert!(
                    matches!(&err, NippyJarError::ConfigChecksumMismatch(path) if *path == config_path),
                    "{pos}: {err}"
                );
            }
        }

        std::fs::write(&config_path, &config[..config.len() - 1]).unwrap();
        assert!(matches!(
            NippyJar::load_without_header(file.path()),
            Err(NippyJarError::ConfigChecksumMismatch(_))
        ));

        // An unchecked configuration with an unknown version.
        let mut unchecked = Vec::new();
        NippyJar::new_without_header(2, file.path()).save_to_writer(&mut unchecked).unwrap();
        unchecked[0] = 3;
        assert!(matches!(
            NippyJar::<()>::load_from_reader(&unchecked[..]),
            Err(NippyJarError::UnsupportedVersion(3))
        ));

        std::fs::write(&config_path, &config).unwrap();
        assert!(read_all_rows(file.path()).iter().all(Result::is_ok));

        // A checksummed configuration naming dictionary zstd, with a valid checksum: refused at
        // load, before a read could reach the cursor's dictionary `expect`s. Without dictionaries
        // zstd still loads.
        for use_dict in [true, false] {
            let mut bytes = Vec::new();
            NippyJar::new_without_header(2, file.path())
                .with_zstd(use_dict, 5000)
                .with_checksums()
                .save_to_writer(&mut bytes)
                .unwrap();
            let loaded = NippyJar::<()>::load_from_reader(&bytes[..]);
            assert_eq!(matches!(loaded, Err(NippyJarError::UnsupportedCompression)), use_dict);
            assert_eq!(loaded.is_ok(), !use_dict);
        }
    }

    /// TN(TN-0120): a changed byte anywhere in a value or its checksum fails that value's read
    /// with an error naming the jar, row and column, plain or compressed (before the
    /// decompressor sees it), while every other value still reads.
    #[test]
    fn test_checksum_detects_damaged_value() {
        let (col1, col2) = test_data(Some(9));
        for lz4 in [false, true] {
            let file = tempfile::NamedTempFile::new().unwrap();
            freeze_checksummed(file.path(), &col1, &col2, lz4);
            let nippy = NippyJar::load_without_header(file.path()).unwrap();
            let reader = nippy.open_data_reader().unwrap();

            let (row, column) = (37, 1);
            let start = reader.offset(row * 2 + column).unwrap();
            let end = reader.offset(row * 2 + column + 1).unwrap();
            drop(reader);

            // The first, a middle and the last stored byte, and a byte of the checksum.
            for pos in [start, (start + end) / 2, end - 5, end - 1] {
                flip_byte(file.path(), pos, 0x01);
                let results = read_all_rows(file.path());
                for (r, result) in results.iter().enumerate() {
                    if r == row {
                        assert!(
                            matches!(
                                result,
                                Err(NippyJarError::ChecksumMismatch { path, row: 37, column: 1 })
                                    if path == file.path()
                            ),
                            "lz4 {lz4}, pos {pos}: {result:?}"
                        );
                    } else {
                        assert!(result.is_ok(), "lz4 {lz4}, row {r}: {result:?}");
                    }
                }
                // Column 0 of the damaged row is intact and still reads on its own.
                let mut cursor = NippyJarCursor::new(&nippy).unwrap();
                assert_eq!(
                    cursor.row_by_number_with_cols(row, 0b01).unwrap().unwrap(),
                    [col1[row].as_slice()]
                );
                flip_byte(file.path(), pos, 0x01);
            }
            assert!(read_all_rows(file.path()).iter().all(Result::is_ok));
        }
    }

    /// TN(TN-0120): offsets moved onto another value read that value with its own valid
    /// checksum; the row and column in the checksum fail it. An unchecked jar returns the other
    /// value as if it were this one.
    #[test]
    fn test_checksum_detects_offset_moved_to_another_value() {
        let (col1, col2) = test_data(Some(10));
        for checksums in [true, false] {
            let file = tempfile::NamedTempFile::new().unwrap();
            if checksums {
                freeze_checksummed(file.path(), &col1, &col2, false);
            } else {
                NippyJar::new_without_header(2, file.path())
                    .freeze(
                        vec![clone_with_result(&col1), clone_with_result(&col2)],
                        col1.len() as u64,
                    )
                    .unwrap();
            }
            let nippy = NippyJar::load_without_header(file.path()).unwrap();
            let reader = nippy.open_data_reader().unwrap();
            // Row 5, column 0 now spans exactly row 6, column 0.
            let (other_start, other_end) = (reader.offset(12).unwrap(), reader.offset(13).unwrap());
            drop(reader);
            write_offset(file.path(), 10, other_start);
            write_offset(file.path(), 11, other_end);

            let nippy = NippyJar::load_without_header(file.path()).unwrap();
            let mut cursor = NippyJarCursor::new(&nippy).unwrap();
            let read = cursor.row_by_number_with_cols(5, 0b01);
            if checksums {
                assert!(
                    matches!(read, Err(NippyJarError::ChecksumMismatch { row: 5, column: 0, .. })),
                    "{read:?}"
                );
                // A value shorter than a checksum fails too, rather than slicing out of range.
                write_offset(file.path(), 11, other_start + 3);
                let mut cursor = NippyJarCursor::new(&nippy).unwrap();
                assert!(matches!(
                    cursor.row_by_number_with_cols(5, 0b01),
                    Err(NippyJarError::ChecksumMismatch { row: 5, column: 0, .. })
                ));
            } else {
                assert_eq!(read.unwrap().unwrap(), [col1[6].as_slice()]);
            }
        }
    }

    /// TN(TN-0120): the heal and prune keep a checksummed jar's framing: after an append that
    /// was never committed, a heal, a prune of committed and uncommitted rows, and appends after
    /// each, every row reads and verifies, including the appended ones at their new row indexes.
    #[test]
    fn test_checksummed_jar_heal_and_prune() {
        let (col1, col2) = test_data(Some(11));
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        freeze_checksummed(path, &col1, &col2, false);
        let rows = col1.len();

        // Rows written but not committed, as a crash leaves them: the heal drops them.
        {
            let nippy = NippyJar::load_without_header(path).unwrap();
            let mut writer = NippyJarWriter::new(nippy).unwrap();
            append_row(&mut writer, &col1, &col2, 0);
            append_row(&mut writer, &col1, &col2, 1);
            writer.data_file().flush().unwrap();
            writer.commit_offsets().unwrap();
        }
        let nippy = NippyJar::load_without_header(path).unwrap();
        assert_eq!(nippy.rows, rows);
        let mut writer = NippyJarWriter::new(nippy).unwrap();
        assert_eq!(writer.rows(), rows);
        assert!(read_all_rows(path).iter().all(Result::is_ok));

        // A prune of uncommitted and committed rows, then appends at the new row indexes.
        append_row(&mut writer, &col1, &col2, 0);
        writer.prune_rows(3).unwrap();
        assert_eq!(writer.rows(), rows - 2);
        append_row(&mut writer, &col1, &col2, 2);
        append_row(&mut writer, &col1, &col2, 3);
        writer.commit().unwrap();
        drop(writer);

        assert!(read_all_rows(path).iter().all(Result::is_ok));
        let nippy = NippyJar::load_without_header(path).unwrap();
        NippyJarChecker::new(NippyJar::load_without_header(path).unwrap())
            .check_consistency()
            .unwrap();
        let mut cursor = NippyJarCursor::new(&nippy).unwrap();
        assert_eq!(nippy.rows, rows);
        for (row, expected) in [(rows - 3, rows - 3), (rows - 2, 2), (rows - 1, 3)] {
            let values = cursor.row_by_number(row).unwrap().unwrap();
            assert_eq!(
                (values[0], values[1]),
                (col1[expected].as_slice(), col2[expected].as_slice())
            );
        }
    }

    /// The data, offsets and configuration files of the jar at `path`.
    fn jar_files(path: &Path) -> [Vec<u8>; 3] {
        [
            std::fs::read(path).unwrap(),
            std::fs::read(path.with_extension(OFFSETS_FILE_EXTENSION)).unwrap(),
            std::fs::read(path.with_extension(CONFIG_FILE_EXTENSION)).unwrap(),
        ]
    }

    /// Writes `files` ([`jar_files`]) as the jar at `path`.
    fn write_jar_files(path: &Path, files: &[Vec<u8>; 3]) {
        std::fs::write(path, &files[0]).unwrap();
        std::fs::write(path.with_extension(OFFSETS_FILE_EXTENSION), &files[1]).unwrap();
        std::fs::write(path.with_extension(CONFIG_FILE_EXTENSION), &files[2]).unwrap();
    }

    /// Freezes `col1` and `col2` into a jar at `path`, checksummed if `checksums`.
    fn freeze_version(path: &Path, col1: &ColumnValues, col2: &ColumnValues, checksums: bool) {
        if checksums {
            freeze_checksummed(path, col1, col2, false);
        } else {
            NippyJar::new_without_header(2, path)
                .freeze(vec![clone_with_result(col1), clone_with_result(col2)], col1.len() as u64)
                .unwrap();
        }
    }

    /// TN(TN-0130): files shorter than their configuration's committed rows: one row's offsets
    /// missing, or a final offset past the data (one flipped high bit). A prune commits the
    /// configuration before it cuts the files, so in a checksummed jar this is damage: the open
    /// fails and every file is left as it was. A plain jar keeps upstream's heal, which cuts the
    /// row, since it may hold an interrupted prune of an older build (which cut the files first).
    #[test]
    fn test_heal_of_files_shorter_than_the_config() {
        let (col1, col2) = test_data(Some(15));
        let rows = col1.len();
        let damages: [fn(&Path); 2] = [
            // The offsets file loses its final offset.
            |path| {
                let offsets = path.with_extension(OFFSETS_FILE_EXTENSION);
                let len = std::fs::metadata(&offsets).unwrap().len();
                OpenOptions::new().write(true).open(offsets).unwrap().set_len(len - 8).unwrap();
            },
            // The final offset's high bit flips.
            |path| {
                let offsets = path.with_extension(OFFSETS_FILE_EXTENSION);
                let len = std::fs::metadata(&offsets).unwrap().len();
                flip_byte(&offsets, len - 1, 0x80);
            },
        ];
        for (damage, file) in damages.into_iter().zip(["offsets", "data"]) {
            // Checksummed: refused, nothing cut.
            let jar = tempfile::NamedTempFile::new().unwrap();
            freeze_version(jar.path(), &col1, &col2, true);
            damage(jar.path());
            let damaged = jar_files(jar.path());
            let err = NippyJarWriter::new(NippyJar::load_without_header(jar.path()).unwrap())
                .unwrap_err();
            assert!(
                matches!(&err, NippyJarError::CommittedRowsMissing { path, file: short }
                    if path == jar.path() && *short == file),
                "{err}"
            );
            assert_eq!(jar_files(jar.path()), damaged);

            // Plain: upstream's heal cuts the row, and the rows it keeps read.
            let jar = tempfile::NamedTempFile::new().unwrap();
            freeze_version(jar.path(), &col1, &col2, false);
            damage(jar.path());
            drop(NippyJarWriter::new(NippyJar::load_without_header(jar.path()).unwrap()).unwrap());
            let nippy = NippyJar::load_without_header(jar.path()).unwrap();
            assert_eq!(nippy.rows, rows - 1);
            let mut cursor = NippyJarCursor::new(&nippy).unwrap();
            for row in 0..rows - 1 {
                let values = cursor.row_by_number(row).unwrap().unwrap();
                assert_eq!((values[0], values[1]), (col1[row].as_slice(), col2[row].as_slice()));
            }
        }
    }

    /// TN(TN-0130): a prune stopped at any point leaves a state the next open heals to either the
    /// jar before the prune or the finished prune, byte for byte, in both formats. A prune whose
    /// configuration cannot be written (a directory where its temporary file goes) has cut
    /// nothing; the configuration written but no file cut yet, or only the offsets cut, heals to
    /// the finished prune. Fails if a prune cuts a file before it commits the configuration.
    #[test]
    fn test_interrupted_prune_heals_at_every_point() {
        let (col1, col2) = test_data(Some(16));
        let rows = col1.len();
        for checksums in [false, true] {
            for prune in [1, 3, rows] {
                let jar = tempfile::NamedTempFile::new().unwrap();
                let path = jar.path();
                freeze_version(path, &col1, &col2, checksums);
                let before = jar_files(path);

                // The configuration cannot be written: the prune fails, nothing is cut.
                let mut writer =
                    NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap();
                append_row(&mut writer, &col1, &col2, 0);
                let blocker = path.with_extension("tmp");
                std::fs::create_dir(&blocker).unwrap();
                assert!(writer.prune_rows(prune).is_err());
                drop(writer);
                std::fs::remove_dir(&blocker).unwrap();
                drop(NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap());
                assert_eq!(jar_files(path), before, "checksums {checksums}, prune {prune}");

                // The finished prune.
                let mut writer =
                    NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap();
                append_row(&mut writer, &col1, &col2, 0);
                writer.prune_rows(prune + 1).unwrap();
                drop(writer);
                let after = jar_files(path);
                assert_jar_rows(path, &col1, &col2, rows - prune);

                // Stopped after the configuration, before any cut; and after the offsets cut.
                let [data, offsets, config] = after.clone();
                for stopped in [
                    [before[0].clone(), before[1].clone(), config.clone()],
                    [before[0].clone(), offsets.clone(), config.clone()],
                ] {
                    write_jar_files(path, &stopped);
                    drop(
                        NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap(),
                    );
                    assert_eq!(jar_files(path), after, "checksums {checksums}, prune {prune}");
                }
                assert_eq!(data, after[0]);
                assert!(read_all_rows(path).iter().all(Result::is_ok));
            }
        }
    }

    /// TN(TN-0131): a jar whose configuration counts no rows holds nothing committed, so the
    /// writer's open restores the files a new jar starts with, whatever a crash left in them: an
    /// empty or torn offsets file (a rotation's power loss after the new jar's configuration was
    /// written), a damaged final offset, or another jar's leftover files. Both formats. The
    /// reader refuses an empty offsets file with an error instead of a panic.
    #[test]
    fn test_jar_without_rows_opens_whatever_its_files_hold() {
        let (col1, col2) = test_data(Some(17));
        let mut created = vec![writer::OFFSET_SIZE_BYTES];
        created.extend_from_slice(&[0; writer::OFFSET_SIZE_BYTES as usize]);
        let mut damaged_final = created.clone();
        damaged_final[8] = 0x80;
        for checksums in [false, true] {
            let leftover = tempfile::NamedTempFile::new().unwrap();
            freeze_version(leftover.path(), &col1, &col2, checksums);
            let [leftover_data, leftover_offsets, _] = jar_files(leftover.path());

            for (offsets, data) in [
                (vec![], vec![]),
                (created[..5].to_vec(), vec![]),
                (damaged_final.clone(), vec![]),
                (created.clone(), leftover_data[..40].to_vec()),
                (leftover_offsets.clone(), leftover_data.clone()),
            ] {
                let jar = tempfile::NamedTempFile::new().unwrap();
                let path = jar.path();
                let mut nippy = NippyJar::new_without_header(2, path);
                if checksums {
                    nippy = nippy.with_checksums();
                }
                nippy.freeze_config().unwrap();
                std::fs::write(path, &data).unwrap();
                std::fs::write(path.with_extension(OFFSETS_FILE_EXTENSION), &offsets).unwrap();
                if offsets.is_empty() {
                    let err = NippyJar::load_without_header(path).unwrap().open_data_reader();
                    assert!(matches!(err, Err(NippyJarError::EmptyOffsetsFile(_))), "{err:?}");
                }

                let mut writer =
                    NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap();
                assert_eq!(std::fs::read(path).unwrap(), Vec::<u8>::new());
                assert_eq!(
                    std::fs::read(path.with_extension(OFFSETS_FILE_EXTENSION)).unwrap(),
                    created
                );
                append_row(&mut writer, &col1, &col2, 0);
                append_row(&mut writer, &col1, &col2, 1);
                writer.commit().unwrap();
                drop(writer);
                assert_jar_rows(path, &col1, &col2, 2);
            }
        }
    }

    /// TN(TN-0132): a jar created where a deletion stopped part way (`NippyJar::delete` removes
    /// the data file first) starts empty whatever the other files hold: the old jar's offsets
    /// file, or a data file without offsets. A jar whose configuration counts rows and has lost a
    /// file is refused, not created again.
    #[test]
    fn test_writer_creates_over_leftovers_and_refuses_a_missing_file() {
        let (col1, col2) = test_data(Some(18));
        for checksums in [false, true] {
            // (the leftover files kept)
            for kept in [&[OFFSETS_FILE_EXTENSION][..], &[""], &[OFFSETS_FILE_EXTENSION, ""]] {
                let jar = tempfile::NamedTempFile::new().unwrap();
                let path = jar.path();
                freeze_version(path, &col1, &col2, checksums);
                for ext in ["", OFFSETS_FILE_EXTENSION, CONFIG_FILE_EXTENSION] {
                    if !kept.contains(&ext) {
                        let file = if ext.is_empty() {
                            path.to_path_buf()
                        } else {
                            path.with_extension(ext)
                        };
                        std::fs::remove_file(file).unwrap();
                    }
                }
                if kept.len() == 2 {
                    // Both files there: the jar without rows the open finds is restored instead.
                    std::fs::remove_file(path.with_extension(OFFSETS_FILE_EXTENSION)).unwrap();
                }
                let mut nippy = NippyJar::new_without_header(2, path);
                if checksums {
                    nippy = nippy.with_checksums();
                }
                let mut writer = NippyJarWriter::new(nippy).unwrap();
                append_row(&mut writer, &col1, &col2, 0);
                writer.commit().unwrap();
                drop(writer);
                assert_jar_rows(path, &col1, &col2, 1);
            }

            // A committed jar without its data or offsets file.
            for ext in ["", OFFSETS_FILE_EXTENSION] {
                let jar = tempfile::NamedTempFile::new().unwrap();
                let path = jar.path();
                freeze_version(path, &col1, &col2, checksums);
                let missing =
                    if ext.is_empty() { path.to_path_buf() } else { path.with_extension(ext) };
                std::fs::remove_file(&missing).unwrap();
                let config = std::fs::read(path.with_extension(CONFIG_FILE_EXTENSION)).unwrap();
                let err =
                    NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap_err();
                assert!(matches!(&err, NippyJarError::MissingFile(file) if file == &missing));
                assert!(!missing.exists());
                assert_eq!(
                    std::fs::read(path.with_extension(CONFIG_FILE_EXTENSION)).unwrap(),
                    config
                );
            }
        }
    }

    /// TN(TN-0120): both CRC implementations give the same value for every length and initial
    /// value (they take the initial value in different forms), and seed 0 is the standard CRC-32.
    #[test]
    fn test_checksum_implementations_agree() {
        let mut rng = SmallRng::seed_from_u64(12);
        let mut bytes = vec![0u8; 300];
        rng.fill_bytes(&mut bytes);
        for len in 0..=300 {
            for seed in [0, u32::MAX, value_seed(0, 0), rng.next_u32(), rng.next_u32()] {
                let input = &bytes[..len];
                let expected = checksum_crc32fast(seed, input);
                assert_eq!(checksum_slice16(seed, input), expected, "len {len}, seed {seed:#x}");
                assert_eq!(checksum(seed, input), expected, "len {len}, seed {seed:#x}");
            }
            assert_eq!(checksum(0, &bytes[..len]), crc32fast::hash(&bytes[..len]));
        }
        assert_eq!(checksum(0, b"123456789"), 0xCBF4_3926);
        // Neighbouring positions get different seeds.
        assert_ne!(value_seed(5, 0), value_seed(6, 0));
        assert_ne!(value_seed(5, 0), value_seed(5, 1));
        assert_ne!(value_seed(5, 1), value_seed(6, 0));
    }

    /// TN(TN-0120): an unverified read returns the stored bytes without the checksum, even when
    /// they are damaged (the verified read of the same value fails); a value shorter than a
    /// checksum and a row past the end are not handed out.
    #[test]
    fn test_unverified_value() {
        let (col1, col2) = test_data(Some(13));
        let file = tempfile::NamedTempFile::new().unwrap();
        freeze_checksummed(file.path(), &col1, &col2, false);
        let nippy = NippyJar::load_without_header(file.path()).unwrap();
        let cursor = NippyJarCursor::new(&nippy).unwrap();
        for row in [0, 41, col1.len() - 1] {
            assert_eq!(cursor.unverified_value(row, 0).unwrap(), Some(col1[row].as_slice()));
            assert_eq!(cursor.unverified_value(row, 1).unwrap(), Some(col2[row].as_slice()));
        }
        assert_eq!(cursor.unverified_value(col1.len(), 0).unwrap(), None);
        assert_eq!(cursor.unverified_value(0, 2).unwrap(), None);
        drop(cursor);

        let start = nippy.open_data_reader().unwrap().offset(41 * 2).unwrap();
        flip_byte(file.path(), start, 0x80);
        let nippy = NippyJar::load_without_header(file.path()).unwrap();
        let mut cursor = NippyJarCursor::new(&nippy).unwrap();
        let mut damaged = col1[41].clone();
        damaged[0] ^= 0x80;
        assert_eq!(cursor.unverified_value(41, 0).unwrap(), Some(damaged.as_slice()));
        assert!(matches!(
            cursor.row_by_number_with_cols(41, 0b01),
            Err(NippyJarError::ChecksumMismatch { row: 41, column: 0, .. })
        ));

        write_offset(file.path(), 2 * 41 + 1, start + 3);
        let nippy = NippyJar::load_without_header(file.path()).unwrap();
        let cursor = NippyJarCursor::new(&nippy).unwrap();
        assert!(matches!(
            cursor.unverified_value(41, 0),
            Err(NippyJarError::ChecksumMismatch { row: 41, column: 0, .. })
        ));
    }

    /// TN(TN-0120): nanoseconds per checksum by value size, for each implementation. Prints a
    /// table; the `x86_64` rows decide the 128-byte switch in [`checksum`].
    #[test]
    #[ignore = "heavy: run with --release"]
    fn checksum_cost_heavy() {
        let iterations = 2_000_000u32;
        let mut bytes = vec![0u8; 600];
        SmallRng::seed_from_u64(14).fill_bytes(&mut bytes);
        println!("arch {}; ns per checksum", std::env::consts::ARCH);
        println!(
            "{:>5} {:>10} {:>10} {:>10} {:>10}",
            "bytes", "unseeded", "crc32fast", "slice16", "dispatch"
        );
        type Scheme = fn(u32, &[u8]) -> u32;
        let schemes: [Scheme; 4] =
            [|_, b| crc32fast::hash(b), checksum_crc32fast, checksum_slice16, checksum];
        for len in [20, 36, 44, 52, 60, 63, 64, 69, 80, 120, 127, 128, 176, 600] {
            let input = &bytes[..len];
            let ns = schemes.map(|scheme| {
                let start = std::time::Instant::now();
                let mut acc = 0u32;
                for i in 0..iterations {
                    acc ^= scheme(std::hint::black_box(i), std::hint::black_box(input));
                }
                std::hint::black_box(acc);
                start.elapsed().as_nanos() as f64 / iterations as f64
            });
            println!("{len:>5} {:>10.1} {:>10.1} {:>10.1} {:>10.1}", ns[0], ns[1], ns[2], ns[3]);
        }
    }

    fn append_row(writer: &mut NippyJarWriter, col1: &[Vec<u8>], col2: &[Vec<u8>], row: usize) {
        writer.append_column(Some(Ok(&col1[row]))).unwrap();
        writer.append_column(Some(Ok(&col2[row]))).unwrap();
    }

    /// Asserts that the jar on disk is consistent, claims exactly `rows` rows, and that every
    /// one of them reads back as the corresponding entry of `col1` and `col2`.
    fn assert_jar_rows(file_path: &Path, col1: &[Vec<u8>], col2: &[Vec<u8>], rows: usize) {
        let nippy = NippyJar::load_without_header(file_path).unwrap();
        assert_eq!(nippy.rows, rows);
        NippyJarChecker::new(NippyJar::load_without_header(file_path).unwrap())
            .check_consistency()
            .unwrap();

        let mut cursor = NippyJarCursor::new(&nippy).unwrap();
        for row in 0..rows {
            let values = cursor.row_by_number(row).unwrap().unwrap();
            assert_eq!((values[0], values[1]), (col1[row].as_slice(), col2[row].as_slice()));
        }
        assert!(cursor.row_by_number(rows).unwrap().is_none());
    }
}
