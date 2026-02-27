//! Implement a simple integer to file position index.  The integer should be incrementing by one.

use std::{
    fs,
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    path::{Path, PathBuf},
};

use crate::archive::{
    crc::{add_crc32, check_crc},
    data_file::DataFile,
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    index::Index,
    pack::DataHeader,
};

/// Size of a header.
pub const PDX_HEADER_SIZE: usize = 30;

/// Header for an pdx (index) file.  This contains file positions at fixed locations for
/// looking up records by incrementing integer.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
struct PdxHeader {
    type_id: [u8; 8], // The characters "telcoinx"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
}

impl PdxHeader {
    /// Return a default HdxHeader with any values from data_header overridden.
    /// This includes the version, uid, appnum, bucket_size and bucket_elements.
    fn from_data_header(data_header: &DataHeader) -> Self {
        Self {
            type_id: *b"telcoinx",
            version: data_header.version(),
            uid: data_header.uid(),
            appnum: data_header.appnum(),
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    fn load_header(hdx_file: &mut DataFile) -> Result<Self, LoadHeaderError> {
        hdx_file.rewind()?;
        let mut buffer = [0_u8; PDX_HEADER_SIZE];
        let mut buf16 = [0_u8; 2];
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
        let header = Self { type_id, version, uid, appnum };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    fn write_header(&mut self, hdx_file: &mut DataFile) -> Result<(), io::Error> {
        hdx_file.rewind()?;
        let mut buffer = [0_u8; PDX_HEADER_SIZE];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        add_crc32(&mut buffer[..]);
        hdx_file.write_all(&buffer[..])?;
        Ok(())
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
}

/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
pub struct PositionIndex {
    _header: PdxHeader,
    pdx_file: DataFile,
    _index_dir: PathBuf,
}

impl PositionIndex {
    /// Open a PDX index file and return the open index.
    pub fn open_pdx_file<P: AsRef<Path>>(
        dir: P,
        data_header: &DataHeader,
        read_only: bool,
    ) -> Result<PositionIndex, LoadHeaderError> {
        let dir = dir.as_ref();
        let _ = fs::create_dir(dir);
        let mut pdx_file = DataFile::open(dir.join("index.pdx"), read_only)?;

        let header = if pdx_file.is_empty() {
            let mut header = PdxHeader::from_data_header(data_header);
            header.write_header(&mut pdx_file)?;
            header
        } else {
            let header = PdxHeader::load_header(&mut pdx_file)?;
            // Basic validation of the pdx header.
            if header.version() != data_header.version() {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != data_header.appnum() {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != data_header.uid() {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            header
        };
        Ok(Self { _header: header, pdx_file, _index_dir: dir.to_owned() })
    }

    /// Return the number of values in this index.
    pub fn len(&self) -> usize {
        (self.pdx_file.len().saturating_sub(PDX_HEADER_SIZE as u64) / 8) as usize
    }

    /// True if there are no keys stored in this index.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return an iterator over file positions with up to len items.
    pub fn iter(&mut self, len: usize) -> Result<PositionIter, std::io::Error> {
        let data_len = if len < self.len() { len } else { self.len() };
        let mut data = vec![0_u8; data_len * 8];
        self.pdx_file.seek(SeekFrom::Start(PDX_HEADER_SIZE as u64))?;
        self.pdx_file.read_exact(data.as_mut_slice())?;
        Ok(PositionIter::new(data))
    }

    /// Return a reverse iterator over file positions with up to len items.
    pub fn rev_iter(&mut self, len: usize) -> Result<PositionIter, std::io::Error> {
        let data_len = if len < self.len() { len } else { self.len() };
        let mut data = vec![0_u8; data_len * 8];
        self.pdx_file.seek(SeekFrom::End(-(data_len as i64 * 8)))?;
        self.pdx_file.read_exact(data.as_mut_slice())?;
        Ok(PositionIter::new_rev(data))
    }

    /// Truncate the index to key (inclusive).
    pub fn truncate_to_index(&mut self, key: u64) -> Result<(), io::Error> {
        let pos = PDX_HEADER_SIZE as u64 + (key * 8) + 8;
        self.pdx_file.set_len(pos)
    }

    /// Truncate the index to just the header.
    pub fn truncate_all(&mut self) -> Result<(), io::Error> {
        let pos = PDX_HEADER_SIZE as u64;
        self.pdx_file.set_len(pos)
    }
}

impl Index<u64> for PositionIndex {
    fn save(&mut self, key: u64, record_pos: u64) -> Result<(), AppendError> {
        if self.len() != key as usize {
            Err(AppendError::SerializeValue(format!(
                "{} must add the next item by position, expected {} got {key}",
                self.pdx_file.path().to_string_lossy(),
                self.len()
            )))
        } else {
            self.pdx_file.write_all(&record_pos.to_le_bytes())?;
            Ok(())
        }
    }

    fn load(&mut self, key: u64) -> Result<u64, FetchError> {
        let pos = PDX_HEADER_SIZE as u64 + (key * 8);
        self.pdx_file.seek(SeekFrom::Start(pos))?;
        let mut buf = [0_u8; 8];
        self.pdx_file.read_exact(&mut buf[..])?;
        Ok(u64::from_le_bytes(buf))
    }

    fn sync(&mut self) -> Result<(), CommitError> {
        self.pdx_file.flush().map_err(CommitError::IndexFileSync)?;
        self.pdx_file.sync_all().map_err(CommitError::IndexFileSync)?;
        Ok(())
    }
}

/// Iterator of u64 record positions from a position index.
#[derive(Debug)]
pub struct PositionIter {
    data: Vec<u8>,
    pos: usize,
    done: bool,
    reverse: bool,
}

impl PositionIter {
    /// New position iter over data.
    fn new(data: Vec<u8>) -> Self {
        let done = data.is_empty();
        Self { data, pos: 0, done, reverse: false }
    }

    /// New reverse iter over data.
    fn new_rev(data: Vec<u8>) -> Self {
        let done = data.is_empty();
        let pos = data.len().saturating_sub(8);
        Self { data, pos, done, reverse: true }
    }
}

impl Iterator for PositionIter {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.done {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&self.data[self.pos..self.pos + 8]);
            if self.reverse {
                self.done = self.pos == 0;
                self.pos = self.pos.saturating_sub(8);
            } else {
                self.pos = self.pos.saturating_add(8);
                self.done = self.pos >= self.data.len();
            }
            Some(u64::from_le_bytes(buf))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_archive_pdx_index() {
        let tmp_path = TempDir::with_prefix("test_archive_pdx_index").expect("temp dir");
        let data_header = DataHeader::new(0);
        let mut idx: PositionIndex =
            PositionIndex::open_pdx_file(tmp_path.path().join("index.pdx"), &data_header, false)
                .expect("pdx file");
        for i in 0..1_000_000 {
            idx.save(i, i * 100).expect("add to index");
        }
        for i in 0..1_000_000 {
            assert_eq!(idx.load(i).expect("load idx"), i * 100, "failed on iteration {i}");
        }
        drop(idx);
        let mut idx: PositionIndex =
            PositionIndex::open_pdx_file(tmp_path.path().join("index.pdx"), &data_header, false)
                .expect("pdx file");
        for i in (0..1_000_000).rev() {
            assert_eq!(
                idx.load(i).expect(&format!("load idx {i}")),
                i * 100,
                "failed on iteration {i}"
            );
        }
        idx.save(1_000_000, 66).expect("add to index");
        assert_eq!(idx.load(1_000_000).expect("load idx"), 66);
        drop(idx);
        let mut idx: PositionIndex =
            PositionIndex::open_pdx_file(tmp_path.path().join("index.pdx"), &data_header, true)
                .expect("pdx file");
        for i in (0..1_000_000).rev() {
            assert_eq!(
                idx.load(i).expect(&format!("load idx {i}")),
                i * 100,
                "failed on iteration {i}"
            );
        }

        // Test reverse iter.
        let iter = idx.rev_iter(1000).unwrap();
        assert_eq!(iter.count(), 1000, "asked for 1000 items");
        let mut iter = idx.rev_iter(1000).unwrap();
        assert_eq!(iter.next().unwrap(), 66, "last record wrong");
        let d = 999_999;
        for (i, pos) in iter.enumerate() {
            assert_eq!(pos, ((d - i) * 100) as u64, "failed rev on iteration {i}");
        }

        // Test iter
        let iter = idx.iter(1000).unwrap();
        assert_eq!(iter.count(), 1000, "asked for 1000 items");
        let iter = idx.iter(1000).unwrap();
        for (i, pos) in iter.enumerate() {
            assert_eq!(pos, (i * 100) as u64, "failed rev on iteration {i}");
        }

        assert_eq!(idx.load(1_000_000).expect("load idx"), 66);
    }
}
