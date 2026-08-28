//! Contains the Hash Index overflow buckets (ODX) structure and code.

use crate::archive::{
    crc::{add_crc32, check_crc},
    data_file::fsync_directory,
    data_file_mmap::{MmapAccess, MmapDataFile, MmapFileOptions, WriteMode},
    error::load_header::LoadHeaderError,
};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

/// Minimum size of an index overflow header, includes the crc32 checksum following the header.
const MIN_HEADER_SIZE: usize = 28;

/// Header for an odx (index overflow) file.  This contains the overflow hash buckets for lookups.
/// This file is an append only log file and the header and buckets will NOT change in place over
/// time. This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug, Copy, Clone)]
pub struct OdxHeader {
    type_id: [u8; 8], // The characters "telcoinx"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u32,      // Application defined constant
    header_size: usize, /* Size of the header (not saved to file, max of bucket_size or
                       * MIN_HEADER_SIZE). */
    read_only: bool, // Is this file read only?
}

impl OdxHeader {
    fn open_header<F, P>(
        file: &mut F,
        version: u16,
        uid: u64,
        appnum: u32,
        path: P,
        read_only: bool,
    ) -> Result<OdxHeader, LoadHeaderError>
    where
        F: Write + Read + Seek + ?Sized,
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let file_end = file.seek(SeekFrom::End(0))?;

        let header = if file_end == 0 {
            if read_only {
                return Err(LoadHeaderError::ReadOnlyEmpty);
            }
            let header = OdxHeader::new(version, uid, appnum, read_only);
            header.write_header(file)?;
            // New odx file was just created and its header written; fsync the parent so the entry
            // is durable.
            if let Some(parent) = path.parent() {
                let _ = fsync_directory(parent);
            }
            header
        } else {
            let header = OdxHeader::load_header(file, read_only)?;
            // Basic validation of the odx header.
            if header.version() != version {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != appnum {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != uid {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            header
        };
        Ok(header)
    }

    /// Open the index overflow file (odx file) and return the open file and header.
    pub fn open_odx_file_mmap<P: AsRef<Path>>(
        version: u16,
        uid: u64,
        appnum: u32,
        path: P,
        read_only: bool,
    ) -> Result<(MmapDataFile, OdxHeader), LoadHeaderError> {
        let path = path.as_ref();
        // The digest index does point lookups over fixed-offset hash buckets — random
        // access with no benefit from readahead — so hint `MADV_RANDOM`.
        let opts = MmapFileOptions {
            write_mode: WriteMode::Random,
            access: MmapAccess::Random,
            ..Default::default()
        };
        let mut file = MmapDataFile::open_with(path, read_only, opts)?;

        let header = Self::open_header(&mut file, version, uid, appnum, path, read_only)?;
        Ok((file, header))
    }

    /// Return a default OdxHeader with any values from hdx_header overridden.
    /// This includes the version, uid, appnum and bucket_size.
    fn new(version: u16, uid: u64, appnum: u32, read_only: bool) -> Self {
        let header_size = MIN_HEADER_SIZE;
        Self { type_id: *b"telcoinx", version, uid, appnum, header_size, read_only }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    fn load_header<F: Read + Seek + ?Sized>(
        source: &mut F,
        read_only: bool,
    ) -> Result<Self, LoadHeaderError> {
        let header_size = MIN_HEADER_SIZE;
        source.rewind()?;
        let mut buffer = vec![0_u8; header_size];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        if !check_crc(&buffer) {
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
        let header = Self { type_id, version, uid, appnum, header_size, read_only };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    fn write_header<F: Write + Seek + ?Sized>(&self, sync: &mut F) -> Result<(), LoadHeaderError> {
        if self.read_only {
            return Err(LoadHeaderError::ReadOnly);
        }
        let mut buffer = vec![0_u8; self.header_size];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 4)].copy_from_slice(&self.appnum.to_le_bytes());
        add_crc32(&mut buffer);
        sync.write_all(&buffer)?;
        Ok(())
    }

    /// File version number.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Unique ID generated on creation
    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// Application defined constant
    pub fn appnum(&self) -> u32 {
        self.appnum
    }
}
