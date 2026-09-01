//! On-disk header for the B+tree index, stored in page 0.
//!
//! The header occupies a full [`PAGE_SIZE`] page (so node pages start on a page boundary) and ends
//! with a CRC32 over the page.  It records the paired pack's `version`/`uid`/`appnum` (for
//! cross-checking), the page/key/value geometry (validated on reopen), and the mutable tree state
//! (`root_page`, `height`, `page_count`, `values`, `first_leaf`, `last_leaf`, `data_file_length`).

use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
};

use crate::archive::{
    btree_index::page::PAGE_SIZE,
    crc::{add_crc32, check_crc},
    error::load_header::LoadHeaderError,
    pack::{DataHeader, DATA_HEADER_BYTES},
};

/// Type identifier stamped at the start of a B+tree index header.
pub(crate) const HEADER_TYPE_ID: [u8; 8] = *b"telcoinb";

/// Fixed value size (a `u64` file offset) recorded in the header geometry.
pub(crate) const VALUE_SIZE: u16 = 8;

/// The parsed page-0 header of a B+tree index file.
#[derive(Debug, Clone)]
pub(crate) struct BtreeHeader {
    pub(crate) type_id: [u8; 8],
    pub(crate) version: u16,
    pub(crate) uid: u64,
    pub(crate) appnum: u32,
    pub(crate) page_size: u32,
    pub(crate) ksize: u16,
    pub(crate) value_size: u16,
    pub(crate) root_page: u32,
    pub(crate) height: u32,
    pub(crate) page_count: u32,
    pub(crate) values: u64,
    pub(crate) first_leaf: u32,
    pub(crate) last_leaf: u32,
    pub(crate) data_file_length: u64,
}

impl BtreeHeader {
    /// Build a fresh header for a brand-new index, stamping identity from `data_header` and the
    /// compile-time geometry.  The tree starts as a single empty leaf on page 1.
    pub(crate) fn new(data_header: &DataHeader, ksize: u16) -> Self {
        Self {
            type_id: HEADER_TYPE_ID,
            version: data_header.version(),
            uid: data_header.uid(),
            appnum: data_header.appnum(),
            page_size: PAGE_SIZE as u32,
            ksize,
            value_size: VALUE_SIZE,
            root_page: 1,
            height: 1,
            page_count: 2, // page 0 = header, page 1 = root leaf
            values: 0,
            first_leaf: 1,
            last_leaf: 1,
            data_file_length: DATA_HEADER_BYTES as u64,
        }
    }

    /// Load and CRC-check the header page from the start of `file`.  Identity/geometry validation
    /// against the paired pack is done by the caller (`open_btx_file`).
    pub(crate) fn load(file: &mut File) -> Result<Self, LoadHeaderError> {
        file.rewind()?;
        let mut buf = vec![0_u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;
        if !check_crc(&buf) {
            return Err(LoadHeaderError::CrcFailed);
        }
        let mut type_id = [0_u8; 8];
        type_id.copy_from_slice(&buf[0..8]);
        if type_id != HEADER_TYPE_ID {
            return Err(LoadHeaderError::InvalidType);
        }
        let rd16 = |o: usize| u16::from_le_bytes(buf[o..o + 2].try_into().unwrap());
        let rd32 = |o: usize| u32::from_le_bytes(buf[o..o + 4].try_into().unwrap());
        let rd64 = |o: usize| u64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
        Ok(Self {
            type_id,
            version: rd16(8),
            uid: rd64(10),
            appnum: rd32(18),
            page_size: rd32(22),
            ksize: rd16(26),
            value_size: rd16(28),
            root_page: rd32(30),
            height: rd32(34),
            page_count: rd32(38),
            values: rd64(42),
            first_leaf: rd32(50),
            last_leaf: rd32(54),
            data_file_length: rd64(58),
        })
    }

    /// Serialize the header into a fresh page-0 buffer with a trailing CRC32.
    pub(crate) fn to_page(&self) -> Vec<u8> {
        let mut buf = vec![0_u8; PAGE_SIZE];
        buf[0..8].copy_from_slice(&self.type_id);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf[10..18].copy_from_slice(&self.uid.to_le_bytes());
        buf[18..22].copy_from_slice(&self.appnum.to_le_bytes());
        buf[22..26].copy_from_slice(&self.page_size.to_le_bytes());
        buf[26..28].copy_from_slice(&self.ksize.to_le_bytes());
        buf[28..30].copy_from_slice(&self.value_size.to_le_bytes());
        buf[30..34].copy_from_slice(&self.root_page.to_le_bytes());
        buf[34..38].copy_from_slice(&self.height.to_le_bytes());
        buf[38..42].copy_from_slice(&self.page_count.to_le_bytes());
        buf[42..50].copy_from_slice(&self.values.to_le_bytes());
        buf[50..54].copy_from_slice(&self.first_leaf.to_le_bytes());
        buf[54..58].copy_from_slice(&self.last_leaf.to_le_bytes());
        buf[58..66].copy_from_slice(&self.data_file_length.to_le_bytes());
        add_crc32(&mut buf);
        buf
    }

    /// Write the header page to the start of `file`.
    pub(crate) fn write(&self, file: &mut File) -> Result<(), io::Error> {
        let buf = self.to_page();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf)?;
        Ok(())
    }
}
