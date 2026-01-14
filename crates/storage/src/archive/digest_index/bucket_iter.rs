//! Implements the iterator for a buckets elements.  This also handles overflow buckets and allows
//! buckets to accessed without worrying about underlying structure or files.
//! NOTE: This is ONLY appropriate for the core DB.

use crate::archive::{crc::check_crc, digest_index::index::HdxIndex};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

/// Iterates over the (hash, record_position) values contained in a bucket.
/// If supplied with a hash will only return the hash/positions that match hash (i.e. hash will
/// always be the same). Uses a binary search of buckets when given a hash.
pub(crate) struct BucketIter<'bucket> {
    buffer: &'bucket [u8],
    overflow_buffer: Vec<u8>,
    bucket_pos: usize,
    start_pos: usize,
    end_pos: usize,
    overflow_pos: u64,
    elements: u32,
    crc_failure: bool,
}

impl<'bucket> BucketIter<'bucket> {
    pub(super) fn new(buffer: &'bucket [u8]) -> Self {
        let mut buf = [0_u8; 8]; // buffer for converting to u64s (needs an array)
        buf.copy_from_slice(&buffer[0..8]);
        let overflow_pos = u64::from_le_bytes(buf);
        let mut buf = [0_u8; 4]; // buffer for converting to u32s (needs an array)
        buf.copy_from_slice(&buffer[8..12]);
        let elements = u32::from_le_bytes(buf);
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = 0;
        Self {
            buffer,
            overflow_buffer: vec![],
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            crc_failure: false, // Assume the initial bucket we are provided is valid.
        }
    }

    pub(super) fn new_with_empty_buffer(buffer: &'bucket [u8]) -> Self {
        let overflow_pos = 0;
        let elements = 0;
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = 0;
        let overflow_buffer = vec![];
        Self {
            buffer,
            overflow_buffer,
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            crc_failure: true, // Force next to always return None.
        }
    }

    pub(super) fn new_with_elements_overflow(
        buffer: &'bucket [u8],
        elements: u32,
        overflow_pos: u64,
    ) -> Self {
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = 0;
        let overflow_buffer = vec![];
        Self {
            buffer,
            overflow_buffer,
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            crc_failure: false, // Assume the initial bucket we are provided is valid.
        }
    }

    pub(super) fn crc_failure(&self) -> bool {
        self.crc_failure
    }

    fn get_element<const KSIZE: usize>(&self, bucket_pos: usize) -> (&[u8], u64) {
        let mut buf64 = [0_u8; 8];
        // 12- 8 bytes for overflow position and 4 for the elements in the bucket.
        let mut pos = 12 + (bucket_pos * HdxIndex::<KSIZE>::BUCKET_ELEMENT_SIZE);
        let key_pos = pos;
        pos += KSIZE;
        buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
        let rec_pos = u64::from_le_bytes(buf64);
        (&self.buffer[key_pos..(key_pos + KSIZE)], rec_pos)
    }

    fn reset_next_overflow(&mut self, odx_file: &mut File) -> Option<()> {
        self.overflow_buffer.resize(self.buffer.len(), 0);
        // For reading u64 values, needs an array.
        let mut buf64 = [0_u8; 8];
        odx_file.seek(SeekFrom::Start(self.overflow_pos)).ok()?;
        odx_file.read_exact(&mut self.overflow_buffer[..]).ok()?;
        if !check_crc(&self.overflow_buffer) {
            self.crc_failure = true;
            return None;
        }
        buf64.copy_from_slice(&self.overflow_buffer[0..8]);
        self.overflow_pos = u64::from_le_bytes(buf64);
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(&self.overflow_buffer[8..12]);
        self.elements = u32::from_le_bytes(buf);
        self.start_pos = 0;
        self.end_pos = self.elements as usize;
        self.bucket_pos = 0;
        let buffer = unsafe {
            (&self.overflow_buffer as *const Vec<u8>).as_ref().expect("this can't be null")
        };
        self.buffer = buffer;
        Some(())
    }
}

impl<const KSIZE: usize> HdxIndex<KSIZE> {
    /// Advance and return the next hash/position for the bucket defined by BucketIter.
    /// Setup and get a BucketIter with a call to bucket_iter().  If the BucketIter is given a hash
    /// then all returned hash values will match it (will return the elements that have that hash).
    pub(crate) fn next_bucket_element<'s>(
        &mut self,
        iter: &'s mut BucketIter<'_>,
    ) -> Option<(&'s [u8], u64)> {
        if iter.crc_failure {
            return None;
        }
        loop {
            if iter.elements == 0 {
                if iter.overflow_pos > 0 {
                    iter.reset_next_overflow(&mut self.odx_file)?;
                    continue;
                }
                return None;
            }
            if iter.bucket_pos < iter.elements as usize {
                let bucket_pos = iter.bucket_pos;
                iter.bucket_pos += 1;
                let (rec_hash, rec_pos) = iter.get_element::<KSIZE>(bucket_pos);
                return Some((rec_hash, rec_pos));
            } else if iter.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                iter.reset_next_overflow(&mut self.odx_file)?;
            } else {
                return None;
            }
        }
    }
}
