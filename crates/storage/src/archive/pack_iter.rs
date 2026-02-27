//! Raw iterator over a sldb data file.  This will work without an index file and can be used to
//! open the file directly.

use std::{
    fmt::Debug,
    io,
    io::{BufReader, Read, Seek},
    marker::PhantomData,
};

use serde::{de::DeserializeOwned, Serialize};
use tn_types::decode;

use crate::archive::{
    error::{fetch::FetchError, load_header::LoadHeaderError},
    pack::DataHeader,
};

/// Iterate over a Db's key, value pairs in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
#[derive(Debug)]
pub struct PackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
    R: Read,
{
    _val: PhantomData<V>,
    reader: BufReader<R>,
    buffer: Vec<u8>,
}

impl<V, R> PackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
    R: Read + Seek,
{
    /// Open the iterator using reader as a data source.
    /// Produces an iterator over all the (key, values).  All and records
    /// are returned in insert order.
    pub fn open(mut reader: R, uid_idx: u64) -> Result<Self, LoadHeaderError> {
        let _header = DataHeader::load_header(&mut reader, uid_idx)?;
        let reader = BufReader::new(reader);
        Ok(PackIter { _val: PhantomData, reader, buffer: Vec::new() })
    }

    /// Return the current position of the data file.
    pub fn position(&mut self) -> io::Result<u64> {
        self.reader.stream_position()
    }

    /// Sets the current position of the data file.
    pub fn set_position(&mut self, position: u64) -> io::Result<()> {
        self.reader.seek(io::SeekFrom::Start(position))?;
        Ok(())
    }

    /// Read the next record or return an error if an overflow bucket.
    /// This expects the file cursor to be positioned at the records first byte.
    fn read_record_file<R2: Read + Seek>(
        file: &mut R2,
        buffer: &mut Vec<u8>,
    ) -> Result<V, FetchError> {
        let mut crc32_hasher = crc32fast::Hasher::new();
        let mut val_size_buf = [0_u8; 4];
        if let Err(err) = file.read_exact(&mut val_size_buf) {
            // An EOF here should be caused by no more records although it is possible there
            // was a bit of garbage at the end of the file, not worrying about that now (maybe
            // ever).
            if let io::ErrorKind::UnexpectedEof = err.kind() {
                return Err(FetchError::NotFound);
            }
            return Err(FetchError::IO(err));
        }
        crc32_hasher.update(&val_size_buf);
        let val_size = u32::from_le_bytes(val_size_buf);
        buffer.resize(val_size as usize, 0);
        file.read_exact(buffer)?;
        crc32_hasher.update(buffer);
        let calc_crc32 = crc32_hasher.finalize();
        let val = decode::<V>(&buffer[..]);
        let mut buf_u32 = [0_u8; 4];
        file.read_exact(&mut buf_u32)?;
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        Ok(val)
    }
}

impl<V, R: Read + Seek> Iterator for PackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
{
    type Item = Result<V, FetchError>;

    fn next(&mut self) -> Option<Self::Item> {
        match Self::read_record_file(&mut self.reader, &mut self.buffer) {
            Ok(val) => Some(Ok(val)),
            Err(err) => match err {
                FetchError::NotFound => None,
                _ => Some(Err(err)),
            },
        }
    }
}
