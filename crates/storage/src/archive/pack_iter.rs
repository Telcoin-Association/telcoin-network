//! Raw iterator over a sldb data file.  This will work without an index file and can be used to
//! open the file directly.

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek};
use std::marker::PhantomData;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tn_types::decode;

use crate::archive::error::fetch::FetchError;
use crate::archive::error::load_header::LoadHeaderError;
use crate::archive::pack::DataHeader;

/// Iterate over a Db's key, value pairs in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
#[derive(Debug)]
pub struct PackIter<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    _val: PhantomData<V>,
    file: BufReader<File>,
    buffer: Vec<u8>,
}

impl<V> PackIter<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    /// Open the data file in dir with base_name (note do not include the .dat- that is appended).
    /// Produces an iterator over all the (key, values).  Does not use the index at all and records
    /// are returned in insert order.
    pub fn open<P: AsRef<Path>>(data_name: P) -> Result<Self, LoadHeaderError> {
        let mut data_file =
            OpenOptions::new().read(true).write(false).create(false).open(data_name)?;

        let _header = DataHeader::load_header(&mut data_file)?;
        let file = BufReader::new(data_file);
        Ok(Self { _val: PhantomData, file, buffer: Vec::new() })
    }

    /// Same as open but created from an existing File.
    pub fn with_file(dat_file: File) -> Result<Self, LoadHeaderError> {
        let mut dat_file = dat_file;
        let _header = DataHeader::load_header(&mut dat_file)?;
        let file = BufReader::new(dat_file);
        Ok(Self { _val: PhantomData, file, buffer: Vec::new() })
    }

    /// Return the current position of the data file.
    pub fn position(&mut self) -> io::Result<u64> {
        self.file.stream_position()
    }

    /// Read the next record or return an error if an overflow bucket.
    /// This expects the file cursor to be positioned at the records first byte.
    fn read_record_file<R: Read + Seek>(
        file: &mut R,
        buffer: &mut Vec<u8>,
    ) -> Result<V, FetchError> {
        let mut crc32_hasher = crc32fast::Hasher::new();
        let mut val_size_buf = [0_u8; 4];
        if let Err(err) = file.read_exact(&mut val_size_buf) {
            // An EOF here should be caused by no more records although it is possible there
            // was a bit of garbage at the end of the file, not worrying about that now (maybe ever).
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

impl<V> Iterator for PackIter<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    type Item = Result<V, FetchError>;

    fn next(&mut self) -> Option<Self::Item> {
        match Self::read_record_file(&mut self.file, &mut self.buffer) {
            Ok(val) => Some(Ok(val)),
            Err(err) => match err {
                FetchError::NotFound => None,
                _ => Some(Err(err)),
            },
        }
    }
}
