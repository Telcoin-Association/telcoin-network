//! Contains the error for the insert() function.

use std::{error::Error, fmt, io};

/// Custom error type for Inserts.
#[derive(Debug)]
pub enum AppendError {
    /// Error serializing the value to store in DB.
    SerializeValue(String),
    /// Database opened read-only.
    ReadOnly,
    /// Got an io error writing the key/value record. Every such error moves a pack to its failed
    /// state.
    WriteDataError(io::Error),
    /// The record is larger than any read path accepts, so it was refused and nothing of it is
    /// left in the log (a record refused part-way through its write is rolled back). A
    /// caller/value error: the pack stays healthy.
    RecordTooLarge {
        /// The rejected size in bytes: decoded (the size at which the encode passed the cap), or
        /// framed after compression.
        size: usize,
        /// The per-record maximum.
        max: u32,
    },
    /// Attempted to insert a duplicate key to an index.
    DuplicateKey,
    /// The key is not the index's fixed key size. A caller error: nothing was written.
    KeySize {
        /// The index's key size in bytes.
        expected: usize,
        /// The size of the key given.
        got: usize,
    },
    /// CRC problem, some index types might need this.
    CrcError,
    /// A structural on-disk index value was out of range while rewriting the index (bad bucket
    /// element count, invalid overflow pointer, or a broken split invariant). Surfaced instead of
    /// panicking on an untrusted value.
    CorruptIndex(String),
}

impl Error for AppendError {}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::SerializeValue(e) => write!(f, "value serialization: {e}"),
            Self::ReadOnly => write!(f, "read only"),
            Self::WriteDataError(e) => write!(f, "write data failed: {e}"),
            Self::RecordTooLarge { size, max } => {
                write!(f, "record size {size} exceeds the maximum {max}")
            }
            Self::DuplicateKey => write!(f, "duplicate key"),
            Self::KeySize { expected, got } => {
                write!(f, "key is {got} bytes, the index's keys are {expected}")
            }
            Self::CrcError => write!(f, "crc error"),
            Self::CorruptIndex(e) => write!(f, "corrupt index: {e}"),
        }
    }
}

impl From<io::Error> for AppendError {
    fn from(err: io::Error) -> Self {
        Self::WriteDataError(err)
    }
}
