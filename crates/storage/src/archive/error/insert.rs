//! Contains the error for the insert() function.

use std::{error::Error, fmt, io};

/// Custom error type for Inserts.
#[derive(Debug)]
pub enum AppendError {
    /// Error serializing the value to store in DB.
    SerializeValue(String),
    /// Database opened read-only.
    ReadOnly,
    /// Got an io error writing the key/value record.
    WriteDataError(io::Error),
    /// Attempted to insert a duplicate key to an index.
    DuplicateKey,
    /// CRC problem, some index types might need this.
    CrcError,
}

impl Error for AppendError {}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::SerializeValue(e) => write!(f, "value serialization: {e}"),
            Self::ReadOnly => write!(f, "read only"),
            Self::WriteDataError(e) => write!(f, "write data failed: {e}"),
            Self::DuplicateKey => write!(f, "duplicate key"),
            Self::CrcError => write!(f, "crc error"),
        }
    }
}

impl From<io::Error> for AppendError {
    fn from(err: io::Error) -> Self {
        Self::WriteDataError(err)
    }
}
