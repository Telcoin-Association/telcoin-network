//! Contains the error for the data fetching.

use std::{error::Error, fmt, io};

/// Error on reading a DB record.
#[derive(Debug)]
pub enum FetchError {
    /// Failed to deserialize the value.
    DeserializeValue(String),
    /// An IO error seeking or reading in the data file.
    IO(io::Error),
    /// Requested item was not found.
    NotFound,
    /// The calculated and recorded crc32 codes do not match for the record.
    CrcFailed,
}

impl Error for FetchError {}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::DeserializeValue(e) => write!(f, "deserialize value: {e}"),
            Self::IO(e) => write!(f, "io: {e}"),
            Self::NotFound => write!(f, "not found"),
            Self::CrcFailed => write!(f, "crc32 mismatch"),
        }
    }
}

impl From<io::Error> for FetchError {
    fn from(io_err: io::Error) -> Self {
        Self::IO(io_err)
    }
}
