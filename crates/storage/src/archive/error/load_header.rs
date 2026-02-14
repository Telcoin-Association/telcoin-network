//! Error type for a failure to load a header.

use std::{error::Error, fmt, io};

/// Error on loading a file (inder or data) header.
#[derive(Debug)]
pub enum LoadHeaderError {
    /// The type string for the header was invalid- corrupted or incorrect file type.
    InvalidType,
    /// An underlying IO error while loading the header
    IO(io::Error),
    /// CRC failed on header data.
    CrcFailed,
    /// The data file does not match the config.
    InvalidAppNum,
    /// The data file version invalid (not supported).
    InvalidVersion,
    /// The HDX index file version was wrong.
    InvalidIndexVersion,
    /// The HDX index UUID did not match the data file.
    InvalidIndexUID,
    /// The HDX index app number did not match the data file.
    InvalidIndexAppNum,
    /// The salt when hashed with provided hasher did not produce the pepper.
    InvalidHasher,
    /// The ODX index overflow file version was wrong.
    InvalidOverflowVersion,
    /// The ODX index overflow file UUID did not match the data/index file.
    InvalidOverflowUID,
    /// The ODX index overflowfile app number did not match the data file.
    InvalidOverflowAppNum,
}

impl Error for LoadHeaderError {}

impl fmt::Display for LoadHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::InvalidType => write!(f, "invalid type id"),
            Self::IO(e) => write!(f, "io: {e}"),
            Self::CrcFailed => write!(f, "invalid crc32 checksum"),
            Self::InvalidVersion => write!(f, "invalid version (should be 0)"),
            Self::InvalidAppNum => write!(f, "invalid appnum"),
            Self::InvalidIndexVersion => write!(f, "invalid index version"),
            Self::InvalidIndexUID => write!(f, "invalid index uid"),
            Self::InvalidIndexAppNum => write!(f, "invalid index appnum"),
            Self::InvalidHasher => write!(f, "invalid hash algorithm"),
            Self::InvalidOverflowVersion => write!(f, "invalid index overflow version"),
            Self::InvalidOverflowUID => write!(f, "invalid index overflow uid"),
            Self::InvalidOverflowAppNum => write!(f, "invalid index overflow appnum"),
        }
    }
}

impl From<io::Error> for LoadHeaderError {
    fn from(io_err: io::Error) -> Self {
        Self::IO(io_err)
    }
}
