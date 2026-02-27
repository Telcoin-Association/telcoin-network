//! Contains the error type for the flush() function.

use std::{error::Error, fmt, io};

/// Error from read_key().
#[derive(Debug)]
pub enum FlushError {
    /// Error writing to the data file.
    WriteData(io::Error),
    /// Database opened read-only.
    ReadOnly,
}

impl Error for FlushError {}

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::WriteData(e) => write!(f, "write data: {e}"),
            Self::ReadOnly => write!(f, "read only"),
        }
    }
}
