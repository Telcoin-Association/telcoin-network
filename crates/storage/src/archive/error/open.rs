//! Errors when opening.

use std::{error::Error, fmt, io};

use crate::archive::error::load_header::LoadHeaderError;

/// Error on opening a DB.
#[derive(Debug)]
pub enum OpenError {
    /// Error opening the data file.
    DataFileOpen(LoadHeaderError),
    /// Error reading the data file (priming the read buffer for instance).
    DataReadError(io::Error),
    /// Error opening the index file.
    IndexFileOpen(LoadHeaderError),
    /// An seeking in the data file (this should be hard to get).
    Seek(io::Error),
    /// Tried to open using files that were invalid for some reason.
    InvalidFiles,
    /// DB was not closed cleanly (data file length and index record mismatch).
    InvalidShutdown,
}

impl Error for OpenError {}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::DataFileOpen(e) => write!(f, "data open failed: {e}"),
            Self::DataReadError(e) => write!(f, "data read failed: {e}"),
            Self::IndexFileOpen(e) => write!(f, "index open failed: {e}"),
            Self::Seek(e) => write!(f, "seek: {e}"),
            Self::InvalidFiles => write!(f, "invalid files"),
            Self::InvalidShutdown => write!(f, "invalid shutdown"),
        }
    }
}
