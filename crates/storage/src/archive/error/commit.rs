//! Contains error when committing.

use std::{error::Error, fmt, io};

use crate::archive::error::flush::FlushError;

/// Error from commit().
#[derive(Debug)]
pub enum CommitError {
    /// An error flushing any cached data.
    Flush(FlushError),
    /// An io error occured syncing the data file.
    DataFileSync(io::Error),
    /// DB is opened read-only.
    ReadOnly,
}

impl Error for CommitError {}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::Flush(e) => write!(f, "flush: {e}"),
            Self::DataFileSync(io_err) => write!(f, "data sync: {io_err}"),
            Self::ReadOnly => write!(f, "read only"),
        }
    }
}
