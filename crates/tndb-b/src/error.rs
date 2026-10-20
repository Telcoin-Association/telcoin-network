//! The crate's error type.

use std::io;

/// Every error tndb-b returns.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error from the page file or its directory.
    #[error("tndb-b: i/o error: {0}")]
    Io(#[from] io::Error),
    /// Damage found in the file: a page or meta that fails its checks. The environment is
    /// latched (later writes fail); reads continue.
    #[error("tndb-b: corrupt database: {0}")]
    Corrupt(String),
    /// The environment is latched by an earlier failure (I/O, growth, commit or damage), or this
    /// operation's own failure latched it: the outcome of the failed commit is unknown until the
    /// file is reopened.
    #[error("tndb-b: environment failed: {0}")]
    Failed(String),
    /// A key longer than the page size allows.
    #[error("tndb-b: key of {len} bytes exceeds the maximum of {max}")]
    KeyTooLarge {
        /// The key's length.
        len: usize,
        /// The largest key this page size allows.
        max: usize,
    },
    /// A value longer than the page size allows for its key.
    #[error("tndb-b: value of {len} bytes exceeds the maximum of {max} for its key")]
    ValueTooLarge {
        /// The value's length.
        len: usize,
        /// The largest value this page size allows with the given key.
        max: usize,
    },
    /// An append ([`crate::RwTxn::append`], [`crate::RwCursor::append`]) of a key that does not
    /// sort strictly after the table's last key. It is refused before anything changes.
    #[error("tndb-b: append of a key that does not sort after the table's last key")]
    AppendOutOfOrder,
    /// No table with this name exists.
    #[error("tndb-b: table {0:?} not found")]
    TableNotFound(String),
    /// The table handle refers to a table that was dropped (and perhaps re-created) since the
    /// handle was obtained.
    #[error("tndb-b: table handle {0:?} is stale")]
    StaleTable(String),
    /// A table name that is empty or longer than 255 bytes.
    #[error("tndb-b: invalid table name (must be 1..=255 bytes)")]
    InvalidTableName,
    /// The file is not empty and is not a tndb-b database.
    #[error("tndb-b: not a tndb-b database")]
    NotTndbB,
    /// The file is a tndb-b database of a format version this build does not read.
    #[error("tndb-b: unsupported format version {0}")]
    UnsupportedVersion(u32),
    /// A page size that is not a power of two in 4 KiB..=64 KiB.
    #[error("tndb-b: invalid page size {0}")]
    InvalidPageSize(u32),
    /// Another handle (in this process or another) holds the database open.
    #[error("tndb-b: database is locked by another handle")]
    Locked,
}

/// A `Result` with [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_errors_convert() {
        let e: Error = io::Error::other("boom").into();
        assert!(matches!(e, Error::Io(_)));
        assert!(e.to_string().contains("boom"));
    }
}
