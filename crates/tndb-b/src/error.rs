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
    /// A value longer than a table accepts with its key: 16 MiB (in an overflow run) for a key
    /// that leaves room for the run's reference in its leaf, else what the leaf holds
    /// ([`crate::Env::max_value_len`]).
    #[error("tndb-b: value of {len} bytes exceeds the maximum of {max} for its key")]
    ValueTooLarge {
        /// The value's length.
        len: usize,
        /// The largest value accepted with the given key at this page size.
        max: usize,
    },
    /// An append ([`crate::RwTxn::append`], [`crate::RwCursor::append`],
    /// [`crate::TxnCursor::append`]) of a key that does not sort strictly after the table's last
    /// key. It is refused before anything changes.
    #[error("tndb-b: append of a key that does not sort after the table's last key")]
    AppendOutOfOrder,
    /// An insert ([`crate::RwTxn::insert`], [`crate::RwCursor::insert`],
    /// [`crate::TxnCursor::insert`]) of a key the table already holds. It is refused before
    /// anything changes and does not latch.
    #[error("tndb-b: insert of a key the table already holds")]
    KeyExists,
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
    /// Another handle (in this process or another) holds the database open: any handle, for
    /// [`crate::Env::open`]; one with a writer, for [`crate::Env::open_read_only`].
    #[error("tndb-b: database is locked by another handle")]
    Locked,
    /// A write transaction asked of an environment opened read-only
    /// ([`crate::Env::open_read_only`]), which has no writer. It changes nothing and does not
    /// latch.
    #[error("tndb-b: the environment is open read-only")]
    ReadOnly,
    /// A detached cursor ([`crate::TxnCursor`]) used with a write transaction other than the one
    /// that made it (a later one, even one that reuses the transaction id of an aborted one). It
    /// changes nothing and does not latch.
    #[error("tndb-b: a cursor used with a write transaction other than its own")]
    WrongTxn,
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
