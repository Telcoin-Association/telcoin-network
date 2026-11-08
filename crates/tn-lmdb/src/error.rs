//! The crate's error type.

use std::io;

/// Every error tn-lmdb returns.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error from the page file or its directory.
    #[error("tn-lmdb: i/o error: {0}")]
    Io(#[from] io::Error),
    /// Damage found in the file: a page or meta that fails its checks. Found by a read, a write
    /// or [`crate::Env::check`], it latches the environment (later writes fail; reads continue);
    /// from [`crate::Env::open`] or [`crate::Env::open_read_only`] there is no environment to
    /// latch.
    #[error("tn-lmdb: corrupt database: {0}")]
    Corrupt(String),
    /// The environment is latched by an earlier failure (I/O, growth, commit or damage), or this
    /// operation's own failure latched it: the outcome of the failed commit is unknown until the
    /// file is reopened.
    #[error("tn-lmdb: environment failed: {0}")]
    Failed(String),
    /// The host refused to map the file: `mmap` failed with `ENOMEM` (the address space, `ulimit
    /// -v`, or the process's count of mappings exhausted, as many environments open at once can
    /// do). `bytes` is the length of the mapping asked for: the writer's reservation
    /// (`EnvOptions::map_reserve` at open, a larger one at a growth past it) or the file, for
    /// [`crate::Env::open_read_only`]. From a growth inside a write it latches the environment,
    /// as an I/O error does.
    #[error("tn-lmdb: out of memory: mapping {bytes} bytes of the file failed: {source}")]
    OutOfMemory {
        /// The length of the mapping refused.
        bytes: u64,
        /// The `mmap` error.
        source: io::Error,
    },
    /// The file has no page number left for a write: a file holds at most 2^32 − 1 pages (16 TiB
    /// at 4 KiB pages, 256 TiB at 64 KiB; [`crate::EnvOptions::page_size`]). It latches the
    /// environment, as an I/O error does; reads continue, and a reopen can still reuse the pages
    /// freed back to the free list.
    #[error("tn-lmdb: the file is full: {0}")]
    Full(String),
    /// A key longer than the page size allows, or an outer key longer than its dup table's
    /// `key_len` passed to [`crate::RwTxn::delete_prefix`] or to a dup cursor's bounded seek
    /// (`seek_key`, `seek_dup`, `seek_dup_with_key_tail`), `max` then the `key_len`.
    #[error("tn-lmdb: key of {len} bytes exceeds the maximum of {max}")]
    KeyTooLarge {
        /// The key's length.
        len: usize,
        /// The largest key this page size (or this dup table's outer key width) allows.
        max: usize,
    },
    /// A key too short for its dup table: a point operation's key with no subkey (at most
    /// `key_len` bytes), or an outer key shorter than `key_len` passed to
    /// [`crate::RwTxn::delete_prefix`] or to a dup cursor's bounded seek (`seek_key`, `seek_dup`,
    /// `seek_dup_with_key_tail`). It is refused before anything changes and does not latch.
    #[error("tn-lmdb: key of {len} bytes is shorter than the minimum of {min}")]
    KeyTooShort {
        /// The key's length.
        len: usize,
        /// The shortest key the call accepts for this table.
        min: usize,
    },
    /// A value longer than a table accepts with its key: 16 MiB (in an overflow run) for every key
    /// within the key limit ([`crate::Env::max_value_len`]).
    #[error("tn-lmdb: value of {len} bytes exceeds the maximum of {max} for its key")]
    ValueTooLarge {
        /// The value's length.
        len: usize,
        /// The largest value accepted with the given key at this page size.
        max: usize,
    },
    /// An append ([`crate::RwTxn::append`], [`crate::RwCursor::append`],
    /// [`crate::TxnCursor::append`], [`crate::DupTxnCursor::append`]) of a key that does not sort
    /// strictly after the table's last key. It is refused before anything changes.
    #[error("tn-lmdb: append of a key that does not sort after the table's last key")]
    AppendOutOfOrder,
    /// An insert ([`crate::RwTxn::insert`], [`crate::RwCursor::insert`],
    /// [`crate::TxnCursor::insert`], [`crate::DupTxnCursor::insert`]) of a key the table already
    /// holds. It is refused before anything changes and does not latch.
    #[error("tn-lmdb: insert of a key the table already holds")]
    KeyExists,
    /// No table with this name exists.
    #[error("tn-lmdb: table {0:?} not found")]
    TableNotFound(String),
    /// The table handle refers to a table that was dropped (and perhaps re-created) since the
    /// handle was obtained.
    #[error("tn-lmdb: table handle {0:?} is stale")]
    StaleTable(String),
    /// A call that needs the other kind of table: [`crate::RwTxn::create_table`] on a dup table,
    /// [`crate::RwTxn::create_dup_table`] on a plain table or on a dup table of other options, a
    /// plain cursor on a dup table, or [`crate::RwTxn::delete_prefix`] on a plain table. It
    /// changes nothing and does not latch.
    #[error("tn-lmdb: table {0:?} is of the other kind for this call")]
    TableKind(String),
    /// [`crate::DupOptions`] this build does not accept (see [`crate::RwTxn::create_dup_table`]).
    /// It changes nothing and does not latch.
    #[error("tn-lmdb: invalid dup table options: {0}")]
    InvalidDupOptions(String),
    /// A table name that is empty or longer than 255 bytes.
    #[error("tn-lmdb: invalid table name (must be 1..=255 bytes)")]
    InvalidTableName,
    /// The file is not empty and is not a tn-lmdb database.
    #[error("tn-lmdb: not a tn-lmdb database")]
    NotTnLmdb,
    /// The file is a tn-lmdb database of a format version this build does not read.
    #[error("tn-lmdb: unsupported format version {0}")]
    UnsupportedVersion(u32),
    /// A page size that is not a power of two in 4 KiB..=64 KiB.
    #[error("tn-lmdb: invalid page size {0}")]
    InvalidPageSize(u32),
    /// An `EnvOptions::max_growth` under [`crate::MIN_GROWTH`] (1 MiB), refused by
    /// [`crate::Env::open`] before it touches the file.
    #[error("tn-lmdb: invalid max growth {0} (at least {} bytes)", crate::MIN_GROWTH)]
    InvalidMaxGrowth(u64),
    /// An `EnvOptions::map_reserve` past `isize::MAX` bytes, which no mapping can reserve,
    /// refused by [`crate::Env::open`] before it touches the file.
    #[error("tn-lmdb: invalid map reserve {0} (at most {} bytes)", isize::MAX)]
    InvalidMapReserve(u64),
    /// Another handle (in this process or another) holds the database open: any handle, for
    /// [`crate::Env::open`]; one with a writer, for [`crate::Env::open_read_only`].
    #[error("tn-lmdb: database is locked by another handle")]
    Locked,
    /// A write transaction asked of an environment opened read-only
    /// ([`crate::Env::open_read_only`]), which has no writer. It changes nothing and does not
    /// latch.
    #[error("tn-lmdb: the environment is open read-only")]
    ReadOnly,
    /// A detached cursor ([`crate::TxnCursor`], [`crate::DupTxnCursor`]) used with a write
    /// transaction other than the one that made it (a later one, even one that reuses the
    /// transaction id of an aborted one). It changes nothing and does not latch.
    #[error("tn-lmdb: a cursor used with a write transaction other than its own")]
    WrongTxn,
}

/// A `Result` with [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// True for an error that latches the environment it happens in: damage, an I/O error, a
    /// mapping refused for want of memory, a full file, a broken invariant or an environment
    /// latched already (after it, writes and commits return [`Error::Failed`] until a reopen; a
    /// write cursor it meets is left unset). Validation errors and an open's refusals change
    /// nothing and do not latch.
    pub fn latches(&self) -> bool {
        match self {
            Error::Corrupt(_)
            | Error::Io(_)
            | Error::Failed(_)
            | Error::OutOfMemory { .. }
            | Error::Full(_) => true,
            Error::KeyTooLarge { .. }
            | Error::KeyTooShort { .. }
            | Error::ValueTooLarge { .. }
            | Error::AppendOutOfOrder
            | Error::KeyExists
            | Error::TableNotFound(_)
            | Error::StaleTable(_)
            | Error::TableKind(_)
            | Error::InvalidDupOptions(_)
            | Error::InvalidTableName
            | Error::NotTnLmdb
            | Error::UnsupportedVersion(_)
            | Error::InvalidPageSize(_)
            | Error::InvalidMaxGrowth(_)
            | Error::InvalidMapReserve(_)
            | Error::Locked
            | Error::ReadOnly
            | Error::WrongTxn => false,
        }
    }
}

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
