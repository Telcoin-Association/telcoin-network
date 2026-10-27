//! Engine errors as reth's [`DatabaseError`], with MDBX's error codes and messages (the crate
//! docs have the table).
//!
//! reth code and reth's tests compare a database error's [`DatabaseErrorInfo`] (a message and a
//! code) with `reth_libmdbx::Error::X.into()`, so each engine error maps to the MDBX error that
//! means the same thing, with MDBX's code and message. The codes are MDBX's (`mdbx.h`), the
//! errno-based ones from `libc`, so they match the platform's MDBX build.

use reth_db_api::DatabaseError;
use reth_storage_errors::db::{DatabaseErrorInfo, DatabaseWriteError, DatabaseWriteOperation};

/// An MDBX error: its code and the message `reth_libmdbx::Error` displays for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Mdbx {
    code: i32,
    message: &'static str,
}

impl Mdbx {
    /// `MDBX_KEYEXIST`.
    pub(crate) const KEY_EXIST: Self =
        Self { code: -30799, message: "key/data pair already exists" };
    /// `MDBX_NOTFOUND`.
    pub(crate) const NOT_FOUND: Self =
        Self { code: -30798, message: "no matching key/data pair found" };
    /// `MDBX_ENODATA` (the platform's `ENODATA`).
    pub(crate) const NO_DATA: Self =
        Self { code: libc::ENODATA, message: "the cursor is already at the end of data" };
    /// `MDBX_CORRUPTED`.
    pub(crate) const CORRUPTED: Self = Self { code: -30796, message: "database is corrupted" };
    /// `MDBX_PANIC`.
    pub(crate) const PANIC: Self = Self { code: -30795, message: "fatal environment error" };
    /// `MDBX_VERSION_MISMATCH`.
    pub(crate) const VERSION_MISMATCH: Self = Self { code: -30794, message: "DB version mismatch" };
    /// `MDBX_INVALID`.
    pub(crate) const INVALID: Self = Self { code: -30793, message: "file is not an MDBX file" };
    /// `MDBX_BAD_TXN`.
    pub(crate) const BAD_TXN: Self =
        Self { code: -30782, message: "transaction is not valid for requested operation" };
    /// `MDBX_BAD_VALSIZE`.
    pub(crate) const BAD_VALSIZE: Self = Self {
        code: -30781,
        message: "invalid size or alignment of key or data for the target database",
    };
    /// `MDBX_BUSY`.
    pub(crate) const BUSY: Self =
        Self { code: -30778, message: "another write transaction is running" };
    /// `MDBX_EKEYMISMATCH`.
    pub(crate) const KEY_MISMATCH: Self = Self {
        code: -30418,
        message: "the given key value is mismatched to the current cursor position",
    };
    /// `MDBX_EACCESS` (the platform's `EACCES`).
    pub(crate) const ACCESS: Self = Self {
        code: libc::EACCES,
        message: "the environment opened in read-only, check \
                  <https://reth.rs/run/troubleshooting.html> for more",
    };
    /// `MDBX_EINVAL` (the platform's `EINVAL`).
    pub(crate) const INVALID_PARAMETER: Self =
        Self { code: libc::EINVAL, message: "invalid parameter specified" };

    /// This error as reth's error information.
    pub(crate) fn info(self) -> DatabaseErrorInfo {
        DatabaseErrorInfo { message: self.message.into(), code: self.code }
    }

    /// This error's code with `message` in place of MDBX's.
    fn with_message(self, message: String) -> DatabaseErrorInfo {
        DatabaseErrorInfo { message: message.into(), code: self.code }
    }
}

/// The error information for `error` (see the module docs).
pub(crate) fn info(error: &tndb_b::Error) -> DatabaseErrorInfo {
    use tndb_b::Error as E;
    match error {
        E::KeyExists => Mdbx::KEY_EXIST.info(),
        E::AppendOutOfOrder => Mdbx::KEY_MISMATCH.info(),
        E::KeyTooLarge { .. } | E::ValueTooLarge { .. } => Mdbx::BAD_VALSIZE.info(),
        E::Corrupt(_) => Mdbx::CORRUPTED.info(),
        E::Locked => Mdbx::BUSY.info(),
        E::ReadOnly => Mdbx::ACCESS.info(),
        E::WrongTxn => Mdbx::BAD_TXN.info(),
        E::TableNotFound(_) | E::StaleTable(_) => Mdbx::NOT_FOUND.info(),
        E::InvalidTableName | E::InvalidPageSize(_) => Mdbx::INVALID_PARAMETER.info(),
        E::Failed(_) => Mdbx::PANIC.with_message(error.to_string()),
        E::NotTndbB => Mdbx::INVALID.with_message(error.to_string()),
        E::UnsupportedVersion(_) => Mdbx::VERSION_MISMATCH.with_message(error.to_string()),
        E::Io(io) => DatabaseErrorInfo {
            message: error.to_string().into(),
            code: io.raw_os_error().unwrap_or(libc::EIO),
        },
    }
}

/// A failed read, as [`DatabaseError::Read`].
pub(crate) fn read(error: tndb_b::Error) -> DatabaseError {
    DatabaseError::Read(info(&error))
}

/// A failed delete or clear, as [`DatabaseError::Delete`].
pub(crate) fn delete(error: tndb_b::Error) -> DatabaseError {
    DatabaseError::Delete(info(&error))
}

/// A failed write of `key` into table `table_name` by `operation`, as [`DatabaseError::Write`].
pub(crate) fn write(
    error: &tndb_b::Error,
    operation: DatabaseWriteOperation,
    table_name: &'static str,
    key: &[u8],
) -> DatabaseError {
    DatabaseWriteError { info: info(error), operation, table_name, key: key.to_vec() }.into()
}

/// The error for a write transaction already committed or aborted, met by one of its cursors.
pub(crate) const fn finished() -> tndb_b::Error {
    tndb_b::Error::WrongTxn
}

/// The error for a dup-sort table this crate does not store, and for a dup cursor call on a
/// table without duplicates.
pub(crate) fn dup_sort_unsupported(table: &'static str) -> DatabaseError {
    DatabaseError::Other(format!("tndb-b-reth: dup-sort table {table} is not supported"))
}

/// The error for a write of a dup-sort row this crate cannot store (a key not of the table's
/// width, or a value shorter than its subkey): MDBX's `MDBX_BAD_VALSIZE`, as a write error naming
/// the key.
pub(crate) fn bad_size(
    operation: DatabaseWriteOperation,
    table_name: &'static str,
    key: &[u8],
) -> DatabaseError {
    DatabaseWriteError { info: Mdbx::BAD_VALSIZE.info(), operation, table_name, key: key.to_vec() }
        .into()
}

/// The error for a write of a second value for a (key, subkey) that dup-sort table `table_name`
/// holds with another value: this crate stores one value per subkey (the crate docs' differences
/// from MDBX), so it refuses the write, which changed nothing, with MDBX's `MDBX_KEYEXIST` code and
/// a message of its own, as a write error naming the key.
pub(crate) fn second_dup_value(
    operation: DatabaseWriteOperation,
    table_name: &'static str,
    key: &[u8],
) -> DatabaseError {
    let message = format!(
        "tndb-b-reth stores one value per subkey in dup-sort table {table_name}, and this key and \
         subkey already hold a different value"
    );
    let info = Mdbx::KEY_EXIST.with_message(message);
    DatabaseWriteError { info, operation, table_name, key: key.to_vec() }.into()
}

/// The error for a table that does not exist, as MDBX reports a table it cannot open.
pub(crate) fn table_not_found() -> DatabaseError {
    DatabaseError::Open(Mdbx::NOT_FOUND.info())
}
