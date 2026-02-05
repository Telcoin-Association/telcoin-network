//! Errors when renaming a file.

use std::{error::Error, fmt, io};

/// Error renaming a DB.
#[derive(Debug)]
pub enum RenameError {
    /// One or more of the target named files already exist.
    FilesExist,
    /// Error renaming the data file.
    RenameIO(io::Error),
    /// Rename is not supported because file paths were individual set by user.
    CanNotRename,
}

impl Error for RenameError {}

impl fmt::Display for RenameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::FilesExist => write!(f, "target directory for rename already exist"),
            Self::RenameIO(e) => write!(f, "rename failed: {e}"),
            Self::CanNotRename => write!(f, "rename not supported for manually set file paths"),
        }
    }
}
