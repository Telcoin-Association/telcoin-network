//! Utility functions for node startup and shutdown, for example path parsing.

use alloy_rpc_types_engine::{JwtError, JwtSecret};
use std::{
    env::VarError,
    path::{Path, PathBuf},
};
use tracing::{debug, info};

/// Parses a user-specified path with support for environment variables and common shorthands (e.g.
/// ~ for the user's home directory).
pub fn parse_path(value: &str) -> Result<PathBuf, shellexpand::LookupError<VarError>> {
    shellexpand::full(value).map(|path| PathBuf::from(path.into_owned()))
}

/// Attempts to retrieve or create a JWT secret from the specified path.
pub fn get_or_create_jwt_secret_from_path(path: &Path) -> Result<JwtSecret, JwtError> {
    if path.exists() {
        debug!(target: "reth::cli", ?path, "Reading JWT auth secret file");
        JwtSecret::from_file(path)
    } else {
        info!(target: "reth::cli", ?path, "Creating JWT auth secret file");
        JwtSecret::try_create_random(path)
    }
}

// TN(TN-0008): `get_single_header` and `get_single_body` (fetching blocks from devp2p peers) are
// removed.
