//! Cloud snapshot support for telcoin-network.
//!
//! This crate produces, uploads, verifies, and restores signed chain snapshots so a node
//! can bootstrap from remote object storage instead of replaying consensus from genesis. It
//! is organized into focused modules:
//!
//! - [`manifest`]: the snapshot manifest format and its (de)serialization.
//! - [`store`]: object-store construction and access from a snapshot URL (S3, GCS, Azure, HTTP, or
//!   local file).
//! - [`export`]: packaging local chain data into an uploadable snapshot.
//! - [`verify`]: trustless verification of a downloaded snapshot against a local trust root.
//! - [`restore`]: laying a verified snapshot down onto an empty datadir.
//! - [`service`]: the long-running task that periodically exports and uploads snapshots.
//!
//! The crate's entire error surface is [`SnapshotError`] (aliased as [`SnapshotResult`]), and
//! [`has_chain_data`] is the shared precondition guarding restore against clobbering a datadir
//! that already holds chain data.

// Used only by the standalone integration-test binaries under `tests/`.
#[cfg(test)]
mod clippy {
    use proptest as _;
}

use std::{path::Path, time::Duration};
use tn_config::TelcoinDirs;

pub mod export;
pub mod manifest;
pub mod restore;
pub mod service;
pub mod store;
pub mod verify;

// metrics are internal to the crate
pub(crate) mod metrics;

/// Filename of the marker written under [`TelcoinDirs::snapshots_path`] while a restore is in
/// progress.
///
/// Its presence means a prior restore was interrupted, so the on-disk data cannot be trusted.
/// [`has_chain_data`] reports `true` whenever it exists, which makes both manual and automatic
/// restore refuse to run until an operator clears the datadir.
pub const RESTORE_INCOMPLETE_MARKER: &str = ".restore-incomplete";

/// Default number of most-recent remote snapshots to retain; older ones are pruned.
pub const DEFAULT_KEEP_LAST: u32 = 3;

/// Default deadline for a single snapshot upload-and-certify cycle.
pub const DEFAULT_CERT_TIMEOUT: Duration = Duration::from_secs(300);

/// The error type for every fallible snapshot operation.
///
/// This enum is the crate's complete error surface: every module returns [`SnapshotResult`],
/// and the variants below intentionally cover export, upload, verification, and restore so no
/// module needs a bespoke error type. [`SnapshotError::Other`] is the escape hatch for errors
/// that do not fit a specific variant.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Local filesystem I/O failed while staging, reading, or writing snapshot data.
    #[error("snapshot io error: {0}")]
    Io(#[from] std::io::Error),

    /// The remote object store (S3, GCS, Azure, or HTTP) returned an error.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// A manifest or metadata document failed JSON (de)serialization.
    #[error("json (de)serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// The configured snapshot URL could not be parsed.
    #[error("invalid snapshot url: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// The snapshot URL parsed but its scheme is not a supported object-store backend.
    ///
    /// Supported schemes are `s3`, `gs`, `az`/`azure`, `http`/`https`, and `file`.
    #[error("unsupported snapshot url scheme: {0}")]
    UnsupportedScheme(String),

    /// A manifest is structurally invalid: an unknown version, an unexpected kind, or a
    /// chain/genesis binding that does not match this node.
    #[error("invalid snapshot manifest: {0}")]
    Manifest(String),

    /// Trustless verification failed: a broken epoch-record chain, a header mismatch, or an
    /// aggregate BLS signature that did not validate.
    #[error("snapshot verification failed: {0}")]
    Verification(String),

    /// A downloaded object failed an integrity check: a sha256 digest mismatch, a size
    /// mismatch, or a decompressed size that exceeded the zstd anti-bomb cap.
    #[error("snapshot integrity check failed: {0}")]
    Integrity(String),

    /// Restore refused to run because the datadir already holds chain data.
    #[error("chain data already exists; refusing to overwrite on restore")]
    ChainDataExists,

    /// No local trust root was found: the genesis or `committee.yaml` file needed to anchor
    /// verification is absent.
    #[error("missing trust root: genesis/committee configuration not found")]
    MissingTrustRoot,

    /// A previous restore did not finish: the [`RESTORE_INCOMPLETE_MARKER`] is present, so the
    /// on-disk data is in an inconsistent state.
    #[error("restore did not complete; datadir is in an inconsistent state")]
    RestoreIncomplete,

    /// An operation exceeded its deadline.
    #[error("snapshot operation timed out: {0}")]
    Timeout(String),

    /// Catch-all for errors that do not fit a more specific variant.
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

/// Result alias for fallible snapshot operations.
pub type SnapshotResult<T> = Result<T, SnapshotError>;

/// Configuration for uploading snapshots to a remote object store.
///
/// Constructed by the CLI from operator-supplied flags. [`UploadConfig::new`] fills
/// [`DEFAULT_KEEP_LAST`] and [`DEFAULT_CERT_TIMEOUT`] for everything but the URL.
#[derive(Clone, Debug)]
pub struct UploadConfig {
    /// Base URL of the object store to upload snapshots to.
    ///
    /// Supported schemes are `s3`, `gs`, `az`/`azure`, `http`/`https`, and `file`.
    pub url: String,
    /// Number of most-recent snapshots to retain remotely before older ones are pruned.
    pub keep_last: u32,
    /// Deadline for a single upload-and-certify cycle before it is abandoned.
    pub cert_timeout: Duration,
}

impl UploadConfig {
    /// Create an upload configuration for `url` using the default retention and timeout.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), ..Default::default() }
    }
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            keep_last: DEFAULT_KEEP_LAST,
            cert_timeout: DEFAULT_CERT_TIMEOUT,
        }
    }
}

/// Returns `true` if the node's datadir already contains chain data that a restore must not
/// overwrite.
///
/// This is the precondition guarding both manual restore and node-startup auto-restore: a
/// snapshot may only be laid down onto an empty datadir. It reports `true` when ANY of the
/// following holds:
///
/// - the reth database directory exists and is non-empty,
/// - the reth `static_files` directory exists and is non-empty,
/// - the consensus database directory exists and is non-empty, or
/// - a [`RESTORE_INCOMPLETE_MARKER`] file is present under
///   [`snapshots_path`](TelcoinDirs::snapshots_path).
///
/// The marker check is deliberately independent of the data-directory checks: an interrupted
/// restore may have written partial data, so its presence alone is enough to refuse another
/// restore until an operator intervenes.
pub fn has_chain_data(dirs: &impl TelcoinDirs) -> bool {
    let reth_db = dirs.reth_db_path();
    // reth's static_files directory sits alongside the reth db under the datadir root
    let static_files = reth_db.parent().map(|root| root.join("static_files"));

    dir_has_entries(&reth_db)
        || static_files.is_some_and(|p| dir_has_entries(&p))
        || dir_has_entries(&dirs.consensus_db_path())
        || dirs.snapshots_path().join(RESTORE_INCOMPLETE_MARKER).exists()
}

/// Returns `true` if `path` is a directory containing at least one entry.
///
/// A missing or unreadable path, or an empty directory, returns `false`.
fn dir_has_entries(path: &Path) -> bool {
    std::fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};
    use tempfile::tempdir;

    /// Bind a tempdir to an owned `PathBuf` so it satisfies the `'static` bound on the blanket
    /// [`TelcoinDirs`] impl.
    fn datadir(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().to_path_buf()
    }

    #[test]
    fn empty_datadir_has_no_chain_data() {
        let dir = tempdir().unwrap();
        assert!(!has_chain_data(&datadir(&dir)));
    }

    #[test]
    fn present_but_empty_db_dir_has_no_chain_data() {
        let dir = tempdir().unwrap();
        let dd = datadir(&dir);
        // an empty reth db directory must not count as chain data
        fs::create_dir_all(dd.reth_db_path()).unwrap();
        fs::create_dir_all(dd.consensus_db_path()).unwrap();
        assert!(!has_chain_data(&dd));
    }

    #[test]
    fn non_empty_reth_db_counts_as_chain_data() {
        let dir = tempdir().unwrap();
        let dd = datadir(&dir);
        let db = dd.reth_db_path();
        fs::create_dir_all(&db).unwrap();
        fs::write(db.join("mdbx.dat"), b"x").unwrap();
        assert!(has_chain_data(&dd));
    }

    #[test]
    fn non_empty_static_files_counts_as_chain_data() {
        let dir = tempdir().unwrap();
        let dd = datadir(&dir);
        let static_files = dd.reth_db_path().parent().unwrap().join("static_files");
        fs::create_dir_all(&static_files).unwrap();
        fs::write(static_files.join("headers.static"), b"x").unwrap();
        assert!(has_chain_data(&dd));
    }

    #[test]
    fn non_empty_consensus_db_counts_as_chain_data() {
        let dir = tempdir().unwrap();
        let dd = datadir(&dir);
        let cdb = dd.consensus_db_path();
        fs::create_dir_all(&cdb).unwrap();
        fs::write(cdb.join("consensus.redb"), b"x").unwrap();
        assert!(has_chain_data(&dd));
    }

    #[test]
    fn restore_incomplete_marker_counts_as_chain_data() {
        let dir = tempdir().unwrap();
        let dd = datadir(&dir);
        let snapshots = dd.snapshots_path();
        fs::create_dir_all(&snapshots).unwrap();
        fs::write(snapshots.join(RESTORE_INCOMPLETE_MARKER), b"").unwrap();
        assert!(has_chain_data(&dd));
    }
}
