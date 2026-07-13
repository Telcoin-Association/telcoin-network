//! Cloud snapshot support for telcoin-network.
//!
//! A fresh node normally reaches the chain tip by replaying consensus from genesis. This crate is
//! an opt-in shortcut: an observer node publishes a signed, self-describing snapshot of the chain
//! state at each epoch boundary, and a new node downloads, cryptographically verifies, and installs
//! one so it starts at the last epoch boundary instead of from genesis. Snapshots are state-only —
//! the restored node holds the final EVM state and the checkpoints needed to resume consensus, not
//! the full pre-snapshot history. The trust model below is what makes accepting state from an
//! untrusted bucket safe; the limitations at the end are what it costs.
//!
//! # Trust model
//!
//! The sole trust root is the node's LOCAL configuration — the genesis committee in
//! `committee.yaml` and the genesis block hash — never anything fetched from the bucket. Restore
//! and verify refuse to run without it ([`SnapshotError::MissingTrustRoot`]). Verification then
//! re-establishes trust outward from that root as one unbroken chain of bindings:
//!
//! 1. **Signed epoch-record chain.** The bucket ships the full
//!    [`EpochRecord`](tn_types::EpochRecord)/[`EpochCertificate`](tn_types::EpochCertificate) chain
//!    for epochs `0..=N`. Epoch `0`'s record must commit to the local genesis committee with a zero
//!    `parent_hash`; every later record must chain to its predecessor by `parent_hash` and inherit
//!    the predecessor's `next_committee`; and every record must carry an aggregate-BLS certificate
//!    signed by a super-quorum (`2/3 + 1` of committee members; all members carry equal voting
//!    power) of *its own* committee. Committee `N` thereby certifies who committee `N+1` is, so
//!    trust walks from genesis to the snapshot epoch without ever trusting the bucket.
//! 2. **Manifest binding.** Record `N`'s digest, `final_state` (the closing execution block number
//!    and hash), and `final_consensus` must equal the [`Manifest`](manifest::Manifest)'s. The
//!    certified chain now pins the exact boundary block the snapshot claims.
//! 3. **Header window.** The shipped execution headers must be non-empty, consecutively numbered,
//!    internally hash-linked, and end exactly at `final_state`, tying the certified boundary block
//!    back through a verifiable header chain.
//! 4. **From-scratch state-root recompute.** The plain-state dump is rebuilt from scratch and its
//!    recomputed root hard-checked against the certified boundary header's `state_root`. `restore`
//!    does this as it imports the state into the datadir; `verify` does it too, by default, as a
//!    dry-run rebuild into a throwaway datadir it discards afterward (opt out with
//!    `--skip-state-root`, which then trusts each chunk's sha256 alone). Either way `tn-reth`
//!    scaffolds the boundary header from the verified window, re-imports the dump, recomputes the
//!    state root FROM SCRATCH, and hard-fails on any mismatch (restore additionally re-checks that
//!    the reconstructed tip hash equals `final_state`). Because the BLS-certified block hash
//!    already commits to that state root, tampered state cannot survive the recompute — the sha256
//!    and decompression checks alone only pin the bytes, not the state they decode to.
//!
//! Every check compares typed values, never serialized strings: a digest and a hash have more than
//! one textual encoding, so string comparison would be both wrong and fragile.
//!
//! What this leaves an adversary: a malicious bucket can NEVER forge a snapshot for a different
//! committee or serve tampered state — each attempt fails a signature, chain, header, or
//! state-root check. It CAN only withhold the newest snapshot and serve an OLDER, still-valid one,
//! because freshness is unauthenticated: nothing signs "this is the latest epoch". Pinning the
//! epoch (the CLI's `--epoch`) bypasses the `latest.json` pointer and defeats withholding.
//!
//! # Module map
//!
//! - [`manifest`] — the two JSON documents that describe a bucket ([`Manifest`](manifest::Manifest)
//!   per epoch, [`Pointer`](manifest::Pointer) at the root) plus the validation gates every
//!   consumer parses through.
//! - [`store`] — [`SnapshotStore`](store::SnapshotStore): object-store construction from a URL (S3,
//!   GCS, Azure, HTTP, or local file) with per-scheme credentials read from the environment, the
//!   canonical bucket key layout, streaming integrity-checked transfers, and zstd helpers with an
//!   anti-bomb decompression cap.
//! - [`export`] — [`export_epoch`](export::export_epoch): stage the artifact set (chunked state,
//!   header window, epoch-record chain) and the manifest for one closed epoch, and compute the
//!   `fee_derivable` precheck. It stages to a local directory but never uploads.
//! - [`verify`] — [`verify_snapshot`](verify::verify_snapshot): the full trust gate above,
//!   returning a [`VerifiedSnapshot`](verify::VerifiedSnapshot) as proof the checks passed.
//! - [`restore`] — [`restore_from_snapshot`](restore::restore_from_snapshot): orchestrate download
//!   → verify → install onto an empty datadir, with strict preconditions and crash-atomic failure
//!   handling.
//! - [`service`] — the observer-only [`SnapshotUploader`](service::SnapshotUploader): an
//!   epoch-boundary state machine that pins, exports, uploads, and prunes, writing the pointer LAST
//!   so a crash never publishes a half-uploaded epoch.
//! - `metrics` (crate-internal) — Prometheus series under the `tn_snapshot` scope.
//!
//! # Bucket layout
//!
//! ```text
//! <bucket>/<prefix>/
//!   latest.json                 # Pointer to the newest epoch; written LAST
//!   epoch-0000000042/           # one directory per snapshot, epoch zero-padded to 10 digits
//!     manifest.json             # Manifest: chain/genesis binding, checkpoints, artifact digests
//!     state-000.jsonl.zst       # reth init-state JSONL, split at line boundaries, zstd-compressed
//!     state-001.jsonl.zst       # ...one or more chunks, concatenating back to the state dump
//!     headers.json.zst          # the header window as a JSON array of headers, zstd-compressed
//!     epoch-records.bcs.zst     # Vec<(EpochRecord, EpochCertificate)> for epochs 0..=N, BCS + zstd
//! ```
//!
//! # Flows
//!
//! **Upload** (observer, once per boundary).
//! [`on_epoch_closed`](service::SnapshotUploader::on_epoch_closed) fires inside the boundary quiet
//! window. Synchronously it claims a single-flight gate and pins the closing epoch's state view,
//! then spawns the heavy work off the consensus path: await epoch `N`'s certificate, call
//! [`export_epoch`](export::export_epoch) to stage the artifacts and manifest, upload every
//! artifact, upload the manifest, and write `latest.json` LAST, then prune to `keep_last`. Any
//! failure, skip, or shutdown before the pointer write leaves the pointer naming the previous
//! epoch. An epoch whose `fee_derivable` precheck fails is skipped rather than published.
//!
//! **Restore** (fresh node, manual or on startup).
//! [`restore_from_snapshot`](restore::restore_from_snapshot) checks its preconditions (local trust
//! root present, no existing chain data, no leftover marker), resolves the target epoch (pinned or
//! via `latest.json`), and downloads into a staging directory. It runs
//! [`verify_snapshot`](verify::verify_snapshot); then, under a [`RESTORE_INCOMPLETE_MARKER`] that
//! guards failure atomicity, it imports the verified state into reth, installs the epoch-record
//! chain, seeds the consensus stores to the boundary, writes a receipt, and clears the marker. A
//! failure before install wipes staging only and leaves the datadir untouched; a failure during
//! install deletes the partial chain data this run created but KEEPS the marker, quarantining the
//! datadir until an operator clears it.
//!
//! # Limitations
//!
//! - A restored node serves no pre-snapshot history: it holds the final state and a bounded header
//!   window by design, not the full chain.
//! - The uploader skips an epoch whose EIP-1559 base fees are not derivable from the captured state
//!   — a fee-configured worker that produced no block that epoch leaves a restored node with no
//!   chain-observable anchor to resume fee calculation from.
//! - Freshness is unauthenticated (see the trust model's withholding caveat); pin `--epoch` to
//!   defeat a withholding bucket.
//! - After a failed restore the datadir stays quarantined by the [`RESTORE_INCOMPLETE_MARKER`];
//!   clearing it is a deliberate manual step.
//!
//! # Errors and shared preconditions
//!
//! The crate's entire error surface is [`SnapshotError`] (aliased as [`SnapshotResult`]);
//! [`SnapshotError::Other`] is the escape hatch for anything without a specific variant.
//! [`has_chain_data`] is the shared precondition guarding both manual restore and startup
//! auto-restore against clobbering a datadir that already holds chain data.

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

    /// The resolved snapshot epoch is below the operator-configured minimum, so restore refused
    /// it.
    ///
    /// A bucket cannot forge a snapshot for the wrong committee, but it CAN serve an older,
    /// still-valid one in place of the newest. An operator-set floor (`--snapshot-min-epoch`)
    /// turns that unauthenticated-freshness gap into a hard bound on how far back an
    /// attacker-chosen start may be.
    #[error(
        "resolved snapshot epoch {resolved} is below the configured minimum epoch {min_epoch}; \
         the bucket may be withholding newer snapshots"
    )]
    EpochTooOld {
        /// The snapshot epoch resolution selected, whether pinned or taken from `latest.json`.
        resolved: u32,
        /// The operator-configured minimum epoch the restore requires.
        min_epoch: u32,
    },

    /// An operation exceeded its deadline.
    #[error("snapshot operation timed out: {0}")]
    Timeout(String),

    /// Catch-all for errors that do not fit a more specific variant.
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

impl SnapshotError {
    /// Returns `true` if this error is transient — a retry on the same input might succeed.
    ///
    /// Boot-time auto-restore uses this to decide whether to retry a failed download. A flaky
    /// object store (throttling, a 5xx, a dropped connection) or a [`Timeout`](Self::Timeout)
    /// is worth retrying; a deterministic failure is not. A
    /// [`NotFound`](object_store::Error::NotFound) is deliberately treated as PERMANENT: on a
    /// read-after-write-consistent store a missing `latest.json` or artifact will not appear
    /// within a boot's retry budget, so retrying only delays a startup that should fail fast.
    /// Every verification, integrity, manifest, or precondition failure is deterministic and
    /// therefore permanent — including [`EpochTooOld`](Self::EpochTooOld), which reflects
    /// operator policy, not a flaky source.
    pub fn is_transient(&self) -> bool {
        match self {
            // a missing object won't materialize within a boot's retry budget on a
            // read-after-write-consistent store, so NotFound is permanent; every other object-store
            // failure (throttling, 5xx, connection resets) is worth retrying.
            SnapshotError::ObjectStore(object_store::Error::NotFound { .. }) => false,
            SnapshotError::ObjectStore(_) | SnapshotError::Timeout(_) => true,
            _ => false,
        }
    }
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

    #[tokio::test]
    async fn transience_classification() {
        use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStoreExt as _};

        // a timeout is transient: the deadline may have been a blip.
        assert!(SnapshotError::Timeout("slow".into()).is_transient());

        // a real NotFound is permanent: a read-after-write-consistent bucket will not surface the
        // missing object within a boot's retry budget. obtain a genuine NotFound from the store
        // rather than constructing one, so the arm is tested against the real variant shape.
        let not_found = InMemory::new().get(&ObjPath::from("missing")).await.unwrap_err();
        assert!(matches!(not_found, object_store::Error::NotFound { .. }), "expected NotFound");
        assert!(!SnapshotError::ObjectStore(not_found).is_transient());

        // any other object-store failure (throttling, 5xx, connection reset) is transient.
        let generic = object_store::Error::Generic { store: "test", source: "boom".into() };
        assert!(SnapshotError::ObjectStore(generic).is_transient());

        // every deterministic error is permanent: a retry cannot change the outcome.
        for err in [
            SnapshotError::Verification("bad sig".into()),
            SnapshotError::Integrity("digest mismatch".into()),
            SnapshotError::Manifest("bad layout".into()),
            SnapshotError::ChainDataExists,
            SnapshotError::MissingTrustRoot,
            SnapshotError::RestoreIncomplete,
            SnapshotError::Io(std::io::Error::other("disk full")),
            SnapshotError::EpochTooOld { resolved: 3, min_epoch: 5 },
        ] {
            assert!(!err.is_transient(), "{err:?} must be permanent");
        }
    }
}
