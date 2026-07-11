//! Publishing staged snapshots to the object store.
//!
//! Currently hosts the shared publish path used by the `snapshot create` CLI; the full
//! epoch-boundary uploader service ([`publish_exported`]'s background caller) lands with the node
//! wiring.
//!
//! # Crash-safety contract
//!
//! The publish order is load-bearing: every artifact is uploaded first, then the manifest bytes,
//! and the `latest.json` pointer is written **last**. A reader following `latest.json` therefore
//! never sees a pointer into a half-uploaded epoch. If a publish fails at any point before the
//! pointer write, the pointer keeps naming the previous epoch.

use crate::{
    export::ExportedSnapshot,
    manifest::{Pointer, FORMAT_VERSION},
    store::SnapshotStore,
    SnapshotError, SnapshotResult,
};
use sha2::{Digest as _, Sha256};
use tn_types::{hex, now, Epoch};

/// Filename of the manifest staged next to the artifacts by the export step.
const MANIFEST_FILENAME: &str = "manifest.json";

/// What a successful publish uploaded.
///
/// Returned to the `snapshot create` CLI (to report what it published). The manifest and
/// `latest.json` pointer are not counted in [`artifacts`](Self::artifacts).
#[derive(Debug, Clone, Copy)]
pub struct PublishSummary {
    /// Number of artifact objects uploaded (excludes the manifest and pointer).
    pub artifacts: usize,
    /// Total uploaded artifact bytes.
    pub bytes: u64,
}

/// Upload a staged snapshot to the store, writing the `latest.json` pointer last.
///
/// This is the shared publish path, so the crash-safety ordering and integrity binding live in
/// exactly one place. It uploads every artifact in manifest order, re-checking each store-returned
/// `(size, sha256)` against its manifest entry so a mismatch aborts before the pointer is written.
/// It then uploads the staged `manifest.json` bytes verbatim — hashing exactly those bytes — so the
/// pointer's `manifest_sha256` binds precisely what a reader will fetch, and finally writes the
/// pointer. Any failure returns before the pointer write, leaving `latest.json` unchanged.
///
/// # Errors
///
/// Returns [`SnapshotError::Integrity`] on an artifact digest/size mismatch or a manifest/staged
/// count mismatch, and propagates store or filesystem errors.
pub async fn publish_exported(
    store: &SnapshotStore,
    chain_id: u64,
    epoch: Epoch,
    exported: &ExportedSnapshot,
) -> SnapshotResult<PublishSummary> {
    let manifest = &exported.manifest;

    // the staged file list must line up 1:1 with the manifest's artifact entries (same order)
    if manifest.artifacts.len() != exported.artifacts.len() {
        return Err(SnapshotError::Integrity(format!(
            "artifact count mismatch for epoch {epoch}: manifest lists {}, staged {}",
            manifest.artifacts.len(),
            exported.artifacts.len()
        )));
    }

    // 1. artifacts, verifying each upload against its manifest entry
    let mut total_bytes = 0u64;
    for (entry, path) in manifest.artifacts.iter().zip(exported.artifacts.iter()) {
        let key = SnapshotStore::artifact_key(epoch, &entry.name);
        let (size, sha256) = store.put_file(&key, path).await?;
        if size != entry.size || !sha256.eq_ignore_ascii_case(&entry.sha256) {
            return Err(SnapshotError::Integrity(format!(
                "artifact {} upload mismatch: manifest {}:{}, uploaded {size}:{sha256}",
                entry.name, entry.size, entry.sha256
            )));
        }
        total_bytes += size;
    }

    // 2. the staged manifest bytes verbatim, so the pointer's digest matches what readers fetch
    let manifest_bytes = tokio::fs::read(exported.staging_dir.join(MANIFEST_FILENAME)).await?;
    let manifest_sha256 = sha256_hex(&manifest_bytes);
    store.put_json_bytes(&SnapshotStore::manifest_key(epoch), manifest_bytes).await?;

    // 3. the pointer LAST: a reader never sees a pointer into a half-uploaded epoch
    let pointer = Pointer {
        format_version: FORMAT_VERSION,
        chain_id,
        epoch,
        manifest_key: SnapshotStore::manifest_key(epoch),
        manifest_sha256,
        updated_at: now(),
    };
    store.put_json_bytes(SnapshotStore::LATEST_KEY, pointer.to_json()?).await?;

    Ok(PublishSummary { artifacts: manifest.artifacts.len(), bytes: total_bytes })
}

/// Compute the lowercase-hex sha256 of `bytes` (matching the store's artifact digest encoding).
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
