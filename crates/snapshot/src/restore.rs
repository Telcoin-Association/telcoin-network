//! Restoring a verified snapshot onto an empty datadir.
//!
//! [`restore_from_snapshot`] is the orchestration the `snapshot restore` CLI subcommand and the
//! node's startup auto-restore both drive. It downloads a snapshot from remote object storage,
//! re-establishes trust against the local genesis/committee, and lays the verified state down onto
//! an empty datadir — never overwriting existing chain data. [`verify_snapshot_source`] runs the
//! same download-and-verify path but stops short of installing anything, so an operator can vet a
//! bucket before committing to a restore.
//!
//! # Trust
//!
//! The only trust anchor is the node's LOCAL configuration: the genesis (`genesis.yaml`) and the
//! genesis committee (`committee.yaml`). Both must be present or restore refuses with
//! [`SnapshotError::MissingTrustRoot`] — a restore never fabricates a trust root from the untrusted
//! bucket. Everything downloaded is checked against that anchor by
//! [`verify_snapshot`] before a single byte reaches the datadir.
//!
//! # Withholding caveat
//!
//! When no epoch is pinned, the newest snapshot is chosen via the bucket's `latest.json` pointer. A
//! malicious bucket cannot forge a snapshot for a different committee, but it CAN withhold the
//! newest one and serve an older, still-valid snapshot. Passing an explicit `epoch` pins the choice
//! and bypasses the pointer. The chosen epoch is always logged prominently.
//!
//! # Failure atomicity
//!
//! Installation begins when the [`RESTORE_INCOMPLETE_MARKER`] is written under
//! [`snapshots_path`](tn_config::TelcoinDirs::snapshots_path). A failure BEFORE that point only
//! wipes the download staging directory — the datadir is untouched. A failure AFTER that point
//! deletes the partial chain data this run created (reth db, static files, consensus db) but KEEPS
//! the marker, so [`has_chain_data`] keeps reporting `true` and an operator must clear the datadir
//! before another attempt.
//!
//! Once every install step succeeds the marker is cleared BEFORE the informational receipt is
//! written, so a failure writing that receipt can never re-quarantine an already-restored datadir.

use crate::{
    has_chain_data,
    manifest::{Counts, Manifest, Pointer, LATEST_KEY},
    store::SnapshotStore,
    verify::{verify_snapshot, DecompressLimits, VerifiedSnapshot},
    SnapshotError, SnapshotResult, RESTORE_INCOMPLETE_MARKER,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, TelcoinDirs};
use tn_reth::{RethChainSpec, RethConfig, RethEnv, SnapshotRestorer};
use tn_storage::{
    consensus::{create_empty_epoch_pack, seed_latest_consensus},
    epoch_records::EpochRecordDb,
};
use tn_types::{
    now, Address, BlockNumHash, BlsPublicKey, Committee, CommitteeBuilder, ConsensusNumHash, Epoch,
    Genesis, TaskManager, B256,
};
use tracing::{info, warn};

/// Restore a verified snapshot from `source_url` onto `datadir`, returning a receipt of what
/// landed.
///
/// The caller supplies a `reth_config` built exactly as the node CLI builds it (see
/// `telcoin-network-cli`'s `node` command): this crate cannot construct one itself because a
/// [`RethConfig`] is assembled from reth clap arguments that are not reachable here. Restore OWNS
/// the reth database it opens from that config, so it can safely delete partial chain data if a
/// later step fails.
///
/// `epoch` pins the snapshot to a specific epoch and bypasses the bucket's `latest.json` pointer;
/// `None` selects the newest snapshot the pointer advertises (see the module's withholding caveat).
///
/// # Preconditions
///
/// - The local genesis and committee must exist, or [`SnapshotError::MissingTrustRoot`] is
///   returned.
/// - A leftover [`RESTORE_INCOMPLETE_MARKER`] yields [`SnapshotError::RestoreIncomplete`].
/// - Any existing chain data yields [`SnapshotError::ChainDataExists`] (there is no `--force` in
///   v1).
///
/// These three errors are exact so an auto-restore caller can decide skip-vs-fail from them.
///
/// # Errors
///
/// Returns the specific precondition errors above, plus [`SnapshotError::Manifest`] /
/// [`SnapshotError::Integrity`] / [`SnapshotError::Verification`] from resolution and verification,
/// and [`SnapshotError::Other`] wrapping any reth or consensus-store failure during installation.
pub async fn restore_from_snapshot<TND: TelcoinDirs>(
    datadir: &TND,
    source_url: &str,
    epoch: Option<u32>,
    reth_config: &RethConfig,
) -> SnapshotResult<RestoreReceipt> {
    // trust root and preconditions first: nothing is downloaded until the datadir is proven empty.
    let trust = precheck(datadir)?;
    let snapshots_path = datadir.snapshots_path();
    let marker = snapshots_path.join(RESTORE_INCOMPLETE_MARKER);

    // steps 2-3: resolve, download, and verify into a staging directory. a failure here is before
    // install begins, so it only wipes staging and leaves no marker.
    let staging_root = snapshots_path.join("restore-staging");
    let prepared = match prepare_snapshot(source_url, &trust, epoch, &staging_root).await {
        Ok(prepared) => prepared,
        Err(e) => {
            let _ = fs::remove_dir_all(&staging_root);
            return Err(e);
        }
    };

    // install begins: the marker's presence means the on-disk data can no longer be trusted until a
    // restore completes. write it durably (fsync file + dir) so a crash right after install cannot
    // lose it and silently un-quarantine the datadir. if writing it fails, no install has happened,
    // so treat it as a pre-install failure (wipe staging, no marker).
    if let Err(e) = write_marker_durably(&snapshots_path, &marker) {
        let _ = fs::remove_dir_all(&staging_root);
        return Err(e);
    }

    // steps 4-5: install the reth state, then the consensus records/packs. a failure now deletes
    // the partial chain data this run created but KEEPS the marker so the datadir stays
    // quarantined.
    let state_root =
        match install(datadir, reth_config, &prepared.verified, &prepared.staging_dir).await {
            Ok(root) => root,
            Err(e) => {
                delete_chain_data(datadir);
                let _ = fs::remove_dir_all(&staging_root);
                return Err(e);
            }
        };

    // step 6: installation is complete. clear the marker, then write a best-effort receipt, then
    // drop the staging directory. clearing the marker before writing the receipt is load-bearing
    // (see finalize_restore).
    let manifest = &prepared.verified.manifest;
    let receipt = RestoreReceipt {
        epoch: manifest.epoch,
        final_state: manifest.final_state,
        final_consensus: manifest.final_consensus,
        state_root,
        counts: manifest.counts,
        chain_id: trust.chain_id,
        genesis_hash: trust.genesis_hash,
        node_version: manifest.node_version.clone(),
        source_url: source_url.to_string(),
        restored_at: now(),
    };
    finalize_restore(&snapshots_path, &marker, &receipt);
    let _ = fs::remove_dir_all(&staging_root);

    info!(
        target: "tn::snapshot",
        epoch = receipt.epoch,
        block = receipt.final_state.number,
        ?state_root,
        "snapshot restore complete"
    );
    Ok(receipt)
}

/// Download and verify the snapshot at `source_url` WITHOUT installing anything.
///
/// Serves the `snapshot verify` CLI subcommand: it exercises the full resolve/download/verify path
/// against the local trust root and returns a [`VerifiedSummary`], then removes its own staging
/// directory so the datadir is left exactly as it was found. Because it never installs, it does not
/// consult [`has_chain_data`] or the restore marker — a snapshot can be vetted regardless of what
/// the datadir already holds.
///
/// # Errors
///
/// Returns [`SnapshotError::MissingTrustRoot`] if the local genesis/committee are absent, and the
/// same resolution, integrity, and verification errors as [`restore_from_snapshot`].
pub async fn verify_snapshot_source<TND: TelcoinDirs>(
    datadir: &TND,
    source_url: &str,
    epoch: Option<u32>,
) -> SnapshotResult<VerifiedSummary> {
    let trust = load_trust_root(datadir)?;
    let staging_root = datadir.snapshots_path().join("verify-staging");
    let outcome = prepare_snapshot(source_url, &trust, epoch, &staging_root).await;
    // verification leaves no trace, whether it passed or failed.
    let _ = fs::remove_dir_all(&staging_root);
    let prepared = outcome?;
    let manifest = prepared.verified.manifest;
    Ok(VerifiedSummary {
        epoch: manifest.epoch,
        final_state: manifest.final_state,
        final_consensus: manifest.final_consensus,
        counts: manifest.counts,
        fee_derivable: manifest.fee_derivable,
        chain_id: trust.chain_id,
        genesis_hash: trust.genesis_hash,
        node_version: manifest.node_version,
        source_url: source_url.to_string(),
    })
}

/// A record of a completed restore, serialized to `snapshots/last-restore.json`.
///
/// Written only after every install step succeeds, so its presence is proof the datadir holds a
/// fully-restored snapshot for the recorded epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreReceipt {
    /// Epoch whose closing state was restored.
    pub epoch: Epoch,
    /// Execution checkpoint (block number and hash) the restored node resumes from.
    pub final_state: BlockNumHash,
    /// Consensus checkpoint paired with [`final_state`](Self::final_state).
    pub final_consensus: ConsensusNumHash,
    /// State root recomputed from scratch during import and confirmed against `header(B)`.
    pub state_root: B256,
    /// Aggregate item counts copied from the restored manifest.
    pub counts: Counts,
    /// EVM chain id of the local trust root the snapshot was verified against.
    pub chain_id: u64,
    /// Local genesis hash the snapshot was bound to.
    pub genesis_hash: B256,
    /// Version string of the node that produced the snapshot.
    pub node_version: String,
    /// The object-store URL the snapshot was restored from.
    pub source_url: String,
    /// Unix timestamp (seconds) at which the restore completed.
    pub restored_at: u64,
}

/// The result of a successful [`verify_snapshot_source`]: what the snapshot claims, all confirmed
/// against the local trust root, without anything installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedSummary {
    /// Epoch whose closing state the snapshot captures.
    pub epoch: Epoch,
    /// Execution checkpoint a restore would resume from.
    pub final_state: BlockNumHash,
    /// Consensus checkpoint paired with [`final_state`](Self::final_state).
    pub final_consensus: ConsensusNumHash,
    /// Aggregate item counts recorded in the manifest.
    pub counts: Counts,
    /// Whether the manifest advertises that base fees are derivable after restore.
    pub fee_derivable: bool,
    /// EVM chain id of the local trust root the snapshot was verified against.
    pub chain_id: u64,
    /// Local genesis hash the snapshot was bound to.
    pub genesis_hash: B256,
    /// Version string of the node that produced the snapshot.
    pub node_version: String,
    /// The object-store URL the snapshot was verified from.
    pub source_url: String,
}

/// Filename of the restore receipt written under the snapshots directory.
const LAST_RESTORE_FILENAME: &str = "last-restore.json";

/// Maximum number of artifact entries a manifest may list before restore or verify refuses it.
///
/// State chunks rotate at roughly 256 MiB uncompressed, so 4096 artifacts already covers on the
/// order of a terabyte of state — far beyond any realistic snapshot. The cap exists to stop a
/// hostile bucket from advertising a million-entry manifest that would exhaust memory or file
/// descriptors before a single byte is verified. It is a documented constant in v1; making it
/// operator-configurable is deferred.
const MAX_MANIFEST_ARTIFACTS: usize = 4096;

/// Maximum combined size, in bytes, of every artifact a manifest may list (compressed, as stored).
///
/// This budgets the download itself. [`DecompressLimits`] bounds only the DECOMPRESSED size of each
/// chunk, so it cannot substitute for a cap on the compressed bytes actually pulled to disk. A
/// documented constant in v1; operator configurability is deferred.
const MAX_TOTAL_ARTIFACT_BYTES: u64 = 512 * 1024 * 1024 * 1024;

/// Multiplier applied to the download size when checking free disk.
///
/// The download is only the first consumer of disk: restore then rebuilds a reth database and ETL
/// scratch from it, and deep verification does the same into a throwaway datadir, so the installed
/// footprint needs headroom BEYOND the raw download budget. A factor of two is a coarse but safe
/// floor for that combined footprint.
const DISK_HEADROOM_FACTOR: u64 = 2;

/// The local trust root a snapshot is verified against: the genesis committee plus the genesis
/// binding (`chain_id` + `genesis_hash`) all read from the node's own configuration.
#[derive(Debug)]
struct TrustRoot {
    /// Genesis committee loaded from `committee.yaml`.
    committee: Committee,
    /// Genesis block hash derived from the local genesis.
    genesis_hash: B256,
    /// EVM chain id from the local genesis.
    chain_id: u64,
}

/// A downloaded, verified snapshot staged on disk, ready to install or summarize.
struct Prepared {
    /// The trusted, parsed snapshot contents.
    verified: VerifiedSnapshot,
    /// The staging directory holding the downloaded artifact files.
    staging_dir: PathBuf,
}

/// Load the local trust root, mapping any missing or unreadable component to
/// [`SnapshotError::MissingTrustRoot`].
///
/// The genesis supplies the `chain_id` and, once turned into a chain spec, the genesis hash the
/// snapshot must be bound to. The committee is the genesis committee epoch `0`'s record must match.
fn load_trust_root<TND: TelcoinDirs>(datadir: &TND) -> SnapshotResult<TrustRoot> {
    let genesis: Genesis = Config::load_from_path(datadir.genesis_file_path(), ConfigFmt::YAML)
        .map_err(|_| SnapshotError::MissingTrustRoot)?;
    let committee: Committee = Config::load_from_path(datadir.committee_path(), ConfigFmt::YAML)
        .map_err(|_| SnapshotError::MissingTrustRoot)?;

    // read chain_id before genesis is consumed into the chain spec, then derive the same genesis
    // hash reth's init_genesis would compute for this config.
    let chain_id = genesis.config.chain_id;
    let chain_spec: RethChainSpec = genesis.into();
    let genesis_hash = chain_spec.sealed_genesis_header().hash();

    Ok(TrustRoot { committee, genesis_hash, chain_id })
}

/// Load the trust root and enforce the install preconditions, in the order an auto-restore caller
/// depends on.
///
/// The trust root is loaded first so a datadir with no local configuration fails with
/// [`SnapshotError::MissingTrustRoot`] regardless of its chain-data state. The marker is checked
/// before [`has_chain_data`] so an interrupted prior restore surfaces the specific
/// [`SnapshotError::RestoreIncomplete`] rather than the generic [`SnapshotError::ChainDataExists`]
/// (the marker also makes `has_chain_data` report `true`).
fn precheck<TND: TelcoinDirs>(datadir: &TND) -> SnapshotResult<TrustRoot> {
    let trust = load_trust_root(datadir)?;
    if datadir.snapshots_path().join(RESTORE_INCOMPLETE_MARKER).exists() {
        return Err(SnapshotError::RestoreIncomplete);
    }
    if has_chain_data(datadir) {
        return Err(SnapshotError::ChainDataExists);
    }
    Ok(trust)
}

/// Resolve, download, and verify a snapshot into `<staging_root>/epoch-<N>/`.
///
/// Shared by [`restore_from_snapshot`] and [`verify_snapshot_source`]. The chosen epoch is logged
/// prominently: withholding is the one attack a valid trust root cannot rule out, so an operator
/// must be able to see which epoch was served.
async fn prepare_snapshot(
    source_url: &str,
    trust: &TrustRoot,
    epoch: Option<u32>,
    staging_root: &Path,
) -> SnapshotResult<Prepared> {
    let store = SnapshotStore::open(source_url)?;
    let (chosen_epoch, manifest) = resolve_manifest(&store, trust.chain_id, epoch).await?;

    // enforce artifact count and total-size budgets before any download; the manifest is now bound
    // to the local chain but its artifact list is still remote-controlled in size.
    let total_artifact_bytes = check_artifact_budgets(&manifest)?;

    if epoch.is_some() {
        info!(target: "tn::snapshot", epoch = chosen_epoch, source = source_url, "resolved pinned snapshot epoch");
    } else {
        info!(
            target: "tn::snapshot",
            epoch = chosen_epoch,
            source = source_url,
            "resolved latest snapshot epoch (a bucket can withhold newer snapshots; pin --epoch to override)"
        );
    }

    // wipe and recreate the per-epoch staging dir so a partial prior attempt cannot leak stale
    // files
    let staging_dir = staging_root.join(SnapshotStore::epoch_dir(chosen_epoch));
    if staging_dir.exists() {
        fs::remove_dir_all(&staging_dir)?;
    }
    fs::create_dir_all(&staging_dir)?;

    // refuse now if the filesystem cannot hold the download plus install/verify headroom, rather
    // than filling the disk mid-install and quarantining the datadir.
    check_free_disk(&staging_dir, total_artifact_bytes)?;

    // download every artifact, integrity-checked as it streams to disk
    for entry in &manifest.artifacts {
        let key = SnapshotStore::artifact_key(chosen_epoch, &entry.name);
        let dest = staging_dir.join(&entry.name);
        store.get_verified(&key, entry.size, &entry.sha256, &dest).await?;
    }

    // verification is CPU-bound (decompression + aggregate BLS), so run it off the async runtime.
    let verified = {
        let manifest = manifest.clone();
        let staging_dir = staging_dir.clone();
        let committee = trust.committee.clone();
        let genesis_hash = trust.genesis_hash;
        let chain_id = trust.chain_id;
        tokio::task::spawn_blocking(move || {
            verify_snapshot(
                &manifest,
                &staging_dir,
                &committee,
                genesis_hash,
                chain_id,
                DecompressLimits::default(),
            )
        })
        .await
        .map_err(|e| SnapshotError::Other(eyre::eyre!("snapshot verify task failed: {e}")))??
    };

    Ok(Prepared { verified, staging_dir })
}

/// Resolve the manifest to restore, either by pinned epoch or via the bucket's `latest.json`.
///
/// A pinned epoch fetches `epoch-<N>/manifest.json` directly and bypasses the pointer. The pointer
/// path additionally binds the pointer to the manifest by a sha256 of the manifest bytes, so a
/// tampered manifest cannot be substituted behind a valid pointer.
async fn resolve_manifest(
    store: &SnapshotStore,
    chain_id: u64,
    epoch: Option<u32>,
) -> SnapshotResult<(u32, Manifest)> {
    match epoch {
        Some(n) => {
            let bytes = store.get_json_bytes(&SnapshotStore::manifest_key(n)).await?;
            let manifest = Manifest::from_json(&bytes)?;
            if manifest.epoch != n {
                return Err(SnapshotError::Manifest(format!(
                    "pinned epoch {n} but manifest advertises epoch {}",
                    manifest.epoch
                )));
            }
            check_manifest_chain(&manifest, chain_id)?;
            Ok((n, manifest))
        }
        None => {
            let pointer_bytes = store.get_json_bytes(LATEST_KEY).await?;
            let pointer = Pointer::from_json(&pointer_bytes)?;
            if !pointer.matches_chain(chain_id) {
                return Err(SnapshotError::Manifest(format!(
                    "latest.json targets chain {}, local chain is {chain_id}",
                    pointer.chain_id
                )));
            }
            // the pointer's manifest_key must be exactly the canonical key for the epoch it names.
            // the sha256 binding below pins the manifest's CONTENT but not its LOCATION, so without
            // this a bucket could redirect a valid pointer at an off-layout object it controls.
            let canonical_key = SnapshotStore::manifest_key(pointer.epoch);
            if pointer.manifest_key != canonical_key {
                return Err(SnapshotError::Manifest(format!(
                    "latest.json manifest_key {:?} is not the canonical key {canonical_key:?} for epoch {}",
                    pointer.manifest_key, pointer.epoch
                )));
            }
            let manifest_bytes = store.get_json_bytes(&pointer.manifest_key).await?;
            let actual = sha256_hex(&manifest_bytes);
            if !actual.eq_ignore_ascii_case(&pointer.manifest_sha256) {
                return Err(SnapshotError::Integrity(format!(
                    "latest.json manifest sha256 mismatch: pointer says {}, manifest hashes to {actual}",
                    pointer.manifest_sha256
                )));
            }
            let manifest = Manifest::from_json(&manifest_bytes)?;
            check_manifest_chain(&manifest, chain_id)?;
            // the canonical-key check pins the layout; this pins the manifest's own epoch to the
            // one the pointer advertised, so a pointer cannot name epoch X while serving epoch Y.
            if manifest.epoch != pointer.epoch {
                return Err(SnapshotError::Manifest(format!(
                    "latest.json advertises epoch {} but the manifest at its canonical key is epoch {}",
                    pointer.epoch, manifest.epoch
                )));
            }
            Ok((manifest.epoch, manifest))
        }
    }
}

/// Confirm a manifest targets the local chain before it is trusted for downloads.
fn check_manifest_chain(manifest: &Manifest, chain_id: u64) -> SnapshotResult<()> {
    if manifest.chain_id != chain_id {
        return Err(SnapshotError::Manifest(format!(
            "manifest targets chain {}, local chain is {chain_id}",
            manifest.chain_id
        )));
    }
    Ok(())
}

/// Enforce the manifest's artifact-count and total-size budgets, returning the summed size.
///
/// Runs after [`resolve_manifest`] has bound the manifest to the local chain but before any
/// artifact is fetched, so a hostile bucket cannot make a fresh node download an unbounded number
/// or volume of objects. The returned total feeds [`check_free_disk`]. The sum uses
/// `saturating_add` so a manifest full of `u64::MAX` sizes reports the cap as exceeded rather than
/// overflowing.
///
/// # Errors
///
/// Returns [`SnapshotError::Manifest`] if the artifact count exceeds [`MAX_MANIFEST_ARTIFACTS`] or
/// the combined size exceeds [`MAX_TOTAL_ARTIFACT_BYTES`].
fn check_artifact_budgets(manifest: &Manifest) -> SnapshotResult<u64> {
    let count = manifest.artifacts.len();
    if count > MAX_MANIFEST_ARTIFACTS {
        return Err(SnapshotError::Manifest(format!(
            "manifest lists {count} artifacts, exceeding the maximum of {MAX_MANIFEST_ARTIFACTS}"
        )));
    }
    let total = manifest.artifacts.iter().fold(0u64, |acc, entry| acc.saturating_add(entry.size));
    if total > MAX_TOTAL_ARTIFACT_BYTES {
        return Err(SnapshotError::Manifest(format!(
            "manifest artifacts total {total} bytes, exceeding the maximum of {MAX_TOTAL_ARTIFACT_BYTES}"
        )));
    }
    Ok(total)
}

/// Refuse to start a download that cannot fit under `staging_dir`'s filesystem with headroom.
///
/// `needed` is the combined compressed size returned by [`check_artifact_budgets`]; the check
/// requires `needed * DISK_HEADROOM_FACTOR` free because the download is only the first consumer of
/// disk (restore rebuilds a reth database and ETL scratch from it, and deep verification does the
/// same in a throwaway datadir). Checking up front turns a mid-install `ENOSPC` — which would
/// quarantine the datadir — into a clean pre-install refusal.
///
/// On non-Unix targets `statvfs` is unavailable, so this is a no-op: a full disk still fails the
/// download safely, just without the early check.
///
/// # Errors
///
/// Returns [`SnapshotError::Other`] if the free space is below `needed * DISK_HEADROOM_FACTOR`, or
/// if the filesystem cannot be queried.
#[cfg(unix)]
fn check_free_disk(staging_dir: &Path, needed: u64) -> SnapshotResult<()> {
    let stat = rustix::fs::statvfs(staging_dir).map_err(|e| {
        SnapshotError::Other(eyre::eyre!(
            "failed to query free disk for the snapshot download: {e}"
        ))
    })?;
    let available = stat.f_bavail.saturating_mul(stat.f_frsize);
    let required = needed.saturating_mul(DISK_HEADROOM_FACTOR);
    if available < required {
        return Err(SnapshotError::Other(eyre::eyre!(
            "insufficient free disk for snapshot: {available} bytes available, need {required} \
             ({needed} bytes to download x{DISK_HEADROOM_FACTOR} headroom for install and verify)"
        )));
    }
    Ok(())
}

/// No-op free-disk check on targets without `statvfs`.
#[cfg(not(unix))]
fn check_free_disk(_staging_dir: &Path, _needed: u64) -> SnapshotResult<()> {
    Ok(())
}

/// Install a verified snapshot: reth state first, then the consensus records and packs.
///
/// Returns the state root recomputed during the reth import. The reth side is completed (and its
/// database handle released) before the consensus side begins, so a consensus-side failure can
/// safely delete the reth data too.
async fn install<TND: TelcoinDirs>(
    datadir: &TND,
    reth_config: &RethConfig,
    verified: &VerifiedSnapshot,
    staging_dir: &Path,
) -> SnapshotResult<B256> {
    // the reth import is fully blocking (MDBX writes + ETL) and long-running. the manual CLI
    // restore path reaches here without reth's block_in_place guard, so run it on the blocking pool
    // to avoid stalling the async runtime for the duration of the import (mirrors verify).
    let state_root = {
        let reth_config = reth_config.clone();
        let verified = verified.clone();
        let staging_dir = staging_dir.to_path_buf();
        let reth_db_path = datadir.reth_db_path();
        tokio::task::spawn_blocking(move || {
            install_reth(&reth_db_path, &reth_config, &verified, &staging_dir)
        })
        .await
        .map_err(|e| SnapshotError::Other(eyre::eyre!("snapshot install task failed: {e}")))??
    };
    install_consensus(datadir, verified).await?;
    Ok(state_root)
}

/// Rebuild the reth database from the verified state dump and header window.
///
/// Owns the reth database for the duration: [`SnapshotRestorer::open`] takes the handle by value
/// and [`finish`](SnapshotRestorer::finish) consumes the restorer, so on both success and failure
/// the MDBX environment is released by the time this returns — a prerequisite for
/// [`delete_chain_data`] to remove the files if a later step fails.
fn install_reth(
    reth_db_path: &Path,
    reth_config: &RethConfig,
    verified: &VerifiedSnapshot,
    staging_dir: &Path,
) -> SnapshotResult<B256> {
    let final_state = verified.manifest.final_state;

    // build and own the reth db from the caller's config, exactly as the node CLI does before
    // constructing a RethEnv. the local task manager only supervises the restorer's env.
    let reth_db = RethEnv::new_database(reth_config, reth_db_path)?;
    let task_manager = TaskManager::new("snapshot-restore");
    let restorer = SnapshotRestorer::open(reth_config, reth_db, &task_manager)?;

    restorer.import_chain_scaffold(&verified.headers, final_state)?;

    // the state dump streams through chained zstd decoders; the ETL scratch lives under staging.
    let etl_dir = staging_dir.join("etl");
    fs::create_dir_all(&etl_dir)?;
    let reader =
        ChainedStateReader::new(verified.state_chunks.clone(), DecompressLimits::default());
    let state_root = restorer.import_state(BufReader::new(reader), etl_dir)?;

    // the restored node enters epoch N+1; confirm the shipped window seeds its first base fees.
    let entered = verified.manifest.epoch.saturating_add(1);
    restorer.derive_fee_precondition(entered, &verified.headers)?;

    // consumes the restorer, releasing the db handle
    restorer.finish(final_state)?;
    Ok(state_root)
}

/// Seed the consensus datadir so a restored node opens the current epoch and backfills from `N`.
///
/// Persists the signed record/certificate chain for epochs `0..=N`, seeds the `LatestConsensus`
/// bookkeeping to `(N, records[N].final_consensus.number)`, and stages an empty `epoch-N` pack.
/// The empty pack is required because seeding a non-zero epoch forces `ConsensusChain::new` down
/// its "already running" branch, which reopens `epoch-N` via `open_append_exists` and errors if the
/// directory is absent. The pack's committee is a placeholder reconstructed from `records[N-1]`'s
/// `next_committee`; real epoch content (and the real committee) arrive later via peer catch-up,
/// which replaces the `epoch-N` directory wholesale.
async fn install_consensus<TND: TelcoinDirs>(
    datadir: &TND,
    verified: &VerifiedSnapshot,
) -> SnapshotResult<()> {
    let epochs_path = datadir.epochs_db_path();
    fs::create_dir_all(&epochs_path)?;

    let records = &verified.records;
    let n = verified.manifest.epoch;

    // persist records + certs in epoch order; save() is order-enforcing and idempotent, and
    // persist() flushes durably (and surfaces any async write error) before the db handle drops.
    {
        let db = EpochRecordDb::open(&epochs_path).map_err(consensus_error)?;
        for (record, cert) in records.iter() {
            db.save(record.clone(), cert.clone()).await.map_err(consensus_error)?;
        }
        db.persist().await.map_err(consensus_error)?;
    }

    // seed the double-buffered latest-consensus slots to epoch N's final consensus number. the
    // record lookups here are defensive: verify_snapshot guarantees records covers epochs 0..=N,
    // but install_consensus indexes the chain by the manifest's epoch, so an inconsistent
    // pairing must fail closed with a verification error rather than panic on an out-of-bounds
    // record.
    let final_number = records
        .get(n as usize)
        .ok_or_else(|| {
            SnapshotError::Verification(format!(
                "manifest epoch {n} has no matching record in the verified chain"
            ))
        })?
        .0
        .final_consensus
        .number;
    seed_latest_consensus(&epochs_path, n, final_number).await.map_err(consensus_error)?;

    // stage the empty epoch-N pack, whose committee must match records[N-1].next_committee.
    let previous = n
        .checked_sub(1)
        .and_then(|k| records.get(k as usize))
        .ok_or_else(|| {
            SnapshotError::Verification(format!(
                "manifest epoch {n} has no preceding record to reconstruct its committee"
            ))
        })?
        .0
        .clone();
    let committee_n = reconstruct_committee(n, &previous.next_committee);
    create_empty_epoch_pack(&epochs_path, previous, committee_n).await.map_err(consensus_error)?;

    Ok(())
}

/// Reconstruct epoch `N`'s committee from the previous record's `next_committee`.
///
/// This mirrors the seam the consensus store itself uses to reconstruct a committee from records
/// (`ConsensusPack::open_append` derives the expected committee from
/// `previous_epoch.next_committee` and validates only its BLS keys). The execution addresses are
/// placeholders: the empty pack this committee is written into is replaced wholesale by peer
/// catch-up before any consensus output is reconstructed against it, so only the key set — checked
/// against `next_committee` on reopen — is load-bearing.
fn reconstruct_committee(epoch: Epoch, next_committee: &[BlsPublicKey]) -> Committee {
    let mut builder = CommitteeBuilder::new(epoch);
    for key in next_committee {
        builder.add_authority(*key, Address::ZERO);
    }
    builder.build()
}

/// Delete the chain-data directories a failed install created, best-effort.
///
/// Removes the reth database, reth static files, and the consensus database (which contains the
/// epoch packs and records). The restore marker lives under the snapshots directory and is left
/// untouched, so the datadir stays quarantined until an operator clears it.
fn delete_chain_data<TND: TelcoinDirs>(datadir: &TND) {
    let reth_db = datadir.reth_db_path();
    if let Some(root) = reth_db.parent() {
        let _ = fs::remove_dir_all(root.join("static_files"));
    }
    let _ = fs::remove_dir_all(&reth_db);
    let _ = fs::remove_dir_all(datadir.consensus_db_path());
}

/// Write the restore-incomplete marker durably before install begins.
///
/// The marker is the datadir's quarantine flag: while it exists, [`has_chain_data`] reports `true`
/// and both manual and automatic restore refuse to run. Reth's own install commits its state
/// durably and INDEPENDENTLY, under a different directory, so if the marker were only buffered in
/// the page cache when power was lost the machine could come back up with reth's state at block `B`
/// but no marker and no consensus store — a datadir that looks restorable but is actually
/// half-built and silently un-quarantined. Fsyncing the marker file and its parent directory puts
/// the quarantine flag on disk before any install step runs, closing that gap.
///
/// On failure the marker is removed best-effort: no install has begun, so the datadir does not
/// warrant a quarantine.
///
/// # Errors
///
/// Returns [`SnapshotError::Io`] if the snapshots directory or the marker cannot be created or
/// synced.
fn write_marker_durably(snapshots_path: &Path, marker: &Path) -> SnapshotResult<()> {
    let write = || -> io::Result<()> {
        tn_storage::archive::data_file::create_dir_synced(snapshots_path)?;
        let file = fs::File::create(marker)?;
        file.sync_all()?;
        tn_storage::archive::data_file::fsync_directory(snapshots_path)
    };
    if let Err(e) = write() {
        // no install happened, so leave nothing behind that would quarantine the datadir.
        let _ = fs::remove_file(marker);
        return Err(SnapshotError::Io(e));
    }
    Ok(())
}

/// Finalize a completed restore: clear the incomplete-restore marker, then write the receipt.
///
/// The ordering is load-bearing. By the time this runs every install step has succeeded, so the
/// datadir holds a good restored snapshot. Removing the marker FIRST means a failure writing the
/// purely-informational receipt can never re-quarantine that good datadir. An earlier version wrote
/// the receipt first and propagated its error, which surfaced as a fatal
/// [`SnapshotError::RestoreIncomplete`] on the next boot even though the restore had in fact
/// completed. Both the marker removal and the receipt write are therefore best-effort: any failure
/// is logged and swallowed, never returned.
fn finalize_restore(snapshots_path: &Path, marker: &Path, receipt: &RestoreReceipt) {
    if let Err(e) = fs::remove_file(marker) {
        warn!(
            target: "tn::snapshot",
            error = %e,
            "restore completed but clearing the incomplete-restore marker failed; the datadir stays quarantined until the marker is cleared manually"
        );
    }

    // fsync the directory so the marker's removal is durable; a crash must not resurrect the
    // just-removed marker and re-quarantine an already-completed restore.
    let _ = tn_storage::archive::data_file::fsync_directory(snapshots_path);

    match serde_json::to_vec_pretty(receipt) {
        Ok(bytes) => {
            if let Err(e) = fs::write(snapshots_path.join(LAST_RESTORE_FILENAME), bytes) {
                warn!(target: "tn::snapshot", error = %e, "restore completed but writing the receipt failed");
            }
        }
        Err(e) => {
            warn!(target: "tn::snapshot", error = %e, "restore completed but serializing the receipt failed");
        }
    }
}

/// Wrap a consensus-store error (which is not part of the crate's `From` set) as
/// [`SnapshotError::Other`].
fn consensus_error<E: std::fmt::Display>(err: E) -> SnapshotError {
    SnapshotError::Other(eyre::eyre!("snapshot restore: consensus store error: {err}"))
}

/// Compute the lowercase-hex sha256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        // writing to a string is infallible
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A [`Read`] over a sequence of zstd-compressed state chunks, decompressed and concatenated in
/// order as one continuous stream.
///
/// The chunks were already sha256-pinned and cap-checked by verification, so this reader trusts the
/// bytes; it exists to feed reth's `init_from_state_dump` (a [`BufRead`] consumer) the
/// reconstructed state dump WITHOUT ever holding a whole chunk in memory. Chunks are opened lazily
/// one at a time, and a total-decompressed-byte cap — the same per-chunk cap verification enforced,
/// summed over the chunks — is carried as defense in depth so a maliciously crafted chunk cannot
/// expand without bound even if it slipped past the earlier check.
struct ChainedStateReader {
    /// Remaining chunk paths to decode, in concatenation order.
    chunks: std::vec::IntoIter<PathBuf>,
    /// The chunk currently being decoded, if one is open.
    current: Option<zstd::Decoder<'static, BufReader<fs::File>>>,
    /// Remaining decompressed bytes allowed before the anti-bomb cap trips.
    remaining_cap: u64,
}

impl ChainedStateReader {
    /// Build a reader over `chunks` (in concatenation order) capped at `limits.state_chunk` bytes
    /// per chunk in aggregate.
    fn new(chunks: Vec<PathBuf>, limits: DecompressLimits) -> Self {
        // each chunk decompressed to at most limits.state_chunk during verification, so the sum is
        // a cap that cannot trip on already-verified data yet still bounds a hostile
        // stream.
        let remaining_cap = limits.state_chunk.saturating_mul(chunks.len() as u64);
        Self { chunks: chunks.into_iter(), current: None, remaining_cap }
    }
}

impl Read for ChainedStateReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // open the next chunk if none is active; a `None` from the chunk iterator is true EOF.
            if self.current.is_none() {
                match self.chunks.next() {
                    Some(path) => {
                        // Decoder::new wraps the file in a BufReader internally
                        let decoder = zstd::Decoder::new(fs::File::open(path)?)?;
                        self.current = Some(decoder);
                    }
                    None => return Ok(0),
                }
            }

            let decoder = self.current.as_mut().expect("current chunk set above");
            let read = decoder.read(buf)?;
            if read == 0 {
                // this chunk is exhausted; advance to the next on the following loop iteration.
                self.current = None;
                continue;
            }
            self.remaining_cap = self.remaining_cap.checked_sub(read as u64).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot restore: state stream exceeded its decompression cap",
                )
            })?;
            return Ok(read);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        manifest::FORMAT_VERSION,
        verify::tests::{assemble, finish, happy_spec},
    };
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::BTreeSet;
    use tempfile::tempdir;
    use tn_storage::consensus::ConsensusChain;
    use tn_types::BlsKeypair;
    use url::Url;

    /// The chain id used by the test trust root and every fixture manifest.
    const TEST_CHAIN_ID: u64 = 2017;

    /// A minimal genesis pinned to [`TEST_CHAIN_ID`].
    fn a_genesis() -> Genesis {
        let mut genesis = Genesis::default();
        genesis.config.chain_id = TEST_CHAIN_ID;
        genesis
    }

    /// The `(chain_id, genesis_hash)` binding a snapshot must match, derived exactly as
    /// [`load_trust_root`] derives it.
    fn genesis_binding(genesis: &Genesis) -> (u64, B256) {
        let chain_id = genesis.config.chain_id;
        let chain_spec: RethChainSpec = genesis.clone().into();
        (chain_id, chain_spec.sealed_genesis_header().hash())
    }

    /// Write a valid genesis and genesis committee into `datadir` so [`load_trust_root`] succeeds.
    fn write_trust_root(datadir: &PathBuf, committee: &Committee, genesis: &Genesis) {
        Config::write_to_path(datadir.committee_path(), committee, ConfigFmt::YAML).unwrap();
        Config::write_to_path(datadir.genesis_file_path(), genesis, ConfigFmt::YAML).unwrap();
    }

    /// Open a `file://` snapshot store rooted at `dir`.
    fn file_store(dir: &Path) -> SnapshotStore {
        let url = Url::from_directory_path(dir).unwrap();
        SnapshotStore::open(url.as_str()).unwrap()
    }

    // ----- preconditions -----

    #[test]
    fn precheck_missing_trust_root_on_empty_datadir() {
        let dir = tempdir().unwrap();
        let err = precheck(&dir.path().to_path_buf()).unwrap_err();
        assert!(matches!(err, SnapshotError::MissingTrustRoot), "got {err:?}");
    }

    #[tokio::test]
    async fn verify_source_missing_trust_root() {
        // verify does not install, but still refuses without a local trust root
        let dir = tempdir().unwrap();
        let err = verify_snapshot_source(&dir.path().to_path_buf(), "file:///dev/null", None)
            .await
            .unwrap_err();
        assert!(matches!(err, SnapshotError::MissingTrustRoot), "got {err:?}");
    }

    #[test]
    fn precheck_rejects_restore_incomplete_marker() {
        let dir = tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        write_trust_root(&datadir, &finish(&happy_spec()).genesis, &a_genesis());

        let snapshots = datadir.snapshots_path();
        fs::create_dir_all(&snapshots).unwrap();
        fs::write(snapshots.join(RESTORE_INCOMPLETE_MARKER), b"").unwrap();

        let err = precheck(&datadir).unwrap_err();
        assert!(matches!(err, SnapshotError::RestoreIncomplete), "got {err:?}");
    }

    #[test]
    fn precheck_rejects_existing_chain_data() {
        let dir = tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        write_trust_root(&datadir, &finish(&happy_spec()).genesis, &a_genesis());

        let db = datadir.reth_db_path();
        fs::create_dir_all(&db).unwrap();
        fs::write(db.join("mdbx.dat"), b"x").unwrap();

        let err = precheck(&datadir).unwrap_err();
        assert!(matches!(err, SnapshotError::ChainDataExists), "got {err:?}");
    }

    #[test]
    fn precheck_passes_on_empty_datadir_with_trust_root() {
        let dir = tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        write_trust_root(&datadir, &finish(&happy_spec()).genesis, &a_genesis());

        let trust = precheck(&datadir).expect("empty datadir with a trust root must pass");
        assert_eq!(trust.chain_id, TEST_CHAIN_ID);
    }

    // ----- committee reconstruction -----

    #[test]
    fn reconstruct_committee_carries_epoch_and_key_set() {
        let mut rng = StdRng::seed_from_u64(1);
        let keys: Vec<BlsPublicKey> =
            (0..4).map(|_| *BlsKeypair::generate(&mut rng).public()).collect();

        let committee = reconstruct_committee(5, &keys);
        assert_eq!(committee.epoch(), 5);
        let expected: BTreeSet<BlsPublicKey> = keys.iter().copied().collect();
        assert_eq!(
            committee.bls_keys(),
            expected,
            "reconstructed key set must match next_committee"
        );
    }

    // ----- chained state reader -----

    #[test]
    fn chained_state_reader_concatenates_chunks_in_order() {
        let dir = tempdir().unwrap();
        let parts: [&[u8]; 3] = [b"root-line\n", b"account-a\naccount-b\n", b"account-c\n"];
        let mut paths = Vec::new();
        let mut expected = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            let path = dir.path().join(format!("state-{i:03}.jsonl.zst"));
            fs::write(&path, zstd::encode_all(*part, 3).unwrap()).unwrap();
            paths.push(path);
            expected.extend_from_slice(part);
        }

        let mut reader = ChainedStateReader::new(paths, DecompressLimits::default());
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, expected, "decoded chunks concatenate in order");
    }

    #[test]
    fn chained_state_reader_trips_on_decompression_cap() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state-000.jsonl.zst");
        // highly compressible: 4 KiB of zeros shrinks to a tiny frame but decompresses past the cap
        fs::write(&path, zstd::encode_all(vec![0u8; 4096].as_slice(), 3).unwrap()).unwrap();

        let limits = DecompressLimits { records: 1, headers: 1, state_chunk: 16 };
        let mut reader = ChainedStateReader::new(vec![path], limits);
        let mut out = Vec::new();
        let err = reader.read_to_end(&mut out).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // ----- download budgets -----

    /// A realistic manifest to exercise the budget checks against.
    fn a_manifest() -> Manifest {
        assemble(&finish(&happy_spec()), TEST_CHAIN_ID, B256::from([0xaa; 32])).manifest
    }

    #[test]
    fn budgets_reject_too_many_artifacts() {
        let mut manifest = a_manifest();
        let entry = manifest.artifacts[0].clone();
        manifest.artifacts = vec![entry; MAX_MANIFEST_ARTIFACTS + 1];
        let err = check_artifact_budgets(&manifest).unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
    }

    #[test]
    fn budgets_reject_total_size_over_cap() {
        let mut manifest = a_manifest();
        let mut first = manifest.artifacts[0].clone();
        let mut second = first.clone();
        // two artifacts each just under half of u64::MAX: saturating_add must report the cap as
        // exceeded without panicking on overflow.
        first.size = u64::MAX / 2;
        second.size = u64::MAX / 2;
        manifest.artifacts = vec![first, second];
        let err = check_artifact_budgets(&manifest).unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
    }

    #[test]
    fn budgets_accept_a_realistic_manifest() {
        let manifest = a_manifest();
        let total =
            check_artifact_budgets(&manifest).expect("a realistic manifest must pass the budgets");
        let expected: u64 = manifest.artifacts.iter().map(|entry| entry.size).sum();
        assert_eq!(total, expected, "returned total must equal the summed artifact sizes");
    }

    #[cfg(unix)]
    #[test]
    fn free_disk_rejects_impossible_requirement() {
        let dir = tempdir().unwrap();
        // no real filesystem has u64::MAX bytes free, so this must be refused
        let err = check_free_disk(dir.path(), u64::MAX).unwrap_err();
        assert!(matches!(err, SnapshotError::Other(_)), "got {err:?}");
    }

    #[cfg(unix)]
    #[test]
    fn free_disk_accepts_zero_requirement() {
        let dir = tempdir().unwrap();
        check_free_disk(dir.path(), 0).expect("a zero-byte requirement always fits");
    }

    // ----- manifest resolution -----

    #[tokio::test]
    async fn resolve_pin_bypasses_pointer() {
        let bucket = tempdir().unwrap();
        let store = file_store(bucket.path());
        let fx = assemble(&finish(&happy_spec()), TEST_CHAIN_ID, B256::from([0xaa; 32]));
        let n = fx.manifest.epoch;
        store
            .put_bytes(&SnapshotStore::manifest_key(n), fx.manifest.to_json().unwrap())
            .await
            .unwrap();

        // no latest.json is uploaded: a pinned epoch must resolve without the pointer
        let (epoch, manifest) = resolve_manifest(&store, TEST_CHAIN_ID, Some(n)).await.unwrap();
        assert_eq!(epoch, n);
        assert_eq!(manifest, fx.manifest);
    }

    #[tokio::test]
    async fn resolve_rejects_pointer_sha_mismatch() {
        let bucket = tempdir().unwrap();
        let store = file_store(bucket.path());
        let fx = assemble(&finish(&happy_spec()), TEST_CHAIN_ID, B256::from([0xaa; 32]));
        let n = fx.manifest.epoch;
        let manifest_key = SnapshotStore::manifest_key(n);
        store.put_bytes(&manifest_key, fx.manifest.to_json().unwrap()).await.unwrap();

        let pointer = Pointer {
            format_version: FORMAT_VERSION,
            chain_id: TEST_CHAIN_ID,
            epoch: n,
            manifest_key,
            manifest_sha256: "00".repeat(32),
            updated_at: 1,
        };
        store.put_bytes(LATEST_KEY, pointer.to_json().unwrap()).await.unwrap();

        let err = resolve_manifest(&store, TEST_CHAIN_ID, None).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn resolve_rejects_pointer_chain_id_mismatch() {
        let bucket = tempdir().unwrap();
        let store = file_store(bucket.path());
        let pointer = Pointer {
            format_version: FORMAT_VERSION,
            chain_id: 999,
            epoch: 3,
            manifest_key: SnapshotStore::manifest_key(3),
            manifest_sha256: "00".repeat(32),
            updated_at: 1,
        };
        store.put_bytes(LATEST_KEY, pointer.to_json().unwrap()).await.unwrap();

        let err = resolve_manifest(&store, TEST_CHAIN_ID, None).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn resolve_rejects_noncanonical_manifest_key() {
        let bucket = tempdir().unwrap();
        let store = file_store(bucket.path());
        let fx = assemble(&finish(&happy_spec()), TEST_CHAIN_ID, B256::from([0xaa; 32]));
        let n = fx.manifest.epoch;
        let manifest_bytes = fx.manifest.to_json().unwrap();

        // publish the real manifest at BOTH its canonical key and an off-layout key
        store.put_bytes(&SnapshotStore::manifest_key(n), manifest_bytes.clone()).await.unwrap();
        store.put_bytes("evil/manifest.json", manifest_bytes.clone()).await.unwrap();

        // the pointer names the off-layout key with the CORRECT sha, so only the canonical-key
        // check can reject it: this proves the layout rule beats the sha binding.
        let pointer = Pointer {
            format_version: FORMAT_VERSION,
            chain_id: TEST_CHAIN_ID,
            epoch: n,
            manifest_key: "evil/manifest.json".to_string(),
            manifest_sha256: sha256_hex(&manifest_bytes),
            updated_at: 1,
        };
        store.put_bytes(LATEST_KEY, pointer.to_json().unwrap()).await.unwrap();

        let err = resolve_manifest(&store, TEST_CHAIN_ID, None).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn resolve_rejects_pointer_epoch_mismatch() {
        let bucket = tempdir().unwrap();
        let store = file_store(bucket.path());
        let fx = assemble(&finish(&happy_spec()), TEST_CHAIN_ID, B256::from([0xaa; 32]));
        let n = fx.manifest.epoch;
        let manifest_bytes = fx.manifest.to_json().unwrap();

        // publish the epoch-n manifest at epoch n+1's canonical key and point at n+1 with the
        // correct sha: the layout check passes, so the manifest.epoch cross-check must fire.
        let key = SnapshotStore::manifest_key(n + 1);
        store.put_bytes(&key, manifest_bytes.clone()).await.unwrap();
        let pointer = Pointer {
            format_version: FORMAT_VERSION,
            chain_id: TEST_CHAIN_ID,
            epoch: n + 1,
            manifest_key: key,
            manifest_sha256: sha256_hex(&manifest_bytes),
            updated_at: 1,
        };
        store.put_bytes(LATEST_KEY, pointer.to_json().unwrap()).await.unwrap();

        let err = resolve_manifest(&store, TEST_CHAIN_ID, None).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
    }

    // ----- consensus install (the C5 consensus-side contract) -----

    #[tokio::test]
    async fn install_consensus_seeds_records_and_reopens_chain() {
        let parts = finish(&happy_spec());
        let n = parts.records.len() as u32 - 1;
        let fx = assemble(&parts, TEST_CHAIN_ID, B256::from([0xaa; 32]));
        let verified = VerifiedSnapshot {
            manifest: fx.manifest.clone(),
            records: parts.records.clone(),
            headers: vec![],
            state_chunks: vec![],
        };

        let dir = tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        install_consensus(&datadir, &verified).await.expect("consensus install must succeed");

        let epochs_path = datadir.epochs_db_path();
        // the staged epoch-N pack directory exists on disk
        assert!(epochs_path.join(format!("epoch-{n}")).exists(), "epoch-{n} pack dir must exist");

        // ConsensusChain::new takes its "already running" branch and reopens epoch-N via the seeded
        // LatestConsensus slots (committee_zero is only used to pre-open epoch 0, so it is
        // ignored).
        let chain = ConsensusChain::new(epochs_path.clone(), parts.genesis.clone())
            .expect("restored chain must reopen the staged pack");
        assert_eq!(chain.latest_consensus_epoch(), n, "seeded epoch must be exposed");
        assert_eq!(
            chain.latest_consensus_number(),
            parts.records[n as usize].0.final_consensus.number,
            "seeded number must be epoch N's final consensus number"
        );

        // records 0..=N are all persisted
        let db = EpochRecordDb::open(&epochs_path).unwrap();
        for k in 0..=n {
            assert!(db.record_by_epoch(k).await.is_some(), "record for epoch {k} must be present");
        }
    }

    #[tokio::test]
    async fn install_consensus_errors_on_inconsistent_epoch() {
        // verify_snapshot always produces a records vec covering epochs 0..=manifest.epoch, but
        // install_consensus indexes the chain by manifest.epoch. if that pairing were ever
        // inconsistent the lookup must fail closed with a verification error, never panic on the
        // out-of-bounds record index.
        let parts = finish(&happy_spec());
        let mut fx = assemble(&parts, TEST_CHAIN_ID, B256::from([0xaa; 32]));
        // four records (epochs 0..=3) but a manifest claiming epoch 9: records.get(9) is None.
        fx.manifest.epoch = 9;
        let verified = VerifiedSnapshot {
            manifest: fx.manifest.clone(),
            records: parts.records.clone(),
            headers: vec![],
            state_chunks: vec![],
        };

        let dir = tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        let err = install_consensus(&datadir, &verified).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Verification(_)), "got {err:?}");
    }

    // ----- verify_snapshot_source end-to-end against a file:// bucket -----

    #[tokio::test]
    async fn verify_snapshot_source_end_to_end() {
        let genesis = a_genesis();
        let (chain_id, genesis_hash) = genesis_binding(&genesis);

        // trust root: committee.yaml is the genesis committee epoch-0's record commits to
        let parts = finish(&happy_spec());
        let datadir_dir = tempdir().unwrap();
        let datadir = datadir_dir.path().to_path_buf();
        write_trust_root(&datadir, &parts.genesis, &genesis);

        // stage the snapshot and publish it (artifacts, manifest, then pointer) to a file:// bucket
        let fx = assemble(&parts, chain_id, genesis_hash);
        let n = fx.manifest.epoch;
        let bucket = tempdir().unwrap();
        let store = file_store(bucket.path());
        for entry in &fx.manifest.artifacts {
            store
                .put_file(
                    &SnapshotStore::artifact_key(n, &entry.name),
                    &fx.staged.join(&entry.name),
                )
                .await
                .unwrap();
        }
        let manifest_bytes = fx.manifest.to_json().unwrap();
        store.put_bytes(&SnapshotStore::manifest_key(n), manifest_bytes.clone()).await.unwrap();
        let pointer = Pointer {
            format_version: FORMAT_VERSION,
            chain_id,
            epoch: n,
            manifest_key: SnapshotStore::manifest_key(n),
            manifest_sha256: sha256_hex(&manifest_bytes),
            updated_at: 1,
        };
        store.put_bytes(LATEST_KEY, pointer.to_json().unwrap()).await.unwrap();

        let url = Url::from_directory_path(bucket.path()).unwrap();
        let summary = verify_snapshot_source(&datadir, url.as_str(), None)
            .await
            .expect("a valid published snapshot must verify against the local trust root");

        assert_eq!(summary.epoch, n);
        assert_eq!(summary.chain_id, chain_id);
        assert_eq!(summary.genesis_hash, genesis_hash);
        assert_eq!(summary.final_state, fx.manifest.final_state);
        // verification leaves no staging directory behind
        assert!(!datadir.snapshots_path().join("verify-staging").exists());
    }

    // ----- install-failure cleanup -----

    #[test]
    fn delete_chain_data_removes_dirs_but_keeps_marker() {
        let dir = tempdir().unwrap();
        let datadir = dir.path().to_path_buf();

        let reth_db = datadir.reth_db_path();
        fs::create_dir_all(&reth_db).unwrap();
        fs::write(reth_db.join("mdbx.dat"), b"x").unwrap();
        let static_files = reth_db.parent().unwrap().join("static_files");
        fs::create_dir_all(&static_files).unwrap();
        fs::write(static_files.join("headers.static"), b"x").unwrap();
        let cdb = datadir.consensus_db_path();
        fs::create_dir_all(&cdb).unwrap();
        fs::write(cdb.join("consensus.redb"), b"x").unwrap();

        let snapshots = datadir.snapshots_path();
        fs::create_dir_all(&snapshots).unwrap();
        let marker = snapshots.join(RESTORE_INCOMPLETE_MARKER);
        fs::write(&marker, b"").unwrap();

        delete_chain_data(&datadir);

        assert!(!reth_db.exists(), "reth db must be removed");
        assert!(!static_files.exists(), "static files must be removed");
        assert!(!cdb.exists(), "consensus db must be removed");
        assert!(marker.exists(), "the marker must survive so the datadir stays quarantined");
    }

    // ----- restore finalization -----

    /// A minimal restore receipt for the finalize tests.
    fn a_receipt() -> RestoreReceipt {
        RestoreReceipt {
            epoch: 7,
            final_state: BlockNumHash::new(4200, B256::from([0x22; 32])),
            final_consensus: ConsensusNumHash::new(99, B256::from([0x33; 32]).into()),
            state_root: B256::from([0x44; 32]),
            counts: Counts::default(),
            chain_id: TEST_CHAIN_ID,
            genesis_hash: B256::from([0x11; 32]),
            node_version: "tn-test/0.1.0".to_string(),
            source_url: "file:///bucket".to_string(),
            restored_at: 1,
        }
    }

    #[test]
    fn finalize_restore_removes_marker_then_writes_receipt() {
        let dir = tempdir().unwrap();
        let snapshots = dir.path().join("snapshots");
        fs::create_dir_all(&snapshots).unwrap();
        let marker = snapshots.join(RESTORE_INCOMPLETE_MARKER);
        fs::write(&marker, b"").unwrap();

        let receipt = a_receipt();
        finalize_restore(&snapshots, &marker, &receipt);

        assert!(!marker.exists(), "marker must be cleared");
        let bytes = fs::read(snapshots.join(LAST_RESTORE_FILENAME)).unwrap();
        let parsed: RestoreReceipt = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, receipt, "the written receipt must parse back to the original");
    }

    #[test]
    fn finalize_restore_survives_receipt_write_failure() {
        let dir = tempdir().unwrap();
        let snapshots = dir.path().join("snapshots");
        fs::create_dir_all(&snapshots).unwrap();
        let marker = snapshots.join(RESTORE_INCOMPLETE_MARKER);
        fs::write(&marker, b"").unwrap();
        // pre-create the receipt path AS A DIRECTORY so the receipt write fails
        fs::create_dir_all(snapshots.join(LAST_RESTORE_FILENAME)).unwrap();

        // must not panic and must still clear the marker despite the receipt write failing
        finalize_restore(&snapshots, &marker, &a_receipt());
        assert!(!marker.exists(), "marker must be cleared even when the receipt write fails");
    }

    #[test]
    fn write_marker_durably_creates_dir_and_marker() {
        let dir = tempdir().unwrap();
        // the snapshots directory does not exist yet; the helper must create it
        let snapshots = dir.path().join("snapshots");
        let marker = snapshots.join(RESTORE_INCOMPLETE_MARKER);

        write_marker_durably(&snapshots, &marker).expect("marker write must succeed");
        assert!(marker.exists(), "marker must exist after a durable write");

        // a second call is idempotent: File::create truncates an existing marker without error
        write_marker_durably(&snapshots, &marker).expect("second marker write must succeed");
        assert!(marker.exists(), "marker must still exist after an idempotent re-write");
    }
}
