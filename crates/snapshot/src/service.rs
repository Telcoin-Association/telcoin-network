//! The long-running snapshot export-and-upload service.
//!
//! [`SnapshotUploader`] is the observer-only service that publishes a signed state snapshot to
//! remote object storage once per epoch boundary. It is driven by the node calling
//! [`on_epoch_closed`](SnapshotUploader::on_epoch_closed) at the end of each epoch, from inside the
//! boundary quiet window.
//!
//! # Boundary path vs. background job
//!
//! The boundary call must never block or crash consensus, so it does the minimum synchronously —
//! claim a single-flight gate and pin the closing epoch's state view — then spawns the heavy work
//! (await-certificate, export, upload, prune) onto the node-lifetime [`TaskSpawner`] as a
//! non-critical task and returns immediately. A non-critical task can fail freely without taking
//! the node down.
//!
//! # Crash-safety contract
//!
//! The publish order is load-bearing: every artifact is uploaded first, then the manifest bytes,
//! and the `latest.json` pointer is written **last**. A reader following `latest.json` therefore
//! never sees a pointer into a half-uploaded epoch. If the job fails, is skipped, or is cancelled
//! by shutdown at any point before the pointer write, the pointer keeps naming the previous epoch.
//!
//! # Single-flight and cleanup
//!
//! At most one job runs at a time: a boundary that arrives while a prior job is still running is
//! recorded as an overlap skip rather than queued. Each job holds the gate for its whole lifetime
//! via an [`OwnedMutexGuard`], and cleans its per-epoch staging directory, through RAII guards that
//! fire on every exit path including shutdown cancellation. [`SnapshotUploader::new`] additionally
//! wipes the staging root at startup as a backstop for anything a crash left behind.

use crate::{
    export::{export_epoch, ExportArgs, ExportedSnapshot},
    manifest::{Pointer, FORMAT_VERSION},
    metrics::{JobOutcome, SnapshotMetrics},
    store::SnapshotStore,
    SnapshotError, SnapshotResult, UploadConfig,
};
use sha2::{Digest as _, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tn_config::TelcoinDirs;
use tn_reth::{PinnedStateView, RethEnv};
use tn_storage::epoch_records::EpochRecordDb;
use tn_types::{hex, now, Epoch, EpochRecord, Noticer, Notifier, TaskSpawner, B256};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{error, info, warn};

/// Interval between certificate-availability polls while a job waits for epoch `N`'s certificate.
const CERT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Filename of the manifest staged next to the artifacts by the export step.
const MANIFEST_FILENAME: &str = "manifest.json";

/// Publishes signed per-epoch state snapshots from an observer node to remote object storage.
///
/// Construct one with [`SnapshotUploader::new`] (which hard-refuses to run on a validator) and wire
/// [`on_epoch_closed`](Self::on_epoch_closed) into the node's epoch-boundary teardown. The uploader
/// holds only node-lifetime, shareable handles, so a single instance serves every epoch for the
/// life of the node.
#[derive(Clone)]
pub struct SnapshotUploader {
    /// Operator-supplied upload configuration (bucket URL, retention, certificate timeout).
    config: UploadConfig,
    /// The object store this uploader publishes to.
    store: SnapshotStore,
    /// Node-lifetime spawner used to run each background publish job as a non-critical task.
    spawner: TaskSpawner,
    /// Handle to the epoch-records database, polled for epoch `N`'s certificate before export.
    epoch_db: EpochRecordDb,
    /// Root under which each job stages its `epoch-<N>` directory (`<datadir>/snapshots/staging`).
    staging_root: PathBuf,
    /// EVM chain id pinned into every manifest and pointer.
    chain_id: u64,
    /// Genesis block hash pinned into every manifest.
    genesis_hash: B256,
    /// Version string of this node, recorded in each manifest.
    node_version: String,
    /// Node shutdown signal; each job subscribes a fresh [`Noticer`] to abort cleanly on shutdown.
    shutdown: Notifier,
    /// Single-flight gate: a job holds it for its lifetime so a later boundary skips rather than
    /// overlapping.
    gate: Arc<Mutex<()>>,
    /// Metrics recorded by the boundary path and the background jobs.
    metrics: SnapshotMetrics,
}

impl SnapshotUploader {
    /// Create an uploader, failing node startup if it cannot run or reach its bucket.
    ///
    /// This performs three startup checks so a misconfiguration surfaces loudly at boot rather than
    /// silently at the first epoch boundary:
    ///
    /// 1. **Observer gate** — returns an error unless `observer_mode` is `true`. A committee
    ///    validator must never spend boundary time uploading; the CLI also gates this, and this is
    ///    the defense-in-depth backstop.
    /// 2. **Bucket probe** — opens the store from `config.url` and issues one cheap `list` call, so
    ///    bad credentials or an unreachable/misspelled URL fail here instead of per-epoch.
    /// 3. **Staging wipe** — recreates `<snapshots>/staging`, discarding any directory a previous
    ///    run left behind so stale files can never leak into a fresh snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if `observer_mode` is `false`, if the store URL is invalid/unsupported or
    /// unreachable, or if the staging root cannot be recreated.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: UploadConfig,
        spawner: TaskSpawner,
        epoch_db: EpochRecordDb,
        dirs: &impl TelcoinDirs,
        chain_id: u64,
        genesis_hash: B256,
        node_version: String,
        shutdown: Notifier,
        observer_mode: bool,
    ) -> SnapshotResult<Self> {
        // defense-in-depth: a voting validator must never construct the uploader
        if !observer_mode {
            return Err(SnapshotError::Other(eyre::eyre!(
                "snapshot uploader may only run on an observer node"
            )));
        }

        let store = SnapshotStore::open(&config.url)?;

        // probe the bucket with the cheapest authenticated round-trip so bad creds or a bad URL
        // fail node startup rather than the first boundary
        store.list_epochs().await.map_err(|err| {
            SnapshotError::Other(eyre::eyre!(
                "snapshot uploader could not reach the object store at {}: {err}",
                config.url
            ))
        })?;

        // wipe stale staging from a previous run (precedent: consensus staging-dir cleanup on open)
        let staging_root = dirs.snapshots_path().join("staging");
        if staging_root.exists() {
            std::fs::remove_dir_all(&staging_root)?;
        }
        std::fs::create_dir_all(&staging_root)?;

        Ok(Self {
            config,
            store,
            spawner,
            epoch_db,
            staging_root,
            chain_id,
            genesis_hash,
            node_version,
            shutdown,
            gate: Arc::new(Mutex::new(())),
            metrics: SnapshotMetrics::default(),
        })
    }

    /// React to the close of epoch `N` by pinning its state and spawning a publish job.
    ///
    /// This runs inside the epoch-boundary quiet window and is deliberately tiny: it claims the
    /// single-flight gate, pins the state view, verifies the pin observes `record.final_state`, and
    /// hands everything else to a spawned background job before returning. It **never** returns an
    /// error and never panics — any failure is logged, recorded, and swallowed so the boundary
    /// cannot be blocked or brought down.
    ///
    /// The state pin is the only work done on the boundary path; a boundary that arrives while a
    /// previous job is still running is recorded as an overlap skip and returns without pinning.
    pub async fn on_epoch_closed(&self, reth_env: &RethEnv, record: EpochRecord) {
        let epoch = record.epoch;

        // epoch-0 snapshots are unsupported: verify/restore reject them ("epoch-0 snapshots are
        // unsupported"), so publishing one only wastes an upload and leaves a latest.json a restore
        // must skip. bail before claiming the gate so an epoch-0 boundary is a no-op.
        if epoch == 0 {
            info!(target: "tn::snapshot", "skipping snapshot publish for epoch 0 (unsupported by restore)");
            self.metrics.record_job(JobOutcome::SkippedEpochZero);
            return;
        }

        // single-flight: skip rather than queue behind an in-flight job
        let Some(gate) = self.claim_gate(epoch) else { return };

        // pin the closing epoch's state view — the one and only boundary-path operation
        let pinned_state = match reth_env.pin_state_view() {
            Ok(view) => view,
            Err(err) => {
                error!(target: "tn::snapshot", epoch, %err, "failed to pin state view; skipping snapshot");
                self.metrics.record_job(JobOutcome::Failed);
                return;
            }
        };

        // confirm the pin observes exactly the epoch's final block before shipping its state
        match pinned_state.verify_tip(record.final_state) {
            Ok(true) => {}
            Ok(false) => {
                error!(
                    target: "tn::snapshot", epoch, final_block = record.final_state.number,
                    "pinned state tip does not match the epoch's final block; skipping snapshot"
                );
                self.metrics.record_job(JobOutcome::Failed);
                return;
            }
            Err(err) => {
                error!(target: "tn::snapshot", epoch, %err, "failed to verify pinned state tip; skipping snapshot");
                self.metrics.record_job(JobOutcome::Failed);
                return;
            }
        }

        // reth_env is cheap to clone (its inner is Arc'd); the job needs owned access for the
        // header/registry reads the export performs
        let plan = ExportPlan::Reth { reth_env: reth_env.clone(), pinned_state };
        self.dispatch(record, plan, gate);
    }

    /// Try to claim the single-flight gate, recording an overlap skip on contention.
    ///
    /// Returns the held guard on success. On contention it records
    /// [`JobOutcome::SkippedOverlap`] and returns `None`, which makes the caller return before
    /// spawning anything.
    fn claim_gate(&self, epoch: Epoch) -> Option<OwnedMutexGuard<()>> {
        match Arc::clone(&self.gate).try_lock_owned() {
            Ok(gate) => Some(gate),
            Err(_) => {
                self.metrics.record_job(JobOutcome::SkippedOverlap);
                info!(target: "tn::snapshot", epoch, "skipping snapshot: a previous upload job is still running");
                None
            }
        }
    }

    /// Spawn the background publish job for `plan`, moving the held `gate` into it.
    ///
    /// The job runs on the node-lifetime spawner as a **non-critical** task: when it finishes or
    /// errors the node is unaffected, and when the node shuts down the spawner cancels it. It
    /// always resolves to `Ok(())` because a snapshot job must never trigger a node shutdown.
    fn dispatch(&self, record: EpochRecord, plan: ExportPlan, gate: OwnedMutexGuard<()>) {
        let epoch = record.epoch;
        let ctx = self.job_ctx();
        let shutdown = self.shutdown.subscribe();
        self.spawner.spawn_task(format!("snapshot-upload-epoch-{epoch}"), async move {
            run_job(ctx, record, plan, CertWaitMode::Poll, shutdown, gate).await;
            Ok(())
        });
    }

    /// Clone the shareable handles a background job needs into an owned [`JobCtx`].
    fn job_ctx(&self) -> JobCtx {
        JobCtx {
            store: self.store.clone(),
            epoch_db: self.epoch_db.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            staging_root: self.staging_root.clone(),
            chain_id: self.chain_id,
            genesis_hash: self.genesis_hash,
            node_version: self.node_version.clone(),
        }
    }
}

impl std::fmt::Debug for SnapshotUploader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // omit the store/spawner/db/metrics handles, which have no useful debug output
        f.debug_struct("SnapshotUploader")
            .field("config", &self.config)
            .field("staging_root", &self.staging_root)
            .field("chain_id", &self.chain_id)
            .field("genesis_hash", &self.genesis_hash)
            .field("node_version", &self.node_version)
            .finish_non_exhaustive()
    }
}

/// The owned handles and configuration a background publish job runs against.
///
/// Every field is cheap to clone (object-store and db handles are `Arc`-backed), so each job gets
/// its own copy and the uploader stays usable for the next epoch.
#[derive(Clone)]
struct JobCtx {
    /// Object store to publish to.
    store: SnapshotStore,
    /// Epoch-records database, polled for the epoch certificate.
    epoch_db: EpochRecordDb,
    /// Upload configuration (retention, certificate timeout, bucket URL for logging).
    config: UploadConfig,
    /// Metrics handle.
    metrics: SnapshotMetrics,
    /// Staging root the job's `epoch-<N>` directory lives under.
    staging_root: PathBuf,
    /// EVM chain id for the manifest/pointer.
    chain_id: u64,
    /// Genesis hash for the manifest.
    genesis_hash: B256,
    /// Node version string for the manifest.
    node_version: String,
}

/// How a background job obtains the staged snapshot it publishes.
///
/// Production always uses [`ExportPlan::Reth`], which owns a cloned reth environment and the state
/// pin taken on the boundary path and runs [`export_epoch`] on a blocking thread. Tests inject a
/// pre-staged [`ExportedSnapshot`] via [`ExportPlan::Staged`] to exercise the await-certificate,
/// upload, prune, and cleanup stages without a live chain. The test variant is boxed to keep the
/// enum small.
enum ExportPlan {
    /// Run the real export against a pinned state view.
    Reth {
        /// Cloned execution environment for the export's header/registry reads.
        reth_env: RethEnv,
        /// State view pinned at the boundary; consumed by the export.
        pinned_state: PinnedStateView,
    },
    /// A pre-staged snapshot supplied by a test, bypassing the reth-dependent pin/export.
    #[cfg(test)]
    Staged(Box<ExportedSnapshot>),
}

/// Whether a job must wait for epoch `N`'s certificate before exporting.
///
/// Production always uses [`CertWaitMode::Poll`]. Tests inject [`CertWaitMode::Ready`] to skip the
/// wait, because a real [`EpochCertificate`](tn_types::EpochCertificate) (a BLS aggregate plus a
/// roaring bitmap) is not constructible from this crate's dependency set; the poll itself is
/// covered against an empty database and, end to end, by the live e2e suite.
enum CertWaitMode {
    /// Poll the epoch-records database until the certificate arrives.
    Poll,
    /// Assume the certificate is already present and skip the wait.
    #[cfg(test)]
    Ready,
}

/// Outcome of waiting for epoch `N`'s certificate to appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CertWait {
    /// The record and its certificate are present; the job may proceed to export.
    Ready,
    /// The certificate did not arrive before the configured deadline.
    TimedOut,
    /// The node began shutting down while waiting.
    ShuttingDown,
}

/// Summary of a successful [`publish_exported`] run: the counts a caller reports after upload.
///
/// Returned to the background service (for its success log line) and to the `snapshot create` CLI
/// (to report what it published). The manifest and `latest.json` pointer are not counted in
/// [`artifacts`](Self::artifacts).
#[derive(Debug, Clone, Copy)]
pub struct PublishSummary {
    /// Number of artifact objects uploaded (excludes the manifest and pointer).
    pub artifacts: usize,
    /// Total uploaded artifact bytes.
    pub bytes: u64,
}

/// RAII cleanup for a job's per-epoch staging directory.
///
/// Removes the directory on drop so a partial staging tree never survives a job, whatever the exit
/// path — success, skip, error, or shutdown cancellation (which drops the job future and hence this
/// guard). Removal is best-effort: a leftover directory is disposable and re-wiped by
/// [`SnapshotUploader::new`] at the next startup.
struct StagingGuard {
    /// The staging directory to remove on drop.
    dir: PathBuf,
}

impl StagingGuard {
    /// Create a guard that removes `dir` when dropped.
    fn new(dir: PathBuf) -> Self {
        Self { dir }
    }
}

impl Drop for StagingGuard {
    fn drop(&mut self) {
        if self.dir.exists() {
            if let Err(err) = std::fs::remove_dir_all(&self.dir) {
                warn!(target: "tn::snapshot", dir = %self.dir.display(), %err, "failed to remove snapshot staging dir");
            }
        }
    }
}

/// Run the full publish state machine for one closed epoch.
///
/// Stages, in order: await the epoch certificate, export (or accept a pre-staged snapshot), refuse
/// to publish an epoch whose base fees are not derivable, upload artifacts then manifest then the
/// pointer, prune old epochs, and record success. The `_gate` guard is held for the whole call and
/// released on return; the [`StagingGuard`] cleans the staging directory on every path. This
/// function never returns an error and never panics — every failure is logged and recorded so the
/// spawning task can report success to the node.
async fn run_job(
    ctx: JobCtx,
    record: EpochRecord,
    plan: ExportPlan,
    cert_wait: CertWaitMode,
    shutdown: Noticer,
    _gate: OwnedMutexGuard<()>,
) {
    let epoch = record.epoch;
    // clean this epoch's staging dir on any exit, including shutdown cancellation
    let _staging = StagingGuard::new(ctx.staging_root.join(SnapshotStore::epoch_dir(epoch)));

    // await cert: the epoch record can exist at the boundary before its certificate aggregates
    let cert_wait = match cert_wait {
        CertWaitMode::Poll => {
            await_certificate(&ctx.epoch_db, epoch, ctx.config.cert_timeout, &shutdown).await
        }
        #[cfg(test)]
        CertWaitMode::Ready => CertWait::Ready,
    };
    match cert_wait {
        CertWait::Ready => {}
        CertWait::TimedOut => {
            ctx.metrics.record_job(JobOutcome::SkippedCertTimeout);
            warn!(
                target: "tn::snapshot", epoch, timeout_secs = ctx.config.cert_timeout.as_secs(),
                "skipping snapshot: epoch certificate did not arrive before the deadline"
            );
            return;
        }
        CertWait::ShuttingDown => {
            info!(target: "tn::snapshot", epoch, "aborting snapshot job: node is shutting down");
            return;
        }
    }

    // export: heavy state dump runs on a blocking thread; the Staged variant is a test injection
    let exported = match plan {
        ExportPlan::Reth { reth_env, pinned_state } => {
            match run_reth_export(&ctx, &record, reth_env, pinned_state).await {
                Ok(exported) => exported,
                Err(err) => {
                    error!(target: "tn::snapshot", epoch, %err, "snapshot export failed");
                    ctx.metrics.record_job(JobOutcome::Failed);
                    return;
                }
            }
        }
        #[cfg(test)]
        ExportPlan::Staged(exported) => *exported,
    };

    // never publish an epoch a restored node could not resume fee calculation from
    if !exported.fee_derivable {
        ctx.metrics.record_job(JobOutcome::SkippedFeeUnderivable);
        warn!(
            target: "tn::snapshot", epoch,
            "skipping snapshot: an EIP-1559 worker has no chain-observable base-fee anchor in this epoch"
        );
        return;
    }

    // avoid spending upload I/O if the node started shutting down during the export
    if shutdown.noticed() {
        info!(target: "tn::snapshot", epoch, "aborting snapshot job before upload: node is shutting down");
        return;
    }

    // upload: artifacts, then the manifest bytes, then the latest.json pointer LAST
    let summary = match publish_exported(&ctx.store, ctx.chain_id, epoch, &exported).await {
        Ok(summary) => summary,
        Err(err) => {
            error!(target: "tn::snapshot", epoch, %err, "snapshot upload failed");
            ctx.metrics.record_job(JobOutcome::Failed);
            return;
        }
    };

    // prune old epochs, never deleting the one just published. failures are non-fatal: the
    // snapshot is already live.
    match ctx.store.prune(ctx.config.keep_last, Some(epoch)).await {
        Ok(deleted) if !deleted.is_empty() => {
            info!(target: "tn::snapshot", epoch, ?deleted, "pruned old snapshots")
        }
        Ok(_) => {}
        Err(err) => warn!(target: "tn::snapshot", epoch, %err, "failed to prune old snapshots"),
    }

    // success: staging is removed by the guard on return
    ctx.metrics.record_job(JobOutcome::Uploaded);
    ctx.metrics.last_uploaded_epoch.set(f64::from(epoch));
    info!(
        target: "tn::snapshot", epoch, bucket = %ctx.config.url,
        artifacts = summary.artifacts, bytes = summary.bytes,
        "published epoch snapshot"
    );
}

/// Poll the epoch-records database until epoch `N`'s certificate is present, `timeout` elapses, or
/// shutdown is signalled.
///
/// The record may be written at the boundary before votes finish aggregating into a certificate, so
/// this waits for the certificate specifically. The whole poll loop is bounded by `timeout` so a
/// long poll interval cannot overshoot a short deadline.
async fn await_certificate(
    db: &EpochRecordDb,
    epoch: Epoch,
    timeout: Duration,
    shutdown: &Noticer,
) -> CertWait {
    let poll = async {
        loop {
            // the getter returns the record plus its certificate if one has been stored
            if let Some((_record, Some(_cert))) = db.get_epoch_by_number(epoch).await {
                return CertWait::Ready;
            }
            tokio::select! {
                _ = shutdown => return CertWait::ShuttingDown,
                _ = tokio::time::sleep(CERT_POLL_INTERVAL) => {}
            }
        }
    };
    match tokio::time::timeout(timeout, poll).await {
        Ok(result) => result,
        Err(_elapsed) => CertWait::TimedOut,
    }
}

/// Run [`export_epoch`] on a blocking thread, driving its async body to completion there.
///
/// The state dump and zstd compression are CPU- and IO-heavy, so they are moved off the async
/// worker pool with [`tokio::task::spawn_blocking`]; [`export_epoch`] is async (it awaits the
/// epoch-records channel), so it is driven with `Handle::block_on` inside the blocking thread. The
/// blocking task cannot be cancelled mid-dump; on shutdown the job awaits it to completion before
/// returning, which is why the staging cleanup and gate release only run once the dump is finished.
async fn run_reth_export(
    ctx: &JobCtx,
    record: &EpochRecord,
    reth_env: RethEnv,
    pinned_state: PinnedStateView,
) -> SnapshotResult<ExportedSnapshot> {
    let epoch_records = ctx.epoch_db.clone();
    let staging_root = ctx.staging_root.clone();
    let chain_id = ctx.chain_id;
    let genesis_hash = ctx.genesis_hash;
    let node_version = ctx.node_version.clone();
    let record = record.clone();
    let created_at = now();

    let joined = tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(export_epoch(ExportArgs {
            reth_env: &reth_env,
            pinned_state,
            epoch_record: &record,
            epoch_records: &epoch_records,
            staging_root: &staging_root,
            chain_id,
            genesis_hash,
            node_version,
            created_at,
        }))
    })
    .await;

    match joined {
        Ok(result) => result,
        Err(join_err) => {
            Err(SnapshotError::Other(eyre::eyre!("snapshot export task panicked: {join_err}")))
        }
    }
}

/// Upload a staged snapshot to the store, writing the `latest.json` pointer last.
///
/// This is the shared publish path: the background [`SnapshotUploader`] and the `snapshot create`
/// CLI both call it so the crash-safety ordering and integrity binding live in exactly one place.
/// It uploads every artifact in manifest order, re-checking each store-returned `(size, sha256)`
/// against its manifest entry so a mismatch aborts before the pointer is written. It then uploads
/// the staged `manifest.json` bytes verbatim — hashing exactly those bytes — so the pointer's
/// `manifest_sha256` binds precisely what a reader will fetch, and finally writes the pointer. Any
/// failure returns before the pointer write, leaving `latest.json` unchanged.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ArtifactEntry, ArtifactKind, Counts, Manifest, KIND_STATE_ONLY};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use object_store::memory::InMemory;
    use std::future::Future;
    use tempfile::{tempdir, TempDir};
    use tn_reth::StateExportStats;
    use tn_types::{BlockNumHash, ConsensusNumHash, EpochDigest, TaskManager};

    const TEST_CHAIN_ID: u64 = 2017;
    const JOBS_TOTAL: &str = "tn_snapshot.jobs_total";
    const LAST_UPLOADED: &str = "tn_snapshot.last_uploaded_epoch";

    /// A metric value recovered from the debugging recorder, reduced to the two kinds this crate
    /// emits so assertions never name the recorder's verbose tuple types.
    enum Recorded {
        Counter(u64),
        Gauge(f64),
    }

    /// One recorded metric: name, optional `outcome` label value, and value.
    type MetricRow = (String, Option<String>, Recorded);

    /// Run `body` under a fresh local metrics recorder on a current-thread runtime, returning the
    /// metrics it emitted. The body receives metric handles bound to that local recorder, so its
    /// `record_job`/gauge calls are captured deterministically regardless of any global recorder.
    fn with_recorded_metrics<F, Fut>(body: F) -> Vec<MetricRow>
    where
        F: FnOnce(SnapshotMetrics) -> Fut,
        Fut: Future<Output = ()>,
    {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        metrics::with_local_recorder(&recorder, || {
            // new_with_labels binds handles to the local recorder (Default caches globally)
            let metrics = SnapshotMetrics::new_with_labels(Vec::<metrics::Label>::new());
            rt.block_on(body(metrics));
        });
        // into_vec resets counters, so snapshot exactly once
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(ck, _unit, _desc, value)| {
                let key = ck.key();
                let name = key.name().to_string();
                let outcome = key
                    .labels()
                    .find(|label| label.key() == "outcome")
                    .map(|label| label.value().to_string());
                let recorded = match value {
                    DebugValue::Counter(count) => Recorded::Counter(count),
                    DebugValue::Gauge(gauge) => Recorded::Gauge(gauge.0),
                    DebugValue::Histogram(_) => return None,
                };
                Some((name, outcome, recorded))
            })
            .collect()
    }

    /// Value of the `jobs_total` counter series carrying `outcome`, or 0 if absent.
    fn counter(rows: &[MetricRow], name: &str, outcome: &str) -> u64 {
        rows.iter()
            .find(|(n, o, _)| n == name && o.as_deref() == Some(outcome))
            .map(|(_, _, value)| match value {
                Recorded::Counter(count) => *count,
                Recorded::Gauge(_) => 0,
            })
            .unwrap_or(0)
    }

    /// Value of the unlabeled gauge series `name`, if present.
    fn gauge(rows: &[MetricRow], name: &str) -> Option<f64> {
        rows.iter().find(|(n, o, _)| n == name && o.is_none()).and_then(|(_, _, value)| match value
        {
            Recorded::Gauge(value) => Some(*value),
            Recorded::Counter(_) => None,
        })
    }

    /// A synthetic epoch-0 record; its digest keys the certificate the cert-await step polls for.
    fn epoch_record(epoch: Epoch) -> EpochRecord {
        EpochRecord {
            epoch,
            committee: vec![],
            next_committee: vec![],
            parent_hash: EpochDigest::default(),
            final_state: BlockNumHash::new(10, B256::repeat_byte(2)),
            final_consensus: ConsensusNumHash::new(3, B256::repeat_byte(3).into()),
        }
    }

    /// Stage a synthetic snapshot under `<staging_root>/epoch-<epoch>` and return an
    /// [`ExportedSnapshot`] describing it.
    ///
    /// `with_manifest` controls whether `manifest.json` is written (a missing manifest lets a test
    /// fail the publish between the artifact and pointer writes); `artifact_present` controls
    /// whether the single staged artifact file exists (an absent file fails the artifact upload).
    fn stage_snapshot(
        staging_root: &std::path::Path,
        epoch: Epoch,
        fee_derivable: bool,
        with_manifest: bool,
        artifact_present: bool,
    ) -> ExportedSnapshot {
        let dir = staging_root.join(SnapshotStore::epoch_dir(epoch));
        std::fs::create_dir_all(&dir).unwrap();

        let artifact_name = "state-000.jsonl.zst";
        let artifact_bytes = b"synthetic-state-artifact".to_vec();
        let artifact_path = dir.join(artifact_name);
        if artifact_present {
            std::fs::write(&artifact_path, &artifact_bytes).unwrap();
        }

        let entry = ArtifactEntry {
            name: artifact_name.to_string(),
            kind: ArtifactKind::StateChunk,
            size: artifact_bytes.len() as u64,
            sha256: sha256_hex(&artifact_bytes),
        };
        let manifest = Manifest {
            format_version: FORMAT_VERSION,
            kind: KIND_STATE_ONLY.to_string(),
            chain_id: TEST_CHAIN_ID,
            genesis_hash: B256::repeat_byte(1),
            epoch,
            final_state: BlockNumHash::new(10, B256::repeat_byte(2)),
            final_consensus: ConsensusNumHash::new(3, B256::repeat_byte(3).into()),
            epoch_record_digest: EpochDigest::default(),
            created_at: 1,
            node_version: "test/0.1".to_string(),
            artifacts: vec![entry],
            counts: Counts::default(),
            fee_derivable,
        };
        if with_manifest {
            std::fs::write(dir.join(MANIFEST_FILENAME), manifest.to_json().unwrap()).unwrap();
        }

        ExportedSnapshot {
            manifest,
            staging_dir: dir,
            artifacts: vec![artifact_path],
            fee_derivable,
            stats: StateExportStats::default(),
        }
    }

    /// Build a [`JobCtx`] over an injected store/db with a given certificate timeout.
    fn job_ctx(
        store: SnapshotStore,
        epoch_db: EpochRecordDb,
        staging_root: PathBuf,
        metrics: SnapshotMetrics,
        cert_timeout: Duration,
    ) -> JobCtx {
        JobCtx {
            store,
            epoch_db,
            config: UploadConfig { url: "memory://test".to_string(), keep_last: 3, cert_timeout },
            metrics,
            staging_root,
            chain_id: TEST_CHAIN_ID,
            genesis_hash: B256::repeat_byte(1),
            node_version: "test/0.1".to_string(),
        }
    }

    /// Assemble an uploader over in-memory handles with injected metrics (bypassing `new`'s I/O).
    fn test_uploader(dir: &TempDir, metrics: SnapshotMetrics) -> (SnapshotUploader, TaskManager) {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
        let staging_root = dir.path().join("staging");
        std::fs::create_dir_all(&staging_root).unwrap();
        let task_manager = TaskManager::new("snapshot-test");
        let uploader = SnapshotUploader {
            config: UploadConfig::new("memory://test"),
            store,
            spawner: task_manager.get_spawner(),
            epoch_db,
            staging_root,
            chain_id: TEST_CHAIN_ID,
            genesis_hash: B256::repeat_byte(1),
            node_version: "test/0.1".to_string(),
            shutdown: Notifier::new(),
            gate: Arc::new(Mutex::new(())),
            metrics,
        };
        (uploader, task_manager)
    }

    #[tokio::test]
    async fn new_refuses_non_observer() {
        let dir = tempdir().unwrap();
        let task_manager = TaskManager::new("t");
        let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
        let datadir = dir.path().to_path_buf();
        let err = SnapshotUploader::new(
            UploadConfig::new("memory://x"),
            task_manager.get_spawner(),
            epoch_db,
            &datadir,
            TEST_CHAIN_ID,
            B256::repeat_byte(1),
            "test/0.1".to_string(),
            Notifier::new(),
            false,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SnapshotError::Other(_)), "validator construction must be refused");
    }

    #[tokio::test]
    async fn new_rejects_bogus_url() {
        let dir = tempdir().unwrap();
        let task_manager = TaskManager::new("t");
        let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
        let datadir = dir.path().to_path_buf();
        // an unsupported scheme fails when opening the store, before any boundary work
        let err = SnapshotUploader::new(
            UploadConfig::new("ftp://example.com/snap"),
            task_manager.get_spawner(),
            epoch_db,
            &datadir,
            TEST_CHAIN_ID,
            B256::repeat_byte(1),
            "test/0.1".to_string(),
            Notifier::new(),
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SnapshotError::UnsupportedScheme(scheme) if scheme == "ftp"));
    }

    #[test]
    fn overlap_gate_records_skip_and_does_not_spawn() {
        let dir = tempdir().unwrap();
        let rows = with_recorded_metrics(|metrics| async {
            let (uploader, _task_manager) = test_uploader(&dir, metrics);
            // a prior job holds the gate
            let _held = Arc::clone(&uploader.gate).try_lock_owned().unwrap();
            // a second attempt must skip: claim_gate returns None, so dispatch/spawn never runs
            assert!(uploader.claim_gate(7).is_none(), "held gate must block a second job");
            assert!(uploader.store.list_epochs().await.unwrap().is_empty(), "nothing uploaded");
        });
        assert_eq!(counter(&rows, JOBS_TOTAL, "skipped_overlap"), 1);
    }

    #[test]
    fn epoch_zero_skips_before_gate_and_uploads_nothing() {
        let dir = tempdir().unwrap();
        let reth_tmp = tempdir().unwrap();
        let rows = with_recorded_metrics(|metrics| async {
            let (uploader, reth_tm) = test_uploader(&dir, metrics);
            // reth_env is required by the signature but never touched on the epoch-0 path
            let chain: Arc<tn_reth::RethChainSpec> = Arc::new(tn_types::test_genesis().into());
            let reth_env =
                RethEnv::new_for_temp_chain(chain, reth_tmp.path(), &reth_tm, None).unwrap();

            uploader.on_epoch_closed(&reth_env, epoch_record(0)).await;

            // the gate was never claimed, so it stays free for the next boundary
            assert!(
                Arc::clone(&uploader.gate).try_lock_owned().is_ok(),
                "epoch-0 skip must leave the gate released"
            );
            assert!(uploader.store.list_epochs().await.unwrap().is_empty(), "nothing uploaded");
        });
        assert_eq!(counter(&rows, JOBS_TOTAL, "skipped_epoch_zero"), 1);
        assert_eq!(counter(&rows, JOBS_TOTAL, "failed"), 0, "epoch-0 skip is not a failure");
    }

    #[tokio::test]
    async fn await_certificate_times_out_on_empty_db() {
        let dir = tempdir().unwrap();
        let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
        let shutdown = Notifier::new();
        let result =
            await_certificate(&epoch_db, 0, Duration::from_millis(50), &shutdown.subscribe()).await;
        assert_eq!(result, CertWait::TimedOut);
    }

    #[tokio::test]
    async fn publish_exported_writes_pointer_last_with_matching_sha() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        let staging = tempdir().unwrap();
        let exported = stage_snapshot(staging.path(), 0, true, true, true);

        let summary = publish_exported(&store, TEST_CHAIN_ID, 0, &exported).await.unwrap();
        assert_eq!(summary.artifacts, 1);

        // the pointer exists and its digest matches exactly the uploaded manifest bytes
        let pointer_bytes = store.get_json_bytes(SnapshotStore::LATEST_KEY).await.unwrap();
        let pointer = Pointer::from_json(&pointer_bytes).unwrap();
        let stored_manifest = store.get_json_bytes(&SnapshotStore::manifest_key(0)).await.unwrap();
        assert_eq!(pointer.manifest_sha256, sha256_hex(&stored_manifest));
        assert_eq!(pointer.manifest_key, SnapshotStore::manifest_key(0));
        assert_eq!(store.list_epochs().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn publish_exported_missing_manifest_leaves_no_pointer() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        let staging = tempdir().unwrap();
        // artifacts present but manifest.json absent: publish fails between artifact and pointer
        let exported = stage_snapshot(staging.path(), 0, true, false, true);

        let err = publish_exported(&store, TEST_CHAIN_ID, 0, &exported).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Io(_)));
        // the pointer was never written, so a reader still sees no latest snapshot
        assert!(store.get_json_bytes(SnapshotStore::LATEST_KEY).await.is_err());
    }

    #[test]
    fn run_job_cert_timeout_cleans_up_and_releases_gate() {
        let dir = tempdir().unwrap();
        let rows = with_recorded_metrics(|metrics| async {
            let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
            // empty db: the certificate never arrives
            let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
            let staging_root = dir.path().join("staging");
            let exported = stage_snapshot(&staging_root, 0, true, true, true);
            let ctx = job_ctx(
                store.clone(),
                epoch_db,
                staging_root.clone(),
                metrics,
                Duration::from_millis(50),
            );
            let shutdown = Notifier::new();
            let gate = Arc::new(Mutex::new(()));
            let held = Arc::clone(&gate).try_lock_owned().unwrap();

            // Poll against the empty db so the certificate never arrives and the deadline fires
            run_job(
                ctx,
                epoch_record(0),
                ExportPlan::Staged(Box::new(exported)),
                CertWaitMode::Poll,
                shutdown.subscribe(),
                held,
            )
            .await;

            assert!(!staging_root.join(SnapshotStore::epoch_dir(0)).exists(), "staging cleaned");
            assert!(store.list_epochs().await.unwrap().is_empty(), "nothing uploaded");
            assert!(Arc::clone(&gate).try_lock_owned().is_ok(), "gate released for the next job");
        });
        assert_eq!(counter(&rows, JOBS_TOTAL, "skipped_cert_timeout"), 1);
    }

    #[test]
    fn run_job_fee_underivable_skips_without_uploading() {
        let dir = tempdir().unwrap();
        let rows = with_recorded_metrics(|metrics| async {
            let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
            let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
            let record = epoch_record(0);
            let staging_root = dir.path().join("staging");
            // fee_derivable = false: the job must refuse to publish
            let exported = stage_snapshot(&staging_root, 0, false, true, true);
            let ctx = job_ctx(
                store.clone(),
                epoch_db,
                staging_root.clone(),
                metrics,
                Duration::from_secs(5),
            );
            let gate = Arc::new(Mutex::new(()));
            let held = Arc::clone(&gate).try_lock_owned().unwrap();

            run_job(
                ctx,
                record,
                ExportPlan::Staged(Box::new(exported)),
                CertWaitMode::Ready,
                Notifier::new().subscribe(),
                held,
            )
            .await;

            assert!(
                store.list_epochs().await.unwrap().is_empty(),
                "unverifiable epoch not uploaded"
            );
            assert!(!staging_root.join(SnapshotStore::epoch_dir(0)).exists(), "staging cleaned");
            assert!(Arc::clone(&gate).try_lock_owned().is_ok(), "gate released");
        });
        assert_eq!(counter(&rows, JOBS_TOTAL, "skipped_fee_underivable"), 1);
        assert_eq!(counter(&rows, JOBS_TOTAL, "uploaded"), 0);
    }

    #[test]
    fn run_job_failure_records_failed_and_releases_gate() {
        let dir = tempdir().unwrap();
        let rows = with_recorded_metrics(|metrics| async {
            let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
            let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
            let record = epoch_record(0);
            let staging_root = dir.path().join("staging");
            // artifact file absent: the upload fails, exercising the failure/cleanup path
            let exported = stage_snapshot(&staging_root, 0, true, true, false);
            let ctx = job_ctx(
                store.clone(),
                epoch_db,
                staging_root.clone(),
                metrics,
                Duration::from_secs(5),
            );
            let gate = Arc::new(Mutex::new(()));
            let held = Arc::clone(&gate).try_lock_owned().unwrap();

            run_job(
                ctx,
                record,
                ExportPlan::Staged(Box::new(exported)),
                CertWaitMode::Ready,
                Notifier::new().subscribe(),
                held,
            )
            .await;

            assert!(store.get_json_bytes(SnapshotStore::LATEST_KEY).await.is_err(), "no pointer");
            assert!(!staging_root.join(SnapshotStore::epoch_dir(0)).exists(), "staging cleaned");
            // a subsequent job can claim the gate
            assert!(Arc::clone(&gate).try_lock_owned().is_ok(), "gate released after failure");
        });
        assert_eq!(counter(&rows, JOBS_TOTAL, "failed"), 1);
    }

    #[test]
    fn run_job_uploaded_publishes_pointer_last_and_sets_gauge() {
        let dir = tempdir().unwrap();
        let rows = with_recorded_metrics(|metrics| async {
            let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
            let epoch_db = EpochRecordDb::open(dir.path().join("epochs")).unwrap();
            let record = epoch_record(0);
            let staging_root = dir.path().join("staging");
            let exported = stage_snapshot(&staging_root, 0, true, true, true);
            let ctx = job_ctx(
                store.clone(),
                epoch_db,
                staging_root.clone(),
                metrics,
                Duration::from_secs(5),
            );
            let gate = Arc::new(Mutex::new(()));
            let held = Arc::clone(&gate).try_lock_owned().unwrap();

            run_job(
                ctx,
                record,
                ExportPlan::Staged(Box::new(exported)),
                CertWaitMode::Ready,
                Notifier::new().subscribe(),
                held,
            )
            .await;

            // the pointer is present and binds the uploaded manifest bytes
            let pointer_bytes = store.get_json_bytes(SnapshotStore::LATEST_KEY).await.unwrap();
            let pointer = Pointer::from_json(&pointer_bytes).unwrap();
            let manifest_bytes =
                store.get_json_bytes(&SnapshotStore::manifest_key(0)).await.unwrap();
            assert_eq!(pointer.manifest_sha256, sha256_hex(&manifest_bytes));
            assert_eq!(store.list_epochs().await.unwrap(), vec![0]);
            assert!(!staging_root.join(SnapshotStore::epoch_dir(0)).exists(), "staging cleaned");
            assert!(Arc::clone(&gate).try_lock_owned().is_ok(), "gate released");
        });
        assert_eq!(counter(&rows, JOBS_TOTAL, "uploaded"), 1);
        assert_eq!(gauge(&rows, LAST_UPLOADED), Some(0.0));
    }
}
