//! On-demand EVM execution-state snapshot export.
//!
//! [`ExecStateExporter`] runs state exports on a dedicated background thread so a (potentially
//! slow) full plain-state walk never blocks the caller. An export is triggered by calling
//! [`trigger_export`](ExecStateExporter::trigger_export), which enqueues a request and returns a
//! receiver the caller may await for the result — or drop for fire-and-forget.
//!
//! The caller names the recent block to snapshot. Because the pinned plain-state reflects the
//! persisted execution tip, the export succeeds only once the requested block IS that tip: each
//! attempt pins a read-consistent view ([`RethEnv::pin_state_view`]) and confirms it matches the
//! requested block
//! ([`PinnedStateView::verify_tip`](tn_reth::snapshot::PinnedStateView::verify_tip)),
//! retrying briefly while a just-agreed block is still catching up to persistence. It then writes
//! the accounts plus that block's header and recent ancestors into an exec state pack
//! ([`PinnedStateView::export_state_pack`](tn_reth::snapshot::PinnedStateView::export_state_pack)).
//!
//! `EpochManager` spawns one exporter when `--enable-state-export` is set and triggers it at each
//! epoch boundary (see `export_epoch_state` in `close_epoch`); an on-demand trigger source (e.g. an
//! admin RPC) could reuse the same handle.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use eyre::eyre;
use tn_reth::{snapshot::check_fees_resumable, RethEnv};
use tn_storage::exec_state_pack::ExecStateStats;
use tn_types::{BlockNumHash, ExecHeader, SealedHeader};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, warn};

/// Number of ancestor headers (below the snapshot block) to embed for the EVM `BLOCKHASH` opcode
/// and the restore-side scaffold window.
const BLOCKHASH_ANCESTORS: u64 = 256;

/// How many times to re-pin while waiting for the requested block to become the persisted tip.
const MAX_PIN_ATTEMPTS: usize = 8;

/// Backoff between pin attempts while the requested block catches up to persistence.
const PIN_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Bound on queued export requests before [`trigger_export`](ExecStateExporter::trigger_export)
/// reports the exporter busy.
const REQUEST_QUEUE: usize = 4;

/// The result of a completed export.
#[derive(Debug, Clone)]
pub struct ExportOutcome {
    /// The block whose state was snapshotted (the persisted tip at export time).
    pub block: BlockNumHash,
    /// Account / storage-slot / bytecode tallies from the written pack.
    pub stats: ExecStateStats,
    /// Directory the pack was written to.
    pub path: PathBuf,
}

/// A queued export request: the env + block to export, where to write, and where to reply.
struct ExportRequest {
    reth_env: RethEnv,
    block: BlockNumHash,
    out_dir: PathBuf,
    reply: oneshot::Sender<eyre::Result<Option<ExportOutcome>>>,
}

/// Message to the background worker.
enum WorkerMsg {
    /// Run an export.
    Export(ExportRequest),
    /// Stop the worker loop.
    Shutdown,
}

/// Handle to the background execution-state snapshot exporter.
///
/// Cheap to clone (the worker thread is shared); the thread runs until [`shutdown`](Self::shutdown)
/// is called or the last handle is dropped.
#[derive(Clone)]
pub struct ExecStateExporter {
    tx: mpsc::Sender<WorkerMsg>,
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl ExecStateExporter {
    /// Spawn the background export worker. Each export supplies its own [`RethEnv`] (the node's
    /// execution engine — and its `RethEnv` — is recreated per epoch), so the worker holds none.
    pub fn spawn() -> eyre::Result<Self> {
        let (tx, rx) = mpsc::channel(REQUEST_QUEUE);
        let worker = std::thread::Builder::new()
            .name("evm-state-exporter".to_string())
            .spawn(move || run_worker(rx))
            .map_err(|e| eyre::eyre!("failed to spawn evm-state-exporter thread: {e}"))?;
        Ok(Self { tx, worker: Arc::new(Mutex::new(Some(worker))) })
    }

    /// Trigger an export of the execution state at `block` (read through `reth_env`) into
    /// `out_dir`.
    ///
    /// `block` is the recent block to snapshot; the export succeeds once it is the persisted
    /// execution tip (the worker waits briefly if it is still catching up to persistence).
    ///
    /// Non-blocking: enqueues the request and returns a receiver that resolves to the export result
    /// once the background worker finishes — `Ok(Some(outcome))` on success, or `Ok(None)` if the
    /// snapshot was intentionally skipped as un-resumable (see [`export_once`]). Drop the receiver
    /// for fire-and-forget. Returns an error if the worker's queue is full (an export is already in
    /// flight/queued) or the worker stopped.
    pub fn trigger_export(
        &self,
        reth_env: RethEnv,
        block: BlockNumHash,
        out_dir: PathBuf,
    ) -> eyre::Result<oneshot::Receiver<eyre::Result<Option<ExportOutcome>>>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .try_send(WorkerMsg::Export(ExportRequest { reth_env, block, out_dir, reply }))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    eyre!("state exporter is busy (an export is already queued)")
                }
                mpsc::error::TrySendError::Closed(_) => eyre!("state exporter worker has stopped"),
            })?;
        Ok(rx)
    }

    /// Stop the worker and wait for it to finish any in-flight export.
    ///
    /// Blocks the calling thread on the join, so call it from teardown, not a hot async path.
    /// A no-op when other clones of the handle are still alive.
    pub fn shutdown(self) {
        if Arc::strong_count(&self.worker) == 1 {
            let handle = self.worker.lock().expect("exporter mutex poisoned").take();
            // an explicit stop message breaks the loop even before the channel closes
            let _ = self.tx.try_send(WorkerMsg::Shutdown);
            if let Some(handle) = handle {
                if handle.join().is_err() {
                    error!(target: "tn::snapshot", "evm-state-exporter thread panicked");
                }
            }
        }
    }
}

impl std::fmt::Debug for ExecStateExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecStateExporter").finish_non_exhaustive()
    }
}

impl Drop for ExecStateExporter {
    fn drop(&mut self) {
        // when the last handle drops without an explicit shutdown, signal the worker to stop and
        // detach — it finishes any in-flight export and exits on its own rather than blocking Drop.
        if Arc::strong_count(&self.worker) == 1 {
            if let Ok(mut guard) = self.worker.lock() {
                if guard.take().is_some() {
                    let _ = self.tx.try_send(WorkerMsg::Shutdown);
                    warn!(
                        target: "tn::snapshot",
                        "ExecStateExporter dropped without shutdown(); worker detaching"
                    );
                }
            }
        }
    }
}

/// Background worker: process export requests one at a time until told to stop or the channel
/// closes.
fn run_worker(mut rx: mpsc::Receiver<WorkerMsg>) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            WorkerMsg::Export(req) => {
                let result = export_once(&req.reth_env, req.block, req.out_dir);
                // the receiver may have been dropped (fire-and-forget); that's fine.
                let _ = req.reply.send(result);
            }
            WorkerMsg::Shutdown => break,
        }
    }
    debug!(target: "tn::snapshot", "evm-state-exporter worker stopped");
}

/// Pin the execution state at `block` (once it is the persisted tip) and write it to a pack in
/// `out_dir`.
///
/// Returns `Ok(Some(outcome))` on a completed export, or `Ok(None)` when the snapshot is
/// intentionally skipped because a node could not resume from it — an idle epoch (an EIP-1559
/// worker with no genuine block in the window) leaves the base-fee derivation nothing to anchor, so
/// a node bootstrapped from the bundle would walk below the snapshot and halt, and `db load-state`
/// would reject it anyway. The check is the same one the importer runs and is done before the
/// expensive state walk, so idle epochs cost only the header scan.
fn export_once(
    reth_env: &RethEnv,
    block: BlockNumHash,
    out_dir: PathBuf,
) -> eyre::Result<Option<ExportOutcome>> {
    for attempt in 1..=MAX_PIN_ATTEMPTS {
        let view = reth_env.pin_state_view()?;
        // the pinned plain-state reflects the persisted tip, so `block` can only be exported once
        // it IS that tip. a just-agreed block may still be catching up to persistence, so
        // wait a little and retry rather than failing immediately.
        if !view.verify_tip(block)? {
            debug!(
                target: "tn::snapshot",
                attempt,
                number = block.number,
                "requested block is not yet the persisted tip; retrying"
            );
            std::thread::sleep(PIN_RETRY_BACKOFF);
            continue;
        }

        // `block` is the pinned tip and canonical; fetch its header for the state root + to embed.
        let anchor = reth_env
            .sealed_header_by_number(block.number)?
            .ok_or_else(|| eyre!("no sealed header at block {}", block.number))?;
        let headers = gather_headers(reth_env, &anchor)?;

        // Do not produce a bundle a node could not resume from. `gather_headers` yields headers
        // newest-first, so reverse into the ascending window the check expects (its last header is
        // `block`, the snapshot's B). This is the same fee-derivability check the importer applies;
        // skip the export (write nothing) when it fails.
        let window: Vec<SealedHeader> =
            headers.iter().rev().cloned().map(SealedHeader::seal_slow).collect();
        if let Err(e) = check_fees_resumable(reth_env, &window) {
            warn!(
                target: "tn::snapshot",
                number = block.number,
                error = %e,
                "skipping state export: snapshot would not be resumable"
            );
            return Ok(None);
        }

        let stats = view
            .export_state_pack(anchor.state_root, &headers, &out_dir)
            .map_err(|e| eyre!("state export failed: {e}"))?;

        debug!(
            target: "tn::snapshot",
            number = block.number,
            accounts = stats.account_count,
            storage_slots = stats.storage_slots,
            bytecodes = stats.bytecodes,
            "exported execution state snapshot"
        );
        return Ok(Some(ExportOutcome { block, stats, path: out_dir }));
    }
    Err(eyre!(
        "requested block {}:{} did not become the persisted execution tip after \
         {MAX_PIN_ATTEMPTS} attempts",
        block.number,
        block.hash
    ))
}

/// Collect the snapshot header (first) plus up to [`BLOCKHASH_ANCESTORS`] real ancestor headers
/// (excluding genesis), so the pack is self-sufficient for `BLOCKHASH` and the restore scaffold.
fn gather_headers(reth_env: &RethEnv, anchor: &SealedHeader) -> eyre::Result<Vec<ExecHeader>> {
    let mut headers = Vec::with_capacity(BLOCKHASH_ANCESTORS as usize + 1);
    headers.push(anchor.header().clone());

    // ancestors from anchor-1 down to max(1, anchor - BLOCKHASH_ANCESTORS), newest first;
    // genesis (block 0) is intentionally excluded (the restore scaffold rejects it).
    let lowest = anchor.number.saturating_sub(BLOCKHASH_ANCESTORS).max(1);
    for number in (lowest..anchor.number).rev() {
        let header = reth_env
            .sealed_header_by_number(number)?
            .ok_or_else(|| eyre!("missing ancestor header at block {number}"))?;
        headers.push(header.header().clone());
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_reth::{RethChainSpec, RethEnv};
    use tn_storage::exec_state_pack::ExecStatePackReader;
    use tn_types::{test_genesis, TaskManager};

    #[tokio::test]
    async fn triggers_export_on_background_thread() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let db_dir = TempDir::new()?;
        let task_manager = TaskManager::new("export exec test");
        let reth_env = RethEnv::new_for_temp_chain(chain, db_dir.path(), &task_manager, None)?;
        let genesis = reth_env.sealed_header_by_number(0)?.expect("genesis header");
        let (genesis_root, genesis_block) =
            (genesis.state_root, BlockNumHash::new(0, genesis.hash()));

        let exporter = ExecStateExporter::spawn().expect("exporter to spawn");

        // trigger an export of the (genesis) tip on the background thread and await its result
        let out = TempDir::new()?;
        let rx = exporter.trigger_export(reth_env, genesis_block, out.path().to_path_buf())?;
        // With a registry-less test genesis the entered-epoch read is a no-op skip, so the export
        // still runs; unwrap the `Some` to reach the outcome.
        let outcome = rx
            .await
            .expect("worker replied")
            .expect("export succeeded")
            .expect("export not skipped");

        // exported the requested genesis block, with the funded genesis accounts
        assert_eq!(outcome.block, genesis_block);
        assert!(outcome.stats.account_count > 0, "genesis has funded accounts");
        assert_eq!(outcome.path.as_path(), out.path());

        // the pack is a real, readable snapshot at the genesis state root
        let mut reader = ExecStatePackReader::open(out.path())?;
        assert_eq!(reader.meta().state_root, genesis_root);
        assert_eq!(reader.snapshot_header().number, 0);
        let accounts = reader.accounts().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(accounts.len() as u64, outcome.stats.account_count);
        assert!(!accounts.is_empty());

        exporter.shutdown();
        Ok(())
    }
}
