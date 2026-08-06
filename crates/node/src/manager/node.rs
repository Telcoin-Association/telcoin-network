//! Node/application-lifetime layer of the epoch manager.
//!
//! This file owns the [`EpochManager`] struct and the resources that live for the entire process:
//! the long-running primary/worker p2p networks, the execution engine, the consensus DB, the
//! consensus chain (epoch pack files), and the app-scoped fetch/collector tasks. It also drives
//! the epoch loop: `run` builds the process-lifetime components, then `run_epochs` repeatedly
//! invokes `run_epoch` until shutdown.
//!
//! Per-epoch orchestration lives in the sibling `run_epoch` module. Code here is concerned with
//! what survives across epochs; code there is concerned with setting up and tearing down a single
//! epoch's consensus components.

use crate::{
    engine::{ExecutionNode, TnBuilder},
    health::HealthcheckServer,
    manager::{
        exex::{run_critical_exex_future, run_isolated_exex_future},
        spawn_epoch_vote_collector, ExecStateExporter,
    },
    metrics::EpochMetrics,
};
use eyre::{eyre, WrapErr as _};
use state_sync::{request_missing_packs, spawn_fetch_consensus, spawn_fetch_recent_consensus};
use std::{collections::BTreeMap, future::ready, sync::Arc};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{types::NetworkEvent, ConsensusNetwork};
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp, NodeMode, QueChannel};
use tn_reth::{system_calls::EpochState, RethDb, RethEnv};
use tn_storage::{consensus::ConsensusChain, open_db, DatabaseType};
use tn_types::{
    deconstruct_nonce,
    gas_accumulator::{entry_fee_for_worker, GasAccumulator},
    BlsPublicKey, BootstrapServer, Committee, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusNumHash, ConsensusOutput, Database as TNDatabase, EngineUpdate, Epoch, Notifier,
    SealedHeader, TaskError, TaskManager, TaskSpawner, TimestampSec, WorkerId, DEFAULT_WORKER_ID,
};
// The canonical worker-attribution helper lives in `tn-types` (one implementation, no drift);
// re-export so the crate-internal call sites and tests keep referring to it by bare name.
pub(crate) use tn_types::gas_accumulator::worker_id_from_header;
use tn_worker::{WorkerNetworkHandle, WorkerRequest, WorkerResponse};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod close_epoch;
mod run_epoch;
mod start_epoch;
pub use close_epoch::build_epoch_record;
use run_epoch::retry_provider_faults;
pub(crate) use run_epoch::RunEpochMode;

/// Name of the process-lifetime [`TaskManager`] that owns tasks outliving any single epoch
/// (p2p networks, engine updates, consensus fetchers).
const NODE_TASK_MANAGER: &str = "Node Task Manager";

/// The worker's base task manager name. This is used by `fn worker_task_manager_name(id)`.
pub(crate) const WORKER_TASK_BASE: &str = "Worker Task";

/// Capacity of the ExEx → manager `events` channel.
///
/// `FinishedHeight` is latest-wins, so this only needs to absorb a short burst;
/// a small bound is enough and keeps a buggy ExEx from growing it without limit.
const EXEX_EVENT_CAPACITY: usize = 16;

/// Capacity of the manager → engine `to_engine` channel.
///
/// Each item is a full [`ConsensusOutput`] (subdag + batches), and the engine additionally
/// bounds its own backlog at [`tn_engine::MAX_QUEUED_OUTPUTS`], so total in-flight memory is
/// capped at roughly `TO_ENGINE_CAPACITY + MAX_QUEUED_OUTPUTS` outputs. A deep channel here
/// would defeat the engine's bound by buffering the backlog one hop upstream; 64 absorbs
/// bursts (e.g. restart replay) while a persistently full channel backpressures the epoch
/// manager's forwarder instead of consuming memory.
const TO_ENGINE_CAPACITY: usize = 64;

/// The long-running owner that oversees epoch transitions.
///
/// One instance exists for the lifetime of the process. It holds the resources that must survive
/// across epochs (p2p network handles, consensus DB, consensus bus, consensus chain) alongside the
/// small amount of cross-epoch carry-over state that the next epoch needs to start correctly -
/// notably [`last_consensus_header`](Self::last_consensus_header),
/// [`last_forwarded_consensus_number`](Self::last_forwarded_consensus_number), and
/// [`network_initialized`](Self::network_initialized). Per-epoch consensus components are built and
/// dropped inside the epoch loop rather than stored here.
#[derive(Debug)]
pub(crate) struct EpochManager<P, DB> {
    /// The builder for node configuration
    builder: TnBuilder,
    /// The data directory
    tn_datadir: P,
    /// Primary network handle.
    primary_network_handle: Option<PrimaryNetworkHandle>,
    /// Worker network handle.
    worker_network_handle: Option<WorkerNetworkHandle>,
    /// Key config - loaded once for application lifetime.
    key_config: KeyConfig,
    /// The epoch manager's [Notifier] to shutdown all node processes.
    node_shutdown: Notifier,
    /// The timestamp to close the current epoch.
    ///
    /// The manager monitors leader timestamps for the epoch boundary.
    /// If the timestamp of the leader is >= the epoch_boundary then the
    /// manager closes the epoch after the engine executes all data.
    epoch_boundary: TimestampSec,
    /// Whether the long-running p2p networks have completed their one-time, per-process setup
    /// (start listening, register bootstrap peers).
    ///
    /// This setup normally runs on the `Initial` epoch, but the `Initial` iteration can return
    /// early from [`EpochManager::replay_missed_consensus`] - when a restart must replay-and-close
    /// an epoch boundary - *before* `create_consensus` runs the setup. In that case the setup runs
    /// on the first following `NewEpoch` iteration instead. Gating on this flag, rather than on
    /// [`RunEpochMode::Initial`], guarantees the networks are set up exactly once even on that
    /// restart path (mirrors the `are_workers_initialized` guard used for worker components).
    ///
    /// Committee slots are NOT gated on this flag. They are set every epoch from authoritative
    /// state via `update_committees`.
    network_initialized: bool,
    /// Reth (MDBX) database handle. Held for the whole process so the execution engine can be
    /// recreated without reopening storage.
    reth_db: RethDb,
    /// Consensus (REDB) database handle. Held for the whole process; shared with the p2p networks
    /// and per-epoch consensus components.
    consensus_db: DB,
    /// Application-scoped consensus bus. Survives epoch boundaries and is reset between epochs via
    /// `reset_for_epoch`; carries `recent_blocks`, node mode, and other cross-component state.
    consensus_bus: ConsensusBusApp,
    /// Persistent event stream for the long-running worker network. Outlives any single epoch so
    /// the worker swarm does not have to be rebuilt on each transition.
    worker_event_stream: QueChannel<NetworkEvent<WorkerRequest, WorkerResponse>>,

    /// Final consensus header of the epoch that just closed, carried into the next epoch so it can
    /// be used as the starting point for the new epoch's chain.
    last_consensus_header: Option<ConsensusHeader>,

    /// Highest consensus number actually forwarded to the execution engine (not merely persisted
    /// to the DB). Carried across epochs to avoid waiting on consensus that was stored but never
    /// sent to the engine.
    last_forwarded_consensus_number: u64,

    /// Handle to the epoch pack files that durably store consensus data. Persisted on startup and
    /// at shutdown; read by the fetch tasks that backfill missing epochs.
    consensus_chain: ConsensusChain,

    /// Bootstrap servers loaded once from the genesis committee, used to seed peer discovery on
    /// the long-running networks.
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,

    /// Static version string for the running node, reported by node-info surfaces.
    version_str: &'static str,

    /// Background execution-state exporter. `Some` only when `--enable-state-export` is set;
    /// exports each epoch's final state at the epoch boundary. Stops on drop.
    exec_state_exporter: Option<ExecStateExporter>,

    /// Prometheus metrics for the epoch lifecycle.
    metrics: EpochMetrics,
}

/// Restore the [`GasAccumulator`]'s gas stats and leader counts after a mid-epoch restart.
///
/// This is the first of three recovery stages (see the module docs on
/// [`tn_types::gas_accumulator`] for the full picture). It runs once at startup, before
/// execution resumes, and performs the following:
///
/// 1. **Worker count** — sizes the accumulator via [`sync_num_workers_from_chain`], reading the
///    on-chain `WorkerConfigs` count at the epoch's first block's parent (the previous epoch's
///    closing block), with the epoch's first block resolved from the pinned epoch state.
/// 2. **Gas stats** — iterates every reth block from the epoch's start height through the finalized
///    tip, extracting the worker id from each block's `difficulty` field and calling
///    [`GasAccumulator::inc_block`] to rebuild per-worker gas totals.
/// 3. **Leader counts** — walks the consensus DB in reverse, counting each leader's committed
///    blocks for rounds that have already been executed (i.e. `leader_round <=
///    last_executed_round`). Rounds beyond the last executed round are intentionally skipped
///    because [`EpochManager::replay_missed_consensus`] will re-execute them, which increments
///    leader counts through the normal payload-builder path.
///
/// Base fees are NOT restored here: the epoch entry seeding in `run_epoch` owns both the worker
/// count and every worker's base fee for the entered epoch, reading them from the previous
/// epoch's closing-block on-chain record ([`read_base_fees_for_entered_epoch`]) on every entry.
///
/// The restored per-worker gas totals and worker count are consensus-critical, not merely local
/// fee inputs: the next epoch close feeds them into the closing block's on-chain base-fee record
/// (`record_next_epoch_base_fees` in `tn-reth`), so an inaccurate restore here diverges that
/// block's hash from the rest of the fleet.
///
/// Every chain-derived input (the block scan range's start and end, the worker-count read block,
/// and the epoch used to bound leader counting) is pinned to the single finalized
/// [`SealedHeader`]. Startup heals the finalized marker to the persisted canonical tip
/// (`RethEnv::heal_finalized_to_persisted_tip`) before calling this, so on the production path
/// the pinned header IS the canonical tip and the scan misses nothing. `canonical_epoch` is the
/// epoch the caller read from the canonical tip; the guard comparing it against the pinned
/// header's epoch remains as a tripwire: a mismatch after the heal means the two views diverge
/// for a reason the heal could not repair (database inconsistency), so the restore hard-errors
/// rather than rebuilding against the wrong epoch's state. The comparison stays at epoch
/// granularity, so an unhealed mid-epoch lag (possible only for callers that skip the heal,
/// e.g. tests) still passes.
///
/// A scanned header referencing a worker id at or beyond the synced count fails the restore with
/// a descriptive error; the same condition panics in [`GasAccumulator::inc_block`] on the live
/// path.
///
/// If there is no finalized header (fresh genesis), this is a no-op; the epoch entry seeding
/// sizes the accumulator from genesis state.
pub async fn catchup_accumulator(
    reth_env: RethEnv,
    gas_accumulator: &GasAccumulator,
    consensus_chain: &mut ConsensusChain,
    canonical_epoch: Epoch,
) -> eyre::Result<()> {
    if let Some(block) = reth_env.finalized_header()? {
        // Pin the range start and the epoch classification to the SAME sealed header that
        // supplies the range end: the epoch state is read AT the finalized header rather than
        // at the canonical tip, so an inconsistent (finalized, canonical-tip) pair can never
        // yield a silently empty range that drops the restore.
        let epoch_state = reth_env.epoch_state_at_header(&block)?;

        // Cross-view guard: a finalized header pinned in a different epoch than the canonical
        // tip would pin the whole restore (scan range, worker count, leader-count bound) to the
        // wrong epoch's state. The marker commits atomically with the blocks, and startup
        // heals any lag left by a pre-fix database (`RethEnv::heal_finalized_to_persisted_tip`)
        // before this restore runs, so a benign marker lag can no longer reach this comparison
        // — a firing guard means the two views still disagree after the heal: genuine database
        // inconsistency. Investigate the database; do not restart-loop past this.
        if epoch_state.epoch != canonical_epoch {
            return Err(eyre!(
                "startup accumulator restore: finalized header {} pins epoch {} but the \
                 canonical tip reports epoch {canonical_epoch} — the views disagree across an \
                 epoch boundary even though startup heals the finalized marker to the canonical \
                 tip before this restore. This is database inconsistency, not a benign marker \
                 lag; refusing to rebuild gas stats against the wrong epoch's state",
                block.number,
                epoch_state.epoch,
            ));
        }

        // Size the accumulator from the on-chain worker count BEFORE the per-worker writes
        // below, reading at the epoch's first block's parent resolved from the SAME pinned
        // epoch state.
        sync_num_workers_from_chain(&reth_env, gas_accumulator, epoch_state.epoch_info.blockHeight)
            .await?;

        let nonce: u64 = block.nonce.into();
        let (last_executed_epoch, last_executed_round) = deconstruct_nonce(nonce);

        let blocks =
            reth_env.blocks_for_range(epoch_state.epoch_info.blockHeight..=block.number)?;

        // A committed header referencing a worker id at or beyond the synced count means the
        // chain and the `WorkerConfigs` contract disagree about the worker set — fail with
        // context instead of letting `inc_block` panic below.
        if let Some(max_worker_id) = blocks.iter().map(worker_id_from_header).max() {
            let num_workers = gas_accumulator.num_workers();
            if max_worker_id as usize >= num_workers {
                return Err(eyre!(
                    "startup accumulator restore: scanned blocks {}..={} reference worker id \
                     {max_worker_id} but the on-chain WorkerConfigs count at the epoch's start \
                     is {num_workers} (valid ids 0..{num_workers})",
                    epoch_state.epoch_info.blockHeight,
                    block.number,
                ));
            }
        }

        // loop through blocks to accumulate gas stats
        for current in blocks {
            let gas = current.gas_used;
            let limit = current.gas_limit;

            let worker_id = worker_id_from_header(&current);
            gas_accumulator.inc_block(worker_id, gas, limit);
        }

        // count leaders from consensus db for the current epoch
        // NOTE: replay_missed_consensus catches up rounds above last_executed_round.
        if last_executed_round > 0 && last_executed_epoch == epoch_state.epoch {
            consensus_chain
                .count_leaders(last_executed_round, gas_accumulator.rewards_counter().clone())
                .await?;
        }
    };

    Ok(())
}

/// Resize the [`GasAccumulator`] to the on-chain worker count for the epoch whose first block is
/// `epoch_first_block`.
///
/// The `WorkerConfigs` contract is the absolute source of truth for the worker count, and the
/// count for epoch E is its state at block `epoch_first_block - 1` - E's first block's parent,
/// i.e. the previous epoch's closing block (`saturating_sub` makes epoch 0 read genesis state).
/// That block is identical for a live producer at the boundary, a restarting node, and a
/// mid-epoch syncing node, and it is immune to mid-epoch `setNumWorkers` writes, which by design
/// only take effect at the next boundary.
///
/// READ-FAILURE POLICY: the count keys every per-worker write, so its failure is classified by
/// [`StateReadError`](tn_reth::error::StateReadError) and BOTH classes halt. There is deliberately
/// no fail-open arm: both callers — [`catchup_accumulator`] at startup and the epoch-0 arm of
/// `run_epoch`'s entry seeding — are ENTERING, so they hold no prior count to keep, and proceeding
/// on an unverifiable one would land worker-id-keyed writes in a wrongly sized accumulator. A
/// [`Provider`](tn_reth::error::StateReadError::Provider) fault is node-local (peers reading the
/// same block may succeed), so it is retried briefly first.
///
/// Reading at the closing block also makes the count value-stable for the whole epoch: a
/// mid-epoch (ModeChange) re-entry re-reads the identical count while the engine may still be
/// executing leftover output, so the resize is a no-op - the value-stability contract on
/// [`GasAccumulator::set_num_workers`]. No caller needs to quiesce execution first.
pub async fn sync_num_workers_from_chain(
    reth_env: &RethEnv,
    gas_accumulator: &GasAccumulator,
    epoch_first_block: u64,
) -> eyre::Result<()> {
    // `read_block` is derived from the caller's `epoch_first_block`, so the header resolved here is
    // the pin the retry below threads into every attempt.
    let read_block = epoch_first_block.saturating_sub(1);
    let header = reth_env
        .sealed_header_by_number(read_block)
        .wrap_err_with(|| format!("failed to read header {read_block} while syncing worker count"))?
        .ok_or_else(|| eyre!("no header at block {read_block} while syncing worker count"))?;

    let (num_workers, _entries) =
        retry_provider_faults("epoch-entry worker count", &header, |pin| {
            ready(reth_env.get_worker_fee_configs_at_block(pin.hash()))
        })
        .await
        .wrap_err_with(|| {
            format!("failed to read WorkerConfigs at block {read_block} while syncing worker count")
        })?;

    let current = gas_accumulator.num_workers();
    if current != num_workers {
        info!(
            target: "epoch-manager",
            current,
            on_chain = num_workers,
            read_block,
            "syncing GasAccumulator worker count to on-chain WorkerConfigs"
        );
    }
    gas_accumulator.set_num_workers(num_workers);
    Ok(())
}

/// Per-worker base fees for an entered epoch, read from the previous epoch's closing block by
/// [`read_base_fees_for_entered_epoch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochBaseFees {
    /// The on-chain worker count read from `WorkerConfigs` at the previous epoch's closing block
    /// (i.e. the count for the entered epoch).
    pub num_workers: usize,
    /// One fee per configured worker, indexed by worker id. Every slot is filled: the closing
    /// block records a fee for each `Eip1559` worker and a `Static` worker's fee is its config's
    /// value, so no worker's fee has to be recovered from anywhere else.
    pub fees: Vec<u64>,
}

impl EpochBaseFees {
    /// Install the fees into `gas_accumulator`.
    ///
    /// Resizes the accumulator to `num_workers` FIRST (so every configured slot exists), then
    /// writes every worker's base fee. Gas counters are deliberately untouched — the entered
    /// epoch starts at zero gas.
    ///
    /// Safe to run while the engine is still executing leftover consensus output on a ModeChange
    /// re-entry: both values are pinned to the previous epoch's closing block, so the re-entry
    /// re-reads the identical count — the resize no-ops — and rewrites the identical fees. That
    /// value-stability argument covers ModeChange re-entry ONLY. Where the count moves,
    /// [`GasAccumulator::set_num_workers`] truncates unconditionally (staying above in-flight
    /// worker ids is the CALLER obligation its doc records), with the accepted residual that a
    /// leftover batch from a removed worker would panic in `GasAccumulator::inc_block`.
    ///
    /// A governance shrink takes effect at the boundary, but on the LIVE boundary this resize is
    /// not the one that applies it. `apply_close_time_fee_updates` (in `run_epoch`) is a third
    /// production `set_num_workers` caller alongside this method and
    /// [`sync_num_workers_from_chain`], and it runs first — at close time, off `entries.len()` read
    /// at the very closing block this entry then pins — so the live NewEpoch entry finds the count
    /// already truncated and its own resize no-ops. A shrink therefore reaches production THROUGH
    /// this method only on the closes that skip the close-time update: the two
    /// `close_epoch(None, ..)` recovery closes in `run_epoch` (which carry three shapes between
    /// them — replay-and-close, crash-after-close, and leftover-drain), and the two chain-global
    /// fail-open arms on the close path, which both leave the count untruncated —
    /// `adjust_base_fees`' identity read, which returns before `apply_close_time_fee_updates` is
    /// reached at all, and `apply_close_time_fee_updates`' own config read, which returns without
    /// resizing. On those paths the bound is the boundary-drain
    /// ordering — the closed epoch's output is executed through the boundary before the entry runs.
    pub fn apply(&self, gas_accumulator: &GasAccumulator) {
        gas_accumulator.set_num_workers(self.num_workers);
        for (worker_id, fee) in self.fees.iter().enumerate() {
            gas_accumulator.base_fee(worker_id as WorkerId).set_base_fee(*fee);
        }
    }
}

/// Read the per-worker base fees for `entered` out of the `WorkerConfigs` state pinned at
/// `closing_header`, the previous epoch's closing block.
///
/// ONE state read, O(1) in the epoch's length, replacing the whole-epoch header scan that used to
/// recompute the same values. The closing block's fourth system call (`record_next_epoch_base_fees`
/// in `tn-reth`) writes each `Eip1559` worker's next-epoch fee into its `WorkerConfigs` row as it
/// seats the new committee, so the values this read returns were written exactly once, at the
/// boundary, by the very block being read. That is a strictly stronger pin than re-deriving them:
/// a value written into the closing block's state cannot be recomputed differently by a node with
/// a different view of the epoch's headers. Per-row interpretation (including the never-written
/// word that maps to `MIN_PROTOCOL_BASE_FEE`) lives in
/// [`entry_fee_for_worker`](tn_types::gas_accumulator::entry_fee_for_worker).
///
/// READ-FAILURE POLICY: the fees are a consensus input, so their failure is classified by
/// [`StateReadError`](tn_reth::error::StateReadError) and BOTH classes halt. There is deliberately
/// no fail-open arm — deliberately stricter than the close-time update's chain-global fail-open,
/// which is safe there only because keeping the current fees is a value every committee member
/// computes identically: the node is ENTERING the epoch, so it holds no prior fees to keep, and
/// entering on an unverifiable fee is a consensus-safety failure while halting is a single-node
/// liveness failure. A [`Provider`](tn_reth::error::StateReadError::Provider) fault is node-local
/// (peers reading the same block may succeed), so it is retried briefly first.
///
/// Three guards protect the read (this fn is pub-exported, so callers beyond `run_epoch` exist):
/// - `entered` must be at least 1: epoch 0 has no previous epoch to close, and its entry is seeded
///   by [`sync_num_workers_from_chain`] instead.
/// - `closing_header` must actually be `entered`'s boundary pin: the registry at it must report
///   epoch `entered` beginning at `closing_header.number + 1` (`concludeEpoch` runs INSIDE the
///   closing block, so only that block satisfies this). The check runs the same canonical boundary
///   predicate the snapshot restore's entry-readiness precondition uses
///   ([`RethEnv::get_current_epoch_info_at_header`]) — deliberately ONE predicate for what counts
///   as a closing block. Production upholds it by construction: `run_epoch` passes the pin resolved
///   by `RethEnv::epoch_state_at_epoch_start`, which self-validates.
///
///   The `blockHeight` side of that comparison is trustworthy because a begun epoch always
///   carries a real height: epoch 0 alone is stamped by the registry's constructor at genesis and
///   reports `blockHeight = 0` for the life of the chain, and `RethEnv::get_epoch_info_at_block`
///   rejects a `blockHeight = 0` record for any epoch but 0 as not-yet-begun. Since this guard
///   only ever runs for `entered >= 1`, a zero height cannot reach the comparison as a false
///   match.
/// - The read must return at least one worker (unreachable while the contract clamps its count;
///   mirrors the snapshot side's guard).
pub async fn read_base_fees_for_entered_epoch(
    reth_env: &RethEnv,
    entered: Epoch,
    closing_header: &SealedHeader,
) -> eyre::Result<EpochBaseFees> {
    if entered == 0 {
        return Err(eyre!(
            "epoch 0 has no previous closing block to read base fees from; its entry is seeded \
             via sync_num_workers_from_chain"
        ));
    }

    // Pin check: the header must be the boundary the fees were written at, validated through the
    // same predicate the snapshot restore's entry-readiness precondition runs.
    //
    // Both reads below retry node-local provider faults, threading `closing_header` — a pin the
    // CALLER resolved — as the retry's pin, so no `_from_tip` re-sampling dance is needed: there is
    // nothing per-attempt left to vary.
    let (epoch_at_pin, epoch_info) =
        retry_provider_faults("epoch-entry pin guard", closing_header, |pin| {
            ready(reth_env.get_current_epoch_info_at_header(pin))
        })
        .await
        .wrap_err_with(|| {
            format!(
                "failed to read the registry epoch record at epoch {entered}'s pinned closing \
                 block {} ({:?})",
                closing_header.number,
                closing_header.hash()
            )
        })?;
    if epoch_at_pin != entered || epoch_info.blockHeight != closing_header.number + 1 {
        return Err(eyre!(
            "header {} ({:?}) is not epoch {entered}'s closing-block pin: the registry at it \
             reports epoch {epoch_at_pin} beginning at block {}, expected epoch {entered} \
             beginning at block {}",
            closing_header.number,
            closing_header.hash(),
            epoch_info.blockHeight,
            closing_header.number + 1,
        ));
    }

    let (num_workers, entries) =
        retry_provider_faults("epoch-entry base fees", closing_header, |pin| {
            ready(reth_env.get_worker_fee_configs_at_block(pin.hash()))
        })
        .await
        .wrap_err_with(|| {
            format!(
                "failed to read WorkerConfigs at epoch {entered}'s pinned closing block {} ({:?})",
                closing_header.number,
                closing_header.hash()
            )
        })?;
    // UNREACHABLE BY CONSTRUCTION, and deliberately untested for the same reason as its twin in
    // `tn_reth::snapshot::check_entry_readiness`. See the comment there for the searches that
    // establish it: both writers of `numWorkers` in `WorkerConfigs.sol` floor it at 1, the CLI
    // rejects an empty `--worker-fee-config` list, and a reverted constructor fails genesis
    // creation instead of committing the empty storage that would read back as zero. Kept as the
    // epoch-entry half of that pair, so a zero from a hand-built genesis or a future contract
    // revision names itself here instead of surfacing as an empty accumulator.
    if num_workers == 0 {
        return Err(eyre!(
            "WorkerConfigs at epoch {entered}'s pinned closing block {} reports zero workers; \
             an epoch cannot be entered without at least one worker",
            closing_header.number
        ));
    }

    let fees = entries
        .iter()
        .enumerate()
        .map(|(worker_id, entry)| {
            entry_fee_for_worker(worker_id as WorkerId, entry).map_err(|e| {
                eyre!(
                    "worker {worker_id}: cannot read epoch {entered}'s entry base fee at the \
                     pinned closing block {} ({:?}): {e}",
                    closing_header.number,
                    closing_header.hash()
                )
            })
        })
        .collect::<eyre::Result<Vec<u64>>>()?;

    info!(
        target: "epoch-manager",
        entered,
        closing_block = closing_header.number,
        num_workers,
        ?fees,
        "read entered-epoch base fees from the closing block's on-chain record"
    );

    Ok(EpochBaseFees { num_workers, fees })
}

/// Open the process-lifetime consensus DB, creating its directory if absent.
///
/// The returned handle is meant to be held for the whole process and shared across epochs; it is
/// not reopened per epoch.
pub(crate) fn open_consensus_db<P: TelcoinDirs + 'static>(tn_datadir: &P) -> DatabaseType {
    let consensus_db_path = tn_datadir.consensus_db_path();

    // ensure dir exists
    let _ = std::fs::create_dir_all(&consensus_db_path);
    let db = open_db(&consensus_db_path);

    info!(target: "epoch-manager", ?consensus_db_path, "opened consensus storage");

    db
}

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Construct the manager and its process-lifetime state.
    ///
    /// Opens the consensus chain, builds the application-scoped consensus bus (forced into
    /// `Observer` mode when configured as an observer), and loads bootstrap servers from the
    /// genesis committee. Network handles are left `None` until [`run`](Self::run) spawns the
    /// networks. Panics if the consensus chain cannot be opened, since that is unrecoverable at
    /// startup.
    pub(crate) async fn new(
        builder: TnBuilder,
        tn_datadir: P,
        consensus_db: DB,
        key_config: KeyConfig,
        version_str: &'static str,
    ) -> eyre::Result<Self> {
        // Note this can only fail if the consensus DB is very broken (bad path for instance).
        // So we will panic for now, this will kill the node on startup for a critical error.
        let committee_zero = if let Ok(committee_zero) =
            Config::load_from_path::<Committee>(tn_datadir.committee_path(), ConfigFmt::YAML)
        {
            committee_zero
        } else {
            error!(target: "epoch-manager", "Unable to load committee zero from the genesis committee!");
            return Err(eyre::eyre!(
                "unable to load committee zero (genesis committee), this is fatal"
            ));
        };
        let epochs_db_path = tn_datadir.epochs_db_path();
        let _ = std::fs::create_dir_all(&epochs_db_path);
        let consensus_chain = ConsensusChain::new(epochs_db_path, committee_zero)?;
        // shutdown long-running node components
        let node_shutdown = Notifier::new();

        let reth_db = builder.reth_db.clone();

        let consensus_bus =
            ConsensusBusApp::new_with_recent_blocks(builder.tn_config.parameters.gc_depth);
        if builder.tn_config.observer {
            // Don't risk keeping the default CVV active mode...
            consensus_bus.node_mode().send_replace(NodeMode::Observer);
        }
        let worker_event_stream = QueChannel::new();
        let bootstrap_servers = if let Ok(committee_zero) =
            Config::load_from_path_or_default::<Committee>(
                tn_datadir.committee_path(),
                ConfigFmt::YAML,
            ) {
            committee_zero.bootstrap_servers()
        } else {
            error!(target: "epoch-manager", "Unable to load bootstrap servers from the genesis committee!");
            BTreeMap::new()
        };

        // Spawn the state exporter once, only when the feature is enabled.
        let exec_state_exporter =
            builder.enable_state_export.then(ExecStateExporter::spawn).transpose()?;

        // With export enabled, clean up any orphaned temp export dirs left by a crashed/interrupted
        // prior run. Safe here (startup) because no export is in flight; the per-epoch export path
        // only ever clears its own epoch's temp, so it can never delete an in-flight one.
        if exec_state_exporter.is_some() {
            close_epoch::sweep_stale_tmp_exports(
                &tn_datadir.consensus_db_path().join("state_exports"),
            );
        }

        Ok(Self {
            builder,
            tn_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            key_config,
            node_shutdown,
            epoch_boundary: Default::default(),
            network_initialized: false,
            reth_db,
            consensus_db,
            consensus_bus,
            worker_event_stream,
            last_consensus_header: None,
            last_forwarded_consensus_number: 0,
            consensus_chain,
            bootstrap_servers,
            version_str,
            exec_state_exporter,
            metrics: EpochMetrics::default(),
        })
    }

    /// Build the process-lifetime components, then drive the epoch loop until shutdown.
    ///
    /// Startup proceeds in order: create the execution engine and start it, heal any
    /// finalized-marker lag left by a pre-fix database to the persisted canonical tip
    /// (`RethEnv::heal_finalized_to_persisted_tip` — before anything reads the marker), recover
    /// the [`GasAccumulator`] via [`catchup_accumulator`], spawn the long-running p2p networks
    /// ([`spawn_node_networks`](Self::spawn_node_networks)), spawn the epoch-record and vote
    /// collectors, restore execution state ([`try_restore_state`](Self::try_restore_state)),
    /// and spawn the engine-update task. It then requests any missing epoch pack files and
    /// launches the app-scoped consensus fetch workers.
    ///
    /// Finally it selects over two futures: the node task manager running to exit, and the epoch
    /// loop ([`run_epochs`](Self::run_epochs)). Whichever resolves first ends the node; the
    /// consensus chain is persisted and remaining tasks are awaited before returning.
    pub(crate) async fn run(&mut self) -> eyre::Result<()> {
        // Surface any errors that may have been triggered on create.
        self.consensus_chain.persist_current().await?;
        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();
        // Prime the last forwarded consensus number at startup.
        // Normally this is not needed but is a layer of safety in case
        // run_epoch() does not process any output for some reason.
        // Use the same anchor the subscriber numbers new output from (pack ground truth,
        // falling back over the executed tip) rather than the slot-file hint: the hint can be
        // stale after a hard crash, and a low prime would make `wait_for_epoch_boundary`'s
        // continuity check report a spurious gap on the first live output (and the leftover
        // drain re-forward already-executed output).
        // A failed lookup aborts startup here rather than priming from a silently-defaulted
        // height 0.
        self.last_forwarded_consensus_number =
            state_sync::last_consensus_parent(&self.consensus_bus, &self.consensus_chain).await?.1;

        info!(target: "epoch-manager", "starting node and launching first epoch");

        // create channels for engine that survive the lifetime of the node
        let (to_engine, for_engine) = mpsc::channel(TO_ENGINE_CAPACITY);

        // Create the epoch gas accumulator with a single worker slot. The on-chain WorkerConfigs
        // contract is the absolute source of truth for the worker count: catchup_accumulator
        // sizes the accumulator from finalized-pinned closing-block state below, and every epoch
        // entry re-seeds the count alongside the base fees from the entered epoch's closing
        // block, so all nodes converge on the governance-set count.
        let gas_accumulator = GasAccumulator::new(1);
        // create channel for engine updates to consensus
        let (engine_update_tx, engine_update_rx) = mpsc::channel(64);

        // create the engine
        let engine = self.create_engine(&node_task_manager, &gas_accumulator)?;
        engine
            .start_engine(
                for_engine,
                self.node_shutdown.subscribe(),
                gas_accumulator.clone(),
                engine_update_tx,
            )
            .await?;

        // Heal a finalized marker left lagging the persisted canonical tip by a pre-fix
        // version (which committed blocks and the marker in separate transactions) BEFORE
        // anything reads the marker: the catchup below pins every chain-derived input to the
        // finalized header. Current versions commit the marker atomically with the blocks, so
        // this is defense-in-depth for old databases.
        let reth_env = engine.get_reth_env().await;
        reth_env.heal_finalized_to_persisted_tip()?;
        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        // The canonical epoch cross-checks the finalized header catchup pins its reads to.
        catchup_accumulator(reth_env, &gas_accumulator, &mut self.consensus_chain, epoch).await?;
        self.try_restore_state(&engine).await?;

        // read the network config or use the default, then stamp the genesis chain id
        // onto it so every wire protocol and gossip topic is chain-namespaced (issue
        // #765). Genesis is the single source of truth; this one value is read by the
        // network builder, the gossip handles, and the gossip-validation handlers.
        let mut network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        network_config.set_chain_id(self.builder.tn_config.genesis().config.chain_id);
        self.spawn_node_networks(node_task_spawner, &network_config, epoch).await?;
        let primary_network_handle =
            self.primary_network_handle.as_ref().expect("primary network").clone();
        // `epoch_vote_topic` and `consensus_output_topic` are committee-only publish topics, so
        // they are subscribed per-epoch in `spawn_primary_network_for_epoch` against a
        // committee-restricted publisher set (alongside `primary_topic`), and intentionally not
        // once here in the process-lifetime path. That makes the network layer drop non-committee
        // messages before re-propagation and refreshes the authorized set on every committee
        // rotation, rather than accepting any publisher once at node start. See issues #898 and
        // #912. `worker_batch_topic` follows the same per-epoch pattern in
        // `spawn_worker_network_for_epoch`, and is additionally gated on node mode: only
        // committee validators subscribe, observers unsubscribe (issue #960).
        state_sync::spawn_epoch_record_collector(
            self.consensus_chain.clone(),
            primary_network_handle.clone(),
            self.consensus_bus.clone(),
            node_task_manager.get_spawner(),
            self.node_shutdown.subscribe(),
        )
        .await?;

        spawn_epoch_vote_collector(
            self.consensus_chain.clone(),
            self.consensus_bus.clone(),
            self.key_config.clone(),
            primary_network_handle.clone(),
            node_task_manager.get_spawner(),
            self.node_shutdown.subscribe(),
        );

        // spawn task to update the latest execution results for consensus
        self.spawn_engine_update_task(engine_update_rx, &node_task_manager);

        // Spawn ExEx manager and ExEx tasks if any are registered
        if !self.builder.exex_fns.is_empty() {
            let reth_env = engine.get_reth_env().await;
            let canon_stream = reth_env.canonical_block_stream();

            // Subscribe to ConsensusBus broadcast channels for ExEx (fed from the
            // consensus-following path, not the validator hot path).
            let rx_certs = self.consensus_bus.subscribe_exex_certificates();
            let rx_consensus_output = self.consensus_bus.subscribe_exex_consensus_output();

            // Whether ExEx tasks + manager run as critical tasks (operator opt-in
            // via `Config::exex_critical`; default false → isolated, non-critical).
            let exex_critical = self.builder.tn_config.exex_critical;

            let mut exex_txs = Vec::new();
            let mut event_rxs = Vec::new();

            for (name, capacity, install_fn) in self.builder.exex_fns.drain(..) {
                // Clamp an operator-supplied `0` up to `1`: `mpsc::channel(0)`
                // panics, and the capacity from `install_exex_with_capacity` is
                // otherwise unvalidated (ExEx review finding #3).
                let (notif_tx, notif_rx) =
                    mpsc::channel(tn_exex::resolve_exex_channel_capacity(capacity));
                let (event_tx, event_rx) = mpsc::channel(EXEX_EVENT_CAPACITY);

                let ctx = tn_exex::TnExExContext::new(
                    notif_rx,
                    event_tx,
                    reth_env.clone(),
                    self.consensus_chain.clone(),
                );

                let exex_fut = install_fn(ctx);
                let label = format!("exex-{name}");
                let spawner = node_task_manager.get_spawner();
                if exex_critical {
                    // Operator opted in: a load-bearing ExEx. Spawn CRITICAL so a
                    // failure, panic, or clean exit propagates to the task manager
                    // and shuts the node down.
                    spawner.spawn_critical_task(
                        label.clone(),
                        run_critical_exex_future(label, exex_fut),
                    );
                } else {
                    // Default: optional, possibly third-party extension. Spawn
                    // NON-critical (a stop/error/panic must never shut the node
                    // down); panics are contained inside `run_isolated_exex_future`.
                    spawner.spawn_task(label.clone(), run_isolated_exex_future(label, exex_fut));
                }

                exex_txs.push((name, notif_tx));
                event_rxs.push(event_rx);
            }

            // NOTE: `_handle` exposes the minimum finished height across ExExes for
            // future pruning coordination. TN currently runs in archive mode (no
            // pruning), so there is no consumer yet and the handle is intentionally
            // dropped. See `tn_exex::TnExExEvent::FinishedHeight`.
            let (manager, _handle) = tn_exex::TnExExManager::new(
                canon_stream,
                rx_certs,
                rx_consensus_output,
                exex_txs,
                event_rxs,
            );
            // The manager follows the same policy as the ExEx tasks it serves.
            if exex_critical {
                node_task_manager.get_spawner().spawn_critical_task(
                    "exex-manager",
                    run_critical_exex_future("exex-manager".to_string(), manager.run()),
                );
                info!(target: "epoch-manager", "ExEx manager and tasks spawned (critical)");
            } else {
                // Non-critical: if it dies, live ExEx delivery stops (logged
                // loudly) but the node — host to an optional subsystem — stays up.
                node_task_manager.get_spawner().spawn_task(
                    "exex-manager",
                    run_isolated_exex_future("exex-manager".to_string(), manager.run()),
                );
                info!(target: "epoch-manager", "ExEx manager and tasks spawned (isolated, non-critical)");
            }
        }

        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // spawn node healthcheck service if enabled
        if let Some(port) = self.builder.healthcheck {
            // probe worker 0's readiness per request; capture the engine handle
            let engine = engine.clone();
            let worker_ready = move || {
                let engine = engine.clone();
                async move { engine.is_worker_initialized(DEFAULT_WORKER_ID).await }
            };
            let _ =
                HealthcheckServer::spawn(node_task_manager.get_spawner(), port, worker_ready).await;
        }

        // spawn prometheus metrics endpoint if enabled
        //
        // bind errors are propagated (unlike healthcheck) - the operator explicitly
        // requested the endpoint, so failing to serve it should fail startup
        if let Some(addr) = self.builder.metrics {
            let db = self.reth_db.clone();
            let hooks = tn_metrics::MetricsHooks::default()
                .with_hook(move || tn_reth::report_db_metrics(&db));
            tn_metrics::start_metrics_server(
                addr,
                &node_task_manager.get_spawner(),
                self.version_str,
                hooks,
            )
            .await?;

            // mirror consensus watch channels (rounds, heights, node mode) into gauges
            tn_primary::spawn_bus_metrics_mirror(
                &self.consensus_bus,
                &node_task_manager.get_spawner(),
                self.node_shutdown.subscribe(),
            );
        }

        // Do a sanity check, request any pack files for complete epochs we are missing.
        request_missing_packs(&self.consensus_bus, &self.consensus_chain).await;
        // spawn three critical workers that will fetch epoch pack files from an epoch work queue.
        // Note, these workers will just go dormant once we have caught up- that's ok.
        for i in 0..3 {
            let shutdown = self.node_shutdown.subscribe();
            let consensus_bus = self.consensus_bus.clone();
            let primary_network_handle = primary_network_handle.clone();
            let consensus_chain = self.consensus_chain.clone();
            node_task_manager.spawn_critical_task(
                format!("epoch-consensus-worker-{i}"),
                async move {
                    spawn_fetch_consensus(
                        shutdown,
                        consensus_bus,
                        primary_network_handle,
                        i,
                        consensus_chain,
                    )
                    .await;
                    Ok(())
                },
            );
        }
        // Fire up a app scoped task to fetch rencent consensus.
        // This will not be used by CVVs but won't hurt anything and
        // will be used when not active or catching up and needs to
        // run with app scope (not epoch).
        let shutdown = self.node_shutdown.subscribe();
        let consensus_bus = self.consensus_bus.clone();
        let primary_network_handle = primary_network_handle.clone();
        let consensus_chain = self.consensus_chain.clone();
        let db = self.consensus_db.clone();
        let task_spawner = node_task_manager.get_spawner();
        let rx_consensus_request = consensus_bus.subscribe_consensus_request_queue();
        node_task_manager.spawn_critical_task("fetch-recent-consensus", async move {
            spawn_fetch_recent_consensus(
                db,
                consensus_bus,
                primary_network_handle,
                consensus_chain,
                shutdown,
                task_spawner,
                rx_consensus_request,
            )
            .await;
            Ok(())
        });

        // await all tasks on epoch-task-manager or node shutdown
        let result = tokio::select! {
            // run long-living node tasks
            res = node_task_manager.until_exit(self.node_shutdown.clone()) => {
                match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(eyre!("Node task shutdown: {e}")),
                }
            }

            // loop through short-term epochs
            epoch_result = self.run_epochs(&engine, network_config, to_engine, gas_accumulator) => epoch_result,
        };
        self.consensus_chain.persist_current().await?;
        node_task_manager.wait_for_task_shutdown().await;

        result
    }

    /// Spawn the process-lifetime primary and worker [`ConsensusNetwork`] swarms.
    ///
    /// Each swarm runs as a critical task until node shutdown. The resulting network handles are
    /// stored on the manager for use by every epoch; the worker handle is seeded with the starting
    /// `epoch` and its task spawner is refreshed on each epoch transition.
    async fn spawn_node_networks(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
        epoch: Epoch,
    ) -> eyre::Result<()> {
        // Reject an invalid peer-score config before it is installed into the process-global,
        // first-write-wins `GLOBAL_SCORE_CONFIG` by the `PeerManager` built below
        // (`init_peer_score_config`). This is the boot-path install funnel, so validating here
        // fails the node fast at start with a field-named error rather than letting a
        // `min_score > max_score` or `NaN` bound poison the scoring path and later panic
        // `Score::add`'s `f64::clamp` on the first peer penalty.
        // `ConsensusConfig::new_with_committee` validates the same config for the
        // construction path (tests, epoch transitions); this guard covers the node boot
        // that actually performs the one-time install.
        network_config.peer_config().score_config.validate()?;

        //
        //=== PRIMARY
        //

        // create long-running network task for primary
        let primary_network = ConsensusNetwork::new_for_primary(
            network_config,
            self.consensus_bus.primary_network_events_cloned(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.tn_config.node_info.primary_network_address().clone(),
        )?;
        let primary_network_handle = primary_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Primary Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                },
                res = primary_network.run() => {
                    warn!(target: "epoch-manager", ?res, "primary network stopped");
                    Ok(res?)
                },
            )
        });

        // primary network handle
        self.primary_network_handle =
            Some(PrimaryNetworkHandle::new(primary_network_handle, network_config.chain_id()));

        // pass through the worker's RPC descriptor so peers can discover this
        // validator's JSON-RPC endpoint via kademlia. validators that did not
        // configure RPC leave the descriptor `None`. fail fast on a misconfigured
        // endpoint rather than advertising something peers will reject.
        let worker_rpc = self.builder.tn_config.node_info.p2p_info.worker.rpc.clone();
        if let Some(rpc) = &worker_rpc {
            rpc.validate()
                .wrap_err("invalid `node_info.p2p_info.worker.rpc` endpoint in node config")?;
        }

        // create long-running network task for worker
        let worker_network = ConsensusNetwork::new_for_worker(
            DEFAULT_WORKER_ID,
            network_config,
            self.worker_event_stream.clone(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.tn_config.node_info.worker_network_address().clone(),
            worker_rpc,
        )?;
        let worker_network_handle = worker_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Worker Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                }
                res = worker_network.run() => {
                    warn!(target: "epoch-manager", ?res, "worker network stopped");
                    Ok(res?)
                }
            )
        });

        // set temporary task spawner - this is updated with each epoch
        self.worker_network_handle = Some(WorkerNetworkHandle::new(
            worker_network_handle,
            node_task_spawner.clone(),
            epoch,
            network_config.chain_id(),
        ));

        Ok(())
    }

    /// Loop, starting a new epoch on each iteration until shutdown.
    ///
    /// Begins in [`RunEpochMode::Initial`]; each `run_epoch` call returns the [`RunEpochMode`] to
    /// carry into the next iteration, so the mode threads epoch-to-epoch state (e.g. whether this
    /// is a fresh start or a continuation). Any epoch error aborts the loop. After each epoch
    /// the consensus bus is reset and the task yields so the wrapping select can cancel it on
    /// shutdown; the loop also checks the shutdown notifier before starting the next epoch.
    async fn run_epochs(
        &mut self,
        engine: &ExecutionNode,
        network_config: NetworkConfig,
        to_engine: mpsc::Sender<ConsensusOutput>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        // initialize long-running components for node startup
        let mut run_epoch_mode = RunEpochMode::Initial;

        let node_ended_sub = self.node_shutdown.subscribe();

        // loop through epochs
        loop {
            let epoch_result = self
                .run_epoch(
                    engine,
                    &network_config,
                    &to_engine,
                    run_epoch_mode,
                    gas_accumulator.clone(),
                )
                .await;

            // ensure no errors
            run_epoch_mode = epoch_result.inspect_err(|e| {
                error!(target: "epoch-manager", ?e, "epoch returned error");
            })?;

            self.consensus_bus.reset_for_epoch();

            // Need a yield point so the task can be ended by the wrapping select when the node is
            // exiting.
            tokio::task::yield_now().await;

            // Make sure we don't start a new epoch when we are shutting down.
            if node_ended_sub.noticed() {
                break Ok(());
            }
            info!(target: "epoch-manager", "looping run epoch");
        }
    }

    /// Build the execution engine and its underlying reth environment.
    ///
    /// The reth env is wired to the shared `reth_db`, the configured base-fee address, and a clone
    /// of the live gas accumulator so execution, gas/fee accounting, and reward accounting all
    /// observe the same shared state.
    fn create_engine(
        &self,
        engine_task_manager: &TaskManager,
        gas_accumulator: &GasAccumulator,
    ) -> eyre::Result<ExecutionNode> {
        // create execution components (ie - reth env)
        let basefee_address = self.builder.tn_config.parameters.basefee_address;
        let reth_env = RethEnv::new(
            &self.builder.node_config,
            engine_task_manager,
            self.reth_db.clone(),
            basefee_address,
            gas_accumulator.clone(),
        )?;
        // Give the consensus bus a canonical-DB fallback for `wait_for_execution` (issue #1036):
        // an execution tip evicted from the in-memory `recent_blocks` ring but still persisted as
        // canonical in the DB must not be misread as a fork. The bus is constructed before the
        // engine exists, so the reader is wired here; a per-epoch rebuild simply refreshes it.
        self.consensus_bus.set_canonical_reader(Arc::new(reth_env.clone()));
        let engine = ExecutionNode::new(&self.builder, reth_env)?;

        Ok(engine)
    }

    /// Prime the consensus bus `recent_blocks` watch from the last executed blocks.
    ///
    /// On restart the in-memory `recent_blocks` history is empty; this backfills it (up to the
    /// watch's capacity) so consensus components can resolve recent consensus number/hash lookups.
    /// Each block's consensus hash is recovered from `parent_beacon_block_root`; round is set to 0
    /// because it is not persisted, which is sufficient for hash resolution during catch-up.
    async fn try_restore_state(&self, engine: &ExecutionNode) -> eyre::Result<()> {
        // prime the recent_blocks watch with latest executed blocks
        let block_capacity = self.consensus_bus.recent_blocks_capacity();

        for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
            // On restore, use the block's consensus hash from parent_beacon_block_root.
            // Round is set to 0 since we don't persist it; consensus number/hash still allows
            // wait_for_consensus_execution to resolve hash lookups.
            let consensus_hash: ConsensusHeaderDigest =
                recent_block.parent_beacon_block_root.unwrap_or_default().into();
            let (epoch, round) = deconstruct_nonce(recent_block.nonce.into());
            let consensus_number = self
                .consensus_chain
                .consensus_header_by_digest(epoch, consensus_hash)
                .await?
                .map(|h| h.number)
                .unwrap_or_default();
            let consensus_num_hash = ConsensusNumHash::new(consensus_number, consensus_hash);
            self.consensus_bus.recent_blocks().send_modify(|blocks| {
                blocks.push_latest(round, consensus_num_hash, Some(recent_block))
            });
        }

        // Startup consistency guard against an incomplete state restore. Resolve the executed tip's
        // producing consensus header from the tip's OWN nonce (epoch) and
        // `parent_beacon_block_root` (hash) via `last_executed_consensus_block` — the
        // slot-hint-immune signal — and hand both to `check_restore_consistency`, which
        // decides whether the pair is coherent.
        //
        // The `?` is load-bearing: `last_executed_consensus_block` distinguishes a legitimately
        // absent header (`Ok(None)`) from a failed storage lookup (`Err`), and only the former may
        // reach the guard. Collapsing a read failure into `None` would make this abort with the
        // "incomplete state restore" diagnosis below, which tells the operator to delete their
        // chain-data directories and re-import — catastrophic advice for a datadir that is
        // actually intact.
        let tip = self.consensus_bus.recent_blocks().borrow().latest_execution_block();
        let producing_header =
            state_sync::last_executed_consensus_block(&self.consensus_bus, &self.consensus_chain)
                .await?;
        check_restore_consistency(&tip, producing_header.as_ref())?;

        Ok(())
    }

    /// Spawn a task to update `ConsensusBus::recent_blocks` every time the engine processes a
    /// consensus output (with or without blocks).
    ///
    /// This is the live counterpart to [`try_restore_state`](Self::try_restore_state): the latter
    /// seeds `recent_blocks` once at startup, this keeps it current thereafter. If the engine
    /// update channel closes the engine is gone, so the task returns an error to bring the node
    /// down.
    fn spawn_engine_update_task(
        &self,
        mut engine_update: mpsc::Receiver<EngineUpdate>,
        task_manager: &TaskManager,
    ) {
        let consensus_bus = self.consensus_bus.clone();
        task_manager.spawn_critical_task("engine updates for consensus", async move {
            while let Some((latest_round, consensus_num_hash, latest_executed_block)) =
                engine_update.recv().await
            {
                consensus_bus.recent_blocks().send_modify(|blocks| {
                    blocks.push_latest(latest_round, consensus_num_hash, latest_executed_block)
                });
            }
            error!(target: "engine", "engine updates ended, node will exit");
            Err(TaskError::from_message("engine updates ended, node will exit"))
        });
    }
}

/// Refuse to start on an incomplete state restore: reth is populated to a tip past genesis but the
/// consensus store has no record of the consensus output that produced that tip.
///
/// `tip` is reth's canonical execution tip (as seeded into `recent_blocks`), and `producing_header`
/// is the tip's producing consensus header as resolved by
/// [`last_executed_consensus_block`](state_sync::last_executed_consensus_block) — `None` when the
/// header is absent from the consensus store. Absent means absent: that lookup surfaces a failed
/// storage read as an `Err` the caller propagates, so a read failure never arrives here disguised
/// as `None` and never triggers the destructive remediation advice below.
///
/// Invariant relied on: "execution follows consensus" — `save_consensus_output` persists a block's
/// producing consensus HEADER to the consensus store BEFORE that block executes. So any healthy
/// node that has executed block `B > 0` always resolves `B`'s producing header, whether it is
/// running, mid multi-epoch catch-up (epoch RECORDS may lag, but the header does not), or
/// restarting right at/after an epoch boundary (the closing epoch's pack is persisted static before
/// the next opens, so it stays readable). The only way the header is absent with a populated reth
/// tip is a state-only / partial / crashed-mid-import restore, which this rejects.
///
/// Genesis / fresh nodes (`tip.number == 0`) are exempt: the genesis header has no producing
/// consensus output, so `producing_header` is legitimately `None` there.
/// `last_executed_consensus_block` keys on the tip's OWN nonce/`parent_beacon_block_root`, never
/// the slot-hint-derived `consensus_header_latest`, so a legitimate node with a stale/torn resume
/// hint is not flagged.
fn check_restore_consistency(
    tip: &SealedHeader,
    producing_header: Option<&ConsensusHeader>,
) -> eyre::Result<()> {
    if tip.number > 0 && producing_header.is_none() {
        let (tip_epoch, _) = deconstruct_nonce(tip.nonce.into());
        return Err(eyre!(
            "datadir is an incomplete state restore: execution is at block {} (epoch {tip_epoch}) \
             but the consensus store has no record of the consensus that produced it. Remove the \
             chain-data directories (`db`, `static_files`, and `consensus-db`) under the datadir \
             and re-run `db load-state` with a COMPLETE export bundle — do NOT delete the datadir \
             itself, which holds your node keys. Or start from an empty datadir to sync from \
             genesis.",
            tip.number
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::{ExecHeader, B256};

    /// A tip sealed header at `number` whose nonce encodes `epoch` (upper 32 bits), matching the
    /// payload builder's `nonce = epoch << 32 | round` layout that `deconstruct_nonce` reads back.
    fn tip_at(number: u64, epoch: u32) -> SealedHeader {
        let header =
            ExecHeader { number, nonce: ((epoch as u64) << 32).into(), ..Default::default() };
        SealedHeader::new(header, B256::repeat_byte(0xab))
    }

    #[test]
    fn restore_guard_fires_on_populated_tip_without_producing_header() {
        // reth executed past genesis (block 5, epoch 7) but the tip's producing consensus header is
        // absent from the store — the incomplete-restore signature.
        let err = check_restore_consistency(&tip_at(5, 7), None)
            .expect_err("populated tip with no producing header must be refused");
        let msg = err.to_string();
        assert!(msg.contains("incomplete state restore"), "unexpected error: {msg}");
        assert!(msg.contains("block 5"), "error should name the tip block: {msg}");
        assert!(msg.contains("epoch 7"), "error should name the tip epoch: {msg}");
    }

    #[test]
    fn restore_guard_does_not_fire_at_genesis() {
        // fresh/genesis node: tip is block 0, which has no producing consensus output, so a None
        // header is legitimate and the guard must not fire.
        check_restore_consistency(&tip_at(0, 0), None).expect("genesis tip must not be refused");
    }

    #[test]
    fn restore_guard_does_not_fire_on_consistent_store() {
        // normal populated node: reth is past genesis and the tip's producing header resolves, so
        // the guard must not fire (covers a running node, mid-catch-up, and an epoch-boundary
        // restart — all of which resolve the header per the "execution follows consensus"
        // invariant).
        let header = ConsensusHeader::default();
        check_restore_consistency(&tip_at(5, 7), Some(&header))
            .expect("populated tip with a resolved producing header must not be refused");
    }
}
