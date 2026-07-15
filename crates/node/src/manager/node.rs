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
        spawn_epoch_vote_collector,
    },
    metrics::EpochMetrics,
};
use eyre::{eyre, WrapErr as _};
use state_sync::{request_missing_packs, spawn_fetch_consensus, spawn_fetch_recent_consensus};
use std::collections::{BTreeMap, HashMap};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{types::NetworkEvent, ConsensusNetwork};
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp, NodeMode, QueChannel};
use tn_reth::{system_calls::EpochState, RethDb, RethEnv};
use tn_storage::{consensus::ConsensusChain, open_db, DatabaseType};
use tn_types::{
    deconstruct_nonce,
    gas_accumulator::{GasAccumulator, WorkerFeeConfig},
    BlsPublicKey, BootstrapServer, Committee, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusNumHash, ConsensusOutput, Database as TNDatabase, EngineUpdate, Epoch, Notifier,
    SealedHeader, TaskError, TaskManager, TaskSpawner, TimestampSec, WorkerId, B256,
    DEFAULT_WORKER_ID, MIN_PROTOCOL_BASE_FEE,
};
use tn_worker::{WorkerNetworkHandle, WorkerRequest, WorkerResponse};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod close_epoch;
mod run_epoch;
mod start_epoch;
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
/// count and every worker's base fee for the entered epoch, deriving them from the previous
/// epoch's closing-block state on every entry.
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
        // wrong epoch's state. Startup heals the finalized marker to the persisted canonical
        // tip (`RethEnv::heal_finalized_to_persisted_tip`) before this restore runs, so the
        // benign crash-window lag can no longer reach this comparison — a firing guard means
        // the two views still disagree after the heal: genuine database inconsistency.
        // Investigate the database; do not restart-loop past this.
        if epoch_state.epoch != canonical_epoch {
            return Err(eyre!(
                "startup accumulator restore: finalized header {} pins epoch {} but the \
                 canonical tip reports epoch {canonical_epoch} — the views disagree across an \
                 epoch boundary even though startup heals the finalized marker to the canonical \
                 tip before this restore. This is database inconsistency, not the benign \
                 crash-window marker lag; refusing to rebuild gas stats against the wrong \
                 epoch's state",
                block.number,
                epoch_state.epoch,
            ));
        }

        // Size the accumulator from the on-chain worker count BEFORE the per-worker writes
        // below, reading at the epoch's first block's parent resolved from the SAME pinned
        // epoch state.
        sync_num_workers_from_chain(
            &reth_env,
            gas_accumulator,
            epoch_state.epoch_info.blockHeight,
        )?;

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
/// FAIL-HARD: any read failure (header unresolvable, `WorkerConfigs` contract absent or
/// unreadable) is an error. Both callers - [`catchup_accumulator`] at startup and the epoch-0
/// arm of `run_epoch`'s entry seeding - must not proceed on an unverifiable count: per-worker
/// writes keyed by worker id would land in a wrongly sized accumulator.
///
/// Reading at the closing block also makes the count value-stable for the whole epoch: a
/// mid-epoch (ModeChange) re-entry re-reads the identical count while the engine may still be
/// executing leftover output, so the resize is a no-op - the value-stability contract on
/// [`GasAccumulator::set_num_workers`]. No caller needs to quiesce execution first.
pub fn sync_num_workers_from_chain(
    reth_env: &RethEnv,
    gas_accumulator: &GasAccumulator,
    epoch_first_block: u64,
) -> eyre::Result<()> {
    let read_block = epoch_first_block.saturating_sub(1);
    let header = reth_env
        .sealed_header_by_number(read_block)
        .wrap_err_with(|| format!("failed to read header {read_block} while syncing worker count"))?
        .ok_or_else(|| eyre!("no header at block {read_block} while syncing worker count"))?;

    let (num_workers, _configs) =
        reth_env.get_worker_fee_configs_at_block(header.hash()).wrap_err_with(|| {
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

/// Worker id encoded in a header's `difficulty` (low 16 bits of `batch_index << 16 | worker_id`).
pub(crate) fn worker_id_from_header(header: &SealedHeader) -> WorkerId {
    (header.difficulty.into_limbs()[0] & 0xffff) as u16
}

/// Return the most recent on-chain `base_fee_per_gas` for each worker that produced a block in
/// `headers`.
///
/// `headers` are the executed reth blocks for the current epoch (epoch-start height..=tip), in
/// ascending block-number order, so the last header seen for a worker is its latest block. The
/// worker id is read from each header's `difficulty` field (lower 16 bits, matching how
/// [`GasAccumulator::inc_block`] callers encode it). Workers that produced no block in the range
/// are absent from the returned map.
///
/// Used to seed per-worker base fees from the chain on sync and restart, preserving the
/// base-fee-from-chain invariant (see the [`tn_types::gas_accumulator`] module docs).
pub(crate) fn latest_base_fee_per_worker(headers: &[SealedHeader]) -> HashMap<WorkerId, u64> {
    let mut fees = HashMap::new();
    for header in headers {
        let worker_id = worker_id_from_header(header);
        if let Some(base_fee) = header.base_fee_per_gas {
            fees.insert(worker_id, base_fee);
        }
    }
    fees
}

/// True when `header` is a genuine worker batch block.
///
/// Two on-chain block shapes are NOT worker batch blocks and must be excluded from per-worker
/// fee/gas attribution:
/// - the genesis block (`number == 0`), which carries no worker payload, and
/// - the synthetic empty-close block the engine builds when an epoch closes with no batches. That
///   block is stamped worker 0 and copies its PARENT's base fee (see `tn_engine`'s
///   `execute_consensus_output`), so attributing it would poison worker 0 with another worker's
///   fee. It is identified by `ommers_hash == B256::ZERO`: the header's `ommers_hash` carries the
///   batch digest, and only the synthetic block passes `B256::ZERO` (real batch digests are never
///   zero).
///
/// A non-empty epoch-closing block built from real batches has a non-zero `ommers_hash` and IS a
/// genuine worker block.
pub(crate) fn is_worker_batch_block(header: &SealedHeader) -> bool {
    header.number != 0 && header.ommers_hash != B256::ZERO
}

/// Sum `gas_used` per worker over `headers`.
///
/// Blocks with `gas_used == 0` are skipped, mirroring [`GasAccumulator::inc_block`]'s early
/// return, so a fold over one epoch's genuine worker blocks is arithmetically identical to the
/// gas totals the live accumulator held at that epoch's close (header `gas_used` counts only user
/// transactions; system calls never touch it). Workers with no gas-consuming block in the range
/// are absent from the returned map.
pub(crate) fn gas_used_per_worker(headers: &[SealedHeader]) -> HashMap<WorkerId, u64> {
    let mut totals: HashMap<WorkerId, u64> = HashMap::new();
    for header in headers {
        if header.gas_used == 0 {
            continue;
        }
        let worker_id = worker_id_from_header(header);
        *totals.entry(worker_id).or_default() += header.gas_used;
    }
    totals
}

/// Fold each configured worker's next-epoch base fee from the fee it held and the gas it used
/// during the epoch that just closed.
///
/// One slot per entry in `configs` (the on-chain `WorkerConfigs` order, indexed by worker id).
/// A worker present in `held_fees` folds through `next_base_fee_for_config` — the SAME formula
/// `adjust_base_fees` (in the `run_epoch` module) applies at a live epoch close, so entry
/// derivation and close-time adjustment produce identical values from identical inputs. A worker
/// absent from `held_fees` (no genuine block in the scanned range, so the chain does not reveal
/// the fee it held) yields `None`: callers must leave that worker's container untouched.
pub fn fold_next_epoch_base_fees(
    configs: &[WorkerFeeConfig],
    held_fees: &HashMap<WorkerId, u64>,
    gas_totals: &HashMap<WorkerId, u64>,
) -> Vec<Option<u64>> {
    configs
        .iter()
        .enumerate()
        .map(|(worker_id, config)| {
            let worker_id = worker_id as WorkerId;
            held_fees.get(&worker_id).map(|&held_fee| {
                let gas_used = gas_totals.get(&worker_id).copied().unwrap_or_default();
                run_epoch::next_base_fee_for_config(*config, held_fee, gas_used)
            })
        })
        .collect()
}

/// Per-worker base fees for an entered epoch, derived purely from the previous epoch's on-chain
/// state by [`derive_base_fees_for_entered_epoch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedBaseFees {
    /// The on-chain worker count read from `WorkerConfigs` at the previous epoch's closing block
    /// (i.e. the count for the entered epoch).
    pub num_workers: usize,
    /// One slot per configured worker. [`derive_base_fees_for_entered_epoch`] fills EVERY slot:
    /// workers with a genuine block in the scanned range fold from that block's fee, and workers
    /// without one derive from earlier closing-block state via [`derive_idle_worker_fee`]. `None`
    /// (leave the container untouched) survives only as `fold_next_epoch_base_fees`' intermediate
    /// for callers that fold without the walk-back fill.
    pub fees: Vec<Option<u64>>,
    /// The previous epoch's per-worker `gas_used` totals from the header scan. Exposed so tests
    /// can pin scan ≡ [`GasAccumulator::inc_block`] equivalence.
    pub gas_totals: HashMap<WorkerId, u64>,
}

impl DerivedBaseFees {
    /// Install the derived fees into `gas_accumulator`.
    ///
    /// Resizes the accumulator to `num_workers` FIRST (so every configured slot exists), then
    /// sets the base fee for `Some` slots only; `None` slots keep their current value. Gas
    /// counters are deliberately untouched — the entered epoch starts at zero gas.
    pub fn apply(&self, gas_accumulator: &GasAccumulator) {
        gas_accumulator.set_num_workers(self.num_workers);
        for (worker_id, fee) in self.fees.iter().enumerate() {
            if let Some(fee) = fee {
                gas_accumulator.base_fee(worker_id as WorkerId).set_base_fee(*fee);
            }
        }
    }
}

/// Derive the per-worker base fees for `entered_epoch` from the previous epoch's on-chain state.
///
/// The entered epoch's fee is a PURE function of prior-epoch chain data: the previous epoch's
/// per-worker gas totals, each worker's last held fee (both read from that epoch's genuine worker
/// blocks), and the `WorkerConfigs` strategies at `closing_header` (the previous epoch's closing
/// block). Live producers crossing the boundary, restarted nodes, and re-entering nodes therefore
/// all converge on the identical value through this one seam — including the recovery shapes
/// where `close_epoch(None, ..)` skipped the close-time adjustment entirely.
///
/// Steps:
/// 1. `getEpochInfo(entered_epoch - 1)` at `closing_header` yields the previous epoch's first block
///    (clamped to 1: constructor-seeded epochs report `blockHeight = 0`).
/// 2. Scan the sealed headers `first..=closing`, keeping only genuine worker batch blocks
///    ([`is_worker_batch_block`] — excludes genesis and the synthetic empty-close block).
/// 3. Extract each worker's last held fee ([`latest_base_fee_per_worker`]) and gas total
///    ([`gas_used_per_worker`]) from the filtered slice.
/// 4. Fold through the per-worker strategies read at `closing_header`
///    ([`fold_next_epoch_base_fees`]).
/// 5. Any worker WITHOUT a genuine block in the scanned range (the fold's `None` slots) derives its
///    fee from earlier closing-block state instead ([`derive_idle_worker_fee`]), so `apply`
///    installs a fee for EVERY configured worker. Leaving such a slot at the fresh-restart MIN
///    default would diverge from the live committee, whose close-time `adjust_base_fees` computed
///    the idle worker's fee in memory.
///
/// Errors (registry/config read failures, unresolvable headers) must bubble: fees are exact-match
/// consensus values, so producing with an unverifiable fee is a safety failure while halting is
/// only a single-node liveness failure.
pub fn derive_base_fees_for_entered_epoch(
    reth_env: &RethEnv,
    entered_epoch: Epoch,
    closing_header: &SealedHeader,
) -> eyre::Result<DerivedBaseFees> {
    let scan_start = std::time::Instant::now();
    let prior_epoch = entered_epoch
        .checked_sub(1)
        .ok_or_else(|| eyre!("cannot derive base fees for entered epoch 0: no prior epoch"))?;

    // the previous epoch's block range, read from the registry AT the closing block (the ring
    // buffer holds the four most recent epochs, so the prior epoch is always resolvable here)
    let epoch_info = reth_env.get_epoch_info_at_block(prior_epoch, closing_header.hash())?;
    let range = epoch_info.blockHeight.max(1)..=closing_header.number;
    let range_len = range.end().saturating_sub(*range.start()).saturating_add(1);

    let headers = reth_env.blocks_for_range(range.clone())?;
    let genuine: Vec<SealedHeader> = headers.into_iter().filter(is_worker_batch_block).collect();

    let held_fees = latest_base_fee_per_worker(&genuine);
    let gas_totals = gas_used_per_worker(&genuine);

    // worker strategies and count at the closing block = the entered epoch's configuration
    let (num_workers, configs) = reth_env.get_worker_fee_configs_at_block(closing_header.hash())?;
    let mut fees = fold_next_epoch_base_fees(&configs, &held_fees, &gas_totals);

    // Workers with no genuine block in the prior epoch still have a chain-derivable fee: walk
    // back through earlier closing blocks (last-produced anchor / `Static` pin / slot-creation
    // anchor / epoch-0 MIN base case) so every configured worker recovers the value the live
    // committee holds in memory.
    for (worker_id, fee) in fees.iter_mut().enumerate() {
        if fee.is_none() {
            *fee = Some(derive_idle_worker_fee_at(
                reth_env,
                entered_epoch,
                worker_id as WorkerId,
                closing_header,
            )?);
        }
    }

    info!(
        target: "epoch-manager",
        entered_epoch,
        prior_epoch,
        range = ?range,
        range_len,
        genuine_blocks = genuine.len(),
        num_workers,
        elapsed = ?scan_start.elapsed(),
        "derived entered-epoch base fees from prior epoch chain state"
    );

    Ok(DerivedBaseFees { num_workers, fees, gas_totals })
}

/// One boundary record collected by [`derive_idle_worker_fee`]'s backward walk.
///
/// A step describes the boundary INTO some epoch `k`: the worker's strategy read at epoch
/// `k - 1`'s closing block, plus what epoch `k - 1`'s genuine blocks reveal about the worker.
/// Folding a step ([`fold_forward`]) reproduces the `next_base_fee_for_config` application the
/// live committee ran at that boundary's close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EpochFeeStep {
    /// The worker's [`WorkerFeeConfig`] in force for the entered epoch, read at the boundary's
    /// closing block (the only state allowed to define an epoch's configuration).
    config: WorkerFeeConfig,
    /// The base fee carried by the worker's LAST genuine block in the epoch that closed at this
    /// boundary. `None` when the worker produced no genuine block — the chain does not reveal
    /// the fee it held, so the walk must continue to an earlier anchor.
    held_fee: Option<u64>,
    /// The worker's summed `gas_used` over that epoch's genuine blocks (zero when idle).
    gas_used: u64,
}

/// Fold a worker's base fee forward through boundary `steps` in CHRONOLOGICAL order.
///
/// Starting from `anchor` — the fee the worker held during the epoch below the earliest step's
/// boundary — each step applies `next_base_fee_for_config(step.config, current, step.gas_used)`,
/// the SAME formula a live `adjust_base_fees` ran at that boundary. The result is the fee the
/// worker holds entering the epoch above the last step's boundary.
pub(crate) fn fold_forward(anchor: u64, steps: &[EpochFeeStep]) -> u64 {
    steps.iter().fold(anchor, |current, step| {
        run_epoch::next_base_fee_for_config(step.config, current, step.gas_used)
    })
}

/// Derive the base fee worker `worker_id` holds during `epoch` purely from on-chain state —
/// including workers whose fee is NOT observable from `epoch`'s own blocks because they never
/// produced one.
///
/// `latest_base_fee_per_worker`-style recovery only sees workers with on-chain blocks; an idle
/// (or governance-added) worker's fee exists solely in live nodes' memory until its first block.
/// This walk reconstructs that value: starting at the boundary into `epoch`, it steps backward
/// one epoch boundary at a time, collecting one [`EpochFeeStep`] per boundary, until it reaches a
/// chain-observable anchor, then folds forward ([`fold_forward`]) through the collected steps.
///
/// The walk resolves its first boundary from the pinned finalized header; see
/// [`derive_idle_worker_fee_at`] for the per-boundary anchor rules and the closing-block pinning
/// discipline. Errors bubble (same policy as [`derive_base_fees_for_entered_epoch`]): fees are
/// exact-match consensus values, so halting beats producing with an unverifiable fee.
pub fn derive_idle_worker_fee(
    reth_env: &RethEnv,
    epoch: Epoch,
    worker_id: WorkerId,
) -> eyre::Result<u64> {
    // epoch-0 base case short-circuits before any chain read (see derive_idle_worker_fee_at)
    if epoch == 0 {
        return Ok(MIN_PROTOCOL_BASE_FEE);
    }
    let entry_header = reth_env.finalized_header()?.ok_or_else(|| {
        eyre!("no finalized header to derive worker {worker_id}'s base fee for epoch {epoch}")
    })?;
    derive_idle_worker_fee_at(reth_env, epoch, worker_id, &entry_header)
}

/// [`derive_idle_worker_fee`] with the walk's first epoch-info read pinned to `entry_header`
/// instead of a fresh `finalized_header()` read, so entry paths that already hold the pinned
/// header keep every read on one source.
///
/// `entry_header` must carry registry state whose 4-epoch ring buffer covers `epoch` — true for
/// both production entry shapes (the previous epoch's closing block, whose registry state IS
/// `epoch`; or a mid-`epoch` tip). Every read is self-validating: `getEpochInfo` reverts on an
/// epoch outside the ring buffer, so a revert means the walk's assumptions were genuinely
/// violated and the error bubbles.
///
/// Walking `k = epoch, epoch - 1, ..`, each step resolves `S` = epoch `k - 1`'s closing block
/// (`getEpochInfo(k).blockHeight - 1`; `concludeEpoch` stamps the entered epoch's first block at
/// the close), reads the worker's strategy at `S` — configuration reads happen ONLY at closing
/// blocks, because the previous epoch's closing state rules the entire epoch — and scans epoch
/// `k - 1`'s genuine blocks. First match wins:
/// 1. The worker produced in `k - 1`: anchor at its last genuine block's fee (fees only move at
///    boundaries, so that block reveals the fee held all epoch) with the epoch's summed gas.
/// 2. The strategy at `S` is `Static`: stop — the collected step pins the fee regardless of
///    anything deeper (the anchor value is irrelevant; `fold_forward_static_anchor_absorbs_history`
///    pins this).
/// 3. The slot does not exist at `S`: the worker was governance-added at the first boundary ABOVE
///    this one (already collected), so anchor at the fresh-slot MIN default — folding the earliest
///    collected step then applies `next(config, MIN, 0)`, exactly how a live `adjust_base_fees`
///    prices a governance-added worker at its first boundary.
/// 4. Otherwise (an `Eip1559` strategy and no blocks): record the idle step and keep walking.
/// 5. Reaching epoch 0 anchors at `MIN_PROTOCOL_BASE_FEE` for every worker and strategy, with no
///    config read at genesis: containers hold the MIN default until the FIRST epoch close, so
///    configured fees activate entering epoch 1 — recovery must mirror that live behavior.
///
/// `k` strictly decreases to an anchor or epoch 0, so the walk terminates in at most `epoch`
/// steps; real walks are short (producing workers anchor in one step, `Static` absorbs
/// immediately, new slots anchor at creation). Each step costs one epoch-info read, one config
/// read, and one header-range scan. NOTE: the walk reads historical headers and historical
/// contract state; TN nodes run reth in archive mode (no pruning), so full history is available.
pub(crate) fn derive_idle_worker_fee_at(
    reth_env: &RethEnv,
    epoch: Epoch,
    worker_id: WorkerId,
    entry_header: &SealedHeader,
) -> eyre::Result<u64> {
    // Epoch-0 base case: every container holds MIN until the first close. Do NOT price
    // the genesis configuration here — it activates entering epoch 1.
    if epoch == 0 {
        return Ok(MIN_PROTOCOL_BASE_FEE);
    }
    let walk_start = std::time::Instant::now();

    // steps collect newest-boundary-first; reversed into chronological order for the fold
    let mut steps: Vec<EpochFeeStep> = Vec::new();
    let mut k = epoch;
    // epoch info for `k`, read at the current pinned state of the walk (initially the entry
    // header, then each boundary's closing block — always within the registry's ring buffer)
    let mut epoch_info = reth_env.get_epoch_info_at_block(k, entry_header.hash())?;

    let anchor = loop {
        // S = epoch k-1's closing block; a zero blockHeight for a started epoch k >= 1 means
        // the chain view is genuinely inconsistent
        let closing_number = epoch_info.blockHeight.checked_sub(1).ok_or_else(|| {
            eyre!("epoch {k} reports blockHeight 0 while deriving worker {worker_id}'s base fee")
        })?;
        let closing = reth_env.sealed_header_by_number(closing_number)?.ok_or_else(|| {
            eyre!("missing sealed header {closing_number} (epoch {} closing block)", k - 1)
        })?;

        // the strategy in force for epoch k, read at the closing block ONLY
        let (_num_workers, configs) = reth_env.get_worker_fee_configs_at_block(closing.hash())?;
        let Some(config) = configs.get(worker_id as usize).copied() else {
            // rule 3: slot absent at this boundary — anchor at the fresh-slot MIN default
            break MIN_PROTOCOL_BASE_FEE;
        };

        // scan epoch k-1's genuine worker blocks for this worker's last fee and summed gas
        let prior_info = reth_env.get_epoch_info_at_block(k - 1, closing.hash())?;
        let headers = reth_env.blocks_for_range(prior_info.blockHeight.max(1)..=closing_number)?;
        let genuine: Vec<SealedHeader> =
            headers.into_iter().filter(is_worker_batch_block).collect();
        let held_fee = latest_base_fee_per_worker(&genuine).get(&worker_id).copied();
        let gas_used = gas_used_per_worker(&genuine).get(&worker_id).copied().unwrap_or_default();

        steps.push(EpochFeeStep { config, held_fee, gas_used });

        // rule 1: the worker produced in k-1 — its last genuine block anchors the fold
        if let Some(held) = steps.last().and_then(|step| step.held_fee) {
            break held;
        }
        // rule 2: Static pins regardless of deeper history — the pushed step absorbs the anchor
        if matches!(config, WorkerFeeConfig::Static { .. }) {
            break MIN_PROTOCOL_BASE_FEE;
        }

        // rule 4: idle Eip1559 — continue to the previous boundary
        k -= 1;
        if k == 0 {
            // rule 5: epoch-0 base case
            break MIN_PROTOCOL_BASE_FEE;
        }
        epoch_info = prior_info;
    };

    steps.reverse();
    let fee = fold_forward(anchor, &steps);

    debug!(
        target: "epoch-manager",
        epoch,
        worker_id,
        anchor,
        fee,
        walk_depth = steps.len(),
        elapsed = ?walk_start.elapsed(),
        "derived idle worker base fee from prior closing-block configs"
    );

    Ok(fee)
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
            metrics: EpochMetrics::default(),
        })
    }

    /// Build the process-lifetime components, then drive the epoch loop until shutdown.
    ///
    /// Startup proceeds in order: create the execution engine and start it, heal any
    /// crash-window finalized-marker lag to the persisted canonical tip
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
        self.last_forwarded_consensus_number =
            state_sync::last_consensus_parent(&self.consensus_bus, &self.consensus_chain).await.1;

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

        // Heal a finalized marker left lagging the persisted canonical tip by a crash between
        // the blocks-commit and finalize-commit transactions BEFORE anything reads the marker:
        // the catchup below pins every chain-derived input to the finalized header.
        let reth_env = engine.get_reth_env().await;
        reth_env.heal_finalized_to_persisted_tip()?;
        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        // The canonical epoch cross-checks the finalized header catchup pins its reads to.
        catchup_accumulator(reth_env, &gas_accumulator, &mut self.consensus_chain, epoch).await?;

        // read the network config or use the default, then stamp the genesis chain id
        // onto it so every wire protocol and gossip topic is chain-namespaced (issue
        // #765). Genesis is the single source of truth; this one value is read by the
        // network builder, the gossip handles, and the gossip-validation handlers.
        let mut network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        network_config.set_chain_id(self.builder.tn_config.genesis().config.chain_id);
        self.spawn_node_networks(node_task_spawner, &network_config, epoch).await?;
        let primary_network_handle =
            self.primary_network_handle.as_ref().expect("primary network").clone();
        // The epoch-vote and consensus-output gossip topics are subscribed per epoch in
        // `spawn_primary_network_for_epoch` with a committee-restricted publisher set (issue
        // #912), alongside `primary_topic`, so their authorized publishers track committee
        // rotation. They are intentionally not subscribed here in the process-lifetime path.
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

        self.try_restore_state(&engine).await?;
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
    /// The reth env is wired to the shared `reth_db`, the configured base-fee address, and the
    /// accumulator's rewards counter so execution and reward accounting stay consistent.
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
            gas_accumulator.rewards_counter(),
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::{
        gas_accumulator::compute_next_base_fee_eip1559, ExecHeader, MIN_PROTOCOL_BASE_FEE, U256,
    };

    /// Build a sealed header shaped like an executed worker block for scan tests.
    ///
    /// `worker_id` lands in the low 16 bits of `difficulty` (matching the payload builder's
    /// `batch_index << 16 | worker_id` encoding read back by `worker_id_from_header`).
    fn scan_header(
        number: u64,
        worker_id: WorkerId,
        gas_used: u64,
        base_fee: u64,
        ommers_hash: B256,
    ) -> SealedHeader {
        let header = ExecHeader {
            number,
            gas_used,
            difficulty: U256::from(worker_id),
            base_fee_per_gas: Some(base_fee),
            ommers_hash,
            ..Default::default()
        };
        SealedHeader::new(header, B256::repeat_byte(0xab))
    }

    /// Non-zero stand-in for a real batch digest carried in `ommers_hash`.
    fn batch_digest() -> B256 {
        B256::repeat_byte(1)
    }

    #[test]
    fn is_worker_batch_block_excludes_genesis_and_synthetic_close() {
        // a real executed batch block: non-genesis number, non-zero batch digest
        let genuine = scan_header(5, 0, 21_000, 7, batch_digest());
        assert!(is_worker_batch_block(&genuine));

        // genesis carries no worker payload
        let genesis = scan_header(0, 0, 0, 7, batch_digest());
        assert!(!is_worker_batch_block(&genesis));

        // the synthetic empty-close block passes batch digest B256::ZERO (and is stamped
        // worker 0 with its parent's fee - the poison the filter exists to exclude)
        let synthetic = scan_header(9, 0, 0, 7, B256::ZERO);
        assert!(!is_worker_batch_block(&synthetic));
    }

    #[test]
    fn gas_used_per_worker_sums_per_worker_and_ignores_zero_gas() {
        let headers = vec![
            scan_header(1, 0, 100, 7, batch_digest()),
            scan_header(2, 1, 50, 7, batch_digest()),
            scan_header(3, 0, 200, 7, batch_digest()),
            // zero-gas blocks are skipped, mirroring GasAccumulator::inc_block's early return
            scan_header(4, 1, 0, 7, batch_digest()),
            // a worker whose only block used zero gas is absent entirely
            scan_header(5, 2, 0, 7, batch_digest()),
        ];

        let totals = gas_used_per_worker(&headers);

        assert_eq!(totals.get(&0), Some(&300));
        assert_eq!(totals.get(&1), Some(&50));
        assert_eq!(totals.get(&2), None);
        assert_eq!(totals.len(), 2);
    }

    #[test]
    fn fold_next_epoch_fees_multi_worker_mixed_strategies() {
        // worker 0 Static, worker 1 Eip1559 - each slot folds independently from its own
        // held fee and gas total
        let configs = [
            WorkerFeeConfig::Static { fee: 12_345 },
            WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 },
        ];
        let held_fees = HashMap::from([(0u16, 999u64), (1u16, 1_000_000u64)]);
        let gas_totals = HashMap::from([(0u16, 5_000u64), (1u16, 2_000_000u64)]);

        let fees = fold_next_epoch_base_fees(&configs, &held_fees, &gas_totals);

        assert_eq!(fees.len(), 2);
        // static pins to the governance value regardless of held fee and gas
        assert_eq!(fees[0], Some(12_345));
        // eip1559 at 2x target rises by the max 12.5%
        assert_eq!(fees[1], Some(1_125_000));
        assert_eq!(fees[1], Some(compute_next_base_fee_eip1559(1_000_000, 2_000_000, 1_000_000)));
    }

    #[test]
    fn fold_next_epoch_fees_absent_worker_is_none() {
        // worker 1 has a config but produced no genuine block in the scanned range, so the
        // chain does not reveal the fee it held: its slot must be None (container untouched)
        let configs = [WorkerFeeConfig::Static { fee: 500 }, WorkerFeeConfig::Static { fee: 600 }];
        let held_fees = HashMap::from([(0u16, 7u64)]);
        let gas_totals = HashMap::from([(0u16, 100u64)]);

        let fees = fold_next_epoch_base_fees(&configs, &held_fees, &gas_totals);

        assert_eq!(fees, vec![Some(500), None]);
    }

    #[test]
    fn fold_matches_next_base_fee_for_config() {
        // THE single-formula pin: for identical (config, held fee, gas) inputs the entry
        // derivation's fold must equal next_base_fee_for_config - the function the live
        // producer's close-time adjust_base_fees applies - so entry derivation ≡ close-time
        // adjustment.
        let cases = [
            (WorkerFeeConfig::Static { fee: 12_345 }, MIN_PROTOCOL_BASE_FEE, 0u64),
            (WorkerFeeConfig::Static { fee: 1 }, 1_000_000, 42),
            (WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 }, 1_000_000, 2_000_000),
            (WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 }, 1_000_000, 0),
            (WorkerFeeConfig::Eip1559 { target_gas: u64::MAX }, MIN_PROTOCOL_BASE_FEE, 5_000_000),
        ];

        for (config, held_fee, gas_used) in cases {
            let fees = fold_next_epoch_base_fees(
                &[config],
                &HashMap::from([(0u16, held_fee)]),
                &HashMap::from([(0u16, gas_used)]),
            );
            assert_eq!(
                fees,
                vec![Some(run_epoch::next_base_fee_for_config(config, held_fee, gas_used))],
                "fold diverged from next_base_fee_for_config for {config:?}",
            );
        }
    }

    /// An idle boundary step for the walk-back fold: no blocks, no gas.
    fn idle_step(config: WorkerFeeConfig) -> EpochFeeStep {
        EpochFeeStep { config, held_fee: None, gas_used: 0 }
    }

    #[test]
    fn fold_forward_decays_eip1559_per_idle_epoch() {
        // a worker idle for N epochs under an Eip1559 strategy decays once per boundary: each
        // step applies next_base_fee_for_config(cfg, current, 0), exactly the chain a live
        // committee's per-close adjust_base_fees would have run
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 };
        let steps = [idle_step(cfg); 3];
        let anchor = 1_000_000u64;

        let mut oracle = anchor;
        for _ in 0..steps.len() {
            oracle = compute_next_base_fee_eip1559(oracle, 0, 1_000_000);
        }

        assert_eq!(fold_forward(anchor, &steps), oracle);
        // the decay is real: three idle boundaries move a non-MIN fee down (and never below MIN)
        assert!((MIN_PROTOCOL_BASE_FEE..anchor).contains(&oracle));
        // and each boundary equals the one-formula seam
        assert_eq!(
            fold_forward(anchor, &steps[..1]),
            run_epoch::next_base_fee_for_config(cfg, anchor, 0),
        );
    }

    #[test]
    fn fold_forward_static_anchor_absorbs_history() {
        // a Static boundary pins the fee regardless of anything deeper: two wildly different
        // anchors converge to the same result. This property is what lets the walk stop at a
        // Static config without resolving deeper history.
        let eip = idle_step(WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 });
        let steps = [idle_step(WorkerFeeConfig::Static { fee: 500 }), eip, eip];

        assert_eq!(fold_forward(MIN_PROTOCOL_BASE_FEE, &steps), fold_forward(9_999_999, &steps));
        // and the result is exactly the fold from the pinned 500 through the later boundaries
        assert_eq!(fold_forward(MIN_PROTOCOL_BASE_FEE, &steps), fold_forward(500, &steps[1..]));
    }

    #[test]
    fn fold_forward_from_min_stays_min() {
        // Eip1559 decay from MIN floors at MIN, so a never-produced worker walking to the
        // epoch-0 base case folds trivially: MIN in, MIN out, any number of idle boundaries
        let steps = [idle_step(WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 }); 4];
        assert_eq!(fold_forward(MIN_PROTOCOL_BASE_FEE, &steps), MIN_PROTOCOL_BASE_FEE);
    }

    #[test]
    fn fold_forward_anchors_min_at_worker_creation_boundary() {
        // a slot that first appears at boundary S' anchors at MIN below it, so the earliest
        // step applies next(config@S', MIN, 0) — how a live adjust_base_fees prices a
        // governance-added worker at its first boundary
        let creation_static = idle_step(WorkerFeeConfig::Static { fee: 800 });
        assert_eq!(
            fold_forward(MIN_PROTOCOL_BASE_FEE, &[creation_static]),
            run_epoch::next_base_fee_for_config(
                WorkerFeeConfig::Static { fee: 800 },
                MIN_PROTOCOL_BASE_FEE,
                0,
            ),
        );

        // an Eip1559 creation strategy prices the new worker at MIN
        let creation_eip = idle_step(WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 });
        assert_eq!(fold_forward(MIN_PROTOCOL_BASE_FEE, &[creation_eip]), MIN_PROTOCOL_BASE_FEE);

        // later boundaries fold from the creation-priced fee
        assert_eq!(
            fold_forward(MIN_PROTOCOL_BASE_FEE, &[creation_static, creation_eip]),
            run_epoch::next_base_fee_for_config(
                WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 },
                800,
                0,
            ),
        );
    }
}
