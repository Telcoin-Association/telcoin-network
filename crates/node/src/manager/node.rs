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

use std::collections::BTreeMap;

use crate::{
    engine::{ExecutionNode, TnBuilder},
    health::HealthcheckServer,
    manager::spawn_epoch_vote_collector,
    metrics::EpochMetrics,
};
use eyre::{eyre, WrapErr as _};
use state_sync::{request_missing_packs, spawn_fetch_consensus, spawn_fetch_recent_consensus};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{types::NetworkEvent, ConsensusNetwork};
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp, NodeMode, QueChannel};
use tn_reth::{system_calls::EpochState, RethDb, RethEnv};
use tn_storage::{consensus::ConsensusChain, open_db, DatabaseType};
use tn_types::{
    deconstruct_nonce, gas_accumulator::GasAccumulator, BlsPublicKey, BootstrapServer, Committee,
    ConsensusHeader, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput,
    Database as TNDatabase, EngineUpdate, Epoch, Notifier, TaskError, TaskManager, TaskSpawner,
    TimestampSec, DEFAULT_WORKER_ID, MIN_PROTOCOL_BASE_FEE,
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

/// Restore the [`GasAccumulator`] state after a mid-epoch restart.
///
/// This is the first of three recovery stages (see the module docs on
/// [`tn_types::gas_accumulator`] for the full picture). It runs once at startup, before
/// execution resumes, and performs the following:
///
/// 1. **Base fee** — sets worker 0's base fee from the finalized reth header. In a multi-worker
///    configuration this will need per-worker restoration.
/// 2. **Gas stats** — iterates every reth block from the epoch's start height through the finalized
///    tip, extracting the worker id from each block's `difficulty` field and calling
///    [`GasAccumulator::inc_block`] to rebuild per-worker gas totals.
/// 3. **Leader counts** — walks the consensus DB in reverse, counting each leader's committed
///    blocks for rounds that have already been executed (i.e. `leader_round <=
///    last_executed_round`). Rounds beyond the last executed round are intentionally skipped
///    because [`EpochManager::replay_missed_consensus`] will re-execute them, which increments
///    leader counts through the normal payload-builder path.
///
/// If there is no finalized header (fresh genesis), this is a no-op.
pub async fn catchup_accumulator(
    reth_env: RethEnv,
    gas_accumulator: &GasAccumulator,
    consensus_chain: &mut ConsensusChain,
) -> eyre::Result<()> {
    if let Some(block) = reth_env.finalized_header()? {
        let epoch_state = reth_env.epoch_state_from_canonical_tip()?;

        // Note WORKER: In a single worker world this should be suffecient to set the base fee.
        // In a multi-worker world (future) this will NOT work and needs updating.
        gas_accumulator
            .base_fee(0)
            .set_base_fee(block.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE));

        let nonce: u64 = block.nonce.into();
        let (last_executed_epoch, last_executed_round) = deconstruct_nonce(nonce);

        let blocks =
            reth_env.blocks_for_range(epoch_state.epoch_info.blockHeight..=block.number)?;

        // loop through blocks to accumulate gas stats
        for current in blocks {
            let gas = current.gas_used;
            let limit = current.gas_limit;

            // difficulty contains the worker id and batch index:
            // `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`
            let lower64 = current.difficulty.into_limbs()[0];
            let worker_id = (lower64 & 0xffff) as u16;
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
    /// Startup proceeds in order: create the execution engine and start it, recover the
    /// [`GasAccumulator`] via [`catchup_accumulator`], spawn the long-running p2p networks
    /// ([`spawn_node_networks`](Self::spawn_node_networks)), subscribe the primary to the
    /// epoch-vote and consensus-output gossip topics, spawn the epoch-record and vote
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
        self.last_forwarded_consensus_number = self.consensus_chain.latest_consensus_number();

        info!(target: "epoch-manager", "starting node and launching first epoch");

        // create channels for engine that survive the lifetime of the node
        let (to_engine, for_engine) = mpsc::channel(1000);

        // Create our epoch gas accumulator, we currently have one worker.
        // All nodes have to agree on the worker count, do not change this for an existing chain.
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

        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        catchup_accumulator(
            engine.get_reth_env().await,
            &gas_accumulator,
            &mut self.consensus_chain,
        )
        .await?;

        // read the network config or use the default
        let network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        self.spawn_node_networks(node_task_spawner, &network_config, epoch).await?;
        let primary_network_handle =
            self.primary_network_handle.as_ref().expect("primary network").clone();
        primary_network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::epoch_vote_topic())
            .await?;
        primary_network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::consensus_output_topic())
            .await?;
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

        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // spawn node healthcheck service if enabled
        if let Some(port) = self.builder.healthcheck {
            let _ = HealthcheckServer::spawn(node_task_manager.get_spawner(), port).await;
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
        self.primary_network_handle = Some(PrimaryNetworkHandle::new(primary_network_handle));

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
        self.worker_network_handle =
            Some(WorkerNetworkHandle::new(worker_network_handle, node_task_spawner.clone(), epoch));

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
