//! The node/epoch manager type.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.
//! This file defines the struct and the node/application scoped code.

use crate::{
    engine::{ExecutionNode, TnBuilder},
    health::HealthcheckServer,
    manager::spawn_epoch_vote_collector,
};
use eyre::eyre;
use state_sync::spawn_fetch_consensus;
use tn_config::{KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{types::NetworkEvent, ConsensusNetwork};
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp, NodeMode, QueChannel};
use tn_reth::{system_calls::EpochState, RethDb, RethEnv};
use tn_storage::{consensus::ConsensusChain, open_db, DatabaseType};
use tn_types::{
    deconstruct_nonce, gas_accumulator::GasAccumulator, BlockNumHash, ConsensusHeader,
    ConsensusOutput, Database as TNDatabase, EngineUpdate, Epoch, Notifier, TaskError, TaskManager,
    TaskSpawner, TimestampSec, MIN_PROTOCOL_BASE_FEE,
};
use tn_worker::{WorkerNetworkHandle, WorkerRequest, WorkerResponse};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod epoch;
use epoch::RunEpochMode;

/// The long-running task manager name.
const NODE_TASK_MANAGER: &str = "Node Task Manager";

/// The worker's base task manager name. This is used by `fn worker_task_manager_name(id)`.
pub(crate) const WORKER_TASK_BASE: &str = "Worker Task";

/// The long-running type that oversees epoch transitions.
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
    /// Reth DB, keep for entire execution.
    reth_db: RethDb,
    /// Consensus DB, keep for entire execution.
    consensus_db: DB,
    /// ConsensusBus for the application life.
    consensus_bus: ConsensusBusApp,
    /// Persistent event stream for worker network events.
    worker_event_stream: QueChannel<NetworkEvent<WorkerRequest, WorkerResponse>>,

    /// The last consenses header for a closing epoch.
    last_consensus_header: Option<ConsensusHeader>,

    /// Track the last consensus number that was actually forwarded to the execution engine.
    /// This prevents waiting on consensus that was saved to the DB but never sent to the engine.
    last_forwarded_consensus_number: u64,

    /// Access to the epoch pack files storing consensus data.
    consensus_chain: ConsensusChain,
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

/// Create a consensus DB that lives for program lifetime.
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
    /// Create a new instance of [Self].
    pub(crate) async fn new(
        builder: TnBuilder,
        tn_datadir: P,
        consensus_db: DB,
        key_config: KeyConfig,
    ) -> Self {
        // Note this can only fail if the consensus DB is very broken (bad path for instance).
        // So we will panic for now, this will kill the node on startup for a critical error.
        let epochs_db_path = tn_datadir.epochs_db_path();
        let _ = std::fs::create_dir_all(&epochs_db_path);
        let consensus_chain = ConsensusChain::new(epochs_db_path).expect("open consensus DB");
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

        Self {
            builder,
            tn_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            key_config,
            node_shutdown,
            epoch_boundary: Default::default(),
            reth_db,
            consensus_db,
            consensus_bus,
            worker_event_stream,
            last_consensus_header: None,
            last_forwarded_consensus_number: 0,
            consensus_chain,
        }
    }

    /// Run the node, handling epoch transitions.
    pub(crate) async fn run(&mut self) -> eyre::Result<()> {
        // Surface any errors that may have been triggered on create.
        self.consensus_chain.persist_current().await?;
        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();

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

        // spawn three critical workers that will fetch epoch pack files from an epoch work queue.
        // Note, these workers will just go dormant once we have caught up- that's ok.
        for i in 0..3 {
            node_task_manager.spawn_critical_task(
                format!("epoch-consensus-worker-{i}"),
                spawn_fetch_consensus(
                    self.node_shutdown.subscribe(),
                    self.consensus_bus.clone(),
                    primary_network_handle.clone(),
                    i,
                    self.consensus_chain.clone(),
                ),
            );
        }

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

    /// Startup for the node. This creates all components on startup before starting the first
    /// epoch.
    ///
    /// This will create the long-running primary/worker [ConsensusNetwork]s for p2p swarm.
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
                    Ok::<_, TaskError>(())
                },
                res = primary_network.run() => {
                    warn!(target: "epoch-manager", ?res, "primary network stopped");
                    Ok(res?)
                },
            )
        });

        // primary network handle
        self.primary_network_handle = Some(PrimaryNetworkHandle::new(primary_network_handle));

        // create long-running network task for worker
        let worker_network = ConsensusNetwork::new_for_worker(
            network_config,
            self.worker_event_stream.clone(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.tn_config.node_info.worker_network_address().clone(),
        )?;
        let worker_network_handle = worker_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Worker Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok::<_, TaskError>(())
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

    /// Execute a loop to start new epochs until shutdown.
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

    /// Helper method to create all engine components.
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

    /// Helper method to restore execution state for the consensus components.
    async fn try_restore_state(&self, engine: &ExecutionNode) -> eyre::Result<()> {
        // prime the recent_blocks watch with latest executed blocks
        let block_capacity = self.consensus_bus.recent_blocks_capacity();

        for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
            // On restore, use the block's consensus hash from parent_beacon_block_root.
            // Round is set to 0 since we don't persist it; consensus number/hash still allows
            // wait_for_consensus_execution to resolve hash lookups.
            let consensus_hash = recent_block.parent_beacon_block_root.unwrap_or_default();
            let (epoch, round) = deconstruct_nonce(recent_block.nonce.into());
            let consensus_number = self
                .consensus_chain
                .consensus_header_by_digest(epoch, consensus_hash)
                .await?
                .map(|h| h.number)
                .unwrap_or_default();
            let consensus_num_hash = BlockNumHash::new(consensus_number, consensus_hash);
            self.consensus_bus.recent_blocks().send_modify(|blocks| {
                blocks.push_latest(round, consensus_num_hash, Some(recent_block))
            });
        }

        Ok(())
    }

    /// Spawn a task to update `ConsensusBus::recent_blocks` every time the engine processes a
    /// consensus output (with or without blocks).
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
            Err(eyre::eyre!("engine updates ended, node will exit"))
        });
    }
}
