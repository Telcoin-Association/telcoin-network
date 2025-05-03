//! The epoch manager type.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.

use crate::{
    engine::{ExecutionNode, TnBuilder},
    primary::PrimaryNode,
    worker::WorkerNode,
};
use consensus_metrics::start_prometheus_server;
use eyre::eyre;
use std::{str::FromStr as _, sync::Arc, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{types::NetworkHandle, ConsensusNetwork, PeerId, TNMessage};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_reth::{RethDb, RethEnv};
use tn_storage::{open_db, tables::ConsensusBlocks, DatabaseType};
use tn_types::{
    BatchValidation, ConsensusHeader, Database as TNDatabase, Multiaddr, Noticer, Notifier,
    SealedBlock, TaskManager, TaskSpawner,
};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle};
use tokio::sync::mpsc;
use tracing::info;

/// The long-running task manager name.
const NODE_TASK_MANAGER: &str = "Node Task Manager";

/// The epoch-specific task manager name.
const EPOCH_TASK_MANAGER: &str = "Epoch Task Manager";

/// The execution engine task manager name.
const ENGINE_TASK_MANAGER: &str = "Engine Task Manager";

/// The primary task manager name.
pub(super) const PRIMARY_TASK_MANAGER: &str = "Primary Task Manager";

/// The worker's base task manager name. This is used by `fn worker_task_manager_name(id)`.
pub(super) const WORKER_TASK_MANAGER_BASE: &str = "Worker Task Manager";

/// The long-running type that oversees epoch transitions.
#[derive(Debug)]
pub struct EpochManager<P> {
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
}

impl<P> EpochManager<P>
where
    P: TelcoinDirs + 'static,
{
    /// Create a new instance of [Self].
    pub fn new(
        builder: TnBuilder,
        tn_datadir: P,
        passphrase: Option<String>,
    ) -> eyre::Result<Self> {
        let passphrase = if std::fs::exists(
            tn_datadir.validator_keys_path().join(tn_config::BLS_WRAPPED_KEYFILE),
        )
        .unwrap_or(false)
        {
            passphrase
        } else {
            None
        };

        // create key config for lifetime of the app
        let key_config = KeyConfig::read_config(&tn_datadir, passphrase)?;

        // shutdown long-running node components
        let node_shutdown = Notifier::new();

        Ok(Self {
            builder,
            tn_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            key_config,
            node_shutdown,
        })
    }

    /// Run the node, handling epoch transitions.
    ///
    /// This will bring up a tokio runtime and start the app within it.
    /// It also will shutdown this runtime, potentially violently, to make
    /// sure any lefteover tasks are ended.  This allows it to be called more
    /// than once per program execution to support changing modes of the
    /// running node.
    pub async fn run(&mut self) -> eyre::Result<()> {
        // create dbs to survive between sync state transitions
        let reth_db =
            RethEnv::new_database(&self.builder.node_config, self.tn_datadir.reth_db_path())?;

        // open the consensus db for this epoch
        //
        // TODO: consensus db should be epoch-specific - see issue #269
        let consensus_db_path = self.tn_datadir.consensus_db_path();
        info!(target: "epoch-manager", ?consensus_db_path, "node storage opened for epoch");
        let consensus_db = open_db(consensus_db_path);

        // start initial epoch
        let mut running = true;

        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);

        // The task manager that resets every epoch.
        // Short-running tasks for the lifetime of the epoch.
        let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);

        while running {
            running = {
                let epoch_result = self
                    .run_epoch(
                        reth_db.clone(),
                        consensus_db.clone(),
                        &mut node_task_manager,
                        &mut epoch_task_manager,
                    )
                    .await;
                // abort all epoch-related tasks
                epoch_task_manager.abort_all_tasks();

                // create new manager for next epoch
                epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);

                // return result after aborting all tasks
                epoch_result?
            }
        }

        Ok(())
    }

    /// Run a single epoch.
    ///
    /// If it returns Ok(true) this indicates a mode change occurred and a restart
    /// is required.
    async fn run_epoch(
        &mut self,
        reth_db: RethDb,
        consensus_db: DatabaseType,
        node_task_manager: &mut TaskManager,
        epoch_task_manager: &mut TaskManager,
    ) -> eyre::Result<bool> {
        info!(target: "epoch-manager", "Starting node");

        // start consensus metrics for the epoch
        if let Some(metrics_socket) = self.builder.consensus_metrics {
            start_prometheus_server(metrics_socket, epoch_task_manager);
        }

        // create submanager for engine tasks
        let mut engine_task_manager = TaskManager::new(ENGINE_TASK_MANAGER);

        // create the engine
        let engine = self.create_engine(&engine_task_manager, reth_db)?;

        // create primary and worker nodes
        let (primary, worker_node) = self
            .create_consensus(&engine, consensus_db, node_task_manager, epoch_task_manager)
            .await?;

        // create receiving channel before spawning primary to ensure messages are not lost
        let consensus_bus = primary.consensus_bus().await;
        let consensus_output_rx = consensus_bus.subscribe_consensus_output();

        // start primary
        let mut primary_task_manager = primary.start().await?;

        // start worker
        let (mut worker_task_manager, worker) = worker_node.start().await?;

        // consensus config for shutdown subscribers
        let consensus_config = primary.consensus_config().await;

        // start engine
        engine
            .start_engine(
                consensus_output_rx,
                &engine_task_manager,
                consensus_config.shutdown().subscribe(),
            )
            .await?;

        engine
            .start_batch_builder(
                worker.id(),
                worker.batches_tx(),
                &engine_task_manager,
                consensus_config.shutdown().subscribe(),
            )
            .await?;

        // update tasks
        primary_task_manager.update_tasks();
        engine_task_manager.update_tasks();
        worker_task_manager.update_tasks();

        // add epoch-specific tasks to manager
        epoch_task_manager.add_task_manager(primary_task_manager);
        epoch_task_manager.add_task_manager(worker_task_manager);

        // add long-running tasks to manager
        epoch_task_manager.add_task_manager(engine_task_manager);

        info!(target: "epoch-manager", tasks=?epoch_task_manager, "EPOCH TASKS");
        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS");

        // await all tasks on epoch-task-manager or node shutdown
        tokio::select! {
            _ = node_task_manager.join_until_exit(self.node_shutdown.clone()) => {
                Err(eyre!("Node task shutdown"))
            }
            _ = epoch_task_manager.join_until_exit(consensus_config.shutdown().clone()) => {
                // epoch complete
                let running = consensus_bus.restart();

                // reset to default - this is updated during restart sequence
                consensus_bus.clear_restart();

                info!(target: "epoch-manager", "TASKS complete, restart: {running}");

                Ok(running)
            }
        }
    }

    /// Helper method to create all engine components.
    fn create_engine(
        &self,
        engine_task_manager: &TaskManager,
        reth_db: RethDb,
    ) -> eyre::Result<ExecutionNode> {
        // create execution components (ie - reth env)
        let reth_env = RethEnv::new(&self.builder.node_config, engine_task_manager, reth_db)?;
        let engine = ExecutionNode::new(&self.builder, reth_env)?;

        Ok(engine)
    }

    /// Helper method to create all consensus-related components for this epoch.
    ///
    /// Consensus components are short-lived and only relevant for the current epoch.
    async fn create_consensus(
        &mut self,
        engine: &ExecutionNode,
        consensus_db: DatabaseType,
        node_task_manager: &TaskManager,
        epoch_task_manager: &TaskManager,
    ) -> eyre::Result<(PrimaryNode<DatabaseType>, WorkerNode<DatabaseType>)> {
        // create config for consensus
        let network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        let consensus_config = ConsensusConfig::new(
            self.builder.tn_config.clone(),
            &self.tn_datadir,
            consensus_db.clone(),
            self.key_config.clone(),
            network_config,
        )?;

        let primary = self
            .create_primary_node_components(
                &consensus_config,
                node_task_manager,
                epoch_task_manager.get_spawner(),
            )
            .await?;

        // only spawns one worker for now
        let worker = self
            .spawn_worker_node_components(
                &consensus_config,
                engine.new_batch_validator().await,
                node_task_manager,
            )
            .await?;

        // set execution state for consensus
        let consensus_bus = primary.consensus_bus().await;
        self.try_restore_state(&consensus_bus, &consensus_db, engine).await?;

        let primary_network_handle = primary.network_handle().await;
        let _mode = self
            .identify_node_mode(&consensus_bus, &consensus_config, &primary_network_handle)
            .await?;

        // spawn task to update the latest execution results for consensus
        //
        // NOTE: this should live and die with epochs because it updates the consensus bus
        self.spawn_engine_update_task(
            consensus_config.shutdown().subscribe(),
            consensus_bus.clone(),
            engine.canonical_block_stream().await,
            epoch_task_manager,
        );

        Ok((primary, worker))
    }

    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
    async fn create_primary_node_components<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        node_task_manager: &TaskManager,
        epoch_task_spawner: TaskSpawner,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let consensus_bus =
            ConsensusBus::new_with_args(consensus_config.config().parameters.gc_depth);
        let state_sync = StateSynchronizer::new(consensus_config.clone(), consensus_bus.clone());

        // use primary network if it exist or spawn the network
        let network_handle = if let Some(handle) = self.primary_network_handle.as_ref() {
            handle.clone()
        } else {
            let handle = self
                .spawn_primary_network(
                    consensus_config,
                    &consensus_bus,
                    state_sync.clone(),
                    node_task_manager,
                    epoch_task_spawner,
                )
                .await?;
            self.primary_network_handle = Some(handle.clone());
            handle
        };

        // spawn primary - create node and spawn network
        let primary =
            PrimaryNode::new(consensus_config.clone(), consensus_bus, network_handle, state_sync);

        Ok(primary)
    }

    /// Create a [WorkerNode].
    async fn spawn_worker_node_components<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        validator: Arc<dyn BatchValidation>,
        node_task_manager: &TaskManager,
    ) -> eyre::Result<WorkerNode<DB>> {
        // only support one worker for now - otherwise, loop here
        let (worker_id, _worker_info) = consensus_config.config().workers().first_worker()?;

        // use worker network if it exist or spawn the network
        let network_handle = if let Some(handle) = self.worker_network_handle.as_ref() {
            handle.clone()
        } else {
            let handle = self
                .spawn_worker_network(
                    consensus_config,
                    worker_id,
                    validator.clone(),
                    node_task_manager,
                )
                .await?;
            self.worker_network_handle = Some(handle.clone());
            handle
        };

        let worker =
            WorkerNode::new(*worker_id, consensus_config.clone(), network_handle, validator);

        Ok(worker)
    }

    /// Create the primary network.
    async fn spawn_primary_network<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        node_task_manager: &TaskManager,
        epoch_task_spawner: TaskSpawner,
    ) -> eyre::Result<PrimaryNetworkHandle> {
        // create event streams for the primary network handler
        let (event_stream, rx_event_stream) = mpsc::channel(1000);
        let network = ConsensusNetwork::new_for_primary(consensus_config, event_stream)?;
        let network_handle = network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_manager.spawn_task("Primary Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                }
                res = network.run() => {
                    res
                }
            )
        });

        // set committee for network to prevent banning
        network_handle.new_epoch(consensus_config.primary_network_map()).await?;

        // subscribe to epoch closing gossip messages
        network_handle
            .subscribe(
                consensus_config.network_config().libp2p_config().primary_topic(),
                consensus_config.committee_peer_ids(),
            )
            .await?;

        // start listening for p2p messages
        let primary_address = consensus_config.primary_address();
        let primary_multiaddr =
            Self::get_multiaddr_from_env_or_config("PRIMARY_MULTIADDR", primary_address);
        network_handle.start_listening(primary_multiaddr).await?;

        // dial peers
        for (authority_id, addr, _) in consensus_config
            .committee()
            .others_primaries_by_id(consensus_config.authority_id().as_ref())
        {
            let peer_id = authority_id.peer_id();
            self.dial_peer(network_handle.clone(), peer_id, addr, node_task_manager.get_spawner());
        }

        // wait until the primary has connected with at least 1 peer
        let mut peers = network_handle.connected_peers().await.map(|p| p.len()).unwrap_or(0);
        while peers == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            peers = network_handle.connected_peers().await.map(|p| p.len()).unwrap_or(0);
        }

        // spawn primary network
        let primary_network_handle = PrimaryNetworkHandle::new(network_handle);
        PrimaryNetwork::new(
            rx_event_stream,
            primary_network_handle.clone(),
            consensus_config.clone(),
            consensus_bus.clone(),
            state_sync,
            epoch_task_spawner, // tasks should abort with epoch
        )
        .spawn(node_task_manager);

        Ok(primary_network_handle)
    }

    /// Check the environment to possibly overwrite the host.
    fn get_multiaddr_from_env_or_config(env_var: &str, fallback: Multiaddr) -> Multiaddr {
        let multiaddr = std::env::var(env_var)
            .ok()
            .and_then(|addr_str| Multiaddr::from_str(&addr_str).ok())
            .unwrap_or(fallback);
        info!(target: "node", ?multiaddr, env_var);
        multiaddr
    }

    /// Dial peer.
    fn dial_peer<Req: TNMessage, Res: TNMessage>(
        &self,
        handle: NetworkHandle<Req, Res>,
        peer_id: PeerId,
        peer_addr: Multiaddr,
        node_task_spawner: TaskSpawner,
    ) {
        // spawn dials on long-running task manager
        let task_name = format!("DialPeer {peer_id}");
        node_task_spawner.spawn_task(task_name, async move {
            let mut backoff = 1;
            while let Err(e) = handle.dial(peer_id, peer_addr.clone()).await {
                tracing::warn!(target: "node", "failed to dial {peer_id} at {peer_addr}: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 120 {
                    backoff += backoff;
                }
                if let Ok(peers) = handle.connected_peers().await {
                    if peers.contains(&peer_id) {
                        return;
                    };
                }
            }
        });
    }

    /// Create the worker network.
    async fn spawn_worker_network<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        node_task_manager: &TaskManager,
    ) -> eyre::Result<WorkerNetworkHandle> {
        // create event streams for the worker network handler
        let (event_stream, rx_event_stream) = mpsc::channel(1000);
        let network = ConsensusNetwork::new_for_worker(consensus_config, event_stream)?;
        let network_handle = network.network_handle();
        let rx_shutdown = self.node_shutdown.subscribe();

        // spawn long-running worker network task
        node_task_manager.spawn_task("Worker Network", async move {
            tokio::select!(
                _ = &rx_shutdown => {
                    Ok(())
                }
                res = network.run() => {
                    res
                }

            )
        });

        let worker_address = consensus_config.worker_address(worker_id);
        let worker_multiaddr =
            Self::get_multiaddr_from_env_or_config("WORKER_MULTIADDR", worker_address.clone());
        network_handle.start_listening(worker_multiaddr).await?;
        network_handle.new_epoch(consensus_config.worker_network_map()).await?;

        for (peer_id, addr) in consensus_config.worker_cache().all_workers() {
            if addr != worker_address {
                self.dial_peer(
                    network_handle.clone(),
                    peer_id,
                    addr,
                    node_task_manager.get_spawner(),
                );
            }
        }

        // spawn worker network
        let worker_network_handle = WorkerNetworkHandle::new(network_handle);
        WorkerNetwork::new(
            rx_event_stream,
            worker_network_handle.clone(),
            consensus_config.clone(),
            *worker_id,
            validator,
        )
        .spawn(node_task_manager);

        Ok(worker_network_handle)
    }

    /// Helper method to restore execution state for the consensus components.
    async fn try_restore_state(
        &self,
        consensus_bus: &ConsensusBus,
        db: &DatabaseType,
        engine: &ExecutionNode,
    ) -> eyre::Result<()> {
        // prime the recent_blocks watch with latest executed blocks
        let block_capacity = consensus_bus.recent_blocks().borrow().block_capacity();
        for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
            consensus_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(recent_block));
        }

        // prime the last consensus header from the DB
        let (_, last_db_block) =
            db.last_record::<ConsensusBlocks>().unwrap_or_else(|| (0, ConsensusHeader::default()));
        consensus_bus.last_consensus_header().send(last_db_block)?;

        Ok(())
    }

    /// Helper method to identify the node's mode:
    /// - "Committee-voting Validator" (CVV)
    /// - "Non-voting Validator" (NVV)
    /// - "Observer"
    ///
    /// This method also updates the `ConsensusBus::node_mode()`.
    async fn identify_node_mode<DB: TNDatabase>(
        &self,
        consensus_bus: &ConsensusBus,
        consensus_config: &ConsensusConfig<DB>,
        primary_network_handle: &PrimaryNetworkHandle,
    ) -> eyre::Result<NodeMode> {
        let mode = if self.builder.tn_config.observer {
            NodeMode::Observer
        } else if state_sync::can_cvv(consensus_bus, consensus_config, primary_network_handle).await
        {
            NodeMode::CvvActive
        } else {
            NodeMode::CvvInactive
        };

        // update consensus bus
        consensus_bus.node_mode().send_modify(|v| *v = mode);

        Ok(mode)
    }

    /// Spawn a task to update `ConsensusBus::recent_blocks` everytime the engine produces a new
    /// final block.
    fn spawn_engine_update_task(
        &self,
        shutdown: Noticer,
        consensus_bus: ConsensusBus,
        mut engine_state: mpsc::Receiver<SealedBlock>,
        epoch_task_manager: &TaskManager,
    ) {
        // spawn epoch-specific task to forward blocks from the engine to consensus
        epoch_task_manager.spawn_task("latest execution block", async move {
            loop {
                tokio::select!(
                    _ = &shutdown => {
                        info!(target: "engine", "received shutdown from consensus to stop updating consensus bus recent blocks");
                        break;
                    }
                    latest = engine_state.recv() => {
                        if let Some(latest) = latest {
                            consensus_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(latest.header));
                        } else {
                            break;
                        }
                    }
                )
            }
        });
    }
}
