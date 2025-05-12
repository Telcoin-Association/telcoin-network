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
use eyre::{eyre, OptionExt};
use std::{collections::HashMap, str::FromStr as _, sync::Arc, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{
    types::{NetworkHandle, NetworkInfo},
    ConsensusNetwork, PeerId, TNMessage,
};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_reth::{RethDb, RethEnv};
use tn_storage::{open_db, tables::ConsensusBlocks, DatabaseType};
use tn_types::{
    BatchValidation, BlsPublicKey, Committee, CommitteeBuilder, ConsensusHeader,
    Database as TNDatabase, Multiaddr, Noticer, Notifier, SealedBlock, TaskManager, TaskSpawner,
};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle};
use tokio::sync::{
    mpsc::{self},
    oneshot,
};
use tokio_stream::StreamExt as _;
use tracing::{debug, info};

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
    /// The oneshot sender to acknowledge the start of the first epoch.
    ///
    /// The node manager needs at least one task to await. This is an option
    /// that is held for the first epoch. The first epoch spawns node operatons,
    /// and returns the ack that the first epoch has started. At this point,
    /// the node manager has other tasks to await and will run normally.
    node_ready: Option<oneshot::Sender<()>>,
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
            node_ready: None,
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

        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();
        // create oneshot channel to await the first epoch to start
        let (start, ready) = oneshot::channel();
        node_task_spawner.spawn_task("epoch-started", ready);

        // track the status for when the node is ready
        self.node_ready = Some(start);
        // ensure task is tracked
        node_task_manager.update_tasks();

        info!(target: "epoch-manager", "starting node and launching first epoch");

        // read the network config or use the default
        let network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        self.spawn_node_networks(node_task_spawner, &network_config)?;

        // await all tasks on epoch-task-manager or node shutdown
        tokio::select! {
            // run long-living node tasks
            _ = node_task_manager.join_until_exit(self.node_shutdown.clone()) => {
                Err(eyre!("Node task shutdown"))
            }

            // loop through short-term epochs
            epoch_result = self.run_epochs(reth_db, consensus_db, network_config) => epoch_result
        }
    }

    /// Startup for the node. This creates all components on startup before starting the first
    /// epoch.
    ///
    /// This will create the long-running primary/worker [ConsensusNetwork]s for p2p swarm.
    fn spawn_node_networks(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
    ) -> eyre::Result<()> {
        //
        //=== PRIMARY
        //
        // this is a temporary event stream - replaced at the start of every epoch
        let (tmp_event_stream, _temp_rx) = mpsc::channel(1000);

        // create long-running network task for primary
        let primary_network = ConsensusNetwork::new_for_primary(
            network_config,
            tmp_event_stream,
            self.key_config.clone(),
        )?;
        let network_handle = primary_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Primary Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                },
                res = primary_network.run() => {
                    res
                },
            )
        });

        self.primary_network_handle = Some(PrimaryNetworkHandle::new(network_handle));

        //
        //=== WORKER
        //
        // this is a temporary event stream - replaced at the start of every epoch
        let (tmp_event_stream, _temp_rx) = mpsc::channel(1000);

        // create long-running network task for worker
        let worker_network = ConsensusNetwork::new_for_worker(
            network_config,
            tmp_event_stream,
            self.key_config.clone(),
        )?;
        let network_handle = worker_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Worker Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                }
                res = worker_network.run() => {
                    res
                }
            )
        });

        // set temporary task spawner - this is updated with each epoch
        self.worker_network_handle =
            Some(WorkerNetworkHandle::new(network_handle, node_task_spawner.clone()));

        Ok(())
    }

    /// Execute a loop to start new epochs until shutdown.
    async fn run_epochs(
        &mut self,
        reth_db: RethDb,
        consensus_db: DatabaseType,
        network_config: NetworkConfig,
    ) -> eyre::Result<()> {
        // The task manager that resets every epoch.
        // Short-running tasks for the lifetime of the epoch.
        let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);
        let mut running = true;

        // initialize networks to start listening for the first epoch
        let mut initialize_networks = true;

        // start the epoch
        while running {
            running = {
                let epoch_result = self
                    .run_epoch(
                        reth_db.clone(),
                        consensus_db.clone(),
                        &mut epoch_task_manager,
                        &network_config,
                        &mut initialize_networks,
                    )
                    .await;

                info!(target: "epoch-manager", ?epoch_result, "aborting epoch tasks for next epoch");

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
        epoch_task_manager: &mut TaskManager,
        network_config: &NetworkConfig,
        initialize_networks: &mut bool,
    ) -> eyre::Result<bool> {
        info!(target: "epoch-manager", "Starting epoch");

        let metrics_shutdown = Notifier::new();
        // start consensus metrics for the epoch
        if let Some(metrics_socket) = self.builder.consensus_metrics {
            start_prometheus_server(
                metrics_socket,
                epoch_task_manager,
                metrics_shutdown.subscribe(),
            );
        }

        // create submanager for engine tasks
        let mut engine_task_manager = TaskManager::new(ENGINE_TASK_MANAGER);

        // create the engine
        let engine = self.create_engine(&engine_task_manager, reth_db)?;

        // create primary and worker nodes
        let (primary, worker_node) = self
            .create_consensus(
                &engine,
                consensus_db,
                epoch_task_manager,
                network_config,
                initialize_networks,
            )
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
        epoch_task_manager.add_task_manager(engine_task_manager);

        info!(target: "epoch-manager", tasks=?epoch_task_manager, "EPOCH TASKS\n");

        // indicate node is ready if this is the first epoch
        if let Some(ack) = self.node_ready.take() {
            let _res = ack.send(());
            debug!(target: "epoch-manager", ?_res, "sent ack for node startup");
        }

        epoch_task_manager.join_until_exit(consensus_config.shutdown().clone()).await;

        // epoch complete
        let running = consensus_bus.restart();

        // reset to default - this is updated during restart sequence
        consensus_bus.clear_restart();

        info!(target: "epoch-manager", "TASKS complete, restart: {running}");

        Ok(running)
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
        epoch_task_manager: &TaskManager,
        network_config: &NetworkConfig,
        initialize_networks: &mut bool,
    ) -> eyre::Result<(PrimaryNode<DatabaseType>, WorkerNode<DatabaseType>)> {
        // send these to the swarm for validator discovery
        let _committee_bls_keys = self.create_committee_from_state(engine).await?;

        // create config for consensus
        let consensus_config = ConsensusConfig::new(
            self.builder.tn_config.clone(),
            &self.tn_datadir,
            consensus_db.clone(),
            self.key_config.clone(),
            network_config.clone(),
        )?;

        let primary = self
            .create_primary_node_components(
                &consensus_config,
                epoch_task_manager.get_spawner(),
                initialize_networks,
            )
            .await?;

        // only spawns one worker for now
        let worker = self
            .spawn_worker_node_components(
                &consensus_config,
                engine.new_batch_validator().await,
                epoch_task_manager.get_spawner(),
                initialize_networks,
            )
            .await?;

        // ensure initialized networks is false after the first run
        *initialize_networks = false;

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

    /// Create the [Committee] for the current epoch.
    ///
    /// This is the first step for configuring consensus.
    async fn create_committee_from_state(&self, engine: &ExecutionNode) -> eyre::Result<Committee> {
        info!(target: "epoch-manager", "creating committee from state");

        // retrieve the committee from state
        let epoch_state = engine.read_committee_from_chain().await?;
        let validators = epoch_state
            .validators
            .iter()
            .map(|v| {
                let decoded_bls = BlsPublicKey::from_bytes_on_chain(v.blsPubkey.as_ref());
                decoded_bls.map(|decoded| (decoded, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|err| eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;

        // retrieve network information for committee
        let primary_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("missing primary network handle for epoch manager")?;

        let mut primary_network_infos = primary_handle
            .inner_handle()
            .find_authorities(validators.keys().cloned().collect())
            .await?;

        let epoch_boundary = epoch_state.epoch_start + epoch_state.epoch_info.epochDuration as u64;

        // build the committee
        let mut committee_builder = CommitteeBuilder::new(epoch_state.epoch, epoch_boundary);
        // TODO: how to set the epoch duration?
        // - need to include `prev_epoch: Option<EpochInfo>` on EpochState
        // - if on the first epoch (is that 0 or 1 ?) return `None`, otherwise `Some(prev_epoch)`
        // - also include epoch_num from tn_reth call for architecture reasons
        // - the canonical tip will have the epoch number, so easier to parse in engine level

        while let Some(info) = primary_network_infos.next().await {
            debug!(target: "epoch-manager", ?info, "awaited next primary network info");
            // TODO: multiaddrs can be more than one, but info only takes one
            let (protocol_key, NetworkInfo { pubkey, multiaddr, hostname }) = info??;
            let validator = validators
                .get(&protocol_key)
                .ok_or_eyre("network returned validator that isn't in the committee")?;
            let execution_address = validator.validatorAddress;

            committee_builder.add_authority(
                protocol_key,
                1, // set stake so every authority's weight is equal
                multiaddr,
                execution_address,
                pubkey,
                hostname,
            );
        }

        let committee = committee_builder.build();

        Ok(committee)
    }

    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
    async fn create_primary_node_components<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        epoch_task_spawner: TaskSpawner,
        initialize_networks: &bool,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let consensus_bus =
            ConsensusBus::new_with_args(consensus_config.config().parameters.gc_depth);
        let state_sync = StateSynchronizer::new(consensus_config.clone(), consensus_bus.clone());
        let network_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("primary network handle missing from epoch manager")?
            .clone();

        // create the epoch-specific `PrimaryNetwork`
        self.create_primary_network_for_epoch(
            consensus_config,
            &consensus_bus,
            state_sync.clone(),
            epoch_task_spawner.clone(),
            &network_handle,
            initialize_networks,
        )
        .await?;

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
        epoch_task_spawner: TaskSpawner,
        initialize_networks: &bool,
    ) -> eyre::Result<WorkerNode<DB>> {
        // only support one worker for now - otherwise, loop here
        let (worker_id, _worker_info) = consensus_config.config().workers().first_worker()?;

        // update the network handle's task spawner for reporting batches in the epoch
        {
            let network_handle = self
                .worker_network_handle
                .as_mut()
                .ok_or_eyre("worker network handle missing from epoch manager")?;
            network_handle.update_task_spawner(epoch_task_spawner.clone());
        }

        let network_handle = self
            .worker_network_handle
            .as_ref()
            .ok_or_eyre("worker network handle missing from epoch manager")?
            .clone();

        self.create_worker_network_for_epoch(
            consensus_config,
            worker_id,
            validator.clone(),
            epoch_task_spawner,
            &network_handle,
            initialize_networks,
        )
        .await?;

        let worker = WorkerNode::new(
            *worker_id,
            consensus_config.clone(),
            network_handle.clone(),
            validator,
        );

        Ok(worker)
    }

    /// Create the primary network for the specific epoch.
    ///
    /// This is not the swarm level, but the [PrimaryNetwork] interface.
    async fn create_primary_network_for_epoch<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &PrimaryNetworkHandle,
        initialize_networks: &bool,
    ) -> eyre::Result<()> {
        // create event streams for the primary network handler
        let (event_stream, rx_event_stream) = mpsc::channel(1000);

        // set committee for network to prevent banning
        network_handle
            .inner_handle()
            .new_epoch(consensus_config.primary_network_map(), event_stream)
            .await?;

        // start listening if the network needs to be initialized
        if *initialize_networks {
            // start listening for p2p messages
            let primary_address = consensus_config.primary_address();
            let primary_multiaddr =
                Self::get_multiaddr_from_env_or_config("PRIMARY_MULTIADDR", primary_address);
            network_handle.inner_handle().start_listening(primary_multiaddr).await?;
        }

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                consensus_config.network_config().libp2p_config().primary_topic(),
                consensus_config.committee_peer_ids(),
            )
            .await?;

        // always dial peers for the new epoch
        for (authority_id, addr, _) in consensus_config
            .committee()
            .others_primaries_by_id(consensus_config.authority_id().as_ref())
        {
            let peer_id = authority_id.peer_id();
            self.dial_peer(
                network_handle.inner_handle().clone(),
                peer_id,
                addr,
                epoch_task_spawner.clone(),
            );
        }

        // wait until the primary has connected with at least 1 peer
        let mut peers = network_handle.connected_peers().await.map(|p| p.len()).unwrap_or(0);
        while peers == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            peers = network_handle.connected_peers().await.map(|p| p.len()).unwrap_or(0);
        }

        // spawn primary network
        PrimaryNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            consensus_bus.clone(),
            state_sync,
            epoch_task_spawner.clone(), // tasks should abort with epoch
        )
        .spawn(&epoch_task_spawner);

        Ok(())
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
    async fn create_worker_network_for_epoch<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &WorkerNetworkHandle,
        initialize_networks: &bool,
    ) -> eyre::Result<()> {
        // create event streams for the worker network handler
        let (event_stream, rx_event_stream) = mpsc::channel(1000);
        let worker_address = consensus_config.worker_address(worker_id);

        network_handle
            .inner_handle()
            .new_epoch(consensus_config.worker_network_map(), event_stream)
            .await?;

        // start listening if the network needs to be initialized
        if *initialize_networks {
            let worker_multiaddr =
                Self::get_multiaddr_from_env_or_config("WORKER_MULTIADDR", worker_address.clone());
            network_handle.inner_handle().start_listening(worker_multiaddr).await?;
        }

        // always dial peers for the new epoch
        for (peer_id, addr) in consensus_config.worker_cache().all_workers() {
            if addr != worker_address {
                self.dial_peer(
                    network_handle.inner_handle().clone(),
                    peer_id,
                    addr,
                    epoch_task_spawner.clone(),
                );
            }
        }

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe(consensus_config.network_config().libp2p_config().worker_txn_topic())
            .await?;

        // spawn worker network
        WorkerNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            *worker_id,
            validator,
        )
        .spawn(&epoch_task_spawner);

        Ok(())
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
