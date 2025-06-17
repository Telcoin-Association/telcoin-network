//! The epoch manager type.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.

use crate::{
    engine::{ExecutionNode, TnBuilder},
    primary::PrimaryNode,
    worker::WorkerNode,
    EngineToPrimaryRpc,
};
use consensus_metrics::start_prometheus_server;
use eyre::{eyre, OptionExt};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tn_config::{
    Config, ConfigFmt, ConfigTrait as _, ConsensusConfig, KeyConfig, NetworkConfig, TelcoinDirs,
};
use tn_network_libp2p::{
    error::NetworkError,
    types::{NetworkHandle, NetworkInfo},
    ConsensusNetwork, PeerId, TNMessage,
};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_reth::{
    system_calls::{ConsensusRegistry, EpochState},
    CanonStateNotificationStream, RethDb, RethEnv,
};
use tn_storage::{
    open_db, open_network_db,
    tables::{
        CertificateDigestByOrigin, CertificateDigestByRound, Certificates,
        ConsensusBlockNumbersByDigest, ConsensusBlocks, LastProposed, Payload, Votes,
    },
    DatabaseType,
};
use tn_types::{
    gas_accumulator::GasAccumulator, AuthorityIdentifier, BatchValidation, BlsPublicKey, Committee,
    CommitteeBuilder, ConsensusHeader, ConsensusOutput, Database as TNDatabase, Epoch, Multiaddr,
    Noticer, Notifier, TaskManager, TaskSpawner, TimestampSec, WorkerCache, WorkerIndex,
    WorkerInfo, B256, MIN_PROTOCOL_BASE_FEE,
};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle};
use tokio::sync::{
    broadcast,
    mpsc::{self},
};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

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
    /// Address of the worker network once it is set.
    worker_network_addr: Option<Multiaddr>,
    /// Key config - loaded once for application lifetime.
    key_config: KeyConfig,
    /// The epoch manager's [Notifier] to shutdown all node processes.
    node_shutdown: Notifier,
    /// Output channel for consensus blocks to be executed.
    ///
    /// This should only be accessed elsewhere through the epoch's [ConsensusBus].
    consensus_output: broadcast::Sender<ConsensusOutput>,
    /// The timestamp to close the current epoch.
    ///
    /// The manager monitors leader timestamps for the epoch boundary.
    /// If the timestamp of the leader is >= the epoch_boundary then the
    /// manager closes the epoch after the engine executes all data.
    epoch_boundary: TimestampSec,
}

/// When rejoining a network mid epoch this will accumulate any gas state for previous epoch blocks.
fn catchup_accumulator<DB: TNDatabase>(
    db: &DB,
    reth_env: RethEnv,
    gas_accumulator: &GasAccumulator,
) -> eyre::Result<()> {
    if let Some(block) = reth_env.finalized_header()? {
        let epoch_state = reth_env.epoch_state_from_canonical_tip()?;

        // Note WORKER: In a single worker world this should be suffecient to set the base fee.
        // In a multi-worker world (furture) this will NOT work and needs updating.
        gas_accumulator
            .base_fee(0)
            .set_base_fee(block.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE));

        let blocks = reth_env.blocks_for_range(epoch_state.epoch_start..=block.number)?;
        let mut consensus_leaders: HashMap<B256, AuthorityIdentifier> = HashMap::default();
        for current in blocks {
            let gas = current.gas_used;
            let limit = current.gas_limit;
            let lower64 = current.difficulty.into_limbs()[0];
            let worker_id = (lower64 & 0xffff) as u16;
            gas_accumulator.inc_block(worker_id, gas, limit);
            // increment or leader counts.
            if let Some(hash) = current.parent_beacon_block_root {
                if let Some(leader) = consensus_leaders.get(&hash) {
                    gas_accumulator.rewards_counter().inc_leader_count(leader);
                } else if let Some(number) = db.get::<ConsensusBlockNumbersByDigest>(&hash)? {
                    if let Some(consensus_header) = db.get::<ConsensusBlocks>(&number)? {
                        let leader = consensus_header.sub_dag.leader.origin();
                        gas_accumulator.rewards_counter().inc_leader_count(leader);
                        // Cache the leader.
                        consensus_leaders.insert(hash, leader.clone());
                    }
                }
            }
        }
    };
    Ok(())
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
        let passphrase =
            if std::fs::exists(tn_datadir.node_keys_path().join(tn_config::BLS_WRAPPED_KEYFILE))
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
        let (consensus_output, _rx_consensus_output) = broadcast::channel(100);

        Ok(Self {
            builder,
            tn_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            worker_network_addr: None,
            key_config,
            node_shutdown,
            consensus_output,
            epoch_boundary: Default::default(),
        })
    }

    /// Run the node, handling epoch transitions.
    pub async fn run(&mut self) -> eyre::Result<()> {
        // create dbs to survive between sync state transitions
        let reth_db =
            RethEnv::new_database(&self.builder.node_config, self.tn_datadir.reth_db_path())?;

        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();

        info!(target: "epoch-manager", "starting node and launching first epoch");

        // create submanager for engine tasks
        let engine_task_manager = TaskManager::new(ENGINE_TASK_MANAGER);

        // create channels for engine that survive the lifetime of the node
        let (to_engine, for_engine) = mpsc::channel(1000);

        // Create our epoch gas accumulator, we currently have one worker.
        // All nodes have to agree on the worker count, do not change this for an existing chain.
        let gas_accumulator = GasAccumulator::new(1);
        // create the engine
        let engine = self.create_engine(&engine_task_manager, reth_db, &gas_accumulator)?;
        engine
            .start_engine(for_engine, self.node_shutdown.subscribe(), gas_accumulator.clone())
            .await?;

        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        let consensus_db = self.open_consensus_db().await?;
        catchup_accumulator(&consensus_db, engine.get_reth_env().await, &gas_accumulator)?;

        // read the network config or use the default
        let network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        self.spawn_node_networks(node_task_spawner, &network_config).await?;

        // start consensus metrics for the epoch
        let metrics_shutdown = Notifier::new();
        if let Some(metrics_socket) = self.builder.metrics {
            start_prometheus_server(
                metrics_socket,
                &node_task_manager,
                metrics_shutdown.subscribe(),
            );
        }

        // add engine task manager
        node_task_manager.add_task_manager(engine_task_manager);
        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // await all tasks on epoch-task-manager or node shutdown
        let result = tokio::select! {
            // run long-living node tasks
            res = node_task_manager.join_until_exit(self.node_shutdown.clone()) => {
                match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(eyre!("Node task shutdown: {e}")),
                }
            }

            // loop through short-term epochs
            epoch_result = self.run_epochs(&engine, consensus_db, network_config, to_engine, gas_accumulator) => epoch_result
        };

        // shutdown metrics
        metrics_shutdown.notify();

        result
    }

    /// Create the epoch directory for consensus data if it doesn't exist and open the consensus
    /// database connection.
    async fn open_consensus_db(&self) -> eyre::Result<DatabaseType> {
        let consensus_db_path = self.tn_datadir.consensus_db_path();

        // ensure dir exists
        let _ = std::fs::create_dir_all(&consensus_db_path);
        let db = open_db(&consensus_db_path);

        info!(target: "epoch-manager", ?consensus_db_path, "opened consensus storage");

        Ok(db)
    }

    /// Startup for the node. This creates all components on startup before starting the first
    /// epoch.
    ///
    /// This will create the long-running primary/worker [ConsensusNetwork]s for p2p swarm.
    async fn spawn_node_networks(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
    ) -> eyre::Result<()> {
        //
        //=== PRIMARY
        //
        // this is a temporary event stream - replaced at the start of every epoch
        let (tmp_event_stream, _temp_rx) = mpsc::channel(1000);

        // create network db
        let primary_network_db = self.tn_datadir.network_db_path().join("primary");
        let _ = std::fs::create_dir_all(&primary_network_db);
        info!(target: "epoch-manager", ?primary_network_db, "opening primary network storage at:");
        let primary_network_db = open_network_db(primary_network_db);

        // create long-running network task for primary
        let primary_network = ConsensusNetwork::new_for_primary(
            network_config,
            tmp_event_stream,
            self.key_config.clone(),
            primary_network_db,
            node_task_spawner.clone(),
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
                    res
                },
            )
        });

        // primary network handle
        self.primary_network_handle = Some(PrimaryNetworkHandle::new(primary_network_handle));

        //
        //=== WORKER
        //
        // this is a temporary event stream - replaced at the start of every epoch
        let (tmp_event_stream, _temp_rx) = mpsc::channel(1000);

        // create network db
        let worker_network_db = self.tn_datadir.network_db_path().join("worker");
        let _ = std::fs::create_dir_all(&worker_network_db);
        info!(target: "epoch-manager", ?worker_network_db, "opening worker network storage at:");
        let worker_network_db = open_network_db(worker_network_db);

        // create long-running network task for worker
        let worker_network = ConsensusNetwork::new_for_worker(
            network_config,
            tmp_event_stream,
            self.key_config.clone(),
            worker_network_db,
            node_task_spawner.clone(),
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
                    res
                }
            )
        });

        // set temporary task spawner - this is updated with each epoch
        self.worker_network_handle =
            Some(WorkerNetworkHandle::new(worker_network_handle, node_task_spawner.clone()));

        Ok(())
    }

    /// Execute a loop to start new epochs until shutdown.
    async fn run_epochs(
        &mut self,
        engine: &ExecutionNode,
        consensus_db: DatabaseType,
        network_config: NetworkConfig,
        to_engine: mpsc::Sender<ConsensusOutput>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        // initialize long-running components for node startup
        let mut initial_epoch = true;

        // loop through epochs
        loop {
            let epoch_result = self
                .run_epoch(
                    engine,
                    consensus_db.clone(),
                    &network_config,
                    &to_engine,
                    &mut initial_epoch,
                    gas_accumulator.clone(),
                )
                .await;

            // ensure no errors
            epoch_result.inspect_err(|e| {
                error!(target: "epoch-manager", ?e, "epoch returned error");
            })?;

            info!(target: "epoch-manager", "looping run epoch");
        }
    }

    /// Run a single epoch.
    ///
    /// If it returns Ok(true) this indicates a mode change occurred and a restart
    /// is required.
    async fn run_epoch(
        &mut self,
        engine: &ExecutionNode,
        mut consensus_db: DatabaseType,
        network_config: &NetworkConfig,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        initial_epoch: &mut bool,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        info!(target: "epoch-manager", "Starting epoch");

        // The task manager that resets every epoch and manages
        // short-running tasks for the lifetime of the epoch.
        let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);

        // subscribe to output early to prevent missed messages
        let consensus_output = self.consensus_output.subscribe();

        // create primary and worker nodes
        let (primary, worker_node) = self
            .create_consensus(
                engine,
                consensus_db.clone(),
                &epoch_task_manager,
                network_config,
                initial_epoch,
                gas_accumulator.clone(),
            )
            .await?;

        gas_accumulator.rewards_counter().set_committee(primary.current_committee().await);
        // start primary
        let mut primary_task_manager = primary.start().await?;

        // start worker
        let (mut worker_task_manager, worker) = worker_node.start().await?;

        // consensus config for shutdown subscribers
        let consensus_shutdown = primary.shutdown_signal().await;

        let batch_builder_task_spawner = epoch_task_manager.get_spawner();
        engine
            .start_batch_builder(
                worker.id(),
                worker.batches_tx(),
                &batch_builder_task_spawner,
                gas_accumulator.base_fee(worker.id()),
            )
            .await?;

        // update tasks
        primary_task_manager.update_tasks();
        worker_task_manager.update_tasks();

        // add epoch-specific tasks to manager
        epoch_task_manager.add_task_manager(primary_task_manager);
        epoch_task_manager.add_task_manager(worker_task_manager);

        info!(target: "epoch-manager", tasks=?epoch_task_manager, "EPOCH TASKS\n");

        // await the epoch boundary or the epoch task manager exiting
        // this can also happen due to committee nodes re-syncing and errors
        let consensus_shutdown_clone = consensus_shutdown.clone();

        // indicate if the node is restarting to join the committe or if the epoch is changed and
        // tables should be cleared
        let mut clear_tables_for_next_epoch = false;

        tokio::select! {
            // wait for epoch boundary to transition
            res = self.wait_for_epoch_boundary(to_engine, engine, consensus_output, consensus_shutdown.clone(), gas_accumulator) => {
                res.inspect_err(|e| {
                    error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                })?;

                info!(target: "epoch-manager", "epoch boundary success - clearing consensus db tables for next epoch");

                // toggle bool to clear tables
                clear_tables_for_next_epoch = true;
            },

            // return any errors
            res = epoch_task_manager.join_until_exit(consensus_shutdown_clone) => {
                res.inspect_err(|e| {
                    error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                })?;
                info!(target: "epoch-manager", "epoch task manager exited - likely syncing with committee");
            },
        }

        consensus_shutdown.notify();
        // abort all epoch-related tasks
        epoch_task_manager.abort_all_tasks();
        // Expect complaints from join so swallow those errors...
        // If we timeout here something is not playing nice and shutting down so return the timeout.
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            epoch_task_manager.join(consensus_shutdown),
        )
        .await?;

        // clear tables
        if clear_tables_for_next_epoch {
            self.clear_consensus_db_for_next_epoch(&mut consensus_db)?;
        }

        Ok(())
    }

    /// Monitor consensus output for the last block of the epoch.
    ///
    /// This method forwards all consensus output to the engine for execution.
    /// Once the epoch boundary is reached, the manager initiates the epoch transitions.
    async fn wait_for_epoch_boundary(
        &self,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        engine: &ExecutionNode,
        mut consensus_output: broadcast::Receiver<ConsensusOutput>,
        shutdown_consensus: Notifier,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        // receive output from consensus and forward to engine
        'epoch: while let Ok(mut output) = consensus_output.recv().await {
            // observe epoch boundary to initiate epoch transition
            if output.committed_at() >= self.epoch_boundary {
                info!(
                    target: "epoch-manager",
                    epoch=?output.leader().epoch(),
                    commit=?output.committed_at(),
                    epoch_boundary=?self.epoch_boundary,
                    "epoch boundary detected",
                );
                // subscribe to engine blocks to confirm epoch closed on-chain
                let mut executed_output = engine.canonical_block_stream().await;

                // update output so engine closes epoch
                output.close_epoch = true;

                // obtain hash to monitor execution progress
                let target_hash = output.consensus_header_hash();

                gas_accumulator.rewards_counter().inc_leader_count(output.leader().origin());
                // forward the output to the engine
                to_engine.send(output).await?;

                // begin consensus shutdown while engine executes
                shutdown_consensus.notify();

                // wait for execution result before proceeding
                while let Some(output) = executed_output.next().await {
                    // ensure canonical tip is updated with closing epoch info
                    if output.tip().sealed_header().parent_beacon_block_root == Some(target_hash) {
                        // return
                        break 'epoch;
                    }
                }

                // `None` indicates all senders have dropped
                error!(
                    target: "epoch-manager",
                    "canon state notifications dropped while awaiting engine execution for closing epoch",
                );
                return Err(eyre!("engine failed to report output for closing epoch"));
            } else {
                gas_accumulator.rewards_counter().inc_leader_count(output.leader().origin());
                // only forward the output to the engine
                to_engine.send(output).await?;
            }
        }
        // Use accumulated gas information to set each workers base fee for the epoch.
        for worker_id in 0..gas_accumulator.num_workers() {
            let worker_id = worker_id as u16;
            let (_blocks, _gas_used, _gas_limit) = gas_accumulator.get_values(worker_id);
            // Change this base fee to update base fee in batches workers create.
            let _base_fee = gas_accumulator.base_fee(worker_id);
        }
        gas_accumulator.clear(); // Clear the accumlated values for next epoch.

        Ok(())
    }

    /// Helper method to create all engine components.
    fn create_engine(
        &self,
        engine_task_manager: &TaskManager,
        reth_db: RethDb,
        gas_accumulator: &GasAccumulator,
    ) -> eyre::Result<ExecutionNode> {
        // create execution components (ie - reth env)
        let basefee_address = self.builder.tn_config.parameters.basefee_address;
        let reth_env = RethEnv::new(
            &self.builder.node_config,
            engine_task_manager,
            reth_db,
            basefee_address,
            gas_accumulator.rewards_counter(),
        )?;
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
        initial_epoch: &mut bool,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<(PrimaryNode<DatabaseType>, WorkerNode<DatabaseType>)> {
        // create config for consensus
        let consensus_config =
            self.configure_consensus(engine, network_config, &consensus_db).await?;

        let primary = self
            .create_primary_node_components(
                &consensus_config,
                epoch_task_manager.get_spawner(),
                initial_epoch,
            )
            .await?;

        let engine_to_primary =
            EngineToPrimaryRpc::new(primary.consensus_bus().await, consensus_db.clone());
        // only spawns one worker for now
        let worker = self
            .spawn_worker_node_components(
                &consensus_config,
                engine,
                epoch_task_manager.get_spawner(),
                initial_epoch,
                engine_to_primary,
                gas_accumulator,
            )
            .await?;

        // ensure initialized networks is false after the first run
        *initial_epoch = false;

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

    /// Configure consensus for the current epoch.
    ///
    /// This method reads the canonical tip to read the epoch information needed
    /// to create the current committee and the consensus config.
    async fn configure_consensus(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
        consensus_db: &DatabaseType,
    ) -> eyre::Result<ConsensusConfig<DatabaseType>> {
        // retrieve epoch information from canonical tip
        let EpochState { epoch, epoch_info, validators, epoch_start } =
            engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch_info, "epoch state from canonical tip for epoch {}", epoch);
        let validators = validators
            .iter()
            .map(|v| {
                let decoded_bls = BlsPublicKey::from_literal_bytes(v.blsPubkey.as_ref());
                decoded_bls.map(|decoded| (decoded, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|err| eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;

        self.epoch_boundary = epoch_start + epoch_info.epochDuration as u64;
        debug!(target: "epoch-manager", new_epoch_boundary=self.epoch_boundary, "resetting epoch boundary");

        // send these to the swarm for validator discovery
        let keys_for_worker_cache = validators.keys().cloned().collect();
        debug!(target: "epoch-manager", ?validators, "creating committee for validators");
        let committee = self.create_committee_from_state(epoch, validators).await?;
        let worker_cache =
            self.create_worker_cache_from_state(epoch, keys_for_worker_cache).await?;

        // create config for consensus
        let consensus_config = ConsensusConfig::new_for_epoch(
            self.builder.tn_config.clone(),
            consensus_db.clone(),
            self.key_config.clone(),
            committee,
            worker_cache,
            network_config.clone(),
        )?;

        Ok(consensus_config)
    }

    /// Create the [Committee] for the current epoch.
    ///
    /// This is the first step for configuring consensus.
    async fn create_committee_from_state(
        &self,
        epoch: Epoch,
        validators: HashMap<BlsPublicKey, &ConsensusRegistry::ValidatorInfo>,
    ) -> eyre::Result<Committee> {
        info!(target: "epoch-manager", "creating committee from state");

        // the network must be live
        let committee = if epoch == 0 {
            // read from fs for genesis
            Config::load_from_path_or_default::<Committee>(
                self.tn_datadir.committee_path(),
                ConfigFmt::YAML,
            )?
        } else {
            // retrieve network information for committee
            let primary_handle = self
                .primary_network_handle
                .as_ref()
                .ok_or_eyre("missing primary network handle for epoch manager")?;

            let mut primary_network_infos = primary_handle
                .inner_handle()
                .find_authorities(validators.keys().cloned().collect())
                .await?;

            debug!(target: "epoch-manager", "requsting info validator info for {} authorities", primary_network_infos.len());

            // build the committee using kad network
            let mut committee_builder = CommitteeBuilder::new(epoch);

            // loop through the primary info returned from network query
            while let Some(info) = primary_network_infos.next().await {
                debug!(target: "epoch-manager", ?info, "awaited next primary network info");
                let (protocol_key, NetworkInfo { pubkey, multiaddr, hostname }) = info??;
                debug!(target: "epoch-manager", peer_id=?pubkey.to_peer_id(), "awaited next primary network info");
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

            committee_builder.build()
        };

        // load committee
        committee.load();

        Ok(committee)
    }

    /// Create the [WorkerCache] for the current epoch.
    ///
    /// This is the first step for configuring consensus.
    async fn create_worker_cache_from_state(
        &self,
        epoch: Epoch,
        validators: Vec<BlsPublicKey>,
    ) -> eyre::Result<WorkerCache> {
        info!(target: "epoch-manager", "creating worker cache from state");

        let worker_cache = if epoch == 0 {
            debug!(target: "epoch-manager", "loading worker cache from config for epoch 0");
            Config::load_from_path_or_default::<WorkerCache>(
                self.tn_datadir.worker_cache_path(),
                ConfigFmt::YAML,
            )?
        } else {
            debug!(target: "epoch-manager", "creating worker cache from network records");
            // create worker cache
            let worker_handle = self
                .worker_network_handle
                .as_ref()
                .ok_or_eyre("missing primary network handle for epoch manager")?;

            let mut workers = Vec::with_capacity(validators.len());
            let mut worker_network_infos =
                worker_handle.inner_handle().find_authorities(validators).await?;

            // loop through the worker info returned from network query
            while let Some(info) = worker_network_infos.next().await {
                let (protocol_key, NetworkInfo { pubkey, multiaddr, .. }) = info??;
                // only one worker per authority for now
                let worker_index =
                    WorkerIndex(vec![WorkerInfo { name: pubkey, worker_address: multiaddr }]);
                workers.push((protocol_key, worker_index));
            }

            WorkerCache { epoch, workers: Arc::new(workers.into_iter().collect()) }
        };

        Ok(worker_cache)
    }

    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
    async fn create_primary_node_components<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: &bool,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let consensus_bus = ConsensusBus::new_with_args(
            consensus_config.config().parameters.gc_depth,
            self.consensus_output.clone(),
        );
        let state_sync = StateSynchronizer::new(consensus_config.clone(), consensus_bus.clone());
        let network_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("primary network handle missing from epoch manager")?
            .clone();

        // create the epoch-specific `PrimaryNetwork`
        self.spawn_primary_network_for_epoch(
            consensus_config,
            &consensus_bus,
            state_sync.clone(),
            epoch_task_spawner.clone(),
            &network_handle,
            initial_epoch,
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
        engine: &ExecutionNode,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: &bool,
        engine_to_primary: EngineToPrimaryRpc<DB>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<WorkerNode<DB>> {
        // only support one worker for now (with id 0) - otherwise, loop here
        let worker_id = 0;
        let base_fee = gas_accumulator.base_fee(worker_id);

        // update the network handle's task spawner for reporting batches in the epoch
        {
            let network_handle = self
                .worker_network_handle
                .as_mut()
                .ok_or_eyre("worker network handle missing from epoch manager")?;

            network_handle.update_task_spawner(epoch_task_spawner.clone());
            // initialize worker components on startup
            // This will use the new epoch_task_spawner on network_handle.
            if *initial_epoch {
                engine
                    .initialize_worker_components(
                        worker_id,
                        network_handle.clone(),
                        engine_to_primary,
                    )
                    .await?;
            } else {
                // We updated our epoch task spawner so make sure worker network tasks are
                // restarted.
                engine.respawn_worker_network_tasks(network_handle.clone()).await;
            }
        }

        let network_handle = self
            .worker_network_handle
            .as_ref()
            .ok_or_eyre("worker network handle missing from epoch manager")?
            .clone();

        let validator = engine.new_batch_validator(&worker_id, base_fee).await;
        self.spawn_worker_network_for_epoch(
            consensus_config,
            &worker_id,
            validator.clone(),
            epoch_task_spawner,
            &network_handle,
            initial_epoch,
        )
        .await?;

        let worker =
            WorkerNode::new(worker_id, consensus_config.clone(), network_handle.clone(), validator);

        Ok(worker)
    }

    /// Create the primary network for the specific epoch.
    ///
    /// This is not the swarm level, but the [PrimaryNetwork] interface.
    async fn spawn_primary_network_for_epoch<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &PrimaryNetworkHandle,
        initial_epoch: &bool,
    ) -> eyre::Result<()> {
        // create event streams for the primary network handler
        let (event_stream, rx_event_stream) = mpsc::channel(1000);

        // set committee for network to prevent banning
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "spawning primary network for epoch");
        network_handle
            .inner_handle()
            .new_epoch(consensus_config.primary_network_map(), event_stream)
            .await?;
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "event stream updated!");

        // start listening if the network needs to be initialized
        if *initial_epoch {
            // start listening for p2p messages
            let primary_address = consensus_config.primary_address();
            network_handle.inner_handle().start_listening(primary_address).await?;
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

            debug!(target: "epoch-manager", ?peer_id, "dialing peer");

            // skip dialing already connected peers
            if let Ok(peers) = handle.connected_peers().await {
                if peers.contains(&peer_id) {
                    debug!(target: "epoch-manager", ?peer_id, "skipping dial for peer");
                    return;
                };
            }

            debug!(target: "epoch-manager", ?peer_id, "peer not connected - dialing peer");
            while let Err(e) = handle.dial(peer_id, peer_addr.clone()).await {
                // ignore errors for peers that are already connected or being dialed
                if matches!(e, NetworkError::AlreadyConnected(_)) || matches!(e, NetworkError::AlreadyDialing(_)) {
                    return;
                }

                tracing::warn!(target: "epoch-manager", "failed to dial {peer_id} at {peer_addr}: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 120 {
                    backoff += backoff;
                }
            }
        });
    }

    /// Create the worker network.
    async fn spawn_worker_network_for_epoch<DB: TNDatabase>(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &WorkerNetworkHandle,
        initial_epoch: &bool,
    ) -> eyre::Result<()> {
        // create event streams for the worker network handler
        let (event_stream, rx_event_stream) = mpsc::channel(1000);
        debug!(target: "epoch-manager", "spawning worker network for epoch");

        network_handle
            .inner_handle()
            .new_epoch(consensus_config.worker_network_map(), event_stream)
            .await?;

        // start listening if the network needs to be initialized
        if *initial_epoch {
            let worker_address = consensus_config.worker_address(worker_id);
            self.worker_network_addr = Some(worker_address.clone());
            network_handle.inner_handle().start_listening(worker_address).await?;
        }

        let worker_address =
            self.worker_network_addr.clone().expect("worker address set at this point");

        // always attempt to dial peers for the new epoch
        // the network's peer manager will intercept dial attempts for peers that are already
        // connected
        debug!(target: "epoch-manager", ?worker_address, "spawning worker network for epoch");
        for (peer_id, addr) in consensus_config.worker_cache().all_workers() {
            if worker_address != addr {
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

        // prime the watch channel with data from the db this will be updated by state-sync if this
        // node can_cvv
        consensus_bus.last_consensus_header().send(last_db_block)?;

        Ok(())
    }

    /// Helper method to identify the node's mode:
    /// - "Committee-voting Validator" (CVV)
    /// - "Committee-voting Validator Inactive" (CVVInactive - syncing to rejoin)
    /// - "Observer"
    ///
    /// This method also updates the `ConsensusBus::node_mode()`.
    async fn identify_node_mode<DB: TNDatabase>(
        &self,
        consensus_bus: &ConsensusBus,
        consensus_config: &ConsensusConfig<DB>,
        primary_network_handle: &PrimaryNetworkHandle,
    ) -> eyre::Result<NodeMode> {
        debug!(target: "epoch-manager", authority_id=?consensus_config.authority_id(), "identifying node mode..." );
        let in_committee = consensus_config
            .authority_id()
            .map(|id| consensus_config.in_committee(&id))
            .unwrap_or(false);
        let mode = if !in_committee || self.builder.tn_config.observer {
            NodeMode::Observer
        } else if state_sync::can_cvv(consensus_bus, consensus_config, primary_network_handle).await
        {
            NodeMode::CvvActive
        } else {
            NodeMode::CvvInactive
        };

        debug!(target: "epoch-manager", ?mode, "node mode identified");
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
        mut engine_state: CanonStateNotificationStream,
        epoch_task_manager: &TaskManager,
    ) {
        // spawn epoch-specific task to forward blocks from the engine to consensus
        epoch_task_manager.spawn_critical_task("latest execution block", async move {
            loop {
                tokio::select!(
                    _ = &shutdown => {
                        info!(target: "engine", "received shutdown from consensus to stop updating consensus bus recent blocks");
                        break;
                    }
                    latest = engine_state.next() => {
                        if let Some(latest) = latest {
                            consensus_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(latest.tip().clone_sealed_header()));
                        } else {
                            break;
                        }
                    }
                )
            }
        });
    }

    /// Clear the epoch-related tables for consensus.
    ///
    /// These tables are epoch-specific. Complete historic data is stored
    /// in the `ConsensusBlocks` table.
    fn clear_consensus_db_for_next_epoch(
        &self,
        consensus_db: &mut DatabaseType,
    ) -> eyre::Result<()> {
        consensus_db.clear_table::<LastProposed>()?;
        consensus_db.clear_table::<Votes>()?;
        consensus_db.clear_table::<Certificates>()?;
        consensus_db.clear_table::<CertificateDigestByRound>()?;
        consensus_db.clear_table::<CertificateDigestByOrigin>()?;
        consensus_db.clear_table::<Payload>()?;
        Ok(())
    }
}
