//! The epoch manager type.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.

use crate::{
    engine::{ExecutionNode, TnBuilder},
    health::HealthcheckServer,
    primary::PrimaryNode,
    worker::{worker_task_manager_name, WorkerNode},
    EngineToPrimaryRpc,
};
use consensus_metrics::start_prometheus_server;
use eyre::{eyre, OptionExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tn_config::{
    Config, ConfigFmt, ConfigTrait as _, ConsensusConfig, KeyConfig, NetworkConfig, TelcoinDirs,
};
use tn_network_libp2p::{
    error::NetworkError,
    types::{NetworkEvent, NetworkHandle},
    ConsensusNetwork, TNMessage,
};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, QueChannel, StateSynchronizer,
};
use tn_reth::{
    bytes_to_txn,
    system_calls::{ConsensusRegistry, EpochState},
    CanonStateNotificationStream, RethDb, RethEnv,
};
use tn_storage::{
    open_db,
    tables::{
        Batches, CertificateDigestByOrigin, CertificateDigestByRound, Certificates,
        ConsensusBlocks, EpochCerts, EpochRecords, LastProposed, NodeBatchesCache, Payload, Votes,
    },
    ConsensusStore, DatabaseType, EpochStore as _,
};
use tn_types::{
    error::HeaderError, gas_accumulator::GasAccumulator, Batch, BatchValidation, BlockHash,
    BlsAggregateSignature, BlsPublicKey, BlsSignature, CertifiedBatch, CommittedSubDag, Committee,
    CommitteeBuilder, ConsensusOutput, Database as TNDatabase, Epoch, EpochCertificate,
    EpochRecord, EpochVote, Hash, Multiaddr, NetworkPublicKey, Notifier, TaskJoinError,
    TaskManager, TaskSpawner, TimestampSec, TnReceiver, TnSender, B256, MIN_PROTOCOL_BASE_FEE,
};
use tn_worker::{
    quorum_waiter::QuorumWaiterTrait, Worker, WorkerNetwork, WorkerNetworkHandle, WorkerRequest,
    WorkerResponse,
};
use tokio::sync::mpsc::{self};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, info_span, warn, Instrument};

/// The long-running task manager name.
const NODE_TASK_MANAGER: &str = "Node Task Manager";

/// The epoch-specific task manager name.
const EPOCH_TASK_MANAGER: &str = "Epoch Task Manager";

/// The execution engine task manager name.
const ENGINE_TASK_MANAGER: &str = "Engine Task Manager";

/// The worker's base task manager name. This is used by `fn worker_task_manager_name(id)`.
pub(super) const WORKER_TASK_BASE: &str = "Worker Task";

/// Modes for an epoch.
#[derive(Debug, Copy, Clone)]
enum RunEpochMode {
    /// This is the initial epoch when the system starts, will need to get established.
    Initial,
    /// We re-ran an epoch as a result of a node change (CVV is to far behind or CVV has caught up
    /// for instance).
    ModeChange,
    /// This a fresh new epoch on an already running node.
    NewEpoch,
}

impl RunEpochMode {
    fn replay_consensus(&self) -> bool {
        match self {
            RunEpochMode::ModeChange => false,
            RunEpochMode::Initial | RunEpochMode::NewEpoch => true,
        }
    }

    fn initial_epoch(&self) -> bool {
        matches!(self, RunEpochMode::Initial)
    }
}

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
    consensus_bus: ConsensusBus,
    /// Persistent event stream for worker network events.
    worker_event_stream: QueChannel<NetworkEvent<WorkerRequest, WorkerResponse>>,

    /// The record for a just completed epoch.
    epoch_record: Option<EpochRecord>,
}

/// When rejoining a network mid epoch this will accumulate any gas state for previous epoch blocks.
pub fn catchup_accumulator<DB: TNDatabase>(
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

        let blocks =
            reth_env.blocks_for_range(epoch_state.epoch_info.blockHeight..=block.number)?;
        let mut last_round: Option<u32> = None;

        // loop through blocks to increment leader counts
        for current in blocks {
            let gas = current.gas_used;
            let limit = current.gas_limit;

            // difficulty contains the worker id and batch index:
            // `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`
            let lower64 = current.difficulty.into_limbs()[0];
            let worker_id = (lower64 & 0xffff) as u16;
            gas_accumulator.inc_block(worker_id, gas, limit);

            // extract epoch and round from nonce
            // - epoch: first 32 bits
            // - round: lower 32 bits
            let nonce: u64 = current.nonce.into();
            let (epoch, round) = RethEnv::deconstruct_nonce(nonce);
            // skip genesis
            if round == 0 {
                continue;
            }

            debug!(target: "epoch-manager", ?epoch, ?round, block=current.number, "catchup from nonce:");

            // only increment leader count for new rounds
            if last_round != Some(round) {
                // this is a new round, increment the leader count
                let consensus_digest =
                    current.parent_beacon_block_root.ok_or_eyre("consensus root missing")?;
                let leader = db
                    .get_consensus_by_hash(consensus_digest)
                    .ok_or_eyre("missing consensus block")?
                    .sub_dag
                    .leader
                    .origin()
                    .clone();

                gas_accumulator.rewards_counter().inc_leader_count(&leader);
            }
            last_round = Some(round);
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
    pub(crate) fn new(
        builder: TnBuilder,
        tn_datadir: P,
        consensus_db: DB,
        key_config: KeyConfig,
    ) -> Self {
        // shutdown long-running node components
        let node_shutdown = Notifier::new();

        let reth_db = builder.reth_db.clone();

        let consensus_bus = ConsensusBus::new_with_args(builder.tn_config.parameters.gc_depth);
        if builder.tn_config.observer {
            // Don't risk keeping the default CVV active mode...
            let _ = consensus_bus.node_mode().send(NodeMode::Observer);
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
            epoch_record: None,
        }
    }

    /// Run the node, handling epoch transitions.
    pub(crate) async fn run(&mut self) -> eyre::Result<()> {
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
        let engine = self.create_engine(&engine_task_manager, &gas_accumulator)?;
        engine
            .start_engine(for_engine, self.node_shutdown.subscribe(), gas_accumulator.clone())
            .await?;

        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        catchup_accumulator(&self.consensus_db, engine.get_reth_env().await, &gas_accumulator)?;

        // read the network config or use the default
        let network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        self.spawn_node_networks(node_task_spawner, &network_config).await?;
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
            self.consensus_db.clone(),
            primary_network_handle,
            self.consensus_bus.clone(),
            node_task_manager.get_spawner(),
            self.node_shutdown.subscribe(),
        )
        .await?;

        // start consensus metrics for the epoch
        let metrics_shutdown = Notifier::new();
        if let Some(metrics_socket) = self.builder.metrics {
            start_prometheus_server(
                metrics_socket,
                &node_task_manager,
                metrics_shutdown.subscribe(),
            );
        }

        self.try_restore_state(&engine).await?;
        // spawn task to update the latest execution results for consensus
        self.spawn_engine_update_task(engine.canonical_block_stream().await, &node_task_manager);

        // add engine task manager
        node_task_manager.add_task_manager(engine_task_manager);
        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // spawn node healthcheck service if enabled
        if let Some(port) = self.builder.healthcheck {
            let _ = HealthcheckServer::spawn(node_task_manager.get_spawner(), port).await;
        }

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
            epoch_result = self.run_epochs(&engine, network_config, to_engine, gas_accumulator) => epoch_result
        };

        // shutdown metrics
        metrics_shutdown.notify();

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
                    res
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
                    Ok(())
                }
                res = worker_network.run() => {
                    warn!(target: "epoch-manager", ?res, "worker network stopped");
                    res
                }
            )
        });

        // set temporary task spawner - this is updated with each epoch
        self.worker_network_handle = Some(WorkerNetworkHandle::new(
            worker_network_handle,
            node_task_spawner.clone(),
            network_config.libp2p_config().max_rpc_message_size,
        ));

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
            // Make sure we don't start a new epoch when we are shutting down.
            if node_ended_sub.noticed() {
                break Ok(());
            }
            info!(target: "epoch-manager", "looping run epoch");
        }
    }

    /// Collect any batches that never got into consensus (at epoch change or node restart) and
    /// Re-introduce them into the mempool for inclusion in future batches.
    fn orphan_batches<QuorumWaiter: QuorumWaiterTrait>(
        &self,
        epoch_task_manager: &TaskManager,
        engine: ExecutionNode,
        worker: Worker<DB, QuorumWaiter>,
        epoch: Epoch,
    ) -> eyre::Result<()> {
        let mut orphan_batches: Vec<(BlockHash, Batch)> =
            self.consensus_db.iter::<NodeBatchesCache>().collect();
        if !orphan_batches.is_empty() {
            self.consensus_db.clear_table::<NodeBatchesCache>()?;
            let consensus_bus = self.consensus_bus.clone();
            let span =
                info_span!(target: "telcoin", "orphan-batches", epoch = tracing::field::Empty);
            span.record("epoch", epoch.to_string());
            epoch_task_manager.spawn_task("Orphaned Batches", async move {
                info!(target: "epoch-manager", "Re-introducing orphaned batchs {} transactions", orphan_batches.len());
                let pools = engine.get_all_worker_transaction_pools().await;
                let is_cvv = consensus_bus.is_active_cvv();
                for (digest, batch) in orphan_batches.drain(..) {
                    // Loop through any orphaned batches and resubmit it's transactions.
                    // This is most likely because of epoch changes but could be caused by a restart as
                    // well.
                    if is_cvv {
                        for tx_bytes in batch.transactions() {
                            // Put txn back into the mem pool.
                            if let Ok(tx) = bytes_to_txn(tx_bytes) {
                                if let Some(pool) = pools.get(batch.worker_id as usize) {
                                    let _ = pool.add_raw_transaction_external(tx).await;
                                }
                            }
                        }
                    } else {
                        // If we are not a CVV then go ahead and disburse the txns from the batch directly.
                        let _ = worker.disburse_txns(batch.seal(digest)).await;
                    }
                }
            }.instrument(span));
        } else {
            info!(target: "epoch-manager", "No batches leftover");
        }
        Ok(())
    }

    /// Turn a CommittedSubDag with consensus header info into ConsensusOutput.
    /// It will retrieve any missing Batches so the ConsensusOutput will be ready
    /// to execute.
    /// Note, an error here is BAD and will most likely cause node shutdown (clean).  Do
    /// not provide a bogus sub dag...
    async fn fetch_local_batches(
        &self,
        deliver: CommittedSubDag,
        parent_hash: B256,
        number: u64,
        committee: &Committee,
    ) -> eyre::Result<ConsensusOutput> {
        let num_blocks = deliver.num_primary_blocks();
        let num_certs = deliver.len();

        if num_blocks == 0 {
            debug!(target: "epoch-manager", "No blocks to fetch, payload is empty");
            return Ok(ConsensusOutput {
                sub_dag: Arc::new(deliver),
                parent_hash,
                number,
                ..Default::default()
            });
        }

        let sub_dag = Arc::new(deliver);
        let mut consensus_output = ConsensusOutput {
            sub_dag: sub_dag.clone(),
            batches: Vec::with_capacity(num_certs),
            parent_hash,
            number,
            ..Default::default()
        };

        let mut batch_set: HashSet<BlockHash> = HashSet::new();

        for cert in &sub_dag.certificates {
            for (digest, _) in cert.header().payload().iter() {
                batch_set.insert(*digest);
                consensus_output.batch_digests.push_back(*digest);
            }
        }

        // map all fetched batches to their respective certificates for applying block rewards
        for cert in &sub_dag.certificates {
            // create collection of batches to execute for this certificate
            let mut cert_batches = Vec::with_capacity(cert.header().payload().len());

            // retrieve fetched batch by digest
            for digest in cert.header().payload().keys() {
                if let Some(batch) = self.consensus_db.get::<Batches>(digest)? {
                    cert_batches.push(batch);
                } else {
                    return Err(eyre::eyre!("Failed to find required batch {digest}"));
                }
            }

            let address = committee.authority(cert.origin()).map(|a| a.execution_address());
            if let Some(address) = address {
                // main collection for execution
                consensus_output.batches.push(CertifiedBatch { address, batches: cert_batches });
            } else {
                return Err(eyre::eyre!("Unknown authority address {}", cert.origin()));
            }
        }
        debug!(target: "epoch-manager", "returning output to subscriber");
        Ok(consensus_output)
    }

    /// If we have any consensus that made it into the consensus chain but was not executed
    /// then make sure we submit it to the engine for execution now.
    /// Note, this has to be called correctly or it can lead to double execution.
    async fn replay_missed_consensus(
        &self,
        engine: &ExecutionNode,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        gas_accumulator: &GasAccumulator,
    ) -> eyre::Result<()> {
        // We have not created this epoch's primary yet (no committee) so get it from chain
        // ourselves... Note, any consensus output to replay should be in the same epoch...
        let EpochState { epoch, epoch_info: _, validators, epoch_start: _ } =
            engine.epoch_state_from_canonical_tip().await?;
        let validators = validators
            .iter()
            .map(|v| {
                let decoded_bls = BlsPublicKey::from_literal_bytes(v.blsPubkey.as_ref());
                decoded_bls.map(|decoded| (decoded, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|err| eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;

        let committee = self.create_committee_from_state(epoch, validators).await?;
        // Need to set the committee early or we will get failures to execute...
        gas_accumulator.rewards_counter().set_committee(committee.clone());

        // Make sure any old consensus that was not executed gets executed.
        // Note, "missing" in this context is consensus that was reached but not executed
        // before the last shutdown.  We need to execute it now so that everything will be
        // in sync, otherwise we could get out of order execution racing with Bullshark.
        let missing =
            state_sync::get_missing_consensus(&self.consensus_db, &self.consensus_bus).await?;
        for consensus_header in missing.into_iter() {
            let consensus_output = self
                .fetch_local_batches(
                    consensus_header.sub_dag.clone(),
                    consensus_header.parent_hash,
                    consensus_header.number,
                    &committee,
                )
                .await?;
            if let Err(e) = to_engine.send(consensus_output).await {
                error!(target: "epoch-manager", "error sending consensus output to engine: {}", e);
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Run a single epoch.
    async fn run_epoch(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        epoch_mode: RunEpochMode,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<RunEpochMode> {
        info!(target: "epoch-manager", "Starting epoch");

        // Lets make sure our consesus db has a clear write queue and is ready to go.
        self.consensus_db.persist::<Batches>().await;
        self.consensus_db.persist::<ConsensusBlocks>().await;
        if epoch_mode.replay_consensus() {
            // If we are starting up then make sure that any consensus we previously validated goes
            // to the engine and is executed.  Otherwise we could miss consensus execution.
            self.replay_missed_consensus(engine, to_engine, &gas_accumulator).await?;
        }
        // If we are restarting the epoch not on a boundary
        // and we sent some consensus output to the engine
        // then we need to pause for the engine to execute.
        // If we don't we can have races when the epoch restarts
        // that will send consensus to the engine more than once.
        if let Some(last_consensus) =
            self.consensus_db.last_record::<ConsensusBlocks>().map(|(_, block)| block.digest())
        {
            info!(target: "epoch-manager", "Waiting for execution of consensus {last_consensus}");
            self.consensus_bus.wait_for_consensus_execution(last_consensus).await?;
            info!(target: "epoch-manager", "Confirmed execution of consensus {last_consensus}");
        }

        let node_ended = self.node_shutdown.subscribe();

        // The task manager that resets every epoch and manages
        // short-running tasks for the lifetime of the epoch.
        let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);
        // Do not wait long for tasks to exit, just drop them and move on to next epoch.
        epoch_task_manager.set_join_wait(200);

        // subscribe to output early to prevent missed messages
        let mut consensus_output = self.consensus_bus.consensus_output().subscribe();

        // create primary and worker nodes
        let (primary, worker_node) = self
            .create_consensus(
                engine,
                &epoch_task_manager,
                network_config,
                epoch_mode.initial_epoch(),
                gas_accumulator.clone(),
            )
            .await?;
        // consensus config for shutdown subscribers
        let consensus_shutdown = primary.shutdown_signal().await;
        let epoch_shutdown_rx = consensus_shutdown.subscribe();

        // This needs to be created early so required machinery for other tasks exists when needed.
        let mut worker = worker_node.new_worker().await?;
        let current_epoch = primary.current_committee().await.epoch();

        // Produce a "dummy" epoch 0 EpochRecord if missing.
        // This will let us use simple code to find any epoch including 0 at startup.
        if self.consensus_db.get_committee_keys(0).is_none() {
            if current_epoch != 0 {
                return Err(eyre::eyre!(
                    "We have epoch 0 in our database if we are past epoch 0, on {current_epoch}"
                ));
            }
            // No keys for epoch 0, fix that.
            // We are on epoch 0 so load up that committee in Db as well.
            let committee: Vec<BlsPublicKey> =
                primary.current_committee().await.bls_keys().iter().copied().collect();
            let next_committee = committee.clone();
            let epoch_rec =
                EpochRecord { epoch: 0, committee, next_committee, ..Default::default() };
            // Save the "dummy" record, should be overwritten once epoch 0 closes.
            // This will NOT be signed.
            self.consensus_db.save_epoch_record(&epoch_rec);
        }

        gas_accumulator.rewards_counter().set_committee(primary.current_committee().await);
        // start primary
        primary.start(&epoch_task_manager).await?;

        let worker_task_manager_name = worker_task_manager_name(worker_node.id().await);
        // start batch builder
        worker.spawn_batch_builder(&worker_task_manager_name, &epoch_task_manager);

        let batch_builder_task_spawner = epoch_task_manager.get_spawner();
        engine
            .start_batch_builder(
                worker.id(),
                worker.batches_tx(),
                &batch_builder_task_spawner,
                gas_accumulator.base_fee(worker.id()),
                current_epoch,
            )
            .await?;

        self.orphan_batches(&epoch_task_manager, engine.clone(), worker.clone(), current_epoch)?;

        // update tasks
        epoch_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?epoch_task_manager, "EPOCH TASKS\n");

        // await the epoch boundary or the epoch task manager exiting
        // this can also happen due to committee nodes re-syncing and errors
        let consensus_shutdown_clone = consensus_shutdown.clone();

        // indicate if the node is restarting to join the committe or if the epoch is changed and
        // tables should be cleared
        let mut clear_tables_for_next_epoch = false;

        // New Epoch, should be able to collect the certs from the last epoch.
        if let Some(epoch_rec) = self.epoch_record.take() {
            let _ = self.collect_epoch_votes(&primary, epoch_rec, &epoch_task_manager).await;
        }

        let mut need_join = false;
        tokio::select! {
            _ = node_ended => {
                need_join = true;
            },
            // wait for epoch boundary to transition
            res = self.wait_for_epoch_boundary(to_engine, &mut consensus_output) => {
                // toggle bool to clear tables
                clear_tables_for_next_epoch = true;
                let target_hash = res.inspect_err(|e| {
                    error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                })?;
                self.close_epoch(consensus_shutdown.clone(), &gas_accumulator, target_hash)
                    .await?;

                // Write the epoch record to DB and save in manager for next epoch.
                self.write_epoch_record(&primary, engine).await?;

                info!(target: "epoch-manager", "epoch boundary success - clearing consensus db tables for next epoch");
                need_join = true;
            },

            // return any errors
            res = epoch_task_manager.join(consensus_shutdown_clone) => {
                match res {
                    Ok(()) => info!(target: "epoch-manager", "epoch task manager exited - likely syncing with committee"),
                    // There are times when the epoch task manager can exit with Ok...
                    Err(TaskJoinError::CriticalExitOk(task)) => {
                        // It is possible for the epoch to get a shutdown signal before the join.
                        // In that case it will not reconize the Ok task exit so we double check it here
                        // with a noticer that was aquired much earlier on epoch startup.
                        if epoch_shutdown_rx.noticed() {
                            info!(target: "epoch-manager", "epoch task manager exited - likely syncing with committee");
                        } else {
                            error!(target: "epoch-manager", ?task, "failed to reach epoch boundary");
                            return Err(TaskJoinError::CriticalExitOk(task).into());
                        }
                    }
                    Err(e) => {
                        error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                        return Err(e.into());
                    }
                }
            },
        }

        let mut res = RunEpochMode::NewEpoch;
        // If the select exitted because of a join() then do not join() again- we are already
        // shutting down.
        if need_join {
            consensus_shutdown.notify();
            // abort all epoch-related tasks
            epoch_task_manager.abort_all_tasks();
            // Expect complaints from join so swallow those errors...
            // If we timeout here something is not playing nice and shutting down so return the
            // timeout.
            let _ = tokio::time::timeout(
                Duration::from_millis(500),
                epoch_task_manager.join(consensus_shutdown),
            )
            .await?;
            // The epoch is over now and consensus should be shutdown.
            // Do a sanity check that no "extra" consensus was produced
            // past the epoch end and clean the DB if so otherwise we
            // could produce invalid blocks with it and fork later.
            // This should not really happen but it is dificult to guarentee
            // it so deal with it.
            while let Ok(output) = consensus_output.try_recv() {
                if current_epoch == output.sub_dag.leader_epoch() {
                    // Found some extra output...
                    // Anything from the epoch we closed should be garbage.
                    self.consensus_db.remove_consensus_by_hash(output.digest().into());
                }
            }
        } else {
            self.send_leftover_consensus_output_to_engine(&mut consensus_output, to_engine).await;
            res = RunEpochMode::ModeChange;
        }

        // clear tables
        if clear_tables_for_next_epoch {
            self.clear_consensus_db_for_next_epoch()?;
        }

        Ok(res)
    }

    // If we stopped waiting on the epoch boundary so lets make sure that the consensus queue
    // is sent to the engine. If we don't do this it is possible that a quick
    // exit could orphan output (for instance a CVV that is behind).
    // We need to go until all the consensus output in DB has been sent to the engine (if it was
    // saved it should have been sent).
    async fn send_leftover_consensus_output_to_engine(
        &mut self,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
        to_engine: &mpsc::Sender<ConsensusOutput>,
    ) {
        while let Ok(output) = consensus_output.try_recv() {
            // only forward the output to the engine
            let _ = to_engine.send(output).await;
        }
    }

    /// Record the epoch record for just completed epoch in our DB.
    /// Also record this in the manager for posible signing/collection of signatures.
    async fn write_epoch_record(
        &mut self,
        primary: &PrimaryNode<DB>,
        engine: &ExecutionNode,
    ) -> eyre::Result<()> {
        let committee = primary.current_committee().await;
        let epoch = committee.epoch();
        if epoch == 0 {
            // Epoch 0 will have a "dummy" epoch record to make the initial committee avaliable to
            // code using these records. In this case there will not be a cert so we
            // want to overwrite this with the correct record. That is why we need to
            // use Some(_) (this means we have a certificate) instead of _ like in the general case.
            // Without this we never overwrite the dummy epoch 0 record with the proper record and
            // would break sync.
            if let Some((epoch_rec, Some(_))) = self.consensus_db.get_epoch_by_number(epoch) {
                // We already have this record...
                self.epoch_record = Some(epoch_rec);
                return Ok(());
            }
        } else if let Some((epoch_rec, _)) = self.consensus_db.get_epoch_by_number(epoch) {
            // We already have this record...
            self.epoch_record = Some(epoch_rec);
            return Ok(());
        }

        let committee_keys = engine.validators_for_epoch(epoch).await?;
        let next_committee_keys = engine.validators_for_epoch(epoch + 1).await?;
        let parent_hash = if epoch == 0 {
            B256::default()
        } else if let Some(prev) = self.consensus_db.get::<EpochRecords>(&(epoch - 1))? {
            if committee_keys != prev.next_committee {
                error!(
                    target: "epoch-manager",
                    "Last epochs next committee not equal to this epochs committee! previous {:?}, current {:?}",
                    prev.next_committee,
                    committee_keys
                );
                return Err(eyre!(
                    "Last epochs next committee not equal to this epochs committee!"
                ));
            }
            prev.digest()
        } else {
            error!(
                target: "epoch-manager",
                "failed to find previous epoch record when starting epoch",
            );
            return Err(eyre!("failed to find previous epoch record when starting epoch"));
        };
        // Note on the unwrap_or_default(), if we are here then consensus was produced or followed
        // so this watch would have to have a value.
        // If somehow this is not true then this will produce an invalid epoch record which
        // will not get signed so will not pollute the network.
        let target_hash = self
            .consensus_bus
            .last_consensus_header()
            .borrow()
            .clone()
            .ok_or_eyre("no consensus header after an epoch!")?
            .digest();
        let parent_state = self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();

        let epoch_rec = EpochRecord {
            epoch,
            committee: committee_keys,
            next_committee: next_committee_keys,
            parent_hash,
            parent_state,
            parent_consensus: target_hash,
        };

        self.consensus_db.save_epoch_record(&epoch_rec);
        self.epoch_record = Some(epoch_rec);
        Ok(())
    }

    /// Extremely local helper fn used by the committee_epoch_certs task..
    /// Checks to see if any keys in committee_keys signed the cert and
    /// that the cert hash matches hash and returns the BlsPublicKey of the signer if found.
    fn signed_by_committee(
        committee_keys: &[BlsPublicKey],
        vote: &EpochVote,
        hash: B256,
    ) -> Option<BlsPublicKey> {
        if vote.epoch_hash == hash
            && committee_keys.contains(&vote.public_key)
            && vote.check_signature()
        {
            return Some(vote.public_key);
        }
        None
    }

    /// Start a task to collect the epoch record votes previous epochs record.
    /// This should run quickly at epoch start and make epoch records/certs available to syncing
    /// nodes.
    #[tracing::instrument(
        target = "telcoin",
        skip(self, primary, epoch_task_manager),
        level = "info"
    )]
    async fn collect_epoch_votes(
        &self,
        primary: &PrimaryNode<DB>,
        epoch_rec: EpochRecord,
        epoch_task_manager: &TaskManager,
    ) -> eyre::Result<()> {
        if let Some((_, Some(_))) = self.consensus_db.get_epoch_by_number(epoch_rec.epoch) {
            // We already have this record and cert...
            return Ok(());
        }

        let mut committee_keys: HashSet<BlsPublicKey> =
            epoch_rec.committee.iter().copied().collect();
        let committee_index: HashMap<BlsPublicKey, usize> =
            epoch_rec.committee.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        let consensus_db = self.consensus_db.clone();

        let epoch_hash = epoch_rec.digest();

        let me = self.builder.tn_config.primary_bls_key();
        let committee_size = committee_keys.len() as u64;
        let quorum = epoch_rec.super_quorum();
        let mut sigs = Vec::new();
        let mut signed_authorities = roaring::RoaringBitmap::new();
        let primary_network = primary.network_handle().await;
        let mut my_vote = None;
        // We are in the committee so sign and gossip the epoch record.
        if committee_keys.contains(me) {
            committee_keys.remove(me);
            let epoch_vote = epoch_rec.sign_vote(&self.key_config);
            sigs.push(epoch_vote.signature);
            if let Some(idx) = committee_index.get(&self.key_config.primary_public_key()) {
                signed_authorities.insert(*idx as u32);
            }
            info!(
                target: "epoch-manager",
                "publishing epoch record {epoch_hash}",
            );
            let _ = primary_network.publish_epoch_vote(epoch_vote).await;
            my_vote = Some(epoch_vote);
        }

        let mut rx = self.consensus_bus.new_epoch_votes().subscribe();
        epoch_task_manager.spawn_task("Collect Epoch Signatures", async move {
            let mut reached_quorum = false;
            let mut timeout = Duration::from_secs(5);
            let mut timeouts = 0;
            let mut alt_recs: HashMap<B256, usize> = HashMap::default();
            loop {
                match tokio::time::timeout(timeout, rx.recv()).await {
                    Ok(Some((vote, vote_tx))) => {
                        if let Some(source) =
                            Self::signed_by_committee(&epoch_rec.committee, &vote, epoch_hash)
                        {
                            let _ = vote_tx.send(Ok(())); // If we lost this channel somehow then no big deal.
                            if committee_keys.remove(&source) {
                                sigs.push(vote.signature);
                                if let Some(idx) = committee_index.get(&source) {
                                    signed_authorities.insert(*idx as u32);
                                }
                                if signed_authorities.len() >= quorum as u64 {
                                    reached_quorum = true;
                                    // We have quorum so just wait a sec longer for new certs then
                                    // move on.
                                    timeout = Duration::from_secs(1);
                                }
                                if signed_authorities.len() >= committee_size {
                                    break;
                                }
                            }
                        } else {
                            // Send an error back to punish the peer that sent a bad epoch vote.
                            let err = if vote.epoch_hash != epoch_hash {
                                if epoch_rec.committee.contains(&vote.public_key)
                                    && vote.check_signature()
                                {
                                    // If we got a valid vote on another epoch record track that and break if we get quorum...
                                    // This will cause us to query the record and cert from a peer.
                                    let votes = *alt_recs.get(&vote.epoch_hash).unwrap_or(&0);
                                    if votes + 1 >= quorum {
                                        error!(
                                            target: "epoch-manager",
                                            "Reached quorum on epoch record {} instead of {}.",
                                            vote.epoch_hash,
                                            epoch_hash,
                                        );
                                        let _ = vote_tx.send(Err(HeaderError::InvalidHeaderDigest));
                                        break;
                                    }
                                    alt_recs.insert(vote.epoch_hash, votes + 1);
                                }
                                HeaderError::InvalidHeaderDigest
                            } else if epoch_rec.committee.contains(&vote.public_key) {
                                HeaderError::UnknownAuthority(format!(
                                    "{} not in the committee for epoch {epoch_hash}",
                                    vote.public_key
                                ))
                            } else {
                                HeaderError::PeerNotAuthor
                            };
                            error!(
                                target: "epoch-manager",
                                ?err,
                                "Received an invalid epoch cert from {} for {}.",
                                vote.public_key,
                                vote.epoch_hash,
                            );
                            let _ = vote_tx.send(Err(err)); // If we lost this channel somehow then no big deal.
                        }
                    }
                    Ok(None) => break, // channel issues...
                    Err(_) => {
                        // We timed out but have reached quorum so good enough
                        // Or we have failed after a minute try break and try to request the cert
                        // instead.
                        if reached_quorum || timeouts > 12 {
                            break;
                        }
                        timeouts += 1;
                        // Timed out, maybe we are not the only ones having issues so republish.
                        if let Some(vote) = my_vote {
                            let _ = primary_network.publish_epoch_vote(vote).await;
                        }
                    }
                }
            }
            if reached_quorum {
                info!(
                    target: "epoch-manager",
                    "reached quorum on epoch close for {epoch_hash}",
                );
                match BlsAggregateSignature::aggregate(&sigs[..], true) {
                    Ok(aggregated_signature) => {
                        let signature: BlsSignature = aggregated_signature.to_signature();
                        let cert = EpochCertificate { epoch_hash, signature, signed_authorities };
                        // Sanity check that we have generated a valid cert before saving.
                        if epoch_rec.verify_with_cert(&cert) {
                            let _ = consensus_db.insert::<EpochCerts>(&cert.epoch_hash, &cert);
                        } else {
                            error!(
                                target: "epoch-manager",
                                "failed to verify epoch record and cert for {epoch_hash}",
                            );
                        }
                    }
                    Err(_) => {
                        error!(
                            target: "epoch-manager",
                            "failed to aggregate epoch record signatures for {epoch_hash}",
                        );
                    }
                }
            } else {
                error!(
                    target: "epoch-manager",
                    "failed to reach quorum on epoch close for {epoch_hash} {epoch_rec:?}",
                );
                // Try to recover by downloading the epoch record and cert from a peer.
                let mut got_epoch_record = false;
                for _ in 0..3 {
                    // Request by epoch number in case we had a bad hash...
                    match primary_network.request_epoch_cert(Some(epoch_rec.epoch), None).await {
                        Ok((new_epoch_rec, cert)) => {
                            if new_epoch_rec.verify_with_cert(&cert) {
                                let new_epoch_hash = new_epoch_rec.digest();
                                // Humm, we got another epoch record than the one we expected...
                                // The network came to quorum on this one so lets go with it...
                                if new_epoch_hash != epoch_hash {
                                    warn!(
                                        target: "epoch-manager",
                                        "Over wrote expected epoch record {epoch_hash} with verified epoch record {new_epoch_hash}",
                                    );
                                    consensus_db.save_epoch_record_with_cert(&new_epoch_rec, &cert);
                                } else {
                                    info!(
                                        target: "epoch-manager",
                                        "retrieved cert for epoch {new_epoch_hash} from a peer",
                                    );
                                    let _ = consensus_db.insert::<EpochCerts>(&new_epoch_hash, &cert);
                                }
                                got_epoch_record = true;
                                break;
                            }
                        }
                        Err(err) => error!(
                            target: "epoch-manager",
                            "failed to retrieve epoch from a peer {epoch_hash}: {err}",
                        ),
                    }
                }
                if !got_epoch_record {
                    error!(
                        target: "epoch-manager",
                        "Failed to retrieve an epoch record for epoch {}", epoch_rec.epoch,
                    );
                }
            }
        });
        Ok(())
    }

    /// Monitor consensus output for the last block of the epoch.
    ///
    /// This method forwards all consensus output to the engine for execution.
    /// Once the epoch boundary is reached, the manager initiates the epoch transitions.
    async fn wait_for_epoch_boundary(
        &self,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
    ) -> eyre::Result<B256> {
        // receive output from consensus and forward to engine
        while let Some(mut output) = consensus_output.recv().await {
            // observe epoch boundary to initiate epoch transition
            if output.committed_at() >= self.epoch_boundary {
                info!(
                    target: "epoch-manager",
                    epoch=?output.leader().epoch(),
                    commit=?output.committed_at(),
                    epoch_boundary=?self.epoch_boundary,
                    "epoch boundary detected",
                );
                // update output so engine closes epoch
                output.close_epoch = true;

                // obtain hash to monitor execution progress
                let target_hash = output.consensus_header_hash();

                // forward the output to the engine
                to_engine.send(output).await?;
                return Ok(target_hash);
            } else {
                // only forward the output to the engine
                to_engine.send(output).await?;
            }
        }
        Err(eyre::eyre!("invalid wait for epoch end"))
    }

    /// Use accumulated gas information to set each workers base fee for the epoch.
    /// Currently a no-op.
    fn adjust_base_fees(&self, gas_accumulator: &GasAccumulator) {
        for worker_id in 0..gas_accumulator.num_workers() {
            let worker_id = worker_id as u16;
            let (_blocks, _gas_used, _gas_limit) = gas_accumulator.get_values(worker_id);
            // Change this base fee to update base fee in batches workers create.
            let _base_fee = gas_accumulator.base_fee(worker_id);
        }
    }

    /// Close an epoch after wait_for_epoch_boundary returns.
    ///
    /// This is broken out so it can shutdown the epoch tasks and not suffer race conditions
    /// in the run_epoch() select.
    async fn close_epoch(
        &self,
        shutdown_consensus: Notifier,
        gas_accumulator: &GasAccumulator,
        target_hash: B256,
    ) -> eyre::Result<()> {
        // begin consensus shutdown while engine executes
        shutdown_consensus.notify();
        self.consensus_bus.wait_for_consensus_execution(target_hash).await?;
        self.adjust_base_fees(gas_accumulator);
        gas_accumulator.clear(); // Clear the accumlated values for next epoch.
        Ok(())
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

    /// Helper method to create all consensus-related components for this epoch.
    ///
    /// Consensus components are short-lived and only relevant for the current epoch.
    async fn create_consensus(
        &mut self,
        engine: &ExecutionNode,
        epoch_task_manager: &TaskManager,
        network_config: &NetworkConfig,
        initial_epoch: bool,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<(PrimaryNode<DB>, WorkerNode<DB>)> {
        // create config for consensus
        let (consensus_config, preload_keys) =
            self.configure_consensus(engine, network_config).await?;
        let _mode = self.identify_node_mode(&consensus_config).await?;

        let primary = self
            .create_primary_node_components(
                &consensus_config,
                epoch_task_manager.get_spawner(),
                initial_epoch,
            )
            .await?;

        let engine_to_primary =
            EngineToPrimaryRpc::new(primary.consensus_bus().await, self.consensus_db.clone());
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

        let primary_handle = primary.network_handle().await;
        let prefetches = preload_keys.clone();
        // Attempt to pre-load the next couple of committee's network info.
        let _ = primary_handle.inner_handle().find_authorities(prefetches).await;
        let worker_handle = worker.network_handle().await;
        let prefetches = preload_keys.clone();
        // Attempt to pre-load the next couple of committee's network info.
        let _ = worker_handle.inner_handle().find_authorities(prefetches).await;
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
    ) -> eyre::Result<(ConsensusConfig<DB>, Vec<BlsPublicKey>)> {
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

        debug!(target: "epoch-manager", ?validators, "creating committee for validators");

        let mut next_vals: HashSet<BlsPublicKey> = HashSet::new();
        next_vals.extend(validators.keys().copied());
        let committee = self.create_committee_from_state(epoch, validators).await?;

        next_vals.extend(engine.validators_for_epoch(epoch + 1).await?.into_iter());
        next_vals.extend(engine.validators_for_epoch(epoch + 2).await?.into_iter());

        // create config for consensus
        let consensus_config = ConsensusConfig::new_for_epoch(
            self.builder.tn_config.clone(),
            self.consensus_db.clone(),
            self.key_config.clone(),
            committee,
            network_config.clone(),
        )?;

        Ok((consensus_config, next_vals.into_iter().collect()))
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
            // build the committee using kad network
            let mut committee_builder = CommitteeBuilder::new(epoch);

            for validator in validators {
                committee_builder.add_authority(
                    validator.0,
                    1, // set stake so every authority's weight is equal
                    validator.1.validatorAddress,
                );
            }
            committee_builder.build()
        };

        // load committee
        committee.load();

        Ok(committee)
    }

    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
    async fn create_primary_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: bool,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let state_sync = StateSynchronizer::new(
            consensus_config.clone(),
            self.consensus_bus.clone(),
            epoch_task_spawner.clone(),
        );
        let network_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("primary network handle missing from epoch manager")?
            .clone();

        // create the epoch-specific `PrimaryNetwork`
        self.spawn_primary_network_for_epoch(
            consensus_config,
            state_sync.clone(),
            epoch_task_spawner.clone(),
            &network_handle,
            initial_epoch,
        )
        .await?;

        // spawn primary - create node and spawn network
        let primary = PrimaryNode::new(
            consensus_config.clone(),
            self.consensus_bus.clone(),
            network_handle,
            state_sync,
        );

        Ok(primary)
    }

    /// Create a [WorkerNode].
    async fn spawn_worker_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        engine: &ExecutionNode,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: bool,
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
            if initial_epoch {
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

        let validator = engine
            .new_batch_validator(&worker_id, base_fee, consensus_config.committee().epoch())
            .await;
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
    async fn spawn_primary_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        state_sync: StateSynchronizer<DB>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &PrimaryNetworkHandle,
        initial_epoch: bool,
    ) -> eyre::Result<()> {
        // get event streams for the primary network handler
        let event_stream = self.consensus_bus.primary_network_events().clone();
        let rx_event_stream = event_stream.subscribe();

        // set committee for network to prevent banning
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "spawning primary network for epoch");
        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

        if initial_epoch {
            // Make sure we at least hove bootstrap peers on first epoch.
            network_handle
                .inner_handle()
                .add_bootstrap_peers(
                    consensus_config
                        .committee()
                        .bootstrap_servers()
                        .iter()
                        .map(|(k, v)| (*k, v.primary.clone()))
                        .collect(),
                )
                .await?;
        }

        network_handle.inner_handle().new_epoch(committee_keys.clone()).await?;
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "event stream updated!");

        // start listening if the network needs to be initialized
        if initial_epoch {
            // start listening for p2p messages
            let primary_address = Self::parse_listener_address_for_swarm(
                "PRIMARY_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.primary_address(),
            )?;
            info!(target: "epoch-manager", ?primary_address, "listening to {primary_address}");
            network_handle.inner_handle().start_listening(primary_address).await?;
        }

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::primary_topic(),
                committee_keys.into_iter().collect(),
            )
            .await?;

        let mut peers = network_handle.connected_peers_count().await.unwrap_or(0);
        if peers == 0 || self.consensus_bus.is_cvv() {
            // always dial peers for the new epoch
            // do this if a CVV (may need to connect to the other CVVs) or if we don't have any
            // peers if we are not a committee member and have peers then do not pester
            // the committee
            for (_authority_id, bls_pubkey) in consensus_config
                .committee()
                .others_primaries_by_id(consensus_config.authority_id().as_ref())
            {
                self.dial_peer_bls(
                    network_handle.inner_handle().clone(),
                    bls_pubkey,
                    epoch_task_spawner.clone(),
                );
            }
        }

        // wait until the primary has connected with at least 1 peer
        let mut retries = 0;
        while peers == 0 {
            retries += 1;
            if retries > 240 {
                // If we could not get on the network in about 2 minutes then trigger node exit.
                // This indicates a fundemental problem (maybe no network access?) that should be
                // addressed. Since this is a blockchain there is nothing useful for a
                // node to do without being on the network.
                return Err(eyre::eyre!(
                    "Unable to join telcoin network, can not connect to any peers!"
                ));
            }
            if retries % 10 == 0 {
                // Log error about every 5 seconds.
                error!(target: "epoch-manager", "failed to join the network!")
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            peers = network_handle.connected_peers_count().await.unwrap_or(0);
        }

        // spawn primary network
        PrimaryNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            self.consensus_bus.clone(),
            state_sync,
            epoch_task_spawner.clone(), // tasks should abort with epoch
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }

    /// Dial peer.
    fn dial_peer_bls<Req: TNMessage, Res: TNMessage>(
        &self,
        handle: NetworkHandle<Req, Res>,
        bls_pubkey: BlsPublicKey,
        node_task_spawner: TaskSpawner,
    ) {
        // spawn dials on long-running task manager
        let task_name = format!("DialPeer {bls_pubkey}");
        node_task_spawner.spawn_task(task_name, async move {
            let mut backoff = 1;
            let mut retries = 0;

            debug!(target: "epoch-manager", ?bls_pubkey, "dialing peer");
            while let Err(e) = handle.dial_by_bls(bls_pubkey).await {
                // ignore errors for peers that are already connected or being dialed
                if matches!(e, NetworkError::AlreadyConnected(_))
                    || matches!(e, NetworkError::AlreadyDialing(_))
                {
                    return;
                }
                retries += 1;

                warn!(target: "epoch-manager", "failed to dial {bls_pubkey}: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 120 {
                    backoff += backoff;
                }
                let peers = handle.connected_peer_count().await.unwrap_or(0);
                // We have been trying for a while (at least two max backoffs at 120 secs), if we
                // have any other peers give up.
                if retries > 10 && peers > 0 {
                    error!(target = "dial_peer", "failed to reach peer {bls_pubkey}, giving up");
                    return;
                }
            }
        });
    }

    /// Create the worker network.
    async fn spawn_worker_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &WorkerNetworkHandle,
        initial_epoch: bool,
    ) -> eyre::Result<()> {
        // get event streams for the worker network handler
        let rx_event_stream = self.worker_event_stream.subscribe();
        debug!(target: "epoch-manager", "spawning worker network for epoch");

        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();
        network_handle.inner_handle().new_epoch(committee_keys.clone()).await?;

        // start listening if the network needs to be initialized
        if initial_epoch {
            let worker_address = Self::parse_listener_address_for_swarm(
                "WORKER_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.worker_address(),
            )?;
            network_handle.inner_handle().start_listening(worker_address).await?;
            // Make sure we at least hove bootstrap peers on first epoch.
            network_handle
                .inner_handle()
                .add_bootstrap_peers(
                    consensus_config
                        .committee()
                        .bootstrap_servers()
                        .iter()
                        .map(|(k, v)| (*k, v.worker.clone()))
                        .collect(),
                )
                .await?;
        }

        let worker_address = consensus_config.worker_address();

        // always attempt to dial peers for the new epoch
        // the network's peer manager will intercept dial attempts for peers that are already
        // connected
        debug!(target: "epoch-manager", ?worker_address, "spawning worker network for epoch");
        for (_, peer) in consensus_config
            .committee()
            .others_primaries_by_id(consensus_config.authority().as_ref().map(|a| a.id()).as_ref())
        {
            self.dial_peer_bls(
                network_handle.inner_handle().clone(),
                peer,
                epoch_task_spawner.clone(),
            );
        }

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::worker_txn_topic())
            .await?;
        // Get gossip from committee members about batches.
        // Useful for non-CVVs to prefetch and harmless for CVVs.
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::worker_batch_topic(),
                committee_keys.into_iter().collect(),
            )
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
    async fn try_restore_state(&self, engine: &ExecutionNode) -> eyre::Result<()> {
        // prime the recent_blocks watch with latest executed blocks
        let block_capacity = self.consensus_bus.recent_blocks().borrow().block_capacity();

        for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
            self.consensus_bus
                .recent_blocks()
                .send_modify(|blocks| blocks.push_latest(recent_block));
        }

        Ok(())
    }

    /// Helper method to identify the node's mode:
    /// - "Committee-voting Validator" (CVV)
    /// - "Committee-voting Validator Inactive" (CVVInactive - syncing to rejoin)
    /// - "Observer"
    ///
    /// This method also updates the `ConsensusBus::node_mode()`.
    async fn identify_node_mode(
        &self,
        consensus_config: &ConsensusConfig<DB>,
    ) -> eyre::Result<NodeMode> {
        if self.consensus_bus.is_cvv_inactive() {
            // If we have an inactive mode then it was set so keep it for now.
            return Ok(NodeMode::CvvInactive);
        }
        debug!(target: "epoch-manager", authority_id=?consensus_config.authority_id(), "identifying node mode..." );
        let in_committee = consensus_config
            .authority_id()
            .map(|id| consensus_config.in_committee(&id))
            .unwrap_or(false);
        state_sync::prime_consensus(&self.consensus_bus, consensus_config).await;
        let mode = if !in_committee || self.builder.tn_config.observer {
            NodeMode::Observer
        } else {
            // Assume we are caught up, will be demoted to inactive if this is not true...
            NodeMode::CvvActive
        };

        debug!(target: "epoch-manager", ?mode, "node mode identified");
        // update consensus bus
        self.consensus_bus.node_mode().send_modify(|v| *v = mode);

        Ok(mode)
    }

    /// Spawn a task to update `ConsensusBus::recent_blocks` everytime the engine produces a new
    /// final block.
    fn spawn_engine_update_task(
        &self,
        mut engine_state: CanonStateNotificationStream,
        task_manager: &TaskManager,
    ) {
        // spawn epoch-specific task to forward blocks from the engine to consensus
        let consensus_bus = self.consensus_bus.clone();
        task_manager.spawn_critical_task("latest execution block", async move {
            while let Some(latest) = engine_state.next().await {
                consensus_bus
                    .recent_blocks()
                    .send_modify(|blocks| blocks.push_latest(latest.tip().clone_sealed_header()));
            }
            error!(target: "engine", "engine state stream ended, node will exit");
        });
    }

    /// Clear the epoch-related tables for consensus.
    ///
    /// These tables are epoch-specific. Complete historic data is stored
    /// in the `ConsensusBlocks` table.
    fn clear_consensus_db_for_next_epoch(&self) -> eyre::Result<()> {
        self.consensus_db.clear_table::<LastProposed>()?;
        self.consensus_db.clear_table::<Votes>()?;
        self.consensus_db.clear_table::<Certificates>()?;
        self.consensus_db.clear_table::<CertificateDigestByRound>()?;
        self.consensus_db.clear_table::<CertificateDigestByOrigin>()?;
        self.consensus_db.clear_table::<Payload>()?;
        Ok(())
    }

    /// Helper method for parsing provided env var with fallback [Multiaddr]. This is useful to
    /// override the primary/worker swarm listner address for cloud deployments.
    fn parse_listener_address_for_swarm(
        env_var: &str,
        network_pubkey: NetworkPublicKey,
        fallback: Multiaddr,
    ) -> eyre::Result<Multiaddr> {
        std::env::var(env_var)
            .map(|addr| {
                addr.parse()
                    .map_err(|e| {
                        eyre::eyre!(
                            "Failed to parse listener multiaddr from env {env_var} ({addr})\n{e}"
                        )
                    })
                    // add Protocol::P2p to multiaddr to maintain consistency with
                    // bin/telcoin-network/src/keytool/generate.rs
                    .and_then(|multi: Multiaddr| {
                        multi.with_p2p(network_pubkey.into()).map_err(|_| {
                            eyre::eyre!(
                                "{env_var} multiaddr contains a different P2P protocol {:?}",
                                std::env::var(env_var)
                            )
                        })
                    })
            })
            .unwrap_or(Ok(fallback))
    }
}
