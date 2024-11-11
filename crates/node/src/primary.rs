// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::{engine::ExecutionNode, error::NodeError, try_join_all, FuturesUnordered};
use anemo::PeerId;
use fastcrypto::traits::VerifyingKey;
use narwhal_primary::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule},
    ConsensusBus, Primary, CHANNEL_CAPACITY,
};
use narwhal_primary_metrics::Metrics;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use std::{sync::Arc, time::Instant};
use tn_config::ConsensusConfig;
use tn_executor::{get_restored_consensus_output, Executor, SubscriberResult};
use tn_storage::traits::Database as ConsensusDatabase;
use tn_types::{BlsPublicKey, ConsensusOutput, DEFAULT_BAD_NODES_STAKE_THRESHOLD};
use tokio::{
    sync::{broadcast, RwLock},
    task::JoinHandle,
};
use tracing::{info, instrument};

struct PrimaryNodeInner<CDB> {
    /// Consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    /// Container for consensus channels.
    consensus_bus: ConsensusBus,
    /// The task handles created from primary
    handles: FuturesUnordered<JoinHandle<()>>,
    /// Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
    /// Consensus broadcast channel.
    ///
    /// NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
    consensus_output_notification_sender: broadcast::Sender<ConsensusOutput>,
}

impl<CDB: ConsensusDatabase> PrimaryNodeInner<CDB> {
    /// The window where the schedule change takes place in consensus. It represents number
    /// of committed sub dags.
    /// TODO: move this to node properties
    const CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS: u64 = 300;

    /// Starts the primary node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(name = "primary_node", skip_all)]
    async fn start<DB, Evm, CE>(
        &mut self,
        // Execution components needed to spawn the EL Executor
        execution_components: &ExecutionNode<DB, Evm, CE>,
    ) -> eyre::Result<()>
    where
        DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
        Evm: BlockExecutorProvider + Clone + 'static,
        CE: ConfigureEvm,
    {
        if self.is_running().await {
            return Err(NodeError::NodeAlreadyRunning.into());
        }

        self.own_peer_id = Some(PeerId(
            self.consensus_config.key_config().primary_network_public_key().0.to_bytes(),
        ));

        // used to retrieve the last executed certificate in case of restarts
        let last_executed_sub_dag_index =
            execution_components.last_executed_output().await.expect("execution found HEAD");

        // create receiving channel before spawning primary to ensure messages are not lost
        let consensus_output_rx = self.subscribe_consensus_output();

        // spawn primary if not already running
        let primary_handles = self.spawn_primary(last_executed_sub_dag_index).await?;

        // start engine
        execution_components.start_engine(consensus_output_rx).await?;

        // now keep the handlers
        self.handles.clear();
        self.handles.extend(primary_handles);

        Ok(())
    }

    /// Will shutdown the primary node and wait until the node has shutdown by waiting on the
    /// underlying components handles. If the node was not already running then the
    /// method will return immediately.
    #[instrument(level = "info", skip_all)]
    async fn shutdown(&mut self) {
        if !self.is_running().await {
            return;
        }

        // send the shutdown signal to the node
        let now = Instant::now();

        self.consensus_config.network_client().shutdown();
        self.consensus_config.shutdown();

        // Now wait until handles have been completed
        try_join_all(&mut self.handles).await.unwrap();

        info!(
            "Narwhal primary shutdown is complete - took {} seconds",
            now.elapsed().as_secs_f64()
        );
    }

    // Helper method useful to wait on the execution of the primary node
    async fn wait(&mut self) {
        try_join_all(&mut self.handles).await.unwrap();
    }

    // If any of the underlying handles haven't still finished, then this method will return
    // true, otherwise false will returned instead.
    async fn is_running(&self) -> bool {
        self.handles.iter().any(|h| !h.is_finished())
    }

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing
    /// transactions.
    pub async fn spawn_primary(
        &self,
        // Used for recovering after crashes/restarts
        last_executed_sub_dag_index: u64,
    ) -> SubscriberResult<Vec<JoinHandle<()>>> {
        let mut handles = Vec::new();

        let (consensus_handles, leader_schedule) =
            self.spawn_consensus(&self.consensus_bus, last_executed_sub_dag_index).await?;
        handles.extend(consensus_handles);

        // Spawn the primary.
        let primary_handles =
            Primary::spawn(self.consensus_config.clone(), &self.consensus_bus, leader_schedule);
        handles.extend(primary_handles);

        Ok(handles)
    }

    /// Spawn the consensus core and the client executing transactions.
    ///
    /// Pass the sender channel for consensus output and executor metrics.
    async fn spawn_consensus(
        &self,
        consensus_bus: &ConsensusBus,
        last_executed_sub_dag_index: u64,
    ) -> SubscriberResult<(Vec<JoinHandle<()>>, LeaderSchedule)>
    where
        BlsPublicKey: VerifyingKey,
    {
        // TODO: this may need to be adjusted depending on how TN executes output
        //
        // if executor engine executes-per-batch:
        //  - executing consensus output per batch ~> 1 batch == 1 block
        //  - use nonce or mixed hash or difficulty to track the output's "last committed subdag"
        //  - how to handle restart mid-execution?
        //      - only some of the batches completed
        //      - so, don't use `last_executed_sub_dag_index + 1`
        //          - use just `last_executed_sub_dag_index` and re-execute the output again
        //          - less efficient, but ensures correctness
        //          - maybe there's a way to optimize this like only re-execute if the
        //            blocks/batches don't match the output batch count?
        //              - but this is probably unlikely to happen anyway
        //                  - ie) crashes/restarts at a clean execution boundary
        //
        // for now, all batches are contained within a single block, (ie 1 consensus output = 1
        // executed block) so use block number for restored consensus output
        // since block number == last_executed_sub_dag_index

        // Check for any sub-dags that have been sent by consensus but were not processed by the
        // executor.
        let restored_consensus_output = get_restored_consensus_output(
            self.consensus_config.node_storage().consensus_store.clone(),
            self.consensus_config.node_storage().certificate_store.clone(),
            last_executed_sub_dag_index,
        )
        .await?;

        let num_sub_dags = restored_consensus_output.len() as u64;
        if num_sub_dags > 0 {
            info!(
                "Consensus output on its way to the executor was restored for {num_sub_dags} sub-dags",
            );
        }
        self.consensus_bus.consensus_metrics().recovered_consensus_output.inc_by(num_sub_dags);

        let leader_schedule = LeaderSchedule::from_store(
            self.consensus_config.committee().clone(),
            self.consensus_config.node_storage().consensus_store.clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            self.consensus_config.committee().clone(),
            self.consensus_config.node_storage().consensus_store.clone(),
            self.consensus_bus.consensus_metrics().clone(),
            Self::CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS,
            leader_schedule.clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );
        let consensus_handle =
            Consensus::spawn(self.consensus_config.clone(), consensus_bus, ordering_engine);

        // Spawn the client executing the transactions. It can also synchronize with the
        // subscriber handler if it missed some transactions.
        let executor_handle = Executor::spawn(
            self.consensus_config.clone(),
            self.consensus_config.subscribe_shutdown(),
            consensus_bus.clone(),
            restored_consensus_output,
            self.consensus_output_notification_sender.clone(),
        )?;

        let handles = vec![executor_handle, consensus_handle];

        Ok((handles, leader_schedule))
    }

    /// Subscribe to [ConsensusOutput] broadcast.
    ///
    /// NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
    pub fn subscribe_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        self.consensus_output_notification_sender.subscribe()
    }
}

#[derive(Clone)]
pub struct PrimaryNode<CDB> {
    internal: Arc<RwLock<PrimaryNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> PrimaryNode<CDB> {
    pub fn new(consensus_config: ConsensusConfig<CDB>) -> PrimaryNode<CDB> {
        // TODO: what is an appropriate channel capacity? CHANNEL_CAPACITY currently set to 10k
        // which seems really high but is consistent for now
        let (consensus_output_notification_sender, _receiver) =
            tokio::sync::broadcast::channel(CHANNEL_CAPACITY);

        let consensus_bus = ConsensusBus::new();
        let inner = PrimaryNodeInner {
            consensus_config,
            consensus_bus,
            handles: FuturesUnordered::new(),
            own_peer_id: None,
            consensus_output_notification_sender,
        };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start<DB, Evm, CE>(
        &self,
        // Execution components needed to spawn the EL Executor
        execution_components: &ExecutionNode<DB, Evm, CE>,
    ) -> eyre::Result<()>
    where
        DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
        Evm: BlockExecutorProvider + Clone + 'static,
        CE: ConfigureEvm,
        CDB: ConsensusDatabase,
    {
        let mut guard = self.internal.write().await;
        guard.start(execution_components).await
    }

    pub async fn shutdown(&self) {
        let mut guard = self.internal.write().await;
        guard.shutdown().await
    }

    pub async fn is_running(&self) -> bool {
        let guard = self.internal.read().await;
        guard.is_running().await
    }

    pub async fn wait(&self) {
        let mut guard = self.internal.write().await;
        guard.wait().await
    }

    pub async fn subscribe_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        let guard = self.internal.read().await;
        guard.consensus_output_notification_sender.subscribe()
    }

    /// Return the consensus metrics.
    pub async fn consensus_metrics(&self) -> Arc<ConsensusMetrics> {
        self.internal.read().await.consensus_bus.consensus_metrics()
    }

    /// Return the primary metrics.
    pub async fn primary_metrics(&self) -> Arc<Metrics> {
        self.internal.read().await.consensus_bus.primary_metrics()
    }
}
