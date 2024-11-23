// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::{engine::ExecutionNode, error::NodeError, try_join_all, FuturesUnordered};
use anemo::{Network, PeerId};
use fastcrypto::traits::VerifyingKey;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_primitives::B256;
use std::{sync::Arc, time::Instant};
use tn_config::ConsensusConfig;
use tn_executor::{Executor, SubscriberResult};
use tn_network::inner_node::PrimaryInnerNetworkHandle;
use tn_primary::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule},
    ConsensusBus, Primary,
};
use tn_primary_metrics::Metrics;
use tn_storage::traits::Database as ConsensusDatabase;
use tn_types::{BlsPublicKey, DEFAULT_BAD_NODES_STAKE_THRESHOLD};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{info, instrument};

struct PrimaryNodeInner<CDB> {
    /// Consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    /// Container for consensus channels.
    consensus_bus: ConsensusBus,
    /// Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
    /// The primary struct that holds handles and network.
    ///
    /// Option is some after the primary has started.
    primary: Option<Primary>,
    /// The handles for consensus.
    handles: FuturesUnordered<JoinHandle<()>>,
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
        let last_executed_consensus_hash =
            execution_components.last_executed_output().await.expect("execution found HEAD");

        // create receiving channel before spawning primary to ensure messages are not lost
        let consensus_output_rx = self.consensus_bus.subscribe_consensus_output();

        // spawn primary and update `self`
        self.spawn_primary(last_executed_consensus_hash).await?;

        // start engine
        execution_components.start_engine(consensus_output_rx).await?;

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

        self.consensus_config.local_network().shutdown();
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

    /// Boolean if any of the join handles are not "finished".
    async fn is_running(&self) -> bool {
        self.handles.iter().any(|h| !h.is_finished())
    }

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing
    /// transactions.
    pub async fn spawn_primary(
        &mut self,
        // Used for recovering after crashes/restarts
        last_executed_consensus_hash: B256,
    ) -> SubscriberResult<()> {
        let (consensus_handles, leader_schedule) =
            self.spawn_consensus(&self.consensus_bus, last_executed_consensus_hash).await?;

        // already ensures node was NOT running, but clear to be sure
        self.handles.clear();
        self.handles.extend(consensus_handles);

        // start primary add extend handles
        let (primary, handles) =
            Primary::spawn(self.consensus_config.clone(), &self.consensus_bus, leader_schedule);
        self.handles.extend(handles);

        // Spawn the primary.
        self.primary = Some(primary);
        Ok(())
    }

    /// Spawn the consensus core and the client executing transactions.
    ///
    /// Pass the sender channel for consensus output and executor metrics.
    async fn spawn_consensus(
        &self,
        consensus_bus: &ConsensusBus,
        last_executed_consensus_hash: B256,
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
            last_executed_consensus_hash,
        )?;

        let handles = vec![executor_handle, consensus_handle];

        Ok((handles, leader_schedule))
    }
}

#[derive(Clone)]
pub struct PrimaryNode<CDB> {
    internal: Arc<RwLock<PrimaryNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> PrimaryNode<CDB> {
    pub fn new(consensus_config: ConsensusConfig<CDB>) -> PrimaryNode<CDB> {
        let consensus_bus = ConsensusBus::new();
        let inner = PrimaryNodeInner {
            consensus_config,
            consensus_bus,
            own_peer_id: None,
            primary: None,
            handles: FuturesUnordered::new(),
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

    /// Return the consensus metrics.
    pub async fn consensus_metrics(&self) -> Arc<ConsensusMetrics> {
        self.internal.read().await.consensus_bus.consensus_metrics()
    }

    /// Return the primary metrics.
    pub async fn primary_metrics(&self) -> Arc<Metrics> {
        self.internal.read().await.consensus_bus.primary_metrics()
    }

    /// Return a copy of the primaries consensus bus.
    pub async fn consensus_bus(&self) -> ConsensusBus {
        self.internal.read().await.consensus_bus.clone()
    }

    /// Return the WAN if the primary is runnig.
    pub async fn network(&self) -> Option<Network> {
        self.internal.read().await.primary.as_ref().map(|p| p.network().clone())
    }
}
