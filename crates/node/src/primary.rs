//! Hierarchical type to hold tasks spawned for a worker in the network.
use anemo::Network;
use fastcrypto::traits::VerifyingKey;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_executor::{Executor, SubscriberResult};
use tn_network_libp2p::types::{NetworkEvent, NetworkHandle};
use tn_primary::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule},
    network::{PrimaryRequest, PrimaryResponse},
    ConsensusBus, Primary,
};
use tn_primary_metrics::Metrics;
use tn_storage::traits::Database as ConsensusDatabase;
use tn_types::{BlsPublicKey, TaskManager, DEFAULT_BAD_NODES_STAKE_THRESHOLD};
use tokio::sync::{mpsc, RwLock};
use tracing::instrument;

struct PrimaryNodeInner<CDB> {
    /// Consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    /// Container for consensus channels.
    consensus_bus: ConsensusBus,
    /// The primary struct that holds handles and network.
    primary: Primary<CDB>,
}

impl<CDB: ConsensusDatabase> PrimaryNodeInner<CDB> {
    /// The window where the schedule change takes place in consensus. It represents number
    /// of committed sub dags.
    /// TODO: move this to node properties
    const CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS: u32 = 300;

    /// Starts the primary node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(name = "primary_node", skip_all)]
    async fn start(&mut self) -> eyre::Result<TaskManager> {
        let task_manager = TaskManager::new("Primary Task Manager");
        // spawn primary and update `self`
        self.spawn_primary(&task_manager).await?;

        Ok(task_manager)
    }

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing
    /// transactions.
    async fn spawn_primary(&mut self, task_manager: &TaskManager) -> SubscriberResult<()> {
        let leader_schedule = self.spawn_consensus(&self.consensus_bus, task_manager).await?;

        self.primary.spawn(
            self.consensus_config.clone(),
            &self.consensus_bus,
            leader_schedule,
            task_manager,
        );
        Ok(())
    }

    /// Spawn the consensus core and the client executing transactions.
    ///
    /// Pass the sender channel for consensus output and executor metrics.
    async fn spawn_consensus(
        &self,
        consensus_bus: &ConsensusBus,
        task_manager: &TaskManager,
    ) -> SubscriberResult<LeaderSchedule>
    where
        BlsPublicKey: VerifyingKey,
    {
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
        Consensus::spawn(
            self.consensus_config.clone(),
            consensus_bus,
            ordering_engine,
            task_manager,
        );

        // Spawn the client executing the transactions. It can also synchronize with the
        // subscriber handler if it missed some transactions.
        Executor::spawn(
            self.consensus_config.clone(),
            self.consensus_config.shutdown().subscribe(),
            consensus_bus.clone(),
            task_manager,
            self.primary.network().clone(),
        );

        Ok(leader_schedule)
    }
}

#[derive(Clone)]
pub struct PrimaryNode<CDB> {
    internal: Arc<RwLock<PrimaryNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> PrimaryNode<CDB> {
    pub fn new(
        consensus_config: ConsensusConfig<CDB>,
        consensus_bus: ConsensusBus,
        network_p2p_handle: NetworkHandle<PrimaryRequest, PrimaryResponse>,
        network_event_stream: mpsc::Receiver<NetworkEvent<PrimaryRequest, PrimaryResponse>>,
    ) -> PrimaryNode<CDB> {
        let primary = Primary::new(
            consensus_config.clone(),
            &consensus_bus,
            network_p2p_handle,
            network_event_stream,
        );

        let inner = PrimaryNodeInner { consensus_config, consensus_bus, primary };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start(&self) -> eyre::Result<TaskManager>
    where
        CDB: ConsensusDatabase,
    {
        let mut guard = self.internal.write().await;
        guard.start().await
    }

    pub async fn shutdown(&self) {
        let guard = self.internal.write().await;
        guard.consensus_config.shutdown().notify();
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
    pub async fn network(&self) -> Network {
        self.internal.read().await.primary.network().clone()
    }

    /// Return the WAN handke if the primary p2p is runnig.
    pub async fn network_handle(&self) -> NetworkHandle<PrimaryRequest, PrimaryResponse> {
        self.internal.read().await.primary.network_p2p().clone()
    }
}
