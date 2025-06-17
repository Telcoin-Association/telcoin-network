//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::manager::PRIMARY_TASK_MANAGER;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_executor::{Executor, SubscriberResult};
use tn_primary::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus, Primary, StateSynchronizer,
};
use tn_primary_metrics::Metrics;
use tn_types::{
    Committee, Database as ConsensusDatabase, Notifier, TaskManager,
    DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use tokio::sync::RwLock;

#[derive(Debug)]
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
    async fn start(&mut self) -> eyre::Result<TaskManager> {
        let task_manager = TaskManager::new(PRIMARY_TASK_MANAGER);
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
    ) -> SubscriberResult<LeaderSchedule> {
        let leader_schedule = LeaderSchedule::from_store(
            self.consensus_config.committee().clone(),
            self.consensus_config.node_storage().clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            self.consensus_config.committee().clone(),
            self.consensus_config.node_storage().clone(),
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

        // Spawn the client executing the transactions.
        // It also synchronizes with the subscriber handler if it missed some transactions.
        Executor::spawn(
            self.consensus_config.clone(),
            self.consensus_config.shutdown().subscribe(),
            consensus_bus.clone(),
            task_manager,
            self.primary.network_handle().clone(),
        );

        Ok(leader_schedule)
    }
}

#[derive(Clone, Debug)]
pub struct PrimaryNode<CDB> {
    internal: Arc<RwLock<PrimaryNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> PrimaryNode<CDB> {
    pub fn new(
        consensus_config: ConsensusConfig<CDB>,
        consensus_bus: ConsensusBus,
        network: PrimaryNetworkHandle,
        state_sync: StateSynchronizer<CDB>,
    ) -> PrimaryNode<CDB> {
        let primary = Primary::new(consensus_config.clone(), &consensus_bus, network, state_sync);

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

    /// Return the WAN handle if the primary p2p is runnig.
    pub async fn network_handle(&self) -> PrimaryNetworkHandle {
        self.internal.read().await.primary.network_handle().clone()
    }

    /// Return the [StateSynchronizer]
    pub async fn state_sync(&self) -> StateSynchronizer<CDB> {
        self.internal.read().await.primary.state_sync()
    }

    /// Return the [Noticer] shutdown for consensus.
    pub async fn shutdown_signal(&self) -> Notifier {
        self.internal.read().await.consensus_config.shutdown().clone()
    }

    /// Returns the current committee.
    pub async fn current_committee(&self) -> Committee {
        self.internal.read().await.consensus_config.committee().clone()
    }
}
