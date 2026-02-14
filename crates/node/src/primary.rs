//! Hierarchical type to hold tasks spawned for a worker in the network.
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_executor::{Executor, SubscriberResult};
use tn_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus, Primary, StateSynchronizer,
};
use tn_storage::consensus::ConsensusChain;
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
    /// 60 should rotate the reputations about every 10 minutes with 10 second commits (based on 5
    /// second round times).
    /// NOTE: Changing this value WILL REQUIRE A FORK.  All nodes must agree on the schedule change.
    const CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS: u32 = 60;

    /// Starts the primary node with the provided info. If the node is already running then this
    /// method will return an error instead.
    async fn start(
        &mut self,
        task_manager: &TaskManager,
        consensus_chain: ConsensusChain,
    ) -> eyre::Result<()> {
        // spawn primary and update `self`
        self.spawn_primary(task_manager, consensus_chain).await?;

        Ok(())
    }

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing
    /// transactions.
    async fn spawn_primary(
        &mut self,
        task_manager: &TaskManager,
        consensus_chain: ConsensusChain,
    ) -> SubscriberResult<()> {
        let leader_schedule =
            self.spawn_consensus(&self.consensus_bus, task_manager, consensus_chain).await?;

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
        mut consensus_chain: ConsensusChain,
    ) -> SubscriberResult<LeaderSchedule> {
        let leader_schedule = LeaderSchedule::from_store(
            self.consensus_config.committee().clone(),
            &mut consensus_chain,
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        )
        .await;

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            self.consensus_config.committee().clone(),
            Self::CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS,
            leader_schedule.clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );
        Consensus::spawn(
            self.consensus_config.clone(),
            consensus_bus,
            ordering_engine,
            task_manager,
            consensus_chain.clone(),
        )
        .await;

        // Spawn the client executing the transactions.
        // It also synchronizes with the subscriber handler if it missed some transactions.
        Executor::spawn(
            self.consensus_config.clone(),
            self.consensus_config.shutdown().subscribe(),
            consensus_bus.clone(),
            task_manager,
            self.primary.network_handle().clone(),
            consensus_chain,
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

    pub async fn start(
        &self,
        task_manager: &TaskManager,
        consensus_chain: ConsensusChain,
    ) -> eyre::Result<()>
    where
        CDB: ConsensusDatabase,
    {
        let mut guard = self.internal.write().await;
        guard.start(task_manager, consensus_chain).await
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
