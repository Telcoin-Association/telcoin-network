//! Hierarchical type to hold tasks spawned for a worker in the network.
use anemo::{Network, PeerId};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_types::{BatchValidation, Database as ConsensusDatabase, TaskManager, WorkerId};
use tn_worker::{metrics::Metrics, quorum_waiter::QuorumWaiter, BatchProvider, Worker};
use tokio::sync::RwLock;
use tracing::instrument;

pub struct WorkerNodeInner<CDB> {
    /// The worker's id
    id: WorkerId,
    /// The consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    /// Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
    /// Keep the worker around.
    worker: Option<Worker<CDB>>,
}

impl<CDB: ConsensusDatabase> WorkerNodeInner<CDB> {
    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(name = "worker", skip_all)]
    async fn start(
        &mut self,
        validator: Arc<dyn BatchValidation>,
    ) -> eyre::Result<(TaskManager, BatchProvider<CDB, QuorumWaiter>)> {
        let task_manager = TaskManager::new("Worker Task Manager");
        self.own_peer_id = Some(PeerId(
            self.consensus_config.key_config().primary_network_public_key().0.to_bytes(),
        ));

        let metrics = Metrics::default();

        let (worker, batch_provider) = Worker::spawn(
            self.id,
            validator,
            metrics,
            self.consensus_config.clone(),
            &task_manager,
        );

        // now keep the handles
        self.worker = Some(worker);

        Ok((task_manager, batch_provider))
    }
}

#[derive(Clone)]
pub struct WorkerNode<CDB> {
    internal: Arc<RwLock<WorkerNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> WorkerNode<CDB> {
    pub fn new(id: WorkerId, consensus_config: ConsensusConfig<CDB>) -> WorkerNode<CDB> {
        let inner = WorkerNodeInner { id, consensus_config, own_peer_id: None, worker: None };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start(
        &self,
        validator: Arc<dyn BatchValidation>,
    ) -> eyre::Result<(TaskManager, BatchProvider<CDB, QuorumWaiter>)> {
        let mut guard = self.internal.write().await;
        guard.start(validator).await
    }

    /// Return the WAN if the worker is runnig.
    pub async fn network(&self) -> Option<Network> {
        self.internal.read().await.worker.as_ref().map(|w| w.network().clone())
    }
}
