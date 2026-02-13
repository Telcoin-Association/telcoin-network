//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::manager::WORKER_TASK_BASE;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_types::{BatchValidation, Database as ConsensusDatabase, WorkerId};
use tn_worker::{new_worker, quorum_waiter::QuorumWaiter, Worker, WorkerNetworkHandle};
use tokio::sync::RwLock;

#[derive(Debug)]
/// The inner-worker type.
pub struct WorkerNodeInner<CDB> {
    /// The worker's id
    id: WorkerId,
    /// The consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    /// The handle to the network.
    network_handle: WorkerNetworkHandle,
    /// The batch validator.
    validator: Arc<dyn BatchValidation>,
}

impl<CDB: ConsensusDatabase> WorkerNodeInner<CDB> {
    /// Starts the worker node with the provided info.
    ///
    /// If the node is already running then this method will return an error instead.
    ///
    /// Return the task manager for the worker and the [Worker] struct for spawning execution tasks.
    async fn new_worker(&mut self) -> eyre::Result<Worker<CDB, QuorumWaiter>> {
        let batch_provider = new_worker(
            self.id,
            self.validator.clone(),
            self.consensus_config.clone(),
            self.network_handle.clone(),
        );

        Ok(batch_provider)
    }
}

#[derive(Clone, Debug)]
pub struct WorkerNode<CDB> {
    internal: Arc<RwLock<WorkerNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> WorkerNode<CDB> {
    pub fn new(
        id: WorkerId,
        consensus_config: ConsensusConfig<CDB>,
        network_handle: WorkerNetworkHandle,
        validator: Arc<dyn BatchValidation>,
    ) -> WorkerNode<CDB> {
        let inner = WorkerNodeInner { id, consensus_config, network_handle, validator };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn new_worker(&self) -> eyre::Result<Worker<CDB, QuorumWaiter>> {
        let mut guard = self.internal.write().await;
        guard.new_worker().await
    }

    /// Return the workers network handle.
    pub async fn network_handle(&self) -> WorkerNetworkHandle {
        let guard = self.internal.read().await;
        guard.network_handle.clone()
    }

    /// Return the worker id.
    pub async fn id(&self) -> WorkerId {
        let guard = self.internal.read().await;
        guard.id
    }
}

/// Helper method to create a worker's task manager name by id.
pub fn worker_task_manager_name(id: WorkerId) -> String {
    format!("{WORKER_TASK_BASE} - {id}")
}
