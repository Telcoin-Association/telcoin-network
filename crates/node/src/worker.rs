//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::manager::WORKER_TASK_MANAGER_BASE;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_types::{BatchValidation, Database as ConsensusDatabase, TaskManager, WorkerId};
use tn_worker::{
    metrics::Metrics, new_worker, quorum_waiter::QuorumWaiter, Worker, WorkerNetworkHandle,
};
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
    async fn start(&mut self) -> eyre::Result<(TaskManager, Worker<CDB, QuorumWaiter>)> {
        let task_manager_name = worker_task_manager_name(self.id);
        let mut task_manager = TaskManager::new(task_manager_name);
        let metrics = Metrics::default();

        let batch_provider = new_worker(
            self.id,
            self.validator.clone(),
            metrics,
            self.consensus_config.clone(),
            self.network_handle.clone(),
            &mut task_manager,
        );

        Ok((task_manager, batch_provider))
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

    pub async fn start(&self) -> eyre::Result<(TaskManager, Worker<CDB, QuorumWaiter>)> {
        let mut guard = self.internal.write().await;
        guard.start().await
    }
}

/// Helper method to create a worker's task manager name by id.
pub fn worker_task_manager_name(id: WorkerId) -> String {
    format!("{WORKER_TASK_MANAGER_BASE} - {id}")
}
