// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::{engine::ExecutionNode, error::NodeError, try_join_all, FuturesUnordered};
use anemo::PeerId;
use narwhal_worker::{metrics::Metrics, Worker};
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use std::{sync::Arc, time::Instant};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database as ConsensusDatabase;
use tn_types::WorkerId;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{info, instrument};

pub struct WorkerNodeInner<CDB> {
    // The worker's id
    id: WorkerId,
    // The consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    // The task handles created from primary
    handles: FuturesUnordered<JoinHandle<()>>,
    // Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
    // Keep the worker around.
    worker: Option<Worker<CDB>>,
}

impl<CDB: ConsensusDatabase> WorkerNodeInner<CDB> {
    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(name = "worker", skip_all)]
    async fn start<DB, Evm, CE>(
        &mut self,
        // used to create the batch maker process
        execution_node: &ExecutionNode<DB, Evm, CE>,
    ) -> eyre::Result<()>
    where
        DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
        Evm: BlockExecutorProvider + Clone + 'static,
        CE: ConfigureEvm,
        CDB: ConsensusDatabase,
    {
        if self.is_running().await {
            return Err(NodeError::NodeAlreadyRunning.into());
        }

        self.own_peer_id = Some(PeerId(
            self.consensus_config.key_config().primary_network_public_key().0.to_bytes(),
        ));

        let metrics = Metrics::default();

        let batch_validator = execution_node.new_block_validator().await;

        let worker = Worker::new(self.id, self.consensus_config.clone());

        let (handles, block_provider) =
            worker.spawn(batch_validator, metrics, self.consensus_config.clone());

        // spawn batch maker for worker
        execution_node.start_block_builder(self.id, block_provider.blocks_rx()).await?;

        // now keep the handlers
        self.handles.clear();
        self.handles.extend(handles);
        self.worker = Some(worker);

        Ok(())
    }

    /// Will shutdown the worker node and wait until the node has shutdown by waiting on the
    /// underlying components handles. If the node was not already running then the
    /// method will return immediately.
    #[instrument(level = "info", skip_all)]
    async fn shutdown(&mut self) {
        if !self.is_running().await {
            return;
        }

        let now = Instant::now();
        self.consensus_config.shutdown();

        // Now wait until handles have been completed
        try_join_all(&mut self.handles).await.unwrap();

        info!(
            "Narwhal worker {} shutdown is complete - took {} seconds",
            self.id,
            now.elapsed().as_secs_f64()
        );
    }

    /// If any of the underlying handles haven't still finished, then this method will return
    /// true, otherwise false will returned instead.
    async fn is_running(&self) -> bool {
        self.handles.iter().any(|h| !h.is_finished())
    }

    // Helper method useful to wait on the execution of the primary node
    async fn wait(&mut self) {
        try_join_all(&mut self.handles).await.unwrap();
    }
}

#[derive(Clone)]
pub struct WorkerNode<CDB> {
    internal: Arc<RwLock<WorkerNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> WorkerNode<CDB> {
    pub fn new(id: WorkerId, consensus_config: ConsensusConfig<CDB>) -> WorkerNode<CDB> {
        let inner = WorkerNodeInner {
            id,
            consensus_config,
            handles: FuturesUnordered::new(),
            own_peer_id: None,
            worker: None,
        };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start<DB, Evm, CE>(
        &self,
        // used to create the batch maker process
        execution_node: &ExecutionNode<DB, Evm, CE>,
    ) -> eyre::Result<()>
    where
        DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
        Evm: BlockExecutorProvider + Clone + 'static,
        CE: ConfigureEvm,
    {
        let mut guard = self.internal.write().await;
        guard.start(execution_node).await
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
}
