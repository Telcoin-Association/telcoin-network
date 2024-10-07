// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::{engine::ExecutionNode, error::NodeError, try_join_all, FuturesUnordered};
use anemo::PeerId;
use fastcrypto::traits::KeyPair;
use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use narwhal_typed_store::traits::Database as ConsensusDatabase;
use narwhal_worker::{metrics::Metrics, Worker};
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use std::{sync::Arc, time::Instant};
use tn_types::{
    BlsPublicKey, Committee, NetworkKeypair, Notifier, Parameters, WorkerCache, WorkerId,
};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{info, instrument};

pub struct WorkerNodeInner<CDB: ConsensusDatabase> {
    // The worker's id
    id: WorkerId,
    // The configuration parameters.
    parameters: Parameters,
    // The task handles created from primary
    handles: FuturesUnordered<JoinHandle<()>>,
    // The shutdown signal channel
    tx_shutdown: Option<Notifier>,
    // Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
    // Keep the worker around.
    worker: Option<Worker<CDB>>,
}

impl<CDB: ConsensusDatabase> WorkerNodeInner<CDB> {
    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "worker", skip_all)]
    async fn start<DB, Evm, CE>(
        &mut self,
        // The primary's id
        primary_name: BlsPublicKey,
        // The private-public network key pair of this authority.
        network_keypair: NetworkKeypair,
        // The committee information.
        committee: Committee,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's store
        // TODO: replace this by a path so the method can open and independent storage
        store: &NodeStorage<CDB>,
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

        self.own_peer_id = Some(PeerId(network_keypair.public().0.to_bytes()));

        let mut tx_shutdown = Notifier::new();

        let authority = committee.authority_by_key(&primary_name).unwrap_or_else(|| {
            panic!("Our node with key {:?} should be in committee", primary_name)
        });

        let metrics = Metrics::default();

        let batch_validator = execution_node.new_block_validator().await;

        let worker = Worker::new(
            authority.clone(),
            network_keypair,
            self.id,
            committee.clone(),
            worker_cache.clone(),
            self.parameters.clone(),
            store.batch_store.clone(),
        );

        let (handles, block_provider) =
            worker.spawn(batch_validator, client.clone(), metrics, &mut tx_shutdown);

        // spawn batch maker for worker
        execution_node.start_block_builder(self.id, block_provider.blocks_rx()).await?;

        // now keep the handlers
        self.handles.clear();
        self.handles.extend(handles);
        self.tx_shutdown = Some(tx_shutdown);
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
        if let Some(mut tx_shutdown) = self.tx_shutdown.take() {
            tx_shutdown.notify();
        }

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
pub struct WorkerNode<CDB: ConsensusDatabase> {
    internal: Arc<RwLock<WorkerNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> WorkerNode<CDB> {
    pub fn new(id: WorkerId, parameters: Parameters) -> WorkerNode<CDB> {
        let inner = WorkerNodeInner {
            id,
            parameters,
            handles: FuturesUnordered::new(),
            tx_shutdown: None,
            own_peer_id: None,
            worker: None,
        };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start<DB, Evm, CE>(
        &self,
        // The primary's public key of this authority.
        primary_key: BlsPublicKey,
        // The private-public network key pair of this authority.
        network_keypair: NetworkKeypair,
        // The committee information.
        committee: Committee,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's store
        // TODO: replace this by a path so the method can open and independent storage
        store: &NodeStorage<CDB>,
        // used to create the batch maker process
        execution_node: &ExecutionNode<DB, Evm, CE>,
    ) -> eyre::Result<()>
    where
        DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
        Evm: BlockExecutorProvider + Clone + 'static,
        CE: ConfigureEvm,
    {
        let mut guard = self.internal.write().await;
        guard
            .start(
                primary_key,
                network_keypair,
                committee,
                worker_cache,
                client,
                store,
                execution_node,
            )
            .await
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
