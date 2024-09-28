// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::{engine::ExecutionNode, error::NodeError, try_join_all, FuturesUnordered};
use anemo::PeerId;
use consensus_metrics::metered_channel::channel_with_total;
use fastcrypto::traits::KeyPair;
use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use narwhal_typed_store::traits::Database as ConsensusDatabase;
use narwhal_worker::{
    metrics::{Metrics, WorkerChannelMetrics},
    Worker, CHANNEL_CAPACITY, NUM_SHUTDOWN_RECEIVERS,
};
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use std::{sync::Arc, time::Instant};
use tn_types::{
    BlsPublicKey, Committee, NetworkKeypair, Parameters, PreSubscribedBroadcastSender, WorkerCache,
    WorkerId,
};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{info, instrument};

pub struct WorkerNodeInner {
    // The worker's id
    id: WorkerId,
    // The configuration parameters.
    parameters: Parameters,
    // The task handles created from primary
    handles: FuturesUnordered<JoinHandle<()>>,
    // The shutdown signal channel
    tx_shutdown: Option<PreSubscribedBroadcastSender>,
    // Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
}

impl WorkerNodeInner {
    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "worker", skip_all)]
    async fn start<DB, Evm, CE, CDB>(
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

        let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

        let authority = committee.authority_by_key(&primary_name).unwrap_or_else(|| {
            panic!("Our node with key {:?} should be in committee", primary_name)
        });

        let metrics = Metrics::default();
        // For EL batch maker
        let channel_metrics: Arc<WorkerChannelMetrics> = metrics.channel_metrics.clone();
        let (tx_batch_maker, rx_batch_maker) = channel_with_total(
            CHANNEL_CAPACITY,
            &channel_metrics.tx_block_maker,
            &channel_metrics.tx_block_maker_total,
        );

        let batch_validator = execution_node.new_block_validator().await;

        let handles = Worker::spawn(
            authority.clone(),
            network_keypair,
            self.id,
            committee.clone(),
            worker_cache.clone(),
            self.parameters.clone(),
            batch_validator,
            client.clone(),
            store.batch_store.clone(),
            metrics,
            &mut tx_shutdown,
            channel_metrics,
            rx_batch_maker,
        );

        // spawn batch maker for worker
        execution_node.start_batch_maker(tx_batch_maker, self.id).await?;

        // now keep the handlers
        self.handles.clear();
        self.handles.extend(handles);
        self.tx_shutdown = Some(tx_shutdown);

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
        if let Some(tx_shutdown) = self.tx_shutdown.as_ref() {
            tx_shutdown.send().expect("Couldn't send the shutdown signal to downstream components");
            self.tx_shutdown = None;
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
pub struct WorkerNode {
    internal: Arc<RwLock<WorkerNodeInner>>,
}

impl WorkerNode {
    pub fn new(id: WorkerId, parameters: Parameters) -> WorkerNode {
        let inner = WorkerNodeInner {
            id,
            parameters,
            handles: FuturesUnordered::new(),
            tx_shutdown: None,
            own_peer_id: None,
        };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start<DB, Evm, CE, CDB>(
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
        CDB: ConsensusDatabase,
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
