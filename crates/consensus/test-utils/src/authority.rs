// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Authority fixture for the cluster

use crate::{primary::PrimaryNodeDetails, worker::WorkerNodeDetails};
use fastcrypto::traits::KeyPair as _;
use jsonrpsee::http_client::HttpClient;
use narwhal_network::client::NetworkClient;
use reth_db::{test_utils::TempDatabase, DatabaseEnv};
use reth_node_ethereum::EthExecutorProvider;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tn_node::engine::ExecutionNode;
use tn_types::{
    AuthorityIdentifier, BlsKeypair, BlsPublicKey, Committee, ConsensusOutput, Multiaddr,
    NetworkKeypair, Parameters, WorkerCache, WorkerId,
};
use tokio::sync::{broadcast, RwLock, RwLockWriteGuard};
use tracing::info;

/// The authority details hold all the necessary structs and details
/// to identify and manage a specific authority. An authority is
/// composed of its primary node and the worker nodes. Via this struct
/// we can manage the nodes one by one or in batch fashion (ex stop_all).
/// The Authority can be cloned and reused across the instances as its
/// internals are thread safe. So changes made from one instance will be
/// reflected to another.
#[allow(dead_code)]
#[derive(Clone)]
pub struct AuthorityDetails {
    pub id: usize,
    pub name: AuthorityIdentifier,
    pub public_key: BlsPublicKey,
    internal: Arc<RwLock<AuthorityDetailsInternal>>,
}

/// Inner type for authority's details.
struct AuthorityDetailsInternal {
    client: Option<NetworkClient>,
    primary: PrimaryNodeDetails,
    worker_keypairs: Vec<NetworkKeypair>,
    workers: HashMap<WorkerId, WorkerNodeDetails>,
    execution: ExecutionNode<Arc<TempDatabase<DatabaseEnv>>, EthExecutorProvider>,
}

#[allow(clippy::arc_with_non_send_sync, clippy::too_many_arguments)]
impl AuthorityDetails {
    pub fn new(
        id: usize,
        name: AuthorityIdentifier,
        key_pair: BlsKeypair,
        network_key_pair: NetworkKeypair,
        worker_keypairs: Vec<NetworkKeypair>,
        parameters: Parameters,
        committee: Committee,
        worker_cache: WorkerCache,
        execution: ExecutionNode<Arc<TempDatabase<DatabaseEnv>>, EthExecutorProvider>,
    ) -> Self {
        // Create all the nodes we have in the committee
        let public_key = key_pair.public().clone();

        let primary = PrimaryNodeDetails::new(
            id,
            name,
            key_pair,
            network_key_pair,
            parameters.clone(),
            committee.clone(),
            worker_cache.clone(),
        );

        // Create all the workers - even if we don't intend to start them all. Those
        // act as place holder setups. That gives us the power in a clear way manage
        // the nodes independently.
        let mut workers = HashMap::new();
        for (worker_id, addresses) in worker_cache.workers.get(&public_key).unwrap().0.clone() {
            let worker = WorkerNodeDetails::new(
                worker_id,
                name,
                public_key.clone(),
                parameters.clone(),
                addresses.transactions.clone(),
                committee.clone(),
                worker_cache.clone(),
            );
            workers.insert(worker_id, worker);
        }

        let internal =
            AuthorityDetailsInternal { client: None, primary, worker_keypairs, workers, execution };

        Self { id, public_key, name, internal: Arc::new(RwLock::new(internal)) }
    }

    pub async fn client(&self) -> NetworkClient {
        let internal = self.internal.read().await;
        internal
            .client
            .as_ref()
            .expect("Requested network client which has not been initialised yet")
            .clone()
    }

    /// Starts the node's primary and workers. If the num_of_workers is provided
    /// then only those ones will be started. Otherwise all the available workers
    /// will be started instead.
    ///
    /// If the preserve_store value is true then the previous node's storage
    /// will be preserved. If false then the node will  start with a fresh
    /// (empty) storage.
    ///
    /// When a worker/primary is started, the authority's [ExecutionNode] is used
    /// to construct the necessary components.
    pub async fn start(
        &self,
        preserve_store: bool,
        num_of_workers: Option<usize>,
    ) -> eyre::Result<()> {
        self.start_primary(preserve_store).await?;

        let workers_to_start;
        {
            let internal = self.internal.read().await;
            workers_to_start = num_of_workers.unwrap_or(internal.workers.len());
        }

        for id in 0..workers_to_start {
            self.start_worker(id as WorkerId, preserve_store).await?;
        }

        Ok(())
    }

    /// Starts the primary node. If the preserve_store value is true then the
    /// previous node's storage will be preserved. If false then the node will
    /// start with a fresh (empty) storage.
    pub async fn start_primary(&self, preserve_store: bool) -> eyre::Result<()> {
        let mut internal = self.internal.write().await;
        let client = self.create_network_client(&mut internal).await;

        let execution_components = internal.execution.clone();

        internal.primary.start(client, preserve_store, &execution_components).await
    }

    pub async fn stop_primary(&self) {
        let internal = self.internal.read().await;

        internal.primary.stop().await;

        // TODO: spawned with task executor
        // either implement with TaskManager or setup kill signal
        // internal.execution.shutdown_engine().await;
    }

    pub async fn start_all_workers(&self, preserve_store: bool) -> eyre::Result<()> {
        let mut internal = self.internal.write().await;
        let client = self.create_network_client(&mut internal).await;

        let worker_keypairs =
            internal.worker_keypairs.iter().map(|kp| kp.copy()).collect::<Vec<NetworkKeypair>>();

        let execution_engine = internal.execution.clone();

        for (id, worker) in internal.workers.iter_mut() {
            let keypair = worker_keypairs.get(*id as usize).unwrap().copy();
            worker.start(keypair, client.clone(), preserve_store, &execution_engine).await?;
        }

        Ok(())
    }

    /// Starts the worker node by the provided id. If worker is not found then
    /// a panic is raised. If the preserve_store value is true then the
    /// previous node's storage will be preserved. If false then the node will
    /// start with a fresh (empty) storage.
    pub async fn start_worker(&self, id: WorkerId, preserve_store: bool) -> eyre::Result<()> {
        let mut internal = self.internal.write().await;
        let client = self.create_network_client(&mut internal).await;
        let execution_engine = internal.execution.clone();

        let keypair = internal.worker_keypairs.get(id as usize).unwrap().copy();
        let worker = internal
            .workers
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id));

        worker.start(keypair, client, preserve_store, &execution_engine).await
    }

    pub async fn stop_worker(&self, id: WorkerId) {
        let internal = self.internal.read().await;

        internal
            .workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .stop()
            .await;

        // only log errors for now
        // TODO: these are only spawned with TaskExecutor for now
        // if let Err(e) = internal.execution.shutdown_worker(&id).await {
        //     error!(?e);
        // }
    }

    /// Stops all the nodes (primary & workers).
    pub async fn stop_all(&self) {
        let mut internal = self.internal.write().await;

        if let Some(client) = internal.client.as_ref() {
            client.shutdown();
        }
        internal.client = None;

        internal.primary.stop().await;
        info!("{} - primary stopped", self.name);
        for (worker_id, worker) in internal.workers.iter() {
            worker.stop().await;
            info!("{} - worker {worker_id:} shut down", self.name);
        }

        // TODO: should this be shutdown between primary and worker?
        // internal.execution.shutdown_all().await;
        // info!("{} - execution node shutdown for authority", self.name);
    }

    /// Will restart the node with the current setup that has been chosen
    /// (ex same number of nodes).
    /// `preserve_store`: if true then the same storage will be used for the
    /// node
    /// `delay`: before starting again we'll wait for that long. If zero provided
    /// then won't wait at all
    pub async fn restart(&self, preserve_store: bool, delay: Duration) -> eyre::Result<()> {
        let num_of_workers = self.workers().await.len();

        self.stop_all().await;

        tokio::time::sleep(delay).await;

        // now start again the node with the same workers
        self.start(preserve_store, Some(num_of_workers)).await
    }

    /// Returns the current primary node running as a clone. If the primary
    /// node stops and starts again and it's needed by the user then this
    /// method should be called again to get the latest one.
    pub async fn primary(&self) -> PrimaryNodeDetails {
        let internal = self.internal.read().await;

        internal.primary.clone()
    }

    /// Returns the worker with the provided id. If not found then a panic
    /// is raised instead. If the worker is stopped and started again then
    /// the worker will need to be fetched again via this method.
    pub async fn worker(&self, id: WorkerId) -> WorkerNodeDetails {
        let internal = self.internal.read().await;

        internal
            .workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .clone()
    }

    /// Return the current execution node running. If the authority restarts, this
    /// method should be called again to ensure the latest reference is used.
    pub async fn execution_components(
        &self,
    ) -> eyre::Result<ExecutionNode<Arc<TempDatabase<DatabaseEnv>>, EthExecutorProvider>> {
        let internal = self.internal.read().await;
        Ok(internal.execution.clone())
    }

    /// Helper method to return transaction addresses of
    /// all the worker nodes.
    ///
    /// Important: only the addresses of the running workers will
    /// be returned.
    pub async fn worker_transaction_addresses(&self) -> Vec<Multiaddr> {
        self.workers().await.iter().map(|w| w.transactions_address.clone()).collect()
    }

    /// Returns all the running workers
    async fn workers(&self) -> Vec<WorkerNodeDetails> {
        let internal = self.internal.read().await;
        let mut workers = Vec::new();

        for worker in internal.workers.values() {
            if worker.is_running().await {
                workers.push(worker.clone());
            }
        }

        workers
    }

    /// This method returns a new client to send transactions to the dictated
    /// worker identified by the `worker_id`. If the worker_id is not found then
    /// an error is returned.
    pub async fn new_transactions_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<HttpClient>> {
        let internal = self.internal.read().await;
        let client = internal.execution.worker_http_client(worker_id).await?;
        Ok(client)
    }

    /// This method will return true either when the primary or any of
    /// the workers is running. In order to make sure that we don't end up
    /// in intermediate states we want to make sure that everything has
    /// stopped before we report something as not running (in case we want
    /// to start them again).
    pub async fn is_running(&self) -> bool {
        let internal = self.internal.read().await;

        if internal.primary.is_running().await {
            return true;
        }

        // if internal.execution.engine_is_running().await {
        //     return true;
        // }

        // // TODO: this only works for one worker for now
        // if internal.execution.any_workers_running().await {
        //     return true;
        // }

        for (_, worker) in internal.workers.iter() {
            if worker.is_running().await {
                return true;
            }
        }

        false
    }

    /// Creates a new network client if there isn't one yet initialised.
    async fn create_network_client(
        &self,
        internal: &mut RwLockWriteGuard<'_, AuthorityDetailsInternal>,
    ) -> NetworkClient {
        if internal.client.is_none() {
            let client = NetworkClient::new_from_keypair(&internal.primary.network_key_pair);
            internal.client = Some(client);
        }
        internal.client.as_ref().unwrap().clone()
    }

    /// Subscribe to [ConsensusOutput] broadcast.
    ///
    /// NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
    pub async fn subscribe_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        let internal = self.internal.read().await;
        internal.primary.subscribe_consensus_output().await
    }
}
