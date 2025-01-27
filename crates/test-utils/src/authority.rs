//! Authority fixture for the cluster

use crate::{
    primary::PrimaryNodeDetails, worker::WorkerNodeDetails, TelcoinTempDirs, TestExecutionNode,
    WorkerFixture,
};
use anemo::Network;
use fastcrypto::{hash::Hash, traits::KeyPair as _};
use jsonrpsee::http_client::HttpClient;
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};
use tn_config::{Config, ConsensusConfig, KeyConfig};
use tn_network::local::LocalNetwork;
use tn_storage::traits::Database;
use tn_types::Address;
use tn_types::{
    Authority, AuthorityIdentifier, BlsKeypair, BlsPublicKey, Certificate, Committee, Header,
    HeaderBuilder, Multiaddr, NetworkKeypair, NetworkPublicKey, Round, Vote, WorkerCache, WorkerId,
};
use tokio::sync::RwLock;

/// The authority details hold all the necessary structs and details
/// to identify and manage a specific authority.
///
/// An authority is composed of its primary node and the worker nodes. Via this struct
/// we can manage the nodes one by one or in batch fashion (ex stop_all).
/// The Authority can be cloned and reused across the instances as its
/// internals are thread safe. So changes made from one instance will be
/// reflected to another.
#[allow(dead_code)]
#[derive(Clone)]
pub struct AuthorityDetails<DB> {
    pub id: usize,
    pub name: AuthorityIdentifier,
    pub public_key: BlsPublicKey,
    internal: Arc<RwLock<AuthorityDetailsInternal<DB>>>,
}

/// Inner type for authority's details.
struct AuthorityDetailsInternal<DB> {
    client: Option<LocalNetwork>,
    primary: PrimaryNodeDetails<DB>,
    workers: HashMap<WorkerId, WorkerNodeDetails<DB>>,
    execution: TestExecutionNode,
}

impl<DB: Database> AuthorityDetails<DB> {
    pub fn new(
        id: usize,
        name: AuthorityIdentifier,
        consensus_config: ConsensusConfig<DB>,
        execution: TestExecutionNode,
    ) -> Self {
        // Create all the nodes we have in the committee
        let public_key = consensus_config.key_config().primary_public_key();

        let primary = PrimaryNodeDetails::new(id, name, consensus_config.clone());

        // Create all the workers - even if we don't intend to start them all. Those
        // act as place holder setups. That gives us the power in a clear way manage
        // the nodes independently.
        let mut workers = HashMap::new();
        for (worker_id, addresses) in
            consensus_config.worker_cache().workers.get(&public_key).unwrap().0.clone()
        {
            let worker = WorkerNodeDetails::new(
                worker_id,
                name,
                consensus_config.clone(),
                addresses.transactions.clone(),
            );
            workers.insert(worker_id, worker);
        }

        let internal = AuthorityDetailsInternal { client: None, primary, workers, execution };

        Self { id, public_key, name, internal: Arc::new(RwLock::new(internal)) }
    }

    pub async fn client(&self) -> LocalNetwork {
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
        self.start_primary().await?;

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
    pub async fn start_primary(&self) -> eyre::Result<()> {
        let mut internal = self.internal.write().await;

        internal.primary.start().await
    }

    pub async fn start_all_workers(&self, preserve_store: bool) -> eyre::Result<()> {
        let mut internal = self.internal.write().await;

        let execution_engine = internal.execution.clone();

        for (_id, worker) in internal.workers.iter_mut() {
            worker.start(preserve_store, &execution_engine).await?;
        }

        Ok(())
    }

    /// Starts the worker node by the provided id. If worker is not found then
    /// a panic is raised. If the preserve_store value is true then the
    /// previous node's storage will be preserved. If false then the node will
    /// start with a fresh (empty) storage.
    pub async fn start_worker(&self, id: WorkerId, preserve_store: bool) -> eyre::Result<()> {
        let mut internal = self.internal.write().await;
        let execution_engine = internal.execution.clone();

        let worker = internal
            .workers
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id));

        worker.start(preserve_store, &execution_engine).await
    }

    /// Returns the current primary node running as a clone. If the primary
    /// node stops and starts again and it's needed by the user then this
    /// method should be called again to get the latest one.
    pub async fn primary(&self) -> PrimaryNodeDetails<DB> {
        let internal = self.internal.read().await;

        internal.primary.clone()
    }

    /// Returns the worker with the provided id. If not found then a panic
    /// is raised instead. If the worker is stopped and started again then
    /// the worker will need to be fetched again via this method.
    pub async fn worker(&self, id: WorkerId) -> WorkerNodeDetails<DB> {
        let internal = self.internal.read().await;

        internal
            .workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .clone()
    }

    /// Return the current execution node running. If the authority restarts, this
    /// method should be called again to ensure the latest reference is used.
    pub async fn execution_components(&self) -> eyre::Result<TestExecutionNode> {
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

    /// Returns all the workers
    async fn workers(&self) -> Vec<WorkerNodeDetails<DB>> {
        let internal = self.internal.read().await;
        let mut workers = Vec::new();

        for worker in internal.workers.values() {
            workers.push(worker.clone());
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

    /// Returns an owned primary WAN if it exists.
    pub async fn primary_network(&self) -> Network {
        self.primary().await.network().await
    }

    /// Returns an owned worker WAN if it exists.
    pub async fn worker_network(&self, worker_id: WorkerId) -> Option<Network> {
        self.worker(worker_id).await.network().await
    }
}

/// Fixture representing an validator node within the network.
///
/// [AuthorityFixture] holds keypairs and should not be used in production.
#[derive(Debug)]
pub struct AuthorityFixture<DB> {
    /// Thread-safe cell with a reference to the [Authority] struct used in production.
    authority: Authority,
    /// All workers for this authority as a [WorkerFixture].
    worker: WorkerFixture,
    /// Config for this authority.
    consensus_config: ConsensusConfig<DB>,
    /// The testing primary key.
    primary_keypair: BlsKeypair,
}

impl<DB: Database> AuthorityFixture<DB> {
    /// The owned [AuthorityIdentifier] for the authority
    pub fn id(&self) -> AuthorityIdentifier {
        self.authority.id()
    }

    /// The [Authority] struct used in production.
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// The authority's bls12381 [KeyPair] used to sign consensus messages.
    pub fn keypair(&self) -> &BlsKeypair {
        &self.primary_keypair
    }

    /// The authority's ed25519 [NetworkKeypair] used to sign messages on the network.
    pub fn primary_network_keypair(&self) -> NetworkKeypair {
        self.consensus_config.key_config().primary_network_keypair().copy()
    }

    /// The authority's [Address] for execution layer.
    pub fn execution_address(&self) -> Address {
        self.authority.execution_address()
    }

    /// Create a new anemo network for consensus.
    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.authority.primary_network_address().to_anemo_address().unwrap())
            .server_name("tn-test")
            .private_key(self.primary_network_keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    /// A reference to the authority's [Multiaddr] on the consensus network.
    pub fn network_address(&self) -> &Multiaddr {
        self.authority.primary_network_address()
    }

    /// Return a reference to a [WorkerFixture] for this authority.
    pub fn worker(&self) -> &WorkerFixture {
        &self.worker
    }

    /// The authority's [PublicKey].
    pub fn primary_public_key(&self) -> BlsPublicKey {
        self.consensus_config.key_config().primary_public_key()
    }

    /// The authority's [NetworkPublicKey].
    pub fn primary_network_public_key(&self) -> NetworkPublicKey {
        self.consensus_config.key_config().primary_network_public_key()
    }

    /// Create a [Header] with a default payload based on the [Committee] argument.
    pub fn header(&self, committee: &Committee) -> Header {
        self.header_builder(committee).payload(Default::default()).build().unwrap()
    }

    /// Create a [Header] with a default payload based on the [Committee] and [Round] arguments.
    pub fn header_with_round(&self, committee: &Committee, round: Round) -> Header {
        self.header_builder(committee).payload(Default::default()).round(round).build().unwrap()
    }

    /// Return a [HeaderV1Builder] for round 1. The builder is constructed
    /// with a genesis certificate as the parent.
    pub fn header_builder(&self, committee: &Committee) -> HeaderBuilder {
        HeaderBuilder::default()
            .author(self.id())
            .round(1)
            .epoch(committee.epoch())
            .parents(Certificate::genesis(committee).iter().map(|x| x.digest()).collect())
    }

    /// Sign a [Header] and return a [Vote] with no additional validation.
    pub fn vote(&self, header: &Header) -> Vote {
        Vote::new_sync(header, &self.id(), self.consensus_config.key_config())
    }

    /// Return the consensus config.
    pub fn consensus_config(&self) -> ConsensusConfig<DB> {
        self.consensus_config.clone()
    }

    /// Generate a new [AuthorityFixture].
    pub(crate) fn generate<P>(
        number_of_workers: NonZeroUsize,
        mut get_port: P,
        authority: Authority,
        primary_keypair: BlsKeypair,
        key_config: KeyConfig,
        committee: Committee,
        db: DB,
    ) -> Self
    where
        P: FnMut(&str) -> u16,
    {
        // Make sure our keys are correct.
        assert_eq!(&key_config.primary_public_key(), authority.protocol_key());
        assert_eq!(key_config.primary_network_public_key(), authority.network_key());
        assert_eq!(primary_keypair.public(), &key_config.primary_public_key());
        // Currently only support one worker per node.
        // If/when this is relaxed then the key_config below will need to change.
        assert_eq!(number_of_workers.get(), 1);
        let mut config = Config::default();
        // These key updates don't return errors...
        let _ = config.update_protocol_key(key_config.primary_public_key());
        let _ = config.update_primary_network_key(key_config.primary_network_public_key());
        let _ = config.update_worker_network_key(key_config.worker_network_public_key());
        config.validator_info.primary_info.network_address =
            authority.primary_network_address().clone();

        let tn_datadirs = TelcoinTempDirs::default();
        let node_config = tn_node::NodeStorage::reopen(db);
        let consensus_config = ConsensusConfig::new_with_committee(
            config,
            tn_datadirs,
            node_config,
            key_config.clone(),
            committee,
            None,
        )
        .expect("failed to generate config!");

        let worker = WorkerFixture::generate(key_config.clone(), authority.id().0, &mut get_port);

        Self { authority, worker, consensus_config, primary_keypair }
    }

    pub(crate) fn set_worker_cache(&mut self, worker_cache: WorkerCache) {
        self.consensus_config.set_worker_cache(worker_cache);
    }
}
