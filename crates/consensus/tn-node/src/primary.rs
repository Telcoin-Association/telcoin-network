// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hierarchical type to hold tasks spawned for a worker in the network.
use crate::{
    engine::ExecutionNode, error::NodeError, metrics::new_registry, try_join_all, FuturesUnordered,
};
use anemo::PeerId;
use consensus_metrics::{
    metered_channel::{self, Sender},
    RegistryID, RegistryService,
};
use fastcrypto::traits::{KeyPair as _, VerifyingKey};
use narwhal_executor::{
    get_restored_consensus_output, Executor, ExecutorMetrics, SubscriberResult,
};
use narwhal_network::client::NetworkClient;
use narwhal_primary::{
    consensus::{
        Bullshark, ChannelMetrics, Consensus, ConsensusMetrics, ConsensusRound, LeaderSchedule,
    },
    Primary, PrimaryChannelMetrics, NUM_SHUTDOWN_RECEIVERS,
};
use narwhal_storage::NodeStorage;
use prometheus::{IntGauge, Registry};
use std::{sync::Arc, time::Instant};
use tn_config::Parameters;
use tn_types::{
    AuthorityIdentifier, BlsKeypair, BlsPublicKey, Certificate, ChainIdentifier, Committee,
    ConditionalBroadcastReceiver, ConsensusOutput, NetworkKeypair, PreSubscribedBroadcastSender,
    Round, WorkerCache, BAD_NODES_STAKE_THRESHOLD,
};
use tokio::{
    sync::{watch, RwLock},
    task::JoinHandle,
};
use tracing::{info, instrument};

struct PrimaryNodeInner {
    /// The configuration parameters.
    parameters: Parameters,
    /// A prometheus RegistryService to use for the metrics
    registry_service: RegistryService,
    /// The latest registry id & registry used for the node
    registry: Option<(RegistryID, Registry)>,
    /// The task handles created from primary
    handles: FuturesUnordered<JoinHandle<()>>,
    /// Keeping NetworkClient here for quicker shutdown.
    client: Option<NetworkClient>,
    /// The shutdown signal channel
    tx_shutdown: Option<PreSubscribedBroadcastSender>,
    /// Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
}

impl PrimaryNodeInner {
    /// The window where the schedule change takes place in consensus. It represents number
    /// of committed sub dags.
    /// TODO: move this to node properties
    const CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS: u64 = 300;

    /// Starts the primary node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(level = "info", skip_all)]
    async fn start(
        &mut self, // The private-public key pair of this authority.
        keypair: BlsKeypair,
        // The private-public network key pair of this authority.
        network_keypair: NetworkKeypair,
        // The committee information.
        committee: Committee,
        chain: ChainIdentifier,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's store
        // TODO: replace this by a path so the method can open and independent storage
        store: &NodeStorage,
        // // The state used by the client to execute transactions.
        // execution_state: State,

        // Execution components needed to spawn the EL Executor
        execution_components: &ExecutionNode,
    ) -> Result<(), NodeError>
where
        // State: ExecutionState + Send + Sync + 'static,
    {
        if self.is_running().await {
            return Err(NodeError::NodeAlreadyRunning);
        }

        self.own_peer_id = Some(PeerId(network_keypair.public().0.to_bytes()));

        // create a new registry
        let registry = new_registry()?;

        // create the channel to send the shutdown signal
        let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

        // this is what connects the EL and CL
        let executor_metrics = ExecutorMetrics::new(&registry);
        let (tx_notifier, rx_notifier) = metered_channel::channel(
            narwhal_primary::CHANNEL_CAPACITY,
            &executor_metrics.tx_notifier,
        );

        // spawn primary if not already running
        let primary_handles = Self::spawn_primary(
            keypair,
            network_keypair,
            committee,
            worker_cache,
            client,
            store,
            chain,
            self.parameters.clone(),
            &registry,
            &mut tx_shutdown,
            executor_metrics,
            tx_notifier,
        )
        .await?;

        // start engine
        execution_components.start_engine(rx_notifier).await?;

        // store the registry
        self.swap_registry(Some(registry));

        // now keep the handlers
        self.handles.clear();
        self.handles.extend(primary_handles);
        self.tx_shutdown = Some(tx_shutdown);

        Ok(())
    }

    /// Will shutdown the primary node and wait until the node has shutdown by waiting on the
    /// underlying components handles. If the node was not already running then the
    /// method will return immediately.
    #[instrument(level = "info", skip_all)]
    async fn shutdown(&mut self) {
        if !self.is_running().await {
            return;
        }

        // send the shutdown signal to the node
        let now = Instant::now();

        if let Some(c) = self.client.take() {
            c.shutdown();
        }

        if let Some(tx_shutdown) = self.tx_shutdown.as_ref() {
            tx_shutdown.send().expect("Couldn't send the shutdown signal to downstream components");
            self.tx_shutdown = None
        }

        // Now wait until handles have been completed
        try_join_all(&mut self.handles).await.unwrap();

        self.swap_registry(None);

        info!(
            "Narwhal primary shutdown is complete - took {} seconds",
            now.elapsed().as_secs_f64()
        );
    }

    // Helper method useful to wait on the execution of the primary node
    async fn wait(&mut self) {
        try_join_all(&mut self.handles).await.unwrap();
    }

    // If any of the underlying handles haven't still finished, then this method will return
    // true, otherwise false will returned instead.
    async fn is_running(&self) -> bool {
        self.handles.iter().any(|h| !h.is_finished())
    }

    /// Accepts an Option registry. If it's Some, then the new registry will be added in the
    /// registry service and the registry_id will be updated. Also, any previous registry will
    /// be removed. If None is passed, then the registry_id is updated to None and any old
    /// registry is removed from the RegistryService.
    fn swap_registry(&mut self, registry: Option<Registry>) {
        if let Some((registry_id, _registry)) = self.registry.as_ref() {
            self.registry_service.remove(*registry_id);
        }

        if let Some(registry) = registry {
            self.registry = Some((self.registry_service.add(registry.clone()), registry));
        } else {
            self.registry = None
        }
    }

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing
    /// transactions.
    pub async fn spawn_primary(
        // The private-public key pair of this authority.
        keypair: BlsKeypair,
        // The private-public network key pair of this authority.
        network_keypair: NetworkKeypair,
        // The committee information.
        committee: Committee,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's storage.
        store: &NodeStorage,
        chain: ChainIdentifier,
        // The configuration parameters.
        parameters: Parameters,
        // // The state used by the client to execute transactions.
        // execution_state: State,
        // A prometheus exporter Registry to use for the metrics
        registry: &Registry,
        // The channel to send the shutdown signal
        tx_shutdown: &mut PreSubscribedBroadcastSender,
        // The metrics for executor
        // Passing here bc the tx notifier is needed to create the metrics.
        executor_metrics: ExecutorMetrics,
        // Receiving half goes to the EL Executor
        tx_notifier: Sender<ConsensusOutput>,
    ) -> SubscriberResult<Vec<JoinHandle<()>>>
where
        // State: ExecutionState + Send + Sync + 'static,
    {
        // These gauge is porcelain: do not modify it without also modifying
        // `primary::metrics::PrimaryChannelMetrics::replace_registered_new_certificates_metric`
        // This hack avoids a cyclic dependency in the initialization of consensus and primary
        let new_certificates_counter = IntGauge::new(
            PrimaryChannelMetrics::NAME_NEW_CERTS,
            PrimaryChannelMetrics::DESC_NEW_CERTS,
        )
        .unwrap();
        let (tx_new_certificates, rx_new_certificates) =
            metered_channel::channel(narwhal_primary::CHANNEL_CAPACITY, &new_certificates_counter);

        let committed_certificates_counter = IntGauge::new(
            PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
            PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
        )
        .unwrap();
        let (tx_committed_certificates, rx_committed_certificates) = metered_channel::channel(
            narwhal_primary::CHANNEL_CAPACITY,
            &committed_certificates_counter,
        );

        // Compute the public key of this authority.
        let name = keypair.public().clone();

        // Figure out the id for this authority
        let authority = committee
            .authority_by_key(&name)
            .unwrap_or_else(|| panic!("Our node with key {:?} should be in committee", name));

        let mut handles = Vec::new();
        let (tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(0, 0));

        let (consensus_handles, leader_schedule) = Self::spawn_consensus(
            authority.id(),
            worker_cache.clone(),
            committee.clone(),
            client.clone(),
            store,
            parameters.clone(),
            // execution_state,
            tx_shutdown.subscribe_n(3),
            rx_new_certificates,
            tx_committed_certificates.clone(),
            tx_consensus_round_updates,
            registry,
            executor_metrics,
            tx_notifier,
        )
        .await?;
        handles.extend(consensus_handles);

        // TODO: the same set of variables are sent to primary, consensus and downstream
        // components. Consider using a holder struct to pass them around.

        // Spawn the primary.
        let primary_handles = Primary::spawn(
            authority.clone(),
            keypair,
            network_keypair,
            committee.clone(),
            worker_cache.clone(),
            chain,
            parameters.clone(),
            client,
            store.certificate_store.clone(),
            store.proposer_store.clone(),
            store.payload_store.clone(),
            store.vote_digest_store.clone(),
            tx_new_certificates,
            rx_committed_certificates,
            rx_consensus_round_updates,
            tx_shutdown,
            tx_committed_certificates,
            registry,
            leader_schedule,
        );
        handles.extend(primary_handles);

        Ok(handles)
    }

    /// Spawn the consensus core and the client executing transactions.
    ///
    /// Pass the sender channel for consensus output and executor metrics.
    ///
    /// TODO: Executor metrics is needed to create the metered channel. This
    /// could be done a better way, but bigger priorities right now.
    async fn spawn_consensus(
        authority_id: AuthorityIdentifier,
        worker_cache: WorkerCache,
        committee: Committee,
        client: NetworkClient,
        store: &NodeStorage,
        parameters: Parameters,
        // execution_state: State,
        mut shutdown_receivers: Vec<ConditionalBroadcastReceiver>,
        rx_new_certificates: metered_channel::Receiver<Certificate>,
        tx_committed_certificates: metered_channel::Sender<(Round, Vec<Certificate>)>,
        tx_consensus_round_updates: watch::Sender<ConsensusRound>,
        registry: &Registry,
        executor_metrics: ExecutorMetrics,
        tx_notifier: Sender<ConsensusOutput>,
    ) -> SubscriberResult<(Vec<JoinHandle<()>>, LeaderSchedule)>
    where
        BlsPublicKey: VerifyingKey,
        // State: ExecutionState + Send + Sync + 'static,
    {
        let consensus_metrics = Arc::new(ConsensusMetrics::new(registry));
        let channel_metrics = ChannelMetrics::new(registry);

        let (tx_sequence, rx_sequence) = metered_channel::channel(
            narwhal_primary::CHANNEL_CAPACITY,
            &channel_metrics.tx_sequence,
        );

        // TODO: this should read from execution db
        //
        // can we still ensure last sub dag index + 1 is correct?

        // Check for any sub-dags that have been sent by consensus but were not processed by the
        // executor.
        let restored_consensus_output = get_restored_consensus_output(
            store.consensus_store.clone(),
            store.certificate_store.clone(),
            0, // TODO: this currently assumes the last finalized block number
        )
        .await?;

        let num_sub_dags = restored_consensus_output.len() as u64;
        if num_sub_dags > 0 {
            info!(
                "Consensus output on its way to the executor was restored for {num_sub_dags} sub-dags",
            );
        }
        consensus_metrics.recovered_consensus_output.inc_by(num_sub_dags);

        let leader_schedule =
            LeaderSchedule::from_store(committee.clone(), store.consensus_store.clone());

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            committee.clone(),
            store.consensus_store.clone(),
            consensus_metrics.clone(),
            Self::CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS,
            leader_schedule.clone(),
            BAD_NODES_STAKE_THRESHOLD,
        );
        let consensus_handle = Consensus::spawn(
            committee.clone(),
            parameters.gc_depth,
            store.consensus_store.clone(),
            store.certificate_store.clone(),
            shutdown_receivers.pop().unwrap(),
            rx_new_certificates,
            tx_committed_certificates,
            tx_consensus_round_updates,
            tx_sequence,
            ordering_engine,
            consensus_metrics.clone(),
        );

        // Spawn the client executing the transactions. It can also synchronize with the
        // subscriber handler if it missed some transactions.
        let executor_handle = Executor::spawn(
            authority_id,
            worker_cache,
            committee.clone(),
            client,
            // execution_state,
            shutdown_receivers.pop().unwrap(),
            rx_sequence,
            restored_consensus_output,
            tx_notifier,
            executor_metrics,
        )?;

        // let handles =
        //     executor_handles.into_iter().chain(std::iter::once(consensus_handles)).collect();
        let handles = vec![executor_handle, consensus_handle];

        Ok((handles, leader_schedule))
    }
}

#[derive(Clone)]
pub struct PrimaryNode {
    internal: Arc<RwLock<PrimaryNodeInner>>,
}

impl PrimaryNode {
    pub fn new(parameters: Parameters, registry_service: RegistryService) -> PrimaryNode {
        let inner = PrimaryNodeInner {
            parameters,
            registry_service,
            registry: None,
            handles: FuturesUnordered::new(),
            client: None,
            tx_shutdown: None,
            own_peer_id: None,
        };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start(
        &self,
        // The private-public key pair of this authority.
        keypair: BlsKeypair,
        // The private-public network key pair of this authority.
        network_keypair: NetworkKeypair,
        // The committee information.
        committee: Committee,
        chain: ChainIdentifier,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's store
        // TODO: replace this by a path so the method can open and independent storage
        store: &NodeStorage,
        // // The state used by the client to execute transactions.
        // execution_state: State,
        // Execution components needed to spawn the EL Executor
        execution_components: &ExecutionNode,
    ) -> Result<(), NodeError>
where
        // State: ExecutionState + Send + Sync + 'static,
    {
        let mut guard = self.internal.write().await;
        guard.client = Some(client.clone());
        guard
            .start(
                keypair,
                network_keypair,
                committee,
                chain,
                worker_cache,
                client,
                store,
                // execution_state,
                execution_components,
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

    pub async fn registry(&self) -> Option<(RegistryID, Registry)> {
        let guard = self.internal.read().await;
        guard.registry.clone()
    }
}
