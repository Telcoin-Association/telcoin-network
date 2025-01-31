//! The Primary type

use crate::{
    anemo_network::{PrimaryReceiverHandler, WorkerReceiverHandler},
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::LeaderSchedule,
    proposer::Proposer,
    state_handler::StateHandler,
    synchronizer::Synchronizer,
    ConsensusBus,
};
use anemo::{
    codegen::InboundRequestLayer,
    types::{Address, PeerInfo},
    Network, PeerId,
};
use anemo_tower::{
    auth::{AllowedPeers, RequireAuthorizationLayer},
    callback::CallbackLayer,
    inflight_limit, rate_limit,
    set_header::{SetRequestHeaderLayer, SetResponseHeaderLayer},
    trace::{DefaultMakeSpan, DefaultOnFailure, TraceLayer},
};
use fastcrypto::traits::KeyPair as _;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tn_config::ConsensusConfig;
use tn_network::{
    epoch_filter::{AllowedEpoch, EPOCH_HEADER_KEY},
    failpoints::FailpointsMakeCallbackHandler,
    metrics::MetricsMakeCallbackHandler,
};
use tn_network_types::PrimaryToPrimaryServer;
use tn_storage::traits::Database;
use tn_types::{traits::EncodeDecodeBase64, Multiaddr, NetworkPublicKey, TaskManager};
use tower::ServiceBuilder;
use tracing::info;

#[cfg(test)]
#[path = "tests/primary_tests.rs"]
pub mod primary_tests;

pub struct Primary<DB> {
    /// The Primary's network.
    network: Network,
    synchronizer: Arc<Synchronizer<DB>>,
    peer_types: Option<HashMap<PeerId, String>>,
}

impl<DB: Database> Primary<DB> {
    pub fn new(config: ConsensusConfig<DB>, consensus_bus: &ConsensusBus) -> Self {
        // Write the parameters to the logs.
        config.parameters().tracing();

        // Some info statements
        let own_peer_id = PeerId(config.key_config().primary_network_public_key().0.to_bytes());
        info!(
            "Boot primary node with peer id {} and public key {}",
            own_peer_id,
            config.authority().protocol_key().encode_base64(),
        );

        let worker_receiver_handler = WorkerReceiverHandler::new(
            consensus_bus.clone(),
            config.node_storage().payload_store.clone(),
        );

        // TODO: remove this
        config
            .local_network()
            .set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler));

        let synchronizer = Arc::new(Synchronizer::new(config.clone(), consensus_bus));

        // TODO: REMOVE THIS
        let network = Self::start_network(&config, synchronizer.clone(), consensus_bus);

        let mut peer_types = HashMap::new();

        // Add other primaries
        let primaries = config
            .committee()
            .others_primaries_by_id(config.authority().id())
            .into_iter()
            .map(|(_, address, network_key)| (network_key, address));

        for (public_key, address) in primaries {
            let (peer_id, address) = Self::add_peer_in_network(&network, public_key, &address);
            peer_types.insert(peer_id, "other_primary".to_string());
            info!("Adding others primaries with peer id {} and address {}", peer_id, address);
        }

        // TODO: this is only needed to accurately return peer count from admin server and should
        // probably be removed - two tests fail - 1 primary and 1 worker
        // (peer count from admin server)
        //
        // Add my workers
        for worker in config
            .worker_cache()
            .our_workers(config.authority().protocol_key())
            .expect("own workers in worker cache")
        {
            let (peer_id, address) =
                Self::add_peer_in_network(&network, worker.name, &worker.worker_address);
            peer_types.insert(peer_id, "our_worker".to_string());
            info!("Adding our worker with peer id {} and address {}", peer_id, address);
        }

        // Add others workers
        for (_, worker) in config.worker_cache().others_workers(config.authority().protocol_key()) {
            let (peer_id, address) =
                Self::add_peer_in_network(&network, worker.name, &worker.worker_address);
            peer_types.insert(peer_id, "other_worker".to_string());
            info!("Adding others worker with peer id {} and address {}", peer_id, address);
        }
        Self { network, synchronizer, peer_types: Some(peer_types) }
    }

    /// Spawns the primary.
    pub fn spawn(
        &mut self,
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        leader_schedule: LeaderSchedule,
        task_manager: &TaskManager,
    ) {
        if consensus_bus.node_mode().borrow().is_active_cvv() {
            self.synchronizer.spawn(task_manager);
        }
        let _ = tn_network::connectivity::ConnectionMonitor::spawn(
            self.network.downgrade(),
            consensus_bus.primary_metrics().network_connection_metrics.clone(),
            self.peer_types.take().expect("peer types not set, was spawn called more than once?"),
            config.shutdown().subscribe(),
            task_manager,
        );

        info!(
            "Primary {} listening to network admin messages on 127.0.0.1:{}",
            config.authority().id(),
            config.parameters().network_admin_server.primary_network_admin_server_port
        );

        tn_network::admin::start_admin_server(
            config.parameters().network_admin_server.primary_network_admin_server_port,
            self.network.clone(),
            config.shutdown().subscribe(),
            task_manager,
        );

        Certifier::spawn(
            config.clone(),
            consensus_bus.clone(),
            self.synchronizer.clone(),
            self.network.clone(),
            task_manager,
        );

        // The `CertificateFetcher` waits to receive all the ancestors of a certificate before
        // looping it back to the `Synchronizer` for further processing.
        CertificateFetcher::spawn(
            config.authority().id(),
            config.committee().clone(),
            self.network.clone(),
            config.node_storage().certificate_store.clone(),
            consensus_bus.clone(),
            config.shutdown().subscribe(),
            self.synchronizer.clone(),
            task_manager,
        );

        // Only run the proposer task if we are a CVV.
        if consensus_bus.node_mode().borrow().is_cvv() {
            // When the `Synchronizer` collects enough parent certificates, the `Proposer` generates
            // a new header with new block digests from our workers and sends it to the `Certifier`.
            let proposer = Proposer::new(config.clone(), consensus_bus.clone(), leader_schedule);

            proposer.spawn(task_manager);
        }

        // Keeps track of the latest consensus round and allows other tasks to clean up their their
        // internal state
        StateHandler::spawn(
            config.authority().id(),
            consensus_bus,
            config.shutdown().subscribe(),
            self.network.clone(),
            task_manager,
        );

        // // store handle to network
        // self.network_handle = Some(network_handle);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            config.authority().id(),
            config.authority().primary_network_address()
        );
    }

    /// Start the anemo network for the primary.
    fn start_network(
        config: &ConsensusConfig<DB>,
        synchronizer: Arc<Synchronizer<DB>>,
        consensus_bus: &ConsensusBus,
    ) -> Network {
        // Spawn the network receiver listening to messages from the other primaries.
        let address = config.authority().primary_network_address();
        let mut primary_service = PrimaryToPrimaryServer::new(PrimaryReceiverHandler::new(
            config.clone(),
            synchronizer,
            consensus_bus.clone(),
            Default::default(),
        ))
        // Allow only one inflight RequestVote RPC at a time per peer.
        // This is required for correctness.
        .add_layer_for_request_vote(InboundRequestLayer::new(
            inflight_limit::InflightLimitLayer::new(1, inflight_limit::WaitMode::ReturnError),
        ))
        // Allow only one inflight FetchCertificates RPC at a time per peer.
        // These are already a block request; an individual peer should never need more than one.
        .add_layer_for_fetch_certificates(InboundRequestLayer::new(
            inflight_limit::InflightLimitLayer::new(1, inflight_limit::WaitMode::ReturnError),
        ));

        // Apply other rate limits from configuration as needed.
        if let Some(limit) = config.parameters().anemo.send_certificate_rate_limit {
            primary_service = primary_service.add_layer_for_send_certificate(
                InboundRequestLayer::new(rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                )),
            );
        }

        let addr = address.to_anemo_address().unwrap();

        let epoch_string: String = config.committee().epoch().to_string();

        let primary_peer_ids = config
            .committee()
            .authorities()
            .map(|authority| PeerId(authority.network_key().0.to_bytes()));
        let routes = anemo::Router::new()
            .add_rpc_service(primary_service)
            .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(primary_peer_ids)))
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));

        let service = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_server_errors()
                    .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN)),
            )
            .layer(CallbackLayer::new(MetricsMakeCallbackHandler::new(
                consensus_bus.primary_metrics().inbound_network_metrics.clone(),
                config.parameters().anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetResponseHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string.clone(),
            ))
            .service(routes);

        let outbound_layer = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_client_and_server_errors()
                    .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN)),
            )
            .layer(CallbackLayer::new(MetricsMakeCallbackHandler::new(
                consensus_bus.primary_metrics().outbound_network_metrics.clone(),
                config.parameters().anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetRequestHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string,
            ))
            .into_inner();

        let anemo_config = config.anemo_config();
        let mut tries = 0;
        let network = loop {
            match anemo::Network::bind(addr.clone())
                .server_name("telcoin-network")
                .private_key(
                    config.key_config().primary_network_keypair().copy().private().0.to_bytes(),
                )
                .config(anemo_config.clone())
                .outbound_request_layer(outbound_layer.clone())
                .start(service.clone())
            {
                Ok(network) => break network,
                Err(e) => {
                    // In case we are starting quickly (like in a test) allow three tries to bind to
                    // our address.
                    tries += 1;
                    if tries > 3 {
                        panic!("primary network bind to {addr} {e}");
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        };

        info!("Primary {} listening on {}", config.authority().id(), address);
        network
    }

    fn add_peer_in_network(
        network: &Network,
        peer_name: NetworkPublicKey,
        address: &Multiaddr,
    ) -> (PeerId, Address) {
        let peer_id = PeerId(peer_name.0.to_bytes());
        let address = address.to_anemo_address().unwrap();
        let peer_info = PeerInfo {
            peer_id,
            affinity: anemo::types::PeerAffinity::High,
            address: vec![address.clone()],
        };
        network.known_peers().insert(peer_info);

        (peer_id, address)
    }

    /// Return a reference to the Primary's network.
    pub fn network(&self) -> &Network {
        &self.network
    }
}
