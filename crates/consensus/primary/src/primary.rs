// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::LeaderSchedule,
    network::{PrimaryReceiverHandler, WorkerReceiverHandler},
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
use consensus_metrics::spawn_logged_monitored_task;
use consensus_network::{
    epoch_filter::{AllowedEpoch, EPOCH_HEADER_KEY},
    failpoints::FailpointsMakeCallbackHandler,
    metrics::MetricsMakeCallbackHandler,
};
use consensus_network_types::{PrimaryToPrimaryServer, WorkerToPrimaryServer};
use fastcrypto::traits::KeyPair as _;
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, thread::sleep, time::Duration};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{traits::EncodeDecodeBase64, Multiaddr, NetworkPublicKey, Protocol};
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tracing::{error, info};

#[cfg(test)]
#[path = "tests/primary_tests.rs"]
pub mod primary_tests;

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 10_000;

pub struct Primary;

impl Primary {
    /// Spawns the primary and returns the JoinHandles of its tasks, as well as a metered receiver
    /// for the Consensus.
    pub fn spawn<DB: Database>(
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        leader_schedule: LeaderSchedule,
    ) -> Vec<JoinHandle<()>> {
        // Write the parameters to the logs.
        config.parameters().tracing();

        // Some info statements
        let own_peer_id = PeerId(config.key_config().primary_network_public_key().0.to_bytes());
        info!(
            "Boot primary node with peer id {} and public key {}",
            own_peer_id,
            config.authority().protocol_key().encode_base64(),
        );

        let synchronizer = Arc::new(Synchronizer::new(config.clone(), consensus_bus));

        // Spawn the network receiver listening to messages from the other primaries.
        let address = config.authority().primary_network_address();
        let address =
            address.replace(0, |_protocol| Some(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))).unwrap();
        let mut primary_service = PrimaryToPrimaryServer::new(PrimaryReceiverHandler::new(
            config.clone(),
            synchronizer.clone(),
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

        let worker_receiver_handler = WorkerReceiverHandler::new(
            consensus_bus.clone(),
            config.node_storage().payload_store.clone(),
        );

        // TODO: remove this
        config
            .local_network()
            .set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler));

        // let worker_service = WorkerToPrimaryServer::new(worker_receiver_handler);

        let addr = address.to_anemo_address().unwrap();

        let epoch_string: String = config.committee().epoch().to_string();

        let our_worker_peer_ids = config
            .worker_cache()
            .our_workers(config.authority().protocol_key())
            .unwrap()
            .into_iter()
            .map(|worker_info| PeerId(worker_info.name.0.to_bytes()));
        // let worker_to_primary_router = anemo::Router::new()
        //     .add_rpc_service(worker_service)
        //     // Add an Authorization Layer to ensure that we only service requests from our workers
        //     .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(our_worker_peer_ids)))
        //     .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));

        let primary_peer_ids = config
            .committee()
            .authorities()
            .map(|authority| PeerId(authority.network_key().0.to_bytes()));
        let routes = anemo::Router::new()
            .add_rpc_service(primary_service)
            .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(primary_peer_ids)))
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));
        // .merge(worker_to_primary_router);

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
        let network;
        let mut retries_left = 90;
        loop {
            let network_result = anemo::Network::bind(addr.clone())
                .server_name("telcoin-network")
                .private_key(
                    config.key_config().primary_network_keypair().copy().private().0.to_bytes(),
                )
                .config(anemo_config.clone())
                .outbound_request_layer(outbound_layer.clone())
                .start(service.clone());
            match network_result {
                Ok(n) => {
                    network = n;
                    break;
                }
                Err(_) => {
                    retries_left -= 1;

                    if retries_left <= 0 {
                        panic!("Failed to initialize Network!");
                    }
                    error!(
                        "Address {} should be available for the primary Narwhal service, retrying in one second",
                        addr
                    );
                    sleep(Duration::from_secs(1));
                }
            }
        }

        // TODO: remove this
        config.local_network().set_primary_network(network.clone());

        info!("Primary {} listening on {}", config.authority().id(), address);

        let mut peer_types = HashMap::new();

        // Add my workers
        for worker in config.worker_cache().our_workers(config.authority().protocol_key()).unwrap()
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

        let (connection_monitor_handle, _) =
            consensus_network::connectivity::ConnectionMonitor::spawn(
                network.downgrade(),
                consensus_bus.primary_metrics().network_connection_metrics.clone(),
                peer_types,
                config.subscribe_shutdown(),
            );

        info!(
            "Primary {} listening to network admin messages on 127.0.0.1:{}",
            config.authority().id(),
            config.parameters().network_admin_server.primary_network_admin_server_port
        );

        let admin_handles = consensus_network::admin::start_admin_server(
            config.parameters().network_admin_server.primary_network_admin_server_port,
            network.clone(),
            config.subscribe_shutdown(),
        );

        let core_handle = Certifier::spawn(
            config.clone(),
            consensus_bus.clone(),
            synchronizer.clone(),
            network.clone(),
        );

        // The `CertificateFetcher` waits to receive all the ancestors of a certificate before
        // looping it back to the `Synchronizer` for further processing.
        let certificate_fetcher_handle = CertificateFetcher::spawn(
            config.authority().id(),
            config.committee().clone(),
            network.clone(),
            config.node_storage().certificate_store.clone(),
            consensus_bus.clone(),
            config.subscribe_shutdown(),
            synchronizer,
        );

        // When the `Synchronizer` collects enough parent certificates, the `Proposer` generates
        // a new header with new block digests from our workers and sends it to the `Certifier`.
        let proposer = Proposer::new(config.clone(), consensus_bus.clone(), None, leader_schedule);

        // TODO: include this with other handles
        let _proposer_handle = spawn_logged_monitored_task!(proposer, "ProposerTask");

        // TODO: all handles should return error
        //
        // can't include proposer handle yet
        let mut handles = vec![
            core_handle,
            certificate_fetcher_handle,
            // proposer_handle,
            connection_monitor_handle,
        ];
        handles.extend(admin_handles);

        // Keeps track of the latest consensus round and allows other tasks to clean up their their
        // internal state
        let state_handler_handle = StateHandler::spawn(
            config.authority().id(),
            consensus_bus,
            config.subscribe_shutdown(),
            network,
        );
        handles.push(state_handler_handle);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            config.authority().id(),
            config.authority().primary_network_address()
        );

        handles
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
}
