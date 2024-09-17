// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::{ConsensusRound, LeaderSchedule},
    peer_handler::{PrimaryReceiverHandler, WorkerReceiverHandler},
    proposer::Proposer,
    state_handler::StateHandler,
    synchronizer::Synchronizer,
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
use consensus_metrics::metered_channel::{channel_with_total, Receiver, Sender};
use fastcrypto::{
    serde_helpers::ToFromByteArray,
    signature_service::SignatureService,
    traits::{KeyPair as _, ToFromBytes},
};
use narwhal_network::{
    client::NetworkClient,
    epoch_filter::{AllowedEpoch, EPOCH_HEADER_KEY},
    failpoints::FailpointsMakeCallbackHandler,
    metrics::MetricsMakeCallbackHandler,
};
use narwhal_primary_metrics::Metrics;
use narwhal_storage::{CertificateStore, PayloadStore, ProposerStore, VoteDigestStore};
use narwhal_typed_store::traits::Database;
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, thread::sleep, time::Duration};
use tn_types::{
    traits::EncodeDecodeBase64, Authority, BlsKeypair, ChainIdentifier, Committee, Multiaddr,
    NetworkKeypair, NetworkPublicKey, Parameters, Protocol, RandomnessPrivateKey, WorkerCache,
};

use narwhal_network_types::{PrimaryToPrimaryServer, WorkerToPrimaryServer};
use tn_types::{Certificate, PreSubscribedBroadcastSender, Round};
use tokio::{sync::watch, task::JoinHandle};
use tower::ServiceBuilder;
use tracing::{error, info};

#[cfg(test)]
#[path = "tests/primary_tests.rs"]
pub mod primary_tests;

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 10_000;

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 27;

pub struct Primary;

impl Primary {
    /// Spawns the primary and returns the JoinHandles of its tasks, as well as a metered receiver
    /// for the Consensus.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn<DB: Database>(
        authority: Authority,
        signer: BlsKeypair,
        network_signer: NetworkKeypair,
        committee: Committee,
        worker_cache: WorkerCache,
        chain_identifier: ChainIdentifier,
        parameters: Parameters,
        client: NetworkClient,
        certificate_store: CertificateStore<DB>,
        proposer_store: ProposerStore<DB>,
        payload_store: PayloadStore<DB>,
        vote_digest_store: VoteDigestStore<DB>,
        tx_new_certificates: Sender<Certificate>,
        rx_committed_certificates: Receiver<(Round, Vec<Certificate>)>,
        rx_consensus_round_updates: watch::Receiver<ConsensusRound>,
        tx_shutdown: &mut PreSubscribedBroadcastSender,
        leader_schedule: LeaderSchedule,
        metrics: &Metrics,
    ) -> Vec<JoinHandle<()>> {
        // Write the parameters to the logs.
        parameters.tracing();

        // Some info statements
        let own_peer_id = PeerId(network_signer.public().0.to_bytes());
        info!(
            "Boot primary node with peer id {} and public key {}",
            own_peer_id,
            authority.protocol_key().encode_base64(),
        );

        let (tx_our_digests, rx_our_digests) = channel_with_total(
            CHANNEL_CAPACITY,
            &metrics.primary_channel_metrics.tx_our_digests,
            &metrics.primary_channel_metrics.tx_our_digests_total,
        );
        let (tx_system_messages, rx_system_messages) = channel_with_total(
            CHANNEL_CAPACITY,
            &metrics.primary_channel_metrics.tx_system_messages,
            &metrics.primary_channel_metrics.tx_system_messages_total,
        );
        let (tx_parents, rx_parents) = channel_with_total(
            CHANNEL_CAPACITY,
            &metrics.primary_channel_metrics.tx_parents,
            &metrics.primary_channel_metrics.tx_parents_total,
        );
        let (tx_headers, rx_headers) = channel_with_total(
            CHANNEL_CAPACITY,
            &metrics.primary_channel_metrics.tx_headers,
            &metrics.primary_channel_metrics.tx_headers_total,
        );
        let (tx_certificate_fetcher, rx_certificate_fetcher) = channel_with_total(
            CHANNEL_CAPACITY,
            &metrics.primary_channel_metrics.tx_certificate_fetcher,
            &metrics.primary_channel_metrics.tx_certificate_fetcher_total,
        );
        let (tx_committed_own_headers, rx_committed_own_headers) = channel_with_total(
            CHANNEL_CAPACITY,
            &metrics.primary_channel_metrics.tx_committed_own_headers,
            &metrics.primary_channel_metrics.tx_committed_own_headers_total,
        );

        let (tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(0u64);

        let synchronizer = Arc::new(Synchronizer::new(
            authority.id(),
            committee.clone(),
            worker_cache.clone(),
            parameters.gc_depth,
            client.clone(),
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates.clone(),
            metrics.node_metrics.clone(),
            &metrics.primary_channel_metrics,
        ));

        // Convert authority private key into key used for random beacon.
        let randomness_private_key = fastcrypto::groups::bls12381::Scalar::from_byte_array(
            signer.copy().private().as_bytes().try_into().expect("key length should match"),
        )
        .expect("should work to convert BLS key to Scalar");
        let signature_service = SignatureService::new(signer);

        // Spawn the network receiver listening to messages from the other primaries.
        let address = authority.primary_network_address();
        let address =
            address.replace(0, |_protocol| Some(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))).unwrap();
        let mut primary_service = PrimaryToPrimaryServer::new(PrimaryReceiverHandler::new(
            authority.id(),
            committee.clone(),
            worker_cache.clone(),
            synchronizer.clone(),
            signature_service.clone(),
            certificate_store.clone(),
            vote_digest_store,
            rx_narwhal_round_updates,
            Default::default(),
            metrics.node_metrics.clone(),
        ))
        // Allow only one inflight RequestVote RPC at a time per peer.
        // This is required for correctness.
        .add_layer_for_request_vote(InboundRequestLayer::new(
            inflight_limit::InflightLimitLayer::new(1, inflight_limit::WaitMode::ReturnError),
        ))
        // Allow only one inflight FetchCertificates RPC at a time per peer.
        // These are already a batch request; an individual peer should never need more than one.
        .add_layer_for_fetch_certificates(InboundRequestLayer::new(
            inflight_limit::InflightLimitLayer::new(1, inflight_limit::WaitMode::ReturnError),
        ));

        // Apply other rate limits from configuration as needed.
        if let Some(limit) = parameters.anemo.send_certificate_rate_limit {
            primary_service = primary_service.add_layer_for_send_certificate(
                InboundRequestLayer::new(rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                )),
            );
        }

        let worker_receiver_handler = WorkerReceiverHandler::new(tx_our_digests, payload_store);

        client.set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler.clone()));

        let worker_service = WorkerToPrimaryServer::new(worker_receiver_handler);

        let addr = address.to_anemo_address().unwrap();

        let epoch_string: String = committee.epoch().to_string();

        let our_worker_peer_ids = worker_cache
            .our_workers(authority.protocol_key())
            .unwrap()
            .into_iter()
            .map(|worker_info| PeerId(worker_info.name.0.to_bytes()));
        let worker_to_primary_router = anemo::Router::new()
            .add_rpc_service(worker_service)
            // Add an Authorization Layer to ensure that we only service requests from our workers
            .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(our_worker_peer_ids)))
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));

        let primary_peer_ids =
            committee.authorities().map(|authority| PeerId(authority.network_key().0.to_bytes()));
        let routes = anemo::Router::new()
            .add_rpc_service(primary_service)
            .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(primary_peer_ids)))
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())))
            .merge(worker_to_primary_router);

        let service = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_server_errors()
                    .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN)),
            )
            .layer(CallbackLayer::new(MetricsMakeCallbackHandler::new(
                metrics.inbound_network_metrics.clone(),
                parameters.anemo.excessive_message_size(),
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
                metrics.outbound_network_metrics.clone(),
                parameters.anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetRequestHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string,
            ))
            .into_inner();

        let anemo_config = {
            let mut quic_config = anemo::QuicConfig::default();
            // Allow more concurrent streams for burst activity.
            quic_config.max_concurrent_bidi_streams = Some(10_000);
            // Increase send and receive buffer sizes on the primary, since the primary also
            // needs to fetch payloads.
            // With 200MiB buffer size and ~500ms RTT, the max throughput ~400MiB/s.
            quic_config.stream_receive_window = Some(100 << 20);
            quic_config.receive_window = Some(200 << 20);
            quic_config.send_window = Some(200 << 20);
            quic_config.crypto_buffer_size = Some(1 << 20);
            quic_config.socket_receive_buffer_size = Some(20 << 20);
            quic_config.socket_send_buffer_size = Some(20 << 20);
            quic_config.allow_failed_socket_buffer_size_setting = true;
            quic_config.max_idle_timeout_ms = Some(30_000);
            // Enable keep alives every 5s
            quic_config.keep_alive_interval_ms = Some(5_000);
            let mut config = anemo::Config::default();
            config.quic = Some(quic_config);
            // Set the max_frame_size to be 1 GB to work around the issue of there being too many
            // delegation events in the epoch change txn.
            config.max_frame_size = Some(1 << 30);
            // Set a default timeout of 300s for all RPC requests
            config.inbound_request_timeout_ms = Some(300_000);
            config.outbound_request_timeout_ms = Some(300_000);
            config.shutdown_idle_timeout_ms = Some(1_000);
            config.connectivity_check_interval_ms = Some(2_000);
            config.connection_backoff_ms = Some(1_000);
            config.max_connection_backoff_ms = Some(20_000);
            config
        };

        let network;
        let mut retries_left = 90;

        loop {
            let network_result = anemo::Network::bind(addr.clone())
                .server_name("narwhal")
                .private_key(network_signer.copy().private().0.to_bytes())
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
        client.set_primary_network(network.clone());

        info!("Primary {} listening on {}", authority.id(), address);

        let mut peer_types = HashMap::new();

        // Add my workers
        for worker in worker_cache.our_workers(authority.protocol_key()).unwrap() {
            let (peer_id, address) =
                Self::add_peer_in_network(&network, worker.name, &worker.worker_address);
            peer_types.insert(peer_id, "our_worker".to_string());
            info!("Adding our worker with peer id {} and address {}", peer_id, address);
        }

        // Add others workers
        for (_, worker) in worker_cache.others_workers(authority.protocol_key()) {
            let (peer_id, address) =
                Self::add_peer_in_network(&network, worker.name, &worker.worker_address);
            peer_types.insert(peer_id, "other_worker".to_string());
            info!("Adding others worker with peer id {} and address {}", peer_id, address);
        }

        // Add other primaries
        let primaries = committee
            .others_primaries_by_id(authority.id())
            .into_iter()
            .map(|(_, address, network_key)| (network_key, address));

        for (public_key, address) in primaries {
            let (peer_id, address) = Self::add_peer_in_network(&network, public_key, &address);
            peer_types.insert(peer_id, "other_primary".to_string());
            info!("Adding others primaries with peer id {} and address {}", peer_id, address);
        }

        let (connection_monitor_handle, _) =
            narwhal_network::connectivity::ConnectionMonitor::spawn(
                network.downgrade(),
                metrics.network_connection_metrics.clone(),
                peer_types,
                Some(tx_shutdown.subscribe()),
            );

        info!(
            "Primary {} listening to network admin messages on 127.0.0.1:{}",
            authority.id(),
            parameters.network_admin_server.primary_network_admin_server_port
        );

        let admin_handles = narwhal_network::admin::start_admin_server(
            parameters.network_admin_server.primary_network_admin_server_port,
            network.clone(),
            tx_shutdown.subscribe(),
        );

        let core_handle = Certifier::spawn(
            authority.id(),
            committee.clone(),
            certificate_store.clone(),
            synchronizer.clone(),
            signature_service,
            tx_shutdown.subscribe(),
            rx_headers,
            metrics.node_metrics.clone(),
            network.clone(),
        );

        // The `CertificateFetcher` waits to receive all the ancestors of a certificate before
        // looping it back to the `Synchronizer` for further processing.
        let certificate_fetcher_handle = CertificateFetcher::spawn(
            authority.id(),
            committee.clone(),
            network.clone(),
            certificate_store,
            rx_consensus_round_updates,
            tx_shutdown.subscribe(),
            rx_certificate_fetcher,
            synchronizer,
            metrics.node_metrics.clone(),
        );

        // When the `Synchronizer` collects enough parent certificates, the `Proposer` generates
        // a new header with new batch digests from our workers and sends it to the `Certifier`.
        let proposer_handle = Proposer::spawn(
            authority.id(),
            committee.clone(),
            proposer_store,
            parameters.header_num_of_batches_threshold,
            parameters.max_header_num_of_batches,
            parameters.max_header_delay,
            parameters.min_header_delay,
            None,
            tx_shutdown.subscribe(),
            rx_parents,
            rx_our_digests,
            rx_system_messages,
            tx_headers,
            tx_narwhal_round_updates,
            rx_committed_own_headers,
            metrics.node_metrics.clone(),
            leader_schedule,
        );

        let mut handles = vec![
            core_handle,
            certificate_fetcher_handle,
            proposer_handle,
            connection_monitor_handle,
        ];
        handles.extend(admin_handles);

        // Keeps track of the latest consensus round and allows other tasks to clean up their their
        // internal state
        let state_handler_handle = StateHandler::spawn(
            &chain_identifier,
            authority.id(),
            committee,
            rx_committed_certificates,
            tx_shutdown.subscribe(),
            Some(tx_committed_own_headers),
            tx_system_messages,
            RandomnessPrivateKey::from(randomness_private_key),
            network,
            // Some(800), // TODO: this value based on sui's current default. may not want this yet
            None,
        );
        handles.push(state_handler_handle);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            authority.id(),
            authority.primary_network_address()
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
