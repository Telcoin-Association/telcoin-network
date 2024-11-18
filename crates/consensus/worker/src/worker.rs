// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    block_fetcher::WorkerBlockFetcher,
    block_provider::BlockProvider,
    metrics::{Metrics, WorkerMetrics},
    network::{PrimaryReceiverHandler, WorkerReceiverHandler},
    quorum_waiter::QuorumWaiter,
};
use anemo::{
    codegen::InboundRequestLayer,
    types::{Address, PeerInfo},
    Network, PeerId,
};
use anemo_tower::{
    auth::{AllowedPeers, RequireAuthorizationLayer},
    callback::CallbackLayer,
    rate_limit,
    set_header::{SetRequestHeaderLayer, SetResponseHeaderLayer},
    trace::{DefaultMakeSpan, DefaultOnFailure, TraceLayer},
};
use consensus_metrics::spawn_logged_monitored_task;
use consensus_network::{
    epoch_filter::{AllowedEpoch, EPOCH_HEADER_KEY},
    failpoints::FailpointsMakeCallbackHandler,
    local::LocalNetwork,
    metrics::MetricsMakeCallbackHandler,
};
use consensus_network_types::WorkerToWorkerServer;
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc};
use tn_block_validator::BlockValidation;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    traits::KeyPair as _, AuthorityIdentifier, Multiaddr, NetworkPublicKey, Noticer, Protocol,
    WorkerId,
};
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tracing::{error, info};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The main worker struct that holds all information needed for worker.
pub struct Worker<DB> {
    /// The id of this worker used for index-based lookup by other NW nodes.
    _id: WorkerId,
    /// Configuration for the worker.
    _consensus_config: ConsensusConfig<DB>,
    /// The worker's WAN.
    network: Network,
}

impl<DB: Database> Worker<DB> {
    /// Retrieve the worker's network address by id.
    fn worker_address(id: &WorkerId, consensus_config: &ConsensusConfig<DB>) -> Multiaddr {
        let address = consensus_config
            .worker_cache()
            .worker(consensus_config.authority().protocol_key(), id)
            .expect("Our public key or worker id is not in the worker cache")
            .worker_address;
        if let Some(addr) =
            address.replace(0, |_protocol| Some(Protocol::Ip4(Ipv4Addr::UNSPECIFIED)))
        {
            addr
        } else {
            address
        }
    }

    /// Spawn the worker.
    ///
    /// Create an instance of `Self` and start all tasks to participate in consensus.
    pub fn spawn(
        id: WorkerId,
        validator: impl BlockValidation,
        metrics: Metrics,
        consensus_config: ConsensusConfig<DB>,
    ) -> (Self, Vec<JoinHandle<()>>, BlockProvider<DB, QuorumWaiter>) {
        let worker_name = consensus_config.key_config().worker_network_public_key();
        let worker_peer_id = PeerId(worker_name.0.to_bytes());
        info!("Boot worker node with id {} peer id {}", id, worker_peer_id,);

        let node_metrics = metrics.worker_metrics.clone();

        // Receive incoming messages from other workers.
        let network = Self::start_network(id, &consensus_config, validator.clone(), &metrics);

        let block_fetcher = WorkerBlockFetcher::new(
            worker_name,
            network.clone(),
            consensus_config.node_storage().batch_store.clone(),
            node_metrics.clone(),
        );
        consensus_config.local_network().set_primary_to_worker_local_handler(
            worker_peer_id,
            Arc::new(PrimaryReceiverHandler {
                id,
                committee: consensus_config.committee().clone(),
                worker_cache: consensus_config.worker_cache().clone(),
                store: consensus_config.database().clone(),
                request_batches_timeout: consensus_config.parameters().sync_retry_delay,
                network: Some(network.clone()),
                batch_fetcher: Some(block_fetcher),
                validator,
            }),
        );

        let mut peer_types = HashMap::new();

        let other_workers = consensus_config
            .worker_cache()
            .others_workers_by_id(consensus_config.authority().protocol_key(), &id)
            .into_iter()
            .map(|(_, info)| (info.name, info.worker_address));

        // Add other workers we want to talk with to the known peers set.
        for (public_key, address) in other_workers {
            let (peer_id, address) = Self::add_peer_in_network(&network, public_key, &address);
            peer_types.insert(peer_id, "other_worker".to_string());
            info!(target: "worker::worker", "Adding others workers with peer id {} and address {}", peer_id, address);
        }

        // Connect worker to its corresponding primary.
        let (peer_id, address) = Self::add_peer_in_network(
            &network,
            consensus_config.authority().network_key(),
            consensus_config.authority().primary_network_address(),
        );
        peer_types.insert(peer_id, "our_primary".to_string());
        info!(target: "worker::worker", "Adding our primary with peer id {} and address {}", peer_id, address);

        // update the peer_types with the "other_primary". We do not add them in the Network
        // struct, otherwise the networking library will try to connect to it
        let other_primaries: Vec<(AuthorityIdentifier, Multiaddr, NetworkPublicKey)> =
            consensus_config.committee().others_primaries_by_id(consensus_config.authority().id());
        for (_, _, network_key) in other_primaries {
            peer_types.insert(PeerId(network_key.0.to_bytes()), "other_primary".to_string());
        }

        let (connection_monitor_handle, _) =
            consensus_network::connectivity::ConnectionMonitor::spawn(
                network.downgrade(),
                metrics.network_connection_metrics.clone(),
                peer_types,
                consensus_config.subscribe_shutdown(),
            );

        // TODO: revisit this soon
        let network_admin_server_base_port = consensus_config
            .parameters()
            .network_admin_server
            .worker_network_admin_server_base_port
            .checked_add(id)
            .unwrap();
        info!(target: "worker::worker",
            "Worker {} listening to network admin messages on 127.0.0.1:{}",
            id, network_admin_server_base_port
        );

        let admin_handles = consensus_network::admin::start_admin_server(
            network_admin_server_base_port,
            network.clone(),
            consensus_config.subscribe_shutdown(),
        );

        let block_provider = Self::new_block_provider(
            id,
            &consensus_config,
            node_metrics,
            consensus_config.local_network().clone(),
            network.clone(),
        );

        let network_shutdown_handle =
            Self::shutdown_network_listener(consensus_config.subscribe_shutdown(), network.clone());

        // NOTE: This log entry is used to compute performance.
        info!(target: "worker::worker",
            "Worker {} successfully booted on {}",
            id,
            consensus_config
                .worker_cache()
                .worker(consensus_config.authority().protocol_key(), &id)
                .expect("Our public key or worker id is not in the worker cache")
                .transactions
        );

        let mut handles = vec![connection_monitor_handle, network_shutdown_handle];
        handles.extend(admin_handles);
        (Self { _id: id, _consensus_config: consensus_config, network }, handles, block_provider)
    }

    /// Start the anemo network for the primary.
    fn start_network(
        id: WorkerId,
        consensus_config: &ConsensusConfig<DB>,
        validator: impl BlockValidation,
        metrics: &Metrics,
    ) -> Network {
        let mut worker_service = WorkerToWorkerServer::new(WorkerReceiverHandler {
            id,
            client: consensus_config.local_network().clone(),
            store: consensus_config.node_storage().batch_store.clone(),
            validator,
        });

        // Apply rate limits from configuration as needed.
        if let Some(limit) = consensus_config.config().parameters.anemo.report_batch_rate_limit {
            worker_service = worker_service.add_layer_for_report_block(InboundRequestLayer::new(
                rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                ),
            ));
        }
        if let Some(limit) = consensus_config.config().parameters.anemo.request_batches_rate_limit {
            worker_service = worker_service.add_layer_for_request_blocks(InboundRequestLayer::new(
                rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                ),
            ));
        }

        let address = Self::worker_address(&id, consensus_config);
        let addr = address.to_anemo_address().unwrap();
        let epoch_string: String = consensus_config.committee().epoch().to_string();
        let worker_peer_ids = consensus_config
            .worker_cache()
            .all_workers()
            .into_iter()
            .map(|(worker_name, _)| PeerId(worker_name.0.to_bytes()));
        let routes = anemo::Router::new()
            .add_rpc_service(worker_service)
            .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(worker_peer_ids)))
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));
        // .merge(primary_to_worker_router);

        let service = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_server_errors()
                    .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN)),
            )
            .layer(CallbackLayer::new(MetricsMakeCallbackHandler::new(
                metrics.inbound_network_metrics.clone(),
                consensus_config.config().parameters.anemo.excessive_message_size(),
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
                consensus_config.config().parameters.anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetRequestHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string,
            ))
            .into_inner();

        let anemo_config = consensus_config.anemo_config();

        let network = anemo::Network::bind(addr.clone())
            .server_name("telcoin-network")
            .private_key(
                consensus_config
                    .key_config()
                    .worker_network_keypair()
                    .copy()
                    .private()
                    .0
                    .to_bytes(),
            )
            .config(anemo_config.clone())
            .outbound_request_layer(outbound_layer.clone())
            .start(service.clone())
            .expect("worker network bind");

        info!(target: "worker::worker", "Worker {} listening to worker messages on {}", id, address);
        network
    }

    /// Spawns a task responsible for explicitly shutting down the network
    /// when a shutdown signal has been sent to the node.
    fn shutdown_network_listener(rx_shutdown: Noticer, network: Network) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            async move {
                rx_shutdown.await;
                if let Err(e) = network.shutdown().await {
                    error!(target: "worker::worker", "Error while shutting down network: {e}");
                }
                info!(target: "worker::worker", "Worker network server shutdown");
            },
            "WorkerShutdownNetworkListenerTask"
        )
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

    /// Builds a new block provider responsible for handling client transactions.
    fn new_block_provider(
        id: WorkerId,
        consensus_config: &ConsensusConfig<DB>,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        network: anemo::Network,
    ) -> BlockProvider<DB, QuorumWaiter> {
        info!(target: "worker::worker", "Starting handler for transactions");

        // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the block
        // before forwarding the block to the `Processor`
        let quorum_waiter = QuorumWaiter::new(
            consensus_config.authority().clone(),
            id,
            consensus_config.committee().clone(),
            consensus_config.worker_cache().clone(),
            network,
            node_metrics.clone(),
        );

        BlockProvider::new(
            id,
            quorum_waiter,
            node_metrics,
            client,
            consensus_config.database().clone(),
            consensus_config.parameters().worker_block_vote_timeout,
        )
    }

    /// Return an owned copy of the WAN.
    pub fn network(&self) -> Network {
        self.network.clone()
    }
}
