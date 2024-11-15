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
use consensus_network_types::{PrimaryToWorkerServer, WorkerToWorkerServer};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, thread::sleep, time::Duration};
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
    id: WorkerId,
    consensus_config: ConsensusConfig<DB>,
}

impl<DB: Database> Worker<DB> {
    pub fn new(id: WorkerId, consensus_config: ConsensusConfig<DB>) -> Self {
        let worker_name = consensus_config.key_config().worker_network_public_key();
        let worker_peer_id = PeerId(worker_name.0.to_bytes());
        info!("Boot worker node with id {} peer id {}", id, worker_peer_id,);

        // Define a worker instance.
        Self { id, consensus_config }
    }

    fn my_address(&self) -> Multiaddr {
        let address = self
            .consensus_config
            .worker_cache()
            .worker(self.consensus_config.authority().protocol_key(), &self.id)
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

    pub fn spawn(
        &self,
        validator: impl BlockValidation,
        metrics: Metrics,
        config: ConsensusConfig<DB>,
    ) -> (Vec<JoinHandle<()>>, BlockProvider<DB, QuorumWaiter>) {
        let worker_name = self.consensus_config.key_config().worker_network_public_key();
        let worker_peer_id = PeerId(worker_name.0.to_bytes());
        info!("Boot worker node with id {} peer id {}", self.id, worker_peer_id,);

        let node_metrics = metrics.worker_metrics.clone();

        let mut worker_service = WorkerToWorkerServer::new(WorkerReceiverHandler {
            id: self.id,
            client: self.consensus_config.local_network().clone(),
            store: self.consensus_config.node_storage().batch_store.clone(),
            validator: validator.clone(),
        });
        // Apply rate limits from configuration as needed.
        if let Some(limit) = self.consensus_config.config().parameters.anemo.report_batch_rate_limit
        {
            worker_service = worker_service.add_layer_for_report_block(InboundRequestLayer::new(
                rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                ),
            ));
        }
        if let Some(limit) =
            self.consensus_config.config().parameters.anemo.request_batches_rate_limit
        {
            worker_service = worker_service.add_layer_for_request_blocks(InboundRequestLayer::new(
                rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                ),
            ));
        }

        // // Legacy RPC interface, only used by delete_batches() for external consensus.
        // let primary_service = PrimaryToWorkerServer::new(PrimaryReceiverHandler {
        //     id: self.id,
        //     committee: self.consensus_config.committee().clone(),
        //     worker_cache: self.consensus_config.worker_cache().clone(),
        //     store: self.consensus_config.node_storage().batch_store.clone(),
        //     request_batches_timeout: self.consensus_config.config().parameters.sync_retry_delay,
        //     network: None,
        //     batch_fetcher: None,
        //     validator: validator.clone(),
        // });

        // Receive incoming messages from other workers.
        let address = self.my_address();
        let addr = address.to_anemo_address().unwrap();

        let epoch_string: String = self.consensus_config.committee().epoch().to_string();

        // Set up anemo Network.
        let our_primary_peer_id =
            PeerId(self.consensus_config.authority().network_key().0.to_bytes());
        // let primary_to_worker_router = anemo::Router::new()
        //     .add_rpc_service(primary_service)
        //     // Add an Authorization Layer to ensure that we only service requests from our primary
        //     .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new([our_primary_peer_id])))
        //     .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));

        let worker_peer_ids = self
            .consensus_config
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
                self.consensus_config.config().parameters.anemo.excessive_message_size(),
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
                self.consensus_config.config().parameters.anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetRequestHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string,
            ))
            .into_inner();

        let anemo_config = self.consensus_config.anemo_config();

        let network;
        let mut retries_left = 90;
        loop {
            let network_result = anemo::Network::bind(addr.clone())
                .server_name("telcoin-network")
                .private_key(
                    self.consensus_config
                        .key_config()
                        .worker_network_keypair()
                        .copy()
                        .private()
                        .0
                        .to_bytes(),
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
                        panic!();
                    }
                    error!(target: "worker::worker",
                        "Address {} should be available for the primary Narwhal service, retrying in one second",
                        addr
                    );
                    sleep(Duration::from_secs(1));
                }
            }
        }
        self.consensus_config.local_network().set_worker_network(self.id, network.clone());

        info!(target: "worker::worker", "Worker {} listening to worker messages on {}", self.id, address);

        let block_fetcher = WorkerBlockFetcher::new(
            worker_name,
            network.clone(),
            self.consensus_config.node_storage().batch_store.clone(),
            node_metrics.clone(),
        );
        self.consensus_config.local_network().set_primary_to_worker_local_handler(
            worker_peer_id,
            Arc::new(PrimaryReceiverHandler {
                id: self.id,
                committee: self.consensus_config.committee().clone(),
                worker_cache: self.consensus_config.worker_cache().clone(),
                store: self.consensus_config.database().clone(),
                request_batches_timeout: self.consensus_config.parameters().sync_retry_delay,
                network: Some(network.clone()),
                batch_fetcher: Some(block_fetcher),
                validator: validator.clone(),
            }),
        );

        let mut peer_types = HashMap::new();

        let other_workers = self
            .consensus_config
            .worker_cache()
            .others_workers_by_id(self.consensus_config.authority().protocol_key(), &self.id)
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
            self.consensus_config.authority().network_key(),
            self.consensus_config.authority().primary_network_address(),
        );
        peer_types.insert(peer_id, "our_primary".to_string());
        info!(target: "worker::worker", "Adding our primary with peer id {} and address {}", peer_id, address);

        // update the peer_types with the "other_primary". We do not add them in the Network
        // struct, otherwise the networking library will try to connect to it
        let other_primaries: Vec<(AuthorityIdentifier, Multiaddr, NetworkPublicKey)> = self
            .consensus_config
            .committee()
            .others_primaries_by_id(self.consensus_config.authority().id());
        for (_, _, network_key) in other_primaries {
            peer_types.insert(PeerId(network_key.0.to_bytes()), "other_primary".to_string());
        }

        let (connection_monitor_handle, _) =
            consensus_network::connectivity::ConnectionMonitor::spawn(
                network.downgrade(),
                metrics.network_connection_metrics.clone(),
                peer_types,
                config.subscribe_shutdown(),
            );

        let network_admin_server_base_port = self
            .consensus_config
            .parameters()
            .network_admin_server
            .worker_network_admin_server_base_port
            .checked_add(self.id)
            .unwrap();
        info!(target: "worker::worker",
            "Worker {} listening to network admin messages on 127.0.0.1:{}",
            self.id, network_admin_server_base_port
        );

        let admin_handles = consensus_network::admin::start_admin_server(
            network_admin_server_base_port,
            network.clone(),
            config.subscribe_shutdown(),
        );

        let block_provider = self.new_block_provider(
            node_metrics,
            self.consensus_config.local_network().clone(),
            network.clone(),
        );

        let network_shutdown_handle =
            Self::shutdown_network_listener(config.subscribe_shutdown(), network);

        // NOTE: This log entry is used to compute performance.
        info!(target: "worker::worker",
            "Worker {} successfully booted on {}",
            self.id,
            self.consensus_config
                .worker_cache()
                .worker(self.consensus_config.authority().protocol_key(), &self.id)
                .expect("Our public key or worker id is not in the worker cache")
                .transactions
        );

        let mut handles = vec![connection_monitor_handle, network_shutdown_handle];
        handles.extend(admin_handles);
        (handles, block_provider)
    }

    // Spawns a task responsible for explicitly shutting down the network
    // when a shutdown signal has been sent to the node.
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
        &self,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        network: anemo::Network,
    ) -> BlockProvider<DB, QuorumWaiter> {
        info!(target: "worker::worker", "Starting handler for transactions");

        // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the block
        // before forwarding the block to the `Processor`
        let quorum_waiter = QuorumWaiter::new(
            self.consensus_config.authority().clone(),
            self.id,
            self.consensus_config.committee().clone(),
            self.consensus_config.worker_cache().clone(),
            network,
            node_metrics.clone(),
        );

        BlockProvider::new(
            self.id,
            quorum_waiter,
            node_metrics,
            client,
            self.consensus_config.database().clone(),
            self.consensus_config.parameters().worker_block_vote_timeout,
        )
    }
}
