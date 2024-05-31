//! Node implementation for reth compatibility
//!
//! Inspired by reth_node_ethereum crate.
//!
//! A network implementation for worker RPC
//!
//! This is useful for wiring components together that don't require network but still need to be
//! generic over it.

use enr::{secp256k1::SecretKey, Enr};
use reth_db::{database::Database, database_metrics::{DatabaseMetadata, DatabaseMetrics}};
use reth_discv4::DEFAULT_DISCOVERY_PORT;
use reth_eth_wire::DisconnectReason;
use reth_network::NetworkHandle;
use reth_network_api::{
    NetworkError, NetworkInfo, PeerInfo, PeerKind, Peers, PeersInfo, Reputation,
    ReputationChangeKind,
};
use reth_node_builder::{
    components::{ComponentsBuilder, NetworkBuilder},
    node::{FullNodeTypes, NodeTypes},
    BuilderContext, ConfigureEvm,
};
use reth_node_ethereum::{
    node::{EthereumExecutorBuilder, EthereumNetworkBuilder, EthereumPayloadBuilder, EthereumPoolBuilder},
    EthEngineTypes, EthEvmConfig,
};
use reth_primitives::NodeRecord;
use reth_network_types::PeerId;
use reth_provider::providers::BlockchainProvider;
use reth_rpc_types::{admin::EthProtocolInfo, NetworkStatus};
use reth_transaction_pool::TransactionPool;
use std::{
    marker::PhantomData,
    net::{IpAddr, SocketAddr},
};
use tn_types::adiri_chain_spec;

/// Type configuration for a regular Telcoin node.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct WorkerNode<DB, Evm> {
    db: PhantomData<DB>,
    evm: PhantomData<Evm>,
}

// impl<DB, Evm> WorkerNode<DB, Evm> {
//     /// Returns an execution layer's [ComponentsBuilder] configured for a Worker node.
//     pub fn components<Node>(
//     ) -> ComponentsBuilder<Node, EthereumPoolBuilder, EthereumPayloadBuilder, EthereumNetworkBuilder, EthereumExecutorBuilder>
//     where
//         Node: FullNodeTypes<Engine = EthEngineTypes>,
//     {
//         ComponentsBuilder::default()
//             .node_types::<Node>()
//             .pool(EthereumPoolBuilder::default())
//             .payload(EthereumPayloadBuilder::default())
//             .network(EthereumNetworkBuilder::default())
//     }
// }

impl<DB, Evm> NodeTypes for WorkerNode<DB, Evm>
where
    DB: Send + Sync + 'static,
    Evm: Send + Sync + 'static,
{
    type Primitives = ();
    type Engine = EthEngineTypes;
}

impl<DB, Evm> FullNodeTypes for WorkerNode<DB, Evm>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Unpin + Clone + 'static,
    Evm: ConfigureEvm + Clone + 'static,
{
    type DB = DB;

    type Provider = BlockchainProvider<DB>;
}

/// A builder for the worker's "network".
///
/// Primarily used for RPC information.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerNetworkBuilder {
    // TODO add closure to modify network
}

impl<Node, Pool> NetworkBuilder<Node, Pool> for WorkerNetworkBuilder
where
    Node: FullNodeTypes,
    Pool: TransactionPool + Unpin + 'static,
{
    async fn build_network(
        self,
        _ctx: &BuilderContext<Node>,
        _pool: Pool,
    ) -> eyre::Result<NetworkHandle> {
        // let network = ctx.network_builder().await?;
        // let handle = ctx.start_network(network, pool);

        // Ok(handle)
        todo!()
    }
}

/// A type that implements all network trait that does nothing.
///
/// Intended for testing purposes where network is not used.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[derive(Default)]
pub struct WorkerNetwork;

impl NetworkInfo for WorkerNetwork {
    fn local_addr(&self) -> SocketAddr {
        (IpAddr::from(std::net::Ipv4Addr::UNSPECIFIED), DEFAULT_DISCOVERY_PORT).into()
    }

    async fn network_status(&self) -> Result<NetworkStatus, NetworkError> {
        let chain_spec = adiri_chain_spec();
        Ok(NetworkStatus {
            client_version: "adiri".to_string(),
            protocol_version: 1,
            eth_protocol_info: EthProtocolInfo {
                difficulty: Default::default(),
                network: 2017,
                genesis: chain_spec.genesis_hash(),
                head: Default::default(),
                config: chain_spec.genesis().config.clone(),
            },
        })
    }

    fn chain_id(&self) -> u64 {
        2017
    }

    fn is_syncing(&self) -> bool {
        false
    }

    fn is_initially_syncing(&self) -> bool {
        false
    }

    #[cfg(feature = "optimism")]
    fn sequencer_endpoint(&self) -> Option<&str> {
        None
    }
}

impl PeersInfo for WorkerNetwork {
    fn num_connected_peers(&self) -> usize {
        0
    }

    fn local_node_record(&self) -> NodeRecord {
        NodeRecord::new(self.local_addr(), PeerId::random())
    }

    fn local_enr(&self) -> Enr<SecretKey> {
        let sk = SecretKey::from_slice(&[0xcd; 32]).unwrap();
        Enr::builder().build(&sk).unwrap()
    }
}

impl Peers for WorkerNetwork {
    fn add_trusted_peer_id(&self, _peer: PeerId) {}

    fn add_peer_kind(&self, _peer: PeerId, _kind: PeerKind, _addr: SocketAddr) {}

    async fn get_peers_by_kind(&self, _kind: PeerKind) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    async fn get_all_peers(&self) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    async fn get_peer_by_id(&self, _peer_id: PeerId) -> Result<Option<PeerInfo>, NetworkError> {
        Ok(None)
    }

    async fn get_peers_by_id(&self, _peer_id: Vec<PeerId>) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    fn remove_peer(&self, _peer: PeerId, _kind: PeerKind) {}

    fn disconnect_peer(&self, _peer: PeerId) {}

    fn disconnect_peer_with_reason(&self, _peer: PeerId, _reason: DisconnectReason) {}

    fn reputation_change(&self, _peer_id: PeerId, _kind: ReputationChangeKind) {}

    async fn reputation_by_id(&self, _peer_id: PeerId) -> Result<Option<Reputation>, NetworkError> {
        Ok(None)
    }
}
