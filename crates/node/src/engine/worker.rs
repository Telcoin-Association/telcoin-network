//! Node implementation for reth compatibility
//!
//! Inspired by reth_node_ethereum crate.
//!
//! A network implementation for worker RPC.
//!
//! This is useful for wiring components together that don't require network but still need to be
//! generic over it.

use enr::{secp256k1::SecretKey, Enr};
use reth::rpc::builder::RpcServerHandle;
use reth_chainspec::ChainSpec;
use reth_discv4::DEFAULT_DISCOVERY_PORT;
use reth_eth_wire::DisconnectReason;
use reth_network_api::{
    EthProtocolInfo, NetworkError, NetworkInfo, NetworkStatus, PeerInfo, PeerKind, Peers,
    PeersInfo, Reputation, ReputationChangeKind,
};
use reth_network_peers::{NodeRecord, PeerId};
use reth_node_builder::NodeTypesWithDB;
use reth_provider::providers::BlockchainProvider;
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

/// The explicit type for the worker's transaction pool.
pub type WorkerTxPool<DB> = EthTransactionPool<BlockchainProvider<DB>, DiskFileBlobStore>;

/// Execution components on a per-worker basis.
pub(super) struct WorkerComponents<DB>
where
    DB: NodeTypesWithDB,
{
    /// The RPC handle.
    rpc_handle: RpcServerHandle,
    /// The worker's transaction pool.
    pool: WorkerTxPool<DB>,
}

impl<DB> WorkerComponents<DB>
where
    DB: NodeTypesWithDB,
{
    /// Create a new instance of [Self].
    pub fn new(rpc_handle: RpcServerHandle, pool: WorkerTxPool<DB>) -> Self {
        Self { rpc_handle, pool }
    }

    /// Return a reference to the rpc handle
    pub fn rpc_handle(&self) -> &RpcServerHandle {
        &self.rpc_handle
    }

    /// Return a reference to the worker's transaction pool.
    pub fn pool(&self) -> WorkerTxPool<DB> {
        self.pool.clone()
    }
}

/// A type that implements all network trait that does nothing.
///
/// Intended for testing purposes where network is not used.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkerNetwork {
    /// Chainspec
    chain_spec: Arc<ChainSpec>,
}

impl WorkerNetwork {
    /// Create a new instance of self.
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

impl NetworkInfo for WorkerNetwork {
    fn local_addr(&self) -> SocketAddr {
        (IpAddr::from(std::net::Ipv4Addr::UNSPECIFIED), DEFAULT_DISCOVERY_PORT).into()
    }

    #[allow(deprecated, reason = "EthProtocolInfo::difficulty is deprecated")]
    async fn network_status(&self) -> Result<NetworkStatus, NetworkError> {
        Ok(NetworkStatus {
            client_version: "v0.0.1".to_string(),
            protocol_version: 1,
            eth_protocol_info: EthProtocolInfo {
                difficulty: None,
                network: self.chain_id(),
                genesis: self.chain_spec.genesis_hash(),
                head: Default::default(),
                config: self.chain_spec.genesis().config.clone(),
            },
        })
    }

    fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    fn is_syncing(&self) -> bool {
        false
    }

    fn is_initially_syncing(&self) -> bool {
        false
    }
}

impl PeersInfo for WorkerNetwork {
    fn num_connected_peers(&self) -> usize {
        0
    }

    fn local_node_record(&self) -> NodeRecord {
        NodeRecord::new(self.local_addr(), PeerId::random())
    }

    // TODO: this is not supported
    fn local_enr(&self) -> Enr<SecretKey> {
        let sk = SecretKey::from_slice(&[0xcd; 32]).expect("secret key derived from static slice");
        Enr::builder().build(&sk).expect("ENR builds from key")
    }
}

impl Peers for WorkerNetwork {
    fn add_trusted_peer_id(&self, _peer: PeerId) {}

    fn add_peer_kind(
        &self,
        _peer: PeerId,
        _kind: PeerKind,
        _tcp_addr: SocketAddr,
        _udp_addr: Option<SocketAddr>,
    ) {
    }

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

    fn connect_peer_kind(
        &self,
        _peer: PeerId,
        _kind: PeerKind,
        _tcp_addr: SocketAddr,
        _udp_addr: Option<SocketAddr>,
    ) {
        // unimplemented!
    }
}
