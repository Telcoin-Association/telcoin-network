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
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_discv4::DEFAULT_DISCOVERY_PORT;
use reth_eth_wire::DisconnectReason;
use reth_evm::execute::BlockExecutorProvider;
use reth_network::NetworkHandle;
use reth_network_api::{
    NetworkError, NetworkInfo, NetworkStatus, PeerInfo, PeerKind, Peers, PeersInfo, Reputation,
    ReputationChangeKind,
};
use reth_network_peers::{NodeRecord, PeerId};
use reth_node_builder::{
    components::NetworkBuilder,
    node::{FullNodeTypes, NodeTypes},
    BuilderContext,
};
use reth_node_ethereum::EthEngineTypes;
use reth_provider::providers::BlockchainProvider;
use reth_rpc_types::admin::EthProtocolInfo;
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool, TransactionPool};
use std::{
    marker::PhantomData,
    net::{IpAddr, SocketAddr},
};
use tn_types::{adiri_chain_spec, PendingWorkerBlock};
use tokio::sync::watch;

/// The explicit type for the worker's transaction pool.
pub type WorkerTxPool<DB> = EthTransactionPool<BlockchainProvider<DB>, DiskFileBlobStore>;

/// Convenience type for managing worker watch channels for pending block.
#[derive(Clone, Debug)]
pub(super) struct PendingBlockWatchChannels {
    /// The sending half of the watch channel.
    sender: watch::Sender<PendingWorkerBlock>,
    /// The receiving half of the watch channel.
    receiver: watch::Receiver<PendingWorkerBlock>,
}

impl PendingBlockWatchChannels {
    /// Create a new instance of [Self].
    pub fn new(
        sender: watch::Sender<PendingWorkerBlock>,
        receiver: watch::Receiver<PendingWorkerBlock>,
    ) -> Self {
        Self { sender, receiver }
    }

    /// Return an owned sender channel.
    pub fn sender(&self) -> watch::Sender<PendingWorkerBlock> {
        self.sender.clone()
    }

    /// Return an owned receiver channel.
    pub fn receiver(&self) -> watch::Receiver<PendingWorkerBlock> {
        self.receiver.clone()
    }
}

/// Execution components on a per-worker basis.
pub(super) struct WorkerComponents<DB> {
    /// The RPC handle.
    rpc_handle: RpcServerHandle,
    /// The current pending block watch channel.
    pending_channels: PendingBlockWatchChannels,
    /// The worker's transaction pool.
    pool: WorkerTxPool<DB>,
}

impl<DB> WorkerComponents<DB> {
    /// Create a new instance of [Self].
    pub async fn new(
        rpc_handle: RpcServerHandle,
        pending_channels: PendingBlockWatchChannels,
        pool: WorkerTxPool<DB>,
    ) -> Self {
        Self { rpc_handle, pending_channels, pool }
    }

    /// Return a reference to the rpc handle
    pub fn rpc_handle(&self) -> &RpcServerHandle {
        &self.rpc_handle
    }

    /// Return a reference to the worker's transaction pool.
    pub fn pool(&self) -> WorkerTxPool<DB> {
        self.pool.clone()
    }

    /// Return a receiver for pending block watch channel.
    pub fn pending_block_receiver(&self) -> watch::Receiver<PendingWorkerBlock> {
        self.pending_channels.receiver()
    }

    /// Return a sender for pending block watch channel.
    pub fn pending_block_sender(&self) -> watch::Sender<PendingWorkerBlock> {
        self.pending_channels.sender()
    }
}

/// Type configuration for a regular worker node.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct WorkerNode<DB, Evm> {
    db: PhantomData<DB>,
    evm: PhantomData<Evm>,
}

impl<DB, Evm> NodeTypes for WorkerNode<DB, Evm>
where
    DB: Send + Sync + Unpin + 'static,
    Evm: Send + Sync + Unpin + 'static,
{
    type Primitives = ();
    type Engine = EthEngineTypes;
}

impl<DB, Evm> FullNodeTypes for WorkerNode<DB, Evm>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Unpin + Clone + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
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
                // TODO: update chain spec with genesis hash so this doesn't `hash_slow()` everytime
                //
                // genesis.into() explicitly sets this to `None` for some reason
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
}
