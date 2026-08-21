//! RPC-facing network shim and per-worker execution components.
//!
//! Inspired by reth_node_ethereum crate. TN does not run reth's devp2p networking, but reth's
//! RPC builder needs `NetworkInfo`/`Peers` implementations to serve the `net`, `web3`, and
//! `eth` namespaces. [`WorkerNetwork`] is that shim: it answers with the chain id, a
//! status headed at the canonical tip, and a peer count that a background task refreshes every 15
//! seconds by polling the worker's libp2p network handle. Everything reth-specific (peer
//! management, ENR/node records, the admin namespace) is a deliberate no-op. The chain spec is held
//! behind an `Arc`, so cloning the shim is cheap.
//!
//! [`WorkerComponents`] bundles what the node keeps per worker: the RPC server handle, the
//! worker's transaction pool, and the [`WorkerNetwork`] (retained so its peer-count task can be
//! respawned when the epoch rolls over).

use crate::{ChainSpec, RethEnv, WorkerTxPool};
use parking_lot::RwLock;
use reth::{network::config::SecretKey, rpc::builder::RpcServerHandle};
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_discv4::DEFAULT_DISCOVERY_PORT;
use reth_eth_wire::DisconnectReason;
use reth_network_api::{
    EthProtocolInfo, NetworkError, NetworkInfo, NetworkStatus, PeerInfo, PeerKind, Peers,
    PeersInfo, Reputation, ReputationChangeKind,
};
use reth_network_peers::{Enr, NodeRecord, PeerId as RethPeerId};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tn_worker::WorkerNetworkHandle;

/// Execution components on a per-worker basis.
#[derive(Debug)]
pub struct WorkerComponents {
    /// The RPC handle.
    rpc_handle: RpcServerHandle,
    /// The worker's transaction pool.
    pool: WorkerTxPool,
    /// Keep the WorkerNetwork around so we can update it's task(s).
    network: WorkerNetwork,
}

impl WorkerComponents {
    /// Create a new instance of [Self].
    pub fn new(rpc_handle: RpcServerHandle, pool: WorkerTxPool, network: WorkerNetwork) -> Self {
        Self { rpc_handle, pool, network }
    }

    /// Return a reference to the rpc handle
    pub fn rpc_handle(&self) -> &RpcServerHandle {
        &self.rpc_handle
    }

    /// Return a reference to the worker's transaction pool.
    pub fn pool(&self) -> WorkerTxPool {
        self.pool.clone()
    }

    /// Return the worker network inteface (RPC helper) for this worker.
    pub fn worker_network(&self) -> &WorkerNetwork {
        &self.network
    }
}

/// A type that implements traits used by Reth for it's RPC.
/// Traits are filled out to provide data for net, web3 and eth namespaces when available.
/// Much of these traits are NO-OPS are not used, they support the admin namespace for
/// instance which TN does not use or support.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkerNetwork {
    /// Chainspec
    chain_spec: Arc<RethChainSpec>,
    /// Track our peer count for queries.
    peer_count: Arc<RwLock<usize>>,
    /// App version.
    version: &'static str,
    /// Consensus catch-up state backing `eth_syncing` (issue #1231).
    sync_flags: Arc<RwLock<SyncFlags>>,
    /// Execution env backing the `network_status` head; `None` only in tests (issue #1231).
    reth_env: Option<RethEnv>,
}

/// Sync flags backing the shim's `eth_syncing` answers.
///
/// `syncing` mirrors whether the node is catching up on consensus output; the node manager
/// drives it from the consensus node-mode watch. `completed_initial_sync` latches on the
/// first caught-up report so `is_initially_syncing` distinguishes the first catch-up of
/// this process from a later mid-epoch fall-behind.
#[derive(Debug, Default)]
struct SyncFlags {
    /// True while the node is catching up on consensus output.
    syncing: bool,
    /// True once the node has reported caught-up at least once.
    completed_initial_sync: bool,
}

impl WorkerNetwork {
    /// Create a new instance of self.
    pub fn new(
        chain_spec: ChainSpec,
        worker_network: WorkerNetworkHandle,
        version: &'static str,
        reth_env: RethEnv,
    ) -> Self {
        let peer_count = Arc::new(RwLock::new(0));
        let peer_count_clone = peer_count.clone();
        let spawner = worker_network.get_task_spawner().clone();
        spawner.spawn_task("Worker Network Peers", async move {
            loop {
                if let Ok(peers) = worker_network.connected_peers_count().await {
                    let mut guard = peer_count_clone.write();
                    *guard = peers;
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
        Self {
            chain_spec: chain_spec.reth_chain_spec(),
            peer_count,
            version,
            sync_flags: Arc::new(RwLock::new(SyncFlags::default())),
            reth_env: Some(reth_env),
        }
    }

    /// Create a test instance that does not track peers.
    ///
    /// The peer count stays zero. Tests that build an RPC server use this so they
    /// do not need a live worker network handle.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test(chain_spec: ChainSpec) -> Self {
        Self {
            chain_spec: chain_spec.reth_chain_spec(),
            peer_count: Arc::new(RwLock::new(0)),
            version: "test",
            sync_flags: Arc::new(RwLock::new(SyncFlags::default())),
            reth_env: None,
        }
    }

    /// Record whether the node is catching up on consensus output (issue #1231).
    ///
    /// The node manager drives this from the consensus node-mode watch; the stock reth
    /// `eth_syncing` handler reads it back through [`NetworkInfo::is_syncing`]. The first
    /// caught-up report latches `completed_initial_sync`, so a later mid-epoch fall-behind
    /// reports as syncing but no longer as initially syncing. The node boots
    /// optimistic-current, so the driver's first not-syncing report usually latches the
    /// initial sync as already complete; `is_initially_syncing` is true only when a
    /// demotion lands before that first report. Nothing in TN's RPC surface consumes the
    /// initial-sync distinction today: `eth_syncing` reads only `is_syncing`.
    pub fn set_syncing(&self, syncing: bool) {
        let mut flags = self.sync_flags.write();
        flags.completed_initial_sync = flags.completed_initial_sync || !syncing;
        flags.syncing = syncing;
    }

    /// Spawn a new task to keep up with peer counts.
    /// Use this when the epoch rolls over and the worker_network gets a new task manager.
    pub fn respawn_peer_count(&self, worker_network: WorkerNetworkHandle) {
        let peer_count = self.peer_count.clone();
        let spawner = worker_network.get_task_spawner().clone();
        spawner.spawn_task("Worker Network Peers", async move {
            loop {
                if let Ok(peers) = worker_network.connected_peers_count().await {
                    let mut guard = peer_count.write();
                    *guard = peers;
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
    }
}

impl NetworkInfo for WorkerNetwork {
    // TN Unused
    fn local_addr(&self) -> SocketAddr {
        (IpAddr::from(std::net::Ipv4Addr::UNSPECIFIED), DEFAULT_DISCOVERY_PORT).into()
    }

    #[allow(deprecated, reason = "EthProtocolInfo::difficulty is deprecated")]
    async fn network_status(&self) -> Result<NetworkStatus, NetworkError> {
        // The head is the canonical tip from the execution env (issue #1231). Test shims
        // have no env; the genesis hash is the true tip of their fresh chain.
        let head = self
            .reth_env
            .as_ref()
            .map(|env| env.canonical_tip().hash())
            .unwrap_or_else(|| self.chain_spec.genesis_hash());
        Ok(NetworkStatus {
            client_version: self.version.to_string(), // web3_clientVersion
            // TN speaks no eth wire protocol, so `eth_protocolVersion` has no real value
            // to report; this is a fixed placeholder, and `capabilities` is honestly empty.
            protocol_version: 1,
            eth_protocol_info: EthProtocolInfo {
                difficulty: None,
                network: self.chain_id(),
                genesis: self.chain_spec.genesis_hash(),
                head,
                config: self.chain_spec.genesis().config.clone(),
            },
            capabilities: vec![],
        })
    }

    // eth_chainId AND net_version
    fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    // eth_syncing
    fn is_syncing(&self) -> bool {
        self.sync_flags.read().syncing
    }

    fn is_initially_syncing(&self) -> bool {
        let flags = self.sync_flags.read();
        flags.syncing && !flags.completed_initial_sync
    }
}

impl PeersInfo for WorkerNetwork {
    // net_peerCount
    fn num_connected_peers(&self) -> usize {
        *self.peer_count.read()
    }

    // TN Unused
    fn local_node_record(&self) -> NodeRecord {
        NodeRecord::new(self.local_addr(), RethPeerId::random())
    }

    // TN Unused
    fn local_enr(&self) -> Enr<SecretKey> {
        let sk = SecretKey::from_slice(&[0xcd; 32]).expect("secret key derived from static slice");
        Enr::builder().build(&sk).expect("ENR builds from key")
    }
}

// These appear to support Reth's admin namespace- TN does not use this.
impl Peers for WorkerNetwork {
    fn add_trusted_peer_id(&self, _peer: RethPeerId) {}

    fn add_peer_kind(
        &self,
        _peer: RethPeerId,
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

    async fn get_peer_by_id(&self, _peer_id: RethPeerId) -> Result<Option<PeerInfo>, NetworkError> {
        Ok(None)
    }

    async fn get_peers_by_id(
        &self,
        _peer_id: Vec<RethPeerId>,
    ) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    fn remove_peer(&self, _peer: RethPeerId, _kind: PeerKind) {}

    fn disconnect_peer(&self, _peer: RethPeerId) {}

    fn disconnect_peer_with_reason(&self, _peer: RethPeerId, _reason: DisconnectReason) {}

    fn reputation_change(&self, _peer_id: RethPeerId, _kind: ReputationChangeKind) {}

    async fn reputation_by_id(
        &self,
        _peer_id: RethPeerId,
    ) -> Result<Option<Reputation>, NetworkError> {
        Ok(None)
    }

    fn connect_peer_kind(
        &self,
        _peer: RethPeerId,
        _kind: PeerKind,
        _tcp_addr: SocketAddr,
        _udp_addr: Option<SocketAddr>,
    ) {
        // unimplemented!
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tn_types::{test_genesis, TaskManager, B256};

    /// Build a shim over the test genesis without a live worker network handle.
    fn test_network() -> WorkerNetwork {
        WorkerNetwork {
            chain_spec: Arc::new(test_genesis().into()),
            peer_count: Arc::new(RwLock::new(0)),
            version: "test",
            sync_flags: Arc::new(RwLock::new(SyncFlags::default())),
            reth_env: None,
        }
    }

    /// `network_status` heads at the canonical tip when an execution env is present
    /// (issue #1231). A fresh chain's tip is its genesis block, whose hash is non-zero,
    /// so the non-zero assert also pins that the head is no longer left defaulted.
    #[tokio::test]
    async fn test_network_status_head_is_canonical_tip() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
        let network = WorkerNetwork { reth_env: Some(reth_env.clone()), ..test_network() };

        let status = network.network_status().await?;
        assert_eq!(status.eth_protocol_info.head, reth_env.canonical_tip().hash());
        assert_ne!(status.eth_protocol_info.head, B256::ZERO);

        Ok(())
    }

    /// Without an execution env (`new_for_test`) the head falls back to the genesis hash,
    /// the true tip of a fresh chain (issue #1231).
    #[tokio::test]
    async fn test_network_status_head_falls_back_to_genesis() -> eyre::Result<()> {
        let network = test_network();
        let status = network.network_status().await?;
        assert_eq!(status.eth_protocol_info.head, network.chain_spec.genesis_hash());
        Ok(())
    }

    /// `is_syncing` follows the recorded state and the initial-sync latch covers only the
    /// first catch-up of the process (issue #1231).
    #[test]
    fn test_sync_flags_follow_recorded_state() {
        let network = test_network();
        assert!(!network.is_syncing());
        assert!(!network.is_initially_syncing());

        // A node that starts behind is initially syncing.
        network.set_syncing(true);
        assert!(network.is_syncing());
        assert!(network.is_initially_syncing());

        network.set_syncing(false);
        assert!(!network.is_syncing());
        assert!(!network.is_initially_syncing());

        // A later fall-behind reports syncing, but the initial sync is over.
        network.set_syncing(true);
        assert!(network.is_syncing());
        assert!(!network.is_initially_syncing());
    }
}
