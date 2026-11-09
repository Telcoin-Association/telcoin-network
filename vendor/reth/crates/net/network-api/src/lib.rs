//! Reth interface definitions and commonly used types for the reth-network crate.
//!
//! Provides abstractions for the reth-network crate.
//!
//! ## Feature Flags
//!
//! - `serde` (default): Enable serde support

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// TN(TN-0031): reth's RPC needs only `NetworkInfo` and `PeersInfo` here, and TN runs no devp2p
// network. Upstream's `downloaders`, `events` and `test_utils` modules, the `FullNetwork` and
// `Peers` traits, `PeerInfo` and `Direction` are removed, and with them reth-network-p2p and
// reth-network-types.

/// Network Error
pub mod error;
/// Implementation of network traits for that does nothing.
pub mod noop;

pub use alloy_rpc_types_admin::EthProtocolInfo;
pub use error::NetworkError;

use reth_eth_wire_types::Capability;
use reth_network_peers::NodeRecord;
use std::{future::Future, net::SocketAddr};

/// The `PeerId` type.
pub type PeerId = alloy_primitives::B512;

/// Provides general purpose information about the network.
#[auto_impl::auto_impl(&, Arc)]
pub trait NetworkInfo: Send + Sync {
    /// Returns the [`SocketAddr`] that listens for incoming connections.
    fn local_addr(&self) -> SocketAddr;

    /// Returns the current status of the network being ran by the local node.
    fn network_status(&self) -> impl Future<Output = Result<NetworkStatus, NetworkError>> + Send;

    /// Returns the chain id
    fn chain_id(&self) -> u64;

    /// Returns `true` if the network is undergoing sync.
    fn is_syncing(&self) -> bool;

    /// Returns `true` when the node is undergoing the very first Pipeline sync.
    fn is_initially_syncing(&self) -> bool;
}

/// Provides general purpose information about Peers in the network.
#[auto_impl::auto_impl(&, Arc)]
pub trait PeersInfo: Send + Sync {
    /// Returns how many peers the network is currently connected to.
    ///
    /// Note: this should only include established connections and _not_ ongoing attempts.
    fn num_connected_peers(&self) -> usize;

    /// Returns the Ethereum Node Record of the node.
    fn local_node_record(&self) -> NodeRecord;

    /// Returns the local ENR of the node.
    fn local_enr(&self) -> enr::Enr<enr::secp256k1::SecretKey>;
}

/// The status of the network being ran by the local node.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NetworkStatus {
    /// The local node client version.
    pub client_version: String,
    /// The current ethereum protocol version
    pub protocol_version: u64,
    /// Information about the Ethereum Wire Protocol.
    pub eth_protocol_info: EthProtocolInfo,
    /// The list of supported capabilities and their versions.
    pub capabilities: Vec<Capability>,
}
