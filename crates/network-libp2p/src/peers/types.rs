//! Types for managing peers.

use libp2p::Multiaddr;
use serde::Serialize;

/// Types of connections between peers.
pub(super) enum ConnectionType {
    /// This node is dialing the peer.
    Dialing,
    /// A peer has successfully dialed this node.
    IncomingConnection {
        /// The peer's multiaddr.
        multiaddr: Multiaddr,
    },
    /// This node has successfully dialed a peer.
    OutgoingConnection {
        /// The peer's multiaddr.
        multiaddr: Multiaddr,
    },
}

/// Direction of connection between peers from the local node's perspective.
#[derive(Debug, Clone, Serialize)]
pub(super) enum ConnectionDirection {
    /// The connection was established by a peer dialing this node.
    Incoming,
    /// The connection was established by this node dialing a peer.
    Outgoing,
}
