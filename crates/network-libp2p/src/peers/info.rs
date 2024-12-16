//! Information shared between peers.

use super::{
    score::Score,
    status::{ConnectionStatus, SyncStatus},
};
use libp2p::Multiaddr;
use serde::Serialize;
use std::collections::HashSet;

/// Information about a given connected peer.
#[derive(Serialize, Clone, Debug)]
pub struct PeerInfo {
    /// The peers reputation
    score: Score,
    /// The known listening addresses of this peer. This is given by identify and can be arbitrary
    /// (including local IPs).
    listening_addresses: Vec<Multiaddr>,
    /// The multiaddrs this node has witnessed the peer using.
    seen_multiaddrs: HashSet<Multiaddr>,
    /// Connection status of the peer.
    connection_status: ConnectionStatus,
    /// The peer's syncing status compared to the most recent state change.
    sync_status: SyncStatus,
    /// Trusted peers are specifically included by node operators.
    is_trusted: bool,
    // TODO: include TNR?
}
