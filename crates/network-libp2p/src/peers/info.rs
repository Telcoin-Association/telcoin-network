//! Information shared between peers.

use super::status::ConnectionStatus;
use libp2p::Multiaddr;
use serde::Serialize;
use std::{collections::HashSet, time::Instant};

/// Information about a given connected peer.
#[derive(Serialize, Clone, Debug)]
pub struct PeerInfo {
    /// The peers reputation
    score: Score,
    /// Connection status of this peer
    connection_status: ConnectionStatus,
    /// The known listening addresses of this peer. This is given by identify and can be arbitrary
    /// (including local IPs).
    listening_addresses: Vec<Multiaddr>,
    /// These are the multiaddrs we have physically seen and is what we use for banning/un-banning
    /// peers.
    seen_multiaddrs: HashSet<Multiaddr>,
    /// The current syncing state of the peer. The state may be determined after it's initial
    /// connection.
    sync_status: SyncStatus,
    /// The ENR subnet bitfield of the peer. This may be determined after it's initial
    /// connection.
    meta_data: Option<MetaData<E>>,
    /// Subnets the peer is connected to.
    subnets: HashSet<Subnet>,
    /// The target amount of time to retain this peer. After this time, the peer is no longer
    /// necessary.
    #[serde(skip)]
    min_ttl: Option<Instant>,
    /// Is the peer a trusted peer.
    is_trusted: bool,
    /// Direction of the first connection of the last (or current) connected session with this peer.
    /// None if this peer was never connected.
    connection_direction: Option<ConnectionDirection>,
    // TODO: include TNR?
}
