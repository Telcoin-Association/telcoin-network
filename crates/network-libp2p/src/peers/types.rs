//! Types for managing peers.

use std::collections::{hash_map::IntoIter, HashMap, HashSet};

use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

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

/// Wrapper for a map of [PeerId] to a collection of [Multiaddr].
///
/// This is a convenience wrapper because PeerId doesn't implement `Deserialize`.
/// Peers exchange information to facilitate discovery.
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct PeerExchangeMap(
    #[serde_as(as = "HashMap<DisplayFromStr, HashSet<DisplayFromStr>>")]
    pub  HashMap<PeerId, HashSet<Multiaddr>>,
);

impl IntoIterator for PeerExchangeMap {
    type Item = (PeerId, HashSet<Multiaddr>);
    type IntoIter = IntoIter<PeerId, HashSet<Multiaddr>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<HashMap<PeerId, HashSet<Multiaddr>>> for PeerExchangeMap {
    fn from(value: HashMap<PeerId, HashSet<Multiaddr>>) -> Self {
        Self(value)
    }
}
