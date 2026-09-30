//! Types for managing peers.

use crate::types::NetworkResult;
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::IntoIter, HashMap, HashSet},
    net::IpAddr,
};
use tn_types::{BlsPublicKey, NetworkPublicKey};
use tokio::sync::oneshot;

/// Domain identity for a tracked peer.
///
/// Telcoin associates a [BlsPublicKey] with a peer once its network settings are known
/// (committee, trusted, or otherwise known peers). Until then a peer discovered through
/// libp2p - an inbound connection or a kad-dialed candidate - is known only by its libp2p
/// [PeerId]. This sum type captures that distinction so the domain key (the bls public key)
/// is used wherever it is known, and the libp2p id only where it isn't.
///
/// `Confirmed` and `Unidentified` are distinct keys: a peer first seen as `Unidentified`
/// is re-keyed onto its `Confirmed` identity once its bls key is learned.
///
/// The variants differ in size (a bls key is larger than a libp2p id), but this type is a
/// `Copy` map key resolved on every libp2p connection event; boxing would forfeit `Copy` and
/// add indirection to those lookups, so the inline layout is intentional.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum PeerIdentity {
    /// A peer whose [BlsPublicKey] is known. This is the telcoin-domain identity.
    Confirmed(BlsPublicKey),
    /// A peer known only by its libp2p [PeerId] (no bls key learned yet).
    Unidentified(PeerId),
}

/// Why a peer is exempt from the score model and penalties for the current epoch.
///
/// A peer subject to normal scoring has no basis (`None`). The two provenances are kept
/// distinct on purpose (issue #715): operator allowlisting is sticky - set at construction
/// and never altered by epoch rotation - whereas validator status is derived live from the
/// tracked committee slots, so a validator rotating out of committee can never strip operator
/// trust.
/// Only the exemption *decision* (presence) drives behaviour; the variant is carried for
/// observability (it names which provenance suppressed a penalty in logs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TrustBasis {
    /// Explicitly allowlisted by the node operator.
    Operator,
    /// Sits in a tracked committee slot: the previous, current, or next epoch's committee.
    Validator,
}

/// Events for the `PeerManager`.
#[derive(Debug)]
pub(crate) enum PeerEvent {
    /// Connected with peer.
    PeerConnected(PeerId, Multiaddr),
    /// Peer was disconnected.
    PeerDisconnected(PeerId),
    /// Disconnect from the peer without exchanging peer information.
    /// This is the event for disconnecting from penalized peers.
    DisconnectPeer(PeerId),
    /// Disconnect from the peer and share peer information for discovery.
    /// This is the event for disconnecting from excess peers with otherwise trusted reputations.
    DisconnectPeerX(PeerId, PeerExchangeMap),
    /// Peer manager has identified a peer and associated ip addresses to ban.
    Banned(PeerId),
    /// Peer manager has unbanned a peer and associated ip addresses.
    Unbanned(PeerId),
    /// Authorities are missing from the peer map. This triggers kad queries.
    MissingAuthorities(Vec<BlsPublicKey>),
    /// Initiate a discovery attempt because discovery peer counts are low.
    Discovery,
}

/// The action to take after a peer's reputation or connection status changes.
///
/// Both reputation and connection status changes may require the manager to take
/// action to update the peer.
#[derive(Debug)]
pub(super) enum PeerAction {
    /// Ban the peer and the associated IP addresses.
    Ban(Vec<IpAddr>),
    /// No action needed.
    NoAction,
    /// Disconnect from peer.
    Disconnect,
    /// Disconnect a peer with peer exchange information to support discovery.
    /// This results in a temporary ban to prevent immediate reconnection attempts.
    DisconnectWithPX,
    /// Unban the peer and it's known ip addresses.
    Unban(Vec<IpAddr>),
}

impl PeerAction {
    /// Helper method if the action is to ban the peer.
    pub(super) fn is_ban(&self) -> bool {
        matches!(self, PeerAction::Ban(_))
    }
}

/// Penalties applied to peers based on the significance of their actions.
///
/// Each variant has an associated score change.
///
/// NOTE: the number of variations is intentionally low.
/// Too many variations or specific penalties would result in more complexity.
#[derive(Debug, Clone, Copy)]
pub enum Penalty {
    /// The penalty assessed for actions that result in an error and are likely not malicious.
    ///
    /// Peers have a high tolerance for this type of error and will be banned ~50 occurances.
    Mild,
    /// The penalty assessed for actions that result in an error and are likely not malicious.
    ///
    /// Peers have a medium tolerance for this type of error and will be banned ~10 occurances.
    Medium,
    /// The penalty assessed for actions that are likely not malicious, but will not be tolerated.
    ///
    /// The peer will be banned after ~5 occurances (based on -100).
    Severe,
    /// The penalty assessed for unforgiveable actions.
    ///
    /// This type of action results in disconnecting from a peer and banning them.
    Fatal,
}

/// Request for dialing peers.
#[derive(Debug)]
pub(crate) struct DialRequest {
    /// The peer's network id.
    pub(crate) peer_id: PeerId,
    /// The multiaddr to dial.
    pub(crate) multiaddrs: Vec<Multiaddr>,
    /// The channel to forward results and errors.
    /// Optional in case dial is the result of a peer-exchange.
    pub(crate) reply: Option<oneshot::Sender<NetworkResult<()>>>,
}

/// Types of connections between peers.
#[derive(Debug)]
pub(super) enum ConnectionType {
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct PeerExchangeMap(pub HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>);

impl IntoIterator for PeerExchangeMap {
    type Item = (BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>));
    type IntoIter = IntoIter<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>> for PeerExchangeMap {
    fn from(value: HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>) -> Self {
        Self(value)
    }
}
