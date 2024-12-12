//! Peer service to track known peers.

use banned::BannedPeers;
use info::PeerInfo;
use libp2p::PeerId;
use std::collections::HashMap;
mod banned;
mod info;
mod status;

pub struct PeerService {
    /// The collection of known connected peers, their status and reputation
    peers: HashMap<PeerId, PeerInfo>,
    /// The number of peers that have disconnected from this node.
    disconnected_peers: usize,
    /// Information for peers that scored poorly enough to become banned.
    banned_peers: BannedPeers,
    /// Bool if peer scoring is active.
    peer_scoring_active: bool,
}
