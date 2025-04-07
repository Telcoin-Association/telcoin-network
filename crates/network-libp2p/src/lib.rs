// SPDX-License-Identifier: MIT or Apache-2.0
//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod codec;
mod consensus;
pub mod error;
mod peers;
pub mod types;

// export types
pub use codec::{TNCodec, TNMessage};
pub use consensus::ConsensusNetwork;
pub use peers::{PeerExchangeMap, Penalty};

// re-export specific libp2p types
pub use libp2p::{
    gossipsub::Message as GossipMessage, identity::PeerId, request_response::ResponseChannel,
    Multiaddr,
};
