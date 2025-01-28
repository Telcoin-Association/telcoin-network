// SPDX-License-Identifier: MIT or Apache-2.0
//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod codec;
mod consensus;
pub mod error;
mod messages;
pub mod types;

// export types
pub use codec::{TNCodec, TNMessage, MAX_GOSSIP_SIZE, MAX_REQUEST_SIZE};
pub use consensus::ConsensusNetwork;
pub use messages::{WorkerRequest, WorkerResponse};

// re-export specific libp2p types
pub use libp2p::{identity::PeerId, request_response::ResponseChannel};
