//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod codec;
mod consensus;
pub mod error;
mod messages;
pub mod types;

// export types
pub use codec::TNMessage;
pub use consensus::ConsensusNetwork;
pub use libp2p::request_response::ResponseChannel;
pub use messages::{WorkerRequest, WorkerResponse};
