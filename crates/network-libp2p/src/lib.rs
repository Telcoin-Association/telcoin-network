//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod codec;
mod consensus;
pub mod error;
mod messages;
pub mod types;

// export types
pub use consensus::ConsensusNetwork;
pub use messages::{PrimaryRequest, PrimaryResponse, WorkerRequest, WorkerResponse};
