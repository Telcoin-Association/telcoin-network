// SPDX-License-Identifier: MIT or Apache-2.0
//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod codec;
mod consensus;
pub mod error;
mod messages;
pub mod types;

// export message types
pub use messages::{PrimaryRequest, PrimaryResponse, WorkerRequest, WorkerResponse};
