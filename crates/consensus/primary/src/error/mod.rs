//! Error types for Telcoin Network Primary tasks.

mod proposer;
pub(crate) use proposer::{ProposerError, ProposerResult};
mod network;
pub(crate) use network::{PrimaryNetworkError, PrimaryNetworkResult};
