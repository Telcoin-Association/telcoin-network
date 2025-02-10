//! Error types for Telcoin Network Primary tasks.

mod proposer;
pub(crate) use proposer::{ProposerError, ProposerResult};
mod network;
pub(crate) use network::{PrimaryNetworkError, PrimaryNetworkResult};
mod gc;
pub(crate) use gc::{GarbageCollectorError, GarbageCollectorResult};
mod cert_manager;
pub(crate) use cert_manager::{CertManagerError, CertManagerResult};
