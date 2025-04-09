//! Module for managing network peers.

mod all_peers;
mod banned;
mod behavior;
mod cache;
mod manager;
mod peer;
mod score;
mod status;
mod types;
pub use manager::PeerManager;
pub use types::{PeerAction, PeerEvent, PeerExchangeMap, Penalty};

// visibility for tests
#[cfg(test)]
pub(crate) use score::GLOBAL_SCORE_CONFIG;
