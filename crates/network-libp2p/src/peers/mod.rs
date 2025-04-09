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
pub use manager::{PeerEvent, PeerManager};
pub use score::Penalty;
pub use types::PeerExchangeMap;

// visibility for tests
#[cfg(test)]
pub(crate) use score::GLOBAL_SCORE_CONFIG;
