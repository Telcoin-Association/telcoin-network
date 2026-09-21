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
pub(crate) use manager::{PeerManager, PutRecordRate};
pub(crate) use types::PeerEvent;
pub use types::{PeerExchangeMap, Penalty};

// visibility for tests
#[cfg(test)]
pub(crate) use score::GLOBAL_SCORE_CONFIG;

/// Per-peer storage cap used by the cap-consistency regression tests.
#[cfg(test)]
pub(crate) use peer::MAX_MULTIADDRS_PER_PEER;

/// Shared production thresholds used by the consensus call-site regression tests.
#[cfg(test)]
pub(crate) use manager::{
    MAX_PUT_RECORDS_PER_WINDOW, PUT_RECORD_DISCONNECT_THRESHOLD, PUT_RECORD_PENALTY_THRESHOLD,
};
