//! Primary types used for consensus.

use std::time::{Duration, SystemTime};
mod block;
mod certificate;
mod header;
mod info;
mod output;
mod reputation;
mod vote;

pub use block::*;
pub use certificate::*;
pub use header::*;
pub use info::*;
pub use output::*;
pub use reputation::*;
pub use vote::*;

/// The default primary udp port for consensus messages.
pub const DEFAULT_PRIMARY_PORT: u16 = 44894;

/// For now, use 0 to prevent any removal of bad nodes since validator sets are static.
///
/// The following note is quoted from sui regarding a default of `20`:
/// "Taking a baby step approach, we consider only 20% by stake as bad nodes so
/// we have a 80% by stake of nodes participating in the
/// leader committee. That allow us for more redundancy in
/// case we have validators under performing - since the
/// responsibility is shared amongst more nodes. We can increase that once we do
/// have higher confidence."
pub const DEFAULT_BAD_NODES_STAKE_THRESHOLD: u64 = 0;

/// The round number.
/// Becomes the lower 32 bits of a nonce (with epoch the high bits).
pub type Round = u32;

/// The epoch UNIX timestamp in seconds.
pub type TimestampSec = u64;

/// Timestamp trait for calculating the amount of time that elapsed between
/// timestamp and "now".
pub trait Timestamp {
    /// Returns the time elapsed between the timestamp
    /// and "now". The result is a Duration.
    fn elapsed(&self) -> Duration;
}

impl Timestamp for TimestampSec {
    fn elapsed(&self) -> Duration {
        let diff = now().saturating_sub(*self);
        Duration::from_secs(diff)
    }
}

/// Returns the current time expressed as UNIX
/// timestamp in seconds
pub fn now() -> TimestampSec {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs() as TimestampSec,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
