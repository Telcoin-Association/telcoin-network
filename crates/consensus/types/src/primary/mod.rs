//! Primary types used for consensus.

use std::time::{Duration, SystemTime};
mod batch;
mod certificate;
mod header;
mod metadata;
mod vote;

pub use batch::*;
pub use certificate::*;
pub use header::*;
pub use metadata::*;
pub use vote::*;

pub const DEFAULT_BAD_NODES_STAKE_THRESHOLD: u64 = 0;

/// The round number.
pub type Round = u64;

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
