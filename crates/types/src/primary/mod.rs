//! Primary types used for consensus.

use std::{
    fmt,
    time::{Duration, SystemTime},
};
mod block;
mod certificate;
mod epoch;
mod header;
mod info;
mod node_mode;
mod output;
mod reputation;
mod seed_chain;
mod vote;

pub use block::*;
pub use certificate::*;
pub use epoch::*;
pub use header::*;
pub use info::*;
pub use node_mode::*;
pub use output::*;
pub use reputation::*;
pub use seed_chain::*;
pub use vote::*;

/// The default primary udp port for consensus messages.
pub const DEFAULT_PRIMARY_PORT: u16 = 44894;

/// 33% of nodes can be labelled as "bad".  This means no more than 33% of the committee can be
/// considered bad nodes and at least 33% of the committee should be considered "good" nodes.  This
/// can be violated only in some extreme edge cases where scores/number of nodes require it.  Note
/// that nodes will NOT be considered "bad" unless they actually have low reputation relative to the
/// other nodes.  The bad list is expected to be empty except in the case of node(s) being down or
/// having bad connectivity, etc.  Also note that nodes with the same reputation will wind up on the
/// same list (good or bad) not unfairly be punished while another node is rewarded.
pub const DEFAULT_BAD_NODES_STAKE_THRESHOLD: u64 = 33;

/// Maximum garbage-collection depth, in consensus rounds, that the protocol supports.
///
/// This is the garbage-collection horizon, not the depth a commit actually reaches: `order_dag`
/// descends only to `gc_round + 1` and additionally skips any round already committed per
/// authority, so a `CommittedSubDag` is usually only a handful of rounds deep (a leader commits
/// every couple of rounds).  It serves purely as a safe ceiling: because no certificate at or below
/// `gc_round` can ever be linked into a commit, no sub-DAG can span more than this many rounds, a
/// deliberately loose over-estimate that holds regardless of commit cadence.  A node's configured
/// `gc_depth` is validated against this ceiling by `Parameters::validate`, and the consensus-pack
/// reconstruction bound (`max_batches_per_output`) is derived from it as a conservative
/// over-estimate, so raising this value requires re-deriving that bound.
pub const MAX_GC_DEPTH: Round = 50;

/// Rounds subtracted from a node's configured `gc_depth` when the consensus network handler
/// computes its "activity window", the number of rounds a node may lag execution before it steps
/// back as too far behind to be an active voter.
///
/// The buffer makes a node go inactive *before* it rides the garbage-collection horizon exactly,
/// where subtle races live.  Because the window is `gc_depth - GC_ACTIVITY_BUFFER`, a configured
/// `gc_depth` at or below this buffer collapses the window to zero and wedges the node inactive
/// during normal operation.  `Parameters::validate_operational_floors` therefore requires
/// `gc_depth > GC_ACTIVITY_BUFFER` at the production startup entry points, keeping the floor
/// coupled to the buffer so the two can never drift apart.
pub const GC_ACTIVITY_BUFFER: Round = 10;

/// Maximum number of batch digests a single primary `Header` may reference.
///
/// The proposer caps its own headers at the configured `max_header_num_of_batches`, and
/// `Header::validate` rejects any inbound header that exceeds this protocol ceiling, so the
/// per-header batch count is a genuine consensus invariant rather than a proposer-only convention.
/// Together with [`MAX_GC_DEPTH`] and the committee size it bounds the number of unique batches a
/// committed `ConsensusOutput` can contain, which the consensus-pack reader relies on to
/// reconstruct every executed output.
pub const MAX_HEADER_NUM_OF_BATCHES: usize = 10;

/// The round number.
/// Becomes the lower 32 bits of a nonce (with epoch the high bits).
pub type Round = u32;

/// The epoch UNIX timestamp in seconds.
pub type TimestampSec = u64;

/// A UNIX timestamp in milliseconds.
///
/// This is an in-memory type only: it deliberately has no serde implementation, so it cannot be
/// serialized directly into a network message or database record.  Encoded structures carry a
/// [`TimestampSec`] plus a separate `u16` millisecond field instead.  Storing milliseconds in an
/// existing seconds field would still decode on a binary that predates the change and be misread
/// by a factor of 1000; an added field makes such a binary fail to decode loudly instead.
///
/// Construction and addition saturate at `u64::MAX` instead of wrapping or panicking.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampMs(u64);

impl TimestampMs {
    /// Creates a timestamp from a count of milliseconds since the UNIX epoch.
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms)
    }

    /// Creates a timestamp from whole seconds and a millisecond offset within that second.
    ///
    /// The result saturates at `u64::MAX`.  `millis` is not range-checked: a value above 999
    /// carries into the seconds, so callers decoding untrusted input must reject out-of-range
    /// values before calling this.
    pub fn from_parts(secs: TimestampSec, millis: u16) -> Self {
        Self(secs.saturating_mul(1000).saturating_add(u64::from(millis)))
    }

    /// Returns the number of milliseconds since the UNIX epoch.
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns the whole seconds since the UNIX epoch, rounded down.
    pub const fn secs(self) -> TimestampSec {
        self.0 / 1000
    }

    /// Returns the millisecond offset within the timestamp's second, always in `0..1000`.
    pub const fn subsec_millis(self) -> u16 {
        // the remainder is below 1000, so the cast cannot truncate
        (self.0 % 1000) as u16
    }

    /// Returns the time elapsed between this timestamp and [`now_ms`].
    ///
    /// A timestamp in the future yields [`Duration::ZERO`].
    pub fn elapsed(self) -> Duration {
        Duration::from_millis(now_ms().0.saturating_sub(self.0))
    }

    /// Returns this timestamp advanced by `ms` milliseconds, saturating at `u64::MAX`.
    pub fn saturating_add_millis(self, ms: u64) -> Self {
        Self(self.0.saturating_add(ms))
    }
}

impl fmt::Display for TimestampMs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

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

impl Timestamp for TimestampMs {
    fn elapsed(&self) -> Duration {
        // path syntax resolves to the inherent method, not back into this trait method
        TimestampMs::elapsed(*self)
    }
}

/// Returns the current time expressed as UNIX
/// timestamp in seconds.
///
/// Computed as `now_ms().secs()`, so seconds and milliseconds readings share one clock source and
/// the same floor rounding.
pub fn now() -> TimestampSec {
    now_ms().secs()
}

/// Returns the current time expressed as UNIX timestamp in milliseconds.
pub fn now_ms() -> TimestampMs {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        // a u64 holds epoch milliseconds for roughly 584 million years, so the cast cannot
        // truncate in practice
        Ok(n) => TimestampMs::from_millis(n.as_millis() as u64),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_ms_splits_into_secs_and_subsec_millis() {
        let t = TimestampMs::from_millis(3999);
        assert_eq!(t.secs(), 3);
        assert_eq!(t.subsec_millis(), 999);
        assert_eq!(TimestampMs::from_parts(3, 999).as_millis(), 3999);
    }

    #[test]
    fn timestamp_ms_saturates_instead_of_overflowing() {
        // overflow in the seconds multiplication
        assert_eq!(TimestampMs::from_parts(u64::MAX, 999).as_millis(), u64::MAX);
        // multiplication fits, the millisecond addition overflows
        assert_eq!(TimestampMs::from_parts(u64::MAX / 1000, 999).as_millis(), u64::MAX);
        assert_eq!(
            TimestampMs::from_millis(1).saturating_add_millis(u64::MAX).as_millis(),
            u64::MAX
        );
    }

    #[test]
    fn timestamp_ms_round_trips_through_parts() {
        for ms in [0, 999, 1000, 3999, u64::MAX - 1, u64::MAX] {
            let t = TimestampMs::from_millis(ms);
            assert_eq!(TimestampMs::from_parts(t.secs(), t.subsec_millis()), t, "ms = {ms}");
        }
    }

    #[test]
    fn timestamp_now_is_seconds_of_now_ms() {
        // bracketing with two millisecond reads tolerates the second rolling over between calls
        let before = now_ms().secs();
        let secs = now();
        let after = now_ms().secs();
        assert!(before <= secs && secs <= after, "{before} <= {secs} <= {after}");
    }

    #[test]
    fn timestamp_ms_display_prints_raw_millis() {
        assert_eq!(TimestampMs::from_millis(3999).to_string(), "3999");
    }

    #[test]
    fn timestamp_ms_orders_by_millis() {
        assert!(TimestampMs::from_millis(1000) < TimestampMs::from_millis(1001));
        assert!(TimestampMs::from_parts(1, 999) < TimestampMs::from_parts(2, 0));
    }

    #[test]
    fn timestamp_ms_elapsed_saturates_for_future() {
        let future = now_ms().saturating_add_millis(60_000);
        assert_eq!(future.elapsed(), Duration::ZERO);
        assert_eq!(Timestamp::elapsed(&future), Duration::ZERO);

        let past = TimestampMs::from_millis(0);
        assert!(past.elapsed() > Duration::ZERO);
        assert!(Timestamp::elapsed(&past) > Duration::ZERO);
    }
}
