//! Primary types used for consensus.

use std::time::{Duration, SystemTime};
mod block;
mod certificate;
mod epoch;
mod header;
mod info;
mod node_mode;
mod output;
mod reputation;
mod vote;

pub use block::*;
pub use certificate::*;
pub use epoch::*;
pub use header::*;
pub use info::*;
pub use node_mode::*;
pub use output::*;
pub use reputation::*;
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
