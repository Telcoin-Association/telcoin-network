//! Consensus

mod bullshark;
mod leader_schedule;
mod state;
mod utils;
pub use crate::consensus::{
    bullshark::Bullshark,
    leader_schedule::{LeaderSchedule, LeaderSwapTable},
    state::{Consensus, ConsensusRound, ConsensusState, Dag},
    utils::gc_round,
};
use thiserror::Error;
use tn_storage::StoreError;
use tn_types::{Certificate, HeaderDigest, Round};

/// The default channel size used in the consensus and subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 25;

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Certificate {0:?} equivocates with earlier certificate {1:?}")]
    CertificateEquivocation(Box<Certificate>, Box<Certificate>),

    #[error("System shutting down")]
    ShuttingDown,

    #[error("Parent digest {0:?} not found in DAG for {1:?}!")]
    MissingParent(HeaderDigest, Box<Certificate>),

    #[error("Parent round not found in DAG for {0:?}!")]
    MissingParentRound(Box<Certificate>),

    /// A Bullshark construction invariant (or a startup-recovery self-consistency
    /// invariant) did not hold. None of these conditions is reachable under the `< f`
    /// fault assumption on current `main`; converting the former always-active
    /// `assert!`/`.expect` panics into this typed error turns a would-be node panic
    /// (with a low-context backtrace) into a diagnosable, structured shutdown.
    #[error("Consensus invariant violated: {0}")]
    Invariant(#[from] ConsensusInvariant),
}

/// A violated internal construction invariant of the Bullshark commit hot path or of
/// startup recovery.
///
/// Each variant records the operands that disagreed so a future regression is
/// diagnosable from the error alone, rather than from a bare `assert_eq!` backtrace.
/// These are defense-in-depth: on current `main` none is reachable under the standard
/// `< f` Byzantine fault assumption.
#[derive(Debug, Error)]
pub enum ConsensusInvariant {
    /// The reputation-score map did not carry exactly the committee's authorities.
    #[error("reputation score authority count {actual} does not match committee size {expected}")]
    ReputationAuthorityCount {
        /// Number of authorities present in the reputation-score map.
        actual: usize,
        /// Number of authorities in the current committee.
        expected: usize,
    },

    /// The child round of a committable leader was absent from the in-memory DAG, so
    /// the leader's support could not be tallied.
    #[error("missing round {0} in DAG while tallying leader support (expected full history)")]
    MissingLeaderChildRound(Round),

    /// A leader was elected on an odd round; leaders are only ever elected on even rounds.
    #[error("leader round {0} is odd (leaders are elected only on even rounds)")]
    LeaderRoundNotEven(Round),

    /// On startup recovery, the recovered latest sub-dag's leader round did not equal the
    /// recovered last committed round.
    #[error(
        "recovered sub-dag leader round {leader_round} does not equal last committed round {last_committed_round}"
    )]
    RecoveredLeaderRoundMismatch {
        /// Leader round of the recovered latest sub-dag.
        leader_round: Round,
        /// Recovered last committed round.
        last_committed_round: Round,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    // Certificate is not processed, since it's below the latest committed round for its origin.
    CertificateBelowCommitRound,

    // Certificate processed is of an even round, so the previous one is an odd round and
    // no leader election takes process.
    NoLeaderElectedForOddRound,

    // Leader has been elected, but it's below the latest commit round, so commit happens.
    LeaderBelowCommitRound,

    // Tried to do a leader election, but leader was not found for the round, not commit will
    // take place.
    LeaderNotFound,

    // Leader has been found,  but there was no enough support from the children nodes, so leader
    // can't be used to commit.
    NotEnoughSupportForLeader,

    // Processed Certificate triggered a commit.
    Commit,

    // When the schedule has changed during a commit, then this is return with everything that has
    // been committed so far.
    ScheduleChanged,
}
