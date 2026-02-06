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
use tn_types::{Certificate, CertificateDigest};

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
    MissingParent(CertificateDigest, Box<Certificate>),

    #[error("Parent round not found in DAG for {0:?}!")]
    MissingParentRound(Box<Certificate>),
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
