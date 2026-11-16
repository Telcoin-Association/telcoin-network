//! Aggregate messages from peers.

pub(crate) mod certificates;
mod votes;
pub(crate) use votes::{RoundVoter, VoteRound, VotesAggregator};
