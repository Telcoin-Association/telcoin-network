//! Aggregate messages from peers.

pub(crate) mod certificates;
pub(crate) mod sync;
mod votes;
pub(crate) use votes::VotesAggregator;
