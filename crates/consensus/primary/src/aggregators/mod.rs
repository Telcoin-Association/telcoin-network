//! Aggregate messages from peers.

mod certificates;
mod votes;
pub(crate) use certificates::CertificatesAggregatorManager;
pub(crate) use votes::VotesAggregator;
