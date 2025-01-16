//! Aggregate messages from peers.

mod certificates;
mod votes;
pub(crate) use certificates::CertificatesAggregator;
pub(crate) use votes::VotesAggregator;
