//! Aggregate certificates for the round.

use crate::{error::CertManagerResult, ConsensusBus};
use std::collections::{BTreeMap, HashSet};
use tn_types::{AuthorityIdentifier, Certificate, Committee, Round, TnSender as _, VotingPower};
use tracing::trace;

/// Manage certificates as they aggregate through rounds.
#[derive(Debug, Clone)]
pub(crate) struct CertificatesAggregatorManager {
    /// Collection of [CertificatesAggregator]s.
    aggregators: BTreeMap<Round, Box<CertificatesAggregator>>,
    /// Consensus bus to forward parents for a round to the proposer.
    consensus_bus: ConsensusBus,
}

impl CertificatesAggregatorManager {
    /// Create a new instance of self with allocation for the max gc-depth.
    pub(crate) fn new(consensus_bus: ConsensusBus) -> Self {
        Self { aggregators: BTreeMap::new(), consensus_bus }
    }

    /// Append a certificate by round and alert proposer if quorum is reached (2f+1).
    pub(crate) async fn append_certificate(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> CertManagerResult<()> {
        let round = certificate.round();

        // append certificate
        let quorum = self
            .aggregators
            .entry(round)
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate, committee);

        // forward to proposer if enough parents to advance the round (2f+1)
        if let Some(parents) = quorum {
            self.consensus_bus.parents().send((parents, round)).await?;
        }

        Ok(())
    }

    /// Process the next gc round and remove old parents that can never be accepted in the DAG.
    pub(crate) fn garbage_collect(&mut self, gc_round: &Round) {
        self.aggregators.retain(|k, _| k > gc_round);
    }
}

/// Aggregate certificates until quorum is reached
#[derive(Debug, Clone)]
struct CertificatesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: VotingPower,
    /// The certificates aggregated for this round.
    certificates: Vec<Certificate>,
    /// The collection of authority ids that have already voted.
    authorities_seen: HashSet<AuthorityIdentifier>,
}

impl CertificatesAggregator {
    /// Create a new instance of `Self`.
    fn new() -> Self {
        Self { weight: 0, certificates: Vec::new(), authorities_seen: HashSet::new() }
    }

    /// Append the certificate to the collection.
    ///
    /// This method protects against equivocation by keeping track of peers that have already issued
    /// certificates.
    fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> Option<Vec<Certificate>> {
        let origin = certificate.origin().clone();

        // ensure authority hasn't issued certificate already
        if !self.authorities_seen.insert(origin.clone()) {
            return None;
        }

        // accumulate certificates and voting power
        self.certificates.push(certificate);
        self.weight += committee.voting_power_by_id(&origin);

        // check for quorum
        if self.weight >= committee.quorum_threshold() {
            trace!(target: "primary::certificate_aggregator", "quorum reached");
            // NOTE: do not reset the weight here
            //
            // this method could be called again if the proposer doesn't
            // advance the round
            return Some(self.certificates.drain(..).collect());
        }

        None
    }
}
