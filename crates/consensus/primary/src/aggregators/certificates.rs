// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Aggregate certificates for the round.
use std::collections::HashSet;
use tn_types::{AuthorityIdentifier, Certificate, Committee, Stake};
use tracing::trace;

/// Aggregate certificates until quorum is reached
pub struct CertificatesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: Stake,
    /// The certificates aggregated for this round.
    certificates: Vec<Certificate>,
    /// The collection of authority ids that have already voted.
    authorities_seen: HashSet<AuthorityIdentifier>,
}

impl CertificatesAggregator {
    /// Create a new instance of `Self`.
    pub(crate) fn new() -> Self {
        Self { weight: 0, certificates: Vec::new(), authorities_seen: HashSet::new() }
    }

    /// Append the certificate to the collection.
    ///
    /// This method protects against equivocation by keeping track of peers that have already issued
    /// certificates.
    pub(crate) fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> Option<Vec<Certificate>> {
        let origin = certificate.origin();

        // ensure authority hasn't issued certificate already
        if !self.authorities_seen.insert(origin) {
            return None;
        }

        // accumulate certificates and voting power
        self.certificates.push(certificate);
        self.weight += committee.stake_by_id(origin);

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
