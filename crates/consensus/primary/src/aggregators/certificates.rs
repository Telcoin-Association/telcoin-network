// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Aggregate certificates for the round.
use fastcrypto::hash::{Digest, Hash};
use narwhal_primary_metrics::PrimaryMetrics;
use std::{collections::HashSet, sync::Arc};
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    to_intent_message, BlsAggregateSignature, BlsSignature, Certificate, Header,
    SignatureVerificationState, ValidatorAggregateSignature, ValidatorSignature, Vote,
};
use tn_types::{AuthorityIdentifier, Committee, Stake};
use tracing::warn;

/// Aggregate certificates until quorum is reached
pub struct CertificatesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: Stake,
    /// The certificates aggregated for this round.
    certificates: Vec<Certificate>,
    ///
    authorities_seen: HashSet<AuthorityIdentifier>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self { weight: 0, certificates: Vec::new(), authorities_seen: HashSet::new() }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> Option<Vec<Certificate>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.authorities_seen.insert(origin) {
            return None;
        }

        self.certificates.push(certificate);
        self.weight += committee.stake_by_id(origin);
        if self.weight >= committee.quorum_threshold() {
            // Note that we do not reset the weight here. If this function is called again and
            // the proposer didn't yet advance round, we can add extra certificates as parents.
            // This is required when running Bullshark as consensus.
            return Some(self.certificates.drain(..).collect());
        }
        None
    }
}
