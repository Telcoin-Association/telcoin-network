// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Aggregate votes after proposing a header.
use fastcrypto::hash::{Digest, Hash};
use std::{collections::HashSet, sync::Arc};
use tn_primary_metrics::PrimaryMetrics;
use tn_types::{AuthorityIdentifier, Committee, Stake};

use tn_types::{
    ensure,
    error::{DagError, DagResult},
    to_intent_message, BlsAggregateSignature, BlsSignature, Certificate, Header, ProtocolSignature,
    SignatureVerificationState, ValidatorAggregateSignature, Vote,
};
use tracing::{trace, warn};

/// Aggregates votes for a particular header to form a certificate
pub(crate) struct VotesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: Stake,
    /// The vote received from a peer.
    votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    /// The collection of authority ids that have already voted.
    authorities_seen: HashSet<AuthorityIdentifier>,
    /// Metrics for votes aggregator.
    metrics: Arc<PrimaryMetrics>,
}

impl VotesAggregator {
    /// Create a new instance of `Self`.
    pub(crate) fn new(metrics: Arc<PrimaryMetrics>) -> Self {
        metrics.votes_received_last_round.set(0);

        Self { weight: 0, votes: Vec::new(), authorities_seen: HashSet::new(), metrics }
    }

    /// Append the vote to the collection.
    ///
    /// This method protects against equivocation by keeping track of peers that have already voted.
    pub(crate) fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        // ensure authority hasn't voted already
        let author = vote.author();
        ensure!(self.authorities_seen.insert(author), DagError::AuthorityReuse(author.to_string()));

        // accumulate vote and voting power
        self.votes.push((author, vote.signature().clone()));
        self.weight += committee.stake_by_id(author);

        // update metrics
        self.metrics.votes_received_last_round.set(self.votes.len() as i64);

        // check if this vote reaches quorum
        if self.weight >= committee.quorum_threshold() {
            let mut cert =
                Certificate::new_unverified(committee, header.clone(), self.votes.clone())?;
            let (_, pks) = cert.signed_by(committee);

            let certificate_digest: Digest<{ tn_types::DIGEST_LENGTH }> =
                Digest::from(cert.digest());

            // check aggregate signature verification
            if let Err(e) = BlsAggregateSignature::try_from(
                cert.aggregated_signature().ok_or(DagError::InvalidSignature)?,
            )
            .map_err(|_| DagError::InvalidSignature)?
            .verify_secure(&to_intent_message(certificate_digest), &pks[..])
            {
                warn!(
                    target: "primary::votes_aggregator",
                    ?e,
                    ?certificate_digest,
                    "Failed to verify aggregated sig on certificate",
                );
                self.votes.retain(|(id, sig)| {
                    let pk = committee.authority_safe(id).protocol_key();
                    if sig.verify_secure(&to_intent_message(certificate_digest), pk).is_err() {
                        warn!(target: "primary::votes_aggregator", "Invalid signature on header from authority: {}", id);
                        self.weight -= committee.stake(pk);
                        false
                    } else {
                        true
                    }
                });

                return Ok(None);
            } else {
                trace!(target: "primary::votes_aggregator", ?cert, "certificate verified");
                // cert signature verified
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedDirectly(
                        cert.aggregated_signature().ok_or(DagError::InvalidSignature)?.clone(),
                    ),
                );

                return Ok(Some(cert));
            }
        }
        Ok(None)
    }
}
