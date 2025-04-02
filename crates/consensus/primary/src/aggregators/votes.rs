//! Aggregate votes after proposing a header.

use std::{collections::HashSet, sync::Arc};
use tn_primary_metrics::PrimaryMetrics;
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    to_intent_message, AuthorityIdentifier, BlsAggregateSignature, BlsSignature, Certificate,
    Committee, Digest, Hash as _, Header, ProtocolSignature, SignatureVerificationState,
    ValidatorAggregateSignature, Vote, VotingPower,
};
use tracing::{trace, warn};

/// Aggregates votes for a particular header to form a certificate
pub(crate) struct VotesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: VotingPower,
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
        ensure!(
            self.authorities_seen.insert(author.clone()),
            DagError::AuthorityReuse(author.to_string())
        );

        // accumulate vote and voting power
        self.votes.push((author.clone(), *vote.signature()));
        self.weight += committee.voting_power_by_id(author);

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
            if !BlsAggregateSignature::from_signature(
                &cert.aggregated_signature().ok_or(DagError::InvalidSignature)?,
            )
            .verify_secure(&to_intent_message(certificate_digest), &pks[..])
            {
                warn!(
                    target: "primary::votes_aggregator",
                    ?certificate_digest,
                    "Failed to verify aggregated sig on certificate",
                );
                self.votes.retain(|(id, sig)| {
                    if let Some(auth) = committee.authority(id) {
                        let pk = auth.protocol_key();
                        if !sig.verify_secure(&to_intent_message(certificate_digest), pk) {
                            warn!(target: "primary::votes_aggregator", "Invalid signature on header from authority: {}", id);
                            self.weight -= committee.voting_power(pk);
                            false
                        } else {
                            true
                        }
                    } else {
                        false
                    }
                });

                return Ok(None);
            } else {
                trace!(target: "primary::votes_aggregator", ?cert, "certificate verified");
                // cert signature verified
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedDirectly(
                        cert.aggregated_signature().ok_or(DagError::InvalidSignature)?,
                    ),
                );

                return Ok(Some(cert));
            }
        }
        Ok(None)
    }
}
