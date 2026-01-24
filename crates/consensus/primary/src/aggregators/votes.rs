//! Aggregate votes after proposing a header.

use std::collections::HashSet;
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    to_intent_message, AuthorityIdentifier, BlsSignature, Certificate, Committee, Header,
    ProtocolSignature, SignatureVerificationState, Vote, VotingPower,
};
use tracing::trace;

/// Aggregates votes for a particular header to form a certificate
pub(crate) struct VotesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: VotingPower,
    /// The vote received from a peer.
    verified_votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    /// The collection of authority ids that have already voted.
    authorities_seen: HashSet<AuthorityIdentifier>,
}

impl VotesAggregator {
    /// Create a new instance of `Self`.
    pub(crate) fn new() -> Self {
        Self { weight: 0, verified_votes: Vec::new(), authorities_seen: HashSet::new() }
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
        // ensure digest matches the header
        ensure!(vote.header_digest == header.digest(), DagError::InvalidHeaderDigest);
        // ensure this came from a committee member and that the signature is valid
        if let Some(auth) = committee.authority(author) {
            ensure!(
                vote.signature()
                    .verify_secure(&to_intent_message(vote.header_digest), auth.protocol_key()),
                DagError::InvalidSignature
            );
        } else {
            return Err(DagError::UnknownAuthority(author.to_string()));
        }

        // accumulate vote and voting power
        // note that we have verified the vote already so are good to save and count it
        self.verified_votes.push((author.clone(), *vote.signature()));
        self.weight += committee.voting_power_by_id(author);

        // check if this vote reaches quorum
        if self.weight >= committee.quorum_threshold() {
            let mut cert = Certificate::new_unverified(
                committee,
                header.clone(),
                self.verified_votes.clone(),
            )?;

            trace!(target: "primary::votes_aggregator", ?cert, "certificate verified");
            // cert signature verified
            cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                cert.aggregated_signature().ok_or(DagError::InvalidSignature)?,
            ));

            return Ok(Some(cert));
        }
        Ok(None)
    }
}
