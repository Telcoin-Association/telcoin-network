//! Aggregate votes after proposing a header.

use parking_lot::Mutex;
use std::{collections::HashSet, fmt, sync::Arc, time::Instant};
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    to_intent_message, AuthorityIdentifier, BlsSignature, Certificate, Committee, Header,
    ProtocolSignature, SignatureVerificationState, Vote, VotingPower,
};
use tracing::{info, instrument, trace};

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
    /// Each counted vote updates the tally of `vote_round`; the vote that reaches quorum ends the
    /// round, and the `quorum reached` line carries the round's answers.
    #[instrument(level = "debug", skip_all, fields(voter = ?vote.author(), weight = self.weight))]
    pub(crate) fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
        vote_round: &VoteRound,
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
        vote_round.set_tally(self.verified_votes.len(), self.weight);

        // check if this vote reaches quorum
        if self.weight >= committee.quorum_threshold() {
            let mut cert = Certificate::new_unverified(
                committee,
                header.clone(),
                self.verified_votes.clone(),
            )?;

            // the vote that reaches quorum ends the vote round
            let answers = vote_round.finish();
            // Metric: quorum_reached - tracks when vote quorum is achieved
            info!(
                target: "consensus::metrics",
                round = header.round(),
                epoch = header.epoch(),
                voting_power = self.weight,
                quorum_threshold = committee.quorum_threshold(),
                num_votes = self.verified_votes.len(),
                vote_arrivals = %answers.arrivals,
                vote_errors = %answers.errors,
                votes_at_end = answers.votes,
                stake_at_end = answers.stake,
                "quorum reached"
            );

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

/// The answers the author of a header received during its vote round, kept for the line that ends
/// the round: `quorum reached`, or `header uncertified` for a round given up.
///
/// A cheap handle: the round's vote request tasks share one copy. Time zero is the instant
/// [`Self::start`] marks just before the round's vote requests are spawned, read on the monotonic
/// clock; a request issued again later in the round is still timed from that instant. Each voter
/// has at most one entry per list, so both lists are bounded by the committee size. Answers
/// recorded after [`Self::finish`] are not listed: the round has ended.
#[derive(Clone, Debug, Default)]
pub(crate) struct VoteRound {
    /// The answers, behind a lock that only this round's request tasks take, each briefly.
    answers: Arc<Mutex<VoteRoundAnswers>>,
}

impl VoteRound {
    /// The handle that lists the answers of `voter` in this round.
    pub(crate) fn voter(&self, voter: AuthorityIdentifier) -> RoundVoter {
        RoundVoter { round: self.clone(), voter }
    }

    /// Mark time zero: the round's vote requests are about to be spawned.
    pub(crate) fn start(&self) {
        self.answers.lock().started = Instant::now();
    }

    /// List the vote `voter` sent, timed now.
    pub(crate) fn vote(&self, voter: &AuthorityIdentifier) {
        let mut answers = self.answers.lock();
        let ms = answers.elapsed_ms();
        answers.record_vote(voter, ms);
    }

    /// List an answer from `voter` that was not a vote, under the outcome code `kind`, timed now.
    pub(crate) fn answer(&self, voter: &AuthorityIdentifier, kind: &'static str) {
        let mut answers = self.answers.lock();
        let ms = answers.elapsed_ms();
        answers.record_error(voter, ms, kind);
    }

    /// Record the votes, this node's own included, and the voting power counted so far.
    fn set_tally(&self, votes: usize, stake: VotingPower) {
        let mut answers = self.answers.lock();
        if !answers.ended {
            answers.votes = votes;
            answers.stake = stake;
        }
    }

    /// End the round and copy out its answers and tally. Later answers are not listed, and a second
    /// call copies the same answers.
    ///
    /// The lock is held only for the copy (each list has at most one entry per voter); the lists are
    /// rendered when the line that ends the round is written, and not at all when it is disabled.
    pub(crate) fn finish(&self) -> VoteRoundEnd {
        let mut answers = self.answers.lock();
        answers.ended = true;
        VoteRoundEnd {
            arrivals: VoteArrivals(answers.arrivals.clone()),
            errors: VoteErrors(answers.errors.clone()),
            votes: answers.votes,
            stake: answers.stake,
        }
    }
}

/// One voter's handle on a [`VoteRound`]: it lists that voter's answers.
#[derive(Clone, Debug)]
pub(crate) struct RoundVoter {
    /// The round the answers are listed in.
    round: VoteRound,
    /// The voter whose answers are listed.
    voter: AuthorityIdentifier,
}

impl RoundVoter {
    /// The voter whose answers this handle lists.
    pub(crate) fn id(&self) -> &AuthorityIdentifier {
        &self.voter
    }

    /// List the voter's vote, timed now.
    pub(crate) fn vote(&self) {
        self.round.vote(&self.voter);
    }

    /// List an answer of the voter that was not a vote, under the outcome code `kind`, timed now.
    pub(crate) fn answer(&self, kind: &'static str) {
        self.round.answer(&self.voter, kind);
    }
}

/// A vote round's answers, copied for the line that ends the round; each list renders through
/// `Display`, so a disabled line renders nothing.
///
/// A voter is rendered as its full authority id (base58, which has no `:` or `,`).
#[derive(Debug)]
pub(crate) struct VoteRoundEnd {
    /// `[voter:ms,…]`: every voter whose vote reached this node before the round ended, in arrival
    /// order, with the milliseconds since the round's requests were sent; `[]` if none.
    pub(crate) arrivals: VoteArrivals,
    /// `[voter:first_ms:last_ms:kind:count,…]`: every voter that answered other than with a vote
    /// before the round ended, in the order of its first such answer, with the milliseconds of its
    /// first and latest such answer, the outcome code of the latest and how many there were; `[]`
    /// if none.
    pub(crate) errors: VoteErrors,
    /// The votes the round counted, this node's own included; 0 for a round that never started.
    pub(crate) votes: usize,
    /// The voting power of those votes.
    pub(crate) stake: VotingPower,
}

/// The votes of an ended round, in arrival order, rendered as `[voter:ms,…]`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VoteArrivals(Vec<(AuthorityIdentifier, u64)>);

impl fmt::Display for VoteArrivals {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (index, (voter, ms)) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{voter}:{ms}")?;
        }
        f.write_str("]")
    }
}

/// The answers other than votes of an ended round, in the order of each voter's first such answer,
/// rendered as `[voter:first_ms:last_ms:kind:count,…]`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VoteErrors(Vec<VoteRoundError>);

impl fmt::Display for VoteErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (index, listed) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(
                f,
                "{}:{}:{}:{}:{}",
                listed.voter, listed.first_ms, listed.last_ms, listed.kind, listed.count
            )?;
        }
        f.write_str("]")
    }
}

/// What a [`VoteRound`] holds behind its lock.
#[derive(Debug)]
struct VoteRoundAnswers {
    /// Time zero of the round.
    started: Instant,
    /// Set once the round's end has been rendered.
    ended: bool,
    /// The voter and milliseconds of each vote, in arrival order.
    arrivals: Vec<(AuthorityIdentifier, u64)>,
    /// One entry per voter that answered other than with a vote.
    errors: Vec<VoteRoundError>,
    /// The votes counted.
    votes: usize,
    /// The voting power counted.
    stake: VotingPower,
}

impl Default for VoteRoundAnswers {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            ended: false,
            arrivals: Vec::new(),
            errors: Vec::new(),
            votes: 0,
            stake: 0,
        }
    }
}

impl VoteRoundAnswers {
    /// Milliseconds since time zero.
    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// List the vote of `voter` at `ms`, unless the round has ended or the voter is listed.
    fn record_vote(&mut self, voter: &AuthorityIdentifier, ms: u64) {
        // a request ends with its vote and only a failed one is issued again, so a voter votes
        // once per round; the check keeps the list bounded by the committee all the same
        if self.ended || self.arrivals.iter().any(|(listed, _)| listed == voter) {
            return;
        }
        self.arrivals.push((voter.clone(), ms));
    }

    /// List an answer of `voter` other than a vote at `ms`, unless the round has ended.
    fn record_error(&mut self, voter: &AuthorityIdentifier, ms: u64, kind: &'static str) {
        if self.ended {
            return;
        }
        match self.errors.iter_mut().find(|listed| listed.voter == *voter) {
            Some(listed) => {
                listed.last_ms = ms;
                listed.kind = kind;
                listed.count = listed.count.saturating_add(1);
            }
            None => self.errors.push(VoteRoundError {
                voter: voter.clone(),
                first_ms: ms,
                last_ms: ms,
                kind,
                count: 1,
            }),
        }
    }
}

/// One voter's answers, other than votes, in a vote round.
#[derive(Clone, Debug, PartialEq)]
struct VoteRoundError {
    /// The voter.
    voter: AuthorityIdentifier,
    /// Milliseconds of its first such answer.
    first_ms: u64,
    /// Milliseconds of its latest such answer.
    last_ms: u64,
    /// The outcome code of its latest such answer.
    kind: &'static str,
    /// How many such answers it sent.
    count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_round_lists_answers_in_arrival_order() {
        let round = VoteRound::default();
        let [a, b, c] = [1, 2, 3].map(AuthorityIdentifier::dummy_for_test);
        {
            let mut answers = round.answers.lock();
            answers.record_error(&a, 5, "deferred");
            answers.record_vote(&c, 41);
            answers.record_error(&b, 60, "missing_parents");
            answers.record_error(&a, 260, "refused");
            answers.record_vote(&b, 44);
        }
        round.set_tally(3, 3);
        let end = round.finish();
        assert_eq!(end.arrivals.to_string(), format!("[{c}:41,{b}:44]"));
        assert_eq!(end.errors.to_string(), format!("[{a}:5:260:refused:2,{b}:60:60:missing_parents:1]"));
        assert_eq!((end.votes, end.stake), (3, 3));

        // the round has ended: later answers and tallies are not listed
        round.vote(&a);
        round.answer(&c, "timeout");
        round.set_tally(4, 4);
        let again = round.finish();
        assert_eq!((again.arrivals, again.errors), (end.arrivals, end.errors));
        assert_eq!((again.votes, again.stake), (3, 3));

        // a round that never started renders empty lists and no votes
        let never = VoteRound::default().finish();
        assert_eq!(format!("{} {}", never.arrivals, never.errors), "[] []");
        assert_eq!((never.votes, never.stake), (0, 0));
    }

    #[test]
    fn vote_round_lists_each_voter_once() {
        let round = VoteRound::default();
        let voters: Vec<_> = (0..10).map(AuthorityIdentifier::dummy_for_test).collect();
        {
            let mut answers = round.answers.lock();
            for (ms, voter) in (0u64..).zip(&voters) {
                for retry in 0..3 {
                    answers.record_error(voter, ms + retry * 100, "deferred");
                }
                answers.record_vote(voter, ms + 1_000);
                answers.record_vote(voter, ms + 2_000);
            }
        }
        let end = round.finish();
        let (arrivals, errors) = (end.arrivals.to_string(), end.errors.to_string());
        assert_eq!(arrivals.split(',').count(), voters.len());
        assert_eq!(errors.split(',').count(), voters.len());
        assert!(arrivals.starts_with(&format!("[{}:1000,{}:1001,", voters[0], voters[1])));
        assert!(errors.starts_with(&format!("[{}:0:200:deferred:3,", voters[0])));
        assert!(arrivals.ends_with(&format!("{}:1009]", voters[9])));
    }
}
