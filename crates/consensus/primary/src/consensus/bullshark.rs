//! Bullshark

use crate::consensus::{
    utils, ConsensusError, ConsensusMetrics, ConsensusState, Dag, LeaderSchedule, LeaderSwapTable,
    Outcome,
};
use std::{collections::VecDeque, sync::Arc};
use tn_storage::ConsensusStore;
use tn_types::{
    Certificate, CommittedSubDag, Committee, Hash as _, ReputationScores, Round, VotingPower,
};
use tokio::time::Instant;
use tracing::debug;

#[cfg(test)]
#[path = "tests/bullshark_tests.rs"]
pub mod bullshark_tests;

#[cfg(test)]
#[path = "tests/randomized_tests.rs"]
pub mod randomized_tests;

/// The consensus protocol for ordering transactions.
///
/// This implements bullshark, but also tracks weak votes for a fast-commit (based on Shoal++).
pub struct Bullshark<DB> {
    /// The committee information.
    pub committee: Committee,
    /// Persistent storage to safe ensure crash-recovery.
    pub store: DB,
    /// The most recent round of inserted certificate
    pub max_inserted_certificate_round: Round,
    pub metrics: Arc<ConsensusMetrics>,
    /// The last time we had a successful leader election
    pub last_successful_leader_election_timestamp: Instant,
    /// The number of committed subdags that will trigger the schedule change and reputation
    /// score reset.
    pub num_sub_dags_per_schedule: u32,
    /// The leader election schedule to be used when need to find a round's leader
    pub leader_schedule: LeaderSchedule,
    /// The bad node stake threshold for [LeaderSwapBoard].
    pub bad_nodes_stake_threshold: u64,
}

impl<DB: ConsensusStore> Bullshark<DB> {
    /// Create a new Bullshark consensus instance.
    pub fn new(
        committee: Committee,
        store: DB,
        metrics: Arc<ConsensusMetrics>,
        num_sub_dags_per_schedule: u32,
        leader_schedule: LeaderSchedule,
        bad_nodes_stake_threshold: u64,
    ) -> Self {
        Self {
            committee,
            store,
            last_successful_leader_election_timestamp: Instant::now(),
            max_inserted_certificate_round: 0,
            metrics,
            num_sub_dags_per_schedule,
            leader_schedule,
            bad_nodes_stake_threshold,
        }
    }

    /// Calculates the reputation score for the current commit by taking into account the reputation
    /// scores from the previous commit (assuming that exists). It returns the updated reputation
    /// score.
    fn resolve_reputation_score(
        &self,
        state: &mut ConsensusState,
        committed_sequence: &[Certificate],
        sub_dag_index: u64,
    ) -> ReputationScores {
        // we reset the scores for every schedule change window, or initialise when it's the first
        // sub dag we are going to create.
        // TODO: when schedule change is implemented we should probably change a little bit
        // this logic here.
        // sub_dag_index is based on epoch and round so / 2 so this check works on commit rounds
        // (every other one).
        let mut reputation_score =
            if (sub_dag_index / 2) % self.num_sub_dags_per_schedule as u64 == 0 {
                ReputationScores::new(&self.committee)
            } else if let Some(last) = state.last_committed_sub_dag.as_ref() {
                last.reputation_score.clone()
            } else {
                ReputationScores::new(&self.committee)
            };

        // update the score for the previous leader. If no previous leader exists,
        // then this is the first time we commit a leader, so no score update takes place
        if let Some(last_committed_sub_dag) = state.last_committed_sub_dag.as_ref() {
            let leader_digest = last_committed_sub_dag.leader.digest();
            let leader_round = last_committed_sub_dag.leader.round();
            for certificate in committed_sequence.iter().filter(|c| c.round() == leader_round + 1) {
                if certificate.header().parents().contains(&leader_digest) {
                    reputation_score.add_score(certificate.origin(), 1);
                }
            }
        }

        // we check if this is the last sub dag of the current schedule. If yes then we mark the
        // scores as final_of_schedule = true so any downstream user can now that those are the last
        // ones calculated for the current schedule.
        // sub_dag_index is based on epoch and round so / 2 so this check works on commit rounds
        // (every other one).
        reputation_score.final_of_schedule =
            ((sub_dag_index / 2) + 1) % self.num_sub_dags_per_schedule as u64 == 0;

        // Always ensure that all the authorities are present in the reputation scores - even
        // when score is zero.
        assert_eq!(reputation_score.total_authorities() as usize, self.committee.size());

        reputation_score
    }

    /// Process a new certificate.
    pub fn process_certificate(
        &mut self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        debug!("Processing {:?}", certificate);
        let round = certificate.round();

        // Add the new certificate to the local storage.
        if !state.try_insert(&certificate)? {
            // Certificate has not been added to the dag since it's below commit round
            return Ok((Outcome::CertificateBelowCommitRound, vec![]));
        }

        self.report_leader_on_time_metrics(round, state);

        // Try to order the dag to commit. Start from the highest round for which we have at least
        // f+1 certificates. This is because we need them to provide
        // enough support to the leader.
        let r = round - 1;

        // We only elect leaders for even round numbers.
        if r % 2 != 0 || r < 2 {
            return Ok((Outcome::NoLeaderElectedForOddRound, Vec::new()));
        }

        // Get the certificate's digest of the leader. If we already ordered this leader,
        // there is nothing to do.
        let leader_round = r;
        if leader_round <= state.last_round.committed_round {
            return Ok((Outcome::LeaderBelowCommitRound, Vec::new()));
        }

        let mut committed_sub_dags = Vec::new();
        let outcome = loop {
            let (outcome, committed) = self.process_leader_round(leader_round, state)?;

            // always extend the returned sub dags
            committed_sub_dags.extend(committed);

            // break the loop and return the result as long as there is no schedule change.
            // We want to retry if there is a schedule change.
            if outcome != Outcome::ScheduleChanged {
                break outcome;
            }
        };

        // If we have no sub dag to commit then we simply return the outcome directly.
        // Otherwise we let the rest of the method run.
        if committed_sub_dags.is_empty() {
            return Ok((outcome, committed_sub_dags));
        }

        // record the last time we got a successful leader election
        let elapsed = self.last_successful_leader_election_timestamp.elapsed();

        self.metrics.commit_rounds_latency.observe(elapsed.as_secs_f64());

        self.last_successful_leader_election_timestamp = Instant::now();

        // The total leader_commits are expected to grow the same amount on validators,
        // but strong vs weak counts are not expected to be the same across validators.
        self.metrics.leader_commits.with_label_values(&["strong"]).inc();
        self.metrics
            .leader_commits
            .with_label_values(&["weak"])
            .inc_by(committed_sub_dags.len() as u64 - 1);

        // Log the latest committed round of every authority (for debug).
        // Performance note: if tracing at the debug log level is disabled, this is cheap, see
        // https://github.com/tokio-rs/tracing/pull/326
        for (name, round) in &state.last_committed {
            debug!("Latest commit of {}: Round {}", name, round);
        }

        let total_committed_certificates: u64 =
            committed_sub_dags.iter().map(|sub_dag| sub_dag.certificates.len() as u64).sum();

        self.metrics.committed_certificates.report(total_committed_certificates);

        Ok((Outcome::Commit, committed_sub_dags))
    }

    /// Commits the leader of round `leader_round`. It is also recursively committing any earlier
    /// leader that hasn't been committed, assuming that's possible.
    /// If the schedule has changed due to a commit and there are more leaders to commit, then this
    /// method will return the enum `ScheduleChanged` so the caller will know to retry for the
    /// uncommitted leaders with the updated schedule now.
    fn process_leader_round(
        &mut self,
        leader_round: Round,
        state: &mut ConsensusState,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        let leader = match self.leader_schedule.leader_certificate(leader_round, &state.dag) {
            (_leader_authority, Some(certificate)) => certificate.clone(),
            (_leader_authority, None) => {
                // leader has not been found
                return Ok((Outcome::LeaderNotFound, vec![]));
            }
        };

        // Check if the leader has f+1 support from its children (ie. leader_round+1).
        let leader_digest = leader.digest();
        let voting_power: VotingPower = state
            .dag
            .get(&(leader_round + 1))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header().parents().contains(&leader_digest))
            .map(|(_, x)| self.committee.voting_power_by_id(x.origin()))
            .sum();

        if voting_power < self.committee.validity_threshold() {
            debug!("Leader {:?} does not have enough support", leader);
            return Ok((Outcome::NotEnoughSupportForLeader, vec![]));
        }

        // Get an ordered list of past leaders that are linked to the current leader.
        debug!("Leader {:?} has enough support", leader);

        self.commit_leader(&leader, state)
    }

    /// Order the uncommitted leaders from the oldest to newest round.
    fn order_leaders(&self, leader: &Certificate, state: &ConsensusState) -> VecDeque<Certificate> {
        let mut to_commit = VecDeque::new();
        to_commit.push_front(leader.clone());

        let mut leader = leader;
        assert_eq!(leader.round() % 2, 0);
        for r in (state.last_round.committed_round + 2..=leader.round() - 2).rev().step_by(2) {
            // Get the certificate proposed by the previous leader.
            let (prev_leader, authority) =
                match self.leader_schedule.leader_certificate(r, &state.dag) {
                    (authority, Some(x)) => (x, authority),
                    (authority, None) => {
                        self.metrics
                            .leader_election
                            .with_label_values(&["not_found", &authority.id().to_string()])
                            .inc();

                        continue;
                    }
                };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                // always add on the front so in the end we create a list with the leaders ordered
                // from the lowest to the highest round.
                to_commit.push_front(prev_leader.clone());
                leader = prev_leader;
            } else {
                self.metrics
                    .leader_election
                    .with_label_values(&["no_path", &authority.id().to_string()])
                    .inc();
            }
        }

        // Now just report all the found leaders
        let committee = self.committee.clone();
        let metrics = self.metrics.clone();

        to_commit.iter().for_each(|certificate| {
            let authority = committee
                .authority(certificate.origin())
                .expect("verified certificate signed by authority in committee");

            metrics
                .leader_election
                .with_label_values(&["committed", &authority.id().to_string()])
                .inc();
        });

        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = if let Some(r) = dag.get(&r) {
                r.values()
                    .filter(|(digest, _)| {
                        parents.iter().any(|x| x.header().parents().contains(digest))
                    })
                    .map(|(_, certificate)| certificate)
                    .collect()
            } else {
                vec![]
            };
        }
        parents.contains(&prev_leader)
    }

    /// When the provided `reputation_scores` are "final" for the current schedule window, then we
    /// create the new leader swap table and update the leader schedule to use it. Otherwise we do
    /// nothing. If the schedule has been updated then true is returned.
    fn update_leader_schedule(
        &mut self,
        leader_round: Round,
        reputation_scores: &ReputationScores,
    ) -> bool {
        if reputation_scores.final_of_schedule {
            // create the new swap table and update the scheduler
            self.leader_schedule.update_leader_swap_table(LeaderSwapTable::new(
                &self.committee,
                leader_round,
                reputation_scores,
                self.bad_nodes_stake_threshold,
            ));

            self.metrics.num_of_bad_nodes.set(self.leader_schedule.num_of_bad_nodes() as i64);

            true
        } else {
            false
        }
    }

    fn report_leader_on_time_metrics(&mut self, certificate_round: Round, state: &ConsensusState) {
        if certificate_round > self.max_inserted_certificate_round
            && certificate_round % 2 == 0
            && certificate_round > 2
        {
            let previous_leader_round = certificate_round - 2;

            // This metric reports the leader election success for the last leader election round.
            // Our goal is to identify the rate of missed/failed leader elections which are a source
            // of tx latency. The metric's authority label can not be considered fully accurate when
            // we do change schedule as we'll try to calculate the previous leader round by using
            // the updated scores and consequently the new swap table. If the leader for
            // that position has changed, then a different hostname will be erroneously
            // reported. For now not a huge issue as it will be affect either:
            // * only the round where we switch schedules
            // * on long periods of asynchrony where we end up changing schedules late
            // and we don't really expect it to happen frequently.
            let authority = self.leader_schedule.leader(previous_leader_round);

            if state.last_round.committed_round < previous_leader_round {
                self.metrics
                    .leader_commit_accuracy
                    .with_label_values(&["miss", &authority.id().to_string()])
                    .inc();
            } else {
                self.metrics
                    .leader_commit_accuracy
                    .with_label_values(&["hit", &authority.id().to_string()])
                    .inc();
            }
        }

        self.max_inserted_certificate_round =
            self.max_inserted_certificate_round.max(certificate_round);
    }

    /// Try to commit leaders using the Fast Direct Commit rule (2f+1 weak votes)
    pub fn try_fast_commit(
        &mut self,
        state: &mut ConsensusState,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        // get max round from the DAG directly
        let max_round = state.dag.keys().max().copied().unwrap_or(0);

        // check each potential leader round starting from last committed + 2
        let start_round = state.last_round.committed_round + 2;

        for leader_round in (start_round..=max_round).step_by(2) {
            if leader_round % 2 != 0 {
                continue; // leaders are only on even rounds
            }

            // get the leader for this round
            let leader = self.leader_schedule.leader(leader_round);

            // check if we have enough weak votes for fast commit
            if state.can_fast_commit(leader_round, &leader, &self.committee) {
                debug!(leader_round, ?leader, "Enough weak votes to fast commit");
                // try to get the leader's certificate
                let leader_certificate =
                    match self.leader_schedule.leader_certificate(leader_round, &state.dag) {
                        (_authority, Some(certificate)) => Some(certificate.clone()), // Clone the certificate
                        (_authority, None) => None,
                    };

                if let Some(certificate) = leader_certificate {
                    debug!("Fast committing leader at round {} with 2f+1 weak votes", leader_round);

                    // record fast commit in metrics
                    self.metrics
                        .leader_election
                        .with_label_values(&["fast_commit", &leader.id().to_string()])
                        .inc();

                    // TODO: should this be in a loop as well like `process_leader_round`?
                    // fast commit this leader
                    // We already verified 2f+1 weak votes, so we can skip the f+1 certificate check
                    debug!("Fast committing leader {:?} with 2f+1 weak votes", leader);
                    let (outcome, sub_dags) = self.commit_leader(&certificate, state)?;

                    if outcome == Outcome::Commit {
                        // Update metrics for successful fast commit
                        self.last_successful_leader_election_timestamp = Instant::now();
                        return Ok((outcome, sub_dags));
                    }
                } else {
                    // leader certificate not yet in dag, can't commit yet
                    debug!(
                        "Leader at round {} has weak votes but certificate not in DAG yet",
                        leader_round
                    );
                }
            }
        }

        Ok((Outcome::NoLeaderElectedForOddRound, Vec::new()))
    }

    // 214
    // 313
    // 460
    /// Commit the leader that either has f+1 certificates or 2f+1 weak votes.
    fn commit_leader(
        &mut self,
        leader: &Certificate,
        state: &mut ConsensusState,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        let mut committed_sub_dags = Vec::new();
        let mut leaders_to_commit = self.order_leaders(leader, state);

        while let Some(leader) = leaders_to_commit.pop_front() {
            let sub_dag_index = leader.nonce();
            let mut min_round = leader.round();
            let mut sequence = Vec::new();

            // flatten the sub-dag referenced by the leader
            for x in utils::order_dag(&leader, state) {
                state.update(&x);
                min_round = min_round.min(x.round());
                sequence.push(x);
            }

            debug!(min_round, "Fast commit subdag has {} certificates", sequence.len());

            // resolve reputation score
            let reputation_score = self.resolve_reputation_score(state, &sequence, sub_dag_index);

            let leader_round = leader.round();
            let sub_dag = CommittedSubDag::new(
                sequence,
                leader,
                sub_dag_index,
                reputation_score.clone(),
                state.last_committed_sub_dag.as_ref(),
            );

            state.last_committed_sub_dag = Some(sub_dag.clone());
            committed_sub_dags.push(sub_dag);

            if self.update_leader_schedule(leader_round, &reputation_score) {
                if !leaders_to_commit.is_empty() {
                    return Ok((Outcome::ScheduleChanged, committed_sub_dags));
                }
            }
        }

        Ok((Outcome::Commit, committed_sub_dags))
    }
}
