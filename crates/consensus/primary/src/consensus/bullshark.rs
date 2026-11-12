//! Bullshark

use crate::consensus::{
    utils, ConsensusError, ConsensusInvariant, ConsensusState, Dag, LeaderSchedule,
    LeaderSwapTable, Outcome,
};
use metrics::{counter, Counter};
use std::collections::{BTreeMap, VecDeque};
use tn_types::{
    AuthorityIdentifier, Certificate, CommittedSubDag, Committee, Hash as _, ReputationScores,
    Round, VotingPower,
};
use tokio::time::Instant;
use tracing::{debug, error_span, info, instrument};

#[cfg(test)]
#[path = "tests/bullshark_tests.rs"]
mod bullshark_tests;

#[cfg(test)]
#[path = "tests/randomized_tests.rs"]
mod randomized_tests;

/// Bullshark implementation.
#[derive(Debug)]
pub struct Bullshark {
    /// The committee information.
    pub committee: Committee,
    /// The most recent round of inserted certificate
    pub max_inserted_certificate_round: Round,
    /// The last time we had a successful leader election
    pub last_successful_leader_election_timestamp: Instant,
    /// The number of committed subdags that will trigger the schedule change and reputation
    /// score reset.
    pub num_sub_dags_per_schedule: u32,
    /// The leader election schedule to be used when need to find a round's leader
    pub leader_schedule: LeaderSchedule,
    /// The bad node stake threshold for [`LeaderSwapTable`].
    pub bad_nodes_stake_threshold: u64,
    /// Leader outcome counters, pre-resolved for every committee member.
    leader_metrics: LeaderMetrics,
}

impl Bullshark {
    /// Create a new Bullshark consensus instance.
    pub fn new(
        committee: Committee,
        num_sub_dags_per_schedule: u32,
        leader_schedule: LeaderSchedule,
        bad_nodes_stake_threshold: u64,
    ) -> Self {
        let leader_metrics = LeaderMetrics::new(&committee);
        Self {
            committee,
            last_successful_leader_election_timestamp: Instant::now(),
            max_inserted_certificate_round: 0,
            num_sub_dags_per_schedule,
            leader_schedule,
            bad_nodes_stake_threshold,
            leader_metrics,
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
    ) -> Result<ReputationScores, ConsensusError> {
        // we reset the scores for every schedule change window, or initialise when it's the first
        // sub dag we are going to create.
        // sub_dag_index is based on epoch and round so / 2 so this check works on commit rounds
        // (every other one).
        let mut reputation_score =
            if (sub_dag_index / 2).is_multiple_of(self.num_sub_dags_per_schedule as u64) {
                ReputationScores::new(&self.committee)
            } else if let Some(last) = state.last_committed_sub_dag.as_ref() {
                last.reputation_scores().clone()
            } else {
                ReputationScores::new(&self.committee)
            };

        // update the score for the previous leader. If no previous leader exists,
        // then this is the first time we commit a leader, so no score update takes place
        if let Some(last_committed_sub_dag) = state.last_committed_sub_dag.as_ref() {
            let leader_digest = last_committed_sub_dag.leader().digest();
            let leader_round = last_committed_sub_dag.leader().round();
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
            ((sub_dag_index / 2) + 1).is_multiple_of(self.num_sub_dags_per_schedule as u64);

        // Always ensure that all the authorities are present in the reputation scores - even
        // when score is zero. This is a construction invariant: every committed certificate's
        // origin is committee-checked upstream, so it holds under the `< f` fault assumption.
        // If a future regression ever breaks it, surface a typed error and shut down cleanly
        // rather than panicking on the single consensus task.
        let authorities = reputation_score.total_authorities() as usize;
        (authorities == self.committee.size()).then_some(reputation_score).ok_or_else(|| {
            ConsensusInvariant::ReputationAuthorityCount {
                actual: authorities,
                expected: self.committee.size(),
            }
            .into()
        })
    }

    #[instrument(level = "debug", skip_all, fields(round = certificate.round(), origin = ?certificate.origin()))]
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

        self.update_max_inserted_round(round);

        // Try to order the dag to commit. Start from the highest round for which we have at least
        // f+1 certificates. This is because we need them to provide
        // enough support to the leader.
        let r = round - 1;

        // We only elect leaders for even round numbers.
        if !r.is_multiple_of(2) || r < 2 {
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
            let (outcome, committed) = self.commit_leader(leader_round, state)?;

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
        self.last_successful_leader_election_timestamp = Instant::now();

        // Log the latest committed round of every authority (for debug).
        // Performance note: if tracing at the debug log level is disabled, this is cheap, see
        // https://github.com/tokio-rs/tracing/pull/326
        for (name, round) in &state.last_committed {
            debug!("Latest commit of {}: Round {}", name, round);
        }

        Ok((Outcome::Commit, committed_sub_dags))
    }

    /// Commits the leader of round `leader_round`. It is also recursively committing any earlier
    /// leader that hasn't been committed, assuming that's possible.
    /// If the schedule has changed due to a commit and there are more leaders to commit, then this
    /// method will return the enum `ScheduleChanged` so the caller will know to retry for the
    /// uncommitted leaders with the updated schedule now.
    #[instrument(level = "debug", skip_all, fields(leader_round))]
    fn commit_leader(
        &mut self,
        leader_round: Round,
        state: &mut ConsensusState,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        let leader = match self.leader_schedule.leader_certificate(leader_round, &state.dag) {
            (_leader_authority, Some(certificate)) => certificate,
            (_leader_authority, None) => {
                // leader has not been found - we don't have any certificate
                return Ok((Outcome::LeaderNotFound, vec![]));
            }
        };

        // Check if the leader has f+1 support from its children (ie. leader_round+1).
        let leader_digest = leader.digest();
        let voting_power: VotingPower = state
            .dag
            .get(&(leader_round + 1))
            .ok_or(ConsensusInvariant::MissingLeaderChildRound(leader_round + 1))?
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

        // If it is the case, we can commit the leader. But first, we need to recursively go back to
        // the last committed leader, and commit all preceding leaders in the right order.
        // Committing a leader block means committing all its dependencies.
        let mut committed_sub_dags = Vec::new();
        let mut leaders_to_commit = self.order_leaders(leader, state)?;

        while let Some(leader) = leaders_to_commit.pop_front() {
            let sub_dag_index = leader.nonce();
            let _span = error_span!("bullshark_process_sub_dag", sub_dag_index);

            debug!("Leader {:?} has enough support", leader);

            // before `state.update` moves the committed round to this leader's round
            self.record_scheduled_leaders(&leader, state);

            let mut min_round = leader.round();
            let sequence = utils::order_dag(&leader, state);

            // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
            for x in &sequence {
                // Update and clean up internal state.
                state.update(x);

                // For logging.
                min_round = min_round.min(x.round());
            }
            let num_certificates = sequence.len();
            debug!(min_round, "Subdag has {} certificates", num_certificates);

            // We resolve the reputation score that should be stored alongside with this sub dag.
            let reputation_score =
                self.resolve_reputation_score(state, &sequence, sub_dag_index)?;

            // The seed chain folds in the existing oldest-first `pop_front` commit order, which is
            // the same order the sub-dags are sent on the `sequence` channel and the same order the
            // consensus header chain records them, so every honest node folds identical inputs.
            let sub_dag: CommittedSubDag = CommittedSubDag::new_with_commit_floor(
                sequence,
                leader.clone(),
                sub_dag_index,
                reputation_score.clone(),
                state.last_committed_sub_dag.as_ref(),
                state.epoch_commit_floor(),
                state.seed_chain(),
            );

            // Metric: subdag_committed - tracks subdag commits with key metrics
            info!(
                target: "consensus::metrics",
                subdag_index = sub_dag_index,
                leader_round = leader.round(),
                num_certificates = num_certificates,
                min_round = min_round,
                committed_round = state.last_round.committed_round,
                leader = %leader.origin(),
                leader_created_at_ms = leader.header().created_at_ms().as_millis(),
                commit_timestamp_ms = sub_dag.commit_timestamp_ms().as_millis(),
                "subdag committed"
            );
            self.leader_metrics.record_commit(leader.origin());

            // Update the last sub dag
            state.last_committed_sub_dag = Some(sub_dag.clone());
            // Advance the seed chain so the next commit of this epoch folds on top of this one.
            state.set_seed_chain(sub_dag.seed_chain_value());

            committed_sub_dags.push(sub_dag);

            // If the leader schedule has been updated, then we'll need to recalculate any upcoming
            // leaders for the rest of the recursive commits. We do that by repeating the leader
            // election for the round that triggered the original commit
            if self.update_leader_schedule(leader.round(), &reputation_score) {
                // return that schedule has changed only when there are more leaders to commit
                // until, the `leader_round`, otherwise we have committed everything
                // we could and practically the leader of `leader_round` is the one
                // that changed the schedule.
                if !leaders_to_commit.is_empty() {
                    return Ok((Outcome::ScheduleChanged, committed_sub_dags));
                }
            }
        }

        Ok((Outcome::Commit, committed_sub_dags))
    }

    /// Logs and counts the scheduled leader of every leader round that committing `leader`
    /// decides, once per round.
    ///
    /// The decided rounds are the even rounds after the last committed round up to and including
    /// the leader's round. Must run before the leader's sub dag updates `state`, so the committed
    /// round still marks the previous commit. Earlier rounds in the range are skipped by this
    /// commit: their `found` reports whether the scheduled leader's certificate reached the DAG
    /// at all, which separates absent leaders from leaders that arrived but were not linked.
    /// Rounds still undecided when the epoch ends are never reported.
    fn record_scheduled_leaders(&self, leader: &Certificate, state: &ConsensusState) {
        // the smallest even round above the last committed round
        let first = state.last_round.committed_round.saturating_add(2) & !1;
        for leader_round in (first..=leader.round()).step_by(2) {
            let (authority, certificate, swapped) =
                self.leader_schedule.leader_certificate_with_swap(leader_round, &state.dag);
            let found = certificate.is_some();
            debug!(
                target: "primary::bullshark",
                leader_round,
                leader = %authority.id(),
                swapped,
                found,
                "scheduled leader"
            );
            self.leader_metrics.record_round(&authority.id(), swapped, found);
        }
    }

    /// Order the past leaders that we didn't already commit. It orders the leaders from the one
    /// of the older (smaller) round to the newest round.
    #[instrument(level = "debug", skip_all, fields(leader_round = leader.round()))]
    fn order_leaders(
        &self,
        leader: &Certificate,
        state: &ConsensusState,
    ) -> Result<VecDeque<Certificate>, ConsensusError> {
        let mut to_commit = VecDeque::new();
        to_commit.push_front(leader.clone());

        let mut leader = leader;
        // Leaders are elected only on even rounds. This is a construction invariant of the
        // schedule; surface a typed error rather than asserting so a regression degrades to a
        // clean shutdown instead of a bare `assert_eq!` backtrace on the consensus task.
        leader
            .round()
            .is_multiple_of(2)
            .then_some(())
            .ok_or(ConsensusInvariant::LeaderRoundNotEven(leader.round()))?;
        for r in (state.last_round.committed_round + 2..=leader.round() - 2).rev().step_by(2) {
            // Get the certificate proposed by the previous leader.
            let (prev_leader, _authority) =
                match self.leader_schedule.leader_certificate(r, &state.dag) {
                    (authority, Some(x)) => (x, authority),
                    (_authority, None) => {
                        continue;
                    }
                };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                // always add on the front so in the end we create a list with the leaders ordered
                // from the lowest to the highest round.
                to_commit.push_front(prev_leader.clone());
                leader = prev_leader;
            }
        }

        Ok(to_commit)
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

    // When the provided `reputation_scores` are "final" for the current schedule window, then we
    // create the new leader swap table and update the leader schedule to use it. Otherwise we do
    // nothing. If the schedule has been updated then true is returned.
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

            true
        } else {
            false
        }
    }

    fn update_max_inserted_round(&mut self, certificate_round: Round) {
        self.max_inserted_certificate_round =
            self.max_inserted_certificate_round.max(certificate_round);
    }
}

/// Leader outcome counters for one epoch's committee, keyed by authority.
///
/// Every handle is registered when [`Bullshark`] is built, so each committee member exports all
/// of its series (at zero) from the start of the epoch and recording a round does no registry
/// lookup. Label values are bs58 authority ids, bounded by the committee size.
#[derive(Debug)]
struct LeaderMetrics {
    /// The handles for each committee member.
    by_leader: BTreeMap<AuthorityIdentifier, LeaderHandles>,
}

impl LeaderMetrics {
    /// Registers the handles for every member of `committee`.
    fn new(committee: &Committee) -> Self {
        let by_leader = committee
            .authorities()
            .iter()
            .map(|authority| {
                let leader = authority.id().to_string();
                let rounds = |swapped: bool, found: bool| {
                    counter!(
                        "tn_primary.leader_rounds_total",
                        "leader" => leader.clone(),
                        "swapped" => bool_label(swapped),
                        "found" => bool_label(found),
                    )
                };
                let handles = LeaderHandles {
                    rounds: [
                        [rounds(false, false), rounds(false, true)],
                        [rounds(true, false), rounds(true, true)],
                    ],
                    commits: counter!("tn_primary.leader_commits_total", "leader" => leader.clone()),
                };
                (authority.id(), handles)
            })
            .collect();
        Self { by_leader }
    }

    /// Counts one decided leader round for `leader`.
    fn record_round(&self, leader: &AuthorityIdentifier, swapped: bool, found: bool) {
        if let Some(handles) = self.by_leader.get(leader) {
            handles.rounds[usize::from(swapped)][usize::from(found)].increment(1);
        }
    }

    /// Counts one committed sub dag led by `leader`.
    fn record_commit(&self, leader: &AuthorityIdentifier) {
        if let Some(handles) = self.by_leader.get(leader) {
            handles.commits.increment(1);
        }
    }
}

/// The metric handles for one committee member.
#[derive(Debug)]
struct LeaderHandles {
    /// `tn_primary_leader_rounds_total`, indexed by `[swapped][found]`.
    rounds: [[Counter; 2]; 2],
    /// `tn_primary_leader_commits_total`.
    commits: Counter,
}

/// Returns the static label value for a boolean outcome.
fn bool_label(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

#[cfg(test)]
mod leader_metrics_tests {
    use super::*;
    use crate::test_utils::make_optimal_certificates;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::collections::BTreeSet;
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;

    /// Counts the scheduled leader of every decided leader round and the leader of every commit.
    ///
    /// The round 2 leader is absent for rounds 1 and 2, so its round is decided as not found when
    /// round 4 commits. The round 6 round-robin authority is the one bad node of the swap table,
    /// so round 6 commits a swapped-in good node.
    #[test]
    fn leader_counters_track_rounds_and_commits() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let ids: Vec<AuthorityIdentifier> =
            committee.authorities().iter().map(|authority| authority.id()).collect();
        assert_eq!(ids.len(), 4);

        let mut scores = ReputationScores::new(&committee);
        scores.final_of_schedule = true;
        for (position, id) in ids.iter().enumerate() {
            scores.add_score(id, if position == 2 { 0 } else { 10 });
        }
        let table = LeaderSwapTable::new(&committee, 2, &scores, 33);
        let schedule = LeaderSchedule::new(committee.clone(), table);

        // the round 2 leader is missing for rounds 1 and 2, everyone is present for rounds 3 to 7
        let absent = schedule.leader(2).id();
        let present: Vec<_> = ids.iter().filter(|id| **id != absent).cloned().collect();
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let (mut certificates, parents) =
            make_optimal_certificates(&committee, 1..=2, &genesis, &present);
        let (rest, _) = make_optimal_certificates(&committee, 3..=7, &parents, &ids);
        certificates.extend(rest);

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let committed_leaders = metrics::with_local_recorder(&recorder, || {
            // built inside the closure so the pre-resolved handles bind to the local recorder
            let mut bullshark = Bullshark::new(committee.clone(), 100, schedule.clone(), 33);
            let mut state = ConsensusState::new(50);
            let mut committed_leaders = Vec::new();
            for certificate in certificates {
                let (_, sub_dags) = bullshark.process_certificate(&mut state, certificate).unwrap();
                committed_leaders.extend(sub_dags.iter().map(|d| d.leader().round()));
            }
            committed_leaders
        });
        assert_eq!(committed_leaders, vec![4, 6]);

        // (leader, swapped, found) -> count for the three decided leader rounds
        let mut expected_rounds = BTreeMap::new();
        let mut expected_commits = BTreeMap::new();
        for (round, found) in [(2, false), (4, true), (6, true)] {
            let (leader, swapped) = schedule.leader_with_swap(round);
            assert_eq!(swapped, round == 6, "only the round 6 leader is swapped");
            *expected_rounds.entry((leader.id().to_string(), swapped, found)).or_insert(0u64) += 1;
            if found {
                *expected_commits.entry(leader.id().to_string()).or_insert(0u64) += 1;
            }
        }

        let mut rounds_series = 0;
        let mut commits_series = 0;
        let mut actual_rounds = BTreeMap::new();
        let mut actual_commits = BTreeMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let label = |name: &str| {
                key.key()
                    .labels()
                    .find(|label| label.key() == name)
                    .map(|label| label.value().to_string())
                    .unwrap_or_else(|| panic!("missing label {name}"))
            };
            let DebugValue::Counter(count) = value else { continue };
            match key.key().name() {
                "tn_primary.leader_rounds_total" => {
                    rounds_series += 1;
                    if count > 0 {
                        let entry =
                            (label("leader"), label("swapped") == "true", label("found") == "true");
                        actual_rounds.insert(entry, count);
                    }
                }
                "tn_primary.leader_commits_total" => {
                    commits_series += 1;
                    if count > 0 {
                        actual_commits.insert(label("leader"), count);
                    }
                }
                _ => {}
            }
        }

        // every committee member exports all four outcomes and its commits from the start
        assert_eq!(rounds_series, 16);
        assert_eq!(commits_series, 4);
        assert_eq!(actual_rounds, expected_rounds);
        assert_eq!(actual_commits, expected_commits);
    }
}
