//! Leader schedule for identifying the next leader for the round.

use super::Dag;
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IndexedRandom as _, SeedableRng};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};
use tn_storage::ConsensusStore;
use tn_types::{
    Authority, AuthorityIdentifier, Certificate, Committee, ReputationScores, Round, VotingPower,
};
use tracing::{debug, trace};

#[cfg(test)]
#[path = "tests/leader_schedule_tests.rs"]
mod leader_schedule_tests;

#[derive(Default, Clone)]
pub struct LeaderSwapTable {
    /// The round on which the leader swap table get into effect.
    round: Round,
    /// The list of `f` (by stake) authorities with best scores as those defined by the provided
    /// `ReputationScores`. Those authorities will be used in the position of the `bad_nodes`
    /// on the final leader schedule.
    good_nodes: Vec<Authority>,
    /// The set of `f` (by stake) authorities with the worst scores as those defined by the
    /// provided `ReputationScores`. Every time where such authority is elected as leader on
    /// the schedule, it will swapped by one of the authorities of the `good_nodes`.
    bad_nodes: HashMap<AuthorityIdentifier, Authority>,
}

impl Debug for LeaderSwapTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "LeaderSwapTable round:{}, good_nodes:{:?} with stake:{}, bad_nodes:{:?} with stake:{}",
            self.round,
            self.good_nodes.iter().map(|a| a.id()).collect::<Vec<AuthorityIdentifier>>(),
            self.good_nodes.iter().map(|a| a.voting_power()).sum::<VotingPower>(),
            self.bad_nodes.iter().map(|a| a.0.clone()).collect::<Vec<AuthorityIdentifier>>(),
            self.bad_nodes.iter().map(|a| a.1.voting_power()).sum::<VotingPower>(),
        ))
    }
}

impl LeaderSwapTable {
    /// Constructs a new table based on the provided reputation scores. The
    /// `bad_nodes_stake_threshold` designates the total (by stake) nodes that will be
    /// considered as "bad" based on their scores and will be replaced by good nodes.
    ///
    /// The `bad_nodes_stake_threshold` should be in the range of [0 - 33].
    ///
    /// For now, we use 0.
    pub fn new(
        committee: &Committee,
        round: Round,
        reputation_scores: &ReputationScores,
        bad_nodes_stake_threshold: u64,
    ) -> Self {
        assert!((0..=33).contains(&bad_nodes_stake_threshold), "The bad_nodes_stake_threshold should be in range [0 - 33], out of bounds parameter detected");
        assert!(reputation_scores.final_of_schedule, "Only reputation scores that have been calculated on the end of a schedule are accepted");

        // calculating the good nodes
        let good_nodes = Self::retrieve_first_nodes(
            committee,
            reputation_scores.authorities_by_score_desc().into_iter(),
            bad_nodes_stake_threshold,
        );

        // calculating the bad nodes
        // we revert the sorted authorities to score ascending so we get the first low scorers
        // up to the dictated stake threshold.
        let bad_nodes = Self::retrieve_first_nodes(
            committee,
            reputation_scores.authorities_by_score_desc().into_iter().rev(),
            bad_nodes_stake_threshold,
        )
        .into_iter()
        .map(|authority| (authority.id(), authority))
        .collect::<HashMap<AuthorityIdentifier, Authority>>();

        good_nodes.iter().for_each(|good_node| {
            debug!(
                "Good node on round {}: {} -> {}",
                round,
                good_node.hostname(),
                reputation_scores
                    .scores_per_authority
                    .get(&good_node.id())
                    .expect("good node in scores per authority")
            );
        });

        bad_nodes.iter().for_each(|(_id, bad_node)| {
            debug!(
                "Bad node on round {}: {} -> {}",
                round,
                bad_node.hostname(),
                reputation_scores
                    .scores_per_authority
                    .get(&bad_node.id())
                    .expect("bad node in scores by authority")
            );
        });

        debug!("Reputation scores on round {round}: {reputation_scores:?}");

        Self { round, good_nodes, bad_nodes }
    }

    /// Checks whether the provided leader is a bad performer and needs to be swapped in the
    /// schedule with a good performer. If not, then the method returns None. Otherwise the
    /// leader to swap with is returned instead. The `leader_round` represents the DAG round on
    /// which the provided AuthorityIdentifier is a leader on and is used as a seed to random
    /// function in order to calculate the good node that will swap in that round with the bad
    /// node. We are intentionally not doing weighted randomness as we want to give to all the
    /// good nodes equal opportunity to get swapped with bad nodes and not have one node with
    /// enough stake end up swapping bad nodes more frequently than the others on
    /// the final schedule.
    pub fn swap(&self, leader: &AuthorityIdentifier, leader_round: Round) -> Option<Authority> {
        if self.bad_nodes.contains_key(leader) {
            let mut seed_bytes = [0u8; 32];
            seed_bytes[32 - 8..].copy_from_slice(&(leader_round as u64).to_le_bytes());
            let mut rng = StdRng::from_seed(seed_bytes);

            let good_node = self
                .good_nodes
                .choose(&mut rng)
                .expect("There should be at least one good node available");

            trace!(
                "Swapping bad leader {} -> {} for round {}",
                leader,
                good_node.id(),
                leader_round
            );

            return Some(good_node.to_owned());
        }
        None
    }

    /// Retrieves the first nodes provided by the iterator `authorities` until the `stake_threshold`
    /// has been reached. The `stake_threshold` should be between [0, 100] and expresses the
    /// percentage of stake that is considered the cutoff. Basically we keep adding to the
    /// response authorities until the sum of the stake reaches the `stake_threshold`. It's the
    /// caller's responsibility to ensure that the elements of the `authorities`
    /// input is already sorted.
    fn retrieve_first_nodes(
        committee: &Committee,
        authorities: impl Iterator<Item = (AuthorityIdentifier, u64)>,
        voting_power_threshold: u64,
    ) -> Vec<Authority> {
        let mut filtered_authorities = Vec::new();

        let mut voting_power = 0;
        for (authority_id, _score) in authorities {
            voting_power += committee.voting_power_by_id(&authority_id);

            // if the total accumulated stake has surpassed the stake threshold then we omit this
            // last authority and we exit the loop.
            if voting_power
                > (voting_power_threshold * committee.total_voting_power()) / 100 as VotingPower
            {
                break;
            }
            if let Some(auth) = committee.authority(&authority_id) {
                filtered_authorities.push(auth.to_owned());
            }
        }

        filtered_authorities
    }
}

/// The LeaderSchedule is responsible for producing the leader schedule across an epoch.
///
/// It provides methods to derive the leader of a round based on the provided leader swap table.
/// This struct can be cloned and shared freely as the internal parts are atomically updated.
#[derive(Clone)]
pub struct LeaderSchedule {
    pub committee: Committee,
    pub leader_swap_table: Arc<RwLock<LeaderSwapTable>>,
}

impl LeaderSchedule {
    pub fn new(committee: Committee, table: LeaderSwapTable) -> Self {
        Self { committee, leader_swap_table: Arc::new(RwLock::new(table)) }
    }

    /// Restores the LeaderSchedule by using the storage. It will attempt to retrieve the last
    /// committed "final" ReputationScores and use them to create build a LeaderSwapTable to use
    /// for the LeaderSchedule.
    pub fn from_store<DB: ConsensusStore>(
        committee: Committee,
        store: DB,
        bad_nodes_stake_threshold: u64,
    ) -> Self {
        let table = store
            .read_latest_commit_with_final_reputation_scores(committee.epoch())
            .map_or(LeaderSwapTable::default(), |subdag| {
                LeaderSwapTable::new(
                    &committee,
                    subdag.leader_round(),
                    &subdag.reputation_score,
                    bad_nodes_stake_threshold,
                )
            });
        // create the schedule
        Self::new(committee, table)
    }

    /// Atomically updates the leader swap table with the new provided one. Any leader queried from
    /// now on will get calculated according to this swap table until a new one is provided again.
    pub fn update_leader_swap_table(&self, table: LeaderSwapTable) {
        trace!("Updating swap table {:?}", table);

        let mut write = self.leader_swap_table.write();
        *write = table;
    }

    /// Returns the leader for the provided round. Keep in mind that this method will return a
    /// leader according to the provided LeaderSwapTable. Providing a different table can
    /// potentially produce a different leader for the same round.
    pub fn leader(&self, round: Round) -> Authority {
        assert_eq!(round % 2, 0, "We should never attempt to do a leader election for odd rounds");

        // TODO: split the leader election logic for testing from the production code.
        cfg_if::cfg_if! {
            if #[cfg(test)] {
                // We apply round robin in leader election. Since we expect round to be an even number,
                // 2, 4, 6, 8... it can't work well for leader election as we'll omit leaders. Thus
                // we can always divide by 2 to get a monotonically incremented sequence,
                // 2/2 = 1, 4/2 = 2, 6/2 = 3, 8/2 = 4  etc, and then do minus 1 so we can always
                // start with base zero 0.
                let next_leader = (round as u64 / 2 + self.committee.size() as u64 - 1) as usize % self.committee.size();

                let leader: Authority = self.committee.authorities().get(next_leader).expect("authority out of bounds!").clone();
                let table = self.leader_swap_table.read();

                table.swap(&leader.id(), round).unwrap_or(leader)
            } else {
                // Elect the leader in a stake-weighted choice seeded by the round
                let leader = self.committee.leader(round as u64);

                let table = self.leader_swap_table.read();
                table.swap(&leader.id(), round).unwrap_or(leader)
            }
        }
    }

    /// Returns the certificate originated by the leader of the specified round (if any). The
    /// Authority leader of the round is always returned and that's irrespective of whether the
    /// certificate exists as that's deterministically determined. The provided
    /// `leader_swap_table` is being used to determine any overrides that need to be performed
    /// to the original schedule.
    pub fn leader_certificate<'a>(
        &self,
        round: Round,
        dag: &'a Dag,
    ) -> (Authority, Option<&'a Certificate>) {
        // Note: this function is often called with even rounds only. While we do not aim at random
        // selection yet (see issue https://github.com/MystenLabs/sui/issues/5182), repeated calls to this function
        // should still pick from the whole roster of leaders.
        let leader = self.leader(round);

        // Return its certificate and the certificate's digest.
        match dag.get(&round).and_then(|x| x.get(&leader.id())) {
            None => (leader, None),
            Some((_, certificate)) => (leader, Some(certificate)),
        }
    }

    pub fn num_of_bad_nodes(&self) -> usize {
        let read = self.leader_swap_table.read();
        read.bad_nodes.len()
    }
}
