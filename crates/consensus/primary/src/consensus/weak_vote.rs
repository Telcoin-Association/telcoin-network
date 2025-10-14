//! TODO:
//! TODO
//! TODO
//! TODO
//!
//! update this comment

use crate::consensus::Dag;
use std::collections::{BTreeMap, HashMap, HashSet};
use tn_types::{AuthorityIdentifier, Round, WeakVote};
#[cfg(test)]
#[path = "tests/fast_commit_tests.rs"]
pub mod fast_commit;

/// Track header parents as weak votes for fast commit rule.
pub struct WeakVoteTracker {
    /// Tracks which proposers we've seen for each round (to prevent double-counting)
    pub proposers_seen: BTreeMap<Round, HashSet<AuthorityIdentifier>>,
    /// Tracks weak vote counts: round -> authority -> count
    pub vote_counts: BTreeMap<Round, HashMap<AuthorityIdentifier, u32>>,
}

impl WeakVoteTracker {
    /// Create a new instance of Self.
    pub fn new() -> Self {
        Self { proposers_seen: BTreeMap::new(), vote_counts: BTreeMap::new() }
    }

    ///
    pub fn track_proposal(&mut self, weak_vote: WeakVote, dag: &Dag) {
        // check if authority has already proposed a weak vote for this round
        let seen = self.proposers_seen.entry(weak_vote.round).or_insert_with(HashSet::new);

        if !seen.insert(weak_vote.authority) {
            // already processed a proposal from this proposer for this round
            return;
        }

        // parents are from the previous round
        let parent_round = weak_vote.round.saturating_sub(1);
        if parent_round == 0 {
            return;
        }

        // track weak votes for each parent
        if let Some(dag_round) = dag.get(&parent_round) {
            for parent_digest in &weak_vote.parents {
                for (authority, (digest, _)) in dag_round {
                    if digest == parent_digest {
                        let votes =
                            self.vote_counts.entry(parent_round).or_insert_with(HashMap::new);
                        *votes.entry(authority.clone()).or_insert(0) += 1;
                        break;
                    }
                }
            }
        }
    }

    /// Retrieve weak votes for round by authority.
    pub fn get_votes(&self, round: Round, authority: &AuthorityIdentifier) -> u32 {
        self.vote_counts.get(&round).and_then(|v| v.get(authority)).copied().unwrap_or(0)
    }

    /// Remove votes outside garbage collection round.
    pub fn cleanup(&mut self, gc_round: Round) {
        self.proposers_seen.retain(|&r, _| r > gc_round);
        self.vote_counts.retain(|&r, _| r > gc_round);
    }
}
