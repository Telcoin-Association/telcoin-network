//! Property-based tests for consensus invariants.
//!
//! These tests verify critical consensus properties:
//! - Leader selection is deterministic
//! - Round robin rotation covers all authorities
//! - Swap table produces consistent results
//! - Committee membership invariants

use proptest::prelude::*;
use std::collections::HashSet;
use tn_primary::consensus::{LeaderSchedule, LeaderSwapTable};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{AuthorityIdentifier, ReputationScores};

proptest! {
    /// Leader selection must be deterministic: same round always produces same leader.
    #[test]
    fn prop_leader_deterministic(
        committee_size in 4usize..15,
        round in (1u32..100).prop_map(|r| r * 2) // Even rounds only
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(std::num::NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();

        let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());

        // Call leader() multiple times for the same round
        let leader1 = schedule.leader(round);
        let leader2 = schedule.leader(round);
        let leader3 = schedule.leader(round);

        prop_assert_eq!(
            leader1.id(), leader2.id(),
            "Leader must be deterministic for round {}", round
        );
        prop_assert_eq!(
            leader2.id(), leader3.id(),
            "Leader must be deterministic for round {}", round
        );
    }

    /// Round robin must cover all authorities within N rounds (where N = committee size).
    #[test]
    fn prop_round_robin_covers_all(
        committee_size in 4usize..15
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(std::num::NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();

        let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());

        let mut leaders_seen: HashSet<AuthorityIdentifier> = HashSet::new();

        // Check rounds 2, 4, 6, ... up to 2*committee_size
        for i in 1..=committee_size {
            let round = (i * 2) as u32;
            let leader = schedule.leader(round);
            leaders_seen.insert(leader.id());
        }

        prop_assert_eq!(
            leaders_seen.len(),
            committee_size,
            "Round robin should cover all {} authorities within one cycle",
            committee_size
        );
    }

    /// No consecutive leaders should be the same (round robin property).
    #[test]
    fn prop_no_consecutive_leaders(
        committee_size in 4usize..15,
        start_round in 1u32..50
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(std::num::NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();

        let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());

        let base_round = start_round * 2; // Ensure even
        let next_round = base_round + 2;

        let leader1 = schedule.leader(base_round);
        let leader2 = schedule.leader(next_round);

        prop_assert_ne!(
            leader1.id(),
            leader2.id(),
            "Consecutive rounds {} and {} should have different leaders",
            base_round, next_round
        );
    }

    /// Leader must always be a member of the committee.
    #[test]
    fn prop_leader_is_committee_member(
        committee_size in 4usize..15,
        round in (1u32..100).prop_map(|r| r * 2)
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(std::num::NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();

        let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
        let leader = schedule.leader(round);

        prop_assert!(
            committee.is_authority(&leader.id()),
            "Leader {:?} must be a committee member", leader.id()
        );
    }

    /// Swap table produces deterministic swaps for same round.
    #[test]
    fn prop_swap_deterministic(
        committee_size in 4usize..15,
        round in (1u32..100).prop_map(|r| r * 2)
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(std::num::NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();
        let authority_ids: Vec<AuthorityIdentifier> =
            fixture.authorities().map(|a| a.id()).collect();

        // Create scores where authority 0 is bad
        let mut scores = ReputationScores::new(&committee);
        scores.final_of_schedule = true;
        for (score, id) in authority_ids.iter().enumerate() {
            scores.add_score(id, score as u64);
        }

        let swap_table1 = LeaderSwapTable::new(&committee, round, &scores, 33);
        let swap_table2 = LeaderSwapTable::new(&committee, round, &scores, 33);

        let schedule1 = LeaderSchedule::new(committee.clone(), swap_table1);
        let schedule2 = LeaderSchedule::new(committee.clone(), swap_table2);

        let leader1 = schedule1.leader(round);
        let leader2 = schedule2.leader(round);

        prop_assert_eq!(
            leader1.id(),
            leader2.id(),
            "Swap must be deterministic for round {}",
            round
        );
    }

    /// Leader schedule cycles correctly.
    #[test]
    fn prop_leader_schedule_cycles(
        committee_size in 4usize..15,
        num_cycles in 2usize..5
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(std::num::NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();

        let schedule = LeaderSchedule::new(committee, LeaderSwapTable::default());

        // Collect leaders for first cycle
        let first_cycle: Vec<AuthorityIdentifier> = (1..=committee_size)
            .map(|i| schedule.leader((i * 2) as u32).id())
            .collect();

        // Verify subsequent cycles match
        for cycle in 1..num_cycles {
            for i in 1..=committee_size {
                let round = ((cycle * committee_size + i) * 2) as u32;
                let leader = schedule.leader(round);
                let expected = &first_cycle[i - 1];

                prop_assert_eq!(
                    &leader.id(),
                    expected,
                    "Cycle {} round {} should match cycle 0 round {}",
                    cycle, round, i * 2
                );
            }
        }
    }
}

/// Test that bad nodes with low scores get swapped.
#[test]
fn test_bad_node_gets_swapped() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(std::num::NonZeroUsize::new(7).unwrap())
        .build();
    let committee = fixture.committee();
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // No swap schedule
    let no_swap_schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let original_leader_round_2 = no_swap_schedule.leader(2).id();

    // Create scores where first authority (position 0) has lowest score
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (idx, id) in authority_ids.iter().enumerate() {
        // Give authority 0 score 0, others get higher scores
        scores.add_score(id, idx as u64);
    }

    // Create swap table
    let swap_table = LeaderSwapTable::new(&committee, 2, &scores, 33);
    let swap_schedule = LeaderSchedule::new(committee.clone(), swap_table);
    let swapped_leader_round_2 = swap_schedule.leader(2).id();

    // The original round 2 leader is authority[0] (round robin)
    assert_eq!(original_leader_round_2, authority_ids[0]);

    // After swapping, the leader should be different (a good node)
    assert_ne!(swapped_leader_round_2, original_leader_round_2, "Bad node should be swapped out");
}
