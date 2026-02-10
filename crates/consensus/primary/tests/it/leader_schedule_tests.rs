//! Leader schedule integration tests.
//!
//! These tests verify specific leader schedule behaviors that are NOT covered
//! by property-based tests in consensus_props.rs:
//! - Dynamic swap table updates
//! - Odd round panic behavior

use tn_primary::consensus::{LeaderSchedule, LeaderSwapTable};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{AuthorityIdentifier, ReputationScores};

/// Test that updating the swap table changes future leader elections.
/// This tests the dynamic update capability which is not covered by property tests.
#[tokio::test]
async fn test_leader_swap_table_update() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Create schedule with no swaps
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader_before = schedule.leader(2).id();

    // Create scores where authority 0 is bad
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    // Update the swap table
    let swap_table = LeaderSwapTable::new(&committee, 2, &scores, 33);
    schedule.update_leader_swap_table(swap_table);

    // Leader should now be different (swapped)
    let leader_after = schedule.leader(2).id();

    assert_ne!(leader_before, leader_after, "Leader should change after swap table update");
}

/// Test that only even rounds have leaders (odd rounds panic).
/// This is a critical safety invariant - Bullshark only elects leaders on even rounds.
#[tokio::test]
#[should_panic(expected = "We should never attempt to do a leader election for odd rounds")]
async fn test_leader_only_even_rounds() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let schedule = LeaderSchedule::new(committee, LeaderSwapTable::default());

    // This should panic - odd rounds don't have leaders
    let _ = schedule.leader(3);
}
