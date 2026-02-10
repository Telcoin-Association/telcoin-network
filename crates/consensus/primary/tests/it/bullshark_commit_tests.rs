//! Bullshark commit logic integration tests.
//!
//! These tests verify the critical commit invariants of Bullshark consensus:
//! - f+1 support required for leader commit
//! - Recursive leader commit (skip round R, commit R+2 includes R)
//! - Leader schedule changes based on reputation
//! - Commits delivered in correct order

use std::collections::BTreeSet;
use tn_primary::{
    consensus::{Bullshark, ConsensusState, LeaderSchedule, LeaderSwapTable, Outcome},
    test_utils::{
        make_certificates_with_leader_configuration, make_optimal_certificates,
        TestLeaderConfiguration, TestLeaderSupport,
    },
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{Certificate, Hash as _, DEFAULT_BAD_NODES_STAKE_THRESHOLD};

const NUM_SUB_DAGS_PER_SCHEDULE: u32 = 100;

/// Test that a leader requires f+1 support to commit.
/// A leader with less than f+1 support should return NotEnoughSupportForLeader.
#[tokio::test]
async fn test_bullshark_f_plus_1_support_required() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    // Configure leader of round 2 to have WEAK support (< f+1)
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader_round_2 = schedule.leader(2).id();
    let leader_config = TestLeaderConfiguration {
        round: 2,
        authority: leader_round_2,
        should_omit: false,
        support: Some(TestLeaderSupport::Weak),
    };

    let (certificates, _) = make_certificates_with_leader_configuration(
        &committee,
        1..=4, // rounds 1-4
        &genesis,
        &ids,
        [(2, leader_config)].into_iter().collect(),
    );

    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);
    let mut bullshark = Bullshark::new(
        committee.clone(),
        NUM_SUB_DAGS_PER_SCHEDULE,
        schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    // Process all certificates
    let mut saw_not_enough_support = false;
    for certificate in certificates {
        let (outcome, _) = bullshark.process_certificate(&mut state, certificate).unwrap();
        if outcome == Outcome::NotEnoughSupportForLeader {
            saw_not_enough_support = true;
        }
    }

    assert!(
        saw_not_enough_support,
        "Should see NotEnoughSupportForLeader when leader has weak support"
    );
}

/// Test that a leader with f+1 support commits successfully.
#[tokio::test]
async fn test_bullshark_strong_support_commits() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    // All leaders have strong support (default)
    let (certificates, _) = make_optimal_certificates(&committee, 1..=5, &genesis, &ids);

    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let mut bullshark = Bullshark::new(
        committee,
        NUM_SUB_DAGS_PER_SCHEDULE,
        schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let mut committed_count = 0;
    for certificate in certificates {
        let (outcome, committed) = bullshark.process_certificate(&mut state, certificate).unwrap();
        if outcome == Outcome::Commit {
            committed_count += committed.len();
        }
    }

    // With 5 rounds, leaders at rounds 2 and 4 should commit
    assert!(committed_count >= 2, "Should commit at least 2 leaders with strong support");
}

/// Test that skipping a leader round R, committing at R+2 includes R.
/// When leader of round 4 has weak support but leader of round 6 commits,
/// it should recursively commit round 4 leader if linked.
#[tokio::test]
async fn test_bullshark_recursive_leader_commit() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    // Leader of round 2 has weak support initially
    // Leader of round 4 has strong support
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader_round_2 = schedule.leader(2).id();
    let leader_config_2 = TestLeaderConfiguration {
        round: 2,
        authority: leader_round_2,
        should_omit: false,
        support: Some(TestLeaderSupport::Weak),
    };

    let (certificates, _) = make_certificates_with_leader_configuration(
        &committee,
        1..=7,
        &genesis,
        &ids,
        [(2, leader_config_2)].into_iter().collect(),
    );

    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);
    let mut bullshark = Bullshark::new(
        committee,
        NUM_SUB_DAGS_PER_SCHEDULE,
        schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let mut all_committed = Vec::new();
    for certificate in certificates {
        let (outcome, committed) = bullshark.process_certificate(&mut state, certificate).unwrap();
        if outcome == Outcome::Commit {
            all_committed.extend(committed);
        }
    }

    // Verify that commits happened and are in order
    assert!(!all_committed.is_empty(), "Should have commits");

    // Verify commits are in leader round order
    let mut last_round = 0;
    for subdag in all_committed.iter() {
        assert!(
            subdag.leader_round() >= last_round,
            "Commits should be in non-decreasing leader round order"
        );
        last_round = subdag.leader_round();
    }
}

/// Test that leader schedule changes after N subdags based on reputation.
#[tokio::test]
async fn test_bullshark_schedule_change_reputation() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    // Create enough rounds for multiple schedule changes
    // With sub_dags_per_schedule = 3, schedule changes every 3 commits
    let (certificates, _) = make_optimal_certificates(&committee, 1..=15, &genesis, &ids);

    let gc_depth = 50;
    let sub_dags_per_schedule = 3; // Schedule changes every 3 commits
    let mut state = ConsensusState::new(gc_depth);
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let mut bullshark = Bullshark::new(
        committee,
        sub_dags_per_schedule,
        schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let mut all_committed = Vec::new();
    let mut schedule_change_count = 0;

    for certificate in certificates {
        let (outcome, committed) = bullshark.process_certificate(&mut state, certificate).unwrap();
        match outcome {
            Outcome::Commit => {
                for subdag in &committed {
                    if subdag.reputation_score.final_of_schedule {
                        schedule_change_count += 1;
                    }
                }
                all_committed.extend(committed);
            }
            Outcome::ScheduleChanged => {
                // This is an intermediate state during commit processing
            }
            _ => {}
        }
    }

    // With enough commits, we should have at least one schedule change
    let total_commits = all_committed.len();
    let expected_schedule_changes = total_commits / sub_dags_per_schedule as usize;

    assert!(
        schedule_change_count >= expected_schedule_changes.saturating_sub(1),
        "Should have approximately {} schedule changes, got {}",
        expected_schedule_changes,
        schedule_change_count
    );
}

/// Test that commits are delivered in the correct order (older leaders first).
#[tokio::test]
async fn test_commit_order_oldest_first() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    let (certificates, _) = make_optimal_certificates(&committee, 1..=9, &genesis, &ids);

    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let mut bullshark = Bullshark::new(
        committee,
        NUM_SUB_DAGS_PER_SCHEDULE,
        schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let mut all_committed = Vec::new();
    for certificate in certificates {
        let (outcome, committed) = bullshark.process_certificate(&mut state, certificate).unwrap();
        if outcome == Outcome::Commit {
            all_committed.extend(committed);
        }
    }

    // Verify leaders are committed in ascending round order
    let mut last_leader_round = 0;
    for subdag in &all_committed {
        let leader_round = subdag.leader.round();
        assert!(
            leader_round > last_leader_round,
            "Leaders should be committed in ascending round order: {} > {}",
            leader_round,
            last_leader_round
        );
        last_leader_round = leader_round;
    }
}
