//! Leader schedule tests

use crate::consensus::{Dag, LeaderSchedule, LeaderSwapTable};
use std::{collections::BTreeSet, num::NonZeroUsize};
use tempfile::TempDir;
use tn_primary::test_utils::mock_certificate;
use tn_storage::{mem_db::MemDatabase, open_db, ConsensusStore};
use tn_test_utils::CommitteeFixture;
use tn_types::{AuthorityIdentifier, Certificate, CommittedSubDag, ReputationScores, Round};

#[tokio::test]
async fn test_leader_swap_table() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    // protocol_config.set_consensus_bad_nodes_stake_threshold(33);

    // the authority ids
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Adding some scores
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    let table = LeaderSwapTable::new(
        &committee, 2, &scores, // protocol_config.consensus_bad_nodes_stake_threshold(),
        33,
    );

    // Only one bad authority should be calculated since all have equal stake
    assert_eq!(table.bad_nodes.len(), 1);

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 1 {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        } else {
            assert!(table.swap(id, index as Round).is_none());
        }
    }

    // Now we create a larger committee with more score variation - still all the authorities have
    // equal stake.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();

    // the authority ids
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Adding some scores
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    // We expect the first 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(
        &committee, 2, &scores, // protocol_config.consensus_bad_nodes_stake_threshold(),
        33,
    );

    assert_eq!(table.bad_nodes.len(), 3);
    assert!(table.bad_nodes.contains_key(&authority_ids[0]));
    assert!(table.bad_nodes.contains_key(&authority_ids[1]));
    assert!(table.bad_nodes.contains_key(&authority_ids[2]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 3 {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        } else {
            assert!(table.swap(id, index as Round).is_none());
        }
    }
}

#[tokio::test]
async fn test_leader_schedule() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    // protocol_config.set_consensus_bad_nodes_stake_threshold(33);

    // the authority ids
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Create a leader schedule with a default swap table, so no authority will be swapped.
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());

    // Call the leader for round 2. It should give us the validator of position 0
    let original_leader = authority_ids.first().unwrap().clone();
    let leader_2 = schedule.leader(2);

    assert_eq!(leader_2.id(), original_leader);

    // Now update the scores to consider the authority of position 0 as slow
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    // Update the schedule
    let table = LeaderSwapTable::new(
        &committee, 2, &scores, // protocol_config.consensus_bad_nodes_stake_threshold(),
        33,
    );
    schedule.update_leader_swap_table(table.clone());

    // Now call the leader for round 2 again. It should be swapped with another node
    let leader_2 = schedule.leader(2);

    // The returned leader should not be the one of position 0
    assert_ne!(leader_2.id(), original_leader);

    // The returned leader should be the one returned by the swap table when using the updated
    // leader scores.
    let swapped_leader = table.swap(&original_leader, 2).unwrap().id();
    assert_eq!(leader_2.id(), table.swap(&original_leader, 2).unwrap().id());

    // Now create an empty DAG
    let mut dag = Dag::new();

    // Now try to retrieve the leader's certificate
    let (leader_authority, leader_certificate) = schedule.leader_certificate(2, &dag);
    assert_eq!(leader_authority.id(), swapped_leader);
    assert!(leader_certificate.is_none());

    // Populate the leader's certificate and try again
    let (digest, certificate) =
        mock_certificate(&committee, leader_authority.id(), 2, BTreeSet::new());
    dag.entry(2).or_default().insert(leader_authority.id(), (digest, certificate.clone()));

    let (leader_authority, leader_certificate_result) = schedule.leader_certificate(2, &dag);
    assert_eq!(leader_authority.id(), swapped_leader);
    assert_eq!(certificate, leader_certificate_result.unwrap().clone());
}

/// TODO: this test is failing - I think it's due to bad stake threshold
#[tokio::test]
async fn test_leader_schedule_from_store() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Create a leader schedule with a default swap table, so no authority will be swapped and find
    // the leader at position 2. We expect the leader of round 2 to be the authority of position
    // 0 , since round robin is used in tests.
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader_2 = schedule.leader(2);
    assert_eq!(leader_2.id(), authority_ids[0]);

    // AND we add a commit with a final score where the validator 0 is expected to be the
    // lowest score one.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in fixture.authorities().map(|a| a.id()).enumerate() {
        scores.add_score(&id, score as u64);
    }

    let sub_dag = CommittedSubDag::new(vec![], Certificate::default(), 0, scores, None);

    store.write_subdag_for_test(0, sub_dag);

    // WHEN
    let schedule = LeaderSchedule::from_store(committee, store, 33);

    // THEN the stored schedule should be returned and eventually the low score leader should be
    // swapped with a high score one.
    let new_leader_2 = schedule.leader(2);

    assert_ne!(leader_2.id(), new_leader_2.id());
}
