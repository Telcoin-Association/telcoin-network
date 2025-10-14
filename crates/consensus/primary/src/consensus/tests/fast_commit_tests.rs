//! Tests for optimizing subdag commit when 2f+1 proposals reference a parent.
use super::*;
use crate::{
    consensus::{
        Bullshark, Consensus, ConsensusMetrics, ConsensusState, LeaderSchedule, LeaderSwapTable,
        Outcome,
    },
    test_utils::{make_optimal_certificates, mock_certificate, temp_dir},
    ConsensusBus, NodeMode,
};
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};
use tn_storage::{mem_db::MemDatabase, open_db};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    test_utils::init_test_tracing, AuthorityIdentifier, Certificate, CertificateDigest, Committee,
    ExecHeader, Hash as _, Round, SealedHeader, TaskManager, TnReceiver as _, TnSender as _, B256,
    DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};

/// Test that fast commit works when we have 2f+1 weak votes
#[tokio::test]
async fn fast_commit_with_weak_votes() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    // Create certificates for rounds 1-2
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let gc_depth = 50;
    let mut state = ConsensusState::new(metrics.clone(), gc_depth);
    let db = temp_dir();
    let store = open_db(db.path());
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let mut bullshark = Bullshark::new(
        committee.clone(),
        store,
        metrics,
        100,
        schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    // Insert all certificates into the DAG
    for certificate in &certificates {
        state.try_insert(certificate).unwrap();
        // track max inserted certificate round
        bullshark.max_inserted_certificate_round =
            bullshark.max_inserted_certificate_round.max(certificate.round());
    }

    // Get the leader certificate for round 2
    let leader = schedule.leader(2);
    let leader_digest = state
        .dag
        .get(&2)
        .and_then(|round| round.get(&leader.id()))
        .map(|(digest, _)| *digest)
        .expect("Leader should be in DAG");

    // WHEN: Simulate receiving 2f+1 proposals for round 3 that reference the leader
    // This simulates header proposals being received before certificates
    let quorum_size = committee.quorum_threshold() as usize; // this is 2f+1

    for i in 0..quorum_size {
        let proposer = ids[i % ids.len()].clone();
        let mut parents = BTreeSet::new();
        parents.insert(leader_digest);

        // Add other parents to make it valid
        for (auth_id, (digest, _)) in state.dag.get(&2).unwrap() {
            if auth_id != &leader.id() && parents.len() < 3 {
                parents.insert(*digest);
            }
        }

        // Track the weak vote
        let weak_vote = WeakVote { authority: proposer, round: 3, parents };
        state.track_weak_votes(weak_vote);
    }

    // THEN: Check that we have enough weak votes for fast commit
    let weak_votes = state.get_weak_votes(2, &leader.id());
    assert_eq!(weak_votes, quorum_size as u32);
    assert!(state.can_fast_commit(2, &leader, &committee));

    // Verify the fast commit can be triggered
    let (outcome, committed_sub_dags) = bullshark.try_fast_commit(&mut state).unwrap();
    assert_eq!(outcome, Outcome::Commit);
    assert!(!committed_sub_dags.is_empty());

    // Verify the leader was committed
    let committed_leader = &committed_sub_dags[0].leader;
    assert_eq!(committed_leader.round(), 2);
    assert_eq!(committed_leader.origin(), &leader.id());
}

/// Test that weak votes are not double-counted from the same proposer
#[tokio::test]
async fn weak_votes_no_double_counting() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let mut state = ConsensusState::new(metrics, 50);

    for certificate in &certificates {
        state.try_insert(certificate).unwrap();
    }

    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader = schedule.leader(2);
    let leader_digest = state
        .dag
        .get(&2)
        .and_then(|round| round.get(&leader.id()))
        .map(|(digest, _)| *digest)
        .unwrap();

    // WHEN: Same proposer sends multiple proposals (simulating equivocation)
    let proposer = ids[0].clone();
    let mut parents = BTreeSet::new();
    parents.insert(leader_digest);

    // Track first proposal
    let weak_vote1 = WeakVote { authority: proposer.clone(), round: 3, parents: parents.clone() };
    state.track_weak_votes(weak_vote1);
    assert_eq!(state.get_weak_votes(2, &leader.id()), 1);

    // Track second proposal from same proposer - should not increase count
    let weak_vote2 = WeakVote { authority: proposer.clone(), round: 3, parents: parents.clone() };
    state.track_weak_votes(weak_vote2);
    assert_eq!(state.get_weak_votes(2, &leader.id()), 1);

    // Track third proposal from same proposer - still should not increase
    let weak_vote3 = WeakVote { authority: proposer, round: 3, parents };
    state.track_weak_votes(weak_vote3);
    assert_eq!(state.get_weak_votes(2, &leader.id()), 1);
}

/// Test fallback to normal Direct Commit when not enough weak votes
#[tokio::test]
async fn fallback_to_direct_commit() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (mut certificates, parents) = make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let mut state = ConsensusState::new(metrics.clone(), 50);

    for certificate in &certificates {
        state.try_insert(certificate).unwrap();
    }

    let db = temp_dir();
    let store = open_db(db.path());
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader = schedule.leader(2);

    // Track only f weak votes (not enough for fast commit which needs 2f+1)
    let f = committee.validity_threshold() as usize - 1; // f nodes
    for i in 0..f {
        let proposer = ids[i].clone();
        let leader_digest = state
            .dag
            .get(&2)
            .and_then(|round| round.get(&leader.id()))
            .map(|(digest, _)| *digest)
            .unwrap();

        let mut parents = BTreeSet::new();
        parents.insert(leader_digest);

        let weak_vote = WeakVote { authority: proposer, round: 3, parents };
        state.track_weak_votes(weak_vote);
    }

    // Verify we don't have enough weak votes for fast commit
    assert!(!state.can_fast_commit(2, &leader, &committee));

    // Create f+1 certificates for round 3 that reference the leader
    // This should trigger normal Direct Commit
    for i in 0..(f + 1) {
        let (_, certificate) = mock_certificate(&committee, ids[i].clone(), 3, parents.clone());
        certificates.push_back(certificate);
    }

    let mut bullshark =
        Bullshark::new(committee, store, metrics, 100, schedule, DEFAULT_BAD_NODES_STAKE_THRESHOLD);

    // Process certificates and verify normal commit works
    for certificate in certificates.iter().skip(8) {
        // Skip first 8 (rounds 1-2)
        let (outcome, committed) =
            bullshark.process_certificate(&mut state, certificate.clone()).unwrap();

        if !committed.is_empty() {
            assert_eq!(outcome, Outcome::Commit);
            assert_eq!(committed[0].leader.round(), 2);
            break;
        }
    }
}

/// Test that weak votes are properly cleaned up during garbage collection
#[tokio::test]
async fn weak_votes_garbage_collection() {
    const GC_DEPTH: Round = 4;

    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=10, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let mut state = ConsensusState::new(metrics.clone(), GC_DEPTH);

    // Insert certificates and track weak votes for multiple rounds
    for certificate in &certificates {
        state.try_insert(certificate).unwrap();

        // Simulate weak votes for each round
        if certificate.round() >= 2 {
            let proposer = certificate.origin().clone();
            let parents: BTreeSet<_> = certificate.header().parents().iter().copied().collect();

            let weak_vote = WeakVote { authority: proposer, round: certificate.round(), parents };
            state.track_weak_votes(weak_vote);
        }
    }

    // Verify weak votes exist for all rounds
    for round in 1..=9 {
        // There should be some weak votes tracked for each round's certificates
        let has_votes = state.weak_votes.vote_counts.contains_key(&round);
        assert!(has_votes || round == 1); // Round 1 has no parents, so no votes
    }

    let db = temp_dir();
    let store = open_db(db.path());
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let mut bullshark =
        Bullshark::new(committee, store, metrics, 100, schedule, DEFAULT_BAD_NODES_STAKE_THRESHOLD);

    // Process certificates to trigger commits and GC
    for certificate in certificates {
        let _ = bullshark.process_certificate(&mut state, certificate).unwrap();
    }

    // After GC, old weak votes should be cleaned up
    let gc_round = state.last_round.gc_round;
    for round in 1..=gc_round {
        assert!(
            !state.weak_votes.vote_counts.contains_key(&round),
            "Round {} should be GC'd but still has weak votes",
            round
        );
    }

    for round in (gc_round + 1)..=10 {
        if state.dag.contains_key(&round) {
            // Weak votes for rounds still in DAG might still exist
            // depending on what got committed
        }
    }
}

/// Test fast commit with Byzantine proposer trying to equivocate
#[tokio::test]
async fn fast_commit_with_byzantine_equivocation() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let mut state = ConsensusState::new(metrics.clone(), 50);

    for certificate in &certificates {
        state.try_insert(certificate).unwrap();
    }

    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader = schedule.leader(2);
    let leader_digest = state
        .dag
        .get(&2)
        .and_then(|round| round.get(&leader.id()))
        .map(|(digest, _)| *digest)
        .unwrap();

    // Simulate f Byzantine nodes trying to equivocate
    let f = (committee.validity_threshold() - 1) as usize;

    // Byzantine nodes send proposals with the leader
    for i in 0..f {
        let proposer = ids[i].clone();
        let mut parents = BTreeSet::new();
        parents.insert(leader_digest);

        let weak_vote = WeakVote { authority: proposer, round: 3, parents };
        state.track_weak_votes(weak_vote);
    }

    // Byzantine nodes try to send different proposals (equivocation)
    // These should not be counted
    for i in 0..f {
        let proposer = ids[i].clone();
        // Different parents (not including leader)
        let other_digests: BTreeSet<_> = state
            .dag
            .get(&2)
            .unwrap()
            .values()
            .filter(|(d, _)| *d != leader_digest)
            .map(|(d, _)| *d)
            .take(2)
            .collect();

        let weak_vote = WeakVote { authority: proposer, round: 3, parents: other_digests };
        state.track_weak_votes(weak_vote);
    }

    // Verify Byzantine equivocations didn't increase vote count
    assert_eq!(state.get_weak_votes(2, &leader.id()), f as u32);

    // Add honest nodes' votes to reach 2f+1
    for i in f..(f * 2 + 1) {
        let proposer = ids[i % ids.len()].clone();
        let mut parents = BTreeSet::new();
        parents.insert(leader_digest);

        let weak_vote = WeakVote { authority: proposer, round: 3, parents };
        state.track_weak_votes(weak_vote);
    }

    // Should now have enough for fast commit
    assert!(state.can_fast_commit(2, &leader, &committee));
}

/// Integration test: Fast commit in full consensus pipeline
#[tokio::test]
async fn fast_commit_integration_test() {
    init_test_tracing();
    // Setup
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let store = config.node_storage().clone();
    let metrics = Arc::new(ConsensusMetrics::default());

    let bullshark = Bullshark::new(
        committee.clone(),
        store.clone(),
        metrics.clone(),
        100,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    cb.node_mode().send_modify(|mode| *mode = NodeMode::CvvActive);
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));

    // Spawn consensus
    let mut rx_output = cb.sequence().subscribe();
    let task_manager = TaskManager::default();
    Consensus::spawn(config, &cb, bullshark, &task_manager);

    // Generate and send certificates for rounds 1-2
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, parents) = make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    for certificate in &certificates {
        cb.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // give certificates time to be processed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Send header proposals for round 3 (2f+1 of them)
    let leader = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()).leader(2);
    let leader_digest = certificates
        .iter()
        .find(|c| c.round() == 2 && c.origin() == &leader.id())
        .map(|c| c.digest())
        .unwrap();

    let quorum_size = committee.quorum_threshold() as usize;
    for i in 0..quorum_size {
        let proposer = ids[i % ids.len()].clone();
        let mut proposal_parents = BTreeSet::new();
        proposal_parents.insert(leader_digest);

        // Add other parents
        for cert in certificates.iter().filter(|c| c.round() == 2) {
            if cert.digest() != leader_digest && proposal_parents.len() < quorum_size {
                proposal_parents.insert(cert.digest());
            }
        }

        let weak_vote = WeakVote { authority: proposer, round: 3, parents: proposal_parents };
        cb.header_proposals().send(weak_vote).await.unwrap();
    }

    // should trigger fast commit before receiving certificates
    let committed_sub_dag =
        tokio::time::timeout(std::time::Duration::from_secs(3), rx_output.recv())
            .await
            .unwrap()
            .unwrap();

    assert_eq!(committed_sub_dag.leader.round(), 2);
    assert_eq!(committed_sub_dag.leader.origin(), &leader.id());
}

/// Test WeakVoteTracker directly
#[tokio::test]
async fn test_weak_vote_tracker() {
    let mut tracker = WeakVoteTracker::new();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    // Create a simple DAG
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    let mut dag = Dag::new();
    for cert in certificates {
        dag.entry(cert.round()).or_default().insert(cert.origin().clone(), (cert.digest(), cert));
    }

    // get round 2 digests mapped to their authorities
    let round2_certs = dag.get(&2).unwrap();

    // hashmap is unpredictable
    // get specific digests for each authority
    let digest_0 = round2_certs.get(&ids[0]).unwrap().0;
    let digest_1 = round2_certs.get(&ids[1]).unwrap().0;
    let digest_2 = round2_certs.get(&ids[2]).unwrap().0;

    // test tracking proposals
    let proposer1 = ids[0].clone();
    let proposer2 = ids[1].clone();

    // track first proposal - proposer1 votes for ids[0] and ids[1]
    let weak_vote1 = WeakVote {
        authority: proposer1.clone(),
        round: 3,
        parents: vec![digest_0, digest_1].into_iter().collect(),
    };
    tracker.track_proposal(weak_vote1, &dag);
    assert_eq!(tracker.get_votes(2, &ids[0]), 1);
    assert_eq!(tracker.get_votes(2, &ids[1]), 1);
    assert_eq!(tracker.get_votes(2, &ids[2]), 0);

    // Track second proposal - proposer2 votes for ids[1] and ids[2]
    let weak_vote2 = WeakVote {
        authority: proposer2.clone(),
        round: 3,
        parents: vec![digest_1, digest_2].into_iter().collect(),
    };
    tracker.track_proposal(weak_vote2, &dag);
    assert_eq!(tracker.get_votes(2, &ids[0]), 1);
    assert_eq!(tracker.get_votes(2, &ids[1]), 2);
    assert_eq!(tracker.get_votes(2, &ids[2]), 1);

    // Try to track duplicate from proposer1 - should not increase counts
    let weak_vote3 = WeakVote {
        authority: proposer1,
        round: 3,
        parents: vec![digest_0, digest_1, digest_2].into_iter().collect(),
    };
    tracker.track_proposal(weak_vote3, &dag);
    assert_eq!(tracker.get_votes(2, &ids[0]), 1); // Still 1
    assert_eq!(tracker.get_votes(2, &ids[1]), 2); // Still 2
    assert_eq!(tracker.get_votes(2, &ids[2]), 1); // Still 1

    // Test cleanup
    tracker.cleanup(1);
    assert!(tracker.vote_counts.contains_key(&2)); // round 2 still there

    tracker.cleanup(2);
    assert!(!tracker.vote_counts.contains_key(&2)); // round 2 cleaned up
}
