//! Test for Primary <-> Primary handler.

use crate::{
    error::PrimaryNetworkError,
    network::{
        message::{ConsensusResult, PrimaryGossip},
        MissingCertificatesRequest, RequestHandler,
    },
    state_sync::StateSynchronizer,
    ConsensusBus, RecentBlocks,
};
use assert_matches::assert_matches;
use std::collections::{BTreeMap, BTreeSet};
use tn_network_libp2p::{GossipMessage, TopicHash};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    error::HeaderError, now, AuthorityIdentifier, BlockHash, BlockHeader, BlockNumHash,
    BlsPublicKey, Certificate, CertificateDigest, EpochVote, ExecHeader, Hash as _, SealedHeader,
    TaskManager, TnReceiver, TnSender, B256,
};
use tracing::debug;

#[test]
// for primary::network::message
fn test_missing_certs_request() {
    let max = 10;
    let expected_gc_round = 3;
    let expected_skip_rounds: BTreeMap<_, _> = [
        (AuthorityIdentifier::dummy_for_test(0), BTreeSet::from([4, 5, 6, 7])),
        (AuthorityIdentifier::dummy_for_test(2), BTreeSet::from([6, 7, 8])),
    ]
    .into_iter()
    .collect();
    let missing_req = MissingCertificatesRequest::default()
        .set_bounds(expected_gc_round, expected_skip_rounds.clone())
        .expect("boundary set")
        .set_max_response_size(max);
    let (decoded_gc_round, decoded_skip_rounds) =
        missing_req.get_bounds().expect("decode missing bounds");
    assert_eq!(expected_gc_round, decoded_gc_round);
    assert_eq!(expected_skip_rounds, decoded_skip_rounds);
}

/// The type for holding testng components.
struct TestTypes<DB = MemDatabase> {
    /// Committee committee with authorities that vote.
    committee: CommitteeFixture<DB>,
    // /// The authority that receives messages.
    // authority: &'a AuthorityFixture<DB>,
    /// The handler for requests.
    handler: RequestHandler<DB>,
    /// The parent execution result for all primary headers.
    ///
    /// num: 0
    /// hash: 0x78dec18c6d7da925bbe773c315653cdc70f6444ed6c1de9ac30bdb36cff74c3b
    parent: SealedHeader,
    /// Task manager the synchronizer (in RequestHandler) is spawned on.
    /// Save it so that task is not dropped early if needed.
    task_manager: TaskManager,
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types() -> TestTypes {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    let cb = ConsensusBus::new();

    // spawn the synchronizer
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    // last execution result
    let parent = SealedHeader::seal_slow(ExecHeader::default());

    // set the latest execution result to genesis - test headers are proposed for round 1
    let mut recent = RecentBlocks::new(1);
    recent.push_latest(0, B256::default(), Some(parent.clone()));
    cb.recent_blocks()
        .send(recent)
        .expect("watch channel updates for default parent in primary handler tests");

    let handler = RequestHandler::new(config.clone(), cb.clone(), synchronizer);
    TestTypes { committee, handler, parent, task_manager }
}

#[tokio::test]
async fn test_vote_succeeds() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert!(res.is_ok());
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_too_many_parents() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    // last authority produced 2 certs for round 1
    let mut too_many_parents: Vec<_> = Certificate::genesis(&committee.committee());
    let extra_parent = too_many_parents.last().expect("last cert").clone();
    too_many_parents.push(extra_parent.clone());

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, too_many_parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::TooManyParents(received, expected))) if received == 5 && expected == 4 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_authority_network_key() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let random_key = BlsPublicKey::default();

    // process vote
    let res = handler.vote(random_key, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor)));
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_genesis_parent() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // start with the expected parents in genesis
    let mut expected_parents: Vec<_> =
        Certificate::genesis(&committee.committee()).iter().map(|x| x.digest()).collect();
    let extra_parent = CertificateDigest::new(BlockHash::random().0);
    expected_parents.pop();
    expected_parents.push(extra_parent);
    let wrong_genesis: BTreeSet<_> = expected_parents.into_iter().collect();

    // create header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .parents(wrong_genesis)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidGenesisParent(wrong))) if wrong == extra_parent);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_execution_result() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, task_manager: _task_manager, .. } = create_test_types();

    // create header proposed by last peer in the committee for round 1
    let header = committee.header_from_last_authority();
    let parents = Vec::new();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownExecutionResult(wrong_hash))) if wrong_hash.hash == BlockHash::ZERO);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_header_digest() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, task_manager: _task_manager, .. } = create_test_types();

    let parents = Vec::new();

    // create header proposed by last peer in the committee for round 1
    let mut header = committee.header_from_last_authority();
    // change values so digest doesn't match
    header.latest_execution_block = BlockNumHash::new(0, BlockHash::random());
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidHeaderDigest)));
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_timestamp() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let wrong_time = now() + 100000; // too far in the future
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(wrong_time)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidTimestamp{created: wrong, ..})) if wrong == wrong_time);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_epoch() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let wrong_epoch = 3;
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .epoch(wrong_epoch)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch{ theirs: wrong, ours: correct })) if wrong == wrong_epoch && correct == 0 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_authority() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let wrong_authority = AuthorityIdentifier::dummy_for_test(100);
    let header = committee
        .header_builder_last_authority()
        .author(wrong_authority.clone())
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownAuthority(wrong))) if wrong == wrong_authority.to_string());
    Ok(())
}

/// Test that primary pub/sub is enforcing topics.
#[tokio::test]
async fn test_primary_batch_gossip_topics() {
    let TestTypes { handler, .. } = create_test_types();

    let gossip = PrimaryGossip::Certificate(Box::new(Certificate::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::primary_topic());
    let goodish_msg =
        GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&goodish_msg).await;
    // This will be rejected for other reasons, but make sure not for an invalid topic.
    assert!(!matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Consensus(Box::new(ConsensusResult::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::consensus_output_topic());
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&good_msg).await.is_ok());

    // EpochVote::default() has an invalid signature, so check_signature() fails in the handler
    // and returns InvalidHeader(PeerNotAuthor).
    let gossip = PrimaryGossip::EpochVote(Box::new(EpochVote::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::epoch_vote_topic());
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&good_msg).await;
    // Not rejected for InvalidTopic — rejected for invalid signature instead.
    assert!(!matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Certificate(Box::new(Certificate::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::epoch_vote_topic());
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&bad_msg).await;
    // This will be rejected for other reasons, but make sure it is for an invalid topic.
    assert!(matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Consensus(Box::new(ConsensusResult::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::primary_topic());
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&bad_msg).await.is_err());

    let gossip = PrimaryGossip::EpochVote(Box::new(EpochVote::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::consensus_output_topic());
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&bad_msg).await.is_err());
}

// ============================================================================
// Equivocation Detection Tests
// ============================================================================
// These tests verify that validators cannot vote for conflicting headers
// in the same round (equivocation), which is critical for consensus safety.

/// Test that voting twice for the same header (same digest) returns cached response.
#[tokio::test]
async fn test_vote_same_digest_returns_cached() -> eyre::Result<()> {
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // Create valid header
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // First vote should succeed
    let res1 = handler.vote(peer, header.clone(), parents.clone()).await;
    assert!(res1.is_ok(), "First vote should succeed");

    // Second vote for same header should return cached response (also success)
    let res2 = handler.vote(peer, header, parents).await;
    assert!(res2.is_ok(), "Second vote for same digest should return cached success");

    Ok(())
}

/// Test that voting for different header in same round is rejected (equivocation).
#[tokio::test]
async fn test_vote_different_digest_same_round_rejected() -> eyre::Result<()> {
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // Create first valid header
    let header1 = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // First vote should succeed
    let res1 = handler.vote(peer, header1, parents.clone()).await;
    assert!(res1.is_ok(), "First vote should succeed");

    // Create different header for same round (different timestamp = different digest)
    let header2 = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(2) // Different timestamp
        .build();

    // Second vote for different digest in same round should be rejected
    let res2 = handler.vote(peer, header2, parents).await;
    assert_matches!(
        res2,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::AlreadyVotedForLaterRound { .. })),
        "Vote for different header in same round should be rejected as equivocation"
    );

    Ok(())
}

/// Test that voting for older round after voting for newer round is rejected.
#[tokio::test]
async fn test_vote_older_round_rejected() -> eyre::Result<()> {
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();
    let peer = *committee.last_authority().authority().protocol_key();

    // Create and vote for round 2 header first
    let header_round2 = committee
        .header_builder_last_authority()
        .round(2)
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(2)
        .build();

    // Vote for round 2 (may fail for other reasons but registers the round)
    let _res1 = handler.vote(peer, header_round2, parents.clone()).await;

    // Now try to vote for round 1 (should be rejected - already voted for later round)
    let header_round1 = committee
        .header_builder_last_authority()
        .round(1)
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();

    let res2 = handler.vote(peer, header_round1, parents).await;
    // This should be rejected because we already processed a header for a later round
    assert_matches!(
        res2,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::AlreadyVotedForLaterRound { .. })),
        "Vote for older round should be rejected"
    );

    Ok(())
}

/// Test that the equivocation cache is per-authority.
#[tokio::test]
async fn test_vote_equivocation_per_authority() -> eyre::Result<()> {
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // Get two different authorities
    let authorities: Vec<_> = committee.authorities().collect();
    assert!(authorities.len() >= 2, "Need at least 2 authorities for this test");
    let committee_ref = committee.committee();

    // Create header from first authority
    let header1 = authorities[0]
        .header_builder(&committee_ref)
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();
    let peer1 = *authorities[0].authority().protocol_key();

    // Vote from first authority
    let _res1 = handler.vote(peer1, header1, parents.clone()).await;

    // Create header from second authority (same round, different author)
    let header2 = authorities[1]
        .header_builder(&committee_ref)
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();
    let peer2 = *authorities[1].authority().protocol_key();

    // Vote from second authority should NOT be rejected (different author)
    let res2 = handler.vote(peer2, header2, parents).await;
    // Should not fail due to equivocation (may fail for other reasons)
    assert!(
        !matches!(
            res2,
            Err(PrimaryNetworkError::InvalidHeader(HeaderError::AlreadyVotedForLaterRound { .. }))
        ),
        "Vote from different authority should not trigger equivocation check"
    );

    Ok(())
}
