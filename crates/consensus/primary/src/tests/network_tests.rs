//! Test for Primary <-> Primary handler.

use crate::{
    error::PrimaryNetworkError,
    network::{
        message::{ConsensusResult, PrimaryGossip},
        MissingCertificatesRequest, PendingEpochStream, RequestHandler,
        MAX_CONCURRENT_EPOCH_STREAMS, PENDING_REQUEST_TIMEOUT,
    },
    state_sync::StateSynchronizer,
    ConsensusBus, ConsensusBusApp, NodeMode, RecentBlocks,
};
use assert_matches::assert_matches;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tn_config::Parameters;
use tn_network_libp2p::{GossipMessage, TopicHash};
use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    error::HeaderError, now, AuthorityIdentifier, BlockHash, BlockHeader, BlockNumHash,
    BlsPublicKey, Certificate, CertificateDigest, EpochVote, ExecHeader, Hash as _, SealedHeader,
    TaskManager, B256,
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
    /// The consensus bus app for manipulating shared state in tests.
    consensus_bus: ConsensusBusApp,
    /// Task manager the synchronizer (in RequestHandler) is spawned on.
    /// Save it so that task is not dropped early if needed.
    task_manager: TaskManager,
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.  Allow params to be overridden.
async fn create_test_types_with_params(path: &Path, params: Option<Parameters>) -> TestTypes {
    let committee = if let Some(params) = params {
        CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .with_consensus_parameters(params)
            .build()
    } else {
        CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build()
    };
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
    recent.push_latest(0, BlockNumHash::new(0, B256::default()), Some(parent.clone()));
    cb.app().recent_blocks().send_replace(recent);

    let consensus_chain =
        ConsensusChain::new_for_test(path.to_owned(), committee.committee()).await.unwrap();
    let consensus_bus = cb.app().clone();
    let handler =
        RequestHandler::new(config.clone(), cb.app().clone(), synchronizer, consensus_chain);
    TestTypes { committee, handler, parent, consensus_bus, task_manager }
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
async fn create_test_types(path: &Path) -> TestTypes {
    create_test_types_with_params(path, None).await
}

#[tokio::test]
async fn test_vote_succeeds() -> eyre::Result<()> {
    // common types
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;
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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, .. } = create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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

// ============================================================================
// Locking and Timeout Tests
// ============================================================================
// These tests cover the per-authority locking and timeout behavior added to vote().

/// Helper: same as `create_test_types` but overrides `max_header_delay`.
async fn create_test_types_with_delay(path: &Path, max_header_delay: Duration) -> TestTypes {
    let mut params = Parameters::default();
    params.max_header_delay = max_header_delay;
    create_test_types_with_params(path, Some(params)).await
}

/// Two concurrent vote requests for the same authority with the same header must both
/// complete without deadlock.  The per-authority `TokioMutex` serialises the two calls;
/// whichever wins the lock processes `vote_inner` normally and stores the cached result,
/// while the other sees the cached response on its turn.
#[tokio::test]
async fn test_vote_per_authority_lock_concurrent_same_header() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // Clone shares the same Arc<AuthEquivocationMap>, so both tasks contend on the
    // same per-authority mutex.
    let handler1 = handler.clone();
    let handler2 = handler.clone();
    let header1 = header.clone();
    let header2 = header.clone();

    let task1 = tokio::spawn(async move { handler1.vote(peer, header1, vec![]).await });
    let task2 = tokio::spawn(async move { handler2.vote(peer, header2, vec![]).await });

    let (res1, res2) = tokio::join!(task1, task2);

    // Both must succeed: one runs vote_inner, the other returns the cached result.
    assert!(res1.expect("task1 panicked").is_ok(), "first concurrent vote should succeed");
    assert!(
        res2.expect("task2 panicked").is_ok(),
        "second concurrent vote should succeed (cached)"
    );

    Ok(())
}

/// When `vote_inner` blocks longer than `max_header_delay`, `vote()` must return
/// `PrimaryNetworkError::Timeout`.
///
/// To force a reliable block we request a header whose `latest_execution_block` is at
/// block number 1, while the test environment only has block 0.  This causes
/// `wait_for_execution` to suspend on the watch channel.  We use
/// `tokio::time::pause/advance` so the test completes instantly without real sleeping.
#[tokio::test]
async fn test_vote_inner_timeout() -> eyre::Result<()> {
    tokio::time::pause();

    let temp_dir = TempDir::new().unwrap();
    // Use a 50 ms timeout — short enough to be clearly exceeded after a 100 ms advance.
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types_with_delay(temp_dir.path(), Duration::from_millis(50)).await;

    // Block number 1 will never be executed in this test; vote_inner blocks in
    // wait_for_execution until the outer timeout fires.
    let future_block = BlockNumHash::new(1, BlockHash::random());
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(future_block)
        .created_at(1)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // Spawn on a separate task so we can advance the mock clock while it is suspended.
    let vote_task = tokio::spawn(async move { handler.vote(peer, header, vec![]).await });

    // Advance mock clock past the 50 ms deadline.
    tokio::time::advance(Duration::from_millis(100)).await;

    let res = vote_task.await.expect("vote task panicked");
    assert_matches!(res, Err(PrimaryNetworkError::Timeout(_)), "expected Timeout error");

    Ok(())
}

/// Test that the equivocation cache is per-authority.
#[tokio::test]
async fn test_vote_equivocation_per_authority() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

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

// ============================================================================
// behind_consensus Tests
// ============================================================================
// These tests verify the behind_consensus detection logic, including the fix
// for false-positive detection when the engine lags behind consensus commits.

/// Non-active CVV should never be considered behind.
#[tokio::test]
async fn test_behind_consensus_not_active_cvv() {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, consensus_bus, .. } = create_test_types(temp_dir.path()).await;

    // Set node to inactive — behind_consensus should return false immediately
    consensus_bus.node_mode().send_replace(NodeMode::CvvInactive);

    let result = handler.behind_consensus(0, 999, None).await;
    assert!(!result, "non-active CVV should never report as behind");
}

/// When the engine's processed_round lags but committed_round is current,
/// behind_consensus should return false (the node IS participating in consensus).
#[tokio::test]
async fn test_behind_consensus_committed_round_prevents_false_positive() {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, consensus_bus, .. } = create_test_types(temp_dir.path()).await;

    // Simulate: engine has only processed round 18 (stale), but consensus core
    // has committed through round 59 (current).
    // Default gc_depth is 50, safety buffer subtracts 10 → effective gc_depth = 40.
    // Without the fix: effective_exec_round = 18, 18 + 40 = 58 < 59 → false positive!
    // With the fix: effective_exec_round = max(0, 18, 59) = 59, 59 + 40 = 99 > 59 → correct.
    let mut recent = RecentBlocks::new(1);
    recent.push_latest(18, BlockNumHash::new(0, B256::default()), None);
    consensus_bus.recent_blocks().send_replace(recent);

    // Set committed round to 59 (as Bullshark would)
    consensus_bus.committed_round_updates().send_replace(59);

    // Incoming certificate at round 59 in the same epoch (0)
    let result = handler.behind_consensus(0, 59, None).await;
    assert!(!result, "committed_round should prevent false-positive behind detection");
}

/// When both engine and committed round are genuinely behind, detection should trigger.
#[tokio::test]
async fn test_behind_consensus_genuinely_behind() {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, consensus_bus, .. } = create_test_types(temp_dir.path()).await;

    // Node was offline — both engine and consensus core are at round 5.
    // Incoming gossip is at round 60 in the same epoch.
    // effective_exec_round = max(0, 5, 5) = 5, gc_depth = 40, 5 + 40 = 45 < 60 → behind.
    let mut recent = RecentBlocks::new(1);
    recent.push_latest(5, BlockNumHash::new(0, B256::default()), None);
    consensus_bus.recent_blocks().send_replace(recent);
    consensus_bus.committed_round_updates().send_replace(5);

    let result = handler.behind_consensus(0, 60, None).await;
    assert!(result, "genuinely behind node should be detected");
}

/// A peer that re-requests the same epoch while an entry is already pending must not be
/// able to reset the cleanup timer. If the replacement path rearmed `created_at`, a peer
/// could re-request every 20s and hold a slot forever. This test exercises the
/// preservation logic used by `process_epoch_stream` and verifies the entry is evicted on
/// schedule relative to the *original* insertion time.
#[tokio::test]
async fn test_pending_epoch_stream_replacement_preserves_created_at() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_EPOCH_STREAMS));
    let mut pending_map: HashMap<(BlsPublicKey, B256), PendingEpochStream> = HashMap::new();

    let peer = BlsPublicKey::default();
    let digest = B256::random();
    let key = (peer, digest);
    let epoch: u32 = 7;

    // initial insertion at T0, where T0 is just past the cleanup horizon so we can
    // assert eviction without waiting on wall-clock time
    let t0 = Instant::now() - PENDING_REQUEST_TIMEOUT - Duration::from_secs(1);
    let permit = semaphore.clone().try_acquire_owned().expect("permit available");
    pending_map.insert(key, PendingEpochStream::new_with_created_at(epoch, permit, t0));

    // simulate a re-request: production code looks up the existing entry's
    // `created_at` and reuses it when building the replacement
    let new_permit = semaphore.clone().try_acquire_owned().expect("permit available");
    let preserved_created_at =
        pending_map.get(&key).map(|p| p.created_at).unwrap_or_else(Instant::now);
    let replacement =
        PendingEpochStream { epoch, created_at: preserved_created_at, _permit: new_permit };
    assert!(pending_map.insert(key, replacement).is_some(), "expected replacement");

    // the replacement must carry the original `created_at`, not a fresh one
    let after = pending_map.get(&key).expect("entry present after replacement");
    assert_eq!(
        after.created_at, t0,
        "replacement must preserve original created_at to prevent cleanup-timer reset"
    );

    // cleanup mirrors `PrimaryNetwork::cleanup_stale_pending_requests`: entries whose
    // age >= PENDING_REQUEST_TIMEOUT must be evicted. Since created_at is t0 (stale),
    // the entry must drop.
    let now = Instant::now();
    pending_map
        .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);

    assert!(
        pending_map.is_empty(),
        "stale entry must be evicted by cleanup even though it was 'replaced' moments ago"
    );

    // and the permit must have returned to the semaphore
    assert_eq!(
        semaphore.available_permits(),
        MAX_CONCURRENT_EPOCH_STREAMS,
        "dropping the evicted pending entry must release its semaphore permit"
    );
}
