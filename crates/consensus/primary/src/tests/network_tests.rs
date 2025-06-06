//! Test for Primary <-> Primary handler.

use crate::{
    error::PrimaryNetworkError,
    network::{MissingCertificatesRequest, RequestHandler},
    state_sync::StateSynchronizer,
    ConsensusBus, RecentBlocks,
};
use assert_matches::assert_matches;
use std::collections::{BTreeMap, BTreeSet};
use tn_network_libp2p::PeerId;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    error::HeaderError, network_public_key_to_libp2p, now, AuthorityIdentifier, BlockHash,
    BlockHeader, BlockNumHash, Certificate, CertificateDigest, ExecHeader, Hash as _, SealedHeader,
    TaskManager,
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
        .set_max_items(max);
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
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types() -> TestTypes {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    let cb = ConsensusBus::new();

    // spawn the synchronizer
    let synchronizer = StateSynchronizer::new(config.clone(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);

    // last execution result
    let parent = SealedHeader::seal_slow(ExecHeader::default());

    // set the latest execution result to genesis - test headers are proposed for round 1
    let mut recent = RecentBlocks::new(1);
    recent.push_latest(parent.clone());
    cb.recent_blocks()
        .send(recent)
        .expect("watch channel updates for default parent in primary handler tests");

    let handler = RequestHandler::new(config.clone(), cb.clone(), synchronizer);
    TestTypes { committee, handler, parent }
}

#[tokio::test]
async fn test_vote_succeeds() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();

    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert!(res.is_ok());
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_too_many_parents() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();

    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

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

    // process vote
    let res = handler.vote(peer_id, header, too_many_parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::TooManyParents(received, expected))) if received == 5 && expected == 4 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_authority_network_key() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();
    let parents = Vec::new();
    let random_peer_id = PeerId::random();

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();

    // process vote
    let res = handler.vote(random_peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownNetworkKey(peer_id))) if peer_id == random_peer_id);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_genesis_parent() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();

    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // start with the expected parents in genesis
    let mut expected_parents: Vec<_> =
        Certificate::genesis(&committee.committee()).iter().map(|x| x.digest()).collect();
    let extra_parent = CertificateDigest::new(BlockHash::random().0);
    expected_parents.push(extra_parent);
    let wrong_genesis: BTreeSet<_> = expected_parents.into_iter().collect();

    // create header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .parents(wrong_genesis)
        .build();

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidGenesisParent(wrong))) if wrong == extra_parent);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_execution_result() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, .. } = create_test_types();

    // create header proposed by last peer in the committee for round 1
    let header = committee.header_from_last_authority();
    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownExecutionResult(wrong_hash))) if wrong_hash.hash == BlockHash::ZERO);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_header_digest() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, .. } = create_test_types();

    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // create header proposed by last peer in the committee for round 1
    let mut header = committee.header_from_last_authority();
    // change values so digest doesn't match
    header.latest_execution_block = BlockNumHash::new(0, BlockHash::random());

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidHeaderDigest)));
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_timestamp() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();

    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // create valid header proposed by last peer in the committee for round 1
    let wrong_time = now() + 100000; // too far in the future
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(wrong_time)
        .build();

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidTimestamp{created: wrong, ..})) if wrong == wrong_time);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_epoch() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();

    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // create valid header proposed by last peer in the committee for round 1
    let wrong_epoch = 3;
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .epoch(wrong_epoch)
        .build();

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch{ theirs: wrong, ours: correct })) if wrong == wrong_epoch && correct == 0 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_authority() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, .. } = create_test_types();

    let parents = Vec::new();
    let peer_id =
        network_public_key_to_libp2p(&committee.last_authority().primary_network_public_key());

    // create valid header proposed by last peer in the committee for round 1
    let wrong_authority = AuthorityIdentifier::dummy_for_test(100);
    let header = committee
        .header_builder_last_authority()
        .author(wrong_authority.clone())
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownAuthority(wrong))) if wrong == wrong_authority.to_string());
    Ok(())
}
