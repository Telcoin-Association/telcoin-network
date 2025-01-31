//! Test for Primary <-> Primary handler.

use crate::{network::RequestHandler, synchronizer::Synchronizer, ConsensusBus, RecentBlocks};
use assert_matches::assert_matches;
use fastcrypto::hash::Hash as _;
use std::{collections::BTreeSet, sync::Arc};
use tn_config::ConsensusConfig;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    error::HeaderError, now, traits::InsecureDefault, AuthorityIdentifier, BlockHash, Certificate,
    CertificateDigest, ExecHeader, SealedHeader, TaskManager,
};
use tracing::debug;

/// The type for holding testng components.
struct TestTypes<DB = MemDatabase> {
    /// Committee committee with authorities that vote.
    committee: CommitteeFixture<DB>,
    // /// The authority that receives messages.
    // authority: &'a AuthorityFixture<DB>,
    /// The handler for requests.
    handler: RequestHandler<DB>,
    /// Consensus config for the authority receiving requests.
    config: ConsensusConfig<DB>,
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
    let synchronizer = Arc::new(Synchronizer::new(config.clone(), &cb));
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);

    // last execution result
    let parent = SealedHeader::seal(ExecHeader::default());

    // set the latest execution result to genesis - test headers are proposed for round 1
    let mut recent = RecentBlocks::new(1);
    recent.push_latest(parent.clone());
    cb.recent_blocks()
        .send(recent)
        .expect("watch channel updates for default parent in primary handler tests");

    let handler = RequestHandler::new(config.clone(), cb.clone(), synchronizer);
    TestTypes { committee, handler, config, parent }
}

#[tokio::test]
async fn test_vote_succeeds() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, parent, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(1) // parent is 0
        .build()?;

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert!(res.is_ok());
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_too_many_parents() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, parent, .. } = create_test_types();

    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // last authority produced 2 certs for round 1
    let mut too_many_parents: Vec<_> = Certificate::genesis(&committee.committee());
    let extra_parent = too_many_parents.last().expect("last cert").clone();
    too_many_parents.push(extra_parent.clone());

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(1) // parent is 0
        .build()?;

    // process vote
    let res = handler.vote(peer_id, header, too_many_parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::TooManyParents(received, expected)) if received == 5 && expected == 4 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_authority_network_key() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, config, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    // workaround until anemo/fastcrypto replaced
    let default = fastcrypto::ed25519::Ed25519PublicKey::insecure_default();
    let random_peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&default)
        .expect("fastcrypto to libp2p");

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(1) // parent is 0
        .build()?;

    // process vote
    let res = handler.vote(random_peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::UnknownNetworkKey(peer_id)) if peer_id == random_peer_id);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_genesis_parent() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, parent, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // start with the expected parents in genesis
    let mut expected_parents: Vec<_> =
        Certificate::genesis(&committee.committee()).iter().map(|x| x.digest()).collect();
    let extra_parent = CertificateDigest::new(BlockHash::random().0);
    expected_parents.push(extra_parent.clone());
    let wrong_genesis: BTreeSet<_> = expected_parents.into_iter().collect();

    // create header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(1) // parent is 0
        .parents(wrong_genesis)
        .build()?;

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::InvalidGenesisParent(wrong)) if wrong == extra_parent);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_execution_result() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, .. } = create_test_types();

    // create header proposed by last peer in the committee for round 1
    let header = committee.header_from_last_authority();
    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::UnknownExecutionResult(wrong_num, wrong_hash)) if wrong_num == 0 && wrong_hash == BlockHash::ZERO);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_header_digest() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // create header proposed by last peer in the committee for round 1
    let mut header = committee.header_from_last_authority();
    // change values so digest doesn't match
    header.latest_execution_block = BlockHash::random();

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    assert_matches!(res, Err(HeaderError::InvalidHeaderDigest));
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_timestamp() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, parent, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // create valid header proposed by last peer in the committee for round 1
    let wrong_time = now() + 100000; // too far in the future
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(wrong_time)
        .build()?;

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::InvalidTimestamp(wrong, _)) if wrong == wrong_time);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_epoch() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, parent, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // create valid header proposed by last peer in the committee for round 1
    let wrong_epoch = 3;
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(1) // parent is 0
        .epoch(wrong_epoch)
        .build()?;

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::InvalidEpoch(wrong, correct)) if wrong == wrong_epoch && correct == 0 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_authority() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, config, parent, .. } = create_test_types();

    let parents = Vec::with_capacity(0);
    let peer_id = config
        .network_config()
        .ed25519_fastcrypto_to_libp2p(&committee.last_authority().primary_network_public_key())
        .expect("valid peer id for last authority");

    // create valid header proposed by last peer in the committee for round 1
    let wrong_authority = AuthorityIdentifier(100);
    let header = committee
        .header_builder_last_authority()
        .author(wrong_authority)
        .latest_execution_block(parent.hash())
        .latest_execution_block_num(parent.number)
        .created_at(1) // parent is 0
        .build()?;

    // process vote
    let res = handler.vote(peer_id, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(HeaderError::UnknownAuthority(wrong)) if wrong == wrong_authority.to_string());
    Ok(())
}
