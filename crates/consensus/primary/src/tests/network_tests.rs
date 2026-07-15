//! Test for Primary <-> Primary handler.

use crate::{
    error::PrimaryNetworkError,
    network::{
        message::{PrimaryGossip, PrimaryResponse},
        MissingCertificatesRequest, PendingStreamRequest, RequestHandler, StreamRequestKind,
        MAX_CONCURRENT_EPOCH_STREAMS, MAX_CONSENSUS_CERTS, MAX_TALLIES_PER_SIGNER_PER_NUMBER,
        PENDING_REQUEST_TIMEOUT,
    },
    state_sync::StateSynchronizer,
    ConsensusBus, ConsensusBusApp, NodeMode, RecentBlocks,
};
use assert_matches::assert_matches;
use rand::{rngs::StdRng, SeedableRng};
use roaring::RoaringBitmap;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tn_config::{ConsensusConfig, KeyConfig, Parameters};
use tn_network_libp2p::{GossipMessage, TopicHash};
use tn_storage::{
    consensus::{ConsensusChain, ConsensusChainError},
    consensus_pack::PackError,
    mem_db::MemDatabase,
    tables::Votes,
};
use tn_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use tn_types::{
    encode, error::HeaderError, now, to_intent_message, AuthorityIdentifier, BlockHash,
    BlockHeader, BlockNumHash, BlsKeypair, BlsPublicKey, BlsSigner as _, Certificate,
    CommittedSubDag, ConsensusHeaderDigest, ConsensusNumHash, ConsensusResult, Database, Epoch,
    EpochRecord, EpochVote, ExecHeader, Hash as _, HeaderDigest, ReputationScores, Round,
    SealedHeader, TaskManager, TnReceiver as _, VoteDigest, VoteInfo, B256,
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
        missing_req.get_bounds(1_000, 4).expect("decode missing bounds");
    assert_eq!(expected_gc_round, decoded_gc_round);
    assert_eq!(expected_skip_rounds, decoded_skip_rounds);
}

/// A `RoaringBitmap` is a *compressed* set, so a `MissingCertificates` request that stays under the
/// 1 MiB RPC size cap can still decode to millions of skip rounds (and, via run containers -- which
/// the deserializer accepts -- to the full ~4.29e9 `u32` range; see GHSA-wwqq-q2xx-4jf9 /
/// GHSA-4ggp-fcpj-g9f3). `get_bounds` must reject such a request by cardinality *before*
/// materializing the set into a `BTreeSet`, otherwise `.collect()` allocates tens of GB and
/// OOM-kills the validator.
#[test]
fn test_get_bounds_rejects_oversized_skip_rounds() {
    use roaring::RoaringBitmap;

    let max_skip_rounds = 1_000;

    // 5M set bits serialize to ~0.6 MiB: comfortably under the 1 MiB RPC cap, yet 5000x the
    // per-authority skip-round limit. The bound is checked on cardinality (not serialized size), so
    // a regression here fails by returning `Ok` rather than by OOM-ing the test runner.
    let mut bomb = RoaringBitmap::new();
    assert_eq!(bomb.insert_range(0..=5_000_000), 5_000_001);
    let mut serialized = Vec::new();
    bomb.serialize_into(&mut serialized).expect("serialize skip-round bitmap");
    assert!(
        serialized.len() < 1024 * 1024,
        "bomb stays under the RPC cap: {} bytes",
        serialized.len()
    );

    let request = MissingCertificatesRequest {
        exclusive_lower_bound: 0,
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), serialized)],
        max_response_size: 0,
    };

    // Rejected on cardinality, before the ~5M-element `BTreeSet` is materialized (the ~77 dense
    // containers clear the container-count gate, so the cardinality gate is what fires).
    assert_matches!(
        request.get_bounds(max_skip_rounds, 4),
        Err(PrimaryNetworkError::InvalidRequest(msg)) if msg.contains("too large:")
    );
}

/// Nothing but the committee size bounds how many `skip_rounds` entries a request can name:
/// each accepted entry costs a bitmap decode plus a retained `BTreeSet`, so tens of thousands
/// of entries (each individually under the per-authority limit) would still materialize a
/// multi-hundred-MiB map. `get_bounds` must reject on entry count before decoding anything.
#[test]
fn test_get_bounds_rejects_too_many_authorities() {
    use roaring::RoaringBitmap;

    let max_skip_rounds = 1_000;
    let max_authorities = 4;

    // One more entry than the committee has authorities; every entry is individually tiny and
    // well-formed, so the rejection can only come from the entry-count gate.
    let skip_rounds: Vec<_> = (0..=4u8)
        .map(|i| {
            let mut serialized = Vec::new();
            [1u32, 2, 3]
                .into_iter()
                .collect::<RoaringBitmap>()
                .serialize_into(&mut serialized)
                .expect("serialize skip-round bitmap");
            (AuthorityIdentifier::dummy_for_test(i), serialized)
        })
        .collect();
    assert_eq!(skip_rounds.len(), max_authorities + 1);

    let request =
        MissingCertificatesRequest { exclusive_lower_bound: 0, skip_rounds, max_response_size: 0 };

    assert_matches!(
        request.get_bounds(max_skip_rounds, max_authorities),
        Err(PrimaryNetworkError::InvalidRequest(msg)) if msg.contains("exceeds committee size")
    );
}

/// A skip-round count at or below the limit still decodes normally: the cardinality gate must not
/// reject legitimate `MissingCertificates` requests.
#[test]
fn test_get_bounds_accepts_within_limit_skip_rounds() {
    use roaring::RoaringBitmap;

    let max_skip_rounds = 1_000;

    let mut bitmap = RoaringBitmap::new();
    assert_eq!(bitmap.insert_range(0..=(max_skip_rounds as u32 - 1)), max_skip_rounds as u64);
    let mut serialized = Vec::new();
    bitmap.serialize_into(&mut serialized).expect("serialize skip-round bitmap");

    let request = MissingCertificatesRequest {
        exclusive_lower_bound: 10,
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), serialized)],
        max_response_size: 0,
    };

    let (lower_bound, skip_rounds) =
        request.get_bounds(max_skip_rounds, 4).expect("within-limit ok");
    assert_eq!(lower_bound, 10);
    assert_eq!(skip_rounds[&AuthorityIdentifier::dummy_for_test(0)].len(), max_skip_rounds);
}

#[test]
// for primary::network::message
fn test_missing_certs_request_skip_round_overflow() {
    let mut serialized = Vec::new();
    [1u32, 2]
        .into_iter()
        .collect::<RoaringBitmap>()
        .serialize_into(&mut serialized)
        .expect("serialize skip rounds");
    // `exclusive_lower_bound + 2` exceeds u32::MAX and must surface as an invalid request
    // instead of wrapping (release) or panicking (debug) on peer-supplied input
    let missing_req = MissingCertificatesRequest {
        exclusive_lower_bound: Round::MAX - 1,
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), serialized)],
        max_response_size: 10,
    };
    assert_matches!(missing_req.get_bounds(1_000, 4), Err(PrimaryNetworkError::InvalidRequest(_)));
}

#[test]
// for primary::network::message
fn test_get_bounds_rejects_decompression_bomb() {
    // A serialized RoaringBitmap header declaring 65_536 run containers is only 4 bytes on the
    // wire, yet `deserialize_from` would expand it to ~512 MiB of heap (roaring 0.10 has no run
    // store, so each run container becomes an ~8 KiB array/bitmap store). The container count is
    // read straight from the header and rejected before a single container is allocated.
    // See GHSA-4ggp-fcpj-g9f3.
    const RUN_COOKIE: u32 = 12347;
    let container_count: u32 = 65_536;
    let cookie = ((container_count - 1) << 16) | RUN_COOKIE;
    let request = MissingCertificatesRequest {
        exclusive_lower_bound: 0,
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), cookie.to_le_bytes().to_vec())],
        max_response_size: 10,
    };
    assert_matches!(request.get_bounds(1_000, 4), Err(PrimaryNetworkError::InvalidRequest(_)));
}

#[test]
// for primary::network::message
fn test_get_bounds_rejects_oversized_cardinality() {
    // A well-formed bitmap whose cardinality exceeds the limit is rejected before the
    // `O(cardinality)` collect, even though it occupies a single cheap container.
    let mut serialized = Vec::new();
    (0..=1_000u32)
        .collect::<RoaringBitmap>()
        .serialize_into(&mut serialized)
        .expect("serialize skip rounds");
    let request = MissingCertificatesRequest {
        exclusive_lower_bound: 0,
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), serialized)],
        max_response_size: 10,
    };
    // 1_001 rounds against a limit of 1_000
    assert_matches!(request.get_bounds(1_000, 4), Err(PrimaryNetworkError::InvalidRequest(_)));
}

#[test]
// for primary::network::message
fn test_get_bounds_accepts_cardinality_at_limit() {
    // The bound is inclusive: a bitmap with exactly the maximum number of rounds is accepted.
    let expected: BTreeSet<Round> = (1..=1_000).collect();
    let mut serialized = Vec::new();
    expected
        .iter()
        .copied()
        .collect::<RoaringBitmap>()
        .serialize_into(&mut serialized)
        .expect("serialize skip rounds");
    let origin = AuthorityIdentifier::dummy_for_test(0);
    let request = MissingCertificatesRequest {
        exclusive_lower_bound: 0,
        skip_rounds: vec![(origin.clone(), serialized)],
        max_response_size: 10,
    };
    let (lower_bound, skip_rounds) =
        request.get_bounds(1_000, 4).expect("bitmap at the limit is accepted");
    assert_eq!(lower_bound, 0);
    assert_eq!(skip_rounds.get(&origin), Some(&expected));
}

#[test]
// for primary::network::message
fn test_get_bounds_accepts_container_count_at_limit() {
    // Pin the pre-deserialize container-count gate's inclusive boundary: a legitimate bitmap of
    // `cap` rounds, each in a distinct 65_536-round block, occupies exactly `cap` containers and
    // must be accepted. A `<` instead of `<=` on the container check would wrongly reject it, and
    // the single-container fixtures above would not catch that. See GHSA-4ggp-fcpj-g9f3.
    let expected: BTreeSet<Round> = (0..1_000u32).map(|block| block * 65_536).collect();
    let mut serialized = Vec::new();
    expected
        .iter()
        .copied()
        .collect::<RoaringBitmap>()
        .serialize_into(&mut serialized)
        .expect("serialize skip rounds");
    let origin = AuthorityIdentifier::dummy_for_test(0);
    let request = MissingCertificatesRequest {
        exclusive_lower_bound: 0,
        skip_rounds: vec![(origin.clone(), serialized)],
        max_response_size: 10,
    };
    let (_, skip_rounds) =
        request.get_bounds(1_000, 4).expect("max-container bitmap at the limit is accepted");
    assert_eq!(skip_rounds.get(&origin), Some(&expected));
}

#[test]
// for primary::network::message
fn test_get_bounds_rejects_malformed_bitmap_header() {
    // An unrecognized cookie and a truncated header are both rejected without deserializing.
    let unknown_cookie = vec![0xFF, 0xFF, 0xFF, 0xFF];
    let truncated = vec![0x3A, 0x30]; // first two bytes of the NO_RUNCONTAINER cookie (12346)
    for serialized in [unknown_cookie, truncated] {
        let request = MissingCertificatesRequest {
            exclusive_lower_bound: 0,
            skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), serialized)],
            max_response_size: 10,
        };
        assert_matches!(request.get_bounds(1_000, 4), Err(PrimaryNetworkError::InvalidRequest(_)));
    }
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
    recent.push_latest(
        0,
        ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
        Some(parent.clone()),
    );
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

/// Helper function to create an instance of [RequestHandler] for the first authority of a
/// committee at the supplied epoch. Mirrors [`create_test_types_with_params`] but threads the
/// epoch through the [`CommitteeFixture`] builder so that the handler's view of the committee
/// reports `epoch` rather than the default `0`.
async fn create_test_types_at_epoch(path: &Path, epoch: Epoch) -> TestTypes {
    let committee =
        CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).epoch(epoch).build();
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
    recent.push_latest(
        0,
        ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
        Some(parent.clone()),
    );
    cb.app().recent_blocks().send_replace(recent);

    let consensus_chain =
        ConsensusChain::new_for_test(path.to_owned(), committee.committee()).await.unwrap();
    let consensus_bus = cb.app().clone();
    let handler =
        RequestHandler::new(config.clone(), cb.app().clone(), synchronizer, consensus_chain);
    TestTypes { committee, handler, parent, consensus_bus, task_manager }
}

/// Like [`create_test_types`] but with an explicit committee size, so a test can exercise a
/// quorum threshold larger than the default four-node committee (e.g. to keep a set of colluding
/// co-signers strictly below quorum).
async fn create_test_types_with_committee_size(
    path: &Path,
    committee_size: NonZeroUsize,
) -> TestTypes {
    let committee = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(committee_size)
        .build();
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
    recent.push_latest(
        0,
        ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
        Some(parent.clone()),
    );
    cb.app().recent_blocks().send_replace(recent);

    let consensus_chain =
        ConsensusChain::new_for_test(path.to_owned(), committee.committee()).await.unwrap();
    let consensus_bus = cb.app().clone();
    let handler =
        RequestHandler::new(config.clone(), cb.app().clone(), synchronizer, consensus_chain);
    TestTypes { committee, handler, parent, consensus_bus, task_manager }
}

#[tokio::test]
async fn test_retrieve_consensus_output() {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;
    let committee_obj = committee.committee();

    // Populate a few consensus outputs in the epoch-0 pack. Numbers start at 1 so each is greater
    // than the latest consensus number and is actually saved (mirror of storage_tests.rs).
    for number in 1..=3u64 {
        let cert = Certificate::default();
        let sub_dag = CommittedSubDag::new(
            vec![cert.clone()],
            cert,
            number,
            ReputationScores::new(&committee_obj),
            None,
        );
        handler.consensus_chain().write_subdag_for_test(number, sub_dag).await;
    }

    // The server serves the raw output bytes for every stored number.
    for number in 1..=3u64 {
        let bytes = handler
            .consensus_output_bytes(number)
            .await
            .expect("stored consensus output should be served");
        assert!(!bytes.is_empty(), "served output {number} bytes must not be empty");
    }

    // A number we do not have is a benign miss: it errors and carries no penalty.
    let err =
        handler.consensus_output_bytes(999).await.expect_err("unknown consensus output must error");
    assert_matches!(
        err,
        PrimaryNetworkError::ConsensusChainError(ConsensusChainError::PackError(
            PackError::ConsensusNumberTooHigh
        ))
    );
    let penalty: Option<tn_network_libp2p::Penalty> = (&err).into();
    assert!(penalty.is_none(), "an unknown consensus output must not penalize the peer");
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

/// Regression test for issue #803: a node that is **not** a member of the current committee must
/// reject an inbound vote request with a graceful error instead of panicking. The vote path
/// previously called `authority_id().expect("only validators can vote")`, which aborts the whole
/// process for any non-validator that reaches it (e.g. an observer served a misrouted request).
#[tokio::test]
async fn test_vote_non_committee_member_returns_error() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();

    // Build a ConsensusConfig keyed by an identity that is NOT in the committee, so that
    // `authority_id()` is `None` (a non-validator / observer node). Reuse the fixture's
    // genesis-aligned `Config` and `NetworkConfig`, swapping only the signing key.
    let base = committee.first_authority().consensus_config();
    let outsider_key =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let config = ConsensusConfig::new_with_committee_for_test(
        base.config().clone(),
        MemDatabase::default(),
        outsider_key,
        committee.committee(),
        base.network_config().clone(),
    )?;
    assert!(config.authority_id().is_none(), "outsider key must not be a committee member");

    // Build the handler against the outsider config, mirroring `create_test_types_with_params`.
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    // Seed the latest execution result to genesis so a round-1 header passes execution checks.
    let parent = SealedHeader::seal_slow(ExecHeader::default());
    let mut recent = RecentBlocks::new(1);
    recent.push_latest(
        0,
        ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
        Some(parent.clone()),
    );
    cb.app().recent_blocks().send_replace(recent);

    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.committee())
            .await
            .unwrap();
    let handler =
        RequestHandler::new(config.clone(), cb.app().clone(), synchronizer, consensus_chain);

    // A valid round-1 header proposed by a real committee member (identical to test_vote_succeeds),
    // so the request passes the peer/author and header validation and reaches the former panic
    // site.
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // The non-validator must return a graceful error rather than panicking.
    let res = handler.vote(peer, header, vec![]).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::NotCommitteeMember)));

    // keep the synchronizer's task alive until the vote has been processed
    drop(task_manager);
    Ok(())
}

/// Regression test: a stale `VoteInfo` from a prior epoch left over in the `Votes` table must
/// not block a vote on a header from the current epoch. Without the explicit
/// `header.epoch() > vote_info.epoch()` branch in the vote handler, an older entry's `round`
/// would short-circuit the equivocation check and reject the new header as a duplicate vote.
#[tokio::test]
async fn test_vote_succeeds_with_stale_prior_epoch_vote_info() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types_at_epoch(temp_dir.path(), 1).await;

    // Seed the Votes table with a stale entry from epoch 0 keyed under the authority that
    // will submit the current-epoch header. F3 should clear this row on epoch close, so the
    // scenario is theoretically impossible — but the new branch defends against the row
    // leaking, and the regression test pins that defence in place.
    let author_id = committee.last_authority().id();
    let stale = VoteInfo { epoch: 0, round: 5, vote_digest: VoteDigest::default() };
    committee
        .first_authority()
        .consensus_config()
        .node_storage()
        .insert::<Votes>(&author_id, &stale)?;

    // current-epoch (epoch=1) header from the same author at round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    let res = handler.vote(peer, header, vec![]).await?;
    assert_matches!(res, PrimaryResponse::Vote(_));

    // The stale row must have been overwritten with a current-epoch entry.
    let stored: Option<VoteInfo> =
        committee.first_authority().consensus_config().node_storage().get::<Votes>(&author_id)?;
    let stored = stored.expect("vote info written");
    assert_eq!(stored.epoch, committee.committee().epoch());
    assert_eq!(stored.round, 1);

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
    let extra_parent = HeaderDigest::new(BlockHash::random().0);
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

/// Regression for #802: the Byzantine errors the vote RPC returns must be penalizable.
///
/// `process_vote_request` now reports `(&err).into()` before collapsing the result into a
/// response, exactly like every sibling request handler. That wiring only penalizes a
/// misbehaving peer if the vote handler's error variants map to `Some(Penalty)` in the
/// central `From<&PrimaryNetworkError>` table. Pin that contract here so a future error
/// reshuffle that silently downgrades a vote error to `None` (re-opening #802 from the
/// other side) is caught.
#[tokio::test]
async fn test_vote_byzantine_errors_are_penalizable() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    // A vote request whose peer is not the header's author -> PeerNotAuthor.
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let not_author = BlsPublicKey::default();
    let err = handler
        .vote(not_author, header, Vec::new())
        .await
        .expect_err("a vote whose peer is not the header author must fail");
    assert_matches!(err, PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor));
    let penalty: Option<tn_network_libp2p::Penalty> = (&err).into();
    assert_matches!(
        penalty,
        Some(tn_network_libp2p::Penalty::Fatal),
        "a non-author vote must penalize the peer so process_vote_request can report it"
    );

    // A vote request authored by an authority outside the committee -> UnknownAuthority.
    let wrong_authority = AuthorityIdentifier::dummy_for_test(100);
    let header = committee
        .header_builder_last_authority()
        .author(wrong_authority.clone())
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();
    let err = handler
        .vote(peer, header, Vec::new())
        .await
        .expect_err("a vote authored by an unknown authority must fail");
    assert_matches!(
        err,
        PrimaryNetworkError::InvalidHeader(HeaderError::UnknownAuthority(ref a))
            if *a == wrong_authority.to_string()
    );
    let penalty: Option<tn_network_libp2p::Penalty> = (&err).into();
    assert_matches!(
        penalty,
        Some(tn_network_libp2p::Penalty::Fatal),
        "an unknown-authority vote must penalize the peer"
    );

    Ok(())
}

/// #802 follow-through: header errors that reflect a LOCAL or transient condition (our own
/// storage failure, or our execution lagging behind a peer that is merely ahead) must NOT
/// penalize the peer now that the vote RPC is wired into the penalty pipeline. Pin the
/// central table so these stay `None`, consistent with the sibling
/// `PrimaryNetworkError::Storage` and `*::Timeout` arms.
#[test]
fn test_local_header_errors_are_not_penalized() {
    let storage: PrimaryNetworkError = HeaderError::Storage(eyre::eyre!("local db failure")).into();
    let penalty: Option<tn_network_libp2p::Penalty> = (&storage).into();
    assert!(penalty.is_none(), "a local storage failure must not penalize the peer");

    let exec_lag: PrimaryNetworkError =
        HeaderError::UnknownExecutionResult(BlockNumHash::new(0, BlockHash::default())).into();
    let penalty: Option<tn_network_libp2p::Penalty> = (&exec_lag).into();
    assert!(
        penalty.is_none(),
        "a peer that is merely ahead of our execution must not be penalized"
    );
}

/// Test that primary pub/sub is enforcing topics.
#[tokio::test]
async fn test_primary_batch_gossip_topics() {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, .. } = create_test_types(temp_dir.path()).await;

    let gossip = PrimaryGossip::Certificate(Box::new(Certificate::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::primary_topic(0));
    let goodish_msg =
        GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&goodish_msg).await;
    // This will be rejected for other reasons, but make sure not for an invalid topic.
    assert!(!matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Consensus(Box::new(ConsensusResult::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::consensus_output_topic(0));
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&good_msg).await.is_ok());

    // EpochVote::default()'s all-zero public_key is not a committee member, so the committee gate
    // rejects it (before the signature verify); see GHSA-j2g4-553f-875r.
    let gossip = PrimaryGossip::EpochVote(Box::new(EpochVote::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::epoch_vote_topic(0));
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&good_msg).await;
    // Not rejected for InvalidTopic — rejected for non-committee membership instead.
    assert!(!matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Certificate(Box::new(Certificate::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::epoch_vote_topic(0));
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&bad_msg).await;
    // This will be rejected for other reasons, but make sure it is for an invalid topic.
    assert!(matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Consensus(Box::new(ConsensusResult::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::primary_topic(0));
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&bad_msg).await.is_err());

    let gossip = PrimaryGossip::EpochVote(Box::new(EpochVote::default()));
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::consensus_output_topic(0));
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&bad_msg).await.is_err());
}

// ============================================================================
// EpochVote Authorization-Before-Verify Tests (GHSA-j2g4-553f-875r)
// ============================================================================
// `epoch_vote_topic` is an open gossip topic, so a non-committee observer can publish an
// `EpochVote` with arbitrary fields. The handler must authorize a vote (committee membership by
// epoch number) *before* paying the expensive BLS pairing verify, must drop a vote for an
// unknown epoch before the verify, and must not turn a bad vote into a `Fatal` penalty charged
// to the honest relayer.

/// Build a gossip message carrying `vote` on `epoch_vote_topic` for `chain_id`.
fn epoch_vote_gossip(vote: EpochVote, chain_id: u64) -> GossipMessage {
    let data = encode(&PrimaryGossip::EpochVote(Box::new(vote)));
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::epoch_vote_topic(chain_id));
    GossipMessage { source: None, data, sequence_number: None, topic }
}

/// A vote whose `public_key` is not in the committee for a *known* epoch must be rejected by the
/// committee gate *before* the signature verify. The error variant is the evidence: had the
/// verify run first, the garbage signature would surface as `InvalidHeader(PeerNotAuthor)` (a
/// `Fatal` penalty charged to the relayer on the gossip path); the gate running first surfaces
/// the benign, non-penalizing `PeerNotInCommittee`. This is the core guarantee of
/// GHSA-j2g4-553f-875r.
#[tokio::test]
async fn test_epoch_vote_non_committee_rejected_before_verify() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    // `EpochVote::default()` is epoch 0 (a known committee in the fixture) with an all-zero
    // `public_key` that is not a committee member and a garbage signature.
    let mut rx = consensus_bus.subscribe_new_epoch_votes();
    let res = handler.process_gossip(&epoch_vote_gossip(EpochVote::default(), 0)).await;

    assert_matches!(
        res,
        Err(PrimaryNetworkError::PeerNotInCommittee(_)),
        "a non-committee vote for a known epoch must be rejected by the committee gate before the \
         verify (which would yield the Fatal PeerNotAuthor charged to the relayer): {res:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
        "a rejected vote must not reach the collector"
    );
    Ok(())
}

/// A vote for an epoch whose committee the node does not know must be dropped *before* the verify
/// (so a garbage `epoch`/`epoch_hash` cannot force a pairing verify), and dropped silently — no
/// error, no penalty. Under the pre-fix ordering the garbage signature ran through the verify and
/// surfaced `InvalidHeader(PeerNotAuthor)`; here it returns `Ok`.
#[tokio::test]
async fn test_epoch_vote_unknown_epoch_dropped_before_verify() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    // Epoch 9999 has no committee in the fixture, so `get_committee` returns `None`.
    let mut rx = consensus_bus.subscribe_new_epoch_votes();
    let vote = EpochVote { epoch: 9999, ..Default::default() };
    let res = handler.process_gossip(&epoch_vote_gossip(vote, 0)).await;

    assert!(
        res.is_ok(),
        "an unknown-epoch vote must be dropped before the verify (pre-fix this returned \
         Err(PeerNotAuthor) from the verify): {res:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
        "an unknown-epoch vote must not reach the collector"
    );
    Ok(())
}

/// A valid vote from a committee member of a known epoch must pass the committee gate and the
/// verify and be forwarded to the collector. Guards the reorder against over-rejecting
/// legitimate votes (a liveness regression).
#[tokio::test]
async fn test_epoch_vote_valid_committee_member_forwarded() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    // Sign a vote for epoch 0 (the fixture's current committee) with a real committee member's
    // key, so `public_key` is a committee member and `check_signature` succeeds.
    let auth = committee.authorities().next().expect("committee has authorities");
    let key_config = auth.consensus_config().key_config().clone();
    let epoch_rec = EpochRecord { epoch: 0, ..Default::default() };
    let vote = epoch_rec.sign_vote(&key_config);
    assert!(vote.check_signature(), "test vote must be validly signed");

    let mut rx = consensus_bus.subscribe_new_epoch_votes();
    handler.process_gossip(&epoch_vote_gossip(vote, 0)).await?;

    let received = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("a valid committee vote must be forwarded to the collector")
        .expect("epoch vote channel unexpectedly closed");
    assert_eq!(received.public_key, vote.public_key);
    assert_eq!(received.epoch, vote.epoch);
    Ok(())
}

/// Documents the residual of GHSA-j2g4-553f-875r. An attacker who copies a committee member's
/// public BLS key (public information) passes the committee gate, so a garbage-signed
/// impersonation vote still reaches `check_signature` (forcing one pairing verify) and fails it
/// with `InvalidHeader(PeerNotAuthor)`. That variant is relayer-attributed and `Fatal`, matching
/// how the codebase attributes other embedded-signer faults; the peer manager's `Validator` trust
/// exemption means a committee relayer is never banned, so consensus-mesh peers are safe. Fully
/// removing this residual (the forced verify and the attribution edge for a non-committee relayer)
/// needs the network-layer topic restriction, not the handler.
#[tokio::test]
async fn test_epoch_vote_committee_key_bad_sig_reaches_verify() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    // A real committee member's public key with a default (invalid) signature: the gate admits it
    // (the key is in the committee), so the verify runs and fails.
    let member = committee
        .authorities()
        .next()
        .expect("committee has authorities")
        .consensus_config()
        .key_config()
        .public_key();
    let vote = EpochVote { epoch: 0, public_key: member, ..Default::default() };

    let res = handler.process_gossip(&epoch_vote_gossip(vote, 0)).await;
    assert_matches!(
        res,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor)),
        "a committee-key vote with a bad signature must reach and fail the verify (PeerNotAuthor), \
         documenting the copied-key residual: {res:?}"
    );
    Ok(())
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
    recent.push_latest(18, ConsensusNumHash::new(0, ConsensusHeaderDigest::default()), None);
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
    recent.push_latest(5, ConsensusNumHash::new(0, ConsensusHeaderDigest::default()), None);
    consensus_bus.recent_blocks().send_replace(recent);
    consensus_bus.committed_round_updates().send_replace(5);

    let result = handler.behind_consensus(0, 60, None).await;
    assert!(result, "genuinely behind node should be detected");
}

/// Server-side partial epoch streaming: `send_epoch_over_stream` with `Some(stop_number)` must
/// emit exactly the verifiable prefix (every output up to and including the stop number) of the
/// in-progress current epoch, and a later cutoff must extend that same prefix.
#[tokio::test]
async fn test_send_partial_epoch_over_stream() {
    use tokio::io::AsyncReadExt as _;

    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;
    let committee_obj = committee.committee();
    let peer = *committee.first_authority().authority().protocol_key();

    // Populate the in-progress (never finalized) epoch-0 pack with some outputs.
    let num_outputs = 15u64;
    for number in 1..=num_outputs {
        let cert = Certificate::default();
        let sub_dag = CommittedSubDag::new(
            vec![cert.clone()],
            cert,
            number,
            ReputationScores::new(&committee_obj),
            None,
        );
        handler.consensus_chain().write_subdag_for_test(number, sub_dag).await;
    }

    // The server streams a partial prefix up to consensus number `k`.
    let k = 9u64;
    let mut sent = Vec::new();
    RequestHandler::<MemDatabase>::send_epoch_over_stream(
        &mut sent,
        handler.consensus_chain(),
        0,
        Some(k),
        Duration::from_secs(10),
        peer,
    )
    .await
    .expect("send partial epoch stream");

    // The streamed bytes must equal exactly the verifiable prefix the chain exposes — i.e. the
    // data file truncated at `output_end(k)`.
    let (stream, len) =
        handler.consensus_chain().get_partial_epoch_stream(0, k).await.expect("partial stream");
    let mut expected = Vec::new();
    stream.take(len).read_to_end(&mut expected).await.unwrap();
    assert_eq!(sent.len() as u64, len, "streamed byte count must equal the partial cutoff");
    assert_eq!(sent, expected, "streamed bytes must equal the verifiable prefix");

    // A later cutoff streams strictly more, and the smaller prefix is a true prefix of it.
    let mut sent_more = Vec::new();
    RequestHandler::<MemDatabase>::send_epoch_over_stream(
        &mut sent_more,
        handler.consensus_chain(),
        0,
        Some(num_outputs),
        Duration::from_secs(10),
        peer,
    )
    .await
    .expect("send larger partial stream");
    assert!(sent_more.len() > sent.len(), "a later cutoff must stream more bytes");
    assert_eq!(&sent_more[..sent.len()], &sent[..], "smaller prefix must prefix the larger one");
}

/// Serving a partial prefix over the sync protocol (`EpochPackPartial`, item 9)
/// writes `Ack` then streams exactly the verifiable prefix bytes as `Data` frames:
/// the bytes reassembled from the frames must equal the data file truncated at
/// `output_end(k)`, and a later cutoff must stream strictly more (with the smaller
/// prefix a true prefix of it): the sync mirror of `test_send_partial_epoch_over_stream`.
#[tokio::test]
async fn test_sync_partial_epoch_pack_over_stream() {
    use tokio::io::AsyncReadExt as _;

    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;
    let committee_obj = committee.committee();
    let peer = *committee.first_authority().authority().protocol_key();

    // Populate the in-progress (never finalized) epoch-0 pack with some outputs.
    let num_outputs = 15u64;
    for number in 1..=num_outputs {
        let cert = Certificate::default();
        let sub_dag = CommittedSubDag::new(
            vec![cert.clone()],
            cert,
            number,
            ReputationScores::new(&committee_obj),
            None,
        );
        handler.consensus_chain().write_subdag_for_test(number, sub_dag).await;
    }

    // Reassemble the pack bytes streamed by the sync serve for a stop point `k`: read
    // the leading `Ack`, then feed the remaining `Data`/`End` frames through
    // `sync_pack_reader` (the reader the real requester uses).
    async fn reassemble_partial(
        consensus_chain: &tn_storage::consensus::ConsensusChain,
        stop_number: u64,
        peer: BlsPublicKey,
    ) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        crate::network::sync_codec::send_sync_epoch_pack_over_stream(
            &mut out,
            consensus_chain,
            0,
            Some(stop_number),
            Duration::from_secs(10),
            peer,
        )
        .await
        .expect("serve partial epoch pack over sync");

        let mut cursor = futures::io::Cursor::new(out);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack = tn_network_libp2p::read_frame::<_, tn_network_libp2p::PrimarySyncRequest>(
            &mut cursor,
            &mut dec,
            &mut comp,
            crate::network::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read ack frame");
        assert_matches!(ack, tn_network_libp2p::SyncFrame::Ack);

        let mut reassembled = Vec::new();
        crate::network::sync_codec::sync_pack_reader(cursor)
            .read_to_end(&mut reassembled)
            .await
            .expect("reassemble partial pack data frames");
        reassembled
    }

    // The reassembled bytes must equal the verifiable prefix the chain exposes, i.e.
    // the data file truncated at `output_end(k)`.
    let k = 9u64;
    let reassembled = reassemble_partial(handler.consensus_chain(), k, peer).await;
    let (stream, len) =
        handler.consensus_chain().get_partial_epoch_stream(0, k).await.expect("partial stream");
    let mut expected = Vec::new();
    stream.take(len).read_to_end(&mut expected).await.unwrap();
    assert_eq!(reassembled.len() as u64, len, "streamed byte count must equal the partial cutoff");
    assert_eq!(reassembled, expected, "reassembled bytes must equal the verifiable prefix");

    // A later cutoff streams strictly more, and the smaller prefix is a true prefix of it.
    let reassembled_more = reassemble_partial(handler.consensus_chain(), num_outputs, peer).await;
    assert!(reassembled_more.len() > reassembled.len(), "a later cutoff must stream more bytes");
    assert_eq!(
        &reassembled_more[..reassembled.len()],
        &reassembled[..],
        "smaller prefix must prefix the larger one"
    );
}

/// A full epoch pack the responder does not hold is shed with
/// `Deny(Unavailable)`, so a sync requester retries another peer immediately
/// instead of waiting out its ack timeout. (The `Ack`+`Data`+`End` happy path is
/// unit-tested in `sync_codec` and exercised end-to-end by the ignored
/// observer-pack-import e2e test.)
#[tokio::test]
async fn test_sync_epoch_pack_unavailable_denies() {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { handler, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;
    let peer = BlsPublicKey::default();

    // epoch 0 has no finalized pack, so the responder cannot serve a full epoch pack
    let mut out: Vec<u8> = Vec::new();
    crate::network::sync_codec::send_sync_epoch_pack_over_stream(
        &mut out,
        handler.consensus_chain(),
        0,
        None,
        Duration::from_secs(5),
        peer,
    )
    .await
    .expect("serving an unavailable pack sheds cleanly without erroring");

    // the first (and only) frame must be Deny(Unavailable)
    let (mut dec, mut comp) = (Vec::new(), Vec::new());
    let frame = tn_network_libp2p::read_frame::<_, tn_network_libp2p::PrimarySyncRequest>(
        &mut futures::io::Cursor::new(out),
        &mut dec,
        &mut comp,
        crate::network::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
    )
    .await
    .expect("read deny frame");
    assert_matches!(
        frame,
        tn_network_libp2p::SyncFrame::Deny(tn_network_libp2p::DenyReason::Unavailable)
    );
}

/// A peer that re-requests the same epoch while an entry is already pending must not be
/// able to reset the cleanup timer. If the replacement path rearmed `created_at`, a peer
/// could re-request every 20s and hold a slot forever. This test exercises the
/// preservation logic used by `process_epoch_stream` and verifies the entry is evicted on
/// schedule relative to the *original* insertion time.
#[tokio::test]
async fn test_pending_epoch_stream_replacement_preserves_created_at() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_EPOCH_STREAMS));
    let mut pending_map: HashMap<(BlsPublicKey, B256), PendingStreamRequest> = HashMap::new();

    let peer = BlsPublicKey::default();
    let digest = B256::random();
    let key = (peer, digest);
    let epoch: u32 = 7;
    let kind = StreamRequestKind::EpochPack(epoch);

    // initial insertion at T0, where T0 is just past the cleanup horizon so we can
    // assert eviction without waiting on wall-clock time
    let t0 = Instant::now() - PENDING_REQUEST_TIMEOUT - Duration::from_secs(1);
    let permit = semaphore.clone().try_acquire_owned().expect("permit available");
    pending_map.insert(key, PendingStreamRequest::new_with_created_at(kind, permit, t0));

    // simulate a re-request: production code looks up the existing entry's
    // `created_at` and reuses it when building the replacement
    let new_permit = semaphore.clone().try_acquire_owned().expect("permit available");
    let preserved_created_at =
        pending_map.get(&key).map(|p| p.created_at).unwrap_or_else(Instant::now);
    let replacement =
        PendingStreamRequest { kind, created_at: preserved_created_at, _permit: new_permit };
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

// ============================================================================
// Consensus Result Signature Aggregation Tests
// ============================================================================
// These tests cover how the handler counts the validator signatures gossiped for a
// consensus result. A result is only "published" (forwarded to followers) once a quorum of
// *distinct* validators have signed it, and each validator must count at most once.

/// Build a gossip message carrying a [`ConsensusResult`] signed by `auth` over the given
/// `(epoch, round, number, hash)` tuple. Mirrors how the subscriber publishes results: the
/// signature is over `to_intent_message(ConsensusResult::digest_data(..))`.
fn signed_consensus_gossip(
    auth: &AuthorityFixture<MemDatabase>,
    epoch: Epoch,
    round: Round,
    number: u64,
    hash: ConsensusHeaderDigest,
) -> GossipMessage {
    let digest = ConsensusResult::digest_data(epoch, round, number, hash);
    let config = auth.consensus_config();
    let key_config = config.key_config();
    let signature = key_config.request_signature_direct(&encode(&to_intent_message(digest)));
    let validator = key_config.public_key();
    let result = ConsensusResult { epoch, round, number, hash, validator, signature };
    let data = encode(&PrimaryGossip::Consensus(Box::new(result)));
    let topic =
        TopicHash::from_raw(tn_config::LibP2pConfig::consensus_output_topic(config.chain_id()));
    GossipMessage { source: None, data, sequence_number: None, topic }
}

/// A quorum (`1/3 + 1`) of distinct validators signing the same consensus result must cause
/// the handler to publish it, and not before. This also pins the entry-creation path: the
/// very first signature must be recorded (a regression here would mean a quorum is never
/// reached and the result is never published).
#[tokio::test]
async fn test_consensus_result_publishes_on_quorum() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    let (epoch, round, number) = (0u32, 1u32, 1u64);
    let hash = ConsensusHeaderDigest::from(B256::random());
    let quorum = committee.committee().size() / 3 + 1;
    let authorities: Vec<_> = committee.authorities().collect();
    assert!(authorities.len() >= quorum, "need at least a quorum of authorities");

    // Feed distinct signers one at a time; nothing should publish until the quorum-th.
    for (seen, auth) in authorities.iter().take(quorum).enumerate() {
        let msg = signed_consensus_gossip(auth, epoch, round, number, hash);
        handler.process_gossip(&msg).await?;

        if seen + 1 < quorum {
            assert_eq!(
                consensus_bus.published_consensus_num_hash(),
                (0, 0, ConsensusHeaderDigest::default()),
                "must not publish before a quorum of distinct signers ({} of {quorum})",
                seen + 1,
            );
        }
    }

    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, number, hash),
        "result must be published once a quorum of distinct signers is reached",
    );
    Ok(())
}

/// The same validator gossiping a result repeatedly must be counted once. Replaying one
/// signer more times than the quorum must not publish; only adding the remaining *distinct*
/// signers may.
#[tokio::test]
async fn test_consensus_result_duplicate_signature_counted_once() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    let (epoch, round, number) = (0u32, 1u32, 1u64);
    let hash = ConsensusHeaderDigest::from(B256::random());
    let quorum = committee.committee().size() / 3 + 1;
    let authorities: Vec<_> = committee.authorities().collect();
    assert!(authorities.len() >= quorum, "need at least a quorum of authorities");

    // Replay the first signer's result more than `quorum` times. If duplicates were counted,
    // this alone would reach quorum and publish — it must not.
    let dup = signed_consensus_gossip(authorities[0], epoch, round, number, hash);
    for _ in 0..quorum + 1 {
        handler.process_gossip(&dup).await?;
    }
    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (0, 0, ConsensusHeaderDigest::default()),
        "repeated signatures from one validator must count once and stay below quorum",
    );

    // Add the remaining distinct signers (signer 0 already counted once) to reach quorum.
    for auth in authorities.iter().take(quorum).skip(1) {
        let msg = signed_consensus_gossip(auth, epoch, round, number, hash);
        handler.process_gossip(&msg).await?;
    }
    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, number, hash),
        "a quorum of distinct signers must publish even after duplicates were ignored",
    );
    Ok(())
}

/// A single committee member equivocating on one consensus number (many distinct hashes for the
/// same number) must not grow `consensus_certs` without bound. The per-(signer, number)
/// equivocation limit caps a lone flooder at `MAX_TALLIES_PER_SIGNER_PER_NUMBER` live tallies for
/// that number no matter how many hashes it signs. The limit must not break legitimate
/// aggregation: the SAME validator's genuine signature for a *different* number is not throttled,
/// so a real quorum still publishes afterward.
#[tokio::test]
async fn test_consensus_certs_same_number_flood_bounded() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types(temp_dir.path()).await;

    let (epoch, round) = (0u32, 1u32);
    let quorum = committee.committee().size() / 3 + 1;
    let authorities: Vec<_> = committee.authorities().collect();
    assert!(authorities.len() >= quorum, "need at least a quorum of authorities");

    // Flood: 50 distinct hashes for the SAME number (1) from a single validator. Each hash is a
    // distinct digest, none reaches quorum, so nothing clears the map.
    for _ in 0..50 {
        let hash = ConsensusHeaderDigest::from(B256::random());
        let msg = signed_consensus_gossip(authorities[0], epoch, round, 1, hash);
        handler.process_gossip(&msg).await?;
    }

    // The per-(signer, number) limit holds the flooder to at most MAX_TALLIES_PER_SIGNER_PER_NUMBER
    // live tallies for number 1, far below the committee-scaled memory cap.
    assert!(
        handler.consensus_certs_len() <= MAX_TALLIES_PER_SIGNER_PER_NUMBER,
        "one signer must not create more than MAX_TALLIES_PER_SIGNER_PER_NUMBER tallies for a \
         single number, got {}",
        handler.consensus_certs_len(),
    );

    // A legitimate result for a DIFFERENT number must still reach quorum and publish — even from a
    // quorum that includes the flooder, because the limit is per (signer, number), not per signer.
    let hash_l = ConsensusHeaderDigest::from(B256::random());
    for auth in authorities.iter().take(quorum) {
        let msg = signed_consensus_gossip(auth, epoch, round, 2, hash_l);
        handler.process_gossip(&msg).await?;
    }
    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, 2, hash_l),
        "a legitimate quorum must publish despite the same-number flood",
    );
    // Publishing clears the map.
    assert_eq!(handler.consensus_certs_len(), 0, "map must be cleared after a publish");

    Ok(())
}

/// Two (or more) colluding committee members can co-sign an unbounded stream of distinct hashes
/// for the same consensus number. Each tuple then carries two valid signatures, so a heuristic
/// that only evicts *singleton* entries would keep every one of them (the residual of
/// GHSA-2r5c-c4h7-gp5h). The per-(signer, number) equivocation limit bounds the map regardless of
/// how many members collude: each colluder can be a signer of only
/// `MAX_TALLIES_PER_SIGNER_PER_NUMBER` distinct live tallies for a number, so `f` colluders occupy
/// at most `f * MAX_TALLIES_PER_SIGNER_PER_NUMBER` tallies. A genuine quorum must still publish
/// afterward.
#[tokio::test]
async fn test_consensus_certs_bounded_under_collusion() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    // A committee of seven has a quorum of three, so two co-signers stay strictly below quorum:
    // their entries never publish and therefore never clear the map during the flood.
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types_with_committee_size(temp_dir.path(), NonZeroUsize::new(7).unwrap()).await;

    let (epoch, round) = (0u32, 1u32);
    let quorum = committee.committee().size() / 3 + 1;
    assert!(quorum > 2, "collusion test needs quorum > 2 so two co-signers stay sub-quorum");
    let authorities: Vec<_> = committee.authorities().collect();

    // Flood: 50 distinct hashes for number 1, each co-signed by the SAME two validators. Every
    // resulting entry has two signers, so a "keep any entry with more than one signer" guard would
    // retain all 50. None reaches the quorum of three, so no publish clears the map mid-flood.
    for _ in 0..50 {
        let hash = ConsensusHeaderDigest::from(B256::random());
        for auth in authorities.iter().take(2) {
            let msg = signed_consensus_gossip(auth, epoch, round, 1, hash);
            handler.process_gossip(&msg).await?;
        }
    }

    // Two colluders can be signers of at most 2 * MAX_TALLIES_PER_SIGNER_PER_NUMBER live tallies
    // for number 1 (fewer when they co-sign the same digests, as here).
    assert!(
        handler.consensus_certs_len() <= 2 * MAX_TALLIES_PER_SIGNER_PER_NUMBER,
        "colluders must not create more than 2 * MAX_TALLIES_PER_SIGNER_PER_NUMBER tallies for one \
         number, got {}",
        handler.consensus_certs_len(),
    );

    // A legitimate quorum for a different number must still publish after the collusion flood.
    let hash_l = ConsensusHeaderDigest::from(B256::random());
    for auth in authorities.iter().take(quorum) {
        let msg = signed_consensus_gossip(auth, epoch, round, 2, hash_l);
        handler.process_gossip(&msg).await?;
    }
    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, 2, hash_l),
        "a legitimate quorum must publish even after a collusion flood",
    );
    assert_eq!(handler.consensus_certs_len(), 0, "map must be cleared after a publish");

    Ok(())
}

/// A same-number equivocation flood must not evict an honest tally that is still climbing to
/// quorum. A Byzantine member signs an unbounded stream of distinct hashes for number 1; before
/// each honest signature for the real result (number 2) a burst larger than the cap is injected.
/// Under LRU eviction alone every burst would fill the map and evict the honest tally, and because
/// honest validators gossip each result only once the lost signatures never return — a permanent
/// stall (GHSA-2r5c-c4h7-gp5h / GHSA-pvhw-9pmg-q2hg). The per-(signer, number) limit caps the
/// flooder at a couple of slots so the map never fills and the honest tally survives to publish.
#[tokio::test]
async fn test_consensus_certs_publisher_flood_cannot_evict_honest_tally() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types_with_committee_size(temp_dir.path(), NonZeroUsize::new(7).unwrap()).await;

    let (epoch, round) = (0u32, 1u32);
    let quorum = committee.committee().size() / 3 + 1;
    let authorities: Vec<_> = committee.authorities().collect();
    assert!(authorities.len() >= quorum + 1, "need a flooder plus a distinct honest quorum");

    let hash_real = ConsensusHeaderDigest::from(B256::random());
    for auth in authorities.iter().skip(1).take(quorum) {
        for _ in 0..(MAX_CONSENSUS_CERTS + 5) {
            let flood = ConsensusHeaderDigest::from(B256::random());
            handler
                .process_gossip(&signed_consensus_gossip(authorities[0], epoch, round, 1, flood))
                .await?;
        }
        handler.process_gossip(&signed_consensus_gossip(auth, epoch, round, 2, hash_real)).await?;
        assert!(
            handler.consensus_certs_len() <= MAX_TALLIES_PER_SIGNER_PER_NUMBER + 1,
            "map must stay bounded during the flood, got {}",
            handler.consensus_certs_len(),
        );
    }

    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, 2, hash_real),
        "the honest tally must survive the same-number flood and publish at quorum",
    );
    assert_eq!(handler.consensus_certs_len(), 0, "map must be cleared after a publish");

    Ok(())
}

/// An honest validator that is the first to gossip several consecutive still-un-quorumed consensus
/// numbers on a lagging receiver must NOT have its own signatures dropped. Each result is for a
/// different number, so the per-(signer, number) equivocation limit never fires. A per-signer
/// limit that counted tallies across all numbers would instead drop the validator's own genuine
/// signature for the third number — an honest-liveness regression. This is the guard for that
/// (GHSA-pvhw-9pmg-q2hg): under such a limit `consensus_certs_has(&d3)` is false and number 3
/// never publishes.
#[tokio::test]
async fn test_consensus_certs_honest_creator_across_numbers_not_dropped() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types_with_committee_size(temp_dir.path(), NonZeroUsize::new(7).unwrap()).await;
    let (epoch, round) = (0u32, 1u32);
    let quorum = committee.committee().size() / 3 + 1;
    let authorities: Vec<_> = committee.authorities().collect();
    let h = authorities[0];

    let h1 = ConsensusHeaderDigest::from(B256::random());
    let h2 = ConsensusHeaderDigest::from(B256::random());
    let h3 = ConsensusHeaderDigest::from(B256::random());
    let d1 = ConsensusResult::digest_data(epoch, round, 1, h1);
    let d2 = ConsensusResult::digest_data(epoch, round, 2, h2);
    let d3 = ConsensusResult::digest_data(epoch, round, 3, h3);

    // H is the first to gossip numbers 1, 2 and 3. None has reached quorum on this receiver yet,
    // so all three tallies stay live and each contains H. The per-(signer, number) limit does not
    // fire because each tally is for a different number.
    handler.process_gossip(&signed_consensus_gossip(h, epoch, round, 1, h1)).await?;
    handler.process_gossip(&signed_consensus_gossip(h, epoch, round, 2, h2)).await?;
    handler.process_gossip(&signed_consensus_gossip(h, epoch, round, 3, h3)).await?;
    assert!(handler.consensus_certs_has(&d1), "H's tally for number 1 must be live");
    assert!(handler.consensus_certs_has(&d2), "H's tally for number 2 must be live");
    assert!(handler.consensus_certs_has(&d3), "H's OWN tally for number 3 must NOT be dropped");

    // Number 3 reaches quorum using H's retained signature plus other honest signers.
    for auth in authorities.iter().skip(1).take(quorum - 1) {
        handler.process_gossip(&signed_consensus_gossip(auth, epoch, round, 3, h3)).await?;
    }
    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, 3, h3),
        "number 3 must reach quorum including H's retained signature",
    );
    Ok(())
}

/// At a committee large enough that `2 * f` exceeds the `MAX_CONSENSUS_CERTS` floor, a FIXED cap
/// would let Byzantine members fill the map and evict the honest tally (the large-committee gap;
/// Telcoin targets ~100 validators). The committee-scaled cap keeps the effective cap above the
/// Byzantine footprint (`f * MAX_TALLIES_PER_SIGNER_PER_NUMBER`), so the honest result still
/// reaches quorum and publishes. Committee 34 -> f = 11, quorum = 12, 2f = 22 > 20.
#[tokio::test]
async fn test_consensus_certs_large_committee_flood_survives() -> eyre::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let TestTypes { committee, handler, consensus_bus, task_manager: _task_manager, .. } =
        create_test_types_with_committee_size(temp_dir.path(), NonZeroUsize::new(34).unwrap())
            .await;

    let (epoch, round) = (0u32, 1u32);
    let quorum = committee.committee().size() / 3 + 1;
    let f = (committee.committee().size() - 1) / 3;
    assert!(
        2 * f >= MAX_CONSENSUS_CERTS,
        "committee must be large enough to break a fixed cap; 2f={}",
        2 * f
    );
    let authorities: Vec<_> = committee.authorities().collect();
    assert!(authorities.len() >= f + quorum, "need a Byzantine set plus a disjoint honest quorum");

    // Byzantine set = authorities[0..f]; honest quorum = authorities[f..f+quorum]. Before each
    // honest signature for the real result (number 2), every Byzantine key signs a batch of fresh
    // distinct hashes for number 1. The per-(signer, number) limit caps the whole Byzantine set at
    // f * MAX_TALLIES_PER_SIGNER_PER_NUMBER = 2f tallies for number 1, and the committee-scaled cap
    // (= committee size) stays above that, so the map never fills and the honest number-2 tally is
    // never evicted. Under a fixed cap this same flood would evict it and number 2 would stall.
    let hash_real = ConsensusHeaderDigest::from(B256::random());
    for h in f..(f + quorum) {
        for b in 0..f {
            for _ in 0..MAX_TALLIES_PER_SIGNER_PER_NUMBER {
                let flood = ConsensusHeaderDigest::from(B256::random());
                handler
                    .process_gossip(&signed_consensus_gossip(
                        authorities[b],
                        epoch,
                        round,
                        1,
                        flood,
                    ))
                    .await?;
            }
        }
        handler
            .process_gossip(&signed_consensus_gossip(authorities[h], epoch, round, 2, hash_real))
            .await?;
        assert!(
            handler.consensus_certs_len() <= 2 * f + 1,
            "map must stay near the Byzantine footprint (2f) plus the honest tally, got {}",
            handler.consensus_certs_len(),
        );
    }

    assert_eq!(
        consensus_bus.published_consensus_num_hash(),
        (epoch, 2, hash_real),
        "the honest result must survive the large-committee flood and publish",
    );
    assert_eq!(handler.consensus_certs_len(), 0, "map must be cleared after a publish");

    Ok(())
}
