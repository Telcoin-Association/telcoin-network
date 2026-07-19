//! Test network handler tests.

use assert_matches::assert_matches;
use std::{collections::BTreeSet, sync::Arc};
use tn_batch_validator::NoopBatchValidator;
use tn_network_libp2p::{
    types::{NetworkCommand, NetworkHandle},
    GossipMessage, Penalty, TopicHash,
};
use tn_storage::{mem_db::MemDatabase, tables::NodeBatchesCache};
use tn_test_utils::CommitteeFixture;
use tn_types::{
    Batch, BatchValidation, BatchValidationError, BcsError, BlsPublicKey, Database, SealedBatch,
    TaskManager, B256,
};
use tn_worker::{
    RequestHandler, WorkerGossip, WorkerNetworkError, WorkerNetworkHandle, WorkerRequest,
    WorkerResponse, MAX_CONCURRENT_BATCH_STREAMS,
};
use tokio::sync::mpsc;

/// The type for holding testing components.
struct TestTypes<DB = MemDatabase> {
    /// Committee committee with authorities that vote.
    committee: CommitteeFixture<DB>,
    /// The handler for requests.
    handler: RequestHandler<DB>,
    /// Task manager the synchronizer (in RequestHandler) is spawned on.
    /// Save it so that task is not dropped early if needed.
    task_manager: TaskManager,
    /// Receiver for network commands.
    network_commands_rx: mpsc::Receiver<NetworkCommand<WorkerRequest, WorkerResponse>>,
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types() -> TestTypes {
    create_test_types_with_chain_id(0)
}

/// Like [`create_test_types`], but stamps `chain_id` onto the network config so the
/// handler validates (and the handle publishes) gossip topics namespaced by that id.
fn create_test_types_with_chain_id(chain_id: u64) -> TestTypes {
    let mut network_config = tn_config::NetworkConfig::default();
    network_config.set_chain_id(chain_id);
    let committee = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .with_network_config(network_config)
        .build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    let task_manager = TaskManager::default();
    let worker_id = 0;
    let batch_validator = Arc::new(NoopBatchValidator);
    let (tx, network_commands_rx) = mpsc::channel(10);
    let network_handle =
        WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0, chain_id);
    let handler = RequestHandler::new(worker_id, batch_validator, config, network_handle);
    TestTypes { committee, handler, task_manager, network_commands_rx }
}

/// Like [`create_test_types`], but injects a custom batch validator and also returns a handle to
/// the shared node store, so a test can assert exactly what the handler chose to cache.
fn create_test_types_with_validator(
    validator: Arc<dyn BatchValidation>,
) -> (TestTypes, MemDatabase) {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    // clone shares the same in-memory backing, so reads see the handler's writes
    let store = config.node_storage().clone();
    let task_manager = TaskManager::default();
    let worker_id = 0;
    let (tx, network_commands_rx) = mpsc::channel(10);
    let network_handle =
        WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0, 0);
    let handler = RequestHandler::new(worker_id, validator, config, network_handle);
    (TestTypes { committee, handler, task_manager, network_commands_rx }, store)
}

/// A [`BatchValidation`] that rejects every batch. Used to prove the gossip-prefetch path
/// validates a fetched body before caching it.
#[derive(Debug)]
struct RejectingBatchValidator;

impl BatchValidation for RejectingBatchValidator {
    fn validate_batch(&self, _batch: SealedBatch) -> Result<(), BatchValidationError> {
        Err(BatchValidationError::EmptyBatch)
    }

    fn submit_txn_if_mine(&self, _tx_bytes: &[u8], _committee_size: u64, _committee_slot: u64) {}
}

// ============================================================================
// Report Batch Tests
// ============================================================================

#[tokio::test]
async fn test_report_batch_success() {
    let TestTypes { committee, handler, task_manager: _, .. } = create_test_types();
    let batch_digest = B256::random();
    let sealed_batch = SealedBatch::new(Default::default(), batch_digest);
    // batch proposed by committee member
    let good_peer = committee.last_authority().primary_public_key();
    let res = handler.pub_process_report_batch(&good_peer, sealed_batch).await;
    assert_matches!(res, Ok(()));
}

#[tokio::test]
async fn test_report_batch_fails_non_committee_peer() {
    let TestTypes { handler, task_manager: _, .. } = create_test_types();
    let batch_digest = B256::random();
    let sealed_batch = SealedBatch::new(Default::default(), batch_digest);
    // invalid public key - cannot be within committee
    let bad_peer = BlsPublicKey::default();
    let res = handler.pub_process_report_batch(&bad_peer, sealed_batch).await;
    assert_matches!(res, Err(WorkerNetworkError::NonCommitteeBatch));
}

// ============================================================================
// Gossip Tests
// ============================================================================

/// Test that worker pub/sub is enforcing topics.
#[tokio::test]
async fn test_batch_gossip_topics() {
    let TestTypes { network_commands_rx: _, handler, task_manager: _, committee: _ } =
        create_test_types();
    let batch_digest = B256::random();
    let gossip = WorkerGossip::Batch(0, batch_digest);
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(0));
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&good_msg).await.is_ok());

    // A batch gossiped on any other topic must be rejected as an invalid topic.
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::epoch_vote_topic(0));
    let bad_msg = GossipMessage { source: None, data, sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&bad_msg).await.is_err());
}

/// The handler validates gossip topics against the chain id from its config, not a
/// hardcoded value: with a non-zero chain id, a message on the chain-0 namespace (what
/// an un-stamped node would publish) is rejected as an invalid topic. This is the
/// property that makes the namespacing actually isolate chains.
#[tokio::test]
async fn test_batch_gossip_topic_is_chain_namespaced() {
    let TestTypes { handler, .. } = create_test_types_with_chain_id(2017);
    let batch_digest = B256::random();
    let data = tn_types::encode(&WorkerGossip::Batch(0, batch_digest));

    // the config's chain id (2017) is accepted: the topic check passes
    let good = GossipMessage {
        source: None,
        data: data.clone(),
        sequence_number: None,
        topic: TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(2017)),
    };
    assert!(handler.pub_process_gossip_for_test(&good).await.is_ok());

    // the chain-0 namespace is a different topic and is rejected
    let bad = GossipMessage {
        source: None,
        data,
        sequence_number: None,
        topic: TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(0)),
    };
    assert_matches!(
        handler.pub_process_gossip_for_test(&bad).await,
        Err(WorkerNetworkError::InvalidTopic)
    );
}

/// A prefetch already in flight for a digest deduplicates a repeat gossip of the
/// same digest: the second `process_gossip` returns without emitting any further
/// `request_batches` network command. This is the wiring that bounds a Byzantine
/// author's gossip-driven fetch amplification.
#[tokio::test]
async fn test_batch_gossip_prefetch_deduplicates_in_flight_digest() {
    let TestTypes { mut network_commands_rx, handler, task_manager, committee: _ } =
        create_test_types();
    let batch_digest = B256::random();
    let data = tn_types::encode(&WorkerGossip::Batch(0, batch_digest));
    let make_msg = || GossipMessage {
        source: None,
        data: data.clone(),
        sequence_number: None,
        topic: TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(0)),
    };

    // Task A: the first prefetch. It reaches `request_batches` (emitting a network
    // command) and then parks awaiting a reply, holding the in-flight dedup slot for
    // this digest. A clone shares the same prefetch dedup set + concurrency semaphore.
    let handler_a = handler.clone();
    let msg_a = make_msg();
    task_manager.spawn_task("prefetch-a", async move {
        let _ = handler_a.pub_process_gossip_for_test(&msg_a).await;
        Ok(())
    });

    // Wait until A has entered `request_batches` (its first command). From here A is
    // parked with the dedup slot held; we deliberately never reply.
    let _first = network_commands_rx.recv().await.expect("first prefetch emits a command");

    // Task B: a duplicate gossip for the same digest. It must be deduplicated and
    // return without emitting any network command of its own.
    let msg_b = make_msg();
    handler.pub_process_gossip_for_test(&msg_b).await.expect("duplicate gossip returns Ok");

    // The duplicate emitted nothing: no second fetch fan-out for an in-flight digest.
    assert_matches!(
        network_commands_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "duplicate gossip must not trigger a second batch fetch"
    );
}

/// A batch fetched by the gossip prefetch is validated before it is cached: a body that fails
/// validation is dropped, never written to `NodeBatchesCache`.
///
/// This is the fix for issue #933. The vote-path sync (`PrimaryReceiverHandler::synchronize`)
/// treats a digest already present in `NodeBatchesCache` as validated-and-available and skips
/// `validate_batch` for it, so an unvalidated prefetched body reaching that cache would let a
/// Byzantine committee member get a semantically-invalid batch voted on and certified.
#[tokio::test]
async fn test_gossip_prefetch_rejects_invalid_batch() {
    let (TestTypes { handler, .. }, store) =
        create_test_types_with_validator(Arc::new(RejectingBatchValidator));
    // current-epoch batch (fixture epoch is 0), so only validation can reject it
    let batch = Batch::default();
    let digest = batch.digest();

    // dropping an invalid body is not an error (it is a peer content fault, not a local one)
    let res = handler.pub_validate_and_cache_prefetched_batch(&digest, &batch);
    assert_matches!(res, Ok(()));

    // the security property: an unvalidated body never reaches the cache the vote path trusts
    assert!(
        store.get::<NodeBatchesCache>(&digest).expect("read store").is_none(),
        "an invalid gossip-prefetched batch must not be cached"
    );
}

/// The counterpart to [`test_gossip_prefetch_rejects_invalid_batch`]: an identical current-epoch
/// batch that passes validation *is* cached, so presence in `NodeBatchesCache` faithfully means
/// "validated" for the vote path. This isolates validation (not the epoch check) as the reason the
/// rejected batch above was dropped.
#[tokio::test]
async fn test_gossip_prefetch_caches_valid_batch() {
    let (TestTypes { handler, .. }, store) =
        create_test_types_with_validator(Arc::new(NoopBatchValidator));
    let batch = Batch::default();
    let digest = batch.digest();

    let res = handler.pub_validate_and_cache_prefetched_batch(&digest, &batch);
    assert_matches!(res, Ok(()));

    assert!(
        store.get::<NodeBatchesCache>(&digest).expect("read store").is_some(),
        "a validated gossip-prefetched batch must be cached"
    );
}

// ============================================================================
// Stream Request Error Tests
// ============================================================================

/// Test that UnknownStreamRequest error is properly returned.
///
/// This tests the error type that should be returned when a stream arrives
/// without a matching pending request.
#[tokio::test]
async fn test_unknown_stream_request_error_type() {
    // Verify the error type exists and has the expected structure
    let fake_digest = B256::random();
    let error = WorkerNetworkError::UnknownStreamRequest(fake_digest);

    // Check error message indicates no pending request
    let error_string = error.to_string();
    assert!(
        error_string.contains("pending request") || error_string.contains("stream"),
        "Error should mention pending request or stream: {}",
        error_string
    );
}

// ============================================================================
// Negotiation Flow Tests
// ============================================================================

/// Test that request_batches returns empty when no peers are connected.
#[tokio::test]
async fn test_request_batches_no_peers() {
    let (tx, mut rx) = mpsc::channel(10);
    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0, 0);

    // reply with empty peer list
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            if let NetworkCommand::ConnectedPeers { reply } = cmd {
                reply.send(vec![]).expect("send peers");
            }
        }
    });

    let mut digests = BTreeSet::from([B256::random()]);
    let result = handle.pub_request_batches(&mut digests).await;
    // no peers → returns Ok(empty)
    assert!(result.unwrap().is_empty());
}

// ============================================================================
// Error Penalty Mapping Tests
// ============================================================================

#[tokio::test]
async fn test_stream_error_penalties() {
    // Fatal penalties
    let cases_fatal = vec![
        WorkerNetworkError::InvalidTopic,
        WorkerNetworkError::Bcs(BcsError::Eof),
        WorkerNetworkError::TooManyBatches { expected: 1, received: 5 },
        WorkerNetworkError::UnexpectedBatch(B256::random()),
        WorkerNetworkError::DuplicateBatch(B256::random()),
        WorkerNetworkError::RequestHashMismatch,
    ];
    for error in cases_fatal {
        let penalty: Option<Penalty> = error.into();
        assert!(matches!(penalty, Some(Penalty::Fatal)), "expected Fatal penalty");
    }

    // No penalty (None)
    let timeout_err = WorkerNetworkError::Timeout(
        tokio::time::timeout(std::time::Duration::ZERO, async {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        })
        .await
        .unwrap_err(),
    );
    let penalty: Option<Penalty> = timeout_err.into();
    assert!(penalty.is_none(), "Timeout should have no penalty");

    let stream_closed = WorkerNetworkError::StreamClosed;
    let penalty: Option<Penalty> = stream_closed.into();
    assert!(penalty.is_none(), "StreamClosed should have no penalty");
}

/// Author-content faults (issue #801) must be classified so the gossip handler does not
/// Fatal-ban the relaying peer for a fault that belongs to the message author.
#[test]
fn test_is_author_content_fault() {
    // Faults determined by message content. These are exactly the Fatal
    // protocol-violation variants; `Bcs` and `InvalidTopic` are the two reachable from
    // the gossip handler, which previously Fatal-banned the innocent relayer.
    let author_content = vec![
        WorkerNetworkError::Bcs(BcsError::Eof),
        WorkerNetworkError::InvalidTopic,
        WorkerNetworkError::TooManyBatches { expected: 1, received: 5 },
        WorkerNetworkError::UnexpectedBatch(B256::random()),
        WorkerNetworkError::DuplicateBatch(B256::random()),
        WorkerNetworkError::RequestHashMismatch,
    ];
    for err in author_content {
        assert!(
            err.is_author_content_fault(),
            "{err:?} is determined by message content and must not penalize a relayer"
        );
        // every author-content fault is a Fatal protocol violation: this is exactly the
        // ban the gossip handler now suppresses for the relaying peer.
        let penalty: Option<Penalty> = err.into();
        assert_matches!(penalty, Some(Penalty::Fatal));
    }

    // Faults attributable to the transport peer's behavior, or local to this node. The
    // relayer is correctly penalized for these (when a penalty applies at all).
    let not_author_content = vec![
        WorkerNetworkError::StreamClosed,
        WorkerNetworkError::NonCommitteeBatch,
        WorkerNetworkError::Internal("boom".to_string()),
        WorkerNetworkError::InvalidRequest("bad request".to_string()),
        WorkerNetworkError::UnknownStreamRequest(B256::random()),
        WorkerNetworkError::StdIo(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        // content-determined and Fatal for some subvariants, but only reachable on the
        // request/response path (peer == originator), never via gossip (see #801).
        WorkerNetworkError::BatchValidation(tn_types::BatchValidationError::EmptyBatch),
        WorkerNetworkError::BatchEpochMismatch(1, 2),
    ];
    for err in not_author_content {
        assert!(!err.is_author_content_fault(), "{err:?} is not a message-content fault");
    }
}

// ============================================================================
// Semaphore & Concurrency Tests
// ============================================================================

/// Test that acquiring all MAX_CONCURRENT_BATCH_STREAMS permits causes try_acquire_owned to fail,
/// which is how the sync responder sheds load (a denied admission writes `Deny(AtCapacity)`).
#[tokio::test]
async fn test_semaphore_exhaustion_denies_admission() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS));

    // acquire all permits
    let permits: Vec<_> = (0..MAX_CONCURRENT_BATCH_STREAMS)
        .map(|_| semaphore.clone().try_acquire_owned().expect("permit should be available"))
        .collect();
    assert_eq!(permits.len(), MAX_CONCURRENT_BATCH_STREAMS);

    // next acquire should fail (the responder denies admission)
    assert!(
        semaphore.clone().try_acquire_owned().is_err(),
        "semaphore should be exhausted after {MAX_CONCURRENT_BATCH_STREAMS} acquires"
    );
}
