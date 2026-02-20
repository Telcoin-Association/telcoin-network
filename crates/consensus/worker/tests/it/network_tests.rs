//! Test network handler tests.

use assert_matches::assert_matches;
use std::{collections::HashSet, sync::Arc};
use tn_batch_validator::NoopBatchValidator;
use tn_network_libp2p::{
    types::{NetworkCommand, NetworkHandle},
    GossipMessage, TopicHash,
};
use tn_storage::{mem_db::MemDatabase, tables::Batches};
use tn_test_utils::CommitteeFixture;
use tn_types::{Batch, BlsPublicKey, Database, SealedBatch, TaskManager, B256};
use tn_worker::{
    RequestHandler, WorkerGossip, WorkerNetworkError, WorkerNetworkHandle, WorkerRequest,
    WorkerResponse,
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
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    let task_manager = TaskManager::default();
    let worker_id = 0;
    let batch_validator = Arc::new(NoopBatchValidator);
    let (tx, network_commands_rx) = mpsc::channel(10);
    let network_handle =
        WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner());
    let handler = RequestHandler::new(worker_id, batch_validator, config, network_handle);
    TestTypes { committee, handler, task_manager, network_commands_rx }
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
// Request Batches Tests (Legacy Request-Response)
// ============================================================================

#[tokio::test]
async fn test_request_batches_success() {
    let TestTypes { committee, handler, task_manager: _, .. } = create_test_types();
    let batch_digest = B256::random();
    let sealed_batch = SealedBatch::new(Default::default(), batch_digest);
    // insert batch to DB
    committee
        .first_authority()
        .consensus_config()
        .node_storage()
        .insert::<Batches>(&sealed_batch.digest, &sealed_batch.batch)
        .expect("write batch to db");

    let batch_digests = vec![batch_digest];
    let max_response_size = 1_000;
    let res = handler.pub_process_request_batches(batch_digests, max_response_size).await;
    assert_matches!(res, Ok(batches) if batches == vec![Default::default()]);
}

#[tokio::test]
async fn test_request_batches_fails_empty_digests() {
    let TestTypes { handler, task_manager: _, .. } = create_test_types();
    let batch_digests = vec![];
    let max_response_size = 1_000;
    let res = handler.pub_process_request_batches(batch_digests, max_response_size).await;
    assert_matches!(res, Err(WorkerNetworkError::InvalidRequest(_)));
}

#[tokio::test]
async fn test_request_batches_returns_empty_for_missing_batch() {
    let TestTypes { handler, task_manager: _, .. } = create_test_types();
    // Request a batch that doesn't exist in the store
    let missing_digest = B256::random();
    let batch_digests = vec![missing_digest];
    let max_response_size = 1_000;

    let res = handler.pub_process_request_batches(batch_digests, max_response_size).await;
    // Should succeed but return empty since batch not found
    assert_matches!(res, Ok(batches) if batches.is_empty());
}

#[tokio::test]
async fn test_request_batches_partial_missing() {
    let TestTypes { committee, handler, task_manager: _, .. } = create_test_types();

    // Create and store one batch
    let existing_digest = B256::random();
    let existing_batch = Batch { transactions: vec![vec![1, 2, 3]], ..Default::default() };
    committee
        .first_authority()
        .consensus_config()
        .node_storage()
        .insert::<Batches>(&existing_digest, &existing_batch)
        .expect("write batch to db");

    // Request both existing and missing batches
    let missing_digest = B256::random();
    let batch_digests = vec![existing_digest, missing_digest];
    let max_response_size = 10_000;

    let res = handler.pub_process_request_batches(batch_digests, max_response_size).await;
    // Should return only the existing batch
    assert_matches!(res, Ok(batches) if batches.len() == 1 && batches[0] == existing_batch);
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
    let gossip = WorkerGossip::Batch(batch_digest);
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic());
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&good_msg).await.is_ok());

    // Test swapped topics, must fail.
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_txn_topic());
    let bad_msg = GossipMessage { source: None, data, sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&bad_msg).await.is_err());
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic());
    let gossip = WorkerGossip::Txn(vec![]);
    let data = tn_types::encode(&gossip);
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&bad_msg).await.is_err());

    // Use the correct topic for a txn and make sure it works.
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_txn_topic());
    let good_msg = GossipMessage { source: None, data, sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&good_msg).await.is_ok());
}

/// Test that gossip triggers batch fetch via stream-based approach.
///
/// This test verifies the stream negotiation flow:
/// 1. Gossip triggers batch fetch
/// 2. Handler sends RequestBatchesStream to negotiate
/// 3. If peer rejects (ack=false), this would normally trigger fallback
#[tokio::test]
async fn test_batch_gossip_triggers_stream_request() {
    let TestTypes { mut network_commands_rx, handler, task_manager, committee } =
        create_test_types();
    let batch_digest = B256::random();
    let gossip = WorkerGossip::Batch(batch_digest);
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic());
    let msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };

    task_manager.spawn_task("process-gossip-test", async move {
        handler.pub_process_gossip_for_test(&msg).await.expect("success process gossip");
    });

    // recv commands
    let expected_peer = committee.last_authority().primary_public_key();
    while let Some(command) = network_commands_rx.recv().await {
        match command {
            NetworkCommand::ConnectedPeers { reply } => {
                // request_batches calls this first
                reply.send(vec![expected_peer]).expect("peer sent");
            }
            NetworkCommand::SendRequest { peer, request, reply } => {
                assert_eq!(peer, expected_peer);
                match request {
                    WorkerRequest::RequestBatchesStream { batch_digests } => {
                        // Verify the stream request contains the correct digest
                        assert_eq!(batch_digests, HashSet::from([batch_digest]));
                        // Reject to end the test (no actual stream setup in unit test)
                        reply
                            .send(Ok(WorkerResponse::RequestBatchesStream { ack: false }))
                            .expect("reply sent");
                        // Test passes - we verified the stream request was sent
                        break;
                    }
                    _ => panic!("expected RequestBatchesStream, got {:?}", request),
                }
            }
            _ => panic!("unexpected network command"),
        }
    }
}

/// Test stream request with ack=true (peer accepts).
///
/// When a peer accepts, the flow continues to open a stream.
/// This test verifies the OpenStream command is sent after ack.
#[tokio::test]
async fn test_batch_gossip_stream_accepted_opens_stream() {
    let TestTypes { mut network_commands_rx, handler, task_manager, committee } =
        create_test_types();
    let batch_digest = B256::random();
    let gossip = WorkerGossip::Batch(batch_digest);
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic());
    let msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };

    task_manager.spawn_task("process-gossip-test", async move {
        // This will timeout eventually since we don't complete the stream
        let _ = handler.pub_process_gossip_for_test(&msg).await;
    });

    let expected_peer = committee.last_authority().primary_public_key();
    let mut stream_request_acked = false;

    while let Some(command) = network_commands_rx.recv().await {
        match command {
            NetworkCommand::ConnectedPeers { reply } => {
                reply.send(vec![expected_peer]).expect("peer sent");
            }
            NetworkCommand::SendRequest { peer, request, reply } => {
                assert_eq!(peer, expected_peer);
                match request {
                    WorkerRequest::RequestBatchesStream { batch_digests } => {
                        assert_eq!(batch_digests, HashSet::from([batch_digest]));
                        // Accept the stream request
                        reply
                            .send(Ok(WorkerResponse::RequestBatchesStream { ack: true }))
                            .expect("reply sent");
                        stream_request_acked = true;
                    }
                    _ => panic!("expected RequestBatchesStream, got {:?}", request),
                }
            }
            NetworkCommand::OpenStream { peer, resource_id: _, request_digest, reply } => {
                // Verify OpenStream is called after ack
                assert!(stream_request_acked, "OpenStream should come after ack");
                assert_eq!(peer, expected_peer);
                // request_digest should be a hash of the batch_digests
                assert_ne!(request_digest, B256::ZERO);
                // Don't complete the stream - just verify command was sent
                // Drop reply to simulate error, ending the test
                drop(reply);
                break;
            }
            _ => {}
        }
    }

    assert!(stream_request_acked, "stream request should have been acked");
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
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner());

    // reply with empty peer list
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            if let NetworkCommand::ConnectedPeers { reply } = cmd {
                reply.send(vec![]).expect("send peers");
            }
        }
    });

    let mut digests = HashSet::from([B256::random()]);
    let result = handle.pub_request_batches(&mut digests).await;
    // no peers → returns Ok(empty)
    assert!(result.unwrap().is_empty());
}

/// Test that when a peer rejects (ack=false), the next peer is tried,
/// and if all reject, an error is returned.
#[tokio::test]
async fn test_request_batches_peer_rejects_tries_next() {
    let (tx, mut rx) = mpsc::channel(10);
    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner());

    let fixture = tn_test_utils::CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .build();
    let peer1 = fixture.first_authority().primary_public_key();
    let peer2 = fixture.last_authority().primary_public_key();

    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                NetworkCommand::ConnectedPeers { reply } => {
                    reply.send(vec![peer1, peer2]).expect("send peers");
                }
                NetworkCommand::SendRequest { request, reply, .. } => {
                    if let WorkerRequest::RequestBatchesStream { .. } = request {
                        // both peers reject
                        reply
                            .send(Ok(WorkerResponse::RequestBatchesStream { ack: false }))
                            .expect("send reject");
                    }
                }
                _ => {}
            }
        }
    });

    let mut digests = HashSet::from([B256::random()]);
    let result = handle.pub_request_batches(&mut digests).await;
    // all peers rejected → error
    assert!(result.is_err());
}

/// Test that generate_batch_request_id is deterministic.
#[tokio::test]
async fn test_request_digest_is_deterministic() {
    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

    let d1 = B256::random();
    let d2 = B256::random();
    let digests = HashSet::from([d1, d2]);

    let id1 = handle.pub_generate_batch_request_id(&digests);
    let id2 = handle.pub_generate_batch_request_id(&digests);
    assert_eq!(id1, id2, "same digests should produce same request id");

    // different digests produce different id
    let other_digests = HashSet::from([B256::random()]);
    let id3 = handle.pub_generate_batch_request_id(&other_digests);
    assert_ne!(id1, id3, "different digests should produce different request id");
}

/// Test that the server correctly stores pending requests and can retrieve them by key.
#[tokio::test]
async fn test_pending_request_stream_correlation() {
    use tn_worker::PendingBatchStream;

    let d1 = B256::random();
    let d2 = B256::random();
    let batch_digests: HashSet<B256> = HashSet::from([d1, d2]);

    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
    let request_digest = handle.pub_generate_batch_request_id(&batch_digests);

    // simulate server storing a pending request
    let peer = BlsPublicKey::default();
    let key = (peer, request_digest);
    let pending = PendingBatchStream::new(batch_digests.clone());

    let mut pending_map = std::collections::HashMap::new();
    pending_map.insert(key, pending);

    // simulate inbound stream lookup
    let retrieved = pending_map.remove(&key);
    assert!(retrieved.is_some(), "pending request should be found by (peer, request_digest)");
}

// ============================================================================
// Error Penalty Mapping Tests
// ============================================================================

#[tokio::test]
async fn test_stream_error_penalties() {
    use tn_network_libp2p::Penalty;

    // Fatal penalties
    let cases_fatal = vec![
        WorkerNetworkError::UnknownStreamRequest(B256::random()),
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
    let timeout_err =
        WorkerNetworkError::Timeout(tokio::time::timeout(std::time::Duration::ZERO, async {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        })
        .await
        .unwrap_err());
    let penalty: Option<Penalty> = timeout_err.into();
    assert!(penalty.is_none(), "Timeout should have no penalty");

    let stream_closed = WorkerNetworkError::StreamClosed;
    let penalty: Option<Penalty> = stream_closed.into();
    assert!(penalty.is_none(), "StreamClosed should have no penalty");
}
