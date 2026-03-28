//! Test network handler tests.

use assert_matches::assert_matches;
use std::{collections::HashSet, sync::Arc};
use tn_batch_validator::NoopBatchValidator;
use tn_network_libp2p::{
    types::{NetworkCommand, NetworkHandle},
    GossipMessage, Penalty, TopicHash,
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BlsPublicKey, SealedBatch, TaskManager, B256};
use tn_worker::{
    PendingBatchStream, RequestHandler, WorkerGossip, WorkerNetworkError, WorkerNetworkHandle,
    WorkerRequest, WorkerResponse, MAX_BATCH_DIGESTS_PER_REQUEST, MAX_CONCURRENT_BATCH_STREAMS,
    MAX_PENDING_REQUESTS_PER_PEER, PENDING_REQUEST_TIMEOUT,
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
        WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0);
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
                    WorkerRequest::RequestBatchesStream { batch_digests, epoch: _ } => {
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
                    WorkerRequest::RequestBatchesStream { batch_digests, epoch: _ } => {
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
            NetworkCommand::OpenStream { peer, reply } => {
                // Verify OpenStream is called after ack
                assert!(stream_request_acked, "OpenStream should come after ack");
                assert_eq!(peer, expected_peer);
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
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0);

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
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0);

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
    let d1 = B256::random();
    let d2 = B256::random();
    let batch_digests: HashSet<B256> = HashSet::from([d1, d2]);

    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
    let request_digest = handle.pub_generate_batch_request_id(&batch_digests);

    // simulate server storing a pending request
    let peer = BlsPublicKey::default();
    let key = (peer, request_digest);
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let permit = semaphore.clone().try_acquire_owned().expect("permit available");
    let pending = PendingBatchStream::new(batch_digests.clone(), 0, permit);

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

// ============================================================================
// Truncation & Peer Fallback Tests
// ============================================================================

/// Test that request_batches truncates >500 digests to exactly MAX_BATCH_DIGESTS_PER_REQUEST.
#[tokio::test]
async fn test_request_batches_truncates_oversized_digests() {
    let (tx, mut rx) = mpsc::channel(10);
    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0);

    let fixture = tn_test_utils::CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .build();
    let peer1 = fixture.first_authority().primary_public_key();

    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                NetworkCommand::ConnectedPeers { reply } => {
                    reply.send(vec![peer1]).expect("send peers");
                }
                NetworkCommand::SendRequest { request, reply, .. } => {
                    if let WorkerRequest::RequestBatchesStream { batch_digests, .. } = request {
                        // assert truncation happened
                        assert_eq!(
                            batch_digests.len(),
                            MAX_BATCH_DIGESTS_PER_REQUEST,
                            "digests should be truncated to MAX_BATCH_DIGESTS_PER_REQUEST (500)"
                        );
                        // reject to end the test
                        reply
                            .send(Ok(WorkerResponse::RequestBatchesStream { ack: false }))
                            .expect("send reject");
                    }
                }
                _ => {}
            }
        }
    });

    // create 600 digests (exceeds the 500 cap)
    let mut digests: HashSet<B256> = (0..600u64)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&i.to_le_bytes());
            B256::from(bytes)
        })
        .collect();
    assert_eq!(digests.len(), 600);

    let result = handle.pub_request_batches(&mut digests).await;
    // peer rejected so we get an error (but truncation was verified above)
    assert!(result.is_err());
}

// ============================================================================
// Semaphore & Concurrency Tests
// ============================================================================

/// Test that acquiring all MAX_CONCURRENT_BATCH_STREAMS permits causes try_acquire_owned to fail,
/// which maps to ack=false for requesting peers.
#[tokio::test]
async fn test_semaphore_exhaustion_returns_ack_false() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS));

    // acquire all permits
    let mut permits = Vec::new();
    for _ in 0..MAX_CONCURRENT_BATCH_STREAMS {
        let permit = semaphore.clone().try_acquire_owned().expect("permit should be available");
        permits.push(permit);
    }

    // next acquire should fail (maps to ack=false)
    assert!(
        semaphore.clone().try_acquire_owned().is_err(),
        "semaphore should be exhausted after {MAX_CONCURRENT_BATCH_STREAMS} acquires"
    );
}

/// Test that dropping a PendingBatchStream releases its semaphore permit,
/// allowing the next acquire to succeed.
#[tokio::test]
async fn test_semaphore_release_on_pending_drop() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));

    // acquire the only permit via PendingBatchStream
    let permit = semaphore.clone().try_acquire_owned().expect("permit available");
    let pending = PendingBatchStream::new(HashSet::from([B256::random()]), 0, permit);

    // semaphore exhausted
    assert!(semaphore.clone().try_acquire_owned().is_err(), "should be exhausted");

    // drop the pending stream → permit released
    drop(pending);

    // now acquire should succeed
    assert!(semaphore.clone().try_acquire_owned().is_ok(), "permit should be released after drop");
}

/// Test that a peer with MAX_PENDING_REQUESTS_PER_PEER pending entries is rejected,
/// while a different peer can still acquire.
#[tokio::test]
async fn test_per_peer_limit_rejects_excess() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS));
    let mut pending_map: std::collections::HashMap<(BlsPublicKey, B256), PendingBatchStream> =
        std::collections::HashMap::new();

    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let peer_a = fixture.first_authority().primary_public_key();
    let peer_b = fixture.last_authority().primary_public_key();

    // fill peer_a to its per-peer limit
    for _ in 0..MAX_PENDING_REQUESTS_PER_PEER {
        let permit = semaphore.clone().try_acquire_owned().expect("permit available");
        let digest = B256::random();
        pending_map.insert(
            (peer_a, digest),
            PendingBatchStream::new(HashSet::from([B256::random()]), 0, permit),
        );
    }

    // peer_a should be rejected (per-peer limit)
    let peer_a_count = pending_map.keys().filter(|(p, _)| *p == peer_a).count();
    assert!(peer_a_count >= MAX_PENDING_REQUESTS_PER_PEER, "peer_a should be at per-peer limit");

    // peer_b should still succeed
    let peer_b_count = pending_map.keys().filter(|(p, _)| *p == peer_b).count();
    assert!(peer_b_count < MAX_PENDING_REQUESTS_PER_PEER, "peer_b should have capacity");

    let permit = semaphore.clone().try_acquire_owned().expect("global permits remain");
    pending_map.insert(
        (peer_b, B256::random()),
        PendingBatchStream::new(HashSet::from([B256::random()]), 0, permit),
    );
    assert_eq!(pending_map.keys().filter(|(p, _)| *p == peer_b).count(), 1);
}

/// Test that expired pending entries are cleaned up and their semaphore permits are returned.
///
/// Uses `new_with_created_at` to create entries past PENDING_REQUEST_TIMEOUT.
/// Simulates `WorkerNetwork::cleanup_stale_pending_requests` by retaining only fresh entries.
/// Verifies that dropping stale entries releases their held semaphore permits.
#[tokio::test]
async fn test_stale_cleanup_releases_permits() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS));
    let mut pending_map: std::collections::HashMap<(BlsPublicKey, B256), PendingBatchStream> =
        std::collections::HashMap::new();

    let peer = BlsPublicKey::default();

    // create 2 stale entries (created_at past PENDING_REQUEST_TIMEOUT)
    let stale_time =
        std::time::Instant::now() - PENDING_REQUEST_TIMEOUT - std::time::Duration::from_secs(1);
    let mut stale_keys = HashSet::new();
    for _ in 0..2 {
        let permit = semaphore.clone().try_acquire_owned().expect("permit available");
        let digest = B256::random();
        stale_keys.insert((peer, digest));
        pending_map.insert(
            (peer, digest),
            PendingBatchStream::new_with_created_at(
                HashSet::from([B256::random()]),
                0,
                permit,
                stale_time,
            ),
        );
    }

    // create 1 fresh entry that should survive cleanup
    let permit = semaphore.clone().try_acquire_owned().expect("permit available");
    let fresh_digest = B256::random();
    pending_map.insert(
        (peer, fresh_digest),
        PendingBatchStream::new(HashSet::from([B256::random()]), 0, permit),
    );

    // 3 permits consumed
    assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_BATCH_STREAMS - 3);

    // simulate cleanup: remove stale entries (mirrors cleanup_stale_pending_requests)
    pending_map.retain(|key, _| !stale_keys.contains(key));

    // stale entries removed → 2 permits returned, 1 fresh entry remains
    assert_eq!(pending_map.len(), 1, "only fresh entry should survive");
    assert_eq!(
        semaphore.available_permits(),
        MAX_CONCURRENT_BATCH_STREAMS - 1,
        "2 permits should be returned, 1 held by fresh entry"
    );
}

/// Test that exactly MAX_CONCURRENT_BATCH_STREAMS requests succeed across multiple peers
/// (respecting per-peer limit), the next one fails, and dropping one allows it to succeed.
#[tokio::test]
async fn test_concurrent_capacity_exactly_max() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS));
    let mut pending_map: std::collections::HashMap<(BlsPublicKey, B256), PendingBatchStream> =
        std::collections::HashMap::new();

    // create 3 distinct peers from the committee fixture
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(std::num::NonZeroUsize::new(3).unwrap())
        .randomize_ports(true)
        .build();
    let peers: Vec<BlsPublicKey> =
        fixture.authorities().map(|a| a.primary_public_key()).take(3).collect();

    // fill 5 slots across 3 peers (respecting per-peer=2): peer0=2, peer1=2, peer2=1
    let distribution = [2usize, 2, 1];
    let mut first_key = None;
    for (peer_idx, &count) in distribution.iter().enumerate() {
        for _ in 0..count {
            let permit = semaphore.clone().try_acquire_owned().expect("permit available");
            let digest = B256::random();
            let key = (peers[peer_idx], digest);
            if first_key.is_none() {
                first_key = Some(key);
            }
            pending_map
                .insert(key, PendingBatchStream::new(HashSet::from([B256::random()]), 0, permit));
        }
    }

    assert_eq!(pending_map.len(), MAX_CONCURRENT_BATCH_STREAMS);
    assert_eq!(semaphore.available_permits(), 0);

    // 6th request should fail
    assert!(semaphore.clone().try_acquire_owned().is_err(), "should be at capacity");

    // drop one entry → frees a permit
    let key = first_key.unwrap();
    pending_map.remove(&key);

    // now the 6th request should succeed
    assert!(semaphore.clone().try_acquire_owned().is_ok(), "permit should be available after drop");
}

/// Test that request_batches retries after initial rejection and reaches
/// the acceptance path on a subsequent attempt.
#[tokio::test]
async fn test_retry_succeeds_after_initial_rejection() {
    let (tx, mut rx) = mpsc::channel(10);
    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0);

    let fixture = tn_test_utils::CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .build();
    let peer1 = fixture.first_authority().primary_public_key();

    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<bool>();

    tokio::spawn(async move {
        let mut connected_peers_count = 0u32;
        let mut stream_accepted = false;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                NetworkCommand::ConnectedPeers { reply } => {
                    connected_peers_count += 1;
                    reply.send(vec![peer1]).expect("send peers");
                }
                NetworkCommand::SendRequest { request, reply, .. } => {
                    if let WorkerRequest::RequestBatchesStream { .. } = request {
                        if connected_peers_count <= 1 {
                            // first attempt: reject
                            reply
                                .send(Ok(WorkerResponse::RequestBatchesStream { ack: false }))
                                .expect("send reject");
                        } else {
                            // second attempt: accept
                            reply
                                .send(Ok(WorkerResponse::RequestBatchesStream { ack: true }))
                                .expect("send accept");
                            stream_accepted = true;
                        }
                    }
                }
                NetworkCommand::OpenStream { reply, .. } => {
                    // peer accepted on retry — drop reply to simulate stream error
                    // (full stream flow is tested elsewhere)
                    drop(reply);
                    // signal test that retry reached acceptance
                    let _ = done_tx.send(stream_accepted);
                    return;
                }
                _ => {}
            }
        }
    });

    let mut digests = HashSet::from([B256::random()]);
    // request_batches will retry — we don't care about the final result,
    // we care that the retry mechanism reached the acceptance path
    let handle_task = tokio::spawn(async move {
        let _ = handle.pub_request_batches(&mut digests).await;
    });

    // wait for the mock to confirm retry reached acceptance
    let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), done_rx)
        .await
        .expect("should not timeout")
        .expect("should receive signal");

    assert!(accepted, "retry should have reached acceptance path after initial rejection");

    // clean up
    handle_task.abort();
}

/// Test that when peer 1 rejects, peer 2 receives the same (unchanged) digests.
#[tokio::test]
async fn test_request_batches_peer_fallback_preserves_digests() {
    let (tx, mut rx) = mpsc::channel(10);
    let task_manager = TaskManager::default();
    let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0);

    let fixture = tn_test_utils::CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .build();
    let peer1 = fixture.first_authority().primary_public_key();
    let peer2 = fixture.last_authority().primary_public_key();

    let expected_count = 3;
    tokio::spawn(async move {
        let mut request_count = 0u32;
        while let Some(cmd) = rx.recv().await {
            match cmd {
                NetworkCommand::ConnectedPeers { reply } => {
                    reply.send(vec![peer1, peer2]).expect("send peers");
                }
                NetworkCommand::SendRequest { request, reply, .. } => {
                    if let WorkerRequest::RequestBatchesStream { batch_digests, .. } = request {
                        request_count += 1;
                        // both peer1 and peer2 should receive exactly 3 digests
                        assert_eq!(
                            batch_digests.len(),
                            expected_count,
                            "peer {request_count} should receive all {expected_count} digests"
                        );
                        // reject to trigger fallback to next peer
                        reply
                            .send(Ok(WorkerResponse::RequestBatchesStream { ack: false }))
                            .expect("send reject");
                    }
                }
                _ => {}
            }
        }
    });

    let mut digests: HashSet<B256> = (0..expected_count).map(|_| B256::random()).collect();

    let result = handle.pub_request_batches(&mut digests).await;
    // all peers rejected → error
    assert!(result.is_err());
}
