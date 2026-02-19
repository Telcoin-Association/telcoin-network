//! Test network handler tests.

use assert_matches::assert_matches;
use std::sync::Arc;
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

/// The type for holding testng components.
struct TestTypes<DB = MemDatabase> {
    /// Committee committee with authorities that vote.
    committee: CommitteeFixture<DB>,
    // /// The authority that receives messages.
    // authority: &'a AuthorityFixture<DB>,
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
    let network_handle = WorkerNetworkHandle::new(
        worker_id,
        NetworkHandle::new(tx),
        task_manager.get_spawner(),
        config.network_config().libp2p_config().max_rpc_message_size,
    );
    let handler = RequestHandler::new(worker_id, batch_validator, config, network_handle);
    TestTypes { committee, handler, task_manager, network_commands_rx }
}

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
async fn test_request_batches_fails_response_too_small() {
    let TestTypes { handler, task_manager: _, .. } = create_test_types();
    let batch_digest = B256::random();
    let batch_digests = vec![batch_digest];
    let too_small = 1;
    let res = handler.pub_process_request_batches(batch_digests, too_small).await;
    assert_matches!(res, Err(WorkerNetworkError::InvalidRequest(_)));
}

#[tokio::test]
async fn test_request_batches_capped_at_response_max_requestor() {
    let TestTypes { committee, handler, task_manager: _, .. } = create_test_types();
    let batch_digest_1 = B256::random();
    let batch_digest_2 = B256::random();
    let batch_digests = vec![batch_digest_1, batch_digest_2];
    let batch = Batch { transactions: vec![vec![1_u8; 10]], ..Default::default() };
    let expected_batch = SealedBatch::new(batch, batch_digest_1);
    // create large batch that exceeds limit
    let max_response_size = 1_000;
    let big_tx = vec![1u8; max_response_size];
    let too_big = SealedBatch::new(
        Batch { transactions: vec![big_tx], ..Default::default() },
        batch_digest_2,
    );

    // store both batches to db
    for batch in [&expected_batch, &too_big] {
        committee
            .first_authority()
            .consensus_config()
            .node_storage()
            .insert::<Batches>(&batch.digest, &batch.batch)
            .expect("write batch to db");
    }

    let res = handler.pub_process_request_batches(batch_digests, max_response_size).await;
    // only batch_1 returned per requestor's max response size
    assert_matches!(res, Ok(batch) if batch == vec![expected_batch.batch]);
}

#[tokio::test]
async fn test_request_batches_capped_at_response_max_internal() {
    let TestTypes { committee, handler, task_manager: _, .. } = create_test_types();
    let batch_digest_1 = B256::random();
    let batch_digest_2 = B256::random();
    let batch_digests = vec![batch_digest_1, batch_digest_2];
    let batch_1 = Batch { transactions: vec![vec![1_u8; 10_000]], ..Default::default() };
    let expected_batch = SealedBatch::new(batch_1, batch_digest_1);
    // create large batch that exceeds limit
    let internal_max = committee
        .first_authority()
        .consensus_config()
        .network_config()
        .libp2p_config()
        .max_rpc_message_size;

    let big_tx = vec![1u8; internal_max];
    let too_big = SealedBatch::new(
        Batch { transactions: vec![big_tx], ..Default::default() },
        batch_digest_2,
    );

    // store both batches to db
    for batch in [&expected_batch, &too_big] {
        committee
            .first_authority()
            .consensus_config()
            .node_storage()
            .insert::<Batches>(&batch.digest, &batch.batch)
            .expect("write batch to db");
    }

    // ensure requestor's max size is larger
    let max_response_size = internal_max * 2;
    let res = handler.pub_process_request_batches(batch_digests, max_response_size).await;
    // only batch_1 returned per requestor's max response size
    assert_matches!(res, Ok(batch) if batch == vec![expected_batch.batch]);
}

/// Test that worker pub/sub is enforcing topics.
#[tokio::test]
async fn test_batch_gossip_topics() {
    let TestTypes { network_commands_rx: _, handler, task_manager: _, committee: _ } =
        create_test_types();
    let batch_digest = B256::random();
    let gossip = WorkerGossip::Batch(batch_digest);
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(0));
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&good_msg).await.is_ok());

    // Test swapped topics, must fail.
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_txn_topic(0));
    let bad_msg = GossipMessage { source: None, data, sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&bad_msg).await.is_err());
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(0));
    let gossip = WorkerGossip::Txn(vec![]);
    let data = tn_types::encode(&gossip);
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&bad_msg).await.is_err());

    // Use the correct topic for a txn and make sure it works.
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_txn_topic(0));
    let good_msg = GossipMessage { source: None, data, sequence_number: None, topic };
    assert!(handler.pub_process_gossip_for_test(&good_msg).await.is_ok());
}

#[tokio::test]
async fn test_batch_gossip_succeeds() {
    let TestTypes { mut network_commands_rx, handler, task_manager, committee } =
        create_test_types();
    let batch_digest = B256::random();
    let gossip = WorkerGossip::Batch(batch_digest);
    let data = tn_types::encode(&gossip);
    let topic = TopicHash::from_raw(tn_config::LibP2pConfig::worker_batch_topic(0));
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
            NetworkCommand::SendRequest { peer, request, .. } => {
                // assert expected output
                let max_response_size = committee
                    .first_authority()
                    .consensus_config()
                    .network_config()
                    .libp2p_config()
                    .max_rpc_message_size;
                let expected_request = WorkerRequest::RequestBatches {
                    batch_digests: vec![batch_digest],
                    max_response_size,
                };
                assert_eq!(peer, expected_peer);
                assert_eq!(request, expected_request);
            }
            _ => panic!("unexpected network command"),
        }
    }
}
