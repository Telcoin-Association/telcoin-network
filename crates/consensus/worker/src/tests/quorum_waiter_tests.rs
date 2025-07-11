//! Unit tests for the worker's quorum waiter.

use super::*;
use crate::{WorkerRequest, WorkerResponse};
use tn_network_libp2p::types::{NetworkCommand, NetworkHandle};
use tn_reth::test_utils::batch;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::TaskManager;
use tokio::sync::mpsc;

#[tokio::test]
async fn wait_for_quorum() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let my_primary = fixture.authorities().next().unwrap();
    let node_metrics = Arc::new(WorkerMetrics::default());
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner());
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter = QuorumWaiter::new(
        my_primary.authority().clone(),
        /* worker_id */ 0,
        committee.clone(),
        worker_cache.clone(),
        network,
        node_metrics,
    );

    // Make a batch.
    let sealed_batch = batch().seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let attest_handle = quorum_waiter.verify_batch(
        sealed_batch.clone(),
        Duration::from_secs(10),
        &task_manager.get_spawner(),
    );

    for _i in 0..3 {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply.send(Ok(WorkerResponse::ReportBatch)).unwrap();
            }
            Some(_) => panic!("failed to get a batch!"),
            None => panic!("failed to get a batch!"),
        }
    }
    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    assert!(attest_handle.await.unwrap().is_ok());

    // Send a second batch.
    let sealed_batch2 = batch().seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let attest2_handle = quorum_waiter.verify_batch(
        sealed_batch2.clone(),
        Duration::from_secs(10),
        &task_manager.get_spawner(),
    );

    // Ensure the other listeners correctly received the batches.
    for _i in 0..3 {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch2);
                reply.send(Ok(WorkerResponse::ReportBatch)).unwrap();
            }
            Some(_) => panic!("failed to get a batch!"),
            None => panic!("failed to get a batch!"),
        }
    }

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    attest2_handle.await.unwrap().unwrap();
}
