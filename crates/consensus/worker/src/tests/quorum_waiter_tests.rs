//! Unit tests for the worker's quorum waiter.

use super::*;
use tn_network::test_utils::WorkerToWorkerMockServer;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{batch, test_network, CommitteeFixture};

#[tokio::test]
async fn wait_for_quorum() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let my_primary = fixture.authorities().next().unwrap();
    let myself = fixture.authorities().next().unwrap().worker();

    let node_metrics = Arc::new(WorkerMetrics::default());

    // setup network
    let network = test_network(myself.keypair(), &myself.info().worker_address);
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter = QuorumWaiter::new(
        my_primary.authority().clone(),
        /* worker_id */ 0,
        committee.clone(),
        worker_cache.clone(),
        network.clone(),
        node_metrics,
    );

    // Make a batch.
    let sealed_batch = batch().seal_slow();
    let message = WorkerBlockMessage { sealed_worker_block: sealed_batch.clone() };

    // Spawn enough listeners to acknowledge our batches.
    let mut listener_handles = Vec::new();
    for worker in fixture.authorities().skip(1).map(|a| a.worker()) {
        let handle =
            WorkerToWorkerMockServer::spawn(worker.keypair(), worker.info().worker_address.clone());
        listener_handles.push(handle);

        // ensure that the networks are connected
        network.connect(worker.info().worker_address.to_anemo_address().unwrap()).await.unwrap();
    }

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let attest_handle = quorum_waiter.verify_batch(sealed_batch.clone(), Duration::from_secs(10));

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    assert!(attest_handle.await.unwrap().is_ok());

    // Send a second batch.
    let sealed_batch2 = batch().seal_slow();
    let message2 = WorkerBlockMessage { sealed_worker_block: sealed_batch2.clone() };

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let attest2_handle = quorum_waiter.verify_batch(sealed_batch2.clone(), Duration::from_secs(10));

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    //assert!(attest2_handle.await.unwrap().is_ok());
    attest2_handle.await.unwrap().unwrap();

    // Ensure the other listeners correctly received the batches.
    for (mut handle, _network) in listener_handles {
        assert_eq!(handle.recv().await.unwrap(), message);
        assert_eq!(handle.recv().await.unwrap(), message2);
    }
}
