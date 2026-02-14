//! Unit tests for the worker's quorum waiter.

use assert_matches::assert_matches;
use std::{num::NonZeroUsize, time::Duration};
use tn_network_libp2p::types::{NetworkCommand, NetworkHandle};
use tn_reth::test_utils::batch;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{test_chain_spec_arc, TaskManager};
use tn_worker::{
    quorum_waiter::{QuorumWaiter, QuorumWaiterError, QuorumWaiterTrait as _},
    WorkerNetworkHandle, WorkerRPCError, WorkerRequest, WorkerResponse,
};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_wait_for_quorum_happy_path() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let max_rpc_msg_size =
        my_primary.consensus_config().network_config().libp2p_config().max_rpc_message_size;
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(
        NetworkHandle::new(sender),
        task_manager.get_spawner(),
        max_rpc_msg_size,
    );
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let attest_handle = quorum_waiter.verify_batch(
        sealed_batch.clone(),
        Duration::from_secs(10),
        &task_manager.get_spawner(),
    );

    let threshold = committee.quorum_threshold();
    for _i in 0..threshold {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply.send(Ok(WorkerResponse::ReportBatch)).unwrap();
            }
            Some(_) => panic!("unexpected network command!"),
            None => panic!("failed to get a batch!"),
        }
    }
    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    assert!(attest_handle.await.unwrap().is_ok());
}

#[tokio::test]
async fn test_batch_rejected_timeout() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let max_rpc_msg_size =
        my_primary.consensus_config().network_config().libp2p_config().max_rpc_message_size;
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(
        NetworkHandle::new(sender),
        task_manager.get_spawner(),
        max_rpc_msg_size,
    );
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let timeout = Duration::from_millis(150);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // send one vote for batch
    match network_rx.recv().await {
        Some(NetworkCommand::SendRequest {
            peer: _,
            request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
            reply,
        }) => {
            assert_eq!(in_batch, sealed_batch);
            reply.send(Ok(WorkerResponse::ReportBatch)).unwrap();
        }
        Some(_) => panic!("unexpected network command!"),
        None => panic!("failed to get a batch!"),
    }

    // sleep for timeout
    tokio::time::sleep(timeout).await;

    // expect timeout error
    assert_matches!(attest_handle.await.unwrap(), Err(QuorumWaiterError::Timeout));
}

#[tokio::test]
async fn test_batch_some_rejected_stake_still_passes() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let max_rpc_msg_size =
        my_primary.consensus_config().network_config().libp2p_config().max_rpc_message_size;
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(
        NetworkHandle::new(sender),
        task_manager.get_spawner(),
        max_rpc_msg_size,
    );
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());
    let threshold = committee.quorum_threshold();

    // send one rejection for batch
    match network_rx.recv().await {
        Some(NetworkCommand::SendRequest {
            peer: _,
            request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
            reply,
        }) => {
            assert_eq!(in_batch, sealed_batch);
            reply
                .send(Ok(WorkerResponse::Error(WorkerRPCError("REJECTED!!!".to_string()))))
                .unwrap();
        }
        Some(_) => panic!("unexpected network command!"),
        None => panic!("failed to get a batch!"),
    }

    // account for first msg (rejection)
    for _i in 0..(threshold - 1) {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply.send(Ok(WorkerResponse::ReportBatch)).unwrap();
            }
            Some(_) => panic!("unexpected network command!"),
            None => panic!("failed to get a batch!"),
        }
    }

    // expect timeout error
    assert_matches!(attest_handle.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn test_batch_rejected_quorum() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let max_rpc_msg_size =
        my_primary.consensus_config().network_config().libp2p_config().max_rpc_message_size;
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(
        NetworkHandle::new(sender),
        task_manager.get_spawner(),
        max_rpc_msg_size,
    );
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // 1/2 of committee rejects
    let threshold = committee.size() / 2;
    for _i in 0..threshold {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply
                    .send(Ok(WorkerResponse::Error(WorkerRPCError("REJECTED!!!".to_string()))))
                    .unwrap();
            }
            Some(_) => panic!("unexpected network command!"),
            None => panic!("failed to get a batch!"),
        }
    }

    // expect timeout error
    assert_matches!(attest_handle.await.unwrap(), Err(QuorumWaiterError::QuorumRejected));
}

// test code
// - threshold
// - timeout
// - num_messages to send

#[tokio::test]
async fn test_batch_rejected_antiquorum() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let max_rpc_msg_size =
        my_primary.consensus_config().network_config().libp2p_config().max_rpc_message_size;
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(
        NetworkHandle::new(sender),
        task_manager.get_spawner(),
        max_rpc_msg_size,
    );
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // 1/2 of committee byzantine
    let threshold = committee.size() / 2;
    for _i in 0..threshold {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                drop(reply);
            }
            Some(_) => panic!("unexpected network command!"),
            None => panic!("failed to get a batch!"),
        }
    }

    // expect timeout error
    assert_matches!(attest_handle.await.unwrap(), Err(QuorumWaiterError::AntiQuorum));
}

/// Make sure that we exit early with anti-quorum when we get network errors.
/// I.e. make sure we track available stake correctly in the QW (this was a bug at one time).
#[tokio::test]
async fn test_batch_early_anti_quorum() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let max_rpc_msg_size =
        my_primary.consensus_config().network_config().libp2p_config().max_rpc_message_size;
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network = WorkerNetworkHandle::new(
        NetworkHandle::new(sender),
        task_manager.get_spawner(),
        max_rpc_msg_size,
    );
    // Spawn a `QuorumWaiter` instance.
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // send three accepts, three rejects and drop the rest (network error) for batch
    // This will create an anti-quorum, make sure we produce the error without waiting the 10 second
    // timeout.
    for i in 0..8 {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                match i {
                    0 | 1 | 2 => reply.send(Ok(WorkerResponse::ReportBatch)).unwrap(),
                    3 | 4 | 5 => reply
                        .send(Ok(WorkerResponse::Error(WorkerRPCError("REJECTED!!!".to_string()))))
                        .unwrap(),
                    _ => drop(reply), // These will produce network errors in the QW.
                }
            }
            Some(_) => panic!("unexpected network command!"),
            None => panic!("failed to get a batch!"),
        }
    }

    // expect to NOT timeout, note this timeout must be much lower than the verify_batch timeout for
    // this test
    match tokio::time::timeout(Duration::from_secs(2), attest_handle).await {
        Err(_) => panic!("should not timeout, should reach anti-quorum early"),
        Ok(Ok(r)) => assert_matches!(r, Err(QuorumWaiterError::AntiQuorum)),
        Ok(Err(_)) => panic!("unexpected recv error!"),
    }
}
