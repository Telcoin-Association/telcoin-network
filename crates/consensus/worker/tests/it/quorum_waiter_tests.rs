//! Unit tests for the worker's quorum waiter.

use assert_matches::assert_matches;
use std::{collections::HashMap, num::NonZeroUsize, time::Duration};
use tn_network_libp2p::types::{NetworkCommand, NetworkHandle, NetworkResponseMessage};
use tn_reth::test_utils::batch;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{test_chain_spec_arc, BlsPublicKey, TaskManager};
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
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
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
                peer,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply
                    .send(Ok(NetworkResponseMessage { peer, result: WorkerResponse::ReportBatch }))
                    .unwrap();
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
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
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
            peer,
            request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
            reply,
        }) => {
            assert_eq!(in_batch, sealed_batch);
            reply
                .send(Ok(NetworkResponseMessage { peer, result: WorkerResponse::ReportBatch }))
                .unwrap();
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
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
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
            peer,
            request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
            reply,
        }) => {
            assert_eq!(in_batch, sealed_batch);
            reply
                .send(Ok(NetworkResponseMessage {
                    peer,
                    result: WorkerResponse::Error(WorkerRPCError("REJECTED!!!".to_string())),
                }))
                .unwrap();
        }
        Some(_) => panic!("unexpected network command!"),
        None => panic!("failed to get a batch!"),
    }

    // account for first msg (rejection)
    for _i in 0..(threshold - 1) {
        match network_rx.recv().await {
            Some(NetworkCommand::SendRequest {
                peer,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply
                    .send(Ok(NetworkResponseMessage { peer, result: WorkerResponse::ReportBatch }))
                    .unwrap();
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
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
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
                peer,
                request: WorkerRequest::ReportBatch { sealed_batch: in_batch },
                reply,
            }) => {
                assert_eq!(in_batch, sealed_batch);
                reply
                    .send(Ok(NetworkResponseMessage {
                        peer,
                        result: WorkerResponse::Error(WorkerRPCError("REJECTED!!!".to_string())),
                    }))
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
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
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

    // Background handler: drop all replies to simulate network errors.
    // With retries, peers will send multiple requests - drop them all.
    tokio::spawn(async move {
        while let Some(cmd) = network_rx.recv().await {
            match cmd {
                NetworkCommand::SendRequest { reply, .. } => {
                    drop(reply);
                }
                _ => panic!("unexpected network command!"),
            }
        }
    });

    // All peers exhaust retries → all return WaiterError::Network → AntiQuorum
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
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
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

    // Background handler that tracks per-peer behavior:
    // - First 3 unique peers: accept
    // - Next 3 unique peers: reject (RPCError)
    // - Rest: always drop (network error, retries also dropped)
    // This creates an anti-quorum once the dropped peers exhaust retries.
    tokio::spawn(async move {
        let mut peer_behavior: HashMap<BlsPublicKey, u8> = HashMap::new();
        let mut next_index = 0u8;
        while let Some(cmd) = network_rx.recv().await {
            match cmd {
                NetworkCommand::SendRequest { peer, reply, .. } => {
                    let behavior = *peer_behavior.entry(peer).or_insert_with(|| {
                        let idx = next_index;
                        next_index += 1;
                        idx
                    });
                    match behavior {
                        0..=2 => {
                            let _ = reply.send(Ok(NetworkResponseMessage {
                                peer,
                                result: WorkerResponse::ReportBatch,
                            }));
                        }
                        3..=5 => {
                            let _ = reply.send(Ok(NetworkResponseMessage {
                                peer,
                                result: WorkerResponse::Error(WorkerRPCError(
                                    "REJECTED!!!".to_string(),
                                )),
                            }));
                        }
                        _ => drop(reply),
                    }
                }
                _ => panic!("unexpected network command!"),
            }
        }
    });

    // expect to NOT timeout - anti-quorum should be detected early.
    // Retries add ~600ms max, well within the 3-second window.
    match tokio::time::timeout(Duration::from_secs(3), attest_handle).await {
        Err(_) => panic!("should not timeout, should reach anti-quorum early"),
        Ok(Ok(r)) => assert_matches!(r, Err(QuorumWaiterError::AntiQuorum)),
        Ok(Err(_)) => panic!("unexpected recv error!"),
    }
}

/// Verify that retries allow quorum to be reached after initial network failures.
#[tokio::test]
async fn test_network_error_retry_then_quorum() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // Background handler: all peers fail first attempt, succeed on retry
    tokio::spawn(async move {
        let mut attempt_count: HashMap<BlsPublicKey, u32> = HashMap::new();
        while let Some(cmd) = network_rx.recv().await {
            match cmd {
                NetworkCommand::SendRequest { peer, reply, .. } => {
                    let count = attempt_count.entry(peer).or_insert(0);
                    *count += 1;
                    if *count == 1 {
                        drop(reply);
                    } else {
                        let _ = reply.send(Ok(NetworkResponseMessage {
                            peer,
                            result: WorkerResponse::ReportBatch,
                        }));
                    }
                }
                _ => panic!("unexpected network command!"),
            }
        }
    });

    // All peers succeed on retry → quorum reached
    assert!(attest_handle.await.unwrap().is_ok());
}

/// Verify that partial retry success can achieve quorum.
#[tokio::test]
async fn test_network_error_partial_retry_success() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // Background handler:
    // - First 3 unique peers: succeed immediately
    // - Next 3 unique peers: fail first, succeed on retry
    // - Rest: always fail (drop)
    tokio::spawn(async move {
        let mut peer_order: HashMap<BlsPublicKey, u8> = HashMap::new();
        let mut next_index = 0u8;
        let mut attempt_count: HashMap<BlsPublicKey, u32> = HashMap::new();
        while let Some(cmd) = network_rx.recv().await {
            match cmd {
                NetworkCommand::SendRequest { peer, reply, .. } => {
                    let order = *peer_order.entry(peer).or_insert_with(|| {
                        let idx = next_index;
                        next_index += 1;
                        idx
                    });
                    let count = attempt_count.entry(peer).or_insert(0);
                    *count += 1;
                    match order {
                        0..=2 => {
                            let _ = reply.send(Ok(NetworkResponseMessage {
                                peer,
                                result: WorkerResponse::ReportBatch,
                            }));
                        }
                        3..=5 => {
                            if *count == 1 {
                                drop(reply);
                            } else {
                                let _ = reply.send(Ok(NetworkResponseMessage {
                                    peer,
                                    result: WorkerResponse::ReportBatch,
                                }));
                            }
                        }
                        _ => drop(reply),
                    }
                }
                _ => panic!("unexpected network command!"),
            }
        }
    });

    // 3 immediate + 3 on retry + self = 7 stake >= threshold
    assert!(attest_handle.await.unwrap().is_ok());
}

/// Verify that bounded retries produce AntiQuorum, not Timeout.
#[tokio::test]
async fn test_network_error_all_retries_exhausted() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();
    let my_primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();

    // setup network
    let (sender, mut network_rx) = mpsc::channel(100);
    let network =
        WorkerNetworkHandle::new(NetworkHandle::new(sender), task_manager.get_spawner(), 0);
    let quorum_waiter =
        QuorumWaiter::new(my_primary.authority().clone(), committee.clone(), network);

    // Make a batch.
    let chain = test_chain_spec_arc();
    let sealed_batch = batch(chain).seal_slow();

    let timeout = Duration::from_secs(10);
    let attest_handle =
        quorum_waiter.verify_batch(sealed_batch.clone(), timeout, &task_manager.get_spawner());

    // Background handler: always drop replies
    tokio::spawn(async move {
        while let Some(cmd) = network_rx.recv().await {
            match cmd {
                NetworkCommand::SendRequest { reply, .. } => drop(reply),
                _ => panic!("unexpected network command!"),
            }
        }
    });

    // All retries exhausted → AntiQuorum (not Timeout), well within 5 seconds
    match tokio::time::timeout(Duration::from_secs(5), attest_handle).await {
        Err(_) => panic!("should not timeout waiting for quorum waiter"),
        Ok(Ok(r)) => assert_matches!(r, Err(QuorumWaiterError::AntiQuorum)),
        Ok(Err(_)) => panic!("unexpected recv error!"),
    }
}
