//! Unit tests for worker network handle.

use std::sync::Arc;

use tn_network_libp2p::{types::{NetworkCommand, NetworkEvent, NetworkHandle}, TNMessage};
use tn_test_utils::CommitteeFixture;
use tokio::sync::mpsc;
use tn_storage::mem_db::MemDatabase;
use tn_types::{BlsPublicKey, SealedBatch, TaskManager, TaskSpawner};
use tn_batch_validator::{NoopBatchValidator};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle, WorkerRequest, WorkerResponse};

/// The type for holding testng components.
struct TestTypes {
    /// The peer's id.
    peer_id: BlsPublicKey,
    /// Sender for network events.
    network_events_tx: mpsc::Sender<NetworkEvent<WorkerRequest, WorkerResponse>>,
    /// Receiver for network commands.
    network_commands_rx: mpsc::Receiver<NetworkCommand<WorkerRequest, WorkerResponse>>
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types(task_spawner: TaskSpawner) -> TestTypes {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    let peer_id = authority.primary_public_key();
    let (network_events_tx, network_events_rx) = mpsc::channel(1);
    let (tx, network_commands_rx) = mpsc::channel(1);
    let network_handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_spawner.clone());
    let network = WorkerNetwork::new(network_events_rx, network_handle, config, 0, Arc::new(NoopBatchValidator));
    // spawn network
    network.spawn(&task_spawner);
    TestTypes { peer_id, network_events_tx, network_commands_rx }
}

/// Test process reported batch from non-committee member fails.
#[tokio::test]
async fn test_batch_from_non_committee_peer_fails() -> eyre::Result<()> {
    let mut task_manager = TaskManager::default();
    let committee_worker = create_test_types(task_manager.get_spawner());
    let bad_actor = create_test_types(task_manager.get_spawner());

    let peer = bad_actor.peer_id;
    // can't create a dummy channel
    committee_worker.network_events_tx.send(NetworkEvent::Request { peer, request: WorkerRequest::ReportBatch { sealed_batch }, channel: (), cancel: () });

    // let committee_network =
    // let worker_in_committee =

    Ok(())
}
