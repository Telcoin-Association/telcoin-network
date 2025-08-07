//! Subscriber IT tests

use std::sync::Arc;
use tn_executor::subscriber::spawn_subscriber;
use tn_network_libp2p::types::{MessageId, NetworkCommand};
use tn_network_types::MockPrimaryToWorkerClient;
use tn_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus,
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{create_signed_certificates_for_rounds, CommitteeFixture};
use tn_types::{
    ExecHeader, SealedHeader, TaskManager, TnReceiver as _, TnSender as _, B256,
    DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_output_to_header() -> eyre::Result<()> {
    let num_sub_dags_per_schedule = 3;
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let task_manager = TaskManager::new("subscriber tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();

    // subscribe to channels early
    let rx_consensus_headers = consensus_bus.last_consensus_header().subscribe();
    let mut consensus_output = consensus_bus.consensus_output().subscribe();

    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // spawn the executor
    spawn_subscriber(config.clone(), rx_shutdown, consensus_bus.clone(), &task_manager, network);

    // yield for subscriber to spawn
    tokio::task::yield_now().await;

    // make certificates for rounds 1 to 7 (inclusive)
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=7, &fixture);

    // Set up mock worker.
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        // metrics.clone(),
        Arc::new(Default::default()),
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &consensus_bus, bullshark, &task_manager);

    // forward certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    let expected_num = 3;
    let mut consensus_headers_seen: Vec<_> = Vec::with_capacity(expected_num);
    while let Some(output) = consensus_output.recv().await {
        // assert epoch boundary not reached
        assert!(!output.close_epoch);

        let num = output.number;
        let consensus_header = output.consensus_header();
        consensus_headers_seen.push(consensus_header);
        if num == expected_num as u64 {
            break;
        }

        // yield for other tasks
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let last_header = rx_consensus_headers.borrow().clone();
    assert!(last_header.number == expected_num as u64);

    // NOTE: output.consensus_header() creates the consensus header and should be the same
    // result
    assert_eq!(
        last_header.digest(),
        consensus_headers_seen.last().expect("last consensus header").digest()
    );

    Ok(())
}
