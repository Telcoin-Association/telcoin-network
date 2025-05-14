//! This mod tests retrieving peers from the admin server.
//!
//! Admin server isn't publicly exposed and likely to be removed in the future.
//! This test is preserved for the time being as an example of testing anemo RPC server code.
//!
//! NOTE: this test is never run and is #[ignore]d for clarity.

use super::*;
use async_trait::async_trait;
use prometheus::Registry;
use std::time::Duration;
use tempfile::TempDir;
use tn_batch_validator::NoopBatchValidator;
use tn_primary::test_utils::CommitteeFixture;
use tn_primary::{
    consensus::{LeaderSchedule, LeaderSwapTable},
    Primary,
};
use tn_storage::mem_db::MemDatabase;
use tn_types::SealedBatch;

// A test validator that rejects every batch
#[derive(Clone)]
#[allow(dead_code)]
struct NilBatchValidator;

#[async_trait]
impl BlockValidation for NilBatchValidator {
    type Error = eyre::Report;

    async fn validate_batch(&self, _txs: SealedBatch) -> Result<(), Self::Error> {
        eyre::bail!("Invalid batch");
    }
}

#[ignore]
#[tokio::test]
async fn get_network_peers_from_admin_server() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let authority_1 = fixture.authorities().next().unwrap();
    let config_1 = authority_1.consensus_config();

    let worker_id = 0;
    let worker_1_keypair = authority_1.worker().keypair().copy();

    // Make the data store.
    // In case the DB dir does not yet exist.
    let temp_dir = TempDir::new().unwrap();
    let _ = std::fs::create_dir_all(temp_dir.path());

    let cb_1 = tn_primary::ConsensusBus::new();
    let mut primary_1 = Primary::new(config_1.clone(), &cb_1);
    // Spawn Primary 1
    primary_1.spawn(
        config_1.clone(),
        &cb_1,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_1 = Registry::new();
    let metrics_1 = Metrics::new_with_registry(&registry_1);

    let worker_1_parameters = config_1.config().parameters.clone();

    // Spawn a `Worker` instance for primary 1.
    let _worker = Worker::spawn(worker_id, NoopBatchValidator, metrics_1.clone(), config_1.clone());

    let primary_1_peer_id =
        Hex::encode(authority_1.primary_network_keypair().copy().public().0.as_bytes());
    let worker_1_peer_id = Hex::encode(worker_1_keypair.copy().public().0.as_bytes());

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test getting all known peers for worker 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        worker_1_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (1 primary + 3 other workers)
    assert_eq!(4, resp.len());

    // Test getting all connected peers for worker 1 (worker at index 0 for primary 1)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_1_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 1 peer (only worker's primary spawned)
    assert_eq!(1, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    let authority_2 = fixture.authorities().nth(1).unwrap();
    let config_2 = authority_2.consensus_config();

    let worker_2_keypair = authority_2.worker().keypair().copy();

    let cb_2 = tn_primary::ConsensusBus::new();
    let mut primary_2 = Primary::new(config_2.clone(), &cb_2);
    // Spawn Primary 2
    primary_2.spawn(
        config_2.clone(),
        &cb_2,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_2 = Registry::new();
    let metrics_2 = Metrics::new_with_registry(&registry_2);

    let worker_2_parameters = config_2.config().parameters.clone();

    // Spawn a `Worker` instance for primary 2.
    let _worker = Worker::spawn(worker_id, NoopBatchValidator, metrics_2.clone(), config_2.clone());

    // Wait for tasks to start. Sleeping longer here to ensure all primaries and workers
    // have  a chance to connect to each other.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let primary_2_peer_id =
        Hex::encode(authority_2.primary_network_keypair().copy().public().0.as_bytes());
    let worker_2_peer_id = Hex::encode(worker_2_keypair.copy().public().0.as_bytes());

    // Test getting all known peers for worker 2 (worker at index 0 for primary 2)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        worker_2_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 4 peers (1 primary + 3 other workers)
    assert_eq!(4, resp.len());

    // Test getting all connected peers for worker 1 (worker at index 0 for primary 1)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_1_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (2 primaries spawned + 1 other worker spawned)
    assert_eq!(3, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id, &primary_2_peer_id, &worker_2_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    // Test getting all connected peers for worker 2 (worker at index 0 for primary 2)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_2_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (2 primaries spawned  + 1 other worker spawned)
    assert_eq!(3, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id, &primary_2_peer_id, &worker_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    // Assert network connectivity metrics are also set as expected
    let filters = vec![
        (primary_2_peer_id.as_str(), "our_primary"),
        (primary_1_peer_id.as_str(), "other_primary"),
        (worker_1_peer_id.as_str(), "other_worker"),
    ];

    for f in filters {
        let mut m = HashMap::new();
        m.insert("peer_id", f.0);
        m.insert("type", f.1);

        assert_eq!(
            1,
            metrics_2
                .clone()
                .network_connection_metrics
                .network_peer_connected
                .get_metric_with(&m)
                .unwrap()
                .get()
        );
    }
}
