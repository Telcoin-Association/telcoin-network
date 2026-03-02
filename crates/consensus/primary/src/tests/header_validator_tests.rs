//! Header validator tests.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{state_sync::HeaderValidator, ConsensusBus};
use assert_matches::assert_matches;
use tn_network_types::{FetchBatchResponse, PrimaryToWorkerClient, WorkerSynchronizeMessage};
use tn_reth::test_utils::fixture_batch_with_transactions;
use tn_storage::{mem_db::MemDatabase, CertificateStore, PayloadStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{error::HeaderError, BlockHash, Hash as _};

#[derive(Debug)]
struct TrackingPrimaryToWorkerClient {
    synchronize_calls: Arc<AtomicUsize>,
}

impl TrackingPrimaryToWorkerClient {
    fn new(synchronize_calls: Arc<AtomicUsize>) -> Self {
        Self { synchronize_calls }
    }
}

#[async_trait::async_trait]
impl PrimaryToWorkerClient for TrackingPrimaryToWorkerClient {
    async fn synchronize(&self, _message: WorkerSynchronizeMessage) -> eyre::Result<()> {
        self.synchronize_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn fetch_batches(
        &self,
        _digests: HashSet<BlockHash>,
    ) -> eyre::Result<FetchBatchResponse> {
        Ok(FetchBatchResponse { batches: HashMap::new() })
    }
}

#[tokio::test]
async fn test_sync_batches_drops_old_rounds() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();
    let cb = ConsensusBus::new();
    let header_validator = HeaderValidator::new(primary.consensus_config(), cb.clone());

    // create 4 certificates
    // write to certificate and payload stores
    let certs: HashMap<_, _> = fixture
        .authorities()
        .map(|a| {
            let header = a
                .header_builder(&committee)
                .with_payload_batch(fixture_batch_with_transactions(10), 0)
                .build();
            let cert = fixture.certificate(&header);
            let digest = cert.digest();
            certificate_store.write(cert.clone()).expect("write cert to storage");
            // write to payload store
            for (digest, worker_id) in cert.header().payload() {
                payload_store.write_payload(digest, worker_id).unwrap();
            }
            (digest, cert)
        })
        .collect();

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certs.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // update round
    let committed_round = 30;
    cb.committed_round_updates().send_replace(committed_round);

    let expected_digest = test_header.digest();
    let expected_round = test_header.round();
    let max_age = 10;
    let expected_max_round = committed_round - max_age;
    let err = header_validator.sync_header_batches(&test_header, false, max_age).await;
    assert_matches!(
        err, Err(HeaderError::TooOld{ digest, header_round, max_round })
        if digest == expected_digest
        && header_round == expected_round
        && max_round == expected_max_round
    );
    Ok(())
}

#[tokio::test]
async fn test_sync_batches_uses_worker_specific_local_network() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .number_of_workers(NonZeroUsize::new(2).expect("non-zero worker count"))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let config = primary.consensus_config();
    let cb = ConsensusBus::new();
    let header_validator = HeaderValidator::new(config.clone(), cb.clone());

    let worker0_calls = Arc::new(AtomicUsize::new(0));
    let worker1_calls = Arc::new(AtomicUsize::new(0));
    config.local_network(0).set_primary_to_worker_local_handler(Arc::new(
        TrackingPrimaryToWorkerClient::new(worker0_calls.clone()),
    ));
    config.local_network(1).set_primary_to_worker_local_handler(Arc::new(
        TrackingPrimaryToWorkerClient::new(worker1_calls.clone()),
    ));

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .with_payload_batch(fixture_batch_with_transactions(5), 1)
        .build();
    cb.committed_round_updates().send_replace(2);
    header_validator.sync_header_batches(&test_header, false, 100).await?;

    assert_eq!(worker0_calls.load(Ordering::Relaxed), 0);
    assert_eq!(worker1_calls.load(Ordering::Relaxed), 1);
    Ok(())
}
