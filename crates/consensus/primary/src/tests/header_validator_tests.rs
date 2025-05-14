//! Header validator tests.

use std::collections::HashMap;

use crate::{consensus::ConsensusRound, state_sync::HeaderValidator, ConsensusBus};
use assert_matches::assert_matches;
use tn_reth::test_utils::fixture_batch_with_transactions;
use tn_storage::{mem_db::MemDatabase, CertificateStore, PayloadStore};
use tn_test_utils::CommitteeFixture;
use tn_types::{error::HeaderError, Hash as _};

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
                .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
                .build();
            let cert = fixture.certificate(&header);
            let digest = cert.digest();
            certificate_store.write(cert.clone()).expect("write cert to storage");
            // write to payload store
            for (digest, (worker_id, _)) in cert.header().payload() {
                payload_store.write_payload(digest, worker_id).unwrap();
            }
            (digest, cert)
        })
        .collect();

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certs.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build();

    // update round
    let committed_round = 30;
    cb.update_consensus_rounds(ConsensusRound::new(committed_round, 0))
        .expect("update consensus rounds");

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
