//! Certificate fetcher tests

use crate::{
    certificate_fetcher::CertificateFetcher,
    error::CertManagerError,
    network::{PrimaryRequest, PrimaryResponse},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use assert_matches::assert_matches;
use std::{collections::BTreeSet, time::Duration};
use tn_network_libp2p::types::{NetworkCommand, NetworkHandle};
use tn_storage::{mem_db::MemDatabase, CertificateStore, PayloadStore};
use tn_test_utils::CommitteeFixture;
use tn_types::{
    BlsSignature, Certificate, Hash as _, Header, SignatureVerificationState, TaskManager,
};
use tokio::{
    sync::mpsc::{self, error::TryRecvError},
    time::sleep,
};

async fn verify_certificates_in_store<DB: CertificateStore>(
    certificate_store: &DB,
    certificates: &[Certificate],
    expected_verified_directly_count: u64,
    expected_verified_indirectly_count: u64,
) {
    let mut missing = None;
    let mut verified_indirectly = 0;
    let mut verified_directly = 0;
    for _ in 0..20 {
        missing = None;
        verified_directly = 0;
        verified_indirectly = 0;
        for (i, _) in certificates.iter().enumerate() {
            if let Ok(Some(cert)) = certificate_store.read(certificates[i].digest()) {
                match cert.signature_verification_state() {
                    SignatureVerificationState::VerifiedDirectly(_) => verified_directly += 1,
                    SignatureVerificationState::VerifiedIndirectly(_) => verified_indirectly += 1,
                    _ => panic!(
                        "Found unexpected stored signature state {:?}",
                        cert.signature_verification_state()
                    ),
                };
                continue;
            }
            missing = Some(i);
            break;
        }
        if missing.is_none() {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    if let Some(i) = missing {
        panic!(
            "Missing certificate in store: input index {}, certificate: {:?}",
            i, certificates[i]
        );
    }

    assert_eq!(
        verified_directly, expected_verified_directly_count,
        "Verified {verified_directly} certificates directly in the store, expected {expected_verified_directly_count}"
    );
    assert_eq!(
        verified_indirectly, expected_verified_indirectly_count,
        "Verified {verified_indirectly} certificates indirectly in the store, expected {expected_verified_indirectly_count}"
    );
}

fn verify_certificates_not_in_store<DB: CertificateStore>(
    certificate_store: &DB,
    certificates: &[Certificate],
) {
    let found_certificates =
        certificate_store.read_all(certificates.iter().map(|c| c.digest())).unwrap();

    let found_count = found_certificates.iter().filter(|&c| c.is_some()).count();

    assert_eq!(found_count, 0, "Found {found_count} certificates in the store");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fetch_certificates_basic() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();

    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    // Signal rounds

    let cb = ConsensusBus::new();
    // Make a synchronizer for certificates.
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    // Make a certificate fetcher
    CertificateFetcher::spawn(
        primary.consensus_config(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // Generate headers and certificates in successive rounds
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    let mut current_round: Vec<_> =
        genesis_certs.into_iter().map(|cert| cert.header().clone()).collect();
    let mut headers = vec![];
    let rounds = 100;
    for i in 0..rounds {
        let parents: BTreeSet<_> =
            current_round.into_iter().map(|header| fixture.certificate(&header).digest()).collect();
        (_, current_round) = fixture.headers_round(i, &parents);
        headers.extend(current_round.clone());
    }

    // Avoid any sort of missing payload by pre-populating the batch
    for (digest, (worker_id, _)) in headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }

    let total_certificates = fixture.authorities().count() * rounds as usize;
    // Create certificates test data.
    let mut certificates = vec![];
    for header in headers.into_iter() {
        certificates.push(fixture.certificate(&header));
    }
    assert_eq!(certificates.len(), total_certificates); // note genesis is not included
    assert_eq!(400, total_certificates);

    let mut num_written = 4;
    for cert in certificates.iter_mut().take(num_written) {
        // Manually writing the certificates to store so we can consider them verified
        // directly
        cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            cert.aggregated_signature().expect("Invalid Signature"),
        ));
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    // Send a primary message for a certificate with parents that do not exist locally, to trigger
    // fetching.
    let target_index = 123;
    let expected_digest = certificates[target_index].digest();
    let error = synchronizer.process_peer_certificate(certificates[target_index].clone()).await;
    assert_matches!(error, Err(CertManagerError::Pending(digest)) if digest == expected_digest);

    // Verify the fetch request.
    let mut first_batch_len = 0;
    let mut first_batch_resp = vec![];
    if let Some(NetworkCommand::SendRequest {
        peer: _,
        request: PrimaryRequest::MissingCertificates { inner },
        reply,
    }) = fake_receiver.recv().await
    {
        let (lower_bound, skip_rounds) = inner.get_bounds().unwrap();
        assert_eq!(lower_bound, 0);
        assert_eq!(skip_rounds.len(), fixture.authorities().count());
        for rounds in skip_rounds.values() {
            assert_eq!(rounds, &(1..2).collect());
        }

        // Send back another 62 certificates.
        first_batch_len = 62;
        first_batch_resp = certificates
            .iter()
            .skip(num_written)
            .take(first_batch_len)
            .cloned()
            .collect::<Vec<_>>();
        reply.send(Ok(PrimaryResponse::RequestedCertificates(first_batch_resp.clone()))).unwrap();
    }

    // The certificates up to index 66 (4 + 62) should be written to store eventually by core.
    verify_certificates_in_store(
        &certificate_store,
        &certificates[0..(num_written + first_batch_len)],
        6,  // 2 fetched certs verified directly + the initial 4 inserted
        60, // verified indirectly
    )
    .await;
    num_written += first_batch_len;
    // The certificate fetcher should send out another fetch request, because it has not received
    // certificate 123.
    let second_batch_len;
    let second_batch_resp;
    loop {
        match fake_receiver.recv().await {
            Some(NetworkCommand::SendRequest {
                peer: _,
                request: PrimaryRequest::MissingCertificates { inner },
                reply,
            }) => {
                let (_, skip_rounds) = inner.get_bounds().unwrap();
                if skip_rounds.values().next().unwrap().len() == 1 {
                    // Drain the fetch requests sent out before the last reply, when only 1 round in
                    // skip_rounds.
                    reply
                        .send(Ok(PrimaryResponse::RequestedCertificates(first_batch_resp.clone())))
                        .unwrap();
                    continue;
                }
                let (_, skip_rounds) = inner.get_bounds().unwrap();
                assert_eq!(skip_rounds.len(), fixture.authorities().count());
                for (_, rounds) in skip_rounds {
                    let rounds = rounds.into_iter().collect::<Vec<_>>();
                    assert!(
                        rounds == (1..=16).collect::<Vec<_>>()
                            || rounds == (1..=17).collect::<Vec<_>>()
                    );
                }

                // Send back another 123 + 1 - 66 = 58 certificates.
                second_batch_len = target_index + 1 - num_written;
                second_batch_resp = certificates
                    .iter()
                    .skip(num_written)
                    .take(second_batch_len)
                    .cloned()
                    .collect::<Vec<_>>();
                reply
                    .send(Ok(PrimaryResponse::RequestedCertificates(second_batch_resp.clone())))
                    .unwrap();
                break;
            }
            Some(_) => {}
            None => panic!("Unexpected channel closing!"),
        }
    }

    // The certificates up to index 124 (4 + 62 + 58) should become available in store eventually.
    verify_certificates_in_store(
        &certificate_store,
        &certificates[0..(num_written + second_batch_len)],
        10,  // 6 fetched certs verified directly + the initial 4 inserted
        114, // verified indirectly
    )
    .await;
    num_written += second_batch_len;

    // No new fetch request is expected.
    sleep(Duration::from_secs(5)).await;
    loop {
        match fake_receiver.try_recv() {
            Ok(NetworkCommand::SendRequest {
                peer: _,
                request: PrimaryRequest::MissingCertificates { inner },
                reply,
            }) => {
                let (_, skip_rounds) = inner.get_bounds().unwrap();
                let first_num_skip_rounds = skip_rounds.values().next().unwrap().len();
                if first_num_skip_rounds == 16 || first_num_skip_rounds == 17 {
                    // Drain the fetch requests sent out before the last reply.
                    reply
                        .send(Ok(PrimaryResponse::RequestedCertificates(second_batch_resp.clone())))
                        .unwrap();
                    continue;
                }
                panic!("No more fetch request is expected! {inner:#?}");
            }
            Ok(_) => {}
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => panic!("Unexpected disconnect!"),
        }
    }

    let target_index = num_written + 204;
    let expected_digest = certificates[target_index].digest();
    let error = synchronizer.process_peer_certificate(certificates[target_index].clone()).await;
    assert_matches!(error, Err(CertManagerError::Pending(digest)) if digest == expected_digest);

    // Verify the fetch request.
    if let Some(req) = fake_receiver.recv().await {
        match req {
            NetworkCommand::SendRequest { peer: _, request, reply } => match request {
                PrimaryRequest::MissingCertificates { inner } => {
                    let (lower_bound, skip_rounds) = inner.get_bounds().unwrap();
                    assert_eq!(lower_bound, 0);
                    assert_eq!(skip_rounds.len(), fixture.authorities().count());
                    for rounds in skip_rounds.values() {
                        assert_eq!(rounds, &(1..32).collect());
                    }

                    // Send out a batch of malformed certificates.
                    let mut certs = Vec::new();
                    // Add cert missing parent info.
                    let mut cert = certificates[num_written].clone();
                    cert.header_mut_for_test().clear_parents_for_test();
                    certs.push(cert);
                    // Add cert with incorrect digest.
                    let mut cert = certificates[num_written].clone();

                    // Use dummy, default header for bad data
                    let wolf_header = Header::default();
                    cert.update_header_for_test(wolf_header);
                    certs.push(cert);
                    // Add cert without all parents in storage.
                    certs.push(certificates[num_written + 1].clone());
                    reply.send(Ok(PrimaryResponse::RequestedCertificates(certs))).unwrap();
                }
                _ => panic!("not missing certs!"),
            },
            _ => panic!("not send request!"),
        }
    } else {
        panic!("no request!")
    }

    // Verify no certificate is written to store.
    sleep(Duration::from_secs(1)).await;
    verify_certificates_not_in_store(&certificate_store, &certificates[num_written..target_index]);

    assert!(!synchronizer
        .identify_unkown_parents(&certificates[target_index].header)
        .await
        .unwrap()
        .is_empty());

    // Verify the fetch request.
    if let Some(req) = fake_receiver.recv().await {
        match req {
            NetworkCommand::SendRequest { peer: _, request, reply } => match request {
                PrimaryRequest::MissingCertificates { inner } => {
                    let (lower_bound, skip_rounds) = inner.get_bounds().unwrap();
                    assert_eq!(lower_bound, 0);
                    assert_eq!(skip_rounds.len(), fixture.authorities().count());
                    for rounds in skip_rounds.values() {
                        assert_eq!(rounds, &(1..32).collect());
                    }

                    // Send out a batch of certificates with bad signatures for all certificates.
                    let mut certs = Vec::new();
                    for cert in certificates.iter().skip(num_written).take(204) {
                        let mut cert = cert.clone();
                        cert.set_signature_verification_state(
                            SignatureVerificationState::Unverified(BlsSignature::default()),
                        );
                        certs.push(cert);
                    }
                    reply.send(Ok(PrimaryResponse::RequestedCertificates(certs))).unwrap();
                }
                _ => panic!("not missing certs!"),
            },
            _ => panic!("not send request!"),
        }
    } else {
        panic!("no request!")
    }

    sleep(Duration::from_secs(1)).await;
    verify_certificates_not_in_store(&certificate_store, &certificates[num_written..target_index]);

    assert!(!synchronizer
        .identify_unkown_parents(&certificates[target_index].header)
        .await
        .unwrap()
        .is_empty());

    // Verify the fetch request.
    if let Some(req) = fake_receiver.recv().await {
        match req {
            NetworkCommand::SendRequest { peer: _, request, reply } => match request {
                PrimaryRequest::MissingCertificates { inner } => {
                    let (lower_bound, skip_rounds) = inner.get_bounds().unwrap();
                    assert_eq!(lower_bound, 0);
                    assert_eq!(skip_rounds.len(), fixture.authorities().count());
                    for rounds in skip_rounds.values() {
                        assert_eq!(rounds, &(1..32).collect());
                    }

                    // Send out a batch of certificates with good signatures.
                    // The certificates 4 + 62 + 58 + 204 = 328 should become available in store
                    // eventually
                    let mut certs = Vec::new();
                    for cert in certificates.iter().skip(num_written).take(204) {
                        certs.push(cert.clone());
                    }
                    reply.send(Ok(PrimaryResponse::RequestedCertificates(certs))).unwrap();
                }
                _ => panic!("not missing certs!"),
            },
            _ => panic!("not send request!"),
        }
    } else {
        panic!("no request!")
    }

    verify_certificates_in_store(
        &certificate_store,
        &certificates[(target_index - 60)..(target_index)],
        4,  /* 18,  // 14 fetched certs verified directly + the initial 4 inserted (what's left
             * in the range) */
        56, //310, // to be verified indirectly (what's left in the range)
    )
    .await;
}
