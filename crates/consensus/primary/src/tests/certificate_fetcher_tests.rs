//! Certificate fetcher tests

use crate::{
    certificate_fetcher::{CertificateFetcher, CertificateFetcherCommand, FetchTask},
    error::CertManagerError,
    network::{PrimaryRequest, PrimaryResponse},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use assert_matches::assert_matches;
use std::{collections::BTreeSet, future::Future as _, time::Duration};
use tn_config::Parameters;
use tn_network_libp2p::{
    error::NetworkError,
    types::{NetworkCommand, NetworkHandle},
};
use tn_storage::{mem_db::MemDatabase, CertificateStore, PayloadStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    test_utils::init_test_tracing, BlsSignature, Certificate, Hash as _, Header,
    SignatureVerificationState, TaskManager, TnSender as _,
};
use tokio::{
    sync::mpsc::{self, error::TryRecvError},
    time::{sleep, timeout},
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
async fn test_fetch_certificates_basic() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    // spawn certificate fetcher
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

    // store batches to prevent missing payload issues
    for (digest, worker_id) in headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }

    let total_certificates = fixture.authorities().count() * rounds as usize;
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_fetch_cancellation_on_success() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_consensus_parameters(Parameters {
            parallel_fetch_request_delay_interval: Duration::from_millis(100),
            ..Default::default()
        })
        .randomize_ports(true)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        primary.consensus_config(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // setup: only store genesis certificates
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    // create certificates for multiple rounds but don't store them
    let mut all_certificates = vec![];
    let mut current_parents: BTreeSet<_> = genesis_certs.iter().map(|c| c.digest()).collect();

    // round 1 certificates - DON'T STORE
    let (_, round1_headers) = fixture.headers_round(1, &current_parents);
    for (digest, worker_id) in round1_headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }
    let round1_certs: Vec<_> = round1_headers.iter().map(|h| fixture.certificate(h)).collect();
    current_parents = round1_certs.iter().map(|c| c.digest()).collect();
    all_certificates.extend(round1_certs.clone());

    // round 2 certificates - DON'T STORE
    let (_, round2_headers) = fixture.headers_round(2, &current_parents);
    for (digest, worker_id) in round2_headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }
    let round2_certs: Vec<_> = round2_headers.iter().map(|h| fixture.certificate(h)).collect();
    all_certificates.extend(round2_certs.clone());

    // try to process a round 2 certificate - this will find missing round 1 parents
    let target_cert = round2_certs[0].clone();
    let result = synchronizer.process_peer_certificate(target_cert).await;

    // should be pending due to missing parents
    assert!(result.is_err(), "Should fail due to missing parents");

    // expecxt a fetch request for the missing certificates
    if let Some(NetworkCommand::SendRequest {
        peer: _,
        request: PrimaryRequest::MissingCertificates { .. },
        reply,
    }) = timeout(Duration::from_secs(2), fake_receiver.recv())
        .await
        .expect("Should get fetch request")
    {
        // first peer responds immediately with all missing certificates
        reply.send(Ok(PrimaryResponse::RequestedCertificates(all_certificates.clone()))).unwrap();
    } else {
        panic!("Expected a fetch request for missing certificates");
    }

    // wait for potential second request (should not happen due to cancellation)
    let timeout_result = timeout(Duration::from_millis(500), fake_receiver.recv()).await;

    // should timeout - no second request should be made after success
    assert!(timeout_result.is_err(), "No additional requests should be made after success");

    // verify certificates were actually stored
    sleep(Duration::from_millis(100)).await;
    for cert in &round1_certs {
        assert!(
            certificate_store.read(cert.digest()).unwrap().is_some(),
            "Round 1 certificates should be stored after fetch"
        );
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_timeout_scenario() {
    init_test_tracing();
    let parallel_fetch_request_delay_interval = Duration::from_millis(100);
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_consensus_parameters(Parameters {
            parallel_fetch_request_delay_interval,
            ..Default::default()
        })
        .randomize_ports(true)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let certificate_store = config.node_storage().clone();
    let payload_store = config.node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        config.clone(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // setup: only store genesis certificates
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    // create round 1 certificates but DON'T store them
    let parents: BTreeSet<_> = genesis_certs.iter().map(|c| c.digest()).collect();
    let (_, round1_headers) = fixture.headers_round(1, &parents);
    for (digest, worker_id) in round1_headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }
    let round1_certificates: Vec<_> =
        round1_headers.iter().map(|h| fixture.certificate(&h)).collect();

    // Create round 2 certificates that depend on round 1 (which are missing)
    let round1_parents: BTreeSet<_> = round1_certificates.iter().map(|c| c.digest()).collect();
    let (_, round2_headers) = fixture.headers_round(2, &round1_parents);
    for (digest, worker_id) in round2_headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }
    let round2_certificates: Vec<_> =
        round2_headers.iter().map(|h| fixture.certificate(&h)).collect();

    // trigger fetch by processing a round 2 certificate (which needs round 1 parents)
    let result = synchronizer.process_peer_certificate(round2_certificates[0].clone()).await;
    assert!(result.is_err(), "Should fail due to missing parents");

    // calculate expected timeout
    let num_peers = fixture.committee().authorities().len() - 1;
    let expected_timeout =
        parallel_fetch_request_delay_interval * config.committee().authorities().len() as u32;
    let start_time = tokio::time::Instant::now();
    let mut request_count = 0;

    // collect all requests but never respond (simulate timeout)
    loop {
        match timeout(Duration::from_millis(200), fake_receiver.recv()).await {
            Ok(Some(NetworkCommand::SendRequest {
                peer: _,
                request: PrimaryRequest::MissingCertificates { .. },
                reply: _, // don't respond - let the oneshot channel drop to simulate no response
            })) => {
                request_count += 1;
                // continue collecting requests until we've seen them all
            }
            _ => {
                // either timeout or channel closed
                if request_count >= num_peers {
                    break;
                }
                // keep trying if we haven't collected all requests yet
            }
        }
    }

    // wait for the fetch operation to timeout
    tokio::time::sleep(expected_timeout + Duration::from_secs(1)).await;

    // verify timeout occurred within expected timeframe
    let elapsed = start_time.elapsed();
    assert!(
        elapsed >= expected_timeout,
        "Should have waited at least {:?}, but only waited {:?}",
        expected_timeout,
        elapsed
    );

    // round 1 certificates should NOT be in store (fetch timed out)
    verify_certificates_not_in_store(&certificate_store, &round1_certificates);
    // round 2 certificates should also NOT be in store
    verify_certificates_not_in_store(&certificate_store, &round2_certificates);

    // expect follow up attempt
    match timeout(Duration::from_millis(200), fake_receiver.recv()).await {
        Ok(Some(NetworkCommand::SendRequest { .. })) => {
            return;
        }
        _ => {
            // either timeout or channel closed
            panic!("cert fetcher gave up");
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_gc_round_update_during_fetch() {
    init_test_tracing();
    // overwrite parallel fetch request delay interval for tests
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_consensus_parameters(Parameters {
            parallel_fetch_request_delay_interval: Duration::from_millis(100),
            ..Default::default()
        })
        .randomize_ports(true)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        primary.consensus_config(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // Setup certificates across multiple rounds
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    let mut all_certificates = vec![];
    let mut current_parents: BTreeSet<_> = genesis_certs.iter().map(|c| c.digest()).collect();

    for round in 1..=10 {
        let (_, headers) = fixture.headers_round(round, &current_parents);
        for (digest, worker_id) in headers.iter().flat_map(|h| h.payload().iter()) {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        let round_certs: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        current_parents = round_certs.iter().map(|c| c.digest()).collect();
        all_certificates.extend(round_certs);
    }

    // trigger fetch for round 8 certificate
    let target_cert = all_certificates.iter().find(|c| c.header().round() == 8).unwrap();
    let _ = synchronizer.process_peer_certificate(target_cert.clone()).await;

    // first fetch request
    if let Some(NetworkCommand::SendRequest {
        peer: _,
        request: PrimaryRequest::MissingCertificates { inner },
        reply,
    }) = fake_receiver.recv().await
    {
        let (lower_bound, _) = inner.get_bounds().unwrap();
        assert_eq!(lower_bound, 0, "initial GC round should be 0");

        // don't respond yet
        drop(reply);
    }

    // update GC round to 5
    cb.gc_round_updates().send(5).unwrap();
    sleep(Duration::from_millis(100)).await;

    // trigger another fetch with Kick
    cb.certificate_fetcher().send(CertificateFetcherCommand::Kick).await.unwrap();

    // next fetch should use updated gc round
    loop {
        match timeout(Duration::from_secs(2), fake_receiver.recv()).await {
            Ok(Some(NetworkCommand::SendRequest {
                peer: _,
                request: PrimaryRequest::MissingCertificates { inner },
                reply,
            })) => {
                let (lower_bound, skip_rounds) = inner.get_bounds().unwrap();
                if lower_bound == 5 {
                    // request received with updated GC round
                    assert_eq!(lower_bound, 5, "should use updated GC round");

                    // verify skip_rounds don't include rounds <= 5
                    for rounds in skip_rounds.values() {
                        assert!(
                            rounds.iter().all(|&r| r > 5),
                            "should not fetch rounds <= GC round"
                        );
                    }

                    reply
                        .send(Ok(PrimaryResponse::RequestedCertificates(
                            all_certificates
                                .iter()
                                .filter(|c| c.header().round() > 5)
                                .cloned()
                                .collect(),
                        )))
                        .unwrap();

                    // break here to prevent timeout
                    break;
                }
                // continue for old requests
                drop(reply);
            }
            _ => {
                panic!("Should receive fetch request with updated GC round but timedout");
            }
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_network_failure_keeps_trying() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let certificate_store = config.node_storage().clone();
    let payload_store = config.node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        config,
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // only store genesis certificates
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    // create 1 round but only store payloads
    let parents: BTreeSet<_> = genesis_certs.iter().map(|c| c.digest()).collect();
    let (_, round1_headers) = fixture.headers_round(1, &parents);
    for (digest, worker_id) in round1_headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }
    let round1_certs: Vec<_> =
        round1_headers.into_iter().map(|h| fixture.certificate(&h)).collect();

    // create round 2 certificates that depend on missing round 1
    let round1_parents: BTreeSet<_> = round1_certs.iter().map(|c| c.digest()).collect();
    let (_, round2_headers) = fixture.headers_round(2, &round1_parents);
    for (digest, worker_id) in round2_headers.iter().flat_map(|h| h.payload().iter()) {
        payload_store.write_payload(digest, worker_id).unwrap();
    }
    let round2_certs: Vec<_> = round2_headers.iter().map(|h| fixture.certificate(&h)).collect();

    // trigger fetch
    let result = synchronizer.process_peer_certificate(round2_certs[0].clone()).await;
    assert!(result.is_err(), "should fail due to missing parents");

    let num_peers = fixture.committee().authorities().len() - 1;
    let mut response_count = 0;

    // all peers return network errors
    loop {
        match timeout(Duration::from_secs(5), fake_receiver.recv()).await {
            Ok(Some(NetworkCommand::SendRequest { reply, .. })) => {
                // simulate network failure
                reply.send(Err(NetworkError::Timeout)).unwrap();
                response_count += 1;

                if response_count >= num_peers {
                    break;
                }
            }
            _ => break,
        }
    }

    // certificates are missing
    verify_certificates_not_in_store(&certificate_store, &round1_certs);
    verify_certificates_not_in_store(&certificate_store, &round2_certs);

    // expect follow up attempt
    match timeout(Duration::from_millis(200), fake_receiver.recv()).await {
        Ok(Some(NetworkCommand::SendRequest { .. })) => {
            return;
        }
        _ => {
            // either timeout or channel closed
            panic!("cert fetcher gave up");
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_partial_response_handling_rejects_invalid_cert() {
    init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        primary.consensus_config(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // genesis certificates
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    let mut all_certificates = vec![];
    let mut current_parents: BTreeSet<_> = genesis_certs.iter().map(|c| c.digest()).collect();

    for round in 1..=3 {
        let (_, headers) = fixture.headers_round(round, &current_parents);
        for (digest, worker_id) in headers.iter().flat_map(|h| h.payload().iter()) {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        let round_certs: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        current_parents = round_certs.iter().map(|c| c.digest()).collect();
        all_certificates.extend(round_certs);
    }

    // trigger fetch
    let target_index = all_certificates.len() - 1;
    let _ = synchronizer.process_peer_certificate(all_certificates[target_index].clone()).await;

    // track bad cert for later assertion
    //
    // NOTE: malform cert from latest round to ensure direct verification
    // threat model is 1/3, so picking bad cert in the middle will be indirectly verified
    // because 2f+1 peers reference it as a parent
    let mut bad_cert = all_certificates[target_index - 1].clone();

    // first peer returns partial response with some invalid certificates
    if let Some(NetworkCommand::SendRequest { reply, .. }) = fake_receiver.recv().await {
        let mut response = vec![];

        // add some valid certificates
        response.extend(all_certificates.iter().take(5).cloned());

        // add certificate with bad signature
        // indicate verified directly to ensure this is reset
        bad_cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(), // invalid
        ));
        response.push(bad_cert.clone());

        // add more valid certificates
        response.extend(all_certificates.iter().skip(6).take(4).cloned());

        // send malicious payload
        reply.send(Ok(PrimaryResponse::RequestedCertificates(response))).unwrap();
    }

    // allow time for writes to db
    sleep(Duration::from_secs(2)).await;
    // NOTE: the cert manager will reject - not fetcher
    verify_certificates_not_in_store(&certificate_store, &vec![bad_cert]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_bad_cert_in_fetch_rejects_all() {
    init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, mut fake_receiver) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        primary.consensus_config(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // genesis certificates
    let genesis_certs: Vec<_> = Certificate::genesis(&fixture.committee());
    for cert in genesis_certs.iter() {
        certificate_store.write(cert.clone()).expect("Writing certificate to store failed");
    }

    let mut all_certificates = vec![];
    let mut current_parents: BTreeSet<_> = genesis_certs.iter().map(|c| c.digest()).collect();

    for round in 1..=3 {
        let (_, headers) = fixture.headers_round(round, &current_parents);
        for (digest, worker_id) in headers.iter().flat_map(|h| h.payload().iter()) {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        let round_certs: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        current_parents = round_certs.iter().map(|c| c.digest()).collect();
        all_certificates.extend(round_certs);
    }

    // trigger fetch
    let target_index = all_certificates.len() - 1;
    let _ = synchronizer.process_peer_certificate(all_certificates[target_index].clone()).await;

    // track bad cert for later assertion
    // use middle round
    let mut bad_cert = all_certificates[5].clone();

    // first peer returns partial response with some invalid certificates
    if let Some(NetworkCommand::SendRequest { reply, .. }) = fake_receiver.recv().await {
        let mut response = vec![];

        // add some valid certificates
        response.extend(all_certificates.iter().take(5).cloned());

        // NOTE: genesis certs are generated locally and always valid
        // they should never be sent by peers
        bad_cert.set_signature_verification_state(SignatureVerificationState::Genesis);
        response.push(bad_cert.clone());

        // add more valid certificates
        response.extend(all_certificates.iter().skip(6).take(4).cloned());

        // send malicious payload
        reply.send(Ok(PrimaryResponse::RequestedCertificates(response))).unwrap();
    }

    // allow time for writes to db
    sleep(Duration::from_secs(2)).await;
    // NOTE: the cert manager will reject - not fetcher
    verify_certificates_not_in_store(&certificate_store, &all_certificates);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_fetch_task_replacement() {
    init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    let (sender, _) = mpsc::channel(1000);
    let client_network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    CertificateFetcher::spawn(
        primary.consensus_config(),
        client_network.into(),
        cb.clone(),
        synchronizer.clone(),
        &task_manager,
    );

    // test FetchTask behavior
    let mut fetch_task = FetchTask::new();
    assert!(fetch_task.is_none(), "New FetchTask should have no pending task");

    // set a task
    let (tx1, rx1) = tokio::sync::oneshot::channel();
    fetch_task.set_task(rx1);
    assert!(!fetch_task.is_none(), "FetchTask should have pending task");

    // replace with new task
    let (tx2, rx2) = tokio::sync::oneshot::channel();
    fetch_task.set_task(rx2);
    assert!(!fetch_task.is_none(), "FetchTask should have new pending task");

    // first task should be cancelled
    assert!(tx1.is_closed(), "First task should be cancelled when replaced");

    // complete second task
    tx2.send(Ok(())).unwrap();

    // poll the future to completion
    let waker = futures::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let result = std::pin::Pin::new(&mut fetch_task).poll(&mut cx);
    assert!(matches!(result, std::task::Poll::Ready(Ok(()))));
    assert!(fetch_task.is_none(), "FetchTask should be empty after completion");
}
