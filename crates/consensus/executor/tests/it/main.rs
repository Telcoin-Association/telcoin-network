//! Subscriber IT tests

#![allow(unused_crate_dependencies)]

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tempfile::TempDir;
use tn_executor::subscriber::spawn_subscriber;
use tn_network_libp2p::types::{MessageId, NetworkCommand};
use tn_network_types::MockPrimaryToWorkerClient;
use tn_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus,
};
use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase};
use tn_test_utils::{create_signed_certificates_for_rounds, CommitteeFixture};
use tn_types::{
    now, test_chain_spec_arc, Batch, BlockHash, CommittedSubDag, ConsensusHeaderDigest,
    ConsensusNumHash, ExecHeader, Hash as _, HeaderBuilder, HeaderDigest, ReputationScores,
    SealedHeader, TaskManager, TnReceiver as _, TnSender as _, WorkerId, B256,
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
    let task_manager = TaskManager::new("subscriber tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();
    let temp_dir = TempDir::with_prefix("test_output_to_header").unwrap();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), config.committee().clone())
            .await
            .unwrap();

    // subscribe to channels early
    let rx_consensus_headers = consensus_bus.app().last_consensus_header().subscribe();
    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

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
    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        consensus_chain.clone(),
        u64::max_value(),
    );

    // yield for subscriber to spawn
    tokio::task::yield_now().await;

    // make certificates for rounds 1 to 7 (inclusive)
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=7, &fixture, &[]);

    // Set up mock worker.
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await;
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    Consensus::spawn(
        config.clone(),
        &consensus_bus,
        bullshark,
        &task_manager,
        consensus_chain,
        None,
    )
    .await;

    // forward certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    let expected_num = 3;
    let mut consensus_headers_seen: Vec<_> = Vec::with_capacity(expected_num);
    while let Some(output) = consensus_output.recv().await {
        // assert epoch boundary not reached
        assert!(!output.close_epoch());

        let num = output.number();
        let consensus_header = output.consensus_header();
        consensus_headers_seen.push(consensus_header);
        if num == expected_num as u64 {
            break;
        }

        // yield for other tasks
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let last_header = rx_consensus_headers.borrow().clone().unwrap_or_default();
    assert!(last_header.number == expected_num as u64);

    // NOTE: output.consensus_header() creates the consensus header and should be the same
    // result
    assert_eq!(
        last_header.digest(),
        consensus_headers_seen.last().expect("last consensus header").digest()
    );

    Ok(())
}

/// Test that ConsensusOutput is delivered in strict sequential order.
/// The output.number should be monotonically increasing.
#[tokio::test]
async fn test_executor_output_ordering() -> eyre::Result<()> {
    let num_sub_dags_per_schedule = 100; // High to avoid schedule changes
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let task_manager = TaskManager::new("ordering tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();
    let temp_dir = TempDir::with_prefix("test_executor_output_ordering").unwrap();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).await.unwrap();

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        consensus_chain.clone(),
        u64::max_value(),
    );
    tokio::task::yield_now().await;

    // Create more rounds for multiple commits
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=11, &fixture, &[]);

    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await;
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager2 = TaskManager::default();
    Consensus::spawn(
        config.clone(),
        &consensus_bus,
        bullshark,
        &task_manager2,
        consensus_chain.clone(),
        None,
    )
    .await;

    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // Verify outputs are in strict order
    let expected_count = 5;
    let mut last_number = 0u64;
    let mut count = 0;

    while let Some(output) = consensus_output.recv().await {
        let num = output.number();

        // Verify monotonically increasing
        assert!(num > last_number, "Output number {} should be > previous {}", num, last_number);
        last_number = num;
        count += 1;

        if count >= expected_count {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(count >= expected_count, "Should receive at least {} outputs", expected_count);
    Ok(())
}

/// Test that all batches from certificates are included in ConsensusOutput.
#[tokio::test]
async fn test_executor_batch_fetching() -> eyre::Result<()> {
    let num_sub_dags_per_schedule = 100;
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let task_manager = TaskManager::new("batch fetching tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();
    let temp_dir = TempDir::with_prefix("test_executor_output_ordering").unwrap();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).await.unwrap();

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        consensus_chain.clone(),
        u64::max_value(),
    );
    tokio::task::yield_now().await;

    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=7, &fixture, &[]);

    let batch_count = batches.len();
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await;
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager2 = TaskManager::default();
    Consensus::spawn(
        config.clone(),
        &consensus_bus,
        bullshark,
        &task_manager2,
        consensus_chain,
        None,
    )
    .await;

    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // Collect batches from outputs
    let mut total_batches_received = 0;
    let expected_outputs = 3;
    let mut outputs_received = 0;

    while let Some(output) = consensus_output.recv().await {
        // Count batches in this output
        for certified_batch in output.batches() {
            total_batches_received += certified_batch.batches.len();
        }

        // Also count batch_digests
        let digest_count = output.batch_digests().len();
        assert!(
            digest_count > 0 || output.batches().iter().all(|cb| cb.batches.is_empty()),
            "Should have batch digests if batches exist"
        );

        outputs_received += 1;
        if outputs_received >= expected_outputs {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Verify we received batches
    assert!(
        total_batches_received > 0,
        "Should have received batches, got {} from {} total available",
        total_batches_received,
        batch_count
    );

    Ok(())
}

/// Creates a random payload with the given number of batches.
///
/// Returns the payload map for a header and the batch map for the mock client.
fn random_payload(count: usize) -> (HashMap<BlockHash, Batch>, Vec<(Batch, WorkerId)>) {
    let chain = test_chain_spec_arc();
    let mut batch_map = HashMap::new();
    let mut payload_entries = Vec::with_capacity(count);
    for _ in 0..count {
        let batch = tn_reth::test_utils::batch(chain.clone());
        let d = batch.digest();
        batch_map.insert(d, batch.clone());
        payload_entries.push((batch, 0));
    }
    (batch_map, payload_entries)
}

/// Test that duplicate batch digests within a single committed SubDag are handled gracefully.
///
/// Constructs a DAG where one authority includes the same batch digest in certificates at two
/// different rounds. When Bullshark commits a SubDag containing both certificates, the
/// subscriber must not crash with `MissingFetchedBatch` on the second occurrence.
#[tokio::test]
async fn test_duplicate_batch_digest() -> eyre::Result<()> {
    let num_sub_dags_per_schedule = 100;
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let task_manager = TaskManager::new("duplicate batch tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();
    let temp_dir = TempDir::with_prefix("test_duplicate_batch_digest").unwrap();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).await.unwrap();

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    // network mock to handle publish commands
    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        consensus_chain.clone(),
        u64::max_value(),
    );
    tokio::task::yield_now().await;

    // collect authority ids in order
    let authorities: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    // the batch that will appear in two different certificates (authority[2], rounds 2 and 3)
    let chain = test_chain_spec_arc();
    let shared_batch = tn_reth::test_utils::batch(chain);
    let shared_digest = shared_batch.digest();

    // accumulate all batches for the mock client
    let mut all_batches: HashMap<BlockHash, Batch> = HashMap::new();
    all_batches.insert(shared_digest, shared_batch.clone());

    // genesis parents for round 1
    let mut parents: BTreeSet<HeaderDigest> = fixture.genesis().collect();
    let mut all_certificates = Vec::new();

    // build 5 rounds of certificates
    for round in 1u32..=5 {
        let mut round_digests = BTreeSet::new();

        for (idx, auth_id) in authorities.iter().enumerate() {
            // authority[2] gets the shared batch in rounds 2 and 3
            let use_shared = idx == 2 && (round == 2 || round == 3);

            let (batch_map, payload_entries) = if use_shared {
                // include the shared batch plus some random ones
                let (mut bmap, mut entries) = random_payload(2);
                bmap.insert(shared_digest, shared_batch.clone());
                entries.push((shared_batch.clone(), 0));
                (bmap, entries)
            } else {
                random_payload(3)
            };

            all_batches.extend(batch_map);

            // build header using with_payload_batch for each batch
            let mut builder = HeaderBuilder::default()
                .author(auth_id.clone())
                .round(round)
                .epoch(0)
                .parents(parents.clone())
                .created_at(now());

            for (batch, worker_id) in payload_entries {
                builder = builder.with_payload_batch(&batch, worker_id);
            }

            let header = builder.build();
            let cert = fixture.certificate(&header);
            round_digests.insert(cert.digest());
            all_certificates.push(cert);
        }

        parents = round_digests;
    }

    // set up mock worker with all batches
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches: all_batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await;
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager2 = TaskManager::default();
    Consensus::spawn(
        config.clone(),
        &consensus_bus,
        bullshark,
        &task_manager2,
        consensus_chain,
        None,
    )
    .await;

    // feed all certificates to trigger subdag commits
    for certificate in all_certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // collect outputs with a timeout to avoid hanging
    let expected_outputs = 2;
    let mut outputs_received = 0;
    let mut found_shared_digest = false;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(output) = consensus_output.recv().await {
            // check if any output contains the shared digest
            for digest in output.batch_digests() {
                if *digest == shared_digest {
                    found_shared_digest = true;
                }
            }

            outputs_received += 1;
            if outputs_received >= expected_outputs {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "Timed out waiting for consensus outputs — subscriber may have crashed"
    );
    assert!(
        outputs_received >= expected_outputs,
        "Expected at least {expected_outputs} outputs, got {outputs_received}"
    );
    assert!(found_shared_digest, "The shared batch digest should appear in at least one output");

    Ok(())
}

/// Test a single output containing one batch digest referenced by two certificates.
///
/// The subscriber must clone the duplicate so `batch_digests` and the flattened batches
/// stay aligned: every flattened index maps to its own digest (block digest/mix hash
/// binding) and the final index satisfies `close_epoch_for_last_batch` (epoch closing
/// system calls).  Feeds a crafted subdag directly to the subscriber, bypassing Bullshark,
/// so the duplicate is deterministically within one output.
#[tokio::test]
async fn test_subscriber_dup_batch_across_certs() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let task_manager = TaskManager::new("dup batch alignment tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();
    let temp_dir = TempDir::with_prefix("test_subscriber_dup_batch").unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).await.unwrap();

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    // network mock to handle publish commands
    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        consensus_chain.clone(),
        u64::max_value(),
    );
    tokio::task::yield_now().await;

    // three batches; batch_1 is shared between the two certificates
    let chain = test_chain_spec_arc();
    let batch_0 = tn_reth::test_utils::batch(chain.clone());
    let batch_1 = tn_reth::test_utils::batch(chain.clone());
    let batch_2 = tn_reth::test_utils::batch(chain);
    let mock_batches: HashMap<BlockHash, Batch> = [
        (batch_0.digest(), batch_0.clone()),
        (batch_1.digest(), batch_1.clone()),
        (batch_2.digest(), batch_2.clone()),
    ]
    .into_iter()
    .collect();
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches: mock_batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    // two round 1 certificates from different authorities sharing batch_1
    let authorities: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let parents: BTreeSet<HeaderDigest> = fixture.genesis().collect();
    let header_a = HeaderBuilder::default()
        .author(authorities[0].clone())
        .round(1)
        .epoch(0)
        .parents(parents.clone())
        .created_at(now())
        .with_payload_batch(&batch_0, 0)
        .with_payload_batch(&batch_1, 0)
        .build();
    let cert_a = fixture.certificate(&header_a);
    let header_b = HeaderBuilder::default()
        .author(authorities[1].clone())
        .round(1)
        .epoch(0)
        .parents(parents)
        .created_at(now())
        .with_payload_batch(&batch_1, 0)
        .with_payload_batch(&batch_2, 0)
        .build();
    let cert_b = fixture.certificate(&header_b);

    let sub_dag = CommittedSubDag::new(
        vec![cert_a, cert_b.clone()],
        cert_b,
        1,
        ReputationScores::default(),
        None,
    );
    // expected digests in subdag iteration order (contains the duplicate)
    let expected_digests: Vec<BlockHash> =
        sub_dag.headers().iter().flat_map(|header| header.payload().keys().copied()).collect();
    assert_eq!(expected_digests.len(), 4, "two payloads of two with one shared digest");

    // feed the subdag straight to the subscriber
    consensus_bus.sequence().send(sub_dag).await?;

    let mut output = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        consensus_output.recv().await
    })
    .await
    .expect("timed out waiting for consensus output — subscriber may have crashed")
    .expect("consensus output");

    // digests and flattened batches must stay aligned, including the duplicate
    assert_eq!(output.batch_digests().len(), expected_digests.len());
    let flattened = output.flatten_batches();
    assert_eq!(
        flattened.len(),
        output.batch_digests().len(),
        "uneven number of batches and batch digests"
    );
    for (index, (cert_idx, batch_idx)) in flattened.iter().enumerate() {
        let batch = &output.batches()[*cert_idx].batches[*batch_idx];
        let digest = output.get_batch_digest(index).expect("digest for index");
        assert_eq!(digest, expected_digests[index], "wrong digest order at index {index}");
        assert_eq!(
            batch.digest(),
            digest,
            "batch executed at index {index} bound to the wrong digest"
        );
    }

    // both certificates carry their full batch lists (duplicate cloned into the second)
    let batches = output.batches();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].batches.len(), 2);
    assert_eq!(batches[1].batches.len(), 2);
    assert_eq!(batches[1].batches[0].digest(), batch_1.digest(), "duplicate batch not cloned");

    // the last flattened index closes the epoch (engine applies closing system calls)
    output.set_epoch_close();
    let last = output.batch_digests().len() - 1;
    for index in 0..last {
        assert_eq!(
            output.close_epoch_for_last_batch(index),
            Some(false),
            "index {index} must not close the epoch"
        );
    }
    assert_eq!(
        output.close_epoch_for_last_batch(last),
        Some(true),
        "last batch must close the epoch"
    );

    Ok(())
}
