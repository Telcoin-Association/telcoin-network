//! Node IT tests

// unused deps lint confusion
#![allow(unused_crate_dependencies)]

use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_engine::ExecutorEngine;
use tn_executor::subscriber::spawn_subscriber;
use tn_network_libp2p::types::{MessageId, NetworkCommand};
use tn_network_types::MockPrimaryToWorkerClient;
use tn_node::catchup_accumulator;
use tn_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    test_utils::temp_dir,
    ConsensusBus,
};
use tn_reth::{test_utils::seeded_genesis_from_random_batches, RethChainSpec};
use tn_storage::{mem_db::MemDatabase, tables::ConsensusBlocks, ConsensusStore};
use tn_test_utils::{
    create_signed_certificates_for_rounds, default_test_execution_node, CommitteeFixture,
};
use tn_types::{
    adiri_genesis, gas_accumulator::GasAccumulator, now, Batch, BlockNumHash, Certificate,
    CertificateDigest, Database as _, ExecHeader, Hash as _, HeaderBuilder, Notifier, SealedHeader,
    TaskManager, TnReceiver as _, TnSender as _, B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::debug;

#[tokio::test]
async fn test_catchup_accumulator() -> eyre::Result<()> {
    let tmp = temp_dir();
    // create deterministic committee fixture and use first authority's components
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let consensus_bus = ConsensusBus::new();

    // make certificates for rounds 1 to 7 with batches of txs
    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    // fund accounts in genesis so txs execute
    let genesis = adiri_genesis();
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution env
    let gas_accumulator = GasAccumulator::new(1);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;

    // manually create engine
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let max = Some(max_round as u64 - 1); // consensus needs 1 extra round to commit
    let parent = chain.sealed_genesis_header();

    // start engine
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        max,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
    });

    // subscribe to output early
    let mut consensus_output = consensus_bus.subscribe_consensus_output();

    // spawn consensus to send output to engine for full execution
    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        consensus_store.clone(),
        &task_manager,
    );

    // send certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // simulate epoch manager's role:
    // forward consensus output to engine until `max_round`
    let mut rewards = HashMap::new();
    loop {
        tokio::select! {
            // forward output from consensus to engine
            Some(output) = consensus_output.recv() => {
                debug!(target: "gas-test", output=?output.leader(), round=output.leader().round(), "received output");
                let leader = output.leader().origin().clone();
                // manually track values as well
                rewards.entry(leader).and_modify(|count| *count += 1).or_insert(1);
                to_engine.send(output).await?;
            }
            // wait for engine to reach `max_round` or timeout
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                // engine shutdown
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    // check results
    debug!(target: "gas-test", "gas accumulator:\n{:#?}", gas_accumulator);
    // initialize a new gas accumulator to simulate node recovery
    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(&consensus_store, reth_env.clone(), &recovered)?;
    // assert recovered and active track the same expected values
    //      G48pDy85GhyGMp9afPBvWgaNzgPAnvBtMxjReQTe1NiN: 3,
    //      Agv7rsffEbxoa7ybTJj57TiAHchf27ia7ziB5CVrHNTk: 3,
    //      73HL4cMSiCfGthUE7xM1F8JwwYfmM53wQi4r34ECrs3F: 3,
    //      2VDmuopDmr9KZcp4z9q9ne2CAxkaF2ftMt6ejzp42FM7: 1,
    debug!(target: "gas-test", "recovered accumulator:\n{:#?}", recovered);
    assert_eq!(gas_accumulator.get_values(0), (231, 9702000, 6930000000));
    assert_matching_worker_gas_values(&gas_accumulator, &recovered);

    // convert manually calculated rewards for assertion
    let expected: BTreeMap<_, _> = rewards
        .iter()
        .map(|(auth, count)| {
            (fixture.authority_by_id(auth).expect("in committee").execution_address(), *count)
        })
        .collect();

    // assert rewards
    assert_eq!(expected, gas_accumulator.rewards_counter().get_address_counts());
    assert_eq!(expected, recovered.rewards_counter().get_address_counts());

    Ok(())
}

/// Build certificates with alternating worker ids so execution and catchup exercise
/// per-worker paths.
fn create_signed_multi_worker_certificates_for_rounds(
    range: std::ops::RangeInclusive<u32>,
    fixture: &CommitteeFixture<MemDatabase>,
    worker_count: u16,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>, HashMap<B256, Batch>) {
    assert!(worker_count > 0, "worker_count must be greater than 0");

    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let mut certificates = VecDeque::new();
    let mut next_parents = BTreeSet::new();
    let mut batches = HashMap::new();
    let mut parents: BTreeSet<_> = fixture.genesis().collect();
    let chain = tn_types::test_chain_spec_arc();

    for round in range {
        next_parents.clear();
        for (authority_index, authority_id) in ids.iter().enumerate() {
            let worker_id = ((round as usize + authority_index) % worker_count as usize) as u16;
            let batch = tn_reth::test_utils::batch(chain.clone());
            let batch_digest = batch.digest();
            let header = HeaderBuilder::default()
                .author(authority_id.clone())
                .round(round)
                .epoch(fixture.committee().epoch())
                .parents(parents.clone())
                .created_at(now())
                .with_payload_batch(batch.clone(), worker_id)
                .build();
            let cert = fixture.certificate(&header);
            next_parents.insert(cert.digest());
            batches.insert(batch_digest, batch);
            certificates.push_back(cert);
        }
        parents.clone_from(&next_parents);
    }

    (certificates, next_parents, batches)
}

#[tokio::test]
async fn test_catchup_accumulator_multi_worker_roundtrip() -> eyre::Result<()> {
    let tmp = temp_dir();
    let worker_count = NonZeroUsize::new(2).expect("non-zero worker count");
    // create deterministic committee fixture and use first authority's components
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .number_of_workers(worker_count)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let consensus_bus = ConsensusBus::new();

    let max_round = 21;
    let (certificates, _next_parents, batches) = create_signed_multi_worker_certificates_for_rounds(
        1..=max_round,
        &fixture,
        worker_count.get() as u16,
    );

    // fund accounts in genesis so txs execute
    let genesis = adiri_genesis();
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution env
    let gas_accumulator = GasAccumulator::new(worker_count.get());
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;

    // manually create engine
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let max = Some(max_round as u64 - 1); // consensus needs 1 extra round to commit
    let parent = chain.sealed_genesis_header();

    // start engine
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        max,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
    });

    // subscribe to output early
    let mut consensus_output = consensus_bus.subscribe_consensus_output();

    // spawn consensus to send output to engine for full execution
    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        consensus_store.clone(),
        &task_manager,
    );

    // send certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // simulate epoch manager's role:
    // forward consensus output to engine until `max_round`
    let mut rewards = HashMap::new();
    loop {
        tokio::select! {
            // forward output from consensus to engine
            Some(output) = consensus_output.recv() => {
                let leader = output.leader().origin().clone();
                rewards.entry(leader).and_modify(|count| *count += 1).or_insert(1);
                to_engine.send(output).await?;
            }
            // wait for engine to reach `max_round` or timeout
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                // engine shutdown
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    // initialize a new gas accumulator to simulate node recovery
    let recovered = GasAccumulator::new(worker_count.get());
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(&consensus_store, reth_env.clone(), &recovered)?;
    assert_matching_worker_gas_values(&gas_accumulator, &recovered);
    let saw_worker_one_payload =
        consensus_store.reverse_iter::<ConsensusBlocks>().any(|(_, header)| {
            header.sub_dag.certificates.iter().any(|certificate| {
                certificate.header().payload().values().any(|worker_id| *worker_id == 1)
            })
        });
    assert!(saw_worker_one_payload, "expected committed certificates to include worker 1 payloads");

    // convert manually calculated rewards for assertion
    let expected: BTreeMap<_, _> = rewards
        .iter()
        .map(|(auth, count)| {
            (fixture.authority_by_id(auth).expect("in committee").execution_address(), *count)
        })
        .collect();

    // assert rewards
    assert_eq!(expected, gas_accumulator.rewards_counter().get_address_counts());
    assert_eq!(expected, recovered.rewards_counter().get_address_counts());

    Ok(())
}

/// Test that rewards tracking handles a mix of empty and non-empty consensus outputs.
///
/// With skip-empty-execution, rounds with no batches and no epoch close skip EVM execution.
/// This test verifies that `catchup_accumulator` still restores leader counts and gas totals
/// consistently when empty outputs are present in the committed consensus sequence.
#[tokio::test]
async fn test_catchup_accumulator_with_empty_outputs() -> eyre::Result<()> {
    let tmp = temp_dir();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let consensus_bus = ConsensusBus::new();

    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    let genesis = adiri_genesis();
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let gas_accumulator = GasAccumulator::new(1);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        None,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
    });

    let mut consensus_output = consensus_bus.subscribe_consensus_output();

    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        consensus_store.clone(),
        &task_manager,
    );

    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // Collect committed outputs from consensus first.
    let mut real_outputs = Vec::new();
    let target_round = max_round as u64 - 1;
    loop {
        tokio::select! {
            Some(output) = consensus_output.recv() => {
                let done = output.leader_round() as u64 >= target_round;
                real_outputs.push(output);
                if done { break; }
            }
            _ = timeout(Duration::from_secs(30), &mut rx) => {
                panic!("engine shut down before all outputs were received");
            }
        }
    }

    // Send outputs to engine and inject deterministic empty outputs in between.
    let mut rewards = HashMap::new();
    let mut empty_outputs_seen = 0u32;
    let mut synthetic_number = 100_000u64;
    for (i, output) in real_outputs.into_iter().enumerate() {
        let leader = output.leader().origin().clone();
        rewards.entry(leader.clone()).and_modify(|count| *count += 1).or_insert(1);
        to_engine.send(output.clone()).await?;

        // Inject an empty output periodically using the same leader/round so it is counted
        // by catchup when bounded by last_executed_round.
        if i > 0 && i % 3 == 0 {
            use tn_types::{Certificate, CommittedSubDag, ConsensusOutput, ReputationScores};
            let mut empty_leader = Certificate::default();
            empty_leader.header.round = output.leader().round();
            empty_leader.header.epoch = output.leader().epoch();
            empty_leader.header.created_at = tn_types::now();
            empty_leader.header_mut_for_test().author = leader.clone();

            let empty_subdag = CommittedSubDag::new(
                vec![empty_leader.clone()],
                empty_leader,
                synthetic_number,
                ReputationScores::default(),
                None,
            );
            let empty_output = ConsensusOutput {
                sub_dag: empty_subdag.clone().into(),
                number: synthetic_number,
                parent_hash: output.parent_hash,
                extra: output.extra,
                ..Default::default()
            };
            // Persist the synthetic output in consensus chain storage for catchup.
            consensus_store.write_subdag_for_test(synthetic_number, empty_subdag);
            rewards.entry(leader).and_modify(|count| *count += 1).or_insert(1);
            to_engine.send(empty_output).await?;

            empty_outputs_seen += 1;
            synthetic_number += 1;
        }
    }

    // Close stream so engine drains queue and exits.
    drop(to_engine);
    let engine_result = timeout(Duration::from_secs(30), rx).await??;
    assert!(engine_result.is_err(), "engine should return error when stream closes");
    assert!(empty_outputs_seen > 0, "expected at least one empty consensus output");

    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(&consensus_store, reth_env.clone(), &recovered)?;
    assert_matching_worker_gas_values(&gas_accumulator, &recovered);

    let expected: BTreeMap<_, _> = rewards
        .iter()
        .map(|(auth, count)| {
            (fixture.authority_by_id(auth).expect("in committee").execution_address(), *count)
        })
        .collect();
    assert_eq!(expected, gas_accumulator.rewards_counter().get_address_counts());
    assert_eq!(expected, recovered.rewards_counter().get_address_counts());

    Ok(())
}

/// Test that `catchup_accumulator` only restores rewards for rounds that were executed.
///
/// Rounds committed in consensus after shutdown are restored by replay logic on startup.
#[tokio::test]
async fn test_catchup_accumulator_partial_execution() -> eyre::Result<()> {
    let tmp = temp_dir();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let consensus_bus = ConsensusBus::new();

    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    let genesis = adiri_genesis();
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let gas_accumulator = GasAccumulator::new(1);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let engine_stop_round = 10u64;
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        Some(engine_stop_round),
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        debug!(target: "gas-test", ?res, "partial res:");
        let _ = tx.send(res);
    });

    let mut consensus_output = consensus_bus.subscribe_consensus_output();

    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        consensus_store.clone(),
        &task_manager,
    );

    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    let mut executed_rewards = HashMap::new();
    loop {
        tokio::select! {
            Some(output) = consensus_output.recv() => {
                let leader = output.leader().origin().clone();
                let round = output.leader().round();
                if round <= engine_stop_round as u32 {
                    executed_rewards.entry(leader).and_modify(|c| *c += 1).or_insert(1u32);
                }
                // The engine may have already stopped, ignore send errors in that case.
                let _ = to_engine.send(output).await;
            }
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    // Ensure consensus DB has rounds beyond what execution processed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let has_later_rounds = consensus_store
            .reverse_iter::<ConsensusBlocks>()
            .any(|(_, header)| header.sub_dag.leader_round() > engine_stop_round as u32);
        if has_later_rounds {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for ConsensusBlocks entries beyond round {engine_stop_round}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(&consensus_store, reth_env.clone(), &recovered)?;

    assert_matching_worker_gas_values(&gas_accumulator, &recovered);

    let expected: BTreeMap<_, _> = executed_rewards
        .iter()
        .map(|(auth, count)| {
            (fixture.authority_by_id(auth).expect("in committee").execution_address(), *count)
        })
        .collect();
    assert_eq!(expected, recovered.rewards_counter().get_address_counts());
    assert_eq!(expected, gas_accumulator.rewards_counter().get_address_counts());

    Ok(())
}

/// Helper to spawn consensus components.
fn spawn_consensus(
    fixture: &CommitteeFixture<MemDatabase>,
    consensus_bus: &ConsensusBus,
    batches: HashMap<B256, Batch>,
    config: ConsensusConfig<MemDatabase>,
    consensus_store: MemDatabase,
    task_manager: &TaskManager,
) {
    // components for tasks
    let committee = fixture.committee();
    let rx_shutdown = config.shutdown().subscribe();

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // spawn the executor
    spawn_subscriber(config.clone(), rx_shutdown, consensus_bus.clone(), task_manager, network);

    // Set up mock worker.
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    for worker_id in 0..config.config().num_workers() {
        config.local_network(worker_id).set_primary_to_worker_local_handler(mock_client.clone());
    }

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        3,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    // spawn consensus to await certificates
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.recent_blocks().send_modify(|blocks| {
        blocks.push_latest(0, BlockNumHash::new(0, B256::default()), Some(dummy_parent))
    });
    Consensus::spawn(config, consensus_bus, bullshark, task_manager);
}

fn assert_matching_worker_gas_values(gas_accumulator: &GasAccumulator, recovered: &GasAccumulator) {
    assert_eq!(gas_accumulator.num_workers(), recovered.num_workers());
    for worker_id in 0..gas_accumulator.num_workers() {
        let worker_id = worker_id as u16;
        assert_eq!(
            gas_accumulator.get_values(worker_id),
            recovered.get_values(worker_id),
            "mismatched gas values for worker {worker_id}"
        );
    }
}
