//! Node IT tests

// unused deps lint confusion
#![allow(unused_crate_dependencies)]

use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::{BTreeMap, HashMap},
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
use tn_storage::{mem_db::MemDatabase, CertificateStore};
use tn_test_utils::{
    create_signed_certificates_for_rounds, default_test_execution_node, CommitteeFixture,
};
use tn_types::{
    adiri_genesis, gas_accumulator::GasAccumulator, Batch, ExecHeader, Notifier, SealedHeader,
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
        create_signed_certificates_for_rounds(1..=max_round, &fixture);

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
    let worker_id = 0;
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
    assert_eq!(gas_accumulator.get_values(worker_id), (231, 9702000, 6930000000));
    assert_eq!(gas_accumulator.get_values(worker_id), recovered.get_values(worker_id));

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
/// The engine still increments leader counts for these rounds. However, `catchup_accumulator`
/// only sees leaders from blocks actually written to the DB, so it will have fewer leader
/// counts than the live gas accumulator.
///
/// This test verifies:
/// 1. `gas_accumulator` counts ALL leaders (including skipped rounds)
/// 2. `catchup_accumulator` counts only leaders for rounds that produced blocks
/// 3. Both accumulators track the same gas values (only non-empty rounds produce gas)
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

    // Use the normal test with all batches first to get the outputs,
    // then inject empty outputs between the real ones.
    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture);

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

    // manually create engine - no max_round so it runs until channel closes
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let parent = chain.sealed_genesis_header();

    // start engine
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        None, // no max round - engine runs until channel closes
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
    let mut consensus_output = consensus_bus.consensus_output().subscribe();

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

    // Collect all outputs from consensus, then we'll inject empty ones between them
    let mut real_outputs = Vec::new();
    let mut all_leader_counts: HashMap<_, u32> = HashMap::new();
    // Use max_round - 1 as the stopping condition since consensus needs 1 extra round
    let target_round = max_round as u64 - 1;
    loop {
        tokio::select! {
            Some(output) = consensus_output.recv() => {
                debug!(target: "gas-test", output=?output.leader(), round=output.leader().round(), "received output");
                let leader = output.leader().origin().clone();
                *all_leader_counts.entry(leader).or_insert(0) += 1;
                let done = output.leader_round() as u64 >= target_round;
                real_outputs.push(output);
                if done { break; }
            }
            _ = timeout(Duration::from_secs(30), &mut rx) => {
                panic!("engine shut down before all outputs received");
            }
        }
    }

    // Now send outputs to engine, interleaving empty outputs for some rounds.
    // Create empty outputs that mimic rounds with no batches and no epoch close.
    let mut empty_count = 0u32;
    for (i, output) in real_outputs.into_iter().enumerate() {
        // Before every 3rd real output, inject an empty output using the same leader
        if i > 0 && i % 3 == 0 {
            use tn_types::{Certificate, CommittedSubDag, ConsensusOutput, ReputationScores};
            let mut empty_leader = Certificate::default();
            empty_leader.header.round = 100 + i as u32; // unique round for empty output
            empty_leader.header.created_at = tn_types::now();
            empty_leader.header_mut_for_test().author = output.leader().origin().clone();
            let empty_output = ConsensusOutput {
                sub_dag: CommittedSubDag::new(
                    vec![empty_leader.clone()],
                    empty_leader,
                    100 + i as u64,
                    ReputationScores::default(),
                    None,
                )
                .into(),
                ..Default::default()
            };
            // Save to consensus store so catchup_accumulator can find it
            // (the skip path doesn't produce blocks, so catchup won't see this leader)
            let _ = consensus_store.write(empty_output.sub_dag.leader.clone());
            let _ = consensus_store.write_all(empty_output.sub_dag.certificates.clone());
            to_engine.send(empty_output).await?;
            empty_count += 1;
        }
        to_engine.send(output).await?;
    }

    // Drop the sending channel to shut engine down
    drop(to_engine);

    // Wait for engine to finish
    let engine_result = timeout(Duration::from_secs(30), rx).await??;
    // Engine should shut down with ConsensusOutputStreamClosed
    assert!(engine_result.is_err(), "engine should return error when stream closes");

    assert!(empty_count > 0, "test should have injected at least one empty output");

    // check results
    debug!(target: "gas-test", "gas accumulator:\n{:#?}", gas_accumulator);

    // initialize a new gas accumulator to simulate node recovery
    let worker_id = 0;
    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(&consensus_store, reth_env.clone(), &recovered)?;
    debug!(target: "gas-test", "recovered accumulator:\n{:#?}", recovered);

    // gas values should be the same (only non-empty rounds contribute gas)
    assert_eq!(
        gas_accumulator.get_values(worker_id),
        recovered.get_values(worker_id),
        "gas values should match between live and recovered accumulators"
    );

    // gas_accumulator counts ALL leaders (including empty skipped outputs)
    // catchup_accumulator only counts leaders for rounds that produced blocks
    let gas_leader_counts = gas_accumulator.rewards_counter().get_address_counts();
    let recovered_leader_counts = recovered.rewards_counter().get_address_counts();

    let total_gas: u32 = gas_leader_counts.values().sum();
    let total_recovered: u32 = recovered_leader_counts.values().sum();
    assert!(
        total_gas > total_recovered,
        "gas_accumulator ({total_gas}) should have more leader counts than \
         catchup_accumulator ({total_recovered}) due to {empty_count} empty skipped rounds"
    );

    // The difference should be exactly the number of empty outputs we injected
    assert_eq!(
        total_gas - total_recovered,
        empty_count,
        "difference in leader counts should equal number of injected empty outputs"
    );

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
    config.local_network().set_primary_to_worker_local_handler(mock_client);

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
    consensus_bus
        .recent_blocks()
        .send_modify(|blocks| blocks.push_latest(0, B256::default(), Some(dummy_parent)));
    Consensus::spawn(config, consensus_bus, bullshark, task_manager);
}
