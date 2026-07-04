//! Node IT tests

// unused deps lint confusion
#![allow(unused_crate_dependencies)]

use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tn_config::{ConsensusConfig, WORKER_CONFIGS_ADDRESS};
use tn_engine::ExecutorEngine;
use tn_executor::subscriber::spawn_subscriber;
use tn_network_libp2p::types::{MessageId, NetworkCommand};
use tn_network_types::MockPrimaryToWorkerClient;
use tn_node::{
    catchup_accumulator, derive_base_fees_for_entered_epoch, sync_num_workers_from_chain,
};
use tn_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus,
};
use tn_reth::{
    payload::TNPayload,
    test_utils::{
        create_committee_from_state, seeded_genesis_from_random_batches,
        test_genesis_with_consensus_registry, test_genesis_with_consensus_registry_and_workers,
        TransactionFactory,
    },
    ExecutedBlock, NewCanonicalChain, RethChainSpec, RethEnv,
};
use tn_rpc::{EngineToPrimary, RpcNodeInfo};
use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase};
use tn_test_utils::{
    create_signed_certificates_for_rounds, default_test_execution_node, CommitteeFixture,
};
use tn_types::{
    adiri_genesis,
    gas_accumulator::{compute_next_base_fee_eip1559, GasAccumulator},
    Address, Batch, BlsSignature, Certificate, CommittedSubDag, ConsensusHeader,
    ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch, EpochCertificate, EpochDigest,
    EpochRecord, ExecHeader, GenesisAccount, Notifier, ReputationScores, SealedHeader,
    SignatureVerificationState, TaskManager, TnReceiver as _, TnSender as _, WorkerId, B256,
    DEFAULT_BAD_NODES_STAKE_THRESHOLD, MIN_PROTOCOL_BASE_FEE, U256,
};
use tn_worker::WorkerNetworkHandle;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::debug;

#[tokio::test]
async fn test_catchup_accumulator() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("test_catchup_accumulator").unwrap();
    // create deterministic committee fixture and use first authority's components
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let committee = config.committee().clone();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee).await.unwrap();

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
        &temp_dir.path().join("reth"),
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
        let res = engine.run().await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
        Ok(())
    });

    // subscribe to output early
    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    // spawn consensus to send output to engine for full execution
    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        &task_manager,
        consensus_chain.clone(),
    )
    .await;

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
                let leader = output.leader().author().clone();
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

    // Catchup must restore each worker's base fee from the chain. Capture the chain's
    // finalized base fee, poison the recovered accumulator with a different value, and confirm
    // catchup overwrites it with the value read from the chain (not left at the stale value).
    let expected_base_fee = reth_env
        .finalized_header()?
        .expect("finalized header exists after producing blocks")
        .base_fee_per_gas
        .expect("executed blocks carry a base fee");
    recovered.base_fee(worker_id).set_base_fee(expected_base_fee + 999);

    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain).await?;

    assert_eq!(
        recovered.base_fee(worker_id).base_fee(),
        expected_base_fee,
        "catchup must restore the worker's base fee from the chain, overwriting the stale value",
    );
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

/// No-op [`EngineToPrimary`] for tests that only need worker components initialized.
///
/// These methods back the `tn` RPC namespace, which `initialize_worker_components` registers but
/// never calls during setup (and these tests never issue RPC requests), so the bodies are
/// `unreachable!`.
struct NoopEngineToPrimary;

impl EngineToPrimary for NoopEngineToPrimary {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        unreachable!("EngineToPrimary RPC is not exercised in this test")
    }

    async fn epoch(
        &self,
        _epoch: Option<Epoch>,
        _hash: Option<EpochDigest>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        unreachable!("EngineToPrimary RPC is not exercised in this test")
    }

    fn node_info(&self) -> &RpcNodeInfo {
        unreachable!("EngineToPrimary RPC is not exercised in this test")
    }
}

/// A worker's transaction pool charges the base fee supplied at epoch setup (the
/// accumulator's per-worker value) instead of a hardcoded `MIN_PROTOCOL_BASE_FEE`, and
/// `set_worker_base_fee` updates it for the every-epoch (respawn) path.
#[tokio::test]
async fn test_worker_pool_base_fee_sourced_from_accumulator() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("test_worker_pool_base_fee")?;
    let chain: Arc<RethChainSpec> = Arc::new(adiri_genesis().into());

    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        None,
    )?;

    // keep the task manager alive for the test so the worker RPC + network tasks keep running.
    let task_manager = TaskManager::default();
    let network_handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

    let worker_id: WorkerId = 0;
    // a deliberately non-MIN value: proves the pool doesn't hardcodes MIN_PROTOCOL_BASE_FEE.
    let base_fee = MIN_PROTOCOL_BASE_FEE + 1234;

    execution_node
        .initialize_worker_components(worker_id, network_handle, NoopEngineToPrimary, base_fee)
        .await?;

    let pool = execution_node.get_worker_transaction_pool(&worker_id).await?;
    assert_eq!(
        pool.block_info().pending_basefee,
        base_fee,
        "worker pool base fee should equal the value passed at setup",
    );

    // the every-epoch setter updates the pool (covers the respawn path where init is skipped).
    let new_fee = base_fee + 50;
    execution_node.set_worker_base_fee(worker_id, new_fee).await;
    let pool = execution_node.get_worker_transaction_pool(&worker_id).await?;
    assert_eq!(
        pool.block_info().pending_basefee,
        new_fee,
        "set_worker_base_fee should update the worker pool base fee",
    );

    Ok(())
}

/// Test that rewards tracking handles a mix of empty and non-empty consensus outputs.
///
/// With skip-empty-execution, rounds with no batches and no epoch close skip EVM execution.
/// This test verifies that `catchup_accumulator` still restores leader counts and gas totals
/// consistently when empty outputs are present in the committed consensus sequence.
#[tokio::test]
async fn test_catchup_accumulator_with_empty_outputs() -> eyre::Result<()> {
    let tmp = TempDir::with_prefix("catch_acc_with_out").unwrap();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let mut consensus_chain =
        ConsensusChain::new_for_test(tmp.path().to_owned(), fixture.committee()).await.unwrap();

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
        let res = engine.run().await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
        Ok(())
    });

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        &task_manager,
        consensus_chain.clone(),
    )
    .await;

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

    // Wait for the subscriber to persist all real outputs so synthetic writes are sequential.
    let expected = real_outputs.len() as u64;
    for _ in 0..200 {
        if consensus_chain.latest_consensus_number() >= expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut synthetic_number = consensus_chain.latest_consensus_number() + 1;

    // Send outputs to engine and inject deterministic empty outputs in between.
    let mut rewards = HashMap::new();
    let mut empty_outputs_seen = 0u32;
    for (i, output) in real_outputs.into_iter().enumerate() {
        let leader = output.leader().author().clone();
        rewards.entry(leader.clone()).and_modify(|count| *count += 1).or_insert(1);
        to_engine.send(output.clone()).await?;

        // Inject an empty output periodically using the same leader/round so it is counted
        // by catchup when bounded by last_executed_round.
        if i > 0 && i % 3 == 0 {
            use tn_types::{Certificate, CommittedSubDag, ConsensusOutput, ReputationScores};
            let mut empty_leader = Certificate::default();
            empty_leader.update_header_round_for_test(output.leader().round());
            empty_leader.update_header_epoch_for_test(output.leader().epoch());
            empty_leader.update_header_created_at_for_test(tn_types::now());
            empty_leader.update_header_author_for_test(leader.clone());

            let empty_subdag = CommittedSubDag::new(
                vec![empty_leader.clone()],
                empty_leader,
                synthetic_number,
                ReputationScores::default(),
                None,
            );
            let empty_output = ConsensusOutput::new(
                empty_subdag.clone(),
                output.parent_hash(),
                synthetic_number,
                false,
                VecDeque::new(),
                vec![],
            );
            // Persist the synthetic output in consensus chain storage for catchup.
            consensus_chain.write_subdag_for_test(synthetic_number, empty_subdag).await;
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

    let worker_id = 0;
    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain).await?;
    assert_eq!(gas_accumulator.get_values(worker_id), recovered.get_values(worker_id));

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
    let tmp = TempDir::with_prefix("catch_acc_part_exe").unwrap();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let mut consensus_chain =
        ConsensusChain::new_for_test(tmp.path().to_owned(), fixture.committee()).await.unwrap();

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
        let res = engine.run().await;
        debug!(target: "gas-test", ?res, "partial res:");
        let _ = tx.send(res);
        Ok(())
    });

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        &task_manager,
        consensus_chain.clone(),
    )
    .await;

    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    let mut executed_rewards = HashMap::new();
    loop {
        tokio::select! {
            Some(output) = consensus_output.recv() => {
                let leader = output.leader().author().clone();
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

    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain).await?;

    let worker_id = 0;
    assert_eq!(gas_accumulator.get_values(worker_id), recovered.get_values(worker_id));

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

/// `sync_num_workers_from_chain` sizes the accumulator to the on-chain `WorkerConfigs` count,
/// growing an undersized accumulator and shrinking an oversized one.
#[tokio::test]
async fn test_sync_num_workers_from_chain_adjusts_to_on_chain_count() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("sync_num_workers")?;
    // genesis deploys WorkerConfigs with 2 workers (worker 0 EIP-1559, worker 1 static)
    let genesis = test_genesis_with_consensus_registry_and_workers(
        4,
        vec![(0u8, 30_000_000u64), (1u8, 500u64)],
    );
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let execution_node =
        default_test_execution_node(Some(chain), None, &temp_dir.path().join("reth"), None)?;
    let reth_env = execution_node.get_reth_env().await;
    let epoch_first_block = reth_env.epoch_state_from_canonical_tip()?.epoch_info.blockHeight;

    // the startup default (1 worker) grows to the on-chain count
    let gas_accumulator = GasAccumulator::new(1);
    sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_first_block);
    assert_eq!(gas_accumulator.num_workers(), 2, "undersized accumulator grows to on-chain count");

    // an oversized accumulator shrinks to the on-chain count
    let gas_accumulator = GasAccumulator::new(3);
    sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_first_block);
    assert_eq!(gas_accumulator.num_workers(), 2, "oversized accumulator shrinks to on-chain count");

    Ok(())
}

/// FAIL-OPEN: on a chain without the `WorkerConfigs` contract (older networks), the worker-count
/// sync must keep the accumulator's current size and return without error.
#[tokio::test]
async fn test_sync_num_workers_fail_open_when_contract_absent() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("sync_num_workers_fail_open")?;
    // strip the WorkerConfigs account from the alloc so the contract read is guaranteed to fail
    let mut genesis = test_genesis_with_consensus_registry(4);
    genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let execution_node =
        default_test_execution_node(Some(chain), None, &temp_dir.path().join("reth"), None)?;
    let reth_env = execution_node.get_reth_env().await;
    let epoch_first_block = reth_env.epoch_state_from_canonical_tip()?.epoch_info.blockHeight;

    let gas_accumulator = GasAccumulator::new(1);
    sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_first_block);
    assert_eq!(gas_accumulator.num_workers(), 1, "fail-open keeps the current worker count");

    Ok(())
}

/// Mid-epoch recovery on a chain whose `WorkerConfigs` declares 2 workers, mirroring the startup
/// order: `sync_num_workers_from_chain` sizes the accumulator first, then `catchup_accumulator`
/// restores per-worker state into the correctly sized slots.
///
/// Consensus still only produces worker-0 blocks (worker spawning is a follow-up), so worker 0
/// must match the live accumulator exactly while worker 1 carries an idle default slot.
#[tokio::test]
async fn test_sync_then_catchup_recovers_two_worker_accumulator() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("sync_then_catchup").unwrap();
    // create deterministic committee fixture and use first authority's components
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let committee = config.committee().clone();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee).await.unwrap();

    // make certificates with batches of txs (all worker 0)
    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    // 2-worker WorkerConfigs at genesis; fund the batch senders so txs execute
    let genesis = test_genesis_with_consensus_registry_and_workers(
        4,
        vec![(0u8, 30_000_000u64), (1u8, 500u64)],
    );
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // live accumulator sized 2, as the startup sync would have left it
    let gas_accumulator = GasAccumulator::new(2);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
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
        let res = engine.run().await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
        Ok(())
    });

    // subscribe to output early
    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    // spawn consensus to send output to engine for full execution
    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        &task_manager,
        consensus_chain.clone(),
    )
    .await;

    // send certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // forward consensus output to engine until `max_round`
    loop {
        tokio::select! {
            Some(output) = consensus_output.recv() => {
                to_engine.send(output).await?;
            }
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    // simulate node recovery in the startup order: sync worker count first, then catchup
    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    // poison worker 0's fee to prove the resize preserves the slot and catchup then restores it
    let expected_base_fee = reth_env
        .finalized_header()?
        .expect("finalized header exists after producing blocks")
        .base_fee_per_gas
        .expect("executed blocks carry a base fee");
    recovered.base_fee(0).set_base_fee(expected_base_fee + 999);

    let epoch_first_block = reth_env.epoch_state_from_canonical_tip()?.epoch_info.blockHeight;
    sync_num_workers_from_chain(&reth_env, &recovered, epoch_first_block);
    assert_eq!(recovered.num_workers(), 2, "sync sizes the accumulator before catchup");

    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain).await?;

    // worker 0: totals and fee restored to match the live accumulator
    let (blocks, gas_used, gas_limit) = recovered.get_values(0);
    assert_eq!((blocks, gas_used, gas_limit), gas_accumulator.get_values(0));
    assert!(gas_used > 0, "worker 0 accumulated gas this epoch");
    assert_eq!(recovered.base_fee(0).base_fee(), expected_base_fee);
    // worker 1: idle governance-declared slot stays at defaults
    assert_eq!(recovered.get_values(1), (0, 0, 0));
    assert_eq!(recovered.base_fee(1).base_fee(), MIN_PROTOCOL_BASE_FEE);
    // rewards restored identically
    assert_eq!(
        gas_accumulator.rewards_counter().get_address_counts(),
        recovered.rewards_counter().get_address_counts()
    );

    Ok(())
}

/// Minimal consensus output for driving the payload builder directly (no live consensus).
///
/// Mirrors the shape `tn-reth`'s close-epoch tests use: a default leader certificate with a
/// verified (default) BLS signature so `CommittedSubDag::new` derives deterministic randomness.
fn manual_consensus_output(
    round: u32,
    epoch: Epoch,
    number: u64,
    close_epoch: bool,
) -> ConsensusOutput {
    let mut leader = Certificate::default();
    leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
        BlsSignature::default(),
    ));
    leader.update_header_round_for_test(round);
    leader.update_header_epoch_for_test(epoch);
    leader.update_header_created_at_for_test(tn_types::now());
    let sub_dag = CommittedSubDag::new(
        vec![leader.clone()],
        leader,
        number,
        ReputationScores::default(),
        None,
    );
    ConsensusOutput::new(
        sub_dag,
        ConsensusHeaderDigest::default(),
        number,
        close_epoch,
        VecDeque::new(),
        vec![],
    )
}

/// Build a payload extending `parent` with an explicit worker base fee (mirrors
/// `TNPayload::new_for_test` but lets the test choose the fee the chain carries).
fn payload_with_base_fee(
    parent: SealedHeader,
    output: &ConsensusOutput,
    base_fee_per_gas: u64,
    worker_id: WorkerId,
) -> TNPayload {
    let gas_limit = parent.gas_limit;
    TNPayload::new(
        parent,
        Address::random(),
        0,
        B256::random(),
        output,
        B256::ZERO,
        base_fee_per_gas,
        gas_limit,
        B256::random(),
        worker_id,
    )
}

/// Make an executed block canonical, mirroring `tn_engine`'s payload-builder chain update.
///
/// Finalization is intentionally left to the caller so tests can construct a chain whose
/// finality lags the canonical tip.
fn extend_canonical_chain(reth_env: &RethEnv, block: ExecutedBlock) -> eyre::Result<SealedHeader> {
    let header = block.recovered_block.clone_sealed_header();
    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    canonical_in_memory_state.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
    canonical_in_memory_state.set_canonical_head(header.clone());
    reth_env.finish_executing_output(vec![block], None)?;
    Ok(header)
}

/// F16 regression (`issues/dual-header-read-robustness.md`): catchup derives the block scan
/// range AND the epoch classification from the ONE pinned finalized header, so a canonical tip
/// that advanced past finality across an epoch boundary can no longer produce a silently empty
/// range that drops per-worker base-fee restoration.
///
/// Chain shape: block 1 (epoch 0, worker 0, non-default base fee) is canonical AND finalized;
/// block 2 closes epoch 0 and is canonical but NOT finalized. The old dual-source read took the
/// range start from the canonical tip's epoch state (epoch 1's first block, past the tip) and
/// the range end from the finalized header (block 1), yielding an empty range and silently
/// skipping the restore. The pinned read derives both ends from the finalized header's own epoch
/// state and restores worker 0's fee. The epoch-entry seeding in `run_epoch` derives its range
/// the same pinned way.
#[tokio::test]
async fn test_catchup_restores_fees_when_finality_lags_canonical_tip() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("catchup_pinned_finalized")?;
    // committee fixture only backs the consensus-chain handle catchup takes
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee()).await?;

    // registry-backed genesis (so the epoch-closing system call can run at block 2) with a
    // funded sender so block 1 carries real gas
    let mut tx_factory = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(7));
    let genesis = test_genesis_with_consensus_registry(4).extend_accounts([(
        tx_factory.address(),
        GenesisAccount::default().with_balance(U256::from(1_000_000_000_000_000_000u64)),
    )]);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        None,
    )?;
    let reth_env = execution_node.get_reth_env().await;

    // worker 0's on-chain fee for epoch 0: distinguishable from MIN and from the poison below
    let worker_id: WorkerId = 0;
    let chain_fee = MIN_PROTOCOL_BASE_FEE + 77;

    // block 1: mid-epoch-0 block carrying worker 0's fee and one executed transfer (so gas
    // stats accumulate) - canonical AND finalized. Leader round 0 keeps catchup's leader-count
    // stage (gated on `last_executed_round > 0`) out of scope; this test pins the fee-restore
    // and gas-stat stages.
    let transfer = tx_factory.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(Address::random()),
        U256::from(1),
        Default::default(),
    );
    let output1 = manual_consensus_output(0, 0, 1, false);
    let genesis_header = chain.sealed_genesis_header();
    let payload1 = payload_with_base_fee(genesis_header.clone(), &output1, chain_fee, worker_id);
    let block1 = reth_env.build_block_from_batch_payload(
        payload1,
        &vec![transfer],
        genesis_header.hash(),
        &[],
    )?;
    let block1_header = extend_canonical_chain(&reth_env, block1)?;
    reth_env.finalize_block(block1_header.clone())?;

    // block 2: closes epoch 0 - canonical but NOT finalized (finality lags the canonical tip)
    let no_txs: Vec<Vec<u8>> = vec![];
    let output2 = manual_consensus_output(1, 0, 2, true);
    let payload2 = payload_with_base_fee(block1_header.clone(), &output2, chain_fee, worker_id);
    let block2 =
        reth_env.build_block_from_batch_payload(payload2, &no_txs, block1_header.hash(), &[])?;
    extend_canonical_chain(&reth_env, block2)?;

    // precondition: the OLD code's two sources now genuinely disagree - the canonical tip's
    // epoch state places the epoch start PAST the finalized header, i.e. the old
    // (canonical-tip start ..= finalized end) range is empty
    let finalized = reth_env.finalized_header()?.expect("finalized header");
    assert_eq!(finalized.number, block1_header.number, "finality pinned at block 1");
    assert_eq!(finalized.hash(), block1_header.hash(), "pinned header carries its hash");
    let tip_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(tip_state.epoch, 1, "canonical tip state crossed the epoch boundary");
    assert!(
        tip_state.epoch_info.blockHeight > finalized.number,
        "old dual-source range start ({}) must exceed the finalized range end ({})",
        tip_state.epoch_info.blockHeight,
        finalized.number,
    );

    // recovery: catchup must restore worker 0's fee from the finalized header's own epoch range
    let recovered = GasAccumulator::new(1);
    recovered.base_fee(worker_id).set_base_fee(chain_fee + 999); // poison
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain).await?;

    assert_eq!(
        recovered.base_fee(worker_id).base_fee(),
        chain_fee,
        "catchup must restore the fee from the range pinned to the finalized header, not \
         silently skip on an inconsistent (finalized, canonical-tip) pair",
    );
    // and the pinned range was non-empty: blocks were scanned into the gas stats
    let (blocks_counted, _gas_used, _gas_limit) = recovered.get_values(worker_id);
    assert!(blocks_counted > 0, "pinned range must not be empty");

    Ok(())
}

/// F1 regression, flipped to RECOVERS: after an epoch closes, a node whose finalized tip is the
/// closing block derives the entered epoch's per-worker base fees purely from the closed epoch's
/// chain state. This is the entry state shared by all three `close_epoch(None, ..)` shapes -
/// replay-and-close, crash-after-close, and the live leftover-drain - which previously skipped
/// both the close-time `adjust_base_fees` and the epoch-entry seeding, leaving a fresh
/// accumulator stuck at `MIN_PROTOCOL_BASE_FEE` while the committee agreed on the configured fee.
///
/// Worker 0 is configured `Static { fee: 12_345 }` in the genesis `WorkerConfigs`. A full epoch
/// of batches executes and the last output is flagged as the epoch close (so the closing block
/// runs `concludeEpoch`). A fresh accumulator then follows the production entry order -
/// `sync_num_workers_from_chain`, `derive_base_fees_for_entered_epoch`, `apply` - and must land
/// on 12_345. Also pins gas equivalence (header scan ≡ the live accumulator's `inc_block`
/// totals) and derivation idempotence (the fee is a pure function of the closing block).
#[tokio::test]
async fn test_derive_base_fees_recovers_committee_fee_at_boundary() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("derive_base_fees_boundary").unwrap();
    // create deterministic committee fixture and use first authority's components
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let committee = config.committee().clone();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee).await.unwrap();

    // make certificates with batches of txs (all worker 0)
    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    // worker 0 configured Static { fee: 12_345 } (strategy 1) in the genesis WorkerConfigs;
    // fund the batch senders so txs execute
    let static_fee = 12_345u64;
    let genesis = test_genesis_with_consensus_registry_and_workers(4, vec![(1u8, static_fee)]);
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let gas_accumulator = GasAccumulator::new(1);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;

    // manually create engine
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    // consensus needs 1 extra round to commit; the engine stops after executing this round
    let last_executed_round = max_round as u64 - 1;
    let parent = chain.sealed_genesis_header();

    // start engine
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        Some(last_executed_round),
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.run().await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
        Ok(())
    });

    // subscribe to output early
    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    // spawn consensus to send output to engine for full execution
    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        &task_manager,
        consensus_chain.clone(),
    )
    .await;

    // send certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // forward consensus output to engine, flagging the final output as the epoch close so the
    // closing block runs the boundary system calls (mirrors process_output's boundary flag)
    loop {
        tokio::select! {
            Some(mut output) = consensus_output.recv() => {
                if output.leader_round() as u64 >= last_executed_round {
                    output.set_epoch_close();
                }
                to_engine.send(output).await?;
            }
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    // production preconditions shared by every F1 shape: the pinned tip IS the closed epoch's
    // closing block (nonce still carries epoch 0) while the registry state it holds already
    // reports the entered epoch
    let closing = reth_env.finalized_header()?.expect("closing block finalized");
    assert_eq!(
        RethEnv::extract_epoch_from_header(&closing),
        0,
        "closing block's nonce carries the closed epoch",
    );
    let entered_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(entered_state.epoch, 1, "registry state crossed to the entered epoch");
    assert_eq!(
        entered_state.epoch_info.blockHeight,
        closing.number + 1,
        "entered epoch starts on the block after the closing block",
    );

    // simulate the F1 restart shapes: fresh accumulator recovered in the production entry order
    let recovered = GasAccumulator::new(1);
    sync_num_workers_from_chain(&reth_env, &recovered, entered_state.epoch_info.blockHeight);
    assert_eq!(recovered.num_workers(), 1);
    assert_eq!(
        recovered.base_fee(0).base_fee(),
        MIN_PROTOCOL_BASE_FEE,
        "the F1 failure state: a fresh accumulator holds the MIN default before derivation",
    );

    let derived = derive_base_fees_for_entered_epoch(&reth_env, 1, &closing)?;
    derived.apply(&recovered);

    // the committee-agreed fee is recovered instead of running the epoch at MIN
    assert_eq!(derived.num_workers, 1);
    assert_eq!(derived.fees, vec![Some(static_fee)]);
    assert_eq!(
        recovered.base_fee(0).base_fee(),
        static_fee,
        "entry derivation must recover the governance-set static fee",
    );

    // gas equivalence: the filtered header scan reproduces the live accumulator's inc_block
    // totals for the closed epoch (apply() must NOT have copied them - entered epoch starts
    // at zero gas)
    let (_blocks, live_gas_used, _limit) = gas_accumulator.get_values(0);
    assert!(live_gas_used > 0, "epoch accumulated real gas");
    assert_eq!(
        derived.gas_totals.get(&0).copied().unwrap_or_default(),
        live_gas_used,
        "header scan must equal the live accumulator's inc_block gas total",
    );
    assert_eq!(recovered.get_values(0), (0, 0, 0), "apply must not touch gas counters");

    // idempotence: the fee is a pure function of the closing block's chain state
    let derived_again = derive_base_fees_for_entered_epoch(&reth_env, 1, &closing)?;
    assert_eq!(derived, derived_again, "derivation must be deterministic and idempotent");

    Ok(())
}

/// Eip1559 variant of [`test_derive_base_fees_recovers_committee_fee_at_boundary`]: with worker 0
/// configured `Eip1559 { target_gas }`, the derived entry fee must equal the tn-types oracle
/// `compute_next_base_fee_eip1559(held_fee, gas_total, target)` where `held_fee` is the fee the
/// chain's last genuine block carries and `gas_total` is the live accumulator's epoch total -
/// i.e. exactly the inputs the live producer's close-time `adjust_base_fees` folded.
#[tokio::test]
async fn test_derive_base_fees_eip1559_variant_matches_oracle() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("derive_base_fees_eip1559").unwrap();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let committee = config.committee().clone();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee).await.unwrap();

    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    // worker 0 configured Eip1559 (strategy 0) with a small target the epoch's gas far exceeds,
    // so the derived fee must move instead of staying floored at MIN
    let target_gas = 1_000_000u64;
    let genesis = test_genesis_with_consensus_registry_and_workers(4, vec![(0u8, target_gas)]);
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let gas_accumulator = GasAccumulator::new(1);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let last_executed_round = max_round as u64 - 1;
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        Some(last_executed_round),
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.run().await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
        Ok(())
    });

    let mut consensus_output = consensus_bus.app().subscribe_consensus_output();

    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        &task_manager,
        consensus_chain.clone(),
    )
    .await;

    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    loop {
        tokio::select! {
            Some(mut output) = consensus_output.recv() => {
                if output.leader_round() as u64 >= last_executed_round {
                    output.set_epoch_close();
                }
                to_engine.send(output).await?;
            }
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    let closing = reth_env.finalized_header()?.expect("closing block finalized");
    assert_eq!(RethEnv::extract_epoch_from_header(&closing), 0);
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1);

    let derived = derive_base_fees_for_entered_epoch(&reth_env, 1, &closing)?;

    // scan ≡ inc_block, then the fold must match the tn-types oracle for the same inputs
    let (_blocks, live_gas_used, _limit) = gas_accumulator.get_values(0);
    assert!(live_gas_used > 0, "epoch accumulated real gas");
    assert_eq!(derived.gas_totals.get(&0).copied().unwrap_or_default(), live_gas_used);

    // the fee held during the closed epoch is the one its last genuine block carries (the
    // closing block was built from real batches, so it is that block)
    let held_fee = closing.base_fee_per_gas.expect("executed blocks carry a base fee");
    let expected = compute_next_base_fee_eip1559(held_fee, live_gas_used, target_gas);
    assert_eq!(derived.fees, vec![Some(expected)]);
    // gas far above target: the derived fee must have actually moved off the held value
    assert!(expected > held_fee, "gas above target must raise the fee");

    let recovered = GasAccumulator::new(1);
    derived.apply(&recovered);
    assert_eq!(recovered.base_fee(0).base_fee(), expected);

    Ok(())
}

/// The synthetic empty-close block must be excluded from fee/gas attribution when deriving
/// entered-epoch base fees.
///
/// An epoch that closes with NO batches makes the engine build a synthetic block that is stamped
/// worker 0 and copies its PARENT's base fee (`batch_digest = B256::ZERO`, carried in
/// `ommers_hash`). Without the genuine-block filter, `derive_base_fees_for_entered_epoch` would
/// treat that block as worker 0's latest and fold a fee for it (`Some(..)`); with the filter the
/// scanned range holds no genuine block, so worker 0's slot is `None`, gas totals are empty, and
/// `apply` leaves the container untouched.
#[tokio::test]
async fn test_derive_base_fees_excludes_synthetic_close_block() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("derive_excludes_synthetic").unwrap();
    // registry + WorkerConfigs genesis: worker 0 Static { fee: 12_345 }
    let genesis = test_genesis_with_consensus_registry_and_workers(4, vec![(1u8, 12_345u64)]);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let gas_accumulator = GasAccumulator::new(1);
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )?;
    // the empty-close path resolves the leader's execution address through the rewards
    // counter's committee, so build the committee from on-chain registry state
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let leader_id = committee.authorities().first().expect("first authority").id();
    gas_accumulator.rewards_counter().set_committee(committee);

    // consensus output with NO batches and close_epoch: true -> the engine executes the single
    // synthetic block to close the epoch
    let mut leader = Certificate::default();
    leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
        BlsSignature::default(),
    ));
    leader.update_header_round_for_test(0);
    leader.update_header_epoch_for_test(0);
    leader.update_header_created_at_for_test(tn_types::now());
    leader.update_header_author_for_test(leader_id);
    let sub_dag =
        CommittedSubDag::new(vec![leader.clone()], leader, 0, ReputationScores::default(), None);
    let output = ConsensusOutput::new(
        sub_dag,
        ConsensusHeaderDigest::default(),
        0,
        true, // close_epoch
        VecDeque::new(),
        vec![],
    );

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let reth_env = execution_node.get_reth_env().await;
    let parent = chain.sealed_genesis_header();
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        None,
        from_consensus,
        parent.clone(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        engine_update_tx,
    );

    to_engine.send(output).await?;
    // drop the sending channel so the engine drains and exits
    drop(to_engine);

    let (tx, rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.run().await;
        let _ = tx.send(res);
        Ok(())
    });
    let engine_result = timeout(Duration::from_secs(30), rx).await??;
    assert!(engine_result.is_err(), "engine should return error when stream closes");

    // the closing block is the synthetic empty-close shape: worker-0 stamped, zero batch
    // digest in ommers_hash, parent's base fee copied, zero user gas
    let closing = reth_env.finalized_header()?.expect("closing block finalized");
    assert_eq!(closing.number, 1);
    assert_eq!(RethEnv::extract_epoch_from_header(&closing), 0);
    assert_eq!(closing.ommers_hash, B256::ZERO, "synthetic block carries a zero batch digest");
    assert_eq!(
        closing.base_fee_per_gas, parent.base_fee_per_gas,
        "synthetic block copies its parent's base fee - the attribution poison",
    );
    assert_eq!(closing.gas_used, 0, "system calls never count toward gas_used");
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1, "epoch closed");

    let derived = derive_base_fees_for_entered_epoch(&reth_env, 1, &closing)?;

    // without the is_worker_batch_block filter this would be vec![Some(..)]: the synthetic
    // block's parent-copied fee attributed to worker 0 and folded through its config
    assert_eq!(derived.num_workers, 1);
    assert_eq!(derived.fees, vec![None], "synthetic close block must not attribute a fee");
    assert!(derived.gas_totals.is_empty(), "no genuine blocks -> no gas attribution");

    // applying a None slot must leave the worker's container untouched
    let recovered = GasAccumulator::new(1);
    recovered.base_fee(0).set_base_fee(4242);
    derived.apply(&recovered);
    assert_eq!(recovered.base_fee(0).base_fee(), 4242, "None slot must not touch the container");

    Ok(())
}

/// Helper to spawn consensus components.
async fn spawn_consensus(
    fixture: &CommitteeFixture<MemDatabase>,
    consensus_bus: &ConsensusBus,
    batches: HashMap<B256, Batch>,
    config: ConsensusConfig<MemDatabase>,
    task_manager: &TaskManager,
    mut consensus_chain: ConsensusChain,
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
    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        task_manager,
        network,
        consensus_chain.clone(),
        u64::max_value(),
    );

    // Set up mock worker.
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await
    .unwrap();
    let bullshark = Bullshark::new(
        committee.clone(),
        3,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    // spawn consensus to await certificates
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    consensus_bus.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    Consensus::spawn(config, consensus_bus, bullshark, task_manager, &consensus_chain, None)
        .await
        .unwrap();
}
