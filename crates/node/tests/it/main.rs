//! Node IT tests

// unused deps lint confusion
#![allow(unused_crate_dependencies)]

use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
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
    build_epoch_record, catchup_accumulator, derive_base_fees_for_entered_epoch,
    derive_idle_worker_fee, sync_num_workers_from_chain,
};
use tn_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus,
};
use tn_reth::{
    payload::TNPayload,
    test_utils::{
        create_committee_from_state, execute_payload_and_update_canonical_chain,
        governance_burn_tx, governance_owner_factory, plant_finalized_marker,
        seeded_genesis_from_random_batches, test_genesis_with_consensus_registry,
        test_genesis_with_consensus_registry_and_workers, TransactionFactory,
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
    Address, Batch, BlockNumHash, BlsPublicKey, BlsSignature, Certificate, CommittedSubDag,
    ConsensusHeader, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch,
    EpochCertificate, EpochDigest, EpochRecord, ExecHeader, GenesisAccount, Notifier,
    ReputationScores, SealedHeader, SignatureVerificationState, TaskManager, TnReceiver as _,
    TnSender as _, WorkerId, B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD, MIN_PROTOCOL_BASE_FEE, U256,
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

    let canonical_epoch = reth_env.epoch_state_from_canonical_tip()?.epoch;
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
        .await?;

    // Base fees are owned by the epoch entry seeding, not catchup: the container keeps its MIN
    // default (which IS the committee value for epoch 0 - fees only move at the first close).
    assert_eq!(
        recovered.base_fee(worker_id).base_fee(),
        MIN_PROTOCOL_BASE_FEE,
        "catchup rebuilds gas stats and leader counts only - fees keep the epoch-0 MIN default",
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

    // Simulate a pre-fix database: rewind the persisted finalized marker to a mid-chain header
    // and seed the watches to match, as if this node restarted on a database written by a
    // version that committed blocks and the marker in separate transactions and died between
    // them. Current versions commit both atomically, so only a pre-fix database presents this
    // lag.
    let tip_header = reth_env.finalized_header()?.expect("live run finalized the tip");
    let rewind_to =
        reth_env.sealed_header_by_number(tip_header.number / 2)?.expect("mid-chain header exists");
    plant_finalized_marker(&reth_env, rewind_to.clone())?;
    let stale = reth_env.finalized_header()?.expect("rewound finalized header");
    assert_eq!(stale.number, rewind_to.number, "precondition: the marker lags the tip");

    // startup's heal advances the marker back to the persisted tip...
    reth_env.heal_finalized_to_persisted_tip()?;
    let healed = reth_env.finalized_header()?.expect("healed finalized header");
    assert_eq!(healed.number, tip_header.number, "heal restores the marker to the tip");
    assert_eq!(healed.hash(), tip_header.hash(), "healed marker carries the tip's hash");

    // ...so a fresh catchup rebuilds the gap rounds' gas AND leader counts in full: identical
    // to the live accumulator, with the leader bound coming from the healed tip's nonce
    let healed_recovery = GasAccumulator::new(1);
    healed_recovery.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(reth_env.clone(), &healed_recovery, &mut consensus_chain, canonical_epoch)
        .await?;
    assert_eq!(gas_accumulator.get_values(worker_id), healed_recovery.get_values(worker_id));
    assert_eq!(expected, healed_recovery.rewards_counter().get_address_counts());

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

    fn node_mode(&self) -> tn_types::NodeMode {
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
    execution_node.set_worker_base_fee(worker_id, new_fee).await?;
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
    let canonical_epoch = reth_env.epoch_state_from_canonical_tip()?.epoch;
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
        .await?;
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
    let canonical_epoch = reth_env.epoch_state_from_canonical_tip()?.epoch;
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
        .await?;

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
    sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_first_block)?;
    assert_eq!(gas_accumulator.num_workers(), 2, "undersized accumulator grows to on-chain count");

    // an oversized accumulator shrinks to the on-chain count
    let gas_accumulator = GasAccumulator::new(3);
    sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_first_block)?;
    assert_eq!(gas_accumulator.num_workers(), 2, "oversized accumulator shrinks to on-chain count");

    Ok(())
}

/// FAIL-HARD: on a chain without the `WorkerConfigs` contract, the worker-count sync must
/// error - the caller (startup catchup or the epoch-0 entry arm) cannot proceed on an
/// unverifiable count.
///
/// The accumulator starts at 3 workers (not 1) so the second assertion also discriminates a
/// clean error from a partial resize: an erroring read that still wrote a clamped count would
/// drop 3 to 1 and fail it.
#[tokio::test]
async fn test_sync_num_workers_errors_when_contract_absent() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("sync_num_workers_fail_hard")?;
    // strip the WorkerConfigs account from the alloc so the contract read is guaranteed to fail
    let mut genesis = test_genesis_with_consensus_registry(4);
    genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let execution_node =
        default_test_execution_node(Some(chain), None, &temp_dir.path().join("reth"), None)?;
    let reth_env = execution_node.get_reth_env().await;
    let epoch_first_block = reth_env.epoch_state_from_canonical_tip()?.epoch_info.blockHeight;

    let gas_accumulator = GasAccumulator::new(3);
    let result = sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_first_block);
    assert!(result.is_err(), "a missing WorkerConfigs contract must be a hard error");
    assert_eq!(gas_accumulator.num_workers(), 3, "a failed sync must not resize the accumulator");

    Ok(())
}

/// Recovery on a chain whose `WorkerConfigs` declares 2 workers, staged so one chain pins both
/// entry shapes for the idle worker (consensus still only produces worker-0 blocks; worker
/// spawning is a follow-up):
///
/// 1. MID-EPOCH-0: hold the epoch-closing output back and recover at startup —
///    `catchup_accumulator` sizes the accumulator from pinned chain state and restores per-worker
///    gas stats. Worker 0's totals must match the live accumulator exactly; fees are owned by the
///    entry seeding and hold `MIN_PROTOCOL_BASE_FEE` for BOTH workers, which IS the committee value
///    during epoch 0 (containers hold the MIN default until the FIRST close, pinned via
///    `derive_idle_worker_fee(_, 0, _)`).
/// 2. Close epoch 0 with the held output.
/// 3. RESTART-AFTER-CLOSE (the crash-after-close shape on a 2-worker chain): catchup scans an empty
///    range at a closing-block tip, so post-catchup worker 1 still holds MIN, and the entry
///    derivation must flip it to the governance-set `Static { fee: 500 }` — restarting nodes
///    converge with the fee the live committee computed at the boundary.
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
    // consensus needs 1 extra round to commit; the engine stops after executing this round
    let last_executed_round = max_round as u64 - 1;
    let parent = chain.sealed_genesis_header();

    // start engine
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let (engine_update_tx, mut engine_update_rx) = tokio::sync::mpsc::channel(64);
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

    // PHASE 1 (mid-epoch-0): collect the committed outputs, holding the epoch-closing one back
    // so recovery is exercised against a mid-epoch chain first
    let mut outputs = Vec::new();
    loop {
        let output = timeout(Duration::from_secs(30), consensus_output.recv())
            .await?
            .expect("consensus output");
        let done = output.leader_round() as u64 >= last_executed_round;
        outputs.push(output);
        if done {
            break;
        }
    }
    let mut closing_output = outputs.pop().expect("epoch-closing output");

    // forward every pre-close output, then wait for the engine to execute each (the engine
    // sends exactly one update per consensus output) so every block recovery scans is committed
    let sent = outputs.len();
    for output in outputs {
        to_engine.send(output).await?;
    }
    for _ in 0..sent {
        timeout(Duration::from_secs(30), engine_update_rx.recv())
            .await?
            .expect("engine update per output");
    }

    // finish_executing_output commits the blocks AND the finalized/safe markers in one
    // transaction before it sends the engine update, so the persisted marker cannot lag the
    // persisted tip here. The in-memory finalized watch catchup reads, though, is set by
    // finalize_block moments later in the engine task — settle it deterministically with the
    // same header the engine writes (idempotent) instead of racing that update. A real restart
    // never sees this transient: the watch is seeded from the settled database rows.
    let settled_tip = reth_env
        .sealed_header_by_number(reth_env.last_block_number()?)?
        .expect("persisted tip header exists");
    reth_env.finalize_block(settled_tip)?;

    // simulate node recovery at startup: catchup sizes the accumulator from pinned chain state
    // and rebuilds gas stats (fees are owned by the entry seeding)
    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    let canonical_epoch = reth_env.epoch_state_from_canonical_tip()?.epoch;
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
        .await?;
    assert_eq!(recovered.num_workers(), 2, "catchup sizes the accumulator to the on-chain count");

    // worker 0: totals restored to match the live accumulator
    let (blocks, gas_used, gas_limit) = recovered.get_values(0);
    assert_eq!((blocks, gas_used, gas_limit), gas_accumulator.get_values(0));
    assert!(gas_used > 0, "worker 0 accumulated gas this epoch");
    // BOTH workers hold the MIN default: catchup does not seed fees, and MIN is the committee
    // value during epoch 0 anyway (fees only move at the first close)
    assert_eq!(recovered.base_fee(0).base_fee(), MIN_PROTOCOL_BASE_FEE);
    // worker 1: idle governance-declared slot stays at defaults
    assert_eq!(recovered.get_values(1), (0, 0, 0));
    assert_eq!(recovered.base_fee(1).base_fee(), MIN_PROTOCOL_BASE_FEE);
    // rewards restored identically
    assert_eq!(
        gas_accumulator.rewards_counter().get_address_counts(),
        recovered.rewards_counter().get_address_counts()
    );

    // epoch-0 base case: mid-epoch-0 the walk-back also reports MIN for EVERY worker — the
    // containers hold the MIN default until the FIRST close, so worker 1's Static { fee: 500 }
    // activates entering epoch 1, not during epoch 0 (recovery mirrors live behavior)
    assert_eq!(derive_idle_worker_fee(&reth_env, 0, 0)?, MIN_PROTOCOL_BASE_FEE);
    assert_eq!(derive_idle_worker_fee(&reth_env, 0, 1)?, MIN_PROTOCOL_BASE_FEE);

    // PHASE 2: close epoch 0 with the held-back output (mirrors process_output's boundary flag)
    closing_output.set_epoch_close();
    to_engine.send(closing_output).await?;
    let engine_task = timeout(Duration::from_secs(30), &mut rx).await;
    assert!(engine_task.is_ok(), "engine exits at max round after executing the epoch close");

    // PHASE 3 (restart after close — the crash-after-close shape on a 2-worker chain): the
    // pinned tip IS epoch 0's closing block, so catchup scans an empty range and restores
    // nothing; the entry derivation alone must recover every configured worker's fee.
    let closing = reth_env.finalized_header()?.expect("closing block finalized");
    assert_eq!(
        RethEnv::extract_epoch_from_header(&closing),
        0,
        "closing block's nonce carries the closed epoch",
    );
    let entered_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(entered_state.epoch, 1, "registry state crossed to the entered epoch");

    let restarted = GasAccumulator::new(1);
    restarted.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(reth_env.clone(), &restarted, &mut consensus_chain, entered_state.epoch)
        .await?;
    assert_eq!(restarted.num_workers(), 2, "catchup sizes from the closing block's configs");
    // post-catchup: fees still hold the MIN default — the entry derivation below owns them
    assert_eq!(restarted.base_fee(1).base_fee(), MIN_PROTOCOL_BASE_FEE);

    let derived = derive_base_fees_for_entered_epoch(&reth_env, 1, &closing)?;
    derived.apply(&restarted);

    // worker 1 never produced a block, yet its governance-set Static { fee: 500 } is recovered
    // from the closing block's WorkerConfigs — restarting nodes converge with the fee the live
    // committee computed at the boundary.
    assert_eq!(derived.fees[1], Some(500), "idle worker's fee derives from chain, not MIN");
    assert_eq!(restarted.base_fee(1).base_fee(), 500);
    // the standalone walk-back seam agrees
    assert_eq!(derive_idle_worker_fee(&reth_env, 1, 1)?, 500);

    // worker 0 folds from the fee its last genuine block carries and the epoch's real gas
    let held_fee = closing.base_fee_per_gas.expect("executed blocks carry a base fee");
    let (_blocks, live_gas_used, _limit) = gas_accumulator.get_values(0);
    assert!(live_gas_used > 0, "epoch accumulated real gas");
    assert_eq!(
        derived.gas_totals.get(&0).copied().unwrap_or_default(),
        live_gas_used,
        "header scan must equal the live accumulator's inc_block gas total",
    );
    assert_eq!(
        derived.fees[0],
        Some(compute_next_base_fee_eip1559(held_fee, live_gas_used, 30_000_000)),
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
/// `finish_executing_output` commits the finalized/safe markers atomically with the block, so
/// the persisted marker tracks the tip here; the in-memory watch update (`finalize_block`) is
/// left to the caller. Tests that need a chain whose finality lags the canonical tip plant the
/// lag afterwards via `plant_finalized_marker` (simulating a pre-fix database).
fn extend_canonical_chain(reth_env: &RethEnv, block: ExecutedBlock) -> eyre::Result<SealedHeader> {
    let header = block.recovered_block.clone_sealed_header();
    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    canonical_in_memory_state.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
    canonical_in_memory_state.set_canonical_head(header.clone());
    reth_env.finish_executing_output(vec![block], None)?;
    Ok(header)
}

/// Catchup pins every chain-derived input (scan range, worker-count read block, leader-count
/// bound) to the ONE finalized header, and startup heals that marker to the persisted
/// canonical tip BEFORE catchup runs (`RethEnv::heal_finalized_to_persisted_tip`). A lagging
/// marker is the artifact of pre-fix versions that committed blocks and the marker in
/// separate transactions — current versions commit both atomically, so each phase plants the
/// lag directly (`plant_finalized_marker`) to model such a database. Healing is sound because
/// every persisted canonical block is consensus-final by construction.
///
/// Phase A — mid-epoch lag (block 1 finalized; block 2 canonical-only; both carry a real
/// transfer): catchup ALONE still pins its scan to the stale marker and undercounts (control),
/// while heal-then-catchup counts BOTH blocks' gas — the startup sequence recovers the gap.
///
/// Phase B — boundary-crossing lag (block 3 closes epoch 0, canonical-only): catchup ALONE
/// refuses with the cross-view guard error (control — post-heal, a firing guard means genuine
/// database inconsistency, so it stays a tripwire), while heal-then-catchup for the entered
/// epoch 1 succeeds where startup previously hard-halted: the marker advances to the closing
/// block, epoch 1's first block (4) lies past the healed tip (3), and the empty scan counts
/// zero gas.
///
/// Leader counting stays out of scope here: Phase A's healed pin (round 1, epoch 0) walks this
/// test's EMPTY consensus DB (a harmless no-op), and Phase B's pin carries epoch 0's nonce
/// while the entered epoch is 1, so the epoch gate skips it. `test_catchup_accumulator` covers
/// healed leader recounts against real consensus data.
#[tokio::test]
async fn catchup_heals_finality_lag_across_epoch_boundary() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("catchup_heals_finality_lag")?;
    // committee fixture only backs the consensus-chain handle catchup takes
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee()).await?;

    // registry-backed genesis (so the epoch-closing system call can run at block 3) with a
    // funded sender so blocks 1 and 2 carry real gas
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

    // worker 0's on-chain fee for epoch 0: distinguishable from MIN
    let worker_id: WorkerId = 0;
    let chain_fee = MIN_PROTOCOL_BASE_FEE + 77;

    // block 1: mid-epoch-0 block carrying worker 0's fee and one executed transfer (so gas
    // stats accumulate) - canonical AND finalized
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
    assert!(block1_header.gas_used > 0, "block 1's transfer must land for sharp gas assertions");

    // block 2 (epoch 0): canonical but NOT finalized - the pre-fix crash-window mid-epoch lag
    // - with a second executed transfer so the healed recount is measurably larger than the
    // control
    let transfer2 = tx_factory.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(Address::random()),
        U256::from(1),
        Default::default(),
    );
    let output2 = manual_consensus_output(1, 0, 2, false);
    let payload2 = payload_with_base_fee(block1_header.clone(), &output2, chain_fee, worker_id);
    let block2 = reth_env.build_block_from_batch_payload(
        payload2,
        &vec![transfer2],
        block1_header.hash(),
        &[],
    )?;
    let block2_header = extend_canonical_chain(&reth_env, block2)?;
    // extend committed the marker atomically with block 2; rewind it to block 1 (database rows
    // + watches) to model a pre-fix database whose separate marker write was lost in a crash
    plant_finalized_marker(&reth_env, block1_header.clone())?;
    assert!(block2_header.gas_used > 0, "block 2's transfer must land for sharp gas assertions");

    // Phase A control: catchup ALONE pins its scan to the stale marker (block 1), passes the
    // guard (same epoch), and misses block 2's gas - the undercount the startup heal prevents
    let canonical_epoch = reth_env.epoch_state_from_canonical_tip()?.epoch;
    assert_eq!(canonical_epoch, 0, "mid-epoch canonical tip stays in epoch 0");
    let recovered = GasAccumulator::new(1);
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
        .await?;
    let (blocks_counted, gas_used, _gas_limit) = recovered.get_values(worker_id);
    assert!(blocks_counted > 0, "mid-epoch lag alone must not skip the restore");
    assert_eq!(
        gas_used, block1_header.gas_used,
        "catchup alone is bounded by the stale finalized marker and misses block 2",
    );
    // catchup leaves fees to the entry seeding: block 1's non-MIN fee is NOT adopted
    assert_eq!(recovered.base_fee(worker_id).base_fee(), MIN_PROTOCOL_BASE_FEE);

    // Phase A heal: the marker advances to the persisted canonical tip - the heal rewrites the
    // database rows and updates the in-memory watch catchup reads through finalize_block
    reth_env.heal_finalized_to_persisted_tip()?;
    let healed = reth_env.finalized_header()?.expect("healed finalized header");
    assert_eq!(healed.number, block2_header.number, "heal advances the marker to the tip");
    assert_eq!(healed.hash(), block2_header.hash(), "healed marker carries the tip's hash");

    // Phase A: heal-then-catchup (the startup sequence) recovers the gap block's gas
    let recovered = GasAccumulator::new(1);
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
        .await?;
    let (_blocks_counted, gas_used, _gas_limit) = recovered.get_values(worker_id);
    assert_eq!(
        gas_used,
        block1_header.gas_used + block2_header.gas_used,
        "healed catchup counts the previously-unfinalized block 2",
    );

    // block 3: closes epoch 0 - canonical but NOT finalized (the lag now crosses the boundary)
    let no_txs: Vec<Vec<u8>> = vec![];
    let output3 = manual_consensus_output(2, 0, 3, true);
    let payload3 = payload_with_base_fee(block2_header.clone(), &output3, chain_fee, worker_id);
    let block3 =
        reth_env.build_block_from_batch_payload(payload3, &no_txs, block2_header.hash(), &[])?;
    let block3_header = extend_canonical_chain(&reth_env, block3)?;
    // rewind the atomically-committed marker to block 2: the pre-fix lag now crosses the
    // epoch boundary
    plant_finalized_marker(&reth_env, block2_header.clone())?;

    // precondition: the views genuinely disagree at epoch granularity - the canonical tip's
    // epoch state places the epoch start PAST the finalized header (block 2)
    let finalized = reth_env.finalized_header()?.expect("finalized header");
    assert_eq!(finalized.number, block2_header.number, "finality pinned at block 2");
    let tip_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(tip_state.epoch, 1, "canonical tip state crossed the epoch boundary");
    assert_eq!(
        tip_state.epoch_info.blockHeight,
        block3_header.number + 1,
        "epoch 1's first block is the one after the closing block",
    );
    assert!(
        tip_state.epoch_info.blockHeight > finalized.number,
        "entered-epoch range start ({}) must exceed the finalized range end ({})",
        tip_state.epoch_info.blockHeight,
        finalized.number,
    );

    // Phase B control: catchup ALONE refuses the boundary-crossing lag rather than rebuilding
    // against the wrong epoch's state - the guard stays a tripwire (post-heal, its firing
    // means genuine database inconsistency)
    let recovered = GasAccumulator::new(1);
    let err =
        catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, tip_state.epoch)
            .await
            .expect_err("boundary-crossing finality lag must refuse the restore");
    assert!(
        err.to_string().contains("across an epoch boundary"),
        "error must identify the cross-boundary view disagreement: {err}",
    );
    // the guard fires before any write: the accumulator is untouched
    assert_eq!(recovered.num_workers(), 1);
    assert_eq!(recovered.get_values(worker_id), (0, 0, 0));

    // Phase B heal: the marker advances across the epoch boundary to the closing block
    reth_env.heal_finalized_to_persisted_tip()?;
    let healed = reth_env.finalized_header()?.expect("healed finalized header");
    assert_eq!(healed.number, block3_header.number, "heal advances the marker to block 3");
    assert_eq!(healed.hash(), block3_header.hash(), "healed marker carries block 3's hash");

    // Phase B: heal-then-catchup recovers where startup previously hard-halted. The healed pin
    // is epoch 0's closing block, whose epoch state reports the entered epoch 1: the guard
    // passes, the scan (4..=3) is empty, the worker count syncs from chain state, and leader
    // counting is skipped (the pin's nonce carries epoch 0, not 1)
    let recovered = GasAccumulator::new(1);
    catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, tip_state.epoch)
        .await?;
    assert_eq!(recovered.num_workers(), 1, "worker count synced from chain state");
    assert_eq!(
        recovered.get_values(worker_id),
        (0, 0, 0),
        "entered epoch 1 has no executed blocks yet - the healed scan is empty",
    );

    Ok(())
}

/// The heal refuses a node whose finalized marker points PAST the persisted canonical tip: the
/// marker only ever trails or equals the tip (it commits atomically with the blocks; pre-fix
/// versions committed the blocks first), so a marker ahead of it means the execution database
/// lost blocks this node already attested as final.
#[tokio::test]
async fn heal_errors_when_finalized_marker_ahead_of_tip() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("heal_marker_ahead")?;
    let execution_node =
        default_test_execution_node(None, None, &temp_dir.path().join("reth"), None)?;
    let reth_env = execution_node.get_reth_env().await;

    // Plant a marker past the (genesis-only) tip via a direct database write - only a
    // corrupted or pre-fix database can present this state, since the production path commits
    // the marker atomically with the blocks and can never run ahead of them.
    let ahead = SealedHeader::new(ExecHeader { number: 5, ..Default::default() }, B256::random());
    plant_finalized_marker(&reth_env, ahead)?;

    let err = reth_env
        .heal_finalized_to_persisted_tip()
        .expect_err("a marker past the tip means the db lost attested blocks");
    assert!(
        err.to_string().contains("ahead of the persisted canonical tip"),
        "error must name the marker-ahead condition: {err}",
    );

    Ok(())
}

/// On a fresh genesis chain the heal is a no-op: the marker was never written, the tip is
/// genesis, and genesis must STAY unfinalized so
/// `finalized_block_hash_number_for_startup`'s genesis fallback still marks a fresh node.
#[tokio::test]
async fn heal_noop_on_fresh_genesis() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("heal_fresh_genesis")?;
    let execution_node =
        default_test_execution_node(None, None, &temp_dir.path().join("reth"), None)?;
    let reth_env = execution_node.get_reth_env().await;

    reth_env.heal_finalized_to_persisted_tip()?;
    assert!(
        reth_env.finalized_header()?.is_none(),
        "fresh genesis stays unfinalized after the heal",
    );

    Ok(())
}

/// The kill-between-commits gap can no longer open: blocks and the finalized/safe markers
/// commit in ONE database transaction (`RethEnv::finish_executing_output`), so a node that
/// dies immediately after the blocks-commit — before `finalize_block`'s in-memory update ever
/// runs — restarts on a database whose finalized marker ALREADY equals the persisted tip.
/// There is no between-commits state, so the startup heal has nothing to repair and takes its
/// no-op path (no warn-path heal needed).
#[tokio::test]
async fn finalized_marker_commits_atomically_with_blocks() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("atomic_finalized_marker")?;
    let chain: Arc<RethChainSpec> = Arc::new(adiri_genesis().into());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        None,
    )?;
    let reth_env = execution_node.get_reth_env().await;

    // execute one consensus output and commit it WITHOUT calling finalize_block: the exact
    // on-disk state of a node killed right after finish_executing_output's commit
    let genesis_header = chain.sealed_genesis_header();
    let no_txs: Vec<Vec<u8>> = vec![];
    let output = manual_consensus_output(0, 0, 1, false);
    let payload = payload_with_base_fee(genesis_header.clone(), &output, MIN_PROTOCOL_BASE_FEE, 0);
    let block =
        reth_env.build_block_from_batch_payload(payload, &no_txs, genesis_header.hash(), &[])?;
    let block_header = extend_canonical_chain(&reth_env, block)?;

    // fresh read-only provider reads: the persisted marker already equals the persisted tip
    assert_eq!(reth_env.last_block_number()?, block_header.number);
    assert_eq!(
        reth_env.last_finalized_block_number()?,
        block_header.number,
        "the finalized marker commits atomically with the blocks",
    );

    // the heal has nothing to repair: it takes the no-op path, which never touches the
    // in-memory watches (still unset because finalize_block never ran)
    reth_env.heal_finalized_to_persisted_tip()?;
    assert!(
        reth_env.finalized_header()?.is_none(),
        "a no-op heal leaves the watches untouched - no warn-path heal was needed",
    );
    assert_eq!(
        reth_env.last_finalized_block_number()?,
        block_header.number,
        "the no-op heal leaves the settled marker in place",
    );

    Ok(())
}

/// A committed block referencing a worker id at or beyond the on-chain `WorkerConfigs` count
/// means the chain and the contract disagree about the worker set. Catchup fails the restore
/// with a descriptive error - naming the scanned range, the offending id, and the on-chain
/// count - instead of writing out-of-range per-worker state or dying on `inc_block`'s panic
/// (which remains the live-path tripwire for the same condition).
///
/// Chain shape (manual blocks like the finality-lag test, worker ids stamped directly): the
/// genesis `WorkerConfigs` declares ONE worker;
/// - block 1 (epoch 0): worker 0, non-MIN fee, one executed transfer (real gas);
/// - block 2 (epoch 0): worker 1, a different non-MIN fee, one executed transfer - the out-of-range
///   datum. Canonical AND finalized.
#[tokio::test]
async fn catchup_errors_on_worker_id_beyond_onchain_count() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("catchup_worker_id_bound")?;
    // committee fixture only backs the consensus-chain handle catchup takes
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee()).await?;

    // ONE-worker WorkerConfigs at genesis, with a funded sender so both blocks carry real gas
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

    // distinct non-MIN fees so the restore provably reads each worker's own block
    let worker0_fee = MIN_PROTOCOL_BASE_FEE + 77;
    let worker1_fee = MIN_PROTOCOL_BASE_FEE + 500;

    // block 1 (epoch 0): worker 0 with one executed transfer. Leader round 0 keeps catchup's
    // leader-count stage (gated on `last_executed_round > 0`) out of scope; this test pins the
    // fee-restore and gas-stat stages.
    let transfer0 = tx_factory.create_eip1559_encoded(
        chain.clone(),
        None,
        1_000,
        Some(Address::random()),
        U256::from(1),
        Default::default(),
    );
    let genesis_header = chain.sealed_genesis_header();
    let output1 = manual_consensus_output(0, 0, 1, false);
    let payload1 = payload_with_base_fee(genesis_header.clone(), &output1, worker0_fee, 0);
    let block1 = reth_env.build_block_from_batch_payload(
        payload1,
        &vec![transfer0],
        genesis_header.hash(),
        &[],
    )?;
    let block1_header = extend_canonical_chain(&reth_env, block1)?;

    // block 2 (epoch 0): worker 1 with one executed transfer - the out-of-range datum.
    // Canonical AND finalized so the pinned scan covers it.
    let transfer1 = tx_factory.create_eip1559_encoded(
        chain.clone(),
        None,
        1_000,
        Some(Address::random()),
        U256::from(1),
        Default::default(),
    );
    let output2 = manual_consensus_output(0, 0, 2, false);
    let payload2 = payload_with_base_fee(block1_header.clone(), &output2, worker1_fee, 1);
    let block2 = reth_env.build_block_from_batch_payload(
        payload2,
        &vec![transfer1],
        block1_header.hash(),
        &[],
    )?;
    let block2_header = extend_canonical_chain(&reth_env, block2)?;
    reth_env.finalize_block(block2_header.clone())?;

    // the scanned range genuinely carries worker-1 data
    assert!(block2_header.gas_used > 0, "worker 1's block must carry real gas");

    // the on-chain count (1) cannot cover the block-observed worker id (1): the restore must
    // fail with the descriptive bound error, not grow the accumulator and not panic
    let canonical_epoch = reth_env.epoch_state_from_canonical_tip()?.epoch;
    let recovered = GasAccumulator::new(1);
    let err =
        catchup_accumulator(reth_env.clone(), &recovered, &mut consensus_chain, canonical_epoch)
            .await
            .expect_err("a worker id at/beyond the on-chain count must fail the restore");
    assert!(
        err.to_string().contains("reference worker id 1"),
        "error must name the offending worker id: {err}",
    );
    assert_eq!(recovered.num_workers(), 1, "the failed restore must not grow the accumulator");
    assert_eq!(recovered.get_values(0), (0, 0, 0), "the bound check fires before gas accrual");
    assert_eq!(
        recovered.base_fee(0).base_fee(),
        MIN_PROTOCOL_BASE_FEE,
        "no per-worker state is written on the error path",
    );

    Ok(())
}

/// Regression, flipped to RECOVERS: after an epoch closes, a node whose finalized tip is the
/// closing block derives the entered epoch's per-worker base fees purely from the closed epoch's
/// chain state. This is the entry state shared by all three `close_epoch(None, ..)` shapes -
/// replay-and-close, crash-after-close, and the live leftover-drain - which previously skipped
/// both the close-time `adjust_base_fees` and the epoch-entry seeding, leaving a fresh
/// accumulator stuck at `MIN_PROTOCOL_BASE_FEE` while the committee agreed on the configured fee.
///
/// Worker 0 is configured `Static { fee: 12_345 }` in the genesis `WorkerConfigs`. A full epoch
/// of batches executes and the last output is flagged as the epoch close (so the closing block
/// runs `concludeEpoch`). A fresh accumulator then follows the production entry order -
/// `derive_base_fees_for_entered_epoch` at the closing block, then `apply` (which also sizes
/// the worker count) - and must land on 12_345. Also pins gas equivalence (header scan ≡ the
/// live accumulator's `inc_block` totals) and derivation idempotence (the fee is a pure
/// function of the closing block).
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

    // production preconditions shared by every close_epoch(None) recovery shape: the pinned
    // tip IS the closed epoch's closing block (nonce still carries epoch 0) while the registry
    // state it holds already reports the entered epoch
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

    // simulate the restart shapes: fresh accumulator recovered in the production entry order
    let recovered = GasAccumulator::new(1);
    assert_eq!(
        recovered.base_fee(0).base_fee(),
        MIN_PROTOCOL_BASE_FEE,
        "the failure state: a fresh accumulator holds the MIN default before derivation",
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

/// Regression: pin the closing-block identity the
/// base-fee config reads depend on. The close-time read (`adjust_base_fees`, at the canonical
/// tip) asserts the identity as its tripwire, and the entry seeding resolves its closing block
/// directly from the entered epoch's `blockHeight - 1`, so both price fees off the true closing
/// block ONLY because `concludeEpoch` stamps the entered epoch's `blockHeight = closing block +
/// 1` (ConsensusRegistry) AND the close system call is pinned to the LAST batch of the boundary
/// output (`ConsensusOutput::close_epoch_for_last_batch`). A future multi-block-boundary or
/// contract change would otherwise break that convention silently.
///
/// This drives a REAL epoch close through the ExecutorEngine/`spawn_consensus` scaffold where the
/// boundary output is MULTI-BLOCK (its committed subdag flattens to more than one batch, so it
/// executes as more than one block), then pins:
/// - `canonical_tip().number + 1 == epoch_info.blockHeight`, with the epoch info read AT the tip
///   via `epoch_state_from_canonical_tip` — exactly the read the close-time tripwire in
///   `adjust_base_fees` performs (`epoch_state_at_header(&canonical_tip())`), so this exercises
///   that tripwire's happy path on a real boundary (the check returns `Ok` here);
/// - the close is pinned to the LAST batch: the boundary output's penultimate block has NOT yet
///   advanced the epoch, so the tip IS the boundary output's last (closing) block. This is why the
///   identity holds even for a multi-block boundary output — the exact fact the identity's safety
///   argument rests on.
#[tokio::test]
async fn epoch_block_height_is_closing_block_plus_one() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("epoch_block_height_identity").unwrap();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_bus = ConsensusBus::new();
    let committee = config.committee().clone();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee).await.unwrap();

    // full epoch of certificates with batches (all worker 0), so the committed boundary subdag
    // flattens to several batches -> a multi-block boundary output
    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture, &[]);

    // worker 0 configured Static { fee } so the close-time config read at the tip returns a real
    // per-worker config (mirrors the derive-boundary fixture)
    let static_fee = 7_777u64;
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

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    // consensus needs 1 extra round to commit; the engine stops after executing this round
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

    // forward outputs, flagging the boundary output as the epoch close (mirrors process_output);
    // stash the FIRST flagged output - the engine stops right after executing it (max_round), so
    // it is THE boundary output whose LAST batch runs concludeEpoch
    let mut boundary_output: Option<ConsensusOutput> = None;
    loop {
        tokio::select! {
            Some(mut output) = consensus_output.recv() => {
                if output.leader_round() as u64 >= last_executed_round {
                    output.set_epoch_close();
                    if boundary_output.is_none() {
                        boundary_output = Some(output.clone());
                    }
                }
                let _ = to_engine.send(output).await;
            }
            engine_task = timeout(Duration::from_secs(30), &mut rx) => {
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    let boundary_output = boundary_output.expect("a boundary output was flagged and executed");

    // the boundary output is MULTI-BLOCK: a non-empty committed subdag flattens to one block per
    // batch, so this pins the identity across a boundary output that produced more than one block
    let boundary_blocks = boundary_output.flatten_batches().len();
    assert!(
        boundary_blocks >= 2,
        "regression requires a multi-block boundary output, got {boundary_blocks} block(s)",
    );

    // the canonical tip is the closing block (last executed); its nonce still carries the closed
    // epoch while the registry state it holds already reports the entered epoch
    let closing = reth_env.canonical_tip();
    let closed_epoch = RethEnv::extract_epoch_from_header(&closing);
    assert_eq!(closed_epoch, 0, "closing block's nonce carries the closed epoch");
    assert_eq!(
        reth_env.finalized_header()?.expect("closing block finalized").number,
        closing.number,
        "canonical tip is the finalized closing block",
    );

    // THE IDENTITY, read AT the tip via `epoch_state_from_canonical_tip` == the close-time
    // tripwire's `epoch_state_at_header(&canonical_tip())`: the entered epoch's on-chain
    // blockHeight is the closing block + 1. This is that tripwire's happy path on a real
    // multi-block boundary - the check returns Ok here.
    let entered_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(
        entered_state.epoch,
        closed_epoch + 1,
        "registry state crossed to the entered epoch",
    );
    assert_eq!(
        closing.number + 1,
        entered_state.epoch_info.blockHeight,
        "canonical tip + 1 must equal the entered epoch's blockHeight",
    );

    // the close is pinned to the LAST batch of the boundary output: because the output is
    // multi-block, its penultimate block (closing.number - 1) is one of ITS batches, and that block
    // must NOT have advanced the epoch yet - concludeEpoch runs only in the last batch (the tip).
    // So the tip IS the boundary output's last block, and the identity holds despite >1 block.
    let penultimate = reth_env
        .sealed_header_by_number(closing.number - 1)?
        .expect("penultimate boundary block exists");
    assert_eq!(
        reth_env.epoch_state_at_header(&penultimate)?.epoch,
        closed_epoch,
        "epoch must not advance until the boundary output's LAST batch (the tip is that block)",
    );

    // the close-time tripwire's second read (worker configs at the SAME tip) also resolves: the one
    // configured worker is present at the closing block
    let (num_workers, configs) = reth_env.get_worker_fee_configs_at_block(closing.hash())?;
    assert_eq!(num_workers, 1, "one configured worker at the closing block");
    assert_eq!(configs.len(), 1, "config arity matches the worker count");

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
/// `ommers_hash`). Genesis carries a non-MIN base fee here, so that copied fee is a
/// distinguishable poison: without the genuine-block filter, `derive_base_fees_for_entered_epoch`
/// would treat the synthetic block as worker 0's latest and fold its `Eip1559` config from the
/// poison fee (a non-MIN result); with the filter the scanned range holds no genuine block, gas
/// totals stay empty, and the idle-worker walk-back prices the worker from the epoch-0 MIN base
/// case — `Some(MIN_PROTOCOL_BASE_FEE)`, exactly what the live committee held.
#[tokio::test]
async fn test_derive_base_fees_excludes_synthetic_close_block() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("derive_excludes_synthetic").unwrap();
    // registry + WorkerConfigs genesis: worker 0 Eip1559 { target_gas: 30M } (strategy 0), with
    // a genesis base fee the synthetic block will copy — provably different from the walk-back's
    // epoch-0 MIN base case AND from any fold of it
    let poison_fee = MIN_PROTOCOL_BASE_FEE + 777;
    let mut genesis =
        test_genesis_with_consensus_registry_and_workers(4, vec![(0u8, 30_000_000u64)]);
    genesis.base_fee_per_gas = Some(poison_fee as u128);
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
    // digest in ommers_hash, parent's (non-MIN) base fee copied, zero user gas
    let closing = reth_env.finalized_header()?.expect("closing block finalized");
    assert_eq!(closing.number, 1);
    assert_eq!(RethEnv::extract_epoch_from_header(&closing), 0);
    assert_eq!(closing.ommers_hash, B256::ZERO, "synthetic block carries a zero batch digest");
    assert_eq!(
        closing.base_fee_per_gas,
        Some(poison_fee),
        "synthetic block copies its parent's base fee - the attribution poison",
    );
    assert_eq!(closing.base_fee_per_gas, parent.base_fee_per_gas);
    assert_eq!(closing.gas_used, 0, "system calls never count toward gas_used");
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1, "epoch closed");

    let derived = derive_base_fees_for_entered_epoch(&reth_env, 1, &closing)?;

    // the value a BROKEN filter would produce: the synthetic block's parent-copied fee
    // attributed to worker 0 and folded through its Eip1559 config - provably distinct from
    // the genuine walk-back result (the epoch-0 MIN base case)
    let poisoned_fold = compute_next_base_fee_eip1559(poison_fee, 0, 30_000_000);
    assert_ne!(
        poisoned_fold, MIN_PROTOCOL_BASE_FEE,
        "setup: the poison fold must be distinguishable from the walk-back value",
    );

    // with the filter, the scanned range holds no genuine block, so worker 0's fee comes from
    // the idle-worker walk-back: idle all of epoch 0 -> the epoch-0 base case -> MIN (the
    // committee value; containers hold the MIN default until the first close)
    assert_eq!(derived.num_workers, 1);
    assert_eq!(
        derived.fees,
        vec![Some(MIN_PROTOCOL_BASE_FEE)],
        "synthetic close block must not attribute a fee - the walk-back prices the worker",
    );
    assert!(derived.gas_totals.is_empty(), "no genuine blocks -> no gas attribution");
    // the standalone walk-back seam agrees
    assert_eq!(derive_idle_worker_fee(&reth_env, 1, 0)?, MIN_PROTOCOL_BASE_FEE);

    // the fill guarantees apply() writes EVERY configured worker: a stale container value is
    // overwritten with the derived fee (a None slot would have left 4242 in place)
    let recovered = GasAccumulator::new(1);
    recovered.base_fee(0).set_base_fee(4242);
    derived.apply(&recovered);
    assert_eq!(
        recovered.base_fee(0).base_fee(),
        MIN_PROTOCOL_BASE_FEE,
        "apply must install the derived fee for every configured worker",
    );

    Ok(())
}

/// The walk-back reconstructs an idle worker's `Eip1559` fee across multiple boundaries: it
/// anchors at the fee of the worker's last genuine block (in the last epoch it produced), then
/// decays once per idle boundary — the exact `compute_next_base_fee_eip1559` chain the live
/// committee's per-close `adjust_base_fees` ran.
///
/// Manual two-epoch chain (worker ids and fees stamped directly, like the finality-lag test):
/// - block 1 (epoch 0): worker 1's ONLY block, carrying a non-MIN fee — the anchor;
/// - block 2: closes epoch 0 (worker 0);
/// - block 3 (epoch 1): worker 0 only — worker 1 is idle;
/// - block 4: closes epoch 1 (worker 0).
///
/// Pins both entry states for the idle worker:
/// - mid-epoch-1: the entry derivation (from epoch 0's closing block) prices worker 1 = ONE
///   boundary folded from the block-1 anchor;
/// - entering epoch 2: `derive_idle_worker_fee` and the `derive_base_fees_for_entered_epoch` fill
///   both equal the TWO-boundary oracle chain.
#[tokio::test]
async fn test_derive_idle_worker_fee_eip1559_decays_from_last_produced_epoch() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("derive_idle_eip1559").unwrap();
    // 2-worker WorkerConfigs: worker 0 Eip1559 { 30M }, worker 1 Eip1559 { 1M } (strategy 0)
    let target_gas = 1_000_000u64;
    let genesis = test_genesis_with_consensus_registry_and_workers(
        4,
        vec![(0u8, 30_000_000u64), (0u8, target_gas)],
    );
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        None,
    )?;
    let reth_env = execution_node.get_reth_env().await;

    // worker 1 held this fee during epoch 0: its only genuine block carries it on-chain
    let worker1_fee = 1_000_000u64;
    let no_txs: Vec<Vec<u8>> = vec![];

    // block 1 (epoch 0): worker 1's only block
    let genesis_header = chain.sealed_genesis_header();
    let output1 = manual_consensus_output(0, 0, 1, false);
    let payload1 = payload_with_base_fee(genesis_header.clone(), &output1, worker1_fee, 1);
    let block1 =
        reth_env.build_block_from_batch_payload(payload1, &no_txs, genesis_header.hash(), &[])?;
    let block1_header = extend_canonical_chain(&reth_env, block1)?;

    // block 2: closes epoch 0 (worker 0 at MIN)
    let output2 = manual_consensus_output(1, 0, 2, true);
    let payload2 = payload_with_base_fee(block1_header.clone(), &output2, MIN_PROTOCOL_BASE_FEE, 0);
    let block2 =
        reth_env.build_block_from_batch_payload(payload2, &no_txs, block1_header.hash(), &[])?;
    let block2_header = extend_canonical_chain(&reth_env, block2)?;
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1, "epoch 0 closed");

    // block 3 (epoch 1): worker 0 only - worker 1 goes idle
    let output3 = manual_consensus_output(0, 1, 3, false);
    let payload3 = payload_with_base_fee(block2_header.clone(), &output3, MIN_PROTOCOL_BASE_FEE, 0);
    let block3 =
        reth_env.build_block_from_batch_payload(payload3, &no_txs, block2_header.hash(), &[])?;
    let block3_header = extend_canonical_chain(&reth_env, block3)?;
    reth_env.finalize_block(block3_header.clone())?;

    // one boundary folded from the block-1 anchor (its block carried no user gas)
    let fee_entering_1 = compute_next_base_fee_eip1559(worker1_fee, 0, target_gas);
    assert!(
        fee_entering_1 > MIN_PROTOCOL_BASE_FEE,
        "decay from a non-MIN anchor must stay non-MIN for the oracle to be meaningful",
    );

    // mid-epoch-1 restart: the entry derivation prices EVERY worker from epoch 0's closing
    // block (block 2). Worker 1 has no epoch-0 gas, so its fee is the anchor folded once;
    // worker 0's fee is an explicit write of the same MIN its epoch-0 blocks carry (zero gas
    // folds back to MIN under its 30M-target Eip1559 config).
    let recovered = GasAccumulator::new(1);
    let derived_mid = derive_base_fees_for_entered_epoch(&reth_env, 1, &block2_header)?;
    derived_mid.apply(&recovered);
    assert_eq!(recovered.num_workers(), 2, "apply resizes to the on-chain worker count");
    assert_eq!(
        recovered.base_fee(1).base_fee(),
        fee_entering_1,
        "entry derivation prices the idle worker from its last produced epoch",
    );
    assert_eq!(
        recovered.base_fee(0).base_fee(),
        MIN_PROTOCOL_BASE_FEE,
        "worker 0's zero-gas epoch folds back to MIN - written explicitly by apply",
    );
    // the standalone seam agrees mid-epoch
    assert_eq!(derive_idle_worker_fee(&reth_env, 1, 1)?, fee_entering_1);

    // block 4: closes epoch 1 (worker 0 at MIN)
    let output4 = manual_consensus_output(1, 1, 4, true);
    let payload4 = payload_with_base_fee(block3_header.clone(), &output4, MIN_PROTOCOL_BASE_FEE, 0);
    let block4 =
        reth_env.build_block_from_batch_payload(payload4, &no_txs, block3_header.hash(), &[])?;
    let block4_header = extend_canonical_chain(&reth_env, block4)?;
    reth_env.finalize_block(block4_header.clone())?;
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 2, "epoch 1 closed");

    // entering epoch 2: TWO boundaries folded from the anchor - the oracle decay chain
    let fee_entering_2 = compute_next_base_fee_eip1559(fee_entering_1, 0, target_gas);
    assert_ne!(fee_entering_2, fee_entering_1, "the second boundary must decay again");
    assert_eq!(
        derive_idle_worker_fee(&reth_env, 2, 1)?,
        fee_entering_2,
        "walk-back must equal the per-boundary eip1559 oracle chain",
    );

    // and the boundary-entry fill installs the same value for the idle worker
    let derived = derive_base_fees_for_entered_epoch(&reth_env, 2, &block4_header)?;
    assert_eq!(derived.num_workers, 2);
    assert_eq!(derived.fees[1], Some(fee_entering_2), "entry fill covers the idle worker");
    // worker 0 produced in epoch 1 (blocks 3 and 4 at MIN, zero gas): folds to MIN
    assert_eq!(derived.fees[0], Some(MIN_PROTOCOL_BASE_FEE));

    Ok(())
}

/// One epoch-entry pass over the accumulator exactly as `run_epoch` performs it: the atomic
/// `epoch_state_at_epoch_start` read yields the entered epoch's state together with the pin
/// header — the previous epoch's closing block (`blockHeight - 1`, written once at the
/// boundary) — and the worker count and every worker's base fee derive+apply from that pinned
/// state. Epoch 0 has no prior epoch: the count syncs from genesis `WorkerConfigs` state and
/// fees keep the MIN defaults.
fn run_epoch_entry_sequence(
    reth_env: &RethEnv,
    gas_accumulator: &GasAccumulator,
) -> eyre::Result<()> {
    // run_epoch's entry read is pinned to the previous epoch's closing block; the pin resolves
    // from boundary-written-once scalars, so any mid-epoch tip yields the identical header
    let (entered_state, epoch_start_header) = reth_env.epoch_state_at_epoch_start()?;
    if entered_state.epoch == 0 {
        sync_num_workers_from_chain(
            reth_env,
            gas_accumulator,
            entered_state.epoch_info.blockHeight,
        )?;
    } else {
        derive_base_fees_for_entered_epoch(reth_env, entered_state.epoch, &epoch_start_header)?
            .apply(gas_accumulator);
    }
    Ok(())
}

/// A ModeChange re-entry re-runs the epoch-entry sequence mid-epoch while the engine may still
/// be executing leftover consensus output (`send_leftover_consensus_output_to_engine` forwards
/// it WITHOUT waiting for execution). What makes that safe is NOT quiescence but
/// value-stability: the entered epoch's `blockHeight` is written once at the boundary and the
/// derivation is a pure function of the prior epoch's closing state, so the re-entry
/// re-derives identical values - the resize no-ops and the fee writes rewrite the same values
/// - while in-flight `inc_block` calls (ids < count) keep landing.
///
/// Chain shape (2 static workers so both derive sub-paths carry non-MIN fees):
/// - block 1 closes epoch 0 (worker 0 at MIN - the live epoch-0 fee; statics activate entering
///   epoch 1);
/// - block 2 (epoch 1): worker 0 at its static fee - chain-consistent with the derived value;
/// - block 3 (epoch 1): the "leftover" executed between the entries, advancing the tip.
///
/// The first entry seeds count = 2 and fees [700, 500] (worker 0 folds through its static
/// config from its epoch-0 block; idle worker 1 walks back to its static config). Live gas
/// accumulates, block 3 lands, then the re-entry runs while a hammer thread plays the engine
/// still executing leftovers - `inc_block`/`base_fee` for both workers (worker 1 = count - 1
/// pins the no-shrink-below-in-flight-id bound). Asserts: count unchanged, every fee
/// unchanged, accumulated gas exactly preserved (sequential + concurrent increments), no
/// panic. The thread overlap is best-effort; every assertion is timing-independent.
#[tokio::test]
async fn mode_change_reentry_is_idempotent() -> eyre::Result<()> {
    const WORKER0_FEE: u64 = 700;
    const WORKER1_FEE: u64 = 500;
    const HAMMER_BLOCKS: u64 = 20_000;

    let temp_dir = TempDir::with_prefix("mode_change_reentry").unwrap();
    // 2-worker WorkerConfigs, both Static (strategy 1) so the epoch-1 fees are non-MIN and
    // self-consistent across both derive sub-paths (produced fold and idle walk-back)
    let genesis = test_genesis_with_consensus_registry_and_workers(
        4,
        vec![(1u8, WORKER0_FEE), (1u8, WORKER1_FEE)],
    );
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &temp_dir.path().join("reth"),
        None,
    )?;
    let reth_env = execution_node.get_reth_env().await;
    let no_txs: Vec<Vec<u8>> = vec![];

    // block 1: closes epoch 0 (worker 0 at MIN - containers hold MIN until the first close)
    let genesis_header = chain.sealed_genesis_header();
    let output1 = manual_consensus_output(1, 0, 1, true);
    let payload1 =
        payload_with_base_fee(genesis_header.clone(), &output1, MIN_PROTOCOL_BASE_FEE, 0);
    let block1 =
        reth_env.build_block_from_batch_payload(payload1, &no_txs, genesis_header.hash(), &[])?;
    let block1_header = extend_canonical_chain(&reth_env, block1)?;
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1, "epoch 0 closed");

    // block 2 (epoch 1): worker 0 produces at its now-active static fee; worker 1 stays idle
    let output2 = manual_consensus_output(0, 1, 2, false);
    let payload2 = payload_with_base_fee(block1_header.clone(), &output2, WORKER0_FEE, 0);
    let block2 =
        reth_env.build_block_from_batch_payload(payload2, &no_txs, block1_header.hash(), &[])?;
    let block2_header = extend_canonical_chain(&reth_env, block2)?;
    reth_env.finalize_block(block2_header.clone())?;

    // FIRST entry (mid-epoch-1): derive from epoch 0's closing block, then apply
    let gas_accumulator = GasAccumulator::new(1);
    let block_height_first = reth_env.epoch_state_from_canonical_tip()?.epoch_info.blockHeight;
    run_epoch_entry_sequence(&reth_env, &gas_accumulator)?;
    assert_eq!(gas_accumulator.num_workers(), 2, "count derived from the closing block's configs");
    assert_eq!(
        gas_accumulator.base_fee(0).base_fee(),
        WORKER0_FEE,
        "worker 0's fee folds through its static config from the closing block",
    );
    assert_eq!(
        gas_accumulator.base_fee(1).base_fee(),
        WORKER1_FEE,
        "idle worker 1's fee walks back through prior closing-block state",
    );

    // live execution before the mode change: deterministic totals the re-entry must preserve
    gas_accumulator.inc_block(0, 100_000, 150_000);
    gas_accumulator.inc_block(1, 42_000, 60_000);

    // a leftover output executes between the exit and the re-entry (there is no execution
    // wait): block 3 extends epoch 1 at the SAME fee - mid-epoch fees are constants
    let output3 = manual_consensus_output(1, 1, 3, false);
    let payload3 = payload_with_base_fee(block2_header.clone(), &output3, WORKER0_FEE, 0);
    let block3 =
        reth_env.build_block_from_batch_payload(payload3, &no_txs, block2_header.hash(), &[])?;
    let block3_header = extend_canonical_chain(&reth_env, block3)?;
    reth_env.finalize_block(block3_header)?;

    // the count-read input is value-stable across the advanced tip: epoch info is written once
    // at the boundary
    let block_height_second = reth_env.epoch_state_from_canonical_tip()?.epoch_info.blockHeight;
    assert_eq!(block_height_second, block_height_first, "entry reads pin the same closing block");

    // RE-ENTRY (ModeChange): re-run the entry sequence while a hammer thread drives in-flight
    // execution - inc_block for both workers (worker 1 = count - 1 pins the id bound) plus fee
    // reads that must only ever observe the seeded epoch values
    let hammer_accumulator = gas_accumulator.clone();
    let hammer = std::thread::spawn(move || {
        for _ in 0..HAMMER_BLOCKS {
            hammer_accumulator.inc_block(0, 21_000, 30_000_000);
            hammer_accumulator.inc_block(1, 10_000, 30_000_000);
            assert_eq!(hammer_accumulator.base_fee(0).base_fee(), WORKER0_FEE);
            assert_eq!(hammer_accumulator.base_fee(1).base_fee(), WORKER1_FEE);
        }
    });
    let reentry = run_epoch_entry_sequence(&reth_env, &gas_accumulator);
    hammer.join().expect("in-flight inc_block/base_fee must not panic across the re-entry");
    reentry?;

    // idempotent: the resize is a no-op and every per-worker fee is unchanged
    assert_eq!(gas_accumulator.num_workers(), 2, "re-entry resize must be a no-op");
    assert_eq!(gas_accumulator.base_fee(0).base_fee(), WORKER0_FEE, "re-seed rewrites same value");
    assert_eq!(gas_accumulator.base_fee(1).base_fee(), WORKER1_FEE, "re-fill rewrites same value");

    // the epoch's accumulated gas is exactly preserved: sequential + concurrent increments,
    // nothing cleared or overwritten by the re-entry
    assert_eq!(
        gas_accumulator.get_values(0),
        (1 + HAMMER_BLOCKS, 100_000 + HAMMER_BLOCKS * 21_000, 150_000 + HAMMER_BLOCKS * 30_000_000,),
        "worker 0's live gas totals must survive the re-entry",
    );
    assert_eq!(
        gas_accumulator.get_values(1),
        (1 + HAMMER_BLOCKS, 42_000 + HAMMER_BLOCKS * 10_000, 60_000 + HAMMER_BLOCKS * 30_000_000),
        "worker 1's live gas totals must survive the re-entry",
    );

    Ok(())
}

/// IT-3: the epoch-record chain survives a mid-epoch governance ejection end-to-end.
///
/// A 5-validator registry chain closes epoch 0 normally (rec0), then governance burns a
/// current-committee validator mid-epoch-1 before the epoch-1 close. The producer's inputs
/// are REAL post-ejection chain reads (the same `getCommitteeBlsPubkeys` path the node's
/// `write_epoch_record` uses), so rec1's committee is the shrunken, swap-and-popped 4-member
/// array while rec0's `next_committee` promised 5 — the exact shape that made
/// `build_epoch_record`'s strict comparison halt every node at the boundary before it
/// tolerated compatible shrinks via [`EpochRecord::committee_compatible`]. Epoch 2 then
/// closes with an exact 4-member handoff (rec2), and all three records round-trip the
/// epoch-record db in order.
#[tokio::test]
async fn test_epoch_record_chain_across_mid_epoch_ejection() -> eyre::Result<()> {
    let record_dir = TempDir::with_prefix("epoch_record_ejection_records")?;
    let reth_dir = TempDir::with_prefix("epoch_record_ejection_reth")?;
    // the committee fixture only backs the consensus-chain handle holding the record store
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let consensus_chain =
        ConsensusChain::new_for_test(record_dir.path().to_owned(), fixture.committee()).await?;
    let records = consensus_chain.epochs();

    // 5-validator registry genesis so one ejection leaves a 4-member committee (the sync
    // tolerance floor)
    let genesis = test_genesis_with_consensus_registry(5);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let task_manager = TaskManager::new("epoch record ejection test");
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), reth_dir.path(), &task_manager, None)?;

    // committee reads through the same `getCommitteeBlsPubkeys` path the node performs for
    // `write_epoch_record`. The node pins that read to the epoch-closing block; every read
    // below happens while the canonical tip IS the relevant closing block.
    let keys_for_epoch = |e: u32| -> eyre::Result<Vec<BlsPublicKey>> {
        Ok(reth_env
            .bls_pubkeys_for_epoch_at_block(e, reth_env.canonical_tip().hash())?
            .iter()
            .filter_map(|bls| BlsPublicKey::from_literal_bytes(bls.as_ref()).ok())
            .collect())
    };

    // block 1: close epoch 0 normally
    let worker_id: WorkerId = 0;
    let output1 = manual_consensus_output(1, 0, 1, true);
    let payload1 = payload_with_base_fee(
        chain.sealed_genesis_header(),
        &output1,
        MIN_PROTOCOL_BASE_FEE,
        worker_id,
    );
    let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload1, vec![])?;
    let header1 = block1.recovered_block.clone_sealed_header();
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1);

    // rec0 from real post-close reads: a full 5-member handoff
    let committee0 = keys_for_epoch(0)?;
    assert_eq!(committee0.len(), 5);
    let rec0 = build_epoch_record(
        0,
        committee0,
        keys_for_epoch(1)?,
        None,
        BlockNumHash::new(header1.number, header1.hash()),
        ConsensusNumHash::new(1, ConsensusHeaderDigest::default()),
    )?;
    records.save_record(rec0.clone()).await?;

    // mid-epoch 1: governance burns a seated validator (a middle slot so swap-and-pop
    // visibly reorders the survivors)
    let epoch1_pre_burn = keys_for_epoch(1)?;
    assert_eq!(epoch1_pre_burn, rec0.next_committee, "epoch 1 starts on the promised committee");
    let target_bls = epoch1_pre_burn[1];
    let target_addr = reth_env.epoch_state_from_canonical_tip()?.validators[1].validatorAddress;
    let mut governance = governance_owner_factory();
    let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target_addr);
    let output2 = manual_consensus_output(1, 1, 2, false);
    let payload2 = payload_with_base_fee(header1, &output2, MIN_PROTOCOL_BASE_FEE, worker_id);
    let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload2, vec![burn_tx])?;
    let header2 = block2.recovered_block.clone_sealed_header();

    // block 3: close epoch 1 over the shrunken committee
    let output3 = manual_consensus_output(2, 1, 3, true);
    let payload3 = payload_with_base_fee(header2, &output3, MIN_PROTOCOL_BASE_FEE, worker_id);
    let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload3, vec![])?;
    let header3 = block3.recovered_block.clone_sealed_header();
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 2);

    // rec1 from real post-ejection reads, chained to the db round-tripped rec0: the exact
    // producer shape that halted the network before the tolerance fix
    let committee1 = keys_for_epoch(1)?;
    assert_eq!(committee1.len(), 4, "on-chain committee shrank mid-epoch");
    assert!(!committee1.contains(&target_bls));
    let prev0 = records.record_by_epoch(0).await.expect("rec0 round-trips the record db");
    assert_eq!(prev0, rec0);
    let rec1 = build_epoch_record(
        1,
        committee1.clone(),
        keys_for_epoch(2)?,
        Some(&prev0),
        BlockNumHash::new(header3.number, header3.hash()),
        ConsensusNumHash::new(3, ConsensusHeaderDigest::default()),
    )
    .expect("mid-epoch ejection must not prevent the epoch record from building");
    assert_eq!(rec1.committee, committee1, "the record carries the shrunken on-chain read");
    assert_eq!(rec1.parent_hash, prev0.digest(), "rec1 chains to rec0");

    // the producer built exactly the shape the sync verifier's tolerance accepts
    let promised: BTreeSet<BlsPublicKey> = prev0.next_committee.iter().copied().collect();
    assert!(rec1.committee_compatible(&promised));
    let recorded: BTreeSet<BlsPublicKey> = rec1.committee.iter().copied().collect();
    assert!(recorded.is_subset(&promised));
    let ejected: Vec<_> = promised.difference(&recorded).collect();
    assert_eq!(ejected, vec![&target_bls], "exactly the burned key left the committee");
    records.save_record(rec1.clone()).await?;

    // block 4: close epoch 2 — the chain continues normally after the ejection epoch
    let output4 = manual_consensus_output(1, 2, 4, true);
    let payload4 = payload_with_base_fee(header3, &output4, MIN_PROTOCOL_BASE_FEE, worker_id);
    let block4 = execute_payload_and_update_canonical_chain(&reth_env, payload4, vec![])?;
    let header4 = block4.recovered_block.clone_sealed_header();

    let committee2 = keys_for_epoch(2)?;
    let prev1 = records.record_by_epoch(1).await.expect("rec1 round-trips the record db");
    assert_eq!(prev1, rec1);
    assert_eq!(committee2, prev1.next_committee, "the post-ejection handoff is exact again");
    let rec2 = build_epoch_record(
        2,
        committee2,
        keys_for_epoch(3)?,
        Some(&prev1),
        BlockNumHash::new(header4.number, header4.hash()),
        ConsensusNumHash::new(4, ConsensusHeaderDigest::default()),
    )?;
    assert_eq!(rec2.parent_hash, prev1.digest(), "rec2 chains through the ejection epoch");
    records.save_record(rec2.clone()).await?;

    // the full record chain reads back in order
    for expected in [&rec0, &rec1, &rec2] {
        let stored =
            records.record_by_epoch(expected.epoch).await.expect("saved record is readable");
        assert_eq!(&stored, expected);
    }

    Ok(())
}

/// ENTRY-READ INVARIANT end-to-end: the previous epoch's closing block rules the ENTIRE epoch —
/// re-entry timing cannot change the committee or the rewards rows.
///
/// A governance `burn` swap-and-pops the ejected validator out of the CURRENT epoch's stored
/// committee arrays immediately, so a node re-entering `run_epoch` mid-epoch (crash-restart or
/// ModeChange) that read the canonical tip would seed `RewardsCounter::set_committee` with the
/// shrunken post-burn committee. `get_address_counts` resolves tallied authorities through that
/// committee, so the ejected leader's accumulated reward row silently vanishes and the
/// `generate_withdrawals` set the closing block commits diverges from peers that kept the
/// epoch-start committee — a different `withdrawals_root`, a different closing-block hash, and
/// a different epoch-record digest across the fleet. The entry read is therefore pinned to the
/// previous epoch's closing block (`epoch_state_at_epoch_start`), making every entry shape
/// derive the identical committee.
///
/// Four legs:
/// - A: on-time entry (before the burn) derives the full committee from the pinned read and seeds
///   the counter exactly as `run_epoch` does; every member tallies leader blocks.
/// - B: after a mid-epoch burn, the RE-ENTRY pinned read returns the identical committee (victim
///   included) while the tip read shows the shrunken set; re-seeding from the pinned read preserves
///   the victim's row and the production close consumer (`generate_withdrawals`, the
///   `withdrawals_root` input) still emits all N entries.
/// - C: control proving the machinery detects the pre-fix bug — the same tallies seeded from the
///   post-burn TIP committee drop the victim's row and emit N-1 withdrawals.
/// - D: epoch 0 pins genesis, and the pinned-read committee equals the tip-read committee — entry
///   semantics for the genesis epoch are unchanged.
#[tokio::test]
async fn mid_epoch_burn_reentry_keeps_epoch_start_committee_and_rewards() -> eyre::Result<()> {
    const VICTIM_LEADER_BLOCKS: u32 = 7;

    let reth_dir = TempDir::with_prefix("burn_reentry_reth")?;
    // 5-validator registry genesis: one ejection leaves 4 members, so pinned (5) and tip (4)
    // committee sizes are distinguishable
    let genesis = test_genesis_with_consensus_registry(5);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let task_manager = TaskManager::new("burn reentry test");
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), reth_dir.path(), &task_manager, None)?;

    // block 1: close epoch 0 — this closing block seats epoch 1's committee and is the pin
    // every epoch-1 entry read must derive from
    let worker_id: WorkerId = 0;
    let output1 = manual_consensus_output(1, 0, 1, true);
    let payload1 = payload_with_base_fee(
        chain.sealed_genesis_header(),
        &output1,
        MIN_PROTOCOL_BASE_FEE,
        worker_id,
    );
    let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload1, vec![])?;
    let header1 = block1.recovered_block.clone_sealed_header();
    assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1);

    // Leg A — entry BEFORE the burn: the pinned read production performs at every entry
    let (state_a, pin_a) = reth_env.epoch_state_at_epoch_start()?;
    assert_eq!(state_a.epoch, 1);
    assert_eq!(pin_a.hash(), header1.hash(), "pin is epoch 0's closing block");
    // the soon-to-be-burned victim: a middle slot so swap-and-pop visibly reorders survivors
    let victim_addr = state_a.validators[1].validatorAddress;
    let victim_bls = BlsPublicKey::from_literal_bytes(state_a.bls_pubkeys[1].as_ref())
        .map_err(|err| eyre::eyre!("failed to decode victim bls key: {err:?}"))?;
    let committee_a = create_committee_from_state(state_a).await?;
    assert_eq!(committee_a.epoch(), 1);
    assert_eq!(committee_a.size(), 5, "on-time entry derives the full epoch-start committee");

    // seed the counter exactly as run_epoch does at entry, then tally leader blocks for every
    // member — the victim's count is distinctive so its row is unmistakable
    let gas_accumulator = GasAccumulator::new(1);
    let rewards = gas_accumulator.rewards_counter();
    rewards.set_committee(committee_a.clone());
    for authority in committee_a.authorities() {
        let count =
            if authority.execution_address() == victim_addr { VICTIM_LEADER_BLOCKS } else { 1 };
        for _ in 0..count {
            rewards.inc_leader_count(&authority.id());
        }
    }
    let counts_a = rewards.get_address_counts();
    assert_eq!(counts_a.len(), 5, "every committee member has a reward row");
    assert_eq!(counts_a.get(&victim_addr), Some(&VICTIM_LEADER_BLOCKS));

    // Leg B — mid-epoch burn, then re-entry (the crash-restart / ModeChange shape)
    let mut governance = governance_owner_factory();
    let burn_tx = governance_burn_tx(&mut governance, chain.clone(), victim_addr);
    let output2 = manual_consensus_output(1, 1, 2, false);
    let payload2 = payload_with_base_fee(header1, &output2, MIN_PROTOCOL_BASE_FEE, worker_id);
    execute_payload_and_update_canonical_chain(&reth_env, payload2, vec![burn_tx])?;

    // the tip view shrinks immediately — this is what a pre-fix re-entry would have read
    let tip_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(tip_state.epoch, 1);
    assert_eq!(tip_state.validators.len(), 4, "tip committee shrank post-burn");
    assert!(tip_state.validators.iter().all(|v| v.validatorAddress != victim_addr));

    // RE-ENTRY read: pinned to the same closing block, so the committee is IDENTICAL to leg
    // A's — victim included; the pin is exactly what differs from the tip read above
    let (state_b, pin_b) = reth_env.epoch_state_at_epoch_start()?;
    assert_eq!(pin_b.hash(), pin_a.hash(), "pin unchanged by the burn");
    let committee_b = create_committee_from_state(state_b).await?;
    assert_eq!(
        committee_b.bls_keys(),
        committee_a.bls_keys(),
        "re-entry derives the exact epoch-start committee"
    );
    assert!(committee_b.bls_keys().contains(&victim_bls), "the burned victim is still a member");

    // re-seed from the pinned read as a real re-entry does: every accumulated reward row
    // survives, and the production close consumer (generate_withdrawals feeds the closing
    // block's withdrawals_root) still emits all 5 entries
    rewards.set_committee(committee_b);
    let counts_b = rewards.get_address_counts();
    assert_eq!(counts_b, counts_a, "re-entry preserves every reward row");
    let withdrawals_b = rewards.generate_withdrawals();
    assert_eq!(withdrawals_b.len(), 5, "the epoch close pays every epoch-start member");
    assert!(withdrawals_b
        .iter()
        .any(|w| w.address == victim_addr && w.amount == VICTIM_LEADER_BLOCKS as u64));

    // Leg C — control: the same tallies seeded from the post-burn TIP committee drop the
    // victim's row — the divergence the pin prevents, proving the assertions above would
    // catch a regression to tip-seeded entry
    let tip_committee = create_committee_from_state(tip_state).await?;
    assert_eq!(tip_committee.size(), 4);
    let control = GasAccumulator::new(1).rewards_counter();
    for authority in committee_a.authorities() {
        let count =
            if authority.execution_address() == victim_addr { VICTIM_LEADER_BLOCKS } else { 1 };
        for _ in 0..count {
            control.inc_leader_count(&authority.id());
        }
    }
    control.set_committee(tip_committee);
    let control_counts = control.get_address_counts();
    assert_eq!(control_counts.len(), 4, "tip seeding silently drops the ejected leader's row");
    assert!(!control_counts.contains_key(&victim_addr));
    let control_withdrawals = control.generate_withdrawals();
    assert_eq!(control_withdrawals.len(), 4);
    assert_ne!(
        control_withdrawals, withdrawals_b,
        "tip-seeded close builds different withdrawals — a divergent withdrawals_root"
    );

    // Leg D — epoch 0: a fresh chain pins genesis, and the pinned committee equals the tip
    // committee — genesis-epoch entry semantics are unchanged by the pin
    let fresh_dir = TempDir::with_prefix("burn_reentry_epoch0")?;
    let fresh_task_manager = TaskManager::new("burn reentry epoch 0");
    let fresh_env =
        RethEnv::new_for_temp_chain(chain.clone(), fresh_dir.path(), &fresh_task_manager, None)?;
    let (state_0, pin_0) = fresh_env.epoch_state_at_epoch_start()?;
    assert_eq!(pin_0.number, 0, "epoch 0 pins genesis");
    assert_eq!(state_0.epoch, 0);
    let committee_0 = create_committee_from_state(state_0).await?;
    let committee_0_tip =
        create_committee_from_state(fresh_env.epoch_state_from_canonical_tip()?).await?;
    assert_eq!(committee_0.size(), 5);
    assert_eq!(
        committee_0.bls_keys(),
        committee_0_tip.bls_keys(),
        "epoch 0's pinned view matches the tip view"
    );

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
