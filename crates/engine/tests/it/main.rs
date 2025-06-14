//! Test execution engine for full batches.

use std::{collections::VecDeque, str::FromStr as _, sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_batch_builder::test_utils::execute_test_batch;
use tn_engine::ExecutorEngine;
use tn_reth::{test_utils::seeded_genesis_from_random_batches, FixedBytes, RethChainSpec};
use tn_test_utils::default_test_execution_node;
use tn_types::{
    gas_accumulator::GasAccumulator, max_batch_gas, now, test_chain_spec_arc, test_genesis,
    Address, BlockHash, Bloom, Bytes, Certificate, CommittedSubDag, ConsensusOutput, Hash as _,
    Notifier, ReputationScores, TaskManager, B256, EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS,
    MIN_PROTOCOL_BASE_FEE, U256,
};
use tokio::{sync::oneshot, time::timeout};
use tracing::debug;

/// This tests that a single block is executed if the output from consensus contains no
/// transactions.
#[tokio::test]
async fn test_empty_output_executes_early_finalize() -> eyre::Result<()> {
    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let mut leader = Certificate::default();
    let sub_dag_index = 0;
    leader.header.round = sub_dag_index as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let beneficiary = Address::from_str("0x5555555555555555555555555555555555555555")
        .expect("beneficiary address from str");
    let consensus_output = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![Certificate::default()],
            leader,
            sub_dag_index,
            reputation_scores,
            previous_sub_dag,
        )
        .into(),
        beneficiary,
        early_finalize: true,
        ..Default::default()
    };
    let consensus_output_hash = consensus_output.consensus_header_hash();

    let chain = test_chain_spec_arc();

    let tmp_dir = TempDir::new().expect("temp dir");
    // execution node components
    let execution_node = default_test_execution_node(Some(chain.clone()), None, tmp_dir.path())?;

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let reth_env = execution_node.get_reth_env().await;
    let max_round = None;
    let genesis_header = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        max_round,
        from_consensus,
        genesis_header.clone(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
    );

    // send output
    let broadcast_result = to_engine.send(consensus_output.clone()).await;
    assert!(broadcast_result.is_ok());

    // drop sending channel to shut engine down
    drop(to_engine);

    let (tx, rx) = oneshot::channel();

    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    assert_eq!(canonical_in_memory_state.canonical_chain().count(), 0);

    // spawn engine task
    task_manager.spawn_blocking(Box::pin(async move {
        let res = engine.await;
        let _ = tx.send(res);
    }));

    let engine_task = timeout(Duration::from_secs(10), rx).await?;
    assert!(engine_task.is_ok());

    // assert memory is clean after execution
    assert_eq!(canonical_in_memory_state.canonical_chain().count(), 0);
    let last_block_num = reth_env.last_block_number()?;
    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    assert_eq!(last_block_num, final_block.number);

    let expected_block_height = 1;
    // assert 1 empty block was executed for consensus
    assert_eq!(last_block_num, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_hash);

    // pull newly executed block from database (skip genesis)
    let expected_block =
        reth_env.sealed_block_by_number(1)?.expect("block 1 successfully executed");
    assert_eq!(expected_block_height, expected_block.number);

    // min basefee in genesis
    let expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    // assert expected basefee
    assert_eq!(genesis_header.base_fee_per_gas, Some(expected_base_fee));
    // basefee comes from workers - if no batches, then use parent's basefee
    assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));

    // assert blocks are executed as expected
    assert!(expected_block.senders()?.is_empty());
    assert!(expected_block.body().transactions.is_empty());

    // assert basefee is same as worker's block
    assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));
    // beneficiary overwritten
    assert_eq!(expected_block.beneficiary, beneficiary);
    // nonce matches subdag index and method all match
    assert_eq!(<FixedBytes<8> as Into<u64>>::into(expected_block.nonce), sub_dag_index);
    assert_eq!(<FixedBytes<8> as Into<u64>>::into(expected_block.nonce), consensus_output.nonce());

    // ommers root
    assert_eq!(expected_block.header().ommers_hash, EMPTY_OMMER_ROOT_HASH,);
    // timestamp
    assert_eq!(expected_block.timestamp, consensus_output.committed_at());
    // parent beacon block root is output digest
    assert_eq!(
        expected_block.parent_beacon_block_root,
        Some(consensus_output.consensus_header_hash())
    );
    // first block's parent is expected to be genesis
    assert_eq!(expected_block.parent_hash, chain.genesis_hash());
    // expect state roots to be same because empty output has no state change
    assert_eq!(expected_block.state_root, genesis_header.state_root);
    // expect header number genesis + 1
    assert_eq!(expected_block.number, expected_block_height);

    // mix hash is xor bitwise with worker sealed block's hash and consensus output
    // just use consensus output hash if no batches in the round
    let consensus_output_hash = B256::from(consensus_output.digest());
    assert_eq!(expected_block.mix_hash, consensus_output_hash);
    // bloom expected to be the same bc all proposed transactions should be good
    // ie) no duplicates, etc.
    //
    // TODO: randomly generate contract transactions as well!!!
    assert_eq!(expected_block.logs_bloom, genesis_header.logs_bloom);
    // gas limit should come from parent for empty execution
    assert_eq!(expected_block.gas_limit, genesis_header.gas_limit);
    // no gas should be used - no txs
    assert_eq!(expected_block.gas_used, 0);
    // difficulty should be 0 to indicate first (and only) block from round
    assert_eq!(expected_block.difficulty, U256::ZERO);
    // assert extra data is default bytes
    assert_eq!(expected_block.extra_data, Bytes::default());
    // assert batch digest match requests hash
    assert!(expected_block.requests_hash.is_none());
    // assert withdrawals are empty
    //
    // NOTE: this is currently always empty
    assert_eq!(expected_block.withdrawals_root, genesis_header.withdrawals_root);

    Ok(())
}

/// This tests that a single block is NOT executed if the output from consensus contains no
/// transactions and we are not setting early finalize.
#[tokio::test]
async fn test_empty_output_executes_late_finalize() -> eyre::Result<()> {
    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let mut leader = Certificate::default();
    let sub_dag_index = 0;
    leader.header.round = sub_dag_index as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let beneficiary = Address::from_str("0x5555555555555555555555555555555555555555")
        .expect("beneficiary address from str");
    let consensus_output = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![Certificate::default()],
            leader,
            sub_dag_index,
            reputation_scores,
            previous_sub_dag,
        )
        .into(),
        beneficiary,
        early_finalize: false,
        ..Default::default()
    };

    let chain = test_chain_spec_arc();

    let tmp_dir = TempDir::new().expect("temp dir");
    // execution node components
    let execution_node = default_test_execution_node(Some(chain.clone()), None, tmp_dir.path())?;

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let reth_env = execution_node.get_reth_env().await;
    let max_round = None;
    let genesis_header = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        max_round,
        from_consensus,
        genesis_header.clone(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
    );

    // send output
    let broadcast_result = to_engine.send(consensus_output.clone()).await;
    assert!(broadcast_result.is_ok());

    // drop sending channel to shut engine down
    drop(to_engine);

    let (tx, rx) = oneshot::channel();

    // spawn engine task
    task_manager.spawn_blocking(Box::pin(async move {
        let res = engine.await;
        let _ = tx.send(res);
    }));

    let engine_task = timeout(Duration::from_secs(10), rx).await?;
    assert!(engine_task.is_ok());

    let last_block_num = reth_env.last_block_number()?;
    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?;
    assert!(final_block.is_none());

    let expected_block_height = 1;
    // assert 1 empty block was executed for consensus
    assert_eq!(last_block_num, expected_block_height);
    assert_eq!(canonical_tip.number, expected_block_height);
    // assert last executed output is not finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, BlockHash::default());
    Ok(())
}

/// Test the engine shuts down after the sending half of the broadcast channel is closed.
///
/// One output is queued (simulating output already received) in the engine and another is sent
/// on the channel. Then, the sender is dropped and the engine task is started.
///
/// Expected result:
/// - engine receives last broadcast
/// - engine processes queued output first
/// - engine processes last broadcast second
/// - engine has no more output in queue and gracefully shuts down
///
/// NOTE: all batches are built with genesis as the parent. Building blocks from historic
/// parents is currently valid.
#[tokio::test]
async fn test_queued_output_executes_after_sending_channel_closed() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().expect("temp dir");
    // create batches for consensus output
    let mut batches_1 = tn_reth::test_utils::batches(4); // create 4 batches
    let mut batches_2 = tn_reth::test_utils::batches(4); // create 4 batches

    // okay to clone these because they are only used to seed genesis, decode transactions, and
    // recover signers
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // use default genesis and seed accounts to execute batches
    let genesis = test_genesis();
    let (genesis, txs_by_block, signers_by_block) =
        seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution node components
    let execution_node =
        default_test_execution_node(Some(chain.clone()), None, &tmp_dir.path().join("exc-node"))?;
    let parent = chain.sealed_genesis_header();

    // execute batches to update headers with valid data
    let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

    // updated batches separately because they are mutated in-place
    // and need to be passed to different outputs
    //
    // update first round
    for (idx, batch) in batches_1.iter_mut().enumerate() {
        // increase basefee
        inc_base_fee += idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = Some(inc_base_fee);

        // actually execute the block now
        execute_test_batch(batch, &parent);
        debug!("{idx}\n{:?}\n", batch);
    }

    // update second round
    for (idx, batch) in batches_2.iter_mut().enumerate() {
        // continue increasing basefee
        // add 4 to continue where previous round left off
        // this makes assertions easier at the end
        inc_base_fee += 4 + idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = Some(inc_base_fee);

        // actually execute the block now
        execute_test_batch(batch, &parent);
        debug!("{idx}\n{:?}\n", batch);
    }
    // Reload all_batches so we can calculate mix_hash properly later.
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader_1 = Certificate::default();
    // update timestamp
    leader_1.update_created_at_for_test(timestamp);
    let sub_dag_index_1 = 1;
    leader_1.header.round = sub_dag_index_1 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let mut batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
    let subdag_1 = Arc::new(CommittedSubDag::new(
        vec![Certificate::default()],
        leader_1,
        sub_dag_index_1,
        reputation_scores,
        previous_sub_dag,
    ));
    let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
        .expect("beneficiary address from str");
    let consensus_output_1 = ConsensusOutput {
        sub_dag: subdag_1.clone(),
        batches: vec![batches_1],
        beneficiary: beneficiary_1,
        batch_digests: batch_digests_1.clone(),
        early_finalize: true,
        ..Default::default()
    };

    // create second output
    let mut leader_2 = Certificate::default();
    // update timestamp
    leader_2.update_created_at_for_test(timestamp + 2);
    let sub_dag_index_2 = 2;
    leader_2.header.round = sub_dag_index_2 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = Some(subdag_1.as_ref());
    let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
    let subdag_2 = CommittedSubDag::new(
        vec![Certificate::default()],
        leader_2,
        sub_dag_index_2,
        reputation_scores,
        previous_sub_dag,
    )
    .into();
    let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
        .expect("beneficiary address from str");
    let consensus_output_2 = ConsensusOutput {
        sub_dag: subdag_2,
        batches: vec![batches_2],
        beneficiary: beneficiary_2,
        batch_digests: batch_digests_2.clone(),
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        early_finalize: true,
        ..Default::default()
    };
    let consensus_output_2_hash = consensus_output_2.consensus_header_hash();

    // combine VecDeque and convert to Vec for assertions later
    batch_digests_1.extend(batch_digests_2);
    let all_batch_digests: Vec<BlockHash> = batch_digests_1.into();

    //=== Execution

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let max_round = None;
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let mut engine = ExecutorEngine::new(
        reth_env.clone(),
        max_round,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
    );

    // assert the canonical chain in-memory is empty
    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    assert_eq!(canonical_in_memory_state.canonical_chain().count(), 0);

    // queue the first output - simulate already received from channel
    engine.push_back_queued_for_test(consensus_output_1.clone());

    // send second output
    let broadcast_result = to_engine.send(consensus_output_2.clone()).await;
    assert!(broadcast_result.is_ok());

    // drop sending channel before receiver has a chance to process message
    drop(to_engine);

    // channels for engine shutting down
    let (tx, rx) = oneshot::channel();

    // spawn engine task
    //
    // one output already queued up, one output waiting in broadcast stream
    task_manager.spawn_blocking(Box::pin(async move {
        let res = engine.await;
        let _ = tx.send(res);
    }));

    let engine_task = timeout(Duration::from_secs(10), rx).await?;
    assert!(engine_task.is_ok());

    let last_block_num = reth_env.last_block_number()?;
    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    debug!("last block num {last_block_num:?}");
    debug!("canonical tip: {canonical_tip:?}");
    debug!("final block num {final_block:?}");

    let expected_block_height = 8;
    // assert canonical memory is cleaned up
    assert_eq!(canonical_in_memory_state.canonical_chain().count(), 0);
    // assert all 8 batches were executed
    assert_eq!(last_block_num, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_2_hash); // round of consensus

    // pull newly executed blocks from database (skip genesis)
    //
    // Uses the provided `headers_range` to get the headers for the range, and `assemble_block`
    // to construct blocks from the following inputs:
    //     – Header
    //     - Transactions
    //     – Ommers
    //     – Withdrawals
    //     – Requests
    //     – Senders
    let executed_blocks = reth_env.block_with_senders_range(1..=expected_block_height)?;
    assert_eq!(expected_block_height, executed_blocks.len() as u64);

    // basefee intentionally increased with loop
    let mut expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    let output_digest_1: B256 = consensus_output_1.digest().into();
    let output_digest_2: B256 = consensus_output_2.digest().into();

    // assert blocks are executed as expected
    for (idx, txs) in txs_by_block.iter().enumerate() {
        let block = &executed_blocks[idx];
        let signers = &signers_by_block[idx];
        assert_eq!(&block.senders(), signers);
        assert_eq!(&block.body().transactions, txs);

        // basefee was increased for each batch
        expected_base_fee += idx as u64;
        // assert basefee is same as worker's block
        assert_eq!(block.base_fee_per_gas, Some(expected_base_fee));

        // define re-usable variable here for asserting all values against expected output
        let mut expected_output = &consensus_output_1;
        let mut expected_beneficiary = &beneficiary_1;
        let mut expected_subdag_index = &sub_dag_index_1;
        let mut output_digest = output_digest_1;
        let mut expected_parent_beacon_block_root = consensus_output_1.consensus_header_hash();
        let mut expected_batch_index = idx;

        // update values based on index for all assertions below
        if idx >= 4 {
            // use different output for last 4 blocks
            expected_output = &consensus_output_2;
            expected_beneficiary = &beneficiary_2;
            expected_subdag_index = &sub_dag_index_2;
            output_digest = output_digest_2;
            expected_parent_beacon_block_root = consensus_output_2.consensus_header_hash();
            // takeaway 4 to compensate for independent loops for executing batches
            expected_batch_index = idx - 4;
        }

        // beneficiary overwritten
        assert_eq!(&block.beneficiary, expected_beneficiary);
        // nonce matches subdag index and method all match
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), *expected_subdag_index);
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), expected_output.nonce());

        // timestamp
        assert_eq!(block.timestamp, expected_output.committed_at());
        // parent beacon block root is output digest
        assert_eq!(block.parent_beacon_block_root, Some(expected_parent_beacon_block_root));

        if idx == 0 {
            // first block's parent is expected to be genesis
            assert_eq!(block.parent_hash, chain.genesis_hash());
            // expect header number 1 for batch bc of genesis
            assert_eq!(block.number, 1);
        } else {
            // assert parents executed in order (sanity check)
            let expected_parent = executed_blocks[idx - 1].header().hash_slow();
            assert_eq!(block.parent_hash, expected_parent);
            // expect block numbers NOT the same as batch's headers
            assert_ne!(block.number, 1);
        }

        // mix hash is xor batch's hash and consensus output digest
        let expected_mix_hash = all_batches[idx].digest() ^ output_digest;
        assert_eq!(block.mix_hash, expected_mix_hash);
        // bloom expected to be the same bc all proposed transactions should be good
        // ie) no duplicates, etc.
        //
        // TODO: randomly generate contract transactions as well!!!
        assert_eq!(block.logs_bloom, Bloom::default());
        // gas limit should come from batch
        assert_eq!(block.gas_limit, max_batch_gas(block.number));
        // difficulty should match the batch's index within consensus output
        assert_eq!(block.difficulty, U256::from(expected_batch_index << 16));
        // assert batch digest match extra data
        assert_eq!(block.extra_data, Bytes::default());
        // assert batch digest match requests hash
        assert_eq!(block.requests_hash, Some(all_batch_digests[idx]));
        // assert batch's withdrawals match
        //
        // NOTE: this is currently always empty
        assert_eq!(block.withdrawals_root, Some(EMPTY_WITHDRAWALS));
    }

    Ok(())
}

/// Test the engine successfully executes a duplicate batch (duplicate transactions);
///
/// Expected result:
/// - engine receives output with duplicate transactions
/// - engine produces empty block for duplicate batch
/// - engine has no more output in queue and gracefully shuts down
///
/// NOTE: all batches are built with genesis as the parent. Building blocks from historic
/// parents is currently valid.
#[tokio::test]
async fn test_execution_succeeds_with_duplicate_transactions() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    // create batches for consensus output
    let mut batches_1 = tn_reth::test_utils::batches(4); // create 4 batches
    let mut batches_2 = tn_reth::test_utils::batches(4); // create 4 batches

    // duplicate transactions in last batch for each round
    //
    // simulate duplicate batches from same round
    // and
    // duplicate transactions from a previous round
    batches_1[3] = batches_1[0].clone();
    batches_2[3] = batches_1[1].clone();

    // okay to clone these because they are only used to seed genesis, decode transactions, and
    // recover signers
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // use default genesis and seed accounts to execute batches
    let genesis = test_genesis();
    let (genesis, txs_by_block, signers_by_block) =
        seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution node components
    let execution_node =
        default_test_execution_node(Some(chain.clone()), None, &tmp_dir.path().join("exc-node"))?;
    let parent = chain.sealed_genesis_header();

    // execute batches to update headers with valid data
    let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

    // updated batches separately because they are mutated in-place
    // and need to be passed to different outputs
    //
    // update first round
    for (idx, batch) in batches_1.iter_mut().enumerate() {
        // increase basefee
        inc_base_fee += idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = Some(inc_base_fee);

        // actually execute the block now
        execute_test_batch(batch, &parent);
        debug!("{idx}\n{:?}\n", batch);
    }

    // update second round
    for (idx, batch) in batches_2.iter_mut().enumerate() {
        // continue increasing basefee
        // add 4 to continue where previous round left off
        // this makes assertions easier at the end
        inc_base_fee += 4 + idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = Some(inc_base_fee);

        // actually execute the block now
        execute_test_batch(batch, &parent);
        debug!("{idx}\n{:?}\n", batch);
    }
    // Reload all_batches so we can calculate mix_hash properly later.
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // store ref as variable for clarity
    let duplicated_batch_for_round_1 = &batches_1[0];
    let duplicated_batch_for_round_2 = &batches_1[1];
    let duplicate_batch_round_1 = &batches_1[3];
    let duplicate_batch_round_2 = &batches_2[3];

    // assert duplicate txs are same, but batches are different
    //
    // round 1
    assert_eq!(duplicate_batch_round_1.transactions(), duplicated_batch_for_round_1.transactions());
    assert_ne!(duplicate_batch_round_1, duplicated_batch_for_round_1);
    // round 2
    assert_eq!(duplicate_batch_round_2.transactions(), duplicated_batch_for_round_2.transactions());
    assert_ne!(duplicate_batch_round_2, duplicated_batch_for_round_2);

    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader_1 = Certificate::default();
    // update timestamp
    leader_1.update_created_at_for_test(timestamp);
    let sub_dag_index_1: u64 = 1;
    leader_1.header.round = sub_dag_index_1 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let mut batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
    let mut cert_1 = Certificate::default();
    cert_1.header.round = 1;
    let subdag_1 = Arc::new(CommittedSubDag::new(
        vec![cert_1],
        leader_1,
        sub_dag_index_1,
        reputation_scores,
        previous_sub_dag,
    ));
    let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
        .expect("beneficiary address from str");
    let consensus_output_1 = ConsensusOutput {
        sub_dag: subdag_1.clone(),
        batches: vec![batches_1],
        beneficiary: beneficiary_1,
        batch_digests: batch_digests_1.clone(),
        early_finalize: true,
        ..Default::default()
    };

    // create second output
    let mut leader_2 = Certificate::default();
    // update timestamp
    leader_2.update_created_at_for_test(timestamp + 2);
    let sub_dag_index_2 = 2;
    leader_2.header.round = sub_dag_index_2 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = Some(subdag_1.as_ref());
    let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
    let mut cert_2 = Certificate::default();
    cert_2.header.round = 2;
    let subdag_2 = CommittedSubDag::new(
        vec![cert_2],
        leader_2,
        sub_dag_index_2,
        reputation_scores,
        previous_sub_dag,
    )
    .into();
    let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
        .expect("beneficiary address from str");
    let consensus_output_2 = ConsensusOutput {
        sub_dag: subdag_2,
        batches: vec![batches_2],
        beneficiary: beneficiary_2,
        batch_digests: batch_digests_2.clone(),
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        early_finalize: true,
        ..Default::default()
    };
    let consensus_output_2_hash = consensus_output_2.consensus_header_hash();

    // combine VecDeque and convert to Vec for assertions later
    batch_digests_1.extend(batch_digests_2);
    let all_batch_digests: Vec<BlockHash> = batch_digests_1.into();

    //=== Execution

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let max_round = None;
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let mut engine = ExecutorEngine::new(
        reth_env.clone(),
        max_round,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
    );

    // queue the first output - simulate already received from channel
    engine.push_back_queued_for_test(consensus_output_1.clone());

    // send second output
    let broadcast_result = to_engine.send(consensus_output_2.clone()).await;
    assert!(broadcast_result.is_ok());

    // drop sending channel before receiver has a chance to process message
    drop(to_engine);

    // channels for engine shutting down
    let (tx, rx) = oneshot::channel();

    // spawn engine task
    //
    // one output already queued up, one output waiting in broadcast stream
    task_manager.spawn_blocking(Box::pin(async move {
        let res = engine.await;
        let _ = tx.send(res);
    }));

    let engine_task = timeout(Duration::from_secs(10), rx).await?;
    assert!(engine_task.is_ok());

    let last_block_num = reth_env.last_block_number()?;
    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    debug!("last block num {last_block_num:?}");
    debug!("canonical tip: {canonical_tip:?}");
    debug!("final block num {final_block:?}");

    // expect 1 block per batch still, but 2 blocks will be empty because they contained
    // duplicate transactions
    let expected_block_height = 8;
    let expected_duplicate_block_num_round_1 = 4;
    let expected_duplicate_block_num_round_2 = 8;
    // assert all 8 batches were executed
    assert_eq!(last_block_num, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_2_hash); // round of consensus

    // pull newly executed blocks from database (skip genesis)
    //
    // Uses the provided `headers_range` to get the headers for the range, and `assemble_block`
    // to construct blocks from the following inputs:
    //     – Header
    //     - Transactions
    //     – Ommers
    //     – Withdrawals
    //     – Requests
    //     – Senders
    let executed_blocks = reth_env.block_with_senders_range(1..=expected_block_height)?;
    assert_eq!(expected_block_height, executed_blocks.len() as u64);

    // basefee intentionally increased with loop
    let mut expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    let output_digest_1: B256 = consensus_output_1.digest().into();
    let output_digest_2: B256 = consensus_output_2.digest().into();

    // assert blocks are execute as expected
    for (idx, txs) in txs_by_block.iter().enumerate() {
        let block = &executed_blocks[idx];
        let signers = &signers_by_block[idx];

        // expect blocks 4 and 8 to be empty (no txs bc they are duplicates)
        // sub 1 to account for loop idx starting at 0
        if idx == expected_duplicate_block_num_round_1 - 1
            || idx == expected_duplicate_block_num_round_2 - 1
        {
            assert!(block.senders().is_empty());
            assert!(block.body().transactions.is_empty());
            // gas used should NOT be the same as bc duplicate transaction are ignored
            assert_ne!(block.gas_used, max_batch_gas(block.number));
            // gas used should be zero bc all transactions were duplicates
            assert_eq!(block.gas_used, 0);
        } else {
            assert_eq!(&block.senders(), signers);
            assert_eq!(&block.body().transactions, txs);
        }

        // basefee was increased for each batch
        expected_base_fee += idx as u64;
        // assert basefee is same as worker's block
        assert_eq!(block.base_fee_per_gas, Some(expected_base_fee));

        // define re-usable variable here for asserting all values against expected output
        let mut expected_output = &consensus_output_1;
        let mut expected_beneficiary = &beneficiary_1;
        let mut expected_subdag_index = &sub_dag_index_1;
        let mut output_digest = output_digest_1;
        // We just set this to default in the test...
        let mut expected_parent_beacon_block_root = consensus_output_1.consensus_header_hash();
        let mut expected_batch_index = idx;

        // update values based on index for all assertions below
        if idx >= 4 {
            // use different output for last 4 blocks
            expected_output = &consensus_output_2;
            expected_beneficiary = &beneficiary_2;
            expected_subdag_index = &sub_dag_index_2;
            output_digest = output_digest_2;
            expected_parent_beacon_block_root = consensus_output_2.consensus_header_hash();
            // takeaway 4 to compensate for independent loops for executing batches
            expected_batch_index = idx - 4;
        }

        // beneficiary overwritten
        assert_eq!(&block.beneficiary, expected_beneficiary);
        // nonce matches subdag index and method all match
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), *expected_subdag_index);
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), expected_output.nonce());

        // timestamp
        assert_eq!(block.timestamp, expected_output.committed_at());
        // parent beacon block root is output digest
        assert_eq!(block.parent_beacon_block_root, Some(expected_parent_beacon_block_root));

        if idx == 0 {
            // first block's parent is expected to be genesis
            assert_eq!(block.parent_hash, chain.genesis_hash());
        } else {
            // assert parents executed in order (sanity check)
            let expected_parent = executed_blocks[idx - 1].header().hash_slow();
            assert_eq!(block.parent_hash, expected_parent);
        }

        // mix hash is xor batch's hash and consensus output digest
        let expected_mix_hash = all_batches[idx].digest() ^ output_digest;
        assert_eq!(block.mix_hash, expected_mix_hash);
        // bloom expected to be the same bc all proposed transactions should be good
        // ie) no duplicates, etc.
        //
        // TODO: this doesn't actually test anything bc there are no contract txs
        assert_eq!(block.logs_bloom, Bloom::default());
        // gas limit should come from batch
        assert_eq!(block.gas_limit, max_batch_gas(block.number));
        // difficulty should match the batch's index within consensus output
        assert_eq!(block.difficulty, U256::from(expected_batch_index << 16));
        // assert batch digest match extra data
        assert_eq!(block.extra_data, Bytes::default());
        // assert batch digest match requests hash
        assert_eq!(block.requests_hash, Some(all_batch_digests[idx]));
        // assert batch's withdrawals match
        //
        // NOTE: this is currently always empty
        assert_eq!(block.withdrawals_root, Some(EMPTY_WITHDRAWALS));
    }

    Ok(())
}

#[tokio::test]
async fn test_max_round_terminates_early() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    // create batches for consensus output
    let mut batches_1 = tn_reth::test_utils::batches(4); // create 4 batches
    let mut batches_2 = tn_reth::test_utils::batches(4); // create 4 batches

    // okay to clone these because they are only used to seed genesis, decode transactions, and
    // recover signers
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // use default genesis and seed accounts to execute batches
    let genesis = test_genesis();
    let (genesis, _txs_by_block, _signers_by_block) =
        seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution node components
    let execution_node =
        default_test_execution_node(Some(chain.clone()), None, &tmp_dir.path().join("exc-node"))?;
    let parent = chain.sealed_genesis_header();

    // execute batches to update headers with valid data
    let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

    // updated batches separately because they are mutated in-place
    // and need to be passed to different outputs
    //
    // update first round
    for (idx, batch) in batches_1.iter_mut().enumerate() {
        // increase basefee
        inc_base_fee += idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = Some(inc_base_fee);

        // actually execute the block now
        execute_test_batch(batch, &parent);
        debug!("{idx}\n{:?}\n", batch);
    }

    // update second round
    for (idx, batch) in batches_2.iter_mut().enumerate() {
        // continue increasing basefee
        // add 4 to continue where previous round left off
        // this makes assertions easier at the end
        inc_base_fee += 4 + idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = Some(inc_base_fee);

        // actually execute the block now
        execute_test_batch(batch, &parent);
        debug!("{idx}\n{:?}\n", batch);
    }

    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader_1 = Certificate::default();
    // update timestamp
    leader_1.update_created_at_for_test(timestamp);
    let sub_dag_index_1 = 1;
    leader_1.header.round = sub_dag_index_1 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
    let subdag_1 = Arc::new(CommittedSubDag::new(
        vec![Certificate::default()],
        leader_1,
        sub_dag_index_1,
        reputation_scores,
        previous_sub_dag,
    ));
    let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
        .expect("beneficiary address from str");
    let consensus_output_1 = ConsensusOutput {
        sub_dag: subdag_1.clone(),
        batches: vec![batches_1],
        beneficiary: beneficiary_1,
        batch_digests: batch_digests_1,
        early_finalize: true,
        ..Default::default()
    };
    let consensus_output_1_hash = consensus_output_1.consensus_header_hash();

    // create second output
    let mut leader_2 = Certificate::default();
    // update timestamp
    leader_2.update_created_at_for_test(timestamp + 2);
    let sub_dag_index_2 = 2;
    leader_2.header.round = sub_dag_index_2 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = Some(subdag_1.as_ref());
    let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
    let subdag_2 = CommittedSubDag::new(
        vec![Certificate::default()],
        leader_2,
        sub_dag_index_2,
        reputation_scores,
        previous_sub_dag,
    )
    .into();
    let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
        .expect("beneficiary address from str");
    let consensus_output_2 = ConsensusOutput {
        sub_dag: subdag_2,
        batches: vec![batches_2],
        beneficiary: beneficiary_2,
        batch_digests: batch_digests_2,
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        early_finalize: true,
        ..Default::default()
    };

    //=== Execution

    let (_to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    // set max round to "1" - this should receive both digests, but stop after the first round
    let max_round = Some(1);
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let mut engine = ExecutorEngine::new(
        reth_env.clone(),
        max_round,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
    );

    // queue both output - simulate already received from channel
    engine.push_back_queued_for_test(consensus_output_1);
    engine.push_back_queued_for_test(consensus_output_2);

    // NOTE: sending channel is NOT dropped in this test, so engine should continue listening
    // until max block reached

    // channels for engine shutting down
    let (tx, rx) = oneshot::channel();

    // spawn engine task
    //
    // one output already queued up, one output waiting in broadcast stream
    task_manager.spawn_blocking(Box::pin(async move {
        let res = engine.await;
        let _ = tx.send(res);
    }));

    let engine_task = timeout(Duration::from_secs(10), rx).await?;
    assert!(engine_task.is_ok());

    let last_block_num = reth_env.last_block_number()?;
    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    debug!("last block num {last_block_num:?}");
    debug!("canonical tip: {canonical_tip:?}");
    debug!("final block num {final_block:?}");

    let expected_block_height = 4;
    // assert all 4 batches were executed from round 1
    assert_eq!(last_block_num, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_1_hash);

    Ok(())
}
