//! Batch maker (EL) collects transactions
//! and creates a batch. The batch is now a "pending block" in the blockchain tree.
//!
//! Execution engine receives consensus output and executes the round.
use fastcrypto::hash::Hash as _;
use fastcrypto::hash::Hash;
use narwhal_test_utils::{default_test_execution_node, TestExecutionNode};
use reth_blockchain_tree::BlockchainTreeViewer;
use reth_chainspec::ChainSpec;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{alloy_primitives::U160, GenesisAccount};
use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE, keccak256, proofs, Address, BlockHashOrNumber, B256,
    EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_primitives::{BlockBody, SealedBlock, TransactionSigned, Withdrawals};
use reth_provider::providers::BlockchainProvider;
use reth_provider::{BlockIdReader, BlockNumReader, BlockReader as _, TransactionVariant};
use reth_tasks::{TaskExecutor, TaskManager};
use reth_tracing::init_test_tracing;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, PoolConfig, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{collections::VecDeque, str::FromStr as _, sync::Arc, time::Duration};
use tn_block_maker::{BatchMakerBuilder, MiningMode};
use tn_engine::ExecutorEngine;
use tn_types::test_utils::test_genesis;
use tn_types::{
    adiri_chain_spec_arc, adiri_genesis, now,
    test_utils::{execute_test_batch, seeded_genesis_from_random_batches, OptionalTestBatchParams},
    BatchAPI as _, BatchDigest, Certificate, CertificateAPI, CommittedSubDag, ConsensusOutput,
    MetadataAPI as _, ReputationScores,
};
use tn_types::{
    test_utils::{get_gas_price, TransactionFactory},
    BatchAPI, MetadataAPI,
};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::wrappers::BroadcastStream;
use tracing::debug;

#[tokio::test]
async fn test_empty_output_executes() -> eyre::Result<()> {
    init_test_tracing();
    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    //
    // TODO: this does not use a "real" `ConsensusOutput` certificate
    //
    // refactor with valid data once test util helpers are in place
    let leader = Certificate::default();
    let sub_dag_index = 1;
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
        batches: Default::default(), // empty
        beneficiary,
        batch_digests: Default::default(), // empty
    };

    // adiri genesis with TxFactory funded
    let genesis = test_genesis();

    // let genesis = genesis.extend_accounts(account);
    let head_timestamp = genesis.timestamp;
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // execution node components
    let manager = TaskManager::current();
    let executor = manager.executor();
    let execution_node = default_test_execution_node(Some(chain.clone()), None, executor.clone())?;

    // mine a batch
    //
    // - the pending block is built off genesis
    // - the output extends canonical tip from genesis to block 1
    let pending_block =
        mine_batch(chain.clone(), &execution_node, head_timestamp, executor.clone()).await?;

    //
    // == taken from engine unit test for empty block
    //
    let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
    let consensus_output_stream = BroadcastStream::from(from_consensus);
    let provider = execution_node.get_provider().await;
    let evm_config = execution_node.get_evm_config().await;
    let max_round = None;
    let genesis_header = chain.sealed_genesis_header();

    let engine = ExecutorEngine::new(
        provider.clone(),
        evm_config,
        max_round,
        consensus_output_stream,
        genesis_header.clone(),
    );

    // send output
    let broadcast_result = to_engine.send(consensus_output.clone());
    assert!(broadcast_result.is_ok());

    // drop sending channel to shut engine down
    drop(to_engine);

    let (tx, rx) = oneshot::channel();

    // spawn engine task
    executor.spawn_blocking(async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await?;
    assert!(engine_task.is_ok());

    let last_block_num = provider.last_block_number()?;
    let canonical_tip = provider.canonical_tip();
    let final_block = provider.finalized_block_num_hash()?.expect("finalized block");

    assert_eq!(canonical_tip, final_block);
    assert_eq!(last_block_num, final_block.number);

    let expected_block_height = 1;
    // assert 1 empty block was executed for consensus
    assert_eq!(last_block_num, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip, final_block);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, sub_dag_index); // round of consensus

    // pull newly executed block from database (skip genesis)
    let expected_block = provider
        .block_with_senders(BlockHashOrNumber::Number(1), TransactionVariant::NoHash)?
        .expect("block 1 successfully executed");
    assert_eq!(expected_block_height, expected_block.number);

    // min basefee in genesis
    let expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    let output_digest: B256 = consensus_output.digest().into();
    // assert expected basefee
    assert_eq!(genesis_header.base_fee_per_gas, Some(expected_base_fee));
    // basefee comes from workers - if no batches, then use parent's basefee
    assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));

    // assert blocks are executed as expected
    assert!(expected_block.senders.is_empty());
    assert!(expected_block.body.is_empty());

    // assert basefee is same as worker's block
    assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));
    // beneficiary overwritten
    assert_eq!(expected_block.beneficiary, beneficiary);
    // nonce matches subdag index and method all match
    assert_eq!(expected_block.nonce, sub_dag_index);
    assert_eq!(expected_block.nonce, consensus_output.nonce());

    // ommers contains headers from all batches from consensus output
    let expected_ommers = consensus_output.ommers();
    assert_eq!(expected_block.ommers, expected_ommers);
    // ommers root
    assert_eq!(expected_block.header.ommers_hash, EMPTY_OMMER_ROOT_HASH,);
    // timestamp
    assert_eq!(expected_block.timestamp, consensus_output.committed_at());
    // parent beacon block root is output digest
    assert_eq!(expected_block.parent_beacon_block_root, Some(output_digest));
    // first block's parent is expected to be genesis
    assert_eq!(expected_block.parent_hash, chain.genesis_hash());
    // expect state roots to be the same as genesis bc no txs
    assert_eq!(expected_block.state_root, genesis_header.state_root);
    // expect header number genesis + 1
    assert_eq!(expected_block.number, expected_block_height);

    // mix hash is calculated from parent blocks parent_beacon_block_root and output's timestamp
    let expected_mix_hash = consensus_output.mix_hash_for_empty_payload(&genesis_header.mix_hash);
    assert_eq!(expected_block.mix_hash, expected_mix_hash);
    let manual_mix_hash = keccak256(
        [
            genesis_header.mix_hash.as_slice(),
            consensus_output.committed_at().to_le_bytes().as_slice(),
        ]
        .concat(),
    );
    assert_eq!(expected_block.mix_hash, manual_mix_hash);
    // bloom expected to be the same bc all proposed transactions should be good
    // ie) no duplicates, etc.
    //
    // TODO: randomly generate contract transactions as well!!!
    assert_eq!(expected_block.logs_bloom, genesis_header.logs_bloom);
    // gas limit should come from parent for empty execution
    //
    // TODO: ensure batch validation prevents peer workers from changing this value
    assert_eq!(expected_block.gas_limit, genesis_header.gas_limit);
    // no gas should be used - no txs
    assert_eq!(expected_block.gas_used, 0);
    // difficulty should be 0 to indicate first (and only) block from round
    assert_eq!(expected_block.difficulty, U256::ZERO);
    // assert extra data is empty 32-bytes (B256::ZERO)
    assert_eq!(expected_block.extra_data.as_ref(), &[0; 32]);
    // assert withdrawals are empty
    //
    // TODO: this is currently always empty
    assert_eq!(expected_block.withdrawals_root, genesis_header.withdrawals_root);

    //
    // === End unit test from engine empty block
    //

    // test worker's pending block that should be included in the next round
    // assert worker's block still appears as pending block in blockchain_tree
    let pending_after_consensus_round =
        BlockchainTreeViewer::pending_block(&provider).expect("worker's own pending block");
    assert_eq!(pending_block, pending_after_consensus_round);

    Ok(())
}

/// Mine a batch so there is a pending block in the blockchain tree.
///
/// This is taken from the batch_maker unit test
async fn mine_batch(
    chain: Arc<ChainSpec>,
    execution_node: &TestExecutionNode,
    head_timestamp: u64,
    executor: TaskExecutor,
) -> eyre::Result<SealedBlock> {
    let mut tx_factory = TransactionFactory::new();
    let blockchain_db = execution_node.get_provider().await;
    // txpool
    let blob_store = InMemoryBlobStore::default();
    let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
        .with_head_timestamp(head_timestamp)
        .with_additional_tasks(1)
        .build_with_tasks(blockchain_db.clone(), executor, blob_store.clone());

    let txpool =
        reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
    let max_transactions = 1;
    let mining_mode = MiningMode::instant(max_transactions, txpool.pending_transactions_listener());

    // worker channel
    let (to_worker, mut worker_rx) = tn_types::test_channel!(1);
    let address = Address::from(U160::from(33));

    let evm_config = EthEvmConfig::default();
    let block_executor = EthExecutorProvider::new(chain.clone(), evm_config);

    // build batch maker
    let task = BatchMakerBuilder::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        txpool.clone(),
        to_worker,
        mining_mode,
        address,
        block_executor,
    )
    .build();

    let gas_price = get_gas_price(&blockchain_db);
    debug!("gas price: {gas_price:?}");
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

    // submit 1 transaction
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Address::ZERO,
        value, // 1 TEL
    );

    // add transactions - panics on failure
    let _ = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;

    // spawn mining task
    let _mining_task = tokio::spawn(Box::pin(task));

    // wait for new batch
    let too_long = Duration::from_secs(5);
    let new_batch = timeout(too_long, worker_rx.recv())
        .await
        .expect("new batch created within time")
        .expect("new batch is Some()");

    debug!("new batch: {new_batch:?}");
    // number of transactions in the batch
    let batch_txs = new_batch.batch.transactions();

    // ensure decoded batch transaction is transaction1
    let batch_tx_bytes = batch_txs.first().cloned().expect("one tx in batch");
    let decoded_batch_tx = TransactionSigned::decode_enveloped(&mut batch_tx_bytes.as_ref())
        .expect("tx bytes are uncorrupted");
    assert_eq!(decoded_batch_tx, transaction1);

    // send the worker's ack to task
    let digest = new_batch.batch.digest();
    let _ack = new_batch.ack.send(digest);

    // yield to try and give pool a chance to update
    tokio::task::yield_now().await;
    // ensure tx1 is removed
    assert!(!txpool.contains(transaction1.hash_ref()));

    // assert batch appears as pending block in blockchain_tree
    let pending =
        BlockchainTreeViewer::pending_block(&blockchain_db).expect("worker's own pending block");
    let proposed_header = new_batch.batch.versioned_metadata().sealed_header();
    let expected = SealedBlock::new(
        proposed_header.clone(),
        BlockBody {
            transactions: vec![decoded_batch_tx],
            ommers: vec![],
            withdrawals: Some(Withdrawals::new(vec![])),
            requests: None,
        },
    );

    assert_eq!(&pending, &expected);

    let tip = blockchain_db.canonical_tip();
    // assert genesis is still canonical tip
    assert_eq!(tip.hash, chain.genesis_hash());
    Ok(expected)
}
