//! Block builder (EL) collects transactions and creates blocks.
//!
//! Block builder (CL) receives the block from EL and forwards it to the Quorum Waiter for votes
//! from peers.

use assert_matches::assert_matches;
use narwhal_network::client::NetworkClient;
use narwhal_network_types::MockWorkerToPrimary;
use narwhal_typed_store::{open_db, tables::WorkerBlocks, traits::Database};
use narwhal_worker::{
    metrics::WorkerMetrics,
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
    BlockProvider,
};
use reth_blockchain_tree::{
    noop::NoopBlockchainTree, BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree,
    TreeExternals,
};
use reth_chainspec::ChainSpec;
use reth_db::test_utils::{create_test_rw_db, tempdir_path};
use reth_db_common::init::init_genesis;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{
    alloy_primitives::U160, Address, BlockBody, Bytes, SealedBlock, SealedHeader, U256,
};
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    CanonStateSubscriptions, ProviderFactory,
};
use reth_prune::PruneModes;
use reth_tasks::TaskManager;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, PoolConfig, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_block_builder::{test_utils::execute_test_worker_block, BlockBuilder};
use tn_block_validator::{BlockValidation, BlockValidator};
use tn_engine::execute_consensus_output;
use tn_types::{
    test_utils::{get_gas_price, test_genesis, TransactionFactory},
    AutoSealConsensus, BuildArguments, Certificate, CommittedSubDag, Consensus, ConsensusOutput,
    LastCanonicalUpdate, ReputationScores, WorkerBlock,
};
use tokio::time::timeout;
use tracing::debug;

#[derive(Clone, Debug)]
struct TestMakeBlockQuorumWaiter();
impl QuorumWaiterTrait for TestMakeBlockQuorumWaiter {
    fn verify_block(
        &self,
        _block: WorkerBlock,
        _timeout: Duration,
    ) -> tokio::task::JoinHandle<Result<(), QuorumWaiterError>> {
        tokio::spawn(async move { Ok(()) })
    }
}

#[tokio::test]
async fn test_make_block_el_to_cl() {
    // reth_tracing::init_test_tracing();

    //
    //=== Consensus Layer
    //

    let network_client = NetworkClient::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let node_metrics = WorkerMetrics::default();

    // Mock the primary client to always succeed.
    let mut mock_server = MockWorkerToPrimary::new();
    mock_server.expect_report_own_block().returning(|_| Ok(anemo::Response::new(())));
    network_client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    let qw = TestMakeBlockQuorumWaiter();
    let timeout = Duration::from_secs(5);
    let block_provider = BlockProvider::new(
        0,
        qw.clone(),
        Arc::new(node_metrics),
        network_client,
        store.clone(),
        timeout,
    );

    //
    //=== Execution Layer
    //

    // adiri genesis with TxFactory funded
    let genesis = test_genesis();

    // let genesis = genesis.extend_accounts(account);
    let head_timestamp = genesis.timestamp;
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // temp db
    let db = create_test_rw_db();

    // provider
    let factory = ProviderFactory::new(
        Arc::clone(&db),
        Arc::clone(&chain),
        StaticFileProvider::read_write(tempdir_path())
            .expect("static file provider read write created with tempdir path"),
    );

    let genesis_hash = init_genesis(factory.clone()).expect("init genesis");
    let blockchain_db = BlockchainProvider::new(factory, Arc::new(NoopBlockchainTree::default()))
        .expect("test blockchain provider");

    debug!("genesis hash: {genesis_hash:?}");

    // task manger
    let manager = TaskManager::current();
    let executor = manager.executor();

    // TODO: abstract the txpool creation to call in engine::inner and here
    //
    // txpool
    let blob_store = InMemoryBlobStore::default();
    let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
        .with_head_timestamp(head_timestamp)
        .with_additional_tasks(1)
        .build_with_tasks(blockchain_db.clone(), executor, blob_store.clone());

    let txpool =
        reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
    let address = Address::from(U160::from(333));
    let tx_pool_latest = txpool.block_info();
    let tip = SealedBlock::new(chain.sealed_genesis_header(), BlockBody::default());

    let latest_canon_state = LastCanonicalUpdate {
        tip, // genesis
        pending_block_base_fee: tx_pool_latest.pending_basefee,
        pending_block_blob_fee: tx_pool_latest.pending_blob_fee,
    };

    // build execution block proposer
    let block_builder = BlockBuilder::new(
        blockchain_db.clone(),
        txpool.clone(),
        blockchain_db.canonical_state_stream(),
        latest_canon_state,
        block_provider.blocks_rx(),
        address,
        Duration::from_secs(1),
        30_000_000, // 30mil gas limit
        1_000_000,  // 1MB size
    );

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();

    // create 3 transactions
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );
    debug!("transaction 1: {transaction1:?}");

    let transaction2 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );
    debug!("transaction 2: {transaction2:?}");

    let transaction3 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );
    debug!("transaction 3: {transaction3:?}");

    let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction1.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction2.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction3.hash());

    // txpool size
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 3);

    // spawn block_builder once worker is ready
    let _block_builder = tokio::spawn(Box::pin(block_builder));

    //
    //=== Test block flow
    //

    // wait for new block
    let mut block = None;
    for _ in 0..5 {
        let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        // Ensure the block is stored
        if let Some((_, wb)) = store.iter::<WorkerBlocks>().next() {
            block = Some(wb);
            break;
        }
    }
    let block = block.unwrap();

    // ensure block validator succeeds
    let block_validator = BlockValidator::new(blockchain_db.clone(), 1_000_000, 30_000_000);

    let valid_block_result = block_validator.validate_block(&block).await;
    assert!(valid_block_result.is_ok());

    // ensure expected transaction is in block
    let expected_block = WorkerBlock::new(
        vec![transaction1.clone(), transaction2.clone(), transaction3.clone()],
        block.sealed_header().clone(),
    );
    let block_txs = block.transactions();
    assert_eq!(block_txs, expected_block.transactions());

    // ensure enough time passes for store to pass
    let _ = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let first_batch = store.iter::<WorkerBlocks>().next();
    debug!("first batch? {:?}", first_batch);

    // Ensure the batch is stored
    let batch_from_store = store
        .get::<WorkerBlocks>(&expected_block.digest())
        .expect("store searched for batch")
        .expect("batch in store");
    let sealed_header_from_batch_store = batch_from_store.sealed_header();
    assert_eq!(sealed_header_from_batch_store.beneficiary, address);

    // txpool should be empty after mining
    // test_make_block_no_ack_txs_in_pool_still tests for txs in pool without mining event
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 0);
}

/// Create 4 transactions.
///
/// First 3 mined in first block.
/// Before a canonical state change, mine the 4th transaction in the next block.
#[tokio::test]
async fn test_block_builder_produces_valid_blocks() {
    // reth_tracing::init_test_tracing();
    //
    //=== Execution Layer
    //
    // adiri genesis with TxFactory funded
    let genesis = test_genesis();

    // let genesis = genesis.extend_accounts(account);
    let head_timestamp = genesis.timestamp;
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // temp db
    let db = create_test_rw_db();

    // provider
    let factory = ProviderFactory::new(
        Arc::clone(&db),
        Arc::clone(&chain),
        StaticFileProvider::read_write(tempdir_path())
            .expect("static file provider read write created with tempdir path"),
    );

    let genesis_hash = init_genesis(factory.clone()).expect("init genesis");
    let blockchain_db = BlockchainProvider::new(factory, Arc::new(NoopBlockchainTree::default()))
        .expect("test blockchain provider");

    debug!("genesis hash: {genesis_hash:?}");

    // task manger
    let manager = TaskManager::current();
    let executor = manager.executor();

    // TODO: abstract the txpool creation to call in engine::inner and here
    //
    // txpool
    let blob_store = InMemoryBlobStore::default();
    let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
        .with_head_timestamp(head_timestamp)
        .with_additional_tasks(1)
        .build_with_tasks(blockchain_db.clone(), executor, blob_store.clone());

    let txpool =
        reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
    let address = Address::from(U160::from(333));
    let tx_pool_latest = txpool.block_info();
    let tip = SealedBlock::new(chain.sealed_genesis_header(), BlockBody::default());

    let latest_canon_state = LastCanonicalUpdate {
        tip, // genesis
        pending_block_base_fee: tx_pool_latest.pending_basefee,
        pending_block_blob_fee: tx_pool_latest.pending_blob_fee,
    };

    let (to_worker, mut from_block_builder) = tokio::sync::mpsc::channel(2);
    let max_block_gas_limit = 30_000_000;
    let max_block_bytes_size = 1_000_000;

    // build execution block proposer
    let block_builder = BlockBuilder::new(
        blockchain_db.clone(),
        txpool.clone(),
        blockchain_db.canonical_state_stream(),
        latest_canon_state,
        to_worker,
        address,
        Duration::from_secs(1),
        max_block_gas_limit,  // 30mil gas limit
        max_block_bytes_size, // 1MB size
    );

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();

    // create 3 transactions
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction2 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction3 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction1.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction2.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction3.hash());

    // txpool size
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 3);

    // spawn block_builder once worker is ready
    let _block_builder = tokio::spawn(Box::pin(block_builder));

    //
    //=== Test block flow
    //

    // plenty of time for block production
    let duration = std::time::Duration::from_secs(5);

    // receive next block
    let (first_block, ack) = timeout(duration, from_block_builder.recv())
        .await
        .expect("block builder's sender didn't drop")
        .expect("worker block was built");

    // submit new transaction before sending ack
    let expected_tx_hash = tx_factory
        .create_and_submit_eip1559_pool_tx(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
            &txpool,
        )
        .await;

    // assert all 4 txs in pending pool
    let pending_pool_len = txpool.pool_size().pending;
    assert_eq!(pending_pool_len, 4);

    // send ack to mine first 3 transactions
    let _ = ack.send(Ok(()));

    // validate first block
    let block_validator =
        BlockValidator::new(blockchain_db.clone(), max_block_bytes_size, max_block_gas_limit);

    let valid_block_result = block_validator.validate_block(&first_block).await;
    assert!(valid_block_result.is_ok());

    // ensure expected transaction is in block
    let expected_block = WorkerBlock::new(
        vec![transaction1.clone(), transaction2.clone(), transaction3.clone()],
        first_block.sealed_header().clone(),
    );
    let block_txs = first_block.transactions();
    assert_eq!(block_txs, expected_block.transactions());

    // receive next block
    let (block, ack) = timeout(duration, from_block_builder.recv())
        .await
        .expect("block builder's sender didn't drop")
        .expect("worker block was built");
    // send ack to mine block
    let _ = ack.send(Ok(()));

    // validate second block
    let valid_block_result = block_validator.validate_block(&block).await;
    assert!(valid_block_result.is_ok());

    // assert only transaction in block
    assert_eq!(block.transactions().len(), 1);

    // confirm 4th transaction hash matches one submitted
    let tx = block.transactions().first().expect("block transactions length is one");
    assert_eq!(tx.hash(), expected_tx_hash);

    // yield to try and give pool a chance to update
    tokio::task::yield_now().await;

    // assert all transactions mined
    let pending_pool_len = txpool.pool_size().pending;
    assert_eq!(pending_pool_len, 0);
}

/// Create 4 transactions.
///
/// First 3 mined in first block.
/// Before a canonical state change, mine the 4th transaction in the next block.
#[tokio::test]
async fn test_canonical_notification_updates_pool() {
    // reth_tracing::init_test_tracing();
    //
    //=== Execution Layer
    //
    // adiri genesis with TxFactory funded
    let genesis = test_genesis();
    // let first_round_blocks = tn_types::test_utils::batches(4); // create 4 batches
    // let (genesis, _txs, _signers) =
    //     seeded_genesis_from_random_batches(genesis, first_round_blocks.iter());

    let head_timestamp = genesis.timestamp;
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // temp db
    let db = create_test_rw_db();

    // provider
    let factory = ProviderFactory::new(
        Arc::clone(&db),
        Arc::clone(&chain),
        StaticFileProvider::read_write(tempdir_path())
            .expect("static file provider read write created with tempdir path"),
    );

    let genesis_hash = init_genesis(factory.clone()).expect("init genesis");

    // TODO: figure out a better way to ensure this matches engine::inner::new
    let evm_config = EthEvmConfig::default();
    let executor = EthExecutorProvider::new(Arc::clone(&chain), evm_config);
    let auto_consensus: Arc<dyn Consensus> = Arc::new(AutoSealConsensus::new(Arc::clone(&chain)));
    let tree_config = BlockchainTreeConfig::default();
    let tree_externals =
        TreeExternals::new(factory.clone(), auto_consensus.clone(), executor.clone());
    let tree = BlockchainTree::new(tree_externals, tree_config, PruneModes::none())
        .expect("new blockchain tree");

    let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

    let blockchain_db =
        BlockchainProvider::new(factory, blockchain_tree).expect("test blockchain provider");

    debug!("genesis hash: {genesis_hash:?}");

    // task manger
    let manager = TaskManager::current();
    let executor = manager.executor();

    // TODO: abstract the txpool creation to call in engine::inner and here
    //
    // txpool
    let blob_store = InMemoryBlobStore::default();
    let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
        .with_head_timestamp(head_timestamp)
        .with_additional_tasks(1)
        .build_with_tasks(blockchain_db.clone(), executor, blob_store.clone());

    let txpool =
        reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
    let address = Address::from(U160::from(333));
    let tx_pool_latest = txpool.block_info();
    let tip = SealedBlock::new(chain.sealed_genesis_header(), BlockBody::default());

    let latest_canon_state = LastCanonicalUpdate {
        tip, // genesis
        pending_block_base_fee: tx_pool_latest.pending_basefee,
        pending_block_blob_fee: tx_pool_latest.pending_blob_fee,
    };

    let (to_worker, mut from_block_builder) = tokio::sync::mpsc::channel(2);
    let max_block_gas_limit = 30_000_000;
    let max_block_bytes_size = 1_000_000;

    // build execution block proposer
    let block_builder = BlockBuilder::new(
        blockchain_db.clone(),
        txpool.clone(),
        blockchain_db.canonical_state_stream(),
        latest_canon_state,
        to_worker,
        address,
        Duration::from_secs(1),
        max_block_gas_limit,  // 30mil gas limit
        max_block_bytes_size, // 1MB size
    );

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();

    // create 3 transactions
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction2 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction3 = tx_factory.create_eip1559(
        chain.clone(),
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    // let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
    // assert_matches!(added_result, hash if hash == transaction1.hash());

    // let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    // assert_matches!(added_result, hash if hash == transaction2.hash());

    // let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    // assert_matches!(added_result, hash if hash == transaction3.hash());

    // txpool size
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 0);

    // spawn block_builder once worker is ready
    let _block_builder = tokio::spawn(Box::pin(block_builder));

    //
    //=== Test block flow
    //

    // submit new transaction before sending ack
    let _ = tx_factory
        .create_and_submit_eip1559_pool_tx(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
            &txpool,
        )
        .await;

    // assert all 4 txs in pending pool
    let queued_pool_len = txpool.pool_size().queued;
    assert_eq!(queued_pool_len, 1);

    // ensure expected transaction is in block
    let mut first_block = WorkerBlock::new(
        vec![transaction1.clone(), transaction2.clone(), transaction3.clone()],
        SealedHeader::default(),
    );

    execute_test_worker_block(&mut first_block, &chain.sealed_genesis_header());

    // execute worker block - create output for consistency
    let block_digests = VecDeque::from([first_block.digest()]);
    let output = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![Certificate::default()],
            Certificate::default(),
            0,
            ReputationScores::default(),
            None,
        )
        .into(),
        blocks: vec![vec![first_block]],
        beneficiary: address,
        block_digests,
    };

    // execute output to trigger canonical update
    let args = BuildArguments::new(blockchain_db.clone(), output, chain.sealed_genesis_header());
    let _final_header = execute_consensus_output(evm_config, args).expect("output executed");

    // let output_digest: B256 = output.digest().into();
    // let ommers = output.ommers();
    // let ommers_root = proofs::calculate_ommers_root(&ommers);
    // let mix_hash = output_digest ^ sealed_block_with_senders.hash();
    // let withdrawals =
    //     sealed_block_with_senders.withdrawals.clone().unwrap_or_else(||
    // Withdrawals::new(vec![])); let payload_attributes = TNPayloadAttributes::new(
    //     chain.sealed_genesis_header(),
    //     ommers,
    //     ommers_root,
    //     0, // index
    //     sealed_block_with_senders.hash(),
    //     &output,
    //     output_digest,
    //     sealed_block_with_senders.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE),
    //     sealed_block_with_senders.gas_limit,
    //     mix_hash,
    //     withdrawals,
    // );
    // let payload = TNPayload::new(payload_attributes);
    // let executed_block = build_block_from_batch_payload(
    //     &evm_config,
    //     payload,
    //     &blockchain_db,
    //     chain,
    //     sealed_block_with_senders,
    // )
    // .expect("engine block from worker block");

    // let next_canonical_hash = executed_block.block.hash();
    // // apply canonical tip update
    // blockchain_db
    //     .insert_block(executed_block, BlockValidationKind::SkipStateRootValidation)
    //     .expect("worker block made canonical");

    // // this sends update and apply canonical changes to pool
    // blockchain_db.make_canonical(next_canonical_hash).expect("worker block is canonical");

    // sleep to ensure canonical update received before ack
    let _ = tokio::time::sleep(Duration::from_secs(1)).await;

    // assert 4th transaction demoted to queued pool
    let pool_size = txpool.pool_size();
    println!("poolsize: {pool_size:?}");
    assert_eq!(pool_size.queued, 0);
    assert_eq!(pool_size.pending, 1);

    // UPdate comment for test:
    //
    // - add 4th transaction to pool so it's queued
    // - execute a canonical block that unlocks queued transaction
    // - assert the queued tx is now pending
    //

    // plenty of time for block production
    let duration = std::time::Duration::from_secs(5);

    // receive next block
    let (first_block, ack) = timeout(duration, from_block_builder.recv())
        .await
        .expect("block builder's sender didn't drop")
        .expect("worker block was built");

    // send ack to mine transaction
    let _ = ack.send(Ok(()));

    // validate block
    let block_validator =
        BlockValidator::new(blockchain_db, max_block_bytes_size, max_block_gas_limit);

    let valid_block_result = block_validator.validate_block(&first_block).await;
    assert!(valid_block_result.is_ok());

    // yield to try and give pool a chance to update
    tokio::task::yield_now().await;

    // assert pool empty
    let pool_size = txpool.pool_size();
    assert_eq!(pool_size.queued, 0);
    assert_eq!(pool_size.pending, 0);
}
