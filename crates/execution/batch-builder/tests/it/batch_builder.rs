//! Batch builder (EL) collects transactions and creates batches.
//!
//! Batch builder (CL) receives the batch from EL and forwards it to the Quorum Waiter for votes
//! from peers.

use assert_matches::assert_matches;
use reth_blockchain_tree::{
    noop::NoopBlockchainTree, BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree,
    TreeExternals,
};
use reth_chainspec::ChainSpec;
use reth_db::test_utils::{create_test_rw_db, tempdir_path};
use reth_db_common::init::init_genesis;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{alloy_primitives::U160, Address, BlockBody, Bytes, SealedBlock, U256};
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
use tn_batch_builder::{test_utils::execute_test_batch, BatchBuilder};
use tn_batch_validator::BatchValidator;
use tn_engine::execute_consensus_output;
use tn_network::local::LocalNetwork;
use tn_network_types::MockWorkerToPrimary;
use tn_storage::{open_db, tables::Batches, traits::Database};
use tn_test_utils::{get_gas_price, test_genesis, TransactionFactory};
use tn_types::{
    AutoSealConsensus, Batch, BatchValidation, BuildArguments, Certificate, CommittedSubDag,
    Consensus, ConsensusHeader, ConsensusOutput, LastCanonicalUpdate, ReputationScores,
    SealedBatch,
};
use tn_worker::{
    metrics::WorkerMetrics,
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
    BatchProvider,
};
use tokio::time::timeout;
use tracing::debug;

#[derive(Clone, Debug)]
struct TestMakeBlockQuorumWaiter();
impl QuorumWaiterTrait for TestMakeBlockQuorumWaiter {
    fn verify_batch(
        &self,
        _batch: SealedBatch,
        _timeout: Duration,
    ) -> tokio::task::JoinHandle<Result<(), QuorumWaiterError>> {
        tokio::spawn(async move { Ok(()) })
    }
}

#[tokio::test]
async fn test_make_batch_el_to_cl() {
    //
    //=== Consensus Layer
    //

    let network_client = LocalNetwork::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let node_metrics = WorkerMetrics::default();

    // Mock the primary client to always succeed.
    let mut mock_server = MockWorkerToPrimary::new();
    mock_server.expect_report_own_batch().returning(|_| Ok(anemo::Response::new(())));
    network_client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    let qw = TestMakeBlockQuorumWaiter();
    let timeout = Duration::from_secs(5);
    let batch_provider = BatchProvider::new(
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

    // build execution batch proposer
    let batch_builder = BatchBuilder::new(
        blockchain_db.clone(),
        txpool.clone(),
        blockchain_db.canonical_state_stream(),
        latest_canon_state,
        batch_provider.batches_tx(),
        address,
        Duration::from_secs(1),
    );

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();

    // create 3 transactions
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        None,
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );
    debug!("transaction 1: {transaction1:?}");

    let transaction2 = tx_factory.create_eip1559(
        chain.clone(),
        None,
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );
    debug!("transaction 2: {transaction2:?}");

    let transaction3 = tx_factory.create_eip1559(
        chain.clone(),
        None,
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

    // spawn batch_builder once worker is ready
    let _batch_builder = tokio::spawn(Box::pin(batch_builder));

    //
    //=== Test batch flow
    //

    // wait for new batch
    let mut sealed_batch = None;
    for _ in 0..5 {
        let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        // Ensure the batch is stored
        if let Some((digest, wb)) = store.iter::<Batches>().next() {
            sealed_batch = Some(SealedBatch::new(wb, digest));
            break;
        }
    }
    let sealed_batch = sealed_batch.unwrap();

    // ensure batch validator succeeds
    let batch_validator = BatchValidator::new(blockchain_db.clone());

    let valid_batch_result = batch_validator.validate_batch(sealed_batch.clone());
    assert!(valid_batch_result.is_ok());

    // ensure expected transaction is in batch
    let expected_batch = Batch {
        transactions: vec![transaction1.clone(), transaction2.clone(), transaction3.clone()],
        received_at: None,
        ..*sealed_batch.batch()
    }
    .seal_slow();

    let batch_txs = sealed_batch.batch().transactions();
    assert_eq!(batch_txs, expected_batch.batch().transactions());

    // ensure enough time passes for store to pass
    let _ = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let first_batch = store.iter::<Batches>().next();
    debug!("first batch? {:?}", first_batch);

    // Ensure the batch is stored
    let batch_from_store = store
        .get::<Batches>(&expected_batch.digest())
        .expect("store searched for batch")
        .expect("batch in store");
    assert_eq!(batch_from_store.beneficiary, address);

    // txpool should be empty after mining
    // test_make_batch_no_ack_txs_in_pool_still tests for txs in pool without mining event
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 0);
}

/// Create 4 transactions.
///
/// First 3 mined in first batch.
/// Before a canonical state change, mine the 4th transaction in the next batch.
#[tokio::test]
async fn test_batch_builder_produces_valid_batchess() {
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

    let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

    // build execution batch proposer
    let batch_builder = BatchBuilder::new(
        blockchain_db.clone(),
        txpool.clone(),
        blockchain_db.canonical_state_stream(),
        latest_canon_state,
        to_worker,
        address,
        Duration::from_secs(1),
    );

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();

    // create 3 transactions
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        None,
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction2 = tx_factory.create_eip1559(
        chain.clone(),
        None,
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction3 = tx_factory.create_eip1559(
        chain.clone(),
        None,
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

    // spawn batch_builder once worker is ready
    let _batch_builder = tokio::spawn(Box::pin(batch_builder));

    //
    //=== Test batch flow
    //

    // plenty of time for batch production
    let duration = std::time::Duration::from_secs(5);

    // receive next batch
    let (first_batch, ack) = timeout(duration, from_batch_builder.recv())
        .await
        .expect("batch builder's sender didn't drop")
        .expect("batch was built");

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

    // validate first batch
    let batch_validator = BatchValidator::new(blockchain_db.clone());

    let valid_batch_result = batch_validator.validate_batch(first_batch.clone());
    assert!(valid_batch_result.is_ok());

    // ensure expected transaction is in batch
    let expected_batch = Batch {
        transactions: vec![transaction1.clone(), transaction2.clone(), transaction3.clone()],
        received_at: None,
        ..*first_batch.batch()
    };
    let batch_txs = first_batch.batch().transactions();
    assert_eq!(batch_txs, expected_batch.transactions());

    // receive next block
    let (next_batch, ack) = timeout(duration, from_batch_builder.recv())
        .await
        .expect("block builder's sender didn't drop")
        .expect("batch was built");
    // send ack to mine block
    let _ = ack.send(Ok(()));

    // validate second block
    let valid_batch_result = batch_validator.validate_batch(next_batch.clone());
    assert!(valid_batch_result.is_ok());

    // assert only transaction in block
    assert_eq!(next_batch.batch().transactions().len(), 1);

    // confirm 4th transaction hash matches one submitted
    let tx = next_batch.batch().transactions().first().expect("block transactions length is one");
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

    let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

    // build execution block proposer
    let batch_builder = BatchBuilder::new(
        blockchain_db.clone(),
        txpool.clone(),
        blockchain_db.canonical_state_stream(),
        latest_canon_state,
        to_worker,
        address,
        Duration::from_secs(1),
    );

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();

    // create 3 transactions
    let transaction1 = tx_factory.create_eip1559(
        chain.clone(),
        None,
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction2 = tx_factory.create_eip1559(
        chain.clone(),
        None,
        gas_price,
        Some(Address::ZERO),
        value, // 1 TEL
        Bytes::new(),
    );

    let transaction3 = tx_factory.create_eip1559(
        chain.clone(),
        None,
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

    // spawn batch_builder once worker is ready
    let _batch_builder = tokio::spawn(Box::pin(batch_builder));

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

    // ensure expected transaction is in batch
    let mut first_batch = Batch {
        transactions: vec![transaction1.clone(), transaction2.clone(), transaction3.clone()],
        ..Default::default()
    };

    execute_test_batch(&mut first_batch, &chain.sealed_genesis_header());

    // execute batch - create output for consistency
    let batch_digests = VecDeque::from([first_batch.digest()]);
    let output = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![Certificate::default()],
            Certificate::default(),
            0,
            ReputationScores::default(),
            None,
        )
        .into(),
        batches: vec![vec![first_batch]],
        beneficiary: address,
        batch_digests,
        parent_hash: ConsensusHeader::default().digest(),
        number: 0,
    };

    // execute output to trigger canonical update
    let args = BuildArguments::new(blockchain_db.clone(), output, chain.sealed_genesis_header());
    let _final_header = execute_consensus_output(evm_config, args).expect("output executed");

    // sleep to ensure canonical update received before ack
    let _ = tokio::time::sleep(Duration::from_secs(1)).await;

    // assert 4th transaction demoted to queued pool
    let pool_size = txpool.pool_size();
    println!("poolsize: {pool_size:?}");
    assert_eq!(pool_size.queued, 0);
    assert_eq!(pool_size.pending, 1);

    // plenty of time for block production
    let duration = std::time::Duration::from_secs(5);

    // receive next block
    let (first_batch, ack) = timeout(duration, from_batch_builder.recv())
        .await
        .expect("block builder's sender didn't drop")
        .expect("batch was built");

    // send ack to mine transaction
    let _ = ack.send(Ok(()));

    // validate batch
    let batch_validator = BatchValidator::new(blockchain_db);

    let valid_batch_result = batch_validator.validate_batch(first_batch.clone());
    assert!(valid_batch_result.is_ok());

    // yield to try and give pool a chance to update
    tokio::task::yield_now().await;

    // assert pool empty
    let pool_size = txpool.pool_size();
    assert_eq!(pool_size.queued, 0);
    assert_eq!(pool_size.pending, 0);
}
