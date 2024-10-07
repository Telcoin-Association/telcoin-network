//! Block provider (EL) collects transactions
//! and creates a block.
//!
//! Block provider (CL) receives the block from EL
//! and forwards it to the Quorum Waiter.

use assert_matches::assert_matches;
use narwhal_network::client::NetworkClient;
use narwhal_network_types::MockWorkerToPrimary;
use narwhal_typed_store::{open_db, tables::WorkerBlocks, traits::Database};
use narwhal_worker::{
    metrics::WorkerMetrics,
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
    BlockProvider,
};
use reth_blockchain_tree::noop::NoopBlockchainTree;
use reth_chainspec::ChainSpec;
use reth_db::test_utils::{create_test_rw_db, tempdir_path};
use reth_db_common::init::init_genesis;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{alloy_primitives::U160, Address, Bytes, U256};
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    ProviderFactory,
};
use reth_tracing::init_test_tracing;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, PoolConfig, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_block_proposer::{BlockProposerBuilder, MiningMode};
use tn_block_validator::{BlockValidation, BlockValidator};
use tn_types::{
    test_utils::{get_gas_price, test_genesis, TransactionFactory},
<<<<<<< HEAD
    PendingWorkerBlock, PreSubscribedBroadcastSender, WorkerBlock,
=======
    Consensus, PendingWorkerBlock, WorkerBlock,
>>>>>>> main
};
use tokio::sync::watch;
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
    init_test_tracing();

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
    let block_provider =
        BlockProvider::new(0, qw.clone(), Arc::new(node_metrics), network_client, store.clone());

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
    let address = Address::from(U160::from(333));

    let evm_config = EthEvmConfig::default();
    let block_executor = EthExecutorProvider::new(chain.clone(), evm_config);
    let (tx, _rx) = watch::channel(PendingWorkerBlock::default());

    // build execution block proposer
    let task = BlockProposerBuilder::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        txpool.clone(),
        mining_mode,
        address,
        block_executor.clone(),
        tx,
        block_provider.blocks_rx(),
    )
    .build();

    let gas_price = get_gas_price(&blockchain_db);
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
    let mut tx_factory = TransactionFactory::new();
    println!("\n\ncreating first transaction....\n\n");

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

    // spawn mining task once worker is ready
    let _mining_task = tokio::spawn(Box::pin(task));

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
    let expected_block =
        WorkerBlock::new(vec![transaction1.clone()], block.sealed_header().clone());
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

    // txpool size after mining
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 2);

    // ensure tx1 is removed
    assert!(!txpool.contains(transaction1.hash_ref()));
    // ensure tx2 & tx3 are in the pool still
    assert!(txpool.contains(transaction2.hash_ref()));
    assert!(txpool.contains(transaction3.hash_ref()));
}
