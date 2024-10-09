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
use reth_blockchain_tree::noop::NoopBlockchainTree;
use reth_chainspec::ChainSpec;
use reth_db::test_utils::{create_test_rw_db, tempdir_path};
use reth_db_common::init::init_genesis;
use reth_primitives::{alloy_primitives::U160, Address, BlockBody, Bytes, SealedBlock, U256};
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    CanonStateSubscriptions, ProviderFactory,
};
use reth_tasks::TaskManager;
use reth_tracing::init_test_tracing;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, PoolConfig, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_block_builder::BlockBuilder;
use tn_block_validator::{BlockValidation, BlockValidator};
use tn_types::{
    test_utils::{get_gas_price, test_genesis, TransactionFactory},
    LastCanonicalUpdate, WorkerBlock,
};
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
        txpool.pending_transactions_listener(),
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
