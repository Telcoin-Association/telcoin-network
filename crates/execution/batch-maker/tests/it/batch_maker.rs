//! Batch maker (EL) collects transactions
//! and creates a batch.
//!
//! Batch maker (CL) receives the batch from EL
//! and forwards it to the Quorum Waiter.

use assert_matches::assert_matches;
use fastcrypto::hash::Hash;
use narwhal_network::client::NetworkClient;
use narwhal_network_types::MockWorkerToPrimary;
use narwhal_typed_store::{open_db, tables::Batches, traits::Database};
use narwhal_worker::{metrics::WorkerMetrics, BatchMaker, NUM_SHUTDOWN_RECEIVERS};
use reth::{beacon_consensus::EthBeaconConsensus, tasks::TaskManager};
use reth_blockchain_tree::noop::NoopBlockchainTree;
use reth_chainspec::ChainSpec;
use reth_db::test_utils::{create_test_rw_db, tempdir_path};
use reth_db_common::init::init_genesis;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{alloy_primitives::U160, Address, Bytes, TransactionSigned, U256};
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
use tn_batch_maker::{BatchMakerBuilder, MiningMode};
use tn_batch_validator::{BatchValidation, BatchValidator};
use tn_types::{
    test_utils::{get_gas_price, test_genesis, TransactionFactory},
    Batch, BatchAPI, Consensus, MetadataAPI, PreSubscribedBroadcastSender,
};
use tokio::time::timeout;
use tracing::debug;

#[tokio::test]
async fn test_make_batch_el_to_cl() {
    init_test_tracing();

    // worker channel
    let (to_worker, rx_batch_maker) = tn_types::test_channel!(1);

    //
    //=== Consensus Layer
    //

    let network_client = NetworkClient::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);
    let (tx_quorum_waiter, mut rx_quorum_waiter) = tn_types::test_channel!(1);
    let node_metrics = WorkerMetrics::default();

    // Mock the primary client to always succeed.
    let mut mock_server = MockWorkerToPrimary::new();
    mock_server.expect_report_own_batch().returning(|_| Ok(anemo::Response::new(())));
    network_client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    // Spawn a `BatchMaker` instance.
    let id = 0;
    let _batch_maker_handle = BatchMaker::spawn(
        id,
        /* max_batch_size */ 200,
        /* max_batch_delay */
        Duration::from_millis(1_000_000), // Ensure the timer is not triggered.
        tx_shutdown.subscribe(),
        rx_batch_maker,
        tx_quorum_waiter,
        Arc::new(node_metrics),
        network_client,
        store.clone(),
    );

    // worker's batch maker takes a long time to start
    tokio::task::yield_now().await;

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

    // build execution batch maker
    let task = BatchMakerBuilder::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        txpool.clone(),
        to_worker,
        mining_mode,
        address,
        block_executor.clone(),
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
    //=== Test batch flow
    //

    // wait for quorum waiter's channel to recv batch
    let too_long = Duration::from_secs(5);
    let (batch, resp) = timeout(too_long, rx_quorum_waiter.recv())
        .await
        .expect("new batch created within time")
        .expect("new batch is Some()");

    // ensure batch validator succeeds
    let consensus: Arc<dyn Consensus> = Arc::new(EthBeaconConsensus::new(chain.clone()));
    let batch_validator = BatchValidator::new(consensus, blockchain_db.clone(), block_executor);

    let valid_batch_result = batch_validator.validate_batch(&batch).await;
    assert!(valid_batch_result.is_ok());

    // ensure expected transaction is in batch
    let expected_batch = Batch::new(vec![transaction1.envelope_encoded().into()]);
    let batch_txs = batch.transactions();
    assert_eq!(batch_txs, expected_batch.transactions());

    // ack to CL batch maker
    assert!(resp.send(()).is_ok());

    // ensure batch transaction decodes correctly
    let batch_tx_bytes = batch_txs.first().cloned().expect("one tx in batch");
    let decoded_batch_tx = TransactionSigned::decode_enveloped(&mut batch_tx_bytes.as_ref())
        .expect("tx bytes are uncorrupted");
    assert_eq!(decoded_batch_tx, transaction1);

    // ensure enough time passes for store to pass
    let _ = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let first_batch = store.iter::<Batches>().next();
    debug!("first batch? {:?}", first_batch);

    // Ensure the batch is stored
    let batch_from_store = store
        .get::<Batches>(&expected_batch.digest())
        .expect("store searched for batch")
        .expect("batch in store");
    let sealed_header_from_batch_store = batch_from_store.versioned_metadata().sealed_header();
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
