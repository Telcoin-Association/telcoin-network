//! Batch builder (EL) collects transactions and creates batches.
//!
//! Batch builder (CL) receives the batch from EL and forwards it to the Quorum Waiter for votes
//! from peers.

use assert_matches::assert_matches;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_batch_builder::{test_utils::execute_test_batch, BatchBuilder};
use tn_batch_validator::BatchValidator;
use tn_engine::execute_consensus_output;
use tn_network_types::{local::LocalNetwork, MockWorkerToPrimary};
use tn_reth::{
    payload::BuildArguments, recover_raw_transaction, test_utils::TransactionFactory,
    RethChainSpec, RethEnv,
};
use tn_storage::{open_db, tables::Batches};
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    test_genesis, Address, Batch, BatchValidation, Bytes, Certificate, CommittedSubDag,
    ConsensusOutput, Database, Encodable2718 as _, ReputationScores, SealedBatch, TaskManager,
    U160, U256,
};
use tn_worker::{
    metrics::WorkerMetrics, test_utils::TestMakeBlockQuorumWaiter, Worker, WorkerNetworkHandle,
};
use tokio::time::timeout;
use tracing::debug;

#[tokio::test]
async fn test_make_batch_el_to_cl() {
    let tmp_dir = TempDir::new().expect("temp dir");
    let mut task_manager = TaskManager::default();
    //
    //=== Consensus Layer
    //

    let network_client = LocalNetwork::new_with_empty_id();
    let store = open_db(tmp_dir.path().join("c-db"));
    let node_metrics = WorkerMetrics::default();

    // Mock the primary client to always succeed.
    let mock_server = MockWorkerToPrimary();
    network_client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    let qw = TestMakeBlockQuorumWaiter::new_test();
    let timeout = Duration::from_secs(5);
    let batch_provider = Worker::new(
        0,
        Some(qw.clone()),
        Arc::new(node_metrics),
        network_client,
        store.clone(),
        timeout,
        WorkerNetworkHandle::new_for_test(),
        &mut task_manager,
    );

    //
    //=== Execution Layer
    //

    // adiri genesis with TxFactory funded
    let genesis = test_genesis();

    // let genesis = genesis.extend_accounts(account);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager).unwrap();
    let txpool = reth_env.init_txn_pool().unwrap();
    let address = Address::from(U160::from(333));

    // build execution block proposer
    let batch_builder = BatchBuilder::new(
        &reth_env,
        txpool.clone(),
        batch_provider.batches_tx(),
        address,
        Duration::from_secs(1),
        task_manager.get_spawner(),
        0,
        BaseFeeContainer::default(),
    );

    let gas_price = reth_env.get_gas_price().unwrap();
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
    assert_matches!(added_result, hash if &hash == transaction1.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if &hash == transaction2.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if &hash == transaction3.hash());

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
    let batch_validator =
        BatchValidator::new(reth_env.clone(), Some(txpool.clone()), 0, BaseFeeContainer::default());

    let valid_batch_result = batch_validator.validate_batch(sealed_batch.clone());
    assert!(valid_batch_result.is_ok());

    // ensure expected transaction is in batch
    let expected_batch = Batch {
        transactions: vec![
            transaction1.encoded_2718(),
            transaction2.encoded_2718(),
            transaction3.encoded_2718(),
        ],
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
    //
    //=== Execution Layer
    //
    // adiri genesis with TxFactory funded
    let genesis = test_genesis();

    // let genesis = genesis.extend_accounts(account);
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let address = Address::from(U160::from(333));
    let tmp_dir = TempDir::new().unwrap();
    let task_manager = TaskManager::default();
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager).unwrap();
    let txpool = reth_env.init_txn_pool().unwrap();

    let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

    // build execution block proposer
    let batch_builder = BatchBuilder::new(
        &reth_env,
        txpool.clone(),
        to_worker,
        address,
        Duration::from_secs(1),
        task_manager.get_spawner(),
        0,
        BaseFeeContainer::default(),
    );

    let gas_price = reth_env.get_gas_price().unwrap();
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
    assert_matches!(added_result, hash if &hash == transaction1.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if &hash == transaction2.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if &hash == transaction3.hash());

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
            txpool.clone(),
        )
        .await;

    // assert all 4 txs in pending pool
    let pending_pool_len = txpool.pool_size().pending;
    assert_eq!(pending_pool_len, 4);

    // send ack to mine first 3 transactions
    let _ = ack.send(Ok(()));

    // validate first batch
    let batch_validator =
        BatchValidator::new(reth_env.clone(), Some(txpool.clone()), 0, BaseFeeContainer::default());

    let valid_batch_result = batch_validator.validate_batch(first_batch.clone());
    assert!(valid_batch_result.is_ok());

    // ensure expected transaction is in batch
    let expected_batch = Batch {
        transactions: vec![
            transaction1.encoded_2718(),
            transaction2.encoded_2718(),
            transaction3.encoded_2718(),
        ],
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
    let tx_bytes =
        next_batch.batch().transactions().first().expect("block transactions length is one");
    let tx = recover_raw_transaction(tx_bytes).expect("recover raw tx for test");
    assert_eq!(tx.hash(), &expected_tx_hash);

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
    //
    //=== Execution Layer
    //
    // adiri genesis with TxFactory funded
    let genesis = test_genesis();
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let tmp_dir = TempDir::new().unwrap();
    let task_manager = TaskManager::default();
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager).unwrap();
    let txpool = reth_env.init_txn_pool().unwrap();
    let address = Address::from(U160::from(333));

    let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

    // build execution block proposer
    let batch_builder = BatchBuilder::new(
        &reth_env,
        txpool.clone(),
        to_worker,
        address,
        Duration::from_secs(1),
        task_manager.get_spawner(),
        0,
        BaseFeeContainer::default(),
    );

    let gas_price = reth_env.get_gas_price().unwrap();
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
            txpool.clone(),
        )
        .await;

    // assert all 4 txs in pending pool
    let queued_pool_len = txpool.pool_size().queued;
    assert_eq!(queued_pool_len, 1);

    // ensure expected transaction is in batch
    let mut first_batch = Batch {
        transactions: vec![
            transaction1.encoded_2718(),
            transaction2.encoded_2718(),
            transaction3.encoded_2718(),
        ],
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
        beneficiary: address,
        early_finalize: true,
        batch_digests,
        batches: vec![vec![first_batch]],
        ..Default::default()
    };

    // execute output to trigger canonical update
    let args = BuildArguments::new(reth_env.clone(), output, chain.sealed_genesis_header());
    let _final_header =
        execute_consensus_output(args, GasAccumulator::default()).expect("output executed");

    // sleep to ensure canonical update received before ack
    let _ = tokio::time::sleep(Duration::from_secs(1)).await;

    // assert 4th transaction demoted to queued pool
    let pool_size = txpool.pool_size();
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
    let batch_validator =
        BatchValidator::new(reth_env.clone(), Some(txpool.clone()), 0, BaseFeeContainer::default());

    let valid_batch_result = batch_validator.validate_batch(first_batch.clone());
    assert!(valid_batch_result.is_ok());

    // yield to try and give pool a chance to update
    tokio::task::yield_now().await;

    // assert pool empty
    let pool_size = txpool.pool_size();
    assert_eq!(pool_size.queued, 0);
    assert_eq!(pool_size.pending, 0);
}
