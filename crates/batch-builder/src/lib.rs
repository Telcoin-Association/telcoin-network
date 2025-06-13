// SPDX-License-Identifier: MIT or Apache-2.0
//! The block builder maintains the transaction pool and builds the next block.
//!
//! The block builder listens for canonical state changes from the engine and updates the
//! transaction pool. These updates move transactions to the correct sub-pools. Only transactions in
//! the pending pool are considered for the next block.
//!
//! Upon successfully building the next block, the block builder forwards to the worker's block
//! provider. The worker's block provider reliably broadcasts the block and tries to reach quorum
//! within a time limit. If quorum fails, the block builder receives the error and does not mine the
//! transactions. If quorum is reached, the transactions are mined and removed from the pending
//! pool. When this task removes transactions from the pending pool, it uses the current canonical
//! tip and basefee calculated for the round. Only the engine's canonical updates affect the pool's
//! tracked `tip`, basefee, and blob fees sorting transactions into sub-pools.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub use batch::{build_batch, BatchBuilderOutput};
use error::{BatchBuilderError, BatchBuilderResult};
use futures_util::{FutureExt, StreamExt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tn_reth::{CanonStateNotificationStream, RethEnv, TxPool as _, WorkerTxPool};
use tn_types::{
    error::BlockSealError, gas_accumulator::BaseFeeContainer, Address, BatchBuilderArgs,
    BatchSender, PendingBatchConfig, SealedBlock, TaskSpawner, TxHash, WorkerId,
};
use tokio::{sync::oneshot, time::Interval};
use tracing::{debug, error, warn};

mod batch;
mod error;
#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Type alias for the blocking task that locks the tx pool and builds the next batch.
type BuildResult = oneshot::Receiver<BatchBuilderResult<Vec<TxHash>>>;

/// The type that builds blocks for workers to propose.
///
/// This is a future that:
/// - listens for canonical state changes and updates the tx pool
/// - polls the transaction pool for pending transactions
///     - tries to build the next batch when there transactions are available
#[derive(Debug)]
pub struct BatchBuilder {
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<BuildResult>,
    /// The transaction pool with pending transactions.
    pool: WorkerTxPool,
    /// The sending side to the worker's batch maker.
    ///
    /// Sending the new block through this channel triggers a broadcast to all peers.
    ///
    /// The worker's block maker sends an ack once the block has been stored in db
    /// which guarantees the worker will attempt to broadcast the new block until
    /// quorum is reached.
    to_worker: BatchSender,
    /// The address for batch's beneficiary.
    address: Address,
    /// Maximum amount of time to wait before querying block builds.
    ///
    /// This interval wakes the task periodically to check on the progress of the latest built
    /// block and the pending transaction pool.
    max_delay_interval: Interval,

    /// This channel will receive a header on canonical update.  We use it to wakeup the future and
    /// save the canonical update.
    state_changed: CanonStateNotificationStream,
    /// The last canonical update, saved when state_changed sends a new update.
    last_canonical_update: SealedBlock,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// Worker id this batch builder belongs too.
    worker_id: WorkerId,
    /// The current base fee for this worker.
    base_fee: BaseFeeContainer,
}

impl BatchBuilder {
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reth_env: &RethEnv,
        pool: WorkerTxPool,
        to_worker: BatchSender,
        address: Address,
        max_delay: Duration,
        task_spawner: TaskSpawner,
        worker_id: WorkerId,
        base_fee: BaseFeeContainer,
    ) -> Self {
        let max_delay_interval = tokio::time::interval(max_delay);
        let state_changed = reth_env.canonical_block_stream();
        let last_canonical_update = Self::latest_canon_block(reth_env);
        Self {
            pending_task: None,
            pool,
            to_worker,
            address,
            max_delay_interval,
            state_changed,
            last_canonical_update,
            task_spawner,
            worker_id,
            base_fee,
        }
    }

    /// Spawns a task to build the batch and proposer to peers.
    ///
    /// This approach allows the block builder to yield back to the runtime while mining blocks.
    ///
    /// The task performs the following actions:
    /// - create a block
    /// - send the block to worker's block proposer
    /// - wait for ack that quorum was reached
    /// - convert result to fatal/non-fatal
    /// - return result
    ///
    /// Workers only propose one block at a time.
    fn spawn_execution_task(&self) -> BuildResult {
        let pool = self.pool.clone();
        let to_worker = self.to_worker.clone();

        // configure params for next building the next batch
        let config = PendingBatchConfig::new(self.address, self.last_canonical_update.clone());
        let build_args = BatchBuilderArgs::new(pool.clone(), config);
        let (result, done) = oneshot::channel();
        let worker_id = self.worker_id;
        let base_fee = self.base_fee.base_fee();

        // spawn block building task and forward to worker
        self.task_spawner.spawn_task("next-batch", async move {
            // ack once worker reaches quorum
            let (ack, rx) = oneshot::channel();

            // this is safe to call without a semaphore bc it's held as a single `Option`
            let BatchBuilderOutput { batch, mined_transactions } = build_batch(build_args, worker_id, base_fee);

            // forward to worker and wait for ack that quorum was reached
            if let Err(e) = to_worker.send((batch.seal_slow(), ack)).await {
                error!(target: "worker::batch_builder", ?e, "failed to send next batch to worker");
                // try to return error if worker channel closed
                let _ = result.send(Err(e.into()));
                return;
            }

            // wait for worker to ack quorum reached then update pool with mined transactions
            match rx.await {
                Ok(res) => {
                    match res {
                        Ok(_) => {
                            debug!(target: "block-builder", ?res, "received ack");
                            // signal to Self that this task is complete
                            if let Err(e) = result.send(Ok(mined_transactions)) {
                                error!(target: "worker::batch_builder", ?e, "failed to send block builder result to block builder task");
                            }
                        }
                        Err(error) => {
                            error!(target: "worker::batch_builder", ?error, "error while sealing batch");
                            let converted = match error {
                                BlockSealError::FatalDBFailure => {
                                    // fatal - return error
                                    Err(BatchBuilderError::FatalDBFailure)
                                }
                                BlockSealError::QuorumRejected
                                | BlockSealError::AntiQuorum
                                | BlockSealError::Timeout
                                | BlockSealError::NotValidator
                                | BlockSealError::FailedQuorum => {
                                    // potentially non-fatal error
                                    //
                                    // return empty vec to indicate no transactions mined
                                    // NOTE: this will apply no changes to transaction pool
                                    Ok(vec![])
                                }
                            };

                            if let Err(e) = result.send(converted) {
                                error!(target: "worker::batch_builder", ?e, "failed to send block builder result to block builder task");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(target: "worker::batch_builder", ?e, "quorum waiter failed ack failed");
                    if let Err(e) = result.send(Err(e.into())) {
                        error!(target: "worker::batch_builder", ?e, "failed to send block builder result to block builder task");
                    }
                }
            }
        });

        // return oneshot channel for receiving completion status
        done
    }

    fn latest_canon_block(reth_env: &RethEnv) -> SealedBlock {
        let num = reth_env.last_block_number().unwrap_or_default();
        if let Ok(Some(header)) = reth_env.sealed_block_by_number(num) {
            header
        } else {
            reth_env.chainspec().sealed_genesis_block()
        }
    }
}

/// The [BatchBuilder] is a future that loops through the following:
/// - check/apply canonical state changes that affect the next build
/// - poll any pending block building tasks
/// - otherwise, build next block if pending transactions are available
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins
/// executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and
/// any output that is queued.
impl Future for BatchBuilder {
    type Output = BatchBuilderResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // loop when a successful block is built
        loop {
            // This is used as a "wake up" when canonical state updates.
            while let Poll::Ready(Some(latest)) = this.state_changed.poll_next_unpin(cx) {
                this.last_canonical_update = latest.tip().sealed_block().clone()
            }

            // only propose one block at a time
            if this.pending_task.is_none() {
                // check for pending transactions
                if this.pool.pending_transactions().is_empty() {
                    // reset interval to wake up after some time
                    //
                    // only need to reset here if there is no pending block being built
                    this.max_delay_interval.reset();

                    // tick interval to ensure it advances
                    let _ = this.max_delay_interval.poll_tick(cx);

                    // nothing pending
                    break;
                }

                // start building the next block
                this.pending_task = Some(this.spawn_execution_task());

                // don't break so pending_task receiver gets polled
            }

            // poll receiver that returns mined transactions once the batch reaches quorum
            if let Some(mut receiver) = this.pending_task.take() {
                // poll here so waker is notified when ack received
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        debug!(target: "block-builder", ?res, "pending task complete");
                        // TODO: update tree's pending block?

                        // ensure no fatal errors
                        let mined_transactions = res??;

                        // NOTE: empty vec returned for non-fatal error during block proposal
                        if mined_transactions.is_empty() {
                            // return pending and wait for canonical update to wake up again
                            break;
                        }

                        debug!(target: "block-builder", "applying block builder's update");

                        let base_fee_per_gas = this
                            .last_canonical_update
                            .base_fee_per_gas
                            .unwrap_or_else(|| this.pool.get_pending_base_fee());

                        // update pool to remove mined transactions
                        this.pool.update_canonical_state(
                            &this.last_canonical_update,
                            base_fee_per_gas,
                            None,
                            mined_transactions,
                            vec![],
                        );

                        // loop again to check for any other pending transactions
                        // and possibly start building the next block
                        //
                        // NOTE: continuing here is important.
                        // To prevent the following scenario, do not wait for task's waker:
                        // - there were more transactions in the pool than could fit in the first
                        //   block
                        // - pending transaction notifications already drained
                        // - have to wait for engine's next canonical update to wake up
                        continue;
                    }

                    Poll::Pending => {
                        this.pending_task = Some(receiver);

                        // break loop and return Poll::Pending
                        break;
                    }
                }
            }
        }

        // all output executed, yield back to runtime
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::{path::Path, str::FromStr, sync::Arc, time::Duration};
    use tempfile::TempDir;
    use tn_engine::execute_consensus_output;
    use tn_network_types::{local::LocalNetwork, MockWorkerToPrimaryHang};
    use tn_reth::{
        payload::BuildArguments, recover_raw_transaction, test_utils::TransactionFactory,
        RethChainSpec,
    };
    use tn_storage::{open_db, tables::Batches};
    use tn_types::{
        gas_accumulator::GasAccumulator, test_genesis, Bytes, ConsensusOutput, Database,
        GenesisAccount, TaskManager, U160, U256,
    };
    use tn_worker::{
        metrics::WorkerMetrics, test_utils::TestMakeBlockQuorumWaiter, Worker, WorkerNetworkHandle,
    };
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_make_block_no_ack_txs_in_pool_still() {
        let genesis = test_genesis();
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // task manger
        let task_manager = TaskManager::new("Test Task Manager");
        let tmp_dir = TempDir::new().unwrap();
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager).unwrap();
        let txpool = reth_env.init_txn_pool().unwrap();
        let address = Address::from(U160::from(33));
        let client = LocalNetwork::new_with_empty_id();
        let worker_to_primary = Arc::new(MockWorkerToPrimaryHang {});
        client.set_worker_to_primary_local_handler(worker_to_primary);
        let temp_dir = TempDir::new().unwrap();
        let store = open_db(temp_dir.path());
        let qw = TestMakeBlockQuorumWaiter::new_test();
        let node_metrics = WorkerMetrics::default();
        let timeout = Duration::from_secs(5);
        let mut task_manager = TaskManager::new("Batch Builder Test");
        let block_provider = Worker::new(
            0,
            Some(qw),
            Arc::new(node_metrics),
            client,
            store.clone(),
            timeout,
            WorkerNetworkHandle::new_for_test(),
            &mut task_manager,
        );

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            block_provider.batches_tx(),
            address,
            Duration::from_secs(1),
            task_manager.get_spawner(),
            0,
            BaseFeeContainer::default(),
        );

        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

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
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let _batch_builder = tokio::spawn(Box::pin(batch_builder));

        // wait for new batch
        let mut new_batch = None;
        for _ in 0..5 {
            let _ = tokio::time::sleep(Duration::from_secs(1)).await;
            // Ensure the block is stored
            if let Some((_, wb)) = store.iter::<Batches>().next() {
                new_batch = Some(wb);
                break;
            }
        }
        let new_batch = new_batch.unwrap();

        // number of transactions in the block
        let block_txs = new_batch.transactions();

        // check max tx for task matches num of transactions in block
        let num_block_txs = block_txs.len();
        assert_eq!(3, num_block_txs);

        // ensure decoded block transaction is transaction1
        let block_tx_bytes = block_txs.first().expect("one tx in block");
        let block_tx =
            recover_raw_transaction(block_tx_bytes).expect("recover raw tx for test").into_inner();

        assert_eq!(block_tx, transaction1);

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // transactions should be in pool still since ack wasn't received
        // IT test ensures these transactions are cleared
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);
    }

    /// Convenience struct for creating test assets.
    struct TestTools {
        /// Factory for creating and signing valid transactions.
        tx_factory: TransactionFactory,
        /// Execution components:
        /// - BlockchainProvider (db)
        /// - TransactionPool
        /// - ChainSpec
        /// - TaskManager (so executor tasks don't drop)
        execution_components: TestExecutionComponents,
    }

    /// Convenience type for holding execution components.
    struct TestExecutionComponents {
        /// The reth execution environment.
        reth_env: RethEnv,
        /// The transaction pool for the block builder.
        txpool: WorkerTxPool,
        /// The chainspec with seeded genesis.
        chain: Arc<RethChainSpec>,
        /// Own manager so executor's tasks don't drop (reth).
        _manager: TaskManager,
    }

    /// Helper function to create common testing infrastructure.
    fn get_test_tools(path: &Path) -> TestTools {
        let tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        let genesis = test_genesis().extend_accounts([(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // task manger
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env = RethEnv::new_for_temp_chain(chain.clone(), path, &task_manager).unwrap();
        let txpool = reth_env.init_txn_pool().unwrap();

        let execution_components =
            TestExecutionComponents { reth_env, txpool, chain, _manager: task_manager };
        TestTools { tx_factory, execution_components }
    }

    /// Test all possible errors from the worker while trying to reach quorum from peers.
    ///
    /// Non-fatal errors return empty vecs of mined transactions.
    /// Fatal error causes shutdown.
    #[tokio::test]
    async fn test_all_possible_error_outcomes() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components } = get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);
        let task_manager = TaskManager::default();

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_millis(1),
            task_manager.get_spawner(),
            0,
            BaseFeeContainer::default(),
        );

        // expected to be 7 wei for first block
        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

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
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let batch_builder_task = tokio::spawn(Box::pin(batch_builder));

        // plenty of time for block production
        let duration = std::time::Duration::from_secs(5);

        // simulate engine to create canonical blocks from empty rounds
        let mut parent = chain.sealed_genesis_header();

        let non_fatal_errors = vec![
            BlockSealError::QuorumRejected,
            BlockSealError::AntiQuorum,
            BlockSealError::Timeout,
            BlockSealError::FailedQuorum,
        ];

        // receive new blocks and return non-fatal errors
        // non-fatal errors cause the loop to break and wait for txpool updates
        // submitting a new pending transaction is one of the ways this task wakes up
        for (subdag_index, error) in non_fatal_errors.into_iter().enumerate() {
            let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
                .await
                .expect("block builder built another block after canonical update")
                .expect("batch was built");

            // all 3 transactions present
            assert_eq!(sealed_batch.batch().transactions().len(), 3 + subdag_index);

            // send non-fatal error
            let _ = ack.send(Err(error));

            // submit another tx to pool
            tx_factory
                .create_and_submit_eip1559_pool_tx(
                    chain.clone(),
                    gas_price,
                    Address::ZERO,
                    value, // 1 TEL
                    txpool.clone(),
                )
                .await;

            // canonical update to wake up task
            let output = ConsensusOutput {
                beneficiary: address,
                early_finalize: true,
                ..Default::default()
            };
            // execute output to trigger canonical update
            let args = BuildArguments::new(reth_env.clone(), output, parent);
            let final_header =
                execute_consensus_output(args, GasAccumulator::default()).expect("output executed");

            // update values for next loop
            parent = final_header;

            // sleep to ensure canonical update received before ack
            let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // wait for next block
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
            .expect("batch was built");

        // expect 7 transactions after loop added 4 more
        assert_eq!(sealed_batch.batch().transactions().len(), 7);

        // now send fatal error
        let _ = ack.send(Err(BlockSealError::FatalDBFailure));

        // ensure block builder shuts down from fatal error
        let result = batch_builder_task.await.expect("ack channel delivered result");
        assert!(result.is_err());

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // transactions should be in pool still since ack was error
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 7);
    }

    /// Test transactions are mined from the pool.
    #[tokio::test]
    async fn test_pool_updates_after_txs_mined() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components } = get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);
        let task_manager = TaskManager::default();

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

        // expected to be 7 wei for first block
        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

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
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let _batch_builder_task = tokio::spawn(Box::pin(batch_builder));

        // plenty of time for block production
        let duration = std::time::Duration::from_secs(5);

        // receive proposed block with 3 transactions
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
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

        // assert first 3 txs in block
        assert_eq!(sealed_batch.batch().transactions().len(), 3);

        // assert all 4 txs in pending pool
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 4);

        // send ack to mine first 3 transactions
        let _ = ack.send(Ok(()));

        // receive next block
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
            .expect("batch was built");
        // send ack to mine block
        let _ = ack.send(Ok(()));

        // assert only transaction in block
        assert_eq!(sealed_batch.batch().transactions().len(), 1);

        // confirm 4th transaction hash matches one submitted
        let tx_bytes =
            sealed_batch.batch().transactions().first().expect("block transactions length is one");
        let tx = recover_raw_transaction(tx_bytes).expect("recover raw tx for test");
        assert_eq!(tx.hash(), &expected_tx_hash);

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // assert all transactions mined
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 0);
    }
}
