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
use reth_execution_types::ChangedAccount;
use reth_provider::{CanonStateNotification, CanonStateNotificationStream, Chain};
use reth_transaction_pool::{
    CanonicalStateUpdate, PoolTransaction, PoolUpdateKind, TransactionPool, TransactionPoolExt,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tn_types::{
    error::BlockSealError, Address, BatchBuilderArgs, BatchSender, LastCanonicalUpdate,
    PendingBlockConfig, TransactionSigned, TxHash, MIN_PROTOCOL_BASE_FEE,
};
use tokio::{sync::oneshot, time::Interval};
use tracing::{debug, error, trace, warn};

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
/// -
#[derive(Debug)]
pub struct BatchBuilder<BT, Pool> {
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<BuildResult>,
    /// The type used to query both the database and the blockchain tree.
    ///
    /// TODO: leaving this for now to prevent generics refactor
    _blockchain: BT,
    /// The transaction pool with pending transactions.
    pool: Pool,
    /// Canonical state changes from the engine.
    ///
    /// Notifications are sent on this stream after each round of consensus
    /// is executed. These updates are used to apply changes to the transaction pool.
    canonical_state_stream: CanonStateNotificationStream,
    /// Type to track the last canonical state update.
    ///
    /// The worker applies updates to the pool when it mines new transactions, but
    /// the canonical tip and basefee only change through engine updates. This type
    /// allows the worker to apply mined transactions updates without affecting the
    /// tip or basefee between rounds of consensus.
    ///
    /// This is a solution until TN has it's own transaction pool implementation.
    latest_canon_state: LastCanonicalUpdate,
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
}

impl<BT, Pool> BatchBuilder<BT, Pool>
where
    Pool: TransactionPoolExt + 'static,
    Pool::Transaction: PoolTransaction<Consensus = TransactionSigned>,
{
    /// Create a new instance of [Self].
    pub fn new(
        _blockchain: BT,
        pool: Pool,
        canonical_state_stream: CanonStateNotificationStream,
        latest_canon_state: LastCanonicalUpdate,
        to_worker: BatchSender,
        address: Address,
        max_delay: Duration,
    ) -> Self {
        let max_delay_interval = tokio::time::interval(max_delay);
        Self {
            pending_task: None,
            _blockchain,
            pool,
            canonical_state_stream,
            latest_canon_state,
            to_worker,
            address,
            max_delay_interval,
        }
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to update pool before building the next block.
    fn process_canon_state_update(&mut self, update: Arc<Chain>) {
        trace!(target: "worker::block-builder", ?update, "canon state update from engine");

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        // collect all accounts that changed in last round of consensus
        let changed_accounts: Vec<ChangedAccount> = state
            .accounts_iter()
            .filter_map(|(addr, acc)| acc.map(|acc| (addr, acc)))
            .map(|(address, acc)| ChangedAccount {
                address,
                nonce: acc.nonce,
                balance: acc.balance,
            })
            .collect();

        debug!(target: "block-builder", ?changed_accounts);

        // collect tx hashes to remove any transactions from this pool that were mined
        let mined_transactions: Vec<TxHash> = blocks.transaction_hashes().collect();

        debug!(target: "block-builder", ?mined_transactions);

        // TODO: calculate the next basefee HERE for the entire round
        //
        // for now, always use lowest base fee possible
        let pending_block_base_fee = MIN_PROTOCOL_BASE_FEE;

        // Canonical update
        let update = CanonicalStateUpdate {
            new_tip: &tip.block,          // finalized block
            pending_block_base_fee,       // current base fee for worker (network-wide)
            pending_block_blob_fee: None, // current blob fee for worker (network-wide)
            changed_accounts,             // entire round of consensus
            mined_transactions,           // entire round of consensus
            update_kind: PoolUpdateKind::Commit,
        };

        // track latest update to apply batches
        let latest = LastCanonicalUpdate {
            tip: tip.block.clone(),
            pending_block_base_fee,
            pending_block_blob_fee: None,
        };

        debug!(target: "block-builder", ?update, ?latest, "applying update to txpool");

        // track canon update so worker updates don't overwrite the tip or base fees
        self.latest_canon_state = latest;

        // sync fn so self will block until all pool updates are complete
        self.pool.on_canonical_state_change(update);
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

        // configure params for next block to build
        let config = PendingBlockConfig::new(self.address, self.latest_canon_state.clone());
        let build_args = BatchBuilderArgs::new(pool.clone(), config);
        let (result, done) = oneshot::channel();

        // spawn block building task and forward to worker
        tokio::spawn(async move {
            // ack once worker reaches quorum
            let (ack, rx) = oneshot::channel();

            // this is safe to call without a semaphore bc it's held as a single `Option`
            let BatchBuilderOutput { batch, mined_transactions } = build_batch(build_args);

            // forward to worker and wait for ack that quorum was reached
            if let Err(e) = to_worker.send((batch.seal_slow(), ack)).await {
                error!(target: "worker::batch_builder", ?e, "failed to send next block to worker");
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
                            error!(target: "worker::batch_builder", ?error, "error while sealing block");
                            let converted = match error {
                                BlockSealError::FatalDBFailure => {
                                    // fatal - return error
                                    Err(BatchBuilderError::FatalDBFailure)
                                }
                                BlockSealError::QuorumRejected
                                | BlockSealError::AntiQuorum
                                | BlockSealError::Timeout
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
impl<BT, Pool> Future for BatchBuilder<BT, Pool>
where
    BT: Unpin,
    Pool: TransactionPool + TransactionPoolExt + Unpin + 'static,
    Pool::Transaction: PoolTransaction<Consensus = TransactionSigned>,
{
    type Output = BatchBuilderResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // loop when a successful block is built
        loop {
            // check for canon updates before mining the transaction pool
            //
            // this is critical to ensure worker's block is building off canonical tip
            // block until canon updates are applied
            while let Poll::Ready(Some(canon_update)) =
                this.canonical_state_stream.poll_next_unpin(cx)
            {
                debug!(target: "block-builder", ?canon_update, "received canonical update");
                // poll canon updates stream and update pool `.on_canon_update`
                //
                // maintenance task will handle worker's pending block update
                match canon_update {
                    CanonStateNotification::Commit { new } => {
                        this.process_canon_state_update(new);
                    }
                    _ => unreachable!("TN reorgs are impossible"),
                }
            }

            // only propose one block at a time
            if this.pending_task.is_none() {
                // TODO: is there a more efficient approach? only need pending pool stats
                // create upstream PR for reth?
                //
                // check for pending transactions
                //
                // considered using: pool.pool_size().pending
                // but that calculates size for all sub-pools
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

                        // use latest values so only mined transactions are updated
                        let new_tip = &this.latest_canon_state.tip;
                        let pending_block_base_fee = this.latest_canon_state.pending_block_base_fee;
                        let pending_block_blob_fee = this.latest_canon_state.pending_block_blob_fee;

                        // create canonical state update
                        let update = CanonicalStateUpdate {
                            new_tip,
                            pending_block_base_fee,
                            pending_block_blob_fee,
                            changed_accounts: vec![], // only updated by engine updates
                            mined_transactions,
                            update_kind: PoolUpdateKind::Commit,
                        };

                        debug!(target: "block-builder", ?update, "applying block builder's update");

                        // TODO: should this be a spawned blocking task?
                        //
                        // update pool to remove mined transactions
                        this.pool.on_canonical_state_change(update);

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
    use reth_blockchain_tree::{
        noop::NoopBlockchainTree, BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree,
        TreeExternals,
    };
    use reth_chainspec::ChainSpec;
    use reth_consensus::FullConsensus;
    use reth_db::{
        test_utils::{create_test_rw_db, tempdir_path, TempDatabase},
        DatabaseEnv,
    };
    use reth_db_common::init::init_genesis;
    use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
    use reth_provider::{
        providers::{BlockchainProvider, StaticFileProvider},
        CanonStateSubscriptions as _, ProviderFactory,
    };
    use reth_transaction_pool::{
        blobstore::InMemoryBlobStore, CoinbaseTipOrdering, EthPooledTransaction,
        EthTransactionValidator, Pool, PoolConfig, TransactionValidationTaskExecutor,
    };
    use std::{str::FromStr, time::Duration};
    use tempfile::TempDir;
    use tn_engine::execute_consensus_output;
    use tn_network::local::LocalNetwork;
    use tn_node_traits::{BuildArguments, TNExecution, TelcoinNode};
    use tn_storage::{open_db, tables::Batches};
    use tn_test_utils::{adiri_genesis_seeded, get_gas_price, TransactionFactory};
    use tn_types::{
        adiri_genesis, BlockBody, Bytes, CommittedSubDag, ConsensusHeader, ConsensusOutput,
        Database, GenesisAccount, SealedBatch, SealedBlock, TaskManager, U160, U256,
    };
    use tn_worker::{
        metrics::WorkerMetrics,
        quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
        BatchProvider,
    };
    use tokio::time::timeout;

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
    async fn test_make_block_no_ack_txs_in_pool_still() {
        let genesis = adiri_genesis();
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
        let head_timestamp = genesis.timestamp;
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // init genesis
        let db = create_test_rw_db();
        // provider
        let provider_factory = ProviderFactory::new(
            Arc::clone(&db),
            Arc::clone(&chain),
            StaticFileProvider::read_write(tempdir_path())
                .expect("static file provider read write created with tempdir path"),
        );
        let _genesis_hash = init_genesis(&provider_factory).expect("init genesis");

        let blockchain_db: BlockchainProvider<TelcoinNode<_>> =
            BlockchainProvider::new(provider_factory, Arc::new(NoopBlockchainTree::default()))
                .expect("test blockchain provider");

        // task manger
        let task_manager = TaskManager::new("Test Task Manager");

        // txpool
        let blob_store = InMemoryBlobStore::default();
        let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
            .with_head_timestamp(head_timestamp)
            .with_additional_tasks(1)
            .build_with_tasks(
                blockchain_db.clone(),
                task_manager.get_spawner(),
                blob_store.clone(),
            );

        let txpool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
        let address = Address::from(U160::from(33));
        let client = LocalNetwork::new_with_empty_id();
        let temp_dir = TempDir::new().unwrap();
        let store = open_db(temp_dir.path());
        let qw = TestMakeBlockQuorumWaiter();
        let node_metrics = WorkerMetrics::default();
        let timeout = Duration::from_secs(5);
        let block_provider =
            BatchProvider::new(0, qw, Arc::new(node_metrics), client, store.clone(), timeout);

        let tx_pool_latest = txpool.block_info();
        let tip = SealedBlock::new(chain.sealed_genesis_header(), BlockBody::default());

        let latest_canon_state = LastCanonicalUpdate {
            tip, // genesis
            pending_block_base_fee: tx_pool_latest.pending_basefee,
            pending_block_blob_fee: tx_pool_latest.pending_blob_fee,
        };

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            blockchain_db.clone(),
            txpool.clone(),
            blockchain_db.canonical_state_stream(),
            latest_canon_state,
            block_provider.batches_tx(),
            address,
            Duration::from_secs(1),
        );

        let gas_price = get_gas_price(&blockchain_db);
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
        assert_matches!(added_result, hash if hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction3.hash());

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
        let block_tx = block_txs.first().cloned().expect("one tx in block");
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
        /// Last canonical update - expected to be genesis in these tests.
        last_canonical_update: LastCanonicalUpdate,
        /// Execution components:
        /// - BlockchainProvider (db)
        /// - TransactionPool
        /// - ChainSpec
        /// - TaskManager (so executor tasks don't drop)
        execution_components: TestExecutionComponents,
    }

    type TestPool = Pool<
        TransactionValidationTaskExecutor<
            EthTransactionValidator<
                BlockchainProvider<TelcoinNode<Arc<TempDatabase<DatabaseEnv>>>>,
                EthPooledTransaction,
            >,
        >,
        CoinbaseTipOrdering<EthPooledTransaction>,
        InMemoryBlobStore,
    >;

    /// Convenience type for holding execution components.
    struct TestExecutionComponents {
        /// The database client.
        blockchain_db: BlockchainProvider<TelcoinNode<Arc<TempDatabase<DatabaseEnv>>>>,
        /// The transaction pool for the block builder.
        txpool: TestPool,
        /// The chainspec with seeded genesis.
        chain: Arc<ChainSpec>,
        /// Own manager so executor's tasks don't drop (reth).
        _manager: TaskManager,
    }

    /// Helper function to create common testing infrastructure.
    fn get_test_tools() -> TestTools {
        let tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        let genesis = adiri_genesis_seeded(vec![factory_address]);
        let head_timestamp = genesis.timestamp;
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // init genesis
        let db = create_test_rw_db();
        // provider
        let provider_factory = ProviderFactory::new(
            Arc::clone(&db),
            Arc::clone(&chain),
            StaticFileProvider::read_write(tempdir_path())
                .expect("static file provider read write created with tempdir path"),
        );
        let _genesis_hash = init_genesis(&provider_factory).expect("init genesis");

        let executor = EthExecutorProvider::ethereum(Arc::clone(&chain));
        let auto_consensus: Arc<dyn FullConsensus> = Arc::new(TNExecution);
        let tree_config = BlockchainTreeConfig::default();
        let tree_externals =
            TreeExternals::new(provider_factory.clone(), auto_consensus.clone(), executor.clone());
        let tree = BlockchainTree::new(tree_externals, tree_config).expect("new blockchain tree");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        let blockchain_db = BlockchainProvider::new(provider_factory, blockchain_tree)
            .expect("test blockchain provider");

        // task manger
        let task_manager = TaskManager::new("Test Task Manager");

        // txpool
        let blob_store = InMemoryBlobStore::default();
        let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
            .with_head_timestamp(head_timestamp)
            .with_additional_tasks(1)
            .build_with_tasks(
                blockchain_db.clone(),
                task_manager.get_spawner(),
                blob_store.clone(),
            );

        let txpool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
        let tx_pool_latest = txpool.block_info();
        let tip = SealedBlock::new(chain.sealed_genesis_header(), BlockBody::default());

        let last_canonical_update = LastCanonicalUpdate {
            tip, // genesis
            pending_block_base_fee: tx_pool_latest.pending_basefee,
            pending_block_blob_fee: tx_pool_latest.pending_blob_fee,
        };

        let execution_components =
            TestExecutionComponents { blockchain_db, txpool, chain, _manager: task_manager };
        TestTools { tx_factory, last_canonical_update, execution_components }
    }

    /// Test all possible errors from the worker while trying to reach quorum from peers.
    ///
    /// Non-fatal errors return empty vecs of mined transactions.
    /// Fatal error causes shutdown.
    #[tokio::test]
    async fn test_all_possible_error_outcomes() {
        let TestTools { mut tx_factory, last_canonical_update, execution_components } =
            get_test_tools();
        let TestExecutionComponents { blockchain_db, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);
        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            blockchain_db.clone(),
            txpool.clone(),
            blockchain_db.canonical_state_stream(),
            last_canonical_update,
            to_worker,
            address,
            Duration::from_millis(1),
        );

        // expected to be 7 wei for first block
        let gas_price = get_gas_price(&blockchain_db);
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
        assert_matches!(added_result, hash if hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction3.hash());

        // txpool size
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let batch_builder_task = tokio::spawn(Box::pin(batch_builder));

        // plenty of time for block production
        let duration = std::time::Duration::from_secs(5);

        // simulate engine to create canonical blocks from empty rounds
        let evm_config = EthEvmConfig::new(chain.clone());
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
                    &txpool,
                )
                .await;

            // canonical update to wake up task
            let output = ConsensusOutput {
                sub_dag: CommittedSubDag::new(
                    vec![Default::default()],
                    Default::default(),
                    subdag_index as u64,
                    Default::default(),
                    None,
                )
                .into(),
                batches: vec![vec![]],
                beneficiary: address,
                batch_digests: Default::default(),
                parent_hash: ConsensusHeader::default().digest(),
                number: 0,
            };
            // execute output to trigger canonical update
            let args = BuildArguments::new(blockchain_db.clone(), output, parent);
            let final_header =
                execute_consensus_output(&evm_config, args).expect("output executed");

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
        let TestTools { mut tx_factory, last_canonical_update, execution_components } =
            get_test_tools();
        let TestExecutionComponents { blockchain_db, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            blockchain_db.clone(),
            txpool.clone(),
            blockchain_db.canonical_state_stream(),
            last_canonical_update,
            to_worker,
            address,
            Duration::from_secs(1),
        );

        // expected to be 7 wei for first block
        let gas_price = get_gas_price(&blockchain_db);
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
        assert_matches!(added_result, hash if hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction3.hash());

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
                &txpool,
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
        let tx =
            sealed_batch.batch().transactions().first().expect("block transactions length is one");
        assert_eq!(tx.hash(), expected_tx_hash);

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // assert all transactions mined
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 0);
    }
}
