//! A [Consensus] implementation for local testing purposes
//! that automatically seals blocks.
//!
//! The Mining task polls a [MiningMode], and will return a list of transactions that are ready to
//! be mined.
//!
//! These downloaders poll the miner, assemble the block, and return transactions that are ready to
//! be mined.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use consensus_metrics::metered_channel::Sender;
use reth_chainspec::ChainSpec;
use reth_evm::execute::{
    BlockExecutionError, BlockExecutionOutput, BlockExecutorProvider, BlockValidationError,
    Executor,
};
use reth_execution_errors::InternalBlockExecutionError;
use reth_primitives::{
    constants::{EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT},
    keccak256, proofs, Address, Block, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber,
    Header, SealedHeader, TransactionSigned, Withdrawals, B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{BlockReaderIdExt, ExecutionOutcome, StateProviderFactory};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::TransactionPool;
use reth_trie::HashedPostState;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tn_types::{now, AutoSealConsensus, NewBatch, PendingWorkerBlock};
use tokio::sync::{watch, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, error, trace, warn};

// mod client;
mod mode;
mod task;

// pub use crate::client::AutoSealClient;
pub use mode::{FixedBlockTimeMiner, MiningMode, ReadyTransactionMiner};
pub use task::MiningTask;

/// Builder type for configuring the setup
#[derive(Debug)]
pub struct BatchMakerBuilder<Client, Pool, EvmConfig> {
    client: Client,
    consensus: AutoSealConsensus,
    pool: Pool,
    mode: MiningMode,
    storage: Storage,
    to_worker: Sender<NewBatch>,
    evm_config: EvmConfig,
    watch_tx: watch::Sender<PendingWorkerBlock>,
}

// === impl AutoSealBuilder ===

impl<Client, Pool, EvmConfig> BatchMakerBuilder<Client, Pool, EvmConfig>
where
    Client: BlockReaderIdExt,
    Pool: TransactionPool,
{
    /// Creates a new builder instance to configure all parts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        client: Client,
        pool: Pool,
        to_worker: Sender<NewBatch>,
        mode: MiningMode,
        address: Address,
        evm_config: EvmConfig,
        watch_tx: watch::Sender<PendingWorkerBlock>,
        // TODO: pass max_block here to shut down batch maker?
    ) -> Self {
        let latest_header = client
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        Self {
            storage: Storage::new(latest_header, address),
            client,
            consensus: AutoSealConsensus::new(chain_spec),
            pool,
            mode,
            to_worker,
            evm_config,
            watch_tx,
        }
    }

    /// Sets the [MiningMode] it operates in, default is [MiningMode::Auto]
    pub fn mode(mut self, mode: MiningMode) -> Self {
        self.mode = mode;
        self
    }

    /// Consumes the type and returns all components
    #[track_caller]
    pub fn build(self) -> MiningTask<Client, Pool, EvmConfig> {
        let Self { client, consensus, pool, mode, storage, to_worker, evm_config, watch_tx } = self;
        // let auto_client = AutoSealClient::new(storage.clone());

        // (consensus, auto_client, task)
        MiningTask::new(
            Arc::clone(consensus.chain_spec()),
            mode,
            to_worker,
            storage,
            client,
            pool,
            evm_config,
            watch_tx,
        )
    }
}

/// In memory storage
#[derive(Debug, Clone, Default)]
pub(crate) struct Storage {
    inner: Arc<RwLock<StorageInner>>,
}

// == impl Storage ===

impl Storage {
    fn new(header: SealedHeader, address: Address) -> Self {
        let (header, best_hash) = header.split();
        let mut storage = StorageInner {
            best_hash,
            total_difficulty: header.difficulty,
            best_block: header.number,
            address,
            ..Default::default()
        };
        storage.headers.insert(0, header);
        storage.bodies.insert(best_hash, BlockBody::default());
        Self { inner: Arc::new(RwLock::new(storage)) }
    }

    /// Returns the write lock of the storage
    pub(crate) async fn write(&self) -> RwLockWriteGuard<'_, StorageInner> {
        self.inner.write().await
    }

    /// Returns the read lock of the storage
    pub(crate) async fn _read(&self) -> RwLockReadGuard<'_, StorageInner> {
        self.inner.read().await
    }
}

/// In-memory storage for the chain the auto seal engine is building.
#[derive(Default, Debug)]
pub(crate) struct StorageInner {
    /// Headers buffered for download.
    pub(crate) headers: HashMap<BlockNumber, Header>,
    /// A mapping between block hash and number.
    pub(crate) hash_to_number: HashMap<BlockHash, BlockNumber>,
    /// Bodies buffered for download.
    pub(crate) bodies: HashMap<BlockHash, BlockBody>,
    /// Tracks best block
    pub(crate) best_block: u64,
    /// Tracks hash of best block
    pub(crate) best_hash: B256,
    /// The total difficulty of the chain until this block
    pub(crate) total_difficulty: U256,
    /// The address for batch beneficiary.
    pub(crate) address: Address,
}

// === impl StorageInner ===

impl StorageInner {
    /// Returns the block hash for the given block number if it exists.
    pub(crate) fn _block_hash(&self, num: u64) -> Option<BlockHash> {
        self.hash_to_number.iter().find_map(|(k, v)| num.eq(v).then_some(*k))
    }

    /// Returns the matching header if it exists.
    pub(crate) fn _header_by_hash_or_number(
        &self,
        hash_or_num: BlockHashOrNumber,
    ) -> Option<Header> {
        let num = match hash_or_num {
            BlockHashOrNumber::Hash(hash) => self.hash_to_number.get(&hash).copied()?,
            BlockHashOrNumber::Number(num) => num,
        };
        self.headers.get(&num).cloned()
    }

    /// Inserts a new header+body pair
    pub(crate) fn insert_new_block(&mut self, mut header: Header, body: BlockBody) {
        header.number = self.best_block + 1;
        header.parent_hash = self.best_hash;

        self.best_hash = header.hash_slow();
        self.best_block = header.number;
        self.total_difficulty += header.difficulty;

        trace!(target: "execution::batch_maker", num=self.best_block, hash=?self.best_hash, "inserting new block");
        self.headers.insert(header.number, header);
        self.bodies.insert(self.best_hash, body);
        self.hash_to_number.insert(self.best_hash, self.best_block);
        tracing::debug!(target: "execution::batch_maker", storage_size=?self.bodies.len());
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(
        &self,
        transactions: &[TransactionSigned],
        chain_spec: Arc<ChainSpec>,
        parent: &SealedHeader,
        withdrawals: Option<&Withdrawals>,
    ) -> Header {
        // // check previous block for base fee
        // let base_fee_per_gas = self
        //     .headers
        //     .get(&self.best_block)
        //     .and_then(|parent| parent.next_block_base_fee(chain_spec.base_fee_params));

        // use finalized parent for this batch base fee
        //
        // TODO: use this worker's previous batch for base fee instead?
        let base_fee_per_gas =
            parent.next_block_base_fee(chain_spec.base_fee_params_at_timestamp(now()));

        let mut header = Header {
            parent_hash: parent.hash(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: self.address,
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: withdrawals.map(|w| proofs::calculate_withdrawals_root(w)),
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: parent.number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            gas_used: 0,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            mix_hash: Default::default(),
            nonce: 0,
            base_fee_per_gas,
            blob_gas_used: None,
            excess_blob_gas: None,
            extra_data: Default::default(),
            parent_beacon_block_root: None,
            requests_root: None,
        };

        header.transactions_root = if transactions.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            proofs::calculate_transaction_root(transactions)
        };

        // TODO: is there a better way?
        //
        // sometimes batches are produced too quickly
        // resulting in batch timestamp == parent timestamp
        if header.timestamp == parent.timestamp {
            warn!(target: "execution::batch_maker", "header template timestamp same as parent");
            header.timestamp = parent.timestamp + 1;
        }

        // TODO: this is easy to manipulate
        //
        // calculate mix hash as a source of randomness
        // - consensus output digest from parent (beacon block root)
        // - timestamp
        //
        // see https://eips.ethereum.org/EIPS/eip-4399
        if let Some(root) = parent.parent_beacon_block_root {
            header.mix_hash =
                keccak256([root.as_slice(), header.timestamp.to_le_bytes().as_slice()].concat());
        }

        header
    }

    /// Builds and executes a new block with the given transactions, on the provided executor.
    ///
    /// This returns the header of the executed block, as well as the poststate from execution.
    pub(crate) fn build_and_execute<Provider, Executor>(
        &mut self,
        transactions: Vec<TransactionSigned>,
        withdrawals: Option<Withdrawals>,
        provider: &Provider,
        chain_spec: Arc<ChainSpec>,
        executor: &Executor,
    ) -> Result<(SealedHeader, ExecutionOutcome), BlockExecutionError>
    where
        Executor: BlockExecutorProvider,
        Provider: StateProviderFactory + BlockReaderIdExt,
    {
        // use the last canonical block for next batch
        let parent = provider.latest_header()
            .map_err(|e| {
                error!(target: "execution::batch_maker", "error retrieving client.latest_header() {e}");
                BlockExecutionError::Internal(InternalBlockExecutionError::LatestBlock(e))
            })?
            .ok_or_else(|| {
                error!(target: "execution::batch_maker", "error retrieving client.latest_header() returned `None`");
                BlockExecutionError::Internal(InternalBlockExecutionError::LatestBlock(reth_provider::ProviderError::FinalizedBlockNotFound))
            })?;

        debug!(target: "execution::batch_maker", latest=?parent);

        let header = self.build_header_template(
            &transactions,
            chain_spec.clone(),
            &parent,
            withdrawals.as_ref(),
        );

        let block = Block {
            header,
            body: transactions,
            ommers: vec![],
            withdrawals: withdrawals.clone(),
            requests: None,
        }
        .with_recovered_senders()
        .ok_or(BlockExecutionError::Validation(BlockValidationError::SenderRecoveryError))?;

        trace!(target: "execution::batch_maker", transactions=?&block.body, "executing transactions");

        // TODO: should this use the latest or finalized for next batch?
        //
        // for now, keep it consistent with latest block retrieved for header template
        let mut db = StateProviderDatabase::new(provider.latest().map_err(|e| {
            BlockExecutionError::Internal(InternalBlockExecutionError::LatestBlock(e))
        })?);

        let block_number = block.number;

        // execute the block
        let BlockExecutionOutput { state, receipts, gas_used, .. } =
            executor.executor(&mut db).execute((&block, U256::ZERO).into())?;
        let bundle_state = ExecutionOutcome::new(state, receipts.into(), block_number, vec![]);

        let Block { mut header, body, .. } = block.block;
        let body = BlockBody { transactions: body, ommers: vec![], withdrawals, requests: None };

        trace!(target: "execution::batch_maker", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // set header's gas used
        header.gas_used = gas_used;

        // see reth::crates::payload::ethereum::default_ethereum_payload_builder()
        //
        // expensive calculations - update header
        let hashed_state = HashedPostState::from_bundle_state(&bundle_state.state().state);
        header.state_root = db.state_root(hashed_state)?;
        header.receipts_root = bundle_state.receipts_root_slow(block_number)
            .ok_or_else(|| {
                error!(target: "execution::batch_maker", "error calculating receipts root from bundle state");
                BlockExecutionError::msg("Failed to create receipts root from bundle state".to_string())
            })?;
        header.logs_bloom = bundle_state.block_logs_bloom(block_number)
            .ok_or_else(|| {
                error!(target: "execution::batch_maker", "error calculating logs bloom from bundle state");
                BlockExecutionError::msg("Failed to calculate logs bloom from bundle state".to_string())
            })?;

        // finally insert into storage
        self.insert_new_block(header.clone(), body);

        // set new header with hash that should have been updated by insert_new_block
        let new_header = header.seal(self.best_hash);

        Ok((new_header, bundle_state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use fastcrypto::hash::Hash;
    use reth::tasks::TaskManager;
    use reth_blockchain_tree::noop::NoopBlockchainTree;
    use reth_db::test_utils::{create_test_rw_db, tempdir_path};
    use reth_db_common::init::init_genesis;
    use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
    use reth_primitives::{alloy_primitives::U160, Bytes, GenesisAccount};
    use reth_provider::{
        providers::{BlockchainProvider, StaticFileProvider},
        ProviderFactory,
    };
    use reth_tracing::init_test_tracing;
    use reth_transaction_pool::{
        blobstore::InMemoryBlobStore, PoolConfig, TransactionValidationTaskExecutor,
    };
    use std::{str::FromStr, time::Duration};
    use tn_types::{
        adiri_chain_spec_arc, adiri_genesis,
        test_utils::{get_gas_price, TransactionFactory},
        BatchAPI,
    };
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_make_batch() {
        init_test_tracing();
        let genesis = adiri_genesis();
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        debug!("seeding factory address: {factory_address:?}");

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        debug!("seeded genesis: {genesis:?}");
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
        let genesis_hash = init_genesis(provider_factory.clone()).expect("init genesis");

        debug!("genesis hash: {genesis_hash:?}");

        let blockchain_db =
            BlockchainProvider::new(provider_factory, Arc::new(NoopBlockchainTree::default()))
                .expect("test blockchain provider");

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
        let mining_mode =
            MiningMode::instant(max_transactions, txpool.pending_transactions_listener());

        // worker channel
        let (to_worker, mut worker_rx) = tn_types::test_channel!(1);
        let address = Address::from(U160::from(33));

        let evm_config = EthEvmConfig::default();
        let block_executor = EthExecutorProvider::new(chain.clone(), evm_config);
        let (tx, _rx) = watch::channel(PendingWorkerBlock::default());

        // build batch maker
        let task = BatchMakerBuilder::new(
            Arc::clone(&chain),
            blockchain_db.clone(),
            txpool.clone(),
            to_worker,
            mining_mode,
            address,
            block_executor,
            tx,
        )
        .build();

        let gas_price = get_gas_price(&blockchain_db);
        debug!("gas price: {gas_price:?}");
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 1: {transaction1:?}");
        debug!("transaction 1 encoded: {:?}", transaction1.clone().envelope_encoded());

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 2: {transaction2:?}");
        debug!("transaction 2 encoded: {:?}", transaction2.clone().envelope_encoded());

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 3: {transaction3:?}");
        debug!("transaction 3 encoded: {:?}", transaction3.clone().envelope_encoded());

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

        // check max tx for task matches num of transactions in batch
        let num_batch_txs = batch_txs.len();
        assert_eq!(max_transactions, num_batch_txs);

        // ensure decoded batch transaction is transaction1
        let batch_tx_bytes = batch_txs.first().cloned().expect("one tx in batch");
        let decoded_batch_tx = TransactionSigned::decode_enveloped(&mut batch_tx_bytes.as_ref())
            .expect("tx bytes are uncorrupted");
        assert_eq!(decoded_batch_tx, transaction1);

        // send the worker's ack to task
        let digest = new_batch.batch.digest();
        let _ack = new_batch.ack.send(digest);

        // // retrieve block number 1 from storage
        // //
        // // at this point, we know the task has completed because
        // // the task's Storage write lock must be dropped for the
        // // read lock to be available here
        // let storage_header = client
        //     .get_headers_with_priority(
        //         HeadersRequest { start: 1.into(), limit: 1, direction: HeadersDirection::Rising
        // },         Priority::Normal,
        //     )
        //     .await
        //     .expect("header is available from storage")
        //     .into_data()
        //     .first()
        //     .expect("header included")
        //     .to_owned();

        // debug!("awaited first reply from storage header");

        // let storage_sealed_header = storage_header.seal_slow();

        // debug!("storage sealed header: {storage_sealed_header:?}");

        // // TODO: this isn't the right thing to test bc storage should be removed
        // //
        // assert_eq!(new_batch.batch.versioned_metadata().sealed_header(), &storage_sealed_header);
        // assert_eq!(storage_sealed_header.beneficiary, address);

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

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

    #[tokio::test]
    async fn test_timestamp_adjusted_if_same_as_parent() {
        // TODO: this isn't a very accurate test
        // when running, please ensure the WARN log appears
        // to verify test is actually testing what is intended
        init_test_tracing();

        // actual error from adiri:
        // WARN request{route=/narwhal.WorkerToWorker/ReportBatch remote_peer_id=0599b3e5
        // direction=outbound}: anemo_tower::trace::on_failure: response failed error=Status code:
        // 400 Bad Request Invalid batch: block timestamp 1707774238 is in the past compared to the
        // parent timestamp 1707774238 latency=0 ms
        let address = Address::from(U160::from(100));
        // let mut sealed_header = SealedHeader::default();
        let block_hash = B256::default();
        let mut header = Header::default();
        let system_time = now();
        header.timestamp = system_time;
        let sealed_header = SealedHeader::new(header, block_hash);

        let chain_spec = adiri_chain_spec_arc();

        // create storage with the same sealed header so timestamps are the same
        let storage = Storage::new(sealed_header.clone(), address);

        let withdrawals = Some(Withdrawals::default());
        // create header template
        // warning should appear with RUST_LOG=info
        let template = storage.write().await.build_header_template(
            &Vec::new(),
            chain_spec,
            &sealed_header,
            withdrawals.as_ref(),
        );
        let expected: u64 = system_time + 1;
        assert!(template.timestamp == expected);
    }
}
