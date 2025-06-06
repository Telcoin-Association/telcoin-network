//! Implement an abstraction around the Reth transaction pool.
//! This should insolate from shifting Reth internals, etc.

use std::{sync::Arc, time::Instant};

use futures::StreamExt as _;
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, EthTransactionPool,
    TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_primitives_traits::SignerRecoverable;
use reth_provider::{
    providers::BlockchainProvider, CanonStateNotification, CanonStateSubscriptions as _, Chain,
    ChangedAccount,
};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError},
    identifier::TransactionId,
    BestTransactions, CanonicalStateUpdate, EthPooledTransaction, PoolSize, PoolTransaction,
    PoolUpdateKind, TransactionEvents, TransactionOrigin, TransactionPool as _,
    TransactionPoolExt as _, ValidPoolTransaction,
};
use tn_types::{
    Address, EnvKzgSettings, Recovered, SealedBlock, TaskSpawner, TransactionSigned, TxHash,
    MIN_PROTOCOL_BASE_FEE,
};
use tracing::{debug, info, trace};

use crate::{error::TnRethResult, traits::TelcoinNode};

/// A pooled transaction id.
pub type PoolTxnId = TransactionId;
/// A pooled transaction.
pub type PoolTxn = ValidPoolTransaction<EthPooledTransaction>;
/// A recovered pooled transaction.
pub type RecoveredPoolTxn = Recovered<EthPooledTransaction>;

pub use reth_primitives_traits::InMemorySize as TxnSize;

/// Generate a new pooled transaction from an eth transaction and id.
pub fn new_pool_txn(transaction: EthPooledTransaction, transaction_id: PoolTxnId) -> PoolTxn {
    ValidPoolTransaction {
        transaction,
        transaction_id,
        propagate: false,
        timestamp: Instant::now(),
        origin: TransactionOrigin::External,
        authority_ids: None,
    }
}

/// Decode transaction bytes back to a ['TransactionSigned'].
pub fn bytes_to_txn(tx_bytes: &[u8]) -> eyre::Result<TransactionSigned> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
        .map_err(|_| eyre::eyre!("failed to recover transaction"))?;

    Ok(recovered.into_inner())
}

/// Trait on a transaction pool to produce the best transaction.
pub trait TxPool {
    /// Return an iterator over the best transactions in a pool.
    fn best_transactions(&self) -> BestTxns;
    /// Return the pending txn base fee.
    fn get_pending_base_fee(&self) -> u64;
}

/// A telcoin network transaction pool.
#[derive(Clone, Debug)]
pub struct WorkerTxPool(EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore>);

impl From<WorkerTxPool> for EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore> {
    fn from(value: WorkerTxPool) -> Self {
        value.0
    }
}

impl WorkerTxPool {
    /// Create a new instance of `Self`.
    pub fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
    ) -> eyre::Result<Self> {
        let head = node_config.lookup_head(blockchain_provider)?;
        let data_dir = node_config.datadir();
        let pool_config = node_config.txpool.pool_config();
        let blob_store = DiskFileBlobStore::open(data_dir.blobstore(), Default::default())?;
        let validator = TransactionValidationTaskExecutor::eth_builder(blockchain_provider.clone())
            .with_head_timestamp(head.timestamp)
            .kzg_settings(EnvKzgSettings::Default)
            .with_local_transactions_config(pool_config.local_transactions_config.clone())
            .with_additional_tasks(node_config.txpool.additional_validation_tasks)
            .build_with_tasks(task_spawner.clone(), blob_store.clone());

        let transaction_pool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, pool_config);

        info!(target: "tn::execution", "Transaction pool initialized");

        /* TODO: replace this functionality to save and load the txn pool on start/stop
           The reth function backup_local_transactions_task's shutdown param can not be easily created.
           The internal functions are not easy to just copy.
           Basically this interface does not work when using your own TaskManager.  Best solution may be to
           open a PR with Reth to fix this.
        let transactions_path = data_dir.txpool_transactions();
        let transactions_backup_config =
            reth_transaction_pool::maintain::LocalTransactionBackupConfig::with_local_txs_backup(transactions_path);

        // spawn task to backup local transaction pool in case of restarts
        ctx.task_executor().spawn_critical_with_graceful_shutdown_signal(
            "local transactions backup task",
            |shutdown| {
                reth_transaction_pool::maintain::backup_local_transactions_task(
                    shutdown,
                    transaction_pool.clone(),
                    transactions_backup_config,
                )
            },
        );
        */

        let mut state_stream = blockchain_provider.canonical_state_stream();
        let this = Self(transaction_pool);
        let txn_pool_clone = this.clone();
        // Update the txn pool as the canonical tip changes.
        task_spawner.spawn_critical_task("canonical txn pool", async move {
            while let Some(update) = state_stream.next().await {
                match update {
                    CanonStateNotification::Commit { new } => {
                        txn_pool_clone.process_canon_state_update(new);
                    }
                    _ => unreachable!("TN reorgs are impossible"),
                }
            }
        });
        Ok(this)
    }

    /// update pool to remove mined transactions
    pub fn update_canonical_state(
        &self,
        new_tip: &SealedBlock,
        pending_block_base_fee: u64,
        pending_block_blob_fee: Option<u128>,
        mined_transactions: Vec<TxHash>,
        changed_accounts: Vec<ChangedAccount>,
    ) {
        // create canonical state update
        let update = CanonicalStateUpdate {
            new_tip,
            pending_block_base_fee,
            pending_block_blob_fee,
            changed_accounts,
            mined_transactions,
            update_kind: PoolUpdateKind::Commit,
        };

        // TODO: should this be a spawned blocking task?
        //
        // update pool to remove mined transactions
        self.0.on_canonical_state_change(update);
    }

    /// Return pending transactions.
    pub fn pending_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.0.pending_transactions()
    }

    /// Return queued transaction (not able to execute yet).
    pub fn queued_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.0.queued_transactions()
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to update pool before building the next block.
    fn process_canon_state_update(&self, update: Arc<Chain>) {
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

        let base_fee_per_gas = tip.base_fee_per_gas.unwrap_or_else(|| self.get_pending_base_fee());
        // sync fn so self will block until all pool updates are complete
        self.update_canonical_state(
            tip.sealed_block(),
            base_fee_per_gas,
            None,
            mined_transactions,
            changed_accounts,
        );
    }

    /// Return the current status of the pool.
    pub fn block_info(&self) -> BlockInfo {
        self.0.block_info()
    }

    /// Set the current status of the pool.
    pub fn set_block_info(&self, block_info: BlockInfo) {
        self.0.set_block_info(block_info);
    }

    /// Return the transactions for an address from the pool.
    pub fn get_transactions_by_sender(&self, address: Address) -> Vec<Arc<PoolTxn>> {
        self.0.get_transactions_by_sender(address)
    }

    /// Adds a local (NOT external) transaction to the pool.
    pub async fn add_transaction_local(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<TxHash, crate::PoolError> {
        self.0.add_transaction(TransactionOrigin::Local, recovered).await
    }

    /// Adds an external transaction to the pool.
    pub async fn add_raw_transaction_external(
        &self,
        tx: TransactionSigned,
    ) -> Result<TxHash, crate::PoolError> {
        let hash = *tx.hash();
        let pooled_tx = tx
            .try_into_pooled()
            .map_err(|_| PoolError::other(hash, "Not into pooled".to_string()))?;
        let recovered = pooled_tx
            .try_into_recovered()
            .map_err(|_| PoolError::other(hash, "Failed to recover ec tx".to_string()))?;
        let eth_tx = EthPooledTransaction::from_pooled(recovered);
        self.0.add_transaction(TransactionOrigin::External, eth_tx).await
    }

    /// Adds a local (NOT external) transaction to the pool and subscribes to transaction events.
    pub async fn add_transaction_and_subscribe_local(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<TransactionEvents, crate::EthApiError> {
        Ok(self.0.add_transaction_and_subscribe(TransactionOrigin::Local, recovered).await?)
    }

    /// Retrieves a transaction by hash from the pool.
    pub fn get(&self, tx: &TxHash) -> Option<Arc<PoolTxn>> {
        self.0.get(tx)
    }

    /// Retrieve the pool size stats for the pool.
    pub fn pool_size(&self) -> PoolSize {
        self.0.pool_size()
    }
}

/// Block info defining a transaction pool status.
pub type BlockInfo = RethBlockInfo;

impl TxPool for WorkerTxPool {
    fn best_transactions(&self) -> BestTxns {
        BestTxns { inner: self.0.best_transactions() }
    }

    /// Return the pending txn base fee.  Currently just the min protocol base fee.
    fn get_pending_base_fee(&self) -> u64 {
        // TODO issue 114: calculate the next basefee HERE for the entire round
        //
        // for now, always use lowest base fee possible
        MIN_PROTOCOL_BASE_FEE
    }
}

/// An iterator that produces the best transactions from a pool.
pub struct BestTxns {
    inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>,
}

impl std::fmt::Debug for BestTxns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BestTxns iterator")
    }
}

impl BestTxns {
    /// Create a new BestTxns (for testing only- normally this comes from a call on the pool).
    pub fn new_for_test(inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>) -> Self {
        Self { inner }
    }
}

impl BestTxns {
    /// When the best transactions exceed our gas limit notify the pool.
    pub fn exceeds_gas_limit(&mut self, pool_tx: &Arc<PoolTxn>, gas_limit: u64) {
        self.inner.mark_invalid(
            pool_tx,
            InvalidPoolTransactionError::ExceedsGasLimit(pool_tx.gas_limit(), gas_limit),
        );
    }

    /// When the best transactions are to large for a batch notify the pool.
    pub fn max_batch_size(&mut self, pool_tx: &Arc<PoolTxn>, tx_size: usize, max_size: usize) {
        self.inner
            .mark_invalid(pool_tx, InvalidPoolTransactionError::OversizedData(tx_size, max_size));
    }
}

impl Iterator for BestTxns {
    type Item = Arc<PoolTxn>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Recover bytes into a transaction.
pub fn recover_raw_transaction(tx: &[u8]) -> TnRethResult<Recovered<TransactionSigned>> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    Ok(recovered)
}

/// Recover bytes into a signed transaction.
pub fn recover_signed_transaction(tx: &[u8]) -> TnRethResult<TransactionSigned> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    Ok(recovered.into_inner())
}

/// Recover a pooled transaction.
pub fn recover_pooled_transaction(
    tx: &[u8],
) -> eyre::Result<EthPooledTransaction<TransactionSigned>> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    let pooled = EthPooledTransaction::try_from_consensus(recovered)?;
    Ok(pooled)
}
