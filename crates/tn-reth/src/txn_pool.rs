//! Implement an abstraction around the Reth transaction pool.
//! This should isolate from shifting Reth internals, etc.
//!
//! TN-specific pool behavior worth knowing:
//!
//! - [`WorkerTxPool::new`] spawns a CRITICAL task consuming the provider's raw canonical-state
//!   broadcast subscription, applying each `Commit` notification to the pool (mined transactions
//!   removed, changed accounts refreshed). A `Reorg` notification is skipped with a warning: TN
//!   never reorgs (consensus output only extends the canonical chain) and aborting the critical
//!   task would take down the whole node. The task subscribes to the raw receiver rather than
//!   `canonical_state_stream()` (whose wrapper silently swallows broadcast lag) so it can observe
//!   `Lagged`, mark every pool sender dirty, and reload canonical account state in bounded chunks,
//!   discarding transactions mined in the lost rounds (issue #1236).
//! - [`TxPool::get_pending_base_fee`] currently returns `MIN_PROTOCOL_BASE_FEE` (7 wei)
//!   unconditionally; issue 114 tracks computing a real per-round base fee. Both callers (the task
//!   above and the batch builder's maintenance path) use it only as the fallback when a tip header
//!   carries no `base_fee_per_gas` — otherwise the pool's pending base fee tracks the new tip's.
//! - [`new_pool_txn`] hard-codes `propagate: false` (reth's flag for devp2p tx gossip): transaction
//!   distribution happens via the worker batch protocol, and observer nodes forward RPC submissions
//!   to committee validators over JSON-RPC (see `forward.rs`) — never via devp2p gossip.
//! - The per-sender slot default is 256 (`TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER` in `src/cli.rs`,
//!   seeded process-wide by `init_reth_defaults`) instead of reth's 16.
//! - Blob (EIP-4844) transactions are unsupported in batches: the batch builder strips them via
//!   [`TxPool::remove_eip4844_txs`] (removes descendants and deletes sidecars from the blob store),
//!   and every canonical pool update — `process_canon_state_update` here and the batch builder's
//!   equivalent — passes `pending_block_blob_fee: Some(u128::MAX)`, pricing all blob transactions
//!   out of the pending set.

use alloy::primitives::map::AddressSet;
use futures::StreamExt as _;
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, EthTransactionPool,
    TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_primitives_traits::SignerRecoverable;
use reth_provider::{
    providers::BlockchainProvider, AccountReader as _, CanonStateNotification,
    CanonStateSubscriptions as _, Chain, ChangedAccount,
};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    error::{
        Eip4844PoolTransactionError, Eip7702PoolTransactionError, InvalidPoolTransactionError,
        PoolError,
    },
    AddedTransactionOutcome, BestTransactions, CanonicalStateUpdate, EthPooledTransaction,
    PoolSize, PoolTransaction, PoolUpdateKind, TransactionEvents, TransactionOrigin,
    TransactionPool as _, TransactionPoolExt as _, ValidPoolTransaction,
};
use std::{sync::Arc, time::Instant};
use tn_types::{
    Address, EnvKzgSettings, Recovered, SealedBlock, TaskError, TaskSpawner, TransactionSigned,
    TxHash, MIN_PROTOCOL_BASE_FEE, U256,
};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tracing::{debug, info, trace, warn};

use crate::{
    error::TnRethResult, evm::TnEvmConfig, metrics::RETH_METRICS, traits::TelcoinNode, PoolTxn,
    PoolTxnId,
};

pub use reth_primitives_traits::InMemorySize as TxnSize;

/// Upper bound on canonical account reads per maintenance-loop iteration while recovering
/// from canonical-state broadcast lag (reth's `max_reload_accounts` analogue).
///
/// Lag means the loop is already behind, so recovery must not stall it further: each
/// iteration reloads at most this many dirty senders and carries the rest to the next
/// iteration, which arrives at consensus-round rate.
const MAX_RELOAD_ACCOUNTS: usize = 100;

/// Generate a new pooled transaction from an eth transaction and id.
///
/// Hard-codes `propagate: false`: reth's `propagate` flag drives devp2p tx gossip, which TN
/// does not use — transactions move between nodes through the worker batch protocol and the
/// observer JSON-RPC forwarder (`forward.rs`).
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

/// Trait on a transaction pool to produce the best transaction.
pub trait TxPool {
    /// Return an iterator over the best transactions in a pool.
    fn best_transactions(&self) -> BestTxns;
    /// Return the pending txn base fee.
    fn get_pending_base_fee(&self) -> u64;
    /// Remove EIP-4844 blob transactions from the pool and delete the sidecars from blob store.
    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>);
    /// Remove transactions whose EIP-2718 type is outside the executable allowlist from the
    /// pool, along with their descendants.
    fn remove_unsupported_txs(&mut self, txs: Vec<TxHash>);
    /// Return the canonical balance of `address` as of the latest committed block.
    ///
    /// Used to build the optimistic per-sender balance in a post-mining pool update. A missing
    /// account (or a read error) yields [`U256::ZERO`], which is the conservative choice: it can
    /// only keep a sender's remaining transactions parked, never promote an unfunded one, and the
    /// engine's authoritative canonical update corrects it within the same consensus round.
    fn get_account_balance(&self, address: Address) -> U256;
}

/// A telcoin network transaction pool.
///
/// The second field is a handle to the blockchain provider, retained so the pool can read a
/// sender's canonical balance when constructing optimistic pool updates after mining a batch
/// (see [`TxPool::get_account_balance`]).
#[derive(Clone, Debug)]
pub struct WorkerTxPool(
    EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore, TnEvmConfig>,
    BlockchainProvider<TelcoinNode>,
);

impl From<WorkerTxPool>
    for EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore, TnEvmConfig>
{
    fn from(value: WorkerTxPool) -> Self {
        value.0
    }
}

impl WorkerTxPool {
    /// Create a new instance of `Self` and spawn its canonical-state maintenance task.
    pub fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
    ) -> eyre::Result<Self> {
        let this = Self::build(node_config, task_spawner, blockchain_provider, evm_config)?;
        this.spawn_maintenance_task(task_spawner, blockchain_provider);
        Ok(this)
    }

    /// Construct the pool without spawning the canonical-state maintenance task.
    ///
    /// Kept separate from [`WorkerTxPool::new`] so tests can reproduce a pool that missed
    /// canonical updates (the drifted state the maintenance task's lag handling recovers
    /// from) without racing a live subscription (see issue #1236).
    pub(crate) fn build(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
    ) -> eyre::Result<Self> {
        let data_dir = node_config.datadir();
        let pool_config = node_config.txpool.pool_config();
        let blob_store = DiskFileBlobStore::open(data_dir.blobstore(), Default::default())?;
        let validator = TransactionValidationTaskExecutor::eth_builder(
            blockchain_provider.clone(),
            evm_config.clone(),
        )
        // Reject EIP-4844 (blob) and EIP-7702 (set-code) transactions at admission. TN never
        // mines either type: the batch builder strips them and the batch validator rejects any
        // batch that carries one, so an admitted transaction of either type can never be executed.
        // For blobs this is also a denial-of-service fix. On a successful add reth writes the blob
        // sidecar to the on-disk DiskFileBlobStore, but that store uses deferred deletion whose
        // only unlink runs in reth's maintain_transaction_pool loop. TN drives pool
        // maintenance itself and never runs that loop, so nothing removes the sidecars at
        // runtime and a remote unprivileged sender could grow a validator's disk without
        // bound. Rejecting both unsupported types here, before insertion, closes that
        // vector and mirrors reth's own node builder for a chain that supports neither
        // type. See issue #1159.
        .no_eip4844()
        .no_eip7702()
        .kzg_settings(EnvKzgSettings::Default)
        // Apply the operator's `--rpc.txfeecap`. The validator checks it only for
        // transactions it treats as local (`LocalTransactionConfig::is_local`); raw
        // RPC submissions are External, so `crate::rpc_fee_cap` guards those at the
        // RPC boundary (issue #1160).
        .set_tx_fee_cap(node_config.rpc.rpc_tx_fee_cap)
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

        Ok(Self(transaction_pool, blockchain_provider.clone()))
    }

    /// Spawn the CRITICAL task that applies canonical-state updates to the pool.
    ///
    /// Subscribes to the raw broadcast receiver rather than `canonical_state_stream()`: the
    /// stream wrapper maps broadcast lag to a debug log and skips ahead, so a consumer that
    /// falls more than the channel capacity (256 in reth v1.11.3) behind loses `Commit`
    /// notifications without ever observing the gap: mined transactions stay pending and
    /// sender snapshots go stale, permanently, because later notifications carry only their
    /// own rounds (issue #1236). Wrapping the receiver in a [`BroadcastStream`] keeps the
    /// `Stream` shape but surfaces `Lagged` as an error item, so the task can mark every
    /// pool sender dirty and reload canonical account state in bounded chunks, mirroring
    /// reth's `maintain_transaction_pool` drift recovery.
    fn spawn_maintenance_task(
        &self,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
    ) {
        let mut state_stream =
            BroadcastStream::new(blockchain_provider.subscribe_to_canonical_state());
        let txn_pool_clone = self.clone();
        // Update the txn pool as the canonical tip changes.
        task_spawner.spawn_critical_task("canonical txn pool", async move {
            let mut dirty_addresses = AddressSet::default();
            while let Some(update) = state_stream.next().await {
                let newly_dirty = update
                    .map(|notification| {
                        txn_pool_clone.apply_canon_notification(notification);
                        AddressSet::default()
                    })
                    .unwrap_or_else(|BroadcastStreamRecvError::Lagged(missed)| {
                        txn_pool_clone.mark_drifted(missed)
                    });
                dirty_addresses = txn_pool_clone.reload_dirty_accounts(
                    dirty_addresses.into_iter().chain(newly_dirty).collect(),
                    MAX_RELOAD_ACCOUNTS,
                );
            }
            Err(TaskError::from_message(
                "canonical txn pool task ended because state_stream closed",
            ))
        });
    }

    /// Apply one canonical-state notification to the pool.
    fn apply_canon_notification(&self, notification: CanonStateNotification) {
        match notification {
            CanonStateNotification::Commit { new } => self.process_canon_state_update(new),
            // TN never reorgs: consensus output only extends the canonical chain, so a
            // Reorg notification here is a bug upstream. Skip it rather than panic . . .
            // this runs inside a critical task, and aborting it would take down the
            // whole node over a pool-maintenance miss.
            CanonStateNotification::Reorg { .. } => warn!(
                target: "txpool",
                "unexpected canonical state notification (TN never reorgs); skipping \
                 transaction pool update"
            ),
        }
    }

    /// Record a canonical-state broadcast lag event and return the sender set to resync.
    ///
    /// The skipped `Commit`s are gone from the broadcast channel, so their mined
    /// transactions and changed accounts can never be replayed: treat every sender with
    /// transactions in the pool as dirty, exactly like reth's
    /// `MaintainedPoolState::Drifted`.
    fn mark_drifted(&self, missed: u64) -> AddressSet {
        warn!(
            target: "txpool",
            missed,
            "canonical state notifications lost to broadcast lag; resyncing pool sender \
             accounts from canonical state"
        );
        RETH_METRICS.canon_state_lagged_total.increment(1);
        self.0.unique_senders()
    }

    /// Reload up to `max_reload` dirty sender accounts from canonical state, apply them to
    /// the pool, and return the addresses still awaiting reload.
    ///
    /// Applying the loaded accounts via `update_accounts` refreshes each sender's
    /// nonce/balance snapshot and discards pool transactions whose nonce is below the
    /// canonical account nonce, i.e. the transactions mined in the lost rounds. The
    /// per-call bound keeps the maintenance loop responsive during recovery (see
    /// [`MAX_RELOAD_ACCOUNTS`]); addresses whose state read fails stay dirty and are
    /// retried on a later iteration.
    ///
    /// The reload reads the LATEST canonical state while older retained notifications may
    /// still be queued behind the `Lagged` marker. Draining those can transiently re-apply
    /// an older snapshot for a sender, but the drain's own tail restores exact state: the
    /// backlog's final `Commit` is authoritative for every sender it touches, and senders
    /// touched only in the lost rounds are never overwritten by the backlog at all.
    /// Discarded transactions cannot be resurrected by the transient regression.
    fn reload_dirty_accounts(&self, dirty: AddressSet, max_reload: usize) -> AddressSet {
        let mut pending = dirty.into_iter();
        let chunk: Vec<Address> = pending.by_ref().take(max_reload).collect();
        let loaded: Vec<Result<ChangedAccount, Address>> =
            chunk.into_iter().map(|address| self.load_changed_account(address)).collect();
        let accounts: Vec<ChangedAccount> =
            loaded.iter().filter_map(|result| result.as_ref().ok().copied()).collect();
        if !accounts.is_empty() {
            self.0.update_accounts(accounts);
        }
        pending.chain(loaded.iter().filter_map(|result| result.as_ref().err().copied())).collect()
    }

    /// Read `address`'s canonical account and shape it as a [`ChangedAccount`] for the pool.
    ///
    /// A missing account maps to [`ChangedAccount::empty`] (nonce 0, zero balance), matching
    /// reth's `load_accounts`; a provider read error returns the address so the caller keeps
    /// it dirty and retries.
    fn load_changed_account(&self, address: Address) -> Result<ChangedAccount, Address> {
        self.1
            .basic_account(&address)
            .map(|maybe_account| {
                maybe_account
                    .map(|account| ChangedAccount {
                        address,
                        nonce: account.nonce,
                        balance: account.balance,
                    })
                    .unwrap_or_else(|| ChangedAccount::empty(address))
            })
            .map_err(|error| {
                debug!(
                    target: "txpool",
                    ?address,
                    ?error,
                    "failed to reload account state for pool resync"
                );
                address
            })
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
            Some(u128::MAX), // set max fee for blobs
            mined_transactions,
            changed_accounts,
        );
    }

    /// Return the current status of the pool.
    pub fn block_info(&self) -> RethBlockInfo {
        self.0.block_info()
    }

    /// Set the current status of the pool.
    pub fn set_block_info(&self, block_info: RethBlockInfo) {
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
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        self.0.add_transaction(TransactionOrigin::Local, recovered).await
    }

    /// Adds an external transaction to the pool.
    pub async fn add_raw_transaction_external(
        &self,
        tx: TransactionSigned,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
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

    /// Adds an already-recovered external transaction to the pool, avoiding redundant ECDSA
    /// recovery. Used to submit gossipped transactions.
    pub async fn add_recovered_transaction_external(
        &self,
        recovered: Recovered<TransactionSigned>,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        let hash = *recovered.hash();
        let eth_tx = EthPooledTransaction::try_from_consensus(recovered)
            .map_err(|_| PoolError::other(hash, "Failed to create pooled tx".to_string()))?;
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

    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>) {
        self.0.remove_transactions_and_descendants(blobs.clone());
        self.0.delete_blobs(blobs);
    }

    fn remove_unsupported_txs(&mut self, txs: Vec<TxHash>) {
        self.0.remove_transactions_and_descendants(txs);
    }

    fn get_account_balance(&self, address: Address) -> U256 {
        self.1
            .basic_account(&address)
            .ok()
            .flatten()
            .map(|account| account.balance)
            .unwrap_or(U256::ZERO)
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
            &InvalidPoolTransactionError::ExceedsGasLimit(pool_tx.gas_limit(), gas_limit),
        );
    }

    /// When the best transactions are too large for a batch notify the pool.
    pub fn max_batch_size(&mut self, pool_tx: &Arc<PoolTxn>, tx_size: usize, max_size: usize) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::OversizedData { size: tx_size, limit: max_size },
        );
    }

    /// Mark the EIP-4844 transaction as invalid.
    pub fn ignore_eip4844(&mut self, pool_tx: &Arc<PoolTxn>) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::Eip4844(Eip4844PoolTransactionError::NoEip4844Blobs),
        );
    }

    /// Mark a transaction outside the executable type allowlist as invalid.
    ///
    /// Mirrors [`Self::ignore_eip4844`]: the nearest upstream error kind stands in for a
    /// type the batch allowlist refuses (only EIP-7702 decodes today).
    pub fn ignore_eip7702(&mut self, pool_tx: &Arc<PoolTxn>) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::Eip7702(
                Eip7702PoolTransactionError::MissingEip7702AuthorizationList,
            ),
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            TransactionFactory,
        },
        RethChainSpec, RethEnv,
    };
    use rand::{rngs::StdRng, SeedableRng as _};
    use reth_chainspec::EthChainSpec as _;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_types::{
        test_genesis, Address, Bytes, Encodable2718 as _, GenesisAccount, TaskManager, U256,
    };

    /// Build a pool over a chain whose genesis funds the factory's sender, so a rejected
    /// transaction can only be refused by a validator policy, never by insufficient balance.
    fn funded_pool_for_test(
        tx_factory: &TransactionFactory,
        tmp_dir: &TempDir,
        task_manager: &TaskManager,
    ) -> (Arc<RethChainSpec>, RethEnv, WorkerTxPool) {
        let genesis = test_genesis().extend_accounts([(
            tx_factory.address(),
            GenesisAccount::default().with_balance(U256::MAX),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), task_manager, None).unwrap();
        let pool = reth_env.init_txn_pool().unwrap();
        (chain, reth_env, pool)
    }

    #[test]
    fn test_recover_raw_transaction_preserves_signer() {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let mut tx_factory = TransactionFactory::new();
        let tx = tx_factory.create_eip1559(
            chain,
            None,
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let original_hash = *tx.hash();
        let encoded = tx.encoded_2718();

        let recovered = recover_raw_transaction(&encoded).expect("recovery should succeed");
        assert_eq!(recovered.signer(), tx_factory.address());
        assert_eq!(*recovered.hash(), original_hash);
    }

    #[test]
    fn test_recover_raw_transaction_invalid_bytes() {
        assert!(recover_raw_transaction(b"not a real transaction").is_err());
    }

    #[tokio::test]
    async fn test_add_recovered_transaction_external() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool().unwrap();

        let mut tx_factory = TransactionFactory::new();
        let tx = tx_factory.create_eip1559(
            chain,
            None,
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let encoded = tx.encoded_2718();
        let recovered = recover_raw_transaction(&encoded).unwrap();
        let hash = *recovered.hash();

        let result = pool.add_recovered_transaction_external(recovered).await;
        assert!(result.is_ok());
        assert_eq!(pool.pool_size().pending, 1);
        assert!(pool.get(&hash).is_some());
    }

    /// The DoS fix for issue #1159: the pool refuses EIP-4844 (blob) transactions at
    /// admission. The sender is funded at genesis and the blob's KZG proof is valid, so the
    /// only remaining reason for rejection is the `.no_eip4844()` type gate in
    /// [`WorkerTxPool::new`].
    #[tokio::test]
    async fn test_pool_rejects_blob_transaction() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let mut tx_factory = TransactionFactory::new_random();
        let (chain, reth_env, pool) = funded_pool_for_test(&tx_factory, &tmp_dir, &task_manager);

        let gas_price = reth_env.get_gas_price().unwrap();
        let pooled = tx_factory.create_eip4844_pooled(chain.clone(), None, gas_price);
        let result = pool.add_transaction_local(pooled).await;
        assert!(result.is_err());

        // The pool admitted nothing. Reth writes the blob sidecar to the blob store only on
        // successful insertion, so an empty pool proves no sidecar reached disk.
        let s = pool.pool_size();
        assert_eq!(s.pending, 0);
        assert_eq!(s.blob, 0);
        assert_eq!(s.queued, 0);
    }

    /// The pool refuses EIP-7702 (set-code) transactions at admission. Prague is active at
    /// genesis so the transaction is fork-valid, and the sender is funded, so rejection is due
    /// to the `.no_eip7702()` type gate in [`WorkerTxPool::new`], consistent with TN's existing
    /// policy of treating EIP-7702 as an unsupported transaction type.
    #[tokio::test]
    async fn test_pool_rejects_eip7702_transaction() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let mut tx_factory = TransactionFactory::new_random();
        let (chain, reth_env, pool) = funded_pool_for_test(&tx_factory, &tmp_dir, &task_manager);

        let gas_price = reth_env.get_gas_price().unwrap();
        let signed = tx_factory.create_eip7702(chain.chain_id(), None, gas_price);
        // 7702 carries no sidecar, so the production external ingress accepts the raw tx.
        let result = pool.add_raw_transaction_external(signed).await;
        assert!(result.is_err());

        // The pool admitted nothing.
        let s = pool.pool_size();
        assert_eq!(s.pending, 0);
        assert_eq!(s.blob, 0);
        assert_eq!(s.queued, 0);
    }

    #[tokio::test]
    async fn test_recover_and_submit_batch_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool().unwrap();

        let mut tx_factory = TransactionFactory::new();
        let encoded_txs: Vec<Vec<u8>> = (0..3)
            .map(|_| {
                tx_factory
                    .create_eip1559(
                        chain.clone(),
                        None,
                        7,
                        Some(Address::ZERO),
                        U256::from(100),
                        Bytes::new(),
                    )
                    .encoded_2718()
            })
            .collect();

        for encoded in &encoded_txs {
            let recovered = recover_raw_transaction(encoded).unwrap();
            let result = pool.add_recovered_transaction_external(recovered).await;
            assert!(result.is_ok());
        }
        assert_eq!(pool.pool_size().pending, 3);
    }

    /// Issue #1236: after a canonical-state broadcast lag, the drift resync reloads sender
    /// accounts from canonical state and discards transactions mined in the lost rounds.
    ///
    /// The pool is built WITHOUT its maintenance task, then a block that mines the pool's
    /// transaction is committed to the canonical chain. This reproduces exactly the state a
    /// lagged worker is left in: the mined transaction still pending and the sender snapshot
    /// stale. The pre-resync assertion is the negative control proving the drift is real;
    /// the resync must then clear it.
    #[tokio::test]
    async fn test_lag_resync_discards_transactions_mined_in_lost_rounds() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let mut tx_factory = TransactionFactory::new_random();
        let genesis = test_genesis().extend_accounts([(
            tx_factory.address(),
            GenesisAccount::default().with_balance(U256::MAX),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool_without_maintenance().unwrap();

        let tx = tx_factory.create_eip1559(
            chain.clone(),
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let hash = *tx.hash();
        let encoded = tx.encoded_2718();
        let recovered = recover_raw_transaction(&encoded).unwrap();
        pool.add_recovered_transaction_external(recovered).await.unwrap();
        assert_eq!(pool.pool_size().pending, 1);

        // commit a canonical block that mines the transaction; with no maintenance task
        // subscribed, the pool never sees the notification . . . the lag scenario
        let output = consensus_output_for_tests(1, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![encoded]).unwrap();

        // the block must actually mine the transaction (canonical nonce advanced), or the
        // resync below would pass vacuously
        let account = pool.load_changed_account(tx_factory.address()).unwrap();
        assert_eq!(account.nonce, 1, "test block must mine the transaction");
        // negative control: the pool is drifted, the mined transaction is still pending
        assert_eq!(pool.pool_size().pending, 1);

        // the lag path: mark drifted and reload the dirty senders
        let dirty = pool.mark_drifted(1);
        let remaining = pool.reload_dirty_accounts(dirty, MAX_RELOAD_ACCOUNTS);

        assert!(remaining.is_empty());
        assert_eq!(pool.pool_size().pending, 0);
        assert!(pool.get(&hash).is_none());
    }

    /// The resync reload is bounded: each call reloads at most `max_reload` addresses and
    /// returns the rest, so a large sender set drains across maintenance-loop iterations
    /// instead of stalling one.
    #[tokio::test]
    async fn test_reload_dirty_accounts_is_bounded() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool_without_maintenance().unwrap();

        let dirty: AddressSet = (1u8..=3).map(Address::repeat_byte).collect();

        let after_one = pool.reload_dirty_accounts(dirty, 1);
        assert_eq!(after_one.len(), 2);
        let after_two = pool.reload_dirty_accounts(after_one, 1);
        assert_eq!(after_two.len(), 1);
        let after_three = pool.reload_dirty_accounts(after_two, 1);
        assert!(after_three.is_empty());
    }

    #[tokio::test]
    async fn test_validator_applies_tx_fee_cap_to_local_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        // 1,000 wei cap: a 21,000-gas transfer at 7 wei/gas costs at most 147,000 wei.
        let rpc_args = reth::args::RpcServerArgs { rpc_tx_fee_cap: 1_000, ..Default::default() };
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain.clone(),
            tmp_dir.path(),
            &task_manager,
            None,
            rpc_args,
        )
        .unwrap();
        let pool = reth_env.init_txn_pool().unwrap();

        let mut tx_factory = TransactionFactory::new();
        let tx = tx_factory.create_eip1559(
            chain,
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let pooled = tx.try_into_pooled().unwrap().try_into_recovered().unwrap();
        let err = pool
            .add_transaction_local(EthPooledTransaction::from_pooled(pooled))
            .await
            .expect_err("local transaction over the cap is refused by the validator");
        assert!(format!("{err:?}").contains("ExceedsFeeCap"), "unexpected error: {err:?}");
    }

    #[test]
    fn test_parallel_recovery_preserves_order() {
        use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
        use tn_types::Encodable2718;

        // Create 20 transactions from different random signers so each tx is unique.
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let num_txs = 20;
        let mut encoded_txs = Vec::with_capacity(num_txs);
        for i in 0..num_txs {
            let mut factory =
                TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(i as u64));
            let tx = factory.create_eip1559(
                chain.clone(),
                None,
                100_000,
                Some(Address::ZERO),
                U256::from(1),
                Default::default(),
            );
            encoded_txs.push(tx.encoded_2718());
        }

        // Recover sequentially
        let sequential: Vec<_> = encoded_txs
            .iter()
            .map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .expect("sequential recovery")
            })
            .collect();

        // Recover in parallel (using rayon, same as production code)
        let parallel: Vec<_> = encoded_txs
            .par_iter()
            .map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .expect("parallel recovery")
            })
            .collect();

        // Assert same length
        assert_eq!(sequential.len(), parallel.len());

        // Assert same order by comparing tx hashes and recovered signer addresses
        for (seq, par) in sequential.iter().zip(parallel.iter()) {
            assert_eq!(seq.hash(), par.hash(), "transaction hashes must match in order");
            assert_eq!(seq.signer(), par.signer(), "recovered signers must match in order");
        }
    }
}
