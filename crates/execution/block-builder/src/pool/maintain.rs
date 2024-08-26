//! Maintenance task for worker's transction pool.
//!
//! This background task monitors when the worker has mined the next block to update the transaction pool.
//!
//! see reth-v0.1.3 transaction_pool/src/maintain.rs

use crate::pool::backup::{changed_accounts_iter, load_accounts, LoadedAccounts};

use super::metrics::MaintainPoolMetrics;
use futures_util::{
    future::{BoxFuture, Fuse, FusedFuture},
    FutureExt, Stream, StreamExt,
};
use reth_fs_util::FsPathError;
use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE, Address, BlockHash, BlockNumber, BlockNumberOrTag,
    FromRecoveredPooledTransaction, PooledTransactionsElementEcRecovered, TransactionSigned,
    TryFromRecoveredTransaction, U256,
};
use reth_provider::{
    BlockReaderIdExt, CanonStateNotification, Chain, ChainSpecProvider, ExecutionOutcome,
    ProviderError, StateProviderFactory,
};
use reth_tasks::TaskSpawner;
use reth_transaction_pool::{
    error::PoolError, BlockInfo, CanonicalStateUpdate, ChangedAccount, TransactionOrigin,
    TransactionPool, TransactionPoolExt,
};
use std::{
    borrow::Borrow,
    collections::HashSet,
    future::Future,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::WorkerBlockUpdate;
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

/// Additional settings for maintaining the transaction pool
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMaintenanceConfig {
    /// Maximum number of accounts to reload from state at once when updating the transaction pool.
    ///
    /// Default: 100
    pub max_reload_accounts: usize,
}

impl Default for PoolMaintenanceConfig {
    fn default() -> Self {
        Self { max_reload_accounts: 100 }
    }
}

// maintenance task listens to canon state updates and applies those
// then listens to worker block updates and drains those (should only ever be 1)
// then checks for reloaded accounts request
//
// TODO: can this task support the "pending" block inquiry?

/// Long-running task that updates the transaction pool based on new worker block builds and engine execution.
pub struct MaintainTxPool<Provider, Pool, C, W> {
    /// The configuration for pool maintenance.
    config: PoolMaintenanceConfig,
    /// The type used to query the database.
    provider: Provider,
    /// The transaction pool to maintain.
    pool: Pool,
    /// The stream for canonical state updates.
    canonical_state_updates: C,
    /// The stream for worker block updates.
    ///
    /// These are notifications for when the worker has successfully built a new block and guarantees to broadcast the block. The underlying promise is that this newly built block is stored in the database and the worker will continue to broadcast the block until it reaches a quorum of votes.
    worker_events: W,
    /// The current pending block for the worker.
    ///
    /// This is the last seen block the worker built.
    pending_block: PendingBlockTracker,
    /// Accounts that are out of sync with the pool.
    dirty_addresses: HashSet<Address>,
    /// The current state of maintenance.
    maintenance_state: MaintainedPoolState,
    /// Metrics for maintenance.
    metrics: MaintainPoolMetrics,

    /// The worker's current base fee.
    basefee: Option<u64>,
}

impl<Provider, Pool, C, W> MaintainTxPool<Provider, Pool, C, W>
where
    Pool: TransactionPoolExt,
{
    /// Create a new instance of [Self].
    pub fn new(
        config: PoolMaintenanceConfig,
        provider: Provider,
        pool: Pool,
        canonical_state_updates: C,
        worker_events: W,
        basefee: Option<u64>,
    ) -> Self {
        let metrics = MaintainPoolMetrics::default();

        // TODO: keep track of mined blob transaction so we can clean finalized transactions
        // let mut blob_store_tracker = BlobStoreCanonTracker::default();

        // keeps track of the pending worker's block
        let pending_block = PendingBlockTracker::new(None);

        // keeps track of any dirty accounts that we know of are out of sync with the pool
        let dirty_addresses = HashSet::new();

        // keeps track of the state of the pool wrt to blocks
        let maintenance_state = MaintainedPoolState::InSync;

        Self {
            config,
            provider,
            pool,
            canonical_state_updates,
            worker_events,
            pending_block,
            dirty_addresses,
            maintenance_state,
            metrics,
            basefee,
        }
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to Update pool before building the next block.
    fn process_canon_state_update(&self, update: Arc<Chain>) {
        // TODO: ensure the engine's update includes all accounts that changed during execution
        warn!(target: "Canon update inside block builder!!!!\n", ?update);

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        let mut changed_accounts = Vec::with_capacity(state.state().len());
        for acc in changed_accounts_iter(state) {
            changed_accounts.push(acc);
        }

        // TODO: these should have already been mined when the worker proposed its block
        // but still needs to be included for any txs mined by peers
        let mined_transactions = blocks.transaction_hashes().collect();
        let pending_block_base_fee = self.basefee.unwrap_or(MIN_PROTOCOL_BASE_FEE);

        // Canonical update
        let update = CanonicalStateUpdate {
            new_tip: &tip.block,          // finalized block
            pending_block_base_fee,       // current base fee for worker
            pending_block_blob_fee: None, // current base fee for worker
            changed_accounts,             // finalized block
            mined_transactions,           // finalized block (but these should be a noop)
        };

        // sync fn so self will block until all pool updates are complete
        self.pool.on_canonical_state_change(update);
    }
}

impl<Provider, Pool, C, W> Future for MaintainTxPool<Provider, Pool, C, W>
where
    Provider: StateProviderFactory
        + BlockReaderIdExt
        + ChainSpecProvider
        + Clone
        + Send
        + Unpin
        + 'static,
    Pool: TransactionPoolExt + Unpin + 'static,
    C: Stream<Item = CanonStateNotification> + Send + Unpin + 'static,
    W: Stream<Item = WorkerBlockUpdate> + Send + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let PoolMaintenanceConfig { max_reload_accounts } = self.config;

        // The future that reloads accounts from database.
        let mut reload_accounts_fut = Fuse::terminated();

        let this = self.get_mut();

        // update loop that drains all canon updates and worker updates
        // then updates the pool
        loop {
            trace!(target: "txpool", state=?this.maintenance_state, "awaiting new worker block or canonical udpate");

            this.metrics.set_dirty_accounts_len(this.dirty_addresses.len());
            let pool_info = this.pool.block_info();

            // after performing a pool update after a new block we have some time to properly update
            // dirty accounts and correct if the pool drifted from current state, for example after
            // restart or a pipeline run
            if this.maintenance_state.is_drifted() {
                this.metrics.inc_drift();
                // assuming all senders are dirty
                this.dirty_addresses = this.pool.unique_senders();
                // make sure we toggle the state back to in sync
                this.maintenance_state = MaintainedPoolState::InSync;
            }

            // reload accounts that are out-of-sync in chunks
            if !this.dirty_addresses.is_empty() && reload_accounts_fut.is_terminated() {
                let (tx, rx) = oneshot::channel();
                let c = this.provider.clone();
                let at = pool_info.last_seen_block_hash;
                let fut = if this.dirty_addresses.len() > max_reload_accounts {
                    // need to chunk accounts to reload
                    let accs_to_reload = this
                        .dirty_addresses
                        .iter()
                        .copied()
                        .take(max_reload_accounts)
                        .collect::<Vec<_>>();
                    for acc in &accs_to_reload {
                        // make sure we remove them from the dirty set
                        this.dirty_addresses.remove(acc);
                    }
                    async move {
                        let res = load_accounts(c, at, accs_to_reload.into_iter());
                        let _ = tx.send(res);
                    }
                    .boxed()
                } else {
                    // can fetch all dirty accounts at once
                    let accs_to_reload = std::mem::take(&mut this.dirty_addresses);
                    async move {
                        let res = load_accounts(c, at, accs_to_reload.into_iter());
                        let _ = tx.send(res);
                    }
                    .boxed()
                };
                reload_accounts_fut = rx.fuse();
                tokio::task::spawn_blocking(|| fut);
            }

            // check the reloaded accounts future

            // check for canon updates first
            //
            // this is critical to ensure worker's block is building off canonical tip
            while let Poll::Ready(Some(canon_update)) =
                this.canonical_state_updates.poll_next_unpin(cx)
            {
                // TODO: this needs to update `self` with new tip info
                // for broadcasting worker update

                // TODO: ensure that engine's canon update includes `Chain` with all updates
                // instead of multiple updates

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

            // drain all worker block updates
            while let Poll::Ready(Some(WorkerBlockUpdate { parent, pending, state })) =
                this.worker_events.poll_next_unpin(cx)
            {
                // update pending block
                // update basefee
                // update pool with basefee
                // update
            }

            // TODO: REMOVE THIS BREAK - removes warning for now
            break;
        }

        Poll::Pending
    }
}

/// Returns a spawnable future for maintaining the state of the transaction pool.
pub fn maintain_transaction_pool_future<Provider, P, C, W, Tasks>(
    provider: Provider,
    pool: P,
    canon_events: C,
    worker_events: W,
    task_spawner: Tasks,
    config: PoolMaintenanceConfig,
) -> BoxFuture<'static, ()>
where
    Provider: StateProviderFactory + BlockReaderIdExt + ChainSpecProvider + Clone + Send + 'static,
    P: TransactionPoolExt + 'static,
    C: Stream<Item = CanonStateNotification> + Send + Unpin + 'static,
    W: Stream<Item = WorkerBlockUpdate> + Send + Unpin + 'static,
    Tasks: TaskSpawner + 'static,
{
    async move {
        maintain_transaction_pool(
            provider,
            pool,
            canon_events,
            worker_events,
            task_spawner,
            config,
        )
        .await;
    }
    .boxed()
}

/// Maintains the state of the transaction pool by handling new blocks.
///
/// This listens for any new worker blocks or canonical updates from the engine then updates the transaction pool's state accordingly.
pub async fn maintain_transaction_pool<Provider, P, C, W, Tasks>(
    provider: Provider,
    pool: P,
    mut canon_events: C,
    mut worker_events: W,
    task_spawner: Tasks,
    config: PoolMaintenanceConfig,
) where
    Provider: StateProviderFactory + BlockReaderIdExt + ChainSpecProvider + Clone + Send + 'static,
    P: TransactionPoolExt + 'static,
    C: Stream<Item = CanonStateNotification> + Send + Unpin + 'static,
    W: Stream<Item = WorkerBlockUpdate> + Send + Unpin + 'static,
    Tasks: TaskSpawner + 'static,
{
    let metrics = MaintainPoolMetrics::default();
    let PoolMaintenanceConfig { max_reload_accounts, .. } = config;
    // ensure the pool points to latest state
    if let Ok(Some(latest)) = provider.header_by_number_or_tag(BlockNumberOrTag::Latest) {
        let latest = latest.seal_slow();
        let chain_spec = provider.chain_spec();
        let info = BlockInfo {
            last_seen_block_hash: latest.hash(),
            last_seen_block_number: latest.number,
            pending_basefee: latest
                .next_block_base_fee(chain_spec.base_fee_params_at_block(latest.number + 1))
                .unwrap_or_default(),
            // pending_blob_fee: latest.next_block_blob_fee(),
            pending_blob_fee: None,
        };
        pool.set_block_info(info);
    }

    // // keeps track of mined blob transaction so we can clean finalized transactions
    // let mut blob_store_tracker = BlobStoreCanonTracker::default();

    // keeps track of the latest finalized block
    let mut last_finalized_block =
        PendingBlockTracker::new(provider.finalized_block_number().ok().flatten());

    // keeps track of any dirty accounts that we know of are out of sync with the pool
    let mut dirty_addresses = HashSet::new();

    // keeps track of the state of the pool wrt to blocks
    let mut maintained_state = MaintainedPoolState::InSync;

    // the future that reloads accounts from state
    let mut reload_accounts_fut = Fuse::terminated();

    // The update loop that waits for new blocks and reorgs and performs pool updated
    // Listen for new chain events and derive the update action for the pool
    loop {
        trace!(target: "txpool", state=?maintained_state, "awaiting new block or reorg");

        metrics.set_dirty_accounts_len(dirty_addresses.len());
        let pool_info = pool.block_info();

        // after performing a pool update after a new block we have some time to properly update
        // dirty accounts and correct if the pool drifted from current state, for example after
        // restart or a pipeline run
        if maintained_state.is_drifted() {
            metrics.inc_drift();
            // assuming all senders are dirty
            dirty_addresses = pool.unique_senders();
            // make sure we toggle the state back to in sync
            maintained_state = MaintainedPoolState::InSync;
        }

        // if we have accounts that are out of sync with the pool, we reload them in chunks
        if !dirty_addresses.is_empty() && reload_accounts_fut.is_terminated() {
            let (tx, rx) = oneshot::channel();
            let c = provider.clone();
            let at = pool_info.last_seen_block_hash;
            let fut = if dirty_addresses.len() > max_reload_accounts {
                // need to chunk accounts to reload
                let accs_to_reload =
                    dirty_addresses.iter().copied().take(max_reload_accounts).collect::<Vec<_>>();
                for acc in &accs_to_reload {
                    // make sure we remove them from the dirty set
                    dirty_addresses.remove(acc);
                }
                async move {
                    let res = load_accounts(c, at, accs_to_reload.into_iter());
                    let _ = tx.send(res);
                }
                .boxed()
            } else {
                // can fetch all dirty accounts at once
                let accs_to_reload = std::mem::take(&mut dirty_addresses);
                async move {
                    let res = load_accounts(c, at, accs_to_reload.into_iter());
                    let _ = tx.send(res);
                }
                .boxed()
            };
            reload_accounts_fut = rx.fuse();
            task_spawner.spawn_blocking(fut);
        }

        // check if we have a new finalized block
        if let Some(_finalized) =
            last_finalized_block.update(provider.finalized_block_number().ok().flatten())
        {
            // TODO: this should update canon chain for worker / tx pool?
            // would need to pull SealedBlock, bundle, etc. to handle
            // updated accounts

            // match blob_store_tracker.on_finalized_block(finalized) {
            //     BlobStoreUpdates::None => {}
            //     BlobStoreUpdates::Finalized(blobs) => {
            //         metrics.inc_deleted_tracked_blobs(blobs.len());
            //         // remove all finalized blobs from the blob store
            //         pool.delete_blobs(blobs);
            //     }
            // }
            // // also do periodic cleanup of the blob store
            // let pool = pool.clone();
            // task_spawner.spawn_blocking(Box::pin(async move {
            //     debug!(target: "txpool", finalized_block = %finalized, "cleaning up blob store");
            //     pool.cleanup_blobs();
            // }));
        }

        // outcomes of the futures we are waiting on
        let mut event = None;
        let mut reloaded = None;

        // select of account reloads and new canonical state updates which should arrive at the rate
        // of the worker block proposal time (default 1s)
        //
        // NOTE: reth expects this task to run ~12s
        tokio::select! {
            res = &mut reload_accounts_fut =>  {
                reloaded = Some(res);
            }
            ev = worker_events.next() =>  {
                 if ev.is_none() {
                    // the stream ended - task is done
                    break;
                }
                event = ev;
            }
        }

        // handle the result of the account reload
        match reloaded {
            Some(Ok(Ok(LoadedAccounts { accounts, failed_to_load }))) => {
                // reloaded accounts successfully
                // extend accounts we failed to load from database
                dirty_addresses.extend(failed_to_load);
                // update the pool with the loaded accounts
                pool.update_accounts(accounts);
            }
            Some(Ok(Err(res))) => {
                // Failed to load accounts from state
                let (accs, err) = *res;
                debug!(target: "txpool", %err, "failed to load accounts");
                dirty_addresses.extend(accs);
            }
            Some(Err(_)) => {
                // failed to receive the accounts, sender dropped, only possible if task panicked
                maintained_state = MaintainedPoolState::Drifted;
            }
            None => {}
        }

        // handle the new block
        let Some(WorkerBlockUpdate { parent, pending, state }) = event else { continue };
        //
        let chain_spec = provider.chain_spec();

        // fees for the next block: `pending worker block +1`
        let pending_block_base_fee = pending
            .next_block_base_fee(chain_spec.base_fee_params_at_block(parent.number + 1))
            .unwrap_or_default();
        // let pending_block_blob_fee = pending.next_block_blob_fee();

        trace!(
            target: "txpool",
            pending = pending.number,
            ?parent,
            pool_block = pool_info.last_seen_block_number,
            "update pool on new commit"
        );

        let mut changed_accounts = Vec::with_capacity(state.state().len());
        for acc in changed_accounts_iter(&state) {
            // we can always clear the dirty flag for this account
            dirty_addresses.remove(&acc.address);
            changed_accounts.push(acc);
        }

        let mined_transactions = pending.transactions().map(|tx| tx.hash).collect();

        // check if the range of the commit is canonical with the pool's block
        if pending.parent_hash != pool_info.last_seen_block_hash {
            // we received a new canonical chain commit but the commit is not canonical with
            // the pool's block, this could happen after initial sync or
            // long re-sync
            maintained_state = MaintainedPoolState::Drifted;
        }

        // TODO: is there a better way to do this?
        //
        // ensure parent is still the latest, otherwise pull from db

        // update pool with worker's pending block update
        let update = CanonicalStateUpdate {
            new_tip: &parent,             // parent
            pending_block_base_fee,       // worker
            pending_block_blob_fee: None, // worker
            changed_accounts,             // worker
            mined_transactions,           // worker
        };

        pool.on_canonical_state_change(update);

        // keep track of mined blob transactions
        // blob_store_tracker.add_new_chain_blocks(&blocks);
    }
}

struct PendingBlockTracker {
    last_finalized_block: Option<BlockNumber>,
}

impl PendingBlockTracker {
    const fn new(last_finalized_block: Option<BlockNumber>) -> Self {
        Self { last_finalized_block }
    }

    /// Updates the tracked finalized block and returns the new finalized block if it changed
    fn update(&mut self, finalized_block: Option<BlockNumber>) -> Option<BlockNumber> {
        match (self.last_finalized_block, finalized_block) {
            (Some(last), Some(finalized)) => {
                self.last_finalized_block = Some(finalized);
                if last < finalized {
                    Some(finalized)
                } else {
                    None
                }
            }
            (None, Some(finalized)) => {
                self.last_finalized_block = Some(finalized);
                Some(finalized)
            }
            _ => None,
        }
    }
}

/// Keeps track of the pool's state, whether the accounts in the pool are in sync with the actual
/// state.
#[derive(Debug, PartialEq, Eq)]
enum MaintainedPoolState {
    /// Pool is assumed to be in sync with the current state
    InSync,
    /// Pool could be out of sync with the state
    Drifted,
}

impl MaintainedPoolState {
    /// Returns `true` if the pool is assumed to be out of sync with the current state.
    #[inline]
    const fn is_drifted(&self) -> bool {
        matches!(self, Self::Drifted)
    }
}

/// A unique `ChangedAccount` identified by its address that can be used for deduplication
#[derive(Eq)]
struct ChangedAccountEntry(ChangedAccount);

impl PartialEq for ChangedAccountEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.address == other.0.address
    }
}

impl Hash for ChangedAccountEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.address.hash(state);
    }
}

impl Borrow<Address> for ChangedAccountEntry {
    fn borrow(&self) -> &Address {
        &self.0.address
    }
}
