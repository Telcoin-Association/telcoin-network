//! Maintenance task for worker's transction pool.
//!
//! This background task monitors when the worker has mined the next block to update the transaction pool.
//!
//! see reth-v0.1.3 transaction_pool/src/maintain.rs

use crate::pool::backup::{changed_accounts_iter, load_accounts, LoadedAccounts};

use super::metrics::MaintainPoolMetrics;
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt};
use reth_chainspec::ChainSpec;
use reth_primitives::{constants::MIN_PROTOCOL_BASE_FEE, Address, U256};
use reth_provider::{
    BlockReaderIdExt, CanonStateNotification, Chain, ChainSpecProvider, ProviderError,
    StateProviderFactory,
};
use reth_tasks::TaskSpawner;
use reth_transaction_pool::{CanonicalStateUpdate, TransactionPoolExt};
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tracing::{debug, trace};

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

//
//
// new strategy:
// - collect and combine all canon updates
// - extend any worker updates as well with the canon ones
// - apply pool on change once at the end
// - if no messages, return pending
//
// engine updates:
// - update this canonical_tip
// - extend changed accounts
// - extend mined transactions
//
// worker updates:
// - update this base fee
// - extend changed accounts
// - extend mined transactions
//
// then apply pool update
//
//  TODO: short-circuit rpc looking up pending block
//  TODO: either faucet subscribes to worker block updates OR keeps track of x amount of transactions it successfully submitted to keep track of own-nonce

/// Long-running task that updates the transaction pool based on new worker block builds and engine execution.
pub struct MaintainTxPool<Provider, Pool, C> {
    /// The configuration for pool maintenance.
    config: PoolMaintenanceConfig,
    /// The type used to query the database.
    provider: Provider,
    /// The transaction pool to maintain.
    pool: Pool,
    /// The stream for canonical state updates.
    canonical_state_updates: C,
    /// Accounts that are out of sync with the pool.
    dirty_addresses: HashSet<Address>,
    /// The current state of maintenance.
    maintenance_state: MaintainedPoolState,
    /// Metrics for maintenance.
    metrics: MaintainPoolMetrics,

    /// The worker's current base fee.
    basefee: Option<u64>,

    /// The task for reloading dirty accounts.
    reload_accounts_task: Option<ReloadAccountsTask>,
}

/// The task for reloading drifted accounts.
type ReloadAccountsTask =
    oneshot::Receiver<Result<LoadedAccounts, Box<(HashSet<Address>, ProviderError)>>>;

impl<Provider, Pool, C> MaintainTxPool<Provider, Pool, C>
where
    Pool: TransactionPoolExt + Unpin,
{
    /// Create a new instance of [Self].
    pub fn new(
        config: PoolMaintenanceConfig,
        provider: Provider,
        pool: Pool,
        canonical_state_updates: C,
        basefee: Option<u64>,
    ) -> Self {
        let metrics = MaintainPoolMetrics::default();

        // TODO: keep track of mined blob transaction so we can clean finalized transactions
        // let mut blob_store_tracker = BlobStoreCanonTracker::default();

        // keeps track of any dirty accounts that we know of are out of sync with the pool
        let dirty_addresses = HashSet::new();

        // keeps track of the state of the pool wrt to blocks
        let maintenance_state = MaintainedPoolState::InSync;

        // assume account state is clean
        let reload_accounts_task = None;

        Self {
            config,
            provider,
            pool,
            canonical_state_updates,
            dirty_addresses,
            maintenance_state,
            metrics,
            basefee,
            reload_accounts_task,
        }
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to Update pool before building the next block.
    fn process_canon_state_update(&self, update: Arc<Chain>) {
        trace!(target: "worker::pool_maintenance", ?update, "canon state update from engine");

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        let mut changed_accounts = Vec::with_capacity(state.state().len());
        for acc in changed_accounts_iter(state) {
            changed_accounts.push(acc);
        }

        // remove any transactions that were mined
        //
        // NOTE: this worker's txs should already be removed during the block building process
        let mined_transactions = blocks.transaction_hashes().collect();

        // TODO: basefee should come from engine's watch channel
        // and engine should update basefee then make round canonical
        //
        // the watch channel MUST be updated before the canon notification
        let pending_block_base_fee = self.basefee.unwrap_or(MIN_PROTOCOL_BASE_FEE);

        // Canonical update
        let update = CanonicalStateUpdate {
            new_tip: &tip.block,          // finalized block
            pending_block_base_fee,       // current base fee for worker (network-wide)
            pending_block_blob_fee: None, // current base fee for worker (network-wide)
            changed_accounts,             // entire round of consensus
            mined_transactions,           // entire round of consensus
        };

        // sync fn so self will block until all pool updates are complete
        self.pool.on_canonical_state_change(update);
    }
}

impl<Provider, Pool, C> Future for MaintainTxPool<Provider, Pool, C>
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
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let PoolMaintenanceConfig { max_reload_accounts } = self.config;

        let this = self.get_mut();

        // update loop that drains all canon updates and worker updates
        // then updates the pool
        loop {
            trace!(target: "txpool", state=?this.maintenance_state, "awaiting new worker block or canonical udpate");

            this.metrics.set_dirty_accounts_len(this.dirty_addresses.len());
            let pool_info = this.pool.block_info();
            // check for canon updates first
            //
            // this is critical to ensure worker's block is building off canonical tip
            while let Poll::Ready(Some(canon_update)) =
                this.canonical_state_updates.poll_next_unpin(cx)
            {
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

            // check the reloaded accounts future
            if let Some(mut receiver) = this.reload_accounts_task.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(reloaded) => {
                        match reloaded {
                            Ok(Ok(LoadedAccounts { accounts, failed_to_load })) => {
                                // reloaded accounts successfully
                                // extend accounts we failed to load from database
                                this.dirty_addresses.extend(failed_to_load);
                                // update the pool with the loaded accounts
                                this.pool.update_accounts(accounts);
                            }
                            Ok(Err(res)) => {
                                // Failed to load accounts from state
                                let (accs, err) = *res;
                                debug!(target: "worker::pool_maintenance", %err, "failed to load accounts");
                                this.dirty_addresses.extend(accs);
                            }
                            Err(_) => {
                                // failed to receive the accounts, sender dropped, only possible if task panicked
                                this.maintenance_state = MaintainedPoolState::Drifted;
                            }
                        }
                    }
                    Poll::Pending => {
                        this.reload_accounts_task = Some(receiver);

                        // nothing to do - already polled canon update
                        break;
                    }
                }
            }

            // properly update pool after canon update in case pool has drifted
            if this.maintenance_state.is_drifted() {
                this.metrics.inc_drift();
                // assuming all senders are dirty
                this.dirty_addresses = this.pool.unique_senders();
                // make sure we toggle the state back to in sync
                this.maintenance_state = MaintainedPoolState::InSync;
            }

            // reload accounts that are out-of-sync in chunks
            if !this.dirty_addresses.is_empty() && this.reload_accounts_task.is_none() {
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
                this.reload_accounts_task = Some(rx);
                tokio::task::spawn_blocking(|| fut);

                // loop once to poll future
                continue;
            }

            // nothing to do
            break;
        }

        Poll::Pending
    }
}

/// Returns a spawnable future for maintaining the state of the transaction pool.
pub fn maintain_transaction_pool_future<Provider, P, C, Tasks>(
    provider: Provider,
    pool: P,
    canon_events: C,
    task_spawner: Tasks,
    config: PoolMaintenanceConfig,
) -> BoxFuture<'static, ()>
where
    Provider: StateProviderFactory
        + BlockReaderIdExt
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + Clone
        + Send
        + Unpin
        + 'static,
    P: TransactionPoolExt + Unpin + 'static,
    C: Stream<Item = CanonStateNotification> + Send + Unpin + 'static,
    Tasks: TaskSpawner + 'static,
{
    async move { MaintainTxPool::new(config, provider, pool, canon_events, None).await }.boxed()
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
