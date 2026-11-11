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
//!   discarding transactions mined in the lost rounds (issue #1236). A retry interval re-arms the
//!   residual reload between notifications, so a large dirty set drains at the retry cadence even
//!   when notification traffic goes quiet (issue #1304).
//! - The pool's pending base fee always comes from the shared per-worker [`BaseFeeContainer`] (the
//!   gas accumulator's fee for the current epoch). Canonical tip headers never set it: at an epoch
//!   boundary the tip is the previous epoch's closing block, whose header carries the old epoch's
//!   fee (issue #1262).
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
//! - With `sealed_tx_keep` on, the batch builder keeps a quorum-acked batch's transactions in the
//!   pool and marks them in [`SealedTxs`] instead of removing them (issue #1807). The canonical
//!   update clears the marks of each executed block's batch digest (`ommers_hash`) and of every
//!   mined hash, re-pooling clears a transaction's mark, and lost canonical notifications clear
//!   them all. With the switch off nothing is ever marked. A pool built for a node with the switch
//!   on ([`WorkerTxPool::new_with_sealed_tx_keep`]) takes twice the configured pending sub-pool
//!   limits, because its in-flight transactions stay pending.

use crate::metrics::{
    record_retired_pool_txns_dropped, record_retired_pool_txns_moved, HandoffDropReason,
};
use alloy::primitives::{keccak256, map::AddressSet, B256};
use futures::{future::OptionFuture, stream, Stream, StreamExt as _};
use reth_chainspec::ChainSpec;
use reth_node_core::{cli::config::RethTransactionPoolConfig, node_config::NodeConfig};
use reth_primitives_traits::SignerRecoverable;
use reth_provider::{
    providers::BlockchainProvider, AccountReader as _, CanonStateNotification,
    CanonStateSubscriptions as _, Chain, ChangedAccount, StateProviderBox,
    StateProviderFactory as _, TransactionsProvider as _,
};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    blobstore::DiskFileBlobStore,
    error::{
        Eip4844PoolTransactionError, Eip7702PoolTransactionError, InvalidPoolTransactionError,
        PoolError, PoolTransactionError,
    },
    AddedTransactionOutcome, BestTransactions, BlockInfo as RethBlockInfo, CanonicalStateUpdate,
    EthPooledTransaction, EthTransactionPool, PoolSize, PoolTransaction, PoolUpdateKind,
    SubPoolLimit, TransactionEvents, TransactionOrigin, TransactionPool as _,
    TransactionPoolExt as _, TransactionValidationTaskExecutor, ValidPoolTransaction,
};
use std::{
    collections::{BTreeMap, HashMap},
    pin::pin,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tn_types::{
    gas_accumulator::BaseFeeContainer, min_batch_size, Address, BlockBody, BlockHash, BlsPublicKey,
    Decodable2718 as _, EnvKzgSettings, Recovered, SealedBlock, SealedHeader, TaskError,
    TaskSpawner, TransactionSigned, TxHash, U256,
};
use tokio::{task::JoinError, time::MissedTickBehavior};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, IntervalStream};
use tracing::{debug, info, trace, warn};

use crate::{
    error::TnRethResult,
    evm::TnEvmConfig,
    forward::{FORWARD_BATCH_BUDGET, FORWARD_PENDING_LIFETIME, REQUEUE_GRACE},
    forward_pending::{PendingForwards, RetentionLimits, SubmissionHead},
    metrics::{ForwarderMetrics, RETH_METRICS},
    peer_batch::PeerBatchTxs,
    sealed_txs::{SealTicket, SealedTxs},
    traits::TelcoinNode,
    PoolTxn, PoolTxnId,
};

pub use reth_primitives_traits::InMemorySize as TxnSize;

/// Upper bound on canonical account reads per maintenance-loop iteration while recovering
/// from canonical-state broadcast lag (reth's `max_reload_accounts` analogue).
///
/// Lag means the loop is already behind, so recovery must not stall it further: each
/// iteration reloads at most this many dirty senders and carries the rest to the next
/// reload event, which is the next notification or the [`RELOAD_RETRY_INTERVAL`] tick,
/// whichever comes first.
const MAX_RELOAD_ACCOUNTS: usize = 100;

/// How many times the configured pending sub-pool limits a worker pool takes with the switch
/// `sealed_tx_keep` on (see [`pending_limit`]).
///
/// At reth's defaults the pending sub-pool then holds up to 20,000 transactions and 40 MiB of
/// them (reth measures a transaction's in-memory size) instead of 10,000 and 20 MiB.
const SEALED_TX_KEEP_PENDING_FACTOR: usize = 2;

/// A transaction-pool setting incompatible with TN's batch or fee policy.
#[derive(Debug, PartialEq, Eq)]
enum TxPoolConfigError {
    /// A transaction admitted at this byte limit could never fit in a batch.
    InputLimitExceedsBatch {
        /// The operator's per-transaction byte limit.
        configured: usize,
        /// The batch protocol's byte limit.
        maximum: usize,
    },
    /// TN has no priority fee market and does not support a pool priority fee floor.
    MinimumPriorityFee,
}

impl std::fmt::Display for TxPoolConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InputLimitExceedsBatch { configured, maximum } => write!(
                f,
                "--txpool.max-tx-input-bytes {configured} exceeds TN's batch byte limit {maximum}"
            ),
            Self::MinimumPriorityFee => write!(
                f,
                "--txpool.minimum-priority-fee is unsupported on TN: omit this flag to accept zero-tip transactions"
            ),
        }
    }
}

impl std::error::Error for TxPoolConfigError {}

/// Interval at which the maintenance loop re-arms the residual dirty-sender reload between
/// canonical-state notifications.
///
/// After a `Lagged` event the broadcast ring still holds up to its capacity in buffered
/// `Commit`s, so the first reload chunks drain back-to-back. The residual beyond that used
/// to advance only when the next notification arrived: consensus-round cadence at best, and
/// a post-spike lull (exactly what follows the volume spike that builds a large dirty set)
/// stalls it entirely. Re-arming on this interval drains the residual at one chunk per
/// interval instead, about 30 s at the theoretical ~30,000-dirty-sender maximum, independent
/// of notification traffic; the interval is a deliberate throttle on recovery DB pressure.
/// This adapts reth's `maintain_transaction_pool`, which re-arms its `reload_accounts_fut`
/// on every loop iteration; here the re-arm is paced by an explicit interval (issue #1304).
const RELOAD_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// One input to the pool maintenance loop (see [`WorkerTxPool::maintain_pool`]).
enum MaintenanceEvent {
    /// A canonical-state broadcast item: a notification, or `Lagged` when the subscriber
    /// fell behind.
    Update(Result<CanonStateNotification, BroadcastStreamRecvError>),
    /// The reload retry interval fired: reload one residual dirty-sender chunk, if any.
    RetryTick,
    /// The canonical-state stream closed; the loop ends and the critical task reports it.
    Closed,
}

impl MaintenanceEvent {
    /// True when the canonical-state stream has closed and the maintenance loop must end.
    fn is_closed(&self) -> bool {
        match self {
            MaintenanceEvent::Update(_) => false,
            MaintenanceEvent::RetryTick => false,
            MaintenanceEvent::Closed => true,
        }
    }
}

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
    /// Remove EIP-4844 blob transactions from the pool and delete the sidecars from blob store.
    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>);
    /// Remove transactions unsupported by the batch protocol, along with their descendants.
    /// This includes non-allowlisted EIP-2718 types and transactions exceeding a whole batch's
    /// gas or encoded-byte limit.
    fn remove_unsupported_txs(&mut self, txs: Vec<TxHash>);
    /// Return the canonical balances of `addresses` as of the latest committed block.
    ///
    /// Used to build the optimistic per-sender balances in a post-mining pool update. The
    /// accessor is batched so implementations can acquire ONE state provider for the whole set:
    /// a per-address `BlockchainProvider::basic_account` call builds a fresh
    /// `ConsistentProvider` -- an MDBX read transaction plus a `MemoryOverlayStateProvider` over
    /// the in-memory canonical blocks -- and a batch bounded only by the 30M-gas/1MB limits can
    /// hold ~1,400 distinct senders, i.e. ~1,400 repetitions of that setup per build.
    ///
    /// The result holds an entry for every requested address. A missing account (or a read
    /// error) yields [`U256::ZERO`], which is the conservative choice: it can only keep a
    /// sender's remaining transactions parked, never promote an unfunded one, and the engine's
    /// authoritative canonical update corrects it within the same consensus round. A failure to
    /// acquire the state provider itself degrades the WHOLE set to that conservative zero, so
    /// implementations log it before degrading.
    fn get_account_balances(&self, addresses: &[Address]) -> HashMap<Address, U256>;
    /// Remember `hashes` as packed by a peer batch this node has just validated.
    ///
    /// The builder skips a remembered hash for
    /// [`PEER_BATCH_DEFER_TTL`](crate::PEER_BATCH_DEFER_TTL) so this node does not pack a copy
    /// of a transaction a peer is already proposing.
    fn record_peer_batch(&self, hashes: &[TxHash]);
    /// Return true if `hash` is still deferred by a validated peer batch.
    fn is_peer_deferred(&self, hash: &TxHash) -> bool;
    /// Return true if `hash` is sealed into one of this node's batches that has not executed yet
    /// (switch `sealed_tx_keep`, see [`SealedTxs`]).
    ///
    /// The builder skips such a transaction without calling any [`BestTxns`] method. A plain skip
    /// leaves the sender's later nonces in the iterator, so they can go into the next batch while
    /// the earlier nonces execute from the sealed one. The default is false, for pools that never
    /// mark.
    fn is_sealed(&self, _hash: &TxHash) -> bool {
        false
    }
    /// Return true if the pool still holds `hash`.
    ///
    /// With `sealed_tx_keep` on, the builder asks this about a candidate that
    /// [`Self::is_sealed`] has just reported unmarked: selection walks a snapshot of the pending
    /// set, while the canonical update that unmarks an executed batch first removes its
    /// transactions from the pool, so a candidate marked when the snapshot was taken and
    /// unmarked now is no longer held. The default is true, for pools that never mark.
    fn holds(&self, _hash: &TxHash) -> bool {
        true
    }
}

/// A telcoin network transaction pool.
///
/// The second field is a handle to the blockchain provider, retained so the pool can read
/// senders' canonical balances when constructing optimistic pool updates after mining a batch
/// (see [`TxPool::get_account_balances`]).
#[derive(Clone, Debug)]
pub struct WorkerTxPool(
    EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore, TnEvmConfig>,
    BlockchainProvider<TelcoinNode>,
    /// The shared per-worker base-fee container: the single source of the pool's pending base
    /// fee (issue #1262).
    BaseFeeContainer,
    /// The transactions this node has seen inside a validated peer batch, deferred by the
    /// builder while that peer batch is in flight (issue #1329).
    PeerBatchTxs,
    /// Observer transactions awaiting canonical inclusion, shared across epoch forwarders.
    Arc<Mutex<PendingForwards<TxHash, BlsPublicKey, B256>>>,
    /// The transactions this node sealed into a quorum-acked batch that has not executed yet,
    /// kept in the pool and skipped by the builder (switch `sealed_tx_keep`, issue #1807).
    SealedTxs,
);

impl From<WorkerTxPool>
    for EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore, TnEvmConfig>
{
    fn from(value: WorkerTxPool) -> Self {
        value.0
    }
}

impl WorkerTxPool {
    /// Set this epoch's fee in both the pool and its canonical-update fee handle.
    ///
    /// A worker that is removed and later reactivated gets a new accumulator slot. Its
    /// persistent pool still holds the old container, so update that container as well as the
    /// pool's pending fee.
    ///
    /// Store the container before reading and writing the pool's `BlockInfo`.
    /// [`Self::update_canonical_state`] samples the container outside reth's pool lock, then
    /// applies the sampled fee under that lock. Publishing the container first ensures that
    /// canonical updates sampling it afterwards use the current epoch's fee.
    ///
    /// This sequence is not atomic: reth exposes no API to hold the pool lock across
    /// `block_info()` and `set_block_info()`. A canonical update that already sampled the
    /// previous fee can still overwrite `pending_basefee` after this method returns. The next
    /// canonical commit that samples the updated container restores the fee. Conversely, a
    /// canonical update between our `block_info()` read and `set_block_info()` write can have
    /// its other `BlockInfo` fields overwritten by our older snapshot.
    ///
    /// Reth's `TxPool::set_block_info` calls `update_basefee` to reclassify transactions already
    /// in the pool: a fee increase demotes transactions that no longer meet it, while a decrease
    /// can promote transactions from the base-fee subpool. No separate re-sort is needed.
    pub fn set_epoch_base_fee(&self, base_fee: u64) {
        self.2.set_base_fee(base_fee);
        let mut block_info = self.block_info();
        block_info.pending_basefee = base_fee;
        self.set_block_info(block_info);
    }

    /// Create a pool and spawn canonical-state maintenance and queued-transaction expiry.
    ///
    /// The pool takes the node's configured limits, as with the switch `sealed_tx_keep` off (see
    /// [`Self::new_with_sealed_tx_keep`]).
    pub fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
    ) -> eyre::Result<Self> {
        Self::new_with_sealed_tx_keep(
            node_config,
            task_spawner,
            blockchain_provider,
            evm_config,
            base_fee,
            false,
        )
    }

    /// Create a pool for a node whose switch `sealed_tx_keep` is `sealed_tx_keep`, and spawn
    /// canonical-state maintenance and queued-transaction expiry.
    ///
    /// With the switch on, the pending sub-pool's limits are raised (see [`pending_limit`]);
    /// with it off, every limit is the node's configured one, as in [`Self::new`].
    pub fn new_with_sealed_tx_keep(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
        sealed_tx_keep: bool,
    ) -> eyre::Result<Self> {
        let this = Self::build_pool(
            node_config,
            task_spawner,
            blockchain_provider,
            evm_config,
            base_fee,
            sealed_tx_keep,
        )?;
        this.spawn_maintenance_task(task_spawner, blockchain_provider);
        this.spawn_expiry_task(task_spawner);
        Ok(this)
    }

    /// Construct the pool without spawning the canonical-state maintenance task.
    ///
    /// Kept separate from [`WorkerTxPool::new`] so tests can reproduce a pool that missed
    /// canonical updates (the drifted state the maintenance task's lag handling recovers
    /// from) without racing a live subscription (see issue #1236).
    #[cfg(test)]
    pub(crate) fn build(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
    ) -> eyre::Result<Self> {
        Self::build_pool(
            node_config,
            task_spawner,
            blockchain_provider,
            evm_config,
            base_fee,
            false,
        )
    }

    /// Construct the pool, with the pending limits [`pending_limit`] gives for `sealed_tx_keep`,
    /// without spawning any task.
    fn build_pool(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
        sealed_tx_keep: bool,
    ) -> eyre::Result<Self> {
        // The pool and validator survive epoch changes, so admission must fit the smallest
        // batch limit across all supported epochs. Non-blob reth validation measures the full
        // EIP-2718 encoding, just like the batch protocol.
        let maximum = min_batch_size();
        (node_config.txpool.max_tx_input_bytes <= maximum).then_some(()).ok_or(
            TxPoolConfigError::InputLimitExceedsBatch {
                configured: node_config.txpool.max_tx_input_bytes,
                maximum,
            },
        )?;
        // A configured floor would conflict with TN's zero-tip fee policy (#1340).
        node_config
            .txpool
            .minimum_priority_fee
            .is_none()
            .then_some(())
            .ok_or(TxPoolConfigError::MinimumPriorityFee)?;
        let data_dir = node_config.datadir();
        let mut pool_config = node_config.txpool.pool_config();
        // the forwarder retains unsealed transactions only: it keeps the configured limits
        let forward_limits = RetentionLimits::new(
            pool_config.pending_limit.max_txs,
            pool_config.pending_limit.max_size,
        );
        pool_config.pending_limit = pending_limit(pool_config.pending_limit, sealed_tx_keep);
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
        // These limits live on reth's validator, so Pool::eth_pool cannot apply them.
        .with_max_tx_input_bytes(node_config.txpool.max_tx_input_bytes)
        .with_max_tx_gas_limit(node_config.txpool.max_tx_gas_limit)
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

        Ok(Self(
            transaction_pool,
            blockchain_provider.clone(),
            base_fee,
            PeerBatchTxs::default(),
            Arc::new(Mutex::new(PendingForwards::new(forward_limits))),
            SealedTxs::new(),
        ))
    }

    /// Spawn the critical task that expires parked transactions even when the chain is idle.
    ///
    /// Match reth's queued-lifetime sweep and local-origin exemptions. A zero lifetime
    /// means expire on the next sweep; clamp only the timer period to avoid a zero-period
    /// panic or a busy loop. Pending transactions never expire through this task.
    /// A panic or unexpected exit must shut down the node, like canonical-state maintenance,
    /// rather than silently leave the pool running without its configured age limit.
    fn spawn_expiry_task(&self, task_spawner: &TaskSpawner) {
        let pool = self.clone();
        let period = self.0.config().max_queued_lifetime.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        task_spawner.spawn_critical_task("queued txn pool expiry", async move {
            IntervalStream::new(interval)
                .for_each(move |_| {
                    pool.evict_stale_transactions(Instant::now());
                    futures::future::ready(())
                })
                .await;
            Err(TaskError::from_message(
                "queued txn pool expiry task ended: interval stream closed",
            ))
        });
    }

    /// Remove expired queued and basefee transactions using the pool's configured lifetime.
    ///
    /// Reth timestamps admission with `std::time::Instant`; accepting `now` explicitly
    /// keeps boundary tests deterministic. Local and private origins retain reth's
    /// exemption unless `--txpool.nolocals` is set. Blob transactions cannot enter this pool.
    fn evict_stale_transactions(&self, now: Instant) {
        let config = self.0.config();
        let stale = self
            .0
            .queued_transactions()
            .into_iter()
            .filter(|tx| {
                (tx.origin.is_external() || config.local_transactions_config.no_exemptions)
                    && now.saturating_duration_since(tx.timestamp) >= config.max_queued_lifetime
            })
            .map(|tx| *tx.hash())
            .collect();
        let removed = self.0.remove_transactions(stale);
        RETH_METRICS.record_txpool_expired_transactions(removed.len());
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
    ///
    /// The loop body lives in [`Self::maintain_pool`] so tests can drive it with a
    /// synthetic stream. Tests run on real time: the `RethEnv` harness holds a
    /// `spawn_blocking` task for its whole life, which inhibits tokio's paused-clock
    /// auto-advance (see the test module comment).
    fn spawn_maintenance_task(
        &self,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
    ) {
        let state_stream = BroadcastStream::new(blockchain_provider.subscribe_to_canonical_state());
        let txn_pool_clone = self.clone();
        // Update the txn pool as the canonical tip changes.
        task_spawner.spawn_critical_task("canonical txn pool", async move {
            txn_pool_clone
                .maintain_pool(state_stream, RELOAD_RETRY_INTERVAL, MAX_RELOAD_ACCOUNTS)
                .await
        });
    }

    /// Drive pool maintenance until `state_stream` closes.
    ///
    /// Applies every canonical-state notification, marks all pool senders dirty on
    /// `Lagged`, and reloads dirty senders in chunks of `max_reload`. A `retry_interval`
    /// tick re-arms the reload between notifications, so a residual dirty set drains at
    /// the retry cadence even when notification traffic goes quiet after the volume spike that
    /// built it (issue #1304). Each event also expires retained forwards and requeues eligible
    /// ones; acknowledged transactions require canonical output progress before becoming
    /// eligible.
    ///
    /// Canonical pool updates finish before the next maintenance event is processed.
    /// The chunk reload is awaited inline, so a tick can never start a second reload while
    /// one is in flight; ticks that would fire during a reload are pushed back a full
    /// `retry_interval` ([`MissedTickBehavior::Delay`]).
    async fn maintain_pool(
        self,
        state_stream: impl Stream<Item = Result<CanonStateNotification, BroadcastStreamRecvError>>
            + Send,
        retry_interval: Duration,
        max_reload: usize,
    ) -> Result<(), TaskError> {
        let mut interval = tokio::time::interval(retry_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let ticks = stream::unfold(interval, |mut interval| async move {
            interval.tick().await;
            Some((MaintenanceEvent::RetryTick, interval))
        });
        // `select` polls `ticks` forever, so the merged stream alone would never end. The
        // chained `Closed` sentinel plus `take_while` end it when `state_stream` closes,
        // preserving the shutdown semantics of the plain notification loop.
        let updates = state_stream
            .map(MaintenanceEvent::Update)
            .chain(stream::once(std::future::ready(MaintenanceEvent::Closed)));
        let mut events = pin!(stream::select(updates, ticks)
            .take_while(|event| std::future::ready(!event.is_closed())));
        let mut dirty_addresses = AddressSet::default();
        while let Some(event) = events.next().await {
            let newly_dirty = match event {
                MaintenanceEvent::Update(update) => match update {
                    Ok(notification) => {
                        self.apply_canon_notification(notification).await?;
                        AddressSet::default()
                    }
                    Err(BroadcastStreamRecvError::Lagged(missed)) => self.mark_drifted(missed),
                },
                MaintenanceEvent::RetryTick => AddressSet::default(),
                // `take_while` ends the stream at `Closed`, so this arm never runs; a
                // plain value keeps the match total (no panic in a critical task).
                MaintenanceEvent::Closed => AddressSet::default(),
            };
            let to_reload: AddressSet = dirty_addresses.into_iter().chain(newly_dirty).collect();
            dirty_addresses = if to_reload.is_empty() {
                to_reload
            } else {
                // The reload is synchronous MDBX I/O. Run it on the blocking pool so it
                // never occupies one of the async worker threads this runtime also uses
                // for consensus and networking (reth offloads the same work:
                // `maintain_transaction_pool` runs under `spawn_blocking_task`).
                let pool = self.clone();
                let retained = to_reload.clone();
                tokio::task::spawn_blocking(move || {
                    pool.reload_dirty_accounts(to_reload, max_reload)
                })
                .await
                .unwrap_or_else(|error| {
                    // the blocking task was dropped or panicked: keep every sender
                    // dirty so the next reload event retries the reload
                    warn!(
                        target: "txpool",
                        ?error,
                        "dirty-account reload task failed; retrying on the next reload event"
                    );
                    retained
                })
            };
            self.retry_forwarded(Instant::now(), max_reload).await;
            self.sweep_sealed_marks(Instant::now()).await;
        }
        Err(TaskError::from_message("canonical txn pool task ended because state_stream closed"))
    }

    /// Access the pool-owned forwarding state without holding the lock across provider I/O.
    pub(crate) fn pending_forwards(
        &self,
    ) -> MutexGuard<'_, PendingForwards<TxHash, BlsPublicKey, B256>> {
        self.4.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reserve retained bytes before a successful forwarding admission lets the builder prune them.
    /// Reading the actual canonical head keeps queued historical notifications out of the window.
    pub(crate) fn admit_forwards(&self, transactions: Vec<Vec<u8>>) -> Option<Vec<Vec<u8>>> {
        let head = self.1.canonical_in_memory_state().get_canonical_head();
        self.pending_forwards().admit(
            transactions.into_iter().map(|tx| (keccak256(&tx), tx)).collect(),
            SubmissionHead::new(head.number, head.parent_beacon_block_root),
            Instant::now(),
            FORWARD_BATCH_BUDGET,
            FORWARD_PENDING_LIFETIME,
            REQUEUE_GRACE,
        )
    }

    /// Return eligible forwards to the ordinary pool after checking canonical inclusion locally.
    /// The existing maintenance batch limit also bounds provider reads and validations per event.
    async fn retry_forwarded(&self, now: Instant, limit: usize) {
        let (ready, expired) = self.pending_forwards().ready(now, limit);
        if expired > 0 {
            ::metrics::counter!("tn_reth.forwarded_txns_expired_total")
                .increment(u64::try_from(expired).unwrap_or(u64::MAX));
            warn!(target: "worker::forward", expired, "forward inclusion tracking expired");
        }
        stream::iter(ready)
            .for_each(|(hash, bytes)| async move {
                let provider = self.1.clone();
                let included = tokio::task::spawn_blocking(move || provider.transaction_by_hash(hash))
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|result| {
                        result.map(|transaction| transaction.is_some()).map_err(|error| error.to_string())
                    });
                if included.as_ref().is_ok_and(|included| *included) {
                    self.pending_forwards().remove(&hash);
                } else if included.is_err() {
                    self.pending_forwards().retry_later(&hash);
                    warn!(target: "worker::forward", ?included, "cannot check forwarded transaction inclusion");
                } else {
                    let added = OptionFuture::from(
                        recover_raw_transaction(&bytes)
                            .ok()
                            .map(|transaction| self.add_recovered_transaction_external(transaction)),
                    )
                    .await
                    .is_some_and(|result| result.is_ok());
                    if added {
                        ForwarderMetrics::record_txns_requeued(1);
                    }
                    // A new batch may already own the inserted transaction by this point.
                    let present = added || self.0.get(&hash).is_some();
                    self.pending_forwards().reinserted(&hash, present);
                }
            })
            .await;
    }

    /// Apply one canonical-state notification, awaiting pool maintenance before the next one.
    async fn apply_canon_notification(
        &self,
        notification: CanonStateNotification,
    ) -> Result<(), JoinError> {
        match notification {
            CanonStateNotification::Commit { new } => self.process_canon_state_update(new).await,
            // TN never reorgs: consensus output only extends the canonical chain, so a
            // Reorg notification here is a bug upstream. Skip it rather than panic . . .
            // this runs inside a critical task, and aborting it would take down the
            // whole node over a pool-maintenance miss.
            CanonStateNotification::Reorg { .. } => {
                warn!(
                    target: "txpool",
                    "unexpected canonical state notification (TN never reorgs); skipping \
                     transaction pool update"
                );
                Ok(())
            }
        }
    }

    /// Record a canonical-state broadcast lag event and return the sender set to resync.
    ///
    /// The skipped `Commit`s are gone from the broadcast channel, so their mined
    /// transactions and changed accounts can never be replayed: treat every sender with
    /// transactions in the pool as dirty, exactly like reth's
    /// `MaintainedPoolState::Drifted`.
    ///
    /// The lost `Commit`s also carried the batch digests that executed, so the sealed marks they
    /// would have cleared are unknown: drop every mark. A mark that outlived its batch would keep
    /// the transaction, and every later nonce of its sender, out of all future batches; dropping
    /// a live mark costs at most a duplicate copy that execution skips (see [`SealedTxs`]).
    fn mark_drifted(&self, missed: u64) -> AddressSet {
        warn!(
            target: "txpool",
            missed,
            "canonical state notifications lost to broadcast lag; resyncing pool sender \
             accounts from canonical state"
        );
        RETH_METRICS.canon_state_lagged_total.increment(1);
        RETH_METRICS.canon_state_notifications_missed_total.increment(missed);
        let marks = self.5.clear_all_lag();
        if marks > 0 {
            warn!(
                target: "txpool",
                marks,
                "sealed transaction marks dropped after canonical state broadcast lag"
            );
        }
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
        if chunk.is_empty() {
            pending.collect()
        } else {
            // Acquire ONE state provider for the whole chunk, like reth's `load_accounts`.
            // `BlockchainProvider::basic_account` builds a fresh `ConsistentProvider` on
            // every call - an MDBX read transaction plus a `MemoryOverlayStateProvider`
            // over the in-memory canonical blocks - so a per-address read would repeat
            // that setup `max_reload` times per iteration.
            let loaded: Vec<Result<ChangedAccount, Address>> = self
                .1
                .latest()
                .map(|state| {
                    chunk
                        .iter()
                        .map(|address| Self::load_changed_account(&state, *address))
                        .collect()
                })
                .unwrap_or_else(|error| {
                    debug!(
                        target: "txpool",
                        ?error,
                        "failed to acquire canonical state for pool resync"
                    );
                    // nothing was reloaded: every chunk address stays dirty and is
                    // retried on a later iteration
                    chunk.iter().map(|address| Err(*address)).collect()
                });
            let accounts: Vec<ChangedAccount> =
                loaded.iter().filter_map(|result| result.as_ref().ok().copied()).collect();
            if !accounts.is_empty() {
                self.0.update_accounts(accounts);
            }
            let failures: Vec<Address> =
                loaded.iter().filter_map(|result| result.as_ref().err().copied()).collect();
            if !failures.is_empty() {
                RETH_METRICS
                    .canon_state_resync_read_failures_total
                    .increment(u64::try_from(failures.len()).unwrap_or(u64::MAX));
                // One aggregated debug line per iteration, matching reth's
                // `maintain_transaction_pool`. Persistent provider failures retry every
                // `RELOAD_RETRY_INTERVAL`; the sustained rate of
                // `canon_state_resync_read_failures_total` carries the alert.
                debug!(
                    target: "txpool",
                    failed = failures.len(),
                    "canonical account reads failed during pool resync; senders stay \
                     dirty for retry"
                );
            }
            pending.chain(failures).collect()
        }
    }

    /// Read `address`'s canonical account from `state` and shape it as a [`ChangedAccount`]
    /// for the pool.
    ///
    /// A missing account maps to [`ChangedAccount::empty`] (nonce 0, zero balance), matching
    /// reth's `load_accounts`; a provider read error returns the address so the caller keeps
    /// it dirty and retries.
    fn load_changed_account(
        state: &StateProviderBox,
        address: Address,
    ) -> Result<ChangedAccount, Address> {
        state
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

    /// Apply a canonical state update to the pool: remove mined transactions and refresh
    /// changed accounts.
    ///
    /// The pending base fee always comes from the worker's shared [`BaseFeeContainer`], the fee
    /// for the current epoch. At an epoch boundary the canonical tip is the previous epoch's
    /// closing block and its header carries the old epoch's fee, so tip headers must never set
    /// the pool's fee (issue #1262).
    ///
    /// Pool maintenance runs on the blocking executor because it can hold pool locks and
    /// process many transactions. Awaiting it preserves the caller's update ordering and
    /// reports a blocking-task failure instead of continuing with an incomplete pool update.
    /// Only the tip header is needed; retaining it avoids copying a block's transactions on
    /// the calling async thread.
    pub async fn update_canonical_state(
        &self,
        new_tip: &SealedHeader,
        pending_block_blob_fee: Option<u128>,
        mined_transactions: Vec<TxHash>,
        changed_accounts: Vec<ChangedAccount>,
    ) -> Result<(), JoinError> {
        let pool = self.clone();
        let new_tip = new_tip.clone();
        tokio::task::spawn_blocking(move || {
            // Reth's pool and validator only read header fields from the tip. This synthetic
            // block satisfies that API without copying a body and stays inside this closure.
            let new_tip = SealedBlock::from_sealed_parts(new_tip, BlockBody::default());
            pool.apply_canonical_update(
                &new_tip,
                pending_block_blob_fee,
                mined_transactions,
                changed_accounts,
            );
        })
        .await
    }

    /// Apply a canonical update synchronously on the blocking executor.
    ///
    /// Both maintenance paths read the current epoch's base fee here, when the update is applied.
    fn apply_canonical_update(
        &self,
        new_tip: &SealedBlock,
        pending_block_blob_fee: Option<u128>,
        mined_transactions: Vec<TxHash>,
        changed_accounts: Vec<ChangedAccount>,
    ) {
        let update = CanonicalStateUpdate {
            new_tip,
            pending_block_base_fee: self.2.base_fee(),
            pending_block_blob_fee,
            changed_accounts,
            mined_transactions,
            update_kind: PoolUpdateKind::Commit,
        };
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

    /// Subscribe to admissions in every subpool, including transactions that cannot propagate.
    ///
    /// Retired workers subscribe before draining so in-flight admissions also reach an active
    /// worker, and the batch builder wakes on them. Include private and parked transactions
    /// regardless of their propagation policy.
    pub fn admitted_transactions(&self) -> impl Stream<Item = ()> + Send + 'static {
        tokio_stream::wrappers::ReceiverStream::new(
            self.0
                .new_transactions_listener_for(reth_transaction_pool::TransactionListenerKind::All),
        )
        .map(|_| ())
    }

    /// Move pending and parked transactions to an active worker under its normal admission rules.
    ///
    /// Preserve each transaction's origin. A transaction the destination already holds, including
    /// an `AlreadyImported` result, is complete and is not logged. Validation or capacity refusals
    /// are discarded, counted by [`HandoffDropReason`] and summarized in one `warn!` per pass;
    /// the detail for each refusal is at `debug`. Remove from this pool only after the destination
    /// has handled the transaction, so cancelling a handoff cannot lose it. Callers must stop this
    /// worker's producers and keep relaying late admissions until regrowth.
    pub(crate) async fn migrate_transactions_to(&self, destination: &Self) -> HandoffSummary {
        let summary = stream::iter(self.0.all_transactions())
            .fold(HandoffSummary::default(), |summary, transaction| async move {
                let hash = *transaction.hash();
                let outcome = if destination.get(&hash).is_some() {
                    HandoffOutcome::AlreadyPresent
                } else {
                    destination
                        .0
                        .add_transaction(transaction.origin, transaction.transaction.clone())
                        .await
                        .map_or_else(
                            |error| HandoffOutcome::from_error(&error),
                            |_| HandoffOutcome::Moved,
                        )
                };
                self.0.remove_transaction(hash);
                summary.record(outcome)
            })
            .await;
        summary.report();
        summary
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Collect account changes and mined hashes on the blocking executor, borrowing the tip
    /// from the shared chain. Await completion before receiving another notification.
    ///
    /// After the pool update, clear the sealed marks the update settles: every block's
    /// `ommers_hash` is the digest of the batch it executed, so its batch's marks are cleared
    /// (what the block skipped becomes buildable again), and every mined hash loses its mark.
    /// Clearing after the pool update means the pool never holds a mined transaction unmarked.
    /// The builder walks a snapshot of the pending set, which can still list such a
    /// transaction, so with `sealed_tx_keep` it reads the mark first and then whether the pool
    /// still holds the transaction ([`TxPool::holds`]).
    async fn process_canon_state_update(&self, update: Arc<Chain>) -> Result<(), JoinError> {
        let pool = self.clone();
        tokio::task::spawn_blocking(move || {
            trace!(target: "worker::block-builder", ?update, "canon state update from engine");

            let (blocks, state) = update.inner();
            let tip = blocks.tip();

            // Collect all accounts that changed in the last round of consensus.
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

            // Collect hashes to remove transactions mined in this canonical update.
            let mined_transactions: Vec<TxHash> = blocks.transaction_hashes().collect();

            tip.parent_beacon_block_root.into_iter().for_each(|output| {
                pool.pending_forwards().committed(
                    tip.number,
                    output,
                    mined_transactions.iter().copied(),
                );
            });

            debug!(target: "block-builder", ?mined_transactions);

            pool.apply_canonical_update(
                tip.sealed_block(),
                Some(u128::MAX), // set max fee for blobs
                mined_transactions,
                changed_accounts,
            );

            pool.5.clear_executed(
                blocks.iter().map(|(_, block)| block.ommers_hash),
                blocks.transaction_hashes(),
            );
        })
        .await
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
    /// recovery.
    ///
    /// The node calls this to re-pool: the epoch orphan path (`repool_batch_txns` in
    /// `close_epoch.rs`) and the forwarder's requeue of undelivered transactions. So the
    /// transaction's sealed mark is cleared first, whatever the add returns: a marked transaction
    /// is still pooled, answers `AlreadyImported`, and becomes buildable again. A client resend
    /// reaches the pool through reth's RPC handle, never through this method, so it cannot clear
    /// a mark. A gossip path that called this for client transactions would clear marks too.
    pub async fn add_recovered_transaction_external(
        &self,
        recovered: Recovered<TransactionSigned>,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        let hash = *recovered.hash();
        self.5.clear_repooled(&hash);
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

    /// The shared window of transactions seen inside a validated peer batch.
    ///
    /// The batch validator records into this window and the batch builder reads it, so a
    /// transaction a peer is already proposing is not packed again here (issue #1329).
    pub fn peer_batch_txs(&self) -> &PeerBatchTxs {
        &self.3
    }

    /// The marks for transactions this node sealed into a batch that has not executed yet
    /// (switch `sealed_tx_keep`, issue #1807).
    pub fn sealed_txs(&self) -> &SealedTxs {
        &self.5
    }

    /// Take a ticket for a batch whose transaction selection has just finished.
    ///
    /// Take it after the build returns and before the batch goes to the worker, and pass it to
    /// [`Self::mark_sealed`] when the quorum ack arrives (see [`SealTicket`]).
    pub fn seal_ticket(&self) -> SealTicket {
        self.5.ticket()
    }

    /// Mark the transactions of the quorum-acked batch `digest` as sealed, keeping them in the
    /// pool, and return how many were newly marked (switch `sealed_tx_keep`).
    ///
    /// This replaces the post-seal [`Self::update_canonical_state`] call: the transactions stay
    /// pooled, so they are found by hash, a resend of their bytes answers `AlreadyImported`, and
    /// the pending nonce counts them. Only `hashes` the pool still holds are marked. Nothing is
    /// marked if `digest` executed, or canonical notifications were lost, after `ticket` was
    /// taken, because no later canonical update would clear those marks. The marks clear when a
    /// block whose `ommers_hash` is `digest` executes, when a hash is mined, when a transaction
    /// is re-pooled, or when it leaves the pool.
    ///
    /// Only for batches sealed by quorum (`Worker::seals_via_quorum`). A non-committee worker
    /// forwards its batch to the committee, which re-packs the transactions under its own
    /// digests, so this node's digest never executes and a mark would only clear by mined hash
    /// or requeue; a forward whose tracking expires would leave its transactions marked for good.
    ///
    /// The pool lookup can wait on reth's pool lock behind a canonical update, so it runs on the
    /// blocking executor, like [`Self::update_canonical_state`].
    pub async fn mark_sealed(
        &self,
        ticket: SealTicket,
        digest: BlockHash,
        hashes: Vec<TxHash>,
    ) -> Result<usize, JoinError> {
        let pool = self.clone();
        tokio::task::spawn_blocking(move || {
            pool.5.mark_present(ticket, digest, &hashes, |candidates| pool.held(candidates))
        })
        .await
    }

    /// Drop the marks of transactions the pool no longer holds (evicted, expired, or removed by
    /// a nonce reload), and return how many were dropped.
    pub(crate) fn drop_evicted_marks(&self) -> usize {
        self.5.drop_missing(|marked| self.held(marked))
    }

    /// Run [`Self::drop_evicted_marks`] on the blocking executor if anything is marked and the
    /// last sweep was at least
    /// [`SEALED_TXS_SWEEP_INTERVAL`](crate::sealed_txs::SEALED_TXS_SWEEP_INTERVAL) before `now`.
    ///
    /// The maintenance loop calls this after every event, retry ticks included, so the marks of
    /// evicted transactions are dropped even while canonical updates stall.
    async fn sweep_sealed_marks(&self, now: Instant) {
        if self.5.sweep_due(now) {
            let pool = self.clone();
            // the sweep reads every marked hash under reth's pool lock
            if let Err(error) = tokio::task::spawn_blocking(move || pool.drop_evicted_marks()).await
            {
                warn!(
                    target: "txpool",
                    ?error,
                    "sealed transaction mark sweep failed; retrying after the next interval"
                );
            }
        }
    }

    /// Return the subset of `hashes` the pool holds, under one pool read lock.
    fn held(&self, hashes: Vec<TxHash>) -> Vec<TxHash> {
        self.0.get_all(hashes).into_iter().map(|transaction| *transaction.hash()).collect()
    }
}

impl TxPool for WorkerTxPool {
    fn best_transactions(&self) -> BestTxns {
        BestTxns { inner: self.0.best_transactions() }
    }

    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>) {
        self.0.remove_transactions_and_descendants(blobs.clone());
        self.0.delete_blobs(blobs);
    }

    fn remove_unsupported_txs(&mut self, txs: Vec<TxHash>) {
        self.0.remove_transactions_and_descendants(txs);
    }

    fn get_account_balances(&self, addresses: &[Address]) -> HashMap<Address, U256> {
        // An empty set needs no state: acquiring the provider opens an MDBX read
        // transaction (and, mid-round, collects the in-memory canonical blocks into a
        // memory overlay). `build_batch` reaches this call with no senders whenever the
        // pending set drained between the build gate and `best_transactions()`.
        if addresses.is_empty() {
            return HashMap::new();
        }
        // one state provider (one MDBX read transaction + memory overlay) for the whole set;
        // a failure to acquire it reports the documented conservative zero for every address,
        // so it is logged loudly rather than degrading silently
        let provider = self
            .1
            .latest()
            .inspect_err(|error| {
                warn!(
                    target: "txpool",
                    ?error,
                    num_addresses = addresses.len(),
                    "failed to acquire state provider; reporting zero balance for all senders"
                );
            })
            .ok();
        addresses
            .iter()
            .map(|address| {
                let balance = provider
                    .as_ref()
                    .and_then(|state| state.basic_account(address).ok().flatten())
                    .map(|account| account.balance)
                    .unwrap_or(U256::ZERO);
                (*address, balance)
            })
            .collect()
    }

    fn record_peer_batch(&self, hashes: &[TxHash]) {
        self.3.record(hashes)
    }

    fn is_peer_deferred(&self, hash: &TxHash) -> bool {
        self.3.is_deferred(hash)
    }

    fn is_sealed(&self, hash: &TxHash) -> bool {
        self.5.is_marked(hash)
    }

    fn holds(&self, hash: &TxHash) -> bool {
        self.0.contains(hash)
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

    /// Skip a transaction a validated peer batch already carries.
    ///
    /// Marking it invalid for this build also skips the sender's later nonces: a nonce-gapped
    /// copy would only be skipped at execution, after paying for batch space and a vote round.
    /// The transaction stays in the pool and is packed normally once the deferral expires (or
    /// leaves the pool with the peer batch's execution), so this is not a rejection.
    pub fn peer_deferred(&mut self, pool_tx: &Arc<PoolTxn>) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::Other(Box::new(PeerBatchDeferred)),
        );
    }
}

/// The pool error reported when the builder skips a transaction already packed by a validated
/// peer batch (issue #1329).
///
/// This is a local scheduling decision, not a judgement about the transaction: `is_bad_transaction`
/// is false, so no peer is penalized and the transaction stays poolable.
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerBatchDeferred;

impl std::fmt::Display for PeerBatchDeferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transaction deferred: already packed by a validated peer batch")
    }
}

impl std::error::Error for PeerBatchDeferred {}

impl PoolTransactionError for PeerBatchDeferred {
    fn is_bad_transaction(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
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

/// Decode bytes into a signed transaction without signer recovery.
///
/// The decode rules and errors match [`recover_signed_transaction`]. Empty input fails, and
/// trailing bytes fail. This function does not check the signature. Call
/// [`verify_transaction_signature`] before you trust the transaction.
pub fn decode_signed_transaction(tx: &[u8]) -> TnRethResult<TransactionSigned> {
    if tx.is_empty() {
        Err(crate::EthApiError::EmptyRawTransactionData.into())
    } else {
        TransactionSigned::decode_2718_exact(tx)
            .map_err(|_| crate::EthApiError::FailedToDecodeSignedTransaction.into())
    }
}

/// Verify the signature of a signed transaction by signer recovery.
///
/// The recovery applies the EIP-2 rule for the `s` value. This function discards the recovered
/// signer. A bad signature returns the same error as [`recover_signed_transaction`].
pub fn verify_transaction_signature(tx: &TransactionSigned) -> TnRethResult<()> {
    tx.recover_signer()
        .map(|_| ())
        .map_err(|_| crate::EthApiError::InvalidTransactionSignature.into())
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
mod config_tests;

/// What happened to one retired-worker transaction in a handoff pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HandoffOutcome {
    /// The active worker accepted the transaction.
    Moved,
    /// The active worker already held the transaction.
    AlreadyPresent,
    /// The active worker refused the transaction, and the handoff discarded it.
    Refused(HandoffDropReason),
}

impl HandoffOutcome {
    /// Classify a failed insertion, and log the detail of a real refusal at `debug`.
    ///
    /// `AlreadyImported` means the destination holds the transaction, so the handoff is complete.
    fn from_error(error: &PoolError) -> Self {
        HandoffDropReason::from_kind(&error.kind).map_or(Self::AlreadyPresent, |reason| {
            debug!(target: "tn::execution", hash = ?error.hash, ?error,
                "active worker refused a retired worker transaction; discarding");
            Self::Refused(reason)
        })
    }
}

/// The result of one pass of [`WorkerTxPool::migrate_transactions_to`].
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct HandoffSummary {
    /// Transactions the active worker accepted.
    moved: u64,
    /// Transactions the active worker already held.
    already_present: u64,
    /// Transactions the active worker refused, by reason.
    refused: BTreeMap<HandoffDropReason, u64>,
}

impl HandoffSummary {
    /// Add one transaction's outcome to the pass totals.
    fn record(self, outcome: HandoffOutcome) -> Self {
        match outcome {
            HandoffOutcome::Moved => Self { moved: self.moved.saturating_add(1), ..self },
            HandoffOutcome::AlreadyPresent => {
                Self { already_present: self.already_present.saturating_add(1), ..self }
            }
            HandoffOutcome::Refused(reason) => {
                let count = self.refused.get(&reason).map_or(1, |count| count.saturating_add(1));
                Self {
                    refused: self.refused.into_iter().chain([(reason, count)]).collect(),
                    ..self
                }
            }
        }
    }

    /// Record the pass on the handoff counters, and log one `warn!` when the pass refused any
    /// transaction.
    fn report(&self) {
        record_retired_pool_txns_moved(self.moved);
        self.refused
            .iter()
            .for_each(|(reason, count)| record_retired_pool_txns_dropped(*reason, *count));
        if !self.refused.is_empty() {
            warn!(target: "tn::execution", moved = self.moved,
                already_present = self.already_present, refused = ?self.refused,
                "active worker refused retired worker transactions; discarding");
        }
    }
}

/// The pending sub-pool's limits for a worker pool: the `configured` ones with the switch
/// `sealed_tx_keep` off, and [`SEALED_TX_KEEP_PENDING_FACTOR`] times them, count and bytes,
/// with it on.
///
/// With the switch on, a quorum-acked batch's transactions stay in the pending sub-pool until the
/// batch executes, so this node's in-flight transactions and its unsealed ones share one cap.
/// With it off, the in-flight ones are out of the pool and the cap bounds only unsealed ones.
/// Above the cap reth evicts the worst senders' highest nonces with all their descendants, and
/// only a transaction evicted while it is being inserted learns of it: an earlier accepted one
/// just disappears. Twice the configured limits keep the configured share for unsealed
/// transactions and give the in-flight ones as much again.
fn pending_limit(configured: SubPoolLimit, sealed_tx_keep: bool) -> SubPoolLimit {
    if sealed_tx_keep {
        SubPoolLimit::new(
            configured.max_txs.saturating_mul(SEALED_TX_KEEP_PENDING_FACTOR),
            configured.max_size.saturating_mul(SEALED_TX_KEEP_PENDING_FACTOR),
        )
    } else {
        configured
    }
}

#[cfg(test)]
mod canonical_update_tests;

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
        test_genesis, Address, Bytes, Encodable2718 as _, GenesisAccount, TaskManager,
        MIN_PROTOCOL_BASE_FEE, U256,
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
        let pool = reth_env.init_txn_pool(BaseFeeContainer::default()).unwrap();
        (chain, reth_env, pool)
    }

    /// A real canonical notification retires retained forwarding state independently of pool
    /// pruning.
    #[tokio::test]
    async fn forwarded_inclusion_retires_retained_state() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, reth_env, _background_pool) =
            funded_pool_for_test(&factory, &tmp_dir, &task_manager);
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default())?;
        let tx = factory.create_eip1559(
            chain.clone(),
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let encoded = tx.encoded_2718();
        let hash = *tx.hash();
        assert_eq!(pool.admit_forwards(vec![encoded.clone()]), Some(vec![encoded.clone()]));
        let mut notifications = pool.1.subscribe_to_canonical_state();
        let output = consensus_output_for_tests(1, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![encoded])?;
        assert!(
            pool.1.transaction_by_hash(hash)?.is_some(),
            "the transaction must really be included"
        );
        let notification =
            tokio::time::timeout(Duration::from_secs(5), notifications.recv()).await??;
        pool.apply_canon_notification(notification).await?;
        assert_eq!(
            pool.pending_forwards().ready(Instant::now() + FORWARD_PENDING_LIFETIME, 1),
            (vec![], 0),
            "canonical inclusion must remove the retained record before expiry",
        );
        Ok(())
    }

    /// An eligible retained payload returns to the real pool and can be admitted for its next send.
    #[tokio::test]
    async fn missing_inclusion_requeues_the_retained_payload() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, reth_env, _background_pool) =
            funded_pool_for_test(&factory, &tmp_dir, &task_manager);
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default())?;
        let tx = factory.create_eip1559(
            chain,
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let encoded = tx.encoded_2718();
        let hash = *tx.hash();
        assert_eq!(pool.admit_forwards(vec![encoded.clone()]), Some(vec![encoded.clone()]));
        pool.pending_forwards().defer(&hash);
        (1_u8..=3).for_each(|output| {
            pool.pending_forwards().committed(u64::from(output), B256::repeat_byte(output), []);
        });
        assert!(pool.get(&hash).is_none());
        pool.retry_forwarded(Instant::now() + REQUEUE_GRACE, 1).await;
        assert!(pool.get(&hash).is_some(), "the original signed payload must return to the pool");
        assert_eq!(pool.admit_forwards(vec![encoded.clone()]), Some(vec![encoded]));
        Ok(())
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
        let pool = reth_env.init_txn_pool(BaseFeeContainer::default()).unwrap();

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

    /// A retired worker hands off pending, base-fee-parked and nonce-queued transactions once.
    #[tokio::test]
    async fn retired_pool_migrates_every_subpool_and_deduplicates() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let tasks = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, env, source) = funded_pool_for_test(&factory, &tmp_dir, &tasks);
        source.set_epoch_base_fee(100);
        let destination = env.init_txn_pool(BaseFeeContainer::default())?;
        destination.set_epoch_base_fee(150);
        let pending = recover_raw_transaction(&factory.create_eip1559_encoded(
            chain.clone(),
            None,
            200,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        ))?;
        let pending_hash = *pending.hash();
        source
            .add_transaction_local(
                EthPooledTransaction::try_from_consensus(pending)
                    .map_err(|_| eyre::eyre!("expected an EIP1559 pool transaction"))?,
            )
            .await?;
        let parked = recover_raw_transaction(&factory.create_eip1559_encoded(
            chain.clone(),
            None,
            50,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        ))?;
        let parked_hash = *parked.hash();
        source.add_recovered_transaction_external(parked).await?;
        factory.set_nonce(4);
        let queued = recover_raw_transaction(&factory.create_eip1559_encoded(
            chain,
            None,
            200,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        ))?;
        let queued_hash = *queued.hash();
        source.add_recovered_transaction_external(queued.clone()).await?;
        destination.add_recovered_transaction_external(queued).await?;
        assert_eq!(source.pool_size().pending, 1);
        assert_eq!(source.pool_size().basefee, 1);
        assert_eq!(source.pool_size().queued, 1);

        assert_eq!(
            source.migrate_transactions_to(&destination).await,
            HandoffSummary { moved: 2, already_present: 1, refused: BTreeMap::new() }
        );
        assert_eq!(source.migrate_transactions_to(&destination).await, HandoffSummary::default());
        assert_eq!(source.0.all_transactions().count(), 0);
        assert_eq!(destination.0.all_transactions().count(), 3);
        assert!(destination.get(&pending_hash).is_some());
        assert!(destination.get(&parked_hash).is_some());
        assert!(destination.get(&queued_hash).is_some());
        assert_eq!(
            destination.get(&pending_hash).map(|transaction| transaction.origin),
            Some(TransactionOrigin::Local)
        );
        assert_eq!(destination.pool_size().pending, 1);
        assert_eq!(destination.pool_size().basefee, 1);
        assert_eq!(destination.pool_size().queued, 1);
        assert_eq!(destination.block_info().pending_basefee, 150);
        Ok(())
    }

    /// Destination refusal discards the retired copy without replacing an active transaction.
    #[tokio::test]
    async fn retired_pool_discards_refused_transactions() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let tasks = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, env, source) = funded_pool_for_test(&factory, &tmp_dir, &tasks);
        let destination = env.init_txn_pool(BaseFeeContainer::default())?;
        let retired = recover_raw_transaction(&factory.create_eip1559_encoded(
            chain.clone(),
            None,
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        ))?;
        let retired_hash = *retired.hash();
        source.add_recovered_transaction_external(retired).await?;
        factory.set_nonce(0);
        let active = recover_raw_transaction(&factory.create_eip1559_encoded(
            chain,
            None,
            14,
            Some(Address::ZERO),
            U256::from(200),
            Bytes::new(),
        ))?;
        let active_hash = *active.hash();
        destination.add_recovered_transaction_external(active).await?;

        assert_eq!(
            source.migrate_transactions_to(&destination).await,
            HandoffSummary {
                moved: 0,
                already_present: 0,
                refused: BTreeMap::from([(HandoffDropReason::ReplacementUnderpriced, 1)]),
            }
        );
        assert_eq!(source.0.all_transactions().count(), 0);
        assert!(destination.get(&retired_hash).is_none());
        assert!(destination.get(&active_hash).is_some());
        assert_eq!(destination.0.all_transactions().count(), 1);
        Ok(())
    }

    /// Handoff observes non-propagating admissions and preserves their private origin.
    #[tokio::test]
    async fn retired_pool_observes_non_propagating_admissions() -> eyre::Result<()> {
        use futures::StreamExt as _;
        let tmp_dir = TempDir::new()?;
        let tasks = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, env, source) = funded_pool_for_test(&factory, &tmp_dir, &tasks);
        let destination = env.init_txn_pool(BaseFeeContainer::default())?;
        let mut admissions = pin!(source.admitted_transactions());
        let recovered = recover_raw_transaction(&factory.create_eip1559_encoded(
            chain,
            None,
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        ))?;
        let hash = *recovered.hash();
        source
            .0
            .add_transaction(
                TransactionOrigin::Private,
                EthPooledTransaction::try_from_consensus(recovered)
                    .map_err(|_| eyre::eyre!("expected an EIP1559 pool transaction"))?,
            )
            .await?;
        assert!(source.get(&hash).is_some_and(|transaction| !transaction.propagate));
        tokio::time::timeout(Duration::from_secs(5), admissions.next())
            .await?
            .ok_or_else(|| eyre::eyre!("all-admissions listener closed"))?;
        assert_eq!(
            source.migrate_transactions_to(&destination).await,
            HandoffSummary { moved: 1, ..HandoffSummary::default() }
        );
        assert!(source.get(&hash).is_none());
        assert_eq!(
            destination.get(&hash).map(|transaction| transaction.origin),
            Some(TransactionOrigin::Private)
        );
        Ok(())
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
        let pool = reth_env.init_txn_pool(BaseFeeContainer::default()).unwrap();

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
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default()).unwrap();

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
        let state = pool.1.latest().unwrap();
        let account = WorkerTxPool::load_changed_account(&state, tx_factory.address()).unwrap();
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
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default()).unwrap();

        let dirty: AddressSet = (1u8..=3).map(Address::repeat_byte).collect();

        let after_one = pool.reload_dirty_accounts(dirty, 1);
        assert_eq!(after_one.len(), 2);
        let after_two = pool.reload_dirty_accounts(after_one, 1);
        assert_eq!(after_two.len(), 1);
        let after_three = pool.reload_dirty_accounts(after_two, 1);
        assert!(after_three.is_empty());
    }

    /// Issue #1304: residual dirty senders beyond the per-event reload bound must drain on
    /// the retry interval alone. The synthetic stream delivers one `Lagged` marker and then
    /// goes silent (the post-spike lull), so any progress past the first chunk can come
    /// only from the re-armed reload, never from a notification.
    //
    // The three issue #1304 tests run on real time. With `start_paused`, the `RethEnv`
    // harness keeps `spawn_blocking` tasks alive for its whole life
    // (`TaskSpawner::spawn_reth_task` wraps `Handle::block_on`), and tokio inhibits
    // paused-clock auto-advance while a blocking task is in flight, so every timer
    // waits forever.
    #[tokio::test]
    async fn test_residual_dirty_senders_drain_between_notifications() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let mut factories: Vec<TransactionFactory> =
            (0..3).map(|_| TransactionFactory::new_random()).collect();
        let genesis =
            test_genesis().extend_accounts(factories.iter().map(|factory| {
                (factory.address(), GenesisAccount::default().with_balance(U256::MAX))
            }));
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default()).unwrap();

        let encoded_txs: Vec<Vec<u8>> = factories
            .iter_mut()
            .map(|factory| {
                factory
                    .create_eip1559(
                        chain.clone(),
                        Some(21_000),
                        7,
                        Some(Address::ZERO),
                        U256::from(100),
                        Bytes::new(),
                    )
                    .encoded_2718()
            })
            .collect();
        let outcomes = futures::future::join_all(encoded_txs.iter().map(|encoded| {
            let recovered = recover_raw_transaction(encoded).unwrap();
            pool.add_recovered_transaction_external(recovered)
        }))
        .await;
        outcomes.into_iter().for_each(|outcome| {
            outcome.unwrap();
        });
        assert_eq!(pool.pool_size().pending, 3);

        // commit a canonical block that mines all three transactions; with no maintenance
        // task subscribed, the pool never sees the notification . . . the lag scenario
        let output = consensus_output_for_tests(1, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, encoded_txs).unwrap();
        let state = pool.1.latest().unwrap();
        factories.iter().for_each(|factory| {
            let account = WorkerTxPool::load_changed_account(&state, factory.address()).unwrap();
            assert_eq!(account.nonce, 1, "test block must mine every transaction");
        });
        // negative control: the pool is drifted, the mined transactions are still pending
        assert_eq!(pool.pool_size().pending, 3);

        // one Lagged marker, then a silent stream: the loop reloads one sender on the
        // marker (chunk size 1) and must drain the residual two on retry ticks alone
        let updates: Vec<Result<CanonStateNotification, BroadcastStreamRecvError>> =
            vec![Err(BroadcastStreamRecvError::Lagged(1))];
        let state_stream = stream::iter(updates).chain(stream::pending());
        let maintenance =
            tokio::spawn(pool.clone().maintain_pool(state_stream, Duration::from_millis(100), 1));

        let mut poll = pin!(stream::iter(0..600u32)
            .then(|_| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                pool.pool_size().pending
            })
            .skip_while(|pending| std::future::ready(*pending != 0)));
        let drained = tokio::time::timeout(Duration::from_secs(60), poll.next())
            .await
            .expect("drain poll must finish within its wall-clock deadline");
        assert_eq!(drained, Some(0), "residual dirty senders must drain on retry ticks alone");
        assert!(!maintenance.is_finished(), "a silent stream must keep the maintenance loop alive");
        maintenance.abort();
        // join the aborted task so test teardown cannot race the maintenance loop
        let _ = maintenance.await;
    }

    /// The retry ticks alone must not keep the maintenance loop alive: when the
    /// canonical-state stream closes, the loop ends and the critical task reports the
    /// closure, exactly as the plain notification loop did before issue #1304.
    #[tokio::test]
    async fn test_maintenance_loop_ends_when_state_stream_closes() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default()).unwrap();

        let updates: Vec<Result<CanonStateNotification, BroadcastStreamRecvError>> = Vec::new();
        let ended = tokio::time::timeout(
            Duration::from_secs(5),
            pool.maintain_pool(stream::iter(updates), Duration::from_millis(100), 1),
        )
        .await
        .expect("maintenance loop must end when the canonical-state stream closes");
        assert!(
            format!("{ended:?}").contains("state_stream closed"),
            "unexpected loop exit: {ended:?}"
        );
    }

    /// A live `Commit` notification still drives the pool through the merged-stream loop:
    /// a maintenance loop subscribed to the real canonical-state broadcast removes the
    /// mined transaction, proving the notification arm survived the issue #1304
    /// restructuring.
    #[tokio::test]
    async fn test_maintenance_loop_applies_canonical_notifications() {
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
        let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default()).unwrap();

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

        // subscribe before the commit so the notification reaches the loop
        let state_stream = BroadcastStream::new(pool.1.subscribe_to_canonical_state());
        let maintenance = tokio::spawn(pool.clone().maintain_pool(
            state_stream,
            Duration::from_millis(100),
            MAX_RELOAD_ACCOUNTS,
        ));

        let output = consensus_output_for_tests(1, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![encoded]).unwrap();

        let mut poll = pin!(stream::iter(0..600u32)
            .then(|_| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                pool.pool_size().pending
            })
            .skip_while(|pending| std::future::ready(*pending != 0)));
        let applied = tokio::time::timeout(Duration::from_secs(60), poll.next())
            .await
            .expect("apply poll must finish within its wall-clock deadline");
        assert_eq!(applied, Some(0), "the maintenance loop must apply the Commit notification");
        assert!(pool.get(&hash).is_none());
        maintenance.abort();
        // join the aborted task so test teardown cannot race the maintenance loop
        let _ = maintenance.await;
    }

    #[tokio::test]
    async fn test_validator_applies_tx_fee_cap_to_local_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        // 1,000 wei cap: a 21,000-gas transfer at 7 wei/gas costs at most 147,000 wei.
        let rpc_args =
            reth_node_core::args::RpcServerArgs { rpc_tx_fee_cap: 1_000, ..Default::default() };
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain.clone(),
            tmp_dir.path(),
            &task_manager,
            None,
            rpc_args,
        )
        .unwrap();
        let pool = reth_env.init_txn_pool(BaseFeeContainer::default()).unwrap();

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

    /// Regression test for issue #1262: a canonical tip whose header carries another epoch's
    /// base fee must not overwrite the pool's pending base fee. The pending fee always comes
    /// from the worker's shared [`BaseFeeContainer`].
    #[tokio::test]
    async fn test_canonical_update_cannot_clobber_epoch_base_fee() -> Result<(), JoinError> {
        const EPOCH_FEE: u64 = MIN_PROTOCOL_BASE_FEE + 1234;
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let pool = reth_env.init_txn_pool(BaseFeeContainer::new(EPOCH_FEE)).unwrap();

        // The genesis header carries the chain's default fee. It must differ from EPOCH_FEE,
        // or the assertion below could not distinguish the container from the tip header.
        let genesis_header = reth_env.chainspec().sealed_genesis_header();
        assert_ne!(genesis_header.base_fee_per_gas, Some(EPOCH_FEE));

        pool.update_canonical_state(&genesis_header, Some(u128::MAX), vec![], vec![]).await?;

        assert_eq!(
            pool.block_info().pending_basefee,
            EPOCH_FEE,
            "a canonical tip from the previous epoch must not clobber the epoch base fee",
        );
        Ok(())
    }

    /// A saturated blocking executor keeps canonical maintenance pending while the async
    /// executor makes progress, and completion guarantees that the pool update was applied.
    #[tokio::test]
    async fn test_canonical_update_awaits_blocking_pool_work() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
        let pool = reth_env
            .init_txn_pool_without_maintenance(BaseFeeContainer::new(MIN_PROTOCOL_BASE_FEE))?;
        let genesis_header = reth_env.chainspec().sealed_genesis_header();
        pool.set_block_info(RethBlockInfo { pending_basefee: 0, ..pool.block_info() });

        // Keep validator tasks on the outer runtime so they cannot occupy the sole blocking
        // slot used by this test's independent runtime.
        tokio::task::spawn_blocking(move || -> eyre::Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()?;
            runtime.block_on(async {
                let (started, ready) = tokio::sync::oneshot::channel();
                let (release, blocked) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = started.send(());
                    blocked.recv()
                });
                ready.await?;

                let update =
                    pool.update_canonical_state(&genesis_header, Some(u128::MAX), vec![], vec![]);
                tokio::pin!(update);
                let first_poll = futures::poll!(&mut update);
                let fee_before_release = pool.block_info().pending_basefee;

                // Release before assertions so a regression cannot strand the blocking task
                // and prevent the test runtime from shutting down.
                release.send(())?;
                blocker.await??;
                assert!(first_poll.is_pending(), "canonical work must yield to the executor");
                assert_eq!(fee_before_release, 0, "queued pool work must not run inline");

                update.await?;
                assert_eq!(pool.block_info().pending_basefee, MIN_PROTOCOL_BASE_FEE);
                Ok(())
            })
        })
        .await??;
        Ok(())
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

    // ---- bench6 sealed-keep ----

    use reth_transaction_pool::error::PoolErrorKind;

    /// A pool WITHOUT its maintenance task over a chain that funds the given senders, so a test
    /// applies every canonical notification itself, in order.
    struct SealedBench {
        chain: Arc<RethChainSpec>,
        reth_env: RethEnv,
        pool: WorkerTxPool,
        notifications: tokio::sync::broadcast::Receiver<CanonStateNotification>,
        tip: SealedHeader,
        outputs: u64,
        // dropped last: the manager shuts down the env's tasks, the directory holds its database
        _task_manager: TaskManager,
        _tmp_dir: TempDir,
    }

    impl SealedBench {
        fn new(senders: &[&TransactionFactory]) -> eyre::Result<Self> {
            let tmp_dir = TempDir::new()?;
            let task_manager = TaskManager::default();
            let genesis = test_genesis().extend_accounts(senders.iter().map(|sender| {
                (sender.address(), GenesisAccount::default().with_balance(U256::MAX))
            }));
            let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
            let reth_env =
                RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
            let pool = reth_env.init_txn_pool_without_maintenance(BaseFeeContainer::default())?;
            let notifications = pool.1.subscribe_to_canonical_state();
            let tip = chain.sealed_genesis_header();
            Ok(Self {
                chain,
                reth_env,
                pool,
                notifications,
                tip,
                outputs: 0,
                _task_manager: task_manager,
                _tmp_dir: tmp_dir,
            })
        }

        /// Sign `count` transfers from `sender` at its next nonces.
        fn transfers(
            &self,
            sender: &mut TransactionFactory,
            count: usize,
        ) -> Vec<TransactionSigned> {
            (0..count)
                .map(|_| {
                    sender.create_eip1559(
                        self.chain.clone(),
                        Some(21_000),
                        7,
                        Some(Address::ZERO),
                        U256::from(1),
                        Bytes::new(),
                    )
                })
                .collect()
        }

        /// Submit `txs` as an RPC client's raw transactions and return their hashes.
        async fn submit(&self, txs: &[TransactionSigned]) -> eyre::Result<Vec<TxHash>> {
            for tx in txs {
                self.pool.add_raw_transaction_external(tx.clone()).await?;
            }
            Ok(hashes(txs))
        }

        /// Mark `txs` as sealed under `digest` with a fresh ticket, as the builder does on ack.
        async fn seal(&self, digest: BlockHash, txs: &[TransactionSigned]) -> eyre::Result<usize> {
            Ok(self.pool.mark_sealed(self.pool.seal_ticket(), digest, hashes(txs)).await?)
        }

        /// Execute `txs` as the block of batch `digest` (its `ommers_hash`) and apply the
        /// canonical notification to the pool.
        async fn execute(
            &mut self,
            digest: BlockHash,
            txs: &[TransactionSigned],
        ) -> eyre::Result<()> {
            self.outputs += 1;
            let output = consensus_output_for_tests(1, 0, self.outputs, false);
            let payload = TNPayload::new_for_test_with(
                self.tip.clone(),
                &output,
                Address::random(),
                digest,
                B256::random(),
            );
            let encoded = txs.iter().map(|tx| tx.encoded_2718()).collect();
            let block =
                execute_payload_and_update_canonical_chain(&self.reth_env, payload, encoded)?;
            self.tip = block.recovered_block.clone_sealed_header();
            let notification =
                tokio::time::timeout(Duration::from_secs(5), self.notifications.recv()).await??;
            self.pool.apply_canon_notification(notification).await?;
            Ok(())
        }

        /// The sender's nonce as `eth_getTransactionCount(.., "pending")` computes it: one past
        /// the highest pooled nonce consecutive from the canonical nonce.
        fn pending_nonce(&self, sender: Address) -> eyre::Result<u64> {
            let state = self.pool.1.latest()?;
            let canonical = WorkerTxPool::load_changed_account(&state, sender)
                .map_err(|address| eyre::eyre!("cannot read account {address}"))?
                .nonce;
            Ok(self
                .pool
                .0
                .get_highest_consecutive_transaction_by_sender(sender, canonical)
                .map_or(canonical, |tx| tx.nonce() + 1))
        }

        /// What `build_batch` may select with sealed-keep on: the best transactions in order,
        /// marked ones skipped without `mark_invalid`.
        fn selectable(&self) -> Vec<TxHash> {
            self.pool
                .best_transactions()
                .filter(|tx| !self.pool.is_sealed(tx.hash()))
                .map(|tx| *tx.hash())
                .collect()
        }
    }

    /// The hashes of `txs`, in order.
    fn hashes(txs: &[TransactionSigned]) -> Vec<TxHash> {
        txs.iter().map(|tx| *tx.hash()).collect()
    }

    /// A sender with a fixed key, so each test's chain is reproducible.
    fn sender(seed: u64) -> TransactionFactory {
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed))
    }

    /// I-2 S2/S3: a sealed transaction stays visible by hash, a resend of its bytes answers
    /// `AlreadyImported`, the pending nonce counts it, and the builder offers none of it.
    #[tokio::test]
    async fn sealed_transactions_stay_visible_known_and_counted() -> eyre::Result<()> {
        let mut a = sender(61);
        let bench = SealedBench::new(&[&a])?;
        let txs = bench.transfers(&mut a, 4);
        let sealed = bench.submit(&txs).await?;
        assert_eq!(bench.seal(B256::repeat_byte(1), &txs).await?, 4);

        sealed.iter().for_each(|hash| {
            assert!(bench.pool.get(hash).is_some(), "a sealed transaction is found by hash");
            assert!(bench.pool.is_sealed(hash));
        });
        let resend = bench.pool.add_raw_transaction_external(txs[0].clone()).await;
        assert!(
            matches!(&resend, Err(error) if matches!(error.kind, PoolErrorKind::AlreadyImported)),
            "a resend of sealed bytes must answer already known, got {resend:?}"
        );
        assert_eq!(bench.pending_nonce(a.address())?, 4, "the pending nonce counts sealed nonces");
        assert!(bench.selectable().is_empty(), "the builder offers no sealed transaction");
        Ok(())
    }

    /// Pin of the switch-off path: the builder's post-seal update (mined hashes plus the
    /// optimistic nonce) removes the sealed transactions, so a lookup misses, the pending nonce
    /// falls back to the canonical one, and a resend is accepted as new (bench 5's 300-s repair).
    #[tokio::test]
    async fn switch_off_post_seal_update_forgets_sealed_transactions() -> eyre::Result<()> {
        let mut a = sender(62);
        let bench = SealedBench::new(&[&a])?;
        let txs = bench.transfers(&mut a, 4);
        let sealed = bench.submit(&txs).await?;
        let optimistic = ChangedAccount { address: a.address(), nonce: 4, balance: U256::MAX };
        bench
            .pool
            .update_canonical_state(&bench.tip, Some(u128::MAX), sealed.clone(), vec![optimistic])
            .await?;

        assert!(bench.pool.get(&sealed[0]).is_none());
        assert_eq!(bench.pending_nonce(a.address())?, 0);
        assert!(bench.pool.add_raw_transaction_external(txs[0].clone()).await.is_ok());
        assert!(bench.pool.sealed_txs().is_empty(), "the switch-off path never marks");
        Ok(())
    }

    /// The update for the block whose `ommers_hash` is the sealed batch's digest removes the
    /// batch and clears its marks. An update for another digest leaves them, and the sender's
    /// next nonces are pending and selectable, not queued behind a gap (I-2 S8).
    #[tokio::test]
    async fn only_its_own_batch_execution_clears_a_mark() -> eyre::Result<()> {
        let (mut a, mut b) = (sender(63), sender(64));
        let mut bench = SealedBench::new(&[&a, &b])?;
        let (sealed_digest, other_digest) = (B256::repeat_byte(1), B256::repeat_byte(2));
        let sealed = bench.transfers(&mut a, 4);
        bench.submit(&sealed).await?;
        bench.seal(sealed_digest, &sealed).await?;

        let other = bench.transfers(&mut b, 1);
        bench.submit(&other).await?;
        bench.execute(other_digest, &other).await?;
        assert_eq!(bench.pool.sealed_txs().len(), 4, "another digest's block clears nothing");
        assert!(hashes(&sealed).iter().all(|hash| bench.pool.get(hash).is_some()));

        let next = bench.transfers(&mut a, 4);
        bench.submit(&next).await?;
        assert_eq!(bench.pool.pool_size().queued, 0, "later nonces are not gapped");
        assert_eq!(
            bench.selectable(),
            hashes(&next),
            "descendants of sealed nonces are selectable"
        );

        bench.execute(sealed_digest, &sealed).await?;
        assert!(bench.pool.sealed_txs().is_empty());
        assert!(hashes(&sealed).iter().all(|hash| bench.pool.get(hash).is_none()));
        assert_eq!(bench.selectable(), hashes(&next));
        assert_eq!(bench.pending_nonce(a.address())?, 8);
        Ok(())
    }

    /// I-2 S1 at pool level: the batch with the later nonces executes first and skips them
    /// (nonce too high). Its update unmarks them, still pooled, while the earlier batch stays
    /// marked; once the earlier batch executes, the later nonces are the next selection.
    #[tokio::test]
    async fn out_of_order_execution_unmarks_the_skipped_batch() -> eyre::Result<()> {
        let mut a = sender(65);
        let mut bench = SealedBench::new(&[&a])?;
        let (first_digest, second_digest) = (B256::repeat_byte(1), B256::repeat_byte(2));
        let first = bench.transfers(&mut a, 4);
        let second = bench.transfers(&mut a, 4);
        bench.submit(&first).await?;
        bench.submit(&second).await?;
        bench.seal(first_digest, &first).await?;
        bench.seal(second_digest, &second).await?;
        assert!(bench.selectable().is_empty());

        bench.execute(second_digest, &second).await?;
        assert!(hashes(&second).iter().all(|hash| bench.pool.get(hash).is_some()));
        assert!(hashes(&second).iter().all(|hash| !bench.pool.is_sealed(hash)));
        assert!(hashes(&first).iter().all(|hash| bench.pool.is_sealed(hash)));
        assert_eq!(bench.selectable(), hashes(&second));
        assert_eq!(bench.pending_nonce(a.address())?, 8);

        bench.execute(first_digest, &first).await?;
        assert!(bench.pool.sealed_txs().is_empty());
        assert_eq!(bench.selectable(), hashes(&second));
        Ok(())
    }

    /// The epoch orphan path re-pools through `add_recovered_transaction_external`: it clears
    /// the mark even though the transaction is still pooled and the add answers already known.
    #[tokio::test]
    async fn repooling_clears_the_mark() -> eyre::Result<()> {
        let mut a = sender(66);
        let bench = SealedBench::new(&[&a])?;
        let txs = bench.transfers(&mut a, 2);
        bench.submit(&txs).await?;
        bench.seal(B256::repeat_byte(1), &txs).await?;

        let recovered = recover_raw_transaction(&txs[0].encoded_2718())?;
        let repooled = bench.pool.add_recovered_transaction_external(recovered).await;
        assert!(
            matches!(&repooled, Err(error) if matches!(error.kind, PoolErrorKind::AlreadyImported))
        );
        assert!(!bench.pool.is_sealed(txs[0].hash()));
        assert!(bench.pool.is_sealed(txs[1].hash()), "only the re-pooled transaction is unmarked");
        assert_eq!(bench.selectable(), vec![*txs[0].hash()]);
        Ok(())
    }

    /// A marked transaction that leaves the pool without executing (reth's eviction, simulated
    /// by a direct removal) loses its mark and moves the eviction counter. The maintenance loop
    /// runs the same sweep on its retry ticks, with no canonical update at all.
    #[tokio::test]
    async fn an_evicted_marked_transaction_drops_its_mark() -> eyre::Result<()> {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let mut a = sender(67);
        let bench = SealedBench::new(&[&a])?;
        let txs = bench.transfers(&mut a, 4);
        bench.submit(&txs).await?;
        bench.seal(B256::repeat_byte(1), &txs).await?;

        assert_eq!(bench.pool.0.remove_transactions(vec![*txs[3].hash()]).len(), 1);
        let recorder = DebuggingRecorder::new();
        let dropped = metrics::with_local_recorder(&recorder, || bench.pool.drop_evicted_marks());
        assert_eq!(dropped, 1);
        let evicted = recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, ..)| key.key().name() == "tn_reth.sealed_txs_dropped_evicted_total")
            .map(|(.., value)| value);
        assert_eq!(evicted, Some(DebugValue::Counter(1)));
        assert!(!bench.pool.is_sealed(txs[3].hash()));
        assert_eq!(bench.pool.sealed_txs().len(), 3);

        assert_eq!(bench.pool.0.remove_transactions(vec![*txs[2].hash()]).len(), 1);
        let maintenance = tokio::spawn(bench.pool.clone().maintain_pool(
            stream::pending::<Result<CanonStateNotification, BroadcastStreamRecvError>>(),
            Duration::from_millis(50),
            MAX_RELOAD_ACCOUNTS,
        ));
        let mut poll = pin!(stream::iter(0..200u32)
            .then(|_| async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                bench.pool.is_sealed(txs[2].hash())
            })
            .skip_while(|sealed| std::future::ready(*sealed)));
        let swept = tokio::time::timeout(Duration::from_secs(10), poll.next()).await?;
        maintenance.abort();
        // join the aborted task so test teardown cannot race the maintenance loop
        let _ = maintenance.await;
        assert_eq!(swept, Some(false), "a retry tick sweeps without any canonical update");
        assert_eq!(bench.pool.sealed_txs().len(), 2);
        Ok(())
    }

    /// Lost canonical notifications drop every mark: the executed digests are unknown, and a
    /// mark that outlived its batch would block its sender for good.
    #[tokio::test]
    async fn broadcast_lag_drops_every_mark() -> eyre::Result<()> {
        let mut a = sender(68);
        let bench = SealedBench::new(&[&a])?;
        let txs = bench.transfers(&mut a, 2);
        bench.submit(&txs).await?;
        bench.seal(B256::repeat_byte(1), &txs[..1]).await?;
        // a batch whose selection finished before the lag, acked after it
        let in_flight = bench.pool.seal_ticket();

        let dirty = bench.pool.mark_drifted(3);
        assert!(dirty.contains(&a.address()));
        assert!(bench.pool.sealed_txs().is_empty());
        assert_eq!(bench.selectable(), hashes(&txs));
        let late = bench.pool.mark_sealed(in_flight, B256::repeat_byte(2), hashes(&txs[1..]));
        assert_eq!(late.await?, 0, "it may have executed in the lost rounds");
        Ok(())
    }

    /// The late-ack race: a batch's block executes, skipping nonce-gapped transactions, before
    /// the builder handles its quorum ack. A ticket taken before the execution refuses the mark,
    /// so nothing is left marked with no execution to clear it; an identical batch selected
    /// after that execution is marked normally.
    #[tokio::test]
    async fn a_mark_after_its_batch_executed_is_refused() -> eyre::Result<()> {
        let mut a = sender(69);
        let mut bench = SealedBench::new(&[&a])?;
        let digest = B256::repeat_byte(1);
        let gap = bench.transfers(&mut a, 2);
        let gapped = bench.transfers(&mut a, 2);
        bench.submit(&gap).await?;
        bench.submit(&gapped).await?;

        let ticket = bench.pool.seal_ticket();
        bench.execute(digest, &gapped).await?;
        assert!(hashes(&gapped).iter().all(|hash| bench.pool.get(hash).is_some()));
        assert_eq!(bench.pool.mark_sealed(ticket, digest, hashes(&gapped)).await?, 0);
        assert!(bench.pool.sealed_txs().is_empty());

        let reseal = bench.pool.seal_ticket();
        assert_eq!(bench.pool.mark_sealed(reseal, digest, hashes(&gapped)).await?, 2);
        Ok(())
    }

    /// The limits a built pool enforces, read back from reth: with `sealed_tx_keep` off they are
    /// exactly the configured ones (reth's defaults, or an operator's flags); with it on the
    /// pending sub-pool's count and byte limits are twice those and every other limit is the
    /// same.
    #[tokio::test]
    async fn sealed_keep_doubles_only_the_pending_limits() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let env = RethEnv::new_for_temp_chain(
            Arc::new(test_genesis().into()),
            tmp_dir.path(),
            &task_manager,
            None,
        )?;
        let built = |config: &NodeConfig<ChainSpec>, keep: bool| {
            WorkerTxPool::new_with_sealed_tx_keep(
                config,
                env.get_task_spawner(),
                env.blockchain_provider(),
                env.evm_config(),
                BaseFeeContainer::default(),
                keep,
            )
            .map(|pool| pool.0.config().clone())
        };
        let mut operator = env.node_config().clone();
        operator.txpool.pending_max_count = 3_000;
        operator.txpool.pending_max_size = 7;

        for config in [env.node_config(), &operator] {
            let configured = config.txpool.pool_config();
            let off = built(config, false)?;
            let on = built(config, true)?;
            let default = WorkerTxPool::new(
                config,
                env.get_task_spawner(),
                env.blockchain_provider(),
                env.evm_config(),
                BaseFeeContainer::default(),
            )?;
            for pool in [&off, default.0.config()] {
                assert_eq!(pool.pending_limit, configured.pending_limit);
                assert_eq!(pool.basefee_limit, configured.basefee_limit);
                assert_eq!(pool.queued_limit, configured.queued_limit);
                assert_eq!(pool.blob_limit, configured.blob_limit);
                assert_eq!(pool.max_account_slots, configured.max_account_slots);
            }
            assert_eq!(
                on.pending_limit,
                SubPoolLimit::new(
                    configured.pending_limit.max_txs * 2,
                    configured.pending_limit.max_size * 2
                )
            );
            assert_eq!(on.basefee_limit, configured.basefee_limit);
            assert_eq!(on.queued_limit, configured.queued_limit);
            assert_eq!(on.blob_limit, configured.blob_limit);
            assert_eq!(on.max_account_slots, configured.max_account_slots);
        }
        // reth's defaults, which the devnet runs with: 10,000 transactions and 20 MiB
        let defaults = built(env.node_config(), true)?.pending_limit;
        assert_eq!(defaults, SubPoolLimit::new(20_000, 40 * 1024 * 1024));
        // a limit an operator set to "unbounded" stays unbounded instead of overflowing
        assert_eq!(pending_limit(SubPoolLimit::max(), true), SubPoolLimit::max());
        Ok(())
    }
}
