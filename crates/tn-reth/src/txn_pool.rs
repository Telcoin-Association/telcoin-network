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

use alloy::{
    consensus::Transaction as _,
    primitives::{keccak256, map::AddressSet, B256},
};
use futures::{future::OptionFuture, stream, Stream, StreamExt as _};
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, EthTransactionPool,
    TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_primitives_traits::SignerRecoverable;
use reth_provider::{
    providers::BlockchainProvider, AccountReader as _, CanonStateNotification,
    CanonStateSubscriptions as _, Chain, ChangedAccount, StateProviderBox,
    StateProviderFactory as _, TransactionsProvider as _,
};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    error::{
        Eip4844PoolTransactionError, Eip7702PoolTransactionError, InvalidPoolTransactionError,
        PoolError, PoolTransactionError,
    },
    AddedTransactionOutcome, BestTransactions, CanonicalStateUpdate, EthPooledTransaction,
    PoolSize, PoolTransaction, PoolUpdateKind, TransactionEvents, TransactionOrigin,
    TransactionPool as _, TransactionPoolExt as _, ValidPoolTransaction,
};
use std::{
    collections::HashMap,
    pin::pin,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tn_types::{
    gas_accumulator::BaseFeeContainer, min_batch_size, Address, BlockBody, BlsPublicKey,
    EnvKzgSettings, Recovered, SealedBlock, SealedHeader, TaskError, TaskSpawner,
    TransactionSigned, TxHash, U256,
};
use tokio::{task::JoinError, time::MissedTickBehavior};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, IntervalStream};
use tracing::{debug, info, trace, warn};

use crate::{
    error::TnRethResult,
    evm::TnEvmConfig,
    forward::{FORWARD_BATCH_BUDGET, FORWARD_PENDING_LIFETIME, REQUEUE_GRACE},
    forward_pending::{ForwardRetentionStatus, PendingForwards, RetentionLimits, SubmissionHead},
    metrics::{ForwarderMetrics, RETH_METRICS},
    peer_batch::PeerBatchTxs,
    traits::TelcoinNode,
    PoolTxn, PoolTxnId,
};

pub use reth_primitives_traits::InMemorySize as TxnSize;

/// Shared limits for locally sealed bytes awaiting execution or validated replay.
pub(crate) const LOCAL_SEAL_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Shared transaction entry ceiling for every worker pool in the node.
pub(crate) const LOCAL_SEAL_MAX_ENTRIES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Worker identity within one explicit node recovery owner.
struct LocalPoolId(
    /// Allocated identity, shared by clones of this pool.
    u64,
);

/// Ownership transition of one local seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalSealPhase {
    /// A build task owns bytes before the run loop accepts its result.
    Reserved,
    /// The run loop owns the forthcoming optimistic prune.
    Accepted,
    /// Optimistic removal has completed.
    Pruned,
}

/// Execution feedback independent of optimistic pruning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalExecutionPhase {
    /// The original accepted batch has not reported a skip.
    Pending,
    /// This exact observer batch handed its bytes to normal forwarding admission.
    ObserverForwarding,
    /// Forward ownership ended or early readmission was removed by optimistic pruning.
    ObserverRetryReady,
    /// Execution omitted these bytes; await canonical confirmation of this output.
    NonceGap(B256),
    /// The omitted output is canonical, so ordinary validation may readmit the bytes.
    ReplayReady,
    /// An asynchronous replay lease owns the current attempt.
    Replaying,
}

/// Signed bytes and canonical account identity owned by one seal.
#[derive(Debug)]
struct RetainedLocalTransaction {
    /// Exact signed transaction hash.
    hash: TxHash,
    /// Sender whose canonical nonce resolves ownership.
    sender: Address,
    /// Signed transaction nonce.
    nonce: u64,
    /// Exact admitted transaction encoding.
    raw: Vec<u8>,
    /// Confirmed omission and replay status.
    execution: LocalExecutionPhase,
}

/// One seal's independent ownership, including duplicate reservation references.
#[derive(Debug)]
struct RetainedLocalSeal {
    /// Worker pool to which replay is routed.
    pool: LocalPoolId,
    /// Optimistic removal status.
    phase: LocalSealPhase,
    /// Build task guards still referring to this seal.
    reservations: usize,
    /// Bytes retained through canonical resolution or normal readmission.
    transactions: Vec<RetainedLocalTransaction>,
}

/// One owner is shared by every worker pool belonging to a RethEnv.
/// Accepted bytes have no expiry and are never evicted to admit another seal.
#[derive(Debug)]
pub(crate) struct LocalSealRecovery {
    /// Digest identifies independent accepted batch ownership.
    seals: HashMap<B256, RetainedLocalSeal>,
    /// Total owned raw encoding bytes.
    bytes: usize,
    /// Total owned transactions, counting independent seals.
    entries: usize,
    /// Allocate worker identities within this node.
    next_pool: u64,
    /// Shared byte ceiling, clamped to the normal pending pool budget.
    max_bytes: usize,
    /// Shared transaction ceiling, clamped to the normal pending pool budget.
    max_entries: usize,
}

impl Default for LocalSealRecovery {
    fn default() -> Self {
        Self {
            seals: HashMap::new(),
            bytes: 0,
            entries: 0,
            next_pool: 0,
            max_bytes: LOCAL_SEAL_MAX_BYTES,
            max_entries: LOCAL_SEAL_MAX_ENTRIES,
        }
    }
}

impl LocalSealRecovery {
    /// Release one canonically resolved or successfully handed-back transaction.
    fn remove_transaction(&mut self, batch: &B256, hash: &TxHash) {
        let removed = self.seals.get_mut(batch).map(|seal| {
            let bytes = seal
                .transactions
                .iter()
                .filter(|transaction| transaction.hash == *hash)
                .map(|transaction| transaction.raw.len())
                .sum::<usize>();
            let before = seal.transactions.len();
            seal.transactions.retain(|transaction| transaction.hash != *hash);
            (
                bytes,
                before - seal.transactions.len(),
                seal.transactions.is_empty() && seal.reservations == 0,
            )
        });
        removed.into_iter().for_each(|(bytes, entries, empty)| {
            self.bytes -= bytes;
            self.entries -= entries;
            if empty {
                self.seals.remove(batch);
            }
        });
        crate::metrics::record_local_seal_retention(self.bytes, self.entries);
    }

    /// Record explicit omission only for bytes already owned by this local batch.
    pub(crate) fn nonce_gap(&mut self, batch: B256, hash: TxHash, output: B256) {
        self.seals.get_mut(&batch).into_iter().for_each(|seal| {
            seal.transactions.iter_mut().filter(|transaction| transaction.hash == hash).for_each(
                |transaction| {
                    if transaction.execution == LocalExecutionPhase::Pending {
                        transaction.execution = LocalExecutionPhase::NonceGap(output);
                    }
                },
            );
        });
    }

    /// Actual canonical hashes release bytes; output confirmation enables skipped-byte replay.
    fn canonical(&mut self, output: Option<B256>, hashes: &[TxHash]) {
        let batches: Vec<B256> = self.seals.keys().copied().collect();
        batches.into_iter().for_each(|batch| {
            hashes.iter().for_each(|hash| self.remove_transaction(&batch, hash));
        });
        self.seals.values_mut().for_each(|seal| {
            seal.transactions.iter_mut().for_each(|transaction| {
                if output.is_some_and(|root| {
                    transaction.execution == LocalExecutionPhase::NonceGap(root)
                }) {
                    transaction.execution = LocalExecutionPhase::ReplayReady;
                }
            });
        });
    }

    /// Mark actual optimistic pool removal, independently of canonical execution timing.
    fn pruned(&mut self, pool: LocalPoolId, hashes: &[TxHash]) {
        self.seals.values_mut().filter(|seal| seal.pool == pool).for_each(|seal| {
            if seal.phase == LocalSealPhase::Accepted
                && seal.transactions.iter().all(|transaction| hashes.contains(&transaction.hash))
            {
                seal.phase = LocalSealPhase::Pruned;
            }
        });
    }
}

/// A local seal cannot proceed unless its complete admitted bytes can be retained.
#[derive(Debug)]
pub enum LocalSealRecoveryError {
    /// Shared retention capacity cannot admit another accepted seal.
    Capacity,
    /// The signed hash lacks a validated normal pool owner.
    TransactionNotInPool(TxHash),
    /// Signed bytes cannot be decoded or their sender recovered.
    InvalidTransaction(String),
}

impl std::fmt::Display for LocalSealRecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capacity => formatter.write_str("local sealed transaction retention is full"),
            Self::TransactionNotInPool(hash) => {
                write!(formatter, "local sealed transaction is not validated in this pool: {hash}")
            }
            Self::InvalidTransaction(error) => {
                write!(formatter, "invalid local sealed transaction: {error}")
            }
        }
    }
}

impl std::error::Error for LocalSealRecoveryError {}

/// Rolls back an unacknowledged seal without releasing another seal's ownership.
#[derive(Debug)]
pub struct LocalSealReservation {
    /// Node-wide retained-byte owner.
    owner: Arc<Mutex<LocalSealRecovery>>,
    /// A live build task may roll this reservation back.
    pending: Option<B256>,
}

impl LocalSealReservation {
    /// Transfer the reservation to the accepted batch after its worker acknowledgement.
    pub fn accepted(mut self) {
        self.pending.take().into_iter().for_each(|batch| {
            let mut recovery = self.owner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            recovery.seals.get_mut(&batch).into_iter().for_each(|seal| {
                seal.reservations -= 1;
                if seal.phase == LocalSealPhase::Reserved {
                    seal.phase = LocalSealPhase::Accepted;
                }
            });
            if recovery
                .seals
                .get(&batch)
                .is_some_and(|seal| seal.reservations == 0 && seal.transactions.is_empty())
            {
                recovery.seals.remove(&batch);
            }
        });
    }
}

impl Drop for LocalSealReservation {
    fn drop(&mut self) {
        self.pending.take().into_iter().for_each(|batch| {
            let mut recovery = self.owner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let remove = recovery.seals.get_mut(&batch).is_some_and(|seal| {
                seal.reservations -= 1;
                seal.reservations == 0
                    && (seal.phase == LocalSealPhase::Reserved || seal.transactions.is_empty())
            });
            if remove {
                recovery.seals.remove(&batch).into_iter().for_each(|seal| {
                    recovery.bytes -= seal
                        .transactions
                        .iter()
                        .map(|transaction| transaction.raw.len())
                        .sum::<usize>();
                    recovery.entries -= seal.transactions.len();
                });
                crate::metrics::record_local_seal_retention(recovery.bytes, recovery.entries);
            }
        });
    }
}

/// Cancellation restores replay eligibility without dropping accepted bytes.
#[derive(Debug)]
struct LocalReplayLease {
    /// Node-wide byte owner survives asynchronous admission.
    owner: Arc<Mutex<LocalSealRecovery>>,
    /// Exact original seal ownership.
    batch: B256,
    /// Exact signed transaction being readmitted.
    hash: TxHash,
    /// Restore this role's replay state after cancellation or admission refusal.
    resume: LocalExecutionPhase,
}

impl Drop for LocalReplayLease {
    fn drop(&mut self) {
        let mut recovery = self.owner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        recovery.seals.get_mut(&self.batch).into_iter().for_each(|seal| {
            seal.transactions
                .iter_mut()
                .filter(|transaction| transaction.hash == self.hash)
                .for_each(|transaction| {
                    if transaction.execution == LocalExecutionPhase::Replaying {
                        transaction.execution = self.resume;
                    }
                });
        });
    }
}

/// Upper bound on canonical account reads per maintenance-loop iteration while recovering
/// from canonical-state broadcast lag (reth's `max_reload_accounts` analogue).
///
/// Lag means the loop is already behind, so recovery must not stall it further: each
/// iteration reloads at most this many dirty senders and carries the rest to the next
/// reload event, which is the next notification or the [`RELOAD_RETRY_INTERVAL`] tick,
/// whichever comes first.
const MAX_RELOAD_ACCOUNTS: usize = 100;

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
    /// Node-wide accepted local bytes, bounded across all worker pools.
    Arc<Mutex<LocalSealRecovery>>,
    /// Route omitted byte replay to its original worker pool.
    LocalPoolId,
    /// Per-worker cursor avoids duplicate reads and starvation between worker pools.
    Arc<Mutex<usize>>,
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
    pub fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
    ) -> eyre::Result<Self> {
        Self::new_with_local_recovery(
            node_config,
            task_spawner,
            blockchain_provider,
            evm_config,
            base_fee,
            Arc::new(Mutex::new(LocalSealRecovery::default())),
        )
    }

    /// Construct a worker pool with its explicit node-wide accepted-byte owner.
    pub(crate) fn new_with_local_recovery(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
        local_recovery: Arc<Mutex<LocalSealRecovery>>,
    ) -> eyre::Result<Self> {
        let this = Self::build_with_local_recovery(
            node_config,
            task_spawner,
            blockchain_provider,
            evm_config,
            base_fee,
            local_recovery,
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
        Self::build_with_local_recovery(
            node_config,
            task_spawner,
            blockchain_provider,
            evm_config,
            base_fee,
            Arc::new(Mutex::new(LocalSealRecovery::default())),
        )
    }

    /// Build the pool while sharing one recovery budget across the node's workers.
    fn build_with_local_recovery(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        evm_config: &TnEvmConfig,
        base_fee: BaseFeeContainer,
        local_recovery: Arc<Mutex<LocalSealRecovery>>,
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
        let pool_config = node_config.txpool.pool_config();
        let local_max_bytes = pool_config.pending_limit.max_size;
        let local_max_entries = pool_config.pending_limit.max_txs;
        let forward_limits = RetentionLimits::new(
            pool_config.pending_limit.max_txs,
            pool_config.pending_limit.max_size,
        );
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

        let pool_id = {
            let mut recovery =
                local_recovery.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            recovery.max_bytes = recovery.max_bytes.min(local_max_bytes);
            recovery.max_entries = recovery.max_entries.min(local_max_entries);
            recovery.next_pool =
                recovery.next_pool.checked_add(1).ok_or(LocalSealRecoveryError::Capacity)?;
            LocalPoolId(recovery.next_pool)
        };
        Ok(Self(
            transaction_pool,
            blockchain_provider.clone(),
            base_fee,
            PeerBatchTxs::default(),
            Arc::new(Mutex::new(PendingForwards::new(forward_limits))),
            local_recovery,
            pool_id,
            Arc::new(Mutex::new(0)),
        ))
    }

    /// Reserve validated local bytes before any worker can acknowledge this seal.
    pub fn reserve_local_seal(
        &self,
        batch: B256,
        raws: &[Vec<u8>],
    ) -> Result<LocalSealReservation, LocalSealRecoveryError> {
        let transactions: Vec<RetainedLocalTransaction> = raws
            .iter()
            .map(|raw| {
                let recovered = recover_raw_transaction(raw).map_err(|error| {
                    LocalSealRecoveryError::InvalidTransaction(error.to_string())
                })?;
                let hash = *recovered.hash();
                self.get(&hash).ok_or(LocalSealRecoveryError::TransactionNotInPool(hash))?;
                Ok(RetainedLocalTransaction {
                    hash,
                    sender: recovered.signer(),
                    nonce: recovered.nonce(),
                    raw: raw.clone(),
                    execution: LocalExecutionPhase::Pending,
                })
            })
            .collect::<Result<_, LocalSealRecoveryError>>()?;
        let mut recovery = self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // A recovered copy can race with reselection of the exact original digest. Defer
        // that reseal until replay finishes handing off its accepted ownership.
        (!recovery.seals.get(&batch).is_some_and(|seal| seal.phase != LocalSealPhase::Reserved))
            .then_some(())
            .ok_or(LocalSealRecoveryError::Capacity)?;
        if recovery.seals.contains_key(&batch) {
            recovery.seals.get_mut(&batch).into_iter().for_each(|seal| seal.reservations += 1);
        } else {
            let bytes = transactions.iter().map(|transaction| transaction.raw.len()).sum::<usize>();
            let next_bytes = recovery
                .bytes
                .checked_add(bytes)
                .filter(|next| *next <= recovery.max_bytes)
                .ok_or(LocalSealRecoveryError::Capacity)?;
            let next_entries = recovery
                .entries
                .checked_add(transactions.len())
                .filter(|next| *next <= recovery.max_entries)
                .ok_or(LocalSealRecoveryError::Capacity)?;
            if recovery.seals.len() >= recovery.max_entries {
                Err(LocalSealRecoveryError::Capacity)?;
            }
            recovery.seals.insert(
                batch,
                RetainedLocalSeal {
                    pool: self.6,
                    phase: LocalSealPhase::Reserved,
                    reservations: 1,
                    transactions,
                },
            );
            recovery.bytes = next_bytes;
            recovery.entries = next_entries;
        }
        crate::metrics::record_local_seal_retention(recovery.bytes, recovery.entries);
        Ok(LocalSealReservation { owner: self.5.clone(), pending: Some(batch) })
    }

    /// Retry only explicitly omitted, canonically confirmed, optimistically pruned bytes.
    async fn replay_local_seals(&self) {
        let ready = {
            let mut recovery = self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut keys: Vec<(B256, TxHash)> = recovery
                .seals
                .iter()
                .filter(|(_, seal)| seal.pool == self.6 && seal.phase == LocalSealPhase::Pruned)
                .flat_map(|(batch, seal)| {
                    seal.transactions
                        .iter()
                        .filter(|transaction| {
                            matches!(
                                transaction.execution,
                                LocalExecutionPhase::ReplayReady
                                    | LocalExecutionPhase::ObserverRetryReady
                            )
                        })
                        .map(|transaction| (*batch, transaction.hash))
                })
                .collect();
            keys.sort_unstable();
            keys.into_iter()
                .take(MAX_RELOAD_ACCOUNTS)
                .filter_map(|(batch, hash)| {
                    recovery
                        .seals
                        .get_mut(&batch)
                        .and_then(|seal| {
                            seal.transactions
                                .iter_mut()
                                .find(|transaction| transaction.hash == hash)
                        })
                        .map(|transaction| {
                            let resume = transaction.execution;
                            transaction.execution = LocalExecutionPhase::Replaying;
                            (
                                LocalReplayLease { owner: self.5.clone(), batch, hash, resume },
                                transaction.sender,
                                transaction.nonce,
                                transaction.raw.clone(),
                            )
                        })
                })
                .collect::<Vec<_>>()
        };
        futures::stream::iter(ready)
            .for_each(|(lease, sender, nonce, raw)| async move {
                let account = self.local_canonical_account(sender).await;
                OptionFuture::from(account.map(|account| async move {
                    self.0.update_accounts(vec![account]);
                    if account.nonce > nonce {
                        self.5
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove_transaction(&lease.batch, &lease.hash);
                    } else {
                        OptionFuture::from(recover_pooled_transaction(&raw).ok().map(
                            |recovered| async {
                                if self
                                    .0
                                    .add_transaction(TransactionOrigin::External, recovered)
                                    .await
                                    .is_ok()
                                {
                                    self.5
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .remove_transaction(&lease.batch, &lease.hash);
                                } else {
                                    self.finish_local_replay_handoff(&lease);
                                }
                            },
                        ))
                        .await;
                    }
                }))
                .await;
            })
            .await;
    }

    /// Finish a pool-present replay race, keeping quorum ownership stricter than observers.
    fn finish_local_replay_handoff(&self, lease: &LocalReplayLease) {
        let mut recovery = self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let newer_owner = recovery.seals.iter().any(|(batch, seal)| {
            *batch != lease.batch
                && seal.transactions.iter().any(|transaction| transaction.hash == lease.hash)
        });
        let observer_handoff = lease.resume == LocalExecutionPhase::ObserverRetryReady;
        if (observer_handoff || newer_owner) && self.get(&lease.hash).is_some() {
            recovery.remove_transaction(&lease.batch, &lease.hash);
        }
    }

    /// Observer success marks only this exact digest; local quorum bytes keep their role.
    pub fn mark_observer_seal(&self, batch: B256) {
        self.5
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .seals
            .get_mut(&batch)
            .filter(|seal| seal.pool == self.6)
            .into_iter()
            .for_each(|seal| {
                seal.transactions
                    .iter_mut()
                    .filter(|transaction| transaction.execution == LocalExecutionPhase::Pending)
                    .for_each(|transaction| {
                        transaction.execution = LocalExecutionPhase::ObserverForwarding
                    });
            });
    }

    /// Hand back only pruned observer bytes; early requeue must survive a later prune.
    fn reconcile_observer_seals(&self) {
        let pending = self.pending_forwards();
        let mut recovery = self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let observers: Vec<_> = recovery
            .seals
            .iter()
            .filter(|(_, seal)| seal.pool == self.6 && seal.phase == LocalSealPhase::Pruned)
            .flat_map(|(batch, seal)| {
                seal.transactions
                    .iter()
                    .filter(|transaction| {
                        matches!(
                            transaction.execution,
                            LocalExecutionPhase::ObserverForwarding
                                | LocalExecutionPhase::ObserverRetryReady
                        )
                    })
                    .map(|transaction| (*batch, transaction.hash))
            })
            .collect();
        observers.into_iter().for_each(|(batch, hash)| match pending.retention_status(&hash) {
            ForwardRetentionStatus::Retained => {}
            ForwardRetentionStatus::Queued | ForwardRetentionStatus::Absent
                if self.get(&hash).is_some() =>
            {
                recovery.remove_transaction(&batch, &hash)
            }
            ForwardRetentionStatus::Queued | ForwardRetentionStatus::Absent => {
                recovery.seals.get_mut(&batch).into_iter().for_each(|seal| {
                    seal.transactions
                        .iter_mut()
                        .filter(|transaction| transaction.hash == hash)
                        .for_each(|transaction| {
                            transaction.execution = LocalExecutionPhase::ObserverRetryReady
                        });
                });
            }
        });
    }

    /// Read canonical account state away from the asynchronous maintenance task.
    async fn local_canonical_account(&self, sender: Address) -> Option<ChangedAccount> {
        let pool = self.clone();
        tokio::task::spawn_blocking(move || {
            pool.1.latest().ok().and_then(|state| Self::load_changed_account(&state, sender).ok())
        })
        .await
        .ok()
        .flatten()
    }

    /// Prove that the canonical account has consumed a cached transaction's nonce.
    pub async fn local_transaction_canonically_resolved(&self, raw: &[u8]) -> bool {
        OptionFuture::from(recover_raw_transaction(raw).ok().map(|transaction| async move {
            self.local_canonical_account(transaction.signer())
                .await
                .is_some_and(|account| account.nonce > transaction.nonce())
        }))
        .await
        .unwrap_or(false)
    }

    /// Remove durable batch ownership only when canonical nonces consume every signed byte.
    pub fn local_batch_canonically_resolved(&self, raws: &[Vec<u8>]) -> bool {
        self.1.latest().ok().is_some_and(|state| {
            raws.iter().all(|raw| {
                recover_raw_transaction(raw).ok().is_some_and(|transaction| {
                    Self::load_changed_account(&state, transaction.signer())
                        .ok()
                        .is_some_and(|account| account.nonce > transaction.nonce())
                })
            })
        })
    }

    /// Release consumed nonces for every retained seal without making pending batches replayable.
    async fn reconcile_local_seals(&self) {
        let senders = {
            let recovery = self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut senders: Vec<_> = recovery
                .seals
                .values()
                .filter(|seal| seal.pool == self.6)
                .flat_map(|seal| seal.transactions.iter())
                .map(|transaction| transaction.sender)
                .collect();
            senders.sort_unstable();
            senders.dedup();
            let count = senders.len();
            let mut cursor = self.7.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let offset = (*cursor).min(count);
            senders.rotate_left(offset);
            *cursor = if count == 0 { 0 } else { (offset + MAX_RELOAD_ACCOUNTS) % count };
            senders.into_iter().take(MAX_RELOAD_ACCOUNTS).collect::<Vec<_>>()
        };
        stream::iter(senders)
            .for_each(|sender| async move {
                self.local_canonical_account(sender).await.into_iter().for_each(|account| {
                    let mut recovery =
                        self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    let consumed: Vec<_> = recovery
                        .seals
                        .iter()
                        .flat_map(|(batch, seal)| {
                            seal.transactions
                                .iter()
                                .filter(|transaction| {
                                    transaction.sender == sender
                                        && transaction.nonce < account.nonce
                                })
                                .map(|transaction| (*batch, transaction.hash))
                        })
                        .collect();
                    consumed
                        .into_iter()
                        .for_each(|(batch, hash)| recovery.remove_transaction(&batch, &hash));
                });
            })
            .await;
    }

    /// Hand accepted orphan ownership back only after every byte has a validated pool owner.
    pub fn release_local_batch_after_replay(&self, batch: B256) {
        let mut recovery = self.5.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let hashes: Vec<_> = recovery
            .seals
            .get(&batch)
            .filter(|seal| seal.reservations == 0)
            .into_iter()
            .flat_map(|seal| seal.transactions.iter().map(|transaction| transaction.hash))
            .collect();
        hashes.into_iter().for_each(|hash| recovery.remove_transaction(&batch, &hash));
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
            self.reconcile_local_seals().await;
            self.reconcile_observer_seals();
            self.replay_local_seals().await;
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
    fn mark_drifted(&self, missed: u64) -> AddressSet {
        warn!(
            target: "txpool",
            missed,
            "canonical state notifications lost to broadcast lag; resyncing pool sender \
             accounts from canonical state"
        );
        RETH_METRICS.canon_state_lagged_total.increment(1);
        RETH_METRICS.canon_state_notifications_missed_total.increment(missed);
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
            let locally_pruned = mined_transactions.clone();
            // Reth's pool and validator only read header fields from the tip. This synthetic
            // block satisfies that API without copying a body and stays inside this closure.
            let new_tip = SealedBlock::from_sealed_parts(new_tip, BlockBody::default());
            pool.apply_canonical_update(
                &new_tip,
                pending_block_blob_fee,
                mined_transactions,
                changed_accounts,
            );
            pool.5
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pruned(pool.6, &locally_pruned);
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

    /// This method is called when a canonical state update is received.
    ///
    /// Collect account changes and mined hashes on the blocking executor, borrowing the tip
    /// from the shared chain. Await completion before receiving another notification.
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
            pool.5
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .canonical(tip.parent_beacon_block_root, &mined_transactions);

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

    /// The shared window of transactions seen inside a validated peer batch.
    ///
    /// The batch validator records into this window and the batch builder reads it, so a
    /// transaction a peer is already proposing is not packed again here (issue #1329).
    pub fn peer_batch_txs(&self) -> &PeerBatchTxs {
        &self.3
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

    /// One node's cap is shared across worker pools and refusal leaves normal pool bytes intact.
    #[tokio::test]
    async fn local_recovery_capacity_is_shared_across_worker_pools() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, env, first_pool) = funded_pool_for_test(&factory, &tmp_dir, &task_manager);
        let second_pool = env.init_txn_pool(BaseFeeContainer::default())?;
        let transaction = factory.create_eip1559(
            chain,
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(1),
            Bytes::new(),
        );
        let raw = transaction.encoded_2718();
        factory.submit_tx_to_pool(transaction.clone(), first_pool.clone()).await;
        factory.submit_tx_to_pool(transaction.clone(), second_pool.clone()).await;
        {
            let mut owner = first_pool.5.lock().unwrap();
            owner.max_bytes = raw.len();
            owner.max_entries = 1;
        }
        let first_batch = B256::with_last_byte(1);
        first_pool.reserve_local_seal(first_batch, &[raw.clone()])?.accepted();
        assert!(matches!(
            second_pool.reserve_local_seal(B256::with_last_byte(2), &[raw.clone()]),
            Err(LocalSealRecoveryError::Capacity)
        ));
        assert!(first_pool.get(transaction.hash()).is_some());
        assert!(second_pool.get(transaction.hash()).is_some());
        assert_eq!(first_pool.5.lock().unwrap().entries, 1);
        first_pool.release_local_batch_after_replay(first_batch);
        assert!(second_pool.reserve_local_seal(B256::with_last_byte(2), &[raw]).is_ok());
        Ok(())
    }

    /// Canonical execution before ACK releases the final empty seal without another event.
    #[tokio::test]
    async fn local_recovery_canonical_before_ack_and_duplicate_cleanup() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, _env, pool) = funded_pool_for_test(&factory, &tmp_dir, &task_manager);
        let transaction = factory.create_eip1559(
            chain,
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(1),
            Bytes::new(),
        );
        let raw = transaction.encoded_2718();
        factory.submit_tx_to_pool(transaction.clone(), pool.clone()).await;
        let batch = B256::with_last_byte(1);
        let first = pool.reserve_local_seal(batch, &[raw.clone()])?;
        let duplicate = pool.reserve_local_seal(batch, &[raw])?;
        drop(first);
        {
            let mut owner = pool.5.lock().unwrap();
            assert_eq!(owner.entries, 1);
            assert_eq!(owner.seals[&batch].reservations, 1);
            owner.canonical(None, &[*transaction.hash()]);
            assert_eq!(owner.entries, 0);
            assert_eq!(owner.bytes, 0);
            assert_eq!(owner.seals.len(), 1);
        }
        duplicate.accepted();
        assert!(pool.5.lock().unwrap().seals.is_empty());
        Ok(())
    }

    /// Dropping an asynchronous replay attempt preserves bytes and restores eligibility.
    #[tokio::test]
    async fn local_recovery_replay_cancellation_keeps_accepted_bytes() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, _env, pool) = funded_pool_for_test(&factory, &tmp_dir, &task_manager);
        let transaction = factory.create_eip1559(
            chain,
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(1),
            Bytes::new(),
        );
        let raw = transaction.encoded_2718();
        factory.submit_tx_to_pool(transaction.clone(), pool.clone()).await;
        let batch = B256::with_last_byte(1);
        let output = B256::with_last_byte(2);
        pool.reserve_local_seal(batch, &[raw.clone()])?.accepted();
        {
            let mut owner = pool.5.lock().unwrap();
            owner.pruned(pool.6, &[*transaction.hash()]);
            owner.nonce_gap(batch, *transaction.hash(), output);
            owner.canonical(Some(output), &[]);
            owner.seals.get_mut(&batch).unwrap().transactions[0].execution =
                LocalExecutionPhase::Replaying;
        }
        drop(LocalReplayLease {
            owner: pool.5.clone(),
            batch,
            hash: *transaction.hash(),
            resume: LocalExecutionPhase::ReplayReady,
        });
        let owner = pool.5.lock().unwrap();
        assert_eq!(owner.bytes, raw.len());
        assert_eq!(owner.entries, 1);
        assert_eq!(owner.seals[&batch].transactions[0].execution, LocalExecutionPhase::ReplayReady);
        Ok(())
    }

    /// Early forward retry cannot hand off before prune; a late known copy can hand off afterward.
    #[tokio::test]
    async fn observer_retry_handoff_survives_prune_and_known_race() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let tasks = TaskManager::default();
        let mut factory = TransactionFactory::new();
        let (chain, env, _background) = funded_pool_for_test(&factory, &tmp_dir, &tasks);
        let pool = env.init_txn_pool_without_maintenance(BaseFeeContainer::default())?;
        let transaction = factory.create_eip1559(
            chain.clone(),
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(1),
            Bytes::new(),
        );
        let raw = transaction.encoded_2718();
        let hash = *transaction.hash();
        factory.submit_tx_to_pool(transaction.clone(), pool.clone()).await;
        let batch = B256::with_last_byte(5);
        pool.reserve_local_seal(batch, &[raw.clone()])?.accepted();
        pool.mark_observer_seal(batch);
        assert_eq!(pool.admit_forwards(vec![raw.clone()]), Some(vec![raw.clone()]));
        pool.pending_forwards().defer(&hash);
        (1_u8..=3).for_each(|output| {
            pool.pending_forwards().committed(u64::from(output), B256::repeat_byte(output), []);
        });
        pool.retry_forwarded(Instant::now() + REQUEUE_GRACE, 1).await;
        pool.reconcile_observer_seals();
        assert_eq!(
            pool.5.lock().unwrap().entries,
            1,
            "forward retry before prune cannot release ownership"
        );
        pool.update_canonical_state(
            &chain.sealed_genesis_header(),
            Some(u128::MAX),
            vec![hash],
            vec![],
        )
        .await?;
        assert!(pool.get(&hash).is_none());
        pool.reconcile_observer_seals();
        assert_eq!(
            pool.5.lock().unwrap().seals[&batch].transactions[0].execution,
            LocalExecutionPhase::ObserverRetryReady
        );
        pool.5.lock().unwrap().seals.get_mut(&batch).unwrap().transactions[0].execution =
            LocalExecutionPhase::Replaying;
        drop(LocalReplayLease {
            owner: pool.5.clone(),
            batch,
            hash,
            resume: LocalExecutionPhase::ObserverRetryReady,
        });
        assert_eq!(
            pool.5.lock().unwrap().seals[&batch].transactions[0].execution,
            LocalExecutionPhase::ObserverRetryReady
        );
        // Normal admission wins after replay selection, so the retry receives AlreadyKnown.
        factory.submit_tx_to_pool(transaction.clone(), pool.clone()).await;
        assert!(pool
            .0
            .add_transaction(
                TransactionOrigin::External,
                recover_pooled_transaction(&raw).expect("valid signed fixture")
            )
            .await
            .is_err());
        let observer = LocalReplayLease {
            owner: pool.5.clone(),
            batch,
            hash,
            resume: LocalExecutionPhase::ObserverRetryReady,
        };
        pool.finish_local_replay_handoff(&observer);
        assert!(pool.5.lock().unwrap().seals.is_empty());
        drop(observer);
        let resealed = pool.reserve_local_seal(batch, &[raw.clone()])?;
        resealed.accepted();
        pool.5.lock().unwrap().pruned(pool.6, &[hash]);
        let quorum = LocalReplayLease {
            owner: pool.5.clone(),
            batch,
            hash,
            resume: LocalExecutionPhase::ReplayReady,
        };
        pool.finish_local_replay_handoff(&quorum);
        assert_eq!(pool.5.lock().unwrap().entries, 1, "quorum bytes require a newer seal owner");
        drop(quorum);
        pool.release_local_batch_after_replay(batch);
        assert!(pool.reserve_local_seal(batch, &[raw]).is_ok());
        Ok(())
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
        let rpc_args = reth::args::RpcServerArgs { rpc_tx_fee_cap: 1_000, ..Default::default() };
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
}
