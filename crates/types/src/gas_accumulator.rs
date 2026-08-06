//! Accumulate per-worker gas usage, block counts, and leader reward tallies over an epoch.
//!
//! [`GasAccumulator`] tracks gas used and gas limits for every block a worker produces during an
//! epoch, and its embedded [`RewardsCounter`] records how many times each leader committed a
//! block. At epoch boundaries these totals drive base-fee adjustments and validator reward
//! withdrawals.
//!
//! ## Worker count
//!
//! The number of worker slots is not fixed at construction. The on-chain `WorkerConfigs`
//! contract is the absolute source of truth, and the count for an epoch is the contract's state
//! at the previous epoch's closing block: `catchup_accumulator` (in `node::manager`) sizes the
//! accumulator from that state at startup, the epoch entry seeding re-seeds it on every entry
//! (`EpochBaseFees::apply`, or `sync_num_workers_from_chain` for epoch 0), and
//! `adjust_base_fees` resizes at epoch close to the count read from the closing block (the next
//! epoch's count). All resizes go through [`GasAccumulator::set_num_workers`].
//!
//! ## Startup recovery
//!
//! Because the accumulator is purely in-memory, its state must be rebuilt whenever a node restarts
//! mid-epoch. Three codepaths cooperate to restore it:
//!
//! 1. **`catchup_accumulator`** (in `node::manager`) — runs once at startup. It syncs the worker
//!    count from pinned closing-block chain state, walks the already-executed reth blocks for the
//!    current epoch to re-accumulate gas stats per worker, and iterates the consensus DB in reverse
//!    to restore leader counts for rounds that were already executed. Base fees are not restored
//!    here — the epoch entry seeding owns them.
//!
//! 2. **`EpochManager::replay_missed_consensus`** — replays any consensus output that was committed
//!    to the consensus DB but not yet executed before the previous shutdown. These blocks flow
//!    through the normal `payload_builder` execution path, which calls `inc_leader_count` and
//!    `inc_block`, filling in the gap between what `catchup_accumulator` restored and the live tip.
//!
//! 3. **`EpochManager::run_epoch`** — on `Initial` and `NewEpoch` modes, invokes
//!    `replay_missed_consensus` before starting the live consensus loop, ensuring no rounds are
//!    skipped or double-counted. State is updated through the normal path (payload_builder).
//!
//! ## Base fee on sync and restart (the base-fee-from-chain invariant)
//!
//! Base fee is consensus-affecting, so a node must never produce with a fee it cannot verify
//! against the chain. The entered epoch's worker count and per-worker fees are therefore owned
//! by the epoch entry seeding (`run_epoch` in `node::manager`), which reads them on EVERY entry
//! — live boundary crossing, restart, mid-epoch re-entry, or sync — from one pinned block: the
//! previous epoch's closing block, resolved from the entered epoch's registry-recorded first
//! block. That block's own system call recorded each worker's next-epoch fee in `WorkerConfigs`
//! storage, so the entry reads count, strategies, and fees from a single state read
//! (`read_base_fees_for_entered_epoch`, one row per worker through [`entry_fee_for_worker`]).
//! Epoch 0 has no prior epoch, so every [`BaseFeeContainer`] keeps the MIN default. A node that
//! cannot read that state halts rather than produce with an unverifiable fee. The live
//! producer's close-time computation (`adjust_base_fees`) folds the same formula over the same
//! inputs the closing block's record used, so the value it carries between close and the next
//! entry is identical to what the entry reads back.

use crate::{AuthorityIdentifier, Committee, SealedHeader, WorkerId};

use alloy::{
    eips::eip1559::{calc_next_block_base_fee, BaseFeeParams, MIN_PROTOCOL_BASE_FEE},
    primitives::{aliases::U184, Address},
    rpc::types::{Withdrawal, Withdrawals},
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::warn;

/// Fee strategy for a worker, read from the WorkerConfigs contract each epoch.
/// Adding a new strategy = new contract constant + new enum variant + match arm in
/// [`next_base_fee_for_config`] below.
///
/// NOTE: these are mapped in `tn-reth/src/system_calls.rs:decode_worker_fee_configs`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerFeeConfig {
    /// Adjust fee +/-12.5% per epoch based on gas utilization vs target.
    Eip1559 { target_gas: u64 },
    /// Fixed fee set by governance, no utilization-based adjustment.
    Static { fee: u64 },
}

/// The worker id that produced `header`, read from the low 16 bits of its `difficulty` field.
///
/// The engine encodes `difficulty` as `batch_index << 16 | worker_id` (matching how
/// [`GasAccumulator::inc_block`] callers attribute blocks), so the worker id is the low 16 bits and
/// the batch index occupies the upper bits.
///
/// ONE production consumer: `catchup_accumulator`'s startup accumulator restore (in
/// `node::manager`), which bounds every scanned header's worker id against the on-chain worker
/// count and then folds each header's gas into that worker's slot. It is NOT a fee derivation — the
/// entered epoch's fees come from the closing block's `WorkerConfigs` record through
/// [`entry_fee_for_worker`], which needs no header attribution at all. `tn-reth`'s `snapshot`
/// module re-exports the name; every use there is a test.
///
/// The MASK does have a second implementation to stay in sync with:
/// `tn-reth/src/evm/block.rs`'s `BlockCtx::worker_id` reads the same low 16 bits off the execution
/// context while a block is being built (and points back here), so a block's attribution is
/// identical whether it is taken from the context during execution or from the sealed header
/// afterwards. The accumulator's per-worker totals depend on that agreement, so the two move
/// together.
pub fn worker_id_from_header(header: &SealedHeader) -> WorkerId {
    (header.difficulty.into_limbs()[0] & 0xffff) as u16
}

/// One row of the on-chain `WorkerConfigs` table: a worker's fee strategy plus the raw `uint184`
/// `data` word stored alongside it.
///
/// The contract documents `data` as reserved space for the protocol. As of the epoch-close
/// base-fee snapshot it carries an [`WorkerFeeConfig::Eip1559`] worker's NEXT-epoch base fee,
/// written by the closing block's `setWorkerConfigsData` system call, so a node entering an epoch
/// can read the fee from one state slot instead of scanning the previous epoch's headers.
///
/// `data` is authoritative ONLY for a row whose config reads [`WorkerFeeConfig::Eip1559`] at an
/// epoch-closing block after the write path activated. A zero word means it was never written —
/// no epoch has closed since the worker was configured, or the row is [`WorkerFeeConfig::Static`]
/// and its fee already lives in the config's `value` (Static rows are never written). Non-zero
/// does NOT imply current: a row governance switches Eip1559 -> Static keeps its last recorded
/// word forever, and the owner setters can store an arbitrary `uint184`. Readers must gate on the
/// row's strategy, never on `data` alone. Zero stays unambiguous for the rows that do get
/// written, because a recorded fee is never below `MIN_PROTOCOL_BASE_FEE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerConfigEntry {
    /// The worker's fee strategy for the next epoch.
    pub config: WorkerFeeConfig,
    /// The raw `uint184` word stored with the config.
    pub data: U184,
}

/// Tracks how many blocks each leader committed during an epoch for reward distribution.
///
/// The committee must be set via [`RewardsCounter::set_committee`] before any consensus output
/// is executed so that authority identifiers can be resolved to execution addresses.
/// At the epoch boundary, [`RewardsCounter::generate_withdrawals`] converts the accumulated
/// counts into withdrawal records applied to the execution layer.
#[derive(Clone, Debug)]
pub struct RewardsCounter {
    /// The current epoch's committee, used to map authority identifiers to execution addresses.
    /// `None` until [`RewardsCounter::set_committee`] is called at epoch start.
    committee: Arc<RwLock<Option<Committee>>>,
    /// Number of committed leader blocks per authority for the current epoch.
    leader_counts: Arc<Mutex<HashMap<AuthorityIdentifier, u32>>>,
}

impl RewardsCounter {
    /// Increment the leader count for the given leader and return the new total.
    pub fn inc_leader_count(&self, leader: &AuthorityIdentifier) {
        let mut guard = self.leader_counts.lock();
        if let Some(v) = guard.get_mut(leader) {
            *v += 1;
        } else {
            guard.insert(leader.clone(), 1);
        }
    }

    /// Clear all leader counts. Called at epoch boundaries.
    pub fn clear(&self) {
        let mut guard = self.leader_counts.lock();
        guard.clear();
    }

    /// Set the committee on the current epoch.
    ///
    /// NOTE: this must be called before executing any consensus output.
    pub fn set_committee(&self, committee: Committee) {
        *self.committee.write() = Some(committee);
    }

    /// Returns a map of execution addresses to number of leader blocks they committed.
    pub fn get_address_counts(&self) -> BTreeMap<Address, u32> {
        let counts = self.leader_counts.lock();
        let mut result = BTreeMap::default();
        if let Some(committee) = self.committee.read().as_ref() {
            for (authority, count) in counts.iter() {
                if let Some(auth) = committee.authority(authority) {
                    let address = auth.execution_address();
                    // We should not have multiple validators with the same execution address but
                    // cover the case just in case someone does it (merges the
                    // counts for rewards).
                    if let Some(c) = result.get_mut(&address) {
                        *c += count;
                    } else {
                        result.insert(address, *count);
                    }
                }
            }
        }
        result
    }

    /// Generate the withdrawals from governance safe for the validator block rewards applied at the
    /// epoch boundary.
    pub fn generate_withdrawals(&self) -> Withdrawals {
        Withdrawals::new(
            self.get_address_counts()
                .into_iter()
                .map(|(address, amount)| Withdrawal {
                    index: 0,
                    validator_index: 0,
                    address,
                    amount: amount as u64,
                })
                .collect(),
        )
    }

    /// Use the authority's identifier to return an execution address for beneficiary address.
    pub fn get_authority_address(&self, authority_id: &AuthorityIdentifier) -> Option<Address> {
        self.committee.read().as_ref().and_then(|committee| {
            committee.authority(authority_id).map(|authority| authority.execution_address())
        })
    }
}

impl Default for RewardsCounter {
    fn default() -> Self {
        Self {
            leader_counts: Arc::new(Mutex::new(HashMap::default())),
            committee: Arc::new(RwLock::new(None)),
        }
    }
}

/// An interior-mutable, cloneable container for a single worker's base fee.
///
/// Shared between the accumulator and the batch builder so that base-fee
/// adjustments made at epoch boundaries are immediately visible to new batches.
#[derive(Clone, Debug)]
pub struct BaseFeeContainer {
    /// The current base fee value. Uses `Acquire`/`Release` ordering for cross-thread visibility.
    base_fee: Arc<AtomicU64>,
}

impl BaseFeeContainer {
    /// Create a new base fee container with base_fee.
    pub fn new(base_fee: u64) -> Self {
        Self { base_fee: Arc::new(AtomicU64::new(base_fee)) }
    }

    /// Return the contained base fee.
    pub fn base_fee(&self) -> u64 {
        self.base_fee.load(Ordering::Acquire)
    }

    /// Set the contained base fee.
    pub fn set_base_fee(&self, base_fee: u64) {
        self.base_fee.store(base_fee, Ordering::Release);
    }
}

impl Default for BaseFeeContainer {
    fn default() -> Self {
        Self::new(MIN_PROTOCOL_BASE_FEE)
    }
}

/// Running totals of block count and gas metrics for a single worker within one epoch.
#[derive(Debug, Default)]
struct GasTotals {
    /// Total blocks executed so far this epoch.
    blocks: u64,
    /// Total gas used so far this epoch.
    gas_used: u64,
    /// Total gas limit for executed blocks so far this epoch.
    gas_limit: u64,
}

/// Per-worker accumulation state: gas totals and the current base fee.
#[derive(Clone, Debug)]
struct Accumulated {
    /// Mutex-protected running gas totals for this worker.
    gas: Arc<Mutex<GasTotals>>,
    /// The worker's current base fee, shared with the batch builder.
    base_fee: BaseFeeContainer,
}

impl Default for Accumulated {
    fn default() -> Self {
        Self {
            gas: Arc::new(Mutex::new(GasTotals::default())),
            base_fee: BaseFeeContainer::default(),
        }
    }
}

/// Shared, cloneable accumulator for per-worker gas and block statistics over a single epoch.
///
/// Each worker slot stores its own [`GasTotals`] and [`BaseFeeContainer`]. The payload builder
/// calls [`GasAccumulator::inc_block`] after every executed batch, and
/// [`EpochManager::adjust_base_fees`] reads the totals at the epoch boundary to update each
/// worker's base fee for the next epoch.
///
/// The worker count is not fixed at construction: the on-chain `WorkerConfigs` contract is the
/// source of truth, and [`GasAccumulator::set_num_workers`] resizes the slot list in place to
/// match it at each epoch boundary. Resizing in place (rather than replacing the accumulator)
/// keeps the node-lifetime clones held by the engine and the [`RewardsCounter`] held by the EVM
/// config live across epochs.
///
/// If the engine is moved to a separate process in the future, this shared-memory design will
/// need to be replaced with an IPC mechanism (or something similar).
#[derive(Clone, Debug)]
pub struct GasAccumulator {
    /// One [`Accumulated`] entry per worker, indexed by worker id. Wrapped in `Arc` for
    /// cheap cloning across the engine and consensus tasks; the `RwLock` exists solely so
    /// [`GasAccumulator::set_num_workers`] can resize the slot list in place - per-slot state
    /// stays interior-mutable behind its own lock.
    inner: Arc<RwLock<Vec<Accumulated>>>,
    /// Leader block counts used to compute validator rewards at epoch boundaries.
    rewards_counter: RewardsCounter,
}

impl GasAccumulator {
    /// Create a new [`GasAccumulator`] with `workers` slots, all zeroed.
    pub fn new(workers: usize) -> Self {
        Self::new_with_rewards(workers, RewardsCounter::default())
    }

    /// Create a new [`GasAccumulator`] with `workers` slots and a pre-built [`RewardsCounter`].
    pub fn new_with_rewards(workers: usize, rewards: RewardsCounter) -> Self {
        let mut inner = Vec::with_capacity(workers);
        for _ in 0..workers {
            inner.push(Accumulated::default());
        }
        Self { inner: Arc::new(RwLock::new(inner)), rewards_counter: rewards }
    }

    /// Increment the block count, gas used, and gas limit for `worker_id`.
    ///
    /// Blocks with zero `gas_used` are silently skipped to avoid inflating counts on restarts.
    ///
    /// The totals recorded here are consensus-critical: at epoch close the block executor reads
    /// them to price next-epoch base fees and writes the result into EVM state
    /// (`record_next_epoch_base_fees` in `tn-reth`), so a missed or double-counted block
    /// diverges the closing block's hash across the fleet, not just a local fee.
    ///
    /// # Panics
    ///
    /// Panics if `worker_id` is out of range. Any batch that reaches execution has a valid id;
    /// an out-of-range id here means the accumulator disagrees with the chain on the worker
    /// count, and halting beats silently diverging gas totals.
    pub fn inc_block(&self, worker_id: WorkerId, gas_used: u64, gas_limit: u64) {
        // Don't bother accumulating empty blocks- helps with restarts.
        if gas_used == 0 {
            return;
        }
        let inner = self.inner.read();
        let accumulated = inner.get(worker_id as usize).unwrap_or_else(|| {
            panic!(
                "inc_block: worker id {worker_id} out of range for accumulator with {} workers - \
                 the on-chain worker-count sync may have failed or lagged (check epoch-manager \
                 warnings at epoch entry)",
                inner.len()
            )
        });
        let mut guard = accumulated.gas.lock();
        guard.blocks += 1;
        guard.gas_used += gas_used;
        guard.gas_limit += gas_limit;
    }

    /// Reset all gas totals and leader counts to zero. Called at epoch boundaries.
    pub fn clear(&self) {
        for acc in self.inner.read().iter() {
            let mut guard = acc.gas.lock();
            guard.blocks = 0;
            guard.gas_used = 0;
            guard.gas_limit = 0;
        }
        self.rewards_counter.clear();
    }

    /// Return `(blocks, gas_used, gas_limit)` for the given worker.
    ///
    /// # Panics
    ///
    /// Panics if `worker_id` is out of range.
    pub fn get_values(&self, worker_id: WorkerId) -> (u64, u64, u64) {
        let inner = self.inner.read();
        let guard = inner.get(worker_id as usize).expect("valid worker id").gas.lock();
        (guard.blocks, guard.gas_used, guard.gas_limit)
    }

    /// Return the shared [`BaseFeeContainer`] for `worker_id`. Mutations are visible to all
    /// holders of the returned clone.
    pub fn base_fee(&self, worker_id: WorkerId) -> BaseFeeContainer {
        let inner = self.inner.read();
        inner
            .get(worker_id as usize)
            .unwrap_or_else(|| {
                panic!(
                    "base_fee: worker id {worker_id} out of range for accumulator with {} workers \
                     - the on-chain worker-count sync may have failed or lagged (check \
                     epoch-manager warnings at epoch entry)",
                    inner.len()
                )
            })
            .base_fee
            .clone()
    }

    /// Return the number of workers in the accumulator.
    /// Worker ids will be 0 to one less that this value.
    pub fn num_workers(&self) -> usize {
        self.inner.read().len()
    }

    /// Resize the slot list in place to `num_workers` (clamped to at least 1).
    ///
    /// The change is visible to every clone of this accumulator, so node-lifetime handles (the
    /// engine's, the EVM config's rewards counter) stay live. Growing appends default slots
    /// (`MIN_PROTOCOL_BASE_FEE` fee, zero gas); shrinking truncates, discarding the removed
    /// workers' totals and fees. Existing slots and the [`RewardsCounter`] are untouched.
    ///
    /// # Concurrency
    ///
    /// The write lock makes the resize atomic with respect to concurrent
    /// [`GasAccumulator::inc_block`] / [`GasAccumulator::base_fee`] calls (read lock), and a
    /// call with the current size returns without resizing. Callers need not quiesce execution;
    /// the one safety bound is that the count must never shrink to or below a worker id still
    /// present in in-flight consensus output - `inc_block` panics on the truncated id by
    /// design. Grows are index-stable (existing slots keep their index and state), so they are
    /// always safe. The epoch-entry callers uphold the bound through value-stability rather
    /// than quiescence: they pin the count read to the previous epoch's closing block, so a
    /// mid-epoch (ModeChange) re-entry re-reads the identical count and no-ops here while the
    /// engine may still be executing leftover output - whose worker ids are all below that
    /// count.
    pub fn set_num_workers(&self, num_workers: usize) {
        // a chain always has at least worker 0.
        if num_workers == 0 {
            warn!(target: "epoch-manager", "attempt to set num workers to {num_workers}");
        }
        let num_workers = num_workers.max(1);
        let mut inner = self.inner.write();
        if inner.len() == num_workers {
            return;
        }
        inner.resize_with(num_workers, Accumulated::default);
    }

    /// Return a copy of the rewards counter object.
    pub fn rewards_counter(&self) -> RewardsCounter {
        self.rewards_counter.clone()
    }

    /// Use the authority's identifier to return an execution address for beneficiary address.
    pub fn get_authority_address(&self, authority_id: &AuthorityIdentifier) -> Option<Address> {
        self.rewards_counter.get_authority_address(authority_id)
    }
}

/// The largest `current_base_fee` [`compute_next_base_fee_eip1559`] may hand to alloy.
///
/// alloy's [`calc_next_block_base_fee`] adds its delta to `base_fee` with a plain `+`. That
/// addition is unchecked, so it panics under `overflow-checks` (the `dev`, `test` and `e2e`
/// profiles) and wraps silently under `release`, where a wrapped fee is a consensus value every
/// node agrees on and therefore validates cleanly instead of forking.
///
/// With `gas_used` already clamped to `gas_limit`, the delta alloy computes is at most
/// `base_fee * (gas_limit - gas_target) / (gas_target * 8)`, and `gas_limit - gas_target` never
/// exceeds `gas_target`, so the delta never exceeds `base_fee / 8`. Bounding `base_fee` by
/// `u64::MAX - u64::MAX / 8` therefore keeps `base_fee + base_fee / 8` inside `u64`.
///
/// This bound holds in the saturating regime too. For a `target_gas > u64::MAX / 2` the synthetic
/// `gas_limit` saturates at `u64::MAX`, alloy recovers `gas_target = u64::MAX / 2`, and
/// `gas_limit - gas_target` still does not exceed `gas_target`, so the `/ 8` ceiling is unchanged.
const SAFE_MAX_BASE_FEE: u64 = u64::MAX - u64::MAX / 8;

/// EIP-1559-style base fee adjustment computed once per epoch.
///
/// Compares `gas_used` (total gas consumed by a single worker during the epoch)
/// against `target_gas` (governance-set target for the epoch) and nudges the
/// base fee up or down by at most 12.5 % (denominator = 8).
///
/// Delegates the formula to alloy's [`calc_next_block_base_fee`] using
/// [`BaseFeeParams::ethereum`] (`elasticity_multiplier = 2`,
/// `max_change_denominator = 8`). The synthetic `gas_limit` passed to alloy is
/// `target_gas * 2` so alloy recovers the same `gas_target`, except for a
/// `target_gas > u64::MAX / 2`, where the multiply saturates and alloy instead
/// recovers `u64::MAX / 2`. Both inputs into alloy's delta arithmetic are
/// clamped so that arithmetic cannot leave `u64`: `gas_used` to `gas_limit`,
/// enforcing the EIP-1559 elasticity bound, and `current_base_fee` to
/// [`SAFE_MAX_BASE_FEE`], bounding the unchecked addition alloy performs on the
/// fee-increase arm.
///
/// The result is clamped to `[MIN_PROTOCOL_BASE_FEE, u64::MAX]`.
pub fn compute_next_base_fee_eip1559(current_base_fee: u64, gas_used: u64, target_gas: u64) -> u64 {
    if target_gas == 0 {
        return current_base_fee.max(MIN_PROTOCOL_BASE_FEE);
    }

    let params = BaseFeeParams::ethereum();
    let gas_limit = target_gas.saturating_mul(params.elasticity_multiplier as u64);
    let gas_used = gas_used.min(gas_limit);
    let current_base_fee = current_base_fee.min(SAFE_MAX_BASE_FEE);
    let new_base_fee = calc_next_block_base_fee(gas_used, gas_limit, current_base_fee, params);
    new_base_fee.max(MIN_PROTOCOL_BASE_FEE)
}

/// Apply a worker's [`WorkerFeeConfig`] to compute its next-epoch base fee.
///
/// `Eip1559 { target_gas }` nudges the fee toward the gas target via
/// [`compute_next_base_fee_eip1559`] (floored at `MIN_PROTOCOL_BASE_FEE`); `Static { fee }` pins
/// the fee to the governance-set value, ignoring gas usage.
///
/// This is the ONE fee formula, and it lives here so every seam that prices a worker's next-epoch
/// fee dispatches through the same strategy match: `record_next_epoch_base_fees` (in
/// `tn-reth::evm::block`) applies it inside the closing block to write the fee into the worker's
/// on-chain `WorkerConfigs.data` word — the record the epoch entry reads back — and
/// `adjust_base_fees` (in `node::manager`) applies it to the live accumulator at close time, so
/// both seams produce identical values from identical inputs.
pub fn next_base_fee_for_config(
    config: WorkerFeeConfig,
    current_base_fee: u64,
    gas_used: u64,
) -> u64 {
    match config {
        WorkerFeeConfig::Eip1559 { target_gas } => {
            compute_next_base_fee_eip1559(current_base_fee, gas_used, target_gas)
        }
        WorkerFeeConfig::Static { fee } => fee,
    }
}

/// Read worker `worker_id`'s base fee for the epoch being entered out of the
/// [`WorkerConfigEntry`] pinned at the previous epoch's closing block.
///
/// `Static { fee }` returns the configured fee and ignores `data` entirely — including a garbage
/// word. A static row's fee already lives in its config, the write path never records one, and a
/// row governance switched from `Eip1559` keeps its last recorded word forever, so `data` is stale
/// by design for this variant (see the data-semantics doc on [`WorkerConfigEntry`]).
///
/// `Eip1559` reads the recorded word. A zero word maps to `MIN_PROTOCOL_BASE_FEE`, and on
/// pre-activation history that is the same value the whole-epoch header derivation this read
/// replaced computed — but the cutover is value-identical only under two premises, neither of which
/// this function can enforce:
///
/// 1. Every pre-activation `Eip1559` row still reads `data == 0`. The write path activates at
///    `CONSENSUS_REGISTRY_FORK_EPOCH` (in [`crate::forks`]; adiri-gated, so no intra-doc link),
///    whose pre-deploy checklist requires confirming exactly that on the live contract, and whose
///    adiri rollout constraint forbids ANY `WorkerConfigs` write until the fork epoch has passed —
///    a `setWorkerConfig` that lands a word included, and so is a fee-neutral-looking `Static { fee
///    }` -> `Eip1559 { target_gas: u64::MAX }` flip, which has this read price MIN where the header
///    derivation priced ~0.875 · `fee`. See that constant's doc for the full constraint; a non-zero
///    word landed pre-fork splits the fleet through the exact-equality basefee check, and one above
///    `u64::MAX` fail-hards it (below).
/// 2. Pre-activation header base fees never rose above MIN on the live chain. The deleted
///    derivation anchored its fold on the header `base_fee_per_gas` of the worker's LAST genuine
///    block, falling back to MIN only on the epoch-0 base case, a worker with no block in the
///    scanned range, or a `Static` config — so MIN is what it computed only where that anchor was
///    itself MIN. This is an empirical fact about adiri (no governance target has moved a worker's
///    fee off the protocol minimum yet), NOT a structural identity.
///
/// The same floor lifts a hypothetical governance-written `1..=6` to the protocol minimum.
///
/// # Errors
///
/// An `Eip1559` word wider than `u64`. Every honest close records a `u64` fee, so a wider word can
/// only come from a foreign governance write — halting beats truncating an arbitrary word into a
/// consensus-critical fee. This is a tripwire rather than a live hazard: once the write path is
/// active the closing block's own system call rewrites every `Eip1559` row's word, so a read
/// pinned to a closing block only sees words that block itself wrote.
pub fn entry_fee_for_worker(worker_id: WorkerId, entry: &WorkerConfigEntry) -> Result<u64, String> {
    match entry.config {
        WorkerFeeConfig::Static { fee } => Ok(fee),
        WorkerFeeConfig::Eip1559 { .. } => {
            let recorded = u64::try_from(entry.data).map_err(|_| {
                format!(
                    "worker {worker_id}'s recorded WorkerConfigs data word {} exceeds u64::MAX and \
                     cannot be a base fee",
                    entry.data
                )
            })?;
            Ok(recorded.max(MIN_PROTOCOL_BASE_FEE))
        }
    }
}

impl Default for GasAccumulator {
    fn default() -> Self {
        Self::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_attribution_matches_header_encoding() {
        use crate::{ExecHeader, U256};
        // `difficulty` encodes `batch_index << 16 | worker_id`.
        let header = |number: u64, batch_index: u64, wid: u16| -> SealedHeader {
            SealedHeader::seal_slow(ExecHeader {
                number,
                difficulty: U256::from((batch_index << 16) | wid as u64),
                ..Default::default()
            })
        };

        // worker id is the LOW 16 bits; the batch index (upper bits) is ignored
        assert_eq!(worker_id_from_header(&header(5, 42, 3)), 3);
        assert_eq!(worker_id_from_header(&header(5, 0, 0xffff)), 0xffff);
    }

    #[test]
    fn gas_at_target_no_change() {
        // When gas_used == target_gas, delta is 0, fee unchanged
        assert_eq!(compute_next_base_fee_eip1559(1000, 500, 500), 1000);
    }

    #[test]
    fn gas_over_target_increases_fee() {
        // gas_used = 2 * target → delta = 1000 * 1.0 / 8 = 125
        assert_eq!(compute_next_base_fee_eip1559(1000, 1000, 500), 1125);
    }

    #[test]
    fn gas_under_target_decreases_fee() {
        // gas_used = 0 → delta = 1000 * 1.0 / 8 = 125
        assert_eq!(compute_next_base_fee_eip1559(1000, 0, 500), 875);
    }

    #[test]
    fn floor_at_min_protocol_base_fee() {
        // Large decrease should floor at MIN_PROTOCOL_BASE_FEE (7)
        // base=8, gas_used=0, target=1 → delta = 8 * 1 / 1 / 8 = 1, result = 7
        assert_eq!(compute_next_base_fee_eip1559(8, 0, 1), MIN_PROTOCOL_BASE_FEE);
    }

    #[test]
    fn zero_target_returns_current() {
        assert_eq!(compute_next_base_fee_eip1559(1000, 500, 0), 1000);
    }

    #[test]
    fn overflow_safety_large_values() {
        // Even when the caller passes `gas_used = u64::MAX`, the wrapper clamps it to
        // `gas_limit = 2 * target_gas`, so the increase is bounded by the 12.5 % EIP-1559
        // cap and the arithmetic stays within `u64`.
        let base = 1_000_000_000_000_000u64;
        let result = compute_next_base_fee_eip1559(base, u64::MAX, 1);
        assert_eq!(result, base + base / 8);
    }

    /// Mutation guard for the `.min(SAFE_MAX_BASE_FEE)` clamp: with the clamp removed, alloy's
    /// unchecked `base_fee + delta` overflows on every case here: a panic under the test
    /// profile's `overflow-checks`, and a wrapped (far lower) fee in a release build.
    #[test]
    fn ceiling_base_fee_never_overflows() {
        let target = 15_000_000u64;

        // the exact threshold: the unclamped sum is 2^64, which wraps to 0 and would surface as
        // MIN_PROTOCOL_BASE_FEE after the floor
        let threshold = 16_397_105_843_297_379_215u64;
        let at_threshold = compute_next_base_fee_eip1559(threshold, target * 2, target);
        assert!(
            at_threshold >= SAFE_MAX_BASE_FEE,
            "a fee increase must never collapse the fee: {at_threshold}"
        );

        // the widest word `entry_fee_for_worker` accepts, at the maximal +12.5% move
        let at_max = compute_next_base_fee_eip1559(u64::MAX, target * 2, target);
        assert_eq!(at_max, SAFE_MAX_BASE_FEE + SAFE_MAX_BASE_FEE / 8);

        // the smallest over-target move still takes alloy's `max(1, ..)` floored delta
        let barely_over = compute_next_base_fee_eip1559(u64::MAX, target + 1, target);
        assert!(barely_over >= SAFE_MAX_BASE_FEE, "must not wrap: {barely_over}");
    }

    /// The clamp is a no-op for every fee the protocol can actually hold, so no epoch that did not
    /// already overflow changes value. `SAFE_MAX_BASE_FEE` itself is the largest unaffected input.
    #[test]
    fn clamp_does_not_change_in_range_fees() {
        let target = 15_000_000u64;
        assert_eq!(compute_next_base_fee_eip1559(1000, target * 2, target), 1125);
        assert_eq!(
            compute_next_base_fee_eip1559(SAFE_MAX_BASE_FEE, target * 2, target),
            SAFE_MAX_BASE_FEE + SAFE_MAX_BASE_FEE / 8
        );
        // one below the clamp still moves by the full 12.5%
        let below = SAFE_MAX_BASE_FEE - 1;
        assert_eq!(compute_next_base_fee_eip1559(below, target * 2, target), below + below / 8);
    }

    /// A saturating `target_gas` is the other axis into alloy's delta arithmetic; the clamp has to
    /// hold there too, where `gas_target` is `u64::MAX / 2` rather than the configured target.
    #[test]
    fn ceiling_base_fee_never_overflows_with_saturating_target() {
        [u64::MAX / 2 + 1, u64::MAX - 1, u64::MAX].iter().for_each(|&target| {
            let out = compute_next_base_fee_eip1559(u64::MAX, u64::MAX, target);
            assert!(out >= SAFE_MAX_BASE_FEE, "target {target} wrapped to {out}");
        });
    }

    #[test]
    fn max_increase_is_12_5_percent() {
        // Even with huge gas overshoot, increase is capped at base_fee/8
        // excess = min(u64::MAX - 1, 1) = 1, delta = 1_000_000 * 1 / 1 / 8 = 125_000
        let base = 1_000_000u64;
        let result = compute_next_base_fee_eip1559(base, u64::MAX, 1);
        assert_eq!(result, base + base / 8);
    }

    #[test]
    fn max_decrease_is_12_5_percent() {
        // Even with zero gas usage, decrease is capped at base_fee/8
        // deficit = min(1, 1) = 1, delta = 1_000_000 * 1 / 1 / 8 = 125_000
        let base = 1_000_000u64;
        let result = compute_next_base_fee_eip1559(base, 0, 1);
        assert_eq!(result, base - base / 8);
    }

    #[test]
    fn small_values() {
        // Very small base fee
        assert_eq!(
            compute_next_base_fee_eip1559(MIN_PROTOCOL_BASE_FEE, 0, 100),
            MIN_PROTOCOL_BASE_FEE
        );
    }

    #[test]
    fn partial_utilization() {
        // 75% utilization → 25% under target → decrease by 25%/8 = 3.125%
        // base=1000, delta = 1000 * 250 / 1000 / 8 = 31
        assert_eq!(compute_next_base_fee_eip1559(1000, 750, 1000), 969);
    }

    #[test]
    fn slight_over_target() {
        // 110% utilization → 10% over → increase by 10%/8 = 1.25%
        // base=1000, delta = 1000 * 100 / 1000 / 8 = 12 (integer division)
        assert_eq!(compute_next_base_fee_eip1559(1000, 1100, 1000), 1012);
    }

    #[test]
    fn min_base_fee_still_increases_over_target() {
        // At MIN_PROTOCOL_BASE_FEE (7), integer 7/8 = 0 but delta is floored at 1
        assert_eq!(
            compute_next_base_fee_eip1559(MIN_PROTOCOL_BASE_FEE, 200, 100),
            MIN_PROTOCOL_BASE_FEE + 1
        );
    }

    #[test]
    fn eip1559_config_with_max_target_is_inert_at_min() {
        // Genesis/default strategy: Eip1559 { target_gas: u64::MAX }. Against an unreachable target
        // the fee can only ratchet down and floors at MIN, so a worker at MIN stays at MIN
        // regardless of gas used -- the inert guarantee that keeps existing chains unchanged.
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: u64::MAX };
        assert_eq!(
            next_base_fee_for_config(cfg, MIN_PROTOCOL_BASE_FEE, 5_000_000),
            MIN_PROTOCOL_BASE_FEE
        );
        // a non-MIN fee ratchets down (and never below MIN)
        let down = next_base_fee_for_config(cfg, MIN_PROTOCOL_BASE_FEE * 1000, 0);
        assert!((MIN_PROTOCOL_BASE_FEE..MIN_PROTOCOL_BASE_FEE * 1000).contains(&down));
    }

    #[test]
    fn eip1559_config_moves_fee_with_gas_vs_target() {
        let target = 1_000_000u64;
        let current = 1_000_000u64;
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: target };
        // gas above target -> fee increases; below -> decreases; at target -> unchanged.
        assert!(next_base_fee_for_config(cfg, current, 2_000_000) > current);
        assert!(next_base_fee_for_config(cfg, current, 0) < current);
        assert_eq!(next_base_fee_for_config(cfg, current, target), current);
    }

    #[test]
    fn static_config_pins_to_configured_fee() {
        // Static ignores gas usage and the current fee, always returning the governance-set value.
        assert_eq!(
            next_base_fee_for_config(
                WorkerFeeConfig::Static { fee: 12_345 },
                MIN_PROTOCOL_BASE_FEE,
                999_999
            ),
            12_345
        );
        assert_eq!(
            next_base_fee_for_config(WorkerFeeConfig::Static { fee: 500 }, 1_000_000, 0),
            500
        );
    }

    #[test]
    fn entry_fee_static_ignores_the_data_word() {
        // a static row's fee lives in its config; the write path never records one, so any word
        // the row carries is stale by design - never a fee input
        let cfg = WorkerFeeConfig::Static { fee: 12_345 };
        assert_eq!(
            entry_fee_for_worker(0, &WorkerConfigEntry { config: cfg, data: U184::ZERO }),
            Ok(12_345)
        );
        assert_eq!(
            entry_fee_for_worker(1, &WorkerConfigEntry { config: cfg, data: U184::from(999u64) }),
            Ok(12_345)
        );
        // garbage far beyond u64: still ignored, still no error
        assert_eq!(
            entry_fee_for_worker(2, &WorkerConfigEntry { config: cfg, data: U184::MAX }),
            Ok(12_345)
        );
    }

    #[test]
    fn entry_fee_eip1559_reads_the_recorded_word() {
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 };
        let entry = WorkerConfigEntry { config: cfg, data: U184::from(1_125_000u64) };
        assert_eq!(entry_fee_for_worker(0, &entry), Ok(1_125_000));
    }

    #[test]
    fn entry_fee_eip1559_zero_word_maps_to_min() {
        // the cutover case: a never-written row reads MIN, which is what the header-scan
        // derivation computed for every pre-activation epoch
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: u64::MAX };
        let entry = WorkerConfigEntry { config: cfg, data: U184::ZERO };
        assert_eq!(entry_fee_for_worker(0, &entry), Ok(MIN_PROTOCOL_BASE_FEE));
    }

    #[test]
    fn entry_fee_eip1559_floors_sub_min_words() {
        // 1..=6 is unreachable from the write path (a recorded fee is never below MIN) but a
        // governance write could store one; the floor keeps the protocol minimum
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 };
        for word in 1..MIN_PROTOCOL_BASE_FEE {
            let entry = WorkerConfigEntry { config: cfg, data: U184::from(word) };
            assert_eq!(entry_fee_for_worker(0, &entry), Ok(MIN_PROTOCOL_BASE_FEE), "word {word}");
        }
    }

    #[test]
    fn entry_fee_eip1559_accepts_u64_max_word() {
        // the widest word an honest close can record
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 };
        let entry = WorkerConfigEntry { config: cfg, data: U184::from(u64::MAX) };
        assert_eq!(entry_fee_for_worker(0, &entry), Ok(u64::MAX));
    }

    #[test]
    fn entry_fee_eip1559_rejects_word_wider_than_u64() {
        // one above the widest recordable fee: only a foreign write reaches here, so halt
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: 1_000_000 };
        let data = U184::from(u64::MAX) + U184::from(1u64);
        let err = entry_fee_for_worker(3, &WorkerConfigEntry { config: cfg, data })
            .expect_err("a word wider than u64 cannot be a base fee");
        assert!(err.contains("worker 3"), "error must name the worker: {err}");
        assert!(err.contains(&data.to_string()), "error must name the data word: {err}");
    }

    #[test]
    fn set_num_workers_grow_preserves_existing_and_defaults_new_slots() {
        let acc = GasAccumulator::new(1);
        acc.inc_block(0, 100, 200);
        acc.base_fee(0).set_base_fee(999);

        acc.set_num_workers(3);

        assert_eq!(acc.num_workers(), 3);
        // existing slot keeps its gas totals and fee
        assert_eq!(acc.get_values(0), (1, 100, 200));
        assert_eq!(acc.base_fee(0).base_fee(), 999);
        // new slots start at (MIN fee, zero gas)
        for worker_id in 1..3u16 {
            assert_eq!(acc.get_values(worker_id), (0, 0, 0));
            assert_eq!(acc.base_fee(worker_id).base_fee(), MIN_PROTOCOL_BASE_FEE);
        }
    }

    #[test]
    fn set_num_workers_shrink_truncates_high_slots() {
        let acc = GasAccumulator::new(3);
        acc.inc_block(0, 100, 200);
        acc.inc_block(2, 300, 400);
        acc.base_fee(2).set_base_fee(777);

        acc.set_num_workers(2);
        assert_eq!(acc.num_workers(), 2);
        assert_eq!(acc.get_values(0), (1, 100, 200));

        // re-growing yields a fresh default slot, not the truncated one
        acc.set_num_workers(3);
        assert_eq!(acc.get_values(2), (0, 0, 0));
        assert_eq!(acc.base_fee(2).base_fee(), MIN_PROTOCOL_BASE_FEE);
    }

    #[test]
    fn set_num_workers_clamps_zero_to_one() {
        let acc = GasAccumulator::new(2);
        acc.set_num_workers(0);
        assert_eq!(acc.num_workers(), 1);
    }

    /// LIVE-execution tripwire: a batch carrying a worker id the accumulator no longer covers
    /// must halt (see `inc_block`'s Panics doc - halting beats silently diverging gas totals).
    /// Pins a fragment of the panic message so a refactor cannot silently soften the tripwire.
    #[test]
    #[should_panic(expected = "worker id 1 out of range")]
    fn inc_block_after_shrink_panics() {
        let acc = GasAccumulator::new(2);
        acc.set_num_workers(1);
        acc.inc_block(1, 10, 20);
    }

    #[test]
    fn set_num_workers_same_size_is_noop() {
        let acc = GasAccumulator::new(2);
        acc.inc_block(1, 50, 60);
        acc.base_fee(1).set_base_fee(123);

        acc.set_num_workers(2);

        assert_eq!(acc.num_workers(), 2);
        assert_eq!(acc.get_values(1), (1, 50, 60));
        assert_eq!(acc.base_fee(1).base_fee(), 123);
    }

    #[test]
    fn clones_observe_resize() {
        // The engine holds a node-lifetime clone taken before any resize; a resize through the
        // manager's handle must be visible through it (the resize-in-place requirement).
        let acc = GasAccumulator::new(1);
        let engine_handle = acc.clone();

        acc.set_num_workers(2);

        assert_eq!(engine_handle.num_workers(), 2);
        // writes through the pre-resize clone land in the new slot
        engine_handle.inc_block(1, 10, 20);
        assert_eq!(acc.get_values(1), (1, 10, 20));
    }

    #[test]
    fn resize_preserves_rewards_counter_identity() {
        // RethEnv/TnEvmConfig hold a RewardsCounter clone taken at startup; resizing the worker
        // slots must not detach it.
        let acc = GasAccumulator::new(1);
        let evm_handle = acc.rewards_counter();

        acc.set_num_workers(3);

        let leader = AuthorityIdentifier::default();
        evm_handle.inc_leader_count(&leader);
        assert_eq!(acc.rewards_counter().leader_counts.lock().get(&leader), Some(&1));
    }

    /// `get_address_counts` resolves every tallied [`AuthorityIdentifier`] through the
    /// CURRENTLY-set committee: a tallied authority the committee no longer contains is silently
    /// skipped — its row vanishes from the map (and therefore from `generate_withdrawals`),
    /// neither zeroed nor errored. The underlying tally survives, so restoring the original
    /// committee restores the row: the drop is a property of the committee view, not data loss.
    ///
    /// This drop semantic is why the epoch-entry committee read must be pinned to the epoch's
    /// start state (the previous epoch's closing block): a node re-entering an epoch after a
    /// mid-epoch governance burn that seeds this counter from the post-burn tip committee
    /// silently drops the ejected leader's reward row, and the withdrawals it builds at the
    /// epoch close diverge from peers that kept the epoch-start committee — a different
    /// `withdrawals_root`, so a different closing-block hash.
    #[test]
    fn get_address_counts_drops_rows_for_non_committee_authorities() {
        use crate::{BlsKeypair, CommitteeBuilder};
        use rand::rng;

        const VICTIM_LEADER_BLOCKS: u32 = 7;

        let mut rng = rng();
        let keypairs: Vec<BlsKeypair> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let addresses: Vec<Address> = (0..4).map(|i| Address::repeat_byte(i as u8 + 1)).collect();

        // committee A: all 4 authorities (the epoch-start committee)
        let mut builder = CommitteeBuilder::new(1);
        for (keypair, address) in keypairs.iter().zip(addresses.iter()) {
            builder.add_authority(*keypair.public(), *address);
        }
        let committee_a = builder.build();

        let counter = RewardsCounter::default();
        counter.set_committee(committee_a.clone());

        // tally every member; the victim (index 0) gets a distinctive count
        let ids: Vec<AuthorityIdentifier> = keypairs
            .iter()
            .map(|keypair| {
                committee_a
                    .authority_by_key(keypair.public())
                    .expect("keypair is a committee member")
                    .id()
            })
            .collect();
        let victim_address = addresses[0];
        for (i, id) in ids.iter().enumerate() {
            let count = if i == 0 { VICTIM_LEADER_BLOCKS } else { 1 };
            for _ in 0..count {
                counter.inc_leader_count(id);
            }
        }

        // with committee A set, every member has a row
        let counts = counter.get_address_counts();
        assert_eq!(counts.len(), 4);
        assert_eq!(counts.get(&victim_address), Some(&VICTIM_LEADER_BLOCKS));
        for address in &addresses[1..] {
            assert_eq!(counts.get(address), Some(&1));
        }

        // committee B = A minus the victim (a post-burn tip read's committee)
        let mut builder = CommitteeBuilder::new(1);
        for (keypair, address) in keypairs.iter().zip(addresses.iter()).skip(1) {
            builder.add_authority(*keypair.public(), *address);
        }
        counter.set_committee(builder.build());

        // the victim's accumulated count is silently dropped — not zeroed, not an error
        let counts = counter.get_address_counts();
        assert_eq!(counts.len(), 3, "the non-member's row is dropped from the view");
        assert!(!counts.contains_key(&victim_address));
        for address in &addresses[1..] {
            assert_eq!(counts.get(address), Some(&1), "surviving rows are unchanged");
        }

        // the tally itself survived: restoring committee A restores the row intact
        counter.set_committee(committee_a);
        let counts = counter.get_address_counts();
        assert_eq!(counts.len(), 4);
        assert_eq!(counts.get(&victim_address), Some(&VICTIM_LEADER_BLOCKS));
    }

    /// Epoch-boundary invariant: `clear` resets every leader count so no rewards carry into the
    /// next epoch's withdrawals, while the committee survives for the next epoch's tallies.
    #[test]
    fn clear_resets_leader_counts_but_preserves_committee() {
        use crate::{BlsKeypair, CommitteeBuilder};
        use rand::rng;

        let mut rng = rng();
        let keypairs: Vec<BlsKeypair> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let addresses: Vec<Address> = (0..4).map(|i| Address::repeat_byte(i as u8 + 1)).collect();

        let mut builder = CommitteeBuilder::new(1);
        for (keypair, address) in keypairs.iter().zip(addresses.iter()) {
            builder.add_authority(*keypair.public(), *address);
        }
        let committee = builder.build();

        let counter = RewardsCounter::default();
        counter.set_committee(committee.clone());

        let ids: Vec<AuthorityIdentifier> = keypairs
            .iter()
            .map(|keypair| {
                committee
                    .authority_by_key(keypair.public())
                    .expect("keypair is a committee member")
                    .id()
            })
            .collect();

        // tally leader counts for two distinct addresses
        for _ in 0..3 {
            counter.inc_leader_count(&ids[0]);
        }
        counter.inc_leader_count(&ids[1]);

        // sanity: the tallies are visible before the boundary
        let counts = counter.get_address_counts();
        assert_eq!(counts.len(), 2);
        assert_eq!(counts.get(&addresses[0]), Some(&3));
        assert_eq!(counts.get(&addresses[1]), Some(&1));
        assert_eq!(counter.generate_withdrawals().len(), 2);

        counter.clear();

        // counts reset: nothing carries into the next epoch's incentives or withdrawals
        assert!(counter.get_address_counts().is_empty());
        assert!(counter.generate_withdrawals().is_empty());

        // the committee survives clear: a fresh tally still resolves through it
        counter.inc_leader_count(&ids[1]);
        let counts = counter.get_address_counts();
        assert_eq!(counts.len(), 1);
        assert_eq!(counts.get(&addresses[1]), Some(&1));
    }
}
