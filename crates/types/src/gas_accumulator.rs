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
//! contract is the absolute source of truth: `sync_num_workers_from_chain` (in `node::manager`)
//! reads the count for the entered epoch at the epoch's first block's parent and resizes the
//! accumulator in place via [`GasAccumulator::set_num_workers`] - at startup (before catchup)
//! and at every epoch entry - and `adjust_base_fees` resizes at epoch close to the count read
//! from the closing block (the next epoch's count).
//!
//! ## Startup recovery
//!
//! Because the accumulator is purely in-memory, its state must be rebuilt whenever a node restarts
//! mid-epoch. Three codepaths cooperate to restore it:
//!
//! 1. **`catchup_accumulator`** (in `node::manager`) — runs once at startup. It walks the
//!    already-executed reth blocks for the current epoch to re-accumulate gas stats per worker,
//!    restores the base fee from the finalized header, and iterates the consensus DB in reverse to
//!    restore leader counts for rounds that were already executed.
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
//! Base fee is consensus-affecting, so a node that is merely catching up must never recompute it
//! and risk diverging from the value already baked into the chain. At epoch entry, if the
//! canonical tip is already in the epoch being entered, each worker's [`BaseFeeContainer`] is
//! seeded from its most recent on-chain block (`latest_base_fee_per_worker`); otherwise the
//! container keeps the value the live producer just computed at the boundary. Only the live
//! producer crossing the boundary computes the next fee forward; everyone else reads the chain.

use crate::{AuthorityIdentifier, Committee, WorkerId};

/// Fee strategy for a worker, read from the WorkerConfigs contract each epoch.
/// Adding a new strategy = new contract constant + new enum variant + match arm in
/// adjust_base_fees.
///
/// NOTE: these are mapped in `tn-reth/src/lib.rs:worker_fee_configs_inner`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerFeeConfig {
    /// Adjust fee +/-12.5% per epoch based on gas utilization vs target.
    Eip1559 { target_gas: u64 },
    /// Fixed fee set by governance, no utilization-based adjustment.
    Static { fee: u64 },
}

use alloy::{
    eips::eip1559::{calc_next_block_base_fee, BaseFeeParams, MIN_PROTOCOL_BASE_FEE},
    primitives::Address,
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
        let mut guard = inner.get(worker_id as usize).expect("valid worker id").gas.lock();
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
        self.inner.read().get(worker_id as usize).expect("valid worker id").base_fee.clone()
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
    /// Callers must only invoke this while no consensus output is executing (startup before
    /// catchup, epoch entry before replay, epoch close after final execution): a resize that
    /// races `inc_block` for a removed worker id panics there by design.
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

/// EIP-1559-style base fee adjustment computed once per epoch.
///
/// Compares `gas_used` (total gas consumed by a single worker during the epoch)
/// against `target_gas` (governance-set target for the epoch) and nudges the
/// base fee up or down by at most 12.5 % (denominator = 8).
///
/// Delegates the formula to alloy's [`calc_next_block_base_fee`] using
/// [`BaseFeeParams::ethereum`] (`elasticity_multiplier = 2`,
/// `max_change_denominator = 8`). The synthetic `gas_limit` passed to alloy is
/// `target_gas * 2` so alloy recovers the same `gas_target`. `gas_used` is
/// clamped to `gas_limit` to enforce the EIP-1559 elasticity bound and avoid
/// the unbounded delta arithmetic alloy would otherwise produce when callers
/// pass `gas_used > gas_limit`.
///
/// The result is clamped to `[MIN_PROTOCOL_BASE_FEE, u64::MAX]`.
pub fn compute_next_base_fee_eip1559(current_base_fee: u64, gas_used: u64, target_gas: u64) -> u64 {
    if target_gas == 0 {
        return current_base_fee.max(MIN_PROTOCOL_BASE_FEE);
    }

    let params = BaseFeeParams::ethereum();
    let gas_limit = target_gas.saturating_mul(params.elasticity_multiplier as u64);
    let gas_used = gas_used.min(gas_limit);
    let new_base_fee = calc_next_block_base_fee(gas_used, gas_limit, current_base_fee, params);
    new_base_fee.max(MIN_PROTOCOL_BASE_FEE)
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
}
