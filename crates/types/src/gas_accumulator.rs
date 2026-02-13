//! Accumulate per-worker gas usage, block counts, and leader reward tallies over an epoch.
//!
//! [`GasAccumulator`] tracks gas used and gas limits for every block a worker produces during an
//! epoch, and its embedded [`RewardsCounter`] records how many times each leader committed a
//! block. At epoch boundaries these totals drive base-fee adjustments and validator reward
//! withdrawals.
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

use crate::{AuthorityIdentifier, Committee, WorkerId};
use alloy::{
    eips::eip1559::MIN_PROTOCOL_BASE_FEE,
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
/// If the engine is moved to a separate process in the future, this shared-memory design will
/// need to be replaced with an IPC mechanism (or something similar).
#[derive(Clone, Debug)]
pub struct GasAccumulator {
    /// One [`Accumulated`] entry per worker, indexed by worker id. Wrapped in `Arc` for
    /// cheap cloning across the engine and consensus tasks.
    inner: Arc<Vec<Accumulated>>,
    /// Leader block counts used to compute validator rewards at epoch boundaries.
    rewards_counter: RewardsCounter,
}

impl GasAccumulator {
    /// Create a new [`GasAccumulator`] with `workers` slots, all zeroed.
    pub fn new(workers: usize) -> Self {
        let mut inner = Vec::with_capacity(workers);
        for _ in 0..workers {
            inner.push(Accumulated::default());
        }
        Self { inner: Arc::new(inner), rewards_counter: RewardsCounter::default() }
    }

    /// Increment the block count, gas used, and gas limit for `worker_id`.
    ///
    /// Blocks with zero `gas_used` are silently skipped to avoid inflating counts on restarts.
    ///
    /// # Panics
    ///
    /// Panics if `worker_id` is out of range. Any batch that reaches execution has a valid id.
    pub fn inc_block(&self, worker_id: WorkerId, gas_used: u64, gas_limit: u64) {
        // Don't bother accumulating empty blocks- helps with restarts.
        if gas_used == 0 {
            return;
        }
        let mut guard = self.inner.get(worker_id as usize).expect("valid worker id").gas.lock();
        guard.blocks += 1;
        guard.gas_used += gas_used;
        guard.gas_limit += gas_limit;
    }

    /// Reset all gas totals and leader counts to zero. Called at epoch boundaries.
    pub fn clear(&self) {
        for acc in self.inner.iter() {
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
        let guard = self.inner.get(worker_id as usize).expect("valid worker id").gas.lock();
        (guard.blocks, guard.gas_used, guard.gas_limit)
    }

    /// Return the shared [`BaseFeeContainer`] for `worker_id`. Mutations are visible to all
    /// holders of the returned clone.
    pub fn base_fee(&self, worker_id: WorkerId) -> BaseFeeContainer {
        self.inner.get(worker_id as usize).expect("valid worker id").base_fee.clone()
    }

    /// Return the number of workers in the accumulator.
    /// Worker ids will be 0 to one less that this value.
    pub fn num_workers(&self) -> usize {
        self.inner.len()
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

impl Default for GasAccumulator {
    fn default() -> Self {
        Self::new(1)
    }
}
