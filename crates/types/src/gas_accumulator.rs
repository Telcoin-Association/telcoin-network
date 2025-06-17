//! Implement an accumilator to tatal gas and blocks for an epoch.
//! This can be used to adjust per worker base fees on the next epoch.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use alloy::{eips::eip1559::MIN_PROTOCOL_BASE_FEE, primitives::Address};
use parking_lot::{Mutex, RwLock};

use crate::{AuthorityIdentifier, Committee, WorkerId};

/// Maintains a counter of each leader's execution address for an epoch for calculating rewards.
#[derive(Clone, Debug)]
pub struct RewardsCounter {
    committee: Arc<RwLock<Option<Committee>>>,
    leader_counts: Arc<Mutex<HashMap<AuthorityIdentifier, u32>>>,
}

impl RewardsCounter {
    /// Increment the leader count for leader.
    pub fn inc_leader_count(&self, leader: &AuthorityIdentifier) -> u32 {
        let mut guard = self.leader_counts.lock();
        if let Some(v) = guard.get_mut(leader) {
            *v += 1;
        } else {
            guard.insert(leader.clone(), 1);
        }
        guard.get(leader).copied().unwrap_or(0)
    }

    /// Clear counts.
    pub fn clear(&self) {
        let mut guard = self.leader_counts.lock();
        guard.clear();
    }

    /// Set the committee on the current epoch.
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
}

impl Default for RewardsCounter {
    fn default() -> Self {
        Self {
            leader_counts: Arc::new(Mutex::new(HashMap::default())),
            committee: Arc::new(RwLock::new(None)),
        }
    }
}

/// An interiour mutable container for a workers base fee.
#[derive(Clone, Debug)]
pub struct BaseFeeContainer {
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

#[derive(Debug, Default)]
struct GasTotals {
    /// Total blocks executed so far this epoch.
    blocks: u64,
    /// Total gas used so far this epoch.
    gas_used: u64,
    /// Total gas limit for executed blocks so far this epoch.
    gas_limit: u64,
}

#[derive(Clone, Debug)]
struct Accumulated {
    gas: Arc<Mutex<GasTotals>>,
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

/// This is a shared struct to accumulate gas/block info as an epock is built.
/// Can be used to calculate base fees at epoch boundaries.
/// This is a simple implementation that can be shared with the engine, if/when the
/// engine becomes a seperate process this will be a touch point that will need to
/// be changed.
#[derive(Clone, Debug)]
pub struct GasAccumulator {
    // Outer Arc for fast cloning.
    inner: Arc<Vec<Accumulated>>,
    /// Accumulated leader counts for rewards.
    rewards_counter: RewardsCounter,
}

impl GasAccumulator {
    /// Create a new empty ['GasAccumulator'].
    pub fn new(workers: usize) -> Self {
        let mut inner = Vec::with_capacity(workers);
        for _ in 0..workers {
            inner.push(Accumulated::default());
        }
        Self { inner: Arc::new(inner), rewards_counter: RewardsCounter::default() }
    }

    /// Increment the counts for a block.
    /// Note: will panic if given an invalid worker_id.
    /// Any batch that makes it to execution will have a valid worker id.
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

    /// Clear the accumulated values, used at beginning of a new epoch.
    pub fn clear(&self) {
        for acc in self.inner.iter() {
            let mut guard = acc.gas.lock();
            guard.blocks = 0;
            guard.gas_used = 0;
            guard.gas_limit = 0;
        }
        self.rewards_counter.clear();
    }

    /// Return the accumulated blocks, gas and gas limits.
    /// Note: will panic if given an invalid worker_id.
    /// Any batch that makes it to execution will have a valid worker id.
    pub fn get_values(&self, worker_id: WorkerId) -> (u64, u64, u64) {
        let guard = self.inner.get(worker_id as usize).expect("valid worker id").gas.lock();
        (guard.blocks, guard.gas_used, guard.gas_limit)
    }

    /// Return the base fee (can be changed in place) for a worker.
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
}

impl Default for GasAccumulator {
    fn default() -> Self {
        Self::new(1)
    }
}
