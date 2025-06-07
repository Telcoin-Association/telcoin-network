use std::sync::Arc;

use alloy::eips::eip1559::MIN_PROTOCOL_BASE_FEE;
use parking_lot::{Mutex, RwLock};

use crate::WorkerId;

/// An interiour mutable container for a workers base fee.
#[derive(Clone, Debug)]
pub struct BaseFeeContainer {
    base_fee: Arc<RwLock<u64>>,
}

impl BaseFeeContainer {
    /// Create a new base fee container with base_fee.
    pub fn new(base_fee: u64) -> Self {
        Self { base_fee: Arc::new(RwLock::new(base_fee)) }
    }

    /// Return the contained base fee.
    pub fn base_fee(&self) -> u64 {
        *self.base_fee.read()
    }

    /// Set the contained base fee.
    pub fn set_base_fee(&self, base_fee: u64) {
        *self.base_fee.write() = base_fee;
    }
}

impl Default for BaseFeeContainer {
    fn default() -> Self {
        Self::new(MIN_PROTOCOL_BASE_FEE)
    }
}

#[derive(Debug, Default)]
struct Inner {
    blocks: u64,
    gas_used: u64,
    gas_limit: u64,
}

/// This a is shared struct to accumulate gas/block info as an epock is built.
/// Can be used to calculate base fees at epoch boundaries.
/// This is a simple implementation that can be shared with the engine, if/when the
/// engine becomes a seperate process this will be a touch point that will need to
/// be changed.
#[derive(Clone, Debug)]
pub struct GasAccumulator {
    // Outer Arc for fast cloning.
    inner: Arc<Vec<(Arc<Mutex<Inner>>, BaseFeeContainer)>>,
}

impl GasAccumulator {
    /// Create a new empty ['GasAccumulator'].
    pub fn new(workers: usize) -> Self {
        let mut inner = Vec::with_capacity(workers);
        for _ in 0..workers {
            inner.push((Arc::new(Mutex::new(Inner::default())), BaseFeeContainer::default()));
        }
        Self { inner: Arc::new(inner) }
    }

    /// Increment the counts for a block.
    /// Note: will panic if given an invalid worker_id.
    /// Any batch that makes it to execution will have a valid worker id.
    pub fn inc_block(&self, worker_id: WorkerId, gas_used: u64, gas_limit: u64) {
        let mut guard = self.inner.get(worker_id as usize).expect("valid worker id").0.lock();
        guard.blocks += 1;
        guard.gas_used += gas_used;
        guard.gas_limit += gas_limit;
    }

    /// Clear the accumulated values, used at beginning of a new epoch.
    pub fn clear(&self) {
        for acc in self.inner.iter() {
            let mut guard = acc.0.lock();
            guard.blocks = 0;
            guard.gas_used = 0;
            guard.gas_limit = 0;
        }
    }

    /// Return the accumulated blocks, gas and gas limits.
    /// Note: will panic if given an invalid worker_id.
    /// Any batch that makes it to execution will have a valid worker id.
    pub fn get_values(&self, worker_id: WorkerId) -> (u64, u64, u64) {
        let guard = self.inner.get(worker_id as usize).expect("valid worker id").0.lock();
        (guard.blocks, guard.gas_used, guard.gas_limit)
    }

    /// Return the base fee (can be changed in place) for a worker.
    pub fn base_fee(&self, worker_id: WorkerId) -> BaseFeeContainer {
        self.inner.get(worker_id as usize).expect("valid worker id").1.clone()
    }

    /// Return the number of workers in the accumulator.
    /// Worker ids will be 0 to one less that this value.
    pub fn num_workers(&self) -> usize {
        self.inner.len()
    }
}

impl Default for GasAccumulator {
    fn default() -> Self {
        Self::new(1)
    }
}
