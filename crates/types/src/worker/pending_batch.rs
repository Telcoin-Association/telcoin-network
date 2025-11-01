//! Types for interacting with the worker.
//!
//! This is an experimental approach to supporting pending blocks for workers.

use crate::{Address, Epoch};

/// The arguments passed to the worker's block builder.
#[derive(Debug)]
pub struct BatchBuilderArgs<Pool> {
    /// The transaction pool.
    pub pool: Pool,
    /// The worker primary's address.
    pub beneficiary: Address,
    /// The epoch for the batch being built.
    pub epoch: Epoch,
}

impl<Pool> BatchBuilderArgs<Pool> {
    /// Create a new instance of [Self].
    pub fn new(pool: Pool, beneficiary: Address, epoch: Epoch) -> Self {
        Self { pool, beneficiary, epoch }
    }
}
