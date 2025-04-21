//! Types for interacting with the worker.
//!
//! This is an experimental approach to supporting pending blocks for workers.

use crate::{Address, SealedBlock};

/// The arguments passed to the worker's block builder.
#[derive(Debug)]
pub struct BatchBuilderArgs<Pool> {
    /// The transaction pool.
    pub pool: Pool,
    /// The attributes for the next block.
    pub batch_config: PendingBatchConfig,
}

impl<Pool> BatchBuilderArgs<Pool> {
    /// Create a new instance of [Self].
    pub fn new(pool: Pool, batch_config: PendingBatchConfig) -> Self {
        Self { pool, batch_config }
    }
}

/// The configuration to use for building the next batch.
#[derive(Debug)]
pub struct PendingBatchConfig {
    /// The worker primary's address.
    pub beneficiary: Address,
    /// The current information from canonical tip and finalized batch.
    ///
    /// The batch builder always extends the canonical tip. This struct
    /// is updated with rounds of consensus and used by the worker to
    /// build the next batch.
    pub parent_info: SealedBlock,
}

impl PendingBatchConfig {
    /// Creates a new instance of [Self].
    pub fn new(beneficiary: Address, parent_info: SealedBlock) -> Self {
        Self { beneficiary, parent_info }
    }
}
