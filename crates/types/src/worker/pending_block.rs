//! Types for interacting with the worker.
//!
//! This is an experimental approach to supporting pending blocks for workers.

use reth_primitives::{Address, SealedBlock};

/// The arguments passed to the worker's block builder.
#[derive(Debug)]
pub struct WorkerBlockBuilderArgs<Pool> {
    /// The transaction pool.
    pub pool: Pool,
    /// The attributes for the next block.
    pub batch_config: PendingBlockConfig,
}

impl<Pool> WorkerBlockBuilderArgs<Pool> {
    /// Create a new instance of [Self].
    pub fn new(pool: Pool, batch_config: PendingBlockConfig) -> Self {
        Self { pool, batch_config }
    }
}

/// The configuration to use for building the next batch.
#[derive(Debug)]
pub struct PendingBlockConfig {
    /// The worker primary's address.
    pub beneficiary: Address,
    /// The current information from canonical tip and finalized batch.
    ///
    /// The batch builder always extends the canonical tip. This struct
    /// is updated with rounds of consensus and used by the worker to
    /// build the next batch.
    pub parent_info: LastCanonicalUpdate,
}

impl PendingBlockConfig {
    /// Creates a new instance of [Self].
    pub fn new(beneficiary: Address, parent_info: LastCanonicalUpdate) -> Self {
        Self { beneficiary, parent_info }
    }
}

/// The struct that contains information from the latest canonical update.
///
/// Similar to `reth::pool::CanonicalStateUpdate` but without the lifetime headache.
/// This type is useful for tracking state between canonical updates so the block builder
/// can apply mined transaction updates without any other side effects.
#[derive(Debug, Clone)]
pub struct LastCanonicalUpdate {
    /// The finalized block from the latest round of consensus.
    pub tip: SealedBlock,
    /// EIP-1559 Base fee of the _next_ (pending) block
    ///
    /// The base fee of a block depends on the utilization of the last block and its base fee.
    pub pending_block_base_fee: u64,
    /// EIP-4844 blob fee of the _next_ (pending) block
    ///
    /// Only after Cancun
    pub pending_block_blob_fee: Option<u128>,
}
