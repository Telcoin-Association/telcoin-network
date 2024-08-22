//! The worker's transaction pool.

use reth_primitives::{BlockNumHash, SealedBlock, SealedBlockWithSenders};
use reth_provider::ExecutionOutcome;

pub mod maintain;
mod metrics;

/// The information from a worker's pending block proposal that is streamed to the transaction pool's maintenance task for updating transaction status.
pub struct WorkerBlockUpdate {
    /// The finalized, canonical tip used to propose this block.
    pub parent: SealedBlock,
    /// The sealed block the worker is proposing.
    pub pending: SealedBlockWithSenders,
    /// The state from execution outcome.
    pub state: ExecutionOutcome,
}
