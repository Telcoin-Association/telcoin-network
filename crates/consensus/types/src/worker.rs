//! Types for interacting with the worker.

use reth_primitives::{SealedBlock, SealedBlockWithSenders};
use reth_provider::ExecutionOutcome;
use tokio::sync::broadcast;

/// Type alias for a sender that sends [`WorkerBlockUpdates`]
pub type WorkerBlockUpdateSender = broadcast::Sender<WorkerBlockUpdate>;

/// Type alias for a receiver that receives [`CanonStateNotification`]
pub type WorkerBlockUpdates = broadcast::Receiver<WorkerBlockUpdate>;

/// The information from a worker's pending block proposal that is streamed to the transaction pool's maintenance task for updating transaction status.
pub struct WorkerBlockUpdate {
    /// The finalized, canonical tip used to propose this block.
    pub parent: SealedBlock,
    /// The sealed block the worker is proposing.
    pub pending: SealedBlockWithSenders,
    /// The state from execution outcome.
    pub state: ExecutionOutcome,
}
