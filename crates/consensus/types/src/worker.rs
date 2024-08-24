//! Types for interacting with the worker.

use reth_primitives::{SealedBlock, SealedBlockWithSenders, SealedHeader};
use reth_provider::ExecutionOutcome;
use reth_revm::primitives::{BlockEnv, CfgEnvWithHandlerCfg};
use tokio::sync::broadcast;

/// Type alias for a sender that sends [`WorkerBlockUpdates`]
pub type WorkerBlockUpdateSender = broadcast::Sender<WorkerBlockUpdate>;

/// Type alias for a receiver that receives [`CanonStateNotification`]
pub type WorkerBlockUpdates = broadcast::Receiver<WorkerBlockUpdate>;

/// The information from a worker's pending block proposal that is streamed to the transaction pool's maintenance task for updating transaction status.
#[derive(Clone, Debug)]
pub struct WorkerBlockUpdate {
    /// The finalized, canonical tip used to propose this block.
    pub parent: SealedBlock,
    /// The sealed block the worker is proposing.
    pub pending: SealedBlockWithSenders,
    /// The state from execution outcome.
    pub state: ExecutionOutcome,
}

/// The arguments passed to the worker's block builder.
#[derive(Debug)]
pub struct WorkerBlockBuilderArgs<Pool, Provider> {
    /// The state provider.
    pub provider: Provider,
    /// The transaction pool.
    pub pool: Pool,
    /// The attributes for the next block.
    // ConfigPendingBlock
    // PendingBlockConfig
    // NextBlockConfig
    pub block_config: PendingBlockConfig,
}

/// The configuration to use for building the next worker block.
#[derive(Debug)]
pub struct PendingBlockConfig {
    /// The parent to use for building the next block.
    pub parent: SealedHeader,
    /// Pre-configured block environment.
    pub initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub initialized_cfg: CfgEnvWithHandlerCfg,
}
