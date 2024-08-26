//! Types for interacting with the worker.

use std::sync::Arc;

use reth_chainspec::ChainSpec;
use reth_primitives::{Address, SealedBlock, SealedBlockWithSenders, SealedHeader};
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

impl WorkerBlockUpdate {
    /// Create new instance of [Self].
    pub fn new(
        parent: SealedBlock,
        pending: SealedBlockWithSenders,
        state: ExecutionOutcome,
    ) -> Self {
        Self { parent, pending, state }
    }

    /// Return a reference to the worker block's parent.
    pub fn parent(&self) -> &SealedBlock {
        &self.parent
    }
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

impl<Pool, Provider> WorkerBlockBuilderArgs<Pool, Provider> {
    /// Create a new instance of [Self].
    pub fn new(provider: Provider, pool: Pool, block_config: PendingBlockConfig) -> Self {
        Self { provider, pool, block_config }
    }
}

/// The configuration to use for building the next worker block.
#[derive(Debug)]
pub struct PendingBlockConfig {
    /// The parent to use for building the next block.
    pub parent: SealedBlock,
    /// Pre-configured block environment.
    pub initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub initialized_cfg: CfgEnvWithHandlerCfg,
    /// The chain spec.
    pub chain_spec: Arc<ChainSpec>,
    /// The timestamp (sec) for when this block was built.
    pub timestamp: u64,
    /// The worker primary's address.
    pub beneficiary: Address,
}

impl PendingBlockConfig {
    /// Creates a new instance of [Self].
    pub fn new(
        parent: SealedBlock,
        initialized_block_env: BlockEnv,
        initialized_cfg: CfgEnvWithHandlerCfg,
        chain_spec: Arc<ChainSpec>,
        timestamp: u64,
        beneficiary: Address,
    ) -> Self {
        Self { parent, initialized_block_env, initialized_cfg, chain_spec, timestamp, beneficiary }
    }
}
