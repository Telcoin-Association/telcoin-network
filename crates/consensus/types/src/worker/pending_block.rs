//! Types for interacting with the worker.
//!
//! This is an experimental approach to supporting pending blocks for workers.

use reth_chainspec::ChainSpec;
use reth_primitives::{Address, SealedBlock, SealedBlockWithSenders, SealedHeader};
use reth_provider::ExecutionOutcome;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Type alias for a sender that sends [`WorkerBlockUpdates`]
pub type WorkerBlockUpdateSender = broadcast::Sender<WorkerBlockUpdate>;

/// Type alias for a receiver that receives [`CanonStateNotification`]
pub type WorkerBlockUpdates = broadcast::Receiver<WorkerBlockUpdate>;

/// The type that holds the worker's pending block.
///
/// The value is stored in memory and updated each time a worker's batch maker broadcasts a new
/// batch.
#[derive(Debug, Default)]
pub struct PendingWorkerBlock {
    /// The finalized, canonical tip used to propose this block.
    pub parent: SealedBlock,
    /// The sealed block the worker is proposing.
    pub pending: SealedBlockWithSenders,
    /// The state from execution outcome.
    state: Option<ExecutionOutcome>,
}

impl PendingWorkerBlock {
    /// Create a new instance of [Self].
    pub fn new(state: Option<ExecutionOutcome>) -> Self {
        todo!()
        // Self { state }
    }

    /// Return data for worker's current pending block.
    pub fn latest(&self) -> Option<ExecutionOutcome> {
        self.state.clone()
    }

    /// Return the account nonce if it exists.
    ///
    /// Based on reth's `StateProvider::account_nonce`.
    pub fn account_nonce(&self, address: &Address) -> Option<u64> {
        // check the execution output for a particular account's nonce
        self.state
            .as_ref()
            .map(|s| s.account(address))
            .unwrap_or_default()
            .unwrap_or_default()
            .map(|a| a.nonce)
    }
}

/// The information from a worker's pending block proposal that is streamed to the transaction
/// pool's maintenance task for updating transaction status.
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
    /// The chain spec.
    pub chain_spec: Arc<ChainSpec>,
    /// The worker primary's address.
    pub beneficiary: Address,
    /// The current information from canonical tip and finalized block.
    ///
    /// The block builder always extends the canonical tip. This struct
    /// is updated with rounds of consensus and used by the worker to
    /// build the next block.
    pub parent_info: LastCanonicalUpdate,
    /// The maximum gas for a block.
    ///
    /// This value is only measured by a transaction's gas_limit,
    /// not the actual amount of gas used during a transaction's execution.
    pub gas_limit: u64,
    /// The maximum size of the worker's block measured in bytes.
    pub max_size: usize,
}

impl PendingBlockConfig {
    /// Creates a new instance of [Self].
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        beneficiary: Address,
        parent_info: LastCanonicalUpdate,
        gas_limit: u64,
        max_size: usize,
    ) -> Self {
        Self { chain_spec, beneficiary, parent_info, gas_limit, max_size }
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
