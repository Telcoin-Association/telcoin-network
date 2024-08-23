//! The worker's transaction pool.

use reth_primitives::{BlockNumHash, SealedBlock, SealedBlockWithSenders};
use reth_provider::ExecutionOutcome;

pub mod maintain;
mod metrics;
