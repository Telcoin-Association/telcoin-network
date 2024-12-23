//! Outbound messages sent to peers.

use serde::{Deserialize, Serialize};
use tn_types::{BlockHash, SealedWorkerBlock};

/// Requests from workers.
pub enum WorkerRequest {
    /// Broadcast a newly produced worker block.
    NewBlock(SealedWorkerBlock),
    /// Worker is missing blocks
    MissingBlocks(MissingBlocksRequest),
}

/// Response to worker requests.
pub enum WorkerResponse {}

/// Used by worker to request missing blocks from other workers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MissingBlocksRequest {
    /// Vec of requested blocks' digests
    pub block_digests: Vec<BlockHash>,
}
