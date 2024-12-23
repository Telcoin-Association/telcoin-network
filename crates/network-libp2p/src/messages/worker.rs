//! P2p messages between workers.

use serde::{Deserialize, Serialize};
use tn_types::{BlockHash, SealedWorkerBlock};

/// Requests between workers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Broadcast a newly produced worker block.
    ///
    /// NOTE: expect no response
    NewBlock(SealedWorkerBlock),
    /// The missing blocks for this peer.
    MissingBlocks {
        /// The collection of missing [BlockHash]es.
        digests: Vec<BlockHash>,
    },
}

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    MissingBlocks {
        /// The collection of requested blocks.
        blocks: Vec<SealedWorkerBlock>,
        // TODO: calculate this on requesting peer side:
        //  - if missing data, how much was returned?
        //      - request again if size limit reached?
        //  - should be able to calculate independently, without trust
        //
        // /// If true, the primary should request the blocks from the workers again.
        // /// This may not be something that can be trusted from a remote worker.
        // size_limit_reached: bool,
    },
}
