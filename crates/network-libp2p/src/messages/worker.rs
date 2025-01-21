//! P2p messages between workers.

use crate::codec::TNMessage;
use serde::{Deserialize, Serialize};
use tn_types::{BlockHash, SealedBatch};

// impl TNMessage trait for types
impl TNMessage for WorkerRequest {}
impl TNMessage for WorkerResponse {}

/// Requests between workers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Broadcast a newly produced batch.
    ///
    /// NOTE: expect no response
    /// TODO: gossip this instead?
    NewBatch(SealedBatch),
    /// The collection of missing [BlockHash]es for this peer.
    MissingBatches(Vec<BlockHash>),
}

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    /// Return the missing blocks requested by the peer.
    ///
    /// TODO: anemo included `size_limit_reached: bool` field
    /// but this should be trustless. See `RequestBatchesResponse` message.
    MissingBatches {
        /// The collection of requested batches.
        batches: Vec<SealedBatch>,
    },
}
