//! Notification message types.
//!
//! These messages are passed as unreliable send and
//! don't expect a response.
use serde::{Deserialize, Serialize};
use tn_types::{AuthorityIdentifier, BlockHash, SealedBatch, SealedHeader, WorkerId};

/// Used by the primary to request that the worker sync the target missing batches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSynchronizeMessage {
    /// Batch digests that need to be synchronized from peers.
    pub digests: Vec<BlockHash>,
    /// The peer worker's authority.
    pub target: AuthorityIdentifier,
    /// Used to indicate to the worker that it does not need to fully validate
    /// the batch it receives because it is part of a certificate. Only digest
    /// verification is required.
    pub is_certified: bool,
}

/// Used by worker to inform primary it sealed a new batch.
///
/// Fields are private so the id is stamped once at the sending seam and read-only after:
/// these messages never cross the peer wire, and a mutable id would let any holder re-route
/// a digest to another worker's identity.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerOwnBatchMessage {
    /// The worker's id.
    worker_id: WorkerId,
    /// The digest for the batch that reached quorum.
    digest: BlockHash,
}

impl WorkerOwnBatchMessage {
    /// Create a new message stamped with the sending worker's id.
    pub fn new(worker_id: WorkerId, digest: BlockHash) -> Self {
        Self { worker_id, digest }
    }

    /// The worker's id.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// The digest for the batch that reached quorum.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }
}

/// Used by worker to inform primary it received a batch from another authority.
///
/// Fields are private for the same reason as [`WorkerOwnBatchMessage`].
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerOthersBatchMessage {
    /// The peer worker's batch digest.
    digest: BlockHash,
    /// The worker's id.
    worker_id: WorkerId,
}

impl WorkerOthersBatchMessage {
    /// Create a new message stamped with the receiving worker's id.
    pub fn new(digest: BlockHash, worker_id: WorkerId) -> Self {
        Self { digest, worker_id }
    }

    /// The peer worker's batch digest.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }

    /// The worker's id.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }
}

/// Used by workers to send a new batch to peers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchMessage {
    /// The sending worker's batch.
    pub sealed_batch: SealedBatch,
}

/// Engine to primary when canonical tip is updated.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalUpdateMessage {
    /// The latest execution result.
    pub tip: SealedHeader,
}
