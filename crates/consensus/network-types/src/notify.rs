//! Notification message types.
//!
//! These messages are passed as unreliable send and
//! don't expect a response.
use serde::{Deserialize, Serialize};
use tn_types::{AuthorityIdentifier, Batch, BatchDigest, VersionedMetadata, WorkerId};

/// Used by the primary to request that the worker sync the target missing batches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSynchronizeMessage {
    /// Batch digests that need to be synchronized from peers.
    pub digests: Vec<BatchDigest>,
    /// The peer worker's authority.
    pub target: AuthorityIdentifier,
    // Used to indicate to the worker that it does not need to fully validate
    // the batch it receives because it is part of a certificate. Only digest
    // verification is required.
    pub is_certified: bool,
}

/// Used by worker to inform primary it sealed a new batch.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerOwnBatchMessage {
    /// The worker's batch digest.
    pub digest: BatchDigest,
    /// The worker's id.
    pub worker_id: WorkerId,
    /// The metadata for the sealed batch.
    pub metadata: VersionedMetadata,
}

/// Used by worker to inform primary it received a batch from another authority.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerOthersBatchMessage {
    /// The peer worker's batch digest.
    pub digest: BatchDigest,
    /// The worker's id.
    pub worker_id: WorkerId,
}

/// Used by workers to send a new batch to peers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerBatchMessage {
    /// The sending worker's batch.
    pub batch: Batch,
}
