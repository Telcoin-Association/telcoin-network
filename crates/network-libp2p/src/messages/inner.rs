//! Inner-node message types between Worker <-> Primary.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tn_types::{Batch, BlockHash, NetworkPublicKey};

/// Requests between Primary <-> Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum InnerNodeRequest {
    /// Request for missing batches.
    FetchBlocks(FetchBatchesRequest),
}

/// Responses between Primary <-> Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum InnerNodeResponse {
    /// Request for missing batches.
    FetchBlocks(FetchBatchResponse),
}

/// Used by the primary to request that the worker fetch the missing blocks and reply
/// with all of the content.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchBatchesRequest {
    /// Missing block digests to fetch from peers.
    pub digests: HashSet<BlockHash>,
    /// The network public key of the peers.
    ///
    /// TODO: important to keep this isolated to network layer
    /// - worker needs a block, so just send command to "fetch this block"
    pub known_workers: HashSet<NetworkPublicKey>,
}

/// All blocks requested by the primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchBatchResponse {
    /// The missing batches fetched from peers.
    ///
    /// TODO: should this be `SealedBatches`?
    /// Depends on how they're verified - primary should still
    /// verify batches match requested hashes. Could be easier to
    /// match batches to requested hash if they are sealed.
    pub batches: HashMap<BlockHash, Batch>,
}
