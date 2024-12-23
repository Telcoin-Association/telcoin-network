//! Inner-node message types between Worker <-> Primary.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tn_types::{BlockHash, NetworkPublicKey};

pub enum InnerNodeRequest {
    /// Request for missing worker blocks.
    FetchBlocks(FetchBlocksRequest),
}

/// Used by the primary to request that the worker fetch the missing blocks and reply
/// with all of the content.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBlocksRequest {
    /// Missing block digests to fetch from peers.
    pub digests: HashSet<BlockHash>,
    /// The network public key of the peers.
    ///
    /// TODO: important to keep this isolated to network layer
    /// - worker needs a block, so just send command to "fetch this block"
    pub known_workers: HashSet<NetworkPublicKey>,
}
