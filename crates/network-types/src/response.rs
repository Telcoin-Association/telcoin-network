//! Response message types
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tn_types::{Batch, BlockHash};

/// All batches requested by the primary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchResponse {
    /// The missing batches fetched from peers.
    pub batches: HashMap<BlockHash, Batch>,
}
