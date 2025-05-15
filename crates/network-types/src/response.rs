//! Response message types
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tn_types::{Batch, BlockHash, Certificate};

/// Used by the primary to reply to FetchCertificatesRequest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchCertificatesResponse {
    /// Certificates sorted from lower to higher rounds.
    pub certificates: Vec<Certificate>,
}

/// All batches requested by the primary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchResponse {
    /// The missing batches fetched from peers.
    pub batches: HashMap<BlockHash, Batch>,
}

//=== Engine
