//! Messages sent between workers.

use serde::{Deserialize, Serialize};
use tn_network_libp2p::{PeerExchangeMap, TNMessage};
use tn_types::{Batch, BlockHash, SealedBatch};

/// Worker messages on the gossip network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerGossip {
    /// A new is available.
    Batch(BlockHash),
    /// Transaction- published so a committee member can include in a batch.
    Txn(Vec<u8>),
}

// impl TNMessage trait for types
impl TNMessage for WorkerRequest {}
impl TNMessage for WorkerResponse {}

/// Requests from Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Send a new batch to a peer.
    ReportBatch { sealed_batch: SealedBatch },
    /// Request batches by digest from a peer.
    RequestBatches { batch_digests: Vec<BlockHash> },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange { peers: PeerExchangeMap },
}

impl From<PeerExchangeMap> for WorkerRequest {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}

//
//
//=== Response types
//
//

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    ReportBatch,
    RequestBatches(Vec<Batch>),
    /// Exchange peer information.
    PeerExchange {
        peers: PeerExchangeMap,
    },
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(WorkerRPCError),
}

impl WorkerResponse {
    /// Helper method if the response is an error.
    pub fn is_err(&self) -> bool {
        matches!(self, WorkerResponse::Error(_))
    }
}

impl From<WorkerRPCError> for WorkerResponse {
    fn from(value: WorkerRPCError) -> Self {
        Self::Error(value)
    }
}

/// Application-specific error type while handling Worker request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkerRPCError(pub String);

impl From<PeerExchangeMap> for WorkerResponse {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}
