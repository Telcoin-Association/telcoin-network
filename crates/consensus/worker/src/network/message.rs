//! Messages sent between workers.

use serde::{Deserialize, Serialize};
use tn_network_libp2p::{
    types::{NodeRecord, NodeRecordRequest, NodeRecordResponse},
    PeerExchangeMap, TNMessage,
};
use tn_types::{Batch, BlockHash, BlsPublicKey, SealedBatch};

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
impl NodeRecordRequest for WorkerRequest {
    fn node_record_request() -> Self {
        Self::NodeRecord
    }
    fn is_node_record_request(&self) -> bool {
        matches!(self, WorkerRequest::NodeRecord)
    }
}
impl NodeRecordResponse for WorkerResponse {
    fn from_record(record: NodeRecord) -> Self {
        Self::NodeRecord(record)
    }

    fn is_node_record_response(&self) -> bool {
        matches!(self, WorkerResponse::NodeRecord(_))
    }

    fn into_parts(self) -> Option<(BlsPublicKey, NodeRecord)> {
        match self {
            Self::NodeRecord { key, record } => Some((key, record)),
            _ => None,
        }
    }
}

/// Requests from Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Send a new batch to a peer.
    ReportBatch { sealed_batch: SealedBatch },
    /// Request batches by digest from a peer.
    RequestBatches { batch_digests: Vec<BlockHash>, max_response_size: usize },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange { peers: PeerExchangeMap },
    /// Request the peer's [NodeRecord]
    NodeRecord,
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
    /// Status 200 response when a peer accepts a proposed batch.
    ReportBatch,
    /// Provided the requested batches.
    RequestBatches(Vec<Batch>),
    /// Exchange peer information.
    PeerExchange { peers: PeerExchangeMap },
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(WorkerRPCError),
    /// This node's signed [NodeRecord].
    NodeRecord {
        /// The public key used to verify the record's signature.
        key: BlsPublicKey,
        /// The signed record.
        record: NodeRecord,
    },
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
