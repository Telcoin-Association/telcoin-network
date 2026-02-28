//! Messages sent between workers.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tn_network_libp2p::{PeerExchangeMap, TNMessage};
use tn_types::{BlockHash, Epoch, SealedBatch};

/// Worker messages on the gossip network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerGossip {
    /// A new is available.
    Batch(BlockHash),
    /// Transaction- published so a committee member can include in a batch.
    Txn(Vec<u8>),
}

// impl TNMessage trait for types
impl TNMessage for WorkerRequest {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange { peers } => Some(peers.clone()),
            _ => None,
        }
    }
}

impl TNMessage for WorkerResponse {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        None
    }
}

/// Requests from Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Send a new batch to a peer.
    ReportBatch {
        /// The sealed batch that this worker is reporting.
        sealed_batch: SealedBatch,
    },
    /// Request batches via stream.
    ///
    /// This initiates a stream-based batch transfer. The responder will
    /// return `WorkerResponse::RequestBatchesStream` with acceptance status.
    /// If accepted, the requestor opens a stream with the request digest
    /// in the header for correlation.
    RequestBatchesStream {
        /// The batch digests being requested.
        batch_digests: HashSet<BlockHash>,
        /// The epoch these batches were produced.
        epoch: Epoch,
    },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange {
        /// The peer information being exchanged.
        peers: PeerExchangeMap,
    },
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
    /// Response to stream-based batch request.
    ///
    /// If `ack` is true, the requestor should open a stream with the
    /// request digest in the header. The responder will send batches
    /// over that stream.
    RequestBatchesStream {
        /// Whether the request is accepted.
        ack: bool,
    },
    /// Exchange peer information.
    PeerExchange {
        /// The peer information being exchanged.
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
