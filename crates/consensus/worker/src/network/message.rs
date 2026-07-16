//! Messages sent between workers.

use std::collections::BTreeSet;

use crate::network::error::WorkerNetworkError;
use serde::{Deserialize, Serialize};
use tn_network_libp2p::{PeerExchangeMap, TNMessage};
use tn_types::{BlockHash, Epoch, SealedBatch};

/// Worker messages on the gossip network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerGossip {
    /// A new batch digest is available so non-committee peers can fetch it.
    Batch(Epoch, BlockHash),
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
        batch_digests: BTreeSet<BlockHash>,
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
    /// This is an application-layer error response. The requester should treat
    /// it as a permanent rejection and not retry.
    Error(WorkerRPCError),
    /// Retryable RPC error while handling request.
    ///
    /// This is an application-layer error response for a transient,
    /// responder-side condition (a momentary batch-store write failure,
    /// internal channel pressure during an epoch transition). The request is
    /// likely to succeed in the future, so the requester should retry rather
    /// than give up on the peer.
    RecoverableError(WorkerRPCError),
}

impl WorkerResponse {
    /// Helper method if the response is an error (permanent or recoverable).
    pub fn is_err(&self) -> bool {
        matches!(self, WorkerResponse::Error(_) | WorkerResponse::RecoverableError(_))
    }

    /// Classify a [`WorkerNetworkError`] into the response a responder returns to
    /// the requester.
    ///
    /// Transient, responder-side conditions become [`WorkerResponse::RecoverableError`]
    /// so the requester retries; everything else becomes a permanent
    /// [`WorkerResponse::Error`]. This is the type-level distinction between an
    /// explicit rejection and a one-off remote failure: without it, a peer that
    /// hit a momentary internal error would be treated identically to a peer
    /// that deliberately rejected the batch.
    pub(crate) fn into_error_ref(error: &WorkerNetworkError) -> Self {
        match error {
            // transient, responder-side conditions - a retry may clear them
            WorkerNetworkError::Internal(_)
            | WorkerNetworkError::Timeout(_)
            | WorkerNetworkError::Network(_)
            | WorkerNetworkError::StreamClosed
            | WorkerNetworkError::DBInsert(_)
            | WorkerNetworkError::DBCommit(_)
            | WorkerNetworkError::DBRead(_)
            | WorkerNetworkError::StdIo(_) => {
                Self::RecoverableError(WorkerRPCError(error.to_string()))
            }
            // permanent rejections - an identical request would be rejected the
            // same way (invalid batch/request, protocol violation, or a
            // committee/epoch view mismatch for this request)
            WorkerNetworkError::Bcs(_)
            | WorkerNetworkError::BatchValidation(_)
            | WorkerNetworkError::NonCommitteeBatch
            | WorkerNetworkError::InvalidRequest(_)
            | WorkerNetworkError::InvalidTopic
            | WorkerNetworkError::TooManyBatches { .. }
            | WorkerNetworkError::UnexpectedBatch(_)
            | WorkerNetworkError::DuplicateBatch(_)
            | WorkerNetworkError::UnknownStreamRequest(_)
            | WorkerNetworkError::RequestHashMismatch
            | WorkerNetworkError::BatchEpochMismatch(_, _) => {
                Self::Error(WorkerRPCError(error.to_string()))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::{WorkerNetworkError, WorkerResponse};

    /// A transient, responder-side condition is reported as recoverable so the
    /// requester retries instead of treating the peer as having rejected the
    /// batch. Regression coverage for the rejection-versus-transient conflation.
    #[test]
    fn transient_internal_error_is_recoverable() {
        let err = WorkerNetworkError::Internal("failed to write to batch store".to_string());
        assert!(matches!(
            WorkerResponse::into_error_ref(&err),
            WorkerResponse::RecoverableError(_)
        ));
    }

    /// An explicit rejection (reporter is outside the committee) stays a
    /// permanent error so the requester does not pointlessly retry.
    #[test]
    fn non_committee_batch_is_permanent_error() {
        let err = WorkerNetworkError::NonCommitteeBatch;
        assert!(matches!(WorkerResponse::into_error_ref(&err), WorkerResponse::Error(_)));
    }
}
