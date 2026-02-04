//! Worker's network-related errors.
use tn_network_libp2p::{error::NetworkError, Penalty};
use tn_types::{BatchValidationError, BcsError, BlockHash};
use tokio::time::error::Elapsed;

/// Result alias for results that possibly return [`WorkerNetworkError`].
pub(crate) type WorkerNetworkResult<T> = Result<T, WorkerNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum WorkerNetworkError {
    /// Error decoding with bcs. (gossipsub, and stream)
    #[error("BCS encode/decode error: {0}")]
    Bcs(#[from] BcsError),
    /// Batch validation error occured.
    #[error("Failed batch validation: {0}")]
    BatchValidation(#[from] BatchValidationError),
    /// The batch reporter is not in the current committee.
    #[error("Batch reported from outside committee.")]
    NonCommitteeBatch,
    /// Internal error occurred.
    #[error("Internal error: {0}")]
    Internal(String),
    /// A network request timed out.
    #[error("Network request timed out")]
    Timeout(#[from] Elapsed),
    /// Network error.
    #[error("Network error occured: {0}")]
    Network(#[from] NetworkError),
    /// The peer's request is invalid.
    #[error("{0}")]
    InvalidRequest(String),
    /// Invalid topic- something was published to the wrong topic.
    #[error("Gossip was published to the wrong topic")]
    InvalidTopic,
    /// Peer sent more batches than expected.
    #[error("Peer sent too many batches: expected {expected}, received {received}")]
    TooManyBatches {
        /// The expected number of batches.
        expected: usize,
        /// The number of batches received.
        received: usize,
    },
    /// Peer sent a batch we didn't request.
    #[error("Received unexpected batch with digest {0}")]
    UnexpectedBatch(BlockHash),
    /// Peer sent duplicate batch.
    #[error("Received duplicate batch with digest {0}")]
    DuplicateBatch(BlockHash),
    /// Stream was closed unexpectedly.
    #[error("Stream closed unexpectedly")]
    StreamClosed,
    /// No matching pending request for inbound stream.
    #[error("No pending request matches stream hash")]
    UnknownStreamRequest(BlockHash),
    /// Request hash mismatch between negotiation and stream.
    #[error("Request hash mismatch")]
    RequestHashMismatch,
    /// Error conversion from [std::io::Error]
    #[error(transparent)]
    StdIo(#[from] std::io::Error),
}

impl From<WorkerNetworkError> for Option<Penalty> {
    fn from(val: WorkerNetworkError) -> Self {
        //
        // explicitly match every error type to ensure penalties are updated with changes
        //
        match val {
            WorkerNetworkError::BatchValidation(batch_validation_error) => {
                match batch_validation_error {
                    // mild
                    BatchValidationError::CanonicalChain { .. } => Some(Penalty::Mild),
                    // medium
                    BatchValidationError::InvalidEpoch { .. }
                    | BatchValidationError::InvalidTx4844(_) => Some(Penalty::Medium),
                    // severe
                    BatchValidationError::RecoverTransaction(_, _) => Some(Penalty::Severe),
                    // fatal
                    BatchValidationError::EmptyBatch
                    | BatchValidationError::InvalidBaseFee { .. }
                    | BatchValidationError::InvalidWorkerId { .. }
                    | BatchValidationError::InvalidDigest
                    | BatchValidationError::GasOverflow
                    | BatchValidationError::CalculateMaxPossibleGas
                    | BatchValidationError::HeaderMaxGasExceedsGasLimit { .. }
                    | BatchValidationError::HeaderTransactionBytesExceedsMax(_) => {
                        Some(Penalty::Fatal)
                    }
                }
            }
            WorkerNetworkError::InvalidRequest(_) => Some(Penalty::Mild),
            // may occur at epoch boundaries
            WorkerNetworkError::NonCommitteeBatch | WorkerNetworkError::StdIo(_) => {
                Some(Penalty::Medium)
            }
            // protocol violations - fatal penalty
            WorkerNetworkError::InvalidTopic
            | WorkerNetworkError::Bcs(_)
            | WorkerNetworkError::TooManyBatches { .. }
            | WorkerNetworkError::UnexpectedBatch(_)
            | WorkerNetworkError::DuplicateBatch(_)
            | WorkerNetworkError::UnknownStreamRequest(_)
            | WorkerNetworkError::RequestHashMismatch => Some(Penalty::Fatal),
            // ignore
            WorkerNetworkError::Timeout(_)
            | WorkerNetworkError::StreamClosed
            | WorkerNetworkError::Network(_)
            | WorkerNetworkError::Internal(_) => None,
        }
    }
}
