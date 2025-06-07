use tn_network_libp2p::{error::NetworkError, Penalty};
use tn_types::{BatchValidationError, BcsError};
use tokio::time::error::Elapsed;

/// Result alias for results that possibly return [`WorkerNetworkError`].
pub type WorkerNetworkResult<T> = Result<T, WorkerNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerNetworkError {
    /// Error decoding with bcs.
    #[error("Failed to decode gossip message: {0}")]
    Decode(#[from] BcsError),
    /// Batch validation error occured.
    #[error("Failed batch validation: {0}")]
    BatchValidation(#[from] BatchValidationError),
    /// Internal error occurred.
    #[error("Internal error: {0}")]
    Internal(String),
    /// A network request timed out.
    #[error("Network request timed out")]
    Timeout(#[from] Elapsed),
    // Network error.
    #[error("Network error occured: {0}")]
    Network(#[from] NetworkError),
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
                    // severe
                    BatchValidationError::RecoverTransaction(_, _) => Some(Penalty::Severe),
                    // fatal
                    BatchValidationError::EmptyBatch
                    | BatchValidationError::InvalidBaseFee { .. }
                    | BatchValidationError::InvalidWorkerId { .. }
                    | BatchValidationError::InvalidDigest
                    | BatchValidationError::TimestampIsInPast { .. }
                    | BatchValidationError::CalculateMaxPossibleGas
                    | BatchValidationError::HeaderMaxGasExceedsGasLimit { .. }
                    | BatchValidationError::HeaderTransactionBytesExceedsMax(_) => {
                        Some(Penalty::Fatal)
                    }
                }
            }
            // fatal
            WorkerNetworkError::Decode(_) => Some(Penalty::Fatal),
            // ignore
            WorkerNetworkError::Timeout(_)
            | WorkerNetworkError::Network(_)
            | WorkerNetworkError::Internal(_) => None,
        }
    }
}
