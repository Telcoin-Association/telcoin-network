//! Worker's network-related errors.
use tn_network_libp2p::{error::NetworkError, Penalty};
use tn_types::{BatchValidationError, BcsError, BlockHash, Epoch};
use tokio::time::error::Elapsed;

/// Result alias for results that possibly return [`WorkerNetworkError`].
pub(crate) type WorkerNetworkResult<T> = Result<T, WorkerNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum WorkerNetworkError {
    /// Serialization error from BCS.
    #[error("BCS error: {0}")]
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
    /// Error while inserting into the DB.
    #[error("DB Insert Error {0}")]
    DBInsert(String),
    /// Error committing to DB.
    #[error("DB Commit Error {0}")]
    DBCommit(String),
    /// Error reading from DB.
    #[error("DB read Error {0}")]
    DBRead(String),
    /// Invalid batch epoch.
    #[error("Got batch from epoch {0} expected {1}")]
    BatchEpochMismatch(Epoch, Epoch),
}

impl WorkerNetworkError {
    /// Return the penalty for this error if it causes one (None if no penalty).
    pub fn penalty(&self) -> Option<Penalty> {
        //
        // explicitly match every error type to ensure penalties are updated with changes
        //
        match self {
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
            WorkerNetworkError::InvalidRequest(_) | WorkerNetworkError::UnknownStreamRequest(_) => {
                Some(Penalty::Mild)
            }
            WorkerNetworkError::StdIo(ref io_err) => {
                // separate legitimate failures like connection resets from suspicious behavior
                match io_err.kind() {
                    std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted => Some(Penalty::Mild),
                    _ => Some(Penalty::Medium),
                }
            }
            // may occur at epoch boundaries
            WorkerNetworkError::NonCommitteeBatch => Some(Penalty::Medium),
            // protocol violations - fatal penalty
            WorkerNetworkError::InvalidTopic
            | WorkerNetworkError::Bcs(_)
            | WorkerNetworkError::TooManyBatches { .. }
            | WorkerNetworkError::UnexpectedBatch(_)
            | WorkerNetworkError::DuplicateBatch(_)
            | WorkerNetworkError::RequestHashMismatch => Some(Penalty::Fatal),
            // ignore
            WorkerNetworkError::Timeout(_)
            | WorkerNetworkError::DBInsert(_)
            | WorkerNetworkError::DBCommit(_)
            | WorkerNetworkError::DBRead(_)
            | WorkerNetworkError::StreamClosed
            | WorkerNetworkError::Network(_)
            | WorkerNetworkError::BatchEpochMismatch(_, _)
            | WorkerNetworkError::Internal(_) => None,
        }
    }

    /// Whether this fault is determined purely by the *content* of a message - its
    /// encoding, declared topic, identity, or count - and is therefore attributable to
    /// whoever authored that content rather than to the peer that delivered it.
    ///
    /// This distinction matters for gossip: the peer in hand is the relayer
    /// (`propagation_source`), not the author, so an author-content fault must not
    /// penalize it (see issue #801). Direct request/response paths penalize
    /// unconditionally because there the peer *is* the originator of the content.
    ///
    /// These are the protocol-violation group that [`Self::penalty`] maps to
    /// [`Penalty::Fatal`] (faults in a message's encoding, declared topic, identity, or
    /// count). [`Self::BatchValidation`] is also `Fatal` for some subvariants but is
    /// deliberately excluded: it arises only on the request/response path, where the
    /// peer *is* the originator and penalizing it is correct. Of this set, only
    /// [`Self::Bcs`] and [`Self::InvalidTopic`] are currently reachable from the gossip
    /// handler; the rest are classified for forward-safety.
    pub fn is_author_content_fault(&self) -> bool {
        //
        // explicitly match every error type so the classification is revisited with changes
        //
        match self {
            // content-determined protocol violations
            WorkerNetworkError::Bcs(_)
            | WorkerNetworkError::InvalidTopic
            | WorkerNetworkError::TooManyBatches { .. }
            | WorkerNetworkError::UnexpectedBatch(_)
            | WorkerNetworkError::DuplicateBatch(_)
            | WorkerNetworkError::RequestHashMismatch => true,
            // Not author-content faults on any current path. `BatchValidation` is
            // content-determined, but only arises on the request/response path (where
            // the peer is the originator, so its own penalty is correct) and never
            // reaches the gossip handler; the rest are transport-peer behavior,
            // timeouts, or faults local to this node.
            WorkerNetworkError::BatchValidation(_)
            | WorkerNetworkError::NonCommitteeBatch
            | WorkerNetworkError::Internal(_)
            | WorkerNetworkError::Timeout(_)
            | WorkerNetworkError::Network(_)
            | WorkerNetworkError::InvalidRequest(_)
            | WorkerNetworkError::StreamClosed
            | WorkerNetworkError::UnknownStreamRequest(_)
            | WorkerNetworkError::StdIo(_)
            | WorkerNetworkError::DBInsert(_)
            | WorkerNetworkError::DBCommit(_)
            | WorkerNetworkError::DBRead(_)
            | WorkerNetworkError::BatchEpochMismatch(_, _) => false,
        }
    }
}

impl From<WorkerNetworkError> for Option<Penalty> {
    fn from(val: WorkerNetworkError) -> Self {
        val.penalty()
    }
}
