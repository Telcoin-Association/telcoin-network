//! Error types for the subscriber component.
//!
//! The subscriber is responsible for receiving sequenced consensus output, fetching
//! transaction batches from workers, and forwarding complete data to the execution layer.
//! These errors represent the various failure modes that can occur during this process.

use std::fmt::Debug;
use thiserror::Error;
use tn_storage::StoreError;
use tn_types::{AuthorityIdentifier, BlockHash, CertificateDigest, WorkerId};

/// Returns an error if the given condition is false.
///
/// This is a convenience macro for early-return error checking, similar to `assert!`
/// but for `Result` types.
///
/// # Examples
///
/// ```ignore
/// ensure!(value > 0, SubscriberError::ClientExecutionError("Value must be positive".into()));
/// ```
#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

/// Result type alias for subscriber operations.
///
/// All subscriber methods that can fail return this type, with errors represented
/// by [`SubscriberError`].
pub type SubscriberResult<T> = Result<T, SubscriberError>;

/// Errors that can occur during subscriber operations.
///
/// The subscriber handles consensus output by fetching transaction batches and forwarding
/// them to the execution layer. These errors represent failures in communication channels,
/// storage operations, network requests, and protocol violations.
#[derive(Debug, Error)]
pub enum SubscriberError {
    /// A communication channel closed unexpectedly.
    ///
    /// This typically indicates a critical component shutdown or panic. The channel name
    /// is included to help identify which component failed (e.g., "consensus_output",
    /// "consensus_header").
    ///
    /// This is often a fatal error that triggers node shutdown.
    #[error("channel {0} closed unexpectedly")]
    ClosedChannel(String),

    /// An error occurred while reading from or writing to persistent storage.
    ///
    /// This wraps underlying database errors and may occur when saving consensus output,
    /// reading certificates, or managing the consensus chain state.
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    /// Failed to retrieve the payload (transaction batches) for a certificate.
    ///
    /// This occurs when the subscriber cannot fetch the transaction data referenced by
    /// a certificate, which prevents the certificate from being executed. The certificate
    /// digest and error details are included for debugging.
    #[error("Error occurred while retrieving certificate {0} payload: {1}")]
    PayloadRetrieveError(CertificateDigest, String),

    /// Consensus output referenced a worker ID that doesn't exist in the committee.
    ///
    /// This is a protocol violation indicating either corrupted consensus output or
    /// a mismatch in committee configuration. Should not occur in normal operation.
    #[error("Consensus referenced unexpected worker id {0}")]
    UnexpectedWorkerId(WorkerId),

    /// Consensus output referenced an authority not in the current committee.
    ///
    /// This is a protocol violation that occurs when consensus includes a certificate
    /// from an authority that isn't part of the epoch's committee. This should not
    /// happen in correct operation and may indicate consensus corruption.
    #[error("Consensus referenced unexpected authority {0}")]
    UnexpectedAuthority(AuthorityIdentifier),

    /// The connection to the transaction execution engine was lost.
    ///
    /// This indicates the executor component has stopped or crashed, preventing
    /// further block execution.
    #[error("Connection with the transaction executor dropped")]
    ExecutorConnectionDropped,

    /// Failed to deserialize a consensus message.
    ///
    /// This may indicate corrupted data, version mismatch, or incompatible protocol
    /// changes between nodes.
    #[error("Deserialization of consensus message failed: {0}")]
    SerializationError(String),

    /// Received a protocol message that was not expected in the current context.
    ///
    /// This indicates a protocol violation or bug in the consensus implementation.
    #[error("Received unexpected protocol message from consensus")]
    UnexpectedProtocolMessage,

    /// Attempted to create a second consensus client when one already exists.
    ///
    /// The subscriber enforces that only a single consensus client can be active
    /// at any time to prevent conflicts and ensure deterministic ordering.
    #[error("There can only be a single consensus client at the time")]
    OnlyOneConsensusClientPermitted,

    /// The execution engine encountered a fatal internal error.
    ///
    /// This represents failures within the execution layer itself (e.g., EVM errors,
    /// state transition failures) rather than data availability or network issues.
    #[error("Execution engine failed: {0}")]
    NodeExecutionError(String),

    /// A client-submitted transaction was invalid and could not be executed.
    ///
    /// This represents validation failures for user transactions (e.g., insufficient
    /// balance, invalid signature, nonce issues).
    #[error("Client transaction invalid: {0}")]
    ClientExecutionError(String),

    /// All attempts to fetch data from peer workers have failed.
    ///
    /// This occurs when the subscriber cannot retrieve transaction batches from any
    /// worker nodes, possibly due to network partitions or widespread worker failures.
    /// This is a critical error that prevents consensus output execution.
    #[error("Attempts to query all peers has failed")]
    ClientRequestsFailed,

    /// A batch that should have been fetched is missing from the collection.
    ///
    /// This is a protocol violation that occurs when consensus references a batch
    /// but it's not present in the fetched results. This should not happen if workers
    /// are behaving correctly and may indicate either a worker bug or data corruption.
    #[error("A fetched batch is missing from the collection.")]
    MissingFetchedBatch(BlockHash),
}
