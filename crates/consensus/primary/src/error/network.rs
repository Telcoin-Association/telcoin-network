//! Error types for primary's network task.

use tn_storage::StoreError;
use tn_types::{
    error::{CertificateError, HeaderError},
    BcsError, BlockHash,
};

/// Result alias for results that possibly return [`PrimaryNetworkError`].
pub(crate) type PrimaryNetworkResult<T> = Result<T, PrimaryNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrimaryNetworkError {
    /// Error while processing a peer's request for vote.
    #[error("Error processing header vote request: {0}")]
    InvalidHeader(#[from] HeaderError),
    /// Error decoding with bcs.
    #[error("Failed to decode gossip message: {0}")]
    Decode(#[from] BcsError),
    /// Error processing certificate.
    #[error("Failed to process certificate: {0}")]
    Certificate(#[from] CertificateError),
    /// Error conversion from [std::io::Error]
    #[error(transparent)]
    StdIo(#[from] std::io::Error),
    /// Error retrieving value from storage.
    #[error("Storage failure: {0}")]
    Storage(#[from] StoreError),
    /// The peer's request is invalid.
    #[error("{0}")]
    InvalidRequest(String),
    /// Internal error occurred.
    #[error("Internal error: {0}")]
    Internal(String),
    /// Unknown consensus header.
    #[error("Unknown consensus header: {0}")]
    UnknowConsensusHeaderNumber(u64),
    /// Unknown consensus header.
    #[error("Unknown consensus header: {0}")]
    UnknowConsensusHeaderDigest(BlockHash),
}
