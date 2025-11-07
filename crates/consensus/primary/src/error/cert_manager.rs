//! Errors encountered during long-running certificate manager task.

use tn_network_libp2p::error::NetworkError;
use tn_storage::StoreError;
use tn_types::{error::CertificateError, CertificateDigest, SendError};

use super::GarbageCollectorError;

/// Result alias for results that possibly return [`CertManagerError`].
pub(crate) type CertManagerResult<T> = Result<T, CertManagerError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CertManagerError {
    /// Error processing certificate.
    #[error(transparent)]
    Certificate(#[from] CertificateError),
    /// Error from garbage collection task.
    #[error(transparent)]
    GC(#[from] GarbageCollectorError),
    /// The certificate's signature verification state is unverified.
    #[error("Unverified signature verification state {0}")]
    UnverifiedSignature(CertificateDigest),
    /// Oneshot channel dropped for certificate manager.
    #[error("Failed to return certificate manager's result.")]
    CertificateManagerOneshot,
    /// The pending certificate is unexpectedly missing. This should not happen.
    #[error("Pending certificate not found by digest: {0}")]
    PendingCertificateNotFound(CertificateDigest),
    /// The certificate was verified, accepted, and stored in storage.
    /// However, an error occurred adding it to the collection of parents.
    /// This is the only way to advance the round and is fatal.
    #[error("Fatal error: failed to append accepted certs to parents.")]
    FatalAppendParent,
    /// The certificate was verified, accepted, and stored in storage.
    /// However, an error occured forwarding the certificate to bullshark consensus.
    /// This results in inconsistent state between consensus DAG and consensus store and is fatal.
    #[error("Fatal error: failed to forward accepted cert to consensus.")]
    FatalForwardAcceptedCertificate,
    /// JoinError for spawned blocking task when verifying many fetched certs.
    #[error("Failed to join blocking certificate verification task.")]
    JoinError,
    /// The certificate is pending acceptance due to missing parents.
    #[error("The certificate {0} is pending acceptance due to missing parents.")]
    Pending(CertificateDigest),
    /// Error retrieving value from storage.
    #[error("Storage failure: {0}")]
    Storage(#[from] StoreError),
    /// A duplicate certificate was received but it has different missing parents.
    #[error("The certificate {0} was already pending, but now it has different missing parents.")]
    PendingParentsMismatch(CertificateDigest),

    /// mpsc sender dropped while processig the certificate
    #[error("Failed to process certificate - TN sender error: {0}")]
    TNSend(String),
    /// Fetch certificates failed.
    #[error("No peer can be reached for fetching certificates! Check if network is healthy.")]
    NoCertificateFetched,
    /// Failed to set the request bounds (in bytes).
    #[error("Failed to set the bounds for MissingCertificatesRequest: {0}")]
    RequestBounds(String),
    /// Network error while fetching certificates from peers.
    #[error(transparent)]
    Network(#[from] NetworkError),
    /// Timeout waiting for response from peer for requested certificates.
    #[error("Timeout waiting for requested certs from all peers")]
    Timeout,
}

impl<T: std::fmt::Debug> From<SendError<T>> for CertManagerError {
    fn from(e: SendError<T>) -> Self {
        Self::TNSend(e.to_string())
    }
}
