//! Error types for primary's network task.

use super::CertManagerError;
use tn_network_libp2p::Penalty;
use tn_storage::StoreError;
use tn_types::{
    error::{CertificateError, HeaderError},
    BcsError, BlockHash,
};

/// Result alias for results that possibly return [`PrimaryNetworkError`].
pub type PrimaryNetworkResult<T> = Result<T, PrimaryNetworkError>;

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
    Certificate(#[from] CertManagerError),
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

impl From<PrimaryNetworkError> for Option<Penalty> {
    fn from(val: PrimaryNetworkError) -> Self {
        //
        // explicitly match every error type to ensure penalties are updated with changes
        //
        match val {
            PrimaryNetworkError::InvalidHeader(header_error) => {
                penalty_from_header_error(header_error)
            }
            PrimaryNetworkError::Certificate(e) => match e {
                CertManagerError::Certificate(certificate_error) => match certificate_error {
                    CertificateError::Header(header_error) => {
                        penalty_from_header_error(header_error)
                    }
                    // mild
                    CertificateError::TooOld(_, _, _) => Some(Penalty::Mild),
                    // fatal
                    CertificateError::RecoverBlsAggregateSignatureBytes
                    | CertificateError::Unsigned
                    | CertificateError::Inquorate { .. }
                    | CertificateError::InvalidSignature => Some(Penalty::Fatal),
                    // ignore
                    CertificateError::ResChannelClosed(_)
                    | CertificateError::TooNew(_, _, _)
                    | CertificateError::Storage(_) => None,
                },
                // severe
                CertManagerError::TooManyFetchedCertificatesReturned { .. } => {
                    Some(Penalty::Severe)
                }
                // fatal
                CertManagerError::UnverifiedSignature(_) => Some(Penalty::Fatal),
                // ignore
                CertManagerError::PendingCertificateNotFound(_)
                | CertManagerError::PendingParentsMismatch(_)
                | CertManagerError::CertificateManagerOneshot
                | CertManagerError::FatalForwardAcceptedCertificate
                | CertManagerError::NoCertificateFetched
                | CertManagerError::FatalAppendParent
                | CertManagerError::GC(_)
                | CertManagerError::JoinError
                | CertManagerError::Pending(_)
                | CertManagerError::Storage(_)
                | CertManagerError::RequestBounds(_)
                | CertManagerError::TNSend(_) => None,
            },
            // mild
            PrimaryNetworkError::UnknowConsensusHeaderNumber(_)
            | PrimaryNetworkError::InvalidRequest(_)
            | PrimaryNetworkError::UnknowConsensusHeaderDigest(_) => Some(Penalty::Mild),
            // medium
            PrimaryNetworkError::StdIo(_) => Some(Penalty::Medium),
            // fatal
            PrimaryNetworkError::Decode(_) => Some(Penalty::Fatal),
            // ignore
            PrimaryNetworkError::Storage(_) | PrimaryNetworkError::Internal(_) => None,
        }
    }
}

/// Helper function to convert `HeaderError` to `Penalty`.
///
/// Header errors are responsible for more than one PrimaryNetworkHandle.
fn penalty_from_header_error(error: HeaderError) -> Option<Penalty> {
    match error {
        // mild
        HeaderError::SyncBatches(_)
        | HeaderError::Storage(_)
        | HeaderError::UnknownExecutionResult(_) => Some(Penalty::Mild),
        // medium
        HeaderError::TooOld { .. } => Some(Penalty::Medium),
        // severe
        HeaderError::InvalidTimestamp { .. } | HeaderError::InvalidParentRound => {
            Some(Penalty::Severe)
        }
        // fatal
        HeaderError::AlreadyVotedForLaterRound { .. }
        | HeaderError::AlreadyVoted(_, _)
        | HeaderError::DuplicateParents
        | HeaderError::TooManyParents(_, _)
        | HeaderError::UnknownNetworkKey(_)
        | HeaderError::PeerNotAuthor
        | HeaderError::InvalidGenesisParent(_)
        | HeaderError::ParentMissingSignature
        | HeaderError::InvalidParentTimestamp { .. }
        | HeaderError::UnkownWorkerId
        | HeaderError::InvalidEpoch { .. }
        | HeaderError::InvalidHeaderDigest
        | HeaderError::UnknownAuthority(_) => Some(Penalty::Fatal),
        // ignore
        HeaderError::PendingCertificateOneshot
        | HeaderError::TNSend(_)
        | HeaderError::ClosedWatchChannel => None,
    }
}
