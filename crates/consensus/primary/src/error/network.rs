//! Error types for primary's network task.

use super::CertManagerError;
use tn_network_libp2p::Penalty;
use tn_storage::{consensus::ConsensusChainError, StoreError};
use tn_types::{
    error::{CertificateError, HeaderError},
    BcsError, BlockHash, BlsPublicKey, ConsensusHeaderDigest, Epoch, EpochDigest,
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
    /// Unknown consensus header certificate.
    #[error("Unknown consensus header certificate for: {0}")]
    UnknownConsensusHeaderCert(ConsensusHeaderDigest),
    /// Unknown consensus output (raw bytes) for the requested number.
    #[error("Unknown consensus output: {0}")]
    UnknownConsensusOutput(u64),
    /// Peer that is not committee published invalid gosip.
    /// Temparily disabled, will be back soon.
    #[error("Peer {0} is not in the committee!")]
    PeerNotInCommittee(Box<BlsPublicKey>),
    /// Unavaliable epoch (either it is invalid or this node does not have it).
    #[error("Unknown epoch record: {0}")]
    UnavailableEpoch(Epoch),
    /// Unavaliable epoch hash (either it is invalid or this node does not have it).
    #[error("Unknown epoch record digest: {0}")]
    UnavailableEpochDigest(EpochDigest),
    /// Invalid epoch request.
    #[error("Must suply an epoch or hash when requesting an epoch record")]
    InvalidEpochRequest,
    /// Invalid topic- something was published to the wrong topic.
    #[error("Gossip was published to the wrong topic")]
    InvalidTopic,
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    /// No matching pending request for inbound stream.
    #[error("No pending request matches stream hash")]
    UnknownStreamRequest(BlockHash),
    /// A requested pack file stream is not available.
    #[error("Pack file is not available to stream for epoch {0}")]
    StreamUnavailable(Epoch),
    /// Pass through from the consesus chain.
    #[error("ConsensusChainError {0}")]
    ConsensusChainError(ConsensusChainError),
}

impl PrimaryNetworkError {
    /// Whether this fault is determined purely by the *content* of a gossip message — its BCS
    /// encoding or its declared topic — and is therefore attributable to the message author
    /// (`GossipMessage::source`) rather than the peer that relayed it.
    ///
    /// Mirrors `WorkerNetworkError::is_author_content_fault` (tn-worker) and is consulted only on
    /// the gossip path (`PrimaryNetwork::process_gossip`): the network layer accepted and forwarded
    /// the message after a shallow check, so an honest relayer could not have screened a
    /// content-determined fault, and charging the relayer for it lets a Byzantine author ban honest
    /// forwarders (see issues #801/#819). The request/response paths penalize the peer
    /// unconditionally because there the peer *is* the originator.
    ///
    /// Restricted to the two `Fatal` faults authored by the gossip source and reachable from the
    /// gossip handler: [`Self::Decode`] (`try_decode` of the payload) and [`Self::InvalidTopic`]
    /// (the payload variant was published to the wrong topic). Every other variant is either
    /// benign on the gossip path or concerns embedded certificate/consensus content whose signer
    /// is carried inside the payload and is not necessarily the gossip source, so its existing
    /// relayer/no-penalty attribution is left unchanged.
    pub(crate) fn is_author_content_fault(&self) -> bool {
        //
        // explicitly match every variant so the classification is revisited with changes
        //
        match self {
            PrimaryNetworkError::Decode(_) | PrimaryNetworkError::InvalidTopic => true,
            PrimaryNetworkError::InvalidHeader(_)
            | PrimaryNetworkError::Certificate(_)
            | PrimaryNetworkError::StdIo(_)
            | PrimaryNetworkError::Storage(_)
            | PrimaryNetworkError::InvalidRequest(_)
            | PrimaryNetworkError::Internal(_)
            | PrimaryNetworkError::UnknownConsensusHeaderCert(_)
            | PrimaryNetworkError::UnknownConsensusOutput(_)
            | PrimaryNetworkError::PeerNotInCommittee(_)
            | PrimaryNetworkError::UnavailableEpoch(_)
            | PrimaryNetworkError::UnavailableEpochDigest(_)
            | PrimaryNetworkError::InvalidEpochRequest
            | PrimaryNetworkError::Timeout(_)
            | PrimaryNetworkError::UnknownStreamRequest(_)
            | PrimaryNetworkError::StreamUnavailable(_)
            | PrimaryNetworkError::ConsensusChainError(_) => false,
        }
    }
}

impl From<&PrimaryNetworkError> for Option<Penalty> {
    fn from(val: &PrimaryNetworkError) -> Self {
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
                | CertManagerError::Timeout
                | CertManagerError::Network(_)
                | CertManagerError::ChannelClosed
                | CertManagerError::TNSend(_) => None,
            },
            // Benign "miss": observers legitimately request not-yet-served headers/outputs.
            // No penalty so honest sync flows are not banned during catch-up.
            PrimaryNetworkError::UnknownConsensusOutput(_) => None,
            PrimaryNetworkError::InvalidRequest(_)
            | PrimaryNetworkError::UnknownStreamRequest(_)
            | PrimaryNetworkError::UnknownConsensusHeaderCert(_) => Some(Penalty::Mild),
            PrimaryNetworkError::InvalidEpochRequest
            | PrimaryNetworkError::StdIo(_) => Some(Penalty::Medium),
            PrimaryNetworkError::InvalidTopic
            | PrimaryNetworkError::Decode(_) => Some(Penalty::Fatal),
            PrimaryNetworkError::UnavailableEpoch(_)  // A node might not have this yet...
            | PrimaryNetworkError::UnavailableEpochDigest(_)  // A node might not have this yet....
            | PrimaryNetworkError::PeerNotInCommittee(_)
            | PrimaryNetworkError::Storage(_)
            | PrimaryNetworkError::Timeout(_)
            | PrimaryNetworkError::StreamUnavailable(_)
            | PrimaryNetworkError::ConsensusChainError(_)
            | PrimaryNetworkError::Internal(_) => None,
        }
    }
}

/// Helper function to convert `HeaderError` to `Penalty`.
///
/// Header errors are responsible for more than one PrimaryNetworkHandle.
fn penalty_from_header_error(error: &HeaderError) -> Option<Penalty> {
    match error {
        // mild
        HeaderError::SyncBatches(_) | HeaderError::TooNew { .. } => Some(Penalty::Mild),
        // medium
        HeaderError::InvalidParents
        | HeaderError::WrongNumberOfParents(_, _)
        | HeaderError::TooOld { .. } => Some(Penalty::Medium),
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
        | HeaderError::InvalidRound(_)
        | HeaderError::ParentMissingSignature
        | HeaderError::InvalidParentTimestamp { .. }
        | HeaderError::UnkownWorkerId
        | HeaderError::UnknownAuthority(_) => Some(Penalty::Fatal),
        // ignore (local/transient, not the peer's fault)
        //
        // Storage is our own DB error, and UnknownExecutionResult means a peer is merely
        // ahead of our execution while we catch up (the vote handler notes "this doesn't
        // hurt"). Penalizing either would punish an honest peer for a local condition, and
        // contradicts the sibling PrimaryNetworkError::Storage and *::Timeout arms that
        // already map to None.
        HeaderError::PendingCertificateOneshot
        | HeaderError::Storage(_)
        | HeaderError::UnknownExecutionResult(_)
        | HeaderError::TNSend(_)
        | HeaderError::InvalidEpoch { .. }
        | HeaderError::NotCommitteeMember
        | HeaderError::ClosedWatchChannel => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Finding 2 (#819): faults determined by the gossip envelope's content — a malformed payload
    /// (`Decode`) or a wrong declared topic (`InvalidTopic`) — are the message author's, so the
    /// gossip path charges the author rather than Fatal-banning the honest relaying peer (#801).
    #[test]
    fn author_content_faults_are_decode_and_invalid_topic() {
        assert!(PrimaryNetworkError::Decode(BcsError::Eof).is_author_content_fault());
        assert!(PrimaryNetworkError::InvalidTopic.is_author_content_fault());
    }

    /// Every other fault keeps its relayer / no-penalty attribution: transport and local faults
    /// are not the author's, and embedded certificate/consensus content is signed by a party
    /// carried inside the payload, not by the gossip source.
    #[test]
    fn non_envelope_faults_are_not_author_content() {
        assert!(!PrimaryNetworkError::InvalidEpochRequest.is_author_content_fault());
        assert!(!PrimaryNetworkError::UnknownConsensusOutput(0).is_author_content_fault());
        assert!(!PrimaryNetworkError::InvalidRequest("bad".to_string()).is_author_content_fault());
        assert!(!PrimaryNetworkError::Internal("boom".to_string()).is_author_content_fault());
    }
}
