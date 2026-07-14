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
    /// Whether this gossip fault is determined by the *content* of the message and is therefore
    /// charged to its authenticated publisher (the gossip source `GossipMessage::source`, resolved
    /// to `author`), rather than to the peer that relayed it.
    ///
    /// Consulted only on the gossip path (`PrimaryNetwork::process_gossip`), whose deep validation
    /// of a signed payload runs *after* gossipsub has already accepted and forwarded the message.
    /// An honest relayer therefore cannot screen a content-determined fault before relaying it, and
    /// charging the relayer for one lets a Byzantine author ban honest forwarders (#801/#785/#819).
    /// The request/response paths never consult this classifier: there the peer *is* the originator
    /// and is penalized unconditionally.
    ///
    /// The publisher is the accountable party (not the relayer, and not a signer embedded in the
    /// payload) for two reasons:
    ///
    /// 1. The source is authenticated. Gossipsub runs with `MessageAuthenticity::Signed` and
    ///    `ValidationMode::Strict` (`crates/network-libp2p/src/consensus.rs`), so an accepted
    ///    message carries a source signed by that peer's key; an attacker cannot publish under a
    ///    victim's identity. Honest nodes only gossip content they produced and (for consensus
    ///    results / epoch votes) self-signed (`publish_certificate` / `publish_consensus` /
    ///    `publish_epoch_vote`), so a content-determined fault is the publisher's own.
    ///
    /// 2. Charging the *embedded* signer would be unsafe. A certificate is a multi-party aggregate,
    ///    and the vote / consensus-result faults here are exactly signature-verification failures,
    ///    so the key named in the payload is unauthenticated at the point of fault. An attacker
    ///    could gossip a `ConsensusResult { validator: <victim>, .. }`, or an `EpochVote` naming
    ///    any committee key, with a bad signature and have the innocent victim penalized: a framing
    ///    vector the authenticated publisher carries no risk of.
    ///
    /// Parallels `WorkerNetworkError::is_author_content_fault` (tn-worker) at the mechanism level,
    /// though the primary additionally covers the deep embedded-signature faults its signed
    /// payloads carry, which the worker has no equivalent for.
    ///
    /// True for the envelope faults [`Self::Decode`] / [`Self::InvalidTopic`] and for the embedded
    /// signed-payload content faults reachable from the gossip handler: [`Self::Certificate`] (a
    /// malformed / unsigned / inquorate / bad-signature certificate), [`Self::InvalidHeader`] (an
    /// `EpochVote` failing its signature or committee-membership check), and
    /// [`Self::UnknownConsensusHeaderCert`] (a `ConsensusResult` with a bad signature). The
    /// remaining arms are transport/local/benign and keep their relayer / no-penalty attribution;
    /// [`Self::PeerNotInCommittee`] is content-determined but already maps to no penalty, so the
    /// relayer was never charged for it.
    pub(crate) fn is_author_content_fault(&self) -> bool {
        //
        // explicitly match every variant so the classification is revisited with changes
        //
        match self {
            PrimaryNetworkError::Decode(_)
            | PrimaryNetworkError::InvalidTopic
            | PrimaryNetworkError::InvalidHeader(_)
            | PrimaryNetworkError::Certificate(_)
            | PrimaryNetworkError::UnknownConsensusHeaderCert(_) => true,
            PrimaryNetworkError::StdIo(_)
            | PrimaryNetworkError::Storage(_)
            | PrimaryNetworkError::InvalidRequest(_)
            | PrimaryNetworkError::Internal(_)
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

    /// Transport and local faults keep their relayer / no-penalty attribution: they are not
    /// determined by the gossip payload's content, so the authenticated publisher is not the
    /// accountable party. (The embedded signed-payload content faults, by contrast, *are*
    /// author-charged: see `embedded_signed_payload_faults_are_author_content`.)
    #[test]
    fn non_content_faults_are_not_author_charged() {
        assert!(!PrimaryNetworkError::InvalidEpochRequest.is_author_content_fault());
        assert!(!PrimaryNetworkError::UnknownConsensusOutput(0).is_author_content_fault());
        assert!(!PrimaryNetworkError::InvalidRequest("bad".to_string()).is_author_content_fault());
        assert!(!PrimaryNetworkError::Internal("boom".to_string()).is_author_content_fault());
    }

    /// Finding #871: the primary's signed gossip payloads carry content faults that deep validation
    /// only surfaces after gossipsub has forwarded the message. Each is charged to the
    /// authenticated author, not the honest relayer: a malformed certificate (`Certificate`),
    /// an `EpochVote` that fails its signature (`PeerNotAuthor`) or committee-membership
    /// (`UnknownAuthority`) check, and a `ConsensusResult` with a bad signature
    /// (`UnknownConsensusHeaderCert`).
    #[test]
    fn embedded_signed_payload_faults_are_author_content() {
        // gossiped Certificate faults (validate_received / process_peer_certificate)
        assert!(PrimaryNetworkError::Certificate(CertManagerError::Certificate(
            CertificateError::Unsigned
        ))
        .is_author_content_fault());
        assert!(PrimaryNetworkError::Certificate(CertManagerError::Certificate(
            CertificateError::InvalidSignature
        ))
        .is_author_content_fault());
        // gossiped EpochVote: bad signature -> PeerNotAuthor; non-committee key -> UnknownAuthority
        assert!(PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor)
            .is_author_content_fault());
        assert!(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownAuthority(
            "key not in committee".to_string()
        ))
        .is_author_content_fault());
        // gossiped ConsensusResult with a bad signature
        assert!(PrimaryNetworkError::UnknownConsensusHeaderCert(ConsensusHeaderDigest::default())
            .is_author_content_fault());
    }

    /// Finding #871 attribution guard. `process_gossip` charges the accountable peer with
    /// `if fault.is_author_content_fault() { author } else { relayer }.zip((&fault).into())`, so
    /// for each fault this PR moved to the author arm the charged party is the authenticated
    /// author and the penalty is the fault's own. Both halves are asserted here: a future edit
    /// that re-routes a variant to the relayer, or drops its penalty (which `zip` would silently
    /// swallow, charging no one), fails this test. The mirror of the selection is exercised by the
    /// closing control case, and the honest relayer's protection is what the change exists for.
    #[test]
    fn moved_gossip_faults_charge_the_author_not_the_relayer() {
        // the penalty the gossip handler charges the *author*, or `None` if the fault is not
        // author-content (so the relayer would be charged instead) or carries no penalty
        let author_penalty = |fault: &PrimaryNetworkError| {
            fault.is_author_content_fault().then(|| Option::<Penalty>::from(fault)).flatten()
        };

        // certificate: unsigned / bad-signature -> Fatal, charged to the author
        assert!(matches!(
            author_penalty(&PrimaryNetworkError::Certificate(CertManagerError::Certificate(
                CertificateError::Unsigned
            ))),
            Some(Penalty::Fatal)
        ));
        assert!(matches!(
            author_penalty(&PrimaryNetworkError::Certificate(CertManagerError::Certificate(
                CertificateError::InvalidSignature
            ))),
            Some(Penalty::Fatal)
        ));
        // epoch vote: bad signature (PeerNotAuthor) / non-committee key (UnknownAuthority) -> Fatal
        assert!(matches!(
            author_penalty(&PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor)),
            Some(Penalty::Fatal)
        ));
        assert!(matches!(
            author_penalty(&PrimaryNetworkError::InvalidHeader(HeaderError::UnknownAuthority(
                "key not in committee".to_string()
            ))),
            Some(Penalty::Fatal)
        ));
        // consensus result: bad signature -> Mild
        assert!(matches!(
            author_penalty(&PrimaryNetworkError::UnknownConsensusHeaderCert(
                ConsensusHeaderDigest::default()
            )),
            Some(Penalty::Mild)
        ));

        // control: a non-content fault is not author-charged, so the relayer arm is taken and the
        // author penalty is `None` even though the fault itself carries one
        let relayer_fault = PrimaryNetworkError::InvalidEpochRequest;
        assert!(!relayer_fault.is_author_content_fault());
        assert!(author_penalty(&relayer_fault).is_none());
        assert!(matches!(Option::<Penalty>::from(&relayer_fault), Some(Penalty::Medium)));
    }
}
