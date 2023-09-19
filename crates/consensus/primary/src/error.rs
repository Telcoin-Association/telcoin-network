//! Error for primary

use fastcrypto::hash::Digest;
use lattice_network::LocalClientError;
use lattice_typed_store::StoreError;
use thiserror::Error;
use tn_types::consensus::{HeaderDigest, Round, VoteDigest, crypto, Epoch, TimestampSec, CertificateDigest, error::{AcceptNotification, DagError}};

/// Result alias for returning Primary error.
pub type PrimaryResult<T> = Result<T, PrimaryError>;

#[derive(Clone, Debug, Error)]
pub enum PrimaryError {
    /// Client errors
    #[error("Local network error: {0}")]
    Client(#[from] LocalClientError),

    /// Dag error
    #[error("Dag error: {0}")]
    DagError(#[from] DagError),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Too many parents in RequestVoteRequest {0} > {1}")]
    TooManyParents(usize, usize),

    #[error("Channel {0} has closed unexpectedly")]
    ClosedChannel(String),

    #[error("Invalid Authorities Bitmap: {0}")]
    InvalidBitmap(String),

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Invalid header digest")]
    InvalidHeaderDigest,

    #[error("Header {0} has bad worker IDs")]
    HeaderHasBadWorkerIds(HeaderDigest),

    #[error("Header {0} has parents with invalid round numbers")]
    HeaderHasInvalidParentRoundNumbers(HeaderDigest),

    #[error("Header {0} has parents with invalid timestamp")]
    HeaderHasInvalidParentTimestamp(HeaderDigest),

    #[error("Header {0} has more than one parent certificate with the same authority")]
    HeaderHasDuplicateParentAuthorities(HeaderDigest),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(String),

    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(String),

    #[error("Received unexpected vote for header {0}")]
    UnexpectedVote(HeaderDigest),

    #[error("Already voted with a different digest {0} at round {2}, for header {1}")]
    AlreadyVoted(VoteDigest, HeaderDigest, Round),

    #[error("Already voted a newer header for digest {0} round {1} < {2}")]
    AlreadyVotedNewerHeader(HeaderDigest, Round, Round),

    #[error("Could not form a certificate for header {0}")]
    CouldNotFormCertificate(HeaderDigest),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Cannot load certificates from our own proposed header")]
    ProposedHeaderMissingCertificates,

    #[error("Parents of header {0} are not a quorum")]
    HeaderRequiresQuorum(HeaderDigest),

    #[error("Message {0} (round {1}) too old for GC round {2}")]
    TooOld(Digest<{ crypto::DIGEST_LENGTH }>, Round, Round),

    #[error("Message {0} (round {1}) is too new for this primary at round {2}")]
    TooNew(Digest<{ crypto::DIGEST_LENGTH }>, Round, Round),

    #[error("Vote {0} (round {1}) too old for round {2}")]
    VoteTooOld(Digest<{ crypto::DIGEST_LENGTH }>, Round, Round),

    #[error("Invalid epoch (expected {expected}, received {received})")]
    InvalidEpoch { expected: Epoch, received: Epoch },

    #[error("Invalid round (expected {expected}, received {received})")]
    InvalidRound { expected: Round, received: Round },

    #[error("Invalid timestamp (created at {created_time}, received at {local_time})")]
    InvalidTimestamp { created_time: TimestampSec, local_time: TimestampSec },

    #[error("Invalid parent {0} (not found in genesis)")]
    InvalidGenesisParent(CertificateDigest),

    #[error("No peer can be reached for fetching certificates! Check if network is healthy.")]
    NoCertificateFetched,

    #[error("Too many certificates in the FetchCertificatesResponse {0} > {1}")]
    TooManyFetchedCertificatesReturned(usize, usize),

    #[error("Processing was suspended to retrieve parent certificates")]
    Suspended(AcceptNotification),

    #[error("System shutting down")]
    ShuttingDown,

    #[error("Channel full")]
    ChannelFull,

    #[error("Operation was canceled")]
    Canceled,
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for PrimaryError {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        match err {
            tokio::sync::mpsc::error::TrySendError::Full(_) => PrimaryError::ChannelFull,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => PrimaryError::ShuttingDown,
        }
    }
}
