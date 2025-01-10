// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    crypto, CertificateDigest, Epoch, HeaderDigest, Round, TimestampSec, VoteDigest, WorkerId,
};
use fastcrypto::hash::Digest;
use reth_primitives::BlockHash;
use std::sync::Arc;
use thiserror::Error;
use tn_utils::sync::notify_once::NotifyOnce;

/// Return an error if the condition is false.
#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

pub type DagResult<T> = Result<T, DagError>;

// Notification for certificate accepted.
pub type AcceptNotification = Arc<NotifyOnce>;

pub type StoreError = eyre::Report;

#[derive(Debug, Error)]
pub enum DagError {
    // TEMPORARY - use this in certificate error instead
    #[error(transparent)]
    Header(#[from] HeaderError),

    #[error("Channel {0} has closed unexpectedly")]
    ClosedChannel(String),

    #[error("Invalid Authorities Bitmap: {0}")]
    InvalidBitmap(String),

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Invalid randomness signature")]
    InvalidRandomnessSignature,

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Invalid header digest")]
    InvalidHeaderDigest,

    #[error("Invalid system message")]
    InvalidSystemMessage,

    #[error("Duplicate system message")]
    DuplicateSystemMessage,

    #[error("Invalid certificate version")]
    InvalidCertificateVersion,

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

    #[error("Too many parents in RequestVoteRequest {0} > {1}")]
    TooManyParents(usize, usize),

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

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Processing was suspended to retrieve parent certificates")]
    Suspended(AcceptNotification),

    #[error("System shutting down")]
    ShuttingDown,

    #[error("Channel full")]
    ChannelFull,

    #[error("Operation was canceled")]
    Canceled,
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for DagError {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        match err {
            tokio::sync::mpsc::error::TrySendError::Full(_) => DagError::ChannelFull,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => DagError::ShuttingDown,
        }
    }
}

/// Errors that can be reported while seal a block.
#[derive(Clone, Debug, Error)]
pub enum BlockSealError {
    #[error("Block was rejected by enough peers to never reach quorum")]
    QuorumRejected,
    #[error("Anti quorum reached for block (note this may not be permanent)")]
    AntiQuorum,
    #[error("Timed out waiting for quorum")]
    Timeout,
    #[error("Failed to get enough responses to reach quorum")]
    FailedQuorum,
    #[error("Failed to access consensus DB, this is fatal")]
    FatalDBFailure,
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Node {0} is not in the worker cache")]
    NotInWorkerCache(String),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },
}

#[derive(Error, Debug)]
pub enum CommitteeUpdateError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Node {0} was not in the update")]
    MissingFromUpdate(String),

    #[error("Node {0} has a different stake than expected")]
    DifferentStake(String),
}

/// Result alias for [`HeaderError`].
pub type HeaderResult<T> = Result<T, HeaderError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, Error)]
pub enum HeaderError {
    /// Invalid header request
    #[error("Invalid epoch (expected {expected}, received {received})")]
    InvalidEpoch { expected: Epoch, received: Epoch },
    /// The expected digest does not match the peer's sealed header.
    #[error("Invalid header digest")]
    InvalidHeaderDigest,
    /// The author is not in the current committee.
    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(String),
    /// Worker's ID is not in the cache.
    #[error("Header has an unknown worker ID")]
    UnkownWorkerId,
    /// Vote request includes too many parents.
    #[error("Too many parents in vote request: {0} > {1}")]
    TooManyParents(usize, usize),
    /// Authority network key is missing from committee.
    #[error("Failed to find author in committee by network key.")]
    AuthorityByNetworkKey,
    /// The header wasn't proposed by the author.
    #[error("The proposing peer is not the author.")]
    PeerNotAuthor,
    /// The watch channel for execution results was dropped.
    #[error("Watch channel for execution results dropped.")]
    ClosedWatchChannel,
    /// The proposed header contains a different execution result.
    #[error("Peer's execution result for block {0}: {1:?}")]
    UnknownExecutionResult(u64, BlockHash),
    /// Invalid parent for genesis.
    #[error("Invalid parent for genesis: {0}")]
    InvalidGenesisParent(CertificateDigest),
    /// Error retrieving value from storage.
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),
    /// The proposed header's round is too far behind.
    #[error("Header round {0} is too old for GC round {1}")]
    TooOld(Round, Round),

    /// TODO: this is temporary
    ///
    /// Failed to convert libp2p::PeerId to fastcrypto::ed25519
    #[error("Failed to convert libp2p::PeerId into fastcrypto::ed25519")]
    PeerId,
}
