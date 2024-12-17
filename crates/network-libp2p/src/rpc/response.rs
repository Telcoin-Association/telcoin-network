//! Response data for RPC.

use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use std::collections::HashMap;
use tn_types::{BlockHash, Certificate, CertificateDigest, Epoch, Vote, WorkerBlock};

/// The STATUS request/response handshake message.
#[derive(Encode, Decode, Clone, Debug, PartialEq)]
pub struct StatusMessage {
    /// The fork version for this chain.
    pub fork_id: [u8; 4],
    /// Hash of the latest finalized block.
    ///
    /// TODO: BlockHash doesn't impl `encode` so using this workaround.
    pub finalized_hash: [u8; 32],
    /// The current epoch this node is tracking.
    pub epoch: Epoch,
}

/// Response from peers after receiving a certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendCertificateResponse {
    /// Boolean if the certificate was considered valid by peer.
    pub accepted: bool,
}

/// Used by the primary to reply to RequestVoteRequest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RequestVoteResponse {
    /// The peer's vote.
    pub vote: Option<Vote>,
    /// Indicates digests of missing certificates without which a vote cannot be provided.
    pub missing: Vec<CertificateDigest>,
}

/// Used by the primary to reply to FetchCertificatesRequest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchCertificatesResponse {
    /// Certificates sorted from lower to higher rounds.
    pub certificates: Vec<Certificate>,
}

/// All blocks requested by the primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchBlocksResponse {
    /// The missing blocks fetched from peers.
    pub blocks: HashMap<BlockHash, WorkerBlock>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBlocksResponse {
    /// Requested blocks.
    pub blocks: Vec<WorkerBlock>,
    /// If true, the primary should request the blocks from the workers again.
    /// This may not be something that can be trusted from a remote worker.
    pub is_size_limit_reached: bool,
}
