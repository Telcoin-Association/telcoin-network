//! Response message types
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tn_types::{
    BlockHash, Certificate, CertificateDigest, Vote, WorkerBlock, WorkerId, WorkerInfo,
};

/// Response from peers after receiving a certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendCertificateResponse {
    /// Boolean if the certificate was considered valid by peer.
    pub accepted: bool,
}

/// Used by the primary to reply to RequestVoteRequest.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBlocksResponse {
    /// The missing blocks fetched from peers.
    pub blocks: HashMap<BlockHash, WorkerBlock>,
}

/// Information for the workers.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerInfoResponse {
    /// Map of workers' id and their network addresses.
    pub workers: BTreeMap<WorkerId, WorkerInfo>,
}

//=== Worker

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBlocksResponse {
    /// Requested blocks.
    pub blocks: Vec<WorkerBlock>,
    /// If true, the primary should request the blocks from the workers again.
    /// This may not be something that can be trusted from a remote worker.
    pub is_size_limit_reached: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedBlockResponse {
    /// The block.
    pub block: WorkerBlock,
    /// The digest of the sealed block.
    pub digest: BlockHash,
    /// Worker id who broadcast the block
    pub worker_id: WorkerId,
}
