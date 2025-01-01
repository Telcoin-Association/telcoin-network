//! Response message types
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tn_types::{
    BlockHash, Certificate, CertificateDigest, Vote, WorkerBlock, WorkerId, WorkerInfo,
};

type NetworkPublicKey = fastcrypto::ed25519::Ed25519PublicKey;

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
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
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

//=== Engine

/// Verification result for peer's latest execution result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyExecutionResponse {
    /// Boolean if the execution header was considered valid by engine.
    ///
    /// Engine verifies the integrity of the data and if the block num hash matches.
    ///
    /// TODO: should this return anything else?
    pub valid: bool,
}
