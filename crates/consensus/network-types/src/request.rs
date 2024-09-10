//! Request message types
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tn_types::{
    AuthorityIdentifier, BlockHash, Certificate, Header, NetworkPublicKey, Round,
    TransactionSigned, WorkerBlock, WorkerId,
};
use tracing::warn;

/// Request for broadcasting certificates to peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendCertificateRequest {
    /// Sending primary's certificate for round.
    pub certificate: Certificate,
}

/// Used by the primary to request a vote from other primaries on newly produced headers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// This primary's header for round.
    pub header: Header,

    // Parent certificates provided by the requester, in case the primary's peer doesn't yet
    // have them. The peer requires parent certs in order to offer a vote.
    pub parents: Vec<Certificate>,
}

/// Used by the primary to fetch certificates from other primaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchCertificatesRequest {
    /// The exclusive lower bound is a round number where each primary should return certificates
    /// above that. This corresponds to the GC round at the requestor.
    pub exclusive_lower_bound: Round,
    /// This contains per authority serialized RoaringBitmap for the round diffs between
    /// - rounds of certificates to be skipped from the response and
    /// - the GC round.
    ///
    /// These rounds are skipped because the requestor already has them.
    pub skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    /// Maximum number of certificates that should be returned.
    pub max_items: usize,
}

impl FetchCertificatesRequest {
    #[allow(clippy::mutable_key_type)]
    pub fn get_bounds(&self) -> (Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>) {
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .filter_map(|(k, serialized)| {
                match RoaringBitmap::deserialize_from(&mut &serialized[..]) {
                    Ok(bitmap) => {
                        let rounds: BTreeSet<Round> = bitmap
                            .into_iter()
                            .map(|r| self.exclusive_lower_bound + r as Round)
                            .collect();
                        Some((*k, rounds))
                    }
                    Err(e) => {
                        warn!("Failed to deserialize RoaringBitmap {e}");
                        None
                    }
                }
            })
            .collect();
        (self.exclusive_lower_bound, skip_rounds)
    }

    #[allow(clippy::mutable_key_type)]
    pub fn set_bounds(
        mut self,
        gc_round: Round,
        skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    ) -> Self {
        self.exclusive_lower_bound = gc_round;
        self.skip_rounds = skip_rounds
            .into_iter()
            .map(|(k, rounds)| {
                let mut serialized = Vec::new();
                rounds
                    .into_iter()
                    .map(|v| u32::try_from(v - gc_round).unwrap())
                    .collect::<RoaringBitmap>()
                    .serialize_into(&mut serialized)
                    .unwrap();
                (k, serialized)
            })
            .collect();
        self
    }

    pub fn set_max_items(mut self, max_items: usize) -> Self {
        self.max_items = max_items;
        self
    }
}

/// Used by the primary to request that the worker fetch the missing batches and reply
/// with all of the content.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchesRequest {
    /// Missing batch digests to fetch from peers.
    pub digests: HashSet<BlockHash>,
    /// The network public key of the peers.
    pub known_workers: HashSet<NetworkPublicKey>,
}

/// TODO: probably delete this
///
/// Used by the Engine to request missing batches from the worker's store
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MissingBatchesRequest {
    /// Missing batches
    pub digests: HashSet<BlockHash>,
}

impl From<HashSet<BlockHash>> for MissingBatchesRequest {
    fn from(digests: HashSet<BlockHash>) -> Self {
        Self { digests }
    }
}

//=== Workers

/// Used by primary to bulk request batches from workers local store.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RequestBatchesRequest {
    /// Vec of requested batches' digests
    pub batch_digests: Vec<BlockHash>,
}

/// Worker's batch maker request to EL after timer goes off.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildBatchRequest {
    /// The worker_id for the batch.
    pub worker_id: WorkerId,
}

impl From<WorkerId> for BuildBatchRequest {
    fn from(worker_id: WorkerId) -> Self {
        Self { worker_id }
    }
}

/// Engine to worker after a batch is built.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealBatchRequest {
    /// Collection of transactions encoded as bytes.
    pub payload: Vec<TransactionSigned>,
    /// Execution data for validation.
    pub worker_block: WorkerBlock,
}

/// Used by workers to validate a peer's batch using EL.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidateBatchRequest {
    /// The peer's batch to validate.
    pub batch: WorkerBlock,
    /// The worker's id.
    ///
    /// TODO: this is redundant because
    /// there is a method on anemo requests, but it returns
    /// an option which makes me wonder if it's reliably present.
    pub worker_id: WorkerId,
}

impl From<SealBatchRequest> for WorkerBlock {
    fn from(value: SealBatchRequest) -> Self {
        WorkerBlock::new(value.payload, value.worker_block.sealed_header.clone())
    }
}
