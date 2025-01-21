//! Request message types

use reth_primitives::SealedHeader;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tn_types::{AuthorityIdentifier, BlockHash, Certificate, Header, NetworkPublicKey, Round};
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

    /// Parent certificates provided by the requester, in case the primary's peer doesn't yet
    /// have them. The peer requires parent certs in order to offer a vote.
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
                    .map(|v| v - gc_round)
                    .collect::<RoaringBitmap>()
                    .serialize_into(&mut serialized)
                    .expect(
                        "rounds serialize into roaring bitmap for FetchCertificatesRequest bounds",
                    );
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

/// Used by the primary to request that the worker fetch the missing blocks and reply
/// with all of the content.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchesRequest {
    /// Missing block digests to fetch from peers.
    pub digests: HashSet<BlockHash>,
    /// The network public key of the peers.
    pub known_workers: HashSet<NetworkPublicKey>,
}

//=== Workers

/// Used by primary to bulk request blocks from workers local store.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RequestBatchesRequest {
    /// Vec of requested batches' digests
    pub batch_digests: Vec<BlockHash>,
}

/// Primary to engine request to verify a peer's latest execution result.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifyExecutionRequest {
    /// Execution data to verify.
    ///
    /// The peer sends their latest sealed header of the finalized canonical tip.
    pub latest: SealedHeader,
}

/// Used by the Engine to forward requests from peers trying to sync.
///
/// TODO: this should probably be a peer request.
/// The primary would add this peer and then the peer
/// can request through another request/response method
/// on PrimaryToPrimary or something similar to indicate
/// non-consensus related requests. Maybe use "SyncState"
/// namespace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncStateRequest {
    /// Missing blocks
    pub digests: HashSet<BlockHash>,
}

impl From<HashSet<BlockHash>> for SyncStateRequest {
    fn from(digests: HashSet<BlockHash>) -> Self {
        Self { digests }
    }
}

/// Request a consensus chain header with consensus output.
///
/// If both number and hash are set they should match (no need to set them both).
/// If neither number or hash are set then will return the latest consensus chain header.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ConsensusOutputRequest {
    /// Block number requesting if not None.
    pub number: Option<u64>,
    /// Block hash requesting if not None.
    pub hash: Option<BlockHash>,
}
