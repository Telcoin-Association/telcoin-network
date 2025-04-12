//! Messages exchanged between primaries.

use crate::error::{PrimaryNetworkError, PrimaryNetworkResult};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tn_network_libp2p::{types::IntoRpcError, PeerExchangeMap, TNMessage};
use tn_types::{
    AuthorityIdentifier, BlockHash, Certificate, CertificateDigest, ConsensusHeader, Header, Round,
    Vote,
};

/// Primary messages on the gossip network.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryGossip {
    /// A new certificate broadcast from peer.
    ///
    /// Certificates are small and okay to gossip uncompressed:
    /// - 3 signatures ~= 0.3kb
    /// - 99 signatures ~= 3.5kb
    ///
    /// NOTE: `snappy` is slightly larger than uncompressed.
    Certificate(Box<Certificate>),
    /// Consensus output reached- publish the consensus chain height and new block hash.
    Consenus(u64, BlockHash),
}

// impl TNMessage trait for types
impl TNMessage for PrimaryRequest {}
impl TNMessage for PrimaryResponse {}

/// Requests from Primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryRequest {
    /// Primary request for vote on new header.
    Vote {
        /// This primary's header for the round.
        header: Arc<Header>,
        /// Parent certificates provided by the requesting peer in case the primary's peer is
        /// missing them. The peer requires parent certs in order to vote.
        parents: Vec<Certificate>,
    },
    /// Request for missing certificates.
    MissingCertificates {
        /// Inner type with specific helper methods for requesting missing certificates.
        inner: MissingCertificatesRequest,
    },
    /// Request a consensus chain header with consensus output.
    ///
    /// If both number and hash are set they should match (no need to set them both).
    /// If neither number or hash are set then will return the latest consensus chain header.
    ConsensusHeader {
        /// Block number requesting if not None.
        number: Option<u64>,
        /// Block hash requesting if not None.
        hash: Option<BlockHash>,
    },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange { peers: PeerExchangeMap },
}

// unit test for this struct in primary::src::tests::network_tests::test_missing_certs_request
/// Used by the primary to fetch certificates from other primaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingCertificatesRequest {
    /// The request is for certificates AFTER this round (non-inclusive). The boundary indicates
    /// the difference between the requestor's GC round and is the last round for which this peer
    /// has sufficient certificates.
    pub exclusive_lower_bound: Round,
    /// Rounds that should be skipped while processing this request (by authority). The rounds are
    /// serialized as [RoaringBitmap]s.
    pub skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    /// The maximum number of expected certificates included in the response.
    pub max_items: usize,
}

impl MissingCertificatesRequest {
    /// Deserialize the [RoaringBitmap] representing the difference between the requesting peer's
    /// lower boundary and their GC round.
    pub(crate) fn get_bounds(
        &self,
    ) -> PrimaryNetworkResult<(Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>)> {
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .map(|(k, serialized)| {
                let rounds = RoaringBitmap::deserialize_from(&serialized[..])?
                    .into_iter()
                    .map(|r| self.exclusive_lower_bound + r as Round)
                    .collect::<BTreeSet<Round>>();
                Ok((k.clone(), rounds))
            })
            .collect::<PrimaryNetworkResult<BTreeMap<_, _>>>()?;
        Ok((self.exclusive_lower_bound, skip_rounds))
    }

    /// Set the bounds for requesting missing certificates based on the current GC round.
    ///
    /// This method specifies which rounds should be skipped because they are already in storage.
    pub(crate) fn set_bounds(
        mut self,
        gc_round: Round,
        skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    ) -> PrimaryNetworkResult<Self> {
        self.exclusive_lower_bound = gc_round;
        self.skip_rounds = skip_rounds
            .into_iter()
            .map(|(k, rounds)| {
                let mut serialized = Vec::new();
                rounds
                    .into_iter()
                    .map(|v| v - gc_round)
                    .collect::<RoaringBitmap>()
                    .serialize_into(&mut serialized)?;

                Ok((k, serialized))
            })
            .collect::<PrimaryNetworkResult<Vec<_>>>()?;

        Ok(self)
    }

    /// Specify the maximum number of expected certificates in the peer's response.
    pub fn set_max_items(mut self, max_items: usize) -> Self {
        self.max_items = max_items;
        self
    }
}

impl From<PeerExchangeMap> for PrimaryRequest {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}

//
//
//=== Response types
//
//

/// Response to primary requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryResponse {
    /// The peer's vote if the peer considered the proposed header valid.
    Vote(Vote),
    /// The requested certificates requested by a peer.
    RequestedCertificates(Vec<Certificate>),
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<CertificateDigest>),
    /// The requested consensus header.
    ConsensusHeader(Arc<ConsensusHeader>),
    /// Exchange peer information.
    PeerExchange { peers: PeerExchangeMap },
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(PrimaryRPCError),
}

impl PrimaryResponse {
    /// Helper method if the response is an error.
    pub fn is_err(&self) -> bool {
        matches!(self, PrimaryResponse::Error(_))
    }
}

impl IntoRpcError<PrimaryNetworkError> for PrimaryResponse {
    fn into_error(error: PrimaryNetworkError) -> Self {
        Self::Error(PrimaryRPCError(error.to_string()))
    }
}

impl From<PrimaryRPCError> for PrimaryResponse {
    fn from(value: PrimaryRPCError) -> Self {
        Self::Error(value)
    }
}

/// Application-specific error type while handling Primary request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PrimaryRPCError(pub String);

impl From<PeerExchangeMap> for PrimaryResponse {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}
