//! Messages exchanged between primaries.

use crate::error::{PrimaryNetworkError, PrimaryNetworkResult};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tn_network_libp2p::{types::IntoRpcError, PeerExchangeMap, ServiceClass, TNMessage};
use tn_types::{
    error::HeaderError, roaring_container_count, AuthorityIdentifier, Certificate, ConsensusResult,
    Epoch, EpochCertificate, EpochDigest, EpochRecord, EpochVote, Header, HeaderDigest, Round,
    Vote,
};

/// Primary messages on the gossip network.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum PrimaryGossip {
    /// A new certificate broadcast from peer.
    ///
    /// Certificates are small and okay to gossip uncompressed:
    /// - 3 signatures ~= 0.3kb
    /// - 99 signatures ~= 3.5kb
    ///
    /// NOTE: `snappy` is slightly larger than uncompressed.
    Certificate(Box<Certificate>),
    /// Consensus output reached- publish the consensus chain height and new block hash.
    Consensus(Box<ConsensusResult>),
    /// Signed hash sent out by committee memebers at epoch start.
    EpochVote(Box<EpochVote>),
}

// impl TNMessage trait for types
impl TNMessage for PrimaryRequest {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange { peers } => Some(peers.clone()),
            _ => None,
        }
    }

    fn service_class(&self) -> ServiceClass {
        match self {
            Self::Vote { .. } => ServiceClass::Vote,
            Self::EpochRecord { .. } => ServiceClass::EpochRecord,
            Self::PeerExchange { .. } => ServiceClass::Other,
        }
    }
}
impl TNMessage for PrimaryResponse {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        None
    }
}

/// The most parent certificates one [`PrimaryRequest::Vote`] carries on the wire.
///
/// An honest request carries at most one parent certificate for each committee member, so this
/// value must stay at or above the largest committee size. The decoder rejects a longer list from
/// its length prefix, before it decodes one certificate. Each certificate holds two BLS
/// signatures, and the decode of each signature decompresses a curve point. Without the bound,
/// the sender sets the decode cost of one request, up to the message size cap.
///
/// A full parent set for a committee near this size does not fit in the default
/// `max_rpc_message_size`, so change the two values together.
const MAX_VOTE_PARENTS: usize = 256;

/// Requests from Primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryRequest {
    /// Primary request for vote on new header.
    Vote {
        /// This primary's header for the round.
        header: Arc<Header>,
        /// Parent certificates provided by the requesting peer in case the primary's peer is
        /// missing them. The peer requires parent certs in order to vote.
        ///
        /// The decoder accepts at most `MAX_VOTE_PARENTS` certificates.
        #[serde(deserialize_with = "bounded_vote_parents")]
        parents: Vec<Certificate>,
    },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange { peers: PeerExchangeMap },
    /// Request an ['EpochRecord'] with ['EpochCertificate'].
    ///
    /// If both number and hash are set they should match (no need to set them both).
    /// If neither number or hash are set then will return the latest epoch record the node has
    /// available.
    EpochRecord {
        /// Block number requesting if not None.
        epoch: Option<Epoch>,
        /// Block hash requesting if not None.
        hash: Option<EpochDigest>,
    },
}

/// Decode the `parents` of a [`PrimaryRequest::Vote`] with the [`MAX_VOTE_PARENTS`] bound.
///
/// Only the decoder changes. The derived `Serialize` still writes the list, so the bytes on the
/// wire are those of a plain `Vec<Certificate>`.
fn bounded_vote_parents<'de, D>(deserializer: D) -> Result<Vec<Certificate>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_seq(BoundedVoteParents)
}

/// Visitor that enforces [`MAX_VOTE_PARENTS`] from the length prefix and from the count.
struct BoundedVoteParents;

impl BoundedVoteParents {
    /// Accept a count of parents at or under [`MAX_VOTE_PARENTS`]. Give the length error for a
    /// larger count.
    fn check_count<E>(&self, count: usize) -> Result<(), E>
    where
        E: serde::de::Error,
    {
        if count > MAX_VOTE_PARENTS {
            Err(E::invalid_length(count, self))
        } else {
            Ok(())
        }
    }
}

impl<'de> serde::de::Visitor<'de> for BoundedVoteParents {
    type Value = Vec<Certificate>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "at most {MAX_VOTE_PARENTS} parent certificates")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        // reject from the prefix before any certificate is decoded
        self.check_count::<A::Error>(seq.size_hint().unwrap_or(0))?;
        // take one past the cap so an unhinted stream over the cap is still rejected
        let parents = std::iter::from_fn(|| seq.next_element::<Certificate>().transpose())
            .take(MAX_VOTE_PARENTS.saturating_add(1))
            .collect::<Result<Vec<_>, _>>()?;
        self.check_count::<A::Error>(parents.len())?;
        Ok(parents)
    }
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
    /// The maximum size of the uncompressed response message (in bytes). The caller shares this so
    /// the response doesn't get rejected by the request_response codec.
    pub max_response_size: usize,
}

impl MissingCertificatesRequest {
    /// Deserialize the [RoaringBitmap] representing the difference between the requesting peer's
    /// lower boundary and their GC round.
    ///
    /// Every skip-round entry is attacker-controlled, so three request-shaped allocations are
    /// bounded here, each rejected before it happens:
    /// - `max_authorities` (the caller passes the committee size) bounds the number of
    ///   `skip_rounds` entries (GHSA-wwqq-q2xx-4jf9). One bitmap per committee authority is the
    ///   invariant; nothing else limits how many entries a peer can name in a single request, and
    ///   each accepted entry costs a decode plus a retained `BTreeSet`.
    /// - `max_skip_rounds_per_authority` (the committee's `max_skip_rounds_for_missing_certs`)
    ///   bounds each bitmap twice (GHSA-4ggp-fcpj-g9f3): once on the container count read straight
    ///   from the serialized header *before* [`RoaringBitmap::deserialize_from`] allocates anything
    ///   (a few hundred KiB of run-encoded bitmap would otherwise decompress to hundreds of MiB of
    ///   heap during deserialization -- roaring 0.10 has no run store, so every run container is
    ///   expanded into an array/bitmap store), and once on the decoded cardinality *before* the
    ///   `O(cardinality)` collect below.
    pub(crate) fn get_bounds(
        &self,
        max_skip_rounds_per_authority: usize,
        max_authorities: usize,
    ) -> PrimaryNetworkResult<(Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>)> {
        // One skip-round bitmap per committee authority is the documented invariant; reject a
        // request naming more authorities than the committee has before decoding anything.
        (self.skip_rounds.len() <= max_authorities).then_some(()).ok_or_else(|| {
            PrimaryNetworkError::InvalidRequest(format!(
                "skip_rounds authority count exceeds committee size: {} > {max_authorities}",
                self.skip_rounds.len()
            ))
        })?;

        let max_cardinality = u64::try_from(max_skip_rounds_per_authority).unwrap_or(u64::MAX);
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .map(|(k, serialized)| {
                // Reject a decompression bomb *before* `deserialize_from` materializes it: the
                // header declares the container count, and a genuine bitmap of cardinality N spans
                // at most N containers, so the cardinality limit doubles as a sound container-count
                // limit that never rejects a well-formed request while capping the ~8 KiB-per-
                // container heap that deserialization would otherwise allocate.
                let containers = roaring_container_count(serialized).map_err(|e| {
                    PrimaryNetworkError::InvalidRequest(format!("skip_rounds bitmap: {e}"))
                })?;
                (containers <= max_cardinality).then_some(()).ok_or_else(|| {
                    PrimaryNetworkError::InvalidRequest(format!(
                        "skip_rounds bitmap declares too many containers: {containers} > {max_cardinality}"
                    ))
                })?;

                let bitmap = RoaringBitmap::deserialize_from(&serialized[..])?;

                // Reject on cardinality *before* the `O(cardinality)` collect: the container bound
                // still admits up to `max_cardinality` full (65_536-round) containers.
                let cardinality = bitmap.len();
                (cardinality <= max_cardinality).then_some(()).ok_or_else(|| {
                    PrimaryNetworkError::InvalidRequest(format!(
                        "skip_rounds bitmap too large: {cardinality} > {max_cardinality}"
                    ))
                })?;

                let rounds = bitmap
                    .into_iter()
                    .map(|r| {
                        // r: u32 == Round; reject rather than wrap (release) or panic (debug)
                        self.exclusive_lower_bound.checked_add(r).ok_or_else(|| {
                            PrimaryNetworkError::InvalidRequest(
                                "skip round exceeds u32 round space".into(),
                            )
                        })
                    })
                    .collect::<PrimaryNetworkResult<BTreeSet<Round>>>()?;
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
    pub fn set_max_response_size(mut self, max_size: usize) -> Self {
        self.max_response_size = max_size;
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
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<HeaderDigest>),
    /// The requested epoch record and certificate.
    EpochRecord { record: EpochRecord, certificate: EpochCertificate },
    /// Exchange peer information.
    PeerExchange { peers: PeerExchangeMap },
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(PrimaryRPCError),
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    /// This error is likely to succeed in the future and can be retried.
    RecoverableError(PrimaryRPCError),
}

impl PrimaryResponse {
    /// Helper method if the response is an error.
    pub fn is_err(&self) -> bool {
        matches!(self, PrimaryResponse::Error(_))
    }

    /// The reply a peer sends for an epoch-record request it cannot serve because it holds no
    /// certified record for `epoch` (or `hash`).
    ///
    /// This is the error the request handler returns for the same request, encoded the way every
    /// request error is encoded, so a requester can tell this definitive answer apart from other
    /// error replies (such as a responder that has not resolved the requester yet). A request
    /// naming neither gets the invalid-request reply, which is equally final.
    pub fn unavailable_epoch_record(epoch: Option<Epoch>, hash: Option<EpochDigest>) -> Self {
        // the precedence mirrors `RequestHandler::retrieve_epoch_record`: a digest wins
        let error = match (epoch, hash) {
            (_, Some(hash)) => PrimaryNetworkError::UnavailableEpochDigest(hash),
            (Some(epoch), None) => PrimaryNetworkError::UnavailableEpoch(epoch),
            (None, None) => PrimaryNetworkError::InvalidEpochRequest,
        };
        Self::into_error_ref(&error)
    }

    pub(crate) fn into_error_ref(error: &PrimaryNetworkError) -> Self {
        match error {
            PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch { ours, theirs })
                if *theirs == ours + 1 =>
            {
                // This is a common race condition on epoch restart so report as recoverable.
                Self::RecoverableError(PrimaryRPCError(error.to_string()))
            }
            // a request that ran out of time here (e.g. a vote waiting on execution, parents, or
            // batches) can succeed when repeated later, so the requester retries instead of
            // giving up on this node
            PrimaryNetworkError::Timeout(_) => {
                Self::RecoverableError(PrimaryRPCError(error.to_string()))
            }
            // the voter is catching up or its execution is behind the header and refuses to vote
            // for now: the same request succeeds once it has caught up, so the requester retries
            // instead of giving up on this node
            PrimaryNetworkError::InvalidHeader(
                HeaderError::NotActiveCvv | HeaderError::ExecutionBehind { .. },
            ) => Self::RecoverableError(PrimaryRPCError(error.to_string())),
            // the voter has a newer request from the same author: the requester has normally
            // cancelled this one, and one that has not retries instead of giving up on this node
            PrimaryNetworkError::SupersededRequest { .. } => {
                Self::RecoverableError(PrimaryRPCError(error.to_string()))
            }
            PrimaryNetworkError::InvalidHeader(_)
            | PrimaryNetworkError::Decode(_)
            | PrimaryNetworkError::Certificate(_)
            | PrimaryNetworkError::StdIo(_)
            | PrimaryNetworkError::Storage(_)
            | PrimaryNetworkError::InvalidRequest(_)
            | PrimaryNetworkError::Internal(_)
            | PrimaryNetworkError::PeerNotInCommittee(_)
            | PrimaryNetworkError::UnavailableEpoch(_)
            | PrimaryNetworkError::UnavailableEpochDigest(_)
            | PrimaryNetworkError::InvalidTopic
            | PrimaryNetworkError::UnknownConsensusHeaderCert(_)
            | PrimaryNetworkError::UnknownConsensusOutput(_)
            | PrimaryNetworkError::ConsensusChainError(_)
            | PrimaryNetworkError::InvalidEpochVote(_, _, _)
            | PrimaryNetworkError::InvalidEpochRequest => {
                Self::Error(PrimaryRPCError(error.to_string()))
            }
        }
    }
}

impl IntoRpcError<PrimaryNetworkError> for PrimaryResponse {
    fn into_error(error: PrimaryNetworkError) -> Self {
        Self::into_error_ref(&error)
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

#[cfg(test)]
mod tests {
    use super::{PrimaryRequest, MAX_VOTE_PARENTS};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tn_network_libp2p::{PeerExchangeMap, ServiceClass, TNMessage};
    use tn_types::{encode, try_decode, Certificate, Header};

    /// The vote request with the derived decoder, which has no bound on `parents`. This is the
    /// request as nodes decoded it before the bound. `Vote` is variant 0 here and in
    /// [`PrimaryRequest`], so the two encodings can be compared.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum UnboundedVoteRequest {
        /// Mirror of [`PrimaryRequest::Vote`].
        Vote {
            /// The header for the round.
            header: Arc<Header>,
            /// The parent certificates, with no bound at decode.
            parents: Vec<Certificate>,
        },
    }

    /// A vote request for the default header with `count` default parent certificates.
    fn vote_with_parents(count: usize) -> PrimaryRequest {
        PrimaryRequest::Vote {
            header: Arc::new(Header::default()),
            parents: vec![Certificate::default(); count],
        }
    }

    /// A vote request has the vote class.
    #[test]
    fn vote_is_vote_class() {
        let request =
            PrimaryRequest::Vote { header: Arc::new(Header::default()), parents: Vec::new() };
        assert_eq!(request.service_class(), ServiceClass::Vote);
    }

    /// An epoch record request has the epoch record class.
    #[test]
    fn epoch_record_is_epoch_record_class() {
        let request = PrimaryRequest::EpochRecord { epoch: None, hash: None };
        assert_eq!(request.service_class(), ServiceClass::EpochRecord);
    }

    /// Peer exchange is answered inline and has the other class.
    #[test]
    fn peer_exchange_is_other_class() {
        let request = PrimaryRequest::PeerExchange { peers: PeerExchangeMap::default() };
        assert_eq!(request.service_class(), ServiceClass::Other);
    }

    /// A vote request with exactly `MAX_VOTE_PARENTS` parents decodes.
    #[test]
    fn vote_at_parent_cap_decodes() {
        let request = vote_with_parents(MAX_VOTE_PARENTS);
        let decoded = try_decode::<PrimaryRequest>(&encode(&request))
            .expect("a request at the parent cap decodes");
        assert_eq!(decoded, request);
    }

    /// A length prefix one over the cap fails with the length error, before the decoder reads a
    /// certificate.
    #[test]
    fn vote_over_parent_cap_is_rejected_from_the_length_prefix() {
        // `parents` is the last field, so the encoding of an empty list ends with the prefix 0
        let empty = encode(&vote_with_parents(0));
        let (last, head) = empty.split_last().expect("the encoded request has bytes");
        assert_eq!(*last, 0);
        // a list of unit values encodes as its length prefix with no element bytes after it
        let unit_list = (0..=MAX_VOTE_PARENTS).map(|_| ()).collect::<Vec<_>>();
        assert_eq!(unit_list.len(), MAX_VOTE_PARENTS + 1);
        let over_cap = head.iter().copied().chain(encode(&unit_list)).collect::<Vec<u8>>();

        // without the bound, the decoder reads past the prefix and fails at the end of the input
        let error = try_decode::<PrimaryRequest>(&over_cap)
            .expect_err("a length prefix over the parent cap is rejected");
        assert!(
            error.to_string().contains(&format!("invalid length {}", MAX_VOTE_PARENTS + 1)),
            "expected the length error, got: {error}"
        );
    }

    /// The bound changes the decoder only. A vote request has the bytes that the derived codec
    /// gives it, and each decoder reads the bytes of the other.
    #[test]
    fn vote_wire_bytes_match_the_derived_codec() {
        let header = Arc::new(Header::default());
        let parents = vec![Certificate::default(); 3];
        let unbounded =
            UnboundedVoteRequest::Vote { header: header.clone(), parents: parents.clone() };
        let bounded = PrimaryRequest::Vote { header, parents };

        let bytes = encode(&bounded);
        assert_eq!(bytes, encode(&unbounded));
        let from_bounded =
            try_decode::<PrimaryRequest>(&bytes).expect("the bounded decoder reads the bytes");
        assert_eq!(from_bounded, bounded);
        let from_unbounded = try_decode::<UnboundedVoteRequest>(&bytes)
            .expect("the derived decoder reads the bytes");
        assert_eq!(from_unbounded, unbounded);
    }
}
