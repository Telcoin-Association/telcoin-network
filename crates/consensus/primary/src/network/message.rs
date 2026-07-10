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
    error::HeaderError, AuthorityIdentifier, Certificate, ConsensusResult, Epoch, EpochCertificate,
    EpochDigest, EpochRecord, EpochVote, Header, HeaderDigest, Round, Vote,
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
}
impl TNMessage for PrimaryResponse {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        None
    }
}

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
    /// Request to stream a pack file of the consensus data for epoch.
    StreamEpoch {
        /// The epoch we are requesting consensus data for.
        epoch: Epoch,
    },
    /// Request to stream the raw (serialized) consensus output bytes for a consensus chain number.
    ///
    /// Unlike [`Self::ConsensusHeader`], this returns the full pack-file encoded output
    /// (batches + consensus header) rather than just the header, so a peer can reconstruct
    /// the [`tn_types::ConsensusOutput`] without separately fetching its batches.
    ///
    /// The output is streamed (rather than returned via request/response) because a single
    /// output can exceed the request/response codec's message size limit.
    StreamConsensusOutput {
        /// The consensus chain number being requested.
        number: u64,
    },
    /// Request to stream a verifiable PREFIX of an epoch's pack file, stopping after the consensus
    /// output with `last_consensus_number`.
    ///
    /// Unlike [`Self::StreamEpoch`] (which streams a complete epoch), this streams the in-progress
    /// current epoch up to an already-committed, verifiable point. The number is a chain consensus
    /// header number, not a pack-relative index.
    StreamEpochPartial {
        /// The epoch we are requesting consensus data for.
        epoch: Epoch,
        /// The final (inclusive) consensus header number to stream up to.
        last_consensus_number: u64,
    },
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
                let containers = roaring_container_count(serialized)?;
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

/// Cookie prefixing a serialized [`RoaringBitmap`] with no run containers: a `u32` container count
/// follows it. (roaring 0.10.12, `bitmap/serialization.rs`.)
const ROARING_SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
/// Cookie (low 16 bits) prefixing a serialized [`RoaringBitmap`] that may contain run containers:
/// the container count minus one is packed into the high 16 bits of the cookie word.
const ROARING_SERIAL_COOKIE: u32 = 12347;

/// Number of containers declared in a serialized [`RoaringBitmap`] header, read without
/// materializing the bitmap.
///
/// Roaring's portable format opens with either [`ROARING_SERIAL_COOKIE_NO_RUNCONTAINER`] followed
/// by a `u32` count or [`ROARING_SERIAL_COOKIE`] with `count - 1` packed into the cookie word's
/// high 16 bits; this reads only that count so a caller can bound a bitmap's size before allocating
/// its containers. Mirrors `RoaringBitmap::deserialize_from`. Errors on a truncated header or an
/// unrecognized cookie (both of which `deserialize_from` would also reject).
fn roaring_container_count(serialized: &[u8]) -> PrimaryNetworkResult<u64> {
    let le_u32 = |start: usize| -> PrimaryNetworkResult<u32> {
        serialized
            .get(start..start.saturating_add(4))
            .and_then(|word| <[u8; 4]>::try_from(word).ok())
            .map(u32::from_le_bytes)
            .ok_or_else(|| {
                PrimaryNetworkError::InvalidRequest("skip_rounds bitmap header truncated".into())
            })
    };

    let cookie = le_u32(0)?;
    if cookie == ROARING_SERIAL_COOKIE_NO_RUNCONTAINER {
        le_u32(4).map(u64::from)
    } else if cookie & 0xFFFF == ROARING_SERIAL_COOKIE {
        Ok(u64::from(cookie >> 16) + 1)
    } else {
        Err(PrimaryNetworkError::InvalidRequest(
            "skip_rounds bitmap has an unrecognized roaring cookie".into(),
        ))
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
    /// Response to a stream-based request (epoch pack or single consensus output).
    ///
    /// If `ack` is true, the requestor should open a stream with the
    /// request digest in the header. The responder will send the requested
    /// data over that stream.
    StreamRequestAck {
        /// Whether the request is accepted.
        ack: bool,
    },
}

impl PrimaryResponse {
    /// Helper method if the response is an error.
    pub fn is_err(&self) -> bool {
        matches!(self, PrimaryResponse::Error(_))
    }

    pub(crate) fn into_error_ref(error: &PrimaryNetworkError) -> Self {
        match error {
            PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch { ours, theirs })
                if *theirs == ours + 1 =>
            {
                // This is a common race condition on epoch restart so report as recoverable.
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
            | PrimaryNetworkError::Timeout(_)
            | PrimaryNetworkError::UnknownStreamRequest(_)
            | PrimaryNetworkError::StreamUnavailable(_)
            | PrimaryNetworkError::ConsensusChainError(_)
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
