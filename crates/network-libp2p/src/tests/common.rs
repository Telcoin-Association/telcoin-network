//! Fixtures used in multiple tests.

use crate::{peers::GLOBAL_SCORE_CONFIG, PeerExchangeMap, TNMessage};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Once};
use tn_config::ScoreConfig;
use tn_types::{BlockHash, Certificate, CertificateDigest, Header, SealedBatch, Vote};

// ensure `init_peer_score_config` is only set once
static INIT: Once = Once::new();

// allow dead code due to compile warning that this fn is never used
// but it is used in `all_peers` and `banned_peers`
/// Initialize without error for unit tests.
#[allow(dead_code)]
pub fn ensure_score_config() {
    INIT.call_once(|| {
        // use default
        let config = ScoreConfig::default();
        // ignore result
        let _ = GLOBAL_SCORE_CONFIG.set(Arc::new(config));
    });
}

// impl TNMessage trait for types
impl TNMessage for TestWorkerRequest {}
impl TNMessage for TestWorkerResponse {}
impl TNMessage for TestPrimaryRequest {}
impl TNMessage for TestPrimaryResponse {}

/// Requests between workers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestWorkerRequest {
    /// Broadcast a newly produced worker block.
    ///
    /// NOTE: expect no response
    NewBatch(SealedBatch),
    /// The collection of missing [BlockHash]es for this peer.
    MissingBatches(Vec<BlockHash>),
    /// Peer exchange.
    PeerExchange(PeerExchangeMap),
}

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestWorkerResponse {
    /// Return the missing blocks requested by the peer.
    ///
    /// but this should be trustless. See `RequestBlocksResponse` message.
    MissingBatches {
        /// The collection of requested blocks.
        batches: Vec<SealedBatch>,
    },
    /// Peer exchange.
    PeerExchange(PeerExchangeMap),
}

/// Requests from Primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestPrimaryRequest {
    NewCertificate { certificate: Certificate },
    Vote { header: Header, parents: Vec<Certificate> },
}

/// Response to primary requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestPrimaryResponse {
    Vote(Vote),
    MissingCertificates(Vec<Certificate>),
    MissingParents(Vec<CertificateDigest>),
}

impl From<PeerExchangeMap> for TestWorkerRequest {
    fn from(map: PeerExchangeMap) -> Self {
        Self::PeerExchange(map)
    }
}

impl From<PeerExchangeMap> for TestWorkerResponse {
    fn from(map: PeerExchangeMap) -> Self {
        Self::PeerExchange(map)
    }
}

impl From<PeerExchangeMap> for TestPrimaryRequest {
    fn from(_: PeerExchangeMap) -> Self {
        unimplemented!()
    }
}

impl From<PeerExchangeMap> for TestPrimaryResponse {
    fn from(_: PeerExchangeMap) -> Self {
        unimplemented!()
    }
}
