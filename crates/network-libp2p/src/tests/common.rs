//! Fixtures used in multiple tests.

use crate::{peers::GLOBAL_SCORE_CONFIG, PeerExchangeMap, TNMessage};
use libp2p::Multiaddr;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{Arc, Once},
};
use tn_config::ScoreConfig;
use tn_types::{BlockHash, Certificate, CertificateDigest, Header, SealedBatch, Vote};

/// Default heartbeat for tests.
#[allow(dead_code)] // used in network_tests.rs
pub(crate) const TEST_HEARTBEAT_INTERVAL: u64 = 1;

// ensure `init_peer_score_config` is only set once
static INIT: Once = Once::new();

// allow dead code due to compile warning that this fn is never used
// but it is used in `all_peers` and `banned_peers`
/// Initialize without error for unit tests.
#[allow(dead_code)]
pub(crate) fn ensure_score_config(config: Option<ScoreConfig>) {
    INIT.call_once(|| {
        // ignore result
        let _ = GLOBAL_SCORE_CONFIG.set(Arc::new(config.unwrap_or_default()));
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

/// Helper function to create a random IPV4 address.
#[allow(dead_code)]
pub(crate) fn random_ip_addr() -> IpAddr {
    let mut rng = rand::rng();
    // random between IPv4 and IPv6 (80% v4, 20% v6)
    if rng.random_bool(0.8) {
        // random IPv4
        let a = rng.random_range(1..255);
        let b = rng.random_range(0..255);
        let c = rng.random_range(0..255);
        let d = rng.random_range(1..255);
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    } else {
        // random IPv6
        let random: Vec<u16> = [(); 8].iter().map(|_| rng.random_range(0..255)).collect();
        IpAddr::V6(Ipv6Addr::new(
            random[0], random[1], random[2], random[3], random[4], random[5], random[6], random[7],
        ))
    }
}

/// Helper function to create a [Multiaddr] for tests.
#[allow(dead_code)]
pub(crate) fn create_multiaddr(ip: Option<IpAddr>) -> Multiaddr {
    let ip = ip.unwrap_or_else(random_ip_addr);
    let ip = match ip {
        IpAddr::V4(ip) => format!("/ip4/{ip}"),
        IpAddr::V6(ip) => format!("/ip6/{ip}"),
    };
    format!("{}/tcp/8000", &ip).parse().unwrap()
}
