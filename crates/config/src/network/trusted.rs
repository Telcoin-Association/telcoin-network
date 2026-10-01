//! Trusted hubs and validation of their per-swarm identity bindings.

use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};
use tn_types::{BlsPublicKey, BootstrapServer, P2pNode, WorkerId};

/// A hub's transport identities and address hints, keyed by worker ID.
///
/// Every supported worker ID must be present. The BLS key is the containing map's key.
/// Transport authentication and signed records still apply.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TrustedNode {
    /// The endpoint used only by the primary swarm.
    primary: P2pNode,
    /// Endpoints used only by the swarm with the matching worker ID.
    workers: BTreeMap<WorkerId, P2pNode>,
}

impl TrustedNode {
    /// Construct a hub entry; startup validates coverage and identity bindings.
    pub fn new(primary: P2pNode, workers: BTreeMap<WorkerId, P2pNode>) -> Self {
        Self { primary, workers }
    }

    /// Return the primary endpoint.
    pub fn primary(&self) -> &P2pNode {
        &self.primary
    }

    /// Return the endpoint for exactly this worker ID.
    pub fn worker(&self, worker_id: WorkerId) -> Option<&P2pNode> {
        self.workers.get(&worker_id)
    }
}

/// An actionable configuration failure, naming the contradictory entry.
#[derive(Debug)]
pub enum TrustedNodeConfigError {
    /// A configured endpoint or worker set violates a startup invariant.
    InvalidField {
        /// The configuration field requiring correction.
        field: String,
        /// The invariant violated by the entry.
        reason: String,
    },
}

impl fmt::Display for TrustedNodeConfigError {
    /// Name the configuration field and the identity or worker invariant that failed.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField { field, reason } => write!(formatter, "{field}: {reason}"),
        }
    }
}

impl std::error::Error for TrustedNodeConfigError {}

/// Validate all entries before any swarm or dial work is created.
pub(super) fn validate(
    trusted: &BTreeMap<BlsPublicKey, TrustedNode>,
    bootstrap: &BTreeMap<BlsPublicKey, BootstrapServer>,
    num_workers: usize,
) -> Result<(), TrustedNodeConfigError> {
    trusted.iter().try_for_each(|(bls, node)| {
        if node.workers.keys().copied().map(usize::from).eq(0..num_workers) {
            Ok(())
        } else {
            Err(TrustedNodeConfigError::InvalidField {
                field: format!("trusted_nodes[{bls}].workers"),
                reason: format!(
                    "expected every worker ID in 0..{num_workers}, found {:?}",
                    node.workers.keys()
                ),
            })
        }
    })?;

    let mut by_bls = BTreeMap::new();
    let mut by_peer = BTreeMap::new();
    let bootstrap_endpoints = bootstrap.iter().flat_map(|(bls, node)| {
        std::iter::once((None, bls, &node.primary, format!("bootstrap_peers[{bls}].primary")))
            .chain(node.workers.iter().enumerate().map(move |(id, endpoint)| {
                (Some(id), bls, endpoint, format!("bootstrap_peers[{bls}].workers[{id}]"))
            }))
    });
    let trusted_endpoints = trusted.iter().flat_map(|(bls, node)| {
        std::iter::once((None, bls, &node.primary, format!("trusted_nodes[{bls}].primary"))).chain(
            node.workers.iter().map(move |(id, endpoint)| {
                (
                    Some(usize::from(*id)),
                    bls,
                    endpoint,
                    format!("trusted_nodes[{bls}].workers[{id}]"),
                )
            }),
        )
    });
    bootstrap_endpoints.chain(trusted_endpoints).try_for_each(|(swarm, bls, node, field)| {
        let peer: PeerId = node.network_key.clone().into();
        let address_peer = node
            .network_address
            .iter()
            .last()
            .map(|protocol| protocol.to_string())
            .filter(|protocol| protocol.starts_with("/p2p/"));
        let reason = [
            address_peer
                .is_some_and(|id| id != format!("/p2p/{peer}"))
                .then(|| format!("address /p2p identity must match network key {peer}")),
            by_bls
                .insert((swarm, *bls), peer)
                .is_some_and(|previous| previous != peer)
                .then(|| format!("BLS key {bls} has contradictory PeerId bindings in this swarm")),
            by_peer
                .insert(peer, *bls)
                .is_some_and(|previous| previous != *bls)
                .then(|| format!("PeerId {peer} is assigned to multiple BLS keys")),
        ]
        .into_iter()
        .flatten()
        .next();
        reason.map_or(Ok(()), |reason| Err(TrustedNodeConfigError::InvalidField { field, reason }))
    })
}
