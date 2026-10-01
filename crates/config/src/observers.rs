//! Operator inventories and finite connectivity budgets for DAO observer hubs.

use libp2p::{multiaddr::Protocol, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tn_types::{BlsPublicKey, BootstrapServer, P2pNode, WorkerId};

/// Finite established connection allowance for one authenticated peer on one swarm.
pub const MAX_ESTABLISHED_CONNECTIONS_PER_PEER: u32 = 8;

/// One provisioned node's authenticated transport identities and dial addresses.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TrustedNode {
    /// Primary swarm identity and address.
    primary: P2pNode,
    /// Worker identities and addresses keyed by their actual worker IDs.
    workers: BTreeMap<WorkerId, P2pNode>,
}

impl TrustedNode {
    /// Construct a node inventory entry.
    pub fn new(primary: P2pNode, workers: BTreeMap<WorkerId, P2pNode>) -> Self {
        Self { primary, workers }
    }
    /// Primary swarm identity and address.
    pub fn primary(&self) -> &P2pNode {
        &self.primary
    }
    /// Worker-specific identity and address, with no fallback to another worker.
    pub fn worker(&self, id: WorkerId) -> Option<&P2pNode> {
        self.workers.get(&id)
    }
    /// Every explicitly provisioned worker identity.
    pub fn workers(&self) -> &BTreeMap<WorkerId, P2pNode> {
        &self.workers
    }
}

/// A hub deployment profile owned and rolled out by DAO deployment operators.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaoObserverProfile {
    /// DAO observer inventory, keyed by each observer's own BLS public key.
    observers: BTreeMap<BlsPublicKey, TrustedNode>,
    /// Absolute identity budget on each swarm, including ordinary and privileged peers.
    max_peers: u32,
}

impl DaoObserverProfile {
    /// Construct an explicit inventory and measured per-swarm peer budget.
    pub fn new(observers: BTreeMap<BlsPublicKey, TrustedNode>, max_peers: u32) -> Self {
        Self { observers, max_peers }
    }
    /// DAO observers whose connectivity the hub reserves.
    pub fn observers(&self) -> &BTreeMap<BlsPublicKey, TrustedNode> {
        &self.observers
    }
    /// Absolute peer budget, including all reserved observer identities.
    pub fn max_peers(&self) -> u32 {
        self.max_peers
    }
}

/// Actionable failure in an operator inventory or its reserved capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverConfigError {
    /// An inventory entry or its capacity contradicts the deployment requirements.
    InvalidInventory(String),
}
impl std::fmt::Display for ObserverConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInventory(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for ObserverConfigError {}

/// Validate inventory bindings before any network task or listener is started.
pub(crate) fn validate_inventory(
    trusted: &BTreeMap<BlsPublicKey, TrustedNode>,
    profile: Option<&DaoObserverProfile>,
    bootstrap: &BTreeMap<BlsPublicKey, BootstrapServer>,
    workers: impl IntoIterator<Item = WorkerId>,
    ordinary_budget: usize,
) -> Result<(), ObserverConfigError> {
    use ObserverConfigError::InvalidInventory;
    let observers = profile.map(DaoObserverProfile::observers);
    let entries: BTreeMap<_, _> = trusted
        .iter()
        .chain(observers.into_iter().flat_map(|entries| entries.iter()))
        .map(|(key, node)| (*key, node))
        .collect();
    trusted.iter().try_for_each(|(key, node)| {
        if observers.and_then(|entries| entries.get(key)).is_some_and(|other| other != node) {
            Err(InvalidInventory(format!("trusted_nodes and dao_observers disagree for BLS {key}")))
        } else {
            Ok(())
        }
    })?;
    let required_workers: Vec<_> = workers.into_iter().collect();
    entries.iter().try_for_each(|(key, node)| {
        required_workers.iter().try_for_each(|id| {
            node.worker(*id).map(|_| ()).ok_or_else(|| {
                InvalidInventory(format!(
                    "BLS {key} is missing the identity/address for worker {id}"
                ))
            })
        })
    })?;
    // Transport keys cannot identify different BLS principals or different swarms.
    let mut bindings = HashMap::new();
    bootstrap.iter().filter(|_| !entries.is_empty()).flat_map(|(key, node)| {
        std::iter::once((*key, None, &node.primary)).chain(
            node.workers.iter().zip(0..=WorkerId::MAX).map(|(node, id)| (*key, Some(id), node)),
        )
    }).chain(entries.iter().flat_map(|(key, node)| {
        std::iter::once((*key, None, node.primary())).chain(
            node.workers().iter().map(|(id, node)| (*key, Some(*id), node)),
        )
    })).try_for_each(|(key, role, node)| {
        let peer: PeerId = node.network_key.clone().into();
        let mismatched = node.network_address.iter().any(|part| {
            matches!(part, Protocol::P2p(address_peer) if address_peer != peer)
        });
        (node.network_address.is_empty() || mismatched)
            .then(|| InvalidInventory(format!("BLS {key}, swarm {role:?}: address does not match transport identity {peer}")))
            .map_or(Ok(()), Err)?;
        bindings.insert(peer, (key, role)).filter(|old| *old != (key, role))
            .map(|_| InvalidInventory(format!("transport identity {peer} has contradictory BLS/swarm bindings")))
            .map_or(Ok(()), Err)?;
        bootstrap.get(&key).and_then(|entry| role.map_or(Some(&entry.primary), |id| entry.worker(id)))
            .filter(|hint| hint.network_key != node.network_key)
            .map_or(Ok(()), |_| Err(InvalidInventory(format!("bootstrap and operator transport keys disagree for BLS {key}, swarm {role:?}"))))
    })?;
    profile.map_or(Ok(()), |profile| {
        profile.max_peers.checked_mul(MAX_ESTABLISHED_CONNECTIONS_PER_PEER)
            .ok_or_else(|| InvalidInventory("DAO hub connection budget overflows u32".into()))?;
        let minimum = ordinary_budget.checked_add(entries.len())
            .ok_or_else(|| InvalidInventory("peer budget overflow".into()))?;
        if usize::try_from(profile.max_peers).map_or(true, |budget| budget < minimum) {
            Err(InvalidInventory(format!("dao_observers.max_peers must be at least {minimum} (ordinary headroom plus configured identities)")))
        } else { Ok(()) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyConfig, NetworkConfig};
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{BlsKeypair, BlsPublicKey};

    /// Give every swarm a distinct transport key and an explicit address.
    fn inventory() -> (BlsPublicKey, TrustedNode) {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let primary = P2pNode {
            network_address: libp2p::Multiaddr::empty().with(Protocol::Memory(1)),
            network_key: keys.primary_network_public_key(),
            rpc: None,
        };
        let workers = [0, 7]
            .into_iter()
            .map(|id| {
                (
                    id,
                    P2pNode {
                        network_address: libp2p::Multiaddr::empty()
                            .with(Protocol::Memory(u64::from(id) + 2)),
                        network_key: keys.worker_network_public_key(id),
                        rpc: None,
                    },
                )
            })
            .collect();
        (keys.primary_public_key(), TrustedNode::new(primary, workers))
    }

    /// Sparse worker identities survive YAML and JSON and never fall back to worker zero.
    #[test]
    fn dao_profile_round_trip() -> Result<(), String> {
        let (key, node) = inventory();
        let mut config = NetworkConfig::default();
        config.set_dao_observers(Some(DaoObserverProfile::new(
            BTreeMap::from([(key, node.clone())]),
            46,
        )));
        let yaml: NetworkConfig = serde_yaml::from_str(
            &serde_yaml::to_string(&config).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        let json: NetworkConfig =
            serde_json::from_str(&serde_json::to_string(&yaml).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        let saved = json
            .dao_observers()
            .and_then(|profile| profile.observers().get(&key))
            .ok_or("missing observer after round trip")?;
        assert_eq!(saved, &node);
        assert!(saved.worker(1).is_none());
        json.validate_operator_inventory(&BTreeMap::new(), [0, 7])
            .map_err(|error| error.to_string())?;
        let missing = json
            .validate_operator_inventory(&BTreeMap::new(), [0, 1])
            .err()
            .ok_or("missing worker must be rejected")?;
        assert!(missing.to_string().contains("worker 1"));
        Ok(())
    }

    /// All startup consumers validate the effective CLI inventory rather than stale YAML hints.
    #[test]
    fn dao_cli_bootstrap_override_replaces_conflicting_hints() -> Result<(), ObserverConfigError> {
        let (key, node) = inventory();
        let (other_key, other) = inventory();
        let bootstrap = BTreeMap::from([(
            key,
            BootstrapServer {
                primary: other.primary().clone(),
                workers: other.workers().values().cloned().collect(),
            },
        )]);
        let cli = BTreeMap::from([(
            other_key,
            BootstrapServer {
                primary: other.primary().clone(),
                workers: other.workers().values().cloned().collect(),
            },
        )]);
        let mut config = crate::NetworkConfig::default();
        config.set_dao_observers(Some(DaoObserverProfile::new(BTreeMap::from([(key, node)]), 46)));
        config.configure_bootstrap_peers(&bootstrap, None);
        assert!(config.validate_operator_inventory(config.bootstrap_peers(), [0, 7]).is_err());
        let effective = config.configure_bootstrap_peers(&bootstrap, Some(&cli));
        assert_eq!(config.bootstrap_peers(), &effective);
        assert_eq!(effective, cli);
        config.validate_operator_inventory(config.bootstrap_peers(), [0, 7])
    }

    /// Contradictory identities and insufficient headroom fail before networks are started.
    #[test]
    fn dao_profile_rejects_conflicts_and_underprovisioning() -> Result<(), ObserverConfigError> {
        let (key, node) = inventory();
        let (other_key, other) = inventory();
        let entries = BTreeMap::from([(key, node.clone())]);
        let small = DaoObserverProfile::new(entries.clone(), 45);
        assert!(validate_inventory(&BTreeMap::new(), Some(&small), &BTreeMap::new(), [0, 7], 45)
            .is_err());
        let valid = DaoObserverProfile::new(entries, 46);
        validate_inventory(&BTreeMap::new(), Some(&valid), &BTreeMap::new(), [0, 7], 45)?;
        assert!(validate_inventory(
            &BTreeMap::from([(other_key, node.clone())]),
            Some(&valid),
            &BTreeMap::new(),
            [],
            0
        )
        .is_err());
        assert!(validate_inventory(
            &BTreeMap::from([(key, other.clone())]),
            Some(&valid),
            &BTreeMap::new(),
            [],
            0
        )
        .is_err());
        let bootstrap = BTreeMap::from([(
            key,
            BootstrapServer {
                primary: other.primary().clone(),
                workers: other.workers().values().cloned().collect(),
            },
        )]);
        assert!(validate_inventory(&BTreeMap::new(), Some(&valid), &bootstrap, [], 0).is_err());
        let mut wrong_address = node.primary().clone();
        wrong_address
            .network_address
            .push(Protocol::P2p(other.primary().network_key.clone().into()));
        let bad = BTreeMap::from([(key, TrustedNode::new(wrong_address, node.workers().clone()))]);
        assert!(validate_inventory(&bad, None, &BTreeMap::new(), [], 0).is_err());
        Ok(())
    }
}
