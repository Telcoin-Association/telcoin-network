//! Validated, bounded operator connectivity snapshots shared by all node swarms.

use super::{NetworkConfig, TrustedNodeConfigError};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::File,
    io::Read,
    path::Path,
};
use tn_types::{BlsPublicKey, BootstrapServer, P2pNode, WorkerId};

/// Maximum YAML input accepted for a connectivity reload, including startup-only fields.
pub const MAX_PEER_POLICY_FILE_BYTES: usize = 1024 * 1024;

/// Number of process-lifetime worker swarms, fixed at startup.
#[derive(Clone, Copy, Debug)]
pub struct PolicyWorkerCount(usize);

impl From<usize> for PolicyWorkerCount {
    /// Capture the configured worker count at the node boundary.
    fn from(count: usize) -> Self {
        Self(count)
    }
}

/// Maximum operator-owned BLS identities per swarm, fixed at startup.
#[derive(Clone, Copy, Debug)]
pub struct PolicyPeerLimit(usize);

impl From<usize> for PolicyPeerLimit {
    /// Capture the startup peer population budget.
    fn from(limit: usize) -> Self {
        Self(limit)
    }
}

/// Complete configuration-owned hints for one swarm, independent of committee grants.
#[derive(Clone, Debug, Default)]
pub struct SwarmPeerPolicy {
    /// Hubs entitled to admission, retention, mesh and load-scoring protection.
    trusted: BTreeMap<BlsPublicKey, P2pNode>,
    /// Discovery hints entitled to admission, with ordinary retention and load scoring.
    bootstrap: BTreeMap<BlsPublicKey, P2pNode>,
}

impl SwarmPeerPolicy {
    /// Return this swarm's trusted endpoints.
    pub fn trusted(&self) -> &BTreeMap<BlsPublicKey, P2pNode> {
        &self.trusted
    }

    /// Return this swarm's bootstrap endpoints.
    pub fn bootstrap(&self) -> &BTreeMap<BlsPublicKey, P2pNode> {
        &self.bootstrap
    }

    /// Whether this policy owns reconnect work for a BLS identity.
    pub fn contains(&self, key: &BlsPublicKey) -> bool {
        self.trusted.contains_key(key) || self.bootstrap.contains_key(key)
    }
}

/// One validated operator snapshot for the primary and every configured worker.
#[derive(Clone, Debug)]
pub struct OperatorPeerPolicy {
    /// Primary-only endpoints.
    primary: SwarmPeerPolicy,
    /// Worker-only endpoints indexed by the local worker identity.
    workers: BTreeMap<WorkerId, SwarmPeerPolicy>,
}

impl OperatorPeerPolicy {
    /// Return the primary swarm's endpoints.
    pub fn primary(&self) -> &SwarmPeerPolicy {
        &self.primary
    }

    /// Return one worker's endpoints without borrowing another worker's identity.
    pub fn worker(&self, id: WorkerId) -> Option<&SwarmPeerPolicy> {
        self.workers.get(&id)
    }

    /// Whether a node-owned reconnect schedule covers this BLS identity.
    pub fn contains(&self, key: &BlsPublicKey) -> bool {
        self.primary.contains(key)
    }
}

/// A reload rejected before any consumer receives a new connectivity snapshot.
#[derive(Debug)]
pub enum PeerPolicyConfigError {
    /// The operator-owned file could not be read.
    Read(std::io::Error),
    /// The file exceeds the documented byte budget.
    FileTooLarge,
    /// The complete YAML document could not be decoded.
    Decode(serde_yaml::Error),
    /// An endpoint or identity binding is contradictory or incomplete.
    Invalid(TrustedNodeConfigError),
    /// The endpoint union exceeds the startup peer population budget.
    TooManyPeers {
        /// Startup target shared by initial installation and all reloads.
        limit: usize,
    },
}

impl PeerPolicyConfigError {
    /// Return a finite, key-free rejection category for logs and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::FileTooLarge => "file_size",
            Self::Decode(_) => "decode",
            Self::Invalid(_) => "identity",
            Self::TooManyPeers { .. } => "peer_count",
        }
    }
}

impl fmt::Display for PeerPolicyConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(f, "cannot read peer policy: {error}"),
            Self::FileTooLarge => {
                write!(f, "peer policy file exceeds {MAX_PEER_POLICY_FILE_BYTES} bytes")
            }
            Self::Decode(error) => write!(f, "cannot decode peer policy: {error}"),
            Self::Invalid(error) => error.fmt(f),
            Self::TooManyPeers { limit } => {
                write!(f, "peer policy exceeds startup target_num_peers ({limit})")
            }
        }
    }
}

impl std::error::Error for PeerPolicyConfigError {}

impl NetworkConfig {
    /// Validate the entire connectivity policy before creating a snapshot.
    ///
    /// CLI precedence, genesis fallback, worker count and peer budget stay fixed for this process.
    /// Other network settings remain startup-only.
    pub fn operator_peer_policy(
        &self,
        genesis: &BTreeMap<BlsPublicKey, BootstrapServer>,
        cli: Option<&BTreeMap<BlsPublicKey, BootstrapServer>>,
        num_workers: PolicyWorkerCount,
        peer_limit: PolicyPeerLimit,
    ) -> Result<OperatorPeerPolicy, PeerPolicyConfigError> {
        let PolicyWorkerCount(num_workers) = num_workers;
        let PolicyPeerLimit(peer_limit) = peer_limit;
        let bootstrap = self.resolve_bootstrap_peers(genesis, cli);
        self.validate_trusted_nodes(&bootstrap, num_workers)
            .map_err(PeerPolicyConfigError::Invalid)?;
        let primary = SwarmPeerPolicy {
            trusted: self
                .trusted_nodes()
                .iter()
                .map(|(key, node)| (*key, node.primary().clone()))
                .collect(),
            bootstrap: bootstrap.iter().map(|(key, node)| (*key, node.primary.clone())).collect(),
        };
        let count =
            primary.bootstrap.keys().chain(primary.trusted.keys()).collect::<BTreeSet<_>>().len();
        (count <= peer_limit)
            .then_some(())
            .ok_or(PeerPolicyConfigError::TooManyPeers { limit: peer_limit })?;
        let workers = (0..num_workers)
            .map(|id| {
                let id = WorkerId::try_from(id).map_err(|_| {
                    PeerPolicyConfigError::Invalid(TrustedNodeConfigError::InvalidField {
                        field: "workers".to_string(),
                        reason: "local worker count exceeds the worker identity range".to_string(),
                    })
                })?;
                Ok((
                    id,
                    SwarmPeerPolicy {
                        trusted: self.trusted_worker_peers(id),
                        bootstrap: bootstrap
                            .iter()
                            .filter_map(|(key, node)| {
                                node.worker(id).cloned().map(|worker| (*key, worker))
                            })
                            .collect(),
                    },
                ))
            })
            .collect::<Result<_, PeerPolicyConfigError>>()?;
        Ok(OperatorPeerPolicy { primary, workers })
    }

    /// Read a bounded reload file without creating a missing file or defaulting it.
    ///
    /// Write a sibling file and atomically rename it into place before SIGHUP.
    pub fn read_operator_peer_policy(
        path: &Path,
        genesis: &BTreeMap<BlsPublicKey, BootstrapServer>,
        cli: Option<&BTreeMap<BlsPublicKey, BootstrapServer>>,
        num_workers: PolicyWorkerCount,
        peer_limit: PolicyPeerLimit,
    ) -> Result<OperatorPeerPolicy, PeerPolicyConfigError> {
        std::fs::metadata(path)
            .map_err(PeerPolicyConfigError::Read)?
            .is_file()
            .then_some(())
            .ok_or_else(|| {
                PeerPolicyConfigError::Read(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "peer policy must be a regular file",
                ))
            })?;
        let file = File::open(path).map_err(PeerPolicyConfigError::Read)?;
        let limit = u64::try_from(MAX_PEER_POLICY_FILE_BYTES)
            .map_err(|_| PeerPolicyConfigError::FileTooLarge)?;
        let mut bytes = Vec::new();
        file.take(limit + 1).read_to_end(&mut bytes).map_err(PeerPolicyConfigError::Read)?;
        (bytes.len() <= MAX_PEER_POLICY_FILE_BYTES)
            .then_some(())
            .ok_or(PeerPolicyConfigError::FileTooLarge)?;
        let config: Self = serde_yaml::from_slice(&bytes).map_err(PeerPolicyConfigError::Decode)?;
        config.operator_peer_policy(genesis, cli, num_workers, peer_limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TrustedNode;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::{BlsKeypair, NetworkKeypair};

    /// Construct an independent endpoint for policy binding tests.
    fn endpoint(seed: u8) -> eyre::Result<(BlsPublicKey, P2pNode)> {
        Ok((
            *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public(),
            P2pNode {
                network_key: NetworkKeypair::generate_ed25519().public().into(),
                network_address: "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?,
                rpc: None,
            },
        ))
    }

    /// The same validated revision contains the primary and exactly the configured workers.
    #[test]
    fn peer_policy_workers_and_cli_precedence() -> eyre::Result<()> {
        let (hub_key, primary) = endpoint(1)?;
        let (_, zero) = endpoint(2)?;
        let (_, one) = endpoint(3)?;
        let (bootstrap_key, bootstrap_primary) = endpoint(4)?;
        let bootstrap = BTreeMap::from([(
            bootstrap_key,
            BootstrapServer {
                primary: bootstrap_primary.clone(),
                workers: vec![zero.clone(), one.clone()],
            },
        )]);
        let config = NetworkConfig {
            trusted_nodes: BTreeMap::from([(
                hub_key,
                TrustedNode::new(
                    primary.clone(),
                    BTreeMap::from([(0, zero.clone()), (1, one.clone())]),
                ),
            )]),
            ..Default::default()
        };
        // Bootstrap worker identities must not claim another BLS identity's trusted endpoints.
        assert!(config
            .operator_peer_policy(&bootstrap, None, 2usize.into(), 2usize.into())
            .is_err());
        let (_, boot_zero) = endpoint(5)?;
        let (_, boot_one) = endpoint(6)?;
        let cli = BTreeMap::from([(
            bootstrap_key,
            BootstrapServer {
                primary: bootstrap_primary,
                workers: vec![boot_zero.clone(), boot_one.clone()],
            },
        )]);
        let policy = config.operator_peer_policy(
            &BTreeMap::new(),
            Some(&cli),
            2usize.into(),
            2usize.into(),
        )?;
        assert_eq!(policy.primary().trusted().get(&hub_key), Some(&primary));
        assert_eq!(policy.worker(0).and_then(|worker| worker.trusted().get(&hub_key)), Some(&zero));
        assert_eq!(policy.worker(1).and_then(|worker| worker.trusted().get(&hub_key)), Some(&one));
        assert_eq!(
            policy.worker(1).and_then(|worker| worker.bootstrap().get(&bootstrap_key)),
            Some(&boot_one)
        );
        assert!(policy.worker(2).is_none());
        assert_eq!(policy.primary().bootstrap().len(), 1);
        assert!(config
            .operator_peer_policy(&BTreeMap::new(), Some(&cli), 1usize.into(), 2usize.into())
            .is_err());
        assert!(matches!(
            config.operator_peer_policy(&BTreeMap::new(), Some(&cli), 2usize.into(), 1usize.into()),
            Err(PeerPolicyConfigError::TooManyPeers { limit: 1 })
        ));
        let mut malformed = primary;
        malformed.rpc =
            Some(tn_types::RpcInfo { http: "ftp://hub.example.com/".parse()?, ws: None });
        let invalid_rpc = NetworkConfig {
            trusted_nodes: BTreeMap::from([(
                hub_key,
                TrustedNode::new(malformed, BTreeMap::from([(0, zero), (1, one)])),
            )]),
            ..Default::default()
        };
        assert!(matches!(
            invalid_rpc.operator_peer_policy(&BTreeMap::new(), None, 2usize.into(), 2usize.into()),
            Err(PeerPolicyConfigError::Invalid(_))
        ));
        Ok(())
    }

    /// Only fully decoded, bounded regular files can produce a policy snapshot.
    #[test]
    fn peer_policy_file_faults_and_exact_byte_boundary() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("network-config");
        let read = || {
            NetworkConfig::read_operator_peer_policy(
                &path,
                &BTreeMap::new(),
                None,
                2usize.into(),
                2usize.into(),
            )
        };
        assert!(matches!(read(), Err(PeerPolicyConfigError::Read(_))));
        std::fs::write(&path, "trusted_nodes: [")?;
        assert!(matches!(read(), Err(PeerPolicyConfigError::Decode(_))));
        std::fs::write(&path, "trusted_nods: {}\n")?;
        assert!(matches!(read(), Err(PeerPolicyConfigError::Decode(_))));
        std::fs::write(&path, vec![b' '; MAX_PEER_POLICY_FILE_BYTES + 1])?;
        assert!(matches!(read(), Err(PeerPolicyConfigError::FileTooLarge)));
        let mut exact = b"{}\n".to_vec();
        exact.resize(MAX_PEER_POLICY_FILE_BYTES, b' ');
        std::fs::write(&path, exact)?;
        let accepted = read()?;
        assert!(accepted.primary().trusted().is_empty());
        assert!(accepted.worker(0).is_some());
        assert!(accepted.worker(1).is_some());
        Ok(())
    }
}
