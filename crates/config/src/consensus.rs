//! Configuration for consensus network (primary and worker).
use crate::{
    Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, Parameters, TelcoinDirs,
};
use fastcrypto::hash::Hash as _;
use libp2p::PeerId;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tn_network_types::local::LocalNetwork;
use tn_types::{
    network_public_key_to_libp2p, Authority, AuthorityIdentifier, Certificate, CertificateDigest,
    Committee, Database, Multiaddr, Notifier, WorkerCache, WorkerId,
};

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    node_storage: DB,
    key_config: KeyConfig,
    authority: Authority,
    local_network: LocalNetwork,
    network_config: NetworkConfig,
    authority_map: AuthorityMapping,
    genesis: HashMap<CertificateDigest, Certificate>,
}

#[derive(Debug, Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
    worker_cache: WorkerCache,
    shutdown: Notifier,
}

impl<DB> ConsensusConfig<DB>
where
    DB: Database,
{
    pub fn new<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: &TND,
        node_storage: DB,
        key_config: KeyConfig,
    ) -> eyre::Result<Self> {
        // load committee from file
        let mut committee: Committee =
            Config::load_from_path(tn_datadir.committee_path(), ConfigFmt::YAML)?;
        committee.load();
        tracing::info!(target: "telcoin::consensus_config", "committee loaded");
        // TODO: make worker cache part of committee?
        let worker_cache: WorkerCache =
            Config::load_from_path(tn_datadir.worker_cache_path(), ConfigFmt::YAML)?;
        // TODO: this could be a separate method on `Committee` to have robust checks in place
        // - all public keys are unique
        // - thresholds / stake
        //
        // assert committee loaded correctly
        // assert!(committee.size() >= 4, "not enough validators in committee.");

        // TODO: better assertion here
        // right now, each validator should only have 1 worker
        // this assertion would incorrectly pass if 1 authority had 2 workers and another had 0
        //
        // assert worker cache loaded correctly
        assert!(
            worker_cache.all_workers().len() == committee.size(),
            "each validator within committee must have one worker"
        );

        tracing::info!(target: "telcoin::consensus_config", "worker cache loaded");
        Self::new_with_committee(config, node_storage, key_config, committee, worker_cache)
    }

    /// Create a new config with a committe.
    ///
    /// This should only be called by `Self::new`.
    /// The method is exposed publicly for testing ONLY.
    pub fn new_with_committee(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        worker_cache: WorkerCache,
    ) -> eyre::Result<Self> {
        let local_network =
            LocalNetwork::new_from_public_key(&key_config.primary_network_public_key());

        let primary_public_key = key_config.primary_public_key();
        let authority = committee
            .authority_by_key(&primary_public_key)
            .unwrap_or_else(|| {
                panic!("Our node with key {:?} should be in committee", primary_public_key)
            })
            .clone();

        let shutdown = Notifier::new();
        let network_config = NetworkConfig::default();
        let authority_map = AuthorityMapping::new(&committee);
        let genesis = Certificate::genesis(&committee)
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();

        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                node_storage,
                key_config,
                authority,
                local_network,
                network_config,
                authority_map,
                genesis,
            }),
            worker_cache,
            shutdown,
        })
    }

    /// Returns a reference to the shutdown Noticer.
    /// Can use this subscribe to the shutdown event or send it.
    pub fn shutdown(&self) -> &Notifier {
        &self.shutdown
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn genesis(&self) -> &HashMap<CertificateDigest, Certificate> {
        &self.inner.genesis
    }

    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    pub fn worker_cache(&self) -> &WorkerCache {
        &self.worker_cache
    }

    pub fn worker_cache_clone(&self) -> WorkerCache {
        self.worker_cache.clone()
    }

    pub fn node_storage(&self) -> &DB {
        &self.inner.node_storage
    }

    pub fn key_config(&self) -> &KeyConfig {
        &self.inner.key_config
    }

    pub fn authority(&self) -> &Authority {
        &self.inner.authority
    }

    pub fn parameters(&self) -> &Parameters {
        &self.inner.config.parameters
    }

    pub fn database(&self) -> &DB {
        &self.inner.node_storage
    }

    pub fn local_network(&self) -> &LocalNetwork {
        &self.inner.local_network
    }

    pub fn network_config(&self) -> &NetworkConfig {
        &self.inner.network_config
    }

    /// Committee network peer ids.
    pub fn committee_peer_ids(&self) -> HashSet<PeerId> {
        self.inner.authority_map.peer_id_to_authority.keys().copied().collect()
    }

    /// Return the libp2p network [PeerId] for an authority.
    pub fn peer_id_for_authority(&self, authority_id: &AuthorityIdentifier) -> Option<PeerId> {
        self.inner.authority_map.authority_to_peer_id.get(authority_id).copied()
    }

    /// Return the [AuthorityIdentifier] for a libp2p network [PeerId].
    pub fn authority_for_peer_id(&self, peer_id: &PeerId) -> Option<AuthorityIdentifier> {
        self.inner.authority_map.peer_id_to_authority.get(peer_id).copied()
    }

    /// Retrieve the worker's network address by id.
    /// Note, will panic if id is not valid (not found in our worker cache).
    pub fn worker_address(&self, id: &WorkerId) -> Multiaddr {
        self.worker_cache()
            .worker(self.authority().protocol_key(), id)
            .expect("Our public key or worker id is not in the worker cache")
            .worker_address
    }
}

/// Authority mappings between authority id (used by consensus) and peer id (used by network).
#[derive(Debug)]
pub struct AuthorityMapping {
    /// Map the [AuthorityIdentifier] to the network [PeerId].
    authority_to_peer_id: HashMap<AuthorityIdentifier, PeerId>,
    /// Map the [PeerId] to the network [AuthorityIdentifier].
    peer_id_to_authority: HashMap<PeerId, AuthorityIdentifier>,
}

impl AuthorityMapping {
    /// Create a new instance of [Self].
    pub fn new(committee: &Committee) -> Self {
        let authority_to_peer_id: HashMap<AuthorityIdentifier, PeerId> = committee
            .authorities()
            .map(|a| {
                let fc = a.network_key();
                let peer_id = network_public_key_to_libp2p(&fc);
                (a.id(), peer_id)
            })
            .collect();

        let peer_id_to_authority =
            authority_to_peer_id.iter().map(|(a, p_id)| (*p_id, *a)).collect();

        Self { authority_to_peer_id, peer_id_to_authority }
    }
}
