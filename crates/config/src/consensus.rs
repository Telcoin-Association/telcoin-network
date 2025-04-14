//! Configuration for consensus network (primary and worker).
use crate::{
    Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, Parameters, TelcoinDirs,
};
use libp2p::PeerId;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tn_network_types::local::LocalNetwork;
use tn_types::{
    Authority, AuthorityIdentifier, Certificate, CertificateDigest, Committee, Database, Hash as _,
    Multiaddr, Notifier, WorkerCache, WorkerId,
};
use tracing::info;

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    node_storage: DB,
    key_config: KeyConfig,
    authority: Authority,
    local_network: LocalNetwork,
    network_config: NetworkConfig,
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
        network_config: NetworkConfig,
    ) -> eyre::Result<Self> {
        // load committee from file
        let committee: Committee =
            Config::load_from_path(tn_datadir.committee_path(), ConfigFmt::YAML)?;
        committee.load();
        info!(target: "telcoin", "committee loaded");
        let worker_cache: WorkerCache =
            Config::load_from_path(tn_datadir.worker_cache_path(), ConfigFmt::YAML)?;

        info!(target: "telcoin", "worker cache loaded");
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            worker_cache,
            network_config,
        )
    }

    /// Create a new config with a committe.
    ///
    /// The method is exposed publicly for testing ONLY.
    pub fn new_with_committee_for_test(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        worker_cache: WorkerCache,
        network_config: NetworkConfig,
    ) -> eyre::Result<Self> {
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            worker_cache,
            network_config,
        )
    }

    /// Create a new config with a committe.
    ///
    /// This should only be called by `Self::new` or by the testing wrapper.
    fn new_with_committee(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        worker_cache: WorkerCache,
        network_config: NetworkConfig,
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

    pub fn local_network(&self) -> &LocalNetwork {
        &self.inner.local_network
    }

    pub fn network_config(&self) -> &NetworkConfig {
        &self.inner.network_config
    }

    /// Committee network peer ids.
    pub fn committee_peer_ids(&self) -> HashSet<PeerId> {
        self.inner.committee.authorities().iter().map(|a| a.peer_id()).collect()
    }

    /// Map of primary peer ids and multiaddrs in the current committee.
    pub fn primary_network_map(&self) -> HashMap<PeerId, Multiaddr> {
        self.inner
            .committee
            .authorities()
            .iter()
            .map(|a| (a.peer_id(), a.primary_network_address().clone()))
            .collect()
    }

    /// Bool indicating if an authority identifier is in the current committee.
    pub fn in_committee(&self, id: &AuthorityIdentifier) -> bool {
        self.inner.committee.is_authority(id)
    }

    /// Retrieve the worker's network address by id.
    /// Note, will panic if id is not valid (not found in our worker cache).
    pub fn worker_address(&self, id: &WorkerId) -> Multiaddr {
        self.worker_cache()
            .worker(self.authority().protocol_key(), id)
            .expect("Our public key or worker id is not in the worker cache")
            .worker_address
    }

    /// Map of worker peer ids and multiaddrs in the current committee.
    pub fn worker_network_map(&self) -> HashMap<PeerId, Multiaddr> {
        self.worker_cache().all_workers()
    }
}
