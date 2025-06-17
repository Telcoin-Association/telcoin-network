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
    Authority, AuthorityIdentifier, Certificate, CertificateDigest, Committee, Database, Epoch,
    Hash as _, Multiaddr, Notifier, WorkerCache, WorkerId,
};
use tracing::info;

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    node_storage: DB,
    key_config: KeyConfig,
    authority: Option<Authority>,
    local_network: LocalNetwork,
    network_config: NetworkConfig,
    genesis: HashMap<CertificateDigest, Certificate>,
}

/// The configuration for consensus.
///
/// This structure holds all necessary configuration data for both primary and worker
/// consensus components. It manages committee membership, cryptographic keys, network
/// topology, and genesis state required for consensus participation.
///
/// The configuration is designed to be shared across consensus components and provides
/// both authority-specific and network-wide configuration access.
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
    /// Creates a new consensus configuration by loading committee and worker cache from disk.
    ///
    /// This is the primary constructor that loads configuration from the filesystem,
    /// including committee membership and worker topology from YAML files.
    pub fn new<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: &TND,
        node_storage: DB,
        key_config: KeyConfig,
        network_config: NetworkConfig,
    ) -> eyre::Result<Self> {
        // load committee from file
        let committee: Committee =
            Config::load_from_path_or_default(tn_datadir.committee_path(), ConfigFmt::YAML)?;
        committee.load();
        info!(target: "telcoin", "committee loaded");
        let worker_cache: WorkerCache =
            Config::load_from_path_or_default(tn_datadir.worker_cache_path(), ConfigFmt::YAML)?;

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

    /// Creates a new configuration with a pre-loaded committee for testing purposes.
    ///
    /// **WARNING: This method is exposed publicly for testing ONLY.**
    /// Production code should use `new()` or `new_for_epoch()` to ensure proper configuration
    /// loading.
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

    /// Creates configuration for the next consensus epoch.
    ///
    /// This constructor is used during epoch transitions to initialize configuration
    /// with updated committee membership and worker topology for the new epoch.
    pub fn new_for_epoch(
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

    /// Internal constructor that initializes consensus configuration with provided committee.
    ///
    /// This method performs the core initialization logic including:
    /// - Setting up local network identity
    /// - Resolving authority status within the committee
    /// - Creating genesis certificates
    /// - Initializing shutdown notification system
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
        let authority = committee.authority_by_key(&primary_public_key);

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

    /// Returns a reference to the shutdown notifier.
    ///
    /// The shutdown notifier can be used to either subscribe to shutdown events
    /// or trigger shutdown across consensus components.
    pub fn shutdown(&self) -> &Notifier {
        &self.shutdown
    }

    /// Returns a reference to the inner config parameters.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Returns a reference to the genesis certificate collection.
    ///
    /// Genesis certificates establish the initial state and authority set
    /// for the consensus protocol to produce the first primary `Header`.
    pub fn genesis(&self) -> &HashMap<CertificateDigest, Certificate> {
        &self.inner.genesis
    }

    /// Returns a reference to the current committee membership.
    ///
    /// The committee defines the set of authorities participating in consensus
    /// for the current epoch.
    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    /// Returns a reference to the worker cache.
    ///
    /// The worker cache contains network topology information for all worker
    /// processes across the committee.
    pub fn worker_cache(&self) -> &WorkerCache {
        &self.worker_cache
    }

    /// Returns a reference to the node's persistent storage database for the current epoch.
    pub fn node_storage(&self) -> &DB {
        &self.inner.node_storage
    }

    /// Returns a reference to the cryptographic key configuration.
    ///
    /// Contains both primary and worker cryptographic keys used for
    /// consensus participation and network communication.
    pub fn key_config(&self) -> &KeyConfig {
        &self.inner.key_config
    }

    /// Returns the authority information for this node. Optional if it is a committee member or
    /// not.
    pub fn authority(&self) -> &Option<Authority> {
        &self.inner.authority
    }

    /// Returns the authority identifier for this node, if it is a committee member.
    pub fn authority_id(&self) -> Option<AuthorityIdentifier> {
        self.inner.authority.as_ref().map(|a| a.id())
    }

    /// Returns a reference to the consensus protocol parameters.
    ///
    /// Parameters include timing constraints, batch sizes, and other
    /// protocol-specific configuration values.
    pub fn parameters(&self) -> &Parameters {
        &self.inner.config.parameters
    }

    /// Returns a reference to the local network configuration.
    ///
    /// Contains network identity and local networking setup information.
    /// This is how Primary <-> Workers communicate.
    pub fn local_network(&self) -> &LocalNetwork {
        &self.inner.local_network
    }

    /// Returns a reference to the network configuration.
    ///
    /// Contains p2p settings and connectivity parameters for libp2p.
    pub fn network_config(&self) -> &NetworkConfig {
        &self.inner.network_config
    }

    pub fn epoch(&self) -> Epoch {
        self.inner.committee.epoch()
    }

    /// Committee network peer ids.
    pub fn committee_peer_ids(&self) -> HashSet<PeerId> {
        self.inner.committee.authorities().iter().map(|a| a.peer_id()).collect()
    }

    /// Retrieve the primaries network address.
    /// Note if this node is not an authority, retrieve the next available udp port assigned by the
    /// system.
    pub fn primary_address(&self) -> Multiaddr {
        if let Some(authority) = self.authority() {
            authority.primary_network_address().clone()
        } else {
            let host = std::env::var("TN_PRIMARY_HOST").unwrap_or("0.0.0.0".to_string());
            let primary_udp_port = std::env::var("TN_PRIMARY_PORT").unwrap_or_else(|_| {
                tn_types::get_available_udp_port(&host).unwrap_or(49584).to_string()
            });
            format!("/ip4/{}/udp/{}/quic-v1", &host, primary_udp_port)
                .parse()
                .expect("multiaddr parsed for primary consensus")
        }
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
    /// Note, will panic if id is not valid (not found in our worker cache) and the node is an
    /// authority. Otherwise, retrieve the next available udp port assigned by the system.
    pub fn worker_address(&self, id: &WorkerId) -> Multiaddr {
        if let Some(authority) = self.authority() {
            self.worker_cache()
                .worker(authority.protocol_key(), id)
                .expect("Our public key or worker id is not in the worker cache")
                .worker_address
        } else {
            let host = std::env::var("TN_WORKER_HOST").unwrap_or("0.0.0.0".to_string());
            let worker_udp_port = std::env::var("TN_WORKER_PORT").unwrap_or_else(|_| {
                tn_types::get_available_udp_port(&host).unwrap_or(49594).to_string()
            });
            format!("/ip4/{}/udp/{}/quic-v1", &host, worker_udp_port)
                .parse()
                .expect("multiaddr parsed for worker consensus")
        }
    }

    /// Map of worker peer ids and multiaddrs in the current committee.
    pub fn worker_network_map(&self) -> HashMap<PeerId, Multiaddr> {
        self.worker_cache().all_workers()
    }
}
