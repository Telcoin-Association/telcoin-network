//! Configuration for consensus network (primary and worker).
use anemo::Config as AnemoConfig;
use libp2p::PeerId;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tn_network::local::LocalNetwork;
use tn_storage::{traits::Database, NodeStorage};
use tn_types::{Authority, AuthorityIdentifier, Committee, Notifier, WorkerCache};

use crate::{
    Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, Parameters, TelcoinDirs,
};

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    node_storage: NodeStorage<DB>,
    key_config: KeyConfig,
    authority: Authority,
    local_network: LocalNetwork,
    anemo_config: AnemoConfig,
    network_config: NetworkConfig,
    authority_map: AuthorityMapping,
}

#[derive(Debug, Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
    worker_cache: Option<Arc<WorkerCache>>,
    shutdown: Notifier,
}

impl<DB> ConsensusConfig<DB>
where
    DB: Database,
{
    pub fn new<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: &TND,
        node_storage: NodeStorage<DB>,
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
        Self::new_with_committee(config, node_storage, key_config, committee, Some(worker_cache))
    }

    /// Create a new config with a committe.
    ///
    /// This should only be called by `Self::new`.
    /// The method is exposed publicly for testing ONLY.
    pub fn new_with_committee(
        config: Config,
        node_storage: NodeStorage<DB>,
        key_config: KeyConfig,
        committee: Committee,
        mut worker_cache: Option<WorkerCache>,
    ) -> eyre::Result<Self> {
        let local_network =
            LocalNetwork::new_from_public_key(config.validator_info.primary_network_key());

        let primary_public_key = key_config.primary_public_key();
        let authority = committee
            .authority_by_key(&primary_public_key)
            .unwrap_or_else(|| {
                panic!("Our node with key {:?} should be in committee", primary_public_key)
            })
            .clone();

        let worker_cache = worker_cache.take().map(Arc::new);
        let shutdown = Notifier::new();
        let anemo_config = Self::create_anemo_config();
        let network_config = NetworkConfig::default();
        let authority_map = AuthorityMapping::new(&committee, &network_config);

        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                node_storage,
                key_config,
                authority,
                local_network,
                anemo_config,
                network_config,
                authority_map,
            }),
            worker_cache,
            shutdown,
        })
    }

    /// The configurable variables for anemo p2p network.
    ///
    /// Used for cosensus by both the primary and workers.
    fn create_anemo_config() -> AnemoConfig {
        let mut quic_config = anemo::QuicConfig::default();
        // Allow more concurrent streams for burst activity.
        quic_config.max_concurrent_bidi_streams = Some(10_000);
        // Increase send and receive buffer sizes on the worker, since the worker is
        // responsible for broadcasting and fetching payloads.
        // With 200MiB buffer size and ~500ms RTT, the max throughput ~400MiB.
        quic_config.stream_receive_window = Some(100 << 20);
        quic_config.receive_window = Some(200 << 20);
        quic_config.send_window = Some(200 << 20);
        quic_config.crypto_buffer_size = Some(1 << 20);
        quic_config.socket_receive_buffer_size = Some(20 << 20);
        quic_config.socket_send_buffer_size = Some(20 << 20);
        quic_config.allow_failed_socket_buffer_size_setting = true;
        quic_config.max_idle_timeout_ms = Some(30_000);
        // Enable keep alives every 5s
        quic_config.keep_alive_interval_ms = Some(5_000);
        let mut config = anemo::Config::default();
        config.quic = Some(quic_config);
        // Set the max_frame_size to be 1 GB to work around the issue of there being too many
        // delegation events in the epoch change txn.
        config.max_frame_size = Some(1 << 30);
        // Set a default timeout of 300s for all RPC requests
        config.inbound_request_timeout_ms = Some(300_000);
        config.outbound_request_timeout_ms = Some(300_000);
        config.shutdown_idle_timeout_ms = Some(1_000);
        config.connectivity_check_interval_ms = Some(2_000);
        config.connection_backoff_ms = Some(1_000);
        config.max_connection_backoff_ms = Some(20_000);
        config
    }

    /// Returns a reference to the shutdown Noticer.
    /// Can use this subscribe to the shutdown event or send it.
    pub fn shutdown(&self) -> &Notifier {
        &self.shutdown
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    pub fn worker_cache(&self) -> &WorkerCache {
        self.worker_cache.as_ref().expect("invalid config- missing worker cache!")
    }

    pub fn set_worker_cache(&mut self, worker_cache: WorkerCache) {
        assert!(self.worker_cache.is_none(), "Can not change the working cache on a config!");
        self.worker_cache = Some(Arc::new(worker_cache));
    }

    pub fn node_storage(&self) -> &NodeStorage<DB> {
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
        &self.inner.node_storage.batch_store
    }

    pub fn local_network(&self) -> &LocalNetwork {
        &self.inner.local_network
    }

    pub fn anemo_config(&self) -> &AnemoConfig {
        &self.inner.anemo_config
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
    pub fn new(committee: &Committee, network_config: &NetworkConfig) -> Self {
        let authority_to_peer_id: HashMap<AuthorityIdentifier, PeerId> = committee
            .authorities()
            .map(|a| {
                let fc = a.network_key();
                let peer_id = network_config
                    .ed25519_fastcrypto_to_libp2p(&fc)
                    .expect("fastcrypto to libp2p PeerId always works");
                (a.id(), peer_id)
            })
            .collect();

        let peer_id_to_authority =
            authority_to_peer_id.iter().map(|(a, p_id)| (*p_id, *a)).collect();

        Self { authority_to_peer_id, peer_id_to_authority }
    }
}
