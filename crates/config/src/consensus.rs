use std::sync::Arc;

use consensus_network::client::{PrimaryClient, WorkerClient};
use parking_lot::Mutex;
use tn_storage::{traits::Database, NodeStorage};
use tn_types::{Authority, Committee, Noticer, Notifier, WorkerCache};

use crate::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, Parameters, TelcoinDirs};

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    tn_datadir: Arc<dyn TelcoinDirs>,
    network_client: PrimaryClient,
    worker_to_primary_client: WorkerClient,
    // primary_to_worker_client: PrimaryClient,
    node_storage: NodeStorage<DB>,
    key_config: KeyConfig,
    authority: Authority,
}

#[derive(Debug, Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
    worker_cache: Option<Arc<WorkerCache>>,
    shutdown: Arc<Mutex<Notifier>>,
}

impl<DB> ConsensusConfig<DB>
where
    DB: Database,
{
    pub fn new<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: TND,
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
        Self::new_with_committee(
            config,
            tn_datadir,
            node_storage,
            key_config,
            committee,
            Some(worker_cache),
        )
    }

    /// Create a new config with a committe.
    ///
    /// This should only be called by `Self::new`.
    /// The method is exposed publicly for testing ONLY.
    pub fn new_with_committee<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: TND,
        node_storage: NodeStorage<DB>,
        key_config: KeyConfig,
        committee: Committee,
        mut worker_cache: Option<WorkerCache>,
    ) -> eyre::Result<Self> {
        let network_client =
            PrimaryClient::new_from_public_key(config.validator_info.primary_network_key());

        let primary_public_key = key_config.primary_public_key();
        let authority = committee
            .authority_by_key(&primary_public_key)
            .unwrap_or_else(|| {
                panic!("Our node with key {:?} should be in committee", primary_public_key)
            })
            .clone();

        let tn_datadir = Arc::new(tn_datadir);
        let worker_cache = worker_cache.take().map(Arc::new);
        let shutdown = Arc::new(Mutex::new(Notifier::new()));
        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                tn_datadir,
                network_client,
                node_storage,
                key_config,
                authority,
            }),
            worker_cache,
            shutdown,
        })
    }

    /// Return a Noticer that will signal when shutdown has occurred.
    pub fn subscribe_shutdown(&self) -> Noticer {
        self.shutdown.lock().subscribe()
    }

    /// Sends the shudown signal to all subscribers of the configs shutdown Noticer.
    pub fn shutdown(&self) {
        self.shutdown.lock().notify();
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

    pub fn tn_datadir(&self) -> Arc<dyn TelcoinDirs> {
        self.inner.tn_datadir.clone()
    }

    pub fn network_client(&self) -> &PrimaryClient {
        &self.inner.network_client
    }

    /// Local interface for worker to primary communication.
    pub fn worker_to_primary_client(&self) -> &PrimaryClient {
        &self.inner.worker_to_primary_client
    }

    /// Local interface for primary to worker communication.
    pub fn primary_to_worker_client(&self) -> &PrimaryClient {
        &self.inner.primary_to_worker_client
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
}
