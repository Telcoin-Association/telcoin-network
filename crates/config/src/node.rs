//! Configurations for the Telcoin Network.

use crate::{ConfigFmt, ConfigTrait, NodeInfo, TelcoinDirs};
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, time::Duration};
use tn_types::{
    get_available_tcp_port, get_available_udp_port, test_genesis, Address, BlsPublicKey,
    BlsSignature, Genesis, Multiaddr, NetworkPublicKey, WorkerIndex, MAINNET_COMMITTEE,
    MAINNET_GENESIS, MAINNET_PARAMETERS, MAINNET_WORKER_CACHE, TESTNET_COMMITTEE, TESTNET_GENESIS,
    TESTNET_PARAMETERS, TESTNET_WORKER_CACHE,
};
use tracing::info;

/// The filename to use when reading/writing the validator's BlsKey.
pub const BLS_KEYFILE: &str = "bls.key";
// The filename to use when reading/writing a wrapped (encypted) validator BlsKey.
pub const BLS_WRAPPED_KEYFILE: &str = "bls.kw";
/// The filename to use when reading/writing the primary's network keys seed.
pub const PRIMARY_NETWORK_SEED_FILE: &str = "primary.seed";
/// The filename to use when reading/writing the network key seed used by all workers.
pub const WORKER_NETWORK_SEED_FILE: &str = "worker.seed";

/// Configuration for the Telcoin Network node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// [NodeInfo] for the node
    pub node_info: NodeInfo,

    /// Parameters for the network.
    pub parameters: Parameters,

    /// The [Genesis] for the node.
    pub genesis: Genesis,

    /// Is this an observer node?
    pub observer: bool,

    /// Refernce to the apps version string.
    #[serde(skip)]
    pub version: &'static str,
}

impl ConfigTrait for Config {}

impl Config {
    /// Create a Config for testing.
    pub fn default_for_test() -> Self {
        Self {
            // defaults
            node_info: Default::default(),
            parameters: Default::default(),
            genesis: test_genesis(),
            observer: false,
            version: "UNKNOWN",
        }
    }

    /// Load a config from it's component parts.
    /// Fallback to defaults if files are missing.
    pub fn load_or_default<P: TelcoinDirs>(
        tn_datadir: &P,
        observer: bool,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let node_info: NodeInfo =
            Config::load_from_path_or_default(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters = Config::load_from_path_or_default(
            tn_datadir.node_config_parameters_path(),
            ConfigFmt::YAML,
        )?;
        let genesis: Genesis =
            Config::load_from_path_or_default(tn_datadir.genesis_file_path(), ConfigFmt::YAML)?;

        Ok(Config { node_info, parameters, genesis, observer, version })
    }

    /// Load a config from it's component parts.
    pub fn load<P: TelcoinDirs>(
        tn_datadir: &P,
        observer: bool,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            Config::load_from_path(tn_datadir.node_config_parameters_path(), ConfigFmt::YAML)?;
        let genesis: Genesis =
            Config::load_from_path(tn_datadir.genesis_file_path(), ConfigFmt::YAML)?;

        Ok(Config { node_info: validator_info, parameters, genesis, observer, version })
    }

    /// Load a config from it's component parts.
    pub fn load_adiri<P: TelcoinDirs>(
        tn_datadir: &P,
        observer: bool,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            serde_yaml::from_str(TESTNET_PARAMETERS).expect("bad adiri parameters yaml data");
        let genesis: Genesis =
            serde_yaml::from_str(TESTNET_GENESIS).expect("bad adiri genesis yaml data");
        // If the default committee file does not exist then save it.
        let committee_path = tn_datadir.committee_path();
        if !committee_path.exists() {
            File::create_new(committee_path)?.write_all(TESTNET_COMMITTEE.as_bytes())?
        }
        // If the default worker cache file does not exist then save it.
        let worker_cache_path = tn_datadir.worker_cache_path();
        if !worker_cache_path.exists() {
            File::create_new(worker_cache_path)?.write_all(TESTNET_WORKER_CACHE.as_bytes())?
        }

        Ok(Config { node_info: validator_info, parameters, genesis, observer, version })
    }

    /// Load a config from it's component parts.
    pub fn load_mainnet<P: TelcoinDirs>(
        tn_datadir: &P,
        observer: bool,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            serde_yaml::from_str(MAINNET_PARAMETERS).expect("bad adiri parameters yaml data");
        let genesis: Genesis =
            serde_yaml::from_str(MAINNET_GENESIS).expect("bad adiri genesis yaml data");
        // If the default committee file does not exist then save it.
        let committee_path = tn_datadir.committee_path();
        if !committee_path.exists() {
            File::create_new(committee_path)?.write_all(MAINNET_COMMITTEE.as_bytes())?
        }
        // If the default worker cache file does not exist then save it.
        let worker_cache_path = tn_datadir.worker_cache_path();
        if !worker_cache_path.exists() {
            File::create_new(worker_cache_path)?.write_all(MAINNET_WORKER_CACHE.as_bytes())?
        }

        Ok(Config { node_info: validator_info, parameters, genesis, observer, version })
    }

    /// Update the authority protocol key.
    pub fn update_protocol_key(&mut self, value: BlsPublicKey) -> eyre::Result<()> {
        self.node_info.bls_public_key = value;
        Ok(())
    }

    /// Update the authority execution address.
    pub fn update_proof_of_possession(&mut self, value: BlsSignature) -> eyre::Result<()> {
        self.node_info.proof_of_possession = value;
        Ok(())
    }

    /// Update the authority network key.
    pub fn update_primary_network_key(&mut self, value: NetworkPublicKey) -> eyre::Result<()> {
        self.node_info.p2p_info.network_key = value;
        Ok(())
    }

    /// Update the worker network key.
    pub fn update_worker_network_key(&mut self, value: NetworkPublicKey) -> eyre::Result<()> {
        for worker in self.node_info.p2p_info.worker_index.0.iter_mut() {
            worker.name = value.clone();
        }
        Ok(())
    }

    /// Update the authority execution address.
    pub fn update_execution_address(&mut self, value: Address) -> eyre::Result<()> {
        self.node_info.execution_address = value;
        Ok(())
    }

    /// Update genesis.
    pub fn with_genesis(mut self, genesis: Genesis) -> Self {
        self.genesis = genesis;
        self
    }

    /// Return a reference to the
    pub fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    /// Return the ChainSpec for the configured Genesis
    pub fn chain_spec(&self) -> ChainSpec {
        self.genesis.clone().into()
    }

    /// Return a reference to the exeuction address for suggested fee recipient.
    pub fn execution_address(&self) -> &Address {
        &self.node_info.execution_address
    }

    /// Return a reference to the primary's public BLS key.
    pub fn primary_bls_key(&self) -> &BlsPublicKey {
        self.node_info.public_key()
    }

    /// Return a reference to the primary's [WorkerIndex].
    ///
    /// The [WorkerIndex] contains all workers for this validator.
    pub fn workers(&self) -> &WorkerIndex {
        self.node_info.worker_index()
    }
}

/// Holds all the node properties.
///
/// An example is provided to
/// showcase the usage and deserialization from a json file.
/// To define a Duration on the property file can use either
/// milliseconds or seconds (e.x 5s, 10ms , 2000ms).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Parameters {
    /// When the primary has `header_num_of_batches_threshold` num of batch digests available,
    /// then it can propose a new header.
    #[serde(default = "Parameters::default_header_num_of_batches_threshold")]
    pub header_num_of_batches_threshold: usize,

    /// The maximum number of batch digests included in a header.
    #[serde(default = "Parameters::default_max_header_num_of_batches")]
    pub max_header_num_of_batches: usize,

    /// The maximum delay that the primary should wait between generating two headers, even if
    /// other conditions are not satisfied besides having enough parent stakes.
    #[serde(with = "humantime_serde", default = "Parameters::default_max_header_delay")]
    pub max_header_delay: Duration,
    /// When the delay from last header reaches `min_header_delay`, a new header can be proposed
    /// even if batches have not reached `header_num_of_batches_threshold`.
    #[serde(with = "humantime_serde", default = "Parameters::default_min_header_delay")]
    pub min_header_delay: Duration,

    /// The depth of the garbage collection (Denominated in number of rounds).
    #[serde(default = "Parameters::default_gc_depth")]
    pub gc_depth: u32,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    #[serde(with = "humantime_serde", default = "Parameters::default_sync_retry_delay")]
    pub sync_retry_delay: Duration,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    #[serde(default = "Parameters::default_sync_retry_nodes")]
    pub sync_retry_nodes: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached.
    #[serde(with = "humantime_serde", default = "Parameters::default_max_batch_delay")]
    pub max_batch_delay: Duration,
    /// The maximum number of concurrent requests for messages accepted from an un-trusted entity
    #[serde(default = "Parameters::default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Worker timeout when request vote from peers.
    #[serde(default = "Parameters::default_batch_vote_timeout")]
    pub batch_vote_timeout: Duration,
    /// If set the Address that will recieve basefees.
    pub basefee_address: Option<Address>,
}

impl Parameters {
    fn default_header_num_of_batches_threshold() -> usize {
        5
    }

    fn default_max_header_num_of_batches() -> usize {
        10
    }

    fn default_max_header_delay() -> Duration {
        Duration::from_secs(10)
    }

    fn default_min_header_delay() -> Duration {
        Duration::from_secs(5)
    }

    pub fn default_gc_depth() -> u32 {
        50
    }

    fn default_sync_retry_delay() -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_sync_retry_nodes() -> usize {
        3
    }

    fn default_max_batch_delay() -> Duration {
        Duration::from_secs(1)
    }

    fn default_max_concurrent_requests() -> usize {
        500_000
    }

    fn default_batch_vote_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Admin server settings.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct NetworkAdminServerParameters {
    /// Primary network admin server port number
    pub primary_network_admin_server_port: u16,
    /// Worker network admin server base port number
    pub worker_network_admin_server_base_port: u16,
}

impl Default for NetworkAdminServerParameters {
    fn default() -> Self {
        let host = "127.0.0.1";
        Self {
            primary_network_admin_server_port: get_available_udp_port(host)
                .expect("udp port is available for primary"),
            worker_network_admin_server_base_port: get_available_udp_port(host)
                .expect("udp port is available for worker admin server"),
        }
    }
}

/// Prometheus metrics multiaddr.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PrometheusMetricsParameters {
    /// Socket address the server should be listening to.
    pub socket_addr: Multiaddr,
}

impl Default for PrometheusMetricsParameters {
    fn default() -> Self {
        let host = "127.0.0.1";
        Self {
            socket_addr: format!(
                "/ip4/{}/tcp/{}/http",
                host,
                get_available_tcp_port(host)
                    .expect("os has available TCP port for default prometheus metrics")
            )
            .parse()
            .expect("default prometheus metrics to parse available socket addr on localhost"),
        }
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_num_of_batches_threshold: Parameters::default_header_num_of_batches_threshold(),
            max_header_num_of_batches: Parameters::default_max_header_num_of_batches(),
            max_header_delay: Parameters::default_max_header_delay(),
            min_header_delay: Parameters::default_min_header_delay(),
            gc_depth: Parameters::default_gc_depth(),
            sync_retry_delay: Parameters::default_sync_retry_delay(),
            sync_retry_nodes: Parameters::default_sync_retry_nodes(),
            max_batch_delay: Parameters::default_max_batch_delay(),
            max_concurrent_requests: Parameters::default_max_concurrent_requests(),
            batch_vote_timeout: Parameters::default_batch_vote_timeout(),
            basefee_address: None,
        }
    }
}

impl Parameters {
    /// Tracing::info! for [Self].
    pub fn tracing(&self) {
        info!("Header number of batches threshold set to {}", self.header_num_of_batches_threshold);
        info!("Header max number of batches set to {}", self.max_header_num_of_batches);
        info!("Max header delay set to {} ms", self.max_header_delay.as_millis());
        info!("Min header delay set to {} ms", self.min_header_delay.as_millis());
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay.as_millis());
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Max batch delay set to {} ms", self.max_batch_delay.as_millis());
        info!("Max concurrent requests set to {}", self.max_concurrent_requests);
    }
}
