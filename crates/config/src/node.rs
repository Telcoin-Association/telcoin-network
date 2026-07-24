//! Configurations for the Telcoin Network.

use crate::{ConfigFmt, ConfigTrait, NodeInfo, TelcoinDirs};
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, time::Duration};
use tn_types::{
    get_available_udp_port, test_genesis, Address, BlsPublicKey, BlsSignature, Genesis,
    NetworkPublicKey, MAINNET_COMMITTEE, MAINNET_GENESIS, MAINNET_PARAMETERS, TESTNET_COMMITTEE,
    TESTNET_GENESIS, TESTNET_PARAMETERS,
};
use tracing::info;

/// The filename to use when reading/writing the validator's BlsKey.
pub const BLS_KEYFILE: &str = "bls.key";
/// The filename to use when reading/writing a wrapped (encypted) validator BlsKey.
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

    /// Spawn ExEx tasks (and the ExEx manager) as critical tasks.
    ///
    /// Default `false`: ExExes run isolated and non-critical, so a stuck,
    /// panicking, or finished ExEx can never shut the node down. Set `true` when
    /// an ExEx is load-bearing for this deployment and its failure (or clean
    /// exit) *should* take the node down — e.g. a bridge that must not silently
    /// stop following the chain.
    #[serde(default)]
    pub exex_critical: bool,

    /// Reference to the apps version string.
    #[serde(skip)]
    pub version: &'static str,
}

impl ConfigTrait for Config {}

impl Config {
    /// Create a Config for testing.
    pub fn default_for_test() -> Self {
        Self::default_for_test_with_genesis(test_genesis())
    }

    /// Create a Config for testing.
    pub fn default_for_test_with_genesis(genesis: Genesis) -> Self {
        Self {
            // defaults
            node_info: Default::default(),
            parameters: Default::default(),
            genesis,
            observer: false,
            exex_critical: false,
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

        Ok(Config { node_info, parameters, genesis, observer, exex_critical: false, version })
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

        Ok(Config {
            node_info: validator_info,
            parameters,
            genesis,
            observer,
            exex_critical: false,
            version,
        })
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
            std::fs::create_dir_all(tn_datadir.genesis_path())?;
            File::create_new(committee_path)?.write_all(TESTNET_COMMITTEE.as_bytes())?
        }

        Ok(Config {
            node_info: validator_info,
            parameters,
            genesis,
            observer,
            exex_critical: false,
            version,
        })
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
            std::fs::create_dir_all(tn_datadir.genesis_path())?;
            File::create_new(committee_path)?.write_all(MAINNET_COMMITTEE.as_bytes())?
        }

        Ok(Config {
            node_info: validator_info,
            parameters,
            genesis,
            observer,
            exex_critical: false,
            version,
        })
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
        self.node_info.p2p_info.primary.network_key = value;
        Ok(())
    }

    /// Update the worker network key.
    pub fn update_worker_network_key(&mut self, value: NetworkPublicKey) -> eyre::Result<()> {
        self.node_info.p2p_info.worker.network_key = value;
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
    /// The default duration between parallel/fallback fetch requests to peers for missing
    /// certificates.
    #[serde(default = "Parameters::default_parallel_fetch_request_delay_interval")]
    pub parallel_fetch_request_delay_interval: Duration,
}

impl Parameters {
    fn default_header_num_of_batches_threshold() -> usize {
        5
    }

    fn default_max_header_num_of_batches() -> usize {
        tn_types::MAX_HEADER_NUM_OF_BATCHES
    }

    fn default_max_header_delay() -> Duration {
        Duration::from_millis(2500)
    }

    fn default_min_header_delay() -> Duration {
        Duration::from_millis(1000)
    }

    /// The default gc depth for consensus.
    pub fn default_gc_depth() -> u32 {
        tn_types::MAX_GC_DEPTH
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

    fn default_parallel_fetch_request_delay_interval() -> Duration {
        Duration::from_secs(5)
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
            parallel_fetch_request_delay_interval:
                Parameters::default_parallel_fetch_request_delay_interval(),
        }
    }
}

impl Parameters {
    /// Validate the protocol **ceilings** every node (production or test fixture) must honor.
    ///
    /// `gc_depth` and `max_header_num_of_batches` together bound how many unique batches a single
    /// committed [`ConsensusOutput`](tn_types::ConsensusOutput) can reference. The consensus-pack
    /// reader derives its reconstruction bound from [`tn_types::MAX_GC_DEPTH`] and
    /// [`tn_types::MAX_HEADER_NUM_OF_BATCHES`], so a node configured above those ceilings could
    /// commit an output that no node (including itself, on restart) can later reconstruct.
    /// Rejecting such a configuration keeps the producing and reconstructing halves in
    /// agreement.
    ///
    /// These are safe to enforce everywhere, so this runs for every constructor. The lower
    /// **operational floors** are enforced separately by
    /// [`Parameters::validate_operational_floors`] at the production entry points only, so DAG
    /// test fixtures may still use small `gc_depth` values.
    pub fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(
            self.gc_depth <= tn_types::MAX_GC_DEPTH,
            "gc_depth {} exceeds the protocol maximum {}",
            self.gc_depth,
            tn_types::MAX_GC_DEPTH,
        );
        eyre::ensure!(
            self.max_header_num_of_batches <= tn_types::MAX_HEADER_NUM_OF_BATCHES,
            "max_header_num_of_batches {} exceeds the protocol maximum {}",
            self.max_header_num_of_batches,
            tn_types::MAX_HEADER_NUM_OF_BATCHES,
        );
        Ok(())
    }

    /// Validate the operational **floors** and cross-field coupling: the values below which a node
    /// degrades silently instead of erroring.
    ///
    /// Unlike the ceilings in [`Parameters::validate`], these are enforced only at the production
    /// startup entry points (`ConsensusConfig::new` / `new_for_epoch`), never in the shared
    /// test-fixture constructor, so DAG tests that deliberately drive a small `gc_depth` keep
    /// working (see issue #954, "retain test-only overrides for DAG tests requiring small values").
    ///
    /// - `gc_depth` must exceed [`tn_types::GC_ACTIVITY_BUFFER`]. The consensus network handler
    ///   computes its activity window as `gc_depth - GC_ACTIVITY_BUFFER`; a `gc_depth` at or below
    ///   the buffer collapses that window to zero and wedges the node inactive during normal
    ///   operation. Coupling the floor to the same constant the handler subtracts keeps the two
    ///   from drifting apart.
    /// - `max_header_num_of_batches` must be at least one, otherwise the proposer caps every header
    ///   at zero batch digests: it drains no transactions while its digest queue grows without
    ///   bound.
    /// - `header_num_of_batches_threshold` must be at least one and must not exceed
    ///   `max_header_num_of_batches`. The proposer seals a header once it holds `threshold` digests
    ///   but includes at most `max_header_num_of_batches` of them, so a zero threshold seals empty
    ///   headers on the fast path and a threshold above the max makes the two conditions mutually
    ///   inconsistent.
    pub fn validate_operational_floors(&self) -> eyre::Result<()> {
        eyre::ensure!(
            self.gc_depth > tn_types::GC_ACTIVITY_BUFFER,
            "gc_depth {} must exceed the activity buffer {} or the node cannot stay active",
            self.gc_depth,
            tn_types::GC_ACTIVITY_BUFFER,
        );
        eyre::ensure!(
            self.max_header_num_of_batches >= 1,
            "max_header_num_of_batches must be at least 1, got {}",
            self.max_header_num_of_batches,
        );
        eyre::ensure!(
            self.header_num_of_batches_threshold >= 1,
            "header_num_of_batches_threshold must be at least 1, got {}",
            self.header_num_of_batches_threshold,
        );
        eyre::ensure!(
            self.header_num_of_batches_threshold <= self.max_header_num_of_batches,
            "header_num_of_batches_threshold {} must not exceed max_header_num_of_batches {}",
            self.header_num_of_batches_threshold,
            self.max_header_num_of_batches,
        );
        Ok(())
    }

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

#[cfg(test)]
mod test {
    use super::Parameters;

    #[test]
    fn default_parameters_are_within_protocol_ceilings() {
        Parameters::default().validate().expect("default parameters must be within ceilings");
    }

    #[test]
    fn default_parameters_pass_operational_floors() {
        Parameters::default()
            .validate_operational_floors()
            .expect("default parameters must satisfy the operational floors");
    }

    #[test]
    fn parameters_reject_gc_depth_over_ceiling() {
        let params = Parameters { gc_depth: tn_types::MAX_GC_DEPTH + 1, ..Default::default() };
        assert!(params.validate().is_err(), "gc_depth over the protocol ceiling must be rejected");
    }

    #[test]
    fn parameters_reject_max_header_num_of_batches_over_ceiling() {
        let params = Parameters {
            max_header_num_of_batches: tn_types::MAX_HEADER_NUM_OF_BATCHES + 1,
            ..Default::default()
        };
        assert!(
            params.validate().is_err(),
            "max_header_num_of_batches over the protocol ceiling must be rejected"
        );
    }

    #[test]
    fn parameters_reject_gc_depth_at_or_below_activity_buffer() {
        let at_buffer = Parameters { gc_depth: tn_types::GC_ACTIVITY_BUFFER, ..Default::default() };
        assert!(
            at_buffer.validate_operational_floors().is_err(),
            "gc_depth equal to the activity buffer collapses the activity window and must be rejected"
        );
        let below_buffer =
            Parameters { gc_depth: tn_types::GC_ACTIVITY_BUFFER - 1, ..Default::default() };
        assert!(
            below_buffer.validate_operational_floors().is_err(),
            "gc_depth below the activity buffer must be rejected"
        );
    }

    #[test]
    fn parameters_accept_gc_depth_one_above_activity_buffer() {
        let params =
            Parameters { gc_depth: tn_types::GC_ACTIVITY_BUFFER + 1, ..Default::default() };
        assert!(
            params.validate_operational_floors().is_ok(),
            "the smallest gc_depth leaving a positive activity window must be accepted"
        );
    }

    #[test]
    fn parameters_reject_zero_max_header_num_of_batches() {
        let params = Parameters {
            max_header_num_of_batches: 0,
            header_num_of_batches_threshold: 0,
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_err(),
            "a zero max_header_num_of_batches drains no digests and must be rejected"
        );
    }

    #[test]
    fn parameters_reject_zero_header_num_of_batches_threshold() {
        let params = Parameters { header_num_of_batches_threshold: 0, ..Default::default() };
        assert!(
            params.validate_operational_floors().is_err(),
            "a zero header_num_of_batches_threshold seals empty headers and must be rejected"
        );
    }

    #[test]
    fn parameters_reject_threshold_above_max_header_num_of_batches() {
        let params = Parameters {
            max_header_num_of_batches: 2,
            header_num_of_batches_threshold: 3,
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_err(),
            "a threshold above max_header_num_of_batches is mutually inconsistent and must be rejected"
        );
    }

    #[test]
    fn parameters_accept_minimal_valid_batch_bounds() {
        let params = Parameters {
            max_header_num_of_batches: 1,
            header_num_of_batches_threshold: 1,
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_ok(),
            "threshold == max == 1 is the minimal consistent batch configuration and must be accepted"
        );
    }
}
