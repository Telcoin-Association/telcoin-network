//! Configurations for the Telcoin Network.

use crate::{ConfigFmt, ConfigTrait, NodeInfo, TelcoinDirs, GOVERNANCE_SAFE_ADDRESS};
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, io::Write, time::Duration};
use tn_types::{
    get_available_udp_port, test_genesis, Address, BlsPublicKey, BlsSignature, BootstrapServer,
    Committee, Genesis, NetworkPublicKey, WorkerId, MAINNET_BOOTSTRAP_SERVERS, MAINNET_COMMITTEE,
    MAINNET_GENESIS, MAINNET_PARAMETERS, TESTNET_BOOTSTRAP_SERVERS, TESTNET_COMMITTEE,
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

    /// Spawn ExEx tasks (and the ExEx manager) as critical tasks.
    ///
    /// Default `false`: ExExes run isolated and non-critical, so a stuck,
    /// panicking, or finished ExEx can never shut the node down. Set `true` when
    /// an ExEx is load-bearing for this deployment and its failure (or clean
    /// exit) *should* take the node down — e.g. a bridge that must not silently
    /// stop following the chain.
    #[serde(default)]
    pub exex_critical: bool,

    /// The bootstrap servers built into the binary for a named chain, set by
    /// [`Config::load_adiri`] and [`Config::load_mainnet`].
    ///
    /// `None` for a network loaded from the datadir: such a node takes its bootstrap servers from
    /// the genesis committee file. Either set is the default that `network-config`'s
    /// `bootstrap_peers` and the `--bootstrap-peers` flag replace.
    #[serde(skip)]
    pub bootstrap_servers: Option<BTreeMap<BlsPublicKey, BootstrapServer>>,

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
            exex_critical: false,
            bootstrap_servers: None,
            version: "UNKNOWN",
        }
    }

    /// Load a config from it's component parts.
    /// Fallback to defaults if files are missing.
    pub fn load_or_default<P: TelcoinDirs>(
        tn_datadir: &P,
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

        Ok(Config {
            node_info,
            parameters,
            genesis,
            exex_critical: false,
            bootstrap_servers: None,
            version,
        })
    }

    /// Load a config from it's component parts.
    pub fn load<P: TelcoinDirs>(tn_datadir: &P, version: &'static str) -> eyre::Result<Self> {
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
            exex_critical: false,
            bootstrap_servers: None,
            version,
        })
    }

    /// Load the bundled adiri (testnet) config: node info from the datadir, parameters, genesis
    /// and bootstrap servers compiled in.
    ///
    /// Writes nothing to the datadir. Call [`Config::write_adiri_committee`] once the caller has
    /// decided to use this config, so a start refused after the load leaves the datadir as it was.
    pub fn load_adiri<P: TelcoinDirs>(tn_datadir: &P, version: &'static str) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            serde_yaml::from_str(TESTNET_PARAMETERS).expect("bad adiri parameters yaml data");
        let genesis: Genesis =
            serde_yaml::from_str(TESTNET_GENESIS).expect("bad adiri genesis yaml data");
        let bootstrap_servers = Some(parse_bootstrap_servers("adiri", TESTNET_BOOTSTRAP_SERVERS)?);

        Ok(Config {
            node_info: validator_info,
            parameters,
            genesis,
            exex_critical: false,
            bootstrap_servers,
            version,
        })
    }

    /// Load the bundled mainnet config: node info from the datadir, parameters, genesis and
    /// bootstrap servers compiled in.
    ///
    /// Writes nothing to the datadir. Call [`Config::write_mainnet_committee`] once the caller
    /// has decided to use this config, so a start refused after the load leaves the datadir as it
    /// was.
    pub fn load_mainnet<P: TelcoinDirs>(
        tn_datadir: &P,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            serde_yaml::from_str(MAINNET_PARAMETERS).expect("bad adiri parameters yaml data");
        let genesis: Genesis =
            serde_yaml::from_str(MAINNET_GENESIS).expect("bad adiri genesis yaml data");
        let bootstrap_servers =
            Some(parse_bootstrap_servers("mainnet", MAINNET_BOOTSTRAP_SERVERS)?);

        Ok(Config {
            node_info: validator_info,
            parameters,
            genesis,
            exex_critical: false,
            bootstrap_servers,
            version,
        })
    }

    /// Write the bundled adiri (testnet) genesis committee to `genesis/committee.yaml` unless
    /// that file already exists.
    pub fn write_adiri_committee<P: TelcoinDirs>(tn_datadir: &P) -> eyre::Result<()> {
        write_committee_if_missing(tn_datadir, TESTNET_COMMITTEE)
    }

    /// Write the bundled mainnet genesis committee to `genesis/committee.yaml` unless that file
    /// already exists.
    pub fn write_mainnet_committee<P: TelcoinDirs>(tn_datadir: &P) -> eyre::Result<()> {
        write_committee_if_missing(tn_datadir, MAINNET_COMMITTEE)
    }

    /// The chain's default bootstrap servers: the list built into the binary for a named chain,
    /// or else `genesis_committee`'s, the genesis committee file of a network started from datadir
    /// genesis files. `network-config`'s `bootstrap_peers` and the `--bootstrap-peers` flag
    /// replace this default.
    pub fn default_bootstrap_servers(
        &self,
        genesis_committee: &Committee,
    ) -> BTreeMap<BlsPublicKey, BootstrapServer> {
        self.bootstrap_servers.clone().unwrap_or_else(|| genesis_committee.bootstrap_servers())
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

    /// Update the network key of worker `worker_id`.
    ///
    /// Errors if this node runs no worker with that id.
    pub fn update_worker_network_key(
        &mut self,
        worker_id: WorkerId,
        value: NetworkPublicKey,
    ) -> eyre::Result<()> {
        self.node_info
            .p2p_info
            .worker_mut(worker_id)
            .map(|worker| worker.network_key = value)
            .ok_or_else(|| eyre::eyre!("no worker {worker_id} in node info"))
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
    /// How long a voter may spend evaluating one vote request, and how far past the drift
    /// tolerance a future-dated header can be and still get a retryable answer.
    ///
    /// Both uses are on the voter side. Each vote request is evaluated under this time limit; a
    /// request that runs out of time gets a retryable response instead of a vote. A header ahead
    /// of the voter's clock by more than `max_header_time_drift_tolerance` but by no more than
    /// that tolerance plus this timeout also gets a retryable response rather than a rejection.
    /// The requesting proposer retries on either response, so a slow voter or a small clock skew
    /// delays that vote rather than ending the request.
    ///
    /// Kept separate from `max_header_delay` because a voter may hold its vote while it waits out
    /// a future-dated header's lead, up to `max_header_time_drift_tolerance`, before it answers.
    /// A limit tied to the header cadence can expire while an honest voter is still waiting, and
    /// the shorter the cadence the more often that happens.
    ///
    /// Must be at least `max_header_delay + max_header_time_drift_tolerance` (the tolerance lives
    /// in the network config's `sync_config`; while the sub-second gate is dormant for the epoch
    /// it is rounded up to whole seconds, matching the voter), above the voter's 1 s execution
    /// wait (`VOTE_EXECUTION_WAIT`) and below 9 s: the 10 s libp2p request timeout less the
    /// execution wait, which the voter does before it takes the author lock and starts this
    /// timeout.
    /// `ConsensusConfig::validate_epoch_timing` rejects other values, and the node runs
    /// it on entering any epoch it can still vote in, so it refuses to start such an epoch with
    /// one. An epoch its committee has already closed is replayed without the check. Defaults to
    /// 5 s.
    #[serde(with = "humantime_serde", default = "Parameters::default_vote_timeout")]
    pub vote_timeout: Duration,

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
    /// The address that receives the base-fee portion of every transaction's gas payment.
    ///
    /// CONSENSUS-CRITICAL. The EVM credits this account on every transaction, so its balance
    /// enters the state root. All nodes on a network must hold the same value. If two nodes
    /// disagree, they still agree on the batch, the certificate, and the commit order, but they
    /// build different state roots from the same committed output and the network splits. See the
    /// `BASEFEE_ADDRESS` documentation in `tn-reth` for the execution-side detail.
    ///
    /// The key is required: a parameters file that omits it fails to deserialize with an error
    /// that names the field, wherever that file is parsed. (The one adjacent gap is a missing
    /// FILE under [`Config::load_or_default`], which substitutes the complete
    /// [`Parameters::default`] and writes it back to disk; its only caller today is a test
    /// helper, and the production node start loads through `Config::load`, which errors on a
    /// missing file.) This field deliberately declares no serde default. A default here would fill
    /// a fallback on a missing key in silence, which is the exact failure mode of issue #1113
    /// moved one layer up. Test fixtures obtain an address through [`Parameters::default`],
    /// which supplies `GOVERNANCE_SAFE_ADDRESS`. The chain presets and the genesis ceremony
    /// both write the key, so `--chain mainnet`, `--chain adiri`, and ceremony-founded
    /// networks all parse.
    pub basefee_address: Address,
    /// The default duration between parallel/fallback fetch requests to peers for missing
    /// certificates.
    #[serde(default = "Parameters::default_parallel_fetch_request_delay_interval")]
    pub parallel_fetch_request_delay_interval: Duration,

    /// Allow observer transaction forwarding to dial advertised JSON-RPC endpoints whose host is
    /// not a public internet address.
    ///
    /// An observer forwards each transaction it seals to the endpoint the owning validator
    /// advertised on its node record, so the dial target is chosen by a committee member rather
    /// than by this node. Default `false`: an endpoint on a loopback, private, link-local,
    /// unique-local, shared-address-space or unspecified address is refused, so a committee member
    /// cannot aim this node's outbound HTTP at hosts inside its own perimeter (issue #1092).
    ///
    /// Set `true` only where every committee member is under the same operator as this node --
    /// single-host and docker-compose deployments, where validators legitimately advertise
    /// `127.0.0.1` -- since it restores dialing of arbitrary internal addresses.
    #[serde(default = "Parameters::default_allow_private_forward_targets")]
    pub allow_private_forward_targets: bool,
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

    fn default_vote_timeout() -> Duration {
        Duration::from_secs(5)
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

    /// Refuse non-public forwarding targets unless the operator opts in.
    fn default_allow_private_forward_targets() -> bool {
        false
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
            vote_timeout: Parameters::default_vote_timeout(),
            gc_depth: Parameters::default_gc_depth(),
            sync_retry_delay: Parameters::default_sync_retry_delay(),
            sync_retry_nodes: Parameters::default_sync_retry_nodes(),
            max_batch_delay: Parameters::default_max_batch_delay(),
            max_concurrent_requests: Parameters::default_max_concurrent_requests(),
            batch_vote_timeout: Parameters::default_batch_vote_timeout(),
            // The test-fixture path. Production reads a parameters file, and a file without the
            // key fails to deserialize, so this value never reaches a production node. The
            // governance safe is the address an unset `Option` historically resolved to inside
            // `set_basefee_address`, so fixtures keep their old effective value.
            basefee_address: GOVERNANCE_SAFE_ADDRESS,
            parallel_fetch_request_delay_interval:
                Parameters::default_parallel_fetch_request_delay_interval(),
            allow_private_forward_targets: Parameters::default_allow_private_forward_targets(),
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
    /// - `gc_depth + gc_activity_window(gc_depth) + RETENTION_SLACK` must not exceed
    ///   [`tn_types::ROUNDS_TO_KEEP`]. Peers prune certificates below their newest written round
    ///   minus `ROUNDS_TO_KEEP`, and a validator that restarts consensus up to the activity window
    ///   behind (a rejoin, a restart) rebuilds its DAG from the `gc_depth` rounds below that point,
    ///   which it can fetch only while some peer still holds them. With `ROUNDS_TO_KEEP` 128 any
    ///   `gc_depth` above 64 is refused here (100 <= 128 at `gc_depth` 50); the protocol ceiling
    ///   [`tn_types::MAX_GC_DEPTH`] (50) refuses those first today, so this check binds once that
    ///   ceiling is raised.
    /// - `max_header_num_of_batches` must be at least one, otherwise the proposer caps every header
    ///   at zero batch digests: it drains no transactions while its digest queue grows without
    ///   bound.
    /// - `header_num_of_batches_threshold` must be at least one and must not exceed
    ///   `max_header_num_of_batches`. The proposer seals a header once it holds `threshold` digests
    ///   but includes at most `max_header_num_of_batches` of them, so a zero threshold seals empty
    ///   headers on the fast path and a threshold above the max makes the two conditions mutually
    ///   inconsistent.
    /// - `min_header_delay` must not exceed `max_header_delay`. The proposer treats the minimum as
    ///   an early-proposal point inside the maximum's window; with the two inverted the maximum
    ///   expires first and the configured minimum never takes effect.
    ///
    /// `basefee_address` needs no floor here: the field is a required key with no serde default,
    /// so a `parameters.yaml` that omits it already fails to deserialize, before any constructor
    /// runs, with an error that names the field.
    pub fn validate_operational_floors(&self) -> eyre::Result<()> {
        eyre::ensure!(
            self.gc_depth > tn_types::GC_ACTIVITY_BUFFER,
            "gc_depth {} must exceed the activity buffer {} or the node cannot stay active",
            self.gc_depth,
            tn_types::GC_ACTIVITY_BUFFER,
        );
        // the window is `gc_depth - GC_ACTIVITY_BUFFER` here (the check above), so the largest
        // gc_depth that fits is `(ROUNDS_TO_KEEP + GC_ACTIVITY_BUFFER - RETENTION_SLACK) / 2`
        let window = tn_types::gc_activity_window(self.gc_depth);
        let retention =
            self.gc_depth.saturating_add(window).saturating_add(tn_types::RETENTION_SLACK);
        eyre::ensure!(
            retention <= tn_types::ROUNDS_TO_KEEP,
            "gc_depth {} + activity window {} + retention slack {} = {} exceeds ROUNDS_TO_KEEP {}: \
             peers prune the certificate rounds a rejoining validator rebuilds its DAG from, so a \
             gc_depth above {} refuses to start",
            self.gc_depth,
            window,
            tn_types::RETENTION_SLACK,
            retention,
            tn_types::ROUNDS_TO_KEEP,
            (tn_types::ROUNDS_TO_KEEP + tn_types::GC_ACTIVITY_BUFFER - tn_types::RETENTION_SLACK) / 2,
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
        eyre::ensure!(
            self.min_header_delay <= self.max_header_delay,
            "min_header_delay {:?} must not exceed max_header_delay {:?}",
            self.min_header_delay,
            self.max_header_delay,
        );
        Ok(())
    }

    /// Tracing::info! for [Self].
    pub fn tracing(&self) {
        info!("Header number of batches threshold set to {}", self.header_num_of_batches_threshold);
        info!("Header max number of batches set to {}", self.max_header_num_of_batches);
        info!("Max header delay set to {} ms", self.max_header_delay.as_millis());
        info!("Min header delay set to {} ms", self.min_header_delay.as_millis());
        info!("Vote timeout set to {} ms", self.vote_timeout.as_millis());
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay.as_millis());
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Max batch delay set to {} ms", self.max_batch_delay.as_millis());
        info!("Max concurrent requests set to {}", self.max_concurrent_requests);
        // Consensus-critical, and the only parameter here whose value enters the state root. Two
        // operators debugging a state-root split have no other way to compare this value without
        // reading each other's files.
        info!(
            "Basefee address set to {} (consensus-critical: all nodes must agree)",
            self.basefee_address
        );
    }
}

/// Write `committee_yaml` to the datadir's `genesis/committee.yaml` unless that file exists.
fn write_committee_if_missing<P: TelcoinDirs>(
    tn_datadir: &P,
    committee_yaml: &str,
) -> eyre::Result<()> {
    let committee_path = tn_datadir.committee_path();
    if !committee_path.exists() {
        std::fs::create_dir_all(tn_datadir.genesis_path())?;
        File::create_new(committee_path)?.write_all(committee_yaml.as_bytes())?
    }
    Ok(())
}

/// Parse `chain`'s bundled bootstrap servers file (`chain-configs/*/bootstrap_servers.yaml`).
///
/// The text is compiled into the binary, so a failure here is a build defect; it is returned
/// rather than panicking so the node stops with the chain's name and the reason.
pub fn parse_bootstrap_servers(
    chain: &str,
    yaml: &str,
) -> eyre::Result<BTreeMap<BlsPublicKey, BootstrapServer>> {
    serde_yaml::from_str(yaml)
        .map_err(|e| eyre::eyre!("the bootstrap servers built in for {chain} do not parse: {e}"))
}

#[cfg(test)]
mod test {
    use super::Parameters;
    use crate::{
        genesis_validator_safes, Config, ConfigFmt, ConfigTrait as _, NetworkGenesis,
        CONSENSUS_REGISTRY_JSON, GOVERNANCE_SAFE_ADDRESS,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::Duration,
    };
    use tn_types::{
        address, keccak256, Address, Bytes, Committee, Genesis, GenesisAccount, B256,
        MAINNET_COMMITTEE, MAINNET_GENESIS, MAINNET_PARAMETERS, TESTNET_PARAMETERS, U256,
    };

    /// The fixed mainnet deployment address of the `ConsensusRegistry` system contract.
    ///
    /// Mirrors `tn_reth::system_calls::CONSENSUS_REGISTRY_ADDRESS`; tn-config sits below
    /// tn-reth in the dependency graph, so the address is restated here for the compiled-in
    /// mainnet genesis gate.
    const CONSENSUS_REGISTRY_ADDRESS: Address =
        address!("07E17e17E17e17E17e17E17E17E17e17e17E17e1");

    /// How to regenerate the committed mainnet chain-config placeholders when a gate below
    /// fails (issue #1063).
    const REGENERATION_LEVER: &str =
        "regenerate chain-configs/mainnet with `cargo test -p telcoin-network-cli \
         regenerate_mainnet_chain_configs -- --ignored`";

    /// The embedded mainnet chain-config files must load through the same deserialization
    /// paths `--chain mainnet` uses.
    ///
    /// `Config::load_mainnet` parses `MAINNET_GENESIS`/`MAINNET_PARAMETERS` with
    /// `serde_yaml::from_str` and `Config::write_mainnet_committee` writes `MAINNET_COMMITTEE` to
    /// `<datadir>/genesis/committee.yaml`, which the epoch manager later reads back via
    /// `Config::load_from_path::<Committee>` (crates/node/src/manager/node.rs). This gate
    /// drives all three files through exactly those paths, so a schema-stale committed file
    /// fails here instead of at node startup. It also checks that the committee seats exactly
    /// one validator under each genesis validator Safe.
    #[test]
    fn mainnet_chain_configs_load_via_the_node_paths() {
        let _genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize like Config::load_mainnet");
        let _parameters: Parameters = serde_yaml::from_str(MAINNET_PARAMETERS)
            .expect("embedded mainnet parameters.yaml must deserialize like Config::load_mainnet");

        // mirror the node's real committee read: file on disk -> Config::load_from_path
        let dir = tempfile::tempdir().expect("tempdir for committee round-trip");
        let committee_path = dir.path().join("committee.yaml");
        std::fs::write(&committee_path, MAINNET_COMMITTEE)
            .expect("write embedded mainnet committee.yaml to disk");
        let committee: Committee = Config::load_from_path(&committee_path, ConfigFmt::YAML)
            .unwrap_or_else(|e| {
                panic!(
                    "embedded mainnet committee.yaml must deserialize as tn_types::Committee \
                     via Config::load_from_path (the node's read path); schema-stale file \
                     (issue #1063) - {REGENERATION_LEVER}: {e}"
                )
            });
        // the ceremony keys authorities by BLS key, so compare the stake addresses as a set
        let stake_addresses: std::collections::BTreeSet<Address> =
            committee.authorities().iter().map(|authority| authority.execution_address()).collect();
        assert_eq!(
            (committee.size(), stake_addresses),
            (
                crate::genesis_validator_safes().len(),
                crate::genesis_validator_safes().into_iter().collect()
            ),
            "the mainnet genesis committee must seat one validator under each genesis validator \
             Safe - {REGENERATION_LEVER}"
        );
    }

    /// The named-chain loaders write nothing to the datadir; the committee writers add
    /// `genesis/committee.yaml` once and leave an existing file alone.
    #[test]
    fn named_chain_load_writes_nothing_and_committee_write_keeps_existing_file() {
        use crate::{NodeInfo, TelcoinDirs as _};

        let dir = tempfile::tempdir().expect("datadir");
        let datadir = dir.path().to_path_buf();
        Config::write_to_path(datadir.node_info_path(), NodeInfo::default(), ConfigFmt::YAML)
            .expect("seed node info");
        Config::load_mainnet(&datadir, "test").expect("load mainnet");
        Config::load_adiri(&datadir, "test").expect("load adiri");
        assert!(!datadir.genesis_path().exists(), "a load must not create genesis/");

        Config::write_mainnet_committee(&datadir).expect("write mainnet committee");
        let written = std::fs::read_to_string(datadir.committee_path()).expect("committee");
        assert_eq!(written, MAINNET_COMMITTEE);
        // an existing committee file is kept as it is
        Config::write_adiri_committee(&datadir).expect("existing committee kept");
        let kept = std::fs::read_to_string(datadir.committee_path()).expect("committee");
        assert_eq!(kept, MAINNET_COMMITTEE);
    }

    /// The mainnet genesis `ConsensusRegistry` account must carry the CURRENT tn-contracts
    /// runtime bytecode.
    ///
    /// The genesis ceremony (`RethEnv::create_consensus_registry_genesis_accounts`) splices
    /// the tmp-chain DEPLOYED registry code into the registry account: the artifact's
    /// `deployedBytecode.object` with the constructor-patched immutables at the artifact's
    /// `immutableReferences` sites (issue #1278). The node's system calls unconditionally
    /// speak that artifact's ABI. A stale committed code blob means a mainnet node can
    /// neither start epoch 0 (`getCommitteeBlsPubkeys`) nor close an epoch
    /// (`getValidatorsInfo`/`getNextCommitteeSize`) - issue #1063.
    #[test]
    fn mainnet_genesis_registry_code_matches_current_artifact() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");

        // same extraction the ceremony performs (deployedBytecode.object, hex-decoded)
        let artifact: serde_json::Value = serde_json::from_str(CONSENSUS_REGISTRY_JSON)
            .expect("embedded ConsensusRegistry artifact json must parse");
        let expected: Bytes = artifact
            .pointer("/deployedBytecode/object")
            .and_then(serde_json::Value::as_str)
            .expect("ConsensusRegistry artifact must contain deployedBytecode.object")
            .parse()
            .expect("deployedBytecode.object must be valid hex");

        let actual = genesis
            .alloc
            .get(&CONSENSUS_REGISTRY_ADDRESS)
            .and_then(|account| account.code.clone())
            .expect("mainnet genesis must allocate code for the ConsensusRegistry account");

        // The genesis ceremony splices the DEPLOYED tmp-chain code (issue #1278): identical to
        // the artifact's compile-time code except at the immutable sites, where forge ships
        // zeros and the constructor patched real values. Compare accordingly: byte-equal
        // outside `deployedBytecode.immutableReferences`, non-zero inside.
        let sites: Vec<(usize, usize)> = artifact
            .pointer("/deployedBytecode/immutableReferences")
            .and_then(serde_json::Value::as_object)
            .map(|refs| {
                refs.values()
                    .filter_map(serde_json::Value::as_array)
                    .flatten()
                    .filter_map(|site| {
                        site.get("start")
                            .and_then(serde_json::Value::as_u64)
                            .zip(site.get("length").and_then(serde_json::Value::as_u64))
                            .map(|(start, length)| (start as usize, length as usize))
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Fail loud on artifact schema drift: with zero parsed sites the comparison below
        // degrades to byte-equality, which a still-zeroed genesis satisfies (issue #1278).
        assert!(
            !sites.is_empty(),
            "registry artifact must list immutable sites (the Solady EIP712 cache); a missing \
             or renamed immutableReferences key would make this gate vacuous"
        );
        let inside_site =
            |i: usize| sites.iter().any(|(start, length)| i >= *start && i < start + length);
        let matches_outside_immutables = actual.len() == expected.len()
            && actual
                .iter()
                .zip(expected.iter())
                .enumerate()
                .all(|(i, (spliced, compiled))| inside_site(i) || spliced == compiled);
        assert!(
            matches_outside_immutables,
            "chain-configs/mainnet/genesis.yaml seeds the ConsensusRegistry with {} bytes of \
             runtime code but the current tn-contracts artifact deploys {} bytes; the node's \
             system calls speak the current ABI, so a mainnet node cannot start or close an \
             epoch (issue #1063) - {REGENERATION_LEVER}",
            actual.len(),
            expected.len(),
        );
        assert!(
            sites.iter().all(|(start, length)| {
                actual
                    .get(*start..start + length)
                    .is_some_and(|segment| segment.iter().any(|byte| *byte != 0))
            }),
            "chain-configs/mainnet/genesis.yaml ships an ALL-ZERO EIP712 immutable in the \
             ConsensusRegistry code: the ceremony spliced compile-time bytecode instead of the \
             deployed tmp-chain code (issue #1278) - {REGENERATION_LEVER}"
        );
    }

    /// The runtime code of every Safe v1.5.0 proxy, as vendored by tn-contracts.
    const SAFE_V150_PROXY_RUNTIME: &str = include_str!(
        "../../../tn-contracts/deployments/genesis/canonical-bytecode/safe-v1.5.0/SafeProxy.hex"
    );

    /// The canonical Safe v1.5.0 `SafeL2` singleton that each genesis validator Safe proxies to.
    const SAFE_V150_L2_SINGLETON: Address = address!("Edd160fEBBD92E350D4D398fb636302fccd67C7e");

    /// The canonical Safe v1.5.0 `CompatibilityFallbackHandler` of each genesis validator Safe.
    const SAFE_V150_FALLBACK_HANDLER: Address =
        address!("3EfCBb83A4A7AfcB4F68D501E2c2203a38be77f4");

    /// The sentinel that heads and ends a Safe's owner and module linked lists.
    const SAFE_SENTINEL: Address = address!("0000000000000000000000000000000000000001");

    /// Safe storage slot 0, the proxy's singleton (`SafeProxy.singleton`).
    const SAFE_SINGLETON_SLOT: B256 = B256::ZERO;

    /// Safe storage slot 1, the `ModuleManager.modules` linked list.
    const SAFE_MODULES_MAPPING_SLOT: u8 = 1;

    /// Safe storage slot 2, the `OwnerManager.owners` linked list.
    const SAFE_OWNERS_MAPPING_SLOT: u8 = 2;

    /// Safe storage slot 3, the owner count (`OwnerManager.ownerCount`).
    const SAFE_OWNER_COUNT_SLOT: B256 = B256::with_last_byte(3);

    /// Safe storage slot 4, the signature threshold (`OwnerManager.threshold`).
    const SAFE_THRESHOLD_SLOT: B256 = B256::with_last_byte(4);

    /// The owners of the mainnet governance Safe in the order of its owner list, as tn-contracts'
    /// genesis generator sets them (`_setGovernanceSafeConfig` in
    /// `script/GenerateGenesisPrecompileConfig.s.sol`).
    const MAINNET_GOVERNANCE_SAFE_OWNERS: [Address; 7] = [
        address!("2358CF87e62618663E781CE52EE7a7F777aC4e65"),
        address!("389C4bd707FAb237578A9603F55A02554CAa034b"),
        address!("DE5346d15Dc5e0D7b3bE7feFF5d96f548c321e88"),
        address!("B5DfE3a88c77a885362a9616405EAAbb5e0dB280"),
        address!("DCe4Ef7679E8A81EEE8c71917b21EbbCef45B5BA"),
        address!("a21B09Ff93A6ffc466F9d2D979fb2268fDaff248"),
        address!("5b9e70501a845FA8105B2f8228a0d243A98d97Fc"),
    ];

    /// The signature threshold of the mainnet governance Safe, as tn-contracts' genesis
    /// generator sets it (see [`MAINNET_GOVERNANCE_SAFE_OWNERS`]).
    const MAINNET_GOVERNANCE_SAFE_THRESHOLD: usize = 3;

    /// The ten reserve Safes of the mainnet genesis, in order: Safes prefunded for the next ten
    /// validators that join mainnet, which are not staked at genesis.
    ///
    /// tn-contracts' genesis generator puts them at `RESERVE_SAFE_BASE` plus `0` to `9` in
    /// `script/GenerateGenesisPrecompileConfig.s.sol`. They are not in `validator-safes.yaml`,
    /// which lists only the genesis validators' Safes, so their addresses are pinned here.
    const MAINNET_RESERVE_SAFES: [Address; 10] = [
        address!("000000000000000000000000000007a00005eed0"),
        address!("000000000000000000000000000007a00005eed1"),
        address!("000000000000000000000000000007a00005eed2"),
        address!("000000000000000000000000000007a00005eed3"),
        address!("000000000000000000000000000007a00005eed4"),
        address!("000000000000000000000000000007a00005eed5"),
        address!("000000000000000000000000000007a00005eed6"),
        address!("000000000000000000000000000007a00005eed7"),
        address!("000000000000000000000000000007a00005eed8"),
        address!("000000000000000000000000000007a00005eed9"),
    ];

    /// `FallbackManager.FALLBACK_HANDLER_STORAGE_SLOT`, the same in Safe v1.4.1 and v1.5.0.
    fn safe_fallback_handler_slot() -> B256 {
        keccak256("fallback_manager.handler.address")
    }

    /// `GuardManager.GUARD_STORAGE_SLOT`, the transaction guard.
    fn safe_guard_slot() -> B256 {
        keccak256("guard_manager.guard.address")
    }

    /// `ModuleManager.MODULE_GUARD_STORAGE_SLOT`, the module guard Safe v1.5.0 adds.
    fn safe_module_guard_slot() -> B256 {
        keccak256("module_manager.module_guard.address")
    }

    /// The slot of `owners[key]`, the entry of a Safe's owner list that points from `key` to the
    /// next owner: `keccak256(abi.encode(key, 2))`. The list starts at `owners[SENTINEL_OWNERS]`
    /// and its last owner points back at the sentinel.
    fn safe_owners_slot(key: Address) -> B256 {
        let mut preimage = [0u8; 64];
        preimage[12..32].copy_from_slice(key.as_slice());
        preimage[63] = SAFE_OWNERS_MAPPING_SLOT;
        keccak256(preimage)
    }

    /// The slot of `modules[SENTINEL_MODULES]`, the head of a Safe's module list:
    /// `keccak256(abi.encode(SENTINEL_MODULES, 1))`. The list is empty when the head points back
    /// at the sentinel.
    fn safe_module_list_head_slot() -> B256 {
        let mut preimage = [0u8; 64];
        preimage[12..32].copy_from_slice(SAFE_SENTINEL.as_slice());
        preimage[63] = SAFE_MODULES_MAPPING_SLOT;
        keccak256(preimage)
    }

    /// Each genesis validator Safe in the committed mainnet genesis is a Safe v1.5.0 proxy: the
    /// vendored proxy runtime over the `SafeL2` singleton, with the v1.5.0
    /// `CompatibilityFallbackHandler`, no module, no guard, no module guard and no balance.
    ///
    /// The genesis validators stake under these Safes. The slots are Safe's own storage layout,
    /// unchanged from v1.4.1 to v1.5.0 apart from the module guard v1.5.0 adds.
    #[test]
    fn mainnet_genesis_validator_safes_are_safe_v150_proxies() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let proxy_runtime: Bytes = SAFE_V150_PROXY_RUNTIME
            .trim()
            .parse()
            .expect("tn-contracts' vendored safe-v1.5.0/SafeProxy.hex must be hex");

        for safe in genesis_validator_safes() {
            let account = genesis.alloc.get(&safe).unwrap_or_else(|| {
                panic!(
                    "chain-configs/mainnet/genesis.yaml has no account for the genesis validator \
                     Safe {safe} - {REGENERATION_LEVER}"
                )
            });
            let word = |slot: B256| {
                account.storage.as_ref().and_then(|storage| storage.get(&slot)).copied()
            };
            assert_eq!(
                account.code.as_ref(),
                Some(&proxy_runtime),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} does not carry the \
                 Safe v1.5.0 proxy runtime - {REGENERATION_LEVER}"
            );
            assert_eq!(
                word(SAFE_SINGLETON_SLOT),
                Some(SAFE_V150_L2_SINGLETON.into_word()),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} does not proxy to the \
                 Safe v1.5.0 SafeL2 singleton {SAFE_V150_L2_SINGLETON} - {REGENERATION_LEVER}"
            );
            assert_eq!(
                word(safe_fallback_handler_slot()),
                Some(SAFE_V150_FALLBACK_HANDLER.into_word()),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} does not use the Safe \
                 v1.5.0 CompatibilityFallbackHandler {SAFE_V150_FALLBACK_HANDLER} - \
                 {REGENERATION_LEVER}"
            );
            assert_eq!(
                word(safe_module_list_head_slot()),
                Some(SAFE_SENTINEL.into_word()),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} must have an empty \
                 module list - {REGENERATION_LEVER}"
            );
            assert_eq!(
                (word(safe_guard_slot()), word(safe_module_guard_slot())),
                (None, None),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} must set neither a \
                 guard nor a module guard - {REGENERATION_LEVER}"
            );
            assert!(
                account.balance.is_zero(),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} must hold no balance \
                 (its stake sits in the ConsensusRegistry) but holds {} wei - {REGENERATION_LEVER}",
                account.balance
            );
        }
    }

    /// Each genesis validator Safe has the governance Safe's owners, in the same order, its owner
    /// count and its threshold.
    ///
    /// The gate compares storage slot for slot against the governance Safe of the same genesis
    /// file, so the owner list is never restated here. Every slot must match except the two that
    /// name a Safe's version, the singleton and the fallback handler: the governance Safe is a
    /// Safe v1.4.1 and the validator Safes are v1.5.0, which keep the same storage layout.
    #[test]
    fn mainnet_genesis_validator_safes_share_the_governance_safe_owners() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let version_slots = [SAFE_SINGLETON_SLOT, safe_fallback_handler_slot()];
        let owner_storage = |address: &Address| -> BTreeMap<B256, B256> {
            genesis
                .alloc
                .get(address)
                .and_then(|account| account.storage.clone())
                .unwrap_or_default()
                .into_iter()
                .filter(|(slot, _)| !version_slots.contains(slot))
                .collect()
        };

        let governance = owner_storage(&GOVERNANCE_SAFE_ADDRESS);
        // an empty governance Safe would let every comparison below pass
        assert!(
            governance.get(&SAFE_THRESHOLD_SLOT).is_some_and(|threshold| !threshold.is_zero()),
            "chain-configs/mainnet/genesis.yaml: the governance Safe {GOVERNANCE_SAFE_ADDRESS} \
             has no threshold - {REGENERATION_LEVER}"
        );
        for safe in genesis_validator_safes() {
            let validator = owner_storage(&safe);
            let differing: Vec<B256> = governance
                .keys()
                .chain(validator.keys())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|slot| governance.get(*slot) != validator.get(*slot))
                .copied()
                .collect();
            assert!(
                differing.is_empty(),
                "chain-configs/mainnet/genesis.yaml: validator Safe {safe} differs from the \
                 governance Safe {GOVERNANCE_SAFE_ADDRESS} in its owners, owner order, owner \
                 count or threshold, at slots {differing:?} - {REGENERATION_LEVER}"
            );
        }
    }

    /// The governance Safe of the committed mainnet genesis has the owners
    /// [`MAINNET_GOVERNANCE_SAFE_OWNERS`], in that order, and the threshold
    /// [`MAINNET_GOVERNANCE_SAFE_THRESHOLD`].
    ///
    /// `mainnet_genesis_validator_safes_share_the_governance_safe_owners` copies these signers to
    /// the ten validator Safes and `mainnet_genesis_reserve_safes_match_validator_safe_0` to the
    /// ten reserve Safes, so a change to them in tn-contracts reaches every genesis Safe on the
    /// next regeneration. The gate walks the owner list from the sentinel, as
    /// `OwnerManager.getOwners` does, and reads the owner count and threshold slots.
    #[test]
    fn mainnet_governance_safe_has_the_pinned_owners_and_threshold() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let storage = genesis
            .alloc
            .get(&GOVERNANCE_SAFE_ADDRESS)
            .and_then(|account| account.storage.clone())
            .unwrap_or_default();
        let word = |slot: B256| storage.get(&slot).copied().unwrap_or_default();

        // the walk stops one entry past the pinned length, so a cycle cannot loop forever
        let mut owners = Vec::new();
        let mut next = Address::from_word(word(safe_owners_slot(SAFE_SENTINEL)));
        while next != SAFE_SENTINEL
            && !next.is_zero()
            && owners.len() <= MAINNET_GOVERNANCE_SAFE_OWNERS.len()
        {
            owners.push(next);
            next = Address::from_word(word(safe_owners_slot(next)));
        }
        let owner_count = U256::from_be_bytes(word(SAFE_OWNER_COUNT_SLOT).0);
        let threshold = U256::from_be_bytes(word(SAFE_THRESHOLD_SLOT).0);
        assert_eq!(
            (owners, next, owner_count, threshold),
            (
                MAINNET_GOVERNANCE_SAFE_OWNERS.to_vec(),
                SAFE_SENTINEL,
                U256::from(MAINNET_GOVERNANCE_SAFE_OWNERS.len()),
                U256::from(MAINNET_GOVERNANCE_SAFE_THRESHOLD)
            ),
            "chain-configs/mainnet/genesis.yaml: the governance Safe {GOVERNANCE_SAFE_ADDRESS} \
             (left: owners in list order, the entry that ends the list, owner count, threshold) \
             differs from the pinned signers (right). A change of signers is deliberate: it \
             updates this pin together with `_setGovernanceSafeConfig` in tn-contracts' \
             script/GenerateGenesisPrecompileConfig.s.sol and the regenerated genesis, in one \
             commit"
        );
    }

    /// Each reserve Safe of the committed mainnet genesis has the code and the whole storage of
    /// genesis validator Safe 0, and none of them is a genesis validator Safe.
    ///
    /// Equal storage means the same singleton, fallback handler, owners, owner order and
    /// threshold, so a reserve Safe is configured exactly like the Safes the genesis validators
    /// stake under. Each holds 1,000,001 TEL (pinned in [`MAINNET_FUNDED_ACCOUNTS`]): a reserve
    /// Safe that drifted from that configuration would be a Safe with other owners or another
    /// singleton holding that balance.
    #[test]
    fn mainnet_genesis_reserve_safes_match_validator_safe_0() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let validator_safes = genesis_validator_safes();
        let validator_safe_0 = validator_safes[0];
        let config = |address: &Address| {
            genesis.alloc.get(address).map(|account| (&account.code, &account.storage))
        };
        // a validator Safe 0 without code or storage would let reserve Safes without them pass
        let expected = config(&validator_safe_0);
        assert!(
            expected.is_some_and(|(code, storage)| code.is_some() && storage.is_some()),
            "chain-configs/mainnet/genesis.yaml has no code and storage for the genesis \
             validator Safe {validator_safe_0} - {REGENERATION_LEVER}"
        );

        for safe in MAINNET_RESERVE_SAFES {
            assert!(
                !validator_safes.contains(&safe),
                "the reserve Safe {safe} is listed as a genesis validator Safe in tn-contracts' \
                 validator-safes.yaml"
            );
            assert!(
                config(&safe) == expected,
                "chain-configs/mainnet/genesis.yaml: the reserve Safe {safe} is missing or differs \
                 from the genesis validator Safe {validator_safe_0} in code or storage - \
                 {REGENERATION_LEVER}"
            );
        }
    }

    /// The committed mainnet genesis carries every account of tn-contracts'
    /// `deployments/genesis/precompile-config.yaml` with identical code, storage, balance and
    /// nonce.
    ///
    /// The ceremony etches these accounts after the system contracts, so a genesis regenerated
    /// from the current submodule holds each of them unchanged. A genesis left behind by a
    /// submodule bump, missing accounts the new generator writes or holding their old code,
    /// storage, balance or nonce, fails here.
    #[test]
    fn mainnet_genesis_carries_every_precompile_config_account() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let accounts = NetworkGenesis::fetch_precompile_genesis_accounts()
            .expect("tn-contracts' precompile-config.yaml must parse");
        assert!(!accounts.is_empty(), "tn-contracts' precompile-config.yaml lists no account");

        let code = |account: &GenesisAccount| account.code.clone().unwrap_or_default();
        let storage = |account: &GenesisAccount| account.storage.clone().unwrap_or_default();
        // an account without a nonce has nonce 0, and the config's nonce is checked only where
        // the config carries one
        let nonce_differs = |actual: &GenesisAccount, expected: &GenesisAccount| {
            expected.nonce.is_some_and(|nonce| actual.nonce.unwrap_or_default() != nonce)
        };
        let mut missing = BTreeSet::new();
        let mut differing = BTreeSet::new();
        for (address, expected) in &accounts {
            match genesis.alloc.get(address) {
                None => {
                    missing.insert(*address);
                }
                Some(actual)
                    if code(actual) != code(expected)
                        || storage(actual) != storage(expected)
                        || actual.balance != expected.balance
                        || nonce_differs(actual, expected) =>
                {
                    differing.insert(*address);
                }
                Some(_) => {}
            }
        }
        assert!(
            missing.is_empty() && differing.is_empty(),
            "chain-configs/mainnet/genesis.yaml is stale against tn-contracts' \
             deployments/genesis/precompile-config.yaml: of its {} accounts, {} are missing \
             {missing:?} and {} differ in code, storage, balance or nonce {differing:?} - \
             {REGENERATION_LEVER}",
            accounts.len(),
            missing.len(),
            differing.len(),
        );
    }

    /// The accounts of the committed mainnet genesis that hold a balance, with their balances in
    /// whole TEL.
    ///
    /// The governance Safe's balance comes from tn-contracts' precompile config, the
    /// `ConsensusRegistry` holds the ten genesis stakes of 1,000,000 TEL, and
    /// `0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a` is the placeholder dev-funded account that the
    /// mainnet regeneration passes.
    ///
    /// The balances of the ten [`MAINNET_RESERVE_SAFES`] also come from tn-contracts' precompile
    /// config (`RESERVE_SAFE_BALANCE`): 1,000,001 TEL each, the 1,000,000 TEL stake of a validator
    /// that joins after genesis plus 1 TEL for gas.
    const MAINNET_FUNDED_ACCOUNTS: [(Address, u64); 13] = [
        (GOVERNANCE_SAFE_ADDRESS, 10),
        (CONSENSUS_REGISTRY_ADDRESS, 10_000_000),
        (address!("3DCc9a6f3A71F0A6C8C659c65558321c374E917a"), 1_000_000_000),
        (MAINNET_RESERVE_SAFES[0], 1_000_001),
        (MAINNET_RESERVE_SAFES[1], 1_000_001),
        (MAINNET_RESERVE_SAFES[2], 1_000_001),
        (MAINNET_RESERVE_SAFES[3], 1_000_001),
        (MAINNET_RESERVE_SAFES[4], 1_000_001),
        (MAINNET_RESERVE_SAFES[5], 1_000_001),
        (MAINNET_RESERVE_SAFES[6], 1_000_001),
        (MAINNET_RESERVE_SAFES[7], 1_000_001),
        (MAINNET_RESERVE_SAFES[8], 1_000_001),
        (MAINNET_RESERVE_SAFES[9], 1_000_001),
    ];

    /// The accounts of the committed mainnet genesis that hold a balance are exactly
    /// [`MAINNET_FUNDED_ACCOUNTS`], with exactly those balances.
    ///
    /// Every TEL that exists at genesis sits in one of these accounts, so an allocation nobody
    /// intended, from a precompile-config entry, an `--accounts` file or a changed ceremony
    /// argument, fails here.
    #[test]
    fn mainnet_genesis_funds_only_the_pinned_accounts() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let funded: BTreeMap<Address, U256> = genesis
            .alloc
            .iter()
            .filter(|(_, account)| !account.balance.is_zero())
            .map(|(address, account)| (*address, account.balance))
            .collect();
        let wei_per_tel = U256::from(10).pow(U256::from(18));
        let pinned: BTreeMap<Address, U256> = MAINNET_FUNDED_ACCOUNTS
            .iter()
            .map(|(address, tel)| (*address, U256::from(*tel) * wei_per_tel))
            .collect();
        assert_eq!(
            funded, pinned,
            "chain-configs/mainnet/genesis.yaml funds the accounts on the left (balances in wei), \
             not the pinned ones on the right. A change to the genesis allocation is deliberate: \
             it updates MAINNET_FUNDED_ACCOUNTS together with the regenerated genesis, in one \
             commit"
        );
    }

    /// The `ConsensusRegistry` code of the committed mainnet genesis caches the genesis chain id
    /// in its EIP-712 immutables.
    ///
    /// The registry inherits Solady's `EIP712`, whose constructor caches `block.chainid` in an
    /// immutable. The ceremony deploys the registry on a temporary chain with the genesis chain
    /// id, so code that caches another chain id comes from a genesis whose `chainId` was edited
    /// after its ceremony. The cached address is the temporary deployment's, not the registry's,
    /// so the registry recomputes its domain separator on every use and the stale value costs
    /// only gas, but it shows that the genesis was not regenerated. The gate reads the 32-byte
    /// immutables at the artifact's `deployedBytecode.immutableReferences` sites and requires
    /// one of them to hold the chain id.
    #[test]
    fn mainnet_genesis_registry_code_caches_the_genesis_chain_id() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let artifact: serde_json::Value = serde_json::from_str(CONSENSUS_REGISTRY_JSON)
            .expect("embedded ConsensusRegistry artifact json must parse");
        let code = genesis
            .alloc
            .get(&CONSENSUS_REGISTRY_ADDRESS)
            .and_then(|account| account.code.clone())
            .expect("mainnet genesis must allocate code for the ConsensusRegistry account");

        let immutables: Vec<B256> = artifact
            .pointer("/deployedBytecode/immutableReferences")
            .and_then(serde_json::Value::as_object)
            .into_iter()
            .flat_map(|refs| refs.values())
            .filter_map(serde_json::Value::as_array)
            .flatten()
            .filter(|site| site.get("length").and_then(serde_json::Value::as_u64) == Some(32))
            .filter_map(|site| site.get("start").and_then(serde_json::Value::as_u64))
            .filter_map(|start| usize::try_from(start).ok())
            .filter_map(|start| code.get(start..start + 32))
            .map(B256::from_slice)
            .collect();
        assert!(
            !immutables.is_empty(),
            "the registry artifact must list 32-byte immutable sites (the Solady EIP712 cache)"
        );
        let chain_id = B256::from(U256::from(genesis.config.chain_id).to_be_bytes::<32>());
        assert!(
            immutables.contains(&chain_id),
            "chain-configs/mainnet/genesis.yaml: the ConsensusRegistry code caches no chain id {} \
             among its EIP-712 immutables {immutables:?}, so it was built for another chain id - \
             {REGENERATION_LEVER}",
            genesis.config.chain_id
        );
    }

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

    #[test]
    fn parameters_reject_min_header_delay_above_max_header_delay() {
        let params = Parameters {
            min_header_delay: Duration::from_millis(1_001),
            max_header_delay: Duration::from_millis(1_000),
            ..Default::default()
        };
        let err = params
            .validate_operational_floors()
            .expect_err("a min_header_delay above max_header_delay must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("min_header_delay") && msg.contains("max_header_delay"),
            "the error must name both fields: {msg}"
        );
    }

    #[test]
    fn parameters_accept_min_header_delay_equal_to_max_header_delay() {
        let params = Parameters {
            min_header_delay: Duration::from_secs(1),
            max_header_delay: Duration::from_secs(1),
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_ok(),
            "min_header_delay == max_header_delay (the mainnet preset) must be accepted"
        );
    }

    #[test]
    fn default_vote_timeout_is_five_seconds() {
        assert_eq!(Parameters::default().vote_timeout, Duration::from_secs(5));
    }

    #[test]
    fn vote_timeout_parses_from_humantime_yaml() {
        let yaml = format!("basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"\nvote_timeout: \"7s\"");
        let params: Parameters =
            serde_yaml::from_str(&yaml).expect("a humantime vote_timeout must parse");
        assert_eq!(params.vote_timeout, Duration::from_secs(7));
    }

    /// Parameters files written before `vote_timeout` existed omit the key and must still load.
    #[test]
    fn absent_vote_timeout_key_defaults_to_five_seconds() {
        let yaml = format!("basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"");
        let params: Parameters =
            serde_yaml::from_str(&yaml).expect("a parameters file without vote_timeout must parse");
        assert_eq!(params.vote_timeout, Duration::from_secs(5));
    }

    /// A parameters file that omits `basefee_address` must fail to deserialize.
    ///
    /// Every other key in the file carries a serde default, so this partial file isolates the one
    /// required key. Without this pin, a `#[serde(default)]` reintroduced on the field would fill
    /// a fallback on a missing key in silence, which is the failure mode of issue #1113.
    #[test]
    fn absent_basefee_address_key_fails_to_deserialize() {
        let err = serde_yaml::from_str::<Parameters>("gc_depth: 50")
            .expect_err("a parameters file without basefee_address must not parse");
        assert!(
            err.to_string().contains("basefee_address"),
            "the error must name the field an operator has to fix: {err}"
        );
    }

    /// A parameters file with an explicit `basefee_address: null` must also fail to deserialize.
    ///
    /// This is the upgrade shape, not a hypothetical: before this change `Parameters::default()`
    /// held `None`, and `load_from_path_or_default` writes the default back to disk when the file
    /// is absent, so a datadir created under the old code can hold `basefee_address: null`. That
    /// file takes serde's type-error path, not the missing-field path the previous test pins, so
    /// it needs its own pin.
    #[test]
    fn null_basefee_address_key_fails_to_deserialize() {
        let err = serde_yaml::from_str::<Parameters>("gc_depth: 50\nbasefee_address: null")
            .expect_err("a parameters file with a null basefee_address must not parse");
        assert!(
            err.to_string().contains("basefee_address"),
            "the error must name the field an operator has to fix: {err}"
        );
    }

    /// Both shipped presets must keep an explicit, correct address.
    ///
    /// The addresses are pinned by value. They are consensus-critical, so a change to either is a
    /// hard fork for that chain and updating this test is the intended cost of making one.
    #[test]
    fn shipped_chain_presets_pin_their_basefee_address() {
        let mainnet: Parameters =
            serde_yaml::from_str(MAINNET_PARAMETERS).expect("mainnet parameters parse");
        let adiri: Parameters =
            serde_yaml::from_str(TESTNET_PARAMETERS).expect("adiri parameters parse");
        assert_eq!(
            mainnet.basefee_address,
            address!("0x9999999999999999999999999999999999999999"),
            "mainnet's committed basefee address changed"
        );
        assert_eq!(
            adiri.basefee_address, GOVERNANCE_SAFE_ADDRESS,
            "adiri's committed basefee address changed"
        );
        mainnet
            .validate_operational_floors()
            .expect("the mainnet preset must satisfy the operational floors");
        adiri
            .validate_operational_floors()
            .expect("the adiri preset must satisfy the operational floors");
    }

    /// The certificate-retention floor accepts the devnet `gc_depth` (50 + 40 + 10 = 100 <= 128)
    /// and the largest that fits (64 + 54 + 10 = 128).
    #[test]
    fn parameters_accept_gc_depth_within_certificate_retention() {
        for gc_depth in [50, 64] {
            let params = Parameters { gc_depth, ..Default::default() };
            assert!(
                params.validate_operational_floors().is_ok(),
                "gc_depth {gc_depth} fits ROUNDS_TO_KEEP and must be accepted"
            );
        }
    }

    /// The certificate-retention floor refuses a `gc_depth` whose rebuild window peers may have
    /// pruned, with a message naming the numbers and the rule.
    #[test]
    fn parameters_reject_gc_depth_beyond_certificate_retention() {
        let params = Parameters { gc_depth: 65, ..Default::default() };
        assert!(
            params.validate_operational_floors().is_err(),
            "gc_depth 65 needs 130 rounds of retention and must be rejected"
        );
        let params = Parameters { gc_depth: 79, ..Default::default() };
        let err = params
            .validate_operational_floors()
            .expect_err("gc_depth 79 needs 158 rounds of retention and must be rejected");
        assert_eq!(
            err.to_string(),
            "gc_depth 79 + activity window 69 + retention slack 10 = 158 exceeds ROUNDS_TO_KEEP \
             128: peers prune the certificate rounds a rejoining validator rebuilds its DAG from, \
             so a gc_depth above 64 refuses to start"
        );
    }

    /// Each bundled bootstrap servers file parses, lists servers, names no BLS key twice, and
    /// passes the node's startup checks on bootstrap entries: every address ends with the
    /// `/p2p/` identity of its network key, and no network key serves two BLS keys. The adiri
    /// servers are public: a loopback address there would dial nothing outside the host.
    #[test]
    fn bundled_bootstrap_servers_are_dialable() -> eyre::Result<()> {
        use libp2p::multiaddr::Protocol;
        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::{
            BlsKeypair, Multiaddr, NetworkKeypair, P2pNode, MAINNET_BOOTSTRAP_SERVERS,
            TESTNET_BOOTSTRAP_SERVERS,
        };

        for (chain, yaml, public) in [
            ("adiri", TESTNET_BOOTSTRAP_SERVERS, true),
            ("mainnet", MAINNET_BOOTSTRAP_SERVERS, false),
        ] {
            let servers = super::parse_bootstrap_servers(chain, yaml)?;
            assert!(!servers.is_empty(), "{chain}: no bootstrap servers");
            // a repeated key decodes last-wins (into a map and into serde_yaml's own Mapping), so
            // count the file's top-level entries as written
            let entries = serde::Deserializer::deserialize_map(
                serde_yaml::Deserializer::from_str(yaml),
                CountEntries,
            )?;
            assert_eq!(entries, servers.len(), "{chain}: a BLS key is listed twice");

            servers
                .iter()
                .flat_map(|(key, server)| {
                    std::iter::once(&server.primary)
                        .chain(&server.workers)
                        .map(move |node| (key, node))
                })
                .for_each(|(key, node)| {
                    let address = &node.network_address;
                    assert!(
                        matches!(address.iter().last(), Some(Protocol::P2p(_))),
                        "{chain}: {key}: {address} has no /p2p/<peer id>"
                    );
                    assert!(
                        address.iter().any(|protocol| matches!(protocol, Protocol::QuicV1)),
                        "{chain}: {key}: {address} is not a QUIC address"
                    );
                    let loopback = address.iter().any(|protocol| match protocol {
                        Protocol::Ip4(ip) => ip.is_loopback(),
                        Protocol::Ip6(ip) => ip.is_loopback(),
                        _ => false,
                    });
                    assert!(
                        !(public && loopback),
                        "{chain}: {key}: {address} is a loopback address"
                    );
                });

            // the same check the node runs on its selected bootstrap set before any network starts
            let endpoint = || {
                P2pNode::from((
                    NetworkKeypair::generate_ed25519().public().into(),
                    Multiaddr::empty(),
                ))
            };
            let local = *BlsKeypair::generate(&mut StdRng::from_seed([7; 32])).public();
            crate::NetworkConfig::default()
                .validate_trusted_nodes(&servers, 1, &local, &endpoint(), &[endpoint()])
                .map_err(|e| eyre::eyre!("{chain}: {e}"))?;
        }
        Ok(())
    }

    /// Counts a YAML map's entries as written, duplicates included.
    struct CountEntries;

    impl<'de> serde::de::Visitor<'de> for CountEntries {
        type Value = usize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<usize, A::Error> {
            use serde::de::IgnoredAny;
            let mut entries = 0;
            while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {
                entries += 1;
            }
            Ok(entries)
        }
    }

    /// The entry count sees a repeated key that the map decode drops.
    #[test]
    fn count_entries_sees_repeated_keys() -> eyre::Result<()> {
        let count = |yaml| {
            serde::Deserializer::deserialize_map(
                serde_yaml::Deserializer::from_str(yaml),
                CountEntries,
            )
        };
        assert_eq!(count("a: 1\nb: 2\n")?, 2);
        assert_eq!(count("a: 1\na: 2\n")?, 2);
        Ok(())
    }

    /// A named chain's config carries its bundled bootstrap servers; a datadir config carries
    /// none, so the node falls back to its genesis committee file.
    #[test]
    fn named_chain_configs_carry_bundled_bootstrap_servers() -> eyre::Result<()> {
        use crate::{NodeInfo, TelcoinDirs as _};
        use tn_types::{MAINNET_BOOTSTRAP_SERVERS, TESTNET_BOOTSTRAP_SERVERS};

        let dir = tempfile::tempdir()?;
        let datadir = dir.path().to_path_buf();
        Config::write_to_path(datadir.node_info_path(), NodeInfo::default(), ConfigFmt::YAML)?;
        let adiri = Config::load_adiri(&datadir, "test")?;
        assert_eq!(
            adiri.bootstrap_servers,
            Some(super::parse_bootstrap_servers("adiri", TESTNET_BOOTSTRAP_SERVERS)?)
        );
        let mainnet = Config::load_mainnet(&datadir, "test")?;
        assert_eq!(
            mainnet.bootstrap_servers,
            Some(super::parse_bootstrap_servers("mainnet", MAINNET_BOOTSTRAP_SERVERS)?)
        );
        assert_eq!(Config::load_or_default(&datadir, "test")?.bootstrap_servers, None);
        assert_eq!(Config::default_for_test().bootstrap_servers, None);
        Ok(())
    }

    /// A named chain's default bootstrap servers are its built-in list, even when the genesis
    /// committee file (an existing datadir's, written by an older binary) lists others; a network
    /// from datadir genesis files takes the committee file's.
    #[test]
    fn default_bootstrap_servers_prefer_the_built_in_list() -> eyre::Result<()> {
        use crate::{NodeInfo, TelcoinDirs as _};
        use tn_types::{test_utils::bootstrap_servers_fixture, CommitteeBuilder};

        let listed = bootstrap_servers_fixture();
        let mut builder = CommitteeBuilder::new(0);
        listed.iter().for_each(|(key, server)| {
            builder.add_authority_and_bootstrap(
                *key,
                server.primary.clone(),
                server.workers.clone(),
                Address::ZERO,
            )
        });
        let committee = builder.build();
        assert_eq!(committee.bootstrap_servers(), listed);

        let dir = tempfile::tempdir()?;
        let datadir = dir.path().to_path_buf();
        Config::write_to_path(datadir.node_info_path(), NodeInfo::default(), ConfigFmt::YAML)?;
        let adiri = Config::load_adiri(&datadir, "test")?;
        let built_in = adiri.bootstrap_servers.clone().expect("adiri carries its list");
        assert_ne!(built_in, listed);
        assert_eq!(adiri.default_bootstrap_servers(&committee), built_in);
        let mainnet = Config::load_mainnet(&datadir, "test")?;
        assert_eq!(Some(mainnet.default_bootstrap_servers(&committee)), mainnet.bootstrap_servers);
        assert_eq!(
            Config::load_or_default(&datadir, "test")?.default_bootstrap_servers(&committee),
            listed
        );
        Ok(())
    }

    /// Parses a parameters file that holds the one required key plus the `extra` lines.
    fn parse_with(extra: &str) -> Parameters {
        let yaml = format!("basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"\n{extra}");
        serde_yaml::from_str(&yaml).unwrap_or_else(|e| panic!("{extra:?} must parse: {e}"))
    }

    /// Re-serializing a parameters file keeps every key it held at the same value, and the result
    /// parses back to the same parameters.
    ///
    /// Strings compare without case: an address is written back in lowercase hex whatever its
    /// checksum case in the file, which is the same address.
    #[test]
    fn reserializing_keeps_the_existing_keys() {
        let lowercase = |value: &serde_yaml::Value| match value {
            serde_yaml::Value::String(text) => serde_yaml::Value::String(text.to_lowercase()),
            other => other.clone(),
        };
        let bench5 = format!(
            "basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"\nmax_header_delay: 300ms\n\
             min_header_delay: 250ms\nvote_timeout: 5s\ngc_depth: 50\n\
             header_num_of_batches_threshold: 1\nmax_header_num_of_batches: 10\n\
             max_batch_delay: 200ms\nsync_retry_delay: 5s\n"
        );
        for text in [bench5.as_str(), MAINNET_PARAMETERS, TESTNET_PARAMETERS] {
            let input: serde_yaml::Value = serde_yaml::from_str(text).expect("yaml parses");
            let params: Parameters = serde_yaml::from_str(text).expect("parameters parse");
            let output = serde_yaml::to_value(&params).expect("parameters serialize");
            for (key, value) in input.as_mapping().expect("a parameters file is a mapping") {
                assert_eq!(
                    output.get(key).map(lowercase),
                    Some(lowercase(value)),
                    "key {key:?} changed on re-serialization"
                );
            }
            let reparsed: Parameters = serde_yaml::from_value(output).expect("output parses");
            assert_eq!(reparsed, params);
        }
    }

    /// An unknown key is ignored, so a mistyped key leaves the node at its defaults.
    #[test]
    fn unknown_keys_are_ignored() {
        assert_eq!(parse_with("no_such_switch: true\nanother_unknown: 3ms"), parse_with(""));
    }
}
