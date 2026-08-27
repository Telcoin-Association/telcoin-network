//! Configurations for the Telcoin Network.

use crate::{ConfigFmt, ConfigTrait, NodeInfo, TelcoinDirs, GOVERNANCE_SAFE_ADDRESS};
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, time::Duration};
use tn_types::{
    get_available_udp_port, test_genesis, Address, BlsPublicKey, BlsSignature, Genesis,
    NetworkPublicKey, WorkerId, MAINNET_COMMITTEE, MAINNET_GENESIS, MAINNET_PARAMETERS,
    TESTNET_COMMITTEE, TESTNET_GENESIS, TESTNET_PARAMETERS,
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
    /// - `max_header_num_of_batches` must be at least one, otherwise the proposer caps every header
    ///   at zero batch digests: it drains no transactions while its digest queue grows without
    ///   bound.
    /// - `header_num_of_batches_threshold` must be at least one and must not exceed
    ///   `max_header_num_of_batches`. The proposer seals a header once it holds `threshold` digests
    ///   but includes at most `max_header_num_of_batches` of them, so a zero threshold seals empty
    ///   headers on the fast path and a threshold above the max makes the two conditions mutually
    ///   inconsistent.
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
        // Consensus-critical, and the only parameter here whose value enters the state root. Two
        // operators debugging a state-root split have no other way to compare this value without
        // reading each other's files.
        info!(
            "Basefee address set to {} (consensus-critical: all nodes must agree)",
            self.basefee_address
        );
    }
}

#[cfg(test)]
mod test {
    use super::Parameters;
    use crate::{
        Config, ConfigFmt, ConfigTrait as _, CONSENSUS_REGISTRY_JSON, GOVERNANCE_SAFE_ADDRESS,
    };
    use tn_types::{
        address, Address, Bytes, Committee, Genesis, MAINNET_COMMITTEE, MAINNET_GENESIS,
        MAINNET_PARAMETERS, TESTNET_PARAMETERS,
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
    /// `serde_yaml::from_str` and materializes `MAINNET_COMMITTEE` to
    /// `<datadir>/genesis/committee.yaml`, which the epoch manager later reads back via
    /// `Config::load_from_path::<Committee>` (crates/node/src/manager/node.rs). This gate
    /// drives all three files through exactly those paths, so a schema-stale committed file
    /// fails here instead of at node startup.
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
        assert_eq!(
            committee.size(),
            4,
            "mainnet placeholder committee must seat the 4 placeholder validators"
        );
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
}
