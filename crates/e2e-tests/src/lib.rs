//! End-to-end tests for Telcoin Network
//!
//! Utilities for it tests.
//!
//! # Fleet-wide parameters: `TN_E2E_PARAMETERS`
//!
//! `TN_E2E_PARAMETERS` holds top-level `parameters.yaml` entries that every start helper merges
//! into the `parameters.yaml` of every node it starts, restarts included, so a lane can run an
//! existing test set with a node switch on without editing a test:
//!
//! ```text
//! TN_E2E_PARAMETERS='pipelined_persist: true; recovery_threads: 4' cargo nextest run ...
//! ```
//!
//! The value is a list of `key: value` entries separated by `;`. A key is a plain top-level name
//! (ASCII letters, digits and `_`) and a value is one line of YAML written into the file as given,
//! so it cannot itself hold a `;`. Whitespace around an entry, its key and its value is trimmed,
//! an empty entry (a trailing `;`) is skipped, and an unset or blank variable means no entries.
//!
//! The entries go in before the test's own [`NodeOverrides`] for that node, so a test that sets
//! the same key keeps its value (see [`NodeOverrides::layered_over`]). The variable is read once
//! per test process, and the first start that applies it prints what it applies. A value that is
//! not a list of entries, or that the node's loader cannot read as `Parameters`, fails the start
//! with an error naming the variable. As with any [`NodeOverrides`] entry, a key the node does not
//! know is ignored by the node, so check the node's `node switches` log line, not the merge.

// ignore for IT test
#![allow(unused_crate_dependencies)]

use clap::Parser;
use escargot::{CargoBuild, CargoRun};
use eyre::WrapErr as _;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use telcoin_network_cli::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand, NoArgs};
use tn_config::{Config, ConfigFmt, ConfigTrait, KeyConfig, Parameters};
use tn_node::launch_node;
use tn_reth::init_reth_defaults;
use tn_types::{test_utils::CommandParser, Address, Genesis, GenesisAccount};
use tracing::{error, info};
// unused deps warnings

/// A telcoin-network binary for the e2e tests.
///
/// Building the node binary via escargot costs ~3-10s of cargo overhead per test process.
/// Setting `TN_BIN_PATH` to a prebuilt binary (see `make build-e2e-bin`) skips that
/// entirely; when it is unset we fall back to an escargot build so `cargo nextest` and
/// `cargo test` still work with no extra setup.
#[derive(Debug)]
pub enum TestBinary {
    /// Prebuilt binary located via the `TN_BIN_PATH` environment variable.
    Prebuilt(PathBuf),
    /// Binary built on demand by escargot.
    Cargo(CargoRun),
}

impl TestBinary {
    /// Build a [`std::process::Command`] that runs this binary.
    ///
    /// Pins every fork-epoch override on the child so spawned nodes run the fork points the
    /// harness states rather than their build defaults (non-adiri builds are otherwise active
    /// from genesis for every fork). With nothing in the harness environment the seed-signature,
    /// multi-workers and PREVRANDAO pins are `u32::MAX`, holding those forks dormant:
    /// wire-identical to pre-fork mainnet for the seed signature, the legacy single-worker layout
    /// for the committee worker list, the legacy `output_digest ^ batch_digest` mix hash for
    /// PREVRANDAO. The leader-seeded-ordering and sub-second-timestamp pins default to `0`
    /// instead: their gates (`tn_types::forks::leader_seeded_ordering_active` and
    /// `tn_types::forks::subsecond_timestamp_active`) conjoin the seed-signature fork
    /// fail-closed, so with the seed fork dormant the seeded ordering and the millisecond
    /// timestamps stay off regardless of these pins, and `0` means "the fork point itself never
    /// blocks; the seed fork governs", which is what a non-adiri production build does. A
    /// harness-level value is forwarded verbatim so a lane can export
    /// `TN_SEED_SIGNATURE_FORK_EPOCH=0`, `TN_MULTI_WORKERS_FORK_EPOCH=1`,
    /// `TN_PREVRANDAO_FORK_EPOCH=0`, `TN_LEADER_SEEDED_ORDERING_FORK_EPOCH=1` or
    /// `TN_SUBSECOND_TIMESTAMP_FORK_EPOCH=1`, and a single test can still override a pin with
    /// its own later `env()` call. Only binaries built with `tn-types/test-utils` (pulled in via
    /// `tn-storage/test-utils`, see `make build-e2e-bin`) consult these variables; production
    /// binaries ignore them.
    ///
    /// The governance-Safe pin is the odd one out: its whole mechanism is `adiri`-gated, so a
    /// default e2e binary neither honors the forwarded value nor reports it (its
    /// `fork_epoch_overrides` row is carried under the same cfg as its consumer). It is forwarded
    /// all the same, with the same dormant `u32::MAX` default, so that the one lane running an
    /// `adiri` binary (`make test-e2e-governance-safe`, see `tests/it/governance_safe_fork.rs`)
    /// pins the fork rather than inheriting the compiled-in constant, and so no other test on
    /// that binary can pick the fork up by accident.
    pub fn command(&self) -> std::process::Command {
        let mut command = match self {
            TestBinary::Prebuilt(path) => std::process::Command::new(path),
            TestBinary::Cargo(run) => run.command(),
        };
        // one loop rather than a block per variable so the forks cannot drift apart in mechanism;
        // they arm independently, so each is read and forwarded on its own, and each carries its
        // own default. The conjoined forks are why the defaults are not uniform: reaching the
        // seeded PREVRANDAO derivation needs the seed fork armed too (`prevrandao_seed_active`),
        // so pinning PREVRANDAO dormant here keeps a seed-armed lane from silently arming it on a
        // non-adiri build, while the leader-seeded and sub-second-timestamp forks are governed by
        // that same seed conjunct and so default to the always-armed 0 rather than dormant
        [
            ("TN_SEED_SIGNATURE_FORK_EPOCH", u32::MAX),
            ("TN_MULTI_WORKERS_FORK_EPOCH", u32::MAX),
            ("TN_PREVRANDAO_FORK_EPOCH", u32::MAX),
            ("TN_LEADER_SEEDED_ORDERING_FORK_EPOCH", 0),
            ("TN_SUBSECOND_TIMESTAMP_FORK_EPOCH", 0),
            ("TN_GOVERNANCE_SAFE_FORK_EPOCH", u32::MAX),
        ]
        .into_iter()
        .for_each(|(var, default)| {
            let fork_epoch = std::env::var(var).unwrap_or_else(|_| default.to_string());
            command.env(var, fork_epoch);
        });
        command
    }
}

/// Resolve the main bin once for all tests.
pub static TELCOIN_BINARY: OnceLock<TestBinary> = OnceLock::new();

/// PBKDF2 round count for test-generated BLS keyfiles.
///
/// Tests intentionally wrap keys with a trivially weak KDF so suites don't spend CPU on
/// 1,000,000-round PBKDF2 per node. The round count is not stored on disk; the spawned node
/// binary, which does not share this constant (its only test hooks are the storage/types
/// `test-utils` features), recovers these keys by trying the weak round count when reading
/// (and warns that they are weakly wrapped).
const INSECURE_TEST_KDF_ROUNDS: u32 = 1;

/// RPC endpoints for a single node across all transports.
#[derive(Debug)]
pub struct NodeEndpoints {
    /// HTTP transport address.
    pub http_url: String,
    /// WS transport address.
    pub ws_url: String,
    /// IPS transport address.
    pub ipc_path: String,
}

/// Execute genesis ceremony inside tempdir
pub fn create_validator_info(
    dir: &Path,
    address: &str,
    passphrase: Option<String>,
) -> eyre::Result<()> {
    create_validator_info_with_workers(dir, address, passphrase, 1)
}

/// Generate all validator worker records through the same CLI arguments operators use.
fn create_validator_info_with_workers(
    dir: &Path,
    address: &str,
    passphrase: Option<String>,
    workers: usize,
) -> eyre::Result<()> {
    let workers = workers.to_string();
    let keys_command = CommandParser::<KeyArgs>::try_parse_from([
        "tn",
        "generate",
        "validator",
        "--address",
        address,
        "--workers",
        &workers,
    ])?;
    keys_command.args.execute_insecure(
        dir.to_path_buf(),
        passphrase.map(Into::into),
        INSECURE_TEST_KDF_ROUNDS,
    )
}

/// Execute observer config inside tempdir
fn create_observer_info(
    datadir: PathBuf,
    passphrase: Option<String>,
    workers: usize,
) -> eyre::Result<()> {
    let workers = workers.to_string();
    // keytool
    let keys_command = CommandParser::<KeyArgs>::try_parse_from([
        "tn",
        "generate",
        "observer",
        "--address",
        "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "--workers",
        &workers,
    ])?;
    keys_command.args.execute_insecure(
        datadir,
        passphrase.map(Into::into),
        INSECURE_TEST_KDF_ROUNDS,
    )
}

/// Create validator info, genesis ceremony, and configure local testnet.
pub fn config_local_testnet(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
) -> eyre::Result<()> {
    config_local_testnet_with_epoch_duration(temp_path, passphrase, accounts, None)
}

/// Like [`config_local_testnet`], but lets the caller override the epoch duration in seconds.
/// Pass `None` to use the genesis CLI default (8 hours). Useful for tests that need to
/// observe an epoch boundary within the test budget.
pub fn config_local_testnet_with_epoch_duration(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
    epoch_duration_secs: Option<u32>,
) -> eyre::Result<()> {
    config_local_testnet_inner(temp_path, passphrase, accounts, epoch_duration_secs, &[], None)
}

/// Like [`config_local_testnet_with_epoch_duration`], but also lets the caller set per-worker
/// fee strategies at genesis via `--worker-fee-config`.
///
/// Each entry is a `"WORKER_ID:STRATEGY:VALUE"` string, where `STRATEGY` is `0` for EIP-1559
/// (`VALUE` = target gas) or `1` for a static fee (`VALUE` = fee in wei). Entries must cover
/// contiguous worker ids starting at 0 (the genesis ceremony validates this).
/// Validators and the observer provision that many worker records through `keytool generate`.
///
/// When `worker_fee_configs` is empty this is identical to
/// [`config_local_testnet_with_epoch_duration`]: the genesis CLI default
/// `Eip1559 { target_gas: u64::MAX }` applies, which keeps every worker pinned at
/// `MIN_PROTOCOL_BASE_FEE`. When it is non-empty, the provided configs *replace* that default —
/// do not also pass the default value.
pub fn config_local_testnet_with_worker_fee_configs(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
    epoch_duration_secs: Option<u32>,
    worker_fee_configs: &[&str],
) -> eyre::Result<()> {
    config_local_testnet_inner(
        temp_path,
        passphrase,
        accounts,
        epoch_duration_secs,
        worker_fee_configs,
        None,
    )
}

/// Like [`config_local_testnet`], but sets a restart-test garbage-collection depth so a
/// killed CVV crosses the `CvvInactive` demotion threshold after a shorter downtime.
pub fn config_local_testnet_with_gc_depth(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
    gc_depth: Option<u32>,
) -> eyre::Result<()> {
    config_local_testnet_inner(temp_path, passphrase, accounts, None, &[], gc_depth)
}

/// Like [`config_local_testnet_with_gc_depth`], but also sets the epoch duration in seconds, so a
/// restart test can cross epoch boundaries within the test budget.
pub fn config_local_testnet_with_gc_depth_and_epoch_duration(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
    gc_depth: u32,
    epoch_duration_secs: u32,
) -> eyre::Result<()> {
    config_local_testnet_inner(
        temp_path,
        passphrase,
        accounts,
        Some(epoch_duration_secs),
        &[],
        Some(gc_depth),
    )
}

/// Shared implementation for the `config_local_testnet*` helpers.
///
/// Builds the genesis CLI argument vector, optionally appending `--epoch-duration-in-secs` and one
/// `--worker-fee-config` flag per entry in `worker_fee_configs`, runs the genesis ceremony, and
/// distributes the resulting genesis/committee/parameters files to every validator and the
/// observer.
fn config_local_testnet_inner(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
    epoch_duration_secs: Option<u32>,
    worker_fee_configs: &[&str],
    gc_depth: Option<u32>,
) -> eyre::Result<()> {
    let validators = [
        ("validator-1", "0x1111111111111111111111111111111111111111"),
        ("validator-2", "0x2222222222222222222222222222222222222222"),
        ("validator-3", "0x3333333333333333333333333333333333333333"),
        ("validator-4", "0x4444444444444444444444444444444444444444"),
    ];

    // create shared genesis dir
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;
    // The fee configuration and generated identities describe the same worker prefix.
    let workers = worker_fee_configs.len().max(1);
    validators.iter().try_for_each(|(v, addr)| -> eyre::Result<()> {
        let dir = temp_path.join(v);
        // init genesis ceremony to create committee files
        create_validator_info_with_workers(&dir, addr, passphrase.clone(), workers)?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
        Ok(())
    })?;

    // create committee from shared genesis dir
    let mut genesis_args: Vec<String> = vec![
        "tn".into(),
        "--basefee-address".into(),
        "0x9999999999999999999999999999999999999999".into(),
        "--consensus-registry-owner".into(),
        "0x00000000000000000000000000000000000007a0".into(),
        "--dev-funded-account".into(),
        "test-source".into(),
        "--max-header-delay-ms".into(),
        "500".into(),
        "--min-header-delay-ms".into(),
        "250".into(),
        "--max-batch-delay-ms".into(),
        "250".into(),
    ];
    if let Some(duration) = epoch_duration_secs {
        genesis_args.push("--epoch-duration-in-secs".into());
        genesis_args.push(duration.to_string());
    }
    if let Some(gc_depth) = gc_depth {
        genesis_args.push("--gc-depth".into());
        genesis_args.push(gc_depth.to_string());
    }
    // Append one `--worker-fee-config` flag per provided entry. When empty, clap falls back to the
    // genesis default (`0:0:u64::MAX`, an inert EIP-1559 strategy). Any provided configs replace
    // that default, so callers must supply contiguous worker ids starting at 0.
    for cfg in worker_fee_configs {
        genesis_args.push("--worker-fee-config".into());
        genesis_args.push((*cfg).to_string());
    }
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from(genesis_args);
    create_committee_command.args.execute(shared_genesis_dir.clone())?;

    // Every node in this harness runs on one host and advertises `http://127.0.0.1:<port>`, which
    // the observer's transaction forwarder refuses by default (issue #1092). This is the
    // single-host deployment the opt-in exists for, so enable it on the shared parameters before
    // they are distributed, rather than leaving observer forwarding silently dead in e2e tests.
    let parameters_path = shared_genesis_dir.join("parameters.yaml");
    let mut parameters: Parameters = Config::load_from_path(&parameters_path, ConfigFmt::YAML)?;
    parameters.allow_private_forward_targets = true;
    Config::write_to_path(&parameters_path, &parameters, ConfigFmt::YAML)?;

    // If provided optional accounts then hack them into genesis now...
    if let Some(accounts) = accounts {
        let data_dir = shared_genesis_dir.join("genesis/genesis.yaml");
        let genesis: Genesis = Config::load_from_path(&data_dir, ConfigFmt::YAML)?;
        let genesis = genesis.extend_accounts(accounts);
        Config::write_to_path(&data_dir, &genesis, ConfigFmt::YAML)?;
    }

    for (v, _addr) in validators.iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(dir.join("genesis"))?;
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    config_observer(temp_path, "observer", passphrase, workers)
}

/// Give an observer its own keys under `temp_path/<name>` and copy the network's genesis,
/// committee and parameters files next to them.
///
/// The `config_local_testnet*` helpers call it for the `observer` datadir. It needs the genesis
/// ceremony's output in `temp_path/shared-genesis`, so a test that wants another observer, with an
/// identity of its own, calls it after one of those helpers. `workers` must match the worker count
/// the network was configured with.
pub fn config_observer(
    temp_path: &Path,
    name: &str,
    passphrase: Option<String>,
    workers: usize,
) -> eyre::Result<()> {
    let dir = temp_path.join(name);
    create_observer_info(dir.clone(), passphrase, workers)?;

    let shared_genesis_dir = temp_path.join("shared-genesis");
    std::fs::create_dir_all(dir.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        dir.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        dir.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    Ok(())
}

/// Settings a test layers onto one node of a local fleet, on top of the harness's own setup.
///
/// Both lists are empty by default, and an empty [`NodeOverrides`] starts the node exactly as the
/// harness does without one: on a datadir no earlier start gave entries to, its `parameters.yaml`
/// is neither read nor rewritten, and its process gets no extra variable. The one exception is
/// `TN_E2E_PARAMETERS` (see the crate docs), whose entries every start helper puts in before the
/// node's own (see [`NodeOverrides::apply_with_e2e_parameters`]).
///
/// - `parameters` holds top-level `parameters.yaml` entries as `(key, raw YAML value)` pairs. The
///   start helpers merge them into the node's own `parameters.yaml`, the file the `config_*`
///   genesis helpers wrote, right before they spawn the node (see
///   [`NodeOverrides::apply_parameters`] and [`merge_parameters_yaml`]). Every start rewrites the
///   file as the harness wrote it plus that start's entries, so a node restarted with other
///   entries, or with none, does not keep an earlier start's. The harness knows none of the keys:
///   the node's `Parameters` ignores a top-level key it does not know, so a test can name a switch
///   whether or not the node under test reads it. The same holds for a misspelled key, or one the
///   node reads from another file, so a test should check the setting took effect from the node's
///   own report of it (its `node switches` log line) rather than trust the merge.
/// - `env` holds environment variables for the node's process only, set after the variables
///   [`TestBinary::command`] forwards, so an entry overrides a forwarded variable of the same name.
///   This is how a test arms the hooks in `tn_types::test_hooks` (for example
///   `TN_TEST_EXECUTION_DELAY_MS`) on chosen nodes. Never export such a variable from the harness
///   process instead: every node spawned afterwards would inherit it.
///
/// A setting that changes what the chain computes, such as a `TN_*_FORK_EPOCH` variable or the
/// `basefee_address` parameter, forks the fleet when only some nodes carry it. Give those to every
/// node or to none, unless the divergence is what the test is about.
///
/// [`FleetOverrides`] assigns these per node across a fleet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeOverrides {
    /// Top-level `parameters.yaml` entries, `(key, raw YAML value)`, merged in order, so a later
    /// entry for the same key wins.
    pub parameters: Vec<(String, String)>,
    /// Environment variables for the node's process, `(name, value)`, set in order, so a later
    /// entry for the same name wins.
    pub env: Vec<(String, String)>,
}

impl NodeOverrides {
    /// Add the top-level `parameters.yaml` entry `key: value`.
    ///
    /// `value` is raw one-line YAML, written into the file as given: `true`, `400`, `750ms`, or a
    /// flow collection such as `[1, 2]`.
    pub fn parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.push((key.into(), value.into()));
        self
    }

    /// Add the environment variable `name=value` for the node's process.
    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((name.into(), value.into()));
        self
    }

    /// Whether this overrides nothing.
    pub fn is_empty(&self) -> bool {
        self.parameters.is_empty() && self.env.is_empty()
    }

    /// These overrides with the `parameters.yaml` entries of `fleet_wide` placed before their own.
    ///
    /// Entries merge in order and a later entry for a key wins (see [`merge_parameters_yaml`]), so
    /// a key this node sets keeps this node's value and every other `fleet_wide` entry is added.
    /// The environment variables are kept as they are.
    pub fn layered_over(&self, fleet_wide: &[(String, String)]) -> NodeOverrides {
        let parameters = fleet_wide.iter().chain(&self.parameters).cloned().collect();
        NodeOverrides { parameters, env: self.env.clone() }
    }

    /// [`NodeOverrides::apply_parameters`] with the entries of `TN_E2E_PARAMETERS` placed before
    /// this node's own (see [`NodeOverrides::layered_over`]); the start helpers call this before
    /// every start of a node, restarts included.
    ///
    /// With the variable unset or blank this is [`NodeOverrides::apply_parameters`]. The first call
    /// in a test process that applies entries prints them once.
    ///
    /// # Errors
    ///
    /// A variable that is not a list of `key: value` entries, or a merged file the node's loader
    /// cannot read, fails with an error naming `TN_E2E_PARAMETERS`; the node's file is then left
    /// as it was.
    pub fn apply_with_e2e_parameters(&self, node_dir: &Path) -> eyre::Result<()> {
        self.apply_layered(node_dir, e2e_parameters()?)
    }

    /// [`NodeOverrides::apply_parameters`] of these overrides layered over `fleet_wide`, the
    /// entries of `TN_E2E_PARAMETERS`, with an error that names the variable when it set any.
    fn apply_layered(&self, node_dir: &Path, fleet_wide: &[(String, String)]) -> eyre::Result<()> {
        if fleet_wide.is_empty() {
            return self.apply_parameters(node_dir);
        }
        self.layered_over(fleet_wide).apply_parameters(node_dir).wrap_err_with(|| {
            format!(
                "{E2E_PARAMETERS_VAR} entries {fleet_wide:?} (merged first) and the test's own \
                 entries {:?} do not apply to {}",
                self.parameters,
                node_dir.display()
            )
        })
    }

    /// Write `<node_dir>/parameters.yaml` as the harness wrote it plus
    /// [`NodeOverrides::parameters`] (see [`merge_parameters_yaml`]).
    ///
    /// The first application with entries keeps the harness's file beside it as
    /// `parameters.yaml.harness-base`, and every later one merges into that copy, so applying the
    /// same entries again leaves the file as it was and applying other entries does not keep the
    /// earlier ones. Applying no entries puts the harness's file back if an earlier application
    /// changed it, and otherwise neither reads nor rewrites anything. The start helpers call this
    /// before every start of a node, restarts included.
    ///
    /// The merged text is written to `parameters.yaml.merging` first and moved into place only
    /// once the node's own loader reads it as `Parameters`, so a malformed value fails here and
    /// leaves the node's file as it was (and the refused merge in place for inspection). That
    /// proves the file deserializes, not that the node accepts every value: the epoch timing
    /// checks, for one, run when the node enters an epoch it can vote in, so a duration of the
    /// right type that those checks refuse still fails there.
    ///
    /// The start helpers reach this through [`NodeOverrides::apply_with_e2e_parameters`].
    pub fn apply_parameters(&self, node_dir: &Path) -> eyre::Result<()> {
        let path = node_dir.join("parameters.yaml");
        let harness_base = node_dir.join("parameters.yaml.harness-base");
        if self.parameters.is_empty() {
            if harness_base.exists() {
                std::fs::rename(&harness_base, &path).wrap_err_with(|| {
                    format!("restoring {} from {}", path.display(), harness_base.display())
                })?;
            }
            return Ok(());
        }
        if !harness_base.exists() {
            std::fs::copy(&path, &harness_base).wrap_err_with(|| {
                format!("keeping {} as {}", path.display(), harness_base.display())
            })?;
        }
        let base = std::fs::read_to_string(&harness_base)
            .wrap_err_with(|| format!("reading {} to merge overrides", harness_base.display()))?;
        let merged = merge_parameters_yaml(&base, &self.parameters)?;
        let staged = node_dir.join("parameters.yaml.merging");
        std::fs::write(&staged, merged)
            .wrap_err_with(|| format!("writing merged overrides to {}", staged.display()))?;
        Config::load_from_path::<Parameters>(&staged, ConfigFmt::YAML).wrap_err_with(|| {
            format!(
                "{} does not load as node parameters after merging {:?}",
                staged.display(),
                self.parameters
            )
        })?;
        std::fs::rename(&staged, &path)
            .wrap_err_with(|| format!("moving {} to {}", staged.display(), path.display()))?;
        Ok(())
    }
}

/// Per-node [`NodeOverrides`] for a local fleet, keyed by the name of each node's datadir under
/// the test's temp dir.
///
/// The start helpers look a node up by that name, so a fleet means the same nodes to every helper
/// and to a restart of a subset: `validator-<instance + 1>` for the helpers that take an
/// `instance` (the `config_local_testnet*` layout), the name in the `validators` slice for those
/// that take one. A node with no entry starts as the harness starts it. The builders add one
/// setting to every node they name, so a mixed fleet reads as one chain:
///
/// ```
/// use e2e_tests::FleetOverrides;
///
/// // validators 1 and 2 (instances 0 and 1) run with the switch, 3 and 4 without it
/// let overrides = FleetOverrides::default().parameter(
///     ["validator-1", "validator-2"],
///     "pipelined_persist",
///     "true",
/// );
/// assert!(overrides.node("validator-3").is_empty());
///
/// // three of four validators execute each output 400 ms later
/// let overrides = FleetOverrides::default().env(
///     ["validator-2", "validator-3", "validator-4"],
///     "TN_TEST_EXECUTION_DELAY_MS",
///     "400",
/// );
/// assert!(overrides.node("validator-1").is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FleetOverrides {
    /// The overrides of each node that has any, by datadir name.
    nodes: BTreeMap<String, NodeOverrides>,
}

impl FleetOverrides {
    /// Add the top-level `parameters.yaml` entry `key: value` (see [`NodeOverrides::parameter`])
    /// to every node in `nodes`, named by datadir.
    pub fn parameter(
        mut self,
        nodes: impl IntoIterator<Item = impl Into<String>>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let (key, value) = (key.into(), value.into());
        for node in nodes {
            self.nodes
                .entry(node.into())
                .or_default()
                .parameters
                .push((key.clone(), value.clone()));
        }
        self
    }

    /// Add the environment variable `name=value` (see [`NodeOverrides::env`]) to every node in
    /// `nodes`, named by datadir.
    pub fn env(
        mut self,
        nodes: impl IntoIterator<Item = impl Into<String>>,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let (name, value) = (name.into(), value.into());
        for node in nodes {
            self.nodes.entry(node.into()).or_default().env.push((name.clone(), value.clone()));
        }
        self
    }

    /// Replace the overrides of the node whose datadir is `node` with `overrides`.
    pub fn set_node(mut self, node: impl Into<String>, overrides: NodeOverrides) -> Self {
        self.nodes.insert(node.into(), overrides);
        self
    }

    /// The overrides of the node whose datadir is `node`, empty when the fleet sets none for it.
    pub fn node(&self, node: &str) -> &NodeOverrides {
        /// The overrides of a node the fleet sets nothing for.
        static NONE: NodeOverrides = NodeOverrides { parameters: Vec::new(), env: Vec::new() };
        self.nodes.get(node).unwrap_or(&NONE)
    }
}

/// `base`, the text of a `parameters.yaml`, with each `(key, value)` of `overrides` as a top-level
/// `key: value` entry.
///
/// A pure text merge, so it needs no knowledge of the keys and leaves every other line of `base`
/// byte for byte. Each entry, in order, first removes the top-level entry `key` already has in the
/// text (its `key:` line at column 0 and the indented or `- ` lines of a block value below it),
/// then appends `key: value` as the last line. A later entry for a key therefore replaces an
/// earlier one, and merging the same overrides twice gives the text the first merge gave. With no
/// overrides `base` comes back unchanged.
///
/// `base` must be one YAML document whose top level is a block mapping, the shape `serde_yaml`
/// writes. A key must be a plain top-level name (ASCII letters, digits and `_`), and a value one
/// non-empty line of YAML; anything else is an error. The value is not parsed here:
/// [`NodeOverrides::apply_parameters`] checks the merged file against the node's loader.
pub fn merge_parameters_yaml(base: &str, overrides: &[(String, String)]) -> eyre::Result<String> {
    if overrides.is_empty() {
        return Ok(base.to_owned());
    }
    let mut lines: Vec<String> = base.lines().map(str::to_owned).collect();
    for (key, value) in overrides {
        eyre::ensure!(
            !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "parameters override key {key:?} is not a plain top-level key"
        );
        eyre::ensure!(
            !value.trim().is_empty() && !value.contains(['\n', '\r']),
            "parameters override `{key}` needs a one-line YAML value, got {value:?}"
        );
        if let Some(start) = lines.iter().position(|line| starts_top_level_entry(line, key)) {
            let end = lines[start + 1..]
                .iter()
                .position(|line| !continues_block_value(line))
                .map_or(lines.len(), |offset| start + 1 + offset);
            lines.drain(start..end);
        }
        lines.push(format!("{key}: {value}"));
    }
    let mut merged = lines.join("\n");
    merged.push('\n');
    Ok(merged)
}

/// Whether `line` opens the top-level mapping entry `key`: `key:` at column 0, followed by the
/// end of the line or whitespace.
fn starts_top_level_entry(line: &str, key: &str) -> bool {
    line.strip_prefix(key)
        .and_then(|rest| rest.strip_prefix(':'))
        .is_some_and(|rest| rest.is_empty() || rest.starts_with([' ', '\t']))
}

/// Whether `line` still belongs to the block value of the top-level entry above it: a blank
/// line, an indented line, or a sequence item at column 0 (`serde_yaml` may write a top-level
/// key's sequence unindented). The next top-level key or a document marker ends the value.
fn continues_block_value(line: &str) -> bool {
    line.trim().is_empty() || line.starts_with([' ', '\t']) || line == "-" || line.starts_with("- ")
}

/// The environment variable whose `parameters.yaml` entries every start helper merges into every
/// node it starts (see the crate docs).
pub const E2E_PARAMETERS_VAR: &str = "TN_E2E_PARAMETERS";

/// The entries of [`E2E_PARAMETERS_VAR`], read and parsed once per test process (see
/// [`parse_e2e_parameters`]); empty when the variable is unset or blank.
///
/// The first call prints the entries it read, once, on stderr and to the test's tracing, so a
/// lane's output shows what every node of the test was given.
///
/// # Errors
///
/// A value [`parse_e2e_parameters`] refuses, on this and every later call.
pub fn e2e_parameters() -> eyre::Result<&'static [(String, String)]> {
    /// The parsed variable, or the parse error as text (a report is not `Sync`).
    static PARSED: OnceLock<Result<Vec<(String, String)>, String>> = OnceLock::new();
    let parsed = PARSED.get_or_init(|| {
        // a non-unicode value is read lossily so it fails the parse rather than reading as unset
        let raw = std::env::var_os(E2E_PARAMETERS_VAR)
            .map(|raw| raw.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parsed = parse_e2e_parameters(&raw).map_err(|e| format!("{e:#}"))?;
        if !parsed.is_empty() {
            let applied =
                parsed.iter().map(|(key, value)| format!("{key}: {value}")).collect::<Vec<_>>();
            eprintln!(
                "e2e: {E2E_PARAMETERS_VAR} applies {applied:?} to every node this test starts \
                 (a test's own entry for a key wins)"
            );
            info!(?applied, "e2e: {E2E_PARAMETERS_VAR} applies to every node this test starts");
        }
        Ok(parsed)
    });
    parsed.as_deref().map_err(|e| eyre::eyre!("{e}"))
}

/// Parse a value of [`E2E_PARAMETERS_VAR`] into `(key, value)` entries, in order.
///
/// The value is `key: value` entries separated by `;`. Each entry is split at its first `:`, and
/// its key and value are trimmed; an entry that is empty after trimming is skipped, so a blank
/// value yields no entries. A key must be a plain top-level name (ASCII letters, digits and `_`)
/// and a value one non-empty line. The value is not read as YAML here: the start helpers check the
/// merged file against the node's loader.
///
/// # Errors
///
/// An entry without a `:`, with a key that is not a plain name, or with an empty or multi-line
/// value; the error names [`E2E_PARAMETERS_VAR`] and the entry.
pub fn parse_e2e_parameters(raw: &str) -> eyre::Result<Vec<(String, String)>> {
    let mut entries = Vec::new();
    for entry in raw.split(';').map(str::trim).filter(|entry| !entry.is_empty()) {
        let (key, value) = entry.split_once(':').ok_or_else(|| {
            eyre::eyre!("{E2E_PARAMETERS_VAR} entry {entry:?} is not `key: value` (in {raw:?})")
        })?;
        let (key, value) = (key.trim(), value.trim());
        eyre::ensure!(
            !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "{E2E_PARAMETERS_VAR} entry {entry:?} needs a plain top-level key (ASCII letters, \
             digits and `_`), got {key:?} (in {raw:?})"
        );
        eyre::ensure!(
            !value.is_empty() && !value.contains(['\n', '\r']),
            "{E2E_PARAMETERS_VAR} entry {entry:?} needs a one-line value (in {raw:?})"
        );
        entries.push((key.to_owned(), value.to_owned()));
    }
    Ok(entries)
}

/// Create validator info, genesis ceremony, and spawn node command.
pub fn spawn_local_testnet(
    temp_path: &Path,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
) -> eyre::Result<Vec<NodeEndpoints>> {
    config_local_testnet(temp_path, None, accounts)?;

    let validators = ["validator-1", "validator-2", "validator-3", "validator-4"];
    let (tx, rx) = std::sync::mpsc::channel();
    let mut endpoints = Vec::new();

    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        NodeOverrides::default().apply_with_e2e_parameters(&dir)?;
        let rpc_port = tn_types::get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port");

        let ws_port = tn_types::get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral ws port");

        // IPC - unique path under temp_path to avoid cross-test conflicts
        let ipc_path = temp_path.join(format!("{v}.ipc"));
        let ipc_path_str = ipc_path.to_string_lossy().to_string();

        endpoints.push(NodeEndpoints {
            http_url: format!("http://127.0.0.1:{rpc_port}"),
            ws_url: format!("ws://127.0.0.1:{ws_port}"),
            ipc_path: ipc_path_str.clone(),
        });

        // seed reth's process-global defaults before the parse resolves
        // `--txpool.max-account-slots`
        init_reth_defaults();
        let command = NodeCommand::parse_from([
            "tn",
            "--http",
            "--http.port",
            &rpc_port.to_string(),
            "--ws",
            "--ws.port",
            &ws_port.to_string(),
            "--ipcpath",
            &ipc_path_str,
        ]);

        let key_config = KeyConfig::read_config(&dir, Some("it_test_pass".to_string().into()))?;
        let tx = tx.clone();
        let name = v.to_string();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .thread_name("telcoin-network")
                .enable_io()
                .enable_time()
                .build()
                .expect("can build tokio runtime");

            runtime.block_on(async move {
                match command.execute(
                    dir,
                    key_config,
                    |builder, _: NoArgs, tn_datadir, passphrase, version| {
                        launch_node(builder, tn_datadir, passphrase, version)
                    },
                ) {
                    Ok(handle) => {
                        let err = handle.await;
                        error!("{name} exited: {err:?}");
                    }
                    Err(e) => {
                        let msg = format!("{name} failed to start: {e:?}");
                        error!("{msg}");
                        let _ = tx.send(msg);
                    }
                }
            });
        });
    }

    // Give threads a moment to fail fast, then check for early errors
    std::thread::sleep(std::time::Duration::from_millis(500));
    if let Ok(err) = rx.try_recv() {
        eyre::bail!("Node startup failed: {err}");
    }

    Ok(endpoints)
}

/// Verify HTTP, WS, and IPC transports are all reachable for a node.
pub async fn verify_all_transports(endpoint: &NodeEndpoints) -> eyre::Result<()> {
    use alloy::{
        network::Ethereum,
        providers::{Provider, ProviderBuilder, RootProvider},
    };

    // HTTP
    let http_provider = ProviderBuilder::new().connect_http(endpoint.http_url.parse()?);
    http_provider.get_chain_id().await?;

    // WS
    let ws_provider: RootProvider<Ethereum> = RootProvider::connect(&endpoint.ws_url)
        .await
        .map_err(|e| eyre::eyre!("WS connect failed for {}: {e}", endpoint.ws_url))?;
    ws_provider.get_chain_id().await?;

    // IPC
    let ipc_provider: RootProvider<Ethereum> = RootProvider::connect(&endpoint.ipc_path)
        .await
        .map_err(|e| eyre::eyre!("IPC connect failed for {}: {e}", endpoint.ipc_path))?;
    ipc_provider.get_chain_id().await?;

    Ok(())
}

/// Configure a command to write stdout to a per-node log file under `test_logs/`.
///
/// Logs are written to `crates/e2e-tests/test_logs/<test>/node<instance>-run<run>.log`.
pub fn setup_log_dir(
    command: &mut std::process::Command,
    instance: impl std::fmt::Display,
    test: &str,
    run: u32,
) -> std::path::PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let log_dir = std::path::PathBuf::from(manifest_dir).join("test_logs");
    let test_dir = log_dir.join(test);
    std::fs::create_dir_all(&test_dir).expect("failed to create test log dir");
    let out_file = std::fs::File::create(test_dir.join(format!("node{instance}-run{run}.log")))
        .expect("valid log file");
    let stdout: std::process::Stdio = out_file.into();
    command.stdout(stdout);

    // Capture stderr (panics, error-level output) to a separate log file
    let err_file =
        std::fs::File::create(test_dir.join(format!("node{instance}-run{run}.stderr.log")))
            .expect("valid stderr log file");
    let stderr: std::process::Stdio = err_file.into();
    command.stderr(stderr);

    test_dir
}

/// Retrieve the main project binary, resolving it once for the whole test process.
///
/// Honors `TN_BIN_PATH`: when set, the prebuilt binary at that path is used and no
/// per-process cargo build runs. Otherwise the binary is built once via escargot.
pub fn get_telcoin_network_binary() -> &'static TestBinary {
    TELCOIN_BINARY.get_or_init(|| {
        if let Ok(prebuilt) = std::env::var("TN_BIN_PATH") {
            let path = PathBuf::from(&prebuilt);
            assert!(path.is_file(), "TN_BIN_PATH is set to {prebuilt:?} but no file exists there");
            info!("using prebuilt telcoin-network binary from TN_BIN_PATH: {prebuilt}");
            return TestBinary::Prebuilt(path);
        }

        // TN_BIN_PATH is unset, so build the node binary in-process via escargot. Under the
        // `e2e` profile (opt-level 2) this is a multi-minute compile, and nextest captures
        // stdout/stderr, so without this notice the first e2e test looks frozen until the build
        // finishes. Announce it on stderr (shown on completion, and live under `--no-capture`) so
        // the delay is attributable rather than an invisible hang.
        eprintln!(
            "e2e: TN_BIN_PATH not set; building telcoin-network via cargo before the first test. \
             This can take several minutes (opt-level 2 under the `e2e` profile), and nextest \
             hides it until the build finishes (add `--no-capture` to watch it). To skip it, run \
             `make test-e2e`, or prebuild once and point TN_BIN_PATH at the built binary \
             (see crates/e2e-tests/README.md)."
        );
        info!("building main binary for e2e tests");
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let path = PathBuf::from(manifest_dir);
        let workspace_root =
            path.parent().and_then(|p| p.parent()).expect("Cannot find workspace root");

        // Build under the `e2e` profile (opt-level 2, safety checks kept on; defined in
        // .cargo/config.toml) so this in-process build and `make build-e2e-bin` produce and
        // share one set of artifacts under `<target root>/e2e/`.
        //
        // No `.current_target()`: passing `--target <triple>` would emit into
        // `<target root>/<triple>/e2e/`, a tree that shares no artifacts with the plain
        // `<target root>/e2e/` that `make build-e2e-bin` and ordinary `cargo build --profile e2e`
        // populate, forcing a guaranteed cold rebuild. Building into `<target root>/e2e/` lets
        // escargot reuse an already-compiled binary (reported "Fresh" by cargo) instead.
        TestBinary::Cargo(
            CargoBuild::new()
                .bin("telcoin-network")
                .features("tn-storage/test-utils")
                // Match the Makefile's `build-e2e-bin`: opt-level 2 with debug-assertions and
                // overflow-checks kept on (see `[profile.e2e]` in .cargo/config.toml).
                .args(["--profile", "e2e"])
                .manifest_path(workspace_root.join("Cargo.toml"))
                // escargot's `.target_dir()` emits a `--target-dir` CLI flag, which outranks the
                // `CARGO_TARGET_DIR` environment variable. Honor the developer's configured
                // target root when set (matching `make build-e2e-bin` and `test-and-attest.sh`)
                // and fall back to cargo's default `<workspace root>/target` otherwise. An empty
                // value counts as unset (mirroring the shell `:-` fallback at the other two
                // sites), and a relative value is anchored at the workspace root so this in-test
                // build shares one tree with the root-invoked builds even though nextest sets
                // the test cwd to the package directory (`Path::join` keeps an absolute value
                // as-is).
                .target_dir(
                    std::env::var_os("CARGO_TARGET_DIR")
                        .filter(|dir| !dir.is_empty())
                        .map(|dir| workspace_root.join(dir))
                        .unwrap_or_else(|| workspace_root.join("target")),
                )
                .run()
                .expect("Failed to build telcoin-network binary"),
        )
    })
}

/// The older node binary from `TN_BIN_PATH_PREV`, resolved once for the whole test process;
/// `None` when the variable is unset.
static PREVIOUS_TELCOIN_BINARY: OnceLock<Option<TestBinary>> = OnceLock::new();

/// Retrieve the node binary built from an older ref (a release, or the last commit before a
/// fork landed), for the mixed-binary (rolling upgrade) tests that run an older node version
/// beside the current one or upgrade a running network's binary in place.
///
/// Honors `TN_BIN_PATH_PREV`, a prebuilt binary only: unlike [`get_telcoin_network_binary`] there
/// is no escargot fallback, because cargo can only build the tree in front of it, not an older
/// ref. `make build-e2e-bin-prev` builds that binary (as does `make build-e2e-bin` run at the
/// older ref), and the tests receive its path through `TN_BIN_PATH_PREV`. When the variable is
/// unset this returns `None` and a test that needs the binary skips with a warning. A variable
/// that is set but names no file panics, so a mistyped path cannot pass as a skipped run. The
/// binary must come from `make build-e2e-bin` at the older ref, so it honors the same
/// `TN_*_FORK_EPOCH` variables that [`TestBinary::command`] forwards (except any fork that ref
/// does not know, which it ignores).
pub fn get_previous_telcoin_network_binary() -> Option<&'static TestBinary> {
    PREVIOUS_TELCOIN_BINARY
        .get_or_init(|| {
            let prebuilt = std::env::var("TN_BIN_PATH_PREV").ok()?;
            let path = PathBuf::from(&prebuilt);
            assert!(
                path.is_file(),
                "TN_BIN_PATH_PREV is set to {prebuilt:?} but no file exists there"
            );
            info!("using previous telcoin-network binary from TN_BIN_PATH_PREV: {prebuilt}");
            Some(TestBinary::Prebuilt(path))
        })
        .as_ref()
}

// imports for traits used in faucet tests only
#[cfg(feature = "faucet")]
use jsonrpsee::core::client::ClientT;
#[cfg(feature = "faucet")]
use std::str::FromStr as _;
#[cfg(feature = "faucet")]
use tn_types::U256;

/// RPC request to continually check until an account balance is above 0.
///
/// Warning: this should only be called with a timeout - could result in infinite loop otherwise.
#[cfg(feature = "faucet")]
pub async fn ensure_account_balance_infinite_loop(
    client: &jsonrpsee::http_client::HttpClient,
    address: Address,
    expected_bal: U256,
) -> eyre::Result<U256> {
    while let Ok(bal) =
        client.request::<String, _>("eth_getBalance", jsonrpsee::rpc_params!(address)).await
    {
        tracing::debug!(target: "faucet-test", "{address} bal: {bal:?}");
        let balance = U256::from_str(&bal)?;

        // return Ok if expected bal
        if balance == expected_bal {
            return Ok(balance);
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(U256::ZERO)
}

#[cfg(test)]
mod tests {
    use super::{
        merge_parameters_yaml, parse_e2e_parameters, FleetOverrides, NodeOverrides,
        E2E_PARAMETERS_VAR,
    };
    use std::time::Duration;
    use tn_config::{Config, ConfigFmt, ConfigTrait as _, Parameters};

    /// `(key, value)` pairs as owned strings, the shape [`NodeOverrides::parameters`] holds.
    fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect()
    }

    /// A file shaped like `serde_yaml`'s output: scalars, a nested block mapping holding an
    /// indented sequence, and a sequence written at column 0.
    const BASE: &str = "---\nfirst: 1\nnested:\n  inner: 2\n  list:\n    - a\n    - b\nlast: x\n\
                        seq:\n- 1\n- 2\ntail: true\n";

    #[test]
    fn merge_without_overrides_returns_base_unchanged() {
        let base = "---\nfirst: 1"; // no final newline, which a rewrite would add
        assert_eq!(merge_parameters_yaml(base, &[]).unwrap(), base);
    }

    #[test]
    fn merge_appends_a_new_key_and_keeps_every_other_line() {
        let merged = merge_parameters_yaml(BASE, &pairs(&[("pipelined_persist", "true")])).unwrap();
        assert_eq!(merged, format!("{BASE}pipelined_persist: true\n"));
    }

    #[test]
    fn merge_replaces_an_existing_scalar_entry() {
        let merged = merge_parameters_yaml(BASE, &pairs(&[("first", "9")])).unwrap();
        assert_eq!(
            merged,
            "---\nnested:\n  inner: 2\n  list:\n    - a\n    - b\nlast: x\nseq:\n- 1\n- 2\n\
             tail: true\nfirst: 9\n"
        );
    }

    #[test]
    fn merge_replaces_a_whole_block_value() {
        let merged =
            merge_parameters_yaml(BASE, &pairs(&[("nested", "{}"), ("seq", "[3]")])).unwrap();
        assert_eq!(merged, "---\nfirst: 1\nlast: x\ntail: true\nnested: {}\nseq: [3]\n");
    }

    #[test]
    fn merge_matches_whole_keys_only() {
        // `last` must not match `last_round`, and an indented `first:` is not top level
        let base = "---\nlast_round: 1\nnested:\n  first: 2\n";
        let merged = merge_parameters_yaml(base, &pairs(&[("last", "x"), ("first", "y")])).unwrap();
        assert_eq!(merged, format!("{base}last: x\nfirst: y\n"));
    }

    #[test]
    fn merge_later_entry_wins_and_is_idempotent() {
        let overrides = pairs(&[("switch", "false"), ("first", "2"), ("switch", "true")]);
        let once = merge_parameters_yaml(BASE, &overrides).unwrap();
        assert_eq!(once.matches("switch:").count(), 1, "{once}");
        assert!(once.ends_with("first: 2\nswitch: true\n"), "{once}");
        assert_eq!(merge_parameters_yaml(&once, &overrides).unwrap(), once);
    }

    #[test]
    fn merge_rejects_keys_and_values_it_cannot_write_as_one_entry() {
        for (key, value) in [
            ("", "true"),
            ("nested.inner", "3"),
            (" indented", "true"),
            ("key: x", "true"),
            ("switch", ""),
            ("switch", "  "),
            ("switch", "a\nb: c"),
        ] {
            assert!(
                merge_parameters_yaml(BASE, &pairs(&[(key, value)])).is_err(),
                "merged {key:?}: {value:?}"
            );
        }
    }

    /// The harness's own `parameters.yaml` (as `config_local_testnet` writes it) takes an unknown
    /// switch and a typed override, and the node's loader reads the typed value back.
    #[test]
    fn apply_parameters_reaches_the_node_loader() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("parameters.yaml");
        Config::write_to_path(&path, Parameters::default(), ConfigFmt::YAML).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();

        // nothing to apply: the file is left as written
        NodeOverrides::default()
            .env("TN_TEST_EXECUTION_DELAY_MS", "400")
            .apply_parameters(dir.path())
            .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), written);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "a no-op left files behind");

        let overrides = NodeOverrides::default()
            .parameter("bench6_unknown_switch", "true")
            .parameter("max_header_delay", "750ms")
            .parameter("allow_private_forward_targets", "true");
        overrides.apply_parameters(dir.path()).unwrap();
        let merged = std::fs::read_to_string(&path).unwrap();
        assert_eq!(merged.matches("max_header_delay:").count(), 1, "{merged}");
        assert!(merged.contains("\nbench6_unknown_switch: true\n"), "{merged}");
        let loaded: Parameters = Config::load_from_path(&path, ConfigFmt::YAML).unwrap();
        assert_eq!(loaded.max_header_delay, Duration::from_millis(750));
        assert!(loaded.allow_private_forward_targets);

        // a restart applies the same overrides again and changes nothing
        overrides.apply_parameters(dir.path()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), merged);

        // a value the node cannot read fails the merge and leaves the node's file alone
        let bad = NodeOverrides::default().parameter("max_header_delay", "soon");
        assert!(bad.apply_parameters(dir.path()).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), merged);
    }

    /// Each start gets the harness's file plus its own entries: a restart with other entries
    /// drops the earlier ones, and a restart with none puts the harness's file back.
    #[test]
    fn apply_parameters_starts_each_run_from_the_harness_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("parameters.yaml");
        Config::write_to_path(&path, Parameters::default(), ConfigFmt::YAML).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();

        NodeOverrides::default()
            .parameter("switch_a", "true")
            .apply_parameters(dir.path())
            .unwrap();
        NodeOverrides::default()
            .parameter("switch_b", "true")
            .apply_parameters(dir.path())
            .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), format!("{written}switch_b: true\n"));

        NodeOverrides::default().apply_parameters(dir.path()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), written);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "the restore left files");
    }

    #[test]
    fn fleet_overrides_assign_each_setting_to_the_named_nodes() {
        let fleet = FleetOverrides::default()
            .parameter(["validator-1", "validator-2"], "pipelined_persist", "true")
            .env(
                ["validator-2", "validator-3", "validator-4"],
                "TN_TEST_EXECUTION_DELAY_MS",
                "400",
            );
        assert_eq!(
            fleet.node("validator-1"),
            &NodeOverrides::default().parameter("pipelined_persist", "true")
        );
        assert_eq!(
            fleet.node("validator-2"),
            &NodeOverrides::default()
                .parameter("pipelined_persist", "true")
                .env("TN_TEST_EXECUTION_DELAY_MS", "400")
        );
        assert_eq!(
            fleet.node("validator-4"),
            &NodeOverrides::default().env("TN_TEST_EXECUTION_DELAY_MS", "400")
        );
        assert!(fleet.node("new-validator").is_empty());
        assert!(FleetOverrides::default().node("validator-1").is_empty());

        let replaced = fleet.set_node("validator-2", NodeOverrides::default());
        assert!(replaced.node("validator-2").is_empty());
    }

    #[test]
    fn parse_e2e_parameters_reads_entries_in_order() {
        assert_eq!(
            parse_e2e_parameters("pipelined_persist: true; recovery_threads: 4").unwrap(),
            pairs(&[("pipelined_persist", "true"), ("recovery_threads", "4")])
        );
        // whitespace is trimmed, empty entries are skipped, a value keeps its own colons and
        // spaces, and a repeated key stays repeated (the merge lets the later one win)
        assert_eq!(
            parse_e2e_parameters(" a:1 ;; nested : {x: 1, y: [2]} ;a: two words; ").unwrap(),
            pairs(&[("a", "1"), ("nested", "{x: 1, y: [2]}"), ("a", "two words")])
        );
        for blank in ["", "  ", ";", " ; ;"] {
            assert!(parse_e2e_parameters(blank).unwrap().is_empty(), "{blank:?}");
        }
    }

    #[test]
    fn parse_e2e_parameters_rejects_malformed_entries_naming_the_variable() {
        for raw in [
            "pipelined_persist",
            "pipelined_persist true",
            "pipelined_persist=true",
            ": true",
            "two words: true",
            "nested.inner: 3",
            "pipelined_persist:",
            "pipelined_persist:   ; recovery_threads: 4",
            "a: 1\nb: 2",
            "ok: 1; broken",
        ] {
            let err = parse_e2e_parameters(raw).expect_err(raw);
            assert!(err.to_string().contains(E2E_PARAMETERS_VAR), "{raw:?}: {err}");
        }
    }

    /// The fleet-wide entries go in first, so the test's own entry for a key wins and every
    /// other fleet-wide entry is still merged; the node's environment is kept.
    #[test]
    fn layered_over_puts_the_test_entries_last() {
        let fleet_wide = pairs(&[("pipelined_persist", "true"), ("recovery_threads", "4")]);
        let node = NodeOverrides::default()
            .parameter("pipelined_persist", "false")
            .env("TN_TEST_EXECUTION_DELAY_MS", "400");
        let layered = node.layered_over(&fleet_wide);
        assert_eq!(
            layered.parameters,
            pairs(&[
                ("pipelined_persist", "true"),
                ("recovery_threads", "4"),
                ("pipelined_persist", "false")
            ])
        );
        assert_eq!(layered.env, node.env);
        let merged = merge_parameters_yaml(BASE, &layered.parameters).unwrap();
        assert!(merged.ends_with("recovery_threads: 4\npipelined_persist: false\n"), "{merged}");
        assert_eq!(merged.matches("pipelined_persist:").count(), 1, "{merged}");

        // nothing fleet-wide leaves the node's overrides as they are
        assert_eq!(node.layered_over(&[]), node);
    }

    /// Applied to a datadir, the fleet-wide entries reach the node's loader beneath the test's
    /// own; a fleet-wide value the loader cannot read fails with the variable's name and leaves
    /// the file alone; with no fleet-wide entries the apply is the plain one.
    #[test]
    fn apply_layered_merges_fleet_wide_entries_under_the_test_entries() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("parameters.yaml");
        Config::write_to_path(&path, Parameters::default(), ConfigFmt::YAML).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();

        let fleet_wide = pairs(&[("max_header_delay", "750ms"), ("bench6_switch", "true")]);
        let node = NodeOverrides::default().parameter("max_header_delay", "900ms");
        node.apply_layered(dir.path(), &fleet_wide).unwrap();
        let merged = std::fs::read_to_string(&path).unwrap();
        assert!(merged.contains("\nbench6_switch: true\n"), "{merged}");
        let loaded: Parameters = Config::load_from_path(&path, ConfigFmt::YAML).unwrap();
        assert_eq!(loaded.max_header_delay, Duration::from_millis(900));

        // a node with no entries of its own still gets the fleet-wide ones
        NodeOverrides::default().apply_layered(dir.path(), &fleet_wide).unwrap();
        let loaded: Parameters = Config::load_from_path(&path, ConfigFmt::YAML).unwrap();
        assert_eq!(loaded.max_header_delay, Duration::from_millis(750));
        let applied = std::fs::read_to_string(&path).unwrap();

        let bad = pairs(&[("max_header_delay", "soon")]);
        let err = NodeOverrides::default().apply_layered(dir.path(), &bad).unwrap_err();
        assert!(format!("{err:#}").contains(E2E_PARAMETERS_VAR), "{err:#}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), applied);

        // no fleet-wide entries and none of its own: the harness's file comes back
        NodeOverrides::default().apply_layered(dir.path(), &[]).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), written);
    }
}
