//! End-to-end tests for Telcoin Network
//!
//! Utilities for it tests.

// ignore for IT test
#![allow(unused_crate_dependencies)]

use clap::Parser;
use escargot::{CargoBuild, CargoRun};
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};
use telcoin_network_cli::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand, NoArgs};
use tn_config::{Config, ConfigFmt, ConfigTrait, KeyConfig};
use tn_node::launch_node;
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
    pub fn command(&self) -> std::process::Command {
        match self {
            TestBinary::Prebuilt(path) => std::process::Command::new(path),
            TestBinary::Cargo(run) => run.command(),
        }
    }
}

/// Resolve the main bin once for all tests.
pub static TELCOIN_BINARY: OnceLock<TestBinary> = OnceLock::new();

/// PBKDF2 round count for test-generated BLS keyfiles.
///
/// Tests intentionally wrap keys with a trivially weak KDF so suites don't spend CPU on
/// 1,000,000-round PBKDF2 per node. The round count is not stored on disk; the spawned node
/// binary - built without any test features - recovers these keys by trying the weak round
/// count when reading (and warns that they are weakly wrapped).
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
    let datadir = dir.to_path_buf();

    // keytool
    let keys_command =
        CommandParser::<KeyArgs>::parse_from(["tn", "generate", "validator", "--address", address]);
    keys_command.args.execute_insecure(datadir, passphrase, INSECURE_TEST_KDF_ROUNDS)?;

    Ok(())
}

/// Execute observer config inside tempdir
fn create_observer_info(datadir: PathBuf, passphrase: Option<String>) -> eyre::Result<()> {
    // keytool
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "tn",
        "generate",
        "observer",
        "--address",
        "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    ]);
    keys_command.args.execute_insecure(datadir, passphrase, INSECURE_TEST_KDF_ROUNDS)
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
    config_local_testnet_inner(temp_path, passphrase, accounts, epoch_duration_secs, &[])
}

/// Like [`config_local_testnet_with_epoch_duration`], but also lets the caller set per-worker
/// fee strategies at genesis via `--worker-fee-config`.
///
/// Each entry is a `"WORKER_ID:STRATEGY:VALUE"` string, where `STRATEGY` is `0` for EIP-1559
/// (`VALUE` = target gas) or `1` for a static fee (`VALUE` = fee in wei). Entries must cover
/// contiguous worker ids starting at 0 (the genesis ceremony validates this).
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
    // create validator info and copy to shared genesis dir
    for (v, addr) in validators.iter() {
        let dir = temp_path.join(v);
        // init genesis ceremony to create committee files
        create_validator_info(&dir, addr, passphrase.clone())?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // Create an observer config.
    let dir = temp_path.join("observer");
    // init config ceremony for observer
    create_observer_info(dir, passphrase.clone())?;

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
        "1000".into(),
        "--min-header-delay-ms".into(),
        "500".into(),
    ];
    if let Some(duration) = epoch_duration_secs {
        genesis_args.push("--epoch-duration-in-secs".into());
        genesis_args.push(duration.to_string());
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

    let dir = temp_path.join("observer");
    // copy genesis files back to observer dirs
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

        let key_config = KeyConfig::read_config(&dir, Some("it_test_pass".to_string()))?;
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

        info!("building main binary for e2e tests");
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let path = PathBuf::from(manifest_dir);
        let workspace_root =
            path.parent().and_then(|p| p.parent()).expect("Cannot find workspace root");

        TestBinary::Cargo(
            CargoBuild::new()
                .bin("telcoin-network")
                .features("tn-storage/test-utils")
                .manifest_path(workspace_root.join("Cargo.toml"))
                .target_dir(workspace_root.join("target"))
                .current_target()
                .run()
                .expect("Failed to build telcoin-network binary"),
        )
    })
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
