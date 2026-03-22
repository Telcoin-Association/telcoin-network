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

/// Only compile main bin once for all tests.
pub static TELCOIN_BINARY: OnceLock<CargoRun> = OnceLock::new();

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
    keys_command.args.execute(datadir, passphrase)?;

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
    keys_command.args.execute(datadir, passphrase)
}

/// Create validator info, genesis ceremony, and configure local testnet.
pub fn config_local_testnet(
    temp_path: &Path,
    passphrase: Option<String>,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
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
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        "0x00000000000000000000000000000000000007a0",
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
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
        let instance: u16 =
            v.chars().last().expect("validator instance").to_digit(10).expect("instance digit")
                as u16;
        let rpc_port = tn_types::get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port");
        // reth's adjust_instance_ports does: http_port -= instance - 1
        // So pass rpc_port + (instance - 1) to compensate, making node listen on rpc_port
        let http_port_arg = rpc_port + instance - 1;

        // WS - dynamic port, compensate for adjust_instance_ports: ws_port += instance * 2 - 2
        let ws_port = tn_types::get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral ws port");
        let ws_port_arg = ws_port - (instance * 2 - 2);

        // IPC - unique path under temp_path to avoid cross-test conflicts
        // adjust_instance_ports appends "-{instance}" to ipcpath
        let ipc_base = temp_path.join(format!("{v}.ipc"));
        let ipc_base_str = ipc_base.to_string_lossy().to_string();
        let actual_ipc_path = format!("{ipc_base_str}-{instance}");

        endpoints.push(NodeEndpoints {
            http_url: format!("http://127.0.0.1:{rpc_port}"),
            ws_url: format!("ws://127.0.0.1:{ws_port}"),
            ipc_path: actual_ipc_path,
        });

        let command = NodeCommand::parse_from([
            "tn",
            "--http",
            "--http.port",
            &http_port_arg.to_string(),
            "--ws",
            "--ws.port",
            &ws_port_arg.to_string(),
            "--ipcpath",
            &ipc_base_str,
            "--instance",
            &instance.to_string(),
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
                    |builder, _: NoArgs, tn_datadir, passphrase| {
                        launch_node(builder, tn_datadir, passphrase)
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

/// Helper to retrieve and build the main project binary.
pub fn get_telcoin_network_binary() -> &'static CargoRun {
    info!("building main binary for e2e tests");
    TELCOIN_BINARY.get_or_init(|| {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let path = PathBuf::from(manifest_dir);
        let workspace_root =
            path.parent().and_then(|p| p.parent()).expect("Cannot find workspace root");

        CargoBuild::new()
            .bin("telcoin-network")
            .features("tn-storage/test-utils")
            .manifest_path(workspace_root.join("Cargo.toml"))
            .target_dir(workspace_root.join("target"))
            .current_target()
            .run()
            .expect("Failed to build telcoin-network binary")
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
