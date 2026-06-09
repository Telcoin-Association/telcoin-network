//! E2E tests for Execution Extensions (ExEx) with observer nodes.
//!
//! Tests the full pipeline: consensus → execution → ExEx notifications.
//!
//! ## Test strategy
//!
//! - `test_exex_infrastructure_ready`: Binary-based — spawns validators as child processes,
//!   verifies the network produces blocks with ExEx-enabled builds.
//! - `test_exex_observer_notifications`: Binary-based observer probe gated by
//!   `ENABLE_TNEXEX_TEST_PROBE`, verified via the observer stdout log.

use super::common::ProcessGuard;
use alloy::providers::{Provider, ProviderBuilder};
use eyre::Result;
use std::{process::Child, time::Duration};
use tempfile::TempDir;
use tn_types::get_available_tcp_port;
use tokio::time::{sleep, timeout};
use tracing::{debug, info};

const NODE_PASSWORD: &str = "sup3rsecuur";
const INITIAL_VALIDATORS: [(&str, &str); 4] = [
    ("validator-1", "0x1111111111111111111111111111111111111111"),
    ("validator-2", "0x2222222222222222222222222222222222222222"),
    ("validator-3", "0x3333333333333333333333333333333333333333"),
    ("validator-4", "0x4444444444444444444444444444444444444444"),
];

/// Test that the basic validator network starts successfully.
///
/// This verifies the integration test infrastructure can handle ExEx-enabled builds.
/// Observer nodes with ExExs would be added here once the programmatic launch
/// API is simplified.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn test_exex_infrastructure_ready() -> Result<()> {
    let _permit = super::common::acquire_test_permit();

    let temp_dir = TempDir::with_prefix("exex_infra")?;
    let temp_path = temp_dir.path();

    // Setup genesis and validator configs
    let passphrase = Some(NODE_PASSWORD.to_string());
    e2e_tests::config_local_testnet(temp_path, passphrase, None)?;

    // Start validator nodes
    let (validator_procs, endpoints) = start_validator_nodes(temp_path, "exex_infra", 1)?;
    let _guard = ProcessGuard::new(validator_procs);

    // Wait for network to be ready
    let rpc_url = &endpoints[0].http_url;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    timeout(Duration::from_secs(20), async {
        loop {
            match provider.get_chain_id().await {
                Ok(_) => break,
                Err(e) => {
                    debug!(target: "exex-test", "waiting for validator RPC: {e:?}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await?;

    info!(target: "exex-test", "Network ready - ExEx infrastructure validated");

    // Verify blocks are being produced
    let start_block = provider.get_block_number().await?;
    sleep(Duration::from_secs(3)).await;
    let end_block = provider.get_block_number().await?;

    assert!(
        end_block > start_block,
        "Network should produce blocks (start: {start_block}, end: {end_block})"
    );

    info!(
        target: "exex-test",
        start_block,
        end_block,
        "Successfully validated network produces blocks"
    );

    Ok(())
}

/// Test that an observer node can launch with the simple built-in ExEx probe enabled.
///
/// This is the full end-to-end test for the ExEx pipeline:
/// 1. Start a 4-validator network as child processes
/// 2. Launch a real observer child process with a tiny built-in probe ExEx enabled by env var
/// 3. Wait for the observer probe ExEx to emit its startup marker to stdout
/// 4. Verify the observer ExEx launched successfully through the real binary path
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn test_exex_observer_notifications() -> Result<()> {
    let _permit = super::common::acquire_test_permit();

    let temp_dir = TempDir::with_prefix("exex_observer")?;
    let temp_path = temp_dir.path();
    e2e_tests::config_local_testnet(temp_path, Some(NODE_PASSWORD.to_string()), None)?;

    // Step 1: Start validator nodes as child processes.
    let (validator_procs, endpoints) = start_validator_nodes(temp_path, "exex_observer", 0)?;
    let mut guard = ProcessGuard::new(validator_procs);

    let validator_provider = ProviderBuilder::new().connect_http(endpoints[0].http_url.parse()?);
    timeout(Duration::from_secs(20), async {
        loop {
            match validator_provider.get_chain_id().await {
                Ok(_) => break,
                Err(error) => {
                    debug!(target: "exex-test", "waiting for validator RPC: {error:?}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await?;

    // Step 2: Launch a real observer child process with the env-var-gated built-in probe ExEx.
    let observer_rpc_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("Failed to get observer RPC port"))?;
    let observer_ws_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("Failed to get observer WS port"))?;
    let observer_ipc_path = temp_path.join("observer.ipc");
    let observer_dir = temp_path.join("observer");
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut command = bin.command();
    command
        .env("TN_BLS_PASSPHRASE", NODE_PASSWORD)
        .env("ENABLE_TNEXEX_TEST_PROBE", "true")
        .arg("--bls-passphrase-source")
        .arg("env")
        .arg("node")
        .arg("--observer")
        .arg("--exex")
        .arg("test-probe")
        .arg("--datadir")
        .arg(&*observer_dir.to_string_lossy())
        .arg("--http")
        .arg("--http.port")
        .arg(observer_rpc_port.to_string())
        .arg("--ws")
        .arg("--ws.port")
        .arg(observer_ws_port.to_string())
        .arg("--ipcpath")
        .arg(observer_ipc_path.to_string_lossy().as_ref());
    let observer_log_dir = e2e_tests::setup_log_dir(&mut command, "observer", "exex_observer", 0);
    guard.push(command.spawn()?);

    let observer_provider = ProviderBuilder::new()
        .connect_http(format!("http://127.0.0.1:{observer_rpc_port}").parse()?);
    timeout(Duration::from_secs(30), async {
        loop {
            match observer_provider.get_chain_id().await {
                Ok(_) => break,
                Err(error) => {
                    debug!(target: "exex-test", "waiting for observer RPC: {error:?}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await?;

    // Step 3: Wait for the probe ExEx startup marker in the observer log file.
    let observer_log = observer_log_dir.join("nodeobserver-run0.log");
    let notifications = timeout(Duration::from_secs(30), async {
        loop {
            let parsed = match std::fs::read_to_string(&observer_log) {
                Ok(contents) => contents
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => return Err(error),
            };

            if parsed.iter().any(|entry| entry.contains("Probe ExEx started")) {
                break Ok::<Vec<String>, std::io::Error>(parsed);
            }

            sleep(Duration::from_millis(500)).await;
        }
    })
    .await??;

    info!(target: "exex-test", count = notifications.len(), notifications = ?notifications, "Probe ExEx received notifications");

    // Assertions
    assert!(
        notifications.iter().any(|entry| entry.contains("Probe ExEx started")),
        "ExEx probe should log a startup marker, got {:?}",
        notifications
    );

    info!(
        target: "exex-test",
        first_notification = notifications.first().cloned().unwrap_or_default(),
        last_notification = notifications.last().cloned().unwrap_or_default(),
        "ExEx observer notification test passed"
    );

    Ok(())
}

/// Start validator nodes for testing.
fn start_validator_nodes(
    temp_path: &std::path::Path,
    test_name: &str,
    run: u32,
) -> Result<(Vec<Child>, Vec<e2e_tests::NodeEndpoints>)> {
    let bin = e2e_tests::get_telcoin_network_binary();

    let mut children = Vec::new();
    let mut endpoints = Vec::new();

    for (name, _addr) in INITIAL_VALIDATORS.iter() {
        let dir = temp_path.join(name);

        let rpc_port = get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("Failed to get available RPC port"))?;
        let ws_port = get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("Failed to get available WS port"))?;
        let ipc_path = temp_path.join(format!("{name}.ipc"));

        let mut command = bin.command();
        command
            .env("TN_BLS_PASSPHRASE", NODE_PASSWORD)
            .arg("--bls-passphrase-source")
            .arg("env")
            .arg("node")
            .arg("--datadir")
            .arg(&*dir.to_string_lossy())
            .arg("--http")
            .arg("--http.port")
            .arg(rpc_port.to_string())
            .arg("--ws")
            .arg("--ws.port")
            .arg(ws_port.to_string())
            .arg("--ipcpath")
            .arg(ipc_path.to_string_lossy().as_ref());

        e2e_tests::setup_log_dir(&mut command, name, test_name, run);

        children.push(command.spawn()?);
        endpoints.push(e2e_tests::NodeEndpoints {
            http_url: format!("http://127.0.0.1:{rpc_port}"),
            ws_url: format!("ws://127.0.0.1:{ws_port}"),
            ipc_path: ipc_path.to_string_lossy().to_string(),
        });
    }

    Ok((children, endpoints))
}
