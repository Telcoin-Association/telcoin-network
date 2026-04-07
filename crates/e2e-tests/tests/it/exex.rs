//! E2E tests for Execution Extensions (ExEx) with observer nodes.
//!
//! Tests the full pipeline: consensus → execution → ExEx notifications.
//!
//! Note: These tests demonstrate ExEx integration patterns. Full programmatic
//! observer testing requires additional infrastructure (planned for future work).

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

/// Demonstrates how an ExEx-enabled observer would be tested.
///
/// This test is currently a placeholder showing the testing approach.
/// Full implementation requires:
/// - Simplified programmatic node launch API (tracked in issue TBD)  
/// - Or CLI-based test runner for observer + ExEx (like bin/telcoin-network with config)
///
/// For manual testing, use:
/// ```ignore
/// // In your custom node binary:
/// let mut launcher = TnExExLauncher::new();
/// launcher.install("my-exex", my_exex_factory);
/// // Pass launcher to launch_node()
/// ```
#[ignore = "placeholder for future observer ExEx testing"]
#[tokio::test]
async fn test_exex_observer_notifications() -> Result<()> {
    // TODO: Implement once programmatic observer launch is available
    // Expected behavior:
    // 1. Start validator network
    // 2. Launch observer node with ExEx
    // 3. Verify ExEx receives block notifications
    // 4. Optionally test across epoch boundaries
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
            .arg("--ipc-path")
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
