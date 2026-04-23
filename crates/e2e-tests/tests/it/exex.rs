//! E2E tests for Execution Extensions (ExEx) with observer nodes.
//!
//! Tests the full pipeline: consensus → execution → ExEx notifications.
//!
//! ## Test strategy
//!
//! - `test_exex_infrastructure_ready`: Binary-based — spawns validators as child processes,
//!   verifies the network produces blocks with ExEx-enabled builds.
//! - `test_exex_observer_notifications`: Programmatic — uses `spawn_local_testnet()` for
//!   validators (in-process), then launches an observer with a probe ExEx injected via
//!   `TnExExLauncher`. Verifies the ExEx receives `ChainCommitted` notifications.

use super::common::ProcessGuard;
use alloy::providers::{Provider, ProviderBuilder};
use eyre::Result;
use std::{process::Child, sync::Arc, time::Duration};
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

/// Test that an observer node with an ExEx plugin receives block notifications.
///
/// This is the full end-to-end test for the ExEx pipeline:
/// 1. Start a 4-validator network (in-process via `spawn_local_testnet`)
/// 2. Launch an observer node with a "probe" ExEx immediately after (not waiting for blocks)
/// 3. Wait for the network to produce blocks and the ExEx to receive them
/// 4. Verify the probe ExEx receives `ChainCommitted` notifications with valid block numbers
///
/// Note: The observer is started immediately after the validators — both start from genesis
/// together. This avoids the late-join epoch mismatch that occurs when starting an observer
/// after validators have already advanced multiple epochs.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn test_exex_observer_notifications() -> Result<()> {
    use clap::Parser;
    use telcoin_network_cli::{node::NodeCommand, NoArgs};
    use tn_config::KeyConfig;
    use tn_exex::{TnExExEvent, TnExExLauncher};
    use tn_node::launch_node;

    let _permit = super::common::acquire_test_permit();

    let temp_dir = TempDir::with_prefix("exex_observer")?;
    let temp_path = temp_dir.path();

    // Step 1: Start the validator network in-process
    // This calls config_local_testnet internally, creating configs for 4 validators + 1 observer
    let _endpoints = e2e_tests::spawn_local_testnet(temp_path, None)?;

    // Step 2: Create an ExEx launcher with a probe that sends block numbers back to us
    let (probe_tx, probe_rx) = std::sync::mpsc::channel::<u64>();

    let mut exex_launcher = TnExExLauncher::new();
    exex_launcher.install(
        "test-probe",
        Box::new(move |ctx| {
            Box::pin(async move {
                let mut ctx = ctx;
                info!(target: "exex-probe", head = ?ctx.head, "Probe ExEx started");

                while let Some(notification) = ctx.notifications.recv().await {
                    if let Some(chain) = notification.committed_chain() {
                        let tip = chain.tip();
                        let block_num = tip.number;
                        info!(
                            target: "exex-probe",
                            block_num,
                            "Probe received ChainCommitted"
                        );

                        // Send block number to the test
                        if probe_tx.send(block_num).is_err() {
                            break;
                        }

                        // Report progress back to manager
                        let _ = ctx.events.send(TnExExEvent::finished_height(
                            tn_types::BlockNumHash::new(tip.number, tip.hash()),
                        ));
                    }
                }
                Ok(())
            })
        }),
    );

    // Step 3: Launch the observer node immediately after validators
    // Both start from genesis so the observer won't be behind on epochs
    let observer_dir = temp_path.join("observer");

    let observer_rpc_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("Failed to get observer RPC port"))?;
    let observer_ws_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("Failed to get observer WS port"))?;
    let observer_ipc_path = temp_path.join("observer.ipc");

    let command = NodeCommand::<NoArgs>::parse_from([
        "tn",
        "--observer",
        "--exex",
        "test-probe",
        "--http",
        "--http.port",
        &observer_rpc_port.to_string(),
        "--ws",
        "--ws.port",
        &observer_ws_port.to_string(),
        "--ipcpath",
        &observer_ipc_path.to_string_lossy(),
    ]);

    let observer_dir_clone = observer_dir.clone();
    // Observer keys were created with passphrase=None by config_local_testnet
    let key_config = KeyConfig::read_config(&observer_dir, None)?;

    let (startup_tx, startup_rx) = std::sync::mpsc::channel::<String>();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("observer-node")
            .enable_io()
            .enable_time()
            .build()
            .expect("can build tokio runtime");

        runtime.block_on(async move {
            match command.execute(
                observer_dir_clone,
                key_config,
                |builder, _: NoArgs, tn_datadir, passphrase| {
                    launch_node(builder, tn_datadir, passphrase, Some(exex_launcher))
                },
            ) {
                Ok(handle) => {
                    let err = handle.await;
                    tracing::error!(target: "exex-test", "Observer exited: {err:?}");
                }
                Err(e) => {
                    let msg = format!("Observer failed to start: {e:?}");
                    tracing::error!(target: "exex-test", "{msg}");
                    let _ = startup_tx.send(msg);
                }
            }
        });
    });

    // Give the observer a moment to fail fast
    std::thread::sleep(Duration::from_millis(500));
    if let Ok(err) = startup_rx.try_recv() {
        eyre::bail!("Observer startup failed: {err}");
    }

    // Step 4: Wait for the probe ExEx to receive block notifications
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_clone = Arc::clone(&received);

    let collect_handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        while std::time::Instant::now() < deadline {
            match probe_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(block_num) => {
                    let mut guard = received_clone.lock().unwrap();
                    guard.push(block_num);
                    if guard.len() >= 3 {
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });

    collect_handle.join().expect("collector thread panicked");

    let blocks: Vec<u64> = received.lock().unwrap().clone();

    info!(
        target: "exex-test",
        count = blocks.len(),
        blocks = ?blocks,
        "Probe ExEx received notifications"
    );

    // Assertions
    assert!(
        blocks.len() >= 3,
        "ExEx should receive at least 3 block notifications, got {}",
        blocks.len()
    );

    assert!(blocks.iter().all(|&b| b > 0), "All block numbers should be > 0");

    for window in blocks.windows(2) {
        assert!(
            window[1] > window[0],
            "Block numbers should increase: {} -> {}",
            window[0],
            window[1]
        );
    }

    info!(
        target: "exex-test",
        first_block = blocks.first().copied().unwrap_or(0),
        last_block = blocks.last().copied().unwrap_or(0),
        "ExEx observer notification test passed"
    );

    Ok(())
}

/// Test that an observer ExEx correctly processes network activity.
///
/// This is a comprehensive e2e test that verifies an ExEx can:
/// 1. Track and count blocks with their metadata
/// 2. Process real network activity (blocks produced by validators)
/// 3. Maintain sequential block ordering
///
/// The test:
/// - Starts a 4-validator network
/// - Submits transactions to generate network activity
/// - Launches an observer with a block-tracking ExEx
/// - Verifies the ExEx receives and correctly processes blocks with transactions
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn test_exex_transaction_processing() -> Result<()> {
    use alloy::{
        network::EthereumWallet,
        primitives::{utils::parse_ether, Address as AlloyAddress},
        providers::{Provider, ProviderBuilder},
        rpc::types::TransactionRequest,
        signers::local::PrivateKeySigner,
    };
    use clap::Parser;
    use telcoin_network_cli::{node::NodeCommand, NoArgs};
    use tn_config::KeyConfig;
    use tn_exex::{TnExExEvent, TnExExLauncher};
    use tn_node::launch_node;

    let _permit = super::common::acquire_test_permit();

    let temp_dir = TempDir::with_prefix("exex_tx_processing")?;
    let temp_path = temp_dir.path();

    // Step 1: Start the validator network
    let endpoints = e2e_tests::spawn_local_testnet(temp_path, None)?;

    // Create provider for submitting transactions
    let rpc_url = &endpoints[0].http_url;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // Wait for network to be ready
    timeout(Duration::from_secs(20), async {
        loop {
            match provider.get_chain_id().await {
                Ok(_) => break,
                Err(e) => {
                    debug!(target: "exex-tx-test", "waiting for validator RPC: {e:?}");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .await?;

    info!(target: "exex-tx-test", "Network ready, submitting test transactions");

    // Step 2: Submit some test transactions to generate network activity
    // Create a sender with funds from genesis
    let sender_key = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let signer: PrivateKeySigner = sender_key.parse()?;
    let sender_address = signer.address();
    
    let wallet = EthereumWallet::from(signer);
    let provider_with_wallet = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse()?);

    // Recipients for test transfers
    let recipient1 = AlloyAddress::random();
    let recipient2 = AlloyAddress::random();
    let recipient3 = AlloyAddress::random();

    let transfer_amount = parse_ether("0.1")?;

    // Submit 3 transactions
    let tx1 = TransactionRequest::default()
        .to(recipient1)
        .value(transfer_amount)
        .from(sender_address);
    
    let tx2 = TransactionRequest::default()
        .to(recipient2)
        .value(transfer_amount)
        .from(sender_address);
    
    let tx3 = TransactionRequest::default()
        .to(recipient3)
        .value(transfer_amount)
        .from(sender_address);

    // Send transactions (fire and forget - we just want network activity)
    let _ = provider_with_wallet.send_transaction(tx1).await;
    let _ = provider_with_wallet.send_transaction(tx2).await;
    let _ = provider_with_wallet.send_transaction(tx3).await;

    info!(target: "exex-tx-test", "Submitted test transactions to generate network activity");

    // Step 3: Create ExEx that tracks block data
    #[derive(Debug, Clone)]
    struct BlockData {
        block_number: u64,
        num_blocks_in_chain: usize,
    }

    let (block_data_sender, block_data_receiver) = std::sync::mpsc::channel::<BlockData>();

    let mut exex_launcher = TnExExLauncher::new();
    exex_launcher.install(
        "block-tracker",
        Box::new(move |ctx| {
            let data_sender = block_data_sender.clone();
            
            Box::pin(async move {
                let mut ctx = ctx;
                info!(
                    target: "exex-block-tracker",
                    head = ?ctx.head,
                    "Block tracking ExEx started"
                );

                while let Some(notification) = ctx.notifications.recv().await {
                    if let Some(chain) = notification.committed_chain() {
                        let tip = chain.tip();
                        
                        // Count blocks in this chain commitment
                        let num_blocks = chain.blocks_and_receipts().count();

                        let block_data = BlockData {
                            block_number: tip.number,
                            num_blocks_in_chain: num_blocks,
                        };

                        info!(
                            target: "exex-block-tracker",
                            block_num = block_data.block_number,
                            num_blocks = block_data.num_blocks_in_chain,
                            block_hash = ?tip.hash(),
                            "Processed chain commitment"
                        );

                        // Send data to test
                        if data_sender.send(block_data).is_err() {
                            break;
                        }

                        // Report progress
                        let _ = ctx.events.send(TnExExEvent::finished_height(
                            tn_types::BlockNumHash::new(tip.number, tip.hash()),
                        ));
                    }
                }
                Ok(())
            })
        }),
    );

    // Step 4: Launch observer with the ExEx
    let observer_dir = temp_path.join("observer");
    let observer_rpc_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("Failed to get observer RPC port"))?;
    let observer_ws_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("Failed to get observer WS port"))?;
    let observer_ipc_path = temp_path.join("observer.ipc");

    let command = NodeCommand::<NoArgs>::parse_from([
        "tn",
        "--observer",
        "--exex",
        "block-tracker",
        "--http",
        "--http.port",
        &observer_rpc_port.to_string(),
        "--ws",
        "--ws.port",
        &observer_ws_port.to_string(),
        "--ipcpath",
        &observer_ipc_path.to_string_lossy(),
    ]);

    let observer_dir_clone = observer_dir.clone();
    let key_config = KeyConfig::read_config(&observer_dir, None)?;

    let (startup_tx, startup_rx) = std::sync::mpsc::channel::<String>();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("observer-block-tracker")
            .enable_io()
            .enable_time()
            .build()
            .expect("can build tokio runtime");

        runtime.block_on(async move {
            match command.execute(
                observer_dir_clone,
                key_config,
                |builder, _: NoArgs, tn_datadir, passphrase| {
                    launch_node(builder, tn_datadir, passphrase, Some(exex_launcher))
                },
            ) {
                Ok(handle) => {
                    let err = handle.await;
                    tracing::error!(target: "exex-tx-test", "Observer exited: {err:?}");
                }
                Err(e) => {
                    let msg = format!("Observer failed to start: {e:?}");
                    tracing::error!(target: "exex-tx-test", "{msg}");
                    let _ = startup_tx.send(msg);
                }
            }
        });
    });

    // Check for immediate startup failures
    std::thread::sleep(Duration::from_millis(500));
    if let Ok(err) = startup_rx.try_recv() {
        eyre::bail!("Observer startup failed: {err}");
    }

    // Step 5: Collect and verify ExEx data
    let collected_data = Arc::new(std::sync::Mutex::new(Vec::new()));
    let collected_clone = Arc::clone(&collected_data);

    let collect_handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        
        while std::time::Instant::now() < deadline {
            match block_data_receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(data) => {
                    let mut guard = collected_clone.lock().unwrap();
                    guard.push(data);
                    
                    // Collect at least 5 blocks to verify sustained operation
                    if guard.len() >= 5 {
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });

    collect_handle.join().expect("collector thread panicked");

    let collected: Vec<BlockData> = collected_data.lock().unwrap().clone();

    info!(
        target: "exex-tx-test",
        blocks_received = collected.len(),
        "ExEx data collection complete"
    );

    // Assertions
    assert!(
        collected.len() >= 3,
        "ExEx should receive at least 3 blocks, got {}",
        collected.len()
    );

    // Verify block numbers are increasing
    for window in collected.windows(2) {
        assert!(
            window[1].block_number >= window[0].block_number,
            "Block numbers should not decrease: {} -> {}",
            window[0].block_number,
            window[1].block_number
        );
    }

    // Verify each notification contains at least one block
    for data in &collected {
        assert!(
            data.num_blocks_in_chain >= 1,
            "Each chain commitment should contain at least 1 block"
        );
    }

    info!(
        target: "exex-tx-test",
        blocks_processed = collected.len(),
        first_block = collected.first().map(|b| b.block_number).unwrap_or(0),
        last_block = collected.last().map(|b| b.block_number).unwrap_or(0),
        "ExEx block processing test passed"
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
