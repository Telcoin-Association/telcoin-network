//! E2E test for base fee adjustment and sync recovery.
//!
//! Verifies:
//! 1. Base fees start at MIN_PROTOCOL_BASE_FEE (7 wei)
//! 2. Sending transactions that exceed the gas target causes base fees to increase over epochs
//! 3. A node that was offline can sync and recover the correct base fee

use super::common::ProcessGuard;
use alloy::{
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use clap::Parser as _;
use e2e_tests::{create_validator_info, setup_log_dir, NodeEndpoints};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{path::Path, process::Child, sync::Arc, time::Duration};
use telcoin_network_cli::genesis::GenesisArgs;
use tn_config::{Config, ConfigFmt, ConfigTrait as _, WORKER_CONFIGS_ADDRESS};
use tn_reth::{
    system_calls::{ConsensusRegistry, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_types::{
    get_available_tcp_port, test_utils::CommandParser, Address, Genesis, GenesisAccount,
    MIN_PROTOCOL_BASE_FEE, U256,
};
use tokio::time::{timeout, Instant};
use tracing::{debug, info};

const NODE_PASSWORD: &str = "sup3rsecuur";
const INITIAL_STAKE_AMOUNT: &str = "1_000_000";
const EPOCH_DURATION: u64 = 10;

/// Get the base fee from the latest block via RPC.
async fn get_base_fee(rpc_url: &str) -> eyre::Result<u64> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider
        .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
        .await?
        .ok_or_else(|| eyre::eyre!("no latest block"))?;
    Ok(block.header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE))
}

/// Spawn a background task that sends one value transfer to each node endpoint every second.
/// Fire-and-forget: errors from down nodes are silently ignored.
/// Returns a JoinHandle — call `.abort()` to stop.
fn spawn_tx_sender(
    rpc_urls: Vec<String>,
    mut tx_factory: TransactionFactory,
    chain: Arc<RethChainSpec>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let recipient = Address::from_slice(&[0xAA; 20]);
        let providers: Vec<_> = rpc_urls
            .iter()
            .filter_map(|url| url.parse().ok().map(|u| ProviderBuilder::new().connect_http(u)))
            .collect();

        loop {
            for provider in &providers {
                let tx = tx_factory.create_eip1559_encoded(
                    chain.clone(),
                    Some(21_000),
                    100_000,
                    Some(recipient),
                    U256::from(1),
                    Default::default(),
                );
                let _ = provider.send_raw_transaction(&tx).await.inspect_err(
                    |e| tracing::error!(target: "basefee-test", ?e, "failed to send raw transaction"),
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

/// Poll `getCurrentEpochInfo()` until `iterations` epoch transitions are observed.
/// Returns the epoch id after the last transition.
async fn loop_epochs(start: u32, iterations: u32, rpc_url: &str) -> eyre::Result<u32> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let mut current_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

    let mut last_epoch_block_height = current_epoch_info.blockHeight;
    for i in start..start + iterations {
        let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 4);
        let new_epoch_info = loop {
            let info = consensus_registry.getCurrentEpochInfo().call().await?;
            if info != current_epoch_info {
                break info;
            }
            assert!(
                Instant::now() < deadline,
                "Epoch did not change within {}s on iteration {i}",
                EPOCH_DURATION * 4
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        };

        assert!(new_epoch_info.blockHeight > last_epoch_block_height);
        assert_eq!(new_epoch_info.epochDuration as u64, EPOCH_DURATION);

        last_epoch_block_height = new_epoch_info.blockHeight;
        current_epoch_info = new_epoch_info;
    }
    Ok(current_epoch_info.epochId)
}

/// Start node processes for the given validators.
fn start_nodes(
    temp_path: &Path,
    validators: &[(&str, Address)],
    test: &str,
    run: u32,
) -> eyre::Result<(Vec<Child>, Vec<NodeEndpoints>)> {
    let bin = e2e_tests::get_telcoin_network_binary();

    let mut children = Vec::new();
    let mut endpoints = Vec::new();
    for (v, _) in validators.iter() {
        let dir = temp_path.join(v);

        let rpc_port = get_available_tcp_port("127.0.0.1").expect("available tcp port");
        let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
        let ipc_path = temp_path.join(format!("{v}.ipc"));

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

        setup_log_dir(&mut command, v, test, run);

        children.push(command.spawn().expect("failed to execute"));
        endpoints.push(NodeEndpoints {
            http_url: format!("http://127.0.0.1:{rpc_port}"),
            ws_url: format!("ws://127.0.0.1:{ws_port}"),
            ipc_path: ipc_path.to_string_lossy().to_string(),
        });
    }

    Ok((children, endpoints))
}

/// Configure the initial committee with `--worker-fee-config 0:0:21000` and fund accounts.
fn config_committee_with_basefee(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    passphrase: Option<String>,
    consensus_registry_owner: Address,
    accounts: Vec<(Address, GenesisAccount)>,
    validators: &[(&str, Address)],
) -> eyre::Result<Genesis> {
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;

    for (v, addr) in validators.iter() {
        let dir = temp_path.join(v);
        create_validator_info(&dir, &addr.to_string(), passphrase.clone())?;
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    let min_withdrawal = "1_000";
    let epoch_rewards = "1_000";

    info!(target: "basefee-test", "creating committee with --worker-fee-config 0:0:21000");

    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        &consensus_registry_owner.to_string(),
        "--initial-stake-per-validator",
        INITIAL_STAKE_AMOUNT,
        "--min-withdraw-amount",
        min_withdrawal,
        "--epoch-block-rewards",
        epoch_rewards,
        "--epoch-duration-in-secs",
        &EPOCH_DURATION.to_string(),
        "--worker-fee-config",
        "0:0:21000",
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
    create_committee_command.args.execute(shared_genesis_dir.to_path_buf())?;

    let data_dir = shared_genesis_dir.join("genesis/genesis.yaml");
    let genesis: Genesis = Config::load_from_path(&data_dir, ConfigFmt::YAML)?;
    let genesis = genesis.extend_accounts(accounts);
    Config::write_to_path(&data_dir, &genesis, ConfigFmt::YAML)?;

    for (v, _addr) in validators.iter() {
        let dir = temp_path.join(v);
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
    }

    Ok(genesis)
}

/// Create genesis for the basefee test.
///
/// Funds a tx_sender account for sending value transfers and sets `--worker-fee-config 0:0:21000`.
fn create_basefee_genesis(
    temp_path: &Path,
    tx_sender: Address,
    governance_wallet: Address,
    committee: &[(&str, Address)],
) -> eyre::Result<Genesis> {
    let passphrase = Some(NODE_PASSWORD.to_string());

    let accounts = vec![
        (
            governance_wallet,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        ),
        (tx_sender, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000_000")?))),
    ];

    let shared_genesis_dir = temp_path.join("shared-genesis");

    config_committee_with_basefee(
        temp_path,
        &shared_genesis_dir,
        passphrase,
        governance_wallet,
        accounts,
        committee,
    )
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test that base fees adjust via EIP-1559 and a restarted node recovers the correct base fee.
async fn test_basefee_adjustment_and_sync() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();

    // ── Phase 1: Setup genesis & start 5 validators ──

    let tx_sender = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(99));
    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    let temp_dir = tempfile::TempDir::with_prefix("basefee_test")?;
    let temp_path = temp_dir.path();

    let genesis = create_basefee_genesis(
        temp_path,
        tx_sender.address(),
        governance_wallet.address(),
        &committee,
    )?;

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let (procs, mut endpoints) = start_nodes(temp_path, &committee, "basefee_test", 1)?;
    let mut guard = ProcessGuard::new(procs);

    // ── Phase 2: Wait for RPC, verify initial base fee = 7 ──

    let rpc_url = endpoints[0].http_url.clone();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    timeout(Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "basefee-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    let initial_base_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", initial_base_fee, "initial base fee");
    assert_eq!(
        initial_base_fee, MIN_PROTOCOL_BASE_FEE,
        "genesis should start at MIN_PROTOCOL_BASE_FEE (7)"
    );

    // ── Phase 3: Start background tx sender, run 3 epochs → verify fee > 7 ──

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;

    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_sender(all_rpc_urls, tx_sender, chain.clone());

    loop_epochs(0, 3, &rpc_url).await?;

    let pre_kill_base_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", pre_kill_base_fee, "base fee after 3 epochs of activity");
    assert!(
        pre_kill_base_fee > MIN_PROTOCOL_BASE_FEE,
        "base fee should have increased from {MIN_PROTOCOL_BASE_FEE}, got {pre_kill_base_fee}"
    );

    // ── Phase 4: Kill validator-3 (index 2) ──

    let kill_idx = 2;
    if let Some(mut taken) = guard.take(kill_idx) {
        super::common::kill_child(&mut taken);
    }

    let killed_url = endpoints[kill_idx].http_url.clone();
    let killed_provider = ProviderBuilder::new().connect_http(killed_url.parse()?);
    assert!(killed_provider.get_chain_id().await.is_err(), "Killed node should be down");

    // ── Phase 5: Run 3 more epochs → verify fee increased further ──
    // Background sender keeps going; errors to the dead node are silently ignored.

    loop_epochs(3, 3, &rpc_url).await?;

    let network_base_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", network_base_fee, pre_kill_base_fee, "base fee after 6 epochs");
    assert!(
        network_base_fee > pre_kill_base_fee,
        "base fee should keep increasing: {network_base_fee} > {pre_kill_base_fee}"
    );

    // ── Phase 6: Restart validator-3 with new dynamic ports ──

    let nodes_to_start: &[(&str, Address)] = &[("validator-3", Address::from_slice(&[0x33; 20]))];
    let (mut new_children, mut new_endpoints) =
        start_nodes(temp_path, nodes_to_start, "basefee_test", 2)?;
    let new_child = new_children.pop().expect("child");
    guard.replace(kill_idx, new_child);
    endpoints[kill_idx] = new_endpoints.pop().expect("endpoint");

    // ── Phase 7: Run 3 more epochs for sync to complete ──

    loop_epochs(6, 3, &rpc_url).await?;

    // ── Phase 8: Verify restarted node's base fee matches the network ──

    let restarted_url = endpoints[kill_idx].http_url.clone();

    // Wait for restarted node RPC to become available
    let restarted_provider = ProviderBuilder::new().connect_http(restarted_url.parse()?);
    timeout(Duration::from_secs(EPOCH_DURATION * 4), async {
        let mut result = restarted_provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "basefee-test", "restarted node not ready: {e:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = restarted_provider.get_chain_id().await;
        }
    })
    .await?;

    let synced_node_fee = get_base_fee(&restarted_url).await?;
    let final_network_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", synced_node_fee, final_network_fee, "comparing synced vs network");

    assert_eq!(
        synced_node_fee, final_network_fee,
        "restarted node base fee ({synced_node_fee}) should match network ({final_network_fee})"
    );

    // ── Phase 9: Verify all nodes have base fee 10 then stop sender ──

    for (i, ep) in endpoints.iter().enumerate() {
        let fee = get_base_fee(&ep.http_url).await?;
        info!(target: "basefee-test", i, fee, "node base fee");
        assert_eq!(fee, 10, "node {i} base fee should be > {MIN_PROTOCOL_BASE_FEE}, got {fee}");
    }

    sender.abort();
    info!(target: "basefee-test", "all assertions passed");
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test that governance can update the WorkerConfigs contract and all nodes (including a synced
/// node) pick up the new fee strategy.
async fn test_worker_config_governance_update() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();

    // ── Phase 1: Setup genesis & start 5 validators ──

    let tx_sender = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(99));
    let mut governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    let temp_dir = tempfile::TempDir::with_prefix("worker_config_test")?;
    let temp_path = temp_dir.path();

    let genesis = create_basefee_genesis(
        temp_path,
        tx_sender.address(),
        governance_wallet.address(),
        &committee,
    )?;

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let (procs, mut endpoints) = start_nodes(temp_path, &committee, "worker_config_test", 1)?;
    let mut guard = ProcessGuard::new(procs);

    let rpc_url = endpoints[0].http_url.clone();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    timeout(Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "basefee-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // ── Phase 2: Drive base fee above MIN via background sender ──

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;

    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_sender(all_rpc_urls, tx_sender, chain.clone());

    loop_epochs(0, 3, &rpc_url).await?;

    let base_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", base_fee, "base fee after 3 epochs");
    assert!(
        base_fee > MIN_PROTOCOL_BASE_FEE,
        "base fee should have increased from {MIN_PROTOCOL_BASE_FEE}, got {base_fee}"
    );

    // ── Phase 3: Governance updates WorkerConfigs to Static(42) ──

    let calldata: Bytes =
        WorkerConfigs::setWorkerConfigCall { workerId: 0, strategy: 1, value: 42 }
            .abi_encode()
            .into();

    let tx = governance_wallet.create_eip1559_encoded(
        chain.clone(),
        None,
        100_000,
        Some(WORKER_CONFIGS_ADDRESS),
        U256::ZERO,
        calldata,
    );
    let pending = provider.send_raw_transaction(&tx).await?;
    info!(target: "basefee-test", "sent setWorkerConfig(0, 1, 42), waiting for confirmation");
    timeout(Duration::from_secs(EPOCH_DURATION * 2 + 11), pending.watch()).await??;

    // ── Phase 4: Verify fee = 42 after epoch transition ──

    loop_epochs(3, 1, &rpc_url).await?;

    // The epoch-closing block carries the parent's base fee; the Static(42)
    // fee only appears once the new epoch's batch builder creates its first batch.
    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 2);
    loop {
        let fee = get_base_fee(&rpc_url).await?;
        if fee == 42 {
            info!(target: "basefee-test", "node 0 base fee reached 42");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 0 base fee did not reach 42 within {}s, still {fee}",
            EPOCH_DURATION * 2
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Static fee ignores gas usage — stop the background sender
    sender.abort();

    for (i, ep) in endpoints.iter().enumerate() {
        let fee = get_base_fee(&ep.http_url).await?;
        info!(target: "basefee-test", i, fee, "node base fee after governance update");
        assert_eq!(fee, 42, "node {i} base fee should be 42 (Static), got {fee}");
    }

    // ── Phase 5: Kill one node, run more epochs, verify fee still 42 ──

    let kill_idx = 2;
    if let Some(mut taken) = guard.take(kill_idx) {
        super::common::kill_child(&mut taken);
    }

    loop_epochs(4, 2, &rpc_url).await?;

    for (i, ep) in endpoints.iter().enumerate() {
        if i == kill_idx {
            continue;
        }
        let fee = get_base_fee(&ep.http_url).await?;
        info!(target: "basefee-test", i, fee, "node base fee after kill (should still be 42)");
        assert_eq!(fee, 42, "node {i} base fee should still be 42, got {fee}");
    }

    // ── Phase 6: Restart and verify synced node picks up Static(42) ──

    let nodes_to_start: &[(&str, Address)] = &[("validator-3", Address::from_slice(&[0x33; 20]))];
    let (mut new_children, mut new_endpoints) =
        start_nodes(temp_path, nodes_to_start, "worker_config_test", 2)?;
    let new_child = new_children.pop().expect("child");
    guard.replace(kill_idx, new_child);
    endpoints[kill_idx] = new_endpoints.pop().expect("endpoint");

    let restarted_url = endpoints[kill_idx].http_url.clone();
    let restarted_provider = ProviderBuilder::new().connect_http(restarted_url.parse()?);
    timeout(Duration::from_secs(EPOCH_DURATION * 4), async {
        let mut result = restarted_provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "basefee-test", "restarted node not ready: {e:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = restarted_provider.get_chain_id().await;
        }
    })
    .await?;

    loop_epochs(6, 2, &rpc_url).await?;

    let synced_fee = get_base_fee(&restarted_url).await?;
    info!(target: "basefee-test", synced_fee, "restarted node base fee");
    assert_eq!(synced_fee, 42, "restarted node base fee should be 42, got {synced_fee}");

    for (i, ep) in endpoints.iter().enumerate() {
        let fee = get_base_fee(&ep.http_url).await?;
        info!(target: "basefee-test", i, fee, "final node base fee");
        assert_eq!(fee, 42, "node {i} base fee should be 42, got {fee}");
    }

    info!(target: "basefee-test", "worker config governance update test passed");
    Ok(())
}
