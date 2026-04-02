//! E2E test for base fee adjustment and sync recovery.
//!
//! Verifies:
//! 1. Base fees start at MIN_PROTOCOL_BASE_FEE (7 wei)
//! 2. Sending transactions that exceed the gas target causes base fees to increase over epochs
//! 3. A node that was offline can sync and recover the correct base fee

use super::common::ProcessGuard;
use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
};
use clap::Parser as _;
use e2e_tests::{create_validator_info, setup_log_dir, NodeEndpoints};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{path::Path, process::Child, sync::Arc, time::Duration};
use telcoin_network_cli::genesis::GenesisArgs;
use tn_config::{Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
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

/// Number of value transfers to send per epoch.
const TRANSFERS_PER_EPOCH: usize = 5;

/// Get the base fee from the latest block via RPC.
async fn get_base_fee(rpc_url: &str) -> eyre::Result<u64> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider
        .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
        .await?
        .ok_or_else(|| eyre::eyre!("no latest block"))?;
    Ok(block.header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE))
}

/// Send `count` simple value transfers (21,000 gas each) and wait for confirmation.
async fn send_value_transfers(
    rpc_url: &str,
    tx_factory: &mut TransactionFactory,
    chain: &Arc<RethChainSpec>,
    count: usize,
) -> eyre::Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let recipient = Address::from_slice(&[0xAA; 20]);

    for i in 0..count {
        let tx = tx_factory.create_eip1559_encoded(
            chain.clone(),
            Some(21_000), // exact gas for simple transfer
            100_000,      // generous max_fee_per_gas
            Some(recipient),
            U256::from(1), // 1 wei
            Default::default(),
        );
        let pending = provider.send_raw_transaction(&tx).await?;
        debug!(target: "basefee-test", i, "sent transfer, waiting for confirmation");
        // Allow two full epoch durations + startup buffer for confirmation
        timeout(Duration::from_secs(EPOCH_DURATION * 2 + 11), pending.watch()).await??;
    }
    Ok(())
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
    let epoch_rewards = "1000";

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

    let mut tx_sender = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(99));
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

    // ── Phase 3: Run 3 epochs sending transfers each → verify fee > 7 ──

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;

    for epoch in 0..3 {
        send_value_transfers(&rpc_url, &mut tx_sender, &chain, TRANSFERS_PER_EPOCH).await?;
        info!(target: "basefee-test", epoch, "sent transfers, waiting for epoch transition");
        loop_epochs(epoch, 1, &rpc_url).await?;
    }

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

    // ── Phase 5: Run 3 more epochs with transfers → verify fee increased further ──

    for epoch in 3..6 {
        send_value_transfers(&rpc_url, &mut tx_sender, &chain, TRANSFERS_PER_EPOCH).await?;
        info!(target: "basefee-test", epoch, "sent transfers (node down), waiting for epoch");
        loop_epochs(epoch, 1, &rpc_url).await?;
    }

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

    for epoch in 6..9 {
        send_value_transfers(&rpc_url, &mut tx_sender, &chain, TRANSFERS_PER_EPOCH).await?;
        info!(target: "basefee-test", epoch, "sent transfers (after restart), waiting for epoch");
        loop_epochs(epoch, 1, &rpc_url).await?;
    }

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

    // ── Phase 9: Verify all nodes have base fees > 7 ──

    for (i, ep) in endpoints.iter().enumerate() {
        let fee = get_base_fee(&ep.http_url).await?;
        info!(target: "basefee-test", i, fee, "node base fee");
        assert!(
            fee > MIN_PROTOCOL_BASE_FEE,
            "node {i} base fee should be > {MIN_PROTOCOL_BASE_FEE}, got {fee}"
        );
    }

    info!(target: "basefee-test", "all assertions passed");
    Ok(())
}
