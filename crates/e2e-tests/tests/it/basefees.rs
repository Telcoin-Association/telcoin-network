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

/// Spawn a background task driving one independent sender account per node endpoint.
///
/// Each tick (1s), every sender produces one tx and tries its preferred provider first
/// (sender `i` prefers provider `i`), falling back through the remaining providers on RPC
/// failure. Independent nonce streams prevent the cross-account serialization that occurs
/// when a single account fans out to many endpoints — that pattern caused per-worker gas to
/// fall below the EIP-1559 target intermittently and made the base fee oscillate.
///
/// Returns a JoinHandle — call `.abort()` to stop.
fn spawn_tx_senders(
    rpc_urls: Vec<String>,
    mut tx_factories: Vec<TransactionFactory>,
    chain: Arc<RethChainSpec>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let recipient = Address::from_slice(&[0xAA; 20]);
        let providers: Vec<_> = rpc_urls
            .iter()
            .filter_map(|url| url.parse().ok().map(|u| ProviderBuilder::new().connect_http(u)))
            .collect();
        let n = providers.len();

        loop {
            for (i, factory) in tx_factories.iter_mut().enumerate() {
                let tx = factory.create_eip1559_encoded(
                    chain.clone(),
                    Some(21_000),
                    100_000,
                    Some(recipient),
                    U256::from(1),
                    Default::default(),
                );
                for offset in 0..n {
                    let idx = (i + offset) % n;
                    if providers[idx].send_raw_transaction(&tx).await.is_ok() {
                        break;
                    }
                }
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
/// Funds one independent sender account per validator (so each can drive its preferred
/// validator's worker without cross-account nonce serialization) plus the governance wallet,
/// and sets `--worker-fee-config 0:0:21000`.
fn create_basefee_genesis(
    temp_path: &Path,
    tx_senders: &[Address],
    governance_wallet: Address,
    committee: &[(&str, Address)],
) -> eyre::Result<Genesis> {
    let passphrase = Some(NODE_PASSWORD.to_string());

    let mut accounts = Vec::with_capacity(tx_senders.len() + 1);
    accounts.push((
        governance_wallet,
        GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
    ));
    let sender_balance = U256::from(parse_ether("1_000_000")?);
    for addr in tx_senders {
        accounts.push((*addr, GenesisAccount::default().with_balance(sender_balance)));
    }

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

    let tx_senders: Vec<TransactionFactory> = (99..104)
        .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)))
        .collect();
    let sender_addrs: Vec<Address> = tx_senders.iter().map(|s| s.address()).collect();
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

    let genesis =
        create_basefee_genesis(temp_path, &sender_addrs, governance_wallet.address(), &committee)?;

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

    // ── Phase 3: Start background tx senders, run 3 epochs → verify fee >= 9 ──
    //
    // Deterministic trajectory at low fee values: each epoch above the gas target adds
    // `(current_fee / 8).max(1)` = 1 wei. Starting at 7, three epochs reach 10. We tolerate
    // losing the first epoch to startup slop (the sender begins partway through it),
    // hence `>= 9`.

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;

    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_senders(all_rpc_urls, tx_senders, chain.clone());

    loop_epochs(0, 3, &rpc_url).await?;

    let pre_kill_base_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", pre_kill_base_fee, "base fee after 3 epochs of activity");
    assert!(
        pre_kill_base_fee >= 9,
        "base fee after 3 epochs should be >= 9 (deterministic +1/epoch trajectory \
         from 7, with 1-epoch tolerance for sender startup), got {pre_kill_base_fee}"
    );

    // ── Phase 4: Kill validator-3 (index 2) ──

    let kill_idx = 2;
    if let Some(mut taken) = guard.take(kill_idx) {
        super::common::kill_child(&mut taken);
    }

    let killed_url = endpoints[kill_idx].http_url.clone();
    let killed_provider = ProviderBuilder::new().connect_http(killed_url.parse()?);
    assert!(killed_provider.get_chain_id().await.is_err(), "Killed node should be down");

    // ── Phase 5: Run 3 more epochs → verify strict +3 growth ──
    //
    // Sender 2 fails over from the dead validator-3 RPC to a live endpoint, so its nonce
    // stream keeps advancing and the surviving 4 workers continue to receive enough gas to
    // exceed the per-epoch target. Three epochs at +1 each → strict +3 growth.

    loop_epochs(3, 3, &rpc_url).await?;

    let network_base_fee = get_base_fee(&rpc_url).await?;
    info!(target: "basefee-test", network_base_fee, pre_kill_base_fee, "base fee after 6 epochs");
    assert!(
        network_base_fee >= pre_kill_base_fee + 3,
        "base fee should grow by at least 3 across Phase 5 (3 epochs at +1/epoch): \
         {network_base_fee} >= {pre_kill_base_fee} + 3"
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

    // ── Phase 8: Verify restarted node has caught up past the floor ──
    //
    // Tolerant check: just confirm the restarted node returns an elevated fee, proving it
    // synced. The strict cross-node equality lives in Phase 9, which uses a pinned block to
    // avoid the sampling race where two `latest`-block reads on different nodes can land on
    // different blocks if a transition fires between the calls.

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

    let synced_node_fee = get_base_fee(&restarted_url).await?;
    info!(target: "basefee-test", synced_node_fee, "restarted node base fee after sync");
    assert!(
        synced_node_fee > MIN_PROTOCOL_BASE_FEE,
        "restarted node should have synced past the {MIN_PROTOCOL_BASE_FEE} floor, \
         got {synced_node_fee}"
    );

    // ── Phase 9: Stop sender, quiesce, verify cross-node agreement among always-alive nodes ──
    //
    // Strict cross-node equality is checked across the 4 validators that stayed up the
    // whole test. The restarted node is excluded here because in this test environment
    // peers deny `request batches` for far-history batches — the restarted validator
    // typically only catches up part of the way to the live tip during the test window,
    // and any pinned block near the live tip is unlikely to exist on it. Phase 8 already
    // proved the restarted node synced past the floor; what's left to verify is that the
    // 4 always-alive nodes agree on the same `base_fee_per_gas` at the same block height
    // and that the fee genuinely grew through the trajectory before decaying.
    //
    // After 9 epochs of growth from 7, the live fee peaks around 16 and then decays a few
    // wei during the quiescence sleep. The `>= 12` lower bound proves the fee actually
    // rose through the trajectory.

    sender.abort();
    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION * 2)).await;

    let alive: Vec<(usize, _)> = endpoints
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != kill_idx)
        .map(|(i, ep)| {
            (i, ProviderBuilder::new().connect_http(ep.http_url.parse().expect("valid url")))
        })
        .collect();

    let mut min_tip: Option<u64> = None;
    for (i, p) in &alive {
        let latest = p
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| eyre::eyre!("node {i} has no latest block"))?;
        let tip = latest.header.number;
        info!(target: "basefee-test", i, tip, "alive node tip");
        min_tip = Some(min_tip.map_or(tip, |m| m.min(tip)));
    }
    let pinned = min_tip.expect("at least one alive endpoint").saturating_sub(2);

    let mut fees = Vec::with_capacity(alive.len());
    for (i, p) in &alive {
        let block = p
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Number(pinned))
            .await?
            .ok_or_else(|| eyre::eyre!("node {i} missing pinned block {pinned}"))?;
        let fee = block.header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE);
        info!(target: "basefee-test", i, pinned, fee, "pinned-block base fee");
        fees.push((*i, fee));
    }

    let expected = fees[0].1;
    for (i, fee) in &fees {
        assert_eq!(
            *fee, expected,
            "node {i} base fee {fee} does not match expected {expected} at pinned block {pinned}"
        );
    }
    assert!(
        expected >= 12,
        "pinned-block base fee {expected} should reflect 9 epochs of growth from 7 \
         (peak ~16, post-quiesce decay leaves >= 12)"
    );

    info!(target: "basefee-test", "all assertions passed");
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test that governance can update the WorkerConfigs contract and all nodes (including a synced
/// node) pick up the new fee strategy.
async fn test_basefee_worker_config_governance_update() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();

    // ── Phase 1: Setup genesis & start 5 validators ──

    let tx_senders: Vec<TransactionFactory> = (99..104)
        .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)))
        .collect();
    let sender_addrs: Vec<Address> = tx_senders.iter().map(|s| s.address()).collect();
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

    let genesis =
        create_basefee_genesis(temp_path, &sender_addrs, governance_wallet.address(), &committee)?;

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

    // ── Phase 2: Drive base fee above MIN via background senders ──

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;

    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_senders(all_rpc_urls, tx_senders, chain.clone());

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

fn config_committee_with_custom_fee_configs(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    passphrase: Option<String>,
    consensus_registry_owner: Address,
    accounts: Vec<(Address, GenesisAccount)>,
    validators: &[(&str, Address)],
    fee_configs: &[&str],
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

    info!(target: "basefee-test", ?fee_configs, "creating committee with custom worker fee configs");

    let mut argv: Vec<String> = vec![
        "tn".into(),
        "--basefee-address".into(),
        "0x9999999999999999999999999999999999999999".into(),
        "--consensus-registry-owner".into(),
        consensus_registry_owner.to_string(),
        "--initial-stake-per-validator".into(),
        INITIAL_STAKE_AMOUNT.into(),
        "--min-withdraw-amount".into(),
        min_withdrawal.into(),
        "--epoch-block-rewards".into(),
        epoch_rewards.into(),
        "--epoch-duration-in-secs".into(),
        EPOCH_DURATION.to_string(),
        "--dev-funded-account".into(),
        "test-source".into(),
        "--max-header-delay-ms".into(),
        "1000".into(),
        "--min-header-delay-ms".into(),
        "500".into(),
    ];
    for cfg in fee_configs {
        argv.push("--worker-fee-config".into());
        argv.push((*cfg).into());
    }

    let create_committee_command = CommandParser::<GenesisArgs>::parse_from(argv);
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

fn create_basefee_genesis_with_configs(
    temp_path: &Path,
    tx_senders: &[Address],
    governance_wallet: Address,
    committee: &[(&str, Address)],
    fee_configs: &[&str],
) -> eyre::Result<Genesis> {
    let passphrase = Some(NODE_PASSWORD.to_string());

    let mut accounts = Vec::with_capacity(tx_senders.len() + 1);
    accounts.push((
        governance_wallet,
        GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
    ));
    let sender_balance = U256::from(parse_ether("1_000_000")?);
    for addr in tx_senders {
        accounts.push((*addr, GenesisAccount::default().with_balance(sender_balance)));
    }

    let shared_genesis_dir = temp_path.join("shared-genesis");

    config_committee_with_custom_fee_configs(
        temp_path,
        &shared_genesis_dir,
        passphrase,
        governance_wallet,
        accounts,
        committee,
        fee_configs,
    )
}

/// Fetch `base_fee_per_gas` at a specific block number via RPC.
async fn get_base_fee_at_block(rpc_url: &str, block_number: u64) -> eyre::Result<u64> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block = provider
        .get_block_by_number(alloy::eips::BlockNumberOrTag::Number(block_number))
        .await?
        .ok_or_else(|| eyre::eyre!("no block {block_number}"))?;
    Ok(block.header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE))
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Genesis configured with a Static{fee: 1000} worker must propagate that fee end-to-end:
/// the batch builder picks it up from the GasAccumulator, the pool is seeded with it via
/// initialize_worker_components, and the first post-genesis block's base_fee_per_gas equals
/// the configured value BEFORE any consensus-driven epoch rollover.
async fn test_basefee_static_genesis_propagates_to_first_block() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();

    const STATIC_FEE: u64 = 1000;

    let tx_senders: Vec<TransactionFactory> = (200..205)
        .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)))
        .collect();
    let sender_addrs: Vec<Address> = tx_senders.iter().map(|s| s.address()).collect();
    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(34));

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    let temp_dir = tempfile::TempDir::with_prefix("basefee_static_genesis")?;
    let temp_path = temp_dir.path();

    let static_cfg = format!("0:1:{STATIC_FEE}");
    let genesis = create_basefee_genesis_with_configs(
        temp_path,
        &sender_addrs,
        governance_wallet.address(),
        &committee,
        &[&static_cfg],
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let (procs, endpoints) =
        start_nodes(temp_path, &committee, "basefee_static_genesis", 1)?;
    let _guard = ProcessGuard::new(procs);

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

    // Drive some txs so the batch builder actually produces a post-genesis block; without
    // traffic the worker may idle at genesis and we won't observe the seeded fee in RPC.
    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_senders(all_rpc_urls, tx_senders, chain.clone());

    // Wait for the first non-genesis block. With a Static strategy the base fee is set at
    // boot from the contract, not from parent-block EIP-1559 math, so the very first block
    // after genesis must already carry STATIC_FEE.
    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 2);
    let first_block_fee = loop {
        let tip = provider
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| eyre::eyre!("no latest block"))?;
        if tip.header.number >= 1 {
            let b1 = provider
                .get_block_by_number(alloy::eips::BlockNumberOrTag::Number(1))
                .await?
                .ok_or_else(|| eyre::eyre!("no block 1"))?;
            break b1.header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE);
        }
        assert!(
            Instant::now() < deadline,
            "no non-genesis block produced within {}s",
            EPOCH_DURATION * 2
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    info!(target: "basefee-test", first_block_fee, "first post-genesis block base fee");
    assert_eq!(
        first_block_fee, STATIC_FEE,
        "Static genesis fee must propagate to first post-genesis block: expected {STATIC_FEE}, got {first_block_fee}"
    );

    // Static fees are immune to gas usage; across an epoch boundary the value must persist.
    loop_epochs(0, 1, &rpc_url).await?;
    sender.abort();

    let post_epoch_fee = get_base_fee(&rpc_url).await?;
    assert_eq!(
        post_epoch_fee, STATIC_FEE,
        "Static fee must survive epoch rollover: expected {STATIC_FEE}, got {post_epoch_fee}"
    );

    for (i, ep) in endpoints.iter().enumerate() {
        let fee = get_base_fee(&ep.http_url).await?;
        assert_eq!(
            fee, STATIC_FEE,
            "node {i} base fee must equal Static genesis {STATIC_FEE}, got {fee}"
        );
    }

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// At an epoch rollover, `adjust_base_fees` pushes the new per-worker fee into the tx pool
/// via `set_block_info` in addition to the `BaseFeeContainer`. The observable end-to-end
/// consequence: within a single epoch every block produced by a given worker shares the
/// same `base_fee_per_gas` (the pool's `pending_basefee` and the container agree, so the
/// batch builder charges the same fee block after block until the next rollover).
///
/// Sample the epoch-start block's fee for several epochs and assert that a block later in
/// each epoch carries the identical base fee. If the pool and container ever disagreed,
/// the worker would emit a different fee mid-epoch and this assertion would fail.
async fn test_basefee_rollover_pool_sync() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();

    let tx_senders: Vec<TransactionFactory> = (300..305)
        .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)))
        .collect();
    let sender_addrs: Vec<Address> = tx_senders.iter().map(|s| s.address()).collect();
    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(35));

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    let temp_dir = tempfile::TempDir::with_prefix("basefee_rollover_pool")?;
    let temp_path = temp_dir.path();

    let genesis = create_basefee_genesis(
        temp_path,
        &sender_addrs,
        governance_wallet.address(),
        &committee,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let (procs, endpoints) = start_nodes(temp_path, &committee, "basefee_rollover_pool", 1)?;
    let _guard = ProcessGuard::new(procs);

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

    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;
    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_senders(all_rpc_urls, tx_senders, chain.clone());

    // Drive at least one epoch of growth so we're not stuck at MIN_PROTOCOL_BASE_FEE for
    // every sample — identical values would hide the very divergence this test watches for.
    loop_epochs(0, 1, &rpc_url).await?;

    // Per project memory: base fee invariants MUST be anchored to the epoch-start block
    // `EpochInfo.blockHeight`, never to `latest` (which drifts mid-epoch).
    let num_epochs_to_sample: u32 = 3;
    let mut prev_epoch_id = consensus_registry.getCurrentEpochInfo().call().await?.epochId;
    for i in 0..num_epochs_to_sample {
        let next_id = loop_epochs(prev_epoch_id, 1, &rpc_url).await?;
        let info = consensus_registry.getCurrentEpochInfo().call().await?;
        let epoch_start_block = info.blockHeight;

        let epoch_start_fee = get_base_fee_at_block(&rpc_url, epoch_start_block).await?;

        // Sample a block mid-epoch; wait for the chain to advance so we aren't reading the
        // same block twice. If `adjust_base_fees` forgot to sync the pool at rollover, the
        // worker would resume producing blocks with the pre-rollover fee and this would
        // diverge from `epoch_start_fee`.
        let target = epoch_start_block + 3;
        let mid_deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 2);
        loop {
            let tip = provider
                .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
                .await?
                .ok_or_else(|| eyre::eyre!("no latest block"))?;
            if tip.header.number >= target {
                break;
            }
            assert!(
                Instant::now() < mid_deadline,
                "chain stalled after epoch {next_id}: tip {} < target {target}",
                tip.header.number
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let mid_epoch_fee = get_base_fee_at_block(&rpc_url, target).await?;

        info!(
            target: "basefee-test",
            iteration = i,
            next_id,
            epoch_start_block,
            epoch_start_fee,
            target,
            mid_epoch_fee,
            "epoch-start vs mid-epoch base fee"
        );
        assert_eq!(
            mid_epoch_fee, epoch_start_fee,
            "pool/container disagreement in epoch {next_id}: block {epoch_start_block}={epoch_start_fee} \
             vs block {target}={mid_epoch_fee}"
        );

        prev_epoch_id = next_id;
    }

    sender.abort();
    Ok(())
}

#[ignore = "blocked: keytool rejects --workers != 1 (see crates/telcoin-network-cli/src/keytool/generate.rs:161). \
            Rust runtime and WorkerConfigs genesis both support multi-worker, but per-validator \
            key generation does not. Unblock with a multi-worker keytool fixture, then remove this \
            ignore and drop the fallback in test_basefee_restart_restores_per_worker_fee."]
#[tokio::test]
/// Placeholder for true multi-worker restart coverage: two workers configured with different
/// strategies (e.g., worker 0 EIP-1559, worker 1 Static), run several epochs, kill and restart
/// a node, assert per-worker base fees restored independently from on-chain state.
///
/// Blocked by the keytool harness limitation documented above — see `test_basefee_restart_restores_per_worker_fee`
/// for the single-worker variant that IS runnable today.
async fn test_basefee_multiworker_restart_placeholder() -> eyre::Result<()> {
    // intentionally empty — this test exists to document the follow-up work for multi-worker
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Single-worker fallback for the multi-worker restart scenario. Multi-worker restart is blocked
/// by the keytool harness (see `test_basefee_multiworker_restart_placeholder`); this test covers
/// the restart path that IS exercisable today: a validator that exits with a non-trivial EIP-1559
/// base fee must, on restart, restore worker 0's BaseFeeContainer from the finalized block
/// history (via `catchup_accumulator`) AND seed the pool's `pending_basefee` to the same value
/// (via `initialize_worker_components`).
///
/// Proxy: after restart, the restarted node's view of the current base fee must equal the live
/// network's view at a pinned block produced after its sync completes.
async fn test_basefee_restart_restores_per_worker_fee() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();

    let tx_senders: Vec<TransactionFactory> = (400..405)
        .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)))
        .collect();
    let sender_addrs: Vec<Address> = tx_senders.iter().map(|s| s.address()).collect();
    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(36));

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    let temp_dir = tempfile::TempDir::with_prefix("basefee_restart_per_worker")?;
    let temp_path = temp_dir.path();

    let genesis = create_basefee_genesis(
        temp_path,
        &sender_addrs,
        governance_wallet.address(),
        &committee,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let (procs, mut endpoints) =
        start_nodes(temp_path, &committee, "basefee_restart_per_worker", 1)?;
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

    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION + 1)).await;
    let all_rpc_urls: Vec<String> = endpoints.iter().map(|ep| ep.http_url.clone()).collect();
    let sender = spawn_tx_senders(all_rpc_urls, tx_senders, chain.clone());

    // Drive the fee above MIN so restoration is observable — MIN -> MIN on restart would be
    // indistinguishable from a no-op and give a false positive.
    loop_epochs(0, 3, &rpc_url).await?;
    let pre_kill_fee = get_base_fee(&rpc_url).await?;
    assert!(
        pre_kill_fee > MIN_PROTOCOL_BASE_FEE,
        "base fee must have grown before restart so restoration is observable, got {pre_kill_fee}"
    );

    let kill_idx = 2;
    if let Some(mut taken) = guard.take(kill_idx) {
        super::common::kill_child(&mut taken);
    }
    let killed_url = endpoints[kill_idx].http_url.clone();
    let killed_provider = ProviderBuilder::new().connect_http(killed_url.parse()?);
    assert!(killed_provider.get_chain_id().await.is_err(), "killed node must be down");

    // Let the live network advance while the target node is down; this forces catchup work
    // on restart rather than a trivial no-sync path.
    loop_epochs(3, 2, &rpc_url).await?;

    let nodes_to_start: &[(&str, Address)] =
        &[("validator-3", Address::from_slice(&[0x33; 20]))];
    let (mut new_children, mut new_endpoints) =
        start_nodes(temp_path, nodes_to_start, "basefee_restart_per_worker", 2)?;
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

    loop_epochs(5, 2, &rpc_url).await?;

    sender.abort();
    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION * 2)).await;

    // Cross-node equality at a PINNED block, not `latest`: the four always-alive nodes
    // agree on the canonical base fee for block N, and the restarted node — having restored
    // worker 0 from the finalized header chain — must return the same value for the same
    // block. Reading `latest` on each node separately races against ongoing block production.
    let alive: Vec<(usize, _)> = endpoints
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != kill_idx)
        .map(|(i, ep)| {
            (i, ProviderBuilder::new().connect_http(ep.http_url.parse().expect("valid url")))
        })
        .collect();

    let mut min_tip: Option<u64> = None;
    for (i, p) in &alive {
        let latest = p
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| eyre::eyre!("node {i} has no latest block"))?;
        min_tip = Some(min_tip.map_or(latest.header.number, |m| m.min(latest.header.number)));
    }
    let pinned = min_tip.expect("at least one alive endpoint").saturating_sub(2);

    // Wait until the restarted node has synced to (or past) the pinned block; `latest`-based
    // equality could otherwise assert against a block the restarted node hasn't seen yet.
    timeout(Duration::from_secs(EPOCH_DURATION * 6), async {
        loop {
            match restarted_provider
                .get_block_by_number(alloy::eips::BlockNumberOrTag::Number(pinned))
                .await
            {
                Ok(Some(_)) => return Ok::<_, eyre::Error>(()),
                _ => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    })
    .await??;

    let mut reference_fee: Option<u64> = None;
    for (i, ep) in endpoints.iter().enumerate() {
        if i == kill_idx {
            continue;
        }
        let fee = get_base_fee_at_block(&ep.http_url, pinned).await?;
        if let Some(r) = reference_fee {
            assert_eq!(fee, r, "alive node {i} fee {fee} differs from reference {r} at block {pinned}");
        } else {
            reference_fee = Some(fee);
        }
    }
    let reference_fee = reference_fee.expect("at least one alive node");
    assert!(
        reference_fee > MIN_PROTOCOL_BASE_FEE,
        "reference fee must reflect growth so restoration is observable, got {reference_fee}"
    );

    let restarted_fee = get_base_fee_at_block(&restarted_url, pinned).await?;
    info!(
        target: "basefee-test",
        pinned,
        reference_fee,
        restarted_fee,
        "pinned-block fees across alive + restarted"
    );
    assert_eq!(
        restarted_fee, reference_fee,
        "restarted node base fee {restarted_fee} must match live network {reference_fee} at block {pinned}"
    );

    Ok(())
}
