//! E2e tests for the simplified startup flow.
//!
//! Every node start already exercises the app-level bring-up (bootstrap peers, listeners,
//! dials, soft peer wait) and the epoch record-sync gate; these tests target the startup
//! shapes the rest of the suite does not: a validator restarting mid-epoch plus a
//! simultaneous full restart of an epoch-enabled network, a cold-genesis node booting alone
//! with zero peers, and a fresh node joining an established network after multiple epoch
//! boundaries have passed.

use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::{config_local_testnet, config_local_testnet_with_epoch_duration};
use eyre::Report;
use std::{process::Child, time::Duration};
use tn_reth::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
use tn_types::{get_available_tcp_port, EpochCertificate, EpochRecord};
use tokio::time::Instant;
use tracing::info;

use crate::common::{
    address_from_word, get_block, get_key, get_latest_consensus_header_number, network_advancing,
    send_and_confirm, start_observer, start_validator, ProcessGuard,
};

/// Epoch duration (secs) for the epoch-enabled startup tests; mirrors `epochs.rs`.
const EPOCH_DURATION: u64 = 10;

/// Poll the consensus registry until the current epoch reaches `target`, with a deadline
/// scaled to how many boundaries still have to pass.
async fn wait_for_epoch_at_least(rpc_url: &str, target: u32) -> eyre::Result<u32> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 4 * (target as u64 + 1));
    loop {
        if let Ok(info) = registry.getCurrentEpochInfo().call().await {
            if info.epochId >= target {
                return Ok(info.epochId);
            }
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!("epoch did not reach {target} before the deadline"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Poll `node` until its latest consensus header height reaches `target`, up to `max_secs`.
fn wait_for_consensus_height(node: &str, target: u64, max_secs: u64) -> eyre::Result<u64> {
    for _ in 0..max_secs {
        if let Ok(height) = get_latest_consensus_header_number(node) {
            if height >= target {
                return Ok(height);
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(eyre::eyre!("node {node} did not reach consensus height {target} within {max_secs}s"))
}

/// Fetch a certified epoch record over RPC, retrying until `deadline_secs` elapses.
async fn epoch_record_with_retry(
    rpc_url: &str,
    epoch: u32,
    deadline_secs: u64,
) -> eyre::Result<(EpochRecord, EpochCertificate)> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        match provider
            .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (epoch,))
            .await
        {
            Ok(result) => return Ok(result),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                return Err(eyre::eyre!("epoch record {epoch} not available on {rpc_url}: {e}"))
            }
        }
    }
}

/// Assert all nodes report the same block hash at the lowest common height.
fn assert_blocks_same(client_urls: &[String; 4]) -> eyre::Result<()> {
    let block0 = get_block(&client_urls[0], None)?;
    let number = u64::from_str_radix(&block0["number"].as_str().unwrap_or("0x100_000")[2..], 16)?;
    for url in client_urls.iter().skip(1) {
        let block = get_block(url, Some(number))?;
        if block0["hash"] != block["hash"] {
            return Err(Report::msg(format!(
                "block {number} differs between {} and {url}: {:?} vs {:?}",
                &client_urls[0], block0["hash"], block["hash"]
            )));
        }
    }
    Ok(())
}

/// A validator killed mid-epoch and restarted must rejoin (restart replay path through the
/// record-sync gate), and a simultaneous full restart of every node — all four gating at
/// once — must converge and resume closing epochs.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_startup_restart_mid_epoch_and_full_restart() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
    rt.block_on(restart_mid_epoch_and_full_restart_inner())
}

async fn restart_mid_epoch_and_full_restart_inner() -> eyre::Result<()> {
    info!(target: "startup-test", "test_startup_restart_mid_epoch_and_full_restart");
    let tmp_guard = tempfile::TempDir::with_prefix("startup_restart").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0; 4];
    for i in 0..4 {
        let rpc_port =
            get_available_tcp_port("127.0.0.1").expect("Failed to get an ephemeral rpc port!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, bin, &temp_path, rpc_port, "startup_restart", 0));
    }
    network_advancing(&client_urls)?;

    // let the network cross at least one boundary so the restarted node replays into a
    // running epoch rather than a fresh chain
    wait_for_epoch_at_least(&client_urls[0], 1).await?;
    // land inside an epoch, away from the boundary just crossed
    tokio::time::sleep(Duration::from_secs(2)).await;

    // kill validator-2 mid-epoch and restart it shortly after
    let mut child2 = guard.take(2).expect("missing child 2");
    super::common::kill_child(&mut child2);
    tokio::time::sleep(Duration::from_secs(3)).await;
    guard.replace(2, start_validator(2, bin, &temp_path, rpc_ports[2], "startup_restart", 1));

    // the restarted node must catch back up to the others
    let target = get_latest_consensus_header_number(&client_urls[0])?;
    wait_for_consensus_height(&client_urls[2], target, 120)?;

    // and the network must keep closing epochs with it back
    let epoch_before_full_restart = wait_for_epoch_at_least(&client_urls[0], 2).await?;

    // now stop the entire network and restart every node at once: all four run the
    // record-sync gate simultaneously, each holding complete local records
    guard.kill_all();
    let to_account = address_from_word("startup-restart-target");
    assert!(crate::common::get_balance(&client_urls[0], &to_account.to_string(), 5).is_err());

    for (i, rpc_port) in rpc_ports.iter().enumerate() {
        guard.replace(i, start_validator(i, bin, &temp_path, *rpc_port, "startup_restart", 2));
    }
    network_advancing(&client_urls)?;

    // epochs must resume advancing past where the network stopped
    wait_for_epoch_at_least(&client_urls[0], epoch_before_full_restart + 1).await?;

    // full liveness: a transaction confirms and all nodes agree on blocks
    let key = get_key("test-source");
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0)?;
    assert_blocks_same(&client_urls)?;
    Ok(())
}

/// The first node of a fresh network boots alone: the soft peer wait and the record-sync
/// gate must proceed without peers instead of hanging or exiting, and once peers arrive the
/// node must participate in consensus as an active committee member.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_startup_cold_genesis_staggered() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "startup-test", "test_startup_cold_genesis_staggered");
    let tmp_guard = tempfile::TempDir::with_prefix("startup_cold_genesis").expect("tempdir");
    let temp_path = tmp_guard.path().to_path_buf();
    config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
        .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0; 4];
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port =
            get_available_tcp_port("127.0.0.1").expect("Failed to get an ephemeral rpc port!");
        rpc_ports[i] = rpc_port;
        url.push_str(&format!(":{rpc_port}"));
    }

    // boot ONLY validator-0 and give it well past the soft peer wait (10s) and the gate
    // deadline (20s) alone
    guard.push(start_validator(0, bin, &temp_path, rpc_ports[0], "startup_cold", 0));
    std::thread::sleep(Duration::from_secs(35));

    // the lone node must still be running (no hard failure in bring-up or the gate) and
    // already serving RPC while it waits for peers in the epoch loop
    let child0: &mut Child = guard.get_mut(0).expect("child 0");
    assert!(
        child0.try_wait()?.is_none(),
        "lone genesis node exited during startup. Check logs in test_logs/startup_cold/"
    );
    assert!(
        crate::common::get_block_number(&client_urls[0]).is_ok(),
        "lone genesis node not serving rpc while waiting for peers"
    );

    // the rest of the cohort joins late
    for (i, rpc_port) in rpc_ports.iter().enumerate().skip(1) {
        guard.push(start_validator(i, bin, &temp_path, *rpc_port, "startup_cold", 0));
    }
    network_advancing(&client_urls)?;

    // prove the first node is an ACTIVE cvv, not stuck as an observer: kill validator-3 so
    // quorum (3 of 4) requires validator-0's votes, then confirm a transaction
    if let Some(mut child3) = guard.take(3) {
        super::common::kill_child(&mut child3);
    }
    let key = get_key("test-source");
    let to_account = address_from_word("startup-cold-target");
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0)?;
    Ok(())
}

/// A brand-new node (fresh datadir) joins an established network after two epoch boundaries
/// have already passed: the record-sync gate must fetch and verify the certified record
/// chain from peers, the node runs as an observer (its key is in no committee), and
/// execution catches up to the tip.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_startup_fresh_node_joins_after_two_epochs() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
    rt.block_on(fresh_node_joins_after_two_epochs_inner())
}

async fn fresh_node_joins_after_two_epochs_inner() -> eyre::Result<()> {
    info!(target: "startup-test", "test_startup_fresh_node_joins_after_two_epochs");
    let tmp_guard = tempfile::TempDir::with_prefix("startup_fresh_join").expect("tempdir");
    let temp_path = tmp_guard.path().to_path_buf();
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port =
            get_available_tcp_port("127.0.0.1").expect("Failed to get an ephemeral rpc port!");
        url.push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, bin, &temp_path, rpc_port, "startup_fresh_join", 0));
    }
    network_advancing(&client_urls)?;

    // let two epochs close so the joiner is behind multiple certified records
    wait_for_epoch_at_least(&client_urls[0], 2).await?;

    // certified records for epochs 0 and 1 exist on the validator side (certificates land
    // asynchronously after the boundary, so poll)
    let validator_rec_0 = epoch_record_with_retry(&client_urls[0], 0, EPOCH_DURATION * 6).await?;
    let validator_rec_1 = epoch_record_with_retry(&client_urls[0], 1, EPOCH_DURATION * 6).await?;
    assert!(validator_rec_0.0.verify_with_cert(&validator_rec_0.1));
    assert!(validator_rec_1.0.verify_with_cert(&validator_rec_1.1));

    // now boot the fresh node (empty datadir, key in no committee)
    let obs_rpc_port =
        get_available_tcp_port("127.0.0.1").expect("Failed to get an ephemeral rpc port!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    guard.push(start_observer(4, bin, &temp_path, obs_rpc_port, "startup_fresh_join", 0));

    // it must catch up to the validators' consensus height (pack download + execution)
    let validator_height = get_latest_consensus_header_number(&client_urls[0])?;
    wait_for_consensus_height(&obs_url, validator_height, 120)?;

    // the record chain it synced must match the network's certified records exactly
    let obs_rec_0 = epoch_record_with_retry(&obs_url, 0, EPOCH_DURATION * 3).await?;
    let obs_rec_1 = epoch_record_with_retry(&obs_url, 1, EPOCH_DURATION * 3).await?;
    assert!(obs_rec_0.0.verify_with_cert(&obs_rec_0.1), "fresh node epoch-0 record invalid");
    assert!(obs_rec_1.0.verify_with_cert(&obs_rec_1.1), "fresh node epoch-1 record invalid");
    assert_eq!(obs_rec_0.0, validator_rec_0.0, "epoch-0 record diverges");
    assert_eq!(obs_rec_1.0, validator_rec_1.0, "epoch-1 record diverges");

    // liveness through the fresh node: it accepts a transaction that a validator confirms
    let key = get_key("test-source");
    let to_account = address_from_word("startup-fresh-join-target");
    send_and_confirm(&obs_url, &client_urls[1], &key, to_account, 0)?;
    Ok(())
}
