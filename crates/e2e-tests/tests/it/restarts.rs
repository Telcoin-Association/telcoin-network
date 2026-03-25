use alloy::primitives::address;
use e2e_tests::{config_local_testnet, setup_log_dir};
use escargot::CargoRun;
use ethereum_tx_sign::{LegacyTransaction, Transaction};
use eyre::Report;
use jsonrpsee::{
    core::{client::ClientT, DeserializeOwned},
    http_client::HttpClientBuilder,
    rpc_params,
};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use serde_json::Value;
use std::{collections::HashMap, fmt::Debug, path::Path, process::Child, time::Duration};
use tn_types::{get_available_tcp_port, keccak256, Address};
use tokio::runtime::Builder;
use tracing::{error, info};

use super::common::{kill_child, ProcessGuard};

/// One unit of TEL (10^18) measured in wei.
const WEI_PER_TEL: u128 = 1_000_000_000_000_000_000;

fn send_and_confirm(
    node: &str,
    node_test: &str,
    key: &str,
    to_account: Address,
    nonce: u128,
) -> eyre::Result<()> {
    let basefee_address = address!("0x9999999999999999999999999999999999999999");
    let current = get_balance(node_test, &to_account.to_string(), 1)?;
    let current_basefee = get_balance(node_test, &basefee_address.to_string(), 1)?;
    let amount = 10 * WEI_PER_TEL; // 10 TEL
    let expected = current + amount;
    send_tel(node, key, to_account, amount, 250, 21000, nonce)?;

    // sleep
    std::thread::sleep(Duration::from_millis(1000));
    info!(target: "restart-test", "calling get_positive_balance_with_retry...");

    // get positive bal and kill child2 if error
    let bal = get_balance_above_with_retry(node_test, &to_account.to_string(), expected - 1)?;

    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }
    let bal =
        get_balance_above_with_retry(node_test, &basefee_address.to_string(), current_basefee)?;
    let expected_bal = if nonce > 0 { current_basefee + (current_basefee / (nonce)) } else { 0 };
    if nonce > 0 && bal < expected_bal {
        error!(target: "restart-test", ?bal, ?expected_bal, "basefee error!");
        return Err(Report::msg("Expected a basefee increment!".to_string()));
    }
    Ok(())
}

/// Run the first part tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests1(
    client_urls: &[String; 4],
    child2: &mut Child,
    bin: &'static CargoRun,
    temp_path: &Path,
    rpc_port2: u16,
    delay_secs: u64,
    test: &str,
) -> eyre::Result<Child> {
    network_advancing(client_urls).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to advance network in restart_tests1");
    })?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    info!(target: "restart-test", "testing blocks same first time in restart_tests1");
    test_blocks_same(client_urls)?;
    // Try once more then fail test.
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to send and confirm in restart_tests1");
    })?;

    info!(target: "restart-test", "killing child2...");
    kill_child(child2);
    info!(target: "restart-test", "child2 dead :D sleeping...");
    std::thread::sleep(Duration::from_secs(delay_secs));

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 5).is_ok() {
        error!(target: "restart-test", "tests1: get_balancer worked for shutdown validator - returning error!");
        return Err(Report::msg("Validator not down!".to_string()));
    }

    info!(target: "restart-test", "restarting child2...");
    // Restart
    let mut child2 = start_validator(2, bin, temp_path, rpc_port2, test, 2);
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "failed to get positive balance with retry in restart_tests1");
        })?;
    if 10 * WEI_PER_TEL != bal {
        error!(target: "restart-test", "tests1 after restart: 10 * WEI_PER_TEL != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_TEL)));
    }
    // Try once more then fail test.
    send_and_confirm(&client_urls[0], &client_urls[2], &key, to_account, 1).inspect_err(|e| {
        error!(target: "restart-test", ?e, "send and confirm nonce 1 failed - killing child2...");
        kill_child(&mut child2);
    })?;

    info!(target: "restart-test", "testing blocks same again in restart_tests1");

    test_blocks_same(client_urls).inspect_err(|e| {
        error!(target: "restart-test", ?e, "test blocks same failed - killing child2...");
        kill_child(&mut child2);
    })?;
    Ok(child2)
}

/// Run the first part tests, broken up like this to allow more robust node shutdown.
/// This versoin is intended to leave the restarted node in a lagged (not caught up state)
/// in order to exercise more restart code.
fn run_restart_tests_lagged1(
    client_urls: &[String; 4],
    child2: &mut Child,
    bin: &'static CargoRun,
    temp_path: &Path,
    rpc_port2: u16,
    delay_secs: u64,
    test: &str,
) -> eyre::Result<Child> {
    network_advancing(client_urls).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to advance network in restart_tests1");
    })?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    info!(target: "restart-test", "testing blocks same first time in restart_tests1");
    test_blocks_same(client_urls)?;
    // Try once more then fail test.
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to send and confirm in restart_tests1");
    })?;

    info!(target: "restart-test", "killing child2...");
    kill_child(child2);
    info!(target: "restart-test", "child2 dead :D sleeping...");
    std::thread::sleep(Duration::from_secs(delay_secs));

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 5).is_ok() {
        error!(target: "restart-test", "tests1: get_balancer worked for shutdown validator - returning error!");
        return Err(Report::msg("Validator not down!".to_string()));
    }

    let current = get_balance(&client_urls[0], &to_account.to_string(), 1)?;
    let amount = 10 * WEI_PER_TEL; // 10 TEL
    let expected = current + amount;
    send_tel(&client_urls[0], &key, to_account, amount, 250, 21000, 1)?;
    std::thread::sleep(Duration::from_millis(5000));

    info!(target: "restart-test", "restarting child2...");
    // Restart
    let mut child2 = start_validator(2, bin, temp_path, rpc_port2, test, 2);
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "failed to get positive balance with retry in restart_tests1");
        })?;
    if 10 * WEI_PER_TEL != bal {
        error!(target: "restart-test", "tests1 after restart: 10 * WEI_PER_TEL != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_TEL)));
    }
    let bal = get_balance_above_with_retry(&client_urls[2], &to_account.to_string(), expected - 1)?;
    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }

    info!(target: "restart-test", "testing blocks same again in restart_tests1");

    Ok(child2)
}

/// Run the second part of tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests2(client_urls: &[String; 4]) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    std::thread::sleep(Duration::from_secs(2));

    // After full restart, some nodes may still be catching up consensus/execution.
    // Find the highest reported EL block and wait for all nodes to reach it
    // before comparing block hashes.
    let mut max_block = 0u64;
    for url in client_urls.iter() {
        if let Ok(n) = get_block_number(url) {
            max_block = max_block.max(n);
        }
    }
    assert!(max_block > 0, "max block is 0");
    for url in client_urls.iter() {
        wait_for_block(url, max_block)?;
    }

    test_blocks_same(client_urls)?;
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    for (i, uri) in client_urls.iter().enumerate().take(4) {
        let bal = get_positive_balance_with_retry(uri, &to_account.to_string())?;
        if 20 * WEI_PER_TEL != bal {
            return Err(Report::msg(format!(
                "Expected a balance of {} got {bal} for node {i}!",
                20 * WEI_PER_TEL
            )));
        }
    }
    let number_start = get_block_number(&client_urls[3])?;
    if let Err(e) = send_and_confirm(&client_urls[0], &client_urls[3], &key, to_account, 2) {
        let number_0 = get_block_number(&client_urls[0])?;
        let number_1 = get_block_number(&client_urls[1])?;
        let number_2 = get_block_number(&client_urls[2])?;
        let number_3 = get_block_number(&client_urls[3])?;
        if number_start == number_3 {
            return Err(eyre::eyre!(
                "Stuck on block {number_3}, other nodes {number_0}, {number_1}, {number_2}, error: {e}"
            ));
        }
        return Err(e);
    }
    test_blocks_same(client_urls)?;
    Ok(())
}

fn network_advancing(client_urls: &[String; 4]) -> eyre::Result<()> {
    // Wait for all nodes to respond to RPC.
    // With skip-empty-execution, blocks are only produced when transactions
    // exist or an epoch closes, so we cannot rely on block_number advancing
    // during idle periods. Actual block production is verified later by
    // send_and_confirm().
    let mut i = 0;
    loop {
        let mut all_responsive = true;
        for url in client_urls {
            if get_block_number(url).is_err() {
                all_responsive = false;
                break;
            }
        }
        if all_responsive {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
        i += 1;
        if i > 45 {
            return Err(eyre::eyre!("Network not responding within 45 seconds!"));
        }
    }
}

fn do_restarts(delay: u64, lagged: bool, test: &str) -> eyre::Result<()> {
    info!(target: "restart-test", "do_restarts, delay: {delay}");
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .expect("failed to config");
    }
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0, 0, 0, 0];
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, &bin, &temp_path, rpc_port, test, 0));
    }

    // Take child2 out of guard for restart testing
    let mut child2 = guard.take(2).expect("missing child 2");

    info!(target: "restart-test", "Running restart tests 1");
    let res1 = if lagged {
        run_restart_tests_lagged1(
            &client_urls,
            &mut child2,
            &bin,
            &temp_path,
            rpc_ports[2],
            delay,
            test,
        )
    } else {
        run_restart_tests1(&client_urls, &mut child2, &bin, &temp_path, rpc_ports[2], delay, test)
    };
    info!(target: "restart-test", "Ran restart tests 1: {res1:?}");
    let is_ok = res1.is_ok();
    let assert_str = match res1 {
        Ok(mut child2_restarted) => {
            kill_child(&mut child2_restarted);
            "".to_string()
        }
        Err(err) => {
            tracing::error!(target: "restart-test", "Got error: {err}");
            err.to_string()
        }
    };

    // Kill all remaining children (child2 slot is already None from take)
    guard.kill_all();

    // Make sure we shutdown nodes even if an error in first testing.
    assert!(is_ok, "Phase 1 failed: {assert_str}. Check logs in test_logs/{test}/");
    let to_account = address_from_word("testing");
    assert!(get_balance(&client_urls[0], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[1], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[2], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[3], &to_account.to_string(), 5).is_err());

    info!(target: "restart-test", "all nodes shutdown...restarting network");
    // Restart network
    for i in 0..4 {
        guard.replace(i, start_validator(i, &bin, &temp_path, rpc_ports[i], test, 3));
    }

    info!(target: "restart-test", "Running restart tests 2");
    let res2 = run_restart_tests2(&client_urls);
    info!(target: "restart-test", "Ran restart tests 2: {res2:?}");

    // guard.drop() handles final cleanup
    res2
}

/// Test a restart case with a short delay, the stopped node should rejoin consensus.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restartstt() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    do_restarts(2, false, "restarts")
}

/// Wait for `node` to reach at least `target_block`, polling every second for up to 60 seconds.
fn wait_for_block(node: &str, target_block: u64) -> eyre::Result<()> {
    for _ in 0..60 {
        if let Ok(n) = get_block_number(node) {
            if n >= target_block {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(eyre::eyre!("Node {node} did not reach block {target_block} within 60 seconds"))
}

/// Run some test to make sure an observer is participating in the network.
fn run_observer_tests(client_urls: &[String; 4], obs_url: &str) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    test_blocks_same(client_urls)?;
    // Send to observer, validator confirms.
    send_and_confirm(obs_url, &client_urls[2], &key, to_account, 0)?;

    // After the first transaction, EL block 1 exists on all validators. Wait for the observer
    // to sync to that block before reading state from it — the observer starts at genesis and
    // may not have caught up yet, which would cause the basefee baseline to be 0.
    // Use client_urls[2] since send_and_confirm already verified that validator's state,
    // guaranteeing it has executed the TX block. client_urls[0] may lag slightly.
    let target_block = get_block_number(&client_urls[2])?;
    wait_for_block(obs_url, target_block)?;

    // Send to observer, validator confirms- second time.
    send_and_confirm(obs_url, &client_urls[3], &key, to_account, 1)?;

    // Send to a validator, observer sees transfer.
    send_and_confirm(&client_urls[0], obs_url, &key, to_account, 2)?;

    test_blocks_same(client_urls)?;
    Ok(())
}

/// Test an observer node can submit txns.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_observer() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "restart-test", "do_restarts_observer");
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .expect("failed to config");
    }
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        client_urls[i].push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, &bin, &temp_path, rpc_port, "observer", 0));
    }
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for child!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    guard.push(start_observer(4, &bin, &temp_path, obs_rpc_port, "observer", 0));

    // Guard cleanup handles all process shutdown on drop
    run_observer_tests(&client_urls, &obs_url)
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_delayed() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    do_restarts(70, false, "restarts_delayed")
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.  Lag the restarted validator.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_lagged_delayed() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    do_restarts(70, true, "restarts_lagged_delayed")
}

/// Start a process running a validator node.
fn start_validator(
    instance: usize,
    bin: &'static CargoRun,
    base_dir: &Path,
    rpc_port: u16,
    test: &str,
    run: u32,
) -> Child {
    let data_dir = base_dir.join(format!("validator-{}", instance + 1));
    let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
    // IPC: use temp-dir-based path to avoid cross-test conflicts
    let ipc_path = base_dir.join(format!("validator-{}.ipc", instance + 1));
    let mut command = bin.command();

    command
        .env("TN_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        .arg("--ws")
        .arg("--ws.port")
        .arg(format!("{ws_port}"))
        .arg("--ipcpath")
        .arg(ipc_path.to_string_lossy().as_ref())
        .arg("--node-name")
        .arg(format!("{test}-node{instance}"));

    setup_log_dir(&mut command, instance, test, run);

    command.spawn().expect("failed to execute")
}

/// Start a process running an observer node.
fn start_observer(
    instance: usize,
    bin: &'static CargoRun,
    base_dir: &Path,
    rpc_port: u16,
    test: &str,
    run: u32,
) -> Child {
    let data_dir = base_dir.join("observer");
    let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
    // IPC: use temp-dir-based path to avoid cross-test conflicts
    let ipc_path = base_dir.join("observer.ipc");
    let mut command = bin.command();
    command
        .env("TN_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--observer")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        .arg("--ws")
        .arg("--ws.port")
        .arg(format!("{ws_port}"))
        .arg("--ipcpath")
        .arg(ipc_path.to_string_lossy().as_ref())
        .arg("--node-name")
        .arg(format!("{test}-node{instance}"));

    setup_log_dir(&mut command, instance, test, run);

    command.spawn().expect("failed to execute")
}

fn test_blocks_same(client_urls: &[String; 4]) -> eyre::Result<()> {
    info!(target: "restart-test", "calling get_block for {:?}", &client_urls[0]);
    let block0 = get_block(&client_urls[0], None)?;
    let number = u64::from_str_radix(&block0["number"].as_str().unwrap_or("0x100_000")[2..], 16)?;
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[1]);
    let block = get_block(&client_urls[1], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg(format!(
            "Blocks between validators not the same (node 0 and 1)! block {number}: {:?} - block: {:?}",
            block0["hash"], block["hash"]
        )));
    }
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[2]);
    let block = get_block(&client_urls[2], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg(format!(
            "Blocks between validators not the same (node 0 and 2)! block {number}: {:?} - block: {:?}",
            block0["hash"], block["hash"]
        )));
    }
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[3]);
    let block = get_block(&client_urls[3], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg(format!(
            "Blocks between validators not the same (node 0 and 3)! block {number}: {:?} - block: {:?}",
            block0["hash"], block["hash"]
        )));
    }
    info!(target: "restart-test", "all rpcs returned same block hash");
    Ok(())
}

/// Send an RPC call to node to get the latest balance for address.
/// Return a tuple of the TEL and remainder (any value left after dividing by 1_e18).
/// Note, balance is in wei and must fit in an u128.
fn get_balance(node: &str, address: &str, retries: usize) -> eyre::Result<u128> {
    let res_str: String =
        call_rpc(node, "eth_getBalance", rpc_params!(address, "latest"), retries, address)?;
    info!(target: "restart-test", "get_balance for {node}: parsing string {res_str}");
    let tel = u128::from_str_radix(&res_str[2..], 16)?;
    info!(target: "restart-test", "get_balance for {node}: {tel:?}");
    Ok(tel)
}

/// Retry up to 10 times to retrieve an account balance > 0.
fn get_positive_balance_with_retry(node: &str, address: &str) -> eyre::Result<u128> {
    get_balance_above_with_retry(node, address, 0)
}

/// Retry up to 45 times to retrieve an account balance > above.
fn get_balance_above_with_retry(node: &str, address: &str, above: u128) -> eyre::Result<u128> {
    let mut bal = get_balance(node, address, 5)?;
    let mut i = 0;
    while i < 45 && bal <= above {
        std::thread::sleep(Duration::from_millis(1200));
        i += 1;
        bal = get_balance(node, address, 5)?;
    }
    if i == 45 && bal <= above {
        error!(target:"restart-test", "get_balance_above_with_retry i == 30 - returning error!!");
        Err(Report::msg(format!("Failed to get a balance {bal} for {address} above {above}")))
    } else {
        Ok(bal)
    }
}

/// If key starts with 0x then return it otherwise generate the key from the key string.
fn get_key(key: &str) -> String {
    if key.starts_with("0x") {
        key.to_string()
    } else {
        let (_, _, key) = account_from_word(key);
        key
    }
}

fn get_block(node: &str, block_number: Option<u64>) -> eyre::Result<HashMap<String, Value>> {
    let debug_params = if let Some(block_number) = block_number {
        format!("0x{block_number:x}")
    } else {
        "latest".to_string()
    };

    let params = rpc_params!(&debug_params, true);
    // Deserialize as Option to handle null responses from syncing/restarted nodes
    // that haven't caught up to the requested block yet.
    let mut result: Option<HashMap<String, Value>> =
        call_rpc(node, "eth_getBlockByNumber", params.clone(), 10, &debug_params)?;
    let mut retries = 0;
    while result.is_none() && retries < 30 {
        std::thread::sleep(Duration::from_secs(1));
        result = call_rpc(node, "eth_getBlockByNumber", params.clone(), 3, &debug_params)?;
        retries += 1;
    }
    result.ok_or_else(|| {
        eyre::eyre!("eth_getBlockByNumber returned null after retries for {debug_params} on {node}")
    })
}

fn get_block_number(node: &str) -> eyre::Result<u64> {
    let block = get_block(node, None)?;
    Ok(u64::from_str_radix(&block["number"].as_str().unwrap_or("0x100_000")[2..], 16)?)
}

fn get_latest_consensus_header(node: &str) -> eyre::Result<HashMap<String, Value>> {
    call_rpc(node, "tn_latestConsensusHeader", rpc_params![], 10, "tn_latestConsensusHeader")
}

fn get_latest_consensus_header_number(node: &str) -> eyre::Result<u64> {
    let header = get_latest_consensus_header(node)?;
    let value = header
        .get("number")
        .ok_or_else(|| Report::msg("tn_latestConsensusHeader missing `number` field"))?;

    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| Report::msg("tn_latestConsensusHeader number is not u64-compatible")),
        Value::String(s) if s.starts_with("0x") => {
            u64::from_str_radix(s.trim_start_matches("0x"), 16)
                .map_err(|e| Report::msg(format!("failed to parse consensus number hex: {e}")))
        }
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|e| Report::msg(format!("failed to parse consensus number: {e}"))),
        _ => Err(Report::msg("tn_latestConsensusHeader number has unexpected type")),
    }
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
fn address_from_word(key_word: &str) -> Address {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (_, public_key) = secp.generate_keypair(&mut rand);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    Address::from_slice(&hash[12..])
}

/// Return the (account, public key, secret key) generated from key_word.
fn account_from_word(key_word: &str) -> (String, String, String) {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand);
    let keypair = Keypair::from_secret_key(&secp, &secret_key);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    let address = Address::from_slice(&hash[12..]);
    let pubkey = keypair.public_key().serialize();
    let secret = keypair.secret_bytes();
    (address.to_string(), const_hex::encode(pubkey), const_hex::encode(secret))
}

/// Create, sign and submit a TXN to transfer TEL from key's account to to_account.
fn send_tel(
    node: &str,
    key: &str,
    to_account: Address,
    amount: u128,
    gas_price: u128,
    gas: u128,
    nonce: u128,
) -> eyre::Result<()> {
    let mut to_addr = [0_u8; 20];
    //const_hex::decode_to_slice(to_account, &mut to_addr[..])?;
    to_addr.copy_from_slice(to_account.as_slice());
    let (from_account, _, _) = decode_key(key)?;
    let new_transaction = LegacyTransaction {
        chain: 0x7e1,
        nonce,
        to: Some(to_addr),
        value: amount,
        gas_price,
        gas,
        data: vec![/* contract code or other data */],
    };
    let decoded = const_hex::decode(key)?;
    let secret_key = SecretKey::from_byte_array(decoded.as_slice().try_into()?)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let transaction_bytes = new_transaction.sign(&ecdsa);
    let res_str: String = call_rpc(
        node,
        "eth_sendRawTransaction",
        rpc_params!(const_hex::encode(&transaction_bytes)),
        1,
        transaction_bytes,
    )?;
    info!(target: "restart-test", "Submitted TEL transfer from {from_account} to {to_account} for {amount}: {res_str}");
    Ok(())
}

/// Decode a secret key into it's public key and account.
/// Returns a tuple of (account, public_key, public_key_long) as hex encoded strings.
fn decode_key(key: &str) -> eyre::Result<(String, String, String)> {
    match const_hex::decode(key) {
        Ok(key) => {
            let key_array: [u8; 32] = key
                .as_slice()
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| Report::msg(e.to_string()))?;
            match SecretKey::from_byte_array(key_array) {
                Ok(secret_key) => {
                    let secp = Secp256k1::new();
                    let keypair = Keypair::from_secret_key(&secp, &secret_key);
                    let public_key = keypair.public_key();
                    // strip out the first byte because that should be the
                    // SECP256K1_TAG_PUBKEY_UNCOMPRESSED tag returned by
                    // libsecp's uncompressed pubkey serialization
                    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
                    let address = Address::from_slice(&hash[12..]);
                    Ok((
                        address.to_string(),
                        const_hex::encode(public_key.serialize()),
                        const_hex::encode(public_key.serialize_uncompressed()),
                    ))
                }
                Err(err) => Err(Report::msg(err.to_string())),
            }
        }
        Err(err) => Err(Report::msg(err.to_string())),
    }
}

/// Make an RPC call to node with command and params.
/// Wraps any Eyre otherwise returns the result as a String.
/// This is for testing and will try up to retries times at one second intervals to send the
/// request.
fn call_rpc<R, Params, DebugParams>(
    node: &str,
    command: &str,
    params: Params,
    retries: usize,
    debug_params: DebugParams,
) -> eyre::Result<R>
where
    R: DeserializeOwned + Debug,
    Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone + Debug,
    DebugParams: Debug,
{
    // jsonrpsee is async AND tokio specific so give it a runtime (and can't use a crate like
    // pollster)...
    let runtime = Builder::new_current_thread().enable_io().enable_time().build()?;

    let resp = runtime.block_on(async move {
        let client = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(10))
            .build(node)
            .expect("couldn't build rpc client");
        let mut resp = client.request(command, params.clone()).await;
        let mut i = 0;
        while i < retries && resp.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let client = HttpClientBuilder::default()
                .request_timeout(Duration::from_secs(10))
                .build(node)
                .expect("couldn't build rpc client");
            resp = client.request(command, params.clone()).await;
            i += 1;
        }
        resp.inspect_err(|error| {
            error!(target: "restart-tests", ?error, ?command, ?node, ?debug_params, "rpc call failed");
        })
    });

    Ok(resp?)
}

/// Test that an observer started AFTER validators have already produced blocks
/// can catch up to the current chain height.
/// This tests the state-sync catch-up path which is critical for observer reliability.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_observer_late_join_catchup() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "restart-test", "test_observer_late_join_catchup");
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .expect("failed to config");
    }
    let bin = e2e_tests::get_telcoin_network_binary();

    // Start 4 validators WITHOUT the observer
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        client_urls[i].push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, &bin, &temp_path, rpc_port, "late_join", 0));
    }

    // Wait for validators to produce blocks
    network_advancing(&client_urls)?;

    // Send transactions to advance chain further
    let key = get_key("test-source");
    let to_account = address_from_word("late-join-target");
    send_and_confirm(&client_urls[0], &client_urls[1], &key, to_account, 0)?;
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 1)?;

    // Record current validator consensus height
    let validator_consensus_height = get_latest_consensus_header_number(&client_urls[0])?;
    info!(target: "restart-test", ?validator_consensus_height, "validators advanced, now starting observer");

    // NOW start the observer (it must catch up from behind)
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    guard.push(start_observer(4, &bin, &temp_path, obs_rpc_port, "late_join", 0));

    // Observer must catch up to at least the validator consensus height we recorded
    let mut retries = 0;
    let max_retries = 120; // 120 seconds max
    let caught_up = loop {
        if let Ok(obs_consensus_height) = get_latest_consensus_header_number(&obs_url) {
            if obs_consensus_height >= validator_consensus_height {
                info!(target: "restart-test", ?obs_consensus_height, ?validator_consensus_height, "observer caught up");
                break true;
            }
            info!(target: "restart-test", ?obs_consensus_height, ?validator_consensus_height, retries, "observer still catching up");
        }
        retries += 1;
        if retries >= max_retries {
            break false;
        }
        std::thread::sleep(Duration::from_secs(1));
    };

    // Guard cleanup handles all process shutdown on drop
    assert!(
        caught_up,
        "Observer did not catch up within {max_retries}s. Check logs in test_logs/late_join/"
    );
    Ok(())
}

/// Test that an observer can recover after being paused (simulating network partition).
/// Uses SIGSTOP/SIGCONT to pause the observer while validators continue producing blocks,
/// then verifies the observer catches back up.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_observer_reconnect_after_pause() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "restart-test", "test_observer_reconnect_after_pause");
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .expect("failed to config");
    }
    let bin = e2e_tests::get_telcoin_network_binary();

    // Start 4 validators + 1 observer
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        client_urls[i].push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, &bin, &temp_path, rpc_port, "reconnect", 0));
    }
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    guard.push(start_observer(4, &bin, &temp_path, obs_rpc_port, "reconnect", 0));

    // Wait for network to advance and observer to be in sync
    network_advancing(&client_urls)?;
    std::thread::sleep(Duration::from_secs(5));

    let initial_obs_consensus_height = get_latest_consensus_header_number(&obs_url)?;
    info!(target: "restart-test", ?initial_obs_consensus_height, "observer synced, pausing it");

    // SIGSTOP the observer (simulate network partition / process freeze)
    let obs_pid = Pid::from_raw(guard.get_mut(4).expect("observer child").id() as i32);
    signal::kill(obs_pid, Signal::SIGSTOP)?;

    // Let validators advance for 15 seconds while observer is paused
    std::thread::sleep(Duration::from_secs(15));
    let validator_consensus_height_during_pause =
        get_latest_consensus_header_number(&client_urls[0])?;
    info!(target: "restart-test", ?validator_consensus_height_during_pause, "validators advanced while observer paused");
    assert!(
        validator_consensus_height_during_pause > initial_obs_consensus_height + 5,
        "Validators should have advanced significantly"
    );

    // SIGCONT the observer (resume)
    signal::kill(obs_pid, Signal::SIGCONT)?;
    info!(target: "restart-test", "observer resumed, waiting for catchup");

    // Observer must catch up
    let mut retries = 0;
    let max_retries = 60;
    let caught_up = loop {
        if let Ok(obs_consensus_height) = get_latest_consensus_header_number(&obs_url) {
            if obs_consensus_height >= validator_consensus_height_during_pause {
                info!(target: "restart-test", ?obs_consensus_height, ?validator_consensus_height_during_pause, "observer recovered");
                break true;
            }
        }
        retries += 1;
        if retries >= max_retries {
            break false;
        }
        std::thread::sleep(Duration::from_secs(1));
    };

    // Guard cleanup handles all process shutdown on drop
    assert!(caught_up, "Observer did not recover within {max_retries}s after SIGCONT. Check logs in test_logs/reconnect/");
    Ok(())
}
