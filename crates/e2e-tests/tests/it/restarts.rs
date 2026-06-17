//! E2e tests for nodes crashing and rejoining and active network.

use super::common::{kill_child, ProcessGuard};
use crate::common::{
    address_from_word, get_balance, get_balance_above_with_retry, get_block, get_block_number,
    get_key, get_latest_consensus_header_number, get_node_info, get_positive_balance_with_retry,
    network_advancing, send_and_confirm, send_tel, start_observer, start_validator, WEI_PER_TEL,
};
use e2e_tests::config_local_testnet;
use escargot::CargoRun;
use eyre::Report;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{path::Path, process::Child, time::Duration};
use tn_types::get_available_tcp_port;
use tracing::{error, info};

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

    let info = get_node_info(&client_urls[0]).unwrap();
    assert_eq!(
        info.get("execution_address").unwrap(),
        "0x1111111111111111111111111111111111111111"
    );
    assert_eq!(info.get("chain_id").unwrap(), &serde_json::Value::Number(911329.into()));
    let info = get_node_info(&client_urls[1]).unwrap();
    assert_eq!(
        info.get("execution_address").unwrap(),
        "0x2222222222222222222222222222222222222222"
    );
    assert_eq!(info.get("chain_id").unwrap(), &serde_json::Value::Number(911329.into()));
    let info = get_node_info(&client_urls[2]).unwrap();
    assert_eq!(
        info.get("execution_address").unwrap(),
        "0x3333333333333333333333333333333333333333"
    );
    assert_eq!(info.get("chain_id").unwrap(), &serde_json::Value::Number(911329.into()));
    let info = get_node_info(&client_urls[3]).unwrap();
    assert_eq!(
        info.get("execution_address").unwrap(),
        "0x4444444444444444444444444444444444444444"
    );
    assert_eq!(info.get("chain_id").unwrap(), &serde_json::Value::Number(911329.into()));
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

    // Wait for the observer to sync the second transaction's block before reading
    // its baseline balance. client_urls[3] confirmed the second tx, so use its
    // block height as the sync target.
    let target_block = get_block_number(&client_urls[3])?;
    wait_for_block(obs_url, target_block)?;

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

    // node1 confirmed the first transfer (EL block 1). The next call reads its baseline
    // balance from node2, so wait for node2 to catch up to that block first — otherwise a
    // stale baseline of 0 makes `expected` 10 TEL too low while the confirm poll observes
    // the full 20 TEL, failing the assertion. Mirrors the barrier in run_observer_tests.
    let target_block = get_block_number(&client_urls[1])?;
    wait_for_block(&client_urls[2], target_block)?;

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
