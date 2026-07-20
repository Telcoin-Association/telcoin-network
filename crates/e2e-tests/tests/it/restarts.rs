//! E2e tests for nodes crashing and rejoining and active network.

use super::common::{kill_child, ProcessGuard};
use crate::common::{
    address_from_word, advertise_worker_rpc, call_rpc, fetch_verified_epoch_record, get_balance,
    get_balance_above_with_retry, get_block, get_block_number, get_key,
    get_latest_consensus_header_number, get_node_info, get_positive_balance_with_retry,
    network_advancing, send_and_confirm, send_tel, start_observer, start_validator,
    start_validator_with_env, wait_for_epoch_at_least, wait_for_mid_epoch, wait_for_rpc,
    EPOCH_DURATION, WEI_PER_TEL,
};
use alloy::providers::ProviderBuilder;
use e2e_tests::{config_local_testnet, TestBinary};
use eyre::Report;
use jsonrpsee::rpc_params;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{
    io::BufRead as _,
    path::Path,
    process::Child,
    time::{Duration, Instant},
};
use tn_test_utils::wait_until_blocking;
use tn_types::get_available_tcp_port;
use tracing::{error, info};

/// Run the first part tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests1(
    client_urls: &[String; 4],
    child2: &mut Child,
    bin: &'static TestBinary,
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
    info!(target: "restart-test", "child2 dead :D waiting out downtime...");
    wait_for_downtime(client_urls, delay_secs)?;

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 0).is_ok() {
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
    bin: &'static TestBinary,
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
    info!(target: "restart-test", "child2 dead :D waiting out downtime...");
    wait_for_downtime(client_urls, delay_secs)?;

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 0).is_ok() {
        error!(target: "restart-test", "tests1: get_balancer worked for shutdown validator - returning error!");
        return Err(Report::msg("Validator not down!".to_string()));
    }

    let current = get_balance(&client_urls[0], &to_account.to_string(), 1)?;
    let amount = 10 * WEI_PER_TEL; // 10 TEL
    let expected = current + amount;
    send_tel(&client_urls[0], &key, to_account, amount, 250, 21000, 1)?;
    // Wait for the transfer to settle on the live network before restarting the lagged
    // node, so it must catch the balance up via sync. Event-driven (polls a live peer
    // until the balance lands) instead of a fixed 5s sleep.
    get_balance_above_with_retry(&client_urls[0], &to_account.to_string(), expected - 1)?;

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

/// Wait out a restarted node's downtime, then proceed only once the live validators are
/// still advancing the consensus chain.
///
/// A validator that misses more than `parameters.gc_depth - 10` (50 - 10 = 40) consensus
/// rounds while offline is pushed outside the GC window (see the primary network handler's
/// `outside_gc_window` check) and must rejoin via the follow/catch-up path rather than
/// live consensus. The `min_secs` floor is the real guarantee: with the e2e genesis's
/// `max_header_delay = 500ms` an idle round is bounded to ~500ms (idle rounds are paced by
/// the header delays, not `max_batch_delay`) and the heavy restart tests run serially
/// (nextest `e2e` group, max-threads = 1) so nothing else steals cores, so 60s reliably
/// clears the ~40-round (~20s) threshold with margin while still cutting ~10s per test vs
/// the old fixed 70s sleep. The peer-advancement check is only a stall-guard against the network
/// wedging (which would make the downtime meaningless), not a substitute for the floor. Node
/// index 2 is the killed one; 0/1/3 stay live.
fn wait_for_downtime(client_urls: &[String; 4], min_secs: u64) -> eyre::Result<()> {
    // Nodes 0/1/3 stay live (index 2 is the killed one).
    let peer_height = || {
        [&client_urls[0], &client_urls[1], &client_urls[3]]
            .into_iter()
            .filter_map(|url| get_latest_consensus_header_number(url).ok())
            .max()
    };

    let start_height = peer_height();
    let start = Instant::now();
    let floor = Duration::from_secs(min_secs);
    // Fail-safe cap so a genuinely stalled network surfaces downstream instead of hanging here.
    let cap = Duration::from_secs(min_secs * 2 + 30);

    loop {
        let elapsed = start.elapsed();
        let advanced = start_height.zip(peer_height()).is_some_and(|(s, now)| now > s);
        if elapsed >= floor && advanced {
            return Ok(());
        }
        if elapsed >= cap {
            info!(target: "restart-test", ?elapsed, "wait_for_downtime hit fail-safe cap; proceeding");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
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
    // These nodes are all shut down, so a dead connection is expected: use retries = 0 so a
    // known-down node fails immediately instead of burning ~5s of retries per check.
    assert!(get_balance(&client_urls[0], &to_account.to_string(), 0).is_err());
    assert!(get_balance(&client_urls[1], &to_account.to_string(), 0).is_err());
    assert!(get_balance(&client_urls[2], &to_account.to_string(), 0).is_err());
    assert!(get_balance(&client_urls[3], &to_account.to_string(), 0).is_err());

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
        // The observer forwards accepted txns to the committee's advertised RPC
        // endpoints; without this the forward has no targets and txns are dropped.
        advertise_worker_rpc(&temp_path, i, rpc_port)?;
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
    do_restarts(60, false, "restarts_delayed")
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.  Lag the restarted validator.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_lagged_delayed() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    do_restarts(60, true, "restarts_lagged_delayed")
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

    // Observer must catch up to at least the validator consensus height we recorded.
    // Guard cleanup handles all process shutdown on drop; on timeout the `?` surfaces the
    // same effective failure ("observer did not catch up") the old assert produced.
    wait_until_blocking(
        Duration::from_secs(120),
        "observer caught up to validator (test_logs/late_join/)",
        || {
            Ok(get_latest_consensus_header_number(&obs_url)
                .map(|h| h >= validator_consensus_height)
                .unwrap_or(false))
        },
    )?;
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

    // Observer must catch up. Guard cleanup handles all process shutdown on drop; on timeout
    // the `?` surfaces the same effective failure ("observer did not recover") the old assert
    // produced.
    wait_until_blocking(
        Duration::from_secs(60),
        "observer recovered after SIGCONT (test_logs/reconnect/)",
        || {
            Ok(get_latest_consensus_header_number(&obs_url)
                .map(|h| h >= validator_consensus_height_during_pause)
                .unwrap_or(false))
        },
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Fleet-wide crash at the epoch boundary (replay-and-close recovery)
// ---------------------------------------------------------------------------------------------

/// Log-dir name (under `test_logs/`) for [`test_restarts_fleet_crash_at_epoch_boundary`].
const BOUNDARY_CRASH_TEST: &str = "restart_boundary_replay";

/// Env var (read inline by `EpochManager::wait_for_epoch_boundary`) carrying the boundary-crash
/// test hook's epoch threshold: a node started with this set self-exits with status 0 the moment
/// it detects the boundary output of the first epoch at or past the value — after the subscriber
/// persisted that output to the consensus DB, before it is forwarded to the engine. Threshold `1`
/// skips epoch 0 (its record handling is special-cased with an unsigned dummy) and fires at
/// epoch 1's close.
const EXIT_AFTER_BOUNDARY_PERSIST_ENV: &str = "TN_TEST_EXIT_AFTER_BOUNDARY_PERSIST";

/// The hook's warn line (grepped in run-0 logs): its presence pins a node's exit to the hook
/// rather than some unrelated death that happened to report status 0.
const HOOK_WARN_LINE: &str =
    "TN_TEST_EXIT_AFTER_BOUNDARY_PERSIST set - exiting before forwarding epoch boundary output";

/// Total budget for the restarted fleet to recover the killed epoch and cross two boundaries.
///
/// Recovery is dominated by fixed costs (process startup, consensus replay, network re-init,
/// peer discovery, and the catch-up epoch closes) rather than the epoch cadence, so this is an
/// absolute bound instead of a multiple of [`EPOCH_DURATION`].
const FLEET_RECOVERY_TIMEOUT_SECS: u64 = 300;

/// Regression test for the devnet incident (epoch 691) where all validators crashed at an epoch
/// boundary AFTER the epoch-closing consensus output was persisted to the consensus DB but
/// BEFORE the engine executed it.
///
/// On restart every node replays the persisted boundary output and closes the epoch through the
/// replay-and-close arm of `EpochManager::run_epoch` (the `replay.take_epoch_close_hash()`
/// branch). That arm previously never wrote the closed epoch's `EpochRecord`, and the next
/// epoch's `open_epoch_pack` hard-requires the previous record — with EVERY node recordless at
/// once there was no peer to fetch it from, so the whole fleet wedged on "Missing previous epoch
/// record for epoch N after waiting". The fix writes the record in the replay-and-close and
/// leftover-drain arms and re-publishes cert-less records so the epoch certificate still forms
/// retroactively.
///
/// Producing the crash window deterministically: every run-0 node is spawned with
/// [`EXIT_AFTER_BOUNDARY_PERSIST_ENV`]` = 1`, a runtime hook in
/// `EpochManager::wait_for_epoch_boundary` that self-exits the process the moment it detects
/// the boundary output of an epoch at or past that threshold — after the subscriber persisted
/// the output to the consensus DB, before it is forwarded to the engine. All four nodes
/// therefore go down inside the persisted-but-unexecuted window (window A — the epoch-691
/// incident shape) with no external-SIGKILL timing race. The residual post-execution sub-window
/// (closing block executed, crash before the record write, nothing left to replay) is NOT
/// exercised here; it is tracked separately and relies on the `open_epoch_pack` peer fetch.
///
/// Asserted recovery, with NO pre-existing recovered peer:
/// (a)/(d) every node enters and moves past the next epoch (reaches `killed_epoch + 2`, proving
///         the fleet formed consensus again and crossed a fresh boundary),
/// (b)/(c) every node serves a certified, verifying `EpochRecord` for the killed epoch (and all
///         earlier epochs) and has executed each record's final block,
///         plus a post-recovery transfer confirms end-to-end liveness,
/// at least one restarted node logged the record-publish evidence ("publishing epoch record"),
/// and no node ever logged the fatal "Missing previous epoch record" wedge. Both log scans are
/// fail-closed: a missing or empty per-node log file is an error, never a silent pass.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
async fn test_restarts_fleet_crash_at_epoch_boundary() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "restart-test", "test_restarts_fleet_crash_at_epoch_boundary");

    let tmp_guard = tempfile::TempDir::with_prefix("boundary_crash").expect("tempdir is okay");
    let temp_path = tmp_guard.path();
    // Short epochs so boundaries arrive within the test budget; passphrase must match the
    // TN_BLS_PASSPHRASE that `start_validator` exports.
    e2e_tests::config_local_testnet_with_epoch_duration(
        temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls: [String; 4] = std::array::from_fn(|_| String::new());
    let mut rpc_ports = [0u16; 4];
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port =
            get_available_tcp_port("127.0.0.1").expect("ephemeral rpc port for validator");
        rpc_ports[i] = rpc_port;
        *url = format!("http://127.0.0.1:{rpc_port}");
        // Run-0 nodes carry the boundary-persist exit hook (threshold 1: survive epoch 0's
        // special-cased close, self-exit at epoch 1's boundary). Run-1 restarts must NOT set
        // it — they have to survive their own epoch closes to prove recovery.
        guard.push(start_validator_with_env(
            i,
            bin,
            temp_path,
            rpc_port,
            BOUNDARY_CRASH_TEST,
            0,
            &[(EXIT_AFTER_BOUNDARY_PERSIST_ENV, "1")],
        ));
    }

    network_advancing(&client_urls)?;
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    wait_for_rpc(&provider).await?;

    // Confirm the fleet survived epoch 0 (the hook's threshold skips its boundary) and snapshot
    // the doomed epoch from mid-epoch, while every RPC is still up: its id names the killed
    // epoch and its duration sizes the self-exit deadline below.
    wait_for_epoch_at_least(&provider, 1).await?;
    let snap = wait_for_mid_epoch(&provider, &client_urls[0]).await?;
    let killed_epoch = snap.epoch_id;
    info!(
        target: "restart-test",
        killed_epoch, "waiting for the boundary-persist hook to self-exit the fleet"
    );

    // Deterministic crash: each node self-exits (status 0) the moment it detects the epoch's
    // boundary output — persisted to the consensus DB, never forwarded to the engine. Wait for
    // all four to exit on their own. A node still alive at the deadline, or one that exited
    // non-zero, means the hook did NOT produce the crash window: SIGKILL any stragglers and
    // fail — recovery assertions against the wrong crash shape would prove nothing.
    let exit_deadline = Instant::now() + Duration::from_secs(snap.epoch_duration * 2 + 10);
    let mut exited = [false; 4];
    while !exited.iter().all(|done| *done) {
        for (i, done) in exited.iter_mut().enumerate() {
            if !*done {
                let child = guard.get_mut(i).expect("validator child in guard");
                if let Some(status) = child.try_wait()? {
                    if !status.success() {
                        return Err(Report::msg(format!(
                            "node{i} exited with {status} instead of the hook's exit(0) at the \
                             epoch {killed_epoch} boundary. Check \
                             test_logs/{BOUNDARY_CRASH_TEST}/"
                        )));
                    }
                    *done = true;
                }
            }
        }
        if Instant::now() >= exit_deadline {
            let stragglers: Vec<usize> =
                exited.iter().enumerate().filter(|(_, done)| !**done).map(|(i, _)| i).collect();
            for i in stragglers.iter() {
                if let Some(child) = guard.get_mut(*i) {
                    let _ = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL);
                    let _ = child.wait();
                }
            }
            return Err(Report::msg(format!(
                "nodes {stragglers:?} still alive at the deadline: the \
                 {EXIT_AFTER_BOUNDARY_PERSIST_ENV} hook did not fire for epoch {killed_epoch}'s \
                 boundary (SIGKILLed them). Check test_logs/{BOUNDARY_CRASH_TEST}/"
            )));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    info!(target: "restart-test", killed_epoch, "all validators self-exited at the epoch boundary");

    // Exit alone is not proof the hook fired: require each node's run-0 log to carry the hook's
    // warn line, pinning every exit to the persisted-but-unexecuted window.
    for i in 0..4 {
        if find_in_node_run_logs(BOUNDARY_CRASH_TEST, i, 0, HOOK_WARN_LINE)?.is_none() {
            return Err(Report::msg(format!(
                "node{i} exited without logging the boundary-persist hook line in its run-0 \
                 log - it did not go down in the persisted-but-unexecuted window. Check \
                 test_logs/{BOUNDARY_CRASH_TEST}/"
            )));
        }
    }

    // The whole fleet must be down: recovery cannot lean on a peer that kept running.
    let probe = address_from_word("testing").to_string();
    for url in client_urls.iter() {
        assert!(
            get_balance(url, &probe, 0).is_err(),
            "validator RPC still up after SIGKILL: {url}"
        );
    }

    // Restart every node with its existing datadir (and rpc port, so client_urls stay valid).
    info!(target: "restart-test", "restarting the fleet with existing datadirs");
    for (i, rpc_port) in rpc_ports.into_iter().enumerate() {
        guard.replace(i, start_validator(i, bin, temp_path, rpc_port, BOUNDARY_CRASH_TEST, 1));
    }

    // (a) + (d): every node must reach `killed_epoch + 2`. Getting there requires each node to
    // recover the killed epoch's close, open the next epoch (impossible without the previous
    // epoch's record — the incident's wedge), re-form consensus, and cross a fresh boundary.
    // One shared deadline: the nodes recover in parallel, so the first wait absorbs most of it.
    let target_epoch = killed_epoch + 2;
    let recovery_deadline = Instant::now() + Duration::from_secs(FLEET_RECOVERY_TIMEOUT_SECS);
    for url in client_urls.iter() {
        let mut last_seen = None;
        loop {
            if let Ok(epoch) = call_rpc::<u32, _, _>(
                url,
                "tn_getCurrentEpoch",
                rpc_params![],
                0,
                "tn_getCurrentEpoch",
            ) {
                last_seen = Some(epoch);
                if epoch >= target_epoch {
                    break;
                }
            }
            if Instant::now() >= recovery_deadline {
                return Err(Report::msg(format!(
                    "{url} stuck at epoch {last_seen:?} (< {target_epoch}) after the fleet-wide \
                     boundary crash — the epoch-record wedge? Check test_logs/{BOUNDARY_CRASH_TEST}/"
                )));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    info!(target: "restart-test", target_epoch, "all nodes recovered past the killed epoch");

    // (b) + (c): every node serves a certified, verifying record for the killed epoch (and all
    // epochs before it) and has executed each record's final block. The killed epoch's
    // certificate can only exist if the fix's cert-less record re-publish let the committee vote
    // retroactively — no node closed that epoch through the live boundary vote window.
    // Certificate formation is a fixed async quorum-voting cost, so floor the per-record
    // deadline at 60s rather than scaling it with EPOCH_DURATION.
    for url in client_urls.iter() {
        for epoch in 0..=killed_epoch {
            let epoch_rec =
                fetch_verified_epoch_record(url, epoch, (EPOCH_DURATION * 6).max(60)).await?;
            get_block(url, Some(epoch_rec.final_state.number)).map_err(|e| {
                Report::msg(format!(
                    "final block {} for epoch {epoch} missing on {url}: {e}",
                    epoch_rec.final_state.number
                ))
            })?;
        }
    }

    // End-to-end liveness after recovery: a transfer submitted to one node confirms on another.
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    send_and_confirm(&client_urls[0], &client_urls[1], &key, to_account, 0)?;

    // Non-vacuity: at least one restarted node must show the record-publish evidence in its
    // run-1 logs, proving the record write and republish actually ran post-restart.
    assert_record_publish_evidence(BOUNDARY_CRASH_TEST)?;

    // No restarted node may ever have hit the fatal missing-record wedge.
    assert_no_missing_record_wedge(BOUNDARY_CRASH_TEST)?;
    Ok(())
}

/// Assert at least one restarted node's (run 1) logs carry the record-publish evidence.
///
/// "publishing epoch record" (see `manage_epoch_votes`) fires when `write_epoch_record`
/// publishes a record on the epoch-record watch — the visible side effect of the recovery
/// arms' record write. Requiring it on at least one node keeps the recovery assertions
/// non-vacuous: they cannot pass while no node ever wrote and republished a record.
fn assert_record_publish_evidence(test: &str) -> eyre::Result<()> {
    for i in 0..4 {
        if find_in_node_run_logs(test, i, 1, "publishing epoch record")?.is_some() {
            return Ok(());
        }
    }
    Err(Report::msg(format!(
        "no restarted node logged \"publishing epoch record\" in its run-1 logs - the recovery \
         record write left no evidence. Check test_logs/{test}/"
    )))
}

/// Assert no restarted node (run 1) logged the fatal "Missing previous epoch record" wedge.
///
/// The epoch-progression assertions already fail when a node wedges; this pins the failure to
/// the exact incident signature so a regression is unmistakable in CI output. Only this
/// invocation's run-1 files are scanned (deterministic names, truncated on creation), so stale
/// logs from other runs cannot leak in. Fail-closed: a missing or empty log file is an error —
/// an absence-of-wedge conclusion drawn from logs that were never written proves nothing.
fn assert_no_missing_record_wedge(test: &str) -> eyre::Result<()> {
    for i in 0..4 {
        if let Some(hit) = find_in_node_run_logs(test, i, 1, "Missing previous epoch record")? {
            return Err(Report::msg(format!(
                "fleet recovery hit the missing-epoch-record wedge: {hit}"
            )));
        }
    }
    Ok(())
}

/// Scan a node's per-run log pair for the first line containing `needle`.
///
/// Returns `Ok(Some("file: line"))` on the first hit, `Ok(None)` when neither file contains it.
/// Fail-closed: both `node<node>-run<run>.log` and its `.stderr.log` sibling must exist and be
/// non-empty (`setup_log_dir` creates them at spawn, and a booted node always writes to both —
/// tracing to stdout, the weak-KDF keyfile warning to stderr), so a scan can never silently
/// "pass" against logs that were never written.
fn find_in_node_run_logs(
    test: &str,
    node: usize,
    run: u32,
    needle: &str,
) -> eyre::Result<Option<String>> {
    let log_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("test_logs").join(test);
    let mut found = None;
    for name in [format!("node{node}-run{run}.log"), format!("node{node}-run{run}.stderr.log")] {
        let path = log_dir.join(&name);
        let file = std::fs::File::open(&path).map_err(|e| {
            Report::msg(format!("expected node log missing: {}: {e}", path.display()))
        })?;
        if file.metadata()?.len() == 0 {
            return Err(Report::msg(format!(
                "expected node log is empty: {} - nothing was captured to scan",
                path.display()
            )));
        }
        if found.is_none() {
            for line in std::io::BufReader::new(file).lines() {
                let Ok(line) = line else { break };
                if line.contains(needle) {
                    found = Some(format!("{name}: {line}"));
                    break;
                }
            }
        }
    }
    Ok(found)
}
