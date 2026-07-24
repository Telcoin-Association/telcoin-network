//! E2e tests for nodes crashing and rejoining and active network.

use super::common::{kill_child, ProcessGuard};
use crate::common::{
    address_from_word, advertise_worker_rpc, get_balance, get_balance_above_with_retry, get_block,
    get_block_number, get_key, get_latest_consensus_header_number, get_node_info, get_node_mode,
    get_positive_balance_with_retry, network_advancing, send_and_confirm, send_tel, start_observer,
    start_validator, WEI_PER_TEL,
};
use e2e_tests::{config_local_testnet, config_local_testnet_with_gc_depth, TestBinary};
use eyre::Report;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{
    path::Path,
    process::Child,
    time::{Duration, Instant},
};
use tn_test_utils::wait_until_blocking;
use tn_types::{get_available_tcp_port, NodeMode};
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
    // Delayed restarts (downtime >= the demotion floor) rejoin via the follow/catch-up path, so
    // the node passes through the transient `CvvInactive` mode. Observe it now, before
    // `get_positive_balance_with_retry` below blocks until the node is fully caught up (by which
    // point it is `CvvActive` again). The short-downtime restart never crosses the GC window and
    // never demotes, so it is gated out here.
    if delay_secs >= RESTART_TEST_DOWNTIME_SECS {
        assert_observed_cvv_inactive(&client_urls[2]).inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "restarted node never entered CvvInactive during catch-up in restart_tests1");
        })?;
    }
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
    // Lagged delayed restart: the downtime is past the GC window, so the restarted validator
    // rejoins via the follow/catch-up path and passes through the transient `CvvInactive` mode
    // before catching up. Observe it now, before the balance-catch-up waits below (which only
    // return once the node is `CvvActive` again). This path is only ever exercised with the
    // delayed downtime, but gate on the floor for parity with `run_restart_tests1`.
    if delay_secs >= RESTART_TEST_DOWNTIME_SECS {
        assert_observed_cvv_inactive(&client_urls[2]).inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "restarted node never entered CvvInactive during catch-up in restart_tests_lagged1");
        })?;
    }
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

/// Wait out a restarted node's downtime, proceeding only once BOTH the wall-clock floor has
/// elapsed AND the live validators have provably advanced enough consensus rounds to push the
/// killed node outside the GC window.
///
/// A validator that misses more than `gc_depth - 10` consensus rounds during downtime is demoted
/// to `CvvInactive` and must rejoin via the follow/catch-up path rather than live consensus (see
/// the primary network handler's `outside_gc_window` check). With the restart tests' lowered
/// `gc_depth` (`RESTART_TEST_GC_DEPTH`) that threshold is `RESTART_TEST_GC_DEPTH - 10` = 15 DAG
/// rounds. The `min_secs` floor alone does not guarantee it: at the nominal ~500ms/round cadence a
/// 25s downtime spans ~50 rounds (safe), but on a loaded runner where idle rounds stretch past
/// ~1.67s/round a 25s wait covers fewer than 15 rounds, the killed node never demotes, and
/// `assert_observed_cvv_inactive` flakes. To close that, the delayed tests also gate on peer
/// progress: each consensus header wraps one committed sub-dag whose leader round strictly exceeds
/// its predecessor's, so a peer header-number delta of D guarantees the live DAG round climbed by
/// at least D past the kill point. Requiring `(RESTART_TEST_GC_DEPTH - 10) + 3` = 18 headers thus
/// guarantees the killed node is strictly more than 15 rounds behind (demotion fires) no matter how
/// slow CI is. Consensus headers advance on idle rounds (empty sub-dags still commit; that is why
/// this tracks the consensus chain, not the EVM block height), so the count keeps climbing
/// throughout the downtime. The time `cap` (`min_secs * 2 + 30`) stays the fail-safe that surfaces
/// a genuinely wedged network downstream instead of hanging here. The short rejoin test
/// (`min_secs = 2`) uses `min_round_gap = 1` and intentionally stays inside the GC window (live
/// rejoin, not demotion). Node index 2 is the killed one; 0/1/3 stay live.
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
    // Only the delayed restart tests (min_secs >= RESTART_TEST_DOWNTIME_SECS) must guarantee the
    // killed node crosses the `gc_depth - 10` demotion threshold; the short rejoin test
    // (min_secs = 2) intentionally stays inside the GC window, so a single header of progress is
    // enough to prove the network is still live. Each consensus header wraps one committed sub-dag
    // whose leader round strictly exceeds its predecessor's, so a header-number delta of D
    // guarantees the live peers' DAG round climbed by at least D past the kill point; requiring
    // `(gc_depth - 10) + 3` headers thus keeps the killed node > 15 rounds behind regardless of CI
    // cadence, with a 3-round margin absorbing kill-instant skew.
    let min_round_gap: u64 = if min_secs >= RESTART_TEST_DOWNTIME_SECS {
        (RESTART_TEST_GC_DEPTH as u64).saturating_sub(10).saturating_add(3)
    } else {
        1
    };

    loop {
        let elapsed = start.elapsed();
        let advanced = start_height
            .zip(peer_height())
            .is_some_and(|(s, now)| now >= s.saturating_add(min_round_gap));
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

/// Garbage-collection depth (DAG rounds) used for the delayed restart tests. Lowering it from the
/// protocol default (`MAX_GC_DEPTH = 50`) shrinks the `CvvInactive` demotion threshold
/// (`gc_depth - 10` rounds) so a killed CVV crosses it after a shorter deliberate downtime while
/// still taking the follow/catch-up path under test. Kept well above the short-rejoin test's
/// downtime (`do_restarts(2, ..)` ~= a few rounds) so that test still exercises live rejoin, not
/// demotion. `Parameters::validate` enforces only `gc_depth <= MAX_GC_DEPTH`, so this passes.
const RESTART_TEST_GC_DEPTH: u32 = 25;

/// Deliberate downtime floor (seconds) for the delayed restart tests, paired with
/// [`RESTART_TEST_GC_DEPTH`]. At the e2e cadence (~500ms/round) the demotion threshold is
/// `RESTART_TEST_GC_DEPTH - 10 = 15` rounds (~7.5s); this floor clears it with margin for slower
/// CI while cutting ~35s per test vs the previous fixed 60s floor.
const RESTART_TEST_DOWNTIME_SECS: u64 = 25;

/// Number of times [`assert_observed_cvv_inactive`] samples a restarted node's mode (~250ms
/// apart), bounding the catch-up observation window to ~30s.
const CVV_INACTIVE_POLL_ATTEMPTS: usize = 120;

/// Assert a restarted validator was observed in [`NodeMode::CvvInactive`] at least once while
/// catching up.
///
/// Only the *delayed* restart tests use this. A validator offline long enough to fall outside the
/// garbage-collection window (see [`wait_for_downtime`]) cannot rejoin live consensus and instead
/// takes the follow/catch-up path, which demotes it to `CvvInactive` until it has synced past the
/// GC window and then promotes it back to `CvvActive`. That `CvvInactive` state is therefore
/// transient, so we poll the node's mode rapidly right after restart and short-circuit on the
/// first sighting via [`Iterator::any`]. The window is deliberately generous because the exact
/// demotion instant races both RPC startup and network sync; a poll that fails while the RPC is
/// briefly down (mid-restart) counts as "not yet observed" rather than fatal. The check is
/// non-vacuous: if the node never enters `CvvInactive` (the follow path never ran) every sample
/// misses and this returns an error instead of silently passing.
fn assert_observed_cvv_inactive(node: &str) -> eyre::Result<()> {
    let observed_inactive = (0..CVV_INACTIVE_POLL_ATTEMPTS).any(|attempt| {
        // Sample immediately on the first attempt (the demotion may already be visible), then pace
        // subsequent samples ~4x/sec across the catch-up window.
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(250));
        }
        matches!(get_node_mode(node), Ok(NodeMode::CvvInactive))
    });
    if observed_inactive {
        info!(target: "restart-test", "observed restarted node {node} in CvvInactive during catch-up");
        Ok(())
    } else {
        Err(Report::msg(format!(
            "restarted node {node} was never observed in CvvInactive during catch-up window"
        )))
    }
}

fn do_restarts(delay: u64, lagged: bool, test: &str) -> eyre::Result<()> {
    info!(target: "restart-test", "do_restarts, delay: {delay}");
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    {
        // Restart tests use a lowered garbage-collection depth so a killed CVV crosses the
        // `CvvInactive` demotion threshold (`gc_depth - 10` DAG rounds) after a shorter downtime,
        // exercising the follow/catch-up path without the full default-`gc_depth` wait. See
        // `wait_for_downtime` for the floor that pairs with this.
        config_local_testnet_with_gc_depth(
            &temp_path,
            Some("restart_test".to_string()),
            None,
            Some(RESTART_TEST_GC_DEPTH),
        )
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
    do_restarts(RESTART_TEST_DOWNTIME_SECS, false, "restarts_delayed")
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.  Lag the restarted validator.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_lagged_delayed() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    do_restarts(RESTART_TEST_DOWNTIME_SECS, true, "restarts_lagged_delayed")
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
