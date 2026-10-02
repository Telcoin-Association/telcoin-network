//! E2e tests for nodes crashing and rejoining and active network.

use super::common::{kill_child, ProcessGuard};
use crate::common::{
    address_from_word, advertise_worker_rpc, assert_epoch_records_verify,
    assert_nodes_agree_on_commit_times, block_commit_time, call_rpc, current_epoch,
    fetch_verified_epoch_record, get_balance, get_balance_above_with_retry, get_block,
    get_block_number, get_key, get_latest_consensus_header_number, get_node_info, get_node_mode,
    get_positive_balance_with_retry, network_advancing, pin_fork_epochs, scrape_metric_value,
    scrape_metrics, send_and_confirm, send_tel, start_observer, start_validator,
    start_validator_with_args, wait_for_epoch_at_least, wait_for_mid_epoch, wait_for_rpc,
    walk_block_commit_times, BlockCommitTime, EVM_TIMESTAMP_CLAMPED_SERIES, WEI_PER_TEL,
};
use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::{
    config_local_testnet, config_local_testnet_with_gc_depth,
    config_local_testnet_with_gc_depth_and_epoch_duration, NodeEndpoints, TestBinary,
};
use eyre::{Report, WrapErr as _};
use jsonrpsee::rpc_params;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{
    cell::RefCell,
    ops::RangeInclusive,
    path::Path,
    process::Child,
    time::{Duration, Instant},
};
use tn_test_utils::wait_until_blocking;
use tn_types::{get_available_tcp_port, Epoch, NodeMode};
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
    let metrics_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no metrics port available for restarted validator"))?;
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let mut child2 = start_validator_with_args(
        2,
        bin,
        temp_path,
        rpc_port2,
        test,
        2,
        &["--metrics", &metrics_addr],
    );
    let [_, _, restarted_node, _] = client_urls;
    wait_for_restarted_rpc(&mut child2, restarted_node, test).inspect_err(|e| {
        kill_child(&mut child2);
        error!(target: "restart-test", ?e, "restarted node did not become RPC-ready");
    })?;
    // The new process's counter retains evidence of the follow/catch-up path even when the
    // transient CvvInactive mode ends before the first RPC poll. Short restarts need not sync.
    if delay_secs >= RESTART_TEST_DOWNTIME_SECS {
        wait_for_restart_catch_up(restarted_node, &metrics_addr).inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "restarted node did not complete state sync in restart_tests1");
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
    wait_for_node_mode(&client_urls[2], NodeMode::CvvActive).inspect_err(|e| {
        error!(target: "restart-test", ?e, "restarted validator did not rejoin active consensus");
        kill_child(&mut child2);
    })?;
    Ok(child2)
}

/// Wait for the restarted validator's RPC endpoint without spending the balance retry budget.
///
/// Use the same startup bound as `network_advancing`, and fail immediately if the child exits.
/// RPC readiness does not imply catch-up; the caller still checks balances and canonical blocks.
pub(crate) fn wait_for_restarted_rpc(
    child: &mut Child,
    node: &str,
    test: &str,
) -> eyre::Result<()> {
    let child = RefCell::new(child);
    let description = format!(
        "restarted validator RPC at {node} (logs: test_logs/{test}/node2-run2.log and \
         node2-run2.stderr.log)"
    );
    wait_until_blocking(Duration::from_secs(45), &description, || {
        child.try_borrow_mut()?.try_wait()?.map_or(Ok(()), |status| {
            eyre::bail!("{description}: child exited before RPC was ready: {status}")
        })?;
        let response: eyre::Result<String> =
            call_rpc(node, "eth_blockNumber", rpc_params![], 0, "restart readiness");
        Ok(response.is_ok())
    })
}

/// Run the first part tests, broken up like this to allow more robust node shutdown.
/// The node misses a transfer while offline, then must apply it during catch-up after restarting.
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

    // Verify the old balance before shutdown. Catch-up may apply the missed transfer before the
    // restarted node serves its first balance query.
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(child2);
            error!(target: "restart-test", ?e, "failed to get balance before shutdown");
        })?;
    if 10 * WEI_PER_TEL != bal {
        kill_child(child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_TEL)));
    }

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
    let metrics_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no metrics port available for restarted validator"))?;
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let mut child2 = start_validator_with_args(
        2,
        bin,
        temp_path,
        rpc_port2,
        test,
        2,
        &["--metrics", &metrics_addr],
    );
    // Require state sync and a return to active consensus, even if catch-up finished before RPC
    // became available. Gate on the delayed downtime for parity with run_restart_tests1.
    if delay_secs >= RESTART_TEST_DOWNTIME_SECS {
        wait_for_restart_catch_up(&client_urls[2], &metrics_addr).inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "restarted node did not complete state sync in restart_tests_lagged1");
        })?;
    }
    let bal = get_balance_above_with_retry(&client_urls[2], &to_account.to_string(), expected - 1)?;
    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }

    wait_for_node_mode(&client_urls[2], NodeMode::CvvActive).inspect_err(|e| {
        error!(target: "restart-test", ?e, "lagged validator did not rejoin active consensus");
        kill_child(&mut child2);
    })?;
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
/// `wait_for_restart_catch_up` times out. To close that, the delayed tests also gate on peer
/// progress: each consensus header wraps one committed sub-dag whose leader round strictly exceeds
/// its predecessor's, so a peer header-number delta of D guarantees the live DAG round climbed by
/// at least D past the kill point. Requiring `(RESTART_TEST_GC_DEPTH - 10) + 3` = 18 headers thus
/// guarantees the killed node is strictly more than 15 rounds behind (demotion fires) no matter how
/// slow CI is. Consensus headers advance on idle rounds (empty sub-dags still commit; that is why
/// this tracks the consensus chain, not the EVM block height), so the count keeps climbing
/// throughout the downtime. The time `cap` (`min_secs * 2 + 30`) fails the test if the network
/// does not make the required progress. The short rejoin test
/// (`min_secs = 2`) uses `min_round_gap = 1` and intentionally stays inside the GC window (live
/// rejoin, not demotion). Node index 2 is the killed one; 0/1/3 stay live.
pub(crate) fn wait_for_downtime(client_urls: &[String; 4], min_secs: u64) -> eyre::Result<()> {
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
    // Fail if a stalled network cannot establish the required gap before restart.
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
            return Err(Report::msg(format!(
                "downtime did not establish a consensus header gap of {min_round_gap}: \
                 start={start_height:?}, latest={:?}, elapsed={elapsed:?}",
                peer_height()
            )));
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

/// Wait for a restarted CVV to apply state-sync headers and return to active consensus.
///
/// The metrics endpoint belongs to the fresh process, so its cumulative applied-header counter
/// proves this restart exercised the follow/catch-up path. Unlike the transient CvvInactive mode,
/// that evidence remains available after catch-up finishes, including before the first RPC poll.
pub(crate) fn wait_for_restart_catch_up(node: &str, metrics_addr: &str) -> eyre::Result<()> {
    wait_until_blocking(
        Duration::from_secs(30),
        &format!("restarted node {node} to apply state-sync headers and return to CvvActive"),
        || {
            Ok(scrape_metrics(metrics_addr).is_ok_and(|metrics| {
                get_node_mode(node).is_ok_and(|mode| restart_catch_up_complete(mode, &metrics))
            }))
        },
    )
}

/// Require both durable evidence of applied state-sync headers and current CVV participation.
fn restart_catch_up_complete(mode: NodeMode, metrics: &str) -> bool {
    mode == NodeMode::CvvActive
        && metrics.lines().any(|line| {
            let mut fields = line.split_whitespace();
            fields.next() == Some("tn_state_sync_headers_fetched_total")
                && fields
                    .next()
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(|count| count.is_finite() && count > 0.0)
        })
}

/// Catch-up may finish before the first mode poll; its cumulative counter must still prove it ran.
#[test]
fn test_restart_catch_up_complete_before_first_poll() {
    let metrics = "# TYPE tn_state_sync_headers_fetched_total counter\n\
                   tn_state_sync_headers_fetched_total 18\n\
                   tn_node_mode{mode=\"cvv_inactive\"} 0\n\
                   tn_node_mode{mode=\"cvv_active\"} 1\n";
    assert!(restart_catch_up_complete(NodeMode::CvvActive, metrics));
}

/// An active node alone cannot prove the restart exercised the follow/catch-up path.
#[test]
fn test_restart_catch_up_requires_applied_headers() {
    [
        "",
        "# TYPE tn_state_sync_headers_fetched_total counter\n",
        "tn_state_sync_headers_fetched_total 0\n",
        "tn_state_sync_headers_fetched_total -1\n",
        "tn_state_sync_headers_fetched_total NaN\n",
        "tn_state_sync_headers_fetched_total inf\n",
        "tn_state_sync_headers_fetched_total invalid\n",
        "tn_state_sync_headers_fetched_total_created 18\n",
        "other_tn_state_sync_headers_fetched_total 18\n",
    ]
    .into_iter()
    .for_each(|metrics| {
        assert!(!restart_catch_up_complete(NodeMode::CvvActive, metrics), "{metrics}");
    });
}

/// Applied headers do not suffice until the restarted validator rejoins active consensus.
#[test]
fn test_restart_catch_up_requires_active_cvv() {
    [NodeMode::CvvInactive, NodeMode::Observer].into_iter().for_each(|mode| {
        assert!(!restart_catch_up_complete(mode, "tn_state_sync_headers_fetched_total 18\n"));
    });
}

/// Wait for a stable membership-derived role, with the URL and expected role in timeout errors.
pub(crate) fn wait_for_node_mode(node: &str, expected: NodeMode) -> eyre::Result<()> {
    wait_until_blocking(
        Duration::from_secs(30),
        &format!("node {node} entered {expected:?}"),
        || Ok(get_node_mode(node).is_ok_and(|mode| mode == expected)),
    )
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
pub(crate) fn wait_for_block(node: &str, target_block: u64) -> eyre::Result<()> {
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
pub(crate) fn run_observer_tests(client_urls: &[String; 4], obs_url: &str) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    // The observer may still be syncing startup epoch records after the validators are ready.
    wait_until_blocking(Duration::from_secs(45), "observer RPC ready", || {
        // A single block-number request avoids the block-fetch helper's nested retries.
        Ok(call_rpc::<String, _, _>(obs_url, "eth_blockNumber", rpc_params![], 0, "readiness")
            .is_ok())
    })?;
    client_urls.iter().try_for_each(|url| wait_for_node_mode(url, NodeMode::CvvActive))?;
    wait_for_node_mode(obs_url, NodeMode::Observer)?;

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    test_blocks_same(client_urls)?;
    // Establish live consensus and observer execution before testing forwarding. RPC can be
    // available while the observer is still starting its worker and synchronizing with peers.
    send_and_confirm(&client_urls[0], obs_url, &key, to_account, 0)
        .wrap_err("validator transfer did not execute on the observer")?;

    // Send to observer, validator confirms. Repeat with the next nonce below.
    send_and_confirm(obs_url, &client_urls[3], &key, to_account, 1)
        .wrap_err("first observer-forwarded transfer did not execute on the validator")?;

    // Wait for the observer to sync the second transaction's block before reading
    // its baseline balance. client_urls[3] confirmed the second tx, so use its
    // block height as the sync target.
    let target_block = get_block_number(&client_urls[3])?;
    wait_for_block(obs_url, target_block)?;

    send_and_confirm(obs_url, &client_urls[2], &key, to_account, 2)
        .wrap_err("second observer-forwarded transfer did not execute on the validator")?;
    let target_block = get_block_number(&client_urls[2])?;
    wait_for_block(obs_url, target_block)?;

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
        // endpoints; without this each seal is refused with NotValidator and the txns
        // stay pending in the observer's pool until an endpoint is discoverable.
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

/// The epoch the sub-second timestamp fork activates at in
/// [`test_restarts_across_subsecond_fork`]; earlier epochs commit in whole seconds.
const SUBSECOND_RESTART_FORK_EPOCH: Epoch = 3;

/// Epoch duration (seconds) for [`test_restarts_across_subsecond_fork`], short enough that the
/// [`RESTART_TEST_DOWNTIME_SECS`] downtime spans several whole epochs.
const SUBSECOND_RESTART_EPOCH_SECS: u32 = 5;

/// How long a node gets to serve each certified epoch record. Certificates take a fixed
/// quorum-voting time that does not shrink with 5 s epochs, so this keeps the 60 s floor the
/// epoch tests use.
const SUBSECOND_RESTART_RECORD_SECS: u64 = 60;

/// Restart validators on their own datadirs while the sub-second timestamp fork activates and
/// after it has.
///
/// The fork is pinned at [`SUBSECOND_RESTART_FORK_EPOCH`] (F = 3), with the seed-signature fork
/// active from genesis because the sub-second gate requires it. Four validators run 5 s epochs at
/// the delayed restart tests' lowered gc depth, each serving `--metrics`.
///
/// Phase 1 stops validator-3 in the middle of epoch F - 1, checks from its datadir that it never
/// opened epoch F before the process exited, and keeps the node down until a live peer has closed
/// epoch F and [`RESTART_TEST_DOWNTIME_SECS`] have passed. The node misses the whole first
/// post-fork epoch and falls far enough behind to be demoted. Restarted on its own pre-fork
/// history, it has to catch up through state sync and return to active consensus. After that, every
/// node serves a certified record for every closed epoch, with the final block that record
/// names. On validator-3 that block carries validator-1's commit time, in whole seconds before F
/// and flagged sub-second from F on, and the post-fork commit times carry milliseconds. Every block
/// validator-3 serves passes the commit-time walk and agrees with validator-1, its engine never
/// clamped an EVM timestamp, and a transfer sent through validator-1 executes on it.
///
/// Phase 2 stops all four and restarts them on their own post-fork history. The cohort agrees on
/// its head, returns to active consensus, and closes another epoch whose final block carries the
/// same sub-second commit time on every node. Every node then serves a certified record for every
/// closed epoch, including one whose certificate the stop may have interrupted. No node clamps an
/// EVM timestamp since its restart, and another transfer executes.
///
/// F = 3 because the first epochs are short: the test can only stop the node mid-epoch once all
/// four validators serve RPC, and by then epochs 0 and 1 may already be over. Epoch 2 is the first
/// it can catch pre-fork. Five-second epochs put several boundaries inside the 25 s downtime, so a
/// whole post-fork epoch opens and closes while the node is down.
#[test]
#[ignore = "only run independently from all other it tests"]
fn test_restarts_across_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // both forced rather than inherited: the claim is a restart across a known sub-second epoch,
    // and the gate (`tn_types::forks::subsecond_timestamp_active`) conjoins the seed fork
    // fail-closed, so a dormant seed fork would keep every epoch on whole seconds
    pin_fork_epochs(None, Some(0), None, Some(SUBSECOND_RESTART_FORK_EPOCH));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    runtime.block_on(restarts_across_subsecond_fork())
}

/// The body of [`test_restarts_across_subsecond_fork`].
async fn restarts_across_subsecond_fork() -> eyre::Result<()> {
    const F: Epoch = SUBSECOND_RESTART_FORK_EPOCH;
    // validator-1 is the reference; validator-3 (index 2) is the node that restarts alone
    const REFERENCE: usize = 0;
    const RESTARTED: usize = 2;
    // short on purpose: node IPC socket paths are built under the temp dir
    let test = "ss_restart";
    let temp_dir = tempfile::TempDir::with_prefix(test)?;
    let temp_path = temp_dir.path();
    config_local_testnet_with_gc_depth_and_epoch_duration(
        temp_path,
        Some("restart_test".to_string()),
        None,
        RESTART_TEST_GC_DEPTH,
        SUBSECOND_RESTART_EPOCH_SECS,
    )?;
    let bin = e2e_tests::get_telcoin_network_binary();
    let free_port =
        || get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no free local port"));

    let mut guard = ProcessGuard::empty();
    let mut rpc_ports = [0u16; 4];
    let mut metrics_addrs: [String; 4] = Default::default();
    for (instance, (rpc_port, metrics)) in
        rpc_ports.iter_mut().zip(metrics_addrs.iter_mut()).enumerate()
    {
        *rpc_port = free_port()?;
        *metrics = format!("127.0.0.1:{}", free_port()?);
        guard.push(start_validator_with_args(
            instance,
            bin,
            temp_path,
            *rpc_port,
            test,
            0,
            &["--metrics", metrics.as_str()],
        ));
    }
    let rpc_urls: [String; 4] = rpc_ports.map(|port| format!("http://127.0.0.1:{port}"));
    let providers = rpc_urls
        .iter()
        .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?)))
        .collect::<eyre::Result<Vec<_>>>()?;
    futures::future::try_join_all(providers.iter().map(wait_for_rpc)).await?;
    let endpoints: Vec<NodeEndpoints> = rpc_urls
        .iter()
        .map(|url| NodeEndpoints {
            http_url: url.clone(),
            ws_url: String::new(),
            ipc_path: String::new(),
        })
        .collect();
    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    // phase 1: validator-3 stops in the last pre-fork epoch and stays down through the first
    // post-fork one
    wait_for_epoch_at_least(&providers[REFERENCE], F - 1).await?;
    let mid = wait_for_mid_epoch(&providers[REFERENCE], &rpc_urls[REFERENCE]).await?.epoch_id;
    eyre::ensure!(
        mid == F - 1,
        "the first mid-epoch window the test caught was in epoch {mid}, not {}: the run cannot \
         stop validator-3 before the fork activates at epoch {F}",
        F - 1
    );
    let mut stopped =
        guard.take(RESTARTED).ok_or_else(|| eyre::eyre!("validator-3 is not running"))?;
    kill_child(&mut stopped);
    eyre::ensure!(
        providers[RESTARTED].get_chain_id().await.is_err(),
        "validator-3 still answers RPC after being stopped"
    );
    // the process has exited, so its datadir shows how far it got. A live peer's epoch cannot
    // show it: `wait_for_mid_epoch` leaves 1-3 s of the 5 s epoch, less up to 1 s of flooring, and
    // `kill_child` polls the exit in 1.2 s steps for up to 6 s, so the network can enter F while
    // validator-3 is already shutting down without validator-3 ever running F. Only the upper
    // bound is the claim: the same flooring lets the window open milliseconds after the boundary,
    // before validator-3 may have opened F - 1 itself
    let epoch_at_kill = last_opened_epoch(&temp_path.join("validator-3"))?;
    eyre::ensure!(
        epoch_at_kill < F,
        "validator-3 opened epoch {epoch_at_kill} before it exited: the run cannot show it was \
         down when the fork activated at epoch {F}"
    );
    info!(target: "restart-test", epoch_at_kill, "validator-3 stopped before opening the fork epoch");

    // the 25 s floor and the consensus-header gap push validator-3 out of the gc window, and the
    // epoch wait makes the network close the first post-fork epoch without it
    wait_for_downtime(&rpc_urls, RESTART_TEST_DOWNTIME_SECS)?;
    let epoch_at_restart = wait_for_epoch_at_least(&providers[REFERENCE], F + 1).await?.epoch_id;
    eyre::ensure!(
        epoch_at_kill < F && F < epoch_at_restart,
        "validator-3 was down from epoch {epoch_at_kill} to {epoch_at_restart}, which does not \
         contain the whole fork epoch {F}"
    );
    info!(target: "restart-test", epoch_at_restart, "restarting validator-3 on its pre-fork history");

    metrics_addrs[RESTARTED] = format!("127.0.0.1:{}", free_port()?);
    guard.replace(
        RESTARTED,
        start_validator_with_args(
            RESTARTED,
            bin,
            temp_path,
            rpc_ports[RESTARTED],
            test,
            2,
            &["--metrics", &metrics_addrs[RESTARTED]],
        ),
    );
    let restarted =
        guard.get_mut(RESTARTED).ok_or_else(|| eyre::eyre!("validator-3 was not restarted"))?;
    wait_for_restarted_rpc(restarted, &rpc_urls[RESTARTED], test)?;
    wait_for_restart_catch_up(&rpc_urls[RESTARTED], &metrics_addrs[RESTARTED])?;
    let headers_fetched = tokio::task::block_in_place(|| {
        scrape_metric_value(&metrics_addrs[RESTARTED], "tn_state_sync_headers_fetched_total")
    })?;
    info!(target: "restart-test", headers_fetched, "validator-3 caught up through state sync");

    // every closed epoch, the ones validator-3 missed included, on every node
    let last_closed = current_epoch(&providers[REFERENCE]).await?.epoch_id - 1;
    eyre::ensure!(
        F <= last_closed,
        "epoch {last_closed} is the last closed one, so no post-fork epoch has a record yet"
    );
    assert_epoch_records_verify(&endpoints, 0..=last_closed, SUBSECOND_RESTART_RECORD_SECS).await?;
    let closing = assert_epoch_closing_commit_times(
        &[
            (&providers[REFERENCE], rpc_urls[REFERENCE].as_str()),
            (&providers[RESTARTED], rpc_urls[RESTARTED].as_str()),
        ],
        0..=last_closed,
    )
    .await?;
    // the subSecond flag comes from the node's fork gate; the milliseconds come from the
    // consensus headers it synced, so a post-fork commit time off a whole second shows they kept
    // millisecond precision
    eyre::ensure!(
        closing.iter().any(|commit| commit.sub_second && commit.timestamp_millis % 1000 != 0),
        "every post-fork epoch up to {last_closed} closed on a whole second: the commit times \
         validator-3 synced carry no milliseconds"
    );

    let mut walks = Vec::with_capacity(2);
    for index in [REFERENCE, RESTARTED] {
        let head = get_block_number(&rpc_urls[index])?;
        walks.push(walk_block_commit_times(&providers[index], &rpc_urls[index], 0..=head).await?);
    }
    assert_nodes_agree_on_commit_times(
        &walks,
        &[rpc_urls[REFERENCE].clone(), rpc_urls[RESTARTED].clone()],
    )?;
    assert_no_evm_timestamp_clamps(&metrics_addrs, &rpc_urls, test)?;
    test_blocks_same(&rpc_urls)?;
    send_and_confirm(&rpc_urls[REFERENCE], &rpc_urls[RESTARTED], &key, to_account, 0)?;

    // phase 2: the whole cohort restarts on its own post-fork history
    guard.kill_all();
    for (provider, url) in providers.iter().zip(&rpc_urls) {
        eyre::ensure!(provider.get_chain_id().await.is_err(), "{url} still answers RPC");
    }
    for (instance, (rpc_port, metrics)) in
        rpc_ports.iter().zip(metrics_addrs.iter_mut()).enumerate()
    {
        *metrics = format!("127.0.0.1:{}", free_port()?);
        guard.replace(
            instance,
            start_validator_with_args(
                instance,
                bin,
                temp_path,
                *rpc_port,
                test,
                3,
                &["--metrics", metrics.as_str()],
            ),
        );
    }
    futures::future::try_join_all(providers.iter().map(wait_for_rpc)).await?;
    let epoch_at_cohort_restart = current_epoch(&providers[REFERENCE]).await?.epoch_id;
    info!(target: "restart-test", epoch_at_cohort_restart, "restarted all four validators");
    network_advancing(&rpc_urls)?;
    test_blocks_same(&rpc_urls)?;
    for url in &rpc_urls {
        wait_for_node_mode(url, NodeMode::CvvActive)?;
    }
    // an epoch the restarted cohort closed itself, after the fork
    let closed =
        wait_for_epoch_at_least(&providers[REFERENCE], epoch_at_cohort_restart + 1).await?.epoch_id
            - 1;
    eyre::ensure!(
        F <= epoch_at_cohort_restart && epoch_at_cohort_restart <= closed,
        "the cohort restarted in epoch {epoch_at_cohort_restart} and closed epoch {closed}: not \
         a post-fork epoch closed after the restart"
    );
    // every record, not only the new one: the cohort can go down between an epoch's close and its
    // record's certificate, and must still certify that record after it restarts
    assert_epoch_records_verify(&endpoints, 0..=closed, SUBSECOND_RESTART_RECORD_SECS).await?;
    let nodes: Vec<_> = providers.iter().zip(rpc_urls.iter().map(String::as_str)).collect();
    assert_epoch_closing_commit_times(&nodes, closed..=closed).await?;
    assert_no_evm_timestamp_clamps(&metrics_addrs, &rpc_urls, test)?;
    send_and_confirm(&rpc_urls[RESTARTED], &rpc_urls[3], &key, to_account, 1)?;
    info!(
        target: "restart-test",
        epoch_at_kill,
        epoch_at_restart,
        epoch_at_cohort_restart,
        closed,
        "restarts across the sub-second fork passed"
    );

    guard.kill_all();
    Ok(())
}

/// The highest epoch whose consensus pack the node under `datadir` opened.
///
/// A node creates `consensus-db/epochs/epoch-<N>` when it starts epoch N (`open_epoch_pack` in
/// `run_epoch`), before it replays or configures any consensus for that epoch, so on a stopped
/// node the highest such directory is the last epoch it began to run. Only directory names are
/// read, which leaves the datadir exactly as the node left it for its restart.
fn last_opened_epoch(datadir: &Path) -> eyre::Result<Epoch> {
    // the node's `TelcoinDirs::epochs_db_path`, as in `read_consensus_headers`
    let base = datadir.join("consensus-db").join("epochs");
    let mut last = None;
    for entry in std::fs::read_dir(&base).wrap_err_with(|| format!("listing {}", base.display()))? {
        let name = entry?.file_name();
        // staging, import and slot entries share the directory; only `epoch-<N>` is a pack
        let Some(epoch) =
            name.to_str().and_then(|name| name.strip_prefix("epoch-")).and_then(|n| n.parse().ok())
        else {
            continue;
        };
        last = last.max(Some(epoch));
    }
    last.ok_or_else(|| eyre::eyre!("no epoch pack under {}", base.display()))
}

#[test]
fn test_last_opened_epoch_reads_only_pack_directories() -> eyre::Result<()> {
    let datadir = tempfile::TempDir::with_prefix("last_opened")?;
    let base = datadir.path().join("consensus-db").join("epochs");
    for dir in ["epoch-0", "epoch-2", "epoch-10", "staging-11", "import-12", "epoch-x"] {
        std::fs::create_dir_all(base.join(dir))?;
    }
    std::fs::write(base.join("consensus_slot1"), [0u8; 16])?;
    assert_eq!(last_opened_epoch(datadir.path())?, 10);

    let empty = tempfile::TempDir::with_prefix("last_opened")?;
    std::fs::create_dir_all(empty.path().join("consensus-db").join("epochs"))?;
    assert!(last_opened_epoch(empty.path()).is_err(), "an empty chain has no opened epoch");
    Ok(())
}

/// Check the commit time each of `nodes` (a provider and its URL) serves for the final block of
/// every epoch in `epochs`, and return the first node's.
///
/// The block is the one the certified epoch record, fetched from the first node, names. Every node
/// must serve the same commit time for it and mark it as closing an epoch. Its `subSecond` flag,
/// which the node derives from the sub-dag leader's epoch
/// (`BlockTimestampMillis::with_consensus` in `crates/execution/tn-rpc/src/rpc_ext.rs`), must be
/// set exactly for epochs at or after [`SUBSECOND_RESTART_FORK_EPOCH`], and a pre-fork commit time
/// must be whole seconds.
async fn assert_epoch_closing_commit_times<P: Provider>(
    nodes: &[(&P, &str)],
    epochs: RangeInclusive<Epoch>,
) -> eyre::Result<Vec<BlockCommitTime>> {
    let Some(&(_, reference_url)) = nodes.first() else {
        return Err(eyre::eyre!("no node to check epoch closing commit times on"));
    };
    let mut closing = Vec::new();
    for epoch in epochs {
        let block =
            fetch_verified_epoch_record(reference_url, epoch, SUBSECOND_RESTART_RECORD_SECS)
                .await?
                .final_state
                .number;
        let mut served = Vec::with_capacity(nodes.len());
        for &(provider, url) in nodes {
            served.push(block_commit_time(provider, url, block).await?);
        }
        let reference = served.remove(0);
        for (commit, (_, url)) in served.iter().zip(&nodes[1..]) {
            eyre::ensure!(
                *commit == reference,
                "{url} disagrees with {reference_url} on block {block}, the final block of epoch \
                 {epoch}: {commit:?} vs {reference:?}"
            );
        }
        let post_fork = epoch >= SUBSECOND_RESTART_FORK_EPOCH;
        eyre::ensure!(
            reference.closes_epoch,
            "block {block}, the final block of epoch {epoch}'s record, does not close an epoch"
        );
        eyre::ensure!(
            reference.sub_second == post_fork,
            "the final block of epoch {epoch} reports subSecond = {}, but the fork is pinned at \
             epoch {SUBSECOND_RESTART_FORK_EPOCH}",
            reference.sub_second
        );
        eyre::ensure!(
            post_fork || reference.timestamp_millis % 1000 == 0,
            "pre-fork epoch {epoch} closed at {} ms, not on a whole second",
            reference.timestamp_millis
        );
        closing.push(reference);
    }
    Ok(closing)
}

/// Require the engine behind each metrics endpoint to report no EVM timestamp clamped up to its
/// parent's since that process started. `metrics_addrs[i]` belongs to the node at `urls[i]`.
fn assert_no_evm_timestamp_clamps(
    metrics_addrs: &[String],
    urls: &[String],
    test: &str,
) -> eyre::Result<()> {
    for (addr, url) in metrics_addrs.iter().zip(urls) {
        // blocking socket I/O with sleeps between retries, so it runs off the runtime worker
        let clamped = tokio::task::block_in_place(|| {
            scrape_metric_value(addr, EVM_TIMESTAMP_CLAMPED_SERIES)
        })?;
        eyre::ensure!(
            clamped == 0.0,
            "{url} clamped {clamped} EVM timestamps up to their parent's: consensus let commit \
             time go backwards in a post-fork epoch (node logs under test_logs/{test}/ carry the \
             \"evm timestamp clamped to parent\" warnings)"
        );
    }
    Ok(())
}

pub(crate) fn test_blocks_same(client_urls: &[String; 4]) -> eyre::Result<()> {
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

/// A validator starting alone at genesis must serve RPC through startup timeouts and join consensus
/// when the rest of its committee starts. The name includes `test_epoch` for the Durable MDBX lane.
#[test]
#[ignore = "run with make test-epochs"]
fn test_epoch_cold_genesis_without_peers() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let temp = tempfile::TempDir::new()?;
    let log_dir = Path::new(&std::env::var("CARGO_MANIFEST_DIR")?).join("test_logs/cold_genesis");
    let peer_readiness_wait = Duration::from_millis(500) * 240;
    let startup_sync_wait = Duration::from_secs(30);
    config_local_testnet(temp.path(), Some("restart_test".to_string()), None)?;
    let bin = e2e_tests::get_telcoin_network_binary();
    let rpc_ports = [
        get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no RPC port available for cold-genesis validator 0"))?,
        get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no RPC port available for cold-genesis validator 1"))?,
        get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no RPC port available for cold-genesis validator 2"))?,
        get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no RPC port available for cold-genesis validator 3"))?,
    ];
    let client_urls = rpc_ports.map(|port| format!("http://127.0.0.1:{port}"));
    let [alone_port, ..] = rpc_ports;
    let [alone_url, peer_url, ..] = &client_urls;
    let mut guard = ProcessGuard::empty();
    guard.push(start_validator(0, bin, temp.path(), alone_port, "cold_genesis", 0));

    {
        let child = RefCell::new(
            guard.get_mut(0).ok_or_else(|| eyre::eyre!("missing cold-genesis validator"))?,
        );
        let check_alive = || {
            child.try_borrow_mut()?.try_wait()?.map_or(Ok(()), |status| {
                eyre::bail!("cold-genesis validator exited: {status} ({})", log_dir.display())
            })
        };
        // Startup sync and primary-network readiness run before the worker creates its RPC
        // server. With no peers, allow both waits to expire plus process-startup headroom.
        wait_until_blocking(
            startup_sync_wait + peer_readiness_wait + Duration::from_secs(45),
            &format!("cold-genesis RPC ready without peers ({})", log_dir.display()),
            || {
                check_alive()?;
                Ok(call_rpc::<String, _, _>(
                    alone_url,
                    "eth_blockNumber",
                    rpc_params![],
                    0,
                    "cold-genesis readiness",
                )
                .is_ok())
            },
        )?;

        // Primary and worker readiness each wait 240 x 500ms. Observe beyond both waits plus the
        // 30s startup-sync deadline, even if every stage spends its entire allowance without peers.
        // Start this window after RPC is ready so slow process startup cannot shorten it.
        let observation = peer_readiness_wait * 2 + startup_sync_wait;
        let started = Instant::now();
        wait_until_blocking(
            observation + Duration::from_secs(30),
            &format!("cold-genesis RPC stays available without peers ({})", log_dir.display()),
            || {
                check_alive()?;
                call_rpc::<String, _, _>(
                    alone_url,
                    "eth_blockNumber",
                    rpc_params![],
                    0,
                    "cold-genesis liveness",
                )
                .wrap_err("cold-genesis RPC stopped responding while alone")?;
                Ok(started.elapsed() >= observation)
            },
        )?;
    }

    rpc_ports.into_iter().enumerate().skip(1).for_each(|(instance, port)| {
        guard.push(start_validator(instance, bin, temp.path(), port, "cold_genesis", 0));
    });
    network_advancing(&client_urls)?;
    wait_for_node_mode(alone_url, NodeMode::CvvActive)?;

    // Active mode is optimistic. Require a transaction submitted by the original process to be
    // confirmed by a peer, then applied locally, to prove that consensus actually formed.
    let key = get_key("test-source");
    send_and_confirm(alone_url, peer_url, &key, address_from_word("cold-genesis-target"), 0)?;
    wait_for_block(alone_url, get_block_number(peer_url)?)?;
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
