//! E2e test for the prometheus `/metrics` endpoint (`--metrics` CLI flag).

use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};
use tn_types::get_available_tcp_port;
use tracing::info;

use crate::common::{network_advancing, start_validator, start_validator_with_args, ProcessGuard};

/// Scrape the metrics endpoint with a raw HTTP GET (no client deps).
fn scrape_metrics(addr: &str) -> eyre::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

/// Poll the endpoint until the body contains all expected substrings (or time out).
fn scrape_until_contains(addr: &str, expected: &[&str], attempts: usize) -> eyre::Result<String> {
    let mut last = String::new();
    for _ in 0..attempts {
        if let Ok(body) = scrape_metrics(addr) {
            if expected.iter().all(|series| body.contains(series)) {
                return Ok(body);
            }
            last = body;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(eyre::eyre!(
        "metrics endpoint never served all of {expected:?}; last response:\n{}",
        &last[..last.len().min(2000)]
    ))
}

/// Start a local testnet with `--metrics` enabled on one validator and assert the
/// endpoint serves both `tn_*` and `reth_*` metric namespaces.
///
/// Uses the process-per-node harness: in-process multi-node setups would share one
/// global recorder and merge every node's series.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_metrics_endpoint_serves_tn_and_reth_metrics() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "e2e-test", "test_metrics_endpoint_serves_tn_and_reth_metrics");

    let tmp_guard = tempfile::TempDir::with_prefix("metrics_endpoint").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    e2e_tests::config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
        .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    let metrics_port = get_available_tcp_port("127.0.0.1").expect("metrics port assigned by host");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");

    // start 4 validators; the first one exposes the metrics endpoint
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        url.push_str(&format!(":{rpc_port}"));
        let child = if i == 0 {
            start_validator_with_args(
                i,
                &bin,
                &temp_path,
                rpc_port,
                "metrics_endpoint",
                0,
                &["--metrics", &metrics_addr],
            )
        } else {
            start_validator(i, &bin, &temp_path, rpc_port, "metrics_endpoint", 0)
        };
        guard.push(child);
    }

    // wait for the network to start serving RPC
    network_advancing(&client_urls)?;

    // the endpoint must serve telcoin metrics, reth's db instrumentation (via the
    // pre-scrape hook), and process metrics
    let body = scrape_until_contains(
        &metrics_addr,
        &[
            // server + recorder infrastructure
            "tn_info{",
            "reth_db_table_size",
            "reth_process_cpu_seconds_total",
            // per-crate instrumentation registered on a running validator
            "tn_primary_round",
            "tn_epoch_current",
            "tn_node_mode{",
            "tn_engine_queued_outputs",
            "tn_worker_batches_sealed_total",
            "tn_batch_builder_base_fee",
            "tn_executor_outputs_ready_total",
            "tn_network_connected_peers",
        ],
        45,
    )?;

    assert!(body.starts_with("HTTP/1.1 200 OK"), "expected 200 OK: {}", &body[..100]);
    info!(target: "e2e-test", "metrics endpoint serving tn_ and reth_ series");

    Ok(())
}
