//! Pipelined persist on real nodes (`pipelined_persist: true` in `parameters.yaml`).
//!
//! A pipelined node builds consensus output N+1 on N's in-memory state while N's persist runs.
//! These tests start four-validator fleets with the switch on some or all nodes and check that a
//! pipelined node computes exactly the chain a sequential node computes, across epoch boundaries,
//! graceful restarts and SIGKILLs, and that no node reports a pipeline invariant breach, a fork, a
//! failed persist or a broken restart replay.
//!
//! On a local network a persist takes milliseconds, so the build of N+1 would almost never wait
//! behind N's persist, and a test would cover half of the pipeline's state machine. Every node
//! under test therefore runs with `TN_TEST_PERSIST_DELAY_MS`, and every test fails unless the
//! pipelined nodes' own logs show the overlap: `output executed` lines (debug, target `engine`)
//! with `pipelined=true` and `persist_wait_ms` above zero. A node logs that wait only for an output
//! that finished building while the previous output's persist was still running.
//!
//! The delay is applied through `tn-reth`'s test hooks, so the node binary needs
//! `tn-reth/test-utils` beside the usual `tn-storage/test-utils`:
//! `cargo build --profile e2e --bin telcoin-network --features
//! tn-storage/test-utils,tn-reth/test-utils`. A binary without it never logs
//! [`PERSIST_DELAY_LOG`], and every test here fails on that.

use super::common::{
    acquire_test_permit, address_from_word, call_rpc, current_epoch, fetch_verified_epoch_record,
    force_kill_and_reap, get_block, get_block_number, get_key, kill_child, log_field,
    log_line_matches, network_advancing, node_log_lines, node_log_path, plain_log_lines,
    scrape_metric_value, send_tel, start_validator_with_overrides, wait_for_node_log_line,
    ProcessGuard,
};
use alloy::{primitives::utils::parse_ether, providers::ProviderBuilder};
use e2e_tests::{config_local_testnet_with_epoch_duration, FleetOverrides, TestBinary};
use eyre::WrapErr as _;
use jsonrpsee::rpc_params;
use serde_json::Value;
use std::{
    collections::{BTreeSet, HashMap},
    io::{Read as _, Seek as _, SeekFrom},
    path::{Path, PathBuf},
    process::Child,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tn_test_utils::wait_until;
use tn_types::{get_available_tcp_port, GenesisAccount, U256};
use tracing::info;

/// The switch under test.
const SWITCH: &str = "pipelined_persist";

/// The test hook that holds every persist of a node for a while (`tn_types::test_hooks`).
const PERSIST_DELAY_ENV: &str = "TN_TEST_PERSIST_DELAY_MS";

/// The persist delay of the nodes under test, in milliseconds.
///
/// Longer than the interval between two loaded outputs on a local fleet, so a pipelined node
/// finishes building the next output while the previous one still persists, and short enough
/// that a sequential node with the same delay keeps pace with the committee.
const PERSIST_DELAY_MS: u64 = 400;

/// The warning a node logs once when it reads [`PERSIST_DELAY_ENV`]; its absence means the binary
/// lacks the hook.
const PERSIST_DELAY_LOG: &str = "test hook: delaying every persist";

/// The node log filter of the nodes under test: the engine's per-output lines are debug.
const NODE_LOG_FILTER: &str = "info,engine=debug";

/// The per-output engine line, emitted when an output is durable and announced.
const OUTPUT_EXECUTED_LOG: &str = "output executed";

/// The account that funds the transfers of [`Load`].
const LOAD_SOURCE: &str = "persist-pipeline-load-source";

/// Text that must not appear in any log of any node: a pipeline invariant breach or failure, a
/// fork seen by the subscriber, the epoch votes or the epoch close, or a broken restart replay.
const FORBIDDEN_LOGS: [&str; 8] = [
    "PipelineInvariant",
    "pipelined persist failed",
    "execution has forked",
    "we have forked",
    "does not match on chain closing block",
    "final state is not this node's closing block",
    "restart replay is not contiguous",
    "Crossed epoch boundary with missing execution",
];

/// Fewest overlapped outputs (see [`OutputLines::overlapped`]) each pipelined node must log for a
/// test to count as having exercised the pipeline.
const MIN_OVERLAPPED_OUTPUTS: usize = 5;

/// How long a restarted node gets to catch up with the rest of the fleet under load.
const CATCH_UP: Duration = Duration::from_secs(240);

/// The pause before each check of a polled condition, so a poll does not flood a node's RPC or
/// reread its log every few milliseconds on a loaded host.
const POLL: Duration = Duration::from_millis(250);

/// Datadir names of the four validators, in instance order.
const VALIDATORS: [&str; 4] = ["validator-1", "validator-2", "validator-3", "validator-4"];

// ---- the fleet --------------------------------------------------------------------------------

/// A four-validator fleet started through the harness, with each node's runs counted so its logs
/// can be read back per run.
struct PipelineFleet {
    /// The test's log directory name under `test_logs/`.
    test: &'static str,
    /// The temp dir that holds the datadirs.
    base: PathBuf,
    /// The node binary.
    bin: &'static TestBinary,
    /// Per-node settings, applied again at every restart.
    overrides: FleetOverrides,
    /// The running processes, by instance.
    guard: ProcessGuard,
    /// Each node's RPC port, kept across restarts.
    rpc_ports: [u16; 4],
    /// Each node's metrics address, kept across restarts.
    metrics: [String; 4],
    /// Each node's current run (its log file suffix).
    runs: [u32; 4],
}

impl PipelineFleet {
    /// Start all four validators (run 0) with `overrides`.
    fn start(test: &'static str, base: &Path, overrides: FleetOverrides) -> eyre::Result<Self> {
        let port = || {
            get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no free port on host"))
        };
        let rpc_ports = [port()?, port()?, port()?, port()?];
        let metrics = [port()?, port()?, port()?, port()?].map(|p| format!("127.0.0.1:{p}"));
        let mut fleet = Self {
            test,
            base: base.to_path_buf(),
            bin: e2e_tests::get_telcoin_network_binary(),
            overrides,
            guard: ProcessGuard::empty(),
            rpc_ports,
            metrics,
            runs: [0; 4],
        };
        for instance in 0..4 {
            let child = fleet.spawn(instance)?;
            fleet.guard.push(child);
        }
        Ok(fleet)
    }

    /// Spawn node `instance` for its current run.
    fn spawn(&self, instance: usize) -> eyre::Result<Child> {
        start_validator_with_overrides(
            instance,
            self.bin,
            &self.base,
            self.rpc_ports[instance],
            self.test,
            self.runs[instance],
            &["--metrics", &self.metrics[instance]],
            &self.overrides,
        )
    }

    /// The RPC URL of node `instance`.
    fn url(&self, instance: usize) -> String {
        format!("http://127.0.0.1:{}", self.rpc_ports[instance])
    }

    /// The RPC URLs of all four nodes.
    fn urls(&self) -> [String; 4] {
        [self.url(0), self.url(1), self.url(2), self.url(3)]
    }

    /// Stop node `instance` (SIGTERM with a SIGKILL fallback, or SIGKILL alone) and check that its
    /// RPC no longer answers.
    fn stop(&mut self, instance: usize, sigkill: bool) -> eyre::Result<()> {
        let mut child = self
            .guard
            .take(instance)
            .ok_or_else(|| eyre::eyre!("node {instance} is not running"))?;
        if sigkill {
            force_kill_and_reap(&mut child);
        } else {
            kill_child(&mut child);
        }
        eyre::ensure!(
            get_block_number(&self.url(instance)).is_err(),
            "node {instance} still answers RPC after it was stopped"
        );
        Ok(())
    }

    /// Start node `instance` again as its next run.
    fn restart(&mut self, instance: usize) -> eyre::Result<()> {
        self.runs[instance] += 1;
        let child = self.spawn(instance)?;
        self.guard.replace(instance, child);
        Ok(())
    }

    /// Every log file (stdout and stderr) of node `instance`, over all its runs so far.
    fn log_files(&self, instance: usize) -> Vec<(u32, PathBuf)> {
        (0..=self.runs[instance])
            .flat_map(|run| {
                [false, true].map(|stderr| (run, node_log_path(self.test, instance, run, stderr)))
            })
            .collect()
    }

    /// The stdout lines of node `instance`, over all its runs so far.
    fn stdout_lines(&self, instance: usize) -> eyre::Result<Vec<String>> {
        let mut lines = Vec::new();
        for run in 0..=self.runs[instance] {
            lines.extend(node_log_lines(self.test, instance, run)?);
        }
        Ok(lines)
    }

    /// Wait until node `instance`'s current run logs its `node switches` line with the switch
    /// set to `expected`.
    async fn wait_for_switch(&self, instance: usize, expected: bool) -> eyre::Result<()> {
        wait_for_node_log_line(
            self.test,
            instance,
            self.runs[instance],
            "node switches",
            &[format!("{SWITCH}={expected}")],
            Duration::from_secs(90),
        )
        .await
    }

    /// Wait until node `instance` holds every block another node held when the wait began.
    async fn wait_caught_up(&self, instance: usize) -> eyre::Result<u64> {
        let target = (0..4)
            .filter(|other| *other != instance)
            .filter_map(|other| get_block_number(&self.url(other)).ok())
            .max()
            .ok_or_else(|| eyre::eyre!("no other node answers RPC"))?;
        let url = self.url(instance);
        wait_until(CATCH_UP, &format!("node {instance} to reach block {target}"), || async {
            tokio::time::sleep(POLL).await;
            Ok(get_block_number(&url).is_ok_and(|height| height >= target))
        })
        .await?;
        Ok(target)
    }

    /// Wait until node 0's head moves `blocks` past `from`.
    async fn wait_for_progress(&self, from: u64, blocks: u64) -> eyre::Result<()> {
        let url = self.url(0);
        wait_until(Duration::from_secs(90), "node 0 to keep building blocks", || async {
            tokio::time::sleep(POLL).await;
            Ok(get_block_number(&url).is_ok_and(|height| height >= from + blocks))
        })
        .await
    }

    /// Wait until all four nodes report the same head, and return it.
    async fn wait_for_common_head(&self) -> eyre::Result<u64> {
        let urls = self.urls();
        let floor = get_block_number(&urls[0])?;
        let common = std::sync::Mutex::new(0);
        wait_until(Duration::from_secs(240), "all four nodes to reach one head", || async {
            tokio::time::sleep(POLL).await;
            let heads =
                urls.iter().map(|url| get_block_number(url)).collect::<eyre::Result<Vec<_>>>();
            Ok(heads.is_ok_and(|heads| {
                let same = heads.iter().all(|head| *head == heads[0] && *head >= floor);
                if same {
                    *common.lock().expect("lock") = heads[0];
                }
                same
            }))
        })
        .await?;
        let head = *common.lock().expect("lock");
        Ok(head)
    }
}

// ---- load -------------------------------------------------------------------------------------

/// Value transfers from [`LOAD_SOURCE`] sent through one node until stopped.
struct Load {
    /// Set to stop the sender.
    stop: Arc<AtomicBool>,
    /// Transfers the node accepted.
    sent: Arc<AtomicU64>,
    /// The sender thread.
    handle: Option<JoinHandle<()>>,
}

impl Load {
    /// Start sending through `node`. A refused send resyncs the nonce from the pool, so a full
    /// pool slows the load to the rate of inclusion instead of ending it.
    fn start(node: String) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let sent = Arc::new(AtomicU64::new(0));
        let handle = {
            let (stop, sent) = (Arc::clone(&stop), Arc::clone(&sent));
            std::thread::spawn(move || {
                let key = get_key(LOAD_SOURCE);
                let source = address_from_word(LOAD_SOURCE);
                let sink = address_from_word("persist-pipeline-load-sink");
                let mut nonce = 0u128;
                while !stop.load(Ordering::Relaxed) {
                    match send_tel(&node, &key, sink, 1, 250, 21_000, nonce) {
                        Ok(_) => {
                            sent.fetch_add(1, Ordering::Relaxed);
                            nonce += 1;
                        }
                        Err(_) => {
                            std::thread::sleep(Duration::from_millis(100));
                            nonce = pending_nonce(&node, &source.to_string()).unwrap_or(nonce);
                        }
                    }
                }
            })
        };
        Self { stop, sent, handle: Some(handle) }
    }

    /// Transfers accepted so far.
    fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    /// Wait until the node has accepted at least one transfer.
    async fn wait_flowing(&self) -> eyre::Result<()> {
        wait_until(Duration::from_secs(60), "the load's first accepted transfer", || async {
            Ok(self.sent() > 0)
        })
        .await
    }

    /// Stop the sender and return how many transfers were accepted.
    fn finish(mut self) -> eyre::Result<u64> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| eyre::eyre!("load thread panicked"))?;
        }
        Ok(self.sent())
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Pending nonce of `address` on `node`.
fn pending_nonce(node: &str, address: &str) -> eyre::Result<u128> {
    let count: String =
        call_rpc(node, "eth_getTransactionCount", rpc_params![address, "pending"], 1, "nonce")?;
    Ok(u128::from_str_radix(count.trim_start_matches("0x"), 16)?)
}

// ---- reading the nodes ------------------------------------------------------------------------

/// The genesis of the tests: a funded load source and `epoch_secs`-second epochs.
fn configure(base: &Path, epoch_secs: u32) -> eyre::Result<()> {
    let funded = vec![(
        address_from_word(LOAD_SOURCE),
        GenesisAccount::default().with_balance(U256::from(parse_ether("10_000_000")?)),
    )];
    config_local_testnet_with_epoch_duration(
        base,
        Some("restart_test".to_string()),
        Some(funded),
        Some(epoch_secs),
    )
}

/// The current epoch on `url`.
async fn epoch_of(url: &str) -> eyre::Result<u32> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    Ok(current_epoch(&provider).await?.epoch_id)
}

/// Wait until the epoch on `url` is at least `target`.
async fn wait_for_epoch(url: &str, target: u32, timeout: Duration) -> eyre::Result<u32> {
    wait_until(timeout, &format!("epoch {target} on {url}"), || async {
        tokio::time::sleep(POLL).await;
        Ok(epoch_of(url).await.is_ok_and(|epoch| epoch >= target))
    })
    .await?;
    epoch_of(url).await
}

/// The number of the block `url` reports for `tag` (`finalized`, `latest`), 0 when it has none.
fn block_number_at(url: &str, tag: &str) -> eyre::Result<u64> {
    let block: Option<HashMap<String, Value>> =
        call_rpc(url, "eth_getBlockByNumber", rpc_params!(tag, false), 3, tag)?;
    block
        .and_then(|block| block.get("number").and_then(Value::as_str).map(str::to_owned))
        .map_or(Ok(0), |number| Ok(u64::from_str_radix(number.trim_start_matches("0x"), 16)?))
}

/// A string field of an RPC block (a hash or a hex number), empty when absent.
fn block_field(block: &HashMap<String, Value>, field: &str) -> String {
    block.get(field).and_then(Value::as_str).unwrap_or_default().to_owned()
}

/// Fail unless every node holds node 0's block at every height from 1 to `height`: the chains are
/// identical, which is `restarts.rs::test_blocks_same`'s assertion at `height` and at every block
/// below it.
fn assert_same_chain(urls: &[String], height: u64) -> eyre::Result<()> {
    for number in 1..=height {
        let reference = get_block(&urls[0], Some(number))?;
        let hash = block_field(&reference, "hash");
        for (node, url) in urls.iter().enumerate().skip(1) {
            let block = get_block(url, Some(number))?;
            eyre::ensure!(
                block_field(&block, "hash") == hash,
                "chains differ at block {number}: node 0 has {hash} (state root {}), node {node} \
                 has {} (state root {})",
                block_field(&reference, "stateRoot"),
                block_field(&block, "hash"),
                block_field(&block, "stateRoot")
            );
        }
    }
    Ok(())
}

/// What a node's `output executed` lines say about its engine.
#[derive(Debug, Default)]
struct OutputLines {
    /// Lines with `pipelined=true`.
    pipelined: usize,
    /// Lines without the field: the sequential engine's.
    sequential: usize,
    /// Pipelined lines with `persist_wait_ms` above zero: outputs built while the previous
    /// output's persist was still running.
    overlapped: usize,
    /// The longest `persist_wait_ms`.
    max_persist_wait_ms: u64,
    /// The shortest `persist_ms` of an output with blocks: at least the persist delay when the
    /// hook is armed.
    min_persist_ms: Option<u64>,
}

impl OutputLines {
    /// Read the `output executed` lines among `lines`.
    fn read(lines: &[String]) -> Self {
        let mut read = Self::default();
        for line in lines.iter().filter(|line| line.contains(OUTPUT_EXECUTED_LOG)) {
            if line.split_whitespace().any(|token| token == "pipelined=true") {
                read.pipelined += 1;
                let wait = log_field(line, "persist_wait_ms").unwrap_or_default();
                if wait > 0 {
                    read.overlapped += 1;
                }
                read.max_persist_wait_ms = read.max_persist_wait_ms.max(wait);
            } else {
                read.sequential += 1;
            }
            if log_field(line, "blocks").is_some_and(|blocks| blocks > 0) {
                let persist = log_field(line, "persist_ms").unwrap_or_default();
                read.min_persist_ms =
                    Some(read.min_persist_ms.map_or(persist, |min| min.min(persist)));
            }
        }
        read
    }

    /// The counts as `name=value` text.
    fn summary(&self) -> String {
        format!(
            "pipelined={} sequential={} overlapped={} max_persist_wait_ms={} min_persist_ms={}",
            self.pipelined,
            self.sequential,
            self.overlapped,
            self.max_persist_wait_ms,
            self.min_persist_ms.map_or_else(|| "none".to_string(), |ms| ms.to_string())
        )
    }
}

/// Fail unless node `instance`'s logs show the engine it was given: on a pipelined node only
/// pipelined lines and at least [`MIN_OVERLAPPED_OUTPUTS`] overlapped outputs; on a sequential
/// node no pipelined line; with the delay armed, the hook's warning in every run and no output
/// with blocks persisted faster than the delay.
fn assert_engine_lines(
    fleet: &PipelineFleet,
    instance: usize,
    pipelined: bool,
    delayed: bool,
) -> eyre::Result<OutputLines> {
    let lines = fleet.stdout_lines(instance)?;
    let read = OutputLines::read(&lines);
    if pipelined {
        eyre::ensure!(
            read.sequential == 0 && read.overlapped >= MIN_OVERLAPPED_OUTPUTS,
            "pipelined node {instance} never overlapped build and persist enough to count: {}",
            read.summary()
        );
    } else {
        eyre::ensure!(
            read.pipelined == 0 && read.sequential > 0,
            "sequential node {instance} logged pipelined outputs: {}",
            read.summary()
        );
    }
    if delayed {
        for run in 0..=fleet.runs[instance] {
            eyre::ensure!(
                node_log_lines(fleet.test, instance, run)?
                    .iter()
                    .any(|line| line.contains(PERSIST_DELAY_LOG)),
                "node {instance} run {run} never logged `{PERSIST_DELAY_LOG}`: the binary lacks \
                 the persist delay hook (build it with tn-reth/test-utils)"
            );
        }
        eyre::ensure!(
            read.min_persist_ms.is_some_and(|min| min >= PERSIST_DELAY_MS),
            "node {instance} persisted an output faster than the {PERSIST_DELAY_MS} ms delay: \
             {}",
            read.summary()
        );
    }
    Ok(read)
}

/// Fail if any log of any node holds a line from [`FORBIDDEN_LOGS`].
fn assert_no_forbidden_lines(fleet: &PipelineFleet) -> eyre::Result<()> {
    let mut found = Vec::new();
    for instance in 0..4 {
        for (run, path) in fleet.log_files(instance) {
            for line in plain_log_lines(&path)? {
                if FORBIDDEN_LOGS.iter().any(|text| line.contains(text)) {
                    found.push(format!("node {instance} run {run}: {line}"));
                }
            }
        }
    }
    eyre::ensure!(found.is_empty(), "forbidden log lines:\n{}", found.join("\n"));
    Ok(())
}

/// Fail unless every run of every node logged its `node switches` line with the switch it was
/// given.
fn assert_switch_lines(fleet: &PipelineFleet, given: [bool; 4]) -> eyre::Result<()> {
    for (instance, expected) in given.into_iter().enumerate() {
        let field = [format!("{SWITCH}={expected}")];
        for run in 0..=fleet.runs[instance] {
            eyre::ensure!(
                node_log_lines(fleet.test, instance, run)?.iter().any(|line| log_line_matches(
                    line,
                    "node switches",
                    &field
                )),
                "node {instance} run {run} has no `node switches` line with {}",
                field[0]
            );
        }
    }
    Ok(())
}

/// The engine's pipeline series on node `instance`, as `name=value` text for the hand-back.
fn pipeline_metrics(fleet: &PipelineFleet, instance: usize) -> eyre::Result<String> {
    let addr = &fleet.metrics[instance];
    let names = [
        "tn_engine_commit_interval_seconds_count",
        "tn_engine_persist_wait_seconds_count",
        "tn_engine_close_barrier_holds_total",
    ];
    let mut text = Vec::new();
    for name in names {
        text.push(format!("{name}={}", scrape_metric_value(addr, name)?));
    }
    Ok(text.join(" "))
}

// ---- tests ------------------------------------------------------------------------------------

/// The go/no-go fleet: validators 1-2 pipelined, 3-4 sequential, all with the persist delay, under
/// load across two epoch boundaries, with one pipelined and one sequential node restarted on the
/// way. At the end all four hold the same chain, every node's finalized block advanced, every run
/// reported the switch it was given, no log carries a forbidden line, and both pipelined nodes
/// show the overlap.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "four-node pipelined persist fleet; run through the e2e lane"]
async fn test_mixed_fleet_blocks_same() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = "persist_pipeline_mixed_fleet";
    let tmp = tempfile::TempDir::with_prefix(test)?;
    configure(tmp.path(), 20)?;
    let delay = PERSIST_DELAY_MS.to_string();
    let overrides = FleetOverrides::default()
        .parameter(["validator-1", "validator-2"], SWITCH, "true")
        // explicit, so a fleet-wide `TN_E2E_PARAMETERS` cannot turn the sequential half on
        .parameter(["validator-3", "validator-4"], SWITCH, "false")
        .env(VALIDATORS, PERSIST_DELAY_ENV, delay.as_str())
        .env(VALIDATORS, "RUST_LOG", NODE_LOG_FILTER);
    let mut fleet = PipelineFleet::start(test, tmp.path(), overrides)?;
    let result = mixed_fleet(&mut fleet).await;
    fleet.guard.kill_all();
    let summary = result.wrap_err(format!("{test} failed, check logs in test_logs/{test}/"))?;
    eprintln!("{test}: {summary}");
    Ok(())
}

/// The body of [`test_mixed_fleet_blocks_same`]; returns the evidence for the hand-back.
async fn mixed_fleet(fleet: &mut PipelineFleet) -> eyre::Result<String> {
    let given = [true, true, false, false];
    network_advancing(&fleet.urls())?;
    for (instance, on) in given.into_iter().enumerate() {
        fleet.wait_for_switch(instance, on).await?;
    }
    let finalized_at_start = fleet
        .urls()
        .iter()
        .map(|url| block_number_at(url, "finalized"))
        .collect::<eyre::Result<Vec<_>>>()?;
    let load = Load::start(fleet.url(0));
    load.wait_flowing().await?;
    let start_epoch = epoch_of(&fleet.url(0)).await?;
    info!(target: "persist-pipeline", start_epoch, "load flowing");

    // one pipelined and one sequential node restarted mid-run, one at a time so the other three
    // keep the quorum
    let mut restarts = Vec::new();
    for instance in [1, 2] {
        fleet.wait_for_progress(get_block_number(&fleet.url(0))?, 3).await?;
        fleet.stop(instance, false)?;
        fleet.wait_for_progress(get_block_number(&fleet.url(0))?, 3).await?;
        fleet.restart(instance)?;
        fleet.wait_for_switch(instance, given[instance]).await?;
        let caught_up = fleet.wait_caught_up(instance).await?;
        restarts.push(format!("node {instance} restarted and reached block {caught_up}"));
    }

    // two boundaries under load in all, and one after both restarts
    let after_restarts = epoch_of(&fleet.url(0)).await?;
    let target = (start_epoch + 2).max(after_restarts + 1);
    let end_epoch = wait_for_epoch(&fleet.url(0), target, Duration::from_secs(240)).await?;
    let sent = load.finish()?;
    let head = fleet.wait_for_common_head().await?;
    assert_same_chain(&fleet.urls(), head)?;

    for (instance, url) in fleet.urls().iter().enumerate() {
        let finalized = block_number_at(url, "finalized")?;
        eyre::ensure!(
            finalized > finalized_at_start[instance],
            "node {instance}'s finalized block did not advance: {} -> {finalized}",
            finalized_at_start[instance]
        );
    }
    assert_switch_lines(fleet, given)?;
    assert_no_forbidden_lines(fleet)?;
    let mut evidence = Vec::new();
    for (instance, on) in given.into_iter().enumerate() {
        let lines = assert_engine_lines(fleet, instance, on, true)?;
        let metrics = pipeline_metrics(fleet, instance)?;
        if on {
            let interval: f64 = scrape_metric_value(
                &fleet.metrics[instance],
                "tn_engine_commit_interval_seconds_count",
            )?;
            eyre::ensure!(
                interval > 0.0,
                "pipelined node {instance} recorded no saturated commit interval"
            );
        }
        evidence.push(format!("node {instance} {} {metrics}", lines.summary()));
    }
    Ok(format!(
        "epochs {start_epoch}->{end_epoch}, {sent} transfers, {head} blocks identical on all \
         four; {}; {}",
        restarts.join("; "),
        evidence.join("; ")
    ))
}

/// All four nodes pipelined with the persist delay, under load across an epoch boundary: every
/// node certifies the same epoch records, the next epoch starts and builds blocks, the chains are
/// identical, and each node's `tn_engine_close_barrier_holds_total` is reported (the design
/// expects 0: the forwarder sends nothing past a close).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "four-node pipelined persist fleet; run through the e2e lane"]
async fn test_pipelined_epoch_close_under_load() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = "persist_pipeline_epoch_close";
    let tmp = tempfile::TempDir::with_prefix(test)?;
    configure(tmp.path(), 15)?;
    let delay = PERSIST_DELAY_MS.to_string();
    let overrides = FleetOverrides::default()
        .parameter(VALIDATORS, SWITCH, "true")
        .env(VALIDATORS, PERSIST_DELAY_ENV, delay.as_str())
        .env(VALIDATORS, "RUST_LOG", NODE_LOG_FILTER);
    let mut fleet = PipelineFleet::start(test, tmp.path(), overrides)?;
    let result = epoch_close_under_load(&mut fleet).await;
    fleet.guard.kill_all();
    let summary = result.wrap_err(format!("{test} failed, check logs in test_logs/{test}/"))?;
    eprintln!("{test}: {summary}");
    Ok(())
}

/// The body of [`test_pipelined_epoch_close_under_load`]; returns the evidence.
async fn epoch_close_under_load(fleet: &mut PipelineFleet) -> eyre::Result<String> {
    network_advancing(&fleet.urls())?;
    for instance in 0..4 {
        fleet.wait_for_switch(instance, true).await?;
    }
    let load = Load::start(fleet.url(0));
    load.wait_flowing().await?;
    let start_epoch = epoch_of(&fleet.url(0)).await?;
    // the pipeline must be busy when the boundary arrives: wait for overlapped outputs first
    wait_until(Duration::from_secs(120), "overlapped outputs before the boundary", || async {
        tokio::time::sleep(POLL).await;
        Ok(OutputLines::read(&fleet.stdout_lines(0)?).overlapped >= MIN_OVERLAPPED_OUTPUTS)
    })
    .await?;
    let closed = epoch_of(&fleet.url(0)).await?;
    let next = wait_for_epoch(&fleet.url(0), closed + 1, Duration::from_secs(120)).await?;
    // the next epoch builds blocks under the same load
    let provider = ProviderBuilder::new().connect_http(fleet.url(0).parse()?);
    let first_block = current_epoch(&provider).await?.block_height;
    let url = fleet.url(0);
    wait_until(Duration::from_secs(120), "a block of the new epoch", || async {
        tokio::time::sleep(POLL).await;
        Ok(get_block_number(&url).is_ok_and(|height| height >= first_block))
    })
    .await?;
    let sent = load.finish()?;
    let head = fleet.wait_for_common_head().await?;
    assert_same_chain(&fleet.urls(), head)?;

    // every node certifies and holds the same records for every closed epoch
    let mut records = Vec::new();
    for epoch in 0..next {
        let mut reference = None;
        for (instance, url) in fleet.urls().iter().enumerate() {
            let record = fetch_verified_epoch_record(url, epoch, 60).await?;
            let block = get_block(url, Some(record.final_state.number))?;
            eyre::ensure!(
                block_field(&block, "hash") == record.final_state.hash.to_string(),
                "node {instance}'s block {} is not the final state of its epoch {epoch} record",
                record.final_state.number
            );
            match &reference {
                None => reference = Some(record),
                Some(first) => eyre::ensure!(
                    *first == record,
                    "node {instance}'s epoch {epoch} record differs from node 0's"
                ),
            }
        }
        if let Some(record) = reference {
            records.push(format!("epoch {epoch} final block {}", record.final_state.number));
        }
    }

    assert_switch_lines(fleet, [true; 4])?;
    assert_no_forbidden_lines(fleet)?;
    let mut evidence = Vec::new();
    for instance in 0..4 {
        let lines = assert_engine_lines(fleet, instance, true, true)?;
        let barrier =
            scrape_metric_value(&fleet.metrics[instance], "tn_engine_close_barrier_holds_total")?;
        evidence.push(format!(
            "node {instance} close_barrier_holds_total={barrier} {}",
            lines.summary()
        ));
    }
    Ok(format!(
        "epochs {start_epoch}->{next} (boundary crossed under load at {closed}->{next}), {sent} \
         transfers, {head} blocks identical; records equal: {}; {}",
        records.join(", "),
        evidence.join("; ")
    ))
}

/// The pipelined node [`test_sigkill_mid_pipeline_replays`] kills.
const VICTIM: usize = 1;

/// What a SIGKILL of [`test_sigkill_mid_pipeline_replays`] waits for in the victim's log before
/// its offset runs.
#[derive(Clone, Copy)]
enum KillTrigger {
    /// The engine queued output c while output c-1 had not executed: with the persist delay, c-1
    /// is committing (or about to) and c builds behind it, then parks until c-1's commit ends.
    QueuedBehindInFlight,
    /// The engine logged `output executed` for an output that had waited behind the previous
    /// persist: the hand-over, after which the next output's commit starts if one is parked.
    ExecutedAfterWait,
}

impl KillTrigger {
    /// The trigger's name for the hand-back.
    fn name(self) -> &'static str {
        match self {
            Self::QueuedBehindInFlight => "queued-behind-in-flight",
            Self::ExecutedAfterWait => "executed-after-wait",
        }
    }
}

/// The five kills of [`test_sigkill_mid_pipeline_replays`]: what each waits for in the victim's
/// log and how many milliseconds later it lands.
///
/// Queued +0 lands while output c builds behind c-1's persist; queued +150 and +300 land inside
/// c-1's persist delay with c built and parked, the case the test must hit; executed +60 and +380
/// land inside or after the next output's persist, usually with nothing built behind it.
const KILL_POINTS: [(KillTrigger, u64); 5] = [
    (KillTrigger::QueuedBehindInFlight, 0),
    (KillTrigger::QueuedBehindInFlight, 150),
    (KillTrigger::ExecutedAfterWait, 60),
    (KillTrigger::QueuedBehindInFlight, 300),
    (KillTrigger::ExecutedAfterWait, 380),
];

/// Wait until the log behind `tail` shows `trigger`, and return the consensus number the
/// triggering line names.
async fn wait_for_trigger(tail: &mut LogTail, trigger: KillTrigger) -> eyre::Result<u64> {
    let (mut queued, mut executed) = (BTreeSet::new(), BTreeSet::new());
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        for line in tail.new_lines()? {
            let number = log_field(&line, "consensus_number");
            if line.contains("output queued") {
                let Some(number) = number else { continue };
                queued.insert(number);
                let previous = number.saturating_sub(1);
                if matches!(trigger, KillTrigger::QueuedBehindInFlight)
                    && queued.contains(&previous)
                    && !executed.contains(&previous)
                {
                    return Ok(number);
                }
            } else if line.contains(OUTPUT_EXECUTED_LOG) {
                executed.extend(number);
                if matches!(trigger, KillTrigger::ExecutedAfterWait)
                    && log_field(&line, "persist_wait_ms").is_some_and(|wait| wait > 0)
                {
                    return Ok(number.unwrap_or_default());
                }
            }
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "the victim's log showed no {} trigger within 180s",
            trigger.name()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The `parentBeaconBlockRoot` of block `number` on `url`: the digest of the consensus header
/// whose output built the block.
fn output_of_block(url: &str, number: u64) -> eyre::Result<String> {
    let block: Option<HashMap<String, Value>> = call_rpc(
        url,
        "eth_getBlockByNumber",
        rpc_params!(format!("0x{number:x}"), false),
        3,
        number,
    )?;
    block
        .map(|block| block_field(&block, "parentBeaconBlockRoot"))
        .ok_or_else(|| eyre::eyre!("{url} has no block {number}"))
}

/// One SIGKILL of the victim, with what its RPC showed right before it.
struct Kill {
    /// What the kill waited for in the victim's log.
    trigger: KillTrigger,
    /// Milliseconds after the triggering line.
    offset_ms: u64,
    /// The consensus number the triggering line names.
    after_output: u64,
    /// The victim's finalized block (durable and announced).
    finalized: u64,
    /// The victim's latest block (the in-memory tip: built, perhaps not durable).
    latest: u64,
    /// Consensus outputs whose blocks lay between the two, read back from node 0 at the end.
    outputs_in_flight: usize,
}

impl Kill {
    /// The kill as text for the hand-back.
    fn summary(&self) -> String {
        format!(
            "{} +{}ms (output {}): finalized {} latest {} outputs_in_flight {}",
            self.trigger.name(),
            self.offset_ms,
            self.after_output,
            self.finalized,
            self.latest,
            self.outputs_in_flight
        )
    }
}

/// New lines appended to a node log since the last read.
struct LogTail {
    /// The log file.
    path: PathBuf,
    /// Bytes already read.
    offset: u64,
    /// A trailing line without its newline yet.
    partial: String,
}

impl LogTail {
    /// Tail `path` from its current end.
    fn at_end(path: PathBuf) -> eyre::Result<Self> {
        let offset = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or_default();
        Ok(Self { path, offset, partial: String::new() })
    }

    /// The complete lines appended since the last call, without ANSI escapes.
    fn new_lines(&mut self) -> eyre::Result<Vec<String>> {
        let mut file = std::fs::File::open(&self.path)
            .wrap_err_with(|| format!("opening {}", self.path.display()))?;
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        self.offset += bytes.len() as u64;
        self.partial.push_str(&String::from_utf8_lossy(&bytes));
        let complete = match self.partial.rfind('\n') {
            Some(end) => {
                let rest = self.partial.split_off(end + 1);
                std::mem::replace(&mut self.partial, rest)
            }
            None => return Ok(Vec::new()),
        };
        Ok(super::common::strip_ansi(&complete).lines().map(str::to_owned).collect())
    }
}

/// One pipelined node under load, with the persist delay, SIGKILLed five times at points read
/// from its own log (see [`KILL_POINTS`]); the other three run sequentially. After each kill
/// the node restarts and catches up; at the end all four hold the same chain, no node logs a
/// forbidden line (a failed replay among them), and at least one kill caught two outputs executed
/// past the finalized tip (one committing, the next built), read from the blocks between the
/// victim's finalized and latest block at the kill.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "four-node pipelined persist fleet; run through the e2e lane"]
async fn test_sigkill_mid_pipeline_replays() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = "persist_pipeline_sigkill";
    let tmp = tempfile::TempDir::with_prefix(test)?;
    configure(tmp.path(), 30)?;
    let victim = VALIDATORS[VICTIM];
    let others = VALIDATORS.into_iter().filter(|name| *name != victim).collect::<Vec<_>>();
    let overrides = FleetOverrides::default()
        .parameter([victim], SWITCH, "true")
        .parameter(others, SWITCH, "false")
        .env([victim], PERSIST_DELAY_ENV, PERSIST_DELAY_MS.to_string())
        .env(VALIDATORS, "RUST_LOG", NODE_LOG_FILTER);
    let mut fleet = PipelineFleet::start(test, tmp.path(), overrides)?;
    let result = sigkill_mid_pipeline(&mut fleet).await;
    fleet.guard.kill_all();
    let summary = result.wrap_err(format!("{test} failed, check logs in test_logs/{test}/"))?;
    eprintln!("{test}: {summary}");
    Ok(())
}

/// The body of [`test_sigkill_mid_pipeline_replays`]; returns the evidence.
async fn sigkill_mid_pipeline(fleet: &mut PipelineFleet) -> eyre::Result<String> {
    let given = [false, true, false, false];
    network_advancing(&fleet.urls())?;
    for (instance, on) in given.into_iter().enumerate() {
        fleet.wait_for_switch(instance, on).await?;
    }
    let load = Load::start(fleet.url(0));
    load.wait_flowing().await?;
    let victim_url = fleet.url(VICTIM);
    let mut kills = Vec::new();
    for (trigger, offset_ms) in KILL_POINTS {
        let mut tail =
            LogTail::at_end(node_log_path(fleet.test, VICTIM, fleet.runs[VICTIM], false))?;
        let after_output = wait_for_trigger(&mut tail, trigger).await?;
        tokio::time::sleep(Duration::from_millis(offset_ms)).await;
        let finalized = block_number_at(&victim_url, "finalized")?;
        let latest = block_number_at(&victim_url, "latest")?;
        fleet.stop(VICTIM, true)?;
        kills.push(Kill {
            trigger,
            offset_ms,
            after_output,
            finalized,
            latest,
            outputs_in_flight: 0,
        });
        fleet.wait_for_progress(get_block_number(&fleet.url(0))?, 3).await?;
        fleet.restart(VICTIM)?;
        fleet.wait_for_switch(VICTIM, true).await?;
        fleet.wait_caught_up(VICTIM).await?;
    }
    let sent = load.finish()?;
    let head = fleet.wait_for_common_head().await?;
    assert_same_chain(&fleet.urls(), head)?;

    // what each kill caught: the consensus outputs between the victim's finalized and latest
    // block, read from node 0 now that the lost blocks were rebuilt identically
    for kill in &mut kills {
        let mut roots = BTreeSet::new();
        for number in kill.finalized + 1..=kill.latest {
            roots.insert(output_of_block(&fleet.url(0), number)?);
        }
        kill.outputs_in_flight = roots.len();
    }
    let caught = kills.iter().map(Kill::summary).collect::<Vec<_>>().join("; ");
    eyre::ensure!(
        kills.iter().all(|kill| kill.outputs_in_flight <= 2),
        "a kill found more than two outputs executed past the finalized block: {caught}"
    );
    eyre::ensure!(
        kills.iter().any(|kill| kill.outputs_in_flight == 2),
        "no kill landed with one output committing and the next built: {caught}"
    );

    assert_switch_lines(fleet, given)?;
    assert_no_forbidden_lines(fleet)?;
    // the restarts themselves: no ERROR line about consistency, and every ERROR line counted
    let mut restart_errors = 0;
    for run in 1..=fleet.runs[VICTIM] {
        for line in node_log_lines(fleet.test, VICTIM, run)? {
            if line.contains(" ERROR ") {
                restart_errors += 1;
                eyre::ensure!(
                    !line.to_lowercase().contains("consisten"),
                    "restart run {run} of the victim reported a consistency error: {line}"
                );
            }
        }
    }
    let lines = assert_engine_lines(fleet, VICTIM, true, true)?;
    for instance in [0, 2, 3] {
        assert_engine_lines(fleet, instance, false, false)?;
    }
    Ok(format!(
        "{} kills, {sent} transfers, {head} blocks identical, {restart_errors} ERROR lines in \
         the victim's restart runs; kills: {caught}; victim {}",
        kills.len(),
        lines.summary()
    ))
}
