//! Fleet helpers the stack's e2e tests share.
//!
//! [`Fleet`] starts four validators as processes, each with an RPC and a metrics endpoint, and
//! [`Load`] drives transfers through every node. [`assert_switches`] checks the `node switches`
//! line of a node against [`expected_switches`], which reads the switch lists here, and
//! [`Fleet::assert_no_fatal_log`] checks a node's log for the lines of [`FATAL_LOGS`].
//! [`node_epoch`], [`wait_for_epoch_on_all`] and [`assert_epoch_records`] read the epochs and their
//! certified records.

use super::common::{
    address_from_word, call_rpc, get_block, get_block_number, get_key, node_log_lines, send_tel,
    start_validator_with_overrides, ProcessGuard,
};
use alloy::{primitives::utils::parse_ether, providers::ProviderBuilder};
use e2e_tests::{
    config_local_testnet, config_local_testnet_with_gc_depth_and_epoch_duration, FleetOverrides,
    TestBinary,
};
use eyre::WrapErr as _;
use jsonrpsee::rpc_params;
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tn_test_utils::wait_until_blocking;
use tn_types::{get_available_tcp_port, Address, GenesisAccount, U256};

/// Time limit for the first transfers of every load thread to be accepted.
const LOAD_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Time limit for every node to execute up to the highest head seen when the load stopped.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

/// Time limit for a node's `node switches` line to appear once it was started.
const SWITCHES_LINE_TIMEOUT: Duration = Duration::from_secs(60);

/// The error the replay guard of `write_epoch_record` logs before it stops the node: a stored
/// epoch record whose `final_state` is not the closing block this node executed.
pub(crate) const HALT_LOG: &str = "stored epoch record final_state does not match this node's \
                                   closing block: this node's execution diverged from the \
                                   recorded chain, stopping the node";

/// The stable start of the error the epoch-record collector logs when a certified record of the
/// network differs from this node's own record of the epoch; [`CONFLICT_STOP_LOG`] follows it.
pub(crate) const CONFLICT_LOG: &str = "epoch record conflict:";

/// The error the epoch-record collector logs as it stops the node on a certified record that
/// conflicts with this node's own.
pub(crate) const CONFLICT_STOP_LOG: &str = "a certified epoch record from peers conflicts with \
                                            this node's own record for the epoch: this node's \
                                            execution diverged from the certified chain, \
                                            stopping the node";

/// The error the leftover-output drain at an epoch close logs before it stops the node: an output
/// that does not follow the last one forwarded to the engine.
pub(crate) const LEFTOVER_GAP_LOG: &str = "leftover consensus output does not follow the last \
                                           output forwarded to the engine: stopping the node \
                                           before the engine executes past the gap";

/// The error the closing-block check logs before it stops the node: an epoch record's
/// `final_state` is not the block where the epoch-closing calls ran for its `final_consensus`.
pub(crate) const CLOSING_BLOCK_HALT_LOG: &str =
    "epoch record final_state is not the block where the epoch-closing calls ran";

/// Every error a node logs as it stops because its chain or its epoch records went wrong; no test
/// here passes while a node logged one of them.
pub(crate) const FATAL_LOGS: [&str; 5] =
    [HALT_LOG, CONFLICT_LOG, CONFLICT_STOP_LOG, LEFTOVER_GAP_LOG, CLOSING_BLOCK_HALT_LOG];

/// The passphrase [`start_validator_with_overrides`] gives every node.
pub(crate) const PASSPHRASE: &str = "restart_test";

/// Every runtime switch of the node at its default, as the `node switches` line renders it, in the
/// order of the node's `runtime_switches!` list (`crates/config/src/node.rs`).
///
/// A switch that arrives later adds its entry here, in that order.
pub(crate) const STACK_SWITCHES_OFF: [(&str, &str); 3] = [
    ("epoch_record_recertify", "false"),
    ("epoch_record_collect_past_uncertified", "false"),
    ("vote_queue_fix", "false"),
];

/// What the `node switches` line of a node given the switch entries `given` must show: every
/// switch of [`STACK_SWITCHES_OFF`], with the value of `given` where it names the switch.
///
/// An entry of `given` that names no switch of the list is an error: the node would ignore the key
/// and the test would check nothing for it.
pub(crate) fn expected_switches(
    given: &[(&'static str, &'static str)],
) -> eyre::Result<Vec<(&'static str, &'static str)>> {
    let unknown = given
        .iter()
        .filter(|(key, _)| STACK_SWITCHES_OFF.iter().all(|(k, _)| k != key))
        .collect::<Vec<_>>();
    eyre::ensure!(unknown.is_empty(), "entries {unknown:?} name no switch of STACK_SWITCHES_OFF");
    Ok(STACK_SWITCHES_OFF
        .iter()
        .map(|(key, default)| {
            given.iter().find(|(k, _)| k == key).copied().unwrap_or((*key, *default))
        })
        .collect())
}

/// The datadir names of the four validators.
pub(crate) fn all_validators() -> impl Iterator<Item = String> {
    (1..=4).map(|i| format!("validator-{i}"))
}

/// Four validators started as processes for one test, with an RPC and a metrics endpoint each.
pub(crate) struct Fleet {
    /// The node processes; declared first so a drop stops them before their datadirs go.
    pub(crate) guard: ProcessGuard,
    /// The test-log directory name under `test_logs/`.
    pub(crate) test: String,
    /// The fleet's datadirs; removed when the fleet drops.
    _temp: tempfile::TempDir,
    /// The path of [`Self::_temp`].
    pub(crate) temp_path: PathBuf,
    /// Each node's HTTP RPC url.
    pub(crate) urls: [String; 4],
    /// Each node's RPC port, kept for restarts.
    rpc_ports: [u16; 4],
    /// Each node's metrics address.
    pub(crate) metrics: [String; 4],
    /// The settings each node is started with.
    overrides: FleetOverrides,
    /// The node binary, resolved before the genesis is built.
    bin: &'static TestBinary,
}

impl Fleet {
    /// Configure four validators, fund the load accounts of [`Load`] and start every node with
    /// `overrides`. `gc_and_epoch` sets the garbage-collection depth and the epoch duration in
    /// seconds; `None` keeps the genesis defaults.
    pub(crate) fn start(
        test: &str,
        overrides: FleetOverrides,
        gc_and_epoch: Option<(u32, u32)>,
    ) -> eyre::Result<Self> {
        // resolved first: a build of the binary after the genesis would eat into epoch 0
        let bin = e2e_tests::get_telcoin_network_binary();
        let temp = tempfile::TempDir::with_prefix(test)?;
        let temp_path = temp.path().to_path_buf();
        let balance = U256::from(parse_ether("10_000_000")?);
        let funded = (0..4)
            .map(|i| {
                (
                    address_from_word(&Load::source(i)),
                    GenesisAccount::default().with_balance(balance),
                )
            })
            .collect::<Vec<_>>();
        let passphrase = Some(PASSPHRASE.to_string());
        match gc_and_epoch {
            Some((gc_depth, epoch_secs)) => config_local_testnet_with_gc_depth_and_epoch_duration(
                &temp_path,
                passphrase,
                Some(funded),
                gc_depth,
                epoch_secs,
            )?,
            None => config_local_testnet(&temp_path, passphrase, Some(funded))?,
        }
        let port = || get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no port"));
        let mut rpc_ports = [0; 4];
        let mut metrics: [String; 4] = Default::default();
        for (rpc_port, metrics) in rpc_ports.iter_mut().zip(metrics.iter_mut()) {
            *rpc_port = port()?;
            *metrics = format!("127.0.0.1:{}", port()?);
        }
        let urls = rpc_ports.map(|p| format!("http://127.0.0.1:{p}"));
        let mut fleet = Self {
            test: test.to_string(),
            _temp: temp,
            temp_path,
            urls,
            rpc_ports,
            metrics,
            overrides,
            bin,
            guard: ProcessGuard::empty(),
        };
        for i in 0..4 {
            let child = fleet.spawn(i, 0)?;
            fleet.guard.push(child);
        }
        Ok(fleet)
    }

    /// Spawn node `instance` as run `run`, with its overrides, RPC port and metrics address.
    pub(crate) fn spawn(&self, instance: usize, run: u32) -> eyre::Result<std::process::Child> {
        let args = ["--metrics", self.metrics[instance].as_str()];
        start_validator_with_overrides(
            instance,
            self.bin,
            &self.temp_path,
            self.rpc_ports[instance],
            &self.test,
            run,
            &args,
            &self.overrides,
        )
    }

    /// Wait until every node has executed up to the highest head any node reports now.
    pub(crate) fn settle(&self) -> eyre::Result<u64> {
        let mut target = 0;
        for url in &self.urls {
            target = target.max(get_block_number(url)?);
        }
        wait_until_blocking(
            SETTLE_TIMEOUT,
            &format!("every node executes block {target}"),
            || Ok(self.urls.iter().all(|url| get_block_number(url).is_ok_and(|h| h >= target))),
        )?;
        Ok(target)
    }

    /// Every node holds the same block at the lowest head of the four (the assertion of
    /// `test_blocks_same` in `restarts.rs`) and at up to [`BLOCK_SAMPLES`] heights spread below it.
    /// Returns that head.
    pub(crate) fn assert_blocks_same(&self) -> eyre::Result<u64> {
        let heads =
            self.urls.iter().map(|url| get_block_number(url)).collect::<Result<Vec<_>, _>>()?;
        let head = heads.iter().copied().min().unwrap_or_default();
        eyre::ensure!(head > 0, "a node is still at genesis: heads {heads:?}");
        let stride = usize::try_from((head / BLOCK_SAMPLES).max(1))?;
        for height in (1..=head).rev().step_by(stride) {
            let expected = block_hash(&self.urls[0], height)?;
            for (node, url) in self.urls.iter().enumerate().skip(1) {
                let hash = block_hash(url, height)?;
                eyre::ensure!(
                    hash == expected,
                    "block {height} differs between node 0 ({expected}) and node {node} ({hash})"
                );
            }
        }
        Ok(head)
    }

    /// Node `node`'s run-`run` log has no line containing `text`.
    pub(crate) fn assert_no_log(&self, node: usize, run: u32, text: &str) -> eyre::Result<()> {
        let hits = node_log_lines(&self.test, node, run)?
            .into_iter()
            .filter(|line| line.contains(text))
            .collect::<Vec<_>>();
        eyre::ensure!(
            hits.is_empty(),
            "node {node} run {run} logged {} `{text}` lines, the first: {:?}",
            hits.len(),
            hits.first()
        );
        Ok(())
    }

    /// Node `node`'s run-`run` log has no line of [`FATAL_LOGS`].
    pub(crate) fn assert_no_fatal_log(&self, node: usize, run: u32) -> eyre::Result<()> {
        FATAL_LOGS.iter().try_for_each(|text| self.assert_no_log(node, run, text))
    }
}

/// How many heights [`Fleet::assert_blocks_same`] compares at most.
const BLOCK_SAMPLES: u64 = 64;

/// The hash of block `height` on `url`.
fn block_hash(url: &str, height: u64) -> eyre::Result<String> {
    get_block(url, Some(height))?
        .get("hash")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| eyre::eyre!("block {height} on {url} has no hash"))
}

/// The message of the line a node logs once at start with every runtime switch and its value.
const SWITCHES_LOG: &str = "node switches";

/// Wait until node `node`'s run-`run` log of `test` has a `node switches` line, then check that it
/// shows every `(key, value)` of `expected` and no other switch. A duration is compared by value.
///
/// A switch the line shows that `expected` does not name fails the check: the switch lists here
/// must name every switch the node has, or a fleet meant to run all of them silently runs one at
/// its default.
pub(crate) fn assert_switches(
    test: &str,
    node: usize,
    run: u32,
    expected: &[(&str, &str)],
) -> eyre::Result<()> {
    let find = |lines: Vec<String>| lines.into_iter().find(|l| l.contains(SWITCHES_LOG));
    wait_until_blocking(SWITCHES_LINE_TIMEOUT, "a `node switches` line", || {
        Ok(find(node_log_lines(test, node, run)?).is_some())
    })
    .wrap_err_with(|| format!("node {node} run {run} of {test}"))?;
    let line = find(node_log_lines(test, node, run)?)
        .ok_or_else(|| eyre::eyre!("node {node} run {run} lost its `node switches` line"))?;
    let fields = line.split_once(SWITCHES_LOG).map_or("", |(_, fields)| fields);
    let shown = switch_fields(fields);
    for (key, value) in expected {
        let (_, shown) = shown
            .iter()
            .find(|(k, _)| k == key)
            .ok_or_else(|| eyre::eyre!("node {node} run {run} shows no `{key}` in: {line}"))?;
        let same = match (duration_nanos(value), duration_nanos(shown)) {
            (Some(want), Some(got)) => want == got,
            _ => shown == value,
        };
        eyre::ensure!(same, "node {node} run {run} shows {key}={shown}, it was given {value}");
    }
    let unlisted =
        shown.iter().filter(|(key, _)| expected.iter().all(|(k, _)| k != key)).collect::<Vec<_>>();
    eyre::ensure!(
        unlisted.is_empty(),
        "node {node} run {run} runs switches the lists here do not name: {unlisted:?}; add them to \
         STACK_SWITCHES_ON and STACK_SWITCHES_OFF"
    );
    Ok(())
}

/// The `key=value` fields of a `node switches` line after its message, each value running to the
/// next field, so a humantime duration printed in parts (`2s 500ms`) reads whole.
fn switch_fields(fields: &str) -> Vec<(String, String)> {
    let mut parsed: Vec<(String, String)> = Vec::new();
    for token in fields.split_whitespace() {
        match (token.split_once('='), parsed.last_mut()) {
            (Some((key, value)), _) => parsed.push((key.to_owned(), value.to_owned())),
            (None, Some((_, value))) => {
                value.push(' ');
                value.push_str(token);
            }
            (None, None) => {}
        }
    }
    parsed
}

/// Nanoseconds of a humantime duration made of `<number><unit>` parts (`150ms`, `2s 500ms`,
/// `1m`), `None` for any other text.
fn duration_nanos(text: &str) -> Option<u128> {
    const UNITS: [(&str, u128); 6] = [
        ("ns", 1),
        ("us", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60_000_000_000),
        ("h", 3_600_000_000_000),
    ];
    text.split_whitespace()
        .map(|part| {
            let digits = part.find(|c: char| !c.is_ascii_digit()).filter(|&at| at > 0)?;
            let (number, unit) = part.split_at(digits);
            let scale = UNITS.iter().find(|(name, _)| *name == unit)?.1;
            number.parse::<u128>().ok()?.checked_mul(scale)
        })
        .try_fold(0u128, |total, part| total.checked_add(part?))
        .filter(|_| !text.trim().is_empty())
}

/// The value of the `name=value` field of a plain log line, without the quotes of a string field.
pub(crate) fn log_str_field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(name)?.strip_prefix('='))
        .map(|value| value.trim_matches('"'))
}

/// Value transfers through every node from its own funded account, until stopped.
pub(crate) struct Load {
    /// Set to end the load.
    stop: Arc<AtomicBool>,
    /// Transfers accepted per node.
    sent: Vec<Arc<AtomicU64>>,
    /// The load threads.
    threads: Vec<JoinHandle<()>>,
}

impl Load {
    /// The word of the funded account that sends through node `node`.
    pub(crate) fn source(node: usize) -> String {
        format!("st-a-load-{node}")
    }

    /// Start one sending thread per url, each pausing `gap` before every send.
    ///
    /// A failed send resyncs the nonce from the pool, so a full per-sender pool slows the load to
    /// the rate of block inclusion instead of ending it.
    pub(crate) fn start(urls: &[String; 4], gap: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let mut sent = Vec::new();
        let mut threads = Vec::new();
        for (node, url) in urls.iter().enumerate() {
            let (stop, count, url) = (Arc::clone(&stop), Arc::new(AtomicU64::new(0)), url.clone());
            sent.push(Arc::clone(&count));
            threads.push(std::thread::spawn(move || {
                let key = get_key(&Load::source(node));
                let source = address_from_word(&Load::source(node));
                let sink = address_from_word("st-a-load-sink");
                let mut nonce = 0u128;
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(gap);
                    if send_tel(&url, &key, sink, 1, 250, 21000, nonce).is_ok() {
                        count.fetch_add(1, Ordering::Relaxed);
                        nonce += 1;
                    } else {
                        std::thread::sleep(Duration::from_millis(100));
                        nonce = pending_nonce(&url, source).unwrap_or(nonce);
                    }
                }
            }));
        }
        Self { stop, sent, threads }
    }

    /// Wait until every thread has had a transfer accepted.
    pub(crate) fn wait_started(&self) -> eyre::Result<()> {
        wait_until_blocking(LOAD_START_TIMEOUT, "every load thread has a transfer accepted", || {
            Ok(self.sent.iter().all(|sent| sent.load(Ordering::Relaxed) > 0))
        })
    }

    /// Stop the load and join its threads. Returns the transfers accepted.
    pub(crate) fn stop(mut self) -> eyre::Result<u64> {
        self.stop.store(true, Ordering::Relaxed);
        for thread in std::mem::take(&mut self.threads) {
            thread.join().map_err(|_| eyre::eyre!("a load thread panicked"))?;
        }
        Ok(self.sent.iter().map(|sent| sent.load(Ordering::Relaxed)).sum())
    }
}

impl Drop for Load {
    /// A load dropped without [`Load::stop`] (a panic, an early return) still ends its threads.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Pending nonce of `address` on `node`, from `eth_getTransactionCount`.
fn pending_nonce(node: &str, address: Address) -> eyre::Result<u128> {
    let count: String = call_rpc(
        node,
        "eth_getTransactionCount",
        rpc_params![address, "pending"],
        1,
        "eth_getTransactionCount",
    )?;
    u128::from_str_radix(count.trim_start_matches("0x"), 16)
        .wrap_err("eth_getTransactionCount returned a count that is not hex")
}

/// The epoch the `ConsensusRegistry` on `url` reports as current.
pub(crate) fn node_epoch(url: &str) -> eyre::Result<u32> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    Ok(runtime.block_on(super::common::current_epoch(&provider))?.epoch_id)
}

/// Wait until every url reports a current epoch of at least `target`, for up to `limit`; returns
/// the lowest epoch the nodes report then.
pub(crate) fn wait_for_epoch_on_all(
    urls: &[String; 4],
    target: u32,
    limit: Duration,
) -> eyre::Result<u32> {
    let deadline = Instant::now() + limit;
    loop {
        let epochs = urls.iter().map(|url| node_epoch(url)).collect::<Vec<_>>();
        if epochs.iter().all(|e| e.as_ref().is_ok_and(|e| *e >= target)) {
            return Ok(epochs.into_iter().filter_map(Result::ok).min().unwrap_or(target));
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "not every node reached epoch {target} within {limit:?}: {epochs:?}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Every node in `urls` serves a certified record for each epoch in `epochs` and holds its final
/// block with the recorded hash (see `common::assert_epoch_records_verify`), and all of them serve
/// the same record for each epoch.
pub(crate) fn assert_epoch_records(
    urls: &[String; 4],
    epochs: std::ops::RangeInclusive<u32>,
) -> eyre::Result<()> {
    let endpoints = urls
        .iter()
        .map(|url| e2e_tests::NodeEndpoints {
            http_url: url.clone(),
            ws_url: String::new(),
            ipc_path: String::new(),
        })
        .collect::<Vec<_>>();
    // multi-threaded: the record check reads blocks through `call_rpc`, which blocks in place
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        super::common::assert_epoch_records_verify(&endpoints, epochs.clone(), 60).await?;
        for epoch in epochs {
            let mut digests = Vec::new();
            for url in urls {
                let record = super::common::fetch_verified_epoch_record(url, epoch, 60).await?;
                digests.push(record.digest());
            }
            eyre::ensure!(
                digests.windows(2).all(|pair| pair[0] == pair[1]),
                "the nodes serve different certified records for epoch {epoch}: {digests:?}"
            );
        }
        Ok(())
    })
}
