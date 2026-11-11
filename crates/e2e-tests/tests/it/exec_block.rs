//! A fast executor is certified under `quorum_exec_block`, and a fleet with the bench-6 switches on
//! only some of its nodes stays one chain (issue #1804).
//!
//! Both tests start four validators as processes and drive transfers through every node. The
//! fleet helpers here ([`Fleet`], [`Load`], [`HeaderWindow`]) are shared with `epoch_drain.rs`.

use super::common::{
    address_from_word, call_rpc, get_block, get_block_number, get_key, log_field, log_line_matches,
    network_advancing, node_log_lines, scrape_metric_value, send_tel,
    start_validator_with_overrides, ProcessGuard,
};
use alloy::{primitives::utils::parse_ether, providers::ProviderBuilder};
use e2e_tests::{
    config_local_testnet, config_local_testnet_with_gc_depth_and_epoch_duration, FleetOverrides,
};
use eyre::WrapErr as _;
use jsonrpsee::rpc_params;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
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

/// The validator that executes at full speed in [`test_fast_executor_is_certified`].
const FAST_NODE: usize = 0;

/// The validators whose engines sleep before every output with batches; 2f+1 = 3 of the four, so
/// they certify each other's headers without [`FAST_NODE`], the shape of bench 5 at f = 1.
const SLOW_NODES: [usize; 3] = [1, 2, 3];

/// Environment variable of the engine's execution-delay test hook
/// (`tn_types::test_hooks::execution_delay`).
pub(crate) const EXECUTION_DELAY_ENV: &str = "TN_TEST_EXECUTION_DELAY_MS";

/// Milliseconds each of [`SLOW_NODES`] sleeps before it executes an output with batches.
///
/// At [`PRODUCTION_CADENCE`] a round lasts about 0.9 s and an output commits every two rounds,
/// about 1.8 s. The delay is longer than a round, so a slow voter reaches the block a fresh fast
/// header names only after the fast node has moved on, and shorter than the commit interval, so
/// the slow nodes keep pace and trail the fast node by one or two outputs, inside the quorum rule's
/// floor of eight ring entries. A delay longer than the commit interval makes the slow nodes' lag
/// grow without bound; past the floor the fix cannot help by design (measured at the quick cadence:
/// the adaptive arm then certified 0.59 of the fast node's headers, nearly all named at the floor).
const SLOW_EXECUTION_DELAY_MS: u64 = 1750;

/// How long each arm of [`test_fast_executor_is_certified`] measures under load: about 100 rounds.
const MEASURE_WINDOW: Duration = Duration::from_secs(90);

/// Pause of each load thread before every transfer: about 20 transfers a second per node, so a
/// worker seals about one batch per [`PRODUCTION_CADENCE`] batch delay and no header is proposed
/// early on a batch count.
const EXEC_LOAD_GAP: Duration = Duration::from_millis(50);

/// Time limit for the first transfers of every load thread to be accepted.
const LOAD_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Time limit for every node to execute up to the highest head seen when the load stopped.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

/// Time limit for a node's `node switches` line to appear once it was started.
const SWITCHES_LINE_TIMEOUT: Duration = Duration::from_secs(60);

/// The prometheus counter of the vote requests a voter refused because its execution had not
/// reached the header's block.
const REFUSED_BEHIND: &str = "tn_primary_votes_refused_execution_behind_total";

/// The error a voter logs when a header names a block its own execution contradicts: the canary
/// the proposer fix must keep alive and silent.
pub(crate) const CANARY_LOG: &str = "unexpected execution result received";

/// The error the final-state check of `epoch_record_state_check` logs before it halts the node.
pub(crate) const HALT_LOG: &str =
    "stored epoch record's final state is not this node's closing block";

/// The error a task manager logs when its tracking channel is full (issue #1806).
pub(crate) const TRACK_FAILED_LOG: &str = "Failed to track spawned task";

/// The passphrase [`start_validator_with_overrides`] gives every node.
const PASSPHRASE: &str = "restart_test";

/// The bench-6 switches in this tree that the mixed fleet turns on, as `parameters.yaml` entries.
///
/// `sealed_tx_keep`, `rpc_admission_backlog_outputs` and `pipelined_persist` are left at their
/// defaults: their code is not complete in this tree.
pub(crate) const BENCH6_SWITCHES_ON: [(&str, &str); 9] = [
    ("quorum_exec_block", "true"),
    ("quorum_exec_block_starved_rounds", "4"),
    ("vote_queue_fix", "true"),
    ("mdbx_random_advice", "true"),
    ("digest_dedupe", "true"),
    ("recovery_threads", "3"),
    ("epoch_close_task_drain", "true"),
    ("epoch_record_state_check", "true"),
    ("parent_grace", "150ms"),
];

/// Every switch at its default, as the `node switches` line renders it.
pub(crate) const BENCH6_SWITCHES_OFF: [(&str, &str); 12] = [
    ("quorum_exec_block", "false"),
    ("quorum_exec_block_starved_rounds", "0"),
    ("vote_queue_fix", "false"),
    ("mdbx_random_advice", "false"),
    ("sealed_tx_keep", "false"),
    ("digest_dedupe", "false"),
    ("recovery_threads", "0"),
    ("epoch_close_task_drain", "false"),
    ("epoch_record_state_check", "false"),
    ("parent_grace", "0s"),
    ("rpc_admission_backlog_outputs", "0"),
    ("pipelined_persist", "false"),
];

/// One arm of [`test_fast_executor_is_certified`]: the switches every node gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecArm {
    /// Every switch off: the base's proposer and voter.
    Control,
    /// `quorum_exec_block` with `quorum_exec_block_starved_rounds: 4` and `vote_queue_fix`.
    Adaptive,
    /// `quorum_exec_block` with `quorum_exec_block_starved_rounds: 0` and `vote_queue_fix`.
    Always,
}

impl ExecArm {
    /// The test-log directory of the arm.
    fn test_name(self) -> &'static str {
        match self {
            Self::Control => "fast_executor_control",
            Self::Adaptive => "fast_executor_adaptive",
            Self::Always => "fast_executor_always",
        }
    }

    /// The `parameters.yaml` entries every node gets.
    fn switches(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Control => &[],
            Self::Adaptive => &[
                ("quorum_exec_block", "true"),
                ("quorum_exec_block_starved_rounds", "4"),
                ("vote_queue_fix", "true"),
            ],
            Self::Always => &[
                ("quorum_exec_block", "true"),
                ("quorum_exec_block_starved_rounds", "0"),
                ("vote_queue_fix", "true"),
            ],
        }
    }

    /// What the `node switches` line of every node must show.
    fn expected_switches(self) -> Vec<(&'static str, &'static str)> {
        let given = self.switches();
        BENCH6_SWITCHES_OFF
            .iter()
            .map(|(key, default)| {
                given.iter().find(|(k, _)| k == key).copied().unwrap_or((*key, *default))
            })
            .collect()
    }
}

/// The node's own defaults for its header and batch cadence, in place of the e2e harness's quick
/// one (headers every 250 to 500 ms, batches every 250 ms).
///
/// At the quick cadence a round lasts about 200 ms and a slow voter must trail the fast node by
/// more than that to miss its header, yet by less than the commit interval of about 400 ms to keep
/// up: a margin a loaded host does not keep. At about a second a round, both margins are about half
/// a second.
const PRODUCTION_CADENCE: [(&str, &str); 3] =
    [("min_header_delay", "1s"), ("max_header_delay", "2500ms"), ("max_batch_delay", "1s")];

/// The most own headers, as a share of those it proposed in the window, the fast validator may
/// certify in the control for the run to show the starvation the fix removes (measured 0.61 to
/// 0.74).
///
/// The control cannot starve the fast node completely while the slow nodes keep pace: an output
/// commits every second round, and the fast header proposed a round after a commit names a block
/// the slow voters reach within that round.
const MAX_CONTROL_SHARE: f64 = 0.85;

/// The fewest own headers, as a share of those it proposed in the window, every validator must
/// certify under the `always` policy, and the slow validators in every arm (measured 0.98 to 1.0).
const MIN_CERTIFIED_SHARE: f64 = 0.93;

/// The fewest own headers, as a share of those it proposed in the window, the fast validator must
/// certify under the adaptive policy (measured 0.922, 0.931 and 0.923 in three runs; 0.81 to 0.82
/// when one certified tip header reset the window). It must also certify more than in the control
/// of the same run.
///
/// Below [`MIN_CERTIFIED_SHARE`] and the bench-6 target of 0.98 by construction at this scale: the
/// window opens with the load, and the climb from `quorum_exec_block_starved_rounds` (4) to the
/// bound of 128 rounds takes five failed returns to the own tip, each costing one or two headers
/// (7 or 8 in the three runs, all of them in the climb). That is 5 to 10 of the window's ~100
/// headers; once at the bound the node loses 2 headers every 130 rounds. The margin below the
/// measured shares allows the full 10 on a loaded machine that proposes fewer headers.
const MIN_ADAPTIVE_SHARE: f64 = 0.88;

/// The fewest of the headers built under the quorum rule (`exec_policy=starved`) that must certify
/// (measured all of them).
const MIN_STARVED_SHARE: f64 = 0.95;

/// The least share of the fast node's headers in the second half of the window that must name the
/// floor under the `always` policy.
const MIN_FLOOR_SHARE: f64 = 0.5;

/// A validator that executes every output at full speed is certified when three of four
/// validators execute late, once the proposer names a block a quorum has executed (issue #1804).
///
/// [`SLOW_NODES`] sleep [`SLOW_EXECUTION_DELAY_MS`] before every output with batches, so under load
/// the fast node's own tip runs ahead of what 2f+1 voters have executed. Three arms, each a fresh
/// network with transfers through all four nodes for [`MEASURE_WINDOW`]:
///
/// - control, every switch off: the bench-5 signature at this scale. The fast node names its own
///   tip, every slow voter refuses some of its requests as execution behind (its
///   `tn_primary_votes_refused_execution_behind_total` grows) and the three slow nodes certify each
///   round without it, so the fast node certifies at most [`MAX_CONTROL_SHARE`] of its own headers
///   while the slow nodes certify at least [`MIN_CERTIFIED_SHARE`]. A control that does not starve
///   fails the test: without it the other arms prove nothing;
/// - `quorum_exec_block` + `quorum_exec_block_starved_rounds: 4` + `vote_queue_fix`: the fast node
///   certifies at least [`MIN_ADAPTIVE_SHARE`] of its headers and more than in the control (the
///   gain measured 0.20 to 0.30), at least [`MIN_STARVED_SHARE`] of its headers built under the
///   quorum rule certify, its `header proposed` lines show both `exec_policy=tip` (the returns to
///   its own tip) and `exec_policy=starved` (the windows a cancelled tip header opens), and the
///   slow nodes certify at least [`MIN_CERTIFIED_SHARE`];
/// - `quorum_exec_block_starved_rounds: 0` (`always`): every node certifies at least
///   [`MIN_CERTIFIED_SHARE`], every fast-node header is `exec_policy=always`, and at least
///   [`MIN_FLOOR_SHARE`] of its headers in the second half of the window are `exec_basis=floor`.
///
/// Every node runs at [`PRODUCTION_CADENCE`] under light transfers ([`EXEC_LOAD_GAP`]) through all
/// four nodes. In every arm each node's `node switches` line shows the switches it was given, no
/// voter logs the execution canary [`CANARY_LOG`], and all four nodes hold the same blocks.
#[test]
#[ignore = "should not run with a default cargo test: starts three four-validator networks"]
fn test_fast_executor_is_certified() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let mut control_share = None;
    for arm in [ExecArm::Control, ExecArm::Adaptive, ExecArm::Always] {
        let (report, fast_share) = run_exec_arm(arm, control_share)
            .wrap_err(format!("arm {arm:?}: check logs in test_logs/{}/", arm.test_name()))?;
        eprintln!("{}: {report}", arm.test_name());
        control_share = control_share.or(Some(fast_share));
    }
    Ok(())
}

/// Start one arm's network, measure it under load and check it; `control_share` is the fast
/// node's certified share in the control. Returns the arm's summary line and the fast node's share.
fn run_exec_arm(arm: ExecArm, control_share: Option<f64>) -> eyre::Result<(String, f64)> {
    let test = arm.test_name();
    let slow = SLOW_NODES.iter().map(|i| format!("validator-{}", i + 1));
    let overrides = arm.switches().iter().fold(
        FleetOverrides::default().env(
            slow,
            EXECUTION_DELAY_ENV,
            SLOW_EXECUTION_DELAY_MS.to_string(),
        ),
        |fleet, (key, value)| fleet.parameter(all_validators(), *key, *value),
    );
    let overrides = PRODUCTION_CADENCE
        .iter()
        .fold(overrides, |fleet, (key, value)| fleet.parameter(all_validators(), *key, *value));
    let mut fleet = Fleet::start(test, overrides, None)?;
    let result = measure_exec_arm(arm, &fleet, control_share);
    fleet.guard.kill_all();
    result
}

/// The measuring part of [`run_exec_arm`], with the fleet already started.
fn measure_exec_arm(
    arm: ExecArm,
    fleet: &Fleet,
    control_share: Option<f64>,
) -> eyre::Result<(String, f64)> {
    let test = fleet.test.as_str();
    network_advancing(&fleet.urls)?;
    for node in 0..4 {
        assert_switches(test, node, 0, &arm.expected_switches())?;
    }
    let load = Load::start(&fleet.urls, EXEC_LOAD_GAP);
    let window = load.wait_started().and_then(|()| {
        // the window opens once the fast node has proposed a header with batches after the load
        // started: the slow nodes then have outputs to execute late
        let marks = fleet.log_marks()?;
        wait_until_blocking(
            LOAD_START_TIMEOUT,
            "the fast node proposes a header with batches",
            || {
                Ok(node_log_lines(test, FAST_NODE, 0)?[marks[FAST_NODE]..].iter().any(|line| {
                    log_line_matches(line, "header proposed", &[])
                        && log_field(line, "num_batches").is_some_and(|n| n > 0)
                }))
            },
        )?;
        let start = fleet.log_marks()?;
        std::thread::sleep(MEASURE_WINDOW);
        Ok((start, fleet.log_marks()?))
    });
    let sent = load.stop()?;
    let (start, end) = window?;
    let refused = SLOW_NODES
        .iter()
        .map(|&i| scrape_metric_value(&fleet.metrics[i], REFUSED_BEHIND))
        .collect::<eyre::Result<Vec<f64>>>()?;
    fleet.settle()?;
    let windows = (0..4)
        .map(|node| HeaderWindow::read(test, node, start[node], end[node]))
        .collect::<eyre::Result<Vec<_>>>()?;
    let fast = &windows[FAST_NODE];
    let shares = windows.iter().map(HeaderWindow::certified_share).collect::<Vec<_>>();
    let fast_share = shares[FAST_NODE];
    let slow_certified = SLOW_NODES.iter().all(|&i| shares[i] >= MIN_CERTIFIED_SHARE);
    let slow = SLOW_NODES.iter().map(|&i| windows[i].describe()).collect::<Vec<_>>();
    let report = format!(
        "{sent} transfers sent; certified share per node {shares:.3?}; fast node: {}; slow \
         voters refused {refused:?} vote requests as execution behind; slow nodes: {slow:?}",
        fast.describe()
    );

    for node in 0..4 {
        fleet.assert_no_log(node, 0, CANARY_LOG)?;
    }
    fleet.assert_blocks_same()?;
    eyre::ensure!(
        slow_certified,
        "a slow validator certified less than {MIN_CERTIFIED_SHARE} of its own headers; {report}"
    );
    match arm {
        ExecArm::Control => {
            eyre::ensure!(
                fast_share <= MAX_CONTROL_SHARE && refused.iter().all(|r| *r > 0.0),
                "the control did not reproduce the bench-5 starvation: the fast node certified \
                 {fast_share:.3} of its headers (at most {MAX_CONTROL_SHARE} expected) and the \
                 slow voters refused {refused:?} requests as execution behind (each must refuse \
                 some); {report}"
            );
        }
        ExecArm::Adaptive => {
            let control = control_share.unwrap_or(1.0);
            eyre::ensure!(
                fast_share >= MIN_ADAPTIVE_SHARE && fast_share > control,
                "the fast node certified {fast_share:.3} of its headers with the adaptive proposer \
                 fix on: at least {MIN_ADAPTIVE_SHARE} and more than the control's {control:.3} \
                 expected; {report}"
            );
            let starved = fast.policy_certified_share("starved");
            eyre::ensure!(
                starved >= MIN_STARVED_SHARE,
                "only {starved:.3} of the fast node's headers built under the quorum rule \
                 certified (at least {MIN_STARVED_SHARE}); {report}"
            );
            eyre::ensure!(
                fast.policy("tip") > 0 && fast.policy("starved") > 0,
                "the fast node's headers did not show both exec_policy=tip and \
                 exec_policy=starved; {report}"
            );
        }
        ExecArm::Always => {
            eyre::ensure!(
                fast_share >= MIN_CERTIFIED_SHARE,
                "the fast node certified {fast_share:.3} of its headers with the always proposer \
                 fix on (at least {MIN_CERTIFIED_SHARE}); {report}"
            );
            eyre::ensure!(
                fast.policy("always") == fast.headers.len(),
                "the fast node proposed headers under another policy than exec_policy=always; \
                 {report}"
            );
            let floor = fast.late_basis_share("floor");
            eyre::ensure!(
                floor >= MIN_FLOOR_SHARE,
                "the fast node's exec_basis did not settle on floor: {floor:.3} of its headers in \
                 the second half of the window (at least {MIN_FLOOR_SHARE}); {report}"
            );
        }
    }
    Ok((report, fast_share))
}

/// The datadir names of the four validators.
pub(crate) fn all_validators() -> impl Iterator<Item = String> {
    (1..=4).map(|i| format!("validator-{i}"))
}

/// Four validators started as processes for one test, with an RPC and a metrics endpoint each.
pub(crate) struct Fleet {
    /// The test-log directory name under `test_logs/`.
    pub(crate) test: String,
    /// The fleet's datadirs; removed when the fleet drops.
    _temp: tempfile::TempDir,
    /// The path of [`Self::_temp`].
    temp_path: PathBuf,
    /// Each node's HTTP RPC url.
    pub(crate) urls: [String; 4],
    /// Each node's RPC port, kept for restarts.
    rpc_ports: [u16; 4],
    /// Each node's metrics address.
    pub(crate) metrics: [String; 4],
    /// The settings each node is started with.
    overrides: FleetOverrides,
    /// The node processes.
    pub(crate) guard: ProcessGuard,
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
        let bin = e2e_tests::get_telcoin_network_binary();
        let args = ["--metrics", self.metrics[instance].as_str()];
        start_validator_with_overrides(
            instance,
            bin,
            &self.temp_path,
            self.rpc_ports[instance],
            &self.test,
            run,
            &args,
            &self.overrides,
        )
    }

    /// The number of lines in each node's run-0 log: marks to measure a window from.
    pub(crate) fn log_marks(&self) -> eyre::Result<[usize; 4]> {
        let mut marks = [0; 4];
        for (node, mark) in marks.iter_mut().enumerate() {
            *mark = node_log_lines(&self.test, node, 0)?.len();
        }
        Ok(marks)
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

    /// Every node holds the same block at node 0's head (the assertion of `test_blocks_same` in
    /// `restarts.rs`) and at up to [`BLOCK_SAMPLES`] heights spread below it.
    pub(crate) fn assert_blocks_same(&self) -> eyre::Result<u64> {
        let head = get_block_number(&self.urls[0])?;
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

/// Wait until node `node`'s run-`run` log of `test` has a `node switches` line, then check that it
/// shows every `(key, value)` of `expected`. A duration is compared by value.
pub(crate) fn assert_switches(
    test: &str,
    node: usize,
    run: u32,
    expected: &[(&str, &str)],
) -> eyre::Result<()> {
    let find = |lines: Vec<String>| lines.into_iter().find(|l| l.contains("node switches"));
    wait_until_blocking(SWITCHES_LINE_TIMEOUT, "a `node switches` line", || {
        Ok(find(node_log_lines(test, node, run)?).is_some())
    })
    .wrap_err_with(|| format!("node {node} run {run} of {test}"))?;
    let line = find(node_log_lines(test, node, run)?)
        .ok_or_else(|| eyre::eyre!("node {node} run {run} lost its `node switches` line"))?;
    for (key, value) in expected {
        let shown = log_str_field(&line, key)
            .ok_or_else(|| eyre::eyre!("node {node} run {run} shows no `{key}` in: {line}"))?;
        let same = match (duration_ms(value), duration_ms(shown)) {
            (Some(want), Some(got)) => want == got,
            _ => shown == *value,
        };
        eyre::ensure!(same, "node {node} run {run} shows {key}={shown}, it was given {value}");
    }
    Ok(())
}

/// Milliseconds of a one-token humantime duration such as `150ms` or `2s`, `None` otherwise.
fn duration_ms(text: &str) -> Option<u64> {
    match text.strip_suffix("ms") {
        Some(ms) => ms.parse().ok(),
        None => text.strip_suffix('s')?.parse::<u64>().ok().map(|s| s * 1000),
    }
}

/// The value of the `name=value` field of a plain log line, without the quotes of a string field.
fn log_str_field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(name)?.strip_prefix('='))
        .map(|value| value.trim_matches('"'))
}

/// One `header proposed` line.
#[derive(Clone, Debug)]
struct ProposedHeader {
    /// `(epoch, round)`.
    at: (u64, u64),
    /// `exec_policy`.
    policy: String,
    /// `exec_basis`.
    basis: String,
    /// `exec_tip - exec_block`, in block numbers.
    lag: u64,
}

/// A node's own headers in a window of its log: proposed, certified and cancelled.
#[derive(Debug)]
pub(crate) struct HeaderWindow {
    /// The `header proposed` lines in the window, in order.
    headers: Vec<ProposedHeader>,
    /// `(epoch, round)` of every `quorum reached` line from the window's start to the log's end,
    /// so a header proposed just before the window ended still counts once it certified.
    certified: BTreeSet<(u64, u64)>,
    /// `header uncertified` lines with `reason="canceled"` in the window.
    cancelled: usize,
}

impl HeaderWindow {
    /// Read node `node`'s run-0 log of `test` between the line marks `start` and `end`.
    pub(crate) fn read(test: &str, node: usize, start: usize, end: usize) -> eyre::Result<Self> {
        let lines = node_log_lines(test, node, 0)?;
        let window = lines.get(start..end.min(lines.len())).unwrap_or_default();
        let at = |line: &str| Some((log_field(line, "epoch")?, log_field(line, "round")?));
        let headers = window
            .iter()
            .filter(|line| line.contains("header proposed"))
            .filter_map(|line| {
                Some(ProposedHeader {
                    at: at(line)?,
                    policy: log_str_field(line, "exec_policy").unwrap_or("absent").to_owned(),
                    basis: log_str_field(line, "exec_basis").unwrap_or("absent").to_owned(),
                    lag: log_field(line, "exec_tip")?
                        .saturating_sub(log_field(line, "exec_block")?),
                })
            })
            .collect();
        let certified = lines
            .get(start..)
            .unwrap_or_default()
            .iter()
            .filter(|line| line.contains("quorum reached"))
            .filter_map(|line| at(line))
            .collect();
        let cancelled = window
            .iter()
            .filter(|line| line.contains("header uncertified") && line.contains("canceled"))
            .count();
        Ok(Self { headers, certified, cancelled })
    }

    /// The share of the distinct rounds proposed in the window that certified.
    pub(crate) fn certified_share(&self) -> f64 {
        let proposed = self.headers.iter().map(|h| h.at).collect::<BTreeSet<_>>();
        if proposed.is_empty() {
            return 0.0;
        }
        proposed.intersection(&self.certified).count() as f64 / proposed.len() as f64
    }

    /// The share of the distinct rounds proposed in the window under `exec_policy=<policy>` that
    /// certified; 1 when there are none.
    fn policy_certified_share(&self, policy: &str) -> f64 {
        let proposed = self
            .headers
            .iter()
            .filter(|h| h.policy == policy)
            .map(|h| h.at)
            .collect::<BTreeSet<_>>();
        if proposed.is_empty() {
            return 1.0;
        }
        proposed.intersection(&self.certified).count() as f64 / proposed.len() as f64
    }

    /// How many headers in the window carry `exec_policy=<policy>`.
    fn policy(&self, policy: &str) -> usize {
        self.headers.iter().filter(|h| h.policy == policy).count()
    }

    /// The share of the second half of the window's headers that carry `exec_basis=<basis>`.
    fn late_basis_share(&self, basis: &str) -> f64 {
        let late = &self.headers[self.headers.len() / 2..];
        if late.is_empty() {
            return 0.0;
        }
        late.iter().filter(|h| h.basis == basis).count() as f64 / late.len() as f64
    }

    /// A one-line account of the window for a test's summary.
    pub(crate) fn describe(&self) -> String {
        let count = |pick: fn(&ProposedHeader) -> &str| {
            self.headers.iter().fold(BTreeMap::<&str, usize>::new(), |mut counts, h| {
                *counts.entry(pick(h)).or_default() += 1;
                counts
            })
        };
        let max_lag = self.headers.iter().map(|h| h.lag).max().unwrap_or_default();
        format!(
            "{} headers proposed, {:.3} certified, {} cancelled, exec_policy {:?}, exec_basis \
             {:?}, max exec_tip-exec_block {max_lag}",
            self.headers.len(),
            self.certified_share(),
            self.cancelled,
            count(|h| h.policy.as_str()),
            count(|h| h.basis.as_str()),
        )
    }
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
    pub(crate) fn stop(self) -> eyre::Result<u64> {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads {
            thread.join().map_err(|_| eyre::eyre!("a load thread panicked"))?;
        }
        Ok(self.sent.iter().map(|sent| sent.load(Ordering::Relaxed)).sum())
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
/// block with the recorded hash (see `common::assert_epoch_records_verify`).
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
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(super::common::assert_epoch_records_verify(&endpoints, epochs, 60))
}

/// How long [`test_mixed_switch_fleet_is_one_chain`]'s epochs last, in seconds.
const MIXED_EPOCH_SECS: u32 = 20;

/// The garbage-collection depth of [`test_mixed_switch_fleet_is_one_chain`].
const MIXED_GC_DEPTH: u32 = 50;

/// The switched-on node [`test_mixed_switch_fleet_is_one_chain`] restarts.
const MIXED_RESTARTED: usize = 1;

/// The epoch [`test_mixed_switch_fleet_is_one_chain`] runs to; the restart falls in epoch 1, so
/// at least one boundary closes with the restarted node back in the fleet.
const MIXED_LAST_EPOCH: u32 = 3;

/// A fleet in which two validators run every bench-6 switch in this tree and two run none stays one
/// chain under load, across epoch boundaries and a restart of a switched-on node.
///
/// The benchmark flips switches by rolling restart, so the fleet is mixed for minutes at every arm
/// change. Validators 1 and 2 (instances 0 and 1) get [`BENCH6_SWITCHES_ON`], validators 3 and 4
/// nothing. Under transfers through all four nodes the test waits for epoch 1, stops instance
/// [`MIXED_RESTARTED`], lets the other three advance ten blocks, restarts it with the same
/// switches, waits for it to catch up and then for epoch [`MIXED_LAST_EPOCH`] on every node.
/// Every node's `node switches` line (both runs of the restarted node) shows what it was given; the
/// chain advances while the node is down and after it returns; every node serves a certified record
/// for every closed epoch and holds its final block with the recorded hash; no node logs the
/// execution canary or the final-state halt; and all four hold the same blocks.
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_mixed_switch_fleet_is_one_chain() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let test = "mixed_switch_fleet";
    let switched = ["validator-1", "validator-2"];
    let overrides =
        BENCH6_SWITCHES_ON.iter().fold(FleetOverrides::default(), |fleet, (key, value)| {
            fleet.parameter(switched, *key, *value)
        });
    let mut fleet = Fleet::start(test, overrides, Some((MIXED_GC_DEPTH, MIXED_EPOCH_SECS)))?;
    let load = Load::start(&fleet.urls, Duration::ZERO);
    let checked = check_mixed_fleet(&mut fleet, &load);
    let sent = load.stop();
    let report = checked.and_then(|report| {
        // after the load: every node executes to the same head and holds the same blocks
        fleet.settle()?;
        let head = fleet.assert_blocks_same()?;
        for (node, run) in [(0, 0), (1, 0), (1, 1), (2, 0), (3, 0)] {
            fleet.assert_no_log(node, run, CANARY_LOG)?;
            fleet.assert_no_log(node, run, HALT_LOG)?;
            if node < 2 {
                // the switched-on nodes drain their task tracker through every close
                fleet.assert_no_log(node, run, TRACK_FAILED_LOG)?;
            }
        }
        Ok(format!("{report}, all four hold the same blocks up to {head}"))
    });
    fleet.guard.kill_all();
    let report = report.wrap_err(format!("mixed fleet is not one chain: test_logs/{test}/"))?;
    eprintln!("{test}: {report}, {} transfers sent", sent?);
    Ok(())
}

/// The checks of [`test_mixed_switch_fleet_is_one_chain`] while its load runs.
fn check_mixed_fleet(fleet: &mut Fleet, load: &Load) -> eyre::Result<String> {
    let test = fleet.test.clone();
    network_advancing(&fleet.urls)?;
    let on = BENCH6_SWITCHES_OFF
        .iter()
        .map(|(key, default)| {
            BENCH6_SWITCHES_ON.iter().find(|(k, _)| k == key).copied().unwrap_or((*key, *default))
        })
        .collect::<Vec<_>>();
    for node in 0..4 {
        let expected: &[(&str, &str)] = if node < 2 { &on } else { &BENCH6_SWITCHES_OFF };
        assert_switches(&test, node, 0, expected)?;
    }
    load.wait_started()?;
    let epoch_wait = Duration::from_secs(u64::from(MIXED_EPOCH_SECS) * 6);
    wait_for_epoch_on_all(&fleet.urls, 1, epoch_wait)?;

    // a rolling restart of a switched-on node: down while the others commit, then back
    let mut stopped = fleet
        .guard
        .take(MIXED_RESTARTED)
        .ok_or_else(|| eyre::eyre!("node {MIXED_RESTARTED} is not running"))?;
    super::common::kill_child(&mut stopped);
    let others = [0, 2, 3];
    let down_at = get_block_number(&fleet.urls[0])?;
    wait_until_blocking(
        Duration::from_secs(60),
        "the three running nodes advance ten blocks while one is down",
        || {
            Ok(others
                .iter()
                .all(|&i| get_block_number(&fleet.urls[i]).is_ok_and(|h| h >= down_at + 10)))
        },
    )?;
    let restarted = fleet.spawn(MIXED_RESTARTED, 1)?;
    fleet.guard.replace(MIXED_RESTARTED, restarted);
    assert_switches(&test, MIXED_RESTARTED, 1, &on)?;
    let back_at = get_block_number(&fleet.urls[0])?;
    wait_until_blocking(
        Duration::from_secs(120),
        &format!("the restarted node executes block {back_at}"),
        || Ok(get_block_number(&fleet.urls[MIXED_RESTARTED]).is_ok_and(|h| h >= back_at)),
    )?;
    let epoch = wait_for_epoch_on_all(&fleet.urls, MIXED_LAST_EPOCH, epoch_wait)?;
    assert_epoch_records(&fleet.urls, 0..=MIXED_LAST_EPOCH - 1)?;
    let after = get_block_number(&fleet.urls[MIXED_RESTARTED])?;
    eyre::ensure!(
        after > back_at,
        "the chain did not advance after the restart: node {MIXED_RESTARTED} at {after}, node 0 \
         was at {back_at} when it came back"
    );
    Ok(format!(
        "node {MIXED_RESTARTED} stopped at block {down_at}, back at {back_at}, epoch {epoch} \
         reached with the restarted node at block {after}"
    ))
}
