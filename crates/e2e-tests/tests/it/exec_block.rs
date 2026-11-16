//! A fast executor is certified under `quorum_exec_block`, and a fleet with the runtime switches on
//! only some of its nodes stays one chain (issue #1804).
//!
//! Both tests start four validators as processes and drive transfers through every node, with the
//! fleet helpers of [`super::fleet`].

use super::{
    common::{
        get_block_number, log_field, log_line_matches, network_advancing, node_log_lines,
        scrape_metric_value,
    },
    fleet::{
        all_validators, assert_epoch_records, assert_switches, expected_switches, log_str_field,
        node_epoch, wait_for_epoch_on_all, Fleet, Load, CANARY_LOG, EXECUTION_DELAY_ENV,
        LOAD_START_TIMEOUT, PRODUCTION_CADENCE, SLOW_EXECUTION_DELAY_MS, STACK_SWITCHES_ON,
    },
};
use e2e_tests::FleetOverrides;
use eyre::WrapErr as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tn_test_utils::wait_until_blocking;

/// The validator that executes at full speed in [`test_fast_executor_is_certified`].
const FAST_NODE: usize = 0;

/// The validators whose engines sleep before every output with batches; 2f+1 = 3 of the four, so
/// they certify each other's headers without [`FAST_NODE`], the shape of bench 5 at f = 1.
const SLOW_NODES: [usize; 3] = [1, 2, 3];

/// How long each arm of [`test_fast_executor_is_certified`] measures under load: about 100 rounds.
const MEASURE_WINDOW: Duration = Duration::from_secs(90);

/// Pause of each load thread before every transfer: about 20 transfers a second per node, so a
/// worker seals about one batch per [`PRODUCTION_CADENCE`] batch delay and no header is proposed
/// early on a batch count.
const EXEC_LOAD_GAP: Duration = Duration::from_millis(50);

/// The prometheus counter of the vote requests a voter refused because its execution had not
/// reached the header's block.
const REFUSED_BEHIND: &str = "tn_primary_votes_refused_execution_behind_total";

/// One arm of [`test_fast_executor_is_certified`]: the switches every node gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecArm {
    /// Every switch off: the base's proposer.
    Control,
    /// `quorum_exec_block` with `quorum_exec_block_starved_rounds: 4`.
    Adaptive,
    /// `quorum_exec_block` with `quorum_exec_block_starved_rounds: 0`.
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
            Self::Adaptive => {
                &[("quorum_exec_block", "true"), ("quorum_exec_block_starved_rounds", "4")]
            }
            Self::Always => {
                &[("quorum_exec_block", "true"), ("quorum_exec_block_starved_rounds", "0")]
            }
        }
    }

    /// What the `node switches` line of every node must show.
    fn expected_switches(self) -> eyre::Result<Vec<(&'static str, &'static str)>> {
        expected_switches(self.switches())
    }
}

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

/// The fewest headers every node must propose in the window for its shares to measure anything:
/// a node that proposed none would read as certifying none, and pass the control's starvation
/// check without starving. [`MEASURE_WINDOW`] holds about 100 rounds at [`PRODUCTION_CADENCE`].
const MIN_WINDOW_HEADERS: usize = 20;

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
/// - `quorum_exec_block` + `quorum_exec_block_starved_rounds: 4`: the fast node certifies at least
///   [`MIN_ADAPTIVE_SHARE`] of its headers and more than in the control (the gain measured 0.20 to
///   0.30), at least [`MIN_STARVED_SHARE`] of its headers built under the quorum rule certify, its
///   `header proposed` lines show both `exec_policy=tip` (the returns to its own tip) and
///   `exec_policy=starved` (the windows a cancelled tip header opens), and the slow nodes certify
///   at least [`MIN_CERTIFIED_SHARE`];
/// - `quorum_exec_block_starved_rounds: 0` (`always`): every node certifies at least
///   [`MIN_CERTIFIED_SHARE`], every fast-node header is `exec_policy=always`, and at least
///   [`MIN_FLOOR_SHARE`] of its headers in the second half of the window are `exec_basis=floor`.
///
/// Every node runs at [`PRODUCTION_CADENCE`] under light transfers ([`EXEC_LOAD_GAP`]) through all
/// four nodes. In every arm each node's `node switches` line shows every switch with the value it
/// was given, every node proposes at least [`MIN_WINDOW_HEADERS`] headers in the window, no voter
/// logs the execution canary [`CANARY_LOG`], no node logs a line of [`FATAL_LOGS`], and all four
/// nodes hold the same blocks. Every arm is measured even when an earlier one misses a threshold,
/// so a failure prints all three arms' numbers.
#[test]
#[ignore = "should not run with a default cargo test: starts three four-validator networks"]
fn test_fast_executor_is_certified() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let mut control_share = None;
    let mut failed = Vec::new();
    for arm in [ExecArm::Control, ExecArm::Adaptive, ExecArm::Always] {
        let logs = format!("test_logs/{}/", arm.test_name());
        let measured = run_exec_arm(arm).wrap_err(format!("arm {arm:?}: check logs in {logs}"))?;
        eprintln!("{}: {}", arm.test_name(), measured.report);
        if let Err(e) = measured.check(arm, control_share) {
            eprintln!("{}: FAILED: {e}", arm.test_name());
            failed.push(format!("arm {arm:?} ({logs}): {e}"));
        }
        control_share = control_share.or(Some(measured.shares[FAST_NODE]));
    }
    eyre::ensure!(failed.is_empty(), "{} of 3 arms missed a threshold: {failed:#?}", failed.len());
    Ok(())
}

/// Start one arm's network, measure it under load and stop it.
fn run_exec_arm(arm: ExecArm) -> eyre::Result<ArmMeasure> {
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
    let result = measure_exec_arm(arm, &fleet);
    fleet.guard.kill_all();
    result
}

/// What one arm of [`test_fast_executor_is_certified`] measured.
struct ArmMeasure {
    /// Each node's own headers in the window, by instance.
    windows: Vec<HeaderWindow>,
    /// Each node's [`HeaderWindow::certified_share`], by instance.
    shares: Vec<f64>,
    /// How much [`REFUSED_BEHIND`] grew on each of [`SLOW_NODES`] over the window.
    refused: Vec<f64>,
    /// The arm's summary line.
    report: String,
}

impl ArmMeasure {
    /// Check the arm's thresholds; `control_share` is the fast node's certified share in the
    /// control.
    fn check(&self, arm: ExecArm, control_share: Option<f64>) -> eyre::Result<()> {
        let Self { windows, shares, refused, report } = self;
        let fast = &windows[FAST_NODE];
        let fast_share = shares[FAST_NODE];
        let few = windows
            .iter()
            .enumerate()
            .filter(|(_, w)| w.headers.len() < MIN_WINDOW_HEADERS)
            .map(|(node, w)| (node, w.headers.len()))
            .collect::<Vec<_>>();
        eyre::ensure!(
            few.is_empty(),
            "nodes proposed fewer than {MIN_WINDOW_HEADERS} headers in the window (node, \
             headers): {few:?}; {report}"
        );
        eyre::ensure!(
            SLOW_NODES.iter().all(|&i| shares[i] >= MIN_CERTIFIED_SHARE),
            "a slow validator certified less than {MIN_CERTIFIED_SHARE} of its own headers; \
             {report}"
        );
        match arm {
            ExecArm::Control => {
                eyre::ensure!(
                    fast.policy("off") == fast.headers.len(),
                    "the fast node proposed headers under another policy than exec_policy=off \
                     with every switch off; {report}"
                );
                eyre::ensure!(
                    fast_share <= MAX_CONTROL_SHARE && refused.iter().all(|r| *r > 0.0),
                    "the control did not reproduce the bench-5 starvation: the fast node \
                     certified {fast_share:.3} of its headers (at most {MAX_CONTROL_SHARE} \
                     expected) and the slow voters refused {refused:?} requests as execution \
                     behind (each must refuse some); {report}"
                );
            }
            ExecArm::Adaptive => {
                let control = control_share.unwrap_or(1.0);
                eyre::ensure!(
                    fast_share >= MIN_ADAPTIVE_SHARE && fast_share > control,
                    "the fast node certified {fast_share:.3} of its headers with the adaptive \
                     proposer fix on: at least {MIN_ADAPTIVE_SHARE} and more than the control's \
                     {control:.3} expected; {report}"
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
                    "the fast node certified {fast_share:.3} of its headers with the always \
                     proposer fix on (at least {MIN_CERTIFIED_SHARE}); {report}"
                );
                eyre::ensure!(
                    fast.policy("always") == fast.headers.len(),
                    "the fast node proposed headers under another policy than exec_policy=always; \
                     {report}"
                );
                let floor = fast.late_basis_share("floor");
                eyre::ensure!(
                    floor >= MIN_FLOOR_SHARE,
                    "the fast node's exec_basis did not settle on floor: {floor:.3} of its \
                     headers in the second half of the window (at least {MIN_FLOOR_SHARE}); \
                     {report}"
                );
            }
        }
        Ok(())
    }
}

/// The measuring part of [`run_exec_arm`], with the fleet already started: the window under load,
/// then the checks that end the arm at once (switches, canary, fatal lines, one chain).
fn measure_exec_arm(arm: ExecArm, fleet: &Fleet) -> eyre::Result<ArmMeasure> {
    let test = fleet.test.as_str();
    network_advancing(&fleet.urls)?;
    let expected = arm.expected_switches()?;
    for node in 0..4 {
        assert_switches(test, node, 0, &expected)?;
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
        let refused_before = fleet.slow_refusals()?;
        std::thread::sleep(MEASURE_WINDOW);
        let end = fleet.log_marks()?;
        let refused = fleet
            .slow_refusals()?
            .iter()
            .zip(refused_before)
            .map(|(after, before)| after - before)
            .collect::<Vec<_>>();
        Ok((start, end, refused))
    });
    let sent = load.stop()?;
    let (start, end, refused) = window?;
    fleet.settle()?;
    let windows = (0..4)
        .map(|node| HeaderWindow::read(test, node, start[node], end[node]))
        .collect::<eyre::Result<Vec<_>>>()?;
    let shares = windows.iter().map(HeaderWindow::certified_share).collect::<Vec<_>>();
    let lags = windows.iter().map(HeaderWindow::max_lag).collect::<Vec<_>>();
    let slow = SLOW_NODES.iter().map(|&i| windows[i].describe()).collect::<Vec<_>>();
    let report = format!(
        "{sent} transfers sent; certified share per node {shares:.3?}; max exec_tip-exec_block \
         per node {lags:?}; fast node: {}; slow voters refused {refused:?} vote requests as \
         execution behind in the window; slow nodes: {slow:?}",
        windows[FAST_NODE].describe()
    );

    let one_chain = (0..4)
        .try_for_each(|node| {
            fleet.assert_no_log(node, 0, CANARY_LOG)?;
            fleet.assert_no_fatal_log(node, 0)
        })
        .and_then(|()| fleet.assert_blocks_same().map(drop));
    one_chain.wrap_err_with(|| report.clone())?;
    Ok(ArmMeasure { windows, shares, refused, report })
}

impl Fleet {
    /// [`REFUSED_BEHIND`] on each of [`SLOW_NODES`] now.
    fn slow_refusals(&self) -> eyre::Result<Vec<f64>> {
        SLOW_NODES.iter().map(|&i| scrape_metric_value(&self.metrics[i], REFUSED_BEHIND)).collect()
    }
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
        Self::read_run(test, node, 0, start, end)
    }

    /// Read node `node`'s run-`run` log of `test` between the line marks `start` and `end`.
    fn read_run(test: &str, node: usize, run: u32, start: usize, end: usize) -> eyre::Result<Self> {
        let lines = node_log_lines(test, node, run)?;
        let window = lines.get(start..end.min(lines.len())).unwrap_or_default();
        let at = |line: &str| Some((log_field(line, "epoch")?, log_field(line, "round")?));
        let header = |line: &str| {
            Some(ProposedHeader {
                at: at(line)?,
                policy: log_str_field(line, "exec_policy")?.to_owned(),
                basis: log_str_field(line, "exec_basis")?.to_owned(),
                lag: log_field(line, "exec_tip")?.saturating_sub(log_field(line, "exec_block")?),
            })
        };
        // every field is read by name: a line that lacks one fails the read, so a renamed field
        // cannot shrink the window to the lines that still match
        let headers = window
            .iter()
            .filter(|line| line.contains("header proposed"))
            .map(|line| {
                header(line).ok_or_else(|| {
                    eyre::eyre!(
                        "node {node} run {run} of {test}: a `header proposed` line lacks epoch, \
                         round, exec_policy, exec_basis, exec_tip or exec_block: {line}"
                    )
                })
            })
            .collect::<eyre::Result<_>>()?;
        let certified = lines
            .get(start..)
            .unwrap_or_default()
            .iter()
            .filter(|line| {
                line.contains("quorum reached") && log_field(line, "quorum_threshold").is_some()
            })
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

    /// How many distinct rounds proposed in the window named a block older than the node's own tip
    /// (`exec_basis=quorum` or `exec_basis=floor`) and certified.
    fn older_block_certified(&self) -> usize {
        self.headers
            .iter()
            .filter(|h| h.basis == "quorum" || h.basis == "floor")
            .map(|h| h.at)
            .collect::<BTreeSet<_>>()
            .intersection(&self.certified)
            .count()
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

    /// The largest `exec_tip - exec_block` of the window's headers, 0 when there are none.
    pub(crate) fn max_lag(&self) -> u64 {
        self.headers.iter().map(|h| h.lag).max().unwrap_or_default()
    }

    /// A one-line account of the window for a test's summary.
    pub(crate) fn describe(&self) -> String {
        let count = |pick: fn(&ProposedHeader) -> &str| {
            self.headers.iter().fold(BTreeMap::<&str, usize>::new(), |mut counts, h| {
                *counts.entry(pick(h)).or_default() += 1;
                counts
            })
        };
        let max_lag = self.max_lag();
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

/// How long [`test_mixed_switch_fleet_is_one_chain`]'s epochs last, in seconds.
const MIXED_EPOCH_SECS: u32 = 20;

/// The garbage-collection depth of [`test_mixed_switch_fleet_is_one_chain`].
const MIXED_GC_DEPTH: u32 = 50;

/// The switched-on node [`test_mixed_switch_fleet_is_one_chain`] restarts.
const MIXED_RESTARTED: usize = 1;

/// The epoch [`test_mixed_switch_fleet_is_one_chain`] runs to at least; the restart starts in
/// epoch 1, and the test runs one epoch past the one the restarted node came back in, so at least
/// one boundary closes with it back in the fleet.
const MIXED_LAST_EPOCH: u32 = 3;

/// The instances [`test_mixed_switch_fleet_is_one_chain`] gives every switch of
/// [`STACK_SWITCHES_ON`]; the other two run every switch at its default and execute late.
const MIXED_SWITCHED: [usize; 2] = [0, 1];

/// The fewest certified headers of the switched nodes in [`test_mixed_switch_fleet_is_one_chain`]
/// that must name a block older than their own tip (`exec_basis=quorum` or `floor`): such a header
/// certifies only with a vote from a switch-off node, so the fleet has run the case that differs
/// from a uniform one. Without the late switch-off nodes a run proposed 4 such headers in about
/// 1,000, too few to count on; with them a run certified 43.
const MIN_MIXED_OLDER_CERTIFIED: usize = 1;

/// A fleet in which two validators run every runtime switch of the node and two run none stays one
/// chain under load, across epoch boundaries and a restart of a switched-on node.
///
/// The benchmark flips switches by rolling restart, so the fleet is mixed for minutes at every arm
/// change. Validators 1 and 2 (instances [`MIXED_SWITCHED`]) get [`STACK_SWITCHES_ON`], validators
/// 3 and 4 nothing, and every node runs at [`PRODUCTION_CADENCE`]. The switch-off nodes sleep
/// [`SLOW_EXECUTION_DELAY_MS`] before every output with batches, so the switched nodes' tips run
/// ahead of them and the quorum rule names older blocks, which a switch-off voter must accept for
/// the header to certify. Under transfers through all four nodes the test waits for epoch 1, stops
/// instance [`MIXED_RESTARTED`], lets the other three advance ten blocks, restarts it with the same
/// switches, waits for it to catch up and then for epoch [`MIXED_LAST_EPOCH`] (or one past the
/// epoch the node came back in) on every node.
///
/// Every node's `node switches` line (both runs of the restarted node) shows every switch with the
/// value it was given, and its `header proposed` lines show the proposer policy that follows from
/// it; at least [`MIN_MIXED_OLDER_CERTIFIED`] certified switched-node headers named an older block;
/// the chain advances while the node is down and after it returns; every node serves the same
/// certified record for every closed epoch and holds its final block with the recorded hash; no
/// node counts an epoch-record conflict, logs the execution canary or a line of [`FATAL_LOGS`];
/// and all four hold the same blocks.
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_mixed_switch_fleet_is_one_chain() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let test = "mixed_switch_fleet";
    let switched = MIXED_SWITCHED.map(|i| format!("validator-{}", i + 1));
    let late = (0..4)
        .filter(|i| !MIXED_SWITCHED.contains(i))
        .map(|i| format!("validator-{}", i + 1))
        .collect::<Vec<_>>();
    let overrides = STACK_SWITCHES_ON.iter().fold(
        FleetOverrides::default().env(
            late,
            EXECUTION_DELAY_ENV,
            SLOW_EXECUTION_DELAY_MS.to_string(),
        ),
        |fleet, (key, value)| fleet.parameter(switched.clone(), *key, *value),
    );
    let overrides = PRODUCTION_CADENCE
        .iter()
        .fold(overrides, |fleet, (key, value)| fleet.parameter(all_validators(), *key, *value));
    let mut fleet = Fleet::start(test, overrides, Some((MIXED_GC_DEPTH, MIXED_EPOCH_SECS)))?;
    let load = Load::start(&fleet.urls, Duration::ZERO);
    let checked = check_mixed_fleet(&mut fleet, &load);
    let sent = load.stop();
    let report = checked.and_then(|report| {
        // after the load: every node executes to the same head and holds the same blocks
        fleet.settle()?;
        let head = fleet.assert_blocks_same()?;
        let mut proposed = Vec::new();
        let mut older_certified = 0;
        for (node, run) in [(0, 0), (1, 0), (1, 1), (2, 0), (3, 0)] {
            fleet.assert_no_log(node, run, CANARY_LOG)?;
            fleet.assert_no_fatal_log(node, run)?;
            let switched = MIXED_SWITCHED.contains(&node);
            let window = assert_header_policies(&fleet.test, node, run, switched)?;
            if switched {
                older_certified += window.older_block_certified();
            }
            proposed.push(format!("node {node} run {run}: {}", window.describe()));
        }
        for node in 0..4 {
            fleet.assert_no_record_conflicts(node)?;
        }
        eyre::ensure!(
            older_certified >= MIN_MIXED_OLDER_CERTIFIED,
            "only {older_certified} certified headers of the switched nodes named a quorum or \
             floor block (at least {MIN_MIXED_OLDER_CERTIFIED}): the fleet never ran a switch-off \
             voter on an older block; headers {proposed:#?}"
        );
        Ok(format!(
            "{report}, all four hold the same blocks up to {head}, {older_certified} certified \
             headers of the switched nodes named a quorum or floor block, headers {proposed:#?}"
        ))
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
    let on = expected_switches(&STACK_SWITCHES_ON)?;
    let off = expected_switches(&[])?;
    for node in 0..4 {
        let expected = if MIXED_SWITCHED.contains(&node) { &on } else { &off };
        assert_switches(&test, node, 0, expected)?;
    }
    load.wait_started()?;
    let epoch_wait = Duration::from_secs(u64::from(MIXED_EPOCH_SECS) * 6);
    wait_for_epoch_on_all(&fleet.urls, 1, epoch_wait)?;

    // a rolling restart of a switched-on node: down while the others commit, then back
    // the restarted node's counter starts over with its new process: read it before the stop
    fleet.assert_no_record_conflicts(MIXED_RESTARTED)?;
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
    // the restart may end in a later epoch than the first: run on until a boundary closes with
    // the restarted node back in the committee
    let back_epoch = node_epoch(&fleet.urls[MIXED_RESTARTED])?;
    let last = MIXED_LAST_EPOCH.max(back_epoch + 1);
    let epoch = wait_for_epoch_on_all(&fleet.urls, last, epoch_wait)?;
    assert_epoch_records(&fleet.urls, 0..=last - 1)?;
    let after = get_block_number(&fleet.urls[MIXED_RESTARTED])?;
    eyre::ensure!(
        after > back_at,
        "the chain did not advance after the restart: node {MIXED_RESTARTED} at {after}, node 0 \
         was at {back_at} when it came back"
    );
    Ok(format!(
        "node {MIXED_RESTARTED} stopped at block {down_at}, back at {back_at} in epoch \
         {back_epoch}, epoch {epoch} reached with the restarted node at block {after}, records of \
         epochs 0..={} agree on every node",
        last - 1
    ))
}

/// Every `header proposed` line in node `node`'s run-`run` log of `test` shows the proposer policy
/// its switches give it: never `exec_policy=off` when `switched` (the node runs
/// `quorum_exec_block` with the [`STACK_SWITCHES_ON`] count), always `exec_policy=off` otherwise.
/// Returns the run's headers.
fn assert_header_policies(
    test: &str,
    node: usize,
    run: u32,
    switched: bool,
) -> eyre::Result<HeaderWindow> {
    let window = HeaderWindow::read_run(test, node, run, 0, usize::MAX)?;
    let (off, total) = (window.policy("off"), window.headers.len());
    eyre::ensure!(total > 0, "node {node} run {run} proposed no header");
    eyre::ensure!(
        if switched { off == 0 } else { off == total },
        "node {node} run {run} (switched on: {switched}) proposed headers under another \
         exec_policy than expected: {}",
        window.describe()
    );
    Ok(window)
}
