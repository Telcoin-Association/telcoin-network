//! A fast executor is certified while three of four validators execute late, because every header
//! names an execution block a quorum of its parents' authors holds (issue #1804).
//!
//! The test starts four validators as processes and drives transfers through every node, with the
//! fleet helpers of [`super::fleet`].

use super::{
    common::{log_field, log_line_matches, network_advancing, node_log_lines, scrape_metric_value},
    fleet::{
        all_validators, log_str_field, Fleet, Load, CANARY_LOG, EXECUTION_DELAY_ENV,
        LOAD_START_TIMEOUT, PRODUCTION_CADENCE, SLOW_EXECUTION_DELAY_MS,
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

/// How long [`test_fast_executor_is_certified`] measures under load: about 100 rounds.
const MEASURE_WINDOW: Duration = Duration::from_secs(90);

/// Pause of each load thread before every transfer: about 20 transfers a second per node, so a
/// worker seals about one batch per [`PRODUCTION_CADENCE`] batch delay and no header is proposed
/// early on a batch count.
const EXEC_LOAD_GAP: Duration = Duration::from_millis(50);

/// The prometheus counter of the vote requests a voter refused because its execution had not
/// reached the header's block.
const REFUSED_BEHIND: &str = "tn_primary_votes_refused_execution_behind_total";

/// The fewest own headers, as a share of those it proposed in the window, every validator must
/// certify (measured 0.98 to 1.0).
const MIN_CERTIFIED_SHARE: f64 = 0.93;

/// The least share of the fast node's headers in the second half of the window that must name the
/// floor.
const MIN_FLOOR_SHARE: f64 = 0.5;

/// The fewest headers every node must propose in the window for its shares to measure anything:
/// a node that proposed none would read as certifying none. [`MEASURE_WINDOW`] holds about 100
/// rounds at [`PRODUCTION_CADENCE`].
const MIN_WINDOW_HEADERS: usize = 20;

/// A validator that executes every output at full speed is certified when three of four
/// validators execute late, because the proposer names a block a quorum has executed (issue
/// #1804).
///
/// [`SLOW_NODES`] sleep [`SLOW_EXECUTION_DELAY_MS`] before every output with batches, so under load
/// the fast node's own tip runs ahead of what 2f+1 voters have executed. A fresh network runs
/// transfers through all four nodes for [`MEASURE_WINDOW`]: every node certifies at least
/// [`MIN_CERTIFIED_SHARE`] of its own headers, and at least [`MIN_FLOOR_SHARE`] of the fast node's
/// headers in the second half of the window are `exec_basis=floor`.
///
/// Every node runs at [`PRODUCTION_CADENCE`] under light transfers ([`EXEC_LOAD_GAP`]) through all
/// four nodes. Every node proposes at least [`MIN_WINDOW_HEADERS`] headers in the window, no voter
/// logs the execution canary [`CANARY_LOG`], no node logs a line of [`FATAL_LOGS`], and all four
/// nodes hold the same blocks. The summary line also gives how many vote requests each slow voter
/// refused as execution behind ([`REFUSED_BEHIND`]) in the window.
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_fast_executor_is_certified() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let test = "fast_executor";
    let measured = run_fast_executor(test).and_then(|measured| {
        eprintln!("{test}: {}", measured.report);
        measured.check()
    });
    measured.wrap_err(format!("check logs in test_logs/{test}/"))
}

/// Start the network, measure it under load and stop it.
fn run_fast_executor(test: &'static str) -> eyre::Result<ExecMeasure> {
    let slow = SLOW_NODES.iter().map(|i| format!("validator-{}", i + 1));
    let overrides = FleetOverrides::default().env(
        slow,
        EXECUTION_DELAY_ENV,
        SLOW_EXECUTION_DELAY_MS.to_string(),
    );
    let overrides = PRODUCTION_CADENCE
        .iter()
        .fold(overrides, |fleet, (key, value)| fleet.parameter(all_validators(), *key, *value));
    let mut fleet = Fleet::start(test, overrides, None)?;
    let result = measure_fast_executor(&fleet);
    fleet.guard.kill_all();
    result
}

/// What [`test_fast_executor_is_certified`] measured.
struct ExecMeasure {
    /// Each node's own headers in the window, by instance.
    windows: Vec<HeaderWindow>,
    /// Each node's [`HeaderWindow::certified_share`], by instance.
    shares: Vec<f64>,
    /// The run's summary line.
    report: String,
}

impl ExecMeasure {
    /// Check the thresholds.
    fn check(&self) -> eyre::Result<()> {
        let Self { windows, shares, report } = self;
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
        eyre::ensure!(
            fast_share >= MIN_CERTIFIED_SHARE,
            "the fast node certified {fast_share:.3} of its headers (at least \
             {MIN_CERTIFIED_SHARE}); {report}"
        );
        let floor = fast.late_basis_share("floor");
        eyre::ensure!(
            floor >= MIN_FLOOR_SHARE,
            "the fast node's exec_basis did not settle on floor: {floor:.3} of its \
             headers in the second half of the window (at least {MIN_FLOOR_SHARE}); \
             {report}"
        );
        Ok(())
    }
}

/// The measuring part of [`run_fast_executor`], with the fleet already started: the window under
/// load, then the checks that end the run at once (canary, fatal lines, one chain).
fn measure_fast_executor(fleet: &Fleet) -> eyre::Result<ExecMeasure> {
    let test = fleet.test.as_str();
    network_advancing(&fleet.urls)?;
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
    Ok(ExecMeasure { windows, shares, report })
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
                         round, exec_basis, exec_tip or exec_block: {line}"
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
            "{} headers proposed, {:.3} certified, {} cancelled, exec_basis {:?}, max \
             exec_tip-exec_block {max_lag}",
            self.headers.len(),
            self.certified_share(),
            self.cancelled,
            count(|h| h.basis.as_str()),
        )
    }
}
