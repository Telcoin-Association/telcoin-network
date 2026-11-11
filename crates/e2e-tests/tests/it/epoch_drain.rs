//! A loaded epoch close with a slow committee member (issue #1806).
//!
//! Four validators run under transfers through every node while one of them executes each output
//! late, so at the close it still has a backlog of committed outputs to execute: the window in
//! which the closing epoch's task manager stopped reading its tracking channel on bench 5.

use super::{
    common::{log_field, network_advancing, node_log_lines, scrape_metric_value},
    exec_block::{
        all_validators, assert_epoch_records, assert_switches, wait_for_epoch_on_all, Fleet, Load,
        BENCH6_SWITCHES_OFF, EXECUTION_DELAY_ENV, HALT_LOG, TRACK_FAILED_LOG,
    },
};
use e2e_tests::FleetOverrides;
use eyre::WrapErr as _;
use std::time::Duration;
use tn_test_utils::wait_until_blocking;

/// The validator whose engine sleeps before every output with batches.
const SLOW_MEMBER: usize = 3;

/// Milliseconds [`SLOW_MEMBER`] sleeps before it executes an output with batches, as in the
/// slow-execution restart test.
///
/// Above the commit interval under load (about three outputs a second), so the slow member's
/// backlog grows for as long as the load runs and it reaches the close well after the closing
/// output committed.
const SLOW_MEMBER_DELAY_MS: u64 = 500;

/// The garbage-collection depth (an activity window of 15 rounds), as in the slow-execution test.
const DRAIN_GC_DEPTH: u32 = 25;

/// Epoch duration in seconds: the load runs through most of epoch 0 before its close.
const DRAIN_EPOCH_SECS: u32 = 40;

/// Time limit for every node to enter epoch 1: the epoch, then the slow member's drain of its
/// backlog (one [`SLOW_MEMBER_DELAY_MS`] per output) and its catch-up.
const NEXT_EPOCH_TIMEOUT: Duration = Duration::from_secs(300);

/// Time limit for every node to log the close of epoch 0 once all of them are in epoch 1.
const DRAINED_LINE_TIMEOUT: Duration = Duration::from_secs(60);

/// The least time, from its closing output's commit to the end of its drain (`detect_lag_ms` +
/// `drain_ms` of its `epoch close drained` line), the slow member must take to close epoch 0 for
/// the close to count as loaded.
///
/// The backlog shows in the detection lag, not in `backlog_outputs`: the closing output reaches the
/// boundary handler only after the outputs before it went to the engine, so by detection the slow
/// member has one output left (measured at a 1-s delay: a 95-s detection lag, a backlog of 1 and a
/// 1.4-s drain).
const MIN_SLOW_CLOSE_MS: u64 = 5_000;

/// The gauge of how many rounds the newest verified certificate led while a node's execution was
/// one epoch behind (`tn_primary_epoch_entry_rounds_behind`); reported, not asserted.
const ENTRY_ROUNDS_BEHIND: &str = "tn_primary_epoch_entry_rounds_behind";

/// The switches of the drain arm.
const DRAIN_SWITCHES: [(&str, &str); 2] =
    [("epoch_close_task_drain", "true"), ("epoch_record_state_check", "true")];

/// An epoch close under load with one slow member completes on every node (issue #1806).
///
/// [`SLOW_MEMBER`] sleeps [`SLOW_MEMBER_DELAY_MS`] before every output with batches, so under the
/// transfers through all four nodes its backlog grows until the close of epoch 0, which it finishes
/// at least [`MIN_SLOW_CLOSE_MS`] after the closing output committed. Two arms, each a fresh
/// network:
///
/// - drain, `epoch_close_task_drain` and `epoch_record_state_check` on every node: the close of
///   epoch 0 completes everywhere, every node logs `epoch close drained` for it, no node logs
///   `Failed to track spawned task` or the final-state halt, every node enters epoch 1 and serves a
///   certified record for epoch 0 whose final block it holds, and all four hold the same blocks;
/// - control, every switch off: the same close completes everywhere, as it does on the base, with
///   the same record and block checks.
///
/// What it does not show: that the overflow is fixed. The tracking channel holds 1,000 handles and
/// bench 5 needed a loaded ten-node fleet to fill it. Here the control logs no `Failed to track
/// spawned task` either: the slow member's backlog drains before it detects the close (a detection
/// lag of about a minute), and the close itself, the window in which the base stops reading the
/// channel, lasts about half a second. The test pins "no regression, the drain line appears and the
/// final-state check stays quiet"; the summary line reports each node's count.
#[test]
#[ignore = "should not run with a default cargo test: starts two four-validator networks"]
fn test_loaded_close_with_a_slow_member() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    for drain in [true, false] {
        let test = if drain { "loaded_close_drain" } else { "loaded_close_control" };
        let report = run_close_arm(test, drain)
            .wrap_err(format!("arm {test}: check logs in test_logs/{test}/"))?;
        eprintln!("{test}: {report}");
    }
    Ok(())
}

/// Start one arm's network, load it across the close of epoch 0 and check the close.
fn run_close_arm(test: &str, drain: bool) -> eyre::Result<String> {
    let switches: &[(&str, &str)] = if drain { &DRAIN_SWITCHES } else { &[] };
    let overrides = switches.iter().fold(
        FleetOverrides::default().env(
            [format!("validator-{}", SLOW_MEMBER + 1)],
            EXECUTION_DELAY_ENV,
            SLOW_MEMBER_DELAY_MS.to_string(),
        ),
        |fleet, (key, value)| fleet.parameter(all_validators(), *key, *value),
    );
    let mut fleet = Fleet::start(test, overrides, Some((DRAIN_GC_DEPTH, DRAIN_EPOCH_SECS)))?;
    let load = Load::start(&fleet.urls, Duration::ZERO);
    let checked = check_close(&fleet, &load, drain);
    let sent = load.stop();
    let report = checked.and_then(|report| {
        fleet.settle()?;
        let head = fleet.assert_blocks_same()?;
        Ok(format!("{report}; all four hold the same blocks up to {head}"))
    });
    fleet.guard.kill_all();
    Ok(format!("{}, {} transfers sent", report?, sent?))
}

/// The checks of one arm of [`test_loaded_close_with_a_slow_member`] while its load runs.
fn check_close(fleet: &Fleet, load: &Load, drain: bool) -> eyre::Result<String> {
    let test = fleet.test.as_str();
    network_advancing(&fleet.urls)?;
    let expected = BENCH6_SWITCHES_OFF
        .iter()
        .map(|(key, default)| {
            let on = drain && DRAIN_SWITCHES.iter().any(|(k, _)| k == key);
            (*key, if on { "true" } else { *default })
        })
        .collect::<Vec<_>>();
    for node in 0..4 {
        assert_switches(test, node, 0, &expected)?;
    }
    load.wait_started()?;
    wait_for_epoch_on_all(&fleet.urls, 1, NEXT_EPOCH_TIMEOUT)?;

    // every node logs its close of epoch 0, the slow member's with a backlog
    let drained = |node: usize| -> eyre::Result<Option<String>> {
        Ok(node_log_lines(test, node, 0)?.into_iter().find(|line| {
            line.contains("epoch close drained") && log_field(line, "epoch") == Some(0)
        }))
    };
    wait_until_blocking(
        DRAINED_LINE_TIMEOUT,
        "every node logs `epoch close drained` for epoch 0",
        || Ok((0..4).all(|node| drained(node).is_ok_and(|line| line.is_some()))),
    )?;
    let mut closes = Vec::new();
    for node in 0..4 {
        let line = drained(node)?.ok_or_else(|| eyre::eyre!("node {node} lost its drain line"))?;
        let field = |name| log_field(&line, name).unwrap_or_default();
        closes.push((field("backlog_outputs"), field("drain_ms"), field("detect_lag_ms")));
    }
    let (_, slow_drain, slow_detect) = closes[SLOW_MEMBER];
    eyre::ensure!(
        slow_detect + slow_drain >= MIN_SLOW_CLOSE_MS,
        "the slow member closed epoch 0 {slow_detect} ms + {slow_drain} ms after its closing \
         output committed (at least {MIN_SLOW_CLOSE_MS} ms for a loaded close): \
         (backlog_outputs, drain_ms, detect_lag_ms) per node {closes:?}"
    );

    assert_epoch_records(&fleet.urls, 0..=0)?;
    let track_failed = (0..4)
        .map(|node| -> eyre::Result<usize> {
            Ok(node_log_lines(test, node, 0)?
                .iter()
                .filter(|line| line.contains(TRACK_FAILED_LOG))
                .count())
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    for node in 0..4 {
        fleet.assert_no_log(node, 0, HALT_LOG)?;
    }
    let entry_behind = scrape_metric_value(&fleet.metrics[SLOW_MEMBER], ENTRY_ROUNDS_BEHIND)?;
    let report = format!(
        "epoch 0 closed on every node, (backlog_outputs, drain_ms, detect_lag_ms) per node \
         {closes:?}, `{TRACK_FAILED_LOG}` lines per node {track_failed:?}, slow member's \
         {ENTRY_ROUNDS_BEHIND} {entry_behind}"
    );
    if drain {
        eyre::ensure!(
            track_failed.iter().all(|count| *count == 0),
            "a node failed to track a spawned task with the drain on: {report}"
        );
    }
    Ok(report)
}
