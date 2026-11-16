//! An epoch whose record the whole network leaves uncertified (issue #1850).
//!
//! Four validators run as processes with 20-second epochs. Validators 1 and 2 withhold their own
//! vote for the record of epoch [`K`] in its first rounds (the test hook
//! `TN_TEST_WITHHOLD_EPOCH_VOTE`), so that record keeps two of the three votes it needs on every
//! node, while the records after it are certified by their own vote rounds. Two observers show
//! what a node outside those rounds can collect: the own-record observer runs through the close of
//! [`K`], stops before the close of `K + 1` and comes back after the close of `K + 2`, so it holds
//! its own uncertified record of [`K`]; the fresh observer starts on an empty datadir after the
//! close of `K + 2` and holds no record of [`K`].
//!
//! The two tests share one driver and differ in how many rounds the withholding validators keep
//! their vote off: every round the test's closes start (the collector walks past an epoch it
//! holds as its own uncertified record, and only then) or the first two (the close of `K + 2`
//! re-votes [`K`] and certifies it).

use super::{
    common::{
        get_block_number, kill_child, log_field, log_line_matches, network_advancing,
        node_log_lines, scrape_metric_value, scrape_metrics, start_validator_with_overrides,
    },
    fleet::{
        assert_epoch_records, log_str_field, node_epoch, wait_for_epoch_on_all, Fleet, Load,
        PASSPHRASE,
    },
};
use alloy::providers::{Provider as _, ProviderBuilder};
use e2e_tests::{config_observer, FleetOverrides};
use eyre::WrapErr as _;
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tn_types::{get_available_tcp_port, EpochCertificate, EpochRecord};

/// The epoch whose record the network leaves uncertified; epoch 0 closes normally first.
const K: u32 = 1;

/// How long each epoch lasts, in seconds.
const EPOCH_SECS: u32 = 20;

/// The garbage-collection depth of the genesis (the node's default).
const GC_DEPTH: u32 = 50;

/// The validators (instances) that withhold their own vote for the record of [`K`].
const WITHHOLDING: [usize; 2] = [0, 1];

/// The test hook naming the epoch whose own vote a node withholds.
const WITHHOLD_EPOCH_ENV: &str = "TN_TEST_WITHHOLD_EPOCH_VOTE";

/// The test hook's count of withheld rounds.
const WITHHOLD_ROUNDS_ENV: &str = "TN_TEST_WITHHOLD_EPOCH_VOTE_ROUNDS";

/// The rounds [`WITHHOLDING`] withhold in [`Arm::Recertify`]: the original round at the close of
/// [`K`] and the failsafe at the close of `K + 1`. With one round the failsafe would certify the
/// record a close later on every build.
const WITHHOLD_ROUNDS: u32 = 2;

/// The rounds [`WITHHOLDING`] withhold in [`Arm::Walk`]: more than the test's closes start for
/// [`K`]. A validator signs for [`K`] in the original round, the failsafe, one re-certify pick at
/// each close from `K + 2` while [`K`] is inside the pick's 16-epoch lookback, and each round a
/// peer's re-vote starts after its own round for [`K`] ended; the test ends a few closes after
/// `K + 2`.
const WALK_WITHHOLD_ROUNDS: u32 = 64;

/// The instance of the own-record observer. [`start_validator_with_overrides`] names a node's
/// datadir `validator-<instance + 1>`; the observers' keys are not in the committee.
const OWN_OBSERVER: usize = 4;

/// The instance of the fresh observer (see [`OWN_OBSERVER`]).
const FRESH_OBSERVER: usize = 5;

/// How long an observer has to collect the records it can collect.
const OBSERVER_WINDOW: Duration = Duration::from_secs(60);

/// Pause of each load thread before every transfer: a light load across the closes.
const LOAD_GAP: Duration = Duration::from_millis(250);

/// The line a node logs for every round whose own vote the hook withholds.
const WITHHOLD_LOG: &str = "test hook: withholding own epoch vote";

/// The line a node logs when it publishes its own record of an epoch at the epoch's close.
const PUBLISHED_LOG: &str = "epoch record published";

/// The line the record collector logs when it walks past an epoch it holds as its own uncertified
/// record.
const WALK_LOG: &str = "epoch record collector walking past an uncertified epoch";

/// The line a node logs when a close re-votes an older uncertified record of its own.
const RECERTIFY_LOG: &str = "epoch record re-certify round";

/// The line a round logs when the votes kept from earlier rounds certify the record.
const KEPT_VOTES_LOG: &str = "epoch record certified from kept votes";

/// The start of the error a vote round logs when it ends without a quorum and no certificate is
/// held; the record's digest follows.
const NO_QUORUM_LOG: &str = "failed to reach quorum on epoch close for";

/// The end of the error a vote round, a re-certify round included, logs as it stops the node when
/// the network certified another record of the epoch. No node may log it here, besides the lines
/// of `FATAL_LOGS`.
const ROUND_FORK_LOG: &str = "we have forked!";

/// The gauge of the last epoch of the certified prefix of a node's stored records.
const CERTIFIED_THROUGH: &str = "tn_state_sync_epoch_records_certified_through";

/// The counter of the uncertified epochs a node's record collection walked past.
const GAP_WALKS: &str = "tn_state_sync_epoch_record_gap_walks_total";

/// The counter of the certified records of the network that differ from a node's own; each one
/// stops the node.
const CONFLICTS: &str = "tn_state_sync_epoch_record_conflicts_total";

/// The counter of the epoch records that contradict a node's own execution, a conflict of
/// [`CONFLICTS`] included; each one stops the node.
const FINAL_STATE_MISMATCHES: &str = "tn_epoch_final_state_mismatches_total";

/// The JSON-RPC error code of `tn_epochRecord` for an epoch the node holds no certified record of
/// (`TNRpcError::NotFound`).
const RESOURCE_NOT_FOUND: i64 = -32001;

/// The counter of re-certify rounds by `result`.
const RECERTIFY_ROUNDS: &str = "tn_node_epoch_record_recertify_rounds_total";

/// The withheld rounds of one test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    /// Every round for [`K`] is withheld: the collector walks past its own uncertified record.
    Walk,
    /// The first two rounds are withheld: the close of `K + 2` re-votes [`K`] and certifies it.
    Recertify,
}

impl Arm {
    /// The test-log directory of the arm.
    fn test_name(self) -> &'static str {
        match self {
            Self::Walk => "uncertified_epoch_walk",
            Self::Recertify => "uncertified_epoch_recertify",
        }
    }

    /// The rounds [`WITHHOLDING`] withhold.
    fn withhold_rounds(self) -> u32 {
        match self {
            Self::Walk => WALK_WITHHOLD_ROUNDS,
            Self::Recertify => WITHHOLD_ROUNDS,
        }
    }

    /// The withhold hook on [`WITHHOLDING`].
    fn overrides(self) -> FleetOverrides {
        let withholding = WITHHOLDING.map(datadir);
        FleetOverrides::default().env(withholding.clone(), WITHHOLD_EPOCH_ENV, K.to_string()).env(
            withholding,
            WITHHOLD_ROUNDS_ENV,
            self.withhold_rounds().to_string(),
        )
    }
}

/// The datadir name [`start_validator_with_overrides`] gives `instance`.
fn datadir(instance: usize) -> String {
    format!("validator-{}", instance + 1)
}

/// A collector walks past an epoch it holds as its own uncertified record, and a collector
/// without that record still stops there.
///
/// The record of [`K`] stays uncertified. Each validator's collector walks past [`K`] exactly once
/// (one gap, one line). The own-record observer, back after the close of `K + 2`, serves the
/// certified records of `K + 1` and `K + 2` and not [`K`], executes on through the close of
/// `K + 2`, and counts a walk for each line it logs.
/// The fresh observer holds no certified record of [`K`], `K + 1` or `K + 2` after
/// [`OBSERVER_WINDOW`] and never walks. The certified-through gauge reads `K - 1` on every node:
/// it counts the certified prefix, which ends below the gap.
#[test]
#[ignore = "should not run with a default cargo test: starts four validators and two observers"]
fn test_uncertified_epoch_walked_past() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_arm(Arm::Walk)
}

/// The close of `K + 2` re-votes the
/// record of [`K`], which the hook no longer withholds, and certifies it.
///
/// Shortly after the close of `K + 2` every validator serves the same certified records of epochs
/// 0 to `K + 2`. At least one validator that withholds logs the re-certify round for [`K`], no
/// validator logs one for another epoch, at least one that logs it counts a certified re-certify
/// round, and none counts a short or skipped one. Every validator walks past [`K`] once before the
/// close of `K + 2` ([`K`] is not certified yet), the observers never. The
/// certified-through gauge then reads the last closed epoch on every validator, and both
/// observers, started after that, collect every closed epoch's record, [`K`] included, and read
/// the last closed epoch on the gauge too.
#[test]
#[ignore = "should not run with a default cargo test: starts four validators and two observers"]
fn test_uncertified_epoch_recertified() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_arm(Arm::Recertify)
}

/// Run one arm: the network under load through the closes and the observers' windows, then the
/// checks every arm shares once the load stopped.
fn run_arm(arm: Arm) -> eyre::Result<()> {
    let test = arm.test_name();
    let overrides = arm.overrides();
    let started = Instant::now();
    let mut fleet = Fleet::start(test, overrides.clone(), Some((GC_DEPTH, EPOCH_SECS)))?;
    let mut net = Observers::new(&mut fleet, overrides)?;
    let load = Load::start(&fleet.urls, LOAD_GAP);
    let driven = net.drive(arm, &mut fleet, &load);
    let sent = load.stop();
    let report = driven.and_then(|timeline| {
        let checks = net.check_after_load(arm, &fleet)?;
        Ok(format!("{timeline}; {checks}"))
    });
    fleet.guard.kill_all();
    let report = report.wrap_err(format!("{arm:?} arm failed: test_logs/{test}/"))?;
    eprintln!(
        "{test}: {report}; {} transfers sent; {:.0} s",
        sent?,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The two observers of a fleet, with their settings and where they serve.
struct Observers {
    /// The settings every node is started with.
    overrides: FleetOverrides,
    /// HTTP RPC url of [`OWN_OBSERVER`] and [`FRESH_OBSERVER`], in that order.
    urls: [String; 2],
    /// Their RPC ports, kept for the restart.
    rpc_ports: [u16; 2],
    /// Their metrics addresses.
    metrics: [String; 2],
    /// Their slots in the fleet's guard, once started.
    slots: [Option<usize>; 2],
    /// The digest of the validators' record of [`K`], once read.
    digest_k: Option<String>,
}

impl Observers {
    /// Give both observers a datadir under the fleet's and start the own-record observer.
    fn new(fleet: &mut Fleet, overrides: FleetOverrides) -> eyre::Result<Self> {
        let port = || get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no port"));
        let rpc_ports = [port()?, port()?];
        let metrics = [format!("127.0.0.1:{}", port()?), format!("127.0.0.1:{}", port()?)];
        for instance in [OWN_OBSERVER, FRESH_OBSERVER] {
            config_observer(&fleet.temp_path, &datadir(instance), Some(PASSPHRASE.to_string()), 1)?;
        }
        let mut net = Self {
            overrides,
            urls: rpc_ports.map(|p| format!("http://127.0.0.1:{p}")),
            rpc_ports,
            metrics,
            slots: [None, None],
            digest_k: None,
        };
        let child = net.spawn(fleet, OWN_OBSERVER, 0)?;
        net.slots[0] = Some(fleet.guard.push(child));
        Ok(net)
    }

    /// The position of observer `instance` ([`OWN_OBSERVER`] or [`FRESH_OBSERVER`]) in
    /// [`Self::urls`], [`Self::metrics`] and [`Self::slots`].
    fn index(instance: usize) -> usize {
        usize::from(instance == FRESH_OBSERVER)
    }

    /// Spawn observer `instance` as run `run` with the fleet's settings.
    fn spawn(&self, fleet: &Fleet, instance: usize, run: u32) -> eyre::Result<std::process::Child> {
        let i = Self::index(instance);
        start_validator_with_overrides(
            instance,
            e2e_tests::get_telcoin_network_binary(),
            &fleet.temp_path,
            self.rpc_ports[i],
            &fleet.test,
            run,
            &["--metrics", self.metrics[i].as_str()],
            &self.overrides,
        )
    }

    /// The network under load: the closes of [`K`], `K + 1` and `K + 2`, the own-record
    /// observer's stop and return, and the observers' windows. Returns the timeline.
    fn drive(&mut self, arm: Arm, fleet: &mut Fleet, load: &Load) -> eyre::Result<String> {
        let test = fleet.test.clone();
        let mut timeline = Vec::new();
        network_advancing(&fleet.urls)?;
        load.wait_started()?;
        let mut heads = vec![head_numbers(&fleet.urls)?];

        // the own-record observer writes its record of K, then stops before the close of K + 1
        let own_k = wait_for_log_line(
            &test,
            OWN_OBSERVER,
            0,
            PUBLISHED_LOG,
            &[format!("epoch={K}")],
            Duration::from_secs(u64::from(EPOCH_SECS) * 6 + 60),
        )?;
        let mut own = fleet
            .guard
            .take(self.slot(OWN_OBSERVER)?)
            .ok_or_else(|| eyre::eyre!("the own-record observer is not running"))?;
        kill_child(&mut own);
        let at_stop = head_numbers(&fleet.urls)?;
        let next = [format!("epoch={}", K + 1)];
        eyre::ensure!(
            log_lines(&test, OWN_OBSERVER, 0, PUBLISHED_LOG, &next)?.is_empty(),
            "the own-record observer published a record of epoch {} before it stopped",
            K + 1
        );
        timeline.push(format!("own-record observer stopped at heads {at_stop:?}"));

        // the closes of K, K + 1 and K + 2 on the validators; the chain advances through each
        let close_limit = Duration::from_secs(u64::from(EPOCH_SECS) * 3);
        for closed in K..=K + 2 {
            wait_for_epoch_on_all(&fleet.urls, closed + 1, close_limit)?;
            let at = head_numbers(&fleet.urls)?;
            let before = heads.last().cloned().unwrap_or_default();
            eyre::ensure!(
                at.iter().zip(&before).all(|(now, then)| now > then),
                "a validator did not advance through the close of epoch {closed}: {before:?} -> \
                 {at:?}"
            );
            timeline.push(format!("close of {closed} seen at heads {at:?}"));
            heads.push(at);
        }

        // every validator published the same record of K, and the observer held it too
        let digest_k = record_digest(&test, 0, 0, K)?;
        for node in 1..4 {
            let digest = record_digest(&test, node, 0, K)?;
            eyre::ensure!(
                digest == digest_k,
                "node {node} built record {K} {digest}, node 0 {digest_k}"
            );
        }
        let own_digest = log_str_field(&own_k, "digest").unwrap_or_default();
        eyre::ensure!(
            own_digest == digest_k,
            "the own-record observer's record of {K} is {own_digest}, the validators' {digest_k}"
        );
        self.digest_k = Some(digest_k);

        // records K + 1 and K + 2 are certified by their own rounds; K only by the re-certify
        match arm {
            Arm::Walk => assert_epoch_records(&fleet.urls, K + 1..=K + 2)?,
            Arm::Recertify => {
                assert_epoch_records(&fleet.urls, 0..=K + 2)?;
                timeline.push(format!(
                    "record {K} certified on all four at heads {:?}",
                    head_numbers(&fleet.urls)?
                ));
                for node in 0..4 {
                    wait_for_certified_through_tip(&fleet.metrics[node], &fleet.urls[0])
                        .wrap_err_with(|| format!("validator {node}"))?;
                }
            }
        }

        // both observers start after the close of K + 2 (and in the re-certify arm, after K is
        // certified): the own-record observer on its datadir, the fresh one on an empty one
        let child = self.spawn(fleet, OWN_OBSERVER, 1)?;
        fleet.guard.replace(self.slot(OWN_OBSERVER)?, child);
        let child = self.spawn(fleet, FRESH_OBSERVER, 0)?;
        self.slots[1] = Some(fleet.guard.push(child));
        let observers_started = Instant::now();
        let [own_url, fresh_url] = self.urls.clone();
        let window_end = observers_started + OBSERVER_WINDOW;
        match arm {
            Arm::Walk => {
                for epoch in K + 1..=K + 2 {
                    let left = window_end.saturating_duration_since(Instant::now());
                    fetch_record(&own_url, epoch, left.as_secs().max(1))
                        .wrap_err("the own-record observer did not walk past the gap")?;
                }
                // with them it executes on through the closes it missed
                let left = window_end.saturating_duration_since(Instant::now());
                let past = [format!("epoch={}", K + 2)];
                wait_for_log_line(&test, OWN_OBSERVER, 1, PUBLISHED_LOG, &past, left)
                    .wrap_err("the own-record observer did not execute past the gap")?;
                assert_serves_record_before_gap("fresh", &fresh_url, window_end)?;
                sleep_until(window_end);
                assert_holds_none("own-record", &own_url, K..=K)?;
                assert_holds_none("fresh", &fresh_url, K..=K + 2)?;
            }
            Arm::Recertify => {
                let last = node_epoch(&fleet.urls[0])? - 1;
                for (name, url) in [("own-record", &own_url), ("fresh", &fresh_url)] {
                    for epoch in 0..=last {
                        let left = window_end.saturating_duration_since(Instant::now());
                        let record = fetch_record(url, epoch, left.as_secs().max(1))
                            .wrap_err_with(|| format!("the {name} observer, record {epoch}"))?;
                        let validators = fetch_record(&fleet.urls[0], epoch, 10)?;
                        eyre::ensure!(
                            record.digest() == validators.digest(),
                            "the {name} observer serves record {epoch} {}, validator 0 {}",
                            record.digest(),
                            validators.digest()
                        );
                    }
                }
                timeline.push(format!("observers collected records 0..={last}"));
            }
        }
        timeline.push(format!(
            "observers checked {:.0} s after they started",
            observers_started.elapsed().as_secs_f64()
        ));
        Ok(timeline.join(", "))
    }

    /// The checks once the load stopped: one chain, the hook's rounds, the arm's walks, re-certify
    /// rounds and gauges, and no conflict or fatal line on any node.
    fn check_after_load(&self, arm: Arm, fleet: &Fleet) -> eyre::Result<String> {
        let test = fleet.test.as_str();
        fleet.settle()?;
        let head = fleet.assert_blocks_same()?;
        let mut report = vec![format!("all four hold the same blocks up to {head}")];
        let validator_runs = (0..4).map(|node| (node, 0));
        let runs = validator_runs
            .chain([(OWN_OBSERVER, 0), (OWN_OBSERVER, 1), (FRESH_OBSERVER, 0)])
            .collect::<Vec<_>>();
        let observers_alive = [(OWN_OBSERVER, 1), (FRESH_OBSERVER, 0)];

        // the hook withholds the first rounds for K on the withholding validators: the first two,
        // or in the walk every round the closes started (never fewer than two, never all it holds)
        for &(node, run) in &runs {
            let lines = log_lines(test, node, run, WITHHOLD_LOG, &[])?;
            let rounds =
                lines.iter().map(|line| (log_field(line, "epoch"), log_field(line, "round")));
            let rounds = rounds.collect::<Vec<_>>();
            let withheld = match arm {
                Arm::Walk => {
                    let started = u32::try_from(rounds.len())?;
                    eyre::ensure!(
                        started < WALK_WITHHOLD_ROUNDS,
                        "node {node} run {run} withheld every round the hook holds"
                    );
                    started.max(WITHHOLD_ROUNDS)
                }
                Arm::Recertify => WITHHOLD_ROUNDS,
            };
            let expected: Vec<(Option<u64>, Option<u64>)> = if WITHHOLDING.contains(&node)
                && run == 0
            {
                (1..=withheld).map(|round| (Some(u64::from(K)), Some(u64::from(round)))).collect()
            } else {
                Vec::new()
            };
            eyre::ensure!(
                rounds == expected,
                "node {node} run {run} withheld rounds {rounds:?}, expected {expected:?}"
            );
        }

        // no conflict, fork or other fatal line anywhere, and no conflict or mismatch counted on a
        // running node
        for &(node, run) in &runs {
            fleet.assert_no_fatal_log(node, run)?;
            fleet.assert_no_log(node, run, ROUND_FORK_LOG)?;
        }
        for metrics in fleet.metrics.iter().chain(&self.metrics) {
            for counter in [CONFLICTS, FINAL_STATE_MISMATCHES] {
                let value = scrape_metric_value(metrics, counter)?;
                eyre::ensure!(value == 0.0, "{metrics} counted {value} {counter}");
            }
        }

        // the walks: with the walk on, each validator walks past K once (it holds its own record
        // of K uncertified until the close of K + 2 at the earliest, and its own round certifies
        // K + 1); the own-record observer walks only when it comes back to the gap (its startup
        // sync and its collector each walk once); the fresh observer never, holding no record of K
        let walks = |node: usize, run: u32| log_lines(test, node, run, WALK_LOG, &[]);
        let mut walked = Vec::new();
        for &(node, run) in &runs {
            let lines = walks(node, run)?;
            walked.push(format!("{node}/{run}: {}", lines.len()));
            let validator = node < 4;
            let fields = if validator {
                vec![format!("epoch={K}"), "gaps=1".to_string()]
            } else {
                vec![format!("epoch={K}")]
            };
            eyre::ensure!(
                lines.iter().all(|line| log_line_matches(line, WALK_LOG, &fields)),
                "node {node} run {run} walked past another epoch than {K}: {lines:?}"
            );
            let count_ok = match (arm, node, run) {
                (_, FRESH_OBSERVER, _) | (_, OWN_OBSERVER, 0) => lines.is_empty(),
                (Arm::Walk, OWN_OBSERVER, _) => !lines.is_empty(),
                (Arm::Recertify, OWN_OBSERVER, _) => lines.is_empty(),
                _ => lines.len() == 1,
            };
            eyre::ensure!(
                count_ok,
                "node {node} run {run} walked {} times in the {arm:?} arm: {lines:?}",
                lines.len()
            );
        }
        report.push(format!("walk lines (node/run) {walked:?}"));
        // every running process counts one walk per line of its run
        let running = (0..4)
            .map(|node| (node, 0, &fleet.metrics[node]))
            .chain([(OWN_OBSERVER, 1, &self.metrics[0]), (FRESH_OBSERVER, 0, &self.metrics[1])]);
        for (node, run, metrics) in running {
            let lines = walks(node, run)?.len();
            let value = scrape_metric_value(metrics, GAP_WALKS)?;
            eyre::ensure!(
                value == f64::from(u32::try_from(lines)?),
                "node {node} run {run} counted {value} gap walks and logged {lines}"
            );
        }

        // the record of K: given up on every validator, or re-certified
        match arm {
            Arm::Walk => {
                let digest = self.digest_k.clone().unwrap_or_default();
                let no_quorum = format!("{NO_QUORUM_LOG} {digest}");
                for node in 0..4 {
                    wait_for_log_line(test, node, 0, &no_quorum, &[], Duration::from_secs(90))
                        .wrap_err_with(|| format!("validator {node}'s rounds for record {K}"))?;
                }
                let urls = fleet.urls.iter().chain(&self.urls).collect::<Vec<_>>();
                for url in urls {
                    assert_holds_none("node", url, K..=K)?;
                }
                // the fresh observer never held a record of K: it started after the closes
                eyre::ensure!(
                    log_lines(test, FRESH_OBSERVER, 0, PUBLISHED_LOG, &[format!("epoch={K}")])?
                        .is_empty(),
                    "the fresh observer published its own record of {K}"
                );
                report
                    .push(format!("record {K} uncertified on every node after its rounds gave up"));
            }
            Arm::Recertify => {
                let mut picked = Vec::new();
                for node in 0..4 {
                    let lines = log_lines(test, node, 0, RECERTIFY_LOG, &[])?;
                    eyre::ensure!(
                        lines.iter().all(|line| log_line_matches(
                            line,
                            RECERTIFY_LOG,
                            &[format!("epoch={K}")]
                        )),
                        "validator {node} re-certified another epoch than {K}: {lines:?}"
                    );
                    if lines.is_empty() {
                        continue;
                    }
                    picked.push(node);
                }
                eyre::ensure!(
                    WITHHOLDING.iter().any(|node| picked.contains(node)),
                    "no withholding validator ran the re-certify round for {K}: {picked:?}"
                );
                // the first withholding validator to pick K completes the quorum after it marked
                // its pick, so at least one picker counts a certified round; a later picker can
                // log its pick while the round is already ending and count nothing
                let certified = |node: usize| {
                    labelled_counter(&fleet.metrics[node], RECERTIFY_ROUNDS, "result=\"certified\"")
                };
                poll(Duration::from_secs(30), "a certified re-certify round", || {
                    let counted = picked.iter().map(|&node| certified(node));
                    Ok(counted.collect::<eyre::Result<Vec<_>>>()?.iter().any(|&c| c >= 1.0))
                })?;
                for node in 0..4 {
                    for result in ["short", "skipped"] {
                        let label = format!("result=\"{result}\"");
                        let value =
                            labelled_counter(&fleet.metrics[node], RECERTIFY_ROUNDS, &label)?;
                        eyre::ensure!(
                            value == 0.0,
                            "validator {node} counted {value} {result} re-certify rounds"
                        );
                    }
                }
                let mut rounds = Vec::new();
                for node in 0..4 {
                    let metrics = &fleet.metrics[node];
                    let kept = log_lines(test, node, 0, KEPT_VOTES_LOG, &[])?.len();
                    rounds.push(format!(
                        "{node}: certified {} short {} skipped {} kept-votes lines {kept}",
                        labelled_counter(metrics, RECERTIFY_ROUNDS, "result=\"certified\"")?,
                        labelled_counter(metrics, RECERTIFY_ROUNDS, "result=\"short\"")?,
                        labelled_counter(metrics, RECERTIFY_ROUNDS, "result=\"skipped\"")?,
                    ));
                }
                report
                    .push(format!("re-certify round on validators {picked:?}, rounds {rounds:?}"));
            }
        }
        if arm == Arm::Walk {
            // every validator re-votes K at the closes after K + 1, and none of those rounds
            // certifies it: the withholding validators keep their votes off in all of them
            for node in 0..4 {
                let lines = log_lines(test, node, 0, RECERTIFY_LOG, &[])?;
                eyre::ensure!(
                    !lines.is_empty()
                        && lines.iter().all(|line| log_line_matches(
                            line,
                            RECERTIFY_LOG,
                            &[format!("epoch={K}")]
                        )),
                    "validator {node} re-certify picks {lines:?}, expected epoch {K} only"
                );
                let certified = labelled_counter(
                    &fleet.metrics[node],
                    RECERTIFY_ROUNDS,
                    "result=\"certified\"",
                )?;
                eyre::ensure!(
                    certified == 0.0,
                    "validator {node} counted {certified} certified re-certify rounds"
                );
            }
            let short = |node: usize| {
                labelled_counter(&fleet.metrics[node], RECERTIFY_ROUNDS, "result=\"short\"")
            };
            poll(Duration::from_secs(90), "a short re-certify round", || {
                let counted = (0..4).map(short).collect::<eyre::Result<Vec<_>>>()?;
                Ok(counted.iter().any(|&c| c >= 1.0))
            })?;
        }

        // the certified-through gauge
        let alive = fleet.metrics.iter().chain(&self.metrics);
        match arm {
            Arm::Walk => {
                for metrics in alive {
                    let value = scrape_metric_value(metrics, CERTIFIED_THROUGH)?;
                    eyre::ensure!(
                        value == f64::from(K) - 1.0,
                        "{metrics} reads {value} certified through, expected {}",
                        K - 1
                    );
                }
            }
            Arm::Recertify => {
                for metrics in alive {
                    wait_for_certified_through_tip(metrics, &fleet.urls[0])?;
                }
            }
        }
        for (node, run) in observers_alive {
            let url = &self.urls[Self::index(node)];
            get_block_number(url)
                .wrap_err_with(|| format!("node {node} run {run} stopped serving"))?;
        }
        Ok(report.join(", "))
    }

    /// The observer's slot in the fleet's guard.
    fn slot(&self, instance: usize) -> eyre::Result<usize> {
        self.slots[Self::index(instance)]
            .ok_or_else(|| eyre::eyre!("node {instance} never started"))
    }
}

/// Every validator's latest block number.
fn head_numbers(urls: &[String; 4]) -> eyre::Result<Vec<u64>> {
    urls.iter().map(|url| get_block_number(url)).collect()
}

/// The lines of node `node`'s run-`run` log that match `text` and `fields`.
fn log_lines(
    test: &str,
    node: usize,
    run: u32,
    text: &str,
    fields: &[String],
) -> eyre::Result<Vec<String>> {
    Ok(node_log_lines(test, node, run)?
        .into_iter()
        .filter(|line| log_line_matches(line, text, fields))
        .collect())
}

/// Poll node `node`'s run-`run` log every half second for up to `limit` until a line matches;
/// returns the first match.
fn wait_for_log_line(
    test: &str,
    node: usize,
    run: u32,
    text: &str,
    fields: &[String],
    limit: Duration,
) -> eyre::Result<String> {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(line) = log_lines(test, node, run, text, fields)?.into_iter().next() {
            return Ok(line);
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "node {node} run {run} of {test} logged no `{text}` {fields:?} within {limit:?}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The digest of the record of `epoch` node `node` published in run `run`.
fn record_digest(test: &str, node: usize, run: u32, epoch: u32) -> eyre::Result<String> {
    let lines = log_lines(test, node, run, PUBLISHED_LOG, &[format!("epoch={epoch}")])?;
    lines
        .first()
        .and_then(|line| log_str_field(line, "digest"))
        .map(str::to_owned)
        .ok_or_else(|| eyre::eyre!("node {node} run {run} published no record of epoch {epoch}"))
}

/// Run `future` on a runtime of its own.
fn block_on<F: Future>(future: F) -> eyre::Result<F::Output> {
    Ok(tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(future))
}

/// The certified record of `epoch` on `url`, waiting up to `limit_secs` for it; its certificate
/// must verify.
fn fetch_record(url: &str, epoch: u32, limit_secs: u64) -> eyre::Result<EpochRecord> {
    block_on(super::common::fetch_verified_epoch_record(url, epoch, limit_secs))?
}

/// The certified record of `epoch` on `url` now, or `None` when the node answers that it holds no
/// certified record of it. A record served with a certificate that does not verify is an error,
/// and so is a node that does not answer.
fn certified_record(url: &str, epoch: u32) -> eyre::Result<Option<EpochRecord>> {
    block_on(certified_record_now(url, epoch))?
}

/// See [`certified_record`].
async fn certified_record_now(url: &str, epoch: u32) -> eyre::Result<Option<EpochRecord>> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    let answer = provider
        .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (epoch,))
        .await;
    match answer {
        Ok((record, cert)) => {
            eyre::ensure!(
                record.verify_with_cert(&cert),
                "{url} serves record {epoch} with a certificate that does not verify"
            );
            Ok(Some(record))
        }
        Err(e) if e.as_error_resp().is_some_and(|resp| resp.code == RESOURCE_NOT_FOUND) => Ok(None),
        Err(e) => Err(eyre::eyre!("{url} did not answer tn_epochRecord({epoch}): {e}")),
    }
}

/// The node `name` at `url` serves the certified record of `K - 1` by `until`: its RPC answers a
/// record it holds, so its "not found" for the records after it is a measurement.
fn assert_serves_record_before_gap(name: &str, url: &str, until: Instant) -> eyre::Result<()> {
    let left = until.saturating_duration_since(Instant::now()).as_secs().max(1);
    fetch_record(url, K - 1, left)
        .map(drop)
        .wrap_err_with(|| format!("the {name} observer never served the record before the gap"))
}

/// `url` (the node `name`) holds no certified record of any epoch in `epochs`.
fn assert_holds_none(
    name: &str,
    url: &str,
    epochs: std::ops::RangeInclusive<u32>,
) -> eyre::Result<()> {
    for epoch in epochs {
        if let Some(record) = certified_record(url, epoch)? {
            eyre::bail!(
                "{name} ({url}) holds a certified record of epoch {epoch}: {}",
                record.digest()
            );
        }
    }
    Ok(())
}

/// Wait up to 30 s until the certified-through gauge at `metrics` reads the epoch before the one
/// `tip_url` reports as current: the last closed epoch.
fn wait_for_certified_through_tip(metrics: &str, tip_url: &str) -> eyre::Result<()> {
    poll(Duration::from_secs(30), "certified through the last closed epoch", || {
        let last_closed = f64::from(node_epoch(tip_url)?) - 1.0;
        Ok(scrape_metric_value(metrics, CERTIFIED_THROUGH)? == last_closed)
    })
    .wrap_err_with(|| metrics.to_owned())
}

/// The sum of the samples of `name` whose label set holds `label` (`key="value"`) in the scrape of
/// `metrics`; zero when there is none (a labelled series appears on its first use).
fn labelled_counter(metrics: &str, name: &str, label: &str) -> eyre::Result<f64> {
    let body = scrape_metrics(metrics)?;
    let mut total = 0.0;
    for line in body.lines().map(str::trim) {
        let Some(rest) = line.strip_prefix(name).and_then(|rest| rest.strip_prefix('{')) else {
            continue;
        };
        let Some((labels, value)) = rest.rsplit_once('}') else { continue };
        if labels.split(',').any(|pair| pair == label) {
            total += value.trim().parse::<f64>().wrap_err_with(|| line.to_owned())?;
        }
    }
    Ok(total)
}

/// Poll `condition` every half second for up to `limit`. The conditions here scrape metrics and
/// call RPC, which `wait_until_blocking`'s 10 ms cadence would repeat a hundred times a second.
fn poll(
    limit: Duration,
    what: &str,
    condition: impl Fn() -> eyre::Result<bool>,
) -> eyre::Result<()> {
    let deadline = Instant::now() + limit;
    loop {
        if condition()? {
            return Ok(());
        }
        eyre::ensure!(Instant::now() < deadline, "not {what} within {limit:?}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Sleep until `at`.
fn sleep_until(at: Instant) {
    std::thread::sleep(at.saturating_duration_since(Instant::now()));
}
