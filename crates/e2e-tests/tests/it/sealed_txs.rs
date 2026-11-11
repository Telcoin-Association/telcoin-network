//! Sealed transactions stay visible and are not executed twice.
//!
//! With `sealed_tx_keep` on, a validator keeps the transactions of a batch it sealed in its pool,
//! marked, from the quorum ack until the batch executes, instead of pruning them at the ack
//! (issue #1807). The builder-level tests cover the mechanism; these tests start real nodes with
//! the switch on and check what a client sees: the switch acts on a live validator (its `batch
//! builder started` line reads `keeps_sealed=true` and its pool holds marks under load), every
//! transaction a client submitted lands exactly once without a client resend, the marks drain to
//! zero once the load has executed, and the fleet holds one chain.
//!
//! The load ([`Load`]) behaves like the benchmark generator without its resend: each sender signs
//! consecutive nonces, keeps at most [`WINDOW`] of them unexecuted, and submits every transaction
//! to one node. It sends the same bytes again only while the node refuses them; a transaction a
//! node accepted is never sent again. "Exactly once" is counted by hash over every block of the
//! chain and cross-checked against each sender's final nonce on every node.
//!
//! The scaffolding here ([`Fleet`], [`Load`], [`GaugeWatch`], the chain walk) is shared with
//! `admission.rs`.

use super::common::{
    acquire_test_permit, advertise_worker_rpc, current_epoch, force_kill_and_reap, kill_child,
    log_line_matches, node_log_lines, start_observer_with_args, start_validator_with_overrides,
    wait_for_node_log_line, ProcessGuard, EPOCH_DURATION,
};
use alloy::{primitives::utils::parse_ether, providers::ProviderBuilder};
use e2e_tests::{config_local_testnet_with_epoch_duration, FleetOverrides};
use eyre::WrapErr as _;
use jsonrpsee::{
    core::{client::ClientT as _, ClientError},
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use rand::{rngs::StdRng, SeedableRng as _};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    io::{Read as _, Write as _},
    net::TcpStream,
    path::PathBuf,
    process::Child,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tn_config::{Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{test_utils::TransactionFactory, RethChainSpec};
use tn_types::{
    get_available_tcp_port, keccak256, Address, Bytes, Genesis, GenesisAccount, NodeMode, B256,
    U256,
};
use tokio::task::JoinHandle;

/// BLS passphrase of every node the `config_local_testnet*` helpers configure.
const PASSPHRASE: &str = "restart_test";

/// Most transactions a sender keeps submitted and unexecuted, the benchmark generator's window.
pub(super) const WINDOW: u64 = 16;

/// Fee cap of every load transfer, in wei; the protocol's minimum base fee is far below it.
const GAS_PRICE: u128 = 100;

/// Gas limit of a plain value transfer.
const TRANSFER_GAS: u64 = 21_000;

/// Funding of each load sender at genesis.
const SENDER_FUNDING: &str = "1_000";

/// Gauge: transactions marked as sealed now, summed over the node's worker pools.
pub(super) const MARKED: &str = "tn_reth_sealed_txs_marked";

/// Counter: marks cleared because their batch executed (or the hash was mined).
pub(super) const CLEARED_EXECUTED: &str = "tn_reth_sealed_txs_cleared_executed_total";

/// The pool-side and builder-side sealed-keep counters reported beside the assertions.
pub(super) const SEALED_COUNTERS: [&str; 5] = [
    "tn_reth_sealed_txs_cleared_repooled_total",
    "tn_reth_sealed_txs_dropped_evicted_total",
    "tn_reth_sealed_txs_cleared_lag_total",
    "tn_reth_sealed_txs_late_marks_refused_total",
    "tn_batch_builder_sealed_txs_skipped_total",
];

/// Counter of transactions block building skipped as invalid, labelled by `reason`.
pub(super) const INVALID_SKIPPED: &str = "tn_reth_invalid_txs_skipped_total";

/// Interval of the gauge polls of [`GaugeWatch`].
const GAUGE_POLL: Duration = Duration::from_millis(100);

/// Message of the batch builder's start line, one per builder per epoch.
pub(super) const BUILDER_STARTED: &str = "batch builder started";

/// Message of the node's switch report, logged once per start after `parameters.yaml` loads.
pub(super) const NODE_SWITCHES: &str = "node switches";

/// Shortest time the load runs, whatever the epoch count says.
const LOAD_MIN: Duration = Duration::from_secs(20);

/// Time limit for everything the load submitted to execute on every node once it stops.
pub(super) const DRAIN_LIMIT: Duration = Duration::from_secs(240);

// ---- the tests --------------------------------------------------------------------------------

/// Four validators with `sealed_tx_keep: true` mark the transactions they seal and every
/// transaction lands exactly once across an epoch boundary (issue #1807, I-2 case S1 on a fleet).
///
/// Asserts: every validator's `node switches` line reads `sealed_tx_keep=true` and every one of
/// its `batch builder started` lines `keeps_sealed=true`; under load at least one validator's
/// [`MARKED`] gauge reads above zero (polled every [`GAUGE_POLL`] on every validator) and the
/// [`CLEARED_EXECUTED`] counter rises; once the load stops, every submitted transaction is in
/// exactly one block, each sender's nonce on every node equals its submitted count, every
/// validator's gauge returns to zero, and the four validators hold the same blocks.
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_validators_keep_and_mark_sealed_transactions() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    run_keep_fleet("validators_keep_and_mark_sealed_transactions", true).await
}

/// The control of [`test_validators_keep_and_mark_sealed_transactions`]: the same fleet and load
/// with `sealed_tx_keep` off on all four validators.
///
/// Asserts: every start line reads `keeps_sealed=false`, the [`MARKED`] gauge stays zero at every
/// poll, and the same exactly-once and same-blocks checks hold. The bench-5 duplicate needs a
/// client that resends after 300 s, which this load does not do; a failure here without a resend
/// is a pruned-before-execution loss (I-2 case S1 with the switch off).
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_validators_without_sealed_keep_do_not_mark() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    run_keep_fleet("validators_without_sealed_keep_do_not_mark", false).await
}

/// Start four validators with `sealed_tx_keep` set to `keep` (written to every node's
/// `parameters.yaml`, so the switch-off run names it too), run the load across an epoch boundary
/// and check what [`test_validators_keep_and_mark_sealed_transactions`] lists.
async fn run_keep_fleet(test: &'static str, keep: bool) -> eyre::Result<()> {
    let overrides =
        FleetOverrides::default().parameter(validator_names(), "sealed_tx_keep", keep.to_string());
    let fleet = Fleet::start(test, overrides, 2 * 4, false).await?;
    keep_fleet_checks(&fleet, keep)
        .await
        .wrap_err(format!("{test} failed; node logs in crates/e2e-tests/test_logs/{test}/"))
}

/// The checks of [`run_keep_fleet`] on a started fleet.
async fn keep_fleet_checks(fleet: &Fleet, keep: bool) -> eyre::Result<()> {
    let keeps_field = [format!("keeps_sealed={keep}")];
    for node in 0..4 {
        fleet.wait_for_log(node, 0, NODE_SWITCHES, &[format!("sealed_tx_keep={keep}")]).await?;
        fleet.wait_for_log(node, 0, BUILDER_STARTED, &keeps_field).await?;
    }

    let marks = GaugeWatch::start(fleet.metrics.to_vec(), MARKED);
    let targets = (0..fleet.senders.len()).map(|i| i % 4).collect();
    let load = Load::start(fleet, targets);
    let epochs = run_load_across_boundary(fleet).await;
    let reports = load.stop().await?;
    let (epoch_start, epoch_end) = epochs?;
    let landed = wait_for_landing(fleet, &reports).await;
    let marked_max = marks.stop();
    let landed = landed?;

    let walk = walk_chain(&fleet.clients).await?;
    let counts = assert_exactly_once(&reports, &walk)?;

    // every builder line, not just the first, carries the effective mode
    let builder_lines = (0..4)
        .map(|node| fleet.log_lines_with(node, 0, BUILDER_STARTED))
        .collect::<eyre::Result<Vec<_>>>()?;
    for (node, lines) in builder_lines.iter().enumerate() {
        let wrong = lines
            .iter()
            .filter(|line| !log_line_matches(line, BUILDER_STARTED, &keeps_field))
            .collect::<Vec<_>>();
        eyre::ensure!(
            wrong.is_empty(),
            "validator-{} logged {} `{BUILDER_STARTED}` lines without {}: {wrong:?}",
            node + 1,
            wrong.len(),
            keeps_field[0]
        );
    }

    let cleared = fleet.metric_sums(CLEARED_EXECUTED, None)?;
    if keep {
        eyre::ensure!(
            marked_max.iter().any(|max| *max > 0.0),
            "no validator's {MARKED} read above zero at any poll under load (maxima {marked_max:?})"
        );
        eyre::ensure!(
            cleared.iter().any(|value| *value > 0.0),
            "no validator's {CLEARED_EXECUTED} rose (values {cleared:?})"
        );
    } else {
        eyre::ensure!(
            marked_max.iter().all(|max| *max == 0.0),
            "{MARKED} read above zero with the switch off (maxima {marked_max:?})"
        );
    }
    // marks clear on execution or at the next sweep, so a drained fleet reaches zero
    fleet.wait_for_gauge_zero(MARKED).await?;

    eprintln!(
        "{}: keep={keep} epochs {epoch_start}->{epoch_end}; {} txs from {} senders each in \
         exactly one block ({} refusals {:?}; all executed {landed:?} after the load stopped); \
         chain height {} with {} txs; {MARKED} max per node {marked_max:?}; {CLEARED_EXECUTED} \
         {cleared:?}; builder start lines per node {:?}; skips by reason {:?}; counters {:?}",
        fleet.test,
        counts.submitted,
        reports.len(),
        counts.refusals,
        counts.refusal_classes,
        walk.height,
        walk.tx_blocks.len(),
        builder_lines.iter().map(Vec::len).collect::<Vec<_>>(),
        skip_reasons(fleet)?,
        sealed_counters(fleet)?,
    );
    Ok(())
}

/// The switched-on half of [`test_mixed_sealed_keep_and_admission_fleet_is_one_chain`]'s fleet.
const MIXED_ON: [usize; 2] = [0, 1];

/// The switched-on validator [`test_mixed_sealed_keep_and_admission_fleet_is_one_chain`] restarts.
const MIXED_RESTARTED: usize = 1;

/// A fleet mixed on `sealed_tx_keep` and `rpc_admission_backlog_outputs` is one chain, across an
/// epoch boundary and a restart of a switched-on validator.
///
/// The benchmark flips switches by rolling restart, so the fleet is mixed for minutes at every arm
/// change. Validators 1 and 2 run `sealed_tx_keep: true` and `rpc_admission_backlog_outputs: 2`,
/// validators 3 and 4 neither; the load submits to all four. Midway, validator 2's senders pause
/// until everything they had accepted has executed, validator 2 is killed and restarted with the
/// same switches, and its senders resume.
///
/// Asserts: each node's `node switches` line (both runs of validator 2) shows what it was given,
/// and its `batch builder started` lines the matching `keeps_sealed`; every accepted transaction
/// is in exactly one block and each sender's nonce on every node equals its accepted count; the
/// four validators hold the same blocks. A refusal by a switched-on node's gate is retried with
/// the same bytes, as a client would, and counted.
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_mixed_sealed_keep_and_admission_fleet_is_one_chain() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = "mixed_sealed_keep_and_admission_fleet_is_one_chain";
    let on = MIXED_ON.map(|instance| validator_names()[instance]);
    let overrides = FleetOverrides::default().parameter(on, "sealed_tx_keep", "true").parameter(
        on,
        "rpc_admission_backlog_outputs",
        "2",
    );
    let mut fleet = Fleet::start(test, overrides, 2 * 4, false).await?;
    mixed_fleet_checks(&mut fleet)
        .await
        .wrap_err(format!("{test} failed; node logs in crates/e2e-tests/test_logs/{test}/"))
}

/// The checks of [`test_mixed_sealed_keep_and_admission_fleet_is_one_chain`].
async fn mixed_fleet_checks(fleet: &mut Fleet) -> eyre::Result<()> {
    let expected = |instance: usize| {
        let on = MIXED_ON.contains(&instance);
        let bound = if on { 2 } else { 0 };
        (
            vec![format!("sealed_tx_keep={on}"), format!("rpc_admission_backlog_outputs={bound}")],
            vec![format!("keeps_sealed={on}")],
        )
    };
    for node in 0..4 {
        let (switches, keeps) = expected(node);
        fleet.wait_for_log(node, 0, NODE_SWITCHES, &switches).await?;
        fleet.wait_for_log(node, 0, BUILDER_STARTED, &keeps).await?;
    }

    let marks = GaugeWatch::start(fleet.metrics.to_vec(), MARKED);
    let load = Load::start(fleet, (0..fleet.senders.len()).map(|i| i % 4).collect());
    let phases = mixed_fleet_phases(fleet, &load).await;
    let reports = load.stop().await?;
    let (epochs, restart_took) = phases?;
    let landed = wait_for_landing(fleet, &reports).await;
    let marked_max = marks.stop();
    let landed = landed?;
    let walk = walk_chain(&fleet.clients).await?;
    let counts = assert_exactly_once(&reports, &walk)?;

    // the restarted node reports the same switches on its second run
    let (switches, keeps) = expected(MIXED_RESTARTED);
    fleet.wait_for_log(MIXED_RESTARTED, 1, NODE_SWITCHES, &switches).await?;
    fleet.wait_for_log(MIXED_RESTARTED, 1, BUILDER_STARTED, &keeps).await?;
    for node in 0..4 {
        let (_, keeps) = expected(node);
        let runs: &[u32] = if node == MIXED_RESTARTED { &[0, 1] } else { &[0] };
        for run in runs {
            let wrong = fleet
                .log_lines_with(node, *run, BUILDER_STARTED)?
                .into_iter()
                .filter(|line| !log_line_matches(line, BUILDER_STARTED, &keeps))
                .collect::<Vec<_>>();
            eyre::ensure!(
                wrong.is_empty(),
                "validator-{} run {run} logged `{BUILDER_STARTED}` without {}: {wrong:?}",
                node + 1,
                keeps[0]
            );
        }
    }
    eyre::ensure!(
        marked_max.iter().enumerate().all(|(node, max)| MIXED_ON.contains(&node) || *max == 0.0),
        "a switched-off validator held marks (maxima {marked_max:?})"
    );
    fleet.wait_for_gauge_zero(MARKED).await?;

    eprintln!(
        "{}: epochs {epochs:?}; validator-{} restarted in {restart_took:?}; {} txs from {} \
         senders each in exactly one block ({} refusals {:?}; all executed {landed:?} after the \
         load stopped); chain height {}; {MARKED} max per node {marked_max:?}; admission refusals \
         per node {:?}; skips by reason {:?}; counters {:?}",
        fleet.test,
        MIXED_RESTARTED + 1,
        counts.submitted,
        reports.len(),
        counts.refusals,
        counts.refusal_classes,
        walk.height,
        fleet.metric_sums("tn_reth_rpc_admission_refused_total", None)?,
        skip_reasons(fleet)?,
        sealed_counters(fleet)?,
    );
    Ok(())
}

/// The load phases of [`test_mixed_sealed_keep_and_admission_fleet_is_one_chain`]: one epoch
/// boundary, the drained restart of [`MIXED_RESTARTED`], then another boundary. Returns the
/// epochs at the start, the restart and the end, and how long the restart took.
async fn mixed_fleet_phases(fleet: &mut Fleet, load: &Load) -> eyre::Result<([u32; 3], Duration)> {
    let start = epoch(&fleet.urls[0]).await?;
    wait_for_epoch(fleet, start + 1).await?;

    let restarting = Instant::now();
    load.pause(MIXED_RESTARTED);
    load.wait_drained(MIXED_RESTARTED, &fleet.clients[0]).await?;
    fleet.kill_validator(MIXED_RESTARTED).await?;
    let at_restart = epoch(&fleet.urls[0]).await?;
    // the other three keep committing while it is down
    let height = block_number(&fleet.clients[0]).await?;
    let client = fleet.clients[0].clone();
    poll_until(Duration::from_secs(60), "the chain to grow without the killed node", || async {
        Ok(block_number(&client).await? > height + 5)
    })
    .await?;
    fleet.restart_validator(MIXED_RESTARTED, 1).await?;
    load.resume(MIXED_RESTARTED);
    let restart_took = restarting.elapsed();

    wait_for_epoch(fleet, at_restart + 2).await?;
    Ok(([start, at_restart, epoch(&fleet.urls[0]).await?], restart_took))
}

/// Wait (bounded) until validator 1 reports epoch `target` or later.
async fn wait_for_epoch(fleet: &Fleet, target: u32) -> eyre::Result<()> {
    let url = &fleet.urls[0];
    poll_until(Duration::from_secs(EPOCH_DURATION * 8), &format!("epoch {target}"), || async {
        Ok(epoch(url).await? >= target)
    })
    .await
}

/// The validator [`test_restart_with_sealed_uncommitted_batches`] kills.
const S9_VICTIM: usize = 1;

/// Shortest load before [`test_restart_with_sealed_uncommitted_batches`] kills its victim.
const S9_LOAD_MIN: Duration = Duration::from_secs(15);

/// A validator killed while it holds marked, unexecuted transactions and restarted: the client
/// sends the same bytes of every transaction of its own that has not executed (I-2 case S9).
///
/// All four validators run `sealed_tx_keep: true`. The load runs at least [`S9_LOAD_MIN`] and
/// until the victim's [`MARKED`] gauge reads above zero; the load stops and the victim is killed
/// with SIGKILL at once, so batches it sealed in its last fraction of a second have not been
/// committed (only its own primary would propose their digests). Once the other three have closed
/// an epoch, the victim restarts, and once it is an active member every transaction its senders
/// had accepted that has not executed on validator 1 is looked up on it
/// (`eth_getTransactionByHash`) and sent to it again with the same bytes. The summary line says
/// how many were left unexecuted: in the measured runs none was, because the other three
/// committed every batch the victim had sealed even when it was killed 65 ms after reading 32
/// marks, so the resend's answer stayed unobserved.
///
/// An earlier form slowed the victim's engine to hold marks longer; that only kept marks for
/// batches the other three had already executed, and nothing was left to resend.
///
/// The marks live in memory, so I-2's expectation (`already known` after the restart) may not
/// hold; the test records the answers and asserts the invariant that must hold either way: every
/// accepted transaction is in exactly one block, none is lost, and the four validators hold the
/// same blocks. The duplicates a resend can seal are counted as the rise of the `nonce_too_low`
/// skips across the restart.
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_restart_with_sealed_uncommitted_batches() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = "restart_with_sealed_uncommitted_batches";
    let overrides =
        FleetOverrides::default().parameter(validator_names(), "sealed_tx_keep", "true");
    let mut fleet = Fleet::start(test, overrides, 2 * 4, false).await?;
    restart_with_marks_checks(&mut fleet)
        .await
        .wrap_err(format!("{test} failed; node logs in crates/e2e-tests/test_logs/{test}/"))
}

/// The checks of [`test_restart_with_sealed_uncommitted_batches`].
async fn restart_with_marks_checks(fleet: &mut Fleet) -> eyre::Result<()> {
    for node in 0..4 {
        fleet.wait_for_log(node, 0, BUILDER_STARTED, &["keeps_sealed=true".to_string()]).await?;
    }
    let load = Load::start(fleet, (0..fleet.senders.len()).map(|i| i % 4).collect());
    let started = Instant::now();
    let victim_metrics = fleet.metrics[S9_VICTIM].clone();
    let last_marks = std::sync::Mutex::new((0.0, started));
    let marked = poll_until(Duration::from_secs(120), "the victim to hold marks", || async {
        let marked = metric_sum(&scrape(&victim_metrics)?, MARKED, None)?.unwrap_or(0.0);
        *last_marks.lock().unwrap_or_else(|e| e.into_inner()) = (marked, Instant::now());
        Ok(marked > 0.0 && started.elapsed() >= S9_LOAD_MIN)
    })
    .await;
    let low_before = fleet.metric_sums(INVALID_SKIPPED, Some("nonce_too_low"))?;
    let reports = load.stop().await?;
    let mut victim = fleet
        .guard
        .take(S9_VICTIM)
        .ok_or_else(|| eyre::eyre!("validator-{} is not running", S9_VICTIM + 1))?;
    force_kill_and_reap(&mut victim);
    let killed = Instant::now();
    marked?;
    let (marked_at_kill, read_at) = *last_marks.lock().unwrap_or_else(|e| e.into_inner());
    let read_before_kill = killed - read_at;

    // the other three keep going; what the victim sealed and reported can still commit there
    let survivor = fleet.clients[0].clone();
    let height = block_number(&survivor).await?;
    poll_until(Duration::from_secs(60), "the chain to grow without the victim", || async {
        Ok(block_number(&survivor).await? > height)
    })
    .await?;
    fleet.restart_validator(S9_VICTIM, 1).await?;
    let restarted = killed.elapsed();

    // every transaction of the victim's senders that validator 1 has not executed: looked up on the
    // restarted victim, then sent to it again with the same bytes
    let victim_client = fleet.clients[S9_VICTIM].clone();
    let mut unexecuted = 0;
    let mut known = 0;
    let mut answers: BTreeMap<String, u64> = BTreeMap::new();
    for report in reports.iter().filter(|report| report.target == S9_VICTIM) {
        let executed = nonce(&survivor, report.address).await? as usize;
        for (hash, raw) in report.hashes.iter().zip(&report.raws).skip(executed) {
            unexecuted += 1;
            let found: Option<Value> =
                victim_client.request("eth_getTransactionByHash", rpc_params![hash]).await?;
            known += usize::from(found.is_some());
            let answer = match victim_client
                .request::<B256, _>("eth_sendRawTransaction", rpc_params![raw])
                .await
            {
                Ok(_) => "accepted as new".to_string(),
                Err(error) => refusal_class(&error),
            };
            *answers.entry(answer).or_insert(0) += 1;
        }
    }
    let landed = wait_for_landing(fleet, &reports).await?;
    let walk = walk_chain(&fleet.clients).await?;
    let counts = assert_exactly_once(&reports, &walk)?;
    let low_after = fleet.metric_sums(INVALID_SKIPPED, Some("nonce_too_low"))?;
    eprintln!(
        "{}: victim validator-{} read {marked_at_kill} marks {read_before_kill:?} before it was killed {:?} into the load, back \
         as an active member {restarted:?} later; {unexecuted} of its senders' transactions had \
         not executed on validator-1 at the restart, {known} of them known to the restarted \
         victim; resend answers {answers:?}; all landed {landed:?} later; {} txs each in exactly \
         one block; nonce_too_low skips per node before the kill {low_before:?} (validator-{} \
         counts from its restart) and after {low_after:?}; chain height {}",
        fleet.test,
        S9_VICTIM + 1,
        started.elapsed(),
        counts.submitted,
        S9_VICTIM + 1,
        walk.height,
    );
    Ok(())
}

// ---- shared scaffolding ---------------------------------------------------------------------

/// The datadir names of the four validators the `config_local_testnet*` helpers create.
pub(super) fn validator_names() -> [&'static str; 4] {
    ["validator-1", "validator-2", "validator-3", "validator-4"]
}

/// Four validators (and optionally an observer) started from one genesis with funded load
/// senders, each serving `--metrics`.
pub(super) struct Fleet {
    /// The node processes, validators at indexes 0 to 3 and the observer at 4; declared before
    /// the tempdir so the nodes stop before their data directories are removed.
    pub(super) guard: ProcessGuard,
    /// The data directories of every node.
    _tmp: TempDir,
    /// Path of `_tmp`.
    pub(super) temp_path: PathBuf,
    /// The test's log directory name under `crates/e2e-tests/test_logs/`.
    pub(super) test: &'static str,
    /// The overrides every start of a validator applies.
    pub(super) overrides: FleetOverrides,
    /// RPC port of each validator, kept across restarts.
    pub(super) rpc_ports: [u16; 4],
    /// RPC url of each validator.
    pub(super) urls: [String; 4],
    /// RPC client of each validator.
    pub(super) clients: [HttpClient; 4],
    /// `--metrics` address of each validator, kept across restarts.
    pub(super) metrics: [String; 4],
    /// RPC client and metrics address of the observer, when one runs (log instance 4).
    pub(super) observer: Option<(HttpClient, String)>,
    /// The chain the senders sign for.
    pub(super) chain: Arc<RethChainSpec>,
    /// The funded load senders; [`Load::start`] signs with clones of them.
    pub(super) senders: Vec<TransactionFactory>,
}

impl Fleet {
    /// Configure a fleet with `senders` funded senders and [`EPOCH_DURATION`]-second epochs,
    /// start the four validators (and an observer when `observer` is set, with every validator
    /// advertising its RPC so the observer can forward to it) and wait until every validator is
    /// an active committee member.
    pub(super) async fn start(
        test: &'static str,
        overrides: FleetOverrides,
        senders: usize,
        observer: bool,
    ) -> eyre::Result<Self> {
        // short on purpose: the node IPC socket paths are built under the temp dir, and a unix
        // socket path holds at most 107 bytes
        let tmp = TempDir::with_prefix("stb")?;
        let temp_path = tmp.path().to_path_buf();
        let senders = (0..senders as u64)
            .map(|i| {
                TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(0x5ea1 + i))
            })
            .collect::<Vec<_>>();
        let funding = U256::from(parse_ether(SENDER_FUNDING)?);
        let accounts = senders
            .iter()
            .map(|sender| (sender.address(), GenesisAccount::default().with_balance(funding)))
            .collect();
        config_local_testnet_with_epoch_duration(
            &temp_path,
            Some(PASSPHRASE.to_string()),
            Some(accounts),
            Some(EPOCH_DURATION as u32),
        )?;
        let genesis: Genesis = Config::load_from_path(
            temp_path.join("shared-genesis").join("genesis").join("genesis.yaml"),
            ConfigFmt::YAML,
        )?;
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let rpc_ports = [free_port()?, free_port()?, free_port()?, free_port()?];
        let metrics = [metrics_addr()?, metrics_addr()?, metrics_addr()?, metrics_addr()?];
        if observer {
            for (instance, port) in rpc_ports.iter().enumerate() {
                advertise_worker_rpc(&temp_path, instance, *port)?;
            }
        }
        let urls = rpc_ports.map(|port| format!("http://127.0.0.1:{port}"));
        let clients = [client(&urls[0])?, client(&urls[1])?, client(&urls[2])?, client(&urls[3])?];
        let mut fleet = Self {
            guard: ProcessGuard::empty(),
            _tmp: tmp,
            temp_path,
            test,
            overrides,
            rpc_ports,
            urls,
            clients,
            metrics,
            observer: None,
            chain,
            senders,
        };
        for instance in 0..4 {
            let child = fleet.spawn_validator(instance, 0)?;
            fleet.guard.push(child);
        }
        if observer {
            let port = free_port()?;
            let obs_metrics = metrics_addr()?;
            fleet.guard.push(start_observer_with_args(
                4,
                e2e_tests::get_telcoin_network_binary(),
                &fleet.temp_path,
                port,
                test,
                0,
                &["--metrics", obs_metrics.as_str()],
            ));
            fleet.observer = Some((client(&format!("http://127.0.0.1:{port}"))?, obs_metrics));
        }
        for node in 0..4 {
            wait_for_mode(&fleet.clients[node], NodeMode::CvvActive).await?;
        }
        if let Some((obs_client, _)) = &fleet.observer {
            wait_for_mode(obs_client, NodeMode::Observer).await?;
        }
        Ok(fleet)
    }

    /// Spawn validator `instance` (its run-`run` log) with the fleet's overrides, on its fixed
    /// RPC port and metrics address.
    pub(super) fn spawn_validator(&self, instance: usize, run: u32) -> eyre::Result<Child> {
        start_validator_with_overrides(
            instance,
            e2e_tests::get_telcoin_network_binary(),
            &self.temp_path,
            self.rpc_ports[instance],
            self.test,
            run,
            &["--metrics", self.metrics[instance].as_str()],
            &self.overrides,
        )
    }

    /// Kill validator `instance` (SIGTERM, then SIGKILL after the guard's grace) and wait until
    /// its RPC no longer answers.
    pub(super) async fn kill_validator(&mut self, instance: usize) -> eyre::Result<()> {
        let mut child = self
            .guard
            .take(instance)
            .ok_or_else(|| eyre::eyre!("validator-{} is not running", instance + 1))?;
        kill_child(&mut child);
        let client = &self.clients[instance];
        poll_until(Duration::from_secs(30), "the killed validator's RPC to stop", || async {
            Ok(client.request::<String, _>("eth_blockNumber", rpc_params![]).await.is_err())
        })
        .await
    }

    /// Start validator `instance` again as run `run` and wait until it is an active committee
    /// member.
    pub(super) async fn restart_validator(
        &mut self,
        instance: usize,
        run: u32,
    ) -> eyre::Result<()> {
        let child = self.spawn_validator(instance, run)?;
        self.guard.replace(instance, child);
        wait_for_mode(&self.clients[instance], NodeMode::CvvActive).await
    }

    /// Wait (bounded) until validator `node`'s run-`run` log has a line with `text` and `fields`.
    pub(super) async fn wait_for_log(
        &self,
        node: usize,
        run: u32,
        text: &str,
        fields: &[String],
    ) -> eyre::Result<()> {
        wait_for_node_log_line(self.test, node, run, text, fields, Duration::from_secs(60)).await
    }

    /// Validator `node`'s run-`run` log lines that contain `text`.
    pub(super) fn log_lines_with(
        &self,
        node: usize,
        run: u32,
        text: &str,
    ) -> eyre::Result<Vec<String>> {
        Ok(node_log_lines(self.test, node, run)?
            .into_iter()
            .filter(|line| line.contains(text))
            .collect())
    }

    /// The sum of the samples of `name` (those whose labels contain `label`, if given) on each
    /// validator; a node that serves no sample of it reads zero.
    pub(super) fn metric_sums(&self, name: &str, label: Option<&str>) -> eyre::Result<Vec<f64>> {
        self.metrics
            .iter()
            .map(|addr| Ok(metric_sum(&scrape(addr)?, name, label)?.unwrap_or(0.0)))
            .collect()
    }

    /// Wait (bounded) until gauge `name` reads zero on every validator.
    pub(super) async fn wait_for_gauge_zero(&self, name: &str) -> eyre::Result<()> {
        let last = std::sync::Mutex::new(Vec::new());
        poll_until(Duration::from_secs(30), &format!("{name} to read 0"), || {
            let values = self.metric_sums(name, None);
            if let Ok(values) = &values {
                *last.lock().unwrap_or_else(|e| e.into_inner()) = values.clone();
            }
            async move { Ok(values?.iter().all(|value| *value == 0.0)) }
        })
        .await
        .wrap_err(format!(
            "{name} per validator at the deadline: {:?}",
            last.lock().unwrap_or_else(|e| e.into_inner())
        ))
    }
}

/// Wait (bounded) until the node behind `client` reports `mode`.
pub(super) async fn wait_for_mode(client: &HttpClient, mode: NodeMode) -> eyre::Result<()> {
    poll_until(Duration::from_secs(90), &format!("a node to report {mode:?}"), || async {
        Ok(client
            .request::<NodeMode, _>("tn_nodeMode", rpc_params![])
            .await
            .is_ok_and(|current| current == mode))
    })
    .await
}

/// One load sender's progress, shared between its task and [`Load`].
struct SenderState {
    /// The sender's address.
    address: Address,
    /// Index of the endpoint the sender submits to.
    target: usize,
    /// Transactions the sender has had accepted, which is also its next nonce.
    accepted: AtomicU64,
}

/// What one sender submitted.
#[derive(Debug)]
pub(super) struct SenderReport {
    /// The sender's address.
    pub(super) address: Address,
    /// Index of the endpoint the sender submitted to.
    pub(super) target: usize,
    /// Hashes of the transactions a node accepted, in nonce order.
    pub(super) hashes: Vec<B256>,
    /// The 0x-prefixed encoded transactions of `hashes`, in the same order.
    pub(super) raws: Vec<String>,
    /// Refused submissions (the same bytes sent again until accepted), counted by error text.
    pub(super) refusals: BTreeMap<String, u64>,
}

/// The load senders, one task each, submitting consecutive nonces until [`Load::stop`].
pub(super) struct Load {
    /// Set to end every sender after its current transaction.
    stop: Arc<AtomicBool>,
    /// Per endpoint: senders that target it sign nothing new while set.
    paused: Arc<Vec<AtomicBool>>,
    /// Every sender's progress.
    states: Vec<Arc<SenderState>>,
    /// The sender tasks.
    tasks: Vec<JoinHandle<eyre::Result<SenderReport>>>,
}

impl Load {
    /// Start one task per fleet sender; sender `i` submits to validator `targets[i]`.
    pub(super) fn start(fleet: &Fleet, targets: Vec<usize>) -> Self {
        Self::start_on(fleet, fleet.clients.to_vec(), targets)
    }

    /// Start one task per fleet sender; sender `i` submits to `endpoints[targets[i]]`.
    pub(super) fn start_on(fleet: &Fleet, endpoints: Vec<HttpClient>, targets: Vec<usize>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new((0..endpoints.len()).map(|_| AtomicBool::new(false)).collect());
        let mut states = Vec::new();
        let mut tasks = Vec::new();
        for (factory, target) in fleet.senders.iter().cloned().zip(targets) {
            let state = Arc::new(SenderState {
                address: factory.address(),
                target,
                accepted: AtomicU64::new(0),
            });
            states.push(Arc::clone(&state));
            tasks.push(tokio::spawn(run_sender(
                factory,
                fleet.chain.clone(),
                endpoints[target].clone(),
                state,
                Arc::clone(&stop),
                Arc::clone(&paused),
            )));
        }
        Self { stop, paused, states, tasks }
    }

    /// Stop senders that target endpoint `target` from signing new transactions.
    pub(super) fn pause(&self, target: usize) {
        self.paused[target].store(true, Ordering::Relaxed);
    }

    /// Let senders that target endpoint `target` sign again.
    pub(super) fn resume(&self, target: usize) {
        self.paused[target].store(false, Ordering::Relaxed);
    }

    /// Wait (bounded) until every transaction the senders that target `target` had accepted has
    /// executed on the validator behind `check`.
    pub(super) async fn wait_drained(&self, target: usize, check: &HttpClient) -> eyre::Result<()> {
        let states = self.states.iter().filter(|state| state.target == target).collect::<Vec<_>>();
        poll_until(DRAIN_LIMIT, &format!("senders of endpoint {target} to drain"), || async {
            for state in &states {
                let accepted = state.accepted.load(Ordering::Relaxed);
                if nonce(check, state.address).await? < accepted {
                    return Ok(false);
                }
            }
            Ok(true)
        })
        .await
    }

    /// End every sender and collect what each submitted.
    pub(super) async fn stop(self) -> eyre::Result<Vec<SenderReport>> {
        self.stop.store(true, Ordering::Relaxed);
        let mut reports = Vec::new();
        for task in self.tasks {
            reports.push(task.await??);
        }
        Ok(reports)
    }
}

/// One sender: sign the next nonce once it has fewer than [`WINDOW`] unexecuted, submit the bytes
/// until a node accepts them, repeat until `stop`.
async fn run_sender(
    mut factory: TransactionFactory,
    chain: Arc<RethChainSpec>,
    endpoint: HttpClient,
    state: Arc<SenderState>,
    stop: Arc<AtomicBool>,
    paused: Arc<Vec<AtomicBool>>,
) -> eyre::Result<SenderReport> {
    let sink = Address::from_slice(&[0x5e; 20]);
    let mut hashes = Vec::new();
    let mut raws = Vec::new();
    let mut refusals = BTreeMap::new();
    let mut executed = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if paused[state.target].load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let next = hashes.len() as u64;
        if next - executed >= WINDOW {
            executed = nonce(&endpoint, state.address).await.unwrap_or(executed);
            if next - executed >= WINDOW {
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
        }
        factory.set_nonce(next);
        let bytes = factory.create_eip1559_encoded(
            chain.clone(),
            Some(TRANSFER_GAS),
            GAS_PRICE,
            Some(sink),
            U256::from(1),
            Bytes::new(),
        );
        let hash = keccak256(&bytes);
        let encoded = const_hex::encode_prefixed(&bytes);
        // the same bytes again only while the node refuses them; an accepted transaction is never
        // sent twice. a refusal once `stop` is set ends the sender without the transaction
        let deadline = Instant::now() + Duration::from_secs(120);
        let accepted = loop {
            match endpoint.request::<B256, _>("eth_sendRawTransaction", rpc_params![&encoded]).await
            {
                Ok(answer) => {
                    eyre::ensure!(answer == hash, "node answered {answer} for {hash}");
                    break true;
                }
                // an earlier attempt reached the pool although its answer was lost
                Err(error) if error.to_string().contains("already known") => break true,
                Err(error) => {
                    *refusals.entry(refusal_class(&error)).or_insert(0) += 1;
                    if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                        break false;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        if !accepted {
            break;
        }
        hashes.push(hash);
        raws.push(encoded);
        state.accepted.store(hashes.len() as u64, Ordering::Relaxed);
    }
    Ok(SenderReport { address: state.address, target: state.target, hashes, raws, refusals })
}

/// The error text a refusal is counted under: the JSON-RPC code and message for a call error,
/// the transport error's kind otherwise.
pub(super) fn refusal_class(error: &ClientError) -> String {
    match error {
        ClientError::Call(object) => format!("{} {}", object.code(), object.message()),
        ClientError::Transport(_) => "transport".to_string(),
        ClientError::RequestTimeout => "timeout".to_string(),
        other => other.to_string(),
    }
}

/// Run the load until the epoch has advanced by two (so at least one boundary passed mid-load)
/// and [`LOAD_MIN`] has passed. Returns the epochs at the start and the end.
pub(super) async fn run_load_across_boundary(fleet: &Fleet) -> eyre::Result<(u32, u32)> {
    let start = epoch(&fleet.urls[0]).await?;
    let started = Instant::now();
    poll_until(
        Duration::from_secs(EPOCH_DURATION * 12),
        "the load to span an epoch boundary",
        || async { Ok(epoch(&fleet.urls[0]).await? >= start + 2 && started.elapsed() >= LOAD_MIN) },
    )
    .await?;
    Ok((start, epoch(&fleet.urls[0]).await?))
}

/// Wait (bounded by [`DRAIN_LIMIT`]) until every sender's nonce equals its accepted count on every
/// validator. Returns how long that took.
pub(super) async fn wait_for_landing(
    fleet: &Fleet,
    reports: &[SenderReport],
) -> eyre::Result<Duration> {
    let started = Instant::now();
    let behind = std::sync::Mutex::new(Vec::new());
    let waited = poll_until(DRAIN_LIMIT, "every accepted transaction to execute", || async {
        let mut now_behind = Vec::new();
        for (node, client) in fleet.clients.iter().enumerate() {
            for report in reports {
                let landed = nonce(client, report.address).await?;
                if landed != report.hashes.len() as u64 {
                    now_behind.push((node, report.address, landed, report.hashes.len()));
                }
            }
        }
        let done = now_behind.is_empty();
        *behind.lock().unwrap_or_else(|e| e.into_inner()) = now_behind;
        Ok(done)
    })
    .await;
    if let Err(error) = waited {
        let behind = behind.lock().unwrap_or_else(|e| e.into_inner()).clone();
        return Err(error.wrap_err(format!(
            "(validator index, sender, nonce, accepted) not landed: {behind:?}; {}",
            stuck_diagnosis(fleet, reports, &behind).await
        )));
    }
    Ok(started.elapsed())
}

/// For each sender validator 1 reports behind: the node it submitted to and, per validator,
/// whether that node still knows its first unexecuted transaction (pooled) or not (in no pool);
/// then each validator's skip counters by reason.
async fn stuck_diagnosis(
    fleet: &Fleet,
    reports: &[SenderReport],
    behind: &[(usize, Address, u64, usize)],
) -> String {
    let mut lines = Vec::new();
    for (_, address, landed, accepted) in behind.iter().filter(|entry| entry.0 == 0) {
        let Some(report) = reports.iter().find(|report| report.address == *address) else {
            continue;
        };
        let Some(hash) = report.hashes.get(*landed as usize) else { continue };
        let mut known = Vec::new();
        for client in &fleet.clients {
            let found = client
                .request::<Option<Value>, _>("eth_getTransactionByHash", rpc_params![hash])
                .await;
            known.push(match found {
                Ok(Some(_)) => "pooled",
                Ok(None) => "unknown",
                Err(_) => "error",
            });
        }
        lines.push(format!(
            "sender {address} (submitted to validator-{}) first unexecuted nonce {landed} of \
             {accepted}, {hash}: per validator {known:?}",
            report.target + 1
        ));
    }
    let skips =
        skip_reasons(fleet).map_or_else(|e| format!("unreadable: {e}"), |s| format!("{s:?}"));
    format!("{}; skips by reason per validator {skips}", lines.join("; "))
}

/// Every block of the chain as the four validators hold it.
pub(super) struct ChainWalk {
    /// The highest block every validator held when the walk started.
    pub(super) height: u64,
    /// Each transaction hash in blocks `1..=height`, with the blocks that hold it.
    pub(super) tx_blocks: HashMap<B256, Vec<u64>>,
}

/// Walk blocks `1..=height` on every validator, where `height` is the lowest head among them:
/// each block's hash must be the same on all, and the transaction hashes are collected.
pub(super) async fn walk_chain(clients: &[HttpClient]) -> eyre::Result<ChainWalk> {
    let mut height = u64::MAX;
    for client in clients {
        height = height.min(block_number(client).await?);
    }
    let mut tx_blocks: HashMap<B256, Vec<u64>> = HashMap::new();
    for number in 1..=height {
        let mut first: Option<B256> = None;
        for (node, client) in clients.iter().enumerate() {
            let block = block_by_number(client, number).await?;
            let hash = block_hash(&block)?;
            match first {
                None => {
                    first = Some(hash);
                    for tx in block["transactions"].as_array().into_iter().flatten() {
                        let tx = tx
                            .as_str()
                            .ok_or_else(|| eyre::eyre!("block {number} tx is not a hash: {tx}"))?
                            .parse::<B256>()?;
                        tx_blocks.entry(tx).or_default().push(number);
                    }
                }
                Some(expected) => eyre::ensure!(
                    hash == expected,
                    "block {number} differs: validator-1 has {expected}, validator-{} has {hash}",
                    node + 1
                ),
            }
        }
    }
    Ok(ChainWalk { height, tx_blocks })
}

/// What [`assert_exactly_once`] counted.
pub(super) struct LandingCounts {
    /// Transactions the senders had accepted.
    pub(super) submitted: usize,
    /// Refused submissions over all senders.
    pub(super) refusals: u64,
    /// Refused submissions by error text.
    pub(super) refusal_classes: BTreeMap<String, u64>,
}

/// Every accepted transaction is in exactly one block of `walk`.
pub(super) fn assert_exactly_once(
    reports: &[SenderReport],
    walk: &ChainWalk,
) -> eyre::Result<LandingCounts> {
    let mut missing = Vec::new();
    let mut repeated = Vec::new();
    for report in reports {
        for (nonce, hash) in report.hashes.iter().enumerate() {
            match walk.tx_blocks.get(hash).map(Vec::as_slice) {
                None | Some([]) => missing.push((report.address, nonce, *hash)),
                Some([_]) => (),
                Some(blocks) => repeated.push((report.address, nonce, *hash, blocks.to_vec())),
            }
        }
    }
    eyre::ensure!(
        missing.is_empty() && repeated.is_empty(),
        "{} accepted transactions are in no block (first {:?}) and {} are in more than one \
         (first {:?})",
        missing.len(),
        missing.first(),
        repeated.len(),
        repeated.first()
    );
    let mut refusal_classes = BTreeMap::new();
    for (class, count) in reports.iter().flat_map(|r| &r.refusals) {
        *refusal_classes.entry(class.clone()).or_insert(0) += count;
    }
    Ok(LandingCounts {
        submitted: reports.iter().map(|r| r.hashes.len()).sum(),
        refusals: refusal_classes.values().sum(),
        refusal_classes,
    })
}

/// Per validator, the [`INVALID_SKIPPED`] counter by `reason` label (nonzero reasons only).
pub(super) fn skip_reasons(fleet: &Fleet) -> eyre::Result<Vec<BTreeMap<String, f64>>> {
    fleet
        .metrics
        .iter()
        .map(|addr| {
            let body = scrape(addr)?;
            let mut reasons = BTreeMap::new();
            for line in body.lines().filter(|line| line.starts_with(INVALID_SKIPPED)) {
                let Some(reason) =
                    line.split("reason=\"").nth(1).and_then(|rest| rest.split('"').next())
                else {
                    continue;
                };
                let value: f64 = line.rsplit(' ').next().unwrap_or("0").parse()?;
                if value > 0.0 {
                    reasons.insert(reason.to_string(), value);
                }
            }
            Ok(reasons)
        })
        .collect()
}

/// Per counter of [`SEALED_COUNTERS`], its value on each validator.
pub(super) fn sealed_counters(fleet: &Fleet) -> eyre::Result<Vec<(&'static str, Vec<f64>)>> {
    SEALED_COUNTERS.iter().map(|name| Ok((*name, fleet.metric_sums(name, None)?))).collect()
}

/// Polls a gauge on several metrics endpoints every [`GAUGE_POLL`] on its own thread and keeps
/// each endpoint's maximum.
pub(super) struct GaugeWatch {
    /// Set to end the polling.
    stop: Arc<AtomicBool>,
    /// The polling thread; returns each endpoint's maximum.
    handle: std::thread::JoinHandle<Vec<f64>>,
}

impl GaugeWatch {
    /// Start polling `name` on every address of `addrs`.
    pub(super) fn start(addrs: Vec<String>, name: &'static str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut maxima = vec![0.0f64; addrs.len()];
            while !stop_thread.load(Ordering::Relaxed) {
                for (max, addr) in maxima.iter_mut().zip(&addrs) {
                    // a failed scrape (a node mid-restart) misses one sample
                    if let Ok(Some(value)) =
                        scrape(addr).and_then(|body| metric_sum(&body, name, None))
                    {
                        *max = max.max(value);
                    }
                }
                std::thread::sleep(GAUGE_POLL);
            }
            maxima
        });
        Self { stop, handle }
    }

    /// Stop polling and return each endpoint's maximum.
    pub(super) fn stop(self) -> Vec<f64> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or_default()
    }
}

// ---- small helpers ----------------------------------------------------------------------------

/// A free local TCP port.
pub(super) fn free_port() -> eyre::Result<u16> {
    get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no free tcp port"))
}

/// A free local `host:port` for `--metrics`.
fn metrics_addr() -> eyre::Result<String> {
    Ok(format!("127.0.0.1:{}", free_port()?))
}

/// A JSON-RPC client for `url` with a 10-s request timeout.
pub(super) fn client(url: &str) -> eyre::Result<HttpClient> {
    Ok(HttpClientBuilder::default().request_timeout(Duration::from_secs(10)).build(url)?)
}

/// Poll `condition` every 250 ms until it holds, failing after `limit` with `what` and the last
/// error the condition returned.
pub(super) async fn poll_until<F, Fut>(
    limit: Duration,
    what: &str,
    condition: F,
) -> eyre::Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = eyre::Result<bool>>,
{
    let deadline = Instant::now() + limit;
    let mut last_error = None;
    loop {
        match condition().await {
            Ok(true) => return Ok(()),
            Ok(false) => (),
            Err(error) => last_error = Some(error),
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "timed out after {limit:?} waiting for {what} (last error: {last_error:?})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The node's executed nonce of `address` (`eth_getTransactionCount` at `latest`).
pub(super) async fn nonce(client: &HttpClient, address: Address) -> eyre::Result<u64> {
    let raw: String =
        client.request("eth_getTransactionCount", rpc_params![address, "latest"]).await?;
    Ok(u64::from_str_radix(raw.trim_start_matches("0x"), 16)?)
}

/// The node's latest block number.
pub(super) async fn block_number(client: &HttpClient) -> eyre::Result<u64> {
    let raw: String = client.request("eth_blockNumber", rpc_params![]).await?;
    Ok(u64::from_str_radix(raw.trim_start_matches("0x"), 16)?)
}

/// Block `number` with transaction hashes only; an absent block is an error.
async fn block_by_number(client: &HttpClient, number: u64) -> eyre::Result<Value> {
    let block: Option<Value> =
        client.request("eth_getBlockByNumber", rpc_params![format!("0x{number:x}"), false]).await?;
    block.ok_or_else(|| eyre::eyre!("block {number} is absent"))
}

/// The `hash` of a block object.
fn block_hash(block: &Value) -> eyre::Result<B256> {
    Ok(block["hash"].as_str().ok_or_else(|| eyre::eyre!("block without hash"))?.parse()?)
}

/// The current epoch on the node at `url`, read from the `ConsensusRegistry`.
pub(super) async fn epoch(url: &str) -> eyre::Result<u32> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    Ok(current_epoch(&provider).await?.epoch_id)
}

/// The text of a `GET /metrics` from `addr`, with the connection closed after the answer.
pub(super) fn scrape(addr: &str) -> eyre::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

/// The sum of the samples of series `name` in a scrape `body` whose label set contains `label`
/// (every sample when `None`), or `None` when the body has no such sample.
pub(super) fn metric_sum(body: &str, name: &str, label: Option<&str>) -> eyre::Result<Option<f64>> {
    let mut total = None;
    for line in body.lines().map(str::trim).filter(|line| !line.starts_with('#')) {
        let Some(rest) = line.strip_prefix(name) else { continue };
        let (labels, value) = match rest.strip_prefix('{') {
            Some(after) => match after.rfind('}') {
                Some(end) => (&after[..end], &after[end + 1..]),
                None => continue,
            },
            None if rest.starts_with(char::is_whitespace) => ("", rest),
            None => continue,
        };
        if label.is_some_and(|label| !labels.contains(label)) {
            continue;
        }
        let raw = value.split_whitespace().next().unwrap_or_default();
        let value: f64 = raw.parse().map_err(|e| eyre::eyre!("`{name}` sample {raw:?}: {e}"))?;
        total = Some(total.unwrap_or(0.0) + value);
    }
    Ok(total)
}
