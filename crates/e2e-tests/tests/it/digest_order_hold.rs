//! The own-chain order hold (issue #1807, switch `digest_order_hold`) keeps every accepted
//! transaction while one validator's own chain of headers keeps breaking.
//!
//! A proposer drains its workers' batch digests into its header at round r. When that header is
//! cancelled by a newer own header, or certified too late to commit, its digests are re-queued
//! after the header at r+1 already drained newer digests of the same worker. A sender's later
//! transaction then executes before its earlier one, fails as "nonce too high" and is lost unless
//! the sender sends it again. With the hold on, a header drains nothing from a worker named in an
//! older own header that is not proven to be its own-chain ancestor.
//!
//! Every test here starts four validators as processes. The certifier hook
//! (`TN_TEST_OWN_CERT_DELAY_MS`, `_STAGE`, `_EVERY`, see `tn_types::test_hooks::own_cert_delay`)
//! holds back the own certificate of [`HOOKED`], so its header at a held round is not a parent of
//! its next header: what a far validator sees on a fleet. [`SLOWED`] executes every output with
//! batches late (the engine's execution-delay hook), so execution is uneven across the fleet and
//! the execution block each header names follows a quorum, not the fastest node. Senders spread
//! over all four validators submit consecutive nonces, up to [`WINDOW`] unexecuted each, and never
//! send an accepted transaction again; the load runs across epoch boundaries. At the end every
//! node's chain is walked block by block.

use super::{
    common::{
        acquire_test_permit, address_from_word, get_block_number, get_key, log_field,
        network_advancing, node_log_lines, scrape_metric_value, scrape_metrics, send_tel,
        WEI_PER_TEL,
    },
    fleet::{
        all_validators, assert_switches, expected_switches, node_epoch, wait_for_epoch_on_all,
        Fleet, Load, EXECUTION_DELAY_ENV, PRODUCTION_CADENCE, SLOW_EXECUTION_DELAY_MS,
        STACK_SWITCHES_ON,
    },
};
use e2e_tests::FleetOverrides;
use ethereum_tx_sign::{LegacyTransaction, Transaction as _};
use eyre::WrapErr as _;
use jsonrpsee::{
    core::{client::ClientT as _, ClientError},
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tn_test_utils::wait_until_blocking;
use tn_types::{keccak256, Address, B256};
use tokio::{runtime::Runtime, task::JoinHandle};

/// The validator whose own certificates the hook holds back: the "far" one.
const HOOKED: usize = 0;

/// The validator whose engine sleeps [`SLOW_EXECUTION_DELAY_MS`] before every output with batches.
const SLOWED: usize = 2;

/// The validator whose funded load account ([`Load::source`]) funds every sender.
const FUNDER: usize = 1;

/// Seconds per epoch: the load spans at least one boundary in about a minute.
const EPOCH_SECS: u32 = 30;

/// Garbage-collection depth in rounds. The hold's one accepted residual is an own header cut off
/// by garbage collection from a commit that includes its descendant; a depth far above the few
/// rounds a broken own chain takes to settle keeps that case out of these runs.
const GC_DEPTH: u32 = 50;

/// Senders per validator.
const SENDERS_PER_NODE: usize = 6;

/// The most transactions a sender has accepted and not yet executed: enough that one sender's
/// transactions spread over several batches of the same worker, and that the hooked node's
/// senders keep sending while its lane is held (with 16 their windows filled during a held
/// stretch, no batch arrived to be held, and `tn_primary_headers_held_total` stayed 0).
const WINDOW: u64 = 48;

/// Gas price of every transfer, in wei.
const GAS_PRICE: u128 = 1_000;

/// Gas of a plain transfer.
const TRANSFER_GAS: u128 = 21_000;

/// The chain id the e2e genesis uses (the same as `common::send_tel`).
const CHAIN_ID: u64 = 0xde7e1;

/// What the funder gives each sender, in TEL.
const SENDER_FUNDING_TEL: u128 = 100;

/// The account every transfer pays into.
const SINK: &str = "hd4b-sink";

/// The shortest load; the load also runs until [`LOAD_EPOCHS`] epochs have begun since it started.
const LOAD_MIN: Duration = Duration::from_secs(60);

/// Epochs that begin while the load runs, so at least one boundary closes mid-load.
const LOAD_EPOCHS: u32 = 2;

/// How long the accepted transactions may take to execute on every validator once the load
/// stopped.
const DRAIN_LIMIT: Duration = Duration::from_secs(240);

/// In the control, how long the landed count may stand still before the test stops waiting for
/// transactions that may be lost.
const CONTROL_QUIESCENCE: Duration = Duration::from_secs(45);

/// How long the senders of the killed node pause before the kill, so its pool has sealed what
/// they sent into batches (a SIGKILL loses a node's pool; sealed batches come back through the
/// orphan re-pool). Three batch delays at [`PRODUCTION_CADENCE`].
const SEAL_WAIT: Duration = Duration::from_secs(3);

/// The hook's variables.
const HOOK_MS_ENV: &str = "TN_TEST_OWN_CERT_DELAY_MS";
/// See [`HOOK_MS_ENV`].
const HOOK_STAGE_ENV: &str = "TN_TEST_OWN_CERT_DELAY_STAGE";
/// See [`HOOK_MS_ENV`].
const HOOK_EVERY_ENV: &str = "TN_TEST_OWN_CERT_DELAY_EVERY";

/// The line the certifier logs for every own certificate the hook holds.
const HOOK_LOG: &str = "test hook: holding own certificate";

/// The warning of invariant I2: a digest re-queued behind a newer digest of its lane with the
/// hold on.
const HOLD_WARN_LOG: &str = "digests re-queued out of order under the order hold";

/// The proposer's line for every re-queue.
const RESENT_LOG: &str = "digests resent";

/// The line the epoch manager logs after it re-pooled the orphaned batches of an epoch.
const ORPHAN_LOG: &str = "Finished processing orphaned batches";

/// Re-queued digests that returned behind a newer digest of their worker, by `cause`.
const OUT_OF_ORDER: &str = "tn_primary_requeued_out_of_order_total";
/// Re-queued digests whose order the proposer could not judge, by `reason`.
const ORDER_UNKNOWN: &str = "tn_primary_requeued_order_unknown_total";
/// Re-queued digests, by `cause` (`canceled`, `committed`).
const REQUEUED: &str = "tn_primary_digests_requeued_total";
/// Own headers that left at least one digest queued in a held lane.
const HEADERS_HELD: &str = "tn_primary_headers_held_total";
/// Cancel reports the certifier dropped, by `reason` (`shutdown`, `full`, `unsubscribed`).
const REPORT_DROPPED: &str = "tn_primary_uncertified_report_dropped_total";
/// Transactions the engine skipped as invalid, by `reason` (`nonce_too_high` among them).
const INVALID_SKIPPED: &str = "tn_reth_invalid_txs_skipped_total";
/// Orphaned transactions by what the re-pool did with them, by `result`.
const ORPHAN_TXS: &str = "tn_node_orphan_txs_total";

/// Every `result` of [`ORPHAN_TXS`]; the [`ORPHAN_LOG`] field of each is `txs_<result>`.
const ORPHAN_RESULTS: [&str; 5] =
    ["repooled", "invalid", "already_pooled", "refused_nonce_too_low", "refused_other"];

/// The re-queue causes; the hold covers both.
const CAUSES: [&str; 2] = ["canceled", "committed"];

/// With the hold on, every transaction a validator accepted lands exactly once while the own chain
/// of one validator keeps breaking, with every stack switch on and the hook at `before_store` (see
/// [`Form::hook`]). See [`check_form`] for the assertions.
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_hold_keeps_every_transaction_before_store() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    run_form(Form::BeforeStore)
}

/// [`test_hold_keeps_every_transaction_before_store`] with the hook at `after_store` (see
/// [`Form::hook`]).
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_hold_keeps_every_transaction_after_store() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    run_form(Form::AfterStore)
}

/// Every stack switch on ([`STACK_SWITCHES_ON`]), with the hook at
/// `after_store` so a lane waits on a commit notice when the kill lands: the hooked validator is
/// killed by SIGKILL under load once an epoch boundary has passed, and restarted on its datadir
/// with the same overrides while the load to the other three continues. Its senders pause
/// [`SEAL_WAIT`] before the kill, so what the node accepted is sealed in batches (held or in
/// uncommitted headers) and comes back through the orphan re-pool; a submission the node refuses
/// while down or inactive is sent again with the same bytes until it is accepted. The run goes on
/// until the restarted node accepts its senders again and one more epoch boundary has closed.
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_hold_survives_a_kill_of_the_far_validator() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    run_form(Form::Kill)
}

/// The control: the fleet of [`test_hold_keeps_every_transaction_before_store`] with only
/// `digest_order_hold` off. The hooked validator re-queues digests out of order
/// ([`OUT_OF_ORDER`] > 0); the transactions lost are reported, not asserted.
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_without_the_hold_the_far_validator_reorders() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    run_form(Form::Control)
}

/// One test's fleet: the switches every node gets and what happens to the hooked node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Form {
    /// Every stack switch on, the hook at `before_store`.
    BeforeStore,
    /// Every stack switch on, the hook at `after_store`.
    AfterStore,
    /// [`Self::AfterStore`], with the hooked node killed and restarted under load.
    Kill,
    /// [`Self::BeforeStore`] with `digest_order_hold: false`.
    Control,
}

impl Form {
    /// The test-log directory.
    fn test_name(self) -> &'static str {
        match self {
            Self::BeforeStore => "hold_before_store",
            Self::AfterStore => "hold_after_store",
            Self::Kill => "hold_kill",
            Self::Control => "hold_off_control",
        }
    }

    /// Whether `digest_order_hold` is on.
    fn hold(self) -> bool {
        self != Self::Control
    }

    /// The `parameters.yaml` switch entries every node gets.
    fn switches(self) -> Vec<(&'static str, &'static str)> {
        STACK_SWITCHES_ON
            .iter()
            .map(|&(key, value)| match (self, key) {
                (Self::Control, "digest_order_hold") => (key, "false"),
                _ => (key, value),
            })
            .collect()
    }

    /// The hook settings of the hooked node: `(stage, milliseconds, every)`.
    ///
    /// Each stage breaks the own chain one way, so a run re-queues through one path: `before_store`
    /// cancels the held header when the next own header reaches the certifier (cause `canceled`),
    /// `after_store` keeps a certified header out of the next header's parents until a later own
    /// header commits without it (cause `committed`). Measured in the first runs: `before_store`
    /// re-queued 16 and 15 digests as `canceled` and none as `committed`, `after_store` 32 as
    /// `committed` and none as `canceled`. [`Self::BeforeStore`] and the control take
    /// `before_store`, [`Self::AfterStore`] and the kill take `after_store`, so the hold runs on
    /// both paths and the kill lands while a lane waits on a commit notice. Holds last 5 s, past
    /// the next own header at this cadence, on every third round: the round after a held one
    /// carries no own parent, and the one after that does, so the lane is free again and
    /// drains.
    fn hook(self) -> (&'static str, u64, u64) {
        match self {
            Self::BeforeStore | Self::Control => ("before_store", 5_000, 3),
            Self::AfterStore | Self::Kill => ("after_store", 5_000, 3),
        }
    }

    /// The `cause` of the re-queues the hook's stage produces (see [`Self::hook`]).
    fn requeue_cause(self) -> &'static str {
        match self.hook().0 {
            "before_store" => "canceled",
            _ => "committed",
        }
    }

    /// Every node's switch entries, the hook on [`HOOKED`], the execution delay on [`SLOWED`],
    /// and the production cadence on every node.
    fn overrides(self) -> FleetOverrides {
        let (stage, ms, every) = self.hook();
        let hooked = [format!("validator-{}", HOOKED + 1)];
        let fleet = FleetOverrides::default()
            .env(hooked.clone(), HOOK_MS_ENV, ms.to_string())
            .env(hooked.clone(), HOOK_STAGE_ENV, stage)
            .env(hooked, HOOK_EVERY_ENV, every.to_string())
            .env(
                [format!("validator-{}", SLOWED + 1)],
                EXECUTION_DELAY_ENV,
                SLOW_EXECUTION_DELAY_MS.to_string(),
            );
        self.switches()
            .into_iter()
            .chain(PRODUCTION_CADENCE)
            .fold(fleet, |fleet, (key, value)| fleet.parameter(all_validators(), key, value))
    }
}

/// Start the form's fleet, drive it and check it; the fleet is stopped either way.
fn run_form(form: Form) -> eyre::Result<()> {
    let test = form.test_name();
    let started = Instant::now();
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?;
    let mut fleet = Fleet::start(test, form.overrides(), Some((GC_DEPTH, EPOCH_SECS)))?;
    let outcome = drive(form, &mut fleet, &runtime);
    fleet.guard.kill_all();
    let report = outcome.wrap_err(format!("{test}: check logs in test_logs/{test}/"))?;
    eprintln!("{test}: wall clock {:.0} s; {report}", started.elapsed().as_secs_f64());
    Ok(())
}

/// Run the load (with the kill in [`Form::Kill`]), let it land, read the chain, the metrics and
/// the logs, and check them.
fn drive(form: Form, fleet: &mut Fleet, runtime: &Runtime) -> eyre::Result<String> {
    let test = fleet.test.clone();
    network_advancing(&fleet.urls)?;
    let expected = expected_switches(&form.switches())?;
    for node in 0..4 {
        assert_switches(&test, node, 0, &expected)?;
    }
    let clients = {
        let _entered = runtime.enter();
        fleet.urls.iter().map(|url| client(url)).collect::<eyre::Result<Vec<_>>>()?
    };
    let funding = fund_senders(fleet, &clients, runtime)?;
    let load = HoldLoad::start(runtime, &clients);
    let mut counters = Vec::new();
    let kill = match form {
        Form::Kill => {
            let (run0, kill) = kill_and_restart(fleet, &load, &expected)?;
            counters.push((HOOKED, 0, run0));
            Some(kill)
        }
        _ => {
            run_across_boundary(fleet)?;
            None
        }
    };
    let mut reports = load.stop(runtime)?;
    reports.push(funding);
    let drained = if form.hold() {
        runtime.block_on(wait_for_landing(&clients, &reports))
    } else {
        runtime.block_on(wait_quiescent(&clients, &reports))
    };
    let head = fleet.settle()?;
    let walk = runtime.block_on(walk_chain(&clients))?;
    let runs = node_runs(form);
    for &(node, run) in &runs {
        if node != HOOKED || form != Form::Kill || run == 1 {
            counters.push((node, run, Counters::read(&fleet.metrics[node], form.hold())?));
        }
    }
    let measured = Measured::new(form, &test, &reports, &walk, counters, kill)?;
    let report = format!("{}; settled at block {head}", measured.report());
    check_form(form, fleet, &runs, &measured, drained.err()).wrap_err_with(|| report.clone())?;
    Ok(report)
}

/// Every `(node, run)` of the form's logs.
fn node_runs(form: Form) -> Vec<(usize, u32)> {
    let mut runs = (0..4).map(|node| (node, 0)).collect::<Vec<_>>();
    if form == Form::Kill {
        runs.push((HOOKED, 1));
    }
    runs
}

/// Fund every sender from the [`FUNDER`]'s load account through the [`FUNDER`] and wait until
/// each funding transfer executed on every node. Returns the funder's own report.
fn fund_senders(
    fleet: &Fleet,
    clients: &[HttpClient],
    runtime: &Runtime,
) -> eyre::Result<SenderReport> {
    let source = Load::source(FUNDER);
    let key = get_key(&source);
    let mut hashes = Vec::new();
    for (nonce, sender) in senders().iter().enumerate() {
        let answer = send_tel(
            &fleet.urls[FUNDER],
            &key,
            address_from_word(&sender.word),
            SENDER_FUNDING_TEL * WEI_PER_TEL,
            GAS_PRICE,
            TRANSFER_GAS,
            u128::try_from(nonce)?,
        )?;
        hashes.push(answer.trim_matches('"').parse::<B256>()?);
    }
    let report = SenderReport {
        address: address_from_word(&source),
        target: FUNDER,
        hashes,
        refusals: BTreeMap::new(),
    };
    runtime.block_on(wait_for_landing(clients, std::slice::from_ref(&report)))?;
    Ok(report)
}

/// Let the load run for at least [`LOAD_MIN`] and until [`LOAD_EPOCHS`] epochs have begun.
fn run_across_boundary(fleet: &Fleet) -> eyre::Result<()> {
    let url = &fleet.urls[FUNDER];
    let start = node_epoch(url)?;
    let started = Instant::now();
    wait_until_blocking(
        Duration::from_secs(u64::from(EPOCH_SECS) * (u64::from(LOAD_EPOCHS) + 4)),
        &format!("the load to run {LOAD_MIN:?} and past epoch {}", start + LOAD_EPOCHS),
        || Ok(node_epoch(url)? >= start + LOAD_EPOCHS && started.elapsed() >= LOAD_MIN),
    )
}

/// What the kill of [`Form::Kill`] saw.
struct KillSeen {
    /// Transactions the hooked node's senders had accepted when it was killed, by sender address.
    accepted_at_kill: BTreeMap<Address, u64>,
    /// Block of node [`FUNDER`] when the hooked node was killed.
    down_at: u64,
    /// Block of node [`FUNDER`] when the restarted node had caught up.
    back_at: u64,
    /// Epoch the restarted node came back in.
    back_epoch: u32,
    /// Epoch in which the restarted node accepted its senders' transactions again.
    active_epoch: u32,
}

/// Run the load past one epoch boundary, kill [`HOOKED`] with SIGKILL, let the others advance,
/// restart it on its datadir with the same overrides, and run on until an epoch boundary closes
/// with it back. Returns its run-0 counters, read just before the kill.
fn kill_and_restart(
    fleet: &mut Fleet,
    load: &HoldLoad,
    expected: &[(&str, &str)],
) -> eyre::Result<(Counters, KillSeen)> {
    let url = fleet.urls[FUNDER].clone();
    let start = node_epoch(&url)?;
    let started = Instant::now();
    wait_until_blocking(
        Duration::from_secs(u64::from(EPOCH_SECS) * 5),
        "the load to run past an epoch boundary before the kill",
        || Ok(node_epoch(&url)? > start && started.elapsed() >= LOAD_MIN / 2),
    )?;
    // a third of an epoch past the boundary: the epoch's orphan re-pool has finished, so the
    // run-0 counters read below match the run-0 log
    std::thread::sleep(Duration::from_secs(u64::from(EPOCH_SECS) / 3));
    load.pause(HOOKED);
    wait_until_blocking(
        Duration::from_secs(30),
        "the hooked node's senders end their submissions in flight",
        || Ok(!load.sending(HOOKED)),
    )?;
    std::thread::sleep(SEAL_WAIT);
    let accepted_at_kill = load.accepted(HOOKED);
    let run0 = Counters::read(&fleet.metrics[HOOKED], true)?;
    let mut child =
        fleet.guard.take(HOOKED).ok_or_else(|| eyre::eyre!("node {HOOKED} is not running"))?;
    // std's kill is SIGKILL on unix: no shutdown path runs
    child.kill()?;
    child.wait()?;
    load.resume(HOOKED);
    let accepted_before: u64 = accepted_at_kill.values().sum();
    let down_at = get_block_number(&url)?;
    let others = (0..4).filter(|&i| i != HOOKED).collect::<Vec<_>>();
    wait_until_blocking(
        Duration::from_secs(90),
        "the three running nodes advance ten blocks while the hooked node is down",
        || {
            Ok(others
                .iter()
                .all(|&i| get_block_number(&fleet.urls[i]).is_ok_and(|h| h >= down_at + 10)))
        },
    )?;
    let restarted = fleet.spawn(HOOKED, 1)?;
    fleet.guard.replace(HOOKED, restarted);
    assert_switches(&fleet.test, HOOKED, 1, expected)?;
    let back_at = get_block_number(&url)?;
    wait_until_blocking(
        Duration::from_secs(180),
        &format!("the restarted node executes block {back_at}"),
        || Ok(get_block_number(&fleet.urls[HOOKED]).is_ok_and(|h| h >= back_at)),
    )?;
    let back_epoch = node_epoch(&fleet.urls[HOOKED])?;
    // the restarted node refuses transactions ("validator is inactive") until it is an active
    // committee member again; the run goes on until its senders are accepted again and an epoch
    // boundary has closed with them sending
    wait_until_blocking(
        Duration::from_secs(u64::from(EPOCH_SECS) * 4),
        "the restarted node accepts its senders' transactions again",
        || Ok(load.accepted(HOOKED).values().sum::<u64>() > accepted_before),
    )?;
    let active_epoch = node_epoch(&fleet.urls[HOOKED])?;
    wait_for_epoch_on_all(
        &fleet.urls,
        active_epoch + 1,
        Duration::from_secs(u64::from(EPOCH_SECS) * 4),
    )?;
    Ok((run0, KillSeen { accepted_at_kill, down_at, back_at, back_epoch, active_epoch }))
}

/// One load sender: the word its key derives from and the validator it submits to.
#[derive(Clone, Debug)]
struct Sender {
    /// The key word ([`get_key`], [`address_from_word`]).
    word: String,
    /// Index of the validator it submits to.
    target: usize,
}

/// Every sender, [`SENDERS_PER_NODE`] per validator.
fn senders() -> Vec<Sender> {
    (0..4)
        .flat_map(|target| {
            (0..SENDERS_PER_NODE)
                .map(move |i| Sender { word: format!("hd4b-sender-{target}-{i}"), target })
        })
        .collect()
}

/// One sender's progress, shared between its task and [`HoldLoad`].
#[derive(Debug)]
struct SenderState {
    /// The sender's address.
    address: Address,
    /// Index of the validator it submits to.
    target: usize,
    /// Transactions accepted so far, which is also its next nonce.
    accepted: AtomicU64,
    /// Set while a submission of the sender is in flight.
    sending: AtomicBool,
}

/// What one sender submitted.
#[derive(Debug)]
struct SenderReport {
    /// The sender's address.
    address: Address,
    /// Index of the validator it submitted to.
    target: usize,
    /// Hashes of the transactions a node accepted, in nonce order.
    hashes: Vec<B256>,
    /// Refused submissions (the same bytes sent again until accepted), counted by error text.
    refusals: BTreeMap<String, u64>,
}

/// The senders, one task each, submitting consecutive nonces until [`HoldLoad::stop`].
struct HoldLoad {
    /// Set to end every sender after its current transaction.
    stop: Arc<AtomicBool>,
    /// Per validator: its senders sign nothing new while set.
    paused: Arc<[AtomicBool; 4]>,
    /// Every sender's progress.
    states: Vec<Arc<SenderState>>,
    /// The sender tasks.
    tasks: Vec<JoinHandle<eyre::Result<SenderReport>>>,
}

impl HoldLoad {
    /// Start one task per sender of [`senders`] on `runtime`, each submitting to its validator's
    /// client.
    fn start(runtime: &Runtime, clients: &[HttpClient]) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
        let mut states = Vec::new();
        let mut tasks = Vec::new();
        for sender in senders() {
            let state = Arc::new(SenderState {
                address: address_from_word(&sender.word),
                target: sender.target,
                accepted: AtomicU64::new(0),
                sending: AtomicBool::new(false),
            });
            states.push(Arc::clone(&state));
            tasks.push(runtime.spawn(run_sender(
                sender.word,
                clients[sender.target].clone(),
                state,
                Arc::clone(&stop),
                Arc::clone(&paused),
            )));
        }
        Self { stop, paused, states, tasks }
    }

    /// Stop the senders of validator `target` from signing new transactions.
    fn pause(&self, target: usize) {
        self.paused[target].store(true, Ordering::Relaxed);
    }

    /// Let the senders of validator `target` sign again.
    fn resume(&self, target: usize) {
        self.paused[target].store(false, Ordering::Relaxed);
    }

    /// Whether a sender of validator `target` has a submission in flight.
    fn sending(&self, target: usize) -> bool {
        self.states
            .iter()
            .any(|state| state.target == target && state.sending.load(Ordering::Relaxed))
    }

    /// Transactions accepted so far by each sender of validator `target`.
    fn accepted(&self, target: usize) -> BTreeMap<Address, u64> {
        self.states
            .iter()
            .filter(|state| state.target == target)
            .map(|state| (state.address, state.accepted.load(Ordering::Relaxed)))
            .collect()
    }

    /// End every sender and collect what each submitted.
    fn stop(mut self, runtime: &Runtime) -> eyre::Result<Vec<SenderReport>> {
        self.stop.store(true, Ordering::Relaxed);
        let tasks = std::mem::take(&mut self.tasks);
        runtime.block_on(async {
            let mut reports = Vec::new();
            for task in tasks {
                reports.push(task.await??);
            }
            Ok(reports)
        })
    }
}

impl Drop for HoldLoad {
    /// A load dropped without [`HoldLoad::stop`] still ends its senders.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// One sender: sign the next nonce once it has fewer than [`WINDOW`] unexecuted, submit the bytes
/// until a node accepts them, repeat until `stop`. An accepted transaction is never sent again.
async fn run_sender(
    word: String,
    client: HttpClient,
    state: Arc<SenderState>,
    stop: Arc<AtomicBool>,
    paused: Arc<[AtomicBool; 4]>,
) -> eyre::Result<SenderReport> {
    let key = const_hex::decode(get_key(&word))?;
    let sink = address_from_word(SINK);
    let mut hashes = Vec::new();
    let mut refusals = BTreeMap::new();
    let mut executed = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if paused[state.target].load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let next = u64::try_from(hashes.len())?;
        if next.saturating_sub(executed) >= WINDOW {
            executed = executed_nonce(&client, state.address).await.unwrap_or(executed);
            if next.saturating_sub(executed) >= WINDOW {
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
        }
        let (raw, hash) = sign_transfer(&key, sink, next)?;
        let encoded = const_hex::encode_prefixed(&raw);
        // the same bytes again only while the node refuses them; a refusal once `stop` is set
        // ends the sender without the transaction
        // the same bytes again only while the node refuses them, without a deadline (an outage
        // of the killed node outlasts any fixed one); a pause or `stop` ends the attempt without
        // the transaction, and the next attempt signs the same nonce into the same bytes
        state.sending.store(true, Ordering::Relaxed);
        let accepted = loop {
            match client.request::<B256, _>("eth_sendRawTransaction", rpc_params![&encoded]).await {
                Ok(answer) => {
                    eyre::ensure!(answer == hash, "node answered {answer} for {hash}");
                    break true;
                }
                // an earlier attempt reached the pool although its answer was lost
                Err(error) if error.to_string().contains("already known") => break true,
                Err(error) => {
                    *refusals.entry(refusal_class(&error)).or_insert(0) += 1;
                    if stop.load(Ordering::Relaxed) || paused[state.target].load(Ordering::Relaxed)
                    {
                        break false;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        state.sending.store(false, Ordering::Relaxed);
        if !accepted {
            continue;
        }
        hashes.push(hash);
        state.accepted.store(u64::try_from(hashes.len())?, Ordering::Relaxed);
    }
    Ok(SenderReport { address: state.address, target: state.target, hashes, refusals })
}

/// A signed legacy transfer of 1 wei from the holder of `key` to `to` at `nonce`: its encoded
/// bytes and hash.
fn sign_transfer(key: &[u8], to: Address, nonce: u64) -> eyre::Result<(Vec<u8>, B256)> {
    let mut to_bytes = [0u8; 20];
    to_bytes.copy_from_slice(to.as_slice());
    let transfer = LegacyTransaction {
        chain: CHAIN_ID,
        nonce: u128::from(nonce),
        to: Some(to_bytes),
        value: 1,
        gas_price: GAS_PRICE,
        gas: TRANSFER_GAS,
        data: Vec::new(),
    };
    let signature = transfer.ecdsa(key).map_err(|e| eyre::eyre!("signing failed: {e:?}"))?;
    let raw = transfer.sign(&signature);
    let hash = keccak256(&raw);
    Ok((raw, hash))
}

/// The error text a refusal is counted under: the JSON-RPC code and message for a call error, the
/// kind otherwise.
fn refusal_class(error: &ClientError) -> String {
    match error {
        ClientError::Call(object) => format!("{} {}", object.code(), object.message()),
        ClientError::Transport(_) => "transport".to_string(),
        ClientError::RequestTimeout => "timeout".to_string(),
        other => other.to_string(),
    }
}

/// A JSON-RPC client for `url` with a 10-s request timeout; build it inside the runtime.
fn client(url: &str) -> eyre::Result<HttpClient> {
    Ok(HttpClientBuilder::default().request_timeout(Duration::from_secs(10)).build(url)?)
}

/// The executed nonce of `address` (`eth_getTransactionCount` at `latest`).
async fn executed_nonce(client: &HttpClient, address: Address) -> eyre::Result<u64> {
    let raw: String =
        client.request("eth_getTransactionCount", rpc_params![address, "latest"]).await?;
    Ok(u64::from_str_radix(raw.trim_start_matches("0x"), 16)?)
}

/// Wait (bounded by [`DRAIN_LIMIT`]) until every sender's executed nonce equals its accepted
/// count on every validator. A timeout names every sender behind and, for each sender validator 1
/// reports behind, whether each validator still knows its first unexecuted transaction.
async fn wait_for_landing(clients: &[HttpClient], reports: &[SenderReport]) -> eyre::Result<()> {
    let deadline = Instant::now() + DRAIN_LIMIT;
    loop {
        let behind = landing_behind(clients, reports).await?;
        if behind.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let mut lost = Vec::new();
            for (node, address, landed, accepted) in behind.iter().filter(|b| b.0 == 0) {
                let Some(report) = reports.iter().find(|r| r.address == *address) else {
                    continue;
                };
                let Some(hash) = report.hashes.get(usize::try_from(*landed)?) else { continue };
                let mut known = Vec::new();
                for client in clients {
                    let found = client
                        .request::<Option<Value>, _>("eth_getTransactionByHash", rpc_params![hash])
                        .await;
                    known.push(match found {
                        Ok(Some(_)) => "pooled",
                        Ok(None) => "unknown",
                        Err(_) => "error",
                    });
                }
                lost.push(format!(
                    "validator {node}: sender {address} (submits to validator {}) first \
                     unexecuted nonce {landed} of {accepted}, {hash}: per validator {known:?}",
                    report.target
                ));
            }
            eyre::bail!(
                "accepted transactions did not execute on every validator within \
                 {DRAIN_LIMIT:?}: {} (validator, sender, nonce, accepted) behind, first {:?}; \
                 {lost:#?}",
                behind.len(),
                behind.first()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// `(validator, sender, executed nonce, accepted)` of every sender whose executed nonce on a
/// validator differs from its accepted count.
async fn landing_behind(
    clients: &[HttpClient],
    reports: &[SenderReport],
) -> eyre::Result<Vec<(usize, Address, u64, usize)>> {
    let mut behind = Vec::new();
    for (node, client) in clients.iter().enumerate() {
        for report in reports {
            let landed = executed_nonce(client, report.address).await?;
            if landed != u64::try_from(report.hashes.len())? {
                behind.push((node, report.address, landed, report.hashes.len()));
            }
        }
    }
    Ok(behind)
}

/// The control's drain: wait until every accepted transaction executed on every validator, or
/// until the executed nonces summed over the senders on validator 1 stood still for
/// [`CONTROL_QUIESCENCE`] (a lost transaction never executes). Bounded by [`DRAIN_LIMIT`].
async fn wait_quiescent(clients: &[HttpClient], reports: &[SenderReport]) -> eyre::Result<()> {
    let deadline = Instant::now() + DRAIN_LIMIT;
    let (mut last, mut since) = (u64::MAX, Instant::now());
    loop {
        if landing_behind(clients, reports).await?.is_empty() {
            return Ok(());
        }
        let mut total = 0;
        for report in reports {
            total += executed_nonce(&clients[0], report.address).await?;
        }
        if total != last {
            (last, since) = (total, Instant::now());
        }
        eyre::ensure!(Instant::now() < deadline, "the control did not settle in {DRAIN_LIMIT:?}");
        if since.elapsed() >= CONTROL_QUIESCENCE {
            return Err(eyre::eyre!(
                "executed nonces stood still for {CONTROL_QUIESCENCE:?} with transactions \
                 unexecuted"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Every block of the chain as the four validators hold it.
struct ChainWalk {
    /// The lowest head of the four when the walk started.
    height: u64,
    /// Each transaction hash in blocks `1..=height`, with the blocks that hold it.
    tx_blocks: BTreeMap<B256, Vec<u64>>,
}

/// Walk blocks `1..=height`, where `height` is the lowest head of the validators: each block's hash
/// must be the same on all of them, and the transaction hashes are collected.
async fn walk_chain(clients: &[HttpClient]) -> eyre::Result<ChainWalk> {
    let mut height = u64::MAX;
    for client in clients {
        let raw: String = client.request("eth_blockNumber", rpc_params![]).await?;
        height = height.min(u64::from_str_radix(raw.trim_start_matches("0x"), 16)?);
    }
    let mut tx_blocks: BTreeMap<B256, Vec<u64>> = BTreeMap::new();
    for number in 1..=height {
        let mut first: Option<B256> = None;
        for (node, client) in clients.iter().enumerate() {
            let block: Option<Value> = client
                .request("eth_getBlockByNumber", rpc_params![format!("0x{number:x}"), false])
                .await?;
            let block =
                block.ok_or_else(|| eyre::eyre!("validator {node} lacks block {number}"))?;
            let hash = block["hash"]
                .as_str()
                .ok_or_else(|| eyre::eyre!("block {number} on validator {node} has no hash"))?
                .parse::<B256>()?;
            match first {
                None => {
                    for tx in block["transactions"].as_array().into_iter().flatten() {
                        let tx = tx
                            .as_str()
                            .ok_or_else(|| eyre::eyre!("block {number} tx is not a hash: {tx}"))?
                            .parse::<B256>()?;
                        tx_blocks.entry(tx).or_default().push(number);
                    }
                    first = Some(hash);
                }
                Some(expected) => eyre::ensure!(
                    hash == expected,
                    "block {number} differs: validator 0 has {expected}, validator {node} has \
                     {hash}"
                ),
            }
        }
    }
    Ok(ChainWalk { height, tx_blocks })
}

/// One process's counters, read from its metrics endpoint.
#[derive(Clone, Debug, Default)]
struct Counters {
    /// [`OUT_OF_ORDER`] by `cause`.
    out_of_order: BTreeMap<String, f64>,
    /// [`ORDER_UNKNOWN`] by `reason`.
    order_unknown: BTreeMap<String, f64>,
    /// [`REQUEUED`] by `cause`.
    requeued: BTreeMap<String, f64>,
    /// [`HEADERS_HELD`].
    headers_held: f64,
    /// [`REPORT_DROPPED`] by `reason`.
    dropped: BTreeMap<String, f64>,
    /// [`INVALID_SKIPPED`] with `reason="nonce_too_high"`.
    nonce_too_high: f64,
    /// [`ORPHAN_TXS`] by `result`.
    orphan_txs: BTreeMap<String, f64>,
}

impl Counters {
    /// Read the counters at `addr`. The series a check reads must be served (a renamed series
    /// fails the read); [`HEADERS_HELD`] only with the hold on. The certifier's drop counter and
    /// the orphan counters register on first use: absent means zero.
    fn read(addr: &str, hold: bool) -> eyre::Result<Self> {
        let mut required = vec![OUT_OF_ORDER, REQUEUED, INVALID_SKIPPED, ORDER_UNKNOWN];
        if hold {
            required.push(HEADERS_HELD);
        }
        for name in required {
            scrape_metric_value(addr, name)?;
        }
        let body = scrape_metrics(addr)?;
        Ok(Self {
            out_of_order: series_by(&body, OUT_OF_ORDER, "cause")?,
            order_unknown: series_by(&body, ORDER_UNKNOWN, "reason")?,
            requeued: series_by(&body, REQUEUED, "cause")?,
            headers_held: series_by(&body, HEADERS_HELD, "")?.values().sum(),
            dropped: series_by(&body, REPORT_DROPPED, "reason")?,
            nonce_too_high: series_by(&body, INVALID_SKIPPED, "reason")?
                .get("nonce_too_high")
                .copied()
                .unwrap_or_default(),
            orphan_txs: series_by(&body, ORPHAN_TXS, "result")?,
        })
    }

    /// [`Self::out_of_order`] summed over the causes the hold covers.
    fn out_of_order_covered(&self) -> f64 {
        CAUSES.iter().filter_map(|cause| self.out_of_order.get(*cause)).sum()
    }
}

/// The samples of series `name` in a scrape `body`, summed by the value of label `label` (every
/// sample under `""` when `label` is empty or the sample lacks it).
fn series_by(body: &str, name: &str, label: &str) -> eyre::Result<BTreeMap<String, f64>> {
    let mut by = BTreeMap::new();
    let pattern = format!("{label}=\"");
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
        let raw = value.split_whitespace().next().unwrap_or_default();
        let value: f64 = raw.parse().map_err(|e| eyre::eyre!("`{name}` sample {raw:?}: {e}"))?;
        let key = (!label.is_empty())
            .then(|| labels.split(&pattern).nth(1).and_then(|r| r.split('"').next()))
            .flatten()
            .unwrap_or_default();
        *by.entry(key.to_owned()).or_insert(0.0) += value;
    }
    Ok(by)
}

/// The hooked node's own headers, from its `header proposed` lines: what the hold cost.
#[derive(Debug, Default)]
struct HoldCost {
    /// Headers proposed.
    headers: usize,
    /// Headers with no batch.
    empty: usize,
    /// Headers whose parents lack the node's own certificate of the round before.
    no_own_parent: usize,
    /// Headers that left digests queued in a held lane (`held > 0`).
    held_headers: usize,
    /// The most digests one header left held.
    max_held: u64,
    /// Lengths in rounds of the stretches in which a lane stayed held (`held_lanes > 0`).
    held_rounds: Vec<u64>,
    /// [`HOOK_LOG`] lines.
    hook_lines: usize,
}

impl HoldCost {
    /// Read node `node`'s run-`run` log of `test` into `self`. Every field is read by name: a line
    /// that lacks one fails the read.
    fn add_run(&mut self, test: &str, node: usize, run: u32) -> eyre::Result<()> {
        let lines = node_log_lines(test, node, run)?;
        self.hook_lines += lines.iter().filter(|line| line.contains(HOOK_LOG)).count();
        // (epoch, first round) of the open stretch, and the last (epoch, round) seen
        let mut open: Option<(u64, u64)> = None;
        let mut last = (0u64, 0u64);
        for line in lines.iter().filter(|line| line.contains("header proposed")) {
            let field = |name: &str| {
                log_field(line, name).ok_or_else(|| {
                    eyre::eyre!("node {node} run {run}: `header proposed` lacks {name}: {line}")
                })
            };
            let (epoch, round) = (field("epoch")?, field("round")?);
            let (batches, held, lanes) =
                (field("num_batches")?, field("held")?, field("held_lanes")?);
            let own_parent = str_field(line, "own_parent").ok_or_else(|| {
                eyre::eyre!("node {node} run {run}: `header proposed` lacks own_parent: {line}")
            })?;
            self.headers += 1;
            self.empty += usize::from(batches == 0);
            self.no_own_parent += usize::from(own_parent == "false");
            self.held_headers += usize::from(held > 0);
            self.max_held = self.max_held.max(held);
            if let Some((open_epoch, from)) = open {
                if open_epoch != epoch {
                    self.held_rounds.push((last.1 + 1).saturating_sub(from));
                    open = None;
                } else if lanes == 0 {
                    self.held_rounds.push(round.saturating_sub(from));
                    open = None;
                }
            }
            if lanes > 0 && open.is_none() {
                open = Some((epoch, round));
            }
            last = (epoch, round);
        }
        if let Some((_, from)) = open {
            self.held_rounds.push((last.1 + 1).saturating_sub(from));
        }
        Ok(())
    }

    /// One line for the report.
    fn describe(&self) -> String {
        let stretches = self.held_rounds.len();
        let mean = if stretches == 0 {
            0.0
        } else {
            self.held_rounds.iter().sum::<u64>() as f64 / stretches as f64
        };
        format!(
            "{} headers ({} empty, {} without own parent), {} left digests held (max {}), {} held \
             stretches (max {} rounds, mean {mean:.1}), {} hook holds",
            self.headers,
            self.empty,
            self.no_own_parent,
            self.held_headers,
            self.max_held,
            stretches,
            self.held_rounds.iter().max().copied().unwrap_or_default(),
            self.hook_lines
        )
    }
}

/// The value of the `name=value` field of a plain log line, without the quotes of a string field.
fn str_field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(name)?.strip_prefix('='))
        .map(|value| value.trim_matches('"'))
}

/// The [`ORPHAN_LOG`] lines of node `node`'s run `run`: how many, and each `txs_<result>` field
/// summed over them, by result.
fn orphan_lines(test: &str, node: usize, run: u32) -> eyre::Result<(usize, BTreeMap<String, f64>)> {
    let lines = node_log_lines(test, node, run)?;
    let lines = lines.iter().filter(|line| line.contains(ORPHAN_LOG)).collect::<Vec<_>>();
    let mut sums = BTreeMap::new();
    for line in &lines {
        for result in ORPHAN_RESULTS {
            let value = log_field(line, &format!("txs_{result}")).ok_or_else(|| {
                eyre::eyre!("node {node} run {run}: `{ORPHAN_LOG}` lacks txs_{result}: {line}")
            })?;
            *sums.entry(result.to_owned()).or_insert(0.0) += value as f64;
        }
    }
    Ok((lines.len(), sums))
}

/// The lines of node `node`'s run `run` containing `text` and, if `field` is set, a nonzero
/// `field`.
fn lines_with(
    test: &str,
    node: usize,
    run: u32,
    text: &str,
    field: Option<&str>,
) -> eyre::Result<Vec<String>> {
    Ok(node_log_lines(test, node, run)?
        .into_iter()
        .filter(|line| line.contains(text))
        .filter(|line| field.is_none_or(|f| log_field(line, f).is_some_and(|v| v > 0)))
        .collect())
}

/// Everything a form measured.
struct Measured {
    /// The form.
    form: Form,
    /// The test-log directory.
    test: String,
    /// Transactions accepted, over every sender and the funder.
    accepted: usize,
    /// Accepted transactions in exactly one block.
    landed: usize,
    /// Accepted transactions in no block: `(sender, nonce, hash)`.
    lost: Vec<(Address, usize, B256)>,
    /// Accepted transactions in more than one block: `(sender, nonce, hash, blocks)`.
    repeated: Vec<(Address, usize, B256, Vec<u64>)>,
    /// Landed transactions of the senders of [`HOOKED`].
    hooked_landed: usize,
    /// Refused submissions by error text, over every sender.
    refusals: BTreeMap<String, u64>,
    /// Refused submissions of the senders of [`HOOKED`], by error text.
    hooked_refusals: BTreeMap<String, u64>,
    /// The height the chain walk compared up to.
    height: u64,
    /// Counters per `(node, run)`.
    counters: Vec<(usize, u32, Counters)>,
    /// The hold's cost on [`HOOKED`], over its runs.
    cost: HoldCost,
    /// The account of the kill, in [`Form::Kill`].
    kill: Option<String>,
}

impl Measured {
    /// Count what landed and read the hooked node's headers.
    fn new(
        form: Form,
        test: &str,
        reports: &[SenderReport],
        walk: &ChainWalk,
        counters: Vec<(usize, u32, Counters)>,
        kill: Option<KillSeen>,
    ) -> eyre::Result<Self> {
        let (mut lost, mut repeated, mut landed, mut hooked_landed) = (vec![], vec![], 0, 0);
        let (mut refusals, mut hooked_refusals) = (BTreeMap::new(), BTreeMap::new());
        for report in reports {
            for (nonce, hash) in report.hashes.iter().enumerate() {
                match walk.tx_blocks.get(hash).map(Vec::as_slice) {
                    None | Some([]) => lost.push((report.address, nonce, *hash)),
                    Some([_]) => {
                        landed += 1;
                        hooked_landed += usize::from(report.target == HOOKED);
                    }
                    Some(blocks) => repeated.push((report.address, nonce, *hash, blocks.to_vec())),
                }
            }
            for (class, count) in &report.refusals {
                *refusals.entry(class.clone()).or_insert(0) += count;
                if report.target == HOOKED {
                    *hooked_refusals.entry(class.clone()).or_insert(0) += count;
                }
            }
        }
        let mut cost = HoldCost::default();
        for (node, run) in node_runs(form) {
            if node == HOOKED {
                cost.add_run(test, node, run)?;
            }
        }
        // the killed node's senders: accepted before the kill (and landed), accepted after it
        let kill = kill.map(|seen| {
            let (mut before, mut before_landed, mut after) = (0, 0, 0);
            for report in reports.iter().filter(|r| r.target == HOOKED) {
                let at = seen.accepted_at_kill.get(&report.address).copied().unwrap_or_default();
                for (nonce, hash) in report.hashes.iter().enumerate() {
                    if (nonce as u64) < at {
                        before += 1;
                        before_landed +=
                            usize::from(walk.tx_blocks.get(hash).is_some_and(|b| b.len() == 1));
                    } else {
                        after += 1;
                    }
                }
            }
            format!(
                "killed at block {}, accepted before the kill {before} (landed once \
                 {before_landed}), accepted after it {after}, back at block {} in epoch {}, \
                 accepting again in epoch {}",
                seen.down_at, seen.back_at, seen.back_epoch, seen.active_epoch
            )
        });
        Ok(Self {
            form,
            test: test.to_owned(),
            accepted: reports.iter().map(|r| r.hashes.len()).sum(),
            landed,
            lost,
            repeated,
            hooked_landed,
            refusals,
            hooked_refusals,
            height: walk.height,
            counters,
            cost,
            kill,
        })
    }

    /// The counters of [`HOOKED`] summed over its runs.
    fn hooked(&self) -> Counters {
        let mut sum = Counters::default();
        let add = |into: &mut BTreeMap<String, f64>, from: &BTreeMap<String, f64>| {
            for (key, value) in from {
                *into.entry(key.clone()).or_insert(0.0) += value;
            }
        };
        for (_, _, c) in self.counters.iter().filter(|(node, ..)| *node == HOOKED) {
            add(&mut sum.out_of_order, &c.out_of_order);
            add(&mut sum.order_unknown, &c.order_unknown);
            add(&mut sum.requeued, &c.requeued);
            add(&mut sum.dropped, &c.dropped);
            add(&mut sum.orphan_txs, &c.orphan_txs);
            sum.headers_held += c.headers_held;
            sum.nonce_too_high += c.nonce_too_high;
        }
        sum
    }

    /// The report line: every figure the hand-back needs.
    fn report(&self) -> String {
        let hooked = self.hooked();
        let per_node = self
            .counters
            .iter()
            .map(|(node, run, c)| {
                format!(
                    "node {node} run {run}: out_of_order {:?}, order_unknown {:?}, requeued {:?}, \
                     headers_held {}, report_dropped {:?}, nonce_too_high {}, orphan_txs {:?}",
                    c.out_of_order,
                    c.order_unknown,
                    c.requeued,
                    c.headers_held,
                    c.dropped,
                    c.nonce_too_high,
                    c.orphan_txs
                )
            })
            .collect::<Vec<_>>();
        let share =
            if self.landed == 0 { 0.0 } else { self.hooked_landed as f64 / self.landed as f64 };
        format!(
            "form {:?}, hook {:?}: accepted {}, landed {}, lost {} (first {:?}), duplicated {} \
             (first {:?}); hooked validator: {} landed (share {share:.3}), refusals {:?}, \
             out_of_order {:?}, requeued {:?}, headers_held {}, report_dropped {:?}, \
             nonce_too_high {}; hold cost: {}; refusals {:?}; chain walked to {}; kill {:?}; \
             counters {per_node:#?}",
            self.form,
            self.form.hook(),
            self.accepted,
            self.landed,
            self.lost.len(),
            self.lost.first(),
            self.repeated.len(),
            self.repeated.first(),
            self.hooked_landed,
            self.hooked_refusals,
            hooked.out_of_order,
            hooked.requeued,
            hooked.headers_held,
            hooked.dropped,
            hooked.nonce_too_high,
            self.cost.describe(),
            self.refusals,
            self.height,
            self.kill
        )
    }
}

/// The checks of every form.
///
/// Every form: every node's `node switches` line shows the switches it was given (checked at
/// start), no node logged a line of `fleet::FATAL_LOGS`, all four validators hold the same
/// block at every height up to the lowest head (the chain walk), the hook held own certificates
/// on the hooked node and nowhere else, and its digests were re-queued. With the hold on (every
/// form but the control):
/// - every accepted transaction is in exactly one block, with no resend by the test;
/// - [`OUT_OF_ORDER`] is 0 for both causes on every node and run, and no node logged
///   [`HOLD_WARN_LOG`]; [`ORDER_UNKNOWN`] is reported, not asserted;
/// - [`INVALID_SKIPPED`] with `reason="nonce_too_high"` is 0 on every node and run;
/// - the hold was exercised on the hooked node: [`HEADERS_HELD`] > 0, digests re-queued with the
///   cause its hook stage produces ([`Form::requeue_cause`]; the other cause is reported), and
///   `header proposed` lines with `held > 0`;
/// - the orphan rule: every [`ORPHAN_LOG`] line of every node and run shows `txs_invalid=0` and
///   `txs_refused_other=0` (the only results that can lose a transaction), and each process's
///   [`ORPHAN_TXS`] by result equals the sum of its lines' `txs_<result>` fields (except the killed
///   run, whose counters are read before the kill while its log runs to the kill).
///
/// The control asserts [`OUT_OF_ORDER`] > 0 and [`HEADERS_HELD`] = 0 on the hooked node, and
/// reports what was lost. Every form checks that each run of the hooked node logged [`HOOK_LOG`]
/// lines, all at the stage of [`Form::hook`] and on rounds divisible by its rate.
fn check_form(
    form: Form,
    fleet: &Fleet,
    runs: &[(usize, u32)],
    measured: &Measured,
    drained: Option<eyre::Report>,
) -> eyre::Result<()> {
    let test = measured.test.as_str();
    let mut failed = Vec::new();
    for &(node, run) in runs {
        if let Err(e) = fleet.assert_no_fatal_log(node, run) {
            failed.push(format!("{e}"));
        }
        if node != HOOKED && !lines_with(test, node, run, HOOK_LOG, None)?.is_empty() {
            failed
                .push(format!("node {node} run {run} logged `{HOOK_LOG}`: the hook is not on it"));
        }
    }
    let hooked = measured.hooked();
    let (stage, _, every) = form.hook();
    for &(node, run) in runs.iter().filter(|(node, _)| *node == HOOKED) {
        let holds = lines_with(test, node, run, HOOK_LOG, None)?;
        let wrong = holds
            .iter()
            .filter(|line| {
                str_field(line, "stage") != Some(stage)
                    || log_field(line, "round").is_none_or(|round| round % every != 0)
            })
            .collect::<Vec<_>>();
        if holds.is_empty() || !wrong.is_empty() {
            failed.push(format!(
                "the hooked node's run {run} logged {} `{HOOK_LOG}` lines, {} of them not at \
                 stage {stage} on a round divisible by {every} (first {:?}): the hook is not set \
                 as given",
                holds.len(),
                wrong.len(),
                wrong.first()
            ));
        }
    }
    if hooked.requeued.values().sum::<f64>() == 0.0 {
        failed.push("the hooked node re-queued no digest: its own chain never broke".to_owned());
    }
    if form.hold() {
        if let Some(e) = drained {
            failed.push(format!("{e:#}"));
        }
        if !measured.lost.is_empty() || !measured.repeated.is_empty() {
            failed.push(format!(
                "{} accepted transactions are in no block (first {:?}) and {} in more than one \
                 (first {:?})",
                measured.lost.len(),
                measured.lost.first(),
                measured.repeated.len(),
                measured.repeated.first()
            ));
        }
        for (node, run, counters) in &measured.counters {
            if counters.out_of_order_covered() > 0.0 {
                let warned = lines_with(test, *node, *run, HOLD_WARN_LOG, None)?;
                let resent = lines_with(test, *node, *run, RESENT_LOG, Some("out_of_order"))?;
                failed.push(format!(
                    "node {node} run {run} re-queued digests out of order with the hold on: {:?}; \
                     warnings {warned:#?}; re-queues {resent:#?}",
                    counters.out_of_order
                ));
            }
            if counters.nonce_too_high > 0.0 {
                failed.push(format!(
                    "node {node} run {run} skipped {} transactions as nonce too high",
                    counters.nonce_too_high
                ));
            }
        }
        for &(node, run) in runs {
            let warned = lines_with(test, node, run, HOLD_WARN_LOG, None)?;
            if !warned.is_empty() {
                failed.push(format!("node {node} run {run} logged {warned:#?}"));
            }
        }
        if hooked.headers_held == 0.0 || measured.cost.held_headers == 0 {
            failed.push(format!(
                "the hold never held a lane on the hooked node: {HEADERS_HELD} {}, `header \
                 proposed` lines with held > 0: {}",
                hooked.headers_held, measured.cost.held_headers
            ));
        }
        let cause = form.requeue_cause();
        if hooked.requeued.get(cause).copied().unwrap_or_default() == 0.0 {
            failed.push(format!(
                "the hooked node re-queued no digest with cause {cause}, the path its hook stage \
                 {:?} breaks the own chain through: {:?}",
                form.hook().0,
                hooked.requeued
            ));
        }
        failed.extend(check_orphans(form, test, runs, measured)?);
    } else if hooked.headers_held != 0.0 {
        failed.push(format!(
            "the control held {} headers on the hooked node with the switch off",
            hooked.headers_held
        ));
    } else if hooked.out_of_order_covered() == 0.0 {
        failed.push(format!(
            "the control re-queued nothing out of order on the hooked node: {:?}",
            hooked.out_of_order
        ));
    }
    eyre::ensure!(failed.is_empty(), "{} checks failed: {failed:#?}", failed.len());
    Ok(())
}

/// The orphan rule of [`check_form`], one message per breach.
fn check_orphans(
    form: Form,
    test: &str,
    runs: &[(usize, u32)],
    measured: &Measured,
) -> eyre::Result<Vec<String>> {
    let mut failed = Vec::new();
    for &(node, run) in runs {
        let (lines, sums) = orphan_lines(test, node, run)?;
        for result in ["invalid", "refused_other"] {
            let value = sums.get(result).copied().unwrap_or_default();
            if value > 0.0 {
                failed.push(format!(
                    "node {node} run {run}: its {lines} `{ORPHAN_LOG}` lines show txs_{result} \
                     {value}"
                ));
            }
        }
        // the killed run's counters are read before the kill and its log runs to the kill
        if form == Form::Kill && (node, run) == (HOOKED, 0) {
            continue;
        }
        let Some((.., counters)) =
            measured.counters.iter().find(|(n, r, _)| (*n, *r) == (node, run))
        else {
            continue;
        };
        for result in ORPHAN_RESULTS {
            let counted = counters.orphan_txs.get(result).copied().unwrap_or_default();
            let logged = sums.get(result).copied().unwrap_or_default();
            if counted != logged {
                failed.push(format!(
                    "node {node} run {run}: {ORPHAN_TXS}{{result={result}}} is {counted}, its \
                     {lines} `{ORPHAN_LOG}` lines sum to {logged}"
                ));
            }
        }
    }
    Ok(failed)
}
