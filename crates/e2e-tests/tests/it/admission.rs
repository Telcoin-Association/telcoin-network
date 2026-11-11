//! The RPC admission bound.
//!
//! With `rpc_admission_backlog_outputs: K` an active validator refuses `eth_sendRawTransaction`
//! with JSON-RPC `-32003` "txpool is full" once its engine is `K` or more consensus outputs behind
//! what the node forwarded to it, and admits again only once the backlog is below `K - 1` (at 0 for
//! `K` = 2 and `K` = 1). The check runs before decoding, and only a submission re-evaluates it, so
//! these tests read the gate with [`gate_closed`]: a submission of bytes that do not decode, which
//! a closed gate refuses with `-32003` and an open one refuses as undecodable, so the probe itself
//! never reaches a pool.
//!
//! One validator's engine is put behind with the execution-delay test hook
//! (`TN_TEST_EXECUTION_DELAY_MS`), which sleeps before each output that carries batches.

use super::sealed_txs::{
    assert_exactly_once, metric_sum, nonce, poll_until, refusal_class, scrape, validator_names,
    wait_for_landing, walk_chain, Fleet, GaugeWatch, Load, SenderReport, NODE_SWITCHES,
};
use e2e_tests::FleetOverrides;
use eyre::WrapErr as _;
use jsonrpsee::{
    core::{client::ClientT as _, ClientError},
    http_client::HttpClient,
    rpc_params,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tn_reth::{test_utils::TransactionFactory, RethChainSpec};
use tn_types::{keccak256, Address, Bytes, B256, U256};

/// Counter: submissions an active validator refused at the gate.
const REFUSED: &str = "tn_reth_rpc_admission_refused_total";

/// Gauge: the last output forwarded to the engine minus the last executed.
const BACKLOG: &str = "tn_node_execution_backlog_outputs";

/// Environment variable of the engine's execution-delay test hook.
const EXECUTION_DELAY_ENV: &str = "TN_TEST_EXECUTION_DELAY_MS";

/// The validator whose engine [`test_backlog_bound_refuses_then_readmits`] slows.
const SLOW: usize = 1;

/// Milliseconds the slowed engine sleeps before each output with batches.
///
/// The slow-execution restart test measured this delay against the e2e committee under load: the
/// node falls behind by about two outputs a second and stays an active voter, well below the
/// commit run-ahead bound over a minute of load.
const SLOW_DELAY_MS: &str = "500";

/// The validators the load of [`test_backlog_bound_refuses_then_readmits`] submits to.
const FAST: [usize; 3] = [0, 2, 3];

/// Load senders per fast validator.
const SENDERS_PER_FAST: usize = 2;

/// Time limit for a gate to close under load.
const CLOSE_LIMIT: Duration = Duration::from_secs(120);

/// The JSON-RPC code of a refusal at the gate.
const POOL_FULL_CODE: i32 = -32003;

/// The message of a refusal at the gate.
const POOL_FULL_MESSAGE: &str = "txpool is full";

/// Fee cap of the probe transfers, in wei.
const GAS_PRICE: u128 = 100;

/// A validator executing each output late refuses submissions with `-32003` "txpool is full"
/// while it is two outputs behind, while the other three admit; the refused transaction is in no
/// pool and does not land until it is submitted again, and once the load stops and the engine
/// catches up the same bytes are admitted and land.
///
/// All four validators run `rpc_admission_backlog_outputs: 2`; validator 2's engine sleeps
/// [`SLOW_DELAY_MS`] before each output with batches. The load (two senders per fast validator,
/// [`super::sealed_txs::WINDOW`] in flight each) submits to the three others only.
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_backlog_bound_refuses_then_readmits() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_backlog_bound("backlog_bound_refuses_then_readmits", 2).await
}

/// The control of [`test_backlog_bound_refuses_then_readmits`]: the same slow validator and load
/// with the bound at 0. The slow validator admits every submission while it is behind, and no
/// node serves the [`REFUSED`] series.
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_backlog_bound_off_admits_slow_validator() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_backlog_bound("backlog_bound_off_admits_slow_validator", 0).await
}

/// Start the fleet of [`test_backlog_bound_refuses_then_readmits`] with the bound at `bound` and
/// run its checks (or the control's at 0).
async fn run_backlog_bound(test: &'static str, bound: u32) -> eyre::Result<()> {
    let overrides = FleetOverrides::default()
        .parameter(validator_names(), "rpc_admission_backlog_outputs", bound.to_string())
        .env([validator_names()[SLOW]], EXECUTION_DELAY_ENV, SLOW_DELAY_MS);
    // the load senders first, then one probe sender per validator and spares for the slow one
    let load_senders = FAST.len() * SENDERS_PER_FAST;
    let fleet = Fleet::start(test, overrides, load_senders + 8, false).await?;
    let checked = if bound > 0 {
        bound_checks(&fleet, bound, load_senders).await
    } else {
        unbounded_checks(&fleet, load_senders).await
    };
    checked.wrap_err(format!("{test} failed; node logs in crates/e2e-tests/test_logs/{test}/"))
}

/// The checks of [`test_backlog_bound_refuses_then_readmits`].
async fn bound_checks(fleet: &Fleet, bound: u32, load_senders: usize) -> eyre::Result<()> {
    for node in 0..4 {
        let field = format!("rpc_admission_backlog_outputs={bound}");
        fleet.wait_for_log(node, 0, NODE_SWITCHES, &[field]).await?;
    }
    // an enabled bound serves its counter at zero before any refusal
    let before = fleet.metric_sums(REFUSED, None)?;
    eyre::ensure!(
        before.iter().all(|value| *value == 0.0),
        "{REFUSED} before the load: {before:?}"
    );

    let mut probes = Probes::new(fleet, load_senders);
    let backlog = GaugeWatch::start(fleet.metrics.to_vec(), BACKLOG);
    let targets = (0..load_senders).map(|i| FAST[i % FAST.len()]).collect();
    let load = Load::start(fleet, targets);
    let started = Instant::now();
    let refused = refuse_on_slow_validator(fleet, &mut probes).await;
    let others = match &refused {
        Ok(_) => admit_on_fast_validators(fleet, &mut probes).await,
        Err(_) => Ok(Vec::new()),
    };
    let reports = load.stop().await?;
    let backlog_max = backlog.stop();
    let (refused_tx, closed_after) = refused?;
    let others = others?;
    let counter = fleet.metric_sums(REFUSED, None)?;
    eyre::ensure!(
        counter[SLOW] > 0.0,
        "validator-{} refused with -32003 but its {REFUSED} reads {counter:?}",
        SLOW + 1
    );

    // a refused transaction is in no pool
    for client in &fleet.clients {
        let known: Option<Value> =
            client.request("eth_getTransactionByHash", rpc_params![refused_tx.hash]).await?;
        eyre::ensure!(known.is_none(), "refused {} is known to a node: {known:?}", refused_tx.hash);
    }

    let mut reports = reports;
    reports.extend(probes.reports());
    let landed = wait_for_landing(fleet, &reports).await?;
    // caught up: the refused transaction has not landed without a resubmission
    for client in &fleet.clients {
        let landed = nonce(client, refused_tx.sender).await?;
        eyre::ensure!(landed == 0, "refused {} landed without a resubmission", refused_tx.hash);
    }
    // the gate reopens once the slow engine has executed everything forwarded to it
    let slow = &fleet.clients[SLOW];
    let reopened = Instant::now();
    poll_until(Duration::from_secs(60), "the slow validator's gate to reopen", || async {
        Ok(!gate_closed(slow).await?)
    })
    .await?;
    let reopened = reopened.elapsed();
    // the same bytes, once, are admitted and land
    let answer: B256 = slow
        .request("eth_sendRawTransaction", rpc_params![&refused_tx.raw])
        .await
        .wrap_err("the caught-up slow validator refused the resubmission")?;
    eyre::ensure!(answer == refused_tx.hash, "resubmission answered {answer}");
    reports.push(SenderReport {
        address: refused_tx.sender,
        target: SLOW,
        hashes: vec![refused_tx.hash],
        raws: vec![refused_tx.raw.clone()],
        refusals: BTreeMap::new(),
    });
    wait_for_landing(fleet, &reports).await?;
    let walk = walk_chain(&fleet.clients).await?;
    let counts = assert_exactly_once(&reports, &walk)?;

    eprintln!(
        "{}: delay {SLOW_DELAY_MS} ms on validator-{}, load {load_senders} senders x window {} \
         on validators {FAST:?}; gate closed {closed_after:?} into the load ({} accepted probes \
         before it); refused {} with {POOL_FULL_CODE} {POOL_FULL_MESSAGE:?}; fast validators \
         admitted after (refusals, wait) {others:?}; {REFUSED} per node {counter:?}; {BACKLOG} \
         max per node {backlog_max:?}; load ran {:?}, drained {landed:?}, gate reopened {reopened:?} \
         after; {} txs each in exactly one block ({} load refusals {:?}); chain height {}",
        fleet.test,
        SLOW + 1,
        super::sealed_txs::WINDOW,
        probes.early_accepted,
        refused_tx.hash,
        started.elapsed(),
        counts.submitted,
        counts.refusals,
        counts.refusal_classes,
        walk.height,
    );
    Ok(())
}

/// The checks of [`test_backlog_bound_off_admits_slow_validator`]: two more senders submit to the
/// slow validator, and every one of their submissions is admitted at once while it falls at least
/// two outputs behind.
async fn unbounded_checks(fleet: &Fleet, load_senders: usize) -> eyre::Result<()> {
    for node in 0..4 {
        let field = "rpc_admission_backlog_outputs=0".to_string();
        fleet.wait_for_log(node, 0, NODE_SWITCHES, &[field]).await?;
    }
    let backlog = GaugeWatch::start(fleet.metrics.to_vec(), BACKLOG);
    let mut targets = (0..load_senders).map(|i| FAST[i % FAST.len()]).collect::<Vec<_>>();
    targets.extend([SLOW, SLOW]);
    let load = Load::start(fleet, targets);
    let started = Instant::now();
    let slow_metrics = &fleet.metrics[SLOW];
    let behind =
        poll_until(CLOSE_LIMIT, "the slow validator to fall two outputs behind", || async {
            Ok(metric_sum(&scrape(slow_metrics)?, BACKLOG, None)?.unwrap_or(0.0) >= 2.0)
        })
        .await;
    let behind_after = started.elapsed();
    if behind.is_ok() {
        // keep submitting to it while it is behind
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    let reports = load.stop().await?;
    let backlog_max = backlog.stop();
    behind?;
    let slow_refusals = reports
        .iter()
        .filter(|report| report.target == SLOW)
        .flat_map(|report| report.refusals.iter())
        .collect::<Vec<_>>();
    eyre::ensure!(
        slow_refusals.is_empty(),
        "the slow validator refused submissions with the bound at 0: {slow_refusals:?}"
    );
    let slow_admitted: usize =
        reports.iter().filter(|report| report.target == SLOW).map(|r| r.hashes.len()).sum();
    let landed = wait_for_landing(fleet, &reports).await?;
    let walk = walk_chain(&fleet.clients).await?;
    let counts = assert_exactly_once(&reports, &walk)?;
    for addr in &fleet.metrics {
        let body = scrape(addr)?;
        eyre::ensure!(
            metric_sum(&body, REFUSED, None)?.is_none(),
            "{addr} serves {REFUSED} with the bound at 0"
        );
    }
    eprintln!(
        "{}: bound 0, delay {SLOW_DELAY_MS} ms on validator-{}: two outputs behind \
         {behind_after:?} into the load; {slow_admitted} submissions to it admitted at once; \
         {BACKLOG} max per node {backlog_max:?}; drained {landed:?}; {} txs each in exactly one \
         block ({} refusals {:?}); no {REFUSED} series",
        fleet.test,
        SLOW + 1,
        counts.submitted,
        counts.refusals,
        counts.refusal_classes,
    );
    Ok(())
}

/// Execution delay of every validator in [`test_observer_forward_survives_closed_gates`].
const ALL_SLOW_DELAY_MS: &str = "300";

/// Probe transactions [`test_observer_forward_survives_closed_gates`] submits to the observer.
const OBSERVER_PROBES: usize = 4;

/// The observer forwarder's counter of transactions it kept and queued again because no
/// validator accepted them.
const REQUEUED: &str = "tn_reth_forwarded_txns_requeued_total";

/// A transaction an observer accepted while every validator's gate was closed is not lost: its
/// forwarder meets the closed gates, keeps the transaction, and it lands exactly once after the
/// gates reopen. The observer itself never answers `-32003`.
///
/// All four validators run `rpc_admission_backlog_outputs: 1` (closed while any output is in
/// flight) and execute each output with batches [`ALL_SLOW_DELAY_MS`] late. A load submitted to
/// the validators runs until all four gates read closed at once and keeps them closed while
/// [`OBSERVER_PROBES`] probes go to the observer: every probe must be answered with its hash, and
/// the observer's forwarder must requeue ([`REQUEUED`] rises, so a forward pass found no validator
/// that admitted). Then the load stops, the gates reopen, and every probe and load transaction
/// must be in exactly one block.
#[ignore = "should not run with a default cargo test"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_observer_forward_survives_closed_gates() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let test = "observer_forward_survives_closed_gates";
    let overrides = FleetOverrides::default()
        .parameter(validator_names(), "rpc_admission_backlog_outputs", "1")
        .env(validator_names(), EXECUTION_DELAY_ENV, ALL_SLOW_DELAY_MS);
    let load_senders = 8;
    let fleet = Fleet::start(test, overrides, load_senders + OBSERVER_PROBES, true).await?;
    observer_checks(&fleet, load_senders)
        .await
        .wrap_err(format!("{test} failed; node logs in crates/e2e-tests/test_logs/{test}/"))
}

/// The checks of [`test_observer_forward_survives_closed_gates`].
async fn observer_checks(fleet: &Fleet, load_senders: usize) -> eyre::Result<()> {
    for node in 0..4 {
        let field = "rpc_admission_backlog_outputs=1".to_string();
        fleet.wait_for_log(node, 0, NODE_SWITCHES, &[field]).await?;
    }
    let (observer, observer_metrics) =
        fleet.observer.clone().ok_or_else(|| eyre::eyre!("the fleet runs no observer"))?;

    let load = Load::start(fleet, (0..load_senders).map(|i| i % 4).collect());
    let started = Instant::now();
    let probed = probe_observer_behind_closed_gates(fleet, &observer, &observer_metrics).await;
    let mut reports = load.stop().await?;
    let (probes, closed_after, requeued_after) = probed?;

    reports.extend(probes.reports());
    let landed = wait_for_landing(fleet, &reports).await?;
    let walk = walk_chain(&fleet.clients).await?;
    let counts = assert_exactly_once(&reports, &walk)?;
    let observer_body = scrape(&observer_metrics)?;
    let forward_series = observer_body
        .lines()
        .filter(|line| line.starts_with("tn_reth_forward") && !line.ends_with(" 0"))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    eprintln!(
        "{}: bound 1 and delay {ALL_SLOW_DELAY_MS} ms on all four; all gates closed \
         {closed_after:?} into the load; {OBSERVER_PROBES} probes answered by the observer with \
         their hashes; the observer requeued {requeued_after:?} after the probes; the load ran \
         {:?}; all landed {landed:?} after it stopped; {} txs each in exactly one block ({} load \
         refusals {:?}); {REFUSED} per validator {:?}; observer forward series {forward_series:?}",
        fleet.test,
        started.elapsed(),
        counts.submitted,
        counts.refusals,
        counts.refusal_classes,
        fleet.metric_sums(REFUSED, None)?,
    );
    Ok(())
}

/// While the load keeps every validator's gate closed, submit [`OBSERVER_PROBES`] probes to the
/// observer, each answered with its hash, and wait until the observer's forwarder has requeued
/// (every validator refused a forward pass). Returns the probes, when all gates first read closed
/// and how long after the probes the requeue showed.
async fn probe_observer_behind_closed_gates(
    fleet: &Fleet,
    observer: &HttpClient,
    observer_metrics: &str,
) -> eyre::Result<(Probes, Duration, Duration)> {
    let started = Instant::now();
    let clients = &fleet.clients;
    poll_until(CLOSE_LIMIT, "all four gates to read closed at once", || async {
        for client in clients {
            if !gate_closed(client).await? {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await?;
    let closed_after = started.elapsed();
    let requeued_before = metric_sum(&scrape(observer_metrics)?, REQUEUED, None)?.unwrap_or(0.0);
    let mut probes = Probes::new(fleet, fleet.senders.len() - OBSERVER_PROBES);
    for _ in 0..OBSERVER_PROBES {
        let probe = probes.next_tx(4)?;
        let answer = observer
            .request::<B256, _>("eth_sendRawTransaction", rpc_params![&probe.raw])
            .await
            .map_err(|error| {
                eyre::eyre!("the observer refused {}: {}", probe.hash, refusal_class(&error))
            })?;
        eyre::ensure!(answer == probe.hash, "the observer answered {answer} for {}", probe.hash);
    }
    let probed = Instant::now();
    poll_until(
        Duration::from_secs(60),
        "the observer's forwarder to requeue behind the closed gates",
        || async {
            let requeued = metric_sum(&scrape(observer_metrics)?, REQUEUED, None)?.unwrap_or(0.0);
            Ok(requeued > requeued_before)
        },
    )
    .await?;
    Ok((probes, closed_after, probed.elapsed()))
}

/// Refuse a real transaction at the slow validator's gate: wait until [`gate_closed`] reads it
/// closed, then submit a fresh probe transaction. A probe admitted because the gate reopened in
/// between is kept (it must land) and the wait starts over with the next probe sender. Returns
/// the refused probe and how long into the load the gate first read closed.
async fn refuse_on_slow_validator(
    fleet: &Fleet,
    probes: &mut Probes,
) -> eyre::Result<(ProbeTx, Duration)> {
    let started = Instant::now();
    let slow = &fleet.clients[SLOW];
    let mut closed_after = None;
    for _ in 0..5 {
        poll_until(CLOSE_LIMIT, "the slow validator's gate to close", || async {
            gate_closed(slow).await
        })
        .await?;
        closed_after.get_or_insert(started.elapsed());
        let probe = probes.next_tx(SLOW)?;
        match slow.request::<B256, _>("eth_sendRawTransaction", rpc_params![&probe.raw]).await {
            Ok(_) => probes.early_accepted += 1,
            Err(ClientError::Call(object)) => {
                eyre::ensure!(
                    object.code() == POOL_FULL_CODE && object.message() == POOL_FULL_MESSAGE,
                    "the slow validator refused with {} {:?}, not {POOL_FULL_CODE} \
                     {POOL_FULL_MESSAGE:?}",
                    object.code(),
                    object.message()
                );
                probes.forget_last(SLOW);
                return Ok((probe, closed_after.unwrap_or_default()));
            }
            Err(other) => eyre::bail!("probe submission failed: {other}"),
        }
    }
    eyre::bail!("the slow validator's gate reopened before each of five probes")
}

/// While the slow validator refuses, each fast validator admits a probe within 10 s; returns, per
/// fast validator, the refusals it answered first and how long it took.
async fn admit_on_fast_validators(
    fleet: &Fleet,
    probes: &mut Probes,
) -> eyre::Result<Vec<(usize, u32, Duration)>> {
    let mut admitted = Vec::new();
    for node in FAST {
        let probe = probes.next_tx(node)?;
        let started = Instant::now();
        let mut refusals = 0;
        loop {
            match fleet.clients[node]
                .request::<B256, _>("eth_sendRawTransaction", rpc_params![&probe.raw])
                .await
            {
                Ok(_) => break,
                Err(error) => {
                    refusals += 1;
                    eyre::ensure!(
                        started.elapsed() < Duration::from_secs(10),
                        "validator-{} refused a probe for 10 s while validator-{} was behind: {}",
                        node + 1,
                        SLOW + 1,
                        refusal_class(&error)
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        admitted.push((node + 1, refusals, started.elapsed()));
    }
    Ok(admitted)
}

/// Whether the gate of the validator behind `client` is closed: bytes that do not decode are
/// refused with [`POOL_FULL_CODE`] by a closed gate and as undecodable by an open one.
async fn gate_closed(client: &HttpClient) -> eyre::Result<bool> {
    match client.request::<B256, _>("eth_sendRawTransaction", rpc_params!["0x00"]).await {
        Ok(hash) => eyre::bail!("a node admitted undecodable bytes as {hash}"),
        Err(ClientError::Call(object)) => Ok(object.code() == POOL_FULL_CODE),
        Err(other) => Err(other.into()),
    }
}

/// A signed probe transfer.
#[derive(Clone, Debug)]
pub(super) struct ProbeTx {
    /// The signer.
    pub(super) sender: Address,
    /// The transaction hash.
    pub(super) hash: B256,
    /// The 0x-prefixed encoded transaction.
    pub(super) raw: String,
}

/// Probe senders: the fleet senders after the load's, each signing nonce 0 once.
pub(super) struct Probes {
    /// Unused probe senders.
    spare: Vec<TransactionFactory>,
    /// The chain they sign for.
    chain: Arc<RethChainSpec>,
    /// The probes sent, by the validator they went to.
    sent: Vec<(usize, ProbeTx)>,
    /// Probes the slow validator admitted while it was meant to be refusing.
    pub(super) early_accepted: u32,
}

impl Probes {
    /// Take the fleet's senders from index `first` on as probe senders.
    pub(super) fn new(fleet: &Fleet, first: usize) -> Self {
        Self {
            spare: fleet.senders[first..].iter().rev().cloned().collect(),
            chain: fleet.chain.clone(),
            sent: Vec::new(),
            early_accepted: 0,
        }
    }

    /// Sign a transfer at nonce 0 from the next spare sender, recorded as sent to `node`.
    pub(super) fn next_tx(&mut self, node: usize) -> eyre::Result<ProbeTx> {
        let mut factory =
            self.spare.pop().ok_or_else(|| eyre::eyre!("no funded probe sender left"))?;
        let raw = factory.create_eip1559_encoded(
            self.chain.clone(),
            Some(21_000),
            GAS_PRICE,
            Some(Address::from_slice(&[0x5e; 20])),
            U256::from(1),
            Bytes::new(),
        );
        let probe = ProbeTx {
            sender: factory.address(),
            hash: keccak256(&raw),
            raw: const_hex::encode_prefixed(&raw),
        };
        self.sent.push((node, probe.clone()));
        Ok(probe)
    }

    /// Drop the last probe sent to `node` from the record (it was refused, so it must not land).
    pub(super) fn forget_last(&mut self, node: usize) {
        if let Some(index) = self.sent.iter().rposition(|(to, _)| *to == node) {
            self.sent.remove(index);
        }
    }

    /// The recorded (admitted) probes as one-transaction sender reports.
    pub(super) fn reports(&self) -> Vec<SenderReport> {
        self.sent
            .iter()
            .map(|(node, probe)| SenderReport {
                address: probe.sender,
                target: *node,
                hashes: vec![probe.hash],
                raws: vec![probe.raw.clone()],
                refusals: BTreeMap::new(),
            })
            .collect()
    }
}
