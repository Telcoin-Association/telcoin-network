//! A network founded on a scheduled genesis.
//!
//! The genesis ceremony can fix the genesis time ahead of the ceremony (`--genesis-timestamp`), so
//! validators can be started well before their chain begins. Epoch 0 then opens with genesis still
//! ahead. Every node is up, connected and observable while it waits, but no validator may create or
//! certify a header before genesis: the proposer holds its first header until then, and voters
//! refuse an epoch-0 header created earlier. So no consensus output and no execution block can
//! exist before genesis, and from genesis on the chain runs as any other.
//!
//! [`test_epoch_scheduled_genesis`] checks this with real node processes. Its first assertion is
//! the absence poll ([`absence_poll`]): every validator's execution block number and consensus
//! block number stay 0 until two seconds before genesis. A node that does not hold its headers for
//! genesis, such as a build that has the ceremony flag but no genesis floor, commits as soon as a
//! quorum is up and fails that assertion first, with a message that begins
//! [`PRODUCED_BEFORE_GENESIS`]. Every check of a log line or a metric that only the floor produces
//! is asserted after the poll.

use super::common::{
    address_from_word, assert_epoch_records_verify, current_epoch, force_kill_and_reap, get_key,
    kill_child, log_field, log_line_matches, node_log_lines, node_log_path, parse_hex_u64,
    pin_fork_epochs, plain_log_lines, read_consensus_headers, scrape_metric_value, scrape_metrics,
    send_tel, send_term, start_observer, start_validator_with_args, sum_metric_samples,
    ProcessGuard, EVM_TIMESTAMP_CLAMPED_SERIES, RPC_REQUEST_TIMEOUT, WEI_PER_TEL,
};
use alloy::providers::ProviderBuilder;
use e2e_tests::{config_local_testnet_with_genesis_timestamp, NodeEndpoints, TestBinary};
use eyre::WrapErr as _;
use jsonrpsee::{
    core::{client::ClientT as _, traits::ToRpcParams, DeserializeOwned},
    http_client::HttpClientBuilder,
    rpc_params,
};
use serde_json::Value;
use std::{
    future::Future,
    io::{Read as _, Write as _},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Child,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tn_reth::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
use tn_types::{get_available_tcp_port, Header, NodeMode};

/// The test's name: its `test_logs` directory and the prefix of its node names.
const TEST: &str = "scheduled_genesis";

/// How far after the ceremony genesis is scheduled. The ceremony and the nodes' start-up use part
/// of it, so every deadline below is measured from the genesis value the ceremony wrote.
const GENESIS_LEAD: Duration = Duration::from_secs(90);

/// Epoch duration in seconds: epoch 0 starts at genesis and closes at genesis plus this.
const EPOCH_DURATION: u32 = 40;

/// The absence poll takes its last sample this many milliseconds before genesis.
const POLL_STOP_MS: i64 = 2_000;

/// Every validator must have answered the absence poll within this many milliseconds of the poll's
/// end, so the poll measured each of them up to the end, the restarted one included.
const POLL_FRESHNESS_MS: u64 = 10_000;

/// Pause between two rounds of the absence poll.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Pause between two attempts of a bounded wait ([`Schedule::wait`]).
const WAIT_INTERVAL: Duration = Duration::from_millis(250);

/// Deadline, in milliseconds after genesis, for everything observed while the nodes wait: the end
/// of the absence poll.
const WAIT_CHECKS_BY_MS: i64 = -POLL_STOP_MS;

/// The restarted validator gets its SIGTERM no later than this, in milliseconds after genesis,
/// leaving time for its restart and its checks before the poll ends.
const RESTART_BY_MS: i64 = -45_000;

/// How long the restarted validator may take to exit on its own after SIGTERM. A shutdown takes
/// about 2.3 s; one blocked by the proposer's hold would last until genesis.
const SIGTERM_GRACE: Duration = Duration::from_secs(15);

/// Deadline, in milliseconds after genesis, for block 1 on every validator.
const FIRST_BLOCK_BY_MS: i64 = 30_000;

/// Deadline, in milliseconds after genesis, for the two transfers sent after genesis and for
/// block 3.
const TRANSFERS_BY_MS: i64 = 60_000;

/// Deadline, in milliseconds after genesis, for validator-1 to enter epoch 1: two epoch durations.
const EPOCH_CLOSE_BY_MS: i64 = 2 * EPOCH_DURATION as i64 * 1_000;

/// How many seconds after its boundary epoch 0's closing block may be timestamped. The boundary
/// closes on the first commit at or past it, and commits come every round (250 to 500 ms here).
const EPOCH_CLOSE_SLACK_SECS: u64 = 10;

/// Deadline, in milliseconds after genesis, for the epoch-0 records and the late joiner's catch-up.
const CATCH_UP_BY_MS: i64 = 150_000;

/// Index of the validator stopped and restarted while it waits for genesis (validator-4).
const RESTARTED: usize = 3;

/// Instance number of the late-joining observer, for its log file.
const OBSERVER: usize = 4;

/// The BLS passphrase the start helpers give every node.
const PASSPHRASE: &str = "restart_test";

/// Word the transfers' recipient address is derived from.
const TRANSFER_TARGET: &str = "scheduled-genesis-target";

/// Start of the absence poll's failure message.
const PRODUCED_BEFORE_GENESIS: &str = "block produced before genesis";

/// The countdown's line while genesis is ahead (target `epoch-manager`), with `genesis_timestamp`
/// and `remaining_secs`. The node's log format prints no target, so the lines below are matched by
/// their message, which tells each of them apart.
const COUNTDOWN_WAITING: &str = "waiting for genesis: epoch 0 opens in";

/// The countdown's last line, at genesis (target `epoch-manager`).
const COUNTDOWN_OPEN: &str = "genesis time reached: epoch 0 is open";

/// The proposer's line when it starts holding its first epoch-0 header (target
/// `primary::proposer`).
const PROPOSER_HOLDING: &str = "waiting for genesis before proposing a header";

/// The proposer's line when genesis releases the held header (target `primary::proposer`).
const PROPOSER_RELEASED: &str = "genesis time reached, proposing the held header";

/// Prometheus series of the gauge holding the current epoch's start, in unix seconds.
const START_TIMESTAMP_SERIES: &str = "tn_epoch_start_timestamp_seconds";

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Four validators started 90 s before a scheduled genesis wait for it, then start the chain.
///
/// The ceremony schedules genesis 90 s ahead with a 40 s epoch, and the validators start right
/// after it. In this order, the run fails unless:
///
/// - no validator reports an execution block or a consensus block before genesis, sampled until two
///   seconds before it ([`absence_poll`], the first assertion);
/// - while they wait, every validator serves RPC, reports `CvvActive` through `tn_nodeMode`, has
///   peers (`net_peerCount`), serves `tn_admissionSnapshot` and reports `reachable` on
///   `/health/network`; validator-4, stopped with SIGTERM while its proposer holds its first
///   header, exits on its own and comes back to the same checks ([`observe_the_wait`]);
/// - every validator's log has the countdown and the proposer's hold lines for this genesis, the
///   restarted one in both of its runs, and its metrics showed [`START_TIMESTAMP_SERIES`] at the
///   genesis timestamp before genesis;
/// - a transfer submitted before genesis executes in block 1, and block 1 is timestamped at or
///   after genesis on every validator;
/// - an observer started once block 3 exists catches up, and never logs a wait for genesis;
/// - epoch 0 closes at genesis plus 40 s, with a certified record every validator agrees on;
/// - no engine clamped an EVM timestamp ([`EVM_TIMESTAMP_CLAMPED_SERIES`] reads 0), and every
///   validator logged the countdown's and the proposer's lines for genesis itself;
/// - every epoch-0 header validator-1 committed was created at or after genesis.
async fn test_epoch_scheduled_genesis() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // millisecond header timestamps from genesis, as a production build runs: the sub-second gate
    // conjoins the seed-signature fork, so both are pinned at 0. the consensus chain read at the
    // end is decoded under the same pins. pinned before the ceremony, which reads fork gates
    pin_fork_epochs(None, Some(0), None, Some(0));

    // short on purpose: node IPC socket paths are built under the temp dir
    let temp_dir = tempfile::TempDir::with_prefix("sched_gen")?;
    let genesis = config_local_testnet_with_genesis_timestamp(
        temp_dir.path(),
        Some(PASSPHRASE.to_string()),
        None,
        GENESIS_LEAD,
        EPOCH_DURATION,
    )?;
    let schedule = Schedule { genesis };
    let mut fleet = Fleet::start(e2e_tests::get_telcoin_network_binary(), temp_dir.path())?;
    let lead_at_start = schedule.genesis_ms().saturating_sub(now_ms());

    // the poll runs on its own task, so it keeps sampling while the checks below block a worker
    let mut poll = tokio::spawn(absence_poll(fleet.urls(), schedule));
    let (report, waited) = {
        let watch = observe_the_wait(&mut fleet, schedule);
        tokio::pin!(watch);
        let mut watched = None;
        let report = loop {
            tokio::select! {
                biased;
                joined = &mut poll => break joined.wrap_err("the absence poll task failed")?,
                result = &mut watch, if watched.is_none() => watched = Some(result),
            }
        };
        // the first assertion: nothing was produced before genesis
        let report = report?;
        let waited = match watched {
            Some(result) => result,
            None => watch.await,
        };
        (report, waited)
    };
    let waited = waited?;
    eyre::ensure!(
        waited.done_at < schedule.genesis_ms(),
        "the checks of the wait finished {} ms after genesis {genesis}, so they did not observe the \
         nodes waiting (the host was too slow for a {GENESIS_LEAD:?} lead)",
        waited.done_at - schedule.genesis_ms()
    );

    // what only the genesis floor produces, read now that the poll has passed
    for instance in 0..4 {
        let run = fleet.runs[instance];
        let waiting = logged(instance, run, COUNTDOWN_WAITING, genesis)?;
        eyre::ensure!(
            waiting.iter().all(|line| log_field(line, "remaining_secs").is_some_and(|s| s > 0)),
            "validator-{} logged a countdown line without a positive remaining_secs: {waiting:?}",
            instance + 1
        );
        logged(instance, run, PROPOSER_HOLDING, genesis)?;
    }
    logged(RESTARTED, 0, PROPOSER_HOLDING, genesis).wrap_err_with(|| {
        format!("validator-{} was stopped before its proposer held its first header", RESTARTED + 1)
    })?;
    eyre::ensure!(
        waited.restart_exited,
        "validator-{} did not exit within {SIGTERM_GRACE:?} of its SIGTERM while it waited for \
         genesis, so the wait held up its shutdown (killed after {:?}; \
         test_logs/{TEST}/node{RESTARTED}-run0.log)",
        RESTARTED + 1,
        waited.restart_took
    );
    for (instance, (taken_at, body)) in waited.scrapes.iter().enumerate() {
        eyre::ensure!(
            *taken_at < schedule.genesis_ms(),
            "validator-{}'s metrics were scraped {} ms after genesis, so they do not show the wait",
            instance + 1,
            taken_at - schedule.genesis_ms()
        );
        let start = sum_metric_samples(body, START_TIMESTAMP_SERIES)?;
        eyre::ensure!(
            start == Some(genesis as f64),
            "validator-{} exported {START_TIMESTAMP_SERIES} = {start:?} while it waited, not the \
             genesis timestamp {genesis}",
            instance + 1
        );
    }

    // genesis: the transfer submitted before it executes in the first block
    tokio::time::sleep(schedule.until(0)).await;
    let early_block =
        receipt_block(schedule, &fleet.url(0), &waited.early_transfer, FIRST_BLOCK_BY_MS).await?;
    eyre::ensure!(
        early_block == 1,
        "the transfer submitted before genesis executed in block {early_block}, not in block 1"
    );
    let mut first_hash = None;
    for instance in 0..4 {
        let block = block_by_number(schedule, &fleet.url(instance), 1, FIRST_BLOCK_BY_MS).await?;
        let timestamp = block.get("timestamp").and_then(parse_hex_u64).ok_or_else(|| {
            eyre::eyre!("validator-{}'s block 1 has no hex timestamp: {block}", instance + 1)
        })?;
        eyre::ensure!(
            timestamp >= genesis,
            "validator-{}'s block 1 is timestamped {timestamp}, {} s before genesis {genesis}",
            instance + 1,
            genesis - timestamp
        );
        let hash = block.get("hash").cloned();
        let expected = first_hash.get_or_insert_with(|| hash.clone());
        eyre::ensure!(
            *expected == hash,
            "validator-{}'s block 1 is {hash:?}, validator-1's is {expected:?}",
            instance + 1
        );
    }

    // two more transfers, sent through other validators, make blocks 2 and 3
    let key = get_key("test-source");
    let target = address_from_word(TRANSFER_TARGET);
    for (nonce, instance) in [(1u128, 1usize), (2, 2)] {
        let url = fleet.url(instance);
        let hash = send_tel(&url, &key, target, WEI_PER_TEL, 250, 21_000, nonce)?;
        receipt_block(schedule, &url, &hash, TRANSFERS_BY_MS).await?;
    }
    let reference = fleet.url(0);
    schedule
        .wait(TRANSFERS_BY_MS, "validator-1 reaches block 3", || async {
            Ok(block_number(&reference).await.is_ok_and(|number| number >= 3))
        })
        .await?;

    // the late joiner, started once block 3 exists
    let observer_port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no RPC port available for the observer"))?;
    let observer_url = format!("http://127.0.0.1:{observer_port}");
    fleet.guard.push(start_observer(OBSERVER, fleet.bin, &fleet.base, observer_port, TEST, 0));
    let joined_at = block_number(&reference).await?;

    // epoch 0 closes at its boundary, genesis plus the epoch duration
    let provider = ProviderBuilder::new().connect_http(reference.parse()?);
    schedule
        .wait(EPOCH_CLOSE_BY_MS, "validator-1 enters epoch 1", || async {
            Ok(current_epoch(&provider).await.is_ok_and(|snap| snap.epoch_id >= 1))
        })
        .await?;
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let epoch_zero = registry.getEpochInfo(0).call().await?;
    eyre::ensure!(
        epoch_zero.epochDuration == EPOCH_DURATION,
        "epoch 0 ran with a {} s duration, not the {EPOCH_DURATION} s the ceremony set",
        epoch_zero.epochDuration
    );
    let closing_block = registry.getEpochInfo(1).call().await?.blockHeight.saturating_sub(1);
    let closing = block_by_number(schedule, &reference, closing_block, EPOCH_CLOSE_BY_MS).await?;
    let closed_at = closing.get("timestamp").and_then(parse_hex_u64).ok_or_else(|| {
        eyre::eyre!("epoch 0's closing block {closing_block} has no hex timestamp: {closing}")
    })?;
    let boundary = genesis + u64::from(EPOCH_DURATION);
    eyre::ensure!(
        (boundary..=boundary + EPOCH_CLOSE_SLACK_SECS).contains(&closed_at),
        "epoch 0 closed with block {closing_block} at {closed_at}, {} s after genesis {genesis}: it \
         should close at genesis + {EPOCH_DURATION} s, within {EPOCH_CLOSE_SLACK_SECS} s",
        closed_at as i64 - genesis as i64
    );
    let endpoints: Vec<NodeEndpoints> = fleet
        .urls()
        .into_iter()
        .map(|http_url| NodeEndpoints { http_url, ws_url: String::new(), ipc_path: String::new() })
        .collect();
    assert_epoch_records_verify(&endpoints, 0..=0, schedule.until(CATCH_UP_BY_MS).as_secs().max(1))
        .await?;

    // the late joiner caught up, and entered epoch 0 after genesis without waiting for it
    schedule
        .wait(CATCH_UP_BY_MS, &format!("the observer catches up to block {joined_at}"), || async {
            Ok(block_number(&observer_url).await.is_ok_and(|number| number >= joined_at))
        })
        .await?;
    let observer_waits: Vec<String> = node_log_lines(TEST, OBSERVER, 0)?
        .into_iter()
        .filter(|line| line.contains("waiting for genesis"))
        .collect();
    eyre::ensure!(
        observer_waits.is_empty(),
        "the observer started after genesis logged a wait for it: {observer_waits:?}"
    );

    // the first commits came at or after genesis, so no engine had to raise a timestamp
    for (instance, addr) in fleet.metrics.iter().enumerate() {
        let clamped = tokio::task::block_in_place(|| {
            scrape_metric_value(addr, EVM_TIMESTAMP_CLAMPED_SERIES)
        })?;
        eyre::ensure!(
            clamped == 0.0,
            "validator-{} clamped {clamped} EVM timestamps up to their parent's after a scheduled \
             genesis (test_logs/{TEST}/node{instance}-run{}.log)",
            instance + 1,
            fleet.runs[instance]
        );
    }
    for instance in 0..4 {
        let run = fleet.runs[instance];
        logged(instance, run, COUNTDOWN_OPEN, genesis)?;
        logged(instance, run, PROPOSER_RELEASED, genesis)?;
    }

    // every epoch-0 header validator-1 committed, read from its stopped consensus chain
    let mut validator_1 =
        fleet.guard.take(0).ok_or_else(|| eyre::eyre!("validator-1 is not running"))?;
    tokio::task::block_in_place(|| kill_child(&mut validator_1));
    let chain = read_consensus_headers(&fleet.base.join("validator-1")).await?;
    let epoch_zero_headers: Vec<&Header> = chain
        .iter()
        .flat_map(|consensus_header| consensus_header.sub_dag.headers())
        .filter(|header| header.epoch() == 0)
        .collect();
    eyre::ensure!(!epoch_zero_headers.is_empty(), "validator-1 committed no epoch-0 header");
    let early: Vec<String> = epoch_zero_headers
        .iter()
        .filter(|header| header.created_at_ms().as_millis() < schedule.genesis_ms())
        .map(|header| {
            format!(
                "round {} by {} at {} ms",
                header.round(),
                header.author(),
                header.created_at_ms().as_millis()
            )
        })
        .collect();
    eyre::ensure!(
        early.is_empty(),
        "{} of the {} epoch-0 headers validator-1 committed were created before genesis {genesis} \
         ({} ms): {early:?}",
        early.len(),
        epoch_zero_headers.len(),
        schedule.genesis_ms()
    );
    let first_created = epoch_zero_headers
        .iter()
        .map(|header| header.created_at_ms().as_millis())
        .min()
        .unwrap_or_default();

    // stderr rather than tracing, as in the other e2e tests: a passing run shows this line under
    // nextest's `--success-output`
    eprintln!(
        "{TEST}: genesis={genesis} nodes_started_ms_before_genesis={lead_at_start} \
         poll_rounds={} poll_answers={:?} restart_exit={:?} checks_done_ms_before_genesis={} \
         first_header_ms_after_genesis={} epoch0_close_secs_after_genesis={} \
         epoch0_headers_committed={}",
        report.rounds,
        report.answered,
        waited.restart_took,
        schedule.genesis_ms() - waited.done_at,
        first_created.saturating_sub(schedule.genesis_ms()),
        closed_at - genesis,
        epoch_zero_headers.len()
    );

    fleet.guard.finish();
    Ok(())
}

/// Wall-clock times measured from the genesis timestamp the ceremony wrote.
#[derive(Debug, Clone, Copy)]
struct Schedule {
    /// Genesis in unix seconds, as read back from `genesis.yaml`.
    genesis: u64,
}

impl Schedule {
    /// Genesis in unix milliseconds.
    fn genesis_ms(self) -> u64 {
        self.genesis * 1_000
    }

    /// The unix millisecond `offset_ms` after genesis (before it when negative).
    fn at(self, offset_ms: i64) -> u64 {
        self.genesis_ms().saturating_add_signed(offset_ms)
    }

    /// How long until `offset_ms` after genesis, zero once that has passed.
    fn until(self, offset_ms: i64) -> Duration {
        Duration::from_millis(self.at(offset_ms).saturating_sub(now_ms()))
    }

    /// Poll `condition` every [`WAIT_INTERVAL`] until it holds, failing once `by_ms` after genesis
    /// has passed without it. An error from `condition` ends the wait at once.
    async fn wait<F, Fut>(self, by_ms: i64, what: &str, condition: F) -> eyre::Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = eyre::Result<bool>>,
    {
        loop {
            if condition().await? {
                return Ok(());
            }
            eyre::ensure!(
                now_ms() < self.at(by_ms),
                "{what}: not by genesis {} {} ms (genesis {}; node logs under test_logs/{TEST}/)",
                if by_ms < 0 { '-' } else { '+' },
                by_ms.unsigned_abs(),
                self.genesis
            );
            tokio::time::sleep(WAIT_INTERVAL).await;
        }
    }
}

/// The current unix time in milliseconds, from the host clock every node of the test reads.
fn now_ms() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the host clock is after the unix epoch");
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

/// The four validators and the ports each runs on; a restart keeps them.
struct Fleet {
    /// The node binary under test.
    bin: &'static TestBinary,
    /// The test's temp dir, which holds every node's datadir.
    base: PathBuf,
    /// Every node process the test started.
    guard: ProcessGuard,
    /// Each validator's HTTP RPC port.
    rpc_ports: [u16; 4],
    /// Each validator's `--metrics` address.
    metrics: [String; 4],
    /// Each validator's `--healthcheck` port.
    health: [u16; 4],
    /// The run number of each validator's current process, which names its log file.
    runs: [u32; 4],
}

impl Fleet {
    /// Allocate every validator's ports and start all four.
    fn start(bin: &'static TestBinary, base: &Path) -> eyre::Result<Self> {
        let port = |what: &str| {
            get_available_tcp_port("127.0.0.1")
                .ok_or_else(|| eyre::eyre!("no {what} port available for a validator"))
        };
        let mut rpc_ports = [0; 4];
        let mut health = [0; 4];
        let mut metrics: [String; 4] = Default::default();
        for instance in 0..4 {
            rpc_ports[instance] = port("RPC")?;
            health[instance] = port("healthcheck")?;
            metrics[instance] = format!("127.0.0.1:{}", port("metrics")?);
        }
        let mut fleet = Self {
            bin,
            base: base.to_path_buf(),
            guard: ProcessGuard::empty(),
            rpc_ports,
            metrics,
            health,
            runs: [0; 4],
        };
        for instance in 0..4 {
            let child = fleet.spawn(instance);
            fleet.guard.push(child);
        }
        Ok(fleet)
    }

    /// Start validator `instance` as its current run.
    fn spawn(&self, instance: usize) -> Child {
        let health = self.health[instance].to_string();
        start_validator_with_args(
            instance,
            self.bin,
            &self.base,
            self.rpc_ports[instance],
            TEST,
            self.runs[instance],
            &["--metrics", &self.metrics[instance], "--healthcheck", &health],
        )
    }

    /// Validator `instance`'s HTTP RPC URL.
    fn url(&self, instance: usize) -> String {
        format!("http://127.0.0.1:{}", self.rpc_ports[instance])
    }

    /// Every validator's HTTP RPC URL, in instance order.
    fn urls(&self) -> [String; 4] {
        std::array::from_fn(|instance| self.url(instance))
    }
}

/// What the absence poll measured.
#[derive(Debug)]
struct PollReport {
    /// Rounds of samples the poll took.
    rounds: usize,
    /// How many samples each validator answered.
    answered: [usize; 4],
}

/// Sample every validator's execution block number (`eth_blockNumber`) and consensus block number
/// (`tn_latestConsensusHeader`) every [`POLL_INTERVAL`], from now until [`POLL_STOP_MS`] before
/// genesis, and fail on the first sample that answered before genesis with either above 0.
///
/// The failure message begins [`PRODUCED_BEFORE_GENESIS`]. A validator that cannot answer, such as
/// one being restarted, is skipped for that round, but each must have answered within
/// [`POLL_FRESHNESS_MS`] of the poll's end, so the poll measured every one of them up to it.
async fn absence_poll(urls: [String; 4], schedule: Schedule) -> eyre::Result<PollReport> {
    let clients = urls
        .iter()
        .map(|url| HttpClientBuilder::default().request_timeout(RPC_REQUEST_TIMEOUT).build(url))
        .collect::<Result<Vec<_>, _>>()?;
    let stop = schedule.at(-POLL_STOP_MS);
    let mut report = PollReport { rounds: 0, answered: [0; 4] };
    let mut last_answer: [Option<u64>; 4] = [None; 4];
    while now_ms() < stop {
        report.rounds += 1;
        let samples = futures::future::join_all(clients.iter().map(|client| async move {
            let block: String = client.request("eth_blockNumber", rpc_params![]).await?;
            let header: Value = client.request("tn_latestConsensusHeader", rpc_params![]).await?;
            let answered_at = now_ms();
            let block = hex_u64(&block)
                .ok_or_else(|| eyre::eyre!("eth_blockNumber answered {block:?}, not a hex u64"))?;
            let consensus = header.get("number").and_then(json_u64).ok_or_else(|| {
                eyre::eyre!("tn_latestConsensusHeader has no numeric `number`: {header}")
            })?;
            Ok::<_, eyre::Report>((block, consensus, answered_at))
        }))
        .await;
        for (instance, sample) in samples.into_iter().enumerate() {
            // a node that cannot answer right now (starting, restarting) is not producing either
            let Ok((block, consensus, answered_at)) = sample else { continue };
            // a sample answered after genesis may legitimately see the chain start
            if answered_at >= schedule.genesis_ms() {
                continue;
            }
            eyre::ensure!(
                block == 0 && consensus == 0,
                "{PRODUCED_BEFORE_GENESIS}: validator-{} ({}) reported execution block {block} and \
                 consensus block {consensus} {} ms before genesis {} (poll round {}; \
                 test_logs/{TEST}/node{instance}-run*.log)",
                instance + 1,
                urls[instance],
                schedule.genesis_ms() - answered_at,
                schedule.genesis,
                report.rounds
            );
            report.answered[instance] += 1;
            last_answer[instance] = Some(answered_at);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    for (instance, last) in last_answer.iter().enumerate() {
        eyre::ensure!(
            last.is_some_and(|at| at + POLL_FRESHNESS_MS >= stop),
            "the absence poll did not hear from validator-{} ({}) in its last {POLL_FRESHNESS_MS} \
             ms before genesis - {POLL_STOP_MS} ms (last answer at {last:?}, {} answers in {} \
             rounds), so it did not measure that node up to genesis",
            instance + 1,
            urls[instance],
            report.answered[instance],
            report.rounds
        );
    }
    Ok(report)
}

/// What [`observe_the_wait`] saw, for the assertions made after the absence poll.
#[derive(Debug)]
struct Waited {
    /// Each validator's metrics scrape, taken while it waited, with the unix millisecond the
    /// scrape returned.
    scrapes: Vec<(u64, String)>,
    /// Whether the restarted validator's first process exited on its own after SIGTERM, within
    /// [`SIGTERM_GRACE`].
    restart_exited: bool,
    /// How long that process took to exit, or to be killed.
    restart_took: Duration,
    /// Hash of the transfer submitted before genesis.
    early_transfer: String,
    /// The unix millisecond the last check returned.
    done_at: u64,
}

/// Check that the validators are live and observable while they wait for genesis, restart one of
/// them during the wait, and submit a transfer.
///
/// Every wait here ends by the end of the absence poll ([`WAIT_CHECKS_BY_MS`]). Nothing here
/// asserts on a log line or a metric that only the genesis floor produces: those are recorded in
/// [`Waited`] and asserted after the poll. Validator-4's proposer hold line is only waited for,
/// without failing, so that its SIGTERM lands during the hold.
async fn observe_the_wait(fleet: &mut Fleet, schedule: Schedule) -> eyre::Result<Waited> {
    let by = WAIT_CHECKS_BY_MS;
    for instance in 0..4 {
        let url = fleet.url(instance);
        schedule
            .wait(by, &format!("validator-{} answers RPC", instance + 1), || async {
                Ok(rpc::<String>(&url, "eth_chainId", rpc_params![]).await.is_ok())
            })
            .await?;
    }

    // a transfer from a genesis-funded account, submitted while the chain has not started
    let early_transfer = send_tel(
        &fleet.url(0),
        &get_key("test-source"),
        address_from_word(TRANSFER_TARGET),
        WEI_PER_TEL,
        250,
        21_000,
        0,
    )?;

    // stop validator-4 while its proposer holds its first header, without asserting that it does
    let first_log = node_log_path(TEST, RESTARTED, 0, false);
    while now_ms() < schedule.at(RESTART_BY_MS)
        && !plain_log_lines(&first_log)
            .is_ok_and(|lines| lines.iter().any(|line| line.contains(PROPOSER_HOLDING)))
    {
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
    let (restart_exited, restart_took) = tokio::task::block_in_place(|| {
        let child = fleet
            .guard
            .get_mut(RESTARTED)
            .ok_or_else(|| eyre::eyre!("validator-{} is not running", RESTARTED + 1))?;
        send_term(child);
        let stopping = std::time::Instant::now();
        while stopping.elapsed() < SIGTERM_GRACE {
            if child.try_wait()?.is_some() {
                return Ok::<_, eyre::Report>((true, stopping.elapsed()));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        force_kill_and_reap(child);
        Ok((false, stopping.elapsed()))
    })?;
    fleet.runs[RESTARTED] += 1;
    let restarted = fleet.spawn(RESTARTED);
    // the old slot holds the reaped first process, which must not be signaled again
    drop(fleet.guard.replace(RESTARTED, restarted));

    // the others first, so the restarted validator has had time to come back when its turn comes
    let mut scrapes = Vec::with_capacity(4);
    for instance in 0..4 {
        let name = format!("validator-{} (run {})", instance + 1, fleet.runs[instance]);
        let url = fleet.url(instance);
        schedule
            .wait(by, &format!("{name} answers RPC"), || async {
                Ok(rpc::<String>(&url, "eth_chainId", rpc_params![]).await.is_ok())
            })
            .await?;
        schedule
            .wait(by, &format!("{name} reports CvvActive through tn_nodeMode"), || async {
                Ok(rpc::<NodeMode>(&url, "tn_nodeMode", rpc_params![])
                    .await
                    .is_ok_and(|mode| mode == NodeMode::CvvActive))
            })
            .await?;
        schedule
            .wait(by, &format!("{name} reports net_peerCount above 0"), || async {
                Ok(rpc::<String>(&url, "net_peerCount", rpc_params![])
                    .await
                    .ok()
                    .and_then(|count| hex_u64(&count))
                    .is_some_and(|count| count > 0))
            })
            .await?;
        schedule
            .wait(by, &format!("{name} serves tn_admissionSnapshot"), || async {
                Ok(rpc::<Value>(&url, "tn_admissionSnapshot", rpc_params![])
                    .await
                    .is_ok_and(|snapshot| lists_primary_swarm(&snapshot)))
            })
            .await?;
        let health = fleet.health[instance];
        schedule
            .wait(by, &format!("{name} reports reachable on /health/network"), || async {
                Ok(tokio::task::block_in_place(|| network_health(health)).is_ok_and(
                    |(status, snapshot)| {
                        status == 200
                            && snapshot.get("status").and_then(Value::as_str) == Some("reachable")
                    },
                ))
            })
            .await?;
        let addr = fleet.metrics[instance].as_str();
        let scrape = std::cell::RefCell::new(None);
        schedule
            .wait(by, &format!("{name} serves its metrics"), || async {
                let body = tokio::task::block_in_place(|| scrape_metrics(addr));
                let served = body.as_ref().is_ok_and(|body| body.starts_with("HTTP/1.1 200"));
                if served {
                    *scrape.borrow_mut() = body.ok().map(|body| (now_ms(), body));
                }
                Ok(served)
            })
            .await?;
        scrapes.push(scrape.into_inner().ok_or_else(|| eyre::eyre!("{name} scrape not kept"))?);
    }

    Ok(Waited { scrapes, restart_exited, restart_took, early_transfer, done_at: now_ms() })
}

/// The lines of node `instance`'s run-`run` log that carry `message` with the field
/// `genesis_timestamp=<genesis>`; an error when there is none.
fn logged(instance: usize, run: u32, message: &str, genesis: u64) -> eyre::Result<Vec<String>> {
    let fields = [format!("genesis_timestamp={genesis}")];
    let lines: Vec<String> = node_log_lines(TEST, instance, run)?
        .into_iter()
        .filter(|line| log_line_matches(line, message, &fields))
        .collect();
    eyre::ensure!(
        !lines.is_empty(),
        "{} has no `{message}` line with `{}`",
        node_log_path(TEST, instance, run, false).display(),
        fields[0]
    );
    Ok(lines)
}

/// One JSON-RPC call to `url` under [`RPC_REQUEST_TIMEOUT`], with no retry and nothing logged, so a
/// bounded wait can poll it.
async fn rpc<R: DeserializeOwned>(
    url: &str,
    method: &str,
    params: impl ToRpcParams + Send,
) -> eyre::Result<R> {
    let client = HttpClientBuilder::default().request_timeout(RPC_REQUEST_TIMEOUT).build(url)?;
    Ok(client.request(method, params).await?)
}

/// `url`'s latest execution block number.
async fn block_number(url: &str) -> eyre::Result<u64> {
    let number: String = rpc(url, "eth_blockNumber", rpc_params![]).await?;
    hex_u64(&number).ok_or_else(|| eyre::eyre!("{url} eth_blockNumber answered {number:?}"))
}

/// Execution block `number` from `url`, waited for until `by_ms` after genesis.
async fn block_by_number(
    schedule: Schedule,
    url: &str,
    number: u64,
    by_ms: i64,
) -> eyre::Result<Value> {
    let block = std::cell::RefCell::new(None);
    let tag = format!("0x{number:x}");
    schedule
        .wait(by_ms, &format!("{url} serves block {number}"), || async {
            let found: Option<Value> =
                rpc(url, "eth_getBlockByNumber", rpc_params![&tag, false]).await.ok().flatten();
            let served = found.is_some();
            *block.borrow_mut() = found;
            Ok(served)
        })
        .await?;
    block.into_inner().ok_or_else(|| eyre::eyre!("{url} block {number} not kept"))
}

/// The block the transaction `hash` executed in, as `url` reports it, waited for until `by_ms`
/// after genesis.
async fn receipt_block(schedule: Schedule, url: &str, hash: &str, by_ms: i64) -> eyre::Result<u64> {
    let number = std::cell::Cell::new(None);
    schedule
        .wait(by_ms, &format!("{url} has a receipt for transaction {hash}"), || async {
            let receipt: Option<Value> =
                rpc(url, "eth_getTransactionReceipt", rpc_params![hash]).await.ok().flatten();
            number.set(
                receipt
                    .as_ref()
                    .and_then(|receipt| receipt.get("blockNumber"))
                    .and_then(parse_hex_u64),
            );
            Ok(number.get().is_some())
        })
        .await?;
    number.get().ok_or_else(|| eyre::eyre!("{url} receipt for {hash} not kept"))
}

/// Whether an admission snapshot is at schema version 1 and reports the primary swarm.
fn lists_primary_swarm(snapshot: &Value) -> bool {
    snapshot.get("version").and_then(Value::as_u64) == Some(1)
        && snapshot.get("swarms").and_then(Value::as_array).is_some_and(|swarms| {
            swarms
                .iter()
                .any(|swarm| swarm.get("network").and_then(Value::as_str) == Some("primary"))
        })
}

/// The status code and JSON body of the node's `GET /health/network` on loopback `port`.
fn network_health(port: u16) -> eyre::Result<(u16, Value)> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    stream.write_all(
        b"GET /health/network HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| eyre::eyre!("/health/network response has no body: {response:?}"))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| eyre::eyre!("/health/network response has no status: {head:?}"))?
        .parse()?;
    Ok((status, serde_json::from_str(body)?))
}

/// A `0x`-prefixed hex quantity as a `u64`.
fn hex_u64(raw: &str) -> Option<u64> {
    u64::from_str_radix(raw.strip_prefix("0x")?, 16).ok()
}

/// A JSON number, a `0x` hex string or a decimal string as a `u64`.
fn json_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => hex_u64(text).or_else(|| text.parse().ok()),
        _ => None,
    }
}
