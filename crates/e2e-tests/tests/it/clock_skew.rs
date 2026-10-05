//! Validators with skewed clocks.
//!
//! Every node of an e2e run reads the same host clock, so the voter's drift tiers
//! (`check_header_lead` in `crates/consensus/primary/src/network/handler.rs`) and the proposer's
//! wait for its latest parent (`crates/consensus/primary/src/proposer.rs`) only ever see leads of a
//! few milliseconds. These tests shift validator-4's wall clock through the test-utils hook in
//! `tn_types::now_ms`, which reads [`CLOCK_OFFSET_ENV`] once per process. The variable is set on
//! that one child through [`start_validator_with_env`], never on the harness, which every node
//! would inherit it from.
//!
//! Every run commits millisecond timestamps from genesis: the sub-second timestamp fork is pinned
//! active at epoch 0, which is where the drift tiers compare leads in milliseconds (see
//! [`run_clock_skew_scenario`]).

use super::common::{
    assert_epoch_records_verify, assert_nodes_agree_on_commit_times, drive_light_tx_load,
    get_node_mode, loop_epochs, node_log_path, pin_fork_epochs, read_consensus_headers,
    scrape_metric_value, start_validator_with_env, strip_ansi, wait_for_rpc,
    walk_block_commit_times, ProcessGuard, EVM_TIMESTAMP_CLAMPED_SERIES, RPC_REQUEST_TIMEOUT,
};
use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
};
use e2e_tests::{config_local_testnet_with_epoch_duration, NodeEndpoints};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{path::Path, sync::Arc, time::Duration};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo, SyncConfig};
use tn_reth::{test_utils::TransactionFactory, RethChainSpec};
use tn_test_utils::wait_until;
use tn_types::{
    get_available_tcp_port, AuthorityIdentifier, Epoch, Genesis, GenesisAccount, NodeMode, U256,
};
use tokio::time::timeout;

/// Epoch duration of these runs in seconds: the consensus minimum, as in `epochs.rs`.
const EPOCH_DURATION: u64 = 5;

/// Epoch the network runs to before the checks start.
///
/// Epochs 0 through 4 have closed by then, and the certified records checked afterwards cover every
/// one of those closes. The nodes start after genesis, so epoch 0 often holds a single commit; the
/// four epochs after it run full length with the skewed validator proposing and voting.
const TARGET_EPOCH: Epoch = 5;

/// Index of the validator whose clock is shifted (validator-4); the other three keep host time.
const SKEWED: usize = 3;

/// Environment variable the node's test-utils clock hook reads: a signed offset in milliseconds.
const CLOCK_OFFSET_ENV: &str = "TN_TEST_CLOCK_OFFSET_MS";

/// Message of the info line the hook logs once, on the node's first clock read, with the offset
/// in its `offset_ms` field.
const CLOCK_OFFSET_APPLIED: &str = "test clock offset applied";

/// Start of the warning a voter logs when it rejects a header for leading its clock by more than
/// the tolerance plus the vote timeout (tier 3 of `check_header_lead`).
const TIMESTAMP_REJECTION: &str = "Rejected header";

/// Vote requests a node answered with a recoverable "not yet" because the header led its clock by
/// more than the drift tolerance but no more than the tolerance plus the vote timeout (tier 2 of
/// `check_header_lead`).
const DEFERRED_VOTES_SERIES: &str = "tn_primary_votes_deferred_future_header_total";

/// How many blocks the skewed node's head may sit from validator-1's when both are read after the
/// run. The load has stopped by then, so new blocks come only from epoch-closing commits.
const MAX_HEAD_LAG: u64 = 3;

/// Which voters a clock offset makes answer vote requests with tier 2 deferrals.
///
/// Every offset these tests use stays below the drift tolerance plus the 5 s default vote timeout,
/// so no header reaches tier 3 (rejected with a penalty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Deferrals {
    /// The offset is within the drift tolerance: every vote waits out the lead (tier 1).
    Nobody,
    /// The skewed clock runs ahead, so its headers lead the honest voters' clocks.
    HonestVoters,
    /// The skewed clock runs behind, so honest headers lead the skewed voter's clock.
    SkewedVoter,
}

impl Deferrals {
    /// The voters `offset_ms` defers on, against the default drift tolerance the harness runs
    /// with (it writes no network config, so every node loads the default).
    fn for_offset(offset_ms: i64) -> Self {
        let tolerance = SyncConfig::default().max_header_time_drift_tolerance;
        if u128::from(offset_ms.unsigned_abs()) <= tolerance.as_millis() {
            Self::Nobody
        } else if offset_ms > 0 {
            Self::HonestVoters
        } else {
            Self::SkewedVoter
        }
    }
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test a validator whose wall clock runs 200 ms ahead of the other three.
///
/// 200 ms is inside the voters' 250 ms drift tolerance, the kind of skew production validators
/// carry on an ordinary day, so every vote on the skewed node's headers waits out the lead (tier 1)
/// and none is deferred. The run proves such a validator stays a full committee member: no node
/// defers a single vote over the whole run, headers it authored are committed, and the checks
/// listed on [`run_clock_skew_scenario`] hold.
async fn test_epoch_clock_skew_ahead_200ms() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_clock_skew_scenario(200, "skew_p200").await
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test a validator whose wall clock runs 2 s ahead of the other three.
///
/// 2 s is past the 250 ms drift tolerance but within the tolerance plus the 5 s vote timeout, so
/// the honest voters answer the skewed node's vote requests with tier 2 deferrals, and the network
/// has to keep committing while that node's headers wait. The run proves every honest node
/// deferred votes while the skewed node deferred none, no node rejected a header for its timestamp
/// (tier 3), and the checks listed on [`run_clock_skew_scenario`] hold.
///
/// That headers the skewed node authored were committed is not asserted, because tier 2 cannot
/// deliver it at this lead. A deferred vote request stays open only while its header's round is
/// current: the proposer moves to the next round on the honest cadence and cancels the open
/// requests. Tier 2 can therefore rescue a header only when the lead minus the tolerance is shorter
/// than one round interval. Here the lead is 1.75 s over the tolerance and the harness's header
/// delays (250 ms minimum, 500 ms maximum) make a round 250 to 500 ms, so no header from the skewed
/// node is ever certified; a measured run committed 230 headers, none of them from it. With
/// production's 1 s minimum and 2.5 s maximum header delay the slack is about 1 to 2.5 s, so a
/// validator this far ahead is in much the same position. This is not a regression: before the
/// drift tiers, a voter rejected such a header outright with a severe penalty, and tier 2 only
/// removes the penalty. The run prints the count to stderr.
async fn test_epoch_clock_skew_ahead_2s() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_clock_skew_scenario(2_000, "skew_p2s").await
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test a validator whose wall clock runs 2 s behind the other three.
///
/// The mirror of the 2 s ahead case: every honest header leads the skewed voter's clock by about
/// 2 s, so the skewed node is the one that defers (tier 2), and its proposer waits for its clock to
/// pass its latest parent before each header. The run proves the skewed node deferred votes while
/// no honest node deferred any, no node rejected a header for its timestamp (tier 3), and the
/// checks listed on [`run_clock_skew_scenario`] hold. The proposer's wait is logged only at debug
/// (`latest parent not yet in the past`), which the nodes' info-level logs do not carry, so it is
/// not asserted.
///
/// That headers the skewed node authored were committed is not asserted either. Before each header
/// its proposer sleeps until its own clock passes the latest parent's timestamp, about 2 s here,
/// and by then the honest nodes are several rounds ahead, so no later certificate takes the header
/// as a parent. Only an epoch's round-1 header, which has no parents and so no wait, gets
/// committed: a measured run committed 209 headers, 4 of them from it, all round 1. The wait
/// outlasts a round whenever the lag exceeds one round interval, 250 to 500 ms with the harness's
/// header delays and about 1 to 2.5 s with production's. As with the ahead case this is not a
/// regression against the voter before the drift tiers. The run prints the count to stderr.
async fn test_epoch_clock_skew_behind_2s() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    run_clock_skew_scenario(-2_000, "skew_m2s").await
}

/// Run four validators with validator-4's clock shifted by `offset_ms`, then check what the run
/// left behind. `test` names the temp dir and the `test_logs` directory.
///
/// The run pins the sub-second timestamp fork active from genesis, the regime adiri is in after
/// its fork and the one the drift tiers are about: headers carry millisecond creation times, and
/// the voter compares a header's lead with the tolerance in milliseconds. The seed-signature fork
/// is pinned at genesis too, because the sub-second gate conjoins it: the lane default leaves the
/// seed fork dormant, which would silently turn the run into whole-second timestamps. The pins are
/// also what keep the harness's in-process decode in [`read_consensus_headers`] on the same fork
/// points as the nodes. The network runs under the light load until [`TARGET_EPOCH`] opens, and
/// the run fails unless:
///
/// - validator-4's stdout log carries the clock hook's one-time line with this offset and no other
///   node's log carries the line, so the binary under test has the hook and only validator-4 is
///   skewed;
/// - every node's blocks pass the per-block commit-time walk and all four agree on every block they
///   share, and validator-4 served a block whose commit time has a non-zero millisecond part;
/// - every node serves a certified record for each epoch below [`TARGET_EPOCH`] and has executed
///   the block each record ends on, with the hash the record commits to;
/// - validator-4 is `CvvActive` at the end, with its head within [`MAX_HEAD_LAG`] blocks of
///   validator-1's;
/// - no engine clamped an EVM timestamp ([`EVM_TIMESTAMP_CLAMPED_SERIES`] reads 0 on all four),
///   since the commit clamp has to absorb the skew before execution sees it;
/// - the deferred-vote counters ([`DEFERRED_VOTES_SERIES`]), read over the whole run, match the
///   tier the offset falls in ([`Deferrals`]), and for an offset past the tolerance no node's log
///   carries a tier 3 rejection ([`TIMESTAMP_REJECTION`]);
/// - for an offset within the tolerance, validator-1's committed consensus chain, read from disk
///   once it is stopped, holds headers validator-4 authored. Past the tolerance the count is only
///   printed: the tests for those offsets say why.
///
/// `assert_consensus_commit_times` in `epochs.rs` is not used: it requires a fork seam inside the
/// chain it walks, and its check at each epoch seam assumes every leader stamps headers from the
/// same clock.
async fn run_clock_skew_scenario(offset_ms: i64, test: &str) -> eyre::Result<()> {
    // a harness-wide offset would reach every node through the inherited environment and leave no
    // unskewed clock to compare against
    eyre::ensure!(
        std::env::var_os(CLOCK_OFFSET_ENV).is_none(),
        "{CLOCK_OFFSET_ENV} is set in the harness environment, so every node would inherit it"
    );
    let deferrals = Deferrals::for_offset(offset_ms);
    // millisecond timestamps from genesis; the gate conjoins the seed fork fail-closed, so a
    // dormant seed fork would keep every epoch on whole seconds. the consensus chain read at
    // the end is decoded under these pins too
    pin_fork_epochs(None, Some(0), None, Some(0));

    // short on purpose: node IPC socket paths are built under the temp dir
    let temp_dir = tempfile::TempDir::with_prefix(test)?;
    let temp_path = temp_dir.path();

    // one funded sender per validator, so every round of load reaches every worker, the skewed
    // node's included
    let mut senders: Vec<TransactionFactory> = (0..4u64)
        .map(|i| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(0x5ce0 + i)))
        .collect();
    let funding = U256::from(parse_ether("1_000")?);
    let accounts = senders
        .iter()
        .map(|sender| (sender.address(), GenesisAccount::default().with_balance(funding)))
        .collect();
    config_local_testnet_with_epoch_duration(
        temp_path,
        Some("restart_test".to_string()),
        Some(accounts),
        Some(EPOCH_DURATION as u32),
    )?;
    let genesis: Genesis = Config::load_from_path(
        temp_path.join("shared-genesis").join("genesis").join("genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let offset = offset_ms.to_string();
    let skew_env = [(CLOCK_OFFSET_ENV, offset.as_str())];
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut rpc_urls = Vec::new();
    let mut metrics_addrs = Vec::new();
    for instance in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
        let metrics_port =
            get_available_tcp_port("127.0.0.1").expect("metrics port assigned by host");
        let metrics_addr = format!("127.0.0.1:{metrics_port}");
        let extra_env: &[(&str, &str)] = if instance == SKEWED { &skew_env } else { &[] };
        guard.push(start_validator_with_env(
            instance,
            bin,
            temp_path,
            rpc_port,
            test,
            0,
            &["--metrics", &metrics_addr],
            extra_env,
        ));
        rpc_urls.push(format!("http://127.0.0.1:{rpc_port}"));
        metrics_addrs.push(metrics_addr);
    }
    let providers = rpc_urls
        .iter()
        .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?)))
        .collect::<eyre::Result<Vec<_>>>()?;
    futures::future::try_join_all(providers.iter().map(wait_for_rpc)).await?;

    // the hook logs on the node's first clock read, long before its RPC is up; the wait only
    // covers the log writer. checked before the run so a binary without the hook fails at once
    // instead of running four unskewed nodes through every later check
    let skewed_log = node_log_path(test, SKEWED, 0, false);
    let offset_field = format!("offset_ms={offset_ms}");
    wait_until(Duration::from_secs(20), "the skewed node to log its clock offset", || async {
        Ok(clock_offset_lines(&skewed_log)?
            .iter()
            .any(|line| line.split_whitespace().any(|token| token == offset_field)))
    })
    .await
    .map_err(|e| {
        eyre::eyre!(
            "{e}: {} has no `{CLOCK_OFFSET_APPLIED}` line with `{offset_field}`, so the node \
             binary was built without the clock hook (TN_BIN_PATH must name a test-utils build of \
             a tree that has it) or the node did not get {CLOCK_OFFSET_ENV}",
            skewed_log.display()
        )
    })?;
    for instance in (0..4).filter(|&instance| instance != SKEWED) {
        let log = node_log_path(test, instance, 0, false);
        let lines = clock_offset_lines(&log)?;
        eyre::ensure!(
            lines.is_empty(),
            "{} logged a clock offset although only validator-{} was given one: {lines:?}",
            log.display(),
            SKEWED + 1,
        );
    }

    // the load only runs while the epochs roll; dropping it with the finished race stops it
    let reached = tokio::select! {
        reached = loop_epochs(0, TARGET_EPOCH, &rpc_urls[0], EPOCH_DURATION) => reached?,
        never = drive_light_tx_load(&providers, &mut senders, chain) => match never {},
    };
    eyre::ensure!(
        reached >= TARGET_EPOCH,
        "network stopped at epoch {reached}, short of {TARGET_EPOCH}"
    );

    let mut served = Vec::with_capacity(providers.len());
    for (provider, url) in providers.iter().zip(&rpc_urls) {
        let head = block_number(provider, url).await?;
        served.push(walk_block_commit_times(provider, url, 0..=head).await?);
    }
    assert_nodes_agree_on_commit_times(&served, &rpc_urls)?;
    let skewed_url = &rpc_urls[SKEWED];
    let millis_blocks = served[SKEWED]
        .iter()
        .filter(|block| block.sub_second && block.timestamp_millis % 1000 != 0)
        .count();
    eyre::ensure!(
        millis_blocks > 0,
        "{skewed_url} (clock {offset_ms} ms off) served no block whose commit time has a non-zero \
         millisecond part, so the run never committed at millisecond resolution"
    );

    // every node, the skewed one included, closed every epoch and ended each on the same block.
    // the same 60 s floor per record as `epochs.rs` uses
    let endpoints: Vec<NodeEndpoints> = rpc_urls
        .iter()
        .map(|url| NodeEndpoints {
            http_url: url.clone(),
            ws_url: String::new(),
            ipc_path: String::new(),
        })
        .collect();
    assert_epoch_records_verify(&endpoints, 0..=TARGET_EPOCH - 1, (EPOCH_DURATION * 6).max(60))
        .await?;

    let mode = get_node_mode(skewed_url)?;
    eyre::ensure!(
        mode == NodeMode::CvvActive,
        "{skewed_url} (clock {offset_ms} ms off) ended the run in {mode:?}, not CvvActive"
    );
    let reference_head = block_number(&providers[0], &rpc_urls[0]).await?;
    let skewed_head = block_number(&providers[SKEWED], skewed_url).await?;
    eyre::ensure!(
        reference_head.abs_diff(skewed_head) <= MAX_HEAD_LAG,
        "{skewed_url} (clock {offset_ms} ms off) is at block {skewed_head}, validator-1 at \
         {reference_head}: more than {MAX_HEAD_LAG} blocks apart"
    );

    // every node is still the process it started as, so each counter covers the whole run
    let deferred = scrape_all(&metrics_addrs, DEFERRED_VOTES_SERIES)?;
    let clamped = scrape_all(&metrics_addrs, EVM_TIMESTAMP_CLAMPED_SERIES)?;
    // stderr rather than tracing: `init_test_tracing` filters on RUST_LOG, and setting RUST_LOG for
    // the harness would also change the nodes' logging, since they inherit it. a passing run shows
    // this line under nextest's `--success-output`
    eprintln!(
        "{test}: offset_ms={offset_ms} reached={reached} deferred={deferred:?} \
         clamped={clamped:?} (node order validator-1..validator-4, validator-{} skewed)",
        SKEWED + 1
    );
    for (count, url) in clamped.iter().zip(&rpc_urls) {
        eyre::ensure!(
            *count == 0.0,
            "{url} clamped {count} EVM timestamps up to their parent's with validator-{} {offset_ms} \
             ms off: the commit clamp let commit time go backwards (node logs under \
             test_logs/{test}/ carry the \"evm timestamp clamped to parent\" warnings)",
            SKEWED + 1
        );
    }
    let honest = || (0..4).filter(|&instance| instance != SKEWED);
    match deferrals {
        Deferrals::Nobody => eyre::ensure!(
            deferred.iter().all(|count| *count == 0.0),
            "a {offset_ms} ms offset is within the drift tolerance, yet votes were deferred \
             (validator-1..validator-4): {deferred:?}"
        ),
        Deferrals::HonestVoters => {
            eyre::ensure!(
                deferred[SKEWED] == 0.0,
                "{skewed_url} runs {offset_ms} ms ahead, so no header can lead its clock, yet it \
                 deferred {} votes",
                deferred[SKEWED]
            );
            eyre::ensure!(
                honest().all(|instance| deferred[instance] > 0.0),
                "validator-{} runs {offset_ms} ms ahead, yet an honest node deferred no vote \
                 (validator-1..validator-4): {deferred:?}",
                SKEWED + 1
            );
            assert_no_timestamp_rejections(test)?;
        }
        Deferrals::SkewedVoter => {
            eyre::ensure!(
                honest().all(|instance| deferred[instance] == 0.0),
                "validator-{} runs {offset_ms} ms behind, so its headers cannot lead an honest \
                 clock, yet honest nodes deferred votes (validator-1..validator-4): {deferred:?}",
                SKEWED + 1
            );
            eyre::ensure!(
                deferred[SKEWED] > 0.0,
                "{skewed_url} runs {offset_ms} ms behind, yet it deferred no vote on an honest \
                 header (validator-1..validator-4): {deferred:?}"
            );
            assert_no_timestamp_rejections(test)?;
        }
    }

    // whether the skewed node took part in the committed DAG, not only followed it. the RPC serves
    // only the latest consensus header, so the chain is read from validator-1's disk, which needs
    // the node stopped
    let skewed_info: NodeInfo = Config::load_from_path(
        temp_path.join(format!("validator-{}", SKEWED + 1)).join("node-info.yaml"),
        ConfigFmt::YAML,
    )?;
    let skewed_id = AuthorityIdentifier::from(skewed_info.bls_public_key);
    let mut validator_1 = guard.take(0).ok_or_else(|| eyre::eyre!("validator-1 is not running"))?;
    super::common::kill_child(&mut validator_1);
    eyre::ensure!(
        providers[0].get_chain_id().await.is_err(),
        "validator-1 still answers RPC after being stopped"
    );
    let chain_headers = read_consensus_headers(&temp_path.join("validator-1")).await?;
    let mut committed = 0usize;
    let mut authored = 0usize;
    let mut authored_round_1 = 0usize;
    let mut led = 0usize;
    for consensus_header in &chain_headers {
        let sub_dag = &consensus_header.sub_dag;
        for header in sub_dag.headers() {
            committed += 1;
            if header.author() == &skewed_id {
                authored += 1;
                if header.round() == 1 {
                    authored_round_1 += 1;
                }
            }
        }
        if sub_dag.leader().author() == &skewed_id {
            led += 1;
        }
    }
    eprintln!(
        "{test}: committed headers {committed}, authored by validator-{} {authored} \
         ({authored_round_1} of them round 1), sub-dags it led {led} (of {} consensus headers)",
        SKEWED + 1,
        chain_headers.len()
    );
    // past the tolerance a deferred header is not certified before its round ends (see the
    // tests for those offsets), so authorship is a guarantee only inside the tolerance
    if deferrals == Deferrals::Nobody {
        eyre::ensure!(
            authored > 0,
            "validator-{} (clock {offset_ms} ms off) authored none of the {committed} committed \
             headers: it followed consensus without taking part in it",
            SKEWED + 1
        );
    }

    guard.kill_all();
    Ok(())
}

/// Read the series `name` from every node's metrics endpoint, in node order.
///
/// The scrape is blocking socket I/O with sleeps between retries, so it runs off the runtime
/// worker.
fn scrape_all(metrics_addrs: &[String], name: &str) -> eyre::Result<Vec<f64>> {
    tokio::task::block_in_place(|| {
        metrics_addrs.iter().map(|addr| scrape_metric_value(addr, name)).collect()
    })
}

/// A node's execution head, read under [`RPC_REQUEST_TIMEOUT`].
async fn block_number<P: Provider>(provider: &P, node: &str) -> eyre::Result<u64> {
    timeout(RPC_REQUEST_TIMEOUT, provider.get_block_number())
        .await
        .map_err(|_| {
            eyre::eyre!("{node} did not answer eth_blockNumber within {RPC_REQUEST_TIMEOUT:?}")
        })?
        .map_err(Into::into)
}

/// The lines of the node log at `log` that contain `needle`, with the ANSI styling the node's log
/// layer adds removed, so a field reads as a plain `key=value` token.
fn log_lines_with(log: &Path, needle: &str) -> eyre::Result<Vec<String>> {
    let raw = std::fs::read(log).map_err(|e| eyre::eyre!("cannot read {}: {e}", log.display()))?;
    Ok(String::from_utf8_lossy(&raw)
        .lines()
        .map(strip_ansi)
        .filter(|line| line.contains(needle))
        .collect())
}

/// The lines of the node log at `log` that carry the clock hook's [`CLOCK_OFFSET_APPLIED`] line.
fn clock_offset_lines(log: &Path) -> eyre::Result<Vec<String>> {
    log_lines_with(log, CLOCK_OFFSET_APPLIED)
}

/// Fail if any of the four nodes' stdout logs from run `test` carries a tier 3 rejection
/// ([`TIMESTAMP_REJECTION`]): every offset these tests use leads by less than the tolerance plus
/// the vote timeout, so a rejection means the voter charged a penalty for skew it should defer.
fn assert_no_timestamp_rejections(test: &str) -> eyre::Result<()> {
    for instance in 0..4 {
        let log = node_log_path(test, instance, 0, false);
        let rejected = log_lines_with(&log, TIMESTAMP_REJECTION)?;
        eyre::ensure!(
            rejected.is_empty(),
            "{} rejected {} headers for their timestamp (tier 3) although every lead in this run \
             stays within the tolerance plus the vote timeout; first: {:?}",
            log.display(),
            rejected.len(),
            rejected.first(),
        );
    }
    Ok(())
}
