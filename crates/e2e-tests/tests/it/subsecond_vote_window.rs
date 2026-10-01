//! The vote-window check on a node catching up through pre-fork epochs to the sub-second fork.
//!
//! A validator's `vote_timeout` has to cover `max_header_delay` plus the longest time its voter
//! may wait out a future-dated header, and that wait depends on the fork regime of the epoch:
//! the network config's `max_header_time_drift_tolerance` once sub-second timestamps are active,
//! the same tolerance rounded up to whole seconds before. One `vote_timeout` can therefore pass
//! the window for a post-fork epoch and fail it for a pre-fork one. The node checks the window
//! only for an epoch it can still vote in (`EpochManager::configure_consensus`); an epoch whose
//! certified record it already holds is replayed without the check. The tests here set such a
//! `vote_timeout` and check both halves: a late validator replays closed pre-fork epochs and
//! votes in the open post-fork one, and a validator booting at genesis, where the open epoch is
//! pre-fork, refuses to start.

use super::common::{
    assert_epoch_records_verify, block_commit_time, current_epoch, fetch_verified_epoch_record,
    get_block_number, get_node_mode, pin_fork_epochs, scrape_metric_value, start_validator,
    start_validator_with_args, wait_for_epoch_at_least, wait_for_head_at_least, wait_for_rpc,
    ProcessGuard,
};
use alloy::providers::ProviderBuilder;
use e2e_tests::{config_local_testnet_with_epoch_duration, NodeEndpoints};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NetworkConfig, Parameters};
use tn_test_utils::wait_until;
use tn_types::{get_available_tcp_port, Epoch, NodeMode};
use tokio::time::Instant;
use tracing::info;

/// Epoch length for both tests, the consensus minimum.
const EPOCH_DURATION: u32 = 5;

/// Leader epoch at which both tests arm the sub-second timestamp fork, with the seed-signature
/// fork active from genesis: epochs 0 and 1 commit whole seconds, every later epoch milliseconds.
///
/// Two pre-fork epochs rather than one because epoch 0 routinely holds a single commit (the nodes
/// start after the genesis timestamp, so their first commit closes it), and a late validator then
/// replays two pre-fork epochs, one of them full length.
const VOTE_WINDOW_FORK_EPOCH: Epoch = 2;

/// The `vote_timeout` both tests write into one validator's `parameters.yaml`.
///
/// With the harness's 500 ms `max_header_delay` and the default 250 ms drift tolerance the vote
/// window is 750 ms for a post-fork epoch and 1.5 s for a pre-fork one, so 1 s passes the first
/// and fails the second. [`set_vote_timeout`] recomputes both windows from the node's files and
/// fails the test if this value no longer sits between them.
const SHORT_VOTE_TIMEOUT: Duration = Duration::from_secs(1);

/// The error `validate_epoch_timing` (`crates/config/src/consensus.rs`) returns when
/// [`SHORT_VOTE_TIMEOUT`] meets the pre-fork window, which the node prints to stderr as it exits.
const PRE_FORK_REFUSAL: &str = "vote_timeout 1s is shorter than max_header_delay 500ms + the \
                                voter's longest drift wait 1s (max_header_time_drift_tolerance \
                                250ms, rounded up to whole seconds while sub-second timestamps \
                                are inactive); raise vote_timeout to at least 1.5s";

/// The line `EpochManager::configure_consensus` (`crates/node/src/manager/node/start_epoch.rs`)
/// logs, with the epoch as a field, when it enters an epoch whose certified record it holds and
/// skips the vote-window check.
const CLOSED_EPOCH_REPLAY: &str =
    "epoch already closed by its committee; replaying it without the vote-window check";

/// The line `Parameters::tracing` (`crates/config/src/node.rs`) logs for each epoch the node
/// configures, here for [`SHORT_VOTE_TIMEOUT`]: it proves the node runs with the rewritten value.
const SHORT_VOTE_TIMEOUT_LOADED: &str = "Vote timeout set to 1000 ms";

/// Instance index of the validator the catch-up test starts late (validator-4).
const LATE_INSTANCE: usize = 3;

/// How long the late validator must stay up after it starts. Before the fix it exited at its
/// first pre-fork epoch, a few seconds after the startup record sync.
const LATE_ALIVE: Duration = Duration::from_secs(20);

/// Bound on the late validator reaching `CvvActive`, counted from its start: it replays every
/// closed epoch (four or so, at 5 s each on the live network) before it can vote.
const LATE_CATCH_UP: Duration = Duration::from_secs(90);

/// Bound on the late validator's head reaching validator-1's once it votes.
const HEAD_CATCH_UP_SECS: u64 = 30;

/// Bound on a certified epoch record showing up on a node. Certificates take a fixed
/// quorum-voting time that does not shrink with 5 s epochs, so this keeps the 60 s floor the
/// epoch tests use.
const RECORD_TIMEOUT_SECS: u64 = 60;

/// Bound on the genesis validator exiting in the refusal test. It enters epoch 0 once its startup
/// record sync ends, which with a connected peer and no certificate to fetch is one 5 s pass.
const REFUSAL_BOUND: Duration = Duration::from_secs(30);

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// A validator configured for the post-fork vote window catches up through closed pre-fork epochs
/// and votes once it reaches the open post-fork epoch.
///
/// The vote window is `max_header_delay` plus the voter's longest drift wait. With the harness's
/// 500 ms header delay and the default 250 ms drift tolerance that is 750 ms post-fork and 1.5 s
/// pre-fork, where the voter compares whole seconds and the tolerance rounds up to 1 s. A 1 s
/// `vote_timeout` sits between the two: valid for every epoch the late validator can vote in here,
/// invalid for the pre-fork epochs it only replays. Before the fix the node checked the window for
/// every epoch it entered, so this validator stopped at epoch 0 with "vote_timeout 1s is shorter
/// than max_header_delay 500ms + the voter's longest drift wait 1s (max_header_time_drift_tolerance
/// 250ms, rounded up to whole seconds while sub-second timestamps are inactive); raise
/// vote_timeout to at least 1.5s".
///
/// Three of the four genesis validators, a quorum, run until epoch 3 is open and epoch 2's record
/// is certified, so both pre-fork epochs and the fork epoch are closed. Validator-4 then starts
/// with `vote_timeout: 1s`. The test proves it:
///
/// - is still running 20 s after it starts and has written nothing about `vote_timeout` to stderr;
/// - logged the closed-epoch replay line for epochs 0 and 1, and not for the post-fork epoch it
///   votes in, which it therefore entered with the check, at the 750 ms window;
/// - reaches `CvvActive`, its head reaches validator-1's, and its headers collect a vote quorum
///   (`tn_primary_certificates_formed_total` above zero);
/// - serves the same commit time as validator-1 for the closing blocks of epochs 1 and 2, whole
///   seconds for epoch 1 and milliseconds for epoch 2, so it executed across the fork seam;
/// - like the other three, serves a certified record for every epoch up to the one open when it
///   started, and executed each record's final block with the hash the record commits to.
async fn test_epoch_late_validator_vote_window_across_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // the claim is about a node crossing a known fork epoch, and the sub-second gate conjoins the
    // seed fork fail-closed, so both are forced; the other two follow the lane
    pin_fork_epochs(None, Some(0), None, Some(VOTE_WINDOW_FORK_EPOCH));

    // short on purpose: node IPC socket paths are built under the temp dir
    let test = "ss_vote_late";
    let temp_dir = tempfile::TempDir::with_prefix(test)?;
    let temp_path = temp_dir.path();
    config_local_testnet_with_epoch_duration(
        temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION),
    )?;

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut rpc_urls = Vec::new();
    for instance in 0..LATE_INSTANCE {
        let rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
        guard.push(start_validator(instance, bin, temp_path, rpc_port, test, 0));
        rpc_urls.push(format!("http://127.0.0.1:{rpc_port}"));
    }
    let providers = rpc_urls
        .iter()
        .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?)))
        .collect::<eyre::Result<Vec<_>>>()?;
    futures::future::try_join_all(providers.iter().map(wait_for_rpc)).await?;

    // three of four validators are a quorum, so the network closes epochs without validator-4.
    // the first post-fork epoch's record being certified is what lets a joining node treat every
    // epoch up to it as closed
    wait_for_epoch_at_least(&providers[0], VOTE_WINDOW_FORK_EPOCH + 1).await?;
    let last_pre_fork =
        fetch_verified_epoch_record(&rpc_urls[0], VOTE_WINDOW_FORK_EPOCH - 1, RECORD_TIMEOUT_SECS)
            .await?;
    let fork =
        fetch_verified_epoch_record(&rpc_urls[0], VOTE_WINDOW_FORK_EPOCH, RECORD_TIMEOUT_SECS)
            .await?;
    let open_at_join = current_epoch(&providers[0]).await?.epoch_id;
    eyre::ensure!(
        open_at_join > VOTE_WINDOW_FORK_EPOCH,
        "epoch {VOTE_WINDOW_FORK_EPOCH} is certified but validator-1 reports epoch {open_at_join} \
         open"
    );
    info!(target: "epoch-test", open_at_join, "pre-fork and fork epochs closed, starting validator-4");

    let late_dir = temp_path.join(format!("validator-{}", LATE_INSTANCE + 1));
    set_vote_timeout(&late_dir, SHORT_VOTE_TIMEOUT)?;
    let late_rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
    let metrics_port = get_available_tcp_port("127.0.0.1").expect("metrics port assigned by host");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let late_url = format!("http://127.0.0.1:{late_rpc_port}");
    let started = Instant::now();
    let late = guard.push(start_validator_with_args(
        LATE_INSTANCE,
        bin,
        temp_path,
        late_rpc_port,
        test,
        0,
        &["--metrics", &metrics_addr],
    ));
    let late_stdout = node_log(test, LATE_INSTANCE, "log")?;
    let late_stderr = node_log(test, LATE_INSTANCE, "stderr.log")?;

    // the check that stopped the node before the fix runs as it enters its first epoch, a few
    // seconds in, so surviving 20 s means it entered the pre-fork epochs without failing it
    let alive_until = started + LATE_ALIVE;
    loop {
        let now = Instant::now();
        ensure_running(&mut guard, late, "validator-4", started, &late_stderr)?;
        if now >= alive_until {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250).min(alive_until - now)).await;
    }
    let stderr = read_log(&late_stderr)?;
    eyre::ensure!(
        !stderr.contains("vote_timeout"),
        "validator-4 wrote about vote_timeout to stderr: {}",
        stderr.trim()
    );

    let deadline = started + LATE_CATCH_UP;
    loop {
        ensure_running(&mut guard, late, "validator-4", started, &late_stderr)?;
        let mode = get_node_mode(&late_url).ok();
        if mode == Some(NodeMode::CvvActive) {
            break;
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "validator-4 did not reach CvvActive within {LATE_CATCH_UP:?} of starting (last mode \
             {mode:?}); see test_logs/{test}/"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let reference_head = get_block_number(&rpc_urls[0])?;
    wait_for_head_at_least(&late_url, reference_head, HEAD_CATCH_UP_SECS).await?;
    let late_provider = ProviderBuilder::new().connect_http(late_url.parse()?);
    let voting_in = current_epoch(&late_provider).await?.epoch_id;
    info!(
        target: "epoch-test",
        elapsed = ?started.elapsed(),
        voting_in,
        reference_head,
        "validator-4 caught up and votes"
    );

    let stdout = read_log(&late_stdout)?;
    eyre::ensure!(
        stdout.contains(SHORT_VOTE_TIMEOUT_LOADED),
        "validator-4 never logged \"{SHORT_VOTE_TIMEOUT_LOADED}\", so it did not run with the \
         rewritten vote_timeout and the test proves nothing"
    );
    let replayed = closed_epoch_replays(&stdout)?;
    info!(target: "epoch-test", ?replayed, "epochs validator-4 entered without the vote-window check");
    eyre::ensure!(
        (0..VOTE_WINDOW_FORK_EPOCH).all(|epoch| replayed.contains(&epoch)),
        "validator-4 logged the closed-epoch replay line for epochs {replayed:?}, which misses a \
         pre-fork epoch below {VOTE_WINDOW_FORK_EPOCH}"
    );
    eyre::ensure!(
        voting_in >= VOTE_WINDOW_FORK_EPOCH && !replayed.contains(&voting_in),
        "validator-4 votes in epoch {voting_in}, which must be post-fork (from \
         {VOTE_WINDOW_FORK_EPOCH}) and entered with the vote-window check (replayed {replayed:?})"
    );

    // its own headers reached a vote quorum, which only happens in an epoch the other three are
    // running, all of them post-fork by now. the scrape blocks with sleeps between retries
    wait_until(
        Duration::from_secs(HEAD_CATCH_UP_SECS),
        "validator-4 forms a certificate from its own header",
        || async {
            Ok(tokio::task::block_in_place(|| {
                scrape_metric_value(&metrics_addr, "tn_primary_certificates_formed_total")
            })
            .is_ok_and(|formed| formed > 0.0))
        },
    )
    .await?;

    // the seam on the late node's own gate: epoch 1 closes on whole seconds and epoch 2 on
    // milliseconds, each with validator-1's commit time
    for (record, sub_second) in [(&last_pre_fork, false), (&fork, true)] {
        let block = record.final_state.number;
        let reference = block_commit_time(&providers[0], &rpc_urls[0], block).await?;
        let late_commit = block_commit_time(&late_provider, &late_url, block).await?;
        eyre::ensure!(
            late_commit == reference,
            "epoch {} closing block {block}: validator-4 serves {late_commit:?}, validator-1 \
             {reference:?}",
            record.epoch
        );
        eyre::ensure!(
            late_commit.sub_second == sub_second,
            "epoch {} closing block {block} reports subSecond {}, expected {sub_second} with the \
             fork at epoch {VOTE_WINDOW_FORK_EPOCH}",
            record.epoch,
            late_commit.sub_second
        );
    }

    let endpoints: Vec<NodeEndpoints> = rpc_urls
        .iter()
        .chain([&late_url])
        .map(|url| NodeEndpoints {
            http_url: url.clone(),
            ws_url: String::new(),
            ipc_path: String::new(),
        })
        .collect();
    assert_epoch_records_verify(&endpoints, 0..=open_at_join, RECORD_TIMEOUT_SECS).await?;
    ensure_running(&mut guard, late, "validator-4", started, &late_stderr)?;

    guard.kill_all();
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// A validator booting at genesis with a `vote_timeout` that misses the pre-fork vote window
/// refuses to start: relaxing the check for closed epochs did not relax it for the open one.
///
/// Epoch 0 is open and pre-fork (the fork is pinned at [`VOTE_WINDOW_FORK_EPOCH`]), so the
/// window is 1.5 s and the 1 s `vote_timeout` misses it. Validator-1 starts with it and
/// validator-2 with the default 5 s, two of four, short of a quorum, so epoch 0 can never close
/// and validator-1 has no certified record to skip the check with. Validator-2 is there so the
/// startup record sync ends at its first 5 s pass instead of waiting 30 s for a peer.
///
/// Validator-1 must exit non-zero within 30 s with the whole pre-fork refusal on stderr, while
/// validator-2, same binary and genesis with the default `vote_timeout`, keeps running.
async fn test_epoch_vote_window_refuses_prefork_genesis_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    pin_fork_epochs(None, Some(0), None, Some(VOTE_WINDOW_FORK_EPOCH));

    let test = "ss_vote_gen";
    let temp_dir = tempfile::TempDir::with_prefix(test)?;
    let temp_path = temp_dir.path();
    config_local_testnet_with_epoch_duration(
        temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION),
    )?;
    set_vote_timeout(&temp_path.join("validator-1"), SHORT_VOTE_TIMEOUT)?;

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let started = Instant::now();
    for instance in 0..2 {
        let rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
        guard.push(start_validator(instance, bin, temp_path, rpc_port, test, 0));
    }
    let refused_stderr = node_log(test, 0, "stderr.log")?;

    let status = loop {
        let child = guard.get_mut(0).ok_or_else(|| eyre::eyre!("validator-1 is not tracked"))?;
        if let Some(status) = child.try_wait()? {
            break status;
        }
        eyre::ensure!(
            started.elapsed() < REFUSAL_BOUND,
            "validator-1 is still running {REFUSAL_BOUND:?} after it started with vote_timeout \
             {SHORT_VOTE_TIMEOUT:?} in pre-fork epoch 0; see test_logs/{test}/"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    info!(target: "epoch-test", ?status, elapsed = ?started.elapsed(), "validator-1 exited");
    eyre::ensure!(!status.success(), "validator-1 exited with {status}, expected a failure");
    let stderr = read_log(&refused_stderr)?;
    eyre::ensure!(
        stderr.contains(PRE_FORK_REFUSAL),
        "validator-1's stderr does not carry the pre-fork vote-window refusal: {}",
        stderr.trim()
    );
    ensure_running(&mut guard, 1, "validator-2", started, &node_log(test, 1, "stderr.log")?)?;

    guard.kill_all();
    Ok(())
}

/// Set `vote_timeout` in the `parameters.yaml` of the node whose data directory is `datadir`,
/// after checking that it passes the node's post-fork vote window and misses its pre-fork one.
///
/// The windows are recomputed the way `validate_epoch_timing` does, from the file's
/// `max_header_delay` and the default network config's drift tolerance (the harness writes no
/// network config, so the node loads the default too). A change to either default fails here
/// with the numbers rather than turning a test into one that cannot fail.
fn set_vote_timeout(datadir: &Path, vote_timeout: Duration) -> eyre::Result<()> {
    let path = datadir.join("parameters.yaml");
    let mut parameters: Parameters = Config::load_from_path(&path, ConfigFmt::YAML)?;
    let tolerance = NetworkConfig::default().sync_config().max_header_time_drift_tolerance;
    let post_fork_window = parameters.max_header_delay + tolerance;
    let whole_seconds = tolerance.as_secs() + u64::from(tolerance.subsec_nanos() > 0);
    let pre_fork_window = parameters.max_header_delay + Duration::from_secs(whole_seconds);
    eyre::ensure!(
        post_fork_window <= vote_timeout && vote_timeout < pre_fork_window,
        "vote_timeout {vote_timeout:?} must pass the post-fork window {post_fork_window:?} and \
         miss the pre-fork window {pre_fork_window:?} (max_header_delay {:?}, drift tolerance \
         {tolerance:?})",
        parameters.max_header_delay
    );
    parameters.vote_timeout = vote_timeout;
    Config::write_to_path(&path, &parameters, ConfigFmt::YAML)?;
    Ok(())
}

/// Fail if the node at `idx` in `guard` has exited, naming its exit status, how long after
/// `started` that was noticed, and its stderr log.
fn ensure_running(
    guard: &mut ProcessGuard,
    idx: usize,
    name: &str,
    started: Instant,
    stderr_log: &Path,
) -> eyre::Result<()> {
    let child = guard.get_mut(idx).ok_or_else(|| eyre::eyre!("{name} is not tracked"))?;
    if let Some(status) = child.try_wait()? {
        eyre::bail!(
            "{name} exited with {status}, noticed {:?} after it started; stderr: {}",
            started.elapsed(),
            read_log(stderr_log)?.trim()
        );
    }
    Ok(())
}

/// The path of a node's log file under `test_logs/<test>/`: `extension` is `log` for stdout and
/// `stderr.log` for stderr. Every test here starts each node once, so the run is 0.
fn node_log(test: &str, instance: usize, extension: &str) -> eyre::Result<PathBuf> {
    Ok(PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .join("test_logs")
        .join(test)
        .join(format!("node{instance}-run0.{extension}")))
}

/// Read a node log with its ANSI colour codes removed, so lines match the plain text and fields
/// read as `name=value`.
fn read_log(path: &Path) -> eyre::Result<String> {
    let raw = String::from_utf8_lossy(&std::fs::read(path)?).into_owned();
    let mut plain = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            plain.push(c);
            continue;
        }
        // a control sequence: ESC '[' then parameters, ended by a byte in '@'..='~'
        if chars.next() == Some('[') {
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        }
    }
    Ok(plain)
}

/// The epochs a node's stdout says it entered without the vote-window check: the `epoch` field of
/// every [`CLOSED_EPOCH_REPLAY`] line. A matching line without a readable field is an error.
fn closed_epoch_replays(stdout: &str) -> eyre::Result<BTreeSet<Epoch>> {
    stdout
        .lines()
        .filter_map(|line| line.split_once(CLOSED_EPOCH_REPLAY))
        .map(|(_, fields)| {
            fields
                .split_whitespace()
                .find_map(|field| field.strip_prefix("epoch="))
                .and_then(|epoch| epoch.parse().ok())
                .ok_or_else(|| eyre::eyre!("replay line without an epoch field: {fields}"))
        })
        .collect()
}
