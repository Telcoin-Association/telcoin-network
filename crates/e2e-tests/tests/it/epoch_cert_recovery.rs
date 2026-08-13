//! E2e test: unattended recovery of a whole committee halted at a fork-active epoch entry
//! holding epoch records but zero epoch certificates.
//!
//! Reproduces the adiri incident at the first seed-signature fork epoch: every validator
//! closed the epoch and wrote its [`EpochRecord`], but no certificate was ever assembled, so
//! every restart parked waiting on a certificate that no listener, subscription, or vote
//! round could ever produce. The patched build must self-heal: re-park with the vote topic
//! subscribed pre-anchor, re-arm the vote round for the stored-but-uncertified record,
//! re-assemble the exact certificate from deterministic re-votes, unpark, produce blocks,
//! and certify the next (fully fork-active) epoch close live.
//!
//! The halted-fleet state is manufactured with the `TN_TEST_SUPPRESS_EPOCH_CERTS` test hook
//! (test-utils builds only): `manage_epoch_votes` reaches quorum and then DISCARDS the
//! assembled certificate, leaving records on disk and certificates nowhere — byte-for-byte
//! the state a whole-committee crash between an epoch close and certification leaves behind.
//!
//! The hook is a process-wide latch (read once from the environment), so incident fidelity
//! takes two seed runs: certificates for every epoch BEFORE the parked boundary existed on
//! the incident fleet (adiri certified epochs 0..382 live; only record 382 was uncertified),
//! and a fleet with NO certificate for epoch 0 can never be healed or followed at all —
//! `rearm_epoch_vote_round` deliberately skips epoch 0 (the slot can hold the uncertifiable
//! dummy record) and peers only serve `(record, certificate)` pairs, so a fresh observer
//! could never validate the record chain from genesis. Run 1 therefore lets epoch 0 certify
//! normally; run 2 restarts with the hook armed before epoch 1 closes, discarding exactly the
//! LAST record's certificate.
//!
//! [`EpochRecord`]: tn_types::EpochRecord

use super::common::{
    acquire_test_permit, address_from_word, call_rpc, force_kill_and_reap, get_balance,
    get_block_number, network_advancing, start_observer_with_envs, start_validator_with_envs,
    ProcessGuard, EPOCH_DURATION,
};
use e2e_tests::config_local_testnet_with_epoch_duration;
use jsonrpsee::rpc_params;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tn_types::{get_available_tcp_port, EpochCertificate, EpochRecord};
use tracing::{error, info};

/// Test name: names the temp dir and the `test_logs/<test>` directory.
const TEST_NAME: &str = "epoch_cert_recovery";

/// Seed-signature fork pinned on every child of this test (seed, recovery, and observer runs).
///
/// Epoch 2, NOT epoch 1: the fleet must park at a boundary whose PREVIOUS epoch is non-zero.
/// `rearm_epoch_vote_round` deliberately skips `previous_epoch == 0` (the epoch-0 slot can
/// hold the uncertifiable dummy record; re-arming it would let a fleet certify the filler),
/// so a fleet parked at the 0 -> 1 boundary with no certificate anywhere has no self-heal
/// path by design. Fork epoch 2 parks the fleet entering epoch 2 on uncertified record 1 —
/// the same shape as the adiri incident (fork epoch 383 parked on uncertified record 382).
const FORK_EPOCH_ENV: (&str, &str) = ("TN_SEED_SIGNATURE_FORK_EPOCH", "2");

/// Seed-run-2-only hook: reach vote quorum, then discard the assembled certificate.
const SUPPRESS_CERTS_ENV: (&str, &str) = ("TN_TEST_SUPPRESS_EPOCH_CERTS", "1");

/// How late validator 3 restarts in the recovery phase (the ONE deliberate plain sleep).
///
/// It must miss the initial re-vote exchange between validators 0-2 so it exercises the
/// late-joiner path: fetching the already-assembled certificate from recovered peers instead
/// of contributing a vote to the quorum.
const LATE_START_SECS: u64 = 60;

// ---------------------------------------------------------------------------------------------
// Log lines asserted against `test_logs/epoch_cert_recovery/node<i>-run<r>.log`.
//
// Every needle is a substring of a single log line emitted by the node (targets
// "epoch-manager"); structured fields and hashes are excluded so format drift in those parts
// cannot break the match. Sources: crates/node/src/manager/epoch_votes.rs and
// crates/node/src/manager/node/run_epoch.rs.
// ---------------------------------------------------------------------------------------------

/// Seed run 1: quorum reached for record 0 (its certificate is stored — no hook in run 1).
const QUORUM_EPOCH_0: &str = "reached quorum on epoch close for 0/";
/// Seed run 1 fallbacks: a node obtained the record-0 certificate from a peer instead of (or
/// after) its own quorum. Either way the certificate exists locally.
const RETRIEVED_CERT_0: &str = "retrieved cert for epoch 0";
const OBTAINED_CERT_0: &str = "certificate for epoch 0 obtained";
/// Seed run 2: quorum reached for record 1 and the certificate was discarded by the hook.
const SUPPRESSED_CERT_EPOCH_1: &str =
    "TEST HOOK: TN_TEST_SUPPRESS_EPOCH_CERTS active - discarding quorum certificate for epoch 1";
/// A node parked entering a fork-active epoch on an uncertified previous record.
const PARK_WARN: &str = "previous epoch record not yet certified, waiting for its certificate";
/// Pre-park fix: the parked node subscribed to the epoch-vote gossip topic.
const PRE_ANCHOR_SUBSCRIBE: &str = "pre-anchor subscribe to epoch vote topic";
/// Pre-park fix: the parked node re-armed the vote round for stored-but-uncertified record 1.
const REARM_EPOCH_1: &str = "re-arming epoch vote round for uncertified epoch 1";
/// A node aggregated a super-quorum of (re-)votes for record 1.
const QUORUM_EPOCH_1: &str = "reached quorum on epoch close for 1/";
/// The first fully fork-active epoch close (epoch 2) certified live.
const QUORUM_EPOCH_2: &str = "reached quorum on epoch close for 2/";
/// Late-joiner path: certificate for record 1 downloaded from a recovered peer. Matches both
/// producers: the vote-round recovery fetch ("retrieved cert for epoch 1/<hash> from a peer")
/// and the state-sync record collector ("retrieved cert for epoch 1: <hash> from a peer").
const RETRIEVED_CERT_1: &str = "retrieved cert for epoch 1";
/// Late-joiner path: a vote round observed the certificate arriving from another source.
const OBTAINED_CERT_1: &str = "certificate for epoch 1 obtained";

/// Directory holding this test's per-node log files (see `setup_log_dir`).
fn node_log_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("test_logs").join(TEST_NAME)
}

/// Per-node log file name for `run` (matches the `setup_log_dir` convention).
fn run_log(instance: usize, run: u32) -> String {
    format!("node{instance}-run{run}.log")
}

/// Read a node's log, tolerating a file that the child has not created yet.
fn read_node_log(file: &str) -> String {
    std::fs::read(node_log_dir().join(file))
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default()
}

/// Poll (250ms cadence) until `file` contains ANY of `needles`, else fail at `timeout` with a
/// message naming the phase, the file, and the missing needles.
fn wait_for_log(file: &str, needles: &[&str], timeout: Duration, phase: &str) -> eyre::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let log = read_node_log(file);
        if needles.iter().any(|needle| log.contains(needle)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "{phase}: {file} did not contain any of {needles:?} within {}s \
                 (see test_logs/{TEST_NAME}/)",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Poll (250ms cadence) until at least `min_count` of `files` contain `needle`, else fail at
/// `timeout` naming the files still missing it.
fn wait_for_log_quorum(
    files: &[String],
    needle: &str,
    min_count: usize,
    timeout: Duration,
    phase: &str,
) -> eyre::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let missing: Vec<&String> =
            files.iter().filter(|file| !read_node_log(file).contains(needle)).collect();
        if files.len() - missing.len() >= min_count {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "{phase}: fewer than {min_count} of {files:?} contained {needle:?} within {}s \
                 (missing from {missing:?}, see test_logs/{TEST_NAME}/)",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Poll (250ms cadence) until `node`'s latest execution block number EXCEEDS `min_height`.
fn wait_for_block_above(
    node: &str,
    min_height: u64,
    timeout: Duration,
    phase: &str,
) -> eyre::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_seen = None;
    loop {
        if let Ok(number) = get_block_number(node) {
            if number > min_height {
                return Ok(());
            }
            last_seen = Some(number);
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "{phase}: {node} block number did not exceed {min_height} within {}s \
                 (last seen: {last_seen:?})",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Fetch `epoch`'s certified record pair over RPC.
///
/// `tn_epochRecord` only serves `(record, certificate)` pairs: a record whose certificate is
/// missing errors with not-found, so `Ok` proves the certificate exists on `node` and `Err`
/// (after the record's epoch has closed) proves it does not.
fn epoch_record_rpc(node: &str, epoch: u32) -> eyre::Result<(EpochRecord, EpochCertificate)> {
    call_rpc(node, "tn_epochRecord", rpc_params![epoch], 0, format!("tn_epochRecord({epoch})"))
}

/// Poll (250ms cadence) until `node` serves the certified record for `epoch`.
fn wait_for_certified_record(
    node: &str,
    epoch: u32,
    timeout: Duration,
    phase: &str,
) -> eyre::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if epoch_record_rpc(node, epoch).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "{phase}: {node} did not serve a certified record for epoch {epoch} within {}s",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Poll (250ms cadence) until `node` reports `tn_getCurrentEpoch >= target`.
fn wait_for_registry_epoch_at_least(
    node: &str,
    target: u32,
    timeout: Duration,
    phase: &str,
) -> eyre::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_seen = None;
    loop {
        if let Ok(epoch) = call_rpc::<u32, _, _>(
            node,
            "tn_getCurrentEpoch",
            rpc_params![],
            0,
            "tn_getCurrentEpoch",
        ) {
            if epoch >= target {
                return Ok(());
            }
            last_seen = Some(epoch);
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "{phase}: {node} did not reach epoch {target} within {}s (last seen: {last_seen:?})",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Recover an entire committee SIGKILLed while parked at its first fork-active epoch entry
/// with the parked-on epoch record on disk everywhere and its certificate NOWHERE.
///
/// 1. SEED run 1: all 4 validators, fork at epoch 2, NO hook. Epoch 0 closes and certifies normally
///    (the incident fleet held certificates for every epoch before the parked boundary), then the
///    fleet is gracefully restarted so run 2's environment latches.
/// 2. SEED run 2: all 4 validators with the cert-suppression hook. Epoch 1 closes, record 1 is
///    written, its vote quorum succeeds and the certificate is DISCARDED on every node, and every
///    node parks entering fork-active epoch 2. RPC probes then pin the manufactured state exactly:
///    every node serves certified record 0 and NO node serves a record-1 certificate.
/// 3. KILL: SIGKILL the whole committee at once, mid-park.
/// 4. RECOVERY (run 3): restart validators 0-2 (and a fresh observer) WITHOUT the hook. Each must
///    re-park, subscribe to the vote topic pre-anchor, re-arm the record-1 vote round, and
///    re-assemble the certificate from deterministic re-votes (super_quorum of 4 = 3, so the three
///    restarted nodes are exactly quorum). Validator 3 restarts ~60s late and must instead fetch
///    the already-assembled certificate from its recovered peers.
/// 5. PROOF OF LIFE: blocks advance on all 4 validators, the first fully fork-active close (epoch
///    2) certifies live, the registry advances to epoch 3+, and the observer follows past the
///    parked height.
#[test]
#[ignore = "only run independently from all other it tests"]
fn test_epoch_cert_recovery() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    info!(target: "epoch-cert-recovery", "configuring testnet for {TEST_NAME}");

    let tmp_guard = tempfile::TempDir::with_prefix(TEST_NAME)?;
    let temp_path = tmp_guard.path().to_path_buf();
    // Standard 4-validator + observer genesis with the shared epoch-test cadence.
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION as u32),
    )?;
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();

    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0; 4];
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
    }

    // ---- 1. SEED run 1: certify epoch 0 normally (no hook) ----------------------------------
    info!(target: "epoch-cert-recovery", "seed run 1: starting 4 validators, epoch 0 certifies");
    for i in 0..4 {
        guard.push(start_validator_with_envs(
            i,
            &bin,
            &temp_path,
            rpc_ports[i],
            TEST_NAME,
            1,
            &[FORK_EPOCH_ENV],
        ));
    }

    // Startup + the epoch-0 close + vote-quorum margin. Only the first wait absorbs real
    // latency; the rest observe events that happened in parallel.
    let seed1_deadline = Duration::from_secs(EPOCH_DURATION * 4 + 30);
    for i in 0..4 {
        // Each node aggregates (or fetches) the record-0 certificate...
        wait_for_log(
            &run_log(i, 1),
            &[QUORUM_EPOCH_0, RETRIEVED_CERT_0, OBTAINED_CERT_0],
            seed1_deadline,
            "seed run 1 epoch-0 certification",
        )?;
        // ...and serves it over RPC, proving it reached the epoch-record store.
        wait_for_certified_record(&client_urls[i], 0, seed1_deadline, "seed run 1 record 0")?;
    }

    // Graceful restart boundary: the suppression hook is read once per process, so it can only
    // arm on a fresh process. Parallel SIGTERM (not SIGKILL) so run 1's state flushes cleanly —
    // the crash under test is the one below, after the state is manufactured. Parallel matters:
    // sequential kills would leave a 3/4 quorum running for seconds, and if that window
    // straddled epoch 1's boundary the un-suppressed survivors would certify record 1.
    guard.kill_all();
    info!(target: "epoch-cert-recovery", "seed run 1 complete: epoch 0 certified, fleet stopped");

    // ---- 2. SEED run 2: suppress the epoch-1 certificate and park the fleet -----------------
    info!(target: "epoch-cert-recovery", "seed run 2: restarting with cert suppression");
    for i in 0..4 {
        guard.replace(
            i,
            start_validator_with_envs(
                i,
                &bin,
                &temp_path,
                rpc_ports[i],
                TEST_NAME,
                2,
                &[FORK_EPOCH_ENV, SUPPRESS_CERTS_ENV],
            ),
        );
    }

    // Restart + replay + the epoch-1 close (live or replayed) + vote-quorum margin.
    let seed2_deadline = Duration::from_secs(EPOCH_DURATION * 6 + 30);
    for i in 0..4 {
        let log = run_log(i, 2);
        // Record 1 reached vote quorum and its certificate was discarded on this node...
        wait_for_log(&log, &[SUPPRESSED_CERT_EPOCH_1], seed2_deadline, "seed run 2 suppression")?;
        // ...and this node parked entering fork-active epoch 2 on the uncertified record.
        wait_for_log(&log, &[PARK_WARN], seed2_deadline, "seed run 2 park")?;
    }

    // Pin the manufactured state over RPC: every node still serves certified record 0, and no
    // node serves a record-1 certificate (`tn_epochRecord` only serves record+cert pairs; the
    // suppressed quorum stores nothing and no other source can supply the cert, so a single
    // post-park probe per node is conclusive).
    for (i, url) in client_urls.iter().enumerate() {
        wait_for_certified_record(url, 0, Duration::from_secs(15), "seed run 2 record 0 kept")?;
        if epoch_record_rpc(url, 1).is_ok() {
            return Err(eyre::eyre!(
                "seed run 2: validator {i} serves a record-1 certificate; suppression failed \
                 (epoch 1 must have closed before the hook armed — see test_logs/{TEST_NAME}/)"
            ));
        }
    }
    info!(target: "epoch-cert-recovery", "seed complete: records everywhere, record-1 cert nowhere, fleet parked");

    // The parked chain tip (epoch 1's closing block); recovery must grow past it everywhere.
    let mut parked_height = 0u64;
    for url in client_urls.iter() {
        let height = get_block_number(url).inspect_err(
            |e| error!(target: "epoch-cert-recovery", ?e, url, "parked height read failed"),
        )?;
        parked_height = parked_height.max(height);
    }
    info!(target: "epoch-cert-recovery", parked_height, "captured parked chain height");

    // ---- 3. KILL: SIGKILL the whole committee at once, mid-park -----------------------------
    for i in 0..4 {
        let mut child = guard
            .take(i)
            .ok_or_else(|| eyre::eyre!("kill phase: no child process for validator {i}"))?;
        force_kill_and_reap(&mut child);
    }
    // Confirm every node is really down before restarting (retries = 0: fail fast).
    let probe_account = address_from_word("cert-recovery-probe").to_string();
    for (i, url) in client_urls.iter().enumerate() {
        if get_balance(url, &probe_account, 0).is_ok() {
            return Err(eyre::eyre!("kill phase: validator {i} still serving RPC after SIGKILL"));
        }
    }
    info!(target: "epoch-cert-recovery", "kill phase complete: all 4 validators down");

    // ---- 4. RECOVERY PHASE (run 3): restart WITHOUT the suppression hook --------------------
    let recovery_started = Instant::now();
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    // Fresh observer follows the recovered network from genesis (index 4 in guard + logs).
    guard.replace(
        4,
        start_observer_with_envs(
            4,
            &bin,
            &temp_path,
            obs_rpc_port,
            TEST_NAME,
            3,
            &[FORK_EPOCH_ENV],
        ),
    );
    // Validators 0-2 restart promptly; validator 3 is deliberately held back (below).
    for i in 0..3 {
        guard.replace(
            i,
            start_validator_with_envs(
                i,
                &bin,
                &temp_path,
                rpc_ports[i],
                TEST_NAME,
                3,
                &[FORK_EPOCH_ENV],
            ),
        );
    }

    // (a) Each restarted node re-parks AND runs the new pre-park arrangements. Ordering
    // invariant makes this race-free: the record-1 quorum needs 3 votes, and each node only
    // votes after its own park -> subscribe -> re-arm sequence, so no certificate can exist
    // anywhere before all three nodes have logged all three lines.
    let repark_deadline = Duration::from_secs(90);
    for i in 0..3 {
        let log = run_log(i, 3);
        wait_for_log(&log, &[PARK_WARN], repark_deadline, "recovery re-park")?;
        wait_for_log(
            &log,
            &[PRE_ANCHOR_SUBSCRIBE],
            repark_deadline,
            "recovery pre-park subscribe",
        )?;
        wait_for_log(&log, &[REARM_EPOCH_1], repark_deadline, "recovery vote-round re-arm")?;
    }

    // (b) The three restarted nodes re-assemble the record-1 certificate from deterministic
    // re-votes: super_quorum(4) = 3, so all three must aggregate a quorum locally.
    let early_logs: Vec<String> = (0..3).map(|i| run_log(i, 3)).collect();
    wait_for_log_quorum(
        &early_logs,
        QUORUM_EPOCH_1,
        3,
        Duration::from_secs(120),
        "recovery re-vote quorum",
    )?;
    info!(target: "epoch-cert-recovery", "recovery quorum reached on nodes 0-2; holding node 3 back");

    // Validator 3 restarts late — the ONE deliberate plain sleep in this test. The re-vote
    // exchange above has already completed, so node 3 must recover via the late-joiner path.
    if let Some(remaining) =
        Duration::from_secs(LATE_START_SECS).checked_sub(recovery_started.elapsed())
    {
        info!(
            target: "epoch-cert-recovery",
            "sleeping {}s more before validator 3's late restart", remaining.as_secs()
        );
        std::thread::sleep(remaining);
    }
    guard.replace(
        3,
        start_validator_with_envs(
            3,
            &bin,
            &temp_path,
            rpc_ports[3],
            TEST_NAME,
            3,
            &[FORK_EPOCH_ENV],
        ),
    );

    // Node 3 obtains the record-1 certificate by any of the three recovery paths: its own
    // (unlikely) re-vote quorum, a peer fetch, or a vote round observing the cert's arrival.
    wait_for_log(
        &run_log(3, 3),
        &[QUORUM_EPOCH_1, RETRIEVED_CERT_1, OBTAINED_CERT_1],
        Duration::from_secs(180),
        "late-joiner cert recovery",
    )?;
    info!(target: "epoch-cert-recovery", "validator 3 recovered the record-1 certificate");

    // (c) Blocks advance on all 4 validators: every node's tip must grow past the parked
    // height (each epoch close mints at least the epoch-closing block).
    network_advancing(&client_urls).inspect_err(|e| {
        error!(target: "epoch-cert-recovery", ?e, "recovered network not advancing");
    })?;
    for (i, url) in client_urls.iter().enumerate() {
        wait_for_block_above(
            url,
            parked_height,
            Duration::from_secs(180),
            &format!("recovery block progress on validator {i}"),
        )?;
    }

    // (d) The NEXT close certifies LIVE: epoch 2 is the first fully fork-active epoch (its
    // headers carry seed signatures), and its close is the live path the incident never
    // survived. Quorum lines on >= 3 nodes plus registry entry into epoch 3 prove it.
    let recovery_logs: Vec<String> = (0..4).map(|i| run_log(i, 3)).collect();
    wait_for_log_quorum(
        &recovery_logs,
        QUORUM_EPOCH_2,
        3,
        Duration::from_secs(EPOCH_DURATION * 6),
        "fork-active live close quorum",
    )?;
    wait_for_registry_epoch_at_least(
        &client_urls[0],
        3,
        Duration::from_secs(EPOCH_DURATION * 6),
        "fork-active epoch progression",
    )?;

    // (e) The observer follows the recovered network past the parked height.
    wait_for_block_above(
        &obs_url,
        parked_height,
        Duration::from_secs(240),
        "observer follows recovery",
    )?;

    info!(target: "epoch-cert-recovery", "test complete: fleet recovered unattended");
    // Clean shutdown of all 5 children via the guard's drop.
    Ok(())
}
