//! E2e test for a validator's reth database moving from MDBX to tndb-b across a restart.
//!
//! A node built with the `tndb-b` feature that finds an MDBX reth database migrates it when it
//! first opens it (`tn_reth`'s `migrate_from_mdbx`). This test runs that on live validators: the
//! committee starts on the default (MDBX) binary, then each validator in turn is stopped and
//! restarted on the `tndb-b` binary, and must come back migrated and follow the chain.
//!
//! **Binaries.** The test uses the harness's two binaries in fixed roles:
//! - `TN_BIN_PATH_PREV` ([`e2e_tests::get_previous_telcoin_network_binary`]) is the default e2e
//!   binary from `make build-e2e-bin`, whose reth database is MDBX;
//! - `TN_BIN_PATH` ([`e2e_tests::get_telcoin_network_binary`]) is the `tndb-b` e2e binary from
//!   `make build-e2e-bin-tndb`.
//!
//! `make test-e2e-reth-migration` passes both, and the marker `TN_E2E_TNDB_BIN`, which says that
//! `TN_BIN_PATH` is the `tndb-b` build. Without the marker the test skips with a warning: `make
//! test-e2e` selects every ignored test with `TN_BIN_PATH` at the default binary, and a
//! `TN_BIN_PATH_PREV` exported for the mixed-binary tests (`make build-e2e-bin-prev`) would
//! otherwise run this test against two MDBX binaries. With the marker set, a missing binary or
//! both variables naming one file fails the test.
//!
//! **No vacuous pass.** The files prove the roles: before each restart the stopped validator's
//! `db/` must hold `mdbx.dat` and no `tndb.dat` (the previous binary is MDBX), and after it the
//! migrated layout plus the migration's line in that run's own log (the current binary is
//! `tndb-b` and migrated this database).

use crate::{
    common::{
        acquire_test_permit, address_from_word, call_rpc, current_epoch, get_block_number, get_key,
        get_node_mode, network_advancing, node_log_path, send_and_confirm, send_term,
        start_validator, strip_ansi, wait_for_epoch_at_least, wait_for_rpc, ProcessGuard,
        EPOCH_DURATION,
    },
    restarts::test_blocks_same,
};
use alloy::providers::ProviderBuilder;
use e2e_tests::{config_local_testnet_with_epoch_duration, TestBinary};
use jsonrpsee::rpc_params;
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    time::Duration,
};
use tn_test_utils::wait_until_blocking;
use tn_types::{get_available_tcp_port, NodeMode};
use tracing::{info, warn};

/// The test's name: its `test_logs/` directory and temp-dir prefix. Short, because every node's
/// IPC socket path is built under the temp dir and a unix socket path is capped at ~104 bytes.
const TEST: &str = "rethmig";

/// The marker `make test-e2e-reth-migration` sets: `TN_BIN_PATH` is the `tndb-b` e2e binary.
const TNDB_BIN_MARKER: &str = "TN_E2E_TNDB_BIN";

/// The line the `tndb-b` node logs once a migration has committed (`migrate_from_mdbx`).
const MIGRATED_LOG: &str = "migrated the reth database from MDBX to tndb-b";

/// The validator restarted first; the others follow in a rolling upgrade.
const FIRST: usize = 2;

/// How long a stopped validator has to exit on SIGTERM. A graceful shutdown takes a few seconds
/// (the task manager's join timeout); more than this is a hang, not a slow exit.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound for a restarted validator to migrate, open and answer RPC.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// Bound for a restarted validator to pass the tip and return to active consensus. Blocks come
/// at least once an epoch (the closing block), so this spans several epochs.
const FOLLOW_TIMEOUT: Duration = Duration::from_secs(90);

/// Decide whether this process is the migration lane, and return its (previous, current)
/// binaries. `Ok(None)` means "not this lane, skip"; `Err` means the lane is misconfigured.
fn lane_binaries() -> eyre::Result<Option<(&'static TestBinary, &'static TestBinary)>> {
    if std::env::var_os(TNDB_BIN_MARKER).filter(|value| !value.is_empty()).is_none() {
        const SKIPPED: &str = "SKIPPING the reth database migration test: TN_E2E_TNDB_BIN is \
                               unset, so TN_BIN_PATH is not declared the tndb-b e2e binary. Run \
                               `make test-e2e-reth-migration`, which builds both binaries and \
                               passes TN_BIN_PATH (tndb-b) and TN_BIN_PATH_PREV (MDBX).";
        warn!(target: "reth-migration-test", "{SKIPPED}");
        // also unconditionally: the test tracing filter drops `warn!` when RUST_LOG is unset
        eprintln!("{SKIPPED}");
        return Ok(None);
    }
    let previous = e2e_tests::get_previous_telcoin_network_binary().ok_or_else(|| {
        eyre::eyre!(
            "{TNDB_BIN_MARKER} is set but TN_BIN_PATH_PREV is unset: the test needs the default \
             (MDBX) e2e binary there (`make test-e2e-reth-migration` passes it)"
        )
    })?;
    // checked before resolving the binary: with TN_BIN_PATH unset the harness builds the
    // default (MDBX) binary through cargo, which would only fail later, after minutes of build
    let current_path = std::env::var_os("TN_BIN_PATH")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            eyre::eyre!(
                "{TNDB_BIN_MARKER} is set but TN_BIN_PATH is unset: the test needs the tndb-b \
                 e2e binary there (`make test-e2e-reth-migration` passes it)"
            )
        })?;
    if let TestBinary::Prebuilt(previous_path) = previous {
        eyre::ensure!(
            std::fs::canonicalize(previous_path)? != std::fs::canonicalize(&current_path)?,
            "TN_BIN_PATH and TN_BIN_PATH_PREV name the same file ({}): the test needs the tndb-b \
             binary in TN_BIN_PATH and the default (MDBX) binary in TN_BIN_PATH_PREV",
            current_path.display()
        );
    }
    Ok(Some((previous, e2e_tests::get_telcoin_network_binary())))
}

/// `true` when `path` exists; a stat error fails rather than reading as absent.
fn exists(path: &Path) -> eyre::Result<bool> {
    path.try_exists().map_err(|e| eyre::eyre!("failed to stat {}: {e}", path.display()))
}

/// A validator's datadir under the test's temp dir (`start_validator`'s layout).
fn datadir(base: &Path, instance: usize) -> PathBuf {
    base.join(format!("validator-{}", instance + 1))
}

/// Assert a stopped validator's reth database is MDBX: `db/mdbx.dat` and no `db/tndb.dat`.
fn assert_mdbx_layout(datadir: &Path) -> eyre::Result<()> {
    let db = datadir.join("db");
    eyre::ensure!(
        exists(&db.join("mdbx.dat"))?,
        "{} has no mdbx.dat: TN_BIN_PATH_PREV must be the default (MDBX) e2e binary",
        db.display()
    );
    eyre::ensure!(
        !exists(&db.join("tndb.dat"))?,
        "{} holds a tndb.dat before the restart on TN_BIN_PATH: TN_BIN_PATH_PREV must be the \
         default (MDBX) e2e binary",
        db.display()
    );
    Ok(())
}

/// Assert a restarted validator's reth database finished its migration: `db/tndb.dat` and the
/// `db/mdbx-migrated` marker, none of MDBX's files, no temp directory, and the static files kept.
fn assert_migrated_layout(datadir: &Path) -> eyre::Result<()> {
    let db = datadir.join("db");
    eyre::ensure!(
        exists(&db.join("tndb.dat"))?,
        "{} has no tndb.dat after the restart: TN_BIN_PATH must be the tndb-b e2e binary",
        db.display()
    );
    eyre::ensure!(
        exists(&db.join("mdbx-migrated"))?,
        "{} has no mdbx-migrated marker after the restart",
        db.display()
    );
    for leftover in ["mdbx.dat", "mdbx.lck", "tndb-migrating"] {
        eyre::ensure!(
            !exists(&db.join(leftover))?,
            "{} still holds {leftover} after the migration",
            db.display()
        );
    }
    let static_files = datadir.join("static_files");
    eyre::ensure!(
        std::fs::metadata(&static_files)?.is_dir(),
        "{} is not a directory after the migration",
        static_files.display()
    );
    Ok(())
}

/// The migration's log line from run `run` of validator `instance`, without ANSI escapes.
fn migrated_log_line(instance: usize, run: u32) -> eyre::Result<String> {
    let path = node_log_path(TEST, instance, run, false);
    let log = String::from_utf8_lossy(&std::fs::read(&path)?).into_owned();
    log.lines()
        .map(strip_ansi)
        .find(|line| line.contains(MIGRATED_LOG))
        .ok_or_else(|| eyre::eyre!("{} has no `{MIGRATED_LOG}` line", path.display()))
}

/// The highest block number among the validators at `instances`.
fn tip(client_urls: &[String; 4], instances: impl IntoIterator<Item = usize>) -> u64 {
    instances.into_iter().filter_map(|i| get_block_number(&client_urls[i]).ok()).max().unwrap_or(0)
}

/// Stop validator `instance` with SIGTERM and wait for it to exit on its own. A validator that
/// does not stop within [`STOP_TIMEOUT`] fails the test (the guard still holds it), rather than
/// being killed, so the database the next run opens is a cleanly closed one.
fn stop_validator(guard: &mut ProcessGuard, instance: usize) -> eyre::Result<()> {
    let child =
        guard.get_mut(instance).ok_or_else(|| eyre::eyre!("no validator-{}", instance + 1))?;
    send_term(child);
    let (_, status) = guard
        .wait_for_natural_exits([instance], STOP_TIMEOUT)?
        .pop()
        .ok_or_else(|| eyre::eyre!("no exit status for validator-{}", instance + 1))?;
    info!(target: "reth-migration-test", instance, ?status, "validator stopped");
    Ok(())
}

/// Wait for a restarted validator's RPC, failing at once if its process exits (a refused or
/// failed migration stops the node).
fn wait_for_restarted_rpc(
    guard: &mut ProcessGuard,
    instance: usize,
    node: &str,
    run: u32,
) -> eyre::Result<()> {
    let child = RefCell::new(
        guard.get_mut(instance).ok_or_else(|| eyre::eyre!("no validator-{}", instance + 1))?,
    );
    let description = format!(
        "restarted validator-{} RPC at {node} (logs: test_logs/{TEST}/node{instance}-run{run}.log \
         and its .stderr.log)",
        instance + 1
    );
    wait_until_blocking(START_TIMEOUT, &description, || {
        child.try_borrow_mut()?.try_wait()?.map_or(Ok(()), |status| {
            eyre::bail!("{description}: the process exited before its RPC was ready: {status}")
        })?;
        let response: eyre::Result<String> =
            call_rpc(node, "eth_blockNumber", rpc_params![], 0, "migration restart readiness");
        Ok(response.is_ok())
    })
}

/// Stop validator `instance`, check its database is MDBX, restart it on `current`, and check that
/// it migrated, passes the tip the network had when it stopped, and rejoins active consensus.
fn upgrade_validator(
    guard: &mut ProcessGuard,
    base: &Path,
    client_urls: &[String; 4],
    rpc_ports: &[u16; 4],
    current: &'static TestBinary,
    instance: usize,
) -> eyre::Result<()> {
    const RUN: u32 = 1;
    let node = &client_urls[instance];
    let datadir = datadir(base, instance);

    stop_validator(guard, instance)?;
    assert_mdbx_layout(&datadir)?;
    let peers = (0..4).filter(|i| *i != instance);
    let tip_at_stop = tip(client_urls, peers);

    guard.replace(
        instance,
        start_validator(instance, current, base, rpc_ports[instance], TEST, RUN),
    );
    wait_for_restarted_rpc(guard, instance, node, RUN)?;
    assert_migrated_layout(&datadir)?;
    let line = migrated_log_line(instance, RUN)?;
    info!(target: "reth-migration-test", instance, %line, "validator migrated");

    wait_until_blocking(
        FOLLOW_TIMEOUT,
        &format!("migrated validator-{} to pass block {tip_at_stop}", instance + 1),
        || Ok(get_block_number(node).is_ok_and(|number| number > tip_at_stop)),
    )?;
    wait_until_blocking(
        FOLLOW_TIMEOUT,
        &format!("migrated validator-{} to return to CvvActive", instance + 1),
        || Ok(get_node_mode(node).is_ok_and(|mode| mode == NodeMode::CvvActive)),
    )
}

/// The test body: the network on `previous`, then a rolling restart onto `current`.
fn run_migration(
    guard: &mut ProcessGuard,
    base: &Path,
    previous: &'static TestBinary,
    current: &'static TestBinary,
) -> eyre::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
    let mut rpc_ports = [0u16; 4];
    for (instance, rpc_port) in rpc_ports.iter_mut().enumerate() {
        *rpc_port =
            get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no free local port"))?;
        guard.push(start_validator(instance, previous, base, *rpc_port, TEST, 0));
    }
    let client_urls = rpc_ports.map(|port| format!("http://127.0.0.1:{port}"));
    let reference = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    runtime.block_on(wait_for_rpc(&reference))?;
    network_advancing(&client_urls)?;

    // real state in the MDBX databases: two closed epochs (their closing blocks run the
    // registry's system calls) and a transfer
    runtime.block_on(wait_for_epoch_at_least(&reference, 2))?;
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    send_and_confirm(&client_urls[0], &client_urls[FIRST], &key, to_account, 0)?;
    test_blocks_same(&client_urls)?;

    // one validator onto tndb-b, beside three on MDBX
    upgrade_validator(guard, base, &client_urls, &rpc_ports, current, FIRST)?;
    // a transfer executed after the migration, read on the migrated validator
    send_and_confirm(&client_urls[0], &client_urls[FIRST], &key, to_account, 1)?;
    test_blocks_same(&client_urls)?;

    // the rest, one at a time: each rejoins active consensus before the next stops, so three of
    // the four (a quorum) always vote
    for instance in (0..4).filter(|i| *i != FIRST) {
        upgrade_validator(guard, base, &client_urls, &rpc_ports, current, instance)?;
    }

    // the network, now all on tndb-b, keeps closing epochs and executing transfers
    let epoch = runtime.block_on(current_epoch(&reference))?.epoch_id;
    runtime.block_on(wait_for_epoch_at_least(&reference, epoch + 1))?;
    send_and_confirm(&client_urls[1], &client_urls[3], &key, to_account, 2)?;
    test_blocks_same(&client_urls)
}

/// A committee started on the MDBX binary moves to the tndb-b binary one validator at a time:
/// each restarted validator migrates its reth database and follows the chain, and the network
/// keeps producing blocks throughout.
#[test]
#[ignore = "should not run with a default cargo test, run with `make test-e2e-reth-migration`"]
fn test_restart_migrates_reth_db_to_tndb() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let Some((previous, current)) = lane_binaries()? else {
        return Ok(());
    };
    let temp_dir = tempfile::TempDir::with_prefix(TEST)?;
    config_local_testnet_with_epoch_duration(
        temp_dir.path(),
        Some("restart_test".to_string()),
        None,
        Some(EPOCH_DURATION as u32),
    )?;

    let mut guard = ProcessGuard::empty();
    match run_migration(&mut guard, temp_dir.path(), previous, current) {
        Ok(()) => {
            guard.finish();
            Ok(())
        }
        Err(error) => {
            // stop the nodes first, so the retained datadirs are the ones they closed
            guard.kill_all();
            let retained = temp_dir.keep();
            Err(error.wrap_err(format!(
                "reth database migration test failed; retained datadir: {}; node logs: \
                 test_logs/{TEST}/",
                retained.display()
            )))
        }
    }
}
