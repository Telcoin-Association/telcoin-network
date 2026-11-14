//! E2e test for rebuilding a validator's execution database from genesis with
//! `telcoin-network db reset-execution`.
//!
//! An operator whose execution database has to change layout (or is suspect) stops the node, runs
//! `db reset-execution --force`, and starts it again: the execution database (`db`,
//! `static_files`, `restored-state-floor`) is moved aside into `execution-reset-<UTC time>`, the
//! consensus database, keys and config stay, and the next start creates a new execution database
//! and rebuilds it by executing the chain again from genesis. This test runs that on a live
//! validator of a network that started on `TN_BIN_PATH_PREV`, after an in-place upgrade.
//!
//! The run, in order:
//!
//! 1. Four validators on `TN_BIN_PATH_PREV` (an MDBX reth database). A token contract is deployed
//!    and called (contract storage), value transfers go around a ring of accounts, and a light
//!    transfer load runs through validator-1 to validator-3 for the whole run, while at least three
//!    epochs close (each closing block runs the `ConsensusRegistry` system calls).
//! 2. In-place upgrade: validator-4 stops and starts on `TN_BIN_PATH` (the default, tn-lmdb, build)
//!    over the same datadir. Its reth database must move MDBX to tn-lmdb, and its consensus
//!    database MDBX to tndb when `TN_BIN_PATH_PREV` is a release from before tndb
//!    (`migrate_from_mdbx` in `tn_reth` and `tn_storage`; a `TN_BIN_PATH_PREV` built after tndb has
//!    nothing to move there). The migrated reth datadir must keep its storage layout, v1, with TN's
//!    layout marker (recorded on this open when `TN_BIN_PATH_PREV` predates it), and log that it
//!    stays v1 although new datadirs get v2. It must follow the chain again.
//! 3. Reset: validator-4 stops, `db reset-execution` runs on its datadir, first as a dry run
//!    (nothing moves), then with `--force` (the execution entries move into `execution-reset-*`).
//! 4. Rebuild: validator-4 starts on `TN_BIN_PATH` again. It must create a new reth datadir in
//!    storage layout v2 (change sets and senders in static files), execute the chain from genesis
//!    to the tip, return to `CvvActive`, take a round of transactions through its own worker, and
//!    close an epoch with the others.
//! 5. Compare validator-4 (now v2) with validator-1, which never moved and still runs
//!    `TN_BIN_PATH_PREV` (v1 on MDBX): every block from genesis (hash, stateRoot, receiptsRoot),
//!    balances, `ConsensusRegistry` and token storage and token `eth_call`s at past blocks, and the
//!    recorded transactions and their receipts by hash.
//!
//! **Binaries and lane.** As in `reth_db_migration`, the test runs only with the marker
//! `TN_E2E_TN_LMDB_BIN` set (`TN_BIN_PATH` is the default, tn-lmdb, build) and skips with a message
//! without it. With the marker set, a missing `TN_BIN_PATH` or `TN_BIN_PATH_PREV` fails the test.
//! `make test-e2e-resync` passes all three. The files prove the roles: validator-4's datadir holds
//! MDBX files before the upgrade and the migrated layout after it, and validator-1's reth database
//! is still MDBX at the end.
//!
//! **The reference.** Validator-1 executes with whatever `TN_BIN_PATH_PREV` is. `make
//! test-e2e-resync` defaults it (`TN_RESYNC_BIN_PREV`) to this tree's MDBX e2e build, which runs
//! this tree's executor: on that lane step 5 compares layout v2 on tn-lmdb with layout v1 on MDBX
//! under the same execution code, and finds a storage or rebuild fault, not an execution change
//! against a released binary. For a released reference, build it (`make build-e2e-bin-prev
//! TN_PREV_REF=<tag>`) and pass it as `TN_RESYNC_BIN_PREV`.

use crate::{
    common::{
        acquire_test_permit, assert_epoch_records_verify, current_epoch, get_block_number,
        get_node_mode, node_log_path, start_validator, strip_ansi, wait_for_epoch_at_least,
        wait_for_head_at_least, wait_for_node_mode, wait_for_rpc, ProcessGuard,
    },
    reth_db_migration::{
        assert_mdbx_layout, assert_migrated_layout, exists, stop_validator, MIGRATED_LOG,
        TN_LMDB_BIN_MARKER,
    },
    upgrade::{block_roots, light_load_senders, LightLoad, Workload},
};
use alloy::{
    eips::BlockId,
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    rpc::types::{TransactionReceipt, TransactionRequest},
    sol,
    sol_types::SolCall as _,
};
use e2e_tests::{config_local_testnet_with_epoch_duration, NodeEndpoints, TestBinary};
use eyre::WrapErr as _;
use serde_json::Value;
use std::{
    cell::RefCell,
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{
    system_calls::CONSENSUS_REGISTRY_ADDRESS, test_utils::TransactionFactory, RethChainSpec,
};
use tn_test_utils::wait_until_blocking;
use tn_types::{get_available_tcp_port, Address, Genesis, GenesisAccount, NodeMode, B256, U256};
use tokio::time::timeout;
use tracing::{info, warn};

/// The test's name: its `test_logs/` directory and temp-dir prefix. Short, because every node's
/// IPC socket path is built under the temp dir and a unix socket path is capped at ~104 bytes.
const TEST: &str = "resync";

/// Validators in the committee.
const VALIDATORS: usize = 4;

/// The validator that is upgraded, reset and rebuilt: validator-4.
const RESET: usize = 3;

/// The validator the rebuilt one is compared with: validator-1, which stays on the previous
/// binary for the whole run.
const REFERENCE: usize = 0;

/// Epoch duration in seconds: short, so the rebuild crosses many epoch boundaries.
const EPOCH_SECS: u64 = 10;

/// Epochs that close on the previous binary before the upgrade.
const PREV_EPOCHS: u32 = 3;

/// BLS passphrase the keys are written with; `start_validator` hands the same one to every node.
const PASSPHRASE: &str = "restart_test";

/// Log run numbers of validator-4's three processes.
const RUN_PREV: u32 = 0;
const RUN_UPGRADED: u32 = 1;
const RUN_REBUILT: u32 = 2;

/// Bound for a started validator to open its databases (migrating them first) and answer RPC.
const START_TIMEOUT: Duration = Duration::from_secs(90);

/// Bound for the upgraded validator to pass the tip and return to `CvvActive`.
const FOLLOW_TIMEOUT: Duration = Duration::from_secs(90);

/// Bound for the reset validator to execute the chain from genesis up to the tip the others had
/// when it started, and to return to `CvvActive`.
const REBUILD_TIMEOUT: Duration = Duration::from_secs(300);

/// Bound for an epoch to close.
const EPOCH_CLOSE_TIMEOUT: Duration = Duration::from_secs(EPOCH_SECS * 6);

/// Bound on each node serving the certified record of a closed epoch.
const EPOCH_RECORD_TIMEOUT_SECS: u64 = 60;

/// `ConsensusRegistry` storage slots compared at past blocks: its own and its parents' plain
/// (non-namespaced) variables, `currentEpoch` and the `epochInfo` ring among them.
const REGISTRY_SLOTS: u64 = 64;

/// The ERC-1967 implementation slot, `keccak256("eip1967.proxy.implementation") - 1`: written by
/// the token proxy's constructor.
const ERC1967_IMPLEMENTATION_SLOT: B256 =
    B256::new(alloy::hex!("360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"));

/// The lines `tn_reth`'s storage layout guard logs on open (target `tn::reth`).
const LAYOUT_MARKER_RECORDED_LOG: &str =
    "recorded TN's storage layout marker in an existing reth datadir";
const NEW_DATADIR_LOG: &str = "new reth datadir: storage layout";
/// Logged on an open of an existing reth datadir in the layout new datadirs get (a substring of
/// [`NEW_DATADIR_LOG`] too, so it is only looked for in a run that opened an existing one).
const EXISTING_DATADIR_LOG: &str = "reth datadir: storage layout";
/// Logged instead of [`EXISTING_DATADIR_LOG`] on an open of an existing reth datadir in the other
/// layout: a migrated v1 datadir under a binary that gives new datadirs v2.
const KEEPS_LAYOUT_LOG: &str = "this reth datadir keeps its storage layout";

/// The name prefix of the static files of the account change set segment, which only storage
/// layout v2 writes (v1 keeps change sets in database tables).
const V2_SEGMENT_PREFIX: &str = "static_file_account-change-sets_";

/// The prefix of the directory `db reset-execution` moves the execution entries into.
const RESET_DIR_PREFIX: &str = "execution-reset-";

/// The datadir entries `db reset-execution` moves.
const EXECUTION_ENTRIES: [&str; 3] = ["db", "static_files", "restored-state-floor"];

sol! {
    /// The read this test makes on the token behind the proxy.
    contract TokenBalance {
        function balanceOf(address account) external view returns (uint256);
    }
}

/// Log a phase result and print it to stderr, which `--no-capture` shows live.
fn progress(message: String) {
    info!(target: "resync-test", "{message}");
    eprintln!("resync: {message}");
}

/// A validator's datadir under the test's temp dir (`start_validator`'s layout).
fn datadir(base: &Path, instance: usize) -> PathBuf {
    base.join(format!("validator-{}", instance + 1))
}

/// The names in `dir`, sorted, each directory with a trailing `/`.
fn root_entries(dir: &Path) -> eyre::Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for entry in std::fs::read_dir(dir).wrap_err_with(|| format!("listing {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let suffix = if entry.file_type()?.is_dir() { "/" } else { "" };
        names.insert(format!("{name}{suffix}"));
    }
    Ok(names)
}

/// The lines of run `run` of validator `instance`'s stdout log that contain `needle`, without
/// ANSI escapes.
fn log_lines(instance: usize, run: u32, needle: &str) -> eyre::Result<Vec<String>> {
    let path = node_log_path(TEST, instance, run, false);
    let log = String::from_utf8_lossy(
        &std::fs::read(&path).wrap_err_with(|| format!("reading {}", path.display()))?,
    )
    .into_owned();
    Ok(log.lines().map(strip_ansi).filter(|line| line.contains(needle)).collect())
}

/// The one line of run `run` of validator `instance` that contains `needle`, which must also
/// report each of `fields` (`layout=v1`, `new_datadirs=v2`).
fn layout_log_line(
    instance: usize,
    run: u32,
    needle: &str,
    fields: &[&str],
) -> eyre::Result<String> {
    let lines = log_lines(instance, run, needle)?;
    let path = node_log_path(TEST, instance, run, false);
    let [line] = lines.as_slice() else {
        eyre::bail!(
            "{} has {} `{needle}` lines, expected one: {lines:?}",
            path.display(),
            lines.len()
        );
    };
    for field in fields {
        eyre::ensure!(line.contains(field), "{}: `{line}` does not report {field}", path.display());
    }
    Ok(line.clone())
}

/// The static files in `datadir` of a segment only storage layout v2 writes.
fn v2_segment_files(datadir: &Path) -> eyre::Result<Vec<String>> {
    Ok(root_entries(&datadir.join("static_files"))?
        .into_iter()
        .filter(|name| name.starts_with(V2_SEGMENT_PREFIX))
        .collect())
}

/// Assert a stopped validator's consensus database is the previous release's MDBX one:
/// `consensus-db/epoch/mdbx.dat` and no tndb directory.
fn assert_mdbx_consensus_layout(datadir: &Path) -> eyre::Result<()> {
    let store = datadir.join("consensus-db");
    eyre::ensure!(
        exists(&store.join("epoch").join("mdbx.dat"))?,
        "{} has no epoch/mdbx.dat: TN_BIN_PATH_PREV must be a release with the MDBX consensus \
         database",
        store.display()
    );
    eyre::ensure!(
        !exists(&store.join("tndb"))?,
        "{} holds a tndb directory before the upgrade: TN_BIN_PATH_PREV must be a release with \
         the MDBX consensus database",
        store.display()
    );
    Ok(())
}

/// Assert a validator's consensus database finished its move from MDBX to tndb: the
/// `tndb/mdbx-migrated` marker records a copy of at least one row (a running validator has epoch
/// and peer records), and none of the MDBX environments is left. Returns the marker's text.
fn assert_tndb_consensus_layout(datadir: &Path) -> eyre::Result<String> {
    let store = datadir.join("consensus-db");
    let marker_path = store.join("tndb").join("mdbx-migrated");
    let marker = std::fs::read_to_string(&marker_path).wrap_err_with(|| {
        format!("reading the consensus migration marker {}", marker_path.display())
    })?;
    let rows: u64 = marker
        .trim()
        .strip_prefix("copied ")
        .and_then(|rest| rest.strip_suffix(" rows from MDBX"))
        .and_then(|rows| rows.parse().ok())
        .ok_or_else(|| {
            eyre::eyre!("{}: unexpected marker text {marker:?}", marker_path.display())
        })?;
    eyre::ensure!(rows > 0, "{}: the migration copied no rows", marker_path.display());
    for env in ["epoch", "kad", "cache"] {
        let data = store.join(env).join("mdbx.dat");
        eyre::ensure!(!exists(&data)?, "{} is still there after the migration", data.display());
    }
    Ok(marker.trim().to_string())
}

/// Wait for validator `instance`'s RPC, failing at once if its process exits.
fn wait_for_started_rpc(
    guard: &mut ProcessGuard,
    instance: usize,
    url: &str,
    run: u32,
) -> eyre::Result<()> {
    let child = RefCell::new(
        guard.get_mut(instance).ok_or_else(|| eyre::eyre!("no validator-{}", instance + 1))?,
    );
    let description = format!(
        "validator-{} RPC at {url} (logs: test_logs/{TEST}/node{instance}-run{run}.log and its \
         .stderr.log)",
        instance + 1
    );
    wait_until_blocking(START_TIMEOUT, &description, || {
        child.try_borrow_mut()?.try_wait()?.map_or(Ok(()), |status| {
            eyre::bail!("{description}: the process exited before its RPC was ready: {status}")
        })?;
        Ok(get_block_number(url).is_ok())
    })
}

/// Run `bin db reset-execution --datadir <datadir>` (with `--force` when `force`) and return its
/// stdout. A failing run fails with both of its outputs.
fn run_reset_execution(bin: &TestBinary, datadir: &Path, force: bool) -> eyre::Result<String> {
    let mut command = bin.command();
    command.arg("db").arg("reset-execution").arg("--datadir").arg(datadir);
    if force {
        command.arg("--force");
    }
    let output = command.output().wrap_err("running db reset-execution")?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    eyre::ensure!(
        output.status.success(),
        "db reset-execution (force: {force}) exited with {}; stdout:\n{stdout}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(stdout)
}

/// The highest execution head among the validators other than `except`.
fn peer_tip(urls: &[String], except: usize) -> u64 {
    urls.iter()
        .enumerate()
        .filter(|(idx, _)| *idx != except)
        .filter_map(|(_, url)| get_block_number(url).ok())
        .max()
        .unwrap_or(0)
}

/// Wait for the epoch open on `url` now to close, and return its id.
async fn close_epoch(url: &str, phase: &str) -> eyre::Result<u32> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    let closing = current_epoch(&provider).await?.epoch_id;
    timeout(EPOCH_CLOSE_TIMEOUT, wait_for_epoch_at_least(&provider, closing + 1)).await.map_err(
        |_| eyre::eyre!("{phase}: epoch {closing} did not close within {EPOCH_CLOSE_TIMEOUT:?}"),
    )??;
    Ok(closing)
}

/// The transactions whose lookups by hash are compared at the end: the first ring transfer and
/// the mint of a workload round.
fn record(recorded: &mut Vec<(B256, u64)>, receipts: &[TransactionReceipt], ring: usize) {
    for receipt in [&receipts[0], &receipts[ring]] {
        recorded.push((receipt.transaction_hash, receipt.block_number.unwrap_or_default()));
    }
}

/// The phases of [`test_resync_execution_from_genesis`] on a network configured under `base`.
async fn run_resync(
    guard: &mut ProcessGuard,
    base: &Path,
    previous: &'static TestBinary,
    current: &'static TestBinary,
) -> eyre::Result<()> {
    let mut workload = Workload::new();
    let funding = U256::from(parse_ether("10_000")?);
    let accounts = workload
        .senders()
        .chain(light_load_senders().iter().map(TransactionFactory::address))
        .map(|address| (address, GenesisAccount::default().with_balance(funding)))
        .collect();
    config_local_testnet_with_epoch_duration(
        base,
        Some(PASSPHRASE.to_string()),
        Some(accounts),
        Some(EPOCH_SECS as u32),
    )?;
    let genesis: Genesis = Config::load_from_path(
        base.join("shared-genesis").join("genesis").join("genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let ring = workload.ring.len();
    // (hash, block) of the transactions compared by hash at the end
    let mut recorded: Vec<(B256, u64)> = Vec::new();

    // phase 1: four validators on PREV, load across several epoch closes
    let phase = "phase 1 (all PREV)";
    let mut rpc_ports = [0u16; VALIDATORS];
    for (instance, rpc_port) in rpc_ports.iter_mut().enumerate() {
        *rpc_port =
            get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no free local port"))?;
        guard.push(start_validator(instance, previous, base, *rpc_port, TEST, RUN_PREV));
    }
    let urls: Vec<String> =
        rpc_ports.iter().map(|port| format!("http://127.0.0.1:{port}")).collect();
    for url in &urls {
        wait_for_rpc(&ProviderBuilder::new().connect_http(url.parse()?)).await.wrap_err(phase)?;
    }
    for url in &urls {
        tokio::task::block_in_place(|| wait_for_node_mode(url, NodeMode::CvvActive))
            .wrap_err(phase)?;
    }
    let reference = &urls[REFERENCE];
    let reset_url = &urls[RESET];
    // through validator-1 to validator-3 only: validator-4 is stopped and rebuilt
    let load = LightLoad::start(&urls[..RESET], chain.clone())?;
    workload.deploy_token(reference, &chain).await.wrap_err(phase)?;
    let token = workload.token()?;
    let first_epoch =
        current_epoch(&ProviderBuilder::new().connect_http(reference.parse()?)).await?.epoch_id;
    for round in 0..PREV_EPOCHS as usize {
        let receipts = workload.round(&urls[round % VALIDATORS], &chain).await.wrap_err(phase)?;
        record(&mut recorded, &receipts, ring);
        let closed = close_epoch(reference, phase).await?;
        progress(format!("{phase}: epoch {closed} closed"));
    }
    let receipts = workload.round(&urls[RESET], &chain).await.wrap_err(phase)?;
    record(&mut recorded, &receipts, ring);
    let prev_tip = get_block_number(reference)?;
    progress(format!(
        "{phase}: epochs {first_epoch}..{} closed on PREV, token at {token}, head {prev_tip}",
        first_epoch + PREV_EPOCHS
    ));

    // phase 2: validator-4 upgraded in place to CURRENT
    let phase = "phase 2 (upgrade validator-4 in place)";
    let dir = datadir(base, RESET);
    tokio::task::block_in_place(|| stop_validator(guard, RESET)).wrap_err(phase)?;
    assert_mdbx_layout(&dir).wrap_err(phase)?;
    // a release from before tndb leaves MDBX consensus files, which CURRENT must migrate; a
    // TN_BIN_PATH_PREV built from a tree after it (the default e2e binary) already runs tndb
    let consensus_on_mdbx = exists(&dir.join("consensus-db").join("epoch").join("mdbx.dat"))?;
    if consensus_on_mdbx {
        assert_mdbx_consensus_layout(&dir).wrap_err(phase)?;
    } else {
        eyre::ensure!(
            exists(&dir.join("consensus-db").join("tndb"))?,
            "{phase}: validator-4's consensus database is neither MDBX nor tndb"
        );
        progress(format!(
            "{phase}: TN_BIN_PATH_PREV already keeps the consensus DB in tndb, so only the reth \
             database migrates"
        ));
    }
    let tip_at_stop = peer_tip(&urls, RESET);
    guard.replace(
        RESET,
        start_validator(RESET, current, base, rpc_ports[RESET], TEST, RUN_UPGRADED),
    );
    tokio::task::block_in_place(|| wait_for_started_rpc(guard, RESET, reset_url, RUN_UPGRADED))
        .wrap_err(phase)?;
    assert_migrated_layout(&dir).wrap_err(phase)?;
    if consensus_on_mdbx {
        let marker = assert_tndb_consensus_layout(&dir).wrap_err(phase)?;
        progress(format!("{phase}: consensus DB marker `{marker}`"));
    }
    let [reth_migrated] = log_lines(RESET, RUN_UPGRADED, MIGRATED_LOG)?.try_into().map_err(
        |lines: Vec<String>| {
            eyre::eyre!("{phase}: expected one `{MIGRATED_LOG}` line, found {lines:?}")
        },
    )?;
    progress(format!("{phase}: {reth_migrated}"));
    // a TN_BIN_PATH_PREV from before the layout marker leaves a datadir without one, which the
    // first open records; one built after it created the datadir with the marker
    let prev_wrote_marker = !log_lines(RESET, RUN_PREV, NEW_DATADIR_LOG)?.is_empty();
    let marker_lines = log_lines(RESET, RUN_UPGRADED, LAYOUT_MARKER_RECORDED_LOG)?;
    if prev_wrote_marker {
        eyre::ensure!(
            marker_lines.is_empty(),
            "{phase}: TN_BIN_PATH_PREV wrote the layout marker, yet the upgrade recorded it again: \
             {marker_lines:?}"
        );
        progress(format!("{phase}: TN_BIN_PATH_PREV already wrote the layout marker"));
    } else {
        let line = layout_log_line(RESET, RUN_UPGRADED, LAYOUT_MARKER_RECORDED_LOG, &["layout=v1"])
            .wrap_err(phase)?;
        progress(format!("{phase}: {line}"));
    }
    // the migrated datadir stays v1 although this binary gives new datadirs v2
    let open_line =
        layout_log_line(RESET, RUN_UPGRADED, KEEPS_LAYOUT_LOG, &["layout=v1", "new_datadirs=v2"])
            .wrap_err(phase)?;
    let same_layout = log_lines(RESET, RUN_UPGRADED, EXISTING_DATADIR_LOG)?;
    eyre::ensure!(
        same_layout.is_empty(),
        "{phase}: the upgraded run opened its datadir in the layout new datadirs get: \
         {same_layout:?}"
    );
    progress(format!("{phase}: {open_line}"));
    tokio::task::block_in_place(|| {
        wait_until_blocking(
            FOLLOW_TIMEOUT,
            &format!("{phase}: validator-4 to pass block {tip_at_stop}"),
            || Ok(get_block_number(reset_url).is_ok_and(|number| number > tip_at_stop)),
        )?;
        wait_until_blocking(
            FOLLOW_TIMEOUT,
            &format!("{phase}: validator-4 to return to CvvActive"),
            || Ok(get_node_mode(reset_url).is_ok_and(|mode| mode == NodeMode::CvvActive)),
        )
    })?;
    // the transactions enter through the upgraded validator's worker
    let receipts = workload.round(reset_url, &chain).await.wrap_err(phase)?;
    record(&mut recorded, &receipts, ring);
    let closed = close_epoch(reference, phase).await?;
    let v2_files = v2_segment_files(&dir)?;
    eyre::ensure!(
        v2_files.is_empty(),
        "{phase}: the migrated v1 datadir wrote v2-only static files: {v2_files:?}"
    );
    progress(format!(
        "{phase}: validator-4 migrated, passed block {tip_at_stop}, is CvvActive; epoch {closed} \
         closed with it"
    ));

    // phase 3: validator-4 stopped, its execution database moved aside
    let phase = "phase 3 (db reset-execution)";
    tokio::task::block_in_place(|| stop_validator(guard, RESET)).wrap_err(phase)?;
    let before = root_entries(&dir)?;
    let tndb = dir.join("consensus-db").join("tndb");
    let tndb_before = root_entries(&tndb)?;
    progress(format!(
        "{phase}: validator-4 datadir root before the reset: {}",
        before.iter().cloned().collect::<Vec<_>>().join(" ")
    ));
    let dry_run = run_reset_execution(current, &dir, false).wrap_err(phase)?;
    eyre::ensure!(
        dry_run.contains("dry run: would move"),
        "{phase}: the dry run printed no plan:\n{dry_run}"
    );
    eyre::ensure!(
        root_entries(&dir)? == before,
        "{phase}: the dry run changed the datadir root: {:?} -> {:?}",
        before,
        root_entries(&dir)?
    );
    progress(format!("{phase}: {}", dry_run.trim()));
    let forced = run_reset_execution(current, &dir, true).wrap_err(phase)?;
    eyre::ensure!(
        forced.contains("moved the execution database"),
        "{phase}: the reset printed no move:\n{forced}"
    );
    progress(format!("{phase}: {}", forced.trim()));
    let after = root_entries(&dir)?;
    let reset_dirs: Vec<&String> =
        after.iter().filter(|name| name.starts_with(RESET_DIR_PREFIX)).collect();
    let [reset_dir] = reset_dirs.as_slice() else {
        eyre::bail!("{phase}: expected one {RESET_DIR_PREFIX}* directory, found {after:?}");
    };
    eyre::ensure!(
        reset_dir.ends_with('/') && !reset_dir.contains(".partial"),
        "{phase}: {reset_dir} is not a finished reset directory"
    );
    let moved_to = dir.join(reset_dir.trim_end_matches('/'));
    // everything else in the root stayed; only the execution entries left it
    let expected: BTreeSet<String> = before
        .iter()
        .filter(|name| !EXECUTION_ENTRIES.contains(&name.trim_end_matches('/')))
        .cloned()
        .chain([(*reset_dir).clone()])
        .collect();
    eyre::ensure!(
        after == expected,
        "{phase}: datadir root after the reset is {after:?}, expected {expected:?}"
    );
    for entry in ["db", "static_files"] {
        eyre::ensure!(!exists(&dir.join(entry))?, "{phase}: {entry} is still in the datadir root");
        eyre::ensure!(
            exists(&moved_to.join(entry))?,
            "{phase}: {entry} is not in {}",
            moved_to.display()
        );
    }
    eyre::ensure!(
        exists(&moved_to.join("db").join("tn-lmdb.dat"))?,
        "{phase}: the moved db is not the migrated tn-lmdb one"
    );
    eyre::ensure!(
        root_entries(&tndb)? == tndb_before,
        "{phase}: the reset changed {}: {tndb_before:?} -> {:?}",
        tndb.display(),
        root_entries(&tndb)?
    );
    progress(format!(
        "{phase}: datadir root after the reset: {}; {} holds {}",
        after.iter().cloned().collect::<Vec<_>>().join(" "),
        moved_to.display(),
        root_entries(&moved_to)?.into_iter().collect::<Vec<_>>().join(" "),
    ));

    // phase 4: validator-4 rebuilds its execution database from genesis
    let phase = "phase 4 (rebuild from genesis)";
    let tip_at_start = peer_tip(&urls, RESET);
    let started = Instant::now();
    guard
        .replace(RESET, start_validator(RESET, current, base, rpc_ports[RESET], TEST, RUN_REBUILT));
    tokio::task::block_in_place(|| wait_for_started_rpc(guard, RESET, reset_url, RUN_REBUILT))
        .wrap_err(phase)?;
    let head_at_rpc = get_block_number(reset_url)?;
    let new_line =
        layout_log_line(RESET, RUN_REBUILT, NEW_DATADIR_LOG, &["layout=v2"]).wrap_err(phase)?;
    for absent in [MIGRATED_LOG, LAYOUT_MARKER_RECORDED_LOG, KEEPS_LAYOUT_LOG] {
        let lines = log_lines(RESET, RUN_REBUILT, absent)?;
        eyre::ensure!(lines.is_empty(), "{phase}: the rebuilt run logged {lines:?}");
    }
    let db = dir.join("db");
    eyre::ensure!(
        exists(&db.join("tn-lmdb.dat"))? && !exists(&db.join("mdbx.dat"))?,
        "{phase}: {} is not a new tn-lmdb database",
        db.display()
    );
    progress(format!("{phase}: {new_line}; head {head_at_rpc} when its RPC came up"));
    tokio::task::block_in_place(|| {
        wait_until_blocking(
            REBUILD_TIMEOUT,
            &format!("{phase}: validator-4 to execute up to block {tip_at_start}"),
            || Ok(get_block_number(reset_url).is_ok_and(|number| number >= tip_at_start)),
        )
    })?;
    let caught_up = started.elapsed();
    tokio::task::block_in_place(|| {
        wait_until_blocking(
            REBUILD_TIMEOUT,
            &format!("{phase}: validator-4 to return to CvvActive"),
            || Ok(get_node_mode(reset_url).is_ok_and(|mode| mode == NodeMode::CvvActive)),
        )
    })?;
    progress(format!(
        "{phase}: validator-4 executed blocks 1..={tip_at_start} within {caught_up:.1?} of \
         starting and is CvvActive after {:.1?}",
        started.elapsed()
    ));
    let receipts = workload.round(reset_url, &chain).await.wrap_err(phase)?;
    record(&mut recorded, &receipts, ring);
    let closed = close_epoch(reference, phase).await?;
    let v2_files = v2_segment_files(&dir)?;
    eyre::ensure!(
        !v2_files.is_empty(),
        "{phase}: the rebuilt datadir has no {V2_SEGMENT_PREFIX}* static files: it is not storage \
         layout v2"
    );
    progress(format!("{phase}: v2 change set static files {v2_files:?}"));
    let endpoints: Vec<NodeEndpoints> = [reference, reset_url]
        .into_iter()
        .map(|url| NodeEndpoints {
            http_url: url.clone(),
            ws_url: String::new(),
            ipc_path: String::new(),
        })
        .collect();
    // every closed epoch's final block, on both nodes, is the block its certified record names
    assert_epoch_records_verify(&endpoints, 0..=closed, EPOCH_RECORD_TIMEOUT_SECS)
        .await
        .wrap_err(phase)?;
    progress(format!(
        "{phase}: epoch {closed} closed with the rebuilt validator; epoch records 0..={closed} \
         verify on validator-1 and validator-4"
    ));

    // phase 5: the rebuilt validator against validator-1 (never moved, still PREV)
    let phase = "phase 5 (compare with validator-1)";
    drop(load);
    let head = get_block_number(reference)?;
    wait_for_head_at_least(reset_url, head, FOLLOW_TIMEOUT.as_secs()).await.wrap_err(phase)?;
    let ref_provider = ProviderBuilder::new().connect_http(reference.parse()?);
    let reset_provider = ProviderBuilder::new().connect_http(reset_url.parse()?);
    let tip = head.min(get_block_number(reset_url)?);
    for number in 0..=tip {
        let expected = block_roots(&ref_provider, "validator-1", number).await?;
        let rebuilt = block_roots(&reset_provider, "validator-4", number).await?;
        eyre::ensure!(
            expected == rebuilt,
            "{phase}: block {number} differs ({}): validator-4 has {rebuilt}, validator-1 has \
             {expected}",
            expected.differing_fields(&rebuilt)
        );
    }
    progress(format!("{phase}: blocks 0..={tip} agree on hash, stateRoot and receiptsRoot"));

    let mut samples: BTreeSet<u64> = [1, tip / 4, tip / 2, 3 * tip / 4, prev_tip, tip].into();
    samples.extend(recorded.iter().map(|(_, block)| *block));
    let mut accounts: Vec<Address> =
        workload.ring.iter().map(TransactionFactory::address).collect();
    accounts.push(CONSENSUS_REGISTRY_ADDRESS);
    let mut registry_by_block = Vec::new();
    let mut balances_by_block = Vec::new();
    for &number in &samples {
        let at = BlockId::number(number);
        let mut balances = Vec::new();
        for &account in &accounts {
            let expected = ref_provider.get_balance(account).block_id(at).await?;
            let rebuilt = reset_provider.get_balance(account).block_id(at).await?;
            eyre::ensure!(
                expected == rebuilt,
                "{phase}: balance of {account} at block {number}: validator-4 {rebuilt}, \
                 validator-1 {expected}"
            );
            balances.push(expected);
        }
        balances_by_block.push(balances);
        let mut registry = Vec::new();
        for slot in 0..REGISTRY_SLOTS {
            let expected = ref_provider
                .get_storage_at(CONSENSUS_REGISTRY_ADDRESS, U256::from(slot))
                .block_id(at)
                .await?;
            let rebuilt = reset_provider
                .get_storage_at(CONSENSUS_REGISTRY_ADDRESS, U256::from(slot))
                .block_id(at)
                .await?;
            eyre::ensure!(
                expected == rebuilt,
                "{phase}: ConsensusRegistry slot {slot} at block {number}: validator-4 {rebuilt}, \
                 validator-1 {expected}"
            );
            registry.push(expected);
        }
        registry_by_block.push(registry);
        let slot = U256::from_be_bytes(ERC1967_IMPLEMENTATION_SLOT.0);
        let expected = ref_provider.get_storage_at(token, slot).block_id(at).await?;
        let rebuilt = reset_provider.get_storage_at(token, slot).block_id(at).await?;
        eyre::ensure!(
            expected == rebuilt,
            "{phase}: token implementation slot at block {number}: validator-4 {rebuilt}, \
             validator-1 {expected}"
        );
        for &holder in &workload.holders {
            let call = TransactionRequest::default().to(token).input(
                Bytes::from(TokenBalance::balanceOfCall { account: holder }.abi_encode()).into(),
            );
            let expected = ref_provider.call(call.clone()).block(at).await?;
            let rebuilt = reset_provider.call(call).block(at).await?;
            eyre::ensure!(
                expected == rebuilt,
                "{phase}: token balanceOf({holder}) at block {number}: validator-4 {rebuilt}, \
                 validator-1 {expected}"
            );
        }
    }
    // the samples span state that changed: otherwise equal reads would prove little
    eyre::ensure!(
        registry_by_block.windows(2).any(|pair| pair[0] != pair[1])
            && registry_by_block.iter().flatten().any(|word| !word.is_zero()),
        "{phase}: ConsensusRegistry slots 0..{REGISTRY_SLOTS} did not change across blocks \
         {samples:?}"
    );
    eyre::ensure!(
        balances_by_block.windows(2).any(|pair| pair[0] != pair[1]),
        "{phase}: no sampled balance changed across blocks {samples:?}"
    );
    progress(format!(
        "{phase}: balances of {} accounts, ConsensusRegistry slots 0..{REGISTRY_SLOTS}, the token's \
         implementation slot and {} token balances agree at blocks {samples:?}",
        accounts.len(),
        workload.holders.len(),
    ));

    for (hash, block) in &recorded {
        for method in ["eth_getTransactionByHash", "eth_getTransactionReceipt"] {
            let expected: Value = ref_provider.raw_request(method.into(), (hash,)).await?;
            let rebuilt: Value = reset_provider.raw_request(method.into(), (hash,)).await?;
            eyre::ensure!(
                !expected.is_null() && expected == rebuilt,
                "{phase}: {method}({hash}) (block {block}): validator-4 {rebuilt}, validator-1 \
                 {expected}"
            );
        }
    }
    progress(format!(
        "{phase}: {} recorded transactions and their receipts agree by hash",
        recorded.len()
    ));

    // validator-1 never left the previous binary
    assert_mdbx_layout(&datadir(base, REFERENCE)).wrap_err(phase)?;
    Ok(())
}

/// A validator of a network started on the previous release is upgraded in place, then its
/// execution database is moved aside with `db reset-execution` and rebuilt from genesis; the
/// rebuilt validator must rejoin the committee and agree with a validator that never moved on
/// every block and on historical reads.
#[ignore = "should not run with a default cargo test, run with `make test-e2e-resync`"]
#[tokio::test(flavor = "multi_thread")]
async fn test_resync_execution_from_genesis() -> eyre::Result<()> {
    if std::env::var_os(TN_LMDB_BIN_MARKER).filter(|value| !value.is_empty()).is_none() {
        const SKIPPED: &str = "SKIPPING test_resync_execution_from_genesis: TN_E2E_TN_LMDB_BIN is \
                               unset, so TN_BIN_PATH is not declared the tn-lmdb e2e binary. Run \
                               `make test-e2e-resync`.";
        warn!(target: "resync-test", "{SKIPPED}");
        eprintln!("{SKIPPED}");
        return Ok(());
    }
    // the marker declares this the resync lane, so a missing binary is a misconfigured lane, not a
    // skip. TN_BIN_PATH is checked before the harness resolves it: unset, it builds a binary
    // through cargo, under features this lane did not choose
    let previous = e2e_tests::get_previous_telcoin_network_binary().ok_or_else(|| {
        eyre::eyre!(
            "{TN_LMDB_BIN_MARKER} is set but TN_BIN_PATH_PREV is unset: the test needs the \
             reference validator's binary there (`make test-e2e-resync` passes it)"
        )
    })?;
    eyre::ensure!(
        std::env::var_os("TN_BIN_PATH").is_some_and(|value| !value.is_empty()),
        "{TN_LMDB_BIN_MARKER} is set but TN_BIN_PATH is unset: the test needs the tn-lmdb e2e \
         binary there (`make test-e2e-resync` passes it)"
    );
    let _permit = acquire_test_permit();
    let current = e2e_tests::get_telcoin_network_binary();
    let temp_dir = tempfile::TempDir::with_prefix(TEST)?;
    let mut guard = ProcessGuard::empty();
    let started = Instant::now();
    match run_resync(&mut guard, temp_dir.path(), previous, current).await {
        Ok(()) => {
            guard.finish();
            progress(format!("passed in {:.0?}", started.elapsed()));
            Ok(())
        }
        Err(error) => {
            // stop the nodes first, so the retained datadirs are the ones they closed
            tokio::task::block_in_place(|| guard.kill_all());
            let retained = temp_dir.keep();
            Err(error.wrap_err(format!(
                "resync test failed after {:.0?}; retained datadirs: {}; node logs: \
                 test_logs/{TEST}/",
                started.elapsed(),
                retained.display()
            )))
        }
    }
}
