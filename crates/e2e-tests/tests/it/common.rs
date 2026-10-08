//! Shared utilities for e2e integration tests.
//!
//! Process management, cleanup guards, and helpers used across all test modules.

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall as _,
};
use clap::Parser as _;
use e2e_tests::{create_validator_info, setup_log_dir, NodeEndpoints, TestBinary};
use ethereum_tx_sign::{LegacyTransaction, Transaction};
use eyre::Report;
use jsonrpsee::{
    core::{client::ClientT as _, DeserializeOwned},
    http_client::HttpClientBuilder,
    rpc_params,
};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use serde_json::Value;
use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, HashMap},
    convert::Infallible,
    fmt::Debug,
    io::{Read, Write},
    net::TcpStream,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    process::{Child, ExitStatus},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use telcoin_network_cli::genesis::GenesisArgs;
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_test_utils::{wait_until, wait_until_blocking};
use tn_types::{
    address,
    forks::{
        leader_seeded_ordering_fork_epoch_override, multi_workers_fork_active,
        seed_signature_active, subsecond_timestamp_fork_epoch_override,
    },
    get_available_tcp_port, keccak256,
    test_utils::{init_test_tracing, CommandParser},
    Address, Epoch, EpochCertificate, EpochRecord, Genesis, GenesisAccount, NodeMode, RpcInfo,
    DEFAULT_WORKER_ID, U256,
};
use tokio::{
    runtime::Builder,
    time::{timeout, Instant},
};
use tracing::{error, info, warn};

/// Max number of e2e tests that can run concurrently.
/// Each test spawns 4-6 node processes; limiting concurrency prevents resource exhaustion.
const MAX_CONCURRENT_TESTS: usize = 2;

static TEST_SEMAPHORE: TestSemaphore = TestSemaphore::new(MAX_CONCURRENT_TESTS);

/// One unit of TEL (10^18) measured in wei.
pub(crate) const WEI_PER_TEL: u128 = 1_000_000_000_000_000_000;

/// Acquire a permit to run an e2e test. Blocks until a slot is available.
/// The returned guard releases the permit on drop. Also ensure test tracing.
pub(crate) fn acquire_test_permit() -> TestSemaphoreGuard<'static> {
    init_test_tracing();
    TEST_SEMAPHORE.acquire()
}

/// Counting semaphore for limiting concurrent test execution.
struct TestSemaphore {
    state: Mutex<usize>,
    cv: Condvar,
    max: usize,
}

impl TestSemaphore {
    const fn new(max: usize) -> Self {
        Self { state: Mutex::new(0), cv: Condvar::new(), max }
    }

    fn acquire(&self) -> TestSemaphoreGuard<'_> {
        let mut count = self.state.lock().unwrap();
        while *count >= self.max {
            count = self.cv.wait(count).unwrap();
        }
        *count += 1;
        TestSemaphoreGuard { sem: self }
    }
}

pub(crate) struct TestSemaphoreGuard<'a> {
    sem: &'a TestSemaphore,
}

impl Drop for TestSemaphoreGuard<'_> {
    fn drop(&mut self) {
        let mut count = self.sem.state.lock().unwrap();
        *count -= 1;
        self.sem.cv.notify_one();
    }
}

/// RAII guard that kills child processes on drop (including during panic unwinding).
///
/// Avoids global `panic::set_hook` which causes cross-test contamination in parallel runs.
/// Sends SIGTERM to all children first (parallel graceful shutdown), then waits for each.
pub(crate) struct ProcessGuard {
    /// Owned child processes that exit on `drop`.
    children: Vec<Option<Child>>,
}

impl ProcessGuard {
    /// Create a guard wrapping existing children.
    pub(crate) fn new(children: Vec<Child>) -> Self {
        Self { children: children.into_iter().map(Some).collect() }
    }

    /// Create an empty guard.
    pub(crate) fn empty() -> Self {
        Self { children: Vec::new() }
    }

    /// Add a child to the guard. Returns the index.
    pub(crate) fn push(&mut self, child: Child) -> usize {
        let idx = self.children.len();
        self.children.push(Some(child));
        idx
    }

    /// Remove and return the child at `idx`.
    /// The caller takes responsibility for killing it — the guard will no longer track it.
    pub(crate) fn take(&mut self, idx: usize) -> Option<Child> {
        self.children.get_mut(idx).and_then(|slot| slot.take())
    }

    /// Replace the child at `idx` with a new one, returning the old child (if any).
    pub(crate) fn replace(&mut self, idx: usize, child: Child) -> Option<Child> {
        if idx >= self.children.len() {
            self.children.resize_with(idx + 1, || None);
        }
        self.children[idx].replace(child)
    }

    /// Get a mutable reference to the child at `idx`, if present.
    pub(crate) fn get_mut(&mut self, idx: usize) -> Option<&mut Child> {
        self.children.get_mut(idx).and_then(|slot| slot.as_mut())
    }

    /// Send SIGTERM to all living children without waiting.
    pub(crate) fn send_term_all(&self) {
        for child in self.children.iter().flatten() {
            send_term_by_id(child.id());
        }
    }

    /// Send SIGTERM to all, wait for each to exit (SIGKILL if needed), then clear all slots.
    /// Safe to call multiple times.
    ///
    /// The exit wait polls EVERY child against one shared 6s deadline (the same shape as
    /// [`Self::wait_for_natural_exits`]) instead of giving each child its own [`wait_or_kill`]
    /// window: a 10ms poll reaps the common case (all children already dying from the parallel
    /// SIGTERM) as soon as the last one exits, instead of rounding each child up to its next
    /// 1.2s poll slot in sequence.
    pub(crate) fn kill_all(&mut self) {
        // Phase 1: SIGTERM all in parallel for fast graceful shutdown
        self.send_term_all();

        // Phase 2: one shared deadline for every child to exit, polled at 10ms.
        let deadline = std::time::Instant::now() + Duration::from_secs(6);
        let all_exited =
            std::iter::repeat(()).take_while(|()| std::time::Instant::now() < deadline).any(|()| {
                std::thread::sleep(Duration::from_millis(10));
                self.children
                    .iter_mut()
                    .flatten()
                    .all(|child| child.try_wait().ok().flatten().is_some())
            });

        // Phase 3: escalate whatever is still running, then clear every slot. `try_wait` on an
        // already-reaped child returns its stored status, so exited children are never signaled.
        if !all_exited {
            self.children.iter_mut().flatten().for_each(|child| {
                if child.try_wait().ok().flatten().is_none() {
                    force_kill_and_reap(child);
                }
            });
        }
        self.children.iter_mut().for_each(|slot| *slot = None);
    }

    /// Wait (bounded) for every child at `indices` to exit on its own, without signaling any of
    /// them. All not-yet-exited children are polled together each round against ONE shared
    /// `timeout`, so the whole wait is bounded by `timeout` — not `timeout * indices.len()`, as a
    /// sequence of per-child waits would be.
    ///
    /// On success every named child has been reaped, so its slot is cleared: `kill_all`/`Drop`
    /// signal raw pids, and the OS may reuse a reaped child's pid, so the guard must never signal
    /// it again. The returned `(index, status)` pairs are ordered by index. On timeout the
    /// still-running children stay guarded so `Drop` still cleans them up, and a named-timeout
    /// error (naming the pending children) is returned.
    pub(crate) fn wait_for_natural_exits(
        &mut self,
        indices: impl IntoIterator<Item = usize>,
        timeout: Duration,
    ) -> eyre::Result<Vec<(usize, ExitStatus)>> {
        let want: Vec<usize> = indices.into_iter().collect();
        for &idx in &want {
            if self.children.get(idx).and_then(|slot| slot.as_ref()).is_none() {
                return Err(eyre::eyre!("no child process at index {idx}"));
            }
        }

        // `wait_until_blocking` takes an `Fn` closure, but `try_wait` needs `&mut Child`, so thread
        // the children and the collected exit statuses through `RefCell`s (the poll loop is
        // single-threaded). Polling every not-yet-exited child on each round lets one shared
        // deadline cover them all.
        let children = RefCell::new(&mut self.children);
        let exits: RefCell<HashMap<usize, ExitStatus>> = RefCell::new(HashMap::new());
        let description = format!("children {want:?} to exit on their own");
        wait_until_blocking(timeout, &description, || {
            let mut children = children.borrow_mut();
            let mut exits = exits.borrow_mut();
            for &idx in &want {
                if exits.contains_key(&idx) {
                    continue;
                }
                if let Some(child) = children[idx].as_mut() {
                    if let Some(status) = child.try_wait()? {
                        exits.insert(idx, status);
                    }
                }
            }
            Ok(exits.len() == want.len())
        })?;

        // Every requested child is reaped; clear its slot so `kill_all`/`Drop` never signal a
        // possibly-reused pid. The `children` borrow of `self.children` has already ended here —
        // its last use was inside the poll closure above — so the Vec can be mutated directly.
        let exits = exits.into_inner();
        for &idx in exits.keys() {
            self.children[idx] = None;
        }
        let mut statuses: Vec<(usize, ExitStatus)> = exits.into_iter().collect();
        statuses.sort_by_key(|&(idx, _)| idx);
        Ok(statuses)
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        self.kill_all();
    }
}

/// Send SIGTERM to a process by PID.
fn send_term_by_id(pid: u32) {
    if let Err(e) = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
        error!(target: "e2e-test", ?e, pid, "error sending SIGTERM");
    }
}

/// Send SIGTERM to a child process.
pub(crate) fn send_term(child: &mut Child) {
    send_term_by_id(child.id());
}

/// Gracefully shut down a child process: SIGTERM -> poll up to 6s -> SIGKILL -> wait.
pub(crate) fn kill_child(child: &mut Child) {
    send_term(child);
    wait_or_kill(child);
}

/// Poll for exit up to 5 times (1.2s each), then SIGKILL + wait.
/// Assumes SIGTERM has already been sent.
fn wait_or_kill(child: &mut Child) {
    for _ in 0..5 {
        match child.try_wait() {
            Ok(Some(_)) => {
                info!(target: "e2e-test", "child exited");
                return;
            }
            Ok(None) => {}
            Err(e) => error!(target: "e2e-test", "error waiting on child to exit: {e}"),
        }
        std::thread::sleep(Duration::from_millis(1200));
    }
    force_kill_and_reap(child);
}

/// SIGKILL a child that did not exit within its SIGTERM grace, then reap it.
///
/// The shared tail of every kill path ([`wait_or_kill`], [`ProcessGuard::kill_all`], and
/// `basefee.rs`'s boundary-kill helper). Failures are logged, not propagated: teardown has no
/// recovery path, and signaling a child that already exited is harmless (`kill` on a reaped
/// child returns `InvalidInput` without touching any pid).
pub(crate) fn force_kill_and_reap(child: &mut Child) {
    child.kill().unwrap_or_else(|e| error!(target: "e2e-test", ?e, "error sending SIGKILL"));
    child.wait().map(drop).unwrap_or_else(
        |e| error!(target: "e2e-test", ?e, "error waiting for child after SIGKILL"),
    );
}

/// Get the block for block_number or latest block if None for node.
pub(crate) fn get_block(
    node: &str,
    block_number: Option<u64>,
) -> eyre::Result<HashMap<String, Value>> {
    let debug_params = if let Some(block_number) = block_number {
        format!("0x{block_number:x}")
    } else {
        "latest".to_string()
    };

    let params = rpc_params!(&debug_params, true);
    // Deserialize as Option to handle null responses from syncing/restarted nodes
    // that haven't caught up to the requested block yet.
    let mut result: Option<HashMap<String, Value>> =
        call_rpc(node, "eth_getBlockByNumber", params.clone(), 10, &debug_params)?;
    let mut retries = 0;
    while result.is_none() && retries < 30 {
        std::thread::sleep(Duration::from_secs(1));
        result = call_rpc(node, "eth_getBlockByNumber", params.clone(), 3, &debug_params)?;
        retries += 1;
    }
    result.ok_or_else(|| {
        eyre::eyre!("eth_getBlockByNumber returned null after retries for {debug_params} on {node}")
    })
}

/// Inner async core for call_rpc.
/// It can be called with or without tokio already running.
async fn call_rpc_inner<R, Params, DebugParams>(
    node: &str,
    command: &str,
    params: Params,
    retries: usize,
    debug_params: DebugParams,
) -> eyre::Result<R>
where
    R: DeserializeOwned + Debug,
    Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone + Debug,
    DebugParams: Debug,
{
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(10))
        .build(node)
        .expect("couldn't build rpc client");
    let mut resp = client.request(command, params.clone()).await;
    let mut i = 0;
    while i < retries && resp.is_err() {
        // Short backoff: these retries mask brief RPC unavailability (e.g. a node
        // mid-restart), so poll ~4x/sec instead of once a second.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let client = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(10))
            .build(node)
            .expect("couldn't build rpc client");
        resp = client.request(command, params.clone()).await;
        i += 1;
    }
    Ok(resp.inspect_err(|error| {
        error!(target: "restart-tests", ?error, ?command, ?node, ?debug_params, "rpc call failed");
    })?)
}

/// Make an RPC call to node with command and params.
/// Wraps any Eyre otherwise returns the result as a String.
/// This is for testing and will try up to retries times at one second intervals to send the
/// request.
pub(crate) fn call_rpc<R, Params, DebugParams>(
    node: &str,
    command: &str,
    params: Params,
    retries: usize,
    debug_params: DebugParams,
) -> eyre::Result<R>
where
    R: DeserializeOwned + Debug,
    Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone + Debug,
    DebugParams: Debug,
{
    // jsonrpsee is async AND tokio specific so give it a runtime if needed (and can't use a crate
    // like pollster)...
    let resp = match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(move || {
            handle.block_on(call_rpc_inner(node, command, params, retries, debug_params))
        }),
        Err(_) => Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?
            .block_on(call_rpc_inner(node, command, params, retries, debug_params)),
    };
    resp
}

/// Check if the network is advancing (query all nodes).
pub(crate) fn network_advancing(client_urls: &[String; 4]) -> eyre::Result<()> {
    // Wait for all nodes to respond to RPC.
    // With skip-empty-execution, blocks are only produced when transactions
    // exist or an epoch closes, so we cannot rely on block_number advancing
    // during idle periods. Actual block production is verified later by
    // send_and_confirm().
    wait_until_blocking(Duration::from_secs(45), "all nodes advancing", || {
        Ok(client_urls.iter().all(|url| get_block_number(url).is_ok()))
    })
}

/// Start a process running a validator node.
pub(crate) fn start_validator(
    instance: usize,
    bin: &'static TestBinary,
    base_dir: &Path,
    rpc_port: u16,
    test: &str,
    run: u32,
) -> Child {
    start_validator_with_args(instance, bin, base_dir, rpc_port, test, run, &[])
}

/// Start a validator node process with additional CLI arguments (e.g. `--metrics`).
pub(crate) fn start_validator_with_args(
    instance: usize,
    bin: &'static TestBinary,
    base_dir: &Path,
    rpc_port: u16,
    test: &str,
    run: u32,
    extra_args: &[&str],
) -> Child {
    start_validator_with_env(instance, bin, base_dir, rpc_port, test, run, extra_args, &[])
}

/// Start a validator node process with additional CLI arguments and extra environment
/// variables for that child only.
///
/// Each `(key, value)` pair is set on the spawned command after the variables
/// [`TestBinary::command`] forwards, so a pair overrides a forwarded variable of the same
/// name. The harness process itself is never touched: a per-node setting such as
/// `TN_TEST_CLOCK_OFFSET_MS` must not be exported with `std::env::set_var`, or every node
/// spawned afterwards would inherit it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_validator_with_env(
    instance: usize,
    bin: &'static TestBinary,
    base_dir: &Path,
    rpc_port: u16,
    test: &str,
    run: u32,
    extra_args: &[&str],
    extra_env: &[(&str, &str)],
) -> Child {
    let data_dir = base_dir.join(format!("validator-{}", instance + 1));
    let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
    // IPC: use temp-dir-based path to avoid cross-test conflicts
    let ipc_path = base_dir.join(format!("validator-{}.ipc", instance + 1));
    let mut command = bin.command();

    command
        .env("TN_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        .arg("--ws")
        .arg("--ws.port")
        .arg(format!("{ws_port}"))
        .arg("--ipcpath")
        .arg(ipc_path.to_string_lossy().as_ref())
        .arg("--node-name")
        .arg(format!("{test}-node{instance}"));

    command.args(extra_args);
    command.envs(extra_env.iter().copied());

    setup_log_dir(&mut command, instance, test, run);

    command.spawn().expect("failed to execute")
}

/// The log file [`setup_log_dir`] gives run `run` of node `instance` under `test_logs/<test>/`:
/// `node<instance>-run<run>.log` for stdout, `node<instance>-run<run>.stderr.log` for stderr.
///
/// The directory is read from `CARGO_MANIFEST_DIR` at run time, as [`setup_log_dir`] reads it,
/// and falls back to the crate's build-time manifest directory only where that variable is unset,
/// in which case [`setup_log_dir`] would have panicked before writing any log.
pub(crate) fn node_log_path(test: &str, instance: usize, run: u32, stderr: bool) -> PathBuf {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")), PathBuf::from);
    let suffix = if stderr { ".stderr" } else { "" };
    manifest_dir.join("test_logs").join(test).join(format!("node{instance}-run{run}{suffix}.log"))
}

/// `text` without its ANSI escape sequences (`ESC [ parameters final-byte`), so node log lines
/// read as plain `name=value` fields.
pub(crate) fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            plain.push(c);
            continue;
        }
        if chars.next() == Some('[') {
            // parameter and intermediate bytes, up to and including the final byte
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        }
    }
    plain
}

/// Advertise a validator's JSON-RPC endpoint on its worker record.
///
/// The genesis ceremony leaves `p2p_info.workers[0].rpc` unset, and a non-committee node
/// forwards accepted transactions to whatever endpoints committee validators advertise
/// (issue #804); with none advertised, each seal is refused with
/// `BlockSealError::NotValidator` and the transactions stay pending in the node's own
/// pool, retried roughly once per `max_batch_delay` until an endpoint is discoverable.
/// Call this between the config ceremony and `start_validator`, passing the same
/// `rpc_port` the validator will serve `--http` on; the node re-signs the record from
/// its `node-info.yaml` at startup, so editing the file is sufficient.
pub(crate) fn advertise_worker_rpc(
    base_dir: &Path,
    instance: usize,
    rpc_port: u16,
) -> eyre::Result<()> {
    let path = base_dir.join(format!("validator-{}", instance + 1)).join("node-info.yaml");
    let mut node_info = Config::load_from_path::<NodeInfo>(&path, ConfigFmt::YAML)?;
    let rpc = Some(RpcInfo { http: format!("http://127.0.0.1:{rpc_port}").parse()?, ws: None });
    node_info
        .p2p_info
        .worker_mut(DEFAULT_WORKER_ID)
        .map(|worker| worker.rpc = rpc)
        .ok_or_else(|| eyre::eyre!("validator-{} node info has no worker 0", instance + 1))?;
    Config::write_to_path(&path, &node_info, ConfigFmt::YAML)?;
    Ok(())
}

/// Start a process running an observer node.
pub(crate) fn start_observer(
    instance: usize,
    bin: &'static TestBinary,
    base_dir: &Path,
    rpc_port: u16,
    test: &str,
    run: u32,
) -> Child {
    let data_dir = base_dir.join("observer");
    let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
    // IPC: use temp-dir-based path to avoid cross-test conflicts
    let ipc_path = base_dir.join("observer.ipc");
    let mut command = bin.command();
    command
        .env("TN_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        .arg("--ws")
        .arg("--ws.port")
        .arg(format!("{ws_port}"))
        .arg("--ipcpath")
        .arg(ipc_path.to_string_lossy().as_ref())
        .arg("--node-name")
        .arg(format!("{test}-node{instance}"));

    setup_log_dir(&mut command, instance, test, run);

    command.spawn().expect("failed to execute")
}

/// Retrieve "latest" execution block and parse the number (block height).
pub(crate) fn get_block_number(node: &str) -> eyre::Result<u64> {
    let block = get_block(node, None)?;
    Ok(u64::from_str_radix(&block["number"].as_str().unwrap_or("0x100_000")[2..], 16)?)
}

/// If key starts with 0x then return it otherwise generate the key from the key string.
pub(crate) fn get_key(key: &str) -> String {
    if key.starts_with("0x") {
        key.to_string()
    } else {
        let (_, _, key) = account_from_word(key);
        key
    }
}

/// Return the (account, public key, secret key) generated from key_word.
fn account_from_word(key_word: &str) -> (String, String, String) {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand);
    let keypair = Keypair::from_secret_key(&secp, &secret_key);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    let address = Address::from_slice(&hash[12..]);
    let pubkey = keypair.public_key().serialize();
    let secret = keypair.secret_bytes();
    (address.to_string(), const_hex::encode(pubkey), const_hex::encode(secret))
}

/// Retrieve a node's latest consensus header.
pub(crate) fn get_latest_consensus_header(node: &str) -> eyre::Result<HashMap<String, Value>> {
    call_rpc(node, "tn_latestConsensusHeader", rpc_params![], 10, "tn_latestConsensusHeader")
}

/// Retrieve a node's identifying information.
pub(crate) fn get_node_info(node: &str) -> eyre::Result<HashMap<String, Value>> {
    call_rpc(node, "tn_info", rpc_params![], 10, "tn_info")
}

/// Retrieve a node's current consensus participation mode ([`NodeMode`]) over RPC.
///
/// Reads the live mode via `tn_nodeMode`. A node whose RPC is not yet up returns an error
/// immediately, leaving retry timing to the caller's bounded wait. A current-mode query cannot
/// establish whether a transient mode occurred between calls.
pub(crate) fn get_node_mode(node: &str) -> eyre::Result<NodeMode> {
    call_rpc(node, "tn_nodeMode", rpc_params![], 0, "tn_nodeMode")
}

/// Scrape the metrics endpoint with a raw HTTP GET (no client dependencies).
pub(crate) fn scrape_metrics(addr: &str) -> eyre::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

/// Query a node's highest consensus chain block height.
/// NOTE: consensus chain is required to grow to detect byzantine validators.
pub(crate) fn get_latest_consensus_header_number(node: &str) -> eyre::Result<u64> {
    let header = get_latest_consensus_header(node)?;
    let value = header
        .get("number")
        .ok_or_else(|| Report::msg("tn_latestConsensusHeader missing `number` field"))?;

    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| Report::msg("tn_latestConsensusHeader number is not u64-compatible")),
        Value::String(s) if s.starts_with("0x") => {
            u64::from_str_radix(s.trim_start_matches("0x"), 16)
                .map_err(|e| Report::msg(format!("failed to parse consensus number hex: {e}")))
        }
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|e| Report::msg(format!("failed to parse consensus number: {e}"))),
        _ => Err(Report::msg("tn_latestConsensusHeader number has unexpected type")),
    }
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
pub(crate) fn address_from_word(key_word: &str) -> Address {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (_, public_key) = secp.generate_keypair(&mut rand);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    Address::from_slice(&hash[12..])
}

/// Send native tokens and confirm the account balance changed.
pub(crate) fn send_and_confirm(
    node: &str,
    node_test: &str,
    key: &str,
    to_account: Address,
    nonce: u128,
) -> eyre::Result<()> {
    let basefee_address = address!("0x9999999999999999999999999999999999999999");
    let current = get_balance(node_test, &to_account.to_string(), 1)?;
    let current_basefee = get_balance(node_test, &basefee_address.to_string(), 1)?;
    let amount = 10 * WEI_PER_TEL; // 10 TEL
    let expected = current + amount;
    send_tel(node, key, to_account, amount, 250, 21000, nonce)?;

    info!(target: "restart-test", "calling get_positive_balance_with_retry...");

    // get positive bal and kill child2 if error
    let bal = get_balance_above_with_retry(node_test, &to_account.to_string(), expected - 1)?;

    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }
    let bal =
        get_balance_above_with_retry(node_test, &basefee_address.to_string(), current_basefee)?;
    let expected_bal =
        current_basefee.checked_div(nonce).map_or(0, |per_tx| current_basefee + per_tx);
    if nonce > 0 && bal < expected_bal {
        error!(target: "restart-test", ?bal, ?expected_bal, "basefee error!");
        return Err(Report::msg("Expected a basefee increment!".to_string()));
    }
    Ok(())
}

/// Send an RPC call to node to get the latest balance for address.
/// Return a tuple of the TEL and remainder (any value left after dividing by 1_e18).
/// Note, balance is in wei and must fit in an u128.
pub(crate) fn get_balance(node: &str, address: &str, retries: usize) -> eyre::Result<u128> {
    let res_str: String =
        call_rpc(node, "eth_getBalance", rpc_params!(address, "latest"), retries, address)?;
    info!(target: "restart-test", "get_balance for {node}: parsing string {res_str}");
    let tel = u128::from_str_radix(&res_str[2..], 16)?;
    info!(target: "restart-test", "get_balance for {node}: {tel:?}");
    Ok(tel)
}

/// Retry up to 10 times to retrieve an account balance > 0.
pub(crate) fn get_positive_balance_with_retry(node: &str, address: &str) -> eyre::Result<u128> {
    get_balance_above_with_retry(node, address, 0)
}

/// Retry up to 45 times to retrieve an account balance > above.
pub(crate) fn get_balance_above_with_retry(
    node: &str,
    address: &str,
    above: u128,
) -> eyre::Result<u128> {
    let mut bal = get_balance(node, address, 5)?;
    let mut i = 0;
    while i < 45 && bal <= above {
        std::thread::sleep(Duration::from_millis(1200));
        i += 1;
        bal = get_balance(node, address, 5)?;
    }
    if i == 45 && bal <= above {
        error!(target:"restart-test", "get_balance_above_with_retry i == 30 - returning error!!");
        Err(Report::msg(format!("Failed to get a balance {bal} for {address} above {above}")))
    } else {
        Ok(bal)
    }
}

/// Create, sign and submit a TXN to transfer TEL from key's account to to_account.
/// Returns the submitted transaction's hash as reported by `eth_sendRawTransaction`, so callers
/// can attribute the tx to its exact block via the receipt.
pub(crate) fn send_tel(
    node: &str,
    key: &str,
    to_account: Address,
    amount: u128,
    gas_price: u128,
    gas: u128,
    nonce: u128,
) -> eyre::Result<String> {
    let mut to_addr = [0_u8; 20];
    //const_hex::decode_to_slice(to_account, &mut to_addr[..])?;
    to_addr.copy_from_slice(to_account.as_slice());
    let (from_account, _, _) = decode_key(key)?;
    let new_transaction = LegacyTransaction {
        chain: 0xde7e1,
        nonce,
        to: Some(to_addr),
        value: amount,
        gas_price,
        gas,
        data: vec![/* contract code or other data */],
    };
    let decoded = const_hex::decode(key)?;
    let secret_key = SecretKey::from_byte_array(decoded.as_slice().try_into()?)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let transaction_bytes = new_transaction.sign(&ecdsa);
    let res_str: String = call_rpc(
        node,
        "eth_sendRawTransaction",
        rpc_params!(const_hex::encode(&transaction_bytes)),
        1,
        transaction_bytes,
    )?;
    info!(target: "restart-test", "Submitted TEL transfer from {from_account} to {to_account} for {amount}: {res_str}");
    Ok(res_str)
}

// ---------------------------------------------------------------------------------------------
// Epoch-test scaffolding shared by epochs.rs, basefee.rs, and eject.rs
// ---------------------------------------------------------------------------------------------

/// Name of the extra (non-genesis-committee) validator node used by epoch and ejection tests.
pub(crate) const NEW_VALIDATOR: &str = "new-validator";
/// BLS passphrase shared by all nodes started via [`start_nodes`].
pub(crate) const NODE_PASSWORD: &str = "sup3rsecuur";
/// Initial stake per validator written into genesis and used by `stake` transactions.
pub(crate) const INITIAL_STAKE_AMOUNT: &str = "1_000_000";
/// Epoch duration (seconds) for epoch-boundary style tests.
///
/// Epoch init creates HDX index files per epoch (open_epoch_pack → new_epoch →
/// ConsensusPack::open_append). With test-utils, these are ~1.3MB each (vs ~130MB in prod).
/// 10s provides margin for parallel test execution and CI load variance.
pub(crate) const EPOCH_DURATION: u64 = 10;

/// Create genesis for epoch/ejection tests.
///
/// Funds `extra_node` (a validator that joins after genesis) and the governance wallet to issue
/// NFTs. This method also configures the initial committee to start the network.
pub(crate) fn create_genesis_for_test(
    temp_path: &Path,
    extra_node: (&str, Address),
    governance_wallet: Address,
    committee: &Vec<(&str, Address)>,
    epoch_duration: u64,
) -> eyre::Result<Genesis> {
    let (extra_name, extra_address) = extra_node;
    // use same passphrase for all nodes
    let passphrase = Some(NODE_PASSWORD.to_string());

    // create validator info for the extra validator to join later
    let extra_node_path = temp_path.join(extra_name);
    create_validator_info(&extra_node_path, &extra_address.to_string(), passphrase.clone())?;

    // fund governance to issue NFT and the extra validator to stake
    let accounts = vec![
        (
            governance_wallet,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)), /* 50mil TEL */
        ),
        (
            extra_address,
            GenesisAccount::default().with_balance(U256::from(parse_ether("2_000_000")?)), /* double stake */
        ),
    ];

    let shared_genesis_dir = temp_path.join("shared-genesis");

    // create the initial committee of validators and create genesis
    let genesis = config_committee(
        temp_path,
        &shared_genesis_dir,
        GenesisConfig {
            passphrase,
            consensus_registry_owner: governance_wallet,
            accounts,
            validators: committee,
            epoch_duration,
            chain_id: None,
        },
    )?;

    // copy genesis for the extra validator
    std::fs::create_dir_all(extra_node_path.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        extra_node_path.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        extra_node_path.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("parameters.yaml"),
        extra_node_path.join("parameters.yaml"),
    )?;

    Ok(genesis)
}

/// Genesis inputs for [`config_committee`].
pub(crate) struct GenesisConfig<'a> {
    /// Passphrase for the validators' keys.
    pub(crate) passphrase: Option<String>,
    /// Owner of the `ConsensusRegistry`.
    pub(crate) consensus_registry_owner: Address,
    /// Accounts funded in genesis.
    pub(crate) accounts: Vec<(Address, GenesisAccount)>,
    /// The initial committee: node name and execution address.
    pub(crate) validators: &'a [(&'a str, Address)],
    /// Epoch duration in seconds.
    pub(crate) epoch_duration: u64,
    /// Overrides the genesis ceremony's default chain id (see [`config_committee`]).
    pub(crate) chain_id: Option<u64>,
}

/// Configure the initial committee and fund accounts for network genesis.
///
/// All data is written to file.
///
/// `chain_id` overrides the genesis ceremony's default chain id (`911329`). Only the
/// governance-Safe fork lane needs it: an `adiri` binary refuses to boot any chain whose id is
/// not `2017` (`telcoin-network-cli::node`), and the default binary refuses one whose id IS
/// `2017`, so the two e2e binaries need different ids and neither can be left implicit on the
/// adiri lane. Pass `None` everywhere else to keep the ceremony default.
pub(crate) fn config_committee(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    config: GenesisConfig<'_>,
) -> eyre::Result<Genesis> {
    let GenesisConfig {
        passphrase,
        consensus_registry_owner,
        accounts,
        validators,
        epoch_duration,
        chain_id,
    } = config;
    // create shared genesis dir
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;
    // create validator info and copy to shared genesis dir
    for (v, addr) in validators.iter() {
        let dir = temp_path.join(v);
        // init genesis ceremony to create committee files
        create_validator_info(&dir, &addr.to_string(), passphrase.clone())?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // configuration for ConesnsusRegistry to pass through CLI
    let min_withdrawal = "1_000";
    let epoch_rewards = "1000";

    info!(target: "epoch-test", "creating committee!");

    // create committee from shared genesis dir
    let mut genesis_args: Vec<String> = vec![
        "tn".into(),
        "--basefee-address".into(),
        "0x9999999999999999999999999999999999999999".into(),
        "--consensus-registry-owner".into(),
        consensus_registry_owner.to_string(),
        "--initial-stake-per-validator".into(),
        INITIAL_STAKE_AMOUNT.into(),
        "--min-withdraw-amount".into(),
        min_withdrawal.into(),
        "--epoch-block-rewards".into(),
        epoch_rewards.into(),
        "--epoch-duration-in-secs".into(),
        epoch_duration.to_string(),
        "--dev-funded-account".into(),
        "test-source".into(),
        "--max-header-delay-ms".into(),
        "500".into(),
        "--min-header-delay-ms".into(),
        "250".into(),
        "--max-batch-delay-ms".into(),
        "250".into(),
    ];
    if let Some(chain_id) = chain_id {
        genesis_args.push("--chain-id".into());
        genesis_args.push(chain_id.to_string());
    }
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from(genesis_args);
    create_committee_command.args.execute(shared_genesis_dir.to_path_buf())?;

    // update genesis with funded accounts
    let data_dir = shared_genesis_dir.join("genesis/genesis.yaml");
    let genesis: Genesis = Config::load_from_path(&data_dir, ConfigFmt::YAML)?;
    let genesis = genesis.extend_accounts(accounts);
    Config::write_to_path(&data_dir, &genesis, ConfigFmt::YAML)?;

    // distribute updated genesis to all validators
    for (v, _addr) in validators.iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(dir.join("genesis"))?;
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    Ok(genesis)
}

/// Start the network using the node cli command.
pub(crate) fn start_nodes(
    temp_path: &Path,
    validators: &[(&str, Address)],
    test: &str,
    run: u32,
) -> eyre::Result<(Vec<Child>, Vec<NodeEndpoints>)> {
    let bin = e2e_tests::get_telcoin_network_binary();

    let mut children = Vec::new();
    let mut endpoints = Vec::new();
    for (v, _) in validators.iter() {
        let dir = temp_path.join(v);

        if *v == NEW_VALIDATOR {
            info!(target: "epoch-test", ?v, "starting new validator");
        }

        // Get dynamic ports for RPC - OS assigns ports, no instance compensation needed
        let rpc_port = get_available_tcp_port("127.0.0.1").expect("available tcp port");
        let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
        // Multi-worker RPC derivation requires the WebSocket base to be at least the HTTP base.
        let (rpc_port, ws_port) = (rpc_port.min(ws_port), rpc_port.max(ws_port));

        // IPC - unique path under temp dir to avoid cross-test conflicts
        let ipc_path = temp_path.join(format!("{v}.ipc"));

        let mut command = bin.command();
        command
            .env("TN_BLS_PASSPHRASE", NODE_PASSWORD)
            .arg("--bls-passphrase-source")
            .arg("env")
            .arg("node")
            .arg("--datadir")
            .arg(&*dir.to_string_lossy())
            .arg("--http")
            .arg("--http.port")
            .arg(rpc_port.to_string())
            .arg("--ws")
            .arg("--ws.port")
            .arg(ws_port.to_string())
            .arg("--ipcpath")
            .arg(ipc_path.to_string_lossy().as_ref());

        setup_log_dir(&mut command, v, test, run);

        children.push(command.spawn().expect("failed to execute"));
        endpoints.push(NodeEndpoints {
            http_url: format!("http://127.0.0.1:{rpc_port}"),
            ws_url: format!("ws://127.0.0.1:{ws_port}"),
            ipc_path: ipc_path.to_string_lossy().to_string(),
        });
    }

    Ok((children, endpoints))
}

/// Watch `iterations` epoch boundaries pass on `rpc_url`, asserting each one closes (the epoch
/// info changes and the block height grows). Returns the epoch id after the final boundary.
///
/// `epoch_duration` is the network's configured epoch duration (seconds); callers pass their own
/// value (e.g. epochs.rs runs a shorter cadence than the ejection tests) so the boundary-wait
/// deadline scales with it and the on-chain duration assertion matches the genesis config.
pub(crate) async fn loop_epochs(
    start: u32,
    iterations: u32,
    rpc_url: &str,
    epoch_duration: u64,
) -> eyre::Result<u32> {
    // create rpc client for node1 default rpc address
    let rpc_url = rpc_url.to_string();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    // retrieve current committee
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let mut current_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

    let mut last_epoch_block_height = current_epoch_info.blockHeight;
    for i in start..start + iterations {
        // Poll until the epoch changes, with a generous timeout for parallel test load. Capture
        // the changed `EpochInfo` from inside the poll (via `RefCell`, since `wait_until` takes
        // an `Fn` closure) so the boundary is read exactly once instead of fetched again after.
        let observed: RefCell<Option<ConsensusRegistry::EpochInfo>> = RefCell::new(None);
        wait_until(
            Duration::from_secs(epoch_duration * 4),
            &format!("epoch to change on iteration {i}"),
            || async {
                let info = consensus_registry.getCurrentEpochInfo().call().await?;
                let changed = info != current_epoch_info;
                if changed {
                    *observed.borrow_mut() = Some(info);
                }
                Ok(changed)
            },
        )
        .await?;
        let new_epoch_info =
            observed.into_inner().expect("wait_until returned Ok, so a changed epoch was observed");

        assert!(new_epoch_info.blockHeight > last_epoch_block_height);
        assert_eq!(new_epoch_info.epochDuration as u64, epoch_duration);

        // store the last seen epoch info that is expected to change every epoch
        last_epoch_block_height = new_epoch_info.blockHeight;
        current_epoch_info = new_epoch_info;
    }
    Ok(current_epoch_info.epochId)
}

/// Generate all the transactions needed for a new validator to be shuffled into the committee.
///
/// The validator's node info is read from `temp_path/new-validator` (see [`NEW_VALIDATOR`]).
pub(crate) fn generate_new_validator_txs(
    temp_path: &Path,
    chain: Arc<RethChainSpec>,
    new_validator: &mut TransactionFactory,
    governance_wallet: &mut TransactionFactory,
) -> eyre::Result<Vec<Vec<u8>>> {
    // read bls public key from fs for new validator
    let new_validator_path = temp_path.join(NEW_VALIDATOR);
    let new_validator_info = Config::load_from_path_or_default::<NodeInfo>(
        new_validator_path.join("node-info.yaml").as_path(),
        ConfigFmt::YAML,
    )?;

    // governance issue nft to new validator tx
    let calldata = ConsensusRegistry::mintCall { validatorAddress: new_validator.address() }
        .abi_encode()
        .into();
    let mint_nft = governance_wallet.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );

    // stake tx
    let proof = ConsensusRegistry::ProofOfPossession {
        signature: new_validator_info.proof_of_possession.to_bytes().into(),
    };
    let calldata = ConsensusRegistry::stakeCall {
        blsPubkey: new_validator_info.bls_public_key.compress().into(),
        proofOfPossession: proof,
    }
    .abi_encode()
    .into();
    let stake_tx = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        parse_ether(INITIAL_STAKE_AMOUNT)?,
        calldata,
    );

    // activation tx
    let calldata = ConsensusRegistry::activateCall {}.abi_encode().into();
    let activate_tx = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );

    Ok(vec![mint_nft, stake_tx, activate_tx])
}

/// Submit a transaction from the consensus-registry owner (governance) wallet to the
/// `ConsensusRegistry` and wait for it to confirm. Returns the tx hash and the block number the
/// transaction landed in (from its receipt) so callers can anchor assertions to an exact epoch.
pub(crate) async fn send_owner_tx(
    rpc_url: &str,
    owner_wallet: &mut TransactionFactory,
    chain: Arc<RethChainSpec>,
    calldata: Bytes,
) -> eyre::Result<(String, u64)> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let tx = owner_wallet.create_eip1559_encoded(
        chain,
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );
    let pending = provider.send_raw_transaction(&tx).await?;
    // txs may land right at an epoch boundary, get orphaned, and be re-injected into the next
    // epoch; allow two full epoch durations + startup buffer for confirmation
    let hash =
        timeout(Duration::from_secs(EPOCH_DURATION * 2 + 11), pending.watch()).await??.to_string();
    let block = get_tx_receipt_block(rpc_url, &hash)?;
    Ok((hash, block))
}

/// Minimal snapshot of an epoch's identity, its first EL block, and its duration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EpochSnapshot {
    pub(crate) epoch_id: u32,
    /// First EL block of the epoch (the block at which the committee became active). Under
    /// skip-empty-execution this block may not exist yet; the block BEFORE it is the previous
    /// epoch's closing block, produced exactly at the boundary.
    pub(crate) block_height: u64,
    /// The epoch's configured duration in seconds.
    pub(crate) epoch_duration: u64,
}

/// Poll a provider until its RPC answers `eth_chainId`.
pub(crate) async fn wait_for_rpc<P: Provider>(provider: &P) -> eyre::Result<()> {
    wait_until(Duration::from_secs(30), "provider RPC answers eth_chainId", || async {
        Ok(provider.get_chain_id().await.is_ok())
    })
    .await
}

/// Read the current epoch snapshot from the `ConsensusRegistry`.
pub(crate) async fn current_epoch<P: Provider>(provider: &P) -> eyre::Result<EpochSnapshot> {
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider);
    let info = registry.getCurrentEpochInfo().call().await?;
    Ok(EpochSnapshot {
        epoch_id: info.epochId,
        block_height: info.blockHeight,
        epoch_duration: u64::from(info.epochDuration),
    })
}

/// Poll the `ConsensusRegistry` until the current epoch id is at least `target`, returning the
/// snapshot of that epoch.
pub(crate) async fn wait_for_epoch_at_least<P: Provider>(
    provider: &P,
    target: u32,
) -> eyre::Result<EpochSnapshot> {
    // A boundary every `EPOCH_DURATION`s; allow generous slack for CI load.
    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 4 * (target as u64 + 1));
    loop {
        let snap = current_epoch(provider).await?;
        if snap.epoch_id >= target {
            return Ok(snap);
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "epoch did not reach {target} within timeout (stuck at {})",
                snap.epoch_id
            ));
        }
        // Poll ~4x/sec: with 5s epochs a 1s cadence adds up to ~1s of slop per boundary.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Poll `node` until its latest execution block number is at least `min_height`.
pub(crate) async fn wait_for_head_at_least(
    node: &str,
    min_height: u64,
    timeout_secs: u64,
) -> eyre::Result<()> {
    wait_until(
        Duration::from_secs(timeout_secs),
        &format!("{node} head to reach block {min_height}"),
        || async { Ok(get_block_number(node)? >= min_height) },
    )
    .await
}

/// Wait (bounded) for `epoch` to be reached on `http_url`, failing with a message that names the
/// calling phase instead of hanging until the harness slow-timeout kills the test.
pub(crate) async fn assert_epoch_reached(
    http_url: &str,
    epoch: u32,
    phase: &str,
) -> eyre::Result<()> {
    let provider = ProviderBuilder::new().connect_http(http_url.parse()?);
    let bound = EPOCH_DURATION * 6;
    match timeout(Duration::from_secs(bound), wait_for_epoch_at_least(&provider, epoch)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            Err(eyre::eyre!("{phase}: node {http_url} failed reaching epoch {epoch}: {e}"))
        }
        Err(_) => {
            Err(eyre::eyre!("{phase}: node {http_url} did not reach epoch {epoch} within {bound}s"))
        }
    }
}

/// Wait until the host clock sits inside a measured mid-epoch window and return that epoch's
/// snapshot.
///
/// `EpochInfo` exposes no epoch-start timestamp, but the previous epoch's closing block
/// (`block_height - 1`) is produced exactly at the boundary, so its timestamp measures when the
/// current epoch started (the registry records `block.number + 1` at `concludeEpoch`). All
/// testnet nodes run on this host, which makes host-clock vs block-timestamp comparison sound.
/// Re-checks on a bounded 250ms cadence, rather than a blind sleep followed by a hard assert,
/// until the measured phase is at least `MIN_PHASE` seconds into the epoch and at least
/// `END_MARGIN` seconds before the next boundary.
pub(crate) async fn wait_for_mid_epoch<P: Provider>(
    provider: &P,
    node: &str,
) -> eyre::Result<EpochSnapshot> {
    /// Seconds past the boundary before the mid-epoch window opens.
    const MIN_PHASE: u64 = 1;
    /// Seconds of margin demanded before the next boundary. With a 5s epoch this leaves a
    /// `[MIN_PHASE, epoch_duration - END_MARGIN] = [1s, 3s]` landing window that keeps the tx (and
    /// the restart kill) clear of both boundaries; at the previous 10s epoch the window was the
    /// wider `[2s, 6s]`. Expressed as small absolute seconds so the window stays non-empty at the
    /// 5s consensus minimum (`MIN_PHASE + END_MARGIN <= epoch_duration`).
    const END_MARGIN: u64 = 2;

    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 4);
    loop {
        let snap = current_epoch(provider).await?;
        let boundary_block = snap.block_height.saturating_sub(1);
        let epoch_start = read_block_timestamp(node, boundary_block)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("host clock is after the unix epoch")
            .as_secs();
        let phase = now.saturating_sub(epoch_start);
        if phase >= MIN_PHASE && phase + END_MARGIN <= snap.epoch_duration {
            info!(
                target: "e2e-test",
                epoch = snap.epoch_id, phase, duration = snap.epoch_duration,
                "measured mid-epoch phase"
            );
            return Ok(snap);
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "no mid-epoch window observed within {}s: epoch {} at phase {phase}s of {}s",
                EPOCH_DURATION * 4,
                snap.epoch_id,
                snap.epoch_duration
            ));
        }
        // Poll ~4x/sec so the mid-epoch window (as narrow as ~2s at a 5s epoch) is caught promptly.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Seconds of runway left before `snap`'s epoch closes, measured from the host clock against the
/// epoch's start.
///
/// Mirrors the phase arithmetic [`wait_for_mid_epoch`] performs: the block before the epoch's
/// first block (`block_height - 1`) is the previous epoch's closing block, produced exactly at the
/// boundary, so its timestamp is when this epoch started; every testnet node runs on this host, so
/// the host-clock vs block-timestamp comparison is sound. Saturates to 0 once the boundary is due.
/// `as_secs()` floors the host clock while the block timestamp is already whole seconds, so the
/// result can over-report the true remaining budget by strictly under a second; a caller must keep
/// a margin over its worst-case sequence rather than treat the value as exact.
pub(crate) fn epoch_seconds_remaining(node: &str, snap: &EpochSnapshot) -> eyre::Result<u64> {
    let boundary_block = snap.block_height.saturating_sub(1);
    let epoch_start = read_block_timestamp(node, boundary_block)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("host clock is after the unix epoch")
        .as_secs();
    let phase = now.saturating_sub(epoch_start);
    Ok(snap.epoch_duration.saturating_sub(phase))
}

/// Fetch the receipt for `tx_hash` from `node` via `eth_getTransactionReceipt` and return the
/// `blockNumber` it landed in.
///
/// Retries briefly: the balance-based landing signal and receipt indexing can race by a moment.
pub(crate) fn get_tx_receipt_block(node: &str, tx_hash: &str) -> eyre::Result<u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let receipt: Option<HashMap<String, Value>> =
            call_rpc(node, "eth_getTransactionReceipt", rpc_params!(tx_hash), 3, tx_hash)?;
        if let Some(receipt) = receipt {
            let raw = receipt.get("blockNumber").ok_or_else(|| {
                eyre::eyre!("receipt for tx {tx_hash} on {node} has no blockNumber field")
            })?;
            return parse_hex_u64(raw).ok_or_else(|| {
                eyre::eyre!(
                    "receipt for tx {tx_hash} on {node} blockNumber is not a hex u64: {raw:?}"
                )
            });
        }
        if std::time::Instant::now() >= deadline {
            return Err(eyre::eyre!("no receipt for confirmed tx {tx_hash} on {node} within 10s"));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Read the `baseFeePerGas` (as `u64`) of `block_number` from `node` via `eth_getBlockByNumber`.
///
/// These testnets run a single worker (worker 0), so every block's base fee is worker 0's fee.
///
/// Do **not** assume a block's fee is the fee of the epoch containing it. Only a
/// **transaction-bearing** block reliably carries its epoch's fee: it takes
/// `batch.base_fee_per_gas` (`crates/engine/src/payload_builder.rs:190`), which is the fee the
/// worker built the batch at. An empty epoch-closing block instead copies its parent's
/// `base_fee_per_gas` verbatim (`:124`), so in a run of idle epochs every block carries the last
/// transaction-bearing block's fee, however many boundaries back that was — including the idle
/// epoch's own single closing block.
///
/// Anchor a fee assertion to something read out of chain state — see
/// `state_export_import::recorded_entry_fee`, which reads the `WorkerConfigs` word a node actually
/// enters on — rather than inferring the expected value from another block's header, and read it
/// off a block that carried transactions.
pub(crate) fn read_base_fee(node: &str, block_number: u64) -> eyre::Result<u64> {
    let block = get_block(node, Some(block_number))?;
    let raw = block
        .get("baseFeePerGas")
        .ok_or_else(|| eyre::eyre!("block {block_number} on {node} has no baseFeePerGas field"))?;
    parse_hex_u64(raw).ok_or_else(|| {
        eyre::eyre!("block {block_number} on {node} baseFeePerGas is not a hex u64: {raw:?}")
    })
}

/// Read the `timestamp` (as `u64`) of `block_number` from `node` via `eth_getBlockByNumber`.
pub(crate) fn read_block_timestamp(node: &str, block_number: u64) -> eyre::Result<u64> {
    let block = get_block(node, Some(block_number))?;
    let raw = block
        .get("timestamp")
        .ok_or_else(|| eyre::eyre!("block {block_number} on {node} has no timestamp field"))?;
    parse_hex_u64(raw).ok_or_else(|| {
        eyre::eyre!("block {block_number} on {node} timestamp is not a hex u64: {raw:?}")
    })
}

/// Parse a JSON value that is expected to be a `0x`-prefixed hex string into a `u64`.
pub(crate) fn parse_hex_u64(value: &Value) -> Option<u64> {
    let s = value.as_str()?;
    let hex = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(hex, 16).ok()
}

/// Poll `http_url` for the certified epoch record of `epoch`, verify the certificate against the
/// record's own committee, and return the record.
///
/// Certificates are produced asynchronously after epoch boundaries via quorum voting, so this
/// polls until `timeout_secs` elapses before failing.
pub(crate) async fn fetch_verified_epoch_record(
    http_url: &str,
    epoch: u32,
    timeout_secs: u64,
) -> eyre::Result<EpochRecord> {
    let provider = ProviderBuilder::new().connect_http(http_url.parse()?);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let (epoch_rec, cert) = loop {
        match provider
            .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (epoch,))
            .await
        {
            Ok(result) => break result,
            Err(_) if Instant::now() < deadline => {
                // Poll ~4x/sec so the record is picked up promptly once quorum voting completes.
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => {
                return Err(eyre::eyre!(
                    "epoch record not available for epoch {epoch} on {http_url}: {e}"
                ));
            }
        }
    };
    eyre::ensure!(
        epoch_rec.verify_with_cert(&cert),
        "invalid epoch record: {} {}/{} {}!",
        http_url,
        epoch_rec.epoch,
        epoch_rec.digest(),
        cert.epoch_hash
    );
    Ok(epoch_rec)
}

/// Assert every node in `endpoints` serves a certified, verifying epoch record for every epoch in
/// `epochs`, and that each node has executed the final block named by each record with the exact
/// hash the record commits to.
///
/// Hash equality is the cross-node divergence detector: two nodes can both HAVE a block at the
/// recorded height while disagreeing on its contents (e.g. a different withdrawals_root ⇒ a
/// different hash), so existence alone cannot catch divergence.
pub(crate) async fn assert_epoch_records_verify(
    endpoints: &[NodeEndpoints],
    epochs: RangeInclusive<u32>,
    per_record_timeout_secs: u64,
) -> eyre::Result<()> {
    for ep in endpoints {
        for epoch in epochs.clone() {
            let epoch_rec =
                fetch_verified_epoch_record(&ep.http_url, epoch, per_record_timeout_secs).await?;
            // Make sure the node has executed the final block from the epoch record.
            // This should prove it has the consensus output as well (i.e. verify the pack data).
            let block =
                get_block(&ep.http_url, Some(epoch_rec.final_state.number)).map_err(|e| {
                    eyre::eyre!(
                        "final block {} for epoch {epoch} missing on {}: {e}",
                        epoch_rec.final_state.number,
                        ep.http_url
                    )
                })?;
            // The block must be the SAME block the record commits to, not merely one at the
            // same height (the RPC serves hex strings; the record stores a typed hash).
            let block_hash = block.get("hash").and_then(Value::as_str).ok_or_else(|| {
                eyre::eyre!(
                    "final block {} for epoch {epoch} on {} has no hash field",
                    epoch_rec.final_state.number,
                    ep.http_url
                )
            })?;
            let expected_hash = epoch_rec.final_state.hash.to_string();
            eyre::ensure!(
                block_hash.eq_ignore_ascii_case(&expected_hash),
                "final block {} for epoch {epoch} on {} hash mismatch: node has {block_hash}, \
                 record commits to {expected_hash}",
                epoch_rec.final_state.number,
                ep.http_url
            );
        }
    }
    Ok(())
}

/// Decode a secret key into it's public key and account.
/// Returns a tuple of (account, public_key, public_key_long) as hex encoded strings.
pub(crate) fn decode_key(key: &str) -> eyre::Result<(String, String, String)> {
    match const_hex::decode(key) {
        Ok(key) => {
            let key_array: [u8; 32] = key
                .as_slice()
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| Report::msg(e.to_string()))?;
            match SecretKey::from_byte_array(key_array) {
                Ok(secret_key) => {
                    let secp = Secp256k1::new();
                    let keypair = Keypair::from_secret_key(&secp, &secret_key);
                    let public_key = keypair.public_key();
                    // strip out the first byte because that should be the
                    // SECP256K1_TAG_PUBKEY_UNCOMPRESSED tag returned by
                    // libsecp's uncompressed pubkey serialization
                    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
                    let address = Address::from_slice(&hash[12..]);
                    Ok((
                        address.to_string(),
                        const_hex::encode(public_key.serialize()),
                        const_hex::encode(public_key.serialize_uncompressed()),
                    ))
                }
                Err(err) => Err(Report::msg(err.to_string())),
            }
        }
        Err(err) => Err(Report::msg(err.to_string())),
    }
}

// ---------------------------------------------------------------------------------------------
// Fork pins
// ---------------------------------------------------------------------------------------------

/// Environment variable selecting the multi-workers fork epoch (issue #554) for this process
/// and every node it spawns (`tn_types::forks::multi_workers_fork_epoch_override`).
pub(crate) const MULTI_WORKERS_FORK_ENV: &str = "TN_MULTI_WORKERS_FORK_EPOCH";

/// Environment variable selecting the seed-signature fork epoch (#1032) for this process and every
/// node it spawns (`tn_types::forks::seed_signature_fork_epoch_override`).
pub(crate) const SEED_SIGNATURE_FORK_ENV: &str = "TN_SEED_SIGNATURE_FORK_EPOCH";

/// Environment variable selecting the leader-seeded-ordering fork epoch (#1260) for this process
/// and every node it spawns (`tn_types::forks::leader_seeded_ordering_fork_epoch_override`).
pub(crate) const LEADER_SEEDED_ORDERING_FORK_ENV: &str = "TN_LEADER_SEEDED_ORDERING_FORK_EPOCH";

/// Environment variable selecting the sub-second-timestamp fork epoch for this process and every
/// node it spawns (`tn_types::forks::subsecond_timestamp_fork_epoch_override`).
pub(crate) const SUBSECOND_TIMESTAMP_FORK_ENV: &str = "TN_SUBSECOND_TIMESTAMP_FORK_EPOCH";

/// Fork epoch for a test that crosses a fork inside its run: epoch 0 runs pre-fork, every later
/// epoch post-fork.
///
/// The cross-fork sync tests, [`crate::epochs::test_epoch_sync_across_multi_workers_fork`] and
/// [`crate::epochs::test_epoch_sync_across_leader_seeded_ordering_fork`], are why it is 1. The
/// kill in [`crate::epochs::test_epoch_sync_inner`] happens after `loop_epochs` has watched three
/// boundaries pass, so the epoch open at that point is at least 3 and the sealed set — which stops
/// two below it, see [`crate::epochs::sealed_epochs`] — always covers epochs 0 and 1. Pinning a
/// fork at 1 therefore guarantees those sealed packs straddle it: epoch 0 written pre-fork (the
/// legacy single-worker committee layout, or the legacy DFS commit order), epoch 1 onward written
/// post-fork.
pub(crate) const CROSS_FORK_EPOCH: Epoch = 1;

/// Pin four fork epochs for this process and every node it spawns: the multi-workers fork
/// (issue #554), the seed-signature fork (#1032), the leader-seeded-ordering fork (#1260), and the
/// sub-second-timestamp fork. The PREVRANDAO and governance-Safe forks are not pinned; see below.
///
/// Two helpers decode consensus data inside this process, and they reach three of those gates:
/// [`crate::epochs::assert_sealed_packs_unchanged`] validates sealed pack bytes, and
/// [`read_consensus_headers`] reads a stopped node's consensus chain. The `EpochMeta`'s
/// [`tn_types::Committee`] is laid out by [`multi_workers_fork_active`], and every nested
/// `ConsensusHeader` by [`seed_signature_active`] and by
/// `tn_types::forks::subsecond_timestamp_active` (the millisecond fields of its sub-DAG and of
/// the headers inside it). So the harness has to resolve all three to the same fork points the
/// nodes wrote under. Left alone the two sides disagree the same way for the first two:
/// `TestBinary::command` forwards `u32::MAX` to a child when the variable is unset, while this
/// (non-adiri) harness build is active from genesis without it. Writing the variables settles
/// both sides at once — children inherit them verbatim at spawn, and the harness's own overrides
/// latch them on first read. The sub-second-timestamp fork's unset default already agrees
/// (`TestBinary::command` forwards `0`, and this build is active from genesis), so its pin is
/// what carries a forced fork point to both sides and turns a latched-earlier override into a
/// named failure. The leader-seeded-ordering fork changes no serialized layout, only the commit
/// order nodes write inside a pack, so neither decoder consults it; it is pinned here for the
/// children (and against a latched-earlier override), with the always-armed `0` default
/// `TestBinary::command` forwards for it.
///
/// All four are pinned, not just the one a given test is about. Pinning only some leaves the rest
/// asymmetric whenever the suite runs outside the Makefile wrapper that exports them, and the
/// symptom is misleading: children write dormant-layout headers, the harness decodes them as
/// genesis-active, and the decoder reports a corrupt pack rather than an environment mismatch.
///
/// The other two forks cannot put the harness and the nodes at odds. PREVRANDAO changes only the
/// executed block's `mix_hash`, which neither decoder reads and no e2e test checks, so children
/// run whatever `TestBinary::command` forwards: the lane's `TN_PREVRANDAO_FORK_EPOCH`, else the
/// dormant `u32::MAX`. Everything the governance-Safe fork is made of is `adiri`-gated, so it is
/// compiled out of this harness and of the default e2e node binary; only the
/// `make test-e2e-governance-safe` lane runs it, and that lane runs `test_governance_safe_fork`
/// alone.
///
/// Each `force_*` argument states that fork epoch outright, for a test whose claim is about a
/// specific boundary. `None` inherits whatever the lane exported, defaulting to what
/// `TestBinary::command` would have forwarded anyway (the dormant `u32::MAX`, or `0` for the
/// leader-seeded-ordering and sub-second-timestamp forks), so
/// `TN_MULTI_WORKERS_FORK_EPOCH=1 make test-epochs` keeps meaning what it says.
///
/// Call once per test, before the first node spawn and before anything in the process reads any
/// gate: the overrides are process-wide `OnceLock`s and the environment is process-wide too. That
/// is sound because nextest runs each test in its own process (`.config/nextest.toml`). Under
/// plain `cargo test`, two of these tests in one process would fight over it, and a later pin
/// would re-point the environment an already-running test spawns its nodes with. So only the
/// first pin in a process is allowed ([`FORKS_PINNED_BY`]); any later one fails at once, before
/// touching the environment, naming the test that holds the pins.
pub(crate) fn pin_fork_epochs(
    force_multi_workers: Option<Epoch>,
    force_seed_signature: Option<Epoch>,
    force_leader_seeded: Option<Epoch>,
    force_subsecond: Option<Epoch>,
) {
    claim_fork_pins();

    // what `TestBinary::command` would forward to a child: the value the lane exported, or the
    // stated per-fork default when it exported nothing. an unparseable value normalizes to the
    // same default the gate would have fallen back to.
    let lane = |var: &str, default: Epoch| -> Epoch {
        std::env::var(var).ok().and_then(|raw| raw.trim().parse().ok()).unwrap_or(default)
    };

    // one shared helper rather than a block per fork, mirroring `TestBinary::command`, so the
    // forks cannot drift apart in mechanism; they arm independently, so each carries its own gate
    pin_fork_epoch(
        MULTI_WORKERS_FORK_ENV,
        force_multi_workers.unwrap_or_else(|| lane(MULTI_WORKERS_FORK_ENV, u32::MAX)),
        multi_workers_fork_active,
    );
    pin_fork_epoch(
        SEED_SIGNATURE_FORK_ENV,
        force_seed_signature.unwrap_or_else(|| lane(SEED_SIGNATURE_FORK_ENV, u32::MAX)),
        seed_signature_active,
    );
    // the leader-seeded gate (`leader_seeded_ordering_active`) conjoins the seed-signature fork
    // fail-closed, so asserting through the gate would entangle this pin with the seed pin's
    // value: with the seed fork dormant the gate reads false at every epoch, pinned or not. pin
    // through the conjunct-free override reader instead; same latched-earlier failure mode.
    pin_fork_epoch_override(
        LEADER_SEEDED_ORDERING_FORK_ENV,
        force_leader_seeded.unwrap_or_else(|| lane(LEADER_SEEDED_ORDERING_FORK_ENV, 0)),
        leader_seeded_ordering_fork_epoch_override,
    );
    // `subsecond_timestamp_active` conjoins the seed-signature fork the same way, so this pin
    // goes through its conjunct-free override reader for the same reason
    pin_fork_epoch_override(
        SUBSECOND_TIMESTAMP_FORK_ENV,
        force_subsecond.unwrap_or_else(|| lane(SUBSECOND_TIMESTAMP_FORK_ENV, 0)),
        subsecond_timestamp_fork_epoch_override,
    );
}

/// The test (libtest names each test's thread after it) that pinned this process's fork epochs.
static FORKS_PINNED_BY: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Claim this process's fork pins for the current test, or fail with how to run the tests apart.
fn claim_fork_pins() {
    let me = std::thread::current().name().unwrap_or("<unnamed test>").to_string();
    let holder = FORKS_PINNED_BY.get_or_init(|| me.clone());
    assert_eq!(
        holder, &me,
        "fork epochs are process-wide and `{holder}` already pinned them in this process, so \
         `{me}` cannot run here. Run each e2e test in its own process: nextest \
         (`make test-e2e` / `make test-epochs`), or `cargo test -p e2e-tests --test it -- \
         <test> --exact --include-ignored`"
    );
}

/// Write `fork_epoch` to the `var` override and check `gate` reads the same fork point back.
///
/// Reading the gate here, rather than leaving it to whatever decodes a pack minutes later, is what
/// turns an override that latched before this pin into a named failure instead of a corrupt-looking
/// pack. A failed pin puts `var` back the way it found it before panicking (see
/// [`restore_fork_override`]).
pub(crate) fn pin_fork_epoch(var: &str, fork_epoch: Epoch, gate: impl Fn(Epoch) -> bool) {
    let previous = std::env::var_os(var);
    std::env::set_var(var, fork_epoch.to_string());

    // The gate is `>=`, so it fires at the fork epoch and nowhere below it; both checks hold for
    // the dormant pin too, since `u32::MAX >= u32::MAX`.
    let active_at_fork = gate(fork_epoch);
    let dormant_below = fork_epoch.checked_sub(1).is_none_or(|below| !gate(below));
    if !(active_at_fork && dormant_below) {
        restore_fork_override(var, previous);
    }
    assert!(
        active_at_fork,
        "harness gate must be active at the pinned fork epoch {fork_epoch}: {var} latched to \
         another value before this test pinned it"
    );
    assert!(
        dormant_below,
        "harness gate must be dormant below the pinned fork epoch {fork_epoch}: {var} latched \
         to another value before this test pinned it"
    );
    info!(target: "epoch-test", var, fork_epoch, "pinned a fork epoch");
}

/// Write `fork_epoch` to the `var` override and check the override reader `read` latched it.
///
/// The [`pin_fork_epoch`] variant for a fork whose public gate conjoins another fork (the
/// leader-seeded ordering conjoins the seed signature): the gate cannot witness this pin on its
/// own, but equality on the conjunct-free override reader gives callers the same guarantee, an
/// override that latched before this pin becomes a named failure instead of nodes silently
/// running a different fork point than the test states. A failed pin puts `var` back the way it
/// found it before panicking (see [`restore_fork_override`]).
pub(crate) fn pin_fork_epoch_override(
    var: &str,
    fork_epoch: Epoch,
    read: impl Fn() -> Option<Epoch>,
) {
    let previous = std::env::var_os(var);
    std::env::set_var(var, fork_epoch.to_string());

    let latched = read();
    if latched != Some(fork_epoch) {
        restore_fork_override(var, previous);
    }
    assert_eq!(
        latched,
        Some(fork_epoch),
        "harness override must read back the pinned fork epoch {fork_epoch}: {var} latched to \
         another value before this test pinned it"
    );
    info!(target: "epoch-test", var, fork_epoch, "pinned a fork epoch");
}

/// Put the `var` override back to `previous`, what it held before a pin wrote it: the old value,
/// or unset when it had none.
///
/// A pin that fails must not leave its value behind. The environment is process-wide, and
/// [`acquire_test_permit`] admits two e2e tests into one process under a single-process runner
/// (plain `cargo test`), so a failed pin's value would otherwise reach the other test's later node
/// spawns through `TestBinary::command` and fail that test with an error about its own network.
/// The check cannot run before the write instead: the override readers latch on first read, so
/// peeking would latch the old value and make the pin fail.
fn restore_fork_override(var: &str, previous: Option<std::ffi::OsString>) {
    match previous {
        Some(value) => std::env::set_var(var, value),
        None => std::env::remove_var(var),
    }
}

// ---------------------------------------------------------------------------------------------
// Commit-time, metrics and consensus-chain readers
// ---------------------------------------------------------------------------------------------

/// How long a block walk waits for any one RPC answer before it fails naming the node and block.
///
/// A node that accepts a connection and never answers would otherwise hold the walk until
/// nextest's terminate-after kill (`.config/nextest.toml`), with no message naming the node. Ten
/// seconds matches `call_rpc`'s request timeout and is far above what a local node takes to
/// serve one block.
pub(crate) const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// One execution block's commit time: its `tn_getBlockTimestampMillis` response, parsed out of
/// its hex-encoded JSON, plus whether `eth_getBlockByNumber` marks the block as closing an epoch.
///
/// [`walk_block_commit_times`] builds these after checking each one against the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockCommitTime {
    /// The execution block's number.
    pub(crate) block_number: u64,
    /// The execution block's hash.
    pub(crate) block_hash: tn_types::B256,
    /// The execution block's whole-second EVM `timestamp`.
    pub(crate) timestamp: u64,
    /// The commit time of the block's consensus header, in milliseconds since the Unix epoch.
    pub(crate) timestamp_millis: u64,
    /// Whether the consensus header's leader epoch commits with millisecond resolution.
    pub(crate) sub_second: bool,
    /// The number of the consensus header the block was executed from; `None` for genesis.
    pub(crate) consensus_number: Option<u64>,
    /// The digest of that consensus header, which is the block's `parentBeaconBlockRoot`; `None`
    /// for genesis.
    pub(crate) consensus_digest: Option<tn_types::B256>,
    /// Whether the block closes an epoch, read from the execution block rather than from
    /// `tn_getBlockTimestampMillis`, which does not report it.
    ///
    /// A block closes an epoch when it is the last block of the epoch's closing output. The node
    /// marks it by storing the closing epoch's 32-byte seed in `extra_data`, which every other
    /// block leaves empty (`TNBlockAssembler::assemble_block` in
    /// `crates/tn-reth/src/evm/block.rs`, read back on replay by `context_for_block` in
    /// `crates/tn-reth/src/evm/config.rs`).
    pub(crate) closes_epoch: bool,
}

/// Fetch and parse `tn_getBlockTimestampMillis` for execution block `block_number`, which
/// `closes_epoch` says whether it closes an epoch (the caller has read that from the block).
///
/// The node answers `null` for a block it does not know, so callers should only ask for heights
/// at or below the node's head. A `null` answer, a missing required field, or a malformed one is an
/// error naming the field. The two consensus fields are optional in the response (genesis has
/// neither), so only a present-but-malformed value fails for them. No answer within
/// [`RPC_REQUEST_TIMEOUT`] is an error naming the block.
async fn get_block_commit_time<P: Provider>(
    provider: &P,
    block_number: u64,
    closes_epoch: bool,
) -> eyre::Result<BlockCommitTime> {
    let response: Option<Value> = timeout(
        RPC_REQUEST_TIMEOUT,
        provider.raw_request("tn_getBlockTimestampMillis".into(), (format!("0x{block_number:x}"),)),
    )
    .await
    .map_err(|_| {
        eyre::eyre!(
            "tn_getBlockTimestampMillis for block {block_number} got no answer within \
             {RPC_REQUEST_TIMEOUT:?}"
        )
    })??;
    let response = response.ok_or_else(|| {
        eyre::eyre!("tn_getBlockTimestampMillis returned null for block {block_number}")
    })?;
    let malformed = |field: &str| {
        eyre::eyre!("block {block_number}: `{field}` missing or malformed: {response}")
    };
    let quantity = |field: &str| response.get(field).and_then(parse_hex_u64);
    let hash = |field: &str| {
        response
            .get(field)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<tn_types::B256>().ok())
    };
    // genesis omits both consensus fields, so only a present-but-unparseable one fails
    let optional = |field: &str, parsed: bool| -> eyre::Result<()> {
        if response.get(field).is_some() && !parsed {
            return Err(malformed(field));
        }
        Ok(())
    };

    let consensus_number = quantity("consensusNumber");
    optional("consensusNumber", consensus_number.is_some())?;
    let consensus_digest = hash("consensusDigest");
    optional("consensusDigest", consensus_digest.is_some())?;
    Ok(BlockCommitTime {
        block_number: quantity("blockNumber").ok_or_else(|| malformed("blockNumber"))?,
        block_hash: hash("blockHash").ok_or_else(|| malformed("blockHash"))?,
        timestamp: quantity("timestamp").ok_or_else(|| malformed("timestamp"))?,
        timestamp_millis: quantity("timestampMillis")
            .ok_or_else(|| malformed("timestampMillis"))?,
        sub_second: response
            .get("subSecond")
            .and_then(Value::as_bool)
            .ok_or_else(|| malformed("subSecond"))?,
        consensus_number,
        consensus_digest,
        closes_epoch,
    })
}

/// Check the sub-second timestamp invariants `provider` (the RPC of `node`) serves for every
/// execution block in `blocks`, and return each block's commit time in block order.
///
/// Per block, `tn_getBlockTimestampMillis` must describe the block `eth_getBlockByNumber` returns
/// (number, hash and `timestamp`); the `timestamp` must equal the commit time floored to whole
/// seconds and be no smaller than the previous walked block's; and the consensus digest must be
/// the block's `parentBeaconBlockRoot`, with genesis (block 0, which has neither) the only
/// exception. Blocks built from one consensus output name the same consensus header there and
/// must report the same commit time. The node reads that time from the header the digest names,
/// so this last check mostly restates the lookup, and how many blocks shared a header is logged,
/// not required.
///
/// The walk requires no coverage: it passes for a range that is all pre-fork, all post-fork, or
/// holds no epoch boundary, so the caller states which cases its run must contain (see
/// `crate::epochs::assert_block_commit_times`). `blocks` is an argument rather than genesis to
/// head so a node restored from a snapshot, which serves nothing below its restore floor, can be
/// walked from there; the first walked block is not compared with its parent. Every block in
/// `blocks` must be at or below the node's head, and each RPC request gets
/// [`RPC_REQUEST_TIMEOUT`].
pub(crate) async fn walk_block_commit_times<P: Provider>(
    provider: &P,
    node: &str,
    blocks: RangeInclusive<u64>,
) -> eyre::Result<Vec<BlockCommitTime>> {
    eyre::ensure!(!blocks.is_empty(), "{node}: no block to walk in {blocks:?}");
    let mut served: Vec<BlockCommitTime> = Vec::new();
    let mut by_beacon_root: BTreeMap<tn_types::B256, u64> = BTreeMap::new();
    let mut shared_root_blocks = 0usize;
    for number in blocks.clone() {
        let block = timeout(
            RPC_REQUEST_TIMEOUT,
            provider.get_block_by_number(BlockNumberOrTag::Number(number)),
        )
        .await
        .map_err(|_| {
            eyre::eyre!(
                "{node} did not answer eth_getBlockByNumber for block {number} within \
                         {RPC_REQUEST_TIMEOUT:?}"
            )
        })??
        .ok_or_else(|| eyre::eyre!("{node} has no block {number} of {blocks:?}"))?;
        let commit = get_block_commit_time(provider, number, !block.header.extra_data.is_empty())
            .await
            .map_err(|e| eyre::eyre!("{node}: {e}"))?;
        let context = format!("{node} block {number}: {commit:?}");
        eyre::ensure!(
            commit.block_number == number
                && commit.block_hash == block.header.hash
                && commit.timestamp == block.header.timestamp,
            "tn_getBlockTimestampMillis disagrees with eth_getBlockByNumber (hash {}, timestamp \
             {}): {context}",
            block.header.hash,
            block.header.timestamp,
        );
        eyre::ensure!(
            commit.timestamp == commit.timestamp_millis / 1000,
            "EVM timestamp is not the consensus commit time floored to seconds: {context}"
        );
        if let Some(parent) = served.last() {
            eyre::ensure!(
                commit.timestamp >= parent.timestamp,
                "EVM timestamp went backwards from block {} at {}: {context}",
                parent.block_number,
                parent.timestamp,
            );
        }
        // genesis carries the eip-4788 field zeroed; every later block names its consensus header
        match block.header.parent_beacon_block_root.filter(|root| !root.is_zero()) {
            None => eyre::ensure!(
                number == 0 && commit.consensus_digest.is_none(),
                "execution block without a consensus header: {context}"
            ),
            Some(root) => {
                eyre::ensure!(
                    commit.consensus_digest == Some(root),
                    "consensusDigest is not the parentBeaconBlockRoot {root}: {context}"
                );
                match by_beacon_root.entry(root) {
                    Entry::Vacant(entry) => {
                        entry.insert(commit.timestamp_millis);
                    }
                    Entry::Occupied(entry) => {
                        eyre::ensure!(
                            *entry.get() == commit.timestamp_millis,
                            "blocks from consensus header {root} report different commit times \
                             ({} ms earlier): {context}",
                            entry.get(),
                        );
                        shared_root_blocks += 1;
                    }
                }
            }
        }
        served.push(commit);
    }
    info!(
        target: "epoch-test",
        node,
        first = blocks.start(),
        last = blocks.end(),
        consensus_headers = by_beacon_root.len(),
        shared_root_blocks,
        "execution block commit times verified",
    );
    Ok(served)
}

/// Assert every node reports the same block and commit time at every height it shares with the
/// first node, the reference; `served[i]` is the walk of `nodes[i]`.
///
/// Both derive from consensus output alone, so a disagreement at a shared height is a fork in the
/// execution chain (the hash) or in the commit-time derivation (the milliseconds). Blocks are
/// matched by number rather than by position, so walks that start at different heights (a node
/// restored from a snapshot starts at its floor) still line up. A node that shares no height with
/// the reference fails, since comparing nothing would pass without checking anything.
pub(crate) fn assert_nodes_agree_on_commit_times(
    served: &[Vec<BlockCommitTime>],
    nodes: &[String],
) -> eyre::Result<()> {
    eyre::ensure!(
        served.len() == nodes.len(),
        "{} block walks for {} nodes: every walk needs the node it came from",
        served.len(),
        nodes.len(),
    );
    let Some((reference, others)) = served.split_first() else {
        return Ok(());
    };
    let reference: BTreeMap<u64, &BlockCommitTime> =
        reference.iter().map(|commit| (commit.block_number, commit)).collect();
    for (other, node) in others.iter().zip(nodes.iter().skip(1)) {
        let mut shared = 0usize;
        for actual in other {
            let Some(&expected) = reference.get(&actual.block_number) else { continue };
            eyre::ensure!(
                expected == actual,
                "{node} disagrees with {} at block {}: {actual:?} vs {expected:?}",
                nodes[0],
                expected.block_number,
            );
            shared += 1;
        }
        eyre::ensure!(
            shared > 0,
            "{node} served no block number that {} also served, so their commit times were never \
             compared",
            nodes[0],
        );
    }
    Ok(())
}

/// Read the value of the prometheus series `name` from the metrics endpoint at `addr` (the
/// address a node was started with `--metrics` on), summed over every label set.
///
/// A series that is absent from the scrape is an error, not zero: counters are the usual subject
/// of a "this never happened" assertion, and reading an unregistered or renamed series as zero
/// would pass that assertion without measuring anything. Retries for up to 30s, so a transient
/// scrape failure or a series that registers on first use late in startup does not fail the read.
pub(crate) fn scrape_metric_value(addr: &str, name: &str) -> eyre::Result<f64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let last = match scrape_metrics(addr) {
            Ok(body) => match sum_metric_samples(&body, name)? {
                Some(value) => return Ok(value),
                None => body,
            },
            Err(e) => format!("scrape failed: {e}"),
        };
        if std::time::Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "metrics endpoint {addr} never served series `{name}`; last response:\n{}",
                &last[..last.len().min(2000)]
            ));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Sum every sample of the prometheus series `name` in a text-format scrape `body`, or `None`
/// when the body holds no sample of it.
///
/// A sample line is `name value` or `name{labels} value`; comment lines and every other series
/// (including ones `name` is a prefix of) are skipped. A sample whose value does not parse is an
/// error rather than a skipped line, so a format change cannot hide the series.
fn sum_metric_samples(body: &str, name: &str) -> eyre::Result<Option<f64>> {
    let mut total = None;
    for line in body.lines().map(str::trim).filter(|line| !line.starts_with('#')) {
        let Some(rest) = line.strip_prefix(name) else { continue };
        // the label set can hold spaces inside quoted values, so step past its closing brace
        // before taking the value token
        let after_labels = match rest.strip_prefix('{') {
            Some(labels) => match labels.rfind('}') {
                Some(end) => &labels[end + 1..],
                None => continue,
            },
            None if rest.starts_with(char::is_whitespace) => rest,
            // a longer series name that merely starts with `name`
            None => continue,
        };
        let raw = after_labels.split_whitespace().next().unwrap_or_default();
        let value: f64 =
            raw.parse().map_err(|e| eyre::eyre!("sample of `{name}` has value {raw:?}: {e}"))?;
        total = Some(total.unwrap_or(0.0) + value);
    }
    Ok(total)
}

/// Read every consensus header a stopped node committed, in consensus-number order.
///
/// Opens the node's consensus chain under `datadir` directly, so the node must not be running
/// and must not be restarted on this datadir afterwards without care: opening heals the open
/// epoch's pack in place and clears leftover staging directories, which would race a live node.
/// The walk ends at the last header the open epoch's pack holds (read from the pack itself rather
/// than the "latest" slot hint, which can run one ahead of a pack cut short by a hard kill) and
/// starts at number 1, since the genesis header (number 0) is never stored. Any number in between
/// that the chain cannot serve is an error, so a gap fails the read instead of shortening the
/// walk.
///
/// The headers are decoded in this process, under its own fork gates, so this process must sit on
/// the same fork epochs as the nodes that wrote them; [`pin_fork_epochs`] establishes that, and a
/// test that reads headers without it can see a decode error that looks like a corrupt pack.
pub(crate) async fn read_consensus_headers(
    datadir: &Path,
) -> eyre::Result<Vec<tn_types::ConsensusHeader>> {
    // the node's `TelcoinDirs::epochs_db_path`
    let base = datadir.join("consensus-db").join("epochs");
    // opening a missing directory would quietly start a brand-new, empty chain there
    eyre::ensure!(base.is_dir(), "no consensus chain at {}", base.display());
    // the committee only seeds a chain that has never committed anything; an existing chain
    // reads every epoch's committee from its own packs
    let chain =
        tn_storage::consensus::ConsensusChain::new(base.clone(), tn_types::Committee::default())
            .map_err(|e| eyre::eyre!("opening consensus chain at {}: {e}", base.display()))?;
    let last = chain
        .consensus_header_latest()
        .await
        .map_err(|e| eyre::eyre!("reading latest consensus header at {}: {e}", base.display()))?
        .ok_or_else(|| eyre::eyre!("consensus chain at {} holds no headers", base.display()))?
        .number;

    let mut headers = Vec::new();
    for number in 1..=last {
        let header = chain
            .consensus_header_by_number(number)
            .await
            .map_err(|e| {
                eyre::eyre!("reading consensus header {number} at {}: {e}", base.display())
            })?
            .ok_or_else(|| {
                eyre::eyre!("consensus header {number} of 1..={last} missing at {}", base.display())
            })?;
        eyre::ensure!(
            header.number == number,
            "consensus header lookup for {number} at {} returned header {}",
            base.display(),
            header.number
        );
        headers.push(header);
    }
    Ok(headers)
}

// ---------------------------------------------------------------------------------------------
// Light transaction load and the EVM-timestamp clamp counter
// ---------------------------------------------------------------------------------------------

/// Prometheus series of the engine counter `tn_engine.evm_timestamp_clamped_total`.
///
/// The engine bumps it whenever it raises an EVM `timestamp` to its parent's. Consensus is meant
/// to produce non-decreasing commit times on its own, so a non-zero reading in
/// [`crate::epochs::test_epoch_subsecond_timestamps_across_fork`] is a consensus bug. On a real
/// network the first commits of epoch 0 can also be raised, when the validators' clocks lag the
/// genesis timestamp, but not in that test: it keeps epoch 0 pre-fork
/// ([`crate::epochs::SUBSECOND_FORK_EPOCH`]), where the engine never clamps, and it stamps genesis
/// from the host clock the nodes share, before they start.
pub(crate) const EVM_TIMESTAMP_CLAMPED_SERIES: &str = "tn_engine_evm_timestamp_clamped_total";

/// Pause between rounds of [`drive_light_tx_load`].
pub(crate) const LIGHT_LOAD_INTERVAL: Duration = Duration::from_millis(750);

/// Keep a light transaction load on every node until the caller drops this future.
///
/// Each round sends one transfer from `senders[i]` to `providers[i]`, so every worker regularly
/// seals a batch of its own and a single commit often carries batches from several workers; the
/// execution blocks built from such a commit share a `parentBeaconBlockRoot`, which is what the
/// per-header commit-time check in [`walk_block_commit_times`] compares.
///
/// [`crate::epochs::assert_block_commit_times`] requires blocks inside post-fork epochs, which only
/// this load produces, so a sender must survive a rejected transfer. Signing has already advanced
/// its nonce, and without a resync every later transfer from it would wait behind the gap. After a
/// rejection the sender takes its next nonce from the node before it sends again. Each rejection is
/// logged at warn and each resync at info (`light-load sender nonce resynced`).
pub(crate) async fn drive_light_tx_load<P: Provider>(
    providers: &[P],
    senders: &mut [TransactionFactory],
    chain: Arc<RethChainSpec>,
) -> Infallible {
    let sink = Address::from_slice(&[0x5e; 20]);
    let mut stale_nonces = vec![false; senders.len()];
    loop {
        for ((provider, sender), stale) in
            providers.iter().zip(senders.iter_mut()).zip(stale_nonces.iter_mut())
        {
            let address = sender.address();
            if *stale {
                // take the next nonce from the count the node has executed. while earlier
                // transfers are still pooled that count trails them, so the next send repeats a
                // pooled nonce and is rejected, and the sender resyncs each round until they
                // execute. a sender whose resync fails sits the round out rather than sign past
                // the gap
                match provider.get_transaction_count(address).await {
                    Ok(nonce) => {
                        sender.set_nonce(nonce);
                        *stale = false;
                        info!(
                            target: "epoch-test",
                            sender = %address,
                            nonce,
                            "light-load sender nonce resynced",
                        );
                    }
                    Err(error) => {
                        warn!(
                            target: "epoch-test",
                            %error,
                            sender = %address,
                            "light-load sender nonce resync failed",
                        );
                        continue;
                    }
                }
            }
            let tx = sender.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                Some(sink),
                U256::from(1),
                Bytes::new(),
            );
            if let Err(error) = provider.send_raw_transaction(&tx).await {
                warn!(
                    target: "epoch-test",
                    %error,
                    sender = %address,
                    "light-load transfer rejected",
                );
                // signing advanced the factory past the rejected nonce whether or not the node
                // took the transfer
                *stale = true;
            }
        }
        tokio::time::sleep(LIGHT_LOAD_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{pin_fork_epoch, pin_fork_epoch_override};
    use std::{
        cell::Cell,
        panic::{catch_unwind, AssertUnwindSafe},
    };
    use tn_types::Epoch;

    /// A variable no fork override reads, so the probe below cannot move a real fork point.
    const PROBE: &str = "TN_TEST_PIN_LEAK_PROBE";

    /// A pin that fails its check puts the variable back the way it found it before it panics: a
    /// variable that was unset is unset again, and one that held a value holds it again. Each
    /// failing check also records what the variable held while it ran, which shows the pin had
    /// written its own epoch first, so the restore is what removed it. A pin whose check passes
    /// keeps its value, so the restore belongs to the failure path alone.
    ///
    /// This is a plain `#[test]` rather than an ignored e2e test because it spawns no node and
    /// needs no node binary, so the default `cargo nextest run --workspace` lane can run it on
    /// every change. The ignored lanes would not catch a regression anyway: nextest gives each of
    /// those tests its own process, where a leaked variable has no other test to reach.
    #[test]
    fn pin_leak_failed_pin_restores_the_variable() {
        const PINNED: Epoch = 3;
        let seen: Cell<Option<String>> = Cell::new(None);
        let seen = &seen;
        // an override reader that latched `latched` before the pin ran
        let reader = move |latched: Epoch| {
            move || {
                seen.set(std::env::var(PROBE).ok());
                Some(latched)
            }
        };

        std::env::remove_var(PROBE);
        let pinned =
            catch_unwind(AssertUnwindSafe(|| pin_fork_epoch_override(PROBE, PINNED, reader(4))));
        assert!(pinned.is_err(), "a pin whose override reads back another epoch must panic");
        assert_eq!(seen.take().as_deref(), Some("3"), "the pin did not write before checking");
        assert_eq!(std::env::var_os(PROBE), None, "a failed pin left an unset variable set");

        std::env::set_var(PROBE, "7");
        let pinned =
            catch_unwind(AssertUnwindSafe(|| pin_fork_epoch_override(PROBE, PINNED, reader(7))));
        assert!(pinned.is_err(), "a pin whose override reads back another epoch must panic");
        assert_eq!(seen.take().as_deref(), Some("3"), "the pin did not write before checking");
        assert_eq!(
            std::env::var(PROBE).as_deref(),
            Ok("7"),
            "a failed pin did not restore the value it overwrote"
        );

        // the gate-checked pin restores the same way; this gate is dormant at every epoch
        let dormant_gate = |_: Epoch| {
            seen.set(std::env::var(PROBE).ok());
            false
        };
        let pinned = catch_unwind(AssertUnwindSafe(|| pin_fork_epoch(PROBE, PINNED, dormant_gate)));
        assert!(pinned.is_err(), "a pin whose gate is dormant at the fork epoch must panic");
        assert_eq!(seen.take().as_deref(), Some("3"), "the pin did not write before checking");
        assert_eq!(
            std::env::var(PROBE).as_deref(),
            Ok("7"),
            "a failed gate-checked pin did not restore the value it overwrote"
        );

        pin_fork_epoch_override(PROBE, PINNED, reader(PINNED));
        assert_eq!(std::env::var(PROBE).as_deref(), Ok("3"), "a passing pin lost its value");

        std::env::remove_var(PROBE);
    }
}
