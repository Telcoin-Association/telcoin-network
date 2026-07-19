//! Shared utilities for e2e integration tests.
//!
//! Process management, cleanup guards, and helpers used across all test modules.

use alloy::{
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
    collections::HashMap,
    fmt::Debug,
    ops::RangeInclusive,
    path::Path,
    process::Child,
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
use tn_test_utils::wait_until_blocking;
use tn_types::{
    address, get_available_tcp_port, keccak256,
    test_utils::{init_test_tracing, CommandParser},
    Address, EpochCertificate, EpochRecord, Genesis, GenesisAccount, RpcInfo, U256,
};
use tokio::{
    runtime::Builder,
    time::{timeout, Instant},
};
use tracing::{error, info};

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
    pub(crate) fn kill_all(&mut self) {
        // Phase 1: SIGTERM all in parallel for fast graceful shutdown
        self.send_term_all();

        // Phase 2: wait for each to exit, escalate to SIGKILL if needed
        for slot in self.children.iter_mut() {
            if let Some(ref mut child) = slot {
                wait_or_kill(child);
            }
            *slot = None;
        }
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
    if let Err(e) = child.kill() {
        error!(target: "e2e-test", ?e, "error sending SIGKILL");
    }
    if let Err(e) = child.wait() {
        error!(target: "e2e-test", ?e, "error waiting for child after SIGKILL");
    }
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
    Ok(resp?)
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

    setup_log_dir(&mut command, instance, test, run);

    command.spawn().expect("failed to execute")
}

/// Advertise a validator's JSON-RPC endpoint on its worker record.
///
/// The genesis ceremony leaves `p2p_info.worker.rpc` unset, and a non-committee node
/// forwards accepted transactions to whatever endpoints committee validators advertise
/// (issue #804); with none advertised, observer-submitted transactions are dropped.
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
    node_info.p2p_info.worker.rpc =
        Some(RpcInfo { http: format!("http://127.0.0.1:{rpc_port}").parse()?, ws: None });
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
        .arg("--observer")
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
    let expected_bal = if nonce > 0 { current_basefee + (current_basefee / (nonce)) } else { 0 };
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
        passphrase,
        governance_wallet,
        accounts,
        committee,
        epoch_duration,
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

/// Configure the initial committee and fund accounts for network genesis.
///
/// All data is written to file.
pub(crate) fn config_committee(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    passphrase: Option<String>,
    consensus_registry_owner: Address,
    accounts: Vec<(Address, GenesisAccount)>,
    validators: &Vec<(&str, Address)>,
    epoch_duration: u64,
) -> eyre::Result<Genesis> {
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
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        &consensus_registry_owner.to_string(),
        "--initial-stake-per-validator",
        INITIAL_STAKE_AMOUNT,
        "--min-withdraw-amount",
        min_withdrawal,
        "--epoch-block-rewards",
        epoch_rewards,
        "--epoch-duration-in-secs",
        &epoch_duration.to_string(),
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
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
        // poll until the epoch changes, with a generous timeout for parallel test load
        let deadline = Instant::now() + Duration::from_secs(epoch_duration * 4);
        let new_epoch_info = loop {
            let info = consensus_registry.getCurrentEpochInfo().call().await?;
            if info != current_epoch_info {
                break info;
            }
            assert!(
                Instant::now() < deadline,
                "Epoch did not change within {}s on iteration {i}",
                epoch_duration * 4
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        };

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
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match provider.get_chain_id().await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(eyre::eyre!("provider RPC never became available: {e}")),
        }
    }
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
                tokio::time::sleep(Duration::from_secs(1)).await;
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
/// `epochs`, and that each node has executed the final block named by each record.
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
            get_block(&ep.http_url, Some(epoch_rec.final_state.number)).map_err(|e| {
                eyre::eyre!(
                    "final block {} for epoch {epoch} missing on {}: {e}",
                    epoch_rec.final_state.number,
                    ep.http_url
                )
            })?;
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
