//! E2e tests for observer submission forwarding (`--forward-txs`, `--sanitize-txs`).
//!
//! The validators never advertise a worker RPC endpoint, so the node-record path an observer
//! otherwise uses to hand transactions to the committee has nowhere to send them. A transaction
//! submitted to the observer can only reach a validator through the `--forward-txs` target list.

use crate::common::{
    acquire_test_permit, address_from_word, call_rpc, get_balance, get_balance_above_with_retry,
    get_key, get_node_info, get_node_mode, network_advancing, scrape_metrics, send_tel,
    start_observer_with_args, start_validator, ProcessGuard, WEI_PER_TEL,
};
use alloy::{
    consensus::{
        transaction::RlpEcdsaEncodableTx as _, SignableTransaction as _, Signed, TxEip1559,
    },
    eips::eip2718::Encodable2718 as _,
    primitives::{Bytes, Signature, TxKind},
    signers::{local::PrivateKeySigner, SignerSync as _},
};
use e2e_tests::config_local_testnet;
use ethereum_tx_sign::{LegacyTransaction, Transaction as _};
use eyre::{eyre, WrapErr as _};
use jsonrpsee::{
    core::{client::ClientT as _, params::ArrayParams, ClientError, DeserializeOwned},
    http_client::HttpClientBuilder,
    rpc_params,
    types::ErrorObjectOwned,
};
use serde_json::Value;
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tempfile::TempDir;
use tn_test_utils::wait_until_blocking;
use tn_types::{get_available_tcp_port, keccak256, Address, NodeMode, U256};
use tokio::runtime::Builder;
use tracing::info;

/// The chain id of the harness genesis, the one `send_tel` signs for.
const TEST_CHAIN_ID: u64 = 0xde7e1;
/// Legacy gas price, in wei, of every transfer these tests sign.
const GAS_PRICE: u128 = 250;
/// Gas limit of a plain value transfer.
const TRANSFER_GAS: u128 = 21_000;
/// Value of each funded transfer.
const TRANSFER_AMOUNT: u128 = 10 * WEI_PER_TEL;
/// The prometheus name of the forwarder's per-submission outcome counter.
const FORWARDED_TOTAL: &str = "tn_reth_rpc_tx_forwarded_total";
/// The prometheus name of the forwarder's per-target failure counter.
const TARGET_FAILURES_TOTAL: &str = "tn_reth_rpc_tx_forward_target_failures_total";
/// Client timeout for `eth_sendRawTransactionSync`, above the node's 30s receipt wait so the
/// node's own answer arrives first.
const SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(45);
/// Client timeout for a plain submission, above the forwarder's 15s submission budget.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(20);

/// Four validators that advertise no worker RPC endpoint, and an observer that forwards
/// submissions to validator 1 through a target list whose first entry is dead.
struct ForwardingNetwork {
    /// The node processes; declared before the tempdir so the nodes stop before their data
    /// directories are removed.
    _guard: ProcessGuard,
    /// The data directories of every node.
    _tmp_dir: TempDir,
    /// RPC urls of the four validators, validator 1 first.
    client_urls: [String; 4],
    /// RPC url of the observer.
    obs_url: String,
    /// The observer's `--metrics` address.
    metrics_addr: String,
    /// Validator 1's RPC address as given to `--forward-txs`, the live target.
    live_target: String,
    /// The first `--forward-txs` entry, a port nothing listens on.
    dead_target: String,
    /// The test's log directory name under `crates/e2e-tests/test_logs/`.
    test: &'static str,
    /// The directory of the observer's rolling debug log file.
    file_log_dir: PathBuf,
}

impl ForwardingNetwork {
    /// Configure and start the network, then wait until every node serves RPC, the validators
    /// are active committee members and the observer follows them.
    fn start(test: &'static str, extra_observer_args: &[&str]) -> eyre::Result<Self> {
        let tmp_dir = TempDir::with_prefix(test)?;
        let temp_path = tmp_dir.path().to_path_buf();
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .wrap_err("failed to config")?;
        let bin = e2e_tests::get_telcoin_network_binary();

        // no `advertise_worker_rpc`: the node-record forwarding path must have no endpoint
        let mut guard = ProcessGuard::empty();
        let mut rpc_ports = [0_u16; 4];
        for (i, port) in rpc_ports.iter_mut().enumerate() {
            *port = free_port()?;
            guard.push(start_validator(i, bin, &temp_path, *port, test, 0));
        }
        let client_urls = rpc_ports.map(|port| format!("http://127.0.0.1:{port}"));

        // allocated and never bound, so connecting to it is refused for the whole test
        let dead_target = format!("127.0.0.1:{}", free_port()?);
        let live_target = format!("127.0.0.1:{}", rpc_ports[0]);
        let forward_txs = format!("{dead_target},{live_target}");
        let metrics_addr = format!("127.0.0.1:{}", free_port()?);
        let obs_rpc_port = free_port()?;
        let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
        // the file log goes into the tempdir, so the scan never reads an earlier run's file
        let file_log_root = temp_path.join("observer-logs");
        let file_log_root_arg = file_log_root.to_string_lossy().into_owned();
        let mut observer_args = vec![
            "--forward-txs",
            forward_txs.as_str(),
            "--metrics",
            metrics_addr.as_str(),
            // the documented hygiene filters on both sinks keep hyper-util's
            // `connecting to <ip:port>` lines out whatever `RUST_LOG` this process passes on
            "--log.stdout.filter",
            "hyper_util::client::legacy::connect=info",
            "--log.file.filter",
            "debug,hyper_util::client::legacy::connect=info",
            "--log.file.directory",
            file_log_root_arg.as_str(),
        ];
        observer_args.extend_from_slice(extra_observer_args);
        guard.push(start_observer_with_args(
            4,
            bin,
            &temp_path,
            obs_rpc_port,
            test,
            0,
            &observer_args,
        ));

        network_advancing(&client_urls)?;
        // the observer may still be syncing startup epoch records after the validators are ready
        wait_until_blocking(Duration::from_secs(45), "observer RPC ready", || {
            Ok(call_rpc::<String, _, _>(&obs_url, "eth_blockNumber", rpc_params![], 0, "ready")
                .is_ok())
        })?;
        client_urls.iter().try_for_each(|url| wait_for_node_mode(url, NodeMode::CvvActive))?;
        wait_for_node_mode(&obs_url, NodeMode::Observer)?;

        Ok(Self {
            _guard: guard,
            _tmp_dir: tmp_dir,
            client_urls,
            obs_url,
            metrics_addr,
            live_target,
            dead_target,
            test,
            // `Cli::run` appends this directory name to `--log.file.directory`
            file_log_dir: file_log_root.join("telcoin-network-logs"),
        })
    }

    /// The observer's current count of submissions that ended with `outcome`.
    fn forwarded(&self, outcome: &str) -> eyre::Result<u64> {
        counter_sample(&self.metrics_addr, FORWARDED_TOTAL, &[&format!("outcome=\"{outcome}\"")])
    }

    /// Assert that neither target address appears in the observer's `tn_info` answer, in
    /// anything the observer process has written to its captured stdout and stderr, or in its
    /// debug log file.
    fn assert_targets_not_exposed(&self) -> eyre::Result<()> {
        let info = serde_json::to_string(&get_node_info(&self.obs_url)?)?;
        let log_dir =
            PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("test_logs").join(self.test);
        // `setup_log_dir` names the observer's files after its instance number, 4
        let stdout = std::fs::read_to_string(log_dir.join("node4-run0.log"))?;
        let stderr = std::fs::read_to_string(log_dir.join("node4-run0.stderr.log"))?;
        eyre::ensure!(!stdout.is_empty(), "the observer's captured stdout is empty");
        // the rolling appender writes reth.log, then reth.log.1, ...
        let mut file_log = String::new();
        for entry in std::fs::read_dir(&self.file_log_dir)? {
            let path = entry?.path();
            if path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("reth.log"))
            {
                file_log.push_str(&std::fs::read_to_string(&path)?);
            }
        }
        // the forwarder's own debug line proves the file holds debug output, so the absence
        // checks below measure something
        eyre::ensure!(
            file_log.contains("forward attempt failed"),
            "the observer's debug log file holds no forwarder debug line"
        );
        for target in [&self.live_target, &self.dead_target] {
            eyre::ensure!(!info.contains(target.as_str()), "tn_info exposes {target}: {info}");
            eyre::ensure!(!stdout.contains(target.as_str()), "the observer log names {target}");
            eyre::ensure!(!stderr.contains(target.as_str()), "the observer stderr names {target}");
            eyre::ensure!(
                !file_log.contains(target.as_str()),
                "the observer log file names {target}"
            );
        }
        Ok(())
    }
}

/// The observer forwards submissions to the live target after the dead one fails, passes the
/// validator's error through unchanged, serves `eth_sendRawTransactionSync` from its own chain,
/// and never exposes the target list.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_observer_forward_txs() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    info!(target: "e2e-test", "test_restarts_observer_forward_txs");
    let net = ForwardingNetwork::start("forward_txs", &[])?;
    let validator = &net.client_urls[0];
    let key = get_key("test-source");
    let to = address_from_word("forward-txs");
    let start_balance = get_balance(&net.obs_url, &to.to_string(), 5)?;

    // the first submission meets the dead target first, so it can only land through failover
    let hash = send_tel(&net.obs_url, &key, to, TRANSFER_AMOUNT, GAS_PRICE, TRANSFER_GAS, 0)?;
    wait_for_receipt(validator, &hash)?;
    get_balance_above_with_retry(&net.obs_url, &to.to_string(), start_balance)?;

    // rebuild the same signed bytes; the hash proves they are the ones `send_tel` submitted
    let raw = signed_transfer(&key, to, 0)?;
    eyre::ensure!(
        format!("{:#x}", keccak256(&raw)) == hash.to_lowercase(),
        "rebuilt transfer does not hash to the submitted {hash}"
    );
    // validator 1's own answer to the same bytes, asked before the nonce-1 transfer so both
    // calls see the same account nonce
    let via_observer = expect_rpc_error_object(rpc_once::<String>(
        &net.obs_url,
        "eth_sendRawTransaction",
        raw.clone(),
        SUBMIT_TIMEOUT,
    )?)?;
    let direct = expect_rpc_error_object(rpc_once::<String>(
        validator,
        "eth_sendRawTransaction",
        raw,
        SUBMIT_TIMEOUT,
    )?)?;
    info!(
        target: "e2e-test",
        code = via_observer.code(),
        message = via_observer.message(),
        "resubmission refused upstream"
    );
    eyre::ensure!(
        via_observer == direct,
        "the observer altered the validator's error: {via_observer:?} vs {direct:?}"
    );
    eyre::ensure!(
        via_observer.message().contains("nonce too low"),
        "expected nonce too low, got {via_observer:?}"
    );

    let raw = signed_transfer(&key, to, 1)?;
    let expected_hash = format!("{:#x}", keccak256(&raw));
    let receipt = rpc_once::<HashMap<String, Value>>(
        &net.obs_url,
        "eth_sendRawTransactionSync",
        raw,
        SYNC_CALL_TIMEOUT,
    )?
    .map_err(|e| eyre!("eth_sendRawTransactionSync through the observer failed: {e}"))?;
    eyre::ensure!(
        receipt.get("transactionHash").and_then(Value::as_str) == Some(expected_hash.as_str()),
        "sync receipt is for another transaction: {receipt:?}"
    );
    eyre::ensure!(
        receipt.get("status").and_then(Value::as_str) == Some("0x1"),
        "sync transfer did not succeed: {receipt:?}"
    );
    get_balance_above_with_retry(
        &net.obs_url,
        &to.to_string(),
        start_balance + 2 * TRANSFER_AMOUNT - 1,
    )?;

    let accepted = net.forwarded("accepted")?;
    let upstream_error = net.forwarded("upstream_error")?;
    let dead_target_failures = counter_sample(
        &net.metrics_addr,
        TARGET_FAILURES_TOTAL,
        &["target=\"0\"", "kind=\"transport\""],
    )?;
    info!(target: "e2e-test", accepted, upstream_error, dead_target_failures, "forwarding metrics");
    eyre::ensure!(accepted >= 2, "expected two accepted submissions, metrics show {accepted}");
    eyre::ensure!(upstream_error >= 1, "the refused resubmission was not counted upstream");
    eyre::ensure!(dead_target_failures >= 1, "the dead first target was never tried");

    net.assert_targets_not_exposed()
}

/// With `--sanitize-txs` the observer refuses a malleable signature and a foreign chain id itself,
/// so neither reaches the validator, and still forwards a valid transfer.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_observer_forward_txs_sanitized() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    info!(target: "e2e-test", "test_restarts_observer_forward_txs_sanitized");
    let net = ForwardingNetwork::start("forward_txs_sanitized", &["--sanitize-txs"])?;
    let chain_id: String = call_rpc(&net.obs_url, "eth_chainId", rpc_params![], 3, "chain id")?;
    eyre::ensure!(
        u64::from_str_radix(chain_id.trim_start_matches("0x"), 16)? == TEST_CHAIN_ID,
        "unexpected test chain id {chain_id}"
    );
    let key = get_key("test-source");
    let signer = PrivateKeySigner::from_slice(&const_hex::decode(&key)?)?;
    let to = address_from_word("forward-txs-sanitized");

    let accepted = net.forwarded("accepted")?;
    let upstream_error = net.forwarded("upstream_error")?;
    let rejected_locally = net.forwarded("rejected_locally")?;

    let (code, message) = expect_rpc_error(rpc_once::<String>(
        &net.obs_url,
        "eth_sendRawTransaction",
        high_s_transfer(&signer, to)?,
        SUBMIT_TIMEOUT,
    )?)?;
    eyre::ensure!(
        code == -32602 && message.contains("invalid transaction signature"),
        "high-s transfer: expected -32602 invalid transaction signature, got {code} {message}"
    );
    let (code, message) = expect_rpc_error(rpc_once::<String>(
        &net.obs_url,
        "eth_sendRawTransaction",
        wrong_chain_transfer(&signer, to)?,
        SUBMIT_TIMEOUT,
    )?)?;
    eyre::ensure!(
        code == -32000 && message.contains("invalid chain ID"),
        "chain id 1 transfer: expected -32000 invalid chain ID, got {code} {message}"
    );

    let rejected_after = net.forwarded("rejected_locally")?;
    eyre::ensure!(
        rejected_after >= rejected_locally + 2,
        "rejected_locally went from {rejected_locally} to {rejected_after}"
    );
    eyre::ensure!(net.forwarded("accepted")? == accepted, "a refused transfer was forwarded");
    eyre::ensure!(
        net.forwarded("upstream_error")? == upstream_error,
        "a refused transfer reached the validator"
    );

    let start_balance = get_balance(&net.obs_url, &to.to_string(), 5)?;
    let hash = send_tel(&net.obs_url, &key, to, TRANSFER_AMOUNT, GAS_PRICE, TRANSFER_GAS, 0)?;
    wait_for_receipt(&net.client_urls[0], &hash)?;
    get_balance_above_with_retry(&net.obs_url, &to.to_string(), start_balance)?;
    eyre::ensure!(
        net.forwarded("accepted")? > accepted,
        "the valid transfer was not counted as accepted"
    );

    net.assert_targets_not_exposed()
}

/// Allocate a localhost TCP port no other port this test process allocates will collide with.
fn free_port() -> eyre::Result<u16> {
    get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre!("no free tcp port on 127.0.0.1"))
}

/// Wait for `node` to report `expected` as its consensus participation mode.
fn wait_for_node_mode(node: &str, expected: NodeMode) -> eyre::Result<()> {
    wait_until_blocking(
        Duration::from_secs(30),
        &format!("node {node} entered {expected:?}"),
        || Ok(get_node_mode(node).is_ok_and(|mode| mode == expected)),
    )
}

/// Wait for `node` to serve a receipt for `tx_hash`.
fn wait_for_receipt(node: &str, tx_hash: &str) -> eyre::Result<()> {
    wait_until_blocking(
        Duration::from_secs(60),
        &format!("receipt for {tx_hash} on {node}"),
        || {
            let receipt: Option<Value> =
                call_rpc(node, "eth_getTransactionReceipt", rpc_params!(tx_hash), 0, tx_hash)
                    .unwrap_or(None);
            Ok(receipt.is_some())
        },
    )
}

/// The test-source transfer `send_tel` signs for these arguments, as raw bytes.
fn signed_transfer(key: &str, to: Address, nonce: u128) -> eyre::Result<Vec<u8>> {
    let tx = LegacyTransaction {
        chain: TEST_CHAIN_ID,
        nonce,
        to: Some(to.into_array()),
        value: TRANSFER_AMOUNT,
        gas_price: GAS_PRICE,
        gas: TRANSFER_GAS,
        data: vec![],
    };
    let ecdsa = tx.ecdsa(&const_hex::decode(key)?).map_err(|_| eyre!("failed to sign"))?;
    Ok(tx.sign(&ecdsa))
}

/// An unsigned EIP-1559 transfer of one wei to `to` at nonce 0 on `chain_id`.
fn eip1559_transfer(chain_id: u64, to: Address) -> TxEip1559 {
    TxEip1559 {
        chain_id,
        nonce: 0,
        gas_limit: 21_000,
        max_fee_per_gas: GAS_PRICE,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(to),
        value: U256::from(1),
        access_list: Default::default(),
        input: Bytes::new(),
    }
}

/// A transfer on the test chain whose signature is the high-s twin of a valid one. The signer
/// still recovers under lax rules, so only the canonical-signature rule refuses it.
fn high_s_transfer(signer: &PrivateKeySigner, to: Address) -> eyre::Result<Vec<u8>> {
    let tx = eip1559_transfer(TEST_CHAIN_ID, to);
    let signature = signer.sign_hash_sync(&tx.signature_hash())?;
    let order = U256::from_be_bytes(secp256k1::constants::CURVE_ORDER);
    let high_s = Signature::new(signature.r(), order - signature.s(), !signature.v());
    let hash = tx.tx_hash(&high_s);
    Ok(Signed::new_unchecked(tx, high_s, hash).encoded_2718())
}

/// A validly signed transfer for chain id 1 instead of the test chain.
fn wrong_chain_transfer(signer: &PrivateKeySigner, to: Address) -> eyre::Result<Vec<u8>> {
    let tx = eip1559_transfer(1, to);
    let signature = signer.sign_hash_sync(&tx.signature_hash())?;
    Ok(tx.into_signed(signature).encoded_2718())
}

/// Submit `raw` with `method` to `node` once, without retries, and return the client's own
/// result so a JSON-RPC error object can be inspected.
fn rpc_once<R: DeserializeOwned>(
    node: &str,
    method: &str,
    raw: Vec<u8>,
    timeout: Duration,
) -> eyre::Result<Result<R, ClientError>> {
    let params: ArrayParams = rpc_params!(const_hex::encode_prefixed(raw));
    Builder::new_current_thread().enable_io().enable_time().build()?.block_on(async {
        let client = HttpClientBuilder::default().request_timeout(timeout).build(node)?;
        Ok::<_, eyre::Report>(client.request(method, params).await)
    })
}

/// The JSON-RPC error object of a call, or an error if the call succeeded or failed in transport.
fn expect_rpc_error_object<R: std::fmt::Debug>(
    result: Result<R, ClientError>,
) -> eyre::Result<ErrorObjectOwned> {
    match result {
        Err(ClientError::Call(error)) => Ok(error),
        Err(other) => Err(eyre!("expected a JSON-RPC error, the call failed with {other}")),
        Ok(value) => Err(eyre!("expected a JSON-RPC error, the call returned {value:?}")),
    }
}

/// The code and message of a JSON-RPC error, or an error if the call succeeded or failed in
/// transport.
fn expect_rpc_error<R: std::fmt::Debug>(
    result: Result<R, ClientError>,
) -> eyre::Result<(i32, String)> {
    expect_rpc_error_object(result).map(|error| (error.code(), error.message().to_string()))
}

/// Read the sample of the counter `name` whose label set holds every one of `labels`, retrying
/// for up to 30s.
///
/// A scrape without such a sample is an error rather than zero, so a renamed series or label
/// cannot pass an "unchanged" assertion without measuring anything.
fn counter_sample(addr: &str, name: &str, labels: &[&str]) -> eyre::Result<u64> {
    let mut last = String::new();
    for _ in 0..30 {
        match scrape_metrics(addr) {
            Ok(body) => match find_sample(&body, name, labels)? {
                Some(value) => return Ok(value),
                None => last = body,
            },
            Err(e) => last = format!("scrape failed: {e}"),
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(eyre!(
        "metrics endpoint {addr} never served {name} with {labels:?}; last response:\n{}",
        &last[..last.len().min(2000)]
    ))
}

/// Find the value of the sample of `name` whose label set holds every one of `labels` in a
/// prometheus text scrape.
fn find_sample(body: &str, name: &str, labels: &[&str]) -> eyre::Result<Option<u64>> {
    let prefix = format!("{name}{{");
    body.lines()
        .filter_map(|line| line.trim().strip_prefix(prefix.as_str()))
        .filter_map(|rest| rest.split_once('}'))
        .find(|(set, _)| labels.iter().all(|label| set.split(',').any(|pair| pair == *label)))
        .map(|(_, value)| {
            let raw = value.split_whitespace().next().unwrap_or_default();
            raw.parse::<u64>().map_err(|e| eyre!("sample of {name} has value {raw:?}: {e}"))
        })
        .transpose()
}
