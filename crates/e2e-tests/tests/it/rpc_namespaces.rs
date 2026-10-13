//! E2e test for JSON-RPC namespace selection (`--http.api`) on running validators.
//!
//! One validator adds `debug` and `trace` to the default namespaces, one serves a literal
//! `eth,net,web3` list and the other two keep the default. A TEL transfer is traced through the
//! first validator; the other two prove what an explicit list and the default leave out.

use crate::common::{
    address_from_word, call_rpc, decode_key, get_balance, get_balance_above_with_retry, get_key,
    get_node_info, get_tx_receipt_block, network_advancing, send_tel, start_validator_with_args,
    ProcessGuard, WEI_PER_TEL,
};
use jsonrpsee::{
    core::{client::Error as ClientError, traits::ToRpcParams},
    rpc_params,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};
use tn_types::{get_available_tcp_port, Address};
use tracing::info;

/// JSON-RPC error code for a method the transport does not serve.
const METHOD_NOT_FOUND: i32 = -32601;

/// The namespaces an HTTP transport serves when `--http.api` is not given.
const DEFAULT_NAMESPACES: [&str; 5] = ["eth", "net", "rpc", "tn", "web3"];

/// The namespaces the tracing validator selects explicitly.
const TRACING_NAMESPACES: [&str; 7] = ["debug", "eth", "net", "rpc", "tn", "trace", "web3"];

/// Retries for calls expected to succeed on a validator that already serves RPC.
const RETRIES: usize = 5;

/// Start four validators with different `--http.api` selections, trace a transfer through
/// `debug` and `trace` on the one that selects them, and check that the other selections serve
/// exactly their namespaces.
///
/// - validator 0: `eth,net,web3,rpc,tn,debug,trace`
/// - validator 1: `eth,net,web3` (an explicit list is literal, so no `tn` and no `rpc`)
/// - validators 2 and 3: no `--http.api` (the default set, without `debug` and `trace`)
#[test]
#[ignore = "should not run with a default cargo test, run e2e tests as a separate step"]
fn test_rpc_namespaces_on_running_validators() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "e2e-test", "test_rpc_namespaces_on_running_validators");

    let tmp_guard = tempfile::TempDir::with_prefix("rpc_namespaces").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    e2e_tests::config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
        .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    let http_api: [&[&str]; 4] = [
        &["--http.api", "eth,net,web3,rpc,tn,debug,trace"],
        &["--http.api", "eth,net,web3"],
        &[],
        &[],
    ];
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for (i, (url, extra_args)) in client_urls.iter_mut().zip(http_api).enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        url.push_str(&format!(":{rpc_port}"));
        guard.push(start_validator_with_args(
            i,
            bin,
            &temp_path,
            rpc_port,
            "rpc_namespaces",
            0,
            extra_args,
        ));
    }

    // every selection keeps `eth`, so all four answer the block-number probe
    network_advancing(&client_urls)?;
    let [tracing_node, literal_node, default_node, _] = &client_urls;

    // land one plain transfer from the genesis-funded account through the tracing validator
    let key = get_key("test-source");
    let sender: Address = decode_key(&key)?.0.parse()?;
    let recipient = address_from_word("rpc-namespaces");
    let amount = 10 * WEI_PER_TEL;
    let before = get_balance(tracing_node, &recipient.to_string(), RETRIES)?;
    let tx_hash = send_tel(tracing_node, &key, recipient, amount, 250, 21_000, 0)?;
    get_balance_above_with_retry(tracing_node, &recipient.to_string(), before)?;
    let block = get_tx_receipt_block(tracing_node, &tx_hash)?;
    info!(target: "e2e-test", %tx_hash, block, "transfer landed");

    // validator 0: the geth call tracer reports one CALL frame with the intrinsic gas
    let call: Value = call_rpc(
        tracing_node,
        "debug_traceTransaction",
        rpc_params!(&tx_hash, json!({ "tracer": "callTracer" })),
        RETRIES,
        "callTracer",
    )?;
    info!(target: "e2e-test", %call, "debug_traceTransaction callTracer");
    assert_eq!(call["type"], "CALL", "callTracer frame: {call}");
    assert_eq!(parse_address(&call["from"]), Some(sender), "callTracer frame: {call}");
    assert_eq!(parse_address(&call["to"]), Some(recipient), "callTracer frame: {call}");
    assert_eq!(hex_u128(&call["value"]), Some(amount), "callTracer frame: {call}");
    assert_eq!(call["gasUsed"], "0x5208", "callTracer frame: {call}");

    // validator 0: a plain transfer runs no opcodes, so the struct logger has no steps
    let steps: Value = call_rpc(
        tracing_node,
        "debug_traceTransaction",
        rpc_params!(&tx_hash, json!({})),
        RETRIES,
        "struct logger",
    )?;
    info!(target: "e2e-test", %steps, "debug_traceTransaction struct logger");
    assert_eq!(steps["failed"], false, "struct logger: {steps}");
    assert_eq!(steps["gas"], 21_000, "struct logger: {steps}");
    assert_eq!(steps["structLogs"], json!([]), "struct logger: {steps}");

    // validator 0: the parity-style trace is one top-level call attributed to the receipt's block
    let traces: Vec<Value> =
        call_rpc(tracing_node, "trace_transaction", rpc_params!(&tx_hash), RETRIES, &tx_hash)?;
    info!(target: "e2e-test", traces = %json!(traces), "trace_transaction");
    let [trace] = traces.as_slice() else {
        eyre::bail!("trace_transaction returned {} traces, expected 1: {traces:?}", traces.len());
    };
    assert_eq!(trace["type"], "call", "trace_transaction: {trace}");
    assert_eq!(trace["action"]["callType"], "call", "trace_transaction: {trace}");
    assert_eq!(parse_address(&trace["action"]["to"]), Some(recipient), "trace: {trace}");
    assert_eq!(hex_u128(&trace["action"]["value"]), Some(amount), "trace: {trace}");
    assert!(is_tx(&trace["transactionHash"], &tx_hash), "trace_transaction: {trace}");
    assert_eq!(trace["blockNumber"], block, "trace_transaction: {trace}");

    // validator 0: the block trace holds the transfer and no proof-of-work reward
    let block_param = format!("0x{block:x}");
    let block_traces: Vec<Value> =
        call_rpc(tracing_node, "trace_block", rpc_params!(&block_param), RETRIES, &block_param)?;
    assert!(
        block_traces.iter().any(|trace| is_tx(&trace["transactionHash"], &tx_hash)),
        "trace_block({block_param}) misses {tx_hash}: {block_traces:?}"
    );
    assert!(
        block_traces.iter().all(|trace| trace["type"] != "reward"),
        "trace_block({block_param}) holds a reward trace: {block_traces:?}"
    );

    // validator 0: `rpc_modules` lists the explicit selection and the real `tn` module answers
    let modules = rpc_module_names(tracing_node)?;
    assert_eq!(modules, namespace_set(&TRACING_NAMESPACES), "rpc_modules on {tracing_node}");
    let epoch: u32 =
        call_rpc(tracing_node, "tn_getCurrentEpoch", rpc_params![], RETRIES, "tn_getCurrentEpoch")?;
    info!(target: "e2e-test", epoch, "tn_getCurrentEpoch");

    // validator 1: an explicit list is literal; `eth` serves the transfer, everything else is
    // refused, including `rpc_modules` because `rpc` is not listed
    assert_eq!(get_tx_receipt_block(literal_node, &tx_hash)?, block);
    assert_method_not_found(literal_node, "tn_info", rpc_params![])?;
    assert_method_not_found(
        literal_node,
        "debug_traceTransaction",
        rpc_params!(&tx_hash, json!({ "tracer": "callTracer" })),
    )?;
    assert_method_not_found(literal_node, "rpc_modules", rpc_params![])?;

    // validator 2: the default set serves `tn` and `rpc` but neither `debug` nor `trace`
    let info = get_node_info(default_node)?;
    assert_eq!(info.get("chain_id"), Some(&Value::from(911_329)), "tn_info: {info:?}");
    let modules = rpc_module_names(default_node)?;
    assert_eq!(modules, namespace_set(&DEFAULT_NAMESPACES), "rpc_modules on {default_node}");
    assert_method_not_found(
        default_node,
        "debug_traceTransaction",
        rpc_params!(&tx_hash, json!({ "tracer": "callTracer" })),
    )?;
    assert_method_not_found(default_node, "trace_transaction", rpc_params!(&tx_hash))?;

    Ok(())
}

/// Return the namespace names `rpc_modules` reports for `node`.
fn rpc_module_names(node: &str) -> eyre::Result<BTreeSet<String>> {
    let modules: BTreeMap<String, String> =
        call_rpc(node, "rpc_modules", rpc_params![], RETRIES, "rpc_modules")?;
    info!(target: "e2e-test", ?modules, node, "rpc_modules");
    Ok(modules.into_keys().collect())
}

/// Collect namespace names into the set [`rpc_module_names`] returns.
fn namespace_set(names: &[&str]) -> BTreeSet<String> {
    names.iter().copied().map(String::from).collect()
}

/// Call `method` on `node` once and require JSON-RPC `-32601 Method not found`.
fn assert_method_not_found<Params>(node: &str, method: &str, params: Params) -> eyre::Result<()>
where
    Params: ToRpcParams + Send + Clone + Debug,
{
    // one attempt: a retry would only repeat the refusal
    let report = match call_rpc::<Value, _, _>(node, method, params, 0, method) {
        Ok(value) => eyre::bail!("{method} on {node} returned {value}, expected method not found"),
        Err(report) => report,
    };
    match report.downcast_ref::<ClientError>() {
        Some(ClientError::Call(error)) if error.code() == METHOD_NOT_FOUND => {
            assert_eq!(error.message(), "Method not found", "{method} on {node}");
            Ok(())
        }
        _ => Err(report.wrap_err(format!("{method} on {node}: expected {METHOD_NOT_FOUND}"))),
    }
}

/// Parse a JSON string holding an address.
fn parse_address(value: &Value) -> Option<Address> {
    value.as_str()?.parse().ok()
}

/// Parse a JSON string holding a `0x`-prefixed hex quantity.
fn hex_u128(value: &Value) -> Option<u128> {
    u128::from_str_radix(value.as_str()?.strip_prefix("0x")?, 16).ok()
}

/// Whether a JSON transaction hash names `tx_hash` (hex case ignored).
fn is_tx(value: &Value, tx_hash: &str) -> bool {
    value.as_str().is_some_and(|hash| hash.eq_ignore_ascii_case(tx_hash))
}
