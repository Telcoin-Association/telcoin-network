//! Namespace selection, and one call into every servable namespace, through the production RPC
//! registration.
//!
//! Every server here is built from parsed CLI arguments through [`RethConfig::new`], the path an
//! operator's `--http.api`/`--ws.api` takes. A temp-chain env built from hand-made
//! `RpcServerArgs` skips the selection rewrite in `RethConfig::new`, and with it the `tn` entry.

use super::*;
use crate::{
    payload::TNPayload,
    test_utils::{consensus_output_for_tests, execute_payload_and_update_canonical_chain},
    RethCommand, RethConfig,
};
use clap::Parser as _;
use jsonrpsee::MethodsError;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Stand-in for TN's `tn_*` module, with one method, `tn_probe`, that answers `"ok"`.
///
/// The real module lives in `tn-rpc`, which tn-reth cannot depend on, and its methods need a
/// deployed ConsensusRegistry. Which transports receive the `tn_*` methods is decided by the
/// selection alone, so a probe proves the merge; the real methods are covered end to end.
pub(super) fn probe_tn() -> RpcModule<()> {
    let mut module = RpcModule::new(());
    module.register_method("tn_probe", |_, _, _| "ok").expect("tn_probe registers once");
    module
}

/// A server built from CLI arguments the way the node builds one, with the env it serves.
struct CliServer {
    /// The built transports.
    server: RpcServer,
    /// The temp-chain env behind the server, for mining blocks.
    env: RethEnv,
    /// The chain the env was built from.
    chain: Arc<RethChainSpec>,
}

/// Parse `args` as the node CLI does, resolve them through [`RethConfig::new`], and build the
/// RPC server on a temp chain with [`probe_tn`] in the `tn` slot.
///
/// `with_unused_ports` is set, so every port is 0; the tests call the registered methods
/// directly and never bind a socket.
fn server_from_cli(
    args: &[&str],
    task_manager: &TaskManager,
    tmp_dir: &TempDir,
) -> eyre::Result<CliServer> {
    init_reth_defaults();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let command =
        RethCommand::try_parse_from(std::iter::once("tn-reth").chain(args.iter().copied()))?;
    let config = RethConfig::new(command, None, tmp_dir.path(), true, chain.clone());
    let env = RethEnv::new_for_temp_chain_with_rpc_args(
        chain.clone(),
        tmp_dir.path(),
        task_manager,
        None,
        config.0.rpc,
    )?;
    let pool = env.init_txn_pool(BaseFeeContainer::default())?;
    let network = WorkerNetwork::new_for_test(env.chainspec());
    let server =
        env.get_rpc_server(pool, network, GasAccumulator::new(1).worker_base_fee(0), probe_tn())?;
    Ok(CliServer { server, env, chain })
}

/// Mine block 1 with a 21,000-gas transfer of 100 wei to `Address::ZERO` and return its hash.
fn mine_transfer(served: &CliServer) -> eyre::Result<B256> {
    let tx = TransactionFactory::new().create_eip1559(
        served.chain.clone(),
        Some(21_000),
        7,
        Some(Address::ZERO),
        U256::from(100),
        Bytes::new(),
    );
    let output = consensus_output_for_tests(1, 0, 1, false);
    let payload = TNPayload::new_for_test(served.chain.sealed_genesis_header(), &output);
    execute_payload_and_update_canonical_chain(&served.env, payload, vec![tx.encoded_2718()])?;
    Ok(*tx.hash())
}

/// The namespace names `rpc_modules` reports on `methods`.
async fn rpc_module_names(methods: &Methods) -> eyre::Result<BTreeSet<String>> {
    let modules: BTreeMap<String, String> = methods.call("rpc_modules", rpc_params![]).await?;
    Ok(modules.into_keys().collect())
}

/// The names in `names` as an owned set, for comparing with [`rpc_module_names`].
fn name_set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// Deserialize `value[key]`, failing with the key and the whole value when it does not fit.
fn field<T: DeserializeOwned>(value: &Value, key: &str) -> eyre::Result<T> {
    serde_json::from_value(value[key].clone())
        .map_err(|e| eyre::eyre!("field `{key}` of {value}: {e}"))
}

/// The JSON-RPC error code and message of a call that must fail.
fn rpc_error<T: std::fmt::Debug>(result: Result<T, MethodsError>) -> eyre::Result<(i32, String)> {
    match result {
        Err(MethodsError::JsonRpc(error)) => Ok((error.code(), error.message().to_string())),
        other => Err(eyre::eyre!("expected a JSON-RPC error, got {other:?}")),
    }
}

/// The length of a JSON array result.
fn array_len(value: &Value) -> eyre::Result<usize> {
    value.as_array().map(Vec::len).ok_or_else(|| eyre::eyre!("expected an array, got {value}"))
}

/// A bare `--http` serves the default set, `tn` included.
#[tokio::test]
async fn tn_namespace_is_served_by_default() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let served = server_from_cli(&["--http", "--ipcdisable"], &task_manager, &tmp_dir)?;
    let http = served.server.http_methods(|_| true).expect("http is enabled");

    assert!(http.method("tn_probe").is_some(), "a bare --http serves the tn namespace");
    assert_eq!(rpc_module_names(&http).await?, name_set(&["eth", "net", "rpc", "tn", "web3"]));
    Ok(())
}

/// An explicit list is literal: without `tn` it serves no `tn_*` method.
#[tokio::test]
async fn explicit_selection_without_tn_serves_no_tn_methods() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let served = server_from_cli(
        &["--http", "--http.api", "eth,net", "--ipcdisable"],
        &task_manager,
        &tmp_dir,
    )?;
    let tn_methods = served.server.http_methods(|name| name.starts_with("tn_")).expect("http");

    assert_eq!(tn_methods.method_names().count(), 0, "eth,net must serve no tn_* method");
    let http = served.server.http_methods(|_| true).expect("http is enabled");
    assert!(http.method("eth_chainId").is_some(), "the listed eth namespace is still served");
    let (code, message) = rpc_error(http.call::<_, String>("tn_probe", rpc_params![]).await)?;
    assert_eq!((code, message.as_str()), (-32601, "Method not found"));
    Ok(())
}

/// The `tn` selection is resolved per transport, not once for the node.
#[tokio::test]
async fn tn_selection_is_per_transport() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let served = server_from_cli(
        &["--http", "--http.api", "eth", "--ws", "--ws.api", "eth,tn", "--ipcdisable"],
        &task_manager,
        &tmp_dir,
    )?;
    let http = served.server.http_methods(|_| true).expect("http is enabled");
    let ws = served.server.ws_methods(|_| true).expect("ws is enabled");

    assert!(http.method("tn_probe").is_none(), "http selected eth only");
    assert!(ws.method("tn_probe").is_some(), "ws selected tn");
    assert!(http.method("eth_chainId").is_some() && ws.method("eth_chainId").is_some());
    Ok(())
}

/// Every namespace an operator can select answers on a chain with one transfer in block 1.
///
/// Block 1 is mined because reth's `debug_traceBlock*` needs the parent's state and fails on
/// genesis, while `trace_block` returns early for a block without transactions.
#[tokio::test]
async fn every_namespace_serves_on_tn() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let served = server_from_cli(
        &["--http", "--http.api", "eth,net,web3,rpc,tn,debug,trace", "--ipcdisable"],
        &task_manager,
        &tmp_dir,
    )?;
    let hash = mine_transfer(&served)?;
    let http = served.server.http_methods(|_| true).expect("http is enabled");

    // web3, net, rpc and tn: the client version and peer count are the worker's values
    let client_version: String = http.call("web3_clientVersion", rpc_params![]).await?;
    assert_eq!(client_version, "test", "web3_clientVersion is the WorkerNetwork version");
    let sha3: B256 =
        http.call("web3_sha3", rpc_params![Bytes::from_static(b"hello world")]).await?;
    assert_eq!(
        sha3,
        "0x47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad".parse::<B256>()?
    );
    let net_version: String = http.call("net_version", rpc_params![]).await?;
    assert_eq!(net_version, served.chain.chain.id().to_string());
    let listening: bool = http.call("net_listening", rpc_params![]).await?;
    assert!(listening);
    let peer_count: U64 = http.call("net_peerCount", rpc_params![]).await?;
    assert_eq!(peer_count, U64::ZERO, "net_peerCount is the worker's peer count");
    assert_eq!(
        rpc_module_names(&http).await?,
        name_set(&["debug", "eth", "net", "rpc", "tn", "trace", "web3"])
    );
    let probe: String = http.call("tn_probe", rpc_params![]).await?;
    assert_eq!(probe, "ok");

    // debug
    let raw_header: Bytes = http.call("debug_getRawHeader", rpc_params!["0x0"]).await?;
    assert!(!raw_header.is_empty());
    let raw_block: Bytes = http.call("debug_getRawBlock", rpc_params!["0x1"]).await?;
    assert!(!raw_block.is_empty());
    let call_frame: Value = http
        .call("debug_traceTransaction", rpc_params![hash, json!({"tracer": "callTracer"})])
        .await?;
    assert_eq!(field::<String>(&call_frame, "type")?, "CALL");
    assert_eq!(field::<Address>(&call_frame, "to")?, Address::ZERO);
    assert_eq!(field::<U256>(&call_frame, "value")?, U256::from(100));
    assert_eq!(field::<U64>(&call_frame, "gasUsed")?, U64::from(21_000));
    let struct_logs: Value =
        http.call("debug_traceTransaction", rpc_params![hash, json!({})]).await?;
    assert!(!field::<bool>(&struct_logs, "failed")?);
    assert_eq!(field::<u64>(&struct_logs, "gas")?, 21_000);
    assert_eq!(field::<Vec<Value>>(&struct_logs, "structLogs")?, Vec::<Value>::new());
    let block_traces: Value = http
        .call("debug_traceBlockByNumber", rpc_params!["0x1", json!({"tracer": "callTracer"})])
        .await?;
    // one entry: the pre-execution system calls are not transactions and are not traced
    assert_eq!(array_len(&block_traces)?, 1, "{block_traces}");
    assert_eq!(field::<B256>(&block_traces[0], "txHash")?, hash);
    // genesis has no parent: reth looks up the zero parent hash and reports it missing
    let (code, message) = rpc_error(
        http.call::<_, Value>(
            "debug_traceBlockByNumber",
            rpc_params!["0x0", json!({"tracer": "callTracer"})],
        )
        .await,
    )?;
    assert_eq!(code, -32001, "{message}");
    assert!(message.starts_with("block not found"), "unexpected error: {message}");

    // trace
    let tx_traces: Value = http.call("trace_transaction", rpc_params![hash]).await?;
    assert_eq!(array_len(&tx_traces)?, 1, "{tx_traces}");
    assert_eq!(field::<String>(&tx_traces[0], "type")?, "call");
    assert_eq!(field::<String>(&tx_traces[0]["action"], "callType")?, "call");
    // exactly the transfer: no system call and no proof-of-work reward trace
    let block_1: Value = http.call("trace_block", rpc_params!["0x1"]).await?;
    assert_eq!(array_len(&block_1)?, 1, "{block_1}");
    assert_eq!(field::<B256>(&block_1[0], "transactionHash")?, hash);
    let block_0: Value = http.call("trace_block", rpc_params!["0x0"]).await?;
    assert_eq!(block_0, json!([]));
    let filtered: Value = http
        .call("trace_filter", rpc_params![json!({"fromBlock": "0x0", "toBlock": "0x1"})])
        .await?;
    assert_eq!(array_len(&filtered)?, 1, "{filtered}");
    Ok(())
}

/// `--rpc.max-trace-filter-blocks` reaches the trace namespace: with a limit of 0 a two-block
/// range is refused while a one-block range is still served.
#[tokio::test]
async fn trace_filter_honours_max_trace_filter_blocks() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let served = server_from_cli(
        &["--http", "--http.api", "trace", "--rpc.max-trace-filter-blocks", "0", "--ipcdisable"],
        &task_manager,
        &tmp_dir,
    )?;
    mine_transfer(&served)?;
    let http = served.server.http_methods(|_| true).expect("http is enabled");

    let (code, message) = rpc_error(
        http.call::<_, Value>(
            "trace_filter",
            rpc_params![json!({"fromBlock": "0x0", "toBlock": "0x1"})],
        )
        .await,
    )?;
    // reth's message names its own default of 100 whatever the configured limit is
    assert_eq!(code, -32602, "{message}");
    assert!(message.starts_with("Block range too large"), "unexpected error: {message}");
    let single: Value = http
        .call("trace_filter", rpc_params![json!({"fromBlock": "0x1", "toBlock": "0x1"})])
        .await?;
    assert_eq!(array_len(&single)?, 1, "{single}");
    Ok(())
}
