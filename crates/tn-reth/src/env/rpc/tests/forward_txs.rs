//! `--forward-txs` through the production RPC registration (`RethEnv::get_rpc_server`).

use super::*;
use crate::rpc_tx_forward::{
    parse_forward_targets,
    tests::{FakeUpstream, Reply},
    TxForwardConfig,
};
use jsonrpsee::{types::ErrorObject, MethodsError};
use std::collections::BTreeSet;

/// A temp env over `chain` that forwards submissions to `targets`, with `--rpc.txfeecap`
/// `cap_wei` (0 disables the cap).
fn forwarding_env(
    chain: Arc<RethChainSpec>,
    targets: &str,
    cap_wei: u128,
    rpc_args: reth::args::RpcServerArgs,
    task_manager: &TaskManager,
    tmp_dir: &TempDir,
) -> eyre::Result<RethEnv> {
    let tx_forward =
        TxForwardConfig { targets: Some(parse_forward_targets(targets)?), sanitize: false };
    RethEnv::new_for_temp_chain_with_tx_forward(
        chain,
        tmp_dir.path(),
        task_manager,
        None,
        reth::args::RpcServerArgs { rpc_tx_fee_cap: cap_wei, ..rpc_args },
        tx_forward,
    )
}

/// Worker `worker_id`'s production RPC server on `reth_env`, with its own pool.
fn worker_server(
    reth_env: &RethEnv,
    accumulator: &GasAccumulator,
    worker_id: WorkerId,
) -> eyre::Result<(RpcServer, WorkerTxPool)> {
    let pool = reth_env.init_txn_pool(accumulator.base_fee(worker_id))?;
    let server = reth_env.get_rpc_server(
        pool.clone(),
        WorkerNetwork::new_for_test(reth_env.chainspec()),
        accumulator.worker_base_fee(worker_id),
        RpcModule::new(()),
    )?;
    Ok((server, pool))
}

/// A signed transfer at 7 wei/gas from the factory's funded account: raw bytes and hash.
fn transfer(
    chain: &Arc<RethChainSpec>,
    factory: &mut TransactionFactory,
    gas_limit: u64,
) -> (Bytes, B256) {
    let tx = factory.create_eip1559(
        chain.clone(),
        Some(gas_limit),
        7,
        Some(Address::ZERO),
        U256::from(100),
        Bytes::new(),
    );
    (Bytes::from(tx.encoded_2718()), *tx.hash())
}

/// The handlers are swapped in place: a forwarding server exposes exactly the method set a
/// non-forwarding server does, the two submission methods included. Which of those methods
/// reach the upstream is pinned by [`test_read_methods_are_never_forwarded`].
#[tokio::test]
async fn test_forwarder_module_exposes_only_submission_methods() -> eyre::Result<()> {
    let task_manager = TaskManager::default();
    let accumulator = GasAccumulator::new(1);
    let upstream = FakeUpstream::start(Reply::Hash(B256::ZERO)).await?;
    let forwarding_dir = TempDir::new()?;
    let forwarding_reth = forwarding_env(
        Arc::new(test_genesis().into()),
        &upstream.url,
        0,
        Default::default(),
        &task_manager,
        &forwarding_dir,
    )?;
    let plain_dir = TempDir::new()?;
    let plain_env = RethEnv::new_for_temp_chain_with_rpc_args(
        Arc::new(test_genesis().into()),
        plain_dir.path(),
        &task_manager,
        None,
        Default::default(),
    )?;

    let names = |server: &RpcServer| -> BTreeSet<&'static str> {
        server.methods_by(|_| true).method_names().collect()
    };
    let forwarding = names(&worker_server(&forwarding_reth, &accumulator, 0)?.0);
    let plain = names(&worker_server(&plain_env, &accumulator, 0)?.0);

    assert_eq!(forwarding, plain);
    assert!(forwarding.contains("eth_sendRawTransaction"));
    assert!(forwarding.contains("eth_sendRawTransactionSync"));
    Ok(())
}

/// Reads and `eth_sendTransaction` stay local; only `eth_sendRawTransaction` reaches the
/// upstream. The upstream serves every method name the node exposes, so a leaked call would be
/// recorded rather than refused as an unknown method.
#[tokio::test]
async fn test_read_methods_are_never_forwarded() -> eyre::Result<()> {
    let task_manager = TaskManager::default();
    let accumulator = GasAccumulator::new(1);
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let mut factory = TransactionFactory::new();
    let (raw, hash) = transfer(&chain, &mut factory, 21_000);

    // learn the node's method names from a non-forwarding server: the previous test pins that
    // forwarding exposes the same set
    let plain_dir = TempDir::new()?;
    let plain_env = RethEnv::new_for_temp_chain_with_rpc_args(
        chain.clone(),
        plain_dir.path(),
        &task_manager,
        None,
        Default::default(),
    )?;
    let names: Vec<&'static str> =
        worker_server(&plain_env, &accumulator, 0)?.0.methods_by(|_| true).method_names().collect();
    let upstream = FakeUpstream::serving(names, Reply::Hash(hash)).await?;

    let tmp_dir = TempDir::new()?;
    let reth_env =
        forwarding_env(chain, &upstream.url, 0, Default::default(), &task_manager, &tmp_dir)?;
    let methods = worker_server(&reth_env, &accumulator, 0)?.0.methods_by(|_| true);

    let sender = factory.address();
    let call = serde_json::json!({ "from": sender, "to": Address::ZERO, "value": "0x1" });
    let reads = [
        ("eth_blockNumber", rpc_params![]),
        ("eth_chainId", rpc_params![]),
        ("eth_getBalance", rpc_params![sender, "latest"]),
        ("eth_getTransactionCount", rpc_params![sender, "latest"]),
        ("eth_getTransactionCount", rpc_params![sender, "pending"]),
        ("eth_call", rpc_params![call.clone(), "latest"]),
        ("eth_estimateGas", rpc_params![call.clone()]),
        ("eth_gasPrice", rpc_params![]),
        ("eth_feeHistory", rpc_params!["0x1", "latest", Vec::<f64>::new()]),
        ("eth_getTransactionByHash", rpc_params![hash]),
    ];
    for (method, params) in reads {
        // an answer or a local error both stay local; only the upstream's recorder matters
        let _ = methods.call::<_, serde_json::Value>(method, params).await;
    }
    methods
        .call::<_, B256>("eth_sendTransaction", rpc_params![call])
        .await
        .expect_err("no local signer, and the request is not forwarded");
    assert!(upstream.calls().is_empty(), "reads reached the upstream: {:?}", upstream.methods());

    let accepted: B256 = methods.call("eth_sendRawTransaction", rpc_params![raw]).await?;
    assert_eq!(accepted, hash);
    assert_eq!(upstream.methods(), ["eth_sendRawTransaction"]);
    Ok(())
}

/// Forward-only: the accepted transaction never enters the observer's pool, so its batch
/// builder cannot send it a second time.
#[tokio::test]
async fn test_forwarded_tx_not_inserted_in_local_pool() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let (raw, hash) = transfer(&chain, &mut TransactionFactory::new(), 21_000);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;
    let reth_env =
        forwarding_env(chain, &upstream.url, 0, Default::default(), &task_manager, &tmp_dir)?;
    let (server, pool) = worker_server(&reth_env, &GasAccumulator::new(1), 0)?;

    let accepted: B256 =
        server.methods_by(|_| true).call("eth_sendRawTransaction", rpc_params![raw]).await?;

    assert_eq!(accepted, hash);
    assert_eq!(upstream.calls().len(), 1);
    assert_eq!(pool.pool_size().pending, 0);
    assert!(pool.get(&hash).is_none());
    Ok(())
}

/// The sync method forwards a plain `eth_sendRawTransaction` and passes the upstream's error
/// through unchanged.
#[tokio::test]
async fn test_sync_submission_forwards_plain_send_raw_transaction() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let (raw, _) = transfer(&chain, &mut TransactionFactory::new(), 21_000);
    let error = ErrorObject::owned(-32000, "nonce too low", Some(serde_json::json!({"next": 5})));
    let upstream = FakeUpstream::start(Reply::Error(error.clone())).await?;
    let reth_env =
        forwarding_env(chain, &upstream.url, 0, Default::default(), &task_manager, &tmp_dir)?;
    let (server, _) = worker_server(&reth_env, &GasAccumulator::new(1), 0)?;

    let err = server
        .methods_by(|_| true)
        .call::<_, serde_json::Value>("eth_sendRawTransactionSync", rpc_params![raw])
        .await
        .expect_err("the upstream error comes back");

    assert!(matches!(err, MethodsError::JsonRpc(ref e) if *e == error), "unexpected: {err:?}");
    assert_eq!(upstream.methods(), ["eth_sendRawTransaction"]);
    Ok(())
}

/// The HTTP registration gets the forwarder too. `methods_by` unions transports and keeps the
/// first occurrence, so serve HTTP alone and check its registration directly.
#[tokio::test]
async fn test_http_transport_gets_the_forwarder() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let (raw, hash) = transfer(&chain, &mut TransactionFactory::new(), 21_000);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;
    let rpc_args = reth::args::RpcServerArgs { http: true, ipcdisable: true, ..Default::default() };
    let reth_env = forwarding_env(chain, &upstream.url, 0, rpc_args, &task_manager, &tmp_dir)?;
    let (server, pool) = worker_server(&reth_env, &GasAccumulator::new(1), 0)?;

    let accepted: B256 = server
        .methods_by(|name| name.starts_with("eth_send"))
        .call("eth_sendRawTransaction", rpc_params![raw])
        .await?;

    assert_eq!(accepted, hash);
    assert_eq!(upstream.calls().len(), 1);
    assert_eq!(pool.pool_size().pending, 0);
    Ok(())
}

/// Every worker's server shares one forwarder: a demotion caused through worker 0 still holds
/// when worker 1 submits.
#[tokio::test]
async fn test_worker_servers_share_one_forwarder() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let mut factory = TransactionFactory::new();
    let (first_raw, first_hash) = transfer(&chain, &mut factory, 21_000);
    let (second_raw, _) = transfer(&chain, &mut factory, 21_000);
    let flaky = FakeUpstream::start(Reply::Garbage).await?;
    let healthy = FakeUpstream::start(Reply::Hash(first_hash)).await?;
    let targets = format!("{},{}", flaky.url, healthy.url);
    let reth_env = forwarding_env(chain, &targets, 0, Default::default(), &task_manager, &tmp_dir)?;
    let accumulator = GasAccumulator::new(2);
    let (worker_zero, _) = worker_server(&reth_env, &accumulator, 0)?;
    let (worker_one, _) = worker_server(&reth_env, &accumulator, 1)?;

    let _: B256 = worker_zero
        .methods_by(|_| true)
        .call("eth_sendRawTransaction", rpc_params![first_raw])
        .await?;
    assert_eq!((flaky.calls().len(), healthy.calls().len()), (1, 1));

    let _: B256 = worker_one
        .methods_by(|_| true)
        .call("eth_sendRawTransaction", rpc_params![second_raw])
        .await?;
    assert_eq!(
        (flaky.calls().len(), healthy.calls().len()),
        (1, 2),
        "worker 1 sees the demotion worker 0 caused"
    );
    Ok(())
}

/// With the cap off (the default), the client's bytes reach the upstream untouched, even bytes
/// that are not a transaction, and the upstream's verdict comes back unchanged.
#[tokio::test]
async fn test_fast_path_forwards_bytes_untouched() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let error = ErrorObject::owned(-32602, "failed to decode signed transaction", None::<()>);
    let upstream = FakeUpstream::start(Reply::Error(error.clone())).await?;
    let reth_env = forwarding_env(
        Arc::new(test_genesis().into()),
        &upstream.url,
        0,
        Default::default(),
        &task_manager,
        &tmp_dir,
    )?;
    let (server, _) = worker_server(&reth_env, &GasAccumulator::new(1), 0)?;
    let junk = Bytes::from_static(b"not a transaction");

    let err = server
        .methods_by(|_| true)
        .call::<_, B256>("eth_sendRawTransaction", rpc_params![junk.clone()])
        .await
        .expect_err("the upstream refuses the junk");

    assert!(matches!(err, MethodsError::JsonRpc(ref e) if *e == error), "unexpected: {err:?}");
    assert_eq!(
        upstream.calls(),
        [("eth_sendRawTransaction".to_string(), format!("[\"{junk}\"]"))],
        "forwarded byte for byte"
    );
    Ok(())
}

/// `--rpc.txfeecap` gates forwarding exactly as it gates the local pool: an over-cap
/// transaction gets reth's cap error and no target sees it.
#[tokio::test]
async fn test_fee_cap_applies_before_forward_without_sanitize() -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let mut factory = TransactionFactory::new();
    // cap 200,000 wei: 21,000 gas at 7 wei/gas is under it, 1,000,000 gas is over
    let (under_raw, under_hash) = transfer(&chain, &mut factory, 21_000);
    let (over_raw, _) = transfer(&chain, &mut factory, 1_000_000);
    let upstream = FakeUpstream::start(Reply::Hash(under_hash)).await?;
    let reth_env =
        forwarding_env(chain, &upstream.url, 200_000, Default::default(), &task_manager, &tmp_dir)?;
    let methods = worker_server(&reth_env, &GasAccumulator::new(1), 0)?.0.methods_by(|_| true);

    let err = methods
        .call::<_, B256>("eth_sendRawTransaction", rpc_params![over_raw.clone()])
        .await
        .expect_err("over the cap");
    assert!(
        err.to_string().contains("tx fee (7000000 wei) exceeds the configured cap (200000 wei)"),
        "unexpected error: {err}"
    );
    let err = methods
        .call::<_, serde_json::Value>("eth_sendRawTransactionSync", rpc_params![over_raw])
        .await
        .expect_err("over the cap on the sync method");
    assert!(err.to_string().contains("exceeds the configured cap"), "unexpected error: {err}");
    assert!(upstream.calls().is_empty(), "an over-cap transaction must not be forwarded");

    let accepted: B256 = methods.call("eth_sendRawTransaction", rpc_params![under_raw]).await?;
    assert_eq!(accepted, under_hash);
    assert_eq!(upstream.calls().len(), 1);
    Ok(())
}
