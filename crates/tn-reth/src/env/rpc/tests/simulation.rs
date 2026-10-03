//! Regression coverage for epoch-priced simulations through the production RPC registration.

use super::*;
use alloy::{
    consensus::{SignableTransaction as _, TxEip1559, TxEnvelope, TxLegacy},
    eips::{eip2930::AccessListResult, BlockId, BlockNumberOrTag},
    rpc::types::{
        state::{AccountOverride, StateOverride},
        BlockOverrides, TransactionRequest,
    },
    signers::SignerSync as _,
};
use futures::future::try_join_all;
use reth_rpc_eth_types::builder::config::PendingBlockKind;
use serde_json::{json, Value};

/// A funded transfer with an explicit EIP-1559 cap and zero priority fee.
fn priced_transfer(fee_cap: u128) -> TransactionRequest {
    TransactionRequest {
        from: Some(TransactionFactory::new().address()),
        to: Some(Address::repeat_byte(0x91).into()),
        max_fee_per_gas: Some(fee_cap),
        max_priority_fee_per_gas: Some(0),
        ..Default::default()
    }
}

/// Exercise the default block selection as well as both explicit tip tags.
fn tip_blocks() -> [Option<BlockId>; 3] {
    [None, Some(BlockId::latest()), Some(BlockId::pending())]
}

/// All three simulation methods must accept the same valid, explicitly priced transfer.
async fn assert_transfer_simulates(
    methods: &Methods,
    request: &TransactionRequest,
    block: Option<BlockId>,
) -> eyre::Result<()> {
    let output: Bytes = methods.call("eth_call", rpc_params![request, block]).await?;
    assert!(output.is_empty());
    let gas: U256 = methods.call("eth_estimateGas", rpc_params![request, block]).await?;
    assert_eq!(gas, U256::from(21_000));
    let access: AccessListResult =
        methods.call("eth_createAccessList", rpc_params![request, block]).await?;
    assert_eq!(access.gas_used, U256::from(21_000));
    assert!(access.access_list.is_empty());
    assert!(access.error.is_none(), "access-list execution failed: {:?}", access.error);
    Ok(())
}

/// A cap below the selected environment's fee must retain reth's fee-cap error.
async fn assert_simulation_cap_rejected(
    methods: &Methods,
    request: &TransactionRequest,
    block: Option<BlockId>,
) -> eyre::Result<()> {
    try_join_all(["eth_call", "eth_estimateGas", "eth_createAccessList"].map(
        |method| async move {
            let result =
                methods.call::<_, serde_json::Value>(method, rpc_params![request, block]).await;
            assert!(
                result.as_ref().err().is_some_and(|error| {
                    error.to_string().contains("max fee per gas less than block base fee")
                }),
                "{method} returned {result:?}"
            );
            Ok::<(), eyre::Report>(())
        },
    ))
    .await?;
    Ok(())
}

/// A downward fee change is honored on HTTP and WebSocket registrations independently of IPC.
#[tokio::test]
async fn test_rpc_simulation_epoch_fee_below_header() -> eyre::Result<()> {
    init_reth_defaults();
    let transports = [
        reth::args::RpcServerArgs { http: true, ipcdisable: true, ..Default::default() },
        reth::args::RpcServerArgs { ws: true, ipcdisable: true, ..Default::default() },
    ];
    try_join_all(transports.into_iter().map(|rpc_args| async move {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let methods = epoch_fee_methods(7, 1_000, rpc_args, &task_manager, &tmp_dir)?;
        let request = priced_transfer(7);
        try_join_all(
            tip_blocks().map(|block| assert_transfer_simulates(&methods, &request, block)),
        )
        .await?;
        // An omitted parameter must select the same default as an explicit null.
        let gas: U256 = methods.call("eth_estimateGas", rpc_params![request]).await?;
        assert_eq!(gas, U256::from(21_000));
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// An upward fee change must not silently accept the preceding header's cheaper cap.
#[tokio::test]
async fn test_rpc_simulation_rejects_cap_below_epoch_fee() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(1_000, 7, Default::default(), &task_manager, &tmp_dir)?;
    let request = priced_transfer(7);
    try_join_all(
        tip_blocks().map(|block| assert_simulation_cap_rejected(&methods, &request, block)),
    )
    .await?;
    Ok(())
}

/// Explicit block numbers and hashes retain the header fee even when the live epoch is cheaper.
#[tokio::test]
async fn test_rpc_simulation_preserves_historical_fee() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
    let mut genesis = tn_types::test_genesis_at(0);
    genesis.base_fee_per_gas = Some(1_000);
    let chain: RethChainSpec = genesis.into();
    let blocks =
        [BlockId::Number(BlockNumberOrTag::Number(0)), BlockId::from(chain.genesis_hash())];
    let request = priced_transfer(7);
    try_join_all(
        blocks.map(|block| assert_simulation_cap_rejected(&methods, &request, Some(block))),
    )
    .await?;
    Ok(())
}

/// Fee correction must reach the EVM and preserve the client's priority fee and state overrides.
#[tokio::test]
async fn test_rpc_simulation_preserves_gasprice_and_state_override() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
    let request = TransactionRequest { max_priority_fee_per_gas: Some(2), ..priced_transfer(9) };
    // Revert unless BASEFEE == 7 and GASPRICE == 9. This also proves all methods
    // execute the overridden code instead of the recipient's empty historical code.
    let overrides = StateOverride::from_iter([(
        Address::repeat_byte(0x91),
        AccountOverride {
            code: Some(Bytes::from_static(&[
                0x48, 0x60, 0x07, 0x14, 0x3a, 0x60, 0x09, 0x14, 0x16, 0x60, 0x0f, 0x57, 0x5f, 0x5f,
                0xfd, 0x5b, 0x00,
            ])),
            ..Default::default()
        },
    )]);
    try_join_all(tip_blocks().map(|block| {
        let request = &request;
        let overrides = &overrides;
        let methods = &methods;
        async move {
            let output: Bytes =
                methods.call("eth_call", rpc_params![request, block, overrides]).await?;
            assert!(output.is_empty());
            let gas: U256 =
                methods.call("eth_estimateGas", rpc_params![request, block, overrides]).await?;
            assert!(gas > U256::from(21_000));
            let access: AccessListResult = methods
                .call("eth_createAccessList", rpc_params![request, block, overrides])
                .await?;
            assert!(access.gas_used > U256::from(21_000));
            assert!(access.error.is_none(), "access-list execution failed: {:?}", access.error);
            Ok::<(), eyre::Report>(())
        }
    }))
    .await?;
    Ok(())
}

/// An explicit block base fee, including zero, wins over the worker's live fee.
#[tokio::test]
async fn test_rpc_simulation_preserves_block_override() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
    let request = priced_transfer(1_000);
    let state = StateOverride::from_iter([(
        Address::repeat_byte(0x91),
        AccountOverride {
            // Return the BASEFEE opcode as a 32-byte word.
            code: Some(Bytes::from_static(&[0x48, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3])),
            ..Default::default()
        },
    )]);
    try_join_all([None, Some(0_u64), Some(23)].map(|fee| {
        let methods = &methods;
        let request = &request;
        let state = &state;
        async move {
            let block = fee.map(|value| {
                Box::new(BlockOverrides { base_fee: Some(U256::from(value)), ..Default::default() })
            });
            let output: Bytes = methods
                .call("eth_call", rpc_params![request, BlockId::latest(), state, block])
                .await?;
            assert_eq!(output.len(), 32);
            assert_eq!(U256::from_be_slice(&output), U256::from(fee.unwrap_or(7)));
            Ok::<(), eyre::Report>(())
        }
    }))
    .await?;
    Ok(())
}

/// Filling an explicitly priced request shares the corrected pending estimation path.
#[tokio::test]
async fn test_rpc_simulation_fill_preserves_explicit_cap() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
    let filled = methods.call("eth_fillTransaction", rpc_params![priced_transfer(7)]).await?;
    assert_filled_transfer(filled, 7, 0)
}

/// Enable eth and debug on a single transport, with explicit pending-block behavior.
fn remaining_rpc_args(http: bool, pending: PendingBlockKind) -> reth::args::RpcServerArgs {
    let modules = vec![RethRpcModule::Eth, RethRpcModule::Debug];
    reth::args::RpcServerArgs {
        http,
        ws: !http,
        ipcdisable: true,
        http_api: Some(modules.clone().into()),
        ws_api: Some(modules.into()),
        rpc_pending_block: pending,
        ..Default::default()
    }
}

/// Requests and output locations for the production handlers that simulate unsigned calls.
fn remaining_requests(
    request: &TransactionRequest,
    block: Option<BlockId>,
    block_override: &Option<BlockOverrides>,
    state_override: &Option<StateOverride>,
) -> [(&'static str, Vec<Value>, &'static str); 4] {
    let bundle = json!({"transactions": [request], "blockOverride": block_override});
    let context = json!({"blockNumber": block});
    let opts = json!({
        "tracer": "callTracer", "blockOverrides": block_override,
        "stateOverrides": state_override
    });
    [
        (
            "eth_callMany",
            vec![json!([bundle]), context.clone(), json!(state_override)],
            "/0/0/value",
        ),
        (
            "eth_simulateV1",
            vec![
                json!({
                    "blockStateCalls": [{
                        "calls": [request], "blockOverrides": block_override,
                        "stateOverrides": state_override
                    }], "validation": true
                }),
                json!(block),
            ],
            "/0/calls/0/returnData",
        ),
        ("debug_traceCall", vec![json!(request), json!(block), opts.clone()], "/output"),
        ("debug_traceCallMany", vec![json!([bundle]), context, opts], "/0/0/output"),
    ]
}

/// Assert execution output for each registered unsigned simulation method.
async fn assert_remaining_output(
    methods: &Methods,
    request: &TransactionRequest,
    block: Option<BlockId>,
    block_override: Option<BlockOverrides>,
    state_override: Option<StateOverride>,
    expected: &str,
) -> eyre::Result<()> {
    try_join_all(remaining_requests(request, block, &block_override, &state_override).map(
        |(method, params, path)| async move {
            let response: Value = methods.call(method, params).await?;
            // Geth omits empty trace output, while eth methods serialize it as "0x".
            let output = response.pointer(path).and_then(Value::as_str).unwrap_or("0x");
            assert_eq!(output, expected, "{method}: {response}");
            if method.starts_with("debug_") {
                assert!(response.pointer(&path.replace("output", "error")).is_none());
            }
            Ok::<(), eyre::Report>(())
        },
    ))
    .await?;
    Ok(())
}

/// All registered handlers accept a cap between the cheaper epoch and the recorded header.
#[tokio::test]
async fn test_rpc_simulation_remaining_fee_decrease() -> eyre::Result<()> {
    init_reth_defaults();
    let cases =
        [(true, 70, 80), (false, 70, 80), (true, 0, u128::from(tn_types::MIN_PROTOCOL_BASE_FEE))];
    try_join_all(cases.map(|(http, epoch_fee, cap)| async move {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let methods = epoch_fee_methods(
            epoch_fee,
            100,
            remaining_rpc_args(http, PendingBlockKind::Full),
            &task_manager,
            &tmp_dir,
        )?;
        let request = priced_transfer(cap);
        try_join_all(
            tip_blocks()
                .map(|block| assert_remaining_output(&methods, &request, block, None, None, "0x")),
        )
        .await?;
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// EIP-1559 caps below the epoch fee cannot use the preceding header's cheaper price.
#[tokio::test]
async fn test_rpc_simulation_remaining_fee_increase() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(
        100,
        70,
        remaining_rpc_args(true, PendingBlockKind::Full),
        &task_manager,
        &tmp_dir,
    )?;
    let request = priced_transfer(80);
    let methods = &methods;
    let request = &request;
    try_join_all(tip_blocks().map(|block| async move {
        try_join_all(remaining_requests(request, block, &None, &None).map(
            |(method, params, _)| async move {
                let result = methods.call::<_, Value>(method, params).await;
                assert!(
                    result.as_ref().err().is_some_and(|err| {
                        err.to_string().contains("max fee per gas less than block base fee")
                    }),
                    "{method}: {result:?}"
                );
                Ok::<(), eyre::Report>(())
            },
        ))
        .await?;
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// Contract code reports the fee actually visible to execution and the client's effective price.
fn fee_probe() -> eyre::Result<(TransactionRequest, StateOverride)> {
    let mut request = priced_transfer(200);
    request.max_priority_fee_per_gas = Some(5);
    let recipient = Address::repeat_byte(0x91);
    let overrides = StateOverride::from_iter([(
        recipient,
        AccountOverride {
            code: Some("0x486000523a60205260406000f3".parse()?),
            ..Default::default()
        },
    )]);
    Ok((request, overrides))
}

/// Compare the two 32-byte ABI words returned by the BASEFEE/GASPRICE probe.
fn fee_probe_output(base_fee: u64, gas_price: u64) -> String {
    format!("0x{base_fee:064x}{gas_price:064x}")
}

/// Historical selections and explicit overrides, including zero, retain their execution fees.
#[tokio::test]
async fn test_rpc_simulation_remaining_fee_environments() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(
        70,
        100,
        remaining_rpc_args(true, PendingBlockKind::Full),
        &task_manager,
        &tmp_dir,
    )?;
    let (request, state) = fee_probe()?;
    let methods = &methods;
    let request = &request;
    let state = &state;
    let epoch_output = fee_probe_output(70, 75);
    try_join_all(tip_blocks().map(|block| {
        assert_remaining_output(methods, request, block, None, Some(state.clone()), &epoch_output)
    }))
    .await?;
    let mut genesis = tn_types::test_genesis_at(0);
    genesis.base_fee_per_gas = Some(100);
    let chain: RethChainSpec = genesis.into();
    let historical_output = fee_probe_output(100, 105);
    try_join_all(
        [
            BlockId::Number(BlockNumberOrTag::Number(0)),
            BlockId::from(chain.genesis_hash()),
            BlockId::Number(BlockNumberOrTag::Safe),
            BlockId::Number(BlockNumberOrTag::Finalized),
        ]
        .map(|block| {
            assert_remaining_output(
                methods,
                request,
                Some(block),
                None,
                Some(state.clone()),
                &historical_output,
            )
        }),
    )
    .await?;
    try_join_all([0, 90].map(|fee| async move {
        let overrides = BlockOverrides {
            base_fee: Some(U256::from(fee)),
            number: Some(U256::from(42)),
            time: Some(777),
            ..Default::default()
        };
        assert_remaining_output(
            methods,
            request,
            Some(BlockId::latest()),
            Some(overrides),
            Some(state.clone()),
            &fee_probe_output(fee, fee + 5),
        )
        .await
    }))
    .await?;
    Ok(())
}

/// Legacy prices remain unchanged; validated simulations enforce them against the epoch fee.
#[tokio::test]
async fn test_rpc_simulation_remaining_legacy_prices() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let (mut request, state) = fee_probe()?;
    request.max_fee_per_gas = None;
    request.max_priority_fee_per_gas = None;
    request.gas_price = Some(80);
    let methods = epoch_fee_methods(
        70,
        100,
        remaining_rpc_args(true, PendingBlockKind::Full),
        &task_manager,
        &tmp_dir,
    )?;
    assert_remaining_output(
        &methods,
        &request,
        Some(BlockId::latest()),
        None,
        Some(state.clone()),
        &fee_probe_output(70, 80),
    )
    .await?;
    let tmp_dir = TempDir::new()?;
    let methods = epoch_fee_methods(
        100,
        70,
        remaining_rpc_args(true, PendingBlockKind::Full),
        &task_manager,
        &tmp_dir,
    )?;
    // Reth disables execution-time fee checks for callMany and debug calls. Preserve
    // their legacy-price behavior, while simulateV1 with validation keeps its check.
    let methods = &methods;
    try_join_all(remaining_requests(&request, Some(BlockId::latest()), &None, &Some(state)).map(
        |(method, params, path)| async move {
            let result = methods.call::<_, Value>(method, params).await;
            if method == "eth_simulateV1" {
                assert!(result.is_err(), "{method}: {result:?}");
            } else {
                let response = result?;
                assert_eq!(response.pointer(path), Some(&json!(fee_probe_output(100, 80))));
            }
            Ok::<(), eyre::Report>(())
        },
    ))
    .await?;
    Ok(())
}

/// Every simulated block receives the fee; validation-disabled blocks keep reth's zero default.
#[tokio::test]
async fn test_rpc_simulation_multiple_blocks_and_validation_disabled() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(
        70,
        100,
        remaining_rpc_args(true, PendingBlockKind::Full),
        &task_manager,
        &tmp_dir,
    )?;
    let (request, state) = fee_probe()?;
    let blocks: Vec<_> =
        (0..3).map(|_| json!({"calls": [request], "stateOverrides": state})).collect();
    let response: Value = methods
        .call(
            "eth_simulateV1",
            rpc_params![
                json!({
                    "blockStateCalls": blocks, "validation": true, "returnFullTransactions": true
                }),
                "latest"
            ],
        )
        .await?;
    let responses = response.as_array().ok_or_else(|| eyre::eyre!("expected simulated blocks"))?;
    assert_eq!(responses.len(), 3);
    responses.iter().enumerate().for_each(|(index, block)| {
        assert_eq!(block.pointer("/calls/0/status"), Some(&json!("0x1")));
        assert_eq!(block.pointer("/calls/0/returnData"), Some(&json!(fee_probe_output(70, 75))));
        assert_eq!(block.get("baseFeePerGas"), Some(&json!("0x46")));
        assert_eq!(block.get("number"), Some(&json!(format!("{:#x}", index + 1))));
    });
    let methods = &methods;
    let request = &request;
    let state = &state;
    try_join_all([None, Some(0), Some(90)].map(|fee| async move {
        let overrides = fee.map(|fee| json!({"baseFeePerGas": format!("{fee:#x}")}));
        let response: Value = methods
            .call(
                "eth_simulateV1",
                rpc_params![
                    json!({
                        "blockStateCalls": [{
                            "calls": [request], "stateOverrides": state, "blockOverrides": overrides
                        }], "validation": false
                    }),
                    "latest"
                ],
            )
            .await?;
        let fee = fee.unwrap_or_default();
        assert_eq!(
            response.pointer("/0/calls/0/returnData"),
            Some(&json!(fee_probe_output(fee, fee + 5)))
        );
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// Fee overrides are independent for dependent bundles and retain reth's shared state.
#[tokio::test]
async fn test_rpc_simulation_multiple_bundles() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(
        70,
        100,
        remaining_rpc_args(true, PendingBlockKind::Full),
        &task_manager,
        &tmp_dir,
    )?;
    let (request, state) = fee_probe()?;
    let bundles: Vec<_> = [None, Some(90), Some(0), None]
        .map(|fee| {
            json!({
                "transactions": [request],
                "blockOverride": fee.map(|fee| json!({"baseFeePerGas": format!("{fee:#x}")}))
            })
        })
        .into_iter()
        .collect();
    let cases = [
        (
            "eth_callMany",
            vec![json!(bundles), json!({"blockNumber": "latest"}), json!(state)],
            "value",
        ),
        (
            "debug_traceCallMany",
            vec![
                json!(bundles),
                json!({"blockNumber": "latest"}),
                json!({"tracer": "callTracer", "stateOverrides": state}),
            ],
            "output",
        ),
    ];
    let methods = &methods;
    try_join_all(cases.map(|(method, params, field)| async move {
        let response: Value = methods.call(method, params).await?;
        let results = response.as_array().ok_or_else(|| eyre::eyre!("expected bundle results"))?;
        assert_eq!(results.len(), 4);
        results.iter().zip([70, 90, 0, 70]).for_each(|(bundle, fee)| {
            assert_eq!(
                bundle.pointer(&format!("/0/{field}")),
                Some(&json!(fee_probe_output(fee, fee + 5))),
                "{method}: {response}"
            );
        });
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// Pending calls preserve default HeaderNotFound errors and work when full pending is enabled.
#[tokio::test]
async fn test_rpc_simulation_default_pending_errors() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods(
        70,
        100,
        remaining_rpc_args(true, PendingBlockKind::None),
        &task_manager,
        &tmp_dir,
    )?;
    let request = priced_transfer(80);
    let methods = &methods;
    try_join_all(remaining_requests(&request, Some(BlockId::pending()), &None, &None).map(
        |(method, params, _)| async move {
            let result = methods.call::<_, Value>(method, params).await;
            if method == "eth_callMany"
                || method == "eth_simulateV1"
                || method == "debug_traceCallMany"
            {
                assert!(
                    result.as_ref().err().is_some_and(|err| {
                        err.to_string().contains("block not found: pending")
                    }),
                    "{method}: {result:?}"
                );
            } else {
                let response = result?;
                assert_eq!(response.get("output").and_then(Value::as_str).unwrap_or("0x"), "0x");
                assert!(response.get("error").is_none());
            }
            Ok::<(), eyre::Report>(())
        },
    ))
    .await?;
    Ok(())
}

/// The two signed fee representations whose validation is owned by reth's bundle handler.
#[derive(Clone, Copy)]
enum BundleFeeStyle {
    /// A legacy gas price is charged without an EIP-1559 priority-fee field.
    Legacy,
    /// An EIP-1559 cap with a five-wei priority fee.
    Dynamic,
}

/// Sign a probe call using the same funded account as the production RPC fixture.
fn signed_bundle_probe(style: BundleFeeStyle, cap: u128) -> eyre::Result<Bytes> {
    let factory = TransactionFactory::new();
    let signer = factory.get_default_signer()?;
    let to = Address::repeat_byte(0x91).into();
    let tx: TxEnvelope = match style {
        BundleFeeStyle::Legacy => {
            let tx = TxLegacy {
                chain_id: Some(2017),
                gas_price: cap,
                gas_limit: 100_000,
                to,
                ..Default::default()
            };
            let signature = signer.sign_hash_sync(&tx.signature_hash())?;
            tx.into_signed(signature).into()
        }
        BundleFeeStyle::Dynamic => {
            let tx = TxEip1559 {
                chain_id: 2017,
                max_fee_per_gas: cap,
                max_priority_fee_per_gas: 5,
                gas_limit: 100_000,
                to,
                ..Default::default()
            };
            let signature = signer.sign_hash_sync(&tx.signature_hash())?;
            tx.into_signed(signature).into()
        }
    };
    Ok(tx.encoded_2718().into())
}

/// Install fee probe code at the bundle destination, since callBundle has no state override.
fn bundle_probe_genesis(header_fee: u64) -> eyre::Result<alloy::genesis::Genesis> {
    let mut genesis = tn_types::test_genesis_at(0);
    genesis.base_fee_per_gas = Some(u128::from(header_fee));
    genesis.alloc.entry(Address::repeat_byte(0x91)).or_default().code =
        Some("0x486000523a60205260406000f3".parse()?);
    Ok(genesis)
}

/// Tip bundles validate signed legacy and EIP-1559 prices against the epoch, in both directions.
#[tokio::test]
async fn test_rpc_simulation_signed_bundle_fee_changes() -> eyre::Result<()> {
    init_reth_defaults();
    try_join_all([(70, 100), (100, 70)].map(|(epoch_fee, header_fee)| async move {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let methods = epoch_fee_methods_from_genesis(
            epoch_fee,
            bundle_probe_genesis(header_fee)?,
            remaining_rpc_args(true, PendingBlockKind::Full),
            &task_manager,
            &tmp_dir,
        )?;
        try_join_all([BundleFeeStyle::Legacy, BundleFeeStyle::Dynamic].map(|style| {
            let methods = &methods;
            async move {
                let tx = signed_bundle_probe(style, 80)?;
                try_join_all(["latest", "pending"].map(|block| {
                    let tx = &tx;
                    async move {
                        let result = methods
                            .call::<_, Value>(
                                "eth_callBundle",
                                rpc_params![json!({
                                    "txs": [tx], "blockNumber": "0x2", "stateBlockNumber": block
                                })],
                            )
                            .await;
                        if epoch_fee > 80 {
                            assert!(
                                result.is_err(),
                                "bundle underpriced at {epoch_fee}: {result:?}"
                            );
                        } else {
                            let response = result?;
                            let price = match style {
                                BundleFeeStyle::Legacy => 80,
                                BundleFeeStyle::Dynamic => 75,
                            };
                            assert_eq!(
                                response.pointer("/results/0/value"),
                                Some(&json!(fee_probe_output(epoch_fee, price)))
                            );
                            assert_eq!(
                                response.pointer("/results/0/gasPrice"),
                                // Bundle gasPrice reports the miner tip, unlike the GASPRICE
                                // opcode.
                                Some(&json!((price - epoch_fee).to_string()))
                            );
                        }
                        Ok::<(), eyre::Report>(())
                    }
                }))
                .await?;
                Ok::<(), eyre::Report>(())
            }
        }))
        .await?;
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// Bundle state selection and explicit overrides, including zero, preserve reth's semantics.
#[tokio::test]
async fn test_rpc_simulation_signed_bundle_overrides_and_history() -> eyre::Result<()> {
    init_reth_defaults();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let methods = epoch_fee_methods_from_genesis(
        70,
        bundle_probe_genesis(100)?,
        remaining_rpc_args(true, PendingBlockKind::None),
        &task_manager,
        &tmp_dir,
    )?;
    let tx = signed_bundle_probe(BundleFeeStyle::Dynamic, 200)?;
    try_join_all(["0x0", "safe", "finalized"].map(|block| {
        let methods = &methods;
        let tx = &tx;
        async move {
            let response: Value = methods
                .call(
                    "eth_callBundle",
                    rpc_params![json!({
                        "txs": [tx], "blockNumber": "0x2", "stateBlockNumber": block
                    })],
                )
                .await?;
            assert_eq!(
                response.pointer("/results/0/value"),
                Some(&json!(fee_probe_output(100, 105)))
            );
            Ok::<(), eyre::Report>(())
        }
    }))
    .await?;
    try_join_all([0, 90].map(|fee| {
        let methods = &methods;
        let tx = &tx;
        async move {
            let response: Value = methods
                .call(
                    "eth_callBundle",
                    rpc_params![json!({
                        "txs": [tx], "blockNumber": "0x2", "stateBlockNumber": "latest",
                        "baseFee": format!("{fee:#x}"), "timestamp": "0x309", "gasLimit": "0x186a0"
                    })],
                )
                .await?;
            assert_eq!(
                response.pointer("/results/0/value"),
                Some(&json!(fee_probe_output(fee, fee + 5)))
            );
            Ok::<(), eyre::Report>(())
        }
    }))
    .await?;
    // Unlike callMany and simulateV1, callBundle can use the pending environment
    // without requiring a recovered pending block when pending-block mode is none.
    let response: Value = methods
        .call(
            "eth_callBundle",
            rpc_params![json!({
                "txs": [tx], "blockNumber": "0x2", "stateBlockNumber": "pending"
            })],
        )
        .await?;
    assert_eq!(response.pointer("/results/0/value"), Some(&json!(fee_probe_output(70, 75))));
    Ok(())
}
