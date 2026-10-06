//! Regression coverage for epoch-priced simulations through the production RPC registration.

use super::*;
use alloy::{
    eips::{eip2930::AccessListResult, BlockId, BlockNumberOrTag},
    rpc::types::{
        state::{AccountOverride, StateOverride},
        BlockOverrides, TransactionRequest,
    },
};
use futures::future::try_join_all;

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
        let (methods, _) = epoch_fee_methods(7, 1_000, rpc_args, &task_manager, &tmp_dir)?;
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
    let (methods, _) = epoch_fee_methods(1_000, 7, Default::default(), &task_manager, &tmp_dir)?;
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
    let (methods, chain) =
        epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
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
    let (methods, _) = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
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
    let (methods, _) = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
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
    let (methods, _) = epoch_fee_methods(7, 1_000, Default::default(), &task_manager, &tmp_dir)?;
    let filled = methods.call("eth_fillTransaction", rpc_params![priced_transfer(7)]).await?;
    assert_filled_transfer(filled, 7, 0)
}
