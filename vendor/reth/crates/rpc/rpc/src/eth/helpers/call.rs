//! Contains RPC handler implementations specific to endpoints that call/execute within evm.

use crate::EthApi;
use reth_rpc_convert::RpcConvert;
use reth_rpc_eth_api::{
    helpers::{estimate::EstimateCall, Call, EthCall},
    FromEvmError, RpcNodeCore,
};
use reth_rpc_eth_types::EthApiError;

impl<N, Rpc> EthCall for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<Primitives = N::Primitives, Error = EthApiError, Evm = N::Evm>,
{
}

impl<N, Rpc> Call for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<Primitives = N::Primitives, Error = EthApiError, Evm = N::Evm>,
{
    #[inline]
    fn call_gas_limit(&self) -> u64 {
        self.inner.gas_cap()
    }

    #[inline]
    fn max_simulate_blocks(&self) -> u64 {
        self.inner.max_simulate_blocks()
    }

    #[inline]
    fn evm_memory_limit(&self) -> u64 {
        self.inner.evm_memory_limit()
    }
}

impl<N, Rpc> EstimateCall for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<Primitives = N::Primitives, Error = EthApiError, Evm = N::Evm>,
{
}

/// TN(TN-0057): `eth_callMany` is bounded as one request, by call count and by gas used.
#[cfg(test)]
mod call_many_budget_tests {
    use crate::EthApiBuilder;
    use alloy_consensus::Header;
    use alloy_eips::{
        eip2935::HISTORY_STORAGE_ADDRESS, eip4788::BEACON_ROOTS_ADDRESS, BlockNumberOrTag,
    };
    use alloy_primitives::{Address, Bytes, TxKind, B256, U256};
    use alloy_rpc_types_eth::{
        simulate::{SimBlock, SimulatePayload, SimulatedBlock},
        state::{AccountOverride, StateOverride},
        BlockOverrides, Bundle, EthCallResponse, StateContext, TransactionInput,
        TransactionRequest,
    };
    use reth_chainspec::{ChainSpecBuilder, ChainSpecProvider};
    use reth_ethereum_primitives::{Block, BlockBody};
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_primitives_traits::Block as _;
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_rpc_eth_api::helpers::{EthCall, SpawnBlocking as _};
    use reth_rpc_eth_types::{EthApiError, GasCap};
    use reth_transaction_pool::test_utils::testing_pool;
    use std::time::Duration;

    /// Runs `count` transfers, each carrying `calldata` non-zero bytes, as one `eth_callMany`
    /// against a funded sender and an empty block 1 on a mock chain, under a gas cap of
    /// `gas_cap`. A transfer costs 21,000 gas plus 16 per calldata byte.
    async fn call_transfers(
        gas_cap: u64,
        count: usize,
        calldata: usize,
    ) -> Result<Vec<Vec<EthCallResponse>>, EthApiError> {
        let provider = MockEthProvider::default()
            .with_chain_spec(ChainSpecBuilder::mainnet().berlin_activated().build());
        let block = Block {
            header: Header { number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody::default(),
        }
        .seal_slow();
        provider.add_block(block.hash(), block.into_block());
        let sender = Address::repeat_byte(0xaa);
        provider.add_account(sender, ExtendedAccount::new(0, U256::from(10u64.pow(18))));
        let api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .gas_cap(GasCap(gas_cap))
        .build();
        let transfer = TransactionRequest {
            from: Some(sender),
            to: Some(TxKind::Call(Address::repeat_byte(0xff))),
            input: TransactionInput::new(Bytes::from(vec![0x01; calldata])),
            ..Default::default()
        };
        EthCall::call_many(
            &api,
            vec![Bundle { transactions: vec![transfer; count], block_override: None }],
            Some(StateContext {
                block_number: Some(BlockNumberOrTag::Number(1).into()),
                transaction_index: None,
            }),
            None,
        )
        .await
    }

    /// Two 21,000-gas transfers fit a 50,000 budget.
    #[tokio::test]
    async fn calls_within_the_budget_run() {
        let results = call_transfers(50_000, 2, 0).await.expect("42,000 fits 50,000");
        assert_eq!(results[0].len(), 2);
        assert!(results[0].iter().all(|response| response.error.is_none()), "{results:?}");
    }

    /// Two calls pass the count check, but the second takes the request past its budget.
    #[tokio::test]
    async fn calls_over_the_budget_are_refused() {
        // 21,000 + 16 * 300 = 25,800 gas each; 51,600 in all
        let err = call_transfers(50_000, 2, 300).await.expect_err("51,600 exceeds 50,000");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "request exceeds the gas budget of 50000"),
            "{err:?}"
        );
    }

    /// More calls than the budget can hold are refused before any runs.
    #[tokio::test]
    async fn too_many_calls_are_refused_first() {
        let err = call_transfers(42_000, 3, 0).await.expect_err("42,000 holds two calls");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "request exceeds the limit of 2 calls"),
            "{err:?}"
        );
    }

    /// TN(TN-0156): a contract whose code loops until it runs out of gas.
    const LOOP: Address = Address::repeat_byte(0xcc);

    /// TN(TN-0156): runs `blocks`, each a list of calls, as one `eth_simulateV1` on top of block
    /// 1 of a mock chain with a funded sender and [`LOOP`], under a gas cap of `gas_cap`.
    async fn simulate(
        gas_cap: u64,
        blocks: Vec<Vec<TransactionRequest>>,
    ) -> Result<Vec<SimulatedBlock<alloy_rpc_types_eth::Block>>, EthApiError> {
        let blocks = blocks
            .into_iter()
            .map(|calls| SimBlock { block_overrides: None, state_overrides: None, calls })
            .collect();
        simulate_blocks(gas_cap, blocks).await
    }

    /// TN(TN-0171): [`simulate`] with each block's overrides.
    async fn simulate_blocks(
        gas_cap: u64,
        block_state_calls: Vec<SimBlock>,
    ) -> Result<Vec<SimulatedBlock<alloy_rpc_types_eth::Block>>, EthApiError> {
        let provider = simulation_chain();
        let api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .gas_cap(GasCap(gas_cap))
        .build();
        let payload = SimulatePayload {
            block_state_calls,
            trace_transfers: false,
            validation: false,
            return_full_transactions: false,
        };
        EthCall::simulate_v1(&api, payload, Some(BlockNumberOrTag::Number(1).into())).await
    }

    /// TN(TN-0183): a contract that returns 1 MiB of zeros (`PUSH3 0x100000; PUSH1 0; RETURN`),
    /// for 2,216,462 gas with the intrinsic 21,000.
    const RETURNS_A_MIB: Address = Address::repeat_byte(0xdd);

    /// TN(TN-0183): a contract that logs 512 KiB of zeros and returns the same 512 KiB
    /// (`PUSH3 0x80000; PUSH1 0; LOG0; PUSH3 0x80000; PUSH1 0; RETURN`), for 4,789,131 gas with
    /// the intrinsic 21,000.
    const LOGS_AND_RETURNS_A_MIB: Address = Address::repeat_byte(0xee);

    /// TN(TN-0156): the mock chain [`simulate`] runs on: block 1, a funded [`SENDER`] and
    /// [`LOOP`], and (TN-0183) [`RETURNS_A_MIB`] and [`LOGS_AND_RETURNS_A_MIB`].
    fn simulation_chain() -> MockEthProvider {
        let provider = MockEthProvider::default()
            .with_chain_spec(ChainSpecBuilder::mainnet().berlin_activated().build());
        let block = Block {
            header: Header { number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody::default(),
        }
        .seal_slow();
        provider.add_block(block.hash(), block.into_block());
        provider.add_account(SENDER, ExtendedAccount::new(0, U256::from(10u64.pow(18))));
        // JUMPDEST; PUSH1 0; JUMP
        provider.add_account(
            LOOP,
            ExtendedAccount::new(0, U256::ZERO)
                .with_bytecode(Bytes::from_static(&[0x5b, 0x60, 0x00, 0x56])),
        );
        provider.add_account(
            RETURNS_A_MIB,
            ExtendedAccount::new(0, U256::ZERO)
                .with_bytecode(Bytes::from_static(&[0x62, 0x10, 0x00, 0x00, 0x60, 0x00, 0xf3])),
        );
        provider.add_account(
            LOGS_AND_RETURNS_A_MIB,
            ExtendedAccount::new(0, U256::ZERO).with_bytecode(Bytes::from_static(&[
                0x62, 0x08, 0x00, 0x00, 0x60, 0x00, 0xa0, 0x62, 0x08, 0x00, 0x00, 0x60, 0x00, 0xf3,
            ])),
        );
        provider
    }

    /// TN(TN-0156): the funded sender of [`simulate`].
    const SENDER: Address = Address::repeat_byte(0xaa);

    /// TN(TN-0156): a 21,000-gas transfer from [`SENDER`], a legacy transaction (the mock
    /// chain is at Berlin).
    fn transfer() -> TransactionRequest {
        TransactionRequest {
            from: Some(SENDER),
            to: Some(TxKind::Call(Address::repeat_byte(0xff))),
            gas_price: Some(0),
            ..Default::default()
        }
    }

    /// TN(TN-0156): three simulated blocks of one transfer each fit a 63,000 budget, and the
    /// third is refused under 50,000.
    #[tokio::test]
    async fn simulated_blocks_share_the_gas_budget() {
        let blocks = vec![vec![transfer()], vec![transfer()], vec![transfer()]];
        let simulated = simulate(63_000, blocks.clone()).await.expect("63,000 holds three");
        assert_eq!(simulated.len(), 3);
        assert!(simulated.iter().all(|block| block.calls[0].gas_used == 21_000), "{simulated:?}");

        let err = simulate(50_000, blocks).await.expect_err("8,000 is left for the third");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "request exceeds the gas budget of 50000"),
            "{err:?}"
        );
    }

    /// TN(TN-0156): a call's own `gas` above the budget runs with the budget only, and the
    /// request fails once the budget cannot pay for the next call.
    #[tokio::test]
    async fn a_simulated_call_runs_with_at_most_the_budget() {
        let spin = TransactionRequest {
            from: Some(SENDER),
            to: Some(TxKind::Call(LOOP)),
            gas: Some(1_000_000),
            gas_price: Some(0),
            ..Default::default()
        };
        let simulated = simulate(100_000, vec![vec![spin.clone()]]).await.expect("one call");
        let call = &simulated[0].calls[0];
        assert!(!call.status, "the loop runs out of gas: {call:?}");
        assert_eq!(call.gas_used, 100_000);

        let err = simulate(100_000, vec![vec![spin], vec![transfer()]])
            .await
            .expect_err("nothing is left for the transfer");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "request exceeds the gas budget of 100000"),
            "{err:?}"
        );
    }

    /// TN(TN-0183): `count` calls to [`RETURNS_A_MIB`] as one `eth_callMany` on the
    /// [`simulation_chain`], under a 100M gas cap.
    async fn call_many_returning_a_mib(
        count: usize,
    ) -> Result<Vec<Vec<EthCallResponse>>, EthApiError> {
        let provider = simulation_chain();
        let api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .gas_cap(GasCap(100_000_000))
        .build();
        let call = TransactionRequest {
            from: Some(SENDER),
            to: Some(TxKind::Call(RETURNS_A_MIB)),
            gas: Some(2_500_000),
            ..Default::default()
        };
        EthCall::call_many(
            &api,
            vec![Bundle { transactions: vec![call; count], block_override: None }],
            Some(StateContext {
                block_number: Some(BlockNumberOrTag::Number(1).into()),
                transaction_index: None,
            }),
            None,
        )
        .await
    }

    /// TN(TN-0183): 32 calls that return 1 MiB each fill the output budget; with a 33rd, the
    /// request is refused, though its gas fits the budget.
    #[tokio::test]
    async fn call_many_output_over_the_budget_is_refused() {
        let results = call_many_returning_a_mib(32).await.expect("32 MiB fits");
        assert_eq!(results[0].len(), 32);
        assert!(results[0]
            .iter()
            .all(|response| response.value.as_ref().is_some_and(|value| value.len() == 1 << 20)));

        let err = call_many_returning_a_mib(33).await.expect_err("33 MiB exceeds the budget");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "request exceeds the output budget of 33554432 bytes"),
            "{err:?}"
        );
    }

    /// TN(TN-0183): `count` calls to [`LOGS_AND_RETURNS_A_MIB`] as one `eth_simulateV1` block
    /// of 200M gas, under a 200M gas cap.
    async fn simulate_logging_a_mib(
        count: usize,
    ) -> Result<Vec<SimulatedBlock<alloy_rpc_types_eth::Block>>, EthApiError> {
        let call = TransactionRequest {
            from: Some(SENDER),
            to: Some(TxKind::Call(LOGS_AND_RETURNS_A_MIB)),
            gas: Some(5_000_000),
            gas_price: Some(0),
            ..Default::default()
        };
        let block = SimBlock {
            block_overrides: Some(BlockOverrides {
                gas_limit: Some(200_000_000),
                ..Default::default()
            }),
            state_overrides: None,
            calls: vec![call; count],
        };
        simulate_blocks(200_000_000, vec![block]).await
    }

    /// TN(TN-0183): a simulated call keeps its return data and its logs' data, 1 MiB for each
    /// call here: 32 fill the output budget, and with a 33rd the request is refused.
    #[tokio::test]
    async fn simulate_output_over_the_budget_is_refused() {
        let simulated = simulate_logging_a_mib(32).await.expect("32 MiB fits");
        assert!(simulated[0].calls.iter().all(|call| call.status &&
            call.gas_used == 4_789_131 &&
            call.return_data.len() == 1 << 19 &&
            call.logs.len() == 1 &&
            call.logs[0].data().data.len() == 1 << 19));
        assert_eq!(simulated[0].calls.len(), 32);

        let err = simulate_logging_a_mib(33).await.expect_err("33 MiB exceeds the budget");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "request exceeds the output budget of 33554432 bytes"),
            "{err:?}"
        );
    }

    /// TN(TN-0171): no block may replace the code of a pre-execution system call's target, by
    /// new code or by moving a precompile there; other overrides of those accounts are kept.
    #[tokio::test]
    async fn system_call_targets_keep_their_code() {
        let with_overrides = |overrides: StateOverride| {
            vec![
                SimBlock { block_overrides: None, state_overrides: None, calls: vec![transfer()] },
                SimBlock {
                    block_overrides: None,
                    state_overrides: Some(overrides),
                    calls: vec![transfer()],
                },
            ]
        };
        let spin = Bytes::from_static(&[0x5b, 0x60, 0x00, 0x56]);
        let refused = [
            (BEACON_ROOTS_ADDRESS, AccountOverride::default().with_code(spin.clone())),
            (HISTORY_STORAGE_ADDRESS, AccountOverride::default().with_code(Bytes::new())),
            (
                Address::with_last_byte(1),
                AccountOverride {
                    move_precompile_to: Some(HISTORY_STORAGE_ADDRESS),
                    ..Default::default()
                },
            ),
        ];
        for (address, account) in refused {
            let target = account.move_precompile_to.unwrap_or(address);
            let err = simulate_blocks(
                100_000,
                with_overrides(std::iter::once((address, account)).collect()),
            )
            .await
            .expect_err("the system call's code is replaced");
            assert!(
                matches!(&err, EthApiError::InvalidParams(message)
                    if *message == format!("state override replaces the code of system contract {target}")),
                "{err:?}"
            );
        }

        let kept = AccountOverride::default()
            .with_balance(U256::from(1))
            .with_state_diff([(B256::ZERO, B256::with_last_byte(1))]);
        let simulated = simulate_blocks(
            100_000,
            with_overrides(std::iter::once((HISTORY_STORAGE_ADDRESS, kept)).collect()),
        )
        .await
        .expect("balance and storage overrides are kept");
        assert_eq!(simulated.len(), 2);
    }
    /// TN(TN-0181): `simulate_v1` takes a tracing permit itself, for its blocking work to hold,
    /// so a request waits while every permit is taken and runs once one is free.
    #[tokio::test]
    async fn simulate_takes_a_tracing_permit() {
        let provider = simulation_chain();
        let api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .proof_permits(1)
        .build();
        let payload = SimulatePayload {
            block_state_calls: vec![SimBlock {
                block_overrides: None,
                state_overrides: None,
                calls: vec![transfer()],
            }],
            trace_transfers: false,
            validation: false,
            return_full_transactions: false,
        };
        let at = Some(BlockNumberOrTag::Number(1).into());
        let free_permit = || {
            tokio::time::timeout(
                Duration::from_millis(500),
                api.tracing_task_guard().clone().acquire_owned(),
            )
        };

        let held = free_permit().await.expect("one permit is free").expect("open");
        let waited = tokio::time::timeout(
            Duration::from_millis(500),
            EthCall::simulate_v1(&api, payload.clone(), at),
        )
        .await;
        assert!(waited.is_err(), "simulated without a tracing permit: {waited:?}");

        drop(held);
        let simulated = EthCall::simulate_v1(&api, payload, at).await.expect("simulates");
        assert_eq!(simulated.len(), 1);
        drop(free_permit().await.expect("the permit is back once the work ends"));
    }
}
