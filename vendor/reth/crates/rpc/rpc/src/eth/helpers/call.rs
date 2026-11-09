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
    use alloy_eips::BlockNumberOrTag;
    use alloy_primitives::{Address, Bytes, TxKind, U256};
    use alloy_rpc_types_eth::{Bundle, StateContext, TransactionInput, TransactionRequest};
    use reth_chainspec::{ChainSpecBuilder, ChainSpecProvider};
    use reth_ethereum_primitives::{Block, BlockBody};
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_primitives_traits::Block as _;
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_rpc_eth_api::helpers::EthCall;
    use reth_rpc_eth_types::{EthApiError, GasCap};
    use reth_transaction_pool::test_utils::testing_pool;

    /// Runs `count` transfers, each carrying `calldata` non-zero bytes, as one `eth_callMany`
    /// against a funded sender and an empty block 1 on a mock chain, under a gas cap of
    /// `gas_cap`. A transfer costs 21,000 gas plus 16 per calldata byte.
    async fn call_transfers(
        gas_cap: u64,
        count: usize,
        calldata: usize,
    ) -> Result<Vec<Vec<alloy_rpc_types_eth::EthCallResponse>>, EthApiError> {
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
}
