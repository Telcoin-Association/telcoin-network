//! `Eth` bundle implementation and helpers.

use alloy_consensus::{transaction::TxHashRef, EnvKzgSettings, Transaction as _};
use alloy_eips::eip7840::BlobParams;
use alloy_evm::env::BlockEnvironment;
use alloy_primitives::{uint, Keccak256, U256};
use alloy_rpc_types_mev::{EthCallBundle, EthCallBundleResponse, EthCallBundleTransactionResult};
use jsonrpsee::core::RpcResult;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_evm::{ConfigureEvm, Evm};
use reth_rpc_eth_api::{
    helpers::{Call, EthTransactions, LoadPendingBlock},
    EthCallBundleApiServer, FromEthApiError, FromEvmError,
};
use reth_rpc_eth_types::{utils::recover_raw_transaction, EthApiError, RpcInvalidTransactionError};
use reth_tasks::{cancel::is_cancelled, pool::BlockingTaskGuard};
use reth_transaction_pool::{
    EthBlobTransactionSidecar, EthPoolTransaction, PoolPooledTx, PoolTransaction, TransactionPool,
};
use revm::{
    context::Block, context_interface::result::ResultAndState, DatabaseCommit, DatabaseRef,
};
use std::sync::Arc;

/// `Eth` bundle implementation.
pub struct EthBundle<Eth> {
    /// All nested fields bundled together.
    inner: Arc<EthBundleInner<Eth>>,
}

impl<Eth> EthBundle<Eth> {
    /// Create a new `EthBundle` instance.
    pub fn new(eth_api: Eth, blocking_task_guard: BlockingTaskGuard) -> Self {
        Self { inner: Arc::new(EthBundleInner { eth_api, blocking_task_guard }) }
    }

    /// Access the underlying `Eth` API.
    pub fn eth_api(&self) -> &Eth {
        &self.inner.eth_api
    }
}

impl<Eth> EthBundle<Eth>
where
    Eth: EthTransactions + LoadPendingBlock + Call + 'static,
{
    /// Simulates a bundle of transactions at the top of a given block number with the state of
    /// another (or the same) block. This can be used to simulate future blocks with the current
    /// state, or it can be used to simulate a past block. The sender is responsible for signing the
    /// transactions and using the correct nonce and ensuring validity
    pub async fn call_bundle(
        &self,
        bundle: EthCallBundle,
    ) -> Result<EthCallBundleResponse, Eth::Error> {
        let EthCallBundle {
            txs,
            block_number,
            coinbase,
            state_block_number,
            timeout: _,
            timestamp,
            gas_limit,
            difficulty,
            base_fee,
            ..
        } = bundle;
        if txs.is_empty() {
            return Err(EthApiError::InvalidParams(
                EthBundleError::EmptyBundleTransactions.to_string(),
            )
            .into())
        }
        if block_number == 0 {
            return Err(EthApiError::InvalidParams(
                EthBundleError::BundleMissingBlockNumber.to_string(),
            )
            .into())
        }

        // TN(TN-0037): upstream takes no permit here (the struct's tracing guard is unused). Hold
        // a blocking-IO permit inside the blocking task, as `eth_callMany` does.
        let permit = self
            .eth_api()
            .acquire_owned_blocking_io()
            .await
            .map_err(|_| EthApiError::InternalEthError)?;

        let transactions = txs
            .into_iter()
            .map(|tx| recover_raw_transaction::<PoolPooledTx<Eth::Pool>>(&tx))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Vec<_>>();

        let block_id: alloy_rpc_types_eth::BlockId = state_block_number.into();
        // Note: the block number is considered the `parent` block: <https://github.com/flashbots/mev-geth/blob/fddf97beec5877483f879a77b7dea2e58a58d653/internal/ethapi/api.go#L2104>
        let (mut evm_env, at) = self.eth_api().evm_env_at(block_id).await?;

        if let Some(coinbase) = coinbase {
            evm_env.block_env.inner_mut().beneficiary = coinbase;
        }

        // need to adjust the timestamp for the next block
        if let Some(timestamp) = timestamp {
            evm_env.block_env.inner_mut().timestamp = U256::from(timestamp);
        } else {
            evm_env.block_env.inner_mut().timestamp += uint!(12_U256);
        }

        if let Some(difficulty) = difficulty {
            evm_env.block_env.inner_mut().difficulty = U256::from(difficulty);
        }

        // Validate that the bundle does not contain more than MAX_BLOB_NUMBER_PER_BLOCK blob
        // transactions.
        let blob_gas_used = transactions.iter().filter_map(|tx| tx.blob_gas_used()).sum::<u64>();
        if blob_gas_used > 0 {
            let blob_params = self
                .eth_api()
                .provider()
                .chain_spec()
                .blob_params_at_timestamp(evm_env.block_env.timestamp().saturating_to())
                .unwrap_or_else(BlobParams::cancun);
            if blob_gas_used > blob_params.max_blob_gas_per_block() {
                return Err(EthApiError::InvalidParams(
                    EthBundleError::Eip4844BlobGasExceeded(blob_params.max_blob_gas_per_block())
                        .to_string(),
                )
                .into())
            }
        }

        // default to call gas limit unless user requests a smaller limit
        evm_env.block_env.inner_mut().gas_limit = self.inner.eth_api.call_gas_limit();
        if let Some(gas_limit) = gas_limit {
            if gas_limit > evm_env.block_env.gas_limit() {
                return Err(
                    EthApiError::InvalidTransaction(RpcInvalidTransactionError::GasTooHigh).into()
                )
            }
            evm_env.block_env.inner_mut().gas_limit = gas_limit;
        }

        if let Some(base_fee) = base_fee {
            evm_env.block_env.inner_mut().basefee = base_fee.try_into().unwrap_or(u64::MAX);
        }

        let state_block_number = evm_env.block_env.number();
        // use the block number of the request
        evm_env.block_env.inner_mut().number = U256::from(block_number);
        let block_gas_limit = evm_env.block_env.gas_limit();

        self.eth_api()
            .spawn_with_state_at_block(at, move |eth_api, db| {
                let _permit = permit;
                let coinbase = evm_env.block_env.beneficiary();
                let basefee = evm_env.block_env.basefee();

                let initial_coinbase = db
                    .basic_ref(coinbase)
                    .map_err(Eth::Error::from_eth_err)?
                    .map(|acc| acc.balance)
                    .unwrap_or_default();
                let mut coinbase_balance_before_tx = initial_coinbase;
                let mut coinbase_balance_after_tx = initial_coinbase;
                let mut total_gas_used = 0u64;
                let mut total_gas_fees = U256::ZERO;
                let mut hasher = Keccak256::new();

                let mut evm = eth_api.evm_config().evm_with_env(db, evm_env);

                let mut results = Vec::with_capacity(transactions.len());
                let mut transactions = transactions.into_iter().peekable();

                while let Some(tx) = transactions.next() {
                    if is_cancelled() {
                        return Err(Eth::Error::from_eth_err(EthApiError::InternalEthError))
                    }

                    // TN(TN-0046): the bundle's transactions share the block gas limit, as a
                    // block's do (upstream 329fcfc73, without its EIP-8037 branch).
                    if tx.gas_limit() > block_gas_limit.saturating_sub(total_gas_used) {
                        return Err(Eth::Error::from_eth_err(EthApiError::InvalidParams(
                            EthBundleError::BundleGasLimitExceeded(block_gas_limit).to_string(),
                        )))
                    }

                    let signer = tx.signer();
                    let tx = {
                        let mut tx = <Eth::Pool as TransactionPool>::Transaction::from_pooled(tx);

                        if let EthBlobTransactionSidecar::Present(sidecar) = tx.take_blob() {
                            tx.validate_blob(&sidecar, EnvKzgSettings::Default.get()).map_err(
                                |e| {
                                    Eth::Error::from_eth_err(EthApiError::InvalidParams(
                                        e.to_string(),
                                    ))
                                },
                            )?;
                        }

                        tx.into_consensus()
                    };

                    hasher.update(*tx.tx_hash());
                    let ResultAndState { result, state } = evm
                        .transact(eth_api.evm_config().tx_env(&tx))
                        .map_err(Eth::Error::from_evm_err)?;

                    let gas_price = tx
                        .effective_tip_per_gas(basefee)
                        .expect("fee is always valid; execution succeeded");
                    let gas_used = result.gas_used();
                    total_gas_used += gas_used;

                    let gas_fees = U256::from(gas_used) * U256::from(gas_price);
                    total_gas_fees += gas_fees;

                    // coinbase is always present in the result state
                    coinbase_balance_after_tx =
                        state.get(&coinbase).map(|acc| acc.info.balance).unwrap_or_default();
                    let coinbase_diff =
                        coinbase_balance_after_tx.saturating_sub(coinbase_balance_before_tx);
                    let eth_sent_to_coinbase = coinbase_diff.saturating_sub(gas_fees);

                    // update the coinbase balance
                    coinbase_balance_before_tx = coinbase_balance_after_tx;

                    // set the return data for the response
                    let (value, revert) = if result.is_success() {
                        let value = result.into_output().unwrap_or_default();
                        (Some(value), None)
                    } else {
                        let revert = result.into_output().unwrap_or_default();
                        (None, Some(revert))
                    };

                    let tx_res = EthCallBundleTransactionResult {
                        coinbase_diff,
                        eth_sent_to_coinbase,
                        from_address: signer,
                        gas_fees,
                        gas_price: U256::from(gas_price),
                        gas_used,
                        to_address: tx.to(),
                        tx_hash: *tx.tx_hash(),
                        value,
                        revert,
                    };
                    results.push(tx_res);

                    // need to apply the state changes of this call before executing the
                    // next call
                    if transactions.peek().is_some() {
                        // need to apply the state changes of this call before executing
                        // the next call
                        evm.db_mut().commit(state)
                    }
                }

                // populate the response

                let coinbase_diff = coinbase_balance_after_tx.saturating_sub(initial_coinbase);
                let eth_sent_to_coinbase = coinbase_diff.saturating_sub(total_gas_fees);
                let bundle_gas_price =
                    coinbase_diff.checked_div(U256::from(total_gas_used)).unwrap_or_default();
                let res = EthCallBundleResponse {
                    bundle_gas_price,
                    bundle_hash: hasher.finalize(),
                    coinbase_diff,
                    eth_sent_to_coinbase,
                    gas_fees: total_gas_fees,
                    results,
                    state_block_number: state_block_number.to(),
                    total_gas_used,
                };

                Ok(res)
            })
            .await
    }
}

#[async_trait::async_trait]
impl<Eth> EthCallBundleApiServer for EthBundle<Eth>
where
    Eth: EthTransactions + LoadPendingBlock + Call + 'static,
{
    async fn call_bundle(&self, request: EthCallBundle) -> RpcResult<EthCallBundleResponse> {
        Self::call_bundle(self, request).await.map_err(Into::into)
    }
}

/// Container type for `EthBundle` internals
#[derive(Debug)]
struct EthBundleInner<Eth> {
    /// Access to commonly used code of the `eth` namespace
    eth_api: Eth,
    // restrict the number of concurrent tracing calls.
    #[expect(dead_code)]
    blocking_task_guard: BlockingTaskGuard,
}

impl<Eth> std::fmt::Debug for EthBundle<Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthBundle").finish_non_exhaustive()
    }
}

impl<Eth> Clone for EthBundle<Eth> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

/// [`EthBundle`] specific errors.
#[derive(Debug, thiserror::Error)]
pub enum EthBundleError {
    /// Thrown if the bundle does not contain any transactions.
    #[error("bundle missing txs")]
    EmptyBundleTransactions,
    /// Thrown if the bundle does not contain a block number, or block number is 0.
    #[error("bundle missing blockNumber")]
    BundleMissingBlockNumber,
    /// Thrown when the blob gas usage of the blob transactions in a bundle exceed the maximum.
    #[error("blob gas usage exceeds the limit of {0} gas per block.")]
    Eip4844BlobGasExceeded(u64),
    /// Thrown when the gas limits of the bundle's transactions exceed the block gas limit.
    #[error("bundle gas exceeds the block gas limit of {0}")]
    BundleGasLimitExceeded(u64),
}

/// TN(TN-0046): the transactions of an `eth_callBundle` request share its block gas limit.
#[cfg(test)]
mod gas_limit_tests {
    use super::*;
    use crate::EthApiBuilder;
    use alloy_consensus::{Header, TxLegacy};
    use alloy_eips::{eip2718::Encodable2718, BlockNumberOrTag};
    use alloy_primitives::{Address, TxKind};
    use reth_chainspec::ChainSpecBuilder;
    use reth_ethereum_primitives::{Block, BlockBody, Transaction};
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_primitives_traits::Block as _;
    use reth_provider::test_utils::MockEthProvider;
    use reth_testing_utils::generators::{self, generate_key, sign_tx_with_key_pair};
    use reth_transaction_pool::test_utils::testing_pool;

    /// Two transfers from one sender, each with a 30,000 gas limit, run as a bundle whose block
    /// gas limit is `gas_limit`. The first uses 21,000 gas.
    async fn call_two_transfers(gas_limit: u64) -> Result<EthCallBundleResponse, EthApiError> {
        let provider = MockEthProvider::default()
            .with_chain_spec(ChainSpecBuilder::mainnet().berlin_activated().build());
        let block = Block {
            header: Header { number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody::default(),
        }
        .seal_slow();
        provider.add_block(block.hash(), block.into_block());
        let api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        let bundle = EthBundle::new(api, BlockingTaskGuard::new(1));
        let key = generate_key(&mut generators::rng());
        let transfer = |nonce| {
            sign_tx_with_key_pair(
                key,
                Transaction::Legacy(TxLegacy {
                    nonce,
                    gas_limit: 30_000,
                    to: TxKind::Call(Address::repeat_byte(0xff)),
                    ..Default::default()
                }),
            )
            .encoded_2718()
            .into()
        };
        bundle
            .call_bundle(EthCallBundle {
                txs: vec![transfer(0), transfer(1)],
                block_number: 2,
                state_block_number: BlockNumberOrTag::Number(1),
                gas_limit: Some(gas_limit),
                ..Default::default()
            })
            .await
    }

    #[tokio::test]
    async fn bundle_within_the_gas_limit_runs() {
        let response = call_two_transfers(60_000).await.expect("30,000 + 30,000 fits 60,000");
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.total_gas_used, 42_000);
    }

    /// The second transfer's 30,000 gas limit exceeds the 29,000 the first one leaves.
    #[tokio::test]
    async fn bundle_over_the_gas_limit_is_refused() {
        let err = call_two_transfers(50_000).await.expect_err("only 29,000 gas is left");
        assert!(
            matches!(&err, EthApiError::InvalidParams(message)
                if message == "bundle gas exceeds the block gas limit of 50000"),
            "{err:?}"
        );
    }
}

/// TN(TN-0037): `eth_callBundle` and `eth_createAccessList` keep their blocking-IO permit until
/// their blocking task ends, not until their request ends.
#[cfg(test)]
mod blocking_io_permit_tests {
    use super::*;
    use crate::{eth::helpers::types::EthRpcConverter, EthApi, EthApiBuilder};
    use alloy_consensus::{Header, TxLegacy};
    use alloy_eips::{eip2718::Encodable2718, BlockId, BlockNumberOrTag};
    use alloy_primitives::{Address, TxKind};
    use alloy_rpc_types_eth::TransactionRequest;
    use reth_chainspec::{ChainSpec, ChainSpecBuilder};
    use reth_ethereum_primitives::{Block, BlockBody, Transaction};
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_primitives_traits::Block as _;
    use reth_provider::test_utils::MockEthProvider;
    use reth_rpc_eth_api::{
        helpers::{EthCall, SpawnBlocking},
        node::RpcNodeCoreAdapter,
    };
    use reth_testing_utils::generators;
    use reth_transaction_pool::test_utils::{testing_pool, TestPool};
    use std::{future::Future, pin::Pin, sync::mpsc, time::Duration};
    use tokio::time::{sleep, timeout, Instant};

    type TestEthApi = EthApi<
        RpcNodeCoreAdapter<MockEthProvider, TestPool, NoopNetwork, EthEvmConfig>,
        EthRpcConverter<ChainSpec>,
    >;

    type Request<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Time for spawned work to start, or for a wrongly freed permit to show.
    const SETTLE: Duration = Duration::from_millis(200);

    /// Bounds every wait, so a regression fails instead of hanging.
    const DEADLINE: Duration = Duration::from_secs(20);

    /// An `EthApi` with one blocking-IO permit over a mock chain with an empty block 1 (Berlin, so
    /// access lists exist), and a signed transaction for bundles.
    struct Fixture {
        provider: MockEthProvider,
        api: TestEthApi,
        bundle: EthBundle<TestEthApi>,
        raw_tx: alloy_primitives::Bytes,
    }

    impl Fixture {
        fn new() -> Self {
            let provider = MockEthProvider::default()
                .with_chain_spec(ChainSpecBuilder::mainnet().berlin_activated().build());
            let block = Block {
                header: Header { number: 1, gas_limit: 30_000_000, ..Default::default() },
                body: BlockBody::default(),
            }
            .seal_slow();
            provider.add_block(block.hash(), block.into_block());
            let api = EthApiBuilder::new(
                provider.clone(),
                testing_pool(),
                NoopNetwork::default(),
                EthEvmConfig::new(provider.chain_spec()),
            )
            .max_blocking_io_requests(1)
            .build();
            let bundle = EthBundle::new(api.clone(), BlockingTaskGuard::new(1));
            let tx = generators::sign_tx_with_random_key_pair(
                &mut generators::rng(),
                Transaction::Legacy(TxLegacy {
                    gas_limit: 21_000,
                    to: TxKind::Call(Address::with_last_byte(1)),
                    ..Default::default()
                }),
            );
            Self { provider, api, bundle, raw_tx: tx.encoded_2718().into() }
        }

        fn call_bundle(&self) -> Request<'_> {
            let request = EthCallBundle {
                txs: vec![self.raw_tx.clone()],
                block_number: 2,
                state_block_number: BlockNumberOrTag::Number(1),
                ..Default::default()
            };
            Box::pin(async move {
                self.bundle.call_bundle(request).await.expect("eth_callBundle");
            })
        }

        fn create_access_list(&self) -> Request<'_> {
            let request = TransactionRequest {
                to: Some(TxKind::Call(Address::with_last_byte(2))),
                ..Default::default()
            };
            Box::pin(async move {
                EthCall::create_access_list_at(
                    &self.api,
                    request,
                    Some(BlockId::Number(1.into())),
                    None,
                )
                .await
                .expect("eth_createAccessList");
            })
        }

        /// Clones of the mock provider alive: a blocking task holds one for its state.
        fn provider_clones(&self) -> usize {
            Arc::strong_count(&self.provider.accounts)
        }

        async fn settle_at(&self, clones: usize) {
            let deadline = Instant::now() + DEADLINE;
            while self.provider_clones() != clones {
                assert!(Instant::now() < deadline, "provider clones stuck at {clones}");
                sleep(Duration::from_millis(5)).await;
            }
        }
    }

    /// Locks the mock's account store from a thread of its own until dropped, so the first
    /// account read of a request blocks inside its blocking task.
    struct AccountGate {
        release: Option<mpsc::Sender<()>>,
        holder: Option<std::thread::JoinHandle<()>>,
    }

    impl AccountGate {
        fn close(provider: &MockEthProvider) -> Self {
            let accounts = Arc::clone(&provider.accounts);
            let (locked_tx, locked_rx) = mpsc::channel();
            let (release, released) = mpsc::channel::<()>();
            let holder = std::thread::spawn(move || {
                let _locked = accounts.lock();
                locked_tx.send(()).unwrap();
                let _ = released.recv();
            });
            locked_rx.recv().unwrap();
            Self { release: Some(release), holder: Some(holder) }
        }
    }

    impl Drop for AccountGate {
        fn drop(&mut self) {
            drop(self.release.take());
            if let Some(holder) = self.holder.take() {
                let _ = holder.join();
            }
        }
    }

    /// Drops a request whose blocking task is still running and checks that the permit stays
    /// taken until that task ends.
    async fn assert_dropped_request_keeps_its_permit(
        method: &str,
        fx: &Fixture,
        start: impl Fn(&Fixture) -> Request<'_>,
    ) {
        // Let `EthApi`'s startup tasks run, then warm the caches with one full request, so the
        // only provider clone a request makes below is its blocking task's state.
        sleep(SETTLE).await;
        let idle = fx.provider_clones();
        timeout(DEADLINE, start(fx)).await.unwrap_or_else(|_| panic!("{method}: warm-up hangs"));
        fx.settle_at(idle).await;
        let permits = fx.api.blocking_io_task_guard();
        assert_eq!(permits.available_permits(), 1, "{method}: the warm-up kept its permit");

        let gate = AccountGate::close(&fx.provider);
        let gated = idle + 1;
        let mut request = start(fx);
        let deadline = Instant::now() + DEADLINE;
        while fx.provider_clones() == gated {
            assert!(Instant::now() < deadline, "{method}: the blocking task never started");
            assert!(
                timeout(Duration::from_millis(5), &mut request).await.is_err(),
                "{method}: the request ended while the account store was locked"
            );
        }
        drop(request);
        sleep(SETTLE).await;
        assert_eq!(fx.provider_clones(), gated + 1, "{method}: the blocking task ended");
        assert_eq!(
            permits.available_permits(),
            0,
            "{method}: the dropped request freed its permit"
        );

        drop(gate);
        let deadline = Instant::now() + DEADLINE;
        while permits.available_permits() == 0 {
            assert!(Instant::now() < deadline, "{method}: the permit never came back");
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_call_bundle_keeps_permit_after_client_leaves() {
        let fx = Fixture::new();
        assert_dropped_request_keeps_its_permit("eth_callBundle", &fx, Fixture::call_bundle).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_access_list_keeps_permit_after_client_leaves() {
        let fx = Fixture::new();
        assert_dropped_request_keeps_its_permit(
            "eth_createAccessList",
            &fx,
            Fixture::create_access_list,
        )
        .await;
    }
}
