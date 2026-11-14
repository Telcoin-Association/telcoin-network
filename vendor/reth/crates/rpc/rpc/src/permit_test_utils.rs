//! TN(TN-0036, TN-0038): the harness for the tests that check a trace or an execution witness
//! keeps its tracing permit until its blocking task ends, not only while its request is open.

use crate::{eth::EthApi, EthApiBuilder};
use alloy_consensus::{Header, TxLegacy};
use alloy_network::Ethereum;
use alloy_primitives::{Address, TxKind};
use jsonrpsee::core::RpcResult;
use reth_chainspec::{ChainSpec, ChainSpecProvider};
use reth_ethereum_primitives::{Block, BlockBody, Transaction};
use reth_evm_ethereum::EthEvmConfig;
use reth_network_api::noop::NoopNetwork;
use reth_primitives_traits::{Block as _, SealedBlock};
use reth_provider::test_utils::MockEthProvider;
use reth_rpc_convert::RpcConverter;
use reth_rpc_eth_api::node::RpcNodeCoreAdapter;
use reth_rpc_eth_types::{receipt::EthReceiptConverter, GasCap};
use reth_testing_utils::generators;
use reth_transaction_pool::test_utils::{testing_pool, TestPool};
use std::{
    future::Future,
    pin::Pin,
    sync::{mpsc, Arc},
    time::Duration,
};
use tokio::time::{timeout, Instant};

pub(crate) type TestEthApi = EthApi<
    RpcNodeCoreAdapter<MockEthProvider, TestPool, NoopNetwork, EthEvmConfig>,
    RpcConverter<Ethereum, EthEvmConfig, EthReceiptConverter<ChainSpec>>,
>;

/// A request under test, with its result discarded.
pub(crate) type RequestFuture<'a> = Pin<Box<dyn Future<Output = RpcResult<()>> + Send + 'a>>;

/// Time for spawned work to start: `EthApi`'s startup tasks, or a second request that wrongly
/// got a permit.
pub(crate) const SETTLE: Duration = Duration::from_millis(200);

/// Bounds every wait, so a regression fails instead of hanging.
pub(crate) const DEADLINE: Duration = Duration::from_secs(20);

/// A mock chain whose block 1 holds one legacy transaction. Every request under test replays or
/// simulates a transaction, so it reads an account inside its blocking task.
pub(crate) struct MockChain {
    pub(crate) provider: MockEthProvider,
    pub(crate) block: SealedBlock<Block>,
}

impl MockChain {
    pub(crate) fn new() -> Self {
        let provider = MockEthProvider::default();
        let tx = generators::sign_tx_with_random_key_pair(
            &mut generators::rng(),
            Transaction::Legacy(TxLegacy {
                gas_limit: 21_000,
                to: TxKind::Call(Address::with_last_byte(1)),
                ..Default::default()
            }),
        );
        let block = Block {
            header: Header { number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody { transactions: vec![tx], ..Default::default() },
        }
        .seal_slow();
        provider.add_block(block.hash(), block.clone().into_block());
        Self { provider, block }
    }

    /// An `eth` API over this chain.
    pub(crate) fn eth_api(&self) -> TestEthApi {
        self.eth_api_with_gas_cap(GasCap::default())
    }

    /// TN(TN-0176): an `eth` API over this chain with `gas_cap` as `--rpc.gascap`.
    pub(crate) fn eth_api_with_gas_cap(&self, gas_cap: GasCap) -> TestEthApi {
        EthApiBuilder::new(
            self.provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(self.provider.chain_spec()),
        )
        .gas_cap(gas_cap)
        .build()
    }

    /// TN(TN-0176): a call of 25,800 gas: a transfer with 300 non-zero bytes of calldata.
    pub(crate) fn call_of_25_800_gas() -> alloy_rpc_types_eth::TransactionRequest {
        alloy_rpc_types_eth::TransactionRequest {
            to: Some(TxKind::Call(Address::with_last_byte(0xff))),
            input: alloy_rpc_types_eth::TransactionInput::new(vec![1u8; 300].into()),
            ..Default::default()
        }
    }

    /// Clones of the mock's account store alive: a request's blocking task holds one for its
    /// state.
    pub(crate) fn provider_clones(&self) -> usize {
        Arc::strong_count(&self.provider.accounts)
    }

    /// Waits until the request tasks and the block cache's loads have dropped their clones.
    pub(crate) async fn settle_at(&self, clones: usize) {
        let deadline = Instant::now() + DEADLINE;
        while self.provider_clones() != clones {
            assert!(
                Instant::now() < deadline,
                "provider clones stuck at {}",
                self.provider_clones()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Locks the mock's account store from a thread of its own until dropped, so the first account
/// read of a request blocks inside its blocking task.
pub(crate) struct AccountGate {
    release: Option<mpsc::Sender<()>>,
    holder: Option<std::thread::JoinHandle<()>>,
}

impl AccountGate {
    pub(crate) fn close(provider: &MockEthProvider) -> Self {
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

/// How the first request ends while its blocking task is still running.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Abandon {
    /// The client goes away: the request future is dropped.
    Drop,
    /// The JS tracer's `timeout` expires and the request returns an error.
    #[cfg(feature = "js-tracer")]
    Timeout,
}

/// With every tracing permit taken by one request, a request that ends while its blocking task
/// still runs keeps its permits, so a second request does not start a blocking task until the
/// first one's ends. `start` starts the request; `name` labels the failures.
pub(crate) async fn assert_abandoned_request_keeps_its_permit<'a>(
    chain: &MockChain,
    name: &str,
    start: impl Fn() -> RequestFuture<'a>,
    abandon: Abandon,
) {
    // Let `EthApi`'s startup tasks run: the fee-history task holds a clone until the mock's
    // canonical-state stream ends.
    tokio::time::sleep(SETTLE).await;
    let idle = chain.provider_clones();
    // Warm the block cache, so the only clone a request makes below is its own state's.
    timeout(DEADLINE, start())
        .await
        .expect("warm-up request hangs")
        .unwrap_or_else(|err| panic!("{name}: warm-up request failed: {err:?}"));
    chain.settle_at(idle).await;

    let gate = AccountGate::close(&chain.provider);
    let gated = idle + 1;
    let mut first = start();
    // Drive the first request until its blocking task has opened state, then end the request.
    let deadline = Instant::now() + DEADLINE;
    while chain.provider_clones() == gated {
        assert!(Instant::now() < deadline, "{name}: the first request never started");
        assert!(
            timeout(Duration::from_millis(5), &mut first).await.is_err(),
            "{name}: the first request ended while the account store was locked"
        );
    }
    match abandon {
        Abandon::Drop => drop(first),
        #[cfg(feature = "js-tracer")]
        Abandon::Timeout => {
            let err = timeout(DEADLINE, first).await.expect("timeout never fired").unwrap_err();
            assert!(err.message().contains("timeout"), "{name}: {err:?}");
        }
    }
    let running = chain.provider_clones();
    assert!(running > gated, "{name}: the first request's task ended");

    let mut second = start();
    assert!(
        timeout(SETTLE, &mut second).await.is_err(),
        "{name}: the second request ran while the first one's task held the account store"
    );
    assert_eq!(
        chain.provider_clones(),
        running,
        "{name}: the second request started a blocking task while the first one's still runs"
    );

    // The first request's task ends and returns the permit; the second request then runs.
    drop(gate);
    timeout(DEADLINE, second)
        .await
        .expect("the second request never got the permit")
        .unwrap_or_else(|err| panic!("{name}: second request failed: {err:?}"));
    chain.settle_at(idle).await;
}
