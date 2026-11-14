//! `eth_` `PubSub` RPC handler implementation

use std::{sync::Arc, time::Duration};

use crate::eth::filter::check_filter_size;
use alloy_primitives::TxHash;
use alloy_rpc_types_eth::{
    pubsub::{Params, PubSubSyncStatus, SubscriptionKind, SyncStatusMetadata},
    Filter, Log,
};
use futures::StreamExt;
use jsonrpsee::{
    server::SubscriptionMessage, types::ErrorObject, PendingSubscriptionSink, SubscriptionSink,
};
use reth_chain_state::CanonStateSubscriptions;
use reth_network_api::NetworkInfo;
use reth_rpc_convert::RpcHeader;
use reth_rpc_eth_api::{
    pubsub::EthPubSubApiServer, EthApiTypes, RpcConvert, RpcNodeCore, RpcTransaction,
};
use reth_rpc_eth_types::logs_utils;
use reth_rpc_server_types::result::{internal_rpc_err, invalid_params_rpc_err};
use reth_storage_api::BlockNumReader;
use reth_tasks::{TaskSpawner, TokioTaskExecutor};
use reth_transaction_pool::{
    NewSubpoolTransactionStream, NewTransactionEvent, PoolTransaction, SubPool,
    TransactionListenerKind, TransactionPool,
};
use serde::Serialize;
use tokio::sync::mpsc::Receiver;
use tokio_stream::{
    wrappers::{BroadcastStream, ReceiverStream},
    Stream,
};
use tracing::error;

/// `Eth` pubsub RPC implementation.
///
/// This handles `eth_subscribe` RPC calls.
#[derive(Clone)]
pub struct EthPubSub<Eth> {
    /// All nested fields bundled together.
    inner: Arc<EthPubSubInner<Eth>>,
}

// === impl EthPubSub ===

impl<Eth> EthPubSub<Eth> {
    /// Creates a new, shareable instance.
    ///
    /// Subscription tasks are spawned via [`tokio::task::spawn`]
    pub fn new(eth_api: Eth) -> Self {
        Self::with_spawner(eth_api, Box::<TokioTaskExecutor>::default())
    }

    /// Creates a new, shareable instance.
    pub fn with_spawner(eth_api: Eth, subscription_task_spawner: Box<dyn TaskSpawner>) -> Self {
        let inner = EthPubSubInner { eth_api, subscription_task_spawner };
        Self { inner: Arc::new(inner) }
    }
}

impl<Eth> EthPubSub<Eth>
where
    Eth: RpcNodeCore + EthApiTypes<RpcConvert: RpcConvert<Primitives = Eth::Primitives>>,
{
    /// Returns the current sync status for the `syncing` subscription
    pub fn sync_status(&self, is_syncing: bool) -> PubSubSyncStatus {
        self.inner.sync_status(is_syncing)
    }

    /// Returns a stream that yields all transaction hashes emitted by the txpool.
    ///
    /// TN(TN-0056): `None` when the pool refuses another RPC listener.
    pub fn pending_transaction_hashes_stream(&self) -> Option<impl Stream<Item = TxHash>> {
        self.inner.pending_transaction_hashes_stream()
    }

    /// Returns a stream that yields all transactions emitted by the txpool.
    ///
    /// TN(TN-0056): `None` when the pool refuses another RPC listener.
    pub fn full_pending_transaction_stream(
        &self,
    ) -> Option<impl Stream<Item = NewTransactionEvent<<Eth::Pool as TransactionPool>::Transaction>>>
    {
        self.inner.full_pending_transaction_stream()
    }

    /// Returns a stream that yields all new RPC blocks.
    pub fn new_headers_stream(&self) -> impl Stream<Item = RpcHeader<Eth::NetworkTypes>> {
        self.inner.new_headers_stream()
    }

    /// Returns a stream that yields all logs that match the given filter.
    pub fn log_stream(&self, filter: Filter) -> impl Stream<Item = Log> {
        self.inner.log_stream(filter)
    }

    /// The actual handler for an accepted [`EthPubSub::subscribe`] call.
    pub async fn handle_accepted(
        &self,
        accepted_sink: SubscriptionSink,
        kind: SubscriptionKind,
        params: Option<Params>,
    ) -> Result<(), ErrorObject<'static>> {
        #[allow(unreachable_patterns)]
        match kind {
            SubscriptionKind::NewHeads => {
                pipe_from_stream(accepted_sink, self.new_headers_stream()).await
            }
            SubscriptionKind::Logs => {
                // if no params are provided, used default filter params
                let filter = match params {
                    Some(Params::Logs(filter)) => *filter,
                    Some(Params::Bool(_)) => {
                        return Err(invalid_params_rpc_err("Invalid params for logs"))
                    }
                    _ => Default::default(),
                };
                pipe_from_stream(accepted_sink, self.log_stream(filter)).await
            }
            SubscriptionKind::NewPendingTransactions => {
                let listener = self.pending_transactions_listener(params)?;
                self.pipe_pending_transactions(accepted_sink, listener).await
            }
            SubscriptionKind::Syncing => {
                // get new block subscription
                let mut canon_state = BroadcastStream::new(
                    self.inner.eth_api.provider().subscribe_to_canonical_state(),
                );
                // get current sync status
                let mut initial_sync_status = self.inner.eth_api.network().is_syncing();
                let current_sub_res = self.sync_status(initial_sync_status);

                // send the current status immediately
                let msg = SubscriptionMessage::new(
                    accepted_sink.method_name(),
                    accepted_sink.subscription_id(),
                    &current_sub_res,
                )
                .map_err(SubscriptionSerializeError::new)?;

                if accepted_sink.send(msg).await.is_err() {
                    return Ok(())
                }

                loop {
                    // Sends only happen when the sync status changes, so a failed send cannot be
                    // relied on to detect a closed subscription.
                    tokio::select! {
                        _ = accepted_sink.closed() => break,
                        maybe_event = canon_state.next() => {
                            if maybe_event.is_none() {
                                break
                            }
                        }
                    }

                    let current_syncing = self.inner.eth_api.network().is_syncing();
                    // Only send a new response if the sync status has changed
                    if current_syncing != initial_sync_status {
                        // Update the sync status on each new block
                        initial_sync_status = current_syncing;

                        // send a new message now that the status changed
                        let sync_status = self.sync_status(current_syncing);
                        let msg = SubscriptionMessage::new(
                            accepted_sink.method_name(),
                            accepted_sink.subscription_id(),
                            &sync_status,
                        )
                        .map_err(SubscriptionSerializeError::new)?;

                        if accepted_sink.send(msg).await.is_err() {
                            break
                        }
                    }
                }

                Ok(())
            }
            _ => {
                // TODO: implement once https://github.com/alloy-rs/alloy/pull/3410 is released
                Err(invalid_params_rpc_err("Unsupported subscription kind"))
            }
        }
    }
}

/// TN(TN-0174): the pool listener of a `newPendingTransactions` subscription, taken before the
/// subscription is accepted so that a refusal reaches the client as an error.
enum PendingTransactionsListener<T: PoolTransaction> {
    /// Hashes of new transactions.
    Hashes(Receiver<TxHash>),
    /// New transactions, of which the subscription sends the pending ones in full.
    Full(Receiver<NewTransactionEvent<T>>),
}

impl<Eth> EthPubSub<Eth>
where
    Eth: RpcNodeCore + EthApiTypes<RpcConvert: RpcConvert<Primitives = Eth::Primitives>>,
{
    /// TN(TN-0174): checks a `newPendingTransactions` subscription's params and takes its pool
    /// listener; fails with the error the client gets when the pool refuses another one.
    fn pending_transactions_listener(
        &self,
        params: Option<Params>,
    ) -> Result<
        PendingTransactionsListener<<Eth::Pool as TransactionPool>::Transaction>,
        ErrorObject<'static>,
    > {
        let pool = self.inner.eth_api.pool();
        let kind = TransactionListenerKind::PropagateOnly;
        match params {
            Some(Params::Bool(true)) => {
                pool.try_new_transactions_listener_for(kind).map(PendingTransactionsListener::Full)
            }
            None | Some(Params::Bool(false) | Params::None) => pool
                .try_pending_transactions_listener_for(kind)
                .map(PendingTransactionsListener::Hashes),
            _ => return Err(invalid_params_rpc_err("Invalid params for newPendingTransactions")),
        }
        .ok_or_else(too_many_listeners_err)
    }

    /// TN(TN-0174): sends what `listener` receives to the subscription: hashes, or the pending
    /// transactions in full.
    async fn pipe_pending_transactions(
        &self,
        accepted_sink: SubscriptionSink,
        listener: PendingTransactionsListener<<Eth::Pool as TransactionPool>::Transaction>,
    ) -> Result<(), ErrorObject<'static>> {
        match listener {
            PendingTransactionsListener::Hashes(receiver) => {
                pipe_from_stream(accepted_sink, ReceiverStream::new(receiver)).await
            }
            PendingTransactionsListener::Full(receiver) => {
                let stream = NewSubpoolTransactionStream::new(receiver, SubPool::Pending)
                    .filter_map(|tx| {
                        let tx_value = match self
                            .inner
                            .eth_api
                            .converter()
                            .fill_pending(tx.transaction.to_consensus())
                        {
                            Ok(tx) => Some(tx),
                            Err(err) => {
                                error!(target = "rpc",
                                    %err,
                                    "Failed to fill transaction with block context"
                                );
                                None
                            }
                        };
                        std::future::ready(tx_value)
                    });
                pipe_from_stream(accepted_sink, stream).await
            }
        }
    }
}

#[async_trait::async_trait]
impl<Eth> EthPubSubApiServer<RpcTransaction<Eth::NetworkTypes>> for EthPubSub<Eth>
where
    Eth: RpcNodeCore + EthApiTypes<RpcConvert: RpcConvert<Primitives = Eth::Primitives>>,
{
    /// Handler for `eth_subscribe`
    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: SubscriptionKind,
        params: Option<Params>,
    ) -> jsonrpsee::core::SubscriptionResult {
        // TN(TN-0155): a `logs` subscription keeps its filter for the life of the connection, so
        // it takes `eth_newFilter`'s size limit, refused before the subscription is accepted.
        if let (SubscriptionKind::Logs, Some(Params::Logs(filter))) = (&kind, &params) &&
            let Err(err) = check_filter_size(filter)
        {
            pending.reject(err).await;
            return Ok(())
        }
        // TN(TN-0174): a `newPendingTransactions` subscription takes its pool listener before
        // it is accepted, so a refused one gets the error instead of an id and no events.
        if kind == SubscriptionKind::NewPendingTransactions {
            let listener = match self.pending_transactions_listener(params) {
                Ok(listener) => listener,
                Err(err) => {
                    pending.reject(err).await;
                    return Ok(())
                }
            };
            let sink = pending.accept().await?;
            let pubsub = self.clone();
            self.inner.subscription_task_spawner.spawn_task(Box::pin(async move {
                let _ = pubsub.pipe_pending_transactions(sink, listener).await;
            }));
            return Ok(())
        }
        let sink = pending.accept().await?;
        let pubsub = self.clone();
        self.inner.subscription_task_spawner.spawn_task(Box::pin(async move {
            let _ = pubsub.handle_accepted(sink, kind, params).await;
        }));

        Ok(())
    }
}

/// Helper to convert a serde error into an [`ErrorObject`]
#[derive(Debug, thiserror::Error)]
#[error("Failed to serialize subscription item: {0}")]
pub struct SubscriptionSerializeError(#[from] serde_json::Error);

impl SubscriptionSerializeError {
    const fn new(err: serde_json::Error) -> Self {
        Self(err)
    }
}

impl From<SubscriptionSerializeError> for ErrorObject<'static> {
    fn from(value: SubscriptionSerializeError) -> Self {
        internal_rpc_err(value.to_string())
    }
}

/// TN(TN-0056): the pool refused another RPC transaction listener.
fn too_many_listeners_err() -> ErrorObject<'static> {
    jsonrpsee::types::error::ErrorObject::owned(
        jsonrpsee::types::error::ErrorCode::ServerIsBusy.code(),
        "too many pending transaction listeners",
        None::<()>,
    )
}

/// TN(TN-0173): how long a subscription waits for room in its connection's send buffer before
/// it ends.
const SUBSCRIPTION_SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Pipes all stream items to the subscription sink.
async fn pipe_from_stream<T, St>(
    sink: SubscriptionSink,
    stream: St,
) -> Result<(), ErrorObject<'static>>
where
    St: Stream<Item = T> + Unpin,
    T: Serialize,
{
    pipe_from_stream_within(sink, stream, SUBSCRIPTION_SEND_TIMEOUT).await
}

/// [`pipe_from_stream`], ending the subscription once an item waits `send_timeout` for room.
///
/// TN(TN-0173): while a send waits, the stream is not polled and its source keeps what it
/// buffers for this subscriber: a full-transaction listener holds up to 1,024 pool transactions,
/// evicted ones included. A subscriber that stops reading would hold them until its connection
/// closed; it now loses the subscription instead, and the stream with its buffer is dropped.
async fn pipe_from_stream_within<T, St>(
    sink: SubscriptionSink,
    mut stream: St,
    send_timeout: Duration,
) -> Result<(), ErrorObject<'static>>
where
    St: Stream<Item = T> + Unpin,
    T: Serialize,
{
    loop {
        tokio::select! {
            _ = sink.closed() => {
                // connection dropped
                break Ok(())
            },
            maybe_item = stream.next() => {
                let item = match maybe_item {
                    Some(item) => item,
                    None => {
                        // stream ended
                        break  Ok(())
                    },
                };
                let msg = SubscriptionMessage::new(
                    sink.method_name(),
                    sink.subscription_id(),
                    &item
                ).map_err(SubscriptionSerializeError::new)?;

                if sink.send_timeout(msg, send_timeout).await.is_err() {
                    break Ok(());
                }
            }
        }
    }
}

impl<Eth> std::fmt::Debug for EthPubSub<Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthPubSub").finish_non_exhaustive()
    }
}

/// Container type `EthPubSub`
#[derive(Clone)]
struct EthPubSubInner<EthApi> {
    /// The `eth` API.
    eth_api: EthApi,
    /// The type that's used to spawn subscription tasks.
    subscription_task_spawner: Box<dyn TaskSpawner>,
}

// == impl EthPubSubInner ===

impl<Eth> EthPubSubInner<Eth>
where
    Eth: RpcNodeCore<Provider: BlockNumReader>,
{
    /// Returns the current sync status for the `syncing` subscription
    fn sync_status(&self, is_syncing: bool) -> PubSubSyncStatus {
        if is_syncing {
            let current_block = self
                .eth_api
                .provider()
                .chain_info()
                .map(|info| info.best_number)
                .unwrap_or_default();
            PubSubSyncStatus::Detailed(SyncStatusMetadata {
                syncing: true,
                starting_block: 0,
                current_block,
                highest_block: Some(current_block),
            })
        } else {
            PubSubSyncStatus::Simple(false)
        }
    }
}

impl<Eth> EthPubSubInner<Eth>
where
    Eth: RpcNodeCore<Pool: TransactionPool>,
{
    /// Returns a stream that yields all transaction hashes emitted by the txpool.
    ///
    /// TN(TN-0056): `None` when the pool refuses another RPC listener.
    fn pending_transaction_hashes_stream(&self) -> Option<impl Stream<Item = TxHash>> {
        self.eth_api
            .pool()
            .try_pending_transactions_listener_for(TransactionListenerKind::PropagateOnly)
            .map(ReceiverStream::new)
    }

    /// Returns a stream that yields all transactions emitted by the txpool.
    ///
    /// TN(TN-0056): `None` when the pool refuses another RPC listener.
    fn full_pending_transaction_stream(
        &self,
    ) -> Option<impl Stream<Item = NewTransactionEvent<<Eth::Pool as TransactionPool>::Transaction>>>
    {
        self.eth_api
            .pool()
            .try_new_transactions_listener_for(TransactionListenerKind::PropagateOnly)
            .map(|receiver| NewSubpoolTransactionStream::new(receiver, SubPool::Pending))
    }
}

impl<Eth> EthPubSubInner<Eth>
where
    Eth: EthApiTypes<RpcConvert: RpcConvert<Primitives = Eth::Primitives>> + RpcNodeCore,
{
    /// Returns a stream that yields all new RPC blocks.
    fn new_headers_stream(&self) -> impl Stream<Item = RpcHeader<Eth::NetworkTypes>> {
        let converter = self.eth_api.converter();
        self.eth_api.provider().canonical_state_stream().flat_map(|new_chain| {
            let headers = new_chain
                .committed()
                .blocks_iter()
                .filter_map(|block| {
                    match converter.convert_header(block.clone_sealed_header(), block.rlp_length())
                    {
                        Ok(header) => Some(header),
                        Err(err) => {
                            error!(target = "rpc", %err, "Failed to convert header");
                            None
                        }
                    }
                })
                .collect::<Vec<_>>();
            futures::stream::iter(headers)
        })
    }

    /// Returns a stream that yields all logs that match the given filter.
    fn log_stream(&self, filter: Filter) -> impl Stream<Item = Log> {
        self.eth_api
            .provider()
            .canonical_state_stream()
            .map(move |canon_state| canon_state.block_receipts())
            .flat_map(futures::stream::iter)
            .flat_map(move |(block_receipts, removed)| {
                let all_logs = logs_utils::matching_block_logs_with_tx_hashes(
                    &filter,
                    block_receipts.block,
                    block_receipts.timestamp,
                    block_receipts.tx_receipts.iter().map(|(tx, receipt)| (*tx, receipt)),
                    removed,
                );
                futures::stream::iter(all_logs)
            })
    }
}

/// TN(TN-0173): a subscriber that stops reading loses the subscription.
#[cfg(test)]
mod send_timeout_tests {
    use super::*;
    use jsonrpsee::RpcModule;
    use std::{
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };

    /// A stream of zeros that never ends and records when it is dropped.
    struct Endless(Arc<AtomicBool>);

    impl Stream for Endless {
        type Item = u64;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<u64>> {
            Poll::Ready(Some(0))
        }
    }

    impl Drop for Endless {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// The subscriber's buffer of four messages is never read: once it is full, the next send
    /// waits the timeout, the subscription ends and its stream is dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_subscriber_that_stops_reading_loses_the_subscription() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut module = RpcModule::new(dropped.clone());
        module
            .register_subscription(
                "test_subscribe",
                "test_item",
                "test_unsubscribe",
                |_, pending, dropped, _| async move {
                    let sink = pending.accept().await?;
                    let stream = Endless(Arc::clone(&dropped));
                    let _ = pipe_from_stream_within(sink, stream, Duration::from_millis(50)).await;
                    jsonrpsee::core::SubscriptionResult::Ok(())
                },
            )
            .unwrap();

        let _subscription = module.subscribe("test_subscribe", Vec::<u8>::new(), 4).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !dropped.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stream is dropped once the buffer stays full");
    }
}

/// TN(TN-0174): a refused `newPendingTransactions` subscription gets the error.
#[cfg(test)]
mod pending_listener_tests {
    use super::*;
    use crate::EthApiBuilder;
    use reth_chainspec::ChainSpecProvider;
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_provider::test_utils::MockEthProvider;
    use reth_transaction_pool::test_utils::testing_pool;

    /// With every RPC listener slot of the pool taken, both kinds of subscription are refused
    /// with -32009 instead of being accepted; once a slot is free they are accepted again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_pending_transactions_subscription_gets_the_error() {
        let provider = MockEthProvider::default();
        let api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        let module = EthPubSub::new(api.clone()).into_rpc();
        let pool = api.pool().clone();
        let kind = TransactionListenerKind::PropagateOnly;
        let hashes: Vec<_> =
            std::iter::from_fn(|| pool.try_pending_transactions_listener_for(kind)).collect();
        let full: Vec<_> =
            std::iter::from_fn(|| pool.try_new_transactions_listener_for(kind)).collect();
        assert!(!hashes.is_empty() && !full.is_empty());

        let hashes_params = ("newPendingTransactions",);
        let full_params = ("newPendingTransactions", true);
        for result in [
            module.subscribe_unbounded("eth_subscribe", hashes_params).await,
            module.subscribe_unbounded("eth_subscribe", full_params).await,
        ] {
            let Err(jsonrpsee::MethodsError::JsonRpc(err)) = result else {
                panic!("expected the refusal, got {result:?}")
            };
            assert_eq!(err.code(), jsonrpsee::types::error::ErrorCode::ServerIsBusy.code());
            assert_eq!(err.message(), "too many pending transaction listeners");
        }

        drop((hashes, full));
        module.subscribe_unbounded("eth_subscribe", hashes_params).await.unwrap();
        module.subscribe_unbounded("eth_subscribe", full_params).await.unwrap();
    }
}
