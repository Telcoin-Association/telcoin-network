use alloy_consensus::{transaction::TxHashRef, BlockHeader};
use alloy_eip7928::BlockAccessList;
use alloy_eips::{eip2718::Encodable2718, BlockId, BlockNumberOrTag};
use alloy_evm::env::BlockEnvironment;
use alloy_genesis::ChainConfig;
use alloy_primitives::{hex::decode, uint, Address, Bytes, B256, U64};
use alloy_rlp::{Decodable, Encodable};
use alloy_rpc_types::BlockTransactionsKind;
use alloy_rpc_types_debug::ExecutionWitness;
use alloy_rpc_types_eth::{state::EvmOverrides, BlockError, Bundle, StateContext};
use alloy_rpc_types_trace::geth::{
    BlockTraceResult, GethDebugTracingCallOptions, GethDebugTracingOptions, GethTrace, TraceResult,
};
use async_trait::async_trait;
use futures::Stream;
use jsonrpsee::core::RpcResult;
use parking_lot::RwLock;
use reth_chainspec::{ChainSpecProvider, EthChainSpec, EthereumHardforks};
use reth_engine_primitives::ConsensusEngineEvent;
use reth_errors::RethError;
use reth_evm::{execute::Executor, ConfigureEvm, EvmEnvFor};
use reth_primitives_traits::{
    Block as BlockTrait, BlockBody, BlockTy, ReceiptWithBloom, RecoveredBlock,
};
use reth_revm::{db::State, witness::ExecutionWitnessRecord};
use reth_rpc_api::DebugApiServer;
use reth_rpc_convert::RpcTxReq;
use reth_rpc_eth_api::{
    helpers::{EthTransactions, TraceExt},
    FromEthApiError, RpcConvert, RpcNodeCore,
};
use reth_rpc_eth_types::EthApiError;
use reth_rpc_server_types::{result::internal_rpc_err, ToRpcResult};
use reth_storage_api::{
    BlockIdReader, BlockReaderIdExt, HeaderProvider, ProviderBlock, ReceiptProviderIdExt,
    StateProofProvider, StateProviderFactory, StateRootProvider, TransactionVariant,
};
use reth_tasks::{pool::BlockingTaskGuard, TaskSpawner};
use reth_trie_common::{updates::TrieUpdates, HashedPostState};
use revm::DatabaseCommit;
use revm_inspectors::tracing::{DebugInspector, TransactionContext};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{AcquireError, OwnedSemaphorePermit};
use tokio_stream::StreamExt;
#[cfg(feature = "js-tracer")]
use {
    alloy_rpc_types_trace::geth::GethDebugTracerType,
    js_interrupt::{Interruptible, JsInspectorInterrupt},
    std::time::Duration,
};

// TN(TN-0017): not at debug/js_interrupt.rs because TN's root .gitignore ignores `debug/`.
#[cfg(feature = "js-tracer")]
#[path = "js_tracer_interrupt.rs"]
mod js_interrupt;

/// `debug` API implementation.
///
/// This type provides the functionality for handling `debug` related requests.
pub struct DebugApi<Eth: RpcNodeCore> {
    inner: Arc<DebugApiInner<Eth>>,
}

impl<Eth> DebugApi<Eth>
where
    Eth: RpcNodeCore,
{
    /// Create a new instance of the [`DebugApi`]
    pub fn new(
        eth_api: Eth,
        blocking_task_guard: BlockingTaskGuard,
        executor: impl TaskSpawner,
        mut stream: impl Stream<Item = ConsensusEngineEvent<Eth::Primitives>> + Send + Unpin + 'static,
    ) -> Self {
        let bad_block_store = BadBlockStore::default();
        let inner = Arc::new(DebugApiInner {
            eth_api,
            blocking_task_guard,
            bad_block_store: bad_block_store.clone(),
        });

        // Spawn a task caching bad blocks
        executor.spawn_task(Box::pin(async move {
            while let Some(event) = stream.next().await {
                if let ConsensusEngineEvent::InvalidBlock(block) = event &&
                    let Ok(recovered) =
                        RecoveredBlock::try_recover_sealed(block.as_ref().clone())
                {
                    bad_block_store.insert(recovered);
                }
            }
        }));

        Self { inner }
    }

    /// Access the underlying `Eth` API.
    pub fn eth_api(&self) -> &Eth {
        &self.inner.eth_api
    }

    /// Access the underlying provider.
    pub fn provider(&self) -> &Eth::Provider {
        self.inner.eth_api.provider()
    }
}

// === impl DebugApi ===

impl<Eth> DebugApi<Eth>
where
    Eth: TraceExt,
{
    /// Acquires a permit to execute a tracing call.
    async fn acquire_trace_permit(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        self.inner.blocking_task_guard.clone().acquire_owned().await
    }

    /// Acquires the tracing permit a trace moves into its blocking task.
    ///
    /// TN(TN-0036): upstream's RPC handlers hold the permit only while the request is open, so a
    /// trace that times out or loses its client frees it while its thread runs on. Each trace
    /// method takes the permit here, once, and its blocking task drops it when the work ends.
    async fn trace_task_permit(&self) -> Result<OwnedSemaphorePermit, Eth::Error> {
        self.acquire_trace_permit().await.map_err(|_| EthApiError::InternalEthError.into())
    }

    /// Trace the entire block asynchronously
    async fn trace_block(
        &self,
        block: Arc<RecoveredBlock<ProviderBlock<Eth::Provider>>>,
        evm_env: EvmEnvFor<Eth::Evm>,
        opts: GethDebugTracingOptions,
        permit: OwnedSemaphorePermit,
    ) -> Result<Vec<TraceResult>, Eth::Error> {
        self.eth_api()
            .spawn_with_state_at_block(block.parent_hash(), move |eth_api, mut db| {
                let _permit = permit;
                let mut results = Vec::with_capacity(block.body().transactions().len());

                eth_api.apply_pre_execution_changes(&block, &mut db, &evm_env)?;

                let mut transactions = block.transactions_recovered().enumerate().peekable();
                let mut inspector = DebugInspector::new(opts).map_err(Eth::Error::from_eth_err)?;
                while let Some((index, tx)) = transactions.next() {
                    let tx_hash = *tx.tx_hash();
                    let tx_env = eth_api.evm_config().tx_env(tx);

                    let res = eth_api.inspect(
                        &mut db,
                        evm_env.clone(),
                        tx_env.clone(),
                        &mut inspector,
                    )?;
                    let result = inspector
                        .get_result(
                            Some(TransactionContext {
                                block_hash: Some(block.hash()),
                                tx_hash: Some(tx_hash),
                                tx_index: Some(index),
                            }),
                            &tx_env,
                            &evm_env.block_env,
                            &res,
                            &mut db,
                        )
                        .map_err(Eth::Error::from_eth_err)?;

                    results.push(TraceResult::Success { result, tx_hash: Some(tx_hash) });
                    if transactions.peek().is_some() {
                        inspector.fuse().map_err(Eth::Error::from_eth_err)?;
                        // need to apply the state changes of this transaction before executing the
                        // next transaction
                        db.commit(res.state)
                    }
                }

                Ok(results)
            })
            .await
    }

    /// Replays the given block and returns the trace of each transaction.
    ///
    /// This expects a rlp encoded block
    ///
    /// Note, the parent of this block must be present, or it will fail.
    pub async fn debug_trace_raw_block(
        &self,
        rlp_block: Bytes,
        opts: GethDebugTracingOptions,
    ) -> Result<Vec<TraceResult>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let block: ProviderBlock<Eth::Provider> = Decodable::decode(&mut rlp_block.as_ref())
            .map_err(BlockError::RlpDecodeRawBlock)
            .map_err(Eth::Error::from_eth_err)?;

        let evm_env = self
            .eth_api()
            .evm_config()
            .evm_env(block.header())
            .map_err(RethError::other)
            .map_err(Eth::Error::from_eth_err)?;

        // Depending on EIP-2 we need to recover the transactions differently
        let senders =
            if self.provider().chain_spec().is_homestead_active_at_block(block.header().number()) {
                block.body().recover_signers()
            } else {
                block.body().recover_signers_unchecked()
            }
            .map_err(Eth::Error::from_eth_err)?;

        self.trace_block(
            Arc::new(block.into_recovered_with_signers(senders)),
            evm_env,
            opts,
            permit,
        )
        .await
    }

    /// Replays a block and returns the trace of each transaction.
    pub async fn debug_trace_block(
        &self,
        block_id: BlockId,
        opts: GethDebugTracingOptions,
    ) -> Result<Vec<TraceResult>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let block_hash = self
            .provider()
            .block_hash_for_id(block_id)
            .map_err(Eth::Error::from_eth_err)?
            .ok_or(EthApiError::HeaderNotFound(block_id))?;

        let ((evm_env, _), block) = futures::try_join!(
            self.eth_api().evm_env_at(block_hash.into()),
            self.eth_api().recovered_block(block_hash.into()),
        )?;

        let block = block.ok_or(EthApiError::HeaderNotFound(block_id))?;

        self.trace_block(block, evm_env, opts, permit).await
    }

    /// Trace the transaction according to the provided options.
    ///
    /// Ref: <https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers>
    pub async fn debug_trace_transaction(
        &self,
        tx_hash: B256,
        opts: GethDebugTracingOptions,
    ) -> Result<GethTrace, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let (transaction, block) = match self.eth_api().transaction_and_block(tx_hash).await? {
            None => return Err(EthApiError::TransactionNotFound.into()),
            Some(res) => res,
        };
        let (evm_env, _) = self.eth_api().evm_env_at(block.hash().into()).await?;

        // we need to get the state of the parent block because we're essentially replaying the
        // block the transaction is included in
        let state_at: BlockId = block.parent_hash().into();
        let block_hash = block.hash();

        let interrupt = JsTraceInterrupt::new(&opts)?;
        let tracer_interrupt = interrupt.clone();
        let trace = self.eth_api().spawn_with_state_at_block(state_at, move |eth_api, mut db| {
            let _permit = permit;
            let block_txs = block.transactions_recovered();

            // configure env for the target transaction
            let tx = transaction.into_recovered();

            eth_api.apply_pre_execution_changes(&block, &mut db, &evm_env)?;

            // replay all transactions prior to the targeted transaction
            let index = eth_api.replay_transactions_until(
                &mut db,
                evm_env.clone(),
                block_txs,
                *tx.tx_hash(),
            )?;

            let tx_env = eth_api.evm_config().tx_env(&tx);

            let mut inspector = tracer_interrupt
                .attach(DebugInspector::new(opts).map_err(Eth::Error::from_eth_err)?);
            let res = eth_api.inspect(&mut db, evm_env.clone(), tx_env.clone(), &mut inspector)?;
            let trace = inspector
                .get_result(
                    Some(TransactionContext {
                        block_hash: Some(block_hash),
                        tx_index: Some(index),
                        tx_hash: Some(*tx.tx_hash()),
                    }),
                    &tx_env,
                    &evm_env.block_env,
                    &res,
                    &mut db,
                )
                .map_err(Eth::Error::from_eth_err)?;

            Ok(trace)
        });
        interrupt.run(trace).await
    }

    /// The `debug_traceCall` method lets you run an `eth_call` within the context of the given
    /// block execution using the final state of parent block as the base.
    ///
    /// If `tx_index` is provided in opts, the call will be traced at the state after executing
    /// transactions up to the specified index within the block (0-indexed).
    /// If not provided, then uses the post-state (default behavior).
    ///
    /// Differences compare to `eth_call`:
    ///  - `debug_traceCall` executes with __enabled__ basefee check, `eth_call` does not: <https://github.com/paradigmxyz/reth/issues/6240>
    pub async fn debug_trace_call(
        &self,
        call: RpcTxReq<Eth::NetworkTypes>,
        block_id: Option<BlockId>,
        opts: GethDebugTracingCallOptions,
    ) -> Result<GethTrace, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let at = block_id.unwrap_or_default();
        let GethDebugTracingCallOptions {
            tracing_options,
            state_overrides,
            block_overrides,
            tx_index,
        } = opts;
        let overrides = EvmOverrides::new(state_overrides, block_overrides.map(Box::new));

        // Check if we need to replay transactions for a specific tx_index
        if let Some(tx_idx) = tx_index {
            return self
                .debug_trace_call_at_tx_index(
                    call,
                    at,
                    tx_idx as usize,
                    tracing_options,
                    overrides,
                    permit,
                )
                .await;
        }

        let interrupt = JsTraceInterrupt::new(&tracing_options)?;
        let tracer_interrupt = interrupt.clone();
        let this = self.clone();
        let trace =
            self.eth_api().spawn_with_call_at(call, at, overrides, move |db, evm_env, tx_env| {
                let _permit = permit;
                let mut inspector = tracer_interrupt.attach(
                    DebugInspector::new(tracing_options).map_err(Eth::Error::from_eth_err)?,
                );
                let res = this.eth_api().inspect(
                    &mut *db,
                    evm_env.clone(),
                    tx_env.clone(),
                    &mut inspector,
                )?;
                let trace = inspector
                    .get_result(None, &tx_env, &evm_env.block_env, &res, db)
                    .map_err(Eth::Error::from_eth_err)?;
                Ok(trace)
            });
        interrupt.run(trace).await
    }

    /// Helper method to execute `debug_trace_call` at a specific transaction index within a block.
    /// This replays transactions up to the specified index, then executes the trace call in that
    /// state.
    async fn debug_trace_call_at_tx_index(
        &self,
        call: RpcTxReq<Eth::NetworkTypes>,
        block_id: BlockId,
        tx_index: usize,
        tracing_options: GethDebugTracingOptions,
        overrides: EvmOverrides,
        permit: OwnedSemaphorePermit,
    ) -> Result<GethTrace, Eth::Error> {
        // Get the target block to check transaction count
        let block = self
            .eth_api()
            .recovered_block(block_id)
            .await?
            .ok_or(EthApiError::HeaderNotFound(block_id))?;

        if tx_index >= block.transaction_count() {
            // tx_index out of bounds
            return Err(EthApiError::InvalidParams(format!(
                "tx_index {} out of bounds for block with {} transactions",
                tx_index,
                block.transaction_count()
            ))
            .into())
        }

        let (evm_env, _) = self.eth_api().evm_env_at(block.hash().into()).await?;

        // execute after the parent block, replaying `tx_index` transactions
        let state_at = block.parent_hash();

        let interrupt = JsTraceInterrupt::new(&tracing_options)?;
        let tracer_interrupt = interrupt.clone();
        let trace = self.eth_api().spawn_with_state_at_block(state_at, move |eth_api, mut db| {
            let _permit = permit;
            // 1. apply pre-execution changes
            eth_api.apply_pre_execution_changes(&block, &mut db, &evm_env)?;

            // 2. replay the required number of transactions
            for tx in block.transactions_recovered().take(tx_index) {
                let tx_env = eth_api.evm_config().tx_env(tx);
                let res = eth_api.transact(&mut db, evm_env.clone(), tx_env)?;
                db.commit(res.state);
            }

            // 3. now execute the trace call on this state
            let (evm_env, tx_env) = eth_api.prepare_call_env(evm_env, call, &mut db, overrides)?;

            let mut inspector = tracer_interrupt
                .attach(DebugInspector::new(tracing_options).map_err(Eth::Error::from_eth_err)?);
            let res = eth_api.inspect(&mut db, evm_env.clone(), tx_env.clone(), &mut inspector)?;
            let trace = inspector
                .get_result(None, &tx_env, &evm_env.block_env, &res, &mut db)
                .map_err(Eth::Error::from_eth_err)?;

            Ok(trace)
        });
        interrupt.run(trace).await
    }

    /// The `debug_traceCallMany` method lets you run an `eth_callMany` within the context of the
    /// given block execution using the first n transactions in the given block as base.
    /// Each following bundle increments block number by 1 and block timestamp by 12 seconds
    pub async fn debug_trace_call_many(
        &self,
        bundles: Vec<Bundle<RpcTxReq<Eth::NetworkTypes>>>,
        state_context: Option<StateContext>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> Result<Vec<Vec<GethTrace>>, Eth::Error> {
        if bundles.is_empty() {
            return Err(EthApiError::InvalidParams(String::from("bundles are empty.")).into())
        }

        let permit = self.trace_task_permit().await?;
        let StateContext { transaction_index, block_number } = state_context.unwrap_or_default();
        let transaction_index = transaction_index.unwrap_or_default();

        let target_block = block_number.unwrap_or_default();
        let ((mut evm_env, _), block) = futures::try_join!(
            self.eth_api().evm_env_at(target_block),
            self.eth_api().recovered_block(target_block),
        )?;

        let opts = opts.unwrap_or_default();
        let block = block.ok_or(EthApiError::HeaderNotFound(target_block))?;
        let GethDebugTracingCallOptions { tracing_options, mut state_overrides, .. } = opts;

        // we're essentially replaying the transactions in the block here, hence we need the state
        // that points to the beginning of the block, which is the state at the parent block
        let mut at = block.parent_hash();
        let mut replay_block_txs = true;

        // if a transaction index is provided, we need to replay the transactions until the index
        let num_txs =
            transaction_index.index().unwrap_or_else(|| block.body().transactions().len());
        // but if all transactions are to be replayed, we can use the state at the block itself
        // this works with the exception of the PENDING block, because its state might not exist if
        // built locally
        if !target_block.is_pending() && num_txs == block.body().transactions().len() {
            at = block.hash();
            replay_block_txs = false;
        }

        self.eth_api()
            .spawn_with_state_at_block(at, move |eth_api, mut db| {
                let _permit = permit;
                // the outer vec for the bundles
                let mut all_bundles = Vec::with_capacity(bundles.len());

                if replay_block_txs {
                    // only need to replay the transactions in the block if not all transactions are
                    // to be replayed
                    let transactions = block.transactions_recovered().take(num_txs);

                    // Execute all transactions until index
                    for tx in transactions {
                        let tx_env = eth_api.evm_config().tx_env(tx);
                        let res = eth_api.transact(&mut db, evm_env.clone(), tx_env)?;
                        db.commit(res.state);
                    }
                }

                // Trace all bundles
                let mut bundles = bundles.into_iter().peekable();
                let mut inspector = DebugInspector::new(tracing_options.clone())
                    .map_err(Eth::Error::from_eth_err)?;
                while let Some(bundle) = bundles.next() {
                    let mut results = Vec::with_capacity(bundle.transactions.len());
                    let Bundle { transactions, block_override } = bundle;

                    let block_overrides = block_override.map(Box::new);

                    let mut transactions = transactions.into_iter().peekable();
                    while let Some(tx) = transactions.next() {
                        // apply state overrides only once, before the first transaction
                        let state_overrides = state_overrides.take();
                        let overrides = EvmOverrides::new(state_overrides, block_overrides.clone());

                        let (evm_env, tx_env) =
                            eth_api.prepare_call_env(evm_env.clone(), tx, &mut db, overrides)?;

                        let res = eth_api.inspect(
                            &mut db,
                            evm_env.clone(),
                            tx_env.clone(),
                            &mut inspector,
                        )?;
                        let trace = inspector
                            .get_result(None, &tx_env, &evm_env.block_env, &res, &mut db)
                            .map_err(Eth::Error::from_eth_err)?;

                        // If there is more transactions, commit the database
                        // If there is no transactions, but more bundles, commit to the database too
                        if transactions.peek().is_some() || bundles.peek().is_some() {
                            inspector.fuse().map_err(Eth::Error::from_eth_err)?;
                            db.commit(res.state);
                        }
                        results.push(trace);
                    }
                    // Increment block_env number and timestamp for the next bundle
                    evm_env.block_env.inner_mut().number += uint!(1_U256);
                    evm_env.block_env.inner_mut().timestamp += uint!(12_U256);

                    all_bundles.push(results);
                }
                Ok(all_bundles)
            })
            .await
    }

    /// Generates an execution witness for the given block hash. see
    /// [`Self::debug_execution_witness`] for more info.
    pub async fn debug_execution_witness_by_block_hash(
        &self,
        hash: B256,
    ) -> Result<ExecutionWitness, Eth::Error> {
        let this = self.clone();
        let block = this
            .eth_api()
            .recovered_block(hash.into())
            .await?
            .ok_or(EthApiError::HeaderNotFound(hash.into()))?;

        self.debug_execution_witness_for_block(block).await
    }

    /// The `debug_executionWitness` method allows for re-execution of a block with the purpose of
    /// generating an execution witness. The witness comprises of a map of all hashed trie nodes to
    /// their preimages that were required during the execution of the block, including during state
    /// root recomputation.
    pub async fn debug_execution_witness(
        &self,
        block_id: BlockNumberOrTag,
    ) -> Result<ExecutionWitness, Eth::Error> {
        let this = self.clone();
        let block = this
            .eth_api()
            .recovered_block(block_id.into())
            .await?
            .ok_or(EthApiError::HeaderNotFound(block_id.into()))?;

        self.debug_execution_witness_for_block(block).await
    }

    /// Generates an execution witness, using the given recovered block.
    pub async fn debug_execution_witness_for_block(
        &self,
        block: Arc<RecoveredBlock<ProviderBlock<Eth::Provider>>>,
    ) -> Result<ExecutionWitness, Eth::Error> {
        let block_number = block.header().number();

        let (mut exec_witness, lowest_block_number) = self
            .eth_api()
            .spawn_with_state_at_block(block.parent_hash(), move |eth_api, mut db| {
                let block_executor = eth_api.evm_config().executor(&mut db);

                let mut witness_record = ExecutionWitnessRecord::default();

                let _ = block_executor
                    .execute_with_state_closure(&block, |statedb: &State<_>| {
                        witness_record.record_executed_state(statedb);
                    })
                    .map_err(|err| EthApiError::Internal(err.into()))?;

                let ExecutionWitnessRecord { hashed_state, codes, keys, lowest_block_number } =
                    witness_record;

                let state = db
                    .database
                    .0
                    .witness(Default::default(), hashed_state)
                    .map_err(EthApiError::from)?;
                Ok((
                    ExecutionWitness { state, codes, keys, ..Default::default() },
                    lowest_block_number,
                ))
            })
            .await?;

        let smallest = match lowest_block_number {
            Some(smallest) => smallest,
            None => {
                // Return only the parent header, if there were no calls to the
                // BLOCKHASH opcode.
                block_number.saturating_sub(1)
            }
        };

        let range = smallest..block_number;
        exec_witness.headers = self
            .provider()
            .headers_range(range)
            .map_err(EthApiError::from)?
            .into_iter()
            .map(|header| {
                let mut serialized_header = Vec::new();
                header.encode(&mut serialized_header);
                serialized_header.into()
            })
            .collect();

        Ok(exec_witness)
    }

    /// Returns the code associated with a given hash at the specified block ID. If no code is
    /// found, it returns None. If no block ID is provided, it defaults to the latest block.
    pub async fn debug_code_by_hash(
        &self,
        hash: B256,
        block_id: Option<BlockId>,
    ) -> Result<Option<Bytes>, Eth::Error> {
        Ok(self
            .provider()
            .state_by_block_id(block_id.unwrap_or_default())
            .map_err(Eth::Error::from_eth_err)?
            .bytecode_by_hash(&hash)
            .map_err(Eth::Error::from_eth_err)?
            .map(|b| b.original_bytes()))
    }

    /// Returns the state root of the `HashedPostState` on top of the state for the given block with
    /// trie updates.
    async fn debug_state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
        block_id: Option<BlockId>,
    ) -> Result<(B256, TrieUpdates), Eth::Error> {
        self.inner
            .eth_api
            .spawn_blocking_io(move |this| {
                let state = this
                    .provider()
                    .state_by_block_id(block_id.unwrap_or_default())
                    .map_err(Eth::Error::from_eth_err)?;
                state.state_root_with_updates(hashed_state).map_err(Eth::Error::from_eth_err)
            })
            .await
    }
}

#[async_trait]
impl<Eth> DebugApiServer<RpcTxReq<Eth::NetworkTypes>> for DebugApi<Eth>
where
    Eth: EthTransactions + TraceExt,
{
    /// Handler for `debug_getRawHeader`
    async fn raw_header(&self, block_id: BlockId) -> RpcResult<Bytes> {
        let header = match block_id {
            BlockId::Hash(hash) => self.provider().header(hash.into()).to_rpc_result()?,
            BlockId::Number(number_or_tag) => {
                let number = self
                    .provider()
                    .convert_block_number(number_or_tag)
                    .to_rpc_result()?
                    .ok_or_else(|| {
                    internal_rpc_err("Pending block not supported".to_string())
                })?;
                self.provider().header_by_number(number).to_rpc_result()?
            }
        };

        let mut res = Vec::new();
        if let Some(header) = header {
            header.encode(&mut res);
        }

        Ok(res.into())
    }

    /// Handler for `debug_getRawBlock`
    async fn raw_block(&self, block_id: BlockId) -> RpcResult<Bytes> {
        let block = self
            .provider()
            .block_by_id(block_id)
            .to_rpc_result()?
            .ok_or(EthApiError::HeaderNotFound(block_id))?;
        let mut res = Vec::new();
        block.encode(&mut res);
        Ok(res.into())
    }

    /// Handler for `debug_getRawTransaction`
    ///
    /// If this is a pooled EIP-4844 transaction, the blob sidecar is included.
    ///
    /// Returns the bytes of the transaction for the given hash.
    async fn raw_transaction(&self, hash: B256) -> RpcResult<Option<Bytes>> {
        self.eth_api().raw_transaction_by_hash(hash).await.map_err(Into::into)
    }

    /// Handler for `debug_getRawTransactions`
    /// Returns the bytes of the transaction for the given hash.
    async fn raw_transactions(&self, block_id: BlockId) -> RpcResult<Vec<Bytes>> {
        let block: RecoveredBlock<BlockTy<Eth::Primitives>> = self
            .provider()
            .block_with_senders_by_id(block_id, TransactionVariant::NoHash)
            .to_rpc_result()?
            .unwrap_or_default();
        Ok(block.into_transactions_recovered().map(|tx| tx.encoded_2718().into()).collect())
    }

    /// Handler for `debug_getRawReceipts`
    async fn raw_receipts(&self, block_id: BlockId) -> RpcResult<Vec<Bytes>> {
        Ok(self
            .provider()
            .receipts_by_block_id(block_id)
            .to_rpc_result()?
            .unwrap_or_default()
            .into_iter()
            .map(|receipt| ReceiptWithBloom::from(receipt).encoded_2718().into())
            .collect())
    }

    /// Handler for `debug_getBadBlocks`
    async fn bad_blocks(&self) -> RpcResult<Vec<serde_json::Value>> {
        let blocks = self.inner.bad_block_store.all();
        let mut bad_blocks = Vec::with_capacity(blocks.len());

        #[derive(Serialize, Deserialize)]
        struct BadBlockSerde<T> {
            block: T,
            hash: B256,
            rlp: Bytes,
        }

        for block in blocks {
            let rlp = alloy_rlp::encode(block.sealed_block()).into();
            let hash = block.hash();

            let block = block
                .clone_into_rpc_block(
                    BlockTransactionsKind::Full,
                    |tx, tx_info| self.eth_api().converter().fill(tx, tx_info),
                    |header, size| self.eth_api().converter().convert_header(header, size),
                )
                .map_err(|err| Eth::Error::from(err).into())?;

            let bad_block = serde_json::to_value(BadBlockSerde { block, hash, rlp })
                .map_err(|err| EthApiError::other(internal_rpc_err(err.to_string())))?;

            bad_blocks.push(bad_block);
        }

        Ok(bad_blocks)
    }

    /// Handler for `debug_traceChain`
    async fn debug_trace_chain(
        &self,
        _start_exclusive: BlockNumberOrTag,
        _end_inclusive: BlockNumberOrTag,
    ) -> RpcResult<Vec<BlockTraceResult>> {
        Err(internal_rpc_err("unimplemented"))
    }

    /// Handler for `debug_traceBlock`
    async fn debug_trace_block(
        &self,
        rlp_block: Bytes,
        opts: Option<GethDebugTracingOptions>,
    ) -> RpcResult<Vec<TraceResult>> {
        // TN(TN-0036): the trace takes its permit itself and holds it in its blocking task.
        Self::debug_trace_raw_block(self, rlp_block, opts.unwrap_or_default())
            .await
            .map_err(Into::into)
    }

    /// Handler for `debug_traceBlockByHash`
    async fn debug_trace_block_by_hash(
        &self,
        block: B256,
        opts: Option<GethDebugTracingOptions>,
    ) -> RpcResult<Vec<TraceResult>> {
        // TN(TN-0036): the trace takes its permit itself and holds it in its blocking task.
        Self::debug_trace_block(self, block.into(), opts.unwrap_or_default())
            .await
            .map_err(Into::into)
    }

    /// Handler for `debug_traceBlockByNumber`
    async fn debug_trace_block_by_number(
        &self,
        block: BlockNumberOrTag,
        opts: Option<GethDebugTracingOptions>,
    ) -> RpcResult<Vec<TraceResult>> {
        // TN(TN-0036): the trace takes its permit itself and holds it in its blocking task.
        Self::debug_trace_block(self, block.into(), opts.unwrap_or_default())
            .await
            .map_err(Into::into)
    }

    /// Handler for `debug_traceTransaction`
    async fn debug_trace_transaction(
        &self,
        tx_hash: B256,
        opts: Option<GethDebugTracingOptions>,
    ) -> RpcResult<GethTrace> {
        // TN(TN-0036): the trace takes its permit itself and holds it in its blocking task.
        Self::debug_trace_transaction(self, tx_hash, opts.unwrap_or_default())
            .await
            .map_err(Into::into)
    }

    /// Handler for `debug_traceCall`
    async fn debug_trace_call(
        &self,
        request: RpcTxReq<Eth::NetworkTypes>,
        block_id: Option<BlockId>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<GethTrace> {
        // TN(TN-0036): the trace takes its permit itself and holds it in its blocking task.
        Self::debug_trace_call(self, request, block_id, opts.unwrap_or_default())
            .await
            .map_err(Into::into)
    }

    async fn debug_trace_call_many(
        &self,
        bundles: Vec<Bundle<RpcTxReq<Eth::NetworkTypes>>>,
        state_context: Option<StateContext>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<Vec<Vec<GethTrace>>> {
        // TN(TN-0036): the trace takes its permit itself and holds it in its blocking task.
        Self::debug_trace_call_many(self, bundles, state_context, opts).await.map_err(Into::into)
    }

    /// Handler for `debug_executionWitness`
    async fn debug_execution_witness(
        &self,
        block: BlockNumberOrTag,
    ) -> RpcResult<ExecutionWitness> {
        let _permit = self.acquire_trace_permit().await;
        Self::debug_execution_witness(self, block).await.map_err(Into::into)
    }

    /// Handler for `debug_executionWitnessByBlockHash`
    async fn debug_execution_witness_by_block_hash(
        &self,
        hash: B256,
    ) -> RpcResult<ExecutionWitness> {
        let _permit = self.acquire_trace_permit().await;
        Self::debug_execution_witness_by_block_hash(self, hash).await.map_err(Into::into)
    }

    async fn debug_get_block_access_list(&self, _block_id: BlockId) -> RpcResult<BlockAccessList> {
        Err(internal_rpc_err("unimplemented"))
    }

    async fn debug_backtrace_at(&self, _location: &str) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_account_range(
        &self,
        _block_number: BlockNumberOrTag,
        _start: Bytes,
        _max_results: u64,
        _nocode: bool,
        _nostorage: bool,
        _incompletes: bool,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_block_profile(&self, _file: String, _seconds: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_chaindb_compact(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_chain_config(&self) -> RpcResult<ChainConfig> {
        Ok(self.provider().chain_spec().genesis().config.clone())
    }

    async fn debug_chaindb_property(&self, _property: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_code_by_hash(
        &self,
        hash: B256,
        block_id: Option<BlockId>,
    ) -> RpcResult<Option<Bytes>> {
        Self::debug_code_by_hash(self, hash, block_id).await.map_err(Into::into)
    }

    async fn debug_cpu_profile(&self, _file: String, _seconds: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_db_ancient(&self, _kind: String, _number: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_db_ancients(&self) -> RpcResult<()> {
        Ok(())
    }

    /// `debug_db_get` - database key lookup
    ///
    /// Currently supported:
    /// * Contract bytecode associated with a code hash. The key format is: `<0x63><code_hash>`
    ///     * Prefix byte: 0x63 (required)
    ///     * Code hash: 32 bytes
    ///   Must be provided as either:
    ///     * Hex string: "0x63..." (66 hex characters after 0x)
    ///     * Raw byte string: raw byte string (33 bytes)
    ///   See Geth impl: <https://github.com/ethereum/go-ethereum/blob/737ffd1bf0cbee378d0111a5b17ae4724fb2216c/core/rawdb/schema.go#L120>
    async fn debug_db_get(&self, key: String) -> RpcResult<Option<Bytes>> {
        let key_bytes = if key.starts_with("0x") {
            decode(&key).map_err(|_| EthApiError::InvalidParams("Invalid hex key".to_string()))?
        } else {
            key.into_bytes()
        };

        if key_bytes.len() != 33 {
            return Err(EthApiError::InvalidParams(format!(
                "Key must be 33 bytes, got {}",
                key_bytes.len()
            ))
            .into());
        }
        if key_bytes[0] != 0x63 {
            return Err(EthApiError::InvalidParams("Key prefix must be 0x63".to_string()).into());
        }

        let code_hash = B256::from_slice(&key_bytes[1..33]);

        // No block ID is provided, so it defaults to the latest block
        self.debug_code_by_hash(code_hash, None).await.map_err(Into::into)
    }

    async fn debug_dump_block(&self, _number: BlockId) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_free_os_memory(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_freeze_client(&self, _node: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_gc_stats(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_get_accessible_state(
        &self,
        _from: BlockNumberOrTag,
        _to: BlockNumberOrTag,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_get_modified_accounts_by_hash(
        &self,
        _start_hash: B256,
        _end_hash: B256,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_get_modified_accounts_by_number(
        &self,
        _start_number: u64,
        _end_number: u64,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_go_trace(&self, _file: String, _seconds: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_intermediate_roots(
        &self,
        _block_hash: B256,
        _opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_mem_stats(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_mutex_profile(&self, _file: String, _nsec: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_preimage(&self, _hash: B256) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_print_block(&self, _number: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_seed_hash(&self, _number: u64) -> RpcResult<B256> {
        Ok(Default::default())
    }

    async fn debug_set_block_profile_rate(&self, _rate: u64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_set_gc_percent(&self, _v: i32) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_set_head(&self, _number: U64) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_set_mutex_profile_fraction(&self, _rate: i32) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_set_trie_flush_interval(&self, _interval: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_stacks(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_standard_trace_bad_block_to_file(
        &self,
        _block: BlockNumberOrTag,
        _opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_standard_trace_block_to_file(
        &self,
        _block: BlockNumberOrTag,
        _opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_start_cpu_profile(&self, _file: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_start_go_trace(&self, _file: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
        block_id: Option<BlockId>,
    ) -> RpcResult<(B256, TrieUpdates)> {
        Self::debug_state_root_with_updates(self, hashed_state, block_id).await.map_err(Into::into)
    }

    async fn debug_stop_cpu_profile(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_stop_go_trace(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_storage_range_at(
        &self,
        _block_hash: B256,
        _tx_idx: usize,
        _contract_address: Address,
        _key_start: B256,
        _max_result: u64,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_trace_bad_block(
        &self,
        _block_hash: B256,
        _opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_verbosity(&self, _level: usize) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_vmodule(&self, _pattern: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_write_block_profile(&self, _file: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_write_mem_profile(&self, _file: String) -> RpcResult<()> {
        Ok(())
    }

    async fn debug_write_mutex_profile(&self, _file: String) -> RpcResult<()> {
        Ok(())
    }
}

impl<Eth: RpcNodeCore> std::fmt::Debug for DebugApi<Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebugApi").finish_non_exhaustive()
    }
}

impl<Eth: RpcNodeCore> Clone for DebugApi<Eth> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

struct DebugApiInner<Eth: RpcNodeCore> {
    /// The implementation of `eth` API
    eth_api: Eth,
    // restrict the number of concurrent calls to blocking calls
    blocking_task_guard: BlockingTaskGuard,
    /// Cache for bad blocks.
    bad_block_store: BadBlockStore<BlockTy<Eth::Primitives>>,
}

/// A bounded, deduplicating store of recently observed bad blocks.
#[derive(Clone, Debug)]
struct BadBlockStore<B: BlockTrait> {
    inner: Arc<RwLock<VecDeque<Arc<RecoveredBlock<B>>>>>,
    limit: usize,
}

impl<B: BlockTrait> BadBlockStore<B> {
    /// Creates a new store with the given capacity.
    fn new(limit: usize) -> Self {
        Self { inner: Arc::new(RwLock::new(VecDeque::with_capacity(limit))), limit }
    }

    /// Inserts a recovered block, keeping only the most recent `limit` entries and deduplicating
    /// by block hash.
    fn insert(&self, block: RecoveredBlock<B>) {
        let hash = block.hash();
        let mut guard = self.inner.write();

        // skip if we already recorded this bad block , and keep original ordering
        if guard.iter().any(|b| b.hash() == hash) {
            return;
        }
        guard.push_back(Arc::new(block));

        while guard.len() > self.limit {
            guard.pop_front();
        }
    }

    /// Returns all cached bad blocks ordered from newest to oldest.
    fn all(&self) -> Vec<Arc<RecoveredBlock<B>>> {
        let guard = self.inner.read();
        guard.iter().rev().cloned().collect()
    }
}

impl<B: BlockTrait> Default for BadBlockStore<B> {
    fn default() -> Self {
        Self::new(64)
    }
}

/// Default timeout of a JS tracer, matching geth.
#[cfg(feature = "js-tracer")]
const DEFAULT_JS_TRACER_TIMEOUT: Duration = Duration::from_secs(5);

/// The inspector the single-transaction trace methods run.
///
/// TN(TN-0017): upstream attaches the interrupt to the JS tracer itself, which needs
/// revm-inspectors 0.44.2; here it wraps the whole inspector (see `js_interrupt`).
#[cfg(feature = "js-tracer")]
type TraceInspector = Interruptible<DebugInspector>;
#[cfg(not(feature = "js-tracer"))]
type TraceInspector = DebugInspector;

/// Interrupts a JS tracer once its request is dropped or its timeout expires.
///
/// Built-in tracers can't be interrupted, so for them this does nothing.
#[derive(Clone, Debug, Default)]
struct JsTraceInterrupt {
    #[cfg(feature = "js-tracer")]
    js: Option<(JsInspectorInterrupt, Duration)>,
}

impl JsTraceInterrupt {
    /// Creates an interrupt if `opts` selects a JS tracer, using its `timeout` or geth's default.
    #[cfg_attr(
        not(feature = "js-tracer"),
        expect(unused_variables, reason = "only JS tracers can be interrupted")
    )]
    fn new(opts: &GethDebugTracingOptions) -> Result<Self, EthApiError> {
        #[cfg(feature = "js-tracer")]
        if let Some(GethDebugTracerType::JsTracer(_)) = &opts.tracer {
            let timeout = match &opts.timeout {
                Some(timeout) => humantime::parse_duration(timeout).map_err(|err| {
                    EthApiError::InvalidParams(format!("invalid tracer timeout {timeout:?}: {err}"))
                })?,
                None => DEFAULT_JS_TRACER_TIMEOUT,
            };
            return Ok(Self { js: Some((JsInspectorInterrupt::new(), timeout)) })
        }
        Ok(Self::default())
    }

    /// Lets this interrupt stop `inspector` if it's a JS tracer.
    #[cfg_attr(
        not(feature = "js-tracer"),
        expect(clippy::missing_const_for_fn, reason = "only JS tracers can be interrupted")
    )]
    fn attach(&self, inspector: DebugInspector) -> TraceInspector {
        #[cfg(feature = "js-tracer")]
        {
            let interrupt = match (&self.js, &inspector) {
                (Some((interrupt, _)), DebugInspector::Js(..)) => Some(interrupt.clone()),
                _ => None,
            };
            Interruptible::new(inspector, interrupt)
        }
        #[cfg(not(feature = "js-tracer"))]
        inspector
    }

    /// Awaits `trace`, interrupting the tracer if the returned future is dropped or the timeout
    /// expires.
    ///
    /// The interrupt is armed before the future is first polled, so dropping it unpolled still
    /// stops a trace that is already running on a blocking task.
    #[cfg_attr(
        not(feature = "js-tracer"),
        expect(
            clippy::manual_async_fn,
            clippy::redundant_async_block,
            reason = "only JS tracers can be interrupted"
        )
    )]
    fn run<T, E: FromEthApiError>(
        &self,
        trace: impl Future<Output = Result<T, E>>,
    ) -> impl Future<Output = Result<T, E>> {
        #[cfg(feature = "js-tracer")]
        let js = self.js.as_ref().map(|(interrupt, timeout)| (interrupt.drop_guard(), *timeout));
        async move {
            #[cfg(feature = "js-tracer")]
            if let Some((_interrupt_on_drop, timeout)) = js {
                return tokio::time::timeout(timeout, trace)
                    .await
                    .map_err(|_| E::from_eth_err(EthApiError::ExecutionTimedOut(timeout)))?
            }
            trace.await
        }
    }
}

#[cfg(all(test, feature = "js-tracer"))]
mod tests {
    use super::*;
    use alloy_rpc_types_trace::geth::GethDebugBuiltInTracerType;
    use revm::{
        context::TxEnv,
        context_interface::result::EVMError,
        database::{CacheDB, EmptyDB},
        handler::MainnetContext,
        primitives::TxKind,
        state::{AccountInfo, Bytecode},
        InspectEvm, Inspector, MainBuilder, MainContext,
    };

    fn js_tracer_opts(tracer: &str, timeout: Option<&str>) -> GethDebugTracingOptions {
        GethDebugTracingOptions {
            tracer: Some(GethDebugTracerType::JsTracer(tracer.into())),
            timeout: timeout.map(Into::into),
            ..Default::default()
        }
    }

    const NOOP_JS: &str = "{ fault: function() {}, result: function() { return null; } }";

    /// Runs `inspector` over a call to a contract that loops until it runs out of gas.
    fn trace_endless_loop<I>(inspector: I, gas_limit: u64) -> Result<(), String>
    where
        I: Inspector<MainnetContext<CacheDB<EmptyDB>>>,
    {
        let target = Address::with_last_byte(0xaa);
        let mut db = CacheDB::<EmptyDB>::default();
        // JUMPDEST; PUSH1 0; JUMP
        let code = Bytecode::new_raw(Bytes::from_static(&[0x5b, 0x60, 0x00, 0x56]));
        db.insert_account_info(target, AccountInfo::default().with_code(code));
        let mut evm = revm::Context::mainnet().with_db(db).build_mainnet_with_inspector(inspector);
        match evm.inspect_tx(TxEnv { gas_limit, kind: TxKind::Call(target), ..Default::default() })
        {
            Ok(_) => Ok(()),
            Err(EVMError::Custom(msg)) => Err(msg),
            Err(err) => panic!("unexpected EVM error: {err:?}"),
        }
    }

    #[test]
    fn js_trace_interrupt_stops_attached_js_tracer() {
        let opts = js_tracer_opts(NOOP_JS, None);
        let interrupt = JsTraceInterrupt::new(&opts).unwrap();
        let inspector = interrupt.attach(DebugInspector::new(opts).unwrap());
        interrupt.js.as_ref().unwrap().0.interrupt();
        assert_eq!(
            trace_endless_loop(inspector, 1_000_000),
            Err(js_interrupt::INTERRUPTED.to_string())
        );
    }

    /// The case that matters for the denial of service: a trace already running on another thread
    /// stops soon after the interrupt, not when the transaction runs out of gas.
    #[test]
    fn js_trace_interrupt_stops_a_running_trace() {
        let step_js = "{ n: 0, step: function() { this.n++; }, fault: function() {}, \
                       result: function() { return this.n; } }";
        let opts = js_tracer_opts(step_js, None);
        let interrupt = JsTraceInterrupt::new(&opts).unwrap();
        let tracer_interrupt = interrupt.clone();
        let start = std::time::Instant::now();
        // As in the RPC methods, the (non-`Send`) JS inspector is built on the worker thread.
        // Uninterrupted, this runs one JS step callback per opcode for 16M gas: minutes in a
        // debug build.
        let worker = std::thread::spawn(move || {
            let inspector = tracer_interrupt.attach(DebugInspector::new(opts).unwrap());
            trace_endless_loop(inspector, 16_000_000)
        });
        std::thread::sleep(Duration::from_millis(50));
        interrupt.js.as_ref().unwrap().0.interrupt();
        let result = worker.join().unwrap();
        assert_eq!(result, Err(js_interrupt::INTERRUPTED.to_string()));
        assert!(start.elapsed() < Duration::from_secs(2), "took {:?}", start.elapsed());
    }

    #[test]
    fn built_in_tracers_are_not_interrupted() {
        let opts = GethDebugTracingOptions {
            tracer: Some(GethDebugBuiltInTracerType::NoopTracer.into()),
            ..Default::default()
        };
        let interrupt = JsTraceInterrupt::new(&opts).unwrap();
        assert!(interrupt.js.is_none());
        let inspector = interrupt.attach(DebugInspector::new(opts).unwrap());
        assert_eq!(trace_endless_loop(inspector, 1_000_000), Ok(()));
    }

    #[test]
    fn invalid_js_tracer_timeout_is_rejected() {
        let err = JsTraceInterrupt::new(&js_tracer_opts(NOOP_JS, Some("soon"))).unwrap_err();
        assert!(matches!(err, EthApiError::InvalidParams(_)), "{err:?}");
    }

    #[tokio::test]
    async fn js_trace_interrupt_times_out_and_interrupts_tracer() {
        let interrupt = JsTraceInterrupt::new(&js_tracer_opts(NOOP_JS, Some("10ms"))).unwrap();
        let err =
            interrupt.run(std::future::pending::<Result<(), EthApiError>>()).await.unwrap_err();
        assert!(
            matches!(err, EthApiError::ExecutionTimedOut(timeout) if timeout == Duration::from_millis(10))
        );
        assert!(interrupt.js.unwrap().0.is_interrupted());
    }

    #[test]
    fn js_trace_interrupt_interrupts_tracer_when_dropped_unpolled() {
        let interrupt = JsTraceInterrupt::new(&js_tracer_opts(NOOP_JS, None)).unwrap();
        let (tracer_interrupt, _) = interrupt.js.clone().unwrap();
        let trace = interrupt.run(std::future::pending::<Result<(), EthApiError>>());
        assert!(!tracer_interrupt.is_interrupted());
        drop(trace);
        assert!(tracer_interrupt.is_interrupted());
    }
}

/// TN(TN-0036): a trace holds its tracing permit until its blocking task ends.
///
/// Outside the `js-tracer` test module so the default build runs it too.
#[cfg(test)]
mod trace_permit_tests {
    use super::*;
    use crate::{eth::EthApi, EthApiBuilder};
    use alloy_consensus::{Header, TxLegacy};
    use alloy_network::Ethereum;
    use alloy_primitives::TxKind;
    use alloy_rpc_types_eth::TransactionRequest;
    use futures::TryFutureExt;
    use reth_chainspec::ChainSpec;
    use reth_ethereum_primitives::{Block, BlockBody, Transaction};
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_primitives_traits::SealedBlock;
    use reth_provider::test_utils::MockEthProvider;
    use reth_rpc_convert::RpcConverter;
    use reth_rpc_eth_api::node::RpcNodeCoreAdapter;
    use reth_rpc_eth_types::receipt::EthReceiptConverter;
    use reth_tasks::TokioTaskExecutor;
    use reth_testing_utils::generators;
    use reth_transaction_pool::test_utils::{testing_pool, TestPool};
    use std::{future::Future, pin::Pin, sync::mpsc, time::Duration};
    use tokio::time::{timeout, Instant};

    type TestDebugApi = DebugApi<
        EthApi<
            RpcNodeCoreAdapter<MockEthProvider, TestPool, NoopNetwork, EthEvmConfig>,
            RpcConverter<Ethereum, EthEvmConfig, EthReceiptConverter<ChainSpec>>,
        >,
    >;

    type TraceFuture<'a> = Pin<Box<dyn Future<Output = RpcResult<()>> + Send + 'a>>;

    /// Time for spawned work to start: `EthApi`'s startup tasks, or a second trace that wrongly
    /// got a permit.
    const SETTLE: Duration = Duration::from_millis(200);

    /// Bounds every wait, so a regression fails instead of hanging.
    const DEADLINE: Duration = Duration::from_secs(20);

    /// The trace methods that take a tracing permit.
    #[derive(Clone, Copy, Debug)]
    enum TraceMethod {
        Block,
        BlockByHash,
        BlockByNumber,
        Transaction,
        Call,
        CallAtTxIndex,
        CallMany,
    }

    const ALL_METHODS: [TraceMethod; 7] = [
        TraceMethod::Block,
        TraceMethod::BlockByHash,
        TraceMethod::BlockByNumber,
        TraceMethod::Transaction,
        TraceMethod::Call,
        TraceMethod::CallAtTxIndex,
        TraceMethod::CallMany,
    ];

    /// A debug API limited to one tracing permit, over a mock chain whose block 1 holds one
    /// legacy transaction. Every method replays or simulates a transaction, so it reads an account
    /// inside its blocking task.
    struct Fixture {
        provider: MockEthProvider,
        api: TestDebugApi,
        block: SealedBlock<Block>,
    }

    impl Fixture {
        fn new() -> Self {
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
            let eth_api = EthApiBuilder::new(
                provider.clone(),
                testing_pool(),
                NoopNetwork::default(),
                EthEvmConfig::new(provider.chain_spec()),
            )
            .build();
            let api = DebugApi::new(
                eth_api,
                BlockingTaskGuard::new(1),
                TokioTaskExecutor::default(),
                futures::stream::empty(),
            );
            Self { provider, api, block }
        }

        /// Starts `method` with `tracing` as its tracer options.
        fn start(&self, method: TraceMethod, tracing: &GethDebugTracingOptions) -> TraceFuture<'_> {
            let api = &self.api;
            let tracing = tracing.clone();
            let call = || TransactionRequest {
                to: Some(TxKind::Call(Address::with_last_byte(2))),
                ..Default::default()
            };
            let call_opts = |tx_index| GethDebugTracingCallOptions {
                tracing_options: tracing.clone(),
                tx_index,
                ..Default::default()
            };
            let block_1 = || BlockId::Number(1.into());
            match method {
                TraceMethod::Block => Box::pin(
                    DebugApiServer::debug_trace_block(
                        api,
                        alloy_rlp::encode(self.block.clone().into_block()).into(),
                        Some(tracing),
                    )
                    .map_ok(drop),
                ),
                TraceMethod::BlockByHash => Box::pin(
                    DebugApiServer::debug_trace_block_by_hash(
                        api,
                        self.block.hash(),
                        Some(tracing),
                    )
                    .map_ok(drop),
                ),
                TraceMethod::BlockByNumber => Box::pin(
                    DebugApiServer::debug_trace_block_by_number(api, 1.into(), Some(tracing))
                        .map_ok(drop),
                ),
                TraceMethod::Transaction => Box::pin(
                    DebugApiServer::debug_trace_transaction(
                        api,
                        *self.block.body().transactions[0].tx_hash(),
                        Some(tracing),
                    )
                    .map_ok(drop),
                ),
                TraceMethod::Call => Box::pin(
                    DebugApiServer::debug_trace_call(api, call(), None, Some(call_opts(None)))
                        .map_ok(drop),
                ),
                TraceMethod::CallAtTxIndex => Box::pin(
                    DebugApiServer::debug_trace_call(
                        api,
                        call(),
                        Some(block_1()),
                        Some(call_opts(Some(0))),
                    )
                    .map_ok(drop),
                ),
                TraceMethod::CallMany => Box::pin(
                    DebugApiServer::debug_trace_call_many(
                        api,
                        vec![Bundle { transactions: vec![call()], block_override: None }],
                        Some(StateContext {
                            block_number: Some(block_1()),
                            transaction_index: None,
                        }),
                        Some(call_opts(None)),
                    )
                    .map_ok(drop),
                ),
            }
        }

        /// Clones of the mock provider alive: a trace's blocking task holds one for its state.
        fn provider_clones(&self) -> usize {
            Arc::strong_count(&self.provider.accounts)
        }

        /// Waits until the trace tasks and the block cache's loads have dropped their clones.
        async fn settle_at(&self, clones: usize) {
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

    /// Locks the mock's account store from a thread of its own until dropped, so the first
    /// account read of a trace blocks inside its blocking task.
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

    /// How the first trace's request ends while its blocking task is still running.
    #[derive(Clone, Copy, Debug)]
    enum Abandon {
        /// The client goes away: the request future is dropped.
        Drop,
        /// The JS tracer's `timeout` expires and the request returns an error.
        #[cfg(feature = "js-tracer")]
        Timeout,
    }

    /// With one tracing permit, a trace whose request ends while its blocking task still runs
    /// keeps the permit, so a second trace does not start a blocking task until the first ends.
    async fn assert_abandoned_trace_keeps_its_permit(
        method: TraceMethod,
        tracing: GethDebugTracingOptions,
        abandon: Abandon,
    ) {
        let fx = Fixture::new();
        // Let `EthApi`'s startup tasks run: the fee-history task holds a clone until the mock's
        // canonical-state stream ends.
        tokio::time::sleep(SETTLE).await;
        let idle = fx.provider_clones();
        // Warm the block cache, so the only clone a trace makes below is its own state's.
        timeout(DEADLINE, fx.start(method, &tracing))
            .await
            .expect("warm-up trace hangs")
            .unwrap_or_else(|err| panic!("{method:?}: warm-up trace failed: {err:?}"));
        fx.settle_at(idle).await;

        let gate = AccountGate::close(&fx.provider);
        let gated = idle + 1;
        let mut first = fx.start(method, &tracing);
        // Drive the first trace until its blocking task has opened state, then end its request.
        let deadline = Instant::now() + DEADLINE;
        while fx.provider_clones() == gated {
            assert!(Instant::now() < deadline, "{method:?}: the first trace never started");
            assert!(
                timeout(Duration::from_millis(5), &mut first).await.is_err(),
                "{method:?}: the first trace ended while the account store was locked"
            );
        }
        match abandon {
            Abandon::Drop => drop(first),
            #[cfg(feature = "js-tracer")]
            Abandon::Timeout => {
                let err = timeout(DEADLINE, first).await.expect("timeout never fired").unwrap_err();
                assert!(err.message().contains("timeout"), "{method:?}: {err:?}");
            }
        }
        assert_eq!(fx.provider_clones(), gated + 1, "{method:?}: the first trace's task ended");

        let mut second = fx.start(method, &tracing);
        assert!(
            timeout(SETTLE, &mut second).await.is_err(),
            "{method:?}: the second trace ran while the first one's task held the account store"
        );
        assert_eq!(
            fx.provider_clones(),
            gated + 1,
            "{method:?}: the second trace started a blocking task while the first one's still runs"
        );

        // The first trace's task ends and returns the permit; the second trace then runs.
        drop(gate);
        timeout(DEADLINE, second)
            .await
            .expect("the second trace never got the permit")
            .unwrap_or_else(|err| panic!("{method:?}: second trace failed: {err:?}"));
        fx.settle_at(idle).await;
    }

    #[tokio::test]
    async fn dropped_trace_keeps_its_permit_until_its_task_ends() {
        for method in ALL_METHODS {
            assert_abandoned_trace_keeps_its_permit(
                method,
                GethDebugTracingOptions::default(),
                Abandon::Drop,
            )
            .await;
        }
    }

    /// The JS-tracer case of T1-2: the request times out while its trace's task still runs.
    #[cfg(feature = "js-tracer")]
    #[tokio::test]
    async fn timed_out_js_trace_keeps_its_permit_until_its_task_ends() {
        use alloy_rpc_types_trace::geth::GethDebugTracerType;

        let tracing = GethDebugTracingOptions {
            tracer: Some(GethDebugTracerType::JsTracer(
                "{ step: function() {}, fault: function() {}, result: function() { return null; } }"
                    .into(),
            )),
            timeout: Some("1s".into()),
            ..Default::default()
        };
        // Only these methods apply the JS `timeout` (TN-0017).
        for method in [TraceMethod::Transaction, TraceMethod::Call, TraceMethod::CallAtTxIndex] {
            assert_abandoned_trace_keeps_its_permit(method, tracing.clone(), Abandon::Timeout)
                .await;
        }
    }
}
