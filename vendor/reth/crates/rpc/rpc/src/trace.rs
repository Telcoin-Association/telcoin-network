use alloy_consensus::BlockHeader as _;
use alloy_eips::BlockId;
use alloy_evm::block::calc::{base_block_reward_pre_merge, block_reward, ommer_reward};
use alloy_primitives::{
    map::{HashMap, HashSet},
    Address, BlockHash, Bytes, B256, U256,
};
use alloy_rpc_types_eth::{
    state::{EvmOverrides, StateOverride},
    BlockOverrides, Index,
};
use alloy_rpc_types_trace::{
    filter::TraceFilter,
    opcode::{BlockOpcodeGas, TransactionOpcodeGas},
    parity::*,
    tracerequest::TraceCallRequest,
};
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use reth_chainspec::{ChainSpecProvider, EthereumHardforks};
use reth_evm::ConfigureEvm;
use reth_primitives_traits::{BlockBody, BlockHeader};
use reth_rpc_api::TraceApiServer;
use reth_rpc_convert::RpcTxReq;
use reth_rpc_eth_api::{
    helpers::{Call, LoadPendingBlock, LoadTransaction, Trace, TraceExt},
    FromEthApiError, RpcNodeCore,
};
use reth_rpc_eth_types::{
    call_many, error::EthApiError, utils::recover_raw_transaction, CallManyBudget, EthConfig,
};
use reth_storage_api::{BlockNumReader, BlockReader};
use reth_tasks::{cancel::is_cancelled, pool::BlockingTaskGuard};
use reth_transaction_pool::{PoolPooledTx, PoolTransaction, TransactionPool};
use revm::{context_interface::result::ResultAndState, DatabaseCommit};
use revm_inspectors::{
    opcode::OpcodeGasInspector,
    storage::StorageInspector,
    tracing::{parity::populate_state_diff, TracingInspector, TracingInspectorConfig},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{AcquireError, OwnedSemaphorePermit};

/// `trace` API implementation.
///
/// This type provides the functionality for handling `trace` related requests.
pub struct TraceApi<Eth> {
    inner: Arc<TraceApiInner<Eth>>,
}

// === impl TraceApi ===

impl<Eth> TraceApi<Eth> {
    /// Create a new instance of the [`TraceApi`]
    pub fn new(
        eth_api: Eth,
        blocking_task_guard: BlockingTaskGuard,
        eth_config: EthConfig,
    ) -> Self {
        let inner = Arc::new(TraceApiInner { eth_api, blocking_task_guard, eth_config });
        Self { inner }
    }

    /// Acquires a permit to execute a tracing call.
    async fn acquire_trace_permit(
        &self,
    ) -> std::result::Result<OwnedSemaphorePermit, AcquireError> {
        self.inner.blocking_task_guard.clone().acquire_owned().await
    }

    /// Access the underlying `Eth` API.
    pub fn eth_api(&self) -> &Eth {
        &self.inner.eth_api
    }
}

impl<Eth: RpcNodeCore> TraceApi<Eth> {
    /// Access the underlying provider.
    pub fn provider(&self) -> &Eth::Provider {
        self.inner.eth_api.provider()
    }
}

// === impl TraceApi === //

impl<Eth> TraceApi<Eth>
where
    // tracing methods do _not_ read from mempool, hence no `LoadBlock` trait
    // bound
    Eth: Trace + Call + LoadPendingBlock + LoadTransaction + 'static,
{
    /// Acquires the tracing permit a trace moves into its blocking task.
    ///
    /// TN(TN-0038): upstream's RPC handlers hold the permit only while the request is open, so a
    /// trace that loses its client frees it while its thread runs on. Each trace method takes the
    /// permit here, once, and its blocking task drops it when the work ends.
    async fn trace_task_permit(&self) -> Result<OwnedSemaphorePermit, Eth::Error> {
        self.acquire_trace_permit().await.map_err(|_| EthApiError::InternalEthError.into())
    }

    /// Executes the given call and returns a number of possible traces for it.
    pub async fn trace_call(
        &self,
        trace_request: TraceCallRequest<RpcTxReq<Eth::NetworkTypes>>,
    ) -> Result<TraceResults, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let at = trace_request.block_id.unwrap_or_default();
        let config = TracingInspectorConfig::from_parity_config(&trace_request.trace_types);
        let overrides =
            EvmOverrides::new(trace_request.state_overrides, trace_request.block_overrides);
        let mut inspector = TracingInspector::new(config);
        let this = self.clone();
        self.eth_api()
            .spawn_with_call_at(trace_request.call, at, overrides, move |db, evm_env, tx_env| {
                let _permit = permit;
                let res = this.eth_api().inspect(&mut *db, evm_env, tx_env, &mut inspector)?;
                let trace_res = inspector
                    .into_parity_builder()
                    .into_trace_results_with_state(&res, &trace_request.trace_types, &db)
                    .map_err(Eth::Error::from_eth_err)?;
                Ok(trace_res)
            })
            .await
    }

    /// Traces a call to `eth_sendRawTransaction` without making the call, returning the traces.
    pub async fn trace_raw_transaction(
        &self,
        tx: Bytes,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
    ) -> Result<TraceResults, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let tx = recover_raw_transaction::<PoolPooledTx<Eth::Pool>>(&tx)?
            .map(<Eth::Pool as TransactionPool>::Transaction::pooled_into_consensus);

        let (evm_env, at) = self.eth_api().evm_env_at(block_id.unwrap_or_default()).await?;
        let tx_env = self.eth_api().evm_config().tx_env(tx);

        let config = TracingInspectorConfig::from_parity_config(&trace_types);

        self.eth_api()
            .spawn_trace_at_with_state(evm_env, tx_env, config, at, move |inspector, res, db| {
                let _permit = permit;
                inspector
                    .into_parity_builder()
                    .into_trace_results_with_state(&res, &trace_types, &db)
                    .map_err(Eth::Error::from_eth_err)
            })
            .await
    }

    /// Performs multiple call traces on top of the same block. Defaults to the latest block when
    /// no block is specified. Each call is executed with the preceding calls applied first.
    ///
    /// Note: Allows tracing dependent transactions, hence all transactions are traced in sequence
    pub async fn trace_call_many(
        &self,
        calls: Vec<(RpcTxReq<Eth::NetworkTypes>, HashSet<TraceType>)>,
        block_id: Option<BlockId>,
    ) -> Result<Vec<TraceResults>, Eth::Error> {
        // TN(TN-0057): the request shares one gas budget, as a block's transactions do.
        let gas_budget = self.eth_api().call_gas_limit();
        call_many::check_call_count(gas_budget, calls.len()).map_err(Eth::Error::from_eth_err)?;
        let permit = self.trace_task_permit().await?;
        let at = block_id.unwrap_or_default();
        let (evm_env, at) = self.eth_api().evm_env_at(at).await?;

        // execute all transactions on top of each other and record the traces
        self.eth_api()
            .spawn_with_state_at_block(at, move |eth_api, mut db| {
                let _permit = permit;
                let mut budget = CallManyBudget::new(gas_budget);
                let mut results = Vec::with_capacity(calls.len());
                let mut calls = calls.into_iter().peekable();

                while let Some((call, trace_types)) = calls.next() {
                    if is_cancelled() {
                        return Err(EthApiError::InternalEthError.into())
                    }
                    let (evm_env, tx_env) = eth_api.prepare_call_env(
                        evm_env.clone(),
                        call,
                        &mut db,
                        Default::default(),
                    )?;
                    let config = TracingInspectorConfig::from_parity_config(&trace_types);
                    let mut inspector = TracingInspector::new(config);
                    let res = eth_api.inspect(&mut db, evm_env, tx_env, &mut inspector)?;
                    budget.charge(res.result.gas_used()).map_err(Eth::Error::from_eth_err)?;

                    let trace_res = inspector
                        .into_parity_builder()
                        .into_trace_results_with_state(&res, &trace_types, &db)
                        .map_err(Eth::Error::from_eth_err)?;

                    results.push(trace_res);

                    // need to apply the state changes of this call before executing the
                    // next call
                    if calls.peek().is_some() {
                        db.commit(res.state)
                    }
                }

                Ok(results)
            })
            .await
    }

    /// Replays a transaction, returning the traces or `None` if the transaction does not exist.
    pub async fn replay_transaction(
        &self,
        hash: B256,
        trace_types: HashSet<TraceType>,
    ) -> Result<Option<TraceResultsWithTransactionHash>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let config = TracingInspectorConfig::from_parity_config(&trace_types);
        self.eth_api()
            .spawn_trace_transaction_in_block(hash, config, move |_, inspector, res, db| {
                let _permit = permit;
                let trace_res = inspector
                    .into_parity_builder()
                    .into_trace_results_with_state(&res, &trace_types, &db)
                    .map_err(Eth::Error::from_eth_err)?;
                Ok(TraceResultsWithTransactionHash {
                    transaction_hash: hash,
                    full_trace: trace_res,
                })
            })
            .await
    }

    /// Returns the transaction trace at the given `traceAddress` path.
    ///
    /// An empty path selects the root. Returns `None` if the transaction or path does not exist.
    pub async fn trace_get(
        &self,
        hash: B256,
        indices: Vec<usize>,
    ) -> Result<Option<LocalizedTransactionTrace>, Eth::Error> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        let permit = self.trace_task_permit().await?;
        self.eth_api()
            .spawn_trace_transaction_in_block(
                hash,
                TracingInspectorConfig::default_parity(),
                move |tx_info, inspector, _, _| {
                    let _permit = permit;
                    Ok(inspector
                        .into_parity_builder()
                        .into_localized_transaction_traces_iter(tx_info)
                        .find(|trace| trace.trace.trace_address == indices))
                },
            )
            .await
            .map(Option::flatten)
    }

    /// Returns the transaction trace at the given flat index, unlike [`Self::trace_get`], which
    /// selects a `traceAddress` path.
    ///
    /// Returns `None` if the trace object at that index does not exist
    pub async fn trace_get_index(
        &self,
        hash: B256,
        index: usize,
    ) -> Result<Option<LocalizedTransactionTrace>, Eth::Error> {
        Ok(self.trace_transaction(hash).await?.and_then(|traces| traces.into_iter().nth(index)))
    }

    /// Returns all traces for the given transaction hash
    pub async fn trace_transaction(
        &self,
        hash: B256,
    ) -> Result<Option<Vec<LocalizedTransactionTrace>>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        self.eth_api()
            .spawn_trace_transaction_in_block(
                hash,
                TracingInspectorConfig::default_parity(),
                move |tx_info, inspector, _, _| {
                    let _permit = permit;
                    let traces =
                        inspector.into_parity_builder().into_localized_transaction_traces(tx_info);
                    Ok(traces)
                },
            )
            .await
    }

    /// Returns all opcodes with their count and combined gas usage for the given transaction in no
    /// particular order.
    pub async fn trace_transaction_opcode_gas(
        &self,
        tx_hash: B256,
    ) -> Result<Option<TransactionOpcodeGas>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        self.eth_api()
            .spawn_trace_transaction_in_block_with_inspector(
                tx_hash,
                OpcodeGasInspector::default(),
                move |_tx_info, inspector, _res, _| {
                    let _permit = permit;
                    let trace = TransactionOpcodeGas {
                        transaction_hash: tx_hash,
                        opcode_gas: inspector.opcode_gas_iter().collect(),
                    };
                    Ok(trace)
                },
            )
            .await
    }

    /// Calculates the base block reward for the given block:
    ///
    /// - if Paris hardfork is activated, no block rewards are given
    /// - if Paris hardfork is not activated, calculate block rewards with block number only
    fn calculate_base_block_reward<H: BlockHeader>(
        &self,
        header: &H,
    ) -> Result<Option<u128>, Eth::Error> {
        let chain_spec = self.provider().chain_spec();

        if chain_spec.is_paris_active_at_block(header.number()) {
            return Ok(None)
        }

        Ok(Some(base_block_reward_pre_merge(&chain_spec, header.number())))
    }

    /// Extracts the reward traces for the given block:
    ///  - block reward
    ///  - uncle rewards
    fn extract_reward_traces<H: BlockHeader>(
        &self,
        header: &H,
        ommers: Option<&[H]>,
        base_block_reward: u128,
    ) -> Vec<LocalizedTransactionTrace> {
        let ommers_cnt = ommers.map(|o| o.len()).unwrap_or_default();
        let mut traces = Vec::with_capacity(ommers_cnt + 1);

        let block_reward = block_reward(base_block_reward, ommers_cnt);
        traces.push(reward_trace(
            header,
            RewardAction {
                author: header.beneficiary(),
                reward_type: RewardType::Block,
                value: U256::from(block_reward),
            },
        ));

        let Some(ommers) = ommers else { return traces };

        for uncle in ommers {
            let uncle_reward = ommer_reward(base_block_reward, header.number(), uncle.number());
            traces.push(reward_trace(
                header,
                RewardAction {
                    author: uncle.beneficiary(),
                    reward_type: RewardType::Uncle,
                    value: U256::from(uncle_reward),
                },
            ));
        }
        traces
    }
}

impl<Eth> TraceApi<Eth>
where
    // tracing methods read from mempool, hence `LoadBlock` trait bound via
    // `TraceExt`
    Eth: TraceExt + 'static,
{
    /// Returns all transaction traces that match the given filter.
    ///
    /// This is similar to [`Self::trace_block`] but only returns traces for transactions that match
    /// the filter. Omitted range bounds default to the latest block.
    pub async fn trace_filter(
        &self,
        filter: TraceFilter,
    ) -> Result<Vec<LocalizedTransactionTrace>, Eth::Error> {
        // We'll reuse the matcher across multiple blocks that are traced in parallel
        let matcher = Arc::new(filter.matcher());
        let TraceFilter { from_block, to_block, mut after, count, .. } = filter;

        let latest_block = self.provider().best_block_number().map_err(Eth::Error::from_eth_err)?;
        let start = from_block.unwrap_or(latest_block);
        if start > latest_block {
            // can't trace that range
            return Err(EthApiError::HeaderNotFound(start.into()).into());
        }
        let end = to_block.unwrap_or(latest_block);
        if end > latest_block {
            return Err(EthApiError::HeaderNotFound(end.into()).into());
        }

        if start > end {
            return Err(EthApiError::InvalidParams(
                "invalid parameters: fromBlock cannot be greater than toBlock".to_string(),
            )
            .into())
        }

        // ensure that the range is not too large, since we need to fetch all blocks in the range
        let distance = end.saturating_sub(start);
        if distance > self.inner.eth_config.max_trace_filter_blocks {
            return Err(EthApiError::InvalidParams(format!(
                "Block range too large; currently limited to {} blocks",
                self.inner.eth_config.max_trace_filter_blocks
            ))
            .into())
        }

        // TN(TN-0038): the filter's two permits are shared by its block fetches and block traces,
        // and return when the last of those blocking tasks ends, not when the request does.
        let permits = Arc::new(
            self.inner
                .blocking_task_guard
                .clone()
                .acquire_many_owned(2)
                .await
                .map_err(|_| EthApiError::InternalEthError)?,
        );
        let mut all_traces = Vec::new();
        let mut block_traces = Vec::with_capacity(self.inner.eth_config.max_tracing_requests);
        for chunk_start in (start..=end).step_by(self.inner.eth_config.max_tracing_requests) {
            let chunk_end = std::cmp::min(
                chunk_start + self.inner.eth_config.max_tracing_requests as u64 - 1,
                end,
            );

            // fetch all blocks in that chunk
            let fetch_permits = Arc::clone(&permits);
            let blocks = self
                .eth_api()
                .spawn_blocking_io(move |this| {
                    let _permits = fetch_permits;
                    Ok(this
                        .provider()
                        .recovered_block_range(chunk_start..=chunk_end)
                        .map_err(Eth::Error::from_eth_err)?
                        .into_iter()
                        .map(Arc::new)
                        .collect::<Vec<_>>())
                })
                .await?;

            // trace all blocks
            for block in &blocks {
                let matcher = matcher.clone();
                let block_permits = Arc::clone(&permits);
                let traces = self.eth_api().trace_block_until(
                    block.hash().into(),
                    Some(block.clone()),
                    None,
                    TracingInspectorConfig::default_parity(),
                    move |tx_info, mut ctx| {
                        let _permits = &block_permits;
                        let mut traces = ctx
                            .take_inspector()
                            .into_parity_builder()
                            .into_localized_transaction_traces(tx_info);
                        traces.retain(|trace| matcher.matches(&trace.trace));
                        Ok(Some(traces))
                    },
                );
                block_traces.push(traces);
            }

            #[allow(clippy::iter_with_drain)]
            let block_traces = futures::future::try_join_all(block_traces.drain(..)).await?;
            all_traces.extend(block_traces.into_iter().flatten().flat_map(|traces| {
                traces.into_iter().flatten().flat_map(|traces| traces.into_iter())
            }));

            // add reward traces for all blocks
            for block in &blocks {
                if let Some(base_block_reward) = self.calculate_base_block_reward(block.header())? {
                    all_traces.extend(
                        self.extract_reward_traces(
                            block.header(),
                            block.body().ommers(),
                            base_block_reward,
                        )
                        .into_iter()
                        .filter(|trace| matcher.matches(&trace.trace)),
                    );
                } else {
                    // no block reward, means we're past the Paris hardfork and don't expect any
                    // rewards because the blocks in ascending order
                    break
                }
            }

            // Skips the first `after` number of matching traces.
            if let Some(cutoff) = after.map(|a| a as usize) &&
                cutoff < all_traces.len()
            {
                all_traces.drain(..cutoff);
                // we removed the first `after` traces
                after = None;
            }

            // Return at most `count` of traces
            if let Some(count) = count {
                let count = count as usize;
                if count < all_traces.len() {
                    all_traces.truncate(count);
                    return Ok(all_traces)
                }
            };
        }

        // If `after` is greater than or equal to the number of matched traces, it returns an
        // empty array.
        if let Some(cutoff) = after.map(|a| a as usize) &&
            cutoff >= all_traces.len()
        {
            return Ok(vec![])
        }

        Ok(all_traces)
    }

    /// Returns traces created at given block.
    pub async fn trace_block(
        &self,
        block_id: BlockId,
    ) -> Result<Option<Vec<LocalizedTransactionTrace>>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let traces = self.eth_api().trace_block_with(
            block_id,
            None,
            TracingInspectorConfig::default_parity(),
            move |tx_info, mut ctx| {
                let _permit = &permit;
                let traces = ctx
                    .take_inspector()
                    .into_parity_builder()
                    .into_localized_transaction_traces(tx_info);
                Ok(traces)
            },
        );

        let block = self.eth_api().recovered_block(block_id);
        let (maybe_traces, maybe_block) = futures::try_join!(traces, block)?;

        let mut maybe_traces =
            maybe_traces.map(|traces| traces.into_iter().flatten().collect::<Vec<_>>());

        if let (Some(block), Some(traces)) = (maybe_block, maybe_traces.as_mut()) &&
            let Some(base_block_reward) = self.calculate_base_block_reward(block.header())?
        {
            traces.extend(self.extract_reward_traces(
                block.header(),
                block.body().ommers(),
                base_block_reward,
            ));
        }

        Ok(maybe_traces)
    }

    /// Replays all transactions in a block
    pub async fn replay_block_transactions(
        &self,
        block_id: BlockId,
        trace_types: HashSet<TraceType>,
    ) -> Result<Option<Vec<TraceResultsWithTransactionHash>>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        self.eth_api()
            .trace_block_with(
                block_id,
                None,
                TracingInspectorConfig::from_parity_config(&trace_types),
                move |tx_info, mut ctx| {
                    let _permit = &permit;
                    let builder = ctx.take_inspector().into_parity_builder();
                    let full_trace = if trace_types.contains(&TraceType::VmTrace) {
                        // TN(TN-0071): upstream fills `vmTrace.code` through revm-inspectors'
                        // `into_trace_results_with_state_parts`, which the vendored 0.34 series
                        // lacks; its state-aware builder wants an owned `ResultAndState`, so the
                        // vmTrace path clones the transaction's state changes.
                        let res = ResultAndState::new(ctx.result, ctx.state.clone());
                        builder
                            .into_trace_results_with_state(&res, &trace_types, &ctx.db)
                            .map_err(Eth::Error::from_eth_err)?
                    } else {
                        let mut full_trace = builder.into_trace_results(&ctx.result, &trace_types);

                        // If statediffs were requested, populate them with the account balance and
                        // nonce from pre-state
                        if let Some(ref mut state_diff) = full_trace.state_diff {
                            populate_state_diff(state_diff, &ctx.db, ctx.state.iter())
                                .map_err(Eth::Error::from_eth_err)?;
                        }
                        full_trace
                    };

                    let trace = TraceResultsWithTransactionHash {
                        transaction_hash: tx_info.hash.expect("tx hash is set"),
                        full_trace,
                    };
                    Ok(trace)
                },
            )
            .await
    }

    /// Returns the opcodes of all transactions in the given block.
    ///
    /// This is the same as [`Self::trace_transaction_opcode_gas`] but for all transactions in a
    /// block.
    pub async fn trace_block_opcode_gas(
        &self,
        block_id: BlockId,
    ) -> Result<Option<BlockOpcodeGas>, Eth::Error> {
        let permit = self.trace_task_permit().await?;
        let res = self
            .eth_api()
            .trace_block_inspector(
                block_id,
                None,
                OpcodeGasInspector::default,
                move |tx_info, ctx| {
                    let _permit = &permit;
                    let trace = TransactionOpcodeGas {
                        transaction_hash: tx_info.hash.expect("tx hash is set"),
                        opcode_gas: ctx.inspector.opcode_gas_iter().collect(),
                    };
                    Ok(trace)
                },
            )
            .await?;

        let Some(transactions) = res else { return Ok(None) };

        let Some(block) = self.eth_api().recovered_block(block_id).await? else { return Ok(None) };

        Ok(Some(BlockOpcodeGas {
            block_hash: block.hash(),
            block_number: block.number(),
            transactions,
        }))
    }

    /// Returns all storage slots accessed during transaction execution along with their access
    /// counts.
    pub async fn trace_block_storage_access(
        &self,
        block_id: BlockId,
    ) -> Result<Option<BlockStorageAccess>, Eth::Error> {
        let res = self
            .eth_api()
            .trace_block_inspector(
                block_id,
                None,
                StorageInspector::default,
                move |tx_info, ctx| {
                    let trace = TransactionStorageAccess {
                        transaction_hash: tx_info.hash.expect("tx hash is set"),
                        storage_access: ctx.inspector.accessed_slots().clone(),
                        unique_loads: ctx.inspector.unique_loads(),
                        warm_loads: ctx.inspector.warm_loads(),
                    };
                    Ok(trace)
                },
            )
            .await?;

        let Some(transactions) = res else { return Ok(None) };

        let Some(block) = self.eth_api().recovered_block(block_id).await? else { return Ok(None) };

        Ok(Some(BlockStorageAccess {
            block_hash: block.hash(),
            block_number: block.number(),
            transactions,
        }))
    }
}

#[async_trait]
impl<Eth> TraceApiServer<RpcTxReq<Eth::NetworkTypes>> for TraceApi<Eth>
where
    Eth: TraceExt + 'static,
{
    /// Executes the given call and returns a number of possible traces for it.
    ///
    /// Handler for `trace_call`
    async fn trace_call(
        &self,
        call: RpcTxReq<Eth::NetworkTypes>,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<TraceResults> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        let request =
            TraceCallRequest { call, trace_types, block_id, state_overrides, block_overrides };
        Ok(Self::trace_call(self, request).await.map_err(Into::into)?)
    }

    /// Handler for `trace_callMany`
    async fn trace_call_many(
        &self,
        calls: Vec<(RpcTxReq<Eth::NetworkTypes>, HashSet<TraceType>)>,
        block_id: Option<BlockId>,
    ) -> RpcResult<Vec<TraceResults>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_call_many(self, calls, block_id).await.map_err(Into::into)?)
    }

    /// Handler for `trace_rawTransaction`
    async fn trace_raw_transaction(
        &self,
        data: Bytes,
        trace_types: HashSet<TraceType>,
        block_id: Option<BlockId>,
    ) -> RpcResult<TraceResults> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_raw_transaction(self, data, trace_types, block_id)
            .await
            .map_err(Into::into)?)
    }

    /// Handler for `trace_replayBlockTransactions`
    async fn replay_block_transactions(
        &self,
        block_id: BlockId,
        trace_types: HashSet<TraceType>,
    ) -> RpcResult<Option<Vec<TraceResultsWithTransactionHash>>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::replay_block_transactions(self, block_id, trace_types)
            .await
            .map_err(Into::into)?)
    }

    /// Handler for `trace_replayTransaction`
    async fn replay_transaction(
        &self,
        transaction: B256,
        trace_types: HashSet<TraceType>,
    ) -> RpcResult<Option<TraceResultsWithTransactionHash>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::replay_transaction(self, transaction, trace_types).await.map_err(Into::into)?)
    }

    /// Handler for `trace_block`
    async fn trace_block(
        &self,
        block_id: BlockId,
    ) -> RpcResult<Option<Vec<LocalizedTransactionTrace>>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_block(self, block_id).await.map_err(Into::into)?)
    }

    /// Handler for `trace_filter`
    ///
    /// This is similar to `eth_getLogs` but for traces.
    ///
    /// # Limitations
    /// This currently requires block filter fields, since reth does not have address indices yet.
    async fn trace_filter(&self, filter: TraceFilter) -> RpcResult<Vec<LocalizedTransactionTrace>> {
        // TN(TN-0038): the filter takes its permits itself and holds them in its tasks.
        Ok(Self::trace_filter(self, filter).await.map_err(Into::into)?)
    }

    /// Returns transaction trace at given index.
    /// Handler for `trace_get`
    async fn trace_get(
        &self,
        hash: B256,
        indices: Vec<Index>,
    ) -> RpcResult<Option<LocalizedTransactionTrace>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_get(self, hash, indices.into_iter().map(Into::into).collect())
            .await
            .map_err(Into::into)?)
    }

    /// Handler for `trace_transaction`
    async fn trace_transaction(
        &self,
        hash: B256,
    ) -> RpcResult<Option<Vec<LocalizedTransactionTrace>>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_transaction(self, hash).await.map_err(Into::into)?)
    }

    /// Handler for `trace_transactionOpcodeGas`
    async fn trace_transaction_opcode_gas(
        &self,
        tx_hash: B256,
    ) -> RpcResult<Option<TransactionOpcodeGas>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_transaction_opcode_gas(self, tx_hash).await.map_err(Into::into)?)
    }

    /// Handler for `trace_blockOpcodeGas`
    async fn trace_block_opcode_gas(&self, block_id: BlockId) -> RpcResult<Option<BlockOpcodeGas>> {
        // TN(TN-0038): the trace takes its permit itself and holds it in its blocking task.
        Ok(Self::trace_block_opcode_gas(self, block_id).await.map_err(Into::into)?)
    }
}

impl<Eth> std::fmt::Debug for TraceApi<Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceApi").finish_non_exhaustive()
    }
}
impl<Eth> Clone for TraceApi<Eth> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

struct TraceApiInner<Eth> {
    /// Access to commonly used code of the `eth` namespace
    eth_api: Eth,
    // restrict the number of concurrent calls to `trace_*`
    blocking_task_guard: BlockingTaskGuard,
    // eth config settings
    eth_config: EthConfig,
}

/// Response type for storage tracing that contains all accessed storage slots
/// for a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionStorageAccess {
    /// Hash of the transaction
    pub transaction_hash: B256,
    /// Tracks storage slots and access counter.
    pub storage_access: HashMap<Address, HashMap<B256, u64>>,
    /// Number of unique storage loads
    pub unique_loads: u64,
    /// Number of warm storage loads
    pub warm_loads: u64,
}

/// Response type for storage tracing that contains all accessed storage slots
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockStorageAccess {
    /// The block hash
    pub block_hash: BlockHash,
    /// The block's number
    pub block_number: u64,
    /// All executed transactions in the block in the order they were executed
    pub transactions: Vec<TransactionStorageAccess>,
}

/// Helper to construct a [`LocalizedTransactionTrace`] that describes a reward to the block
/// beneficiary.
fn reward_trace<H: BlockHeader>(header: &H, reward: RewardAction) -> LocalizedTransactionTrace {
    LocalizedTransactionTrace {
        block_hash: Some(header.hash_slow()),
        block_number: Some(header.number()),
        transaction_hash: None,
        transaction_position: None,
        trace: TransactionTrace {
            trace_address: vec![],
            subtraces: 0,
            action: Action::Reward(reward),
            error: None,
            result: None,
        },
    }
}

/// TN(TN-0038): a `trace_*` request holds its tracing permits until its blocking tasks end.
#[cfg(test)]
mod trace_permit_tests {
    use super::*;
    use crate::permit_test_utils::{
        assert_abandoned_request_keeps_its_permit, Abandon, MockChain, RequestFuture, TestEthApi,
    };
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::TxKind;
    use alloy_rpc_types_eth::TransactionRequest;
    use futures::TryFutureExt;

    /// The handlers that take tracing permits.
    #[derive(Clone, Copy, Debug)]
    enum TraceMethod {
        Call,
        CallMany,
        RawTransaction,
        ReplayBlockTransactions,
        ReplayTransaction,
        Block,
        Filter,
        Get,
        Transaction,
        TransactionOpcodeGas,
        BlockOpcodeGas,
    }

    const ALL_METHODS: [TraceMethod; 11] = [
        TraceMethod::Call,
        TraceMethod::CallMany,
        TraceMethod::RawTransaction,
        TraceMethod::ReplayBlockTransactions,
        TraceMethod::ReplayTransaction,
        TraceMethod::Block,
        TraceMethod::Filter,
        TraceMethod::Get,
        TraceMethod::Transaction,
        TraceMethod::TransactionOpcodeGas,
        TraceMethod::BlockOpcodeGas,
    ];

    impl TraceMethod {
        /// The permits the method takes: `trace_filter` takes two, as upstream's handler did.
        const fn permits(self) -> usize {
            match self {
                Self::Filter => 2,
                _ => 1,
            }
        }
    }

    /// A trace API with exactly the permits `method` takes, over [`MockChain`].
    struct Fixture {
        chain: MockChain,
        api: TraceApi<TestEthApi>,
    }

    impl Fixture {
        fn new(method: TraceMethod) -> Self {
            let chain = MockChain::new();
            let api = TraceApi::new(
                chain.eth_api(),
                BlockingTaskGuard::new(method.permits()),
                EthConfig::default(),
            );
            Self { chain, api }
        }

        fn start(&self, method: TraceMethod) -> RequestFuture<'_> {
            let api = &self.api;
            let tx = &self.chain.block.body().transactions[0];
            let call = || TransactionRequest {
                to: Some(TxKind::Call(Address::with_last_byte(2))),
                ..Default::default()
            };
            let types = || HashSet::from_iter([TraceType::Trace]);
            let block_1 = || BlockId::Number(1.into());
            match method {
                TraceMethod::Call => Box::pin(
                    TraceApiServer::trace_call(api, call(), types(), None, None, None).map_ok(drop),
                ),
                TraceMethod::CallMany => Box::pin(
                    TraceApiServer::trace_call_many(api, vec![(call(), types())], Some(block_1()))
                        .map_ok(drop),
                ),
                TraceMethod::RawTransaction => Box::pin(
                    TraceApiServer::trace_raw_transaction(
                        api,
                        tx.encoded_2718().into(),
                        types(),
                        Some(block_1()),
                    )
                    .map_ok(drop),
                ),
                TraceMethod::ReplayBlockTransactions => Box::pin(
                    TraceApiServer::replay_block_transactions(api, block_1(), types()).map_ok(drop),
                ),
                TraceMethod::ReplayTransaction => Box::pin(
                    TraceApiServer::replay_transaction(api, *tx.tx_hash(), types()).map_ok(drop),
                ),
                TraceMethod::Block => {
                    Box::pin(TraceApiServer::trace_block(api, block_1()).map_ok(drop))
                }
                TraceMethod::Filter => Box::pin(
                    TraceApiServer::trace_filter(
                        api,
                        TraceFilter {
                            from_block: Some(1),
                            to_block: Some(1),
                            ..Default::default()
                        },
                    )
                    .map_ok(drop),
                ),
                TraceMethod::Get => Box::pin(
                    TraceApiServer::trace_get(api, *tx.tx_hash(), vec![Index::from(0)])
                        .map_ok(drop),
                ),
                TraceMethod::Transaction => {
                    Box::pin(TraceApiServer::trace_transaction(api, *tx.tx_hash()).map_ok(drop))
                }
                TraceMethod::TransactionOpcodeGas => Box::pin(
                    TraceApiServer::trace_transaction_opcode_gas(api, *tx.tx_hash()).map_ok(drop),
                ),
                TraceMethod::BlockOpcodeGas => {
                    Box::pin(TraceApiServer::trace_block_opcode_gas(api, block_1()).map_ok(drop))
                }
            }
        }
    }

    /// With every tracing permit taken by one request, a `trace_*` request that loses its client
    /// while its blocking task still runs keeps its permits, so a second request does not start a
    /// blocking task until the first one's ends.
    #[tokio::test]
    async fn dropped_trace_keeps_its_permits_until_its_tasks_end() {
        for method in ALL_METHODS {
            let fx = Fixture::new(method);
            let name = format!("{method:?}");
            assert_abandoned_request_keeps_its_permit(
                &fx.chain,
                &name,
                || fx.start(method),
                Abandon::Drop,
            )
            .await;
        }
    }
}

/// TN(TN-0066): upstream's `trace` tests (the vendored file had none; reth adds them from
/// 8760eb000 on).
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trace_call_many_defaults_to_latest() {
        use crate::EthApiBuilder;
        use alloy_consensus::Header;
        use alloy_rpc_types_eth::TransactionRequest;
        use reth_ethereum_primitives::{Block, BlockBody};
        use reth_evm_ethereum::EthEvmConfig;
        use reth_network_api::noop::NoopNetwork;
        use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
        use reth_transaction_pool::test_utils::testing_pool;

        let provider = MockEthProvider::default();
        let target = Address::with_last_byte(0x42);
        // Return NUMBER as a 32-byte word.
        provider.add_account(
            target,
            ExtendedAccount::new(0, U256::ZERO)
                .with_bytecode("4360005260206000f3".parse().unwrap()),
        );
        let header = Header { number: 1, gas_limit: 30_000_000, ..Default::default() };
        provider.add_block(header.hash_slow(), Block { header, body: BlockBody::default() });
        let eth_api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        let api = TraceApi::new(eth_api, BlockingTaskGuard::new(1), EthConfig::default());
        let calls = vec![(TransactionRequest::default().to(target), HashSet::default())];

        let omitted = api.trace_call_many(calls.clone(), None).await.unwrap();
        let latest = api.trace_call_many(calls.clone(), Some(BlockId::latest())).await.unwrap();
        let request = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "trace_callMany", "params": [calls.clone()],
        });
        let pending = api.trace_call_many(calls, Some(BlockId::pending())).await.unwrap();
        assert_eq!(omitted, latest);
        assert_ne!(omitted, pending);
        assert_eq!(U256::from_be_slice(&omitted[0].output), U256::from(1));
        assert_eq!(U256::from_be_slice(&pending[0].output), U256::from(2));

        let module = api.into_rpc();
        let (response, _) = module.raw_json_request(&request.to_string(), 1).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(response["result"], serde_json::to_value(latest).unwrap());
    }

    #[tokio::test]
    async fn replay_missing_transaction_returns_null() {
        use crate::EthApiBuilder;
        use reth_evm_ethereum::EthEvmConfig;
        use reth_network_api::noop::NoopNetwork;
        use reth_provider::test_utils::MockEthProvider;
        use reth_transaction_pool::test_utils::testing_pool;

        let provider = MockEthProvider::default();
        let eth_api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        let api = TraceApi::new(eth_api, BlockingTaskGuard::new(1), EthConfig::default());
        let module = api.into_rpc();
        for types in [serde_json::json!([]), serde_json::json!(["trace", "stateDiff", "vmTrace"])] {
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "trace_replayTransaction",
                "params": [B256::with_last_byte(1), types],
            });
            let (response, _) = module.raw_json_request(&request.to_string(), 1).await.unwrap();
            let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
            assert_eq!(response, serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": null}));
        }
    }

    #[tokio::test]
    async fn replay_transaction_includes_transaction_hash() {
        use crate::EthApiBuilder;
        use alloy_consensus::{Header, TxLegacy};
        use alloy_primitives::{Signature, TxKind};
        use reth_ethereum_primitives::{Block, BlockBody, TransactionSigned};
        use reth_evm_ethereum::EthEvmConfig;
        use reth_network_api::noop::NoopNetwork;
        use reth_primitives_traits::SignerRecoverable;
        use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
        use reth_transaction_pool::test_utils::testing_pool;

        let provider = MockEthProvider::default();
        let tx = TransactionSigned::new_unhashed(
            TxLegacy {
                gas_limit: 21_000,
                to: TxKind::Call(Address::with_last_byte(0x42)),
                value: U256::from(1),
                ..Default::default()
            }
            .into(),
            Signature::test_signature(),
        );
        let hash = *tx.hash();
        let sender = tx.recover_signer().unwrap();
        provider.add_account(sender, ExtendedAccount::new(0, U256::from(1_000_000)));
        let parent = Header { gas_limit: 30_000_000, ..Default::default() };
        let parent_hash = parent.hash_slow();
        provider.add_header(parent_hash, parent);
        let block = Block {
            header: Header { parent_hash, number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody { transactions: vec![tx], ..Default::default() },
        };
        let block_hash = block.header.hash_slow();
        provider.add_block(block_hash, block);
        let eth_api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        // TN: the vendored MockEthProvider resolves recovered blocks (TN-0036), so upstream's
        // RPC-cache seeding is not needed.
        let api = TraceApi::new(eth_api, BlockingTaskGuard::new(1), EthConfig::default());
        let module = api.clone().into_rpc();
        for types in
            [HashSet::default(), HashSet::from_iter([TraceType::Trace, TraceType::StateDiff])]
        {
            let request = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "trace_replayTransaction",
                "params": [hash, types],
            });
            let (response, _) = module.raw_json_request(&request.to_string(), 1).await.unwrap();
            let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
            assert!(response.get("error").is_none(), "{response}");
            assert_eq!(response["result"]["transactionHash"], serde_json::json!(hash));

            let block_replay =
                api.replay_block_transactions(block_hash.into(), types).await.unwrap().unwrap();
            assert_eq!(response["result"], serde_json::to_value(&block_replay[0]).unwrap());
        }
    }

    #[tokio::test]
    async fn trace_filter_defaults_to_latest() {
        use crate::EthApiBuilder;
        use alloy_consensus::Header;
        use reth_ethereum_primitives::{Block, BlockBody};
        use reth_evm_ethereum::EthEvmConfig;
        use reth_network_api::noop::NoopNetwork;
        use reth_provider::test_utils::MockEthProvider;
        use reth_transaction_pool::test_utils::testing_pool;

        let provider = MockEthProvider::default();
        for number in [1, 2] {
            let header = Header { number, gas_limit: 30_000_000, ..Default::default() };
            provider.add_block(header.hash_slow(), Block { header, body: BlockBody::default() });
        }
        let eth_api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        // TN: `trace_filter` takes its two permits itself (TN-0038), so the guard needs two where
        // upstream's test gets by with one.
        let api = TraceApi::new(
            eth_api,
            BlockingTaskGuard::new(2),
            EthConfig::default().max_trace_filter_blocks(1),
        );

        let omitted = api.trace_filter(TraceFilter::default()).await.unwrap();
        let latest =
            api.trace_filter(TraceFilter::default().from_block(2).to_block(2)).await.unwrap();
        assert_eq!(omitted, latest);
        assert!(api.trace_filter(TraceFilter::default().from_block(1).to_block(2)).await.is_ok());

        let module = api.into_rpc();
        let request = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "trace_filter", "params": [{"toBlock": "0x1"}],
        });
        let (response, _) = module.raw_json_request(&request.to_string(), 1).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(
            response["error"],
            serde_json::json!({
                "code": -32602,
                "message": "invalid parameters: fromBlock cannot be greater than toBlock",
            })
        );
    }

    #[tokio::test]
    async fn trace_get_selects_tree_paths() {
        use crate::EthApiBuilder;
        use alloy_consensus::{Header, TxLegacy};
        use alloy_primitives::{Signature, TxKind};
        use reth_ethereum_primitives::{Block, BlockBody, TransactionSigned};
        use reth_evm_ethereum::EthEvmConfig;
        use reth_network_api::noop::NoopNetwork;
        use reth_primitives_traits::SignerRecoverable;
        use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
        use reth_transaction_pool::test_utils::testing_pool;

        let provider = MockEthProvider::default();
        // The root calls 0x43 (which calls 0x44), then calls 0x45.
        for (address, code) in [
            (0x42, "60006000600060006000604361fffff15060006000600060006000604561fffff15000"),
            (0x43, "600060006000600060006044611000f15000"),
        ] {
            provider.add_account(
                Address::with_last_byte(address),
                ExtendedAccount::new(0, U256::ZERO).with_bytecode(code.parse().unwrap()),
            );
        }
        let tx = TransactionSigned::new_unhashed(
            TxLegacy {
                gas_limit: 500_000,
                to: TxKind::Call(Address::with_last_byte(0x42)),
                value: U256::from(1),
                ..Default::default()
            }
            .into(),
            Signature::test_signature(),
        );
        let hash = *tx.hash();
        let sender = tx.recover_signer().unwrap();
        provider.add_account(sender, ExtendedAccount::new(0, U256::from(1_000_000)));
        let parent = Header { gas_limit: 30_000_000, ..Default::default() };
        let parent_hash = parent.hash_slow();
        provider.add_header(parent_hash, parent);
        let block = Block {
            header: Header { parent_hash, number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody { transactions: vec![tx], ..Default::default() },
        };
        let block_hash = block.header.hash_slow();
        provider.add_block(block_hash, block);
        let eth_api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        // TN: the vendored MockEthProvider resolves recovered blocks (TN-0036), so upstream's
        // RPC-cache seeding is not needed.
        let api = TraceApi::new(eth_api, BlockingTaskGuard::new(1), EthConfig::default());
        let module = api.clone().into_rpc();
        let traces = api.trace_transaction(hash).await.unwrap().unwrap();
        let paths: Vec<_> = traces.iter().map(|trace| trace.trace.trace_address.clone()).collect();
        assert_eq!(paths, [vec![], vec![0], vec![0, 0], vec![1]]);
        for (path, expected) in [
            (vec![], Some(&traces[0])),
            (vec![0], Some(&traces[1])),
            (vec![0, 0], Some(&traces[2])),
            (vec![1], Some(&traces[3])),
            (vec![2], None),
            (vec![0, 1], None),
            (vec![1, 0], None),
        ] {
            let indices: Vec<_> = path.iter().map(|index| format!("0x{index:x}")).collect();
            let request = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "trace_get", "params": [hash, indices],
            });
            let (response, _) = module.raw_json_request(&request.to_string(), 1).await.unwrap();
            let response: serde_json::Value = serde_json::from_str(response.get()).unwrap();
            assert!(response.get("error").is_none(), "{response}");
            assert_eq!(response["result"], serde_json::to_value(expected).unwrap());
        }
        assert!(api.trace_get(B256::ZERO, vec![]).await.unwrap().is_none());
        assert!(api.trace_get(B256::ZERO, vec![0, 0]).await.unwrap().is_none());
        assert_eq!(api.trace_get_index(hash, 2).await.unwrap().as_ref(), Some(&traces[2]));
    }

    #[tokio::test]
    async fn replay_block_vmtrace_includes_root_and_callcode_bytecode() {
        use crate::EthApiBuilder;
        use alloy_consensus::{Header, TxLegacy};
        use alloy_primitives::{hex, Signature, TxKind};
        use reth_ethereum_primitives::{Block, BlockBody, TransactionSigned};
        use reth_evm_ethereum::EthEvmConfig;
        use reth_network_api::noop::NoopNetwork;
        use reth_primitives_traits::SignerRecoverable;
        use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
        use reth_transaction_pool::test_utils::testing_pool;

        let provider = MockEthProvider::default();
        let target = Address::with_last_byte(0x42);
        let child = Address::with_last_byte(0x43);
        // Increment slot zero, CALLCODE the child, then return the incremented value.
        let root_code: Bytes =
            hex!("60005460010160005560006000600060006000604361fffff25060005460005260206000f3")
                .into();
        let child_code: Bytes = hex!("60025000").into();
        provider.add_account(
            target,
            ExtendedAccount::new(0, U256::ZERO).with_bytecode(root_code.clone()),
        );
        provider.add_account(
            child,
            ExtendedAccount::new(0, U256::ZERO).with_bytecode(child_code.clone()),
        );
        let mut transactions = Vec::new();
        for nonce in 0..2 {
            let tx = TransactionSigned::new_unhashed(
                TxLegacy {
                    nonce,
                    gas_limit: 500_000,
                    to: TxKind::Call(target),
                    ..Default::default()
                }
                .into(),
                Signature::test_signature(),
            );
            provider.add_account(
                tx.recover_signer().unwrap(),
                ExtendedAccount::new(nonce, U256::from(1_000_000)),
            );
            transactions.push(tx);
        }
        let parent = Header { gas_limit: 30_000_000, ..Default::default() };
        let parent_hash = parent.hash_slow();
        provider.add_header(parent_hash, parent);
        let block = Block {
            header: Header { parent_hash, number: 1, gas_limit: 30_000_000, ..Default::default() },
            body: BlockBody { transactions, ..Default::default() },
        };
        let block_hash = block.header.hash_slow();
        provider.add_block(block_hash, block);
        let eth_api = EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build();
        // TN: the vendored MockEthProvider resolves recovered blocks (TN-0036), so upstream's
        // RPC-cache seeding is not needed.
        let api = TraceApi::new(eth_api, BlockingTaskGuard::new(1), EthConfig::default());
        for types in [
            HashSet::from_iter([TraceType::VmTrace]),
            HashSet::from_iter([TraceType::VmTrace, TraceType::Trace, TraceType::StateDiff]),
            HashSet::from_iter([TraceType::Trace, TraceType::StateDiff]),
        ] {
            let traces = api
                .replay_block_transactions(block_hash.into(), types.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(traces.len(), 2);
            for (index, trace) in traces.into_iter().enumerate() {
                assert_eq!(
                    trace.full_trace.output.as_ref(),
                    U256::from(index + 1).to_be_bytes::<32>()
                );
                if types.contains(&TraceType::VmTrace) {
                    let vm = trace.full_trace.vm_trace.as_ref().unwrap();
                    assert_eq!(vm.code, root_code);
                    assert_eq!(
                        vm.ops.iter().find_map(|op| op.sub.as_ref()).unwrap().code,
                        child_code
                    );
                }
                let individual = api
                    .replay_transaction(trace.transaction_hash, types.clone())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(individual.transaction_hash, trace.transaction_hash);
                assert_eq!(trace.full_trace, individual.full_trace);
            }
        }
    }
}
