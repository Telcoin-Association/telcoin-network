//! Simulate latest and pending requests with this worker's epoch base fee.
//!
//! Reth validates explicit EIP-1559 fee caps while converting a request into an EVM
//! transaction, before execution can honor `disable_base_fee`. A header can retain
//! the preceding epoch's fee or another worker's fee, so using it here disagrees with
//! TN's fee quotes and pool admission (issue #1348).
//!
//! Only the latest and pending tags use the live fee. Numbered, hashed, safe, and
//! finalized blocks retain their historical environments, and explicit block
//! overrides take precedence. Bundles are corrected after reth's historical replay,
//! and validated simulations receive the fee on each constructed block (#1554).
//! The transaction request is never repriced:
//! reth still owns fee-cap validation, balance checks, gas estimation, and execution.

use crate::evm::TnEvmConfig;
use alloy::{
    eips::{eip2930::AccessListResult, BlockId, BlockNumberOrTag},
    primitives::{Bytes, U256},
    rpc::types::{
        simulate::{SimulatePayload, SimulatedBlock},
        state::{EvmOverrides, StateOverride},
        BlockOverrides, Bundle, EthCallResponse, StateContext, TransactionRequest,
    },
};
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_evm::EvmEnv;
use reth_rpc_eth_api::{
    helpers::{estimate::EstimateCall, Call, EthCall, LoadState, SpawnBlocking, Trace},
    EthApiTypes, RpcBlock, RpcNodeCore,
};
use tn_types::{gas_accumulator::WorkerBaseFee, MIN_PROTOCOL_BASE_FEE};

/// The simulation methods whose tip environments use the worker's current fee.
#[rpc(server, namespace = "eth")]
pub(crate) trait EpochSimulation {
    /// Execute a call, preserving explicit state and block overrides.
    #[method(name = "call")]
    async fn call(
        &self,
        request: TransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<Bytes>;

    /// Estimate gas using the fee belonging to the selected block or current epoch.
    #[method(name = "estimateGas")]
    async fn estimate_gas(
        &self,
        request: TransactionRequest,
        block_number: Option<BlockId>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<U256>;

    /// Create an access list using the selected block or current epoch's fee.
    #[method(name = "createAccessList")]
    async fn create_access_list(
        &self,
        request: TransactionRequest,
        block_number: Option<BlockId>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<AccessListResult>;

    /// Execute dependent bundles without changing the selected block's replay fee.
    #[method(name = "callMany")]
    async fn call_many(
        &self,
        bundles: Vec<Bundle>,
        state_context: Option<StateContext>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<Vec<Vec<EthCallResponse>>>;

    /// Apply the current fee to every validated block simulated on the tip.
    #[method(name = "simulateV1")]
    async fn simulate_v1(
        &self,
        payload: SimulatePayload,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<SimulatedBlock<RpcBlock<alloy::network::Ethereum>>>>;
}

/// The production reth helpers paired with this worker's per-query fee resolver.
#[derive(Debug, Clone)]
pub(crate) struct SimulationWithEpochBaseFee<Api> {
    /// The reth API responsible for state access and execution.
    eth_api: Api,
    /// Resolve the current fee even after accumulator slots are replaced.
    base_fee: WorkerBaseFee,
}

impl<Api> SimulationWithEpochBaseFee<Api> {
    /// Wrap a built reth API without changing its execution configuration.
    pub(crate) const fn new(eth_api: Api, base_fee: WorkerBaseFee) -> Self {
        Self { eth_api, base_fee }
    }
}

/// Whether a request follows the chain tip instead of selecting a historical block.
pub(crate) fn targets_tip(block_id: BlockId) -> bool {
    matches!(block_id, BlockId::Number(BlockNumberOrTag::Latest | BlockNumberOrTag::Pending))
}

/// Fill only a missing base-fee override, preserving zero and every other field.
pub(crate) fn with_base_fee(
    overrides: Option<BlockOverrides>,
    base_fee: u64,
) -> Option<BlockOverrides> {
    let mut overrides = overrides.unwrap_or_default();
    overrides.base_fee.get_or_insert(U256::from(base_fee));
    Some(overrides)
}

/// Resolve and clamp the worker's fee once for all environments in a request.
pub(crate) fn epoch_base_fee(base_fee: &WorkerBaseFee) -> u64 {
    base_fee.base_fee().max(MIN_PROTOCOL_BASE_FEE)
}

/// Correct each bundle's simulation override without repricing recorded transactions.
pub(crate) fn bundles_at_epoch(
    bundles: Vec<Bundle>,
    state_context: &Option<StateContext>,
    base_fee: &WorkerBaseFee,
) -> Vec<Bundle> {
    if targets_tip(state_context.as_ref().and_then(|ctx| ctx.block_number).unwrap_or_default()) {
        let fee = epoch_base_fee(base_fee);
        bundles
            .into_iter()
            .map(|mut bundle| {
                bundle.block_override = with_base_fee(bundle.block_override, fee);
                bundle
            })
            .collect()
    } else {
        bundles
    }
}

/// Load reth's environment and state identifier, correcting only the tip's base fee.
async fn evm_env_at_epoch<Api>(
    eth_api: &Api,
    block_id: BlockId,
    base_fee: &WorkerBaseFee,
) -> Result<(EvmEnv, BlockId), Api::Error>
where
    Api: Call + RpcNodeCore<Evm = TnEvmConfig>,
{
    let (mut evm_env, at) = LoadState::evm_env_at(eth_api, block_id).await?;
    if targets_tip(block_id) {
        evm_env.block_env.basefee = base_fee.base_fee().max(MIN_PROTOCOL_BASE_FEE);
    }
    Ok((evm_env, at))
}

/// Share the corrected estimation path with `eth_fillTransaction` before filling fees.
pub(crate) async fn estimate_gas_at_epoch<Api>(
    eth_api: &Api,
    request: TransactionRequest,
    block_id: BlockId,
    state_override: Option<StateOverride>,
    base_fee: &WorkerBaseFee,
) -> Result<U256, Api::Error>
where
    Api: EstimateCall
        + EthApiTypes<NetworkTypes = alloy::network::Ethereum>
        + RpcNodeCore<Evm = TnEvmConfig>,
{
    let (evm_env, at) = evm_env_at_epoch(eth_api, block_id, base_fee).await?;
    SpawnBlocking::spawn_blocking_io_fut(eth_api, move |this| async move {
        let state = LoadState::state_at_block_id(&this, at).await?;
        EstimateCall::estimate_gas_with(&this, evm_env, request, state, state_override)
    })
    .await
}

#[async_trait]
impl<Api> EpochSimulationServer for SimulationWithEpochBaseFee<Api>
where
    Api: EthCall
        + Trace
        + EthApiTypes<NetworkTypes = alloy::network::Ethereum>
        + RpcNodeCore<Evm = TnEvmConfig>
        + Clone
        + Send
        + Sync
        + 'static,
    jsonrpsee::types::ErrorObject<'static>: From<<Api as EthApiTypes>::Error>,
{
    /// Apply the epoch fee before request conversion, preserving explicit overrides.
    async fn call(
        &self,
        request: TransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<Bytes> {
        tracing::trace!(target: "rpc::eth", ?request, ?block_number, ?state_overrides, ?block_overrides, "Serving eth_call");
        let block_overrides = if targets_tip(block_number.unwrap_or_default()) {
            let mut overrides = block_overrides.unwrap_or_default();
            overrides.base_fee.get_or_insert_with(|| {
                U256::from(self.base_fee.base_fee().max(MIN_PROTOCOL_BASE_FEE))
            });
            Some(overrides)
        } else {
            block_overrides
        };
        EthCall::call(
            &self.eth_api,
            request,
            block_number,
            EvmOverrides::new(state_overrides, block_overrides),
        )
        .await
        .map_err(Into::into)
    }

    /// Delegate estimation with the corrected environment and unchanged client fees.
    async fn estimate_gas(
        &self,
        request: TransactionRequest,
        block_number: Option<BlockId>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<U256> {
        tracing::trace!(target: "rpc::eth", ?request, ?block_number, "Serving eth_estimateGas");
        estimate_gas_at_epoch(
            &self.eth_api,
            request,
            block_number.unwrap_or_default(),
            state_override,
            &self.base_fee,
        )
        .await
        .map_err(Into::into)
    }

    /// Delegate both access-list executions with the same corrected environment.
    async fn create_access_list(
        &self,
        request: TransactionRequest,
        block_number: Option<BlockId>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<AccessListResult> {
        tracing::trace!(target: "rpc::eth", ?request, ?block_number, ?state_override, "Serving eth_createAccessList");
        let (evm_env, at) =
            evm_env_at_epoch(&self.eth_api, block_number.unwrap_or_default(), &self.base_fee)
                .await?;
        EthCall::create_access_list_with(&self.eth_api, evm_env, at, request, state_override)
            .await
            .map_err(Into::into)
    }

    /// Preserve reth's state selection, replay, sequential execution, and error handling.
    async fn call_many(
        &self,
        bundles: Vec<Bundle>,
        state_context: Option<StateContext>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<Vec<Vec<EthCallResponse>>> {
        EthCall::call_many(
            &self.eth_api,
            bundles_at_epoch(bundles, &state_context, &self.base_fee),
            state_context,
            state_override,
        )
        .await
        .map_err(Into::into)
    }

    /// Keep validation-disabled simulations at reth's zero fee unless the client overrides it.
    async fn simulate_v1(
        &self,
        mut payload: SimulatePayload,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<SimulatedBlock<RpcBlock<alloy::network::Ethereum>>>> {
        // Match reth's RPC entry point: the helper itself does not acquire this guard.
        let _permit = self.eth_api.tracing_task_guard().clone().acquire_owned().await;
        // Leave oversized payloads untouched for reth's cheap block-count rejection.
        if payload.validation
            && targets_tip(block.unwrap_or_default())
            && u64::try_from(payload.block_state_calls.len())
                .is_ok_and(|count| count <= self.eth_api.max_simulate_blocks())
        {
            let fee = epoch_base_fee(&self.base_fee);
            payload.block_state_calls = payload
                .block_state_calls
                .into_iter()
                .map(|mut block| {
                    block.block_overrides = with_base_fee(block.block_overrides, fee);
                    block
                })
                .collect();
        }
        EthCall::simulate_v1(&self.eth_api, payload, block).await.map_err(Into::into)
    }
}
