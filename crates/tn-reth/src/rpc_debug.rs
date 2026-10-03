//! Correct debug call simulations while retaining reth's tracing permits and historical replay.
//!
//! Related parity trace audit (reth v1.11.3): `trace_call` uses `spawn_with_call_at`
//! and honors explicit block overrides, but still defaults to the selected header's fee.
//! `trace_callMany` defaults to pending and prepares each call from `evm_env_at` without
//! a block-override argument, so it also retains that fee. These trace-namespace gaps
//! need a separate adapter. `trace_rawTransaction` likewise uses the selected environment.
//! Transaction and block replay methods must keep their recorded fees. `debug_traceCallMany`
//! is covered here through per-bundle overrides applied after recorded transactions replay.
//! Reth's call-many handler consumes tracer and state options but ignores shared block
//! overrides in `GethDebugTracingCallOptions`; clients use each bundle's `blockOverride`.

use crate::rpc_call::{bundles_at_epoch, epoch_base_fee, targets_tip, with_base_fee};
use alloy::{
    eips::BlockId,
    rpc::types::{
        trace::geth::{GethDebugTracingCallOptions, GethTrace},
        Bundle, StateContext, TransactionRequest,
    },
};
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth::rpc::api::DebugApiServer;
use tn_types::gas_accumulator::WorkerBaseFee;

/// The debug methods that simulate new calls on a selected state.
#[rpc(server, namespace = "debug")]
pub(crate) trait EpochDebugSimulation {
    /// Trace a call while preserving tracer, state, block, and transaction-index options.
    #[method(name = "traceCall")]
    async fn trace_call(
        &self,
        request: TransactionRequest,
        block: Option<BlockId>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<GethTrace>;

    /// Trace dependent bundles with the same fee policy as eth_callMany.
    #[method(name = "traceCallMany")]
    async fn trace_call_many(
        &self,
        bundles: Vec<Bundle>,
        state_context: Option<StateContext>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<Vec<Vec<GethTrace>>>;
}

/// The production debug RPC handler paired with this worker's fee resolver.
#[derive(Debug, Clone)]
pub(crate) struct DebugSimulationWithEpochBaseFee<Api> {
    /// Call the RPC trait entry points so reth still acquires its tracing permit.
    debug_api: Api,
    /// Read the current epoch's fee for each request.
    base_fee: WorkerBaseFee,
}

impl<Api> DebugSimulationWithEpochBaseFee<Api> {
    /// Retain the debug handler and guard from the production registry.
    pub(crate) const fn new(debug_api: Api, base_fee: WorkerBaseFee) -> Self {
        Self { debug_api, base_fee }
    }
}

#[async_trait]
impl<Api> EpochDebugSimulationServer for DebugSimulationWithEpochBaseFee<Api>
where
    Api: DebugApiServer<TransactionRequest> + Send + Sync + 'static,
{
    /// Insert a missing fee override before conversion without affecting block replay.
    async fn trace_call(
        &self,
        request: TransactionRequest,
        block: Option<BlockId>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<GethTrace> {
        let opts = if targets_tip(block.unwrap_or_default()) {
            let mut opts = opts.unwrap_or_default();
            opts.block_overrides =
                with_base_fee(opts.block_overrides, epoch_base_fee(&self.base_fee));
            Some(opts)
        } else {
            opts
        };
        self.debug_api.debug_trace_call(request, block, opts).await
    }

    /// Delegate through reth's permit-bearing handler with per-bundle overrides.
    async fn trace_call_many(
        &self,
        bundles: Vec<Bundle>,
        state_context: Option<StateContext>,
        opts: Option<GethDebugTracingCallOptions>,
    ) -> RpcResult<Vec<Vec<GethTrace>>> {
        self.debug_api
            .debug_trace_call_many(
                bundles_at_epoch(bundles, &state_context, &self.base_fee),
                state_context,
                opts,
            )
            .await
    }
}
