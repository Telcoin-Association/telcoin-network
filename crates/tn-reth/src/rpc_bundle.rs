//! Price tip bundle simulations with the worker's epoch fee before reth validates signed txs.

use crate::rpc_call::{epoch_base_fee, targets_tip};
use alloy::rpc::types::mev::{EthCallBundle, EthCallBundleResponse};
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_rpc_eth_api::EthCallBundleApiServer;
use tn_types::gas_accumulator::WorkerBaseFee;

/// The signed-bundle simulation endpoint in the eth namespace.
#[rpc(server, namespace = "eth")]
pub(crate) trait EpochBundle {
    /// Simulate signed transactions while preserving all explicit bundle overrides.
    #[method(name = "callBundle")]
    async fn call_bundle(&self, bundle: EthCallBundle) -> RpcResult<EthCallBundleResponse>;
}

/// The existing reth bundle handler and this worker's live fee resolver.
#[derive(Debug, Clone)]
pub(crate) struct BundleWithEpochBaseFee<Api> {
    /// Reth owns signature recovery, state access, validation, and execution.
    bundle_api: Api,
    /// Resolve the fee after accumulator slots are replaced at epoch boundaries.
    base_fee: WorkerBaseFee,
}

impl<Api> BundleWithEpochBaseFee<Api> {
    /// Wrap the handler from the same registry as the production eth module.
    pub(crate) const fn new(bundle_api: Api, base_fee: WorkerBaseFee) -> Self {
        Self { bundle_api, base_fee }
    }
}

#[async_trait]
impl<Api> EpochBundleServer for BundleWithEpochBaseFee<Api>
where
    Api: EthCallBundleApiServer + Send + Sync + 'static,
{
    /// Use the state block's tag, since the execution block number can be in the future.
    async fn call_bundle(&self, mut bundle: EthCallBundle) -> RpcResult<EthCallBundleResponse> {
        if targets_tip(bundle.state_block_number.into()) {
            bundle.base_fee.get_or_insert_with(|| u128::from(epoch_base_fee(&self.base_fee)));
        }
        self.bundle_api.call_bundle(bundle).await
    }
}
