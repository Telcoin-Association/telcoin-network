//! Serve `eth_fillTransaction` with fee defaults from TN's real fee level.
//!
//! Reth's default handler (`EthTransactions::fill_transaction`, reth
//! `crates/rpc/rpc-eth-api/src/helpers/transaction.rs`) fills a request's missing
//! `maxPriorityFeePerGas` from `suggested_priority_fee()` and its missing
//! `maxFeePerGas` from the latest header's base fee plus that tip. Both sources are
//! wrong on TN (issue #1313):
//! - `suggested_priority_fee()` is the tip-sampling gas-price oracle that issue #1305 removed from
//!   `eth_gasPrice` / `eth_maxPriorityFeePerGas`: 1 gwei on a fresh process, and up to reth's
//!   500-gwei `DEFAULT_MAX_GAS_PRICE` clamp once a client bump loop has fed it. Unlike
//!   `eth_sendTransaction`, the method needs no signer, only a `from` address, so any RPC client
//!   reaches it; a client that fills, signs, and submits overpays by the same ratio #1305
//!   describes, and `reward_beneficiary` credits the excess to the block beneficiary rather than
//!   burning it;
//! - the latest header may have been sealed by a different worker at a different fee, and across an
//!   epoch boundary it carries the closing epoch's fee (the reason `crate::rpc_fee_history` and
//!   `crate::rpc_gas_price` quote the shared container instead).
//!
//! [`FillTransactionWithEpochBaseFee`] replaces the method with a handler that first
//! fills the missing fee fields from the worker's shared [`BaseFeeContainer`] and then
//! delegates to the unchanged reth implementation: a missing tip becomes zero (the
//! `eth_maxPriorityFeePerGas` answer), and a missing fee cap becomes the floored epoch
//! base fee plus the tip (the `eth_gasPrice` answer, preserving reth's `cap >= tip`
//! construction for an explicit client tip). Correcting the request *before* the
//! delegate, rather than the response after it like `crate::rpc_fee_history`, keeps the
//! response coherent: the delegate builds and 2718-encodes the returned transaction
//! from the request, so the `raw` bytes and the `tx` fields both carry the corrected
//! fees, where patching the response would desynchronize them. Every other default the
//! delegate fills (nonce, chain id, gas estimate, value) is fee-independent and passes
//! through unchanged, as do explicit client fee fields and legacy `gasPrice` requests,
//! which reth's own handler already leaves untouched.
//!
//! The delegate's blob-fee default (`maxFeePerBlobGas` for a request with EIP-4844
//! fields) still comes from reth's internal helper. TN refuses EIP-4844 transactions at
//! pool admission (issue #1159), so a filled blob request cannot be submitted here and
//! the field is not worth a second intercept.

use crate::TNPrimitives;
use alloy::rpc::types::TransactionRequest;
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_rpc_eth_api::{
    helpers::{EstimateCall, EthApiSpec, EthTransactions, LoadBlock, LoadFee},
    EthApiTypes, RpcNodeCore,
};
use reth_rpc_eth_types::FillTransaction;
use tn_types::{gas_accumulator::BaseFeeContainer, TransactionSigned, MIN_PROTOCOL_BASE_FEE};

/// The `eth` fill method TN overrides.
///
/// The method name matches reth's `EthApiServer` exactly so the built server can swap
/// the handler in place with `TransportRpcModules::add_or_replace_if_module_configured`.
#[rpc(server, namespace = "eth")]
pub(crate) trait EpochFillTransaction {
    /// `eth_fillTransaction`: fill the missing fee fields from the epoch base fee,
    /// then delegate to reth.
    #[method(name = "fillTransaction")]
    async fn fill_transaction(
        &self,
        request: TransactionRequest,
    ) -> RpcResult<FillTransaction<TransactionSigned>>;
}

/// Epoch-base-fee defaults over reth's `EthApi` fill-transaction method.
#[derive(Debug, Clone)]
pub(crate) struct FillTransactionWithEpochBaseFee<Api> {
    /// The reth `EthApi` this handler delegates to.
    eth_api: Api,
    /// This worker's shared epoch base fee.
    base_fee: BaseFeeContainer,
}

impl<Api> FillTransactionWithEpochBaseFee<Api> {
    /// Create a new handler from the built `EthApi` and the worker's base-fee
    /// container.
    pub(crate) const fn new(eth_api: Api, base_fee: BaseFeeContainer) -> Self {
        Self { eth_api, base_fee }
    }
}

/// Fill the request's missing EIP-1559 fee fields from the worker's epoch base fee.
///
/// Mirrors the fee block of reth's `fill_transaction` with TN's sources: a missing tip
/// becomes zero instead of the oracle's suggestion, and a missing fee cap becomes the
/// floored epoch base fee plus the tip instead of the latest header's base fee plus
/// the tip. The floor is the pool's admission minimum, the same clamp
/// `crate::rpc_gas_price` documents. Explicit client fields survive, and a legacy
/// `gasPrice` request passes through whole: reth's handler fills no fee field for it.
///
/// Filling the tip also covers the request that carries an explicit `maxFeePerGas`
/// but no tip: reth would still ask the oracle for the tip it writes into the built
/// transaction, possibly above the client's own cap.
fn fill_fee_defaults(request: TransactionRequest, epoch_fee: u64) -> TransactionRequest {
    match () {
        () if request.gas_price.is_some() => request,
        () => {
            let tip = request.max_priority_fee_per_gas.unwrap_or_default();
            let fee_cap = u128::from(epoch_fee.max(MIN_PROTOCOL_BASE_FEE)).saturating_add(tip);
            TransactionRequest {
                max_priority_fee_per_gas: Some(tip),
                max_fee_per_gas: request.max_fee_per_gas.or(Some(fee_cap)),
                ..request
            }
        }
    }
}

#[async_trait]
impl<Api> EpochFillTransactionServer for FillTransactionWithEpochBaseFee<Api>
where
    Api: EthTransactions
        + EthApiSpec
        + LoadBlock
        + EstimateCall
        + LoadFee
        + EthApiTypes<NetworkTypes = alloy::network::Ethereum>
        + RpcNodeCore<Primitives = TNPrimitives>
        + Clone
        + Send
        + Sync
        + 'static,
    jsonrpsee::types::ErrorObject<'static>: From<<Api as EthApiTypes>::Error>,
{
    async fn fill_transaction(
        &self,
        request: TransactionRequest,
    ) -> RpcResult<FillTransaction<TransactionSigned>> {
        // Keep reth's request-trace parity: operators grep this target.
        tracing::trace!(target: "rpc::eth", ?request, "Serving eth_fillTransaction");
        let corrected = fill_fee_defaults(request, self.base_fee.base_fee());
        Ok(EthTransactions::fill_transaction(&self.eth_api, corrected).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bare EIP-1559 request fills tip zero and fee cap the epoch base fee.
    #[test]
    fn test_fill_defaults_zero_tip_and_epoch_fee_cap() {
        let subject = fill_fee_defaults(TransactionRequest::default(), 12_345);
        assert_eq!(subject.max_priority_fee_per_gas, Some(0));
        assert_eq!(subject.max_fee_per_gas, Some(12_345));
    }

    /// An explicit client tip survives, and the missing cap adds it to the epoch
    /// fee, preserving reth's `cap >= tip` construction.
    #[test]
    fn test_fill_preserves_an_explicit_client_tip() {
        let request =
            TransactionRequest { max_priority_fee_per_gas: Some(5), ..Default::default() };
        let subject = fill_fee_defaults(request, 12_345);
        assert_eq!(subject.max_priority_fee_per_gas, Some(5));
        assert_eq!(subject.max_fee_per_gas, Some(12_350));
    }

    /// Explicit client fee fields pass through untouched.
    #[test]
    fn test_fill_keeps_explicit_fee_fields() {
        let request = TransactionRequest {
            max_priority_fee_per_gas: Some(5),
            max_fee_per_gas: Some(9),
            ..Default::default()
        };
        let subject = fill_fee_defaults(request.clone(), 12_345);
        assert_eq!(subject, request);
    }

    /// An explicit fee cap with a missing tip fills tip zero and keeps the cap:
    /// without the fill, reth would write the oracle's tip into the built
    /// transaction, possibly above the client's own cap.
    #[test]
    fn test_fill_zeroes_a_missing_tip_under_an_explicit_fee_cap() {
        let request = TransactionRequest { max_fee_per_gas: Some(100), ..Default::default() };
        let subject = fill_fee_defaults(request, 12_345);
        assert_eq!(subject.max_priority_fee_per_gas, Some(0));
        assert_eq!(subject.max_fee_per_gas, Some(100));
    }

    /// A legacy `gasPrice` request passes through whole: reth's handler fills no
    /// fee field for it, so there is nothing to correct.
    #[test]
    fn test_fill_skips_a_legacy_gas_price_request() {
        let request = TransactionRequest { gas_price: Some(9), ..Default::default() };
        let subject = fill_fee_defaults(request.clone(), 12_345);
        assert_eq!(subject, request);
    }

    /// A sub-floor governance fee (an unclamped `WorkerFeeConfig::Static` row) fills
    /// the pool's admission minimum, not a cap the node's own pool would refuse.
    #[test]
    fn test_fill_floors_a_sub_floor_epoch_fee() {
        let subject = fill_fee_defaults(TransactionRequest::default(), 1);
        assert_eq!(subject.max_fee_per_gas, Some(u128::from(MIN_PROTOCOL_BASE_FEE)));
    }
}
