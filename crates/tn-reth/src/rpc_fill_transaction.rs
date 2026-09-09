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
//! [`FillTransactionWithEpochBaseFee`] estimates a missing gas limit from the original
//! request, then fills missing fee fields through the worker's [`WorkerBaseFee`] handle
//! and delegates to reth. A missing tip becomes zero (the `eth_maxPriorityFeePerGas`
//! answer), and a missing fee cap becomes twice the floored epoch base fee plus the tip.
//! Explicit client fee fields remain unchanged. Correcting the request *before* the
//! delegate, rather than the response after it like `crate::rpc_fee_history`, keeps the
//! response coherent: the delegate builds and 2718-encodes the returned transaction
//! from the request, so the `raw` bytes and the `tx` fields both carry the corrected
//! fees, where patching the response would desynchronize them. The delegate's remaining
//! defaults (nonce, chain id, value) are fee-independent. Gas estimation depends on the
//! request's fees: pricing first activates the caller-balance gas cap and validates the
//! filled cap against the pending header's base fee. Estimating before filling fees
//! preserves reth's ordering and avoids introducing either check for an unpriced
//! request. Requests with an explicit gas limit skip estimation.
//!
//! Legacy `gasPrice` requests receive no EIP-1559 defaults from this handler or reth.
//!
//! The blob-fee default (`maxFeePerBlobGas` for a request with EIP-4844 fields) still
//! comes from reth's internal helper, before estimation when gas is missing or from
//! the delegate otherwise. TN refuses EIP-4844 transactions at
//! pool admission (issue #1159), so a filled blob request cannot be submitted here and
//! the field is not worth a second intercept.

use crate::TNPrimitives;
use alloy::rpc::types::TransactionRequest;
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_rpc_eth_api::{
    helpers::{estimate::EstimateCall, EthApiSpec, EthTransactions, LoadBlock, LoadFee},
    EthApiTypes, RpcNodeCore,
};
use reth_rpc_eth_types::FillTransaction;
use tn_types::{gas_accumulator::WorkerBaseFee, TransactionSigned, MIN_PROTOCOL_BASE_FEE};

/// Headroom multiplier for a missing EIP-1559 fee cap.
///
/// Twice the epoch fee tolerates fee increases across epoch boundaries, but does not
/// guarantee admission after an arbitrary governance change. EIP-1559 charges
/// `min(max_fee, base_fee + tip)`, so a zero-tip transaction still pays only the base
/// fee. The pool checks balance against the cap, so headroom increases the balance
/// needed for admission. The legacy `eth_gasPrice` quote has no headroom because its
/// consumer pays the entire quoted price.
const FILL_FEE_CAP_HEADROOM: u128 = 2;

/// The `eth` fill method TN overrides.
///
/// The method name matches reth's `EthApiServer` exactly so the built server can swap
/// the handler in place with `TransportRpcModules::add_or_replace_if_module_configured`.
#[rpc(server, namespace = "eth")]
pub(crate) trait EpochFillTransaction {
    /// `eth_fillTransaction`: estimate missing gas, fill missing fee fields from the
    /// epoch base fee, then delegate to reth.
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
    /// Per-query resolver for this worker's current epoch base fee (issue #1282).
    base_fee: WorkerBaseFee,
}

impl<Api> FillTransactionWithEpochBaseFee<Api> {
    /// Create a new handler from the built `EthApi` and the worker's base-fee
    /// handle.
    pub(crate) const fn new(eth_api: Api, base_fee: WorkerBaseFee) -> Self {
        Self { eth_api, base_fee }
    }
}

/// Fill the request's missing EIP-1559 fee fields from the worker's epoch base fee.
///
/// Mirrors the fee block of reth's `fill_transaction` with TN's sources: a missing tip
/// becomes zero instead of the oracle's suggestion, and a missing fee cap becomes
/// twice the floored epoch base fee plus the tip. The floor is the protocol constant,
/// equal to the pool's default admission minimum, as `crate::rpc_gas_price` documents.
/// Explicit client fields survive, and a legacy `gasPrice` request receives no
/// EIP-1559 fee defaults. Reth may still fill its blob-fee default.
///
/// Filling the tip also covers the request that carries an explicit `maxFeePerGas`
/// but no tip: reth would still ask the oracle for the tip it writes into the built
/// transaction, possibly above the client's own cap.
fn fill_fee_defaults(request: TransactionRequest, epoch_fee: u64) -> TransactionRequest {
    if request.gas_price.is_some() {
        request
    } else {
        let tip = request.max_priority_fee_per_gas.unwrap_or_default();
        let fee_cap = u128::from(epoch_fee.max(MIN_PROTOCOL_BASE_FEE))
            .saturating_mul(FILL_FEE_CAP_HEADROOM)
            .saturating_add(tip);
        TransactionRequest {
            max_priority_fee_per_gas: Some(tip),
            max_fee_per_gas: request.max_fee_per_gas.or(Some(fee_cap)),
            ..request
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
        let mut request = request;
        // Preserve reth's estimate-before-fees ordering. Pricing an unpriced request
        // first activates the sender-balance gas allowance and checks the cap against
        // the pending header's fee, which can exceed this worker's epoch fee.
        // Supplying gas here makes the delegate skip its own estimate.
        // Leave a missing sender to the delegate's validation before any estimate call.
        if request.from.is_some() && request.gas.is_none() {
            // Reth sets the node chain ID and prepares blob fields before estimating.
            // Preserve those defaults without supplying EIP-1559 fee fields yet.
            request.chain_id = Some(self.eth_api.chain_id().to());
            if request.has_eip4844_fields() && request.max_fee_per_blob_gas.is_none() {
                request.max_fee_per_blob_gas =
                    Some(LoadFee::blob_base_fee(&self.eth_api).await?.to());
            }
            if request.sidecar.is_some() && request.blob_versioned_hashes.is_none() {
                request.populate_blob_hashes();
            }
            let gas = EstimateCall::estimate_gas_at(
                &self.eth_api,
                request.clone(),
                alloy::eips::BlockId::pending(),
                None,
            )
            .await?;
            request.gas = Some(gas.saturating_to());
        }
        let corrected = fill_fee_defaults(request, self.base_fee.base_fee());
        EthTransactions::fill_transaction(&self.eth_api, corrected).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bare EIP-1559 request fills tip zero and twice the epoch base fee as its cap.
    #[test]
    fn test_fill_defaults_zero_tip_and_epoch_fee_cap() {
        let subject = fill_fee_defaults(TransactionRequest::default(), 12_345);
        assert_eq!(subject.max_priority_fee_per_gas, Some(0));
        assert_eq!(subject.max_fee_per_gas, Some(24_690));
    }

    /// An explicit client tip survives, and the missing cap adds it to twice the
    /// epoch fee, preserving reth's `cap >= tip` construction.
    #[test]
    fn test_fill_preserves_an_explicit_client_tip() {
        let request =
            TransactionRequest { max_priority_fee_per_gas: Some(5), ..Default::default() };
        let subject = fill_fee_defaults(request, 12_345);
        assert_eq!(subject.max_priority_fee_per_gas, Some(5));
        assert_eq!(subject.max_fee_per_gas, Some(24_695));
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

    /// A legacy `gasPrice` request receives no EIP-1559 fee defaults. Reth may still
    /// fill its blob-fee default after this helper returns.
    #[test]
    fn test_fill_skips_a_legacy_gas_price_request() {
        let request = TransactionRequest { gas_price: Some(9), ..Default::default() };
        let subject = fill_fee_defaults(request.clone(), 12_345);
        assert_eq!(subject, request);
    }

    /// A sub-floor governance fee (an unclamped `WorkerFeeConfig::Static` row) fills
    /// twice the protocol minimum, matching the default pool floor with headroom.
    #[test]
    fn test_fill_floors_a_sub_floor_epoch_fee() {
        let subject = fill_fee_defaults(TransactionRequest::default(), 1);
        assert_eq!(subject.max_fee_per_gas, Some(2 * u128::from(MIN_PROTOCOL_BASE_FEE)));
    }
}
