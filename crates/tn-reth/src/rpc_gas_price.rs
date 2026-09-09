//! Serve `eth_gasPrice` and `eth_maxPriorityFeePerGas` with TN's real fee level.
//!
//! Reth answers both methods from its gas-price oracle, which samples the effective
//! tips of recent blocks (`suggest_tip_cap`, reth `crates/rpc/rpc-eth/src/gas_oracle.rs`)
//! and adds the latest header's base fee for `eth_gasPrice`. No sampling regime lets
//! that oracle quote TN correctly (issue #1305):
//! - TN's base fee is flat within an epoch and sits at the worker's on-chain `WorkerConfigs` fee, 7
//!   wei today. An honestly priced transaction therefore has an effective tip of 0 or 1 wei, below
//!   the oracle's 2-wei `ignore_price` floor, so the oracle discards every honest sample and falls
//!   back to a cached `last_price` that never decays (1 gwei on a fresh process);
//! - a client that prices off the quote and re-submits feeds its bumped price back into the sample
//!   window, so the estimate compounds by the client's bump factor until reth's 500-gwei
//!   `DEFAULT_MAX_GAS_PRICE` clamp becomes the answer. Adiri quoted 500,000,000,007 wei against a
//!   7-wei base fee this way, and the overpayment is not burned: `reward_beneficiary` credits it to
//!   the block beneficiary.
//!
//! [`GasPriceWithEpochBaseFee`] replaces both handlers with quotes from the worker's
//! per-query [`WorkerBaseFee`] handle, no delegation. Resolving the current slot on each
//! request preserves fee updates across worker-count changes (issue #1282):
//! - `eth_gasPrice` answers the worker's current epoch base fee: the fee this node's next batch
//!   actually enforces, tracking whatever governance writes into the worker's `WorkerConfigs` row.
//!   The container wins over the latest header for the reason `crate::rpc_fee_history` documents:
//!   an RPC server belongs to one worker, the latest header may have been sealed by a different
//!   worker at a different fee, and across an epoch boundary the header carries the closing epoch's
//!   fee;
//! - `eth_maxPriorityFeePerGas` answers zero. Batches are ordered by consensus and blocks run far
//!   under the gas limit, so there is no scarcity for a tip to bid on. Like the `eth_blobBaseFee`
//!   override, the honest answer is a constant no provider fault can perturb.

use alloy::primitives::U256;
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use tn_types::{gas_accumulator::WorkerBaseFee, MIN_PROTOCOL_BASE_FEE};

/// The `eth` gas-price oracle methods TN overrides.
///
/// The method names match reth's `EthApiServer` exactly so the built server can swap
/// the handlers in place with `TransportRpcModules::add_or_replace_if_module_configured`.
#[rpc(server, namespace = "eth")]
pub(crate) trait EpochGasPrice {
    /// `eth_gasPrice`: the worker's current epoch base fee, floored at the protocol
    /// constant that equals the pool's default admission minimum.
    #[method(name = "gasPrice")]
    async fn gas_price(&self) -> RpcResult<U256>;

    /// `eth_maxPriorityFeePerGas`: zero, TN has no fee market for a tip to bid in.
    #[method(name = "maxPriorityFeePerGas")]
    async fn max_priority_fee_per_gas(&self) -> RpcResult<U256>;
}

/// Epoch-base-fee quotes over reth's gas-price oracle methods.
///
/// One behavior trade-off is deliberate (issue #1305): reth's pool orders pending
/// transactions by coinbase tip, so a compliant zero-tip transaction sorts last within
/// a batch. At TN's utilization every pending transaction fits in the next batch, so
/// admission, not ordering, decides inclusion.
#[derive(Debug, Clone)]
pub(crate) struct GasPriceWithEpochBaseFee {
    /// Per-query resolver for this worker's current epoch base fee (issue #1282).
    base_fee: WorkerBaseFee,
}

impl GasPriceWithEpochBaseFee {
    /// Create a new handler over the worker's base-fee handle.
    pub(crate) const fn new(base_fee: WorkerBaseFee) -> Self {
        Self { base_fee }
    }
}

#[async_trait]
impl EpochGasPriceServer for GasPriceWithEpochBaseFee {
    async fn gas_price(&self) -> RpcResult<U256> {
        // Keep reth's request-trace parity: operators grep this target.
        tracing::trace!(target: "rpc::eth", "Serving eth_gasPrice");
        // Floor at the protocol constant, which equals the pool's default admission
        // minimum. A `WorkerFeeConfig::Static` row is not clamped. An operator can
        // independently raise `--txpool.minimal-protocol-fee`; such a node may refuse
        // this quote and is outside this handler's default-pool admission guarantee.
        // Quote the chain fee without headroom: legacy transactions pay the full quote.
        Ok(U256::from(self.base_fee.base_fee().max(MIN_PROTOCOL_BASE_FEE)))
    }

    async fn max_priority_fee_per_gas(&self) -> RpcResult<U256> {
        // Keep reth's request-trace parity: operators grep this target.
        tracing::trace!(target: "rpc::eth", "Serving eth_maxPriorityFeePerGas");
        // No delegation: the honest answer is a constant, independent of provider
        // state, so no provider fault can change it.
        Ok(U256::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::gas_accumulator::GasAccumulator;

    /// `eth_gasPrice` quotes the container's current value, not a snapshot taken at
    /// construction: an epoch-boundary `set_base_fee` reaches the next quote.
    #[tokio::test]
    async fn test_gas_price_tracks_the_container() {
        let accumulator = GasAccumulator::new(1);
        let container = accumulator.base_fee(0);
        container.set_base_fee(7);
        let subject = GasPriceWithEpochBaseFee::new(accumulator.worker_base_fee(0));

        assert_eq!(subject.gas_price().await.expect("gas price"), U256::from(7));

        // Governance rewrites the worker's fee at an epoch boundary.
        container.set_base_fee(2_468_013_579);
        assert_eq!(
            subject.gas_price().await.expect("gas price after the epoch update"),
            U256::from(2_468_013_579_u64)
        );
    }

    /// `eth_maxPriorityFeePerGas` is a constant zero.
    #[tokio::test]
    async fn test_max_priority_fee_is_zero() {
        let accumulator = GasAccumulator::new(1);
        let subject = GasPriceWithEpochBaseFee::new(accumulator.worker_base_fee(0));
        assert_eq!(subject.max_priority_fee_per_gas().await.expect("tip"), U256::ZERO);
    }

    /// A sub-floor governance fee (an unclamped `WorkerFeeConfig::Static` row) quotes
    /// the protocol minimum, which equals the pool's default admission minimum.
    #[tokio::test]
    async fn test_gas_price_floors_at_the_protocol_minimum() {
        let accumulator = GasAccumulator::new(1);
        accumulator.base_fee(0).set_base_fee(1);
        let subject = GasPriceWithEpochBaseFee::new(accumulator.worker_base_fee(0));
        assert_eq!(
            subject.gas_price().await.expect("floored gas price"),
            U256::from(MIN_PROTOCOL_BASE_FEE)
        );
    }
}
