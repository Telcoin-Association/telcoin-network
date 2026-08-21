//! Serve `eth_feeHistory` with TN's real next-block base fee.
//!
//! Reth appends one extra `baseFeePerGas` entry for the block after the newest returned
//! block and computes it with Ethereum's EIP-1559 schedule (`next_block_base_fee`, reth
//! `crates/rpc/rpc-eth-api/src/helpers/fee.rs`). TN does not derive base fees that way:
//! each worker's base fee comes from the on-chain `WorkerConfigs` contract and is flat
//! within an epoch (issue #1231). Wallets derive `maxFeePerGas` from exactly this entry,
//! so the stock prediction misquotes the fee by up to 12.5% in either direction after a
//! block that is off the EIP-1559 gas target.
//!
//! [`FeeHistoryWithEpochBaseFee`] replaces the method with a handler that delegates to
//! the unchanged reth implementation and then overwrites the final `baseFeePerGas`
//! entry:
//! - when the block after the newest returned block is already in the database (a historical
//!   range), the entry is that block's actual recorded base fee. Recorded history is
//!   worker-agnostic: the entry reports the fee of whichever worker produced that block, exactly
//!   like the per-block entries reth already returns;
//! - otherwise (the newest returned block is the tip), the entry is a quote: this worker's current
//!   epoch base fee from the shared [`BaseFeeContainer`]. Every RPC server belongs to one worker,
//!   and its clients price new transactions for that worker's next block.
//!
//! Two boundary windows bound the correction's accuracy. The database read has no
//! in-memory overlay ([`RethEnv::header_by_number`]), so a block executed but not yet
//! persisted falls back to the container value; within an epoch the two agree, and the
//! answer self-heals once the block persists. And the container update runs with the
//! epoch close, so for a short window at an epoch boundary the tip quote (or the
//! unpersisted-block fallback) can reflect the neighboring epoch's fee until the update
//! lands or the block persists. All other entries and fields pass through unchanged;
//! the blob-fee columns are issue #1231 item 3.

use crate::RethEnv;
use alloy::{eips::BlockNumberOrTag, primitives::U64, rpc::types::FeeHistory};
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_rpc_eth_api::{helpers::EthFees, EthApiTypes};
use tn_types::gas_accumulator::BaseFeeContainer;

/// The `eth` fee-history method TN overrides.
///
/// The method name matches reth's `EthApiServer` exactly so the built server can swap
/// the handler in place with `TransportRpcModules::add_or_replace_if_module_configured`.
#[rpc(server, namespace = "eth")]
pub(crate) trait EpochFeeHistory {
    /// `eth_feeHistory`: delegate to reth, then correct the final `baseFeePerGas` entry.
    #[method(name = "feeHistory")]
    async fn fee_history(
        &self,
        block_count: U64,
        newest_block: BlockNumberOrTag,
        reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory>;
}

/// Epoch-base-fee correction over reth's `EthApi` fee-history method.
#[derive(Debug, Clone)]
pub(crate) struct FeeHistoryWithEpochBaseFee<Api> {
    /// The reth `EthApi` this handler delegates to.
    eth_api: Api,
    /// Database access for the recorded base fee of an already-executed next block.
    reth_env: RethEnv,
    /// This worker's shared epoch base fee.
    base_fee: BaseFeeContainer,
}

impl<Api> FeeHistoryWithEpochBaseFee<Api> {
    /// Create a new handler from the built `EthApi`, the env, and the worker's
    /// base-fee container.
    pub(crate) const fn new(eth_api: Api, reth_env: RethEnv, base_fee: BaseFeeContainer) -> Self {
        Self { eth_api, reth_env, base_fee }
    }
}

/// Return the number of the block after the newest block `history` covers.
///
/// `gas_used_ratio` holds one entry per returned block, so the newest returned block
/// is `oldest_block + len - 1`. An empty history (reth returns one for a
/// `block_count` of zero) has no next block.
fn next_block_number(history: &FeeHistory) -> Option<u64> {
    u64::try_from(history.gas_used_ratio.len())
        .ok()
        .filter(|returned_blocks| *returned_blocks > 0)
        .map(|returned_blocks| history.oldest_block.saturating_add(returned_blocks))
}

/// Overwrite the final `baseFeePerGas` entry with TN's real next-block base fee.
///
/// The recorded fee wins when the next block already exists; the worker's current
/// epoch fee is the answer for the tip. A history with no returned block stays
/// untouched: the final entry only describes a next block when
/// [`next_block_number`] names one.
fn overwrite_next_base_fee(history: &mut FeeHistory, next_header_fee: Option<u64>, epoch_fee: u64) {
    let corrected = u128::from(next_header_fee.unwrap_or(epoch_fee));
    let has_next_block = next_block_number(history).is_some();
    history
        .base_fee_per_gas
        .last_mut()
        .filter(|_| has_next_block)
        .into_iter()
        .for_each(|entry| *entry = corrected);
}

#[async_trait]
impl<Api> EpochFeeHistoryServer for FeeHistoryWithEpochBaseFee<Api>
where
    Api: EthFees + Clone + Send + Sync + 'static,
    jsonrpsee::types::ErrorObject<'static>: From<<Api as EthApiTypes>::Error>,
{
    async fn fee_history(
        &self,
        block_count: U64,
        newest_block: BlockNumberOrTag,
        reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory> {
        // Keep reth's request-trace parity: operators grep this target.
        tracing::trace!(target: "rpc::eth", ?block_count, ?newest_block, ?reward_percentiles, "Serving eth_feeHistory");
        let mut history =
            EthFees::fee_history(&self.eth_api, block_count.to(), newest_block, reward_percentiles)
                .await?;
        // A failed database read falls back to the container value: the entry is a fee
        // quote, and the delegated call above already surfaced real range errors. The
        // breadcrumb separates a provider fault from an absent block.
        let next_header_fee = next_block_number(&history)
            .and_then(|number| {
                self.reth_env
                    .header_by_number(number)
                    .inspect_err(|error| {
                        tracing::debug!(
                            target: "rpc::eth",
                            %number,
                            ?error,
                            "fee-history next-header read failed; using the epoch base fee",
                        )
                    })
                    .ok()
                    .flatten()
            })
            .and_then(|header| header.base_fee_per_gas);
        overwrite_next_base_fee(&mut history, next_header_fee, self.base_fee.base_fee());
        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fee history with `ratios` returned blocks starting at `oldest` and the given
    /// `baseFeePerGas` column.
    fn history(oldest: u64, ratios: usize, fees: Vec<u128>) -> FeeHistory {
        FeeHistory {
            base_fee_per_gas: fees,
            gas_used_ratio: vec![0.5; ratios],
            oldest_block: oldest,
            ..Default::default()
        }
    }

    #[test]
    fn test_next_block_number_follows_newest_returned_block() {
        // Blocks 5, 6, 7 returned: the predicted entry describes block 8.
        assert_eq!(next_block_number(&history(5, 3, vec![7, 7, 7, 7])), Some(8));
    }

    #[test]
    fn test_next_block_number_empty_history() {
        assert_eq!(next_block_number(&FeeHistory::default()), None);
    }

    #[test]
    fn test_overwrite_prefers_recorded_header_fee() {
        let mut subject = history(0, 1, vec![7, 6]);
        overwrite_next_base_fee(&mut subject, Some(9), 1_000);
        assert_eq!(subject.base_fee_per_gas, vec![7, 9]);
    }

    #[test]
    fn test_overwrite_falls_back_to_epoch_fee_at_tip() {
        let mut subject = history(0, 1, vec![7, 6]);
        overwrite_next_base_fee(&mut subject, None, 1_000);
        assert_eq!(subject.base_fee_per_gas, vec![7, 1_000]);
    }

    #[test]
    fn test_overwrite_leaves_empty_history_untouched() {
        let mut subject = FeeHistory::default();
        overwrite_next_base_fee(&mut subject, None, 1_000);
        assert!(subject.base_fee_per_gas.is_empty());
    }

    #[test]
    fn test_overwrite_requires_a_returned_block() {
        // A lone fee entry with no returned block names no next block: the recorded
        // value must survive.
        let mut subject = history(0, 0, vec![7]);
        overwrite_next_base_fee(&mut subject, None, 1_000);
        assert_eq!(subject.base_fee_per_gas, vec![7]);
    }
}
