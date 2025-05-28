//! Helper functions for EVM execution.
//!
//! Inspired by alloy_evm/src/block/state_changes.

use alloy::primitives::map::foldhash::HashMap;
use reth_chainspec::EthereumHardforks;
use reth_primitives_traits::BlockHeader;
use reth_revm::context::BlockEnv;
use tn_types::{Address, Withdrawals};

/// Collect all balance changes at the end of the block.
///
/// Balance changes only include the block reward for now.
#[inline]
pub fn post_block_balance_increments<H>(
    spec: impl EthereumHardforks,
    block_env: &BlockEnv,
    ommers: &[H],
    withdrawals: Option<&Withdrawals>,
) -> HashMap<Address, u128>
where
    H: BlockHeader,
{
    let balance_increments = HashMap::default();

    // This was only used pre-merge
    //
    // // Add block rewards if they are enabled.
    // if let Some(base_block_reward) = calc::base_block_reward(&spec, block_env.number) {
    //     // Ommer rewards
    //     for ommer in ommers {
    //         *balance_increments.entry(ommer.beneficiary()).or_default() +=
    //             calc::ommer_reward(base_block_reward, block_env.number, ommer.number());
    //     }

    //     // Full block reward
    //     *balance_increments.entry(block_env.beneficiary).or_default() +=
    //         calc::block_reward(base_block_reward, ommers.len());
    // }

    balance_increments
}
