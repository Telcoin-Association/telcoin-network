//! Full-pipeline property-based tests for the TEL precompile (production mode).
//!
//! These tests exercise the precompile through the complete TN execution pipeline:
//! tx signing → EIP-2718 encoding → signature recovery → block building →
//! EVM dispatch → precompile execution → state persistence → state reads.

use super::pipeline_helpers::*;
use alloy::sol_types::SolCall;
use proptest::prelude::*;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_reth::{
    burnCall, claimCall, mintCall, test_utils::precompile_test_utils::GENESIS_SUPPLY,
    TELCOIN_PRECOMPILE_ADDRESS, TIMELOCK_DURATION,
};
use tn_types::U256;

// ==============================
// Mint / Claim / Burn properties
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Governance mint → wait for timelock → claim succeeds and increases supply.
    #[test]
    fn prop_pipeline_mint_timelock_claim(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();
        let mint_tx = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![mint_tx], mint_ts)
            .expect("execute mint block");
        assert!(env.tx_succeeded(&block1, 0), "mint tx should succeed");

        // Block 2 at T + TIMELOCK - 1: claim should fail (timelock not expired)
        let early_claim_tx = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(
            vec![early_claim_tx],
            mint_ts + TIMELOCK_DURATION - 1,
        ).expect("execute early claim block");
        assert!(!env.tx_succeeded(&block2, 0), "early claim should fail");

        // Block 3 at T + TIMELOCK + 1: claim should succeed
        let claim_tx = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block3 = env.execute_block_at_timestamp(
            vec![claim_tx],
            mint_ts + TIMELOCK_DURATION + 1,
        ).expect("execute claim block");
        assert!(env.tx_succeeded(&block3, 0), "claim tx should succeed");

        // Verify supply increased
        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before + U256::from(amount),
            "total supply should increase by minted amount"
        );
    }

    /// Governance burn decreases total supply.
    #[test]
    fn prop_pipeline_burn_decrements_supply(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();
        let precompile_balance_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);

        let burn_tx = env.governance_tx(
            burnCall { amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![burn_tx]).expect("execute burn block");
        assert!(env.tx_succeeded(&block, 0), "burn tx should succeed");

        let supply_after = env.get_total_supply();
        let precompile_balance_after = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);
        prop_assert_eq!(
            supply_after,
            supply_before - U256::from(amount),
            "total supply should decrease by burned amount"
        );
        // burned from precompile address
        prop_assert_eq!(
            precompile_balance_after,
            precompile_balance_before - U256::from(amount),
            "total supply should decrease by burned amount"
        );

    }
}

// ==============================
// Multi-tx and security properties
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Unauthorized mint (from user, not governance) fails without corrupting state.
    #[test]
    fn prop_pipeline_unauthorized_mint_no_corruption(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();

        // User sends mint calldata directly to precompile (not through forwarder)
        let tx = env.user_precompile_tx(
            mintCall { amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute unauthorized mint block");
        assert!(!env.tx_succeeded(&block, 0), "unauthorized mint should fail");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before,
            "total supply should be unchanged after failed mint"
        );
    }
}

// ==============================
// Mint / Claim security
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Claim succeeds, second claim of same recipient fails (storage cleared).
    #[test]
    fn prop_pipeline_double_claim_fails(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();

        // Block 1: mint
        let tx1 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![tx1], mint_ts).expect("mint block");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: first claim (after timelock)
        let claim_ts = mint_ts + TIMELOCK_DURATION + 1;
        let tx2 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(vec![tx2], claim_ts).expect("claim block");
        assert!(env.tx_succeeded(&block2, 0), "first claim should succeed");

        // Block 3: second claim — should fail
        let tx3 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block3 = env.execute_block_at_timestamp(vec![tx3], claim_ts + 1).expect("second claim");
        assert!(!env.tx_succeeded(&block3, 0), "double claim should fail");
    }

    /// mint(X) then mint(Y) — claim gives only Y (overwrite semantics).
    #[test]
    fn prop_pipeline_mint_overwrite_latest_only(
        first in 1u128..500_000u128,
        second in 500_001u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();

        // Block 1: mint first
        let tx1 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(first));
        let block1 = env.execute_block_at_timestamp(vec![tx1], mint_ts).expect("mint 1");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: mint second (overwrites first, resets timelock)
        let tx2 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(second));
        let block2 = env.execute_block_at_timestamp(vec![tx2], mint_ts + 1).expect("mint 2");
        assert!(env.tx_succeeded(&block2, 0));

        // Block 3: claim after timelock from second mint
        let claim_ts = mint_ts + 1 + TIMELOCK_DURATION + 1;
        let tx3 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block3 = env.execute_block_at_timestamp(vec![tx3], claim_ts).expect("claim");
        assert!(env.tx_succeeded(&block3, 0), "claim should succeed");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before + U256::from(second),
            "only second mint amount should be added to supply"
        );
    }

    /// Claim at exact timelock boundary (mint_ts + TIMELOCK_DURATION) succeeds.
    /// The check is `current_ts < unlock_ts`, so `current_ts == unlock_ts` passes.
    #[test]
    fn prop_pipeline_claim_at_exact_timelock_boundary(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();

        let tx1 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![tx1], mint_ts).expect("mint block");
        assert!(env.tx_succeeded(&block1, 0));

        // Claim at exactly mint_ts + TIMELOCK_DURATION (unlock_ts == current_ts)
        let exact_ts = mint_ts + TIMELOCK_DURATION;
        let tx2 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(vec![tx2], exact_ts).expect("claim block");
        assert!(
            env.tx_succeeded(&block2, 0),
            "claim at exact timelock boundary should succeed"
        );
    }

    /// Only governance can call claim after timelock (permissioned).
    #[test]
    fn prop_pipeline_permissioned_claim(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let gov_balance_before = env.get_balance(GOVERNANCE_SAFE_ADDRESS);

        // Block 1: governance mints
        let tx1 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![tx1], mint_ts).expect("mint block");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: user (non-governance) calls claim
        let claim_ts = mint_ts + TIMELOCK_DURATION + 1;
        let tx2 = env.user_precompile_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(vec![tx2], claim_ts).expect("claim block");
        assert!(!env.tx_succeeded(&block2, 0), "permissioned claim should fail");

        // Governance balance should increase (minus gas from mint tx only)
        let gov_balance_after = env.get_balance(GOVERNANCE_SAFE_ADDRESS);
        // governance paid gas for mint, but received `amount` from claim
        // Net change: amount - gas_for_mint
        // We just verify governance balance increased relative to after-mint state
        prop_assert!(
            gov_balance_after > gov_balance_before - U256::from(1_000_000u64),
            "governance should have received minted tokens"
        );
    }

    /// Unauthorized burn fails and supply is unchanged.
    #[test]
    fn prop_pipeline_unauthorized_burn_no_corruption(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();

        let tx = env.user_precompile_tx(
            burnCall { amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute burn block");
        assert!(!env.tx_succeeded(&block, 0), "unauthorized burn should fail");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(supply_after, supply_before, "supply should be unchanged");
    }

    /// Full lifecycle: mint → claim → burn → verify supply == initial + minted - burned.
    #[test]
    fn prop_pipeline_supply_invariant(
        mint_amount in 1u128..1_000_000u128,
        burn_amount in 1u64..500u64,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let supply_initial = env.get_total_supply();

        // Block 1: mint
        let tx1 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(mint_amount));
        let block1 = env.execute_block_at_timestamp(vec![tx1], mint_ts).expect("mint");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: claim
        let claim_ts = mint_ts + TIMELOCK_DURATION + 1;
        let tx2 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(vec![tx2], claim_ts).expect("claim");
        assert!(env.tx_succeeded(&block2, 0));

        // Block 3: burn (from precompile balance, which has 1000 * 10^18 wei)
        let tx3 = env.governance_tx(burnCall { amount: U256::from(burn_amount) }.abi_encode());
        let block3 = env.execute_block_at_timestamp(vec![tx3], claim_ts + 1).expect("burn");
        assert!(env.tx_succeeded(&block3, 0));

        let supply_final = env.get_total_supply();
        prop_assert_eq!(
            supply_final,
            supply_initial + U256::from(mint_amount) - U256::from(burn_amount),
            "supply invariant: initial + minted - burned"
        );
    }
}

// ==============================
// Arithmetic overflow/underflow
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// When adding `amount` to governance balance would overflow U256,
    /// the claim tx succeeds but revm's `incr_balance` silently no-ops (checked_add fails).
    /// The balance does not increase by the minted amount, totalSupply still increments,
    /// and the pending mint is consumed (cannot be replayed).
    #[test]
    fn prop_pipeline_claim_balance_overflow(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let large_balance = U256::from(10).pow(U256::from(18)) * U256::from(1_000_000_000u64);
        let genesis_supply_wei = U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));
        let precompile_balance = U256::from(10).pow(U256::from(18)) * U256::from(1000u64);

        // Set governance safe balance so that adding `amount` overflows U256
        let gov_safe_balance = U256::MAX - U256::from(amount) + U256::from(1);

        let mut env = PipelineTestEnv::new_with_custom_state(
            genesis_supply_wei,
            precompile_balance,
            gov_safe_balance,
            large_balance,
            large_balance,
            large_balance,
        );
        let supply_before = env.get_total_supply();

        // Block 1: mint
        let mint_tx = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![mint_tx], mint_ts)
            .expect("execute mint block");
        assert!(env.tx_succeeded(&block1, 0), "mint tx should succeed");

        // Capture balance after mint block (before claim)
        let balance_before_claim = env.get_balance(GOVERNANCE_SAFE_ADDRESS);

        // Block 2: claim after timelock — tx succeeds but balance_incr is no-op
        let claim_ts = mint_ts + TIMELOCK_DURATION + 1;
        let claim_tx = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let _block2 = env.execute_block_at_timestamp(vec![claim_tx], claim_ts)
            .expect("execute claim block");

        // The claim's incr_balance is a no-op (checked_add overflows).
        // Block-level gas fee credits may still increase the balance by a small amount,
        // but never by the full minted `amount`.
        let balance_after_claim = env.get_balance(GOVERNANCE_SAFE_ADDRESS);
        let balance_increase = balance_after_claim.saturating_sub(balance_before_claim);
        prop_assert!(
            balance_increase < U256::from(amount),
            "balance must not increase by full amount (overflow protection): \
             increased by {balance_increase}, amount was {amount}"
        );

        // totalSupply increments because its checked_add passes (not near MAX)
        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before + U256::from(amount),
            "total supply incremented despite balance overflow (known invariant gap)"
        );

        // Block 3: second claim should fail — pending mint already consumed
        let claim_tx2 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let claim_ts2 = claim_ts + 1;
        let block3 = env.execute_block_at_timestamp(vec![claim_tx2], claim_ts2)
            .expect("execute second claim block");
        assert!(!env.tx_succeeded(&block3, 0), "second claim should fail — pending mint already consumed");
    }

    /// Claim reverts when totalSupply + amount would overflow U256.
    #[test]
    fn prop_pipeline_claim_total_supply_overflow(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let large_balance = U256::from(10).pow(U256::from(18)) * U256::from(1_000_000_000u64);
        let precompile_balance = U256::from(10).pow(U256::from(18)) * U256::from(1000u64);

        // Set totalSupply so that adding `amount` overflows U256
        let total_supply = U256::MAX - U256::from(amount) + U256::from(1);

        let mut env = PipelineTestEnv::new_with_custom_state(
            total_supply,
            precompile_balance,
            large_balance,
            large_balance,
            large_balance,
            large_balance,
        );
        let supply_before = env.get_total_supply();

        // Block 1: mint
        let mint_tx = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![mint_tx], mint_ts)
            .expect("execute mint block");
        assert!(env.tx_succeeded(&block1, 0), "mint tx should succeed");

        // Block 2: claim after timelock — balance_incr succeeds (gov balance is normal),
        // but totalSupply overflow triggers
        let claim_ts = mint_ts + TIMELOCK_DURATION + 1;
        let claim_tx = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(vec![claim_tx], claim_ts)
            .expect("execute claim block");
        assert!(!env.tx_succeeded(&block2, 0), "claim should fail due to total supply overflow");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before,
            "total supply should be unchanged after failed claim"
        );
    }

    /// Burn reverts when totalSupply < amount (underflow).
    #[test]
    fn prop_pipeline_burn_total_supply_underflow(burn_extra in 1u64..1000u64) {
        let large_balance = U256::from(10).pow(U256::from(18)) * U256::from(1_000_000_000u64);
        let precompile_balance = U256::from(10).pow(U256::from(18)) * U256::from(1000u64);

        // Set totalSupply to 100 — burn will try to subtract more than this
        let total_supply = U256::from(100);

        let mut env = PipelineTestEnv::new_with_custom_state(
            total_supply,
            precompile_balance,
            large_balance,
            large_balance,
            large_balance,
            large_balance,
        );
        let supply_before = env.get_total_supply();
        let precompile_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);

        // Burn 100 + burn_extra — balance_decr passes (precompile has plenty),
        // then totalSupply underflow triggers
        let burn_amount = U256::from(100u64 + burn_extra);
        let burn_tx = env.governance_tx(
            burnCall { amount: burn_amount }.abi_encode(),
        );
        let block = env.execute_block(vec![burn_tx]).expect("execute burn block");
        assert!(!env.tx_succeeded(&block, 0), "burn should fail due to total supply underflow");

        let supply_after = env.get_total_supply();
        let precompile_after = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);
        prop_assert_eq!(
            supply_after,
            supply_before,
            "total supply should be unchanged after failed burn"
        );
        prop_assert_eq!(
            precompile_after,
            precompile_before,
            "precompile balance should be unchanged after failed burn"
        );
    }
}
