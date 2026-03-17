//! Full-pipeline property-based tests for the TEL precompile (production mode).
//!
//! These tests exercise the precompile through the complete TN execution pipeline:
//! tx signing → EIP-2718 encoding → signature recovery → block building →
//! EVM dispatch → precompile execution → state persistence → state reads.

use alloy::sol_types::SolCall;
use proptest::prelude::*;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_reth::{
    approveCall, burnCall, claimCall, mintCall, permitCall, transferCall, transferFromCall,
    TELCOIN_PRECOMPILE_ADDRESS, TIMELOCK_DURATION,
};
use tn_types::U256;

use super::pipeline_helpers::*;
use super::tel_precompile_helpers::{permit_signer_address, sign_permit};

// ==============================
// ERC-20 transfer properties
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Transfer via full pipeline conserves balance (minus gas).
    #[test]
    fn prop_pipeline_transfer_conserves_balance(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        let user_before = env.get_balance(user_addr);
        let recipient_before = env.get_balance(recipient_addr);
        let user_before_precompile = env.get_precompile_balance(user_addr);
        let recipient_before_precompile = env.get_precompile_balance(recipient_addr);
        // native balance check matches precompile method
        assert_eq!(user_before, user_before_precompile);
        assert_eq!(recipient_before, recipient_before_precompile);

        let tx = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute block");
        assert!(env.tx_succeeded(&block, 0), "transfer tx should succeed");

        let user_after = env.get_balance(user_addr);
        let recipient_after = env.get_balance(recipient_addr);

        // Recipient gained exactly `amount`
        prop_assert_eq!(
            recipient_after,
            recipient_before + U256::from(amount),
            "recipient balance mismatch"
        );
        // User lost at least `amount` (gas consumes additional)
        prop_assert!(
            user_before - user_after >= U256::from(amount),
            "user should lose at least transfer amount"
        );
    }
}

// ==============================
// Approve / transferFrom properties
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Approve then transferFrom decrements allowance correctly.
    #[test]
    fn prop_pipeline_approve_then_transfer_from(
        allowance_val in 1u128..1_000_000u128,
        pct in 1u64..=100u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();
        let transfer_amt = U256::from(allowance_val) * U256::from(pct) / U256::from(100);
        if transfer_amt.is_zero() {
            return Ok(());
        }

        // Block 1: user approves recipient as spender
        let approve_tx = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(allowance_val) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![approve_tx]).expect("execute approve block");
        assert!(env.tx_succeeded(&block1, 0), "approve tx should succeed");

        let user_before = env.get_balance(user_addr);

        // Block 2: recipient calls transferFrom(user → recipient)
        // (recipient is both spender and destination, which is valid)
        let transfer_from_tx = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: transfer_amt,
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![transfer_from_tx]).expect("execute transferFrom block");
        assert!(env.tx_succeeded(&block2, 0), "transferFrom tx should succeed");

        // Check allowance decremented
        let remaining = env.get_allowance(user_addr, recipient_addr);
        prop_assert_eq!(
            remaining,
            U256::from(allowance_val) - transfer_amt,
            "allowance not decremented correctly"
        );

        // Check user balance decreased by transfer amount (no gas since recipient paid)
        let user_after = env.get_balance(user_addr);
        prop_assert_eq!(
            user_after,
            user_before - transfer_amt,
            "user balance should decrease by transferred amount"
        );
    }

    /// Infinite (U256::MAX) allowance is not decremented by transferFrom.
    #[test]
    fn prop_pipeline_infinite_allowance_not_decremented(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: approve U256::MAX
        let approve_tx = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::MAX }.abi_encode(),
        );
        let block1 = env.execute_block(vec![approve_tx]).expect("execute approve block");
        assert!(env.tx_succeeded(&block1, 0), "approve tx should succeed");

        // Block 2: transferFrom
        let transfer_from_tx = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: GOVERNANCE_SAFE_ADDRESS,
                amount: U256::from(amount),
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![transfer_from_tx]).expect("execute transferFrom block");
        assert!(env.tx_succeeded(&block2, 0), "transferFrom tx should succeed");

        let remaining = env.get_allowance(user_addr, recipient_addr);
        prop_assert_eq!(
            remaining,
            U256::MAX,
            "infinite allowance should not be decremented"
        );
    }
}

// ==============================
// Mint / Claim / Burn properties
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Governance mint → wait for timelock → claim succeeds and increases supply.
    #[test]
    fn prop_pipeline_mint_timelock_claim(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();

        // Block 1 at T: governance mints via forwarder
        let mint_ts = 1_000_000u64;
        let mint_tx = env.governance_tx(
            mintCall { amount: U256::from(amount) }.abi_encode(),
        );
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
    fn prop_pipeline_burn_decrements_supply(amount in 1u64..1000u64) {
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

    /// Multiple transfers in the same block all apply correctly.
    #[test]
    fn prop_pipeline_multi_tx_block(
        a1 in 1u128..500_000u128,
        a2 in 1u128..500_000u128,
    ) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        let user_before = env.get_balance(user_addr);
        let recipient_before = env.get_balance(recipient_addr);

        // Two transfers in the same block: both user→recipient
        let tx1 = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::from(a1) }.abi_encode(),
        );
        let tx2 = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::from(a2) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx1, tx2]).expect("execute multi-tx block");
        assert!(env.tx_succeeded(&block, 0), "first transfer should succeed");
        assert!(env.tx_succeeded(&block, 1), "second transfer should succeed");

        let user_after = env.get_balance(user_addr);
        let recipient_after = env.get_balance(recipient_addr);

        // Recipient gained exactly a1 + a2
        prop_assert_eq!(
            recipient_after,
            recipient_before + U256::from(a1) + U256::from(a2),
            "recipient balance mismatch"
        );
        // User lost at least a1 + a2 (gas also deducted)
        prop_assert!(
            user_before - user_after >= U256::from(a1) + U256::from(a2),
            "user should lose at least total transfer amount"
        );
    }

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
// EIP-2612 Permit pipeline test
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Full pipeline: sign permit off-chain, submit permit tx, then transferFrom in next block.
    #[test]
    fn prop_pipeline_permit_then_transfer_from(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let chain_id = env.chain_id();
        let value = U256::from(amount);
        let deadline = U256::from(env.block_timestamp + 1000);

        // Get current nonce
        let nonce = env.get_nonce(owner);

        // Sign permit off-chain
        let (v, r, s) = sign_permit(owner, spender, value, nonce, deadline, chain_id);

        // Block 1: anyone submits the permit tx (user pays gas)
        let permit_tx = env.user_precompile_tx(
            permitCall { owner, spender, value, deadline, v, r, s }.abi_encode(),
        );
        let block1 = env.execute_block(vec![permit_tx]).expect("execute permit block");
        assert!(env.tx_succeeded(&block1, 0), "permit tx should succeed");

        // Verify allowance was set
        let allowance = env.get_allowance(owner, spender);
        prop_assert_eq!(allowance, value, "allowance should match permit value");

        // Verify nonce incremented
        let new_nonce = env.get_nonce(owner);
        prop_assert_eq!(new_nonce, nonce + U256::from(1), "nonce should be incremented");

        // Block 2: spender (recipient) calls transferFrom(owner → spender)
        let owner_before = env.get_balance(owner);
        let transfer_from_tx = env.recipient_precompile_tx(
            transferFromCall {
                from: owner,
                to: spender,
                amount: value,
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![transfer_from_tx]).expect("execute transferFrom block");
        assert!(env.tx_succeeded(&block2, 0), "transferFrom tx should succeed");

        // Verify owner balance decreased
        let owner_after = env.get_balance(owner);
        prop_assert_eq!(
            owner_after,
            owner_before - value,
            "owner balance should decrease by transfer amount"
        );

        // Verify allowance consumed
        let remaining = env.get_allowance(owner, spender);
        prop_assert_eq!(remaining, U256::ZERO, "allowance should be consumed");
    }
}
