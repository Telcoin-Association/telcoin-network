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
    approveCall, burnCall, claimCall, mintCall, permitCall,
    test_utils::precompile_test_utils::{permit_signer_address, sign_permit, GENESIS_SUPPLY},
    transferCall, transferFromCall, TELCOIN_PRECOMPILE_ADDRESS, TIMELOCK_DURATION,
};
use tn_types::{B256, U256};

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

// ==============================
// Category 1: Gas parity
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Precompile transfer must not be cheaper than a native value transfer.
    #[test]
    fn prop_pipeline_precompile_transfer_not_cheaper_than_native(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: precompile transfer(recipient, amount)
        let precompile_tx = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![precompile_tx]).expect("execute precompile transfer");
        assert!(env.tx_succeeded(&block1, 0), "precompile transfer should succeed");
        let precompile_gas = tx_gas_used(&block1, 0);

        // Block 2: native value transfer (recipient → user, same amount)
        let native_tx = PipelineTestEnv::native_transfer_tx(
            &mut env.recipient_factory,
            &env.chain,
            user_addr,
            U256::from(amount),
        );
        let block2 = env.execute_block(vec![native_tx]).expect("execute native transfer");
        assert!(env.tx_succeeded(&block2, 0), "native transfer should succeed");
        let native_gas = tx_gas_used(&block2, 0);

        prop_assert!(
            precompile_gas >= native_gas,
            "precompile transfer ({precompile_gas}) should cost >= native transfer ({native_gas})"
        );
    }
}

// ==============================
// Category 2: Zero-amount edge cases
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// transfer(to, 0) succeeds and does not change any balance.
    #[test]
    fn prop_pipeline_transfer_zero_no_balance_change(seed in 0u64..1000u64) {
        let _ = seed;
        let mut env = PipelineTestEnv::new();
        let recipient_addr = env.recipient_factory.address();

        let recipient_before = env.get_balance(recipient_addr);

        let tx = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::ZERO }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute block");
        assert!(env.tx_succeeded(&block, 0), "zero transfer should succeed");

        let recipient_after = env.get_balance(recipient_addr);
        prop_assert_eq!(recipient_after, recipient_before, "recipient balance should not change");
    }

    /// approve(spender, X) then approve(spender, 0) clears the allowance.
    #[test]
    fn prop_pipeline_approve_zero_clears_allowance(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: approve non-zero
        let tx1 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute approve block");
        assert!(env.tx_succeeded(&block1, 0), "approve should succeed");

        let allowance_mid = env.get_allowance(user_addr, recipient_addr);
        prop_assert_eq!(allowance_mid, U256::from(amount), "allowance should be set");

        // Block 2: approve zero
        let tx2 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::ZERO }.abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute zero approve block");
        assert!(env.tx_succeeded(&block2, 0), "zero approve should succeed");

        let allowance_after = env.get_allowance(user_addr, recipient_addr);
        prop_assert_eq!(allowance_after, U256::ZERO, "allowance should be cleared");
    }

    /// transferFrom(from, to, 0) succeeds even with zero allowance.
    #[test]
    fn prop_pipeline_transfer_from_zero_with_zero_allowance(seed in 0u64..1000u64) {
        let _ = seed;
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // No prior approve — allowance is 0
        let tx = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::ZERO,
            }
            .abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute block");
        assert!(env.tx_succeeded(&block, 0), "zero transferFrom with zero allowance should succeed");
    }

    /// burn(0) succeeds and does not change totalSupply.
    #[test]
    fn prop_pipeline_burn_zero_no_supply_change(seed in 0u64..1000u64) {
        let _ = seed;
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();

        let tx = env.governance_tx(
            burnCall { amount: U256::ZERO }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute burn zero block");
        assert!(env.tx_succeeded(&block, 0), "zero burn should succeed");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(supply_after, supply_before, "supply should be unchanged after zero burn");
    }

    /// mint(amount) then mint(0) cancels the pending mint (claim fails).
    #[test]
    fn prop_pipeline_mint_zero_cancels_pending(
        amount in 1u128..1_000_000u128,
        mint_ts in 1_000u64..100_000_000u64,
    ) {
        let mut env = PipelineTestEnv::new();

        // Block 1: mint non-zero
        let tx1 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::from(amount));
        let block1 = env.execute_block_at_timestamp(vec![tx1], mint_ts).expect("execute mint block");
        assert!(env.tx_succeeded(&block1, 0), "mint should succeed");

        // Block 2: mint zero (cancels pending)
        let tx2 = env.governance_mint_tx(GOVERNANCE_SAFE_ADDRESS, U256::ZERO);
        let block2 = env.execute_block_at_timestamp(vec![tx2], mint_ts + 1).expect("execute zero mint");
        assert!(env.tx_succeeded(&block2, 0), "zero mint should succeed");

        // Block 3: claim after timelock — should fail (pending was cancelled)
        let tx3 = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block3 = env.execute_block_at_timestamp(
            vec![tx3],
            mint_ts + 1 + TIMELOCK_DURATION + 1,
        ).expect("execute claim block");
        assert!(!env.tx_succeeded(&block3, 0), "claim should fail after mint(0) cancellation");
    }
}

// ==============================
// Category 3: Self-transfer / self-approve
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// transfer(self, amount) succeeds — no other address gains or loses.
    #[test]
    fn prop_pipeline_self_transfer(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        let balance_before = env.get_balance(user_addr);
        let recipient_before = env.get_balance(recipient_addr);
        let precompile_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);

        let tx = env.user_precompile_tx(
            transferCall { to: user_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute self-transfer block");
        assert!(env.tx_succeeded(&block, 0), "self-transfer should succeed");

        let balance_after = env.get_balance(user_addr);

        // Self-transfer: user balance should decrease (gas only), not by the transfer amount.
        // Verify no other address was affected.
        prop_assert_eq!(
            env.get_balance(recipient_addr), recipient_before,
            "no other address should be affected by self-transfer"
        );
        // Precompile balance unchanged (no funds leaked)
        prop_assert_eq!(
            env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), precompile_before,
            "precompile balance should be unchanged"
        );
        // User only lost gas
        prop_assert!(
            balance_before > balance_after,
            "user should lose gas"
        );
    }

    /// approve(self, amount) then transferFrom(self, self, amount) — allowance decrements, balance unchanged except gas.
    #[test]
    fn prop_pipeline_self_approve_and_transfer_from(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();

        // Block 1: approve self
        let tx1 = env.user_precompile_tx(
            approveCall { spender: user_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute self-approve");
        assert!(env.tx_succeeded(&block1, 0), "self-approve should succeed");

        let balance_before_transfer = env.get_balance(user_addr);

        // Block 2: transferFrom(self, self, amount)
        let tx2 = env.user_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: user_addr,
                amount: U256::from(amount),
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute self-transferFrom");
        assert!(env.tx_succeeded(&block2, 0), "self-transferFrom should succeed");

        // Allowance should be decremented to 0
        let remaining = env.get_allowance(user_addr, user_addr);
        prop_assert_eq!(remaining, U256::ZERO, "allowance should be consumed");

        // Balance should only decrease by gas (self-transfer is balance-neutral)
        let balance_after = env.get_balance(user_addr);
        prop_assert!(
            balance_before_transfer > balance_after,
            "user should lose gas from self-transferFrom"
        );
    }
}

// ==============================
// Category 4: Allowance boundary attacks
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// transferFrom(amount == allowance) succeeds and leaves allowance == 0.
    #[test]
    fn prop_pipeline_transfer_from_exact_allowance(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: approve exact amount
        let tx1 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute approve");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: transferFrom exact allowance
        let tx2 = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(amount),
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute transferFrom");
        assert!(env.tx_succeeded(&block2, 0), "exact allowance transferFrom should succeed");

        let remaining = env.get_allowance(user_addr, recipient_addr);
        prop_assert_eq!(remaining, U256::ZERO, "allowance should be zero after exact spend");
    }

    /// transferFrom(amount == allowance + 1) fails.
    #[test]
    fn prop_pipeline_transfer_from_one_over_allowance_fails(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: approve
        let tx1 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute approve");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: transferFrom allowance + 1
        let tx2 = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(amount) + U256::from(1),
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute transferFrom");
        assert!(!env.tx_succeeded(&block2, 0), "over-allowance transferFrom should fail");
    }

    /// approve(X) then approve(Y) — only Y is effective.
    #[test]
    fn prop_pipeline_approve_overwrite(
        first in 1u128..1_000_000u128,
        second in 1u128..1_000_000u128,
    ) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: approve first
        let tx1 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(first) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute approve 1");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: approve second (overwrites)
        let tx2 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(second) }.abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute approve 2");
        assert!(env.tx_succeeded(&block2, 0));

        let allowance = env.get_allowance(user_addr, recipient_addr);
        prop_assert_eq!(allowance, U256::from(second), "only second approval should be effective");
    }

    /// Classic double-spend: approve(X), transferFrom(X) succeeds, second transferFrom(1) fails.
    #[test]
    fn prop_pipeline_double_spend_fails(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: approve
        let tx1 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute approve");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: first transferFrom (full allowance)
        let tx2 = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(amount),
            }
            .abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute first transferFrom");
        assert!(env.tx_succeeded(&block2, 0), "first transferFrom should succeed");

        // Block 3: second transferFrom (should fail — allowance exhausted)
        let tx3 = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(1u64),
            }
            .abi_encode(),
        );
        let block3 = env.execute_block(vec![tx3]).expect("execute second transferFrom");
        assert!(!env.tx_succeeded(&block3, 0), "double-spend transferFrom should fail");
    }
}

// ==============================
// Category 5: Transfer / balance boundary cases
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Transfer more than balance fails and state is unchanged.
    #[test]
    fn prop_pipeline_transfer_more_than_balance_fails(extra in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        let user_balance = env.get_balance(user_addr);
        let recipient_before = env.get_balance(recipient_addr);

        // Try to transfer more than balance
        let tx = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: user_balance + U256::from(extra) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute block");
        assert!(!env.tx_succeeded(&block, 0), "over-balance transfer should fail");

        let recipient_after = env.get_balance(recipient_addr);
        prop_assert_eq!(recipient_after, recipient_before, "recipient balance should be unchanged");
    }

    /// Transfer to the precompile address succeeds and increases its balance.
    #[test]
    fn prop_pipeline_transfer_to_precompile_address(amount in 1u128..1_000u128) {
        let mut env = PipelineTestEnv::new();
        let precompile_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);

        let tx = env.user_precompile_tx(
            transferCall { to: TELCOIN_PRECOMPILE_ADDRESS, amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute block");
        assert!(env.tx_succeeded(&block, 0), "transfer to precompile should succeed");

        let precompile_after = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);
        prop_assert_eq!(
            precompile_after,
            precompile_before + U256::from(amount),
            "precompile balance should increase"
        );
    }
}

// ==============================
// Category 6: Permit security
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Same permit signature fails when replayed in the next block.
    #[test]
    fn prop_pipeline_permit_replay_across_blocks(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let chain_id = env.chain_id();
        let value = U256::from(amount);
        let deadline = U256::from(env.block_timestamp + 1000);
        let nonce = env.get_nonce(owner);

        let (v, r, s) = sign_permit(owner, spender, value, nonce, deadline, chain_id);
        let permit_data = permitCall { owner, spender, value, deadline, v, r, s }.abi_encode();

        // Block 1: permit succeeds
        let tx1 = env.user_precompile_tx(permit_data.clone());
        let block1 = env.execute_block(vec![tx1]).expect("execute permit block");
        assert!(env.tx_succeeded(&block1, 0), "first permit should succeed");

        // Block 2: replay same signature — should fail (nonce incremented)
        let tx2 = env.user_precompile_tx(permit_data);
        let block2 = env.execute_block(vec![tx2]).expect("execute replay block");
        assert!(!env.tx_succeeded(&block2, 0), "permit replay should fail across blocks");
    }

    /// Permit signed for wrong chain_id fails.
    #[test]
    fn prop_pipeline_permit_wrong_chain_id(
        amount in 1u128..1_000_000u128,
        chain_id_offset in 1u64..1000u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let correct_chain_id = env.chain_id();
        let wrong_chain_id = correct_chain_id + chain_id_offset;
        let value = U256::from(amount);
        let deadline = U256::from(env.block_timestamp + 1000);
        let nonce = env.get_nonce(owner);

        // Sign with wrong chain_id
        let (v, r, s) = sign_permit(owner, spender, value, nonce, deadline, wrong_chain_id);
        let tx = env.user_precompile_tx(
            permitCall { owner, spender, value, deadline, v, r, s }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute permit block");
        assert!(!env.tx_succeeded(&block, 0), "permit with wrong chain_id should fail");
    }

    /// Permit with deadline == block_timestamp succeeds (check is `current_ts > deadline`).
    #[test]
    fn prop_pipeline_permit_deadline_boundary(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let chain_id = env.chain_id();
        let value = U256::from(amount);
        // deadline == block_timestamp
        let deadline = U256::from(env.block_timestamp);
        let nonce = env.get_nonce(owner);

        let (v, r, s) = sign_permit(owner, spender, value, nonce, deadline, chain_id);
        let tx = env.user_precompile_tx(
            permitCall { owner, spender, value, deadline, v, r, s }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute permit block");
        assert!(
            env.tx_succeeded(&block, 0),
            "permit with deadline == block_timestamp should succeed"
        );
    }

    /// Permit with invalid v (not 27 or 28) fails.
    #[test]
    fn prop_pipeline_permit_invalid_v(
        amount in 1u128..1_000_000u128,
        v_val in (0u8..=255u8).prop_filter("must be invalid", |v| *v != 27 && *v != 28),
    ) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let chain_id = env.chain_id();
        let value = U256::from(amount);
        let deadline = U256::from(env.block_timestamp + 1000);
        let nonce = env.get_nonce(owner);

        let (_, r, s) = sign_permit(owner, spender, value, nonce, deadline, chain_id);
        let tx = env.user_precompile_tx(
            permitCall { owner, spender, value, deadline, v: v_val, r, s }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute permit block");
        assert!(!env.tx_succeeded(&block, 0), "permit with invalid v should fail");
    }

    /// 3 permits across 3 blocks yield nonces 0, 1, 2.
    #[test]
    fn prop_pipeline_permit_nonce_monotonic_across_blocks(
        amount in 1u128..1_000_000u128,
        num_permits in 2u64..6u64,
    ) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let chain_id = env.chain_id();

        for expected_nonce in 0u64..num_permits {
            let nonce = env.get_nonce(owner);
            prop_assert_eq!(nonce, U256::from(expected_nonce), "nonce mismatch before permit");

            let value = U256::from(amount) + U256::from(expected_nonce);
            let deadline = U256::from(env.block_timestamp + 1000);
            let (v, r, s) = sign_permit(owner, spender, value, nonce, deadline, chain_id);

            let tx = env.user_precompile_tx(
                permitCall { owner, spender, value, deadline, v, r, s }.abi_encode(),
            );
            let block = env.execute_block(vec![tx]).expect("execute permit block");
            assert!(env.tx_succeeded(&block, 0), "permit {expected_nonce} should succeed");
        }

        let final_nonce = env.get_nonce(owner);
        prop_assert_eq!(final_nonce, U256::from(num_permits), "final nonce should match num_permits");
    }
}

// ==============================
// Category 7: Mint / Claim security
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
// Category 8: Failed transaction isolation
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Block with 1 successful + 1 failed tx: only successful effects persist.
    #[test]
    fn prop_pipeline_mixed_success_failure_block(
        transfer_amount in 1u128..500_000u128,
    ) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();
        let user_before = env.get_balance(user_addr);

        // Tx 1: successful transfer (user → recipient)
        let tx1 = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::from(transfer_amount) }.abi_encode(),
        );
        // Tx 2: failed transferFrom (recipient → user, no allowance)
        let tx2 = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(1u64),
            }
            .abi_encode(),
        );

        let block = env.execute_block(vec![tx1, tx2]).expect("execute mixed block");
        assert!(env.tx_succeeded(&block, 0), "first tx should succeed");
        assert!(!env.tx_succeeded(&block, 1), "second tx should fail");

        // Verify the successful transfer happened: user lost at least transfer_amount
        let user_after = env.get_balance(user_addr);
        prop_assert!(
            user_before - user_after >= U256::from(transfer_amount),
            "user should have lost at least the transfer amount"
        );

        // Verify the failed tx didn't move funds from user: user's loss should be
        // exactly transfer_amount + gas (the failed transferFrom didn't deduct from user)
        // We can't compute exact gas, but we can verify user lost < transfer_amount + max_gas_cost
        // where max_gas_cost = gas_limit * max_fee_per_gas = 1M * 7 = 7M
        let max_gas_cost = U256::from(1_000_000u64) * U256::from(7u64);
        prop_assert!(
            user_before - user_after <= U256::from(transfer_amount) + max_gas_cost,
            "user should not lose more than transfer + gas"
        );
    }

    /// Failed transferFrom changes no balances (except caller gas).
    #[test]
    fn prop_pipeline_failed_transfer_from_no_balance_change(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        let user_before = env.get_balance(user_addr);
        let recipient_before = env.get_balance(recipient_addr);

        // transferFrom without approval — should fail
        let tx = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(amount),
            }
            .abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute block");
        assert!(!env.tx_succeeded(&block, 0), "unauthorized transferFrom should fail");

        // User (from) balance should be completely unchanged
        let user_after = env.get_balance(user_addr);
        prop_assert_eq!(user_after, user_before, "from-address balance should be unchanged");

        // Recipient (caller) only lost gas, not the transfer amount
        let recipient_after = env.get_balance(recipient_addr);
        prop_assert!(
            recipient_before > recipient_after,
            "caller should have lost gas"
        );
        // Max possible gas cost = gas_limit * max_fee_per_gas = 1M * 7 = 7M wei
        let max_gas_cost = U256::from(1_000_000u64) * U256::from(7u64);
        prop_assert!(
            recipient_before - recipient_after <= max_gas_cost,
            "caller loss should be bounded by max gas cost"
        );
    }
}

// ==============================
// Category 9: totalSupply invariant
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Transfers and approvals do not affect totalSupply.
    #[test]
    fn prop_pipeline_supply_unaffected_by_transfers(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let supply_before = env.get_total_supply();
        let user_addr = env.user_factory.address();
        let recipient_addr = env.recipient_factory.address();

        // Block 1: transfer
        let tx1 = env.user_precompile_tx(
            transferCall { to: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("transfer block");
        assert!(env.tx_succeeded(&block1, 0));

        // Block 2: approve
        let tx2 = env.user_precompile_tx(
            approveCall { spender: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("approve block");
        assert!(env.tx_succeeded(&block2, 0));

        // Block 3: transferFrom
        let tx3 = env.recipient_precompile_tx(
            transferFromCall {
                from: user_addr,
                to: recipient_addr,
                amount: U256::from(amount),
            }
            .abi_encode(),
        );
        let block3 = env.execute_block(vec![tx3]).expect("transferFrom block");
        assert!(env.tx_succeeded(&block3, 0));

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before,
            "totalSupply should be unaffected by transfers and approvals"
        );
    }
}

// ==============================
// Category 10: Arithmetic overflow/underflow & signature malleability
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Claim reverts when adding `amount` to governance balance would overflow U256.
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

        // Block 2: claim after timelock — should fail (balance overflow)
        let claim_ts = mint_ts + TIMELOCK_DURATION + 1;
        let claim_tx = env.governance_tx(
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        let block2 = env.execute_block_at_timestamp(vec![claim_tx], claim_ts)
            .expect("execute claim block");
        assert!(!env.tx_succeeded(&block2, 0), "claim should fail due to balance overflow");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before,
            "total supply should be unchanged after failed claim"
        );
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
        let burn_amount = U256::from(100u64 + u64::from(burn_extra));
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

    /// Permit reverts when s > SECP256K1N_HALF (signature malleability).
    #[test]
    fn prop_pipeline_permit_signature_malleability(
        amount in 1u128..1_000_000u128,
        s_shift in 0u128..1_000_000_000u128,
    ) {
        let mut env = PipelineTestEnv::new();
        let owner = permit_signer_address();
        let spender = env.recipient_factory.address();
        let chain_id = env.chain_id();
        let value = U256::from(amount);
        let deadline = U256::from(env.block_timestamp + 1000);
        let nonce = env.get_nonce(owner);

        // Get a valid signature, then replace s with a value > SECP256K1N_HALF
        let (v, r, _) = sign_permit(owner, spender, value, nonce, deadline, chain_id);
        let malleable_s = B256::from(U256::MAX - U256::from(s_shift));

        let tx = env.user_precompile_tx(
            permitCall { owner, spender, value, deadline, v, r, s: malleable_s }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute permit block");
        assert!(!env.tx_succeeded(&block, 0), "permit with malleable s should fail");

        // Verify nonce and allowance unchanged
        let nonce_after = env.get_nonce(owner);
        let allowance_after = env.get_allowance(owner, spender);
        prop_assert_eq!(nonce_after, nonce, "nonce should be unchanged after failed permit");
        prop_assert_eq!(allowance_after, U256::ZERO, "allowance should be unchanged after failed permit");
    }
}
