//! Property-based tests for the Telcoin ERC-20 precompile (production mode).
//!
//! These tests verify critical invariants across randomized inputs:
//! - ERC-20 transfer balance conservation
//! - Approve/transferFrom allowance semantics
//! - Mint/claim/burn supply invariants
//! - Calldata validation (short/unknown selectors)

use alloy::sol_types::SolCall;
use proptest::prelude::*;
use tn_reth::{
    allowanceCall, approveCall, balanceOfCall, burnCall, claimCall, decimalsCall,
    grantMintRoleCall, hasMintRoleCall, mintCall, nameCall, revokeMintRoleCall, symbolCall,
    totalSupplyCall, transferCall, transferFromCall,
};
use tn_types::{Address, U256};

use super::tel_precompile_helpers::*;

/// Timelock duration: 7 days in seconds.
const TIMELOCK_DURATION: u64 = 7 * 24 * 60 * 60;

// ==============================
// ERC-20 transfer properties
// ==============================

proptest! {
    /// Transferring `amount` conserves total balance between sender and recipient.
    #[test]
    fn prop_transfer_conserves_balance(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();

        let sender_before = env.get_balance(USER);
        let recipient_before = env.get_balance(RECIPIENT);

        let result = env.exec_default(
            USER,
            transferCall { to: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let sender_after = env.get_balance(USER);
        let recipient_after = env.get_balance(RECIPIENT);

        // sender lost exactly `amount`
        prop_assert_eq!(
            sender_before - U256::from(amount), sender_after,
            "sender balance mismatch: before={}, after={}, amount={}",
            sender_before, sender_after, amount
        );
        // recipient gained exactly `amount`
        prop_assert_eq!(
            recipient_before + U256::from(amount), recipient_after,
            "recipient balance mismatch: before={}, after={}, amount={}",
            recipient_before, recipient_after, amount
        );
    }

    /// Transferring more than balance always fails.
    #[test]
    fn prop_transfer_over_balance_fails(excess in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let over = U256::from(10).pow(U256::from(18)) + U256::from(excess);
        let result = env.exec_default(
            USER,
            transferCall { to: RECIPIENT, amount: over }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Transferring to address(0) always fails.
    #[test]
    fn prop_transfer_to_zero_fails(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            transferCall { to: Address::ZERO, amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }
}

// ==============================
// ERC-20 approve/transferFrom properties
// ==============================

proptest! {
    /// After approve, allowance equals the approved amount exactly.
    #[test]
    fn prop_approve_sets_exact_allowance(amount in 0u128..=u128::MAX) {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            approveCall { spender: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let allowance = env.get_allowance(USER, RECIPIENT);
        prop_assert_eq!(
            allowance, U256::from(amount),
            "allowance mismatch: expected {}, got {}",
            amount, allowance
        );
    }

    /// A second approve overwrites the first.
    #[test]
    fn prop_approve_overwrites(
        a1 in 0u128..1_000_000_000_000_000_000u128,
        a2 in 0u128..1_000_000_000_000_000_000u128
    ) {
        let mut env = TestEnv::new();
        env.exec_default(
            USER,
            approveCall { spender: RECIPIENT, amount: U256::from(a1) }.abi_encode(),
        ).unwrap();
        let result = env.exec_default(
            USER,
            approveCall { spender: RECIPIENT, amount: U256::from(a2) }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let allowance = env.get_allowance(USER, RECIPIENT);
        prop_assert_eq!(
            allowance, U256::from(a2),
            "allowance should be overwritten to {}, got {}",
            a2, allowance
        );
    }

    /// transferFrom succeeds when amount <= allowance and decrements allowance.
    #[test]
    fn prop_transfer_from_respects_allowance(
        allowance_val in 1u128..1_000_000_000_000_000_000u128,
        transfer_pct in 1u64..=100u64,
    ) {
        let mut env = TestEnv::new();
        let transfer_amt = U256::from(allowance_val) * U256::from(transfer_pct) / U256::from(100);
        if transfer_amt.is_zero() {
            return Ok(());
        }

        // USER approves RECIPIENT to spend from USER
        env.exec_default(
            USER,
            approveCall { spender: RECIPIENT, amount: U256::from(allowance_val) }.abi_encode(),
        ).unwrap();

        // RECIPIENT calls transferFrom(USER -> GOVERNANCE)
        let result = env.exec_default(
            RECIPIENT,
            transferFromCall { from: USER, to: GOVERNANCE, amount: transfer_amt }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let remaining = env.get_allowance(USER, RECIPIENT);
        prop_assert_eq!(
            remaining, U256::from(allowance_val) - transfer_amt,
            "allowance not decremented correctly"
        );
    }

    /// transferFrom fails when amount > allowance.
    #[test]
    fn prop_transfer_from_exceeds_allowance_fails(
        allowance_val in 0u128..1_000_000_000_000_000_000u128,
        excess in 1u128..1_000_000_000_000_000_000u128,
    ) {
        let mut env = TestEnv::new();
        env.exec_default(
            USER,
            approveCall { spender: RECIPIENT, amount: U256::from(allowance_val) }.abi_encode(),
        ).unwrap();

        let over = U256::from(allowance_val) + U256::from(excess);
        let result = env.exec_default(
            RECIPIENT,
            transferFromCall { from: USER, to: GOVERNANCE, amount: over }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Infinite (U256::MAX) allowance is not decremented by transferFrom.
    #[test]
    fn prop_infinite_allowance_not_decremented(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        // Approve U256::MAX
        env.exec_default(
            USER,
            approveCall { spender: RECIPIENT, amount: U256::MAX }.abi_encode(),
        ).unwrap();

        let result = env.exec_default(
            RECIPIENT,
            transferFromCall { from: USER, to: GOVERNANCE, amount: U256::from(amount) }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let remaining = env.get_allowance(USER, RECIPIENT);
        prop_assert_eq!(
            remaining, U256::MAX,
            "infinite allowance should not be decremented"
        );
    }
}

// ==============================
// Mint/Claim/Burn properties
// ==============================

proptest! {
    /// After mint + timelock + claim, totalSupply increases by exactly the minted amount.
    #[test]
    fn prop_mint_claim_supply_invariant(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let supply_before = env.get_total_supply();

        env.exec_default(GOVERNANCE, mintCall { amount: U256::from(amount) }.abi_encode()).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(GOVERNANCE, claimCall { recipient: GOVERNANCE }.abi_encode());
        assert_success(&result);

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after, supply_before + U256::from(amount),
            "supply should increase by exactly {}", amount
        );
    }

    /// Claim before timelock expires always fails.
    #[test]
    fn prop_claim_before_timelock_fails(
        amount in 1u128..1_000_000_000_000_000_000u128,
        offset in 0u64..TIMELOCK_DURATION
    ) {
        let mut env = TestEnv::new();
        env.exec_default(GOVERNANCE, mintCall { amount: U256::from(amount) }.abi_encode()).unwrap();
        env.set_timestamp(1000 + offset);
        let result = env.exec_default(GOVERNANCE, claimCall { recipient: GOVERNANCE }.abi_encode());
        assert_not_success(&result);
    }

    /// A second mint overwrites the first; only the second amount is claimable.
    #[test]
    fn prop_mint_overwrites_pending(
        a1 in 1u128..1_000_000_000_000_000_000u128,
        a2 in 1u128..1_000_000_000_000_000_000u128,
    ) {
        let mut env = TestEnv::new();
        let supply_before = env.get_total_supply();

        env.exec_default(GOVERNANCE, mintCall { amount: U256::from(a1) }.abi_encode()).unwrap();
        env.exec_default(GOVERNANCE, mintCall { amount: U256::from(a2) }.abi_encode()).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(GOVERNANCE, claimCall { recipient: GOVERNANCE }.abi_encode());
        assert_success(&result);

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after, supply_before + U256::from(a2),
            "only second mint amount should be credited"
        );
    }

    /// mint(amount) then mint(0) cancels the pending mint; claim fails.
    #[test]
    fn prop_zero_mint_cancels_pending(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        env.exec_default(GOVERNANCE, mintCall { amount: U256::from(amount) }.abi_encode()).unwrap();
        env.exec_default(GOVERNANCE, mintCall { amount: U256::ZERO }.abi_encode()).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(GOVERNANCE, claimCall { recipient: GOVERNANCE }.abi_encode());
        assert_not_success(&result);
    }

    /// After burn, totalSupply decreases by exactly the burned amount.
    #[test]
    fn prop_burn_supply_invariant(amount in 1u64..1000u64) {
        let mut env = TestEnv::new();
        let supply_before = env.get_total_supply();

        let result = env.exec_default(
            GOVERNANCE,
            burnCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_success(&result);

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after, supply_before - U256::from(amount),
            "supply should decrease by exactly {}", amount
        );
    }

    /// Non-governance caller cannot mint.
    #[test]
    fn prop_unauthorized_mint_fails(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            mintCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Non-governance caller cannot burn.
    #[test]
    fn prop_unauthorized_burn_fails(amount in 1u64..1000u64) {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            burnCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }
}

// ==============================
// Calldata validation properties
// ==============================

/// Known selectors for the precompile.
fn known_selectors() -> Vec<[u8; 4]> {
    vec![
        transferCall::SELECTOR,
        approveCall::SELECTOR,
        transferFromCall::SELECTOR,
        balanceOfCall::SELECTOR,
        totalSupplyCall::SELECTOR,
        allowanceCall::SELECTOR,
        claimCall::SELECTOR,
        burnCall::SELECTOR,
        nameCall::SELECTOR,
        symbolCall::SELECTOR,
        decimalsCall::SELECTOR,
        mintCall::SELECTOR,
    ]
}

proptest! {
    /// Truncated calldata (valid selector but too short) always fails.
    #[test]
    fn prop_short_calldata_fails(
        selector_idx in 0usize..7, // transfer, approve, transferFrom, balanceOf, allowance, claim, burn
        extra_len in 0usize..31,   // less than 32 bytes of args
    ) {
        let selectors_with_min_args: Vec<([u8; 4], usize)> = vec![
            (transferCall::SELECTOR, 64),
            (approveCall::SELECTOR, 64),
            (transferFromCall::SELECTOR, 96),
            (balanceOfCall::SELECTOR, 32),
            (allowanceCall::SELECTOR, 64),
            (claimCall::SELECTOR, 32),
            (burnCall::SELECTOR, 32),
        ];
        let (selector, min_args) = selectors_with_min_args[selector_idx];
        // Only test truncated calldata (less than minimum required)
        let truncated_len = extra_len.min(min_args - 1);
        let mut data = Vec::with_capacity(4 + truncated_len);
        data.extend_from_slice(&selector);
        data.extend(std::iter::repeat(0u8).take(truncated_len));

        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE, data);
        assert_not_success(&result);
    }

    /// Unknown function selectors always fail.
    #[test]
    fn prop_unknown_selector_fails(selector_val in 0u32..u32::MAX) {
        let selector_bytes = selector_val.to_be_bytes();
        let known = known_selectors();
        // Skip known selectors
        prop_assume!(!known.iter().any(|s| *s == selector_bytes));
        // Also skip selectors that match faucet-only functions
        prop_assume!(selector_bytes != grantMintRoleCall::SELECTOR);
        prop_assume!(selector_bytes != revokeMintRoleCall::SELECTOR);
        prop_assume!(selector_bytes != hasMintRoleCall::SELECTOR);

        let mut data = Vec::with_capacity(36);
        data.extend_from_slice(&selector_bytes);
        data.extend_from_slice(&[0u8; 32]);

        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE, data);
        assert_not_success(&result);
    }
}

// ==============================
// Deterministic sanity tests
// ==============================

#[test]
fn test_name_symbol_decimals_are_constant() {
    let mut env = TestEnv::new();

    // name
    let result = env.exec_default(GOVERNANCE, nameCall {}.abi_encode());
    let bytes = extract_output_bytes(&result);
    let len = U256::from_be_slice(&bytes[32..64]).to::<usize>();
    assert_eq!(std::str::from_utf8(&bytes[64..64 + len]).unwrap(), "Telcoin");

    // symbol
    let result = env.exec_default(GOVERNANCE, symbolCall {}.abi_encode());
    let bytes = extract_output_bytes(&result);
    let len = U256::from_be_slice(&bytes[32..64]).to::<usize>();
    assert_eq!(std::str::from_utf8(&bytes[64..64 + len]).unwrap(), "TEL");

    // decimals
    let result = env.exec_default(GOVERNANCE, decimalsCall {}.abi_encode());
    assert_eq!(decode_u256(&result), U256::from(18));
}

#[test]
fn test_total_supply_reflects_operations() {
    let mut env = TestEnv::new();
    let genesis = U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));

    // Initial supply
    assert_eq!(env.get_total_supply(), genesis);

    // Mint + claim
    env.exec_default(GOVERNANCE, mintCall { amount: U256::from(1000) }.abi_encode()).unwrap();
    env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
    env.exec_default(GOVERNANCE, claimCall { recipient: GOVERNANCE }.abi_encode()).unwrap();
    assert_eq!(env.get_total_supply(), genesis + U256::from(1000));

    // Burn
    env.exec_default(GOVERNANCE, burnCall { amount: U256::from(400) }.abi_encode()).unwrap();
    assert_eq!(env.get_total_supply(), genesis + U256::from(1000) - U256::from(400));
}
