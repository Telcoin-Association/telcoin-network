//! Property-based tests for the Telcoin ERC-20 precompile (production mode).
//!
//! These tests verify critical invariants across randomized inputs:
//! - ERC-20 transfer balance conservation
//! - Approve/transferFrom allowance semantics
//! - Mint/claim/burn supply invariants
//! - Calldata validation (short/unknown selectors)

use alloy::sol_types::SolCall;
use proptest::prelude::*;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_reth::{
    allowanceCall, approveCall, balanceOfCall, burnCall, claimCall, decimalsCall,
    grantMintRoleCall, hasMintRoleCall, mintCall, nameCall, noncesCall, permitCall,
    revokeMintRoleCall, symbolCall,
    test_utils::precompile_test_utils::{
        assert_not_success, assert_success, decode_bool, decode_u256, extract_output_bytes,
        permit_signer_address, sign_permit, TestEnv, GENESIS_SUPPLY, RECIPIENT, TEST_CHAIN_ID,
        USER,
    },
    totalSupplyCall, transferCall, transferFromCall, DOMAIN_SEPARATORCall, TIMELOCK_DURATION,
};
use tn_types::{Address, U256};

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

        // RECIPIENT calls transferFrom(USER -> GOVERNANCE_SAFE_ADDRESS)
        let result = env.exec_default(
            RECIPIENT,
            transferFromCall { from: USER, to: GOVERNANCE_SAFE_ADDRESS, amount: transfer_amt }.abi_encode(),
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
            transferFromCall { from: USER, to: GOVERNANCE_SAFE_ADDRESS, amount: over }.abi_encode(),
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
            transferFromCall { from: USER, to: GOVERNANCE_SAFE_ADDRESS, amount: U256::from(amount) }.abi_encode(),
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

        env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::from(amount) }.abi_encode()).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode());
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
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::from(amount) }.abi_encode()).unwrap();
        env.set_timestamp(1000 + offset);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode());
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

        env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::from(a1) }.abi_encode()).unwrap();
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::from(a2) }.abi_encode()).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode());
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
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::from(amount) }.abi_encode()).unwrap();
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::ZERO }.abi_encode()).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode());
        assert_not_success(&result);
    }

    /// After burn, totalSupply decreases by exactly the burned amount.
    #[test]
    fn prop_burn_supply_invariant(amount in 1u64..1000u64) {
        let mut env = TestEnv::new();
        let supply_before = env.get_total_supply();

        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_success(&result);

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after, supply_before - U256::from(amount),
            "supply should decrease by exactly {}", amount
        );
    }

    /// Non-governance_SAFE_ADDRESS caller cannot mint.
    #[test]
    fn prop_unauthorized_mint_fails(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            mintCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Non-governance_SAFE_ADDRESS caller cannot burn.
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
        permitCall::SELECTOR,
        noncesCall::SELECTOR,
        DOMAIN_SEPARATORCall::SELECTOR,
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
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
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
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
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
    let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, nameCall {}.abi_encode());
    let bytes = extract_output_bytes(&result);
    let len = U256::from_be_slice(&bytes[32..64]).to::<usize>();
    assert_eq!(std::str::from_utf8(&bytes[64..64 + len]).unwrap(), "Telcoin");

    // symbol
    let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, symbolCall {}.abi_encode());
    let bytes = extract_output_bytes(&result);
    let len = U256::from_be_slice(&bytes[32..64]).to::<usize>();
    assert_eq!(std::str::from_utf8(&bytes[64..64 + len]).unwrap(), "TEL");

    // decimals
    let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, decimalsCall {}.abi_encode());
    assert_eq!(decode_u256(&result), U256::from(18));
}

#[test]
fn test_total_supply_reflects_operations() {
    let mut env = TestEnv::new();
    let genesis = U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));

    // Initial supply
    assert_eq!(env.get_total_supply(), genesis);

    // Mint + claim
    env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::from(1000) }.abi_encode())
        .unwrap();
    env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
    env.exec_default(
        GOVERNANCE_SAFE_ADDRESS,
        claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
    )
    .unwrap();
    assert_eq!(env.get_total_supply(), genesis + U256::from(1000));

    // Burn
    env.exec_default(GOVERNANCE_SAFE_ADDRESS, burnCall { amount: U256::from(400) }.abi_encode())
        .unwrap();
    assert_eq!(env.get_total_supply(), genesis + U256::from(1000) - U256::from(400));
}

// ==============================
// EIP-2612 Permit properties
// ==============================

proptest! {
    /// After a valid permit, the allowance equals the permitted value.
    #[test]
    fn prop_permit_sets_allowance(value in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);
        let (v, r, s) = sign_permit(owner, RECIPIENT, U256::from(value), U256::ZERO, deadline, TEST_CHAIN_ID);
        let data = permitCall {
            owner, spender: RECIPIENT, value: U256::from(value), deadline, v, r, s,
        }.abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let allowance = env.get_allowance(owner, RECIPIENT);
        prop_assert_eq!(allowance, U256::from(value), "allowance mismatch after permit");
    }

    /// Multiple permits monotonically increment the nonce.
    #[test]
    fn prop_permit_nonce_monotonic(count in 1u32..5u32) {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);

        for i in 0..count {
            let nonce = U256::from(i);
            let (v, r, s) = sign_permit(owner, RECIPIENT, U256::from(100u128), nonce, deadline, TEST_CHAIN_ID);
            let data = permitCall {
                owner, spender: RECIPIENT, value: U256::from(100u128), deadline, v, r, s,
            }.abi_encode();
            assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

            let current_nonce = env.get_nonce(owner);
            prop_assert_eq!(current_nonce, U256::from(i + 1), "nonce should be {}", i + 1);
        }
    }

    /// Replaying a permit signature fails.
    #[test]
    fn prop_permit_replay_fails(value in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);
        let (v, r, s) = sign_permit(owner, RECIPIENT, U256::from(value), U256::ZERO, deadline, TEST_CHAIN_ID);
        let data = permitCall {
            owner, spender: RECIPIENT, value: U256::from(value), deadline, v, r, s,
        }.abi_encode();

        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data.clone()));
        assert_not_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));
    }

    /// Permit with expired deadline always fails.
    #[test]
    fn prop_permit_expired_deadline_fails(value in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        // Block timestamp is 1000; deadline is 0..999
        let deadline = U256::from(999u64);
        let (v, r, s) = sign_permit(owner, RECIPIENT, U256::from(value), U256::ZERO, deadline, TEST_CHAIN_ID);
        let data = permitCall {
            owner, spender: RECIPIENT, value: U256::from(value), deadline, v, r, s,
        }.abi_encode();

        assert_not_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));
    }

    /// Permit with wrong signer (USER as owner) always fails.
    #[test]
    fn prop_permit_wrong_signer_fails(value in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let signer_addr = permit_signer_address();
        let deadline = U256::from(2000);

        // Sign with the actual signer
        let (v, r, s) = sign_permit(signer_addr, RECIPIENT, U256::from(value), U256::ZERO, deadline, TEST_CHAIN_ID);
        // Submit with USER as the claimed owner
        let data = permitCall {
            owner: USER, spender: RECIPIENT, value: U256::from(value), deadline, v, r, s,
        }.abi_encode();

        assert_not_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));
    }
}
