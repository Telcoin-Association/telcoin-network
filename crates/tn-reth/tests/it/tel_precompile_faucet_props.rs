//! Property-based tests for the Telcoin precompile faucet feature.
//!
//! These tests verify faucet-specific invariants:
//! - Direct mint credits recipient immediately
//! - Mint role grant/revoke semantics
//! - Supply accounting with faucet mints

use alloy::sol_types::SolCall;
use proptest::prelude::*;
use reth_revm::primitives::{address, Address};
use tn_config::GOVERNANCE_SAFE_ADDRESS as GOVERNANCE;
use tn_reth::{
    claimCall, grantMintRoleCall, mintCall, revokeMintRoleCall,
    test_utils::precompile_test_utils::{
        assert_not_success, assert_success, TestEnv, RECIPIENT, USER,
    },
};
use tn_types::U256;

const FAUCET: Address = address!("0000000000000000000000000000000000000F00");

fn new_faucet_env() -> TestEnv {
    let mut env = TestEnv::new();
    env.add_account(FAUCET, U256::from(10).pow(U256::from(18)));
    env
}

fn grant_faucet_role(env: &mut TestEnv) {
    env.exec_default(GOVERNANCE, grantMintRoleCall { addr: FAUCET }.abi_encode()).unwrap();
}

// ==============================
// Faucet mint properties
// ==============================

proptest! {
    /// Faucet mint directly credits the recipient's balance.
    #[test]
    fn prop_faucet_mint_credits_directly(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);

        let balance_before = env.get_balance(RECIPIENT);
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert_success(&result);

        let balance_after = env.get_balance(RECIPIENT);
        prop_assert_eq!(
            balance_after, balance_before + U256::from(amount),
            "recipient should gain exactly {}", amount
        );
    }

    /// Faucet mint increments totalSupply by exactly the minted amount.
    #[test]
    fn prop_faucet_mint_increments_supply(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);

        let supply_before = env.get_total_supply();
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert_success(&result);

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after, supply_before + U256::from(amount),
            "supply should increase by exactly {}", amount
        );
    }

    /// Two faucet mints to the same recipient are additive.
    #[test]
    fn prop_faucet_mint_additive(
        a1 in 1u128..500_000_000_000_000_000u128,
        a2 in 1u128..500_000_000_000_000_000u128,
    ) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);

        let balance_before = env.get_balance(RECIPIENT);
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(a1) }.abi_encode(),
        ).unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(a2) }.abi_encode(),
        ).unwrap();

        let balance_after = env.get_balance(RECIPIENT);
        prop_assert_eq!(
            balance_after, balance_before + U256::from(a1) + U256::from(a2),
            "two mints should be additive"
        );
    }

    /// Faucet mint with amount=0 always fails.
    #[test]
    fn prop_faucet_zero_mint_fails(_ in 0u8..1) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::ZERO }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Non-role caller cannot faucet mint.
    #[test]
    fn prop_faucet_unauthorized_mint_fails(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = new_faucet_env();
        // USER does not have mint role
        let result = env.exec_default(
            USER,
            mintCall { recipient: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// grantMintRole then mint succeeds.
    #[test]
    fn prop_faucet_grant_enables_mint(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert_success(&result);
    }

    /// grantMintRole then revokeMintRole then mint fails.
    #[test]
    fn prop_faucet_revoke_disables_mint(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);
        env.exec_default(GOVERNANCE, revokeMintRoleCall { addr: FAUCET }.abi_encode()).unwrap();
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Non-governance grantMintRole always fails.
    #[test]
    fn prop_faucet_grant_unauthorized_fails(addr_byte in 1u8..255u8) {
        let mut env = new_faucet_env();
        let addr = Address::from([addr_byte; 20]);
        let result = env.exec_default(
            USER,
            grantMintRoleCall { addr }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Non-governance revokeMintRole always fails.
    #[test]
    fn prop_faucet_revoke_unauthorized_fails(addr_byte in 1u8..255u8) {
        let mut env = new_faucet_env();
        let addr = Address::from([addr_byte; 20]);
        let result = env.exec_default(
            USER,
            revokeMintRoleCall { addr }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Faucet mint does not create pending state; claim after mint fails.
    #[test]
    fn prop_faucet_claim_after_mint_fails(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = new_faucet_env();
        grant_faucet_role(&mut env);
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(amount) }.abi_encode(),
        ).unwrap();
        let result = env.exec_default(
            GOVERNANCE,
            claimCall { recipient: RECIPIENT }.abi_encode(),
        );
        assert_not_success(&result);
    }
}
