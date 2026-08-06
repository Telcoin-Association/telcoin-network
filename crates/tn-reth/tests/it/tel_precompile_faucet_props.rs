//! Property-based tests for the Telcoin precompile faucet feature.
//!
//! These tests verify faucet-specific invariants:
//! - Direct mint credits recipient immediately
//! - Mint role grant/revoke semantics
//! - Supply accounting with faucet mints

use crate::precompile_relays::{CALL_RELAY_BYTECODE, STATICCALL_RELAY_BYTECODE};
use alloy::sol_types::SolCall;
use proptest::prelude::*;
use reth_revm::primitives::{address, Address};
use tn_config::GOVERNANCE_SAFE_ADDRESS as GOVERNANCE;
use tn_reth::{
    claimCall, faucet_mint_role_slot, grantMintRoleCall, hasMintRoleCall, mintCall,
    revokeMintRoleCall,
    test_utils::precompile_test_utils::{
        assert_not_success, assert_success, decode_bool, TestEnv, RECIPIENT, USER,
    },
    TELCOIN_PRECOMPILE_ADDRESS,
};
use tn_types::{Bytes, U256};

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
        let result = env.mint(FAUCET, RECIPIENT, U256::from(amount));
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
        let result = env.mint(FAUCET, RECIPIENT, U256::from(amount));
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
        env.mint(FAUCET, RECIPIENT, U256::from(a1)).unwrap();
        env.mint(FAUCET, RECIPIENT, U256::from(a2)).unwrap();

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
        let result = env.mint(FAUCET, RECIPIENT, U256::ZERO);
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
        let result = env.mint(FAUCET, RECIPIENT, U256::from(amount));
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
        env.mint(FAUCET, RECIPIENT, U256::from(amount)).unwrap();
        let result = env.exec_default(
            GOVERNANCE,
            claimCall { recipient: RECIPIENT }.abi_encode(),
        );
        assert_not_success(&result);
    }
}

// ==============================
// `STATICCALL` write protection (faucet selectors)
// ==============================

/// A `STATICCALL` reaching a faucet role-management selector is refused, and writes nothing.
///
/// The mainnet counterparts of these tests live in `tel_precompile_props.rs`, which `main.rs`
/// compiles only under `#[cfg(not(feature = "faucet"))]`. Without this test the faucet build's
/// mutating selectors, and the dispatcher's faucet-only read-only classification, would carry no
/// static-call coverage at all.
///
/// The relay is hosted at [`GOVERNANCE`] because `STATICCALL` does not preserve `msg.sender`: the
/// precompile authorizes against the relay's own address, so a relay anywhere else would be
/// rejected with `"unauthorized"` whether or not the guard exists, and the test would pass against
/// unfixed code. The outer transaction is driven by [`USER`], since revm rejects a transaction
/// whose sender has code (EIP-3607).
///
/// Each relay gets its own environment: within one env the journal caches account code across
/// transactions while `deploy_code` writes to the database underneath it, so swapping the relay
/// bytecode mid-test would silently re-run the first relay.
#[test]
fn test_staticcall_cannot_grant_mint_role() {
    let role_slot = faucet_mint_role_slot(FAUCET);

    // Positive control: the same relay frame under `CALL` grants the role.
    let mut allowed_env = new_faucet_env();
    allowed_env.deploy_code(GOVERNANCE, Bytes::from_static(CALL_RELAY_BYTECODE));
    let allowed = allowed_env.exec_to(
        USER,
        GOVERNANCE,
        grantMintRoleCall { addr: FAUCET }.abi_encode(),
        200_000,
    );
    assert_success(&allowed);
    assert!(decode_bool(&allowed), "CALL into grantMintRole must be accepted");
    assert_eq!(
        allowed_env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, role_slot),
        U256::from(1),
        "positive control failed: the CALL relay never reached the precompile, so the \
         STATICCALL assertions below would be vacuous"
    );

    // Same caller, same calldata, same authorization - only the opcode changes.
    let mut refused_env = new_faucet_env();
    refused_env.deploy_code(GOVERNANCE, Bytes::from_static(STATICCALL_RELAY_BYTECODE));
    let refused = refused_env.exec_to(
        USER,
        GOVERNANCE,
        grantMintRoleCall { addr: FAUCET }.abi_encode(),
        200_000,
    );
    assert_success(&refused);
    assert!(!decode_bool(&refused), "STATICCALL into grantMintRole must be refused");
    assert_eq!(
        refused_env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, role_slot),
        U256::ZERO,
        "STATICCALL must not write the mint-role slot"
    );
}

/// `hasMintRole` is read-only, so the guard must leave it reachable inside a `STATICCALL`.
///
/// This is the faucet-only half of the dispatcher's read-only classification, the arm gated behind
/// `cfg!(feature = "faucet")`. The role is deliberately not granted first: the assertion is on the
/// relay's returned success flag (did the precompile serve the call), not on `hasMintRole`'s own
/// boolean answer, and granting it would require a transaction that loads `GOVERNANCE` into the
/// journal before the relay is deployed there.
#[test]
fn test_staticcall_still_serves_has_mint_role() {
    let mut env = new_faucet_env();
    env.deploy_code(GOVERNANCE, Bytes::from_static(STATICCALL_RELAY_BYTECODE));

    let result =
        env.exec_to(USER, GOVERNANCE, hasMintRoleCall { addr: FAUCET }.abi_encode(), 200_000);
    assert_success(&result);
    assert!(decode_bool(&result), "hasMintRole must remain callable under STATICCALL");
}
