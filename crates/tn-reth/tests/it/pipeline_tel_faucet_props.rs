//! Full-pipeline property-based tests for the TEL precompile faucet feature.
//!
//! These tests exercise faucet-specific functionality through the complete TN pipeline:
//! mint role management, direct minting, and supply accounting.

use alloy::sol_types::SolCall;
use proptest::prelude::*;
use secp256k1::rand::{rngs::StdRng, SeedableRng as _};
use tn_reth::{
    grantMintRoleCall, mintCall, revokeMintRoleCall, transferCall, TELCOIN_PRECOMPILE_ADDRESS,
};
use tn_types::{Bytes, Encodable2718, U256, MIN_PROTOCOL_BASE_FEE};

use tn_reth::test_utils::TransactionFactory;

use super::pipeline_helpers::*;

/// Setup: fund the faucet address by transferring from user, then grant mint role.
/// Returns the faucet factory.
fn setup_faucet(env: &mut PipelineTestEnv) -> TransactionFactory {
    let mut faucet_factory =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(400));
    let faucet_addr = faucet_factory.address();

    // Block 1: user transfers gas money to faucet
    let fund_tx = env.user_precompile_tx(
        transferCall {
            to: faucet_addr,
            amount: U256::from(10).pow(U256::from(18)) * U256::from(1_000u64),
        }
        .abi_encode(),
    );
    let block = env.execute_block(vec![fund_tx]).expect("fund faucet");
    assert!(env.tx_succeeded(&block, 0), "fund faucet tx should succeed");

    // Block 2: governance grants mint role to faucet via forwarder
    let grant_tx = env.governance_tx(
        grantMintRoleCall { addr: faucet_addr }.abi_encode(),
    );
    let block = env.execute_block(vec![grant_tx]).expect("grant mint role");
    assert!(env.tx_succeeded(&block, 0), "grant mint role tx should succeed");

    faucet_factory
}

/// Create an encoded faucet tx targeting the precompile directly.
fn faucet_precompile_tx(
    factory: &mut TransactionFactory,
    env: &PipelineTestEnv,
    calldata: Vec<u8>,
) -> Vec<u8> {
    PipelineTestEnv::precompile_tx(factory, &env.chain, calldata)
}

// ==============================
// Faucet mint properties
// ==============================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Faucet mint directly credits recipient's balance and increases supply.
    #[test]
    fn prop_pipeline_faucet_mint_credits_directly(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let mut faucet = setup_faucet(&mut env);
        let recipient_addr = env.recipient_factory.address();

        let balance_before = env.get_balance(recipient_addr);
        let supply_before = env.get_total_supply();

        let mint_tx = faucet_precompile_tx(
            &mut faucet,
            &env,
            mintCall { recipient: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![mint_tx]).expect("execute faucet mint");
        assert!(env.tx_succeeded(&block, 0), "faucet mint tx should succeed");

        let balance_after = env.get_balance(recipient_addr);
        let supply_after = env.get_total_supply();

        prop_assert_eq!(
            balance_after,
            balance_before + U256::from(amount),
            "recipient should gain minted amount"
        );
        prop_assert_eq!(
            supply_after,
            supply_before + U256::from(amount),
            "total supply should increase"
        );
    }

    /// Two faucet mints to the same recipient are additive.
    #[test]
    fn prop_pipeline_faucet_mint_additive(
        a1 in 1u128..500_000u128,
        a2 in 1u128..500_000u128,
    ) {
        let mut env = PipelineTestEnv::new();
        let mut faucet = setup_faucet(&mut env);
        let recipient_addr = env.recipient_factory.address();

        let balance_before = env.get_balance(recipient_addr);

        let tx1 = faucet_precompile_tx(
            &mut faucet,
            &env,
            mintCall { recipient: recipient_addr, amount: U256::from(a1) }.abi_encode(),
        );
        let block1 = env.execute_block(vec![tx1]).expect("execute first mint");
        assert!(env.tx_succeeded(&block1, 0), "first mint should succeed");

        let tx2 = faucet_precompile_tx(
            &mut faucet,
            &env,
            mintCall { recipient: recipient_addr, amount: U256::from(a2) }.abi_encode(),
        );
        let block2 = env.execute_block(vec![tx2]).expect("execute second mint");
        assert!(env.tx_succeeded(&block2, 0), "second mint should succeed");

        let balance_after = env.get_balance(recipient_addr);
        prop_assert_eq!(
            balance_after,
            balance_before + U256::from(a1) + U256::from(a2),
            "two mints should be additive"
        );
    }

    /// Grant then revoke mint role disables minting.
    #[test]
    fn prop_pipeline_faucet_revoke_disables(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let mut faucet = setup_faucet(&mut env);
        let faucet_addr = faucet.address();
        let recipient_addr = env.recipient_factory.address();

        let supply_before = env.get_total_supply();

        // Revoke the faucet's mint role
        let revoke_tx = env.governance_tx(
            revokeMintRoleCall { addr: faucet_addr }.abi_encode(),
        );
        let block = env.execute_block(vec![revoke_tx]).expect("execute revoke");
        assert!(env.tx_succeeded(&block, 0), "revoke tx should succeed");

        // Attempt to mint after revocation
        let mint_tx = faucet_precompile_tx(
            &mut faucet,
            &env,
            mintCall { recipient: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![mint_tx]).expect("execute revoked mint");
        assert!(!env.tx_succeeded(&block, 0), "mint after revoke should fail");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before,
            "supply should be unchanged after failed mint"
        );
    }

    /// Unauthorized user (no mint role) cannot faucet mint.
    #[test]
    fn prop_pipeline_faucet_unauthorized_mint_fails(amount in 1u128..1_000_000u128) {
        let mut env = PipelineTestEnv::new();
        let recipient_addr = env.recipient_factory.address();
        let supply_before = env.get_total_supply();

        // User sends mint calldata directly to precompile without mint role
        let tx = env.user_precompile_tx(
            mintCall { recipient: recipient_addr, amount: U256::from(amount) }.abi_encode(),
        );
        let block = env.execute_block(vec![tx]).expect("execute unauthorized mint");
        assert!(!env.tx_succeeded(&block, 0), "unauthorized faucet mint should fail");

        let supply_after = env.get_total_supply();
        prop_assert_eq!(
            supply_after,
            supply_before,
            "supply should be unchanged after unauthorized mint"
        );
    }
}
