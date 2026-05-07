//! Property-based tests for the Telcoin precompile (production mode).
//!
//! These tests verify critical invariants across randomized inputs:
//! - Mint/claim/burn supply invariants
//! - Calldata validation (short/unknown selectors)

use alloy::sol_types::SolCall;
use proptest::prelude::*;
use reth_revm::primitives::{address, Address};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_reth::{
    burnCall, claimCall, grantMintRoleCall, hasMintRoleCall, mintCall, revokeMintRoleCall,
    test_utils::precompile_test_utils::{
        assert_not_success, assert_success, decode_u256, TestEnv, GENESIS_SUPPLY,
    },
    totalSupplyCall, TELCOIN_PRECOMPILE_ADDRESS, TIMELOCK_DURATION,
};
use tn_types::{keccak256, Bytes, U256};

// ==============================
// Mint/Claim/Burn properties
// ==============================

proptest! {
    /// After mint + timelock + claim, totalSupply increases by exactly the minted amount.
    #[test]
    fn prop_mint_claim_supply_invariant(amount in 1u128..1_000_000_000_000_000_000u128) {
        let mut env = TestEnv::new();
        let supply_before = env.get_total_supply();

        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(amount)).unwrap();
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
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(amount)).unwrap();
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

        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(a1)).unwrap();
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(a2)).unwrap();
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
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(amount)).unwrap();
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::ZERO).unwrap();
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
            tn_reth::test_utils::precompile_test_utils::USER,
            mintCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    /// Non-governance_SAFE_ADDRESS caller cannot burn.
    #[test]
    fn prop_unauthorized_burn_fails(amount in 1u64..1000u64) {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            tn_reth::test_utils::precompile_test_utils::USER,
            burnCall { amount: U256::from(amount) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    // ==============================
    // Arithmetic overflow/underflow (unit-layer mirrors of pipeline cases)
    // ==============================

    /// Claim reverts when `totalSupply + amount` would overflow `U256`.
    /// Mirrors the pipeline-level `prop_pipeline_claim_total_supply_overflow`.
    #[test]
    #[cfg(not(feature = "faucet"))]
    fn prop_claim_total_supply_overflow(amount in 1u128..1_000_000u128) {
        let mut env = TestEnv::new();
        env.set_total_supply(U256::MAX - U256::from(amount) + U256::from(1));
        let supply_before = env.get_total_supply();

        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(amount)).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );

        assert_not_success(&result);
        prop_assert_eq!(
            env.get_total_supply(),
            supply_before,
            "total supply must be unchanged after failed claim"
        );
    }

    /// Burn reverts when `totalSupply < amount` (underflow). The precompile balance
    /// is ample, so the failure must come from the supply check, not the balance check.
    /// Mirrors the pipeline-level `prop_pipeline_burn_total_supply_underflow`.
    #[test]
    fn prop_burn_total_supply_underflow(burn_extra in 1u64..1000u64) {
        let mut env = TestEnv::new();
        env.set_total_supply(U256::from(100));
        let supply_before = env.get_total_supply();
        let precompile_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);

        let burn_amount = U256::from(100u64 + burn_extra);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: burn_amount }.abi_encode(),
        );

        assert_not_success(&result);
        prop_assert_eq!(
            env.get_total_supply(),
            supply_before,
            "total supply must be unchanged after failed burn"
        );
        prop_assert_eq!(
            env.get_balance(TELCOIN_PRECOMPILE_ADDRESS),
            precompile_before,
            "precompile balance must be unchanged after failed burn"
        );
    }

    /// When the recipient's native balance is near `U256::MAX`, `balance_incr` silently
    /// no-ops (revm's `incr_balance` uses `checked_add` and ignores overflow). The claim
    /// tx still succeeds, `totalSupply` still increments — this is a documented
    /// invariant gap, mirrored from the pipeline test.
    /// Mirrors the pipeline-level `prop_pipeline_claim_balance_overflow`.
    #[test]
    #[cfg(not(feature = "faucet"))]
    fn prop_claim_balance_overflow(amount in 1u128..1_000_000u128) {
        let governance_bal = U256::MAX - U256::from(amount) + U256::from(1);
        let mut env = TestEnv::new_with_balances(
            governance_bal,
            U256::from(10).pow(U256::from(18)),
            U256::from(1000),
        );
        let supply_before = env.get_total_supply();
        let balance_before = env.get_balance(GOVERNANCE_SAFE_ADDRESS);

        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(amount)).unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );

        assert_success(&result);
        let balance_after = env.get_balance(GOVERNANCE_SAFE_ADDRESS);
        let increase = balance_after.saturating_sub(balance_before);
        prop_assert!(
            increase < U256::from(amount),
            "balance must not increase by full amount under overflow: \
             increased by {increase}, amount was {amount}"
        );
        prop_assert_eq!(
            env.get_total_supply(),
            supply_before + U256::from(amount),
            "total supply increments despite balance overflow (documented invariant gap)"
        );
    }
}

// ==============================
// Calldata validation properties
// ==============================

/// Known selectors for the precompile.
fn known_selectors() -> Vec<[u8; 4]> {
    vec![totalSupplyCall::SELECTOR, claimCall::SELECTOR, burnCall::SELECTOR, mintCall::SELECTOR]
}

proptest! {
    /// Truncated calldata (valid selector but too short) always fails.
    #[test]
    fn prop_short_calldata_fails(
        selector_idx in 0usize..2,
        extra_len in 0usize..31,   // less than 32 bytes of args
    ) {
        let selectors_with_min_args: Vec<([u8; 4], usize)> = vec![
            (claimCall::SELECTOR, 32),
            (burnCall::SELECTOR, 32),
        ];
        let (selector, min_args) = selectors_with_min_args[selector_idx];
        // Only test truncated calldata (less than minimum required)
        let truncated_len = extra_len.min(min_args - 1);
        let mut data = Vec::with_capacity(4 + truncated_len);
        data.extend_from_slice(&selector);
        data.extend(std::iter::repeat_n(0u8, truncated_len));

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
        prop_assume!(!known.contains(&selector_bytes));
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
fn test_total_supply_reflects_operations() {
    let mut env = TestEnv::new();
    let genesis = U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));

    // Initial supply
    assert_eq!(env.get_total_supply(), genesis);

    // Mint + claim
    env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(1000)).unwrap();
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

/// Verify all removed ERC-20 / EIP-2612 selectors revert with "Unknown function selector",
/// while `totalSupply` still succeeds. This locks in the security guarantee that DELEGATECALL
/// into `0x7e1` cannot reach allowance / nonce mutation paths.
#[test]
fn test_removed_selectors_are_rejected() {
    let mut env = TestEnv::new();

    let removed_selectors: &[[u8; 4]] = &[
        [0xa9, 0x05, 0x9c, 0xbb], // transfer(address,uint256)
        [0x09, 0x5e, 0xa7, 0xb3], // approve(address,uint256)
        [0xd5, 0x05, 0xac, 0xcf], // permit(address,address,uint256,uint256,uint8,bytes32,bytes32)
        [0x70, 0xa0, 0x82, 0x31], // balanceOf(address)
        [0xdd, 0x62, 0xed, 0x3e], // allowance(address,address)
        [0x7e, 0xce, 0xbe, 0x00], // nonces(address)
        [0x36, 0x44, 0xe5, 0x15], // DOMAIN_SEPARATOR()
        [0x06, 0xfd, 0xde, 0x03], // name()
        [0x95, 0xd8, 0x9b, 0x41], // symbol()
        [0x31, 0x3c, 0xe5, 0x67], // decimals()
        [0x23, 0xb8, 0x72, 0xdd], // transferFrom(address,address,uint256)
    ];

    for selector in removed_selectors {
        let mut data = Vec::with_capacity(4 + 96);
        data.extend_from_slice(selector);
        data.extend_from_slice(&[0u8; 96]);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    // totalSupply (0x18160ddd) still succeeds.
    let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
    assert!(decode_u256(&result) > U256::ZERO);
}

// ==============================
// `DELEGATECALL` storage-target regression
// ==============================

/// Address used to host the `DELEGATECALL` relay contract in
/// [`test_delegatecall_writes_target_precompile_storage`].
const RELAY_ADDR: Address = address!("dddd0000000000000000000000000000000000d1");

/// Hand-assembled minimal runtime bytecode for a `DELEGATECALL` relay.
///
/// Forwards calldata to `0x7e1` via `DELEGATECALL` and returns whatever the
/// precompile returned. Used to verify revm's storage-target semantics.
///
/// Disassembly:
/// ```text
///   CALLDATASIZE; PUSH1 0; PUSH1 0; CALLDATACOPY     // mem[0..csize] = calldata
///   PUSH1 0; PUSH1 0; CALLDATASIZE; PUSH1 0;          // retSize, retOffset, argsSize, argsOffset
///   PUSH2 0x07e1; GAS; DELEGATECALL; POP              // delegatecall to TEL precompile
///   RETURNDATASIZE; PUSH1 0; PUSH1 0; RETURNDATACOPY  // mem[0..rsize] = returndata
///   RETURNDATASIZE; PUSH1 0; RETURN                   // return mem[0..rsize]
/// ```
const RELAY_BYTECODE: &[u8] = &[
    0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATACOPY(0, 0, CALLDATASIZE)
    0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00, 0x61, 0x07, 0xe1, 0x5a, 0xf4,
    0x50, // DELEGATECALL
    0x3d, 0x60, 0x00, 0x60, 0x00, 0x3e, // RETURNDATACOPY(0, 0, RETURNDATASIZE)
    0x3d, 0x60, 0x00, 0xf3, // RETURN(0, RETURNDATASIZE)
];

/// Lock in revm's `DELEGATECALL`-into-precompile semantics: every `SSTORE` the
/// precompile performs targets the literal `TELCOIN_PRECOMPILE_ADDRESS` it passes
/// to `EvmInternals::sstore`, **not** the calling contract's storage.
///
/// A future revm upgrade that quietly changed this would silently invalidate the
/// security rationale documented in `README.md`. This test fails in CI if that
/// happens.
///
/// Test plan (mainnet feature only — exercises `mint` which writes the pending
/// amount slot under `keccak256(governance, 0)`):
/// 1. Deploy a relay contract that `DELEGATECALL`s `0x7e1` with the inbound calldata.
/// 2. Send `mint(1234)` from `GOVERNANCE_SAFE_ADDRESS` to the relay. Because `msg.sender` is
///    preserved across `DELEGATECALL`, the precompile's governance check passes inside the relay
///    frame.
/// 3. Assert the pending-mint slot under `TELCOIN_PRECOMPILE_ADDRESS` holds `1234`.
/// 4. Assert the same slot under `RELAY_ADDR` is **untouched** (zero) — the `SSTORE` did not bleed
///    into the caller's storage.
#[test]
#[cfg(not(feature = "faucet"))]
fn test_delegatecall_writes_target_precompile_storage() {
    let mut env = TestEnv::new();
    env.deploy_code(RELAY_ADDR, Bytes::from_static(RELAY_BYTECODE));

    let amount = U256::from(1234);
    let calldata = mintCall { amount }.abi_encode();
    let result = env.exec_to(GOVERNANCE_SAFE_ADDRESS, RELAY_ADDR, calldata, 200_000);
    assert_success(&result);

    // Pending-mint amount slot for governance: keccak256(abi.encode(GOVERNANCE_SAFE_ADDRESS, 0)).
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(GOVERNANCE_SAFE_ADDRESS.as_slice());
    let pending_slot = U256::from_be_bytes(keccak256(buf).0);

    assert_eq!(
        env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, pending_slot),
        amount,
        "SSTORE under DELEGATECALL must land in the precompile's storage"
    );
    assert_eq!(
        env.get_storage(RELAY_ADDR, pending_slot),
        U256::ZERO,
        "caller's storage must NOT be written by precompile under DELEGATECALL"
    );
}
