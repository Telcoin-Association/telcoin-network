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
        assert_not_success, assert_success, decode_bool, decode_u256, TestEnv, GENESIS_SUPPLY, USER,
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
// `STATICCALL` write protection
// ==============================

use crate::precompile_relays::{
    CALLCODE_RELAY_BYTECODE, CALL_RELAY_BYTECODE, DELEGATECALL_RELAY_BYTECODE,
    STATICCALL_RELAY_BYTECODE,
};

/// Pending-mint amount slot for governance: `keccak256(abi.encode(GOVERNANCE_SAFE_ADDRESS, 0))`.
fn governance_pending_slot() -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(GOVERNANCE_SAFE_ADDRESS.as_slice());
    U256::from_be_bytes(keccak256(buf).0)
}

/// A `STATICCALL` reaching a state-mutating selector is refused, and writes nothing.
///
/// Registering `0x7e1` as a precompile short-circuits bytecode execution, so the interpreter's
/// `require_non_staticcall!` write protection never runs on the `SSTORE`s and `LOG`s the handlers
/// perform. Only the dispatcher's own check stands between a static frame and a real mutation.
///
/// The relay has to be hosted **at** [`GOVERNANCE_SAFE_ADDRESS`]. `STATICCALL` does not preserve
/// `msg.sender` (unlike the `DELEGATECALL` case above), so the precompile authorizes against the
/// relay's own address; a relay at a fresh address would be rejected with `"unauthorized"` whether
/// or not the static guard exists, and the test would pass vacuously. The outer transaction is
/// therefore driven by [`USER`], since revm rejects a transaction whose sender has code (EIP-3607).
///
/// Test plan:
/// 1. Host a `CALL` relay at governance and `mint(1234)` through it. This is the positive control:
///    it proves the authorization path and the write both work through a relay frame.
/// 2. In a second environment, host the `STATICCALL` relay, identical but for the call opcode and
///    the dropped `value` operand, and `mint(5678)` through it.
/// 3. Assert the inner call reported failure and that nothing was written.
#[test]
#[cfg(not(feature = "faucet"))]
fn test_staticcall_cannot_mutate_precompile_state() {
    let pending_slot = governance_pending_slot();

    // 1. Positive control: the same relay frame under `CALL` mints successfully.
    //
    // This uses its own `TestEnv`. Within one env the journal caches account code across
    // transactions, while `deploy_code` writes to the database underneath it, so redeploying over
    // a contract that has already executed does not take effect and the second transaction would
    // silently re-run the first relay.
    let mut allowed_env = TestEnv::new();
    allowed_env.deploy_code(GOVERNANCE_SAFE_ADDRESS, Bytes::from_static(CALL_RELAY_BYTECODE));
    let allowed = allowed_env.exec_to(
        USER,
        GOVERNANCE_SAFE_ADDRESS,
        mintCall { amount: U256::from(1234) }.abi_encode(),
        200_000,
    );
    assert_success(&allowed);
    assert!(decode_bool(&allowed), "CALL into a mutating selector must be accepted");
    assert_eq!(
        allowed_env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, pending_slot),
        U256::from(1234),
        "positive control failed: the CALL relay never reached the precompile, so the \
         STATICCALL assertions below would be vacuous"
    );

    // 2. Same caller, same calldata shape, same authorization - only the opcode changes.
    let mut refused_env = TestEnv::new();
    refused_env.deploy_code(GOVERNANCE_SAFE_ADDRESS, Bytes::from_static(STATICCALL_RELAY_BYTECODE));
    let refused = refused_env.exec_to(
        USER,
        GOVERNANCE_SAFE_ADDRESS,
        mintCall { amount: U256::from(5678) }.abi_encode(),
        200_000,
    );
    assert_success(&refused);

    // 3. The precompile must have refused, and nothing may have been written.
    assert!(
        !decode_bool(&refused),
        "STATICCALL into a mutating selector must be refused by the precompile"
    );
    assert_eq!(
        refused_env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, pending_slot),
        U256::ZERO,
        "STATICCALL must not write the pending-mint slot"
    );
}

/// The static guard rejects mutation only: read-only selectors stay callable under `STATICCALL`.
///
/// Guards this fix against over-reach. `totalSupply()` is exactly the kind of call a static frame
/// is supposed to be able to make, and the relay used here is the same one that is refused a
/// `mint` in [`test_staticcall_cannot_mutate_precompile_state`], so the selector is the only
/// variable between the two outcomes.
#[test]
fn test_staticcall_still_serves_read_only_selectors() {
    let mut env = TestEnv::new();
    env.deploy_code(GOVERNANCE_SAFE_ADDRESS, Bytes::from_static(STATICCALL_RELAY_BYTECODE));

    let result =
        env.exec_to(USER, GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode(), 200_000);
    assert_success(&result);
    assert!(decode_bool(&result), "totalSupply() must remain callable under STATICCALL");
}

// ==============================
// Direct-call guard: `DELEGATECALL` / `CALLCODE` refusal
// ==============================

/// Address hosting the `DELEGATECALL` relay in the direct-call guard tests.
///
/// The relay must live at an address other than [`GOVERNANCE_SAFE_ADDRESS`]: `DELEGATECALL`
/// preserves the parent frame's `msg.sender`, so hosting it here and driving it from governance is
/// what puts `caller == GOVERNANCE_SAFE_ADDRESS` in front of the precompile together with
/// `target_address == RELAY_ADDR`.
const RELAY_ADDR: Address = address!("dddd0000000000000000000000000000000000d1");

/// A `DELEGATECALL` into `0x7e1` is refused before dispatch, and writes nothing.
///
/// revm dispatches a precompile by the frame's `bytecode_address` alone, so a `DELEGATECALL` whose
/// code address is `0x7e1` reaches the dispatcher with `caller` inherited from the parent frame:
/// here `GOVERNANCE_SAFE_ADDRESS`, because governance is the one calling the relay. Before the
/// direct-call guard existed this frame passed the governance check and wrote the pending-mint
/// slot under the canonical `0x7e1` (the handlers hardcode that address for every `SSTORE`, so
/// nothing landed in the relay's own storage). This test pins the refusal: a contract that
/// governance chooses to `CALL` must not be able to act as governance towards the precompile.
///
/// Test plan:
/// 1. Host a `DELEGATECALL` relay at [`RELAY_ADDR`] and `mint(1234)` through it from
///    [`GOVERNANCE_SAFE_ADDRESS`]. The inner frame has `caller == GOVERNANCE_SAFE_ADDRESS` and
///    `target_address == RELAY_ADDR`, which is not `0x7e1`.
/// 2. Assert the inner call reported failure.
/// 3. Assert the pending-mint slot is untouched under both `0x7e1` and [`RELAY_ADDR`].
///
/// The positive control (the same governance authorization succeeds through a direct `CALL`
/// relay frame) is the first half of [`test_staticcall_cannot_mutate_precompile_state`].
#[test]
#[cfg(not(feature = "faucet"))]
fn test_delegatecall_cannot_reach_precompile() {
    let pending_slot = governance_pending_slot();
    let mut env = TestEnv::new();
    env.deploy_code(RELAY_ADDR, Bytes::from_static(DELEGATECALL_RELAY_BYTECODE));

    let result = env.exec_to(
        GOVERNANCE_SAFE_ADDRESS,
        RELAY_ADDR,
        mintCall { amount: U256::from(1234) }.abi_encode(),
        200_000,
    );
    assert_success(&result);
    assert!(
        !decode_bool(&result),
        "DELEGATECALL into the precompile must be refused even with a governance caller"
    );
    assert_eq!(
        env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, pending_slot),
        U256::ZERO,
        "DELEGATECALL must not write the canonical pending-mint slot"
    );
    assert_eq!(
        env.get_storage(RELAY_ADDR, pending_slot),
        U256::ZERO,
        "DELEGATECALL must not write the relay's storage either"
    );
}

/// The direct-call guard is keyed on the call scheme, not the selector: `totalSupply()` is served
/// to a `STATICCALL` but refused to a `DELEGATECALL`.
///
/// Pins the guard's placement ahead of the read-only classification, so an indirect frame is
/// refused whatever it asks for. The same relay is refused a `mint` in
/// [`test_delegatecall_cannot_reach_precompile`], and the same selector is served through the
/// `STATICCALL` relay in [`test_staticcall_still_serves_read_only_selectors`], so the call scheme
/// is the only variable.
#[test]
fn test_delegatecall_refuses_read_only_selectors_too() {
    let mut env = TestEnv::new();
    env.deploy_code(RELAY_ADDR, Bytes::from_static(DELEGATECALL_RELAY_BYTECODE));

    let result = env.exec_to(USER, RELAY_ADDR, totalSupplyCall {}.abi_encode(), 200_000);
    assert_success(&result);
    assert!(!decode_bool(&result), "totalSupply() must be refused under DELEGATECALL");
}

/// A `CALLCODE` into `0x7e1` is refused before dispatch, and writes nothing.
///
/// `CALLCODE` sets the inner frame's `caller` to the calling contract itself (not the preserved
/// grandparent that `DELEGATECALL` presents), so to put a governance caller in front of the
/// precompile the relay is hosted **at** [`GOVERNANCE_SAFE_ADDRESS`], as the `STATICCALL` tests
/// do, and driven by [`USER`] (EIP-3607 rejects a transaction whose sender has code). That is the
/// strongest case: even a frame whose `caller` genuinely is governance is refused when it did not
/// reach `0x7e1` by a direct call, because `target_address` is the relay's own address. The `CALL`
/// relay at the same address is accepted in [`test_staticcall_cannot_mutate_precompile_state`], so
/// the opcode is the only variable.
#[test]
#[cfg(not(feature = "faucet"))]
fn test_callcode_cannot_reach_precompile() {
    let pending_slot = governance_pending_slot();
    let mut env = TestEnv::new();
    env.deploy_code(GOVERNANCE_SAFE_ADDRESS, Bytes::from_static(CALLCODE_RELAY_BYTECODE));

    let result = env.exec_to(
        USER,
        GOVERNANCE_SAFE_ADDRESS,
        mintCall { amount: U256::from(1234) }.abi_encode(),
        200_000,
    );
    assert_success(&result);
    assert!(!decode_bool(&result), "CALLCODE into the precompile must be refused");
    assert_eq!(
        env.get_storage(TELCOIN_PRECOMPILE_ADDRESS, pending_slot),
        U256::ZERO,
        "CALLCODE must not write the canonical pending-mint slot"
    );
    assert_eq!(
        env.get_storage(GOVERNANCE_SAFE_ADDRESS, pending_slot),
        U256::ZERO,
        "CALLCODE must not write the relay's storage either"
    );
}
