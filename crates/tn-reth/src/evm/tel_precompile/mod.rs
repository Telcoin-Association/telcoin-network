//! Native TEL precompile.
//!
//! Implements the protocol-level token-issuance surface for Telcoin's native token directly at the
//! EVM precompile level. Unlike a standard Solidity contract, this precompile operates on **native
//! account balances** — minting and burning move native wei into and out of accounts. TEL is
//! simultaneously the chain's gas token and the protocol-managed asset issued through this
//! precompile.
//!
//! The precompile is registered as a [`DynPrecompile`] inside a [`PrecompilesMap`] at
//! [`TELCOIN_PRECOMPILE_ADDRESS`] (`0x7e1`). Every call frame whose code address is `0x7e1` is
//! intercepted and routed to [`telcoin_precompile`] instead of executing bytecode. revm keys that
//! lookup on the frame's `bytecode_address` alone, so `DELEGATECALL` and `CALLCODE` frames reach
//! the dispatcher as well as `CALL`/`STATICCALL`. Bypassing bytecode execution leaves three frame
//! properties for [`telcoin_precompile`] to enforce itself. It refuses any frame that did not
//! reach `0x7e1` by a direct call: under `DELEGATECALL` the `caller` it would authorize is
//! inherited from the parent frame, while every handler still writes the canonical `0x7e1`
//! ledger. Inside a `STATICCALL` frame it serves the read-only selectors and refuses every
//! state-mutating one. And the compiled `CALLVALUE` guard a Solidity dispatcher would run never
//! executes here, so [`telcoin_precompile`] rejects nonzero call value on every selector except
//! the explicitly payable `burn(uint256)`.
//!
//! # Module structure
//!
//! | Module       | Responsibility |
//! |--------------|----------------|
//! | [`burnable`] | Timelocked `mint` / `claim` / `burn` lifecycle and `totalSupply()` view (mainnet) |
//! | [`faucet`]   | Instant `mint` with role management (testnet, `faucet` feature) |
//! | [`helpers`]  | Storage-slot computation and balance manipulation utilities |
//! | [`test_utils`] | In-memory EVM test harness (test/test-utils only) |
//!
//! # Storage layout
//!
//! All precompile-managed state is stored under [`TELCOIN_PRECOMPILE_ADDRESS`] using
//! Solidity-compatible mapping layouts:
//!
//! | Base slot | Type | Description |
//! |-----------|------|-------------|
//! | 0 | `mapping(address => uint256)` | Pending mint amounts |
//! | 1 | `mapping(address => uint256)` | Unlock timestamps |
//! | 3 | `mapping(address => bool)` | Mint roles (faucet feature only) |
//! | 100 | `uint256` | Total circulating supply |
//!
//! # Access control
//!
//! - **Governance** ([`GOVERNANCE_SAFE_ADDRESS`](tn_config::GOVERNANCE_SAFE_ADDRESS)): can `mint`,
//!   `claim`, `burn`, and (with faucet) `grantMintRole`/`revokeMintRole`.
//! - **Mint-role holders** (faucet only): can call the faucet `mint(address, uint256)`.
//! - **Any account**: can call `totalSupply()` (read-only).
//! - **Static frames**: regardless of caller, only the read-only selectors (`totalSupply()`, plus
//!   `hasMintRole` with the faucet feature) are served inside a `STATICCALL`.
//! - **Indirect frames**: regardless of caller and selector, a `DELEGATECALL` or `CALLCODE` into
//!   `0x7e1` is refused before dispatch. Only a direct `CALL`/`STATICCALL` reaches a handler.
//! - **Call value**: only `burn(uint256)` accepts nonzero value, and only when the precompile is
//!   the actual transfer target (a plain `CALL`): the attached wei tops up the pool that `burn`
//!   destroys from, and any excess stays in the pool for later burns. Every other value-bearing
//!   call is rejected, so `burn` is the single value inlet at [`TELCOIN_PRECOMPILE_ADDRESS`].
//!
//! # Feature flags
//!
//! - **`faucet`**: Replaces the timelocked `mint(uint256)` with an instant `mint(address, uint256)`
//!   and adds role-management functions. **Must never be enabled in mainnet builds** — it removes
//!   the timelock safety window.
use alloy::{primitives::address, sol_types::SolCall};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use reth_revm::precompile::{PrecompileError, PrecompileId, PrecompileResult};
use tn_types::{Address, U256};

/// Timelocked mint/claim lifecycle and token burning.
mod burnable;
/// Testnet faucet: instant minting with role management (compiled with `faucet` feature).
#[cfg(feature = "faucet")]
mod faucet;
/// Storage-slot computation utilities.
mod helpers;
/// In-memory EVM test harness (available in tests and with `test-utils` feature).
#[cfg(any(test, feature = "test-utils"))]
pub(crate) mod test_utils;

// --- Re-exports for external consumers ---

/// mainnet `mint(uint256)` call type (timelocked, governance-only).
#[cfg(not(feature = "faucet"))]
pub use burnable::mintCall;
/// mainnet timelock duration constant (7 days).
#[cfg(not(feature = "faucet"))]
pub use burnable::TIMELOCK_DURATION;
pub use burnable::{
    burnCall, claimCall, grantMintRoleCall, hasMintRoleCall, revokeMintRoleCall, totalSupplyCall,
};
/// Faucet `mint(address, uint256)` call type (instant, role-gated).
#[cfg(feature = "faucet")]
pub use faucet::mintCall;

/// The canonical address of the Telcoin precompile: `0x7e1`.
///
/// All TEL token state (pending mints, total supply) is stored under this address.
/// Native account balances at other addresses represent TEL holdings for those accounts.
/// Calls targeting this address are intercepted by the [`DynPrecompile`] registered via
/// [`add_telcoin_precompile`] and routed to the precompile dispatcher instead of executing EVM
/// bytecode.
pub const TELCOIN_PRECOMPILE_ADDRESS: Address =
    address!("00000000000000000000000000000000000007e1");

/// Storage slot under [`TELCOIN_PRECOMPILE_ADDRESS`] holding `addr`'s faucet mint role.
///
/// Seeding this slot to `1` in genesis authorizes `addr` to call the faucet
/// `mint(address, uint256)`, mirroring what a runtime [`grantMintRoleCall`] from the
/// governance safe writes.
///
/// When seeding, extend the canonical precompile genesis account rather than replacing
/// it: its nonzero code (`0xfe`) exempts the account from EIP-158 state clearing. A
/// storage-only (empty) account is deleted at the end of the first block that touches
/// the precompile, silently wiping this slot and the total supply.
#[cfg(feature = "faucet")]
pub fn faucet_mint_role_slot(addr: Address) -> U256 {
    faucet::mint_role_slot(addr)
}

/// Fixed storage slot (100) that holds the total circulating supply of TEL.
///
/// Incremented on [`handle_claim`] and decremented on [`handle_burn`].
/// This is a plain slot (not a mapping), so it can be read directly without hashing.
const TOTAL_SUPPLY_SLOT: U256 = U256::from_limbs([100, 0, 0, 0]);

/// Registers the Telcoin precompile at [`TELCOIN_PRECOMPILE_ADDRESS`] in the given map.
pub fn add_telcoin_precompile(map: &mut PrecompilesMap) {
    map.apply_precompile(&TELCOIN_PRECOMPILE_ADDRESS, move |_| {
        Some(DynPrecompile::new_stateful(PrecompileId::Custom("telcoin".into()), move |input| {
            telcoin_precompile(input)
        }))
    });
}

/// Rejection emitted when a state-mutating selector is reached inside a `STATICCALL` frame.
const STATIC_CALL_MUTATION: &str = "static call: state mutation not permitted";

/// Rejection emitted when the precompile is entered through `DELEGATECALL` or `CALLCODE` rather
/// than by a direct call to its own address.
const INDIRECT_ENTRY: &str = "telcoin precompile: only a direct call to 0x7e1 is permitted";

/// Rejection emitted when nonzero call value is attached to a selector not named payable.
const NON_PAYABLE_CALL_VALUE: &str = "call value: selector is not payable";

/// Top-level dispatcher for the Telcoin precompile.
///
/// Extracts the 4-byte selector from calldata and routes to the matching handler.
///
/// # Direct-call guard
///
/// revm looks a precompile up by the frame's `bytecode_address` alone, so a `DELEGATECALL` or
/// `CALLCODE` whose code address is `0x7e1` lands here exactly like a plain `CALL`. Under
/// `DELEGATECALL` the frame's `caller` is inherited from the parent frame: a contract that
/// governance chose to `CALL`, delegating into `0x7e1`, presents `caller ==
/// GOVERNANCE_SAFE_ADDRESS`. Every handler below reads and writes the canonical `0x7e1` ledger,
/// never the delegating contract's storage, so the authorization gates would be trusting a
/// spoofable identity. The first check therefore refuses any frame that is not a direct call,
/// through [`PrecompileInput::is_direct_call`] (`target_address == bytecode_address`): a direct
/// `CALL`/`STATICCALL` sets both to `0x7e1`, whereas `DELEGATECALL`/`CALLCODE` keep the calling
/// contract's own address as `target_address`. It runs ahead of the calldata length check and of
/// selector dispatch, so it covers the read-only selectors too: an indirect frame has no legitimate
/// use of this precompile, and refusing it whole is cheaper to reason about than a per-selector
/// exemption.
///
/// # Static-call write protection
///
/// [`totalSupplyCall`] and (with the `faucet` feature) [`hasMintRoleCall`] only read state, so they
/// stay callable inside a `STATICCALL` frame. Every other selector dispatched below writes storage,
/// native balances, or logs, and is refused when [`PrecompileInput::is_static`] is set.
///
/// Enforcing this here is not redundant with the interpreter. Registering this address as a
/// precompile short-circuits bytecode execution, so the `require_non_staticcall!` check that
/// `SSTORE` and `LOG` expand through never runs for calls routed to [`telcoin_precompile`], and the
/// journal beneath it carries no static-context flag to catch the writes either. Write protection
/// for a precompile is the precompile's own responsibility.
///
/// Classifying by *read-only* rather than by *mutating* selector is deliberate: a selector added to
/// the dispatcher later is guarded unless it is explicitly named read-only here, so the failure
/// direction is a refused read rather than an unguarded write. One consequence is that an
/// unrecognised selector inside a static frame reports [`STATIC_CALL_MUTATION`] rather than
/// `"Unknown function selector"`; both are [`PrecompileError::Other`] and both revert the frame.
///
/// # Payability
///
/// The EVM credits attached call value to [`TELCOIN_PRECOMPILE_ADDRESS`] before the precompile
/// runs, commits the transfer when the call succeeds, and reverts it only when the call errors.
/// The `CALLVALUE` guard a compiled Solidity dispatcher would run never executes for calls routed
/// here, so payability, like static-call write protection, is the precompile's own
/// responsibility. A selector that accepted value it does not account for would strand the wei:
/// no selector pays balance back out of [`TELCOIN_PRECOMPILE_ADDRESS`], so stray value could only
/// ever be destroyed by a future governance `burn`, never returned.
///
/// [`burnCall`] is the single payable selector. `burn` destroys from the precompile's own
/// balance, a plain value send reverts on the calldata-length check above, and the genesis
/// account starts with zero balance, so attaching value to `burn(amount)` is the sanctioned inlet
/// for the pool it draws from: `msg.value == amount` funds and burns in one transaction, and any
/// excess stays in the pool for later burns. The EVM's own transfer of that value emits nothing,
/// so [`handle_burn`](burnable::handle_burn) mirrors it as an inbound `Transfer` log — see there.
/// The classification is a payable-list rather than a
/// reject-list for the same fail-safe reason as the static gate: a selector added later rejects
/// value unless it is explicitly named payable, and an unrecognised value-bearing selector
/// reports [`NON_PAYABLE_CALL_VALUE`] rather than `"Unknown function selector"`. Inside a
/// `DELEGATECALL` or `CALLCODE` frame [`PrecompileInput::value`] carries the calling frame's
/// apparent `msg.value` and nothing is transferred to the precompile. The direct-call guard
/// above already refuses such frames before dispatch; independently of that guard, the payable
/// exemption requires [`PrecompileInput::target_address`] to be the precompile itself, so
/// apparent value that never funded the pool could not authorize a draw from it even if an
/// indirect frame reached this check.
///
/// # Rejection gas semantics
///
/// Every rejection here — the static-call refusal, the payability refusal, an unrecognised
/// selector, and each handler's own gas-limit precheck, `unauthorized`, decode, and arithmetic
/// errors — returns [`PrecompileError`]. revm splits that into two instruction results: the
/// `gas_limit < GAS_COST` precheck each handler runs returns `PrecompileError::OutOfGas`, which
/// maps to `InstructionResult::PrecompileOOG`, and every other variant maps to
/// `InstructionResult::PrecompileError` (`revm-handler`'s `precompile_provider.rs`, keyed on
/// `PrecompileError::is_oog`). Both are a **halt**, not a revert, and a halt consumes all the gas
/// the frame was given. revm returns the unspent remainder only for results that are
/// `is_ok_or_revert()` (`revm-handler`'s `insert_call_outcome` for a sub-call,
/// `last_frame_result` for the top level), and neither halt is, so a rejected sub-call loses the
/// entire 63/64 it was forwarded and a rejected top-level transaction is charged its full
/// `gas_limit`.
///
/// For callers this is a cliff rather than a soft failure. A contract that forwards `msg.value`
/// into [`TELCOIN_PRECOMPILE_ADDRESS`] and inspects the returned boolean does get `false` — a
/// non-ok call result pushes zero — but it holds only the 1/64 it retained, so the outer frame
/// dies instead of handling the failure. The pre-existing static-call refusal has always behaved
/// this way and the payability refusal deliberately matches it: consuming the limit is the
/// EVM's standard price for an invalid operation, and a value-bearing call to a non-payable
/// selector is a caller bug. Callers must satisfy the precompile's rules up front rather than
/// plan to recover from a rejection.
fn telcoin_precompile(mut input: PrecompileInput<'_>) -> PrecompileResult {
    // Refuse DELEGATECALL/CALLCODE before anything else. See "Direct-call guard" above.
    input
        .is_direct_call()
        .then_some(())
        .ok_or_else(|| PrecompileError::Other(INDIRECT_ENTRY.into()))?;

    if input.data.len() < 4 {
        return Err(PrecompileError::Other("Invalid input: too short".into()));
    }

    let selector: [u8; 4] = input.data[0..4].try_into().unwrap();
    let calldata = &input.data[4..];

    // Selectors that touch no state. Everything else the `match` below recognises mutates.
    // `hasMintRole` is dispatched only under the `faucet` feature, so it counts as read-only only
    // there: this list mirrors the read-only arms of that `match` exactly, under both feature sets.
    let read_only = matches!(selector, burnable::totalSupplyCall::SELECTOR)
        || (cfg!(feature = "faucet") && matches!(selector, hasMintRoleCall::SELECTOR));
    (read_only || !input.is_static)
        .then_some(())
        .ok_or_else(|| PrecompileError::Other(STATIC_CALL_MUTATION.into()))?;

    // Selectors permitted to carry nonzero call value: `burn` alone, and only when the precompile
    // is the transfer target, so the value has actually landed in the pool the handler draws
    // from. See "Payability" in the doc comment above.
    let payable = matches!(selector, burnable::burnCall::SELECTOR)
        && input.target_address == TELCOIN_PRECOMPILE_ADDRESS;
    (payable || input.value == U256::ZERO)
        .then_some(())
        .ok_or_else(|| PrecompileError::Other(NON_PAYABLE_CALL_VALUE.into()))?;

    match selector {
        // State-mutating functions
        #[cfg(not(feature = "faucet"))]
        burnable::mintCall::SELECTOR => {
            burnable::handle_mint(&mut input.internals, calldata, input.caller, input.gas)
        }
        burnable::claimCall::SELECTOR => {
            burnable::handle_claim(&mut input.internals, calldata, input.caller, input.gas)
        }
        burnable::burnCall::SELECTOR => burnable::handle_burn(
            &mut input.internals,
            calldata,
            input.caller,
            input.gas,
            input.value,
        ),
        // Read-only functions
        burnable::totalSupplyCall::SELECTOR => {
            burnable::handle_total_supply(&mut input.internals, input.gas)
        }
        // Faucet feature: role management
        #[cfg(feature = "faucet")]
        grantMintRoleCall::SELECTOR => {
            faucet::handle_grant_mint_role(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        revokeMintRoleCall::SELECTOR => {
            faucet::handle_revoke_mint_role(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        hasMintRoleCall::SELECTOR => {
            faucet::handle_has_mint_role(&mut input.internals, calldata, input.gas)
        }
        // faucet state mutation
        #[cfg(feature = "faucet")]
        faucet::mintCall::SELECTOR => {
            faucet::handle_mint_faucet(&mut input.internals, calldata, input.caller, input.gas)
        }

        _ => Err(PrecompileError::Other("Unknown function selector".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::precompile_test_utils::*;
    use alloy::sol_types::{SolCall, SolEvent};
    use tn_config::GOVERNANCE_SAFE_ADDRESS;

    #[test]
    fn test_input_too_short() {
        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, vec![0xAB, 0xCD]);
        assert_not_success(&result);
    }

    #[test]
    fn test_unknown_selector() {
        let mut env = TestEnv::new();
        let mut data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        data.extend_from_slice(&[0u8; 32]);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_read_only_selector_rejects_value() {
        let mut env = TestEnv::new();
        let pool_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);
        let user_before = env.get_balance(USER);
        let result =
            env.exec_with_value(USER, totalSupplyCall {}.abi_encode(), 100_000, U256::from(1));
        assert_not_success(&result);
        // The journaled value transfer is reverted: neither balance moves.
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), pool_before);
        assert_eq!(env.get_balance(USER), user_before);
        // Positive control: the same call without value succeeds.
        let result =
            env.exec_with_value(USER, totalSupplyCall {}.abi_encode(), 100_000, U256::ZERO);
        assert_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_mutating_selector_rejects_value() {
        let mut env = TestEnv::new();
        let pool_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);
        let calldata = mintCall { amount: U256::from(500) }.abi_encode();
        let result =
            env.exec_with_value(GOVERNANCE_SAFE_ADDRESS, calldata.clone(), 100_000, U256::from(1));
        assert_not_success(&result);
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), pool_before);
        // Positive control: the same call without value succeeds.
        let result = env.exec_with_value(GOVERNANCE_SAFE_ADDRESS, calldata, 100_000, U256::ZERO);
        assert_success(&result);
    }

    #[test]
    fn test_burn_value_funds_pool_and_burns() {
        // The pool starts empty, as it does on mainnet: genesis allocates the account zero
        // balance with `0xfe` code, the nonzero code being what exempts it from EIP-158 state
        // clearing while the balance sits at zero.
        let one_tel = U256::from(10).pow(U256::from(18));
        let mut env = TestEnv::new_with_balances(one_tel, one_tel, U256::ZERO);
        let amount = U256::from(500);
        let gov_before = env.get_balance(GOVERNANCE_SAFE_ADDRESS);
        let supply_before = env.get_total_supply();
        let calldata = burnCall { amount }.abi_encode();
        // Negative control: with an empty pool and no value, the burn has nothing to destroy.
        assert_not_success(&env.exec_with_value(
            GOVERNANCE_SAFE_ADDRESS,
            calldata.clone(),
            100_000,
            U256::ZERO,
        ));
        // Attaching `amount` funds the pool and the handler destroys it in the same call.
        let result = env.exec_with_value(GOVERNANCE_SAFE_ADDRESS, calldata, 100_000, amount);
        assert_success(&result);
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), U256::ZERO);
        assert_eq!(env.get_balance(GOVERNANCE_SAFE_ADDRESS), gov_before - amount);
        assert_eq!(env.get_total_supply(), supply_before - amount);
    }

    #[test]
    fn test_value_funded_burn_mirrors_the_top_up_as_a_transfer() {
        let one_tel = U256::from(10).pow(U256::from(18));
        let mut env = TestEnv::new_with_balances(one_tel, one_tel, U256::ZERO);
        let value = U256::from(500);
        let amount = U256::from(200);
        let result = env.exec_with_value(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount }.abi_encode(),
            100_000,
            value,
        );
        let logs = extract_logs(&result);
        // Three logs: the mirrored top-up, then the burn pair. Without the first, an indexer
        // rebuilding balances from these events sees the pool pay out wei it never received.
        assert_eq!(logs.len(), 3);
        assert_eq!(
            logs[0].topics(),
            [
                burnable::Transfer::SIGNATURE_HASH,
                GOVERNANCE_SAFE_ADDRESS.into_word(),
                TELCOIN_PRECOMPILE_ADDRESS.into_word(),
            ]
        );
        // The mirror reports the attached value, not the burned amount.
        assert_eq!(U256::from_be_slice(logs[0].data.data.as_ref()), value);
        assert_eq!(logs[1].topics(), [burnable::Burn::SIGNATURE_HASH]);
        assert_eq!(
            logs[2].topics(),
            [
                burnable::Transfer::SIGNATURE_HASH,
                TELCOIN_PRECOMPILE_ADDRESS.into_word(),
                Address::ZERO.into_word(),
            ]
        );
        assert_eq!(U256::from_be_slice(logs[2].data.data.as_ref()), amount);
    }

    #[test]
    fn test_zero_value_burn_emits_no_inbound_transfer() {
        // The pool is pre-funded, so this burn moves no wei into the precompile.
        let mut env = TestEnv::new();
        let result = env.exec_with_value(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(100) }.abi_encode(),
            100_000,
            U256::ZERO,
        );
        let logs = extract_logs(&result);
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].topics(), [burnable::Burn::SIGNATURE_HASH]);
    }

    #[test]
    fn test_burn_excess_value_stays_in_pool() {
        let one_tel = U256::from(10).pow(U256::from(18));
        let mut env = TestEnv::new_with_balances(one_tel, one_tel, U256::ZERO);
        let supply_before = env.get_total_supply();
        let result = env.exec_with_value(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(200) }.abi_encode(),
            100_000,
            U256::from(500),
        );
        assert_success(&result);
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), U256::from(300));
        // The supply decreases by the burn amount, never by the attached value.
        assert_eq!(env.get_total_supply(), supply_before - U256::from(200));
    }

    #[test]
    fn test_delegatecall_burn_refused_with_and_without_value() {
        // DELEGATECALL relay that propagates failure: copies calldata, delegatecalls the
        // precompile, and reverts when the inner frame reverts (unlike the flag-popping relay
        // in the integration tests). The precompile sees the relay frame's apparent
        // `msg.value`, while no wei moves to the precompile address. The direct-call guard
        // refuses the indirect frame before dispatch, with or without attached value.
        const RELAY_CODE: &[u8] = &[
            0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATASIZE PUSH1 0 PUSH1 0 CALLDATACOPY
            0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00, // PUSH1 0 PUSH1 0 CALLDATASIZE PUSH1 0
            0x61, 0x07, 0xe1, 0x5a, 0xf4, // PUSH2 0x07e1 GAS DELEGATECALL
            0x3d, 0x60, 0x00, 0x60, 0x00,
            0x3e, // RETURNDATASIZE PUSH1 0 PUSH1 0 RETURNDATACOPY
            0x15, 0x60, 0x20, 0x57, // ISZERO PUSH1 0x20 JUMPI
            0x3d, 0x60, 0x00, 0xf3, // RETURNDATASIZE PUSH1 0 RETURN
            0x5b, 0x3d, 0x60, 0x00, 0xfd, // JUMPDEST RETURNDATASIZE PUSH1 0 REVERT
        ];
        const RELAY: Address = address!("4444444000000000000000000000000000000004");
        let mut env = TestEnv::new();
        env.deploy_code(RELAY, RELAY_CODE.into());
        let pool_before = env.get_balance(TELCOIN_PRECOMPILE_ADDRESS);
        let gov_before = env.get_balance(GOVERNANCE_SAFE_ADDRESS);
        let amount = U256::from(100);
        let calldata = burnCall { amount }.abi_encode();
        // Value-bearing: refused. Apparent value never funds the pool, and nothing moves.
        let result =
            env.exec_value_to(GOVERNANCE_SAFE_ADDRESS, RELAY, calldata.clone(), 200_000, amount);
        assert_not_success(&result);
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), pool_before);
        assert_eq!(env.get_balance(GOVERNANCE_SAFE_ADDRESS), gov_before);
        // Zero-value: refused just the same. The indirect frame never reaches dispatch, so
        // the pre-funded pool stays untouched.
        let result = env.exec_value_to(
            GOVERNANCE_SAFE_ADDRESS,
            RELAY,
            calldata.clone(),
            200_000,
            U256::ZERO,
        );
        assert_not_success(&result);
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), pool_before);
        // Positive control: the same burn as a direct call passes the gates and draws from
        // the pre-funded pool.
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, calldata, 200_000);
        assert_success(&result);
        assert_eq!(env.get_balance(TELCOIN_PRECOMPILE_ADDRESS), pool_before - amount);
    }
}
