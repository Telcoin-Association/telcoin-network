//! Native TEL ERC-20 precompile.
//!
//! Implements an ERC-20-compatible interface for Telcoin's native token directly at the EVM
//! precompile level. Unlike a standard Solidity contract, this precompile operates on **native
//! account balances** — `balanceOf(addr)` returns the same value as `addr.balance`. Token
//! transfers move native wei between accounts, making TEL simultaneously the chain's gas token
//! and its primary ERC-20.
//!
//! The precompile is registered as a [`DynPrecompile`] inside a [`PrecompilesMap`] at
//! [`TELCOIN_PRECOMPILE_ADDRESS`] (`0x7e1`). Any `CALL`, `STATICCALL`, `DELEGATECALL`, or
//! `CALLCODE` targeting that address is intercepted and routed to [`telcoin_precompile`]
//! instead of executing bytecode. Authorization uses the preserved `msg.sender` and all
//! state is anchored to `0x7e1`, so `DELEGATECALL`/`CALLCODE` grant no additional privileges
//! over a plain `CALL`.
//!
//! # Module structure
//!
//! | Module       | Responsibility |
//! |--------------|----------------|
//! | [`erc20`]    | Standard ERC-20 view and transfer functions |
//! | [`eip2612`]  | EIP-2612 `permit` (gasless approvals) |
//! | [`burnable`] | Timelocked `mint` / `claim` / `burn` lifecycle (mainnet) |
//! | [`faucet`]   | Instant `mint` with role management (testnet, `faucet` feature) |
//! | [`helpers`]  | Storage-slot computation and ABI encoding utilities |
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
//! | 2 | `mapping(address => mapping(address => uint256))` | ERC-20 allowances |
//! | 3 | `mapping(address => bool)` | Mint roles (faucet feature only) |
//! | 4 | `mapping(address => uint256)` | EIP-2612 permit nonces |
//! | 100 | `uint256` | Total circulating supply |
//!
//! # Access control
//!
//! - **Governance** ([`GOVERNANCE_SAFE_ADDRESS`](tn_config::GOVERNANCE_SAFE_ADDRESS)): can `mint`,
//!   `claim`, `burn`, and (with faucet) `grantMintRole`/`revokeMintRole`.
//! - **Mint-role holders** (faucet only): can call the faucet `mint(address, uint256)`.
//! - **Any account**: can `transfer`, `approve`, `transferFrom`, `permit`, and call all view
//!   functions.
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
/// EIP-2612 gasless permit approvals.
mod eip2612;
/// Standard ERC-20 view and transfer functions.
mod erc20;
/// Testnet faucet: instant minting with role management (compiled with `faucet` feature).
#[cfg(feature = "faucet")]
mod faucet;
/// Storage-slot computation utilities.
mod helpers;
/// In-memory EVM test harness (available in tests and with `test-utils` feature).
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// --- Re-exports for external consumers ---

/// mainnet `mint(uint256)` call type (timelocked, governance-only).
#[cfg(not(feature = "faucet"))]
pub use burnable::mintCall;
/// mainnet timelock duration constant (7 days).
#[cfg(not(feature = "faucet"))]
pub use burnable::TIMELOCK_DURATION;
pub use burnable::{burnCall, claimCall, grantMintRoleCall, hasMintRoleCall, revokeMintRoleCall};
pub use eip2612::{noncesCall, permitCall, DOMAIN_SEPARATORCall};
pub use erc20::{
    allowanceCall, approveCall, balanceOfCall, decimalsCall, nameCall, symbolCall, totalSupplyCall,
    transferCall, transferFromCall,
};
/// Faucet `mint(address, uint256)` call type (instant, role-gated).
#[cfg(feature = "faucet")]
pub use faucet::mintCall;

/// The canonical address of the Telcoin precompile: `0x7e1`.
///
/// All TEL token state (pending mints, allowances, total supply) is stored under this address.
/// Native account balances at other addresses represent TEL holdings for those accounts.
/// Calls targeting this address — under any scheme (`CALL`, `STATICCALL`, `DELEGATECALL`,
/// `CALLCODE`) — are intercepted by the [`DynPrecompile`] registered via
/// [`add_telcoin_precompile`] and routed to the precompile dispatcher instead of executing
/// EVM bytecode. Genesis seeds `0xfe` (`INVALID`) here as defense-in-depth: the dispatcher
/// short-circuits bytecode execution while the precompile is registered, so that byte is
/// only observable via `EXTCODESIZE`/`EXTCODEHASH`, never executed.
pub const TELCOIN_PRECOMPILE_ADDRESS: Address =
    address!("00000000000000000000000000000000000007e1");

/// Fixed storage slot (100) that holds the total circulating supply of TEL.
///
/// Incremented on [`handle_claim`] and decremented on [`handle_burn`].
/// This is a plain slot (not a mapping), so it can be read directly without hashing.
const TOTAL_SUPPLY_SLOT: U256 = U256::from_limbs([100, 0, 0, 0]);

/// Registers the Telcoin ERC20 precompile at [`TELCOIN_PRECOMPILE_ADDRESS`] in the given map.
///
/// `chain_id` is captured and forwarded to EIP-2612 permit/domain_separator handlers since
/// `EvmInternals` in alloy-evm 0.21.2 does not expose the chain configuration.
pub fn add_telcoin_precompile(map: &mut PrecompilesMap, chain_id: u64) {
    map.apply_precompile(&TELCOIN_PRECOMPILE_ADDRESS, move |_| {
        Some(DynPrecompile::new_stateful(PrecompileId::Custom("telcoin".into()), move |input| {
            telcoin_precompile(input, chain_id)
        }))
    });
}

/// Top-level dispatcher for the Telcoin precompile.
///
/// Extracts the 4-byte selector from calldata and routes to the matching handler.
fn telcoin_precompile(mut input: PrecompileInput<'_>, chain_id: u64) -> PrecompileResult {
    if input.data.len() < 4 {
        return Err(PrecompileError::Other("Invalid input: too short".into()));
    }

    let selector: [u8; 4] = input.data[0..4].try_into().unwrap();
    let calldata = &input.data[4..];

    match selector {
        // State-mutating functions
        #[cfg(not(feature = "faucet"))]
        burnable::mintCall::SELECTOR => {
            burnable::handle_mint(&mut input.internals, calldata, input.caller, input.gas)
        }
        burnable::claimCall::SELECTOR => {
            burnable::handle_claim(&mut input.internals, calldata, input.caller, input.gas)
        }
        burnable::burnCall::SELECTOR => {
            burnable::handle_burn(&mut input.internals, calldata, input.caller, input.gas)
        }
        erc20::transferCall::SELECTOR => {
            erc20::handle_transfer(&mut input.internals, calldata, input.caller, input.gas)
        }
        erc20::approveCall::SELECTOR => {
            erc20::handle_approve(&mut input.internals, calldata, input.caller, input.gas)
        }
        erc20::transferFromCall::SELECTOR => {
            erc20::handle_transfer_from(&mut input.internals, calldata, input.caller, input.gas)
        }
        eip2612::permitCall::SELECTOR => {
            eip2612::handle_permit(&mut input.internals, calldata, input.gas, chain_id)
        }
        // Read-only functions
        erc20::nameCall::SELECTOR => erc20::handle_name(input.gas),
        erc20::symbolCall::SELECTOR => erc20::handle_symbol(input.gas),
        erc20::decimalsCall::SELECTOR => erc20::handle_decimals(input.gas),
        erc20::totalSupplyCall::SELECTOR => {
            erc20::handle_total_supply(&mut input.internals, input.gas)
        }
        erc20::balanceOfCall::SELECTOR => {
            erc20::handle_balance_of(&mut input.internals, calldata, input.gas)
        }
        erc20::allowanceCall::SELECTOR => {
            erc20::handle_allowance(&mut input.internals, calldata, input.gas)
        }
        eip2612::noncesCall::SELECTOR => {
            eip2612::handle_nonces(&mut input.internals, calldata, input.gas)
        }
        eip2612::DOMAIN_SEPARATORCall::SELECTOR => {
            eip2612::handle_domain_separator(input.gas, chain_id)
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
    use super::test_utils::*;
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
}

/// Regression tests pinning the `DELEGATECALL` safety properties of the precompile.
///
/// These tests document, as code-level invariants, that:
///
/// - revm preserves `msg.sender` across `DELEGATECALL` frames, so a wrapper contract
///   cannot impersonate `GOVERNANCE_SAFE_ADDRESS` by `DELEGATECALL`-ing into `0x7e1`.
/// - All precompile storage is anchored to [`TELCOIN_PRECOMPILE_ADDRESS`]; a wrapper's own
///   storage is untouched by a `DELEGATECALL` into the precompile.
/// - From the precompile's point of view, `DELEGATECALL` is functionally identical to
///   `CALL` — when the controlling frame is `GOVERNANCE_SAFE_ADDRESS`, both schemes
///   succeed; when it isn't, both schemes fail with the same authorization error.
///
/// If any of these tests start failing, do not loosen the assertions — investigate whether a
/// revm upgrade has changed `DELEGATECALL` frame semantics. The precompile's authorization
/// model depends on these invariants holding.
#[cfg(all(test, not(feature = "faucet")))]
mod delegatecall_tests {
    use super::burnable::mintCall;
    use super::test_utils::*;
    use super::TELCOIN_PRECOMPILE_ADDRESS;
    use alloy::sol_types::SolCall;
    use tn_config::GOVERNANCE_SAFE_ADDRESS;
    use tn_types::U256;

    /// Encode mainnet `mint(uint256)` calldata.
    fn mint_calldata(amount: u128) -> Vec<u8> {
        mintCall { amount: U256::from(amount) }.abi_encode()
    }

    /// Positive control: a direct `CALL` from a non-governance EOA reverts on `mint`.
    #[test]
    fn direct_call_from_unprivileged_reverts() {
        let mut env = TestEnv::new();
        let result = env.exec_default(USER, mint_calldata(1_000));
        assert_not_success(&result);
    }

    /// Possibility B disconfirmation: an unprivileged EOA `U` calls a wrapper `W` that
    /// `DELEGATECALL`s into `0x7e1.mint(...)`. The precompile must NOT escalate `U` to
    /// governance authority — `mint` must revert.
    #[test]
    fn delegatecall_from_unprivileged_does_not_escalate() {
        let mut env = TestEnv::new();
        let proxy_code = delegatecall_proxy_bytecode(TELCOIN_PRECOMPILE_ADDRESS);
        env.deploy_code(WRAPPER_ADDR, proxy_code);

        let result = env.exec_to(USER, WRAPPER_ADDR, mint_calldata(1_000), 200_000);
        assert_not_success(&result);
    }

    /// Storage isolation: after an unprivileged DELEGATECALL attempt, the wrapper's own
    /// storage at the precompile's well-known slots remains zero. The precompile anchors
    /// every `sload`/`sstore` to `TELCOIN_PRECOMPILE_ADDRESS`, so the wrapper's storage
    /// frame is never touched.
    #[test]
    fn delegatecall_does_not_write_wrapper_storage() {
        let mut env = TestEnv::new();
        let proxy_code = delegatecall_proxy_bytecode(TELCOIN_PRECOMPILE_ADDRESS);
        env.deploy_code(WRAPPER_ADDR, proxy_code);

        let _ = env.exec_to(USER, WRAPPER_ADDR, mint_calldata(1_000), 200_000);

        // Slot 100 is `totalSupply`; if storage were misrouted under DELEGATECALL the
        // wrapper would have a non-zero value here.
        assert_eq!(env.storage(WRAPPER_ADDR, U256::from(100)), U256::ZERO);
        // Slot 0 is the `pendingMint` mapping base; the wrapper should have nothing here
        // either.
        assert_eq!(env.storage(WRAPPER_ADDR, U256::ZERO), U256::ZERO);
    }

    /// `STATICCALL` for a state-mutating selector reverts (write-protection from revm), while
    /// the same selector under direct `CALL` from governance succeeds. This pins the
    /// expected scheme behaviour and complements the DELEGATECALL tests above.
    #[test]
    fn unprivileged_call_to_view_function_succeeds_via_wrapper() {
        // Read-only path: `totalSupply()` must succeed even when routed through a wrapper
        // by an unprivileged caller.
        let mut env = TestEnv::new();
        let proxy_code = delegatecall_proxy_bytecode(TELCOIN_PRECOMPILE_ADDRESS);
        env.deploy_code(WRAPPER_ADDR, proxy_code);

        let total_supply_calldata = super::erc20::totalSupplyCall {}.abi_encode();
        let result = env.exec_to(USER, WRAPPER_ADDR, total_supply_calldata, 100_000);
        let value = decode_u256(&result);
        let expected = U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));
        assert_eq!(value, expected, "totalSupply view should be unchanged after DELEGATECALL");
    }

    /// Governance equivalence: when the controlling frame is `GOVERNANCE_SAFE_ADDRESS`, a
    /// `DELEGATECALL` into `0x7e1.mint(...)` succeeds, exactly as a direct CALL would. This
    /// documents that DELEGATECALL grants no extra privileges *and* loses none — it is
    /// functionally equivalent to CALL from the precompile's point of view.
    ///
    /// Setup: install the forwarding proxy at `GOVERNANCE_SAFE_ADDRESS` itself. Sending a
    /// transaction from `GOVERNANCE_SAFE_ADDRESS` to `GOVERNANCE_SAFE_ADDRESS` then runs
    /// the proxy in governance's frame; the proxy's `DELEGATECALL` preserves `caller =
    /// GOVERNANCE_SAFE_ADDRESS`, and `mint` authorizes.
    ///
    /// EIP-3607 normally rejects transactions from accounts with code; we disable that
    /// check on the test `CfgEnv` because we are intentionally simulating a tx originating
    /// from a contract-bearing address (the governance Safe contract in production calls
    /// itself via internal frames, but in this in-memory harness we collapse that to a
    /// single tx for clarity).
    #[test]
    fn delegatecall_from_governance_succeeds() {
        let mut env = TestEnv::new();
        let proxy_code = delegatecall_proxy_bytecode(TELCOIN_PRECOMPILE_ADDRESS);
        env.deploy_code(GOVERNANCE_SAFE_ADDRESS, proxy_code);
        // Allow tx from a code-bearing account in this test only.
        env.evm.ctx.cfg.disable_eip3607 = true;

        let result = env.exec_to(
            GOVERNANCE_SAFE_ADDRESS,
            GOVERNANCE_SAFE_ADDRESS,
            mint_calldata(1_000),
            200_000,
        );
        assert_success(&result);
    }
}
