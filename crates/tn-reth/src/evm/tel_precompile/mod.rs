//! Native TEL ERC-20 precompile.
//!
//! Implements an ERC-20-compatible interface for Telcoin's native token directly at the EVM
//! precompile level. Unlike a standard Solidity contract, this precompile operates on **native
//! account balances** — `balanceOf(addr)` returns the same value as `addr.balance`. Token
//! transfers move native wei between accounts, making TEL simultaneously the chain's gas token
//! and its primary ERC-20.
//!
//! The precompile is registered as a [`DynPrecompile`] inside a [`PrecompilesMap`] at
//! [`TELCOIN_PRECOMPILE_ADDRESS`] (`0x7e1`). Any `CALL`/`STATICCALL` targeting that address is
//! intercepted and routed to [`telcoin_precompile`] instead of executing bytecode.
//!
//! # Module structure
//!
//! | Module       | Responsibility |
//! |--------------|----------------|
//! | [`erc20`]    | Standard ERC-20 view and transfer functions |
//! | [`eip2612`]  | EIP-2612 `permit` (gasless approvals) |
//! | [`burnable`] | Timelocked `mint` / `claim` / `burn` lifecycle (production) |
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
//!   functions. `claim` is also permissionless once the timelock expires.
//!
//! # Feature flags
//!
//! - **`faucet`**: Replaces the timelocked `mint(uint256)` with an instant `mint(address, uint256)`
//!   and adds role-management functions. **Must never be enabled in production builds** — it
//!   removes the timelock safety window.
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

/// Production `mint(uint256)` call type (timelocked, governance-only).
#[cfg(not(feature = "faucet"))]
pub use burnable::mintCall;
/// Production timelock duration constant (7 days).
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
/// Calls targeting this address are intercepted by the [`DynPrecompile`] registered via
/// [`add_telcoin_precompile`] and routed to the precompile dispatcher instead of executing EVM
/// bytecode.
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
