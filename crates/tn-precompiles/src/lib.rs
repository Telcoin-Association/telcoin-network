//! Native TEL ERC-20 precompile.
//!
//! Implements an ERC-20-compatible interface for Telcoin's native token directly at the EVM
//! precompile level. Unlike a standard Solidity contract, this precompile operates on **native
//! account balances** -- `balanceOf(addr)` returns the same value as `addr.balance`. Token
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
//! | [`burnable`] | Timelocked `mint` / `claim` / `burn` lifecycle (mainnet) |
//! | [`faucet`]   | Instant `mint` with role management (testnet, `faucet` feature) |
//! | [`helpers`]  | Storage-slot computation and ABI encoding utilities |
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
//! - **Governance** ([`GOVERNANCE_SAFE_ADDRESS`]): can `mint`,
//!   `claim`, `burn`, and (with faucet) `grantMintRole`/`revokeMintRole`.
//! - **Mint-role holders** (faucet only): can call the faucet `mint(address, uint256)`.
//! - **Any account**: can `transfer`, `approve`, `transferFrom`, `permit`, and call all view
//!   functions.
//!
//! # Feature flags
//!
//! - **`faucet`**: Replaces the timelocked `mint(uint256)` with an instant `mint(address, uint256)`
//!   and adds role-management functions. **Must never be enabled in mainnet builds** -- it removes
//!   the timelock safety window.
use alloy::{primitives::U256, sol_types::SolCall};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use revm::precompile::{PrecompileError, PrecompileId, PrecompileResult};

/// Constants for the Telcoin precompile.
pub mod constants;
/// Timelocked mint/claim lifecycle and token burning.
pub mod burnable;
/// EIP-2612 gasless permit approvals.
pub mod eip2612;
/// Standard ERC-20 view and transfer functions.
pub mod erc20;
/// Testnet faucet: instant minting with role management (compiled with `faucet` feature).
#[cfg(feature = "faucet")]
pub mod faucet;
/// Gas penalty calculation for inefficient gas limit estimation.
pub mod gas_penalty;
/// Telcoin Network hardfork definitions.
pub mod hardfork;
/// Storage-slot computation utilities.
pub mod helpers;

// --- Re-exports for external consumers ---

pub use constants::{
    GOVERNANCE_SAFE_ADDRESS, SYSTEM_ADDRESS, TELCOIN_PRECOMPILE_ADDRESS, basefee_address,
    set_basefee_address,
};
pub use gas_penalty::calculate_gas_penalty;
pub use hardfork::TelcoinHardfork;

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

/// Fixed storage slot (100) that holds the total circulating supply of TEL.
///
/// Incremented on [`handle_claim`] and decremented on [`handle_burn`].
/// This is a plain slot (not a mapping), so it can be read directly without hashing.
pub(crate) const TOTAL_SUPPLY_SLOT: U256 = U256::from_limbs([100, 0, 0, 0]);

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
