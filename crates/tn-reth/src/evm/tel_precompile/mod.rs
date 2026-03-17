//! TEL erc20 precompile.
//!
//!
//! TODOOOOOOOOOOO
//!
//!
//! Telcoin precompile registered as a [`DynPrecompile`] inside a [`PrecompilesMap`].
//!
//! Storage layout at TELCOIN_PRECOMPILE_ADDRESS:
//!   keccak256(abi.encode(recipient, 0)) = pending mint amount
//!   keccak256(abi.encode(recipient, 1)) = unlock timestamp
//!   keccak256(abi.encode(spender, keccak256(abi.encode(owner, 2)))) = allowance
//!   keccak256(abi.encode(owner, 4)) = nonces (EIP-2612 permit nonces)
//!   slot 100 = totalSupply
use alloy::{primitives::address, sol_types::SolCall};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use reth_revm::precompile::{PrecompileError, PrecompileId, PrecompileResult};
use tn_types::{Address, U256};

mod burnable;
mod eip2612;
mod erc20;
#[cfg(feature = "faucet")]
mod faucet;
mod helpers;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
#[cfg(not(feature = "faucet"))]
pub use burnable::mintCall;
#[cfg(not(feature = "faucet"))]
pub use burnable::TIMELOCK_DURATION;
pub use burnable::{burnCall, claimCall, grantMintRoleCall, hasMintRoleCall, revokeMintRoleCall};
pub use eip2612::{noncesCall, permitCall, DOMAIN_SEPARATORCall};
pub use erc20::{
    allowanceCall, approveCall, balanceOfCall, decimalsCall, nameCall, symbolCall, totalSupplyCall,
    transferCall, transferFromCall,
};
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
pub fn add_telcoin_precompile(map: &mut PrecompilesMap) {
    map.extend_precompiles([(
        TELCOIN_PRECOMPILE_ADDRESS,
        DynPrecompile::new_stateful(PrecompileId::Custom("telcoin".into()), telcoin_precompile),
    )]);
}

/// Top-level dispatcher for the Telcoin precompile.
///
/// Extracts the 4-byte selector from calldata and routes to the matching handler.
/// State-mutating selectors are rejected when `input.is_static` is set (STATICCALL).
fn telcoin_precompile(mut input: PrecompileInput<'_>) -> PrecompileResult {
    if input.data.len() < 4 {
        return Err(PrecompileError::Other("Invalid input: too short".into()));
    }

    let selector: [u8; 4] = input.data[0..4].try_into().unwrap();
    let calldata = &input.data[4..];

    match selector {
        // State-mutating functions
        #[cfg(not(feature = "faucet"))]
        burnable::mintCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            burnable::handle_mint(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        faucet::mintCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            faucet::handle_mint_faucet(&mut input.internals, calldata, input.caller, input.gas)
        }
        burnable::claimCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            burnable::handle_claim(&mut input.internals, calldata, input.gas)
        }
        burnable::burnCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            burnable::handle_burn(&mut input.internals, calldata, input.caller, input.gas)
        }
        erc20::transferCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            erc20::handle_transfer(&mut input.internals, calldata, input.caller, input.gas)
        }
        erc20::approveCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            erc20::handle_approve(&mut input.internals, calldata, input.caller, input.gas)
        }
        erc20::transferFromCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            erc20::handle_transfer_from(&mut input.internals, calldata, input.caller, input.gas)
        }
        eip2612::permitCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            eip2612::handle_permit(&mut input.internals, calldata, input.gas)
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
            eip2612::handle_domain_separator(&mut input.internals, input.gas)
        }
        // Faucet feature: role management
        #[cfg(feature = "faucet")]
        grantMintRoleCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            faucet::handle_grant_mint_role(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        revokeMintRoleCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            faucet::handle_revoke_mint_role(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        hasMintRoleCall::SELECTOR => {
            faucet::handle_has_mint_role(&mut input.internals, calldata, input.gas)
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
