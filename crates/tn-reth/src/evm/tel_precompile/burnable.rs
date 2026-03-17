//! TOODDDOO
//!

use alloy::{
    sol,
    sol_types::SolEvent,
};
use alloy_evm::EvmInternals;
use reth_revm::precompile::{PrecompileError, PrecompileOutput, PrecompileResult};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Address, Bytes, U256};

use crate::{
    evm::tel_precompile::{
        erc20::Transfer,
        helpers::{address_to_topic, amount_slot, timestamp_slot},
        TOTAL_SUPPLY_SLOT,
    },
    TELCOIN_PRECOMPILE_ADDRESS,
};
#[cfg(feature = "faucet")]
use crate::evm::tel_precompile::faucet::mint_role_slot;

// ABI definitions for the Telcoin precompile's external interface.
//
// This generates selector constants and encoding/decoding types for each function and event.
// The interface combines a custom mint/claim/burn lifecycle with a standard ERC-20 surface,
// plus optional role-management functions gated behind the `faucet` feature.
//
// Security notes:
// - `mint` creates a pending mint subject to a timelock; it does NOT credit tokens immediately.
// - `claim` is permissionless — anyone can trigger it once the timelock expires.
// - `burn` destroys tokens held by the precompile account; only governance may call it.
// - `grantMintRole` / `revokeMintRole` are only compiled with `feature = "faucet"`.
sol! {
    function claim(address recipient) external;
    function burn(uint256 amount) external;
    function grantMintRole(address addr) external;
    function revokeMintRole(address addr) external;
    function hasMintRole(address addr) external view returns (bool);
    event Mint(address indexed recipient, uint256 amount, uint256 unlockTimestamp);
    event Claim(address indexed recipient, uint256 amount);
    event Burn(uint256 amount);
}

// only mint to governance safe
#[cfg(not(feature = "faucet"))]
sol! {
    function mint(uint256 amount) external;
}

/// Timelock duration applied to new mints before they can be claimed.
///
/// - **Production** (`!faucet`): 7 days (604 800 seconds). Provides a window for governance to
///   cancel malicious mints before tokens enter circulation.
/// - **Testnet / faucet** (`faucet`): 0 seconds. Allows instant claim for development convenience.
///
/// # Security invariant
/// This value is set at **compile time**. A production binary must never be built with
/// `feature = "faucet"` enabled, or the timelock protection is silently disabled.
#[cfg(not(feature = "faucet"))]
pub const TIMELOCK_DURATION: u64 = 7 * 24 * 60 * 60; // 604800s = 7 days

/// Check whether `caller` is authorized to invoke `mint`.
///
/// Returns `true` if **any** of the following hold:
/// 1. `caller == GOVERNANCE_SAFE_ADDRESS` (always, regardless of features).
/// 2. *(faucet only)* The mint-role storage slot for `caller` is non-zero, meaning governance
///    previously called `grantMintRole(caller)`.
///
/// # Security note
/// Without the `faucet` feature, only governance can mint. This is the production invariant.
pub(super) fn has_mint_role(
    #[cfg(feature = "faucet")] internals: &mut EvmInternals<'_>,
    #[cfg(not(feature = "faucet"))] _internals: &mut EvmInternals<'_>,
    caller: Address,
) -> Result<bool, PrecompileError> {
    if caller == GOVERNANCE_SAFE_ADDRESS {
        return Ok(true);
    }

    // faucet can have more than 1 mint
    #[cfg(feature = "faucet")]
    {
        let slot = mint_role_slot(caller);
        let val = internals
            .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
            .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
            .data;
        Ok(!val.is_zero())
    }

    #[cfg(not(feature = "faucet"))]
    Ok(false)
}

/// Check whether `caller` is authorized to invoke `burn`.
///
/// Only [`GOVERNANCE_SAFE_ADDRESS`] may burn. Unlike mint roles, burn authority is never
/// dynamically grantable — not even with the `faucet` feature.
pub(super) fn has_burn_role(caller: Address) -> bool {
    caller == GOVERNANCE_SAFE_ADDRESS
}

/// `mint(uint256 amount)` — creates a timelocked pending mint to governance.
///
/// Stores `amount` at the governance address's amount slot and
/// `block.timestamp + TIMELOCK_DURATION` at the timestamp slot.
/// **Does not credit the balance** — that happens in [`handle_claim`] after the timelock expires.
///
/// # Access control
/// Requires [`has_mint_role`] — only governance qualifies in production.
///
/// # Security notes
/// - The recipient is always [`GOVERNANCE_SAFE_ADDRESS`]; callers cannot choose a target.
/// - A second `mint` **overwrites** the previous pending amount and resets the timelock.
/// - Emits `Mint(recipient, amount, unlockTimestamp)`.
#[cfg(not(feature = "faucet"))] // see `handle_mint_faucet`
pub(super) fn handle_mint(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 41_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_mint_role(internals, caller)? {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("mint: expected 32 bytes (uint256)".into()));
    }

    // NOTE: allow mint to be `0` to replace pending mints during timelock
    let amount = U256::from_be_slice(&calldata[0..32]);
    let recipient = GOVERNANCE_SAFE_ADDRESS;

    // Compute unlock timestamp
    let current_ts = internals.block_timestamp();
    let unlock_ts = current_ts + U256::from(TIMELOCK_DURATION);

    // Store amount at keccak256(recipient, 0)
    let amt_slot = amount_slot(recipient);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, amt_slot, amount)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Store unlock timestamp at keccak256(recipient, 1)
    let ts_slot = timestamp_slot(recipient);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, ts_slot, unlock_ts)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Mint(address recipient, uint256 amount, uint256 unlockTimestamp)
    let topic0 = Mint::SIGNATURE_HASH;
    let mut log_data = Vec::with_capacity(64);
    log_data.extend_from_slice(&amount.to_be_bytes::<32>());
    log_data.extend_from_slice(&unlock_ts.to_be_bytes::<32>());

    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, address_to_topic(recipient)],
        log_data.into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Mint log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `claim(address recipient)` — finalizes a pending mint after the timelock expires.

///
/// Flow:
/// 1. Loads pending `amount` and `unlock_ts` from precompile storage.
/// 2. Verifies `block.timestamp >= unlock_ts` (timelock check).
/// 3. Credits `amount` to the recipient's **native balance** via `balance_incr`.
/// 4. Clears both storage slots (amount and timestamp) to prevent double-claim.
/// 5. Increments `totalSupply`.
/// 6. Emits `Claim(recipient, amount)` and `Transfer(address(0), recipient, amount)`.
///
/// # Access control
/// **Permissionless** — anyone can call `claim` on behalf of any recipient once the
/// timelock has passed.
pub(super) fn handle_claim(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 25_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("claim: expected 32 bytes (address)".into()));
    }

    let recipient = Address::from_slice(&calldata[12..32]);

    // Load pending amount
    let amt_slot = amount_slot(recipient);
    let amount = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, amt_slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    if amount.is_zero() {
        return Err(PrecompileError::Other("claim: no pending mint".into()));
    }

    // Load unlock timestamp
    let ts_slot = timestamp_slot(recipient);
    let unlock_ts = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, ts_slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    // Check timelock
    let current_ts = internals.block_timestamp();
    if current_ts < unlock_ts {
        return Err(PrecompileError::Other("claim: timelock not expired".into()));
    }

    // Credit recipient
    internals
        .balance_incr(recipient, amount)
        .map_err(|e| PrecompileError::Other(format!("balance_incr failed: {e:?}").into()))?;

    // Clear storage
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, amt_slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, ts_slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Increment totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    internals
        .sstore(
            TELCOIN_PRECOMPILE_ADDRESS,
            TOTAL_SUPPLY_SLOT,
            current_supply
                .checked_add(amount)
                .ok_or_else(|| PrecompileError::Other("claim: total supply overflow".into()))?,
        )
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Claim(address recipient, uint256 amount)
    let topic0 = Claim::SIGNATURE_HASH;
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, address_to_topic(recipient)],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Claim log".into()))?;
    internals.log(log);

    // Emit Transfer(address(0), recipient, amount) — ERC20 mint event
    let transfer_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![
            Transfer::SIGNATURE_HASH,
            address_to_topic(Address::ZERO),
            address_to_topic(recipient),
        ],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `burn(uint256 amount)` — destroys tokens held by the precompile account.
///
/// Transfers `amount` from the precompile's native balance to `address(0)` (effectively
/// destroying it), then decrements `totalSupply`.
///
/// # Access control
/// Governance-only via [`has_burn_role`].
pub(super) fn handle_burn(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 8_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_burn_role(caller) {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("burn: expected 32 bytes (uint256)".into()));
    }

    let amount = U256::from_be_slice(&calldata[0..32]);

    // Transfer from precompile to zero address (burn)
    let transfer_result = internals
        .transfer(TELCOIN_PRECOMPILE_ADDRESS, Address::ZERO, amount)
        .map_err(|e| PrecompileError::Other(format!("transfer failed: {e:?}").into()))?;

    if let Some(error) = transfer_result {
        return Err(PrecompileError::Other(format!("burn transfer error: {error:?}").into()));
    }

    // Decrement totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    let new_supply = current_supply
        .checked_sub(amount)
        .ok_or_else(|| PrecompileError::Other("burn: total supply underflow".into()))?;
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT, new_supply)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Burn(uint256 amount)
    let topic0 = Burn::SIGNATURE_HASH;
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Burn log".into()))?;
    internals.log(log);

    // Emit Transfer(precompile, address(0), amount) — ERC20 burn event
    let transfer_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![
            Transfer::SIGNATURE_HASH,
            address_to_topic(TELCOIN_PRECOMPILE_ADDRESS),
            address_to_topic(Address::ZERO),
        ],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::tel_precompile::test_utils::*;
    #[cfg(not(feature = "faucet"))]
    use crate::evm::tel_precompile::erc20::{balanceOfCall, totalSupplyCall};
    use alloy::sol_types::SolCall;
    use tn_config::GOVERNANCE_SAFE_ADDRESS;
    use tn_types::U256;

    #[test]
    fn test_mint_succeeds() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let data = mintCall { amount: U256::from(500) }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = crate::evm::tel_precompile::mintCall {
            recipient: RECIPIENT,
            amount: U256::from(500),
        }
        .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_before_timelock_halts() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_after_timelock_succeeds() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_success(&result);
    }

    #[test]
    fn test_burn_succeeds() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(200) }.abi_encode(),
        );
        assert_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_total_supply_after_claim_and_burn() {
        let mut env = TestEnv::new();
        let genesis = U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18));
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
        assert_eq!(decode_u256(&result), genesis + U256::from(500) - U256::from(200));
    }

    #[test]
    fn test_unauthorized_mint() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let data = mintCall { amount: U256::from(100) }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = crate::evm::tel_precompile::mintCall {
            recipient: RECIPIENT,
            amount: U256::from(100),
        }
        .abi_encode();
        let result = env.exec_default(USER, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_unauthorized_burn() {
        let mut env = TestEnv::new();
        let result = env.exec_default(USER, burnCall { amount: U256::from(100) }.abi_encode());
        assert_not_success(&result);
    }

    #[test]
    fn test_claim_no_pending_mint() {
        let mut env = TestEnv::new();
        let result = env
            .exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: RECIPIENT }.abi_encode());
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_already_claimed() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let claim_data = claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claim_data.clone());
        assert_success(&result);
        let result2 = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claim_data);
        assert_not_success(&result2);
    }

    #[test]
    fn test_burn_insufficient_balance() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(2000) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    fn test_mint_short_calldata() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let short = {
            let full = mintCall { amount: U256::from(500) }.abi_encode();
            full[..4 + 16].to_vec()
        };
        #[cfg(feature = "faucet")]
        let short = {
            let full = crate::evm::tel_precompile::mintCall {
                recipient: RECIPIENT,
                amount: U256::from(500),
            }
            .abi_encode();
            full[..4 + 32].to_vec()
        };
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_mint_oog() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let data = mintCall { amount: U256::from(500) }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = crate::evm::tel_precompile::mintCall {
            recipient: RECIPIENT,
            amount: U256::from(500),
        }
        .abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_oog() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let data = claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_burn_oog() {
        let mut env = TestEnv::new();
        let data = burnCall { amount: U256::from(100) }.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_double_mint_overwrites() {
        let mut env = TestEnv::new();
        let initial_balance = U256::from(10).pow(U256::from(18));
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(300) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), initial_balance + U256::from(300));
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_zero_amount_mint_does_not_overwrite_pending() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        // Zero-amount mint succeeds and cancels the pending mint
        let result =
            env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::ZERO }.abi_encode());
        assert_success(&result);
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        // Claim fails because pending was overwritten to zero
        let claim_result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_not_success(&claim_result);
    }
}
