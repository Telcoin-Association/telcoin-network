//! Testnet faucet: instant minting with dynamic role management.
//!
//! Compiled only with `feature = "faucet"`. Replaces the timelocked `mint(uint256)` from
//! [`burnable`](super::burnable) with `mint(address, uint256)` that directly credits the
//! recipient's native balance -- no pending state, no timelock, no separate `claim` step.
//!
//! Also provides governance-controlled role management:
//! - `grantMintRole(address)` -- allows an address to call `mint`.
//! - `revokeMintRole(address)` -- removes that permission.
//! - `hasMintRole(address)` -- read-only query.
//!
//! # Security warning
//! This module **must never be enabled in mainnet**. It removes the 7-day timelock
//! that protects against malicious mints.
use alloy::{
    primitives::{Address, Bytes, U256},
    sol,
    sol_types::{SolEvent, SolValue},
};
use alloy_evm::EvmInternals;
use alloy::primitives::keccak256;
use revm::precompile::{PrecompileError, PrecompileOutput, PrecompileResult};

use crate::{
    burnable::{has_mint_role, Mint},
    constants::GOVERNANCE_SAFE_ADDRESS,
    erc20::Transfer,
    helpers::balance_incr,
    TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT,
};

// Faucet `mint` ABI: takes a recipient and amount, credits instantly without timelock.
sol! {
    /// Instantly mint `amount` tokens to `recipient`. Requires mint role.
    function mint(address recipient, uint256 amount) external;
}

/// Compute the storage slot for a dynamically granted mint role.
///
/// Layout: `keccak256(abi.encode(address, 3))` -- `mapping(address => bool)` at slot 3.
/// Only compiled with `feature = "faucet"`. A non-zero value means the address may call `mint`.
pub(crate) fn mint_role_slot(addr: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(addr.as_slice());
    buf[63] = 3; // base slot 3
    U256::from_be_bytes(keccak256(buf).0)
}

/// `mint(address recipient, uint256 amount)` -- faucet variant that directly credits the recipient.
///
/// Unlike [`handle_mint`], this does **not** create pending state or require a separate
/// [`handle_claim`] call. The recipient's native balance is incremented immediately and
/// `totalSupply` is updated in the same call.
///
/// # Emits
/// - `Mint(recipient, amount, unlockTimestamp=0)`
/// - `Transfer(address(0), recipient, amount)`
pub(crate) fn handle_mint_faucet(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 30_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_mint_role(internals, caller)? {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other("mint: expected 64 bytes (address, uint256)".into()));
    }

    let amount = U256::from_be_slice(&calldata[32..64]);
    if amount.is_zero() {
        return Err(PrecompileError::Other("mint: amount must be greater than zero".into()));
    }

    let recipient = Address::from_slice(&calldata[12..32]);

    // Directly credit recipient's native balance
    balance_incr(internals, recipient, amount)?;

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
                .ok_or_else(|| PrecompileError::Other("mint: total supply overflow".into()))?,
        )
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Mint(address recipient, uint256 amount, uint256 unlockTimestamp=0)
    let topic0 = Mint::SIGNATURE_HASH;
    let mut log_data = Vec::with_capacity(64);
    log_data.extend_from_slice(&amount.to_be_bytes::<32>());
    log_data.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());

    let log = revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, recipient.into_word()],
        log_data.into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Mint log".into()))?;
    internals.log(log);

    // Emit Transfer(address(0), recipient, amount) -- ERC20 mint event
    let transfer_log = revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Transfer::SIGNATURE_HASH, Address::ZERO.into_word(), recipient.into_word()],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `grantMintRole(address)` -- governance-only. Writes `1` to the mint-role storage slot,
/// enabling `addr` to call `mint`.
pub(crate) fn handle_grant_mint_role(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 22_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if caller != GOVERNANCE_SAFE_ADDRESS {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("grantMintRole: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let slot = mint_role_slot(addr);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, U256::from(1))
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `revokeMintRole(address)` -- governance-only. Writes `0` to the mint-role storage slot,
/// revoking `addr`'s ability to call `mint`.
pub(crate) fn handle_revoke_mint_role(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 22_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if caller != GOVERNANCE_SAFE_ADDRESS {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("revokeMintRole: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let slot = mint_role_slot(addr);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `hasMintRole(address)` -- read-only query. Returns `true` if `addr` is governance or has
/// been dynamically granted the mint role.
pub(crate) fn handle_has_mint_role(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("hasMintRole: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let has_role = has_mint_role(internals, addr)?;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(has_role.abi_encode())))
}
