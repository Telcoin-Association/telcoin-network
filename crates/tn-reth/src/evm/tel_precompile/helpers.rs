//! Storage-slot computation utilities for the TEL precompile.
//!
//! Provides deterministic slot derivation following Solidity's storage layout for mappings.
use alloy_evm::EvmInternals;
use reth_revm::{precompile::PrecompileError, primitives::keccak256};
use tn_types::{Address, U256};

// --- Storage slot helpers ---
//
// All TEL precompile state is stored under TELCOIN_PRECOMPILE_ADDRESS using
// Solidity-style mapping layouts. Base slot indices:
//   0 → pending mint amounts  (mapping: address → uint256)
//   1 → unlock timestamps     (mapping: address → uint256)
//   3 → mint roles            (mapping: address → bool)  [faucet feature only]
// 100 → totalSupply           (plain slot)

/// Compute the storage slot for a recipient's pending mint amount.
///
/// Layout: `keccak256(abi.encode(recipient, 0))` — standard Solidity `mapping(address => uint256)`
/// at slot 0.
pub(super) fn amount_slot(recipient: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(recipient.as_slice());
    // slot index 0 is already zero in buf[32..64]
    U256::from_be_bytes(keccak256(buf).0)
}

/// Compute the storage slot for a recipient's unlock timestamp.
///
/// Layout: `keccak256(abi.encode(recipient, 1))` — `mapping(address => uint256)` at slot 1.
/// A non-zero value means a pending mint exists; the value is the earliest block timestamp
/// at which [`handle_claim`] will succeed.
pub(super) fn timestamp_slot(recipient: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(recipient.as_slice());
    buf[63] = 1; // slot index 1
    U256::from_be_bytes(keccak256(buf).0)
}

// --- Balance manipulation helpers ---

/// Increment the native balance of an address (used for minting).
pub(super) fn balance_incr(
    internals: &mut EvmInternals<'_>,
    addr: Address,
    amount: U256,
) -> Result<(), PrecompileError> {
    internals
        .balance_incr(addr, amount)
        .map_err(|e| PrecompileError::Other(format!("balance_incr failed: {e:?}").into()))
}

/// Decrement the native balance of an address (used for burning).
///
/// Returns `Ok(None)` on success, `Ok(Some(msg))` if the address has insufficient balance.
pub(super) fn balance_decr(
    internals: &mut EvmInternals<'_>,
    addr: Address,
    amount: U256,
) -> Result<Option<String>, PrecompileError> {
    if amount.is_zero() {
        return Ok(None);
    }
    let balance = internals
        .load_account(addr)
        .map_err(|e| PrecompileError::Other(format!("load_account failed: {e:?}").into()))?
        .data
        .info
        .balance;
    if balance < amount {
        return Ok(Some("insufficient balance".to_string()));
    }
    internals
        .set_balance(addr, balance - amount)
        .map_err(|e| PrecompileError::Other(format!("set_balance failed: {e:?}").into()))?;
    Ok(None)
}
