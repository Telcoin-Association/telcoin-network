//! Standard ERC-20 view and state-mutating functions for the TEL precompile.
//!
//! Provides `name`, `symbol`, `decimals`, `totalSupply`, `balanceOf`, `allowance`,
//! `transfer`, `approve`, and `transferFrom`. Balances are stored as **native account
//! balances** (not in precompile storage), while allowances and total supply use precompile
//! storage slots.
//!
//! Emits standard `Transfer` and `Approval` events from [`TELCOIN_PRECOMPILE_ADDRESS`].
use alloy::{
    primitives::{Address, Bytes, U256},
    sol,
    sol_types::{SolEvent, SolValue},
};
use alloy_evm::EvmInternals;
use revm::precompile::{PrecompileError, PrecompileOutput, PrecompileResult};

use crate::{
    helpers::{allowance_slot, transfer_balance},
    TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT,
};

// Standard ERC-20 ABI definitions.
//
// Generates selector constants and Rust encoding/decoding types for the ERC-20 interface.
sol! {
    /// Returns the token name: `"Telcoin"`.
    function name() external view returns (string);
    /// Returns the token symbol: `"TEL"`.
    function symbol() external view returns (string);
    /// Returns the number of decimals: `18`.
    function decimals() external view returns (uint8);
    /// Returns the total circulating supply of TEL (from storage slot 100).
    function totalSupply() external view returns (uint256);
    /// Returns the native account balance of `account` (TEL = native token).
    function balanceOf(address account) external view returns (uint256);
    /// Transfer `amount` TEL from caller to `to`.
    function transfer(address to, uint256 amount) external returns (bool);
    /// Set the allowance for `spender` to spend caller's tokens.
    function approve(address spender, uint256 amount) external returns (bool);
    /// Transfer `amount` TEL from `from` to `to`, spending caller's allowance.
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    /// Returns the remaining allowance that `spender` can spend on behalf of `owner`.
    function allowance(address owner, address spender) external view returns (uint256);
    /// Emitted on `transfer` and `transferFrom`.
    event Transfer(address indexed from, address indexed to, uint256 value);
    /// Emitted on `approve` and `permit`.
    event Approval(address indexed owner, address indexed spender, uint256 value);
}

// --- ERC20 view handlers ---

/// `name()` -> returns ABI-encoded `"Telcoin"`. Pure; no storage access.
pub(crate) fn handle_name(gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 200;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from("Telcoin".abi_encode())))
}

/// `symbol()` -> returns ABI-encoded `"TEL"`. Pure; no storage access.
pub(crate) fn handle_symbol(gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 200;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from("TEL".abi_encode())))
}

/// `decimals()` -> returns ABI-encoded `18`. Pure; no storage access.
pub(crate) fn handle_decimals(gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 200;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(U256::from(18).abi_encode())))
}

/// `totalSupply()` -> reads [`TOTAL_SUPPLY_SLOT`] (slot 100) and returns the current circulating
/// supply.
pub(crate) fn handle_total_supply(
    internals: &mut EvmInternals<'_>,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    let supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(supply.abi_encode())))
}

/// `balanceOf(address)` -> returns the **native account balance** of the given address.
///
/// TEL balances are stored as native ether-equivalent balances, not in precompile storage.
/// This means `balanceOf` loads the account record, not a storage slot.
pub(crate) fn handle_balance_of(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_600;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("balanceOf: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let balance = internals
        .load_account(addr)
        .map_err(|e| PrecompileError::Other(format!("load_account failed: {e:?}").into()))?
        .data
        .info
        .balance;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(balance.abi_encode())))
}

/// `allowance(address owner, address spender)` -> reads the ERC-20 allowance from precompile
/// storage.
pub(crate) fn handle_allowance(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other(
            "allowance: expected 64 bytes (address,address)".into(),
        ));
    }
    let owner = Address::from_slice(&calldata[12..32]);
    let spender = Address::from_slice(&calldata[44..64]);
    let slot = allowance_slot(owner, spender);
    let value = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(value.abi_encode())))
}

// --- ERC20 state-mutating handlers ---

/// `transfer(address to, uint256 amount)` -> moves native balance from `caller` to `to`.
///
/// Emits `Transfer(caller, to, amount)`. Returns ABI-encoded `true` on success.
/// Fails if `caller`'s balance is insufficient.
///
/// Allow transfers to this address for `burn`.
pub(crate) fn handle_transfer(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 12_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other("transfer: expected 64 bytes (address,uint256)".into()));
    }

    let to = Address::from_slice(&calldata[12..32]);
    let amount = U256::from_be_slice(&calldata[32..64]);

    if to == Address::ZERO {
        return Err(PrecompileError::Other("transfer: cannot transfer to address(0)".into()));
    }

    // Transfer native balance
    if let Some(error) = transfer_balance(internals, caller, to, amount)? {
        return Err(PrecompileError::Other(format!("transfer error: {error}").into()));
    }

    // Emit Transfer(from, to, value)
    let log = revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Transfer::SIGNATURE_HASH, caller.into_word(), to.into_word()],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(true.abi_encode())))
}

/// `approve(address spender, uint256 amount)` -> sets allowance for `spender` to spend `caller`'s
/// tokens.
///
/// Overwrites any existing allowance (no incremental add/sub). Emits `Approval(caller, spender,
/// amount)`.
///
/// # Security note
/// Subject to the classic ERC-20 approve front-running race. Callers should set allowance
/// to 0 before setting a new non-zero value, or use `transferFrom` patterns that don't rely
/// on allowance deltas.
pub(crate) fn handle_approve(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 22_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other("approve: expected 64 bytes (address,uint256)".into()));
    }

    let spender = Address::from_slice(&calldata[12..32]);
    if spender == Address::ZERO {
        return Err(PrecompileError::Other("approve: cannot approve address(0)".into()));
    }
    let amount = U256::from_be_slice(&calldata[32..64]);

    // Store allowance
    let slot = allowance_slot(caller, spender);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, amount)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Approval(owner, spender, value)
    let log = revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Approval::SIGNATURE_HASH, caller.into_word(), spender.into_word()],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Approval log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(true.abi_encode())))
}

/// `transferFrom(address from, address to, uint256 amount)` -> spends allowance and transfers.
///
/// 1. Loads `allowance[from][caller]` and verifies it is `>= amount`.
/// 2. Decrements the allowance by `amount` (skipped for infinite `U256::MAX` allowance).
/// 3. Transfers native balance from `from` to `to`.
/// 4. Emits `Transfer(from, to, amount)`.
///
/// Fails with "insufficient allowance" or a transfer error if balance is too low.
pub(crate) fn handle_transfer_from(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 35_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 96 {
        return Err(PrecompileError::Other(
            "transferFrom: expected 96 bytes (address,address,uint256)".into(),
        ));
    }

    let from = Address::from_slice(&calldata[12..32]);
    let to = Address::from_slice(&calldata[44..64]);
    let amount = U256::from_be_slice(&calldata[64..96]);

    if to == Address::ZERO {
        return Err(PrecompileError::Other("transferFrom: cannot transfer to address(0)".into()));
    }

    // Check allowance
    let slot = allowance_slot(from, caller);
    let current_allowance = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    if current_allowance < amount {
        return Err(PrecompileError::Other("transferFrom: insufficient allowance".into()));
    }

    // Decrement allowance (skip for infinite approval)
    if current_allowance != U256::MAX {
        let new_allowance = current_allowance - amount;
        internals
            .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, new_allowance)
            .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    };

    // Transfer native balance
    if let Some(error) = transfer_balance(internals, from, to, amount)? {
        return Err(PrecompileError::Other(format!("transferFrom transfer error: {error}").into()));
    }

    // Emit Transfer(from, to, value)
    let log = revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Transfer::SIGNATURE_HASH, from.into_word(), to.into_word()],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(true.abi_encode())))
}
