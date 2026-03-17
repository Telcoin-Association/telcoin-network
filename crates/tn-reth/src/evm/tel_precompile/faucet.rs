//! Testnet faucet: instant minting with dynamic role management.
//!
//! Compiled only with `feature = "faucet"`. Replaces the timelocked `mint(uint256)` from
//! [`burnable`](super::burnable) with `mint(address, uint256)` that directly credits the
//! recipient's native balance — no pending state, no timelock, no separate `claim` step.
//!
//! Also provides governance-controlled role management:
//! - `grantMintRole(address)` — allows an address to call `mint`.
//! - `revokeMintRole(address)` — removes that permission.
//! - `hasMintRole(address)` — read-only query.
//!
//! # Security warning
//! This module **must never be enabled in production**. It removes the 7-day timelock
//! that protects against malicious mints.
use alloy::{
    sol,
    sol_types::SolEvent,
};
use alloy_evm::EvmInternals;
use reth_revm::{
    precompile::{PrecompileError, PrecompileOutput, PrecompileResult},
    primitives::keccak256,
};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Address, Bytes, U256};

use crate::{
    evm::tel_precompile::{
        burnable::{has_mint_role, Mint},
        erc20::Transfer,
        helpers::{abi_encode_bool, address_to_topic},
        TOTAL_SUPPLY_SLOT,
    },
    TELCOIN_PRECOMPILE_ADDRESS,
};

// Faucet `mint` ABI: takes a recipient and amount, credits instantly without timelock.
sol! {
    /// Instantly mint `amount` tokens to `recipient`. Requires mint role.
    function mint(address recipient, uint256 amount) external;
}

/// Compute the storage slot for a dynamically granted mint role.
///
/// Layout: `keccak256(abi.encode(address, 3))` — `mapping(address => bool)` at slot 3.
/// Only compiled with `feature = "faucet"`. A non-zero value means the address may call `mint`.
pub(super) fn mint_role_slot(addr: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(addr.as_slice());
    buf[63] = 3; // base slot 3
    U256::from_be_bytes(keccak256(buf).0)
}

/// `mint(address recipient, uint256 amount)` — faucet variant that directly credits the recipient.
///
/// Unlike [`handle_mint`], this does **not** create pending state or require a separate
/// [`handle_claim`] call. The recipient's native balance is incremented immediately and
/// `totalSupply` is updated in the same call.
///
/// # Emits
/// - `Mint(recipient, amount, unlockTimestamp=0)`
/// - `Transfer(address(0), recipient, amount)`
pub(super) fn handle_mint_faucet(
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
    internals
        .balance_incr(recipient, amount)
        .map_err(|e| PrecompileError::Other(format!("balance_incr failed: {e:?}").into()))?;

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

    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, address_to_topic(recipient)],
        log_data.into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Mint log".into()))?;
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

/// `grantMintRole(address)` — governance-only. Writes `1` to the mint-role storage slot,
/// enabling `addr` to call `mint`.
pub(super) fn handle_grant_mint_role(
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

/// `revokeMintRole(address)` — governance-only. Writes `0` to the mint-role storage slot,
/// revoking `addr`'s ability to call `mint`.
pub(super) fn handle_revoke_mint_role(
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

/// `hasMintRole(address)` — read-only query. Returns `true` if `addr` is governance or has
/// been dynamically granted the mint role.
pub(super) fn handle_has_mint_role(
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
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_bool(has_role)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::tel_precompile::{
        burnable::{burnCall, claimCall, grantMintRoleCall, hasMintRoleCall, revokeMintRoleCall},
        erc20::{balanceOfCall, totalSupplyCall},
        test_utils::*,
    };
    use alloy::sol_types::SolCall;
    use reth_revm::{
        primitives::{address, KECCAK_EMPTY},
        state::AccountInfo,
    };
    use tn_config::GOVERNANCE_SAFE_ADDRESS;
    use tn_types::U256;

    const FAUCET: Address = address!("0000000000000000000000000000000000000F00");

    fn fund_faucet(env: &mut TestEnv) {
        env.evm.ctx.db_mut().insert_account_info(
            FAUCET,
            AccountInfo {
                balance: U256::from(10).pow(U256::from(18)),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );
    }

    #[test]
    fn test_grant_mint_role() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        let data = grantMintRoleCall { addr: FAUCET }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);
    }

    #[test]
    fn test_faucet_mint() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(1000) }.abi_encode();
        let result = env.exec_default(FAUCET, data);
        assert_success(&result);
    }

    #[test]
    fn test_revoke_mint_role() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let data = revokeMintRoleCall { addr: FAUCET }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);
    }

    #[test]
    fn test_faucet_mint_after_revoke_fails() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, revokeMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(1000) }.abi_encode();
        let result = env.exec_default(FAUCET, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_faucet_mint_directly_credits() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        );
        assert_success(&result);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(500));
    }

    #[test]
    fn test_faucet_mint_increments_total_supply() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        let genesis = U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18));
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
        assert_eq!(decode_u256(&result), genesis + U256::from(500));
    }

    #[test]
    fn test_faucet_double_mint_is_additive() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(300) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(800));
    }

    #[test]
    fn test_faucet_claim_after_mint_fails() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        let result = env
            .exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: RECIPIENT }.abi_encode());
        assert_not_success(&result);
    }

    #[test]
    fn test_faucet_balance_of() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(750) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(750));
    }

    #[test]
    fn test_faucet_total_supply_after_mint_and_burn() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        let genesis = U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18));
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
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
    fn test_zero_amount_mint_fails() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { recipient: RECIPIENT, amount: U256::ZERO }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    fn test_grant_role_unauthorized() {
        let mut env = TestEnv::new();
        let data = grantMintRoleCall { addr: USER }.abi_encode();
        let result = env.exec_default(USER, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_revoke_role_unauthorized() {
        let mut env = TestEnv::new();
        let data = revokeMintRoleCall { addr: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec_default(USER, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_has_mint_role_governance() {
        let mut env = TestEnv::new();
        let data = hasMintRoleCall { addr: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert!(decode_bool(&result));
    }

    #[test]
    fn test_has_mint_role_random() {
        let mut env = TestEnv::new();
        let random = address!("0000000000000000000000000000000000ABCDEF");
        let data = hasMintRoleCall { addr: random }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert!(!decode_bool(&result));
    }
}
