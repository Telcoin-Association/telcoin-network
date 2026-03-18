//! Timelocked mint/claim lifecycle and token burning.
//!
//! Implements the mainnet token-issuance flow:
//! 1. **`mint(uint256)`** — governance creates a pending mint with a 7-day timelock.
//! 2. **`claim(address)`** — only governance can finalize the mint after the timelock expires,
//!    crediting governance safe's native balance and incrementing `totalSupply`.
//! 3. **`burn(uint256)`** — governance destroys tokens held by the precompile account.
//!
//! The timelock provides a safety window for governance to cancel malicious mints before
//! tokens enter circulation. A second `mint` call **overwrites** any pending mint, which
//! can be used to cancel by minting amount zero.
//!
//! With `feature = "faucet"`, the `mint` function signature changes to `mint(address, uint256)`
//! and bypasses the timelock entirely (see [`faucet`](super::faucet) module).

use alloy::{sol, sol_types::SolEvent};
use alloy_evm::EvmInternals;
use reth_revm::precompile::{PrecompileError, PrecompileOutput, PrecompileResult};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Address, Bytes, U256};

#[cfg(feature = "faucet")]
use crate::evm::tel_precompile::faucet::mint_role_slot;
use crate::{
    evm::tel_precompile::{
        erc20::Transfer,
        helpers::{amount_slot, balance_decr, balance_incr, timestamp_slot},
        TOTAL_SUPPLY_SLOT,
    },
    TELCOIN_PRECOMPILE_ADDRESS,
};

// Mint/claim/burn ABI definitions.
//
// Generates selector constants and Rust encoding/decoding types for the token lifecycle
// and role management interface.
sol! {
    /// Finalize a pending mint after the timelock expires.
    function claim(address recipient) external;
    /// Destroy `amount` tokens held by the precompile account. Governance-only.
    function burn(uint256 amount) external;
    /// Grant `addr` the ability to call `mint`. Governance-only. Faucet feature only.
    function grantMintRole(address addr) external;
    /// Revoke `addr`'s mint role. Governance-only. Faucet feature only.
    function revokeMintRole(address addr) external;
    /// Query whether `addr` has the mint role. Read-only. Faucet feature only.
    function hasMintRole(address addr) external view returns (bool);
    /// Emitted when a new mint is created (pending or instant depending on feature).
    event Mint(address indexed recipient, uint256 amount, uint256 unlockTimestamp);
    /// Emitted when a pending mint is finalized via `claim`.
    event Claim(address indexed recipient, uint256 amount);
    /// Emitted when tokens are burned.
    event Burn(uint256 amount);
}

// mainnet `mint` ABI: takes only an amount, always mints to governance with a timelock.
#[cfg(not(feature = "faucet"))]
sol! {
    /// Create a timelocked pending mint of `amount` tokens to governance.
    function mint(uint256 amount) external;
}

/// Timelock duration applied to new mints before they can be claimed.
///
/// - **mainnet** (`!faucet`): 7 days (604 800 seconds). Provides a window for governance to cancel
///   malicious mints before tokens enter circulation.
/// - **Testnet / faucet** (`faucet`): 0 seconds. Allows instant claim for development convenience.
///
/// # Security invariant
/// This value is set at **compile time**. A mainnet binary must never be built with
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
/// Without the `faucet` feature, only governance can mint. This is the mainnet invariant.
pub(super) fn has_mint_role(
    #[cfg(feature = "faucet")] internals: &mut EvmInternals<'_>,
    #[cfg(not(feature = "faucet"))] _internals: &mut EvmInternals<'_>,
    caller: Address,
) -> Result<bool, PrecompileError> {
    // only allow governance to call mint
    if has_governance_role(caller) {
        return Ok(true);
    }

    // faucet can have more than 1 mint
    #[cfg(feature = "faucet")]
    {
        let slot = mint_role_slot(caller);
        let val = internals
            .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
            .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}")))?
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
pub(super) fn has_governance_role(caller: Address) -> bool {
    caller == GOVERNANCE_SAFE_ADDRESS
}

/// `mint(uint256 amount)` — creates a timelocked pending mint to governance.
///
/// Stores `amount` at the governance address's amount slot and
/// `block.timestamp + TIMELOCK_DURATION` at the timestamp slot.
/// **Does not credit the balance** — that happens in [`handle_claim`] after the timelock expires.
///
/// # Access control
/// Requires [`has_mint_role`] — only governance qualifies in mainnet.
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
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}")))?;

    // Store unlock timestamp at keccak256(recipient, 1)
    let ts_slot = timestamp_slot(recipient);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, ts_slot, unlock_ts)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}")))?;

    // Emit Mint(address recipient, uint256 amount, uint256 unlockTimestamp)
    let topic0 = Mint::SIGNATURE_HASH;
    let mut log_data = Vec::with_capacity(64);
    log_data.extend_from_slice(&amount.to_be_bytes::<32>());
    log_data.extend_from_slice(&unlock_ts.to_be_bytes::<32>());

    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, recipient.into_word()],
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
/// Governance-only via [`has_governance_role`].
pub(super) fn handle_claim(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 25_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_governance_role(caller) {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("claim: expected 32 bytes (address)".into()));
    }

    let recipient = Address::from_slice(&calldata[12..32]);

    // Load pending amount
    let amt_slot = amount_slot(recipient);
    let amount = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, amt_slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}")))?
        .data;

    if amount.is_zero() {
        return Err(PrecompileError::Other("claim: no pending mint".into()));
    }

    // Load unlock timestamp
    let ts_slot = timestamp_slot(recipient);
    let unlock_ts = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, ts_slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}")))?
        .data;

    // Check timelock
    let current_ts = internals.block_timestamp();
    if current_ts < unlock_ts {
        return Err(PrecompileError::Other("claim: timelock not expired".into()));
    }

    // Credit recipient
    balance_incr(internals, recipient, amount)?;

    // Clear storage
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, amt_slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}")))?;
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, ts_slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}")))?;

    // Increment totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}")))?
        .data;
    internals
        .sstore(
            TELCOIN_PRECOMPILE_ADDRESS,
            TOTAL_SUPPLY_SLOT,
            current_supply
                .checked_add(amount)
                .ok_or_else(|| PrecompileError::Other("claim: total supply overflow".into()))?,
        )
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}")))?;

    // Emit Claim(address recipient, uint256 amount)
    let topic0 = Claim::SIGNATURE_HASH;
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, recipient.into_word()],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Claim log".into()))?;
    internals.log(log);

    // Emit Transfer(address(0), recipient, amount) — ERC20 mint event
    let transfer_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Transfer::SIGNATURE_HASH, Address::ZERO.into_word(), recipient.into_word()],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `burn(uint256 amount)` — destroys tokens held by the precompile account.
///
/// Decrements the precompile's native balance by `amount`, then decrements `totalSupply`.
///
/// # Access control
/// Governance-only via [`has_governance_role`].
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
    if !has_governance_role(caller) {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("burn: expected 32 bytes (uint256)".into()));
    }

    let amount = U256::from_be_slice(&calldata[0..32]);

    // Decrement precompile balance (destroy tokens)
    if let Some(error) = balance_decr(internals, TELCOIN_PRECOMPILE_ADDRESS, amount)? {
        return Err(PrecompileError::Other(format!("burn error: {error}")));
    }

    // Decrement totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}")))?
        .data;
    let new_supply = current_supply
        .checked_sub(amount)
        .ok_or_else(|| PrecompileError::Other("burn: total supply underflow".into()))?;
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT, new_supply)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}")))?;

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
            TELCOIN_PRECOMPILE_ADDRESS.into_word(),
            Address::ZERO.into_word(),
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
    #[cfg(not(feature = "faucet"))]
    use crate::evm::tel_precompile::erc20::{balanceOfCall, totalSupplyCall};
    use crate::evm::tel_precompile::test_utils::*;
    use alloy::sol_types::SolCall;
    use tn_config::GOVERNANCE_SAFE_ADDRESS;
    use tn_types::U256;

    #[test]
    fn test_mint_succeeds() {
        let mut env = TestEnv::new();
        let result = env.mint(GOVERNANCE_SAFE_ADDRESS, RECIPIENT, U256::from(500));
        assert_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_before_timelock_halts() {
        let mut env = TestEnv::new();
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(500)).unwrap();
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
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(500)).unwrap();
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
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(500)).unwrap();
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
        let data =
            crate::evm::tel_precompile::mintCall { recipient: RECIPIENT, amount: U256::from(100) }
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
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(500)).unwrap();
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
        let data =
            crate::evm::tel_precompile::mintCall { recipient: RECIPIENT, amount: U256::from(500) }
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
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(500)).unwrap();
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(300)).unwrap();
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
    fn test_zero_amount_mint_overwrites_pending() {
        let mut env = TestEnv::new();
        env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::from(500)).unwrap();
        // Zero-amount mint succeeds and cancels the pending mint
        let result = env.mint(GOVERNANCE_SAFE_ADDRESS, GOVERNANCE_SAFE_ADDRESS, U256::ZERO);
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
