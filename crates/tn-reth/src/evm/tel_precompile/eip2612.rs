//! EIP-2612 permit support for gasless ERC-20 approvals.
//!
//! Implements `permit(owner, spender, value, deadline, v, r, s)` which verifies an EIP-712
//! typed-data signature and sets `allowance[owner][spender] = value` without requiring a
//! transaction from `owner`. Also exposes `nonces(owner)` and `DOMAIN_SEPARATOR()` as
//! read-only queries.
//!
//! # Security considerations
//!
//! - Signatures use EIP-712 structured data with a chain-specific domain separator.
//! - Nonces are monotonically incremented to prevent replay attacks.
//! - The `s` value is checked against `SECP256K1N_HALF` to prevent signature malleability.
//! - `v` must be exactly 27 or 28.
use crate::{
    evm::tel_precompile::{
        erc20::Approval,
        helpers::{allowance_slot, nonce_slot},
    },
    TELCOIN_PRECOMPILE_ADDRESS,
};
use alloy::{primitives::Signature, sol, sol_types::{SolEvent, SolValue}};
use alloy_evm::EvmInternals;
use reth_primitives_traits::crypto::SECP256K1N_HALF;
use reth_revm::{
    precompile::{PrecompileError, PrecompileOutput, PrecompileResult},
    primitives::keccak256,
};
use tn_types::{Address, Bytes, B256, U256};

// EIP-2612 ABI definitions.
//
// Generates selector constants and Rust encoding/decoding types for the permit interface.
sol! {
    /// Set `allowance[owner][spender] = value` using an off-chain EIP-712 signature.
    function permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s) external;
    /// Return the current permit nonce for `owner` (monotonically increasing).
    function nonces(address owner) external view returns (uint256);
    /// Return the EIP-712 domain separator for the current chain.
    function DOMAIN_SEPARATOR() external view returns (bytes32);
}

/// EIP-712 domain type hash: `keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")`.
const EIP712_DOMAIN_TYPEHASH: B256 = B256::new([
    0x8b, 0x73, 0xc3, 0xc6, 0x9b, 0xb8, 0xfe, 0x3d, 0x51, 0x2e, 0xcc, 0x4c, 0xf7, 0x59, 0xcc, 0x79,
    0x23, 0x9f, 0x7b, 0x17, 0x9b, 0x0f, 0xfa, 0xca, 0xa9, 0xa7, 0x5d, 0x52, 0x2b, 0x39, 0x40, 0x0f,
]);

/// EIP-2612 permit type hash: `keccak256("Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)")`.
const PERMIT_TYPEHASH: B256 = B256::new([
    0x6e, 0x71, 0xed, 0xae, 0x12, 0xb1, 0xb9, 0x7f, 0x4d, 0x1f, 0x60, 0x37, 0x0f, 0xef, 0x10, 0x10,
    0x5f, 0xa2, 0xfa, 0xae, 0x01, 0x26, 0x11, 0x4a, 0x16, 0x9c, 0x64, 0x84, 0x5d, 0x61, 0x26, 0xc9,
]);

/// Compute the EIP-712 domain separator for the given chain ID.
///
/// `keccak256(abi.encode(EIP712_DOMAIN_TYPEHASH, keccak256("Telcoin"), keccak256("1"), chainId, TELCOIN_PRECOMPILE_ADDRESS))`
pub(super) fn compute_domain_separator(chain_id: u64) -> B256 {
    let mut buf = [0u8; 5 * 32];
    buf[0..32].copy_from_slice(&EIP712_DOMAIN_TYPEHASH.0);
    buf[32..64].copy_from_slice(&keccak256("Telcoin").0);
    buf[64..96].copy_from_slice(&keccak256("1").0);
    buf[96..128].copy_from_slice(&U256::from(chain_id).to_be_bytes::<32>());
    buf[140..160].copy_from_slice(TELCOIN_PRECOMPILE_ADDRESS.as_slice());
    keccak256(buf)
}

/// `nonces(address owner)` → returns the current permit nonce for `owner`.
pub(super) fn handle_nonces(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("nonces: expected 32 bytes (address)".into()));
    }
    let owner = Address::from_slice(&calldata[12..32]);
    let nonce = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, nonce_slot(owner))
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::from(nonce.abi_encode())))
}

/// `DOMAIN_SEPARATOR()` → returns the EIP-712 domain separator for the current chain.
pub(super) fn handle_domain_separator(
    internals: &mut EvmInternals<'_>,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_600;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    let ds = compute_domain_separator(internals.chain_id());
    Ok(PrecompileOutput::new(GAS_COST, Bytes::copy_from_slice(&ds.0)))
}

/// `permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s)`
///
/// EIP-2612 gasless approval. Verifies an off-chain signature from `owner` and sets
/// `allowance[owner][spender] = value`. Increments the owner's nonce to prevent replay.
///
/// Emits `Approval(owner, spender, value)`.
pub(super) fn handle_permit(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 72_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 224 {
        return Err(PrecompileError::Other(
            "permit: expected 224 bytes (address,address,uint256,uint256,uint8,bytes32,bytes32)"
                .into(),
        ));
    }

    let owner = Address::from_slice(&calldata[12..32]);
    let spender = Address::from_slice(&calldata[44..64]);
    if spender == Address::ZERO {
        return Err(PrecompileError::Other("permit: cannot approve address(0)".into()));
    }
    let value = U256::from_be_slice(&calldata[64..96]);
    let deadline = U256::from_be_slice(&calldata[96..128]);
    let v = calldata[159];
    let r = U256::from_be_slice(&calldata[160..192]);
    let s = U256::from_be_slice(&calldata[192..224]);
    // Validate s
    if s > SECP256K1N_HALF {
        return Err(PrecompileError::Other("permit: signature malleability".into()));
    }

    // Check deadline
    let current_ts = internals.block_timestamp();
    if current_ts > deadline {
        return Err(PrecompileError::Other("permit: expired deadline".into()));
    }

    // Validate v
    if v != 27 && v != 28 {
        return Err(PrecompileError::Other("permit: invalid v value".into()));
    }

    // Load current nonce
    let nonce_s = nonce_slot(owner);
    let nonce = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, nonce_s)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    // Compute EIP-712 struct hash
    let struct_hash = {
        let mut buf = [0u8; 6 * 32];
        buf[0..32].copy_from_slice(&PERMIT_TYPEHASH.0);
        buf[44..64].copy_from_slice(owner.as_slice());
        buf[76..96].copy_from_slice(spender.as_slice());
        buf[96..128].copy_from_slice(&value.to_be_bytes::<32>());
        buf[128..160].copy_from_slice(&nonce.to_be_bytes::<32>());
        buf[160..192].copy_from_slice(&deadline.to_be_bytes::<32>());
        keccak256(buf)
    };

    // Compute EIP-712 digest
    let digest = {
        let domain_separator = compute_domain_separator(internals.chain_id());
        let mut buf = [0u8; 2 + 32 + 32];
        buf[0] = 0x19;
        buf[1] = 0x01;
        buf[2..34].copy_from_slice(&domain_separator.0);
        buf[34..66].copy_from_slice(&struct_hash.0);
        keccak256(buf)
    };

    // Recover signer and verify
    let sig = Signature::new(r, s, v == 28);
    let recovered = sig
        .recover_address_from_prehash(&digest)
        .map_err(|_| PrecompileError::Other("permit: invalid signature".into()))?;
    if recovered != owner {
        return Err(PrecompileError::Other("permit: signer != owner".into()));
    }

    // Increment nonce
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, nonce_s, nonce + U256::from(1))
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Set allowance
    let slot = allowance_slot(owner, spender);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, value)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Approval(owner, spender, value)
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Approval::SIGNATURE_HASH, owner.into_word(), spender.into_word()],
        value.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Approval log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::tel_precompile::{
        erc20::{allowanceCall, transferFromCall},
        test_utils::*,
    };
    use alloy::sol_types::SolCall;
    use reth_revm::primitives::keccak256;
    use tn_config::GOVERNANCE_SAFE_ADDRESS;
    use tn_types::{B256, U256};

    #[test]
    fn test_typehash_values() {
        let domain_hash = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        assert_eq!(domain_hash, EIP712_DOMAIN_TYPEHASH);

        let permit_hash = keccak256(
            "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)",
        );
        assert_eq!(permit_hash, PERMIT_TYPEHASH);
    }

    #[test]
    fn test_permit_sets_allowance() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let value = U256::from(500);
        let deadline = U256::from(2000);

        let (v, r, s) = sign_permit(owner, RECIPIENT, value, U256::ZERO, deadline, 1);
        let data = permitCall { owner, spender: RECIPIENT, value, deadline, v, r, s }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);

        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner, spender: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), value);
    }

    #[test]
    fn test_permit_increments_nonce() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);

        // First permit (nonce 0)
        let (v, r, s) =
            sign_permit(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, noncesCall { owner }.abi_encode());
        assert_eq!(decode_u256(&result), U256::from(1));

        // Second permit (nonce 1)
        let (v, r, s) =
            sign_permit(owner, RECIPIENT, U256::from(200), U256::from(1), deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(200), deadline, v, r, s }
                .abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, noncesCall { owner }.abi_encode());
        assert_eq!(decode_u256(&result), U256::from(2));
    }

    #[test]
    fn test_permit_then_transfer_from() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let value = U256::from(500);
        let deadline = U256::from(2000);

        let (v, r, s) = sign_permit(owner, USER, value, U256::ZERO, deadline, 1);
        let data = permitCall { owner, spender: USER, value, deadline, v, r, s }.abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let result = env.exec_default(
            USER,
            transferFromCall { from: owner, to: RECIPIENT, amount: U256::from(200) }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner, spender: USER }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), value - U256::from(200));
    }

    #[test]
    fn test_permit_expired_deadline_fails() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        // Block timestamp is 1000, deadline is 999 (already expired)
        let deadline = U256::from(999);
        let (v, r, s) =
            sign_permit(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_wrong_signer_fails() {
        let mut env = TestEnv::new();
        let signer_addr = permit_signer_address();
        let deadline = U256::from(2000);

        // Sign as the real signer address
        let (v, r, s) =
            sign_permit(signer_addr, USER, U256::from(100), U256::ZERO, deadline, 1);
        // Submit with USER as the claimed owner — signature won't match
        let data = permitCall {
            owner: USER,
            spender: RECIPIENT,
            value: U256::from(100),
            deadline,
            v,
            r,
            s,
        }
        .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_replay_fails() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);

        let (v, r, s) =
            sign_permit(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();

        // First call succeeds
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data.clone()));
        // Replay fails (nonce incremented)
        assert_not_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));
    }

    #[test]
    fn test_permit_invalid_v_fails() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);

        let (_, r, s) =
            sign_permit(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v: 26, r, s }
                .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_short_calldata_fails() {
        let mut env = TestEnv::new();
        let full = permitCall {
            owner: GOVERNANCE_SAFE_ADDRESS,
            spender: USER,
            value: U256::from(100),
            deadline: U256::from(2000),
            v: 27,
            r: B256::ZERO,
            s: B256::ZERO,
        }
        .abi_encode();
        // Truncate to less than 228 bytes (4 selector + 224 calldata)
        let short = full[..4 + 192].to_vec();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_oog() {
        let mut env = TestEnv::new();
        let owner = permit_signer_address();
        let deadline = U256::from(2000);
        let (v, r, s) =
            sign_permit(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_nonces_initial_zero() {
        let mut env = TestEnv::new();
        let result =
            env.exec_default(GOVERNANCE_SAFE_ADDRESS, noncesCall { owner: USER }.abi_encode());
        assert_eq!(decode_u256(&result), U256::ZERO);
    }

    #[test]
    fn test_nonces_short_calldata_fails() {
        let mut env = TestEnv::new();
        let full = noncesCall { owner: USER }.abi_encode();
        let short = full[..4 + 16].to_vec();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_domain_separator_returns_value() {
        let mut env = TestEnv::new();
        let result =
            env.exec_default(GOVERNANCE_SAFE_ADDRESS, DOMAIN_SEPARATORCall {}.abi_encode());
        let bytes = extract_output_bytes(&result);
        assert_eq!(bytes.len(), 32);
        let expected = compute_domain_separator(1);
        assert_eq!(B256::from_slice(&bytes), expected);
    }

    #[test]
    fn test_domain_separator_deterministic() {
        let mut env = TestEnv::new();
        let r1 = env.exec_default(GOVERNANCE_SAFE_ADDRESS, DOMAIN_SEPARATORCall {}.abi_encode());
        let r2 = env.exec_default(GOVERNANCE_SAFE_ADDRESS, DOMAIN_SEPARATORCall {}.abi_encode());
        assert_eq!(extract_output_bytes(&r1), extract_output_bytes(&r2));
    }

    #[test]
    fn test_const_typehash_matches() {
        // eip712
        let eip712_domain_typehash_expected = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        assert_eq!(EIP712_DOMAIN_TYPEHASH, eip712_domain_typehash_expected);

        // eip2612
        let permit_typehash = keccak256(
            "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)",
        );
        assert_eq!(PERMIT_TYPEHASH, permit_typehash);
    }
}
