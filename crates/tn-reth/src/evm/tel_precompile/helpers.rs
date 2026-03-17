//! Storage-slot computation and ABI encoding utilities for the TEL precompile.
//!
//! Provides deterministic slot derivation following Solidity's storage layout for mappings,
//! plus minimal hand-rolled ABI encoders for the small set of return types the precompile needs.
use reth_revm::primitives::keccak256;
use tn_types::{Address, Bytes, B256, U256};

// --- Storage slot helpers ---
//
// All TEL precompile state is stored under TELCOIN_PRECOMPILE_ADDRESS using
// Solidity-style mapping layouts. Base slot indices:
//   0 → pending mint amounts  (mapping: address → uint256)
//   1 → unlock timestamps     (mapping: address → uint256)
//   2 → allowances            (mapping: address → mapping(address → uint256))
//   3 → mint roles            (mapping: address → bool)  [faucet feature only]
//   4 → nonces                (mapping: address → uint256)  [EIP-2612 permit]
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

/// Compute the storage slot for `allowance[owner][spender]`.
///
/// Layout: `keccak256(abi.encode(spender, keccak256(abi.encode(owner, 2))))` — nested
/// Solidity `mapping(address => mapping(address => uint256))` at base slot 2.
///
/// Used by [`handle_approve`], [`handle_transfer_from`], and [`handle_allowance`].
pub(super) fn allowance_slot(owner: Address, spender: Address) -> U256 {
    // Inner hash: keccak256(abi.encode(owner, 2))
    let mut inner_buf = [0u8; 64];
    inner_buf[12..32].copy_from_slice(owner.as_slice());
    inner_buf[63] = 2; // base slot 2
    let inner_hash = keccak256(inner_buf);

    // Outer hash: keccak256(abi.encode(spender, inner_hash))
    let mut outer_buf = [0u8; 64];
    outer_buf[12..32].copy_from_slice(spender.as_slice());
    outer_buf[32..64].copy_from_slice(&inner_hash.0);
    U256::from_be_bytes(keccak256(outer_buf).0)
}

/// Compute the storage slot for an owner's EIP-2612 permit nonce.
///
/// Layout: `keccak256(abi.encode(owner, 4))` — `mapping(address => uint256)` at slot 4.
pub(super) fn nonce_slot(owner: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(owner.as_slice());
    buf[63] = 4; // slot index 4
    U256::from_be_bytes(keccak256(buf).0)
}

// --- ABI encoding helpers ---
//
// Minimal hand-rolled ABI encoders. These exist to avoid pulling in a full ABI codec
// dependency for the small subset of return types the precompile needs.

/// ABI-encode a `bool` as a 32-byte word (`0x00…01` for true, `0x00…00` for false).
pub(super) fn abi_encode_bool(val: bool) -> Bytes {
    let mut buf = [0u8; 32];
    if val {
        buf[31] = 1;
    }
    Bytes::copy_from_slice(&buf)
}

/// ABI-encode a `U256` as a big-endian 32-byte word.
pub(super) fn abi_encode_uint256(val: U256) -> Bytes {
    Bytes::copy_from_slice(&val.to_be_bytes::<32>())
}

/// ABI-encode a dynamic `string` value.
///
/// Returns `[offset (0x20)] [length] [padded UTF-8 data]` per the Solidity ABI spec.
pub(super) fn abi_encode_string(s: &str) -> Bytes {
    let s_bytes = s.as_bytes();
    let padded_len = s_bytes.len().div_ceil(32) * 32;

    // offset (32) + length (32) + padded data
    let mut buf = Vec::with_capacity(64 + padded_len);
    // Offset to string data (0x20 = 32)
    let mut offset_word = [0u8; 32];
    offset_word[31] = 0x20;
    buf.extend_from_slice(&offset_word);
    // String length
    buf.extend_from_slice(&U256::from(s_bytes.len()).to_be_bytes::<32>());
    // Padded string data
    let mut padded = vec![0u8; padded_len];
    padded[..s_bytes.len()].copy_from_slice(s_bytes);
    buf.extend_from_slice(&padded);
    Bytes::from(buf)
}

/// Left-pad a 20-byte address into a 32-byte log topic.
pub(super) fn address_to_topic(addr: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..32].copy_from_slice(addr.as_slice());
    B256::from(bytes)
}
