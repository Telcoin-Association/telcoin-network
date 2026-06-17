//! Native BLS12-381 signature-verification precompile.
//!
//! A native `blst` (`min_sig`) signature verifier registered at [`BLS_G1_PRECOMPILE_ADDRESS`]
//! (`0x…b151`). It verifies that a compressed signature is valid, under a compressed public key,
//! over a caller-supplied message - the **same** `blst` path the consensus layer uses to *produce*
//! signatures ([`tn_types::bls_verify_secure`]). Having one implementation behind both signing and
//! verification removes the byte-for-byte drift risk between the Rust signer and an independent
//! on-chain reimplementation.
//!
//! It is a **generic** primitive: the message is opaque to the precompile, so any contract can
//! verify any BLS-signed message. `ConsensusRegistry` is one caller - it builds its
//! proof-of-possession message (`intentPrefix || compressedPubkey || address`) and verifies it here
//! - but the precompile hard-codes nothing about proof-of-possession.
//!
//! # Structure
//!
//! Mirrors [`tel_precompile`](super::tel_precompile): a [`DynPrecompile`] registered via
//! [`add_bls_precompile`] and dispatched by 4-byte selector. The ABI matches `BlsG1.sol`, so
//! `ConsensusRegistry`'s `BlsG1.blsVerify` `delegatecall` resolves to this precompile.
//!
//! | Selector | Behavior |
//! |----------|----------|
//! | `blsVerify(bytes,bytes,bytes)` | Verify a signature over a message from a compressed G1 sig + G2 pubkey. The one crypto entrypoint. |
//!
//! # Encoding
//!
//! The signature/pubkey arguments are the protocol's own `blst::min_sig` compressed encodings
//! (48-byte compressed G1 signature, 96-byte compressed G2 pubkey) - the identical bytes the
//! genesis assembly and `stake`/`delegateStake` callers pass as the `ProofOfPossession` signature
//! and the stored `blsPubkey`. A strict length gate (exactly 48 / 96 bytes) is applied before
//! decoding, so only the compressed form is accepted: the bytes are fed into `blst` via
//! [`BlsSignature::from_bytes`] / [`BlsPublicKey::from_literal_bytes`], which decompress the points
//! internally; no uncompressed input is accepted. The `message` is verified as raw bytes
//! (hash-to-curve with the protocol DST happens inside the verify).
use alloy::{
    primitives::address,
    sol,
    sol_types::{SolCall, SolValue},
};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use reth_revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use tn_types::{bls_verify_secure, Address, BlsPublicKey, BlsSignature, Bytes};

/// Canonical address of the BLS verification precompile: `0x…b151`.
///
/// Matches `BLS_G1_ADDRESS` in `tn-contracts/src/consensus/BlsG1.sol`. `ConsensusRegistry` is
/// linked against this address at genesis, so its `BlsG1.*` library `delegatecall`s land here.
pub const BLS_G1_PRECOMPILE_ADDRESS: Address = address!("000000000000000000000000000000000000b151");

sol! {
    /// Verifies a BLS12-381 signature over `message` from compressed inputs.
    ///
    /// `signature`: 48-byte compressed G1 point. `pubkey`: 96-byte compressed G2 point (both as
    /// produced by `blst::min_sig` `to_bytes`). `message`: the raw signed bytes. Returns whether the
    /// signature is valid over the message under the pubkey.
    function blsVerify(
        bytes signature,
        bytes pubkey,
        bytes message
    ) external view returns (bool);
}

/// Gas charged for a BLS signature verification.
///
/// Priced to reflect the equivalent EIP-2537 work the verification represents: a 2-pairing check
/// (`37_700 + 2 * 32_600 = 102_900`) plus hash-to-curve and point decompression, rounded up. This
/// keeps the on-chain cost proportional to the cryptography while remaining well within a normal
/// transaction's gas budget (`stake` / `delegateStake` run with a 1M default limit). The native
/// implementation completes in microseconds; the charge exists for metering, not compute time.
///
/// This is a flat charge calibrated for the registry's fixed-size proof-of-possession message (119
/// bytes). As a generic primitive, `blsVerify` accepts an arbitrary `message` whose hash-to-curve
/// cost scales with its length; in practice that length is bounded by the calldata gas the caller
/// already pays to supply it. Before any non-proof-of-possession caller relies on `blsVerify` with
/// large messages, add a per-word component to this charge (mirroring EIP-2537 hash-to-curve
/// pricing) or cap `message.len()`.
const BLS_VERIFY_GAS_COST: u64 = 150_000;

/// Registers the BLS precompile at [`BLS_G1_PRECOMPILE_ADDRESS`] in the given map.
///
/// Called from the EVM factory alongside `add_telcoin_precompile`, so the precompile is present for
/// all execution including pre-genesis registry construction.
pub fn add_bls_precompile(map: &mut PrecompilesMap) {
    map.apply_precompile(&BLS_G1_PRECOMPILE_ADDRESS, move |_| {
        Some(DynPrecompile::new_stateful(PrecompileId::Custom("bls_g1".into()), move |input| {
            bls_precompile(input)
        }))
    });
}

/// Precompile entrypoint. Delegates to [`dispatch`]; the precompile is stateless, so it never
/// touches the EVM internals carried by [`PrecompileInput`].
fn bls_precompile(input: PrecompileInput<'_>) -> PrecompileResult {
    dispatch(input.data, input.gas)
}

/// Selector dispatch: extracts the 4-byte selector from calldata and routes to the handler.
///
/// Split out from [`bls_precompile`] so the selector routing, gas metering, and ABI round-trips can
/// be unit-tested directly with raw calldata, without constructing a full [`PrecompileInput`]
/// (which would require a live EVM for its [`EvmInternals`](alloy_evm::EvmInternals) field).
fn dispatch(data: &[u8], gas: u64) -> PrecompileResult {
    let Some((selector, calldata)) = data.split_first_chunk::<4>() else {
        return Err(PrecompileError::Other("Invalid input: too short".into()));
    };

    match *selector {
        blsVerifyCall::SELECTOR => handle_bls_verify(calldata, gas),
        _ => Err(PrecompileError::Other("Unknown function selector".into())),
    }
}

/// `blsVerify(bytes,bytes,bytes) -> bool`.
fn handle_bls_verify(calldata: &[u8], gas_limit: u64) -> PrecompileResult {
    if gas_limit < BLS_VERIFY_GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }

    let decoded = blsVerifyCall::abi_decode_raw(calldata)
        .map_err(|e| PrecompileError::Other(format!("blsVerify: {e}").into()))?;

    // Reuse the exact crypto the consensus layer uses to *produce* signatures, so signer and
    // verifier can never disagree. A malformed point or failed verification yields `false` (not
    // a revert), matching `BlsG1.blsVerify`'s boolean contract; the caller
    // (`ConsensusRegistry`) is what turns `false` into its own `InvalidProofOfPossession`
    // revert.
    let verified = bls_verify(&decoded.signature, &decoded.pubkey, &decoded.message);

    Ok(PrecompileOutput::new(BLS_VERIFY_GAS_COST, Bytes::from(verified.abi_encode())))
}

/// Decode the compressed sig/pubkey and verify the signature over `message` via `blst`. Any decode
/// or verification failure maps to `false` so bad input can never panic or revert the precompile.
///
/// The explicit length gate is the functional enforcement of the compressed-only encoding: blst's
/// `deserialize` accepts *either* encoding by length, so without this gate a 96-byte uncompressed
/// signature or 192-byte uncompressed pubkey would still decode. Requiring exactly 48 / 96 bytes
/// rejects the uncompressed forms up front; [`BlsSignature::from_bytes`] /
/// [`BlsPublicKey::from_literal_bytes`] then require the compression flag, so a 96-byte flag-clear
/// pubkey is rejected too. Subgroup and infinity checks remain at verify time (unchanged).
fn bls_verify(compressed_sig: &[u8], compressed_pubkey: &[u8], message: &[u8]) -> bool {
    if compressed_sig.len() != 48 || compressed_pubkey.len() != 96 {
        return false;
    }
    let Ok(pubkey) = BlsPublicKey::from_literal_bytes(compressed_pubkey) else {
        return false;
    };
    let Ok(sig) = BlsSignature::from_bytes(compressed_sig) else {
        return false;
    };

    bls_verify_secure(&sig, &pubkey, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{
        construct_proof_of_possession_message, generate_proof_of_possession_bls_for_test,
        BlsKeypair,
    };

    /// A valid signature over a representative message (the consensus proof-of-possession message),
    /// plus the compressed `blst::min_sig` sig/pubkey bytes the protocol passes on-chain (48-byte
    /// G1 sig, 96-byte G2 pubkey).
    struct Vector {
        sig: Vec<u8>,
        pubkey: Vec<u8>,
        message: Vec<u8>,
    }

    /// Builds a valid (signature, pubkey, message) vector from a fixed RNG seed and address byte.
    /// The message is the proof-of-possession message; the signature is produced over it by the
    /// same signer path the protocol uses, so signer and precompile agree by construction.
    fn vector(seed: u8, address_byte: u8) -> Vector {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([seed; 32]));
        let address = Address::repeat_byte(address_byte);
        let message = construct_proof_of_possession_message(keypair.public(), &address);
        let proof = generate_proof_of_possession_bls_for_test(&keypair, &address)
            .expect("generate test PoP");
        Vector {
            sig: proof.to_bytes().to_vec(),
            pubkey: keypair.public().to_bytes().to_vec(),
            message,
        }
    }

    /// ABI-encodes a `blsVerify` call (selector + args), as a caller would.
    fn encode_verify(sig: &[u8], pubkey: &[u8], message: &[u8]) -> Vec<u8> {
        blsVerifyCall {
            signature: Bytes::copy_from_slice(sig),
            pubkey: Bytes::copy_from_slice(pubkey),
            message: Bytes::copy_from_slice(message),
        }
        .abi_encode()
    }

    /// Decodes the ABI-encoded `bool` returned by a `blsVerify` call.
    fn decode_bool(bytes: &[u8]) -> bool {
        <bool as SolValue>::abi_decode(bytes).expect("decode bool return")
    }

    // --- `bls_verify` crypto semantics -----------------------------------------------------------

    /// A well-formed signature verifies through the same `blst` path the signer used. Looped over
    /// several keypairs/messages to stand in for the Solidity fuzz coverage.
    #[test]
    fn bls_verify_accepts_valid_signature() {
        for (seed, addr) in [(7u8, 0x42u8), (11, 0x01), (99, 0xab), (1, 0xff)] {
            let v = vector(seed, addr);
            assert!(bls_verify(&v.sig, &v.pubkey, &v.message), "seed {seed} addr {addr:#x}");
        }
    }

    /// S1 (load-bearing): the precompile is compressed-only. A *valid* 96-byte uncompressed
    /// signature and 192-byte uncompressed pubkey - the pre-change encoding - must be rejected
    /// by the length gate. blst's `deserialize` would otherwise accept these valid points, so
    /// this (not random wrong-length bytes) is the proof that the gate is the enforcement.
    #[test]
    fn bls_verify_rejects_valid_uncompressed_inputs() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([7u8; 32]));
        let address = Address::repeat_byte(0x42);
        let message = construct_proof_of_possession_message(keypair.public(), &address);
        let proof = generate_proof_of_possession_bls_for_test(&keypair, &address)
            .expect("generate test PoP");

        let uncompressed_sig = proof.serialize().to_vec();
        let uncompressed_pubkey = keypair.public().serialize().to_vec();
        assert_eq!(uncompressed_sig.len(), 96, "uncompressed G1 signature");
        assert_eq!(uncompressed_pubkey.len(), 192, "uncompressed G2 pubkey");

        // the same key in compressed form verifies, proving the inputs are otherwise valid
        let compressed_sig = proof.to_bytes().to_vec();
        let compressed_pubkey = keypair.public().to_bytes().to_vec();
        assert!(bls_verify(&compressed_sig, &compressed_pubkey, &message), "compressed control");

        // ...but the valid uncompressed encodings are rejected by the length gate
        assert!(
            !bls_verify(&uncompressed_sig, &uncompressed_pubkey, &message),
            "uncompressed gated"
        );
    }

    /// A signature over one message must not verify against a different message (here, the same key
    /// bound to a different address yields a different message).
    #[test]
    fn bls_verify_rejects_wrong_message() {
        let v = vector(7, 0x42);
        let other = vector(7, 0x43);
        assert!(!bls_verify(&v.sig, &v.pubkey, &other.message));
    }

    /// A signature produced by a different key must fail against the original pubkey (port of the
    /// Solidity "mutated signature" negative case).
    #[test]
    fn bls_verify_rejects_wrong_signature() {
        let v = vector(7, 0x42);
        let other = vector(8, 0x42);
        assert!(!bls_verify(&other.sig, &v.pubkey, &v.message));
    }

    /// A valid signature must not verify against a substituted pubkey (port of the Solidity "pubkey
    /// substitution" attack case).
    #[test]
    fn bls_verify_rejects_pubkey_substitution() {
        let v = vector(7, 0x42);
        let other = vector(8, 0x42);
        assert!(!bls_verify(&v.sig, &other.pubkey, &v.message));
    }

    /// Identity/infinity points and all-zero inputs return `false`, never panic. All-zero is not a
    /// valid compressed point (a valid compressed infinity carries the `0xc0` flag).
    #[test]
    fn bls_verify_rejects_zero_and_infinity_points() {
        let v = vector(7, 0x42);
        assert!(!bls_verify(&[0u8; 48], &[0u8; 96], &v.message));
        assert!(!bls_verify(&[0u8; 48], &v.pubkey, &v.message));
        assert!(!bls_verify(&v.sig, &[0u8; 96], &v.message));
    }

    /// Wrong-length sig/pubkey inputs are rejected by the length gate without panicking.
    #[test]
    fn bls_verify_rejects_wrong_length_inputs() {
        let v = vector(7, 0x42);

        // pubkey lengths that are not the 96-byte compressed G2 form (incl. the 192-byte
        // uncompressed)
        for len in [0usize, 32, 47, 48, 95, 97, 128, 192, 256] {
            assert!(!bls_verify(&v.sig, &vec![0u8; len], &v.message), "pubkey len {len}");
        }
        // signature lengths that are not the 48-byte compressed G1 form (incl. the 96-byte
        // uncompressed)
        for len in [0usize, 32, 47, 49, 96, 128, 192] {
            assert!(!bls_verify(&vec![0u8; len], &v.pubkey, &v.message), "sig len {len}");
        }
    }

    // --- selector dispatch / ABI surface ---------------------------------------------------------

    /// A valid signature through the full ABI path returns ABI-encoded `true` and charges the fixed
    /// cost.
    #[test]
    fn dispatch_verify_valid_returns_true() {
        let v = vector(7, 0x42);
        let out = dispatch(&encode_verify(&v.sig, &v.pubkey, &v.message), BLS_VERIFY_GAS_COST)
            .expect("dispatch ok");
        assert!(decode_bool(&out.bytes));
        assert_eq!(out.gas_used, BLS_VERIFY_GAS_COST);
    }

    /// An invalid signature returns ABI-encoded `false` rather than reverting: the precompile
    /// mirrors `BlsG1.blsVerify`'s boolean contract, leaving the revert to `ConsensusRegistry`.
    #[test]
    fn dispatch_verify_invalid_returns_false_not_revert() {
        let v = vector(7, 0x42);
        let other = vector(7, 0x43);
        let out = dispatch(&encode_verify(&v.sig, &v.pubkey, &other.message), BLS_VERIFY_GAS_COST)
            .expect("dispatch ok (false, not error)");
        assert!(!decode_bool(&out.bytes));
        // gas is still charged for the work performed
        assert_eq!(out.gas_used, BLS_VERIFY_GAS_COST);
    }

    /// Verification with less gas than the fixed cost is metered as out-of-gas.
    #[test]
    fn dispatch_verify_out_of_gas() {
        let v = vector(7, 0x42);
        let res = dispatch(&encode_verify(&v.sig, &v.pubkey, &v.message), BLS_VERIFY_GAS_COST - 1);
        assert!(matches!(res, Err(PrecompileError::OutOfGas)));
    }

    /// Unknown selectors and truncated calldata are rejected.
    #[test]
    fn dispatch_rejects_unknown_selector_and_short_input() {
        // unknown 4-byte selector + padding
        let mut unknown = vec![0xDE, 0xAD, 0xBE, 0xEF];
        unknown.extend_from_slice(&[0u8; 32]);
        assert!(dispatch(&unknown, BLS_VERIFY_GAS_COST).is_err());

        // fewer than 4 bytes cannot carry a selector
        assert!(dispatch(&[0x01, 0x02], BLS_VERIFY_GAS_COST).is_err());
        assert!(dispatch(&[], BLS_VERIFY_GAS_COST).is_err());
    }
}
