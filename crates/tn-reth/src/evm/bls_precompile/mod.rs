//! Native BLS12-381 proof-of-possession precompile.
//!
//! Replaces the Solidity `BlsG1` library (linked at [`BLS_G1_PRECOMPILE_ADDRESS`], `0x…b151`) with
//! a native implementation that delegates to the **same** `blst` (`min_sig`) verification the
//! consensus layer uses to *produce* proofs of possession
//! ([`tn_types::verify_proof_of_possession_bls`]). Having one implementation behind both signing
//! and verification removes the byte-for-byte drift risk between the Rust signer and an independent
//! on-chain reimplementation.
//!
//! # Structure
//!
//! Mirrors [`tel_precompile`](super::tel_precompile): a [`DynPrecompile`] registered via
//! [`add_bls_precompile`] and dispatched by 4-byte selector. The ABI matches `BlsG1.sol`, so
//! `ConsensusRegistry`'s `BlsG1.verifyProofOfPossession` `delegatecall` resolves to this
//! precompile.
//!
//! | Selector | Behavior |
//! |----------|----------|
//! | `verifyProofOfPossession(bytes,bytes,address)` | Verify a PoP from a compressed G1 sig + G2 pubkey. The one crypto entrypoint. |
//!
//! # Encoding
//!
//! The signature/pubkey arguments are the protocol's own `blst::min_sig` compressed encodings
//! (48-byte compressed G1 signature, 96-byte compressed G2 pubkey) - the identical bytes the
//! genesis assembly and `stake`/`delegateStake` callers pass as the `ProofOfPossession` signature
//! and the stored `blsPubkey`. A strict length gate (exactly 48 / 96 bytes) is applied before
//! decoding, so only the compressed form is accepted: the bytes are fed into `blst` via
//! [`BlsSignature::from_bytes`] / [`BlsPublicKey::from_literal_bytes`], which decompress the points
//! internally; no uncompressed input is accepted.
use alloy::{
    primitives::address,
    sol,
    sol_types::{SolCall, SolValue},
};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use reth_revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use tn_types::{verify_proof_of_possession_bls, Address, BlsPublicKey, BlsSignature, Bytes};

/// Canonical address of the BLS proof-of-possession precompile: `0x…b151`.
///
/// Matches `BLS_G1_ADDRESS` in `tn-contracts/src/consensus/BlsG1.sol`. `ConsensusRegistry` is
/// linked against this address at genesis, so its `BlsG1.*` library `delegatecall`s land here.
pub const BLS_G1_PRECOMPILE_ADDRESS: Address = address!("000000000000000000000000000000000000b151");

sol! {
    /// Verifies a validator's BLS12-381 proof of possession from compressed inputs.
    ///
    /// `signature`: 48-byte compressed G1 point. `pubkey`: 96-byte compressed G2 point. Both as
    /// produced by `blst::min_sig` `to_bytes` / `compress`.
    function verifyProofOfPossession(
        bytes signature,
        bytes pubkey,
        address validatorAddress
    ) external view returns (bool);
}

/// Gas charged for a proof-of-possession verification.
///
/// Priced to reflect the equivalent EIP-2537 work the verification represents: a 2-pairing check
/// (`37_700 + 2 * 32_600 = 102_900`) plus hash-to-curve and point decompression, rounded up. This
/// keeps the on-chain cost proportional to the cryptography while remaining well within a normal
/// transaction's gas budget (`stake` / `delegateStake` run with a 1M default limit). The native
/// implementation completes in microseconds; the charge exists for metering, not compute time.
const VERIFY_POP_GAS_COST: u64 = 150_000;

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
        verifyProofOfPossessionCall::SELECTOR => handle_verify_pop(calldata, gas),
        _ => Err(PrecompileError::Other("Unknown function selector".into())),
    }
}

/// `verifyProofOfPossession(bytes,bytes,address) -> bool`.
fn handle_verify_pop(calldata: &[u8], gas_limit: u64) -> PrecompileResult {
    if gas_limit < VERIFY_POP_GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }

    let decoded = verifyProofOfPossessionCall::abi_decode_raw(calldata)
        .map_err(|e| PrecompileError::Other(format!("verifyProofOfPossession: {e}").into()))?;

    // Reuse the exact crypto the consensus layer uses to *produce* PoPs, so signer and verifier can
    // never disagree. A malformed point or failed pairing yields `false` (not a revert), matching
    // `BlsG1.verifyProofOfPossession`'s boolean contract; the caller (`ConsensusRegistry`) is what
    // turns `false` into its own `InvalidProofOfPossession` revert.
    let verified = verify_pop(&decoded.signature, &decoded.pubkey, decoded.validatorAddress);

    Ok(PrecompileOutput::new(VERIFY_POP_GAS_COST, Bytes::from(verified.abi_encode())))
}

/// Decode the compressed inputs and run the `blst` proof-of-possession check. Any decode or
/// verification failure maps to `false` so a bad proof can never panic or revert the precompile.
///
/// The explicit length gate is the functional enforcement of the compressed-only encoding: blst's
/// `deserialize` accepts *either* encoding by length, so without this gate a 96-byte uncompressed
/// signature or 192-byte uncompressed pubkey would still decode. Requiring exactly 48 / 96 bytes
/// rejects the uncompressed forms up front; [`BlsSignature::from_bytes`] /
/// [`BlsPublicKey::from_literal_bytes`] then require the compression flag, so a 96-byte flag-clear
/// pubkey is rejected too. Subgroup and infinity checks remain at verify time (unchanged).
fn verify_pop(compressed_sig: &[u8], compressed_pubkey: &[u8], address: Address) -> bool {
    if compressed_sig.len() != 48 || compressed_pubkey.len() != 96 {
        return false;
    }
    let Ok(pubkey) = BlsPublicKey::from_literal_bytes(compressed_pubkey) else {
        return false;
    };
    let Ok(sig) = BlsSignature::from_bytes(compressed_sig) else {
        return false;
    };

    verify_proof_of_possession_bls(&sig, &pubkey, &address).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{generate_proof_of_possession_bls_for_test, BlsKeypair};

    /// A valid PoP's compressed sig/pubkey bytes for `address`.
    ///
    /// `sig`/`pubkey` are `blst::min_sig` compressed `to_bytes()` output (48-byte G1 sig, 96-byte
    /// G2 pubkey), the exact bytes the protocol passes as the `ProofOfPossession` signature and the
    /// stored `blsPubkey`.
    struct Vector {
        address: Address,
        sig: Vec<u8>,
        pubkey: Vec<u8>,
    }

    /// Builds a valid proof-of-possession vector from a fixed RNG seed and address byte.
    fn vector(seed: u8, address_byte: u8) -> Vector {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([seed; 32]));
        let address = Address::repeat_byte(address_byte);
        let proof = generate_proof_of_possession_bls_for_test(&keypair, &address)
            .expect("generate test PoP");
        let sig = proof.to_bytes().to_vec();
        let pubkey = keypair.public().to_bytes().to_vec();
        Vector { address, sig, pubkey }
    }

    /// ABI-encodes a `verifyProofOfPossession` call (selector + args), as a caller would.
    fn encode_verify(sig: &[u8], pubkey: &[u8], address: Address) -> Vec<u8> {
        verifyProofOfPossessionCall {
            signature: Bytes::copy_from_slice(sig),
            pubkey: Bytes::copy_from_slice(pubkey),
            validatorAddress: address,
        }
        .abi_encode()
    }

    /// Decodes the ABI-encoded `bool` returned by a `verifyProofOfPossession` call.
    fn decode_bool(bytes: &[u8]) -> bool {
        <bool as SolValue>::abi_decode(bytes).expect("decode bool return")
    }

    // --- `verify_pop` crypto semantics -----------------------------------------------------------

    /// A well-formed proof of possession verifies through the same `blst` path the signer used.
    /// Looped over several keypairs/addresses to stand in for the Solidity fuzz coverage.
    #[test]
    fn verify_pop_accepts_valid_proof() {
        for (seed, addr) in [(7u8, 0x42u8), (11, 0x01), (99, 0xab), (1, 0xff)] {
            let v = vector(seed, addr);
            assert!(verify_pop(&v.sig, &v.pubkey, v.address), "seed {seed} addr {addr:#x}");
        }
    }

    /// S1 (load-bearing): the precompile is compressed-only. A *valid* 96-byte uncompressed
    /// signature and 192-byte uncompressed pubkey - the pre-change encoding - must be rejected by
    /// the length gate. blst's `deserialize` would otherwise accept these valid points, so this
    /// (not random wrong-length bytes) is the proof that the gate is the enforcement.
    #[test]
    fn verify_pop_rejects_valid_uncompressed_inputs() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([7u8; 32]));
        let address = Address::repeat_byte(0x42);
        let proof = generate_proof_of_possession_bls_for_test(&keypair, &address)
            .expect("generate test PoP");

        let uncompressed_sig = proof.serialize().to_vec();
        let uncompressed_pubkey = keypair.public().serialize().to_vec();
        assert_eq!(uncompressed_sig.len(), 96, "uncompressed G1 signature");
        assert_eq!(uncompressed_pubkey.len(), 192, "uncompressed G2 pubkey");

        // the same key in compressed form verifies, proving the inputs are otherwise valid
        let compressed_sig = proof.to_bytes().to_vec();
        let compressed_pubkey = keypair.public().to_bytes().to_vec();
        assert!(verify_pop(&compressed_sig, &compressed_pubkey, address), "compressed control");

        // ...but the valid uncompressed encodings are rejected by the length gate
        assert!(
            !verify_pop(&uncompressed_sig, &uncompressed_pubkey, address),
            "uncompressed gated"
        );
    }

    /// A proof bound to a different address must fail (the address is part of the signed message).
    #[test]
    fn verify_pop_rejects_wrong_address() {
        let v = vector(7, 0x42);
        assert!(!verify_pop(&v.sig, &v.pubkey, Address::repeat_byte(0x43)));
    }

    /// A signature produced by a different key must fail against the original pubkey
    /// (port of the Solidity "mutated signature" negative case).
    #[test]
    fn verify_pop_rejects_wrong_signature() {
        let v = vector(7, 0x42);
        let other = vector(8, 0x42);
        assert!(!verify_pop(&other.sig, &v.pubkey, v.address));
    }

    /// A valid signature must not verify against a substituted pubkey
    /// (port of the Solidity "pubkey substitution" attack case).
    #[test]
    fn verify_pop_rejects_pubkey_substitution() {
        let v = vector(7, 0x42);
        let other = vector(8, 0x42);
        assert!(!verify_pop(&v.sig, &other.pubkey, v.address));
    }

    /// Identity/infinity points and all-zero inputs return `false`, never panic. All-zero is not a
    /// valid compressed point (a valid compressed infinity carries the `0xc0` flag).
    #[test]
    fn verify_pop_rejects_zero_and_infinity_points() {
        let v = vector(7, 0x42);
        // all-zero sig + pubkey at the compressed lengths
        assert!(!verify_pop(&[0u8; 48], &[0u8; 96], Address::ZERO));
        // zero signature against an otherwise valid pubkey/address
        assert!(!verify_pop(&[0u8; 48], &v.pubkey, v.address));
        // zero pubkey against an otherwise valid signature/address
        assert!(!verify_pop(&v.sig, &[0u8; 96], v.address));
    }

    /// Wrong-length sig/pubkey inputs are rejected by the length gate without panicking.
    #[test]
    fn verify_pop_rejects_wrong_length_inputs() {
        let v = vector(7, 0x42);

        // pubkey lengths that are not the 96-byte compressed G2 form (incl. the 192-byte
        // uncompressed)
        for len in [0usize, 32, 47, 48, 95, 97, 128, 192, 256] {
            assert!(!verify_pop(&v.sig, &vec![0u8; len], v.address), "pubkey len {len}");
        }
        // signature lengths that are not the 48-byte compressed G1 form (incl. the 96-byte
        // uncompressed)
        for len in [0usize, 32, 47, 49, 96, 128, 192] {
            assert!(!verify_pop(&vec![0u8; len], &v.pubkey, v.address), "sig len {len}");
        }
    }

    // --- selector dispatch / ABI surface ---------------------------------------------------------

    /// A valid PoP through the full ABI path returns ABI-encoded `true` and charges the fixed cost.
    #[test]
    fn dispatch_verify_valid_returns_true() {
        let v = vector(7, 0x42);
        let out = dispatch(&encode_verify(&v.sig, &v.pubkey, v.address), VERIFY_POP_GAS_COST)
            .expect("dispatch ok");
        assert!(decode_bool(&out.bytes));
        assert_eq!(out.gas_used, VERIFY_POP_GAS_COST);
    }

    /// An invalid PoP returns ABI-encoded `false` rather than reverting: the precompile mirrors
    /// `BlsG1.verifyProofOfPossession`'s boolean contract, leaving the revert to
    /// `ConsensusRegistry`.
    #[test]
    fn dispatch_verify_invalid_returns_false_not_revert() {
        let v = vector(7, 0x42);
        let out = dispatch(
            &encode_verify(&v.sig, &v.pubkey, Address::repeat_byte(0x43)),
            VERIFY_POP_GAS_COST,
        )
        .expect("dispatch ok (false, not error)");
        assert!(!decode_bool(&out.bytes));
        // gas is still charged for the work performed
        assert_eq!(out.gas_used, VERIFY_POP_GAS_COST);
    }

    /// Verification with less gas than the fixed cost is metered as out-of-gas.
    #[test]
    fn dispatch_verify_out_of_gas() {
        let v = vector(7, 0x42);
        let res = dispatch(&encode_verify(&v.sig, &v.pubkey, v.address), VERIFY_POP_GAS_COST - 1);
        assert!(matches!(res, Err(PrecompileError::OutOfGas)));
    }

    /// Unknown selectors and truncated calldata are rejected.
    #[test]
    fn dispatch_rejects_unknown_selector_and_short_input() {
        // unknown 4-byte selector + padding
        let mut unknown = vec![0xDE, 0xAD, 0xBE, 0xEF];
        unknown.extend_from_slice(&[0u8; 32]);
        assert!(dispatch(&unknown, VERIFY_POP_GAS_COST).is_err());

        // fewer than 4 bytes cannot carry a selector
        assert!(dispatch(&[0x01, 0x02], VERIFY_POP_GAS_COST).is_err());
        assert!(dispatch(&[], VERIFY_POP_GAS_COST).is_err());
    }
}
