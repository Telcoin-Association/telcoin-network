//! Native BLS12-381 proof-of-possession precompile.
//!
//! Replaces the Solidity `BlsG1` library (linked at [`BLS_G1_PRECOMPILE_ADDRESS`], `0x…b151`) with a
//! native implementation that delegates to the **same** `blst` (`min_sig`) verification the
//! consensus layer uses to *produce* proofs of possession
//! ([`tn_types::verify_proof_of_possession_bls`]). Having one implementation behind both signing and
//! verification removes the byte-for-byte drift risk between the Rust signer and an independent
//! on-chain reimplementation.
//!
//! # Structure
//!
//! Mirrors [`tel_precompile`](super::tel_precompile): a [`DynPrecompile`] registered via
//! [`add_bls_precompile`] and dispatched by 4-byte selector. The ABI matches `BlsG1.sol`, so
//! `ConsensusRegistry`'s existing library `delegatecall`s resolve to this precompile unchanged.
//!
//! | Selector | Behavior |
//! |----------|----------|
//! | `verifyProofOfPossession(bytes,bytes,address)` | Verify a PoP from uncompressed G1 sig + G2 pubkey. The one crypto entrypoint. |
//! | `proofOfPossessionMessage(bytes,address)` | Return the exact bytes the PoP is signed/verified over (used for revert reasons). |
//!
//! # Encoding
//!
//! The signature/pubkey arguments are the protocol's own `blst::min_sig` `serialize()` output (96-byte
//! uncompressed G1 signature, 192-byte uncompressed G2 pubkey) - the identical bytes the genesis
//! assembly and `stake`/`delegateStake` callers already pass to `BlsG1`. The precompile feeds them
//! straight back into `blst` via [`BlsSignature::from_uncompressed_bytes`] /
//! [`BlsPublicKey::from_uncompressed_bytes`]; no EIP-2537 re-encoding or point (de)compression is
//! performed here.
use alloy::{
    primitives::address,
    sol,
    sol_types::{SolCall, SolValue},
};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use reth_revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use tn_types::{
    proof_of_possession_message_bytes, verify_proof_of_possession_bls, Address, BlsPublicKey,
    BlsSignature, Bytes,
};

/// Canonical address of the BLS proof-of-possession precompile: `0x…b151`.
///
/// Matches `BLS_G1_ADDRESS` in `tn-contracts/src/consensus/BlsG1.sol`. `ConsensusRegistry` is linked
/// against this address at genesis, so its `BlsG1.*` library `delegatecall`s land here.
pub const BLS_G1_PRECOMPILE_ADDRESS: Address =
    address!("000000000000000000000000000000000000b151");

sol! {
    /// Verifies a validator's BLS12-381 proof of possession from raw uncompressed inputs.
    ///
    /// `uncompressedSignature`: 96-byte uncompressed G1 point. `uncompressedPubkey`: 192-byte
    /// uncompressed G2 point. Both as produced by `blst::min_sig` `serialize`.
    function verifyProofOfPossession(
        bytes uncompressedSignature,
        bytes uncompressedPubkey,
        address validatorAddress
    ) external view returns (bool);

    /// Returns the proof-of-possession message bytes a validator signs / is verified against.
    function proofOfPossessionMessage(
        bytes uncompressedPubkey,
        address validatorAddress
    ) external pure returns (bytes);
}

/// Gas charged for a proof-of-possession verification.
///
/// Priced to reflect the equivalent EIP-2537 work the verification represents: a 2-pairing check
/// (`37_700 + 2 * 32_600 = 102_900`) plus hash-to-curve and point decoding, rounded up. This keeps
/// the on-chain cost proportional to the cryptography while remaining well within a normal
/// transaction's gas budget (`stake` / `delegateStake` run with a 1M default limit). The native
/// implementation completes in microseconds; the charge exists for metering, not compute time.
const VERIFY_POP_GAS_COST: u64 = 150_000;

/// Gas charged for building the proof-of-possession message (pure serialization, no pairing).
const POP_MESSAGE_GAS_COST: u64 = 5_000;

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

/// Top-level dispatcher: extracts the 4-byte selector from calldata and routes to the handler.
fn bls_precompile(input: PrecompileInput<'_>) -> PrecompileResult {
    if input.data.len() < 4 {
        return Err(PrecompileError::Other("Invalid input: too short".into()));
    }

    let selector: [u8; 4] = input.data[0..4].try_into().unwrap();
    let calldata = &input.data[4..];

    match selector {
        verifyProofOfPossessionCall::SELECTOR => handle_verify_pop(calldata, input.gas),
        proofOfPossessionMessageCall::SELECTOR => handle_pop_message(calldata, input.gas),
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
    let verified = verify_pop(
        &decoded.uncompressedSignature,
        &decoded.uncompressedPubkey,
        decoded.validatorAddress,
    );

    Ok(PrecompileOutput::new(VERIFY_POP_GAS_COST, Bytes::from(verified.abi_encode())))
}

/// Decode the uncompressed inputs and run the `blst` proof-of-possession check. Any decode or
/// verification failure maps to `false` so a bad proof can never panic or revert the precompile.
fn verify_pop(uncompressed_sig: &[u8], uncompressed_pubkey: &[u8], address: Address) -> bool {
    let Ok(pubkey) = BlsPublicKey::from_uncompressed_bytes(uncompressed_pubkey) else {
        return false;
    };
    let Ok(sig) = BlsSignature::from_uncompressed_bytes(uncompressed_sig) else {
        return false;
    };

    verify_proof_of_possession_bls(&sig, &pubkey, &address).is_ok()
}

/// `proofOfPossessionMessage(bytes,address) -> bytes`.
///
/// Returns the canonical message bytes (`encode(IntentMessage)`) the PoP is verified against, so the
/// message reported here and the message verified by [`handle_verify_pop`] are produced by the same
/// code. A malformed pubkey reverts (it cannot be the basis of any valid message).
fn handle_pop_message(calldata: &[u8], gas_limit: u64) -> PrecompileResult {
    if gas_limit < POP_MESSAGE_GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }

    let decoded = proofOfPossessionMessageCall::abi_decode_raw(calldata)
        .map_err(|e| PrecompileError::Other(format!("proofOfPossessionMessage: {e}").into()))?;

    let pubkey = BlsPublicKey::from_uncompressed_bytes(&decoded.uncompressedPubkey)
        .map_err(|e| PrecompileError::Other(format!("invalid uncompressed pubkey: {e:?}").into()))?;
    let message = proof_of_possession_message_bytes(&pubkey, &decoded.validatorAddress)
        .map_err(|e| PrecompileError::Other(format!("message build failed: {e}").into()))?;

    Ok(PrecompileOutput::new(POP_MESSAGE_GAS_COST, Bytes::from(Bytes::from(message).abi_encode())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{generate_proof_of_possession_bls_for_test, BlsKeypair};

    /// A well-formed proof of possession verifies through the same `blst` path the signer used.
    #[test]
    fn verify_pop_accepts_valid_proof() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([7; 32]));
        let address = Address::repeat_byte(0x42);
        let proof = generate_proof_of_possession_bls_for_test(&keypair, &address)
            .expect("generate test PoP");

        // serialize() yields the uncompressed bytes the protocol passes to BlsG1.
        let sig_bytes = proof.serialize();
        let pubkey_bytes = keypair.public().serialize();

        assert!(verify_pop(&sig_bytes, &pubkey_bytes, address));
    }

    /// A proof bound to a different address must fail (the address is part of the signed message).
    #[test]
    fn verify_pop_rejects_wrong_address() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([7; 32]));
        let address = Address::repeat_byte(0x42);
        let proof = generate_proof_of_possession_bls_for_test(&keypair, &address)
            .expect("generate test PoP");

        let sig_bytes = proof.serialize();
        let pubkey_bytes = keypair.public().serialize();

        assert!(!verify_pop(&sig_bytes, &pubkey_bytes, Address::repeat_byte(0x43)));
    }

    /// Structurally invalid points return `false`, never panic.
    #[test]
    fn verify_pop_rejects_garbage() {
        assert!(!verify_pop(&[0u8; 96], &[0u8; 192], Address::ZERO));
        assert!(!verify_pop(&[], &[], Address::ZERO));
    }
}
