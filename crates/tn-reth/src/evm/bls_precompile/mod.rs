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
//! [`add_bls_precompile`] and dispatched by 4-byte selector. The ABI matches `IBlsG1`, so
//! `ConsensusRegistry`'s `blsVerify` staticcall resolves to this precompile.
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
/// Matches `BLS_G1_ADDRESS` in `tn-contracts/src/interfaces/IBlsG1.sol`. `ConsensusRegistry`
/// staticcalls this address, so its BLS verification lands here.
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

/// Base gas charged for a BLS signature verification, independent of message length.
///
/// Priced to reflect the equivalent EIP-2537 work the verification represents: a 2-pairing check
/// (`37_700 + 2 * 32_600 = 102_900`) plus point decompression and the fixed portion of
/// hash-to-curve, rounded up. This keeps the on-chain cost proportional to the cryptography while
/// remaining well within a normal transaction's gas budget (`stake` / `delegateStake` run with a 1M
/// default limit). The native implementation completes in microseconds; the charge exists for
/// metering, not compute time.
///
/// The length-dependent portion of hash-to-curve is charged separately by
/// [`BLS_VERIFY_PER_WORD_GAS_COST`]; the total for a given message is [`bls_verify_gas_cost`].
///
/// The base is the least any call pays. It is not checked on its own: [`handle_bls_verify`] checks
/// the full [`bls_verify_gas_cost`] of the raw argument length before it decodes the calldata.
const BLS_VERIFY_GAS_COST: u64 = 150_000;

/// Per-32-byte-word surcharge over the raw ABI-encoded arguments, not over the message alone.
///
/// The rate is the `SHA256` precompile's 12 gas per word, which is also four times the EVM's
/// 3-gas-per-word copy rate (`CALLDATACOPY`, the `identity` precompile). Each class of word pays
/// for one cost, so the two variable-length costs are not double counted.
///
/// The decode copies the three `bytes` fields out of calldata into owned `Bytes`. Each field lies
/// inside the raw arguments, so each copy is at most the raw argument length, and the three copies
/// together are at most three times it: `abi_decode_raw` does not check the head offsets, so all
/// three fields may point at one tail. Signature and pubkey words are unbounded and are never
/// hashed, so their 12 gas per word pays for a copy that costs at most 9 gas per word even when the
/// three heads alias.
///
/// Message words pay the same 12 gas for the `expand_message_xmd` SHA-256 expansion of
/// [`bls_verify_secure`], which is the `SHA256` precompile's own rate for that hashing. The copy of
/// the message on top of that hash is bounded by [`MAX_BLS_VERIFY_MESSAGE_LEN`]: at most 139 raw
/// words at 3 gas per word, under 1,300 gas even three-way aliased, which the
/// [`BLS_VERIFY_GAS_COST`] base absorbs. Under the old message-priced charge a message word paid
/// this same 12 gas, so no call pays less than it did before.
const BLS_VERIFY_PER_WORD_GAS_COST: u64 = 12;

/// Upper bound on the `message` the precompile will hash.
///
/// A generous ceiling - far above any realistic signed message, and orders of magnitude above the
/// sole on-chain caller's fixed 119-byte proof-of-possession message - that keeps `blsVerify` a
/// general primitive while foreclosing an unbounded hash-to-curve request charged through the
/// per-word rate. It is enforced on the decoded message, after the decode.
///
/// The charge no longer depends on it: [`handle_bls_verify`] computes the charge from the raw
/// argument length before the decode, and the checked arithmetic in [`bls_verify_gas_cost`] keeps
/// that charge total for any length.
const MAX_BLS_VERIFY_MESSAGE_LEN: usize = 4096;

/// Total gas for a call whose ABI-encoded arguments span `calldata_len` bytes: the flat
/// [`BLS_VERIFY_GAS_COST`] base plus [`BLS_VERIFY_PER_WORD_GAS_COST`] for each 32-byte word of
/// those arguments (rounded up). The 4-byte selector is not priced.
///
/// Returns `None` only on arithmetic overflow. That is unreachable for any calldata a block can
/// carry, because a word count that overflows the `u64` charge needs far more bytes than a block
/// gas limit can pay to supply. The checked arithmetic keeps the function total anyway, so the
/// caller meters the impossible case as `OutOfGas` instead of panicking.
fn bls_verify_gas_cost(calldata_len: usize) -> Option<u64> {
    u64::try_from(calldata_len.div_ceil(32))
        .ok()
        .and_then(|words| words.checked_mul(BLS_VERIFY_PER_WORD_GAS_COST))
        .and_then(|word_gas| BLS_VERIFY_GAS_COST.checked_add(word_gas))
}

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
///
/// Gas is gated once, on the raw argument length, before any byte is decoded.
///
/// The decode is the only O(calldata) work in the precompile that no earlier charge covers: it
/// copies all three `bytes` fields out of calldata into owned `Bytes`. Each field lies inside the
/// raw arguments, so each copy is at most the raw argument length, and the three copies together
/// are at most three times it: `abi_decode_raw` is the non-validating decoder, so all three heads
/// may point at one tail. The raw length is known before the decode, so it fixes the price and
/// [`bls_verify_gas_cost`] applies it. The O(1) floor added in #1215 bounded only how many decodes
/// a block can buy, not how large each one is: with the floor met, the precompile's own charge
/// stayed at 150,000 gas however large the `signature` it allocated and copied on every executing
/// node.
///
/// The rate is [`BLS_VERIFY_PER_WORD_GAS_COST`], the `SHA256` precompile rate and four times the
/// EVM's 3-gas-per-word copy rate (`CALLDATACOPY`, the `identity` precompile). Each class of word
/// pays for one cost. Signature and pubkey words are unbounded and are never hashed, so their 12
/// gas per word pays for a copy that costs at most 9 gas per word even when the three heads alias.
/// Message words pay the same 12 gas for the `expand_message_xmd` hash, at the `SHA256`
/// precompile's own rate; the copy of the message on top of that is bounded by
/// [`MAX_BLS_VERIFY_MESSAGE_LEN`] and the [`BLS_VERIFY_GAS_COST`] base absorbs it. Under the old
/// message-priced charge a message word paid this same 12 gas, so no call pays less than before.
///
/// There is no ceiling on the raw length, only a charge. A ceiling of
/// [`MAX_BLS_VERIFY_MESSAGE_LEN`] on the raw length would reject canonical calls, because the
/// canonical envelope around a message adds 352 bytes: three head words of 96, the signature tail
/// of 32 plus 64, the pubkey tail of 32 plus 96, and the message length word of 32. Such a ceiling
/// would refuse every canonical message above 3744 bytes. #1215 also recorded that a raw ceiling
/// turns today's `Ok(false)` for an oversized pubkey into a halt. Pricing the raw length instead
/// makes the copy pay for itself, the way `sha256` and `identity` price their input length without
/// capping it. The message cap stays after the decode, because it bounds the hash-to-curve input of
/// the generic primitive and not the copy.
///
/// The observable change is that `gas_used` now follows the raw argument length. The
/// `ConsensusRegistry` proof-of-possession call (48-byte signature, 96-byte pubkey, 119-byte
/// message, 480 argument bytes, 15 words) goes from 150,048 to 150,180 gas. Its Solidity
/// `staticcall` forwards all remaining gas, so it is unaffected. A call funded between the old
/// message-priced cost and the new raw-priced cost is now `OutOfGas`. For a fully funded call the
/// accepted set is unchanged, because the decode, the cap, and the verification are the same. An
/// under-funded call is `OutOfGas` before the decode whatever its bytes hold. An over-cap message
/// with the cost funded is `Other`.
fn handle_bls_verify(calldata: &[u8], gas_limit: u64) -> PrecompileResult {
    let cost = bls_verify_gas_cost(calldata.len())
        .filter(|cost| gas_limit >= *cost)
        .ok_or(PrecompileError::OutOfGas)?;

    let decoded = blsVerifyCall::abi_decode_raw(calldata)
        .map_err(|e| PrecompileError::Other(format!("blsVerify: {e}").into()))?;

    (decoded.message.len() <= MAX_BLS_VERIFY_MESSAGE_LEN).then_some(()).ok_or_else(|| {
        PrecompileError::Other("blsVerify: message exceeds maximum length".into())
    })?;

    // Reuse the exact crypto the consensus layer uses to *produce* signatures, so signer and
    // verifier can never disagree. A malformed point or failed verification yields `false` (not
    // a revert), matching `BlsG1.blsVerify`'s boolean contract; the caller
    // (`ConsensusRegistry`) is what turns `false` into its own `InvalidProofOfPossession`
    // revert.
    let verified = bls_verify(&decoded.signature, &decoded.pubkey, &decoded.message);

    Ok(PrecompileOutput::new(cost, Bytes::from(verified.abi_encode())))
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
    use alloy::primitives::U256;
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

    /// The gas an encoded call must fund: the charge over its ABI-encoded arguments, which are the
    /// encoded call without its leading 4-byte selector. The selector is not priced.
    fn raw_cost(call: &[u8]) -> u64 {
        bls_verify_gas_cost(call.len().saturating_sub(4)).expect("cost fits u64")
    }

    /// Decodes the ABI-encoded `bool` returned by a `blsVerify` call, pinning the return to the
    /// canonical 32-byte word rather than only its decoded value.
    ///
    /// The decoder is more permissive than the ABI it decodes, so decoding alone would not catch a
    /// regression in the return encoding. It reads one word and ignores every trailing byte, so an
    /// over-wide return still decodes (`vec![false].abi_encode()` decodes as `true` - the leading
    /// offset word is read as the bool), and it detokenizes any non-zero word to `true`, so a
    /// dirty word such as `0x00..02` decodes as `true` too. The validating `abi_decode_validate`
    /// closes neither: it only requires the leading 31 bytes to be zero. The round-trip equality -
    /// not the decode - is therefore what pins the width and canonicity.
    fn decode_bool(bytes: &[u8]) -> bool {
        let decoded = <bool as SolValue>::abi_decode(bytes).expect("decode bool return");
        assert_eq!(bytes, decoded.abi_encode(), "return is the canonical 32-byte ABI bool");
        decoded
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

    /// A valid signature through the full ABI path returns ABI-encoded `true` and charges the
    /// raw-calldata cost: the flat base plus the per-word surcharge over the 480 argument bytes
    /// (15 words) of the fixed proof-of-possession call. The call is funded at exactly that charge,
    /// so this pins the current caller's cost and the inclusive boundary of the gate.
    #[test]
    fn dispatch_verify_valid_returns_true() {
        let v = vector(7, 0x42);
        assert_eq!(v.message.len(), 119, "PoP message is the fixed intentPrefix||pubkey||address");
        let encoded = encode_verify(&v.sig, &v.pubkey, &v.message);
        assert_eq!(
            encoded.len().saturating_sub(4),
            480,
            "the PoP call encodes to 480 argument bytes, which is 15 words"
        );
        let expected = raw_cost(&encoded);
        assert_eq!(expected, BLS_VERIFY_GAS_COST + 15 * BLS_VERIFY_PER_WORD_GAS_COST);

        let out = dispatch(&encoded, expected).expect("dispatch ok");
        assert!(decode_bool(&out.bytes));
        assert_eq!(out.gas_used, expected);
    }

    /// An invalid signature returns ABI-encoded `false` rather than reverting: the precompile
    /// mirrors `BlsG1.blsVerify`'s boolean contract, leaving the revert to `ConsensusRegistry`.
    #[test]
    fn dispatch_verify_invalid_returns_false_not_revert() {
        let v = vector(7, 0x42);
        let other = vector(7, 0x43);
        let encoded = encode_verify(&v.sig, &v.pubkey, &other.message);
        let expected = raw_cost(&encoded);
        let out = dispatch(&encoded, expected).expect("dispatch ok (false, not error)");
        assert!(!decode_bool(&out.bytes));
        // gas is still charged for the work performed
        assert_eq!(out.gas_used, expected);
    }

    /// Verification with one gas less than the raw-calldata charge is metered as out-of-gas. The
    /// budget is above the base, so the refusal comes from the per-word surcharge and not from the
    /// base alone.
    #[test]
    fn dispatch_verify_out_of_gas() {
        let v = vector(7, 0x42);
        let encoded = encode_verify(&v.sig, &v.pubkey, &v.message);
        let cost = raw_cost(&encoded);
        assert!(cost > BLS_VERIFY_GAS_COST, "the arguments must carry a per-word surcharge");
        let res = dispatch(&encoded, cost.saturating_sub(1));
        assert!(matches!(res, Err(PrecompileError::OutOfGas)));
    }

    /// The whole raw-calldata charge is checked before the calldata is decoded. An under-funded
    /// call is `OutOfGas` even when its calldata would not decode or carries an over-cap message,
    /// and the same bytes funded at exactly the charge reach the decoder and fail as `Other`.
    ///
    /// The 1 MiB case is the discriminator against the #1215 floor. `BLS_VERIFY_GAS_COST` admitted
    /// that call to the decoder before this change, which copied every byte of it for a flat
    /// charge. It is now refused before the decode.
    #[test]
    fn dispatch_charges_raw_calldata_before_decode() {
        // Three head words of 0xff: offsets far past the end of the data, so decoding fails.
        let malformed = [blsVerifyCall::SELECTOR.as_slice(), [0xffu8; 96].as_slice()].concat();
        let malformed_cost = raw_cost(&malformed);
        assert_eq!(malformed_cost, BLS_VERIFY_GAS_COST + 3 * BLS_VERIFY_PER_WORD_GAS_COST);
        assert!(matches!(dispatch(&malformed, 0), Err(PrecompileError::OutOfGas)));
        assert!(matches!(
            dispatch(&malformed, malformed_cost.saturating_sub(1)),
            Err(PrecompileError::OutOfGas)
        ));
        // Positive control: funded at the charge, the decoder runs and rejects the bytes, so the
        // `OutOfGas` above comes from the charge and not from a decoder that never saw the input.
        assert!(matches!(dispatch(&malformed, malformed_cost), Err(PrecompileError::Other(_))));

        // A canonically encoded 1 MiB message: 32_779 argument words, so the raw charge is far
        // above the old floor.
        let big_message = vec![0u8; 1 << 20];
        let over = encode_verify(&[0u8; 48], &[0u8; 96], &big_message);
        let over_cost = raw_cost(&over);
        assert_eq!(over_cost, BLS_VERIFY_GAS_COST + 32_779 * BLS_VERIFY_PER_WORD_GAS_COST);
        assert!(matches!(dispatch(&over, 0), Err(PrecompileError::OutOfGas)));
        // The #1215 floor admitted this call to the decoder. It no longer does.
        assert!(matches!(dispatch(&over, BLS_VERIFY_GAS_COST), Err(PrecompileError::OutOfGas)));
        // Positive control: funded at the charge, the decode runs and the message cap rejects it.
        assert!(matches!(dispatch(&over, over_cost), Err(PrecompileError::Other(_))));
    }

    /// The charge scales with the raw argument length, not with the message alone. Three calls hold
    /// a 32-byte message fixed and vary only bytes the old message-priced charge ignored: a wider
    /// `signature`, a wider `pubkey`, and trailing bytes the decoder discards. Each delta over the
    /// canonical call is exactly the per-word surcharge over the 10 extra argument words. Under
    /// message-priced metering all four calls cost the same, so this pins raw-length pricing.
    #[test]
    fn dispatch_meters_by_raw_calldata_length() {
        let message = vec![0u8; 32];
        let sig = [0u8; 48];
        let pubkey = [0u8; 96];
        // 48 bytes pad to a 64-byte tail and 368 bytes to a 384-byte tail: 10 extra words.
        let wide_sig_bytes = [0u8; 368];
        // 96 bytes and 416 bytes are already word-aligned: 10 extra words.
        let wide_pubkey_bytes = [0u8; 416];
        let gas = 10_000_000;

        let canonical = encode_verify(&sig, &pubkey, &message);
        // The decoder ignores trailing bytes, but the caller still made the precompile carry them.
        let padded = [canonical.as_slice(), [0u8; 320].as_slice()].concat();

        let base = dispatch(&canonical, gas).expect("canonical call ok");
        let wide_sig = dispatch(&encode_verify(&wide_sig_bytes, &pubkey, &message), gas)
            .expect("wide signature ok");
        let wide_pubkey = dispatch(&encode_verify(&sig, &wide_pubkey_bytes, &message), gas)
            .expect("wide pubkey ok");
        let trailing = dispatch(&padded, gas).expect("trailing bytes ok");

        // Every call verifies `false` (the points are all zero), so only the metering differs.
        assert!(!decode_bool(&base.bytes));
        assert_eq!(base.gas_used, raw_cost(&canonical));

        let assert_delta = |label: &str, out: &PrecompileOutput| {
            assert!(!decode_bool(&out.bytes), "{label}: an all-zero point verifies false");
            assert_eq!(
                out.gas_used.saturating_sub(base.gas_used),
                10 * BLS_VERIFY_PER_WORD_GAS_COST,
                "{label}: 10 extra argument words cost exactly the per-word surcharge"
            );
        };
        assert_delta("wider signature", &wide_sig);
        assert_delta("wider pubkey", &wide_pubkey);
        assert_delta("trailing bytes", &trailing);
    }

    /// Aliased heads triple the copy but not the price, and the rate margin covers that.
    /// `abi_decode_raw` is the non-validating decoder, so it does not reject three `bytes` heads
    /// that all point at one tail: 160 argument bytes make the decoder copy the same 32-byte tail
    /// three times. The charge is still read off the raw length, and 12 gas per word is four times
    /// the EVM's 3-gas-per-word copy rate, so five words of surcharge pay for a three-way aliased
    /// copy with margin.
    #[test]
    fn dispatch_prices_aliased_heads_by_raw_length() {
        // Three head words of 0x60: every field points at the single tail at offset 96.
        let head = U256::from(96).to_be_bytes::<32>();
        let tail_len = U256::from(32).to_be_bytes::<32>();
        let call = [
            blsVerifyCall::SELECTOR.as_slice(),
            head.as_slice(),
            head.as_slice(),
            head.as_slice(),
            tail_len.as_slice(),
            [0x11u8; 32].as_slice(),
        ]
        .concat();

        let args = call.get(4..).expect("the call carries its arguments");
        assert_eq!(
            args.len(),
            160,
            "three head words plus one 32-byte tail is five argument words"
        );

        // The decoder accepts the aliased heads, so one tail is copied into all three fields.
        let decoded = blsVerifyCall::abi_decode_raw(args).expect("aliased heads decode");
        assert_eq!(decoded.signature.len(), 32, "each field is the one 32-byte tail");
        assert_eq!(decoded.signature, decoded.pubkey, "signature and pubkey alias one tail");
        assert_eq!(decoded.pubkey, decoded.message, "message aliases the same tail");

        let cost = raw_cost(&call);
        assert_eq!(cost, BLS_VERIFY_GAS_COST + 5 * BLS_VERIFY_PER_WORD_GAS_COST);

        // 32 bytes is not a valid compressed signature, so the call is `Ok(false)` and charged.
        let out = dispatch(&call, cost).expect("aliased heads dispatch ok");
        assert!(!decode_bool(&out.bytes), "a 32-byte signature verifies false");
        assert_eq!(out.gas_used, cost, "the price follows the raw length, not the copied length");
    }

    /// `bls_verify_gas_cost` is the flat base plus the per-word surcharge, rounding the argument
    /// length up to whole 32-byte words (empty arguments are the bare base; 1..=32 bytes is one
    /// word).
    #[test]
    fn bls_verify_gas_cost_charges_per_word() {
        assert_eq!(bls_verify_gas_cost(0), Some(BLS_VERIFY_GAS_COST));
        // Concrete-rate anchor: pin the surcharge to its literal value, so zeroing the rate
        // constant - which the constant-symbolic assertions below would not catch - fails here.
        assert_eq!(bls_verify_gas_cost(32), Some(BLS_VERIFY_GAS_COST + 12));
        assert_eq!(
            bls_verify_gas_cost(1),
            Some(BLS_VERIFY_GAS_COST + BLS_VERIFY_PER_WORD_GAS_COST)
        );
        assert_eq!(
            bls_verify_gas_cost(32),
            Some(BLS_VERIFY_GAS_COST + BLS_VERIFY_PER_WORD_GAS_COST)
        );
        assert_eq!(
            bls_verify_gas_cost(33),
            Some(BLS_VERIFY_GAS_COST + 2 * BLS_VERIFY_PER_WORD_GAS_COST)
        );
        // the proof-of-possession call encodes to 480 argument bytes, which is exactly 15 words
        assert_eq!(
            bls_verify_gas_cost(480),
            Some(BLS_VERIFY_GAS_COST + 15 * BLS_VERIFY_PER_WORD_GAS_COST)
        );
    }

    /// The charge also scales with `message.len()`, because the message tail is word-padded, so one
    /// extra message word is one extra argument word. Two calls that differ only in message length
    /// are metered apart by exactly the per-word surcharge over the extra words. Well-formed
    /// sig/pubkey lengths with an invalid (all-zero) key make verification return `false`, but the
    /// length-dependent gas is charged regardless, which is what is measured here.
    #[test]
    fn dispatch_meters_by_message_length() {
        let sig = [0u8; 48];
        let pubkey = [0u8; 96];
        let short = vec![0u8; 32]; // 1 word
        let long = vec![0u8; 32 + 32 * 10]; // 11 words

        let out_short =
            dispatch(&encode_verify(&sig, &pubkey, &short), 1_000_000).expect("short ok");
        let out_long = dispatch(&encode_verify(&sig, &pubkey, &long), 1_000_000).expect("long ok");

        assert!(!decode_bool(&out_short.bytes));
        assert!(!decode_bool(&out_long.bytes));
        assert!(out_long.gas_used > out_short.gas_used, "longer message must cost more gas");
        assert_eq!(
            out_long.gas_used - out_short.gas_used,
            10 * BLS_VERIFY_PER_WORD_GAS_COST,
            "delta is exactly the per-word charge over the 10 extra words"
        );
    }

    /// The message length is capped: a message at `MAX_BLS_VERIFY_MESSAGE_LEN` is accepted (and
    /// charged), while one byte over is rejected as an `Other` error - not charged-and-run - even
    /// with ample gas, proving the cap (not the gas gate) is the enforcement.
    #[test]
    fn dispatch_rejects_oversized_message() {
        let sig = [0u8; 48];
        let pubkey = [0u8; 96];
        let at_cap = vec![0u8; MAX_BLS_VERIFY_MESSAGE_LEN];
        let over_cap = vec![0u8; MAX_BLS_VERIFY_MESSAGE_LEN + 1];

        // At the cap: accepted. Verification returns `false` (invalid key), charged the full cost.
        let encoded = encode_verify(&sig, &pubkey, &at_cap);
        let cost = raw_cost(&encoded);
        let out = dispatch(&encoded, cost).expect("at-cap accepted");
        assert!(!decode_bool(&out.bytes));
        assert_eq!(out.gas_used, cost);

        // One byte over the cap: rejected as an error even with far more gas than any charge, so
        // it is the length bound - not out-of-gas - doing the rejecting.
        let res = dispatch(&encode_verify(&sig, &pubkey, &over_cap), 10_000_000);
        assert!(
            matches!(res, Err(PrecompileError::Other(_))),
            "over-cap message must be rejected, not charged and run"
        );
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
