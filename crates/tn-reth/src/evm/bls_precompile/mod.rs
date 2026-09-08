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
//! `ConsensusRegistry`'s `blsVerify` staticcall resolves to this precompile. Like the TEL
//! dispatcher, the entrypoint enforces payability itself: registering the address short-circuits
//! the interpreter, so no compiled `CALLVALUE` guard ever runs, and no selector here is payable,
//! so nonzero attached value is rejected before dispatch (see [`bls_precompile`]).
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
use tn_types::{bls_verify_secure, Address, BlsPublicKey, BlsSignature, Bytes, U256};

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
/// The base is also the O(1) gas floor that [`handle_bls_verify`] checks before it decodes the
/// calldata: no accepted call pays less than this, so a call that cannot fund it is refused before
/// any of its `bytes` arguments are copied.
const BLS_VERIFY_GAS_COST: u64 = 150_000;

/// Per-32-byte-word surcharge over the hashed `message`.
///
/// `blsVerify` is a generic primitive, so it accepts a message of any length up to
/// [`MAX_BLS_VERIFY_MESSAGE_LEN`]. The message-dependent cost of [`bls_verify_secure`] is the
/// `expand_message_xmd` SHA-256 expansion, which hashes the message once; this rate mirrors the
/// `SHA256` precompile's 12-gas-per-word charge for the same variable-length hashing, so a longer
/// message pays for the extra work it induces instead of riding the flat base charge.
const BLS_VERIFY_PER_WORD_GAS_COST: u64 = 12;

/// Upper bound on the `message` the precompile will hash.
///
/// A generous ceiling - far above any realistic signed message, and orders of magnitude above the
/// sole on-chain caller's fixed 119-byte proof-of-possession message - that keeps `blsVerify` a
/// general primitive while foreclosing an unbounded hash-to-curve request charged through the
/// per-word rate. It also bounds [`bls_verify_gas_cost`]'s arithmetic so the charge cannot overflow
/// for any accepted message.
const MAX_BLS_VERIFY_MESSAGE_LEN: usize = 4096;

/// Total gas for verifying a `message_len`-byte message: the flat [`BLS_VERIFY_GAS_COST`] base plus
/// [`BLS_VERIFY_PER_WORD_GAS_COST`] for each 32-byte word of the message (rounded up).
///
/// Returns `None` on arithmetic overflow. That is unreachable for any `message_len` at or under
/// [`MAX_BLS_VERIFY_MESSAGE_LEN`] (the caller gates on the cap first), but the checked arithmetic
/// keeps the function total rather than panicking if a future caller ever skips the gate.
fn bls_verify_gas_cost(message_len: usize) -> Option<u64> {
    u64::try_from(message_len.div_ceil(32))
        .ok()
        .and_then(|words| words.checked_mul(BLS_VERIFY_PER_WORD_GAS_COST))
        .and_then(|word_gas| BLS_VERIFY_GAS_COST.checked_add(word_gas))
}

/// Registers the BLS precompile at [`BLS_G1_PRECOMPILE_ADDRESS`] in the given map.
///
/// Called from the EVM factory alongside `add_telcoin_precompile`, so the precompile is present for
/// all execution including pre-genesis registry construction.
///
/// Registered with [`DynPrecompile::new_stateful`] deliberately, and that choice is load-bearing
/// for the payability gate: reth's engine-tree precompile cache keys entries on calldata alone,
/// and only `new_stateful` (`is_pure() == false`) keeps this address out of that cache. The
/// result now depends on [`PrecompileInput::value`], so a cached zero-value `Ok` must never be
/// replayed for a value-bearing call with identical calldata; do not "optimize" this to
/// [`DynPrecompile::new`].
pub fn add_bls_precompile(map: &mut PrecompilesMap) {
    map.apply_precompile(&BLS_G1_PRECOMPILE_ADDRESS, move |_| {
        Some(DynPrecompile::new_stateful(PrecompileId::Custom("bls_g1".into()), move |input| {
            bls_precompile(input)
        }))
    });
}

/// The error reported when nonzero call value is attached to a call. No selector of this
/// precompile is payable. The text mirrors the TEL dispatcher's gate; the string itself is
/// diagnostic only, since on-chain both precompiles surface the rejection as the same bare
/// precompile-error halt.
const NON_PAYABLE_CALL_VALUE: &str = "call value: selector is not payable";

/// Precompile entrypoint. Rejects nonzero call value, then delegates to [`dispatch`]; beyond the
/// attached `value` it inspects for that gate, the precompile is stateless and never touches the
/// EVM internals carried by [`PrecompileInput`].
///
/// # Payability
///
/// Registering this address short-circuits the interpreter, so the `CALLVALUE` guard a compiled
/// non-payable dispatcher would run never executes; revm journals the caller-to-target balance
/// transfer before the precompile runs and commits it when the call succeeds. Without this gate a
/// value-bearing `blsVerify` call would both return its verification result and strand the
/// attached wei at [`BLS_G1_PRECOMPILE_ADDRESS`]: nothing reads or pays out that account's
/// balance, and unlike the TEL precompile there is not even a governance `burn` to destroy stray
/// value. `blsVerify` is pure, so the payable list is empty and any nonzero value is refused with
/// [`NON_PAYABLE_CALL_VALUE`], the same fail-safe classification as the TEL dispatcher: a
/// selector added later rejects value unless explicitly made payable. Inside a `DELEGATECALL`
/// frame `value` is the calling frame's apparent `msg.value` and nothing is transferred; a
/// `CALLCODE` frame transfers its explicit value operand from the executing contract to itself
/// (net zero), so neither indirect path can land wei here. Rejecting both still mirrors a
/// compiled non-payable dispatcher, whose `CALLVALUE` check fires in those frames too; what
/// matches Solidity is when the check fires, not how it reports. Like every error from
/// [`dispatch`], the rejection surfaces as a precompile-error halt that consumes the forwarded
/// gas, not a data-carrying Solidity revert.
fn bls_precompile(input: PrecompileInput<'_>) -> PrecompileResult {
    // No selector is payable: refuse attached value before dispatch. See "Payability" above.
    (input.value == U256::ZERO)
        .then_some(())
        .ok_or_else(|| PrecompileError::Other(NON_PAYABLE_CALL_VALUE.into()))?;
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
/// Gas is gated twice, cheapest first.
///
/// 1. An O(1) floor of [`BLS_VERIFY_GAS_COST`] runs before the calldata is decoded. The base is the
///    least any accepted call pays, so a call that cannot fund it is refused before its three
///    `bytes` arguments are copied out of calldata. That copy is the only O(calldata) work in the
///    precompile that no earlier charge covers: the precompile address is warm, and a `STATICCALL`
///    can reach it with 0 forwarded gas, so without the floor a warm call's 100 gas base buys an
///    arbitrarily large memcpy on every executing node (the `signature` and `pubkey` fields are
///    unbounded `bytes`, so the message cap alone does not bound the copy).
/// 2. The decode then yields the message length, which fixes the exact charge. The message is
///    capped at [`MAX_BLS_VERIFY_MESSAGE_LEN`] and charged [`bls_verify_gas_cost`] (base plus a
///    per-word surcharge), so a caller supplying a long message pays for the extra hash-to-curve
///    work it induces instead of riding the flat base.
///
/// An over-cap message is rejected as `Other`; an under-funded call is `OutOfGas` at whichever
/// gate it fails. Every accepted call clears both gates, so the floor changes neither the accepted
/// set nor `gas_used`. The one observable difference is that an under-funded call whose calldata
/// is malformed or carries an over-cap message is now `OutOfGas` instead of `Other`; the EVM treats
/// both the same way (the caller frame gets `0` and its forwarded gas is consumed).
fn handle_bls_verify(calldata: &[u8], gas_limit: u64) -> PrecompileResult {
    (gas_limit >= BLS_VERIFY_GAS_COST).then_some(()).ok_or(PrecompileError::OutOfGas)?;

    let decoded = blsVerifyCall::abi_decode_raw(calldata)
        .map_err(|e| PrecompileError::Other(format!("blsVerify: {e}").into()))?;

    let message_len = decoded.message.len();
    let cost = (message_len <= MAX_BLS_VERIFY_MESSAGE_LEN)
        .then_some(message_len)
        .ok_or_else(|| PrecompileError::Other("blsVerify: message exceeds maximum length".into()))
        .and_then(|len| bls_verify_gas_cost(len).ok_or(PrecompileError::OutOfGas))
        .and_then(|cost| (gas_limit >= cost).then_some(cost).ok_or(PrecompileError::OutOfGas))?;

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
    /// length-dependent cost: the flat base plus the per-word surcharge over the fixed 119-byte
    /// proof-of-possession message (4 words). This pins the current caller's cost as a regression
    /// guard - the 119-byte layout is under the cap and adds only a small, fixed delta.
    #[test]
    fn dispatch_verify_valid_returns_true() {
        let v = vector(7, 0x42);
        assert_eq!(v.message.len(), 119, "PoP message is the fixed intentPrefix||pubkey||address");
        let expected = bls_verify_gas_cost(v.message.len()).expect("cost fits u64");
        assert_eq!(expected, BLS_VERIFY_GAS_COST + 4 * BLS_VERIFY_PER_WORD_GAS_COST);

        let out =
            dispatch(&encode_verify(&v.sig, &v.pubkey, &v.message), expected).expect("dispatch ok");
        assert!(decode_bool(&out.bytes));
        assert_eq!(out.gas_used, expected);
    }

    /// An invalid signature returns ABI-encoded `false` rather than reverting: the precompile
    /// mirrors `BlsG1.blsVerify`'s boolean contract, leaving the revert to `ConsensusRegistry`.
    #[test]
    fn dispatch_verify_invalid_returns_false_not_revert() {
        let v = vector(7, 0x42);
        let other = vector(7, 0x43);
        let expected = bls_verify_gas_cost(other.message.len()).expect("cost fits u64");
        let out = dispatch(&encode_verify(&v.sig, &v.pubkey, &other.message), expected)
            .expect("dispatch ok (false, not error)");
        assert!(!decode_bool(&out.bytes));
        // gas is still charged for the work performed
        assert_eq!(out.gas_used, expected);
    }

    /// Verification with less gas than the length-dependent cost is metered as out-of-gas. The
    /// budget here (`cost - 1`) clears the O(1) floor and fails only the exact post-decode charge,
    /// so this pins the second gate independently of the first.
    #[test]
    fn dispatch_verify_out_of_gas() {
        let v = vector(7, 0x42);
        let cost = bls_verify_gas_cost(v.message.len()).expect("cost fits u64");
        assert!(cost > BLS_VERIFY_GAS_COST, "budget must clear the floor to reach the exact gate");
        let res = dispatch(&encode_verify(&v.sig, &v.pubkey, &v.message), cost - 1);
        assert!(matches!(res, Err(PrecompileError::OutOfGas)));
    }

    /// The O(1) gas floor is checked before the calldata is decoded. Under-funded calls are
    /// `OutOfGas` even when the calldata would not decode or carries an over-cap message, and the
    /// same bytes with the floor met reach the decoder and fail as `Other`. Restoring decode-first
    /// flips the under-funded cases to `Other`, so this pins the ordering.
    #[test]
    fn dispatch_gas_floor_precedes_decode() {
        // Three head words of 0xff: offsets far past the end of the data, so decoding fails.
        let malformed = [blsVerifyCall::SELECTOR.as_slice(), [0xffu8; 96].as_slice()].concat();
        assert!(matches!(dispatch(&malformed, 0), Err(PrecompileError::OutOfGas)));
        assert!(matches!(
            dispatch(&malformed, BLS_VERIFY_GAS_COST - 1),
            Err(PrecompileError::OutOfGas)
        ));
        // Positive control: with the floor met, the decoder runs and rejects the bytes, so the
        // `OutOfGas` above comes from the floor and not from a decoder that never saw the input.
        assert!(matches!(
            dispatch(&malformed, BLS_VERIFY_GAS_COST),
            Err(PrecompileError::Other(_))
        ));

        // An over-cap 1 MiB message under the floor is refused by the floor, not by the cap that
        // runs after the decode; with the floor met the same call reaches the cap.
        let big_message = vec![0u8; 1 << 20];
        let over = encode_verify(&[0u8; 48], &[0u8; 96], &big_message);
        assert!(matches!(dispatch(&over, 0), Err(PrecompileError::OutOfGas)));
        assert!(matches!(dispatch(&over, BLS_VERIFY_GAS_COST), Err(PrecompileError::Other(_))));
    }

    /// `bls_verify_gas_cost` is the flat base plus the per-word surcharge, rounding the message up
    /// to whole 32-byte words (an empty message is the bare base; 1..=32 bytes is one word).
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
        // the fixed 119-byte proof-of-possession message rounds up to 4 words
        assert_eq!(
            bls_verify_gas_cost(119),
            Some(BLS_VERIFY_GAS_COST + 4 * BLS_VERIFY_PER_WORD_GAS_COST)
        );
    }

    /// The charge scales with `message.len()`: two calls that differ only in message length are
    /// metered differently, by exactly the per-word surcharge over the extra words. On the previous
    /// flat charge both calls cost `BLS_VERIFY_GAS_COST`, so this fails there and pins the new
    /// length-dependent pricing. Well-formed sig/pubkey lengths with an invalid (all-zero) key make
    /// verification return `false`, but the length-dependent gas is charged regardless - which is
    /// what is measured here.
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
        let cost = bls_verify_gas_cost(at_cap.len()).expect("cost fits u64");
        let out = dispatch(&encode_verify(&sig, &pubkey, &at_cap), cost).expect("at-cap accepted");
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
