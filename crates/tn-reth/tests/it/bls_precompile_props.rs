//! Property-based tests for the native BLS proof-of-possession precompile (`0x…b151`).
//!
//! These exercise the precompile through the real EVM call path (the same one
//! `ConsensusRegistry`'s linked `BlsG1` library reaches via `delegatecall`), verifying its
//! observable contract across randomized inputs:
//! - A correctly-generated proof of possession verifies; wrong address / wrong key / wrong
//!   signature do not.
//! - Malformed or wrong-length point bytes (including the valid uncompressed encodings) return
//!   `false` rather than reverting (matching `BlsG1.verifyProofOfPossession`'s boolean contract),
//!   and never panic.
//! - Calldata validation: short calldata and unknown selectors revert.

use alloy::{primitives::address, sol, sol_types::SolCall};
use proptest::prelude::*;
use rand::{rngs::StdRng, SeedableRng};
use reth_revm::primitives::Address;
use tn_reth::test_utils::precompile_test_utils::{assert_not_success, decode_bool, TestEnv, USER};
use tn_types::{generate_proof_of_possession_bls_for_test, BlsKeypair, Bytes};

sol! {
    function verifyProofOfPossession(
        bytes signature,
        bytes pubkey,
        address validatorAddress
    ) external view returns (bool);
}

/// Canonical address the BLS precompile is registered at (matches `BlsG1`'s link address). The
/// integration crate cannot see the crate-internal constant, so it is pinned here independently -
/// which also guards against the address silently drifting.
const BLS_G1_PRECOMPILE_ADDRESS: Address = address!("000000000000000000000000000000000000b151");

/// Gas limit for verify calls. The precompile charges 150k for a verification, so the default
/// 100k harness limit is insufficient; mirror the 1M budget `stake`/`delegateStake` run with.
const VERIFY_GAS: u64 = 1_000_000;

/// A valid proof-of-possession vector: the compressed `blst::min_sig` `to_bytes()` bytes (48-byte
/// G1 signature, 96-byte G2 pubkey) - the exact bytes the protocol passes to `BlsG1` / this
/// precompile.
struct Vector {
    sig: Vec<u8>,
    pubkey: Vec<u8>,
}

/// Builds a valid proof of possession for `address` from a deterministic RNG seed.
fn vector(seed: [u8; 32], address: Address) -> Vector {
    let keypair = BlsKeypair::generate(&mut StdRng::from_seed(seed));
    let proof =
        generate_proof_of_possession_bls_for_test(&keypair, &address).expect("generate test PoP");
    let sig = proof.to_bytes().to_vec();
    let pubkey = keypair.public().to_bytes().to_vec();
    Vector { sig, pubkey }
}

/// ABI-encodes a `verifyProofOfPossession` call.
fn verify_calldata(sig: &[u8], pubkey: &[u8], address: Address) -> Vec<u8> {
    verifyProofOfPossessionCall {
        signature: Bytes::copy_from_slice(sig),
        pubkey: Bytes::copy_from_slice(pubkey),
        validatorAddress: address,
    }
    .abi_encode()
}

/// Executes `verifyProofOfPossession` against the precompile and returns the decoded `bool`.
///
/// Asserts the call itself succeeded - an invalid proof returns `Ok(false)`, not a revert, so a
/// revert here would be a contract violation and fails the test inside `decode_bool`.
fn verify(env: &mut TestEnv, sig: &[u8], pubkey: &[u8], address: Address) -> bool {
    let result = env.exec_to(
        USER,
        BLS_G1_PRECOMPILE_ADDRESS,
        verify_calldata(sig, pubkey, address),
        VERIFY_GAS,
    );
    decode_bool(&result)
}

// ==============================
// Verification invariants
// ==============================

proptest! {
    /// A correctly-generated proof of possession always verifies.
    #[test]
    fn prop_valid_pop_verifies(seed in any::<[u8; 32]>(), addr in any::<[u8; 20]>()) {
        let address = Address::from(addr);
        let v = vector(seed, address);
        let mut env = TestEnv::new();
        prop_assert!(verify(&mut env, &v.sig, &v.pubkey, address));
    }

    /// A proof bound to one address never verifies for a different address.
    #[test]
    fn prop_wrong_address_rejected(
        seed in any::<[u8; 32]>(),
        addr_a in any::<[u8; 20]>(),
        addr_b in any::<[u8; 20]>(),
    ) {
        prop_assume!(addr_a != addr_b);
        let bound = Address::from(addr_a);
        let other = Address::from(addr_b);
        let v = vector(seed, bound);
        let mut env = TestEnv::new();
        prop_assert!(!verify(&mut env, &v.sig, &v.pubkey, other));
    }

    /// A valid signature never verifies against a substituted public key.
    #[test]
    fn prop_pubkey_substitution_rejected(
        seed_a in any::<[u8; 32]>(),
        seed_b in any::<[u8; 32]>(),
        addr in any::<[u8; 20]>(),
    ) {
        prop_assume!(seed_a != seed_b);
        let address = Address::from(addr);
        let a = vector(seed_a, address);
        let b = vector(seed_b, address);
        let mut env = TestEnv::new();
        // a's signature, b's pubkey -> must fail
        prop_assert!(!verify(&mut env, &a.sig, &b.pubkey, address));
    }

    /// A signature from a different key never verifies against the original pubkey.
    #[test]
    fn prop_wrong_signature_rejected(
        seed_a in any::<[u8; 32]>(),
        seed_b in any::<[u8; 32]>(),
        addr in any::<[u8; 20]>(),
    ) {
        prop_assume!(seed_a != seed_b);
        let address = Address::from(addr);
        let a = vector(seed_a, address);
        let b = vector(seed_b, address);
        let mut env = TestEnv::new();
        // b's signature, a's pubkey -> must fail
        prop_assert!(!verify(&mut env, &b.sig, &a.pubkey, address));
    }

    /// Random, correctly-sized point bytes return `false` (not a revert) and never panic. The ABI
    /// encoding is well-formed, so the precompile decodes it and reports a failed verification.
    #[test]
    fn prop_garbage_points_return_false(
        sig in prop::collection::vec(any::<u8>(), 48),
        pubkey in prop::collection::vec(any::<u8>(), 96),
        addr in any::<[u8; 20]>(),
    ) {
        let address = Address::from(addr);
        let mut env = TestEnv::new();
        // Astronomically unlikely to be a valid PoP; the invariant is "false, never panic/revert".
        prop_assert!(!verify(&mut env, &sig, &pubkey, address));
    }

    /// Wrong-length pubkey bytes (anything but the 96-byte compressed G2 form, including the
    /// 192-byte uncompressed form) return `false` against an otherwise valid signature.
    #[test]
    fn prop_wrong_length_pubkey_returns_false(
        seed in any::<[u8; 32]>(),
        addr in any::<[u8; 20]>(),
        bad_len in 0usize..256,
    ) {
        prop_assume!(bad_len != 96);
        let address = Address::from(addr);
        let v = vector(seed, address);
        let mut env = TestEnv::new();
        prop_assert!(!verify(&mut env, &v.sig, &vec![0xABu8; bad_len], address));
    }

    /// Wrong-length signature bytes (anything but the 48-byte compressed G1 form, including the
    /// 96-byte uncompressed form) return `false` against an otherwise valid pubkey.
    #[test]
    fn prop_wrong_length_signature_returns_false(
        seed in any::<[u8; 32]>(),
        addr in any::<[u8; 20]>(),
        bad_len in 0usize..200,
    ) {
        prop_assume!(bad_len != 48);
        let address = Address::from(addr);
        let v = vector(seed, address);
        let mut env = TestEnv::new();
        prop_assert!(!verify(&mut env, &vec![0xABu8; bad_len], &v.pubkey, address));
    }
}

// ==============================
// Compressed-only enforcement (S1)
// ==============================

/// The precompile is compressed-only: a *valid* 96-byte uncompressed signature and 192-byte
/// uncompressed pubkey - the pre-change encoding - are rejected by the length gate. blst's
/// `deserialize` would otherwise accept these valid points, so this (not random wrong-length bytes)
/// is the proof that the gate is the enforcement.
#[test]
fn test_valid_uncompressed_inputs_rejected() {
    let address = Address::repeat_byte(0x42);
    let keypair = BlsKeypair::generate(&mut StdRng::from_seed([7; 32]));
    let proof =
        generate_proof_of_possession_bls_for_test(&keypair, &address).expect("generate test PoP");

    let uncompressed_sig = proof.serialize().to_vec();
    let uncompressed_pubkey = keypair.public().serialize().to_vec();
    assert_eq!(uncompressed_sig.len(), 96, "uncompressed G1 signature");
    assert_eq!(uncompressed_pubkey.len(), 192, "uncompressed G2 pubkey");

    let mut env = TestEnv::new();
    // the compressed control verifies (the key/proof are otherwise valid)
    assert!(
        verify(
            &mut env,
            &proof.to_bytes().to_vec(),
            &keypair.public().to_bytes().to_vec(),
            address
        ),
        "compressed control",
    );
    // ...but the valid uncompressed encodings are gated out
    assert!(
        !verify(&mut env, &uncompressed_sig, &uncompressed_pubkey, address),
        "uncompressed gated"
    );
}

// ==============================
// Calldata validation
// ==============================

/// The selectors the precompile implements.
fn known_selectors() -> [[u8; 4]; 1] {
    [verifyProofOfPossessionCall::SELECTOR]
}

proptest! {
    /// Unknown function selectors revert.
    #[test]
    fn prop_unknown_selector_fails(selector_val in any::<u32>()) {
        let selector = selector_val.to_be_bytes();
        prop_assume!(!known_selectors().contains(&selector));

        let mut data = Vec::with_capacity(36);
        data.extend_from_slice(&selector);
        data.extend_from_slice(&[0u8; 32]);

        let mut env = TestEnv::new();
        let result = env.exec_to(USER, BLS_G1_PRECOMPILE_ADDRESS, data, VERIFY_GAS);
        assert_not_success(&result);
    }

    /// Calldata too short to ABI-decode the arguments reverts (the selector is valid but the
    /// dynamic `bytes`/`address` arguments cannot be parsed).
    #[test]
    fn prop_short_calldata_fails(len in 0usize..32) {
        let mut data = Vec::with_capacity(4 + len);
        data.extend_from_slice(&verifyProofOfPossessionCall::SELECTOR);
        data.extend(std::iter::repeat_n(0u8, len));

        let mut env = TestEnv::new();
        let result = env.exec_to(USER, BLS_G1_PRECOMPILE_ADDRESS, data, VERIFY_GAS);
        assert_not_success(&result);
    }
}

// ==============================
// `DELEGATECALL` relay (the `ConsensusRegistry` path)
// ==============================

/// Address hosting the `DELEGATECALL` relay contract.
const RELAY_ADDR: Address = address!("dddd0000000000000000000000000000000000b1");

/// Minimal runtime bytecode that forwards calldata to `0x…b151` via `DELEGATECALL` and returns the
/// precompile's output verbatim. This is exactly how `ConsensusRegistry`'s linked `BlsG1` library
/// reaches the precompile, so it proves that integration path resolves to our native code.
///
/// Disassembly:
/// ```text
///   CALLDATASIZE; PUSH1 0; PUSH1 0; CALLDATACOPY      // mem[0..csize] = calldata
///   PUSH1 0; PUSH1 0; CALLDATASIZE; PUSH1 0;          // retSize, retOffset, argsSize, argsOffset
///   PUSH2 0xb151; GAS; DELEGATECALL; POP              // delegatecall to BLS precompile
///   RETURNDATASIZE; PUSH1 0; PUSH1 0; RETURNDATACOPY  // mem[0..rsize] = returndata
///   RETURNDATASIZE; PUSH1 0; RETURN                   // return mem[0..rsize]
/// ```
const RELAY_BYTECODE: &[u8] = &[
    0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATACOPY(0, 0, CALLDATASIZE)
    0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00, 0x61, 0xb1, 0x51, 0x5a, 0xf4,
    0x50, // PUSH2 0xb151; GAS; DELEGATECALL; POP
    0x3d, 0x60, 0x00, 0x60, 0x00, 0x3e, // RETURNDATACOPY(0, 0, RETURNDATASIZE)
    0x3d, 0x60, 0x00, 0xf3, // RETURN(0, RETURNDATASIZE)
];

/// A valid proof verifies, and a tampered one is rejected, when reached via `DELEGATECALL` - the
/// same way `ConsensusRegistry`'s `BlsG1.verifyProofOfPossession` library call lands here.
#[test]
fn test_delegatecall_verify_pop() {
    let address = Address::repeat_byte(0x42);
    let v = vector([7; 32], address);

    let mut env = TestEnv::new();
    env.deploy_code(RELAY_ADDR, Bytes::from_static(RELAY_BYTECODE));

    // Valid PoP through the relay -> true.
    let ok = env.exec_to(USER, RELAY_ADDR, verify_calldata(&v.sig, &v.pubkey, address), VERIFY_GAS);
    assert!(decode_bool(&ok), "valid PoP must verify via DELEGATECALL");

    // Wrong address through the relay -> false (still a successful call returning `false`).
    let bad = env.exec_to(
        USER,
        RELAY_ADDR,
        verify_calldata(&v.sig, &v.pubkey, Address::repeat_byte(0x43)),
        VERIFY_GAS,
    );
    assert!(!decode_bool(&bad), "wrong-address PoP must be rejected via DELEGATECALL");
}
