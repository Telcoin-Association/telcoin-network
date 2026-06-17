//! Crypto functions for bls signatures.

use super::{BlsKeypair, BlsPublicKey, Intent, IntentMessage, IntentScope, Signer, DST_G1};
use crate::encode;
use alloy::primitives::Address;
use blst::min_sig::{
    AggregateSignature as CoreBlsAggregateSignature, Signature as CoreBlsSignature,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

/// Validator's main protocol key signature.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BlsSignature(CoreBlsSignature);

/// Validator's main protocol key aggrigate signature.
/// Collection of validator main protocol key signatures.
#[derive(Clone, Copy)]
pub struct BlsAggregateSignature(CoreBlsAggregateSignature);

impl BlsSignature {
    pub fn from_bytes(bytes: &[u8]) -> eyre::Result<Self> {
        let sig = CoreBlsSignature::from_bytes(bytes)
            .map_err(|e| eyre::eyre!("Invalid signature bytes! {e:?}"))?;
        Ok(Self(sig))
    }

    /// Verify a signature over a message (raw bytes) with public key.
    pub fn verify_raw(&self, message: &[u8], public_key: &BlsPublicKey) -> bool {
        self.verify(true, message, DST_G1, &[], public_key, true) == blst::BLST_ERROR::BLST_SUCCESS
    }
}

impl Deref for BlsSignature {
    type Target = CoreBlsSignature;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<CoreBlsSignature> for BlsSignature {
    fn from(value: CoreBlsSignature) -> Self {
        Self(value)
    }
}

impl From<&CoreBlsSignature> for BlsSignature {
    fn from(value: &CoreBlsSignature) -> Self {
        Self(*value)
    }
}

impl From<BlsSignature> for CoreBlsSignature {
    fn from(value: BlsSignature) -> Self {
        value.0
    }
}

impl From<&BlsSignature> for CoreBlsSignature {
    fn from(value: &BlsSignature) -> Self {
        value.0
    }
}

impl std::fmt::Debug for BlsSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_bytes()).into_string())
    }
}

impl std::fmt::Display for BlsSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_bytes()).into_string())
    }
}

impl Default for BlsSignature {
    /// Create a default [BlsSignature] using the infinity point.
    /// See more: https://github.com/supranational/blst#serialization-format
    fn default() -> Self {
        // Setting the first byte to 0xc0 (1100), the first bit represents its in compressed form,
        // the second bit represents its infinity point.
        let mut infinity = [0_u8; 48];
        infinity[0] = 0xc0;

        BlsSignature::from_bytes(&infinity).expect("decode infinity signature")
    }
}

impl BlsAggregateSignature {
    // Aggregate
    pub fn aggregate(sigs: &[BlsSignature], sigs_groupcheck: bool) -> eyre::Result<Self> {
        let t_sigs: Vec<CoreBlsSignature> = sigs.iter().map(|s| s.0).collect();
        let sigs: Vec<&CoreBlsSignature> = t_sigs.iter().collect();
        let sig = CoreBlsAggregateSignature::aggregate(&sigs, sigs_groupcheck)
            .map_err(|_| eyre::eyre!("Failed to aggregate signatures!"))?;
        Ok(Self(sig))
    }

    pub fn to_signature(&self) -> BlsSignature {
        BlsSignature(CoreBlsAggregateSignature::to_signature(self))
    }

    pub fn from_signature(signature: &BlsSignature) -> BlsAggregateSignature {
        BlsAggregateSignature(CoreBlsAggregateSignature::from_signature(&signature.0))
    }
}
impl Deref for BlsAggregateSignature {
    type Target = CoreBlsAggregateSignature;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for BlsAggregateSignature {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<CoreBlsAggregateSignature> for BlsAggregateSignature {
    fn from(value: CoreBlsAggregateSignature) -> Self {
        Self(value)
    }
}

impl From<&CoreBlsAggregateSignature> for BlsAggregateSignature {
    fn from(value: &CoreBlsAggregateSignature) -> Self {
        Self(*value)
    }
}

impl std::fmt::Debug for BlsAggregateSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_signature().to_bytes()).into_string())
    }
}

impl std::fmt::Display for BlsAggregateSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_signature().to_bytes()).into_string())
    }
}

/// Creates a proof of that the authority account address is owned by the
/// holder of authority protocol key, and also ensures that the authority
/// protocol public key exists.
///
/// The proof of possession is a [BlsSignature] committed over the intent message
/// `intent || message` (See more at [IntentMessage] and [Intent]).
/// The message is constructed as: EIP2537([BlsPublicKey]) || [Address].
/// Where the public key is uncompressed with G2 point coordinates padded to 64-byte EVM words
/// This is only for testing because it takes a private key.  For prod code use
/// KeyConfig.generate_proof_of_possesion().
pub fn generate_proof_of_possession_bls_for_test(
    keypair: &BlsKeypair,
    address: &Address,
) -> eyre::Result<BlsSignature> {
    let msg = construct_proof_of_possession_message(keypair.public(), address);
    Ok(keypair.sign(&msg))
}

/// Verify a validator's BLS proof of possession: that `proof` is a valid signature, under
/// `public_key`, over [`construct_proof_of_possession_message`] for `address`. Reconstructs the
/// expected message (binding the key and the address) and verifies it through the generic
/// [`bls_verify_secure`] primitive.
pub fn verify_proof_of_possession_bls(
    proof: &BlsSignature,
    public_key: &BlsPublicKey,
    address: &Address,
) -> eyre::Result<()> {
    public_key.validate().map_err(|_| eyre::eyre!("Bls Public Key not valid!"))?;
    let msg = construct_proof_of_possession_message(public_key, address);
    if bls_verify_secure(proof, public_key, &msg) {
        Ok(())
    } else {
        Err(eyre::eyre!("Failed to verify proof of possession!"))
    }
}

/// Verify a BLS signature over a raw `message` under `public_key`, with full security (signature and
/// public-key subgroup checks) and the protocol [`DST_G1`]. This is the generic primitive the native
/// BLS precompile exposes: proof-of-possession is just one message it can verify, so any caller can
/// verify an arbitrary BLS-signed message without the verifier hard-coding the message's meaning.
pub fn bls_verify_secure(
    signature: &BlsSignature,
    public_key: &BlsPublicKey,
    message: &[u8],
) -> bool {
    signature.verify(true, message, DST_G1, &[], public_key, true) == blst::BLST_ERROR::BLST_SUCCESS
}

/// The proof-of-possession message a validator signs to prove control of its BLS key and bind it to
/// its execution `address`. Layout: `intentPrefix(3) || compressedBlsPubkey(96) || address(20)` =
/// 119 raw bytes, where `intentPrefix` is the serialized telcoin proof-of-possession [`Intent`]
/// (`0x000000`). Using the compressed pubkey and the raw address makes this cheaply reconstructable
/// on-chain, so the `ConsensusRegistry` builds the identical bytes and verifies them through the
/// native BLS precompile ([`bls_verify_secure`]). Returned as raw bytes (not wrapped in an
/// [`IntentMessage`]) since both the signer and the on-chain verifier operate on the bytes directly.
pub fn construct_proof_of_possession_message(
    bls_pubkey: &BlsPublicKey,
    address: &Address,
) -> Vec<u8> {
    let mut message = encode(&Intent::telcoin(IntentScope::ProofOfPossession));
    message.extend_from_slice(bls_pubkey.to_bytes().as_slice());
    message.extend_from_slice(address.as_slice());
    message
}

/// A trait for sign and verify over an intent message, instead of the message itself. See more at
/// [struct IntentMessage].
pub trait ProtocolSignature {
    /// Create a new signature over an intent message.
    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer) -> Self
    where
        T: Serialize;

    /// Verify the signature over an intent message against a public key.
    fn verify_secure<T>(&self, value: &IntentMessage<T>, public_key: &BlsPublicKey) -> bool
    where
        T: Serialize;

    /// Create a new signature over a raw byte array,
    /// such as one produced by `construct_proof_of_possession_message`
    fn new_secure_bytes(&self, msg: &[u8], secret: &dyn Signer) -> Self;

    /// Verify the signature over an intent message against a public key.
    fn verify_secure_bytes(&self, value: &[u8], public_key: &BlsPublicKey) -> bool;
}

impl ProtocolSignature for BlsSignature {
    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer) -> Self
    where
        T: Serialize,
    {
        let message = encode(&value);
        secret.sign(&message)
    }

    fn new_secure_bytes(&self, msg: &[u8], secret: &dyn Signer) -> Self {
        secret.sign(msg)
    }

    fn verify_secure<T>(&self, value: &IntentMessage<T>, public_key: &BlsPublicKey) -> bool
    where
        T: Serialize,
    {
        let message = encode(&value);
        self.verify(true, &message, DST_G1, &[], public_key, true) == blst::BLST_ERROR::BLST_SUCCESS
    }

    fn verify_secure_bytes(&self, value: &[u8], public_key: &BlsPublicKey) -> bool {
        self.verify(false, value, DST_G1, &[], public_key, true) == blst::BLST_ERROR::BLST_SUCCESS
    }
}

pub trait ValidatorAggregateSignature {
    fn verify_secure<T>(&self, value: &IntentMessage<T>, pks: &[BlsPublicKey]) -> bool
    where
        T: Serialize;
}

impl ValidatorAggregateSignature for BlsAggregateSignature {
    fn verify_secure<T>(&self, value: &IntentMessage<T>, pks: &[BlsPublicKey]) -> bool
    where
        T: Serialize,
    {
        if pks.is_empty() {
            return false;
        }
        let message = encode(&value);
        let mut pk_s: Vec<&blst::min_sig::PublicKey> = Vec::with_capacity(pks.len());
        let mut messages = Vec::with_capacity(pks.len());
        for pk in pks {
            pk_s.push(pk.deref());
            messages.push(&message[..]);
        }
        self.to_signature().aggregate_verify(true, &messages, DST_G1, &pk_s, true)
            == blst::BLST_ERROR::BLST_SUCCESS
    }
}

// ----- Serde implementations -----

impl Serialize for BlsSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_bytes(&self.0.to_bytes())
        }
    }
}

impl<'de> Deserialize<'de> for BlsSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::*;

        struct BlsSignatureVisitor;

        impl Visitor<'_> for BlsSignatureVisitor {
            type Value = BlsSignature;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "valid bls public key bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // Deserialize into an actual BLS publix key so we are sure to have valid bytes.
                let sig = CoreBlsSignature::from_bytes(v)
                    .map_err(|_| Error::invalid_value(Unexpected::Bytes(v), &self))?;
                Ok(sig.into())
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = bs58::decode(v)
                    .into_vec()
                    .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(BlsSignatureVisitor)
        } else {
            deserializer.deserialize_bytes(BlsSignatureVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{to_intent_message, BlsKeypair};
    use blst::min_sig::Signature as CoreBlsSignature;
    use rand::{rngs::StdRng, SeedableRng};

    fn make_keypair() -> BlsKeypair {
        BlsKeypair::generate(&mut StdRng::from_os_rng())
    }

    fn make_intent_msg() -> IntentMessage<Vec<u8>> {
        to_intent_message(b"test payload".to_vec())
    }

    /// The PoP message is byte-stable regardless of how the `BlsPublicKey` was decoded: a key
    /// decoded from its 96-byte compressed form rebuilds a byte-identical message. This is the
    /// basis of the no-regeneration guarantee - switching the transport encoding to compressed
    /// does not change the signed message, so every previously issued proof of possession still
    /// verifies.
    #[test]
    fn construct_pop_message_stable_across_compressed_roundtrip() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([3u8; 32]));
        let address = Address::repeat_byte(0x11);
        let original = keypair.public();
        // decode the same key from its compressed bytes (the path the precompile uses)
        let from_compressed =
            BlsPublicKey::from_literal_bytes(&original.to_bytes()).expect("compressed roundtrip");

        let msg_a = construct_proof_of_possession_message(original, &address);
        let msg_b = construct_proof_of_possession_message(&from_compressed, &address);
        assert_eq!(msg_a, msg_b, "compressed-decoded key rebuilds a byte-identical PoP message");
    }

    /// Guards the on-chain message layout: `ConsensusRegistry` reconstructs this exact byte string
    /// (`intentPrefix(3 = 0x000000) || compressedPubkey(96) || address(20)`), so any drift in the
    /// layout or the intent prefix must be mirrored on-chain.
    #[test]
    fn pop_message_layout_is_onchain_constructible() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([7u8; 32]));
        let address = Address::repeat_byte(0x22);
        let msg = construct_proof_of_possession_message(keypair.public(), &address);

        assert_eq!(msg.len(), 3 + 96 + 20, "PoP message is intent(3) + pubkey(96) + address(20)");
        assert_eq!(
            &msg[0..3],
            &[0u8, 0, 0],
            "telcoin proof-of-possession intent prefix is 0x000000"
        );
        assert_eq!(
            &msg[3..99],
            keypair.public().to_bytes().as_slice(),
            "compressed pubkey segment"
        );
        assert_eq!(&msg[99..119], address.as_slice(), "address segment");
    }

    // --- BlsSignature::from_bytes ---

    #[test]
    fn test_from_bytes_valid() {
        let kp = make_keypair();
        let sig = kp.sign(b"hello");
        let bytes = sig.to_bytes();
        let sig2 = BlsSignature::from_bytes(&bytes).expect("roundtrip should succeed");
        assert_eq!(sig, sig2);
    }

    #[test]
    fn test_from_bytes_invalid() {
        assert!(BlsSignature::from_bytes(&[0u8; 10]).is_err());
    }

    // --- BlsSignature::default (infinity point) ---

    #[test]
    fn test_default_is_infinity_point() {
        let sig = BlsSignature::default();
        let bytes = sig.to_bytes();
        // First byte 0xc0 = compressed (bit 7) + infinity (bit 6)
        assert_eq!(bytes[0], 0xc0);
    }

    // --- Debug / Display ---

    #[test]
    fn test_debug_and_display_match() {
        let kp = make_keypair();
        let sig = kp.sign(b"hello");
        let debug = format!("{:?}", sig);
        let display = format!("{}", sig);
        assert!(!debug.is_empty());
        assert_eq!(debug, display);
    }

    // --- Serde roundtrips ---

    #[test]
    fn test_serde_json_roundtrip() {
        let kp = make_keypair();
        let sig = kp.sign(b"hello");
        let json = serde_json::to_string(&sig).expect("serialize");
        let sig2: BlsSignature = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(sig, sig2);
    }

    #[test]
    fn test_bincode_roundtrip() {
        let kp = make_keypair();
        let sig = kp.sign(b"hello");
        let bytes = bincode::serialize(&sig).expect("serialize");
        let sig2: BlsSignature = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(sig, sig2);
    }

    // --- From / Into conversions ---

    #[test]
    fn test_from_core_bls_signature_roundtrip() {
        let kp = make_keypair();
        let bls_sig = kp.sign(b"hello");
        let core: CoreBlsSignature = bls_sig.into();
        let restored: BlsSignature = core.into();
        assert_eq!(bls_sig, restored);
    }

    #[test]
    fn test_from_ref_core_bls_signature() {
        let kp = make_keypair();
        let bls_sig = kp.sign(b"hello");
        let core: CoreBlsSignature = (&bls_sig).into();
        let restored: BlsSignature = (&core).into();
        assert_eq!(bls_sig, restored);
    }

    // --- verify_raw ---

    #[test]
    fn test_verify_raw_success() {
        let kp = make_keypair();
        let msg = b"raw message";
        let sig = kp.sign(msg);
        assert!(sig.verify_raw(msg, kp.public()));
    }

    #[test]
    fn test_verify_raw_wrong_key() {
        let kp1 = make_keypair();
        let kp2 = make_keypair();
        let msg = b"raw message";
        let sig = kp1.sign(msg);
        assert!(!sig.verify_raw(msg, kp2.public()));
    }

    #[test]
    fn test_verify_raw_wrong_message() {
        let kp = make_keypair();
        let sig = kp.sign(b"correct message");
        assert!(!sig.verify_raw(b"wrong message", kp.public()));
    }

    // --- ProtocolSignature: new_secure / verify_secure ---

    #[test]
    fn test_new_secure_verify_secure_success() {
        let kp = make_keypair();
        let msg = make_intent_msg();
        let sig = BlsSignature::new_secure(&msg, &kp);
        assert!(sig.verify_secure(&msg, kp.public()));
    }

    #[test]
    fn test_verify_secure_wrong_key() {
        let kp1 = make_keypair();
        let kp2 = make_keypair();
        let msg = make_intent_msg();
        let sig = BlsSignature::new_secure(&msg, &kp1);
        assert!(!sig.verify_secure(&msg, kp2.public()));
    }

    #[test]
    fn test_verify_secure_wrong_message() {
        let kp = make_keypair();
        let msg1 = to_intent_message(b"message one".to_vec());
        let msg2 = to_intent_message(b"message two".to_vec());
        let sig = BlsSignature::new_secure(&msg1, &kp);
        assert!(!sig.verify_secure(&msg2, kp.public()));
    }

    // --- ProtocolSignature: new_secure_bytes / verify_secure_bytes ---

    #[test]
    fn test_new_secure_bytes_verify_secure_bytes_success() {
        let kp = make_keypair();
        let msg = b"raw secure bytes";
        let dummy = BlsSignature::default();
        let sig = dummy.new_secure_bytes(msg, &kp);
        assert!(sig.verify_secure_bytes(msg, kp.public()));
    }

    #[test]
    fn test_verify_secure_bytes_wrong_key() {
        let kp1 = make_keypair();
        let kp2 = make_keypair();
        let msg = b"raw secure bytes";
        let dummy = BlsSignature::default();
        let sig = dummy.new_secure_bytes(msg, &kp1);
        assert!(!sig.verify_secure_bytes(msg, kp2.public()));
    }

    #[test]
    fn test_verify_secure_bytes_wrong_message() {
        let kp = make_keypair();
        let dummy = BlsSignature::default();
        let sig = dummy.new_secure_bytes(b"correct", &kp);
        assert!(!sig.verify_secure_bytes(b"wrong", kp.public()));
    }

    // --- BlsAggregateSignature ---

    #[test]
    fn test_aggregate_single_signature() {
        let kp = make_keypair();
        let sig = kp.sign(b"msg");
        assert!(BlsAggregateSignature::aggregate(&[sig], true).is_ok());
    }

    #[test]
    fn test_aggregate_multiple_signatures() {
        let sigs: Vec<BlsSignature> = (0..3).map(|_| make_keypair().sign(b"msg")).collect();
        assert!(BlsAggregateSignature::aggregate(&sigs, true).is_ok());
    }

    #[test]
    fn test_from_signature_to_signature_roundtrip() {
        let kp = make_keypair();
        let sig = kp.sign(b"hello");
        let agg = BlsAggregateSignature::from_signature(&sig);
        let restored = agg.to_signature();
        assert_eq!(sig, restored);
    }

    // --- ValidatorAggregateSignature::verify_secure ---

    #[test]
    fn test_aggregate_verify_secure_empty_pks_returns_false() {
        let kp = make_keypair();
        let msg = make_intent_msg();
        let sig = BlsSignature::new_secure(&msg, &kp);
        let agg = BlsAggregateSignature::aggregate(&[sig], true).unwrap();
        // Must return false when no public keys are provided
        assert!(!agg.verify_secure(&msg, &[]));
    }

    #[test]
    fn test_aggregate_verify_secure_single_signer() {
        let kp = make_keypair();
        let msg = make_intent_msg();
        let sig = BlsSignature::new_secure(&msg, &kp);
        let agg = BlsAggregateSignature::aggregate(&[sig], true).unwrap();
        assert!(agg.verify_secure(&msg, &[*kp.public()]));
    }

    #[test]
    fn test_aggregate_verify_secure_multiple_signers() {
        let kps: Vec<BlsKeypair> = (0..3).map(|_| make_keypair()).collect();
        let msg = make_intent_msg();
        let sigs: Vec<BlsSignature> =
            kps.iter().map(|kp| BlsSignature::new_secure(&msg, kp)).collect();
        let pks: Vec<BlsPublicKey> = kps.iter().map(|kp| *kp.public()).collect();
        let agg = BlsAggregateSignature::aggregate(&sigs, true).unwrap();
        assert!(agg.verify_secure(&msg, &pks));
    }

    #[test]
    fn test_aggregate_verify_secure_wrong_pk_returns_false() {
        let kp1 = make_keypair();
        let kp2 = make_keypair();
        let msg = make_intent_msg();
        let sig = BlsSignature::new_secure(&msg, &kp1);
        let agg = BlsAggregateSignature::aggregate(&[sig], true).unwrap();
        assert!(!agg.verify_secure(&msg, &[*kp2.public()]));
    }

    #[test]
    fn test_aggregate_verify_secure_wrong_message_returns_false() {
        let kp = make_keypair();
        let msg1 = to_intent_message(b"message one".to_vec());
        let msg2 = to_intent_message(b"message two".to_vec());
        let sig = BlsSignature::new_secure(&msg1, &kp);
        let agg = BlsAggregateSignature::aggregate(&[sig], true).unwrap();
        assert!(!agg.verify_secure(&msg2, &[*kp.public()]));
    }
}
