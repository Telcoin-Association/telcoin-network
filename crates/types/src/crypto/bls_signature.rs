//! Crypto functions for bls signatures.

use super::{BlsKeypair, BlsPublicKey, Intent, IntentMessage, IntentScope, Signer, DST_G1};
use crate::encode;
use alloy::primitives::Address;
use blst::min_sig::{
    AggregateSignature as CoreBlsAggregateSignature, Signature as CoreBlsSignature,
};
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Deref};

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
            .map_err(|_| eyre::eyre!("Invalid signature bytes!"))?;
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
    pub fn aggregate(sigs: &[&BlsSignature], sigs_groupcheck: bool) -> eyre::Result<Self> {
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
/// The message is constructed as: [BlsPublicKey] || [Genesis].
pub fn generate_proof_of_possession_bls(
    keypair: &BlsKeypair,
    address: &Address,
) -> eyre::Result<BlsSignature> {
    let mut msg = keypair.public().to_bytes().to_vec();
    let address_bytes = encode(address);
    msg.extend_from_slice(address_bytes.as_slice());
    let sig = BlsSignature::new_secure(
        &IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg),
        keypair,
    );
    Ok(sig)
}

/// Verify proof of possession against the expected intent message,
///
/// The intent message is expected to contain the validator's public key
/// and the [Genesis] for the network.
pub fn verify_proof_of_possession_bls(
    proof: &BlsSignature,
    public_key: &BlsPublicKey,
    address: &Address,
) -> eyre::Result<()> {
    public_key.validate().map_err(|_| eyre::eyre!("Bls Publkic Key not valid!"))?;
    let mut msg = public_key.to_bytes().to_vec();
    let address_bytes = encode(&address);
    msg.extend_from_slice(address_bytes.as_slice());
    if proof.verify_secure(
        &IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg),
        public_key,
    ) {
        Ok(())
    } else {
        Err(eyre::eyre!("Failed to verify proof of possession!"))
    }
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
}

impl ProtocolSignature for BlsSignature {
    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer) -> Self
    where
        T: Serialize,
    {
        let message = encode(&value);
        secret.sign(&message)
    }

    fn verify_secure<T>(&self, value: &IntentMessage<T>, public_key: &BlsPublicKey) -> bool
    where
        T: Serialize,
    {
        let message = encode(&value);
        self.verify(true, &message, DST_G1, &[], public_key, true) == blst::BLST_ERROR::BLST_SUCCESS
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
            return true;
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

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
