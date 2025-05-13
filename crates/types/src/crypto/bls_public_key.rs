//! Implement thin wrappers around BLST crates public keys.

use blst::{min_sig::PublicKey as CorePublicKey, BLST_ERROR};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// Byte representation of validator's main protocol public key.
/// This should ONLY be created from a valid key and comtain valid bytes.
/// Not enforcing this may cause the From trait to panic.
#[derive(Copy, Clone)]
pub struct BlsPublicKeyBytes([u8; 96]);

impl std::hash::Hash for BlsPublicKeyBytes {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for BlsPublicKeyBytes {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for BlsPublicKeyBytes {}

impl PartialOrd for BlsPublicKeyBytes {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlsPublicKeyBytes {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl std::fmt::Debug for BlsPublicKeyBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl std::fmt::Display for BlsPublicKeyBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

// Validator's main protocol public key.
#[derive(Copy, Clone)]
pub struct BlsPublicKey {
    pubkey: CorePublicKey,
    bytes: BlsPublicKeyBytes,
}

impl Default for BlsPublicKey {
    fn default() -> Self {
        let pubkey = CorePublicKey::default();
        let mut bytes = [0_u8; 96];
        bytes.copy_from_slice(&pubkey.to_bytes());
        Self { pubkey, bytes: BlsPublicKeyBytes(bytes) }
    }
}

impl From<CorePublicKey> for BlsPublicKey {
    fn from(pubkey: CorePublicKey) -> Self {
        let mut bytes = [0_u8; 96];
        bytes.copy_from_slice(&pubkey.to_bytes());
        Self { pubkey, bytes: BlsPublicKeyBytes(bytes) }
    }
}

impl BlsPublicKey {
    /// Encode the public key to base58. This is used for serialize/deserialize.
    pub fn encode_base58(&self) -> String {
        self.to_string()
    }

    /// Decode the public key from bytes on-chain and return result to caller.
    ///
    /// WARNING: do not use this method to deserialize bytes from filesystem.
    /// This method is only used to convert the literal bytes for the pubkey.
    pub fn from_literal_bytes(bytes: &[u8]) -> Result<Self, BLST_ERROR> {
        CorePublicKey::from_bytes(bytes).map(|key| key.into())
    }
}

impl std::hash::Hash for BlsPublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialEq for BlsPublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.pubkey == other.pubkey
    }
}

impl Eq for BlsPublicKey {}

impl PartialOrd for BlsPublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlsPublicKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl std::fmt::Debug for BlsPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let bytes: BlsPublicKeyBytes = self.into();
        write!(f, "{bytes}")
    }
}

impl std::fmt::Display for BlsPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let bytes: BlsPublicKeyBytes = self.into();
        write!(f, "{bytes}")
    }
}

impl AsRef<[u8]> for BlsPublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.bytes.0
    }
}

impl Deref for BlsPublicKey {
    type Target = blst::min_sig::PublicKey;

    fn deref(&self) -> &Self::Target {
        &self.pubkey
    }
}

impl From<BlsPublicKey> for BlsPublicKeyBytes {
    fn from(value: BlsPublicKey) -> Self {
        let mut bytes = [0_u8; 96];
        bytes.copy_from_slice(&value.to_bytes());
        Self(bytes)
    }
}

impl From<&BlsPublicKey> for BlsPublicKeyBytes {
    fn from(value: &BlsPublicKey) -> Self {
        let mut bytes = [0_u8; 96];
        bytes.copy_from_slice(&value.to_bytes());
        Self(bytes)
    }
}

impl From<BlsPublicKeyBytes> for BlsPublicKey {
    fn from(bytes: BlsPublicKeyBytes) -> Self {
        Self {
            pubkey: blst::min_sig::PublicKey::from_bytes(&bytes.0)
                .expect("valid BLS public key bytes"),
            bytes,
        }
    }
}

impl From<&BlsPublicKeyBytes> for BlsPublicKey {
    fn from(bytes: &BlsPublicKeyBytes) -> Self {
        Self {
            pubkey: blst::min_sig::PublicKey::from_bytes(&bytes.0)
                .expect("valid BLS public key bytes"),
            bytes: *bytes,
        }
    }
}

// ----- Serde implementations -----

impl Serialize for BlsPublicKeyBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl Serialize for BlsPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes: BlsPublicKeyBytes = self.into();
        bytes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BlsPublicKeyBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::*;

        struct BlsPublicKeyBytesVisitor;

        impl Visitor<'_> for BlsPublicKeyBytesVisitor {
            type Value = BlsPublicKeyBytes;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "valid bls public key bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // Deserialize into an actual BLS publix key so we are sure to have valid bytes.
                let pubkey: CorePublicKey = blst::min_sig::PublicKey::deserialize(v)
                    .map_err(|_| Error::invalid_value(Unexpected::Bytes(v), &self))?;
                let pubkey: BlsPublicKey = pubkey.into();
                Ok(pubkey.into())
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
            deserializer.deserialize_str(BlsPublicKeyBytesVisitor)
        } else {
            deserializer.deserialize_bytes(BlsPublicKeyBytesVisitor)
        }
    }
}

impl<'de> Deserialize<'de> for BlsPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(BlsPublicKeyBytes::deserialize(deserializer)?.into())
    }
}
