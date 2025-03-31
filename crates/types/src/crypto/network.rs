//! Crypto functions to help with new node handshake using network keys.

use std::{fmt, ops::Deref};

use serde::{Deserialize, Serialize};

use super::{Intent, IntentMessage, IntentScope};
use crate::{encode, Genesis};

/// Public key used to sign network messages between peers during consensus.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NetworkPublicKey(libp2p::identity::PublicKey);
/// Keypair used to sign network messages between peers during consensus.
pub type NetworkKeypair = libp2p::identity::Keypair;
/// Signature using network key.
pub type NetworkSignature = Vec<u8>;

impl NetworkPublicKey {}

impl From<libp2p::identity::PublicKey> for NetworkPublicKey {
    fn from(value: libp2p::identity::PublicKey) -> Self {
        Self(value)
    }
}

impl From<NetworkPublicKey> for libp2p::identity::PublicKey {
    fn from(value: NetworkPublicKey) -> Self {
        value.0
    }
}

impl Deref for NetworkPublicKey {
    type Target = libp2p::identity::PublicKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for NetworkPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(self.encode_protobuf()).into_string())
        } else {
            serializer.serialize_bytes(&self.encode_protobuf()[..])
        }
    }
}

impl<'de> Deserialize<'de> for NetworkPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::*;

        struct NetworkPublicKeyVisitor;

        impl Visitor<'_> for NetworkPublicKeyVisitor {
            type Value = NetworkPublicKey;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "valid network public key")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NetworkPublicKey(
                    libp2p::identity::PublicKey::try_decode_protobuf(v)
                        .map_err(|_| Error::invalid_value(Unexpected::Bytes(v), &self))?,
                ))
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
            deserializer.deserialize_str(NetworkPublicKeyVisitor)
        } else {
            deserializer.deserialize_bytes(NetworkPublicKeyVisitor)
        }
    }
}

/// Generate a proof for handshake.
///
/// This is used to verify network signatures for newly discovered peers.
///
/// The proof of possession is a [NetworkSignature] committed over the intent message
/// `intent || message` (See more at [IntentMessage] and [Intent]).
/// The message is constructed as: [NetworkPublicKey] || [Genesis].
pub fn generate_proof_of_possession_network(
    keypair: &NetworkKeypair,
    genesis: &Genesis,
) -> NetworkSignature {
    let mut msg = keypair.public().encode_protobuf();
    let genesis_bytes = encode(&genesis);
    msg.extend_from_slice(genesis_bytes.as_slice());
    let message = encode(&IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg));
    keypair.sign(&message).expect("failed to sign proof of possession")
}

/// Verify proof of possession against the expected intent message.
///
/// The intent message is expected to contain the validator's public key
/// and the [Genesis] for the network.
pub fn verify_proof_of_possession_network(
    proof: &NetworkSignature,
    public_key: &NetworkPublicKey,
    genesis: &Genesis,
) -> bool {
    let mut msg = public_key.encode_protobuf();
    let genesis_bytes = encode(genesis);
    msg.extend_from_slice(genesis_bytes.as_slice());
    let message = encode(&IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg));
    public_key.verify(&message, proof)
}
