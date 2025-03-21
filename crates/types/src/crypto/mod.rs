//! Type aliases selecting the signature algorithm for the code base.
//!
//! Here we select the types that are used by default in the code base.
//!
//! Guidelines:
//! - refer to these aliases always (avoid using the individual scheme implementations)
//! - use generic schemes (avoid using the algo's `Struct`` impl functions)
//! - change type aliases to update codebase with new crypto

use blake2::digest::consts::U32;
use eyre::Context;
use fastcrypto::{
    bls12381,
    error::FastCryptoError,
    traits::{AggregateAuthenticator, KeyPair, Signer, ToFromBytes, VerifyingKey},
};
use libp2p::PeerId;
use std::{fmt, future::Future, ops::Deref};
// This re-export allows using the trait-defined APIs
pub use fastcrypto::traits;
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
mod intent;
mod network;
use crate::encode;
pub use intent::*;
pub use network::*;

//
// CONSENSUS
//
/// Validator's main protocol public key.
pub type BlsPublicKey = bls12381::min_sig::BLS12381PublicKey;
/// Byte representation of validator's main protocol public key.
pub type BlsPublicKeyBytes = bls12381::min_sig::BLS12381PublicKeyAsBytes;
/// Validator's main protocol key signature.
pub type BlsSignature = bls12381::min_sig::BLS12381Signature;
/// Collection of validator main protocol key signatures.
pub type BlsAggregateSignature = bls12381::min_sig::BLS12381AggregateSignature;
/// Byte representation of the collection of validator main protocol key signatures.
pub type BlsAggregateSignatureBytes = bls12381::min_sig::BLS12381AggregateSignatureAsBytes;
/// Validator's main protocol private key.
pub type BlsPrivateKey = bls12381::min_sig::BLS12381PrivateKey;
/// Validator's main protocol keypair.
pub type BlsKeypair = bls12381::min_sig::BLS12381KeyPair;
//
// NETWORK
//
/// Public key used to sign network messages between peers during consensus.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NetworkPublicKey(libp2p::identity::PublicKey);
/// Keypair used to sign network messages between peers during consensus.
pub type NetworkKeypair = libp2p::identity::Keypair;
/// Signature using network key.
pub type NetworkSignature = Vec<u8>;

//
// EXECUTION
//
/// Public key used for signing transactions in the Execution Layer.
pub type ExecutionPublicKey = secp256k1::PublicKey;
/// Keypair used for signing transactions in the Execution Layer.
pub type ExecutionKeypair = secp256k1::Keypair;

/// Type alias selecting the default hash function for the code base.
pub type DefaultHashFunction = blake2::Blake2b<U32>;
pub const DIGEST_LENGTH: usize = 32;
pub const INTENT_MESSAGE_LENGTH: usize = INTENT_PREFIX_LENGTH + DIGEST_LENGTH;

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

/// Trait to implement Bls key signing.  This allows us to maintain private keys in a
/// secure enclave and provide a signing service.
pub trait BlsSigner: Clone + Send + Sync + Unpin + 'static {
    /// Sync version to sign something with a BLS private key.
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature;

    /// Request a signature asyncronisly.
    /// Note: used the de-sugared signature here (instead of async fn request_signature...)
    /// due to current async trait limitations and the need for + Send.
    fn request_signature(&self, msg: Vec<u8>) -> impl Future<Output = BlsSignature> + Send {
        let this = self.clone();
        let handle = tokio::task::spawn_blocking(move || this.request_signature_direct(&msg));
        async move { handle.await.expect("Failed to receive signature from Signature Service") }
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
    chain_spec: &ChainSpec,
) -> eyre::Result<BlsSignature> {
    let mut msg = keypair.public().as_bytes().to_vec();
    let genesis_bytes = encode(&chain_spec.genesis);
    msg.extend_from_slice(genesis_bytes.as_slice());
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
    chain_spec: &ChainSpec,
) -> eyre::Result<()> {
    public_key.validate().with_context(|| "Provided public key invalid")?;
    let mut msg = public_key.as_bytes().to_vec();
    let genesis_bytes = encode(&chain_spec.genesis);
    msg.extend_from_slice(genesis_bytes.as_slice());
    let result = proof.verify_secure(
        &IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg),
        public_key,
    );

    Ok(result?)
}

/// A trait for sign and verify over an intent message, instead of the message itself. See more at
/// [struct IntentMessage].
pub trait ProtocolSignature {
    /// The type used to verify the signature.
    type Pubkey: VerifyingKey;

    /// Create a new signature over an intent message.
    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer<Self>) -> Self
    where
        T: Serialize;

    /// Verify the signature over an intent message against a public key.
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        public_key: &Self::Pubkey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize;
}

impl ProtocolSignature for BlsSignature {
    type Pubkey = BlsPublicKey;

    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer<Self>) -> Self
    where
        T: Serialize,
    {
        let message = encode(&value);
        secret.sign(&message)
    }

    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        public_key: &BlsPublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = encode(&value);
        public_key.verify(&message, self)
    }
}

pub trait ValidatorAggregateSignature {
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        pks: &[BlsPublicKey],
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize;
}

impl ValidatorAggregateSignature for BlsAggregateSignature {
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        pks: &[BlsPublicKey],
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = encode(&value);
        self.verify(pks, &message)
    }
}

/// Wrap a message in an intent message. Currently in Consensus, the scope is always
/// IntentScope::ConsensusDigest and the app id is AppId::Consensus.
pub fn to_intent_message<T>(value: T) -> IntentMessage<T> {
    IntentMessage::new(Intent::consensus(IntentScope::ConsensusDigest), value)
}

/// Convert an existing NetworkPublicKey into a libp2p PeerId.
pub fn network_public_key_to_libp2p(public_key: &NetworkPublicKey) -> PeerId {
    public_key.to_peer_id()
}

#[cfg(test)]
mod tests {
    use super::{generate_proof_of_possession_bls, verify_proof_of_possession_bls};
    use crate::{adiri_chain_spec_arc, adiri_genesis, BlsKeypair};
    use fastcrypto::traits::KeyPair;
    use rand::{
        rngs::{OsRng, StdRng},
        SeedableRng,
    };

    #[test]
    fn test_proof_of_possession_success() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let proof = generate_proof_of_possession_bls(&keypair, &chain_spec).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &chain_spec).is_ok())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_signature() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let malicious_key = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let proof = generate_proof_of_possession_bls(&malicious_key, &chain_spec).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &chain_spec).is_err())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_public_key() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let malicious_key = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let proof = generate_proof_of_possession_bls(&keypair, &chain_spec).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, malicious_key.public(), &chain_spec).is_err())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_message() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let mut wrong = adiri_genesis();
        wrong.timestamp = 0;
        let proof = generate_proof_of_possession_bls(&keypair, &wrong.into()).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &chain_spec).is_err())
    }
}
