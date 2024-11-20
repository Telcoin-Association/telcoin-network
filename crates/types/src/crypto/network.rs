//! Crypto functions to help with new node handshake using network keys.

use super::{
    Intent, IntentMessage, IntentScope, NetworkKeypair, NetworkPublicKey, NetworkSignature,
    ProtocolSignature,
};
use crate::encode;
use fastcrypto::{
    error::FastCryptoError,
    traits::{KeyPair as _, Signer, ToFromBytes as _, VerifyingKey as _},
};
use reth_primitives::Genesis;
use serde::Serialize;

impl ProtocolSignature for NetworkSignature {
    type Pubkey = NetworkPublicKey;

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
        public_key: &NetworkPublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = encode(&value);
        public_key.verify(&message, self)
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
    let mut msg = keypair.public().as_bytes().to_vec();
    let genesis_bytes = encode(&genesis);
    msg.extend_from_slice(genesis_bytes.as_slice());
    NetworkSignature::new_secure(
        &IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg),
        keypair,
    )
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
    let mut msg = public_key.as_bytes().to_vec();
    let genesis_bytes = encode(genesis);
    msg.extend_from_slice(genesis_bytes.as_slice());
    proof
        .verify_secure(
            &IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg),
            public_key,
        )
        .is_ok()
}
