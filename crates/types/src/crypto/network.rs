//! Crypto functions to help with new node handshake using network keys.

use super::{
    Intent, IntentMessage, IntentScope, NetworkKeypair, NetworkPublicKey, NetworkSignature,
};
use crate::{encode, Genesis};

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
