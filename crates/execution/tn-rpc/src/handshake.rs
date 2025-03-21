//! RPC types for handshake.

use serde::{Deserialize, Serialize};
use tn_types::{
    generate_proof_of_possession_network, verify_proof_of_possession_network, Genesis, Multiaddr,
    NetworkKeypair, NetworkPublicKey, NetworkSignature,
};

/// The struct containing the necessary information for peer handshake.
///
/// TODO: consider including client version.
#[derive(Serialize, Deserialize)]
pub struct Handshake {
    /// The node's network key for signing.
    network_key: NetworkPublicKey,
    /// The signature to prove possession of the network key.
    proof: NetworkSignature,
    /// The multiaddress for this peer.
    address: Multiaddr,
}

impl Handshake {
    /// Create a new instance of Self.
    pub fn new(network_key: NetworkPublicKey, proof: NetworkSignature, address: Multiaddr) -> Self {
        Self { network_key, proof, address }
    }

    /// Verify the peer's proof of possession.
    ///
    /// The expected signature should contain the peer's network public key and the genesis for the
    /// same chain as this node.
    ///
    /// The method returns bool indicating if the signature is valid (true) or invalid (false).
    pub fn verify_proof(&self, genesis: &Genesis) -> bool {
        verify_proof_of_possession_network(&self.proof, &self.network_key, genesis)
    }
}

/// Type to facilitate building the handshake.
pub struct HandshakeBuilder {
    /// This node's network keys for generating proof of possession.
    network_keypair: NetworkKeypair,
    /// This node's network multiaddress for peers to use.
    address: Multiaddr,
    /// [Genesis] of the network this node is trying to join.
    genesis: Genesis,
}

impl HandshakeBuilder {
    /// Create a new instance of Self, uninitialized.
    pub fn new(network_keypair: NetworkKeypair, address: Multiaddr, genesis: Genesis) -> Self {
        Self { network_keypair, address, genesis }
    }

    /// Build the [Handshake] using the builder's values.
    ///
    /// This method calls another function to generate the proof of possession for the provided
    /// network keys.
    pub fn build(self) -> Handshake {
        // deconstruct builder
        let Self { network_keypair, address, genesis } = self;

        // generate proof of possession using network keys
        let proof = generate_proof_of_possession_network(&network_keypair, &genesis);

        Handshake { network_key: network_keypair.public().clone().into(), proof, address }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr as _;
    use tn_types::adiri_genesis;

    #[test]
    fn test_handshake_proof() {
        let multiaddr: Multiaddr =
            Multiaddr::from_str("/ip4/10.10.10.33/udp/49590").expect("valid multiaddr");
        let network_keypair = NetworkKeypair::generate_ed25519();
        let genesis = adiri_genesis();

        let mut handshake =
            HandshakeBuilder::new(network_keypair, multiaddr, genesis.clone()).build();
        assert!(handshake.verify_proof(&genesis));

        // use wrong key
        let malicious_key = NetworkKeypair::generate_ed25519();
        let malicious_sig = generate_proof_of_possession_network(&malicious_key, &genesis);
        handshake.proof = malicious_sig;

        assert!(!handshake.verify_proof(&genesis))
    }
}
