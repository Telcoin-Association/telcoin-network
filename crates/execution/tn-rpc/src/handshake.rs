//! RPC types for handshake.

use reth_primitives::{ChainId, Genesis};
use serde::{Deserialize, Serialize};
use tn_types::{
    adiri_chain_spec, generate_proof_of_possession_network, Multiaddr, NetworkKeypair,
    NetworkPublicKey, NetworkSignature,
};

/// The struct containing the necessary information for peer handshake.
///
/// TODO: consider including client version.
#[derive(Serialize, Deserialize)]
pub struct Handshake {
    /// The chain id for the client.
    ///
    /// This must match the node's chain id.
    pub chain_id: ChainId,
    /// The node's network key for signing.
    pub network_key: NetworkPublicKey,
    /// The signature to prove possession of the network key.
    pub proof: NetworkSignature,
    /// The multiaddress for this peer.
    pub address: Multiaddr,
}

/// Type to facilitate building the handshake.
pub struct HandshakeBuilder {
    /// This node's chain id for the client.
    pub chain_id: ChainId,
    /// This node's network key for signing.
    pub network_key: Option<NetworkPublicKey>,
    /// This node's network multiaddress for peers to use.
    pub address: Option<Multiaddr>,
    /// The signature to prove possession of the network key.
    pub proof: Option<NetworkSignature>,
}

impl Default for HandshakeBuilder {
    fn default() -> Self {
        Self {
            chain_id: adiri_chain_spec().chain().id(),
            network_key: None,
            address: None,
            proof: None,
        }
    }
}

impl HandshakeBuilder {
    /// Create a new instance of Self, uninitialized.
    pub fn new(chain_id: ChainId) -> Self {
        Self { chain_id, network_key: None, address: None, proof: None }
    }

    /// Set the handshake's network key.
    pub fn with_network_key(mut self, network_key: Option<NetworkPublicKey>) -> Self {
        self.network_key = network_key;
        self
    }
    /// Set the handshake's address.
    pub fn with_address(mut self, address: Option<Multiaddr>) -> Self {
        self.address = address;
        self
    }

    /// Set the handshake's proof.
    ///
    /// Devs should prefer leaving this blank for [Self]. `Self::build` constructs this value if it's blank constructs this value if it's none.
    ///
    /// Should only be used in testing.
    pub fn with_proof(mut self, proof: Option<NetworkSignature>) -> Self {
        self.proof = proof;
        self
    }

    /// Generate the proof of possession for the network key.
    ///
    /// TODO: this should be an independent function?
    pub fn generate_proof(keypair: &NetworkKeypair, genesis: &Genesis) -> NetworkSignature {
        generate_proof_of_possession_network(keypair, genesis)
    }

    /// Build the [Handshake] using the values.
    ///
    /// If `Self::proof` is `None`, the builder will construct the proof. Devs should prefer this approach over constructing the proof themselves.
    pub fn build(self, keypair: &NetworkKeypair, genesis: &Genesis) -> Handshake {
        // deconstruct builder
        let Self { chain_id, network_key, address, .. } = self;
        // generate proof of possession using network keys
        let proof = Self::generate_proof(keypair, genesis);

        Handshake {
            chain_id,
            network_key: network_key.expect("network key required"),
            proof,
            address: address.expect("peer multiadress required"),
        }
    }
}

#[cfg(test)]
mod tests {
    use tn_types::adiri_genesis;

    use super::*;

    #[test]
    fn test_handshake_proof() {
        // TODO: test good/bad proofs
        let multiaddr = todo!();
        let network_keypair = todo!();
        let bad_proof = todo!();
        let genesis = adiri_genesis();

        let handshake = HandshakeBuilder::default()
            .with_address(address)
            .with_network_key(network_key)
            .build(keypair, &genesis);
    }
}
