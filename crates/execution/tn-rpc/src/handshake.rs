//! RPC types for handshake.

use reth_primitives::ChainId;
use serde::{Deserialize, Serialize};
use tn_types::{Multiaddr, NetworkPublicKey, NetworkSignature};

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
