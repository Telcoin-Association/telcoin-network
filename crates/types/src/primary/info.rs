//! Primary information for peers.
use crate::{get_available_udp_port, Multiaddr, NetworkKeypair, NetworkPublicKey, WorkerIndex};
use serde::{Deserialize, Serialize};

/// Information for the Primary.
///
/// Currently, Primary details are fanned out in authority details.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct NodeP2pInfo {
    /// The primary's public network key. Used to sign messages (ie - headers, certs, votes)
    pub network_key: NetworkPublicKey,
    /// The WAN address for the primary.
    /// This is where peers should send consensus messages for this primary to process.
    pub network_address: Multiaddr,
    /// The workers for this primary.
    pub worker_index: WorkerIndex,
}

impl NodeP2pInfo {
    /// Create a new instance of [PrimaryInfo].
    pub fn new(
        network_key: NetworkPublicKey,
        network_address: Multiaddr,
        worker_index: WorkerIndex,
    ) -> Self {
        Self { network_key, network_address, worker_index }
    }

    /// Return a reference to the primary's [WorkerIndex].
    pub fn worker_index(&self) -> &WorkerIndex {
        &self.worker_index
    }
}

impl Default for NodeP2pInfo {
    fn default() -> Self {
        // These defaults should only be used by tests.
        // They need to work for tests though so localhost and a random port are good.
        let host = "127.0.0.1".to_string();
        let primary_udp_port = get_available_udp_port(&host).unwrap_or(49584);

        Self {
            network_key: NetworkKeypair::generate_ed25519().public().into(),
            network_address: format!("/ip4/{}/udp/{}/quic-v1", &host, primary_udp_port)
                .parse()
                .expect("multiaddr parsed for primary"),
            worker_index: Default::default(),
        }
    }
}
