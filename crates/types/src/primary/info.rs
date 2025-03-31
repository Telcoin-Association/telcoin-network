//! Primary information for peers.
use crate::{get_available_udp_port, Multiaddr, NetworkKeypair, NetworkPublicKey, WorkerIndex};
use serde::{Deserialize, Serialize};

/// Information for the Primary.
///
/// TODO: update AuthorityFixture, etc. to use this instead.
/// Currently, Primary details are fanned out in authority details.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct PrimaryInfo {
    /// The primary's public network key. Used to sign messages (ie - headers, certs, votes)
    pub network_key: NetworkPublicKey,
    /// The WAN address for the primary.
    /// This is where peers should send consensus messages for this primary to process.
    pub network_address: Multiaddr,
    /// The worker public network key.
    ///
    /// TODO: should each worker have their own key?
    /// Adding this for CLI - expects only 1 worker for now.
    pub worker_network_key: NetworkPublicKey,
    /// The workers for this primary.
    pub worker_index: WorkerIndex,
}

impl PrimaryInfo {
    /// Create a new instance of [PrimaryInfo].
    pub fn new(
        network_key: NetworkPublicKey,
        network_address: Multiaddr,
        worker_network_key: NetworkPublicKey,
        worker_index: WorkerIndex,
    ) -> Self {
        Self { network_key, network_address, worker_network_key, worker_index }
    }

    /// Return a reference to the primary's [WorkerIndex].
    pub fn worker_index(&self) -> &WorkerIndex {
        &self.worker_index
    }
}

impl Default for PrimaryInfo {
    fn default() -> Self {
        let host = std::env::var("NARWHAL_HOST").unwrap_or("127.0.0.1".to_string());
        let primary_udp_port = get_available_udp_port(&host).unwrap_or(49590).to_string();

        Self {
            network_key: NetworkKeypair::generate_ed25519().public().into(),
            network_address: format!("/ip4/{}/udp/{}/quic-v1", &host, primary_udp_port)
                .parse()
                .expect("multiaddr parsed for primary"),
            worker_network_key: NetworkKeypair::generate_ed25519().public().into(),
            worker_index: Default::default(),
        }
    }
}
