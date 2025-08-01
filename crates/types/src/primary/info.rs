//! Primary information for peers.
use crate::{get_available_udp_port, NetworkKeypair, P2pNode};
use serde::{Deserialize, Serialize};

/// Information for the Primary.
///
/// Currently, Primary details are fanned out in authority details.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct NodeP2pInfo {
    /// The primary's public network settings.
    pub primary: P2pNode,
    /// The workers's public network settings.
    pub worker: P2pNode,
}

impl NodeP2pInfo {
    /// Create a new instance of [PrimaryInfo].
    pub fn new(primary: P2pNode, worker: P2pNode) -> Self {
        Self { primary, worker }
    }
}

impl Default for NodeP2pInfo {
    fn default() -> Self {
        // These defaults should only be used by tests.
        // They need to work for tests though so localhost and a random port are good.
        let host = "127.0.0.1".to_string();
        let primary_udp_port = get_available_udp_port(&host).unwrap_or(49584);
        let worker_udp_port = get_available_udp_port(&host).unwrap_or(49584);

        Self {
            primary: P2pNode {
                network_address: format!("/ip4/{}/udp/{}/quic-v1", &host, primary_udp_port)
                    .parse()
                    .expect("multiaddr parsed for primary"),
                network_key: NetworkKeypair::generate_ed25519().public().into(),
            },
            worker: P2pNode {
                network_address: format!("/ip4/{}/udp/{}/quic-v1", &host, worker_udp_port)
                    .parse()
                    .expect("multiaddr parsed for primary"),
                network_key: NetworkKeypair::generate_ed25519().public().into(),
            },
        }
    }
}
