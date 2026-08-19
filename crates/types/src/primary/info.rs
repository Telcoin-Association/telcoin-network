//! Primary information for peers.
use crate::{
    committee::PrimaryWorkersRepr, get_available_udp_port, NetworkKeypair, P2pNode, WorkerId,
};
use serde::{Deserialize, Serialize};

/// Information for the Primary.
///
/// Currently, Primary details are fanned out in authority details.
///
/// The `workers` list holds one [P2pNode] per worker, indexed by [WorkerId]. Every validator in a
/// committee must run the same number of workers (a protocol-level value, see
/// `Committee::number_of_workers`). The list is never empty.
///
/// Serialization: the current on-disk shape is `workers: [..]`. The legacy single-worker shape
/// `worker: {..}` is still accepted on read so existing node-info and validator files load
/// unchanged; it is written back in the new shape.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(from = "PrimaryWorkersRepr")]
pub struct NodeP2pInfo {
    /// The primary's public network settings.
    pub primary: P2pNode,
    /// The public network settings for each worker, indexed by [WorkerId].
    pub workers: Vec<P2pNode>,
}

impl From<PrimaryWorkersRepr> for NodeP2pInfo {
    fn from(value: PrimaryWorkersRepr) -> Self {
        let (primary, workers) = value.into();
        Self { primary, workers }
    }
}

impl NodeP2pInfo {
    /// Create a new instance of [NodeP2pInfo] with one [P2pNode] per worker.
    pub fn new(primary: P2pNode, workers: Vec<P2pNode>) -> Self {
        Self { primary, workers }
    }

    /// Return the [P2pNode] for `worker_id`, or `None` if this node runs no such worker.
    pub fn worker(&self, worker_id: WorkerId) -> Option<&P2pNode> {
        self.workers.get(usize::from(worker_id))
    }

    /// Return a mutable [P2pNode] for `worker_id`, or `None` if this node runs no such worker.
    pub fn worker_mut(&mut self, worker_id: WorkerId) -> Option<&mut P2pNode> {
        self.workers.get_mut(usize::from(worker_id))
    }

    /// Return the number of workers this node runs.
    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }
}

impl Default for NodeP2pInfo {
    fn default() -> Self {
        // These defaults should only be used by tests.
        // They need to work for tests though so localhost and a random port are good.
        let host = "127.0.0.1".to_string();
        let primary_udp_port = get_available_udp_port(&host).unwrap_or(49584);
        let worker_udp_port = get_available_udp_port(&host).unwrap_or(49594);

        Self {
            primary: P2pNode {
                network_address: format!("/ip4/{}/udp/{}/quic-v1", &host, primary_udp_port)
                    .parse()
                    .expect("multiaddr parsed for primary"),
                network_key: NetworkKeypair::generate_ed25519().public().into(),
                rpc: None,
            },
            workers: vec![P2pNode {
                network_address: format!("/ip4/{}/udp/{}/quic-v1", &host, worker_udp_port)
                    .parse()
                    .expect("multiaddr parsed for worker"),
                network_key: NetworkKeypair::generate_ed25519().public().into(),
                rpc: None,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_WORKER_ID;

    /// A [P2pNode] with a fresh random key for serde tests.
    fn p2p_node() -> P2pNode {
        let address: crate::Multiaddr =
            "/ip4/127.0.0.1/udp/4000/quic-v1".parse().expect("multiaddr");
        let key: crate::NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        (address, key).into()
    }

    /// Indent every line of a YAML fragment by two spaces so it nests under a key (the document
    /// start marker is dropped).
    fn indent(fragment: &str) -> String {
        fragment
            .lines()
            .filter(|l| *l != "---")
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn new_shape_round_trips() {
        let info = NodeP2pInfo::new(p2p_node(), vec![p2p_node(), p2p_node()]);
        let yaml = serde_yaml::to_string(&info).expect("serialize");
        assert!(yaml.contains("workers:"), "new shape must serialize `workers`: {yaml}");
        assert!(!yaml.contains("\nworker:"), "new shape must not serialize `worker`: {yaml}");
        let decoded: NodeP2pInfo = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(decoded, info);
        assert_eq!(decoded.num_workers(), 2);
        assert_eq!(decoded.worker(1), info.workers.get(1));
        assert!(decoded.worker(2).is_none());
    }

    #[test]
    fn legacy_shape_decodes_as_one_worker() {
        let primary = p2p_node();
        let worker = p2p_node();
        let expected = NodeP2pInfo::new(primary.clone(), vec![worker.clone()]);
        let primary_yaml = serde_yaml::to_string(&primary).expect("serialize primary");
        let worker_yaml = serde_yaml::to_string(&worker).expect("serialize worker");
        let legacy =
            format!("primary:\n{}\nworker:\n{}\n", indent(&primary_yaml), indent(&worker_yaml));
        let decoded: NodeP2pInfo = serde_yaml::from_str(&legacy).expect("legacy deserialize");
        assert_eq!(decoded, expected);
        assert_eq!(decoded.worker(DEFAULT_WORKER_ID), Some(&worker));
    }

    #[test]
    fn empty_workers_rejected() {
        let primary_yaml = serde_yaml::to_string(&p2p_node()).expect("serialize primary");
        let doc = format!("primary:\n{}\nworkers: []\n", indent(&primary_yaml));
        let result: Result<NodeP2pInfo, _> = serde_yaml::from_str(&doc);
        assert!(result.is_err(), "empty workers must be rejected");
    }
}
