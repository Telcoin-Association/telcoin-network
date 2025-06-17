//! Worker peer information.

use crate::{
    error::ConfigError, get_available_udp_port, BlsPublicKey, Epoch, Multiaddr, NetworkKeypair,
    NetworkPublicKey,
};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// The unique identifier for a worker (per primary).
///
/// Workers communicate with peers of the same `WorkerId`.
pub type WorkerId = u16;

/// Information for the worker
/// - [NetworkPublicKey] for identifying network messages.
/// - [Multiaddr] for receiving transactions.
/// - [Multiaddr] for receiving messages from other workers and the primary.
#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq, Debug)]
pub struct WorkerInfo {
    /// The network public key of this worker used to sign worker messages (ie - batches).
    pub name: NetworkPublicKey,
    /// Address to receive messages from other workers (WAN) and our primary.
    pub worker_address: Multiaddr,
}

impl Default for WorkerInfo {
    fn default() -> Self {
        // These defaults should only be used by tests.
        // They need to work for tests though so localhost and a random port are good.
        let host = "127.0.0.1".to_string();
        let worker_udp_port = get_available_udp_port(&host).unwrap_or(49594);

        Self {
            name: NetworkKeypair::generate_ed25519().public().into(),
            worker_address: format!("/ip4/{}/udp/{}/quic-v1", &host, worker_udp_port)
                .parse()
                .expect("multiaddr parsed for worker consensus"),
        }
    }
}

/// Map of all workers for the authority.
///
/// The map associates the worker's id to [WorkerInfo].
/// The worker id is the index into the vec.
/// All nodes MUST use the same worker count in the same "order",
/// i.e. all worker 0s across nodes talk to each other, etc.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerIndex(pub Vec<WorkerInfo>);

impl Default for WorkerIndex {
    fn default() -> Self {
        Self(vec![WorkerInfo::default()])
    }
}

/// The collection of all workers organized by authority public keys
/// that comprise the [Committee] for a specific [Epoch].
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct WorkerCache {
    /// The epoch number for workers
    pub epoch: Epoch,
    /// The authority to worker index.
    pub workers: Arc<BTreeMap<BlsPublicKey, WorkerIndex>>,
}

impl std::fmt::Display for WorkerIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerIndex {:?}",
            self.0
                .iter()
                .enumerate()
                .map(|(key, value)| { format!("{}:{:?}", key.to_string().as_str(), value) })
                .collect::<Vec<_>>()
        )
    }
}

impl std::fmt::Display for WorkerCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerCache E{}: {:?}",
            self.epoch(),
            self.workers
                .iter()
                .map(|(k, v)| {
                    if let Some(x) = k.encode_base58().get(0..16) {
                        format!("{x}: {v}")
                    } else {
                        format!("Invalid key: {k}")
                    }
                })
                .collect::<Vec<_>>()
        )
    }
}

impl WorkerCache {
    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &BlsPublicKey, id: &WorkerId) -> Result<WorkerInfo, ConfigError> {
        self.workers
            .get(to)
            .ok_or_else(|| {
                ConfigError::NotInWorkerCache(ToString::to_string(&(*to).encode_base58()))
            })?
            .0
            .get(*id as usize)
            .cloned()
            .ok_or_else(|| ConfigError::NotInWorkerCache((*to).encode_base58()))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &BlsPublicKey) -> Result<Vec<WorkerInfo>, ConfigError> {
        let res = self
            .workers
            .get(myself)
            .ok_or_else(|| ConfigError::NotInWorkerCache((*myself).encode_base58()))?
            .0
            .clone();
        Ok(res)
    }

    /// Returns the addresses of all known workers.
    pub fn all_workers(&self) -> HashMap<PeerId, Multiaddr> {
        self.workers
            .iter()
            .flat_map(|(_, w)| w.0.iter().map(|w| (w.name.to_peer_id(), w.worker_address.clone())))
            .collect()
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers_by_id(
        &self,
        myself: &BlsPublicKey,
        id: &WorkerId,
    ) -> Vec<(BlsPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself)
            .flat_map(|(name, authority)| authority.0.get(*id as usize).map(|i| (*name, i.clone())))
            .collect()
    }

    /// Returns the addresses of all workers that are not of our node.
    pub fn others_workers(&self, myself: &BlsPublicKey) -> Vec<(BlsPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself)
            .flat_map(|(name, authority)| authority.0.iter().map(move |v| (*name, v.clone())))
            .collect()
    }
}
