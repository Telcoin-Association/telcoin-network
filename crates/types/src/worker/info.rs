//! Worker peer information.

use crate::{error::ConfigError, BlsPublicKey, Epoch};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// The unique identifier for a worker (per primary).
///
/// Workers communicate with peers of the same `WorkerId`.
pub type WorkerId = u16;

/// Map of all workers for the authority.
///
/// The map associates the worker's id to [WorkerInfo].
/// The worker id is the index into the vec.
/// All nodes MUST use the same worker count in the same "order",
/// i.e. all worker 0s across nodes talk to each other, etc.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerIndex(pub Vec<BlsPublicKey>);

impl Default for WorkerIndex {
    fn default() -> Self {
        Self(vec![BlsPublicKey::default()])
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
    pub fn worker(&self, to: &BlsPublicKey, id: &WorkerId) -> Result<BlsPublicKey, ConfigError> {
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
    pub fn our_workers(&self, myself: &BlsPublicKey) -> Result<Vec<BlsPublicKey>, ConfigError> {
        let res = self
            .workers
            .get(myself)
            .ok_or_else(|| ConfigError::NotInWorkerCache((*myself).encode_base58()))?
            .0
            .clone();
        Ok(res)
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers_by_id(&self, myself: &BlsPublicKey, id: &WorkerId) -> Vec<BlsPublicKey> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself)
            .flat_map(|(_, index)| index.0.get(*id as usize))
            .copied()
            .collect()
    }
}
