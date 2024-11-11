// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(clippy::mutable_key_type)]

use crate::{
    crypto::{BlsPublicKey, NetworkPublicKey},
    Multiaddr,
};
use eyre::ContextCompat;
use fastcrypto::traits::{EncodeDecodeBase64, InsecureDefault};
use serde::{
    de::{self, MapAccess, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    str::FromStr,
};
use thiserror::Error;

use self::utils::get_available_tcp_port;
pub mod utils;

/// The epoch number.
pub type Epoch = u64;

/// Opaque bytes uniquely identifying the current chain. Analogue of the
/// type in `sui-types` crate.
///
/// TODO: this should be replaced with chainspec.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ChainIdentifier([u8; 32]);

impl ChainIdentifier {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn unknown() -> Self {
        Self([0; 32])
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Node {0} is not in the worker cache")]
    NotInWorkerCache(String),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },
}

#[derive(Error, Debug)]
pub enum CommitteeUpdateError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Node {0} was not in the update")]
    MissingFromUpdate(String),

    #[error("Node {0} has a different stake than expected")]
    DifferentStake(String),
}

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
        let primary_udp_port = get_available_tcp_port(&host).unwrap_or(49590).to_string();
        // let primary_udp_port = std::env::var("PRIMARY_UDP_PORT").unwrap_or("49590".to_string());

        Self {
            network_key: NetworkPublicKey::insecure_default(),
            network_address: format!("/ip4/{}/udp/{}", &host, primary_udp_port)
                .parse()
                .expect("multiaddr parsed for primary"),
            worker_network_key: NetworkPublicKey::insecure_default(),
            worker_index: Default::default(),
        }
    }
}

// TODO: This actually represents voting power (out of 10,000) and not amount staked.
// Consider renaming to `VotingPower`.
/// The voting power an authority has within the committee.
pub type Stake = u64;

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
    /// Address to receive client transactions (WAN).
    ///
    /// AKA) RPC multiaddress.
    pub transactions: Multiaddr,
    /// Address to receive messages from other workers (WAN) and our primary.
    pub worker_address: Multiaddr,
}

impl Default for WorkerInfo {
    fn default() -> Self {
        // TODO: env vars should be applied at the CLI level, not here
        let host = std::env::var("NARWHAL_HOST").unwrap_or("127.0.0.1".to_string());
        let worker_udp_port = get_available_tcp_port(&host).unwrap_or(49594).to_string();
        // let worker_udp_port = std::env::var("WORKER_UDP_PORT").unwrap_or("49594".to_string());

        Self {
            name: NetworkPublicKey::insecure_default(),
            transactions: format!(
                "/ip4/{}/tcp/{}/http",
                &host,
                get_available_tcp_port(&host).unwrap_or_default()
            )
            .parse()
            .expect("multiaddress parsed for worker txs"),
            worker_address: format!("/ip4/{}/udp/{}", &host, worker_udp_port)
                .parse()
                .expect("multiaddr parsed for worker consensus"),
        }
    }
}

/// Map of all workers for the authority.
///
/// The map associates the worker's id to [WorkerInfo].
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerIndex(pub BTreeMap<WorkerId, WorkerInfo>);

impl Default for WorkerIndex {
    fn default() -> Self {
        Self(BTreeMap::from([(0, WorkerInfo::default())]))
    }
}

// TODO: abandon TOML for config
// Custom serilaization impl for WorkerIndex bc int types are invalid keys for TOML files.
impl Serialize for WorkerIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.iter() {
            let k = k.to_string();
            map.serialize_entry(&k, &v)?;
        }
        map.end()
    }
}

// Custom dserilaization impl for WorkerIndex bc int types are invalid keys for TOML files.
impl<'de> Deserialize<'de> for WorkerIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Define a custom visitor to handle the deserialization process.
        struct WorkerIndexVisitor;

        // Implement the Visitor trait for our custom visitor.
        impl<'de> Visitor<'de> for WorkerIndexVisitor {
            // Specify the type that will be produced by this visitor.
            type Value = WorkerIndex;

            // Provide a formatter for error messages when deserialization fails.
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of string keys to values")
            }

            // Custom implementation for visiting a map. This method will be called by Serde.
            fn visit_map<M>(self, mut map: M) -> Result<WorkerIndex, M::Error>
            where
                M: MapAccess<'de>,
            {
                // Create a new BTreeMap to store the deserialized entries.
                let mut btree_map = BTreeMap::new();

                // Iterate through each entry in the serialized map.
                while let Some((key, value)) = map.next_entry::<String, WorkerInfo>()? {
                    // Parse the key from a string to a WorkerId (u16).
                    let key = WorkerId::from_str(&key).map_err(de::Error::custom)?;

                    // Insert the key-value pair into the BTreeMap.
                    btree_map.insert(key, value);
                }

                // Return the deserialized WorkerIndex.
                Ok(WorkerIndex(btree_map))
            }
        }

        // Call deserialize_map to start the deserialization process using our custom visitor.
        deserializer.deserialize_map(WorkerIndexVisitor)
    }
}

impl WorkerIndex {
    /// Insert a new worker to the index.
    ///
    /// If the worker id already existed, the worker's info
    /// is updated.
    pub fn insert(&mut self, worker_id: WorkerId, worker_info: WorkerInfo) {
        self.0.insert(worker_id, worker_info);
    }

    /// Return information for the first worker in the index.
    ///
    /// TODO: this is a temporary solution while valiators only
    /// have one worker per primary.
    pub fn first_worker(&self) -> eyre::Result<(&WorkerId, &WorkerInfo)> {
        self.0.first_key_value().with_context(|| "No workers found in index")
    }
}

/// The collection of all workers organized by authority public keys
/// that comprise the [Committee] for a specific [Epoch].
///
/// TODO: remove default? Added for compatibility with [ConfigTrait].
/// Probably a better way to do this.
///
/// TODO: why isn't this a part of [Committee]?
/// It's always needed by members of the committee.
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct WorkerCache {
    /// The epoch number for workers
    pub epoch: Epoch,
    /// The authority to worker index.
    pub workers: BTreeMap<BlsPublicKey, WorkerIndex>,
}

impl std::fmt::Display for WorkerIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerIndex {:?}",
            self.0
                .iter()
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
                    if let Some(x) = k.encode_base64().get(0..16) {
                        format!("{}: {}", x, v)
                    } else {
                        format!("Invalid key: {}", k)
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
            .iter()
            .find_map(|v| match_opt::match_opt!(v, (name, authority) if name == to => authority))
            .ok_or_else(|| {
                ConfigError::NotInWorkerCache(ToString::to_string(&(*to).encode_base64()))
            })?
            .0
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInWorkerCache((*to).encode_base64()))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &BlsPublicKey) -> Result<Vec<WorkerInfo>, ConfigError> {
        let res = self
            .workers
            .iter()
            .find_map(
                |v| match_opt::match_opt!(v, (name, authority) if name == myself => authority),
            )
            .ok_or_else(|| ConfigError::NotInWorkerCache((*myself).encode_base64()))?
            .0
            .values()
            .cloned()
            .collect();
        Ok(res)
    }

    /// Returns the addresses of all known workers.
    pub fn all_workers(&self) -> Vec<(NetworkPublicKey, Multiaddr)> {
        self.workers
            .iter()
            .flat_map(|(_, w)| w.0.values().map(|w| (w.name.clone(), w.worker_address.clone())))
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
            .filter(|(name, _)| *name != myself )
            .flat_map(
                |(name, authority)|  authority.0.iter().flat_map(
                    |v| match_opt::match_opt!(v,(worker_id, addresses) if worker_id == id => (name.clone(), addresses.clone()))))
            .collect()
    }

    /// Returns the addresses of all workers that are not of our node.
    pub fn others_workers(&self, myself: &BlsPublicKey) -> Vec<(BlsPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself)
            .flat_map(|(name, authority)| authority.0.iter().map(|v| (name.clone(), v.1.clone())))
            .collect()
    }

    /// Return the network addresses that are present in the current worker cache
    /// that are from a primary key that are no longer in the committee. Current
    /// committee keys provided as an argument.
    pub fn network_diff(&self, keys: Vec<&BlsPublicKey>) -> HashSet<&Multiaddr> {
        self.workers
            .iter()
            .filter(|(name, _)| !keys.contains(name))
            .flat_map(|(_, authority)| {
                authority
                    .0
                    .values()
                    .map(|address| &address.transactions)
                    .chain(authority.0.values().map(|address| &address.worker_address))
            })
            .collect()
    }
}
