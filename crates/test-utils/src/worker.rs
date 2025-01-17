//! Worker fixture for the cluster

use crate::{temp_dir, TestExecutionNode};
use anemo::Network;
use fastcrypto::traits::KeyPair as _;
use std::path::PathBuf;
use tn_config::{ConsensusConfig, KeyConfig};
use tn_node::worker::WorkerNode;
use tn_storage::traits::Database;
use tn_types::{AuthorityIdentifier, Multiaddr, NetworkKeypair, WorkerId, WorkerInfo};
use tracing::info;

#[derive(Clone)]
pub struct WorkerNodeDetails<DB> {
    pub id: WorkerId,
    pub transactions_address: Multiaddr,
    name: AuthorityIdentifier,
    // Need to assign a type to WorkerNode generic since we create it in this struct.
    node: WorkerNode<DB>,
    store_path: PathBuf,
}

impl<DB: Database> WorkerNodeDetails<DB> {
    pub(crate) fn new(
        id: WorkerId,
        name: AuthorityIdentifier,
        consensus_config: ConsensusConfig<DB>,
        transactions_address: Multiaddr,
    ) -> Self {
        let node = WorkerNode::new(id, consensus_config);

        Self { id, name, store_path: temp_dir(), transactions_address, node }
    }

    /// Starts the node. When preserve_store is true then the last used
    pub(crate) async fn start(
        &mut self,
        preserve_store: bool,
        execution_node: &TestExecutionNode,
    ) -> eyre::Result<()> {
        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        info!(target: "cluster::worker", "starting worker-{} for authority {}", self.id, self.name);

        self.node.start(execution_node.new_block_validator().await).await?;

        self.store_path = store_path;

        Ok(())
    }

    /// Return an owned wide-area [Network] if it is running.
    pub async fn network(&self) -> Option<Network> {
        self.node.network().await
    }
}

/// Fixture representing a worker for an [AuthorityFixture].
///
/// [WorkerFixture] holds keypairs and should not be used in production.
#[derive(Debug)]
pub struct WorkerFixture {
    key_config: KeyConfig,
    pub id: WorkerId,
    pub info: WorkerInfo,
}

impl WorkerFixture {
    pub fn keypair(&self) -> NetworkKeypair {
        self.key_config.worker_network_keypair().copy()
    }

    pub fn info(&self) -> &WorkerInfo {
        &self.info
    }

    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.info().worker_address.to_anemo_address().unwrap())
            .server_name("narwhal")
            .private_key(self.keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    pub(crate) fn generate<P>(key_config: KeyConfig, id: WorkerId, mut get_port: P) -> Self
    where
        P: FnMut(&str) -> u16,
    {
        let worker_name = key_config.worker_network_public_key();
        let host = "127.0.0.1";
        let worker_address = format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();
        let transactions = format!("/ip4/{}/tcp/{}/http", host, get_port(host)).parse().unwrap();

        Self {
            key_config,
            id,
            info: WorkerInfo { name: worker_name, worker_address, transactions },
        }
    }
}
