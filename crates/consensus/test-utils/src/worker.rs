// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Worker fixture for the cluster

use fastcrypto::traits::KeyPair as _;
use narwhal_typed_store::traits::Database;
use std::path::PathBuf;
use tn_config::{ConsensusConfig, KeyConfig};
use tn_node::worker::WorkerNode;
use tn_types::{
    test_utils::temp_dir, AuthorityIdentifier, Multiaddr, NetworkKeypair, WorkerId, WorkerInfo,
};
use tracing::info;

use crate::TestExecutionNode;

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
        if self.is_running().await {
            panic!("Worker with id {} is already running, can't start again", self.id);
        }

        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        info!(target: "cluster::worker", "starting worker-{} for authority {}", self.id, self.name);

        self.node.start(execution_node).await?;

        self.store_path = store_path;

        Ok(())
    }

    pub(crate) async fn stop(&self) {
        self.node.shutdown().await;
        info!("Aborted worker node for id {}", self.id);
    }

    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    pub async fn is_running(&self) -> bool {
        self.node.is_running().await
    }
}

/// Fixture representing a worker for an [AuthorityFixture].
///
/// [WorkerFixture] holds keypairs and should not be used in production.
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
        let worker_name = key_config.worker_network_keypair().public().clone();
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
