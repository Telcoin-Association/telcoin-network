// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Worker fixture for the cluster

use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use narwhal_typed_store::{open_db, RawDatabaseType};
use std::path::PathBuf;
use tn_node::worker::WorkerNode;
use tn_types::{
    test_utils::temp_dir, AuthorityIdentifier, BlsPublicKey, Committee, Multiaddr, NetworkKeypair,
    Parameters, WorkerCache, WorkerId,
};
use tracing::info;

use crate::TestExecutionNode;

#[derive(Clone)]
pub struct WorkerNodeDetails {
    pub id: WorkerId,
    pub transactions_address: Multiaddr,
    name: AuthorityIdentifier,
    primary_key: BlsPublicKey,
    node: WorkerNode,
    committee: Committee,
    worker_cache: WorkerCache,
    store_path: PathBuf,
}

impl WorkerNodeDetails {
    pub(crate) fn new(
        id: WorkerId,
        name: AuthorityIdentifier,
        primary_key: BlsPublicKey,
        parameters: Parameters,
        transactions_address: Multiaddr,
        committee: Committee,
        worker_cache: WorkerCache,
    ) -> Self {
        let node = WorkerNode::new(id, parameters);

        Self {
            id,
            name,
            primary_key,
            store_path: temp_dir(),
            transactions_address,
            committee,
            worker_cache,
            node,
        }
    }

    /// Starts the node. When preserve_store is true then the last used
    pub(crate) async fn start(
        &mut self,
        keypair: NetworkKeypair,
        client: NetworkClient,
        preserve_store: bool,
        execution_node: &TestExecutionNode,
    ) -> eyre::Result<()>
where {
        if self.is_running().await {
            panic!("Worker with id {} is already running, can't start again", self.id);
        }

        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        // In case the DB dir does not yet exist.
        let _ = std::fs::create_dir_all(&store_path);
        let db = open_db(&store_path);
        let worker_store = NodeStorage::reopen(db, Option::<RawDatabaseType>::None);

        info!(target: "cluster::worker", "starting worker-{} for authority {}", self.id, self.name);

        self.node
            .start(
                self.primary_key.clone(),
                keypair,
                self.committee.clone(),
                self.worker_cache.clone(),
                client,
                &worker_store,
                execution_node,
            )
            .await?;

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
