// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Primary fixture for the cluster
use consensus_metrics::RegistryService;
use fastcrypto::traits::KeyPair as _;
use narwhal_executor::SerializedTransaction;
use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use prometheus::{proto::Metric, Registry};
use std::{cell::RefCell, path::PathBuf, rc::Rc, sync::Arc};
use tn_config::Parameters;
use tn_node::primary::PrimaryNode;
use tn_types::{
    test_utils::temp_dir, AuthorityIdentifier, BlsKeypair, ChainIdentifier, Committee,
    ConsensusOutput, NetworkKeypair, WorkerCache,
};
use tokio::{
    sync::{
        broadcast::{self, Sender},
        mpsc::channel,
    },
    task::JoinHandle,
};
use tracing::info;

use crate::TestExecutionNode;

#[derive(Clone)]
pub struct PrimaryNodeDetails {
    pub id: usize,
    pub name: AuthorityIdentifier,
    pub key_pair: Arc<BlsKeypair>,
    pub network_key_pair: Arc<NetworkKeypair>,
    pub tx_transaction_confirmation: Sender<SerializedTransaction>,
    node: PrimaryNode,
    store_path: PathBuf,
    _parameters: Parameters,
    committee: Committee,
    worker_cache: WorkerCache,
    handlers: Rc<RefCell<Vec<JoinHandle<()>>>>,
}

impl PrimaryNodeDetails {
    pub(crate) fn new(
        id: usize,
        name: AuthorityIdentifier,
        key_pair: BlsKeypair,
        network_key_pair: NetworkKeypair,
        parameters: Parameters,
        committee: Committee,
        worker_cache: WorkerCache,
    ) -> Self {
        // used just to initialise the struct value
        let (tx, _) = tokio::sync::broadcast::channel(1);

        let registry_service = RegistryService::new(Registry::new());

        let node = PrimaryNode::new(parameters.clone(), registry_service);

        Self {
            id,
            name,
            key_pair: Arc::new(key_pair),
            network_key_pair: Arc::new(network_key_pair),
            tx_transaction_confirmation: tx,
            node,
            store_path: temp_dir(),
            _parameters: parameters,
            committee,
            worker_cache,
            handlers: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Returns the metric - if exists - identified by the provided name.
    /// If metric has not been found then None is returned instead.
    pub async fn metric(&self, name: &str) -> Option<Metric> {
        let (_registry_id, registry) = self.node.registry().await.unwrap();
        let metrics = registry.gather();

        let metric = metrics.into_iter().find(|m| m.get_name() == name);
        metric.map(|m| m.get_metric().first().unwrap().clone())
    }

    /// TODO: this needs to be cleaned up
    pub(crate) async fn start(
        &mut self,
        client: NetworkClient,
        preserve_store: bool,
        execution_components: &TestExecutionNode,
    ) -> eyre::Result<()> {
        if self.is_running().await {
            panic!("Tried to start a node that is already running");
        }

        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        info!("Primary Node {} will use path {:?}", self.id, store_path.clone());

        // The channel returning the result for each transaction's execution.
        let (_tx_transaction_confirmation, mut rx_transaction_confirmation) = channel(100);

        // Primary node
        let primary_store: NodeStorage = NodeStorage::reopen(store_path.clone(), None);

        self.node
            .start(
                self.key_pair.copy(),
                self.network_key_pair.copy(),
                self.committee.clone(),
                ChainIdentifier::unknown(),
                self.worker_cache.clone(),
                client,
                &primary_store,
                execution_components,
            )
            .await?;

        let (tx, _) = tokio::sync::broadcast::channel(narwhal_primary::CHANNEL_CAPACITY);
        let transactions_sender = tx.clone();
        // spawn a task to listen on the committed transactions
        // and translate to a mpmc channel
        let h = tokio::spawn(async move {
            while let Some(t) = rx_transaction_confirmation.recv().await {
                // send the transaction to the mpmc channel
                let _ = transactions_sender.send(t);
            }
        });

        // add the tasks's handle to the primary's handle so can be shutdown
        // with the others.
        self.handlers.replace(vec![h]);
        self.store_path = store_path;
        self.tx_transaction_confirmation = tx;

        // return receiver for execution engine
        Ok(())
    }

    pub(crate) async fn stop(&self) {
        self.node.shutdown().await;
        self.handlers.borrow().iter().for_each(|h| h.abort());
        info!("Aborted primary node for id {}", self.id);
    }

    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    pub async fn is_running(&self) -> bool {
        self.node.is_running().await
    }

    /// Subscribe to [ConsensusOutput] broadcast.
    ///
    /// NOTE: this broadcasts to all receivers, so it's important to ensure
    /// no subscriber causes the node's execution receiver to lag
    pub async fn subscribe_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        self.node.subscribe_consensus_output().await
    }
}
