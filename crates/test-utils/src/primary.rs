// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Primary fixture for the cluster
use anemo::Network;
use std::{cell::RefCell, rc::Rc, sync::Arc};
use tn_config::ConsensusConfig;
use tn_node::primary::PrimaryNode;
use tn_primary::consensus::ConsensusMetrics;
use tn_storage::traits::Database;
use tn_types::AuthorityIdentifier;
use tokio::task::JoinHandle;
use tracing::info;

use crate::TestExecutionNode;

#[derive(Clone)]
pub struct PrimaryNodeDetails<DB> {
    pub id: usize,
    pub name: AuthorityIdentifier,
    node: PrimaryNode<DB>,
    handlers: Rc<RefCell<Vec<JoinHandle<()>>>>,
}

impl<DB: Database> PrimaryNodeDetails<DB> {
    pub(crate) fn new(
        id: usize,
        name: AuthorityIdentifier,
        consensus_config: ConsensusConfig<DB>,
    ) -> Self {
        let node = PrimaryNode::new(consensus_config);

        Self { id, name, node, handlers: Rc::new(RefCell::new(Vec::new())) }
    }

    /// Retrieve the consensus metrics in use for this primary node.
    pub async fn consensus_metrics(&self) -> Arc<ConsensusMetrics> {
        self.node.consensus_metrics().await
    }

    /// Retrieve the consensus metrics in use for this primary node.
    pub async fn primary_metrics(&self) -> Arc<tn_primary_metrics::Metrics> {
        self.node.primary_metrics().await
    }

    /// TODO: this needs to be cleaned up
    pub(crate) async fn start(
        &mut self,
        execution_components: &TestExecutionNode,
    ) -> eyre::Result<()> {
        if self.is_running().await {
            panic!("Tried to start a node that is already running");
        }

        self.node.start(execution_components).await?;

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

    pub fn node(&self) -> &PrimaryNode<DB> {
        &self.node
    }

    /// Return an owned wide-area [Network] if it is running.
    pub async fn network(&self) -> Option<Network> {
        self.node.network().await
    }
}
