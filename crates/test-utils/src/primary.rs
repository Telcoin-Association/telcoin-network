//! Primary fixture for the cluster

use anemo::Network;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_node::primary::PrimaryNode;
use tn_primary::{consensus::ConsensusMetrics, ConsensusBus};
use tn_storage::traits::Database;
use tn_types::AuthorityIdentifier;

#[derive(Clone)]
pub struct PrimaryNodeDetails<DB> {
    pub id: usize,
    pub name: AuthorityIdentifier,
    node: PrimaryNode<DB>,
}

impl<DB: Database> PrimaryNodeDetails<DB> {
    pub(crate) fn new(
        id: usize,
        name: AuthorityIdentifier,
        consensus_config: ConsensusConfig<DB>,
    ) -> Self {
        let consensus_bus =
            ConsensusBus::new_with_recent_blocks(consensus_config.config().parameters.gc_depth);
        let node = PrimaryNode::new(consensus_config, consensus_bus);

        Self { id, name, node }
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
    pub(crate) async fn start(&mut self) -> eyre::Result<()> {
        self.node.start().await?;

        // return receiver for execution engine
        Ok(())
    }

    pub fn node(&self) -> &PrimaryNode<DB> {
        &self.node
    }

    /// Return an owned wide-area [Network] if it is running.
    pub async fn network(&self) -> Network {
        self.node.network().await
    }
}
