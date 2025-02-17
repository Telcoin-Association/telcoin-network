//! Primary fixture for the cluster

use anemo::Network;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::ConsensusNetwork;
use tn_node::primary::PrimaryNode;
use tn_primary::{consensus::ConsensusMetrics, ConsensusBus};
use tn_types::{AuthorityIdentifier, Database};
use tokio::sync::mpsc;

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
        let (event_stream, rx_event_stream) = mpsc::channel(1000);
        let consensus_bus =
            ConsensusBus::new_with_args(consensus_config.config().parameters.gc_depth);
        let consensus_network = ConsensusNetwork::new_for_primary(&consensus_config, event_stream)
            .expect("p2p network create failed!");
        let consensus_network_handle = consensus_network.network_handle();
        let node = PrimaryNode::new(
            consensus_config,
            consensus_bus,
            consensus_network_handle,
            rx_event_stream,
        );

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
