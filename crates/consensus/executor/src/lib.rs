// SPDX-License-Identifier: Apache-2.0
//! Process consensus output and execute every transaction.

mod errors;
pub mod subscriber;
use crate::subscriber::spawn_subscriber;
pub use errors::{SubscriberError, SubscriberResult};
use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_storage::consensus::ConsensusChain;
use tn_types::{Database, Noticer, TaskManager};
use tracing::info;

#[cfg(test)]
use tempfile as _;

/// A client subscribing to the consensus output and forwarding every transaction to be executed by
/// the engine.
#[derive(Debug)]
pub struct Executor;

impl Executor {
    /// Spawn a new client subscriber.
    pub fn spawn<DB: Database>(
        config: ConsensusConfig<DB>,
        rx_shutdown: Noticer,
        consensus_bus: ConsensusBus,
        task_manager: &TaskManager,
        network: PrimaryNetworkHandle,
        consensus_chain: ConsensusChain,
    ) {
        // Spawn the subscriber.
        spawn_subscriber(
            config,
            rx_shutdown,
            consensus_bus,
            task_manager,
            network,
            consensus_chain,
        );

        info!("Consensus subscriber successfully started");
    }
}

#[cfg(test)]
mod clippy {
    use eyre as _;
    use tn_network_libp2p as _;
    use tn_reth as _;
    use tn_test_utils as _;
}
