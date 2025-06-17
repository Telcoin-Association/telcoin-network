// SPDX-License-Identifier: Apache-2.0
//! Process consensus output and execute every transaction.

#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility,
    unused_crate_dependencies
)]

mod errors;
pub mod subscriber;
use crate::subscriber::spawn_subscriber;
pub use errors::{SubscriberError, SubscriberResult};
use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_types::{Database, Noticer, TaskManager};
use tracing::info;

/// A client subscribing to the consensus output and forwarding every transaction to be executed by
/// the engine.
pub struct Executor;

impl Executor {
    /// Spawn a new client subscriber.
    pub fn spawn<DB: Database>(
        config: ConsensusConfig<DB>,
        rx_shutdown: Noticer,
        consensus_bus: ConsensusBus,
        task_manager: &TaskManager,
        network: PrimaryNetworkHandle,
    ) {
        // Spawn the subscriber.
        spawn_subscriber(config, rx_shutdown, consensus_bus, task_manager, network);

        // Return the handle.
        info!("Consensus subscriber successfully started");
    }
}
