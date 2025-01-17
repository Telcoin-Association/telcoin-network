// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod errors;
mod state;
pub mod subscriber;

pub use errors::{SubscriberError, SubscriberResult};
pub use state::ExecutionIndices;
use tn_primary::ConsensusBus;
use tn_storage::traits::Database;

use crate::subscriber::spawn_subscriber;

use async_trait::async_trait;
use mockall::automock;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_storage::ConsensusStore;
use tn_types::{CommittedSubDag, ConsensusOutput, Noticer, TaskManager};
use tracing::info;

/// Convenience type representing a serialized transaction.
pub type SerializedTransaction = Vec<u8>;

/// Convenience type representing a serialized transaction digest.
pub type SerializedTransactionDigest = u64;

#[automock]
#[async_trait]
pub trait ExecutionState {
    /// Execute the transaction and atomically persist the consensus index.
    async fn handle_consensus_output(&mut self, consensus_output: ConsensusOutput);

    /// Load the last executed sub-dag index from storage
    async fn last_executed_sub_dag_index(&self) -> u64;
}

/// A client subscribing to the consensus output and executing every transaction.
pub struct Executor;

impl Executor {
    /// Spawn a new client subscriber.
    pub fn spawn<DB: Database>(
        config: ConsensusConfig<DB>,
        rx_shutdown: Noticer,
        consensus_bus: ConsensusBus,
        network: anemo::Network,
        task_manager: &TaskManager,
    ) {
        // Spawn the subscriber.
        spawn_subscriber(config, rx_shutdown, consensus_bus, network, task_manager);

        // Return the handle.
        info!("Consensus subscriber successfully started");
    }
}

pub async fn get_restored_consensus_output<DB: Database>(
    consensus_store: Arc<ConsensusStore<DB>>,
    last_executed_sub_dag_index: u64,
) -> Result<Vec<CommittedSubDag>, SubscriberError> {
    // We can safely recover from the `last_executed_sub_dag_index + 1` as we have the guarantee
    // from the consumer that the `last_executed_sub_dag_index` transactions have been atomically
    // processed and don't need to re-send the last sub dag.

    // let last_executed_sub_dag_index = execution_state.last_executed_sub_dag_index().await;
    let restore_sub_dag_index_from = last_executed_sub_dag_index + 1;

    let compressed_sub_dags =
        consensus_store.read_committed_sub_dags_from(&restore_sub_dag_index_from)?;

    let mut sub_dags = Vec::new();
    for compressed_sub_dag in compressed_sub_dags {
        sub_dags.push(compressed_sub_dag);
    }

    Ok(sub_dags)
}
