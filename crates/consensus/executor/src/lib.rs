// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod errors;
mod state;
mod subscriber;

mod metrics;

pub use errors::{SubscriberError, SubscriberResult};
use narwhal_typed_store::traits::Database;
pub use state::ExecutionIndices;

pub use crate::metrics::ExecutorMetrics;
use crate::subscriber::spawn_subscriber;

use async_trait::async_trait;
use consensus_metrics::metered_channel;
use mockall::automock;
use narwhal_network::client::NetworkClient;
use narwhal_storage::{CertificateStore, ConsensusStore};
use std::sync::Arc;
use tn_types::{
    AuthorityIdentifier, CertificateDigest, CommittedSubDag, Committee, ConsensusOutput, Noticer,
    WorkerCache,
};
use tokio::{sync::broadcast, task::JoinHandle};
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
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        authority_id: AuthorityIdentifier,
        worker_cache: WorkerCache,
        committee: Committee,
        client: NetworkClient,
        rx_shutdown: Noticer,
        rx_sequence: metered_channel::Receiver<CommittedSubDag>,
        restored_consensus_output: Vec<CommittedSubDag>,
        consensus_output_notification_sender: broadcast::Sender<ConsensusOutput>,
        // TODO: this is needed to create the tx_notifier channel
        metrics: ExecutorMetrics,
    ) -> SubscriberResult<JoinHandle<()>>
// where
    //     State: ExecutionState + Send + Sync + 'static,
    {
        // TODO: OLD way was to create metrics here,
        // before I moved tx_notifier out

        // let metrics = ExecutorMetrics::new(registry);

        // This will be needed in the `Subscriber`.
        let arc_metrics = Arc::new(metrics);

        // Spawn the subscriber.
        let subscriber_handle = spawn_subscriber(
            authority_id,
            worker_cache,
            committee,
            client,
            rx_shutdown,
            rx_sequence,
            arc_metrics,
            restored_consensus_output,
            consensus_output_notification_sender,
            // execution_state,
        );

        // Return the handle.
        info!("Consensus subscriber successfully started");

        Ok(subscriber_handle)
    }
}

pub async fn get_restored_consensus_output<DB: Database>(
    consensus_store: Arc<ConsensusStore<DB>>,
    certificate_store: CertificateStore<DB>,
    // execution_state: &State,

    // TODO: assume DB looks up finalized block num hash here
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
        let certificate_digests: Vec<CertificateDigest> = compressed_sub_dag.certificates();

        let certificates =
            certificate_store.read_all(certificate_digests)?.into_iter().flatten().collect();

        let leader = certificate_store.read(compressed_sub_dag.leader())?.unwrap();

        sub_dags.push(CommittedSubDag::from_commit(compressed_sub_dag, certificates, leader));
    }

    Ok(sub_dags)
}
