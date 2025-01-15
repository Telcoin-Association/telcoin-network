//! Process consensus output and execute every transaction.

mod errors;
pub mod subscriber;
use crate::subscriber::spawn_subscriber;
use async_trait::async_trait;
pub use errors::{SubscriberError, SubscriberResult};
use mockall::automock;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_primary::ConsensusBus;
use tn_storage::traits::Database;
use tn_storage::{CertificateStore, ConsensusStore};
use tn_types::{CertificateDigest, CommittedSubDag, ConsensusOutput, Noticer, TaskManager};
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

/// Restore the last committed sub dag based on the latest execution result.
///
/// The next subdag after the passed `last_executed_sub_dag_index` is used. Callers must ensure that the value passed is the last fully-executed subdag index.
pub async fn get_restored_consensus_output<DB: Database>(
    consensus_store: Arc<ConsensusStore<DB>>,
    certificate_store: CertificateStore<DB>,
    last_executed_sub_dag_index: u64,
) -> Result<Vec<CommittedSubDag>, SubscriberError> {
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
