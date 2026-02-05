//! The Primary type

use crate::{
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::LeaderSchedule,
    network::{PrimaryNetworkHandle, WorkerReceiverHandler},
    proposer::Proposer,
    state_handler::StateHandler,
    ConsensusBus, StateSynchronizer,
};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_types::{Database, TaskManager, TnSender};
use tracing::info;

#[cfg(test)]
#[path = "tests/primary_tests.rs"]
mod primary_tests;

#[derive(Debug)]
/// The main `Primary` struct.
pub struct Primary<DB> {
    /// Handle to the primary network.
    primary_network: PrimaryNetworkHandle,
    ///  State synchronizer.
    state_sync: StateSynchronizer<DB>,
}

impl<DB: Database> Primary<DB> {
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        primary_network: PrimaryNetworkHandle,
        state_sync: StateSynchronizer<DB>,
    ) -> Self {
        // Write the parameters to the logs.
        config.parameters().tracing();

        // Some info statements
        info!(
            "Boot primary node with public key {:?}",
            config.authority().as_ref().map(|a| a.protocol_key().encode_base58()),
        );

        let worker_receiver_handler =
            WorkerReceiverHandler::new(consensus_bus.clone(), config.node_storage().clone());

        config
            .local_network()
            .set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler));

        Self { primary_network, state_sync }
    }

    /// Spawns the primary.
    pub fn spawn(
        &mut self,
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        leader_schedule: LeaderSchedule,
        task_manager: &TaskManager,
    ) {
        // Probably don't need this for a non-committee member but it keeps channels drained and is
        // not a problem.
        self.state_sync.spawn(task_manager);

        Certifier::spawn(
            config.clone(),
            consensus_bus.clone(),
            self.state_sync.clone(),
            self.primary_network.clone(),
            task_manager,
        );

        // The `CertificateFetcher` waits to receive all the ancestors of a certificate before
        // looping it back to the `Synchronizer` for further processing.
        CertificateFetcher::spawn(
            config.clone(),
            self.primary_network.clone(),
            consensus_bus.clone(),
            self.state_sync.clone(),
            task_manager,
        );

        // Only run the proposer task if we are a CVV.
        if consensus_bus.is_cvv() {
            // When the `Synchronizer` collects enough parent certificates, the `Proposer` generates
            // a new header with new block digests from our workers and sends it to the `Certifier`.
            let proposer = Proposer::new(
                config.clone(),
                config.authority_id().expect("CVV has an auth id"),
                consensus_bus.clone(),
                leader_schedule,
                task_manager.get_spawner(),
            );

            proposer.spawn(task_manager);
        }

        if let Some(authority_id) = config.authority_id() {
            // This only makes sense if we are a validator (i.e. have an authority id).
            // Keeps track of the latest consensus round and allows other tasks to clean up their
            // their internal state
            StateHandler::spawn(
                authority_id,
                consensus_bus,
                config.shutdown().subscribe(),
                task_manager,
            );
        }

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {:?} successfully booted on {:?}",
            config.authority_id(),
            config.config().node_info.p2p_info.primary.network_address
        );
    }

    /// Return a reference to the Primary's network.
    pub fn network_handle(&self) -> &PrimaryNetworkHandle {
        &self.primary_network
    }

    /// Return a clone of the Primary's [StateSynchronizer].
    pub fn state_sync(&self) -> StateSynchronizer<DB> {
        self.state_sync.clone()
    }
}
