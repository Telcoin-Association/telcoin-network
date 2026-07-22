//! The Primary type

use crate::{
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::LeaderSchedule,
    network::{PrimaryNetworkHandle, WorkerReceiverHandler},
    proposer::Proposer,
    ConsensusBus, StateSynchronizer,
};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_types::{Database, TaskManager};
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
        // Every `spawn` below subscribes to its channels synchronously and only then starts its
        // task, so the order of these calls decides which channels are live before the first
        // message is sent. `QueChannel::send` silently DROPS when nothing is subscribed (it returns
        // `Ok(())` without enqueuing), so a sender that starts before its consumer subscribes loses
        // messages with no error anywhere. Two such dependencies exist here:
        //
        //   - `StateSynchronizer` replays the last two rounds of certificates onto the `parents`
        //     channel from `recover_state`, and the `Proposer` is the only subscriber.
        //   - `StateSynchronizer` also drives the `certificate_fetcher` channel, whose only
        //     subscriber is the `CertificateFetcher`.
        //
        // So `state_sync` is spawned LAST, after every consumer it feeds is already subscribed.
        // Previously it was spawned first, which raced both subscriptions: a restarted proposer
        // could lose its recovery parents outright and be left with nothing to advance on.
        //
        // Spawning the consumers first is safe in the other direction because none of them can
        // send to a not-yet-subscribed channel before `state_sync` is up: the certifier blocks on
        // `rx_headers` until the proposer proposes, and the proposer's first header cannot be sent
        // until the certifier has subscribed, because `Certifier::spawn` runs before the proposer
        // task can reach its first send.
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

        // Probably don't need this for a non-committee member but it keeps channels drained and is
        // not a problem.
        //
        // Spawned last: see the ordering note above.
        self.state_sync.spawn(task_manager);

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
