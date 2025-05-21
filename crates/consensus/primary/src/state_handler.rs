//! Filter consensus results to update execution state.

use crate::ConsensusBus;
use consensus_metrics::monitored_future;
use tn_types::{
    AuthorityIdentifier, Certificate, Noticer, Round, TaskManager, TnReceiver, TnSender,
};
use tracing::{debug, error, info};

/// Updates Narwhal system state based on certificates received from consensus.
pub struct StateHandler {
    authority_id: AuthorityIdentifier,

    /// Used for Receives the ordered certificates from consensus.
    consensus_bus: ConsensusBus,
    /// Channel to signal committee changes.
    rx_shutdown: Noticer,
}

impl StateHandler {
    pub fn spawn(
        authority_id: AuthorityIdentifier,
        consensus_bus: &ConsensusBus,
        rx_shutdown: Noticer,
        task_manager: &TaskManager,
    ) {
        let state_handler =
            Self { authority_id, consensus_bus: consensus_bus.clone(), rx_shutdown };
        task_manager.spawn_critical_task(
            "state handler task",
            monitored_future!(
                async move {
                    state_handler.run().await;
                },
                "StateHandlerTask"
            ),
        );
    }

    async fn handle_sequenced(&mut self, commit_round: Round, certificates: Vec<Certificate>) {
        // Now we are going to signal which of our own batches have been committed.
        let own_rounds_committed: Vec<_> = certificates
            .iter()
            .filter_map(|cert| {
                if cert.header().author() == &self.authority_id {
                    Some(cert.header().round())
                } else {
                    None
                }
            })
            .collect();
        debug!(target: "primary::state_handler", "Own committed rounds {:?} at round {:?}", own_rounds_committed, commit_round);

        // If a reporting channel is available send the committed own
        // headers to it.
        if let Err(e) = self
            .consensus_bus
            .committed_own_headers()
            .send((commit_round, own_rounds_committed))
            .await
        {
            error!(target: "primary::state_handler", "error sending commit header: {e}");
        }
    }

    async fn run(mut self) {
        info!(target: "primary::state_handler", "StateHandler on node {} has started successfully.", self.authority_id);
        let mut rx_committed_certificates = self.consensus_bus.committed_certificates().subscribe();
        loop {
            tokio::select! {
                Some((commit_round, certificates)) = rx_committed_certificates.recv() => {
                    self.handle_sequenced(commit_round, certificates).await;
                },

                _ = &self.rx_shutdown => {
                    return;
                }
            }
        }
    }
}
