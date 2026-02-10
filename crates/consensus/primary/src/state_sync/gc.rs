//! Garbage collection service for the entire primary.

use crate::{
    certificate_fetcher::CertificateFetcherCommand,
    error::{GarbageCollectorError, GarbageCollectorResult},
    ConsensusBus,
};
use tn_config::ConsensusConfig;
use tn_types::{Database, TnSender as _};
use tokio::{sync::watch, time::interval};
use tracing::{error, warn};

/// Long running task that manages the garbage collection events from consensus.
///
/// When the DAG advances the GC round, this task updates the [AtomicRound] and notifies
/// subscribers.
#[derive(Debug)]
pub(super) struct GarbageCollector<DB> {
    /// The consensus configuration.
    config: ConsensusConfig<DB>,
    /// Consensus message channels.
    consensus_bus: ConsensusBus,
    /// Watch channel for gc updates.
    rx_committed_round_updates: watch::Receiver<u32>,
}

impl<DB> GarbageCollector<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(super) fn new(config: ConsensusConfig<DB>, consensus_bus: ConsensusBus) -> Self {
        let rx_committed_round_updates = consensus_bus.committed_round_updates().subscribe();
        Self { config, consensus_bus, rx_committed_round_updates }
    }

    /// Request the certificate fetcher to request certificates from peers.
    ///
    /// This method is called after the node hasn't received enough parents from the previous round
    /// to advance. The fallback timer is used to attempt to recover by requesting certificates
    /// from peers.
    async fn process_max_round_timeout(&self) -> GarbageCollectorResult<()> {
        // log warning
        warn!(target: "primary::gc",
            "no consensus commit happened for {:?}, triggering certificate fetching.",
            self.config.network_config().sync_config().max_consenus_round_timeout
        );

        // trigger fetch certs
        if let Err(e) =
            self.consensus_bus.certificate_fetcher().send(CertificateFetcherCommand::Kick).await
        {
            error!(target: "primary::gc", ?e, "failed to send on tx_certificate_fetcher");
            return Err(GarbageCollectorError::TNSend("certificate fetcher".to_string()));
        }

        Ok(())
    }

    /// Poll the gc for timeout or consensus commits.
    ///
    /// Upon a successful commit, updates the atomic gc round for all consensus tasks.
    ///
    /// A non-fatal error is returned for timeouts. In a follow-up PR, the manager should handle
    /// certificate fetching.
    pub(super) async fn ready(&mut self) -> GarbageCollectorResult<()> {
        let mut max_round_timeout =
            interval(self.config.network_config().sync_config().max_consenus_round_timeout);
        // reset so interval doesn't tick right away
        max_round_timeout.reset();

        tokio::select! {
            // fallback timer to trigger requesting certificates from peers
            _ = max_round_timeout.tick() => {
                self.process_max_round_timeout().await?;
                return Err(GarbageCollectorError::Timeout);
            }

            // round update watch channel
            update = self.rx_committed_round_updates.changed() => {
                // ensure change notification isn't an error
                update.map_err(GarbageCollectorError::ConsensusRoundWatchChannel).inspect_err(|e| {
                    error!(target: "primary::gc", ?e, "rx_consensus_round_updates watch error. shutting down...");
                })?;

                // reset timer - the happy path
                max_round_timeout.reset();
            }
        }

        Ok(())
    }
}
