//! Garbage collection service for the entire primary.

use super::{AtomicRound, CertificateManagerCommand};
use crate::{
    aggregators::CertificatesAggregatorManager,
    certificate_fetcher::CertificateFetcherCommand,
    error::{GarbageCollectorError, GarbageCollectorResult},
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU32, Arc},
};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{Round, TnSender as _};
use tokio::{sync::watch, time::interval};
use tracing::{error, warn};

impl AtomicRound {
    /// Store the new atomic round.
    ///
    /// NOTE: only GC should call this
    fn store(&mut self, new: u32) {
        self.inner.atomic.store(new, std::sync::atomic::Ordering::Release);
    }
}

pub struct GarbageCollector<DB> {
    /// The consensus configuration.
    config: ConsensusConfig<DB>,
    /// Consensus message channels.
    consensus_bus: ConsensusBus,
    /// Watch channel for gc updates.
    rx_gc_round_updates: watch::Receiver<u32>,
    /// The atomic gc round.
    ///
    /// This managed by `Self` and is read by CertificateValidator and CertificateManager.
    gc_round: AtomicRound,
}

impl<DB> GarbageCollector<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        gc_round: AtomicRound,
    ) -> Self {
        let rx_gc_round_updates = consensus_bus.gc_round_updates().subscribe();
        Self { config, consensus_bus, rx_gc_round_updates, gc_round }
    }

    /// Spawn the garbage collection task to update the gc round for all consensus tasks.
    ///
    /// The GC is a loop that tries to fetch certificates if the consensus round doesn't update
    /// within `max_timeout_sync_certificates`
    pub async fn run(&mut self) -> GarbageCollectorResult<()> {
        let mut max_round_timeout =
            interval(self.config.network_config().sync_config().max_consenus_round_timeout);
        // reset so interval doesn't tick right away
        max_round_timeout.reset();

        // listen for shutdown
        let shutdown_rx = self.config.shutdown().subscribe();

        // main event loop for gc maintenance
        loop {
            tokio::select! {
                // fallback timer to trigger requesting certificates from peers
                _ = max_round_timeout.tick() => {
                    self.process_max_round_timeout().await?;
                }

                // round update watch channel
                update = self.rx_gc_round_updates.changed() => {
                    // ensure change notification isn't an error
                    update.map_err(|e| GarbageCollectorError::ConsensusRoundWatchChannel(e)).inspect_err(|e| {
                        error!(target: "primary::gc", ?e, "rx_consensus_round_updates watch error. shutting down...");
                    })?;

                    self.process_next_round().await?;

                    // reset timer - the happy path
                    max_round_timeout.reset();
                }

                // shutdown
                _ = &shutdown_rx => break,
            }
        }

        Ok(())
    }

    /// The round advanced within time. Process the round
    async fn process_next_round(&mut self) -> GarbageCollectorResult<()> {
        let _scope = monitored_scope("primary::gc");

        // update gc round
        let new_round = *self.rx_gc_round_updates.borrow_and_update();
        self.gc_round.store(new_round);

        // notify certificate manager
        self.consensus_bus
            .certificate_manager()
            .send(CertificateManagerCommand::NewGCRound)
            .await?;

        Ok(())
    }

    /// Request the certificate fetcher to request certificates from peers.
    ///
    /// This method is called after the node hasn't received enough parents from the previous round
    /// to advance. The fallback timer is used to attempt to recover by requesting certificates
    /// from peers.
    async fn process_max_round_timeout(&self) -> GarbageCollectorResult<()> {
        if let Err(e) =
            self.consensus_bus.certificate_fetcher().send(CertificateFetcherCommand::Kick).await
        {
            error!(target: "primary::gc", ?e, "failed to send on tx_certificate_fetcher");
            return Err(GarbageCollectorError::TNSend("certificate fetcher".to_string()));
        }

        // log metrics and warning
        self.consensus_bus.primary_metrics().node_metrics.synchronizer_gc_timeout.inc();
        warn!(target: "primary::gc", "no consensus commit happened for {:?}, triggering certificate fetching.", self.config.network_config().sync_config().max_consenus_round_timeout);
        Ok(())
    }
}
