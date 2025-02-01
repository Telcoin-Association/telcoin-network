//! Garbage collection service for the entire primary.

use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU32, Arc},
};

use crate::{
    aggregators::CertificatesAggregator,
    certificate_fetcher::CertificateFetcherCommand,
    consensus::AtomicRound,
    error::{GarbageCollectorError, GarbageCollectorResult},
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use parking_lot::Mutex;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{Round, TnSender as _};
use tokio::{sync::watch, time::interval};
use tracing::{error, warn};

pub struct GarbageCollector<DB> {
    /// The consensus configuration.
    config: ConsensusConfig<DB>,
    /// Consensus message channels.
    consensus_bus: ConsensusBus,
    /// Watch channel for gc updates.
    rx_gc_round_updates: watch::Receiver<AtomicRound>,
    /// Aggregates certificates to use as parents for new headers.
    ///
    /// This is shared with synchronizer.
    certificates_aggregator: Arc<Mutex<BTreeMap<Round, Box<CertificatesAggregator>>>>,
    /// Shared state for tracking suspended certificates and when they can be accepted.
    state: Arc<tokio::sync::Mutex<State>>,
}

impl<DB> GarbageCollector<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        certificates_aggregator: Arc<Mutex<BTreeMap<Round, Box<CertificatesAggregator>>>>,
        state: tokio::sync::Mutex<State>,
    ) -> Self {
        let rx_gc_round_updates = consensus_bus.gc_round_updates().subscribe();
        Self { config, consensus_bus, rx_gc_round_updates, certificates_aggregator, state }
    }

    /// Spawn the garbage collection task to update the gc round for all consensus tasks.
    ///
    /// The GC is a loop that tries to fetch certificates if the consensus round doesn't update within
    /// `max_timeout_sync_certificates`
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
                    continue;
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

        let gc_round = self.rx_gc_round_updates.borrow_and_update().load();

        // cleanup aggregated certificates
        {
            self.certificates_aggregator.lock().retain(|k, _| k > &gc_round);
        }

        // L466

        Ok(())
    }

    /// Request the certificate fetcher to request certificates from peers.
    ///
    /// This method is called after the node hasn't received enough parents from the previous round to advance.
    /// The fallback timer is used to attempt to recover by requesting certificates from peers.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::Ordering, Arc};

    #[derive(Clone)]
    struct Test {
        inner: Arc<Inner>,
    }

    impl Test {
        fn update(&mut self) {
            let new = self.inner.atomic.load(Ordering::Acquire) + 1;
            self.inner.atomic.store(new, Ordering::Release);
        }

        fn read(&self) -> u32 {
            let val = self.inner.atomic.load(Ordering::Acquire);
            val
        }
    }

    struct Inner {
        atomic: AtomicU32,
    }

    #[tokio::test]
    async fn test_atomic_val() {
        let mut test = Test { inner: Arc::new(Inner { atomic: AtomicU32::new(0) }) };

        let test2 = test.clone();
        tokio::spawn(async move {
            for _ in 0..=5 {
                println!("updating...");
                test.update();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        // stagger start
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        tokio::spawn(async move {
            for _ in 0..=5 {
                let val = test2.read();
                println!("read val: {val}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_secs(7)).await;

        println!("done");
    }
}
