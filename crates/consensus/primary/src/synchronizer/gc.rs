//! Type to manage garbage collection.
//!
//! Syncs the gc_round in-memory for all tasks.
//! This also triggers an attempt to catchup with peers if  no garbage collection or consensus
//! commit has happened  withthin the timelimit.

use super::Parents;
use crate::{certificate_fetcher::CertificateFetcherCommand, ConsensusBus};
use consensus_metrics::{monitored_scope, spawn_logged_monitored_task};
use prometheus::core::AtomicU64;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tn_storage::CertificateStore;
use tn_types::{error::DagError, TnSender as _};
use tokio::time::timeout;
use tracing::{error, trace, warn};

/// Task to sync garbage collection for Primary.
pub struct SyncGC {
    /// The bus for inner-process communication.
    consensus_bus: ConsensusBus,
    /// The atomically increasing garbage collection round for this primary.
    ///
    /// NOTE: this is the only task that updates the primary's gc_round.
    gc_round: AtomicU64,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: Parents,
}

impl SyncGC {
    /// Spawn this task to manage garbage collection.
    pub fn spawn(
        consensus_bus: ConsensusBus,
        gc_round: AtomicU64,
        certificates_aggregators: Parents,
    ) {
        let mut rx_consensus_round_updates = consensus_bus.consensus_round_updates().subscribe();
        spawn_logged_monitored_task!(
            async move {
                const FETCH_TRIGGER_TIMEOUT: Duration = Duration::from_secs(30);
                //let mut rx_consensus_round_updates = rx_consensus_round_updates.clone();
                loop {
                    let Ok(result) =
                        timeout(FETCH_TRIGGER_TIMEOUT, rx_consensus_round_updates.changed()).await
                    else {
                        // When consensus commit has not happened for 30s, it is possible that no
                        // new certificate is received by this primary or
                        // created in the network, so fetching should
                        // definitely be started. For other reasons of
                        // timing out, there is no harm to start fetching either.
                        if let Err(e) = consensus_bus
                            .certificate_fetcher()
                            .send(CertificateFetcherCommand::Kick)
                            .await
                        {
                            error!(target: "primary::synchronizer::gc", ?e, "failed to send on tx_certificate_fetcher");
                            return;
                        }
                        consensus_bus.primary_metrics().node_metrics.synchronizer_gc_timeout.inc();
                        warn!(target: "primary::synchronizer::gc", "No consensus commit happened for {:?}, triggering certificate fetching.", FETCH_TRIGGER_TIMEOUT);
                        continue;
                    };

                    if let Err(e) = result {
                        error!(target: "primary::synchronizer::gc", ?e, "failed to received rx_consensus_round_updates - shutting down...");
                        return;
                    }

                    let _scope = monitored_scope("Synchronizer::gc_iteration");
                    let gc_round = rx_consensus_round_updates.borrow().gc_round;

                    // this is the only task updating gc_round
                    inner.gc_round.store(gc_round, Ordering::Release);
                    inner.certificates_aggregators.lock().retain(|k, _| k > &gc_round);
                    // Accept certificates at and below gc round, if there is any.
                    let mut state = inner.state.lock().await;
                    while let Some(((round, digest), suspended_cert)) = state.run_gc_once(gc_round)
                    {
                        assert!(round <= gc_round, "Never gc certificates above gc_round as this can lead to missing causal history in DAG");

                        let suspended_children_certs = state.accept_children(round, digest);
                        // Acceptance must be in causal order.
                        for suspended in
                            suspended_cert.into_iter().chain(suspended_children_certs.into_iter())
                        {
                            match inner.accept_suspended_certificate(&state, suspended).await {
                                Ok(()) => {
                                    trace!(target: "primary::synchronizer::gc", "accepted suspended cert")
                                }
                                Err(DagError::ShuttingDown) => {
                                    error!(target: "primary::synchronizer::gc", "error accepting suspended cert - shutting down...");
                                    return;
                                }
                                Err(e) => {
                                    error!(target: "primary::synchronizer::gc", ?e, "error accepting suspended cert - PANIC");
                                    panic!("Unexpected error accepting certificate during GC! {e}")
                                }
                            }
                        }
                    }
                }
            },
            "Synchronizer::GarbageCollection"
        );
    }
}
