//! State management methods for [StateSynchronizer] for primary headers.

use super::CertificateManagerCommand;
use crate::ConsensusBus;
use consensus_metrics::monitored_scope;
use futures::{stream::FuturesOrdered, StreamExt as _};
use std::collections::HashMap;
use tn_config::{ConsensusConfig, RetryConfig};
use tn_network_types::{PrimaryToWorkerClient as _, WorkerSynchronizeMessage};
use tn_storage::{CertificateStore, PayloadStore};
use tn_types::{
    error::{DagError, HeaderError, HeaderResult},
    Certificate, CertificateDigest, Database, Header, Round, TnSender as _,
};
use tokio::sync::oneshot;
use tracing::debug;

#[cfg(test)]
#[path = "../tests/header_validator_tests.rs"]
mod header_validator_tests;

/// Validate header vote requests from peers.
#[derive(Debug, Clone)]
pub(super) struct HeaderValidator<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
}

impl<DB> HeaderValidator<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(super) fn new(config: ConsensusConfig<DB>, consensus_bus: ConsensusBus) -> Self {
        Self { consensus_bus, config }
    }

    /// Returns the parent certificates of the given header, waits for availability if needed.
    pub(super) async fn notify_read_parent_certificates(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<Certificate>> {
        let mut parents = Vec::new();
        if header.round() == 1 {
            for digest in header.parents() {
                match self.config.genesis().get(digest) {
                    Some(certificate) => parents.push(certificate.clone()),
                    None => return Err(HeaderError::InvalidGenesisParent(*digest)),
                };
            }
        } else {
            let mut cert_notifications: FuturesOrdered<_> = header
                .parents()
                .iter()
                .map(|digest| self.config.node_storage().notify_read(*digest))
                .collect();
            while let Some(result) = cert_notifications.next().await {
                parents.push(result?);
            }
        }

        Ok(parents)
    }

    /// Synchronize batches.
    pub(super) async fn sync_header_batches(
        &self,
        header: &Header,
        is_certified: bool,
        max_age: Round,
    ) -> HeaderResult<()> {
        // skip batch sync for own workers
        if let Some(authority_id) = self.config.authority_id() {
            if header.author() == &authority_id {
                debug!(target: "primary::header_validator", "skipping sync_batches for header - no need to sync payload from own workers");
                return Ok(());
            }
        }

        // Clone the round updates channel so we can get update notifications specific to
        // this RPC handler.
        let mut rx_committed_round_updates =
            self.consensus_bus.committed_round_updates().subscribe();
        let mut committed_round = *rx_committed_round_updates.borrow();
        let max_round = committed_round.saturating_sub(max_age);
        if header.round() < max_round {
            return Err(HeaderError::TooOld {
                digest: header.digest(),
                header_round: header.round(),
                max_round,
            });
        }

        let mut missing = HashMap::new();
        for (digest, (worker_id, _)) in header.payload().iter() {
            // The primary must verify that batches come from the correct worker IDs by storing
            // (digest, worker_id) pairs. This prevents a critical attack vector where malicious
            // nodes can cause synchronization deadlocks:
            //
            // Attack scenario:
            // 1. A malicious node distributes batch X through worker_0 to reach 2f honest nodes
            // 2. The malicious node then creates a header claiming batch X came from worker_1
            // 3. The 2f nodes that already have batch X (from worker_0) can validate the header
            //    without syncing, allowing them to participate in certifying the malformed header
            // 4. The remaining honest nodes get stuck in a deadlock - they continually try to sync
            //    batch X from worker_1, but the batch only exists in worker_0
            // 5. This permanently fragments the network, as clients also query worker_1 for batch X
            //    but never receive it
            //
            // By enforcing strict worker ID validation, the primary ensures batches can only
            // be included in headers if they originated from the claimed worker. This prevents
            // malicious nodes from exploiting worker ID mismatches to create unresolvable
            // synchronization states.
            // Note on this note- the soure of batches is now agnostic so this may not be a DOS
            // anymore, still seems like a useful check though...
            if !self.config.node_storage().contains_payload(*digest, *worker_id)? {
                missing.entry(*worker_id).or_insert_with(Vec::new).push(*digest);
            }
        }

        // Build Synchronize requests to workers.
        let mut synchronize_handles = Vec::new();
        for (worker_id, digests) in missing {
            let client = self.config.local_network().clone();
            let retry_config = RetryConfig::default(); // 30s timeout
            let handle = retry_config.retry(move || {
                let digests = digests.clone();
                let message = WorkerSynchronizeMessage {
                    digests: digests.clone(),
                    target: header.author().clone(),
                    is_certified,
                };
                let client = client.clone();
                async move {
                    let result = client.synchronize(message).await.map_err(|e| {
                        backoff::Error::transient(DagError::NetworkError(format!("{e:?}")))
                    });
                    if result.is_ok() {
                        for digest in &digests {
                            self.config
                                .node_storage()
                                .write_payload(digest, &worker_id)
                                .map_err(|e| backoff::Error::permanent(DagError::StoreError(e)))?
                        }
                    }
                    result
                }
            });
            synchronize_handles.push(handle);
        }

        // Wait until results are back, or this request gets too old to continue.
        let mut wait_synchronize = futures::future::try_join_all(synchronize_handles);
        loop {
            tokio::select! {
                results = &mut wait_synchronize => {
                    break results
                        .map(|_| ())
                        .map_err(|e| HeaderError::SyncBatches(format!("error synchronizing batches: {e:?}")))
                },

                // The synchronization abort condition checks against the committed round from consensus.
                // During vote request processing, this creates a timing consideration: synchronization
                // might continue for headers that are already too old relative to the committed round
                // to receive votes. While this extended synchronization does not affect correctness
                // (since requesters can terminate their requests at any time), it may consume
                // unnecessary system resources by synchronizing batches for headers that will be
                // rejected due to age.
                //
                // A future optimization could incorporate the header's round as an additional abort
                // condition, allowing faster termination of synchronization for headers that are
                // too old relative to the committed round. This optimization becomes valuable if
                // monitoring shows significant resource usage from these extended synchronization
                // attempts.
                Ok(()) = rx_committed_round_updates.changed() => {
                    committed_round = *rx_committed_round_updates.borrow_and_update();
                    debug!(target: "primary::header_validator", ?committed_round, "committed round update");

                    if header.round < committed_round.saturating_sub(max_age) {
                        return Err(HeaderError::TooOld{
                            digest: header.digest(),
                            header_round: header.round(),
                            max_round: committed_round,
                        });
                    }
                },
            }
        }
    }

    /// Filter parent digests that do not exist in storage or pending state.
    ///
    /// Returns a collection of missing parent digests.
    pub(super) async fn identify_unkown_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        let _scope = monitored_scope("vote::get_unknown_parent_digests");

        // handle genesis
        if header.round() == 1 {
            for digest in header.parents() {
                if !self.config.genesis().contains_key(digest) {
                    return Err(HeaderError::InvalidGenesisParent(*digest));
                }
            }
            return Ok(Vec::new());
        }

        // check database
        let existence = self.config.node_storage().multi_contains(header.parents().iter())?;
        let unknown: Vec<_> = header
            .parents()
            .iter()
            .zip(existence.iter())
            .filter_map(|(digest, exists)| if *exists { None } else { Some(*digest) })
            .collect();

        // check pending certificates
        let (reply, filtered) = oneshot::channel();
        self.consensus_bus
            .certificate_manager()
            .send(CertificateManagerCommand::FilterUnkownDigests { unknown, reply })
            .await?;
        let unknown = filtered.await.map_err(|_| HeaderError::PendingCertificateOneshot)?;
        Ok(unknown)
    }
}
