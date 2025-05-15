//! Process standalone validated and verified certificates.
//!
//! This module is responsible for checking certificate parents, managing pending certificates, and
//! accepting certificates that become unlocked.

use super::{gc::GarbageCollector, pending_cert_manager::PendingCertificateManager, AtomicRound};
use crate::{
    aggregators::certificates::CertificatesAggregatorManager,
    certificate_fetcher::CertificateFetcherCommand,
    error::{CertManagerError, CertManagerResult, GarbageCollectorError},
    state_sync::cert_validator::certificate_source,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tn_config::ConsensusConfig;
use tn_storage::CertificateStore;
use tn_types::{
    error::{CertificateError, HeaderError},
    Certificate, CertificateDigest, Database, Hash as _, TnReceiver as _, TnSender as _,
};
use tokio::sync::oneshot;
use tracing::{debug, error};

#[cfg(test)]
#[path = "../tests/cert_manager_tests.rs"]
mod cert_manager_tests;

/// Process validated certificates.
///
/// Long-running task to manage pending certificate requests and accept verified certificates.
#[derive(Debug)]
pub struct CertificateManager<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// State for pending certificate.
    pending: PendingCertificateManager,
    /// Collection of parents to advance the round.
    ///
    /// This is shared with the `GarbageCollector`.
    parents: CertificatesAggregatorManager,
    /// The task responsible for managing garbage collection.
    garbage_collector: GarbageCollector<DB>,
    /// Highest garbage collection round.
    ///
    /// This is managed by GarbageCollector and shared with CertificateValidator.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
}

impl<DB> CertificateManager<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
    ) -> Self {
        let parents = CertificatesAggregatorManager::new(consensus_bus.clone());
        let pending = PendingCertificateManager::new(consensus_bus.clone());
        let garbage_collector =
            GarbageCollector::new(config.clone(), consensus_bus.clone(), gc_round.clone());

        Self {
            consensus_bus,
            config,
            pending,
            parents,
            garbage_collector,
            gc_round,
            highest_processed_round,
        }
    }

    /// Process verified certificate.
    ///
    /// Returns an error if a certificate is unverified. This will accept certificate or mark it as
    /// pending if parents are missing.
    async fn process_verified_certificates(
        &mut self,
        certs: Vec<Certificate>,
    ) -> CertManagerResult<()> {
        // process entire collection of certificates
        //
        // these can be single, fetched from certificate fetcher or unlocked pending
        // if any are pending, return the pending error
        let mut result = Ok(());

        // collect results
        for cert in certs {
            let digest = cert.digest();

            // guarantee certificate is verified before storing in pending
            // NOTE: this is the only time this is checked
            if !cert.is_verified() {
                // stop processing certs if unverified
                return Err(CertManagerError::UnverifiedSignature(digest));
            }

            // check pending status
            if self.pending.is_pending(&digest) {
                // metrics
                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .certificates_suspended
                    .with_label_values(&["dedup_locked"])
                    .inc();

                // track error for caller that at least one cert is pending
                // and continue processing other certs
                result = Err(CertManagerError::Pending(digest));
                continue;
            }

            // ensure no missing parents (either pending or garbage collected)
            // check parents are either accounted for or garbage collected
            //
            // NOTE: this also ensures certificates are accepted in causal order
            // which is a strict requirement for consensus to build the DAG correctly
            if cert.round() > self.gc_round.load() + 1 {
                let missing_parents = self.get_missing_parents(&cert).await?;
                if !missing_parents.is_empty() {
                    self.pending.insert_pending(cert, missing_parents)?;
                    // metrics
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_currently_suspended
                        .set(self.pending.num_pending() as i64);

                    // track error for caller that at least one cert is pending
                    // and continue processing other certs
                    result = Err(CertManagerError::Pending(digest));
                    continue;
                }
            }

            // no missing parents - update pending state and
            let mut unlocked = self.pending.update_pending(cert.round(), digest)?;
            // append cert and process all certs in causal order
            unlocked.push_front(cert);
            self.accept_verified_certificates(unlocked).await?;
        }

        result
    }

    /// Check that certificate's parents are in storage. Returns the digests of any parents that are
    /// missing.
    async fn get_missing_parents(
        &self,
        certificate: &Certificate,
    ) -> CertManagerResult<HashSet<CertificateDigest>> {
        let _scope = monitored_scope("primary::state-sync::get_missing_parents");

        // handle genesis cert
        if certificate.round() == 1 {
            debug!(target: "primary::cert_manager", ?certificate, "cert round 1");
            for digest in certificate.header().parents() {
                if !self.config.genesis().contains_key(digest) {
                    return Err(
                        CertificateError::from(HeaderError::InvalidGenesisParent(*digest)).into()
                    );
                }
            }
            return Ok(HashSet::new());
        }

        // check storage
        let existence =
            self.config.node_storage().multi_contains(certificate.header().parents().iter())?;
        let missing_parents: HashSet<_> = certificate
            .header()
            .parents()
            .iter()
            .zip(existence.iter())
            .filter(|(_, exists)| !*exists)
            .map(|(digest, _)| *digest)
            .collect();

        // send request to start fetching parents
        if !missing_parents.is_empty() {
            debug!(target: "primary::cert_manager", ?certificate, "missing {} parents", missing_parents.len());
            // metrics
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .certificates_suspended
                .with_label_values(&["missing_parents"])
                .inc();

            // start fetching parents
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(Arc::new(certificate.clone())))
                .await?;
        }

        Ok(missing_parents)
    }

    /// Try to accept the verified certificate.
    ///
    /// The certificate's state must be verified. This method writes to storage and returns the
    /// result to caller.
    ///
    /// NOTE: `self::process_verified_certificates` checks the verification status, so all
    /// certificates managed here are verified.
    // synchronizer::accept_certificate_internal
    async fn accept_verified_certificates(
        &mut self,
        certificates: VecDeque<Certificate>,
    ) -> CertManagerResult<()> {
        let _scope = monitored_scope("primary::cert_manager::accept_certificate");
        debug!(target: "primary::cert_manager", ?certificates, "accepting {:?} certificates", certificates.len());

        // write certificates to storage
        self.config.node_storage().write_all(certificates.clone())?;

        for cert in certificates.into_iter() {
            // Update metrics for accepted certificates.
            let highest_processed_round =
                self.highest_processed_round.fetch_max(cert.round()).max(cert.round());
            let certificate_source = certificate_source(&self.config, &cert);
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .highest_processed_round
                .with_label_values(&[certificate_source])
                .set(highest_processed_round as i64);
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .certificates_processed
                .with_label_values(&[certificate_source])
                .inc();

            // NOTE: these next two steps are considered critical
            //
            // any error must be treated as fatal to avoid inconsistent state between DAG and
            // certificate store
            //
            // append parent for round
            self.parents
                .append_certificate(cert.clone(), self.config.committee())
                .await
                .inspect_err(|e| {
                    error!(target: "primary::cert_manager", ?e, "failed to append cert");
                })
                .map_err(|_| CertManagerError::FatalAppendParent)?;

            // send to consensus for processing into the DAG
            self.consensus_bus.new_certificates().send(cert).await.inspect_err(|e| {
                error!(target: "primary::cert_manager", ?e, "failed to forward accepted certificate to consensus");
            }).map_err(|_| CertManagerError::FatalForwardAcceptedCertificate)?;
        }

        Ok(())
    }

    /// Update state with new GC round.
    ///
    /// This method checks missing parents for the GC round. If a parent is garbage collected, the
    /// pending collection is updated to collect any dependents that become unlocked (ie - no more
    /// missing parents).
    async fn process_gc_round(&mut self) -> CertManagerResult<()> {
        // load latest gc round
        let gc_round = self.gc_round.load();

        // clear certificate aggregators for expired rounds
        self.parents.garbage_collect(&gc_round);

        // iterate one round at a time to preserver causal order
        while let Some((round, digest)) = self.pending.next_for_gc_round(gc_round) {
            let unlocked = self.pending.update_pending(round, digest)?;
            self.accept_verified_certificates(unlocked).await?;
        }

        Ok(())
    }

    /// Startup tasks to synchronize state for primary.
    async fn recover_state(&mut self) -> CertManagerResult<()> {
        // send last round to proposer
        let last_round_certificates = self
            .config
            .node_storage()
            .last_two_rounds_certs()
            .expect("Failed recovering certificates in primary core");

        // update parents
        for certificate in last_round_certificates {
            self.parents.append_certificate(certificate, self.config.committee()).await?;
        }

        Ok(())
    }

    /// Long running task to manage verified certificates.
    ///
    /// Certificate signature states are first verified, then parents are checked. If certificate
    /// parents are missing, the manager tracks them as pending. As parents become available or are
    /// removed through garbage collection, the certificate manager will update pending state and
    /// try to accept all known certificates.
    pub(crate) async fn run(mut self) -> CertManagerResult<()> {
        let shutdown_rx = self.config.shutdown().subscribe();
        let mut certificate_manager_rx = self.consensus_bus.certificate_manager().subscribe();

        // recover state
        self.recover_state().await?;

        // process certificates until shutdown
        loop {
            tokio::select! {
                // update state
                Some(command) = certificate_manager_rx.recv() => {
                    match command {
                        CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply } => {
                            let result= self.process_verified_certificates(certificates).await;

                            match result{
                                // return fatal errors immediately to force shutdown
                                Err(CertManagerError::FatalAppendParent)
                                | Err(CertManagerError::FatalForwardAcceptedCertificate) => {
                                    error!(target: "primary::cert_manager", ?result, "fatal error. shutting down...");
                                    return result;
                                }

                                non_fatal_results => {
                                    let _ = reply.send(non_fatal_results);
                                }
                            }
                        }

                        CertificateManagerCommand::FilterUnkownDigests { mut unknown, reply } => {
                            self.pending.filter_unknown_digests(&mut unknown);
                            let _ = reply.send(unknown);
                        },
                    }
                }

                result = self.garbage_collector.ready() => {
                    match result {
                        Ok(()) => self.process_gc_round().await?,
                        Err(GarbageCollectorError::Timeout) => (), // ignore non-fatal
                        _ => result? // return fatal error
                    }
                }

                // shutdown signal
                _ = &shutdown_rx => {
                    return Ok(());
                }
            }
        }
    }
}

/// Commands for the [CertficateManager].
#[derive(Debug)]
pub(crate) enum CertificateManagerCommand {
    /// Message from CertificateValidator.
    ProcessVerifiedCertificates {
        /// The certificate that was verified.
        ///
        /// Try to accept this certificate. If it has missing parents, track the certificate as
        /// pending and return an error.
        certificates: Vec<Certificate>,
        /// Return the result to the certificate validator.
        reply: oneshot::Sender<CertManagerResult<()>>,
    },
    /// Filter certificate digests that are not in local storage.
    ///
    /// Remove digests that are already tracked by `Pending`.
    /// This is used to vote on headers.
    FilterUnkownDigests {
        /// The collection of digests not found in local storage.
        unknown: Vec<CertificateDigest>,
        /// Return the result to the header validator.
        reply: oneshot::Sender<Vec<CertificateDigest>>,
    },
}
