//! Main actor processing certificates.
//!
//! This module orchestrates all other certificate components, such as garbage collection, pending certs, and accepting new certificates.

use super::{AtomicRound, StateSynchronizer};
use crate::{
    aggregators::CertificatesAggregatorManager,
    certificate_fetcher::CertificateFetcherCommand,
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    network::MissingCertificatesRequest,
    state_sync::PendingCertCommand,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use fastcrypto::hash::Hash as _;
use futures::{stream::FuturesOrdered, FutureExt};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tn_config::ConsensusConfig;
use tn_network::{PrimaryToWorkerClient as _, RetryConfig};
use tn_network_types::WorkerSynchronizeMessage;
use tn_storage::{traits::Database, CertificateStore};
use tn_types::{
    error::{
        AcceptNotification, CertificateError, CertificateResult, DagError, HeaderError,
        HeaderResult,
    },
    AuthorityIdentifier, Certificate, CertificateDigest, Header, Noticer, Round, TnReceiver,
    TnSender as _,
};
use tn_utils::sync::notify_once::NotifyOnce;
use tokio::{sync::oneshot, time::Instant};
use tokio_stream::StreamExt as _;
use tracing::{debug, error, trace, warn};

impl<DB> StateSynchronizer<DB>
where
    DB: Database,
{
    //
    //
    // === new cert intake
    //
    //

    /// Process certificate from this primary.
    async fn process_own_certificate(&self, certificate: Certificate) -> CertificateResult<()> {
        todo!()
    }

    /// Process a new, unverified certificate.
    ///
    /// Try to validate the certificate and add it to the DAG if there are no missing parents. If the certificate contains unkown parents, then return an error and try to fetch them.
    fn process_external_certificate(&mut self, certificate: Certificate) -> CertificateResult<()> {
        let _scope = monitored_scope("state-sync::try_accept_certificate");
        todo!()
    }

    /// Process the certificate.
    ///
    /// If the certificate is external, the certificate is validated and signatures are verified.
    /// The certificate's signature verification status is updated.
    // TODO: process_certificate_internal
    async fn process_certificate(
        &mut self,
        mut certificate: Certificate,
        external: bool,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("state-sync::process_certificate");

        // see if certificate already processed
        let digest = certificate.digest();
        if self.config.node_storage().certificate_store.contains(&digest)? {
            trace!(target: "primary::state-sync", "Certificate {digest:?} has already been processed. Skip processing.");
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .duplicate_certificates_processed
                .inc();
            return Ok(());
        }

        // scrutinize certificates received from peers
        if external {
            // check pending status
            let pending = self.check_pending_status(digest).await?;

            if pending {
                trace!(target: "primary::state-sync", ?digest, "certificate is still suspended - returning suspended error...");

                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .certificates_suspended
                    .with_label_values(&["dedup"])
                    .inc();

                return Err(CertificateError::Pending(digest));
            }

            certificate = self.validate_certificate(certificate)?;
        }

        debug!(target: "primary::state-sync", round=certificate.round(), ?certificate, "processing certificate");

        let certificate_source =
            if self.config.authority().id().eq(&certificate.origin()) { "own" } else { "other" };
        let highest_received_round =
            self.highest_received_round.fetch_max(certificate.round()).max(certificate.round());
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_received_round
            .with_label_values(&[certificate_source])
            .set(highest_received_round as i64);

        // A well-signed certificate from round r provides important information about network
        // progress, even before its contents are fully validated. The certificate's signatures
        // prove that a majority of honest validators have processed all certificates through
        // round r-1. This must be true because these validators could not have participated
        // in creating the round r certificate without first processing its parent rounds.
        //
        // This knowledge enables an important proposer optimization. Given evidence that the
        // network has progressed to round r, generating proposals with parents from rounds
        // earlier than r-1 becomes pointless. Such proposals would be outdated and unable
        // to achieve consensus. Skipping these older rounds prevents the creation of obsolete
        // proposals that the network would ignore.
        //
        // The optimization allows the proposer to stay synchronized with network progress
        // even while parent certificates and payload data are still being downloaded and
        // validated. It extracts actionable timing information from the certificate's
        // signatures alone, independent of the certificate's complete contents.
        let minimal_round_for_parents = certificate.round().saturating_sub(1);
        self.consensus_bus.parents().send((vec![], minimal_round_for_parents)).await?;

        // Instruct workers to download any missing batches referenced in this certificate.
        // Since this header got certified, we are sure that all the data it refers to (ie. its
        // batches and its parents) are available. We can thus continue the processing of
        // the certificate without blocking on block synchronization.
        let header = certificate.header().clone();
        let max_age = self.config.parameters().gc_depth.saturating_sub(1);
        self.consensus_bus.sync_missing_batches().send((header, max_age)).await?;

        // return error if certificate round is too far ahead
        let highest_processed_round = self.highest_processed_round.load();
        if highest_processed_round
            + self
                .config
                .network_config()
                .sync_config()
                .max_diff_between_external_cert_round_and_highest_local_round
            < certificate.round()
        {
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await?;

            error!(target: "primary::state-sync", "processed certificate that is too new");

            return Err(CertificateError::TooNew(
                certificate.digest(),
                certificate.round(),
                highest_processed_round,
            ));
        }

        // send to certificate acceptor
        // let (sender, res) = oneshot::channel();
        // self.consensus_bus
        //     .certificate_manager()
        //     .send((vec![certificate], sender, external))
        //     .await?;

        // res.await.map_err(|_| CertificateError::CertificateAcceptorOneshot)?;

        Ok(())
    }

    /// Validate the certificate.
    ///
    /// This method validates the certificate and verifies signatures.
    fn validate_certificate(&self, certificate: Certificate) -> CertificateResult<Certificate> {
        // certificates outside gc can never be included in the DAG
        let gc_round = self.gc_round.load();

        if certificate.round() < gc_round {
            return Err(CertificateError::TooOld(
                certificate.digest(),
                certificate.round(),
                gc_round,
            ));
        }

        // validate certificate and verify signatures
        certificate.verify(self.config.committee(), self.config.worker_cache())
    }

    /// Try to accept a certificate.
    ///
    /// TODO: synchronizer::process_certificates_with_lock
    async fn try_accept_verified_certificates(
        &mut self,
        certificates: Vec<Certificate>,
        external: bool,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("primary::state-sync::try_accept_verified_certificates");
        // ensure verification state is verified
        for cert in &certificates {
            if !cert.signature_verification_state().is_verified() {
                return Err(CertificateError::UnverifiedSignature(cert.digest()));
            }
        }

        // >>>>>>
        // TODO: is this check redundant??
        // <<<<<<<<<<<<<
        // see if certificate already processed
        let digests: Vec<_> = certificates.iter().map(|cert| cert.digest()).collect();
        let exists = self.config.node_storage().certificate_store.multi_contains(digests.iter())?;
        let certificates_to_accept: Vec<_> = certificates
            .into_iter()
            .zip(exists.into_iter())
            .zip(digests.into_iter())
            .filter_map(|((cert, exist), digest)| {
                if exist {
                    debug!(target: "primary::state-sync", ?cert, ?digest, "skip processing certificate - already exists");
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .duplicate_certificates_processed
                        .inc();
                    return None;
                }
                Some((digest, cert))
            })
            .collect();

        // return early if all certificates already processed
        if certificates_to_accept.is_empty() {
            return Ok(());
        }

        for (digest, certificate) in certificates_to_accept {
            // check if this certificate is currently pending already and skip further validation
            if external {
                if self.check_pending_status(digest).await? {
                    trace!(target: "primary::synchronizer", "Certificate {digest:?} is still suspended. Skip processing.");
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_suspended
                        .with_label_values(&["dedup_locked"])
                        .inc();
                    return Err(CertificateError::Pending(digest));
                }
            }

            // check parents are either accounted for or garbage collected
            if certificate.round() > self.gc_round.load() + 1 {
                let missing_parents = self.get_missing_parents(&certificate).await?;
                if !missing_parents.is_empty() {
                    // forward to pending certificate manager
                    self.consensus_bus
                        .pending_cert_commands()
                        .send(PendingCertCommand::MissingParents { certificate, missing_parents })
                        .await?;

                    return Err(CertificateError::Pending(digest));
                }
            }

            // no missing parents - update pending state and try to process an ready certs
            // let unlocked_pending_certs =
            //     self.process_accepted_certificate(certificate.round(), digest)?;
            // self.consensus_bus
            //     .pending_cert_commands()
            //     .send(PendingCertCommand::ProcessVerifiedCertificate {
            //         certificate: certificate.clone(),
            //     })
            //     .await?;

            // accept in causal order
            self.try_accept_certificate(certificate, digest).await?;
            // for ready in unlocked_pending_certs {
            //     let ready_digest = ready.digest();
            //     self.try_accept_certificate(ready, ready_digest).await?;
            // }
        }

        // // update metrics
        // self.consensus_bus
        //     .primary_metrics()
        //     .node_metrics
        //     .certificates_currently_suspended
        //     .set(self.pending.len() as i64);

        Ok(())
    }

    /// Forward certificate and digest to the `CertificateAcceptor` task.
    async fn try_accept_certificate(
        &self,
        certificate: Certificate,
        digest: CertificateDigest,
    ) -> CertificateResult<()> {
        let (reply, result) = oneshot::channel();
        self.consensus_bus.verified_certificates().send((certificate, digest, reply)).await?;
        result.await.map_err(|_| CertificateError::CertificateAcceptorOneshot)?
    }

    /// Send an update to the `PendingCertificateManager` to update pending state.
    ///
    /// This method returns a collection of unlocked certificates to accept.

    /// Check that certificate's parents are in storage. Returns the digests of any parents that are missing.
    async fn get_missing_parents(
        &self,
        certificate: &Certificate,
    ) -> CertificateResult<HashSet<CertificateDigest>> {
        let _scope = monitored_scope("primary::state-sync::get_missing_parents");

        // handle genesis cert
        if certificate.round() == 1 {
            for digest in certificate.header().parents() {
                if !self.genesis.contains_key(digest) {
                    return Err(CertificateError::from(HeaderError::InvalidGenesisParent(*digest)));
                }
            }
            return Ok(HashSet::with_capacity(0));
        }

        let existence = self
            .config
            .node_storage()
            .certificate_store
            .multi_contains(certificate.header().parents().iter())?;

        let missing_parents: HashSet<_> = certificate
            .header()
            .parents()
            .iter()
            .zip(existence.iter())
            .filter(|(_, exists)| !*exists)
            .map(|(digest, _)| *digest)
            .collect();

        if !missing_parents.is_empty() {
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await?;
        }

        Ok(missing_parents)
    }

    /// Helper method to query the pending status of a digest.
    async fn check_pending_status(&self, digest: CertificateDigest) -> CertificateResult<bool> {
        // self.consensus_bus.
        todo!()
    }
}
