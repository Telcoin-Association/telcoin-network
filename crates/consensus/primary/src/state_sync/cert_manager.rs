//! Process standalone validated certificates.
//!
//! This module is responsible for checking certificate parents, managing pending certificates, and accepting certificates that become unlocked.

use super::{pending_cert_manager::PendingCertificateManager, AtomicRound};
use crate::{
    aggregators::CertificatesAggregatorManager, certificate_fetcher::CertificateFetcherCommand,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use fastcrypto::hash::Hash as _;
use std::collections::{HashMap, HashSet, VecDeque};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    error::{CertificateError, CertificateResult, HeaderError},
    Certificate, CertificateDigest, Round, TnReceiver as _, TnSender as _,
};
use tokio::sync::oneshot;
use tracing::{debug, error, warn};

/// Process validated certificates.
///
/// Long-running task to anage pending certificate requests and accept verified certificates.
#[derive(Debug)]
pub struct CertificateManager<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// State for pending certificate.
    pending: PendingCertificateManager<DB>,
    /// Collection of parents to advance the round.
    parents: CertificatesAggregatorManager,
    /// Genesis digests and contents.
    genesis: HashMap<CertificateDigest, Certificate>,
    /// Highest garbage collection round.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Highest round of verfied certificate that has been received.
    highest_received_round: AtomicRound,
}

impl<DB> CertificateManager<DB>
where
    DB: Database,
{
    /// TODO: delete this - only copied to review easily
    /// Validate certificate.
    // note: this should not need mut reference to self - only validate cert
    pub fn validate_certificate(&self) -> CertificateResult<()> {
        // validate certificate standalone and forward to CertificateManager
        // - try_accept_certificate
        // - accept_own_certificate
        //
        // synchronizer::process_certificate_internal
        // - check node storage for certificate already exists
        // + ignore pending state -> let next step do this
        // - sanitize certificate
        // + ignore sync batches request (L1140) - duplicate from PrimaryNetwork
        //      - confirm this is duplicate and remove from PrimaryNetwork handler
        // - sync ancestors if too new? Or let pending do this?
        //      - confirm certificate fetcher command is redundant here
        // - forward to certificate manager to check for pending
        //      - return/await oneshot reply
        todo!()
    }

    /// Process validated certificate.
    // note: certs should also be verified by now!
    //
    //
    // from synchronizer::process_certificate_internal:
    // - immediately check if certificate is already pending and return error to caller through oneshot
    //
    // from synchronizer::accept_certificate
    // - check every cert verification state
    //
    // from synchronizer::process_certificates_with_lock
    // + need to check db again for certificate?
    //   - I don't think so bc checked in verification stage
    //   - this should be the only task to manage certificate acceptance
    //   - as long as every cert request goes through here, we don't need to re-check the DB
    //
    // for each cert:
    // - check if certificate is already pending
    // - ensure within gc round
    // - check for missing parents
    // - accept cert and accept_children in that order

    async fn process_verified_certificates(
        &mut self,
        certs: Vec<Certificate>,
    ) -> CertificateResult<()> {
        // process collection of certificates
        //
        // these can be single, fetched from certificate fetcher or unlocked pending
        for cert in certs {
            let digest = cert.digest();

            // check pending status
            if self.pending.is_pending(&digest) {
                // metrics
                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .certificates_suspended
                    .with_label_values(&["dedup_locked"])
                    .inc();

                return Err(CertificateError::Pending(digest));
            }

            // ensure no missing parents (either pending or garbage collected)
            // check parents are either accounted for or garbage collected
            if cert.round() > self.gc_round.load() + 1 {
                let missing_parents = self.get_missing_parents(&cert).await?;
                if !missing_parents.is_empty() {
                    self.pending.insert_pending(cert, missing_parents)?;
                    return Err(CertificateError::Pending(digest));
                }
            }

            // no missing parents - update pending state and
            let mut unlocked = self.pending.update_pending(cert.round(), digest)?;
            // append cert and process all certs in causal order
            unlocked.push_front(cert);
            self.accept_verified_certificates(unlocked).await?;
        }

        Ok(())
    }

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

        // check storage
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

        // send request to start fetching parents
        if !missing_parents.is_empty() {
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await?;
        }

        Ok(missing_parents)
    }

    /// Try to accept the verified certificate.
    ///
    /// The certificate's state must be verified. This method writes to storage and returns the result to caller.
    // synchronizer::accept_certificate_internal
    async fn accept_verified_certificates(
        &self,
        certificates: VecDeque<Certificate>,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("primary::state-sync::accept_certificate");
        debug!(target: "primary::state-sync", ?certificates, "accepting {:?} certificates", certificates.len());
        // TODO: ensure this isn't necessary after certificates become unlocked from pending
        // or through `fetch certificates`
        //
        // check verification status
        // if !certificates.signature_verification_state().is_verified() {
        //     return Err(CertificateError::UnverifiedSignature(digest));
        // }

        // write certificates to storage
        self.config.node_storage().certificate_store.write_all(certificates.clone())?;

        for cert in certificates.into_iter() {
            // Update metrics for accepted certificates.
            let highest_processed_round =
                self.highest_processed_round.fetch_max(cert.round()).max(cert.round());
            let certificate_source =
                if self.config.authority().id().eq(&cert.origin()) { "own" } else { "other" };
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

            //
            // NOTE: these next two steps must complete or else shutdown
            //
            // TODO: check error comments are consistent with synchronizer comments

            // append parent for round
            self.parents
                .append_certificate(cert.clone(), self.config.committee())
                .await
                .inspect_err(|e| {
                    error!(target: "primary::state-sync", ?e, "failed to append cert");
                })
                .map_err(|_| CertificateError::FatalAppendParent)?;

            // send to consensus for processing into the DAG
            self.consensus_bus.new_certificates().send(cert).await.inspect_err(|e| {
                error!(target: "primary::state-sync", ?e, "failed to forward accepted certificate to consensus");
            }).map_err(|_| CertificateError::FatalForwardAcceptedCertificate)?;
        }

        Ok(())
    }

    // listen for verified certificate
    // check for pending parents during vote requests
    // garbage collect
    pub async fn run(mut self) -> CertificateResult<()> {
        // TODO: use this instead of tokio mutex. tokio::select! for shutdown or command on mpsc receiver
        // - receive gc updates
        // - receive certificates for pending
        // - state-sync::process_certificate_with_lock
        // - try accept susupended parents
        //

        let shutdown_rx = self.config.shutdown().subscribe();
        let mut certificate_manager_rx = self.consensus_bus.certificate_manager().subscribe();

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
                                Err(CertificateError::FatalAppendParent)
                                | Err(CertificateError::FatalForwardAcceptedCertificate) => {
                                    error!(target: "primary::state-sync", ?result, "fatal error. shutting down...");
                                    return result;
                                }
                                // otherwise return result to caller
                                _ => {
                                    let _ = reply.send(result);
                                }
                            }
                        }
                        _ => (),
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

/// Commands for the [CertficateManagerCommand].
#[derive(Debug)]
pub enum CertificateManagerCommand {
    /// Message from CertificateValidator.
    ProcessVerifiedCertificates {
        /// The certificate that was verified.
        ///
        /// Try to accept this certificate. If it has missing parents, track the certificate as pending and return an error.
        certificates: Vec<Certificate>,
        /// Return the result to the certificate validator.
        reply: oneshot::Sender<CertificateResult<()>>,
    },
    /// Process new garbage collection round.
    NewGCRound {
        /// The latest garbage collection round.
        round: Round,
        /// Reply to acknowledge receipt?
        reply: oneshot::Sender<()>,
    },
    // /// Check if a digest is pending.
    // CheckPendingStatus {
    //     /// Digest
    //     digest: CertificateDigest,
    //     /// Bool indicating if the certificate is currently pending.
    //     reply: oneshot::Sender<bool>,
    // },
    /// Filter certificate digests that are not in local storage.
    ///
    /// Remove digests that are already tracked by `Pending`.
    /// This is used to vote on headers.
    FilterUnkownDigests {
        unknown: Box<Vec<CertificateDigest>>,
        reply: oneshot::Sender<Box<Vec<CertificateDigest>>>,
    },
}
