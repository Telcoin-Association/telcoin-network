//! Manage pending certificates.
//!
//! Pending certificates are waiting to be accepted due to missing parents.
//! This mod manages and tracks pending certificates for rounds of consensus.

use crate::ConsensusBus;
use fastcrypto::hash::Hash as _;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    error::{CertificateError, CertificateResult, HeaderError},
    Certificate, CertificateDigest, Noticer, Round, TnReceiver, TnSender as _,
};
use tokio::sync::oneshot;
use tracing::{debug, error, trace, warn};

/// A certificate that is missing parents and pending approval.
#[derive(Debug, Clone)]
struct PendingCertificate {
    /// The pending certificate.
    certificate: Certificate,
    /// The certificate's missing parents that must be retrieved before the pending certificate is accepted.
    missing_parent_digests: HashSet<CertificateDigest>,
}

impl PendingCertificate {
    /// Create a new instance of Self.
    fn new(certificate: Certificate, missing_parents: HashSet<CertificateDigest>) -> Self {
        Self { certificate, missing_parent_digests: missing_parents }
    }

    /// Helper method if pending certificate should be validated again after all missing parents obtained.
    fn is_ready(&self) -> bool {
        self.missing_parent_digests.is_empty()
    }

    /// Remove a missing parent, either due to gc round or because missing parent was successfully retrieved.
    fn remove_parent(&mut self, parent_digest: &CertificateDigest) {
        self.missing_parent_digests.remove(parent_digest);
    }
}

/// Manages certificate dependencies and tracks their readiness status.
///
/// Certificates are only accepted after their parents. If a certificate's parents are missing,
/// the certificate is kept here until its parents become available.
#[derive(Debug)]
pub struct PendingCertificateManager<DB> {
    /// Each certificate entry tracks both the certificate itself and its dependency state
    ///
    /// Pending certificates that cannot be accepted yet.
    pending: HashMap<CertificateDigest, PendingCertificate>,
    /// Map of a parent digest to the digests of pending certificates.
    ///
    /// The keys are (round, digest) to enable garbage collection by round.
    missing_parent_to_child: BTreeMap<(Round, CertificateDigest), HashSet<CertificateDigest>>,

    /// The configuration for consensus.
    config: ConsensusConfig<DB>,

    /// Consensus channels.
    consensus_bus: ConsensusBus,
}

impl<DB> PendingCertificateManager<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(config: ConsensusConfig<DB>, consensus_bus: ConsensusBus) -> Self {
        Self {
            pending: Default::default(),
            missing_parent_to_child: Default::default(),
            config,
            consensus_bus,
        }
    }

    /// Attempts to insert a new pending certificate.
    pub fn insert_pending(
        &mut self,
        certificate: Certificate,
        missing_parents: HashSet<CertificateDigest>,
    ) -> CertificateResult<()> {
        let digest = certificate.digest();
        let parent_round = certificate.round() - 1;
        debug!(target: "primary::state-sync::pending", ?digest, "Processing certificate with missing parents");

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_suspended
            .with_label_values(&["missing_parents"])
            .inc();

        // track pending certificate
        let pending = PendingCertificate::new(certificate, missing_parents.clone());
        if let Some(existing) = self.pending.insert(digest, pending) {
            if existing.missing_parent_digests != missing_parents {
                // TODO: use better error type here
                return Err(CertificateError::Suspended);
            }
        }

        // insert missing parents
        for parent in missing_parents {
            self.missing_parent_to_child.entry((parent_round, parent)).or_default().insert(digest);
        }

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_currently_suspended
            .set(self.pending.len() as i64);

        Ok(())
    }

    /// When a certificate is accepted, returns all of its children that are now ready to be verified.
    fn update_pending(
        &mut self,
        round: Round,
        digest: CertificateDigest,
    ) -> CertificateResult<Vec<Certificate>> {
        // return error if parent is still pending
        if self.pending.remove(&digest).is_some() {
            error!(target: "primary::pending-certificate", ?digest, "parent still pending");
            return Err(CertificateError::PendingCertificateNotFound(digest));
        }

        let mut ready_certificates = Vec::new();
        let mut certificates_to_process = VecDeque::new();
        certificates_to_process.push_back((round, digest));

        // Process certificates in a cascading manner
        while let Some((next_round, next_digest)) = certificates_to_process.pop_front() {
            // get pending certificates with missing parents
            let Some(pending_digests) =
                self.missing_parent_to_child.remove(&(next_round, next_digest))
            else {
                continue;
            };

            // remove missing parents from pending certs and process if ready
            for pending_digest in &pending_digests {
                // get pending cert
                let pending_cert = self
                    .pending
                    .get_mut(pending_digest)
                    .ok_or(CertificateError::PendingCertificateNotFound(*pending_digest))?;

                // remove parent
                pending_cert.missing_parent_digests.remove(&next_digest);

                // try to accept if no more missing parents
                if pending_cert.missing_parent_digests.is_empty() {
                    // remove from pending
                    let ready = self
                        .pending
                        .remove(pending_digest)
                        .ok_or(CertificateError::PendingCertificateNotFound(*pending_digest))?;

                    // update any pending certificates waiting for this certificate
                    certificates_to_process.push_back((ready.certificate.round(), *pending_digest));

                    // return this certificate as ready for verification
                    ready_certificates.push(ready.certificate);
                }
            }
        }

        Ok(ready_certificates)
    }

    /// Use the newly verified certificate to update pending state. Forward this certificate and any unlocked certificates to the CertificateAcceptor.
    async fn process_accepted_certificate(
        &mut self,
        certificate: Certificate,
    ) -> CertificateResult<()> {
        let digest = certificate.digest();
        let ready = self.update_pending(certificate.round(), digest)?;

        // try accept in causal order
        self.try_accept_certificate(certificate, digest).await?;
        for cert in ready {
            let digest = cert.digest();
            self.try_accept_certificate(cert, digest).await?;
        }

        // update metrics
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_currently_suspended
            .set(self.pending.len() as i64);

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

    /// Performs garbage collection up to and including the specified round.
    fn garbage_collect(&mut self, gc_round: Round) -> CertificateResult<Vec<Certificate>> {
        let mut ready_certificates = Vec::new();

        // find all certificates at or below gc_round
        while let Some(((round, digest), _children)) = self
            .missing_parent_to_child
            .first_key_value()
            .map(|((round, digest), children)| ((*round, *digest), children.clone()))
        {
            if round > gc_round {
                break;
            }

            // remove this entry since it's being garbage collected
            self.missing_parent_to_child.remove(&(round, digest));

            // try to accept this certificate and any of its children that become ready
            let mut ready = self.update_pending(round, digest)?;
            ready_certificates.append(&mut ready);
        }

        Ok(ready_certificates)
    }

    /// Returns whether a certificate is being tracked
    fn is_pending(&self, digest: &CertificateDigest) -> bool {
        // self.pending.values().any(|pending| pending.missing_parent_digests.contains(digest))
        self.pending.get(digest).is_some()
    }

    /// Filter parents that are pending in place.
    fn filter_unknown_digests(&self, unknown: &mut Box<Vec<CertificateDigest>>) {
        unknown.retain(|digest| !self.pending.contains_key(digest));
    }

    /// Listen for incoming message and update pending certificate state.
    pub async fn run(mut self) -> CertificateResult<()> {
        // TODO: use this instead of tokio mutex. tokio::select! for shutdown or command on mpsc receiver
        // - receive gc updates
        // - receive certificates for pending
        // - state-sync::process_certificate_with_lock
        // - try accept susupended parents
        //

        let shutdown_rx = self.config.shutdown().subscribe();
        let mut pending_cert_commands_rx = self.consensus_bus.pending_cert_commands().subscribe();
        let mut new_unverified_cert = self.consensus_bus.accept_verified_certificates().subscribe();

        // manage pending certificate state until shutdown
        loop {
            tokio::select! {

                // update state
                Some(command) = pending_cert_commands_rx.recv() => {
                    match command {
                        PendingCertCommand::MissingParents { certificate, missing_parents } => {
                            self.insert_pending(certificate, missing_parents)?;
                        },
                        PendingCertCommand::ProcessVerifiedCertificate { certificate, reply } => {
                            let res = self.process_accepted_certificate(certificate).await;
                            let _ = reply.send(res);
                        },
                        PendingCertCommand::NewGCRound { round, reply } => {
                            let ready = self.garbage_collect(round)?;
                        }
                        PendingCertCommand::CheckPendingStatus{ digest, reply } => {
                            reply.send(self.is_pending(&digest));
                        }
                        PendingCertCommand::FilterUnkownDigests{ mut unknown, reply } => {
                            self.filter_unknown_digests(&mut unknown);
                            reply.send(unknown);
                        }
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

/// Commands for the [PendingCertificateManager].
#[derive(Debug)]
pub enum PendingCertCommand {
    /// Track a new pending certificate with missing parents.
    MissingParents {
        /// The certificate with missing parents.
        certificate: Certificate,
        /// The collection of missing parent digests.
        missing_parents: HashSet<CertificateDigest>,
    },
    /// Parent received. Try to accept dependents.
    ProcessVerifiedCertificate {
        /// The certificate that was verified.
        ///
        /// Compare this certificate against any pending certificates and process certificates that become unblocked.
        certificate: Certificate,
        /// Return the result to the certificate validator.
        reply: oneshot::Sender<CertificateResult<()>>,
    },
    /// Process new garbage collection round.
    NewGCRound { round: Round, reply: oneshot::Sender<()> },
    /// Check if a digest is pending.
    CheckPendingStatus { digest: CertificateDigest, reply: oneshot::Sender<bool> },
    /// Filter certificate digests that are not in local storage.
    ///
    /// Remove digests that are already tracked by `Pending`.
    FilterUnkownDigests {
        unknown: Box<Vec<CertificateDigest>>,
        reply: oneshot::Sender<Box<Vec<CertificateDigest>>>,
    },
}
