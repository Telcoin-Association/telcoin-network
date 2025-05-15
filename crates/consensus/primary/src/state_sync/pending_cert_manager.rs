//! Manage pending certificates.
//!
//! Pending certificates are waiting to be accepted due to missing parents.
//! This mod manages and tracks pending certificates for rounds of consensus.

use crate::{
    error::{CertManagerError, CertManagerResult},
    ConsensusBus,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use tn_types::{Certificate, CertificateDigest, Hash as _, Round};
use tracing::debug;

/// A certificate that is missing parents and pending approval.
///
/// All pending certificates must be verified before adding.
#[derive(Debug, Clone)]
struct PendingCertificate {
    /// The pending certificate.
    certificate: Certificate,
    /// The certificate's missing parents that must be retrieved before the pending certificate is
    /// accepted.
    missing_parent_digests: HashSet<CertificateDigest>,
}

impl PendingCertificate {
    /// Create a new instance of Self.
    fn new(certificate: Certificate, missing_parents: HashSet<CertificateDigest>) -> Self {
        Self { certificate, missing_parent_digests: missing_parents }
    }
}

/// Manages certificate dependencies and tracks their readiness status.
///
/// Certificates are only accepted after their parents. If a certificate's parents are missing,
/// the certificate is kept here until its parents become available.
///
/// NOTE: all pending certificates must be verified.
#[derive(Debug)]
pub struct PendingCertificateManager {
    /// Each certificate entry tracks both the certificate itself and its dependency state
    ///
    /// Pending certificates that cannot be accepted yet.
    pending: HashMap<CertificateDigest, PendingCertificate>,
    /// Map of a the missing certificate digests and the pending certificates that are blocked by
    /// them.
    ///
    /// The keys are (round, digest) to enable garbage collection by round.
    missing_for_pending: BTreeMap<(Round, CertificateDigest), HashSet<CertificateDigest>>,
    /// Consensus channels.
    consensus_bus: ConsensusBus,
}

impl PendingCertificateManager {
    /// Create a new instance of Self.
    pub fn new(consensus_bus: ConsensusBus) -> Self {
        Self { pending: Default::default(), missing_for_pending: Default::default(), consensus_bus }
    }

    /// Attempts to insert a new pending certificate.
    ///
    /// Callers must ensure certificates are verified before inserting into pending state.
    pub fn insert_pending(
        &mut self,
        certificate: Certificate,
        missing_parents: HashSet<CertificateDigest>,
    ) -> CertManagerResult<()> {
        let digest = certificate.digest();
        let parent_round = certificate.round() - 1;
        debug!(target: "primary::pending_certs", ?digest, "Processing certificate with missing parents");

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
                return Err(CertManagerError::PendingParentsMismatch(digest));
            }
        }

        // insert missing parents
        for parent in missing_parents {
            self.missing_for_pending.entry((parent_round, parent)).or_default().insert(digest);
        }

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_currently_suspended
            .set(self.pending.len() as i64);

        Ok(())
    }

    /// When a certificate is accepted, returns all of its children that are now ready to be
    /// verified.
    // TODO: remove after tests
    // synchronizer::state::accept_children
    pub(super) fn update_pending(
        &mut self,
        round: Round,
        digest: CertificateDigest,
    ) -> CertManagerResult<VecDeque<Certificate>> {
        let mut ready_certificates = VecDeque::new();
        let mut certificates_to_process = VecDeque::new();
        certificates_to_process.push_back((round, digest));

        // Process certificates in a cascading manner
        while let Some((next_round, next_digest)) = certificates_to_process.pop_front() {
            // get pending certificates with missing parents
            let Some(pending_digests) = self.missing_for_pending.remove(&(next_round, next_digest))
            else {
                continue;
            };

            // remove missing parents from pending certs and process if ready
            for pending_digest in &pending_digests {
                // get pending cert
                let pending_cert = self
                    .pending
                    .get_mut(pending_digest)
                    .ok_or(CertManagerError::PendingCertificateNotFound(*pending_digest))?;

                // remove parent
                pending_cert.missing_parent_digests.remove(&next_digest);

                // try to accept if no more missing parents
                if pending_cert.missing_parent_digests.is_empty() {
                    // remove from pending
                    let ready = self
                        .pending
                        .remove(pending_digest)
                        .ok_or(CertManagerError::PendingCertificateNotFound(*pending_digest))?;

                    // update any pending certificates waiting for this certificate
                    certificates_to_process.push_back((ready.certificate.round(), *pending_digest));

                    // return this certificate as ready for verification
                    ready_certificates.push_back(ready.certificate);
                }
            }
        }

        Ok(ready_certificates)
    }

    /// Return the first key/value in the pending BTreeMap (sorted) that matches the gc round.
    ///
    /// This is useful for iterating through all missing certificates that are blocking pending
    /// certificates from being accepted.
    pub(super) fn next_for_gc_round(
        &mut self,
        gc_round: Round,
    ) -> Option<(Round, CertificateDigest)> {
        let (round, digest) = self
            .missing_for_pending
            .first_key_value()
            .map(|((round, digest), _children)| (*round, *digest))?;

        // check if all gc rounds are processed
        if round > gc_round {
            return None;
        }

        // remove missing parents from gc round
        //
        // NOTE: this digest is returned and will be used to update pending by caller
        if let Some(pending) = self.pending.get_mut(&digest) {
            pending.missing_parent_digests.clear();
        }

        Some((round, digest))
    }

    /// Returns whether a certificate is being tracked
    pub(super) fn is_pending(&self, digest: &CertificateDigest) -> bool {
        self.pending.contains_key(digest)
    }

    /// Returns the number of pending certificates.
    pub(super) fn num_pending(&self) -> usize {
        self.pending.len()
    }

    /// Filter parents that are pending in place.
    ///
    /// This is used when voting for headers.
    pub(super) fn filter_unknown_digests(&self, unknown: &mut Vec<CertificateDigest>) {
        unknown.retain(|digest| !self.pending.contains_key(digest));
    }
}
