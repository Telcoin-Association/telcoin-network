//! Validate certificates received from peers.

use super::AtomicRound;
use crate::{
    certificate_fetcher::CertificateFetcherCommand, state_sync::CertificateManagerCommand,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use fastcrypto::hash::Hash as _;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    error::{CertificateError, CertificateResult},
    Certificate, CertificateDigest, Round, SignatureVerificationState, TnSender as _,
};
use tokio::sync::oneshot;
use tracing::{debug, error, trace};

/// Process unverified headers and certificates.
#[derive(Debug, Clone)]
pub struct CertificateValidator<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// Genesis digests and contents.
    genesis: HashMap<CertificateDigest, Certificate>,
    /// Highest garbage collection round.
    ///
    /// This is managed by GarbageCollector and shared with CertificateValidator.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Highest round of verfied certificate that has been received.
    highest_received_round: AtomicRound,
}

impl<DB> CertificateValidator<DB>
where
    DB: Database,
{
    /// Process a certificate produced by the this node.
    pub async fn process_own_certificate(&self, certificate: Certificate) -> CertificateResult<()> {
        self.process_certificate(certificate, false).await
    }

    /// Process a certificate received from a peer.
    pub async fn process_peer_certificate(
        &self,
        certificate: Certificate,
    ) -> CertificateResult<()> {
        self.process_certificate(certificate, true).await
    }

    /// Validate certificate.
    async fn process_certificate(
        &self,
        mut certificate: Certificate,
        external: bool,
    ) -> CertificateResult<()> {
        // validate certificate standalone and forward to CertificateManager
        // - try_accept_certificate
        // - accept_own_certificate
        //
        // synchronizer::process_certificate_internal
        // - check node storage for certificate already exists
        //      - make this a separate method so vote can call it too
        //          - synchronizer::get_unknown_parent_digests
        //      - return missing
        // + ignore pending state -> let next step do this
        // - sanitize certificate
        //
        //
        //
        //
        // TODO STILLLLLL!!!!!!!!!
        // + ignore sync batches request (L1140) - duplicate from PrimaryNetwork
        //      - confirm this is duplicate and remove from PrimaryNetwork handler
        //      - NOTE: this is never subscribed????
        //
        //
        //
        //
        // - sync ancestors if too new? Or let pending do this?
        //      - confirm certificate fetcher command is redundant here
        // - forward to certificate manager to check for pending
        //      - return/await oneshot reply

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
            // update signature verification
            certificate = self.validate_and_verify(certificate)?;
        }

        // update metrics
        debug!(target: "primary::state-sync", round=certificate.round(), ?certificate, "processing certificate");

        let certificate_source =
            if self.config.authority().id().eq(&certificate.origin()) { "own" } else { "other" };
        self.forward_verified_certs(certificate_source, certificate.round(), vec![certificate])
            .await
    }

    /// Validate and verify the certificate.
    ///
    /// This method validates the certificate and verifies signatures.
    fn validate_and_verify(&self, certificate: Certificate) -> CertificateResult<Certificate> {
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
        // TODO: rename this method too
        certificate.verify(self.config.committee(), self.config.worker_cache())
    }

    /// Update metrics and send to Certificate Manager for final processing.
    async fn forward_verified_certs(
        &self,
        certificate_source: &str,
        highest_round: Round,
        certificates: Vec<Certificate>,
    ) -> CertificateResult<()> {
        let highest_received_round =
            self.highest_received_round.fetch_max(highest_round).max(highest_round);

        // highest received round metric
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
        let minimal_round_for_parents = highest_received_round.saturating_sub(1);
        self.consensus_bus.parents().send((vec![], minimal_round_for_parents)).await?;

        // return error if certificate round is too far ahead
        //
        // trigger certificate fetching
        let highest_processed_round = self.highest_processed_round.load();

        for cert in &certificates {
            if highest_processed_round
                + self
                    .config
                    .network_config()
                    .sync_config()
                    .max_diff_between_external_cert_round_and_highest_local_round
                < cert.round()
            {
                self.consensus_bus
                    .certificate_fetcher()
                    .send(CertificateFetcherCommand::Ancestors(cert.clone()))
                    .await?;

                error!(target: "primary::state-sync", "processed certificate that is too new");

                return Err(CertificateError::TooNew(
                    cert.digest(),
                    cert.round(),
                    highest_processed_round,
                ));
            }
        }

        // forward to certificate manager to check for pending parents and accept
        let (reply, res) = oneshot::channel();
        self.consensus_bus
            .certificate_manager()
            .send(CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply })
            .await?;

        // await response from certificate manager
        res.await.map_err(|_| CertificateError::CertificateManagerOneshot)?
    }

    //
    //=== Parallel verification methods
    //

    /// Process a large collection of certificates downloaded from peers.
    ///
    /// This partitions the collection to verify certificates in chunks.
    pub async fn process_fetched_certificates_in_parallel(
        &self,
        certificates: Vec<Certificate>,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("primary::cert_validator");
        let certificates = self.verify_collection(certificates).await?;

        // update metrics
        let highest_round = certificates.iter().map(|c| c.round()).max().unwrap_or(0);
        self.forward_verified_certs("other", highest_round, certificates).await
    }

    /// Main method to subdivide certificates into groups and verify based on causal relationship.
    async fn verify_collection(
        &self,
        mut certificates: Vec<Certificate>,
    ) -> CertificateResult<Vec<Certificate>> {
        // Early return for empty input
        if certificates.is_empty() {
            return Ok(certificates);
        }

        // Classify certificates for verification strategy
        let certs_for_verification =
            self.classify_certificates_for_verification(&mut certificates)?;

        // Verify certificates that need direct verification
        let verified_certs = self.verify_certificate_chunk(certs_for_verification).await?;

        // Update metrics about verification types
        self.update_fetch_metrics(&certificates, verified_certs.len());

        // Update the original certificates with verified versions
        for (idx, cert) in verified_certs {
            certificates[idx] = cert;
        }

        Ok(certificates)
    }

    /// Determines which certificates in a chunk need direct verification versus
    /// those that can be verified indirectly through their relationships with other certificates.
    fn classify_certificates_for_verification(
        &self,
        certificates: &mut Vec<Certificate>,
    ) -> CertificateResult<Vec<(usize, Certificate)>> {
        // Build certificate relationship maps to identify leaf certificates
        let mut all_digests = HashSet::new();
        let mut all_parents = HashSet::new();
        for cert in certificates.iter() {
            all_digests.insert(cert.digest());
            all_parents.extend(cert.header().parents().iter());
        }

        // Identify certificates requiring direct verification:
        // 1. Leaf certificates that no other certificate depends on
        // 2. Certificates at periodic round intervals for security
        let mut direct_verification_certs = Vec::new();
        for (idx, cert) in certificates.iter_mut().enumerate() {
            if self.requires_direct_verification(cert, &all_parents) {
                direct_verification_certs.push((idx, cert.clone()));
                continue;
            }
            self.mark_verified_indirectly(cert)?;
        }
        Ok(direct_verification_certs)
    }

    /// Determines if a certificate requires direct verification.
    ///
    /// Certificates require direct verification if no other certificates depend on them (ie - not a parent). This method also periodically verifies certificates between intevals if the round % 50 is 0.
    fn requires_direct_verification(
        &self,
        cert: &Certificate,
        all_parents: &HashSet<CertificateDigest>,
    ) -> bool {
        !all_parents.contains(&cert.digest())
            || cert.header().round()
                % self.config.network_config().sync_config().certificate_verification_round_interval
                == 0
    }

    /// Marks a certificate as indirectly verified.
    ///
    /// These chunks are verified through parents being verified.
    fn mark_verified_indirectly(&self, cert: &mut Certificate) -> CertificateResult<()> {
        cert.set_signature_verification_state(SignatureVerificationState::VerifiedIndirectly(
            cert.aggregated_signature()
                .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?
                .clone(),
        ));

        Ok(())
    }

    /// Verifies a chunk of certificates in parallel.
    async fn verify_certificate_chunk(
        &self,
        certs_for_verification: Vec<(usize, Certificate)>,
    ) -> CertificateResult<Vec<(usize, Certificate)>> {
        let verify_tasks: Vec<_> = certs_for_verification
            .chunks(self.config.network_config().sync_config().certificate_verification_chunk_size)
            .map(|chunk| self.spawn_verification_task(chunk.to_vec()))
            .collect();

        let mut verified_certs = Vec::new();
        for task in verify_tasks {
            let group_result = task.await.map_err(|e| {
                error!(target: "primary::state-sync", ?e, "group verify certs task failed");
                CertificateError::JoinError
            })??;
            verified_certs.extend(group_result);
        }
        Ok(verified_certs)
    }

    /// Spawns a single verification task for a chunk of certificates
    fn spawn_verification_task(
        &self,
        certs: Vec<(usize, Certificate)>,
    ) -> tokio::task::JoinHandle<CertificateResult<Vec<(usize, Certificate)>>> {
        let validator = self.clone();
        tokio::task::spawn_blocking(move || {
            let now = Instant::now();
            let mut sanitized_certs = Vec::new();

            for (idx, cert) in certs {
                sanitized_certs.push((idx, validator.validate_and_verify(cert)?))
            }

            // Update metrics for verification time
            validator
                .consensus_bus
                .primary_metrics()
                .node_metrics
                .certificate_fetcher_total_verification_us
                .inc_by(now.elapsed().as_micros() as u64);

            Ok(sanitized_certs)
        })
    }

    /// Update metrics for fetched certificates.
    fn update_fetch_metrics(&self, certificates: &[Certificate], direct_count: usize) {
        let total_count = certificates.len() as u64;
        let direct_count = direct_count as u64;

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .fetched_certificates_verified_directly
            .inc_by(direct_count);

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .fetched_certificates_verified_indirectly
            .inc_by(total_count.saturating_sub(direct_count));
    }
}
