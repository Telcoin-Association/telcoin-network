//! Validate certificates received from peers.

use super::{cert_manager::CertificateManager, AtomicRound, HeaderValidator};
use crate::{
    certificate_fetcher::CertificateFetcherCommand,
    error::{CertManagerError, CertManagerResult},
    state_sync::CertificateManagerCommand,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use std::{collections::HashSet, sync::Arc, time::Instant};
use tn_config::ConsensusConfig;
use tn_storage::CertificateStore;
use tn_types::{
    error::CertificateError, Certificate, CertificateDigest, Database, Hash as _, Round,
    SignatureVerificationState, TnSender as _,
};
use tokio::sync::oneshot;
use tracing::{debug, error, trace};

#[cfg(test)]
#[path = "../tests/cert_validator_tests.rs"]
mod cert_validator_tests;

pub fn certificate_source<DB: Database>(
    config: &ConsensusConfig<DB>,
    certificate: &Certificate,
) -> &'static str {
    if let Some(authority_id) = config.authority_id() {
        if authority_id.eq(certificate.origin()) {
            "own"
        } else {
            "other"
        }
    } else {
        "other"
    }
}

/// Process unverified headers and certificates.
#[derive(Debug, Clone)]
pub(super) struct CertificateValidator<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
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
    /// Create a new instance of Self.
    pub(super) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
        highest_received_round: AtomicRound,
    ) -> Self {
        Self { consensus_bus, config, gc_round, highest_processed_round, highest_received_round }
    }

    /// Convenience method for obtaining a new [CertificateManager].
    ///
    /// This is useful so the primary can handle new/spawn methods separately.
    /// The cert manager only needs to run during `spawn`.
    pub(super) fn new_cert_manager(&self) -> CertificateManager<DB> {
        CertificateManager::new(
            self.config.clone(),
            self.consensus_bus.clone(),
            self.gc_round.clone(),
            self.highest_processed_round.clone(),
        )
    }

    /// Process a certificate produced by the this node.
    pub(super) async fn process_own_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.process_certificate(certificate, false).await
    }

    /// Process a certificate received from a peer.
    pub(super) async fn process_peer_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.process_certificate(certificate, true).await
    }

    /// Validate certificate.
    async fn process_certificate(
        &self,
        mut certificate: Certificate,
        external: bool,
    ) -> CertManagerResult<()> {
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
        // - ignore sync batches request (L1140) - duplicate from PrimaryNetwork
        //      - confirm this is duplicate and remove from PrimaryNetwork handler
        //      - NOTE: this is never subscribed????
        // - sync ancestors if too new? Or let pending do this?
        //      - confirm certificate fetcher command is redundant here
        // - forward to certificate manager to check for pending
        //      - return/await oneshot reply

        // see if certificate already processed
        let digest = certificate.digest();
        if self.config.node_storage().contains(&digest)? {
            trace!(target: "primary::cert_validator", "Certificate {digest:?} has already been processed. Skip processing.");
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
        debug!(target: "primary::cert_validator", round=certificate.round(), ?certificate, "processing certificate");

        let certificate_source = certificate_source(&self.config, &certificate);
        self.forward_verified_certs(certificate_source, certificate.round(), vec![certificate])
            .await
    }

    /// Validate and verify the certificate.
    ///
    /// This method validates the certificate and verifies signatures.
    fn validate_and_verify(&self, certificate: Certificate) -> CertManagerResult<Certificate> {
        // certificates outside gc can never be included in the DAG
        let gc_round = self.gc_round.load();

        if certificate.round() < gc_round {
            return Err(CertificateError::TooOld(
                certificate.digest(),
                certificate.round(),
                gc_round,
            )
            .into());
        }

        // validate certificate and verify signatures
        // TODO: rename this method too
        let verified_cert =
            certificate.verify(self.config.committee(), self.config.worker_cache())?;
        Ok(verified_cert)
    }

    /// Update metrics and send to Certificate Manager for final processing.
    async fn forward_verified_certs(
        &self,
        certificate_source: &str,
        highest_round: Round,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<()> {
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
            // Initiate asynchronous batch downloads for any payloads referenced in this certificate
            // that are not yet available locally. This step is critical for maintaining data
            // availability across the network.
            //
            // The certificate's existence proves these batches must be available somewhere in the
            // network - the certificate could only have been created after enough validators had
            // access to examine and vote on these batches. This availability guarantee allows the
            // protocol to proceed with certificate processing immediately, without waiting for the
            // batch downloads to complete.
            //
            // The max_age parameter, derived from the garbage collection depth, ensures the
            // protocol only attempts to synchronize reasonably recent batches that
            // haven't been cleaned up by garbage collection on other nodes.
            let header = cert.header().clone();
            let max_age = self.config.parameters().gc_depth.saturating_sub(1);
            let config = self.config.clone();
            let bus = self.consensus_bus.clone();

            // spawn task to synchronize batches for this header
            //
            // NOTE: this should be okay bc header is already certified by quorum of signatures
            tokio::task::spawn(async move {
                let sync_header = HeaderValidator::new(config, bus);
                let res = sync_header.sync_header_batches(&header, true, max_age).await;
                if let Err(e) = res {
                    error!(target: "primary::cert_validator", ?e, ?header, ?max_age, "error syncing batches for certified header");
                }
            });

            // trigger certificate fetching if cert is too far ahead of this node
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
                    .send(CertificateFetcherCommand::Ancestors(Arc::new(cert.clone())))
                    .await?;

                error!(target: "primary::cert_validator", "processed certificate that is too new");

                return Err(CertificateError::TooNew(
                    cert.digest(),
                    cert.round(),
                    highest_processed_round,
                )
                .into());
            }
        }

        // forward to certificate manager to check for pending parents and accept
        let (reply, res) = oneshot::channel();
        self.consensus_bus
            .certificate_manager()
            .send(CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply })
            .await?;

        // await response from certificate manager
        res.await.map_err(|_| CertManagerError::CertificateManagerOneshot)?
    }

    //
    //=== Parallel verification methods
    //

    /// Process a large collection of certificates downloaded from peers.
    ///
    /// This partitions the collection to verify certificates in chunks.
    pub(super) async fn process_fetched_certificates_in_parallel(
        &self,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<()> {
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
    ) -> CertManagerResult<Vec<Certificate>> {
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
        certificates: &mut [Certificate],
    ) -> CertManagerResult<Vec<(usize, Certificate)>> {
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
    /// Certificates require direct verification if no other certificates depend on them (ie - not a
    /// parent). This method also periodically verifies certificates between intevals if the round %
    /// 50 is 0.
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
    fn mark_verified_indirectly(&self, cert: &mut Certificate) -> CertManagerResult<()> {
        cert.set_signature_verification_state(SignatureVerificationState::VerifiedIndirectly(
            cert.aggregated_signature()
                .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?,
        ));

        Ok(())
    }

    /// Verifies a chunk of certificates in parallel.
    async fn verify_certificate_chunk(
        &self,
        certs_for_verification: Vec<(usize, Certificate)>,
    ) -> CertManagerResult<Vec<(usize, Certificate)>> {
        let verify_tasks: Vec<_> = certs_for_verification
            .chunks(self.config.network_config().sync_config().certificate_verification_chunk_size)
            .map(|chunk| self.spawn_verification_task(chunk.to_vec()))
            .collect();

        let mut verified_certs = Vec::new();
        for task in verify_tasks {
            let group_result = task.await.map_err(|e| {
                error!(target: "primary::cert_validator", ?e, "group verify certs task failed");
                CertManagerError::JoinError
            })??;
            verified_certs.extend(group_result);
        }
        Ok(verified_certs)
    }

    /// Spawns a single verification task for a chunk of certificates
    fn spawn_verification_task(
        &self,
        certs: Vec<(usize, Certificate)>,
    ) -> tokio::task::JoinHandle<CertManagerResult<Vec<(usize, Certificate)>>> {
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
