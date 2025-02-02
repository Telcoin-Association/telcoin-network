//! Final check for accepting certificates.

use super::AtomicRound;
use crate::{aggregators::CertificatesAggregatorManager, ConsensusBus};
use consensus_metrics::monitored_scope;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    error::{CertificateError, CertificateResult},
    Certificate, CertificateDigest, TnReceiver as _, TnSender,
};
use tracing::{debug, warn};

/// The long-running task that processes approved certificates.
pub struct CertificateAcceptor<DB> {
    /// Highest garbage collection round.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Certificates aggregator to manage parents for each round.
    parents: CertificatesAggregatorManager,
    /// Consensus config.
    config: ConsensusConfig<DB>,
    /// Consensus bus.
    consensus_bus: ConsensusBus,
}

impl<DB> CertificateAcceptor<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
        parents: CertificatesAggregatorManager,
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
    ) -> Self {
        Self { gc_round, highest_processed_round, parents, config, consensus_bus }
    }

    /// Try to accept the verified certificate.
    ///
    /// The certificate's state must be verified. This method writes to storage and returns the result to caller.
    // synchronizer::accept_certificate_internal
    async fn try_accept_verified_certificate(
        &self,
        certificate: Certificate,
        digest: CertificateDigest,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("primary::state-sync::accept_certificate");
        debug!(target: "primary::state-sync", ?certificate, "accepting certificate");

        // validate that certificates are accepted in causal order
        //
        // NOTE: this should be relatively cheap because of certificate store caching
        if certificate.round() > self.gc_round.load() + 1 {
            // final validation
            //
            // TODO: is this redundant?
            // // >>>>>>>>>>>>>>>>>>>>>
            let existence = self
                .config
                .node_storage()
                .certificate_store
                .multi_contains(certificate.header().parents().iter())?;

            for (digest, exists) in certificate.header().parents().iter().zip(existence.iter()) {
                if !*exists {
                    return Err(CertificateError::MissingParent(*digest));
                }
            }
        }

        // TODO: is this redundant?
        //
        // check verification status
        if !certificate.signature_verification_state().is_verified() {
            return Err(CertificateError::UnverifiedSignature(digest));
        }

        // write certificate to storage
        self.config.node_storage().certificate_store.write(certificate.clone())?;

        // Update metrics for accepted certificates.
        let highest_processed_round =
            self.highest_processed_round.fetch_max(certificate.round()).max(certificate.round());
        let certificate_source =
            if self.config.authority().id().eq(&certificate.origin()) { "own" } else { "other" };
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

        // append parent for round
        self.parents
            .append_certificate(certificate.clone(), self.config.committee())
            .await
            .inspect_err(|e| {
                warn!(target: "primary::state-sync", ?e, ?digest, "failed to append certificate");
            })?;

        self.consensus_bus.new_certificates().send(certificate).await.inspect_err(|e| {
            warn!(target: "primary::state-sync", ?e, ?digest, "failed to forward accepted certificate to consensus");
        })?;

        Ok(())
    }

    pub async fn run(self) -> CertificateResult<()> {
        let shutdown_rx = self.config.shutdown().subscribe();
        let mut rx_verified_certificates = self.consensus_bus.verified_certificates().subscribe();

        loop {
            tokio::select! {
                // receive a verified certificate
                Some((cert, digest, reply)) = rx_verified_certificates.recv() => {
                    let res = self.try_accept_verified_certificate(cert, digest).await;
                    reply.send(res).map_err(|_| CertificateError::CertificateAcceptorOneshot)?;
                }

                // shutdown signal
                _ = &shutdown_rx => {
                    return Ok(());
                }
            }
        }
    }
}
