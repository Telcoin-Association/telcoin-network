//! Validate certificates received from peers.

use super::AtomicRound;
use crate::{
    certificate_fetcher::CertificateFetcherCommand, state_sync::PendingCertCommand, ConsensusBus,
};
use fastcrypto::hash::Hash as _;
use std::collections::HashMap;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    error::{CertificateError, CertificateResult},
    Certificate, CertificateDigest, TnSender as _,
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
    // /// Collection of parents to advance the round.
    // parents: CertificatesAggregatorManager,
    /// Genesis digests and contents.
    genesis: HashMap<CertificateDigest, Certificate>,
    /// Highest garbage collection round.
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
    /// Validate certificate.
    // note: this should not need mut reference to self - only validate cert
    //
    // api: process_own / process_external/peer
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
        // + ignore sync batches request (L1140) - duplicate from PrimaryNetwork
        //      - confirm this is duplicate and remove from PrimaryNetwork handler
        //      - NOTE: this is never subscribed????
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

        // return error if certificate round is too far ahead
        //
        // trigger certificate fetching
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

        // forward to certificate manager to check for pending parents and accept
        let (reply, res) = oneshot::channel();
        self.consensus_bus
            .pending_cert_commands()
            .send(PendingCertCommand::ProcessVerifiedCertificate { certificate, reply })
            .await?;

        // TODO: rename this error
        res.await.map_err(|_| CertificateError::CertificateAcceptorOneshot)?
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
}
