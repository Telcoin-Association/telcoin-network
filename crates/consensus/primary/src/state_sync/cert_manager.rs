//! Process standalone validated certificates.
//!
//! This module is responsible for checking certificate parents, managing pending certificates, and accepting certificates that become unlocked.

use super::{pending_cert_manager::PendingCertificateManager, AtomicRound};
use crate::{
    aggregators::CertificatesAggregatorManager, state_sync::PendingCertCommand, ConsensusBus,
};
use std::collections::HashMap;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{
    error::CertificateResult, Certificate, CertificateDigest, TnReceiver as _, TnSender as _,
};

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
    fn process_valid_verified_certificates(&mut self) -> CertificateResult<()> {
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
        todo!()

        // listen for verified certificate
        // check for pending parents during vote requests
        // garbage collect
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
                            self.pending.insert_pending(certificate, missing_parents)?;
                        },
                        PendingCertCommand::ProcessVerifiedCertificate { certificate, reply } => {
                            let res = self.pending.process_accepted_certificate(certificate).await;
                            let _ = reply.send(res);
                        },
                        PendingCertCommand::NewGCRound { round, reply } => {
                            let ready = self.pending.garbage_collect(round)?;
                        }
                        PendingCertCommand::CheckPendingStatus{ digest, reply } => {
                            reply.send(self.pending.is_pending(&digest));
                        }
                        PendingCertCommand::FilterUnkownDigests{ mut unknown, reply } => {
                            self.pending.filter_unknown_digests(&mut unknown);
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
