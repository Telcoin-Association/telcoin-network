//! Modules for synchronizing state between nodes.

use crate::{error::CertManagerResult, ConsensusBus};
use cert_validator::CertificateValidator;
use gc::AtomicRound;
use header_validator::HeaderValidator;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{error::HeaderResult, Certificate, CertificateDigest, Header, Round, TaskManager};
mod cert_collector;
mod cert_manager;
mod cert_validator;
mod gc;
mod header_validator;
mod pending_cert_manager;
pub(crate) use cert_collector::CertificateCollector;
pub(crate) use cert_manager::CertificateManagerCommand;

#[cfg(test)]
#[path = "../tests/certificate_processing_tests.rs"]
/// Test the entire certificate flow.
mod cert_flow;

/// Process unverified headers and certificates.
#[derive(Debug, Clone)]
pub struct StateSynchronizer<DB> {
    /// The type to validate certificates.
    certificate_validator: CertificateValidator<DB>,
    /// The type to validate headers.
    header_validator: HeaderValidator<DB>,
}

impl<DB> StateSynchronizer<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(config: ConsensusConfig<DB>, consensus_bus: ConsensusBus) -> Self {
        let header_validator = HeaderValidator::new(config.clone(), consensus_bus.clone());
        // load highest round number from the certificate store
        let highest_process_round =
            if let Some(subdag) = config.node_storage().consensus_store.get_latest_sub_dag() {
                subdag.leader.round()
            } else {
                0
            };

        let certificate_validator = CertificateValidator::new(
            config,
            consensus_bus,
            AtomicRound::new(0),
            AtomicRound::new(highest_process_round),
            AtomicRound::new(0),
        );

        Self { certificate_validator, header_validator }
    }

    /// Spawn the certificate manager and synchronize state between peers.
    pub(crate) fn spawn(&self, task_manager: &TaskManager) {
        let certificate_manager = self.certificate_validator.new_cert_manager();
        task_manager.spawn_task("certificate-manager", certificate_manager.run());
    }

    //
    //=== Certificate API
    //

    /// Process a certificate produced by the this node.
    pub(crate) async fn process_own_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.certificate_validator.process_own_certificate(certificate).await
    }

    /// Process a certificate received from a peer.
    pub(crate) async fn process_peer_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.certificate_validator.process_peer_certificate(certificate).await
    }

    /// Process a large collection of certificates downloaded from peers.
    ///
    /// This partitions the collection to verify certificates in chunks.
    pub(crate) async fn process_fetched_certificates_in_parallel(
        &self,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<()> {
        self.certificate_validator.process_fetched_certificates_in_parallel(certificates).await
    }

    //
    //=== Header API
    //

    /// Returns the parent certificates of the given header, waits for availability if needed.
    pub(crate) async fn notify_read_parent_certificates(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<Certificate>> {
        self.header_validator.notify_read_parent_certificates(header).await
    }

    /// Synchronize batches.
    pub(crate) async fn sync_header_batches(
        &self,
        header: &Header,
        is_certified: bool,
        max_age: Round,
    ) -> HeaderResult<()> {
        self.header_validator.sync_header_batches(header, is_certified, max_age).await
    }

    /// Filter parent digests that do not exist in storage or pending state.
    ///
    /// Returns a collection of missing parent digests.
    pub(crate) async fn identify_unkown_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        self.header_validator.identify_unkown_parents(header).await
    }
}
