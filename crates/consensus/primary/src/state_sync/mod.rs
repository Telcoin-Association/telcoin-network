//! Modules for synchronizing state between nodes.

use crate::ConsensusBus;
use fastcrypto::hash::Hash as _;
use gc::AtomicRound;
use std::collections::HashMap;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{Certificate, CertificateDigest};
mod cert_collector;
mod cert_manager;
mod cert_validator;
mod gc;
mod headers;
mod pending_cert_manager;
pub(crate) use cert_collector::CertificateCollector;
pub(crate) use cert_manager::CertificateManagerCommand;
pub(crate) use headers::HeaderValidator;

#[cfg(test)]
#[path = "../tests/certificate_processing_tests.rs"]
/// Test the entire certificate flow.
mod cert_flow;

/// Process unverified headers and certificates.
#[derive(Debug, Clone)]
pub struct StateSynchronizer<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// Genesis digests and contents.
    genesis: HashMap<CertificateDigest, Certificate>,
    /// Highest garbage collection round.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Highest round of verfied certificate that has been received.
    highest_received_round: AtomicRound,
}

impl<DB> StateSynchronizer<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        // parents: CertificatesAggregatorManager,
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
        highest_received_round: AtomicRound,
    ) -> Self {
        let genesis = Certificate::genesis(config.committee())
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();

        Self {
            consensus_bus,
            config,
            // parents,
            genesis,
            gc_round,
            highest_processed_round,
            highest_received_round,
        }
    }
}
