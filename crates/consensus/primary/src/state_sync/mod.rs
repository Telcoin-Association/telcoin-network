//! Modules for synchronizing state between nodes.

use crate::{aggregators::CertificatesAggregatorManager, ConsensusBus};
use fastcrypto::hash::Hash as _;
use pending_cert_manager::PendingCertificateManager;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use tn_config::ConsensusConfig;
use tn_storage::traits::Database;
use tn_types::{Certificate, CertificateDigest};

mod cert_acceptor;
mod cert_collector;
mod cert_manager;
mod cert_processor;
mod cert_validator;
mod headers;
mod pending_cert_manager;
pub(crate) use cert_collector::CertificateCollector;
pub(crate) use headers::HeaderValidator;
pub(crate) use pending_cert_manager::PendingCertCommand;

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
        parents: CertificatesAggregatorManager,
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

/// Holds the atomic round.
#[derive(Clone)]
pub struct AtomicRound {
    /// The inner type.
    inner: Arc<InnerAtomicRound>,
}

/// The inner type for [AtomicRound]
struct InnerAtomicRound {
    /// The atomic gc round.
    atomic: AtomicU32,
}

impl AtomicRound {
    /// Create a new instance of Self.
    pub fn new(num: u32) -> Self {
        Self { inner: Arc::new(InnerAtomicRound { atomic: AtomicU32::new(num) }) }
    }

    /// Store the new gc round.
    ///
    /// Only [ConsensusRound] can call this.
    fn store(&mut self, new: u32) {
        self.inner.atomic.store(new, std::sync::atomic::Ordering::Release);
    }

    /// Load the gc round.
    pub fn load(&self) -> u32 {
        self.inner.atomic.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Fetch the max.
    pub fn fetch_max(&self, val: u32) -> u32 {
        self.inner.atomic.fetch_max(val, Ordering::AcqRel)
    }
}

impl std::fmt::Debug for AtomicRound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner.atomic)
    }
}
impl std::default::Default for AtomicRound {
    fn default() -> Self {
        Self { inner: Arc::new(InnerAtomicRound { atomic: AtomicU32::new(0) }) }
    }
}
