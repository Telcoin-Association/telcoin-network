//! Modules for synchronizing state between nodes.

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

mod cert_acceptor;
mod cert_collector;
mod cert_processor;
mod pending_cert_manager;
pub(crate) use cert_collector::CertificateCollector;
pub(crate) use pending_cert_manager::PendingCertCommand;

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
