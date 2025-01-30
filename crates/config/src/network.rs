//! Configuration for network variables.

use std::time::Duration;

/// The container for all network configurations.
#[derive(Debug, Default)]
pub struct NetworkConfig {
    /// The configurations for incoming requests from peers missing certificates.
    missing_certs: CertificateRetrievalConfig,
}

/// Configuration for certificate fetching operations
#[derive(Debug, Clone)]
pub struct CertificateRetrievalConfig {
    /// Maximum number of rounds that can be skipped for a single authority
    pub max_skip_rounds: usize,
    /// Maximum time to spend fetching certificates
    pub max_fetch_duration: Duration,
}

impl Default for CertificateRetrievalConfig {
    fn default() -> Self {
        Self { max_skip_rounds: 1000, max_fetch_duration: Duration::from_secs(30) }
    }
}
