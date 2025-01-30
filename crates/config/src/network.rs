//! Configuration for network variables.

use std::time::Duration;

/// The container for all network configurations.
#[derive(Debug, Default)]
pub struct NetworkConfig {
    /// The configurations for incoming requests from peers missing certificates.
    sync_config: SyncConfig,
}

impl NetworkConfig {
    /// Return a reference to the [SyncConfig].
    pub fn sync_config(&self) -> &SyncConfig {
        &self.sync_config
    }
}

/// Configuration for state syncing operations.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Maximum number of rounds that can be skipped for a single authority when requesting missing certificates.
    pub max_skip_rounds_for_missing_certs: usize,
    /// Maximum time to spend collecting certificates from the local storage.
    pub max_cert_collection_duration: Duration,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_skip_rounds_for_missing_certs: 1000,
            max_cert_collection_duration: Duration::from_secs(10),
        }
    }
}
