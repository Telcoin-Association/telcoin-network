//! Test fixture for worker.
//! Feature-flag only.

use tn_config::KeyConfig;
use tn_types::{NetworkKeypair, WorkerId};

/// Fixture representing a worker for an [AuthorityFixture].
///
/// [WorkerFixture] holds keypairs and should not be used in production.
#[derive(Debug)]
pub struct WorkerFixture {
    key_config: KeyConfig,
    pub id: WorkerId,
}

impl WorkerFixture {
    pub fn keypair(&self) -> &NetworkKeypair {
        self.key_config.worker_network_keypair()
    }

    pub fn generate(key_config: KeyConfig, id: WorkerId) -> Self {
        Self { key_config, id }
    }
}
