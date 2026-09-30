//! Test fixture for worker.
//! Feature-flag only.

use tn_config::KeyConfig;
use tn_types::{NetworkKeypair, WorkerId};

/// Fixture representing a worker for an [AuthorityFixture].
///
/// [WorkerFixture] holds keypairs and should not be used in production.
#[derive(Debug)]
pub struct WorkerFixture {
    /// Key manager deriving this worker's network identity.
    key_config: KeyConfig,
    /// Worker id within its authority, independent of the authority's committee position.
    pub id: WorkerId,
}

impl WorkerFixture {
    /// The derived network keypair for this fixture's worker id.
    pub fn keypair(&self) -> NetworkKeypair {
        self.key_config.worker_network_keypair(self.id)
    }

    /// Create a worker fixture with an id scoped to its authority.
    pub fn generate(key_config: KeyConfig, id: WorkerId) -> Self {
        Self { key_config, id }
    }
}
