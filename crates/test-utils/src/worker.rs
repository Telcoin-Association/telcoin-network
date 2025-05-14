//! Test fixture for worker.
//! Feature-flag only.

use tn_config::KeyConfig;
use tn_types::{NetworkKeypair, WorkerId, WorkerInfo};

/// Fixture representing a worker for an [AuthorityFixture].
///
/// [WorkerFixture] holds keypairs and should not be used in production.
#[derive(Debug)]
pub struct WorkerFixture {
    key_config: KeyConfig,
    pub id: WorkerId,
    pub info: WorkerInfo,
}

impl WorkerFixture {
    pub fn keypair(&self) -> &NetworkKeypair {
        self.key_config.worker_network_keypair()
    }

    pub fn info(&self) -> &WorkerInfo {
        &self.info
    }

    pub fn generate<P>(key_config: KeyConfig, id: WorkerId, mut get_port: P) -> Self
    where
        P: FnMut(&str) -> u16,
    {
        let worker_name = key_config.worker_network_public_key();
        let host = "127.0.0.1";
        let worker_address = format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();

        Self { key_config, id, info: WorkerInfo { name: worker_name, worker_address } }
    }
}
