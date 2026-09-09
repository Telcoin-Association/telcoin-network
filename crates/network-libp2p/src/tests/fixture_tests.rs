//! Regression tests for network identities exposed by committee fixtures.

use std::num::NonZeroUsize;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{NetworkPublicKey, DEFAULT_WORKER_ID};

/// Single-worker and multi-worker fixtures advertise the derived key for each worker id.
#[test]
fn every_authority_fixture_matches_advertised_worker_keys() -> Result<(), &'static str> {
    [1, 3].into_iter().try_for_each(|worker_count| {
        let number_of_workers =
            NonZeroUsize::new(worker_count).ok_or("worker count must be nonzero")?;
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .number_of_workers(number_of_workers)
            .build();
        let committee = fixture.committee();
        let bootstrap_servers = committee.bootstrap_servers();
        assert_eq!(fixture.num_authorities(), 4, "exercise authorities beyond the first position");
        assert_eq!(committee.number_of_workers(), number_of_workers.get());

        fixture.authorities().try_for_each(|authority| {
            let worker = authority.worker();
            let server = bootstrap_servers
                .get(&authority.primary_public_key())
                .ok_or("every authority must advertise its worker network identities")?;
            let advertised = server
                .worker(DEFAULT_WORKER_ID)
                .ok_or("every authority must advertise worker zero")?;
            assert_eq!(
                worker.id, DEFAULT_WORKER_ID,
                "worker id must not depend on authority order"
            );
            let fixture_key: NetworkPublicKey = worker.keypair().public().into();
            assert_eq!(
                fixture_key, advertised.network_key,
                "fixture must authenticate as its authority's advertised worker zero"
            );
            let consensus_config = authority.consensus_config();
            committee.worker_ids().try_for_each(|worker_id| {
                let advertised =
                    server.worker(worker_id).ok_or("every worker must be advertised")?;
                assert_eq!(
                    consensus_config.key_config().worker_network_public_key(worker_id),
                    advertised.network_key,
                    "worker {worker_id} must authenticate as its advertised network identity"
                );
                Ok::<(), &'static str>(())
            })
        })?;
        Ok(())
    })
}
