//! Regression tests for network identities exposed by committee fixtures.

use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{NetworkPublicKey, DEFAULT_WORKER_ID};

/// Every authority fixture exposes worker zero with the key advertised for that worker.
#[test]
fn every_authority_fixture_matches_advertised_worker_zero() -> Result<(), &'static str> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let bootstrap_servers = committee.bootstrap_servers();
    assert_eq!(fixture.num_authorities(), 4, "exercise authorities beyond the first position");
    assert_eq!(committee.number_of_workers(), 1);

    fixture.authorities().try_for_each(|authority| {
        let worker = authority.worker();
        let advertised = bootstrap_servers
            .get(&authority.primary_public_key())
            .and_then(|server| server.worker(DEFAULT_WORKER_ID))
            .ok_or("every authority must advertise worker zero")?;
        assert_eq!(worker.id, DEFAULT_WORKER_ID, "worker id must not depend on authority order");
        let fixture_key: NetworkPublicKey = worker.keypair().public().into();
        assert_eq!(
            fixture_key, advertised.network_key,
            "fixture must authenticate as its authority's advertised worker zero"
        );
        Ok::<(), &'static str>(())
    })?;
    Ok(())
}
