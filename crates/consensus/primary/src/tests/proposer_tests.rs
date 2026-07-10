//! Proposer unit tests.

use super::*;
use crate::consensus::LeaderSwapTable;
use indexmap::IndexMap;
use std::collections::BTreeSet;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{error::HeaderError, now, HeaderBuilder, B256, MAX_HEADER_NUM_OF_BATCHES};

#[tokio::test]
async fn test_empty_proposal() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let task_manager = TaskManager::default();
    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round(), 1);
    assert!(header.payload().is_empty());
    assert!(header.validate(&committee).is_ok());

    // TODO: assert header el state present
}

/// A header at the protocol batch ceiling validates, but one batch over is rejected.  This keeps
/// the per-header batch count a genuine consensus invariant (not just a proposer convention), which
/// is what lets the consensus-pack reader bound the batches per output and always reconstruct a
/// committed sub-DAG (#896).
#[tokio::test]
async fn test_header_rejects_too_many_batches() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    // Random, distinct batch digests keyed to worker 0; only the payload length differs.
    let at_ceiling: IndexMap<B256, u16> =
        (0..MAX_HEADER_NUM_OF_BATCHES).map(|_| (B256::random(), 0)).collect();
    let ok = primary.header_builder(&committee).payload(at_ceiling).build();
    assert!(ok.validate(&committee).is_ok(), "header at the ceiling must validate");

    let over_ceiling: IndexMap<B256, u16> =
        (0..MAX_HEADER_NUM_OF_BATCHES + 1).map(|_| (B256::random(), 0)).collect();
    let too_many = primary.header_builder(&committee).payload(over_ceiling).build();
    assert!(
        matches!(too_many.validate(&committee), Err(HeaderError::TooManyBatches(_, _))),
        "header over the ceiling must be rejected"
    );
}

#[tokio::test]
async fn test_equivocation_protection_after_restart() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    /* Old comments, note if test gets flakey:
     max_header_delay
    Duration::from_secs(1_000), // Ensure it is not triggered.
     min_header_delay
    Duration::from_secs(1_000), // Ensure it is not triggered.
    */
    // Spawn the proposer.
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let mut task_manager = TaskManager::default();
    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send parents
    let parents: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.payload().get(&digest), Some(&worker_id));
    assert!(header.validate(&committee).is_ok());

    // TODO: assert header el state present

    // restart the proposer.
    fixture.notify_shutdown();
    primary.consensus_config().shutdown().notify();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        task_manager.join(primary.consensus_config().shutdown().clone())
    )
    .await
    .is_ok());

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let task_manager = TaskManager::default();
    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send a superset parents, same round but different set from before
    let parents: Vec<_> =
        fixture.headers().iter().take(4).map(|h| fixture.certificate(h)).collect();

    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes the same header as before
    let new_header = rx_headers.recv().await.unwrap();
    if new_header.round() == header.round() {
        assert_eq!(header, new_header);
    }
}

/// Helper to build a header with the given author, round, and payload digests.
fn build_test_header(author: &AuthorityIdentifier, round: Round, digests: &[BlockHash]) -> Header {
    let mut payload = IndexMap::new();
    for &d in digests {
        payload.insert(d, 0u16);
    }
    HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(0)
        .parents(BTreeSet::new())
        .created_at(now())
        .payload(payload)
        .build()
}

#[tokio::test]
async fn test_process_committed_headers() {
    // -- shared setup helper --
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = primary.id();

    // --- Scenario A: header at round > max_committed_round is NOT re-queued ---
    {
        let cb = ConsensusBus::new();
        let task_manager = TaskManager::default();
        let mut proposer = Proposer::new(
            primary.consensus_config(),
            primary.consensus_config().authority_id().expect("authority"),
            cb.clone(),
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            task_manager.get_spawner(),
        );

        // insert headers at rounds 4, 5, 6 with distinct digests
        let d4 = B256::random();
        let d5 = B256::random();
        let d6 = B256::random();
        proposer.proposed_headers.insert(4, build_test_header(&author, 4, &[d4]));
        proposer.proposed_headers.insert(5, build_test_header(&author, 5, &[d5]));
        proposer.proposed_headers.insert(6, build_test_header(&author, 6, &[d6]));

        // commit rounds 4 and 5 => max_committed_round = 5
        // round 6 > 5 so it should NOT be re-queued
        proposer.process_committed_headers(6, vec![4, 5]);

        // nothing re-queued because the only uncommitted header (round 6) is above
        // max_committed_round
        assert!(proposer.digests.is_empty(), "round 6 should not be re-queued");

        // rounds 4, 5 removed as committed; only round 6 remains
        assert_eq!(proposer.proposed_headers.len(), 1);
        assert!(
            proposer.proposed_headers.contains_key(&6),
            "round 6 header should still be in proposed_headers"
        );
    }

    // --- Scenario B: header at round <= max_committed_round IS re-queued ---
    {
        let cb = ConsensusBus::new();
        let task_manager = TaskManager::default();
        let mut proposer = Proposer::new(
            primary.consensus_config(),
            primary.consensus_config().authority_id().expect("authority"),
            cb.clone(),
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            task_manager.get_spawner(),
        );

        // insert headers at rounds 3, 4, 5
        let d3a = B256::random();
        let d3b = B256::random();
        let d4 = B256::random();
        let d5 = B256::random();
        proposer.proposed_headers.insert(3, build_test_header(&author, 3, &[d3a, d3b]));
        proposer.proposed_headers.insert(4, build_test_header(&author, 4, &[d4]));
        proposer.proposed_headers.insert(5, build_test_header(&author, 5, &[d5]));

        // commit rounds 4 and 5 => max_committed_round = 5
        // round 3 <= 5 and NOT in committed list => its digests are re-queued
        proposer.process_committed_headers(5, vec![4, 5]);

        // round 3's digests should have been prepended to self.digests
        assert!(!proposer.digests.is_empty(), "round 3 digests should be re-queued");
        assert_eq!(proposer.digests.len(), 2, "round 3 had two digests");

        // verify the re-queued digests match round 3's payload
        let requeued: Vec<BlockHash> = proposer.digests.iter().map(|pd| pd.digest).collect();
        assert!(requeued.contains(&d3a), "d3a should be re-queued");
        assert!(requeued.contains(&d3b), "d3b should be re-queued");

        // round 3 should also be removed from proposed_headers
        assert!(
            !proposer.proposed_headers.contains_key(&3),
            "round 3 should be removed after re-queue"
        );

        // committed rounds 4, 5 also removed
        assert!(proposer.proposed_headers.is_empty(), "all headers should be removed");
    }

    // --- Scenario C: empty committed headers ---
    {
        let cb = ConsensusBus::new();
        let task_manager = TaskManager::default();
        let mut proposer = Proposer::new(
            primary.consensus_config(),
            primary.consensus_config().authority_id().expect("authority"),
            cb.clone(),
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            task_manager.get_spawner(),
        );

        // insert headers at rounds 4, 5, 6
        let d4 = B256::random();
        let d5 = B256::random();
        let d6 = B256::random();
        proposer.proposed_headers.insert(4, build_test_header(&author, 4, &[d4]));
        proposer.proposed_headers.insert(5, build_test_header(&author, 5, &[d5]));
        proposer.proposed_headers.insert(6, build_test_header(&author, 6, &[d6]));

        // empty committed list => max_committed_round = 0
        // all rounds > 0 so nothing is re-queued
        proposer.process_committed_headers(6, vec![]);

        assert!(proposer.digests.is_empty(), "no digests should be re-queued");
        assert_eq!(
            proposer.proposed_headers.len(),
            3,
            "all headers should remain in proposed_headers"
        );
        assert!(proposer.proposed_headers.contains_key(&4));
        assert!(proposer.proposed_headers.contains_key(&5));
        assert!(proposer.proposed_headers.contains_key(&6));
    }
}
