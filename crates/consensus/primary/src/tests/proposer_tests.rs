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

/// Helper to build a header with the given author, round, epoch, and payload digests.
///
/// Unlike [`build_test_header`] the epoch is caller-supplied, so a test can construct the
/// stale-cross-epoch record that the recovery and guard predicates must reject.
fn build_header_at(
    author: &AuthorityIdentifier,
    round: Round,
    epoch: Epoch,
    digests: &[BlockHash],
) -> Header {
    let payload: IndexMap<BlockHash, u16> = digests.iter().map(|&d| (d, 0u16)).collect();
    HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(epoch)
        .parents(BTreeSet::new())
        .created_at(now())
        .payload(payload)
        .build()
}

/// The proposer recovers its round from the `LastProposed` guard record.
///
/// `prime_consensus` restores `primary_round` from the last *committed* leader round, which trails
/// the last *proposed* round `P` after a crash. Starting at zero would make the restarted node
/// propose a fresh header well below `P`, clobber the guard record, and later rebuild round `P`
/// with a different digest - which voters holding a durable `Votes` record for `P` reject forever.
#[tokio::test]
async fn test_proposer_recovers_round_from_last_proposed() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = primary.id();
    let config = primary.consensus_config();
    let epoch = committee.epoch();
    let task_manager = TaskManager::default();

    let build_proposer = |bus: ConsensusBus| {
        Proposer::new(
            config.clone(),
            config.authority_id().expect("authority"),
            bus,
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            task_manager.get_spawner(),
        )
    };

    // an empty store keeps the pre-recovery behavior of starting at round 0
    assert_eq!(build_proposer(ConsensusBus::new()).round, 0, "empty store must start at round 0");

    // a header from the current epoch seeds `round` to P - 1, so the first proposal lands on P
    // and the repropose filter in `propose_next_header` hits
    config.node_storage().write_last_proposed(&build_header_at(&author, 7, epoch, &[])).unwrap();
    assert_eq!(build_proposer(ConsensusBus::new()).round, 6, "round must be seeded to P - 1");

    // a node that already advanced past P via state sync must not be dragged backwards
    let ahead = ConsensusBus::new();
    ahead.app().primary_round_updates().send_replace(9);
    assert_eq!(build_proposer(ahead).round, 0, "must not seed behind an already-advanced node");

    // rounds restart per epoch, so a record that outlived its epoch must not seed the round
    config
        .node_storage()
        .write_last_proposed(&build_header_at(&author, 7, epoch + 1, &[]))
        .unwrap();
    assert_eq!(
        build_proposer(ConsensusBus::new()).round,
        0,
        "stale cross-epoch record must not seed the round"
    );
}

/// A restarted proposer re-sends the byte-identical header rather than rebuilding it.
///
/// `created_at` is wall-clock and sits inside the digest preimage, so a rebuilt header for the same
/// round carries a *different* digest. This is the premise the fail-closed vote guard depends on:
/// voters holding a durable `Votes` record for round `P` only recast their vote for the identical
/// digest, and reject anything else with `AlreadyVoted`.
#[tokio::test]
async fn test_restart_reproposes_identical_header() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();

    // the header this authority proposed before the crash, still in the guard record
    let proposed = build_header_at(&primary.id(), 7, committee.epoch(), &[B256::random()]);
    config.node_storage().write_last_proposed(&proposed).unwrap();

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let task_manager = TaskManager::default();
    let proposer = Proposer::new(
        config.clone(),
        config.authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    let header = tokio::time::timeout(Duration::from_secs(5), rx_headers.recv())
        .await
        .expect("proposer emitted a header before the timeout")
        .expect("header channel stayed open");

    assert_eq!(header.round(), proposed.round(), "first proposal must land back on round P");
    assert_eq!(header, proposed, "restart must repropose the identical header");
    assert_eq!(header.digest(), proposed.digest(), "the reproposed digest must be unchanged");
}

/// A proposal at a lower round must not erase the guard record for a higher round.
///
/// `LastProposed` is a single-slot record, so an unguarded write lets a transient low-round
/// proposal clobber the record for a round this authority has already broadcast - which is exactly
/// what makes the restarted node rebuild that round with a fresh digest.
#[tokio::test]
async fn test_last_proposed_guard_record_is_not_clobbered() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = primary.id();
    let config = primary.consensus_config();
    let store = config.node_storage().clone();
    let epoch = committee.epoch();

    let cb = ConsensusBus::new();
    // keep the header channel subscribed so `send` is not silently dropped
    let _rx_headers = cb.subscribe_headers();
    let stored = || store.get_last_proposed().unwrap().unwrap();

    let high = build_header_at(&author, 7, epoch, &[B256::random()]);
    Proposer::store_and_send_header(&high, store.clone(), &cb).await.unwrap();
    assert_eq!(stored(), high, "the first proposal is recorded");

    // a lower round must leave the round-7 record intact
    let low = build_header_at(&author, 3, epoch, &[B256::random()]);
    Proposer::store_and_send_header(&low, store.clone(), &cb).await.unwrap();
    assert_eq!(stored(), high, "a lower round must not clobber the guard record");

    // the same round still rewrites, so the in-round repropose path keeps working
    let same = build_header_at(&author, 7, epoch, &[B256::random()]);
    Proposer::store_and_send_header(&same, store.clone(), &cb).await.unwrap();
    assert_eq!(stored(), same, "an equal round must still be recorded");

    // and so does a higher round
    let higher = build_header_at(&author, 8, epoch, &[B256::random()]);
    Proposer::store_and_send_header(&higher, store.clone(), &cb).await.unwrap();
    assert_eq!(stored(), higher, "a higher round must be recorded");

    // a new epoch restarts the rounds, so a low round from a later epoch must supersede.
    // Without this the guard would refuse every write for a whole epoch, silently disabling the
    // anti-equivocation record it exists to protect.
    let next_epoch = build_header_at(&author, 1, epoch + 1, &[B256::random()]);
    Proposer::store_and_send_header(&next_epoch, store.clone(), &cb).await.unwrap();
    assert_eq!(
        stored(),
        next_epoch,
        "a header from a new epoch must supersede regardless of round"
    );
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
