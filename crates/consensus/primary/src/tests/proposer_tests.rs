//! Proposer unit tests.

use super::*;
use crate::consensus::LeaderSwapTable;
use indexmap::IndexMap;
use std::collections::BTreeSet;
use tn_config::{ConsensusConfig, NetworkConfig};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use tn_types::{
    error::HeaderError,
    forks::{seed_signature_fork_epoch_override, subsecond_timestamp_fork_epoch_override},
    now, HeaderBuilder, TimestampMs, B256, MAX_HEADER_NUM_OF_BATCHES,
};

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

/// A header off the REAL proposer path carries a seed signature that verifies against THAT header's
/// own `(epoch, round)`.
///
/// [`EpochSeedMessage::new(epoch, round, ..)`](tn_types::EpochSeedMessage::new) and
/// [`Header::new(author, round, epoch, ..)`](tn_types::Header::new) sit three lines apart in
/// [`Proposer::propose_header`] and take the same two `u32`s in OPPOSITE order, so transposing
/// either pair compiles clean. The consequence is severe and silent locally: every honest voter
/// would reject every header this node authors, while the node itself sees nothing wrong.
///
/// Catches: transposing either argument pair, and signing any round other than the one being
/// proposed (a hard-coded round, or a round cached once per epoch as the pre-round-binding code
/// did). Every other seed test in the tree drives the fixture header builder, which stamps its own
/// signature and never reaches `propose_header`, so nothing else covers this. The expectation is
/// rebuilt from the values the PROPOSED header reports, so a transposition at either call site
/// leaves the signature verifying against nothing.
#[tokio::test]
async fn test_proposed_header_seed_signature_binds_its_own_epoch_and_round() {
    use futures::StreamExt as _;

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();

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

    let header = rx_headers.recv().await.unwrap();

    // A transposition is only observable when the two values differ. The fixture proposes round 1
    // in epoch 0, and this pins that precondition so the test can never pass vacuously.
    assert_ne!(
        header.epoch(),
        header.round(),
        "epoch and round must differ for a transposition to be observable"
    );

    // The voter's reconstruction: same anchor the handler uses, both scalars taken from the header.
    let message =
        EpochSeedMessage::new(header.epoch(), header.round(), config.prior_epoch_record());
    assert!(
        message.verify(
            header.seed_signature().expect("seed signature present for fork-active epoch"),
            &primary.primary_public_key()
        ),
        "the proposed header's seed signature must verify against that header's own (epoch, round)"
    );

    // Advance one round and re-check. At round 1 a signature over a CONSTANT round is
    // indistinguishable from a correctly bound one, so round binding only becomes observable once
    // the proposer moves off its first round. Nothing else in the tree drives `propose_header`
    // past round 1: with `current_round` replaced by a literal `1`, all 170 lib tests and all 40
    // integration tests still pass, so without the assertions below the binding is untested.
    let parents: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();
    cb.parents().send((parents, 1)).await.expect("parents accepted");

    // Skip rather than take the next header: the proposer re-proposes its current round whenever
    // `max_header_delay` fires again before the parents land, so taking one header here is racy.
    let proposals = Box::pin(futures::stream::unfold(rx_headers, |mut rx| async move {
        rx.recv().await.map(|proposed| (proposed, rx))
    }));
    let next = tokio::time::timeout(
        Duration::from_secs(10),
        proposals
            .skip_while(|proposed| futures::future::ready(proposed.round() <= header.round()))
            .next(),
    )
    .await
    .expect("the proposer must advance past its first round once round-1 parents reach quorum")
    .expect("header channel stays open");

    assert!(
        EpochSeedMessage::new(next.epoch(), next.round(), config.prior_epoch_record()).verify(
            next.seed_signature().expect("seed signature present for fork-active epoch"),
            &primary.primary_public_key()
        ),
        "the seed message must be re-signed for every round the proposer advances to"
    );

    // ...and bound to THAT round specifically, not merely to some round this authority once held.
    assert!(
        !EpochSeedMessage::new(next.epoch(), header.round(), config.prior_epoch_record()).verify(
            next.seed_signature().expect("seed signature present for fork-active epoch"),
            &primary.primary_public_key()
        ),
        "a later round's header must not carry the earlier round's seed signature"
    );
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
    // Build a fresh config for the restart, as production does per epoch: the
    // old config's one-shot shutdown is latched, so a proposer subscribing to
    // it would exit immediately. Node storage is shared so the equivocation
    // guard still sees the first proposal.
    let config = primary.consensus_config();
    let restarted_config = ConsensusConfig::new_with_committee_for_test(
        config.config().clone(),
        config.node_storage().clone(),
        config.key_config().clone(),
        committee.clone(),
        NetworkConfig::default(),
    )
    .unwrap();
    let proposer = Proposer::new(
        restarted_config.clone(),
        restarted_config.authority_id().expect("authority"),
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
///
/// The stored header is from a fork-active epoch and carries a non-zero `created_at_millis`, which
/// the digest preimage covers as well, so a guard record that lost the millisecond part would
/// repropose a header those voters reject.
#[tokio::test]
async fn test_restart_reproposes_identical_header() {
    pin_subsecond_fork(true);
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();

    // the header this authority proposed before the crash, still in the guard record
    let proposed = HeaderBuilder::from_header(&build_header_at(
        &primary.id(),
        7,
        committee.epoch(),
        &[B256::random()],
    ))
    .created_at_ms(TimestampMs::from_parts(now(), 737))
    .build();
    assert_eq!(
        proposed.created_at_millis(),
        737,
        "a fork-active header must keep its millisecond part, or this test proves nothing about it"
    );
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
    assert_eq!(
        header.created_at_ms(),
        proposed.created_at_ms(),
        "the reproposed header must keep its millisecond timestamp"
    );
}

/// Pins this test process's sub-second timestamp fork to active (or dormant) from genesis, with
/// the seed-signature fork it requires active from genesis.
///
/// The gates read their `test-utils` environment overrides once per process, so this must run
/// before anything consults a gate, including building the committee fixture. nextest runs each
/// test in its own process, which is what keeps one test's pin from reaching another; a
/// single-process `cargo test` run shares one latch across the whole test binary instead. Reading
/// the overrides back turns a value that latched before the pin into a named failure.
fn pin_subsecond_fork(active: bool) {
    let subsecond_fork: Epoch = if active { 0 } else { Epoch::MAX };
    std::env::set_var("TN_SEED_SIGNATURE_FORK_EPOCH", "0");
    std::env::set_var("TN_SUBSECOND_TIMESTAMP_FORK_EPOCH", subsecond_fork.to_string());
    assert_eq!(
        seed_signature_fork_epoch_override(),
        Some(0),
        "TN_SEED_SIGNATURE_FORK_EPOCH latched to another value before this test pinned it"
    );
    assert_eq!(
        subsecond_timestamp_fork_epoch_override(),
        Some(subsecond_fork),
        "TN_SUBSECOND_TIMESTAMP_FORK_EPOCH latched to another value before this test pinned it"
    );
}

/// Round-1 parent certificates, one per authority in fixture order, created at the given times.
///
/// The proposer takes its parents as already certified and reads only their digests and
/// timestamps, so these carry no votes and no seed signature. Nothing here signs, which keeps the
/// gap between choosing the timestamps and proposing far below the leads under test.
fn parents_created_at(
    fixture: &CommitteeFixture<MemDatabase>,
    created_at: &[TimestampMs],
) -> Vec<Certificate> {
    let committee = fixture.committee();
    let genesis: BTreeSet<_> =
        Certificate::genesis(&committee).iter().map(|c| c.digest()).collect();
    assert_eq!(fixture.authorities().count(), created_at.len(), "one timestamp per authority");
    fixture
        .authorities()
        .zip(created_at)
        .map(|(authority, &created_at)| {
            let header = HeaderBuilder::default()
                .author(authority.id())
                .round(1)
                .epoch(committee.epoch())
                .parents(genesis.clone())
                .created_at_ms(created_at)
                .build();
            Certificate::new_unsigned_for_test(&committee, header, Vec::new())
                .expect("unsigned parent certificate")
        })
        .collect()
}

/// Proposes `primary`'s round-2 header over `parents` and returns it with the time the proposal
/// took on tokio's clock.
///
/// Callers pause tokio's clock, which then advances only when the proposer sleeps, so the returned
/// duration is exactly the proposer's wait, free of scheduling and signing noise. The wall clock
/// behind `now_ms` keeps running, so the header is still stamped from real time.
async fn propose_round_two(
    primary: &AuthorityFixture<MemDatabase>,
    committee: &Committee,
    parents: Vec<Certificate>,
) -> (Header, Duration) {
    let config = primary.consensus_config();
    let cb = ConsensusBus::new();
    // keep the header channel subscribed so `send` is not silently dropped
    let _rx_headers = cb.subscribe_headers();
    let identity = HeaderIdentity {
        round: 2,
        epoch: committee.epoch(),
        author: primary.id(),
        prior_epoch_record: config.prior_epoch_record(),
        key_config: config.key_config().clone(),
    };

    let start = tokio::time::Instant::now();
    let header = Proposer::propose_header(
        identity,
        config.node_storage().clone(),
        &cb,
        parents,
        VecDeque::new(),
    )
    .await
    .expect("header proposed");
    (header, start.elapsed())
}

/// A parent stamped ahead of this node's clock delays a fork-active proposal by the millisecond
/// gap, not whole seconds, and the header lands strictly after the latest parent.
///
/// Fork-active voters require a header's millisecond timestamp to exceed every parent's, so the
/// proposer waits for, and clamps to, one millisecond past the latest parent. The latest parent
/// sits mid-list so reading any parent but the maximum fails the clamp. With tokio's clock paused
/// the wait costs no real time, so the header is normally stamped while the wall clock still
/// trails the parent and the clamp decides the timestamp: a wait rounded up to whole seconds fails
/// the wait bound, and a missing clamp or missing `+ 1` fails the timestamp bound.
#[tokio::test(start_paused = true)]
async fn test_proposal_waits_milliseconds_for_future_parents() {
    pin_subsecond_fork(true);
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    assert!(subsecond_timestamp_active(committee.epoch()), "the pin must activate the fork");

    let base = now_ms();
    let latest_parent = base.saturating_add_millis(30);
    let parents = parents_created_at(
        &fixture,
        &[
            base.saturating_add_millis(10),
            latest_parent,
            base.saturating_add_millis(20),
            base.saturating_add_millis(5),
        ],
    );

    let (header, waited) = propose_round_two(primary, &committee, parents).await;

    let min_created_at = latest_parent.saturating_add_millis(1);
    assert!(
        header.created_at_ms() >= min_created_at,
        "header created at {} must be at least 1ms after the latest parent {latest_parent}",
        header.created_at_ms(),
    );
    // the wait is `min_created_at - now_ms()` measured after `base`; tokio's timer may round the
    // deadline up to its next millisecond tick
    let max_wait = Duration::from_millis(min_created_at.as_millis() - base.as_millis() + 1);
    assert!(
        waited <= max_wait,
        "proposer waited {waited:?}, more than the {max_wait:?} gap to the latest parent"
    );
    assert!(waited < Duration::from_secs(1), "proposer must not wait whole seconds");
}

/// Before the sub-second fork the proposer keeps the seconds-only rule (`>=` against the latest
/// parent's whole second, millisecond part 0) but still waits only the millisecond gap.
///
/// Pre-fork parents carry whole seconds, so the latest parent sits on the next second boundary,
/// less than a second ahead of this node's clock. A seconds-granular sleep waits out a whole
/// second there and overshoots the gap to that boundary.
#[tokio::test(start_paused = true)]
async fn test_pre_fork_proposal_keeps_seconds_rule_with_millisecond_wait() {
    pin_subsecond_fork(false);
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    assert!(!subsecond_timestamp_active(committee.epoch()), "the pin must keep the fork dormant");

    let base = now_ms();
    let latest_parent = TimestampMs::from_parts(base.secs() + 1, 0);
    let current_second = TimestampMs::from_parts(base.secs(), 0);
    let parents = parents_created_at(
        &fixture,
        &[current_second, latest_parent, current_second, current_second],
    );
    assert!(
        parents.iter().all(|p| p.header().created_at_millis() == 0),
        "pre-fork parents must carry whole seconds only"
    );

    let (header, waited) = propose_round_two(primary, &committee, parents).await;

    assert_eq!(header.created_at_millis(), 0, "a pre-fork header must not carry milliseconds");
    assert!(
        *header.created_at() >= latest_parent.secs(),
        "header second {} must not precede the latest parent's second {}",
        header.created_at(),
        latest_parent.secs(),
    );
    // the wait is `latest_parent - now_ms()` measured after `base`; tokio's timer may round the
    // deadline up to its next millisecond tick
    let max_wait = Duration::from_millis(latest_parent.as_millis() - base.as_millis() + 1);
    assert!(
        waited <= max_wait,
        "proposer waited {waited:?}, more than the {max_wait:?} gap to the next second"
    );
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
        assert_eq!(proposer.pending_digests_len(), 2, "round 3 had two digests");

        // verify the re-queued digests match round 3's payload
        let requeued: Vec<BlockHash> =
            proposer.digests.values().flatten().map(|pd| pd.digest).collect();
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

/// Header slots are shared across workers: a deep worker-0 backlog cannot starve worker 1
/// (issue #556). Slots interleave one-per-worker in ascending id order, FIFO within a
/// worker, and a worker's unused share flows to the workers that still have digests.
#[tokio::test]
async fn test_drain_digests_shares_header_slots_across_workers() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );
    assert_eq!(proposer.max_header_num_of_batches, 10, "test assumes the default header cap");

    // worker 0 queues a deep backlog, worker 1 only three digests
    let w0: Vec<B256> = (0..15).map(|_| B256::random()).collect();
    let w1: Vec<B256> = (0..3).map(|_| B256::random()).collect();
    proposer.digests.insert(
        0,
        w0.iter().map(|digest| ProposerDigest { digest: *digest, worker_id: 0 }).collect(),
    );
    proposer.digests.insert(
        1,
        w1.iter().map(|digest| ProposerDigest { digest: *digest, worker_id: 1 }).collect(),
    );

    let selected = proposer.drain_digests_for_header();
    assert_eq!(selected.len(), 10, "header fills to the cap");

    // rounds of one-per-worker while both lanes have digests, then worker 0 takes the rest
    let expected = vec![w0[0], w1[0], w0[1], w1[1], w0[2], w1[2], w0[3], w0[4], w0[5], w0[6]];
    let got: Vec<B256> = selected.iter().map(|digest| digest.digest).collect();
    assert_eq!(got, expected, "slots interleave per worker, FIFO within each worker");

    // worker 1 fully drained, worker 0 keeps its FIFO tail for the next header
    assert_eq!(proposer.digests.get(&1).map(VecDeque::len), Some(0));
    let remaining: Vec<B256> =
        proposer.digests.get(&0).expect("worker 0 lane").iter().map(|d| d.digest).collect();
    assert_eq!(remaining, w0[7..].to_vec(), "worker 0 tail stays FIFO");
}

/// With a single worker the fair drain is exactly the old FIFO drain.
#[tokio::test]
async fn test_drain_digests_single_worker_is_fifo() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    let cap = proposer.max_header_num_of_batches;
    let w0: Vec<B256> = (0..cap + 2).map(|_| B256::random()).collect();
    proposer.digests.insert(
        0,
        w0.iter().map(|digest| ProposerDigest { digest: *digest, worker_id: 0 }).collect(),
    );

    let selected = proposer.drain_digests_for_header();
    let got: Vec<B256> = selected.iter().map(|digest| digest.digest).collect();
    assert_eq!(got, w0[..cap].to_vec(), "single worker drains FIFO up to the cap");
    let remaining: Vec<B256> =
        proposer.digests.get(&0).expect("worker 0 lane").iter().map(|d| d.digest).collect();
    assert_eq!(remaining, w0[cap..].to_vec(), "the tail stays queued in order");
}

/// When more workers have pending digests than a header holds, the slot walk rotates
/// across headers via the cursor, so no lane is excluded permanently (the depth-0 round
/// alone already overflows the cap).
#[tokio::test]
async fn test_drain_digests_rotates_across_headers_when_lanes_exceed_cap() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );
    assert_eq!(proposer.max_header_num_of_batches, 10, "test assumes the default header cap");

    // twelve lanes with one digest each; lane 0 carries a persistent extra backlog
    (0..12u16).for_each(|worker_id| {
        let depth = if worker_id == 0 { 2 } else { 1 };
        proposer.digests.insert(
            worker_id,
            (0..depth).map(|_| ProposerDigest { digest: B256::random(), worker_id }).collect(),
        );
    });

    let first: Vec<WorkerId> =
        proposer.drain_digests_for_header().iter().map(|digest| digest.worker_id).collect();
    assert_eq!(first, (0..10).collect::<Vec<_>>(), "first header serves lanes 0-9");

    // the next header resumes at lane 10 instead of restarting at lane 0
    let second: Vec<WorkerId> =
        proposer.drain_digests_for_header().iter().map(|digest| digest.worker_id).collect();
    assert_eq!(second, vec![10, 11, 0], "the cursor rotates the walk to the unserved lanes");
}
