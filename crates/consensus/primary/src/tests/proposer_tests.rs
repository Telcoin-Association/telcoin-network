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

/// The primary publishes one header retaining the digest-to-worker mapping for three workers.
#[tokio::test]
async fn test_multi_worker_proposed_header_payload() -> eyre::Result<()> {
    use std::num::NonZeroUsize;

    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .number_of_workers(NonZeroUsize::new(3).ok_or_else(|| eyre::eyre!("worker count"))?)
        .build();
    let primary = fixture.first_authority();
    let config = primary.consensus_config();
    let committee = fixture.committee();
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let task_manager = TaskManager::default();
    let mut proposer = Proposer::new(
        config.clone(),
        primary.id(),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );
    let payload: IndexMap<_, _> =
        (0_u8..3).map(|id| (B256::repeat_byte(id), u16::from(id))).collect();
    proposer.digests = payload
        .iter()
        .map(|(digest, worker_id)| (*worker_id, VecDeque::from([queued(*digest, *worker_id)])))
        .collect();
    let selected = proposer.drain_digests_for_header();
    let identity = HeaderIdentity {
        round: 1,
        epoch: config.epoch(),
        author: primary.id(),
        prior_epoch_record: proposer.prior_epoch_record,
        key_config: config.key_config().clone(),
    };
    let header = Proposer::propose_header(
        identity,
        config.node_storage().clone(),
        &cb,
        proposer.last_parents,
        selected,
        untracked_trigger(),
    )
    .await?;

    assert_eq!(header.payload(), &payload);
    assert!(header.validate(&committee).is_ok());
    assert_eq!(rx_headers.try_recv()?.digest(), header.digest());
    Ok(())
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

    // a node that already advanced past P via state sync must not be dragged backwards: it
    // starts at the primed round, never below it
    let ahead = ConsensusBus::new();
    ahead.app().primary_round_updates().send_replace(9);
    assert_eq!(build_proposer(ahead).round, 9, "must not seed behind an already-advanced node");

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

    // the first send above is the max-delay re-send, which leaves the round at P - 1. a quorum of
    // parents at P - 1 must lead `propose_next_header` back to the same stored header rather than
    // a rebuilt one with a fresh digest
    let parents = parents_at(&fixture, 6, committee.epoch(), fixture.authorities().count());
    cb.parents().send((parents, 6)).await.expect("parents channel open");
    let header = tokio::time::timeout(Duration::from_secs(10), rx_headers.recv())
        .await
        .expect("parents at P - 1 lead to a proposal before the timeout")
        .expect("header channel stayed open");
    assert_eq!(
        cb.app().primary_round(),
        proposed.round(),
        "the proposal came from propose_next_header, which moves the round watch to P"
    );
    assert_eq!(header, proposed, "parents at P - 1 must repropose the identical header");
    assert_eq!(header.digest(), proposed.digest(), "the reproposed digest must be unchanged");
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
        untracked_trigger(),
    )
    .await
    .expect("header proposed");
    (header, start.elapsed())
}

/// A parent stamped ahead of this node's clock delays a fork-active proposal by the millisecond
/// gap, not whole seconds, and the header lands strictly after the latest parent.
///
/// Fork-active voters require a header's millisecond timestamp to exceed every parent's, so the
/// proposer waits for, and clamps to, one millisecond past the latest parent. The clamp alone
/// satisfies the timestamp bound; the wait is what keeps the header from being stamped ahead of
/// this node's clock, and so ahead of its voters' clocks. The latest parent sits mid-list so
/// reading any parent but the maximum fails the clamp. With tokio's clock paused the wait costs no
/// real time, so the header is stamped while the wall clock still trails the parent and the clamp
/// decides the timestamp. The wait is bounded from both sides: a skipped wait falls short of the
/// lead the latest parent still held over the wall clock when the proposal returned, and a wait
/// rounded up to whole seconds overshoots the gap from `base`. A missing clamp or missing `+ 1`
/// fails the timestamp bound.
#[tokio::test(start_paused = true)]
async fn test_proposal_waits_milliseconds_for_future_parents() {
    pin_subsecond_fork(true);
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    assert!(subsecond_timestamp_active(committee.epoch()), "the pin must activate the fork");

    let base = now_ms();
    // the lead has to outlast the real time the proposal takes (it signs the seed) even on a
    // loaded machine, or nothing is left for the lower bound on the wait to check. it stays under
    // a second so a wait rounded up to whole seconds still overshoots
    let latest_parent = base.saturating_add_millis(300);
    let parents = parents_created_at(
        &fixture,
        &[
            base.saturating_add_millis(100),
            latest_parent,
            base.saturating_add_millis(200),
            base.saturating_add_millis(50),
        ],
    );

    let (header, waited) = propose_round_two(primary, &committee, parents).await;
    let after = now_ms();

    let min_created_at = latest_parent.saturating_add_millis(1);
    assert!(
        header.created_at_ms() >= min_created_at,
        "header created at {} must be at least 1ms after the latest parent {latest_parent}",
        header.created_at_ms(),
    );
    // the proposer reads the wall clock no later than `after` and sleeps out the gap from that
    // reading, so it waits at least as long as `min_created_at` still led the clock at `after`.
    // the bound comes from `after` rather than from checking `created_at <= now_ms()`: a paused
    // tokio clock sleeps without moving the wall clock, so the header is stamped ahead of the wall
    // clock here whether or not the proposer waited
    let min_wait =
        Duration::from_millis(min_created_at.as_millis().saturating_sub(after.as_millis()));
    assert!(
        !min_wait.is_zero(),
        "the parent's lead ran out before the proposal returned at {after}, so the lower bound on \
         the wait would hold with no wait at all; the fixture needs a longer lead"
    );
    assert!(
        waited >= min_wait,
        "proposer waited {waited:?}, less than the {min_wait:?} that {min_created_at} still led \
         the clock by when the proposal returned"
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
/// parent's whole second, millisecond part 0) but still waits out the millisecond gap to that
/// parent, no less and no more.
///
/// Pre-fork parents carry whole seconds, so the latest parent sits on a second boundary ahead of
/// this node's clock. A skipped wait falls short of the lead that parent still held over the wall
/// clock when the proposal returned, and a seconds-granular sleep waits out whole seconds and
/// overshoots the gap from `base` to the boundary.
#[tokio::test(start_paused = true)]
async fn test_pre_fork_proposal_keeps_seconds_rule_with_millisecond_wait() {
    pin_subsecond_fork(false);
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    assert!(!subsecond_timestamp_active(committee.epoch()), "the pin must keep the fork dormant");

    let base = now_ms();
    // two second boundaries ahead, not the next one. late in a second the next boundary can be a
    // few milliseconds away, the proposal's own latency (the seed fork stays active, so it signs)
    // can use up that lead, and the lower bound on the wait would then be zero. two boundaries
    // ahead the lead is over one second and at most two, whatever `base`'s millisecond part is
    let latest_parent = TimestampMs::from_parts(base.secs() + 2, 0);
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
    let after = now_ms();

    assert_eq!(header.created_at_millis(), 0, "a pre-fork header must not carry milliseconds");
    assert!(
        *header.created_at() >= latest_parent.secs(),
        "header second {} must not precede the latest parent's second {}",
        header.created_at(),
        latest_parent.secs(),
    );
    // pre-fork the proposer waits for the latest parent itself, with no `+ 1`. the bound comes
    // from `after` for the reason given in `test_proposal_waits_milliseconds_for_future_parents`
    let min_wait =
        Duration::from_millis(latest_parent.as_millis().saturating_sub(after.as_millis()));
    assert!(
        !min_wait.is_zero(),
        "the parent's lead ran out before the proposal returned at {after}, so the lower bound on \
         the wait would hold with no wait at all; the fixture needs a longer lead"
    );
    assert!(
        waited >= min_wait,
        "proposer waited {waited:?}, less than the {min_wait:?} that {latest_parent} still led \
         the clock by when the proposal returned"
    );
    // the wait is `latest_parent - now_ms()` measured after `base`; tokio's timer may round the
    // deadline up to its next millisecond tick
    let max_wait = Duration::from_millis(latest_parent.as_millis() - base.as_millis() + 1);
    assert!(
        waited <= max_wait,
        "proposer waited {waited:?}, more than the {max_wait:?} gap to the latest parent's second"
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
    proposer.digests.insert(0, w0.iter().map(|digest| queued(*digest, 0)).collect());
    proposer.digests.insert(1, w1.iter().map(|digest| queued(*digest, 1)).collect());

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
    proposer.digests.insert(0, w0.iter().map(|digest| queued(*digest, 0)).collect());

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
        proposer
            .digests
            .insert(worker_id, (0..depth).map(|_| queued(B256::random(), worker_id)).collect());
    });

    let first: Vec<WorkerId> =
        proposer.drain_digests_for_header().iter().map(|digest| digest.worker_id).collect();
    assert_eq!(first, (0..10).collect::<Vec<_>>(), "first header serves lanes 0-9");

    // the next header resumes at lane 10 instead of restarting at lane 0
    let second: Vec<WorkerId> =
        proposer.drain_digests_for_header().iter().map(|digest| digest.worker_id).collect();
    assert_eq!(second, vec![10, 11, 0], "the cursor rotates the walk to the unserved lanes");
}

/// One unsigned parent certificate at `round` and `epoch` for each of the first `count`
/// authorities in fixture order.
///
/// The proposer takes its parents as already certified and reads only their epoch, round and
/// origin when deciding whether to advance, so these carry no votes.
fn parents_at(
    fixture: &CommitteeFixture<MemDatabase>,
    round: Round,
    epoch: Epoch,
    count: usize,
) -> Vec<Certificate> {
    let committee = fixture.committee();
    fixture
        .authorities()
        .take(count)
        .map(|authority| {
            let header = HeaderBuilder::default()
                .author(authority.id())
                .round(round)
                .epoch(epoch)
                .parents(BTreeSet::new())
                .build();
            Certificate::new_unsigned_for_test(&committee, header, Vec::new())
                .expect("unsigned parent certificate")
        })
        .collect()
}

/// An epoch 9 proposer at round 0 with genesis parents, plus its committee fixture and bus.
fn proposer_in_epoch_nine(
    task_manager: &TaskManager,
) -> (CommitteeFixture<MemDatabase>, ConsensusBus, Proposer<MemDatabase>) {
    let fixture = CommitteeFixture::builder(MemDatabase::default).epoch(9).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let cb = ConsensusBus::new();
    let proposer = Proposer::new(
        config.clone(),
        config.authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee, LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );
    (fixture, cb, proposer)
}

/// A full quorum of parents from the previous epoch must not move a freshly started proposer.
///
/// Rounds restart every epoch. A certificate store that still holds epoch 8's rows when epoch 9
/// starts replays epoch 8's last round to the proposer as a quorum of parents. Following it jumps
/// the round from 0 to 21 and proposes an epoch 9 header with epoch 8 parents, which every voter
/// rejects. The same quorum built in epoch 9 does advance the round, so the stale set is refused
/// for its epoch and nothing else.
#[tokio::test]
async fn test_parents_from_another_epoch_do_not_advance_round() {
    let task_manager = TaskManager::default();
    let (fixture, cb, mut proposer) = proposer_in_epoch_nine(&task_manager);
    let genesis: Vec<_> = proposer.last_parents.iter().map(|cert| cert.digest()).collect();
    let all = fixture.authorities().count();

    proposer.process_parents(parents_at(&fixture, 21, 8, all), 21).unwrap();
    assert_eq!(proposer.round, 0, "stale-epoch parents must not advance the round");
    assert_eq!(cb.app().primary_round(), 0, "stale-epoch parents must not publish a round");
    assert_eq!(
        proposer.last_parents.iter().map(|cert| cert.digest()).collect::<Vec<_>>(),
        genesis,
        "stale-epoch parents must not replace the genesis parents"
    );

    let current = parents_at(&fixture, 21, 9, all);
    proposer.process_parents(current.clone(), 21).unwrap();
    assert_eq!(proposer.round, 21, "a current-epoch quorum advances the round");
    assert_eq!(cb.app().primary_round(), 21);
    assert_eq!(proposer.last_parents, current);
}

/// A batch that mixes epochs advances the round only when its current-epoch part is a quorum on
/// its own, and only that part becomes the next header's parents.
///
/// The aggregator counts certificates by origin, regardless of epoch, so a quorum it reports can
/// lean on stale members. Of four authorities, one current-epoch certificate beside three stale
/// ones is below the quorum of three and must leave the round alone; three beside one stale one
/// is a quorum and advances it without the stale parent.
#[tokio::test]
async fn test_mixed_epoch_parents_need_a_current_epoch_quorum() {
    let task_manager = TaskManager::default();
    let (fixture, _cb, mut proposer) = proposer_in_epoch_nine(&task_manager);
    let committee = fixture.committee();
    assert_eq!(committee.size(), 4, "the thresholds below assume four authorities");

    let stale = parents_at(&fixture, 5, 8, 4);
    let current = parents_at(&fixture, 5, 9, 4);

    let below_quorum =
        vec![current[0].clone(), stale[1].clone(), stale[2].clone(), stale[3].clone()];
    proposer.process_parents(below_quorum, 5).unwrap();
    assert_eq!(proposer.round, 0, "one current-epoch parent is not a quorum");

    let quorum = vec![current[0].clone(), current[1].clone(), current[2].clone(), stale[3].clone()];
    proposer.process_parents(quorum, 5).unwrap();
    assert_eq!(proposer.round, 5, "three current-epoch parents are a quorum");
    assert_eq!(proposer.last_parents, current[..3], "the stale parent is not kept");
}

// ---- bench5 f3-rejoin ----

/// A proposer for `config`'s authority on `cb`, built the way the primary builds it once
/// `prime_consensus` has primed the round watches.
fn proposer_on(
    config: &ConsensusConfig<MemDatabase>,
    committee: &Committee,
    cb: &ConsensusBus,
    task_manager: &TaskManager,
) -> Proposer<MemDatabase> {
    Proposer::new(
        config.clone(),
        config.authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    )
}

/// A bus whose `primary_round` watch is primed at `round`, as `prime_consensus` leaves it for a
/// validator promoted back into the committee at the network head.
fn bus_primed_at(round: Round) -> ConsensusBus {
    let cb = ConsensusBus::new();
    cb.app().primary_round_updates().send_replace(round);
    cb
}

/// `Proposer::new` never starts below the primed `primary_round`, and parents replayed from an
/// older round move neither the round nor the watch backwards.
///
/// The certificate manager replays the store's last two rounds to the proposer at startup. On a
/// validator promoted mid-epoch those rounds predate its demotion. A proposer starting at round 0
/// took them through the future-round branch and wrote the watch back to them, then proposed far
/// below the network head and stored that header as `LastProposed`.
#[tokio::test]
async fn test_proposer_starts_at_primary_round_above_recovered_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let epoch = committee.epoch();
    let task_manager = TaskManager::default();

    // the header stored before the demotion sits below the primed head
    config
        .node_storage()
        .write_last_proposed(&build_header_at(&primary.id(), 7, epoch, &[]))
        .unwrap();
    let cb = bus_primed_at(30);
    let mut proposer = proposer_on(&config, &committee, &cb, &task_manager);
    assert_eq!(proposer.round, 30, "the round starts at the primed head, not at 0 or P - 1");
    assert!(proposer.last_parents.is_empty(), "genesis is not a parent set for round 31");

    let all = fixture.authorities().count();
    proposer.process_parents(parents_at(&fixture, 4, epoch, all), 4).unwrap();
    assert_eq!(proposer.round, 30, "parents replayed from an older round are ignored");
    assert_eq!(cb.app().primary_round(), 30, "the round watch never moves backwards");

    // the restart premise still holds: a stored header above the primed round seeds P - 1
    config
        .node_storage()
        .write_last_proposed(&build_header_at(&primary.id(), 40, epoch, &[]))
        .unwrap();
    assert_eq!(
        proposer_on(&config, &committee, &bus_primed_at(30), &task_manager).round,
        39,
        "a stored header above the primed round still seeds P - 1"
    );
}

/// The max-delay tick re-sends `LastProposed` only when it is at or above the proposer's round.
///
/// A promoted validator keeps the header it stored before its demotion: the record is the
/// equivocation guard for that round and only the epoch close clears it. Re-sent at the network
/// head it is refused by every voter as too old, so it is left stored and not sent. A header at
/// the proposer's round, or the round-`P` header a restart leaves one round above it, is re-sent.
#[tokio::test(start_paused = true)]
async fn test_max_delay_resend_skips_last_proposed_below_the_round() {
    // a stale record below the primed round: stored, never re-sent
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let epoch = committee.epoch();
    let max_delay = config.parameters().max_header_delay;
    let task_manager = TaskManager::default();

    let stale = build_header_at(&primary.id(), 5, epoch, &[B256::random()]);
    config.node_storage().write_last_proposed(&stale).unwrap();
    let cb = bus_primed_at(20);
    let mut rx_headers = cb.subscribe_headers();
    let proposer = proposer_on(&config, &committee, &cb, &task_manager);
    assert_eq!(
        proposer.resendable_last_proposed(),
        None,
        "a header below the round is not re-sent"
    );
    proposer.spawn(&task_manager);
    assert!(
        tokio::time::timeout(max_delay * 3, rx_headers.recv()).await.is_err(),
        "three max-delay ticks without parents must not send the stale header"
    );
    assert_eq!(
        config.node_storage().get_last_proposed().unwrap(),
        Some(stale),
        "the equivocation guard record is kept"
    );

    // a record at the proposer's round: re-sent byte-identical on the tick
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let task_manager = TaskManager::default();

    let current = build_header_at(&primary.id(), 20, epoch, &[B256::random()]);
    config.node_storage().write_last_proposed(&current).unwrap();
    let cb = bus_primed_at(20);
    let mut rx_headers = cb.subscribe_headers();
    let proposer = proposer_on(&config, &committee, &cb, &task_manager);
    assert_eq!(proposer.round, 20);
    assert_eq!(proposer.resendable_last_proposed(), Some(current.clone()));
    proposer.spawn(&task_manager);
    let resent = tokio::time::timeout(max_delay * 3, rx_headers.recv())
        .await
        .expect("the header at the proposer's round is re-sent on the tick")
        .expect("header channel stayed open");
    assert_eq!(resent, current, "the re-sent header is the stored one");

    // a restart: the stored round-P header sits one round above the seeded P - 1. a fresh
    // fixture keeps this record out of the storage the proposer spawned above still reads
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let restart = build_header_at(&primary.id(), 21, epoch, &[B256::random()]);
    config.node_storage().write_last_proposed(&restart).unwrap();
    let proposer = proposer_on(&config, &committee, &bus_primed_at(20), &task_manager);
    assert_eq!(proposer.round, 20, "a restart seeds P - 1");
    assert_eq!(proposer.resendable_last_proposed(), Some(restart), "the restart re-send is kept");
}

/// A proposer seeded above round 0 proposes nothing until parents of its own round arrive.
///
/// A promoted validator's proposer used to start with the genesis certificates as parents, and
/// its first max-delay tick proposed `primary_round + 1` on them: a header every voter rejects
/// with `InvalidParentRound`. It now starts with no parents, and the guard refuses a parent set of
/// any other round, genesis included.
#[tokio::test(start_paused = true)]
async fn test_seeded_round_waits_for_parents_of_that_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let epoch = committee.epoch();
    let max_delay = config.parameters().max_header_delay;
    let task_manager = TaskManager::default();

    let cb = bus_primed_at(20);
    let mut rx_headers = cb.subscribe_headers();
    let mut proposer = proposer_on(&config, &committee, &cb, &task_manager);
    assert_eq!(proposer.round, 20);

    // the guard on its own: genesis parents never build a header above round 1
    proposer.last_parents = Certificate::genesis(&committee);
    assert!(!proposer.has_parents_for_round(), "genesis parents must not build round 21");
    proposer.last_parents.clear();
    assert!(!proposer.has_parents_for_round(), "no parents, no proposal");

    proposer.spawn(&task_manager);
    assert!(
        tokio::time::timeout(max_delay * 3, rx_headers.recv()).await.is_err(),
        "no header may be proposed before parents at the seeded round arrive"
    );

    let parents = parents_at(&fixture, 20, epoch, fixture.authorities().count());
    cb.parents().send((parents.clone(), 20)).await.unwrap();
    let header = tokio::time::timeout(max_delay * 3, rx_headers.recv())
        .await
        .expect("parents at the seeded round lead to a proposal")
        .expect("header channel stayed open");
    assert_eq!(header.round(), 21, "the proposal builds on the seeded round");
    assert_eq!(
        header.parents(),
        &parents.iter().map(|parent| parent.digest()).collect::<BTreeSet<_>>(),
        "the proposal's parents are the certificates of the seeded round"
    );
}

/// The epoch start is unchanged: round 0, the genesis certificates as parents, round 1 proposed
/// on the first tick.
#[tokio::test]
async fn test_epoch_start_proposes_round_one_on_genesis_parents() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let task_manager = TaskManager::default();

    // reset_for_epoch and prime_consensus leave the round watch at 0 at an epoch start
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let proposer = proposer_on(&config, &committee, &cb, &task_manager);
    let genesis = Certificate::genesis(&committee);
    assert_eq!(proposer.round, 0);
    assert_eq!(proposer.last_parents, genesis, "the epoch starts on the genesis certificates");
    assert!(proposer.has_parents_for_round(), "genesis parents build round 1");

    proposer.spawn(&task_manager);
    let header = tokio::time::timeout(Duration::from_secs(5), rx_headers.recv())
        .await
        .expect("the epoch start proposes before the timeout")
        .expect("header channel stayed open");
    assert_eq!(header.round(), 1, "the epoch starts at round 1");
    assert_eq!(
        header.parents(),
        &genesis.iter().map(|parent| parent.digest()).collect::<BTreeSet<_>>(),
        "round 1 builds on the genesis certificates"
    );
    assert!(header.validate(&committee).is_ok());
}

// ---- bench5 land-path-obs ----

/// A digest of `worker_id` that arrived from its worker just now.
fn queued(digest: BlockHash, worker_id: WorkerId) -> ProposerDigest {
    ProposerDigest { digest, worker_id, received: Instant::now() }
}

/// A [`ProposalTrigger`] for tests that read neither the proposal's reason nor its counter.
fn untracked_trigger() -> ProposalTrigger {
    ProposalTrigger {
        reason: ProposeReason::MinDelay,
        pending_before: 0,
        proposed: Counter::noop(),
    }
}

/// Each propose-condition branch reports its own reason, in the order the run loop checks them:
/// the max delay first whatever the digests, then the digest threshold, then the min delay.
///
/// Catches: a swapped branch, or the threshold winning over an expired max delay, either of which
/// files headers under the wrong `reason` in the stage analysis.
#[test]
fn test_propose_reason_names_each_condition_branch() {
    assert_eq!(ProposeReason::select(true, true), ProposeReason::MaxDelay);
    assert_eq!(ProposeReason::select(true, false), ProposeReason::MaxDelay);
    assert_eq!(ProposeReason::select(false, true), ProposeReason::ParentsAndThreshold);
    assert_eq!(ProposeReason::select(false, false), ProposeReason::MinDelay);
    assert_eq!(ProposeReason::MaxDelay.as_str(), "max_delay");
    assert_eq!(ProposeReason::ParentsAndThreshold.as_str(), "parents_and_threshold");
    assert_eq!(ProposeReason::MinDelay.as_str(), "min_delay");
}

/// A proposal carries the reason `propose_next_header` was given to its `header proposed` line and
/// to the `headers_proposed_total` counter of that reason. The line also reports the header's own
/// creation time, its digests as 0x-prefixed hex in payload order, the oldest digest's wait and
/// the queue depth before the header took its share; the oldest wait is also a
/// `digest_wait_seconds` sample.
///
/// Catches: a reason dropped or replaced on its way to the spawned proposal, a counter of the wrong
/// reason, a digest wait measured from the newest digest or from the proposal instead of the
/// digest's arrival, `pending_before` read after the drain, and digests printed in any other form
/// than the hex the stage tool joins on.
#[tokio::test]
async fn test_header_proposed_reports_reason_digests_and_digest_wait() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let task_manager = TaskManager::default();
    // both resolve their metric handles when built, so both are built under the local recorder
    let cb = metrics::with_local_recorder(&recorder, ConsensusBus::new);
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = metrics::with_local_recorder(&recorder, || {
        Proposer::new(
            config.clone(),
            primary.id(),
            cb.clone(),
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            task_manager.get_spawner(),
        )
    });
    assert_eq!(proposer.max_header_num_of_batches, 10, "test assumes the default header cap");

    let delay = Duration::from_millis(80);
    let mut headers = Vec::new();
    for reason in ProposeReason::ALL {
        // twelve digests on one worker: the oldest arrived `delay` ago, and the header takes ten
        let delayed = Instant::now().checked_sub(delay).expect("the clock runs past the delay");
        let queue: VecDeque<_> = (0..12)
            .map(|i| ProposerDigest {
                digest: B256::random(),
                worker_id: 0,
                received: if i == 0 { delayed } else { Instant::now() },
            })
            .collect();
        proposer.digests = BTreeMap::from([(0, queue)]);
        let header = proposer
            .propose_next_header(reason)
            .expect("proposal spawned")
            .await
            .expect("proposal task reported")
            .expect("header proposed");
        assert_eq!(header.payload().len(), 10);
        headers.push((reason, header));
    }

    let lines = capture.events("header proposed");
    assert_eq!(lines.len(), headers.len(), "one line per proposed header");
    for ((reason, header), line) in headers.iter().zip(&lines) {
        assert_eq!(line.field("reason"), Some(reason.as_str()));
        assert_eq!(line.u64_field("round"), Some(u64::from(header.round())));
        assert_eq!(line.u64_field("num_batches"), Some(10));
        assert_eq!(line.u64_field("pending_before"), Some(12), "queue depth before the drain");
        assert_eq!(line.u64_field("created_at_ms"), Some(header.created_at_ms().as_millis()));
        let wait = line.u64_field("oldest_digest_wait_ms").expect("oldest digest wait");
        assert!(wait >= 80, "{} waited {wait} ms, less than the oldest digest", reason.as_str());
        let expected: Vec<String> = header.payload().keys().map(ToString::to_string).collect();
        assert_eq!(line.field("digests"), Some(format!("[{}]", expected.join(",")).as_str()));
        assert!(expected.iter().all(|d| d.len() == 66 && d.starts_with("0x")), "0x + 64 hex chars");
        assert_eq!(expected[0], expected[0].to_lowercase(), "lowercase hex");
    }

    let snapshot = snapshotter.snapshot().into_vec();
    for reason in ProposeReason::ALL {
        let value = snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == "tn_primary.headers_proposed_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "reason" && label.value() == reason.as_str())
            })
            .map(|(.., value)| value)
            .unwrap_or_else(|| panic!("no headers_proposed_total for {}", reason.as_str()));
        assert_eq!(value, &DebugValue::Counter(1), "{}", reason.as_str());
    }
    let samples = snapshot
        .iter()
        .find_map(|(key, _, _, value)| match value {
            DebugValue::Histogram(samples)
                if key.key().name() == "tn_primary.digest_wait_seconds" =>
            {
                Some(samples.clone())
            }
            _ => None,
        })
        .expect("digest_wait_seconds recorded");
    assert_eq!(samples.len(), ProposeReason::ALL.len(), "one sample per header with digests");
    assert!(samples.iter().all(|s| s.0 >= delay.as_secs_f64()), "the oldest digest's wait");
}

/// Re-queuing the digests of an uncommitted own header logs one `digests resent` line with the
/// number of digests, the oldest re-queued header's round and the proposer's round, and restarts
/// the re-queued digests' wait. An uncommitted header without digests re-sends nothing and logs
/// nothing.
///
/// Catches: a missing or per-digest line, a count or round off the re-queued headers, and a
/// re-queued digest keeping a stale or zero arrival time.
#[tokio::test]
async fn test_digests_resent_line_on_requeue() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = primary.id();
    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let mut proposer = proposer_on(&primary.consensus_config(), &committee, &cb, &task_manager);
    proposer.round = 7;

    let (d3, d4a, d4b) = (B256::random(), B256::random(), B256::random());
    proposer.proposed_headers.insert(3, build_test_header(&author, 3, &[d3]));
    proposer.proposed_headers.insert(4, build_test_header(&author, 4, &[d4a, d4b]));
    proposer.proposed_headers.insert(5, build_test_header(&author, 5, &[B256::random()]));
    proposer.proposed_headers.insert(6, build_test_header(&author, 6, &[]));

    // round 5 commits: rounds 3 and 4 fall behind it uncommitted, round 6 is still in flight
    let before = Instant::now();
    proposer.process_committed_headers(6, vec![5]);

    let lines = capture.events("digests resent");
    assert_eq!(lines.len(), 1, "one line per re-queue");
    assert_eq!(lines[0].u64_field("count"), Some(3));
    assert_eq!(lines[0].u64_field("from_round"), Some(3));
    assert_eq!(lines[0].u64_field("round"), Some(7));
    let requeued: Vec<_> = proposer.digests.values().flatten().collect();
    assert_eq!(requeued.iter().map(|d| d.digest).collect::<Vec<_>>(), [d3, d4a, d4b]);
    assert!(requeued.iter().all(|d| d.received >= before), "the wait restarts at the re-queue");

    // round 7 commits: only the empty round-6 header falls behind, so nothing is re-sent
    proposer.digests.clear();
    proposer.process_committed_headers(7, vec![7]);
    assert!(proposer.digests.is_empty());
    assert_eq!(capture.events("digests resent").len(), 1, "an empty header logs no re-send");
}

// ---- bench5 cert-requeue ----

/// A spawned epoch-start proposer whose round-1 header carries `first` on worker 0, with its bus,
/// its header receiver and that header. The bus and the proposer resolve their metric handles
/// under `recorder`.
async fn spawn_with_first_header(
    fixture: &CommitteeFixture<MemDatabase>,
    task_manager: &TaskManager,
    recorder: &metrics_util::debugging::DebuggingRecorder,
    first: &[BlockHash],
) -> (ConsensusBus, impl TnReceiver<Header>, Header) {
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let cb = metrics::with_local_recorder(recorder, ConsensusBus::new);
    let mut rx_headers = cb.subscribe_headers();
    let mut proposer = metrics::with_local_recorder(recorder, || {
        proposer_on(&config, &committee, &cb, task_manager)
    });
    proposer.digests = BTreeMap::from([(0, first.iter().map(|d| queued(*d, 0)).collect())]);
    proposer.spawn(task_manager);
    let header = next_header_at(&mut rx_headers, 1).await;
    assert_eq!(payload_digests(&header), first, "the first header carries the queued digests");
    (cb, rx_headers, header)
}

/// The next header at `round` from `rx_headers`, past the max-delay re-sends of earlier rounds.
async fn next_header_at(rx_headers: &mut impl TnReceiver<Header>, round: Round) -> Header {
    loop {
        let header = tokio::time::timeout(Duration::from_secs(30), rx_headers.recv())
            .await
            .unwrap_or_else(|_| panic!("no header at round {round} before the timeout"))
            .expect("header channel stayed open");
        assert!(header.round() <= round, "round {} came before round {round}", header.round());
        if header.round() == round {
            return header;
        }
    }
}

/// A header's batch digests in payload order.
fn payload_digests(header: &Header) -> Vec<BlockHash> {
    header.payload().keys().copied().collect()
}

/// Queue `digest` on worker 0 through the bus and wait until the proposer has taken it.
async fn send_digest(cb: &ConsensusBus, digest: BlockHash) {
    let (ack_channel, ack) = oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id: 0, ack_channel })
        .await
        .expect("digest queued");
    ack.await.expect("the proposer acks the digest");
}

/// `tn_primary_digests_requeued_total` per cause, in [`RequeueCause::ALL`] order; `None` for a
/// cause without a registered counter.
fn requeued_totals(snapshotter: &metrics_util::debugging::Snapshotter) -> [Option<u64>; 2] {
    let snapshot = snapshotter.snapshot().into_vec();
    RequeueCause::ALL.map(|cause| {
        snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == "tn_primary.digests_requeued_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "cause" && label.value() == cause.as_str())
            })
            .and_then(|(.., value)| match value {
                metrics_util::debugging::DebugValue::Counter(total) => Some(*total),
                _ => None,
            })
    })
}

/// The certifier's report that an own header was cancelled puts that header's digests at the front
/// of the queue, so the next header carries them ahead of newer digests: the header after the one
/// that cancelled it, not the one after the next own commit. The re-queue logs one `digests resent`
/// line with `cause=canceled` and counts the digests under that cause.
///
/// Catches: a proposer that does not subscribe to the report or ignores it, digests appended
/// behind newer ones, a missing or misattributed `cause`, and a per-digest line.
#[tokio::test(start_paused = true)]
async fn test_canceled_header_digests_ride_the_next_header() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let epoch = fixture.committee().epoch();
    let all = fixture.authorities().count();
    let task_manager = TaskManager::default();

    let (d1, d2, d3, d4) = (B256::random(), B256::random(), B256::random(), B256::random());
    let (cb, mut rx_headers, canceled) =
        spawn_with_first_header(&fixture, &task_manager, &recorder, &[d1, d2]).await;

    // round 2 cancels round 1's vote collection in the certifier
    send_digest(&cb, d3).await;
    cb.parents().send((parents_at(&fixture, 1, epoch, all), 1)).await.expect("parents sent");
    let newer = next_header_at(&mut rx_headers, 2).await;
    assert_eq!(payload_digests(&newer), [d3]);

    // the certifier's report, then a digest that arrives after it
    cb.uncertified_own_headers()
        .try_send((canceled.round(), canceled.digest()))
        .expect("report queued");
    send_digest(&cb, d4).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while capture.events("digests resent").is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the proposer re-queues the cancelled header's digests");

    cb.parents().send((parents_at(&fixture, 2, epoch, all), 2)).await.expect("parents sent");
    let next = next_header_at(&mut rx_headers, 3).await;
    assert_eq!(payload_digests(&next), [d1, d2, d4], "re-queued digests ride ahead of newer ones");

    let lines = capture.events("digests resent");
    assert_eq!(lines.len(), 1, "one line per re-queue");
    assert_eq!(lines[0].u64_field("count"), Some(2));
    assert_eq!(lines[0].u64_field("from_round"), Some(1));
    assert_eq!(lines[0].u64_field("round"), Some(2));
    assert_eq!(lines[0].field("cause"), Some("canceled"));
    assert_eq!(requeued_totals(&snapshotter), [Some(2), Some(0)]);
}

/// A header re-sent by the max-delay tick is not cancelled and keeps its digests: the next header
/// carries none of them, and nothing is re-queued.
///
/// Catches: a re-send path that re-queues the digests of a header that may still be certified,
/// which would put them in two headers.
#[tokio::test(start_paused = true)]
async fn test_max_delay_resend_keeps_digests_out_of_the_next_header() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let epoch = fixture.committee().epoch();
    let all = fixture.authorities().count();
    let task_manager = TaskManager::default();

    let (d1, d2) = (B256::random(), B256::random());
    let (cb, mut rx_headers, first) =
        spawn_with_first_header(&fixture, &task_manager, &recorder, &[d1, d2]).await;

    // no parents: the max-delay tick re-sends the stored header unchanged
    let resent = tokio::time::timeout(Duration::from_secs(30), rx_headers.recv())
        .await
        .expect("the max-delay tick re-sends the header")
        .expect("header channel stayed open");
    assert_eq!(resent, first, "the re-send is the stored header");

    cb.parents().send((parents_at(&fixture, 1, epoch, all), 1)).await.expect("parents sent");
    let next = next_header_at(&mut rx_headers, 2).await;
    assert!(payload_digests(&next).is_empty(), "the re-sent header keeps its digests");
    assert!(capture.events("digests resent").is_empty(), "a re-send re-queues nothing");
    assert_eq!(requeued_totals(&snapshotter), [Some(0), Some(0)]);
}

/// The two re-queue paths move each uncommitted header's digests once. A cancelled header leaves
/// the tracked headers, so a later own commit does not re-queue it again; a header the commit path
/// already re-queued ignores a late cancel report. A report for a header still at or above the
/// proposer's round (one the max-delay tick may re-send) or with another digest moves nothing. The
/// commit path still re-queues with `cause=committed`.
///
/// Catches: a digest re-queued by both paths (two headers carry it), a report acted on for a
/// header that may still be certified, and the commit path losing its re-queue or its cause.
#[tokio::test]
async fn test_requeue_causes_move_each_header_once() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = primary.id();
    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let mut proposer = metrics::with_local_recorder(&recorder, || {
        proposer_on(&primary.consensus_config(), &committee, &cb, &task_manager)
    });
    proposer.round = 5;

    let (a, b1, b2, c, e) =
        (B256::random(), B256::random(), B256::random(), B256::random(), B256::random());
    let h2 = build_test_header(&author, 2, &[a]);
    let h3 = build_test_header(&author, 3, &[b1, b2]);
    let h4 = build_test_header(&author, 4, &[c]);
    let h5 = build_test_header(&author, 5, &[e]);
    for header in [&h2, &h3, &h4, &h5] {
        proposer.proposed_headers.insert(header.round(), header.clone());
    }
    let queue = |proposer: &Proposer<MemDatabase>| -> Vec<BlockHash> {
        proposer.digests.values().flatten().map(|d| d.digest).collect()
    };

    // a cancel moves round 3's digests and stops tracking the header
    proposer.process_uncertified_header(3, h3.digest());
    assert_eq!(queue(&proposer), [b1, b2]);
    assert!(!proposer.proposed_headers.contains_key(&3));

    // a repeated report, a digest mismatch and the proposer's own round move nothing
    proposer.process_uncertified_header(3, h3.digest());
    proposer.process_uncertified_header(4, h2.digest());
    proposer.process_uncertified_header(5, h5.digest());
    assert_eq!(queue(&proposer), [b1, b2]);
    assert!(
        proposer.proposed_headers.contains_key(&4) && proposer.proposed_headers.contains_key(&5)
    );

    // round 4 commits: round 2 fell behind it uncommitted and goes ahead of the queue; round 3 is
    // not re-queued a second time
    proposer.process_committed_headers(6, vec![4]);
    assert_eq!(queue(&proposer), [a, b1, b2]);

    // a cancel report for the header the commit path re-queued moves nothing
    proposer.process_uncertified_header(2, h2.digest());
    assert_eq!(queue(&proposer), [a, b1, b2]);

    let lines = capture.events("digests resent");
    assert_eq!(lines.len(), 2, "one line per re-queue that moved digests");
    assert_eq!(lines[0].field("cause"), Some("canceled"));
    assert_eq!(lines[0].u64_field("count"), Some(2));
    assert_eq!(lines[0].u64_field("from_round"), Some(3));
    assert_eq!(lines[1].field("cause"), Some("committed"));
    assert_eq!(lines[1].u64_field("count"), Some(1));
    assert_eq!(lines[1].u64_field("from_round"), Some(2));
    assert_eq!(lines[1].u64_field("round"), Some(5));
    assert_eq!(requeued_totals(&snapshotter), [Some(2), Some(1)]);
}
