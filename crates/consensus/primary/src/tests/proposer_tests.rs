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
        exec_off(&committee, &primary.id()),
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
        exec_off(committee, &primary.id()),
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
    let now = Instant::now();
    ProposerDigest { digest, worker_id, received: now, first_received: now, requeued: false }
}

/// A [`ProposalTrigger`] for tests that read neither the proposal's reason nor its counter.
fn untracked_trigger() -> ProposalTrigger {
    ProposalTrigger {
        reason: ProposeReason::MinDelay,
        pending_before: 0,
        proposed: Counter::noop(),
        grace: Duration::ZERO,
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
/// creation time, its digests as 0x-prefixed hex in payload order, the oldest digest's wait, how
/// many of its digests were re-queued and the queue depth before the header took its share; the
/// oldest wait is also a `digest_wait_seconds` sample.
///
/// Catches: a reason dropped or replaced on its way to the spawned proposal, a counter of the wrong
/// reason, a digest wait measured from the newest digest or from the proposal instead of the
/// digest's arrival, a `requeued` count taken over the queue instead of the header,
/// `pending_before` read after the drain, and digests printed in any other form than the hex the
/// stage tool joins on.
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
        // twelve digests on one worker: the oldest arrived `delay` ago, and the header takes ten.
        // the second and the last were re-queued, and only the second rides this header
        let delayed = Instant::now().checked_sub(delay).expect("the clock runs past the delay");
        let queue: VecDeque<_> = (0..12)
            .map(|i| {
                let received = if i == 0 { delayed } else { Instant::now() };
                ProposerDigest {
                    digest: B256::random(),
                    worker_id: 0,
                    received,
                    first_received: received,
                    requeued: i == 1 || i == 11,
                }
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
        assert_eq!(line.u64_field("requeued"), Some(1), "re-queued digests in this header only");
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
/// number of digests, the oldest re-queued header's round and the proposer's round, restarts the
/// re-queued digests' wait and marks them as re-queued. An uncommitted header without digests
/// re-sends nothing and logs nothing.
///
/// Catches: a missing or per-digest line, a count or round off the re-queued headers, a re-queued
/// digest keeping a stale or zero arrival time, and one the next `header proposed` line would not
/// count as re-queued.
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
    assert!(requeued.iter().all(|d| d.requeued), "a re-queued digest is marked as one");

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

// ---- bench6 quorum-exec-block ----

/// [`ExecBlockInputs`] with the `quorum_exec_block` switch off, for tests that drive
/// [`Proposer::propose_header`] directly: the header names the own tip exactly as before.
fn exec_off(committee: &Committee, author: &AuthorityIdentifier) -> ExecBlockInputs {
    ExecBlockInputs::new(ExecPolicy::Off, committee, author, &[])
}

/// The final block of executed output `output` in the quorum-exec-block models: three blocks per
/// output, so ring entries and block numbers differ.
fn exec_output_block(output: u64) -> tn_types::SealedHeader {
    tn_types::SealedHeader::seal_slow(tn_types::ExecHeader {
        number: 100 + 3 * output,
        ..Default::default()
    })
}

/// Push executed output `output` onto `ring` and return its final block.
fn push_exec_output(ring: &mut RecentBlocks, output: u64) -> BlockNumHash {
    let block = exec_output_block(output);
    let num_hash = block.num_hash();
    let consensus =
        tn_types::ConsensusNumHash::new(output, tn_types::ConsensusHeaderDigest::from([7u8; 32]));
    ring.push_latest(Round::try_from(output).expect("small model"), consensus, Some(block));
    num_hash
}

/// A ring of capacity 50 (a `gc_depth`-sized ring) holding outputs `0..outputs`: block numbers
/// 100, 103, 106, ...
fn exec_ring(outputs: u64) -> RecentBlocks {
    let mut ring = RecentBlocks::new(50);
    for output in 0..outputs {
        push_exec_output(&mut ring, output);
    }
    ring
}

/// The quorum-rule inputs of authority 0 in a ten-member, equal-power committee of epoch 3
/// (quorum 7), on `parents`.
fn table_inputs(policy: ExecPolicy, parents: Vec<ParentExecBlock>) -> ExecBlockInputs {
    ExecBlockInputs {
        policy,
        author: AuthorityIdentifier::dummy_for_test(0),
        own_power: 1,
        epoch: 3,
        quorum_threshold: 7,
        parents,
    }
}

/// A round-5, epoch-3 parent of power 1 by authority `id` naming block `number`.
fn table_parent(id: u8, number: BlockNumber) -> ParentExecBlock {
    ParentExecBlock {
        author: AuthorityIdentifier::dummy_for_test(id),
        power: 1,
        epoch: 3,
        round: 5,
        number,
    }
}

/// `count` parents by authorities `first..first + count`, each naming `number`.
fn table_parents(first: u8, count: u8, number: BlockNumber) -> Vec<ParentExecBlock> {
    (first..first + count).map(|id| table_parent(id, number)).collect()
}

/// The quorum rule, case by case, on a 20-output ring (blocks 100, 103, ..., 157; the floor entry
/// eight back is 133) for authority 0 of a ten-member committee (quorum 7).
///
/// Catches: a rank by count instead of power, a missing cap or floor, a floor counted in block
/// numbers, other-epoch, genesis, own or repeated parents counted, a hostile number moving the
/// choice past the cap or the floor, and an empty or short ring mishandled. (The parents carry no
/// hash here; `test_quorum_exec_block_switch_off_keeps_header_bytes` checks the header's hash is
/// the own ring's.)
#[test]
fn test_quorum_exec_block_choice_table() {
    let ring = exec_ring(20);
    let tip = ring.latest_execution_block_num_hash();
    assert_eq!(tip.number, 157);
    let entry = |number: BlockNumber| ring.newest_at_or_below(number).expect("ring entry");
    let other_epoch = |id: u8, number| ParentExecBlock { epoch: 2, ..table_parent(id, number) };
    let genesis = |id: u8| ParentExecBlock { round: 0, number: 0, ..table_parent(id, 0) };
    let weighted = |id: u8, power, number| ParentExecBlock { power, ..table_parent(id, number) };

    let cases: Vec<(&str, ExecPolicy, Vec<ParentExecBlock>, BlockNumber, ExecBasis)> = vec![
        ("in step", ExecPolicy::Always, table_parents(1, 9, 157), 157, ExecBasis::Tip),
        (
            "ahead by one output",
            ExecPolicy::Always,
            table_parents(1, 9, 154),
            154,
            ExecBasis::Quorum,
        ),
        (
            "ahead by six outputs",
            ExecPolicy::Always,
            table_parents(1, 9, 139),
            139,
            ExecBasis::Quorum,
        ),
        (
            "three parents at the tip are not a quorum with the own tip",
            ExecPolicy::Always,
            [table_parents(1, 3, 157), table_parents(4, 6, 139)].concat(),
            139,
            ExecBasis::Quorum,
        ),
        (
            "six parents at the tip are a quorum with the own tip",
            ExecPolicy::Always,
            [table_parents(1, 6, 157), table_parents(7, 3, 139)].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "own node behind: the cap",
            ExecPolicy::Always,
            table_parents(1, 9, 170),
            157,
            ExecBasis::Tip,
        ),
        (
            "a quorum value between entries resolves to the own entry below it",
            ExecPolicy::Always,
            table_parents(1, 9, 150),
            148,
            ExecBasis::Quorum,
        ),
        (
            "older than the floor",
            ExecPolicy::Always,
            table_parents(1, 9, 120),
            133,
            ExecBasis::Floor,
        ),
        ("exactly the floor", ExecPolicy::Always, table_parents(1, 9, 133), 133, ExecBasis::Quorum),
        (
            "fewer than a quorum of usable parents",
            ExecPolicy::Always,
            table_parents(1, 5, 139),
            157,
            ExecBasis::Tip,
        ),
        (
            "other-epoch and genesis parents are not usable",
            ExecPolicy::Always,
            [
                table_parents(1, 4, 139),
                vec![other_epoch(5, 139), other_epoch(6, 139), genesis(7), genesis(8), genesis(9)],
            ]
            .concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "the own certificate among the parents is skipped",
            ExecPolicy::Always,
            [vec![table_parent(0, 139)], table_parents(1, 5, 139)].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "an author counts once",
            ExecPolicy::Always,
            [table_parents(1, 4, 139), vec![table_parent(1, 139), table_parent(2, 139)]].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "ranked by voting power, not by count",
            ExecPolicy::Always,
            vec![weighted(1, 4, 139), weighted(2, 1, 139), weighted(3, 1, 139)],
            139,
            ExecBasis::Quorum,
        ),
        (
            "f hostile parents naming a huge number do not move the choice",
            ExecPolicy::Always,
            [table_parents(1, 3, u64::MAX), table_parents(4, 6, 151)].concat(),
            151,
            ExecBasis::Quorum,
        ),
        (
            "f low parents beside six honest ones at the tip do not move the choice",
            ExecPolicy::Always,
            [table_parents(1, 3, 0), table_parents(4, 6, 157)].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "f low parents in a minimal parent set drag the choice to the floor and no lower",
            ExecPolicy::Always,
            [table_parents(1, 3, 0), table_parents(4, 3, 157)].concat(),
            133,
            ExecBasis::Floor,
        ),
        (
            "the starved window applies the rule",
            ExecPolicy::Starved,
            table_parents(1, 9, 139),
            139,
            ExecBasis::Quorum,
        ),
        (
            "the tip policy ignores the parents",
            ExecPolicy::Tip,
            table_parents(1, 9, 139),
            157,
            ExecBasis::Tip,
        ),
        (
            "the switch off ignores the parents",
            ExecPolicy::Off,
            table_parents(1, 9, 139),
            157,
            ExecBasis::Tip,
        ),
    ];
    for (name, policy, parents, number, basis) in cases {
        let choice = choose_exec_block(&ring, &table_inputs(policy, parents));
        assert_eq!(choice.block, entry(number), "{name}: the own ring's number and hash");
        assert_eq!(choice.tip, tip, "{name}");
        assert_eq!(choice.basis, basis, "{name}");
    }

    // a short ring floors at its oldest entry
    let short = exec_ring(4);
    let choice =
        choose_exec_block(&short, &table_inputs(ExecPolicy::Always, table_parents(1, 9, 50)));
    assert_eq!(choice.block, short.nth_newest(3).expect("oldest"), "the oldest of four entries");
    assert_eq!((choice.block.number, choice.basis), (100, ExecBasis::Floor));

    // an empty ring keeps the default block the tip read returned before the switch
    let empty = RecentBlocks::new(50);
    let choice =
        choose_exec_block(&empty, &table_inputs(ExecPolicy::Always, table_parents(1, 9, 139)));
    assert_eq!(choice.block, empty.latest_execution_block_num_hash());
    assert_eq!(choice.block.number, 0, "the default header's block");
    assert_eq!(choice.basis, ExecBasis::Tip);
}

/// What one in-step fleet run of the self-feeding model records per round and node: how many
/// outputs the named block trails the tip, the policy and the basis.
type FleetRound = Vec<(usize, ExecPolicy, ExecBasis)>;

/// Ten in-step proposers (one ring stands for all ten: they executed the same outputs), one new
/// output executed before every round, each round's headers the next round's parents. Round 1
/// builds on genesis parents. After round `r`'s proposals, each `(r, node)` in `cancels` reports
/// that node's round-`r - 1` header cancelled; every other node's round-`r` header certifies.
fn self_feeding_fleet(
    starved_rounds: u32,
    rounds: Round,
    cancels: &[(Round, usize)],
) -> Vec<FleetRound> {
    const FLEET: u8 = 10;
    let ids: Vec<_> = (0..FLEET).map(AuthorityIdentifier::dummy_for_test).collect();
    let mut policies = vec![ExecBlockPolicy::new(true, starved_rounds); ids.len()];
    // the fleet has been running: twelve outputs before round 1
    let mut ring = exec_ring(12);
    let mut history: Vec<BlockNumHash> = (0..12).map(|o| exec_output_block(o).num_hash()).collect();
    let mut named = vec![BlockNumHash::default(); ids.len()];
    let mut out = Vec::new();
    for round in 1..=rounds {
        let output = u64::try_from(history.len()).expect("small model");
        history.push(push_exec_output(&mut ring, output));
        let parents: Vec<ParentExecBlock> = ids
            .iter()
            .zip(&named)
            .map(|(author, block)| ParentExecBlock {
                author: author.clone(),
                power: 1,
                epoch: 3,
                // genesis parents for round 1
                round: round - 1,
                number: block.number,
            })
            .collect();
        let mut this_round = Vec::new();
        let mut next_named = Vec::new();
        for (author, policy) in ids.iter().zip(&policies) {
            let inputs = ExecBlockInputs {
                policy: policy.for_round(round),
                author: author.clone(),
                own_power: 1,
                epoch: 3,
                quorum_threshold: 7,
                parents: parents.clone(),
            };
            let choice = choose_exec_block(&ring, &inputs);
            let at = history.iter().position(|b| *b == choice.block).expect("an executed output");
            this_round.push((history.len() - 1 - at, inputs.policy, choice.basis));
            next_named.push(choice.block);
        }
        named = next_named;
        for (node, policy) in policies.iter_mut().enumerate() {
            if cancels.contains(&(round, node)) {
                policy.on_cancelled(round - 1, round);
            } else {
                policy.on_certified(round);
            }
        }
        out.push(this_round);
    }
    out
}

/// I-4 item 1: the choice function fed its own outputs as the next round's parents. An in-step
/// ten-node fleet over 20 rounds settles exactly [`QUORUM_EXEC_BLOCK_FLOOR_ENTRIES`] outputs
/// below its tip under `always` (the quorum value can only rise through the floor), and names its
/// tip under the adaptive policy. Three nodes whose header was cancelled rank the other seven's
/// fresh tips for their four-round window: they trail by the one output executed since the
/// parents were proposed, without ratcheting, and return to the tip when the rounds are spent.
/// With the whole fleet in a window at once the lag climbs as under `always` and drops back to 0
/// when the window ends.
///
/// Catches: a rule that ranks the parents' tips it does not have (the model would show lag 0
/// under `always`), a floor that does not hold the ratchet at N, a window that never closes or
/// returns by value, and a starved node beside tip nodes whose lag grows round by round.
#[test]
fn test_quorum_exec_block_self_feeding_fleet() {
    let n = QUORUM_EXEC_BLOCK_FLOOR_ENTRIES;
    let always = self_feeding_fleet(0, 20, &[]);
    for (round, nodes) in (1..).zip(&always) {
        // round r trails by r - 1 outputs until the floor holds it at N
        let expected = usize::min(round - 1, n);
        for (node, (lag, policy, basis)) in nodes.iter().enumerate() {
            assert_eq!(*policy, ExecPolicy::Always);
            assert_eq!(*lag, expected, "always: round {round}, node {node}");
            if round > n + 1 {
                assert_eq!(*basis, ExecBasis::Floor, "always: round {round} sits on the floor");
            }
        }
    }
    assert!(always[n..].iter().flatten().all(|(lag, ..)| *lag == n), "lag exactly N from round 9");

    let adaptive = self_feeding_fleet(4, 20, &[]);
    assert!(
        adaptive.iter().flatten().all(|entry| *entry == (0, ExecPolicy::Tip, ExecBasis::Tip)),
        "adaptive, in step and certified: every header names its tip"
    );

    // nodes 0-2 learn after round 5 that their round-4 header was cancelled: rounds 6-9 use the
    // rule
    let cancels = [(5, 0), (5, 1), (5, 2)];
    let windowed = self_feeding_fleet(4, 20, &cancels);
    for (round, nodes) in (1..).zip(&windowed) {
        for (node, (lag, policy, basis)) in nodes.iter().enumerate() {
            let starved = node < 3 && (6..=9).contains(&round);
            if starved {
                assert_eq!(
                    (*lag, *policy, *basis),
                    (1, ExecPolicy::Starved, ExecBasis::Quorum),
                    "round {round}, node {node}"
                );
            } else {
                assert_eq!(
                    (*lag, *policy, *basis),
                    (0, ExecPolicy::Tip, ExecBasis::Tip),
                    "round {round}, node {node}"
                );
            }
        }
    }

    // every node learns after round 5 that its round-4 header was cancelled: for the window the
    // parents are earlier picks of the rule, so the lag climbs one output a round as under
    // `always`, and every node is back on its tip when the rounds are spent
    let whole_fleet: Vec<(Round, usize)> = (0..10).map(|node| (5, node)).collect();
    let fleet_window = self_feeding_fleet(4, 20, &whole_fleet);
    for (round, nodes) in (1..).zip(&fleet_window) {
        let expected = if (6..=9).contains(&round) {
            (round - 5, ExecPolicy::Starved, ExecBasis::Quorum)
        } else {
            (0, ExecPolicy::Tip, ExecBasis::Tip)
        };
        for (node, entry) in nodes.iter().enumerate() {
            assert_eq!(*entry, expected, "whole fleet in a window: round {round}, node {node}");
        }
    }
}

/// The adaptive policy's state machine: a cancel opens a window of K rounds after the proposer's
/// round and the policy returns to the tip when they have elapsed; the lagging cancel of the
/// header built before the window is not counted again; repeated cancels double K up to
/// [`QUORUM_EXEC_BLOCK_MAX_STARVED_ROUNDS`]; a certified tip header resets K, a header certified
/// inside a window does not, including the window's last header when a cancel reported at that
/// round extends the run. The switch off and the `always` policy keep no window.
#[test]
fn test_quorum_exec_block_adaptive_policy_state_machine() {
    let mut policy = ExecBlockPolicy::new(true, 4);
    assert!((1..=50).all(|round| policy.for_round(round) == ExecPolicy::Tip));

    // round 3 is cancelled when round 4 is proposed: rounds 5-8 use the rule
    policy.on_cancelled(3, 4);
    let window = |p: &ExecBlockPolicy| -> Vec<Round> {
        (1..=300).filter(|r| p.for_round(*r) == ExecPolicy::Starved).collect()
    };
    assert_eq!(window(&policy), (5..=8).collect::<Vec<_>>());
    assert_eq!(policy.next_window, 8);
    // round 4 named its tip before the window opened: its cancel is the same episode
    policy.on_cancelled(4, 5);
    assert_eq!(window(&policy), (5..=8).collect::<Vec<_>>());
    assert_eq!(policy.next_window, 8);
    // a header certified inside the window does not reset K
    policy.on_certified(6);
    assert_eq!(policy.next_window, 8);

    // the rounds are spent: the round-9 tip probe is cancelled at round 10, so the doubled window
    // covers rounds 11-18
    assert_eq!(policy.for_round(9), ExecPolicy::Tip);
    policy.on_cancelled(9, 10);
    assert_eq!(window(&policy), (11..=18).collect::<Vec<_>>());
    assert_eq!(policy.next_window, 16);

    // a header inside the window cancelled: the window extends from the proposer's round
    policy.on_cancelled(12, 13);
    assert_eq!(window(&policy), (11..=29).collect::<Vec<_>>());
    assert_eq!(policy.next_window, 32);

    // more cancels double K up to the bound and no further
    let mut round = 30;
    for expected in [64, 128, 128, 128] {
        policy.on_cancelled(round, round + 1);
        round = policy.starved_through + 1;
        assert_eq!(policy.next_window, expected);
    }
    assert_eq!(QUORUM_EXEC_BLOCK_MAX_STARVED_ROUNDS, 128);
    let last = policy.starved_through;
    assert_eq!(policy.for_round(last), ExecPolicy::Starved);
    assert_eq!(policy.for_round(last + 1), ExecPolicy::Tip, "returns by rounds elapsed");

    // a certified header that named its tip resets K to the configured value
    policy.on_certified(last + 1);
    assert_eq!(policy.next_window, 4);
    policy.on_cancelled(last + 2, last + 3);
    assert_eq!(policy.for_round(last + 4), ExecPolicy::Starved);
    assert_eq!(policy.for_round(last + 7), ExecPolicy::Starved);
    assert_eq!(policy.for_round(last + 8), ExecPolicy::Tip, "a fresh window of K = 4");

    // round 7, inside the window 5-8, is cancelled when round 8 is proposed: the run extends, so
    // round 8 stays a rule header and its certificate keeps the doubled K
    let mut edge = ExecBlockPolicy::new(true, 4);
    edge.on_cancelled(3, 4);
    edge.on_cancelled(7, 8);
    assert_eq!(window(&edge), (5..=16).collect::<Vec<_>>());
    assert_eq!(edge.next_window, 16);
    edge.on_certified(8);
    assert_eq!(edge.next_window, 16, "a rule header's certificate does not reset K");

    // a configured window above the bound is used as is
    let mut wide = ExecBlockPolicy::new(true, 200);
    wide.on_cancelled(1, 2);
    wide.on_cancelled(250, 251);
    assert_eq!(wide.next_window, 200);

    // the switch off and the `always` policy ignore the reports
    for (mut p, expected) in [
        (ExecBlockPolicy::new(false, 4), ExecPolicy::Off),
        (ExecBlockPolicy::new(true, 0), ExecPolicy::Always),
    ] {
        let before = p.clone();
        p.on_cancelled(3, 4);
        p.on_certified(5);
        assert_eq!(p, before);
        assert!((1..=50).all(|round| p.for_round(round) == expected));
    }
}

/// A fixture of four authorities (quorum 3) whose consensus config carries `params`.
fn exec_fixture(params: tn_config::Parameters) -> CommitteeFixture<MemDatabase> {
    CommitteeFixture::builder(MemDatabase::default).with_consensus_parameters(params).build()
}

/// Round-`round` parents by every authority of `fixture` but the first (the proposer), each
/// naming block `number` under a hash no ring holds.
fn exec_parents(
    fixture: &CommitteeFixture<MemDatabase>,
    round: Round,
    number: BlockNumber,
) -> Vec<Certificate> {
    let committee = fixture.committee();
    fixture
        .authorities()
        .skip(1)
        .map(|authority| {
            let header = HeaderBuilder::default()
                .author(authority.id())
                .round(round)
                .epoch(committee.epoch())
                .parents(BTreeSet::new())
                .latest_execution_block(BlockNumHash::new(number, B256::repeat_byte(0xee)))
                .build();
            Certificate::new_unsigned_for_test(&committee, header, Vec::new())
                .expect("unsigned parent certificate")
        })
        .collect()
}

/// A bus whose ring holds twelve executed outputs: blocks 100, 103, ..., 133.
fn exec_bus() -> ConsensusBus {
    let cb = ConsensusBus::new_with_args(50);
    cb.app().recent_blocks().send_modify(|ring| {
        for output in 0..12 {
            push_exec_output(ring, output);
        }
    });
    cb
}

/// The proposer of `fixture`'s first authority on `cb`, at round 1 with `parents` (of round 1).
fn exec_proposer(
    fixture: &CommitteeFixture<MemDatabase>,
    cb: &ConsensusBus,
    task_manager: &TaskManager,
    parents: Vec<Certificate>,
) -> Proposer<MemDatabase> {
    let config = fixture.first_authority().consensus_config();
    let mut proposer = proposer_on(&config, &fixture.committee(), cb, task_manager);
    proposer.round = 1;
    proposer.last_parents = parents;
    proposer
}

/// The header `proposer` builds next, and its `header proposed` line.
async fn exec_propose(
    proposer: &mut Proposer<MemDatabase>,
    capture: &crate::log_capture::LogCapture,
) -> (Header, crate::log_capture::CapturedEvent) {
    let header = proposer
        .propose_next_header(ProposeReason::MinDelay)
        .expect("proposal spawned")
        .await
        .expect("proposal task reported")
        .expect("header proposed");
    let line = capture.events("header proposed").pop().expect("a header proposed line");
    (header, line)
}

/// I-4 item 9, switch off: with the default parameters a header names
/// `latest_execution_block_num_hash()` exactly as before, whatever its parents name, and its bytes
/// equal the header `Header::new` builds from today's call; the `header proposed` line reports
/// `exec_policy=off`, `exec_basis=tip`. The same ring and parents with `quorum_exec_block: true`
/// name the quorum block, resolved from the own ring (never the parents' hash), so the switch is
/// what decides.
///
/// Catches: the switch-off path reading anything but the previous call, a default that turns the
/// rule on, a parent's hash in a header, and the four new fields missing or misnamed.
#[tokio::test]
async fn test_quorum_exec_block_switch_off_keeps_header_bytes() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let task_manager = TaskManager::default();

    // switch off (the default parameters)
    let fixture = exec_fixture(tn_config::Parameters::default());
    let cb = exec_bus();
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = exec_proposer(&fixture, &cb, &task_manager, exec_parents(&fixture, 1, 112));
    assert_eq!(proposer.exec_block_policy, ExecBlockPolicy::new(false, 0));
    let (header, line) = exec_propose(&mut proposer, &capture).await;

    let today = cb.app().latest_execution_block_num_hash();
    assert_eq!(today.number, 133);
    assert_eq!(header.latest_execution_block(), today);
    let rebuilt = Header::new(
        header.author().clone(),
        header.round(),
        header.epoch(),
        header.payload().clone(),
        header.parents().clone(),
        today,
        header.seed_signature().cloned().unwrap_or_default(),
        header.created_at_ms(),
    );
    assert_eq!(tn_types::encode(&header), tn_types::encode(&rebuilt), "header bytes unchanged");
    assert_eq!(header.digest(), rebuilt.digest());
    assert_eq!(line.u64_field("exec_block"), Some(133));
    assert_eq!(line.u64_field("exec_tip"), Some(133));
    assert_eq!(line.field("exec_basis"), Some("tip"));
    assert_eq!(line.field("exec_policy"), Some("off"));

    // switch on, rule on every header: the same ring and parents name the quorum block
    let fixture = exec_fixture(tn_config::Parameters {
        quorum_exec_block: true,
        ..tn_config::Parameters::default()
    });
    let cb = exec_bus();
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = exec_proposer(&fixture, &cb, &task_manager, exec_parents(&fixture, 1, 112));
    let (header, line) = exec_propose(&mut proposer, &capture).await;
    let own = cb.app().recent_blocks().borrow().newest_at_or_below(112).expect("own entry");
    assert_eq!(own.number, 112);
    assert_eq!(header.latest_execution_block(), own, "the own ring's hash, not the parents'");
    assert_ne!(own.hash, B256::repeat_byte(0xee));
    assert_eq!(line.u64_field("exec_block"), Some(112));
    assert_eq!(line.u64_field("exec_tip"), Some(133));
    assert_eq!(line.field("exec_basis"), Some("quorum"));
    assert_eq!(line.field("exec_policy"), Some("always"));
}

/// The adaptive policy through the proposer: the certifier's cancel report, handled by
/// `process_uncertified_header`, moves the next headers onto the quorum rule
/// (`exec_policy=starved`) for K rounds, and an own certificate among a new header's parents resets
/// K.
///
/// Catches: a cancel report that never reaches the policy, a window counted from anything but the
/// proposer's round, and an own certificate that is not seen as a certified header.
#[tokio::test]
async fn test_quorum_exec_block_cancel_report_reaches_the_policy() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let task_manager = TaskManager::default();
    let fixture = exec_fixture(tn_config::Parameters {
        quorum_exec_block: true,
        quorum_exec_block_starved_rounds: 4,
        ..tn_config::Parameters::default()
    });
    let cb = exec_bus();
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = exec_proposer(&fixture, &cb, &task_manager, exec_parents(&fixture, 1, 112));

    // round 2 names the tip
    let (round_two, line) = exec_propose(&mut proposer, &capture).await;
    assert_eq!(round_two.round(), 2);
    assert_eq!(line.field("exec_policy"), Some("tip"));
    assert_eq!(line.u64_field("exec_block"), Some(133));

    // the certifier reports a round-1 header cancelled: rounds 3-6 use the rule
    proposer.process_uncertified_header(1, HeaderDigest::default());
    proposer.last_parents = exec_parents(&fixture, 2, 112);
    let (round_three, line) = exec_propose(&mut proposer, &capture).await;
    assert_eq!(round_three.round(), 3);
    assert_eq!(line.field("exec_policy"), Some("starved"));
    assert_eq!(line.field("exec_basis"), Some("quorum"));
    assert_eq!(line.u64_field("exec_block"), Some(112));
    assert_eq!(proposer.exec_block_policy.for_round(6), ExecPolicy::Starved);
    assert_eq!(proposer.exec_block_policy.for_round(7), ExecPolicy::Tip);

    // two more counted cancels, with no certified tip header between them, double K twice; a
    // cancel of round 3 arrives once the proposer has moved on to round 4
    assert_eq!(proposer.exec_block_policy.next_window, 8);
    proposer.round = 4;
    proposer.process_uncertified_header(3, round_three.digest());
    assert_eq!(proposer.exec_block_policy.next_window, 16);
    proposer.round = 20;
    proposer.process_uncertified_header(19, HeaderDigest::default());
    assert_eq!(proposer.exec_block_policy.next_window, 32);

    // the own round-40 certificate, a tip header, among the parents of round 41 resets K
    let own = Certificate::new_unsigned_for_test(
        &fixture.committee(),
        HeaderBuilder::default()
            .author(fixture.first_authority().id())
            .round(40)
            .epoch(fixture.committee().epoch())
            .parents(BTreeSet::new())
            .build(),
        Vec::new(),
    )
    .expect("own certificate");
    proposer.round = 40;
    proposer.last_parents = [vec![own], exec_parents(&fixture, 40, 112)].concat();
    let (round_41, line) = exec_propose(&mut proposer, &capture).await;
    assert_eq!(round_41.round(), 41);
    assert_eq!(line.field("exec_policy"), Some("tip"));
    assert_eq!(proposer.exec_block_policy.next_window, 4);
}

// ---- bench6 parent-grace ----

/// The bench fleet's `min_header_delay`, which the parent-grace tests run on.
const GRACE_MIN_DELAY: Duration = Duration::from_millis(350);
/// The bench fleet's `max_header_delay`.
const GRACE_MAX_DELAY: Duration = Duration::from_millis(1_000);
/// The bench-6 arm's `parent_grace`.
const GRACE: Duration = Duration::from_millis(150);

/// Round-`round` certificates by `authors`, in that order.
///
/// Created at time zero, so a proposal never waits for a parent's timestamp and tokio's paused
/// clock moves only on the proposer's timers.
fn grace_certs(
    fixture: &CommitteeFixture<MemDatabase>,
    round: Round,
    authors: &[AuthorityIdentifier],
) -> Vec<Certificate> {
    let committee = fixture.committee();
    authors
        .iter()
        .map(|author| {
            let header = HeaderBuilder::default()
                .author(author.clone())
                .round(round)
                .epoch(committee.epoch())
                .parents(BTreeSet::new())
                .build();
            Certificate::new_unsigned_for_test(&committee, header, Vec::new())
                .expect("unsigned parent certificate")
        })
        .collect()
}

/// How long a parent-grace test lets the proposer settle after a header before it sends parents.
///
/// The proposer resets its readiness when a header's result comes back, so parents that arrive
/// while the header is still pending would wait for the max delay. On the paused clock this sleep
/// ends only once the proposer is idle, so the result has been handled.
const GRACE_SETTLE: Duration = Duration::from_millis(1);

/// A spawned proposer of a four-member committee (quorum 3) with `parent_grace` set, past its
/// round-2 header.
struct GraceRun<R> {
    /// The committee fixture the proposer runs in.
    fixture: CommitteeFixture<MemDatabase>,
    /// The proposer's bus.
    cb: ConsensusBus,
    /// The headers the proposer sends to the certifier.
    rx_headers: R,
    /// Keeps the proposer's tasks alive.
    _task_manager: TaskManager,
    /// Every authority, in fixture order.
    all: Vec<AuthorityIdentifier>,
    /// The authority that is neither the proposer nor a leader of round 2 or 4, so a quorum
    /// without it still lets the proposer advance.
    late: AuthorityIdentifier,
    /// When the round-2 header went out: the round-3 min and max delays run from here. Tests send
    /// round-2 parents [`GRACE_SETTLE`] later.
    round_two_at: tokio::time::Instant,
}

impl<R: TnReceiver<Header>> GraceRun<R> {
    /// Every authority but [`Self::late`].
    fn all_but_late(&self) -> Vec<AuthorityIdentifier> {
        self.all.iter().filter(|id| **id != self.late).cloned().collect()
    }

    /// Send round-`round` certificates by `authors` to the proposer as one parents message, and
    /// return them.
    async fn send(&self, round: Round, authors: &[AuthorityIdentifier]) -> Vec<Certificate> {
        let certs = grace_certs(&self.fixture, round, authors);
        self.cb.parents().send((certs.clone(), round)).await.expect("parents sent");
        certs
    }

    /// The next header at `round` and how long after the round-2 header it went out.
    async fn next(&mut self, round: Round) -> (Header, Duration) {
        let header = next_header_at(&mut self.rx_headers, round).await;
        (header, self.round_two_at.elapsed())
    }
}

/// Spawn an epoch-start proposer with `grace` and run it to its round-2 header.
///
/// The proposer leads neither round 2 nor round 4, so its min and max delays are the configured
/// ones at every round the tests measure. Round 1 builds on the genesis certificates and goes out
/// at once; round 2 builds on the round-1 certificates of every authority, or of every authority
/// but the late one when `round_one_without_late` is set, and goes out at once too (the intervals'
/// first ticks are both immediate, so one is still set). The bus and the proposer resolve their
/// metric handles under `recorder`.
async fn start_grace_run(
    grace: Duration,
    recorder: &metrics_util::debugging::DebuggingRecorder,
    round_one_without_late: bool,
) -> GraceRun<impl TnReceiver<Header>> {
    let params = tn_config::Parameters {
        min_header_delay: GRACE_MIN_DELAY,
        max_header_delay: GRACE_MAX_DELAY,
        parent_grace: grace,
        ..tn_config::Parameters::default()
    };
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).with_consensus_parameters(params).build();
    let committee = fixture.committee();
    assert_eq!(committee.size(), 4, "the rounds below assume four authorities (quorum 3)");
    let all: Vec<_> = fixture.authorities().map(|authority| authority.id()).collect();
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leaders = [schedule.leader(2).id(), schedule.leader(4).id()];
    let mut others = all.iter().filter(|id| !leaders.contains(id)).cloned();
    let own = others.next().expect("a non-leader proposer");
    let late = others.next().expect("a second non-leader");
    let config = fixture
        .authorities()
        .find(|authority| authority.id() == own)
        .expect("the proposer's fixture")
        .consensus_config();

    let task_manager = TaskManager::default();
    let cb = metrics::with_local_recorder(recorder, ConsensusBus::new);
    let mut rx_headers = cb.subscribe_headers();
    let proposer = metrics::with_local_recorder(recorder, || {
        proposer_on(&config, &committee, &cb, &task_manager)
    });
    proposer.spawn(&task_manager);
    next_header_at(&mut rx_headers, 1).await;
    tokio::time::sleep(GRACE_SETTLE).await;

    let round_one: Vec<_> = if round_one_without_late {
        all.iter().filter(|id| **id != late).cloned().collect()
    } else {
        all.clone()
    };
    let certs = grace_certs(&fixture, 1, &round_one);
    cb.parents().send((certs, 1)).await.expect("parents sent");
    next_header_at(&mut rx_headers, 2).await;
    let round_two_at = tokio::time::Instant::now();
    tokio::time::sleep(GRACE_SETTLE).await;

    GraceRun { fixture, cb, rx_headers, _task_manager: task_manager, all, late, round_two_at }
}

/// The parent-grace metrics, read from one snapshot.
#[derive(Debug, PartialEq, Eq)]
struct GraceMetrics {
    /// `tn_primary_parent_grace_total` in [`GraceOutcome::ALL`] order.
    outcomes: [u64; 3],
    /// `tn_primary_late_parents_total` for `own_round_behind`, then `older_round`.
    late: [u64; 2],
    /// `tn_primary_parent_grace_wait_seconds` samples, in whole milliseconds.
    waits_ms: Vec<u64>,
}

/// Snapshot the parent-grace metrics of `snapshotter`. Unregistered counters read zero.
fn grace_metrics(snapshotter: &metrics_util::debugging::Snapshotter) -> GraceMetrics {
    use metrics_util::debugging::DebugValue;

    let snapshot = snapshotter.snapshot().into_vec();
    let counter = |name: &str, label: &str, value: &str| {
        snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == name
                    && key.key().labels().any(|l| l.key() == label && l.value() == value)
            })
            .map_or(0, |(.., total)| match total {
                DebugValue::Counter(total) => *total,
                _ => 0,
            })
    };
    let waits_ms = snapshot
        .iter()
        .find(|(key, ..)| key.key().name() == "tn_primary.parent_grace_wait_seconds")
        .map(|(.., samples)| match samples {
            DebugValue::Histogram(samples) => {
                samples.iter().map(|sample| (sample.0 * 1_000.0).round() as u64).collect()
            }
            _ => Vec::new(),
        })
        .unwrap_or_default();
    GraceMetrics {
        outcomes: GraceOutcome::ALL
            .map(|outcome| counter("tn_primary.parent_grace_total", "outcome", outcome.as_str())),
        late: [LateParentOrigin::OwnRoundBehind, LateParentOrigin::OlderRound]
            .map(|origin| counter("tn_primary.late_parents_total", "origin", origin.as_str())),
        waits_ms,
    }
}

/// The last `header proposed` line of round `round` in `capture`.
fn proposed_line(
    capture: &crate::log_capture::LogCapture,
    round: Round,
) -> crate::log_capture::CapturedEvent {
    capture
        .events("header proposed")
        .into_iter()
        .rfind(|line| line.u64_field("round") == Some(u64::from(round)))
        .unwrap_or_else(|| panic!("no header proposed line for round {round}"))
}

/// Every author of the last header's parents is among the new parents: the header goes out the
/// moment it is ready, with no grace armed and `grace_ms=0`.
///
/// Catches: a grace armed when nobody is missing, which would slow every round by G.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_all_authors_present_proposes_at_once() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(GRACE, &recorder, false).await;

    let all = run.all.clone();
    run.send(2, &all).await;
    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, GRACE_MIN_DELAY, "proposed at the min delay, not later");
    assert_eq!(header.parents().len(), 4);
    let line = proposed_line(&capture, 3);
    assert_eq!(line.u64_field("grace_ms"), Some(0));
    assert_eq!(line.u64_field("num_parents"), Some(4));
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 0, 0], late: [0, 0], waits_ms: vec![] }
    );
}

/// One author of the last header's parents is missing when the header is ready; its certificate
/// arrives 40 ms later. The header goes out then, with it among the parents, `grace_ms=40` and
/// outcome `complete`.
///
/// Catches: a grace that does not wake on the late parent (it would run to the deadline), or a
/// header built before the late parent was added.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_late_author_within_grace_rides_the_header() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(GRACE, &recorder, false).await;

    run.send(2, &run.all_but_late()).await;
    let arrival = GRACE_MIN_DELAY + Duration::from_millis(40);
    tokio::time::sleep_until(run.round_two_at + arrival).await;
    let late = run.send(2, &[run.late.clone()]).await;

    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, arrival, "proposed when the late parent arrived");
    assert_eq!(header.parents().len(), 4);
    assert!(header.parents().contains(&late[0].digest()), "the late parent rides the header");
    let line = proposed_line(&capture, 3);
    assert_eq!(line.u64_field("grace_ms"), Some(40));
    assert_eq!(line.u64_field("num_parents"), Some(4));
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [1, 0, 0], late: [0, 0], waits_ms: vec![40] }
    );
}

/// The missing author never delivers in time: the header goes out exactly G after it was ready,
/// without that author, outcome `expired`. The certificate that turns up afterwards is a late
/// parent one round behind the proposer.
///
/// Catches: a grace that runs past G, a header that never goes out, and a late certificate that
/// is not counted.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_expires_without_the_late_author() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(GRACE, &recorder, false).await;

    let quorum = run.send(2, &run.all_but_late()).await;
    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, GRACE_MIN_DELAY + GRACE, "proposed when the grace expired");
    assert_eq!(
        header.parents(),
        &quorum.iter().map(|cert| cert.digest()).collect::<BTreeSet<_>>(),
        "the header carries the quorum it had, without the late author"
    );
    let line = proposed_line(&capture, 3);
    assert_eq!(line.u64_field("grace_ms"), Some(150));
    assert_eq!(line.u64_field("num_parents"), Some(3));
    assert_eq!(line.field("reason"), Some("min_delay"));

    // the round-2 certificate that missed the header
    run.send(2, &[run.late.clone()]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 1, 0], late: [1, 0], waits_ms: vec![150] }
    );
}

/// The max-delay tick fires while a grace runs: the header goes out on the tick with the parents
/// it has, `reason=max_delay`, outcome `preempted`. With G = 900 ms the grace would run to 1,250 ms
/// after round 2; the tick is at 1,000 ms.
///
/// Catches: a grace that delays a header past `max_header_delay`.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_max_delay_preempts_the_grace() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let long_grace = Duration::from_millis(900);
    let mut run = start_grace_run(long_grace, &recorder, false).await;

    run.send(2, &run.all_but_late()).await;
    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, GRACE_MAX_DELAY, "proposed on the max-delay tick");
    assert_eq!(header.parents().len(), 3);
    let line = proposed_line(&capture, 3);
    assert_eq!(line.field("reason"), Some("max_delay"));
    assert_eq!(line.u64_field("grace_ms"), Some(650));
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 0, 1], late: [0, 0], waits_ms: vec![650] }
    );
}

/// `parent_grace` at zero is today's proposer: the header goes out at the min delay for the same
/// reason and over exactly the quorum it had, without the missing author, `grace_ms=0`, no grace
/// metric moves, and no deadline ever exists. The late certificate is still counted.
///
/// Catches: a zero grace that arms anything, creates a timer, or moves a proposal.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_zero_keeps_todays_timing() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(Duration::ZERO, &recorder, false).await;

    let quorum = run.send(2, &run.all_but_late()).await;
    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, GRACE_MIN_DELAY, "proposed at the min delay");
    assert_eq!(
        header.parents(),
        &quorum.iter().map(|cert| cert.digest()).collect::<BTreeSet<_>>(),
        "over the quorum it had"
    );
    let line = proposed_line(&capture, 3);
    assert_eq!(line.u64_field("grace_ms"), Some(0));
    assert_eq!(line.field("reason"), Some("min_delay"));
    run.send(2, &[run.late.clone()]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 0, 0], late: [1, 0], waits_ms: vec![] }
    );

    // the state itself: nothing is kept, nothing is armed, no deadline
    let mut grace = ParentGrace::new(Duration::ZERO, GRACE_MAX_DELAY, Histogram::noop());
    let round_one = grace_certs(&run.fixture, 1, &run.all);
    let header = HeaderBuilder::default()
        .author(run.all[0].clone())
        .round(2)
        .epoch(run.fixture.committee().epoch())
        .parents(round_one.iter().map(|cert| cert.digest()).collect())
        .build();
    grace.on_propose(&round_one, Duration::from_millis(5));
    assert_eq!(grace.in_flight_wait(), Duration::ZERO, "nothing kept for the proposal");
    grace.on_proposed(&header);
    let quorum = grace_certs(&run.fixture, 2, &run.all_but_late());
    assert_eq!(grace.hold(&quorum), GraceHold::Propose(Duration::ZERO));
    assert_eq!(grace.deadline(), None, "no timer");
}

/// An author that was not among the last header's parents is never waited for: round 2 built on
/// three round-1 certificates, so round 3 goes out at the min delay without the fourth author.
///
/// Catches: a grace that waits for every committee member instead of last round's authors, which
/// would charge G every round for a crashed validator.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_ignores_authors_absent_from_last_header() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(GRACE, &recorder, true).await;

    run.send(2, &run.all_but_late()).await;
    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, GRACE_MIN_DELAY, "the absent author is not waited for");
    assert_eq!(header.parents().len(), 3);
    assert_eq!(proposed_line(&capture, 3).u64_field("grace_ms"), Some(0));
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 0, 0], late: [0, 0], waits_ms: vec![] }
    );

    // a restart's re-proposal sends the stored header, not the parents just collected: the basis
    // is what that header carries (three of the four round-1 certificates here), so the fourth
    // author is not waited for, and a header carrying none of them leaves no basis at all
    let all = &run.all;
    let round_one = grace_certs(&run.fixture, 1, all);
    let stored = |carried: &[Certificate]| {
        HeaderBuilder::default()
            .author(all[0].clone())
            .round(2)
            .epoch(run.fixture.committee().epoch())
            .parents(carried.iter().map(|cert| cert.digest()).collect())
            .build()
    };
    let without_fourth = grace_certs(&run.fixture, 2, &all[..3]);
    let mut grace = ParentGrace::new(GRACE, GRACE_MAX_DELAY, Histogram::noop());
    grace.on_propose(&round_one, Duration::ZERO);
    grace.on_proposed(&stored(&round_one[..3]));
    assert_eq!(grace.hold(&without_fourth), GraceHold::Propose(Duration::ZERO));
    assert_eq!(grace.deadline(), None, "the fourth author was not carried");
    let without_third = grace_certs(&run.fixture, 2, &[&all[..2], &all[3..]].concat());
    assert_eq!(grace.hold(&without_third), GraceHold::Wait, "the third author was carried");

    let mut grace = ParentGrace::new(GRACE, GRACE_MAX_DELAY, Histogram::noop());
    grace.on_propose(&round_one, Duration::ZERO);
    grace.on_proposed(&stored(&[]));
    assert_eq!(grace.hold(&without_fourth), GraceHold::Propose(Duration::ZERO));
    assert_eq!(grace.deadline(), None, "no carried parent, no basis");
}

/// The grace never proposes a header that is not ready. The proposer's quorum is the aggregator's
/// quorum message: while none for round 2 arrives, no round-3 header goes out through the min
/// delay, G and two max-delay ticks (those re-send the round-2 header), and no grace is armed.
/// The re-sends keep the basis: once a quorum without the late author arrives, the grace waits for
/// it and expires.
///
/// Catches: a grace deadline that proposes on its own, and a re-send that drops the basis.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_never_proposes_below_quorum() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(GRACE, &recorder, false).await;

    let quiet_until = run.round_two_at + GRACE_MAX_DELAY * 2 + Duration::from_millis(500);
    let mut resends = 0;
    while let Ok(header) = tokio::time::timeout_at(quiet_until, run.rx_headers.recv()).await {
        let header = header.expect("header channel stayed open");
        assert_eq!(header.round(), 2, "only the round-2 header is re-sent");
        resends += 1;
    }
    assert_eq!(resends, 2, "one re-send per max-delay tick");
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 0, 0], late: [0, 0], waits_ms: vec![] }
    );

    // the min delay re-armed at the last re-send has run out, so the quorum makes it ready now
    run.send(2, &run.all_but_late()).await;
    let (header, elapsed) = run.next(3).await;
    assert_eq!(elapsed, GRACE_MAX_DELAY * 2 + Duration::from_millis(500) + GRACE);
    assert_eq!(header.parents().len(), 3);
    assert_eq!(proposed_line(&capture, 3).u64_field("grace_ms"), Some(150));
    assert_eq!(grace_metrics(&snapshotter).outcomes, [0, 1, 0]);
}

/// At most one grace per round of parents: once a round's grace has ended, a header that is ready
/// over the same round proposes at once, whatever ended the grace, and only one deadline ever
/// exists for the round.
///
/// Catches: a grace re-armed after it expired or was preempted, which would wait 2G or more.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_arms_once_per_round() {
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let all: Vec<_> = fixture.authorities().map(|authority| authority.id()).collect();
    let round_one = grace_certs(&fixture, 1, &all);
    let header = HeaderBuilder::default()
        .author(all[0].clone())
        .round(2)
        .epoch(fixture.committee().epoch())
        .parents(round_one.iter().map(|cert| cert.digest()).collect())
        .build();
    let quorum = grace_certs(&fixture, 2, &all[..3]);
    let new_grace = || {
        let mut grace = metrics::with_local_recorder(&recorder, || {
            let wait = metrics::histogram!("tn_primary.parent_grace_wait_seconds");
            ParentGrace::new(GRACE, GRACE_MAX_DELAY, wait)
        });
        grace.on_propose(&round_one, Duration::ZERO);
        grace.on_proposed(&header);
        grace
    };

    // expired: one deadline, then the same round proposes at once
    let mut grace = new_grace();
    assert_eq!(grace.hold(&quorum), GraceHold::Wait);
    let deadline = grace.deadline().expect("armed");
    assert_eq!(deadline, tokio::time::Instant::now() + GRACE);
    assert_eq!(grace.hold(&quorum), GraceHold::Wait, "a second look keeps the grace");
    assert_eq!(grace.deadline(), Some(deadline), "and its deadline");
    tokio::time::advance(GRACE).await;
    grace.expire();
    assert_eq!(grace.deadline(), None);
    assert_eq!(grace.hold(&quorum), GraceHold::Propose(GRACE));
    assert_eq!(grace.hold(&quorum), GraceHold::Propose(Duration::ZERO), "not armed again");
    assert_eq!(grace.deadline(), None);

    // preempted by the max-delay path: the same round does not arm again either
    let mut grace = new_grace();
    assert_eq!(grace.hold(&quorum), GraceHold::Wait);
    tokio::time::advance(Duration::from_millis(30)).await;
    assert_eq!(grace.end(GraceOutcome::Preempted), Duration::from_millis(30));
    assert_eq!(grace.hold(&quorum), GraceHold::Propose(Duration::ZERO), "not armed again");
    assert_eq!(grace.deadline(), None);
    assert_eq!(grace.end(GraceOutcome::Preempted), Duration::ZERO, "nothing left to end");

    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 1, 1], late: [0, 0], waits_ms: vec![150, 30] }
    );
}

/// No grace without a last own header of the previous round to compare with.
///
/// - Epoch start: round 1 has no last header, and it builds on the genesis certificates, which
///   every authority has, so round 2 does not wait for an author missing from round 1 either.
/// - A skipped round: a grace armed for round 2's parents ends `preempted` when a round-3 quorum
///   jumps the proposer to round 3, and round 4 goes out at once, since there is no own round-3
///   header to compare with. The certificate validator's empty-parents jump ends it the same way. A
///   round-2 certificate that turns up at round 4 is a late parent of an older round.
///
/// Catches: genesis parents taken as liveness evidence, a grace that outlives a round jump (with
/// or without parents), and a basis compared across a skipped round.
#[tokio::test(start_paused = true)]
async fn test_parent_grace_none_at_epoch_start_or_after_a_skipped_round() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());

    // epoch start, on the state itself: round 1 has nothing to compare with, and its genesis
    // parents leave round 2 nothing either
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let all: Vec<_> = fixture.authorities().map(|authority| authority.id()).collect();
    let genesis = Certificate::genesis(&fixture.committee());
    let mut grace = ParentGrace::new(GRACE, GRACE_MAX_DELAY, Histogram::noop());
    assert_eq!(grace.hold(&genesis), GraceHold::Propose(Duration::ZERO), "round 1 at once");
    grace.on_propose(&genesis, Duration::ZERO);
    let round_one_header = HeaderBuilder::default()
        .author(all[0].clone())
        .round(1)
        .epoch(fixture.committee().epoch())
        .parents(genesis.iter().map(|cert| cert.digest()).collect())
        .build();
    grace.on_proposed(&round_one_header);
    let three = grace_certs(&fixture, 1, &all[..3]);
    assert_eq!(grace.hold(&three), GraceHold::Propose(Duration::ZERO), "round 2 at once");
    assert_eq!(grace.deadline(), None, "nothing armed at the epoch start");

    // the empty-parents jump: armed for round 2, then the parents are replaced by none
    let round_one = grace_certs(&fixture, 1, &all);
    grace.on_propose(&round_one, Duration::ZERO);
    let round_two_header = HeaderBuilder::default()
        .author(all[0].clone())
        .round(2)
        .epoch(fixture.committee().epoch())
        .parents(round_one.iter().map(|cert| cert.digest()).collect())
        .build();
    grace.on_proposed(&round_two_header);
    let round_two = grace_certs(&fixture, 2, &all[..3]);
    assert_eq!(grace.hold(&round_two), GraceHold::Wait, "armed for the fourth author");
    grace.end_if_round_moved(&round_two);
    assert!(grace.deadline().is_some(), "the same round keeps the grace");
    grace.end_if_round_moved(&[]);
    assert_eq!(grace.deadline(), None, "the empty-parents jump ends the grace");
    assert_eq!(grace.end(GraceOutcome::Preempted), Duration::ZERO, "already ended");

    // a jump: armed for round 2's parents, then a round-3 quorum arrives 10 ms later
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut run = start_grace_run(GRACE, &recorder, false).await;
    run.send(2, &run.all_but_late()).await;
    let jump = GRACE_MIN_DELAY + Duration::from_millis(10);
    tokio::time::sleep_until(run.round_two_at + jump).await;
    run.send(3, &run.all_but_late()).await;
    let (header, elapsed) = run.next(4).await;
    assert_eq!(elapsed, jump, "round 4 goes out at once after the jump");
    assert_eq!(header.parents().len(), 3);
    assert_eq!(proposed_line(&capture, 4).u64_field("grace_ms"), Some(0));
    // the late author's round-2 certificate reaches the proposer at round 4
    run.send(2, &[run.late.clone()]).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        grace_metrics(&snapshotter),
        GraceMetrics { outcomes: [0, 0, 1], late: [0, 1], waits_ms: vec![10] }
    );
}

// ---- bench6 digest-dedupe ----

/// How long a digest-dedupe test lets the proposer settle after a header before it reports a digest
/// or sends parents: on the paused clock this sleep ends only once the proposer is idle, so the
/// header's result has been handled and the header is tracked.
const DEDUPE_SETTLE: Duration = Duration::from_millis(1);

/// A fixture of four authorities with the `digest_dedupe` switch set to `on`.
fn dedupe_fixture(on: bool) -> CommitteeFixture<MemDatabase> {
    let params = tn_config::Parameters { digest_dedupe: on, ..tn_config::Parameters::default() };
    CommitteeFixture::builder(MemDatabase::default).with_consensus_parameters(params).build()
}

/// `tn_primary_duplicate_digest_suppressed_total` in `snapshotter`, `None` while unregistered.
///
/// A snapshot resets the debugging recorder's counters, so this is the count since the last one.
fn suppressed_total(snapshotter: &metrics_util::debugging::Snapshotter) -> Option<u64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, ..)| key.key().name() == "tn_primary.duplicate_digest_suppressed_total")
        .and_then(|(.., value)| match value {
            metrics_util::debugging::DebugValue::Counter(total) => Some(total),
            _ => None,
        })
}

/// The digests pending across `proposer`'s worker lanes, lane by lane.
fn pending_digests(proposer: &Proposer<MemDatabase>) -> Vec<BlockHash> {
    proposer.digests.values().flatten().map(|digest| digest.digest).collect()
}

/// I-2 S5 cases 1 and 2, switch on, through the run loop: a digest reported twice before the next
/// header is acked twice, queued once and rides one header; reported again while that header is
/// proposed and not certified, it is acked and dropped, and the next header does not carry it.
/// Each drop counts once in `tn_primary_duplicate_digest_suppressed_total` and logs one
/// `batch digest already queued` line naming the digest, its worker and where the proposer holds
/// it.
///
/// Catches: a duplicate queued behind the first copy, a dropped report left without its ack (the
/// worker's report would hang), a proposed header's payload missed by the check, and a drop that is
/// not counted or not logged with its fields.
#[tokio::test(start_paused = true)]
async fn test_digest_dedupe_reported_twice_rides_one_header() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("primary::proposer");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = dedupe_fixture(true);
    let epoch = fixture.committee().epoch();
    let all = fixture.authorities().count();
    let task_manager = TaskManager::default();
    let (cb, mut rx_headers, _) =
        spawn_with_first_header(&fixture, &task_manager, &recorder, &[]).await;
    tokio::time::sleep(DEDUPE_SETTLE).await;

    // reported twice before the next header: both acked (`send_digest` waits for the ack)
    let digest = B256::random();
    send_digest(&cb, digest).await;
    send_digest(&cb, digest).await;
    cb.parents().send((parents_at(&fixture, 1, epoch, all), 1)).await.expect("parents sent");
    let carrying = next_header_at(&mut rx_headers, 2).await;
    assert_eq!(payload_digests(&carrying), [digest], "one copy rides the header");
    assert_eq!(suppressed_total(&snapshotter), Some(1));

    // reported again while the header carrying it is proposed and not certified
    tokio::time::sleep(DEDUPE_SETTLE).await;
    send_digest(&cb, digest).await;
    cb.parents().send((parents_at(&fixture, 2, epoch, all), 2)).await.expect("parents sent");
    let next = next_header_at(&mut rx_headers, 3).await;
    assert!(payload_digests(&next).is_empty(), "the proposed copy is the only one");
    assert_eq!(suppressed_total(&snapshotter), Some(1), "one more since the last snapshot");

    let lines = capture.events("batch digest already queued");
    assert_eq!(lines.len(), 2, "one line per dropped report");
    for (line, held) in lines.iter().zip(["pending", "proposed"]) {
        assert_eq!(line.field("where"), Some(held));
        assert_eq!(line.field("digest"), Some(digest.to_string().as_str()));
        assert_eq!(line.u64_field("worker_id"), Some(0));
    }
}

/// I-2 S5 cases 2 and 3, switch on, on the proposer's state: a digest drained into the header being
/// built is held from the drain until its result returns, and then through the header's payload
/// while the header is tracked. Once its own header commits, the proposer holds it nowhere and a
/// later report queues it again: that copy is left to the worker and to execution.
///
/// Catches: the window between the drain and the proposal's result (the header is in neither the
/// lanes nor the tracked headers), and a committed digest blocked forever, which would strand a
/// re-sealed batch whose transactions the committed copy skipped.
#[tokio::test]
async fn test_digest_dedupe_holds_a_proposed_digest_until_its_header_commits() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("primary::proposer");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = dedupe_fixture(true);
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();
    let cb = metrics::with_local_recorder(&recorder, ConsensusBus::new);
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = metrics::with_local_recorder(&recorder, || {
        proposer_on(&primary.consensus_config(), &committee, &cb, &task_manager)
    });

    let digest = B256::random();
    proposer.queue_digest(queued(digest, 0));
    let in_flight =
        proposer.propose_next_header(ProposeReason::MinDelay).expect("proposal spawned");
    proposer.queue_digest(queued(digest, 0));
    assert!(pending_digests(&proposer).is_empty(), "the copy in flight is the only one");

    let result = in_flight.await.expect("proposal task reported");
    proposer.handle_proposal_result(result).expect("header proposed");
    let round = proposer.round;
    assert!(proposer.proposed_headers[&round].payload().contains_key(&digest));
    proposer.queue_digest(queued(digest, 0));
    assert!(pending_digests(&proposer).is_empty(), "the proposed copy is the only one");

    // its own header commits: the proposer no longer holds the digest
    proposer.process_committed_headers(round, vec![round]);
    proposer.queue_digest(queued(digest, 0));
    assert_eq!(pending_digests(&proposer), [digest], "a committed digest is queued again");

    let lines = capture.events("batch digest already queued");
    let held: Vec<_> = lines.iter().map(|line| line.field("where")).collect();
    assert_eq!(held, [Some("proposed"), Some("proposed")]);
    assert_eq!(suppressed_total(&snapshotter), Some(2));
}

/// I-2 S5 case 4, switch on, through the run loop: the certifier's cancel re-queues the cancelled
/// header's digests without a duplicate check, and they ride the next header ahead of newer ones.
/// Once re-queued they are pending again, so a fresh report of one of them is the duplicate.
///
/// Catches: a re-queue treated as a duplicate (the cancelled header's batches would never be
/// proposed again), and re-queued digests missing from the pending index (a fresh report would
/// put them in the next header twice).
#[tokio::test(start_paused = true)]
async fn test_digest_dedupe_requeue_of_a_canceled_header_is_not_a_duplicate() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("primary::proposer");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = dedupe_fixture(true);
    let epoch = fixture.committee().epoch();
    let all = fixture.authorities().count();
    let task_manager = TaskManager::default();
    let (d1, d2, d3, d4) = (B256::random(), B256::random(), B256::random(), B256::random());
    let (cb, mut rx_headers, _) =
        spawn_with_first_header(&fixture, &task_manager, &recorder, &[]).await;
    tokio::time::sleep(DEDUPE_SETTLE).await;

    send_digest(&cb, d1).await;
    send_digest(&cb, d2).await;
    cb.parents().send((parents_at(&fixture, 1, epoch, all), 1)).await.expect("parents sent");
    let canceled = next_header_at(&mut rx_headers, 2).await;
    assert_eq!(payload_digests(&canceled), [d1, d2]);
    tokio::time::sleep(DEDUPE_SETTLE).await;

    // round 3 cancels round 2's vote collection in the certifier
    send_digest(&cb, d3).await;
    cb.parents().send((parents_at(&fixture, 2, epoch, all), 2)).await.expect("parents sent");
    let newer = next_header_at(&mut rx_headers, 3).await;
    assert_eq!(payload_digests(&newer), [d3]);
    cb.uncertified_own_headers()
        .try_send((canceled.round(), canceled.digest()))
        .expect("report queued");
    tokio::time::timeout(Duration::from_secs(30), async {
        while requeued_totals(&snapshotter)[0] != Some(2) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the proposer re-queues the cancelled header's digests");

    // d1 is pending again after the re-queue, so this report of it is the duplicate
    send_digest(&cb, d1).await;
    send_digest(&cb, d4).await;
    cb.parents().send((parents_at(&fixture, 3, epoch, all), 3)).await.expect("parents sent");
    let next = next_header_at(&mut rx_headers, 4).await;
    assert_eq!(payload_digests(&next), [d1, d2, d4], "re-queued digests ride ahead of newer ones");

    let lines = capture.events("batch digest already queued");
    assert_eq!(lines.len(), 1, "only the fresh report of a re-queued digest is dropped");
    assert_eq!(lines[0].field("where"), Some("pending"));
    assert_eq!(lines[0].field("digest"), Some(d1.to_string().as_str()));
    assert_eq!(suppressed_total(&snapshotter), Some(1));
}

/// Switch off pins today's behaviour through the run loop: every report is acked and queued. A
/// digest reported twice is queued twice (the header's line counts both, and its payload map holds
/// one), and a digest reported again while its header is proposed rides the next header as well,
/// the duplicate carriage bench 5 measured. Nothing is counted as suppressed.
///
/// Catches: a default that turns the dedupe on, and any change to the queue or to a header's
/// payload while the switch is off.
#[tokio::test(start_paused = true)]
async fn test_digest_dedupe_off_queues_every_report() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::DebuggingRecorder;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = dedupe_fixture(false);
    assert!(!tn_config::Parameters::default().digest_dedupe, "the switch defaults to off");
    let epoch = fixture.committee().epoch();
    let all = fixture.authorities().count();
    let task_manager = TaskManager::default();
    let (cb, mut rx_headers, _) =
        spawn_with_first_header(&fixture, &task_manager, &recorder, &[]).await;
    tokio::time::sleep(DEDUPE_SETTLE).await;

    let digest = B256::random();
    send_digest(&cb, digest).await;
    send_digest(&cb, digest).await;
    cb.parents().send((parents_at(&fixture, 1, epoch, all), 1)).await.expect("parents sent");
    let carrying = next_header_at(&mut rx_headers, 2).await;
    assert_eq!(payload_digests(&carrying), [digest]);
    let line = proposed_line(&capture, 2);
    assert_eq!(line.u64_field("pending_before"), Some(2), "both reports were queued");
    assert_eq!(line.u64_field("num_batches"), Some(2), "both copies were drained");

    tokio::time::sleep(DEDUPE_SETTLE).await;
    send_digest(&cb, digest).await;
    cb.parents().send((parents_at(&fixture, 2, epoch, all), 2)).await.expect("parents sent");
    let next = next_header_at(&mut rx_headers, 3).await;
    assert_eq!(payload_digests(&next), [digest], "the digest rides a second header");
    assert_eq!(suppressed_total(&snapshotter), Some(0));
}

/// The pending index of the `digest_dedupe` switch counts exactly the digests in the worker lanes,
/// copy for copy, after every move: reports on two workers, a drain that a small header cap cuts
/// short across both lanes, the commit path's re-queue of headers that fell behind an own commit,
/// and a digest that sits in a lane twice (a pending digest whose stored header is re-sent and then
/// re-queued) drained to the end.
///
/// Catches: a lane change that bypasses the index. A stale entry would drop every later report of
/// that digest, so its batch would never ride a header; a missing entry would let a duplicate in.
#[tokio::test]
async fn test_digest_dedupe_pending_index_follows_the_lanes() {
    let fixture = dedupe_fixture(true);
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let author = primary.id();
    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = proposer_on(&primary.consensus_config(), &committee, &cb, &task_manager);
    proposer.max_header_num_of_batches = 3;
    proposer.round = 5;
    let index_matches = |proposer: &Proposer<MemDatabase>, step: &str| {
        let mut lanes: BTreeMap<BlockHash, usize> = BTreeMap::new();
        for digest in proposer.digests.values().flatten() {
            *lanes.entry(digest.digest).or_default() += 1;
        }
        let dedupe = proposer.digest_dedupe.as_ref().expect("switch on");
        assert_eq!(dedupe.pending, lanes, "{step}");
    };

    let (a0, a1, a2) = (B256::random(), B256::random(), B256::random());
    let (b0, b1) = (B256::random(), B256::random());
    for (digest, worker_id) in [(a0, 0), (a1, 0), (a2, 0), (b0, 1), (b1, 1)] {
        proposer.queue_digest(queued(digest, worker_id));
    }
    index_matches(&proposer, "reports on two workers");

    // three slots: a0 and b0, then a1; a2 and b1 stay pending
    let first: Vec<_> = proposer.drain_digests_for_header().iter().map(|d| d.digest).collect();
    assert_eq!(first, [a0, b0, a1]);
    index_matches(&proposer, "a drain cut short by the cap");
    assert_eq!(proposer.digest_dedupe.as_ref().expect("switch on").in_flight, first);
    proposer.handle_proposal_result(Ok(build_test_header(&author, 3, &first))).expect("tracked");
    assert!(proposer.digest_dedupe.as_ref().expect("switch on").in_flight.is_empty());

    // a re-send of a stored header that carries the pending a2 tracks it a second time
    proposer.handle_proposal_result(Ok(build_test_header(&author, 4, &[a2]))).expect("tracked");
    // round 5 commits: rounds 3 and 4 fell behind it and go back to the front of the lanes
    proposer.process_committed_headers(5, vec![5]);
    index_matches(&proposer, "the commit path's re-queue");
    assert_eq!(proposer.digest_dedupe.as_ref().expect("switch on").pending.get(&a2), Some(&2));

    // each header takes what the cap allows; the index releases a2 only with its second copy
    while proposer.pending_digests_len() > 0 {
        proposer.drain_digests_for_header();
        index_matches(&proposer, "a later drain");
    }
    assert!(proposer.digest_dedupe.as_ref().expect("switch on").pending.is_empty());
}

// ---- bench6 bench6-obs ----

/// Build `proposer`'s next header and track it, as the run loop does with the proposal's result.
async fn propose_and_track(proposer: &mut Proposer<MemDatabase>) -> Header {
    let header = proposer
        .propose_next_header(ProposeReason::MinDelay)
        .expect("proposal spawned")
        .await
        .expect("proposal task reported")
        .expect("header proposed");
    proposer.handle_proposal_result(Ok(header.clone())).expect("header tracked");
    header
}

/// The #1819 review note: a digest re-queued once reports its wait from the worker's report, not
/// from the re-queue. The re-queue keeps the digest's first receipt and restarts only its queue
/// wait; the next header's line reports `oldest_requeue_wait_ms` from the first receipt beside an
/// unchanged `oldest_digest_wait_ms` from the re-queue, and `digest_wait_seconds` records the
/// longer of the two. Once the header commits, the proposer holds no first receipt for it.
///
/// Catches: a re-queue that restamps the first receipt (the far validators' waits under-reported
/// by one proposal cycle per lost header), a histogram still measured from the re-queue, a changed
/// meaning of `oldest_digest_wait_ms`, and first receipts left behind after a commit.
#[tokio::test]
async fn test_requeued_digest_reports_its_wait_from_first_receipt() {
    use crate::log_capture::LogCapture;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();
    let cb = metrics::with_local_recorder(&recorder, ConsensusBus::new);
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = metrics::with_local_recorder(&recorder, || {
        proposer_on(&primary.consensus_config(), &committee, &cb, &task_manager)
    });

    // the worker reported the digest 300 ms ago, and it rides the round-1 header
    let wait = Duration::from_millis(300);
    let reported = Instant::now().checked_sub(wait).expect("the clock runs past the wait");
    let digest = B256::random();
    let report = ProposerDigest {
        digest,
        worker_id: 0,
        received: reported,
        first_received: reported,
        requeued: false,
    };
    proposer.digests = BTreeMap::from([(0, VecDeque::from([report]))]);
    let lost = propose_and_track(&mut proposer).await;
    assert_eq!(payload_digests(&lost), [digest]);

    // round 2 goes out, and the certifier cancels round 1
    propose_and_track(&mut proposer).await;
    let requeued_at = Instant::now();
    proposer.process_uncertified_header(lost.round(), lost.digest());
    let requeued = proposer.digests[&0].front().expect("the digest is pending again");
    assert_eq!(requeued.first_received, reported, "the first receipt survives the re-queue");
    assert!(requeued.received >= requeued_at, "the queue wait restarts at the re-queue");

    let carrying = propose_and_track(&mut proposer).await;
    assert_eq!(payload_digests(&carrying), [digest]);
    let line = proposed_line(&capture, carrying.round());
    assert_eq!(line.u64_field("requeued"), Some(1));
    let requeue_wait = line.u64_field("oldest_requeue_wait_ms").expect("oldest_requeue_wait_ms");
    assert!(requeue_wait >= 300, "waited {requeue_wait} ms from the report, at least 300");
    let queue_wait = line.u64_field("oldest_digest_wait_ms").expect("oldest_digest_wait_ms");
    assert!(queue_wait < 300, "oldest_digest_wait_ms still runs from the re-queue: {queue_wait}");

    // one sample per header with digests, rounds 1 and 3, each from the report
    let samples = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| match value {
            DebugValue::Histogram(samples)
                if key.key().name() == "tn_primary.digest_wait_seconds" =>
            {
                Some(samples)
            }
            _ => None,
        })
        .expect("digest_wait_seconds recorded");
    assert_eq!(samples.len(), 2);
    assert!(samples.iter().all(|sample| sample.0 >= wait.as_secs_f64()), "{samples:?}");
    // the later header's sample is the larger one, and it is the line's longer wait
    let carrying_ms = samples.iter().map(|sample| sample.0).fold(0.0, f64::max) * 1_000.0;
    assert!((carrying_ms - requeue_wait as f64).abs() < 1.0, "{carrying_ms} vs {requeue_wait}");

    // the header commits: its digest's first receipt is released
    proposer.process_committed_headers(carrying.round(), vec![carrying.round()]);
    assert!(proposer.proposed_first_received.is_empty(), "no first receipt outlives its header");
}

/// A header that carries no re-queued digest reports `oldest_requeue_wait_ms=0`, whether it carries
/// fresh digests (their oldest wait is still reported in `oldest_digest_wait_ms`) or none at all.
///
/// Catches: the field computed over every digest instead of the re-queued ones, or left out of a
/// header without digests.
#[tokio::test]
async fn test_header_without_requeued_digest_reports_zero_requeue_wait() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let _rx_headers = cb.subscribe_headers();
    let mut proposer = proposer_on(&primary.consensus_config(), &committee, &cb, &task_manager);

    let reported =
        Instant::now().checked_sub(Duration::from_millis(200)).expect("the clock runs past 200 ms");
    let report = ProposerDigest {
        digest: B256::random(),
        worker_id: 0,
        received: reported,
        first_received: reported,
        requeued: false,
    };
    proposer.digests = BTreeMap::from([(0, VecDeque::from([report, queued(B256::random(), 0)]))]);
    let fresh = propose_and_track(&mut proposer).await;
    let empty = propose_and_track(&mut proposer).await;
    assert_eq!(fresh.payload().len(), 2);
    assert!(empty.payload().is_empty());

    let fresh_line = proposed_line(&capture, fresh.round());
    assert_eq!(fresh_line.u64_field("requeued"), Some(0));
    assert_eq!(fresh_line.u64_field("oldest_requeue_wait_ms"), Some(0));
    let fresh_wait = fresh_line.u64_field("oldest_digest_wait_ms").expect("oldest_digest_wait_ms");
    assert!(fresh_wait >= 200, "the fresh digests' wait is still reported: {fresh_wait}");
    let empty_line = proposed_line(&capture, empty.round());
    assert_eq!(empty_line.u64_field("oldest_requeue_wait_ms"), Some(0));
    assert_eq!(empty_line.u64_field("oldest_digest_wait_ms"), Some(0));
}
