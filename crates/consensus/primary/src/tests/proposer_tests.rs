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
    let selected = proposer.drain_digests_except(&BTreeSet::new());
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
        exec_no_parents(&committee, &primary.id()),
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
        exec_no_parents(committee, &primary.id()),
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

    let selected = proposer.drain_digests_except(&BTreeSet::new());
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

    let selected = proposer.drain_digests_except(&BTreeSet::new());
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

    let first: Vec<WorkerId> = proposer
        .drain_digests_except(&BTreeSet::new())
        .iter()
        .map(|digest| digest.worker_id)
        .collect();
    assert_eq!(first, (0..10).collect::<Vec<_>>(), "first header serves lanes 0-9");

    // the next header resumes at lane 10 instead of restarting at lane 0
    let second: Vec<WorkerId> = proposer
        .drain_digests_except(&BTreeSet::new())
        .iter()
        .map(|digest| digest.worker_id)
        .collect();
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
    ProposerDigest {
        digest,
        worker_id,
        received: now,
        first_received: now,
        requeued: false,
        seq: None,
    }
}

/// A [`ProposalTrigger`] for tests that read neither the proposal's reason nor its counter.
fn untracked_trigger() -> ProposalTrigger {
    ProposalTrigger {
        reason: ProposeReason::MinDelay,
        pending_before: 0,
        proposed: Counter::noop(),
        hold: HoldReport::default(),
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
                    seq: None,
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
        seq: None,
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
        seq: None,
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

// ---- bench6 quorum-exec-block ----

/// [`ExecBlockInputs`] with no parents, for tests that drive [`Proposer::propose_header`]
/// directly: with only its own tip to rank, the header names the own tip.
fn exec_no_parents(committee: &Committee, author: &AuthorityIdentifier) -> ExecBlockInputs {
    ExecBlockInputs::new(committee, author, &[])
}

/// The epoch of the quorum-exec-block models: their parents and their ring's blocks carry it.
const EXEC_MODEL_EPOCH: Epoch = 3;

/// The final block of executed output `output` in the quorum-exec-block models: three blocks per
/// output, so ring entries and block numbers differ. Its nonce names [`EXEC_MODEL_EPOCH`].
fn exec_output_block(output: u64) -> tn_types::SealedHeader {
    exec_epoch_block(EXEC_MODEL_EPOCH, output)
}

/// [`exec_output_block`] built by an output of `epoch`: the nonce carries the epoch in its upper
/// half and the output as the round in its lower half, as execution stamps it.
fn exec_epoch_block(epoch: Epoch, output: u64) -> tn_types::SealedHeader {
    let round = u64::from(Round::try_from(output).expect("small model"));
    tn_types::SealedHeader::seal_slow(tn_types::ExecHeader {
        number: 100 + 3 * output,
        nonce: ((u64::from(epoch) << 32) | round).into(),
        ..Default::default()
    })
}

/// Push executed output `output` of [`EXEC_MODEL_EPOCH`] onto `ring` and return its final block.
fn push_exec_output(ring: &mut RecentBlocks, output: u64) -> BlockNumHash {
    push_epoch_output(ring, EXEC_MODEL_EPOCH, output)
}

/// Push executed output `output` of `epoch` onto `ring` and return its final block.
fn push_epoch_output(ring: &mut RecentBlocks, epoch: Epoch, output: u64) -> BlockNumHash {
    let block = exec_epoch_block(epoch, output);
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
fn table_inputs(parents: Vec<ParentExecBlock>) -> ExecBlockInputs {
    ExecBlockInputs {
        author: AuthorityIdentifier::dummy_for_test(0),
        own_power: 1,
        epoch: EXEC_MODEL_EPOCH,
        quorum_threshold: 7,
        parents,
    }
}

/// A round-5, epoch-3 parent of power 1 by authority `id` naming block `number`.
fn table_parent(id: u8, number: BlockNumber) -> ParentExecBlock {
    ParentExecBlock {
        author: AuthorityIdentifier::dummy_for_test(id),
        power: 1,
        epoch: EXEC_MODEL_EPOCH,
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
/// hash here; `test_quorum_exec_block_header_names_the_own_ring_block` checks the header's hash is
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

    let cases: Vec<(&str, Vec<ParentExecBlock>, BlockNumber, ExecBasis)> = vec![
        ("in step", table_parents(1, 9, 157), 157, ExecBasis::Tip),
        ("ahead by one output", table_parents(1, 9, 154), 154, ExecBasis::Quorum),
        ("ahead by six outputs", table_parents(1, 9, 139), 139, ExecBasis::Quorum),
        (
            "three parents at the tip are not a quorum with the own tip",
            [table_parents(1, 3, 157), table_parents(4, 6, 139)].concat(),
            139,
            ExecBasis::Quorum,
        ),
        (
            "six parents at the tip are a quorum with the own tip",
            [table_parents(1, 6, 157), table_parents(7, 3, 139)].concat(),
            157,
            ExecBasis::Tip,
        ),
        ("own node behind: the cap", table_parents(1, 9, 170), 157, ExecBasis::Tip),
        (
            "a quorum value between entries resolves to the own entry below it",
            table_parents(1, 9, 150),
            148,
            ExecBasis::Quorum,
        ),
        ("older than the floor", table_parents(1, 9, 120), 133, ExecBasis::Floor),
        ("exactly the floor", table_parents(1, 9, 133), 133, ExecBasis::Quorum),
        ("fewer than a quorum of usable parents", table_parents(1, 5, 139), 157, ExecBasis::Tip),
        (
            "other-epoch and genesis parents are not usable",
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
            [vec![table_parent(0, 139)], table_parents(1, 5, 139)].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "an author counts once",
            [table_parents(1, 4, 139), vec![table_parent(1, 139), table_parent(2, 139)]].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "ranked by voting power, not by count",
            vec![weighted(1, 4, 139), weighted(2, 1, 139), weighted(3, 1, 139)],
            139,
            ExecBasis::Quorum,
        ),
        (
            "f hostile parents naming a huge number do not move the choice",
            [table_parents(1, 3, u64::MAX), table_parents(4, 6, 151)].concat(),
            151,
            ExecBasis::Quorum,
        ),
        (
            "f low parents beside six honest ones at the tip do not move the choice",
            [table_parents(1, 3, 0), table_parents(4, 6, 157)].concat(),
            157,
            ExecBasis::Tip,
        ),
        (
            "f low parents in a minimal parent set drag the choice to the floor and no lower",
            [table_parents(1, 3, 0), table_parents(4, 3, 157)].concat(),
            133,
            ExecBasis::Floor,
        ),
    ];
    for (name, parents, number, basis) in cases {
        let choice = choose_exec_block(&ring, &table_inputs(parents));
        assert_eq!(choice.block, entry(number), "{name}: the own ring's number and hash");
        assert_eq!(choice.tip, tip, "{name}");
        assert_eq!(choice.basis, basis, "{name}");
    }

    // a short ring floors at its oldest entry
    let short = exec_ring(4);
    let choice = choose_exec_block(&short, &table_inputs(table_parents(1, 9, 50)));
    assert_eq!(choice.block, short.nth_newest(3).expect("oldest"), "the oldest of four entries");
    assert_eq!((choice.block.number, choice.basis), (100, ExecBasis::Floor));

    // an empty ring names the default block
    let empty = RecentBlocks::new(50);
    let choice = choose_exec_block(&empty, &table_inputs(table_parents(1, 9, 139)));
    assert_eq!(choice.block, empty.latest_execution_block_num_hash());
    assert_eq!(choice.block.number, 0, "the default header's block");
    assert_eq!(choice.basis, ExecBasis::Tip);
}

/// What one in-step fleet run of the self-feeding model records per round and node: how many
/// outputs the named block trails the tip, and the basis.
type FleetRound = Vec<(usize, ExecBasis)>;

/// Ten in-step proposers (one ring stands for all ten: they executed the same outputs), one new
/// output executed before every round, each round's headers the next round's parents. Round 1
/// builds on genesis parents.
fn self_feeding_fleet(rounds: Round) -> Vec<FleetRound> {
    const FLEET: u8 = 10;
    let ids: Vec<_> = (0..FLEET).map(AuthorityIdentifier::dummy_for_test).collect();
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
                epoch: EXEC_MODEL_EPOCH,
                // genesis parents for round 1
                round: round - 1,
                number: block.number,
            })
            .collect();
        let mut this_round = Vec::new();
        let mut next_named = Vec::new();
        for author in &ids {
            let inputs = ExecBlockInputs {
                author: author.clone(),
                own_power: 1,
                epoch: EXEC_MODEL_EPOCH,
                quorum_threshold: 7,
                parents: parents.clone(),
            };
            let choice = choose_exec_block(&ring, &inputs);
            let at = history.iter().position(|b| *b == choice.block).expect("an executed output");
            this_round.push((history.len() - 1 - at, choice.basis));
            next_named.push(choice.block);
        }
        named = next_named;
        out.push(this_round);
    }
    out
}

/// I-4 item 1: the choice function fed its own outputs as the next round's parents. An in-step
/// ten-node fleet over 20 rounds settles exactly [`QUORUM_EXEC_BLOCK_FLOOR_ENTRIES`] outputs
/// below its tip (the quorum value can only rise through the floor).
///
/// Catches: a rule that ranks the parents' tips it does not have (the model would show lag 0),
/// and a floor that does not hold the ratchet at N.
#[test]
fn test_quorum_exec_block_self_feeding_fleet() {
    let n = QUORUM_EXEC_BLOCK_FLOOR_ENTRIES;
    let always = self_feeding_fleet(20);
    for (round, nodes) in (1..).zip(&always) {
        // round r trails by r - 1 outputs until the floor holds it at N
        let expected = usize::min(round - 1, n);
        for (node, (lag, basis)) in nodes.iter().enumerate() {
            assert_eq!(*lag, expected, "always: round {round}, node {node}");
            if round > n + 1 {
                assert_eq!(*basis, ExecBasis::Floor, "always: round {round} sits on the floor");
            }
        }
    }
    assert!(always[n..].iter().flatten().all(|(lag, ..)| *lag == n), "lag exactly N from round 9");
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

/// A bus whose ring holds twelve executed outputs of epoch 0, the fixtures' epoch: blocks 100,
/// 103, ..., 133.
fn exec_bus() -> ConsensusBus {
    let cb = ConsensusBus::new_with_args(50);
    cb.app().recent_blocks().send_modify(|ring| {
        for output in 0..12 {
            push_epoch_output(ring, 0, output);
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

/// I-4 item 9: a header names the quorum block, resolved from the own ring (never the parents'
/// hash), and its `header proposed` line reports `exec_block`, `exec_tip` and `exec_basis`.
///
/// Catches: a parent's hash in a header, and the fields missing or misnamed.
#[tokio::test]
async fn test_quorum_exec_block_header_names_the_own_ring_block() {
    use crate::log_capture::LogCapture;
    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let task_manager = TaskManager::default();
    let fixture = exec_fixture(tn_config::Parameters::default());
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
}

// ---- bench7 quorum-exec-block ----

/// Review item I1: the final bound of [`choose_exec_block`] holds on a ring whose numbers do not
/// rise. The ring holds twenty outputs of the epoch before (blocks 100 to 157; 157 opened the
/// header's epoch), then an entry of the header's epoch numbered 130, below the opening block,
/// then 163 and 166. A quorum at 160 resolves through that entry, so the rule alone names 130, a
/// block a voter restored at the opening block refuses for good. The bound names the opening block
/// instead, under both forms that apply the rule; a debug build stops at the assertion first, so
/// the tests catch any choice that needs the bound.
///
/// Catches: a missing bound, a bound read from anything but the ring's previous-epoch entries, a
/// raised choice that names a block this node did not execute, a bound that moves a choice at or
/// above the opening block, and a debug assertion that does not fire on a broken ring.
#[test]
fn test_quorum_exec_block_bound_holds_on_a_ring_whose_numbers_drop() {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    let mut ring = RecentBlocks::new(50);
    for output in 0..20 {
        push_epoch_output(&mut ring, EXEC_MODEL_EPOCH - 1, output);
    }
    let opening = ring.latest_execution_block_num_hash();
    assert_eq!(opening.number, 157);
    // an entry of the header's epoch below the opening block: the ring's numbers drop
    let dropped = push_exec_output(&mut ring, 10);
    assert_eq!(dropped.number, 130);
    push_exec_output(&mut ring, 21);
    let tip = push_exec_output(&mut ring, 22);
    assert_eq!(tip.number, 166);
    assert_eq!(ring.newest_before_epoch(EXEC_MODEL_EPOCH), Some(opening));
    let bounded = ExecBlockChoice { block: opening, tip, basis: ExecBasis::Floor };

    let inputs = table_inputs(table_parents(1, 6, 160));
    let raw = quorum_rule_choice(&ring, &inputs, Some(opening));
    assert_eq!(
        raw,
        ExecBlockChoice { block: dropped, tip, basis: ExecBasis::Quorum },
        "the rule alone names the entry below the opening block"
    );
    assert_eq!(bound_to_opening(raw, Some(opening)), bounded, "the bound");
    // end to end: a debug build stops at the assertion, a release build names the opening
    // block
    let result = catch_unwind(AssertUnwindSafe(|| choose_exec_block(&ring, &inputs)));
    if cfg!(debug_assertions) {
        assert!(result.is_err(), "the debug assertion fires");
    } else {
        assert_eq!(result.ok(), Some(bounded), "the bound in a release build");
    }

    // the bound leaves a choice at or above the opening block alone, and does nothing without an
    // opening block in the ring
    let at = ExecBlockChoice { block: opening, tip, basis: ExecBasis::Quorum };
    assert_eq!(bound_to_opening(at, Some(opening)), at);
    let below = ExecBlockChoice { block: dropped, tip, basis: ExecBasis::Quorum };
    assert_eq!(bound_to_opening(below, None), below);
}

/// Review finding V3: f faulty parents (one of four equal members) name a block far above what
/// the honest members executed and then withhold their votes. The proposer's own tip is 157; its
/// parents name 10,000 (faulty), 151 and 136 (honest, each holding the blocks up to its own tip).
/// The rule ranks the faulty value first, so the quorum value is 151 where the honest parents
/// alone give 136: the member at 136 cannot vote for 151, the faulty member withholds, and the
/// header gets two of the three votes it needs. Naming 136 it would have certified; naming the
/// tip it fails too. Over a sweep of honest tips and faulty values, the rule never names a block
/// that fewer honest members hold than hold the tip, so its header certifies whenever the tip
/// header would have.
///
/// Catches: a faulty value that moves the choice past the own tip, a choice that is not an own
/// ring entry, and a header under the rule that loses an honest vote the tip header would have
/// had.
#[test]
fn test_quorum_exec_block_faulty_high_parents_cost_the_gain_only() {
    let ring = exec_ring(20);
    let tip = ring.latest_execution_block_num_hash();
    assert_eq!(tip.number, 157);
    // authority 0 proposes, 1 is faulty, 2 and 3 are honest; quorum 3 of 4
    let inputs = |faulty: Option<BlockNumber>, honest: [BlockNumber; 2]| {
        let parents = faulty
            .map(|number| table_parent(1, number))
            .into_iter()
            .chain([table_parent(2, honest[0]), table_parent(3, honest[1])])
            .collect();
        ExecBlockInputs {
            author: AuthorityIdentifier::dummy_for_test(0),
            own_power: 1,
            epoch: EXEC_MODEL_EPOCH,
            quorum_threshold: 3,
            parents,
        }
    };
    // the faulty member withholds; the proposer and each honest member that executed the named
    // block vote
    let votes = |named: BlockNumHash, honest: [BlockNumber; 2]| -> VotingPower {
        let holders = honest.iter().map(|executed| VotingPower::from(*executed >= named.number));
        1 + holders.sum::<VotingPower>()
    };

    let honest = [151, 136];
    let choose = |faulty| choose_exec_block(&ring, &inputs(faulty, honest));
    assert_eq!(votes(tip, honest), 1, "the tip header is cancelled");
    let lifted = ring.newest_at_or_below(151).expect("ring entry");
    assert_eq!(
        choose(Some(10_000)),
        ExecBlockChoice { block: lifted, tip, basis: ExecBasis::Quorum },
        "the faulty value lifts the quorum value to 151"
    );
    assert_eq!(votes(lifted, honest), 2, "the header under the rule is cancelled too");
    let honest_only = choose(None);
    assert_eq!(honest_only.block.number, 136);
    assert_eq!(votes(honest_only.block, honest), 3, "without the faulty value it certifies");

    // the sweep: honest tips below, at and above the own tip, faulty values above it
    let tips: Vec<BlockNumber> = (100..=169).step_by(3).collect();
    for faulty in [158, 1_000, 10_000, BlockNumber::MAX] {
        for &first in &tips {
            for &second in &tips {
                let honest = [first, second];
                let choice = choose_exec_block(&ring, &inputs(Some(faulty), honest));
                let case = format!("faulty {faulty}, honest {honest:?}");
                assert!(choice.block.number <= tip.number, "{case}: capped at the own tip");
                assert_eq!(
                    ring.newest_at_or_below(choice.block.number),
                    Some(choice.block),
                    "{case}: an own ring entry"
                );
                assert!(votes(choice.block, honest) >= votes(tip, honest), "{case}");
                let without = choose_exec_block(&ring, &inputs(None, honest));
                assert!(
                    choice.block.number >= without.block.number,
                    "{case}: the faulty value only lifts the choice"
                );
            }
        }
    }
}

/// The quorum rule in an epoch's first rounds, under both forms that apply it: the ring holds
/// twenty outputs of the epoch before (blocks 100 to 157; 157 closed that epoch) and three of the
/// header's epoch (160, 163, 166), so the floor entry eight back (142) lies before the epoch's
/// opening block. A parent naming a block below 157 is not counted, so nothing below 157 is named:
/// a voter restored from that epoch's export (floor 157) would refuse such a header for good.
///
/// Catches: a quorum value or a floor below the epoch's opening block, a low parent still counted
/// toward the quorum, and an opening block read from anything but the ring's previous-epoch
/// entries.
#[test]
fn test_quorum_exec_block_never_names_a_block_before_the_epoch_opened() {
    let mut ring = RecentBlocks::new(50);
    for output in 0..20 {
        push_epoch_output(&mut ring, EXEC_MODEL_EPOCH - 1, output);
    }
    let opening = ring.latest_execution_block_num_hash();
    assert_eq!(opening.number, 157);
    for output in 20..23 {
        push_exec_output(&mut ring, output);
    }
    let tip = ring.latest_execution_block_num_hash();
    assert_eq!(tip.number, 166);
    assert_eq!(ring.newest_before_epoch(EXEC_MODEL_EPOCH), Some(opening));
    assert_eq!(ring.nth_newest(QUORUM_EXEC_BLOCK_FLOOR_ENTRIES).map(|b| b.number), Some(142));
    let entry = |number: BlockNumber| ring.newest_at_or_below(number).expect("ring entry");
    let own_tip = ExecBlockChoice { block: tip, tip, basis: ExecBasis::Tip };

    let choose = |parents: Vec<ParentExecBlock>| choose_exec_block(&ring, &table_inputs(parents));
    // honest parents still on the opening block name it
    assert_eq!(
        choose(table_parents(1, 6, 157)),
        ExecBlockChoice { block: opening, tip, basis: ExecBasis::Quorum },
        "a quorum on the opening block"
    );
    // one low parent in a minimal set no longer drags the choice below the opening block:
    // without it the set falls short of a quorum and the own tip is named
    for low in [120, 148] {
        let mut parents = table_parents(1, 5, 160);
        parents.push(table_parent(6, low));
        assert_eq!(choose(parents), own_tip, "one parent at {low}");
    }
    // f low parents beside a quorum: the quorum value is set by the counted values only
    let mut parents = table_parents(1, 6, 160);
    parents.extend(table_parents(7, 3, 103));
    assert_eq!(
        choose(parents),
        ExecBlockChoice { block: entry(160), tip, basis: ExecBasis::Quorum },
        "three low parents beside six at 160"
    );
    // whatever the low parent names, the choice is never below the opening block
    for low in 0..=tip.number {
        let mut parents = table_parents(1, 5, 160);
        parents.push(table_parent(6, low));
        let choice = choose(parents);
        assert!(choice.block.number >= opening.number, "a parent at {low} named {choice:?}");
        if low >= opening.number {
            // a usable value still counts and sets the quorum value, as before
            let expected = if low >= 160 { entry(160) } else { entry(low) };
            assert_eq!(choice.block, expected, "a usable parent at {low}");
        }
    }

    // the epoch's first round: the own tip is still the opening block, so it is named whatever the
    // parents name
    let mut first = RecentBlocks::new(50);
    for output in 0..20 {
        push_epoch_output(&mut first, EXEC_MODEL_EPOCH - 1, output);
    }
    for low in [0, 100, 142, 156, 157] {
        let choice = choose_exec_block(&first, &table_inputs(table_parents(1, 9, low)));
        assert_eq!(choice, ExecBlockChoice { block: opening, tip: opening, basis: ExecBasis::Tip });
    }
}

/// The quorum rule's inputs as the proposer builds them, in an epoch above 0: the header's epoch
/// comes from the committee and each parent's block from its certificate, so in a minimal parent
/// set at epoch 4 a parent naming a block of the epoch counts and one naming a block from before
/// the epoch opened does not.
///
/// Catches: an `ExecBlockInputs` epoch that is not the committee's (every parent skipped, or the
/// opening block never found), which the epoch-0 proposer tests cannot see.
#[test]
fn test_quorum_exec_block_inputs_carry_the_committee_epoch() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).epoch(4).build();
    let committee = fixture.committee();
    assert_eq!((committee.epoch(), committee.quorum_threshold()), (4, 3));
    let author = fixture.first_authority().id();
    // epoch 3 closes at block 133; epoch 4 has executed 136 and 139
    let mut ring = RecentBlocks::new(50);
    for output in 0..12 {
        push_epoch_output(&mut ring, 3, output);
    }
    let opening = ring.latest_execution_block_num_hash();
    for output in 12..14 {
        push_epoch_output(&mut ring, 4, output);
    }
    let tip = ring.latest_execution_block_num_hash();
    assert_eq!((opening.number, tip.number), (133, 139));
    assert_eq!(ring.nth_newest(QUORUM_EXEC_BLOCK_FLOOR_ENTRIES).map(|b| b.number), Some(115));

    // round-1 certificates of the committee's epoch by the second and third authority
    let choose = |numbers: [BlockNumber; 2]| {
        let parents: Vec<Certificate> = fixture
            .authorities()
            .skip(1)
            .zip(numbers)
            .map(|(authority, number)| {
                let header = HeaderBuilder::default()
                    .author(authority.id())
                    .round(1)
                    .epoch(committee.epoch())
                    .parents(BTreeSet::new())
                    .latest_execution_block(BlockNumHash::new(number, B256::repeat_byte(0xee)))
                    .build();
                Certificate::new_unsigned_for_test(&committee, header, Vec::new())
                    .expect("unsigned parent certificate")
            })
            .collect();
        let inputs = ExecBlockInputs::new(&committee, &author, &parents);
        assert_eq!(inputs.epoch, 4);
        choose_exec_block(&ring, &inputs)
    };
    let entry = ring.newest_at_or_below(136).expect("ring entry");
    assert_eq!(choose([136, 136]), ExecBlockChoice { block: entry, tip, basis: ExecBasis::Quorum });
    // the parent at 103 is skipped, so the set falls short of a quorum; counted, it would have
    // dragged the choice to the floor entry 115, below the opening block
    assert_eq!(choose([136, 103]), ExecBlockChoice { block: tip, tip, basis: ExecBasis::Tip });
}

// ---- bench7 digest-order-hold ----

/// A batch digest numbered `n`, the same on every run, so a payload can be pinned as a list.
fn fixed_digest(n: u8) -> BlockHash {
    B256::with_last_byte(n)
}

/// A header's payload as `(digest, worker)` entries, in payload order.
fn payload_entries(header: &Header) -> Vec<(BlockHash, WorkerId)> {
    header.payload().iter().map(|(digest, worker_id)| (*digest, *worker_id)).collect()
}

/// Queue `numbers` as digests that `worker_id` just reported, through the proposer's own push.
fn report_fixed(proposer: &mut Proposer<MemDatabase>, worker_id: WorkerId, numbers: &[u8]) {
    for n in numbers {
        proposer.queue_reported(queued(fixed_digest(*n), worker_id));
    }
}

/// The digests pending in `worker_id`'s lane with their lane sequence numbers, front first.
fn lane_entries(
    proposer: &Proposer<MemDatabase>,
    worker_id: WorkerId,
) -> Vec<(BlockHash, Option<u64>)> {
    proposer
        .digests
        .get(&worker_id)
        .map(|lane| lane.iter().map(|d| (d.digest, d.seq)).collect())
        .unwrap_or_default()
}

/// The #1807 counters of one snapshot: `requeued_out_of_order_total` for `canceled` and
/// `committed`, then `requeued_order_unknown_total` for `unknown_seq` and `stale_resend`; `None`
/// for a series that is not registered.
fn order_totals(snapshotter: &metrics_util::debugging::Snapshotter) -> [Option<u64>; 4] {
    let snapshot = snapshotter.snapshot().into_vec();
    let total = |name: &str, key: &str, value: &str| {
        snapshot
            .iter()
            .find(|(composite, ..)| {
                composite.key().name() == name
                    && composite.key().labels().any(|l| l.key() == key && l.value() == value)
            })
            .and_then(|(.., total)| match total {
                metrics_util::debugging::DebugValue::Counter(total) => Some(*total),
                _ => None,
            })
    };
    let out_of_order = "tn_primary.requeued_out_of_order_total";
    let unknown = "tn_primary.requeued_order_unknown_total";
    [
        total(out_of_order, "cause", "canceled"),
        total(out_of_order, "cause", "committed"),
        total(unknown, "reason", "unknown_seq"),
        total(unknown, "reason", "stale_resend"),
    ]
}

/// A proposer for the first authority of a default fixture, its metric handles resolved under
/// `recorder`, with the fixture, the task manager and the header receiver it needs to propose.
fn order_proposer(
    recorder: &metrics_util::debugging::DebuggingRecorder,
) -> (CommitteeFixture<MemDatabase>, TaskManager, impl TnReceiver<Header>, Proposer<MemDatabase>) {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let task_manager = TaskManager::default();
    let cb = metrics::with_local_recorder(recorder, ConsensusBus::new);
    let rx_headers = cb.subscribe_headers();
    let proposer = metrics::with_local_recorder(recorder, || {
        proposer_on(&config, &committee, &cb, &task_manager)
    });
    (fixture, task_manager, rx_headers, proposer)
}

/// Each worker lane numbers its reported digests from zero, and a digest keeps its number through
/// a cancel re-queue and a commit re-queue. A digest of a header this proposer re-sent from the
/// store after a restart has no number, and its re-queue counts as `unknown_seq`, not as out of
/// order.
///
/// Catches: one sequence shared across workers, a sequence lost or renumbered when `push_payload`
/// rebuilds a re-queued digest from the bare header, and a restart's digests judged as if they had
/// a place in the lane.
#[tokio::test]
async fn test_lane_sequence_survives_requeues() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (_fixture, _task_manager, _rx_headers, mut proposer) = order_proposer(&recorder);
    let author = proposer.authority_id.clone();
    let d = fixed_digest;

    report_fixed(&mut proposer, 0, &[1, 2, 3]);
    report_fixed(&mut proposer, 1, &[101]);
    assert_eq!(lane_entries(&proposer, 0), [(d(1), Some(0)), (d(2), Some(1)), (d(3), Some(2))]);
    assert_eq!(lane_entries(&proposer, 1), [(d(101), Some(0))], "each worker counts from zero");

    let h1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[4]);
    let h2 = propose_and_track(&mut proposer).await;
    assert_eq!(payload_digests(&h2), [d(4)]);

    // a cancel re-queue keeps the numbers
    proposer.process_uncertified_header(h1.round(), h1.digest());
    assert_eq!(lane_entries(&proposer, 0), [(d(1), Some(0)), (d(2), Some(1)), (d(3), Some(2))]);
    assert_eq!(lane_entries(&proposer, 1), [(d(101), Some(0))]);

    // a commit re-queue keeps them too: round 3 commits and round 2 died behind it
    let h3 = propose_and_track(&mut proposer).await;
    assert_eq!(h3.payload().len(), 4);
    proposer.process_committed_headers(h3.round() + 2, vec![h3.round()]);
    assert_eq!(lane_entries(&proposer, 0), [(d(4), Some(3))]);
    report_fixed(&mut proposer, 0, &[5]);
    assert_eq!(
        lane_entries(&proposer, 0),
        [(d(4), Some(3)), (d(5), Some(4))],
        "numbers keep growing"
    );

    // a header re-sent from the store after a restart: this proposer never numbered its digest
    let restored = build_test_header(&author, 9, &[d(9)]);
    proposer.proposed_headers.insert(9, restored.clone());
    proposer.round = 10;
    proposer.process_uncertified_header(9, restored.digest());
    assert_eq!(lane_entries(&proposer, 0)[0], (d(9), None));

    // the cancel returned 1, 2 and 3 behind 4, which round 2 had drained; the commit re-queue
    // returned 4 with nothing newer out (round 3 committed only 1, 2 and 3)
    assert_eq!(order_totals(&snapshotter), [Some(3), Some(0), Some(1), Some(0)]);
}

/// Re-queues that keep each lane in order count nothing: a canceled header with nothing newer of
/// its worker out of the queue, two headers returned together, and an older header returned after
/// a newer one already came back (it lands ahead of it).
///
/// Catches: the rule read as "a newer digest was drained since", which would count the last two
/// although the queue ends up in order, and digests of one re-queue judged against each other.
#[tokio::test]
async fn test_requeues_that_keep_lane_order_count_nothing() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (_fixture, _task_manager, _rx_headers, mut proposer) = order_proposer(&recorder);
    let d = fixed_digest;

    // only worker 1 drains after round 1: worker 0's digests come back with nothing newer out
    report_fixed(&mut proposer, 0, &[1, 2]);
    let h1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 1, &[101]);
    propose_and_track(&mut proposer).await;
    proposer.process_uncertified_header(h1.round(), h1.digest());
    assert_eq!(lane_entries(&proposer, 0), [(d(1), Some(0)), (d(2), Some(1))]);

    // rounds 3 and 4 carry 1, 2 and 3; round 5 commits, so both die and return together
    let h3 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[3]);
    let h4 = propose_and_track(&mut proposer).await;
    assert_eq!((payload_digests(&h3), payload_digests(&h4)), (vec![d(1), d(2)], vec![d(3)]));
    let h5 = propose_and_track(&mut proposer).await;
    proposer.process_committed_headers(h5.round() + 2, vec![h5.round()]);
    assert_eq!(lane_entries(&proposer, 0), [(d(1), Some(0)), (d(2), Some(1)), (d(3), Some(2))]);

    // rounds 6 and 7 split them again, and round 7 is canceled before round 6
    proposer.max_header_num_of_batches = 3;
    let h6 = propose_and_track(&mut proposer).await;
    let h7 = propose_and_track(&mut proposer).await;
    propose_and_track(&mut proposer).await;
    assert_eq!(
        (payload_digests(&h6), payload_digests(&h7)),
        (vec![d(101), d(1), d(2)], vec![d(3)])
    );
    proposer.process_uncertified_header(h7.round(), h7.digest());
    proposer.process_uncertified_header(h6.round(), h6.digest());
    let lane: Vec<_> = lane_entries(&proposer, 0).into_iter().map(|(digest, _)| digest).collect();
    assert_eq!(lane, [d(1), d(2), d(3)], "the lane ends up in order");

    let lines = capture.events("digests resent");
    assert_eq!(lines.len(), 4);
    assert!(lines.iter().all(|line| line.u64_field("out_of_order") == Some(0)), "{lines:?}");
    assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
}

/// The `header proposed` line ends with the order-hold fields, after every earlier field:
/// `own_parent` is true only when the parents carry this node's certificate of the own header
/// tracked at the previous round, and `held`, `held_lanes` and `blockers` are zero until the hold
/// exists.
///
/// Catches: `own_parent` read from any own certificate of the previous round (one for another
/// header, or the genesis certificate under round 1), the fields missing or moved ahead of the
/// existing ones the stage tools parse by position.
#[tokio::test]
async fn test_header_proposed_reports_own_parent_and_hold_fields() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let (fixture, _task_manager, _rx_headers, mut proposer) = order_proposer(&recorder);
    let committee = fixture.committee();
    let epoch = committee.epoch();
    let all = fixture.authorities().count();

    // round 1 is built on genesis, which includes this node's genesis certificate
    let h1 = propose_and_track(&mut proposer).await;
    // round 2 is built on round 1 with this node's certificate of the tracked header
    let own = Certificate::new_unsigned_for_test(&committee, h1.clone(), Vec::new())
        .expect("own certificate");
    proposer.last_parents = std::iter::once(own)
        .chain(parents_at(&fixture, 1, epoch, all).into_iter().skip(1))
        .collect();
    let h2 = propose_and_track(&mut proposer).await;
    // round 3: an own certificate of round 2, but for another header than the tracked one
    proposer.last_parents = parents_at(&fixture, 2, epoch, all);
    let h3 = propose_and_track(&mut proposer).await;
    // round 4: the quorum without this node's certificate
    proposer.last_parents = parents_at(&fixture, 3, epoch, all).into_iter().skip(1).collect();
    let h4 = propose_and_track(&mut proposer).await;

    for (header, own_parent) in [(&h1, "false"), (&h2, "true"), (&h3, "false"), (&h4, "false")] {
        let line = proposed_line(&capture, header.round());
        assert_eq!(line.field("own_parent"), Some(own_parent), "round {}", header.round());
        for field in ["held", "held_lanes", "blockers"] {
            assert_eq!(line.u64_field(field), Some(0), "{field} at round {}", header.round());
        }
        let names: Vec<_> = line.fields.iter().map(|(name, _)| *name).collect();
        let tail = names.len() - 4;
        assert_eq!(
            names[tail..],
            ["own_parent", "held", "held_lanes", "blockers"],
            "the new fields close the line"
        );
    }
}

/// When the proposer shuts down it logs one `proposer stopped` line with its epoch and round and
/// what it leaves to the next epoch's orphan re-pool: the queued digests, the own headers proposed
/// and neither committed nor canceled with their digests, and the digests of a header still in
/// flight, the three counted apart.
///
/// Catches: a shutdown arm that still returns silently, a digest counted in two of the three
/// counts, and an in-flight header's digests left out.
#[tokio::test]
async fn test_proposer_stopped_reports_what_it_leaves() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let _rx_headers = cb.subscribe_headers();

    // two tracked headers with three digests, one header in flight with one, two digests queued
    let mut proposer = proposer_on(&config, &committee, &cb, &task_manager);
    report_fixed(&mut proposer, 0, &[1, 2]);
    propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 1, &[101]);
    propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[3]);
    proposer
        .propose_next_header(ProposeReason::MinDelay)
        .expect("proposal spawned")
        .await
        .expect("proposal task reported")
        .expect("header proposed");
    report_fixed(&mut proposer, 0, &[4]);
    report_fixed(&mut proposer, 1, &[102]);
    proposer.log_stopped();

    // the run loop's shutdown arm logs the line: a proposer with no parents for its round and
    // nothing to re-send only waits
    let mut idle = proposer_on(&config, &committee, &cb, &task_manager);
    idle.round = 5;
    idle.last_parents.clear();
    idle.spawn(&task_manager);
    config.shutdown().notify();
    tokio::time::timeout(Duration::from_secs(10), async {
        while capture.events("proposer stopped").len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the shutdown arm logs proposer stopped");

    let lines = capture.events("proposer stopped");
    assert_eq!(lines.len(), 2);
    let names = [
        "epoch",
        "round",
        "queued_digests",
        "proposed_uncommitted",
        "uncommitted_digests",
        "in_flight_digests",
    ];
    let fields = |line: &crate::log_capture::CapturedEvent| names.map(|name| line.u64_field(name));
    let epoch = Some(u64::from(committee.epoch()));
    assert_eq!(fields(&lines[0]), [epoch, Some(3), Some(2), Some(2), Some(3), Some(1)]);
    assert_eq!(fields(&lines[1]), [epoch, Some(5), Some(0), Some(0), Some(0), Some(0)]);
}

/// A fixture with default consensus parameters, for the order hold's tests.
fn hold_fixture() -> CommitteeFixture<MemDatabase> {
    exec_fixture(tn_config::Parameters::default())
}

/// The proposer of `fixture`'s first authority, its metric handles resolved under `recorder`, and
/// the header receiver it sends to.
fn hold_proposer(
    fixture: &CommitteeFixture<MemDatabase>,
    recorder: &metrics_util::debugging::DebuggingRecorder,
    task_manager: &TaskManager,
) -> (impl TnReceiver<Header>, Proposer<MemDatabase>) {
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let cb = metrics::with_local_recorder(recorder, ConsensusBus::new);
    let rx_headers = cb.subscribe_headers();
    let proposer = metrics::with_local_recorder(recorder, || {
        proposer_on(&config, &committee, &cb, task_manager)
    });
    (rx_headers, proposer)
}

/// Round-`round` parents for the proposer of `fixture`'s first authority: the three peers'
/// certificates, plus this node's certificate of `own` when given.
fn hold_parents(
    fixture: &CommitteeFixture<MemDatabase>,
    round: Round,
    own: Option<&Header>,
) -> Vec<Certificate> {
    let committee = fixture.committee();
    let all = fixture.authorities().count();
    let mut parents: Vec<_> =
        parents_at(fixture, round, committee.epoch(), all).into_iter().skip(1).collect();
    if let Some(own) = own {
        let cert = Certificate::new_unsigned_for_test(&committee, own.clone(), Vec::new())
            .expect("own certificate");
        parents.push(cert);
    }
    parents
}

/// Build and track `proposer`'s next header on `parents`.
async fn build_on(proposer: &mut Proposer<MemDatabase>, parents: Vec<Certificate>) -> Header {
    proposer.last_parents = parents;
    propose_and_track(proposer).await
}

/// The order-hold fields of round `round`'s `header proposed` line: `own_parent`, then `held`,
/// `held_lanes` and `blockers`.
fn hold_fields(capture: &crate::log_capture::LogCapture, round: Round) -> (bool, [u64; 3]) {
    let line = proposed_line(capture, round);
    let counts = ["held", "held_lanes", "blockers"].map(|name| line.u64_field(name).unwrap());
    (line.field("own_parent") == Some("true"), counts)
}

/// `tn_primary.headers_held_total` and `tn_primary.digests_held` in one snapshot; `None` for a
/// series that is not registered. A snapshot drains every counter of the recorder, so the count is
/// what was added since the previous snapshot, of this helper or of `order_totals`.
fn hold_metrics(snapshotter: &metrics_util::debugging::Snapshotter) -> (Option<u64>, Option<f64>) {
    use metrics_util::debugging::DebugValue;
    let snapshot = snapshotter.snapshot().into_vec();
    let value = |name: &str| {
        snapshot.iter().find(|(key, ..)| key.key().name() == name).map(|(.., value)| value)
    };
    let headers = match value("tn_primary.headers_held_total") {
        Some(DebugValue::Counter(total)) => Some(*total),
        _ => None,
    };
    let digests = match value("tn_primary.digests_held") {
        Some(DebugValue::Gauge(held)) => Some(held.0),
        _ => None,
    };
    (headers, digests)
}

/// Issue #1807's canceled case. Round 1 carries worker 0's digests 1 and 2 and is never certified;
/// round 2 is built without it while worker 0 reports 3 and worker 1 reports 101; then the
/// certifier cancels round 1 and round 3 is built on round 2.
///
/// Round 2 holds worker 0 only (worker 1 drains), the cancel puts 1 and 2 at the front, round 3
/// carries 1, 2, 3 in order, nothing counts out of order, and the hold's fields and metrics say
/// what was held. Without the hold, round 2 would carry 3 and the cancel would return 1 and 2
/// behind it, so worker 0's lane would execute 3 before 1 and 2.
///
/// Catches: a hold that misses the canceled kind, holds every lane instead of the blocker's, or
/// lets the next header after the cancel stay held.
#[tokio::test]
async fn test_order_hold_keeps_a_canceled_headers_lane_in_order() {
    use crate::log_capture::LogCapture;

    let d = fixed_digest;
    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);

    report_fixed(&mut proposer, 0, &[1, 2]);
    let h1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[3]);
    report_fixed(&mut proposer, 1, &[101]);
    let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
    proposer.process_uncertified_header(h1.round(), h1.digest());
    let h3 = build_on(&mut proposer, hold_parents(&fixture, 2, Some(&h2))).await;

    assert_eq!(payload_entries(&h1), [(d(1), 0), (d(2), 0)]);
    assert_eq!(payload_entries(&h2), [(d(101), 1)], "only worker 0 is held");
    assert_eq!(payload_entries(&h3), [(d(1), 0), (d(2), 0), (d(3), 0)]);
    assert_eq!(hold_fields(&capture, 2), (false, [1, 1, 1]));
    assert_eq!(hold_fields(&capture, 3), (true, [0, 0, 0]));
    assert_eq!(hold_metrics(&snapshotter), (Some(1), Some(0.0)));
    assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
}

/// The race: round 1 is certified, but its certificate misses round 2's parents, so round 2 holds
/// worker 0's lane. The commit notice that settles round 1 releases it either way: naming round 1
/// (committed, so the lane's next digest may follow it), or naming only round 2 (round 1 died and
/// its digest returns to the front, ahead of the held one). A cancel report for round 1 that
/// arrives after the notice moves nothing.
///
/// Catches: a lane released before the older header is settled, a lane left held after the notice,
/// or a late report that re-queues a settled header's digests a second time.
#[tokio::test]
async fn test_order_hold_releases_the_lane_when_the_notice_settles_the_blocker() {
    use crate::log_capture::LogCapture;

    let d = fixed_digest;
    for named in [1, 2] {
        let capture = LogCapture::new("consensus::metrics");
        let _capture_guard = tracing::subscriber::set_default(capture.clone());
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let fixture = hold_fixture();
        let task_manager = TaskManager::default();
        let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);

        report_fixed(&mut proposer, 0, &[1]);
        let h1 = propose_and_track(&mut proposer).await;
        report_fixed(&mut proposer, 0, &[2]);
        let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
        assert!(h2.payload().is_empty(), "named {named}: round 2 holds worker 0");
        assert_eq!(hold_fields(&capture, 2), (false, [1, 1, 1]));

        proposer.process_committed_headers(4, vec![named]);
        proposer.process_uncertified_header(h1.round(), h1.digest());
        let h3 = build_on(&mut proposer, hold_parents(&fixture, 2, Some(&h2))).await;
        let carried = if named == 1 { vec![(d(2), 0)] } else { vec![(d(1), 0), (d(2), 0)] };
        assert_eq!(payload_entries(&h3), carried, "named {named}");
        // a committed round 2 leaves the tracked set, so round 3 no longer counts it as its own
        // parent
        assert_eq!(hold_fields(&capture, 3), (named == 1, [0, 0, 0]), "named {named}");
        assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
        assert_eq!(capture.events("digests resent").len(), usize::from(named == 2));
    }
}

/// While the own chain is intact, the hold changes nothing: a script of rounds that each cite the
/// previous own header drains every lane in rotation, every line reports zero held, and
/// `headers_held_total` stays 0. That is the hold's cost on a healthy node. The payloads are
/// pinned as a golden list, the order this script drained without the hold.
///
/// Catches: a hold that reads a proven own parent as a blocker, or a held-lane filter that moves
/// the drain's cursor when nothing is held.
#[tokio::test]
async fn test_order_hold_holds_nothing_on_an_intact_own_chain() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);

    let mut previous = None;
    let mut payloads = Vec::new();
    for round in 1..=8u8 {
        report_fixed(&mut proposer, 0, &[round * 2, round * 2 + 1]);
        report_fixed(&mut proposer, u16::from(round % 3), &[100 + round]);
        let header = match &previous {
            None => propose_and_track(&mut proposer).await,
            Some(own) => {
                let parents = hold_parents(&fixture, Round::from(round) - 1, Some(own));
                build_on(&mut proposer, parents).await
            }
        };
        assert_eq!(hold_fields(&capture, header.round()), (round > 1, [0, 0, 0]));
        payloads.push(payload_entries(&header));
        previous = Some(header);
    }
    assert_eq!(hold_metrics(&snapshotter).0, Some(0));
    // (digest number, worker) per header, in payload order
    let golden: [&[(u8, WorkerId)]; 8] = [
        &[(2, 0), (101, 1), (3, 0)],
        &[(102, 2), (4, 0), (5, 0)],
        &[(6, 0), (7, 0), (103, 0)],
        &[(104, 1), (8, 0), (9, 0)],
        &[(105, 2), (10, 0), (11, 0)],
        &[(12, 0), (13, 0), (106, 0)],
        &[(107, 1), (14, 0), (15, 0)],
        &[(108, 2), (16, 0), (17, 0)],
    ];
    let golden: Vec<Vec<(BlockHash, WorkerId)>> = golden
        .iter()
        .map(|entries| {
            entries.iter().map(|(n, worker_id)| (fixed_digest(*n), *worker_id)).collect()
        })
        .collect();
    assert_eq!(payloads, golden, "an intact own chain drains exactly as without the hold");
}

/// `unproven` carries through the own chain: once round 2 is built without its own parent, round 1
/// stays a blocker for rounds 3 and 4 although each cites the previous own header, until the cancel
/// report settles it. Two cancel reports handled newest first before the next header (the reverse
/// report order) leave both lanes in order, because the two canceled headers carry different
/// workers. A round jump builds without an own parent, so it holds every tracked lane.
///
/// Catches: a record that proves the whole chain below an own parent (it proves only what that
/// parent proved), a reverse report order that reorders a lane under the hold, and a jump that
/// takes the newest tracked header as an own parent.
#[tokio::test]
async fn test_order_hold_carries_unproven_through_the_chain_and_round_jumps() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let d = fixed_digest;

    // transitive: round 1 is never certified, rounds 3 and 4 cite their own parents
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);
    report_fixed(&mut proposer, 0, &[1]);
    let h1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[2]);
    report_fixed(&mut proposer, 1, &[101]);
    let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
    report_fixed(&mut proposer, 1, &[102]);
    let h3 = build_on(&mut proposer, hold_parents(&fixture, 2, Some(&h2))).await;
    report_fixed(&mut proposer, 1, &[103]);
    let h4 = build_on(&mut proposer, hold_parents(&fixture, 3, Some(&h3))).await;
    for (header, carried) in [(&h2, 101), (&h3, 102), (&h4, 103)] {
        assert_eq!(payload_entries(header), [(d(carried), 1)], "round {}", header.round());
    }
    assert_eq!(hold_fields(&capture, 2), (false, [1, 1, 1]));
    assert_eq!(hold_fields(&capture, 3), (true, [1, 1, 1]));
    assert_eq!(hold_fields(&capture, 4), (true, [1, 1, 1]));
    proposer.process_uncertified_header(h1.round(), h1.digest());
    let h5 = build_on(&mut proposer, hold_parents(&fixture, 4, Some(&h4))).await;
    assert_eq!(payload_entries(&h5), [(d(1), 0), (d(2), 0)]);
    assert_eq!(hold_fields(&capture, 5), (true, [0, 0, 0]));

    // reverse report order: rounds 1 and 2 die, their reports are handled newest first
    let fixture = hold_fixture();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);
    report_fixed(&mut proposer, 0, &[11]);
    let r1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[12]);
    report_fixed(&mut proposer, 1, &[111]);
    let r2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
    report_fixed(&mut proposer, 1, &[112]);
    let r3 = build_on(&mut proposer, hold_parents(&fixture, 2, None)).await;
    assert_eq!(payload_entries(&r2), [(d(111), 1)]);
    assert!(r3.payload().is_empty(), "round 3 holds both lanes");
    assert_eq!(hold_fields(&capture, 3), (false, [2, 2, 2]));
    proposer.process_uncertified_header(r2.round(), r2.digest());
    proposer.process_uncertified_header(r1.round(), r1.digest());
    let r4 = build_on(&mut proposer, hold_parents(&fixture, 3, Some(&r3))).await;
    let lane = |worker_id: WorkerId| -> Vec<BlockHash> {
        r4.payload().iter().filter(|(_, w)| **w == worker_id).map(|(digest, _)| *digest).collect()
    };
    assert_eq!((lane(0), lane(1)), (vec![d(11), d(12)], vec![d(111), d(112)]));

    // a round jump: the parents of round 5 cannot cite an own header of round 5
    let fixture = hold_fixture();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);
    report_fixed(&mut proposer, 0, &[21]);
    propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[22]);
    proposer.process_parents(hold_parents(&fixture, 5, None), 5).expect("parents of round 5");
    let jumped = propose_and_track(&mut proposer).await;
    assert_eq!(jumped.round(), 6);
    assert!(jumped.payload().is_empty(), "the jump holds worker 0");
    assert_eq!(hold_fields(&capture, 6), (false, [1, 1, 1]));

    assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
}

/// A cancel report the certifier drops leaves the canceled header tracked, so its lane stays held.
/// Full channel: the next own commit notice above it re-queues it with cause `committed`, at the
/// front, and the lane drains in order. Shutdown guard and no subscriber: both happen only while
/// the proposer is stopping or gone, so the epoch ends with the lane held; `proposer stopped`
/// reports the held digest as queued and the blocker as proposed and uncommitted, the held gauge
/// goes back to 0, and the next epoch's proposer starts with nothing held.
///
/// Catches: a hold that needs the cancel report to ever release a lane, a held digest lost at the
/// epoch's end, or a hold carried into the next epoch.
#[tokio::test]
async fn test_order_hold_survives_a_dropped_cancel_report() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let d = fixed_digest;
    for full_channel in [true, false] {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let fixture = hold_fixture();
        let task_manager = TaskManager::default();
        let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);

        // round 1 is canceled and its report never arrives
        report_fixed(&mut proposer, 0, &[1]);
        propose_and_track(&mut proposer).await;
        report_fixed(&mut proposer, 0, &[2]);
        let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
        let h3 = build_on(&mut proposer, hold_parents(&fixture, 2, Some(&h2))).await;
        assert!(h2.payload().is_empty() && h3.payload().is_empty(), "worker 0 stays held");
        assert_eq!(hold_metrics(&snapshotter), (Some(2), Some(1.0)));
        assert_eq!(proposer.pending_digests_len(), 1, "a held digest counts toward the threshold");

        if full_channel {
            // round 3 commits with round 2 below it; round 1 died
            proposer.process_committed_headers(5, vec![2, 3]);
            let h4 = build_on(&mut proposer, hold_parents(&fixture, 3, Some(&h3))).await;
            assert_eq!(payload_entries(&h4), [(d(1), 0), (d(2), 0)]);
            // no header held since the snapshot above
            assert_eq!(hold_metrics(&snapshotter), (Some(0), Some(0.0)));
            assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
        } else {
            proposer.log_stopped();
            let stopped = capture.events("proposer stopped");
            let line = stopped.last().expect("proposer stopped");
            let fields = ["queued_digests", "proposed_uncommitted", "uncommitted_digests"]
                .map(|name| line.u64_field(name));
            assert_eq!(fields, [Some(1), Some(3), Some(1)]);
            assert_eq!(hold_metrics(&snapshotter), (Some(0), Some(0.0)), "the gauge is reset");

            // the next epoch's proposer has its own order: nothing tracked, nothing held
            let next = hold_fixture();
            let (_rx_next, mut proposer) = hold_proposer(&next, &recorder, &task_manager);
            report_fixed(&mut proposer, 0, &[1, 2]);
            let first = propose_and_track(&mut proposer).await;
            assert_eq!(payload_entries(&first), [(d(1), 0), (d(2), 0)]);
        }
    }
}

/// A restarted proposer re-sends its stored round-3 header, which carries a digest it never
/// numbered, and tracks it with a record that proves nothing below it. The next header that cites
/// it as its own parent holds nothing; one built without it holds the stored header's lane until
/// the notice that commits it.
///
/// Catches: a re-proposal left without a record (so its own child could not prove it), or one
/// whose lanes are not held while it may still commit.
#[tokio::test]
async fn test_order_hold_restart_reproposal_records_its_header() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let d = fixed_digest;
    for cited in [true, false] {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let fixture = hold_fixture();
        let author = fixture.authorities().next().unwrap().id();
        let stored = build_header_at(&author, 3, fixture.committee().epoch(), &[d(9)]);
        let config = fixture.authorities().next().unwrap().consensus_config();
        config.node_storage().write_last_proposed(&stored).expect("stored header");
        let task_manager = TaskManager::default();
        let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);
        assert_eq!(proposer.round, 2);

        let resent = build_on(&mut proposer, hold_parents(&fixture, 2, None)).await;
        assert_eq!(resent.digest(), stored.digest(), "the stored header, byte for byte");
        report_fixed(&mut proposer, 0, &[10]);
        let own = cited.then_some(&stored);
        let h4 = build_on(&mut proposer, hold_parents(&fixture, 3, own)).await;
        if cited {
            assert_eq!(payload_entries(&h4), [(d(10), 0)]);
            assert_eq!(hold_fields(&capture, 4), (true, [0, 0, 0]));
        } else {
            assert!(h4.payload().is_empty(), "the stored header's lane is held");
            assert_eq!(hold_fields(&capture, 4), (false, [1, 1, 1]));
            proposer.process_committed_headers(5, vec![3]);
            let h5 = build_on(&mut proposer, hold_parents(&fixture, 4, Some(&h4))).await;
            assert_eq!(payload_entries(&h5), [(d(10), 0)]);
        }
    }
}

/// The max-delay tick re-sends round 2, whose record says it proves round 1 through its own parent.
/// The re-send keeps that record, so round 3, citing round 2, does not hold round 1's lane although
/// round 1 is still tracked and uncommitted.
///
/// Catches: a re-send that resets the record to "proves nothing", which would hold every lane of
/// the intact chain below it after each max-delay re-send.
#[tokio::test(start_paused = true)]
async fn test_order_hold_max_delay_resend_keeps_the_record() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.subscribe_headers();
    let mut proposer = metrics::with_local_recorder(&recorder, || {
        proposer_on(&config, &committee, &cb, &task_manager)
    });
    let (d1, d2, d3) = (B256::random(), B256::random(), B256::random());
    proposer.digests = BTreeMap::from([(0, VecDeque::from([queued(d1, 0)]))]);
    proposer.spawn(&task_manager);
    let h1 = next_header_at(&mut rx_headers, 1).await;
    assert_eq!(payload_digests(&h1), [d1]);

    send_digest(&cb, d2).await;
    cb.parents().send((hold_parents(&fixture, 1, Some(&h1)), 1)).await.expect("parents sent");
    let h2 = next_header_at(&mut rx_headers, 2).await;
    assert_eq!(payload_digests(&h2), [d2]);
    let resent = tokio::time::timeout(Duration::from_secs(30), rx_headers.recv())
        .await
        .expect("the max-delay tick re-sends round 2")
        .expect("header channel open");
    assert_eq!(resent.digest(), h2.digest());

    send_digest(&cb, d3).await;
    cb.parents().send((hold_parents(&fixture, 2, Some(&h2)), 2)).await.expect("parents sent");
    let h3 = next_header_at(&mut rx_headers, 3).await;
    assert_eq!(payload_digests(&h3), [d3], "round 1 is proven through round 2");
    assert_eq!(hold_fields(&capture, 3), (true, [0, 0, 0]));
}

/// A committed header that comes back after its commit notice (a max-delay re-send that raced the
/// notice, or a new header whose result came after it) is not tracked again: it never blocks a
/// later header, its committed digests are never re-queued, and its digests leave the order
/// bookkeeping.
///
/// Catches: a stale re-send that holds its lanes behind every header without an own parent until
/// the next own commit, and the duplicate re-queue of its committed digests.
#[tokio::test]
async fn test_order_hold_stale_resend_is_never_a_blocker() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("consensus::metrics");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);
    let d = fixed_digest;

    // the max-delay re-send of committed round 1 comes back after the notice
    report_fixed(&mut proposer, 0, &[1, 2]);
    let h1 = propose_and_track(&mut proposer).await;
    proposer.process_committed_headers(3, vec![h1.round()]);
    proposer.record_held_resent(h1.round());
    proposer.handle_proposal_result(Ok(h1.clone())).expect("re-send handled");
    assert!(proposer.proposed_headers.is_empty(), "a settled header is not tracked again");

    // round 2 cites no own parent and still holds nothing
    report_fixed(&mut proposer, 0, &[3]);
    let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
    assert_eq!(payload_entries(&h2), [(d(3), 0)]);
    assert_eq!(hold_fields(&capture, 2), (false, [0, 0, 0]));

    // a new header whose own notice is handled before its result
    report_fixed(&mut proposer, 0, &[4]);
    proposer.last_parents = hold_parents(&fixture, 2, Some(&h2));
    let h3 = proposer
        .propose_next_header(ProposeReason::MinDelay)
        .expect("proposal spawned")
        .await
        .expect("proposal task reported")
        .expect("header proposed");
    proposer.process_committed_headers(5, vec![h2.round(), h3.round()]);
    proposer.handle_proposal_result(Ok(h3.clone())).expect("result handled");
    assert!(proposer.proposed_headers.is_empty());
    assert_eq!(proposer.lane_seqs.out_of_queue().count(), 0, "no entry outlives its header");
    assert!(proposer.proposed_first_received.is_empty(), "nor does a first receipt");

    proposer.process_committed_headers(7, vec![]);
    assert!(lane_entries(&proposer, 0).is_empty(), "no committed digest comes back");
    assert!(capture.events("digests resent").is_empty());
    assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
}

/// The model test's shrunk hold-off counterexample (`order_hold_model`), run through the real
/// proposer: worker 0 reports d0, round 1 carries it; worker 0 reports d1, round 2 is built while
/// round 1's cancel report is still pending (so without round 1 among its parents); round 2 commits
/// and its notice re-queues round 1 with cause `committed`; round 3 is built on round 2 and
/// commits. Committed order is round 2's payload, then round 3's.
///
/// Round 2 holds worker 0, so d0 and d1 execute in order and nothing counts; without the hold, d1
/// would execute before d0 and the re-queue would count one out of order.
///
/// Catches: a proposer whose drain differs from the model's under the hold, in the case the model
/// found first without it.
#[tokio::test]
async fn test_order_hold_replays_the_model_counterexample() {
    let d = fixed_digest;
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);

    report_fixed(&mut proposer, 0, &[0]);
    let h1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[1]);
    let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, None)).await;
    proposer.process_committed_headers(4, vec![h2.round()]);
    // the cancel report for round 1 arrives late and finds it settled
    proposer.process_uncertified_header(h1.round(), h1.digest());
    let h3 = build_on(&mut proposer, hold_parents(&fixture, 2, Some(&h2))).await;
    proposer.process_committed_headers(5, vec![h3.round()]);

    let executed: Vec<BlockHash> = [&h2, &h3].into_iter().flat_map(payload_digests).collect();
    assert_eq!(executed, [d(0), d(1)], "the hold keeps worker 0's order");
    assert_eq!(order_totals(&snapshotter), [Some(0), Some(0), Some(0), Some(0)]);
}

/// The garbage-collection residual: round 2 cites round 1 as its own parent and carries worker 0's
/// next digest, then commits while round 1, cut below the collection depth, is never ordered. Its
/// re-queue lands behind a committed newer digest: counted out of order, and one
/// warning per re-queue says the hold did not cover the lane.
///
/// Catches: a residual that passes silently, or a warning per digest.
#[tokio::test]
async fn test_order_hold_warns_once_per_requeue_out_of_order() {
    use crate::log_capture::LogCapture;

    let capture = LogCapture::new("primary::proposer");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let fixture = hold_fixture();
    let task_manager = TaskManager::default();
    let (_rx_headers, mut proposer) = hold_proposer(&fixture, &recorder, &task_manager);

    report_fixed(&mut proposer, 0, &[1, 2]);
    let h1 = propose_and_track(&mut proposer).await;
    report_fixed(&mut proposer, 0, &[3]);
    let h2 = build_on(&mut proposer, hold_parents(&fixture, 1, Some(&h1))).await;
    assert_eq!(payload_digests(&h2), [fixed_digest(3)], "a proven own parent holds nothing");
    proposer.process_committed_headers(60, vec![h2.round()]);

    let warnings = capture.events("digests re-queued out of order under the order hold");
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].u64_field("out_of_order"), Some(2));
    assert_eq!(warnings[0].field("cause"), Some("committed"));
    assert_eq!(order_totals(&snapshotter), [Some(0), Some(2), Some(0), Some(0)]);
}
