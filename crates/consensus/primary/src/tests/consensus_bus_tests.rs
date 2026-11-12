// SPDX-License-Identifier: Apache-2.0
//! Tests for ConsensusBus helper methods.

use crate::{consensus_bus::QueChannel, ConsensusBus, ConsensusBusApp, NodeMode};
use tn_types::{EpochRecord, TnReceiver, TnSender};

#[tokio::test]
async fn test_is_cvv() {
    let bus = ConsensusBusApp::new();
    // Default is CvvActive, which is a CVV
    assert!(bus.is_cvv());

    bus.node_mode().send_replace(NodeMode::CvvInactive);
    assert!(bus.is_cvv());

    bus.node_mode().send_replace(NodeMode::Observer);
    assert!(!bus.is_cvv());
}

#[tokio::test]
async fn test_is_active_cvv_default() {
    let bus = ConsensusBus::new();
    // Default NodeMode is CvvActive
    assert!(bus.is_active_cvv());
}

#[tokio::test]
async fn test_is_active_cvv_after_change() {
    let bus = ConsensusBusApp::new();
    bus.node_mode().send_replace(NodeMode::Observer);
    assert!(!bus.is_active_cvv());

    bus.node_mode().send_replace(NodeMode::CvvInactive);
    assert!(!bus.is_active_cvv());

    bus.node_mode().send_replace(NodeMode::CvvActive);
    assert!(bus.is_active_cvv());
}

#[tokio::test]
async fn test_is_cvv_inactive() {
    let bus = ConsensusBusApp::new();
    // Default is CvvActive, not inactive
    assert!(!bus.is_cvv_inactive());

    bus.node_mode().send_replace(NodeMode::CvvInactive);
    assert!(bus.is_cvv_inactive());

    bus.node_mode().send_replace(NodeMode::CvvActive);
    assert!(!bus.is_cvv_inactive());

    bus.node_mode().send_replace(NodeMode::Observer);
    assert!(!bus.is_cvv_inactive());
}

#[tokio::test]
async fn test_committed_round_default() {
    let bus = ConsensusBus::new();
    assert_eq!(bus.app().committed_round(), 0);
}

#[tokio::test]
async fn test_committed_round_after_update() {
    let bus = ConsensusBus::new();
    bus.app().committed_round_updates().send_replace(42);
    assert_eq!(bus.app().committed_round(), 42);
}

#[tokio::test]
async fn test_latest_block_num_hash_default() {
    let bus = ConsensusBusApp::new();
    let num_hash = bus.latest_execution_block_num_hash();
    // Default is empty, so number should be 0
    assert_eq!(num_hash.number, 0);
}

#[tokio::test]
async fn test_primary_round_default() {
    let bus = ConsensusBus::new();
    assert_eq!(bus.app().primary_round(), 0);
}

#[tokio::test]
async fn test_primary_round_after_update() {
    let bus = ConsensusBus::new();
    bus.app().primary_round_updates().send_replace(100);
    assert_eq!(bus.app().primary_round(), 100);
}

#[tokio::test]
async fn test_recent_blocks_capacity() {
    let bus = ConsensusBusApp::new();
    // Default gc_depth is used for capacity
    assert!(bus.recent_blocks_capacity() > 0);
}

#[tokio::test]
async fn test_recent_blocks_capacity_custom() {
    let bus = ConsensusBusApp::new_with_recent_blocks(100);
    assert_eq!(bus.recent_blocks_capacity(), 100);
}

#[tokio::test]
async fn test_que_channel_send_no_subscriber() {
    // Create a QueChannel, send without subscribing.
    // The send should be a no-op and return Ok(()).
    let channel = QueChannel::<i32>::new();

    let result = channel.send(42).await;
    assert!(result.is_ok());

    let result = channel.try_send(99);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_que_channel_send_after_subscribe() {
    // Create a QueChannel, subscribe, then send.
    // The message should arrive at the receiver.
    let channel = QueChannel::<i32>::new();

    let mut rx = channel.subscribe();

    channel.send(42).await.unwrap();
    let received = rx.recv().await.unwrap();
    assert_eq!(received, 42);
}

#[tokio::test]
async fn test_que_channel_send_after_subscribe_then_drop() {
    // Create a QueChannel, subscribe, drop the receiver,
    // then send. The send should be a no-op.
    let channel = QueChannel::<i32>::new();

    let rx = channel.subscribe();
    drop(rx);

    let result = channel.send(42).await;
    assert!(result.is_ok());

    let result = channel.try_send(99);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_que_channel_resubscribe_after_drop() {
    // Simulate an epoch transition: subscribe, use, drop, then subscribe again.
    // This is the real lifecycle for QueChannels like new_epoch_votes and
    // primary_network_events across epoch boundaries.
    let channel = QueChannel::<i32>::new();

    // Epoch 1: subscribe and exchange messages
    let mut rx1 = channel.subscribe();
    channel.send(1).await.unwrap();
    channel.try_send(2).unwrap();
    assert_eq!(rx1.recv().await.unwrap(), 1);
    assert_eq!(rx1.recv().await.unwrap(), 2);

    // Epoch 1 ends: drop the receiver, sends become no-ops
    drop(rx1);
    assert!(channel.send(3).await.is_ok());
    assert!(channel.try_send(4).is_ok());

    // Epoch 2: re-subscribe on the same channel
    let mut rx2 = channel.subscribe();
    channel.send(5).await.unwrap();
    assert_eq!(rx2.recv().await.unwrap(), 5);

    // Messages sent during the no-op gap (3, 4) are not received
    assert!(rx2.try_recv().is_err());
}

#[tokio::test]
async fn test_epoch_record_watch_default() {
    let bus = ConsensusBus::new();
    // Default is None
    assert!(bus.app().epoch_record_watch().borrow().is_none());
}

#[tokio::test]
async fn test_epoch_record_watch_send_and_receive() {
    let bus = ConsensusBus::new();
    let mut rx = bus.app().epoch_record_watch().subscribe();

    let epoch_rec = EpochRecord { epoch: 1, ..Default::default() };
    bus.app().epoch_record_watch().send_replace(Some(epoch_rec.clone()));

    rx.changed().await.unwrap();
    let received = rx.borrow_and_update().clone();
    assert!(received.is_some());
    assert_eq!(received.unwrap().epoch, 1);
}

#[tokio::test]
async fn test_epoch_record_watch_updates() {
    let bus = ConsensusBus::new();
    let mut rx = bus.app().epoch_record_watch().subscribe();

    // Send first epoch record
    let epoch_rec1 = EpochRecord { epoch: 1, ..Default::default() };
    bus.app().epoch_record_watch().send_replace(Some(epoch_rec1));
    rx.changed().await.unwrap();
    assert_eq!(rx.borrow_and_update().as_ref().unwrap().epoch, 1);

    // Send second epoch record
    let epoch_rec2 = EpochRecord { epoch: 2, ..Default::default() };
    bus.app().epoch_record_watch().send_replace(Some(epoch_rec2));
    rx.changed().await.unwrap();
    assert_eq!(rx.borrow_and_update().as_ref().unwrap().epoch, 2);
}

// ---------------------------------------------------------------------------
// wait_for_execution canonical-DB fallback on a ring miss (issue #1036)
// ---------------------------------------------------------------------------

use crate::{ExecutionCheck, ExecutionCheckError};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tn_types::{
    BlockHash, BlockNumHash, BlockNumber, CanonicalExecutionReader, ConsensusHeaderDigest,
    ConsensusNumHash, ExecHeader, SealedHeader,
};

/// In-memory [`CanonicalExecutionReader`] for tests: answers from a fixed number -> hash map, so
/// the ring-eviction fallback can be exercised without a real execution database. A height missing
/// from the map reads as `None`, the answer of a failed or not yet persisted read.
#[derive(Debug, Default)]
struct FakeCanonicalReader {
    by_number: HashMap<BlockNumber, BlockHash>,
    floor: Option<BlockNumber>,
}

impl FakeCanonicalReader {
    fn with(number: BlockNumber, hash: BlockHash) -> Self {
        let mut by_number = HashMap::new();
        by_number.insert(number, hash);
        Self { by_number, floor: None }
    }

    /// The same reader for a datadir restored from a snapshot at `floor`.
    fn restored_at(mut self, floor: BlockNumber) -> Self {
        self.floor = Some(floor);
        self
    }
}

impl CanonicalExecutionReader for FakeCanonicalReader {
    fn canonical_execution_hash(&self, number: BlockNumber) -> Option<BlockHash> {
        // like the execution environment, nothing below the floor is attested
        if self.floor.is_some_and(|floor| number < floor) {
            return None;
        }
        self.by_number.get(&number).copied()
    }

    fn restored_state_floor(&self) -> Option<BlockNumber> {
        self.floor
    }
}

/// Seal a distinct execution header at `number`; `salt` (written to `extra_data`) keeps hashes
/// unique across blocks sharing a height.
fn sealed_at(number: BlockNumber, salt: u8) -> SealedHeader {
    SealedHeader::seal_slow(ExecHeader {
        number,
        extra_data: vec![salt].into(),
        ..Default::default()
    })
}

/// Push `header` onto the bus `recent_blocks` ring as the latest executed tip.
fn push_tip(bus: &ConsensusBusApp, header: SealedHeader) {
    let number = header.num_hash().number;
    let consensus_num_hash = ConsensusNumHash::new(number, ConsensusHeaderDigest::from([0u8; 32]));
    bus.recent_blocks()
        .send_modify(|blocks| blocks.push_latest(0, consensus_num_hash, Some(header)));
}

#[tokio::test]
async fn wait_for_execution_confirms_ring_evicted_block_via_canonical_db() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);

    // The queried tip is NOT in the ring (evicted), but the ring has advanced past its height so
    // `wait_for_execution` reaches the hash check rather than blocking.
    let target = sealed_at(10, 1);
    push_tip(&bus, sealed_at(12, 2));

    // The canonical DB confirms the evicted tip is persisted at its height: not a fork.
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::with(
        target.num_hash().number,
        target.num_hash().hash,
    )));

    assert!(bus.wait_for_execution(target.num_hash()).await.is_ok());
}

#[tokio::test]
async fn wait_for_execution_reports_fork_when_canonical_hash_differs() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);

    let target = sealed_at(10, 1);
    push_tip(&bus, sealed_at(12, 2));

    // The DB holds a DIFFERENT hash at the target height: a genuine fork, still fatal.
    let forked = sealed_at(10, 9);
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::with(
        target.num_hash().number,
        forked.num_hash().hash,
    )));

    assert_eq!(bus.wait_for_execution(target.num_hash()).await, Err(ExecutionCheckError::Fork));
}

#[tokio::test]
async fn wait_for_execution_without_reader_keeps_ring_only_fork_check() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);

    let target = sealed_at(10, 1);
    push_tip(&bus, sealed_at(12, 2));

    // No reader installed (tests / engine-less tools): a ring miss is still reported as a fork.
    assert!(bus.wait_for_execution(target.num_hash()).await.is_err());
}

// ---------------------------------------------------------------------------
// the three-way execution check: number and hash, the restore floor, read errors (SEC-1/6/8)
// ---------------------------------------------------------------------------

/// A block in the ring matches only by its own number and hash; a block above the executed tip
/// (or any block before anything is executed) is not yet checkable.
#[test]
fn execution_check_matches_in_ring_and_waits_above_tip() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    let executed = sealed_at(10, 1);
    assert_eq!(bus.check_execution_block(executed.num_hash()), ExecutionCheck::NotYet, "empty");

    push_tip(&bus, executed.clone());
    assert_eq!(bus.check_execution_block(executed.num_hash()), ExecutionCheck::Match);
    let above = sealed_at(11, 1);
    assert_eq!(bus.check_execution_block(above.num_hash()), ExecutionCheck::NotYet);
}

/// SEC-1: a header naming block 11 with the hash of executed block 10 is not a match, with or
/// without the canonical database, although the ring holds that hash.
#[test]
fn execution_check_same_hash_at_another_number_is_not_a_match() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    let executed = sealed_at(10, 1);
    push_tip(&bus, executed.clone());
    push_tip(&bus, sealed_at(12, 2));
    let poisoned = BlockNumHash::new(11, executed.hash());

    assert_eq!(bus.check_execution_block(poisoned), ExecutionCheck::Fork, "ring only");

    let block11 = sealed_at(11, 3);
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::with(11, block11.hash())));
    assert_eq!(bus.check_execution_block(poisoned), ExecutionCheck::Fork, "with the database");
    assert_eq!(bus.check_execution_block(block11.num_hash()), ExecutionCheck::Match);
}

/// A ring miss at or below the tip with a different canonical hash in the database is a fork.
#[test]
fn execution_check_reports_fork_on_a_different_canonical_hash() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    push_tip(&bus, sealed_at(12, 2));
    let canonical = sealed_at(10, 1);
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::with(10, canonical.hash())));

    assert_eq!(bus.check_execution_block(sealed_at(10, 9).num_hash()), ExecutionCheck::Fork);
    assert_eq!(bus.check_execution_block(canonical.num_hash()), ExecutionCheck::Match);
}

/// SEC-6: below the restored-state floor the node can neither confirm nor refute a block, so the
/// check names the floor instead of a fork; at the floor the hash decides again.
#[test]
fn execution_check_below_the_restore_floor_is_not_a_fork() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    push_tip(&bus, sealed_at(120, 2));
    let at_floor = sealed_at(100, 1);
    bus.set_canonical_reader(Arc::new(
        FakeCanonicalReader::with(100, at_floor.hash()).restored_at(100),
    ));

    // a zero-hash scaffold height and a real-looking hash below the floor alike
    assert_eq!(
        bus.check_execution_block(BlockNumHash::new(50, BlockHash::ZERO)),
        ExecutionCheck::BelowFloor { floor: 100 }
    );
    assert_eq!(
        bus.check_execution_block(sealed_at(99, 7).num_hash()),
        ExecutionCheck::BelowFloor { floor: 100 }
    );
    assert_eq!(bus.check_execution_block(at_floor.num_hash()), ExecutionCheck::Match);
    assert_eq!(bus.check_execution_block(sealed_at(100, 9).num_hash()), ExecutionCheck::Fork);
}

/// SEC-8: a database that cannot answer at or below the tip (a read error, or a block not
/// persisted yet) leaves the block unconfirmed, not forked.
#[test]
fn execution_check_read_error_is_unconfirmed() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    push_tip(&bus, sealed_at(12, 2));
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::default()));

    assert_eq!(bus.check_execution_block(sealed_at(10, 1).num_hash()), ExecutionCheck::Unconfirmed);
}

/// The wait asks an unanswering database again instead of failing, and resolves once it
/// confirms the block.
#[tokio::test(start_paused = true)]
async fn wait_for_execution_retries_an_unconfirmed_block() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    push_tip(&bus, sealed_at(12, 2));
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::default()));
    let target = sealed_at(10, 1);

    let mut wait = Box::pin(bus.wait_for_execution(target.num_hash()));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), &mut wait).await.is_err(),
        "an unconfirmed block keeps the wait pending"
    );
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::with(10, target.hash())));
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), wait).await, Ok(Ok(())));
}

/// The wait returns the floor for a block below it at once.
#[tokio::test]
async fn wait_for_execution_reports_the_restore_floor() {
    let bus = ConsensusBusApp::new_with_recent_blocks(4);
    push_tip(&bus, sealed_at(120, 2));
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::default().restored_at(100)));

    assert_eq!(
        bus.wait_for_execution(BlockNumHash::new(50, BlockHash::ZERO)).await,
        Err(ExecutionCheckError::BelowFloor { floor: 100 })
    );
}

/// A database that keeps failing reads is visible: every retry counts in
/// `tn_primary.execution_check_retries_total`, and the first and every 25th retry log a warning
/// naming the block.
#[tokio::test(start_paused = true)]
async fn wait_for_execution_reports_its_retries() {
    use crate::{log_capture::LogCapture, metrics::test_recorder::TestMetricsRecorder};
    use metrics_util::debugging::DebugValue;

    let capture = LogCapture::new("primary");
    let _capture_guard = tracing::subscriber::set_default(capture.clone());
    let recorder = TestMetricsRecorder::new();
    let bus =
        metrics::with_local_recorder(&recorder, || ConsensusBusApp::new_with_recent_blocks(4));
    push_tip(&bus, sealed_at(12, 2));
    bus.set_canonical_reader(Arc::new(FakeCanonicalReader::default()));
    let target = sealed_at(10, 1);

    // retries at 0, 0.2, ... 5.2 s: 27 retries before the timeout at 5.3 s
    let wait = bus.wait_for_execution(target.num_hash());
    assert!(tokio::time::timeout(Duration::from_millis(5_300), wait).await.is_err());

    let warnings = capture.events("execution block not readable from the canonical db; retrying");
    let retries: Vec<_> = warnings.iter().filter_map(|event| event.u64_field("retries")).collect();
    assert_eq!(retries, [1, 25], "the first retry and every 25th");
    assert_eq!(warnings[0].u64_field("block_number"), Some(10));
    assert!(matches!(
        recorder.metric("tn_primary.execution_check_retries_total"),
        Some(DebugValue::Counter(27))
    ));
}
