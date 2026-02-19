// SPDX-License-Identifier: Apache-2.0
//! Tests for ConsensusBus helper methods.

use crate::{consensus_bus::QueChannel, ConsensusBus, NodeMode};
use tn_types::{EpochRecord, TnReceiver, TnSender};

#[tokio::test]
async fn test_is_cvv() {
    let bus = ConsensusBus::new();
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
    let bus = ConsensusBus::new();
    bus.node_mode().send_replace(NodeMode::Observer);
    assert!(!bus.is_active_cvv());

    bus.node_mode().send_replace(NodeMode::CvvInactive);
    assert!(!bus.is_active_cvv());

    bus.node_mode().send_replace(NodeMode::CvvActive);
    assert!(bus.is_active_cvv());
}

#[tokio::test]
async fn test_is_cvv_inactive() {
    let bus = ConsensusBus::new();
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
    assert_eq!(bus.committed_round(), 0);
}

#[tokio::test]
async fn test_committed_round_after_update() {
    let bus = ConsensusBus::new();
    bus.committed_round_updates().send_replace(42);
    assert_eq!(bus.committed_round(), 42);
}

#[tokio::test]
async fn test_latest_block_num_hash_default() {
    let bus = ConsensusBus::new();
    let num_hash = bus.latest_execution_block_num_hash();
    // Default is empty, so number should be 0
    assert_eq!(num_hash.number, 0);
}

#[tokio::test]
async fn test_primary_round_default() {
    let bus = ConsensusBus::new();
    assert_eq!(bus.primary_round(), 0);
}

#[tokio::test]
async fn test_primary_round_after_update() {
    let bus = ConsensusBus::new();
    bus.primary_round_updates().send_replace(100);
    assert_eq!(bus.primary_round(), 100);
}

#[tokio::test]
async fn test_recent_blocks_capacity() {
    let bus = ConsensusBus::new();
    // Default gc_depth is used for capacity
    assert!(bus.recent_blocks_capacity() > 0);
}

#[tokio::test]
async fn test_recent_blocks_capacity_custom() {
    let bus = ConsensusBus::new_with_args(100);
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
    assert!(bus.epoch_record_watch().borrow().is_none());
}

#[tokio::test]
async fn test_epoch_record_watch_send_and_receive() {
    let bus = ConsensusBus::new();
    let mut rx = bus.epoch_record_watch().subscribe();

    let epoch_rec = EpochRecord { epoch: 1, ..Default::default() };
    bus.epoch_record_watch().send_replace(Some(epoch_rec.clone()));

    rx.changed().await.unwrap();
    let received = rx.borrow_and_update().clone();
    assert!(received.is_some());
    assert_eq!(received.unwrap().epoch, 1);
}

#[tokio::test]
async fn test_epoch_record_watch_updates() {
    let bus = ConsensusBus::new();
    let mut rx = bus.epoch_record_watch().subscribe();

    // Send first epoch record
    let epoch_rec1 = EpochRecord { epoch: 1, ..Default::default() };
    bus.epoch_record_watch().send_replace(Some(epoch_rec1));
    rx.changed().await.unwrap();
    assert_eq!(rx.borrow_and_update().as_ref().unwrap().epoch, 1);

    // Send second epoch record
    let epoch_rec2 = EpochRecord { epoch: 2, ..Default::default() };
    bus.epoch_record_watch().send_replace(Some(epoch_rec2));
    rx.changed().await.unwrap();
    assert_eq!(rx.borrow_and_update().as_ref().unwrap().epoch, 2);
}
