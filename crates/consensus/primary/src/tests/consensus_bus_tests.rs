// SPDX-License-Identifier: Apache-2.0
//! Tests for ConsensusBus helper methods.

use crate::{ConsensusBus, NodeMode};

#[test]
fn test_is_cvv() {
    let bus = ConsensusBus::new();
    // Default is CvvActive, which is a CVV
    assert!(bus.is_cvv());

    bus.node_mode().send(NodeMode::CvvInactive).unwrap();
    assert!(bus.is_cvv());

    bus.node_mode().send(NodeMode::Observer).unwrap();
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
    bus.node_mode().send(NodeMode::Observer).unwrap();
    assert!(!bus.is_active_cvv());

    bus.node_mode().send(NodeMode::CvvInactive).unwrap();
    assert!(!bus.is_active_cvv());

    bus.node_mode().send(NodeMode::CvvActive).unwrap();
    assert!(bus.is_active_cvv());
}

#[tokio::test]
async fn test_is_cvv_inactive() {
    let bus = ConsensusBus::new();
    // Default is CvvActive, not inactive
    assert!(!bus.is_cvv_inactive());

    bus.node_mode().send(NodeMode::CvvInactive).unwrap();
    assert!(bus.is_cvv_inactive());

    bus.node_mode().send(NodeMode::CvvActive).unwrap();
    assert!(!bus.is_cvv_inactive());

    bus.node_mode().send(NodeMode::Observer).unwrap();
    assert!(!bus.is_cvv_inactive());
}

#[test]
fn test_committed_round_default() {
    let bus = ConsensusBus::new();
    assert_eq!(bus.committed_round(), 0);
}

#[test]
fn test_committed_round_after_update() {
    let bus = ConsensusBus::new();
    bus.committed_round_updates().send(42).unwrap();
    assert_eq!(bus.committed_round(), 42);
}

#[test]
fn test_latest_block_num_hash_default() {
    let bus = ConsensusBus::new();
    let num_hash = bus.latest_block_num_hash();
    // Default is empty, so number should be 0
    assert_eq!(num_hash.number, 0);
}
