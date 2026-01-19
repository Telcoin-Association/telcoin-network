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
