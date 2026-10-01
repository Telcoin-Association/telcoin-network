//! Tests for inbound service classes, shed reasons, and pending occupancy.

use super::{InboundOccupancy, ServiceClass, ShedReason};

/// Occupancy with `count` pending requests of `class`.
fn occupancy_of(class: ServiceClass, count: u32) -> InboundOccupancy {
    (0..count).fold(InboundOccupancy::default(), |occupancy, _| occupancy.added(class))
}

/// The label sets are closed and exact, so series cardinality stays bounded.
#[test]
fn label_sets_are_bounded() {
    let classes: Vec<_> = ServiceClass::ALL.iter().map(|class| class.label()).collect();
    assert_eq!(classes, ["vote", "epoch_record", "certificate_sync", "batch", "gossip", "other"]);
    let reasons: Vec<_> = ShedReason::ALL.iter().map(|reason| reason.label()).collect();
    assert_eq!(reasons, ["queue_full"]);
}

/// Add and release change only the named class, by exactly one.
#[test]
fn add_and_release_are_exact_per_class() {
    ServiceClass::ALL.iter().for_each(|class| {
        let three = occupancy_of(*class, 3);
        assert_eq!(three.pending(*class), 3, "{class:?}");
        ServiceClass::ALL
            .iter()
            .filter(|other| *other != class)
            .for_each(|other| assert_eq!(three.pending(*other), 0, "{class:?} {other:?}"));
        let two = three.released(*class);
        assert_eq!(two.pending(*class), 2, "{class:?}");
        assert_eq!(two.released(*class).released(*class), InboundOccupancy::default());
    });
}
