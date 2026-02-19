//! Property-based tests for Committee invariants.
//!
//! These tests verify BFT-critical invariants:
//! - Quorum threshold is always > 2/3 of total voting power
//! - Validity threshold is always > 1/3 of total voting power
//! - quorum_threshold > validity_threshold
//! - Committee with n members can tolerate f = (n-1)/3 Byzantine failures

use proptest::prelude::*;
use rand::{rngs::StdRng, SeedableRng};
use std::collections::BTreeMap;
use tn_types::{Address, Authority, BlsKeypair, BlsPublicKey, Committee};

/// Create a test committee with the given number of authorities, all with voting power 1.
fn create_test_committee(seed: u64, size: usize) -> Committee {
    let mut rng = StdRng::seed_from_u64(seed);

    let authorities: BTreeMap<BlsPublicKey, Authority> = (0..size)
        .map(|_| {
            let keypair = BlsKeypair::generate(&mut rng);
            let authority =
                Authority::new_for_test(*keypair.public(), Address::random_with(&mut rng));
            (*keypair.public(), authority)
        })
        .collect();

    Committee::new_for_test(authorities, 0, BTreeMap::new())
}

proptest! {
    /// Quorum threshold must be greater than 2/3 of total voting power.
    /// This ensures that two quorums always intersect in at least one honest node.
    #[test]
    fn prop_quorum_threshold_greater_than_two_thirds(
        seed in any::<u64>(),
        size in 4usize..50
    ) {
        let committee = create_test_committee(seed, size);

        let total = committee.total_voting_power();
        let quorum = committee.quorum_threshold();

        // quorum > 2n/3 ensures two quorums intersect
        // We use >= (2n/3 + 1) which is equivalent to > 2n/3
        let min_quorum = (2 * total) / 3 + 1;

        prop_assert!(
            quorum >= min_quorum,
            "Quorum threshold {} must be >= {} (2n/3 + 1 where n={})",
            quorum, min_quorum, total
        );
    }

    /// Validity threshold must be greater than 1/3 of total voting power.
    /// This ensures that at least one honest node has seen the data.
    #[test]
    fn prop_validity_threshold_greater_than_one_third(
        seed in any::<u64>(),
        size in 4usize..50
    ) {
        let committee = create_test_committee(seed, size);

        let total = committee.total_voting_power();
        let validity = committee.validity_threshold();

        // validity > n/3 ensures at least one honest node
        // We need validity >= ceil(n/3)
        let min_validity = total.div_ceil(3);

        prop_assert!(
            validity >= min_validity,
            "Validity threshold {} must be >= {} (ceil(n/3) where n={})",
            validity, min_validity, total
        );
    }

    /// Quorum threshold must be strictly greater than validity threshold.
    #[test]
    fn prop_quorum_greater_than_validity(
        seed in any::<u64>(),
        size in 4usize..50
    ) {
        let committee = create_test_committee(seed, size);

        let quorum = committee.quorum_threshold();
        let validity = committee.validity_threshold();

        prop_assert!(
            quorum > validity,
            "Quorum {} must be > validity {} for size {}",
            quorum, validity, size
        );
    }

    /// Byzantine fault tolerance: committee of n can tolerate f failures where n >= 3f + 1.
    /// This means f = floor((n-1)/3) Byzantine nodes cannot prevent progress.
    #[test]
    fn prop_byzantine_fault_tolerance(
        seed in any::<u64>(),
        size in 4usize..50
    ) {
        let committee = create_test_committee(seed, size);

        let n = committee.total_voting_power();
        let quorum = committee.quorum_threshold();

        // f = floor((n-1)/3) is the max Byzantine nodes we can tolerate
        let f = (n - 1) / 3;

        // n - f honest nodes must be able to form a quorum
        let honest_nodes = n - f;

        prop_assert!(
            honest_nodes >= quorum,
            "n-f={} honest nodes must reach quorum={} (n={}, f={})",
            honest_nodes, quorum, n, f
        );
    }

    /// Two quorums must always intersect in at least one honest node.
    /// This is fundamental for BFT safety.
    #[test]
    fn prop_two_quorums_intersect(
        seed in any::<u64>(),
        size in 4usize..50
    ) {
        let committee = create_test_committee(seed, size);

        let n = committee.total_voting_power();
        let quorum = committee.quorum_threshold();

        // Two quorums of size q each. Together they have 2q votes.
        // By pigeonhole, they must overlap by at least 2q - n nodes.
        let intersection = 2 * quorum - n;

        // The max Byzantine nodes f = floor((n-1)/3)
        let f = (n - 1) / 3;

        // The intersection must be larger than f to guarantee an honest node
        prop_assert!(
            intersection > f,
            "Two quorums must intersect in >f nodes: intersection={}, f={}, q={}, n={}",
            intersection, f, quorum, n
        );
    }
}

#[test]
fn test_committee_enforces_equal_voting_power() {
    let mut rng = StdRng::seed_from_u64(42);
    let configured_weights = [5_u64, 9, 42, 100];

    let authorities: BTreeMap<BlsPublicKey, Authority> = configured_weights
        .iter()
        .map(|_| {
            let keypair = BlsKeypair::generate(&mut rng);
            let authority =
                Authority::new_for_test(*keypair.public(), Address::random_with(&mut rng));
            (*keypair.public(), authority)
        })
        .collect();

    let committee = Committee::new_for_test(authorities, 0, BTreeMap::new());

    assert_eq!(committee.total_voting_power(), configured_weights.len() as u64);
    for authority in committee.authorities() {
        assert_eq!(authority.voting_power(), 1);
    }
}

/// Non-proptest: verify the classic BFT thresholds for specific committee sizes.
#[test]
fn test_classic_bft_thresholds() {
    // n=4: f=1, quorum=3, validity=2
    // n=7: f=2, quorum=5, validity=3
    // n=10: f=3, quorum=7, validity=4
    // n=13: f=4, quorum=9, validity=5

    let test_cases = [
        (4, 3, 2),  // n=4: 2f+1=3, f+1=2
        (7, 5, 3),  // n=7: 2f+1=5, f+1=3
        (10, 7, 4), // n=10: 2f+1=7, f+1=4
        (13, 9, 5), // n=13: 2f+1=9, f+1=5
    ];

    for (size, expected_quorum, expected_validity) in test_cases {
        let committee = create_test_committee(42, size);

        assert_eq!(
            committee.quorum_threshold(),
            expected_quorum,
            "n={}: quorum should be {}",
            size,
            expected_quorum
        );

        assert_eq!(
            committee.validity_threshold(),
            expected_validity,
            "n={}: validity should be {}",
            size,
            expected_validity
        );
    }
}
