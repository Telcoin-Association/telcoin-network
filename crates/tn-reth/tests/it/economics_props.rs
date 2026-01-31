//! Property-based tests for economic invariants.
//!
//! These tests verify critical economic properties:
//! - Gas penalty is always <= unused gas
//! - Gas penalty is 0 when usage >= 10%
//! - Gas penalty increases as usage decreases (monotonicity)

use proptest::prelude::*;
use tn_reth::calculate_gas_penalty;

/// Minimum gas limit threshold from the implementation
const MIN_GAS_LIMIT_THRESHOLD: u64 = 210_000;
/// 10% threshold for penalty calculation
const USAGE_THRESHOLD_PCT: u64 = 10;

proptest! {
    /// Gas penalty must never exceed unused gas.
    /// This ensures we can never charge more than the gas that was wasted.
    #[test]
    fn prop_penalty_never_exceeds_unused(
        gas_limit in MIN_GAS_LIMIT_THRESHOLD..60_000_000u64,
        usage_pct in 0u64..100
    ) {
        let gas_used = (gas_limit * usage_pct) / 100;
        let unused_gas = gas_limit - gas_used;

        let penalty = calculate_gas_penalty(gas_limit, gas_used);

        prop_assert!(
            penalty <= unused_gas,
            "Penalty {} must not exceed unused gas {} (limit={}, used={})",
            penalty, unused_gas, gas_limit, gas_used
        );
    }

    /// Gas penalty must be 0 when usage is >= 10%.
    #[test]
    fn prop_no_penalty_above_threshold(
        gas_limit in MIN_GAS_LIMIT_THRESHOLD..60_000_000u64,
        usage_pct in USAGE_THRESHOLD_PCT..=100u64
    ) {
        let gas_used = (gas_limit * usage_pct) / 100;

        let penalty = calculate_gas_penalty(gas_limit, gas_used);

        prop_assert_eq!(
            penalty, 0,
            "Penalty should be 0 when usage is {}% (>= 10%)",
            usage_pct
        );
    }

    /// Gas penalty must be 0 for small transactions below threshold.
    #[test]
    fn prop_no_penalty_below_threshold(
        gas_limit in 1u64..MIN_GAS_LIMIT_THRESHOLD,
        gas_used in 0u64..MIN_GAS_LIMIT_THRESHOLD
    ) {
        // Ensure gas_used <= gas_limit
        let gas_used = gas_used.min(gas_limit);

        let penalty = calculate_gas_penalty(gas_limit, gas_used);

        prop_assert_eq!(
            penalty, 0,
            "Penalty should be 0 for gas_limit {} below threshold {}",
            gas_limit, MIN_GAS_LIMIT_THRESHOLD
        );
    }

    /// Lower usage percentage should result in higher or equal penalty (monotonicity).
    /// As gas efficiency decreases, the penalty should increase.
    #[test]
    fn prop_penalty_increases_with_lower_usage(
        gas_limit in MIN_GAS_LIMIT_THRESHOLD * 2..30_000_000u64,
        usage_pct_high in 1u64..USAGE_THRESHOLD_PCT,
        usage_delta in 1u64..10
    ) {
        let usage_pct_low = usage_pct_high.saturating_sub(usage_delta).max(1);
        prop_assume!(usage_pct_low < usage_pct_high);

        let gas_used_high = (gas_limit * usage_pct_high) / 100;
        let gas_used_low = (gas_limit * usage_pct_low) / 100;

        let penalty_high = calculate_gas_penalty(gas_limit, gas_used_high);
        let penalty_low = calculate_gas_penalty(gas_limit, gas_used_low);

        prop_assert!(
            penalty_low >= penalty_high,
            "Lower usage ({}%) should have >= penalty than higher usage ({}%): {} vs {}",
            usage_pct_low, usage_pct_high, penalty_low, penalty_high
        );
    }

    /// Penalty approaches unused_gas as usage approaches 0%.
    #[test]
    fn prop_near_zero_usage_high_penalty(
        gas_limit in MIN_GAS_LIMIT_THRESHOLD * 10..30_000_000u64
    ) {
        // Very low usage: 0.1%
        let gas_used = gas_limit / 1000;
        let unused_gas = gas_limit - gas_used;

        let penalty = calculate_gas_penalty(gas_limit, gas_used);

        // Penalty should be at least 90% of unused gas for near-zero usage
        let min_expected = (unused_gas * 90) / 100;

        prop_assert!(
            penalty >= min_expected,
            "Near-zero usage should have high penalty: {} >= {} (unused={})",
            penalty, min_expected, unused_gas
        );
    }
}

/// Test boundary conditions at exactly 10% threshold.
#[test]
fn test_threshold_boundary() {
    // Exactly 10% usage - no penalty
    let penalty = calculate_gas_penalty(1_000_000, 100_000);
    assert_eq!(penalty, 0, "Exactly 10% usage should have no penalty");

    // Just below 10% (9.9%) - small penalty
    let penalty = calculate_gas_penalty(1_000_000, 99_000);
    assert!(penalty > 0, "Just below 10% should have some penalty");
    assert!(penalty < 1000, "Just below 10% should have very small penalty");

    // 9% usage
    let penalty_9pct = calculate_gas_penalty(1_000_000, 90_000);
    assert!(penalty_9pct > penalty, "9% should have higher penalty than 9.9%");
}

/// Test that quadratic scaling is working correctly.
#[test]
fn test_quadratic_scaling() {
    let gas_limit = 10_000_000;

    // 5% usage
    let penalty_5pct = calculate_gas_penalty(gas_limit, 500_000);
    // 2.5% usage (half of 5%)
    let penalty_2_5pct = calculate_gas_penalty(gas_limit, 250_000);

    // With quadratic scaling, halving usage should more than double penalty
    // (actually should be ~4x because quadratic)
    assert!(
        penalty_2_5pct > penalty_5pct * 2,
        "Quadratic scaling: 2.5% penalty {} should be > 2x of 5% penalty {}",
        penalty_2_5pct,
        penalty_5pct
    );
}
