//! Property-based tests for economic invariants.
//!
//! These tests verify critical economic properties:
//! - Gas penalty is always <= unused gas
//! - Gas penalty is 0 when usage >= 10%
//! - Gas penalty increases as usage decreases (monotonicity)
//! - `gas_penalty_and_refund` conserves gas, matches the prior inline math when `auth_intrinsic` is
//!   0, never penalizes exact estimates, and never needs its unused-gas cap

use proptest::prelude::*;
use tn_reth::{calculate_gas_penalty, gas_penalty_and_refund};

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
        usage_pct_high in 2u64..USAGE_THRESHOLD_PCT,
        usage_delta in 1u64..10
    ) {
        // usage_pct_high >= 2 guarantees usage_pct_low < usage_pct_high:
        // - if delta < high: low = high - delta >= 1, and low <= high - 1 < high
        // - if delta >= high: saturating_sub yields 0, max(1) = 1 < high (since high >= 2)
        let usage_pct_low = usage_pct_high.saturating_sub(usage_delta).max(1);

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

// Properties of `gas_penalty_and_refund`, the EIP-7702-aware wrapper the EVM handler applies.
// Ordered gas values are derived by construction (no `prop_assume`): `gas_limit / 1000 *
// spent_pm <= gas_limit` for any per-mille `spent_pm <= 1000`, and likewise for `gas_used`
// from `gas_spent`.
proptest! {
    /// The penalty and the caller refund must partition unused gas exactly,
    /// and the penalty alone must never exceed it — for any authorization
    /// intrinsic, so the split can never mint or destroy gas value.
    #[test]
    fn prop_penalty_refund_conserve_gas(
        gas_limit in 0..=60_000_000u64,
        spent_pm in 0..=1000u64,
        used_pm in 0..=1000u64,
        auth_intrinsic in 0..=6_000_000u64
    ) {
        let gas_spent = gas_limit / 1000 * spent_pm;
        let gas_used = gas_spent / 1000 * used_pm;

        let (penalty, refund) =
            gas_penalty_and_refund(gas_limit, gas_spent, gas_used, auth_intrinsic);

        prop_assert_eq!(
            penalty + refund,
            gas_limit - gas_used,
            "penalty {} + refund {} must equal unused gas (limit={}, used={}, auth={})",
            penalty, refund, gas_limit, gas_used, auth_intrinsic
        );
        prop_assert!(
            penalty <= gas_limit - gas_used,
            "penalty {} must not exceed unused gas {} (limit={}, used={}, auth={})",
            penalty, gas_limit - gas_used, gas_limit, gas_used, auth_intrinsic
        );
    }

    /// With `auth_intrinsic == 0` (every non-7702 transaction) the wrapper must
    /// reproduce the prior inline math byte-for-byte: `calculate_gas_penalty`
    /// capped at unused gas, with the refund as the remainder.
    #[test]
    fn prop_zero_auth_matches_prior_inline_math(
        gas_limit in 0..=60_000_000u64,
        spent_pm in 0..=1000u64,
        used_pm in 0..=1000u64
    ) {
        let gas_spent = gas_limit / 1000 * spent_pm;
        let gas_used = gas_spent / 1000 * used_pm;

        // the prior inline math, computed with public APIs
        let unused = gas_limit - gas_used;
        let p = calculate_gas_penalty(gas_limit, gas_spent).min(unused);

        prop_assert_eq!(
            gas_penalty_and_refund(gas_limit, gas_spent, gas_used, 0),
            (p, unused - p),
            "zero auth intrinsic must match the prior math (limit={}, spent={}, used={})",
            gas_limit, gas_spent, gas_used
        );
    }

    /// A transaction that spends exactly its limit pays no penalty for ANY
    /// authorization intrinsic — including intrinsics above the gas limit,
    /// which exercise the saturating subtraction in both arguments.
    #[test]
    fn prop_exact_estimate_pays_no_penalty(
        gas_limit in 0..=60_000_000u64,
        used_pm in 0..=1000u64,
        auth_intrinsic in 0..=60_000_000u64
    ) {
        let gas_spent = gas_limit;
        let gas_used = gas_spent / 1000 * used_pm;

        prop_assert_eq!(
            gas_penalty_and_refund(gas_limit, gas_spent, gas_used, auth_intrinsic),
            (0, gas_limit - gas_used),
            "exact estimate must pay no penalty (limit={}, used={}, auth={})",
            gas_limit, gas_used, auth_intrinsic
        );
    }

    /// The wrapper's `.min(unused_gas)` cap is structurally non-binding: for
    /// every `gas_used <= gas_spent <= gas_limit` the UNCAPPED two-argument
    /// penalty is already bounded by unused gas, so the cap is pure
    /// defense-in-depth.
    #[test]
    fn prop_cap_is_structurally_non_binding(
        gas_limit in 0..=60_000_000u64,
        spent_pm in 0..=1000u64,
        used_pm in 0..=1000u64,
        auth_intrinsic in 0..=60_000_000u64
    ) {
        let gas_spent = gas_limit / 1000 * spent_pm;
        let gas_used = gas_spent / 1000 * used_pm;

        let uncapped = calculate_gas_penalty(
            gas_limit.saturating_sub(auth_intrinsic),
            gas_spent.saturating_sub(auth_intrinsic),
        );

        prop_assert!(
            uncapped <= gas_limit - gas_used,
            "uncapped penalty {} must stay within unused gas {} (limit={}, spent={}, used={}, auth={})",
            uncapped, gas_limit - gas_used, gas_limit, gas_spent, gas_used, auth_intrinsic
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
