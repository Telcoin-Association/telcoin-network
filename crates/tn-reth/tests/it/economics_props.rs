//! Property-based tests for economic invariants.
//!
//! These tests verify critical economic properties:
//! - Gas penalty is always <= unused gas
//! - Gas penalty is 0 when usage >= 10%
//! - Gas penalty increases as usage decreases (monotonicity)
//! - `gas_penalty_and_refund` conserves gas, matches the prior inline math when `auth_intrinsic` is
//!   0, never penalizes exact estimates, and never needs its unused-gas cap
//! - Padding an authorization list with junk tuples never lowers a sender's total cost inside the
//!   batch gas limit
//! - A floor-bound spend prices exactly like its no-auth twin once `effective_auth_intrinsic`
//!   clamps the authorization intrinsic, and stays penalty-free at or above 10% usage
//! - That clamp never raises the penalty on any input revm can physically produce
//! - Exact-estimate pure delegations pay zero penalty for every tuple count that fits a batch

use proptest::prelude::*;
use tn_reth::{
    calculate_gas_penalty, effective_auth_intrinsic, gas_penalty_and_refund,
    test_utils::PER_EMPTY_ACCOUNT_COST,
};

/// Minimum gas limit threshold from the implementation
const MIN_GAS_LIMIT_THRESHOLD: u64 = 210_000;
/// 10% threshold for penalty calculation
const USAGE_THRESHOLD_PCT: u64 = 10;
/// Production batch gas limit (`tn_types::max_batch_gas`) — the most gas a single batch can grant.
/// The anti-padding invariant below is exact only up to this bound: the penalty's derivative in the
/// gas limit is `1 - 120r^2 + 200r^3` for `r = real_work / gas_limit`, which approaches 1 as
/// `r -> 0`, so the real-valued margin approaches zero and the floor division in
/// `usage_ratio_scaled` decides by one gas. Sparse one-gas inversions exist at limits in the low
/// 40-millions and beyond — see `padding_is_one_gas_cheaper_above_the_batch_gas_limit`.
const MAX_BATCH_GAS: u64 = 30_000_000;
/// The base transaction intrinsic. revm charges it on top of any EIP-7702 authorization gas, so it
/// is part of the statement of every property below that models a physically reachable spend.
const BASE_TX_INTRINSIC: u64 = 21_000;

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

        // this hand-inlines `gas_penalty_and_refund`'s internals and must track them in lockstep:
        // if the helper's subtraction shape ever changes, change this expression with it. (The
        // handler's floor clamp runs BEFORE the helper call, so this mirror is unaffected by it.)
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

// Properties of the EIP-7702 authorization-intrinsic exclusion as the EVM handler composes it:
// `effective_auth_intrinsic` first clamps the intrinsic to the portion the EIP-7623 calldata floor
// did not absorb, then `gas_penalty_and_refund` strips that clamped value from both penalty
// arguments. Inputs are built by construction inside the shapes revm can actually produce — see
// each property's domain note; widening a strategy past them yields failures that are artifacts of
// unreachable arithmetic, not bugs.
proptest! {
    /// Padding the authorization list with junk tuples must never lower a sender's total cost.
    ///
    /// This is the security invariant the two-argument subtraction exists to establish. revm
    /// charges `PER_EMPTY_ACCOUNT_COST` per WIRE tuple from the count alone, before any validity
    /// check, so junk tuples raise `gas.spent()` by 25,000 each and earn no refund back: the
    /// padded sender's spent and used gas are both `real_work + A`. Stripping that intrinsic from
    /// both penalty arguments must leave the padded sender paying at least as much as the bare
    /// sender who did the same real work with no authorization list at all:
    ///
    ///   real_work + A + penalty(gas_limit - A, real_work)
    ///     >= real_work + penalty(gas_limit, real_work)
    ///
    /// Stated inside the 30M batch gas limit, the only range production can reach: the margin
    /// narrows to an integer-rounding tie as `real_work / gas_limit -> 0`, and at limits in the
    /// low 40-millions sparse shapes start tipping one gas the wrong way — see
    /// `padding_is_one_gas_cheaper_above_the_batch_gas_limit`.
    ///
    /// Confirm-by-mutation: reverting to the numerator-only subtraction still passes (it
    /// over-penalizes, never under-penalizes); removing the subtraction entirely fails
    /// immediately, because 120 tuples on a 30M limit buy the 10% threshold outright.
    ///
    /// NOTE: junk tuples only. Tuples that apply against pre-existing authorities earn a
    /// 12,500-per-tuple refund and do violate this inequality — a strictly dominated, accepted
    /// lever documented on `gas_penalty_and_refund` itself.
    #[test]
    fn prop_padding_authorizations_never_lowers_total_cost(
        // real work always carries the 21,000 base intrinsic: revm charges it inside `gas.spent()`,
        // and when the EIP-7623 floor binds the floor dominates it. The invariant is NOT true for
        // sub-base real work, so the base is part of the statement.
        real_work in BASE_TX_INTRINSIC..=200_000u64,
        // both bands matter: the margin is tightest at small N, while large N is the batch-hogging
        // attack shape that motivated the exclusion
        num_auths in prop_oneof![0..=8u64, 0..=1_200u64],
        limit_pm in 0..=1000u64
    ) {
        // clamp the tuple count so the padded spend still fits a batch, then derive the gas limit
        // inside `[padded_spent, MAX_BATCH_GAS]` — a range this construction never leaves empty
        let num_auths = num_auths.min((MAX_BATCH_GAS - real_work) / PER_EMPTY_ACCOUNT_COST);
        let auth_intrinsic = num_auths * PER_EMPTY_ACCOUNT_COST;

        // padded: junk tuples apply nothing, so spent == used == real_work + A
        let padded_spent = real_work + auth_intrinsic;
        let gas_limit = padded_spent + (MAX_BATCH_GAS - padded_spent) / 1000 * limit_pm;

        let (padded_penalty, _) =
            gas_penalty_and_refund(gas_limit, padded_spent, padded_spent, auth_intrinsic);

        // bare: the same real work with no authorization list at all
        let (bare_penalty, _) = gas_penalty_and_refund(gas_limit, real_work, real_work, 0);

        prop_assert!(
            padded_spent + padded_penalty >= real_work + bare_penalty,
            "padding {} tuples ({} gas) lowered total cost by {}: limit={} real_work={} \
             padded_penalty={} bare_penalty={}",
            num_auths, auth_intrinsic,
            (real_work + bare_penalty) - (padded_spent + padded_penalty),
            gas_limit, real_work, padded_penalty, bare_penalty
        );
    }

    /// A floor-bound spend must price exactly like its no-auth twin.
    ///
    /// When the EIP-7623 calldata floor binds, revm rewrites `gas.spent()` to the floor and zeroes
    /// the refund, so the handler sees `gas_spent == gas_used == floor_gas`. The floor is priced
    /// from calldata alone and carries no authorization term, so `effective_auth_intrinsic` clamps
    /// the whole intrinsic away and the penalty lands exactly where it would have with no
    /// authorization list at all — for every list size, including intrinsics far above the limit.
    ///
    /// The second assertion restores the issue-#424 penalty-free line for floor-bound 7702
    /// senders: reserving at most 10x the floor is at or above 10% usage, so the penalty is zero,
    /// delegating or not. Before the clamp such a sender was charged for gas it never spent — up
    /// to 29.3% of its limit at the worst shape inside a batch.
    #[test]
    fn prop_floor_bound_composition_equals_no_auth_twin(
        floor_gas in BASE_TX_INTRINSIC..=6_000_000u64,
        limit_pm in 0..=1000u64,
        auth_intrinsic in 0..=60_000_000u64
    ) {
        // the reservation sits at or above the spend by construction
        let gas_limit = floor_gas + (60_000_000 - floor_gas) / 1000 * limit_pm;
        let effective = effective_auth_intrinsic(auth_intrinsic, floor_gas, floor_gas);

        prop_assert_eq!(
            gas_penalty_and_refund(gas_limit, floor_gas, floor_gas, effective),
            gas_penalty_and_refund(gas_limit, floor_gas, floor_gas, 0),
            "floor-bound spend must price like its no-auth twin (limit={}, floor={}, auth={})",
            gas_limit, floor_gas, auth_intrinsic
        );

        if gas_limit <= 10 * floor_gas {
            prop_assert_eq!(
                gas_penalty_and_refund(gas_limit, floor_gas, floor_gas, effective).0,
                0,
                "floor-bound sender within 10x its floor must pay no penalty (limit={}, \
                 floor={}, auth={})",
                gas_limit, floor_gas, auth_intrinsic
            );
        }
    }

    /// The floor clamp must never raise the penalty on any input revm can physically produce.
    ///
    /// `effective_auth_intrinsic` only ever shrinks the intrinsic (`A_eff <= A`), and shrinking it
    /// raises the penalty basis' usage ratio while leaving the `gas_limit - gas_spent` multiplier
    /// untouched, so the clamped penalty is the smaller one. The clamp therefore cannot reopen
    /// issue #424 anywhere in the reachable domain.
    ///
    /// DOMAIN — read this before widening any strategy below. The naive domain `A <= S` and
    /// `floor <= S` is provably WRONG: at `A == S` the small-transaction exemption flips sides.
    /// Take `A = S = 3_000_000, floor = 21_000, L = 3_210_000, U = S`. Unclamped, `L - A` is
    /// exactly `MIN_GAS_LIMIT_THRESHOLD` and therefore exempt (penalty 0), while the clamped basis
    /// `L - A_eff == 231_000` re-enters penalty range and pays 1,735. Physical inputs exclude that
    /// point, but NOT via the guess "spent always covers base + intrinsic": revm's floor check
    /// fires on POST-refund gas yet rewrites PRE-refund gas via `set_spent`, so an applied-tuple
    /// refund can drag `S` below `A + 21_000` (one applied tuple behind 2,324 zero calldata bytes
    /// lands on `S == floor == 44_240 < 46_000`). What actually holds after revm's floor check is
    /// a disjunction: either `S == floor` — where the clamp zeroes the intrinsic outright and
    /// `prop_floor_bound_composition_equals_no_auth_twin` covers the claim — or
    /// `S >= A + 21_000`; `21_000 <= floor <= S` in both. This strategy samples the second
    /// disjunct by construction.
    #[test]
    fn prop_clamp_never_increases_penalty_on_physical_inputs(
        gas_spent in BASE_TX_INTRINSIC..=31_000_000u64,
        auth_pm in 0..=1000u64,
        floor_pm in 0..=1000u64,
        limit_pm in 0..=1000u64,
        used_pm in 0..=1000u64
    ) {
        // `headroom * 1000 <= gas_spent - 21_000`, so `A <= S - 21_000` and `21_000 <= floor <= S`
        let headroom = (gas_spent - BASE_TX_INTRINSIC) / 1000;
        let auth_intrinsic = headroom * auth_pm;
        let floor_gas = BASE_TX_INTRINSIC + headroom * floor_pm;
        // the reservation covers the spend, and post-refund gas never exceeds pre-refund gas
        let gas_limit = gas_spent + (60_000_000 - gas_spent) / 1000 * limit_pm;
        let gas_used = gas_spent / 1000 * used_pm;

        let clamped = effective_auth_intrinsic(auth_intrinsic, gas_spent, floor_gas);
        let (clamped_penalty, _) =
            gas_penalty_and_refund(gas_limit, gas_spent, gas_used, clamped);
        let (unclamped_penalty, _) =
            gas_penalty_and_refund(gas_limit, gas_spent, gas_used, auth_intrinsic);

        prop_assert!(
            clamped_penalty <= unclamped_penalty,
            "clamp raised the penalty from {} to {} (limit={}, spent={}, used={}, auth={}, \
             floor={}, clamped_auth={})",
            unclamped_penalty, clamped_penalty, gas_limit, gas_spent, gas_used, auth_intrinsic,
            floor_gas, clamped
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

/// Documents the boundary behavior of `prop_padding_authorizations_never_lowers_total_cost`, and
/// why that property is stated at the batch gas limit rather than the 60,000,000 the rest of this
/// file samples. The anti-padding margin is an integer-rounding equality as `real_work /
/// gas_limit` approaches 0; the floor division in `usage_ratio_scaled` decides such ties, and
/// sparse one-gas inversions exist from the low-40M limits upward (none anywhere at or below
/// `MAX_BATCH_GAS` — swept to two million cases).
///
/// A single batch can never grant more than `MAX_BATCH_GAS`, so this inversion is unreachable in
/// production. The pin exists for two reasons: so nobody "generalizes" the property to 60,000,000
/// and gets a confusing proptest failure with no explanation, and so a change to the penalty curve
/// that widens the inversion from one gas into something exploitable gets noticed here first.
#[test]
fn padding_is_one_gas_cheaper_above_the_batch_gas_limit() {
    const GAS_LIMIT: u64 = 60_000_000;
    const REAL_WORK: u64 = 21_067;
    let auth_intrinsic = PER_EMPTY_ACCOUNT_COST; // one junk tuple
    let padded_spent = REAL_WORK + auth_intrinsic;

    let (padded_penalty, _) =
        gas_penalty_and_refund(GAS_LIMIT, padded_spent, padded_spent, auth_intrinsic);
    let (bare_penalty, _) = gas_penalty_and_refund(GAS_LIMIT, REAL_WORK, REAL_WORK, 0);

    assert_eq!((padded_penalty, bare_penalty), (59_533_480, 59_558_481));
    assert_eq!(padded_spent + padded_penalty, 59_579_547, "padded total cost");
    assert_eq!(REAL_WORK + bare_penalty, 59_579_548, "bare total cost");
    // this rounding gap is unreachable while the batch gas limit stays at MAX_BATCH_GAS;
    // compile-time so raising the cap past the pinned shape cannot pass silently
    const { assert!(GAS_LIMIT > MAX_BATCH_GAS) };
}

/// The honest-sender contract, swept exhaustively rather than sampled: a pure delegation that
/// estimates its gas exactly pays zero penalty for EVERY authorization tuple count that fits a
/// batch, and keeps its full EIP-3529-capped refund.
///
/// This locks the exact-estimate-pays-zero contract against any future "claw back the applied-tuple
/// lever" formula change. Every formula in the `A - refunded` family — excusing only the unrefunded
/// portion of the intrinsic, or scaling `A` by the refund ratio — provably breaks this lock on a
/// large fraction of the domain, because the refund lands in the `gas_spent - gas_used` gap that
/// the penalty basis never sees. That impossibility is why report finding 2a was accepted and
/// pinned rather than fixed; if this sweep ever fails, the accepted tradeoff has been traded for a
/// worse one.
#[test]
fn pure_delegation_exact_estimates_pay_no_penalty_for_every_tuple_count() {
    // N = 1,199 is the last count whose exact estimate still fits the batch gas limit
    assert_eq!(BASE_TX_INTRINSIC + PER_EMPTY_ACCOUNT_COST * 1_199, 29_996_000);
    const { assert!(BASE_TX_INTRINSIC + PER_EMPTY_ACCOUNT_COST * 1_200 > MAX_BATCH_GAS) };

    for n in 1..=1_199u64 {
        let auth_intrinsic = PER_EMPTY_ACCOUNT_COST * n;
        // exact estimate: limit == spent == the intrinsic revm charges for this shape
        let s = BASE_TX_INTRINSIC + auth_intrinsic;
        // revm refunds `PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST` = 12,500 for each applied
        // tuple, and EIP-3529 caps the total refund at a fifth of the gas spent
        let refund = (12_500 * n).min(s / 5);

        assert_eq!(
            gas_penalty_and_refund(s, s, s - refund, auth_intrinsic),
            (0, refund),
            "N = {n} exact-estimate delegation must pay no penalty and keep its {refund} refund"
        );
    }
}
