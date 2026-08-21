//! Utility functions used by EVM.
//!
//! Home of [`calculate_gas_penalty`], the quadratic penalty `TNEvmHandler::reimburse_caller`
//! applies to transactions that set a gas limit far above actual usage (issue #424). The
//! penalty is zero when the gas limit is at or below 210,000 or when at least 10% of the limit
//! was used; below 10% usage it grows quadratically with the shortfall, approaching
//! confiscation of all unused gas for extreme over-estimates. Integer-only `u128` arithmetic
//! (10^9 fixed-point precision) keeps results identical on every node — a consensus
//! requirement, since the penalty changes account balances.
//!
//! [`gas_penalty_and_refund`] is the EIP-7702-aware wrapper the handler actually calls: it
//! excludes the authorization-tuple intrinsic from the penalty basis — so padded authorization
//! lists cannot buy their way past the threshold and honest delegations are not over-charged —
//! and splits a transaction's unused gas into the penalty and the caller refund.
//! [`effective_auth_intrinsic`] is the clamp the handler applies to that intrinsic before the
//! call, so a transaction whose spend was rewritten to the EIP-7623 calldata floor is never
//! excused authorization gas the floor already covered.

use tracing::debug;

/// Minimum gas limit threshold (10x minimum transaction cost)
const MIN_GAS_LIMIT_THRESHOLD: u64 = 210_000;
/// Precision for calculating usage ratio with 10^18.
// 10^9 precision (1 billion) - sufficient for 0.001% granularity
const PRECISION: u128 = 10_u128.pow(9);
/// Usage ratio threshold below which penalties apply (used to calc 10%)
// 10% threshold = 10^8 with 10^9 precision
const THRESHOLD: u128 = 10_u128.pow(8);
/// THRESHOLD squared
// 10^16 for the denominator
const THRESHOLD_SQUARED: u128 = 10_u128.pow(16);

/// Calculate the gas penalty for inefficient gas limit estimation.
///
/// Uses u128 arithmetic with 10^9 decimal precision to ensure deterministic results
/// across all platforms. This is critical for blockchain consensus.
///
/// Returns the amount of gas that should be charged as a penalty.
/// The penalty scales quadratically based on how much below 10% usage the transaction is.
///
/// # Formula (using u128 math with precision factor 10^9)
/// - If usage_ratio >= 10%: penalty = 0
/// - If usage_ratio < 10%: penalty = ((10^8 - usage_ratio_scaled)^2 * unused_gas) / 10^16
///
/// # Precision Details
/// - 10^9 precision provides granularity down to 0.001% usage ratios
/// - Safe from overflow for gas limits up to 340M (currently 60M max)
/// - Deterministic integer arithmetic ensures consensus across all nodes
///
/// # Examples
///
/// | Gas Limit  | Gas Used | Usage % | Unused Gas  | Penalty Gas | Penalty % of Unused |
/// |------------|----------|---------|-------------|-------------|-------------------|
/// | 21,000     | 21,000   | 100%    | 0           | 0           | 0%                |
/// | 210,000    | 21,000   | 10%     | 189,000     | 0           | 0%                |
/// | 420,000    | 21,000   | 5%      | 399,000     | 99,750      | 25%               |
/// | 1,000,000  | 21,000   | 2.1%    | 979,000     | 610,993     | 62.4%             |
/// | 5,000,000  | 21,000   | 0.42%   | 4,979,000   | 4,569,546   | 91.8%             |
/// | 10,000,000 | 21,000   | 0.21%   | 9,979,000   | 9,564,282   | 95.8%             |
/// | 30,000,000 | 21,000   | 0.07%   | 29,979,000  | 29,560,762  | 98.6%             |
pub fn calculate_gas_penalty(gas_limit: u64, gas_used: u64) -> u64 {
    // skip penalty for small transactions
    if gas_limit <= MIN_GAS_LIMIT_THRESHOLD {
        return 0;
    }

    // cast up to u128
    let gas_limit_u128 = gas_limit as u128;
    let gas_used_u128 = gas_used as u128;

    // calculate usage ratio with 10^9 precision
    let usage_ratio_scaled = PRECISION * gas_used_u128 / gas_limit_u128;

    // no penalty if usage is above threshold
    if usage_ratio_scaled >= THRESHOLD {
        debug!(target: "engine", ?gas_limit, ?gas_used, ?usage_ratio_scaled, threshold=?THRESHOLD, "usage within acceptable range");
        return 0;
    }

    // calculate inefficiency (how far below 10% we are)
    // saturating_sub: the threshold guard above guarantees gas_used < gas_limit here, but this
    // consensus path must not depend on that non-local invariant to avoid underflow
    let unused_gas = gas_limit_u128.saturating_sub(gas_used_u128);
    let inefficiency_scaled = THRESHOLD - usage_ratio_scaled;
    // square values then calculate penalty
    let inefficiency_squared = inefficiency_scaled.pow(2);

    // this is safe: max value is ~6×10^23, well below u128 max
    let penalty = (inefficiency_squared * unused_gas) / THRESHOLD_SQUARED;
    debug!(
        target: "engine",
        ?gas_limit,
        ?gas_used,
        ?penalty,
        ?unused_gas,
        final=penalty.min(unused_gas) as u64,
        "assessing penalty"
    );

    // unused_gas is the fallback if penalty overflows (u128 cast from u64)
    penalty.try_into().unwrap_or(unused_gas as u64)
}

/// Split a transaction's unused gas into the penalty credited to the basefee
/// address and the refund returned to the caller, excluding EIP-7702
/// authorization intrinsic gas from the penalty basis.
///
/// # Parameters
///
/// - `gas_limit`: the transaction's gas limit.
/// - `gas_spent`: pre-refund gas, revm's `gas.spent()`.
/// - `gas_used`: post-refund gas, revm's `gas.spent_sub_refunded()` — what the header records and
///   the sender pays for.
/// - `auth_intrinsic`: the authorization gas to exclude from the basis, supplied by the caller. The
///   handler passes the wire authorization-tuple count multiplied by the spec's per-empty-account
///   cost from revm's cfg gas params — 0 before Prague, exactly what revm charged at Prague and
///   later — after clamping it with [`effective_auth_intrinsic`] to the portion the EIP-7623
///   calldata floor did not absorb.
///
/// # Why the intrinsic comes off BOTH arguments
///
/// The penalty asks "did you use enough of the gas you reserved?" and reads the
/// answer off `gas.spent()`. EIP-7702 charges a flat per-empty-account intrinsic
/// (25,000 gas at Prague) per authorization tuple from the tuple count alone,
/// before any validity check, so a sender can pad the authorization list with
/// junk tuples to inflate `gas.spent()` past the 10% threshold and collapse the
/// quadratic penalty on a batch-hogging gas limit — buying `gas.spent()` without
/// doing any work. Subtracting the intrinsic removes that lever — but subtracting
/// it from only the spent basis moved the penalty-free line to `gas_spent >=
/// 0.10 * gas_limit + 25,000 * N`, silently confiscating the unrefunded
/// 12,500-per-tuple portion of honest delegation work (revm refunds only
/// `PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST` = 12,500 per applied tuple, only
/// for authorities whose accounts already exist, and EIP-3529 caps the total
/// refund at a fifth of spent gas). Subtracting from both arguments prices the
/// penalty as if the authorization block did not exist: an honest exact-estimate
/// delegation pays zero, while the unused-gas multiplier is unchanged —
/// `(gas_limit - A) - (gas_spent - A) = gas_limit - gas_spent` — so the padded
/// attacker (N = 120, 30M limit, 3,021,000 spent) still pays 26,560,959 of the
/// previous 26,979,000, keeping 98.45% of the penalty.
///
/// That 98.45% is a guarantee about JUNK tuples, which earn no refund. Tuples that apply against
/// pre-existing authorities do earn one, and because the refund lands entirely in the
/// `gas_spent - gas_used` gap — which this function never reduces — each such tuple buys 25,000
/// of penalty-basis headroom for a net 12,500, or 20,000 once the EIP-3529 cap binds. In the
/// worst quadrant (N = 1180 applied tuples against a 30M limit) only 161,135 of the 29,560,762
/// an empty reservation would pay survives: 0.545%, pinned by
/// `applied_tuple_over_reservation_is_an_accepted_lever`. That residual is accepted, for three
/// reasons:
///
/// 1. that sender is above the 10% threshold on both raw axes — 98.4% of the limit spent
///    pre-refund, 79% post-refund — so with no 7702 exclusion at all the penalty would be zero. The
///    161,135 it does pay is conservatism relative to the plain threshold rule, not revenue lost to
///    a bypass.
/// 2. the lever is strictly dominated: reserving a batch this way costs 23,777,935 gas
///    out-of-pocket, roughly 8x the ~3M a plain calldata-floor reservation costs on `main` to any
///    sender, delegating or not. It weakens the issue-#424 mitigation in this one quadrant without
///    unlocking a capability.
/// 3. every claw-back formula in this family — excusing only `A - refunded`, or scaling `A` by the
///    refund ratio — provably charges honest exact-estimate delegators, breaking the
///    exact-estimate-pays-zero contract the two-argument subtraction exists to hold. No formula
///    change closes the quadrant without reopening the over-charge, so the formula stays.
///
/// Only the authorization intrinsic is excluded: calldata fairly prices batch
/// bytes (16 gas per nonzero byte / 4 per zero byte under execution pricing, and
/// 40 per nonzero byte / 10 per zero byte when the EIP-7623 floor binds) and the
/// base 21,000 is negligible, so legitimate low-execution transfers keep the
/// pre-fix basis.
///
/// # Consequences of the two-argument subtraction
///
/// - The small-transaction exemption widens from `gas_limit <= 210,000` to `gas_limit <= 210,000 +
///   auth_intrinsic` — correct, because the authorization block is prepaid at full price.
/// - A halted transaction consumes its entire limit, so `gas_spent - A` against `gas_limit - A` is
///   a 100% usage ratio and the penalty is zero even with a large authorization list.
/// - EIP-7623 floor residual — eliminated by the handler, not by this function: when the calldata
///   floor binds, revm's spent value IS the floor, which carries no authorization component, so
///   subtracting the full `A` from it manufactures a penalty out of gas the sender never paid. That
///   residual is not "tiny": at the worst shape inside the 30M batch limit — 71 tuples behind
///   295,834 zero calldata bytes, floor 2,979,340, limit 29,793,400 — it confiscates 8,716,811 gas,
///   29.3% of the limit, from a transaction sitting exactly on this rule's own penalty-free line,
///   and roughly 18.6M once applied-tuple refunds let the floor bind under a larger reservation.
///   `reimburse_caller` therefore clamps its `auth_intrinsic` argument with
///   [`effective_auth_intrinsic`] before calling this function, so only the portion the floor left
///   unabsorbed reaches the basis and the residual is zero (pinned by
///   `floor_bound_worst_case_pays_no_penalty` and
///   `eip7623_floor_bound_intrinsic_is_fully_absorbed`). Note that subtracting from the numerator
///   alone is *worse* in this quadrant rather than zero — the same reduced numerator over the full
///   limit; the zero belongs to the pre-7702 formula, which subtracted nothing.
///
/// # Why cap at unused gas
///
/// The `.min(unused_gas)` cap is now structurally non-binding for every `gas_used <= gas_spent`
/// input — the two-argument penalty is bounded by `(gas_limit - A) - (gas_spent - A) =
/// gas_limit - gas_spent <= gas_limit - gas_used = unused_gas` — but it is kept as
/// defense-in-depth: `reimburse_caller` credits the full penalty to the basefee address, so an
/// uncapped penalty above unused gas would pay out more than the sender prepaid, minting value
/// in consensus-critical code. The cap guarantees exact conservation (`refund + penalty +
/// gas_used == gas_limit`).
///
/// For `auth_intrinsic == 0` (every non-7702 transaction) this reproduces the prior inline
/// math byte-for-byte.
// `pub` for the test-gated re-exports in `evm` and the crate root; in builds where those
// re-exports are compiled out (no `test-utils`, not `cfg(test)`) the item is crate-internal,
// so silence `unreachable_pub` there instead of splitting the signature per cfg.
#[cfg_attr(not(any(test, feature = "test-utils")), allow(unreachable_pub))]
pub fn gas_penalty_and_refund(
    gas_limit: u64,
    gas_spent: u64,
    gas_used: u64,
    auth_intrinsic: u64,
) -> (u64, u64) {
    let unused_gas = gas_limit.saturating_sub(gas_used);
    let penalty = calculate_gas_penalty(
        gas_limit.saturating_sub(auth_intrinsic),
        gas_spent.saturating_sub(auth_intrinsic),
    )
    .min(unused_gas);
    (penalty, unused_gas.saturating_sub(penalty))
}

/// Clamp an EIP-7702 authorization intrinsic to the portion the EIP-7623 calldata floor did not
/// absorb, so only gas the sender actually paid on top of the floor is excused from the penalty
/// basis.
///
/// The floor is priced from calldata alone — `10 * tokens_in_calldata + 21,000`, with no
/// authorization term — so when revm rewrites `gas.spent()` to it the authorization block rides
/// along for free. Only `gas_spent - floor_gas` of the intrinsic was charged on top of the floor,
/// and only that portion may come off the penalty arguments.
///
/// # Why this needs no floor-bound flag
///
/// For `floor_gas <= gas_spent` — guaranteed once revm's floor check has run, since that check
/// either leaves `gas_spent` alone (already at or above the floor) or sets it to the floor
/// exactly — the clamp satisfies
///
/// ```text
/// gas_spent - effective_auth_intrinsic(A, gas_spent, floor_gas) == max(gas_spent - A, floor_gas)
/// ```
///
/// in BOTH branches of that check. The penalty numerator therefore becomes exactly what revm
/// would have charged this same transaction with its authorization list deleted, which is the
/// whole point of the exclusion. That matters because `set_spent` destroys the "did the floor
/// bind?" answer before `reimburse_caller` runs, so a boolean test is not available — and it
/// would introduce a cliff at the binding point, where this correction is continuous in
/// `floor_gas`.
///
/// # Why this cannot reopen issue #424
///
/// The clamp only ever weakens a penalty (`A_eff <= A`). The padding attacker pays for junk
/// tuples on empty calldata, so the floor is the bare 21,000 base, far below `gas_spent`, and
/// `A_eff == A`: the padded penalty is byte-identical to what it was before the clamp existed.
// `pub` for the test-gated re-exports in `evm` and the crate root; in builds where those
// re-exports are compiled out (no `test-utils`, not `cfg(test)`) the item is crate-internal,
// so silence `unreachable_pub` there instead of splitting the signature per cfg.
#[cfg_attr(not(any(test, feature = "test-utils")), allow(unreachable_pub))]
pub fn effective_auth_intrinsic(auth_intrinsic: u64, gas_spent: u64, floor_gas: u64) -> u64 {
    auth_intrinsic.min(gas_spent.saturating_sub(floor_gas))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_revm::{
        context::CfgEnv,
        context_interface::cfg::gas::get_tokens_in_calldata,
        primitives::{
            eip7702::{PER_AUTH_BASE_COST, PER_EMPTY_ACCOUNT_COST},
            hardfork::SpecId,
        },
    };
    use tracing::debug;

    #[test]
    fn test_gas_penalty_calculation() {
        // test cases showing quadratic scaling
        struct TestCase {
            gas_limit: u64,
            gas_used: u64,
            expected_penalty: u64,
            description: &'static str,
        }

        let test_cases = vec![
            TestCase {
                gas_limit: 21_000,
                gas_used: 21_000,
                expected_penalty: 0,
                description: "100% usage, below threshold - no penalty",
            },
            TestCase {
                gas_limit: 210_000,
                gas_used: 21_000,
                expected_penalty: 0,
                description: "10% usage, at threshold - no penalty",
            },
            TestCase {
                gas_limit: 420_000,
                gas_used: 21_000,
                expected_penalty: 99_750,
                description: "5% usage - 25% penalty on unused gas",
            },
            TestCase {
                gas_limit: 1_000_000,
                gas_used: 21_000,
                expected_penalty: 610_993,
                description: "2.1% usage - 62.4% penalty on unused gas",
            },
            TestCase {
                gas_limit: 5_000_000,
                gas_used: 21_000,
                expected_penalty: 4_569_546,
                description: "0.42% usage - 91.8% penalty on unused gas",
            },
            TestCase {
                gas_limit: 10_000_000,
                gas_used: 21_000,
                expected_penalty: 9_564_282,
                description: "0.21% usage - 95.6% penalty on unused gas",
            },
            TestCase {
                gas_limit: 30_000_000,
                gas_used: 21_000,
                expected_penalty: 29_560_762,
                description: "0.07% usage - 98.6% penalty on unused gas",
            },
            TestCase {
                gas_limit: 60_000_000,
                gas_used: 21_000,
                expected_penalty: 59_559_881,
                description: "0.07% usage - 98.6% penalty on unused gas",
            },
        ];

        for tc in test_cases {
            let penalty = calculate_gas_penalty(tc.gas_limit, tc.gas_used);
            let unused = tc.gas_limit - tc.gas_used;
            let penalty_pct =
                if unused > 0 { (penalty as f64 / unused as f64) * 100.0 } else { 0.0 };

            debug!(
                target: "engine",
                "{}: gas_limit={}, gas_used={}, penalty={} ({:.2}% of unused)",
                tc.description, tc.gas_limit, tc.gas_used, penalty, penalty_pct
            );

            assert_eq!(
                penalty, tc.expected_penalty,
                "Expected penalty {}, got {} for {}",
                tc.expected_penalty, penalty, tc.description
            );
        }
    }

    #[test]
    fn test_edge_cases() {
        // test at exactly 10% usage
        let penalty = calculate_gas_penalty(1_000_000, 100_000);
        assert_eq!(penalty, 0, "Should have no penalty at exactly 10% usage");

        // test slightly below 10% usage (9.9%)
        let penalty = calculate_gas_penalty(1_000_000, 99_000);
        assert_eq!(penalty, 90, "Should have small penalty at 9.9% usage");

        // test with gas limit at threshold
        let penalty = calculate_gas_penalty(210_000, 10_000);
        assert_eq!(penalty, 0, "Should have no penalty at minimum threshold");

        // test with gas limit just below threshold
        let penalty = calculate_gas_penalty(209_999, 10_000);
        assert_eq!(penalty, 0, "Should have no penalty below minimum threshold");

        // test with gas limit just above threshold
        let penalty = calculate_gas_penalty(210_001, 10_000);
        assert_eq!(penalty, 54_876, "Should have penalty above minimum threshold");
    }

    /// Pin the usage-ratio guard for overspent inputs.
    ///
    /// Any `gas_used >= gas_limit` puts the usage ratio at or above the 10% threshold, so the
    /// guard returns zero before the subtraction at the top of the penalty math. This test
    /// holds that door shut: a change that weakens the guard turns an overspent input into an
    /// underflow at `inefficiency_scaled` in a consensus path.
    #[test]
    fn test_gas_used_above_limit_no_penalty() {
        // gas used just above the limit
        assert_eq!(calculate_gas_penalty(300_000, 300_001), 0, "overspent by one unit");

        // gas used at exactly the limit (100% usage)
        assert_eq!(calculate_gas_penalty(300_000, 300_000), 0, "full usage");

        // gas used at double the limit
        assert_eq!(calculate_gas_penalty(1_000_000, 2_000_000), 0, "overspent 2x");

        // extreme overspend does not underflow or wrap
        assert_eq!(
            calculate_gas_penalty(MIN_GAS_LIMIT_THRESHOLD + 1, u64::MAX),
            0,
            "extreme overspend"
        );
    }

    /// A padded EIP-7702 authorization list pays a near-maximal penalty.
    ///
    /// 120 junk tuples inflate `gas_spent` to 3,021,000 (21,000 base + 120 ×
    /// 25,000 intrinsic) — 10.07% of a 30M limit, just over the 10% penalty-free
    /// threshold. `gas_penalty_and_refund` strips the 3,000,000 authorization
    /// intrinsic from both arguments, so the penalty is priced as 21,000 of real
    /// work against a 27M reservation — exactly the no-7702 penalty for the same
    /// non-auth work: 26,560,959 of the 26,979,000 unused gas, keeping 98.45% of
    /// the pre-fix 26,979,000 capped penalty. The batch-hogging reservation still
    /// doesn't pay.
    ///
    /// Confirm-by-mutation: reverting to the numerator-only subtraction yields a
    /// capped 26,979,000, and removing the subtraction entirely yields 0 — both
    /// break the pinned pair below.
    #[test]
    fn padded_7702_authorization_list_pays_a_near_maximal_penalty() {
        let gas_limit = 30_000_000;
        let gas_spent = 3_021_000; // 21,000 + 120 * 25,000
        let gas_used = gas_spent; // mismatched-chain tuples apply nothing, so no refund
        let (penalty, refund) = gas_penalty_and_refund(gas_limit, gas_spent, gas_used, 3_000_000);

        assert_eq!((penalty, refund), (26_560_959, 418_041), "near-maximal penalty");

        // the two-argument basis is exactly the no-7702 penalty for the same non-auth work
        assert_eq!(penalty, calculate_gas_penalty(27_000_000, 21_000));

        // the cap is non-binding: the uncapped two-argument penalty is below unused gas
        let uncapped = calculate_gas_penalty(27_000_000, 21_000);
        assert!(uncapped < gas_limit - gas_used, "uncapped penalty stays below unused gas");

        assert_eq!(penalty + refund, 26_979_000, "penalty + refund is all unused gas");
        assert_eq!(penalty + refund, gas_limit - gas_used, "gas is conserved");
    }

    /// The same 3,021,000 spend with no authorization tuples earns no penalty:
    /// 10.07% usage is over the 10% threshold, so a transaction doing real
    /// 3M-gas work is correctly unpenalized. This is exactly the penalty-free
    /// state the padded transaction above bought without doing the work — the
    /// gap the authorization-intrinsic exclusion closes.
    #[test]
    fn identical_spend_without_padding_is_penalty_free() {
        let gas_limit = 30_000_000;
        let gas_spent = 3_021_000;
        let gas_used = gas_spent;
        let (penalty, refund) = gas_penalty_and_refund(gas_limit, gas_spent, gas_used, 0);

        assert_eq!(penalty, 0, "3.02M of real work is over the 10% threshold");
        assert_eq!(refund, 26_979_000, "all unused gas is refunded");
        assert_eq!(penalty + refund + gas_used, gas_limit, "gas is conserved");
    }

    /// For every non-7702 transaction (`auth_intrinsic == 0`) the helper is
    /// identical to the prior inline math — `calculate_gas_penalty(gas_limit,
    /// gas_spent)` with the refund as the remainder — and the cap never binds.
    /// Pins the "zero drift for existing traffic" guarantee across a spread of
    /// gas profiles, including a below-threshold case with a real nonzero penalty
    /// and a case where an SSTORE refund makes `gas_used < gas_spent`.
    #[test]
    fn zero_authorizations_matches_prior_inline_math() {
        // (gas_limit, gas_spent, gas_used); gas_spent >= gas_used always holds
        let cases = [
            (30_000_000u64, 21_000u64, 21_000u64), // extreme over-reserve, near-maximal penalty
            (1_000_000, 99_000, 99_000),           // 9.9% usage, small penalty
            (1_000_000, 500_000, 495_200),         // >10% usage, no penalty, gas_used < gas_spent
            (210_000, 21_000, 21_000),             // at threshold, no penalty
            (100_000, 21_000, 21_000),             // below MIN_GAS_LIMIT_THRESHOLD, no penalty
        ];
        for (gas_limit, gas_spent, gas_used) in cases {
            let (penalty, refund) = gas_penalty_and_refund(gas_limit, gas_spent, gas_used, 0);
            let expected_penalty = calculate_gas_penalty(gas_limit, gas_spent);
            let unused = gas_limit - gas_used;

            assert_eq!(
                penalty, expected_penalty,
                "penalty matches prior math for {gas_limit}/{gas_spent}"
            );
            assert_eq!(refund, unused - expected_penalty, "refund is the remainder");
            assert!(penalty <= unused, "cap never binds for non-7702");
            assert_eq!(penalty + refund + gas_used, gas_limit, "gas is conserved");
        }
    }

    /// An honest N=20 delegation at an exact gas estimate pays no penalty.
    ///
    /// `gas_limit = gas_spent = 521,000` (21,000 base + 20 × 25,000 intrinsic)
    /// with all 20 tuples applied: revm's 12,500 × 20 = 250,000 refund is
    /// EIP-3529-capped at `gas_spent / 5 = 104,200`, hence `gas_used = 416,800`.
    /// Under the numerator-only subtraction this transaction paid a penalty
    /// (21,000 of 521,000 is 4% usage); with the intrinsic off both arguments
    /// the basis is 21,000 of 21,000 — zero penalty, full refund of unused gas.
    #[test]
    fn honest_exact_estimate_with_capped_refund_pays_no_penalty() {
        let (penalty, refund) = gas_penalty_and_refund(521_000, 521_000, 416_800, 500_000);
        assert_eq!((penalty, refund), (0, 104_200), "exact estimate pays no penalty");
        assert_eq!(penalty + refund, 521_000 - 416_800, "gas is conserved");
    }

    /// An honest N=5 delegation at an exact gas estimate pays no penalty — the
    /// unit mirror of the engine e2e's applied-delegation case.
    ///
    /// `gas_limit = gas_spent = 146,000` (21,000 base + 5 × 25,000 intrinsic);
    /// the 12,500 × 5 = 62,500 refund is EIP-3529-capped at `146,000 / 5 =
    /// 29,200`, hence `gas_used = 116,800`.
    #[test]
    fn honest_small_delegation_exact_estimate_pays_no_penalty() {
        let (penalty, refund) = gas_penalty_and_refund(146_000, 146_000, 116_800, 125_000);
        assert_eq!((penalty, refund), (0, 29_200), "exact estimate pays no penalty");
        assert_eq!(penalty + refund, 146_000 - 116_800, "gas is conserved");
    }

    /// Applied authorizations with enough real work stay penalty-free:
    /// `(gas_spent - A) / (gas_limit - A) = 100,000 / 950,000` is over the 10%
    /// threshold, so all unused gas is refunded.
    #[test]
    fn applied_authorizations_above_threshold_pay_no_penalty() {
        let (penalty, refund) = gas_penalty_and_refund(1_000_000, 150_000, 125_000, 50_000);
        assert_eq!((penalty, refund), (0, 875_000), "usage over 10% on the reduced basis");
        assert_eq!(penalty + refund, 1_000_000 - 125_000, "gas is conserved");
    }

    /// Applied authorizations with too little real work pay the quadratic
    /// penalty on the non-auth basis: `(gas_spent - A) / (gas_limit - A) =
    /// 71,000 / 1,000,000` is under the 10% threshold.
    #[test]
    fn applied_authorizations_below_threshold_pay_ratio_penalty() {
        let (penalty, refund) = gas_penalty_and_refund(1_050_000, 121_000, 96_800, 50_000);
        assert_eq!((penalty, refund), (78_128, 875_072), "quadratic penalty on non-auth work");
        assert_eq!(penalty + refund, 953_200, "penalty + refund is all unused gas");
        assert_eq!(penalty + refund, 1_050_000 - 96_800, "gas is conserved");
    }

    /// A halted transaction consumes its entire limit, so the reduced basis is
    /// a 100% usage ratio (`gas_spent - A == gas_limit - A`) and there is no
    /// penalty even with a large authorization list; the post-refund unused gas
    /// is returned in full.
    #[test]
    fn halted_transaction_with_authorization_refund_pays_no_penalty() {
        let (penalty, refund) = gas_penalty_and_refund(3_100_000, 3_100_000, 2_480_000, 3_000_000);
        assert_eq!((penalty, refund), (0, 620_000), "halt spends the whole limit");
        assert_eq!(penalty + refund, 3_100_000 - 2_480_000, "gas is conserved");
    }

    /// When the EIP-7623 calldata floor binds, revm's spent value IS the floor
    /// and carries no authorization component, so the handler's clamp excuses
    /// nothing and the penalty is priced on the unreduced basis — just over 10%
    /// usage here, hence zero.
    ///
    /// The 4-argument helper still honors its own contract: handed the
    /// unclamped 25,000 it produces the 10,021 residual this shape would
    /// otherwise pay. The clamp in `reimburse_caller` is the only thing keeping
    /// that overstatement out of the helper.
    #[test]
    fn eip7623_floor_bound_intrinsic_is_fully_absorbed() {
        assert_eq!(
            effective_auth_intrinsic(25_000, 421_000, 421_000),
            0,
            "the floor absorbed the whole intrinsic"
        );

        let (penalty, refund) = gas_penalty_and_refund(4_200_000, 421_000, 421_000, 0);
        assert_eq!((penalty, refund), (0, 3_779_000), "no penalty on the unreduced basis");
        assert_eq!(penalty + refund, 4_200_000 - 421_000, "gas is conserved");

        // the residual the handler clamp removes; kept so a deleted clamp has something to fail
        assert_eq!(
            gas_penalty_and_refund(4_200_000, 421_000, 421_000, 25_000),
            (10_021, 3_768_979),
            "an unclamped intrinsic still manufactures the old residual"
        );
    }

    /// Pins [`effective_auth_intrinsic`] across the shapes it has to separate.
    ///
    /// - floor-bound: the floor equals spent, so the whole intrinsic rode along free and nothing is
    ///   excused;
    /// - no-op: empty calldata leaves the floor at the bare 21,000 base, far below spent, so the
    ///   intrinsic was paid in full on top and passes through unchanged. These are the two engine
    ///   e2e shapes — the padded attacker and the honest small delegation — which is why the clamp
    ///   cannot move either of them;
    /// - partial: the floor absorbed 150,000 of a 250,000 intrinsic, so only the 100,000 the sender
    ///   actually paid is excused, and the penalty lands between the clamped and unclamped bases;
    /// - disabled floor: with `floor_gas == 0` the clamp degrades to `min(A, gas_spent)`. The
    ///   handler never reaches this arm — it skips the clamp entirely when EIP-7623 is disabled,
    ///   mirroring revm's own zeroing — but the function stays total.
    #[test]
    fn effective_auth_intrinsic_excuses_only_the_unabsorbed_portion() {
        assert_eq!(effective_auth_intrinsic(25_000, 421_000, 421_000), 0, "floor absorbs all");

        assert_eq!(
            effective_auth_intrinsic(3_000_000, 3_021_000, 21_000),
            3_000_000,
            "padded attacker: empty calldata absorbs nothing"
        );
        assert_eq!(
            effective_auth_intrinsic(125_000, 146_000, 21_000),
            125_000,
            "honest N=5 delegation: empty calldata absorbs nothing"
        );

        assert_eq!(
            effective_auth_intrinsic(250_000, 371_000, 271_000),
            100_000,
            "only the unabsorbed 100,000 is excused"
        );
        assert_eq!(
            gas_penalty_and_refund(3_710_000, 371_000, 371_000, 100_000),
            (207_532, 3_131_468),
            "clamped basis charges for the absorbed 150,000"
        );
        assert_eq!(
            gas_penalty_and_refund(3_710_000, 371_000, 371_000, 250_000),
            (1_411_982, 1_927_018),
            "the unclamped basis over-excuses and over-penalizes"
        );

        for (auth_intrinsic, gas_spent) in [(25_000u64, 421_000u64), (3_000_000, 21_000)] {
            assert_eq!(
                effective_auth_intrinsic(auth_intrinsic, gas_spent, 0),
                auth_intrinsic.min(gas_spent),
                "a zero floor degrades the clamp to min(A, gas_spent)"
            );
        }
    }

    /// The worst floor-bound shape inside the 30M batch limit now pays nothing.
    ///
    /// 71 authorization tuples (`A = 1,775,000`) behind 295,834 zero calldata bytes put the
    /// EIP-7623 floor at `10 * 295,834 + 21,000 = 2,979,340`. The floor exceeds the standard
    /// intrinsic, so revm rewrites `gas.spent()` to it and zeroes the refund, leaving
    /// `gas_spent == gas_used == 2,979,340` against a 29,793,400 limit — exactly the 10% usage
    /// this module documents as penalty-free.
    ///
    /// Before the clamp this honest sender paid `calculate_gas_penalty(28_018_400, 1_204_340) =
    /// 8,716,811` gas, 29.3% of its own gas limit, confiscated purely for hitting the calldata
    /// floor. The clamp sees a floor equal to spent, excuses nothing, and restores the zero.
    #[test]
    fn floor_bound_worst_case_pays_no_penalty() {
        let auth_intrinsic = effective_auth_intrinsic(1_775_000, 2_979_340, 2_979_340);
        assert_eq!(auth_intrinsic, 0, "the floor absorbed all 71 tuples");

        let (penalty, refund) =
            gas_penalty_and_refund(29_793_400, 2_979_340, 2_979_340, auth_intrinsic);
        assert_eq!((penalty, refund), (0, 26_814_060), "exactly 10% usage is penalty-free");
        assert_eq!(penalty + refund, 29_793_400 - 2_979_340, "gas is conserved");

        // the magnitude of what the clamp removes, so the doc's figure cannot rot
        assert_eq!(
            calculate_gas_penalty(28_018_400, 1_204_340),
            8_716_811,
            "the residual this shape paid before the clamp"
        );
    }

    /// Authorization tuples that apply against pre-existing authorities buy real penalty relief.
    /// The residual is an accepted tradeoff, not an oversight, so it is pinned here.
    ///
    /// N = 1180 applied tuples on a 30M limit: `gas_spent = 21,000 + 1180 * 25,000 = 29,521,000`,
    /// revm's 12,500-per-tuple refund (14,750,000 raw) is EIP-3529-capped at `gas_spent / 5 =
    /// 5,904,200`, so `gas_used = 23,616,800`. The full 29,500,000 intrinsic comes off both
    /// penalty arguments while the refund never comes off `gas_used`, leaving a 161,135 penalty
    /// where an empty reservation doing the same 21,000 of real work pays 29,560,762 — 0.545%
    /// retained.
    ///
    /// Accepted rather than clawed back because:
    ///
    /// - without the 7702 exclusion this sender pays nothing at all (the companion assert): it
    ///   spent 98.4% of its limit pre-refund and used 79% post-refund, so the plain 10% threshold
    ///   rule already clears it. The exclusion makes this sender pay MORE than the unmodified rule
    ///   would, so the 161,135 is conservatism, not a bypass.
    /// - the lever is strictly dominated. The 1180 tuples cost 29,500,000 of intrinsic (23,595,800
    ///   net of the capped refund) and put total out-of-pocket at 23,777,935 versus 29,581,762 for
    ///   the same work with no tuples — a real 19.6% saving, but still about 8x the ~3M a plain
    ///   calldata-floor reservation costs on `main` to any sender. The per-tuple trade is only
    ///   favorable up to N = 1180; the 1181st tuple costs more than it saves, so this is the
    ///   optimum, not a floor.
    #[test]
    fn applied_tuple_over_reservation_is_an_accepted_lever() {
        let (penalty, refund) =
            gas_penalty_and_refund(30_000_000, 29_521_000, 23_616_800, 29_500_000);
        assert_eq!((penalty, refund), (161_135, 6_222_065), "0.545% of the empty-reservation bill");
        assert_eq!(penalty + refund, 30_000_000 - 23_616_800, "gas is conserved");
        assert_eq!(
            calculate_gas_penalty(30_000_000, 21_000),
            29_560_762,
            "what the same real work pays with no authorization list"
        );

        // the same transaction judged by the plain threshold rule owes nothing, so the exclusion
        // is what makes this sender pay at all
        assert_eq!(
            gas_penalty_and_refund(30_000_000, 29_521_000, 23_616_800, 0),
            (0, 6_383_200),
            "98.4% of the limit spent is far over the 10% threshold"
        );

        // the doc's "N = 1180 is the optimum" claim, pinned: the 1181st tuple raises total
        // out-of-pocket (gas_used + penalty) from 23,777,935 to 23,778,105
        let (penalty_1181, refund_1181) =
            gas_penalty_and_refund(30_000_000, 29_546_000, 23_636_800, 29_525_000);
        assert_eq!((penalty_1181, refund_1181), (141_305, 6_221_895), "N = 1181 shape");
        assert!(
            23_636_800 + penalty_1181 > 23_616_800 + penalty,
            "the 1181st tuple must cost more than it saves"
        );
    }

    /// Pins the gas-table values the wrapper's documentation and the handler's
    /// intrinsic sourcing rely on, in both directions:
    ///
    /// - `PER_EMPTY_ACCOUNT_COST` is the 25,000-gas per-tuple charge and `PER_EMPTY_ACCOUNT_COST -
    ///   PER_AUTH_BASE_COST` is the 12,500-per-tuple refund the doc math quotes;
    /// - revm's mainnet gas params serve exactly `PER_EMPTY_ACCOUNT_COST` at Prague and 0 before it
    ///   — the pre-Prague guard the handler relies on when it multiplies the tuple count by this
    ///   cfg value;
    /// - the EIP-7623 floor parameters the handler's clamp recompute feeds
    ///   [`effective_auth_intrinsic`] with: the floor charge is `21,000 + 10 * tokens` and non-zero
    ///   calldata bytes count 4 tokens apiece (zero bytes 1).
    #[test]
    fn per_empty_account_cost_matches_prague_gas_table() {
        assert_eq!(PER_EMPTY_ACCOUNT_COST, 25_000, "per-tuple intrinsic charge");
        assert_eq!(PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST, 12_500, "per-applied-tuple refund");

        let prague = CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::PRAGUE);
        assert_eq!(
            prague.gas_params.tx_eip7702_per_empty_account_cost(),
            PER_EMPTY_ACCOUNT_COST,
            "Prague gas params serve the spec's per-empty-account cost"
        );

        assert_eq!(prague.gas_params.tx_floor_cost(0), 21_000, "floor base with no calldata");
        assert_eq!(
            prague.gas_params.tx_floor_cost(10_000),
            121_000,
            "floor charges 10 gas per calldata token on the 21,000 base"
        );
        assert_eq!(
            prague.gas_params.tx_token_non_zero_byte_multiplier(),
            4,
            "non-zero calldata bytes count 4 tokens"
        );

        // the handler recomputes revm's floor by hand before clamping; pin that the recompute
        // (`tx_floor_cost` over `get_tokens_in_calldata`) is exactly revm's own `floor_gas`
        // derivation for a 7702 transaction, and that the floor carries no authorization term —
        // the premise the whole clamp rests on
        let input = [0u8, 1, 0, 2];
        let derived = prague.gas_params.initial_tx_gas(&input, false, 0, 0, 2).floor_gas;
        assert_eq!(
            derived,
            prague.gas_params.tx_floor_cost(get_tokens_in_calldata(
                &input,
                prague.gas_params.tx_token_non_zero_byte_multiplier()
            )),
            "handler floor recompute must match revm's own derivation"
        );
        assert_eq!(
            derived,
            prague.gas_params.initial_tx_gas(&input, false, 0, 0, 0).floor_gas,
            "revm's floor is calldata-only: the authorization count must not move it"
        );

        let cancun = CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN);
        assert_eq!(
            cancun.gas_params.tx_eip7702_per_empty_account_cost(),
            0,
            "pre-Prague gas params serve 0, disabling the subtraction"
        );
    }
}
