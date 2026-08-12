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
/// | 10,000,000 | 21,000   | 0.21%   | 9,979,000   | 9,564,282   | 95.6%             |
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
    let unused_gas = gas_limit_u128 - gas_used_u128;
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
/// - `auth_intrinsic`: the wire authorization-tuple count multiplied by the spec's
///   per-empty-account cost, supplied by the caller from revm's cfg gas params — so it is 0 before
///   Prague and matches exactly what revm charged at Prague and later.
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
/// - Residual tradeoff: when the EIP-7623 calldata floor binds, revm's spent value IS the floor,
///   which carries no authorization intrinsic; subtracting `A` from it can produce a small penalty
///   where the pre-fix code gave zero (pinned by `eip7623_floor_basis_can_pay_a_small_penalty`).
///   Accepted: the floor case is rare and the penalty is tiny.
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
pub(crate) fn gas_penalty_and_refund(
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

#[cfg(test)]
mod tests {
    use super::*;
    use reth_revm::{
        context::CfgEnv,
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

    /// Documents the accepted EIP-7623 residual: when the calldata floor binds,
    /// revm's spent value IS the floor, which carries no authorization
    /// intrinsic — so subtracting `A` from it yields a small penalty where the
    /// pre-fix numerator-only code gave zero. Accepted because the floor case
    /// is rare and the penalty is tiny (10,021 of 3,779,000 unused gas here).
    #[test]
    fn eip7623_floor_basis_can_pay_a_small_penalty() {
        let (penalty, refund) = gas_penalty_and_refund(4_200_000, 421_000, 421_000, 25_000);
        assert_eq!((penalty, refund), (10_021, 3_768_979), "small penalty on the floor basis");
        assert_eq!(penalty + refund, 4_200_000 - 421_000, "gas is conserved");
    }

    /// Pins the gas-table values the wrapper's documentation and the handler's
    /// intrinsic sourcing rely on, in both directions:
    ///
    /// - `PER_EMPTY_ACCOUNT_COST` is the 25,000-gas per-tuple charge and `PER_EMPTY_ACCOUNT_COST -
    ///   PER_AUTH_BASE_COST` is the 12,500-per-tuple refund the doc math quotes;
    /// - revm's mainnet gas params serve exactly `PER_EMPTY_ACCOUNT_COST` at Prague and 0 before it
    ///   — the pre-Prague guard the handler relies on when it multiplies the tuple count by this
    ///   cfg value.
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

        let cancun = CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN);
        assert_eq!(
            cancun.gas_params.tx_eip7702_per_empty_account_cost(),
            0,
            "pre-Prague gas params serve 0, disabling the subtraction"
        );
    }
}
