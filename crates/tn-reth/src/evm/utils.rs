//! Utility functions used by EVM.
//!
//! Home of [`calculate_gas_penalty`], the quadratic penalty `TNEvmHandler::reimburse_caller`
//! applies to transactions that set a gas limit far above actual usage (issue #424). The
//! penalty is zero when the gas limit is at or below 210,000 or when at least 10% of the limit
//! was used; below 10% usage it grows quadratically with the shortfall, approaching
//! confiscation of all unused gas for extreme over-estimates. Integer-only `u128` arithmetic
//! (10^9 fixed-point precision) keeps results identical on every node — a consensus
//! requirement, since the penalty changes account balances.

use reth_revm::primitives::eip7702::PER_EMPTY_ACCOUNT_COST;
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
/// authorization intrinsic gas from the penalty basis and capping the penalty at
/// actually-unused gas.
///
/// `gas_spent` is pre-refund (`gas.spent()`); `gas_used` is post-refund
/// (`gas.spent_sub_refunded()`), matching `reimburse_caller`'s existing split.
///
/// # Why exclude the authorization intrinsic
///
/// The penalty asks "did you use enough of the gas you reserved?" and reads the
/// answer off `gas.spent()`. EIP-7702 charges a flat 25,000-gas intrinsic per
/// authorization tuple (`PER_EMPTY_ACCOUNT_COST`) from the tuple count alone,
/// before any validity check, so a sender can pad the authorization list with
/// junk tuples to inflate `gas.spent()` past the 10% threshold and collapse the
/// quadratic penalty on a batch-hogging gas limit — buying `gas.spent()` without
/// doing any work. Subtracting the authorization intrinsic from the penalty basis
/// removes that lever. Only the authorization intrinsic is excluded: calldata
/// (16 gas/byte) fairly prices batch bytes and the base 21,000 is negligible, so
/// legitimate low-execution transfers keep the pre-fix basis.
///
/// # Why cap at unused gas
///
/// Subtracting from the basis makes the penalty larger, and in the padded case it
/// can exceed `unused_gas`. `reimburse_caller` credits the full penalty to the
/// basefee address while flooring the caller refund at zero, so an uncapped
/// penalty would pay out `gas_used + penalty > gas_limit` — more than the sender
/// prepaid, minting value in consensus-critical code. Capping the penalty at
/// `unused_gas` restores exact conservation (`refund + penalty + gas_used ==
/// gas_limit`).
///
/// For `num_authorizations == 0` (every non-7702 transaction) `auth_intrinsic` is
/// zero and `calculate_gas_penalty` is already bounded by `gas_limit - gas_spent
/// <= gas_limit - gas_used = unused_gas`, so the cap never binds and this is
/// identical to the prior inline math.
pub(crate) fn gas_penalty_and_refund(
    gas_limit: u64,
    gas_spent: u64,
    gas_used: u64,
    num_authorizations: usize,
) -> (u64, u64) {
    let auth_intrinsic = (num_authorizations as u64).saturating_mul(PER_EMPTY_ACCOUNT_COST);
    let unused_gas = gas_limit.saturating_sub(gas_used);
    let penalty =
        calculate_gas_penalty(gas_limit, gas_spent.saturating_sub(auth_intrinsic)).min(unused_gas);
    (penalty, unused_gas.saturating_sub(penalty))
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
