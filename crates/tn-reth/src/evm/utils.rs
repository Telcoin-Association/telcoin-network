//! Utility functions used by EVM.
//!
//! Home of [`calculate_gas_penalty`], the quadratic penalty `TNEvmHandler::reimburse_caller`
//! applies to transactions that set a gas limit far above actual usage (issue #424). The
//! penalty is zero when the gas limit is at or below 210,000 or when at least 10% of the limit
//! was used; below 10% usage it grows quadratically with the shortfall, approaching
//! confiscation of all unused gas for extreme over-estimates. Integer-only `u128` arithmetic
//! (10^9 fixed-point precision) keeps results identical on every node — a consensus
//! requirement, since the penalty changes account balances.

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
}
