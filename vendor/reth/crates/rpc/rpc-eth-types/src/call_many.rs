//! TN(TN-0057): the per-request bounds that `eth_callMany`, `trace_callMany` and
//! `debug_traceCallMany` share. Each of them runs any number of calls in one blocking task with
//! only a per-call gas cap; these bound the request as a whole. `eth_simulateV1` shares the gas
//! budget across its blocks (TN-0156).

use crate::EthApiError;
use reth_chainspec::MIN_TRANSACTION_GAS;

/// The most calls a request with a gas budget of `gas_limit` can run: each costs at least the
/// intrinsic gas, so a longer request would exhaust the budget anyway.
pub const fn max_calls(gas_limit: u64) -> usize {
    (gas_limit / MIN_TRANSACTION_GAS) as usize
}

/// Refuses a request of `calls` calls that [`max_calls`] cannot hold, before any state is read.
pub fn check_call_count(gas_limit: u64, calls: usize) -> Result<(), EthApiError> {
    let max = max_calls(gas_limit);
    if calls > max {
        return Err(EthApiError::InvalidParams(format!("request exceeds the limit of {max} calls")))
    }
    Ok(())
}

/// The gas a request may still use across its remaining calls.
///
/// A call is charged after it ran, so a request can overshoot the budget by at most one call's
/// gas cap; the call that crosses it is the last one that runs.
#[derive(Debug, Clone, Copy)]
pub struct CallManyBudget {
    left: u64,
    limit: u64,
}

impl CallManyBudget {
    /// A budget of `gas_limit`, the gas cap for calls on this node.
    pub const fn new(gas_limit: u64) -> Self {
        Self { left: gas_limit, limit: gas_limit }
    }

    /// Refuses a call whose gas limit the remaining budget cannot cover, before it runs; the
    /// rule a block applies to its transactions. For calls whose gas limit the caller signed,
    /// not one `prepare_call_env` capped.
    pub fn check(&self, gas_limit: u64) -> Result<(), EthApiError> {
        if gas_limit > self.left {
            return Err(EthApiError::InvalidParams(format!(
                "request exceeds the gas budget of {}",
                self.limit
            )))
        }
        Ok(())
    }

    /// TN(TN-0156): `gas_limit`, lowered to what the budget has left.
    pub const fn cap(&self, gas_limit: u64) -> u64 {
        if gas_limit < self.left {
            gas_limit
        } else {
            self.left
        }
    }

    /// Charges a finished call; fails once the request has used more than its budget.
    pub fn charge(&mut self, gas_used: u64) -> Result<(), EthApiError> {
        self.left = self.left.checked_sub(gas_used).ok_or_else(|| {
            EthApiError::InvalidParams(format!("request exceeds the gas budget of {}", self.limit))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_follows_the_gas_limit() {
        assert_eq!(max_calls(50_000_000), 2_380);
        assert!(check_call_count(50_000_000, 2_380).is_ok());
        let err = check_call_count(50_000_000, 2_381).unwrap_err();
        assert!(
            matches!(&err, EthApiError::InvalidParams(m) if m == "request exceeds the limit of 2380 calls"),
            "{err:?}"
        );
    }

    #[test]
    fn check_refuses_a_gas_limit_above_what_is_left() {
        let mut budget = CallManyBudget::new(50_000);
        budget.check(50_000).unwrap();
        budget.charge(21_000).unwrap();
        budget.check(29_000).unwrap();
        let err = budget.check(29_001).unwrap_err();
        assert!(
            matches!(&err, EthApiError::InvalidParams(m) if m == "request exceeds the gas budget of 50000"),
            "{err:?}"
        );
    }

    #[test]
    fn cap_lowers_a_gas_limit_to_what_is_left() {
        let mut budget = CallManyBudget::new(50_000);
        assert_eq!(budget.cap(30_000), 30_000);
        budget.charge(21_000).unwrap();
        assert_eq!(budget.cap(30_000), 29_000);
        assert_eq!(budget.cap(29_000), 29_000);
    }

    #[test]
    fn budget_charges_after_each_call() {
        let mut budget = CallManyBudget::new(50_000);
        budget.charge(21_000).unwrap();
        budget.charge(29_000).unwrap();
        let err = budget.charge(1).unwrap_err();
        assert!(
            matches!(&err, EthApiError::InvalidParams(m) if m == "request exceeds the gas budget of 50000"),
            "{err:?}"
        );
    }
}
