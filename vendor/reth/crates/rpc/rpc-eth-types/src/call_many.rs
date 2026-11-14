//! TN(TN-0057): the per-request bounds that `eth_callMany`, `trace_callMany` and
//! `debug_traceCallMany` share. Each of them runs any number of calls in one blocking task with
//! only a per-call gas cap; these bound the request as a whole. `eth_simulateV1` shares the gas
//! budget across its blocks (TN-0156). `eth_callMany`, `eth_simulateV1` and `eth_callBundle` also
//! bound the call output they keep for their response (TN-0183).

use crate::EthApiError;
use reth_chainspec::MIN_TRANSACTION_GAS;

/// TN(TN-0183): the most call output, in bytes, one `eth_callMany`, `eth_simulateV1` or
/// `eth_callBundle` request keeps for its response: 32 MiB of return and revert data, and of log
/// data for `eth_simulateV1`, counted raw (the response doubles it as hex).
///
/// The gas budget alone let a request keep about 2 bytes of output per gas: a call pays for its
/// memory quadratically, but many calls of about 100 KB each pay little, so a request kept about
/// 100 MB at the default 50M `--rpc.gascap`. These requests take one of the eth API's tracing
/// permits (`--rpc.proof-permits`) for their blocking work, so together they keep at most about
/// this many bytes per permit. The trace methods' calls are bounded by the trace cap instead.
pub const MAX_REQUEST_OUTPUT_BYTES: usize = 32 * 1024 * 1024;

/// TN(TN-0183): the call output a request may still keep, out of [`MAX_REQUEST_OUTPUT_BYTES`].
///
/// A call is charged after it ran, so the call that crosses the budget is the last one that runs.
#[derive(Debug, Clone, Copy)]
pub struct OutputBudget {
    left: usize,
}

impl Default for OutputBudget {
    fn default() -> Self {
        Self { left: MAX_REQUEST_OUTPUT_BYTES }
    }
}

impl OutputBudget {
    /// Charges `bytes` of a finished call's output; fails once the request keeps more than
    /// [`MAX_REQUEST_OUTPUT_BYTES`].
    pub fn charge(&mut self, bytes: usize) -> Result<(), EthApiError> {
        self.left = self.left.checked_sub(bytes).ok_or_else(|| {
            EthApiError::InvalidParams(format!(
                "request exceeds the output budget of {MAX_REQUEST_OUTPUT_BYTES} bytes"
            ))
        })?;
        Ok(())
    }
}

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
    /// TN(TN-0183): charged by `eth_callMany` and `eth_simulateV1` only.
    output: OutputBudget,
}

impl CallManyBudget {
    /// A budget of `gas_limit`, the gas cap for calls on this node.
    pub const fn new(gas_limit: u64) -> Self {
        Self {
            left: gas_limit,
            limit: gas_limit,
            output: OutputBudget { left: MAX_REQUEST_OUTPUT_BYTES },
        }
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

    /// TN(TN-0183): charges the output a finished call keeps for the response; see
    /// [`OutputBudget::charge`].
    pub fn charge_output(&mut self, bytes: usize) -> Result<(), EthApiError> {
        self.output.charge(bytes)
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

    /// TN(TN-0183): output up to the budget is kept, and a request is refused once it keeps
    /// more.
    #[test]
    fn output_is_charged_up_to_the_budget() {
        let mut budget = CallManyBudget::new(50_000);
        budget.charge_output(MAX_REQUEST_OUTPUT_BYTES - 1).unwrap();
        budget.charge_output(1).unwrap();
        budget.charge_output(0).unwrap();
        let err = budget.charge_output(1).unwrap_err();
        assert!(
            matches!(&err, EthApiError::InvalidParams(m)
                if m == "request exceeds the output budget of 33554432 bytes"),
            "{err:?}"
        );
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
