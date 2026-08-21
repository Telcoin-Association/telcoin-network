# Gas over-reservation penalty

Telcoin Network charges a penalty when a transaction reserves far more gas than it uses.
This page is for transaction senders, wallet developers, and integrators.
It explains when the penalty applies, how much it costs, where the funds go, and how to detect it.

Contributor documentation lives in the "Fee economics" section of
[`crates/tn-reth/README.md`](../crates/tn-reth/README.md).
The implementation lives in
[`crates/tn-reth/src/evm/utils.rs`](../crates/tn-reth/src/evm/utils.rs) and
[`crates/tn-reth/src/evm/handler.rs`](../crates/tn-reth/src/evm/handler.rs).

## Why the penalty exists

Telcoin Network orders transactions in consensus before it executes them.
Batch capacity is reserved by each transaction's gas limit, not by its final gas usage.
Without a penalty, a sender could set a very large gas limit, use almost none of it,
and consume batch capacity for free.
The penalty makes large over-reservation expensive.
Issue [#424](https://github.com/Telcoin-Association/telcoin-network/issues/424)
has the design history.

## When the penalty applies

The penalty applies only when both conditions hold:

1. The gas limit is greater than 210,000.
   This threshold is `MIN_GAS_LIMIT_THRESHOLD`, ten times the 21,000 minimum transaction cost.
2. The transaction uses less than 10 percent of its gas limit.

If either condition fails, the penalty is zero and the sender receives the normal unused-gas refund.

Three details matter for exact accounting:

- "Gas used" for the penalty is the gas spent **before** EVM refunds
  (for example SSTORE clearing refunds).
  The receipt field `gasUsed` is measured **after** EVM refunds, so the two values can differ.
- The penalty does not depend on transaction success.
  A reverted transaction that meets both conditions also pays it.
- System transactions are exempt.

## The formula

The node computes the penalty in gas units with `u128` integer math
(fixed-point precision `10^9`):

```text
usage_ratio_scaled = (10^9 * gas_spent) / gas_limit        # integer division
if usage_ratio_scaled >= 10^8:                             # usage >= 10%
    penalty_gas = 0
else:
    unused_gas  = gas_limit - gas_spent
    penalty_gas = ((10^8 - usage_ratio_scaled)^2 * unused_gas) / 10^16
```

`gas_spent` is the pre-refund gas.
The penalty grows quadratically as usage falls below 10 percent.
It approaches full confiscation of the unused gas for extreme over-estimates.
It never exceeds the unused gas, so the refund never goes below zero.
Integer-only math keeps the result identical on every node.
The result changes account balances, so it is consensus-critical.

The charge in wei is:

```text
penalty_wei = penalty_gas * effective_gas_price
```

## Examples

| Gas Limit  | Gas Used | Usage % | Unused Gas  | Penalty Gas | Penalty % of Unused |
|------------|----------|---------|-------------|-------------|---------------------|
| 21,000     | 21,000   | 100%    | 0           | 0           | 0%                  |
| 210,000    | 21,000   | 10%     | 189,000     | 0           | 0%                  |
| 420,000    | 21,000   | 5%      | 399,000     | 99,750      | 25%                 |
| 1,000,000  | 21,000   | 2.1%    | 979,000     | 610,993     | 62.4%               |
| 5,000,000  | 21,000   | 0.42%   | 4,979,000   | 4,569,546   | 91.8%               |
| 10,000,000 | 21,000   | 0.21%   | 9,979,000   | 9,564,282   | 95.8%               |
| 30,000,000 | 21,000   | 0.07%   | 29,979,000  | 29,560,762  | 98.6%               |

These rows mirror the reference table on `calculate_gas_penalty`.
The unit test `test_gas_penalty_calculation` in the same file asserts the penalty values.

A concrete cost: a simple transfer (21,000 gas used) sent with a 1,000,000 gas limit
at an effective gas price of 100 gwei pays a penalty of 610,993 gas.
At that price the charge is 6.10993 x 10^16 wei,
which is 0.0610993 TEL with the native token's standard 18 decimal places.
The same transfer sent with a gas limit of 210,000 or less pays no penalty.

## What the sender pays

The sender prepays `gas_limit * effective_gas_price`.
After execution the node refunds:

```text
refund_wei = (gas_limit - gasUsed - penalty_gas) * effective_gas_price
```

`gasUsed` is the post-refund value that appears in the receipt.
The net fee is therefore:

```text
net_fee_wei = (gasUsed + penalty_gas) * effective_gas_price
```

The penalty portion is credited to the chain's base-fee address for governance processing.
It is not burned.

## What the receipt shows

Nothing.
A penalized transaction and an unpenalized one produce identical receipts.
`gasUsed` does not include the penalty.
No EVM log or event marks the charge.
Node operators can see `tracing` debug lines from
`crates/tn-reth/src/evm/utils.rs` and `crates/tn-reth/src/evm/handler.rs`.
System transactions produce no such lines.
RPC clients cannot see them.

## How to detect the penalty from standard RPC

Wallets and indexers can recover the charge without new RPC methods:

1. **Exact, from balances.**
   For a transaction that only sends value from the sender:
   `net_fee_wei = balance_before - balance_after - value`.
   Then `penalty_wei = net_fee_wei - gasUsed * effectiveGasPrice`,
   with both fields taken from the receipt.
   Adjust `balance_after` for any internal transfers the sender receives
   in the same transaction.
2. **Exact or upper bound, from the formula.**
   Recompute the formula with the transaction's `gas` field as `gas_limit`
   and the receipt's `gasUsed` as `gas_spent`.
   If the transaction produced no EVM refunds, `gasUsed` equals the pre-refund gas
   and the result is exact.
   If it produced refunds, `gasUsed` is smaller than the pre-refund gas
   and the result is an upper bound on the true penalty.

## How to avoid the penalty

- Use `eth_estimateGas` and set the gas limit near the estimate.
- Keep the gas limit below ten times the expected gas usage.
- For small transactions, a gas limit of 210,000 or less is always exempt.

## Source of truth

| Behavior | Code |
|----------|------|
| Formula and thresholds | `crates/tn-reth/src/evm/utils.rs` (`calculate_gas_penalty`) |
| Refund split and fund destination | `crates/tn-reth/src/evm/handler.rs` (`TNEvmHandler::reimburse_caller`) |

This page mirrors those files.
Update this page when those files change.
