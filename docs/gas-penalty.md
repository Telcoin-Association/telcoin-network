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

Four details matter for exact accounting:

- "Gas used" for the penalty is the gas spent **before** EVM refunds
  (for example SSTORE clearing refunds).
  The receipt field `gasUsed` is measured **after** EVM refunds, so the two values can differ.
- The penalty does not depend on transaction success.
  A reverted transaction that meets both conditions also pays it.
- System transactions are exempt.
- For EIP-7702 set-code transactions, both conditions above are measured after the
  authorization intrinsic is subtracted from the gas limit and from the gas used.
  The section on set-code transactions below has the details.

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
For an EIP-7702 transaction, both `gas_limit` and `gas_spent` are reduced by the
authorization intrinsic before this formula runs.
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

## EIP-7702 set-code transactions

An EIP-7702 transaction pays a flat 25,000 gas intrinsic for every authorization tuple in its
list.
The charge follows from the tuple's presence alone: the node applies it before it checks whether
the tuple can be used, so unusable tuples cost exactly as much as working ones.
Twenty authorizations add 500,000 gas to the transaction's spend before any of them is examined.
That per-tuple figure is `PER_EMPTY_ACCOUNT_COST` in the Prague gas table.
Before Prague the value is zero and nothing in this section applies.

A tuple that applies against an authority whose account already exists refunds 12,500 of its
25,000, subject to the EIP-3529 cap of one fifth of gas spent.
That refund lowers the receipt's `gasUsed`.
It does not lower the penalty basis, which is pre-refund gas.

The penalty excludes the authorization intrinsic.
Both the gas limit and the pre-refund gas are reduced by it before the formula runs:

```text
auth_intrinsic = 25,000 * number_of_authorization_tuples
penalty_gas    = calculate_gas_penalty(gas_limit - auth_intrinsic,
                                       gas_spent - auth_intrinsic)
penalty_gas    = min(penalty_gas, gas_limit - gasUsed)
```

The penalty is therefore priced as if the authorization list were not there.
Subtracting the intrinsic from both arguments has two consequences:

- A sender whose gas limit matches its actual usage pays nothing, however large the
  authorization list.
  Authorization gas is mandatory and prepaid at full price, so charging an over-reservation
  penalty on it would penalize a correct estimate.
- Padding the list buys no exemption.
  Extra tuples raise `gas_spent`, but they raise the reduced gas limit by the same amount, so the
  usage ratio the formula sees does not move.

Both thresholds shift by the intrinsic as well:

- The small-transaction exemption is `gas_limit <= 210,000 + auth_intrinsic`, not a flat 210,000.
- The 10 percent test is
  `(gas_spent - auth_intrinsic) >= 0.10 * (gas_limit - auth_intrinsic)`.

Only the authorization intrinsic is excluded.
Calldata gas and the 21,000 base are not: calldata gas prices the batch bytes the transaction
actually occupies.

| Gas Limit  | Tuples | Auth Intrinsic | Gas Spent | Gas Used  | Basis Usage % | Penalty Gas | Refund Gas |
|------------|--------|----------------|-----------|-----------|---------------|-------------|------------|
| 146,000    | 5      | 125,000        | 146,000   | 116,800   | 100%          | 0           | 29,200     |
| 521,000    | 20     | 500,000        | 521,000   | 416,800   | 100%          | 0           | 104,200    |
| 1,000,000  | 2      | 50,000         | 150,000   | 125,000   | 10.5%         | 0           | 875,000    |
| 1,050,000  | 2      | 50,000         | 121,000   | 96,800    | 7.1%          | 78,128      | 875,072    |
| 30,000,000 | 120    | 3,000,000      | 3,021,000 | 3,021,000 | 0.078%        | 26,560,959  | 418,041    |

"Basis Usage %" is `(gas_spent - auth_intrinsic) / (gas_limit - auth_intrinsic)`, the ratio the
10 percent test reads.
The first two rows are exact estimates with every tuple applied; the third does enough real work
to clear the threshold; the fourth does not; the fifth is a padded list.
The unit tests `honest_small_delegation_exact_estimate_pays_no_penalty`,
`honest_exact_estimate_with_capped_refund_pays_no_penalty`,
`applied_authorizations_above_threshold_pay_no_penalty`,
`applied_authorizations_below_threshold_pay_ratio_penalty`, and
`padded_7702_authorization_list_pays_a_near_maximal_penalty` assert these rows in that order.

The last row in detail: a 30,000,000 gas limit carrying 120 junk authorization tuples spends
3,021,000 gas, which is 21,000 of real work plus 3,000,000 of authorization intrinsic.
That is 10.07 percent of the limit, so on its face the transaction clears the 10 percent test
while doing 21,000 gas of work and holding 30,000,000 of batch capacity.
Removing the 3,000,000 intrinsic from both arguments prices it as 21,000 against 27,000,000, a
0.078 percent usage ratio.
The penalty is 26,560,959 gas of the 26,979,000 unused, and 418,041 is refunded.
That is the same penalty the same 21,000 of work pays with no authorization list at all.
By contrast, a transaction that spends the same 3,021,000 gas on real execution work pays
nothing, because 10.07 percent usage is over the threshold.

## EIP-7702 and the EIP-7623 calldata floor

EIP-7623 sets a floor on what a transaction pays: `21,000 + 10 * calldata_tokens`, where a zero
byte counts as one token and a non-zero byte as four.
When that floor is above what the transaction would otherwise spend, the floor becomes the
reported gas spend.

The floor is priced from calldata alone and carries no authorization term.
A floor-bound transaction therefore never paid the authorization intrinsic on top of the floor;
the intrinsic rode along inside it.
Subtracting the full `25,000 * N` in that case would remove gas from the penalty basis that the
sender never paid, and manufacture a penalty out of nothing.
So the node excuses only the part of the intrinsic that the floor did not absorb:

```text
effective_auth_intrinsic = min(auth_intrinsic, gas_spent - floor_gas)
```

Three shapes follow from this:

- **Floor at or above the standard cost.**
  Nothing is excused and the penalty is computed on the unreduced numbers.
  71 tuples (1,775,000 gas of intrinsic) behind 295,834 zero calldata bytes put the floor at
  `10 * 295,834 + 21,000 = 2,979,340` gas.
  Against a 29,793,400 gas limit, exactly ten times that floor, the excused intrinsic is 0, the
  usage ratio is exactly 10 percent, the penalty is 0, and all 26,814,060 of unused gas is
  refunded.
  Subtracting the full intrinsic here would have charged 8,716,811 gas, 29.3 percent of the
  sender's own gas limit.
  `floor_bound_worst_case_pays_no_penalty` pins both figures.
- **Floor below the spend.**
  The whole intrinsic is excused, exactly as in the section above.
  Empty calldata leaves the floor at the bare 21,000, so the padded 120-tuple case is unaffected
  by this clamp.
- **Partial absorption.**
  A 371,000 gas spend against a 271,000 gas floor excuses 100,000 of a 250,000 intrinsic.
  On a 3,710,000 gas limit the penalty is 207,532 gas, against the 1,411,982 an unclamped
  subtraction would charge.
  `effective_auth_intrinsic_excuses_only_the_unabsorbed_portion` pins these values.

The clamp only ever reduces a penalty, and it applies only to authorization gas.
No non-7702 transaction is affected by it.

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
3. **For set-code transactions, reduce both inputs first.**
   Subtract `25,000 * N` from the gas limit and from the gas spend before recomputing, where `N`
   is the length of the transaction's `authorizationList`.
   If the transaction carries enough calldata for the EIP-7623 floor to bind, subtract
   `min(25,000 * N, gas_spent - floor_gas)` instead, with
   `floor_gas = 21,000 + 10 * calldata_tokens`.
   Applied authorizations refund 12,500 gas each, which widens the gap between the pre-refund gas
   and the receipt's `gasUsed`, so the upper bound in step 2 is looser for these transactions.

## How to avoid the penalty

- Use `eth_estimateGas` and set the gas limit near the estimate.
- Keep the gas limit below ten times the expected gas usage.
- For small transactions, a gas limit of 210,000 or less is always exempt.
- For an EIP-7702 transaction, apply both rules to the gas that is not authorization intrinsic.
  Keep `gas_limit - 25,000 * N` below ten times the expected non-authorization gas usage, and
  read the flat exemption as `210,000 + 25,000 * N`.
  Extra tuples raise the penalty-free limit only by the gas they themselves cost, so they are
  never a cheap way to hold more batch capacity.

## Source of truth

| Behavior | Code |
|----------|------|
| Formula and thresholds | `crates/tn-reth/src/evm/utils.rs` (`calculate_gas_penalty`) |
| EIP-7702 authorization exclusion | `crates/tn-reth/src/evm/utils.rs` (`gas_penalty_and_refund`) |
| EIP-7623 floor clamp | `crates/tn-reth/src/evm/utils.rs` (`effective_auth_intrinsic`) |
| Refund split and fund destination | `crates/tn-reth/src/evm/handler.rs` (`TNEvmHandler::reimburse_caller`) |

This page mirrors those files.
Update this page when those files change.
