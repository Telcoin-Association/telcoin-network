# Gas Limit Penalty

### Why Penalties Exist

Telcoin Network uses a consensus-first architecture: transactions are ordered by consensus **before** they are executed. This means the actual gas a transaction consumes is unknown at the time it enters a batch. The network must rely on the gas limit declared by the sender to reserve space in each batch.

This creates a denial-of-service vector. A malicious or careless user can set an extremely high gas limit on a cheap transaction, consuming most of a batch's capacity while using almost none of it. The batch ends up nearly empty in terms of useful work, but no other transactions could fit.

To counter this, TN applies a **quadratic penalty** on transactions whose gas limit is grossly inflated relative to their actual usage. The penalty deducts a portion of the unused gas refund and redirects it to the chain's base-fee address for governance processing. This economically disincentivizes gas limit inflation without affecting well-estimated transactions.

### How It Works

For the exact formula, worked examples, and guidance for wallets and integrators on detecting the charge, see [Gas Over-Reservation Penalty](gas-penalty.md).

#### The 10% Rule

The penalty only activates when a transaction uses **less than 10%** of its declared gas limit. For legacy, EIP-2930 and EIP-1559 transactions, using 10% or more of the gas limit pays zero penalty.

**EIP-7702 set-code transactions are measured differently.** Each authorization tuple costs a flat 25,000 gas, charged for the tuple's presence alone. That gas is removed from *both* the usage figure and the gas limit before the 10% test runs, so an authorization list cannot be used to clear the threshold. A type-0x04 transaction can sit above 10% raw usage and still pay a large penalty: 120 tuples plus 21,000 gas of real work under a 30,000,000 gas limit is 10.07% raw usage and pays 26,560,959 gas. Read the [set-code section](gas-penalty.md#eip-7702-set-code-transactions) of the detailed page before setting a gas limit on a set-code transaction.

Additionally, transactions with a gas limit at or below 210,000 are always exempt, regardless of usage ratio. This protects simple transfers and low-cost interactions from unintended penalties.

#### Quadratic Scaling

When the penalty does activate, it scales **quadratically** based on how far below the 10% threshold the usage falls. The further below 10% usage a transaction is, the more aggressively the penalty grows.

In plain terms:

* At **9.9% usage**, the penalty is negligible
* At **5% usage**, roughly 25% of unused gas is penalized
* At **2% usage**, roughly 64% of unused gas is penalized
* At **0.1% usage**, over 95% of unused gas is penalized

The quadratic curve means the penalty ramps up gently near the threshold and steeply for extreme over-estimation, targeting the worst offenders while leaving borderline cases mostly unaffected.

#### The Formula

For transactions above the minimum gas limit threshold and below 10% usage:

```
usage_ratio = gas_used / gas_limit
inefficiency = 0.10 - usage_ratio
penalty_gas = (inefficiency^2 / 0.10^2) * unused_gas

# EIP-7702 only: take the ratio net of the authorization intrinsic.
# unused_gas is unchanged, since the intrinsic cancels out of the difference.
usage_ratio = (gas_used - auth_intrinsic) / (gas_limit - auth_intrinsic)
```

Where:

* `gas_used` is the actual gas consumed (before EVM refunds like SSTORE clearing)
* `gas_limit` is the gas limit declared by the sender
* `unused_gas = gas_limit - gas_used` (computed from the same pre-refund `gas_used`)
* `auth_intrinsic = 25,000 * N` for an EIP-7702 transaction carrying `N` authorization tuples, and zero for every other type
* The result is clamped so the penalty never exceeds `unused_gas`

**Note on EVM refunds:** The penalty calculation uses pre-refund gas (actual execution cost) to determine the usage ratio, so SSTORE refunds do not artificially inflate the penalty. The standard EVM refund is still applied when computing the user's gas reimbursement.

### Where the Fees Go

When a transaction executes on TN, fees are distributed as follows:

| Fee Component      | Recipient                  | Description                                     |
| ------------------ | -------------------------- | ----------------------------------------------- |
| Priority fee (tip) | Batch producer (validator) | `(effective_gas_price - base_fee) * gas_used`   |
| Base fee           | Base-fee address           | `base_fee * gas_used`                           |
| Gas limit penalty  | Base-fee address           | Quadratic penalty on unused gas (if applicable) |
| Remainder          | Refunded to sender         | Unused gas minus any penalty                    |

The penalty is deducted from what would otherwise be refunded to the transaction sender. It does not increase the total fee paid beyond the sender's declared `gas_limit * effective_gas_price` -- it only reduces the refund.

### Example Penalty Table

Every row is a plain transfer with no authorization list. Worked EIP-7702 rows are in the [set-code section](gas-penalty.md#eip-7702-set-code-transactions) of the detailed page.

| Gas Limit  | Gas Used | Usage % | Unused Gas | Penalty Gas | Penalty % of Unused |
| ---------- | -------- | ------- | ---------- | ----------- | ------------------- |
| 21,000     | 21,000   | 100%    | 0          | 0           | 0%                  |
| 210,000    | 21,000   | 10%     | 189,000    | 0           | 0%                  |
| 420,000    | 21,000   | 5%      | 399,000    | 99,750      | 25%                 |
| 1,000,000  | 21,000   | 2.1%    | 979,000    | 610,993     | 62.4%               |
| 5,000,000  | 21,000   | 0.42%   | 4,979,000  | 4,569,546   | 91.8%               |
| 10,000,000 | 21,000   | 0.21%   | 9,979,000  | 9,564,282   | 95.8%               |
| 30,000,000 | 21,000   | 0.07%   | 29,979,000 | 29,560,762  | 98.6%               |

### How to Avoid Penalties

1. **Use accurate gas estimation.** Call `eth_estimateGas` before submitting transactions and set the gas limit based on the estimate. A reasonable buffer (e.g., 1.5x-2x the estimate) will not trigger penalties as long as the final usage stays above 10% of the gas limit. For an EIP-7702 transaction, apply the buffer to the part of the estimate that is not authorization gas; the `25,000 * N` intrinsic is a fixed cost and needs no buffer.
2. **Understand the 10x safe zone.** As a rule of thumb, if your gas limit is no more than 10x your actual gas consumption, you will never pay a penalty. A simple transfer using \~21,000 gas can safely use a gas limit up to 210,000 with zero penalty. **For an EIP-7702 transaction, apply the rule to non-authorization gas only:** keep `gas_limit - 25,000 * N` under ten times the gas you expect to spend outside the authorization list. Ten tuples behind 21,000 gas of work under a 2,710,000 gas limit is exactly 10x the spend, and still pays 2,040,359 gas of penalty.
3. **Small transactions are exempt.** Any transaction with a gas limit of 210,000 or less is exempt from penalties entirely, regardless of usage ratio.
4. **Do not hardcode inflated gas limits.** Setting a gas limit to the maximum batch gas without estimating actual usage is the primary behavior this penalty targets. Bridge contracts and relayers should always estimate gas for each message rather than using a static high value.

### Summary

| Property                      | Value                                                                                |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| Penalty threshold             | Usage below 10% of gas limit; for EIP-7702, usage and limit net of authorization gas |
| Scaling                       | Quadratic (gentle near threshold, steep at extremes)                                 |
| Minimum gas limit for penalty | > 210,000; for EIP-7702, > 210,000 plus authorization gas                            |
| Penalty destination           | Base-fee address (for governance processing)                                         |
| Maximum penalty               | Cannot exceed unused gas                                                             |
| Safe multiplier               | Up to 10x estimated gas = zero penalty; for EIP-7702, 10x the non-authorization gas  |
