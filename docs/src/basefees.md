# Basefees

## Telcoin Network Base Fee Model

### Overview

Telcoin Network (TN) implements an EIP-1559-style base fee mechanism adapted for its epoch-based consensus architecture. The core pricing algorithm is identical to Ethereum's EIP-1559, but the adjustment boundary is **per-epoch** rather than per-block.

### Key Concepts

**Epochs, not blocks:** TN consensus operates in epochs. Each epoch consists of multiple batches (analogous to blocks) produced by workers. The base fee is fixed for the duration of an epoch and only adjusts at epoch boundaries.

**Gas target:** Each epoch has a defined gas target representing the ideal gas consumption across all batches. This target functions identically to Ethereum's block gas and is the point at which the base fee remains unchanged.

**Uniform pricing within an epoch:** All transactions within a given epoch are priced at the same `base_fee_per_gas`, regardless of when during the epoch they are submitted. There is no intra-epoch fee variance.

### Base Fee Adjustment Algorithm

At each epoch boundary, the protocol compares the epoch's total gas consumed against the gas target:

* **Gas used = target** → base fee remains unchanged
* **Gas used > target** → base fee increases for the next epoch
* **Gas used < target** → base fee decreases for the next epoch

The adjustment formula follows EIP-1559:

```
if gas_used > gas_target:
    base_fee_delta = parent_base_fee * (gas_used - gas_target) / gas_target / MAX_CHANGE_DENOMINATOR
    next_base_fee = parent_base_fee + max(base_fee_delta, 1)

if gas_used < gas_target:
    base_fee_delta = parent_base_fee * (gas_target - gas_used) / gas_target / MAX_CHANGE_DENOMINATOR
    next_base_fee = parent_base_fee - base_fee_delta
```

Where `MAX_CHANGE_DENOMINATOR` bounds the maximum rate of change per epoch (the same constant as Ethereum's EIP-1559). The base fee has a protocol-defined minimum floor and cannot drop below it.

### Implications for Bridge Integrations

1. **Fee estimation:** Because the base fee is constant within an epoch, a single `eth_gasPrice` or `eth_getBaseFee` call gives you the exact base fee for all transactions until the current epoch ends. There is no block-to-block volatility to account for within an epoch.
2. **Fee predictability:** The maximum base fee increase between consecutive epochs is bounded by the EIP-1559 max change denominator. Fee spikes are smoothed across epoch boundaries rather than block boundaries, which generally provides more stable pricing over short time horizons.
3. **Transaction pricing:** TN transactions use the same `maxFeePerGas` / `maxPriorityFeePerGas` fields as Ethereum EIP-1559 transactions. The `base_fee_per_gas` portion is burned (and an equivalent amount minted to governance), and the priority fee goes to the batch producer.
4. **Epoch transitions:** If a transaction is submitted near an epoch boundary and lands in the next epoch, it will be priced at the new epoch's base fee. Standard EIP-1559 `maxFeePerGas` protections apply: the transaction will not execute if the new base fee exceeds the sender's `maxFeePerGas`.

### Summary

| Property                      | Ethereum          | Telcoin Network          |
| ----------------------------- | ----------------- | ------------------------ |
| Fee adjustment boundary       | Per block         | Per epoch                |
| Adjustment algorithm          | EIP-1559          | EIP-1559 (same formula)  |
| Fee stability within boundary | 1 block           | Entire epoch             |
| Gas target basis              | Single block      | Epoch aggregate          |
| Transaction type              | EIP-1559 (type 2) | EIP-1559 (type 2)        |
| Base fee floor                | 0 (effectively)   | Protocol-defined minimum |
