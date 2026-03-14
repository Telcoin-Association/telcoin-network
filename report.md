# EVM Security Audit Report

**Scope:** `crates/tn-reth/src/evm/` — Custom EVM handler, gas penalty, block execution, system calls
**Date:** 2026-03-14
**Status:** Complete

---

## Summary

| # | Concern | Status | Severity |
|---|---------|--------|----------|
| 1 | Difficulty field inconsistency | False Positive | Informational |
| 2 | Gas refund interaction with penalty | Confirmed | Medium |
| 3 | Penalty uses full effective_gas_price | False Positive | Informational |
| 4 | Committee shuffle undersized | False Positive | Informational |
| 5 | `set_basefee_address` silent failure | Confirmed | Medium |
| 6 | LazyLock for compile-time constants | Confirmed | Low |

---

## Concern 1: Difficulty Field Inconsistency

**Initial Severity:** Medium
**Status:** False Positive
**Actual Severity:** Informational
**Location:** `crates/tn-reth/src/evm/config.rs:135` vs `config.rs:183`

### Description

`next_evm_env` sets the EVM `BlockEnv.difficulty` to `U256::from(payload.batch_index)`, while `context_for_next_block` packs it as `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`. The block header uses `ctx.difficulty` from `context_for_next_block`.

### Evaluation

**Not a bug — intentional design.** The two difficulty values serve different purposes:

1. Post-merge Ethereum uses `PREVRANDAO` (opcode 0x44), not `DIFFICULTY`. TN correctly sets `prevrandao` in the BlockEnv (`config.rs:136`). The `difficulty` field in BlockEnv is effectively unused by the EVM during transaction execution.

2. The actual block header difficulty always comes from `TNBlockExecutionCtx.difficulty` (set by `context_for_next_block`), which is the packed `batch_index << 16 | worker_id` value. This is confirmed by `assemble_block` (`block.rs:676`) using `ctx.difficulty`.

3. The `evm_env()` method for existing blocks sets `difficulty: U256::ZERO` (`config.rs:99`), further confirming it's not read during execution.

4. Test assertions confirm the packed format: `assert_eq!(block.difficulty, U256::from(expected_batch_index << 16))`.

**Recommendation:** Consider adding a comment in `next_evm_env` explaining why `difficulty` is set to `batch_index` rather than the packed value, to prevent future confusion.

---

## Concern 2: Gas Refund Interaction with Penalty

**Initial Severity:** Medium
**Status:** Confirmed
**Actual Severity:** Medium
**Location:** `crates/tn-reth/src/evm/handler.rs:78-80`

### Description

`reimburse_caller` uses `gas.spent_sub_refunded()` for `gas_used`, which subtracts EVM gas refunds. A `debug_assert!(gas_refunded == 0)` only fires in debug builds.

### Evaluation

**Confirmed — gas refunds can occur and would inflate the penalty.**

- EVM gas refunds (from SSTORE clears, etc.) are not prevented by TN's custom EVM configuration
- Post-London EIP-3529 caps refunds at 20% of gas used, limiting the impact
- The `debug_assert!` is insufficient for production — it's stripped in release builds
- Impact is modest (1-2% penalty inflation in worst case) but real
- Users who clear storage (a beneficial operation) get penalized more than intended

### Proposed Fix

**Option A (Recommended):** Use pre-refund gas for penalty calculation:
```rust
let gas_used_before_refund = gas.spent();  // spent without subtracting refunds
let penalty_gas = calculate_gas_penalty(gas_limit, gas_used_before_refund);
```

**Option B (Monitoring):** Add a runtime warning:
```rust
let gas_refunded = gas.refunded() as u64;
if gas_refunded > 0 {
    tracing::warn!(target: "engine", ?gas_refunded,
        "unexpected gas refund detected - penalty may be overstated");
}
```

---

## Concern 3: Penalty Uses Full effective_gas_price, Not Just Basefee

**Initial Severity:** Medium
**Status:** False Positive
**Actual Severity:** Informational
**Location:** `crates/tn-reth/src/evm/handler.rs:112`

### Description

The gas penalty is calculated as `effective_gas_price * penalty_gas` and sent to the basefee address. `effective_gas_price` includes both basefee + priority fee. The concern was that block producers lose priority fees on penalized gas.

### Evaluation

**Not a bug — correct design with proper accounting.**

Full gas payment flow for a transaction:

1. User pays upfront: `gas_limit * effective_gas_price`
2. Refund to caller: `(unused_gas - penalty_gas) * effective_gas_price`
3. Beneficiary gets: `priority_fee * gas_used`
4. Basefee address gets: `basefee * gas_used + penalty_gas * effective_gas_price`

Verification:
```
Refund + Beneficiary + Basefee + Penalty
= (unused_gas - penalty_gas) * egp + priority_fee * gas_used + basefee * gas_used + penalty_gas * egp
= unused_gas * egp + (basefee + priority_fee) * gas_used
= (gas_limit - gas_used) * egp + egp * gas_used
= gas_limit * egp  ✓
```

The accounting is complete — no double-counting. The penalty includes the priority fee **by design**: penalties should scale with total transaction cost to properly disincentivize gas limit abuse regardless of market conditions. This is validated by the test suite which asserts exact governance safe balances.

---

## Concern 4: Committee Shuffle — Undersized Committee

**Initial Severity:** Medium
**Status:** False Positive
**Actual Severity:** Informational
**Location:** `crates/tn-reth/src/evm/block.rs:296-312`

### Description

If total active + pending_exit validators < `new_committee_size`, the committee would be smaller than required with no error in the Rust code.

### Evaluation

**Not a vulnerability — the on-chain contract enforces the invariant.**

The `ConsensusRegistry` smart contract provides multiple layers of protection:

1. **`setNextCommitteeSize`** validates `newSize <= eligible.length` — cannot set a committee size larger than eligible validators
2. **`concludeEpoch`** validates `futureCommittee.length == nextCommitteeSize` — reverts if size doesn't match
3. **`_checkCommitteeSize`** validates `committeeSize <= activeOrPending` — reverts if insufficient validators

Even if the Rust code produces an undersized committee, the `concludeEpoch` system call would revert on-chain, causing a fatal error that prevents the epoch from closing.

**Optional enhancement:** Add a defensive check in Rust for earlier error detection:
```rust
if new_committee.len() != new_committee_size {
    return Err(TnRethError::EVMCustom(
        format!("Committee size mismatch: expected {}, got {}",
            new_committee_size, new_committee.len())
    ));
}
```

---

## Concern 5: `set_basefee_address` Silent Failure

**Initial Severity:** Low
**Status:** Confirmed
**Actual Severity:** Medium
**Location:** `crates/tn-reth/src/lib.rs:172-174`

### Description

`set_basefee_address` uses `OnceLock::set()` which silently returns `Err` if already set. The error is discarded with `let _ =`.

### Evaluation

**Confirmed — silent failure with potential fund misdirection.**

- Called exactly once in production (`RethEnv::new()` at line 566), so the production risk is low
- In test environments, the static `OnceLock` persists across test cases within the same process. The first test to call `set_basefee_address()` permanently sets it; subsequent tests silently fail
- Base fees and gas penalties are redirected to this address — if wrong, funds go to the wrong recipient permanently
- The fallback `GOVERNANCE_SAFE_ADDRESS` is reasonable but may not match chain-specific configuration

### Proposed Fix

Add warning logging on failure:
```rust
fn set_basefee_address(address: Option<Address>) {
    let target_address = address.unwrap_or(GOVERNANCE_SAFE_ADDRESS);
    if BASEFEE_ADDRESS.set(target_address).is_err() {
        tracing::warn!(
            target: "tn-reth",
            current = ?BASEFEE_ADDRESS.get(),
            attempted = ?target_address,
            "basefee_address already set; ignoring attempt to set to different value"
        );
    }
}
```

---

## Concern 6: LazyLock for Compile-Time Constants

**Initial Severity:** Informational
**Status:** Confirmed
**Actual Severity:** Low
**Location:** `crates/tn-reth/src/evm/utils.rs:10-16`

### Description

`PRECISION`, `THRESHOLD`, `THRESHOLD_SQUARED` use `LazyLock` for values that are simple compile-time constants.

### Evaluation

**Confirmed — these should be `const`.**

- `u128::pow()` has been `const` since Rust 1.46
- `LazyLock` adds unnecessary runtime overhead (initialization synchronization + dereference at each use)
- 5 dereferences per call to `calculate_gas_penalty()`, a consensus-critical path
- No API reason these need to be `LazyLock`

### Proposed Fix

Replace in `crates/tn-reth/src/evm/utils.rs`:
```rust
const PRECISION: u128 = 10_u128.pow(9);
const THRESHOLD: u128 = 10_u128.pow(8);
const THRESHOLD_SQUARED: u128 = 10_u128.pow(16);
```

Then remove `*` dereferences at all use sites (lines 57, 60, 61, 67, 72). Also remove `use std::sync::LazyLock;`.
