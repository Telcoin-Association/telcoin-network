# Dynamic Epoch-Based Base Fee Adjustment

## Context

The `EpochManager` tracks gas used per worker throughout each epoch via `GasAccumulator`, but the `adjust_base_fees()` method (manager.rs:1281) is currently a no-op. This plan implements a dynamic base fee system that adjusts fees at epoch boundaries based on actual vs target gas usage, following EIP-1559 principles adapted for epoch-level granularity. Each worker gets its own independently-configurable adjustment.

---

## Target Gas Formula

### Intuition

Given the network parameters, we estimate how many batches each worker should produce across all validators during one epoch. The "target gas" is that batch capacity multiplied by batch gas limit, scaled by a target utilization ratio.

### Formula

```
// Step 1: Estimate batches per worker per validator

// Production-side: how many batches can one worker on one validator produce?
avg_header_delay = (min_header_delay + max_header_delay) / 2
avg_batches_per_header = (header_threshold + max_header_batches) / 2
headers_per_epoch = epoch_duration / avg_header_delay
production_capacity = headers_per_epoch * avg_batches_per_header / num_workers

// Also capped by batch timing (worker can't seal faster than max_batch_delay)
batch_timing_cap = epoch_duration / max_batch_delay

// Commit-side: how many can consensus commit per validator?
rounds_per_epoch = epoch_duration / target_commit_time
commit_capacity = rounds_per_epoch * avg_batches_per_header / num_workers

// Effective per-validator per-worker capacity (minimum of all constraints)
effective_per_validator = min(production_capacity, batch_timing_cap, commit_capacity)

// Step 2: Total across all validators for this worker-id
total_batches = committee_size * effective_per_validator

// Step 3: Target gas (scaled by worker's target utilization, e.g. 50%)
target_gas = total_batches * max_batch_gas * target_utilization_pct / 100
```

**Factors used:** committee size (N), batches per header (threshold + max), min/max header delay, max batch delay, target commit time, num workers, epoch duration, max batch gas, target utilization %.

### Base Fee Adjustment (EIP-1559-style)

```
if gas_used > target_gas:
    delta = current_fee * (gas_used - target_gas) / target_gas / max_change_denominator
    delta = clamp(delta, 1, current_fee / max_change_denominator)
    new_fee = current_fee + delta
else if gas_used < target_gas:
    delta = current_fee * (target_gas - gas_used) / target_gas / max_change_denominator
    delta = min(delta, current_fee / max_change_denominator)
    new_fee = max(current_fee - delta, MIN_PROTOCOL_BASE_FEE)
else:
    new_fee = current_fee
```

Uses `u128` intermediates to prevent overflow. Max change per epoch is `1/max_change_denominator` (default 1/8 = 12.5%).

---

## Files to Modify

### 1. NEW: `crates/types/src/base_fee.rs`

Pure computation module. Contains:

**`WorkerBaseFeeParams`** — per-worker configurable parameters:
```rust
pub struct WorkerBaseFeeParams {
    /// Target utilization % (0-100). Default: 50.
    pub target_utilization_pct: u64,
    /// Max fee change denominator per epoch. Default: 8 (12.5%).
    pub max_change_denominator: u64,
}
```

**`EpochBaseFeeContext`** — network-level inputs gathered at epoch boundary:
```rust
pub struct EpochBaseFeeContext {
    pub epoch_duration_ms: u64,
    pub target_commit_time_ms: u64,
    pub committee_size: usize,
    pub num_workers: usize,
    pub header_num_of_batches_threshold: usize,
    pub max_header_num_of_batches: usize,
    pub min_header_delay_ms: u64,
    pub max_header_delay_ms: u64,
    pub max_batch_delay_ms: u64,
    pub max_batch_gas: u64,
}
```

**`calculate_target_gas(ctx, worker_params) -> u64`** — computes the target gas for one worker using the formula above.

**`calculate_next_base_fee(current_fee, gas_used, target_gas, worker_params) -> u64`** — EIP-1559-style adjustment.

Unit tests for both functions covering: normal adjustment, empty epoch, max increase cap, floor at MIN_PROTOCOL_BASE_FEE, overflow edge cases, zero committee/zero commit time guards.

### 2. `crates/types/src/lib.rs`

Add `pub mod base_fee;` and re-export key types.

### 3. `crates/config/src/node.rs` — Parameters struct (~line 206)

Add two new fields to `Parameters`:

```rust
/// Expected consensus round commit time. Used for base fee target gas calculation.
#[serde(with = "humantime_serde", default = "Parameters::default_target_commit_time")]
pub target_commit_time: Duration,

/// Per-worker base fee adjustment configuration.
/// Index corresponds to worker ID. Workers beyond the vec length use WorkerBaseFeeParams::default().
#[serde(default)]
pub worker_basefee_params: Vec<WorkerBaseFeeParams>,
```

Default for `target_commit_time`: `Duration::from_millis(2000)`.

### 4. `crates/node/src/manager.rs` — EpochManager

**Add fields** to `EpochManager` struct (~line 97):
```rust
/// Epoch duration in seconds for current epoch (from on-chain EpochInfo).
epoch_duration_secs: u64,
/// Committee size for current epoch.
committee_size: usize,
```

**In `configure_consensus()`** (~line 1399): store these values when setting epoch_boundary:
```rust
self.epoch_duration_secs = epoch_info.epochDuration as u64;
// After committee is created:
self.committee_size = committee.size();
```

**Implement `adjust_base_fees()`** (~line 1281): replace the no-op with real logic:
- Build `EpochBaseFeeContext` from `self.epoch_duration_secs`, `self.committee_size`, and `self.builder.tn_config.parameters`
- For each worker: get `WorkerBaseFeeParams` (from config vec or default), call `calculate_target_gas()`, call `calculate_next_base_fee()`, write via `gas_accumulator.base_fee(worker_id).set_base_fee(new_fee)`
- Log the adjustment per worker (target_gas, gas_used, old_fee, new_fee)

### 5. Config YAML files

Add to `chain-configs/testnet/parameters.yaml` and `chain-configs/mainnet/parameters.yaml`:
```yaml
target_commit_time: 2s
```

(`worker_basefee_params` omitted = all workers use defaults)

---

## Edge Cases

| Case | Behavior |
|------|----------|
| Empty epoch (gas_used=0) | Fee decreases by up to 1/max_change_denominator per epoch, floors at MIN_PROTOCOL_BASE_FEE |
| Sustained 100% utilization | Fee increases max 12.5% per epoch (capped by denominator) |
| target_commit_time=0 | Guard: use 1000ms minimum |
| Committee size=1 | Works correctly — target proportional to 1 validator |
| Worker with no config entry | Falls back to WorkerBaseFeeParams::default() (50% target, 1/8 max change) |
| First epoch from genesis | Starts at MIN_PROTOCOL_BASE_FEE, adjusts normally |
| Restart mid-epoch | catchup_accumulator restores gas stats; adjust_base_fees only runs at actual epoch boundary |

---

## Verification

1. **Unit tests** in `crates/types/src/base_fee.rs`:
   - `calculate_target_gas` with known inputs → verify expected batch count and gas
   - `calculate_next_base_fee` at exact target → fee unchanged
   - `calculate_next_base_fee` above target → fee increases, capped at max change
   - `calculate_next_base_fee` at zero usage → fee decreases to floor
   - Overflow protection with large gas values
   - Different `WorkerBaseFeeParams` produce different adjustments

2. **Build**: `cargo build` and `cargo test -p tn-types`

3. **Existing tests**: `cargo test` — ensure no regressions (the no-op adjustment means existing tests don't depend on specific fee changes)
