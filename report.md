# Code Review: `basefees` branch -> `main`
Date: 2026-04-02
Scope: 6 commits adding EIP-1559-style per-epoch base fee adjustment, WorkerConfigs contract integration, CLI worker-fee-config flag, and e2e test coverage.

## Summary
Implements on-chain governance-driven base fee adjustment per worker per epoch. Core EIP-1559 logic is correct with good test coverage. One test arithmetic bug (cosmetic), one confirmed design gap around worker count synchronization, and one error handling concern. The remaining findings are intentional design decisions or false positives.

| # | Title | Severity | Category | Status |
|---|-------|----------|----------|--------|
| 1 | XOR instead of exponentiation in test balance | Low | Bugs | Confirmed |
| 2 | Worker count divergence between GasAccumulator and contract | Medium | Consensus Safety | Confirmed |
| 3 | Silent fallback on contract read failure | Medium | Error Handling | Confirmed |
| 4 | Test epoch duration inconsistencies | Low | Bugs | Confirmed |

## Findings

### 1. XOR instead of exponentiation in test balance
- **Severity**: Low
- **Category**: Bugs
- **Location**: `crates/tn-reth/src/lib.rs:1697` and `crates/tn-reth/src/lib.rs:2141`
- **Status**: Confirmed
- **Description**: `U256::from((50_000_000 * 10) ^ 18)` uses Rust's `^` (bitwise XOR) instead of exponentiation. Evaluates to ~500,000,018 wei instead of 50 million ether.
- **Impact**: Low. The governance wallet's balance isn't exercised in these tests because pre-genesis contract creation uses `value: U256::ZERO` and `gas_price: 0`. Tests pass despite the bug. However, extending these tests to require governance balance would fail unexpectedly.
- **Analysis**: The e2e basefees test correctly uses `parse_ether("50_000_000")`. These two test locations are the only occurrences of the XOR pattern.
- **Proposed Fix**:
```rust
// Replace both occurrences with:
GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000").unwrap()))
```

### 2. Worker count divergence between GasAccumulator and WorkerConfigs contract
- **Severity**: Medium
- **Category**: Consensus Safety
- **Location**: `crates/node/src/manager/node.rs:216` and `crates/node/src/manager/node/epoch.rs:686`
- **Status**: Confirmed
- **Description**: `GasAccumulator::new(1)` is hardcoded at node startup with 1 worker slot and persists across all epochs. At each epoch boundary, `adjust_base_fees` uses `gas_accumulator.num_workers()` (always 1) as the loop bound for reading WorkerConfigs. If governance calls `setNumWorkers(2)` on the contract, the new worker's config is never read. Conversely, if `gas_accumulator.base_fee(worker_id)` is called with an out-of-range id, it panics via `.expect("valid worker id")`.
- **Impact**: Currently safe because the protocol only runs 1 worker (the comment at line 215 says "we currently have one worker"). However, this is enforced only by convention, not by code. Adding a second worker without updating GasAccumulator initialization would cause silent miscalculation or panics.
- **Analysis**: The `GasAccumulator` is created once in `NodeManager::new()` and never resized. The WorkerConfigs contract has `setNumWorkers()` and `setWorkerConfig()` for governance to update, but GasAccumulator never re-reads from the contract. This is a latent bug that becomes critical when multi-worker support is enabled.
- **Proposed Fix**: Read `numWorkers` from the WorkerConfigs contract at epoch boundaries and validate it matches the GasAccumulator, or recreate the accumulator if it changes.

### 3. Silent fallback on contract read failure in `adjust_base_fees`
- **Severity**: Medium
- **Category**: Error Handling
- **Location**: `crates/node/src/manager/node/epoch.rs:688-694`
- **Status**: Confirmed
- **Description**: If `get_worker_fee_configs()` fails, the function logs a `warn!` and returns without adjusting any base fees. A persistent contract issue (missing deployment, corrupted storage) would silently freeze base fees indefinitely with only log warnings as evidence.
- **Impact**: On networks where WorkerConfigs is not deployed (e.g., older genesis), every epoch boundary would log a warning forever. Base fees would remain at their initialized value, preventing the fee market from responding to congestion or low utilization.
- **Analysis**: Other epoch-boundary operations in the same file use similar warn-and-continue patterns, so this is consistent with the codebase style. However, base fee adjustment is a new critical function that could benefit from stronger signaling.
- **Proposed Fix**: Escalate to `error!` level logging for persistent failures, or track consecutive failure count and escalate after N epochs.

### 4. Test epoch duration inconsistencies
- **Severity**: Low
- **Category**: Bugs
- **Location**: `crates/e2e-tests/tests/it/staking.rs:132` and `crates/tn-reth/src/lib.rs:2133`
- **Status**: Confirmed
- **Description**: The default epoch duration was changed from 86400s (24h) to 28800s (8h) in the CLI. The genesis test was updated, but two other test files still hardcode 86400.
- **Impact**: Tests pass because they explicitly set epoch duration in their test setup rather than relying on CLI defaults. However, these are inconsistencies that could confuse future contributors.
- **Analysis**: These tests construct `StakeConfig` directly with hardcoded values, so they're independent of the CLI default. The inconsistency is cosmetic but could mask issues if the tests are copied as templates for new tests.

## Dismissed Findings

### `adjust_base_fees` determinism (initially Medium) -> False Positive
**Reason**: Base fees are stored purely in-memory via `BaseFeeContainer` (`Arc<AtomicU64>`), not written to chain state. Each validator independently adjusts its own local base fee. There is no consensus requirement for validators to agree on base fee values — the base fee affects only local batch creation by the worker. The `payload_builder` reads base fee from the parent header, not from the atomic container. This is correct by design.

### Asymmetric delta floor (initially Informational) -> Design Decision
**Reason**: The `delta.max(1)` on the increase path (but not decrease) is intentional per commit `1bf5aa54` ("ensure at least 1 wei increase"). This prevents fees from being stuck at `MIN_PROTOCOL_BASE_FEE` (7 wei) when the network is congested. The asymmetry means fees at 7-15 wei have a mild upward bias, which is the desired behavior — the network should be able to escape minimum fees under load. Test `min_base_fee_still_increases_over_target` explicitly validates this.

### Epoch duration change (initially Informational) -> Design Decision
**Reason**: The change from 24h to 8h affects only new networks created with default CLI args. Existing networks are unaffected because epoch duration is set at genesis and stored in the ConsensusRegistry contract. This is a deliberate protocol parameter change.
