# Geographic Diversity for Validator Committee Shuffling

## Context

Telcoin Network validators are exclusively GSMA MNOs (~700-1000 globally). The current committee selection uses a simple Fisher-Yates shuffle with no geographic awareness, meaning a committee of 32 could theoretically draw entirely from one region. This feature adds a `uint8 region` identifier to validators and modifies the shuffle to ensure robust geographic dispersion across the committee.

## Design Decision: Region on ValidatorInfo (Not Separate NFT Mapping)

**Recommendation: Store `region` on `ValidatorInfo` with a governance-only setter.**

The user prefers NFT-based storage for governance control. However, after analysis, ValidatorInfo is the better storage location while still achieving the governance-control goal:

- **Zero additional gas**: Current packed fields (activationEpoch + exitEpoch + currentStatus + isRetired + isDelegated + stakeVersion) = 12 bytes. Adding `uint8 region` = 13 bytes, still fits in the same 32-byte storage slot. No extra SLOAD.
- **A separate `mapping(uint256 => uint8) tokenRegion`** would add 1 cold SLOAD (2100 gas) per validator during shuffle -- 1000 extra SLOADs at epoch boundary. Unnecessary overhead.
- **Governance control is preserved**: A `setValidatorRegion(address, uint8)` function gated by `onlyOwner` gives governance full control, identical to how `setNextCommitteeSize` works today.
- **The NFT is soulbound and 1:1 with ValidatorInfo**: tokenId = uint160(validatorAddress). The lifecycle is identical -- validators can never re-stake after unstaking. Region on ValidatorInfo is just as permanent as region on the NFT.
- **`getValidators(Active)` already returns `ValidatorInfo[]`**: Region comes for free in the existing system call, requiring zero changes to the Rust data flow.

## GSMA MNO Region Mapping (uint8)

Based on GSMA's official market segmentation:

| uint8 | Region | Coverage |
|-------|--------|----------|
| 0 | Unspecified | Default / not yet assigned |
| 1 | Sub-Saharan Africa | All Africa south of Sahara |
| 2 | Middle East & North Africa | MENA region |
| 3 | Greater China | Mainland China, Hong Kong, Macau, Taiwan |
| 4 | Asia Pacific | Rest of Asia-Pacific (excl. Greater China) |
| 5 | Europe | EU, UK, non-CIS European countries |
| 6 | CIS | Commonwealth of Independent States |
| 7 | Latin America & Caribbean | Central/South America, Caribbean |
| 8 | North America | US, Canada |

Values 9-255 reserved for future use. This matches GSMA's standard regional breakdown used in their Intelligence reports and membership structure.

## Shuffle Algorithm: Region-Aware Round-Robin with Fisher-Yates

The algorithm ensures geographic diversity while maintaining determinism and fairness.

### Algorithm (replaces lines 318-330 in `block.rs`)

```
1. Separate candidates into assigned (region 1-8) and unassigned (region 0)
2. Group assigned candidates by region into BTreeMap<u8, Vec<ValidatorInfo>>
   (BTreeMap for deterministic iteration order)
3. Fisher-Yates shuffle within each region group (intra-region fairness)
4. Fisher-Yates shuffle the unassigned (region 0) pool
5. Collect region keys into Vec, Fisher-Yates shuffle them (randomize region visit order)
6. Round-robin: cycle through shuffled regions, taking one validator per region per round
7. After round-robin exhausts or fills diversity slots, fill remaining seats from
   the shuffled region 0 pool (no diversity constraint on these)
8. If still not full, continue round-robin from assigned regions for remaining slots
9. Extract addresses (caller sorts ascending)
```

### Region 0 Handling

Validators with region 0 (unspecified) **bypass diversity constraints entirely**. They fill remaining committee slots after region-aware selection, via standard Fisher-Yates. This means:
- Validators are never blocked from committee participation
- Governance is encouraged but not forced to assign regions
- The diversity guarantee applies only to region-assigned validators

### Diversity Guarantee

For committee size `C` with `R` represented regions (1-8 only) and `U` unassigned validators selected: the region-aware portion fills `C - U` seats with no region getting more than `ceil((C-U)/R)` seats. With typical C=32, R=6-8, and few unassigned validators, no region gets more than ~5-6 seats.

### Edge Cases
- **All validators region 0**: No diversity constraint applies, degrades to plain Fisher-Yates (same as current behavior)
- **All validators region-assigned**: Round-robin fills entire committee with diversity guarantee
- **Mix of assigned and unassigned**: Region-aware selection first, then unassigned fill remaining
- **Region exhausted**: Skipped in subsequent rounds, remaining slots filled from other regions or unassigned pool
- **Not enough candidates**: Existing pending_exit backfill logic runs first (unchanged)

## Implementation Plan

### Subagent 1: Solidity Contract Changes

**Files:**
- `tn-contracts/src/interfaces/IConsensusRegistry.sol` (line 16-25)
  - Add `uint8 region` to `ValidatorInfo` struct after `stakeVersion`
  - Add `event ValidatorRegionUpdated(address validatorAddress, uint8 region)`
  - Add `error InvalidRegion(uint8 region)`
  - Add `function setValidatorRegion(address, uint8) external` to interface

- `tn-contracts/src/consensus/ConsensusRegistry.sol`
  - Add `setValidatorRegion(address validatorAddress, uint8 region)` function (onlyOwner)
    - Requires `region <= 8` (revert `InvalidRegion` otherwise)
    - Requires validator has ConsensusNFT (`_checkConsensusNFTOwner`)
    - Writes `validators[validatorAddress].region = region`
    - Emits `ValidatorRegionUpdated`
  - Update `_recordStaked()` (line 526-548): Add `region` as 9th field in `ValidatorInfo` constructor, defaulting to `0`
  - Update `constructor` (line 894-972): Validate `currentValidator.region` (accept 0-8), the `ValidatorInfo` memory struct passed in will naturally include the new field
  - Keep `mint(address)` signature unchanged -- region is set via separate `setValidatorRegion` call

### Subagent 2: Solidity Test Changes

**Files:**
- `tn-contracts/test/consensus/ConsensusRegistryTestUtils.sol` - Update all `ValidatorInfo(...)` struct literals to include 9th `region` field (default `0` or test values)
- `tn-contracts/test/consensus/ConsensusRegistryTest.t.sol` - Update any tests constructing ValidatorInfo, add tests for `setValidatorRegion`
- `tn-contracts/test/consensus/ConsensusRegistryTestFuzz.t.sol` - Update struct literals

### Subagent 3: Rust Binding & Shuffle Changes

**Files:**
- `crates/tn-reth/src/system_calls.rs` (line 44-66)
  - Add `uint8 region` field to `ValidatorInfo` in `sol!` macro after `stakeVersion`

- `crates/tn-reth/src/evm/block.rs` (lines 271-335)
  - Modify `shuffle_new_committee()` to implement region-aware round-robin:
    1. Keep existing pending_exit backfill logic (lines 300-316)
    2. Replace lines 318-330 with:
       - Group `validators_for_shuffle` by `v.region` into `BTreeMap<u8, Vec<ValidatorInfo>>`
       - Fisher-Yates shuffle within each group
       - Collect and shuffle region keys
       - Round-robin select until `new_committee_size` reached
    3. Return type unchanged: `Vec<Address>`

### Subagent 4: Genesis & Config Changes

**Files:**
- `crates/config/src/genesis.rs` - Update genesis validator construction to include `region` field (default `0`)
- Any genesis JSON fixtures that include `ValidatorInfo` structs
- Regenerate contract artifacts via `forge build`

### Subagent 5: Documentation

**Files:**
- `tn-contracts/src/consensus/design.md` - Add section on geographic diversity and region mapping
- Inline documentation on `setValidatorRegion` and the region enum values

## Verification

1. **Solidity**: `forge test` - all existing tests pass with new field, new `setValidatorRegion` tests pass
2. **Rust**: `cargo test -p tn-reth` - verify sol! macro compiles with new field
3. **E2E**: Run epoch boundary e2e test (`crates/e2e-tests/tests/it/epochs.rs`) with updated genesis
4. **Shuffle correctness**: Unit test the round-robin algorithm with:
   - All validators in 1 region (degrades to current behavior)
   - Even distribution across 8 regions
   - Uneven distribution (1 large region, several small)
   - Region 0 mixed with assigned regions
   - Committee size > validators in any single region

## Parallel Execution Strategy

Subagents 1-5 can run in parallel since they touch independent files:
- **Subagent 1** (Solidity contracts) and **Subagent 2** (Solidity tests) could be combined or run sequentially since tests depend on contract changes
- **Subagent 3** (Rust changes) is independent of Solidity
- **Subagent 4** (Genesis/config) is independent
- **Subagent 5** (Documentation) is independent

Recommended batching: Run subagents 1+2 together, then 3, 4, 5 in parallel.
