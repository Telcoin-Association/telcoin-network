---
description: Staking was designed to be flexible and grow with the protocl.
---

# Future Direction

### Validator Scalability Limits

The ConsensusRegistry contract imposes a practical upper limit on the number of validators that can participate in the network. This limit is not defined by an explicit constant but emerges from gas constraints on storage-intensive loops that iterate over all existing validators. Under the current design, the network can support approximately 2,500-3,000 validators before critical operations risk exceeding gas limits.

This is an accepted tradeoff. The contract is designed for a permissioned validator set of mobile network operators (MNOs), of which there are roughly 1,000 worldwide.

### The Bottleneck: `_getValidators()`

The root cause is `_getValidators()`, an internal function that scans every existing ConsensusNFT to find validators matching a given status. It relies on ERC721Enumerable's `tokenByIndex()`, which performs a cold storage read (SLOAD) per token.

```
function _getValidators(ValidatorStatus status) internal view returns (ValidatorInfo[] memory) {
    ValidatorInfo[] memory validatorsMatched = new ValidatorInfo[]((totalSupply()));

    for (uint256 i; i < validatorsMatched.length; ++i) {
        address validatorAddress = _getAddress(tokenByIndex(i));  // SLOAD
        ValidatorInfo storage current = validators[validatorAddress];  // SLOAD(s)
        ...
    }
    ...
}
```

For each validator in the loop:

| Operation                                                    | Approximate Gas    |
| ------------------------------------------------------------ | ------------------ |
| `tokenByIndex(i)` -- read from `_allTokens[i]`               | 2,100 (cold SLOAD) |
| Read `validators[addr]` struct fields                        | 2,100-4,200        |
| Copy matched struct to memory (including 96-byte BLS pubkey) | 1,500-3,000        |
| **Per-validator total**                                      | **\~5,000-7,000**  |

On top of per-validator storage costs, Solidity's memory allocation cost grows quadratically. The function allocates a memory array sized to `totalSupply()`, and each `ValidatorInfo` consumes approximately 320 bytes in memory due to the dynamic `bytes blsPubkey` field.

### Where `_getValidators()` Is Called

The function appears on every critical path:

| Caller                          | Context                      | Times Called |
| ------------------------------- | ---------------------------- | ------------ |
| `concludeEpoch()`               | System call (epoch boundary) | 3 times      |
| `beginExit()`                   | Validator transaction        | 1 time       |
| `setNextCommitteeSize()`        | Governance transaction       | 1 time       |
| `burn()` via `_consensusBurn()` | Governance transaction       | 1 time       |

#### `concludeEpoch()` -- The Tightest Constraint

`concludeEpoch()` is the most gas-constrained path because it calls `_getValidators()` three times in a single transaction:

1. **`_getValidators(PendingActivation)`** inside `_updateValidatorQueue()` -- to activate queued validators
2. **`_getValidators(PendingExit)`** inside `_updateValidatorQueue()` -- to finalize exits for validators no longer in any committee
3. **`_getValidators(Active)`** after queue updates -- to validate the future committee size against total eligible validators

This function executes as a system call with a hardcoded gas limit of 30,000,000 (30M). The protocol sets this limit in `crates/tn-reth/src/evm/mod.rs` when constructing the system call transaction.

After the first `_getValidators()` call, subsequent calls benefit from warm storage (100 gas per SLOAD instead of 2,100), reducing repeat costs. Even so, the combined gas scales linearly with validator count and quadratically with memory allocation.

#### Estimated Gas for `concludeEpoch()`

| Validators | Storage Reads | Memory (3 arrays) | Estimated Total |
| ---------- | ------------- | ----------------- | --------------- |
| 500        | \~3.5M        | \~0.2M            | \~4M            |
| 1,000      | \~7M          | \~0.7M            | \~8M            |
| 1,500      | \~10.5M       | \~1.5M            | \~12M           |
| 2,000      | \~14M         | \~2.5M            | \~17M           |
| 2,500      | \~17.5M       | \~4M              | \~22M           |
| 3,000      | \~21M         | \~5.5M            | \~27M           |
| \~3,200    | \~22.5M       | \~6.5M            | \~30M (limit)   |

These estimates cover the three `_getValidators()` calls. The remaining work in `concludeEpoch()` -- sorting enforcement, epoch info updates, committee membership checks for pending exits -- adds further overhead, which tightens the practical limit below the theoretical maximum.

#### `applyIncentives()` -- Separate System Call

`applyIncentives()` executes as its own system call with a separate 30M gas budget. It iterates over the `rewardInfos` array (one entry per committee member, not all validators), so it scales with committee size rather than total validator count. The `uint16 nextCommitteeSize` field caps committee size at 65,535, well within gas limits for this function.

#### `beginExit()` -- Validator Transaction

`beginExit()` calls `_getValidators(Active)` once. As a regular transaction (not a system call), it is subject to the block gas limit. A single pass over all validators consumes roughly 6M gas at 1,000 validators, which is feasible under standard block gas limits but becomes a concern as validator count grows.

### Fixed Type Limits

Beyond gas constraints, several storage types impose hard ceilings:

| Field               | Type     | Maximum Value | Implication                              |
| ------------------- | -------- | ------------- | ---------------------------------------- |
| `nextCommitteeSize` | `uint16` | 65,535        | Maximum committee size per epoch         |
| `stakeVersion`      | `uint8`  | 255           | Maximum number of stake version upgrades |
| `currentEpoch`      | `uint32` | \~4.3 billion | Effectively unlimited for epoch count    |

The `uint8 stakeVersion` is the most notable hard limit. After 255 version upgrades, no further stake configuration changes can be made without a contract upgrade.

### Why This Is Acceptable Today

The contract comment on the `_getValidators()` function states the design rationale directly:

> _"There are \~1000 total MNOs in the world so `SLOAD` loops should not run out of gas"_

Telcoin Network validators are permissioned mobile network operators approved by governance via ConsensusNFT minting. The total addressable validator set is bounded by the number of MNOs globally (\~1,000), placing the network well within the \~2,500-3,000 validator gas ceiling.

At 1,000 validators, `concludeEpoch()` consumes approximately 8M gas out of its 30M budget -- roughly 27% utilization with comfortable headroom.

### Future Approaches to Support More Validators

If the network's growth objectives expand beyond the current MNO-focused validator set, several approaches could raise or eliminate the validator ceiling.

#### Increase the System Call Gas Limit

The simplest change. The 30M gas limit for system calls is a protocol-level constant, not a consensus rule. Raising it to 60M or 100M would proportionally increase the validator ceiling. System calls execute at zero gas price and do not count against the block's gas limit, so increasing this value has no impact on user transaction throughput. The tradeoff is longer execution time per epoch boundary, which affects block production latency.

#### Replace ERC721Enumerable with Direct Tracking

The ERC721Enumerable extension maintains an `_allTokens` array that enables `tokenByIndex()` but adds storage overhead on every mint and burn. Replacing it with a purpose-built data structure -- such as a simple `address[]` of active validators maintained alongside the existing `validators` mapping -- would eliminate the indirection through `tokenByIndex()` and reduce per-validator storage reads from \~3 SLOADs to \~1.

This change would also allow the contract to avoid iterating over validators that have exited but not yet unstaked (whose NFTs still exist in the enumerable set), further reducing unnecessary loop iterations.

#### Maintain Validator Sets by Status

Rather than filtering all validators by status at query time, the contract could maintain separate storage arrays per status (e.g., `activeValidators[]`, `pendingActivationQueue[]`, `pendingExitQueue[]`). Each state transition would move the validator between arrays. This converts `_getValidators()` from O(totalSupply) to O(relevantSet), which is dramatically cheaper when only a small subset of validators is in a given status.

The cost is additional bookkeeping on state transitions (swaps and pops on storage arrays), but these operations are constant-time per transition and occur infrequently relative to the epoch-boundary queries.

#### Move Validator Enumeration Off-Chain

The protocol client already has full visibility into the validator set through its own state. The on-chain `_getValidators()` calls serve two purposes: queue processing (activations and exits) and committee size validation. Both could be moved partially or fully to the protocol layer in Rust, with the contract receiving pre-computed results via system call parameters rather than re-deriving them from storage.

For example, `concludeEpoch()` could accept the list of validators to activate and exit as calldata parameters (validated against on-chain state) instead of scanning storage to discover them. This would reduce the function to O(queue size) rather than O(total validators).

#### Paginated or Incremental Queue Processing

Instead of processing all pending activations and exits in a single `concludeEpoch()` call, the protocol could process them incrementally across multiple system calls or in batches. A cursor-based pattern would allow `_updateValidatorQueue()` to resume where it left off if gas runs low, spreading the work across multiple transactions within the same block.

#### SSTORE2 for Static Validator Data

The `ValidatorInfo` struct stores a 96-byte `bytes blsPubkey` in contract storage, which consumes 4 storage slots (length + 3 data slots). Since BLS public keys are immutable after staking, they could be written once using SSTORE2 (storing data as contract bytecode via `CREATE`) and read back with `EXTCODECOPY`. Bytecode reads cost 2,600 gas for the first access plus 3 gas per byte, which is cheaper than multiple SLOADs for large data and avoids the per-slot overhead.

This optimization is most impactful when `_getValidators()` copies BLS pubkeys into memory for every matched validator. Excluding the pubkey from the storage struct and loading it on demand would reduce per-validator storage reads and memory costs.

### Summary

| Constraint                  | Current Limit | Bound By                                |
| --------------------------- | ------------- | --------------------------------------- |
| Practical validator ceiling | \~2,500-3,000 | `concludeEpoch()` gas (30M system call) |
| Committee size              | 65,535        | `uint16 nextCommitteeSize`              |
| Stake versions              | 255           | `uint8 stakeVersion`                    |
| Target validator set        | \~1,000 MNOs  | Network design                          |

The current design prioritizes simplicity and correctness for a known-bounded validator set. The gas ceiling is well above the target population of \~1,000 MNOs, providing roughly 2.5-3x headroom. If the network evolves to require a larger validator set, the most effective path is a combination of maintaining per-status validator arrays and moving enumeration logic to the protocol layer, which together would effectively eliminate the gas-based ceiling.
