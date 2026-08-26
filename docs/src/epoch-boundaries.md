# Epoch Boundaries

### Overview

Telcoin Network organizes consensus into fixed-duration **epochs**. Within an epoch, a stable committee of validators produces and orders transaction batches. At the boundary between epochs, the protocol performs three critical operations:

1. **Distribute validator rewards** for work done during the epoch
2. **Select and shuffle the next validator committee** using verifiable randomness
3. **Adjust the base fee** based on the epoch's aggregate gas usage

These operations execute as system calls within the last block of the epoch. They are deterministic and they do not consume block gas.

### Epoch Boundary Detection

Each epoch has a predefined time boundary. The consensus layer monitors the committed timestamp of each sub-DAG output. When an output's committed timestamp exceeds the epoch boundary, the system sets a `close_epoch` flag on that output.

The execution engine checks this flag during block finalization. When present, it triggers the epoch-closing system calls before completing the block. If the epoch's final consensus output contains no transaction batches, a single empty block is created specifically to carry these system calls.

### Step 1: Distribute Validator Rewards

#### How Rewards Are Tracked

During the epoch, the protocol tracks how many consensus headers each validator committed as leader. Every time a validator's leader certificate triggers a commit, their count increments by one. This happens regardless of whether the output contains transactions. Leaders for empty rounds still receive rewards for advancing the canonical consensus chain.

The tracking is purely in-memory. If a node restarts mid-epoch, it rebuilds the counts by walking already-executed blocks and replaying any consensus output that was committed but not yet processed.

#### How Rewards Are Applied

At the epoch boundary, the accumulated leader counts are converted into reward records:

| Field                  | Value                                                |
| ---------------------- | ---------------------------------------------------- |
| `validatorAddress`     | The validator's execution layer address              |
| `consensusHeaderCount` | Number of consensus headers they committed as leader |

These records are passed to the `ConsensusRegistry` contract via the `applyIncentives` system call. The contract distributes the epoch's issuance among validators, weighted by each validator's stake and consensus header count; accrued rewards are paid out through the Issuance contract when claimed.

Reward records are also stored in the block body's `withdrawals` field (repurposing the Ethereum withdrawal mechanism). This makes reward distribution visible to block explorers and indexers without requiring them to trace system call execution. Outside of epoch boundaries, the withdrawals field is empty. See [EVM Compatibilty](evm-compatibility.md#block-header-differences) for details on repurposed block header fields.

### Step 2: Select and Shuffle the Validator Committee

#### Why Shuffling Matters

A predictable committee order could enable targeted attacks against specific validators. TN uses verifiable randomness derived from BLS signatures to shuffle the committee, ensuring no party can predict or manipulate the ordering.

#### The Selection Process

1. **Read eligible validators** — the protocol queries the `ConsensusRegistry` contract for all validators with `Active` or `PendingActivation` status
2. **Handle insufficient validators** — if the number of active validators is less than the required committee size, validators with `PendingExit` status are randomly selected to fill the remaining slots
3. **Generate randomness** — the epoch seed chain produces a 32-byte seed: each epoch starts from a root derived from the epoch number, and every committed sub-DAG folds the committing leader's round and its deterministic BLS seed signature into the running value. The seed is deterministic (all nodes compute the same value) and unpredictable (it cannot be computed before the immediately preceding commit is published)
4. **Fisher-Yates shuffle** — the validator list is shuffled using the BLS-derived seed as the random number generator input
5. **Truncate to size** — the shuffled list is trimmed to the target committee size
6. **Sort by address** — the selected committee is sorted in ascending address order for deterministic indexing
7. **Commit on-chain** — the new committee is passed to `ConsensusRegistry.concludeEpoch(newCommittee)`

#### Validator Status Transitions

The `ConsensusRegistry` contract manages validator lifecycle:

| Status              | Meaning                                                                       |
| ------------------- | ----------------------------------------------------------------------------- |
| `Active`            | Eligible for committee selection                                              |
| `PendingActivation` | Staked and activated mid-epoch; eligible for selection, becomes `Active` at the boundary |
| `PendingExit`       | Requested to leave but may be retained if the committee is short              |

Validators transition between states based on on-chain governance actions. The epoch boundary is when these transitions take practical effect. Newly activated validators become eligible starting from the next epoch's committee selection.

### Step 3: Adjust the Base Fee

At the epoch boundary, the protocol evaluates each worker's aggregate gas usage against that worker's gas target and adjusts its base fee for the next epoch using the EIP-1559 algorithm.

The `GasAccumulator` tracks three metrics per worker throughout the epoch:

| Metric      | Description                          |
| ----------- | ------------------------------------ |
| `blocks`    | Number of blocks the worker produced |
| `gas_used`  | Total gas consumed across all blocks |
| `gas_limit` | Total gas limit across all blocks    |

These totals drive the base fee adjustment:

* **Gas used = target** — base fee stays the same
* **Gas used > target** — base fee increases (bounded by EIP-1559 max change denominator)
* **Gas used < target** — base fee decreases (with a protocol-defined minimum floor)

After adjustment, all accumulators are cleared for the next epoch. See [Basefees](basefees.md) for the full base fee model.

### System Calls

The epoch-closing operations execute as **system calls**, which are special transactions issued by the protocol rather than by users.

| Property      | Value                                                           |
| ------------- | --------------------------------------------------------------- |
| Caller        | `SYSTEM_ADDRESS` (`0xfffffffffffffffffffffffffffffffffffffffe`) |
| Target        | `ConsensusRegistry` contract                                    |
| Gas limit     | 100,000,000 per call                                            |
| Gas price     | 0 (no cost to the block)                                        |
| Nonce check   | Disabled                                                        |
| State changes | Committed immediately                                           |

System calls are invisible to users. They do not appear in the transaction list, do not consume block gas, and do not affect the gas used counter. Their effects are visible only through state changes (updated balances, committee rotations) and the block header metadata (`extra_data` and `withdrawals` fields).

#### Pre-Execution System Calls

In addition to epoch-closing calls, every block includes pre-execution system calls that run before user transactions:

* **Consensus root storage** (EIP-4788 style) — the first batch in each consensus output writes the `ConsensusHeader` digest to the beacon roots contract, making consensus data accessible to smart contracts
* **Block hash history** (EIP-2935) — the parent block hash is stored in the history contract for on-chain lookups

### Block Header at Epoch Boundaries

Two block header fields change behavior at epoch boundaries:

| Field         | At epoch boundary                                             | Normal blocks |
| ------------- | ------------------------------------------------------------- | ------------- |
| `extra_data`  | Committee-shuffle seed (the epoch seed chain value)            | Empty bytes   |
| `withdrawals` | Validator reward distribution records                         | Empty list    |

The `extra_data` value serves as a public indicator that the epoch closed at this block. It also provides the randomness seed used for committee shuffling, allowing anyone to verify the shuffle was performed correctly.

### Summary

| Property            | Value                                                           |
| ------------------- | --------------------------------------------------------------- |
| Trigger             | Committed timestamp exceeds epoch boundary                      |
| Operations          | Rewards, committee shuffle, base fee adjustment (in that order) |
| Execution method    | System calls (zero gas cost, no user-visible transactions)      |
| Randomness source   | Epoch seed chain (BLS seed signatures folded across the epoch's commits) |
| Shuffle algorithm   | Fisher-Yates with BLS-derived seed                              |
| Reward tracking     | Leader block count per validator across the epoch               |
| Base fee adjustment | EIP-1559 formula applied to epoch aggregate gas usage           |
| Visible in headers  | `extra_data` (shuffle seed) and `withdrawals` (rewards)         |
