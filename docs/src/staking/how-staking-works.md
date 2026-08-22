---
description: >-
  Validators stake TEL on Telcoin Network consensus registry to participate in
  consensus and earn block rewards.
---

# How Staking Works

### Stake Versions

Every validator joins the network under a specific **stake version**. Each version defines a `StakeConfig`:

| Parameter           | Description                                |
| ------------------- | ------------------------------------------ |
| `stakeAmount`       | Exact TEL a validator must stake to join   |
| `minWithdrawAmount` | Minimum claimable reward threshold         |
| `epochIssuance`     | Total TEL distributed as rewards per epoch |
| `epochDuration`     | Length of each epoch                       |

Governance can create new versions by calling `upgradeStakeVersion()`. The new version takes effect next epoch. Validators who already staked retain the version they joined under -- their `stakeVersion` is permanent and set at stake time.

There is no mechanism to upgrade an existing validator's stake version in-place. To move to a new version, a validator would need to fully exit, unstake, and rejoin with a new address and new keys under the current version.

### How Rewards Are Calculated

At each epoch boundary, `applyIncentives()` distributes the epoch's issuance across validators. Each validator's reward is determined by their **weight**:

```
weight = stakeAmount * consensusHeaderCount
```

Where `stakeAmount` comes from the validator's own stake version (the version they joined under), and `consensusHeaderCount` is how many consensus headers they produced that epoch.

Each validator's share of the epoch issuance is:

```
reward = (epochIssuance * weight) / totalWeight
```

Where `totalWeight` is the sum of all validators' weights.

In a committee where all validators share the same stake version, the `stakeAmount` cancels out and rewards depend purely on header count. But in a **mixed committee** -- where validators joined under different versions with different stake amounts -- the version affects reward weight directly.

### Example 1: Mixed Committee After a Stake Increase

Governance raises the required stake from 100,000 TEL to 250,000 TEL.

**Configuration:**

* Version 0: `stakeAmount = 100,000 TEL`
* Version 1: `stakeAmount = 250,000 TEL`
* Epoch issuance: `10,000 TEL`

**Committee (4 validators, each producing 10 headers):**

| Validator | Version | Stake   | Headers | Weight        |
| --------- | ------- | ------- | ------- | ------------- |
| A         | v0      | 100,000 | 10      | 1,000,000     |
| B         | v0      | 100,000 | 10      | 1,000,000     |
| C         | v1      | 250,000 | 10      | 2,500,000     |
| D         | v1      | 250,000 | 10      | 2,500,000     |
| **Total** |         |         |         | **7,000,000** |

**Reward distribution:**

| Validator | Calculation                     | Reward         |
| --------- | ------------------------------- | -------------- |
| A         | 10,000 \* 1,000,000 / 7,000,000 | \~1,428.57 TEL |
| B         | 10,000 \* 1,000,000 / 7,000,000 | \~1,428.57 TEL |
| C         | 10,000 \* 2,500,000 / 7,000,000 | \~3,571.43 TEL |
| D         | 10,000 \* 2,500,000 / 7,000,000 | \~3,571.43 TEL |

Version 1 validators earn **2.5x** more per header than version 0 validators, directly proportional to the stake ratio (250,000 / 100,000).

### Example 2: Mixed Committee After a Stake Decrease

Governance lowers the required stake from 200,000 TEL to 50,000 TEL to reduce barriers to entry.

**Configuration:**

* Version 0: `stakeAmount = 200,000 TEL`
* Version 1: `stakeAmount = 50,000 TEL`
* Epoch issuance: `10,000 TEL`

**Committee (4 validators, each producing 10 headers):**

| Validator | Version | Stake   | Headers | Weight        |
| --------- | ------- | ------- | ------- | ------------- |
| A         | v0      | 200,000 | 10      | 2,000,000     |
| B         | v0      | 200,000 | 10      | 2,000,000     |
| C         | v1      | 50,000  | 10      | 500,000       |
| D         | v1      | 50,000  | 10      | 500,000       |
| **Total** |         |         |         | **5,000,000** |

**Reward distribution:**

| Validator | Calculation                     | Reward    |
| --------- | ------------------------------- | --------- |
| A         | 10,000 \* 2,000,000 / 5,000,000 | 4,000 TEL |
| B         | 10,000 \* 2,000,000 / 5,000,000 | 4,000 TEL |
| C         | 10,000 \* 500,000 / 5,000,000   | 1,000 TEL |
| D         | 10,000 \* 500,000 / 5,000,000   | 1,000 TEL |

Legacy version 0 validators earn **4x** more per header than version 1 validators. Their higher initial stake gives them greater weight in the distribution even though newer validators paid less to join.

### Example 3: Performance Differences in a Mixed Committee

Stake versions set the baseline weight, but header production still matters. A high-performing validator on a lower stake version can out-earn an underperforming validator on a higher version.

**Configuration:**

* Version 0: `stakeAmount = 100,000 TEL`
* Version 1: `stakeAmount = 200,000 TEL`
* Epoch issuance: `10,000 TEL`

**Committee (3 validators with varying performance):**

| Validator | Version | Stake   | Headers | Weight        |
| --------- | ------- | ------- | ------- | ------------- |
| A         | v0      | 100,000 | 30      | 3,000,000     |
| B         | v1      | 200,000 | 10      | 2,000,000     |
| C         | v1      | 200,000 | 20      | 4,000,000     |
| **Total** |         |         |         | **9,000,000** |

**Reward distribution:**

| Validator | Calculation                     | Reward         |
| --------- | ------------------------------- | -------------- |
| A (v0)    | 10,000 \* 3,000,000 / 9,000,000 | \~3,333.33 TEL |
| B (v1)    | 10,000 \* 2,000,000 / 9,000,000 | \~2,222.22 TEL |
| C (v1)    | 10,000 \* 4,000,000 / 9,000,000 | \~4,444.44 TEL |

Validator A on version 0 (lower stake) earns more than Validator B on version 1 (higher stake) because A produced 3x the headers. Performance and stake version both contribute to weight.

### Example 4: Uniform Committee (Single Version)

When all validators share the same version, `stakeAmount` cancels out entirely and rewards are distributed purely by header count.

**Configuration:**

* Version 0: `stakeAmount = 100,000 TEL`
* Epoch issuance: `10,000 TEL`

**Committee (3 validators):**

| Validator | Version | Stake   | Headers | Weight        |
| --------- | ------- | ------- | ------- | ------------- |
| A         | v0      | 100,000 | 15      | 1,500,000     |
| B         | v0      | 100,000 | 10      | 1,000,000     |
| C         | v0      | 100,000 | 25      | 2,500,000     |
| **Total** |         |         |         | **5,000,000** |

**Reward distribution:**

| Validator | Calculation                     | Reward    |
| --------- | ------------------------------- | --------- |
| A         | 10,000 \* 1,500,000 / 5,000,000 | 3,000 TEL |
| B         | 10,000 \* 1,000,000 / 5,000,000 | 2,000 TEL |
| C         | 10,000 \* 2,500,000 / 5,000,000 | 5,000 TEL |

With a uniform version, the stake amount is irrelevant to relative distribution. Rewards are directly proportional to the number of headers produced.

### Key Takeaways

* A validator's stake version is **permanent** -- it is locked in at the time of staking and cannot be changed.
* In a **uniform committee** (all same version), rewards depend only on consensus header production.
* In a **mixed committee**, validators on higher-stake versions earn proportionally more per header.
* If the stake **increases**, new validators have higher weight and earn more per header than legacy validators.
* If the stake **decreases**, legacy validators retain their higher weight advantage over newer, lower-stake validators.
* **Performance still matters**: a high-performing validator on a lower stake version can out-earn an underperforming validator on a higher version.
* The `epochIssuance` from the current epoch's `StakeConfig` determines the total reward pool, regardless of individual validator versions.
