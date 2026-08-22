---
description: >-
  The primary goal is to incentivize validators to run more nodes, directly
  growing the size and resilience of the network. Every design decision flows
  from this objective.
---

# Why Membership Model

### Why This Approach

#### More nodes, not bigger nodes

The fundamental argument is about what economic behavior the staking model encourages.

In a stake-weighted system, an operator with 1,000,000 TEL and a 100,000 TEL minimum stake has no reason to run 10 nodes. Running one node with 1,000,000 TEL staked produces the same (or better, after infrastructure savings) return. The network gets one node.

In the membership model, that same operator can only stake 100,000 TEL per node. To deploy all their capital productively, they launch 10 validators -- each requiring governance approval, its own keys, its own infrastructure, and its own ConsensusNFT. The network gets 10 nodes. Each of those nodes independently produces consensus headers, serves the network, and strengthens its resilience against downtime and partitions.

This is not a theoretical distinction. The validator set size directly determines the network's fault tolerance threshold, geographic distribution potential, and resistance to correlated failures. A network with 100 nodes operated by 20 entities is materially more robust than one with 20 nodes operated by 20 entities where some happen to have larger stakes.

#### Performance-based differentiation

Equal-weight models avoid the concentration problem but introduce a free-rider problem. The membership model solves both: the fixed stake prevents concentration, while the header-count weight rewards active participation. Validators who invest in reliable infrastructure, maintain high uptime, and stay current on protocol upgrades earn more. They are incentivized because the contributed more to the network, not because they contributed more.

#### Version system as a governance lever

The stake version system gives governance a tool to adjust the network's economics without hard forks. If the network needs to raise the barrier to entry (e.g., to increase economic security), governance increments the version with a higher stake amount. If it needs to lower barriers to accelerate growth, it can reduce the stake. Existing validators are not retroactively affected, but earn rewards based on the proportion they have staked relative to all other committee members. This creates a stable, predictable environment for current operators while preserving flexibility for the network's future.

#### Permissioned context

Telcoin Network validators are mobile network operators vetted by governance. This is not an open-entry permissionless system where Sybil resistance must come purely from economic cost. The ConsensusNFT whitelist provides the Sybil resistance layer. The stake serves as an economic commitment and alignment mechanism. "Well known" validators act as the sole defense against malicious actors. This context makes a fixed-stake membership model viable where it might not be in a permissionless network with anonymous node operators.

### Approaches Considered

#### 1. Pure Stake-Weighted Model

In a traditional proof-of-stake system, validators lock variable amounts of stake and earn rewards proportional to the amount staked. More stake means more influence over consensus and a larger share of rewards.

**How it works:** A validator with 1,000,000 TEL staked earns 10x the rewards of a validator with 100,000 TEL. Block production probability, committee selection, and voting power all scale with stake.

**Pros:**

* Well-understood model with extensive research and battle-tested implementations (Ethereum, Cosmos, Solana)
* Economic security scales directly with total value locked — attacking the network requires acquiring proportional stake
* Flexible entry — validators can participate at various capital levels
* Capital efficiency — validators maximize returns from a single node without additional infrastructure

**Cons:**

* **Incentivizes concentration, not growth.** A validator with 1,000,000 TEL earns the same total reward whether they run one node or ten. Since each additional node requires infrastructure costs (hardware, bandwidth, operations), the rational choice is to run as few nodes as possible and concentrate stake. This directly works against network decentralization.
* **Rewards capital over contribution.** A well-capitalized validator running a mediocre node earns more than a high-performing validator with less capital. The network gets fewer, richer nodes rather than more, better nodes.
* **Barriers compound over time.** Early validators accumulate rewards faster, increasing their stake advantage. New entrants face a widening gap, discouraging participation and further concentrating the validator set.
* **Validator count stagnates.** There is no economic reason for an operator to launch additional nodes when they could simply add stake to an existing one. The total node count is bounded by the number of distinct operators, not their available capital.

#### 2. Equal-Weight Model

Every active validator earns an identical share of epoch rewards regardless of stake amount or any other factor. One validator, one equal share.

**How it works:** If there are 20 validators and the epoch issuance is 10,000 TEL, each validator receives 500 TEL.

**Pros:**

* Simplest possible reward scheme with no room for gaming
* No advantage to capital concentration
* Perfectly egalitarian -- every node is equally valued

**Cons:**

* **No performance incentive.** A validator producing minimal headers earns the same as one producing many. There is no reward for reliability, uptime, or active consensus participation.
* **Free-rider problem.** Validators can coast on minimal participation while collecting full rewards, degrading network quality without economic consequence.
* **No economic differentiation across stake versions.** If governance adjusts the required stake over time, validators who committed more capital receive no acknowledgment of that higher commitment in their reward share.

#### 3. Membership Model with Version-Weighted Rewards (TN Solution)

Validators stake a fixed amount determined by the current stake version to gain membership. Rewards are distributed based on the product of each validator's initial stake amount (from their version) and their consensus header count.

**How it works:** Each validator's reward weight is `stakeAmount * consensusHeaderCount`. In a uniform committee this reduces to pure performance-based distribution. In a mixed committee (validators on different stake versions), higher-stake versions earn proportionally more per header.

**Pros:**

* **Directly incentivizes more nodes.** Since each node requires its own fixed stake and earns independently, an operator who wants to maximize rewards must run more nodes. There is no way to "stack" additional capital onto a single node. The only path to higher total earnings is launching additional validators, each with their own ConsensusNFT, keys, and stake. This translates capital directly into network growth.
* **Rewards performance.** Header count is the differentiating factor among same-version validators. Reliable, high-uptime nodes earn more. This aligns economic incentives with network health.
* **Version-aware fairness.** When governance adjusts stake requirements, validators who committed more capital under a higher-stake version are weighted accordingly in reward distribution. This acknowledges their larger economic commitment without introducing variable staking.
* **Governance flexibility.** Stake amounts, issuance rates, and epoch parameters can be tuned per version without protocol changes. The network can adapt its economics as it matures.

**Cons:**

* Mixed-version committees create unequal per-header reward rates, which may be perceived as unfair by validators on lower-stake versions.
* Validators cannot upgrade their version in-place. They must fully exit, retire, and rejoin with a new identity, which involves downtime.
* The permissioned model (ConsensusNFT) means governance is a bottleneck for onboarding, which limits how quickly new nodes can be added even when operators are willing.

### Summary

| Model                         | Incentivizes More Nodes | Rewards Performance | Prevents Concentration | Governance Flexibility |
| ----------------------------- | ----------------------- | ------------------- | ---------------------- | ---------------------- |
| Stake-weighted                | No                      | Indirectly          | No                     | Limited                |
| Equal-weight                  | Neutral                 | No                  | Yes                    | Limited                |
| Membership + version-weighted | **Yes**                 | **Yes**             | **Yes**                | **Yes**                |

The membership model turns capital into nodes rather than into weight on existing nodes. For a network whose primary growth objective is expanding the validator set, this is the correct incentive structure.
