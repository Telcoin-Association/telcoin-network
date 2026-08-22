# Canonical Updates

### Overview

Telcoin Network maintains three complementary chains:

1. **Consensus Chain** — a hash-linked chain of `ConsensusHeader`s that records the ordering decisions made by BFT consensus. This chain is the source of truth for transaction ordering.
2. **Execution Chain** — a standard Ethereum-compatible chain of blocks. Each block contains executed transactions and the resulting state changes.
3. **Epoch Chain** — a hash-linked chain of `EpochRecord`s, one per epoch, that checkpoint committee membership and chain state at each epoch boundary. This chain enables trustless node synchronization without requiring execution state.

The key architectural principle is **consensus-first execution**: transactions are ordered by consensus _before_ they are executed. The consensus layer determines what transactions run and in what order. The execution layer applies them to EVM state.

This separation means the consensus layer is execution-agnostic. It operates on raw transaction bytes and cryptographic certificates without any knowledge of EVM, gas, or smart contracts. Different execution environments can be supported in the future without modifying the consensus protocol.

### The Consensus Chain

#### ConsensusHeaders

The consensus chain is a sequence of `ConsensusHeader`s, each linked to its predecessor by a `parent_hash`:

| Field         | Description                                                          |
| ------------- | -------------------------------------------------------------------- |
| `parent_hash` | Keccak-256 hash of the previous `ConsensusHeader`                    |
| `sub_dag`     | The committed sub-DAG from consensus (contains ordered certificates) |
| `number`      | Block height in the consensus chain                                  |

Each header's digest is computed from `hash(parent_hash || sub_dag.digest() || number)`, forming a tamper-evident chain.

#### Committed Sub-DAGs

TN uses a DAG-based BFT consensus protocol (Bullshark). Validators produce certificates that reference prior certificates, forming a directed acyclic graph. When a leader certificate reaches a quorum of support, the sub-DAG it commits is finalized.

A `CommittedSubDag` contains:

* **Certificates** — signed proposals from validators, each referencing batches of transactions
* **Leader certificate** — the certificate that triggered the commit
* **Reputation scores** — per-validator performance metrics from the round
* **Commit timestamp** — when the sub-DAG was committed

#### BLS Signatures

Every certificate carries an aggregated BLS signature from the quorum of validators that endorsed it. These signatures serve two purposes:

1. **Verification** — any node can verify a certificate's authenticity against the known committee without re-executing transactions
2. **Randomness** — at epoch boundaries, `keccak256(aggregated BLS signature)` from the leader certificate provides deterministic randomness for committee shuffling (see [Epoch Boundaries](epoch-boundaries.md))

Certificates can be verified _directly_ (checked against the current committee) or _indirectly_ (verified through a chain of trust from a previously verified certificate).

### The Execution Chain

#### From Consensus Output to EVM Blocks

When a sub-DAG is committed, the consensus layer produces a `ConsensusOutput`. The output is an enriched view of the committed data that includes the actual transaction bytes. The execution engine processes this output to extend the EVM chain.

The flow:

1. **Consensus commits** a sub-DAG, producing a `ConsensusOutput`
2. **Batches are extracted** — each certificate may contain one or more batches of transactions from different workers
3. **One EVM block per batch** — the engine creates and executes one block for each batch, in order
4. **State is updated** — each block's state changes are committed to the EVM state trie

Each batch carries its own metadata:

* **Beneficiary** — the validator address that produced the batch (receives priority fees)
* **Base fee** — the base fee for this epoch
* **Worker ID** — which worker produced the batch
* **Transactions** — serialized transaction bytes

#### When Blocks Are Produced

Not every consensus output results in new EVM blocks. The behavior depends on whether the output contains batches and whether the epoch is closing:

| Condition                     | Result                                                        |
| ----------------------------- | ------------------------------------------------------------- |
| Output contains batches       | One EVM block per batch                                       |
| No batches, epoch not closing | No block produced — execution is skipped entirely             |
| No batches, epoch is closing  | One empty block created to execute epoch-closing system calls |

The "no batches, not closing" case is important for efficiency. Consensus continues to run and commit sub-DAGs even when there are no user transactions. In these cases, the execution chain does not advance, avoiding unnecessary empty blocks. The consensus leader's participation is still tracked for reward purposes. Consistently extending the consensus chain is important to confirm validators are not byzantine and the network is live.

When an epoch is closing but there are no batches, a single empty block is created specifically to trigger the system calls that distribute rewards and rotate the validator committee. See [Epoch Boundaries](epoch-boundaries.md) for details.

#### Block Header Metadata

Each EVM block header carries consensus metadata in repurposed Ethereum header fields. The `parent_beacon_block_root` field stores the `ConsensusHeader` digest, linking each execution block back to the consensus chain. Other fields encode the epoch, round, worker ID, and batch index. See [EVM Compatibility](evm-compatibility.md#block-header-differences) for the full mapping.

### The Epoch Chain

#### EpochRecords

The epoch chain is a sequence of `EpochRecord`s, one per epoch, each linked to its predecessor by a `parent_hash`:

| Field             | Description                                                       |
| ----------------- | ----------------------------------------------------------------- |
| `epoch`           | The epoch number (block height in the epoch chain)                |
| `committee`       | BLS public keys of the active committee for this epoch            |
| `next_committee`  | BLS public keys of the committee for the next epoch               |
| `parent_hash`     | Keccak-256 hash of the previous `EpochRecord`                     |
| `final_state`     | Block number and hash of the last execution block in this epoch   |
| `final_consensus` | Block number and hash of the last `ConsensusHeader` in this epoch |

Each record's digest is computed from all its fields, forming a tamper-evident chain. The `final_state` and `final_consensus` fields serve as signed checkpoints and anchor the epoch chain to both the execution chain and the consensus chain, allowing any node to verify that all three chains agree.

#### EpochCertificates

Epoch records are signed by the outgoing committee at the start of the next epoch. This happens asynchronously, so the network continues operating while signatures are collected in the background.

The signing process:

1. **Voting** — each committee member produces an `EpochVote`: a BLS signature over the record's digest, scoped with an `EpochBoundary` intent to prevent signature reuse across different protocol contexts
2. **Aggregation** — votes are collected and aggregated into an `EpochCertificate` containing a single aggregated BLS signature and a bitmap indicating which committee members signed
3. **Threshold** — a certificate is valid when it contains signatures from a **super-quorum** (2/3 + 1) of the committee

Different nodes may hold certificates with different signer sets. This is expected and correct because all that matters is that each certificate meets the super-quorum threshold. Every node that stores a valid certificate also becomes a source for other syncing nodes.

#### Why the Epoch Chain Exists

The epoch chain solves a bootstrapping problem: a syncing node needs to know which validators form the committee for each epoch in order to verify consensus output, but the on-chain committee state only becomes available after execution catches up.

The epoch chain breaks this circular dependency by providing committee membership and chain checkpoints through a lightweight, independently verifiable structure. A node with only the genesis committee can:

* Verify epoch 0's record using the genesis committee's signatures
* Learn epoch 1's committee from epoch 0's `next_committee` field
* Verify epoch 1's record using that committee
* Continue forward through every epoch, acquiring all committee memberships

This requires no execution state and no trust beyond the genesis committee. One epoch record is generated per day, so the total sync burden for the epoch chain is minimal even after years of operation.

#### Security Properties

The epoch chain derives its security from five reinforcing mechanisms:

1. **BLS aggregate signature verification** — each certificate is verified against the known committee's public keys. Intent-scoped signing prevents signatures from being replayed across different protocol contexts (e.g., a consensus certificate signature cannot be reused as an epoch vote).
2. **Parent hash chaining** — each record commits to the previous record's digest. Records cannot be reordered, omitted, or inserted without breaking the chain. Verification is sequential from genesis, so a valid chain of records implies a valid history.
3. **Committee continuity** — each record's committee is validated against the previous record's `next_committee` field. This ensures that committee transitions follow the on-chain governance decisions made during the epoch. The validation tolerates validator dropouts between epochs (requires at least 2/3 overlap and a minimum of 4 committee members).
4. **Super-quorum threshold (2/3 + 1)** — the standard BFT safety bound. Up to 1/3 of the committee can be Byzantine (malicious or offline) without compromising the validity of epoch records. A valid certificate proves that at least 2/3 + 1 honest validators agreed on the record's contents.
5. **Deterministic record content** — all nodes produce identical epoch records from the same inputs. There is no ambiguity about what an epoch record should contain for a given epoch. A valid super-quorum certificate therefore proves that the committee reached agreement on a single, deterministic record.

#### Attack Surfaces and Mitigations

| Attack                                        | Risk Level           | Mitigation                                                                                                                                                                                                                       |
| --------------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Fake epoch records from peers                 | Mitigated            | Three-part validation at the application layer: parent hash match, committee validity, BLS certificate verification. Invalid records are rejected before storage.                                                                |
| 1/3 Byzantine committee                       | Accepted (BFT bound) | The 2/3 + 1 signature threshold is the standard Byzantine fault tolerance bound. Safety holds as long as fewer than 1/3 of committee members are malicious.                                                                      |
| Long-range fork (replaying old valid records) | Mitigated            | Parent hash chaining and sequential sync from genesis prevent forking. An attacker cannot substitute an alternative history without producing a valid chain from genesis with super-quorum signatures at every epoch.            |
| Eclipse attack during sync                    | Network-level        | If an attacker controls all of a syncing node's peers, they could serve a fabricated chain. Mitigation relies on peer diversity at the network layer and trusted bootstrap peers.                                                |
| Dropped validator between epochs              | Accepted             | A validator may be removed on-chain between when the epoch record is created and when it is signed. Committee validation tolerates this as long as at least 2/3 overlap is maintained between the expected and actual committee. |
| Delayed certificate availability              | By design            | Certificates are collected asynchronously after the epoch closes. If a peer does not yet have a certificate, the syncing node retries. Sync stalls briefly but cannot accept invalid records.                                    |

### Execution-Agnostic Consensus

The consensus layer has no dependency on EVM or any specific execution environment:

* **Raw bytes** — transactions are stored and ordered as opaque byte arrays (`Vec<Vec<u8>>`). Consensus does not parse, validate, or interpret transaction contents.
* **No gas awareness** — consensus orders transactions before gas costs are known. The gas limit penalty mechanism (see [Gas Limit Penalty](gas-limit-penalty.md)) exists specifically because consensus cannot know actual gas usage at ordering time.
* **Cryptographic verification only** — `ConsensusHeader`s can be validated using only BLS signature verification and hash checks, without executing any transactions.

This design means the consensus protocol could support alternative execution environments (e.g., a WASM VM or a ZK execution layer) by changing only the execution engine, not the consensus layer.

### Trustless Node Synchronization

TN can synchronize any node from genesis with a single trust anchor: knowledge of the genesis committee. The three chains — epoch, consensus, and execution — work together to enable this.

#### Node Types

* **Active validators** participate in consensus directly and execute blocks as they are committed
* **Observer nodes** follow the consensus chain by streaming `ConsensusHeader`s from peers without participating in consensus rounds
* **Rejoining validators** stream headers while syncing batches to catch up with the current state before rejoining consensus

#### Step 1: Sync the Epoch Chain

The syncing node downloads and verifies epoch records sequentially, starting from epoch 0:

1. **Request** an epoch record and its certificate from any peer
2. **Verify** three properties:
   * The record's `parent_hash` matches the previous record's digest (chain integrity)
   * The record's committee matches the previous record's `next_committee` (committee continuity)
   * The certificate contains a valid super-quorum BLS signature from the expected committee (authenticity)
3. **Store** the verified record and certificate to support other syncing nodes
4. **Repeat** for the next epoch until no more records are available

For epoch 0, the genesis committee serves as the trust anchor. Each subsequent epoch's committee is learned from the previous record's `next_committee` field, extending the chain of trust forward.

This step produces two critical outputs:

* **Committee membership for every epoch** (required to verify consensus output)
* **Checkpoint hashes** — `final_consensus` and `final_state` from each record provide verified anchor points for the consensus and execution chains

The epoch chain is lightweight and only contains one record per epoch (approximately one per day). Syncing the epoch chain completes quickly even after years of operation. On node restart, sync always begins from epoch 0 to backfill any gaps.

#### Step 2: Sync Consensus Headers

Using the verified checkpoint hashes from epoch records, the node downloads consensus headers:

1. **Obtain a verified hash** — epoch records provide the `final_consensus` hash (the last `ConsensusHeader` digest for each epoch). Current committee members also gossip live consensus output with signatures, providing the latest hash.
2. **Walk backwards** — request the `ConsensusHeader` for the verified hash from a peer. Verify the header's digest matches. Then request its parent header using the `parent_hash` field. Repeat until all missing headers are retrieved.
3. **Verify each header** — every `ConsensusHeader` contains certificates with aggregated BLS signatures that can be verified against the known committee for that epoch (obtained in Step 1).

Consensus header sync can proceed in parallel with epoch record sync. As soon as a new epoch record provides a checkpoint hash, the node can begin downloading that epoch's consensus headers.

Verified hash sources, in order of availability:

| Source                                        | When Available                  | Notes                                                                                                                 |
| --------------------------------------------- | ------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Epoch records (`final_consensus`)             | After epoch record sync         | Historical checkpoint per epoch                                                                                       |
| Live committee gossip                         | Once current committee is known | Provides the latest consensus header hash in real time                                                                |
| Execution blocks (`parent_beacon_block_root`) | After execution sync            | Each block stores the `ConsensusHeader` digest that produced it — useful for fork detection but lags behind consensus |

#### Step 3: Re-execute Batches

Once the node has an unbroken chain of consensus headers, it replays them forward to reconstruct EVM state:

1. **Order headers** — consensus headers were retrieved backwards (newest to oldest) but must be executed forwards (oldest to newest)
2. **Extract batches** — each `ConsensusHeader` contains a `CommittedSubDag` with ordered certificates referencing transaction batches
3. **Execute blocks** — one EVM block is created and executed per batch, applying each transaction to the state trie
4. **Verify state** — epoch records provide `final_state` checkpoints (block number and hash) that the node can verify against its locally computed execution state at each epoch boundary

This step transforms the cryptographically verified ordering (consensus chain) into the actual account state, balances, and contract storage (execution chain).

#### Recovery After Restart

When a node restarts mid-epoch, it must rebuild its in-memory state:

1. Walk already-executed blocks for the current epoch to restore gas statistics and leader counts
2. Replay any consensus output that was committed but not yet executed before shutdown
3. Resume live consensus participation

This ensures no consensus rounds are skipped or double-counted.

### Summary

| Property     | Consensus Chain                   | Execution Chain                         | Epoch Chain                            |
| ------------ | --------------------------------- | --------------------------------------- | -------------------------------------- |
| Unit         | `ConsensusHeader`                 | EVM Block                               | `EpochRecord`                          |
| Linking      | `parent_hash` → digest chain      | `parent_hash` → block hash chain        | `parent_hash` → record digest chain    |
| Content      | Certificates, batches (raw bytes) | Transactions, receipts, state changes   | Committee keys, state checkpoints      |
| Verification | BLS signatures + hashes           | State root + receipt root               | BLS super-quorum (2/3 + 1) certificate |
| Advancement  | Every committed sub-DAG           | Only when batches exist or epoch closes | Once per epoch                         |
| Purpose      | Transaction ordering              | State execution                         | Trustless committee sync               |
