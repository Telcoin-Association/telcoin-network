# Execution Layer

- verifies incoming transaction requests
    - organizes them into batches for consensus
- verifies information received by the consensus layer
- updates the canonical chain based on consensus output

Different than ethereum:
- transactions are not gossiped

## Extending the Canonical Tip
- CL reaches consensus
- Send `ConsensusOutput` to the EL Executor
- Executor executes each batch in order of ConsensusOutput
    - ignores failures
        - ie) double-spend, etc.

### Block Data
Ethereum block data is not necessarily equivalent for TN. The fields must be kept in order to maintain EVM compatability and to maintain compatibility with upstream reth changes.

Because blocks contain these fields, they are accessible to be re-purposed for TN consensus. These are some ideas of what data to include in the final SealedBlock.

[EIP-3675](https://eips.ethereum.org/EIPS/eip-3675) specifies block structure after the merge. Updating this values will require custom implementations of `Consensus` trait.

##### Ommers
*Ethereum*: A collection of uncle headers. Considered a "constant value" (empty vec) in PoS.

*TN*: The ordered list of all headers that reached consensus in the current round. Worker's propose blocks that validators reach consensus on, and this value contains all headers for the round.

*Logic*: Batches are proposed in parallel. This is used to guarantee a complete ConsensusOutput included all batches. Peers ensure the worker's proposed block is based off a canonical header. Proposed blocks do not contain ommers. Ommers is only included after consensus is reached.

##### Ommers Hash
*Ethereum*: Considered a "constant value" (EMPTY_OMMER_ROOT_HASH) post-merge.

*TN*: Hash of ommers (ordered list of batch hashes in ConsensusOutput) - ensures correct ordering.

*Logic*: Unused hash value that is unlikely to be re-assigned in Ethereum.

##### Nonce
*Ethereum*: Considered a "constant value" (0x0).

*TN*: The sub dag index's `SequenceNumber` (also u64). The "nonce" or "round" for consensus.

*Logic*: This value is used to track the round of consensus.

##### Difficulty
*Ethereum*: No longer used post-merge and must be 0.

*TN*: Consensus output contains multiple blocks in order. The index of the block is tracked in the block's difficulty. Difficulty is already set as an unsigned integer, so this value is used in favor of other options (32 fixed byte hash).

*Logic*: This value is used to verify correct execution order of blocks in consensus output.

##### Mixed Hash
*Ethereum*: A 256-bit hash which, combined with the nonce, proves that a sufficient amount of computation has been carried out on this block (formally Hm). Post-merge, execution clients (reth and geth) use it to hold the prev randao value used to select the next quorum of validators by beacon chain. The block has `mixed_hash` and the beacon block has `prev_randao`. These values are converted when the execution payload is constructed from the built block and used by Beacon chain for validator shuffling.

*TN*: WIP. Validators are shuffled with a separate mechanism, however sufficient randomness is passed to the execution layer through this value. The mix hash is used as a source of randomness on-chain.

Simply using the digest from consensus output is an insufficient source of randomness because batches can be built off historic parents. An attacker could theoretically know the next digest and anticipate the mix hash in the upcoming batch. The ability to predict upcoming mix hashes would undermine the security of on-chain programs that rely on PREVRANDAO as a source of randomness in the EVM.

Instead, the digest of consensus output should be mixed with another value that the worker knows at the time of block construction (ie - number of transactions in the batch? timestamp? some value that can be used to reproduce the mix hash with extreme difficulty in predicting).

Using values from the worker's own block requires trust in the node operator. Malicious operators could intentionally withhold transactions or force block production at a convenient time to ensure favorable execution environment. Using values such as timestamp or number of transactions provides an opportunity for manipulating the mix hash value.

Is there a value peers can use? Might need to be signature-based. What is a value that node operators cannot manipulate without peers witnessing?

The mix hash for a worker's block must be random, providing security for smart contracts relying on it. It also must be verifiable and impossible to manipulate. After consensus, the mix hash value is reused to ensure consistent execution results. During re-execution for finality, the mix hash can be known because no other transactions are possibly included. The only possibility is for them to be removed.

*Logic*:  On-chain programs might rely on this value for randomness, and it must be consistent when the batch is made and the final block is executed. It's also important that the random value is verifiable yet unpredictable and difficult to manipulate. Further consideration is needed to guarantee this value.

##### Extra Data
*Ethereum*: Anything a validator wants to use - 32 bytes.

*TN*: The digest of the worker's proposed block batch. This value is used to validate correct execution results.

*Logic*: It's unclear exactly what data is most useful for indexing blocks down the road. Extra data needs to be consistent amongst all validators to ensure correct hash of executed block, but using extra data now prevents TN from ever using it again.

##### Parent beacon block root
*Ethereum*: the hash of the parent beacon block's root to support minimal trust for accessing consensus state.

*TN*: The `ConsensusOutput` hash. The hash is constructed from the output's sub-dag digest, the beneficiary, and the worker's proposed block (batch) digests.

*Logic*: The parent beacon block root is mostly helpful for supporting staking pools like Lido and isn't really relevant to TN directly. However, it could be useful for calculating epoch shuffles.

## System Calls
At times, the protocol needs to manage state directly. System calls provide a secure, gaselss approach to previewing state changes. The protocol can then use this "preview" of the state change to update the database directly, which avoids the need for gas entirely.

## Telcoin Network: EIP Compatiblity
### [EIP-2935](https://eips.ethereum.org/EIPS/eip-2935): Historic Block Hashes
Support for stateless clients.

#### Abstract
> Store last `HISTORY_SERVE_WINDOW` historical block hashes in the storage of a system contract as part of the block processing logic. Furthermore this EIP has no impact on `BLOCKHASH` resolution mechanism (and hence its range/costs etc).

#### Motivation
> EVM implicitly assumes the client has the recent block (hashes) at hand. This assumption is not future-proof given the prospect of stateless clients. Including the block hashes in the state will allow bundling these hashes in the witness provided to a stateless client. This is already possible in the MPT and will become more efficient post-Verkle.
>
> Extending the range of blocks which BLOCKHASH can serve (BLOCKHASH_SERVE_WINDOW) would have been a semantics change. Using extending that via this contract storage would allow a soft-transition. Rollups can benefit from the longer history window through directly querying this contract.
>
> A side benefit of this approach could be that it allows building/validating proofs related to last HISTORY_SERVE_WINDOW ancestors directly against the current state.

### [EIP-4844](https://eips.ethereum.org/EIPS/eip-4844): Shard Blob Transactions
TODO

Add support for Layer 2s. TN supports layer 2 transaction data type. This is not functional yet because some design decisions need to be carefully considered regarding blob sidecars. Ethereum tasks the consensus layer for persisting the blobs for data availability.

#### Abstract
> Introduce a new transaction format for “blob-carrying transactions” which contain a large amount of data that cannot be accessed by EVM execution, but whose commitment can be accessed. The format is intended to be fully compatible with the format that will be used in full sharding.

#### Motivation
Rollups and Layer 2s are increasingly popular and help to further reduce transaction fees while increasing transaction throughput.

### [EIP-4788](https://eips.ethereum.org/EIPS/eip-4788): Beacon Root Contract
TODO

How can this be used to verify consensus output?

#### Abstract
> Commit to the hash tree root of each beacon chain block in the corresponding execution payload header.
>
> Store each of these roots in a smart contract.

#### Motivation
> Roots of the beacon chain blocks are cryptographic accumulators that allow proofs of arbitrary consensus state. Exposing these roots inside the EVM allows for trust-minimized access to the consensus layer. This functionality supports a wide variety of use cases that improve trust assumptions of staking pools, restaking constructions, smart contract bridges, MEV mitigations and more.

### [EIP-7685](https://eips.ethereum.org/EIPS/eip-7685): Execution to Consensus Layer Requests
TN does not anticipate a need for supporting this EIP at this time.

##### Motivation
> The proliferation of smart contract controlled validators has caused there to be a demand for additional EL triggered behaviors. By allowing these systems to delegate administrative operations to their governing smart contracts, they can avoid intermediaries needing to step in and ensure certain operations occur. This creates a safer system for end users.

TN validators may decide to manage themselves on chain through smart contracts, at which point a TIP will be introduced to support this EIP.

### [EIP-2930](https://eips.ethereum.org/EIPS/eip-2930): Access List for transactions
> Adds a transaction type which contains an access list, a list of addresses and storage keys that the transaction plans to access. Accesses outside the list are possible, but become more expensive.

TN workers could execute blocks they propose to create this list per-block which could better support optimistic parallel execution in the engine.

TN could also consider custom wallets that support this.
