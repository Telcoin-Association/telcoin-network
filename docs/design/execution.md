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

*TN*: The ordered list of batch hashes (flattened) in the ConsensusOutput.

*Logic*: Batches are proposed in parallel. This could be used to guarantee a complete ConsensusOutput included all batches. Batch validation ensures parent hash matches. TN can include this data without any cascading consequences using the current default `EthBeaconConsensus` logic from reth because batches don't have ommers, only final execution blocks.

##### Ommers Hash
*Ethereum*: Considered a "constant value" (EMPTY_OMMER_ROOT_HASH) post-merge.

*TN*: Hash of ommers (ordered list of batch hashes in ConsensusOutput) - ensures correct ordering.

*Logic*: Unused hash value that is unlikely to be re-assigned in Ethereum.

##### Nonce
*Ethereum*: Considered a "constant value" (0x0).

*TN*: The sub dag index's `SequenceNumber` (also u64). The "nonce" for consensus.

*Logic*: Is this redundant? Could help with block explorers. Could help with ensuring all batches in ConsensusOutput are executed. What happens during recovery? Need to ensure all batches were executed correctly. Block number cannot apply to batches or rounds since they are executed in multiples.

##### Difficulty
*Ethereum*: No longer used post-merge and must be 0.

*TN*: Batch index? This could be used for documenting validator rewards? Leave it alone and consider using it for `randao` value CL needs this value from EL?

*Logic*: [EIP-4399](https://eips.ethereum.org/EIPS/eip-4399) in favor of supplanting DIFFICULTY opcode with PREVRANDAO. If so, TN could use this for randao values.

##### Mixed Hash
*Ethereum*: A 256-bit hash which, combined with the nonce, proves that a sufficient amount of computation has been carried out on this block (formally Hm). Post-merge, execution clients (reth and geth) use it to hold the prev randao value used to select the next quorum of validators by beacon chain. The block has `mixed_hash` and the beacon block has `prev_randao`. These values are converted when the execution payload is constructed from the built block and used by Beacon chain for validator shuffling.

*TN*:

*Logic*:  On-chain programs might rely on this value for randomness, and it must be consistent when the batch is made and the final block is executed. It's also important that the random value is verifiable yet unpredictable.

##### Extra Data
*Ethereum*: Anything a validator wants to use - 32 bytes.

*TN*: Batch Hash. The index of the batch? Better to use `difficulty` because U256 type? Also consider: The priority fee rewards for validator? The hash of the batch that was used to execute this block? This could also be used for the `ConsensusOutput` hash? Can't use ms timestamp because execution times could vary between validators. Is it valuable to include the leader's round?

*Logic*: It's unclear exactly what data is most useful for indexing blocks down the road. Extra data needs to be consistent amongst all validators to ensure correct hash of executed block, but using extra data now prevents TN from ever using it again. While other "unused" block fields are in place, prefer to use these instead.

##### Parent beacon block root
*Ethereum*: the hash of the parent beacon block's root to support minimal trust for accessing consensus state.

*TN*: `ConsensusOutput` hash. Or should this be used for managing rewards/validator sets on chain?

*Logic*: The parent beacon block root is mostly helpful for supporting staking pools like Lido and isn't really relevant to TN directly. However, it could be useful for calculating epoch shuffles. IMPORTANT: ensure implementation wouldn't violate EVM expectations.

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
