# TN-Reth

The compatibility crate for using `reth` as an execution library for Telcoin Network consensus.

## EVM

Telcoin Network uses EVM for executing transactions.
The consensus layer is agnostic to the execution environment.
System calls are used to apply incentives at the end of every epoch before calling `concludeEpoch` on the ConsensusRegistry.

The current fork is `Shanghai`.

## Overview

Output from consensus (subdag with batches) is used to execute several blocks in the EVM.
Each batch in the subdag is executed as a different block to support parallel basefees (once multiple workers are live).
The basefee changes with every epoch.

Workers have independent transaction pools, so transactions are only gossipped after consensus.
The engine handles errors for invalid transactions gracefully.

## Execution

TN repurposes some of the header fields for protocol information.

The following header fields are different from Ethereum protocol:

#### mix_hash

Calculated in `tn-engine/src/payload_builder.rs`.
If the output from consensus contains batches, the mix hash is the result of `output_digest ^ batch_digest`.
If the output contains no batches of transactions, then it is just the `output_digest`.

#### nonce

The epoch is the first 32 bits.
The consensus round is the last 32 bits.

```rust
((self.epoch as u64) << 32) | self.round as u64
```

#### basefee

Basefees are set for the epoch and validated at the worker batch level.
The basefees are not adjusted or validated during execution.
This allows for parallel basefees per worker index.

#### extra_data

Execution happens independently for each node.
The results are verified in future rounds of consensus.
As a result, the extra_data field is reserved for protocol use only.

The protocol currently uses this field to indicate an epoch boundary by including the `keccak256` of the BLS signature for the leader certificate.
The default "empty" bytes is used otherwise.

#### difficulty

The worker id and the batch index (relative to the subdag).

#### parent_beacon_block_root

The digest of the `ConsensusHeader` that committed the transactions being executed.


See `evm/block.rs` for block execution details.
