# Batch Builder

## Purpose & Scope

The batch builder is responsible for selecting optimal transactions from the node's transaction pool and assembling them into batches that extend the current canonical tip.
It operates as a Future-based task that coordinates with the consensus layer to ensure batches are only mined after successfully reaching quorum of support from other workers.

## Key Components

### BatchBuilder

- **Core Structure**: Implements the `Future` trait for asynchronous batch assembly
- **Mining Logic**: Selects transactions from the local transaction pool based on current basefee and epoch parameters
- **Lifecycle Management**: Automatically shuts down at epoch boundaries to maintain protocol synchronization

### Batch Assembly Process

Validator nodes maintain unique transaction pools.
Transactions are not gossiped, but are distributed as sealed batches of transactions for other validators to validate.

#### Transaction Selection

Each worker selects the best transactions from their respective transaction pools to include in the next proposed batch.
Workers only propose one batch at a time, and transactions are only removed from the pool once the batch that contains them reaches quorum (2f+1).

Transactions are sorted by default based on the highest fees, although this is not a strict requirement of the protocol.

The only requirement is that transactions must extend the current canonical tip.
The canonical tip is extended once a batch is settled in a DAG commit.
The entire collection of batches is then sent to the `tn-engine` for final execution.

#### Validation

The logic for validating batches is in the `batch-validator` library.
If a node includes a batch that was not validated by the worker's peers, the Primary's `Header` will fail validation.

## Security Considerations

### Threat Models

#### MEV

Transaction pools are isolated to individual nodes, which prevents anonymous MEV attacks.
Validator nodes must obtain an NFT through decentralized governance and are "well-known".

#### Invalid Transactions

Transactions are not gossiped until they are sealed in batches.
Therefor, it's possible for different nodes to include duplicate transactions in their batches or transactions that attempt to double-spend.
Once the batches reach consensus, they are ordered deterministically by the Primary using `Bullshark` and executed by `tn-engine`.
The invalid transactions will fail when the engine executes them.
This is a non-fatal error.
Although this is inefficient, it is considered an acceptable limitation of the protocol at this time.
Future iterations are planned to address this inefficiency.

### Trust Assumptions

- Assumes the Worker will continue processing a single batch until it reaches quorum
  - This is verified when validating Primary Headers
- Relies on execution engine to handle invalid transactions post-consensus
- Trusts basefee calculations are correctly applied at epoch boundaries

### Critical Invariants

- Transactions are only mined from the transaction pool if the batch reaches quorum
- Transaction pool state remains consistent with canonical chain execution AND batch execution
  - Transactions in a batch must always extend the canonical tip, not the preceeding batch
- Basefees only adjust at the start of a new epoch
- Epoch boundary synchronization is maintained across all batch builders (currently TN only supports 1 per node)

## Dependencies & Interfaces

### Dependencies

- **Transaction Pool**: Source of candidate transactions for batch assembly (see tn-reth library)
- **Canonical Chain State**: Current canonical tip updates from `tn-engine`
- **Epoch Management**: Basefee and epoch boundary information

### Interfaces

- **Outbound to Worker**: Sealed batches (batch with hashed digest) are sent to the worker for consensus processing
- **Inbound from Consensus**: Quorum achievement signals that trigger mining operations
- **Validation**: Peers validate batches from other peers (with matching `WorkerId`s) using logic in the `batch-validator` lib
