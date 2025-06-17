# Batch Validator

## Purpose & Scope

The batch validator is responsible for validating batches of transactions that extend the current canonical tip.
All batches must be validated by a quorum of validators in the committee to be included in a `Certificate` for execution.

## Key Components

### BatchValidator

- **Core Structure**: Contains several methods with compartmentalized logic for validating a batch from a peer
- **Validating Logic**: The batch is validated within the context of itself
- **Lifecycle Management**: Automatically shuts down at epoch boundaries to maintain protocol synchronization

### Batch Validation Process

Workers received sealed batches from their peers and use the `BatchValidator` to ensure the batch is valid.
If a batch is valid, the worker acknowledges the peer's batch.
With a quorum of acknowledgements, the worker that proposed the batch forwards the batch digest to its Primary to be included in the next Primary `Header`.

#### Missed Batches

The byzantine nature of the protocol allows batch validation at the Primary level as well.
Primaries assess all batches have been validated by their workers when validating a peer's Primary `Header`.
If a batch has not been locally validated, the worker requests the batch from the worker that originally proposed the batch.
The batch is then validated using the `BatchValidator` so the primary can finish validating the `Header` that contained the missing batch digest.

#### Invalid Batches

Workers return an error to the requesting worker if it deems a batch invalid.

#### Non-committee Node

The `BatchValidator` has a method called `submit_txn_if_mine` to forward transactions to a committee node if this node is not in the current committee.
Only committee validators are able to build and propose batches.
This method is designed to support observer nodes and validator nodes that still contain transactions but aren't in the committee.

## Security Considerations

### Threat Models

#### Gas Usage

Transactions are not fully executed until after consensus.
Therefore, there is no way to actually know how much gas a transaction will cost at the batch validation stage.
Instead, the transaction's gas limit is used to quantify batch size by gas.

#### Invalid Transactions

Transactions are suppose to extend the canonical tip.
The `BatchValidator` attempts to retrieve the batch's canonical header from the database, but uses its own finalized header as a fallback.
Transactions may revert at execution, but this is considered a non-fatal error handled by `tn-engine`.

### Trust Assumptions

- Relies on execution engine to handle invalid transactions post-consensus
- Trusts basefee calculations are correctly applied at epoch boundaries
  - The batch validator also expires with the epoch

### Critical Invariants

- Batches are intentionally designed to be agnostic (transactions are `Vec<Vec<u8>>`) so the network can support different execution environments in future versions
- The engine gracefully ignores invalid transactions
- Basefees only adjust at the start of a new epoch
- Batch validators are only valid within a single epoch

## Dependencies & Interfaces

### Dependencies

- **Worker**: Source of batches to validate
- **Canonical Chain State**: Current canonical tip updates from `tn-engine`
- **Epoch Management**: Basefee and epoch information

### Interfaces

- **Owned by Worker**: Each worker owns an instance of a `BatchValidator`. Workers only support one execution environment and one basefee.
