# Config

The library that contains configuration information for a Telcoin Network node.

## Purpose & Scope

The config crate contains various configuration parameters that allow a node to participate with the protocol.
It contains sensitive information and manages private keys.

## Key Components

### ConsensusConfig

The configuration for consensus layer.
The `Committee` and `WorkerCache` contains all information for committee participates that vote to extend the canonical tip.
The committee is comprised of staked validators and information to verify their communication.

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
