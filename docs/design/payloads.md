# Payload Design
Payloads represent the information exchanged from the Execution Layer (EL) to the Consensus Layer (CL).

Payloads are responses to the CL issuing a command for the EL to execute.

## Batches
Workers submit requests to the EL `BatchBuilderService` using a handle. The service takes the best pending transactions and constructs a payload for the CL to broadcast as a `Batch`.

The EL stores batches in a `SealedPool` for constructing `Headers` for the Primary when it proposes a new block for each round.

### Peer
Peer batches go to the EL engine for validation.

## Headers (Blocks in EL)
Headers are proposed by a Primary once per round. The moment enough `Certificates` from peers reach 2f+1, the Primary proposes a `Header` for the round. The `Header` payload only includes `Vec<BatchDigest>`. Each `Header` also includes specific information from the proposing Primary's EL that was generated during execution using one-way hashing functions, so peers can validate the state of execution.

### Self
The Primary requests a `HeaderPayload` from the EL by sending a list of `BatchDigest`. The EL pulls these transactions from the `SealedPool`, creates a `Batch`, and checks the digest to ensure all transactions are present.

If the digest doesn't match, then the EL requests the `Batch` from the Worker.

Otherwise, it proceeds to execute another EL "Block" with the transactions.

The result of executing every batch in the digest is returned to the Primary. The Primary includes information generated from executing the transactions so other Primaries can validate their peer's EL.

### Peer
The Primary sends peer `Header` to the EL engine for validation. Akin to `new_payload`.

## Canonical Blocks
CL issues `ConsensusOutput` every two rounds. The Execution State is shared with the EL `LatticeEngine` which executes the block and adds it to the `BlockchainTree` (BT).

The EL payload builder should construct the block for the engine. The engine then updates the blockchain tree.
- What about inbetween consensus output rounds?
    - would this be useful to construct between rounds of finality?
