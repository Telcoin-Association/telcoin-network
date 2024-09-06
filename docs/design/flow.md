# Flow of Consensus
The flow a transaction through the Telcoin Network

### Happy path
1. User submits a transaction to a validator's RPC endpoint.
    - Transaction is verified
        - nonce
        - account balance
        - timestamp
        - signature
1. RPC adds valid transaction to pending tx pool
1. Worker requests next batch and broadcasts the batch to peers.
    - pending tx pool delivers the best collection of transactions it has at that time
        - all transactions are valid based on the current state of the canonical chain
1. Worker receives a batch
    - Passed to the execution layer for validation
        - execution layer executes transactions based on current canonical chain state
            - only execute transaction
            - verifies hash of vec of transactions, no other hashes (ie parent, logs, etc.)
        - each batch is validated in isolation from other batches
            - only concerned that a batch is valid in the context of adding transactions on top of current canonical chain
            - ie) duplicate transactions in different batches would still be verified
                - this is an outstanding issue, but also maybe not a big deal since eth gossips transactions anyway
    - akin to `new payload` in beacon consensus, but only concerned with transaction execution
        - instead of adding a block, all valid transactions could be added to a special pool
            - but still need a way to verify all transactions are present when the CL produces its block
            - inefficient, but maybe don't store txs for now
                - only verify transactions are valid based on current state
        - maybe produce a block and return a payload id. when primary requests block from EL, it includes payload ids so the EL can retrieve them from memory quickly
            - what to do if crash recovery?
1. Worker receives validation from the EL
    - return vote to peer
    - store batch for retrieval if a cert is later issued with that batch
1. Worker may or may not receive enough votes to reach quorum for it's proposed batch
    - once quorum is reached, it's batch will be included in the primary's block
    - this can theoretically happen multiple times before a block is produced
        - up to the point where the primary reaches it's maximum number of batches in a block
1. The primary receives enough certificates from the previous round that it proposes the next block for this round
    - EL builds lattice block containing all transactions from all batches in primary's header
        - EL calculates hashes and builds legit block
            - CL orders batches and transactions, so EL should mark transactions as invalid across validators
            - this ensures deterministic block hashes
        - block is added to BT as a fork
1. Validators receive peer's proposed block and verify they have already seen all batches within the header
    - verify batches in store or request missing batches
    - send to EL to execute block and verify hashes, state root, etc.
        - added as fork in BT
1. Eventually enough certificates are collected from the round to advance
1. New leader selected on even rounds
    - results in finality
    - update canonical fork
    - TODO: better to leave execution blocks as certificates
        - ordered DAG says block order?
        - or, combine all transactions into one
            - how to verify execution results?
            - parents of certs?

### Happy Path (old)
1. User submits a transaction to a validator's RPC endpoint.
    - Transaction is verified
        - nonce
        - account balance
        - timestamp
        - signature
1. RPC adds valid transaction to pending tx pool
1. Worker requests next batch and broadcasts the batch to peers.
    - pending tx pool delivers the best collection of transactions it has at that time
        - all transactions are valid based on the current state of the canonical chain
    - batch includes payload information, such as parent block hash
1. Worker receives a batch
    - Passed to the execution layer for validation
        - execution layer looks at batch information to make sure the parent hash matches the tip of it's current canoncial chain
        - calculates hashes to ensure batch is valid
        - each batch is validated in isolation of other batches
            - only concerned that a batch is valid in the context of adding transactions on top of current canonical chain
            - ie) duplicate transactions in different batches would still be verified
                - this is an outstanding issue
    - akin to `new payload` in beacon consensus
        - block is added to tree, but this isn't necessary
        - there seems to be an opportunity for optimization here, but I haven't figured it out
            - like if batches can form trees and that influences the certificate?
1. Worker receives validation from the EL and votes to include this peer's batch in their primary's next "block"
1. Worker may or may not receive enough votes to reach quorum for it's proposed batch
    - once quorum is reached, it's batch will also be included in the primary's block
    - this can happen multiple times before a block is produced
        - this may be an issue
            - logic for verification checks parent block, but what if batch is broadcast right before canonical chain update
1. The primary receives enough certificates from the previous round that it proposes the next block for this round
    - the block includes all peer batches that were verified
    - the block includes all own batches that reached quorum
    - does the EL need to build a block for the certificate?
        - I think so to ensure execution logic is valid
    - for now, assume no?
1. Peer validators verify they have already seen all batches within block header's payload
    - if all batches previously verified, vote in favor
    - request missing batches
1. Validator's block reaches quorum and a certificate is issued
    - eventually enough certificates are received that the next block is proposed

### Batches to Certificates: Producing a Block
Worker receives batch from EL.
    - worker requests new payload
    - pending pool is drained
    - worker reaches quorum and sends entire batch and batch digest to txpool
    - tx pool adds all transactions to `FinalizedPool`
        - `FinalizedTransaction` needs a batch digest associated with it

Worker receives batch from peer.
    - sends batch and batch digest to EL
    - if valid, worker stores data in case certificate is issued with this batch
        - NOTE: a peer's valid batch is not included in this primary's proposed block.
            - only batches from this primary's workers are included in the proposed block.

Primary has enough certificates from round to advance and propose block
    - create header with batch digests locally
    - send to EL to create the full block
- How does the EL find batches by batch digest?
    - EL originally made the batches
        - batch digest = payload id
            - worker requests next batch & responds with batch digest
            - pending pool's payload is stored
    - memory
        - all batches in header must have been validated by EL
            - what if crash recovery?
            - should batches be stored on execution side?
            - what if batch is missing?
    - storage
        - worker has this information stored
            - extra request seems inefficient
        - store on EL side?
            - now storing twice


# Pools
- worker requests best transactions from pending pool
- tx pool needs to prune those transactions so the pools shuffle
    - similar to on_canonical_state_change()
        - `on_batch_quorum_reached()`
            - prune transactions
- need another pool so if the batch fails, so the transactions aren't lost
