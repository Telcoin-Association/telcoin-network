# Building batches

Batch is also sometimes refered to as "payload" in the Execution Layer.

What view of state should the transaction pool consider when creating/validating batches?
- latest
- canonical
- specify?

Genesis, then worker gets it's first batch.

Batch txs must be valid in context of genesis block (aka "latest")

1. Batch reaches quorum
1. Pool moves "quorum" pool to "finalized" pool
    - calculates block roots
1. Primary requests block after enough certs from previous round received
    - all certs for round 1 point to genesis state
    - how are certs validated?
        - receive blocks from peers, send to EL, vote
        - enough votes, the primary follows up with a cert
        - certificate contains header, but primary may not have header yet
            - EL should have header as a fork in the tree
            - primary can also receive certs for blocks it hasn't seen !!!
    - consensus output happy path will include all txs for all certs

Primary's proposed block is what in the tree?
- primary should propose batches based on it's proposed block?



I think for now it's best to build batches off the canonical chain. There is definitely an opportunity for optimization, but for now this is really complicated and I need to think through it more.

One consequence of this is moving pending/quorum transactions.


When should transactions be "pruned" from the pending pool?

Building batches on the canonical chain downsides:
- duplicate transactions could be incldued in a batch
    - inefficient, but will be caught on next canonical output
- canonical state change between the time batch is created and when it is broadcast
    - batch could be invalid
    - this is just as likely as if building off of a proposed block's state as well
- the only real downside I can think of by building off the canonical state is slower tps
    - opportunity for optimization down the road


EL Engine should only be concerned with blocks and consensus output

Batches are handled by the batch generator.

Once a transaction is sealed in the batch, the CL guarantees that the transaction will be retried if block/batch fails.
Happy path is update tx pool as if canonical state change with batch, but don't adjust basefee.
- promote txs in queued pool to pending if nonce gap covered in `sealed_pool`

TxPool:
========
Batches are executed on a block configured with the last canonical block.
- tx that is ready for execution is in pending tx pool
- batch creates block environment based on canonical tip
- tx "promotions" should only occur on canonical state change
- moving tx from pending to sealed pool should not unlock any transactions
    - if the batch or block fails, the tx nonce is still an issue
    - pending pool should have other txs for next sealed batch, but only unlock nonce with canonical update
    - can still discard transactions though


# Batch Building Process
The worker's `batch_maker` starts a timer before requesting a new batch. After the timer reaches zero, the worker sends a command through the `BatchBuilderHandle` for `NewPayload`. This command triggers the service to create a new job that builds a batch based on the canonical tip (except for block 1 - post genesis).

- Right now, the worker uses an mpsc channel (through the builder handle) to receive the next batch
- EL pull transactions for the `pending pool` and executes the `next` best transaction using the canonical state
    - transactions that pass are included in the batch
- The batch is sent to the consensus layer one of three ways:
    - The pending transaction pool is empty (default assumption)
    - The maximum allowed gas per batch is reached
    - The maximum batch size is reached (measured in bytes)
- The worker's `batch_maker` seals the block and passes it to the `quorum_waiter` for broadcasting
    - sealing the block includes storing it in the database
- Once the block is sealed, the `batch_maker` sends a new command to the `BatchBuilderHandle`
    - Command to service triggers the generator to update the transaction pool
        - Pending transactions are "promoted" based on `TransactionId` vector in the `BatchPayload`
            - Transactions are moved to the `SealedPool`
    - This can fail, but probably should not
        - worse case scenario is updating the pending pool fails and the next batch includes duplicate transactions from the previous batch

# Known TODOs
- Potentially propose invalid batches during canonical state change
    - The batches are only proposed when the worker's timer goes off
        - what happens if the canonical state changes immediately after a batch is built by the EL?
            - state changes before broadcast / during quorum
