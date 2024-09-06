# Batch
The consensus layer uses "batches" to shard the block verification process.

Batches are managed by the consensus layer's `Worker` instances. Workers share batches with their peers to receive votes and ultimately reach a quorum. Once a quorum is reached (2f + 1), the worker passes the batch digest (hash of the vector of transactions) to the primary to be included in the round's proposed block. Batches from peers that the worker validates are also included in the primary's proposed block.

- [validation](./validation)
