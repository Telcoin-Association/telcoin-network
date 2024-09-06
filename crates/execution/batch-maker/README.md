# Batch Maker
RPC and transaction pool implementation that validates transactions and executes blocks for worker batches.

Loosely based off of auto-mine consensus in reth library.


## Goal
Create a library for replacing the worker's `transaction_server.rs` and `client.rs` using reth's auto-seal approach.

Mined transactions are removed from the pool once the batch is stored by the worker.
