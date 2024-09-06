# Batch Maker
Batch validator that executes a batch and compares it to the peer's `SealedHeader` that's included in the batch's metadata.

## Goal
Task that receives a batch and:
- executes a block from batch's transactions
- compares the execution result to the batch's `SealedHeader`
