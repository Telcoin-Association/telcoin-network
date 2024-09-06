# Worker

## Execution Layer
The worker needs the EL for proposing its own batches, verifying peer batches. 

The EL needs the worker if transactions are missing from a batch (when building the primary's header for the round).
EL only receives `Vec<BatchDigest>` from the Primary and pulls all transactions from the `SealedPool` that match. It then takes the digest of the batch itself to ensure it matches.

If it doesn't match, then the EL requests a worker to retrieve the batch from storage.
