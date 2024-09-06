# Validation

Workers receive batches from their peers and validate all the transactions.

The transactions in a batch are RLP encoded `TransactionSigned` structs.

## Execution Layer
The execution layer is responsible for validating all transactions within a batch.

### Worker's Own Batch
The batch a worker proposes to peers (aka "own batch") is assembled by a payload builder in the execution layer. Transaction signatures are first verified by the RPC at the time of submission. If the transaction signature is valid, the transaction is sent to the pool's transaction validator (`EthTransactionValidatorInner`) for further validation.

The pool's transaction validator asserts:
- the transaction's size is below the max size allowed
    - prevent DoS attacks
- the transaction is below the max init code size
- the transaction's gas limit is below the block's max gas limit
- the max priority fee per gas is greater than the max fee per gas (if any)
- non-local transactions meet the minimum priority fee asserted by the node
- the chain id matches
- the transactions is signed by an EOA
- the transaction nonce is not less than the account's nonce
- the account balance is sufficient to pay for the transaction

If all of these assertions complete, the transaction is considered valid and sent to the transaction pool. Only transactions that are ready to be executed in the current state of the blockchain are sorted into a "pending transaction pool". Transactions that don't have enough gas or belong to an account with nonce gaps (missing transaction ancestor) must wait for state to change in order to become eligible for the next batch.

Workers create batches from the best group of transactions in the "pending" pool. The pending pool is constantly updated to ensure maximum gas is included in the next batch. If state changes, the pool updates.

The worker then sends the batch to its peers for validation. Once enough peers validate the worker's batch (2f + 1), quorum is reached. The batch digest is shared with the primary to be included in the next proposed block.

### Worker's Peer Batch
Workers receive batches from peers and validate the transactions by sending the vector of bytes to the execution layer. The EL recovers each transaction's signer and sends the transaction to the `BatchTransactionValiator`.

TODO: the above validator is actually just for adding transactions to the pool, not validating a batch within the greater context of the blockchain.

The batch transaction validator asserts:
- the transaction's size is below the max size allowed
    - prevent DoS attacks
- the transaction is below the max init code size
- the transaction's gas limit is below the block's max gas limit
- the max priority fee per gas is greater than the max fee per gas (if any)
- the chain id matches
- the transactions is signed by an EOA
- the transaction nonce is not less than the account's nonce
- the account balance is sufficient to pay for the transaction

difference between "own batch":
- removed: non-local transactions meet the minimum priority fee asserted by the node

All transactions in the batch must be valid for the batch to be valid. If a peer's batch is considered valid, the worker responds to the proposing peer with a vote of approval. The worker then stores the batch information.

#### Data required
Validating peer batches also involves executing the batch to ensure values were calculated correctly.

The goal for including EL data with batches is to keep only the amount needed to verify successful execution of the transactions. Other roots and values are checked when the primary proposes it's header.

It's important batches are quickly and efficiently validated because they distribute/share transactions to peers.

##### Parent Hash
The hash of the block from the last round or consensus output?

The parent hash is used to create the appropriate state to execute the batch against.

##### Gas Used
The amount of gas the batch is expected to take.

##### Base Fee Per Gas
The base fee per gas needs to match.

##### Receipts Root
The receipts generated during contract interaction.

##### Logs Bloom
The condensed bloom for logs.

##### Transactions Root
The root for transactions within the batch.

##### Values not need
Not needed because they're included in the primary's proposed header and don't affect if a group of transactions are necessarily valid:
- fee recipient
- state root
- prev randao
- gas limit // set by the protocol?
- block number
- extra data
- withdrawals



Engine steps:
1. The batch is passed to the engine handle as a `BatchExecutionPayload` with the command `validate_batch`.
2. Attempt to cast the payload into a `SealedBlock` for executing against the tree.
    - defaults used for values not needed in the batch
3. if valid, engine calls `try_insert` on tree if status is not syncing
    - crates/execution/blockchain-tree/src/blockchain_tree.rs

## Optimization: Add Valid TXs to Own Pool
When a batch is considered valid, the worker adds the decoded transactions to it's pending pool.

### Questions
- Can we bypass tx validation when adding to the pool?
    - The worker's own batch validator expensively validated the transactions already
- Should the batch validator optimistically add the transactions (ignore errors) before or after sharing result with worker?
    - before by spawning a new thread
        - `BatchValidator` needs `TaskExecutor` and `Pool`
            - add Pool and Executor generics?
        - executor spawns task before returning `Ok(())`
            - txs added optimistically
            - possible to still have duplicates, but at least we tried
            - doesn't really matter if adding txs failed
                - worth testing what happens when duplicate tx is added to the pending pool
- Is validation spawning threads in an optimal way?
    - this affects when transactions should be added to the txpool
    - assume not, regardless:
- Does this open an attack surface?
    - aka, does adding another worker's transactions to our own pending pool create a way to deny service?
    - what about supporting multiple workers?
        - they would each have their own rpc/txpools
        - these could be gossiped to each other via execution layer p2p
            - workers of the same primary gossip transactions like eth
        - may be useful to have specific worker just for bridging
            - but what if that worker pool gets stuck?
                - bridging stopped
            - better to gossip transactions amongst all workers
                - they are also more likely to get duplicates than geographically distanced nodes
