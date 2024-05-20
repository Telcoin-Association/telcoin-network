# Unknowns
Telcoin Network is an account-based blockchain network that uses the EVM. The protocol is very similar to other EVM-based chains on the execution side, but uses Narwhal and Bullshark to reach consensus.

Narwhal and Bullshark is a mempool and DAG-based consensus that accels in parallel execution primarily seen by UTXO models only.

The nature of Telcoin Network validators is unique and allows the opportunity for some "trust" in the network. However, decentralized systems still need to be Byzantine fault tolerant.

As of the time of writing this, there are no EVM chains that run Narwhal/Bullshark consensus.

### Batches running in parallel to consensus
Validators need to verify their peers are executing blocks correctly.

Batches from eth blocks have many qualities that are useful for verifying execution layer results. Specifically, the block hash itself.

However, the black hash is affected by the parent block hash.

*Scenario*:
Worker requests next batch. The batch is built off of the current canonical tip. All transactions in the batch are valid at the current state of the canonical chain.

While a batch is broadcast and waiting for quorum, consenus could advance the round and update the canonical chain.

A peer that receives the batch forwards it to the execution layer, which has a new canonical tip. The "parent hash" of the batch is no longer valid. Are the transactions? Hopefully, but now we have a problem because the batch is invalid whin the context of the EL.

*Idea*: What if the batch doesn't include the parent hash? During verification, only the transactions are executed on the canonical chain to see if they are valid.

Primaries receive enough certificates from the previous round that they propose their next block. To propose the next block, they request it from the EL. The EL includes all the hash information that other nodes can use to verify a peer's EL.

This would allow workers to continue to produce batches independently of consensus because the batch of transactions is either valid or not.

The validator's EL should execute transactions in the order of the `IndexMap` contained in the header's payload field. This ensures an order for all validators to execute the transactions.

This still has the problem of:
- one bad tx in a batch ruins the entire batch
- validators could be passing around duplicate transactions and wasting bandwidth
    - this is what ethereum already does though
        - as long as all the batches aren't identical, there is some optimization here
        - problem for down the road to solve
    - in the worst case, every peer worker produces the same batch
        - only one batch would be needed to execute the next canonical block
            - first batch executes, all others are duplicates, none have an affect
        - this would be wasteful storage, but as long as execution filters out wasteful data, nodes only need the canonical tip to start producing batches
            - are there attack surfaces to this level of trust?
                - a batch with invalid parent block would not receive votes
