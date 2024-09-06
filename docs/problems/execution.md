# Execution
Ethereum executes multiple blocks and creates chains. These chains may fork from the canonical tip (finalized through beacon consensus) until the next round of consensus is reached, which finalizes the canonical tip and preserves part of the blockchain "tree" for more forks to use.

## Problem
Traditional EVM-compatible chains (Ethereum, polygon, etc.) gossip transactions before consensus. Execution nodes build payloads in preparation for their validator client to request the next "best block". The best block is passed to the beacon node and gossiped for consensus. The approach makes sense considering the history of ethereum using PoW consensus. Gossiped transactions needed to reach most nodes because someone was going to execute the next block.

TN knows which validators are expected to produce blocks. Transactions are essentially "gossiped" in the consensus layer by sharing batches between workers. Workers validate batches based on the individual batch, not within context of extending the chain. Once a batch reaches quorum, it's included in the primary block, then eventually consensus is reached. Reaching consensus in TN is agreeing to the order of which transactions should be executed.

Originally, the naive approach was to include all transactions within `ConsensusOutput` during the execution of a single block. However, this could be exceedingly large: `block_gas_limit` = `batch_gas_limit` * `num_workers` * `num_primaries` * `num_batches_per_round` (expect to be `2`)

For a modest network of 10 validators with 4 workers each and a target batch size of `30mil` gas, the expected block size for output could reach a miximum of: `30_000_000` * `4` * `10` * `2` = `2_400_000_000` gas which is 8 times the gas limit in Ethereum as of this writing.

Unfortunately, this creates a new scalability problem.

Do worker batches reduce batch gas limits to keep the block gas limit at a desired number for `ConsensusOutput`?
    - ie) `batch_gas_limit` = `block_gas_limit` / `num_workers` / `num_primaries` / `num_batches_per_round` ?
    - this is possible, but counter-productive since workers can handle `30mil` gas without issue
        - as the network scales up, workers scale down

In Ethereum, block size is capped to prevent them from being arbitrarily large. Without a limit, more powerful nodes could out-perform other nodes causing other nodes to fall behind to the point they could not contribute towards the network's progression. The need for more compute power would then become a centralizing force.

TN does not have this concern since validators are expected to run enterprise-level hardware. In theory, a block size of `2.4bil` gas within reason. However, it still doesn't solve the issue of scaling works back as the number of validators or workers increases.

Instead, worker batches should be executed as ethereum-style blocks themselves.

## Solution
`ConsensusOutput` orders the batches that are then executed on a per-batch basis.

### Benefits:
- Batches are blocks
- Batches can maintain a `30mil` (or whatever) gas limit while also scaling out the number of validators and workers
- Workers control gas basefees individually
    - Users are incentivized to utilize all the resources available by finding workers with lower transaction traffic (causing lower base fees)
    - pending batch is the `pending_block`
        - gas is set based on the amount of gas used in the worker's previous batch
        - updated each time the batch reaches quorum

### Challenges:
- Canonical block number vs output from a particular round
    - can use block nonce / difficulty
    - block rewards through PoS mechanism
        - consensus decides leader for the output, not the block with this approach
            - output potentially contains several blocks/batches with the same leader
                - leader should still only receive rewards once
- User submits same transaction to multiple workers
    - the tx is only executed once
        - the first batch with the tx gets the gas fees
            - more complicated to split this up amongst all workers with the tx
            - technically unfair
                - there's a cost associated with uploading txs and batches
                - multiple workers paid the cost that gas is suppose to offset
    - workers should keep track of addresses abusing the system and charge higher gas for a certain amount of time
        - ensures gas still contributes to security
        - allows users who are willing to pay higher gas to increase their chances of including a tx sooner
            - still a gamble for workers competing to get the tx's gas fees
        - reactive approach for spam
            - address could do this once without paying a penalty

## Implementation Strategy
Workers manage batches as blocks.

### Batch Maker
Workers operate individual RPCs that collect transactions. Transactions are not gossiped, but organized into unique batches for the worker. Batches are sealed based on one of three conditions being met:
1. The maximum batch size (measured in bytes) is reached
2. The maximum batch size (measured in gas) is reached
3. The maximum batch time (measured in seconds) is reached

Once one of these conditions is met, the batch is "sealed" with a hash and broadcast to peers for validation.

At this point, the pending batch is used to update the RPC's metrics for "pending block".

TODO:
- Is `basefee` adjusted here or after the batch reaches quorum?
    - I think `basefee` for pending block is updated
    - `basefee` for next block is based on last batch to reach quorum

Once quorum is reached, the batch is shared with the Primary and is included in the Primary's next "proposed header". If the Primary's proposed header reaches quorum, then the batch will likely be included in the next round of `ConsensusOutput` (even rounds only).

Question: Are batches built off the canonical block or the last batch?

#### Risks and Mitigation Strategy
Potential issues with this approach and plans to mitigate their impact.

##### Execution
Batches are organized during consensus and executed as blocks in that order.

The result of executing `ConsensusOutput` is multiple blocks that correlate to multiple batches.

The block `nonce` and `mix hash` values can be used to store data associated with the `ConsensOutput`. For instance, the block nonce could be incremented for each output and the mix hash could be the SHA256 hash for the entire output.

Question: Where/how to assign block rewards from `ConsensusOutput`?

##### Duplicate Transactions
Transaction `a` and Transaction `b` have the same data, but sent to different workers. From the context of the worker, they are valid. The batches are validated per batch, so peers also see these batches as valid.

The problem occurs during execution after consensus. The `ConsensusOutput` contains duplicate transactions from two different batches.

Batch 1 contains transactin `b` and Batch 2 contains transaction `a`. Batch 1 is executed first and the worker's validator receives gas fees from that transaction. When batch 2 is executed, transaction `a` is invalid. The executor ignores this transaction, the worker's validator does not receive any gas fees, and the address from transactions `a`/`b` is added to a list for higher gas fees for a certain amount of time.

The worst-case scenario would be if every user submitted the same transaction to every worker. Batch 1 would theoretically be identical to Batch 2..n, where n is number of batches in `ConsensusOutput`. During execution, Batch 1 would execute successfully and then every other batch would fail. Only the validator for batch 1 would receive rewards. All addresses would be added to a higher gas fee schedule for the next x amount of time.

questions:
- the LRU cache could grow significantly large if every user is always submitting duplicate transactions. how to handle this?
- does the time extend if they submit again?
- does gas increase again?

TODO: explain how this is technically different and similar to Ethereum gossiping txs before consensus
