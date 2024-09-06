# Scaling RPCs
RPCs are the primary client for requesting information from the TN blockchain.

## Submitting Transactions
The protocol does not gossip transactions at the txpool level. Instead, each validator bundles transactions that it sees as valid and produces a "batch". The batch is sent to all other validators for verification.

## Update 01/28/2024
The worse-case scenario is every worker batch is identical, which would result in essentially what Ethereum already does. It's a waste of network resources to include duplicate transactions.

What is the correct way to handle duplicate transactions in Consensus Outuput?

- transactions are executed one by one, and errors for the transaction are ignored.
    - ensures only one version of the transaction is included (based on nonce)
    - gas / priority fee will go to the validator who's worker included the tx in the batch
    - IDEA: EL tracks addresses taking advantage of duplicate submissions (from nonce perspective)
        - known addresses are required to pay a higher gas price for 24hours
            - reinforces security by gas so users can't repeatedly submit multiple transactions
            - free market still for users who are eager to include tx and willing to pay more
                - empowers users while ensuring at least one validator is rewarded (a bit of a gamble though)
        - what if the user resubmmits a tx?
            - the price still increases
            - although unfortunate, the gas price is only raised temporarily
            - other validators may choose to waive the increase
                - this should not affect validators at large because the greater cost is paid to upload the tx
                - validators who choose to "ignore" gas increases for duplicate transactions pay the cost of uploading redundant data but reap no additional benefit
- worker batch maker should set base fee on a per batch basis?
    - pending block would be batch that hasn't reached quorum yet?
        - pending batches fill the pending que until the next canonical block forms, then prune the tree?
- execution is per batch within Consensus Output
    - how to track canonical tip block number?
        - use block "nonce"
            - u64: history of chain hasn't used this value, so okay to use
            - sufficiently large to handle block per 0.1 seconds
                - `2^64 / (36000*24 * 365) = 58494241735.5 years to reach max`
    - mechanism to track redundant / duplicate nonce transactions within multiple batches
        - increase gas per address

### Problems

#### Scaling Transaction Submission: One RPC per Validator
Users should only submit one transaction to one validator. Sending the same transaction to multiple validators presents issues within the protocol. Transactions with nonce issues are not a concern (validator `a` has tx1, validator `b` has tx2. validator `b` shouldn't accept tx2 until tx1 is finalized. tx2 could still be included in validator `a`'s batch with tx1).

It seems there's a need to limit which validators a user's wallet can interact with to prevent duplicate transactions or double spend attacks. I don't like that solution.

Maybe there's an element of mobile users submitting transactions on their network (aka to their network's validator)?

*Background for all hypotheticals*:
- 4 validators: a, b, c, d
    - each validator has only one worker
- Two canonical blocks exist: 0, 1. This is the round for block "2".
- tx1: user 1 submits transaction 1
- 2y: validator y's proposed batch the current round (2)

Imagine tx1 is sent to valiators `a` and `b`. They both include the transaction in their batch because it is valid at the current canonical state.

**Naive approach**: batches are viewed independently in relation to the canonical block. Certificate is drained of all transactions to produce the next canonical block in the Execution layer. This is currently how Telcoin Network is proceeding with consensus output.

*Scenario*:
`c` and `d` receive `a`'s batch first. They both agree `tx1` is valid and batch `2a` reaches quorum. When batch `2b` arrives, the validators also agree `tx1` is valid because the state of the canonical chain has not finalized `2a`'s copy of tx1 yet.

Both `2a` and `2b` reach quorum and are included in this round's certificate. The certificate is sent to the execution layer to update the canonical chain. However, the certificate contains duplicate transactions.

*Solution*: take all transactions from the certificate, remove duplicates, and execute the block.
*Downsides*:
- How are the results from the execution layer verified by other validators?
    - is it enough that batches contain "parent block" hash which is the last canonical block?
    - should the EL participate in producing the certificate as well?
        - I think so, but how?
- inefficient for validators to broadcast duplicate transactions
    - punish validators
- easily gamed by users to spam the network with duplicate transactions to every validator to ensure their transactions are included
- what's the point of the CL sequencing batches and transactions if the EL undoes all of this?

**Tree approach**: Batches are added to trees of the blockchain, similarly to Ethereum. The Certificate issued from consensus is the order that batches (eth blocks) should be added to the canonical chain.

*Scenario*:
`a`, `b`, `c`, `d` all produces batches at roughly the same time. Only `a` and `b` have duplicate `tx1` in their proposed batches.

*bad*:
`c` receives `2a` first and tries to add the batch onto its tree of blocks:[0 - 1 - 2c - 2a]. `2a` is valid and `c` votes in favor.

`d` receives `2b` first and tries to add the batch onto its tree of blocks:[0 - 1 - 2d - 2b]. `2b` is valid and `d` votes in favor.

Both `c` and `d` see each other's as valid. 

Neither `a` nor `b` see each other's as valid.

Because `c` saw `a` first, `d` is invalid.
Because `d` saw `b` first, `a` is invalid.

Quorum votes are:
`a`: 2
`b`: 2
`c`: 3
`d`: 3
result: only `c` and `d` reach quorum

All transactions for `a` and `b` are excluded from the block, including the duplicate `tx1`. What is to stop the user from trying again? What if `tx1` was submitted to all the batches? This is essentially a DoS attack since no batches can reach quorum.

**more efficient/sophisticated approach**: 
`c` and `d` receive `a`'s batch first. they vote in favor. then they receive `b` with a duplicate tx1. All validators compare the conflicting batches `a` and `b` to see which one has the most gas fees. That is the batch they keep. In this hypothetical, `b` has more gas fees so it is selected above `a`. However, `c` and `d` have already voted for `a`. 

Even if they've alread voted for `a`, they still vote for `b`. They then remove batch `a` from their primary's proposed block. When `a` sees `b`, it votes for `b` even though it's conflicting because `b` has more gas. `b` sees `a` and does not vote for `a`. `a` would still reach quorum, as would the others. The proposed blocks would not include `a`'s batch because `b` came along and essentially replaced it. The proposed blocks would only includ `2b`, `2c`, and `2d`.

Unless, one validator only saw `2a` and not `2b`. What would happen then? (as validator number increase to ~20). Hopefully enough validators would process `2b` before `2a` that they wouldn't have to retroactively remove `2a` from their list of valid batches. The worker's can't retract a vote for a batch once it's sent, so `2a` could still reach quorum since it votes for it's own batch.

This also presents a tough problem of one bad apple spoils the bunch.

*NOTE*:
The worker's batch is only added to it's primary's block if it reaches quorum.
If worker `a` is able to recognize `b` is a better batch, can it remove the duplicate transaction and resubmit?
Even tell it's primary, remove my batch for this round?

**smart contract hope**: since the native token for Telcoin Network is controlled by a smart contract, is it possible to enable a UTXO-like solution at the contract level? A lot of layer-2 solutions are turning to this and only using the account model after finality.

#### TX Confirmation Lost in Transit
Wallet submits a tx to a validator's rpc endpoint. The RPC receives the tx and sends back "OK", but the confirmation is dropped due to some network issue. The RPC adds it to the next batch and a worker broadcasts the batch containing the user's transaction to all peers for quorum.

The wallet still waits for a response that the tx was received by the RPC, but it never comes. So the wallet retries and submits the transaction again. The RPC needs a cache to keep track of transactions that are 'pending' in batches.

If the RPC only checks the canoncial chain, then the tx is still valid and should be included in the next batch. However, by the time consensus is reached and the next batch is broadcast, the tx will be a duplicate.

###### Solution
RPC must keep track of transactions that were already included in a pending batch.
