# Beacon vs Bullshark
Both consensus mechanisms are based on Proof-of-Stake (PoS). There exists many parallels between the two and a fundamental difference. This document captures some of those thoughts.

## Fundamental Difference
The fundamental difference between the two consensus protocols is how they track ownership of digital assets.

### Balance Models

#### The problem
Blockchains are most commonly used to record transactions of digital assets. Digital assets are most valuable when they are transferrable, and the asset's security relies on the fact that only the owner can initiate a transfer.

Transactions are ultimately just information containing how much of an asset is changing ownership.

What information is needed and how can the blockchain organize this information efficiently in a secure manner?

#### Solutions
UTXO and Account
https://www.horizen.io/academy/utxo-vs-account-model/


## Similarities

### Vocabulary
Bullshark                   Beacon
==========                  =======
Batch (ours)                Payload (get payload)
Batch (peer)                Payload (new payload)
Batch with quorum           Block
Block/Header                Proposed block in a non-canonical chain
Certificate                 Block tree off canonical chain (one cert = one block)
Leader's Certificate        Canonical Chain (forkchoice updated)

### Output

### Proposing
Beacon node requests the next available payload. The payload is sent to peers who include it in the trees.

Bullshark node has workers assemble batches. These batches are similar to ethereum blocks. Batches are verified by other validators. With enough votes to reach quorum (2f + 1), the validator produces the next block. The validator's block contains:
    - all of the batches from it's workers that reached quorum
    - all of the batches it's workers verified that come from other validators
The block header is then sent to all other validators for verification. Vaidators ensure that they have verified every batch contained within the proposed block's header for that round. If they do, they vote in favor. If they are missing a batch, they request a copy to verify it for themselves.

Once a block/header receives enough votes from other validators, the proposing validator brodcasts a Certificate with enough cryptographic information that any validator can verify that another validator voted in favor for the block. The Certificate is inserted into a DAG and used to elect the next leader. The leader's view of state becomes permananent and the chain reaches finality.

Happy path for a round:
- Every validator sees every other validator's batches
    - Every proposed block has every batch included
- A leader is elected and earns mining rewards
- All decendants of this leader's Certificate are built off this view of state

Worker requests it's next batch:
    - resolve the best block from pending transactions pool
        - akin to beacon's `get payload`, but for batches

Worker receives batch from peer:
    - ask EL to verify batch contents
        - execute batch transactions from current canonical state
        - akin to `new payload`

Primary receives enough certs from previous round to propose next block
    - request lattice block from EL
        - akin to `get payload`, but with multiple batches
    - how to optimize generating lattice block?
        - each time batch is added to primary's pending block, update EL tx pool?
        - maybe batches don't need parent hashes, just assign the parent hash when we verify the batch?
    - do batches even need to be verified? Can we just acknowledge transaction signatures are valid? We don't need to execute them until cert?
        - I think we still need to check nonce and balance and signature, so execute the transaction in the current state
        - is the tx valid at this moment in time? If yes, batch is considered valid
            - proposed block will handle the rest
