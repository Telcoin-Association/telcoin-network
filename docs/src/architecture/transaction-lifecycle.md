---
description: >-
  Transactions go through a series of steps before being written to the Telcoin
  Network. These are outlined here.
---

# Transaction lifecycle

1. Transaction creation - A user creates a transaction. This is typically done through a wallet interface or decentralized application (dApp) user interface. An example may be transferring TEL to another user. The user will use the user interface off the dapp or wallet to signal their intentions. They will then sign their transaction using their private key (most likely through their wallet). The signed transaction is now immutable and ready to be sent to a validator.
2.  Transaction certification - Upon receipt of the transaction, validators check the validity of the transaction. It does this by ensuring the transaction has:

    1. A valid user signature.
    2. Been initiated by a user who has access to all assets the transaction is trying to control. In this case, the user must have at least the amount of TEL they are sending (+ gas) present in their wallet.
    3. Correctly encoded data, if the transaction interacts with a smart contract.
    4. Been sent with sufficient gas.

    If the above conditions are met, the validator places the transaction in the pending pool. When the node is ready to propose the next block, valid transactions are pulled from the transaction pool until a gas limit of 30 million is reached or there are no more transactions in the validators pool.
3. Transaction finalization - The full node now broadcasts the transaction certificate to all validators. Validators ensure the validity of the certificate, for example by checking it has enough signatures. Once the certificate has been deemed valid, the validators execute the transaction and unlock any assets that had been locked during certification. The full node checks that the effects, i.e. the state changes on the ledger, reported by each validator are identical. Once a supermajority of signatures is collected here, the node creates an `EffectsCertificate` . At this point, the transaction is executed and is irreversible. It is finalized.

The round trip of sending a transaction and receiving a validator signature takes less than half a second to complete. For this reason, Telcoin Network does not have any concept of `pending` blocks.
