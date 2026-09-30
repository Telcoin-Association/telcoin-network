# Transaction lifecycle

1. Transaction creation - A user creates a transaction. This is typically done through a wallet interface or decentralized application (dApp) user interface. An example may be transferring TEL to another user. The user will use the user interface off the dapp or wallet to signal their intentions. They will then sign their transaction using their private key (most likely through their wallet). The signed transaction is now immutable and ready to be sent to a validator.
2.  Transaction batching - Upon receipt of the transaction, validators check the validity of the transaction. It does this by ensuring the transaction has:

    1. A valid user signature.
    2. Been initiated by a user who has access to all assets the transaction is trying to control. In this case, the user must have at least the amount of TEL they are sending (+ gas) present in their wallet.
    3. Correctly encoded data, if the transaction interacts with a smart contract.
    4. Been sent with sufficient gas.

    If the above conditions are met, the validator places the transaction in the pending pool. When the validator's worker is ready to propose the next batch, valid transactions are pulled from the transaction pool until a gas limit of 30 million is reached or there are no more transactions in the validators pool. The worker then broadcasts the sealed batch to the workers of the other committee validators, which validate the batch and acknowledge it. Once a supermajority (2f+1 by voting power) of the committee acknowledges the batch, its digest is handed to the validator's primary for inclusion in consensus.
3. Transaction finalization - The validator's primary includes the digests of batches that reached quorum in its next header and broadcasts the header to the other validators. Validators ensure the validity of the header and its batches before returning a signed vote. Once votes from a supermajority of the committee are collected, the primary assembles them into a certificate and broadcasts it to the network. Certificates form a DAG that the Bullshark protocol deterministically orders: when a leader certificate gains enough support, its sub-DAG is committed as consensus output, which every node then executes - each batch becoming a block in the EVM. At this point, the transaction is executed and is irreversible. It is finalized.

Blocks are only produced from committed consensus output, so there are no forks or reorganizations. For this reason, Telcoin Network does not have any concept of `pending` blocks.
