# Decisions

Decisions that need to be made regarding the network and what we want it to be

- centralized/decentralized transaction collection
    - validators collect transactions in a decentralized way per MNO
- fee distribution per block based on PoS
    - what about uploading transactions fees?
- duplicate transaction: increased gas for wallet
    - if a wallet submits duplicate nonce txs to multiple validators, they could all appear in Consensus output
    - worse case, the protocol is just as efficient as Ethereum
    - executor should record addresses that submitted duplicates and charge a higher gas fee for x amount of time
        - allows users who are willing to pay more to increase the likelihood of their txs included in next block while incentivizing validators to include the txs
            - executor could distribute gas for tx to all validators (split the fees)
            - or
            - only one validator gets fees still based on PoS order
        - rpc takes optional address parameter
            - it's on the user to pass in the user's address for increased accuracy, otherwise their tx might sit
- block size
    - limit batches so (# validators) * (# workers) = 30mil gas
    - calculate max block size based on 30mil * (# workers) * (# validators) ???
    - I think base fee should be calculated at the worker level
        - RPC closely tied to worker level. workers who are filling up batches need to increase gas prices
- epochs
