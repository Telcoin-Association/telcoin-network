# Quality of Service (QoS)
"for us we need to focus on QoS or high value transactions entering the mempool and how to avoid such sandwich attacks"

MEV attacks are only possible by MNO operators who construct batches.

## Exclusive Worker
Validators could agree to operate one worker each exclusively for their own MNO, QoS transactions.

MNOs would presumeably own multiple wallets. These wallets could be whitelisted to access the special worker's transaction pool. This tx pool would only accept transactions coming from whitelisted addresses. Addresses should only be whitelisted by one validator.

If base fees are calculated per worker, then these special workers with limited access can protect high value transfers from attack.

The idea is that each validator would have their own special access to submit secure transactions through batches only they have access to. Other validators would validate these batches and approve their execution.

## Attestation Service
Cross-chain stablecoin transfers using CCTP require an attestation service. The attestation service could be built into the protocol.

#### Verification Process
1) Monitoring On-Chain Events: The service watches for specific blockchain events, such as token burns.
1) Data Validation: It validates the event data against predefined rules and criteria to ensure authenticity and accuracy.
1) Consensus Mechanism: Consensus mechanism among multiple attesters (nodes) to agree on the validity of the event.
1) Signature Generation: Upon successful validation, the service signs the message to confirm its legitimacy.

It may be desireable to build a special worker for cross-chain attestations as well. Workers would exchange events they picked up and wait for quorum. Once quorum is reached, the worker signs the message. The user retrieves this signature and submits it on the destination chain with message data per contract requirements.

The signature could come from a hardware wallet.

Ideally, peer workers wouldn't propose the same transactions - only validate them. Or, does it matter? If each node's worker is observing events, they could independently verify and sign the attestation. I'm not sure exactly how to implement this, but the contract docs expect:

```solidity
/**
* @dev Attestation format:
* A valid attestation is the concatenated 65-byte signature(s) of exactly
* `thresholdSignature` signatures, in increasing order of attester address.
* ***If the attester addresses recovered from signatures are not in
* increasing order, signature verification will fail.***
* If incorrect number of signatures or duplicate signatures are supplied,
* signature verification will fail.
*/
```

###### Advantages of Protocol Integration
- consensus mechanism in place
- trusted parties for attestation

###### Disadvantage of Protocol Integration
- more overhead for validator nodes
- is attestation part of the protocol or is it a third-party service?
