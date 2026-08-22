---
description: >-
  Telcoin Network boasts a number of security features. The features and
  architecture that enable them is outlined on this page.
---

# Security

### Security Features

Telcoin Network ensures security of digital assets to asset owners. This is achieved through the following key principles:

* Private signature keys are required to authorize any transaction on the Telcoin Network.
* Transactions interacting with a particular asset can only be authorized by the owner of the given asset.
* Smart contracts contain predefined rules which govern the use of particular assets. These rules are created by the smart contract creator and are written in the Solidity language.
* Upon transaction finalization, the changes to the assets it operates on or creates will be persisted to the Telcoin Network ledger. Future transactions will then be able to operate on these assets.
* Telcoin Network's security is preserved even when a small proportion of validators are adversarial.
* Every Telcoin Network operation can be audited to validate that assets have been correctly processed. This boosts transparency but also implies all network actions are publicly visible. Users may wish to maintain privacy by using multiple different addresses.
* Block construction is handled exclusively by known validators, preventing anonymous MEV attacks.

### Security Architecture

Telcoin Network is run by a set of distinct validators run by Global Systems for Mobile Communications Association ([GSMA](https://www.gsma.com/)) members. They run Telcoin Network software that allows them to reach consensus on valid transactions within the network. Users can stake their native TEL tokens on validator nodes, increasing that validator's staking power. The use of Byzantine fault tolerant broadcast and consensus ensures Telcoin Network remains secure so long as over 2/3 of the network's total stake is controlled by honest and non adversarial validators.

#### Keys

A transaction on the Telcoin Network is only valid if signed with the private key which controls the assets operated on by the transaction. This private key must be kept safe by the user and never shared with other parties. This ensures that even an adversarial validator cannot control assets not owned by them.&#x20;

Each private key corresponds to a public address to which assets can be sent. Sending assets to a specific address automatically creates that address. It is therefore as though all possible addresses exist from the genesis block.

#### Smart Contracts

The behaviour of all non native assets on Telcoin Network is defined by a smart contract. Telcoin Network has a few built in system contracts, but also enables anyone to write and deploy their own smart contracts.&#x20;

This permissionless property enables huge innovation but requires more care to be taken by users; It is recommended users only interact with smart contracts they trust, have been audited by individuals they trust, and they understand the logic of. Smart contracts are immutable, allowing them to be audited by third parties and enabling greater degrees of trust.

#### Finality

The asynchronous consensus mechanism “Bullshark” ensures transactions on Telcoin Network are confirmed quickly. Successful transactions are immediately finalized, resulting in no unexpected reorganizations of chain data.

Gas must be paid, in the form of TEL, to pay for the compute required to process a transaction. Transactions can be successful or may be aborted - either due to conditions within the smart contract not being met or due to an insufficient amount of gas being paid. Aborted transactions will have no effect on the state of the blockchain, but will still require some gas to execute. This also protects the system against denial of service attacks.

#### Censorship Resistance

Telcoin Network uses the Delegated Proof of Stake model to select validators. Validators are run by GSMA members and other Mobile Network Operators (MNOs) and allow users to stake their TEL on validators, sharing in revenue from gas fee income. In the rare case of a validator demonstrating poor reliability, the validator and the stakers who have staked to it will receive reduced rewards. The user's stake will always be safe, and cannot be confiscated by any network user, including the validators with whom they have staked. This mechanism ensures that Telcoin Network will continue to be run to the highest standards of reliability, honesty and liveness.
