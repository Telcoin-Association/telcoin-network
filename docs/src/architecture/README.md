---
description: >-
  Telcoin Network is a scalable, secure and efficient blockchain. Here's how
  we've managed it.
---

# Architecture

### Key Components

Telcoin Network is a modular blockchain. This is in contrast to many traditional (monolithic blockchains).  While a monolithic blockchain performs a number of functions on a single layer, a modular blockchain separates the blockchain's services into separate modules, each specializing in one aspect of the blockchain's offering. This allows for greater flexibility, efficiency, and scalability.

The separate modules, or layers, within Telcoin Network are introduced below.

#### Consensus Layer

The consensus layer ensures agreement between validator nodes on the state of their common ledger. It handles transactions broadcast to the network by checking each transaction is valid and is ordered correctly. In this way, it is critical to the smooth and secure operation of the blockchain without the need of a centralized party.

Two key technologies, [Narwhal and Bullshark](./#consensus-layer), contribute to Telcoin Network's consensus layer. Narwhal is primarily concerned with the mempool - the initial handling of transactions which are broadcast to the network, while Bullshark handles the finalizing and recording of transactions within the [Settlement Layer](./#settlement-layer).

#### Execution Layer

The Execution Layer acts as the blockchain's processing unit, analogous to the central processing unit (CPU) of a computer. It processes user initiated transactions, which can range from sending the network's native token, TEL, between accounts, to the more complex execution of smart contracts that govern decentralized applications (dApps). Telcoin Network uses the Ethereum Virtual Machine (EVM) for its execution layer, meaning it can run any smart contracts present on other EVM chains.

#### Data Availability Layer

The Data Availability Layer guarantees that all necessary data for validating transactions is readily accessible to all network participants. Telcoin Network utilizes a scalable number of 'workers' which share collections of transactions, thus ensuring the data required for processing (execution) and validating (consensus) is available to all nodes. This mechanism supports the data availability layer by making sure that all necessary information for confirming the state of the blockchain is accessible, enabling nodes to execute and validate transactions accurately.

#### Settlement Layer

The Settlement Layer is akin to the blockchain's ledger, ensuring the permanence and immutability of transactions. This layer serves as the ultimate source of truth within the blockchain, providing finality and certainty to all operations.

Bullshark is integrated with Narwhal to finalize transactions and ensure they are immutably recorded on the blockchain. It employs a Byzantine Fault Tolerant (BFT) consensus algorithm, optimized for finality and security. Bullshark is responsible for reaching agreement on the state of the blockchain, ensuring all nodes are synchronized and that transactions are irrevocably settled. Settlement happens quickly and efficiently, without the concept of “forks” or “reorganizations”.
