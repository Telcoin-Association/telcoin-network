# TN Network - libp2p

Telcoin Network requires peer-to-peer network communication to achieve consensus.

## General setup

Nodes use a combination of request-response behaviour (for reliable broadcast) and gossipsub (unreliable).
Only validators in the current committee have publishing rights on gossipsub topics related to consensus.

Primary and worker each have their own instance of `ConsensusNetwork`.
These nodes in the network publish `NodeRecord` using the primary's BLS public key as the records key.
The records are queried at the start of each epoch to retrieve network information for the next committee.

## Behaviours

### Identify

Primarily used to confirm external addresses from peers.

### Gossipsub

Used to gossip small messages to indicate events for nodes to start downloading.

### Request/Response

Used to reliably message peers directly and exchange messages of large size (>20kb).

### Peer Manager

The `PeerManager` is controls connectivity for the swarm.
If a peer receives enough penalties, they are disconnected and temporarily banned.
Some penalties result in a permanent ban.

### Kademlia

Distributed hash table for publishing node records.
These records are used to find committee validators.

## Notes on Implementation

### Keep Alive

Connections between peers rely on QUIC transport for idle and keep alive messages.

### Message Verification

Staked validators are the only publishers on the gossipsub network.
The source of the message is used to verify a staked node signed the message before propagating to other peers.
If a peer sends an invalid message, the `PeerManager` assesses a penalty.

Messages are decoded in the applicaiton layer, and penalties are reported for peers who broadcast messages that fail to decode propeerly.
