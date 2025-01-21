# TN Network - libp2p

Telcoin Network requires peer-to-peer network communication to achieve consensus.

Consensus p2p networking was initially built using [anemo](https://github.com/mystenlabs/anemo.git).
However, the protocol is moving to libp2p for all network communication.

This crate is separate while there are two existing network solutions in the codebase.
Eventually, this crate will replace tn-network and be the only network interface for p2p communication.

## General setup

Committee-voting validators (CVVs) use [flood publishing](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#flood-publishing) to broadcast messages to peers.
Non-voting validators (NVVs) subscribe to topics and gossip the messages received from CVVs.
CVVs do not subscribe to topics used to propogate network consensus. Instead, they use reliable broadcasting to other CVVs using a request/response model.
CVVs use anemo for p2p consensus networking at this time, but will eventually transition to libp2p as well.

## Worker Publish

Workers publish batches after they reach quorum.

## Primary Publish

Primaries publish certificates.

## Subscriber Publish

Subscriber publishes committed subdag results as `ConsensusHeader`s.
These headers comprise the consensus chain.
Clients use results from worker and primary topics to execute consensus headers.

## Notes on implementation

### Adding peers

Use `Swarm::add_peer_address` is used to associate a Multiaddr with a PeerId in the Swarm's internal peer storage.
It's useful for informing the Swarm about how it can reach a specific peer.
This method does not initiate a connection but merely adds the address to the list of known addresses for a peer.
This is useful to ensure the Swarm has an address for a future connection associated with a peer's id through manual discovery.

Adding a peer's id and multiaddr, then dialing by peer id does not work in tests.
Dialing the peer by multiaddr without adding the peer works.

`Gossipsub::add_explicit_peer` directly influences how the Gossipsub protocol considers peers for message propagation.
When a peer is explicitly added to Gossipsub, it's included in the Gossipsub internal list, which influences the mesh construction for topics and ensures that the peer is considered for message forwarding.

### State sync

Gossipsub messages use the data type's hash digest as the `MessagId`.
However, `IWANT` and `IHAVE` messages are private, and another protocol type is needed to manage specific state sync requests.
The gossip network is effective for broadcasting new batchess only.

### Message Verification

Staked validators are the only publishers on the gossipsub network.
The source of the message is used to verify a staked node signed the message before propagating to other peers.
If a peer sends an invalid message, the P₄ penalty is applied.

Peer scores will be implemented in a follow up PR to track penalties for peers on the gossipsub network.
Messages should be decoded in the applicaiton layer, and a P₅ penalty for peers who broadcast messages that fail to decode propeerly.
Messages should only be published by known validators, so application penalties are expected to happen infrequently.
