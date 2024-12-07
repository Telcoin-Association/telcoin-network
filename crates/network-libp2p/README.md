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

Workers publish blocks after they reach quorum.

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
The gossip network is effective for broadcasting new blocks only.

## [Peer Scoring](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#peer-scoring)

Peer scores are local values that represent a weighted mix of parameters for all topics on the network that allow nodes to respond quickly to bad actors.

`accept_px_score` should only be attainable by nodes with stake.
Validators are well-known, so they can help with discovery.
Peer exchange can happen when a node prunes too many peers.
Instead of only dropping peers, they also recommend other peers to pruned nodes.
Nodes should be weary of connecting with suggested peers unless it is from a reliable source.


## Heartbeat

Heartbeat interval should be short based on this graphic? https://docs.libp2p.io/concepts/pubsub/overview/#gossip

Heartbeats are when the gossip network applies many state changes.
Peers below the specified threshold (0) are pruned from the mesh during the heartbeat.


## Message Verification

- message.source should only be from current committee member
  - duplicates for ConsensusHeaders
- Message decoding

## Ideal flow

- validators are already running p2p networks
- validators open p2p gossip publish networks
- TAO deploys archive node to support gossip and discovery
- bridge subscribes
  - we want validators to explicitly add these peers?
  - always flood publish to bridge peers
- other nodes permissionlessly join gossip network


- closed source for now
- assume only bridging clients for now (testnet)
- security harden later
