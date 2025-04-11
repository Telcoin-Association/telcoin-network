# Consensus Network

The [`ConsensusNetwork`](./consensus.rs) is the main event loop for all network events.

## LibP2P Behaviours

Consensus network currently uses three network behaviors.
Below is specific information on how they are managed.
The only supported transport layer is QUIC.

### Request/Response

The main RPC protocol between peers.

#### Managing Peers

Peers are managed through the main swarm method `add_peer_address` which emits `FromSwarm::NewExternalAddrOfPeer`.
The request-response behavior responds to this event by registering the peer so it can be dialed by peer id alone.
The `ConsensusNetwork` holds a `VecDeque` of connected peer ids for convenience.

### Gossipsub

The main implementation for high-throughput messages propogating throughout the network.
The protocol is optimized for throughput, so message sizes must be small.
Gossipsub has a robust peer scoring system, but it is not used right now because of the complexity of finely tuning the system.

#### Managing peers

Gossipsub manages peers using the `FromSwarm::ConnectionEstablished` and `FromSwarm::ConnectionClosed`.
Explicit peers are only used for validators and trusted peers so the `PeerManager` remains the source of truth.
Both validators and trusted peers do not receive penalties and maintain max scores.
Peers are still blacklisted directly on the gossipsub out of an abundance of caution.
However, the `PeerManager` is still expected to intercept these connections and initiate a disconnect.

### Peer Manager

The source of truth for peer addresses and reputations.

#### Managing peers

The `PeerManager` emits `PeerEvents` for the `ConsensusNetwork`.
These events affect the request-response and gossipsub protocols directly or indirectly.
See the specific behaviour for more details on how peers are managed.
