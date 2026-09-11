# P2P Network

How Telcoin Network nodes behave on the wire: a QUIC-only transport, four permissioned gossip topics, a persistent Kademlia DHT of signed node records, a reputation-scored peer manager, two request-response protocols, and a bulk-sync stream behavior.
It is written for RPC providers, dapp and indexer operators, bridge partners, and validators who need to know what a node dials, what it accepts, and what it rejects.

[Transport](transport.md) covers the QUIC-only transport, node identities, listener addresses, and the full protocol-ID reference table.
[Gossip](gossip.md) covers the four gossip topics, their committee publisher allowlists, and message validation, while [Discovery](discovery.md) covers Kademlia, signed node records, and the six ways a node finds the committee.
[Peers](peers.md) covers peer reputation scoring, connection limits, and bans.
[Request-Response](request-response.md) covers the consensus RPC and peer-exchange protocols and their wire format, and [Sync Streams](sync-streams.md) covers the bulk-transfer stream behavior used for catch-up.
[Network Architecture](../architecture/network.md) has the node-role overview that frames all of them.
