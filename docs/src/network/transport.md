# Transport

Telcoin Network nodes speak libp2p over QUIC and nothing else.
This page states what a node opens on the wire, the two keys that identify it, where its listener addresses come from, and the full set of chain-namespaced protocol IDs its substreams negotiate.

It is written for RPC providers, dapp and indexer operators, bridge partners, and validators who need to predict what a node dials, what it accepts, and why a peer that looks reachable can still never exchange a message.

The swarm and its QUIC settings live in
[`crates/network-libp2p/src/consensus.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/consensus.rs).
The protocol IDs live in
[`crates/network-libp2p/src/types.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/types.rs),
and the transport defaults live in
[`crates/config/src/network.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/config/src/network.rs).

## QUIC only

The swarm is built with a single transport: QUIC v1 over UDP.
There is no TCP listener and no TCP dialer, so a peer that offers only TCP is unreachable.

QUIC carries TLS 1.3 and multiplexes streams itself, so the node installs no noise handshake and no yamux muxer.
The libp2p dependency is compiled with only the request-response, gossipsub, tokio, QUIC, macros, and kad features, which is the mechanical reason none of the usual alternatives are available at runtime.

Two behaviours operators commonly expect are also absent.
There is no mDNS: a node never discovers peers by LAN broadcast.
There is no identify protocol: a node does not learn a peer's addresses or supported protocols from an identify exchange.
Everything a node knows about a peer comes from the signed records in the DHT and from peer exchange, covered in [Discovery](discovery.md).

Each node runs the primary and every worker as fully isolated swarms in one process.
Seven behaviours compose each swarm: the peer manager, a per-peer connection limiter, gossipsub, the consensus request-response protocol, the peer-exchange request-response protocol, Kademlia, and the bulk-sync stream behaviour.

## Connection parameters

The QUIC and swarm rows below are the shipped defaults, read from the node's network config file with any missing field falling back to the value shown.
The per-peer connection ceiling is not a config field: it is fixed in code and identical on every node.

| Setting | Default | Effect |
|---|---|---|
| QUIC handshake timeout | 65 s | Ceiling on the initial handshake |
| QUIC max idle timeout | 30 s | Inactivity before the connection is torn down |
| QUIC keep-alive interval | 5 s | Inactivity before a keep-alive packet is sent |
| Max concurrent streams | 10,000 | Incoming bidirectional streams a remote peer may hold open on one connection |
| Max stream data | 50 MiB | Unacknowledged bytes in flight on a single stream |
| Max connection data | 100 MiB | Unacknowledged bytes in flight across all streams of a connection |
| Swarm idle connection timeout | 65 s | How long the swarm keeps an idle connection alive |
| Established connections per peer | 8 | Concurrent connections one peer may hold, inbound and outbound combined |

The effective handshake ceiling is the smaller of the handshake timeout and the max idle timeout, so a handshake in practice has 30 seconds, not 65.
The keep-alive interval is set well below the idle timeout on purpose: a healthy but quiet connection is refreshed roughly six times before it would expire.
The swarm's own idle timeout is deliberately not the binding constraint — the strategy is to let QUIC keep connections alive rather than add another swarm behaviour for it.

The per-peer ceiling of 8 is generous headroom.
A peer needs at most one inbound and one outbound connection at a time, because this node dials only peers it is not already connected to.
The remaining slack absorbs reconnection churn while bounding a hostile peer to a fixed, small number of connections instead of an unbounded fan-out.
Peer counts are tracked by distinct identity, so without this ceiling one peer could open connections until the operating system ran out of descriptors.
[Peers](peers.md) covers the peer-count admission gate that sits alongside it.

Losing every listener is fatal.
When the last listener closes, the swarm returns an error rather than continuing, and the node stops.
A Telcoin Network node does not limp along unreachable.

## Two keys, two identities

A node carries two distinct keys, and confusing them is the most common source of "I can see it but I can't reach it".

The **libp2p network key** is the transport identity.
It produces the peer ID a node is dialed at, it terminates the QUIC handshake, and it signs every gossip message the node publishes — gossipsub runs in signed-authenticity mode, so an unsigned message is not a valid message.
The primary and each worker hold separate network keys, because each runs its own swarm.
Worker network keys are derived deterministically from the node's BLS keypair and a fixed seed, so worker 0's peer ID is stable across restarts and reinstalls — that identity is advertised on-chain and cached in other nodes' DHT stores, so it must not change.

The **primary's BLS key** is the consensus identity.
It signs headers, votes, and certificates, it is the key registered on-chain, and it is the DHT record key under which a node is discoverable.
Both the primary's record and every worker's record are stored under the *primary's* BLS key.

The practical consequence: you look a node up by BLS key and you connect to it by peer ID, and neither value is derivable from the other by a third party.
Signed node records are what bind the two together, and [Discovery](discovery.md) describes how they are published and verified.

## Listener addresses

Listener addresses are set once, at key generation.
`keytool generate --external-primary-addr` takes a single multiaddr for the primary and `--external-worker-addrs` takes a comma-separated list for the workers.
Both also read from the environment at generation time, as `TN_EXTERNAL_PRIMARY_ADDR` and `TN_EXTERNAL_WORKER_ADDRS`.

An address must be QUIC v1 over UDP, in the form `/ip4/HOST/udp/PORT/quic-v1`.
Do not append a `/p2p/` component: the tool derives it from the network public key and appends it, and supplying a different one is an error.
When either flag is omitted, the address defaults to `/ip4/127.0.0.1/udp/PORT/quic-v1` on a free port, which is useful only for local test networks.
The result is written to the node's info file and is the address advertised to peers.

Two environment variables override the address a node **binds** at runtime: `PRIMARY_LISTENER_MULTIADDR` and `WORKER_LISTENER_MULTIADDR`.
Each is read once, when its swarm first starts listening, and the node appends the `/p2p/` component the same way key generation does.
`WORKER_LISTENER_MULTIADDR` applies to worker 0 only.
Higher worker ids always bind their configured address, because one variable cannot name several distinct listeners.

These variables change only what the node binds.
The external address advertised to peers still comes from the node's info file, so an override that does not match what was generated makes the node reachable at one address and discoverable at another.

## There are no p2p command-line flags

Operators arriving from other clients go looking for `--p2p-port`, `--bootstrap-peers`, or `--listen-addr`.
None of them exist.
The node command's flags cover the named chain, a consensus-metrics socket, an instance number, observer mode, state export, the re-pack monitor, unused-port mode, a healthcheck port, a node name, and a tracing endpoint, plus a global data directory and the JSON-RPC server arguments.
Nothing on that list touches the p2p transport.

Listener addresses come from the generated node info and the two override variables above.
Bootstrap peers come from the genesis committee file in the data directory, not from a flag.
The transport settings in the table above are read from the network config file in the data directory, where any field may be omitted to take its default.
The chain id is not an operator tunable at all: it is stamped onto the network config from genesis at startup and is deliberately never written to the config file.

## Protocol IDs

Every wire protocol is namespaced by the genesis chain id and by role, so a node advertises a distinct name per `(chain, role)` pair.
`{chain_id}` is the genesis chain id and `{worker_id}` is the worker id.

| Purpose | Primary | Worker |
|---|---|---|
| Gossipsub | `/tn-meshsub-{chain_id}` | same prefix |
| Request/response | `/tn-primary-{chain_id}/0.0.2` | `/tn-worker-{worker_id}-{chain_id}/0.0.2` |
| Peer exchange | `/tn-primary-peer-exchange-{chain_id}/0.0.1` | `/tn-worker-{worker_id}-peer-exchange-{chain_id}/0.0.1` |
| Kademlia | `/tn-primary-kad-{chain_id}/0.0.1` | `/tn-worker-{worker_id}-kad-{chain_id}/0.0.1` |
| Bulk sync | `/tn-primary-sync-{chain_id}/0.0.1` | `/tn-worker-{worker_id}-sync-{chain_id}/0.0.1` |

The gossipsub row is a prefix, not a complete id.
Gossipsub appends its own versions, so a node on adiri, chain id 2017, advertises:

```text
/tn-meshsub-2017/1.1.0
/tn-meshsub-2017/1.0.0
```

Gossipsub is the one protocol that would otherwise be shared across chains, because it negotiates its own `/meshsub` name independently of the names below it.
Namespacing the topics alone would keep the messages apart but still let cross-chain peers open a gossip substream; folding the chain id into the protocol id closes that gap.
[Gossip topics](gossip.md) covers the topics that ride on the mesh, [Request-Response](request-response.md) covers what rides on `/0.0.2`, and [Sync Streams](sync-streams.md) covers the bulk-sync protocol.

## Why request-response is at `/0.0.2`

The request-response protocol is the only one above `/0.0.1`.
Issue [#739](https://github.com/Telcoin-Association/telcoin-network/issues/739) deleted request and response variants that had been kept alive on the wire during a rollout: the streamed epoch, partial epoch, and consensus-output requests, the missing-certificates request, the worker streamed-batch request, and their acknowledgements and replies.

Those variants were dead but positional.
Deleting them shifts the BCS discriminants of every variant that followed, so a message a `/0.0.1` node encodes decodes as a different message on a `/0.0.2` node.
The version bump landed in the same change for exactly that reason: a `/0.0.2` node never negotiates request-response with a `/0.0.1` peer, so the two never exchange a stale discriminant.
Kademlia, bulk sync, and peer exchange were untouched and stay at `/0.0.1`.

> [!WARNING]
> A node still on `/0.0.1` looks healthy against a `/0.0.2` network.
> Gossip, discovery, and bulk sync all negotiate normally, because those protocol names did not change.
> Only request-response silently fails to negotiate, which surfaces as a node that has peers but never completes a direct request.

## Mismatches never negotiate

A chain-id mismatch, a role mismatch, or a worker-id mismatch all produce the same outcome: the substream is never negotiated.
This is structural, not a check that runs after connecting.
A primary and a worker advertise different names, so they cannot talk to each other even inside the same process.
Two nodes on different chains advertise different names for every protocol, so they cannot talk at all.
Worker `k` of one validator reaches only worker `k` of the others.

The failure mode is quiet by design.
There is no error message describing the mismatch, because there is nothing to reject — the two peers simply have no protocol in common.

## Source of truth

| Behavior | Code |
|----------|------|
| QUIC transport, swarm behaviours, per-peer connection ceiling, fatal listener loss | `crates/network-libp2p/src/consensus.rs` |
| Protocol IDs and the gossipsub prefix | `crates/network-libp2p/src/types.rs` |
| QUIC and swarm defaults, chain-id namespacing | `crates/config/src/network.rs` |
| Network and BLS key derivation | `crates/config/src/keys.rs` |
| `--external-primary-addr` and `--external-worker-addrs` | `crates/telcoin-network-cli/src/keytool/generate.rs` |
| `PRIMARY_LISTENER_MULTIADDR` and `WORKER_LISTENER_MULTIADDR` | `crates/node/src/manager/node/start_epoch.rs` |
| Node command flags | `crates/telcoin-network-cli/src/node.rs` |

This page mirrors those files.
Update this page when those files change.
