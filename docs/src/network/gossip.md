# Gossip

Telcoin Network nodes flood four gossipsub topics: certificates, consensus output, epoch votes, and worker batch digests.
All four are permissioned.
A node accepts a gossip message only when the publisher's BLS public key is on that topic's allowlist for the current epoch, and it rejects everything else before re-propagating it.

This page is for RPC providers, dapp and indexer operators, bridge partners, and validators.
It gives the topic names, who may publish on each, how a publisher is authenticated, the message size limit, and which peer is penalized when a message is rejected.

The implementation lives in
[`crates/network-libp2p/src/consensus.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/consensus.rs) and
[`crates/config/src/network.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/config/src/network.rs).
[Transport](transport.md) covers the QUIC transport these topics ride on,
[Discovery](discovery.md) covers the signed node records that resolve publisher identities,
and [Peers](peers.md) covers the reputation system that the gossip penalties feed.

## The four topics

| Topic | Authorized publishers | Payload |
|-------|-----------------------|---------|
| `tn-primary-{chain_id}` | current committee | `Certificate` |
| `tn-consensus-output-{chain_id}` | previous, current, and next committee | `ConsensusResult` |
| `tn-epoch-vote-{chain_id}` | previous, current, and next committee | `EpochVote` |
| `tn-worker-{chain_id}-{worker_id}` | current committee | batch digest, `(Epoch, BlockHash)` |

Topics are identity-hashed, so the strings above are exactly what appears on the wire.
On the Adiri testnet, whose chain id is 2017, a single-worker node uses these four names:

```text
tn-primary-2017
tn-consensus-output-2017
tn-epoch-vote-2017
tn-worker-2017-0
```

The gossipsub protocol id is chain-scoped for the same reason the topics are:
the builder yields `/tn-meshsub-2017/1.1.0` and `/tn-meshsub-2017/1.0.0`.
Without that, meshsub would be the one wire protocol two chains still shared.

## What "permissioned" means

Each subscribed topic carries an allowlist of BLS public keys.
Verification of an inbound message reads that entry and resolves the message author's libp2p peer id to a BLS key:

```text
entry absent          -> topic not subscribed here          -> reject
entry is an allowlist -> accept only if the author's resolved BLS key is in it
entry is None         -> subscribed, any publisher accepted
```

The `None` case is a real capability of the mechanism: it makes a topic permissionless.

> [!NOTE]
> No production topic uses it.
> All four topics are subscribed with an explicit committee allowlist, on every node, on every epoch.
> A message from a peer outside the allowlist is rejected and is never re-propagated by an honest node.

Rejection is not the same as ignoring.
A rejected message is dropped and, where a culprit can be identified, costs that peer reputation.

## Topic scoping

Topics are scoped by chain and, for batch digests, by worker id.
They are never scoped by epoch.
Epoch scoping happens instead by re-subscribing to the same topic name at the start of every epoch, which overwrites the stored allowlist with the new committee's keys.

Subscriptions live on a process-lifetime swarm, so a topic subscribed in one epoch stays subscribed until it is explicitly dropped.
Unsubscribing must therefore remove the allowlist entry, not merely stop reading.
An absent entry means "not subscribed here, reject", which is the correct state for a topic this node has left.
A stale entry left behind would pin the topic to whichever committee was current when the node last subscribed, and honest publishers from every later committee would be rejected as unauthorized.

## Why two topics span three committees

Certificates and batch digests are current-epoch traffic, so the current committee is the whole allowlist.

Epoch votes and consensus output are epoch-boundary traffic.
An epoch's closing vote and its final consensus output are authored by the *outgoing* committee and gossipped into the next epoch.
A current-committee-only allowlist would reject exactly those in-flight boundary messages during rotation, and stop re-propagating them, stalling certification of the epoch that just closed.
A validator rotating in may likewise start publishing slightly early.
Widening the allowlist to the previous, current, and next committee also makes it agree with the window the peer manager already uses for validator penalty exemption, so the propagation-authorization window and the penalty-exemption window cannot disagree.
Peers that were never on a committee remain excluded.

## The authentication chain

An accepted gossip message has passed three independent checks, in this order.

1. **Message authenticity.**
   Gossipsub runs in `Signed` authenticity mode, so every message carries the source peer id and a signature over the message made with that peer's libp2p network key.
   This proves the author owns the network key, and nothing more.
2. **Identity resolution.**
   The peer manager maps a libp2p peer id to a BLS public key only through a signed node record it has verified, and the record's signature is bound to a `(chain, role)` domain so a record signed for one network never verifies on another.
   A record is accepted only when the network key it advertises matches the identity that published it, so relaying someone else's record never lets the relayer claim that identity.
   [Discovery](discovery.md) covers the record itself, and [Peers](peers.md) covers the mapping.
3. **Allowlist membership.**
   The resolved BLS key must appear in the topic's allowlist for the current epoch.

The chain has no shortcuts.
A peer with no resolved BLS identity fails step 3 on every restricted topic, regardless of how well-formed its message is.

## Message size

`MAX_GOSSIP_MESSAGE_SIZE` is 12,000 bytes, sized to hold the largest legitimate gossip payload, a `Certificate`.

It is a network-wide protocol constant, not a per-node tunable, and it is deliberately not readable from operator configuration.
The reason is the penalty path.
An oversized message fatally bans the peer that relayed it, and that attribution is sound only when every honest node applies the identical bound.
If one operator raised the limit locally, that node would forward payloads its neighbours consider oversized, and those honest neighbours would ban it.
If an operator lowered it, that node would ban honest peers forwarding legitimate traffic.

The bound is enforced symmetrically.
A node refuses to publish a message larger than the limit rather than emitting one its peers must reject, so on the first hop the originator is never the victim of its own ban.

## Penalty attribution

Rejection never propagates the message.
The reason for the rejection decides only which peer, if any, loses reputation.

- **Oversized payload: the relayer's fault.**
  An honest node never originates an oversized message and, under strict validation, never forwards one, so delivering one is misbehavior by the peer that delivered it.
  The relaying peer takes a fatal penalty.
- **Unauthorized author: the author's fault.**
  A forwarder that relayed content authored by someone else is not accountable for that content.
  The resolved author takes the fatal penalty and the relayer takes none.

If the accountable identity has not resolved — a peer can be connected and relaying before its node record arrives — the node skips the penalty entirely rather than charging whichever peer happens to be in hand.
The same skip covers an anonymous message and the case where this node's committee view lags a neighbour's.
Accepted messages carry both the relayer and the author onward, as optional identities, so a fault that only deep validation can surface is still charged to the author rather than the forwarder.

## Validation configuration

Gossipsub runs with strict validation mode, manual message validation, and a one-second heartbeat interval.
Manual validation is what allows the allowlist check to run before a message is re-propagated: the node reports accept or reject explicitly, and a rejected message is never forwarded.

Gossipsub's own peer scoring is deliberately left disabled.
The peer manager is the authoritative reputation system, and running gossipsub scoring alongside it would double-count the same misbehavior — once as a gossipsub score decay and again as a peer-manager penalty — with two sets of thresholds that could disagree about when to disconnect.
Two gossipsub events do feed the peer manager directly: a peer that does not support gossipsub is fatally penalized, and a peer that persistently fails to keep up with the mesh takes a mild one.

## Topic re-validation in the application layer

Passing network-layer verification is not the end of the check.
Every application handler re-reads the topic name on the message it received and confirms it matches the topic that payload type belongs on.
A certificate delivered on the epoch-vote topic, or a batch digest delivered on a different worker's topic, is a mismatch.

A mismatch is a fatal ban, charged to the author rather than the relayer, because the declared topic is part of the content the author produced.
The check exists because the two layers are configured independently: the network layer knows only that the publisher was allowlisted for whichever topic the message declared, not that the payload belongs there.

## Worker batch subscription is node-mode gated

The batch digest topic is the only one whose subscription depends on the node's role for the epoch.
Committee validators subscribe, which warms the batch cache the voting path reads.
Observers unsubscribe.

Observers gain nothing from the digest announcements: batch bodies already reach them inside the verified consensus output and epoch packs they download, so prefetching would refetch bytes already in flight.
The decision is two-sided rather than a simple skip, for the reason above — a node that subscribed as a validator in one epoch stays subscribed after it becomes an observer unless the subscription is explicitly dropped, and skipping would also skip the only refresh of that topic's allowlist.
See [Network architecture](../architecture/network.md) for the node roles this gate reads.

## Source of truth

| Behavior | Code |
|----------|------|
| Topic names | `crates/config/src/network.rs` (`LibP2pConfig` topic constructors) |
| Message size limit | `crates/config/src/network.rs` (`MAX_GOSSIP_MESSAGE_SIZE`) |
| Gossipsub configuration | `crates/network-libp2p/src/consensus.rs` (`ConsensusNetwork::new`) |
| Allowlist check and subscribe/unsubscribe | `crates/network-libp2p/src/consensus.rs` (`verify_gossip`, `NetworkCommand::Subscribe`) |
| Penalty attribution | `crates/network-libp2p/src/consensus.rs` (`RejectReason::penalty`) |
| Per-epoch subscription and allowlists | `crates/node/src/manager/node/start_epoch.rs` (`spawn_primary_network_for_epoch`, `spawn_worker_network_for_epoch`) |
| Application-layer topic re-validation | `crates/consensus/primary/src/network/handler.rs`, `crates/consensus/worker/src/network/handler.rs` |

This page mirrors those files.
Update this page when those files change.
