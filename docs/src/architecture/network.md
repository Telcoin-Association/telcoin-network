# Peer-to-peer network

Every Telcoin Network node runs the same software and executes every transaction.
Nodes differ only in what they do on the peer-to-peer network: which topics they publish on, which they subscribe to, and how they hand accepted transactions to the committee.

This page is for RPC providers, dapp and indexer operators, bridge partners, and validators.
It describes the three node roles, the swarms a node runs, and how the chain id isolates one network from every other Telcoin Network deployment.
The [P2P Network](../network/README.md) section covers each subsystem in detail.

The swarm and subscription logic lives in
[`crates/network-libp2p/src/consensus.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/consensus.rs),
and the per-epoch wiring that decides a node's role lives in
[`crates/node/src/manager/node/start_epoch.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/node/src/manager/node/start_epoch.rs).

## Node roles

A node's role is chosen at every epoch boundary and can change mid-epoch.
A node is an `Observer` if it is not in the entering committee or was started with `--observer`.
Otherwise it starts the epoch as `CvvActive`, optimistically assuming it is caught up, and is demoted to `CvvInactive` later if that turns out to be false.
An existing `CvvInactive` state is sticky: it is carried into the next epoch unchanged until the node finishes catching up.

The current role is readable live over RPC as `tn_nodeMode`, so callers can observe transient states such as a restarted validator catching up.

### CvvActive

An active committee validator is fully synced and voting.
It publishes certificates on `tn-primary-{chain_id}`, a signed consensus result for each committed sub-dag on `tn-consensus-output-{chain_id}`, and epoch votes on `tn-epoch-vote-{chain_id}`.
Its workers subscribe to their own batch topic, `tn-worker-{chain_id}-{worker_id}`, so batch bodies are prefetched into a local cache before the vote path needs them.

Batches are not gossiped to reach quorum: a worker reports each sealed batch directly to every committee peer over the worker request-response protocol and waits for a quorum of acknowledgements; only then does it publish the batch digest on the batch topic.
See [Request-response](../network/request-response.md) for that exchange and [Gossip](../network/gossip.md) for the topics and their publisher allowlists.

### CvvInactive

A node in this mode is staked and in the committee but behind the rest of it, typically after a restart or a failure mid-epoch.
It does not run consensus; it follows consensus output through state sync until it is past the garbage-collection window, then promotes itself back to `CvvActive` and restarts the epoch.
The promotion is guarded: the node must reach the same consensus block number as the network's latest known header, and that header's commit timestamp must be less than five seconds old, so a stale record cannot promote a node that is not actually caught up.

An inactive validator stays subscribed to its worker batch topic: warming the batch cache it will shortly vote against is useful work, and a mode change does not clear that cache, so the warm-up survives promotion.

### Observer

An observer is any node that is not in the current committee, plus any node started with `--observer`.
It follows consensus output only and never votes.

An observer **unsubscribes** from the worker batch topic.
Batch bodies already arrive inside the verified consensus output and epoch packs it downloads through [sync streams](../network/sync-streams.md), so digest gossip would only fetch the same bytes a second time.

An observer also does not gossip the transactions it accepts; it forwards each one over JSON-RPC to the committee validator whose committee slot owns the sender, using endpoints discovered from validator records on the DHT.
Routing by sender keeps a single account's transactions on one validator, which preserves nonce ordering.
Delivery is best-effort on a background task, so batch production is never blocked by a slow or unreachable validator.

> [!NOTE]
> If no committee validator has advertised a JSON-RPC endpoint, an observer cannot forward at all.
> The batch is refused rather than dropped, and its transactions stay pending in the pool for a later attempt.

## Swarms and epoch interfaces

A node runs one libp2p swarm for its primary and one more for each configured worker.
Every swarm speaks QUIC and runs as a critical task for the life of the process; see [Transport](../network/transport.md).
Each worker's swarm is fully separate, so a batch gossiped by worker `k` of one validator only ever reaches worker `k` of the others.

Epoch boundaries do not rebuild swarms; they rebuild the network interface layered on top of them.
Each epoch refreshes the previous, current, and next committee membership, re-dials committee peers, and re-subscribes to the gossip topics so their publisher allowlists track the rotation.
Listeners bind only on the first epoch of the process.

Because the swarm outlives the epoch, subscription decisions are two-sided rather than skipped.
A node that subscribed to a batch topic in one epoch stays subscribed into every later epoch unless it explicitly unsubscribes, which is why an observer issues an unsubscribe rather than simply not subscribing.

## Chain namespacing

The chain id comes from genesis and is stamped onto the network configuration at startup.
It is not an operator tunable: it is never serialized to the config file, so genesis remains the single source of truth.

Every gossip topic embeds it:

```text
tn-primary-{chain_id}                 certificates
tn-consensus-output-{chain_id}        signed consensus results
tn-epoch-vote-{chain_id}              epoch-boundary votes
tn-worker-{chain_id}-{worker_id}      batch digests
```

So does every wire protocol, including gossipsub's own protocol id, which is the one family that would otherwise be shared across chains:

```text
gossipsub        /tn-meshsub-{chain_id}/1.1.0 and /1.0.0
request-response /tn-primary-{chain_id}/0.0.2
                 /tn-worker-{worker_id}-{chain_id}/0.0.2
kademlia         /tn-primary-kad-{chain_id}/0.0.1
                 /tn-worker-{worker_id}-kad-{chain_id}/0.0.1
sync streams     /tn-primary-sync-{chain_id}/0.0.1
                 /tn-worker-{worker_id}-sync-{chain_id}/0.0.1
peer exchange    /tn-primary-peer-exchange-{chain_id}/0.0.1
                 /tn-worker-{worker_id}-peer-exchange-{chain_id}/0.0.1
```

Two nodes on different chains therefore fail protocol negotiation before exchanging anything, and never share a gossip mesh.
The remaining protocol families sit at `/0.0.1`; only request-response has been bumped to `/0.0.2`, for a wire-format change that shifted encoded enum discriminants.

## Where to read more

- [Transport](../network/transport.md) — QUIC, connection limits, and the protocol id families above.
- [Gossip](../network/gossip.md) — topics, publisher allowlists, message validation, and re-propagation.
- [Discovery](../network/discovery.md) — the DHT, node records, and how committee endpoints are found each epoch.
- [Peers](../network/peers.md) — scoring, penalties, bans, and the peer exchange sent on a graceful disconnect.
- [Request-response](../network/request-response.md) — header votes, batch reports, and epoch record requests.
- [Sync streams](../network/sync-streams.md) — the bulk transfer path for consensus output, epoch packs, and batches.

## Source of truth

| Behavior | Code |
|----------|------|
| Role definitions and helpers | `crates/consensus/primary/src/consensus_bus.rs` (`NodeMode`), `crates/types/src/primary/node_mode.rs` |
| Role selection, per-epoch subscriptions, swarm interfaces | `crates/node/src/manager/node/start_epoch.rs` (`identify_node_mode`, `should_subscribe_batch_topic`) |
| Per-role consensus following and rejoin | `crates/consensus/executor/src/subscriber.rs` (`spawn_subscriber`, `catch_up_rejoin_consensus`) |
| Gossip topic names and chain id | `crates/config/src/network.rs` (`LibP2pConfig`) |
| Wire protocol ids | `crates/network-libp2p/src/types.rs` |
| Swarm construction and QUIC transport | `crates/network-libp2p/src/consensus.rs` (`ConsensusNetwork::new`) |
| Process-lifetime swarms | `crates/node/src/manager/node.rs` (`spawn_node_networks`) |
| Observer transaction forwarding | `crates/consensus/worker/src/worker.rs` (`Worker::disburse_txns`) |

Update this page when those files change.
